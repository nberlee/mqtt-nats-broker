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
// is the one lock the delivery path must not contend on. A new stream is also the
// schema-compatible shape: an older broker simply never looks at it.
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
// is not optional decoration: after a restart the recovered pending entry is the ONLY
// route by which the claim reaches the terminal paths that must record the affine side
// as complete (see the mqttPending comment on q2AffClaim). Empty for every ordinary
// delivery.
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
func (as *mqttAccountSessionManager) enumerateInflightSubjects(filter string) (map[string]uint64, error) {
	as.mu.RLock()
	hook := as.testInflightEnumerate
	as.mu.RUnlock()
	if hook != nil {
		return hook(filter)
	}
	return as.jsa.streamSubjects(mqttInflightStreamName, filter)
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
	sess.asm.deleteInflightRegister(ack.regSeq)
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
	nonce, q2AffClaim string) (uint64, bool) {
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
		Nonce: nonce, Claim: regClaim, ClaimSeq: regClaimSeq, Q2AffClaim: q2AffClaim, Retired: true,
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
		case cur != nil && cur.Nonce == nonce && !sess.ownsSharedInflightRegister(dur, cur.Claim):
			sess.abandonForeignSharedInflightRegister(c, dur, nonce, subject)
			return 0, false
		case cur != nil && cur.Nonce == nonce && cur.Retired:
			// Ours, already retired: an earlier attempt landed and lost its response.
			return smsg.Sequence, true
		case cur != nil && cur.Nonce == nonce:
			expectSeq = smsg.Sequence // our record, moved: retry the CAS against it
		default:
			// A foreign or unreadable record holds the subject, so this register is gone
			// and cannot be marked. Not a hazard by itself — an unreadable register is
			// itself treated as unaccountable on resume — but not a success either.
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
		as.deleteInflightRegister(cur.regSeq)
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
		case cur != nil && cur.Nonce == nonce && !sess.ownsSharedInflightRegister(jsDur, cur.Claim):
			sess.abandonForeignSharedInflightRegister(c, jsDur, nonce, subject)
			return nil
		case cur != nil && cur.Nonce == nonce && cur.Cancelled:
			return nil // ours, already marked
		case cur != nil && cur.Nonce == nonce:
			expectSeq = smsg.Sequence
		default:
			// A foreign or unreadable record holds the subject, so our register is gone.
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
	seq, ok := sess.markInflightRegisterRetired(c, rs.pi, key.jsDur, key.sseq, rs.regSeq, rs.nonce, rs.q2AffClaim)
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
	if sess.voidRegistersOnStream(c, voided) && !sess.proveVoidedRegs(c, tok) {
		mqttErrorf(c, "MQTT: the void-register verdict(s) of client %q are not durably recorded yet; "+
			"the next convergence point retries", sess.id)
	}
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
// teardown's purge to remove, exactly as before, and only the verdict is recorded.
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
			// about to be freed by the purge. Record the verdict and leave the rest alone.
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
	if c == nil || cc == nil || cc.DeliverSubject == _EMPTY_ {
		return nil
	}
	c.mu.Lock()
	sub := c.subs[cc.DeliverSubject]
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
	for _, f := range filters {
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
		if f.shared {
			for _, dur := range mqttSharedDurablesForKey(f.sessionKey()) {
				void(dur)
			}
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
// The six callers all destroy or abandon a consumer without acking its deliveries —
// a denied reserved-namespace subscription, a 3.1.1 forced shared leave, a deferred
// consumer delete, a shared-consumer release, a surviving disconnect's shared purge,
// and a non-drain UNSUBSCRIBE teardown. Each one frees identifiers, so each one owes
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
				addReg(mqttVoidedReg{seq: ack.regSeq, nonce: ack.regNonce, pi: pi, dur: durable, sseq: sseq})
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
	if !sess.tombstoneCancellationsOnStream(c, cancels) {
		return true
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
func (sess *mqttSession) proveVoidedRegs(c *client, tok recordTx) bool {
	sess.mu.Lock()
	rev, saved := sess.voidedRegsRev, sess.voidedRegsSavedRev
	sess.mu.Unlock()
	if rev == saved {
		return true
	}
	if tok == (recordTx{}) {
		return sess.persistVoidedRegs(c)
	}
	// The caller's transaction, since these paths hold their packet's fence and a
	// transaction of our own would simply be refused. An ambiguous outcome counts as not
	// proven: the caller withholds its irreversible step and the readback settles it.
	if err := sess.save(tok); err != nil {
		mqttWarnf(c, "MQTT: could not persist the void-register verdicts of client %q: %v", sess.id, err)
		return false
	}
	// The save credited the revision it marshalled — at least rev, since the set can only
	// have grown since the sample above. See creditVoidedRegsLocked.
	return true
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
	rec := &mqttInflightRecord{
		PI: rc.pi, Dur: rc.durable, Seq: rc.sseq, QoS: 1, Nonce: rc.nonce, Claim: regClaim, Cancelled: true,
	}
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
			expectSeq = 0
			continue // raced away; retry the create
		case cur != nil && cur.Nonce == rc.nonce && !sess.ownsSharedInflightRegister(rc.durable, cur.Claim):
			sess.abandonForeignSharedInflightRegister(c, rc.durable, rc.nonce, subject)
			return nil
		case cur != nil && cur.Nonce == rc.nonce && cur.Cancelled:
			return nil // already marked, by us or by the store's own fallback
		case cur != nil && cur.Nonce == rc.nonce:
			expectSeq = smsg.Sequence // the store landed; replace it at that sequence
		default:
			// Foreign or unreadable, and nothing here can say which of the two it is.
			// Fail closed rather than overwrite a record that may belong to a delivery
			// the client is holding.
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

// mqttInflightSourceCache resolves the state of many source deliveries with a bounded
// number of JetStream requests instead of two per record: one consumer lookup per
// DURABLE, and one batched message load per stream.
//
// It exists for the resume path, which runs before the CONNACK. Classifying 65_535
// registers one at a time is upwards of 100_000 serial round trips; the same work here is
// a handful of consumer lookups plus a few batches.
//
// Every uncertainty is preserved exactly as the single-record path preserves it: a
// consumer that cannot be read, a consumer that is gone, and a message load with no
// response are all Unknown, never Complete.
type mqttInflightSourceCache struct {
	as        *mqttAccountSessionManager
	consumers map[string]*ConsumerInfo // durable -> info; nil means absent or unreadable
	known     map[string]bool          // durable -> a lookup has been ATTEMPTED
	resolved  map[string]bool          // durable -> that lookup completed, so nil means absent
	states    map[string]mqttInflightSourceState
}

func (as *mqttAccountSessionManager) newInflightSourceCache() *mqttInflightSourceCache {
	return &mqttInflightSourceCache{
		as:        as,
		consumers: make(map[string]*ConsumerInfo),
		known:     make(map[string]bool),
		resolved:  make(map[string]bool),
		states:    make(map[string]mqttInflightSourceState),
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
				if i >= len(ok) || !ok[i] {
					continue // unreadable: no entry, which resolves to Unknown
				}
				// A nil info with ok means authoritatively ABSENT, which classifies as Lost
				// rather than Unknown: unrecoverable, but known to be so.
				sc.consumers[dur], sc.resolved[dur] = infos[i], true
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
				default:
					// Indeterminate: stays Unknown, never Complete.
				}
			}
		}
	}
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
func (sess *mqttSession) storeInflightRegister(c *client, pi uint16, qos byte, jsDur string, sseq uint64, nonce, q2AffClaim string) bool {
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
		case cur != nil && cur.Dur == jsDur && cur.Seq == sseq:
			// Someone else recorded exactly what we are recording. The replacement below
			// is a rewrite of a shared register, so it honors the claim like every other
			// reconcile — and this branch is the one the nonce-match gates cannot cover,
			// because the occupant carries the OTHER writer's nonce. Unclaimed
			// replacement is the three-owner resurrection of the takeover race: a stale
			// object whose own write raced a resume would take the slot back at the
			// claimed sequence, and its close would then delete the exchange's only
			// durable identity. The slot is the claim holder's; this allocation is
			// abandoned, AckWait re-offers the source, and a register the loser already
			// landed surfaces as the existing duplicate-source fail-closed reservation
			// at the next resume.
			if !sess.ownsSharedInflightRegister(jsDur, cur.Claim) {
				sess.abandonForeignSharedInflightRegister(c, jsDur, nonce, subject)
				sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
				return false
			}
			// Take the slot over at its exact sequence so the record carries OUR nonce,
			// rather than adopting a record whose writer may still delete it.
			c.Debugf("MQTT: replacing an in-flight register for the same delivery on %q written elsewhere", subject)
			expectSeq = smsg.Sequence
		case cur != nil && cur.Cancelled:
			// A cancellation tombstone. It records a delivery that was NEVER transmitted,
			// so the identifier it names is free by construction and this record is pure
			// residue — its writer wanted it gone and only wrote it because the delete
			// could not be proven. Reaping it is therefore always safe, and it is what
			// keeps a securing pass whose store never followed (the teardown won the race
			// to the entry) from wedging this identifier until the next resume. Confirmed
			// and at the exact sequence observed (R4): an unproven reap makes the retried
			// create fail identically forever.
			c.Debugf("MQTT: reaping a cancelled in-flight register on %q at sequence %d", subject, smsg.Sequence)
			if derr := as.deleteInflightRegisterConfirmed(smsg.Sequence); derr != nil {
				c.Errorf("MQTT: unable to reap a cancelled in-flight register on %q for client %q: %v",
					subject, sess.id, derr)
				sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
				return false
			}
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
			if derr := as.jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); derr != nil {
				c.Errorf("MQTT: unable to reap a stale in-flight register on %q for client %q: %v",
					subject, sess.id, derr)
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
						results[i].outcome = mqttInflightClaimOwned
						results[i].reg = mqttRecoveredInflightReg{rec: &claimed, seq: seq}
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
					case cur == nil || cur.Nonce != fr.rec.Nonce:
						results[i].reg.seq = smsg.Sequence
					case cur.Cancelled:
						// A matching-nonce tombstone is the close decision. Delete only
						// the exact generation just loaded; a later rewrite is untouched.
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
						// lost.
						results[i].outcome = mqttInflightClaimOwned
						results[i].reg = mqttRecoveredInflightReg{rec: cur, seq: smsg.Sequence}
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
func (sess *mqttSession) recoverInflightRegisters(c *client) error {
	as := sess.asm
	sess.mu.Lock()
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
		return nil
	}

	// (1) The PUBREL records: identifiers already past PUBREC.
	rels, ok := sess.scanPubRelRecords(pubRelSubject)
	if !ok {
		sess.setPISpaceIncomplete(c, "the PUBREL records could not be scanned")
		return nil
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
		return nil
	}
	subjects := make([]string, 0, len(subjectCounts))
	for subject := range subjectCounts {
		subjects = append(subjects, subject)
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
			return nil
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
				return nil
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
	var drop []uint64
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
	drainOwned := make(map[string]bool)
	drainAmbiguous := make(map[string]bool)
	sess.mu.Lock()
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
			st.regSeq = 0
			drainAmbiguous[key] = true
			continue
		}
		st.regSeq = fr.seq
		drainOwned[key] = true
	}
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
	live := make([]mqttRecoveredInflightReg, 0, len(regs))
	for _, fr := range regs {
		if fr.rec.Cancelled || cancelledNonces[fr.rec.Nonce] {
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
			drop = append(drop, fr.seq)
			continue
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
				drop = append(drop, fr.seq)
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
				drop = append(drop, fr.seq)
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
			drop = append(drop, fr.seq)
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
			drop = append(drop, fr.seq)
		}
	}
	var retiredFailed, retiredDisplaced []mqttRecoveredInflightReg
	retired, claimClosed, retiredFailed, retiredDisplaced = sess.claimRecoveredSharedInflight(c, retired)
	displaced = append(displaced, retiredDisplaced...)
	claimConsumed = append(claimConsumed, claimClosed...)
	for _, fr := range claimClosed {
		if fr.seq != 0 {
			drop = append(drop, fr.seq)
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
		return fmt.Errorf("could not take ownership of %d in-flight register(s) during resume — "+
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
			// At least one member of this source's group could not be claimed, so
			// nothing about the group is this session's to decide: every member is
			// reserved, none is installed, and the source gets no cpending mapping.
			// The members keep their identity in poisoned, because a same-object
			// barrier re-run may hold a LIVE entry for one of them — the apply phase
			// demotes it to the same reservation a fresh object would get.
			if len(group) > unowned {
				c.Errorf("MQTT: %d in-flight register(s) of client %q for delivery %s/%d share the source with %d "+
					"register(s) another session owner holds: all are reserved, none is installed, and new outbound "+
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
			delete(sess.pendingPublish, fr.rec.PI)
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
			sess.pendingPublish[fr.rec.PI] = &mqttPending{regSeq: fr.seq, regForeign: fr.foreign}
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
		// The placeholder shape trackAsPubRel(pi, _EMPTY_) writes: the ack subject is
		// filled in by the PUBREL delivery, and mqttDeliverPubRelCb's duplicate guard
		// already tolerates an empty one.
		sess.pendingPubRel[r.pi] = &mqttPending{jsDur: r.srcDur, sseq: r.srcSeq}
		if r.srcDur != _EMPTY_ {
			// Reserve the SOURCE too, so a redelivery of it hits the usedForPubRel
			// guard instead of re-sending a payload the client has already PUBREC'd.
			reserveSource(r.srcDur, r.srcSeq, r.pi)
		}
		restoredRels++
	}
	for _, rv := range reserve {
		if ack, live := sess.pendingPublish[rv.rec.PI]; live {
			// In-memory state wins, but the register's SEQUENCE is the stream's to
			// say: the claim rewrite moved it, and a live entry left holding the old
			// one would aim every later exact-sequence operation at a message that no
			// longer exists. Nonce-guarded, so a newer allocation is untouched.
			if ack != nil && ack.regNonce == rv.rec.Nonce && !ack.regCancelled && ack.regSeq != 0 {
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
	// an operator removing the corrupt record (or recreating the stream, per the
	// compatibility boundary in docs/ARCHITECTURE.md) lifts it on the next resume.
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

	for _, seq := range drop {
		as.deleteInflightRegister(seq)
	}
	// A register the verdict set named is deleted with PROOF, and its verdict released only
	// against that proof. Forgetting it after a fire-and-forget delete would leave a register
	// nothing anywhere explains — an ordinary-looking one whose consumer is gone, which is
	// the exact state the verdict exists to prevent. A failed delete simply KEEPS the
	// verdict: it is already durable (this pass read it from the record), and the next scan
	// retries the delete against it.
	for _, cd := range dropByNonce {
		if derr := as.deleteInflightRegisterConfirmed(cd.seq); derr != nil {
			c.Warnf("MQTT: keeping the void-register verdict for sequence %d of client %q: %v",
				cd.seq, sess.id, derr)
			continue
		}
		sess.forgetCancelledReg(cd.nonce)
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
	return nil
}

// ---------------------------------------------------------------------------
// Stream prerequisites
// ---------------------------------------------------------------------------

// validateInflightStream verifies $MQTT_inflight can carry the registers.
//
// The list is derived from what THIS owner does, which is the doctrine every
// validator here follows and the reason no two of these lists are the same: a
// create-only CAS per identifier, a per-subject singleton, deletes at an exact
// sequence, a filter purge per retiring incarnation, and a bounded forward walk. It
// therefore requires the CAS and acknowledgement semantics, and — unlike
// $MQTT_willcancel, which never removes anything — it requires deletion AND purging
// to be permitted.
//
// It deliberately does NOT check ConsumerLimits.InactiveThreshold: nothing ever
// creates a consumer on this stream, so a stream-forced consumer limit cannot affect
// it. A validator that listed prerequisites its owner does not have would fail
// startups for no reason.
func validateInflightStream(si *StreamInfo, wantReplicas int) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, mqttInflightStreamSubjectPrefix+">", wantReplicas); err != nil {
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
	if cfg.DenyPurge {
		return fmt.Errorf("deny_purge is set: it blocks the per-incarnation purge a session teardown performs")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages on register subjects would be parsed as records", cfg.SubjectDeleteMarkerTTL)
	}
	return nil
}
