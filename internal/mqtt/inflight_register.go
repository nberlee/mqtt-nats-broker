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
	PI         uint16 `json:"pi"`
	Dur        string `json:"dur"`
	Seq        uint64 `json:"seq"`
	QoS        byte   `json:"qos"`
	Nonce      string `json:"nonce"`
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
		sess.cancelledRegNonces = make(map[string]struct{}, 1)
	}
	sess.cancelledRegNonces[nonce] = struct{}{}
	sess.owedRegsRev++
}

// releaseCancelledReservation frees an identifier a consumer teardown withdrew, once its
// register store is over. Held until then so that no other delivery can take the subject
// while a tombstone for the old one is being written to it (see mqttPending.regCancelled).
//
// Session lock NOT held on entry.
func (sess *mqttSession) releaseCancelledReservation(pi uint16) {
	sess.mu.Lock()
	if ack := sess.pendingPublish[pi]; ack != nil && ack.regCancelled {
		delete(sess.pendingPublish, pi)
	}
	sess.mu.Unlock()
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
		sess.owedRegsRev++
	}
	sess.mu.Unlock()
}

// mqttNonceSet and mqttSortedNonces convert between the in-memory set and the record's
// list form. The list is sorted so an unchanged set marshals identically, which keeps a
// record write from looking like a change when it is not.
func mqttNonceSet(list []string) map[string]struct{} {
	if len(list) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(list))
	for _, n := range list {
		if n != _EMPTY_ {
			set[n] = struct{}{}
		}
	}
	return set
}

func mqttSortedNonces(set map[string]struct{}) []string {
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

func mqttDebugf(c *client, format string, args ...any) {
	if c != nil {
		c.Debugf(format, args...)
	}
}

// oweRegisterDelete records a register whose removal is owed, in memory and then durably.
//
// Two things about the ordering are deliberate. The list is mutated BEFORE the fence is
// taken, which inverts the usual discipline, and it is safe here for a reason that does not
// generalise: the mutation is purely additive, and no outcome ever undoes it. A concurrent
// fence holder observing the addition and persisting it early is the desired result, not a
// half-applied transaction, because the obligation is a fact about JetStream that exists
// whether or not the record mentions it.
//
// And the addition can lose a race it cannot see: a readLoop transaction that has already
// marshalled its record commits without the new entry, while this off-readLoop writer was
// refused the fence. So durability is tracked by a revision counter rather than assumed
// from "I just tried to save" — see persistOwedRegisterDeletes.
//
// The nonce, when given, is the cancellation intent this obligation supersedes: both changes
// are made in ONE mutation, so no save can leave the record holding neither.
//
// Session lock NOT held on entry.
func (sess *mqttSession) oweRegisterDelete(c *client, regSeq uint64, nonce string) {
	if regSeq == 0 {
		return
	}
	sess.mu.Lock()
	sess.owedRegisterDeletes = append(sess.owedRegisterDeletes, regSeq)
	if nonce != _EMPTY_ {
		delete(sess.cancelledRegNonces, nonce)
	}
	sess.owedRegsRev++
	sess.mu.Unlock()
	sess.persistOwedRegisterDeletes(c)
}

// persistOwedRegisterDeletes writes the owed-cleanup list to the session record when the
// record is not already known to carry it, and reports whether it now provably does.
//
// The revision it captures before saving is the one it may later declare durable, never the
// list's current revision: an addition that lands after the marshal must stay unsaved, and
// an addition that lands before it is merely re-saved later, which costs nothing. Only
// saveLanded licenses the claim — an ambiguous write is resolved by a readback, and the
// cheaper answer for an unresolved one is to save again.
//
// Session lock NOT held on entry.
func (sess *mqttSession) persistOwedRegisterDeletes(c *client) bool {
	sess.mu.Lock()
	rev, saved := sess.owedRegsRev, sess.owedRegsSavedRev
	sess.mu.Unlock()
	if rev == saved {
		return true
	}
	// tryBeginRecordTx, never the blocking form: this runs on a delivery goroutine, and a
	// refused fence must leave the obligation pending rather than block. The flag stays
	// set, so the next convergence point retries — including the one at the end of the
	// UNSUBSCRIBE that is usually the holder here.
	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		return false
	}
	outcome, err := sess.saveClassified(tok)
	switch outcome {
	case saveLanded:
		sess.mu.Lock()
		if sess.owedRegsSavedRev < rev {
			sess.owedRegsSavedRev = rev
		}
		sess.mu.Unlock()
	case saveDefinitelyFailed:
		// Nothing landed. The in-memory list still drives this connection's retries, and
		// the record is not credited with an obligation it never recorded.
		mqttWarnf(c, "MQTT: could not persist the deferred cleanup of in-flight registers for client %q: %v",
			sess.id, err)
	case saveMaybeLanded:
		// The obligation exists either way and a duplicate delete is harmless, so nothing
		// is undone. The REVISION is a different matter: it may be claimed only on proof,
		// so the readback's continuations carry that decision. Registering neither — and
		// leaving the revision unclaimed unconditionally — would be safe but wasteful;
		// registering only the landed one would be wrong on the other branch.
		sess.retainOnUncertain(sess.owedRegsUncertainActions(c, rev))
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		mqttErrorf(c, "MQTT: record transaction not released after deferring register cleanup for client %q: %v", sess.id, terr)
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

// owedRegsUncertainActions is what the readback owes an obligation whose save was
// ambiguous, as a pair the resolver runs exactly one of.
//
// The not-landed verdict is the dangerous one, and an empty continuation is not an answer to
// it: the obligation would then be durable nowhere, and nothing would notice, because a
// convergence point that finds the list unchanged since its last attempt makes none. So the
// retry happens THERE, under the resolver's token — the only writer allowed at that moment,
// and the reason these continuations are handed one at all. Uncertainty is cleared before
// reverts run, so that save is not refused at entry.
//
// Neither continuation undoes anything: the obligation exists whatever the record says, and
// a duplicate delete is harmless. All they decide is whether rev may be called durable.
func (sess *mqttSession) owedRegsUncertainActions(c *client, rev uint64) (onNotLanded, onLanded func(recordTx)) {
	markSaved := func() {
		sess.mu.Lock()
		if sess.owedRegsSavedRev < rev {
			sess.owedRegsSavedRev = rev
		}
		sess.mu.Unlock()
	}
	return func(tok recordTx) {
		if serr := sess.save(tok); serr != nil {
			mqttWarnf(c, "MQTT: could not persist the deferred cleanup of in-flight registers for client %q "+
				"after an unlanded write: %v", sess.id, serr)
			return
		}
		markSaved()
	}, func(recordTx) { markSaved() }
}

// retryOwedRegisterDeletes re-attempts register deletes whose confirmed removal has not yet
// been proven, and drops only the ones it proves.
//
// A cancelled registration's register cannot be reconciled by anything else: the consumer
// that would classify its source is deleted by the same UNSUBSCRIBE, and a register whose
// consumer is gone is deliberately not read as complete, so it would reserve an identifier
// for the life of the session. Logging and forgetting the obligation is therefore not
// enough. Driven from the places that are guaranteed to come round again on a live session —
// every register store, every resume, and the tail of every UNSUBSCRIBE — so it converges
// without a timer.
//
// An entry stays visible in the list until its delete is PROVEN. Emptying the list first and
// putting the failures back afterwards looks equivalent and is not: a concurrent session
// save falling in that window persists a list with no obligations in it, and the entry that
// comes back is then in memory only, which is exactly the state a restart discards.
//
// Session lock NOT held on entry.
func (sess *mqttSession) retryOwedRegisterDeletes(c *client) {
	sess.mu.Lock()
	owed := append([]uint64(nil), sess.owedRegisterDeletes...)
	sess.mu.Unlock()
	if len(owed) > 0 && sess.asm != nil {
		var done []uint64
		for _, seq := range owed {
			if err := sess.asm.deleteInflightRegisterConfirmed(seq); err != nil {
				continue
			}
			done = append(done, seq)
			mqttDebugf(c, "MQTT: removed a deferred in-flight register at sequence %d for client %q", seq, sess.id)
		}
		if len(done) > 0 {
			sess.mu.Lock()
			// Filtered against the CURRENT list, not the snapshot: another cancellation may
			// have appended while the deletes were in flight.
			sess.owedRegisterDeletes = filterOutSeqs(sess.owedRegisterDeletes, done)
			sess.owedRegsRev++
			sess.mu.Unlock()
		}
	}
	// Unconditional, because "nothing was discharged" does not mean "the record already
	// says what it needs to": the addition may never have reached it.
	sess.persistOwedRegisterDeletes(c)
}

// filterOutSeqs returns seqs with every member of drop removed. Lists here are the size of
// one connection's cancelled registrations, so the quadratic scan is the cheap answer.
func filterOutSeqs(seqs, drop []uint64) []uint64 {
	var out []uint64
	for _, seq := range seqs {
		gone := false
		for _, d := range drop {
			if seq == d {
				gone = true
				break
			}
		}
		if !gone {
			out = append(out, seq)
		}
	}
	return out
}

// deleteInflightRegisterFor removes the register of a pending entry a terminal path
// just took out of the tracking maps. Nil-safe, so every such path can call it
// without first asking whether there was an entry at all.
//
// Session lock NOT held on entry: this issues a JetStream request.
func (sess *mqttSession) deleteInflightRegisterFor(ack *mqttPending) {
	if ack == nil || ack.regSeq == 0 || sess.asm == nil {
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
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return 0, false
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	rec := &mqttInflightRecord{
		PI: pi, Dur: dur, Seq: sseq, QoS: 2,
		Nonce: nonce, Q2AffClaim: q2AffClaim, Retired: true,
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
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return fmt.Errorf("session has no incarnation identity")
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	rec := &mqttInflightRecord{
		PI: pi, Dur: jsDur, Seq: sseq, QoS: 1, Nonce: nonce, Cancelled: true,
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

// deleteInflightRegisters removes a batch of registers at their exact sequences, PROVING each
// removal and recording an obligation for any it cannot.
//
// Confirmed rather than fire-and-forget, unlike the ordinary terminal deletes, and the
// difference is what happens next: a terminal path leaves behind a record whose source stays
// live and classifiable, so the store's reconcile, the next resume or the orphan sweep will
// remove it. These sequences come from a consumer TEARDOWN, and the consumer that would
// classify their sources is about to disappear — after which a surviving register is
// deliberately not read as complete, so a single lost delete reserves an identifier for the
// life of the session. Under a Receive Maximum of 1 that is the session.
//
// A failure therefore becomes an owed delete, persisted under the caller's transaction (tok
// may be zero where there is none) and retried at every convergence point. Callers run this
// BEFORE the consumer delete.
//
// Session lock NOT held on entry: each of these issues a JetStream request.
func (sess *mqttSession) deleteInflightRegisters(c *client, tok recordTx, seqs []uint64) {
	if len(seqs) == 0 || sess.asm == nil {
		return
	}
	owed := false
	for _, seq := range seqs {
		if err := sess.asm.deleteInflightRegisterConfirmed(seq); err != nil {
			mqttWarnf(c, "MQTT: deferring the removal of in-flight register %d for client %q: %v", seq, sess.id, err)
			sess.mu.Lock()
			sess.owedRegisterDeletes = append(sess.owedRegisterDeletes, seq)
			sess.owedRegsRev++
			sess.mu.Unlock()
			owed = true
		}
	}
	if owed && !sess.proveOwedRegs(c, tok) {
		mqttErrorf(c, "MQTT: the deferred in-flight register removal(s) of client %q are not durably "+
			"recorded yet; their identifiers stay reserved until the cleanup is discharged", sess.id)
	}
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
	seqPis := sess.cpending[durable]
	if len(seqPis) == 0 {
		return nil, nil
	}
	var out []mqttRegCancellation
	var transmitted []string
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
		sess.pendingPublish[pi] = &mqttPending{regPending: true, regCancelled: true}
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
			sess.owedRegsRev++
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
func (sess *mqttSession) purgeConsumerPendingLocked(durable string) ([]uint64, []mqttRegCancellation) {
	seen := make(map[uint64]struct{})
	var regs []uint64
	var cancels []mqttRegCancellation
	addReg := func(seq uint64) {
		if seq == 0 {
			return
		}
		if _, dup := seen[seq]; dup {
			return
		}
		seen[seq] = struct{}{}
		regs = append(regs, seq)
	}

	if seqPis, ok := sess.cpending[durable]; ok {
		delete(sess.cpending, durable)
		for sseq, pi := range seqPis {
			if ack := sess.pendingPublish[pi]; ack != nil {
				addReg(ack.regSeq)
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
					sess.pendingPublish[pi] = &mqttPending{regPending: true, regCancelled: true}
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
			addReg(rs.regSeq)
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
	if len(cancels) == 0 {
		return true
	}
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
	if !owed {
		return true
	}
	return sess.proveOwedRegs(c, tok)
}

// proveOwedRegs reports whether the session record is KNOWN to carry the deferred in-flight
// cleanup, saving it if it does not.
//
// Session lock NOT held on entry.
func (sess *mqttSession) proveOwedRegs(c *client, tok recordTx) bool {
	sess.mu.Lock()
	rev, saved := sess.owedRegsRev, sess.owedRegsSavedRev
	sess.mu.Unlock()
	if rev == saved {
		return true
	}
	if tok == (recordTx{}) {
		return sess.persistOwedRegisterDeletes(c)
	}
	// The caller's transaction, for the reason given above. An ambiguous outcome counts as
	// not proven: the caller withholds its irreversible step and the readback settles it.
	if err := sess.save(tok); err != nil {
		mqttWarnf(c, "MQTT: could not persist the deferred in-flight cleanup of client %q: %v", sess.id, err)
		return false
	}
	sess.mu.Lock()
	if sess.owedRegsSavedRev < rev {
		sess.owedRegsSavedRev = rev
	}
	sess.mu.Unlock()
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
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return fmt.Errorf("session has no incarnation identity")
	}
	subject := mqttInflightSubject(idHash, gen, rc.pi)
	rec := &mqttInflightRecord{
		PI: rc.pi, Dur: rc.durable, Seq: rc.sseq, QoS: 1, Nonce: rc.nonce, Cancelled: true,
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
func (sess *mqttSession) storeInflightRegister(c *client, pi uint16, qos byte, jsDur string, sseq uint64, q2AffClaim string) bool {
	as := sess.asm
	// Opportunistic, and cheap when there is nothing owed: this is one of the two points
	// guaranteed to come round again on a live session.
	sess.retryOwedRegisterDeletes(c)
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	ack := sess.pendingPublish[pi]
	stillOurs := ack != nil && ack.regPending && ack.jsDur == jsDur && ack.sseq == sseq
	var nonce string
	if stillOurs {
		nonce = ack.regNonce
	}
	sess.mu.Unlock()
	if as == nil || idHash == _EMPTY_ || gen == _EMPTY_ {
		// Unreachable for a real session: every session carries an incarnation
		// generation (mqttSessGenFromRecord rejects a record without one) and an
		// account manager. Fail closed rather than sending under an identifier nothing
		// records.
		sess.abandonInflightRegister(pi, jsDur, sseq)
		return false
	}
	if !stillOurs {
		// The entry went away under us (a discard, or a terminal path) or is no longer
		// the allocation we were asked to persist. Nothing to confirm and nothing to
		// send — but a cancelled reservation still has to be released, because this is
		// the same "the store is over" moment confirmInflightRegister represents, reached
		// when the teardown won the race to the entry before the first read.
		sess.releaseCancelledReservation(pi)
		return false
	}

	if nonce == _EMPTY_ {
		// Unreachable: trackPublish mints the nonce with every allocation that owes a
		// register. Fail closed rather than write a record with no identity — a nonce is
		// what proves a landed write is ours and what a cancellation names it by.
		sess.abandonInflightRegister(pi, jsDur, sseq)
		return false
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	rec := &mqttInflightRecord{
		PI:         pi,
		Dur:        jsDur,
		Seq:        sseq,
		QoS:        qos,
		Nonce:      nonce,
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
		case cur != nil && cur.Nonce == rec.Nonce:
			// Our own write, whose response we lost. This is the whole reason the nonce
			// exists: the sequence cannot tell us this, since the subject is reused.
			return sess.confirmInflightRegister(c, pi, jsDur, sseq, smsg.Sequence, rec.Nonce)
		case cur != nil && cur.Dur == jsDur && cur.Seq == sseq:
			// Someone else recorded exactly what we are recording. Take the slot over at
			// its exact sequence so the record carries OUR nonce, rather than adopting a
			// record whose writer may still delete it.
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
				sess.abandonInflightRegister(pi, jsDur, sseq)
				return false
			}
		case cur == nil:
			// Unreadable. It cannot be attributed to any delivery, so it cannot be
			// proven stale either — and it occupies an identifier we must not use.
			c.Warnf("MQTT: refusing packet identifier %d for client %q: an unreadable in-flight register occupies %q at sequence %d",
				pi, sess.id, subject, smsg.Sequence)
			sess.abandonInflightRegister(pi, jsDur, sseq)
			return false
		default:
			if as.inflightSourceState(cur) != mqttInflightSourceComplete {
				// A live or unprovable delivery owns this identifier. Never reaped: on
				// another broker this record is that delivery's only durable identity.
				c.Warnf("MQTT: refusing packet identifier %d for client %q: %q still names delivery %s/%d",
					pi, sess.id, subject, cur.Dur, cur.Seq)
				sess.abandonInflightRegister(pi, jsDur, sseq)
				return false
			}
			// Provably finished: a register whose delete was lost. Reaping must be
			// CONFIRMED — an unconfirmed reap makes the retried create fail identically
			// forever — and must target the exact sequence just observed (R4).
			c.Debugf("MQTT: reaping a stale in-flight register on %q at sequence %d (delivery %s/%d is complete)",
				subject, smsg.Sequence, cur.Dur, cur.Seq)
			if derr := as.jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); derr != nil {
				c.Errorf("MQTT: unable to reap a stale in-flight register on %q for client %q: %v",
					subject, sess.id, derr)
				sess.abandonInflightRegister(pi, jsDur, sseq)
				return false
			}
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("in-flight register not resolved after %d attempts", mqttInflightStoreAttempts)
	}
	c.Errorf("MQTT: unable to record the in-flight register for packet identifier %d of client %q: %v",
		pi, sess.id, lastErr)
	sess.abandonInflightRegister(pi, jsDur, sseq)
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
		// A cancelled RESERVATION is released here: a consumer teardown keeps the
		// identifier so that nothing can take the subject while this store is in flight,
		// and this is the moment that stops being true.
		if ack != nil && ack.regCancelled {
			delete(sess.pendingPublish, pi)
		}
		sess.mu.Unlock()
		// The allocation was CANCELLED under us — an UNSUBSCRIBE staging a drain, a
		// discard, a takeover — so this register describes a delivery that will never be
		// sent. Removing it is CONFIRMED and retried, unlike the ordinary terminal delete
		// which may be fire-and-forget: there the record is reconcilable, because its
		// source is still live and classifiable. Here the consumer is usually being deleted
		// in the same breath, and a register whose consumer is gone is deliberately NOT read
		// as complete — so a lost delete leaves an identifier reserved for the life of the
		// session, with no later pass able to resolve it.
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
					// Neither could be proven, which in practice means JetStream was unavailable
					// for both. The obligation is kept — in memory for this connection, and
					// PERSISTED with the session record so a restart cannot lose it. Without
					// that, an UNSUBSCRIBE completing its consumer delete leaves a register
					// indistinguishable from a started delivery, whose identifier and Receive
					// Maximum slot are then consumed for the life of the session.
					c.Warnf("MQTT: deferring cleanup of the in-flight register of cancelled packet identifier %d for client %q: %v (%v)",
						pi, sess.id, derr, merr)
					// The owed sequence REPLACES the cancellation intent, in one mutation, so
					// the record can never hold neither: a save that fails leaves the nonce
					// where it was, and one that lands carries the sequence instead. A
					// sequence is the more specific of the two — it says what to delete
					// rather than what to disregard.
					sess.oweRegisterDelete(c, regSeq, nonce)
				} else {
					// The tombstone is on the register itself, which is strictly stronger than
					// a nonce recorded beside it.
					sess.forgetCancelledReg(nonce)
				}
			} else {
				// Provably gone. There is nothing left for recovery to misread.
				sess.forgetCancelledReg(nonce)
			}
		}
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
func (sess *mqttSession) abandonInflightRegister(pi uint16, jsDur string, sseq uint64) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	ack := sess.pendingPublish[pi]
	if ack == nil || !ack.regPending || ack.jsDur != jsDur || ack.sseq != sseq {
		// A cancelled reservation fails this check by design, and it is released rather
		// than left: the store this attempt owned is over either way, and that is the
		// one thing the reservation was waiting for.
		if ack != nil && ack.regCancelled {
			delete(sess.pendingPublish, pi)
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

	// Anything owed from a cancelled registration goes first, so a register that should not
	// exist is gone before the classification below could reserve its identifier.
	sess.retryOwedRegisterDeletes(c)

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
	type foundReg struct {
		rec *mqttInflightRecord
		seq uint64
	}
	regs := make([]foundReg, 0, 8)
	// opaque holds registers whose body is unusable but whose subject names an
	// identifier this incarnation wrote. They reserve that identifier and nothing else:
	// there is no source to classify, and nothing may be deleted.
	var opaque []foundReg
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
			opaque = append(opaque, foundReg{
				rec: &mqttInflightRecord{PI: name.pi},
				seq: smsg.Sequence,
			})
			continue
		}
		regs = append(regs, foundReg{rec, smsg.Sequence})
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

	var reserve []foundReg
	var drop []uint64
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
	drainRegs := make(map[string][]foundReg)
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
			opaque = append(opaque, foundReg{rec: &mqttInflightRecord{PI: fr.rec.PI}, seq: fr.seq})
		}
	}

	// Retired registers first: these are not PUBLISH-phase deliveries at all. Their
	// exchange is past PUBREC with an unproven source acknowledgement, so the payload must
	// never be sent again while the source may still be re-offered. They reserve NO
	// identifier — the client completed the exchange, which freed it — and instead
	// reinstate the in-memory retirement, so the next attempt at that source is
	// acknowledged and discarded rather than delivered. This is the half a memory-only
	// retirement loses across a restart.
	var retired []foundReg
	live := make([]foundReg, 0, len(regs))
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
			if !fr.rec.Cancelled {
				// Named by a nonce alone, so its delete must be PROVEN before that nonce is
				// released: an unproven one would leave a register nothing anywhere explains.
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
			drop = append(drop, fr.seq)
			continue
		}
		live = append(live, fr)
	}

	// Group what survives by the source delivery it names. One register per source is
	// the ordinary case; more than one means a superseded broker wrote a second, on a
	// different identifier, for the same delivery.
	bySource := make(map[string][]foundReg, len(live))
	order := make([]string, 0, len(live))
	for _, fr := range live {
		key := mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)
		if _, seen := bySource[key]; !seen {
			order = append(order, key)
		}
		bySource[key] = append(bySource[key], fr)
	}
	for _, key := range order {
		group := bySource[key]
		if len(group) == 1 {
			reserve = append(reserve, group[0])
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
			len(group), sess.id, group[0].rec.Dur, group[0].rec.Seq)
		for _, fr := range group {
			opaque = append(opaque, foundReg{
				rec: &mqttInflightRecord{PI: fr.rec.PI},
				seq: fr.seq,
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
		if _, live := sess.pendingPublish[rv.rec.PI]; live {
			skipped++
			continue // in-memory state wins
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
		sess.pendingPublish[rv.rec.PI] = &mqttPending{regSeq: rv.seq}
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
	// A register the cancelled set named is deleted with PROOF, and its intent released only
	// against that proof. Forgetting it after a fire-and-forget delete would leave a register
	// nothing anywhere explains — an ordinary-looking one whose consumer is gone, which is
	// the exact state the intent exists to prevent. A failed delete restates the intent as an
	// owed sequence, which says the same thing more precisely.
	for _, cd := range dropByNonce {
		if derr := as.deleteInflightRegisterConfirmed(cd.seq); derr != nil {
			c.Warnf("MQTT: deferring removal of a cancelled in-flight register at sequence %d for client %q: %v",
				cd.seq, sess.id, derr)
			sess.oweRegisterDelete(c, cd.seq, cd.nonce)
			continue
		}
		sess.forgetCancelledReg(cd.nonce)
	}
	// Intents whose register was never found have nothing left to protect: this pass
	// enumerated the whole incarnation and read every record it named, so no register
	// carrying them exists. Only when nothing was opaque — an unreadable body is precisely
	// the case where a nonce cannot be compared, so its intent must be kept.
	if len(opaque) == 0 {
		found := make(map[string]bool, len(regs))
		for _, fr := range regs {
			found[fr.rec.Nonce] = true
		}
		for nonce := range cancelledNonces {
			if !found[nonce] {
				sess.forgetCancelledReg(nonce)
			}
		}
	}
	// Whatever the loops above changed should reach the record. Refused while the
	// CONNECT holds the fence, which costs nothing: the resume's own save carries it.
	sess.persistOwedRegisterDeletes(c)
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

	wantSubj := mqttInflightStreamSubjectPrefix + ">"
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != wantSubj {
		return fmt.Errorf("subjects %v (want [%q])", cfg.Subjects, wantSubj)
	}
	if cfg.Storage != FileStorage {
		return fmt.Errorf("storage is %v (want file)", cfg.Storage)
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
	if cfg.Mirror != nil || len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has a mirror or sources (want none): writes would land elsewhere")
	}
	if wantReplicas > 0 && cfg.Replicas != wantReplicas {
		return fmt.Errorf("replicas is %d (want %d)", cfg.Replicas, wantReplicas)
	}
	if cfg.MaxMsgSize > 0 {
		// A register is small and fixed-shape, but a cap is not this owner's
		// semantics: it would reject the write that must precede a send, stalling the
		// delivery rather than degrading anything.
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject the register write that must precede a send", cfg.MaxMsgSize)
	}
	if cfg.NoAck {
		return fmt.Errorf("no_ack is set: it suppresses the acknowledgement the create-only CAS depends on")
	}
	if cfg.DenyDelete {
		return fmt.Errorf("deny_delete is set: it blocks the exact-sequence delete every terminal path performs")
	}
	if cfg.DenyPurge {
		return fmt.Errorf("deny_purge is set: it blocks the per-incarnation purge a session teardown performs")
	}
	if cfg.Sealed {
		return fmt.Errorf("stream is sealed: it blocks all writes")
	}
	if cfg.SubjectTransform != nil {
		return fmt.Errorf("a subject transform is configured: it rewrites the per-identifier subject that CAS identity and loadLastMsgFor both depend on")
	}
	if cfg.RePublish != nil {
		return fmt.Errorf("republish is configured: it would mirror internal delivery state onto a user-visible subject")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages on register subjects would be parsed as records", cfg.SubjectDeleteMarkerTTL)
	}
	if cfg.AllowMsgCounter {
		return fmt.Errorf("allow_msg_counter is set: counter streams do not support the store semantics used here")
	}
	if cfg.PersistMode != DefaultPersistMode {
		return fmt.Errorf("persist_mode is %v (want the default): its durability semantics are unproven for this protocol", cfg.PersistMode)
	}
	return nil
}
