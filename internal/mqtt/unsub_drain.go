// The drain: a consumer that outlives the removal of its membership.
//
// TWO operations reach it, under two different rules.
//
// [MQTT-3.10.4-3] requires the server to complete the delivery of any QoS 1 or 2
// message it has already STARTED sending, even after the client unsubscribes.
// UNSUBSCRIBE used to tear the plain consumer down immediately: it marked the
// delivery subscription closed, dropped cpending/pendingPublish, and deleted the
// durable. $MQTT_msgs is interest-retained, so deleting the durable deleted the
// messages it still held — a started PUBLISH was simply lost.
//
// A re-SUBSCRIBE that DOWNGRADES a filter to QoS 0 owes the same completion, under
// [MQTT-3.8.4-3] and [MQTT-3.8.4-4]: the replacement must be accepted and the flow of
// publications must not be interrupted, so the broker may neither refuse the packet nor
// wait for the exchange. That path destroyed outstanding exchanges the same way, and it
// converts the consumer instead — at its packet's final-update boundary rather than at the
// filter's own staging, for the reasons in convertDowngradeCandidatesLocked.
//
// A drain is what that teardown becomes when started deliveries remain. The
// durable and its delivery subscription SURVIVE the membership's removal; the deliveries
// that had already been transmitted keep being redelivered and completed; fresh
// messages on the filter are discarded — at QoS 1/2, since a downgraded filter is served
// by the raw QoS 0 subscription instead. The drain is persisted with the session,
// so it survives a disconnect, a broker restart and a failover, and it lives until
// every started delivery completes or the session ends.
//
// The invariants that make it safe, in the order they matter:
//
//	I1  persist-before-ack. A JetStream delivery of a claimed sequence is acked
//	    only after a CONFIRMED save in which the drain no longer claims it. One
//	    helper owns every such ack; there is no bare sendAck on a claimed sequence.
//	I2  the commit points. QoS 1 has one phase. For QoS 2 the durable PUBREL INTENT
//	    commits the exit from the PUBLISH phase (past it the payload is never
//	    resent), and only a CONFIRMED PUBREL — stored AND in delivery — authorizes
//	    removing and acking the source claim.
//	I3  an entry exists in the record iff its durable must survive; the durable is
//	    deleted only after the save that removed the entry.
//	I5  only a delivery that was actually TRANSMITTED is drained. Sequences withheld
//	    by the in-flight cap were never sent, so they are discarded, not drained.
//	I7  single ownership. From the moment a drain is staged its ConsumerConfig lives
//	    exclusively in sess.drains and its cons[sid] entry is gone, so nothing can
//	    reach a drained consumer through cons — which would otherwise create a
//	    second durable on the same filter and double-deliver every new message.
//
// Scope: PLAIN consumers only. A started shared-subscription QoS 2 delivery is
// already session-affine (qos2_shared_affine.go), and shared QoS 1 redistribution
// to another member is permitted and stays documented.

package mqtt

import (
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
)

// drainPhase is where one drained sequence sits in its exchange. It is
// deliberately three-valued rather than a "do not resend" boolean: a boolean would
// conflate "a PUBREL exists" with "the store outcome is unknown", and the second
// must not authorize finalizing. Finalize-and-ack on an unlanded store would
// delete the drain and leave the client waiting for a PUBREL that does not exist —
// losing the message.
type drainPhase uint8

const (
	// drainPublish: the payload may be (re)delivered. QoS 1 never leaves it.
	drainPublish drainPhase = iota
	// drainPubRelUncertain: past PUBREC, but whether the PUBREL record exists is
	// unknown. The payload is never resent (the durable intent forbids it) and the
	// source is never finalized or acked. This is a WITHIN-CONNECTION state only:
	// it covers the interval between the intent write and the store outcome, and is
	// never a resume-time verdict, because the persisted intent always resolves the
	// question positively.
	drainPubRelUncertain
	// drainPubRelConfirmed: the PUBREL is proven to exist, because it was
	// DELIVERED (or found present at its sequence). Only this authorizes finalizing
	// and acking the source claim.
	drainPubRelConfirmed
)

func (p drainPhase) String() string {
	switch p {
	case drainPubRelUncertain:
		return "pubrel-uncertain"
	case drainPubRelConfirmed:
		return "pubrel-confirmed"
	default:
		return "publish"
	}
}

// mqttPersistedDrain is one draining consumer as it appears in the session record.
//
// It is deliberately NOT written to the record's Cons map, because Cons restores
// straight into sess.cons: a drain carried there would satisfy the cons[sid] lookup
// on a re-SUBSCRIBE, take the fromSubProto early return, and never create a delivery
// subscription — so the filter would look subscribed while nothing delivered it.
type mqttPersistedDrain struct {
	// Cons is the surviving durable: its name, deliver subject and filter subject.
	Cons *ConsumerConfig `json:"cons"`
	// Subject is the NATS subject the subscription delivered on, kept because
	// ConsumerConfig carries none of the MQTT delivery metadata below.
	Subject string `json:"subject"`
	QoS     byte   `json:"qos"`
	NoLocal bool   `json:"no_local,omitempty"`
	RAP     bool   `json:"rap,omitempty"`
	SubID   int    `json:"sub_id,omitempty"`
	// Started is one entry per stream sequence whose onward delivery had begun.
	Started []mqttPersistedDrainSeq `json:"started,omitempty"`
}

// mqttPersistedDrainSeq is one started delivery.
//
// PubRel/PI are the QoS 2 second phase's INTENT, and they are the linearization
// point of the whole thing: written BEFORE the PUBREL is stored, so recovery is
// positive rather than a guess. Either the record owes a PUBREL — recreate it,
// under the original identifier — or it does not, and the payload may be
// redelivered.
//
// Attempt exists because deduplication cannot be trusted as a completion signal: a
// dedupe entry outlives its message, so recreating under the same id can be
// accepted as a duplicate that stores nothing. A recreate that has VERIFIED its
// record absent bumps Attempt and retries under a fresh identity. It must never
// wrap — an overflow fails closed rather than reusing an old identity.
type mqttPersistedDrainSeq struct {
	Seq     uint64 `json:"seq"`
	PubRel  bool   `json:"pubrel,omitempty"`
	PI      uint16 `json:"pi,omitempty"`
	Attempt uint64 `json:"attempt,omitempty"`
	// RegSeq is the exact sequence of this delivery's outbound in-flight register on
	// $MQTT_inflight, so the drain can RELEASE it when the exchange completes. Without
	// it the register outlives the drain's consumer, and the next resume — finding a
	// register whose consumer is gone — reserves its identifier indefinitely, which under
	// a Receive Maximum of 1 stops the session delivering anything at all.
	//
	// Optional: a record written before this field existed carries none, and the resume
	// barrier repairs it from the register scan rather than treating it as "no register".
	// See inflight_register.go.
	RegSeq uint64 `json:"reg_seq,omitempty"`

	// OwnerNonce is the EXCHANGE identity this drained delivery owns, persisted with the
	// intent and BEFORE the PUBREL is stored.
	//
	// Not redundant with the record's own header: recovery may find the record MISSING and
	// have to recreate it, and minting a replacement nonce there would create a DIFFERENT
	// exchange — one the durable owner slot does not name, so the recreated record could
	// never act. The intent is therefore where the identity has to live, because the intent
	// is what proves the exchange is past PUBREC in the first place.
	//
	// MANDATORY wherever PubRel is set. A marker-less intent cannot have been written by
	// this broker, so it is an unsupported persisted shape and REFUSES THE WHOLE SESSION
	// RECORD, read-only, on F-057's precedent — never per entry, because this field is
	// nested in the authoritative $MQTT_sess record and accepting it per entry would force
	// a later save to reserialize an unsupported shape, normalize it (migration), or refuse
	// the save.
	//
	// owesPubRel() deliberately does NOT test it. A missing nonce must never read as "no
	// PUBREL owed": that would redeliver the payload of an exchange already past PUBREC,
	// which is strictly worse than the refusal.
	OwnerNonce string `json:"owner,omitempty"`
}

// drainSeq is the in-memory state of one drained sequence.
//
// claimed mirrors the persisted Started list EXACTLY, and it plus the intent are
// the only fields the record reflects. A failed save leaves them unchanged, so the
// next redelivery simply retries; nothing "moves between maps". The phase is not
// persisted — the intent is — so recovery derives it from one durable authority
// rather than two.
type drainSeq struct {
	claimed bool
	// unclaimUnconfirmed means claimed was cleared by a write whose outcome is
	// UNKNOWN. It is not a third value of claimed: the record may or may not still
	// hold this sequence, so the removal must not authorize anything IRREVERSIBLE —
	// no JetStream ack, no PUBREL forwarded. Those are exactly the actions that
	// cannot be taken back if the write turns out never to have landed, which is why
	// in-memory "not claimed" is not by itself evidence of durable removal (I1).
	// Cleared by the resolution: proving the write durable discharges the ack, and a
	// definite failure restores the claim inside the writer's own transaction.
	unclaimUnconfirmed bool
	phase              drainPhase
	// intent is the persisted fact that this exchange is past PUBREC and owes a
	// PUBREL under pi. It is tracked separately from pi because pi is also set for
	// a delivery still in the PUBLISH phase — a sequence carrying an identifier is
	// NOT the same as one owing a PUBREL, and conflating them would make recovery
	// recreate a PUBREL for an exchange the client never PUBREC'd.
	intent bool
	// pi is the packet identifier this delivery is using.
	pi uint16
	// attempt is the PUBREL store-attempt generation (see mqttPersistedDrainSeq).
	attempt uint64
	// ack is the latest JetStream ack subject seen for this sequence, so a
	// finalize can ack the delivery that is actually outstanding.
	ack string
	// regAmbiguous means recovery found MORE THAN ONE register naming this one drained
	// delivery, on different packet identifiers. A stream sequence orders the writes and
	// not the sends, so which one the client holds is unknowable: NONE may be released and
	// this delivery may not finalize.
	//
	// CONVERGENCE. The flag is an OBSERVATION, not a latch: it is not persisted, and every
	// complete recovery scan recomputes it from scratch over all claimed entries — clearing
	// it first, so a pair that has since vanished entirely does not leave it standing.
	// Review caught that too: an earlier revision only cleared it for groups the scan still
	// found, so two duplicates disappearing left the drain unfinalizable forever.
	//
	// Convergence is therefore at the NEXT RESUME rather than mid-connection, which is the
	// same contract `piSpaceIncomplete` carries for the same underlying state — that flag
	// is likewise "cleared by the next resume that completes its scans and finds nothing
	// unaccountable". A session that never resumes stays degraded, and it was already
	// degraded: recovery logs that all such registers are reserved, none released, and new
	// outbound QoS 1/2 allocation withheld for the session. What this change stops is the
	// drain FINALIZING over that state, not the state itself.
	//
	// It is a field rather than the absence of regSeq because F-034's review found the two
	// being conflated. The release used to run only when regSeq was nonzero, and recovery
	// zeroes regSeq for exactly this ambiguous case — so the finalize skipped
	// classification and went on to remove the claim and acknowledge the source while both
	// registers survived behind a consumer about to be deleted. That is the indefinite
	// identifier reservation this change exists to prevent, reached through the guard meant
	// to prevent it. regSeq is a hint and may never authorize progress, including by being
	// zero.
	regAmbiguous bool
	// ownerUnusable means this sequence's PERSISTED INTENT carries no usable exchange
	// identity, so nothing may be finalized on it.
	//
	// Separate from regAmbiguous, and a review is the reason. I first reused that flag, but
	// recomputeDrainRegisterStateLocked deliberately RECOMPUTES it from scratch on every
	// complete scan — that is what lets a duplicate-register pair converge once the pair
	// vanishes — so the marker was cleared on the next resume and the quarantine evaporated.
	// This one is derived from the record's own bytes, not from a scan, so nothing
	// recomputes it away.
	ownerUnusable bool
	// regSeq is this delivery's in-flight register (see mqttPersistedDrainSeq.RegSeq).
	// The drain owns it from staging until the exchange completes, and releases it there.
	regSeq uint64
	// ownerNonce is the exchange identity (see mqttPersistedDrainSeq.OwnerNonce). Set with
	// the intent, and what a recreate must write so the record names the same exchange the
	// owner slot does.
	ownerNonce string
}

// mqttDrain is a draining consumer in memory.
type mqttDrain struct {
	cc      *ConsumerConfig
	subject string
	qos     byte
	noLocal bool
	rap     bool
	subID   int

	// staged is true between stageDrain and the packet's confirming save. While
	// staged nothing is torn down and a FRESH delivery is neither tracked nor
	// acked — acking before the save would lose it on rollback, and tracking it
	// would enrol it as started.
	staged bool
	// charged records whether THIS process added this durable's MaxAckPending to
	// tmaxack, so finishDrain refunds exactly what was charged. A restored drain is
	// never charged, matching restored consumers.
	charged bool

	// retired is TERMINAL: this drain has been through retirement, so its sid is
	// released, its delivery path is closed and its consumer has been delete-requested.
	// Nothing may hand this object back to the session.
	//
	// Never cleared. Clearing it once a delete looked slow is the refuted design in which
	// a late delete destroys a consumer that has come back into service.
	//
	// Scope: this drain OBJECT within this resident session object's lifetime. A fork
	// sibling's record can rebuild the drain on a fresh object, which no per-object flag
	// reaches; that residual is stated in docs/CONFORMANCE.md. In-memory only, and it
	// needs no crash survival — after a restart the record names no drain for a retired
	// sid, so there is no object to protect.
	retired bool

	// cancelled names the WITHHELD MARKERS this staging owns, keyed by sequence. A ROLLBACK
	// keeps them, so a later first transmission still carries DUP=0; a COMMIT drops them,
	// because the staging they were protecting is settled and no first transmission of
	// theirs remains to protect. Staging state only, never persisted.
	//
	// "the sequences whose registration this staging cancelled" is what this said, and it
	// is too narrow now: a QoS 0 downgrade's conversion transfers the durable's live marker
	// set in here, which includes a delivery withheld at the in-flight CAP — never
	// transmitted, but never registered either. What the field means is ownership of the
	// marker's retirement, not the reason the marker exists.
	cancelled map[mqttDeferredKey]string
	// withdrawn is every CONFIRMED, never-transmitted entry this staging withdrew from the
	// durable (N-7.7): its register and unsent marker are deleted as never transmitted
	// once the packet's save is confirmed (commitDrains hands them to the caller), and
	// its cancellation verdict is recorded now so a crash in between still tells the next
	// resume the delivery never went out. A rollback un-voids them; the entries themselves
	// are re-allocated by the source's next offer, whose store replaces the standing pair.
	withdrawn []mqttVoidedReg

	// seqs is the ONE map of per-sequence state. There is deliberately no second
	// "finalizing" map to fall out of sync with it.
	seqs map[uint64]*drainSeq
}

// startedSeqs renders the drain's claimed sequences in persisted form.
//
// sess.mu must be held.
func (d *mqttDrain) startedSeqs() []mqttPersistedDrainSeq {
	if len(d.seqs) == 0 {
		return nil
	}
	out := make([]mqttPersistedDrainSeq, 0, len(d.seqs))
	for seq, st := range d.seqs {
		if !st.claimed {
			continue
		}
		out = append(out, mqttPersistedDrainSeq{
			Seq: seq, PubRel: st.intent, PI: st.pi, Attempt: st.attempt, RegSeq: st.regSeq,
			OwnerNonce: st.ownerNonce,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// claimedCount reports how many sequences this drain still claims. A drain with
// none is finished and its durable may be torn down.
//
// sess.mu must be held.
func (d *mqttDrain) claimedCount() int {
	n := 0
	for _, st := range d.seqs {
		if st.claimed {
			n++
		}
	}
	return n
}

// persisted renders the drain for the session record.
//
// sess.mu must be held.
func (d *mqttDrain) persisted() *mqttPersistedDrain {
	return &mqttPersistedDrain{
		Cons:    d.cc,
		Subject: d.subject,
		QoS:     d.qos,
		NoLocal: d.noLocal,
		RAP:     d.rap,
		SubID:   d.subID,
		Started: d.startedSeqs(),
	}
}

// mqttDrainFromPersisted rebuilds a drain from the session record. Restored drains
// start uncharged (matching restored consumers, so finishDrain refunds nothing and
// an adoption cannot double-charge) and every phase starts at drainPublish —
// recovery reconstructs the real phase from the persisted intent plus the stored
// PUBREL records, BEFORE any source interest exists.
// An intent with no usable exchange identity is RESTORED, and marked non-finalizable
// rather than refused, and the reason is worth stating because I got it wrong first.
//
// Such a record cannot have been written by this broker — recordPubRelIntent validates the
// identity before it saves, and jsa.storeInflightCAS validates every register write — so it
// only exists if something else produced it. The documented answer for an unsupported
// persisted shape is a read-only refusal of the WHOLE session record (F-057's shape), and I
// implemented it here by returning nil.
//
// A review caught that this does not refuse anything: restoreDrainsFromRecord passes the nil
// to restoreDrainLocked, whose nil guard merely SKIPS the drain. The session was then accepted
// WITHOUT its past-PUBREC drain, and a later save could erase the drain metadata while its
// consumer and its pending exchange were stranded — strictly worse than restoring it.
//
// A real refusal needs an error propagated through normal restore, expiry, teardown and
// fork-repair. That plumbing is not built, so the honest behaviour is: restore the drain, mark
// the sequence NON-FINALIZABLE so nothing acts on evidence it cannot attribute, and say in
// docs/CONFORMANCE.md that the whole-record refusal is unbuilt rather than claiming it.
func mqttDrainFromPersisted(pd *mqttPersistedDrain) *mqttDrain {
	if pd == nil || pd.Cons == nil {
		return nil
	}
	d := &mqttDrain{
		cc:      pd.Cons,
		subject: pd.Subject,
		qos:     pd.QoS,
		noLocal: pd.NoLocal,
		rap:     pd.RAP,
		subID:   pd.SubID,
		seqs:    make(map[uint64]*drainSeq, len(pd.Started)),
	}
	for _, s := range pd.Started {
		if s.Seq == 0 {
			continue
		}
		// The INTENT is what says this exchange is past PUBREC — never the mere
		// presence of a packet identifier, which a PUBLISH-phase delivery also has.
		//
		// `PubRel` ALONE is that fact, and folding the identifier into it was a defect a
		// review round found. The predicate used to be `PubRel && PI != 0`, so a record
		// explicitly saying "past PUBREC" with a zero identifier read as NOT past PUBREC:
		// neither quarantined nor visible to PUBREL recovery, restored as an ordinary
		// claimed source, and therefore eligible to have its payload redelivered — a
		// duplicate of an exchange the client has already answered. An unsupported shape
		// must never be interpreted as the SAFE-looking one.
		ds := &drainSeq{
			claimed: true, intent: s.owesPubRel(), pi: s.PI, attempt: s.Attempt, regSeq: s.RegSeq,
			ownerNonce: s.OwnerNonce,
		}
		if ds.intent && (s.PI == 0 || !s.ownerNonceUsable()) {
			ds.ownerUnusable = true
			// Past PUBREC with nothing that can attribute it — no identifier, or no
			// exchange identity: nothing may finalize on it. The same flag the
			// duplicate-register case uses, and for the same reason — which one the client
			// holds is unknowable, so NONE may be released and this delivery may not
			// finalize. Convergence is at the next resume once an operator has removed the
			// record.
			ds.regAmbiguous = true
		}
		d.seqs[s.Seq] = ds
	}
	return d
}

// owesPubRel reports whether the record says this sequence's exchange is past
// PUBREC, so its payload must never be resent and its PUBREL must be recreated if
// missing. Derived from the PERSISTED intent, never from the absence of evidence.
//
// `PubRel` is the whole predicate. It deliberately does NOT test the identifier or the
// owner nonce: those say whether the intent can be ACTED ON, which is a different question
// from whether the exchange is past PUBREC, and answering the second with the first is how
// an unsupported record came to be read as an ordinary pre-PUBREC delivery. The callers
// quarantine what they cannot attribute; none of them may un-know the phase.
func (s mqttPersistedDrainSeq) owesPubRel() bool { return s.PubRel }

// ownerNonceUsable reports whether this intent carries the exchange identity a recreate
// needs. DELIBERATELY separate from owesPubRel: a missing identity must never read as "no
// PUBREL owed", because that would redeliver the payload of an exchange already past
// PUBREC. It is asked where the intent is RESTORED, so an unsupported one is refused
// rather than turned into a drain nothing can ever complete.
func (s mqttPersistedDrainSeq) ownerNonceUsable() bool {
	return mqttValidInflightNonce(s.OwnerNonce)
}

// ---------------------------------------------------------------------------
// State transitions
// ---------------------------------------------------------------------------

// stageDrain converts a consumer whose membership this packet is removing — an
// UNSUBSCRIBE, or a re-SUBSCRIBE that downgrades the filter to QoS 0 — into a drain, when
// and only when it still holds started deliveries.
//
// It moves cons[sid] into drains[sid] (I7), registers drainByDur so the delivery
// callback can find it, and drops this durable's withheld (never transmitted)
// deliveries, which are discarded rather than drained (I5). It does NOT mark the
// subscription closed, does NOT purge pendingPublish/cpending, and does NOT delete
// the durable — that is the whole point.
//
// Returns false when there is nothing started, in which case the caller performs
// today's immediate teardown.
//
// sess.mu must be held.
func (sess *mqttSession) stageDrain(sid string, cc *ConsumerConfig, sub *subscription) bool {
	if cc == nil || cc.DeliverGroup != _EMPTY_ {
		return false // shared consumers are out of scope
	}
	seqPis := sess.cpending[cc.Durable]
	if len(seqPis) == 0 {
		return false // nothing was started; ordinary teardown
	}
	// A QUARANTINED drain already under this sid is NOT overwritten, and this closes the
	// second-order bypass a review found in the adoption refusal.
	//
	// Refusing adoption keeps the quarantined drain in sess.drains[sid] but lets a fresh
	// live consumer be provisioned under that same sid. A second UNSUBSCRIBE would then
	// stage a NEW drain into the same map slot, leaving the quarantined one reachable only
	// through drainByDur — and because the record serializes drains by the sid map, the
	// next successful save omitted the marker-less intent entirely. After a restart its
	// source looked like an ordinary delivery and could be retransmitted despite durable
	// evidence it was past PUBREC.
	//
	// So the older, held drain wins the slot and this consumer takes the ordinary
	// immediate teardown instead. That is the same disposition draining-not-allowed already
	// uses, and it keeps one owner per sid.
	if sess.drainSidHoldLocked(sid) != mqttDrainNotHeld {
		return false
	}

	d := &mqttDrain{
		cc:      cc,
		staged:  true,
		charged: sess.consCharged[cc.Durable] > 0,
		seqs:    make(map[uint64]*drainSeq, len(seqPis)),
	}
	if sub != nil && sub.mqtt != nil {
		d.subject = string(sub.subject)
		d.qos = sub.mqtt.qos
		d.noLocal = sub.mqtt.noLocal
		d.rap = sub.mqtt.retainAsPublished
		d.subID = sub.mqtt.subID
	}
	// The started set is exactly the durable's pending deliveries: each is a
	// sequence that was put on the wire and has not been acknowledged.
	//
	// cancelled collects the sequences whose registration was still in flight, with the
	// ack subject each was last offered on, so their withheld markers can be (re)installed
	// AFTER the I5 drop below rather than being deleted by it.
	cancelled := map[mqttDeferredKey]string{}
	for sseq, pi := range seqPis {
		ack := sess.pendingPublish[pi]
		if ack != nil && ack.regPending {
			// Allocated, but its in-flight register is not yet confirmed, so it has NOT
			// been transmitted (R1, inflight_register.go). [MQTT-3.10.4-3] covers started
			// deliveries only — the same rule as the withheld ones below.
			//
			// CANCELLED, not merely skipped. Skipping leaves the entry in place, and the
			// delivery callback still waiting in storeInflightRegister would then confirm
			// its CAS and enqueue the PUBLISH — after this UNSUBSCRIBE has committed and
			// given up this membership's tracking, leaving the client holding an exchange
			// nothing on this side remembers. (The consumer itself may SURVIVE, as a drain
			// keeping its durable; what ends either way is this membership's claim on the
			// delivery.) Removing the entry hands the decision to
			// that callback's own ownership revalidation: confirmInflightRegister finds no
			// entry, deletes the register it just wrote and refuses the send.
			//
			// It is also recorded as WITHHELD, below, which keeps the DUP flag honest if
			// this staging is rolled back: nothing was transmitted, so the eventual first
			// transmission must carry DUP=0 ([MQTT-3.3.1-1]) — but its redelivery arrives
			// with a JetStream delivery count above one, and trackPublish derives dup from
			// that count unless the sequence is known to have been withheld. Exactly the
			// state, and the marker, that a delivery withheld at the in-flight cap has.
			cancelled[mqttDeferredKey{cc.Durable, sseq}] = ack.jsAckSubject
			// The cancellation is recorded DURABLY too, keyed by the allocation's nonce.
			// The handoff above is in-memory only, and this packet is about to persist the
			// removal of this membership — tearing the consumer down, or converting it to a
			// drain — while that store may still be on the wire: if the CAS lands and this
			// broker dies before the confirmation runs, nothing written anywhere says the
			// delivery was never transmitted. Being inside this packet's own record
			// transaction is what makes it durable before that removal lands.
			// See cancelRegPendingLocked.
			sess.cancelRegPendingLocked(ack)
			sess.untrackPublishEntry(pi)
			continue
		}
		if ack != nil && sess.provenUnsentReleasableLocked(ack) {
			// Confirmed, and PROVEN never transmitted — the pair still holds its marker, no
			// intent is unresolved: the PUBLISH never reached the client, so
			// [MQTT-3.10.4-3] does not cover it either. A possibly-attempted entry (marker
			// gone, or its intent delete ambiguous) is ENROLLED instead, like a transmitted
			// one: it may have reached the client, who may hold the identifier. Enrolling it
			// would make the drain redeliver, as a "started" delivery with DUP=1, a payload
			// the client has never seen. It is WITHDRAWN instead (N-7.7): untracked here,
			// its NAK marker kept exactly as a cancelled registration's is, its verdict
			// recorded now, and its register and marker deleted as never transmitted once
			// this packet's save is confirmed. An entry whose section is still writing is
			// enrolled: the write may reach the client, and endPublishSend will say so.
			cancelled[mqttDeferredKey{cc.Durable, sseq}] = ack.jsAckSubject
			sess.voidRegNonceLocked(ack.regNonce)
			d.withdrawn = append(d.withdrawn, mqttVoidedReg{
				seq: ack.regSeq, nonce: ack.regNonce, pi: pi, dur: cc.Durable, sseq: sseq,
			})
			sess.untrackPublishEntry(pi)
			continue
		}
		d.seqs[sseq] = &drainSeq{claimed: true, pi: pi}
		if ack != nil {
			d.seqs[sseq].ack = ack.jsAckSubject
			// The register of a transmitted delivery, so the drain can release it when
			// the exchange completes (finding 1 below).
			d.seqs[sseq].regSeq = ack.regSeq
		}
	}
	// Withheld deliveries were never transmitted, so [MQTT-3.10.4-3] does not
	// cover them: drop them rather than enrolling them as started (I5).
	for k := range sess.deferred {
		if k.jsDur == cc.Durable {
			delete(sess.deferred, k)
		}
	}
	// The cancelled registrations go in AFTER that drop, which is why they were collected
	// rather than recorded in place. They are withheld in exactly the I5 sense — never
	// transmitted — and the marker is what makes their eventual first transmission carry
	// DUP=0 if this staging is rolled back.
	//
	// A CALLER may own further live markers this drop also removed and this loop cannot
	// rediscover — one cancelled before the call, or one for a delivery withheld at the
	// in-flight cap, which never had a registration. Re-installing those is the caller's,
	// and convertDowngradeCandidatesLocked does it by snapshotting the durable's live set
	// around this whole call. A COMMITTED teardown drops them again, in
	// commitDrains for a staged drain and in purgeConsumerPendingLocked otherwise, because
	// the staging is settled and the marker has no first transmission left to protect.
	//
	// BEFORE the "nothing staged" return below, not after: when every entry was cancelled
	// there is no drain to stage, but the DUP rule is unchanged.
	for k, ackSubject := range cancelled {
		if ackSubject == _EMPTY_ {
			continue // nothing to NAK, and an empty subject must never reach sendMsg
		}
		if sess.deferred == nil {
			sess.deferred = make(map[mqttDeferredKey]string)
		}
		sess.deferred[k] = ackSubject
	}
	if len(d.seqs) == 0 {
		// Nothing provably started; ordinary teardown. The withdrawn entries' verdicts
		// stand — the ordinary teardown's own void pass finds them already voided.
		return false
	}
	d.cancelled = cancelled

	if sess.drains == nil {
		sess.drains = make(map[string]*mqttDrain)
	}
	if sess.drainByDur == nil {
		sess.drainByDur = make(map[string]*mqttDrain)
	}
	sess.drains[sid] = d
	sess.drainByDur[cc.Durable] = d
	delete(sess.cons, sid)
	return true
}

// commitDrains marks every staged drain committed, after the packet's save has
// been CONFIRMED. From here a fresh delivery on the filter is discarded (acked)
// rather than left to AckWait.
//
// WHOLE-SESSION, which is a precondition on the caller rather than a claim about scope:
// the session is serialized by its locked-map entry and every stager — an UNSUBSCRIBE, or
// a SUBSCRIBE's QoS 0 conversion — commits or rolls back inside its own packet, so the
// staged set is always exactly the calling packet's.
//
// It returns the drains whose durable still carries a per-consumer
// InactiveThreshold, for clearDrainInactivityTimers to deal with outside the lock.
//
// sess.mu must be held.
func (sess *mqttSession) commitDrains() (timed []*mqttDrain, withdrawn []mqttVoidedReg) {
	for _, d := range sess.drains {
		if !d.staged {
			continue
		}
		d.staged = false
		// The withdrawn never-transmitted pairs are the caller's to delete now that the
		// verdicts naming them are durable in this packet's record (N-7.7).
		withdrawn = append(withdrawn, d.withdrawn...)
		d.withdrawn = nil
		// The teardown is committed, so the staging these markers protect is settled and
		// they have no first transmission left to protect; they would otherwise linger. Only
		// a rollback keeps them.
		//
		// Deliberately NOT "the consumer is gone and nothing will re-offer them": this drain
		// KEEPS its consumer, which is the whole point of draining, and an unacknowledged
		// withheld sequence can still be re-offered — the delivery callback's unclaimed arm
		// acknowledges it there. What ends is the marker's purpose, not the re-offer.
		for k := range d.cancelled {
			delete(sess.deferred, k)
		}
		d.cancelled = nil
		if d.cc != nil && d.cc.InactiveThreshold != 0 {
			timed = append(timed, d)
		}
	}
	return timed, withdrawn
}

// clearDrainInactivityTimers removes the per-consumer InactiveThreshold from a
// drained durable.
//
// A consumer created under Options.MQTT.ConsumerInactiveThreshold carries its own
// deletion timer. That is right for an ordinary subscription — an abandoned one
// should not hold interest forever — but fatal for a drain, which exists precisely
// to outlive a disconnect: the timer would reap the durable while the session is
// offline and take its started deliveries with it. Only an updatable field changes,
// so re-issuing the create is an update.
//
// Best-effort by design: a failure is logged, the drain stays valid, and the
// residual is the documented one — the durable can be reaped while offline, which
// recoverDrains reports as LOSS, never as completion. Retried at the next resume.
//
// No lock held on entry.
func (sess *mqttSession) clearDrainInactivityTimers(c *client, drains []*mqttDrain) {
	for _, d := range drains {
		sess.mu.Lock()
		if d.cc == nil || d.cc.InactiveThreshold == 0 {
			sess.mu.Unlock()
			continue
		}
		// A retired drain's durable has been delete-requested, so recreating a consumer
		// under its name would be issuing a create for something this object no longer
		// uses. Re-checked here, immediately before the create, because the loop releases
		// sess.mu between items.
		if d.retired {
			sess.mu.Unlock()
			continue
		}
		cfg := *d.cc
		claimed := d.claimedCount()
		sess.mu.Unlock()

		cfg.InactiveThreshold = 0
		if _, err := sess.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName, Config: cfg}); err != nil {
			c.Errorf("MQTT: unable to clear the inactivity timer of drained consumer %q; it may be reaped while the session is offline, losing %d started delivery(ies): %v",
				cfg.Durable, claimed, err)
			continue
		}
		sess.mu.Lock()
		if d.cc != nil {
			d.cc.InactiveThreshold = 0
		}
		sess.mu.Unlock()
	}
}

// rollbackStagedDrains undoes staging when the packet's save failed: the drains
// vanish and their consumers go back into cons. Nothing was torn down, so the
// subscription and its deliveries simply continue.
//
// The SUBSCRIBE path uses unconvertDowngradeDrainsLocked instead, which does the same
// per named drain: there the un-convert must run before rollbackSubscribePacket
// enumerates sess.cons, and naming the drains makes that disposition provable per drain.
//
// sess.mu must be held.
func (sess *mqttSession) rollbackStagedDrains() {
	for sid, d := range sess.drains {
		if !d.staged {
			continue
		}
		// Nothing was persisted, so the withdrawn entries' verdicts go back too; their
		// pairs still stand and the source's next offer takes them over at its store.
		if len(d.withdrawn) > 0 {
			nonces := make([]string, 0, len(d.withdrawn))
			for _, w := range d.withdrawn {
				nonces = append(nonces, w.nonce)
			}
			sess.unvoidRegNoncesLocked(nonces)
			d.withdrawn = nil
		}
		delete(sess.drains, sid)
		delete(sess.drainByDur, d.cc.Durable)
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[sid] = d.cc
	}
	if len(sess.drains) == 0 {
		sess.drains = nil
	}
	if len(sess.drainByDur) == 0 {
		sess.drainByDur = nil
	}
}

// drainSidForDurable finds the sid a drained durable is filed under.
//
// sess.mu must be held.
func (sess *mqttSession) drainSidForDurable(durable string) (string, bool) {
	for sid, d := range sess.drains {
		if d.cc != nil && d.cc.Durable == durable {
			return sid, true
		}
	}
	return _EMPTY_, false
}

// dropDrainLocked removes a drain from both maps and returns its sid.
//
// sess.mu must be held.
func (sess *mqttSession) dropDrainLocked(d *mqttDrain) string {
	sid, _ := sess.drainSidForDurable(d.cc.Durable)
	if sid != _EMPTY_ {
		delete(sess.drains, sid)
	}
	delete(sess.drainByDur, d.cc.Durable)
	if len(sess.drains) == 0 {
		sess.drains = nil
	}
	if len(sess.drainByDur) == 0 {
		sess.drainByDur = nil
	}
	return sid
}

// restoreDrainLocked files a drain under a sid, in BOTH maps.
//
// It is the only two-map insertion in the package, and that is deliberate: drains alone is
// not enough — the delivery callback finds a drain through drainByDur, drain recovery
// queries that key, and a drain missing from it looks absent to everything except a
// teardown. Three callers: undoing dropDrainLocked when the save that would have made the
// drop durable failed, restoring the record's drains on a resume, and re-asserting a drain a
// merge accepted from a recognised fork's candidate delta (sess_fork_repair.go).
//
// sess.mu must be held, or the session must not yet be reachable.
func (sess *mqttSession) restoreDrainLocked(sid string, d *mqttDrain) {
	if sid == _EMPTY_ || d == nil || d.cc == nil {
		return
	}
	// Terminal retirement, refused at the door. DEFENSIVE, and labelled so rather than
	// presented as a live route: no production schedule reaches here with a retired object
	// today — the fork-repair and record-restore callers all mint fresh objects through
	// mqttDrainFromPersisted, and undoPlans restores only when p.sid is non-empty, which
	// dropDrainLocked returns empty for an already-retired object. The guard is here
	// because this is the package's only re-insertion of an EXISTING drain object into both
	// maps, and a terminal invariant belongs at that one door. (stageDrain writes both maps
	// too, but for a freshly staged drain built from a live consumer, which can never be a
	// retired object.)
	if d.retired {
		return
	}
	if sess.drains == nil {
		sess.drains = make(map[string]*mqttDrain)
	}
	if sess.drainByDur == nil {
		sess.drainByDur = make(map[string]*mqttDrain)
	}
	sess.drains[sid] = d
	sess.drainByDur[d.cc.Durable] = d
}

// ---------------------------------------------------------------------------
// Completion (I1: persist before ack)
// ---------------------------------------------------------------------------

// finalizeDrainDelivery is the SINGLE ack gate for a claimed sequence: it clears
// the claim, persists that, and only then acks JetStream. There is no bare
// sendAck on a claimed sequence anywhere.
//
// handled=false means this durable is not draining, so the caller acks normally.
//
// A refused fence or a failed save restores the claim and acks NOTHING — the
// message stays pending and is redelivered, which is the whole point: the ack must
// never outrun the record. The off-readLoop fence form is used deliberately, so a
// finalizer can neither block the readLoop nor observe a half-applied transaction.
//
// No lock held on entry.
func (sess *mqttSession) finalizeDrainDelivery(c *client, jsDur string, sseq uint64, ackSubject string) (handled bool) {
	sess.mu.Lock()
	d := sess.drainByDur[jsDur]
	if d == nil {
		sess.mu.Unlock()
		return false
	}
	st := d.seqs[sseq]
	if st == nil || !st.claimed {
		// Already finalized — but only ack if that removal is DURABLE. An
		// unconfirmed one may not be in the record at all, and acking would then
		// destroy a message nothing claims (I1). Leave it pending instead; the
		// readback resolves the sequence and AckWait re-offers it.
		unconfirmed := st != nil && st.unclaimUnconfirmed
		sess.mu.Unlock()
		if unconfirmed {
			// This delivery IS a retry clock, and for a QoS 2 exchange it may be the
			// ONLY one: the source consumer's interest can be withheld (the barrier
			// adds an unresolved durable to its skip set), leaving PUBREL redeliveries
			// as the sole recurring event on the session. Retry the readback here or
			// such a drain never resolves.
			sess.resolveUncertainNow(c)
			return true
		}
		if ackSubject != _EMPTY_ {
			sess.jsa.sendAck(ackSubject)
		}
		return true
	}
	if ackSubject != _EMPTY_ {
		st.ack = ackSubject
	}
	// regSeq is deliberately NOT read here. It is a hint, and the release addresses the
	// register by the subject the packet identifier names; reading it into a local was how
	// an earlier revision came to gate the release on it.
	ack := st.ack
	sess.mu.Unlock()

	// Release this delivery's in-flight register FIRST, and confirm it. The drain owns
	// that register from staging (stageDrain) until here, because the ordinary
	// terminal-path delete cannot run for a drained QoS 2 source: its claim is held until
	// the PUBREL is proven, so nothing else reaches this point. Left behind, the register
	// outlives the drain consumer that finishDrainIfEmpty deletes below, and the next
	// resume — finding a register whose consumer is gone, which is deliberately NOT read
	// as "complete" — reserves its identifier for the life of the session. Under a Receive
	// Maximum of 1 that stops the session receiving anything at all.
	//
	// Before the claim removal, the source ack and the PUBREL forward, for the reason every
	// ordering here has: an unconfirmed delete followed by a completed exchange is exactly
	// the state that leaves a phantom behind. A failure withholds the whole finalize, so
	// the next redelivery retries it — nothing is acked and nothing is lost.
	// Addressed by SUBJECT, not by the recorded sequence (F-034).
	//
	// `regSeq` used to address this delete directly, and that was the defect: the store
	// path's replacement branch rewrites a register for the same (Dur, Seq) at a NEW
	// sequence, and after $MQTT_inflight is recreated the recorded sequence names an
	// unrelated register belonging to another session. Deleting there destroyed it —
	// which F-066's table had already recorded, assigning the consequence to "that
	// mechanism's owner", i.e. here.
	//
	// The subject is derived from this delivery's own packet identifier, which the record
	// persists for every started entry, so this needs no new persisted field. `regSeq`
	// survives as a HINT only: it may never select or authorize a deletion.
	//
	// A MISMATCH is not a failure. It means the occupant belongs to another exchange —
	// commonly a later one on the same identifier — so it is preserved and this drain has
	// nothing to release. Only an unreadable or indeterminate occupant withholds the
	// finalize, on the same reasoning every ordering here has: an unconfirmed disposal
	// followed by a completed exchange is what leaves a phantom behind.
	// The register release is NOT keyed on regSeq. F-034's review found that guard was
	// itself the defect: recovery zeroes regSeq for the AMBIGUOUS duplicate-register case,
	// so `regSeq != 0` skipped classification entirely and the finalize went on to remove
	// the claim and acknowledge the source while both registers survived behind a consumer
	// about to be deleted — the indefinite reservation this change exists to prevent,
	// reached through the guard meant to prevent it. regSeq is a hint and may never
	// authorize progress, including by being zero.
	// Captured inside the release block below and re-checked after the fence, so they are
	// declared here. capturedIdentity is false only when there is no account manager and
	// hence no register to release; the drain-currency half of the second check still runs.
	var capIDHash, capGen string
	capturedIdentity := false
	if sess.asm != nil {
		// The one seam in the pre-release window; see testDrainBeforeRegisterRelease.
		sess.asm.mu.RLock()
		beforeRelease := sess.asm.testDrainBeforeRegisterRelease
		sess.asm.mu.RUnlock()
		if beforeRelease != nil {
			beforeRelease()
		}
		// The incarnation identity is captured in the SAME critical section that proves
		// this drain is still the session's, and holistic review round 23 found the
		// defect that makes both halves necessary. This function captures d and st at
		// its head, unlocks, and does JetStream work; a concurrent clear() nils
		// drainByDur, RE-MINTS the generation, and resets last_pi to 0 so a reused
		// object reallocates the same low identifiers. Reading sess.gen here without
		// re-proving the drain therefore pairs the OLD delivery's packet identifier with
		// the NEW generation — and releaseInflightRegisterByIdentity matches on
		// (subject, durable, source sequence), where shared group durables are named
		// deterministically. The fresh incarnation's register is then a full match and
		// gets deleted while its client may still hold that identifier. The record fence
		// below cannot prevent it: the release happens first.
		sess.mu.Lock()
		current := sess.drainByDur[jsDur] == d && d.seqs[sseq] == st
		idHash, gen, pi, ambiguous := sess.idHash, sess.gen, uint16(0), false
		pastPubRec := false
		if cur := d.seqs[sseq]; cur != nil {
			pi, ambiguous = cur.pi, cur.regAmbiguous || cur.ownerUnusable
			// A recorded intent is the durable fact that this exchange is past PUBREC and
			// owes a PUBREL, so its in-flight register is now the owner SLOT and owns the
			// exchange for its whole life (§10.5). Releasing it here — as this path did
			// before — leaves the recreated PUBREL naming a slot admission can no longer
			// find, which fail-closes forever: the exact stranding the two ported drain
			// tests reproduce. The slot is released once, at PUBCOMP terminal cleanup,
			// addressed through the identifier the record persists.
			pastPubRec = cur.intent
		}
		sess.mu.Unlock()
		capIDHash, capGen, capturedIdentity = idHash, gen, true
		if !current {
			// This object was cleared and possibly reused while we were off-lock, so the
			// captured drain is a retired incarnation's. Same disposition as finding no
			// drain at all on entry: nothing here is ours to finalize.
			return false
		}
		switch {
		case ambiguous:
			// Two or more registers name this one delivery on different identifiers, and
			// a stream sequence orders the writes rather than the sends, so which one the
			// client holds is unknowable. Releasing either could free an identifier the
			// client is holding; completing without releasing either strands both. So
			// neither: this delivery does not finalize.
			mqttWarnf(c, "MQTT: withholding the completion of drained sequence %d for client %q: "+
				"more than one in-flight register names it, so none may be released", sseq, sess.id)
			return true
		case pi == 0:
			// No identifier, so no register was ever written (R6: a retained send has no
			// JetStream backing at all). Nothing to release, and nothing to withhold for.
		case pastPubRec:
			// §10.5: a past-PUBREC exchange's register is the owner slot and is NOT
			// released here. The phase gate independently refuses its deletion in every
			// past-PUBREC state, so this is enforced, not merely chosen. The claim/intent
			// removal below still runs (I10), the source is acknowledged, and the PUBREL
			// forwards; PUBCOMP terminal cleanup performs the one release.
		case idHash == _EMPTY_ || gen == _EMPTY_:
			// A real session always carries both. Withhold rather than fall back to
			// addressing by a remembered sequence: falling back IS the defect.
			mqttWarnf(c, "MQTT: withholding the completion of drained sequence %d for client %q: "+
				"its in-flight register cannot be named (idHash=%q gen=%q)", sseq, sess.id, idHash, gen)
			return true
		default:
			// Addressed by the identifier, never by regSeq. Pinned on the production path
			// by TestFinalizeDrainDeliveryAddressesByPacketIdentifier.
			subject := mqttInflightSubject(idHash, gen, pi)
			switch sess.asm.releaseInflightRegisterByIdentity(c, sess, subject, d.cc.Durable, sseq) {
			case mqttInflightUndecided:
				mqttWarnf(c, "MQTT: withholding the completion of drained sequence %d for client %q: "+
					"its in-flight register on %q could not be resolved", sseq, sess.id, subject)
				return true
			case mqttInflightReleased, mqttInflightAlreadyGone:
				// Released, or provably not ours. Either way this drain owes nothing
				// further on that register, so the progress below is authorized.
			}
			sess.mu.Lock()
			if cur := d.seqs[sseq]; cur != nil {
				cur.regSeq = 0 // resolved; nothing may address a remembered sequence again
			}
			sess.mu.Unlock()
		}
	}

	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		// Fence unavailable — held, or free but reserved for an announced blocking writer
		// (see sess_recordtx.go): stays claimed and unacked, retried on redelivery.
		return true
	}
	// Revalidated AFTER the release and AFTER the fence, which is a second window and not
	// a repetition of the first: clear() can finish during the register round trips above,
	// and this tryBeginRecordTx then opens legitimately against the NEW record epoch while
	// every local below still refers to the RETIRED incarnation's st. Mutating, saving or
	// acking on that would carry a retired delivery's decisions into a fresh session's
	// record. The token is released and the delivery withheld rather than undone, because
	// the register release above is already irreversible.
	sess.mu.Lock()
	stillOurs := sess.drainByDur[jsDur] == d && d.seqs[sseq] == st && st.claimed
	if stillOurs && capturedIdentity {
		stillOurs = sess.idHash == capIDHash && sess.gen == capGen
	}
	sess.mu.Unlock()
	if !stillOurs {
		if terr := sess.endRecordTx(tok); terr != nil && c != nil {
			c.Errorf("MQTT: %v", terr)
		}
		mqttWarnf(c, "MQTT: withholding the completion of drained sequence %d for client %q: "+
			"the session was cleared while its in-flight register was being released", sseq, sess.id)
		return true
	}
	// The guard goes on WITH the mutation, not after the save. A delivery or a
	// duplicate PUBREL callback can run at any point during the save, and would
	// otherwise observe claimed=false with no guard set — reading a removal whose
	// outcome is not yet known as durable, and acking or forwarding on it.
	sess.mu.Lock()
	st.claimed = false
	st.unclaimUnconfirmed = true
	sess.mu.Unlock()

	outcome, err := sess.saveClassified(tok)
	switch outcome {
	case saveLanded:
		// Proven durable: the guard comes off and the ack below is authorized.
		sess.mu.Lock()
		st.unclaimUnconfirmed = false
		sess.mu.Unlock()
	case saveDefinitelyFailed:
		// Nothing was stored: put the claim back — INSIDE the transaction, so no
		// other writer can persist the removal this one is undoing — and ack
		// nothing. The message stays pending and is redelivered.
		sess.mu.Lock()
		st.claimed = true
		st.unclaimUnconfirmed = false
		sess.mu.Unlock()
	case saveMaybeLanded:
		// The removal MAY be durable, and the guard is already set. What the resolver
		// owes is the half that was withheld for want of PROOF: clear the guard and
		// perform the ack and the teardown. Without it a removal the resolver makes
		// durable would stay permanently unacked and its drain never torn down.
		sess.retainOnLanded(func(recordTx) {
			sess.mu.Lock()
			st.unclaimUnconfirmed = false
			ackNow := st.ack
			sess.mu.Unlock()
			if ackNow != _EMPTY_ {
				sess.jsa.sendAck(ackNow)
			}
			sess.finishDrainIfEmptyInTx(c, d)
		})
	}
	if terr := sess.endRecordTx(tok); terr != nil && c != nil {
		c.Errorf("MQTT: %v", terr)
	}
	if err != nil {
		// Nothing is acked on either failure: the ack must never outrun a removal
		// that is not known to be durable (I1).
		if c != nil {
			c.Debugf("MQTT: unable to persist the completion of drained sequence %v: %v", sseq, err)
		}
		if outcome == saveMaybeLanded {
			// Resolve it HERE rather than waiting for the periodic pass: an unresolved
			// sequence is neither restored nor acked, and every further save on the
			// session is refused meanwhile, so the sooner it settles the shorter that
			// window is. Redeliveries retry if this attempt is refused.
			sess.resolveUncertainNow(c)
		}
		return true
	}

	if ack != _EMPTY_ {
		sess.jsa.sendAck(ack)
	}
	// The last claim gone means the durable is unreferenced and may be torn down.
	sess.finishDrainIfEmpty(c, d)
	return true
}

// retireDrainLocked is retirement's whole no-request half: it releases the sid AND closes
// the delivery path, both under ONE sess.mu hold, and marks the object terminally retired.
//
// Once this returns ok, this drain object can never again deliver to this session and can
// never be handed back to it — so a delete issued arbitrarily late by this session object
// can only name something that object no longer uses. That is what lets the disposal WAIT
// for an answer (disposeRetiredDrainConsumer) instead of firing and forgetting.
//
// It issues no request, so it is safe to call with a record transaction open.
//
// sess.mu must be held.
func (sess *mqttSession) retireDrainLocked(c *client, d *mqttDrain) (durable string, ok bool) {
	// DURABLY empty. claimedCount() alone was the test, and it is not enough: it counts
	// st.claimed only, so a sequence whose removal is still unconfirmed read as gone and
	// retirement tore the durable down on a write whose outcome was unknown.
	if !d.retirable() {
		return _EMPTY_, false
	}
	// Ownership is proven through the SID, not through drainByDur, and that is F-094's
	// repair rather than a simplification. restoreDrains deletes the drainByDur entry when a
	// delivery subscription fails to restore while sess.drains[sid] survives, so a
	// drainByDur-keyed guard could never retire such a drain — and with the hold widened to
	// every past-PUBREC intent, a drain nothing can retire is a sid refused for the life of
	// the session.
	//
	// It is REPLACED, not removed: the guard exists to stop a stale call acting on a
	// replacement, and scanning by durable and then requiring pointer equality still rejects
	// both a replacement under a different durable and a different object under the same
	// one. The lookup, the equality check and the removal all happen under this single
	// acquisition, which is what makes that true.
	sid, found := sess.drainSidForDurable(d.cc.Durable)
	if !found || sess.drains[sid] != d {
		return _EMPTY_, false // already torn down, or replaced by an adoption
	}
	d.retired = true

	// The delivery barrier goes on WITH the map drop, never after it, and that ordering is
	// the whole of the repair. A callback dispatched before retirement re-checks
	// sub.mqtt.closed and only THEN looks the drain up, both under one acquisition of this
	// same lock (mqttDeliverMsgCbQoS12). Setting closed here means no interleaving can
	// observe closed==false together with an absent drain, which is the state that let such
	// a callback fall through to ordinary PUBLISH processing and retransmit a past-PUBREC
	// source under a fresh identifier. processUnsub alone does not reach it: it removes the
	// subscription from c.subs while the *subscription the callback already holds stays
	// valid, which is why execDeferredConsumerDeleteClassified and
	// closeSharedGroupTargetDeliveryLocked both set this flag IN ADDITION to unsubscribing.
	//
	// Lock order sess.mu -> subsMu -> c.mu, as restoreDrains and reclaimDrainDeliveryLocked
	// both already establish by calling processQOS12Sub with sess.mu held.
	if c != nil && d.cc.DeliverSubject != _EMPTY_ {
		sess.subsMu.Lock()
		c.mu.Lock()
		sub := c.subs[d.cc.DeliverSubject]
		c.mu.Unlock()
		if sub != nil && sub.mqtt != nil {
			sub.mqtt.closed = true
		}
		sess.subsMu.Unlock()
	}

	sess.dropDrainLocked(d)
	return d.cc.Durable, true
}

// disposeRetiredDrainConsumer is the ONLY delete of a drained durable. The drain is out of
// both maps, out of this session's record and terminally retired, so the request can name
// nothing this session object still uses — which is what lets it WAIT for an answer rather
// than fire and forget.
//
// Three outcomes, two of them definite, the same classification
// deleteEphemeralConsumerDefinite uses: deleted, confirmed already absent, or UNKNOWN. The
// unknown one is reported and left to the orphaned-consumer sweep; there is deliberately no
// retry loop, no worker and no timer.
//
// The calling goroutine must NOT be holding a record transaction: this blocks for up to
// jsa.timeout and no save on the session can proceed while the fence is open. Note the
// requirement is that the DISPOSING goroutine does not retain a transaction across the
// request, not that no transaction is open anywhere — another writer may open one
// immediately after any unlock, which neither can be nor needs to be excluded.
//
// No lock held on entry.
func (sess *mqttSession) disposeRetiredDrainConsumer(c *client, durable string) {
	noWait := false
	if h := mqttTestDrainDeleteHookV.Load(); h != nil {
		send, observed, override := (*h)(durable, noWait)
		if override {
			sess.classifyRetiredDrainDisposal(c, durable, observed)
			return
		}
		if !send {
			return
		}
	}
	_, err := sess.jsa.deleteConsumer(mqttStreamName, durable, noWait)
	sess.classifyRetiredDrainDisposal(c, durable, err)
}

// classifyRetiredDrainDisposal turns one delete outcome into one of the three dispositions.
// Separate from the request only so the test seam can supply an observed error without
// duplicating the classification it exists to measure.
func (sess *mqttSession) classifyRetiredDrainDisposal(c *client, durable string, err error) {
	switch {
	case err == nil: // deleted
	case !isErrorOtherThan(err, JSConsumerNotFoundErr): // confirmed already absent
	default:
		// Indeterminate, and this is the one outcome with a named reaper rather than a
		// verdict. The durable is referenced by no resident object and by no record of
		// ours, so sweepOrphanedConsumersBefore reclaims it — eventually, on a pass that
		// completes and whose own delete succeeds, and never while a fork sibling's record
		// still names it. docs/CONFORMANCE.md states both qualifications.
		//
		// It is REPORTED on both routes, and the fallback is why: the queued route runs
		// from endRecordTxLocked with no bound client, so mqttErrorf would swallow exactly
		// the outcome the operator needs in order to know a durable may have survived. The
		// JS API client is this codebase's client-less logging route (see the shared
		// last-holder scan and the forked-record repair).
		msg := "MQTT: the disposal of retired drain consumer %q is unconfirmed (%v); " +
			"the orphaned-consumer sweep reclaims it once no record references it"
		switch {
		case c != nil:
			c.Errorf(msg, durable, err)
		case sess.jsa != nil && sess.jsa.c != nil:
			sess.jsa.c.Errorf(msg, durable, err)
		}
	}
}

// finishDrainIfEmpty tears a drain down once it claims nothing: the record entry
// is already gone — buildPersistedSessionLocked omits a drain that claims nothing,
// so the very save that cleared the last claim also removed the entry — which is
// what makes the durable unreferenced before it is deleted (I3).
//
// Deleting the durable also discards any still-unacked copy of an
// already-finalized sequence. That is intended: those exchanges are complete, or
// are completing through their own stored PUBREL.
//
// Retirement is now two halves. retireDrainLocked releases the sid and closes the delivery
// barrier together under one sess.mu hold, so nothing that was already dispatched can
// deliver afterwards; then the consumer delete is CLASSIFIED rather than assumed. This form
// disposes INLINE, so it is only for callers holding no record transaction — the other three
// go through finishDrainIfEmptyInTx.
//
// No lock held on entry, and no record transaction held by this goroutine.
func (sess *mqttSession) finishDrainIfEmpty(c *client, d *mqttDrain) {
	if durable, ok := sess.retireDrain(c, d); ok {
		sess.disposeRetiredDrainConsumer(c, durable)
	}
}

// finishDrainIfEmptyInTx is finishDrainIfEmpty for the retained continuations, which run
// under the uncertain-write resolver's OPEN record transaction. They may not block on a
// JetStream request while holding the fence, so the disposal is queued and drained at the
// transaction's release boundary in endRecordTxLocked. The split is explicit HERE, at the
// call site, rather than inferred from sess.recordTxOpen — a caller knows whether it owns a
// transaction, and a flag read cannot distinguish "mine" from "somebody else's".
//
// No lock held on entry.
func (sess *mqttSession) finishDrainIfEmptyInTx(c *client, d *mqttDrain) {
	durable, ok := sess.retireDrain(c, d)
	if !ok {
		return
	}
	sess.mu.Lock()
	sess.pendingDrainDisposals = append(sess.pendingDrainDisposals, durable)
	sess.mu.Unlock()
}

// retireDrain is the locking and refund wrapper both forms share: the critical section, the
// charge refund, then the unsubscribe. It issues no JetStream request, so it is safe under
// an open transaction.
//
// No lock held on entry.
func (sess *mqttSession) retireDrain(c *client, d *mqttDrain) (durable string, ok bool) {
	sess.mu.Lock()
	durable, ok = sess.retireDrainLocked(c, d)
	if !ok {
		sess.mu.Unlock()
		return _EMPTY_, false
	}
	if d.charged {
		sess.refundConsumerChargeLocked(durable)
	}
	deliverSubject := d.cc.DeliverSubject
	sess.mu.Unlock()

	// Test seam, fired with sess.mu RELEASED and the barrier and map drop already both
	// applied: it is what lets a test hold retirement here and release a paused delivery
	// callback, so "the barrier is atomic with the map drop" is an assertion rather than a
	// race against the scheduler. It belongs in this caller, not in retireDrainLocked,
	// which is called with the lock already held and neither acquires nor releases it.
	if h := mqttTestDrainRetirementUnlockedHookV.Load(); h != nil {
		(*h)(durable)
	}

	if c != nil && deliverSubject != _EMPTY_ {
		if err := c.processUnsub([]byte(deliverSubject)); err != nil {
			c.Errorf("MQTT: error unsubscribing from a finished drain on %q: %v", deliverSubject, err)
		}
	}
	return durable, true
}

// finalizeDrainSource is the convenience form of finalizeDrainDelivery for the
// terminal paths that already hold the delivery's pending entry: PUBACK, a FAILED
// PUBREC (the receiver rejected the message, so the exchange ends there), the
// Message Expiry discard and the Maximum Packet Size discard.
//
// Returns false when the delivery is not from a draining consumer, so the caller
// acks normally.
//
// A SUCCESSFUL PUBREC deliberately does not come here: for QoS 2 the source claim
// may only be removed once its PUBREL is CONFIRMED, which is the delivery
// callback's job (I2).
//
// No lock held on entry.
func (sess *mqttSession) finalizeDrainSource(c *client, removed *mqttPending) bool {
	if removed == nil || removed.jsDur == _EMPTY_ {
		return false
	}
	return sess.finalizeDrainDelivery(c, removed.jsDur, removed.sseq, removed.jsAckSubject)
}

// qos2DrainDurables lists the draining durables that may owe a PUBREL. It is both
// the test for "does this resume need the PUBREL path" and, when establishing that
// path fails, exactly the set whose source interest must be withheld: without
// PUBREL delivery a QoS 2 drain cannot complete an exchange, and must not be given
// the chance to start resending one.
//
// No lock held on entry.
// drainOwesPubRelLocked reports whether this drain owes PUBREL handling: recreating a
// PUBREL, establishing PUBREL delivery on resume, or being withheld when that fails.
//
// The drain's own qos is a SUBSCRIPTION fact and cannot answer the question on its own.
// A re-SUBSCRIBE can lower it below the QoS of exchanges already started under it --
// stageDrain copies whatever sub.mqtt.qos says at staging time -- so a QoS 1 drain
// routinely holds a QoS 2 exchange. Gating on d.qos alone is what stopped a QoS 2
// exchange past PUBREC inside such a drain from ever having its PUBREL recreated: loss,
// not a wrong header byte.
//
// Three terms, none redundant:
//
//   - d.qos >= 2 covers the ordinary case and needs no per-sequence evidence.
//   - a persisted intent proves an ACCEPTED PUBREC obligation, and only a QoS 2 exchange
//     can be past PUBREC. It is sound conservative evidence for ENABLING the PUBREL path,
//     not a substitute for the register's QoS -- recordPubRelIntent validates identity
//     rather than QoS.
//   - a recovered PUBLISH entry whose register says QoS 2 covers the exchange that is
//     transmitted but NOT yet past PUBREC, which carries no intent. Without it, a resume
//     that fails to establish PUBREL delivery would not withhold such a drain, and source
//     delivery would be restored with no path for the PUBREL the client is about to owe.
//     Sourceable here because register recovery runs, and aborts CONNECT on failure,
//     before any of this runs.
//
// The third term is inert for recreation specifically -- recoverDrainPubRels re-tests
// st.intent per sequence inside its loop -- so admitting a drain on it there widens the
// outer gate and claims nothing extra. One predicate is used at all three sites rather
// than two spellings of the same fact.
//
// sess.mu must be held.
func (sess *mqttSession) drainOwesPubRelLocked(d *mqttDrain) bool {
	if d == nil {
		return false
	}
	if d.qos >= 2 {
		return true
	}
	for sseq, st := range d.seqs {
		if st == nil {
			continue
		}
		if st.intent {
			return true
		}
		if st.pi == 0 || d.cc == nil {
			continue
		}
		// Identity-matched, not merely present: the identifier is reused by later
		// exchanges, so an entry that names a different durable or sequence is another
		// exchange and says nothing about this one.
		if ack := sess.pendingPublish[st.pi]; ack != nil && ack.qos == 2 &&
			ack.jsDur == d.cc.Durable && ack.sseq == sseq {
			return true
		}
	}
	return false
}

func (sess *mqttSession) qos2DrainDurables() []string {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	var out []string
	for _, d := range sess.drains {
		if d.cc != nil && sess.drainOwesPubRelLocked(d) {
			out = append(out, d.cc.Durable)
		}
	}
	return out
}

// needsPubRelDelivery reports whether this resume must re-establish the PUBREL
// delivery path before anything else touches the second phase.
//
// A stored PUBREL alone is enough: pendingPubRel is memory-only, so after a restart
// nothing else drives ensurePubRelConsumerSubscription and a session carrying only
// PUBRELs would never deliver them. A QoS 2 drain needs it because the barrier may
// have to recreate a PUBREL, and $MQTT_out is interest-retained — a store with no
// consumer succeeds and keeps nothing.
//
// No lock held on entry.
func (sess *mqttSession) needsPubRelDelivery() bool {
	sess.mu.Lock()
	if sess.pubRelConsumer != nil {
		sess.mu.Unlock()
		return true
	}
	for _, d := range sess.drains {
		if sess.drainOwesPubRelLocked(d) {
			sess.mu.Unlock()
			return true
		}
	}
	sess.mu.Unlock()
	return false
}

// drainDurables lists the draining durables, for teardown paths that must delete
// them alongside the ordinary consumers.
//
// sess.mu must be held.
func (sess *mqttSession) drainDurablesLocked() []string {
	if len(sess.drains) == 0 {
		return nil
	}
	out := make([]string, 0, len(sess.drains))
	for _, d := range sess.drains {
		if d.cc != nil {
			out = append(out, d.cc.Durable)
		}
	}
	return out
}

// restoreDrainsFromRecord rebuilds the session's drains from a persisted record.
// Called wherever Cons is restored, so a drain is never invisible to a teardown.
//
// sess.mu must not be held (the session is not yet reachable).
func (sess *mqttSession) restoreDrainsFromRecord(ps *mqttPersistedSession) {
	if len(ps.Drains) == 0 {
		return
	}
	for sid, pd := range ps.Drains {
		sess.restoreDrainLocked(sid, mqttDrainFromPersisted(pd))
	}
}

// ---------------------------------------------------------------------------
// Resume
// ---------------------------------------------------------------------------

// restoreDrains re-establishes the delivery subscriptions of drains restored from
// the session record, so their started deliveries can be redelivered and completed
// after a disconnect, a restart or a failover.
//
// drainByDur is registered BEFORE the subscription is created, and the creation
// happens under sess.mu (as processJSConsumer does), so the first delivery that
// interest triggers cannot observe an initialized subscription without its drain
// and process a started delivery as an ordinary one.
//
// Runs from the client's readLoop after the subscriptions have been restored, and
// before resendPendingRedelivery, so a NAK'd redelivery lands on a subscription
// that exists.
//
// No lock held on entry.
func (sess *mqttSession) restoreDrains(c *client, skip map[string]bool) {
	sess.mu.Lock()
	if len(sess.drains) == 0 {
		sess.mu.Unlock()
		return
	}
	type pending struct {
		sid string
		d   *mqttDrain
	}
	todo := make([]pending, 0, len(sess.drains))
	for sid, d := range sess.drains {
		todo = append(todo, pending{sid, d})
	}
	// A restored drain still carrying an inactivity timer is one whose clear failed
	// when it was committed (or that predates the clear). Retry it here: until it
	// succeeds the durable can be reaped out from under the session while offline.
	var timed []*mqttDrain
	for _, p := range todo {
		if p.d.cc != nil && p.d.cc.InactiveThreshold != 0 {
			timed = append(timed, p.d)
		}
	}
	sess.mu.Unlock()
	sess.clearDrainInactivityTimers(c, timed)

	for _, p := range todo {
		d := p.d
		// Test seam, fired with sess.mu RELEASED and before the per-item lock: the only
		// point from which a test can retire this captured object and then let the install
		// proceed with nothing else failing, which is the schedule the recheck below
		// exists for. See setMQTTTestDrainRestorePreInstallHook.
		if h := mqttTestDrainRestorePreInstallHookV.Load(); h != nil {
			(*h)(d.cc.Durable)
		}
		if skip[d.cc.Durable] {
			// The PUBREL recovery could not resolve this drain, so establishing
			// source interest could resend the payload of an exchange already past
			// PUBREC. Withhold it for this connection; the next resume retries.
			c.Warnf("MQTT: withholding source interest for drained consumer %q: its PUBREL state is unresolved", d.cc.Durable)
			continue
		}
		sess.mu.Lock()
		// Re-check ownership AND retirement, under this lock, before installing anything.
		// The todo snapshot was taken under an earlier acquisition and released, so this
		// drain may have been retired in between — and retirement releases the sid and
		// closes the delivery path, so installing a reverse-index entry and a delivery
		// subscription for it would hand a terminally retired object a live delivery route
		// on a durable that has been delete-requested.
		if sid, ok := sess.drainSidForDurable(d.cc.Durable); !ok || sess.drains[sid] != d || d.retired {
			sess.mu.Unlock()
			continue
		}
		if sess.drainByDur == nil {
			sess.drainByDur = make(map[string]*mqttDrain)
		}
		sess.drainByDur[d.cc.Durable] = d
		inbox := d.cc.DeliverSubject
		_, err := sess.processQOS12Sub(c, []byte(inbox), []byte(inbox), nil,
			isMQTTReservedSubscription(d.subject), d.qos, d.noLocal, d.rap, false, d.subID,
			d.cc.Durable, mqttDeliverMsgCbQoS12)
		if err != nil {
			// Roll the registration back rather than leaving a drain nothing
			// delivers for: the next resume retries.
			delete(sess.drainByDur, d.cc.Durable)
		}
		sess.mu.Unlock()
		if err != nil {
			c.Errorf("MQTT: unable to restore the delivery subscription of drained consumer %q: %v", d.cc.Durable, err)
		}
	}
}

// recoverDrains reconciles each restored drain against CONSUMER PENDING STATE,
// which is the authority for what is still deliverable. Stream absence only ever
// PRUNES; it never adds a claim.
//
// Convergence comes from I1 (persist before ack), not from here: this is the
// safety net for TTL reaping and out-of-band consumer deletion.
//
// The consumer being GONE is reported as LOSS, not completion — its started
// deliveries went with it, because the messages stream is interest-retained — and
// the log names the client, the durable and each lost sequence, matching the
// posture the affine path takes for a lost delivery.
//
// It runs AFTER the past-PUBREC barrier and skips every durable the barrier could
// not resolve, because past PUBREC a claim no longer depends on its source message
// at all: what the client is owed there is a PUBREL, and the persisted intent is
// the only record of that obligation. So an intent-bearing claim is never pruned
// here, on any of the paths below (I11) — dropping one because its consumer
// vanished would abandon an exchange the client is still holding open.
//
// Only a POSITIVE fact about a specific sequence may prune it: its ack floor has
// passed it, its message is provably gone from the stream, or its consumer no
// longer exists. Deliberately NOT a rule: "the consumer reports nothing pending".
// This runs before source interest is re-established, and a push consumer whose
// deliver subject has no subscriber reports NumAckPending == 0 for deliveries that
// were merely reset to not-yet-delivered — so treating that as "nothing is owed"
// deleted every claim, and the durable with it, on every broker restart. An
// aggregate count is not evidence about an individual sequence.
//
// Every lookup happens BEFORE the record transaction opens, the transaction opens
// BEFORE the first mutation (I8), and a failed save restores exactly what was
// changed — so no other writer can observe or persist a half-applied
// reconciliation, and a reconciliation that does not land does not linger in
// memory as if it had.
//
// No lock held on entry.
func (sess *mqttSession) recoverDrains(c *client, skip map[string]bool) {
	sess.mu.Lock()
	drains := make([]*mqttDrain, 0, len(sess.drains))
	for _, d := range sess.drains {
		if d.cc == nil || skip[d.cc.Durable] {
			// Barrier-unresolved: its PUBREL state is unknown, so nothing about it
			// may be pruned. The next resume retries.
			continue
		}
		drains = append(drains, d)
	}
	id := sess.id
	sess.mu.Unlock()
	if len(drains) == 0 {
		return
	}

	var firstSeq uint64
	if si, err := sess.jsa.lookupStream(mqttStreamName); err == nil && si != nil {
		firstSeq = si.State.FirstSeq
	}

	// Phase 1: decide, mutating nothing.
	type drainPlan struct {
		d       *mqttDrain
		sid     string
		drop    bool
		unclaim []uint64
		lost    []uint64
	}
	var plans []drainPlan
	for _, d := range drains {
		ci, err := sess.jsa.lookupConsumer(mqttStreamName, d.cc.Durable)
		if err != nil && isErrorOtherThan(err, JSConsumerNotFoundErr) {
			// Indeterminate: keep every claim. Nothing may be pruned on a lookup we
			// could not complete.
			continue
		}
		sess.mu.Lock()
		claimed := make([]uint64, 0, len(d.seqs))
		var owed int
		for seq, st := range d.seqs {
			if !st.claimed {
				continue
			}
			if st.intent {
				owed++ // past PUBREC: the PUBREL obligation outlives its source
				continue
			}
			claimed = append(claimed, seq)
		}
		sess.mu.Unlock()

		p := drainPlan{d: d}
		if ci == nil {
			// The consumer is gone, so its started deliveries are LOST, not
			// completed: nothing can deliver or ack them now. The drain itself only
			// goes away if it owes no PUBREL.
			p.lost, p.unclaim, p.drop = claimed, claimed, owed == 0
		} else {
			for _, seq := range claimed {
				switch {
				case ci.AckFloor.Stream >= seq:
					p.unclaim = append(p.unclaim, seq) // already acked
				case firstSeq > 0 && seq < firstSeq:
					p.unclaim = append(p.unclaim, seq) // reaped off the head
				default:
					// Per-message expiry leaves holes ABOVE the first sequence, so
					// below-first is not the only way a message can be gone. Only an
					// exact lookup that CONFIRMS absence prunes; an unreadable one
					// leaves the claim alone.
					if _, lerr := sess.jsa.loadMsg(mqttStreamName, seq); lerr != nil && !isErrorOtherThan(lerr, JSNoMessageFoundErr) {
						p.unclaim = append(p.unclaim, seq)
					}
				}
			}
		}
		if p.drop || len(p.unclaim) > 0 {
			plans = append(plans, p)
		}
	}
	// The ALREADY-EMPTY sweep, and it runs before the early return below because that is
	// the drain it exists for. A drain that owed nothing on entry produces no plan at all,
	// so the post-save sweep at the end of this function never sees it — and since F-094
	// widened the hold to every past-PUBREC intent, such a drain refuses its filter until
	// something retires it. The reachable shape is a restore whose delivery subscription
	// failed: restoreDrains drops the drainByDur entry and keeps sess.drains[sid].
	//
	// This does NOT replace the post-save sweep. Moving that one here instead of adding
	// this one loses a trigger: a drain emptied by THIS pass's own pruning is still claimed
	// at this point, so only a sweep after the save can retire it.
	sess.retireEmptyDrains(c, drains)

	if len(plans) == 0 {
		return
	}

	// Phase 2: apply inside the transaction, and undo exactly this if it fails.
	tok := sess.beginRecordTx()
	sess.mu.Lock()
	for i := range plans {
		p := &plans[i]
		for _, seq := range p.unclaim {
			if st := p.d.seqs[seq]; st != nil {
				// Guard set WITH the release, so a delivery arriving during the save
				// cannot read it as durable. Cleared below on a proven outcome.
				st.claimed = false
				st.unclaimUnconfirmed = true
			}
		}
		if p.drop {
			p.sid = sess.dropDrainLocked(p.d)
		}
	}
	sess.mu.Unlock()

	outcome, err := sess.saveClassified(tok)
	undoPlans := func() {
		sess.mu.Lock()
		for i := range plans {
			p := &plans[i]
			for _, seq := range p.unclaim {
				if st := p.d.seqs[seq]; st != nil {
					st.claimed = true
					st.unclaimUnconfirmed = false
				}
			}
			if p.drop && p.sid != _EMPTY_ {
				sess.restoreDrainLocked(p.sid, p.d)
			}
		}
		sess.mu.Unlock()
	}
	// landed is what a PROVEN save owes, stated once and used by both landed arms: clear
	// the unconfirmed-removal guards, and refund the local consumer charge of every drain
	// this pass removed.
	//
	// The refund belongs HERE and not at the drop, which is the whole reason this is a
	// helper. A charge released at the drop would be released for a removal that never
	// landed, and saveDefinitelyFailed restores the object through undoPlans STILL CHARGED
	// — so there would be nothing to re-charge and no honest way to re-establish it.
	// Refunding on proof needs neither a recorded amount nor an idempotent re-charge.
	//
	// finishDrainIfEmpty's own refund cannot cover this: the consumer-absent arm removes
	// the drain through p.drop without ever reaching retirement, which is the live leak —
	// such a drain is charged by stageDrain, survives into a resume, and sess.tmaxack is
	// otherwise reset only by clear(), so the session's consumer budget shrinks until
	// SUBSCRIBEs are refused against mqttMaxAckTotalLimit. Double refunding is not a
	// hazard: refundConsumerChargeLocked returns early when the entry is gone, which is
	// also why an already-empty drain retired by this same pass cannot be refunded twice.
	landed := func() {
		sess.mu.Lock()
		for i := range plans {
			p := &plans[i]
			for _, seq := range p.unclaim {
				if st := p.d.seqs[seq]; st != nil {
					st.unclaimUnconfirmed = false
				}
			}
			if p.drop && p.d.charged && p.d.cc != nil {
				sess.refundConsumerChargeLocked(p.d.cc.Durable)
			}
		}
		sess.mu.Unlock()
	}
	switch outcome {
	case saveLanded:
		landed()
	case saveDefinitelyFailed:
		// Undo exactly what was changed, still inside the transaction. The restored drain
		// is still charged, which is what makes landed()'s refund the only one.
		undoPlans()
	case saveMaybeLanded:
		// The record may already carry the reconciliation, so keep it — the guards are
		// already set — and the resolver settles it by re-attempting those exact bytes.
		sess.retainOnLanded(func(recordTx) {
			// Proven durable: the reconciliation stands, so discharge exactly what the
			// immediate landed arm discharges, then tear down whatever it emptied.
			landed()
			for i := range plans {
				sess.finishDrainIfEmptyInTx(c, plans[i].d)
			}
		})
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		c.Errorf("MQTT: %v", terr)
	}
	if err != nil {
		c.Debugf("MQTT: unable to persist drain reconciliation for %q: %v", id, err)
		if outcome == saveMaybeLanded {
			// Runs during CONNECT, so resolve it now rather than leaving the
			// reconciliation to the periodic pass's next tick.
			sess.resolveUncertainNow(c)
		}
		return
	}

	// Persisted, so the losses are real: report them naming the client, the durable
	// and every sequence that went with it.
	for i := range plans {
		if p := &plans[i]; len(p.lost) > 0 {
			c.Errorf("MQTT: drained consumer %q of client %q disappeared; %d started QoS 1/2 delivery(ies) are LOST: %v",
				p.d.cc.Durable, id, len(p.lost), p.lost)
		}
	}
	// Pruning to empty tears the drain down through the normal path.
	sess.retireEmptyDrains(c, drains)
}

// retireEmptyDrains hands every drain that owes nothing to the ordinary retirement path.
//
// Called TWICE from recoverDrains, and both calls are load-bearing. Before the early return
// it catches a drain that was already empty on entry, which produces no pruning plan and
// would otherwise never be visited; after the save it catches one this pass's own pruning
// emptied, which was still claimed when the first call ran. A review round found that
// collapsing the two into one relocated sweep silently drops the second case.
//
// The candidate set is the CALLER's, which is what keeps the skip-set exclusion: recoverDrains
// builds it having already dropped every barrier-unresolved durable, whose PUBREL state is
// unknown and about which nothing may be pruned or retired.
//
// No lock held on entry.
func (sess *mqttSession) retireEmptyDrains(c *client, drains []*mqttDrain) {
	sess.mu.Lock()
	empty := make([]*mqttDrain, 0, len(drains))
	for _, d := range drains {
		// Ownership by SID, matching finishDrainIfEmpty's own guard: a drainByDur test here
		// would skip exactly the drain whose delivery subscription failed to restore.
		sid, ok := sess.drainSidForDurable(d.cc.Durable)
		if ok && sess.drains[sid] == d && d.retirable() {
			empty = append(empty, d)
		}
	}
	sess.mu.Unlock()
	for _, d := range empty {
		sess.finishDrainIfEmpty(c, d)
	}
}

// ---------------------------------------------------------------------------
// The QoS 2 second phase (I2, I10, I11, I12)
// ---------------------------------------------------------------------------

// mqttDrainPubRelAttempts bounds how many times one resume will try to recreate a
// PUBREL whose store keeps being swallowed by deduplication. Exhausting it fails
// closed — the claim survives and the next resume retries — rather than acking a
// source whose PUBREL does not exist.
const mqttDrainPubRelAttempts = 3

// recordPubRelIntent durably records that a drained sequence's exchange has moved
// past PUBREC, BEFORE its PUBREL is stored.
//
// This is the linearization point of the whole second phase. Past it the payload
// is never resent, and a missing PUBREL is RECREATED rather than read as absence —
// so recovery is a positive fact rather than an inference from silence (I11).
//
// definite reports whether a failure is certain (the fence refused, or the CAS was
// rejected) as opposed to ambiguous, because the caller must roll back completely
// in the first case and resolve the sequence in the second.
//
// No lock held on entry.
// ownerNonce is the exchange identity, persisted HERE — before the PUBREL store — for the
// reason the field's own comment gives: a recreate must be able to name the same exchange
// the durable owner slot names, and the intent is the only thing that survives to tell it.
func (sess *mqttSession) recordPubRelIntent(c *client, jsDur string, sseq uint64, pi uint16, ownerNonce string) (attempt uint64, definite bool, err error) {
	// Validated BEFORE anything is mutated or saved. Persisting an intent with no usable
	// exchange identity writes the very shape the restore path now refuses, so the session
	// would be unable to resume — the broker must not be the source of that record.
	if !mqttValidInflightNonce(ownerNonce) {
		return 0, true, errMQTTInvalidSession
	}
	sess.mu.Lock()
	d := sess.drainByDur[jsDur]
	if d == nil {
		sess.mu.Unlock()
		return 0, true, nil // not drained: no intent is owed
	}
	if st := d.seqs[sseq]; st == nil || !st.claimed {
		sess.mu.Unlock()
		return 0, true, nil
	}
	sess.mu.Unlock()

	// The fence opens BEFORE the first mutation (I8): a transaction that begins
	// after the state has already changed cannot make that change invisible to
	// another writer. The BLOCKING form is the right one here — this is a read-loop
	// path, and the off-read-loop form's refusal would turn ordinary fence
	// contention into a dropped connection.
	tok := sess.beginRecordTx()

	sess.mu.Lock()
	st := d.seqs[sseq]
	if st == nil || !st.claimed {
		// Re-validated after acquiring the fence: it completed while we waited.
		sess.mu.Unlock()
		if terr := sess.endRecordTx(tok); terr != nil && c != nil {
			c.Errorf("MQTT: %v", terr)
		}
		return 0, true, nil
	}
	// Set the phase under the lock, so a PUBREL callback that fires the instant the
	// store lands cannot take the resend path.
	prevPhase, prevPI, prevIntent, prevOwner := st.phase, st.pi, st.intent, st.ownerNonce
	st.phase, st.pi, st.intent, st.ownerNonce = drainPubRelUncertain, pi, true, ownerNonce
	attempt = st.attempt
	sess.mu.Unlock()

	outcome, err := sess.saveClassified(tok)
	if outcome == saveDefinitelyFailed {
		// Nothing landed, so put the session back exactly as it was — inside the
		// transaction, before any other writer can see (or persist) the intent.
		sess.rollbackPubRelIntent(d, sseq, prevPhase, prevPI, prevIntent, prevOwner)
	}
	if terr := sess.endRecordTx(tok); terr != nil && c != nil {
		c.Errorf("MQTT: %v", terr)
	}
	switch outcome {
	case saveLanded:
		return attempt, false, nil
	case saveMaybeLanded:
		// The intent MAY have landed, and the phase and the intent STAY: resending the
		// payload would be wrong if it did, and the resolver settles the write forward
		// by re-attempting those exact bytes rather than leaving the question open.
		// Nothing was withheld for want of proof here — the caller's next step is the
		// PUBREL store, gated on the intent it just set — so there is no landed
		// continuation to register (I11).
		return attempt, false, err
	default:
		return 0, true, err
	}
}

// rollbackPubRelIntent restores a sequence's phase after an intent write that
// definitively did not land.
func (sess *mqttSession) rollbackPubRelIntent(d *mqttDrain, sseq uint64, phase drainPhase, pi uint16, intent bool, ownerNonce string) {
	sess.mu.Lock()
	if st := d.seqs[sseq]; st != nil {
		st.phase, st.pi, st.intent, st.ownerNonce = phase, pi, intent, ownerNonce
	}
	sess.mu.Unlock()
}

// drainIntentOwnedBy reports whether the drained sequence's own intent names BOTH owner and
// pi — or whether this source is not a drained one at all, in which case there is no intent
// to contradict and the ordinary path applies.
//
// A drained sequence with a past-PUBREC intent whose identity does NOT match is refused:
// the record belongs to another exchange, and nothing about this source may be finalized on
// it. An intent with no usable identity of its own is refused too, which is what keeps the
// quarantine on a marker-less intent independent of any scan-derived ambiguity.
//
// The IDENTIFIER is part of the same question, and this is the second place it had to be
// added. Recovery compares it because a record naming the right source and exchange under a
// foreign identifier would otherwise confirm the intent; PUBREL delivery is ASYNCHRONOUS
// and races ahead of that comparison, so the same record arriving here would confirm the
// drain, persist the removal of its source claim, acknowledge the source and forward a
// PUBREL under the wrong identifier — leaving the client's original exchange with neither
// its PUBREL nor the evidence to rebuild one. Both readers now ask the whole question.
//
// No lock held on entry.
func (sess *mqttSession) drainIntentOwnedBy(jsDur string, sseq uint64, owner string, pi uint16) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	d := sess.drainByDur[jsDur]
	if d == nil {
		return true // not a drained source
	}
	st := d.seqs[sseq]
	if st == nil {
		return true // this drain does not claim that sequence: not ours to contradict
	}
	if !st.intent {
		// A CLAIMED drain with NO intent is not "no intent to contradict" — it is a
		// contradiction. The drain says this sequence is still in its PUBLISH phase, and a
		// stored PUBREL for it says the client answered PUBREC; both cannot be true of the
		// same exchange, so nothing about this source may be finalized on the strength of
		// the record.
		//
		// Conflating the two was a fail-open path a review round found. Returning true here
		// let the callback call confirmDrainPubRel immediately, which — while the entry is
		// still claimed — removes the persisted claim, acknowledges the source and forwards
		// the PUBREL, with neither the owner nonce nor the identifier compared, because
		// there was no intent to compare them against. A session-record fork produces
		// exactly this pair: the surviving record keeps the pre-PUBREC drain state while a
		// superseded writer's correlated PUBREL survives on $MQTT_out, so another
		// exchange's record could consume this source and its register.
		//
		// An unclaimed entry is different and is permitted: the claim is already released,
		// so there is nothing left for the record to consume, and confirmDrainPubRel
		// refuses an unclaimed sequence on its own.
		return !st.claimed
	}
	return mqttValidInflightNonce(st.ownerNonce) && st.ownerNonce == owner && st.pi == pi
}

// confirmDrainPubRel promotes a sequence to drainPubRelConfirmed. Only DELIVERY of
// the PUBREL (or finding it present at its sequence) may do this — never a store
// response, which on an interest-retained stream is returned for a skipped store
// too (I12).
//
// Returns false when the sequence is not (or no longer) a claimed drained one.
//
// No lock held on entry.
func (sess *mqttSession) confirmDrainPubRel(jsDur string, sseq uint64) (*mqttDrain, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	d := sess.drainByDur[jsDur]
	if d == nil {
		return nil, false
	}
	st := d.seqs[sseq]
	if st == nil {
		return nil, false
	}
	st.phase = drainPubRelConfirmed
	// An UNCONFIRMED removal counts as still claimed here. Its record state is
	// unknown, so forwarding the PUBREL on the strength of it would let the client
	// PUBCOMP — and ack the PUBREL record away — while the source may still be
	// claimed by the stored record, which is precisely the double-delivery the
	// persist-before-forward ordering exists to prevent (I10).
	return d, st.claimed || st.unclaimUnconfirmed
}

// drainPubRelState reports a drained sequence's identifier and attempt, for a
// PUBREL store that must name them.
//
// No lock held on entry.
func (sess *mqttSession) drainPubRelState(jsDur string, sseq uint64) (pi uint16, attempt uint64, drained bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	d := sess.drainByDur[jsDur]
	if d == nil {
		return 0, 0, false
	}
	st := d.seqs[sseq]
	if st == nil {
		return 0, 0, false
	}
	return st.pi, st.attempt, true
}

// bumpDrainPubRelAttempt records a fresh store-attempt generation for a sequence
// whose recreate was swallowed by deduplication, and persists it. Without this a
// poisoned identity could never be escaped: the dedupe entry outlives its message,
// so re-storing under the same id is accepted while storing nothing.
//
// No lock held on entry.
func (sess *mqttSession) bumpDrainPubRelAttempt(c *client, d *mqttDrain, sseq uint64) (uint64, error) {
	sess.mu.Lock()
	st := d.seqs[sseq]
	if st == nil {
		sess.mu.Unlock()
		return 0, nil
	}
	if st.attempt == ^uint64(0) {
		sess.mu.Unlock()
		// Fail closed rather than wrap into an identity we have already used.
		return 0, errMQTTDrainAttemptExhausted
	}
	sess.mu.Unlock()

	// The fence opens before the counter moves (I8), so no other writer can persist
	// an attempt this transaction may still roll back.
	tok := sess.beginRecordTx()
	sess.mu.Lock()
	if st = d.seqs[sseq]; st == nil {
		sess.mu.Unlock()
		if terr := sess.endRecordTx(tok); terr != nil && c != nil {
			c.Errorf("MQTT: %v", terr)
		}
		return 0, nil
	}
	if st.attempt == ^uint64(0) {
		sess.mu.Unlock()
		if terr := sess.endRecordTx(tok); terr != nil && c != nil {
			c.Errorf("MQTT: %v", terr)
		}
		return 0, errMQTTDrainAttemptExhausted
	}
	st.attempt++
	next := st.attempt
	sess.mu.Unlock()

	outcome, err := sess.saveClassified(tok)
	undoBump := func() {
		sess.mu.Lock()
		if st := d.seqs[sseq]; st != nil && st.attempt == next {
			st.attempt--
		}
		sess.mu.Unlock()
	}
	switch outcome {
	case saveDefinitelyFailed:
		// Nothing landed, so the counter goes back — inside the transaction.
		undoBump()
	case saveMaybeLanded:
		// The record may already name this attempt, so keep the bump — reusing the
		// previous identity would be exactly the poisoned deduplication the counter
		// exists to escape — and the resolver makes those exact bytes durable, so the
		// record ends up naming the identity memory holds. Nothing is owed on proof.
	}
	if terr := sess.endRecordTx(tok); terr != nil && c != nil {
		c.Errorf("MQTT: %v", terr)
	}
	if err != nil {
		return 0, err
	}
	return next, nil
}

var (
	errMQTTDrainAttemptExhausted = errors.New("PUBREL store-attempt counter exhausted")
	errMQTTDrainPubRelUnverified = errors.New("could not verify a recreated PUBREL record")
)

// ---------------------------------------------------------------------------
// The past-PUBREC barrier
// ---------------------------------------------------------------------------

// recoverDrainPubRels reconstructs each drained QoS 2 sequence's phase BEFORE any
// source interest exists, and it does so from the PERSISTED INTENT — a positive
// fact, never an inference from absence.
//
// The ordering is forced. The phase is memory-only, so after a restart every claim
// starts at drainPublish; creating the source delivery subscription establishes
// interest that can redeliver the pending PUBLISH immediately, which would take
// the claimed forced-redelivery path and RESEND the payload of an exchange already
// past PUBREC — before the correlated PUBREL callback ever ran. Registering
// drainByDur first prevents misclassification but not that resend, and subscribing
// to PUBREL first does not help either, since its delivery is asynchronous.
//
// $MQTT_out is interest-retained, so a PUBREL still present on the session's PUBREL
// subject has NOT been acked and its exchange is not complete.
//
// Fails CLOSED: a scan error, a failed batch save or a failed recreate leaves the
// affected drains' claims intact and reports which durables must not have their
// source interest installed for this connection. Denying the whole CONNECT would
// turn a JetStream hiccup into an outage, so interest is withheld instead.
//
// Returns the set of durables whose source subscriptions must be skipped.
//
// No lock held on entry. The PUBREL path must already be ready.
// mqttStoredPubRel is one PUBREL record found on a session's PUBREL subject: its
// stream sequence, the packet identifier it will be re-sent under, and the source
// delivery it completes (correlated records only — an uncorrelated one carries no
// source and srcDur is empty).
type mqttStoredPubRel struct {
	seq    uint64
	pi     uint16
	srcDur string
	srcSeq uint64
	// affRef is the session-affine shared QoS 2 assignment this record proves the
	// client answered PUBREC for, as a tri-state: absent on every PUBREL that does not
	// complete an affine copy, malformed on a shape this broker does not write, and it
	// is the caller's job to keep those two apart.
	affRef mqttPubRelAffineRef
	// shapeErr is this record's own decode verdict from mqttDecodeStoredPubRel, non-nil for
	// a shape this broker does not write. It travels WITH the record because the callers'
	// dispositions differ: a reader that would act on the record skips it, while the
	// affine-proof walk and the teardown must still see what they are about to destroy.
	shapeErr error
	// owner is the EXCHANGE identity this record belongs to, as a tri-state. Carried here
	// because every consumer of this scan decides something on the record's behalf — does
	// an intent's PUBREL exist, which identifiers are past PUBREC — and none of those
	// decisions is safe on a record whose exchange cannot be established. Absent or
	// malformed is refused by the consumer, not repaired here.
	owner mqttPubRelOwner
}

// mqttPubRelSourceKey keys a source delivery for the indexes built over these
// records.
func mqttPubRelSourceKey(dur string, seq uint64) string {
	return dur + "/" + strconv.FormatUint(seq, 10)
}

// scanPubRelRecords lists every PUBREL record still on one session incarnation's
// PUBREL subject. They are unacked by construction — an acked one is removed from the
// interest-retained $MQTT_out — so each is an exchange past PUBREC and not yet
// complete.
//
// Shared by the drain's past-PUBREC barrier and by the in-flight register recovery,
// which need the same walk for different questions: "does the PUBREL this intent
// promised exist?" and "which packet identifiers are already owned by a PUBREL rather
// than by a register?". ok is false when the walk could not be completed, and a
// partial walk is NEVER evidence of absence for either caller.
//
// Session lock NOT held on entry.
// Every record is RETURNED, each carrying its own decode verdict in shapeErr, and nothing
// is filtered out here. That is deliberate and it is the correction to a first attempt that
// dropped the refused ones.
//
// The dispositions genuinely differ per caller, so a refusal that is right for one is wrong
// for another. Register recovery and drain recovery must not DERIVE anything from an
// unsupported record — it would suppress a live source as past PUBREC, or confirm an intent
// — so they skip it. The affine-proof walk and the teardown must SEE it: their question is
// what evidence is about to be destroyed with the consumer that retains it, and a record
// they cannot attribute is still evidence they may not destroy unread. Dropping it here
// would have made the teardown silently destroy an affine PUBREC proof it exists to secure.
//
// refused is the count, for the diagnostic a caller emits to an operator.
func (sess *mqttSession) scanPubRelRecords(pubRelSubject string) (records []mqttStoredPubRel, refused int, ok bool) {
	if sess.asm != nil {
		sess.asm.mu.RLock()
		hook := sess.asm.testInflightPubRelScan
		sess.asm.mu.RUnlock()
		if hook != nil {
			return hook(pubRelSubject)
		}
	}
	if pubRelSubject == _EMPTY_ {
		return nil, 0, true
	}
	for seq := uint64(1); ; {
		smsg, err := sess.jsa.loadNextMsgFromSeq(mqttOutStreamName, pubRelSubject, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return records, refused, false
			}
			return records, refused, true
		}
		seq = smsg.Sequence + 1
		// Decoded as ONE shape rather than header by header: a record only some of whose
		// headers are legal is evidence of nothing to any caller that would act on it.
		// See mqttDecodeStoredPubRel.
		//
		// The verdict TRAVELS with the record rather than removing it, and `ok` stays true
		// either way — conflating a per-record refusal with an unreadable SCAN would skip
		// drains that have nothing wrong with them.
		shape, derr := mqttDecodeStoredPubRel(smsg.Header)
		if derr != nil {
			refused++
		}
		records = append(records, mqttStoredPubRel{
			seq: smsg.Sequence, pi: shape.pi, srcDur: shape.srcDur, srcSeq: shape.srcSeq,
			affRef: shape.affRef, owner: shape.owner, shapeErr: derr,
		})
	}
}

func (sess *mqttSession) recoverDrainPubRels(c *client) map[string]bool {
	sess.mu.Lock()
	type claim struct {
		d          *mqttDrain
		seq        uint64
		pi         uint16
		attempt    uint64
		ownerNonce string
	}
	var claims []claim
	// Durables whose past-PUBREC intent cannot be attributed at all. They are SKIPPED
	// rather than claimed: see the branch below.
	skipUnattributable := map[string]bool{}
	for _, d := range sess.drains {
		if !sess.drainOwesPubRelLocked(d) {
			continue
		}
		for seq, st := range d.seqs {
			// Only a sequence whose INTENT is recorded is past PUBREC. One without
			// it provably never got a durable PUBREC, so its payload is redelivered
			// with DUP and the client re-PUBRECs ([MQTT-4.3.3]) — the ordinary
			// failover-before-PUBREC path, which completes rather than stalls.
			if !st.claimed || !st.intent {
				continue
			}
			if st.pi == 0 || st.ownerUnusable {
				// Past PUBREC and UNATTRIBUTABLE. It cannot join the claims below —
				// there is no identifier to compare a record against, and no identity to
				// recreate one under — but it must not be silently absent either: this
				// drain's durable goes into the SKIP set, so recoverDrains does not
				// restore source interest for it and the payload of an exchange the
				// client has already answered is never redelivered. The claim stays,
				// so the source is never acknowledged either.
				skipUnattributable[d.cc.Durable] = true
				continue
			}
			claims = append(claims, claim{d, seq, st.pi, st.attempt, st.ownerNonce})
		}
	}
	pubRelSubject := sess.pubRelSubject
	idHash := sess.idHash
	sess.mu.Unlock()
	if len(claims) == 0 {
		// Still return the skip set: an unattributable intent with no other claim beside
		// it is exactly the case where returning nil would let source interest be
		// restored for it.
		if len(skipUnattributable) == 0 {
			return nil
		}
		return skipUnattributable
	}

	// Index every PUBREL record still on this session's subject by the source it
	// names. An unreadable scan is never evidence of absence.
	records, refused, scanned := sess.scanPubRelRecords(pubRelSubject)
	if !scanned {
		c.Errorf("MQTT: unable to scan the PUBREL records of client %q", sess.id)
	}
	if refused > 0 {
		c.Warnf("MQTT: ignoring %d stored PUBREL record(s) of client %q whose shape this broker "+
			"does not write: they are never transmitted, never acknowledged and reserve no "+
			"identifier; remove them from the %s stream", refused, sess.id, mqttOutStreamName)
	}
	// Indexed by SOURCE, but carrying the record's exchange identity, because the source
	// key alone cannot answer the question this index is used for. Two records can name one
	// source — a superseded broker's late store is a supported shape — and only one of them
	// belongs to the exchange the intent is about.
	type foundRel struct {
		seq   uint64
		pi    uint16
		owner mqttPubRelOwner
	}
	found := make(map[string]foundRel, len(records))
	key := mqttPubRelSourceKey
	for _, r := range records {
		if r.shapeErr != nil {
			// Nothing may be DERIVED from an unsupported record. Left in the index it
			// would satisfy an intent, finalize the claim and let the source be
			// acknowledged; skipped, the exchange recreates its own record under its own
			// identity and identifier, and the stray one is left for the operator or
			// generation cleanup.
			continue
		}
		found[key(r.srcDur, r.srcSeq)] = foundRel{seq: r.seq, pi: r.pi, owner: r.owner}
	}

	skip := skipUnattributable
	if !scanned {
		for _, cl := range claims {
			skip[cl.d.cc.Durable] = true
		}
		return skip
	}

	var confirmed []claim
	for _, cl := range claims {
		if fr, ok := found[key(cl.d.cc.Durable, cl.seq)]; ok {
			// Intent present, record present — but "present" is not enough. The record
			// must belong to THIS EXCHANGE, proven by an exact owner-identity match
			// against the identity the intent persisted.
			//
			// Matching on the source alone was the defect a holistic review found: a
			// record naming the right source but a different exchange, or one whose owner
			// header is absent or malformed, would satisfy the intent and finalize the
			// drain claim — acknowledging a source on the strength of evidence belonging
			// to someone else. A mismatch is treated as NO record, which is the safe
			// reading: the recreate below stores the exchange's own record under its own
			// identity, and the stray one is left for the operator or generation cleanup.
			// The IDENTIFIER is part of that proof, and leaving it out was a second
			// defect of the same kind. The scan parses each record's packet identifier;
			// discarding it let a record with the right source and the right owner but a
			// FOREIGN identifier confirm the intent, after which the delivery path would
			// send a PUBREL under that other identifier — or withhold one entirely — while
			// this exchange's source was acknowledged and its evidence removed. The
			// intent persisted the identifier the client is waiting on, so an exact match
			// is required.
			if fr.owner.valid() && mqttValidInflightNonce(cl.ownerNonce) &&
				fr.owner.Nonce == cl.ownerNonce && fr.pi == cl.pi {
				// The exchange is past PUBREC and its own PUBREL exists. The callback
				// will forward it; the claim is finalized here rather than waiting on
				// that asynchronous delivery.
				confirmed = append(confirmed, cl)
				continue
			}
			c.Warnf("MQTT: the PUBREL found for drained sequence %v of client %q does not belong to "+
				"this exchange (owner state %v, identifier %d, expected %d); recreating this "+
				"exchange's own record", cl.seq, sess.id, fr.owner.State, fr.pi, cl.pi)
		}
		// Intent present, NO record: the store never landed (skipped for lack of
		// interest, say) or the record was deleted. Recreate it under the recorded
		// identifier so the client completes its ORIGINAL exchange.
		//
		// An intent that is still present also proves the exchange was not
		// finalized — the callback removes claim and intent before forwarding — so a
		// missing record can never mean "already completed".
		if err := sess.recreateDrainPubRel(c, idHash, pubRelSubject, cl.d, cl.seq, cl.pi, cl.attempt, cl.ownerNonce); err != nil {
			c.Errorf("MQTT: unable to recreate the PUBREL for drained sequence %v of client %q: %v", cl.seq, sess.id, err)
			skip[cl.d.cc.Durable] = true
			continue
		}
		confirmed = append(confirmed, cl)
	}

	if len(confirmed) == 0 {
		return skip
	}
	// One transaction for the batch: every confirmed claim is removed together, and
	// the fence opens before the first removal (I8).
	tok := sess.beginRecordTx()
	sess.mu.Lock()
	for _, cl := range confirmed {
		if st := cl.d.seqs[cl.seq]; st != nil {
			st.phase = drainPubRelConfirmed
			// Guard set WITH the release; see recoverDrains.
			st.claimed = false
			st.unclaimUnconfirmed = true
		}
	}
	sess.mu.Unlock()

	outcome, err := sess.saveClassified(tok)
	switch outcome {
	case saveLanded:
		sess.mu.Lock()
		for _, cl := range confirmed {
			if st := cl.d.seqs[cl.seq]; st != nil {
				st.unclaimUnconfirmed = false
			}
		}
		sess.mu.Unlock()
	case saveDefinitelyFailed:
		// Nothing landed: restore every claim, inside the transaction.
		sess.mu.Lock()
		for _, cl := range confirmed {
			if st := cl.d.seqs[cl.seq]; st != nil {
				st.claimed = true
				st.unclaimUnconfirmed = false
			}
		}
		sess.mu.Unlock()
	case saveMaybeLanded:
		// May be durable: keep the removals — the guards are already set — and let the
		// resolver settle them by re-attempting these exact bytes.
		batch := confirmed
		sess.retainOnLanded(
			func(recordTx) {
				// Proven durable: clear the markers and discharge the acks and
				// teardowns the batch withheld for want of proof.
				for _, cl := range batch {
					sess.mu.Lock()
					var ack string
					if st := cl.d.seqs[cl.seq]; st != nil {
						st.unclaimUnconfirmed = false
						ack = st.ack
					}
					sess.mu.Unlock()
					if ack != _EMPTY_ {
						sess.jsa.sendAck(ack)
					}
					sess.finishDrainIfEmptyInTx(c, cl.d)
				}
			},
		)
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		c.Errorf("MQTT: %v", terr)
	}
	if err != nil {
		// Withhold interest either way: an ambiguous batch keeps its removals (the
		// record may hold them) but must not be acked or delivered from until the
		// session resolves.
		sess.mu.Lock()
		for _, cl := range confirmed {
			skip[cl.d.cc.Durable] = true
		}
		sess.mu.Unlock()
		c.Errorf("MQTT: unable to persist the PUBREL recovery of client %q: %v", sess.id, err)
		if outcome == saveMaybeLanded {
			sess.resolveUncertainNow(c)
		}
		return skip
	}
	// Persisted: the sources may now be acked, and any emptied drain torn down.
	for _, cl := range confirmed {
		sess.mu.Lock()
		var ack string
		if st := cl.d.seqs[cl.seq]; st != nil {
			ack = st.ack
		}
		sess.mu.Unlock()
		if ack != _EMPTY_ {
			sess.jsa.sendAck(ack)
		}
		sess.finishDrainIfEmpty(c, cl.d)
	}
	return skip
}

// recreateDrainPubRel re-stores a PUBREL the record owes but the stream does not
// have, and VERIFIES it is really there.
//
// Two rules are load-bearing. The PUBREL durable must already exist — $MQTT_out is
// interest-retained, so a store with no consumer returns success and keeps
// nothing, and finalizing on that response would ack the source and leave the
// client waiting forever. And presence is checked at the RETURNED SEQUENCE rather
// than inferred from the response: a deduplication hit stores nothing, and a
// no-interest skip is an unflagged success. A record found absent bumps the
// attempt (a fresh deduplication identity) and retries.
//
// No lock held on entry.
// ownerNonce is the exchange identity from the PERSISTED INTENT, and it is not optional:
// the recreated record must name the same exchange the durable owner slot does, so minting
// a fresh one here would produce a record no slot authorizes and which therefore could
// never act. An intent that carries no valid nonce is an unsupported persisted shape and
// is refused by the caller before reaching here.
func (sess *mqttSession) recreateDrainPubRel(c *client, idHash, pubRelSubject string, d *mqttDrain, sseq uint64, pi uint16, attempt uint64, ownerNonce string) error {
	if !mqttValidInflightNonce(ownerNonce) {
		return errMQTTDrainPubRelUnverified
	}
	for i := 0; i < mqttDrainPubRelAttempts; i++ {
		msgId := mqttPubRelMsgId(idHash, d.cc.Durable, sseq, attempt)
		// No affine reference: a DRAINED source is a plain QoS 2 consumer's, never a
		// shared group's affine copy, so there is no assignment for it to name.
		natsMsg, headerLen := mqttNewCorrelatedPubRel(pi, ownerNonce, d.cc.Durable, sseq, msgId, _EMPTY_, _EMPTY_)
		resp, err := sess.jsa.storeMsg(pubRelSubject, headerLen, natsMsg)
		if err != nil {
			return err
		}
		if resp != nil && resp.Duplicate {
			// Diagnostic only: a deduplication hit is flagged, but a no-interest skip
			// is not, so the presence check below runs either way.
			c.Debugf("MQTT: the PUBREL recreate for sequence %v was deduplicated; verifying", sseq)
		}
		// Presence at the returned sequence is the ONLY proof (I12).
		if resp != nil && resp.Sequence > 0 {
			if smsg, lerr := sess.jsa.loadMsg(mqttOutStreamName, resp.Sequence); lerr == nil && smsg != nil {
				// The owner identity is verified alongside the correlation. A record at
				// this sequence naming the right source but a DIFFERENT exchange is not
				// the record this intent owes — it is another exchange's, and accepting
				// it would let the drain finalize against evidence the owner slot does
				// not authorize.
				// Through the one decoder, so this verification is at least as strict
				// as every reader that will act on the record afterwards — including
				// the IDENTIFIER, which is what the client is waiting on.
				shape, derr := mqttDecodeStoredPubRel(smsg.Header)
				if derr == nil && shape.srcDur == d.cc.Durable && shape.srcSeq == sseq &&
					shape.owner.Nonce == ownerNonce && shape.pi == pi {
					return nil
				}
			}
		}
		next, berr := sess.bumpDrainPubRelAttempt(c, d, sseq)
		if berr != nil {
			return berr
		}
		attempt = next
	}
	return errMQTTDrainPubRelUnverified
}

// adoptDrainLocked reclaims a draining consumer for a re-SUBSCRIBE of the same
// filter: the drain becomes an ordinary membership again, keeping its durable, its
// delivery subscription and — crucially — its started deliveries, which the client
// is still owed and which simply resume being delivered normally.
//
// No consumer is created, so nothing is charged: the drain's existing charge (if
// this process made one) carries straight back, which is why it cannot
// double-charge.
//
// Returns true when a drain was adopted.
//
// sess.mu must be held.
// mqttDrainHold is WHY a drain owns its sid exclusively. Any value other than
// mqttDrainNotHeld means held; the reason exists to tell the two apart in a log line,
// never to let a caller act on one and not the other.
type mqttDrainHold uint8

const (
	mqttDrainNotHeld mqttDrainHold = iota
	// mqttDrainHeldUnusableIdentity: a past-PUBREC intent carrying no usable exchange
	// identity. Nothing may act on it and nothing may take its place, so it is released
	// only when an operator removes the record.
	mqttDrainHeldUnusableIdentity
	// mqttDrainHeldPastPubRecIntent: the drain carries a past-PUBREC intent. Released
	// when the drain is RETIRED — see drainRetirableLocked and the two sweeps.
	mqttDrainHeldPastPubRecIntent
)

// reason renders the hold for the two refusal messages. It is load-bearing rather than
// decoration: before F-094 both sites said "an unusable exchange identity ... remove that
// record to restore the filter" for EVERY refusal, which for an ordinary past-PUBREC intent
// is false twice over — the identity is usable, and the release is the drain's own
// retirement rather than an operator.
func (h mqttDrainHold) reason() string {
	switch h {
	case mqttDrainHeldUnusableIdentity:
		return "is past PUBREC with an unusable exchange identity, so nothing may act on it " +
			"and nothing may take its place; remove that record to restore the filter"
	case mqttDrainHeldPastPubRecIntent:
		// Deliberately does NOT promise release "once that exchange completes". The hold is
		// unqualified, so it also covers an intent whose own exchange is already finalized
		// while another sequence on the same drain is still outstanding — and there the
		// named exchange owes nothing and completing it releases nothing. What actually
		// frees the sid is the DRAIN being retired, so that is what the operator is told.
		return "carries past-PUBREC evidence that adoption would erase, letting the source " +
			"be sent again; the filter stays held until that drain is retired"
	}
	return _EMPTY_
}

// drainSidHoldLocked reports whether a drain under sid holds evidence no subscription cycle
// may take, and why.
//
// This is the PROPERTY three review rounds converged on, and stating it once is the point.
// Each round found the same bypass one subscription cycle further out — adoption erased the
// quarantine, then a second UNSUBSCRIBE took its sid, then the dual drain-and-consumer state
// it left was lossy under fork repair. Each was a symptom of the same thing: the quarantine
// did not OWN its sid exclusively. So it does now, and every path that would occupy or
// provision under that sid asks this one question rather than re-deriving the condition.
//
// F-094 WIDENED it from the unusable-identity case to EVERY past-PUBREC intent, and the
// predicate is deliberately unqualified — no claim flag joins it. Two narrower shapes were
// tried and both were refuted, so neither may come back:
//
//   - `intent && (claimed || unclaimUnconfirmed)`, per sequence. finalizeDrainDelivery
//     clears unclaimUnconfirmed in its saveLanded arm BEFORE sending the source
//     acknowledgement, and that send is fire-and-forget. So a finalized intent whose source
//     ack was lost was adoptable, and the live path cannot rebuild the suppression: PUBREC
//     already removed the source's cpending link, so the re-offer allocates a DIFFERENT
//     identifier and sends the payload of an exchange the client has answered.
//   - The same test hoisted to drain level — an intent anywhere plus any outstanding
//     sequence. It fails at the LAST outstanding sequence: the claim removal lands, the
//     guard clears, endRecordTx releases before the ack and the teardown, and a
//     re-SUBSCRIBE in that gap sees nothing outstanding. Those flags describe claim-removal
//     DURABILITY, not whether the retained consumer can still re-offer.
//
// What the hold establishes is exactly one thing: while an intent-bearing drain exists, no
// adoption and no destructive fork-repair row can erase that intent. It is deliberately NOT
// the claim that nothing can re-offer the source. What excludes a re-offer is retirement's
// own delivery barrier and its classified disposal (see retireDrainLocked and
// disposeRetiredDrainConsumer), which are a separate mechanism from this hold; F-094 does
// not supply them. What remains is stated in docs/CONFORMANCE.md: disposal is confirmed for
// this session object, but an indeterminate delete is reclaimed by the orphan sweep rather
// than retried, and a fork sibling can rebuild the drain on a fresh object.
//
// Session lock held on entry.
func (sess *mqttSession) drainSidHoldLocked(sid string) mqttDrainHold {
	d := sess.drains[sid]
	if d == nil {
		return mqttDrainNotHeld
	}
	hold := mqttDrainNotHeld
	for _, st := range d.seqs {
		if st == nil {
			continue
		}
		if st.ownerUnusable {
			// The stronger condition, and the one naming an operator action, so it wins
			// outright rather than depending on map traversal order.
			return mqttDrainHeldUnusableIdentity
		}
		if st.intent {
			hold = mqttDrainHeldPastPubRecIntent
		}
	}
	return hold
}

// drainRetirableLocked reports whether a drain owes nothing further, so retiring it takes
// nothing with it.
//
// DURABLY empty, not merely unclaimed, and the difference is the whole reason this is not
// claimedCount() == 0. claimedCount counts st.claimed alone, so a sequence mid-saveMaybeLanded
// — claimed already false, unclaimUnconfirmed still true — reads as gone while the RECORD may
// still carry its claim. Retiring there would tear down the durable on a removal whose
// outcome is unknown, which is I1 read backwards.
//
// sess.mu must be held.
func (d *mqttDrain) retirable() bool {
	for _, st := range d.seqs {
		if st != nil && (st.claimed || st.unclaimUnconfirmed) {
			return false
		}
	}
	return true
}

// reclaimDrainDeliveryLocked gives a restored drain the delivery subscription its
// resume could not install, and adopts it, so a re-SUBSCRIBE ends with a consumer
// that actually delivers.
//
// Without it a re-SUBSCRIBE over such a drain is answered SUBACK SUCCESS and then
// delivers nothing for the rest of the connection: adoption moves the durable into
// sess.cons, and processJSConsumer's existing-consumer arm reads that entry as proof
// a delivery subscription exists. Two resumes leave a drain in that state — the
// blanket QoS 2 withholding when PUBREL setup fails, and a restore whose own
// processQOS12Sub failed.
//
// It installs with the SUBSCRIBE'S OWN terms and adopts in this same critical
// section, and both halves of that are load-bearing.
//
// Installing with the DRAIN's terms and letting processJSConsumer's refresh correct
// them afterwards is WRONG, because that refresh runs after this lock is released:
// the callback can select with the old terms against a backlog this very call made
// available. A drain carrying noLocal=true, an unclaimed pending message from this
// session, and a re-SUBSCRIBE asking noLocal=false is enough — the callback acks
// that message away where the unrepaired broker leaves it pending. There is no
// window to close if the terms are right at install.
//
// Adopting HERE rather than leaving it to processJSConsumer keeps the durable from
// being observable as neither a drain nor a consumer.
//
// It deliberately does NOT register drainByDur first. restoreDrains does, and is
// right to, because the drain it installs for STAYS a drain. Here the drain is about
// to stop being one, so the entry would be REDUNDANT: under this one lock nothing can
// observe it between the registration and the adoption that removes it again, which
// is the same fact that makes the adoption safe. A review round checked every
// install-time reader and found none that consults the reverse index.
//
// What it does NOT establish: that a failed filter hands the drain back — adoption keeps
// its existing behavior, and the durable an adopted-then-failed filter strands is filed.
// (This also named the already-started QoS 2 exchange retransmitted at QoS 1 under a QoS 1
// re-SUBSCRIBE; that residual is CLOSED — an exchange's QoS is now carried, not
// recomputed.) Stated in docs/CONFORMANCE.md.
//
// sess.mu must be held; the caller releases it before processJSConsumer, which
// acquires it itself.
func (sess *mqttSession) reclaimDrainDeliveryLocked(c *client, subject, sid string,
	qos byte, noLocal, retainAsPublished bool, subID int) (*mqttDrain, error) {

	d := sess.drains[sid]
	if d == nil || d.cc == nil {
		return nil, nil
	}
	// The helper's OWN refusal, not a copy of adoptDrainLocked's: it runs BEFORE that
	// one, so nothing else covers it, and installing interest for a held drain would
	// hand its filter delivery the hold exists to withhold (F-094).
	if sess.drainSidHoldLocked(sid) != mqttDrainNotHeld {
		return nil, nil
	}
	// Already delivering. Nothing to reclaim, and adoption stays exactly where it is
	// for every healthy drain — this acts on the defective state alone.
	//
	// The test is the SUBSCRIPTION, not the reverse index, and a test that drove the
	// real producer proved why: restoreDrainLocked registers drainByDur the moment the
	// record is restored, BEFORE any subscription exists, and the blanket QoS 2
	// withholding then leaves that entry in place while installing nothing. Keying on
	// the index reads such a drain as healthy and declines to repair the one state
	// this helper exists for. The index says a drain is known; only the subscription
	// says it can deliver.
	//
	// c.mu inside sess.mu is the order closeSharedDeliveryLocked already uses.
	inbox := d.cc.DeliverSubject
	c.mu.Lock()
	existing := c.subs[inbox]
	c.mu.Unlock()
	if existing != nil {
		return nil, nil
	}
	// isMQTTReservedSubscription reads the FILTER, never the "$MQTT.sub.*" deliver
	// subject, and the flag is set once at install and never refreshed — so deriving
	// it from the filter is required rather than tidy. restoreDrains derives it the
	// same way.
	if _, err := sess.processQOS12Sub(c, []byte(inbox), []byte(inbox), nil,
		isMQTTReservedSubscription(subject), qos, noLocal, retainAsPublished, false, subID,
		d.cc.Durable, mqttDeliverMsgCbQoS12); err != nil {
		// Nothing to roll back: no map was touched, and processSub unsubscribes its own
		// NATS interest before c.subs[sid] is assigned, so no subscription survives.
		return nil, err
	}
	sess.adoptDrainLocked(sid)
	return d, nil
}

func (sess *mqttSession) adoptDrainLocked(sid string) bool {
	d := sess.drains[sid]
	if d == nil {
		return false
	}
	// A HELD sequence REFUSES adoption, and a review found why that matters: this is
	// reached by any ordinary QoS>0 re-SUBSCRIBE (and again by fork repair), and it removes
	// the drain from both maps and turns its consumer back into a plain membership. That
	// erased the intent by routine client behaviour — a successful save then omits it
	// entirely, so a stored PUBREL could take the ordinary non-drain path while the
	// unacknowledged source, no longer suppressed by any drain, is re-offered at AckWait and
	// sent as a FRESH PUBLISH of an exchange the client has already answered (F-094).
	//
	// Refusing keeps the drain, its consumer and its persisted intent exactly where they
	// are. The caller reads false as "no drain to adopt" and provisions afresh, which is
	// what it already does for a filter that never had one — so the refusal costs a
	// consumer, not correctness.
	if sess.drainSidHoldLocked(sid) != mqttDrainNotHeld {
		return false
	}
	delete(sess.drains, sid)
	delete(sess.drainByDur, d.cc.Durable)
	if len(sess.drains) == 0 {
		sess.drains = nil
	}
	if len(sess.drainByDur) == 0 {
		sess.drainByDur = nil
	}
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = d.cc
	return true
}

// mqttTestDrainingAllowedV counts calls to drainingAllowed, so a test can pin that the
// precondition is evaluated once per PACKET rather than once per filter. Nil in production.
var mqttTestDrainingAllowedV atomic.Pointer[func()]

func setMQTTTestDrainingAllowedHook(h func()) func() {
	prev := mqttTestDrainingAllowedV.Load()
	if h == nil {
		mqttTestDrainingAllowedV.Store(nil)
	} else {
		mqttTestDrainingAllowedV.Store(&h)
	}
	return func() { mqttTestDrainingAllowedV.Store(prev) }
}

// drainingAllowed reports whether a drain may be created at all.
//
// A drain must outlive an offline session, so a stream-level
// ConsumerLimits.InactiveThreshold is disqualifying: it gives every consumer a
// delete timer the consumer cannot opt out of, and reaping a drained durable
// takes its started deliveries with it (the messages stream is
// interest-retained). Where one is set — or where it cannot be verified — the
// the calling operation — an UNSUBSCRIBE, or a QoS 0 re-SUBSCRIBE — falls back to the
// immediate teardown and says so, rather than creating a drain that will be silently
// destroyed.
//
// Mirrors the identical precondition shared subscriptions already enforce.
//
// No lock held on entry.
func (sess *mqttSession) drainingAllowed(c *client) bool {
	// Test seam, fired on ENTRY: it counts eligibility evaluations specifically, which is
	// what "once per packet" is a claim about. Counting stream lookups in general would
	// not discriminate, because provisioning makes its own. Nil in production.
	if h := mqttTestDrainingAllowedV.Load(); h != nil {
		(*h)()
	}
	si, err := sess.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		c.Warnf("MQTT: cannot verify the messages stream's consumer limits (%v); "+
			"UNSUBSCRIBE and a QoS 0 re-SUBSCRIBE will drop started QoS 1/2 deliveries as before "+
			"([MQTT-3.10.4-3] and [MQTT-3.8.4-4] not honored)", err)
		return false
	}
	if si.Config.ConsumerLimits.InactiveThreshold != 0 {
		c.Warnf("MQTT: the messages stream imposes a consumer inactivity limit, so a drained consumer " +
			"could be reaped with its messages; UNSUBSCRIBE and a QoS 0 re-SUBSCRIBE will drop " +
			"started QoS 1/2 deliveries as before ([MQTT-3.10.4-3] and [MQTT-3.8.4-4] not honored)")
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// A QoS 0 re-SUBSCRIBE converts its consumer into a drain
// ---------------------------------------------------------------------------

// mqttDowngradeConversion is what one SUBSCRIBE packet's conversion produced, carried from
// the final-update boundary to the confirmation (or to the rollback) as one value.
//
// The drains are tracked INDIVIDUALLY rather than being re-derived from sess.drains,
// because the rollback owes a per-drain disposition: a consumer this packet CREATED and
// then converted has left sess.cons, so rollbackSubscribePacket's enumeration cannot see
// it, and the un-convert has to name exactly what to put back.
type mqttDowngradeConversion struct {
	drains []mqttConvertedDrain
	// cancels are the tombstones the FALLBACK arm produced — a candidate stageDrain
	// refused, whose registrations are voided into this same record. They join the
	// packet's own staged cancellations.
	cancels []mqttRegCancellation
	// voided are the transmitted-register verdicts that fallback recorded, so a rollback
	// can withdraw them exactly as it withdraws a staging's.
	voided []string
}

// mqttConvertedDrain pairs a converted drain with the delivery subscription its STAGING
// recorded. The pointer is the identity the completion reopens against: reopening whatever
// currently occupies the deliver subject would be a write against an object another owner
// installed, and the drain maps cannot answer that question — they key on the durable, not
// on the subscription.
type mqttConvertedDrain struct {
	d *mqttDrain
	// sub is what stageDeferredDeleteLocked closed. nil when there was nothing to close —
	// including the case where the subscription was ALREADY closed — and that is not an
	// invitation to reopen whatever is installed instead: the completion requires exact
	// equality, so a nil capture reopens nothing.
	sub *subscription
}

func (conv mqttDowngradeConversion) any() bool {
	return len(conv.drains) > 0 || len(conv.cancels) > 0 || len(conv.voided) > 0
}

// convertDowngradeCandidatesLocked turns this packet's eligible QoS 0 downgrades into
// drains, so the record the caller is about to write names each durable in Drains instead
// of describing a consumer the packet then deletes.
//
// Runs inside updateWithDetailConverting's critical section: the membership is already
// applied, nothing has been saved yet, and the packet's record transaction excludes every
// other local writer. So the downgraded membership and the drain that replaces the consumer
// become durable together or not at all.
//
// THE WITHHELD MARKERS ARE TRANSFERRED AROUND stageDrain, and that is the subtle part.
// stageDrain owns sess.deferred for its durable: it drops every entry (I5 — a withheld
// delivery was never transmitted, so it is not drained) and re-installs only the
// cancellations it discovered itself in cpending. Two kinds of live marker are therefore
// invisible to it and would be destroyed:
//
//   - one this packet's own staging cancelled, whose sequence is already out of cpending;
//   - one for a delivery withheld at the in-flight CAP, which never had a registration at
//     all. HEAD's downgrade teardown performs no I5 drop, so losing this one would be a
//     regression introduced by the conversion rather than an inherited drain behavior.
//
// A marker is what makes a later FIRST transmission carry DUP=0 ([MQTT-3.3.1-1]), since
// trackPublish otherwise derives dup from JetStream's delivery count. So the durable's LIVE
// marker set is snapshotted before the call and re-installed after it — the live map,
// never a descriptor's history, because history is neither complete (the cap case) nor
// attributable to the converting occurrence (a packet carrying the same filter at 0, then
// >0, then 0 leaves the marker on a REVOKED descriptor), and because a marker already
// CONSUMED is absent from the live map and so cannot be wrongly resurrected.
//
// The re-install is UNCONDITIONAL, whatever stageDrain returned: it has three refusal
// sites and one of them is after the drop. That site has no production producer — a
// callback holds sess.mu from its closed check through trackPublish, so no fresh
// allocation lands after staging — so the unconditionality is a helper contract rather than
// a live route, and its test says so.
//
// sess.mu must be held, and the caller must not have saved since the membership was applied.
func (sess *mqttSession) convertDowngradeCandidatesLocked(c *client, dels []*mqttDeferredDel) mqttDowngradeConversion {
	var conv mqttDowngradeConversion
	for _, dd := range dels {
		if dd == nil || !dd.convert || dd.revoked || dd.converted || dd.cc == nil {
			continue
		}
		// The entry must still be the one this packet staged, and the membership this
		// update just applied must actually be QoS 0. Read with the two-value form: a
		// MISSING key also reads as zero, and a missing key is a removal, not a
		// downgrade. Defensive rather than reachable — a revoked occurrence clears
		// convert — but a wrongful conversion destroys a live subscription's delivery.
		if cur, ok := sess.cons[dd.sid]; !ok || cur != dd.cc {
			continue
		}
		key := dd.sid
		if strings.HasSuffix(key, mqttMultiLevelSidSuffix) {
			key = strings.TrimSuffix(key, mqttMultiLevelSidSuffix) + ".>"
		}
		if qos, ok := sess.subs[key]; !ok || qos != 0 {
			continue
		}

		marks := make(map[mqttDeferredKey]string)
		for k, v := range sess.deferred {
			if k.jsDur == dd.cc.Durable {
				marks[k] = v
			}
		}
		// c.mu inside sess.mu is the order closeSharedDeliveryLocked already uses. The
		// subscription is still installed and still closed; stageDrain reads this
		// packet's PRE-downgrade terms off it, which are the terms the drain owes.
		c.mu.Lock()
		sub := c.subs[dd.cc.DeliverSubject]
		c.mu.Unlock()

		staged := sess.stageDrain(dd.sid, dd.cc, sub)

		for k, v := range marks {
			if sess.deferred == nil {
				sess.deferred = make(map[mqttDeferredKey]string)
			}
			sess.deferred[k] = v
		}
		if !staged {
			// Nothing provably started, or a held drain owns this sid. The ordinary
			// destructive teardown stands, and its transmitted-register verdicts belong
			// in THIS record — the one that removes the membership.
			cancels, voided := sess.voidRegistrationsLocked(dd.cc.Durable)
			conv.cancels = append(conv.cancels, cancels...)
			conv.voided = append(conv.voided, voided...)
			dd.voided = append(dd.voided, voided...)
			continue
		}
		d := sess.drains[dd.sid]
		if d == nil {
			continue // unreachable: stageDrain filed it under this sid
		}
		// The drain now OWNS the markers, which is what retires them: commitDrains drops
		// them once the staging is settled, and a rollback leaves them standing because
		// the first transmission they protect is still owed.
		if d.cancelled == nil {
			d.cancelled = make(map[mqttDeferredKey]string, len(marks))
		}
		for k, v := range marks {
			d.cancelled[k] = v
		}
		dd.converted = true
		// dd.closed is the subscription this packet's staging closed — the identity the
		// completion must reopen against, carried rather than re-derived.
		conv.drains = append(conv.drains, mqttConvertedDrain{d: d, sub: dd.closed})
		mqttTestSubPauseHook("converted:" + dd.sid)
	}
	return conv
}

// finishDowngradeDrains is everything a CONFIRMED save owes this packet's conversions, and
// it is one operation because the confirmation arrives two ways: inline, or later from the
// resolver proving an ambiguous write durable. Registered with retainOnLanded in the second
// case, which is why every record write it could make takes the CALLER's token.
//
// It deliberately does NOT secure the cancellation tombstones. That matches the path it
// replaces — the inline arm secures them through the packet's own
// secureRegCancellationsClassified and the ambiguous arm already returns before it — and it
// is what keeps this continuation unable to re-arm uncertainty: securing them can reach
// proveVoidedRegs, which saves. What discharges them instead is the record itself, which
// already carries the nonces: the landed credit releases settled reservations, a late store
// settling takes confirmInflightRegister's cancelled branch, and recoverInflightRegisters
// reaps a surviving nonce on resume.
//
// No lock held on entry.
func (sess *mqttSession) finishDowngradeDrains(c *client, conv mqttDowngradeConversion) {
	if len(conv.drains) == 0 {
		return
	}
	sess.mu.Lock()
	// Whole-session by construction, and that is sound HERE as a precondition rather than
	// as an assumption: the session is serialized by its locked-map entry and every other
	// stager commits or rolls back inside its own packet, so this packet's drains are the
	// only staged ones outstanding.
	timed, withdrawn := sess.commitDrains()
	// Delivery was closed at staging so nothing would allocate behind the snapshot. A
	// committed drain must deliver again — the QoS 1/2 callback tests sub.mqtt.closed
	// BEFORE it looks for a drain, so a drain whose subscription stays closed can never
	// retransmit and its exchanges are stranded for the connection.
	//
	// Validated, in the same critical section as the check: only the object this
	// conversion staged, only while it is still filed in both maps, and never a retired
	// one — retirement has delete-requested the durable, so there is nothing to reopen and
	// nothing that reclaims it later.
	for _, cd := range conv.drains {
		d := cd.d
		if d == nil || d.cc == nil || d.retired {
			continue
		}
		sid, ok := sess.drainSidForDurable(d.cc.Durable)
		if !ok || sess.drains[sid] != d || sess.drainByDur[d.cc.Durable] != d {
			continue
		}
		c.mu.Lock()
		sub := c.subs[d.cc.DeliverSubject]
		c.mu.Unlock()
		if sub == nil {
			// The client is DETACHED: an ambiguous write can stay unresolved while its
			// connection closes, and the resident pass then resolves it and runs this
			// continuation against a client whose subs are gone. Normal, not an anomaly —
			// nothing may touch a detached object, and the surviving drain takes its
			// source interest from the ordinary resume instead.
			continue
		}
		if sub != cd.sub {
			// Not the object this packet's staging closed. EXACT equality, with no
			// exemption for a nil capture: closeSharedDeliveryLocked returns nil for a
			// subscription that was ALREADY closed as well as for an absent one, so a nil
			// capture is not evidence that this packet owns whatever is installed now —
			// it is evidence that this packet closed nothing. Reopening on it would open
			// an object this packet never closed and does not own; the only installers
			// here are the reclaim and the resume phase, neither of which runs between
			// this conversion and its confirmation, so this is loud rather than silent.
			c.Warnf("MQTT: not reopening delivery for drained consumer %q of client %q: the "+
				"delivery subscription on %q was replaced since the downgrade staged it",
				d.cc.Durable, sess.id, d.cc.DeliverSubject)
			continue
		}
		if sub.mqtt == nil {
			continue
		}
		sess.subsMu.Lock()
		sub.mqtt.closed = false
		sess.subsMu.Unlock()
	}
	sess.mu.Unlock()
	// The withdrawn never-transmitted pairs (N-7.7), deleted now that the save carrying
	// their verdicts is confirmed. Stream-only ladder: the record already holds the nonces,
	// and this continuation must not save (see above).
	sess.voidRegistersOnStream(c, withdrawn)
	// A drain must outlive a disconnect, so it cannot keep the per-consumer
	// InactiveThreshold an ordinary subscription is created with. Best effort, and it says
	// so: until the update lands the durable can still be reaped with its messages.
	sess.clearDrainInactivityTimers(c, timed)
}

// unconvertDowngradeDrainsLocked puts this packet's converted consumers back, and it must
// run BEFORE rollbackSubscribePacket enumerates.
//
// That ordering is the whole point. The rollback discovers the consumers this packet
// CREATED by walking sess.cons and skipping the sids its pre-packet snapshot holds. A
// consumer created by an earlier occurrence and converted by a later one has left that map,
// so it would be classified as nothing at all: not restored, not torn down, and its durable
// left named nowhere once the rolled-back record is written. Putting it back first is what
// lets the existing classification see it — as pre-packet, as packet-created, or as an
// adopted drain — and act correctly for each.
//
// REACHABLE, and the barrier does not prevent it — a claim to the contrary was made here
// and refuted. mqttSubsBarrier.wait blocks only until every SUBSCRIBE *in flight when it
// sampled the barrier* finishes, and SUBSCRIBEs entering afterwards are deliberately not
// waited for. So a publisher that sampled the barrier before this packet started is never
// blocked by it, and its store can land after the packet's first occurrence creates its
// DeliverNew consumer — giving that consumer a transmitted delivery for a later occurrence
// to convert. TestMQTTQoS0DowngradeRollbackReleasesAPacketCreatedDurable forces exactly
// that schedule and fails without this call.
//
// Scoped to THIS conversion's drains rather than every staged one: on this path they are
// the same set, and naming them makes the disposition provable per drain instead of resting
// on "nothing else staged concurrently".
//
// sess.mu must be held.
func (sess *mqttSession) unconvertDowngradeDrainsLocked(conv mqttDowngradeConversion) {
	for _, cd := range conv.drains {
		d := cd.d
		if d == nil || d.cc == nil {
			continue
		}
		sid, ok := sess.drainSidForDurable(d.cc.Durable)
		if !ok || sess.drains[sid] != d {
			continue
		}
		sess.dropDrainLocked(d)
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[sid] = d.cc
	}
}

// releaseDowngradedConsumersLocked takes the DESTROYED downgrades out of sess.cons, so the
// record about to be written does not name a durable this packet is about to delete.
// Reports whether it released anything.
//
// Runs at the same boundary as the conversion and for the same reason. A downgrade's
// membership becomes QoS 0 only in this critical section, and the consumer reference is the
// other half of that one fact: a record carrying the membership while still naming the
// consumer describes a state the packet has already decided against, and three recovery
// surfaces read it as live — CONNECT restore hands it to processJSConsumer's
// existing-consumer arm, fork repair's per-sid table reads it as a live consumer, and
// clear()'s teardown marker offers its durable to whichever broker adopts the teardown.
//
// It is a SEPARATE pass rather than an arm of the conversion, because the conversion
// deliberately skips a descriptor whose stream forbids draining (dd.convert false) and that
// is exactly the population that must be released here.
//
// WHAT IT DOES NOT TOUCH, and each exclusion is the whole point:
//
//   - A CONVERTED descriptor. Its durable is a drain now, named in the record's Drains, and
//     stageDrain already removed the cons entry.
//   - A REVOKED one. A later occurrence re-established QoS > 0, so the record ends naming
//     this durable and the client is being served by it.
//   - An entry that is no longer IDENTICALLY dd.cc. Defensive, as in the conversion: the
//     staging that would replace it needs the same identity, so no production producer is
//     demonstrated — but evicting a consumer another occurrence installed would destroy a
//     live subscription's delivery.
//   - A membership that is ABSENT rather than zero. A missing key is a removal, which is
//     UNSUBSCRIBE's path and already omits the consumer by the builder's own presence test.
//     Read with the two-value form so the two facts stay distinguishable.
//
// The IN-MEMORY entry is what leaves; nothing else moves. dd.cc still carries the config the
// teardown needs, so there is no second home for it, and the durable stays referenced by
// this session's own sess.cons only until this boundary — after it, until the teardown
// deletes the durable, an orphan sweep on ANY broker may delete it instead. That is
// deliberate and bounded: every descriptor reaching this pass is one whose teardown is
// already owed, so a sweep performs the packet's own decision early. The same release
// against a drained or adopted durable would be a loss, which is why those are excluded
// above and why Drains is in the record.
//
// sess.mu must be held, the membership must already be applied, and the caller must not
// have saved since.
func (sess *mqttSession) releaseDowngradedConsumersLocked(dels []*mqttDeferredDel) bool {
	var released bool
	for _, dd := range dels {
		if dd == nil || dd.revoked || dd.converted || dd.consReleased || dd.cc == nil {
			continue
		}
		if cur, ok := sess.cons[dd.sid]; !ok || cur != dd.cc {
			continue
		}
		if qos, ok := sess.subs[mqttFilterForSid(dd.sid)]; !ok || qos != 0 {
			continue
		}
		delete(sess.cons, dd.sid)
		dd.consReleased = true
		released = true
	}
	return released
}

// restoreDowngradedConsumersLocked is releaseDowngradedConsumersLocked's inverse, run on a
// DEFINITE save failure before rollbackSubscribePacket enumerates.
//
// The rollback restores sess.cons wholesale from the pre-packet snapshot, which returns
// every entry that predates the packet for free. What it cannot recover is a consumer THIS
// PACKET CREATED and then downgraded: the rollback discovers those by walking sess.cons, so
// one this boundary released would be named nowhere in the rolled-back record and released
// by nobody. Same trap, and same fix, as unconvertDowngradeDrainsLocked above.
//
// It re-inserts only where the sid is ABSENT. Recording that we removed an entry does not
// prove nothing has been installed there since, and overwriting a config another path
// installed would repoint delivery at a durable this packet is about to delete. Where it
// therefore skips, the released durable is left named by nothing and is the orphan sweep's
// — the same disposition the release itself accepts, and the right one, because whatever
// occupies that sid now is what the session is actually serving.
//
// sess.mu must be held.
func (sess *mqttSession) restoreDowngradedConsumersLocked(dels []*mqttDeferredDel) {
	for _, dd := range dels {
		if dd == nil || !dd.consReleased || dd.cc == nil {
			continue
		}
		dd.consReleased = false
		if _, ok := sess.cons[dd.sid]; ok {
			continue
		}
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[dd.sid] = dd.cc
	}
}

// mqttReleasedDowngradeDurables names the durables a released downgrade still owes a
// MaxAckPending refund for, snapshotted while the descriptors are still the packet's.
//
// `consReleased` alone is the predicate, and it is exclusive with the other outcomes by
// construction: the release skips a converted, revoked or replaced descriptor, so a set
// bit means this packet took that consumer out of `sess.cons` and owns what follows.
func mqttReleasedDowngradeDurables(dels []*mqttDeferredDel) []string {
	var durs []string
	for _, dd := range dels {
		if dd == nil || !dd.consReleased || dd.cc == nil {
			continue
		}
		durs = append(durs, dd.cc.Durable)
	}
	return durs
}

// refundReleasedDowngrades gives back what those durables were charged, and does NOTHING
// else — no `sess.cons` entry returns and no durable is deleted.
//
// It exists for the AMBIGUOUS landed save, the one arm where the teardown that would have
// refunded never runs: the packet returns before its deferred-delete loop, and the orphan
// sweep that eventually reaps the durable deletes through `as.jsa.deleteConsumer`, which
// has no session to refund. Left alone, a resident session keeps the charge for the rest of
// its life and a later subscription can be refused by the aggregate MaxAckPending ceiling —
// a documented residual until a holistic round weighed it as lost availability rather than
// lost tidiness.
//
// **Refund only, deliberately.** Restoring the `cons` entry here is the design this item
// refuted: it re-persists `Cons` under the QoS 0 membership through the builder's unchanged
// presence predicate, which is the defect. And the DELETE is not moved here either — that
// needs a JetStream call and a classified result under the resolver's fence, for a route
// whose sweep convergence is an accepted residual, while this refund is memory-only and
// idempotent (`refundConsumerChargeLocked` deletes the entry it consumes).
//
// The charge is honest to give back: the delivery subscription stayed CLOSED on this arm, so
// the orphaned durable holds no in-flight budget the client can use.
//
// sess.mu must NOT be held: the resolver runs its continuations with the mutex released.
func (sess *mqttSession) refundReleasedDowngrades(durs []string) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, dur := range durs {
		sess.refundConsumerChargeLocked(dur)
	}
}
