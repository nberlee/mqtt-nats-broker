// The UNSUBSCRIBE drain.
//
// [MQTT-3.10.4-3] requires the server to complete the delivery of any QoS 1 or 2
// message it has already STARTED sending, even after the client unsubscribes.
// UNSUBSCRIBE used to tear the plain consumer down immediately: it marked the
// delivery subscription closed, dropped cpending/pendingPublish, and deleted the
// durable. $MQTT_msgs is interest-retained, so deleting the durable deleted the
// messages it still held — a started PUBLISH was simply lost.
//
// A drain is what that teardown becomes when started deliveries remain. The
// durable and its delivery subscription SURVIVE the UNSUBSCRIBE; the deliveries
// that had already been transmitted keep being redelivered and completed; fresh
// messages on the filter are discarded. The drain is persisted with the session,
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
// It is deliberately NOT written to the record's Cons map. An older broker
// restoring it would find cons[sid] on a re-SUBSCRIBE, take the fromSubProto early
// return, and never create a delivery subscription — so the filter would look
// subscribed while nothing delivered it.
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
	// Cleared by the readback, which either restores the claim or confirms it gone.
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
	// regSeq is this delivery's in-flight register (see mqttPersistedDrainSeq.RegSeq).
	// The drain owns it from staging until the exchange completes, and releases it there.
	regSeq uint64
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

	// cancelled names the sequences whose in-flight registration this staging cancelled,
	// each with the withheld marker installed for it. A ROLLBACK keeps those markers, so a
	// later first transmission still carries DUP=0; a COMMIT drops them, because the
	// consumer is gone by then and nothing will ever re-offer the sequence. Staging state
	// only, never persisted.
	cancelled map[mqttDeferredKey]string

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
		d.seqs[s.Seq] = &drainSeq{
			claimed: true, intent: s.owesPubRel(), pi: s.PI, attempt: s.Attempt, regSeq: s.RegSeq,
		}
	}
	return d
}

// owesPubRel reports whether the record says this sequence's exchange is past
// PUBREC, so its payload must never be resent and its PUBREL must be recreated if
// missing. Derived from the PERSISTED intent, never from the absence of evidence.
func (s mqttPersistedDrainSeq) owesPubRel() bool { return s.PubRel && s.PI != 0 }

// ---------------------------------------------------------------------------
// State transitions
// ---------------------------------------------------------------------------

// stageDrain converts a consumer that is being unsubscribed into a drain, when —
// and only when — it still holds started deliveries.
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
			// purged the consumer and its tracking, leaving the client holding an exchange
			// nothing on this side remembers. Removing the entry hands the decision to
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
			// The handoff above is in-memory only, and this packet is about to persist its
			// removal and delete the consumer while that store may still be on the wire: if
			// the CAS lands and this broker dies before the confirmation runs, nothing
			// written anywhere says the delivery was never transmitted. Being inside this
			// packet's own record transaction is what makes it durable before the consumer
			// goes. See cancelRegPendingLocked.
			sess.cancelRegPendingLocked(ack)
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
	// DUP=0 if this staging is rolled back. A COMMITTED teardown drops them again, in
	// commitDrains for a staged drain and in purgeConsumerPendingLocked otherwise, since
	// the consumer is gone by then and nothing will ever re-offer the sequence.
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
		return false // nothing provably started; ordinary teardown
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
// It returns the drains whose durable still carries a per-consumer
// InactiveThreshold, for clearDrainInactivityTimers to deal with outside the lock.
//
// sess.mu must be held.
func (sess *mqttSession) commitDrains() []*mqttDrain {
	var timed []*mqttDrain
	for _, d := range sess.drains {
		if !d.staged {
			continue
		}
		d.staged = false
		// The teardown is committed, so the cancelled sequences' consumer is gone and
		// nothing will ever re-offer them: their withheld markers have no first transmission
		// left to protect and would otherwise linger. Only a rollback keeps them.
		for k := range d.cancelled {
			delete(sess.deferred, k)
		}
		d.cancelled = nil
		if d.cc != nil && d.cc.InactiveThreshold != 0 {
			timed = append(timed, d)
		}
	}
	return timed
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
// sess.mu must be held.
func (sess *mqttSession) rollbackStagedDrains() {
	for sid, d := range sess.drains {
		if !d.staged {
			continue
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

// restoreDrainLocked puts a dropped drain back under its original sid, undoing
// dropDrainLocked when the save that would have made the drop durable failed.
//
// sess.mu must be held.
func (sess *mqttSession) restoreDrainLocked(sid string, d *mqttDrain) {
	if sid == _EMPTY_ || d == nil || d.cc == nil {
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
	ack, regSeq := st.ack, st.regSeq
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
	if regSeq != 0 && sess.asm != nil {
		// The CONFIRMED helper, which retries a genuine failure and treats an
		// already-absent sequence as SUCCESS. That second part is load-bearing: if the
		// delete lands and this broker dies before persisting the claim removal, recovery
		// restores the record's old reg_seq with no register left to repair it from, and a
		// retry that read "sequence not found" as a failure would withhold this exchange's
		// completion forever.
		if err := sess.asm.deleteInflightRegisterConfirmed(regSeq); err != nil {
			c.Warnf("MQTT: withholding the completion of drained sequence %d for client %q: "+
				"its in-flight register could not be released: %v", sseq, sess.id, err)
			return true
		}
		sess.mu.Lock()
		if cur := d.seqs[sseq]; cur != nil {
			cur.regSeq = 0 // released; a retry must not delete a sequence again
		}
		sess.mu.Unlock()
	}

	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		return true // fence busy: stays claimed and unacked, retried on redelivery
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
		// The removal MAY be durable, and the guard is already set. Hand the readback
		// BOTH continuations: undo the removal if the write never landed, or — if it
		// did — clear the guard and perform the ack and teardown that were withheld
		// only for want of proof. Registering just the undo would leave a landed
		// removal permanently unacked and its drain never torn down.
		sess.retainOnUncertain(
			func(recordTx) {
				sess.mu.Lock()
				st.claimed = true
				st.unclaimUnconfirmed = false
				sess.mu.Unlock()
			},
			func(recordTx) {
				sess.mu.Lock()
				st.unclaimUnconfirmed = false
				ackNow := st.ack
				sess.mu.Unlock()
				if ackNow != _EMPTY_ {
					sess.jsa.sendAck(ackNow)
				}
				sess.finishDrainIfEmpty(c, d)
			},
		)
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

// finishDrainIfEmpty tears a drain down once it claims nothing: the record entry
// is already gone — buildPersistedSessionLocked omits a drain that claims nothing,
// so the very save that cleared the last claim also removed the entry — which is
// what makes the durable unreferenced before it is deleted (I3).
//
// Deleting the durable also discards any still-unacked copy of an
// already-finalized sequence. That is intended: those exchanges are complete, or
// are completing through their own stored PUBREL.
//
// No lock held on entry.
func (sess *mqttSession) finishDrainIfEmpty(c *client, d *mqttDrain) {
	sess.mu.Lock()
	if d.claimedCount() > 0 {
		sess.mu.Unlock()
		return
	}
	if sess.drainByDur[d.cc.Durable] != d {
		sess.mu.Unlock()
		return // already torn down, or replaced by an adoption
	}
	sess.dropDrainLocked(d)
	if d.charged {
		sess.refundConsumerChargeLocked(d.cc.Durable)
	}
	deliverSubject := d.cc.DeliverSubject
	sess.mu.Unlock()

	if c != nil && deliverSubject != _EMPTY_ {
		if err := c.processUnsub([]byte(deliverSubject)); err != nil {
			c.Errorf("MQTT: error unsubscribing from a finished drain on %q: %v", deliverSubject, err)
		}
	}
	if _, err := sess.jsa.deleteConsumer(mqttStreamName, d.cc.Durable, true); isErrorOtherThan(err, JSConsumerNotFoundErr) && c != nil {
		c.Errorf("MQTT: unable to delete the finished drain consumer %q: %v", d.cc.Durable, err)
	}
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
func (sess *mqttSession) qos2DrainDurables() []string {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	var out []string
	for _, d := range sess.drains {
		if d.qos >= 2 && d.cc != nil {
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
		if d.qos >= 2 {
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
	sess.drains = make(map[string]*mqttDrain, len(ps.Drains))
	sess.drainByDur = make(map[string]*mqttDrain, len(ps.Drains))
	for sid, pd := range ps.Drains {
		d := mqttDrainFromPersisted(pd)
		if d == nil {
			continue
		}
		sess.drains[sid] = d
		sess.drainByDur[d.cc.Durable] = d
	}
	if len(sess.drains) == 0 {
		sess.drains, sess.drainByDur = nil, nil
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
		if skip[d.cc.Durable] {
			// The PUBREL recovery could not resolve this drain, so establishing
			// source interest could resend the payload of an exchange already past
			// PUBREC. Withhold it for this connection; the next resume retries.
			c.Warnf("MQTT: withholding source interest for drained consumer %q: its PUBREL state is unresolved", d.cc.Durable)
			continue
		}
		sess.mu.Lock()
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
	clearGuards := func() {
		sess.mu.Lock()
		for i := range plans {
			p := &plans[i]
			for _, seq := range p.unclaim {
				if st := p.d.seqs[seq]; st != nil {
					st.unclaimUnconfirmed = false
				}
			}
		}
		sess.mu.Unlock()
	}
	switch outcome {
	case saveLanded:
		clearGuards()
	case saveDefinitelyFailed:
		// Undo exactly what was changed, still inside the transaction.
		undoPlans()
	case saveMaybeLanded:
		// The record may already carry the reconciliation, so keep it — the guards are
		// already set — and let the readback undo it all if the write never landed.
		sess.retainOnUncertain(func(recordTx) { undoPlans() }, func(recordTx) {
			// Landed: the reconciliation is durable, so clear the markers and tear
			// down whatever it emptied.
			sess.mu.Lock()
			for i := range plans {
				p := &plans[i]
				for _, seq := range p.unclaim {
					if st := p.d.seqs[seq]; st != nil {
						st.unclaimUnconfirmed = false
					}
				}
			}
			sess.mu.Unlock()
			for i := range plans {
				sess.finishDrainIfEmpty(c, plans[i].d)
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
	sess.mu.Lock()
	empty := make([]*mqttDrain, 0, len(drains))
	for _, d := range drains {
		if sess.drainByDur[d.cc.Durable] == d && d.claimedCount() == 0 {
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
func (sess *mqttSession) recordPubRelIntent(c *client, jsDur string, sseq uint64, pi uint16) (attempt uint64, definite bool, err error) {
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
	prevPhase, prevPI, prevIntent := st.phase, st.pi, st.intent
	st.phase, st.pi, st.intent = drainPubRelUncertain, pi, true
	attempt = st.attempt
	sess.mu.Unlock()

	outcome, err := sess.saveClassified(tok)
	if outcome == saveDefinitelyFailed {
		// Nothing landed, so put the session back exactly as it was — inside the
		// transaction, before any other writer can see (or persist) the intent.
		sess.rollbackPubRelIntent(d, sseq, prevPhase, prevPI, prevIntent)
	}
	if terr := sess.endRecordTx(tok); terr != nil && c != nil {
		c.Errorf("MQTT: %v", terr)
	}
	switch outcome {
	case saveLanded:
		return attempt, false, nil
	case saveMaybeLanded:
		// The intent MAY have landed. The phase and the intent STAY — resending the
		// payload would be wrong if it did land — but if the readback proves it did
		// NOT, they must go back: an intent no record authorizes would make recovery
		// recreate a PUBREL for an exchange the client never durably PUBREC'd, and
		// suppress the payload the record still says is owed (I11).
		sess.retainOnUncertain(func(recordTx) {
			sess.rollbackPubRelIntent(d, sseq, prevPhase, prevPI, prevIntent)
		}, nil)
		return attempt, false, err
	default:
		return 0, true, err
	}
}

// rollbackPubRelIntent restores a sequence's phase after an intent write that
// definitively did not land.
func (sess *mqttSession) rollbackPubRelIntent(d *mqttDrain, sseq uint64, phase drainPhase, pi uint16, intent bool) {
	sess.mu.Lock()
	if st := d.seqs[sseq]; st != nil {
		st.phase, st.pi, st.intent = phase, pi, intent
	}
	sess.mu.Unlock()
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
		// exists to escape. If the readback proves the write never landed, the
		// counter must return to what the record holds, or the next recreate stores
		// under an identity no record authorizes.
		sess.retainOnUncertain(func(recordTx) { undoBump() }, nil)
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
func (sess *mqttSession) scanPubRelRecords(pubRelSubject string) (records []mqttStoredPubRel, ok bool) {
	if pubRelSubject == _EMPTY_ {
		return nil, true
	}
	for seq := uint64(1); ; {
		smsg, err := sess.jsa.loadNextMsgFromSeq(mqttOutStreamName, pubRelSubject, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return records, false
			}
			return records, true
		}
		seq = smsg.Sequence + 1
		r := mqttStoredPubRel{seq: smsg.Sequence, pi: mqttParsePubRelNATSHeader(smsg.Header)}
		if corr := mqttParsePubRelCorrelation(smsg.Header); corr.Ok {
			r.srcDur, r.srcSeq = corr.SrcDur, corr.SrcSeq
		}
		records = append(records, r)
	}
}

func (sess *mqttSession) recoverDrainPubRels(c *client) map[string]bool {
	sess.mu.Lock()
	type claim struct {
		d       *mqttDrain
		seq     uint64
		pi      uint16
		attempt uint64
	}
	var claims []claim
	for _, d := range sess.drains {
		if d.qos < 2 {
			continue
		}
		for seq, st := range d.seqs {
			// Only a sequence whose INTENT is recorded is past PUBREC. One without
			// it provably never got a durable PUBREC, so its payload is redelivered
			// with DUP and the client re-PUBRECs ([MQTT-4.3.3]) — the ordinary
			// failover-before-PUBREC path, which completes rather than stalls.
			if st.claimed && st.intent && st.pi != 0 {
				claims = append(claims, claim{d, seq, st.pi, st.attempt})
			}
		}
	}
	pubRelSubject := sess.pubRelSubject
	idHash := sess.idHash
	sess.mu.Unlock()
	if len(claims) == 0 {
		return nil
	}

	// Index every PUBREL record still on this session's subject by the source it
	// names. An unreadable scan is never evidence of absence.
	records, scanned := sess.scanPubRelRecords(pubRelSubject)
	if !scanned {
		c.Errorf("MQTT: unable to scan the PUBREL records of client %q", sess.id)
	}
	found := make(map[string]uint64, len(records))
	key := mqttPubRelSourceKey
	for _, r := range records {
		found[key(r.srcDur, r.srcSeq)] = r.seq
	}

	skip := make(map[string]bool)
	if !scanned {
		for _, cl := range claims {
			skip[cl.d.cc.Durable] = true
		}
		return skip
	}

	var confirmed []claim
	for _, cl := range claims {
		if _, ok := found[key(cl.d.cc.Durable, cl.seq)]; ok {
			// Intent present, record present: the exchange is past PUBREC and its
			// PUBREL exists. The callback will forward it; the claim is finalized
			// here rather than waiting on that asynchronous delivery.
			confirmed = append(confirmed, cl)
			continue
		}
		// Intent present, NO record: the store never landed (skipped for lack of
		// interest, say) or the record was deleted. Recreate it under the recorded
		// identifier so the client completes its ORIGINAL exchange.
		//
		// An intent that is still present also proves the exchange was not
		// finalized — the callback removes claim and intent before forwarding — so a
		// missing record can never mean "already completed".
		if err := sess.recreateDrainPubRel(c, idHash, pubRelSubject, cl.d, cl.seq, cl.pi, cl.attempt); err != nil {
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
		// May be durable: keep the removals — the guards are already set — and
		// register the undo for the readback.
		batch := confirmed
		sess.retainOnUncertain(
			func(recordTx) {
				sess.mu.Lock()
				for _, cl := range batch {
					if st := cl.d.seqs[cl.seq]; st != nil {
						st.claimed = true
						st.unclaimUnconfirmed = false
					}
				}
				sess.mu.Unlock()
			},
			func(recordTx) {
				// Landed: clear the markers and discharge the acks and teardowns the
				// batch withheld for want of proof.
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
					sess.finishDrainIfEmpty(c, cl.d)
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
func (sess *mqttSession) recreateDrainPubRel(c *client, idHash, pubRelSubject string, d *mqttDrain, sseq uint64, pi uint16, attempt uint64) error {
	for i := 0; i < mqttDrainPubRelAttempts; i++ {
		msgId := mqttPubRelMsgId(idHash, d.cc.Durable, sseq, attempt)
		natsMsg, headerLen := mqttNewCorrelatedPubRel(pi, d.cc.Durable, sseq, msgId)
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
				if corr := mqttParsePubRelCorrelation(smsg.Header); corr.Ok &&
					corr.SrcDur == d.cc.Durable && corr.SrcSeq == sseq {
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
func (sess *mqttSession) adoptDrainLocked(sid string) bool {
	d := sess.drains[sid]
	if d == nil {
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

// drainingAllowed reports whether a drain may be created at all.
//
// A drain must outlive an offline session, so a stream-level
// ConsumerLimits.InactiveThreshold is disqualifying: it gives every consumer a
// delete timer the consumer cannot opt out of, and reaping a drained durable
// takes its started deliveries with it (the messages stream is
// interest-retained). Where one is set — or where it cannot be verified — the
// UNSUBSCRIBE falls back to today's immediate teardown and says so, rather than
// creating a drain that will be silently destroyed.
//
// Mirrors the identical precondition shared subscriptions already enforce.
//
// No lock held on entry.
func (sess *mqttSession) drainingAllowed(c *client) bool {
	si, err := sess.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		c.Warnf("MQTT: cannot verify the messages stream's consumer limits (%v); "+
			"UNSUBSCRIBE will drop started QoS 1/2 deliveries as before ([MQTT-3.10.4-3] not honored)", err)
		return false
	}
	if si.Config.ConsumerLimits.InactiveThreshold != 0 {
		c.Warnf("MQTT: the messages stream imposes a consumer inactivity limit, so a drained consumer " +
			"could be reaped with its messages; UNSUBSCRIBE will drop started QoS 1/2 deliveries as before " +
			"([MQTT-3.10.4-3] not honored)")
		return false
	}
	return true
}
