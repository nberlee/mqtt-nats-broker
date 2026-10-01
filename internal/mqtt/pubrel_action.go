// The durable PUBREL action claim: the mechanism that lets one broker transmit a stored
// PUBREL while every other writer refuses, and that keeps refusing THROUGH the write.
//
// # Why a re-read is not enough, restated here because this is where it is exercised
//
// Broker A reads the slot, pauses before transmitting; broker B performs the peer terminal
// release; a later exchange installs a new slot under that identifier; A resumes and
// transmits the stale PUBREL under an identifier another exchange now owns. That is
// F-092's whole population. `sendsInFlight` is process-local and fences nothing across
// brokers, so moving the read closer to the write only narrows the window.
//
// A successful CAS is different in kind: it establishes an invariant that REMAINS TRUE
// THROUGH the action. While an action is live, custody transfer, slot replacement,
// ordinary deletion, identifier release and terminal deletion all refuse — see the phase
// gate — and only the action's own holder may drain it.
//
// # Arming before the CAS, and why the order is the correctness property
//
// The tentative holder is installed and its abort armed BEFORE the CAS request can be
// issued. Otherwise a CAS that lands while the caller is unwinding — an ordinary panic, an
// early return — leaves a durable action with no local holder and therefore no drain, and
// that is the abandoned-action residual manufactured out of a recoverable failure.
//
// # There is no lease, no TTL and no steal
//
// A lease would not make this correct. A broker can pause AFTER its own expiry check, let
// the lease lapse, and resume into the same stale write; and an MQTT client cannot
// validate a fencing token, so the external effect is not fenceable downstream. An
// abandoned action therefore holds its identifier until its own holder drains it, the
// generation is discarded, or an operator intervenes — a real availability cost, stated as
// such in docs/CONFORMANCE.md rather than disguised as bounded failover.

package mqtt

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// errMQTTPubRelIntentNotRecorded aborts a PUBREC handoff whose past-PUBREC intent could
// not be made durable. It routes into the handoff's EXISTING rollback arm deliberately:
// the exchange simply restarts, which is the recoverable direction, where storing the
// PUBREL anyway would leave the crash window the intent exists to close — permanently and
// on purpose rather than by accident.
var errMQTTPubRelIntentNotRecorded = errors.New(
	"mqtt: the exchange's past-PUBREC intent could not be recorded before its PUBREL store")

// intendPubRelSlot records, DURABLY AND BEFORE THE STORE, that this exchange is past
// PUBREC and owes a canonical PUBREL record.
//
// This is F-098's R-a: the handoff used to store the PUBREL on `$MQTT_out` and re-phase
// the register only afterwards, and JetStream batches are PER STREAM, so nothing made the
// pair atomic. A crash in between left an ordinary `publish` slot — indistinguishable
// from a delivery that never reached PUBREC — which a peer may legitimately reap, freeing
// an identifier the client has already answered under.
//
// IT IS A MONOTONE PHASE, NEVER A HOLDER-EXCLUSIVE ACTION. That distinction is consult
// C1's correction to the original candidate: a holder-exclusive intent is permanently
// pinned by a crash between the CAS and the store, with no other broker able to advance
// it. A phase any custodian can carry forward is what makes the owed record
// MATERIALIZABLE by whoever next holds custody.
//
// A slot left here with no canonical record is OWED one. Materialization supplies it —
// it needs custody but no action — and `phaseSlotToPubRelReady` above is the live path's
// own materialization, reached when the store's record can be proven present.
//
// Session lock NOT held on entry.
func (sess *mqttSession) intendPubRelSlot(c *client, ack *mqttPending) (out mqttPubRelIntentOutcome) {
	// A FOREGROUND authority operation for its whole span: the retained background writers yield to
	// it (outbound_foreground_priority.go).
	release := sess.announceForegroundAuthority()
	defer release()
	defer func() { sess.finishAbandonedAckResolution(ack, out) }()
	if ack != nil && ack.slot.abandonedAck != nil && !ack.slot.abandonedAck.belongsTo(sess) {
		return mqttPubRelIntentUnresolved
	}
	if ack == nil || !ack.slot.slotOK() || ack.slot.name.subject == _EMPTY_ {
		// No captured slot identity: an uncorrelated PUBREL, or a delivery this broker
		// has no pending entry for. There is nothing to intend, and refusing the store
		// here would break a shape that predates the slot entirely.
		return mqttPubRelIntentNoSlot
	}
	if ack.slot.state == mqttInflightPubRelIntended {
		return mqttPubRelIntentConfirmed // an earlier attempt of this same PUBREC already recorded it
	}
	// WHICH RECORDS THE ANSWER MOVES is the acknowledgement class gate's question, the same one
	// the PUBACK arm asks (ackExchangeFor). An ordinary obligation stands beside its exchange,
	// and a successful PUBREC is one fact about both: the slot records the past-PUBREC intent AND
	// the exchange records the ownership transfer, in ONE batch. Moving the slot alone left the
	// exchange at PUBLISH, which recovery classifies as delivery still owed — a payload the client
	// already accepted, offered again.
	if sess.asm == nil {
		return mqttPubRelIntentUnresolved // nothing durable can be read or written
	}
	// A RETAINED exchange's canonical PUBREL is created by this advance (F-105 7c), under an
	// identity minted ONCE per decision so a re-attempt names the same record. Retained is the
	// RECORD's arm, never the entry's kind: an abandoned exchange answers through the ACK-only
	// snapshot, which is a Hold.
	retained := false
	relID := _EMPTY_
	for attempt := 0; attempt < mqttPubRecAdvanceAttempts; attempt++ {
		if attempt > 0 {
			// SPACED, and BEFORE the fresh reads, never between a read and its batch: the
			// contention this outlasts is a burst — the send's own token drain, a resuming
			// CONNECT's successive authority writes — not a steady rate.
			sess.authorityContentionWait()
		}
		cur, smsg, lerr := sess.asm.loadInflightRecord(ack.slot.name.subject)
		if lerr != nil || smsg == nil || cur == nil ||
			!mqttInflightRecordMatchesSubject(cur, ack.slot.name.subject) || !ack.slot.matchesRecord(cur) ||
			mqttValidateInflightShape(cur) != nil ||
			(cur.State != mqttInflightPublish && cur.State != mqttInflightDeliveryAbandoned) ||
			(ack.slot.abandonedAck != nil && (!ack.slot.abandonedAck.belongsTo(sess) ||
				cur.Acq != ack.slot.abandonedAck.acq)) {
			// Not an unanswered PUBLISH of this exchange as far as this read can tell — landed,
			// moved on by a peer, gone, or unreadable. The resolving re-read classifies which.
			return sess.resolvePubRelIntent(c, ack)
		}
		if cur.State == mqttInflightDeliveryAbandoned && !sess.bindAbandonedAckOrigin(ack, cur) {
			return mqttPubRelIntentOther
		}
		if retained = cur.retainedSource(); retained && relID == _EMPTY_ {
			relID = mqttNewInflightNonce()
		}
		class, x := sess.ackExchangeFor(cur, ack.slot.name.gen)
		switch class {
		case mqttAckRefused:
			// No route is licensed and nothing is written. The re-read still honours a slot a peer
			// already moved past PUBREC, so a refusal never contradicts a landed arrow.
			return sess.resolvePubRelIntent(c, ack)
		case mqttAckExchange:
			// THE PHASE IS DERIVED from the packet and the exchange's QoS, never chosen here.
			to, perr := mqttPhaseForAck(mqttAckArrival{Packet: mqttPacketPubRec, Reason: mqttReasonSuccess}, x.QoS)
			if perr != nil {
				return sess.resolvePubRelIntent(c, ack)
			}
			if _, aerr := sess.advanceExchangeAck(&mqttAdvanceExchangeAckRequest{
				ID: sess.id, Epoch: sess.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
				Gen: ack.slot.name.gen, Nonce: cur.Lin, To: to, Acq: cur.Acq,
				Register: cur, RegisterSeq: smsg.Sequence, RelID: relID,
			}); aerr != nil {
				// RE-ATTEMPTED, NOT CLASSIFIED. The authority is rewritten by every acknowledgement
				// advance and acquisition on this session — and by a resuming CONNECT's ownership
				// steps — and the slot by every acquisition, so losing a CAS is ordinary contention,
				// while a "not recorded" here rolls the PUBREC back and closes the connection. Any
				// error re-attempts; nothing here establishes that the batch was refused. The next
				// iteration's read is the authoritative re-read: a batch that landed behind a lost
				// response is found there as past PUBREC and resolved as Confirmed.
				continue
			}
			// AN ACKNOWLEDGED BATCH IS THE PROOF. Nothing is read between it and the store: a
			// failed read here would turn a durable intent into a closed connection. The counter
			// is the one the batch preserved from the read record (the arrow is not
			// counter-bound), and the sequence is refreshed best-effort — no later CAS uses it
			// as a base, each reloads by identity.
			ack.slot.state = mqttInflightPubRelIntended
			if retained {
				// The retained arrow lands on pubrel_ready with its canonical already created:
				// nothing is owed to materialize.
				ack.slot.state = mqttInflightPubRelReady
			}
			ack.slot.acq = cur.Acq
			if _, rsmsg, rerr := sess.asm.loadInflightRecord(ack.slot.name.subject); rerr == nil && rsmsg != nil {
				ack.slot.seq = rsmsg.Sequence
			}
			if !retained {
				sess.scheduleOwedMaterialization(c, &mqttOwedMaterialization{slot: ack.slot})
			}
			return mqttPubRelIntentConfirmed
		}
		return sess.intendPubRelSlotOnly(c, ack)
	}
	// EXHAUSTED. A timed-out attempt may have committed, so nothing is concluded from the count:
	// the re-read decides between landed, proven not recorded, and unknown.
	return sess.resolvePubRelIntent(c, ack)
}

// The successful-PUBREC advance's re-attempt budget, its own rather than mqttInflightStoreAttempts
// (consult 22). A PUBREC records a monotone acknowledgement under the STANDING authority, so it
// stays valid across a handoff and may legitimately outlast a resuming CONNECT's writes, where an
// acquisition authorizes new work under a pinned tenure that a handoff SHOULD defeat. And its
// exhaustion rolls the acknowledgement back and errors the connection, where an acquisition's
// leaves the delivery pending. Eight attempts spaced by 10-25 ms bound the added delay at 70-175
// ms, none on a first-attempt success; they mitigate a contention burst and guarantee nothing
// against sustained contention.
const mqttPubRecAdvanceAttempts = 8

// The AUTHORITY-CONTENTION WAIT, one definition shared by the foreground writers that re-attempt an
// authority transition against a burst of same-session writes (the PUBREC advance; the SUBSCRIBE
// name claim against the retained driver, F-105 7c consult 104). Each writer keeps its own budget.
const (
	mqttAuthorityContentionWaitMin    = 10 * time.Millisecond
	mqttAuthorityContentionWaitJitter = 15 * time.Millisecond
)

// mqttAuthorityContentionBackoff maps r in [0,1) to one contention wait.
func mqttAuthorityContentionBackoff(r float64) time.Duration {
	return mqttAuthorityContentionWaitMin + time.Duration(r*float64(mqttAuthorityContentionWaitJitter))
}

// authorityContentionWait spaces two attempts of an authority transition, independently jittered,
// BEFORE the next attempt's fresh reads, honouring an instance-scoped test seam that observes the
// requested wait instead of sleeping.
//
// Session lock NOT held on entry, and none is taken across the wait.
func (sess *mqttSession) authorityContentionWait() {
	d := mqttAuthorityContentionBackoff(rand.Float64())
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testAuthorityContentionWait
		as.mu.RUnlock()
		if hook != nil {
			hook(d)
			return
		}
	}
	time.Sleep(d)
}

// intendPubRelSlotOnly records the intent on the slot alone, for the classes that have no
// exchange (experimental affine, chapter P).
//
// Session lock NOT held on entry.
func (sess *mqttSession) intendPubRelSlotOnly(c *client, ack *mqttPending) mqttPubRelIntentOutcome {
	// THE IDENTITY THE CAS ACTUALLY WROTE, captured from the record it mutated (holistic
	// review round 6, finding 3). This arrow is deliberately NOT counter-bound — a PUBREC may
	// legitimately land on a retransmission — so the record it advances can carry a counter
	// this handler never read: a retransmission's acquisition can land between the entry's
	// capture and this CAS. Enrolling materialization at the CAPTURED counter then arms a
	// worker whose identity check fails on its first attempt, and it retires the only
	// continuation the owed slot had.
	var landedAcq uint64
	seq, ok := sess.casInflightSlot(c, ack.slot, mqttInflightPublish, mqttSlotAdvancePhase,
		func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelIntended
			landedAcq = next.Acq
		})
	if !ok {
		// "NOT RECORDED, AS FAR AS THIS CALL COULD SEE" is three different facts, and the
		// holistic review found the caller treating all of them as "the slot exists, so the
		// intent is durable" (finding 4). A reread tells them apart.
		return sess.resolvePubRelIntent(c, ack)
	}
	ack.slot.seq = seq
	ack.slot.state = mqttInflightPubRelIntended
	ack.slot.acq = landedAcq
	// ENROLLED IMMEDIATELY, by the transition that CREATED the obligation. This is half
	// of L1's liveness guarantee — the other half is recovery — and it is what stops the
	// obligation waiting on a future event that is not guaranteed to happen.
	//
	// The ordinary path discharges it within milliseconds: the store returns,
	// phaseSlotToPubRelReady installs the canonical tuple, and the first scheduled
	// attempt finds the slot no longer `pubrel_intended` and retires. The enrolment
	// costs one timer on the happy path and is the only thing standing between a failed
	// store and a permanently owed slot.
	sess.scheduleOwedMaterialization(c, &mqttOwedMaterialization{slot: ack.slot})
	return mqttPubRelIntentConfirmed
}

// mqttPubRelIntentOutcome is what recording a PUBREC's intent established, CLASSIFIED,
// because each value owes a different continuation and a slot's mere existence proves none
// of them (holistic review round 1, finding 4).
type mqttPubRelIntentOutcome uint8

const (
	// mqttPubRelIntentNoSlot: the exchange has no durable slot (a retained send, R6), so
	// nothing durable records its PUBREC. The store proceeds, and a failed store rolls
	// the exchange back — the rollback is its only continuation.
	mqttPubRelIntentNoSlot mqttPubRelIntentOutcome = iota
	// mqttPubRelIntentConfirmed: the slot is PROVEN past PUBREC under this exchange's
	// identity — written by this call, found already written, or found landed after a lost
	// response. The store proceeds; a failed store is NOT rolled back, and L1's
	// materialization is enrolled to rebuild the record.
	mqttPubRelIntentConfirmed
	// mqttPubRelIntentNotRecorded: PROVEN not recorded — the slot still reads PUBLISH for
	// this exchange, belongs to another exchange, or is gone. Nothing may be stored under a
	// slot that says PUBLISH, so the whole PUBREC rolls back.
	mqttPubRelIntentNotRecorded
	// mqttPubRelIntentUnresolved: the reread could not tell. Neither continuation is safe —
	// storing might put a record under a slot that still reads PUBLISH, rolling back might
	// contradict a durable past-PUBREC phase — so the placeholder is RETAINED and an
	// identity-bound resolver decides later (scheduleIntentResolution).
	mqttPubRelIntentUnresolved
	// The successful PUBREC is still owed; abandonment is not a rollback to PUBLISH.
	mqttPubRelIntentAbandoned
	// Positive classification of a bound allocation that ended or no longer occupies its slot.
	mqttPubRelIntentEnded
	mqttPubRelIntentOther
)

// intentRecorded reports whether ordinary PUBLISH rollback is forbidden. Durable or
// unresolved intent, abandonment, and a positive ending/successor all forbid that rollback.
func (o mqttPubRelIntentOutcome) intentRecorded() bool {
	return o == mqttPubRelIntentConfirmed || o == mqttPubRelIntentUnresolved ||
		o == mqttPubRelIntentAbandoned || o == mqttPubRelIntentEnded || o == mqttPubRelIntentOther
}

// resolvePubRelIntent rereads the slot after an intent CAS the caller could not see land.
//
// Session lock NOT held on entry.
func (sess *mqttSession) resolvePubRelIntent(c *client, ack *mqttPending) (out mqttPubRelIntentOutcome) {
	defer func() { sess.finishAbandonedAckResolution(ack, out) }()
	if sess.asm == nil || (ack.slot.abandonedAck != nil && !ack.slot.abandonedAck.belongsTo(sess)) {
		return mqttPubRelIntentUnresolved
	}
	cur, smsg, lerr := sess.asm.loadInflightRecord(ack.slot.name.subject)
	switch {
	case lerr != nil:
		return mqttPubRelIntentUnresolved
	case smsg == nil:
		if b := ack.slot.abandonedAck; b != nil {
			x, _, err := sess.jsa.loadOutboundExchange(sess.id, b.epoch, b.name.gen, b.ev.lin)
			if err == nil && b.endedBy(sess, x) {
				return mqttPubRelIntentEnded
			}
			return mqttPubRelIntentUnresolved // Missing slot is not an ended exchange.
		}
		return mqttPubRelIntentNotRecorded // the slot is gone; nothing recorded anything for us
	case cur == nil:
		return mqttPubRelIntentUnresolved // present but unreadable
	case !mqttInflightRecordMatchesSubject(cur, ack.slot.name.subject) ||
		mqttValidateInflightShape(cur) != nil:
		return mqttPubRelIntentUnresolved
	case !ack.slot.matchesRecord(cur) ||
		(ack.slot.abandonedAck != nil && !ack.slot.abandonedAck.belongsTo(sess)):
		if ack.slot.abandonedAck != nil {
			return mqttPubRelIntentOther
		}
		return mqttPubRelIntentNotRecorded
	case cur.State == mqttInflightDeliveryAbandoned:
		if !sess.bindAbandonedAckOrigin(ack, cur) {
			return mqttPubRelIntentOther
		}
		return mqttPubRelIntentAbandoned
	case ack.slot.abandonedAck != nil &&
		(cur.State == mqttInflightPublishTerminal || cur.State == mqttInflightPubRelTerminal):
		if cur.State == mqttInflightPublishTerminal {
			// A competing terminal ACK owns source settlement, not PUBREL creation.
			// Install its continuation before retiring this resolver/binding.
			ref := mqttSlotRefFromRecord(ack.slot.name.subject, smsg.Sequence, cur)
			if ref == nil {
				return mqttPubRelIntentUnresolved
			}
			if !sess.attemptTerminalRegisterRelease(c, cur.PI, *ref, false) {
				sess.scheduleTerminalRegisterRelease(c, cur.PI, *ref, false)
			}
		}
		return mqttPubRelIntentEnded
	case cur.State == mqttInflightPublish:
		if ack.slot.abandonedAck != nil {
			return mqttPubRelIntentUnresolved // An abandoned allocation cannot regress to PUBLISH.
		}
		return mqttPubRelIntentNotRecorded
	case mqttInflightPastPubRec(cur.State):
		// LANDED — this CAS after a lost response, or another path (recovery's route B,
		// admission's direct arrow) already moved the exchange past PUBREC. Either way the
		// exchange is durably past PUBREC and must not be rolled back.
		ack.slot.seq = smsg.Sequence
		ack.slot.state = cur.State
		ack.slot.acq = cur.Acq
		if cur.State == mqttInflightPubRelIntended {
			sess.scheduleOwedMaterialization(c, &mqttOwedMaterialization{slot: ack.slot})
		}
		return mqttPubRelIntentConfirmed
	}
	// A terminal phase for this exchange: it is over, and nothing is owed a PUBREL record.
	return mqttPubRelIntentNotRecorded
}

// scheduleIntentResolution retains the packet-derived obligation before dispatch. The
// server association and resolver instance survive connection retirement and admission refusal.
// Session lock not held.
func (sess *mqttSession) scheduleIntentResolution(c *client, pi uint16, removed *mqttPending) {
	if pi == 0 || removed == nil || !removed.slot.slotOK() || removed.slot.name.subject == _EMPTY_ {
		return
	}
	srv := sess.serverFor(c)
	if srv == nil {
		return // No production session lacks both a connection server and a JSA server.
	}
	key := mqttIntentResolutionKey{
		sess: sess, name: removed.slot.name, nonce: removed.slot.nonce,
		dur: removed.slot.dur, seq: removed.slot.sseq, acq: removed.slot.acq,
		lin: removed.ev.lin, qos: removed.qos,
	}
	if sess.asm != nil {
		key.epoch = sess.asm.sessStreamEpoch
	}
	srv.owedIntentsMu.Lock()
	if srv.owedIntents == nil {
		srv.owedIntents = make(map[mqttIntentResolutionKey]*mqttIntentResolution)
	}
	r := srv.owedIntents[key]
	if r == nil {
		r = &mqttIntentResolution{key: key, srv: srv, c: c, ack: *removed}
		// The exact placeholder, if any, is the only one rollback may remove.
		sess.mu.Lock()
		r.placeholder = sess.pendingPubRel[pi]
		sess.mu.Unlock()
		srv.owedIntents[key] = r
	}
	srv.owedIntentsMu.Unlock()
	r.schedule()
}

// phaseSlotToPubRelReady performs the PUBREC handoff's durable half: the named
// `publish -> pubrel_ready` arrow, writing the canonical PUBREL record's identity.
//
// This REPLACES the register deletion that used to end a register's life at PUBREC. The
// difference is the whole point of F-092: before, the identifier's authority moved from
// the register to the correlated PUBREL record on $MQTT_out, leaving it described by two
// independently changing durable sets read by two non-transactional scans, with nothing
// able to arbitrate between them. Now one record owns the exchange for its whole life.
//
// A failure here is NOT fatal to the exchange and deliberately does not roll the PUBREC
// back: the record is stored, the client has been answered, and the slot simply remains in
// `publish`. Recovery's route B finds exactly that shape — "record stored, slot still
// publish" — and completes the transition. What must never happen is the opposite, a slot
// advanced past a record that was never stored, which is why the CAS runs after the store
// rather than before it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) phaseSlotToPubRelReady(c *client, ack *mqttPending, pubRelSeq uint64) {
	if ack == nil || pubRelSeq == 0 || !ack.slot.slotOK() || ack.slot.name.subject == _EMPTY_ {
		// No canonical record to name, or no captured slot identity to address. Fail
		// closed: leaving the slot in `publish` is recoverable, where advancing it with a
		// tuple naming nothing is not.
		return
	}
	// Derive both names from the SLOT'S captured generation, never from the session
	// object's current generation. clear() reuses the object and remints sess.gen; a
	// delayed PUBREC continuation must not pair its old-generation slot with the new
	// generation's consumer durable.
	// The canonical PUBREL record is STORED on the first return (the $MQTT.out.pubrel
	// subject), NOT the second (the $MQTT.deliver.pubrel delivery subject). Reading the
	// second here made the readback subject-check below always mismatch, so this live
	// handoff silently never advanced and every exchange fell back to the direct arrow.
	pubRelSubject, _, dur := mqttPubRelNames(ack.slot.name.idHash, ack.slot.name.gen)
	if pubRelSubject == _EMPTY_ || dur == _EMPTY_ {
		return
	}
	// THE CANONICAL-ORIGIN INVARIANT (Decision 5). The canonical tuple may be written only
	// from PROOF that the exact record exists at pubRelSeq — never from the store RESPONSE.
	// On the interest-retained $MQTT_out a store with no PUBREL consumer returns success with
	// a sequence while keeping NOTHING (I12), so pubRelSeq can name a PHANTOM. Advancing to
	// pubrel_ready with a tuple that names nothing is the defect that made admission
	// misread the recreated record as a duplicate and acknowledge it away.
	//
	// So read it back and require a present, structurally matching record. Absent or
	// indeterminate: leave the slot in `publish` — the recoverable direction the "record
	// stored, slot still publish" cut already relies on — and let recovery's route B (or the
	// direct arrow at the next delivery) complete the transition once the record exists.
	smsg, lerr := sess.jsa.loadMsg(mqttOutStreamName, pubRelSeq)
	if lerr != nil || smsg == nil || smsg.Subject != pubRelSubject {
		if c != nil {
			c.Debugf("MQTT: the PUBREC handoff for packet identifier %d of client %q found no "+
				"canonical record on %q at sequence %d; leaving the slot in publish for recovery",
				ack.slot.name.pi, sess.id, pubRelSubject, pubRelSeq)
		}
		return
	}
	shape, derr := mqttDecodeStoredPubRel(smsg.Header)
	expected := &mqttInflightRecord{
		// QoS 2 is mandatory: mqttStoredPubRelShapeMatchesSlot requires it, and a PUBREL
		// exchange is QoS 2 by definition. Omitting it left the match always false, so this
		// live re-phase silently never advanced and every exchange fell back to the direct
		// arrow (the intended fast path was dead).
		PI: ack.slot.name.pi, QoS: 2, Nonce: ack.slot.nonce, Dur: ack.jsDur, Seq: ack.sseq,
		Q2AffClaim: ack.q2AffClaim, Q2AffCopy: ack.q2AffCopy,
	}
	if derr != nil || !mqttStoredPubRelShapeMatchesSlot(shape, expected) {
		// The record at that sequence is not this exchange's — a phantom sequence landed on
		// someone else's record, or the read is unusable. Do not advance the slot on it.
		if c != nil {
			c.Debugf("MQTT: the PUBREC handoff for packet identifier %d of client %q read a record "+
				"at sequence %d that does not match this exchange; leaving the slot in publish",
				ack.slot.name.pi, sess.id, pubRelSeq)
		}
		return
	}
	// FROM `pubrel_intended`, NOT FROM `publish`. The intent CAS runs before the store
	// (see intendPubRelSlot), so by the time a proven record exists the slot is already
	// past PUBREC and the only thing missing is the canonical tuple. Requiring `publish`
	// here would make this handoff lose every time and leave every exchange to recovery.
	seq, ok := sess.casInflightSlot(c, ack.slot, mqttInflightPubRelIntended, mqttSlotRebindCanonical,
		func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelReady
			next.PubRelDurable = dur
			next.PubRelStreamSeq = pubRelSeq
		})
	if !ok {
		if c != nil {
			c.Debugf("MQTT: the PUBREC handoff for packet identifier %d of client %q did not "+
				"re-phase its owner slot; recovery completes the transition from the stored record",
				ack.slot.name.pi, sess.id)
		}
		return
	}

	// CARRY THE SLOT ONTO THE PUBREL ENTRY, and this is not bookkeeping — it is what
	// makes the terminal release addressable at all.
	//
	// trackAsPubRel builds a FRESH entry for the PUBREL phase and does not inherit the
	// PUBLISH entry's captured slot reference. At HEAD that was harmless, because the
	// register's life ended here and PUBCOMP had no durable state of its own to settle.
	// Now the slot owns the exchange for its whole life, so PUBCOMP must be able to
	// address it — and without this the terminal transition silently does nothing, the
	// slot stays in `pubrel_ready` forever, and its identifier is reserved for the life of
	// the session. That is the leak the fifth attempt withdrew this transition for.
	ref := ack.slot
	ref.seq = seq
	ref.state = mqttInflightPubRelReady
	mqttRunTestSeam(&mqttTestAfterPubRelPhaseCASV)
	sess.mu.Lock()
	// A PHASE ADVANCE of this exchange's own PUBREL entry — the successor trackAsPubRel built
	// from the PUBLISH entry, already carrying its evidence — and nothing else: the resident
	// must carry this slot's nonce. A different exchange's entry is not rewritten.
	if rel := sess.pendingPubRel[ack.slot.name.pi]; rel != nil && rel.regNonce == ref.nonce {
		rel.slot.seq, rel.slot.state = ref.seq, ref.state
		rel.regSeq = seq
	}
	sess.mu.Unlock()
}

// mqttRecoveredPubRecOutcome is what recording an evidence-proven PUBREC concluded.
type mqttRecoveredPubRecOutcome uint8

const (
	// mqttRecoveredPubRecUnresolved: nothing was concluded — a read failed, the ordinary slot's
	// exchange could not be read, or every attempt was refused. The caller writes nothing more.
	mqttRecoveredPubRecUnresolved mqttRecoveredPubRecOutcome = iota
	// mqttRecoveredPubRecRecorded: the slot is past PUBREC beside an exchange past PUBLISH —
	// landed now, or already so.
	mqttRecoveredPubRecRecorded
	// mqttRecoveredPubRecNotOurs: a successor holds the identifier, or the slot is a slot-only
	// class with no exchange to record: nothing here to pair.
	mqttRecoveredPubRecNotOurs
)

// recordRecoveredPubRec records, for an ORDINARY slot still at publish, the successful PUBREC that
// durable evidence proves — a stored canonical PUBREL matching the exact occupant, or a drain
// intent naming its nonce and identifier — through the ONE PUBREC arrow: slot publish ->
// pubrel_intended and exchange Publish -> PubRecObserved in one advanceExchangeAck batch, the
// phase derived by mqttPhaseForAck. It fabricates no packet and enrols no materialization: the
// caller already holds (or is about to store) the canonical record.
//
// WHY IT EXISTS. The drained PUBREC persists its drain intent before its exchange advance, so a
// crash or takeover between the two leaves evidence of the PUBREC with the exchange still at
// PUBLISH. Every route that then interprets that evidence — recovery's route B, the drain resume,
// PUBREL admission — records the PUBREC here before it moves the slot any further, so no ordinary
// slot is ever past PUBREC beside a PUBLISH exchange (which its PUBCOMP could never end).
//
// Session lock NOT held on entry.
func (sess *mqttSession) recordRecoveredPubRec(ref mqttInflightSlotRef) mqttRecoveredPubRecOutcome {
	if sess.asm == nil || !ref.slotOK() || ref.name.subject == _EMPTY_ {
		return mqttRecoveredPubRecUnresolved
	}
	resolve := func() mqttRecoveredPubRecOutcome {
		cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
		switch {
		case err != nil || smsg == nil || cur == nil:
			return mqttRecoveredPubRecUnresolved
		case !ref.matchesRecord(cur):
			return mqttRecoveredPubRecNotOurs
		case cur.State == mqttInflightPublish:
			return mqttRecoveredPubRecUnresolved
		}
		if mqttDurIsSlotOnlyClass(cur.Dur) {
			return mqttRecoveredPubRecNotOurs
		}
		if class, x := sess.ackExchangeFor(cur, ref.name.gen); class != mqttAckExchange ||
			x.Phase == mqttExchangePublish {
			return mqttRecoveredPubRecUnresolved
		}
		return mqttRecoveredPubRecRecorded
	}
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
		switch {
		case err != nil || cur == nil:
			return mqttRecoveredPubRecUnresolved
		case smsg == nil:
			return mqttRecoveredPubRecNotOurs
		case !ref.matchesRecord(cur):
			return mqttRecoveredPubRecNotOurs
		case cur.State != mqttInflightPublish:
			// Already past PUBREC: recorded only if its exchange agrees.
			return resolve()
		}
		class, x := sess.ackExchangeFor(cur, ref.name.gen)
		switch class {
		case mqttAckSlotOnly:
			return mqttRecoveredPubRecNotOurs
		case mqttAckRefused:
			return mqttRecoveredPubRecUnresolved
		}
		to, perr := mqttPhaseForAck(mqttAckArrival{Packet: mqttPacketPubRec, Reason: mqttReasonSuccess}, x.QoS)
		if perr != nil {
			return mqttRecoveredPubRecUnresolved
		}
		if _, aerr := sess.advanceExchangeAck(&mqttAdvanceExchangeAckRequest{
			ID: sess.id, Epoch: sess.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
			Gen: ref.name.gen, Nonce: cur.Lin, To: to, Acq: cur.Acq,
			Register: cur, RegisterSeq: smsg.Sequence,
		}); aerr != nil {
			continue // re-read: a landed batch behind a lost response shows pubrel_intended
		}
		return mqttRecoveredPubRecRecorded
	}
	// The final attempt's error may hide a landed batch.
	return resolve()
}

// phaseRecoveredPublishSlotToReady completes recovery's route B: the PUBREL record
// exists, but the crash cut left its owner slot in `publish` before the live handoff's
// CAS could run.
//
// The scanned record is the canonical origin. It must match the whole exchange, and its
// actual stream sequence—not a store response or a source-only inference—is what lands in
// the slot. The CAS carries forward the record it loads, so a concurrent retirement or
// custody rewrite is preserved rather than rebuilt from recovery's earlier snapshot.
//
// A claimless pre-transition PUBLISH shape cannot be advanced: custody is mandatory in
// every past-PUBREC state. The caller quarantines that slot instead of manufacturing an
// owner for it.
func (sess *mqttSession) phaseRecoveredPublishSlotToReady(c *client, rv mqttRecoveredInflightReg,
	rel mqttStoredPubRel) (mqttRecoveredInflightReg, bool) {

	if sess.asm == nil || rv.rec == nil || rv.rec.State != mqttInflightPublish || rel.seq == 0 ||
		!mqttStoredPubRelMatchesSlot(rel, rv.rec) {
		return mqttRecoveredInflightReg{}, false
	}
	ref := mqttSlotRefFromRecord(
		mqttInflightSubject(sess.idHash, sess.gen, rv.rec.PI), rv.seq, rv.rec)
	if ref == nil {
		return mqttRecoveredInflightReg{}, false
	}
	_, _, dur := mqttPubRelNames(ref.name.idHash, ref.name.gen)
	if dur == _EMPTY_ {
		return mqttRecoveredInflightReg{}, false
	}
	var landed mqttInflightRecord
	var seq uint64
	var ok bool
	if mqttDurIsSlotOnlyClass(rv.rec.Dur) {
		seq, ok = sess.casInflightSlot(c, *ref, mqttInflightPublish, mqttSlotAdvancePhase,
			func(next *mqttInflightRecord) {
				next.State = mqttInflightPubRelReady
				next.PubRelDurable = dur
				next.PubRelStreamSeq = rel.seq
				landed = *next
			})
	} else {
		// AN ORDINARY SLOT HAS AN EXCHANGE, and the stored record is evidence of a PUBREC that
		// exchange has not recorded yet (the drained PUBREC persists its intent before its
		// advance). Record it through the one PUBREC arrow FIRST, then rebind the canonical
		// tuple from pubrel_intended exactly as the live handoff does. A crash between the two
		// leaves pubrel_intended beside PUBREC-observed and a stored record: the ordinary
		// owed-materialization shape, never a PUBLISH resend and never an inferred completion.
		if sess.recordRecoveredPubRec(*ref) != mqttRecoveredPubRecRecorded {
			return mqttRecoveredInflightReg{}, false
		}
		cur, smsg, lerr := sess.asm.loadInflightRecord(ref.name.subject)
		if lerr == nil && cur != nil && smsg != nil && ref.matchesRecord(cur) &&
			cur.State == mqttInflightPubRelIntended {
			if iref := mqttSlotRefFromRecord(ref.name.subject, smsg.Sequence, cur); iref != nil {
				seq, ok = sess.casInflightSlot(c, *iref, mqttInflightPubRelIntended, mqttSlotRebindCanonical,
					func(next *mqttInflightRecord) {
						next.State = mqttInflightPubRelReady
						next.PubRelDurable = dur
						next.PubRelStreamSeq = rel.seq
						landed = *next
					})
			}
		}
	}
	if !ok {
		// The live handler or another recovery may have won the phase race. Adopt only
		// an authoritative reread of THIS exchange that is already past PUBREC; a
		// PUBLISH reread, absence, mismatch or read error proves no handoff.
		cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
		if err != nil || cur == nil || smsg == nil ||
			!ref.matchesRecord(cur) || !mqttInflightPastPubRec(cur.State) {
			return mqttRecoveredInflightReg{}, false
		}
		traversed := append([]uint64(nil), rv.traversed...)
		traversed = append(traversed, rv.seq, smsg.Sequence)
		return mqttRecoveredInflightReg{rec: cur, seq: smsg.Sequence, traversed: traversed}, true
	}
	traversed := append([]uint64(nil), rv.traversed...)
	traversed = append(traversed, rv.seq, seq)
	return mqttRecoveredInflightReg{rec: &landed, seq: seq, traversed: traversed}, true
}

// mqttAcquirePubRelAction acquires the durable action for the exchange `ref` names,
// arming the local holder BEFORE the CAS can be issued.
//
// Returns the acquired action, or nil when the action was not taken — which is an ordinary
// outcome, not an error: another broker holds it, the slot moved, or the read was
// indeterminate. The caller returns WITHOUT acknowledging the record in every one of those
// cases, so AckWait re-offers it.
//
// Session lock NOT held on entry. The JetStream CAS is the distributed linearization
// point and must not run under sess.mu; the lock is taken only to install and to promote
// the local holder.
//
// lin is the obligation lineage of the fresh read ref came from. ended reports that the
// connection's accepted entitlement is no longer the serving one — TERMINAL for the delivery that
// asked, which must not re-dispatch it under a tenure that is not its own.
//
// admit, when given, admits each authority-transition attempt: a BACKGROUND caller (the retained
// driver) yields every attempt to announced foreground operations (outbound_foreground_priority.go).
func (sess *mqttSession) mqttAcquirePubRelAction(c *client, ref mqttInflightSlotRef, lin string,
	wantCanonical uint64, admit ...func() bool) (act *mqttSendAction, ended bool) {
	if !ref.slotOK() || ref.name.subject == _EMPTY_ || ref.name.pi == 0 {
		return nil, false
	}
	token := mqttNewInflightNonce()

	// ARM FIRST. The holder is installed tentative and its abort armed before the request
	// goes out, so a CAS that lands while this goroutine is unwinding still has an owner
	// that will drain it. Its pre-submit F-105 identity is fixed here too, before any I/O.
	act = &mqttSendAction{
		token: token,
		state: mqttSendActionTentative,
		slot:  ref,
		lin:   lin,
	}
	if c != nil {
		act.guard = &c.outFrames
	}
	sess.mu.Lock()
	if !sess.installTentativeActionLocked(ref.name.pi, act) {
		sess.mu.Unlock()
		return nil, false
	}
	sess.mu.Unlock()

	// ARM THE DRAIN NOW, before the CAS request goes out, and actually install the unwind
	// handler the "arm first" comment promised. A recovered panic or runtime.Goexit after
	// the CAS lands but before this function hands responsibility off would otherwise leave a
	// durable action and a tentative local holder with nothing to drain them, reserving the
	// identifier for the life of the process. handedOff is set on every normal return, so
	// this fires only on an abnormal unwind.
	handedOff := false
	defer func() {
		if !handedOff {
			sess.startActionDrain(c, act)
		}
	}()

	var seq uint64
	var ok bool
	var landed *mqttOutboundAction
	if mqttDurIsSlotOnlyClass(ref.dur) {
		// AN AFFINE SLOT keeps the slot machine's own CAS until chapter P converts its
		// admission: it has no exchange for the authority to arbitrate. A scope boundary, not a
		// fallback — an ordinary slot never takes this arm.
		seq, ok = sess.casInflightSlot(c, ref, mqttInflightPubRelReady, mqttSlotAcquireAction,
			func(next *mqttInflightRecord) {
				// THE PHASE DOES NOT MOVE. `pubrel_sending` is gone: an action is the TOKEN,
				// not a phase, and encoding one in the other is what left a PUBLISH
				// transmission with no durable fence at all. The slot stays `pubrel_ready`
				// and gains a token.
				next.ActionToken = token
				// C-I: every acquisition advances the transmission identity, and nothing
				// ever moves it back. Overflow is REFUSED rather than wrapped, by the
				// mutator's caller — see casInflightSlot.
				next.Acq++
				// The action binds to the custody the slot ALREADY carries, never to what
				// this session object believes its custody to be. The shape validator
				// requires the two to be equal, so deriving them from anywhere else would
				// write a record the next reader refuses.
				next.ActionClaim = next.Claim
				next.ActionClaimSeq = next.ClaimSeq
			})
	} else {
		seq, landed, ok, ended = sess.acquirePubRelThroughAuthority(c, ref, mqttInflightPubRelReady, token, nil, wantCanonical, admit...)
	}
	if !ok {
		// Not acquired. The tentative holder is retired only once a reread proves this
		// token is absent — a DEFINITE rejection and an AMBIGUOUS one are indistinguishable
		// from here, and retiring on the ambiguous case would abandon an action that
		// actually landed.
		handedOff = true
		sess.resolveUnacquiredAction(c, act)
		return nil, ended
	}
	sess.mu.Lock()
	act.state = mqttSendActionAcquired
	act.outAction = landed
	act.slot.seq = seq
	act.slot.state = mqttInflightPubRelReady
	act.slot.acq++
	act.slot.actionLive = true
	act.slot.actionToken = token
	// The acquired slot does NOT become a pending entry here: the delivery callback that runs
	// after this acquisition is the one place an acquired action becomes an entry
	// (bindActionToPubRelLocked), validated against the resident and custody first. What does
	// happen here is the PHASE refresh of this exchange's own resident entry — the terminal
	// release's freshness fence compares its acquisition count — and nothing that authorizes.
	sess.advanceResidentPubRelPhaseLocked(ref.name.pi, act.slot)
	sess.mu.Unlock()
	handedOff = true
	return act, false
}

// acquirePubRelThroughAuthority takes an ORDINARY PUBREL acquisition as ONE authority batch
// (F-105 row 2, §4.3 point 1): the output-custody token installed on the register at its exact
// read sequence BESIDE the create-only action record, arbitrated by the serving authority — in
// place of the standalone slot CAS, never beside it. rebind, for the direct arrow, also moves the
// slot `pubrel_intended -> pubrel_ready` and installs the canonical PUBREL the delivery proved.
//
// The entitlement is the connection's OWN accepted one, passed on every attempt; after a takeover
// every attempt is refused and ended reports it, which is terminal for the caller. Each attempt
// re-reads the register and re-proves the acquisition's premise on that base — the same statement
// the slot CAS applies (mqttSlotAcquisitionBaseHolds) — so contention is re-attempted and a moved
// slot is not. A landing is returned with the register's own new sequence and the exact record the
// frame will consume. Anything else returns not-acquired, and the caller's reread resolves whether
// an ambiguous attempt landed; an acquisition adopted that way is drained, never emitted.
//
// Session lock NOT held on entry.
// wantCanonical is the canonical PUBREL sequence the caller validated its delivered record against,
// for a READY acquisition (F-105 7b): the helper refreshes its base on every attempt, and neither
// matchesRecord nor the acquisition-base check compares the canonical tuple, so a callback that
// validated record S and paused could otherwise acquire against a slot rebound to a new record N.
// Zero for the intended->ready direct arrow, which INSTALLS the tuple it carries in rebind.
func (sess *mqttSession) acquirePubRelThroughAuthority(c *client, ref mqttInflightSlotRef,
	from mqttInflightState, token string, rebind *mqttPubRelRebind,
	wantCanonical uint64, admit ...func() bool) (seq uint64, landed *mqttOutboundAction, ok, ended bool) {

	as := sess.asm
	if as == nil || c == nil {
		return 0, nil, false, false
	}
	ent, entOK := c.outboundEntitlement()
	if !entOK {
		return 0, nil, false, true
	}
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		if len(admit) > 0 && admit[0] != nil && !admit[0]() {
			return 0, nil, false, false
		}
		cur, smsg, lerr := as.loadInflightRecord(ref.name.subject)
		if lerr != nil || smsg == nil || cur == nil ||
			!mqttInflightRecordMatchesSubject(cur, ref.name.subject) || !ref.matchesRecord(cur) ||
			cur.State != from || mqttValidateInflightShape(cur) != nil ||
			!mqttSlotAcquisitionBaseHolds(ref, cur) ||
			(from == mqttInflightPubRelReady && wantCanonical != 0 && cur.PubRelStreamSeq != wantCanonical) {
			return 0, nil, false, false
		}
		_, act, aerr := sess.jsa.opAcquireAction(&mqttAcquireActionRequest{
			ID: sess.id, Epoch: as.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-acquire-pubrel",
			Gen: ent.Gen, Tenure: ent.Tenure,
			Nonce: cur.Lin, Acq: cur.Acq + 1, Kind: mqttActionPubRel,
			// Outstanding is FALSE for the PUBLISH side's reason: admission and the tentative
			// install already refused any live token under this identifier, so no earlier action of
			// this exchange can still emit.
			Binding: c.outBind, Outstanding: false,
			Register: cur, RegisterSeq: smsg.Sequence, Token: token, Rebind: rebind,
		})
		if aerr == nil {
			// The outcome carries the AUTHORITY sequence; the slot's own is read back, and must
			// show our token or the landing is resolved like a lost response.
			rcur, rsmsg, rerr := as.loadInflightRecord(ref.name.subject)
			if rerr != nil || rsmsg == nil || rcur == nil || rcur.ActionToken != token {
				return 0, nil, false, false
			}
			return rsmsg.Sequence, act, true, false
		}
		if errors.Is(aerr, errMQTTEntitlementEnded) {
			return 0, nil, false, true
		}
	}
	return 0, nil, false, false
}

// enterPubRelWriteLocked is the PUBREL write ENTRY (F-105 row 2, consult 28): the one place a
// holder moves acquired -> writing, and it refuses unless this exact holder may still emit.
//
// All of it, under the session lock, for BOTH classes: the holder is acquired, not released, not
// drained; it still holds a live token; it is still the identifier's registered action. An
// ORDINARY holder must also hold the exact acquisition it landed through the authority — its
// record's packet, generation, lineage, counter and identifier matching the holder — and have been
// acquired for THIS connection's frame guard; without an acquisition it refuses, since its bytes
// would be output the authority never authorized. A slot-only holder (experimental affine, chapter P) has
// no authority record, and none is invented.
//
// On success the holder is writing, ONCE, and the returned ticket is the only thing the writer
// checks. A refusal moves nothing; the caller writes nothing and its deferred drain gives the
// acquisition up as before.
//
// Session lock held on entry.
func (sess *mqttSession) enterPubRelWriteLocked(act *mqttSendAction, cc *client) (*mqttWriteEntry, bool) {
	if act == nil || cc == nil {
		return nil, false
	}
	if act.state != mqttSendActionAcquired || act.released || act.drained ||
		act.slot.actionToken == _EMPTY_ || act.slot.actionToken != act.token {
		return nil, false
	}
	if !sess.registeredActionLocked(act.slot.name.pi, act) {
		return nil, false // not the identifier's registered holder: a copy, a replaced or a retired one
	}
	t := &mqttWriteEntry{conn: cc}
	if !mqttDurIsSlotOnlyClass(act.slot.dur) {
		a := act.outAction
		if a == nil || a.Kind != mqttActionPubRel || a.Gen != act.slot.name.gen || a.Nonce != act.lin ||
			a.Acq != act.slot.acq || a.PI != act.slot.name.pi || act.guard != &cc.outFrames {
			return nil, false
		}
		t.action, t.guard = a, act.guard
		t.key = mqttActionKey{gen: a.Gen, nonce: a.Nonce, acq: a.Acq}
	}
	act.state = mqttSendActionWriting
	return t, true
}

// installTentativeActionLocked installs the tentative holder, MERGING into any existing
// one rather than replacing it.
//
// Replacement is the defect this exists to avoid: a same-connection pubCompOwed term
// legitimately survives a returned PUBREL write through endSend, and another PUBREL
// redelivery can then acquire an action — so installing by replacing the map entry would
// silently erase acknowledgement debt and the connection identity that owns it. A retained
// term is preserved for the same reason.
//
// Inert FOREIGN debt keeps today's replacement semantics (F-089's inert-hold takeover):
// a debt owed to a connection that is no longer ours can never be answered, so it must not
// block an action this connection is entitled to take.
//
// Session lock held on entry.
func (sess *mqttSession) installTentativeActionLocked(pi uint16, act *mqttSendAction) bool {
	if sess.sendsInFlight == nil {
		sess.sendsInFlight = make(map[uint16]*mqttSendHold, 1)
	}
	h := sess.sendsInFlight[pi]
	if h == nil {
		sess.sendsInFlight[pi] = &mqttSendHold{action: act}
		return true
	}
	if h.action != nil && !h.action.drained {
		// An action is already live under this identifier. Two actions for one exchange
		// is the exclusion failing, so this refuses rather than overwriting.
		return false
	}
	h.action = act
	return true
}

// resolveUnacquiredAction settles a tentative holder whose CAS did not report success.
//
// The distinction that matters: a DEFINITE rejection means the token was never written,
// and an AMBIGUOUS one means it may have been. They are indistinguishable from the CAS's
// return, so the only safe resolution is to REREAD and ask whether this exact token is
// present. Retiring the holder without that reread would abandon an action that landed.
//
// Session lock NOT held on entry.
func (sess *mqttSession) resolveUnacquiredAction(c *client, act *mqttSendAction) {
	cur, smsg, err := sess.asm.loadInflightRecord(act.slot.name.subject)
	switch {
	case err != nil:
		// Indeterminate. Not an absence and not a licence to retire the holder: the
		// action stays live and reserved, and the drain retries.
		sess.startActionDrain(c, act)
		return
	case smsg == nil:
		// The slot is gone. Nothing of ours survives on it, so the local holder is
		// retired and no drain is owed.
		sess.finishActionAndWake(act, false)
		return
	case cur == nil:
		// PRESENT BUT UNREADABLE. The acquisition may have landed, and unreadable bytes
		// are not proof that its token is absent. Keep the holder and reconcile exactly as
		// for an indeterminate read; retiring it here would abandon a durable action.
		sess.startActionDrain(c, act)
		return
	}
	if cur.ActionToken == act.token {
		// AMBIGUOUS AND IT LANDED. The action is ours and must be drained, not abandoned.
		// `pubrel_terminal_draining` here is PROOF the acquisition landed and a concurrent
		// PUBCOMP advanced this exact token — it is adopted, not restarted.
		sess.mu.Lock()
		act.state = mqttSendActionAcquired
		act.slot.state = cur.State
		act.slot.actionToken = act.token
		sess.mu.Unlock()
		sess.startActionDrain(c, act)
		return
	}
	// A different valid token, or none. Either way this token is not present, so the
	// acquisition definitely failed and the local holder is retired — touching nothing
	// else, and in particular not the other holder's token.
	sess.finishActionAndWake(act, false)
}

// finishActionAndWake retires the local holder of an action that is PROVEN over, and then
// wakes any stored PUBREL that this identifier's transmission was excluding.
//
// The wake belongs here and not only at the end of a PUBLISH send section, because an
// action's end and a section's end are DIFFERENT events: admission registers a waiter
// against any live token, a PUBREL's included, and a PUBREL action outlives the section
// that acquired it. Without this the waiter left behind a PUBREL action would sit until
// something unrelated happened to wake it, which is the AckWait silence one level over.
//
// PROVEN completions only. An indeterminate or unreadable read retires nothing and reaches
// no wake — the holder stays live and the reconciler retries — and the combined check
// inside the wake then refuses to consume a waiter a replacement transmission still
// excludes.
//
// Session lock NOT held on entry.
func (sess *mqttSession) finishActionAndWake(act *mqttSendAction, completed bool) {
	if act == nil {
		return
	}
	sess.serverFor(nil).clearOwedAction(act)
	sess.retireActionLocked_(act, completed)
	sess.wakePubRelRedriveIfClear(act.slot.name.pi, act.slot.name.subject)
}

// retireActionLocked_ marks the action drained and drops the holder term if it now
// reserves nothing. Takes the session lock itself.
// `completed` says this drain ITSELF ENDED THE EXCHANGE — it observed pubrel_terminal and
// its own finalizeTerminalSlot confirmed the completion. Only that licenses dropping the
// last identifier exclusion.
//
// Every other outcome installs one, and the reason is a schedule that needs no storage
// failure: A clears its token from `pubrel_ready` and pauses before retiring, B acquires a
// successor action for the same nonce and source, PUBCOMP terminalizes while B's action is
// live, A's not-yet-retired hold satisfies the handler's heldLocally check so the terminal
// reservation is removed, and A then retires. A observed `pubrel_ready`, so it ran no
// terminal finalization and no exchange-mismatch handoff fires — the identifier would be
// free while B may still transmit. A REPLACEMENT and an ABSENCE are the same interval:
// absence of OUR slot is not proof that no successor exists (F-098).
func (sess *mqttSession) retireActionLocked_(act *mqttSendAction, completed bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	act.drained = true
	act.state = mqttSendActionReleased
	pi := act.slot.name.pi
	if h := sess.sendsInFlight[pi]; h != nil && h.action == act {
		if !completed {
			sess.excludeIdentifierLocked(pi, act.slot.name.subject, _EMPTY_)
		}
		h.action = nil
		if !sess.sendHoldLiveLocked(h) {
			delete(sess.sendsInFlight, pi)
		}
	}
}

// startActionDrain drives the durable drain of an acquired action to a proven conclusion.
//
// FOUR rereads prove this token is gone, and the last two are what stop a finite ambiguous
// drain retrying forever:
//
//   - the exact token resolved to a matching `pubrel_ready` — the ordinary no-PUBCOMP case;
//   - the exact token resolved to a matching `pubrel_terminal`;
//   - a DIFFERENT valid token is present, because a peer acquired from `pubrel_ready`
//     before this reread ran;
//   - the slot is ABSENT, because terminal cleanup exact-deleted it after this drain
//     landed ambiguously.
//
// In the last two the holder retires its OWN local state and acts on nothing else — in
// particular it does not touch the other token and does not resurrect a deleted slot.
//
// Session lock NOT held on entry.
func (sess *mqttSession) startActionDrain(c *client, act *mqttSendAction) {
	if act == nil {
		return
	}
	sess.mu.Lock()
	// The LOCAL released bit first, under the lock, so every saved capability attempting
	// a write is rejected from here on. It does not end the reservation: a locally
	// released but durably unresolved action still holds its identifier.
	act.released = true
	if act.state != mqttSendActionWriting {
		act.state = mqttSendActionReleased
	}
	sess.mu.Unlock()

	// THE TARGET IS DERIVED FROM THE STATE OBSERVED, not from the state at acquisition,
	// and that is the correction a fixture forced.
	//
	// A PUBCOMP arriving while the write is out advances the slot `sending ->
	// terminal_draining` underneath this holder. A drain that remembered `sending` and
	// aimed at `ready` then fails its predecessor check forever, leaving the slot in
	// `terminal_draining` with its identifier reserved for the life of the session — the
	// exact leak this mechanism exists to bound. So each attempt reads the state and picks
	// the arrow that belongs to it:
	//
	//	sending           -> ready     (the write returned, no PUBCOMP)
	//	terminal_draining -> terminal  (PUBCOMP was observed while the write was out)
	//
	// Bounded attempts here, then RESCHEDULED. A finite storage failure must not strand
	// the action: without the reschedule a single failed read after a successful
	// acquisition permanently consumes an identifier and stalls a Receive Maximum 1
	// session, which is the successful-path leak Decision 1 rejected outright.
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, err := sess.asm.loadInflightRecord(act.slot.name.subject)
		switch {
		case err != nil:
			// INDETERMINATE, and it must hand off to the reconciler rather than return.
			// Returning here was the whole defect: this is the branch a transient read
			// failure takes, so it is exactly the case the reconciler exists for, and
			// without the hand-off one failed read left the action and its identifier
			// reserved with no future event guaranteed to retry.
			sess.scheduleActionReconcile(c, act)
			return
		case smsg == nil:
			// ABSENT. Terminal cleanup exact-deleted the slot after this drain landed;
			// nothing of ours remains, so the holder retires and touches nothing else.
			sess.finishActionAndWake(act, false)
			return
		case cur == nil:
			// PRESENT BUT UNREADABLE. It may still carry this exact action, so absence
			// has not been proved and the local holder must remain live.
			sess.scheduleActionReconcile(c, act)
			return
		}
		ordinary := !mqttDurIsSlotOnlyClass(cur.Dur)
		if cur.ActionToken != act.token {
			// OUR PAIRED DRAIN LANDED BEHIND A LOST RESPONSE. For an ordinary exchange the
			// clearance and the Terminal advance were one batch, so a token-free pubrel_terminal
			// slot that is still THIS exchange's is our own completion — and its cleanup is ours
			// to finish or to hand on. Returning bare here would leave a terminal slot nothing
			// owns, its identifier excluded until a restart (the cleanup-ownership property).
			if ordinary && cur.ActionToken == _EMPTY_ && cur.State == mqttInflightPubRelTerminal &&
				act.slot.matchesRecord(cur) {
				ref := act.slot
				ref.seq = smsg.Sequence
				ref.state = mqttInflightPubRelTerminal
				sess.finishActionAndWake(act, sess.settleDrainedTerminal(c, ref, false))
				return
			}
			// A DIFFERENT valid token, or none: a peer acquired from `ready` before this
			// reread, or the drain already landed. Either way this action is gone. The
			// holder retires its OWN state and does not touch the other token.
			sess.finishActionAndWake(act, false)
			return
		}
		// OUR EXACT TOKEN, PROVEN. If this holder never entered its write, the acquisition the
		// token belongs to will never execute: recorded ABANDONED on the connection it was
		// acquired for, keyed by the counter READ beside the token, and BEFORE any clearing while
		// this read still proves it (F-105 row 2). A holder that entered is the write entry's to
		// settle (its ticket's relinquish), so a drain never backfills that case.
		if ordinary {
			sess.mu.Lock()
			entered := act.state == mqttSendActionWriting
			sess.mu.Unlock()
			if !entered {
				act.abandonProven(cur.Acq)
			}
		}
		// THE DRAIN NO LONGER PICKS A PHASE. The switch that stood here derived an
		// arrow from `pubrel_sending` / `pubrel_terminal_draining`, and it existed only
		// because the token was encoded in the phase. With the token its own field, a
		// drain PRESERVES the phase it observes and clears the action — a strictly
		// smaller rule that keeps the correction the switch was written for: the outcome
		// is derived from the state OBSERVED, never the state at acquisition.
		//
		// Which cleanup follows is then dispatched from that observed phase, below.
		observed := cur.State
		ref := act.slot
		ref.seq = smsg.Sequence
		ref.state = observed
		ref.acq = cur.Acq
		// THE DRAIN'S AUTHORITY IS THE EXACT TOKEN IT HOLDS, just proven present above — not
		// whatever the holder's captured slot records. That copy is filled only when an
		// acquisition is SEEN to land, so a holder whose acquisition landed behind a lost
		// response and an unreadable resolution reached here with an empty authority, the
		// drain CAS refused it on every reconcile, and the identifier stayed held for the life
		// of the session. The PUBLISH drain carries the same correction.
		ref.actionToken = act.token
		if ordinary && observed == mqttInflightPubRelTerminal {
			// A PUBCOMP WAS OBSERVED WHILE THIS PUBREL WAS OUT, so the exchange stands at
			// terminal-pending-drain and THIS drain is what ends it: the token clearance and
			// the pending-drain -> terminal advance are ONE batch (opCompleteExchangeDrain).
			if _, derr := sess.completeExchangeDrain(&mqttCompleteExchangeDrainRequest{
				ID: sess.id, Epoch: sess.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-drain-complete",
				Gen: ref.name.gen, Nonce: cur.Lin, Token: act.token,
				Register: cur, RegisterSeq: smsg.Sequence,
			}); derr != nil {
				continue // re-read: a landed batch behind a lost response shows the token gone
			}
			// ACKNOWLEDGED. The op returns the AUTHORITY sequence; finalize's freshness fence
			// needs the slot's own, so read it back.
			cur2, smsg2, lerr := sess.asm.loadInflightRecord(ref.name.subject)
			switch {
			case lerr == nil && smsg2 == nil:
				// Already finalized elsewhere; nothing of ours remains.
				sess.finishActionAndWake(act, false)
			case lerr == nil && cur2 != nil && act.slot.matchesRecord(cur2) &&
				cur2.State == mqttInflightPubRelTerminal && cur2.ActionToken == _EMPTY_:
				ref.seq = smsg2.Sequence
				ref.state = mqttInflightPubRelTerminal
				sess.finishActionAndWake(act, sess.settleDrainedTerminal(c, ref, false))
			default:
				// The completion is durable and its cleanup is unsettled: hand it to the owner
				// of every such cleanup. The action itself is finished — its token is gone.
				ref.state = mqttInflightPubRelTerminal
				sess.ownTerminalCleanup(c, ref)
				sess.finishActionAndWake(act, false)
			}
			return
		}
		seq, ok := sess.casInflightSlot(c, ref, observed, mqttSlotDrainAction,
			func(next *mqttInflightRecord) {
				if next.ActionToken != act.token {
					return
				}
				// The phase is untouched, and Acq is NEVER reset here: a drain ends a
				// transmission's authority, it does not un-count it (C-I).
				next.ActionToken = _EMPTY_
				next.ActionClaim = _EMPTY_
				next.ActionClaimSeq = 0
			})
		if !ok {
			continue // the slot moved; reread and drain against what is there now
		}
		// COMPLETED means THIS drain ended the exchange, and nothing weaker. Only a
		// pubrel_terminal observation whose own finalize CONFIRMED the completion licenses
		// dropping the identifier's last exclusion; every other outcome here leaves an
		// exchange that may still be live somewhere.
		completed := false
		if observed == mqttInflightPubRelTerminal {
			// PUBCOMP was already observed, so the exchange is over and this drain is
			// what makes it deletable. Finish it here rather than waiting for another
			// event: nothing else is coming. (Slot-only classes: an ordinary exchange took
			// the paired completion above.)
			ref.seq = seq
			ref.state = mqttInflightPubRelTerminal
			completed = sess.settleDrainedTerminal(c, ref, true)
		}
		sess.finishActionAndWake(act, completed)
		return
	}
	// UNRESOLVED after the bounded attempts: hand it to the reconciler.
	sess.scheduleActionReconcile(c, act)
}

// settleDrainedTerminal finalizes a pubrel_terminal slot whose PUBREL action this drain just
// ended, and reports whether this drain COMPLETED the exchange — the one fact that licenses
// dropping the identifier's last exclusion.
//
// An unsettled cleanup is handed on, never dropped: an ordinary exchange's to the completion
// worker (ownTerminalCleanup), a slot-only class's to retainAndReconcileTerminal.
//
// Session lock NOT held on entry.
func (sess *mqttSession) settleDrainedTerminal(c *client, ref mqttInflightSlotRef, slotOnly bool) bool {
	switch sess.finalizeTerminalSlot(c, ref) {
	case mqttTerminalIndeterminate:
		if slotOnly {
			sess.retainAndReconcileTerminal(c, ref)
		} else {
			sess.ownTerminalCleanup(c, ref)
		}
	case mqttTerminalRetainedReoffer:
		// The retained slot outlives this drain, so the identifier stays excluded
		// until something confirms the subject empty (F-098, round 5), and its cleanup
		// owner is installed in the same critical section as that exclusion.
		sess.mu.Lock()
		sess.excludeIdentifierLocked(ref.name.pi, ref.name.subject, ref.nonce)
		if w := sess.enrollRetainedTerminalCleanupLocked(c, ref); w != nil {
			sess.scheduleRetainedCleanupLocked(w, sess.retainedCleanupDelayLocked(0))
		}
		sess.mu.Unlock()
	case mqttTerminalHeldByAction:
		// Our token is gone — this drain just cleared it — so the live one is a
		// DIFFERENT holder's, and that holder's own drain dispatches this same
		// cleanup when it ends. Nothing here may act under it.
	default:
		// DONE IS NOT A PROOF OF COMPLETION BY ITSELF. It also covers the two
		// replacement branches, where a successor holds the subject and nothing was
		// proven absent — and those branches install their own exclusion. Treating
		// Done as completed there would suppress the retirement's install on exactly
		// the schedule it exists for, so the proof is the exclusion's absence: this
		// drain completed the exchange only if nothing is excluding the identifier
		// once the finalize has run.
		sess.mu.Lock()
		completed := sess.slotExclusionInstLocked(ref.name.pi) == 0
		sess.mu.Unlock()
		return completed
	}
	return false
}

// mqttActionReconcileDelay is the capped delay between reconciliation attempts.
//
// A CAP with unlimited attempts, never a bounded attempt count: a bounded count converts a
// long storage outage into a permanently abandoned action, which is the one outcome this
// lifecycle exists to keep out of the successful path.
const mqttActionReconcileDelay = 2 * time.Second

// scheduleActionReconcile is the session-owned reconciliation lifecycle: the continuing
// mechanism that makes an unresolved action SELF-HEALING rather than merely improbable.
//
// Decision 1 authorized it as the required completion of admission, and the reasoning is
// that every earlier revision answered a LIFECYCLE question with a fence. A re-read reports
// what was true before acting; it does not remain true through the action. So an action
// whose drain could not be confirmed is not abandoned on the strength of one failed
// attempt — it is retried until a reread PROVES the token gone, which is one of: resolved
// to a matching `pubrel_ready`, resolved to `pubrel_terminal`, replaced by a DIFFERENT
// valid token, or the slot absent.
//
// IN-MEMORY ONLY, which is what keeps it inside the compatibility boundary: the reservation
// it guards is already in memory, so process loss destroys reconciler and reservation
// together and the next resume reclassifies from the slot, which is the durable truth. No
// new durable state, no new stream, no new authority — the slot remains the single
// claimant, and a cached classification authorizes nothing.
//
// SHUTDOWN-AWARE, because an in-process Server.Shutdown() is not process loss: the timer is
// armed only while the shutdown gate admits it, and the gate closing prevents re-arming, so
// an unlimited retry can never hold shutdown open or touch torn-down JetStream state. A
// refused attempt simply leaves the reservation for the next resume to reclassify —
// exactly what admitWillWorker's refusal does for a Will record.
//
// Session lock NOT held on entry.
func (sess *mqttSession) scheduleActionReconcile(c *client, act *mqttSendAction) {
	if act == nil {
		return
	}
	sess.mu.Lock()
	if act.drained {
		sess.mu.Unlock()
		return
	}
	// The GENERATION is part of the identity: a proven discard means the next incarnation
	// uses a different PUBREL subject, so an old-generation action is teardown material
	// rather than retry state.
	if act.slot.name.gen != sess.gen {
		act.drained = true
		sess.mu.Unlock()
		return
	}
	sess.mu.Unlock()
	var srv *Server
	if c != nil {
		srv = c.srv
	}
	if srv == nil || !srv.admitWillWorker() {
		// The shutdown gate is closed, or there is no server to account the work to. Hand the
		// action to the shutdown's own registry rather than leaving it to the next resume:
		// Phase 3b runs while JetStream still answers, and a token left live here is one the
		// next incarnation cannot tell from a peer's and may not steal (round 5, finding 3).
		srv.noteOwedAction(sess, act)
		return
	}
	time.AfterFunc(mqttActionReconcileDelay, func() {
		defer srv.willWorkerWG.Done()
		sess.mu.Lock()
		done := act.drained || act.slot.name.gen != sess.gen
		sess.mu.Unlock()
		if done {
			return
		}
		sess.startActionDrain(c, act)
	})
}

// ---------------------------------------------------------------------------
// L1: owed materialization, and its guaranteed continuation
// ---------------------------------------------------------------------------

// mqttOwedMaterialization is one slot that is OWED a canonical PUBREL record.
//
// It is a DISTINCT WORK KIND in the same reconciliation lifecycle as an action drain,
// rather than a second scheduler, because the problem is identical and was already
// solved once: no future event is guaranteed to retry. It is not an action, and it
// cannot be represented as one — scheduleActionReconcile takes a *mqttSendAction whose
// worker compares action TOKENS, and an owed slot has none by construction.
type mqttOwedMaterialization struct {
	// slot is the durable identity this work targets: subject, nonce and the
	// transmission counter (C-I). Nothing is carried from enrolment except this — every
	// attempt revalidates custody, phase, identity and canonical evidence from scratch.
	slot mqttInflightSlotRef
	// done is set once the obligation is discharged or proven not ours, under sess.mu.
	done bool
}

// scheduleOwedMaterialization is L1's GUARANTEED CONTINUATION.
//
// The gap it closes is liveness, and it is the one attempt 1's round 3 found and refused
// to let pass: re-enrolling "at the next recovery or custody validation" strands a slot,
// because a custodian can sit connected with healthy storage, no source delivery, no
// PUBREL delivery and no new recovery, and nothing ever invokes another attempt. The
// action reconciler already recognised this shape for actions; owed slots get the same
// treatment rather than a disclosed residual.
//
// UNLIMITED ATTEMPTS WITH A CAPPED DELAY, never a bounded count: a bounded budget is
// exactly what converts a long storage outage into a permanently owed slot, which is the
// one outcome this lifecycle exists to keep out of the successful path.
//
// SHUTDOWN-AWARE on the same gate as the action reconciler: the timer is armed only while
// the gate admits it, so an unlimited retry can never hold shutdown open. A refused
// arming leaves the durable owed PHASE, which is what supplies restart enrolment — by
// this broker or by whichever one next takes custody.
//
// Session lock NOT held on entry.
func (sess *mqttSession) scheduleOwedMaterialization(c *client, owed *mqttOwedMaterialization) {
	if owed == nil {
		return
	}
	sess.mu.Lock()
	if owed.done || owed.slot.name.gen != sess.gen {
		// A proven generation discard means the next incarnation uses different subjects
		// entirely, so an old-generation obligation is teardown material, not retry state.
		owed.done = true
		sess.mu.Unlock()
		return
	}
	sess.mu.Unlock()
	srv := sess.serverFor(c)
	if srv == nil || !srv.admitWillWorker() {
		return
	}
	time.AfterFunc(mqttActionReconcileDelay, func() {
		defer srv.willWorkerWG.Done()
		sess.mu.Lock()
		stop := owed.done || owed.slot.name.gen != sess.gen
		sess.mu.Unlock()
		if stop {
			return
		}
		sess.attemptOwedMaterialization(c, owed)
	})
}

// attemptOwedMaterialization makes ONE attempt to bind a canonical PUBREL record to an
// owed slot, and reschedules itself unless the obligation is discharged or proven not
// ours.
//
// EVERY ATTEMPT REVALIDATES, from this observation: the record, its phase, its identity
// including the transmission counter, custody, and whether a canonical record can be
// proven present. Nothing is carried from enrolment but the target identity.
//
// CUSTODY UNCERTAINTY IS NEVER A TERMINAL OUTCOME. HEAD's acceptance proof is tri-state
// for exactly this reason, and copying a boolean refusal into a scheduler would abandon
// the repair on one transient failure. Unknown acceptance, an ambiguous CAS and a living
// predecessor's token all RETAIN the work; only a PROVEN displacement stops it, and the
// successor's own recovery supplies enrolment there.
//
// Session lock NOT held on entry.
func (sess *mqttSession) attemptOwedMaterialization(c *client, owed *mqttOwedMaterialization) {
	as := sess.asm
	if as == nil {
		return
	}
	retire := func() {
		sess.mu.Lock()
		owed.done = true
		sess.mu.Unlock()
	}
	cur, smsg, lerr := as.loadInflightRecord(owed.slot.name.subject)
	// THE THREE READ OUTCOMES ARE SEPARATED, and the order is not interchangeable: the
	// loader reports a genuine absence and a present-but-unreadable record with the same
	// nil record, so testing `cur == nil` first folded an AUTHORITATIVE ABSENCE — the one
	// outcome that retires this work — into an unproven read and retried it forever.
	switch {
	case lerr != nil:
		// Indeterminate. Nothing is proven, so the obligation stands and is retried.
		sess.scheduleOwedMaterialization(c, owed)
		return
	case smsg == nil:
		retire() // proven absent: the slot is gone and nothing is owed
		return
	case cur == nil:
		// Present but unattributable. Not an absence; the obligation stands.
		sess.scheduleOwedMaterialization(c, owed)
		return
	case cur.Nonce != owed.slot.nonce || cur.Acq != owed.slot.acq:
		// A DIFFERENT transmission owns this identifier now. Not ours, and not a retry
		// target: re-aiming at the newer counter is precisely what C-I forbids.
		retire()
		return
	case cur.State != mqttInflightPubRelIntended:
		// Either the obligation was discharged — the canonical tuple is installed and the
		// slot is `pubrel_ready` or beyond — or the slot never owed one. Either way this
		// work is over.
		retire()
		return
	case cur.ActionToken != _EMPTY_:
		// A LIVING predecessor's action. Materialization needs custody, and custody
		// cannot transfer under a live token; the work is RETAINED rather than retired,
		// because the token may yet drain (C-III's retry continuation).
		sess.scheduleOwedMaterialization(c, owed)
		return
	}
	// CUSTODY, for the SLOT-ONLY (experimental affine) class: a slot this session object does
	// not hold cannot be materialized by it, and the transfer's own tri-state proof decides
	// whether that is a transient condition or a settled displacement. An ORDINARY slot carries
	// no custody claim that decides anything (F-105 7b): materializing its canonical record is
	// the client's own PUBREC answer being completed, which any broker may finish, and the
	// binding CAS below is exact.
	if mqttDurIsSlotOnlyClass(cur.Dur) && cur.Claim != sess.regClaim {
		if !sess.transferCustodyForAdmission(c, owed.slot.name.subject, smsg.Sequence, cur) {
			// Unknown, ambiguous, or refused. None of those is a proven displacement, so
			// the work is retained; a genuine successor's recovery will enrol it and this
			// object's own attempts become no-ops at the identity check above.
			sess.scheduleOwedMaterialization(c, owed)
			return
		}
	}
	// RECONSTRUCTION (L1). The slot carries everything the canonical record needs, which
	// is the point of recording intent before the store: the identifier, the exchange
	// nonce, the source durable and sequence, and the affine pair. NO APPLICATION PAYLOAD
	// IS NEEDED — a PUBREL is a four-byte control packet and the stored record exists to
	// carry the exchange's identity, not its message.
	//
	// AN ALREADY-LANDED RECORD IS RECONCILED, NEVER DUPLICATED. The store may have landed
	// and its response been lost, and a reconstruction per attempt would then write one
	// duplicate every reconcile delay for as long as the binding kept failing. So the
	// incarnation's PUBREL subject is scanned first and an exact record for this exchange
	// is BOUND; only a complete scan that finds none stores one. An incomplete scan proves
	// no absence and retries.
	pubRelSubject, _, dur := mqttPubRelNames(owed.slot.name.idHash, owed.slot.name.gen)
	if pubRelSubject == _EMPTY_ || dur == _EMPTY_ {
		// No incarnation names to write under. Nothing here can succeed and rescheduling
		// would spin: the durable owed phase carries the obligation to the next resume.
		retire()
		return
	}
	ref := mqttSlotRefFromRecord(owed.slot.name.subject, smsg.Sequence, cur)
	if ref == nil {
		retire()
		return
	}
	pubRelSeq, found, complete := sess.findOwedPubRelRecord(pubRelSubject, cur)
	switch {
	case !complete:
		// A partial walk is never evidence of absence. Retry rather than store a second
		// record over one this scan simply did not reach.
		sess.scheduleOwedMaterialization(c, owed)
		return
	case !found:
		msg, hdrLen := mqttNewCorrelatedPubRel(cur.PI, cur.Nonce, cur.Dur, cur.Seq, _EMPTY_,
			cur.Q2AffClaim, cur.Q2AffCopy)
		if msg == nil {
			// The builder refuses a record it cannot attribute. That is a slot this
			// broker did not write, and no amount of retrying makes it writable.
			retire()
			return
		}
		resp, serr := sess.jsa.storeMsg(pubRelSubject, hdrLen, msg)
		if serr != nil || resp == nil || resp.Error != nil {
			// AMBIGUOUS OR FAILED, and the two are not distinguished on purpose: the next
			// attempt's scan answers the question by looking, which is the only honest
			// resolution for a store whose outcome is unknown.
			sess.scheduleOwedMaterialization(c, owed)
			return
		}
		pubRelSeq = resp.Sequence
	}
	if pubRelSeq == 0 {
		sess.scheduleOwedMaterialization(c, owed)
		return
	}
	// THE BINDING IS THE SHARED ONE, readback included: the canonical tuple may be written
	// only from PROOF that the exact record exists at that sequence, never from a store
	// response, because an interest-retained stream answers a store with a sequence while
	// keeping nothing (I12). phaseSlotToPubRelReady performs that proof and the
	// `pubrel_intended -> pubrel_ready` CAS, so there is one definition of the arrow.
	sess.phaseSlotToPubRelReady(c, &mqttPending{
		slot: *ref, jsDur: cur.Dur, sseq: cur.Seq,
		q2AffClaim: cur.Q2AffClaim, q2AffCopy: cur.Q2AffCopy,
	}, pubRelSeq)
	// RESCHEDULED RATHER THAN ASSUMED DISCHARGED. The next attempt re-reads the slot: if
	// the tuple landed it finds a phase that is no longer `pubrel_intended` and retires,
	// and if the CAS lost — a concurrent materialization, or a slot that moved — it tries
	// again against what is actually there. Nothing is inferred from a write's return.
	sess.scheduleOwedMaterialization(c, owed)
}

// findOwedPubRelRecord looks for a canonical PUBREL record that already belongs to this
// slot's exchange, so a materialization that lost its store RESPONSE does not write a
// second one on every attempt.
//
// The match is the exchange's whole identity — identifier, owner nonce, source durable and
// sequence, and the affine pair — through the same predicate recovery uses, so a record
// naming the same source but a different exchange can never be bound.
//
// `complete` is the scan's own verdict and is NOT folded into `found`: a partial walk
// proves no absence, and treating it as one is what would license the duplicate store this
// exists to prevent.
func (sess *mqttSession) findOwedPubRelRecord(pubRelSubject string,
	slot *mqttInflightRecord) (seq uint64, found, complete bool) {

	records, _, ok := sess.scanPubRelRecords(pubRelSubject)
	if !ok {
		return 0, false, false
	}
	for _, rec := range records {
		if rec.shapeErr != nil {
			// A shape this broker does not write. Its fields authorize nothing, and the
			// scan returns it deliberately so each caller applies its own disposition:
			// here it is simply NOT a canonical candidate, and it must not suppress the
			// store either — binding to it would name a record no reader will accept.
			continue
		}
		if mqttStoredPubRelMatchesSlot(rec, slot) {
			return rec.seq, true, true
		}
	}
	return 0, false, true
}

// installTerminalReservationLocked installs the process-local shadow of a durable
// terminal slot. It is called while replacing the delivered PUBREL entry, before the
// session lock is released, so allocation never observes an unreserved interval.
//
// sess.mu must be held.
func (sess *mqttSession) installTerminalReservationLocked(ref mqttInflightSlotRef) bool {
	if !sess.terminalRefOKLocked(ref) {
		return false
	}
	// An EMPTY identifier gets a fresh ref-only reservation: it names only the slot nonce and is
	// released nonce-keyed (releaseTerminalReservation), never by lineage. A resident of this
	// exchange with no delivery ACK subject — a recovered placeholder, or a successor that was
	// never delivered — is REPLACED, carrying its evidence and ledger charge, so the lineage's
	// release still takes it. Anything else is another exchange's, or a live delivery's.
	ack := sess.pendingPubRel[ref.name.pi]
	if ack == nil {
		return sess.setPendingLocked(mqttPendingPubRelMap, ref.name.pi, mqttExpectEmpty(), mqttNewTerminalReservation(ref, nil))
	}
	if ack.regNonce != ref.nonce || ack.jsAckSubject != _EMPTY_ {
		return false
	}
	return sess.setPendingLocked(mqttPendingPubRelMap, ref.name.pi, mqttExpectExactly(ack), mqttNewTerminalReservation(ref, ack))
}

// advanceResidentPubRelPhaseLocked refreshes the resident PUBREL entry's slot PHASE to an
// acquisition of the same slot: only an entry whose captured slot names this exchange (subject
// and nonce) is touched, and only its sequence, phase, acquisition and action fields — the
// freshness fence a later terminal release is checked against. Its kind, evidence and identity
// are not writes this makes.
//
// sess.mu must be held.
func (sess *mqttSession) advanceResidentPubRelPhaseLocked(pi uint16, slot mqttInflightSlotRef) {
	rel := sess.pendingPubRel[pi]
	if rel == nil || !rel.slot.slotOK() || rel.slot.nonce != slot.nonce || rel.slot.name.subject != slot.name.subject {
		return
	}
	rel.slot.seq, rel.slot.state, rel.slot.acq = slot.seq, slot.state, slot.acq
	rel.slot.actionLive, rel.slot.actionToken = slot.actionLive, slot.actionToken
	rel.regSeq = slot.seq
}

// terminalRefOKLocked is the reference a terminal reservation may be installed for.
//
// sess.mu must be held.
func (sess *mqttSession) terminalRefOKLocked(ref mqttInflightSlotRef) bool {
	return ref.slotOK() && ref.name.subject != _EMPTY_ && ref.name.pi != 0 && ref.name.gen == sess.gen
}

// pubCompConvertLocked turns the delivered PUBREL entry a PUBCOMP completes into its terminal
// reservation, under the SAME lock as the handler's checks, so allocation never observes an
// unreserved interval while the durable slot remains. ack is the entry those checks passed,
// captured after the lock was re-taken, so a same-record redelivery that refreshed the ACK
// subject is the one converted and its latest subject is the one returned for acknowledgement.
// Its exact mapping goes; its evidence and ledger charge stay on the reservation; no ACK subject
// does. A registerless RETAINED exchange has no slot to reserve, so its reference fails the
// terminal-reference check and it is simply untracked.
//
// sess.mu must be held.
func (sess *mqttSession) pubCompConvertLocked(pi uint16, ack *mqttPending, ref mqttInflightSlotRef) (jsAckSubject string, reserved bool) {
	if !sess.terminalRefOKLocked(ref) {
		return sess.untrackPubRel(pi), false
	}
	jsAckSubject = ack.jsAckSubject
	if !sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectExactly(ack), mqttNewTerminalReservation(ref, ack)) {
		// The entry the handler's checks passed is no longer resident (a retirement replaced
		// it): whatever stands at pi now is not this PUBCOMP's to remove.
		return _EMPTY_, false
	}
	if sess.pubRelConsumer != nil && len(sess.cpending) > 0 {
		if sseqToPi := sess.cpending[ack.jsDur]; sseqToPi != nil && sseqToPi[ack.sseq] == pi {
			delete(sseqToPi, ack.sseq)
		}
	}
	return jsAckSubject, true
}

// releaseTerminalReservation removes only the empty, nonce-bound placeholder belonging
// to ref and wakes whatever Receive Maximum capacity that removal creates. A later
// delivery under the same packet identifier is never touched.
func (sess *mqttSession) releaseTerminalReservation(ref mqttInflightSlotRef) {
	sess.mu.Lock()
	var deferredNaks []string
	if ack := sess.pendingPubRel[ref.name.pi]; ack != nil &&
		ack.regNonce == ref.nonce && ack.jsAckSubject == _EMPTY_ {
		delete(sess.pendingPubRel, ref.name.pi)
		deferredNaks = sess.deferredNaksLocked()
	}
	sess.mu.Unlock()
	for _, subj := range deferredNaks {
		sess.jsa.sendMsg(subj, AckNak)
	}
}

// releaseRetainedTerminalReservation frees the terminal reservation of a slot that
// finalizeTerminalSlot deliberately KEPT — mqttTerminalRetainedReoffer, where the source may
// still re-offer — and hands off the identifier's EXCLUSION as it does (F-098, holistic
// review round 5).
//
// The reservation genuinely must go: the client already completed this exchange, so holding
// its entry would consume Receive Maximum for a finished delivery and stall a client whose
// Receive Maximum is 1. But the REGISTER is still there by construction — retention is the
// whole point of this disposition — and nothing has confirmed it gone, so the identifier is
// not free even though the capacity is. Those are the two halves this change keeps apart
// everywhere else, and this site had merged them.
//
// The store's fail-closed behaviour over a retained terminal slot does NOT cover it: that
// fence protects a re-mint that goes through storeInflightRegister, and a registerless
// retained send allocates without consulting any register at all (R6). `bumpPI` asks the
// send registry, which is exactly what the exclusion is recorded in.
//
// Capacity release is unchanged — an exclusion charges none — so a Receive Maximum 1 client
// still proceeds.
//
// The exclusion and the slot's cleanup owner are installed in ONE critical section
// (retained_terminal_cleanup.go), so no source settlement can see the one without the other.
//
// Takes the session lock itself.
func (sess *mqttSession) releaseRetainedTerminalReservation(c *client, ref mqttInflightSlotRef) {
	if ref.name.pi != 0 && ref.name.subject != _EMPTY_ {
		sess.mu.Lock()
		sess.excludeIdentifierLocked(ref.name.pi, ref.name.subject, ref.nonce)
		if w := sess.enrollRetainedTerminalCleanupLocked(c, ref); w != nil {
			sess.scheduleRetainedCleanupLocked(w, sess.retainedCleanupDelayLocked(0))
		}
		sess.mu.Unlock()
	}
	sess.releaseTerminalReservation(ref)
}

// clearTerminalRetirementLocked_ removes the source suppression belonging to a terminal
// exchange after its slot deletion is confirmed. At that point finalizeTerminalSlot has
// already proved the source Complete or Lost, so no future offer exists to suppress.
// Identity is checked in full: a later exchange using the same map key is untouched.
//
// Takes the session lock itself.
func (sess *mqttSession) clearTerminalRetirementLocked_(ref mqttInflightSlotRef) {
	key := mqttDeferredKey{ref.dur, ref.sseq}
	sess.mu.Lock()
	if rs := sess.srcAckOwed[key]; rs != nil && rs.pi == ref.name.pi && rs.nonce == ref.nonce {
		delete(sess.srcAckOwed, key)
	}
	sess.mu.Unlock()
}

// retainAndReconcileTerminal keeps a completed exchange's identifier reserved while its
// durable terminal cleanup is indeterminate, and arranges an unlimited, shutdown-aware
// retry. The slot remains the authority; the pending entry is only its process-local
// shadow so bumpPI cannot re-mint the identifier before a later successful cleanup.
//
// The reservation is deliberately distinguishable from a delivered PUBREL: it has no
// acknowledgement subject or consumer sequence. A retry removes only that exact
// nonce-bound placeholder, so it cannot untrack a later delivery that reused the map key.
//
// Session lock NOT held on entry.
func (sess *mqttSession) retainAndReconcileTerminal(c *client, ref mqttInflightSlotRef) {
	if !ref.slotOK() || ref.name.subject == _EMPTY_ || ref.name.pi == 0 {
		return
	}

	sess.mu.Lock()
	if ref.name.gen != sess.gen {
		sess.mu.Unlock()
		return
	}
	ack := sess.pendingPubRel[ref.name.pi]
	if ack == nil {
		if !sess.installTerminalReservationLocked(ref) {
			sess.mu.Unlock()
			return
		}
		ack = sess.pendingPubRel[ref.name.pi]
	} else if ack.regNonce != ref.nonce || ack.jsAckSubject != _EMPTY_ {
		// A different live entry already reserves this identifier. Do not overwrite its
		// evidence; the durable terminal slot remains for recovery to reconcile.
		sess.mu.Unlock()
		return
	} else {
		// This exchange's resident with no delivery ACK subject — possibly a successor never
		// delivered, which after PUBCOMP acts for nothing more — becomes the terminal
		// reservation, by replacement.
		if !sess.setPendingLocked(mqttPendingPubRelMap, ref.name.pi, mqttExpectExactly(ack), mqttNewTerminalReservation(ref, ack)) {
			sess.mu.Unlock()
			return
		}
		ack = sess.pendingPubRel[ref.name.pi]
	}
	if ack.terminalReconcile {
		sess.mu.Unlock()
		return
	}
	ack.terminalReconcile = true
	sess.mu.Unlock()

	var srv *Server
	if c != nil {
		srv = c.srv
	}
	if srv == nil || !srv.admitWillWorker() {
		// No worker was admitted, so leave the reservation retryable by a later recovery
		// or event rather than marking a worker that does not exist.
		sess.mu.Lock()
		if ack := sess.pendingPubRel[ref.name.pi]; ack != nil &&
			ack.regNonce == ref.nonce && ack.jsAckSubject == _EMPTY_ {
			ack.terminalReconcile = false
		}
		sess.mu.Unlock()
		return
	}

	time.AfterFunc(mqttActionReconcileDelay, func() {
		defer srv.willWorkerWG.Done()
		sess.mu.Lock()
		retiredGeneration := ref.name.gen != sess.gen
		sess.mu.Unlock()
		if retiredGeneration {
			sess.releaseTerminalReservation(ref)
			return
		}
		// DISPATCHED EXPLICITLY, because `settled()` is TRUE for a retained slot and so
		// cannot gate a release (F-098, holistic review round 6). This worker is scheduled
		// by an INDETERMINATE cleanup, so a cold recovery reaches it with no send hold at
		// all; if the retry then reads the register successfully while its source is Live
		// or Unknown, finalization deliberately KEEPS that register — and a bare release
		// here would remove the only local reservation standing over it.
		switch out := sess.mqttTerminalizeOnPubComp(c, ref).outcome; {
		case out == mqttTerminalRetainedReoffer:
			// The fourth arm of this disposition, and it owes exactly what the other three
			// owe: the capacity goes, the identifier stays excluded, and the retained slot
			// is reconciled independently once its source settles.
			sess.releaseRetainedTerminalReservation(c, ref)
			return
		case out.settled():
			sess.releaseTerminalReservation(ref)
			return
		}

		// Hand the unresolved reservation to another admitted attempt. Clear the bit
		// first so retainAndReconcileTerminal can own the next worker token.
		sess.mu.Lock()
		if ack := sess.pendingPubRel[ref.name.pi]; ack != nil &&
			ack.regNonce == ref.nonce && ack.jsAckSubject == _EMPTY_ {
			ack.terminalReconcile = false
		}
		sess.mu.Unlock()
		sess.retainAndReconcileTerminal(c, ref)
	})
}

// mqttTerminalOutcome is the tri-state result of PUBCOMP's durable terminal step, because
// two different NON-completions owe the caller different things (Decision: consult 7). An
// INDETERMINATE cleanup keeps the in-memory reservation and retries; a slot DELIBERATELY
// RETAINED for a source that may still be re-offered releases the CAPACITY while the
// identifier stays excluded and the source-keyed suppression stays, and the slot's cleanup
// owner (retained_terminal_cleanup.go) holds the obligation until the slot is gone.
// Conflating the two over-reserves Receive Maximum for an exchange the client has already
// completed.
type mqttTerminalOutcome int

const (
	// mqttTerminalIndeterminate: cleanup could not be confirmed. Keep the reservation and retry.
	mqttTerminalIndeterminate mqttTerminalOutcome = iota
	// mqttTerminalDone: the durable terminal step is settled and needs no in-memory
	// reservation. That covers three shapes — the slot is gone (cleanup confirmed the
	// delete), it belongs to another exchange, OR (from `pubrel_sending`) it advanced to
	// `pubrel_terminal_draining`, where the holder's own drain owns the final arrow and the
	// live action still holds the identifier. Release the reservation in every case.
	mqttTerminalDone
	// mqttTerminalRetainedReoffer: the terminal slot is intentionally kept because its source
	// may still be re-offered. Release the capacity, keep the identifier excluded and the
	// source suppression; the slot is finished by its registered cleanup owner
	// (enrollRetainedTerminalCleanupLocked), not by holding this reservation.
	mqttTerminalRetainedReoffer
	// mqttTerminalHeldByAction: the slot is terminal but an ACTION IS LIVE on it — a PUBREL
	// write whose bytes are still out, ours or a peer's. Nothing may be disposed of under a
	// holder that may still act on the record (C-IV arm 2), so NOTHING was touched: no
	// duplicate scan, no affine discharge, no canonical acknowledgement, no delete. The
	// holder's own drain finishes cleanup from the phase it observes.
	//
	// It is NOT settled: by default the caller keeps its reservation and reconciles, because
	// a token whose holder never drains still owns the identifier. The one caller that may
	// release its in-memory entry is PUBCOMP's own path, and only when this broker's send
	// hold is what reserves the identifier — the hold then carries the reservation, and
	// keeping the entry as well would consume Receive Maximum for an exchange the client has
	// already completed.
	mqttTerminalHeldByAction
)

// settled reports whether the durable terminal step reached a definite conclusion — the slot
// is gone, belongs to another exchange, is draining under the holder's own action, or is
// deliberately retained for a re-offerable source. In every settled case the caller releases
// the in-memory reservation; a retained slot's identifier stays excluded under its cleanup
// owner. Only mqttTerminalIndeterminate keeps the reservation and retries.
func (o mqttTerminalOutcome) settled() bool {
	return o != mqttTerminalIndeterminate && o != mqttTerminalHeldByAction
}

// completed reports mqttTerminalDone specifically: settled AND not a deliberately retained
// slot, so cleanup ran to a confirmed delete or the slot already belonged to another exchange.
// A retained slot is settled but NOT completed. (In PUBCOMP's own path Done is also returned
// for a `pubrel_terminal_draining` slot, where the live drain — not this predicate — owns
// finalization.)
func (o mqttTerminalOutcome) completed() bool { return o == mqttTerminalDone }

// mqttPubCompResult is PUBCOMP's durable conclusion: the terminal outcome, and whether the
// completion itself is DURABLY RECORDED — the slot at pubrel_terminal beside an ended or
// pending-drain exchange, or the slot provably not this exchange's any more.
//
// recorded == false means UNCONFIRMED, not absent: a batch may have landed behind a lost
// response and a failed read. It is what withholds the canonical PUBREL's JetStream ack, so a
// completion this broker cannot show is durable never loses the record that lets the client's
// next PUBCOMP (or recovery) finish it.
type mqttPubCompResult struct {
	outcome  mqttTerminalOutcome
	recorded bool
	// heldToken is the durable action token finalize observed when the outcome is
	// mqttTerminalHeldByAction: delegation to a local drain is valid only for THAT action.
	heldToken string
}

// mqttTerminalizeOnPubComp performs PUBCOMP's durable half: the named arrow out of the state
// the exchange is in, and then the terminal release.
//
// For an ORDINARY obligation (ackExchangeFor), the slot's `pubrel_ready -> pubrel_terminal`
// and the exchange's PUBREC-observed -> Terminal (no PUBREL action live) or ->
// TerminalPendingDrain (a PUBREL action live) are ONE atomic batch through advanceExchangeAck,
// the phase derived by mqttPhaseForAck. An affine slot has no exchange and keeps its
// slot-only arrow. An ordinary slot whose exchange cannot be read writes nothing.
//
// PUBCOMP does not revoke an action, it records that the exchange finished: a live token is
// KEPT on the terminal slot, finalizeTerminalSlot refuses disposal while it stands, and the
// holder's drain completes both records when its write returns (startActionDrain).
//
// A LOOP, because PUBCOMP, the action's drain and an acquisition race the same slot, and
// whichever loses re-reads and re-derives rather than giving up on the arrow. Bounded by
// mqttInflightStoreAttempts: exhaustion is not a refusal — the completion worker continues it,
// and the JetStream ack stays withheld — so PUBREC's longer budget (which exists because its
// exhaustion closed the connection) is not needed here. The FINAL attempt's error may hide a
// landed batch, so exhaustion ends in one resolving read.
//
// Session lock NOT held on entry.
func (sess *mqttSession) mqttTerminalizeOnPubComp(c *client, ref mqttInflightSlotRef) mqttPubCompResult {
	// A FOREGROUND authority operation for its whole span (outbound_foreground_priority.go).
	release := sess.announceForegroundAuthority()
	defer release()
	if !ref.slotOK() || ref.name.subject == _EMPTY_ {
		return mqttPubCompResult{outcome: mqttTerminalDone, recorded: true}
	}
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
		switch {
		case err != nil:
			// Indeterminate. Neither an absence nor a licence.
			return mqttPubCompResult{outcome: mqttTerminalIndeterminate}
		case smsg == nil:
			// Gone: this broker deletes a slot only once it is terminal, so the completion is
			// recorded as far as this exchange is concerned.
			return mqttPubCompResult{outcome: mqttTerminalDone, recorded: true}
		case cur == nil:
			// PRESENT BUT UNREADABLE is not absence.
			return mqttPubCompResult{outcome: mqttTerminalIndeterminate}
		}
		if !ref.matchesRecord(cur) {
			return sess.pubCompOverSuccessor(ref)
		}
		ref.seq = smsg.Sequence
		ref.state = cur.State
		switch cur.State {
		case mqttInflightPubRelReady:
			class, x := sess.ackExchangeFor(cur, ref.name.gen)
			switch class {
			case mqttAckSlotOnly:
				seq, ok := sess.casInflightSlot(c, ref, mqttInflightPubRelReady, mqttSlotAdvancePhase,
					func(next *mqttInflightRecord) {
						next.State = mqttInflightPubRelTerminal
					})
				if !ok {
					continue
				}
				// The ADVANCE'S OWN SEQUENCE, carried forward: finalizeTerminalSlot fences on
				// the observed sequence and the CAS just moved it.
				ref.seq = seq
				ref.state = mqttInflightPubRelTerminal
				return sess.finalizeRecordedPubComp(c, ref)
			case mqttAckRefused:
				// An ordinary slot without a readable matching exchange: no route is licensed,
				// nothing is written, and the completion stays unconfirmed.
				return mqttPubCompResult{outcome: mqttTerminalIndeterminate}
			}
			// The phase is DERIVED: PUBCOMP ends the exchange whatever its reason code, and a
			// live PUBREL token is the outstanding output that makes the ending pending-drain.
			// The dispatch site has no reason code (mqttProcessPubComp(pi)); success is the
			// honest translation mqttAckArrival.Reason documents. The token is the only output
			// carrier a PUBREL has at this checkpoint — PUBREL acquisition writes no F-105
			// action record until row 2 converts it, and no PUBLISH action can be live at
			// pubrel_ready because every PUBREL acquisition requires an empty token.
			to, perr := mqttPhaseForAck(mqttAckArrival{
				Packet: mqttPacketPubComp, Reason: mqttReasonSuccess,
				OutstandingOutput: cur.ActionToken != _EMPTY_,
			}, x.QoS)
			if perr != nil {
				return mqttPubCompResult{outcome: mqttTerminalIndeterminate}
			}
			if _, aerr := sess.advanceExchangeAck(&mqttAdvanceExchangeAckRequest{
				ID: sess.id, Epoch: sess.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
				Gen: ref.name.gen, Nonce: cur.Lin, To: to, Acq: cur.Acq,
				Register: cur, RegisterSeq: smsg.Sequence,
			}); aerr != nil {
				// The next iteration's load IS the authoritative re-read: a landed batch behind
				// a lost response shows pubrel_terminal there.
				continue
			}
			// ACKNOWLEDGED: the completion is durable. The op returns the AUTHORITY sequence,
			// so finalize needs the slot's own, read back.
			return sess.finalizeRecordedPubComp(c, ref)
		case mqttInflightPubRelTerminal:
			// A retried PUBCOMP, a lost response, or the drain finished it. Cleanup is
			// idempotent and neither retransmits nor re-consumes debt.
			return sess.finalizeTerminalObservedPubComp(c, ref, cur)
		default:
			return mqttPubCompResult{outcome: mqttTerminalIndeterminate}
		}
	}
	// EXHAUSTED. The final attempt's error may hide a batch that landed, so one resolving read
	// decides rather than the attempt count.
	cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
	switch {
	case err != nil || (smsg != nil && cur == nil):
		return mqttPubCompResult{outcome: mqttTerminalIndeterminate}
	case smsg == nil:
		return mqttPubCompResult{outcome: mqttTerminalDone, recorded: true}
	case !ref.matchesRecord(cur):
		return sess.pubCompOverSuccessor(ref)
	case cur.State == mqttInflightPubRelTerminal:
		ref.seq = smsg.Sequence
		ref.state = cur.State
		return sess.finalizeTerminalObservedPubComp(c, ref, cur)
	}
	return mqttPubCompResult{outcome: mqttTerminalIndeterminate}
}

// pubCompOverSuccessor is PUBCOMP meeting another exchange on its identifier. PRESERVE it:
// rewriting the current occupant is the cross-exchange mutation F-092 exists to stop. Done
// discharges THIS PUBCOMP's obligation and nothing more; the successor's identifier stays
// excluded, because the caller frees the terminal reservation on this answer (F-098).
func (sess *mqttSession) pubCompOverSuccessor(ref mqttInflightSlotRef) mqttPubCompResult {
	sess.mu.Lock()
	sess.excludeIdentifierLocked(ref.name.pi, ref.name.subject, _EMPTY_)
	sess.mu.Unlock()
	return mqttPubCompResult{outcome: mqttTerminalDone, recorded: true}
}

// finalizeRecordedPubComp finalizes a completion this call just recorded, from the slot's own
// sequence read back after the write. The completion is durable either way; only the cleanup
// can remain unsettled.
func (sess *mqttSession) finalizeRecordedPubComp(c *client, ref mqttInflightSlotRef) mqttPubCompResult {
	cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
	switch {
	case err != nil || (smsg != nil && cur == nil):
		return mqttPubCompResult{outcome: mqttTerminalIndeterminate, recorded: true}
	case smsg == nil:
		return mqttPubCompResult{outcome: mqttTerminalDone, recorded: true}
	case !ref.matchesRecord(cur):
		return sess.pubCompOverSuccessor(ref)
	case cur.State != mqttInflightPubRelTerminal:
		return mqttPubCompResult{outcome: mqttTerminalIndeterminate, recorded: true}
	}
	ref.seq = smsg.Sequence
	ref.state = mqttInflightPubRelTerminal
	out, tok := sess.finalizeTerminalSlotObserved(c, ref)
	return mqttPubCompResult{outcome: out, recorded: true, heldToken: tok}
}

// finalizeTerminalObservedPubComp finalizes a slot OBSERVED at pubrel_terminal. For an ordinary
// obligation the atomic batch makes its exchange Terminal or TerminalPendingDrain; any other
// pairing is a shape this broker does not write, so the completion is not treated as recorded.
func (sess *mqttSession) finalizeTerminalObservedPubComp(c *client, ref mqttInflightSlotRef,
	cur *mqttInflightRecord) mqttPubCompResult {

	if class, x := sess.ackExchangeFor(cur, ref.name.gen); class != mqttAckSlotOnly {
		if class != mqttAckExchange ||
			(x.Phase != mqttExchangeTerminal && x.Phase != mqttExchangeTerminalPendingDrain) {
			return mqttPubCompResult{outcome: mqttTerminalIndeterminate}
		}
	}
	out, tok := sess.finalizeTerminalSlotObserved(c, ref)
	return mqttPubCompResult{outcome: out, recorded: true, heldToken: tok}
}

// finalizeTerminalSlot completes a terminal exchange and exact-deletes its slot.
//
// THREE things are required for the deletion, not two, and the third is the one an
// implementation forgets: subject addressing (F-034), identity authorization, and a
// FRESHNESS FENCE on the observed sequence. A rewrite between the observation and the
// delete means a concurrent operation re-targeted the exchange and reinstalled memory
// around it, so a stale terminal continuation must PRESERVE the record rather than delete
// it. The "a moved register is preserved" case of
// TestMQTTInflightTerminalCleanupRemovesRegisterThenMarker is the fixture for the opposite.
//
// Session lock NOT held on entry.
func (sess *mqttSession) finalizeTerminalSlot(c *client, ref mqttInflightSlotRef) mqttTerminalOutcome {
	out, _ := sess.finalizeTerminalSlotObserved(c, ref)
	return out
}

// finalizeTerminalSlotObserved is finalizeTerminalSlot reporting, for mqttTerminalHeldByAction,
// the durable token it observed: a caller may delegate cleanup to a local drain only when that
// drain's action holds exactly this token (sess.localActionHoldsLocked).
//
// Session lock NOT held on entry.
func (sess *mqttSession) finalizeTerminalSlotObserved(c *client, ref mqttInflightSlotRef) (mqttTerminalOutcome, string) {
	sess.mu.Lock()
	absentInst := sess.slotExclusionInstLocked(ref.name.pi)
	sess.mu.Unlock()
	cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
	if err != nil {
		return mqttTerminalIndeterminate, _EMPTY_
	}
	if smsg == nil {
		// ALREADY GONE, which is the same fact the confirmed delete below establishes: the
		// subject is provably empty, so the exclusion that slot justified is lifted — fenced
		// by the instance captured BEFORE the read, so a restriction installed while it was
		// out survives. Without it, a retained arm that ran late (it observed the source live,
		// then another owner finished the slot) re-installs an exclusion nothing would lift.
		if absentInst != 0 {
			sess.mu.Lock()
			sess.liftIdentifierExclusionLocked(ref.name.pi, absentInst,
				map[string]bool{ref.name.subject: true})
			sess.mu.Unlock()
		}
		return mqttTerminalDone, _EMPTY_
	}
	if cur == nil {
		return mqttTerminalIndeterminate, _EMPTY_
	}
	if !ref.matchesRecord(cur) {
		// A REPLACEMENT, not a completion. This continuation's own obligation is over —
		// which is why Done stays the answer — but a successor occupies the subject and its
		// identifier is not ours to free. The caller releases the terminal reservation on
		// Done, so the exclusion is handed off first, nonce-less because the occupant is not
		// ours to name (F-098).
		sess.mu.Lock()
		sess.excludeIdentifierLocked(ref.name.pi, ref.name.subject, _EMPTY_)
		sess.mu.Unlock()
		return mqttTerminalDone, _EMPTY_
	}
	if cur.State != mqttInflightPubRelTerminal {
		return mqttTerminalIndeterminate, _EMPTY_
	}
	if ref.seq != 0 && smsg.Sequence != ref.seq {
		// THE FRESHNESS FENCE. The slot was rewritten after this continuation observed
		// it, so whatever re-targeted the exchange owns it now.
		return mqttTerminalIndeterminate, _EMPTY_
	}
	if cur.ActionToken != _EMPTY_ {
		// A LIVE ACTION, BEFORE ANY DISPOSAL WORK. The deleted `pubrel_terminal_draining`
		// phase used to keep this shape out of terminal cleanup altogether; PUBCOMP now
		// advances a live-token slot to `pubrel_terminal`, so the guard lives here and at
		// the one delete door. Discharging affine proof or acknowledging canonical records
		// under a holder that may still transmit would destroy evidence it can still need.
		return mqttTerminalHeldByAction, cur.ActionToken
	}
	// A SOURCE THAT MAY STILL BE RE-OFFERED KEEPS ITS SLOT, and this is the one
	// obligation terminal cleanup must not skip.
	//
	// The terminal phase itself proves this exchange's SOURCE is past PUBREC, including
	// the crash window before an explicit Retired mark lands. The source may still be
	// re-offered, and the slot can then be the only cross-restart evidence that its payload
	// was already answered. Deleting it here re-enables that redelivery after PUBCOMP has
	// removed the PUBREL record. So a Live or Unknown source retains the terminal slot;
	// recovery installs the matching in-memory retirement before it restores source
	// interest.
	//
	// Lost is different from Complete but equally non-reofferable. It is NOT a licence to
	// reap a PUBLISH-phase slot, because a disappeared consumer does not prove the client
	// finished that exchange. Here PUBCOMP already supplies that proof, and Lost's positive
	// meaning is that nothing can ever re-offer this source. This is the completed-drain
	// case: its claim was durably removed and its consumer was then deleted before PUBCOMP.
	// A RETAINED exchange has no source to re-offer and no $MQTT_out record (F-105 7c): its
	// canonical PUBREL is the in-flight record its PUBREC created, removed by exact identity.
	retainedSlot := cur.retainedSource()
	if retainedSlot && !sess.disposeRetainedRel(cur) {
		return mqttTerminalIndeterminate, _EMPTY_
	}
	if !retainedSlot && mqttInflightSourceMayReoffer(sess.asm.inflightSourceState(cur)) {
		// DELIBERATELY RETAINED for a source that may still be re-offered. The caller
		// releases the capacity, keeps the identifier excluded and hands the slot to its
		// cleanup owner, which every confirmed settlement of this source wakes. This is NOT
		// the indeterminate case and must not keep the Receive Maximum reservation for an
		// exchange the client already completed.
		return mqttTerminalRetainedReoffer, _EMPTY_
	}
	// COMPLETE THE RECORD SIDE BEFORE THE SLOT SIDE. A peer may have stored more than one
	// same-exchange PUBREL before the phase gate closed record creation. Every such record
	// must have its affine proof discharged first and then be removed with a confirmed JS
	// operation. A partial enumeration, an uninterpretable same-exchange record, a failed
	// proof discharge or a failed removal keeps the terminal slot: deleting it first would
	// leave a slot-less record that admission can only refuse forever.
	if !retainedSlot && !sess.disposeTerminalPubRelRecords(c, cur) {
		return mqttTerminalIndeterminate, _EMPTY_
	}

	// The duplicate scan and its deletes are round trips. Re-read after them and require
	// the exact sequence this continuation observed; this is the freshness fence at the
	// irreversible edge, not merely an early advisory check.
	cur, smsg, err = sess.asm.loadInflightRecord(ref.name.subject)
	if err != nil || cur == nil || smsg == nil || smsg.Sequence != ref.seq ||
		!ref.matchesRecord(cur) || cur.State != mqttInflightPubRelTerminal {
		return mqttTerminalIndeterminate, _EMPTY_
	}
	// No licence: a terminal slot is completion evidence, never an unsent obligation.
	if !sess.reapRegisterWithVerdictGated(smsg.Sequence, cur.Nonce, true, &ref, mqttSlotDeleteTerminal, mqttDestroyLicence{}) {
		return mqttTerminalIndeterminate, _EMPTY_
	}
	if retainedSlot {
		sess.wakeRetainedReclaimForPayload(ref.name.gen, cur.Payload)
	}
	// A confirmed exact-sequence delete can race a same-identity rewrite: the old sequence
	// is then absent and its delete succeeds while the rewritten slot survives. Confirm the
	// SUBJECT consequence before releasing the in-memory reservation.
	sess.mu.Lock()
	preInst := sess.slotExclusionInstLocked(ref.name.pi)
	sess.mu.Unlock()
	cur, smsg, err = sess.asm.loadInflightRecord(ref.name.subject)
	if err != nil {
		return mqttTerminalIndeterminate, _EMPTY_
	}
	if smsg == nil {
		// THE SUBJECT IS PROVABLY EMPTY, which is the one fact that releases an identifier's
		// exclusion: this exchange is over and no successor took its slot. Lifting here is
		// what keeps an ordinary completed PUBREL from stranding its identifier — the drain's
		// retirement installs an exclusion for every outcome it cannot prove completing, and
		// this is where that proof arrives.
		//
		// `preInst` was captured BEFORE the read above, so a restriction installed while that
		// read was out is not cleared by an absence that never observed it.
		if preInst != 0 {
			sess.mu.Lock()
			sess.liftIdentifierExclusionLocked(ref.name.pi, preInst,
				map[string]bool{ref.name.subject: true})
			sess.mu.Unlock()
		}
		sess.clearTerminalRetirementLocked_(ref)
		return mqttTerminalDone, _EMPTY_
	}
	if cur == nil {
		return mqttTerminalIndeterminate, _EMPTY_
	}
	if !ref.matchesRecord(cur) {
		// A SUCCESSOR TOOK THE SUBJECT AFTER THE DELETE, which is a different outcome from
		// the early mismatch above: the exact-sequence delete can succeed against the
		// already-absent old sequence while a peer's replacement survives. This obligation
		// is discharged — hence Done — but the caller frees the terminal reservation on
		// that answer, and the successor's identifier is not ours to free (F-098).
		sess.mu.Lock()
		sess.excludeIdentifierLocked(ref.name.pi, ref.name.subject, _EMPTY_)
		sess.mu.Unlock()
		sess.clearTerminalRetirementLocked_(ref)
		return mqttTerminalDone, _EMPTY_
	}
	return mqttTerminalIndeterminate, _EMPTY_
}

// disposeTerminalPubRelRecords performs §8's complete terminal scan.
//
// The scan is scoped to this session incarnation's PUBREL subject and considers only
// records that positively match the terminal slot's exchange identity. Unrelated and
// different-nonce records are never destroyed on this slot's behalf. A matching record
// must also carry the slot's exact source and affine identity; otherwise it is not a
// record this terminal continuation can safely interpret and the slot is retained.
//
// deleteMsg(wait=true) is used instead of synthesising a JetStream ack reply. A scan sees
// stream records, not delivery attempts, so it does not possess the consumer/delivery
// sequence tuple needed to construct a truthful ack subject. The confirmed exact stream
// deletion is the stronger disposal proof available for both canonical and duplicate
// records and removes them from the interest-retained stream before the owner slot goes.
func (sess *mqttSession) disposeTerminalPubRelRecords(c *client, slot *mqttInflightRecord) bool {
	if slot == nil || slot.State != mqttInflightPubRelTerminal || slot.PI == 0 ||
		slot.Nonce == _EMPTY_ || slot.PubRelDurable == _EMPTY_ {
		return false
	}
	sess.mu.Lock()
	pubRelSubject := sess.pubRelSubject
	idHash, gen := sess.idHash, sess.gen
	pubRelDurable := sess.pubRelDurable()
	sess.mu.Unlock()
	// Recovery finalizes terminal slots before ensurePubRelConsumerSubscription restores
	// sess.pubRelConsumer. The durable is generation-derived, so requiring the in-memory
	// config here would make every crash-after-terminal slot immortal. Derive the same name
	// directly; the scan itself needs only the subject and stream.
	if pubRelDurable == _EMPTY_ {
		_, _, pubRelDurable = mqttPubRelNames(idHash, gen)
	}
	if pubRelSubject == _EMPTY_ || pubRelDurable == _EMPTY_ || pubRelDurable != slot.PubRelDurable {
		return false
	}
	records, _, complete := sess.scanPubRelRecords(pubRelSubject)
	if !complete {
		return false
	}
	for _, rec := range records {
		if rec.pi != slot.PI || rec.owner.State != mqttPubRelOwnerValid ||
			rec.owner.Nonce != slot.Nonce {
			continue
		}
		if rec.shapeErr != nil || rec.srcDur != slot.Dur || rec.srcSeq != slot.Seq {
			return false
		}
		if slot.Q2AffClaim == _EMPTY_ {
			if rec.affRef.State != mqttPubRelAffineRefAbsent {
				return false
			}
		} else {
			if !rec.affRef.valid() || rec.affRef.Claim != slot.Q2AffClaim ||
				rec.affRef.Copy != slot.Q2AffCopy {
				return false
			}
		}
		if !sess.dischargeQ2AffPubRelProof(c, slot.PI,
			mqttDeferredKey{pubRelDurable, rec.seq}, rec.affRef) {
			return false
		}
		if err := sess.jsa.deleteMsg(mqttOutStreamName, rec.seq, true); err != nil {
			return false
		}
	}
	return true
}

// mqttSlotAdvanceOutcome is what a required terminal advance concluded, and it is
// three-valued because "I may not" and "I could not find out" owe the resume different
// things: the first is a normal observation, the second is unresolved work that must not
// be carried past a CONNACK.
type mqttSlotAdvanceOutcome int

const (
	// mqttSlotAdvanceUnresolved: the advance was required and its outcome is unknown.
	// Refuse the resume; the next CONNECT re-establishes the proof and retries.
	mqttSlotAdvanceUnresolved mqttSlotAdvanceOutcome = iota
	// mqttSlotAdvanceNotOurs: a live action, or another exchange holds the identifier.
	// Nothing is owed here and the resume continues.
	mqttSlotAdvanceNotOurs
	// mqttSlotAdvanced: the slot is terminal (or provably gone) and finalization may run.
	mqttSlotAdvanced
)

// advanceFinishedSlotToTerminal moves a past-PUBREC slot that recovery has PROVEN finished
// into `pubrel_terminal`, which is the only state terminal finalization accepts.
//
// This is the missing arrow, not a relaxation of the gate, and the distinction is the
// whole point. `mqttSlotDeleteComplete` is refused from `pubrel_ready` deliberately: a
// ready slot is the identifier's live durable claimant, and letting an ordinary reap vacate
// one is precisely the hazard F-092 exists to close. For a SHARED or AFFINE slot recovery can
// hold a proof the exchange is over — the source is provably complete and no PUBREL record
// names it — and there the exchange did finish: either the client's PUBCOMP acknowledged the
// record away, or the record was never stored, and nothing can transmit it either way.
//
// NOT ON THAT PROOF FOR AN ORDINARY SLOT. Its PUBCOMP is recorded with its exchange in one batch
// and the canonical record's ack is withheld until it is, so "no record" is an absence and no
// proof (the source is acked at PUBREC, so "provably complete" adds nothing). An ordinary slot
// reaches this advance only on its EXCHANGE's word: a durably Terminal exchange for the exact
// obligation (ordinaryExchangeEnded) — the completion is already recorded, and a pubrel_ready slot
// beside it is one a store revived (MAXMSGSPER-REVIVAL). The advance then only reflects that fact
// into the slot; it writes no exchange.
//
// So the proof licenses an ADVANCE, and the advance licenses the delete. Without this a
// re-phased slot whose delete was lost stays reserved for the life of the session, which
// with Receive Maximum 1 stops the session receiving anything at all —
// TestMQTTInflightStaleRegisterLeavesNoPhantomSlot asserts exactly that on the wire.
//
// # Why the outcome is three-valued
//
// A bool conflated two facts the caller must treat differently. "The slot is not mine to
// finish" — an action is live, or another exchange holds the subject — is a normal
// observation the resume continues past. "I could not find out" — a failed CAS, an
// unreadable record, an indeterminate read — is UNRESOLVED WORK, and continuing past it
// accepted a resume whose identifier stays charged with nothing left to retry the advance.
// A finite storage blip permanently consumed a Receive Maximum slot on a session that
// looked healthy.
//
// So an unresolved advance now REFUSES THE RESUME instead, through the pre-CONNACK refusal
// the caller already has. The next CONNECT re-runs recovery and re-establishes the proof.
// A refused resume is visible and self-correcting; an accepted one with a charged
// identifier is neither.
//
// Session lock NOT held on entry.
func (sess *mqttSession) advanceFinishedSlotToTerminal(c *client, ref *mqttInflightSlotRef) mqttSlotAdvanceOutcome {
	if ref == nil || !ref.slotOK() || ref.name.subject == _EMPTY_ {
		// An unusable reference is not a licence to continue: this pass cannot say what
		// happened to the slot it was asked about.
		return mqttSlotAdvanceUnresolved
	}
	switch ref.state {
	case mqttInflightPubRelTerminal:
		// Terminal finalization is already legal from here.
		return mqttSlotAdvanced
	case mqttInflightPublish, mqttInflightPublishTerminal:
		// This helper consumes a past-PUBREC completion proof. Ordinary PUBLISH cleanup
		// has its own disposition and may not manufacture a terminal phase, and
		// publish_terminal already IS one this broker wrote.
		return mqttSlotAdvanceNotOurs
	case mqttInflightPubRelIntended:
		// OWED a canonical record. Advancing it to terminal would discard an obligation
		// nothing has yet materialized; materialization is its continuation, not this.
		return mqttSlotAdvanceNotOurs
	}
	if ref.actionLive {
		// AN ACTION IS LIVE. Recovery may not advance or delete this: the token is
		// somebody's capability, and observing that one exists proves phase progress
		// rather than authority to act. The identifier stays reserved and the exchange
		// stays non-actionable until its own holder drains it (the disclosed residual).
		//
		// Asked of the TOKEN now, not of the phase: a token is legal in every phase, so a
		// phase-shaped question here would miss a live PUBREL transmission on a
		// `pubrel_ready` slot entirely.
		return mqttSlotAdvanceNotOurs
	}
	// pubrel_ready, with recovery's proof behind it.
	seq, ok := sess.casInflightSlot(c, *ref, mqttInflightPubRelReady, mqttSlotAdvancePhase,
		func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelTerminal
		})
	if ok {
		ref.seq = seq
		ref.state = mqttInflightPubRelTerminal
		return mqttSlotAdvanced
	}
	// THE CAS RETURNS ONE `false` FOR SEVERAL DIFFERENT WORLDS — a storage failure, a
	// disappearance, a replacement, a phase that moved — so the disposition cannot be read
	// off it. Read the slot back and classify what is actually there.
	cur, smsg, lerr := sess.asm.loadInflightRecord(ref.name.subject)
	switch {
	case lerr != nil:
		// Indeterminate. Not an absence, and not a licence to proceed.
		return mqttSlotAdvanceUnresolved
	case smsg == nil:
		// DEFINITELY ABSENT, and that is not an inference from absence: recovery already
		// PROVED this exchange finished before the slot was queued for the advance. The
		// absence adds no protocol conclusion and authorizes no acknowledgement — it says
		// only that the advance this pass owed has nothing left to act on. Finalization
		// reloads independently and the release removes only the matching empty
		// reservation, so both remain safe on a slot that is already gone.
		return mqttSlotAdvanced
	case cur == nil:
		// Present but unreadable.
		return mqttSlotAdvanceUnresolved
	case !mqttInflightRecordMatchesSubject(cur, ref.name.subject):
		// A body that does not correspond to the subject it was loaded from is
		// UNATTRIBUTABLE, not evidence of another exchange — the reader treats this shape
		// as unreadable everywhere else, and the CAS refuses it separately. Classifying it
		// as somebody else's would let an unreadable slot pass for a resolved one.
		return mqttSlotAdvanceUnresolved
	case cur.ActionToken != _EMPTY_:
		// An action arrived while this advance was in flight.
		return mqttSlotAdvanceNotOurs
	case !ref.matchesRecord(cur):
		// A different exchange holds the identifier now, which is genuinely not ours.
		return mqttSlotAdvanceNotOurs
	case cur.State == mqttInflightPubRelTerminal:
		// OUR OWN ADVANCE LANDED and only its response was lost. Adopt the readback and
		// finalize; treating an ambiguously landed write as unresolved would refuse a
		// resume that has nothing left to do.
		ref.seq = smsg.Sequence
		ref.state = mqttInflightPubRelTerminal
		return mqttSlotAdvanced
	case cur.State == mqttInflightPubRelReady:
		// Still ours, still ready, and the attempts are spent: the write did not land and
		// nothing here will retry it.
		return mqttSlotAdvanceUnresolved
	default:
		// A state no arrow from here allows. Explicit rather than inherited.
		return mqttSlotAdvanceUnresolved
	}
}

// admitStoredPubRel is PUBREL admission: the gate every stored PUBREL passes before
// anything acts on it.
//
// Returns (action, admitted). A refusal does NOT acknowledge — the caller returns and
// AckWait re-offers the record — which is what makes refusing safe and is why this can
// afford to fail closed on every uncertainty.
//
// # THE INVARIANT, stated exactly, because a weaker version of it was wrong
//
//	No SLOT-BACKED PUBREL bytes are transmitted without first ATOMICALLY ACQUIRING
//	`pubrel_sending`.
//
// The one exception is a LIVE RETAINED exchange: it has no register and so no slot (R6),
// and is authorized instead by the live in-memory owner identity of its incarnation, in
// the `smsg == nil` branch via retainedPubRelAdmittedLocked_. Everything with a slot obeys
// the invariant above.
//
// It is NOT "the slot must already say pubrel_ready". That formulation looked equivalent
// and is not, and the difference cost the ported suite: the store is what makes a record
// deliverable, and JetStream delivers it to this callback BEFORE storeMsg returns to the
// PUBREC handler. So the handler's `publish -> pubrel_ready` CAS cannot be made to precede
// the first delivery by any ordering — the race is against the store itself. Requiring
// `pubrel_ready` therefore refused the very PUBREL the PUBREC had just created, and every
// ordinary outbound QoS 2 exchange waited out AckWait. That is refutation 0's regression
// arriving by another route.
//
// # Why a matching nonce on a `publish` slot is evidence of PUBREC
//
// A stored PUBREL cannot exist unless a PUBREC arrived. The ordinary producer stores only
// on the successful-PUBREC path and takes the nonce off the removed PUBLISH entry; the only
// other producer is route-B recreation from a persisted past-PUBREC intent; and the builder
// refuses an absent or invalid owner nonce, so nothing unattributable is stored at all. So a
// structurally valid record whose nonce matches the current slot is durable evidence that
// THIS exchange received PUBREC. The slot still says `publish` only because the cross-stream
// handoff has not caught up.
//
// # But evidence is not authorization
//
// This is deliberately NOT "the nonce matches, therefore send". The permission is the
// SUCCESSFUL CAS, and every check below can still stop this broker from acquiring even
// though the exchange is genuinely past PUBREC.
//
// Session lock NOT held on entry: this does JetStream I/O, which must never run under it.
func (sess *mqttSession) admitStoredPubRel(c *client, pi uint16, owner mqttPubRelOwner,
	reply string, corr mqttPubRelCorrelation, affRef mqttPubRelAffineRef) (*mqttSendAction, bool) {

	if pi == 0 {
		return nil, false
	}
	if owner.State != mqttPubRelOwnerValid {
		// Absent or malformed. Slice 1 made the header mandatory with a tri-state parser
		// precisely so this refusal can distinguish "no claim" from "a claim this broker
		// cannot read", and both are unattributable.
		c.Warnf("MQTT: withholding a stored PUBREL for packet identifier %d of client %q: its "+
			"owner header is not one this broker writes, so the exchange it names cannot be "+
			"identified", pi, sess.id)
		return nil, false
	}
	// The DELIVERED record's own stream sequence, from its delivery metadata. This is what
	// becomes the canonical identity on the direct branch: the record whose CAS wins IS the
	// canonical record, so nothing is speculative and no second write is needed.
	deliveredSeq, _, _, _, _ := ackReplyInfo(reply)

	// A DRAINED EXCHANGE'S INTENT IS RECOVERY EVIDENCE, NOT TRANSMISSION AUTHORITY
	// (Decision 5). Before §10.5 this branch returned (nil, true) for a drained record and
	// let the callback forward it with NO durable action — justified by "the drain's
	// completion path releases the register, so there may be no slot". §10.5 removed that
	// release: a past-PUBREC drained exchange keeps its owner slot for its whole life, so
	// the slot IS present and admission binds transmission to an action on it exactly like
	// every other slot-backed PUBREL (the sole slot-less exception is a live retained
	// exchange — R6 — authorized by its in-memory owner, which a drained exchange is not).
	// Forwarding without an action contradicts the callback's "nothing below without an
	// action" rule for a slot-backed record and leaves the no-deadline write unfenced — the
	// F-092 hazard the whole item closes.
	//
	// The intent-vs-slot nonce "disagreement" the old shortcut existed for does not occur:
	// recordPubRelIntent and the stored PUBREL both carry the register's own regNonce
	// (mqtt.go:17681, :17723), so the intent, the slot and the record name ONE exchange. A
	// record whose nonce differs from the slot's is a DIFFERENT exchange and is refused
	// below, which is correct. Drain finalization changes source state only; it grants no
	// separate admission capability, so this path falls through to the slot.
	// A RECOVERED RESERVATION IS DELIBERATELY *NOT* AUTHORITY, and it was tried.
	//
	// An in-memory reservation looked like a reasonable third form: recovery installs one
	// for an exchange it found outstanding, so it is this incarnation's record of durable
	// evidence. But holistic round 2 refuted it, and the refutation is the same one the
	// whole item rests on: the reservation is PROCESS-LOCAL, so during a takeover two
	// brokers can hold matching ones, and a lagging predecessor would transmit without
	// observing that a successor changed, completed or replaced the slot. That is F-092
	// recreated by its own fix.
	//
	// So recovered slot-backed deliveries go through a FRESH slot load and an atomic
	// acquisition like every other delivery, and the restart cases that shortcut was
	// papering over are served instead by the universal pre-CONNACK custody refresh, which
	// makes the slot admissible again rather than bypassing it.
	sess.mu.Lock()
	idHash, gen, dur := sess.idHash, sess.gen, sess.pubRelDurable()
	sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	// The canonical PUBREL record for this exchange lives on THIS session's PUBREL STORE
	// subject — the FIRST return of mqttPubRelNames, not the second (the deliver subject).
	// Reading the second made the duplicate branch's subject check reject every real
	// canonical loaded by global sequence, the same mistake phaseSlotToPubRelReady had.
	pubRelSubject, _, _ := mqttPubRelNames(idHash, gen)

	// The custody repair is attempted AT MOST ONCE for this delivery, and the flag lives
	// out here rather than in the loop for that reason: a `continue` after a successful
	// transfer must not be able to re-enter it.
	transferAttempted := false
	pubRecAttempted := false

	// Two concurrent same-nonce deliveries race one CAS, and the loser must REDISPATCH
	// rather than fall through: falling through on "the slot advanced to ready" would
	// recreate exactly the AckWait regression this branch exists to remove.
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, err := sess.asm.loadInflightRecord(subject)
		switch {
		case err != nil:
			// INDETERMINATE, never guessed into an absence: an absence is what would
			// license treating the record as residue, and that is the one decision that
			// can put a duplicate identifier on the wire.
			return nil, false
		case smsg == nil:
			// NO SLOT. That is the RETAINED exchange's normal shape, not an error: a
			// retained send has no JetStream source, so no register at all (R6), and
			// nothing durable for an action to bind to. Across a restart that in-memory
			// identity is gone, which is exactly right — a retained record surviving a
			// restart has no durable authority and is refused rather than transmitted.
			//
			// A drained exchange is not admitted on intent alone. Its whole-life slot is
			// required below just like every other source-backed exchange, so a slot-less
			// delivery reaches the same fail-closed result as residue. Only retained
			// exchanges can be admitted here without a durable slot.
			//
			// NO ABSENT-SLOT DISPOSAL, and this is a correction to the plan rather than
			// an omission.
			//
			// §8.1 proposed acknowledging a source-correlated record away when its slot is
			// definitely absent, justified by "nothing supported erases a `pubrel_ready`
			// or active slot". That premise is true of slots and still misses the case: a
			// register can be legitimately released by the DRAIN path while the exchange
			// is still in `publish`, so an absent slot does not prove the exchange is
			// over — the client may not have answered PUBCOMP at all, and route B may be
			// about to recreate the record. A fixture caught the disposal acknowledging
			// away a PUBREL the client was owed, which is a lost message, not residue.
			//
			// Narrowing the rule twice (source-correlated, then not-draining) still left
			// it wrong, because the discriminator it needs — "this exchange is finished" —
			// is exactly what an absence cannot supply (feedback_absence_is_not_a_fact).
			// So the disposition is FAIL CLOSED: refuse, do not acknowledge. The cost is
			// that genuine residue redelivers and occupies stream space, which is the
			// already-stated residual R3, and it is the right way round — R3 costs stream
			// space, and the alternative costs a message.
			//
			// AND NO SLOT-LESS RETAINED SHAPE REMAINS (F-105 7c): a retained exchange is
			// register-backed and its PUBREL is never stored on $MQTT_out, so a record with no
			// slot is refused for every shape — a pre-7c retained record is an unsupported one.
			c.Warnf("MQTT: withholding a stored PUBREL for packet identifier %d of client %q: no owner "+
				"slot names exchange %q, so nothing can authorize transmitting it", pi, sess.id, owner.Nonce)
			return nil, false
		case cur == nil:
			// PRESENT BUT UNREADABLE is not the slot-less retained shape. It reserves
			// this identifier for an exchange that cannot be attributed, so no in-memory
			// owner may authorize transmitting over it.
			return nil, false
		}
		if cur.Nonce != owner.Nonce {
			// A DIFFERENT EXCHANGE holds this identifier. This is F-092's population, and
			// the refusal is the fix: unacknowledged and untransmitted.
			c.Warnf("MQTT: withholding a stored PUBREL for packet identifier %d of client %q: it "+
				"names exchange %q but the identifier's owner slot names %q, so transmitting it "+
				"would send a PUBREL under an identifier another exchange owns",
				pi, sess.id, owner.Nonce, cur.Nonce)
			return nil, false
		}
		if cur.ActionToken != _EMPTY_ {
			// An action is live. Another holder is transmitting, or one was abandoned;
			// observing a token proves phase progress, never authority to act.
			//
			// A DUPLICATE ARRIVING HERE DOES NOTHING — no steal, no piggyback
			// acknowledgement, no map mutation.
			//
			// BUT THE REFUSAL LEAVES A WAITER, because exclusion without a continuation
			// is indistinguishable from a stall. The common blocker is not a peer's
			// action: it is THIS broker's own PUBLISH action, still draining, while the
			// client's immediate PUBREC has already produced the PUBREL whose delivery
			// callback runs on an independent goroutine. Leaving that to AckWait turns
			// a millisecond of correct exclusion into a 30-second silence.
			sess.registerPubRelRedrive(c, pi, reply, subject, dur)
			return nil, false
		}
		// SAME NONCE IS NOT THE WHOLE EXCHANGE. In particular, duplicate disposal is
		// destructive: it acknowledges this record away. Require the delivered record's
		// source and affine identity to match the slot before either the duplicate branch or
		// ordinary admission can act. Otherwise a stale record reusing the nonce could be
		// discharged merely because some different, present canonical record exists.
		deliveredShape := mqttStoredPubRelShape{
			pi: pi, owner: owner, affRef: affRef,
			srcDur: corr.SrcDur, srcSeq: corr.SrcSeq,
		}
		if !corr.Ok || !mqttStoredPubRelShapeMatchesSlot(deliveredShape, cur) {
			c.Warnf("MQTT: withholding a stored PUBREL for packet identifier %d of client %q: "+
				"its source or affine identity does not match the owner slot", pi, sess.id)
			return nil, false
		}
		// A STRUCTURALLY VALID SAME-NONCE DUPLICATE IS DISPOSED OF, not left to redeliver.
		//
		// Several PUBREL records under one identifier is a supported stale-broker shape: the
		// slot names ONE canonical record, and any other valid record for the same exchange
		// is a duplicate. The client needs exactly one PUBREL, so a duplicate must never be
		// transmitted — but it must be ACKNOWLEDGED, or AckWait re-offers it forever, which
		// is the regression admission introduced by refusing before the pre-existing
		// duplicate branch could run.
		//
		// DISCHARGED BEFORE IT IS ACKNOWLEDGED, and that order is the correctness property:
		// this record may carry the only reference to an affine assignment DIFFERENT from
		// the tracked entry's, and acking it away would destroy that proof and let a later
		// resume infer a loss for a message the client answered. On a refused discharge it
		// is left for AckWait — the same disposition the pre-existing branch made.
		if cur.PubRelStreamSeq != 0 && deliveredSeq != 0 && cur.PubRelStreamSeq != deliveredSeq {
			// A DUPLICATE IS A DUPLICATE ONLY AGAINST A PRESENT CANONICAL (Decision 5). A
			// bare sequence mismatch is not proof: the slot's named canonical
			// (cur.PubRelStreamSeq) can be a PHANTOM — a skipped store wrote a sequence that
			// kept nothing — in which case THIS delivered record may be the exchange's real
			// PUBREL and acknowledging it away is a lost message. So prove the named
			// canonical is present before disposing; if it is absent or unreadable, fail
			// CLOSED (refuse, never acknowledge) and let recovery reconcile the slot.
			csmsg, cerr := sess.jsa.loadMsg(mqttOutStreamName, cur.PubRelStreamSeq)
			var canonical mqttStoredPubRelShape
			if cerr == nil && csmsg != nil {
				// loadMsg addresses the WHOLE $MQTT_out stream by global sequence, so prove
				// the record at that sequence is on THIS exchange's own PUBREL subject before
				// trusting it as the canonical — a header-only match is not enough when the
				// sequence could name residue on another subject.
				if csmsg.Subject != pubRelSubject {
					cerr = fmt.Errorf("the record at the canonical sequence is on %q, not %q",
						csmsg.Subject, pubRelSubject)
				}
				if cerr == nil {
					canonical, cerr = mqttDecodeStoredPubRel(csmsg.Header)
				}
				if cerr == nil && !mqttStoredPubRelShapeMatchesSlot(canonical, cur) {
					cerr = fmt.Errorf("the record does not match the owner slot")
				}
			}
			if cerr != nil || csmsg == nil {
				c.Warnf("MQTT: withholding a stored PUBREL for packet identifier %d of client %q: "+
					"its slot names canonical sequence %d which is absent, unreadable or does not "+
					"match the exchange, so this record cannot be proven a duplicate",
					pi, sess.id, cur.PubRelStreamSeq)
				return nil, false
			}
			if !sess.dischargeQ2AffPubRelProof(c, pi, mqttDeferredKey{dur, deliveredSeq}, affRef) {
				return nil, false
			}
			_, _ = sess.submitSourceAck(reply, false, nil)
			return nil, false
		}
		if cur.State == mqttInflightPubRelTerminal {
			// The exchange is over. Nothing may transmit, and disposing of the record is
			// terminal cleanup's business rather than admission's.
			return nil, false
		}

		// CONTEXTUAL CHECKS, which are separate from the shape the validator already
		// proved and which can refuse even though the exchange IS past PUBREC.
		switch verdict := sess.pubRelAdmissionContext(c, pi, cur, dur, deliveredSeq, corr, affRef); verdict {
		case mqttAdmissionRefused:
			return nil, false
		case mqttAdmissionPublishInFlight:
			// The durable token is gone — the drain landed — but the local send section
			// has not closed yet, so the opener still refuses. That window is this
			// broker's own, and leaving it to AckWait is the same 30-second silence the
			// durable-token branch leaves. Register, and endPublishSend's wake, which
			// fires after BOTH halves, re-offers this exact delivery.
			sess.registerPubRelRedrive(c, pi, reply, subject, dur)
			return nil, false
		case mqttAdmissionCustodyOnly:
			// THE CUSTODY REPAIR, and this is what closes F-092's last open item.
			//
			// The slot is quiescent and everything except its custody admits this
			// delivery. That is exactly what a predecessor's drained action leaves: the
			// drain clears the action fields and leaves the predecessor's Claim, so a
			// successor that resumed while the action was live can never acquire — its
			// admission fails here on every redelivery, forever, and at Receive Maximum 1
			// that stalls the whole session.
			//
			// Nothing else was going to repair it. Recovery's custody refresh runs only
			// before CONNACK, and by then this slot was active and correctly withheld.
			//
			// THE RETRY LOOP IS THE REDELIVERY, not a worker. A refusal here returns
			// without acknowledging and the PUBREL consumer has no delivery limit, so the
			// record comes back until something admits it. That is why this repair can be
			// a single attempt on the delivery that was already going to be refused.
			//
			// ONCE PER CALLBACK. transferAttempted is declared outside this loop, so a
			// reload after a successful transfer cannot re-enter the repair and a
			// contested slot cannot spin here; the next redelivery is the next attempt.
			if transferAttempted {
				return nil, false
			}
			transferAttempted = true
			if !sess.transferCustodyForAdmission(c, subject, smsg.Sequence, cur) {
				return nil, false
			}
			// Reload and re-run every check against the record as it is NOW. The transfer
			// grants CUSTODY and nothing else: the action CAS and the acceptance fence at
			// the callback still decide whether anything is transmitted.
			//
			// This `continue` is an EFFICIENCY choice rather than a SAFETY property, and
			// the distinction is worth stating exactly. Falling through would build the
			// acquisition reference from the pre-transfer body, whose custody the action
			// CAS then refuses, and the loop's existing lost-CAS redispatch would reload
			// and reach the same state — one attempt later, AND only when that attempt's
			// reads succeed. It is not free: the fall-through also mints a tentative
			// action and takes the unsuccessful-acquisition readback path, so a read
			// failure there delays progress through reconciliation. Skipping straight to
			// the reload spends the attempt on the record that can actually be acquired.
			// Either way it consumes one of this loop's bounded attempts, so a transfer
			// landing on the last one is completed by the next redelivery rather than by a
			// wider budget here.
			continue
		}

		ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
		if ref == nil {
			return nil, false
		}
		switch cur.State {
		case mqttInflightPubRelReady:
			// The handoff caught up first. Ordinary acquisition, against the canonical
			// tuple the slot already names.
			act, ended := sess.mqttAcquirePubRelAction(c, *ref, cur.Lin, cur.PubRelStreamSeq)
			if act != nil {
				return act, true
			}
			if ended {
				// THIS CONNECTION'S ENTITLEMENT ENDED. Terminal for the delivery: redispatching
				// would only ask again under a tenure that is not its own.
				return nil, false
			}
			// Lost the CAS. Reread and redispatch.
			continue
		case mqttInflightPublish, mqttInflightPubRelIntended:
			// AN ORDINARY SLOT STILL AT PUBLISH HAS AN EXCHANGE THAT HAS NOT RECORDED THIS
			// PUBREC (a drained PUBREC persists its intent before its advance, and a crash can
			// land between). The delivered record is the durable evidence of it, so record the
			// PUBREC through the one arrow first and re-dispatch: the slot is then at
			// pubrel_intended and takes the direct arrow from there. Never the slot-only arrow
			// from publish, which would leave an exchange its PUBCOMP could never end. Once per
			// callback; an unresolved attempt refuses, and the next redelivery retries it — so
			// the live connection makes progress without waiting for a resume.
			if cur.State == mqttInflightPublish && !mqttDurIsSlotOnlyClass(cur.Dur) {
				if pubRecAttempted {
					return nil, false
				}
				pubRecAttempted = true
				if sess.recordRecoveredPubRec(*ref) != mqttRecoveredPubRecRecorded {
					return nil, false
				}
				continue
			}
			// THE DIRECT ARROW. One CAS installs the canonical identity AND the action, so
			// the evidence and the authorization are established together and there is no
			// window in which the slot is past PUBREC with nobody owning it.
			//
			// `pubrel_intended` IS AN ORIGIN HERE, and it has to be: the intent CAS runs
			// before the PUBREL store (F-098's R-a), so a slot that owes a canonical
			// record is exactly the shape a RECREATED PUBREL arrives at. Omitting it
			// stranded every exchange whose record had to be rebuilt — admission refused,
			// the recreated PUBREL was never transmitted, and the client waited for a
			// PUBREL that would never come. That is the LIVE path's materialization: the
			// delivered record is proven to match, so the tuple is installed and the
			// action acquired in the same step.
			act, ended := sess.acquireActionFromPublish(c, *ref, cur.Lin, dur, deliveredSeq)
			if act != nil {
				return act, true
			}
			if ended {
				return nil, false // the connection's entitlement ended: terminal, as above
			}
			continue
		}
		return nil, false
	}
	return nil, false
}

// acquireActionFromPublish is THE DIRECT ARROW: `publish -> pubrel_sending` in one CAS,
// installing the canonical record identity and the action token together.
//
// One CAS rather than two, and that is the point. Advancing to `pubrel_ready` first and
// acquiring second would leave a window in which the slot is past PUBREC with nobody
// owning it — and it would also lose the race this branch exists to win, because the
// PUBREC handler's own `publish -> pubrel_ready` CAS is racing the same slot. Here the
// evidence (a valid same-nonce record) and the authorization (the action) are established
// in the same durable step.
//
// The CANONICAL IDENTITY is the delivered record itself: the record whose CAS wins IS the
// canonical record, and its sequence comes from its own delivery metadata. Nothing is
// speculative and no second write is owed.
//
// Session lock NOT held on entry.
// mqttPubRelRedriveKey names the STORED RECORD a waiter belongs to: the PUBREL consumer
// durable that delivered it and its own stream sequence. See mqttSession.pubRelRedrive for
// why the identity is the record rather than the identifier or the reply subject.
type mqttPubRelRedriveKey struct {
	dur  string
	sseq uint64
}

// mqttPubRelRedriveWaiter is where to send the redrive, and how new that address is.
type mqttPubRelRedriveWaiter struct {
	reply string
	// dseq is the delivery's consumer sequence, monotone per consumer. A registration
	// only replaces one whose dseq is not newer, so a slow callback for an older delivery
	// cannot overwrite a newer one's address with a stale one.
	dseq uint64
}

// publishSectionLiveLocked reports whether THIS BROKER is transmitting a PUBLISH under
// this identifier right now — the local half of a transmission, which outlives the durable
// action by the width of endPublishSend.
//
// It is the ONE reader of that fact. The question is not "is this identifier reserved",
// which is sendReservedLocked's and would be true of a PUBREL awaiting PUBCOMP with no
// PUBLISH anywhere; it is "is a PUBLISH on the wire", which is the hold's own pubIdent.
// Both the admission refusal and the redrive waiter's reread ask it here rather than each
// reaching the registry, so the two can never drift into disagreeing about whether a
// transmission is live.
//
// Session lock HELD on entry.
func (sess *mqttSession) publishSectionLiveLocked(pi uint16) bool {
	h := sess.sendsInFlight[pi]
	return h != nil && h.pubIdent != _EMPTY_
}

// publishTransmissionStands reports whether a PUBLISH transmission still forbids admitting
// a stored PUBREL under this identifier, over BOTH halves of one transmission: the durable
// action token on the slot, and this broker's local send hold, which the opener refuses on
// separately because it cannot undo a PUBLISH already on the wire.
//
// UNPROVEN READS ANSWER "STANDS". An indeterminate load proves nothing, and a record that
// is PRESENT but unreadable is not an absence either — the loader reports both as a nil
// record, and only smsg tells them apart from a proven absence. Answering "clear" on
// either would wake a waiter into a refusal, and worse, would consume it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) publishTransmissionStands(pi uint16, subject string) bool {
	if sess.asm == nil {
		return true
	}
	cur, smsg, lerr := sess.asm.loadInflightRecord(subject)
	switch {
	case lerr != nil:
		return true // indeterminate
	case smsg != nil && cur == nil:
		return true // present but unattributable
	case cur != nil && cur.ActionToken != _EMPTY_:
		return true // the durable half
	}
	// smsg == nil is a PROVEN absence: there is no durable half to stand.
	sess.mu.Lock()
	stands := sess.publishSectionLiveLocked(pi)
	sess.mu.Unlock()
	return stands
}

// registerPubRelRedrive records that a stored PUBREL was refused because a PUBLISH
// transmission was still live on its slot, so that the transmission's own end can wake it
// instead of leaving it to AckWait.
//
// BOTH BLOCKERS REGISTER HERE, and they are two halves of one transmission rather than two
// mechanisms: the DURABLE action token on the slot, and this broker's LOCAL send hold.
// endPublishSend releases them in that order and wakes after both, so a waiter left by
// either is woken by the same event.
//
// IT CLOSES THE REGISTRATION RACE ITSELF. The transmission can finish between the caller's
// observation and this registration, and a waiter registered after the wake has already
// run would never be woken at all. So the state is RE-READ after registering, over both
// halves: if nothing stands, the wake is issued here. Re-reading after registering, rather
// than before, is what makes the check conclusive; the reverse order has the same hole it
// is meant to close.
//
// THE REREAD ASKS "DOES ANY BLOCKER STILL STAND", NOT "IS IT STILL THE ONE I SAW". Waking
// on a DIFFERENT durable token would produce a NAK, an immediate re-offer, a fresh refusal
// and a fresh registration — a spin for as long as actions rotate on that slot. A foreign
// token whose holder never ends here still falls back to AckWait, exactly as before.
//
// A redrive is a NAK of the exact delivery that was refused, so JetStream re-offers it at
// once and ordinary admission runs again with the slot quiescent. Nothing is transmitted
// here and no state is stolen: the refusal stands, only its retry clock changes, and the
// re-offer repeats every admission check from the beginning.
//
// Session lock NOT held on entry.
func (sess *mqttSession) registerPubRelRedrive(c *client, pi uint16, reply, subject, dur string) {
	if pi == 0 || reply == _EMPTY_ || dur == _EMPTY_ || sess.asm == nil {
		return
	}
	sseq, dseq, _, _, _ := ackReplyInfo(reply)
	if sseq == 0 {
		// No record identity to key on, so a registration could only be a second answer
		// under some other key. AckWait remains the continuation.
		return
	}
	key := mqttPubRelRedriveKey{dur: dur, sseq: sseq}
	sess.mu.Lock()
	if sess.pubRelRedrive == nil {
		sess.pubRelRedrive = make(map[uint16]map[mqttPubRelRedriveKey]mqttPubRelRedriveWaiter, 1)
	}
	byRecord := sess.pubRelRedrive[pi]
	if byRecord == nil {
		byRecord = make(map[mqttPubRelRedriveKey]mqttPubRelRedriveWaiter, 1)
		sess.pubRelRedrive[pi] = byRecord
	}
	if have, ok := byRecord[key]; !ok || dseq >= have.dseq {
		byRecord[key] = mqttPubRelRedriveWaiter{reply: reply, dseq: dseq}
	}
	sess.mu.Unlock()

	if !sess.publishTransmissionStands(pi, subject) {
		// Nothing blocks any more, so no end will wake the waiter. Wake it here.
		sess.wakePubRelRedrive(pi)
	}
}

// wakePubRelRedriveIfClear wakes only when NOTHING blocks this identifier any more.
//
// An unconditional wake at a continuation's end is wrong in both directions: it consumes a
// waiter that a replacement transmission still excludes — and the waiter is one-shot, so
// the re-offer it buys is refused, re-registers, and the wake that was actually owed is the
// one that already fired — while the NAK itself is work nobody asked for.
//
// The waiter set is checked FIRST, under the lock and with no I/O, because the overwhelming
// case is an ordinary send with nothing waiting on it: the combined check reads the durable
// slot, and paying for that read at the end of every send section would make a mechanism
// that exists for a rare window cost something on every message.
//
// Session lock NOT held on entry.
func (sess *mqttSession) wakePubRelRedriveIfClear(pi uint16, subject string) {
	if pi == 0 {
		return
	}
	sess.mu.Lock()
	waiting := len(sess.pubRelRedrive[pi]) > 0
	sess.mu.Unlock()
	if !waiting {
		return
	}
	if sess.publishTransmissionStands(pi, subject) {
		return
	}
	sess.wakePubRelRedrive(pi)
}

// wakePubRelRedrive NAKs every refused stored PUBREL waiting under this identifier, so
// JetStream re-offers each at once.
//
// Called at the END of a PUBLISH send section — after BOTH the durable action drain and
// the local section release — because either alone leaves a window: the durable token can
// be gone while `beginPubRelSendLocked` still refuses the opener on the local hold, and
// waking between the two simply produces a second refusal.
//
// EVERY waiter, because each one is a distinct stored record that was refused and left
// without a continuation. A NAK for a delivery that is no longer refused costs one
// re-offer that ordinary admission then handles; a record left unwoken costs the AckWait
// silence this mechanism exists to remove.
//
// Session lock NOT held on entry; NAKs are sent outside it.
func (sess *mqttSession) wakePubRelRedrive(pi uint16) {
	if pi == 0 {
		return
	}
	sess.mu.Lock()
	byRecord := sess.pubRelRedrive[pi]
	delete(sess.pubRelRedrive, pi)
	replies := make([]string, 0, len(byRecord))
	for _, w := range byRecord {
		if w.reply != _EMPTY_ {
			replies = append(replies, w.reply)
		}
	}
	sess.mu.Unlock()
	if sess.jsa == nil {
		return
	}
	for _, reply := range replies {
		sess.jsa.sendMsg(reply, AckNak)
	}
}

func (sess *mqttSession) acquireActionFromPublish(c *client, ref mqttInflightSlotRef, lin string,
	dur string, deliveredSeq uint64) (act *mqttSendAction, ended bool) {

	if dur == _EMPTY_ || deliveredSeq == 0 || !ref.slotOK() || ref.name.subject == _EMPTY_ {
		return nil, false
	}
	// NEVER FROM PUBLISH FOR AN ORDINARY SLOT. Its exchange must record the PUBREC first
	// (recordRecoveredPubRec); moving the slot past PUBREC alone leaves an exchange at PUBLISH
	// that no PUBCOMP can end. Affine slots have no exchange and keep this origin.
	if ref.state == mqttInflightPublish && !mqttDurIsSlotOnlyClass(ref.dur) {
		return nil, false
	}
	token := mqttNewInflightNonce()
	act = &mqttSendAction{token: token, state: mqttSendActionTentative, slot: ref, lin: lin}
	if c != nil {
		act.guard = &c.outFrames
	}
	// ARM FIRST, exactly as ordinary acquisition does: a CAS that lands while this
	// goroutine is unwinding still has an owner that will drain it.
	sess.mu.Lock()
	if !sess.installTentativeActionLocked(ref.name.pi, act) {
		sess.mu.Unlock()
		return nil, false
	}
	sess.mu.Unlock()

	// Install the unwind handler the "arm first" comment promises: an abnormal exit after
	// the CAS lands but before responsibility is handed off drains the action rather than
	// leaking the identifier for the process's life. handedOff is set on every normal return.
	handedOff := false
	defer func() {
		if !handedOff {
			sess.startActionDrain(c, act)
		}
	}()

	// The predecessor is whatever the reference observed — `publish` or the owed
	// `pubrel_intended` — rather than a fixed `publish`, because both are legal origins
	// for this arrow and pinning one refuses the other outright.
	var seq uint64
	var ok bool
	var landed *mqttOutboundAction
	if mqttDurIsSlotOnlyClass(ref.dur) {
		// Affine: the slot machine's own CAS until chapter P (see mqttAcquirePubRelAction).
		seq, ok = sess.casInflightSlot(c, ref, ref.state, mqttSlotAcquireAction,
			func(next *mqttInflightRecord) {
				// THE DIRECT RACE ARROW, and it now moves the phase to `pubrel_ready`
				// because that is what the slot has become: past PUBREC with a canonical
				// record named. The action is the TOKEN, separately.
				next.State = mqttInflightPubRelReady
				next.Acq++ // C-I: every acquisition advances the transmission identity
				next.PubRelDurable = dur
				next.PubRelStreamSeq = deliveredSeq
				next.ActionToken = token
				// Bound to the custody the slot ALREADY carries: the validator requires the
				// two to be equal, so deriving them from this session's current belief would
				// write a record the next reader refuses.
				next.ActionClaim = next.Claim
				next.ActionClaimSeq = next.ClaimSeq
			})
	} else {
		// AN ORDINARY SLOT takes the arrow inside the authority batch: the canonical PUBREL is
		// installed by the same member that installs the token, so the evidence and the
		// authorization still land together.
		// deliveredSeq also binds a READY base (it is ignored for the intended->ready arrow, which
		// installs it through the rebind).
		seq, landed, ok, ended = sess.acquirePubRelThroughAuthority(c, ref, ref.state, token,
			&mqttPubRelRebind{Durable: dur, StreamSeq: deliveredSeq}, deliveredSeq)
	}
	if !ok {
		handedOff = true
		sess.resolveUnacquiredAction(c, act)
		return nil, ended
	}
	sess.mu.Lock()
	act.state = mqttSendActionAcquired
	act.outAction = landed
	act.slot.seq = seq
	act.slot.state = mqttInflightPubRelReady
	act.slot.acq++
	act.slot.actionLive = true
	act.slot.actionToken = token
	// The acquired slot reaches the PUBREL entry through the delivery callback that follows
	// this acquisition (bindActionToPubRelLocked), not by an unvalidated write here onto
	// whatever entry is resident; this exchange's own resident gets the phase refresh only.
	sess.advanceResidentPubRelPhaseLocked(ref.name.pi, act.slot)
	sess.mu.Unlock()
	handedOff = true
	return act, false
}

// mqttAdmissionVerdict is what the contextual checks concluded. It is four-valued because
// two of the refusals are ACTIONABLE and the rest are not: one is repairable here and now,
// and one is a wait whose end this broker will observe.
//
// The helper is READ-ONLY and runs under sess.mu. It may not perform the repair itself:
// the acceptance proof and the custody CAS both take that lock and do JetStream I/O, so
// doing either here would deadlock. Reporting WHICH refusal happened is what lets the
// caller repair the one it can, outside the lock, without duplicating any of the checks.
type mqttAdmissionVerdict int

const (
	// mqttAdmissionRefused: something other than custody objects. Nothing may repair it
	// from here — the record is left unacknowledged and AckWait re-offers it.
	mqttAdmissionRefused mqttAdmissionVerdict = iota
	// mqttAdmissionCustodyOnly: every other condition passed and the slot's custody names
	// a different incarnation. This is the shape a predecessor's drained action leaves
	// behind, and it is the one refusal a current owner may durably repair.
	mqttAdmissionCustodyOnly
	// mqttAdmissionPublishInFlight: refused because THIS broker is transmitting a PUBLISH
	// under the identifier right now — the local send hold, which is the durable action's
	// in-memory half and outlives it by the width of endPublishSend.
	//
	// Reported separately for one reason: it is the refusal whose END THIS BROKER SEES, so
	// the caller can leave a waiter instead of leaving the client to AckWait. It is still
	// a refusal — nothing is transmitted and nothing is acknowledged.
	//
	// The caller must NOT re-derive this by reading the hold itself. Reporting which
	// refusal happened is exactly what this type exists for, and a second reader of the
	// registry would be a second answer to "is this identifier transmitting".
	mqttAdmissionPublishInFlight
	// mqttAdmissionOK: nothing here objects to acquiring an action.
	mqttAdmissionOK
)

// pubRelAdmissionContext proves the things the shape validator cannot: that the named
// source, consumer durable and delivered record actually correspond to this exchange, and
// that nothing else about the slot forbids acting.
//
// These are the checks that make the direct arrow "evidence to action" rather than
// "nonce, therefore send". None of them disproves that the exchange is past PUBREC; each
// can still stop THIS broker from acquiring.
func (sess *mqttSession) pubRelAdmissionContext(c *client, pi uint16, cur *mqttInflightRecord,
	dur string, deliveredSeq uint64, corr mqttPubRelCorrelation, affRef mqttPubRelAffineRef) mqttAdmissionVerdict {

	if dur == _EMPTY_ || deliveredSeq == 0 {
		// No current PUBREL consumer, or a delivery whose own sequence is unreadable.
		// Neither can name a canonical record.
		return mqttAdmissionRefused
	}
	if cur.PI != pi {
		return mqttAdmissionRefused
	}
	// THE CANONICAL DURABLE IS COMPARED ONLY WHERE THE SLOT CARRIES ONE, and the
	// distinction is what lets an OWED slot be bound at all.
	//
	// `pubrel_intended` is past PUBREC and deliberately names NO canonical record: the
	// phase is written before the store precisely so a crash between them leaves a slot
	// no peer may reap (F-098's R-a). Comparing a canonical durable it cannot have
	// refused every such slot unconditionally — before the direct arrow that exists to
	// bind it — so a recreated PUBREL could never be admitted and the client waited for
	// a PUBREL that never came. That refusal was reached whatever the delivered record
	// said, which is why no amount of widening the acquisition gate moved it.
	//
	// For a slot that DOES carry a tuple the rule is unchanged: the canonical durable is
	// contextual identity, not merely a non-empty string, and acting through a slot
	// naming another incarnation's consumer would transmit a record terminal cleanup can
	// never safely dispose.
	//
	// For the tuple-less INITIAL binding the identity still has to be proved, and it is
	// proved from the SLOT'S OWN CAPTURED INCARNATION rather than from the delivered
	// record's say-so: the expected durable is the one this slot's generation names, so a
	// record produced under any other incarnation is refused exactly as a mismatching
	// tuple is.
	if mqttInflightPastPubRec(cur.State) && cur.PubRelDurable != _EMPTY_ &&
		cur.PubRelDurable != dur {
		return mqttAdmissionRefused
	}
	if cur.State == mqttInflightPubRelIntended && cur.PubRelDurable == _EMPTY_ {
		sess.mu.Lock()
		idHash, gen := sess.idHash, sess.gen
		sess.mu.Unlock()
		_, _, wantDur := mqttPubRelNames(idHash, gen)
		if wantDur == _EMPTY_ || dur != wantDur {
			// The delivered record belongs to another incarnation's PUBREL consumer, so
			// it is not this owed slot's canonical candidate.
			return mqttAdmissionRefused
		}
	}
	// ACTIONABLE CUSTODY, which admission was not checking at all.
	//
	// §3.2 settles what proves current ownership, and it is NOT a sequence comparison:
	// `ClaimSeq` is `sess.seq` snapshotted at store time and every confirmed save replaces
	// `sess.seq`, so no comparison against a current or original acceptance can be made to
	// work. What IS stable is `regClaim` — minted once in the session constructor and never
	// rewritten for the life of the object — so custody is this object's exactly when the
	// slot's `Claim` equals it.
	//
	// Without this a SUPERSEDED broker, or a successor that has not yet taken custody,
	// could acquire a new action. The exception §6.1 grants is narrower than that and is
	// not weakened here: a holder that ALREADY acquired an action may finish it across a
	// later acceptance, because the action is bound to the exchange — but STARTING a new
	// acquisition re-proves current ownership, which is what this is.
	//
	// The shape matrix permits claimless publish records for lifecycle states that never
	// reach acquisition, but this path requires a current claim in both states it can
	// acquire from. Recovery's positive eligibility classifier establishes that claim
	// before the PUBREL consumer starts.
	if mqttInflightPastPubRec(cur.State) && cur.Claim == _EMPTY_ {
		// Past PUBREC with no custody at all is a shape the validator forbids, so a record
		// carrying it is one this broker did not write. Refused rather than acted on.
		return mqttAdmissionRefused
	}
	if cur.Cancelled && !cur.Retired {
		// "Never transmitted" contradicts a stored PUBREL. A contradictory slot is
		// refused rather than normalized: normalizing it would be deciding which half of
		// a shape this broker does not write to believe.
		return mqttAdmissionRefused
	}
	if !corr.Ok || corr.SrcDur != cur.Dur || corr.SrcSeq != cur.Seq {
		// The record names a DIFFERENT source than the slot's exchange. Refused: this is
		// the positive mismatch. An absent correlation is the retained shape, whose normal
		// authority is slot-less; it cannot authorize a source-backed slot merely because
		// an owner nonce happened to match.
		c.Warnf("MQTT: withholding a stored PUBREL for packet identifier %d of client %q: it "+
			"correlates to source %s/%d but the identifier's exchange came from %s/%d",
			pi, sess.id, corr.SrcDur, corr.SrcSeq, cur.Dur, cur.Seq)
		return mqttAdmissionRefused
	}
	// The affine pair must match the slot's, when the record carries one: a record proving
	// a DIFFERENT assignment must not be acknowledged away under this exchange.
	if affRef.valid() &&
		(affRef.Claim != cur.Q2AffClaim || affRef.Copy != cur.Q2AffCopy) {
		return mqttAdmissionRefused
	}
	if cur.Q2AffClaim != _EMPTY_ && affRef.State == mqttPubRelAffineRefAbsent {
		// With no carried reference PUBCOMP would conclude that no affine proof is owed
		// and acknowledge the record away. For an affine slot that would destroy the only
		// evidence from which recovery can prove the copy was delivered.
		return mqttAdmissionRefused
	}
	// AN EXACT LIVE PUBLISH SEND TOKEN forbids acquiring, because the opener owns
	// transmission admission only and cannot undo a PUBLISH already on the wire. It is
	// EXACT — this identifier's own PUBLISH token — and emphatically not a refusal on any
	// live pendingPublish entry, which is refuted fix 2, nor a retained-term refusal,
	// which is refuted fix 1.
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.publishSectionLiveLocked(pi) {
		return mqttAdmissionPublishInFlight
	}
	// CUSTODY IS CHECKED LAST, and the position is load-bearing rather than tidiness.
	//
	// Equality is checked while holding sess.mu, against the constructor-minted claim of
	// this exact session object. Without it a predecessor delayed behind takeover
	// notification could copy a successor's custody into a fresh action and write PUBREL
	// bytes to its own socket.
	//
	// Being last is what makes mqttAdmissionCustodyOnly MEAN something: every other
	// condition has passed, so the only thing standing between this delivery and an
	// action is which incarnation the slot names. That is the one refusal the caller can
	// legitimately repair, and it may only attempt the repair when nothing else objects.
	// Custody transfer is the SLOT-ONLY (experimental affine) class's alone. An ordinary slot's
	// ownership is its exchange's, arbitrated by the connection's accepted entitlement inside the
	// acquisition batch (F-105 7b): a valid claim stamp left by another session object never
	// obstructs it.
	if mqttDurIsSlotOnlyClass(cur.Dur) && (cur.Claim == _EMPTY_ || cur.Claim != sess.regClaim) {
		return mqttAdmissionCustodyOnly
	}
	return mqttAdmissionOK
}

// mqttStoredPubRelShapeMatchesSlot reports whether a decoded record can be the canonical
// evidence named by slot. Malformed affine metadata is not rejected here: its own tri-state
// deliberately permits transmission but makes PUBCOMP withhold acknowledgement. An absent
// affine reference on an affine slot is different—PUBCOMP would treat it as "nothing owed"
// and destroy the proof—so that shape cannot be canonical for the slot.
func mqttStoredPubRelShapeMatchesSlot(shape mqttStoredPubRelShape, slot *mqttInflightRecord) bool {
	if slot == nil || slot.QoS != 2 || shape.pi != slot.PI || shape.owner.State != mqttPubRelOwnerValid ||
		shape.owner.Nonce != slot.Nonce || shape.srcDur != slot.Dur || shape.srcSeq != slot.Seq {
		return false
	}
	if shape.affRef.valid() {
		return shape.affRef.Claim == slot.Q2AffClaim && shape.affRef.Copy == slot.Q2AffCopy
	}
	return slot.Q2AffClaim == _EMPTY_ || shape.affRef.refused()
}

// mqttStoredPubRelMatchesSlot is the recovery form of
// mqttStoredPubRelShapeMatchesSlot. Keeping the scan record's decode verdict in the
// predicate prevents a caller from reconstructing a superficially matching shape from a
// record the canonical decoder refused.
func mqttStoredPubRelMatchesSlot(rel mqttStoredPubRel, slot *mqttInflightRecord) bool {
	if rel.shapeErr != nil {
		return false
	}
	return mqttStoredPubRelShapeMatchesSlot(mqttStoredPubRelShape{
		pi: rel.pi, owner: rel.owner, affRef: rel.affRef,
		srcDur: rel.srcDur, srcSeq: rel.srcSeq,
	}, slot)
}

// handOffPublishEntryLocked is F-093's correction: the ordered handoff from the PUBLISH
// entry to the PUBREL entry, performed under ONE session-lock section.
//
// THE FINDING it closes: the drain released the recovered in-flight register but never
// removed the recovered `pendingPublish` entry or its `cpending` mapping. `trackAsPubRel`
// then installed `pendingPubRel[pi]` BESIDE it, and PUBCOMP removed only the PUBREL entry —
// so the stale PUBLISH entry kept reserving the identifier and its Receive Maximum slot for
// the life of the session, charged TWICE while both entries lived.
//
// THE ORDER IS THE CORRECTNESS PROPERTY, and it is why this is one locked section:
//
//  1. the same-nonce PUBREL owner is already installed (trackAsPubRel ran);
//  2. THE SOURCE RETIREMENT IS INSTALLED BEFORE ANYTHING IS REMOVED. Removing the entry's
//     `cpending` link is what lets a fresh source redelivery miss the old identifier
//     entirely, reach bumpPI, allocate a DIFFERENT one and transmit — recovery's own code
//     exists to prevent exactly that. So the suppression goes in first, and a re-offer then
//     meets sourceRetiredLocked rather than a fresh allocation;
//  3. only then is the PUBLISH entry compare-deleted;
//  4. and its `cpending` mapping compare-deleted — GUARDED, unlike untrackPublishEntry,
//     which deletes `cpending[jsDur][sseq]` unconditionally and would therefore remove a
//     mapping a DIFFERENT identifier now owns.
//
// No step here releases the identifier or deletes the slot: the identifier stays reserved
// by the PUBREL entry, and the slot is released only by PUBCOMP's terminal path.
// Returns false without mutating either side when their exchange identities disagree.
//
// Session lock held on entry.
func (sess *mqttSession) handOffPublishEntryLocked(pi uint16, act *mqttSendAction) bool {
	pub := sess.pendingPublish[pi]
	if pub == nil {
		return true // nothing to hand off: the live PUBREC path already did it
	}
	rel := sess.pendingPubRel[pi]
	if rel == nil {
		// The owner is not installed yet, so removing the PUBLISH entry now would leave
		// the identifier with no reservation at all. Refuse rather than reorder.
		return false
	}
	if act == nil {
		// Slot-less retained admission owns no source-backed PUBLISH entry. Treating an
		// unexpected one as ours would delete another exchange on nonce-less authority.
		return false
	}
	// UNIQUELY OWNED: the entry must be the one this exchange is about. A different
	// source under this identifier is somebody else's exchange, and removing it would be
	// the cross-exchange mutation this whole item exists to stop.
	if pub.regNonce != act.slot.nonce || rel.regNonce != act.slot.nonce ||
		pub.jsDur != act.slot.dur || pub.sseq != act.slot.sseq {
		return false
	}
	// (2) The suppression FIRST — but ONLY where this handoff is the thing that owns the
	// source, and recorded as ALREADY DURABLE because the slot proves it.
	//
	// An acquired action means the slot is the authority for this exchange, and a
	// `pubrel_ready`-or-later slot is itself the durable statement that the exchange is past
	// PUBREC. The retirement is therefore already durable; marking it insecure would make
	// PUBCOMP wait for a second write stating the same fact. Slot-less retained admission
	// has no recovered PUBLISH entry to hand off, so it returns at the top of this helper.
	if mqttInflightPastPubRec(act.slot.state) {
		sess.retireSourceLocked(pub.jsDur, pub.sseq, &mqttRetiredSource{
			pi:         pi,
			regSeq:     pub.regSeq,
			nonce:      pub.regNonce,
			q2AffClaim: pub.q2AffClaim,
			q2AffCopy:  pub.q2AffCopy,
			durable:    true,
		})
	}
	// (3) Compare-delete the entry.
	if cur := sess.pendingPublish[pi]; cur == pub {
		delete(sess.pendingPublish, pi)
	}
	// (4) Compare-delete the mapping, GUARDED on the identifier it names.
	if pub.jsDur != _EMPTY_ && len(sess.cpending) != 0 {
		// Deleted through `sess.cpending[...]` rather than a local alias, deliberately: the
		// census scanner discovers a raw map mutation by the field it names, and a local
		// `m := sess.cpending[dur]; delete(m, seq)` is invisible to it. Writing the site so
		// the property can SEE it is the point — an edge the scanner cannot find is an edge
		// nothing keeps honest.
		if m := sess.cpending[pub.jsDur]; m != nil && m[pub.sseq] == pi {
			delete(sess.cpending[pub.jsDur], pub.sseq)
		}
	}
	return true
}

// The accepted-incarnation proof
// ---------------------------------------------------------------------------

// mqttAcceptanceProof is the tuple a successful proof establishes: this session object's
// generation, the write identity it was last acknowledged for, and the exact $MQTT_sess
// stream sequence its record occupies. The SEQUENCE is what the custody transfer needs —
// a claim is ordered against it — so the proof returns what it proved rather than leaving
// the caller to re-sample a value that may have moved.
type mqttAcceptanceProof struct {
	gen string
	wid string
	seq uint64
}

// mqttAcceptanceOutcome is TRI-STATE, and that is the whole point of this helper.
//
// The two callers owe uncertainty different things. The transmission fence may not write
// on doubt, so it refuses on anything but Current. The custody transfer must not read one
// failed read as "you have been superseded" — a boolean folded those together, and a
// single transient failure would then abandon the repair and leave the exchange stalled
// exactly as F-092's open item described.
type mqttAcceptanceOutcome int

const (
	// mqttAcceptanceIndeterminate: nothing was proved either way. Retry; conclude nothing.
	mqttAcceptanceIndeterminate mqttAcceptanceOutcome = iota
	// mqttAcceptanceDisplaced: DURABLE evidence that this object is no longer the current
	// accepted incarnation, or that it is no longer bound to the connection asking.
	mqttAcceptanceDisplaced
	// mqttAcceptanceCurrent: the record at the head IS this object's own acknowledged
	// write, and (when a connection was named) this object is still bound to it.
	mqttAcceptanceCurrent
)

// proveAcceptedIncarnation proves that this session object is STILL the current accepted
// incarnation of its client, by exact identity rather than by inference.
//
// # What it proves, and why each conjunct is there
//
// The record at the head of $MQTT_sess must be the very write this object was last
// acknowledged for: the same stream sequence AND the same write identity. Sequence alone
// is not enough — a foreign write lands at a sequence too — and generation alone is not
// enough, because a generation survives a resume by another broker. Together with the
// local connection binding they say "the acceptance this object is running under is the
// one the stream still records".
//
// # Why it cannot be a fence on its own
//
// It reports what was true at the read. It does NOT remain true through what follows, so
// it authorizes a custody CAS (which is itself arbitrated by an exact-tuple predicate) and
// it gates a transmission that a durable action has ALREADY been acquired for. It is never
// the thing that makes a write safe by itself; that is the action CAS.
//
// cc may be nil, which asks the OBJECT-level question and skips the connection binding.
//
// Session lock NOT held on entry. Performs one session-record load per attempt.
func (sess *mqttSession) proveAcceptedIncarnation(cc *client) (mqttAcceptanceProof, mqttAcceptanceOutcome) {
	as := sess.asm
	if as == nil {
		return mqttAcceptanceProof{}, mqttAcceptanceIndeterminate
	}
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		ps, headSeq, lerr := as.loadFenceSession(sess.idHash)
		if lerr != nil || ps == nil {
			// Indeterminate, malformed, or absent. An absence is not a displacement: it
			// says the record cannot be read as evidence, not that somebody else owns the
			// session.
			return mqttAcceptanceProof{}, mqttAcceptanceIndeterminate
		}
		sess.mu.Lock()
		seq, wid, gen := sess.seq, sess.wid, sess.gen
		uncertain, bound := sess.seqUncertain, sess.c
		sess.mu.Unlock()

		switch {
		case uncertain:
			// A write of ours may have landed with its response lost, so sess.seq is not
			// the exact fact this proof needs. The resolver settles it; nothing here can.
			return mqttAcceptanceProof{}, mqttAcceptanceIndeterminate
		case cc != nil && bound != cc:
			// The object is no longer this connection's, which is durable enough for the
			// caller that named a connection: whatever it was about to do belongs to
			// whoever holds the object now.
			return mqttAcceptanceProof{}, mqttAcceptanceDisplaced
		case ps.Gen != _EMPTY_ && ps.Gen != gen:
			// Another incarnation's record is at the head. Our own generation changes only
			// through clear() on this object, so this is somebody else's.
			return mqttAcceptanceProof{}, mqttAcceptanceDisplaced
		case headSeq == seq && ps.WID == wid && ps.WID != _EMPTY_:
			return mqttAcceptanceProof{gen: gen, wid: wid, seq: seq}, mqttAcceptanceCurrent
		case headSeq < seq:
			// OUR OWN LATER SAVE probably landed between the load and the snapshot — a
			// SUBSCRIBE on the readLoop while this runs is enough. This is a RETRY
			// CONDITION and never an ownership conclusion: a foreign head can land after
			// the load too, so the next attempt re-establishes exact equality itself
			// rather than inheriting anything from this observation.
			continue
		case headSeq > seq:
			// NOT readable as a foreign writer, which is the trap here. Our own save is
			// committed by JetStream BEFORE persistRecord adopts the response, and in that
			// window the head carries a freshly minted WID that differs from sess.wid
			// exactly as a rival's would. A genuine successor does keep producing this
			// observation — until the record watch or its committed-read backstop detaches
			// this object — and refusing meanwhile costs nothing but a redelivery, where
			// concluding displacement from it would abandon our own landed write.
			continue
		default:
			return mqttAcceptanceProof{}, mqttAcceptanceIndeterminate
		}
	}
	return mqttAcceptanceProof{}, mqttAcceptanceIndeterminate
}

// transferCustodyForAdmission moves a quiescent slot's custody to THIS session object so
// the exchange it owns becomes actionable again.
//
// It exists because custody, once withheld, was never revisited. Recovery refreshes
// custody before CONNACK; a slot that was mid-action then is correctly skipped, and when
// its holder later drains it the slot goes back to a quiescent state still carrying the
// PREDECESSOR's claim. Admission then refuses it forever. This is the repair for exactly
// that shape, performed on the delivery that would otherwise have been refused.
//
// # What authorizes the write
//
// Not the delivery, and not an in-memory reservation — that was tried and refused, because
// a reservation is process-local and two brokers hold matching ones during a takeover.
// Three things together:
//
//   - the caller established that EVERY other admission condition passed for this exact
//     record, so the evidence is this exchange's own durable record plus its slot;
//   - the acceptance proof says this object is still the current accepted incarnation of
//     the session, bound to this connection;
//   - the CAS itself matches the exact custody tuple just observed, so a successor that
//     transferred in between wins and this attempt loses rather than overwriting it.
//
// # And what it deliberately does NOT authorize
//
// Transmission. The proof can go stale between here and any socket write, which is why the
// action CAS and the post-acquisition acceptance fence both still run afterwards. This
// only makes the exchange acquirable by its rightful owner.
//
// Ordering: a claim standing at or past our proven acceptance sequence belongs to an owner
// accepted no earlier than us, so it is left alone. Session lock NOT held on entry.
func (sess *mqttSession) transferCustodyForAdmission(c *client, subject string, seq uint64,
	cur *mqttInflightRecord) bool {

	if h := mqttTestCustodyTransferV.Load(); h != nil {
		(*h)()
	}
	if cur == nil || subject == _EMPTY_ {
		return false
	}
	// Quiescent PHASES only. The phase gate refuses the rest anyway; refusing here keeps
	// the intent explicit rather than relying on a downstream helper to say no.
	//
	// pubrel_intended is a member, and it has to be: C-III quantifies custody over EVERY
	// supported quiescent slot, and an OWED slot nobody can take custody of can never be
	// materialized by the broker that finds it — which is the stranding L1 exists to
	// close.
	if cur.State != mqttInflightPublish && cur.State != mqttInflightPubRelIntended &&
		cur.State != mqttInflightPubRelReady {
		return false
	}
	proof, out := sess.proveAcceptedIncarnation(c)
	if out != mqttAcceptanceCurrent {
		// Displaced, or simply unprovable right now. Either way this broker may not move
		// custody; the record stays unacknowledged and AckWait re-offers it.
		return false
	}
	if cur.ClaimSeq >= proof.seq {
		// The standing claim was written by an owner whose acceptance is no older than
		// ours. Taking it would be the ordering violation the recovery contest refuses.
		return false
	}
	ref := mqttSlotRefFromRecord(subject, seq, cur)
	if ref == nil {
		return false
	}
	sess.mu.Lock()
	claim := sess.regClaim
	sess.mu.Unlock()
	if claim == _EMPTY_ {
		return false
	}
	_, ok := sess.casInflightSlot(c, *ref, cur.State, mqttSlotTransferCustody,
		func(next *mqttInflightRecord) {
			// ONLY custody. Everything else about the exchange is whatever the record
			// says it is; this write is not a rewrite of the exchange.
			next.Claim = claim
			next.ClaimSeq = proof.seq
		})
	if ok && c != nil {
		c.Debugf("MQTT: took custody of the in-flight slot on %q for client %q so its stored "+
			"PUBREL can be completed by this incarnation", subject, sess.id)
	}
	return ok
}
