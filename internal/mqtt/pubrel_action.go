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
	"fmt"
	"time"
)

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
		c.Debugf("MQTT: the PUBREC handoff for packet identifier %d of client %q found no "+
			"canonical record on %q at sequence %d; leaving the slot in publish for recovery",
			ack.slot.name.pi, sess.id, pubRelSubject, pubRelSeq)
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
		c.Debugf("MQTT: the PUBREC handoff for packet identifier %d of client %q read a record "+
			"at sequence %d that does not match this exchange; leaving the slot in publish",
			ack.slot.name.pi, sess.id, pubRelSeq)
		return
	}
	seq, ok := sess.casInflightSlot(c, ack.slot, mqttInflightPublish, mqttSlotAdvancePhase,
		func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelReady
			next.PubRelDurable = dur
			next.PubRelStreamSeq = pubRelSeq
		})
	if !ok {
		c.Debugf("MQTT: the PUBREC handoff for packet identifier %d of client %q did not "+
			"re-phase its owner slot; recovery completes the transition from the stored record",
			ack.slot.name.pi, sess.id)
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
	sess.mu.Lock()
	if rel := sess.pendingPubRel[ack.slot.name.pi]; rel != nil {
		rel.slot = ref
		rel.regSeq = seq
		rel.regNonce = ref.nonce
	}
	sess.mu.Unlock()
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
	seq, ok := sess.casInflightSlot(c, *ref, mqttInflightPublish, mqttSlotAdvancePhase,
		func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelReady
			next.PubRelDurable = dur
			next.PubRelStreamSeq = rel.seq
			landed = *next
		})
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
func (sess *mqttSession) mqttAcquirePubRelAction(c *client, ref mqttInflightSlotRef) *mqttSendAction {
	if !ref.slotOK() || ref.name.subject == _EMPTY_ || ref.name.pi == 0 {
		return nil
	}
	token := mqttNewInflightNonce()

	// ARM FIRST. The holder is installed tentative and its abort armed before the request
	// goes out, so a CAS that lands while this goroutine is unwinding still has an owner
	// that will drain it.
	act := &mqttSendAction{
		token: token,
		state: mqttSendActionTentative,
		slot:  ref,
	}
	sess.mu.Lock()
	if !sess.installTentativeActionLocked(ref.name.pi, act) {
		sess.mu.Unlock()
		return nil
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

	seq, ok := sess.casInflightSlot(c, ref, mqttInflightPubRelReady, mqttSlotAcquireAction,
		func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelSending
			next.ActionToken = token
			// The action binds to the custody the slot ALREADY carries, never to what
			// this session object believes its custody to be. The shape validator
			// requires the two to be equal, so deriving them from anywhere else would
			// write a record the next reader refuses.
			next.ActionClaim = next.Claim
			next.ActionClaimSeq = next.ClaimSeq
		})
	if !ok {
		// Not acquired. The tentative holder is retired only once a reread proves this
		// token is absent — a DEFINITE rejection and an AMBIGUOUS one are indistinguishable
		// from here, and retiring on the ambiguous case would abandon an action that
		// actually landed.
		handedOff = true
		sess.resolveUnacquiredAction(c, act)
		return nil
	}
	sess.mu.Lock()
	act.state = mqttSendActionAcquired
	act.slot.seq = seq
	act.slot.state = mqttInflightPubRelSending
	act.slot.actionToken = token
	// The acquired slot goes onto the PUBREL entry here too. On the ordinary branch the
	// PUBREC handler usually installed it already, but a delivery on ANOTHER broker — or
	// after a restart — has no such handler run behind it, and the terminal release needs
	// something to address either way.
	if rel := sess.pendingPubRel[ref.name.pi]; rel != nil {
		rel.slot = act.slot
		rel.regSeq = seq
		rel.regNonce = ref.nonce
	}
	sess.mu.Unlock()
	handedOff = true
	return act
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
		sess.retireActionLocked_(act)
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
	sess.retireActionLocked_(act)
}

// retireActionLocked_ marks the action drained and drops the holder term if it now
// reserves nothing. Takes the session lock itself.
func (sess *mqttSession) retireActionLocked_(act *mqttSendAction) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	act.drained = true
	act.state = mqttSendActionReleased
	pi := act.slot.name.pi
	if h := sess.sendsInFlight[pi]; h != nil && h.action == act {
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
			sess.retireActionLocked_(act)
			return
		case cur == nil:
			// PRESENT BUT UNREADABLE. It may still carry this exact action, so absence
			// has not been proved and the local holder must remain live.
			sess.scheduleActionReconcile(c, act)
			return
		}
		if cur.ActionToken != act.token {
			// A DIFFERENT valid token, or none: a peer acquired from `ready` before this
			// reread, or the drain already landed. Either way this action is gone. The
			// holder retires its OWN state and does not touch the other token.
			sess.retireActionLocked_(act)
			return
		}
		var target mqttInflightState
		switch cur.State {
		case mqttInflightPubRelSending:
			target = mqttInflightPubRelReady
		case mqttInflightPubRelTerminalDraining:
			target = mqttInflightPubRelTerminal
		default:
			// Our token is present in a state that has no drain arrow. The shape
			// validator forbids that combination, so this is unreachable on a record
			// this broker wrote; retiring the local holder is the fail-closed reading.
			sess.retireActionLocked_(act)
			return
		}
		ref := act.slot
		ref.seq = smsg.Sequence
		ref.state = cur.State
		seq, ok := sess.casInflightSlot(c, ref, cur.State, mqttSlotDrainAction,
			func(next *mqttInflightRecord) {
				if next.ActionToken != act.token {
					return
				}
				next.State = target
				next.ActionToken = _EMPTY_
				next.ActionClaim = _EMPTY_
				next.ActionClaimSeq = 0
			})
		if !ok {
			continue // the slot moved; reread and pick the arrow again
		}
		if target == mqttInflightPubRelTerminal {
			// PUBCOMP was already observed, so the exchange is over and this drain is
			// what makes it deletable. Finish it here rather than waiting for another
			// event: nothing else is coming.
			ref.seq = seq
			ref.state = mqttInflightPubRelTerminal
			switch sess.finalizeTerminalSlot(c, ref) {
			case mqttTerminalIndeterminate:
				sess.retainAndReconcileTerminal(c, ref)
			case mqttTerminalRetainedReoffer:
				sess.reconcileRetainedTerminalSlot(c, ref)
			}
		}
		sess.retireActionLocked_(act)
		return
	}
	// UNRESOLVED after the bounded attempts: hand it to the reconciler.
	sess.scheduleActionReconcile(c, act)
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
		// The shutdown gate is closed, or there is no server to account the work to.
		// Leave the reservation for the next resume; re-arming past the gate is what
		// would hold shutdown open.
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

// installTerminalReservationLocked installs the process-local shadow of a durable
// terminal slot. It is called while replacing the delivered PUBREL entry, before the
// session lock is released, so allocation never observes an unreserved interval.
//
// sess.mu must be held.
func (sess *mqttSession) installTerminalReservationLocked(ref mqttInflightSlotRef) bool {
	if !ref.slotOK() || ref.name.subject == _EMPTY_ || ref.name.pi == 0 || ref.name.gen != sess.gen {
		return false
	}
	if sess.pendingPubRel == nil {
		sess.pendingPubRel = make(map[uint16]*mqttPending)
	}
	if ack := sess.pendingPubRel[ref.name.pi]; ack != nil &&
		(ack.regNonce != ref.nonce || ack.jsAckSubject != _EMPTY_) {
		return false
	}
	sess.pendingPubRel[ref.name.pi] = &mqttPending{
		regSeq:   ref.seq,
		regNonce: ref.nonce,
		slot:     ref,
	}
	return true
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
		ack.regSeq = ref.seq
		ack.slot = ref
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
		if sess.mqttTerminalizeOnPubComp(c, ref).settled() {
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

// reconcileRetainedTerminalSlot drives an INDEPENDENT retry of terminal cleanup for a slot
// that is deliberately retained because its source may still be re-offered (consult 7,
// Decision B). Unlike retainAndReconcileTerminal it installs NO pendingPubRel reservation, so
// it neither consumes Receive Maximum nor keeps the identifier's client-facing bookkeeping —
// the identifier is already freed. It is safe to leave the identifier unreserved because
// storeInflightRegister fail-closes over a retained terminal slot, so a re-mint attempt cannot
// erase the retirement or resend the payload; it merely fails until this cleanup succeeds.
//
// Each attempt re-runs mqttTerminalizeOnPubComp and stops once the slot is COMPLETED (gone),
// which happens as soon as the source settles (proven Complete or Lost) and the may-re-offer
// branch no longer holds. A retired generation ends it. Worker admission bounds it against
// shutdown exactly as the reservation-backed reconciler does.
//
// Session lock NOT held on entry.
func (sess *mqttSession) reconcileRetainedTerminalSlot(c *client, ref mqttInflightSlotRef) {
	sess.reconcileRetainedTerminalSlotAttempt(c, ref, 0)
}

// mqttTerminalReconcileMaxAttempts BOUNDS the reservation-less retained-slot reconcile so a
// source that stays re-offerable (or storage that stays unavailable) cannot spin a 2s timer
// for the life of the session. On exhaustion the retained terminal slot is left in place: it
// is durable, reserves only its own identifier (which bumpPI advances past), and the next
// resume's recovery reconciles it once the source settles — the same backstop the restart
// case already relies on.
const mqttTerminalReconcileMaxAttempts = 30

func (sess *mqttSession) reconcileRetainedTerminalSlotAttempt(c *client, ref mqttInflightSlotRef, attempt int) {
	if !ref.slotOK() || ref.name.subject == _EMPTY_ || ref.name.pi == 0 {
		return
	}
	if attempt >= mqttTerminalReconcileMaxAttempts {
		// Bounded out. The durable retained slot remains for recovery to finish; nothing is
		// lost, because storeInflightRegister fail-closes over it and the identifier is
		// naturally skipped until then.
		return
	}
	var srv *Server
	if c != nil {
		srv = c.srv
	}
	if srv == nil || !srv.admitWillWorker() {
		// No worker admitted: a later resume's recovery reconciles the retained slot instead.
		return
	}
	time.AfterFunc(mqttActionReconcileDelay, func() {
		defer srv.willWorkerWG.Done()
		sess.mu.Lock()
		retiredGeneration := ref.name.gen != sess.gen
		sess.mu.Unlock()
		if retiredGeneration {
			return
		}
		if sess.mqttTerminalizeOnPubComp(c, ref).completed() {
			return
		}
		sess.reconcileRetainedTerminalSlotAttempt(c, ref, attempt+1)
	})
}

// mqttTerminalOutcome is the tri-state result of PUBCOMP's durable terminal step, because
// two different NON-completions owe the caller different things (Decision: consult 7). An
// INDETERMINATE cleanup keeps the in-memory reservation and retries; a slot DELIBERATELY
// RETAINED for a source that may still be re-offered frees the identifier — the durable
// store fail-closes over a retained terminal slot, so re-mint cannot erase the retirement —
// while the source-keyed suppression stays and convergence is left to the source path or a
// later resume. Conflating the two over-reserves Receive Maximum for an exchange the client
// has already completed.
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
	// may still be re-offered. Free the identifier and keep the source suppression; the slot
	// is finished by an INDEPENDENT reservation-less reconcile worker
	// (reconcileRetainedTerminalSlot), not by holding this reservation.
	mqttTerminalRetainedReoffer
)

// settled reports whether the durable terminal step reached a definite conclusion — the slot
// is gone, belongs to another exchange, is draining under the holder's own action, or is
// deliberately retained for a re-offerable source. In every settled case the caller releases
// the in-memory reservation and frees the identifier; only mqttTerminalIndeterminate keeps it
// and retries.
func (o mqttTerminalOutcome) settled() bool { return o != mqttTerminalIndeterminate }

// completed reports mqttTerminalDone specifically: settled AND not a deliberately retained
// slot. Its sole caller is the retained-slot reconcile loop, where no live action is draining,
// so Done there means cleanup ran to a confirmed delete (or the slot already belonged to
// another exchange) and the loop stops; a retained slot is settled but NOT completed and the
// loop retries. (In PUBCOMP's own path Done is also returned for a `pubrel_terminal_draining`
// slot, where the live drain — not this predicate — owns finalization.)
func (o mqttTerminalOutcome) completed() bool { return o == mqttTerminalDone }

// mqttTerminalizeOnPubComp performs PUBCOMP's durable half: the named arrow out of
// whichever state the exchange is in, and then the terminal release.
//
// TWO arrows, and the distinction is the reason PUBCOMP cannot simply delete:
//
//   - from `pubrel_ready` there is no write outstanding, so the slot goes straight to
//     `pubrel_terminal` and cleanup may finish it;
//   - from `pubrel_sending` a write is still on the wire, so it goes to
//     `pubrel_terminal_draining` and frees NOTHING. The same action token is kept —
//     PUBCOMP does not revoke an action, it records that the exchange finished — and the
//     holder's own drain advances to `pubrel_terminal` when the write returns.
//
// Freeing the identifier in the second case is the defect this shape prevents: a PUBREL's
// bytes would still be going out under an identifier bumpPI had already re-minted.
//
// A LOOP, because PUBCOMP and the action's own drain race the same slot and whichever
// loses must REDISPATCH rather than give up on the arrow it chose. The interleaving a
// fixture forced: PUBCOMP reads `sending` and decides on `sending -> terminal_draining`;
// the drain lands `sending -> ready` first; PUBCOMP's CAS then fails its predecessor check
// and, without the loop, simply returned — leaving a `ready` slot that nothing would ever
// terminalize, with its identifier reserved for the life of the session.
//
// Session lock NOT held on entry.
func (sess *mqttSession) mqttTerminalizeOnPubComp(c *client, ref mqttInflightSlotRef) mqttTerminalOutcome {
	if !ref.slotOK() || ref.name.subject == _EMPTY_ {
		return mqttTerminalDone
	}
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
		switch {
		case err != nil:
			// Indeterminate. Neither an absence nor a licence: the client has answered
			// and the in-memory reservation is released by the caller's untrack, so what
			// remains is a durable slot recovery will finish.
			return mqttTerminalIndeterminate
		case smsg == nil:
			return mqttTerminalDone
		case cur == nil:
			// PRESENT BUT UNREADABLE is not absence. The durable occupant may still be
			// this exchange, so keep the local terminal reservation and retry later.
			return mqttTerminalIndeterminate
		}
		if !ref.matchesRecord(cur) {
			// Another exchange holds this identifier now. PRESERVE it: this PUBCOMP is
			// about an exchange whose slot is gone, and rewriting the current occupant is
			// the cross-exchange mutation F-092 exists to stop.
			return mqttTerminalDone
		}
		ref.seq = smsg.Sequence
		ref.state = cur.State
		switch cur.State {
		case mqttInflightPubRelReady:
			seq, ok := sess.casInflightSlot(c, ref, mqttInflightPubRelReady, mqttSlotAdvancePhase,
				func(next *mqttInflightRecord) {
					next.State = mqttInflightPubRelTerminal
				})
			if !ok {
				continue
			}
			// The ADVANCE'S OWN SEQUENCE, carried forward: finalizeTerminalSlot fences on
			// the observed sequence and the CAS just moved it, so a pre-advance sequence
			// would make this continuation refuse itself.
			ref.seq = seq
			ref.state = mqttInflightPubRelTerminal
			return sess.finalizeTerminalSlot(c, ref)
		case mqttInflightPubRelSending:
			// FREES NOTHING. The write is out; the holder's drain advances this to
			// terminal once it returns, and the identifier stays reserved until then.
			if _, ok := sess.casInflightSlot(c, ref, mqttInflightPubRelSending, mqttSlotAdvancePhase,
				func(next *mqttInflightRecord) {
					next.State = mqttInflightPubRelTerminalDraining
				}); !ok {
				continue
			}
			return mqttTerminalDone
		case mqttInflightPubRelTerminalDraining:
			// The drain owns the final arrow; nothing to do but let it run.
			return mqttTerminalDone
		case mqttInflightPubRelTerminal:
			// A retried PUBCOMP, or the drain already finished it. Cleanup is idempotent
			// and neither retransmits nor re-consumes debt.
			return sess.finalizeTerminalSlot(c, ref)
		default:
			return mqttTerminalIndeterminate
		}
	}
	return mqttTerminalIndeterminate
}

// finalizeTerminalSlot completes a terminal exchange and exact-deletes its slot.
//
// THREE things are required for the deletion, not two, and the third is the one an
// implementation forgets: subject addressing (F-034), identity authorization, and a
// FRESHNESS FENCE on the observed sequence. A rewrite between the observation and the
// delete means a concurrent operation re-targeted the exchange and reinstalled memory
// around it, so a stale terminal continuation must PRESERVE the record rather than delete
// it. TestMQTTInflightClaimTerminalRaceHealsPhantomEntry is the fixture for the opposite.
//
// Session lock NOT held on entry.
func (sess *mqttSession) finalizeTerminalSlot(c *client, ref mqttInflightSlotRef) mqttTerminalOutcome {
	cur, smsg, err := sess.asm.loadInflightRecord(ref.name.subject)
	if err != nil {
		return mqttTerminalIndeterminate
	}
	if smsg == nil {
		return mqttTerminalDone
	}
	if cur == nil {
		return mqttTerminalIndeterminate
	}
	if !ref.matchesRecord(cur) {
		return mqttTerminalDone
	}
	if cur.State != mqttInflightPubRelTerminal {
		return mqttTerminalIndeterminate
	}
	if ref.seq != 0 && smsg.Sequence != ref.seq {
		// THE FRESHNESS FENCE. The slot was rewritten after this continuation observed
		// it, so whatever re-targeted the exchange owns it now.
		return mqttTerminalIndeterminate
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
	if mqttInflightSourceMayReoffer(sess.asm.inflightSourceState(cur)) {
		// DELIBERATELY RETAINED for a source that may still be re-offered. The identifier
		// is freed by the caller (the durable store fail-closes over this retained terminal
		// slot), the source-keyed suppression stays, and the source path or a later resume
		// converges the slot. This is NOT the indeterminate case and must not keep the
		// Receive Maximum reservation for an exchange the client already completed.
		return mqttTerminalRetainedReoffer
	}
	// COMPLETE THE RECORD SIDE BEFORE THE SLOT SIDE. A peer may have stored more than one
	// same-exchange PUBREL before the phase gate closed record creation. Every such record
	// must have its affine proof discharged first and then be removed with a confirmed JS
	// operation. A partial enumeration, an uninterpretable same-exchange record, a failed
	// proof discharge or a failed removal keeps the terminal slot: deleting it first would
	// leave a slot-less record that admission can only refuse forever.
	if !sess.disposeTerminalPubRelRecords(c, cur) {
		return mqttTerminalIndeterminate
	}

	// The duplicate scan and its deletes are round trips. Re-read after them and require
	// the exact sequence this continuation observed; this is the freshness fence at the
	// irreversible edge, not merely an early advisory check.
	cur, smsg, err = sess.asm.loadInflightRecord(ref.name.subject)
	if err != nil || cur == nil || smsg == nil || smsg.Sequence != ref.seq ||
		!ref.matchesRecord(cur) || cur.State != mqttInflightPubRelTerminal {
		return mqttTerminalIndeterminate
	}
	if !sess.reapRegisterWithVerdictGated(smsg.Sequence, cur.Nonce, true, &ref, mqttSlotDeleteTerminal) {
		return mqttTerminalIndeterminate
	}
	// A confirmed exact-sequence delete can race a same-identity rewrite: the old sequence
	// is then absent and its delete succeeds while the rewritten slot survives. Confirm the
	// SUBJECT consequence before releasing the in-memory reservation.
	cur, smsg, err = sess.asm.loadInflightRecord(ref.name.subject)
	if err != nil {
		return mqttTerminalIndeterminate
	}
	if smsg == nil {
		sess.clearTerminalRetirementLocked_(ref)
		return mqttTerminalDone
	}
	if cur == nil {
		return mqttTerminalIndeterminate
	}
	if !ref.matchesRecord(cur) {
		sess.clearTerminalRetirementLocked_(ref)
		return mqttTerminalDone
	}
	return mqttTerminalIndeterminate
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

// advanceFinishedSlotToTerminal moves a past-PUBREC slot that recovery has PROVEN finished
// into `pubrel_terminal`, which is the only state terminal finalization accepts.
//
// This is the missing arrow, not a relaxation of the gate, and the distinction is the
// whole point. `mqttSlotDeleteComplete` is refused from `pubrel_ready` deliberately: a
// ready slot is the identifier's live durable claimant, and letting an ordinary reap vacate
// one is precisely the hazard F-092 exists to close. But recovery can hold a PROOF the
// exchange is over — the source is provably complete and no PUBREL record names it — and
// in that case the exchange did finish: either the client's PUBCOMP acknowledged the record
// away, or the record was never stored, and nothing can transmit it either way.
//
// So the proof licenses an ADVANCE, and the advance licenses the delete. Without this a
// re-phased slot whose delete was lost stays reserved for the life of the session, which
// with Receive Maximum 1 stops the session receiving anything at all —
// TestMQTTInflightStaleRegisterLeavesNoPhantomSlot asserts exactly that on the wire.
//
// Returns true when the slot is ready for terminal finalization.
//
// Session lock NOT held on entry.
func (sess *mqttSession) advanceFinishedSlotToTerminal(c *client, ref *mqttInflightSlotRef) bool {
	if ref == nil || !ref.slotOK() || ref.name.subject == _EMPTY_ {
		return false
	}
	switch ref.state {
	case mqttInflightPubRelTerminal:
		// Terminal finalization is already legal from here.
		return true
	case mqttInflightPublish:
		// This helper consumes a past-PUBREC completion proof. Ordinary PUBLISH cleanup
		// has its own disposition and may not manufacture a terminal phase.
		return false
	case mqttInflightPubRelSending, mqttInflightPubRelTerminalDraining:
		// AN ACTION IS LIVE. Recovery may not advance or delete this: the token is
		// somebody's capability, and observing that one exists proves phase progress
		// rather than authority to act. The identifier stays reserved and the exchange
		// stays non-actionable until its own holder drains it (the disclosed residual).
		return false
	}
	// pubrel_ready, with recovery's proof behind it.
	seq, ok := sess.casInflightSlot(c, *ref, mqttInflightPubRelReady, mqttSlotAdvancePhase,
		func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelTerminal
		})
	if !ok {
		return false
	}
	ref.seq = seq
	ref.state = mqttInflightPubRelTerminal
	return true
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
			return nil, sess.retainedPubRelAdmittedLocked_(c, pi, owner)
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
		if mqttInflightActionState(cur.State) {
			// An action is live. Another holder is transmitting, or one was abandoned;
			// observing a token proves phase progress, never authority to act.
			//
			// A DUPLICATE ARRIVING HERE DOES NOTHING — no steal, no piggyback
			// acknowledgement, no map mutation. AckWait re-offers it, and it is disposed
			// of once the canonical exchange is no longer mid-transmission.
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
			sess.jsa.sendAck(reply)
			return nil, false
		}
		if cur.State == mqttInflightPubRelTerminal {
			// The exchange is over. Nothing may transmit, and disposing of the record is
			// terminal cleanup's business rather than admission's.
			return nil, false
		}

		// CONTEXTUAL CHECKS, which are separate from the shape the validator already
		// proved and which can refuse even though the exchange IS past PUBREC.
		if !sess.pubRelAdmissionContextOK(c, pi, cur, dur, deliveredSeq, corr, affRef) {
			return nil, false
		}

		ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
		if ref == nil {
			return nil, false
		}
		switch cur.State {
		case mqttInflightPubRelReady:
			// The handoff caught up first. Ordinary acquisition, against the canonical
			// tuple the slot already names.
			if act := sess.mqttAcquirePubRelAction(c, *ref); act != nil {
				return act, true
			}
			// Lost the CAS. Reread and redispatch.
			continue
		case mqttInflightPublish:
			// THE DIRECT ARROW. One CAS installs the canonical identity AND the action, so
			// the evidence and the authorization are established together and there is no
			// window in which the slot is past PUBREC with nobody owning it.
			if act := sess.acquireActionFromPublish(c, *ref, dur, deliveredSeq); act != nil {
				return act, true
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
func (sess *mqttSession) acquireActionFromPublish(c *client, ref mqttInflightSlotRef,
	dur string, deliveredSeq uint64) *mqttSendAction {

	if dur == _EMPTY_ || deliveredSeq == 0 || !ref.slotOK() || ref.name.subject == _EMPTY_ {
		return nil
	}
	token := mqttNewInflightNonce()
	act := &mqttSendAction{token: token, state: mqttSendActionTentative, slot: ref}
	// ARM FIRST, exactly as ordinary acquisition does: a CAS that lands while this
	// goroutine is unwinding still has an owner that will drain it.
	sess.mu.Lock()
	if !sess.installTentativeActionLocked(ref.name.pi, act) {
		sess.mu.Unlock()
		return nil
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

	seq, ok := sess.casInflightSlot(c, ref, mqttInflightPublish, mqttSlotAcquireAction,
		func(next *mqttInflightRecord) {
			next.State = mqttInflightPubRelSending
			next.PubRelDurable = dur
			next.PubRelStreamSeq = deliveredSeq
			next.ActionToken = token
			// Bound to the custody the slot ALREADY carries: the validator requires the
			// two to be equal, so deriving them from this session's current belief would
			// write a record the next reader refuses.
			next.ActionClaim = next.Claim
			next.ActionClaimSeq = next.ClaimSeq
		})
	if !ok {
		handedOff = true
		sess.resolveUnacquiredAction(c, act)
		return nil
	}
	sess.mu.Lock()
	act.state = mqttSendActionAcquired
	act.slot.seq = seq
	act.slot.state = mqttInflightPubRelSending
	act.slot.actionToken = token
	// CARRY THE ACQUIRED SLOT onto the PUBREL entry. The PUBREC handler's
	// phaseSlotToPubRelReady would normally do this, and on this branch it LOST its CAS —
	// so without this the terminal release would have nothing to address and the
	// identifier would be reserved for the life of the session.
	if rel := sess.pendingPubRel[ref.name.pi]; rel != nil {
		rel.slot = act.slot
		rel.regSeq = seq
		rel.regNonce = ref.nonce
	}
	sess.mu.Unlock()
	handedOff = true
	return act
}

// pubRelAdmissionContextOK proves the things the shape validator cannot: that the named
// source, consumer durable and delivered record actually correspond to this exchange, and
// that nothing else about the slot forbids acting.
//
// These are the checks that make the direct arrow "evidence to action" rather than
// "nonce, therefore send". None of them disproves that the exchange is past PUBREC; each
// can still stop THIS broker from acquiring.
func (sess *mqttSession) pubRelAdmissionContextOK(c *client, pi uint16, cur *mqttInflightRecord,
	dur string, deliveredSeq uint64, corr mqttPubRelCorrelation, affRef mqttPubRelAffineRef) bool {

	if dur == _EMPTY_ || deliveredSeq == 0 {
		// No current PUBREL consumer, or a delivery whose own sequence is unreadable.
		// Neither can name a canonical record.
		return false
	}
	if cur.PI != pi {
		return false
	}
	if mqttInflightPastPubRec(cur.State) && cur.PubRelDurable != dur {
		// The canonical durable is contextual identity, not merely a non-empty string.
		// Acting through a slot naming another incarnation's consumer would transmit a
		// record that terminal cleanup can never safely dispose.
		return false
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
		return false
	}
	if cur.Cancelled && !cur.Retired {
		// "Never transmitted" contradicts a stored PUBREL. A contradictory slot is
		// refused rather than normalized: normalizing it would be deciding which half of
		// a shape this broker does not write to believe.
		return false
	}
	if !corr.Ok || corr.SrcDur != cur.Dur || corr.SrcSeq != cur.Seq {
		// The record names a DIFFERENT source than the slot's exchange. Refused: this is
		// the positive mismatch. An absent correlation is the retained shape, whose normal
		// authority is slot-less; it cannot authorize a source-backed slot merely because
		// an owner nonce happened to match.
		c.Warnf("MQTT: withholding a stored PUBREL for packet identifier %d of client %q: it "+
			"correlates to source %s/%d but the identifier's exchange came from %s/%d",
			pi, sess.id, corr.SrcDur, corr.SrcSeq, cur.Dur, cur.Seq)
		return false
	}
	// The affine pair must match the slot's, when the record carries one: a record proving
	// a DIFFERENT assignment must not be acknowledged away under this exchange.
	if affRef.valid() &&
		(affRef.Claim != cur.Q2AffClaim || affRef.Copy != cur.Q2AffCopy) {
		return false
	}
	if cur.Q2AffClaim != _EMPTY_ && affRef.State == mqttPubRelAffineRefAbsent {
		// With no carried reference PUBCOMP would conclude that no affine proof is owed
		// and acknowledge the record away. For an affine slot that would destroy the only
		// evidence from which recovery can prove the copy was delivered.
		return false
	}
	// AN EXACT LIVE PUBLISH SEND TOKEN forbids acquiring, because the opener owns
	// transmission admission only and cannot undo a PUBLISH already on the wire. It is
	// EXACT — this identifier's own PUBLISH token — and emphatically not a refusal on any
	// live pendingPublish entry, which is refuted fix 2, nor a retained-term refusal,
	// which is refuted fix 1.
	sess.mu.Lock()
	defer sess.mu.Unlock()
	// CUSTODY AND THE CHECK ARE ONE MECHANISM. Recovery contests every positive
	// transferable publish/pubrel_ready slot before CONNACK, including a ready slot whose
	// in-memory pendingPubRel entry already exists. Therefore every state admission can
	// acquire from is covered; active and terminal states are never acquired from here.
	//
	// Equality is checked while holding sess.mu, against the constructor-minted claim of
	// this exact session object. Without it a predecessor delayed behind takeover
	// notification could copy a successor's custody into a fresh action and write PUBREL
	// bytes to its own socket.
	if cur.Claim == _EMPTY_ || cur.Claim != sess.regClaim {
		return false
	}
	if h := sess.sendsInFlight[pi]; h != nil && h.pubIdent != _EMPTY_ {
		return false
	}
	return true
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

// retainedPubRelAdmittedLocked_ decides whether a slot-less PUBREL is this generation's
// own live retained exchange. Takes the session lock itself.
//
// The authority is the LIVE IN-MEMORY owner identity and nothing else — there is no
// durable half to consult, because a retained send has no register (R6). Two facts have to
// hold together: the identifier is tracked as a PUBREL exchange here, and that entry's
// exchange nonce is the one this record names.
//
// A mismatch, or no entry at all, refuses WITHOUT acknowledging. That is what makes a
// retained record surviving a restart safe: the in-memory identity died with the process,
// so nothing can authorize it, and it is refused rather than dropped — refused because
// transmitting it would send a PUBREL for an exchange this broker cannot attribute, and
// not dropped because acknowledging it away would destroy a record an operator may need
// to see.
func (sess *mqttSession) retainedPubRelAdmittedLocked_(c *client, pi uint16, owner mqttPubRelOwner) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	rel := sess.pendingPubRel[pi]
	if rel == nil || rel.regNonce == _EMPTY_ || rel.regNonce != owner.Nonce {
		c.Warnf("MQTT: withholding a stored PUBREL for packet identifier %d of client %q: no owner "+
			"slot names it and no live retained exchange of this incarnation claims exchange %q, so "+
			"nothing can authorize transmitting it", pi, sess.id, owner.Nonce)
		return false
	}
	if rel.regSeq != 0 {
		// It claims a register, so it is not retained — and its slot is absent, which
		// is not enough to prove the exchange is over. Refuse without acknowledging;
		// terminal disposal requires a terminal slot and cannot be inferred here.
		return false
	}
	if rel.jsDur == _EMPTY_ || rel.jsDur != sess.pubRelDurable() {
		// The entry does not name THIS generation's PUBREL consumer, so it is not a live
		// retained exchange of this incarnation. It is a recovered SOURCE-BACKED placeholder
		// (installPubRelLocked writes jsDur = the SOURCE consumer durable, inflight_register
		// .go:6267) whose durable owner slot is merely absent right now — a drain/removal
		// versus PUBREL-store crash cut leaves it — or a source-less recovery placeholder
		// (jsDur empty). Admitting either here would transmit it with no durable action,
		// which is F-092 recreated. Fail closed; recovery reconstructs the slot and admission
		// acquires the action the ordinary way.
		//
		// jsDur is the discriminator, not sseq: trackAsPubRel (mqtt.go:12781) stamps every
		// delivered entry with jsDur = pubRelConsumer.Durable AND fills sseq from the PUBREL
		// delivery, so a genuine retained exchange carries a non-zero sseq on every AckWait
		// redelivery after the first. An sseq test therefore refused legitimate retained
		// redeliveries — a lost PUBREL stalling the exchange, Receive Maximum 1 with it —
		// while jsDur separates the recovered source-backed placeholder cleanly and stays
		// true across redeliveries. A refused delivery returns before trackAsPubRel runs
		// (mqtt.go:19340), so a placeholder keeps its source shape rather than being rewritten
		// to the PUBREL durable on the way to being re-offered.
		return false
	}
	return true
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
