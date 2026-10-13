// Copyright 2026 The mqtt-nats-bridge Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import "sync/atomic"

// ---------------------------------------------------------------------------
// A pending entry's authority is its kind (F-105 cutover, pending-entry structural closure)
// ---------------------------------------------------------------------------
//
// An entry in pendingPublish or pendingPubRel either ACTS for an exchange — it may be
// transmitted, NAKed at resume, admitted, enrolled in a drain, or settle its source from the
// resident entry — or it only OCCUPIES an identifier. Which of the two is decided by the kind,
// and the kind is set by a constructor, never assigned on a resident entry. The zero value acts
// for nothing, so an entry nobody classified fails closed.
//
// Which obligation an entry stands for is its evidence. Evidence is produced by exactly three
// functions, each from a fact the caller holds (a validated register, a landed admission, an
// acquired action), never from a caller's string. The kind decides authority; evidence names the
// obligation, and a Hold may carry it too (a reservation over a named exchange).
//
// Both maps are written by one function, setPendingLocked, whose every call states what it
// expects at the identifier and which refuses an Ordinary entry unless custody holds its
// lineage. The release half is removeLineageProjectionsLocked: a lineage the ledger released
// takes every entry carrying its evidence. With both under sess.mu one precedes the other.
// See docs/ARCHITECTURE.md, "A pending entry's authority is its kind".

// mqttPendingKind is what an entry may DO.
type mqttPendingKind uint8

const (
	// mqttPendingHold occupies an identifier and authorizes no action. Its sub-state is the
	// reservation flags (regPending, regCancelled, regForeign, markerOpaque, the recovery
	// provenance flags, terminalReconcile), which no acting kind carries.
	mqttPendingHold mqttPendingKind = iota
	// mqttPendingOrdinary is an ordinary exchange's projection. It exists only while the
	// custody ledger holds its lineage (ev.lin) in this generation.
	mqttPendingOrdinary
	// mqttPendingRetained is a RETAINED exchange's projection (F-105 7c): register-backed like an
	// ordinary one, custody-gated exactly as Ordinary, and a DISTINCT kind so every selector whose
	// question is about a source delivery — a source ACK, a cpending mapping, a drain, a source
	// settlement — is closed to it by kind rather than by a guard at each site. Before admission
	// a retained allocation is a Hold (mqttNewRetainedAllocation).
	mqttPendingRetained
	// mqttPendingAffine is a slot-only-class exchange (chapter P). Classified here so its
	// entries are not mistaken for either of the others; its behaviour is unchanged.
	mqttPendingAffine
)

// acts reports whether the entry may be acted on at all. Every selector that grants an action
// consults it (or an exact kind); a Hold is refused whatever its other fields say.
func (p *mqttPending) acts() bool {
	return p != nil && p.kind != mqttPendingHold
}

// mqttPendingEvidence is WHICH obligation an entry stands for: the generation, the lineage and
// the exchange nonce it was established from.
type mqttPendingEvidence struct {
	gen, lin, nonce string
}

// mqttEvidenceFromRecord is evidence from a register the caller decoded and validated. A record
// without a lineage or a nonce names no obligation and yields none.
func mqttEvidenceFromRecord(gen string, rec *mqttInflightRecord) mqttPendingEvidence {
	if rec == nil || rec.Lin == _EMPTY_ || rec.Nonce == _EMPTY_ {
		return mqttPendingEvidence{}
	}
	return mqttPendingEvidence{gen: gen, lin: rec.Lin, nonce: rec.Nonce}
}

// mqttLandedRegister is what an admission batch landed: the register it wrote, read back from
// the commit. It is the only carrier of a landed lineage from the store to the entry.
type mqttLandedRegister struct {
	gen    string
	pi     uint16
	jsDur  string
	sseq   uint64
	regSeq uint64
	nonce  string
	lin    string
	qos    byte
}

// mqttEvidenceFromLanding is evidence from a landed admission.
func mqttEvidenceFromLanding(l mqttLandedRegister) mqttPendingEvidence {
	if l.lin == _EMPTY_ || l.nonce == _EMPTY_ {
		return mqttPendingEvidence{}
	}
	return mqttPendingEvidence{gen: l.gen, lin: l.lin, nonce: l.nonce}
}

// mqttEvidenceFromAction is evidence from an ACQUIRED ordinary action: the lineage its fresh
// read fixed and the slot's exchange nonce.
func mqttEvidenceFromAction(act *mqttSendAction) mqttPendingEvidence {
	if act == nil || act.lin == _EMPTY_ || act.slot.nonce == _EMPTY_ {
		return mqttPendingEvidence{}
	}
	return mqttPendingEvidence{gen: act.slot.name.gen, lin: act.lin, nonce: act.slot.nonce}
}

// ordinaryHeldLocked is the custody condition an Ordinary entry exists under. One function,
// applied by setPendingLocked to every Ordinary install and by the PUBREL callback's validation.
//
// Custody holds the obligation when the ledger holds its lineage — or, for an admission whose
// attempt landed behind a lost response, still holds the ALLOCATION's own admission, which the
// reconciler resolves into that exchange (or, if nothing landed, releases). The admission is the
// same obligation before its lineage is known to the ledger, and its release takes the entry's
// identifier with it.
//
// Lock held on entry.
func (sess *mqttSession) ordinaryHeldLocked(ev mqttPendingEvidence) bool {
	if ev.gen != sess.gen || ev.lin == _EMPTY_ || sess.custody == nil {
		return false
	}
	return sess.custody.hasExchange(ev.gen, ev.lin) || sess.custody.holdsAdmission(ev.gen, ev.nonce)
}

// mqttPendingMapID names one of the two session maps.
type mqttPendingMapID uint8

const (
	mqttPendingPublishMap mqttPendingMapID = iota
	mqttPendingPubRelMap
)

// mqttPendingExpect is what a writer requires at the identifier before it writes. Built only by
// the three functions below; every setPendingLocked call passes one of them directly.
type mqttPendingExpect struct {
	mode    uint8
	current *mqttPending
}

const (
	mqttExpectModeEmpty uint8 = iota + 1
	mqttExpectModeExactly
	mqttExpectModeAny
)

// mqttExpectEmpty installs only into an absent identifier.
func mqttExpectEmpty() mqttPendingExpect { return mqttPendingExpect{mode: mqttExpectModeEmpty} }

// mqttExpectExactly replaces only the entry the caller captured when it decided the replacement
// was eligible. A capture removed or replaced in the meantime is never brought back.
func mqttExpectExactly(p *mqttPending) mqttPendingExpect {
	return mqttPendingExpect{mode: mqttExpectModeExactly, current: p}
}

// mqttExpectCaptured is exactly what the caller captured at its eligibility decision, where
// capturing nothing means the identifier must still be empty.
func mqttExpectCaptured(p *mqttPending) mqttPendingExpect {
	if p == nil {
		return mqttExpectEmpty()
	}
	return mqttExpectExactly(p)
}

// mqttExpectAny overwrites. Permitted only where the caller validated the resident itself under
// the same lock (the PUBREL callback's commit) or put back the exact entry it removed (the
// rollback reinsertions); the structural test records those sites.
func mqttExpectAny() mqttPendingExpect { return mqttPendingExpect{mode: mqttExpectModeAny} }

// setPendingLocked is the only write to either session map. It refuses — leaving the resident
// exactly as it was — when the expectation does not hold, or when next is Ordinary and custody
// does not hold its lineage. Callers commit what goes with the entry (its cpending mapping, a
// handoff, an exclusion adoption) only after it reports true, under the same lock.
//
// Lock held on entry.
func (sess *mqttSession) setPendingLocked(which mqttPendingMapID, pi uint16, expect mqttPendingExpect, next *mqttPending) bool {
	if next == nil {
		return false
	}
	m := sess.pendingMapLocked(which)
	cur := m[pi]
	switch expect.mode {
	case mqttExpectModeEmpty:
		if cur != nil {
			return false
		}
	case mqttExpectModeExactly:
		if expect.current == nil || cur != expect.current {
			return false
		}
	case mqttExpectModeAny:
	default:
		return false
	}
	if (next.kind == mqttPendingOrdinary || next.kind == mqttPendingRetained) && !sess.ordinaryHeldLocked(next.ev) {
		return false
	}
	m[pi] = next
	return true
}

// pendingMapLocked returns the named map, creating it on first use.
//
// Lock held on entry.
func (sess *mqttSession) pendingMapLocked(which mqttPendingMapID) map[uint16]*mqttPending {
	if which == mqttPendingPubRelMap {
		if sess.pendingPubRel == nil {
			sess.pendingPubRel = make(map[uint16]*mqttPending)
		}
		return sess.pendingPubRel
	}
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	return sess.pendingPublish
}

// ---------------------------------------------------------------------------
// Constructors. The only places kind and ev are assigned.
// ---------------------------------------------------------------------------

// mqttAllocationFence is the custody state an allocation was taken under: the generation, the
// connection, the ledger and its holder epoch. The allocator never hands out an identifier the
// ledger holds, so at allocation no lineage on this identifier is a holder; an unchanged fence
// carries that fact forward.
type mqttAllocationFence struct {
	valid       bool
	gen         string
	c           *client
	ledger      *mqttCustodyLedger
	holderEpoch uint64
}

// allocationFenceLocked captures the fence for an allocation being taken now.
//
// Lock held on entry.
func (sess *mqttSession) allocationFenceLocked() mqttAllocationFence {
	f := mqttAllocationFence{valid: true, gen: sess.gen, c: sess.c, ledger: sess.custody}
	if sess.custody != nil {
		f.holderEpoch = sess.custody.holderEpoch
	}
	return f
}

// mqttNewAllocation is a new delivery's identifier, held while its register store is in flight
// (R1). It acts for nothing until the landing promotes it.
func mqttNewAllocation(jsDur string, sseq uint64, ackSubj, nonce string, fence mqttAllocationFence) *mqttPending {
	return &mqttPending{jsDur: jsDur, sseq: sseq, jsAckSubject: ackSubj, regPending: true, regNonce: nonce,
		allocFence: fence}
}

// mqttNewUnmintedHold is an entry for a source the session maps to an identifier with no entry
// behind it. It has no nonce and no evidence, and no send authority (beginSendLocked requires a
// transmission state it lacks).
func mqttNewUnmintedHold(jsDur string, sseq uint64, ackSubj string) *mqttPending {
	return &mqttPending{jsDur: jsDur, sseq: sseq, jsAckSubject: ackSubj}
}

// mqttNewCancelledHold is a withdrawn allocation whose store has not settled.
func mqttNewCancelledHold(nonce string) *mqttPending {
	return &mqttPending{regPending: true, regCancelled: true, regNonce: nonce}
}

// mqttRecoveryOpaquePending reserves an identifier behind an unreadable register.
func mqttRecoveryOpaquePending(seq uint64) *mqttPending {
	return &mqttPending{regSeq: seq, regForeign: true, regRecoveryOpaque: true}
}

// mqttNewBoundOpaque binds an opaque reservation to the register it stands over, once that
// register is read: it names the obligation and still acts for nothing.
func mqttNewBoundOpaque(prev *mqttPending, gen string, seq uint64, r *mqttInflightRecord) *mqttPending {
	next := *prev
	next.kind, next.ev = mqttPendingHold, mqttEvidenceFromRecord(gen, r)
	next.regSeq, next.regNonce = seq, r.Nonce
	next.jsDur, next.sseq, next.qos = r.Dur, r.Seq, r.QoS
	return &next
}

// mqttNewDemoted demotes a resident entry to a reservation that keeps its identity, its
// evidence, its ledger charge and its send section. A reactivation replaces it again.
func mqttNewDemoted(prev *mqttPending) *mqttPending {
	next := *prev
	if !prev.regForeign {
		next.regRecoveryDemoted = true
		next.demotedKind = prev.kind
	}
	next.kind, next.regForeign = mqttPendingHold, true
	return &next
}

// mqttNewPoisoned is the reservation left for a register whose group is poisoned or whose claim
// was lost: the identifier stays reserved, carrying forward an active send section.
func mqttNewPoisoned(seq uint64, prev *mqttPending) *mqttPending {
	return &mqttPending{regSeq: seq, regForeign: true, regRecoveryOpaque: true,
		sendNonce: prev.sendNonce, sendSettle: prev.sendSettle}
}

// mqttNewPubRelPlaceholder is the scan's reservation for a recovered PUBREL record: it knows the
// exchange nonce and the source, not the lineage, so it names nothing the ledger could release.
func mqttNewPubRelPlaceholder(srcDur string, srcSeq uint64, nonce string) *mqttPending {
	return &mqttPending{jsDur: srcDur, sseq: srcSeq, regNonce: nonce, regRecoveryPlaceholder: true}
}

// mqttPendingExchangeSource is the SOURCE DELIVERY an entry's exchange was admitted for: the
// captured slot's (durable, sequence) when the entry carries a valid slot — a PUBREC successor, a
// promoted PUBREL action entry, a terminal reservation — and the entry's own coordinates for a
// recovered PUBREL placeholder, which knows its source before any slot is read. A sourceless
// (retained) exchange has none.
func mqttPendingExchangeSource(ack *mqttPending) (dur string, sseq uint64, ok bool) {
	switch {
	case ack == nil:
		return _EMPTY_, 0, false
	case ack.slot.slotOK() && ack.slot.dur != _EMPTY_ && ack.slot.sseq != 0:
		return ack.slot.dur, ack.slot.sseq, true
	case ack.regRecoveryPlaceholder && ack.jsDur != _EMPTY_ && ack.sseq != 0:
		return ack.jsDur, ack.sseq, true
	}
	return _EMPTY_, 0, false
}

// sourcePendingPubRelLocked reports whether the source delivery (jsDur, sseq) is represented by an
// in-memory PUBREL-phase entry: its exchange is past PUBREC on this session, so the source's
// payload must not be admitted again (F-115). One source delivery has at most one un-ended
// exchange, and the PUBREC handoff removes the source's cpending link in the same critical section
// that installs this entry, so a re-offer of the source meets one or the other.
//
// The answer lasts exactly as long as the entry: PUBCOMP's terminal reservation carries it until
// that reservation is released, and nothing past the release is claimed (docs/CONFORMANCE.md).
// The slot-only (experimental affine) class is excluded by the SOURCE's durable, never by the
// PUBREL consumer durable a promoted entry delivers on.
//
// Session lock held on entry.
func (sess *mqttSession) sourcePendingPubRelLocked(jsDur string, sseq uint64) bool {
	if jsDur == _EMPTY_ || sseq == 0 || mqttDurIsSlotOnlyClass(jsDur) {
		return false
	}
	for _, ack := range sess.pendingPubRel {
		if dur, seq, ok := mqttPendingExchangeSource(ack); ok && dur == jsDur && seq == sseq {
			return true
		}
	}
	return false
}

// mqttNewTerminalReservation holds the identifier of an exchange whose terminal cleanup is
// under way. With prev (the same exchange's entry) it carries prev's evidence and ledger charge;
// it never carries an ACK subject, which terminal release and reconcile require empty.
func mqttNewTerminalReservation(ref mqttInflightSlotRef, prev *mqttPending) *mqttPending {
	next := &mqttPending{regSeq: ref.seq, regNonce: ref.nonce, slot: ref}
	if prev != nil {
		// The obligation's evidence and ledger charge, and nothing of the delivery: a terminal
		// reservation names its slot, not a source (the cleanup owners key on that shape).
		next.ev, next.custody = prev.ev, prev.custody
		// A reconcile worker already running for this reservation stays the only one.
		next.terminalReconcile = prev.terminalReconcile
	}
	return next
}

// mqttNewMarkerOpaque is the MARKER-OPAQUE reservation over a register whose transmission state
// could not be read: it names the obligation ev stands for and acts for nothing.
func mqttNewMarkerOpaque(prev *mqttPending, ev mqttPendingEvidence, regSeq uint64, qos byte, slot mqttInflightSlotRef, hasSlot bool) *mqttPending {
	next := *prev
	next.kind, next.ev = mqttPendingHold, ev
	next.regPending, next.markerOpaque = false, true
	next.transmitted, next.markerSeq = false, 0
	next.regSeq, next.regNonce, next.qos = regSeq, ev.nonce, qos
	if hasSlot {
		next.slot = slot
	}
	return &next
}

// mqttPromoteLanded promotes an allocation whose register landed: Ordinary, or Affine for the
// slot-only class, with the landed identity and the send state the pair licenses.
func mqttPromoteLanded(prev *mqttPending, l mqttLandedRegister, markerSeq uint64, transmitted bool, slot mqttInflightSlotRef, hasSlot bool) *mqttPending {
	next := *prev
	next.kind, next.ev = mqttPendingOrdinary, mqttEvidenceFromLanding(l)
	if mqttDurIsSlotOnlyClass(prev.jsDur) {
		next.kind = mqttPendingAffine
	}
	next.qos = l.qos
	next.regSeq, next.regNonce, next.regPending = l.regSeq, l.nonce, false
	next.markerSeq, next.transmitted = markerSeq, transmitted
	if hasSlot {
		next.slot = slot
	}
	return &next
}

// mqttReactivate returns a demoted entry to the kind it had before the demotion, and no more:
// a demoted reservation comes back a reservation. An Ordinary reactivation additionally requires
// the validated record's evidence to be the evidence the demoted entry carries (setPendingLocked
// then requires custody to hold it); an Affine one requires the record to be slot-only.
func mqttReactivate(prev *mqttPending, gen string, rec *mqttInflightRecord) *mqttPending {
	if prev == nil || !prev.regRecoveryDemoted || rec == nil {
		return nil
	}
	next := *prev
	next.regForeign, next.regRecoveryDemoted = false, false
	next.kind, next.demotedKind = prev.demotedKind, mqttPendingHold
	switch next.kind {
	case mqttPendingAffine:
		if !mqttDurIsSlotOnlyClass(rec.Dur) {
			return nil
		}
	case mqttPendingOrdinary:
		if ev := mqttEvidenceFromRecord(gen, rec); ev.lin == _EMPTY_ || ev != prev.ev {
			return nil
		}
	}
	return &next
}

// mqttNewRetainedAllocation is a retained delivery's identifier from its snapshot to its admission
// (F-105 7c): a Hold, held while the materialization and the admission are in flight, acting for
// nothing until the landing promotes it. The nonce is the allocation's and becomes the register's.
func mqttNewRetainedAllocation(nonce string, qos byte, fence mqttAllocationFence) *mqttPending {
	return &mqttPending{regPending: true, regNonce: nonce, qos: qos, allocFence: fence}
}

// mqttPromoteRetainedLanded promotes a retained allocation whose admission landed: kind Retained,
// the landed identity, the unsent marker the admission wrote, and the slot. The ledger's charge is
// the allocation's own, set when its admission opened (openAdmissionCustodyLocked), exactly as an
// ordinary promotion inherits it.
func mqttPromoteRetainedLanded(prev *mqttPending, l mqttLandedRegister, markerSeq uint64, slot mqttInflightSlotRef,
	inc string) *mqttPending {
	next := *prev
	next.retainedInc = inc
	next.kind, next.ev = mqttPendingRetained, mqttEvidenceFromLanding(l)
	next.qos = l.qos
	next.regSeq, next.regNonce, next.regPending = l.regSeq, l.nonce, false
	next.markerSeq, next.transmitted = markerSeq, false
	next.slot = slot
	return &next
}

// mqttNewPubRelSuccessor is the PUBREL-phase entry that follows a PUBREC or a redelivery. It
// inherits its predecessor's kind and evidence and never mints authority: the PUBREL
// predecessor first, else the PUBLISH entry, else a Hold. The exchange's durable identity (slot,
// sequence, nonce) and its ledger charge survive with it.
func mqttNewPubRelSuccessor(rel, pub *mqttPending, jsDur string, sseq uint64, reply string) *mqttPending {
	next := &mqttPending{jsDur: jsDur, sseq: sseq, jsAckSubject: reply}
	src := rel
	if src == nil {
		src = pub
	}
	if src != nil {
		next.kind, next.ev = src.kind, src.ev
		next.slot, next.regSeq, next.regNonce, next.custody = src.slot, src.regSeq, src.regNonce, src.custody
	}
	return next
}

// mqttPromotePubRelAction is the entry an acquired PUBREL action delivers through: Ordinary (or
// Affine for the slot-only class), with the action's evidence and slot, the delivery's ACK
// subject, and the ledger charge and record facts of a compatible predecessor.
func mqttPromotePubRelAction(prev *mqttPending, act *mqttSendAction, jsDur string, sseq uint64, reply string) *mqttPending {
	next := &mqttPending{jsDur: jsDur, sseq: sseq, jsAckSubject: reply,
		slot: act.slot, regSeq: act.slot.seq, regNonce: act.slot.nonce}
	if prev != nil {
		next.custody, next.pubRelAffRef = prev.custody, prev.pubRelAffRef
	}
	if mqttDurIsSlotOnlyClass(act.slot.dur) {
		next.kind = mqttPendingAffine
	} else {
		// An Ordinary entry exists only while the ledger holds its lineage, so the ledger owns
		// its Receive Maximum charge whatever the predecessor recorded.
		next.kind, next.ev, next.custody = mqttPendingOrdinary, mqttEvidenceFromAction(act), true
	}
	return next
}

// mqttRecoveredRetainedPending is a retained exchange restored by the resume (F-105 7c): kind
// Retained with the register's evidence and slot, the send state the marker licensed, and — for a
// PUBLISH — the verified material its replay emits. setPendingLocked's custody gate then requires
// the ledger to hold its lineage.
func mqttRecoveredRetainedPending(gen string, rr *mqttRecoveredRetained, slot mqttInflightSlotRef) *mqttPending {
	next := &mqttPending{kind: mqttPendingRetained, ev: mqttEvidenceFromRecord(gen, rr.rec), qos: rr.rec.QoS,
		regSeq: rr.seq, regNonce: rr.rec.Nonce, slot: slot, custody: true}
	if rr.kind == mqttRecoveredRetainedPublish {
		next.markerSeq, next.transmitted = rr.markerSeq, rr.transmitted
		next.retainedReplay = &mqttRetainedReplayMaterial{m: rr.m, data: rr.data}
		if rr.m != nil {
			next.retainedInc = rr.m.Inc
		}
	}
	return next
}

// mqttWithRetainedReplay is a resident retained PUBLISH entry given its resume's verified material
// and the marker's send state, kind and evidence unchanged.
func mqttWithRetainedReplay(prev *mqttPending, rr *mqttRecoveredRetained) *mqttPending {
	next := *prev
	next.markerSeq, next.transmitted = rr.markerSeq, rr.transmitted
	next.retainedReplay = &mqttRetainedReplayMaterial{m: rr.m, data: rr.data}
	return &next
}

// mqttPromoteRetainedPubRelAction is the entry a retained exchange's acquired PUBREL action
// writes through (F-105 7c): the resident Retained entry with the action's evidence and slot, and
// nothing source-shaped — no delivery reply, no consumer sequence.
func mqttPromoteRetainedPubRelAction(prev *mqttPending, act *mqttSendAction) *mqttPending {
	next := *prev
	next.kind, next.ev = mqttPendingRetained, mqttEvidenceFromAction(act)
	next.slot, next.regSeq, next.regNonce = act.slot, act.slot.seq, act.slot.nonce
	return &next
}

// mqttNewAbandonedRetainedPubRel is the PUBREL-phase entry of an ABANDONED retained exchange
// whose late PUBREC advanced it (F-105 7c): the ACK-only snapshot's evidence and slot, now at
// pubrel_ready with its canonical created by that advance. Kind Retained, so the PUBREL is emitted
// from the canonical and PUBCOMP terminalizes it; the ledger still holds the lineage (an abandoned
// exchange keeps its identifier), which is what the custody gate requires.
func mqttNewAbandonedRetainedPubRel(ack *mqttPending) *mqttPending {
	return &mqttPending{kind: mqttPendingRetained, ev: ack.ev, qos: ack.qos, slot: ack.slot,
		regSeq: ack.slot.seq, regNonce: ack.slot.nonce, custody: true}
}

// mqttNewReleasedDelivery is what the lineage's release leaves of a delivered PUBREL whose
// PUBCOMP connection owner still owes: a Hold that acts for nothing, keeping the ACK subject, the
// slot binding, the evidence and the ledger charge the PUBCOMP handler settles through.
func mqttNewReleasedDelivery(prev *mqttPending, owner uint64) *mqttPending {
	next := *prev
	next.kind, next.awaitingPubCompOwner = mqttPendingHold, owner
	return &next
}

// mqttAckSnapshot is an ACK binding's completion evidence as an entry-shaped value: a Hold
// carrying the binding's evidence, never installed and never scheduling authority. The observed
// state follows the role: a HandBack's register is delivery_abandoned, an AckRoute's is still
// publish (its exchange was transmitted and never answered).
func mqttAckSnapshot(b *mqttAllocationAckBinding) *mqttPending {
	state := mqttInflightDeliveryAbandoned
	if b.role == mqttAckRoleAckRoute {
		state = mqttInflightPublish
	}
	return &mqttPending{
		jsDur: b.dur, sseq: b.seq, regNonce: b.nonce, ev: b.ev, qos: b.qos, custody: true,
		slot: mqttInflightSlotRef{
			name: b.name, nonce: b.nonce, dur: b.dur, sseq: b.seq, acq: b.acq,
			state: state, abandonedAck: b,
		},
	}
}

// ---------------------------------------------------------------------------
// The acquired PUBREL action becomes an entry in one place
// ---------------------------------------------------------------------------

// bindActionToPubRelLocked is the ONE place an acquired PUBREL action becomes a pending entry,
// and it VALIDATES EVERYTHING BEFORE IT MUTATES ANYTHING: the resident PUBREL entry must be this
// exchange's (absent, or carrying the action's nonce — the scan's placeholder, the PUBREC
// successor, an earlier delivery), the resident PUBLISH entry must pass the F-093 handoff's own
// ownership test, and the action must be one the session may act on — an ORDINARY action while
// custody holds its lineage, an AFFINE one (no exchange, no custody) in this generation, its
// authority being the slot CAS it already won. Only then is the entry built, from the action's
// evidence and slot, and committed with its mapping and the handoff under the same lock.
//
// A refusal changes nothing: the resident entries, their mappings and the PUBLISH entry stay as
// they were. A lineage released between the acquisition and here already took its entries with
// it, so its refusal leaves no entry and no mapping. The caller returns before write entry; the
// drain it armed gives the durable action back and AckWait re-offers the record.
//
// Lock held on entry.
func (sess *mqttSession) bindActionToPubRelLocked(pi uint16, act *mqttSendAction, reply string, affRef mqttPubRelAffineRef) bool {
	if act == nil || sess.pubRelConsumer == nil || sess.recoveryPIHeldLocked(pi) {
		return false
	}
	prev := sess.pendingPubRel[pi]
	if prev != nil && (prev.regNonce == _EMPTY_ || prev.regNonce != act.slot.nonce) {
		return false
	}
	if !sess.publishHandoffCompatibleLocked(pi, act) {
		return false
	}
	if mqttDurIsSlotOnlyClass(act.slot.dur) {
		if act.slot.name.gen != sess.gen {
			return false
		}
	} else if !sess.ordinaryHeldLocked(mqttEvidenceFromAction(act)) {
		return false
	}
	jsDur := sess.pubRelConsumer.Durable
	sseq, _, _, _, _ := ackReplyInfo(reply)
	next := mqttPromotePubRelAction(prev, act, jsDur, sseq, reply)
	next.pubRelAffRef = affRef
	// ANY: the resident was validated above under this same lock.
	if !sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectAny(), next) {
		return false
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}
	sseqToPi := sess.cpending[jsDur]
	if sseqToPi == nil {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[jsDur] = sseqToPi
	}
	sseqToPi[sseq] = pi
	// F-093's ordered handoff, now that this exchange's PUBREL-side owner is installed. Its
	// ownership test passed above, so it cannot refuse here.
	sess.handOffPublishEntryLocked(pi, act)
	return true
}

// publishHandoffCompatibleLocked is handOffPublishEntryLocked's ownership test alone: no
// resident PUBLISH entry, or one that is this action's exchange.
//
// Lock held on entry.
func (sess *mqttSession) publishHandoffCompatibleLocked(pi uint16, act *mqttSendAction) bool {
	pub := sess.pendingPublish[pi]
	if pub == nil {
		return true
	}
	return act != nil && pub.regNonce == act.slot.nonce && pub.jsDur == act.slot.dur && pub.sseq == act.slot.sseq
}

// pubRelWriteKindLocked is the exact kind a PUBREL transmission requires of the entry at the
// moment of write entry: an acquired action writes through an Ordinary or Affine entry of its
// own exchange, a slot-less retained exchange through its Retained entry.
//
// Lock held on entry.
func (sess *mqttSession) pubRelWriteKindLocked(pi uint16, act *mqttSendAction) bool {
	rel := sess.pendingPubRel[pi]
	if rel == nil {
		return false
	}
	if act == nil {
		return false // no slot-less retained exchange remains (F-105 7c): every PUBREL holds an action
	}
	return (rel.kind == mqttPendingOrdinary || rel.kind == mqttPendingAffine || rel.kind == mqttPendingRetained) &&
		rel.regNonce == act.slot.nonce
}

// Test seams at the four instants the pending-entry closure's schedules need held open.
var (
	mqttTestBeforePubRelPromotionV        atomic.Pointer[func()]
	mqttTestAfterPubRelCommitV            atomic.Pointer[func()]
	mqttTestBeforeMarkerOpaqueConversionV atomic.Pointer[func()]
	mqttTestAfterMarkerOpaqueConversionV  atomic.Pointer[func()]
	mqttTestAfterPubRelPhaseCASV          atomic.Pointer[func()]
)

func mqttRunTestSeam(v *atomic.Pointer[func()]) {
	if h := v.Load(); h != nil {
		(*h)()
	}
}
