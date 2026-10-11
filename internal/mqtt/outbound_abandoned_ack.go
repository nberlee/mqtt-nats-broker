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

// mqttAllocationAckBindingRole is WHY an ACK-only binding exists (F-088 WP2, R2-F1), and every reader of a
// binding dispatches on it: the two roles share the ACK routing and nothing else.
type mqttAllocationAckBindingRole uint8

const (
	// mqttAckRoleHandBack: the binding of an ABANDONED delivery (its register is
	// delivery_abandoned, its exchange carries the abandonment fact). Terminal reconstruction is
	// allowed; its ending matcher compares the abandonment fact; a SHARED one is also the
	// HandBack that keeps the source-ACK gate closed, so it is removed only when its exchange
	// ended AND its register is proven deleted.
	mqttAckRoleHandBack mqttAllocationAckBindingRole = iota + 1
	// mqttAckRoleAckRoute: the binding of a POSSIBLY ATTEMPTED delivery whose scheduling entry
	// was removed while its exchange was still unanswered (mqttUntrackUnanswered with
	// PossiblyAttempted evidence). Its register is preserved at publish; the binding only
	// routes the client's late acknowledgement to the existing completion owners. It ends on
	// the exchange's ending alone, matched by lineage, shared or not, and it never holds a
	// source gate.
	mqttAckRoleAckRoute
)

// mqttAllocationAckBinding is an immutable allocation identity, not a scheduling entry. The source reply
// remains in the durable register (AckRoute) or abandonment fact (HandBack) for the settlement
// owner.
type mqttAllocationAckBinding struct {
	role  mqttAllocationAckBindingRole
	sess  *mqttSession
	epoch string
	name  mqttInflightName
	nonce string
	// ev is the evidence of the obligation the binding was built from (its record or entry),
	// carried into the completion snapshot (mqttAckSnapshot) that bindAbandonedAckOrigin compares.
	ev  mqttPendingEvidence
	dur string
	seq uint64
	qos byte
	acq uint64
	// payload names a RETAINED delivery's copy (F-105 7c), which has no source.
	payload string
}

type mqttAbandonedAckIndex struct {
	gen string
	pi  uint16
}

func (b *mqttAllocationAckBinding) index() mqttAbandonedAckIndex {
	return mqttAbandonedAckIndex{gen: b.name.gen, pi: b.name.pi}
}

// handBack reports whether b is an abandonment's binding.
func (b *mqttAllocationAckBinding) handBack() bool { return b != nil && b.role == mqttAckRoleHandBack }

// sameAllocation reports whether b and o name the same allocation: identifier, nonce, lineage
// and source delivery. Upgrade (an abandonment's HandBack over an AckRoute) is allowed only here.
func (b *mqttAllocationAckBinding) sameAllocation(o *mqttAllocationAckBinding) bool {
	return b != nil && o != nil && b.name == o.name && b.nonce == o.nonce && b.ev.lin == o.ev.lin &&
		b.dur == o.dur && b.seq == o.seq && b.payload == o.payload
}

// newMQTTAbandonedAckBinding constructs the ACK-only HandBack identity from a validated abandoned
// register. C5 can use the same constructor after reconstructing and pairing the durable
// exchange; constructing this value alone neither installs it nor authorizes output.
// Session lock held.
func newMQTTAbandonedAckBinding(sess *mqttSession, epoch, subject string,
	rec *mqttInflightRecord) *mqttAllocationAckBinding {

	if sess == nil || epoch == _EMPTY_ || rec == nil ||
		rec.State != mqttInflightDeliveryAbandoned || mqttValidateInflightShape(rec) != nil {
		return nil
	}
	ref := mqttSlotRefFromRecord(subject, 0, rec)
	if ref == nil || ref.name.gen != sess.gen || ref.name.idHash != sess.idHash ||
		!mqttInflightRecordMatchesSubject(rec, subject) {
		return nil
	}
	return &mqttAllocationAckBinding{
		role: mqttAckRoleHandBack,
		sess: sess, epoch: epoch, name: ref.name, nonce: rec.Nonce, ev: mqttEvidenceFromRecord(ref.name.gen, rec),
		dur: rec.Dur, seq: rec.Seq, qos: rec.QoS, acq: rec.Acq, payload: rec.Payload,
	}
}

// newMQTTAbandonedAckBindingFromTerminal reconstructs the HandBack of a SHARED abandonment whose
// exchange already reads Terminal: recovery must restore the HandBack it still holds (7a), and a
// terminal register no longer carries the abandoned state the ordinary constructor requires. The
// allocation identity comes from the exchange's immutable fact, validated against the register.
// Session lock held.
func newMQTTAbandonedAckBindingFromTerminal(sess *mqttSession, epoch, subject string,
	rec *mqttInflightRecord, x *mqttOutboundExchange) *mqttAllocationAckBinding {

	if sess == nil || epoch == _EMPTY_ || rec == nil || x == nil || x.Abandoned == nil ||
		rec.State != mqttInflightPublishTerminal || x.Phase != mqttExchangeTerminal ||
		!mqttIsSharedDurable(rec.Dur) || mqttValidateInflightShape(rec) != nil ||
		mqttCheckSharedAcqPair(rec, x) != nil {
		return nil
	}
	f := x.Abandoned
	if f.Alloc != rec.Nonce || f.Acq != rec.Acq || f.Dur != rec.Dur || f.Seq != rec.Seq ||
		x.Nonce != rec.Lin || x.PI != rec.PI || x.QoS != rec.QoS {
		return nil
	}
	ref := mqttSlotRefFromRecord(subject, 0, rec)
	if ref == nil || ref.name.gen != sess.gen || ref.name.idHash != sess.idHash ||
		!mqttInflightRecordMatchesSubject(rec, subject) {
		return nil
	}
	return &mqttAllocationAckBinding{
		role: mqttAckRoleHandBack,
		sess: sess, epoch: epoch, name: ref.name, nonce: rec.Nonce, ev: mqttEvidenceFromRecord(ref.name.gen, rec),
		dur: rec.Dur, seq: rec.Seq, qos: rec.QoS, acq: rec.Acq,
	}
}

// newMQTTAckRouteBinding constructs the AckRoute of a TRANSMITTED entry that is being removed
// unanswered (F-088 WP2). It is built from the ENTRY — its captured slot reference, allocation
// nonce, evidence and acquisition — never from a record read, so it names exactly the
// transmission this connection made: a later acquisition (k+1) is not this binding's, and the
// existing acquisition checks refuse it.
//
// Session lock held.
func newMQTTAckRouteBinding(sess *mqttSession, ack *mqttPending) *mqttAllocationAckBinding {
	// A RETAINED entry is not routed by an AckRoute: its register names its materialization
	// (Payload), which the entry does not carry, so a binding built from the entry could not match
	// its register; it keeps HEAD's handling. The experimental slot-only class is chapter P's.
	if sess == nil || sess.asm == nil || ack == nil || !ack.slot.slotOK() || ack.slot.name.gen != sess.gen ||
		ack.slot.name.idHash != sess.idHash || ack.regNonce == _EMPTY_ || ack.ev.lin == _EMPTY_ ||
		ack.slot.nonce != ack.regNonce || mqttDurIsSlotOnlyClass(ack.jsDur) || ack.kind == mqttPendingRetained {
		return nil
	}
	return &mqttAllocationAckBinding{
		role: mqttAckRoleAckRoute,
		sess: sess, epoch: sess.asm.sessStreamEpoch, name: ack.slot.name, nonce: ack.regNonce, ev: ack.ev,
		dur: ack.slot.dur, seq: ack.slot.sseq, qos: ack.qos, acq: ack.slot.acq,
	}
}

// matchesRecord compares the allocation, not a CAS sequence or current custody.
// An ACK successor may have acquired PUBREL output; the unanswered origin additionally
// requires the captured acquisition, at the operation entrance.
func (b *mqttAllocationAckBinding) matchesRecord(rec *mqttInflightRecord) bool {
	return b != nil && rec != nil && mqttInflightRecordMatchesSubject(rec, b.name.subject) &&
		rec.PI == b.name.pi && rec.Nonce == b.nonce && rec.Lin == b.ev.lin &&
		rec.Dur == b.dur && rec.Seq == b.seq && rec.QoS == b.qos && rec.Payload == b.payload
}

func (b *mqttAllocationAckBinding) belongsTo(sess *mqttSession) bool {
	return b != nil && b.sess == sess && sess.asm != nil &&
		b.epoch == sess.asm.sessStreamEpoch
}

// installAbandonedAckLocked never overwrites another allocation. No scheduling map, identifier
// allocation, charge or output permission is created by this operation.
//
// THE ROLES' INSTALL RULES (F-088 WP2):
//   - a HandBack replaces an AckRoute of the SAME allocation (the abandonment upgrades the
//     routing binding it found; reason REPLACED); nothing else is ever overwritten;
//   - an AckRoute never replaces anything, and is REFUSED for a lineage the custody ledger has
//     already seen end (the ended-lineage veto, N1): binding an ended exchange would route a
//     later identifier reuse's acknowledgement to an obligation that no longer exists.
func (sess *mqttSession) installAbandonedAckLocked(b *mqttAllocationAckBinding) bool {
	if !b.belongsTo(sess) || b.name.gen != sess.gen {
		return false
	}
	if b.role == mqttAckRoleAckRoute && sess.custody.lineageEnded(b.name.gen, b.ev.lin) {
		return false
	}
	if cur := sess.abandonedAcks[b.index()]; cur != nil {
		if *cur == *b {
			return true
		}
		if !(b.handBack() && cur.role == mqttAckRoleAckRoute && cur.sameAllocation(b)) {
			return false
		}
		// REPLACED: an abandonment's HandBack over the same allocation's AckRoute.
	}
	if sess.abandonedAcks == nil {
		sess.abandonedAcks = make(map[mqttAbandonedAckIndex]*mqttAllocationAckBinding)
	}
	sess.abandonedAcks[b.index()] = b
	return true
}

// abandonedAckOfAllocationLocked is the installed binding of ref's exact allocation — its subject,
// nonce and source delivery — or nil. It never matches another allocation's binding on a reused
// identifier or source. Lock held.
func (sess *mqttSession) abandonedAckOfAllocationLocked(ref mqttInflightSlotRef) *mqttAllocationAckBinding {
	b := sess.abandonedAcks[mqttAbandonedAckIndex{gen: ref.name.gen, pi: ref.name.pi}]
	if b == nil || b.name != ref.name || b.nonce != ref.nonce || b.dur != ref.dur || b.seq != ref.sseq {
		return nil
	}
	return b
}

// mqttBindingReleaseProof is what a caller proves when it removes a binding.
type mqttBindingReleaseProof uint8

const (
	// mqttBindingReleaseEnded: the exact exchange positively ended. Enough for an ordinary
	// HandBack and for every AckRoute, whose only role is ACK routing.
	mqttBindingReleaseEnded mqttBindingReleaseProof = iota + 1
	// mqttBindingReleaseEndedAndDeleted: the exchange ended AND the allocation's register was
	// positively established gone (a confirmed identity-bound delete, or an absence read).
	mqttBindingReleaseEndedAndDeleted
	// mqttBindingReleaseDiscard: the generation was discarded by authority.
	mqttBindingReleaseDiscard
	// mqttBindingReleaseHandoff: recovery restored an ACTING entry for this same allocation, which
	// now owns its acknowledgement (F-088 WP2, AckRoute only).
	mqttBindingReleaseHandoff
)

// removeAbandonedAckBindingLocked is THE removal boundary, dispatched on the role.
//
// A SHARED HandBack is also the binding that keeps the positive source-ACK gate closed for its
// source (outbound_source_ack_gate.go), so it is removed only when the exchange ended AND its
// register is proven deleted, or on discard: an ending alone would reopen the gate while the
// register — and the identifier it holds — still stand, and a re-offer would then be acknowledged
// away. An ordinary HandBack keeps its removal on the ending alone. An AckRoute holds no gate:
// its ending suffices whatever its class, and a recovery HANDOFF to an acting entry of the same
// allocation removes it.
//
// Lock held on entry.
func (sess *mqttSession) removeAbandonedAckBindingLocked(b *mqttAllocationAckBinding, proof mqttBindingReleaseProof) bool {
	if b == nil {
		return false
	}
	switch proof {
	case mqttBindingReleaseEndedAndDeleted, mqttBindingReleaseDiscard:
	case mqttBindingReleaseEnded:
		if b.handBack() && mqttIsSharedDurable(b.dur) {
			return false
		}
	case mqttBindingReleaseHandoff:
		if b.role != mqttAckRoleAckRoute {
			return false
		}
	default:
		return false
	}
	return sess.removeAbandonedAckLocked(b)
}

// removeAbandonedAckLocked is an exact compare-delete; a delayed ending cannot discharge
// a replacement binding even if its identifier or source was reused. Reached only through the
// removal boundary above.
func (sess *mqttSession) removeAbandonedAckLocked(b *mqttAllocationAckBinding) bool {
	if b == nil {
		return false
	}
	if cur := sess.abandonedAcks[b.index()]; cur != nil && *cur == *b {
		delete(sess.abandonedAcks, b.index())
		return true
	}
	return false
}

// resolveAbandonedAckLocked captures packet meaning together with the immutable identity, for
// BOTH roles: the handler's no-entry case routes a HandBack's and an AckRoute's acknowledgement
// alike. The caller has already checked the connection. A live scheduling occupant is never
// overridden by this index.
func (sess *mqttSession) resolveAbandonedAckLocked(pi uint16,
	arrival mqttAckArrival) (*mqttAllocationAckBinding, mqttExchangePhase) {

	b := sess.abandonedAcks[mqttAbandonedAckIndex{gen: sess.gen, pi: pi}]
	if !b.belongsTo(sess) || sess.pendingPublish[pi] != nil {
		return nil, 0
	}
	if rel := sess.pendingPubRel[pi]; rel != nil &&
		(rel.regNonce != b.nonce || rel.ev.lin != b.ev.lin) {
		return nil, 0
	}
	phase, err := mqttPhaseForAck(arrival, b.qos)
	if err != nil {
		return nil, 0
	}
	return b, phase
}

// observeAbandonedAckEndLocked releases only an ended exchange's exact binding, by its role's
// matcher: a HandBack by the abandonment fact (which identifies the original acquisition even after
// PUBREL acquisitions), an AckRoute by lineage.
func (sess *mqttSession) observeAbandonedAckEndLocked(x *mqttOutboundExchange) {
	if x == nil || !mqttExchangePhaseEnded(x.Phase) {
		return
	}
	b := sess.abandonedAcks[mqttAbandonedAckIndex{gen: x.Gen, pi: x.PI}]
	if b.endedBy(sess, x) {
		// An ending alone: a shared HandBack is retained until its register is proven deleted.
		sess.removeAbandonedAckBindingLocked(b, mqttBindingReleaseEnded)
	}
}

// endedBy reports whether x is the positive ending of exactly b's exchange. An absent slot
// cannot end an unanswered exchange.
//   - HandBack: the immutable abandonment fact must identify this exact binding.
//   - AckRoute: the exchange of this lineage, identifier, QoS, client, generation and epoch, at an
//     ended phase.
func (b *mqttAllocationAckBinding) endedBy(sess *mqttSession, x *mqttOutboundExchange) bool {
	if b == nil || x == nil || !mqttExchangePhaseEnded(x.Phase) || b.sess != sess || x.ID != sess.id ||
		x.Gen != b.name.gen || x.PI != b.name.pi || x.Epoch != b.epoch || x.Nonce != b.ev.lin || x.QoS != b.qos {
		return false
	}
	switch b.role {
	case mqttAckRoleHandBack:
		return x.Abandoned != nil && x.Abandoned.Alloc == b.nonce && x.Abandoned.Acq == b.acq &&
			x.Abandoned.Dur == b.dur && x.Abandoned.Seq == b.seq && x.Abandoned.Payload == b.payload
	case mqttAckRoleAckRoute:
		return true
	}
	return false
}

// finishAbandonedAckResolution consumes only positively ended/displaced bindings.
// Confirmed intent still hands ACK routing to the PUBREL lifecycle; unreadability,
// failed attempts and epoch mismatch retain the captured binding.
func (sess *mqttSession) finishAbandonedAckResolution(ack *mqttPending, out mqttPubRelIntentOutcome) {
	if ack == nil || (out != mqttPubRelIntentEnded && out != mqttPubRelIntentOther) {
		return
	}
	sess.mu.Lock()
	sess.removeAbandonedAckBindingLocked(ack.slot.abandonedAck, mqttBindingReleaseEnded)
	sess.mu.Unlock()
}

// bindAbandonedAckOrigin also covers a live PUBREC whose scheduling entry was captured
// before abandonment won the durable race. Neither the lineage nor QoS is inferred from
// the occupant; both must agree with that capture before a binding is installed.
func (sess *mqttSession) bindAbandonedAckOrigin(ack *mqttPending, rec *mqttInflightRecord) bool {
	if ack == nil || !ack.slot.matchesRecord(rec) || rec.Lin != ack.ev.lin || rec.QoS != ack.qos {
		return false
	}
	if b := ack.slot.abandonedAck; b != nil && b.handBack() {
		return b.belongsTo(sess) && rec.Acq == b.acq
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.asm == nil {
		return false
	}
	b := newMQTTAbandonedAckBinding(sess, sess.asm.sessStreamEpoch, ack.slot.name.subject, rec)
	if !sess.installAbandonedAckLocked(b) {
		return false
	}
	ack.slot.abandonedAck = b
	ack.slot.acq = b.acq
	return true
}
