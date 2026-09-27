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

// mqttAbandonedAckBinding is an immutable allocation identity, not a scheduling entry.
// The source reply remains in the durable abandonment fact for C4's settlement owner.
type mqttAbandonedAckBinding struct {
	sess  *mqttSession
	epoch string
	name  mqttInflightName
	nonce string
	lin   string
	dur   string
	seq   uint64
	qos   byte
	acq   uint64
}

type mqttAbandonedAckIndex struct {
	gen string
	pi  uint16
}

func (b *mqttAbandonedAckBinding) index() mqttAbandonedAckIndex {
	return mqttAbandonedAckIndex{gen: b.name.gen, pi: b.name.pi}
}

// newMQTTAbandonedAckBinding constructs the ACK-only identity from a validated abandoned
// register. C5 can use the same constructor after reconstructing and pairing the durable
// exchange; constructing this value alone neither installs it nor authorizes output.
// Session lock held.
func newMQTTAbandonedAckBinding(sess *mqttSession, epoch, subject string,
	rec *mqttInflightRecord) *mqttAbandonedAckBinding {

	if sess == nil || epoch == _EMPTY_ || rec == nil ||
		rec.State != mqttInflightDeliveryAbandoned || mqttValidateInflightShape(rec) != nil {
		return nil
	}
	ref := mqttSlotRefFromRecord(subject, 0, rec)
	if ref == nil || ref.name.gen != sess.gen || ref.name.idHash != sess.idHash ||
		!mqttInflightRecordMatchesSubject(rec, subject) {
		return nil
	}
	return &mqttAbandonedAckBinding{
		sess: sess, epoch: epoch, name: ref.name, nonce: rec.Nonce, lin: rec.Lin,
		dur: rec.Dur, seq: rec.Seq, qos: rec.QoS, acq: rec.Acq,
	}
}

// matchesRecord compares the allocation, not a CAS sequence or current custody.
// An ACK successor may have acquired PUBREL output; the unanswered origin additionally
// requires the captured acquisition, at the operation entrance.
func (b *mqttAbandonedAckBinding) matchesRecord(rec *mqttInflightRecord) bool {
	return b != nil && rec != nil && mqttInflightRecordMatchesSubject(rec, b.name.subject) &&
		rec.PI == b.name.pi && rec.Nonce == b.nonce && rec.Lin == b.lin &&
		rec.Dur == b.dur && rec.Seq == b.seq && rec.QoS == b.qos
}

func (b *mqttAbandonedAckBinding) belongsTo(sess *mqttSession) bool {
	return b != nil && b.sess == sess && sess.asm != nil &&
		b.epoch == sess.asm.sessStreamEpoch
}

func (b *mqttAbandonedAckBinding) pending() *mqttPending {
	return &mqttPending{
		jsDur: b.dur, sseq: b.seq, regNonce: b.nonce, lin: b.lin, qos: b.qos, custody: true,
		slot: mqttInflightSlotRef{
			name: b.name, nonce: b.nonce, dur: b.dur, sseq: b.seq, acq: b.acq,
			state: mqttInflightDeliveryAbandoned, abandonedAck: b,
		},
	}
}

// installAbandonedAckLocked never overwrites another allocation. No scheduling map,
// identifier allocation, charge or output permission is created by this operation.
func (sess *mqttSession) installAbandonedAckLocked(b *mqttAbandonedAckBinding) bool {
	if !b.belongsTo(sess) || b.name.gen != sess.gen {
		return false
	}
	if cur := sess.abandonedAcks[b.index()]; cur != nil {
		return *cur == *b
	}
	if sess.abandonedAcks == nil {
		sess.abandonedAcks = make(map[mqttAbandonedAckIndex]*mqttAbandonedAckBinding)
	}
	sess.abandonedAcks[b.index()] = b
	return true
}

// removeAbandonedAckLocked is an exact compare-delete; a delayed ending cannot discharge
// a replacement binding even if its identifier or source was reused.
func (sess *mqttSession) removeAbandonedAckLocked(b *mqttAbandonedAckBinding) bool {
	if b == nil {
		return false
	}
	if cur := sess.abandonedAcks[b.index()]; cur != nil && *cur == *b {
		delete(sess.abandonedAcks, b.index())
		return true
	}
	return false
}

// resolveAbandonedAckLocked captures packet meaning together with the immutable identity.
// The caller has already checked the connection. A live scheduling occupant is never
// overridden by this index.
func (sess *mqttSession) resolveAbandonedAckLocked(pi uint16,
	arrival mqttAckArrival) (*mqttAbandonedAckBinding, mqttExchangePhase) {

	b := sess.abandonedAcks[mqttAbandonedAckIndex{gen: sess.gen, pi: pi}]
	if !b.belongsTo(sess) || sess.pendingPublish[pi] != nil {
		return nil, 0
	}
	if rel := sess.pendingPubRel[pi]; rel != nil &&
		(rel.regNonce != b.nonce || rel.lin != b.lin) {
		return nil, 0
	}
	phase, err := mqttPhaseForAck(arrival, b.qos)
	if err != nil {
		return nil, 0
	}
	return b, phase
}

// observeAbandonedAckEndLocked releases only an ended exchange's exact binding. The
// historical fact identifies the original acquisition even after PUBREL acquisitions.
func (sess *mqttSession) observeAbandonedAckEndLocked(x *mqttOutboundExchange) {
	if x == nil || x.Abandoned == nil || !mqttExchangePhaseEnded(x.Phase) {
		return
	}
	b := sess.abandonedAcks[mqttAbandonedAckIndex{gen: x.Gen, pi: x.PI}]
	if b.endedBy(sess, x) {
		sess.removeAbandonedAckLocked(b)
	}
}

// An absent slot cannot end an unanswered abandoned exchange. The immutable
// abandonment fact must identify this exact binding in a positively ended exchange.
func (b *mqttAbandonedAckBinding) endedBy(sess *mqttSession, x *mqttOutboundExchange) bool {
	return b != nil && x != nil && x.Abandoned != nil && mqttExchangePhaseEnded(x.Phase) &&
		b.sess == sess && x.ID == sess.id && x.Gen == b.name.gen && x.PI == b.name.pi &&
		x.Epoch == b.epoch && x.Nonce == b.lin && x.QoS == b.qos &&
		x.Abandoned.Alloc == b.nonce && x.Abandoned.Acq == b.acq &&
		x.Abandoned.Dur == b.dur && x.Abandoned.Seq == b.seq
}

// finishAbandonedAckResolution consumes only positively ended/displaced bindings.
// Confirmed intent still hands ACK routing to the PUBREL lifecycle; unreadability,
// failed attempts and epoch mismatch retain the captured binding.
func (sess *mqttSession) finishAbandonedAckResolution(ack *mqttPending, out mqttPubRelIntentOutcome) {
	if ack == nil || (out != mqttPubRelIntentEnded && out != mqttPubRelIntentOther) {
		return
	}
	sess.mu.Lock()
	sess.removeAbandonedAckLocked(ack.slot.abandonedAck)
	sess.mu.Unlock()
}

// bindAbandonedAckOrigin also covers a live PUBREC whose scheduling entry was captured
// before abandonment won the durable race. Neither the lineage nor QoS is inferred from
// the occupant; both must agree with that capture before a binding is installed.
func (sess *mqttSession) bindAbandonedAckOrigin(ack *mqttPending, rec *mqttInflightRecord) bool {
	if ack == nil || !ack.slot.matchesRecord(rec) || rec.Lin != ack.lin || rec.QoS != ack.qos {
		return false
	}
	if b := ack.slot.abandonedAck; b != nil {
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
