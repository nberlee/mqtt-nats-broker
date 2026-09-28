package mqtt

// These reservations are scoped by the session's generation and PI map. Unlike
// a resident delivery or a foreign contest, they have no identity of their own.
func mqttRecoveryOpaquePending(seq uint64) *mqttPending {
	return &mqttPending{regSeq: seq, regForeign: true, regRecoveryOpaque: true}
}

func mqttDemoteRecoveryPending(cur *mqttPending) {
	if !cur.regForeign {
		cur.regRecoveryDemoted = true
	}
	cur.regForeign = true
}

// A delivered PUBREL names its transport in jsDur/sseq and its original source
// in slot. A recovery placeholder may instead carry the source directly.
func mqttRecoveryPendingMatches(cur *mqttPending, r *mqttInflightRecord, pubrel bool) bool {
	if cur == nil {
		return true
	}
	if cur.regRecoveryOpaque && cur.regNonce == _EMPTY_ && cur.sendNonce == _EMPTY_ {
		return true
	}
	if cur.regNonce != r.Nonce {
		return false
	}
	if pubrel {
		if (cur.lin != _EMPTY_ && cur.lin != r.Lin) || (cur.qos != 0 && cur.qos != r.QoS) {
			return false
		}
		if cur.slot.slotOK() {
			return cur.slot.matchesRecord(r)
		}
		return cur.jsDur == r.Dur && cur.sseq == r.Seq
	}
	return cur.lin == r.Lin && cur.qos == r.QoS && cur.jsDur == r.Dur && cur.sseq == r.Seq
}

func mqttBindRecoveryOpaque(cur *mqttPending, seq uint64, r *mqttInflightRecord) {
	if cur == nil || !cur.regRecoveryOpaque || !mqttRecoveryPendingMatches(cur, r, false) {
		return
	}
	cur.regSeq, cur.regNonce, cur.lin = seq, r.Nonce, r.Lin
	cur.jsDur, cur.sseq, cur.qos = r.Dur, r.Seq, r.QoS
}

func mqttRecoveryReservation(p *mqttPending) bool {
	return p != nil && (p.regRecoveryOpaque || p.regRecoveryDemoted || p.markerOpaque || p.regRecoveryPlaceholder)
}

// Only a completed classifier may spend this replacement licence.
func mqttRecoveryReplaceable(p *mqttPending, r *mqttInflightRecord) bool {
	return mqttRecoveryReservation(p) && !p.regPending && !p.regCancelled &&
		p.sendNonce == _EMPTY_ && mqttRecoveryPendingMatches(p, r, false)
}

func (sess *mqttSession) recoveryPIHeldLocked(pi uint16) bool {
	for _, p := range []*mqttPending{sess.pendingPublish[pi], sess.pendingPubRel[pi]} {
		if p != nil && (p.regRecoveryOpaque || p.regRecoveryDemoted || p.markerOpaque) {
			return true
		}
	}
	return false
}

// Captured BEFORE enumeration; committed only by this successful CONNECT.
// Neither an ordinary resident nor a foreign contest belongs to this remover.
type mqttRecoveryRelease struct {
	gen, epoch     string
	walk           uint64
	pi             uint16
	pub, rel       *mqttPending
	pubSeq, relSeq uint64
}

func (sess *mqttSession) captureRecoveryReservationsLocked() []mqttRecoveryRelease {
	pis := make(map[uint16]bool)
	for pi := range sess.pendingPublish {
		pis[pi] = true
	}
	for pi := range sess.pendingPubRel {
		pis[pi] = true
	}
	for _, fc := range sess.foreignContested {
		delete(pis, fc.pi)
	}
	var out []mqttRecoveryRelease
	for pi := range pis {
		pub, rel := sess.pendingPublish[pi], sess.pendingPubRel[pi]
		if (pub != nil && !mqttRecoveryReservation(pub)) ||
			(rel != nil && !mqttRecoveryReservation(rel)) {
			continue
		}
		rr := mqttRecoveryRelease{gen: sess.gen, walk: sess.inflightWalks,
			pi: pi, pub: pub, rel: rel}
		if sess.asm != nil {
			rr.epoch = sess.asm.sessStreamEpoch
		}
		if pub != nil {
			rr.pubSeq = pub.regSeq
		}
		if rel != nil {
			rr.relSeq = rel.regSeq
		}
		out = append(out, rr)
	}
	return out
}

func (sess *mqttSession) recoveryReleaseCurrentLocked(rr *mqttRecoveryRelease) bool {
	if sess.gen != rr.gen || sess.asm == nil || sess.asm.sessStreamEpoch != rr.epoch ||
		sess.inflightWalks != rr.walk || !sess.applyProvenanceCompleteLocked() ||
		sess.piSpaceIncomplete || sess.custody.withholdsAllocation() ||
		sess.custody.holdsPI(rr.pi) || sess.disposalPILocked(rr.pi) ||
		sess.pendingPublish[rr.pi] != rr.pub || sess.pendingPubRel[rr.pi] != rr.rel {
		return false
	}
	for _, fc := range sess.foreignContested {
		if fc.pi == rr.pi {
			return false
		}
	}
	return (rr.pub == nil || (mqttRecoveryReservation(rr.pub) &&
		rr.pub.regSeq == rr.pubSeq && rr.pub.sendNonce == _EMPTY_)) &&
		(rr.rel == nil || (mqttRecoveryReservation(rr.rel) &&
			rr.rel.regSeq == rr.relSeq && rr.rel.sendNonce == _EMPTY_))
}

func (sess *mqttSession) commitRecoveryReleaseLocked(c *client, rr *mqttRecoveryRelease) {
	if sess.c != c || !sess.recoveryReleaseCurrentLocked(rr) {
		return
	}
	// Census absence is not consolidated slot release. Transfer exclusion FIRST;
	// its existing reclaim worker proves absence and owns the remaining retry.
	sess.excludeIdentifierLocked(rr.pi, sess.inflightSubjectLocked(rr.pi), _EMPTY_)
	for _, seqs := range sess.cpending {
		for seq, pi := range seqs {
			if pi == rr.pi {
				delete(seqs, seq)
			}
		}
	}
	delete(sess.pendingPublish, rr.pi)
	delete(sess.pendingPubRel, rr.pi)
}

// The caller has already installed a terminal owner or PI exclusion under sess.mu.
func (sess *mqttSession) handOffRecoveryPublishLocked(ref mqttInflightSlotRef) {
	p := sess.pendingPublish[ref.name.pi]
	if !mqttRecoveryReservation(p) || p.regNonce != ref.nonce ||
		p.jsDur != ref.dur || p.sseq != ref.sseq || p.sendNonce != _EMPTY_ ||
		p.regPending || p.regCancelled {
		return
	}
	if sess.cpending[p.jsDur][p.sseq] == ref.name.pi {
		delete(sess.cpending[p.jsDur], p.sseq)
	}
	delete(sess.pendingPublish, ref.name.pi)
}
