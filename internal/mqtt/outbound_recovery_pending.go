package mqtt

// These reservations are scoped by the session's generation and PI map. Unlike
// a resident delivery or a foreign contest, they have no identity of their own.

// A delivered PUBREL names its transport in jsDur/sseq and its original source
// in slot. A recovery placeholder may instead carry the source directly.
func mqttRecoveryPendingMatches(cur *mqttPending, r *mqttInflightRecord, pubrel bool) bool {
	if mqttRefuseSourcelessRecordAtOrdinarySink(r) {
		return false
	}
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
		if (cur.ev.lin != _EMPTY_ && cur.ev.lin != r.Lin) || (cur.qos != 0 && cur.qos != r.QoS) {
			return false
		}
		if cur.slot.slotOK() {
			return cur.slot.matchesRecord(r)
		}
		return cur.jsDur == r.Dur && cur.sseq == r.Seq
	}
	return cur.ev.lin == r.Lin && cur.qos == r.QoS && cur.jsDur == r.Dur && cur.sseq == r.Seq
}

// bindRecoveryOpaqueLocked binds a resident opaque reservation to the register it stands over,
// once that register is read, by replacing it (mqttNewBoundOpaque). The identity match is the
// eligibility decision, so the captured entry is the expectation.
//
// Lock held on entry.
func (sess *mqttSession) bindRecoveryOpaqueLocked(pi uint16, seq uint64, r *mqttInflightRecord) {
	cur := sess.pendingPublish[pi]
	if cur == nil || !cur.regRecoveryOpaque || !mqttRecoveryPendingMatches(cur, r, false) {
		return
	}
	sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectExactly(cur), mqttNewBoundOpaque(cur, sess.gen, seq, r))
}

// reactivateDemotedLocked returns a captured demoted entry to the kind its validated record
// licenses, by replacing exactly that entry; a capture that is no longer resident, or whose
// record no longer names its obligation, is left alone.
//
// Lock held on entry.
func (sess *mqttSession) reactivateDemotedLocked(which mqttPendingMapID, pi uint16, captured *mqttPending, r *mqttInflightRecord) bool {
	if captured == nil || !captured.regRecoveryDemoted {
		return false
	}
	next := mqttReactivate(captured, sess.gen, r)
	if next == nil {
		return false
	}
	return sess.setPendingLocked(which, pi, mqttExpectExactly(captured), next)
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
	for _, dur := range sess.cpending.durables() {
		for seq, pi := range sess.cpending.entries(dur) {
			if pi == rr.pi {
				sess.cpending.deleteSeq(dur, seq)
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
	sess.cpending.deleteIf(p.jsDur, p.sseq, ref.name.pi)
	delete(sess.pendingPublish, ref.name.pi)
}
