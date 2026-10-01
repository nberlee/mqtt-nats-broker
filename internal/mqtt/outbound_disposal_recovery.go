package mqtt

import "strings"

// A C3 instance staged only by synchronous resume. Its existing worker may
// finish a later MQTT ending, but cannot submit a denial or run recovery. Its
// indexes retain every disposal restriction until the CONNECT record transaction
// has classified the save and the resume has passed its remaining refusal exits.
type mqttDisposalRecoveryHold struct {
	previous *mqttDisposal
	c        *client
	walk     uint64
	seq      uint64
	entry    *mqttPending
	drain    *mqttDrain
	floor    *drainSeq // only a claim this apply added; definite failure undoes it
	deny     bool
	saved    bool
}

func mqttDisposalNeverLandedPair(w *mqttDisposal, r *mqttInflightRecord,
	x *mqttOutboundExchange, readOK bool) bool {
	if !readOK {
		return false // Unreadability is not a non-landing certificate.
	}
	return w.matchesRecord(r) && r.State == mqttInflightPublish &&
		r.ActionToken == _EMPTY_ && x != nil && x.Abandoned == nil &&
		x.Phase == mqttExchangePublish && w.pairMatches(r, x) &&
		mqttValidateOutboundExchange(x, w.sess.id, w.key.epoch, w.key.gen, w.key.lin) == nil &&
		x.PI == w.key.pi && x.QoS == w.key.qos
}

// Called after classification, before apply, only for the final ordinary reserve
// population. Lease the old instance across handle settlement, fresh pair reads
// and apply: no timer can submit between the certificate and the replacement.
func (sess *mqttSession) prepareDisposalRecovery(c *client, rv mqttRecoveredInflightReg, walk uint64) (next *mqttDisposal, needed bool) {
	if rv.rec == nil || !mqttOrdinarySourceRegister(rv.rec) ||
		rv.rec.State != mqttInflightPublish || rv.foreign || !rv.send.decided || rv.send.opaque {
		return nil, needed
	}
	ent, ok := c.outboundEntitlement()
	sess.mu.Lock()
	key := mqttDisposalKey{sess.asm.sessStreamEpoch, sess.gen,
		sess.inflightSubjectLocked(rv.rec.PI), rv.rec.Nonce, rv.rec.Lin,
		rv.rec.Dur, rv.rec.Seq, rv.rec.PI, rv.rec.QoS}
	old := sess.disposals[key]
	needed = old != nil && (old.recovery != nil || (ok && old.evidence.Tenure != ent.Tenure))
	if !needed || !ok || ent.Gen != sess.gen || sess.c != c || old.busy || old.landed {
		sess.mu.Unlock()
		return nil, needed
	}
	old.busy = true
	d := sess.drainByDur[key.dur]
	sess.mu.Unlock()
	keep := false
	defer func() {
		if !keep {
			sess.mu.Lock()
			old.busy = false
			sess.mu.Unlock()
		}
	}()
	if !old.settleHandles() {
		return nil, needed
	}
	r, msg, rerr := sess.asm.loadInflightRecord(key.subject)
	x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, key.epoch, key.gen, key.lin)
	if !mqttDisposalNeverLandedPair(old, r, x,
		rerr == nil && xerr == nil && msg != nil && msg.Sequence == rv.seq) {
		return nil, needed
	}
	h := &mqttDisposalRecoveryHold{previous: old, c: c, walk: walk, seq: rv.seq, drain: d}
	next = &mqttDisposal{sess: sess, srv: old.srv, key: key, recovery: h}
	if d == nil {
		// The ordinary delivery guard's fresh decision, from B's snapshot and
		// the exact stored payload. Neither A's reason nor A's tenure is copied.
		source, err := sess.jsa.loadMsg(mqttStreamName, key.seq)
		if err != nil || source == nil || !strings.HasPrefix(source.Subject, mqttStreamSubjectPrefix) {
			return nil, needed
		}
		// THE QUEUE IS THE DELIVERY'S (7a). The authorization decision skips queue-scoped denies
		// for an empty queue, so a shared source must be judged under its group's queue, derived
		// from the validated durable; an unparseable shared durable decides nothing.
		queue, qok := mqttDeliveryQueueOf(key.dur)
		if !qok {
			return nil, needed
		}
		switch c.checkDenySubVerdict(strings.TrimPrefix(source.Subject, mqttStreamSubjectPrefix), queue) {
		case mqttSubDeliverAllow:
		case mqttSubDeliverDenied:
			h.deny = true
			next.evidence = mqttDenyEvidence{Gen: ent.Gen, Tenure: ent.Tenure,
				Subject: key.subject, Nonce: key.nonce, Lin: key.lin, PI: key.pi,
				Dur: key.dur, Seq: key.seq, Reason: mqttAbandonReasonACLDeny, Reply: r.SourceAckReply}
			if !next.evidence.complete() {
				return nil, needed
			}
			next.reply = r.SourceAckReply
		default:
			return nil, needed
		}
	}
	keep = true
	return next, needed
}

func (sess *mqttSession) releaseDisposalRecoveryLeases(candidates []*mqttDisposal) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, w := range candidates {
		if old := w.recovery.previous; old != nil {
			old.busy = false
			w.recovery.previous = nil
		}
	}
}

// The existing recovery apply has already installed the exact ordinary owner.
// Add its restrictive C3 reservation before removing any old restriction.
func (sess *mqttSession) applyDisposalRecoveryLocked(w *mqttDisposal) bool {
	h := w.recovery
	p := sess.pendingPublish[w.key.pi]
	if h == nil || sess.c != h.c || h.walk != sess.inflightWalks ||
		!sess.applyProvenanceCompleteLocked() || sess.piSpaceIncomplete ||
		p == nil || p.regNonce != w.key.nonce || p.ev.lin != w.key.lin ||
		p.jsDur != w.key.dur || p.sseq != w.key.seq || p.qos != w.key.qos ||
		p.regSeq != h.seq || p.regForeign || p.markerOpaque || p.regPending || p.regCancelled ||
		p.sendNonce != _EMPTY_ || sess.sendTransmissionOpenLocked(w.key.pi) ||
		sess.pendingPubRel[w.key.pi] != nil || sess.cpending[w.key.dur][w.key.seq] != w.key.pi ||
		sess.drainByDur[w.key.dur] != h.drain {
		return false
	}
	h.entry = p
	return sess.installDisposalInstanceLocked(w) == w
}

// Only enrolFloorClaimsLocked receives this explicit, frame-local capability.
// Ordinary callbacks and other claim callers never receive an exemption.
func (sess *mqttSession) disposalRecoveryFloorLocked(pi uint16, candidates []*mqttDisposal) bool {
	for _, w := range candidates {
		h := w.recovery
		if h != nil && !h.deny && h.drain != nil && w.key.pi == pi &&
			sess.disposalCurrentLocked(w) && len(sess.disposalsByPI[pi]) == 1 &&
			h.c == sess.c && h.walk == sess.inflightApplyCompletedWalk &&
			h.walk == sess.inflightWalks && h.entry == sess.pendingPublish[pi] {
			return true
		}
	}
	return false
}

func (sess *mqttSession) disposalRecoveryPauseLocked(stage string, old, next *mqttDisposal) {
	if sess.testDisposalRecovery != nil {
		sess.testDisposalRecovery(stage, old, next)
	}
}

// Uses the already-open CONNECT transaction. No callback or timer calls this.
// Ambiguity belongs to persistRecord's existing unresolved-save machinery;
// even its later positive readback cannot activate a refused resume.
func (sess *mqttSession) saveDisposalRecovery(c *client, tok recordTx) error {
	sess.mu.Lock()
	var holds []*mqttDisposal
	for _, w := range sess.disposals {
		if h := w.recovery; h != nil && h.c == c && h.walk == sess.inflightWalks &&
			sess.applyProvenanceCompleteLocked() && !sess.piSpaceIncomplete {
			holds = append(holds, w)
			sess.disposalRecoveryPauseLocked("before-persistence", nil, w)
		}
	}
	sess.mu.Unlock()
	if len(holds) == 0 {
		return nil
	}
	outcome, err := sess.saveClassified(tok)
	sess.mu.Lock()
	for _, w := range holds {
		if !sess.disposalCurrentLocked(w) {
			continue
		}
		h := w.recovery
		if outcome == saveDefinitelyFailed && h.floor != nil && h.drain.seqs[w.key.seq] == h.floor {
			delete(h.drain.seqs, w.key.seq)
			h.floor = nil
		}
		h.saved = outcome == saveLanded
	}
	sess.mu.Unlock()
	if outcome != saveLanded {
		if err != nil {
			return err
		}
		return errMQTTInvalidSession
	}
	return nil
}

// Called only past CONNECT's last refusal and before opening delivery. A fresh
// denial becomes normal C3 work; allow/drain retain their exact recovery entry
// and can now use normal authorized output/drain admission.
func (sess *mqttSession) commitDisposalRecovery(c *client) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, w := range sess.disposals {
		h := w.recovery
		if h == nil || !h.saved || h.c != c || sess.c != c ||
			h.walk != sess.inflightWalks || !sess.applyProvenanceCompleteLocked() ||
			sess.piSpaceIncomplete || h.entry != sess.pendingPublish[w.key.pi] {
			continue
		}
		if h.deny {
			w.recovery = nil
			sess.armDisposalLocked(w)
		} else {
			sess.removeDisposalLocked(w)
		}
	}
}

// mqttDeliveryQueueOf is the queue group a delivery on durable dur was selected under, for an
// authorization decision re-taken away from the delivery callback: a shared group durable's queue,
// derived from the validated durable, or none for any other source. An unparseable shared durable
// decides nothing (ok false), because judging it under no queue would skip every queue-scoped deny.
func mqttDeliveryQueueOf(dur string) (queue string, ok bool) {
	if !mqttIsSharedDurable(dur) {
		return _EMPTY_, true
	}
	return mqttSharedQueueFromGroupDurable(dur)
}
