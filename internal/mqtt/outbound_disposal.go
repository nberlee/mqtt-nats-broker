package mqtt

import "time"

// The key names the allocation, not its current entry, CAS sequence or serving tenure.
type mqttDisposalKey struct {
	epoch, gen, subject, nonce, lin, dur string
	seq                                  uint64
	pi                                   uint16
	qos                                  byte
}

type mqttDisposalStage uint8

const (
	mqttDisposalDecision mqttDisposalStage = iota
	mqttDisposalSettling
	mqttDisposalSource
	mqttDisposalRecovery
)

type mqttDisposal struct {
	sess                                        *mqttSession
	srv                                         *Server
	key                                         mqttDisposalKey
	evidence                                    mqttDenyEvidence // never refreshed from a successor connection
	reply                                       string           // newest validated offer, separate from the immutable evidence
	stage                                       mqttDisposalStage
	recovery                                    *mqttDisposalRecoveryHold
	handles                                     []*mqttTransitionHandle
	busy, scheduled, submitted, landed, settled bool
}

// All state, including the two indexes, is protected by sess.mu. The server
// registry always contains every installed owner, even while a timer is admitted.
// Lock order is sess.mu -> owedDisposalsMu; shutdown snapshots then drops its lock.
func (sess *mqttSession) disposalCurrentLocked(w *mqttDisposal) bool {
	return w != nil && w.sess == sess && sess.asm != nil &&
		sess.gen == w.key.gen && sess.asm.sessStreamEpoch == w.key.epoch &&
		sess.disposals[w.key] == w
}

func (sess *mqttSession) disposalSourceLocked(dur string, seq uint64) bool {
	return len(sess.disposalsBySource[mqttDeferredKey{dur, seq}]) != 0
}

func (sess *mqttSession) disposalPILocked(pi uint16) bool {
	return len(sess.disposalsByPI[pi]) != 0
}

func (sess *mqttSession) disposalDurableLocked(dur string) bool {
	for key, owners := range sess.disposalsBySource {
		if key.jsDur == dur && len(owners) != 0 {
			return true
		}
	}
	return false
}

// This is the only installation entrance. Validate and arm in the SAME critical
// section. An open send retains the decision; its worker waits for that section.
func (sess *mqttSession) installDisposalLocked(c *client, ev mqttDenyEvidence) *mqttDisposal {
	if !ev.complete() || sess.asm == nil || sess.jsa == nil ||
		sess.gen != ev.Gen || sess.c != c || c == nil {
		return nil
	}
	ent, ok := c.outboundEntitlement()
	cur := sess.pendingPublish[ev.PI]
	srv := sess.serverFor(c)
	if !ok || ent.Gen != ev.Gen || ent.Tenure != ev.Tenure || srv == nil || cur == nil ||
		cur.regPending || cur.regForeign || cur.markerOpaque ||
		cur.regNonce != ev.Nonce || cur.ev.lin != ev.Lin || cur.jsDur != ev.Dur ||
		cur.sseq != ev.Seq || sess.cpending[ev.Dur][ev.Seq] != ev.PI || cur.qos == 0 || mqttDurIsSlotOnlyClass(ev.Dur) ||
		ev.Subject != sess.inflightSubjectLocked(ev.PI) || sess.pendingPubRel[ev.PI] != nil {
		return nil
	}
	if st := sess.drainSeqLocked(ev.Dur, ev.Seq); st != nil && (st.claimed || st.unclaimUnconfirmed) {
		return nil
	}
	key := mqttDisposalKey{sess.asm.sessStreamEpoch, ev.Gen, ev.Subject, ev.Nonce,
		ev.Lin, ev.Dur, ev.Seq, ev.PI, cur.qos}
	if old := sess.disposals[key]; old != nil {
		old.reply = ev.Reply
	}
	return sess.installDisposalInstanceLocked(&mqttDisposal{
		sess: sess, srv: srv, key: key, evidence: ev, reply: ev.Reply,
	})
}

// Shared installation boundary for a current denial and a validated recovery fact.
// Reconciliation preserves the exact instance, evidence and unresolved handles.
func (sess *mqttSession) installDisposalInstanceLocked(w *mqttDisposal) *mqttDisposal {
	key := w.key
	var replaced *mqttDisposal
	if old := sess.disposals[key]; old != nil {
		if h := w.recovery; h != nil && h.previous == old && old.busy &&
			len(old.handles) == 0 && !old.landed {
			replaced = old
			sess.disposalRecoveryPauseLocked("before-transfer", old, w)
		} else {
			if old.evidence.Reason != w.evidence.Reason || old.evidence.Tenure != w.evidence.Tenure {
				return nil
			}
			sess.armDisposalLocked(old)
			return old
		}
	}
	if sess.disposals == nil {
		sess.disposals = make(map[mqttDisposalKey]*mqttDisposal)
		sess.disposalsBySource = make(map[mqttDeferredKey]map[*mqttDisposal]struct{})
		sess.disposalsByPI = make(map[uint16]map[*mqttDisposal]struct{})
	}
	sess.disposals[key] = w
	source := mqttDeferredKey{key.dur, key.seq}
	if sess.disposalsBySource[source] == nil {
		sess.disposalsBySource[source] = make(map[*mqttDisposal]struct{})
	}
	if sess.disposalsByPI[key.pi] == nil {
		sess.disposalsByPI[key.pi] = make(map[*mqttDisposal]struct{})
	}
	sess.disposalsBySource[source][w] = struct{}{}
	sess.disposalsByPI[key.pi][w] = struct{}{}
	// Register first: refused admission still leaves the exact owner reachable.
	w.srv.owedDisposalsMu.Lock()
	if w.srv.owedDisposals == nil {
		w.srv.owedDisposals = make(map[*mqttDisposal]struct{})
	}
	w.srv.owedDisposals[w] = struct{}{}
	w.srv.owedDisposalsMu.Unlock()
	if replaced != nil {
		// All successor indexes and the server registry exist before old
		// ownership is retired. The canonical key already names the successor.
		sess.disposalRecoveryPauseLocked("transfer", replaced, w)
		sess.unindexDisposalLocked(replaced)
		sess.disposalRecoveryPauseLocked("after-transfer", replaced, w)
	}
	sess.armDisposalLocked(w)
	return w
}

// mqttRecoveryOwnsExchangeRoute is selection only. x is an already-read census
// observation, never permission to settle a source or install an ACK binding.
func mqttRecoveryOwnsExchangeRoute(r *mqttInflightRecord, x *mqttOutboundExchange) bool {
	return mqttOrdinarySourceRegister(r) && (r.AbandonReason != _EMPTY_ || r.AbandonTenure != _EMPTY_ ||
		r.State == mqttInflightDeliveryAbandoned || mqttTerminalSourceCarrier(r) ||
		(x != nil && x.Abandoned != nil))
}

// recoverExchangeRouteLocked consumes a fresh read of a C5-owned pair. It installs
// only identity-bound work; a successful local read cannot complete either census
// or the recovery apply. No denial is reconstructed without the durable fact.
func (sess *mqttSession) recoverExchangeRouteLocked(c *client, subject string, seq uint64,
	r *mqttInflightRecord, x *mqttOutboundExchange) (recovered bool) {

	if sess.asm == nil || r == nil || seq == 0 ||
		mqttValidateOutboundExchange(x, sess.id, sess.asm.sessStreamEpoch, sess.gen, r.Lin) != nil ||
		x.PI != r.PI || x.QoS != r.QoS {
		return false
	}
	w := &mqttDisposal{
		sess: sess, srv: sess.serverFor(c),
		key: mqttDisposalKey{sess.asm.sessStreamEpoch, sess.gen, subject, r.Nonce,
			r.Lin, r.Dur, r.Seq, r.PI, r.QoS},
	}
	if f := x.Abandoned; f != nil {
		w.evidence = mqttDenyEvidence{Gen: sess.gen, Tenure: f.Tenure, Subject: subject,
			Nonce: f.Alloc, Lin: r.Lin, PI: r.PI, Dur: f.Dur, Seq: f.Seq,
			Reason: f.Reason, Reply: f.Reply}
		w.reply = f.Reply
		if f.Acq > r.Acq || (r.State == mqttInflightPublishTerminal && f.Acq != r.Acq) {
			return false
		}
	}
	if !w.matchesRecord(r) {
		return false
	}
	if !w.pairMatches(r, x) || mqttCheckSharedAcqPair(r, x) != nil {
		return false
	}
	if x.Abandoned == nil && !mqttTerminalSourceCarrier(r) {
		return true // Pre-landing: ordinary recovery, never the old in-memory denial.
	}
	pub, rel := sess.pendingPublish[r.PI], sess.pendingPubRel[r.PI]
	if !mqttRecoveryPendingMatches(pub, r, false) ||
		!mqttRecoveryPendingMatches(rel, r, true) {
		return false
	}
	// Bind only after the fresh pair AND both resident identities validate.
	// Keep the reservation non-actionable until its successor owns the work.
	sess.bindRecoveryOpaqueLocked(r.PI, seq, r)
	pub = sess.pendingPublish[r.PI] // the binding replaced it
	defer func() {
		if recovered {
			// Reactivation is installation, of the entries captured HERE and only if they are
			// still resident: the successful path below deletes the PUBLISH entry before this
			// runs, and a removed capture is never brought back.
			sess.reactivateDemotedLocked(mqttPendingPublishMap, r.PI, pub, r)
			sess.reactivateDemotedLocked(mqttPendingPubRelMap, r.PI, rel, r)
		}
	}()
	if mqttTerminalSourceCarrier(r) {
		ref := mqttSlotRefFromRecord(subject, seq, r)
		if ref == nil {
			return false
		}
		// A SHARED abandonment that ended Terminal before the restart still holds its HandBack
		// until its register is proven deleted (7a, consult 83): the binding is reconstructed from
		// the exchange's fact and carried by the terminal owner, whose deletion-proven exit is the
		// only thing that removes it.
		if f := x.Abandoned; f != nil && mqttIsSharedDurable(r.Dur) {
			b := newMQTTAbandonedAckBindingFromTerminal(sess, sess.asm.sessStreamEpoch, subject, r, x)
			if b == nil || !sess.installAbandonedAckLocked(b) {
				return false
			}
			ref.abandonedAck = b
		}
		next := sess.holdTerminalSourceLocked(*ref)
		if next == nil || (next.record.Nonce != _EMPTY_ &&
			(next.record.Lin != r.Lin || next.record.QoS != r.QoS ||
				next.record.SourceAckReply != r.SourceAckReply)) {
			return false
		}
		next.record = *r
		// An old disposal still owns its unresolved handles. Its normal step
		// performs the C4 handoff only after settling them.
		if old := sess.disposals[w.key]; old != nil {
			sess.armDisposalLocked(old)
		}
	} else {
		if w.srv == nil || sess.installDisposalInstanceLocked(w) == nil {
			return false
		}
		if r.State == mqttInflightDeliveryAbandoned {
			return sess.settleAbandonedDeliveryLocked(w.evidence, r)
		}
		sess.retireSourceLocked(r.Dur, r.Seq,
			&mqttRetiredSource{pi: r.PI, nonce: r.Nonce, durable: true})
	}
	// Successful PUBREC and terminal successors must not retain a stale PUBLISH.
	// The source owner above exists before either scheduling link disappears.
	if cur := sess.pendingPublish[r.PI]; cur != nil {
		delete(sess.pendingPublish, r.PI)
		if sess.cpending[r.Dur][r.Seq] == r.PI {
			delete(sess.cpending[r.Dur], r.Seq)
		}
	}
	return true
}

func (sess *mqttSession) armDisposalLocked(w *mqttDisposal) {
	if !sess.disposalCurrentLocked(w) || w.scheduled {
		return
	}
	select {
	case <-w.srv.quitCh:
		return // The server registry still owns final settlement.
	default:
	}
	if !w.srv.admitWillWorker() {
		return // The registry was installed before this admission attempt.
	}
	w.scheduled = true
	time.AfterFunc(mqttActionReconcileDelay, func() {
		defer w.srv.willWorkerWG.Done()
		w.step()
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if !sess.disposalCurrentLocked(w) {
			return
		}
		w.scheduled = false
		sess.armDisposalLocked(w)
	})
}

// A callback notification is observable independently of the downstream drain
// guard. Hooks run under sess.mu and must not block or reenter the session.
func (sess *mqttSession) offerDisposalLocked(dur, reply string, seq uint64) bool {
	owners := sess.disposalsBySource[mqttDeferredKey{dur, seq}]
	if len(owners) == 0 {
		return false
	}
	for w := range owners {
		if !sess.disposalCurrentLocked(w) {
			continue
		}
		if mqttSourceAckReplyBinds(reply, mqttInflightSourceStream(dur), dur, seq) {
			w.reply = reply
		}
		sess.armDisposalLocked(w)
		if sess.testDisposalNotify != nil {
			sess.testDisposalNotify(w)
		}
	}
	return true
}

// Exact compare-delete for EVERY index; no PI-only or source-only removal.
// Only positive settlement, a positive unsubmitted veto, a proven terminal
// handoff, or the permit-bearing reset may call this boundary.
func (sess *mqttSession) removeDisposalLocked(w *mqttDisposal) bool {
	if !sess.disposalCurrentLocked(w) {
		return false
	}
	sess.unindexDisposalLocked(w)
	return true
}

// Also used by the positively classified synchronous replacement, after its
// successor is installed. Never erase the successor's canonical key.
func (sess *mqttSession) unindexDisposalLocked(w *mqttDisposal) {
	if sess.disposals[w.key] == w {
		delete(sess.disposals, w.key)
	}
	source := mqttDeferredKey{w.key.dur, w.key.seq}
	delete(sess.disposalsBySource[source], w)
	if len(sess.disposalsBySource[source]) == 0 {
		delete(sess.disposalsBySource, source)
	}
	delete(sess.disposalsByPI[w.key.pi], w)
	if len(sess.disposalsByPI[w.key.pi]) == 0 {
		delete(sess.disposalsByPI, w.key.pi)
	}
	w.srv.owedDisposalsMu.Lock()
	delete(w.srv.owedDisposals, w)
	w.srv.owedDisposalsMu.Unlock()
}

// Called only by resetForReuseLocked while it holds retirement authority, before
// re-minting the generation. A tenure change and an absent entry are not discard.
func (sess *mqttSession) discardDisposalsLocked() {
	for _, w := range sess.disposals {
		sess.removeDisposalLocked(w)
	}
}

// claimStep is also the stale-timer fence. A wake cannot steal single-flight
// ownership from a synchronous first step or an already-running timer.
func (w *mqttDisposal) claimStep() bool {
	sess := w.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !sess.disposalCurrentLocked(w) || w.busy {
		return false
	}
	w.busy = true
	return true
}

func (w *mqttDisposal) step() {
	if !w.claimStep() {
		return
	}
	sess := w.sess
	defer func() {
		sess.mu.Lock()
		if sess.disposalCurrentLocked(w) {
			w.busy = false
		}
		sess.mu.Unlock()
	}()
	// A bounded step may retry contention, but never owns work on its stack alone.
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		if !w.settleHandles() {
			return
		}
		sess.mu.Lock()
		if !sess.disposalCurrentLocked(w) {
			sess.mu.Unlock()
			return
		}
		// Only an unfinished transmission blocks classification. C4's terminal
		// identifier hold must survive the handoff, not prevent it.
		open := sess.sendTransmissionOpenLocked(w.key.pi)
		if cur := sess.pendingPublish[w.key.pi]; cur != nil && cur.regNonce == w.key.nonce &&
			cur.ev.lin == w.key.lin && cur.sendNonce != _EMPTY_ {
			open = true
		}
		sess.mu.Unlock()
		if open {
			return
		}
		// Point reads classify ONLY this allocation. They cannot finish recovery,
		// clear identifier/custody incompleteness or authorize a drain floor.
		r, msg, err := sess.asm.loadInflightRecord(w.key.subject)
		if err != nil || msg == nil || !w.matchesRecord(r) {
			return
		}
		x, _, err := sess.jsa.loadOutboundExchange(sess.id, w.key.epoch, w.key.gen, w.key.lin)
		if err != nil || x == nil || x.ID != sess.id || x.Epoch != w.key.epoch ||
			x.Gen != w.key.gen || x.Nonce != w.key.lin || x.PI != w.key.pi || x.QoS != w.key.qos {
			return
		}
		if !w.pairMatches(r, x) {
			return // Includes a pair read across an ACK advance.
		}
		if mqttTerminalSourceCarrier(r) {
			ref := mqttSlotRefFromRecord(w.key.subject, msg.Sequence, r)
			if ref == nil {
				return
			}
			sess.mu.Lock()
			if sess.disposalCurrentLocked(w) {
				// A SHARED abandonment whose exchange a PUBACK ended Terminal before this owner
				// installed its binding still owes the HandBack (7a): the binding is built from the
				// Terminal exchange's fact and attached to the terminal owner BEFORE this owner
				// leaves, or both inputs of the source-ACK gate's predicate would be gone and the
				// terminal cleanup could acknowledge the group's message.
				if x.Abandoned != nil && mqttIsSharedDurable(r.Dur) {
					b := newMQTTAbandonedAckBindingFromTerminal(sess, w.key.epoch, w.key.subject, r, x)
					if b == nil || !sess.installAbandonedAckLocked(b) {
						sess.mu.Unlock()
						return
					}
					if cur := sess.abandonedAcks[b.index()]; cur != nil {
						b = cur
					}
					ref.abandonedAck = b
				}
				// C4 arms its continuation and identifier hold before D goes away.
				if next := sess.holdTerminalSourceLocked(*ref); next != nil {
					if next.ref.abandonedAck == nil {
						next.ref.abandonedAck = ref.abandonedAck
					}
					next.record = *r
					next.reply = w.reply
					if sess.testDisposalHandoff != nil {
						sess.testDisposalHandoff(w, next)
					}
					sess.removeDisposalLocked(w)
				}
			}
			sess.mu.Unlock()
			return
		}
		if x.Abandoned != nil || x.Phase == mqttExchangePubRecObserved ||
			x.Phase == mqttExchangeTerminalPendingDrain || x.Phase == mqttExchangeTerminal {
			sess.mu.Lock()
			hook := sess.testDisposalBeforeSettle
			sess.mu.Unlock()
			if hook != nil {
				hook(w)
			}
			w.settleSource(r, msg.Sequence, x)
			return
		}
		sess.mu.Lock()
		recovering := w.recovery != nil
		sess.mu.Unlock()
		if recovering {
			return // Only synchronous resume may activate this staged disposition.
		}
		auth, _, err := sess.jsa.servingAuthority(sess.id, w.key.epoch)
		if err != nil {
			return
		}
		if auth.Gen != w.evidence.Gen || auth.Tenure != w.evidence.Tenure {
			sess.mu.Lock()
			if sess.disposalCurrentLocked(w) {
				w.stage = mqttDisposalRecovery
			}
			sess.mu.Unlock()
			return // C5 alone may replace this restriction during successful recovery.
		}
		acq, err := sess.abandonCurrentOutputVeto(w.evidence)
		if err != nil {
			sess.mu.Lock()
			if sess.disposalCurrentLocked(w) && err == errMQTTAbandonCurrentOutput && !w.submitted {
				sess.removeDisposalLocked(w)
			}
			sess.mu.Unlock()
			return // Failed reads never stand in for the positive veto.
		}
		sess.mu.Lock()
		if !sess.disposalCurrentLocked(w) {
			sess.mu.Unlock()
			return
		}
		w.submitted = true
		w.stage = mqttDisposalSettling
		sess.mu.Unlock()
		out, _ := sess.submitAbandonDeniedDelivery(w.evidence, acq)
		// The instance owns the submission before the call, and receives its handle
		// before ANY classification, including an apparently successful response.
		sess.mu.Lock()
		if !sess.disposalCurrentLocked(w) {
			sess.mu.Unlock()
			return // Only permit-bearing generation discard can remove a live submission.
		}
		if out.Handle != nil {
			w.handles = append(w.handles, out.Handle)
		}
		hook := sess.testDisposalSubmitted
		sess.mu.Unlock()
		if hook != nil {
			hook(w)
		}
	}
}

func (w *mqttDisposal) settleHandles() bool {
	sess := w.sess
	sess.mu.Lock()
	if !sess.disposalCurrentLocked(w) {
		sess.mu.Unlock()
		return false
	}
	handles := append([]*mqttTransitionHandle(nil), w.handles...)
	sess.mu.Unlock()
	var unresolved []*mqttTransitionHandle
	for _, h := range handles {
		switch sess.settleCustodyHandle(h) { // resolve -> abort -> resolve
		case mqttTransitionNeverLanded, mqttTransitionLanded, mqttTransitionSuperseded:
		default:
			unresolved = append(unresolved, h)
		}
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !sess.disposalCurrentLocked(w) {
		return false
	}
	w.handles = unresolved
	return len(unresolved) == 0
}

func (w *mqttDisposal) matchesRecord(r *mqttInflightRecord) bool {
	return r != nil && mqttInflightRecordMatchesSubject(r, w.key.subject) &&
		mqttValidateInflightShape(r) == nil && r.Nonce == w.key.nonce && r.Lin == w.key.lin &&
		r.PI == w.key.pi && r.QoS == w.key.qos && r.Dur == w.key.dur && r.Seq == w.key.seq
}

func (w *mqttDisposal) pairMatches(r *mqttInflightRecord, x *mqttOutboundExchange) bool {
	if f := x.Abandoned; f != nil {
		if f.Alloc != w.key.nonce || f.Dur != w.key.dur || f.Seq != w.key.seq ||
			f.Reason != r.AbandonReason || f.Tenure != r.AbandonTenure ||
			f.Reason != w.evidence.Reason || f.Tenure != w.evidence.Tenure {
			return false
		}
		if r.State == mqttInflightDeliveryAbandoned && f.Acq != r.Acq {
			return false
		}
	} else if r.AbandonReason != _EMPTY_ || r.AbandonTenure != _EMPTY_ {
		return false
	}
	switch r.State {
	case mqttInflightPublish:
		return x.Phase == mqttExchangePublish && x.Abandoned == nil
	case mqttInflightDeliveryAbandoned:
		return x.Phase == mqttExchangeDeliveryAbandoned && x.Abandoned != nil
	case mqttInflightPublishTerminal:
		return (r.QoS == 1 && x.Phase == mqttExchangeTerminal) ||
			(r.QoS == 2 && x.Phase == mqttExchangeRejected)
	case mqttInflightPubRelIntended, mqttInflightPubRelReady:
		return r.QoS == 2 && x.Phase == mqttExchangePubRecObserved
	case mqttInflightPubRelTerminal:
		return r.QoS == 2 && (x.Phase == mqttExchangeTerminalPendingDrain || x.Phase == mqttExchangeTerminal)
	}
	return false
}

// Source work remains owned here for unanswered abandonment and successful PUBREC.
// Terminal PUBLISH cleanup is handed to C4 instead. No denial authority is needed
// to finish the source of a positively established abandonment or client ending.
func (w *mqttDisposal) settleSource(r *mqttInflightRecord, seq uint64, x *mqttOutboundExchange) {
	sess := w.sess
	sess.mu.Lock()
	if !sess.disposalCurrentLocked(w) {
		sess.mu.Unlock()
		return
	}
	if r.State == mqttInflightDeliveryAbandoned && !sess.settleAbandonedDeliveryLocked(w.evidence, r) {
		sess.mu.Unlock()
		return
	}
	w.landed = x.Abandoned != nil
	w.stage = mqttDisposalSource
	reply := w.reply
	// A SHARED abandonment's source work IS the hand-back (7a, consults 82/83): one paced NAK on
	// the offer, no positive settlement, and this owner settles. The retained abandoned-ACK
	// binding keeps the HandBack standing for every later route (outbound_source_ack_gate.go), so
	// nothing waits on the NAK — AckWait re-offers to the group if it is lost.
	if w.landed && mqttIsSharedDurable(w.key.dur) {
		if reply != _EMPTY_ {
			sess.handBackLocked(reply)
		}
		w.settled = true
		sess.removeDisposalLocked(w)
		sess.mu.Unlock()
		sess.hintCustody(w.key.gen, w.key.lin)
		return
	}
	retired := sess.srcAckOwed[mqttDeferredKey{w.key.dur, w.key.seq}]
	sess.mu.Unlock()
	sess.hintCustody(w.key.gen, w.key.lin)
	proof := sess.completeSourceFor(w.key.subject, seq, r, true,
		func() bool { return sess.disposalCurrentLocked(w) }, reply)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !sess.disposalCurrentLocked(w) || !proof.permits(w.key.subject, seq, r) {
		return
	}
	// The allocation's retained terminal slot, if any, was waiting on exactly this
	// settlement: hand it on BEFORE this owner leaves (retained_terminal_cleanup.go).
	sess.wakeRetainedTerminalCleanupLocked(w.key.nonce, w.key.dur, w.key.seq)
	key := mqttDeferredKey{w.key.dur, w.key.seq}
	if retired != nil && retired.nonce == w.key.nonce && retired.pi == w.key.pi &&
		sess.srcAckOwed[key] == retired {
		delete(sess.srcAckOwed, key)
	}
	w.settled = true
	sess.removeDisposalLocked(w)
}

// Source settlement and destructive paths are blocked until classification;
// the source-stage owner has already settled every handle and proved its pair.
func (sess *mqttSession) disposalBlocksSettlementLocked(dur string, seq uint64) bool {
	for w := range sess.disposalsBySource[mqttDeferredKey{dur, seq}] {
		if w.stage != mqttDisposalSource {
			return true
		}
	}
	return false
}

func (sess *mqttSession) disposalBlocksSettlement(dur string, seq uint64) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.disposalBlocksSettlementLocked(dur, seq)
}

func (s *Server) finishOwedDisposals() {
	s.owedDisposalsMu.Lock()
	var owed []*mqttDisposal
	for w := range s.owedDisposals {
		owed = append(owed, w)
	}
	s.owedDisposalsMu.Unlock()
	for _, w := range owed {
		w.step() // Failure retains the exact registry entry; shutdown invents no success.
	}
}

// A synchronous first step is separately admitted and joined. The installed timer
// remains the continuation even if this caller leaves or loses its connection.
func (w *mqttDisposal) runNow() mqttAbandonResult {
	if w.srv.admitWillWorker() {
		w.step()
		w.srv.willWorkerWG.Done()
	}
	w.sess.mu.Lock()
	defer w.sess.mu.Unlock()
	if w.landed && w.sess.gen == w.key.gen && w.sess.asm != nil &&
		w.sess.asm.sessStreamEpoch == w.key.epoch && (w.sess.disposalCurrentLocked(w) || w.settled) {
		return mqttAbandonDone
	}
	return mqttAbandonPending
}

func (sess *mqttSession) disposalHoldsSource(dur string, seq uint64) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.disposalSourceLocked(dur, seq)
}

func (sess *mqttSession) disposalBlocksReply(reply string) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for key := range sess.disposalsBySource {
		if mqttSourceAckReplyBinds(reply, mqttInflightSourceStream(key.jsDur), key.jsDur, key.sseq) &&
			sess.disposalBlocksSettlementLocked(key.jsDur, key.sseq) {
			return true
		}
	}
	return false
}

// A confirmed source can still have an already-queued offer. The surviving ACK
// binding suppresses it for the lifetime of the unanswered abandoned exchange;
// no extra cleanup owner or source-ACK debt is created.
func (sess *mqttSession) abandonedSourceHeldLocked(dur string, seq uint64) bool {
	for _, b := range sess.abandonedAcks {
		if b.belongsTo(sess) && b.name.gen == sess.gen && b.dur == dur && b.seq == seq {
			return true
		}
	}
	return false
}
