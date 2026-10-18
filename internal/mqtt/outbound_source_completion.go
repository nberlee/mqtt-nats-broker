package mqtt

import "time"

// registerSourceAckReply carries persisted metadata through recovered reservations
// and cancellation tombstones without making them current delivery offers.
// The caller holds sess.mu when reading a live entry.
func (p *mqttPending) registerSourceAckReply() string {
	if p.sourceAckReply != _EMPTY_ {
		return p.sourceAckReply
	}
	return p.jsAckSubject
}

// Ordinary register metadata is deliberately separate from the immutable denial fact.
func mqttOrdinarySourceRegister(r *mqttInflightRecord) bool {
	return r != nil && r.Dur != _EMPTY_ && r.Seq != 0 && !mqttDurIsSlotOnlyClass(r.Dur)
}

func mqttTerminalSourceCarrier(r *mqttInflightRecord) bool {
	return mqttOrdinarySourceRegister(r) && r.State == mqttInflightPublishTerminal
}

// A completion is a capability for ONE loaded record, including its allocation,
// lineage, acquisition, source, reply and storage version. The zero value withholds.
type mqttSourceCompletion struct {
	subject string
	seq     uint64
	record  mqttInflightRecord
}

func (p mqttSourceCompletion) permits(subject string, seq uint64, r *mqttInflightRecord) bool {
	return r != nil && p.subject == subject && subject != _EMPTY_ &&
		p.seq == seq && seq != 0 && p.record == *r
}

// completeSource never treats submission or a lost confirmation as completion.
// Lost proves non-reofferability, not a client ending; the caller must establish
// that ending before using this primitive to release a terminal register.
func (sess *mqttSession) completeSource(subject string, seq uint64, r *mqttInflightRecord,
	confirm bool, offeredReply ...string) mqttSourceCompletion {
	return sess.completeSourceFor(subject, seq, r, confirm, nil, offeredReply...)
}

// completeSourceFor is completeSource for a CONTINUATION: owner revalidates, in the gate's own
// critical section, that the continuation is still the installed owner, so a worker superseded
// between its last check and its submission acknowledges nothing (outbound_source_ack_gate.go).
// A hand-back is never a completion.
func (sess *mqttSession) completeSourceFor(subject string, seq uint64, r *mqttInflightRecord,
	confirm bool, owner mqttSourceAckOwner, offeredReply ...string) mqttSourceCompletion {

	if mqttRefuseSourcelessRecordAtOrdinarySink(r) {
		return mqttSourceCompletion{}
	}
	if sess.asm == nil || seq == 0 || !mqttInflightRecordMatchesSubject(r, subject) ||
		mqttValidateInflightShape(r) != nil || !mqttOrdinarySourceRegister(r) {
		return mqttSourceCompletion{}
	}
	if sess.disposalBlocksSettlement(r.Dur, r.Seq) {
		return mqttSourceCompletion{}
	}
	reply := r.SourceAckReply
	if len(offeredReply) != 0 && mqttSourceAckReplyBinds(offeredReply[0],
		mqttInflightSourceStream(r.Dur), r.Dur, r.Seq) {
		reply = offeredReply[0]
	}
	complete := false
	switch sess.asm.inflightSourceState(r) {
	case mqttInflightSourceComplete, mqttInflightSourceLost:
		complete = true
	case mqttInflightSourceLive:
		complete = confirm && sess.confirmInflightSourceAckFor(reply, owner) == nil
	}
	if !complete {
		return mqttSourceCompletion{}
	}
	return mqttSourceCompletion{subject: subject, seq: seq, record: *r}
}

// mqttTerminalSourceKey names ONE source-suppression owner: the slot, its allocation and the
// source it suppresses. The ACQUISITION ORDINAL is deliberately not part of it. It is not the
// source's identity, and an acknowledgement's resolution may learn a newer ordinal than the one
// its handler captured (an acquisition that landed behind a lost response): keyed by it, the
// resolution forked a second owner and the handler's first one was never removed — permanent
// suppression metadata on every delivery scan (checkpoint review WIP-1).
type mqttTerminalSourceKey struct {
	subject, nonce, dur string
	sseq                uint64
}

func mqttTerminalSourceKeyOf(ref mqttInflightSlotRef) mqttTerminalSourceKey {
	return mqttTerminalSourceKey{ref.name.subject, ref.nonce, ref.dur, ref.sseq}
}

type mqttTerminalSource struct {
	ref    mqttInflightSlotRef
	epoch  string
	busy   bool
	proof  mqttSourceCompletion
	record mqttInflightRecord
	reply  string // newest validated offer; the durable reply remains the cold fallback

	// Source-only confirmation after proven absence; never a register-delete proof.
	absentConfirmed bool
}

// Suppression has one owner per allocation/source; retries additionally carry
// the transmission and its ACK authority. A recovery wake must not consume a
// client's ACK, and an older timer must not consume a newer acquisition's work.
type mqttTerminalRetryKey struct {
	source       mqttTerminalSourceKey
	acq          uint64
	acknowledged bool
	epoch        string
}

type mqttTerminalRetry struct {
	ref       mqttInflightSlotRef
	key       mqttTerminalRetryKey
	c         *client
	srv       *Server
	scheduled bool // timer or executing attempt; sess.mu protects this and membership
	// prompt: the first attempt is owed NOW, not after mqttActionReconcileDelay. Set only by
	// a handoff from a send section (handOffAcknowledgedPublish), whose attempt would
	// otherwise have run inline. Consumed where the worker COMMITS to the attempt, never at
	// scheduling: a worker that yields to a recovery scope keeps it, so the scope's
	// re-dispatch is still prompt.
	prompt bool
}

// Install before untracking, under the same lock. Suppression is independent of
// srcAckOwed: a retirement callback cannot clear this owner's restriction.
// Creation also arms an exact continuation. It has no MQTT ACK authority: an
// unresolved live ACK still belongs to its handler/resolution cell. Once that
// owner establishes a terminal carrier, this continuation can settle its source.
func (sess *mqttSession) holdTerminalSourceLocked(ref mqttInflightSlotRef) *mqttTerminalSource {
	// NO SOURCE, NO OWNER. A slot that names no source delivery — a retained exchange — has no
	// re-offer to suppress and no completion to establish; an owner installed for it could only
	// ever ask a consumer with an empty name, and would hold its identifier for the session's life.
	if mqttRefuseSourcelessAtOrdinarySink(ref.dur, ref.sseq) {
		return nil
	}
	if sess.asm == nil || !ref.slotOK() || mqttDurIsSlotOnlyClass(ref.dur) ||
		ref.name.gen != sess.gen || ref.name.subject == _EMPTY_ || sess.serverFor(nil) == nil {
		return nil
	}
	key := mqttTerminalSourceKeyOf(ref)
	if sess.terminalSources == nil {
		sess.terminalSources = make(map[mqttTerminalSourceKey]*mqttTerminalSource)
	}
	w := sess.terminalSources[key]
	if w != nil && w.epoch != sess.asm.sessStreamEpoch {
		return nil // Another store epoch cannot adopt this source obligation.
	}
	if w == nil {
		w = &mqttTerminalSource{ref: ref, epoch: sess.asm.sessStreamEpoch}
		sess.terminalSources[key] = w
	} else if ref.acq > w.ref.acq {
		// The SAME owner adopts the newer transmission the resolution proved; suppression never
		// lapses in the handoff, and an older capture never re-forks it.
		w.ref = ref
	}
	sess.armTerminalRetryLocked(nil, w.ref, false)
	sess.holdTerminalCleanupLocked(ref.name.pi, ref.nonce)
	return w
}

func (sess *mqttSession) terminalSourceHeldLocked(dur string, seq uint64) bool {
	for key, w := range sess.terminalSources {
		if sess.asm != nil && w.epoch == sess.asm.sessStreamEpoch &&
			w.ref.name.gen == sess.gen && key.dur == dur && key.sseq == seq {
			return true
		}
	}
	return false
}

// A supported terminal pair, including the fact's projection when present, is
// required before source settlement. An unresolved phase advance supplies no proof.
func (sess *mqttSession) terminalSourcePair(ref mqttInflightSlotRef, r *mqttInflightRecord) bool {
	if !mqttTerminalSourceCarrier(r) || mqttValidateInflightShape(r) != nil {
		return false
	}
	class, x := sess.ackExchangeFor(r, ref.name.gen)
	if class != mqttAckExchange || x.QoS != r.QoS ||
		x.ID != sess.id || x.Epoch != sess.asm.sessStreamEpoch || x.Gen != ref.name.gen {
		return false
	}
	if (r.QoS == 1 && x.Phase != mqttExchangeTerminal) ||
		(r.QoS == 2 && x.Phase != mqttExchangeRejected) {
		return false
	}
	if f := x.Abandoned; f != nil {
		return f.Alloc == r.Nonce && f.Acq == r.Acq && f.Dur == r.Dur && f.Seq == r.Seq &&
			f.Reason == r.AbandonReason && f.Tenure == r.AbandonTenure
	}
	return r.AbandonReason == _EMPTY_ && r.AbandonTenure == _EMPTY_
}

// Called from the terminal continuation after protocol advancement and before
// deletion. Work and proof are published only to the exact installed instance.
func (sess *mqttSession) settleTerminalSource(subject string, seq uint64, r *mqttInflightRecord) bool {
	if mqttRefuseSourcelessRecordAtOrdinarySink(r) {
		return false
	}
	ref := mqttSlotRefFromRecord(subject, seq, r)
	if ref == nil {
		return false
	}
	sess.mu.Lock()
	w := sess.holdTerminalSourceLocked(*ref)
	if w == nil || w.busy || (w.record.Nonce != _EMPTY_ &&
		(w.record.Lin != r.Lin || w.record.QoS != r.QoS || w.record.SourceAckReply != r.SourceAckReply)) {
		sess.mu.Unlock()
		return false
	}
	if w.proof.permits(subject, seq, r) {
		sess.mu.Unlock()
		return true
	}
	w.record = *r
	w.busy = true
	retiredKey := mqttDeferredKey{r.Dur, r.Seq}
	retired := sess.srcAckOwed[retiredKey]
	reply := w.reply
	handedBack := sess.sourceHandBackHeldLocked(r.Dur, r.Seq)
	sess.mu.Unlock()

	// A HANDED-BACK shared source has no completion to establish: this member gave the message
	// back to its group (7a). Its terminal slot is settled by the exchange's durable Terminal
	// ending with the abandonment fact — the same typed proof the deletion gate accepts in place
	// of a source completion — and nothing here touches the source.
	if handedBack {
		settled := sess.handBackTerminalProof(subject, seq, r).permits(subject, seq, r)
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.terminalSources[mqttTerminalSourceKeyOf(*ref)] != w {
			return false
		}
		w.busy = false
		return settled
	}

	var proof mqttSourceCompletion
	if sess.terminalSourcePair(*ref, r) {
		key := mqttTerminalSourceKeyOf(*ref)
		proof = sess.completeSourceFor(subject, seq, r, true,
			func() bool { return sess.terminalSources[key] == w }, reply)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.terminalSources[mqttTerminalSourceKeyOf(*ref)] != w {
		return false
	}
	// Release this attempt's single-flight claim even when its proof is stale.
	w.busy = false
	if sess.gen != ref.name.gen || sess.asm.sessStreamEpoch != w.epoch ||
		w.ref.acq != ref.acq {
		return false
	}
	w.proof = proof
	if proof.permits(subject, seq, r) && retired != nil && retired.nonce == r.Nonce &&
		retired.pi == r.PI && sess.srcAckOwed[retiredKey] == retired {
		delete(sess.srcAckOwed, retiredKey)
	}
	return proof.permits(subject, seq, r)
}

// The deletion boundary consumes confirmation for this exact observation, or
// obtains its own positive source classification. It never submits an ACK.
func (sess *mqttSession) terminalSourceProof(subject string, seq uint64, r *mqttInflightRecord) mqttSourceCompletion {
	if !mqttTerminalSourceCarrier(r) {
		return mqttSourceCompletion{}
	}
	ref := mqttSlotRefFromRecord(subject, seq, r)
	if ref == nil {
		return mqttSourceCompletion{}
	}
	sess.mu.Lock()
	w := sess.terminalSources[mqttTerminalSourceKeyOf(*ref)]
	if w != nil && w.epoch == sess.asm.sessStreamEpoch && w.proof.permits(subject, seq, r) {
		proof := w.proof
		sess.mu.Unlock()
		return proof
	}
	sess.mu.Unlock()
	if !sess.terminalSourcePair(*ref, r) {
		return mqttSourceCompletion{}
	}
	return sess.completeSource(subject, seq, r, false)
}

// finishAbsentTerminal releases only the acknowledged allocation's identifier hold.
// Output/action holds and successor exclusions still reserve the identifier. Source
// suppression has its own lifetime: false keeps this attempt's retry/owed owner.
func (sess *mqttSession) finishAbsentTerminal(ref mqttInflightSlotRef, acknowledged bool) bool {
	key := mqttTerminalSourceKeyOf(ref)
	sess.mu.Lock()
	w, as := sess.terminalSources[key], sess.asm
	if as == nil || ref.name.gen != sess.gen ||
		(ref.terminalEpoch != _EMPTY_ && ref.terminalEpoch != as.sessStreamEpoch) ||
		(w != nil && (w.epoch != as.sessStreamEpoch || w.ref.acq > ref.acq)) {
		sess.mu.Unlock()
		return false
	}
	terminal := ref.state == mqttInflightPublishTerminal ||
		(w != nil && w.record.State == mqttInflightPublishTerminal)
	// An absent slot does not positively end an abandoned exchange. Preserve its
	// binding and existing resolution rules until terminal evidence is available.
	authorized := terminal || (acknowledged && ref.abandonedAck == nil)
	if !authorized {
		sess.mu.Unlock()
		return sess.finishTerminalSource(ref, false, false)
	}
	sess.releaseTerminalCleanupHoldLocked(ref.name.pi, ref.nonce)
	reply := _EMPTY_
	if w != nil && !w.busy && !w.absentConfirmed {
		reply = w.reply
		if reply == _EMPTY_ && terminal {
			reply = w.record.SourceAckReply
		}
	}
	confirm := w != nil && mqttSourceAckReplyBinds(reply, mqttInflightSourceStream(ref.dur), ref.dur, ref.sseq)
	if confirm {
		w.busy = true
	}
	sess.mu.Unlock()
	if confirm {
		state := as.inflightSourceState(&mqttInflightRecord{Dur: ref.dur, Seq: ref.sseq})
		settled := state == mqttInflightSourceComplete || state == mqttInflightSourceLost
		if state == mqttInflightSourceLive {
			settled = sess.confirmInflightSourceAckFor(reply,
				func() bool { return sess.terminalSources[key] == w }) == nil
		}
		sess.mu.Lock()
		if sess.terminalSources[key] != w {
			sess.mu.Unlock()
			return false
		}
		w.busy = false
		if sess.asm != as || sess.gen != ref.name.gen ||
			w.epoch != as.sessStreamEpoch || w.ref.acq != ref.acq {
			sess.mu.Unlock()
			return false
		}
		w.absentConfirmed = settled
		sess.mu.Unlock()
	}
	// The subject was read ABSENT: this allocation's register is positively gone.
	return sess.finishTerminalSource(ref, false, true)
}

// finishTerminalSource is the settlement-owner exit boundary.
//
// Invariant: retained restriction => active settlement owner OR positive settlement.
//
// A true result consumes this continuation. Therefore an unsettled restriction
// must either be explicitly handed back to a surviving protocol owner or return
// false, keeping the caller's retry/owed ownership. A positively observed terminal
// carrier cannot be handed back to PUBLISH: it still needs source settlement.
// Identifier exclusions are separate and are never lifted here.
//
// deleted reports that the caller POSITIVELY established this allocation's register is gone — its
// confirmed identity-bound delete, or an absence read of its subject. It is what lets a HANDED-BACK
// shared source settle: no source completion can exist for it, so the register's proven deletion
// against its Terminal exchange is the ending (7a), and only it removes the HandBack binding.
func (sess *mqttSession) finishTerminalSource(ref mqttInflightSlotRef, handedOff, deleted bool) bool {
	key := mqttTerminalSourceKeyOf(ref)
	sess.mu.Lock()
	w := sess.terminalSources[key]
	discarded := ref.name.gen != sess.gen
	if w != nil && !discarded && w.ref.acq > ref.acq {
		sess.mu.Unlock()
		return true // The newer exact continuation owns both restriction and hold.
	}
	as := sess.asm
	if w != nil && !discarded && (as == nil || w.epoch != as.sessStreamEpoch) {
		sess.mu.Unlock()
		return false
	}
	settled := w == nil || discarded || w.absentConfirmed || w.proof.seq != 0 ||
		(handedOff && w.record.State != mqttInflightPublishTerminal) ||
		(deleted && sess.sourceHandBackHeldLocked(ref.dur, ref.sseq))
	if !settled {
		sess.mu.Unlock()
		// Absence alone is not source-completion evidence.
		state := as.inflightSourceState(&mqttInflightRecord{Dur: ref.dur, Seq: ref.sseq})
		settled = state == mqttInflightSourceComplete || state == mqttInflightSourceLost
		sess.mu.Lock()
	}
	defer sess.mu.Unlock()
	if sess.terminalSources[key] != w {
		return false // Do not consume a replacement's restriction or identifier hold.
	}
	if w != nil && !discarded && (w.ref.acq > ref.acq ||
		sess.asm != as || w.epoch != as.sessStreamEpoch) {
		return false
	}
	if !settled {
		return false
	}
	delete(sess.terminalSources, key)
	// Source completion alone does not answer an abandoned exchange's MQTT ACK.
	if discarded || handedOff || ref.state == mqttInflightPublishTerminal ||
		(w != nil && w.record.State == mqttInflightPublishTerminal) {
		// The binding is the one INSTALLED for this exact allocation, never the owner's captured
		// copy: an owner captured before the disposal installed its binding carries none, and the
		// binding would outlive the register — a shared one as a HandBack that NAKs every re-offer,
		// an ordinary one as an identifier index no later allocation can install over (H7a-2/3).
		binding := sess.abandonedAckOfAllocationLocked(ref)
		proof := mqttBindingReleaseEnded
		switch {
		case discarded:
			proof = mqttBindingReleaseDiscard
		case deleted:
			proof = mqttBindingReleaseEndedAndDeleted
		}
		sess.removeAbandonedAckBindingLocked(binding, proof)
	}
	sess.releaseTerminalCleanupHoldLocked(ref.name.pi, ref.nonce)
	return true
}

func (sess *mqttSession) armTerminalRetryLocked(c *client, ref mqttInflightSlotRef,
	acknowledged bool) *mqttTerminalRetry {
	return sess.armTerminalRetryWithLocked(c, ref, acknowledged, false)
}

// armTerminalRetryPromptLocked arms the retry with its first attempt owed at once. An
// already-armed key is left exactly as it is: a duplicate arm neither re-prompts a retry
// whose attempt already failed nor starts a second worker.
func (sess *mqttSession) armTerminalRetryPromptLocked(c *client, ref mqttInflightSlotRef,
	acknowledged bool) *mqttTerminalRetry {
	return sess.armTerminalRetryWithLocked(c, ref, acknowledged, true)
}

func (sess *mqttSession) armTerminalRetryWithLocked(c *client, ref mqttInflightSlotRef,
	acknowledged, prompt bool) *mqttTerminalRetry {

	srv := sess.serverFor(c)
	if srv == nil || sess.asm == nil || ref.name.pi == 0 || ref.name.subject == _EMPTY_ {
		return nil
	}
	if ref.terminalEpoch == _EMPTY_ {
		ref.terminalEpoch = sess.asm.sessStreamEpoch
		if w := sess.terminalSources[mqttTerminalSourceKeyOf(ref)]; w != nil {
			ref.terminalEpoch = w.epoch
		}
	}
	key := mqttTerminalRetryKey{mqttTerminalSourceKeyOf(ref), ref.acq, acknowledged, ref.terminalEpoch}
	if sess.terminalRetries == nil {
		sess.terminalRetries = make(map[mqttTerminalRetryKey]*mqttTerminalRetry)
	}
	if sess.terminalRetries[key] != nil {
		return nil
	}
	w := &mqttTerminalRetry{ref: ref, key: key, c: c, srv: srv, prompt: prompt}
	sess.terminalRetries[key] = w
	sess.dispatchTerminalRetryLocked(w)
	return w
}

// Called only under sess.mu. A deferred token stays in terminalRetries; the
// outermost recovery scope owns dispatch on both successful and failed exits.
func (sess *mqttSession) dispatchTerminalRetryLocked(w *mqttTerminalRetry) {
	if sess.terminalRetries[w.key] != w || w.scheduled || sess.terminalRecoveryDepth != 0 {
		return
	}
	if !w.srv.admitWillWorker() {
		w.srv.recordOwedTerminal(sess, w.ref.name.pi, w.ref, w.key.acknowledged, false)
		delete(sess.terminalRetries, w.key) // T is reachable before R is removed.
		return
	}
	w.scheduled = true
	delay := mqttActionReconcileDelay
	if w.prompt {
		delay = 0
	}
	time.AfterFunc(delay, func() {
		defer w.srv.willWorkerWG.Done()
		sess.mu.Lock()
		if sess.terminalRetries[w.key] != w {
			sess.mu.Unlock()
			return
		}
		if sess.terminalRecoveryDepth != 0 {
			w.scheduled = false // The recovery scope now owns the same token.
			sess.mu.Unlock()
			return
		}
		w.prompt = false // Committed: every later attempt is a retry, paced as one.
		sess.mu.Unlock()
		done := sess.attemptTerminalRegisterRelease(w.c, w.ref.name.pi, w.ref, w.key.acknowledged)
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.terminalRetries[w.key] != w {
			return
		}
		w.scheduled = false
		if done {
			delete(sess.terminalRetries, w.key)
		} else {
			sess.dispatchTerminalRetryLocked(w) // Retain R throughout the retry handoff.
		}
	})
}

// Register the release before a walk can create owners. Nesting keeps retries
// deferred across reclassification and custody bookkeeping in the outer walk.
func (sess *mqttSession) deferTerminalRetries() func() {
	sess.mu.Lock()
	sess.terminalRecoveryDepth++
	sess.mu.Unlock()
	return func() {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		sess.terminalRecoveryDepth--
		if sess.terminalRecoveryDepth == 0 {
			for _, w := range sess.terminalRetries {
				sess.dispatchTerminalRetryLocked(w)
			}
		}
	}
}
