// Ambiguous session-record writes.
//
// sess.save() advances sess.seq only on a CONFIRMED response, so a write that
// LANDED with its response lost leaves the in-memory sequence stale. That is not
// self-correcting: a same-process reconnect reuses the very same session object,
// and its CONNECT accept save runs before any recovery, so the stale CAS fails and
// the reconnect is rejected — recovery is never reached.
//
// So an ambiguous failure is recorded rather than merely returned: the session is
// marked UNRESOLVED, together with the identity of the write it attempted (the CAS
// base sequence and a digest of the exact bytes). Resolving it is a READBACK, and
// the readback is a three-way classification — never "adopt whatever sequence is
// current", which would turn a stale local snapshot into a *valid* CAS and
// overwrite another broker's newer subscriptions, drains, claims or expiry state:
//
//	readSeq == baseSeq                     -> our write did not land; seq is right
//	readSeq > baseSeq && digest matches     -> it landed; adopt readSeq
//	anything else (incl. an absent record)  -> that record is AUTHORITATIVE and not
//	                                           ours: detach and evict, never write
//	the readback itself failing             -> still unresolved; write nothing,
//	                                           delete nothing, retry later
//
// While unresolved, save, clear and the record delete REFUSE AT ENTRY. That is the
// hard guarantee, and it is about teardown as much as about writes: the close path
// would otherwise issue a second stale disconnect save, or run clear() for expiry 0
// — deleting the shared durables and the record before its stale CAS could fail —
// or derive the delayed-Will takeover fence from a stale sequence.
//
// Refusing is not the same as dropping the client's MQTT obligations, though.
// Unresolved is NOT takeover: nobody else has taken responsibility for this
// session's expiry or its Will. So the close path records a DEFERRED DISCONNECT
// ACTION carrying the ORIGINAL deadlines — the disconnect time, the effective
// Session Expiry Interval, and the Will's absolute deadline plus the data it needs
// — and detaches the client without saving or clearing. On resolution the original
// disconnect time is persisted and only the REMAINING duration is armed, or the
// action fires at once if a deadline has already passed. Anything else would let a
// JetStream outage silently extend both the session lifetime and the Will Delay
// Interval.

package mqtt

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
)

// mqttPendingWrite is the identity of an attempted record write whose outcome is
// unknown: the sequence it CAS'd against, and a digest of the exact record bytes
// it tried to store. Both are needed — the sequence alone cannot tell our write
// apart from another broker's, which is the whole point of the classification.
type mqttPendingWrite struct {
	baseSeq uint64
	digest  [sha256.Size]byte
}

// mqttUncertainResolution is how a readback classified an ambiguous write.
type mqttUncertainResolution int

const (
	// mqttUncertainUnresolved: the readback failed. Nothing may be concluded, so
	// nothing may be written or deleted; the retry pass tries again.
	mqttUncertainUnresolved mqttUncertainResolution = iota
	// mqttUncertainOurs: the record is ours, whether or not the write landed.
	// sess.seq is now correct and the session may write again.
	mqttUncertainOurs
	// mqttUncertainForeign: an authoritative record exists that is not ours. Detach
	// and evict; every obligation belongs to its owner.
	mqttUncertainForeign
)

func (r mqttUncertainResolution) String() string {
	switch r {
	case mqttUncertainOurs:
		return "ours"
	case mqttUncertainForeign:
		return "foreign"
	default:
		return "unresolved"
	}
}

// mqttDeferredDisconnect is the MQTT work a close could not do because the
// session's record state was unresolved. Every deadline in it is ABSOLUTE, so
// resolution arms the remaining time rather than starting the interval again.
type mqttDeferredDisconnect struct {
	// disconnectedAt is the ORIGINAL disconnect time (Unix seconds), persisted on
	// resolution so the expiry deadline is the one the client actually earned.
	disconnectedAt int64
	// expiryInterval is the effective Session Expiry Interval at disconnect: 0
	// means the session ends now (a deferred clear()), mqttSessionNeverExpire means
	// it never expires, anything else is a finite deadline measured from
	// disconnectedAt.
	expiryInterval uint32
	// willDeadline is the absolute Unix time the Will becomes due (0 when there is
	// no Will pending). A reconnect BEFORE it cancels the Will; a reconnect after
	// it does not — the Will is still owed and is published on resolution.
	willDeadline int64
	// willSessionSeq / willSessEpoch are the delayed-Will takeover fence baseline
	// captured at the disconnect, so the enqueue is fenced against the incarnation
	// that actually disconnected rather than whatever sequence resolution finds.
	willSessionSeq uint64
	willSessEpoch  string
	// willGen is the enqueuing incarnation's generation, so a displaced-broker
	// cancel can scope itself. See sess_incarnation.go.
	willGen string
	// will / willProps / willUser are the Will itself, captured at the disconnect
	// because the client is closing and c.mqtt.cp will not be readable later. nil
	// will means there is nothing owed. Publish permission was already evaluated at
	// capture time — it is not serializable and cannot be re-checked here — so a
	// denied Will is simply never captured, matching mqttHandleWill.
	will      *mqttWill
	willProps []byte
	willUser  string
	// willDelayed distinguishes a Will that must go through the durable outbox (a
	// v5 Will Delay Interval on a surviving session) from one that is published
	// immediately.
	willDelayed bool
}

// mqttRecordDigest is the identity a readback compares against: the digest of the
// record BODY, which is what a load returns (the CAS header is a message header,
// not part of the stored data).
func mqttRecordDigest(body []byte) [sha256.Size]byte {
	return sha256.Sum256(body)
}

// markWriteUncertainLocked records that a record write may or may not have landed.
//
// A DEFINITE CAS rejection is not uncertainty and must not come here: it proves
// the write did not land, so sess.seq is already right and the session stays
// usable. Only an ambiguous failure — a timeout, a lost response, no responders —
// leaves an outcome nobody can name.
//
// sess.mu must be held.
func (sess *mqttSession) markWriteUncertainLocked(baseSeq uint64, body []byte) {
	sess.seqUncertain = true
	sess.pendingWrite = &mqttPendingWrite{baseSeq: baseSeq, digest: mqttRecordDigest(body)}
}

// isSeqUncertain reports whether this session has an unresolved record write.
func (sess *mqttSession) isSeqUncertain() bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.seqUncertain
}

// mqttIsRecordCASRejection reports whether err is a REJECTED expected-last-subject-
// sequence CAS — the subject was not at the sequence we required, so our write
// definitively did not land.
//
// 10071 comes from the single-writer check and 10164 from the clustered
// inflight-contention branches. Both mean "rejected, nothing stored", which is
// exactly what makes them certain: unlike a timeout, they leave sess.seq correct
// and the session usable. (Where an outcome must be turned into a DECISION rather
// than just an error — the teardown claim — 10164 is still re-read rather than
// read as "somebody else owns this", because it signals contention, not a lost
// race. Here only "did it land?" is being asked, and the answer is no.)
func mqttIsRecordCASRejection(err error) bool {
	return IsNatsErr(err, JSStreamWrongLastSequenceErrF) ||
		IsNatsErr(err, JSStreamWrongLastSequenceConstantErr)
}

// resolveUncertainWrite reads the session record back, classifies the ambiguous
// write against the attempted write's identity, and reconciles memory to match —
// ALL INSIDE A RECORD TRANSACTION.
//
// Holding the fence across the whole thing is the point, not an implementation
// detail. Resolution is a record-state transition like any other, and the three
// things it does — decide, clear the uncertainty, run the retained continuations —
// have to be indivisible with respect to other writers:
//
//   - observing "no transaction is open" and then resolving is a check-then-act: an
//     off-read-loop finalizer takes the fence in between and its save() publishes
//     seqUncertain before it returns, so the resolver settles an ambiguity whose
//     writer has not yet classified it;
//   - clearing the uncertainty and only then running the undos leaves a window in
//     which another writer can persist the still-unreverted attempted state — the
//     exact mutate/save interleaving the fence exists to prevent;
//   - two resolvers can otherwise both act on one attempt: the first reconciles and
//     its continuation advances the record, and the second then reads a record that no
//     longer describes the attempt it asked about.
//
// It never waits for the fence. A caller may already hold a transaction on the same
// goroutine — the SUBSCRIBE packet does — so blocking here would deadlock the
// readLoop against itself. A refusal simply means "not now": the periodic pass and
// the redelivery clocks retry, and nothing is lost because an unresolved session can
// make no progress meanwhile anyway.
//
// The retained continuations receive the resolver's token: they must write under it,
// never open a transaction of their own.
//
// No lock held on entry. Performs a JetStream request.
func (sess *mqttSession) resolveUncertainWrite() (mqttUncertainResolution, error) {
	if !sess.isSeqUncertain() {
		return mqttUncertainOurs, nil
	}
	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		return mqttUncertainUnresolved, errMQTTResolveFenceBusy
	}
	res, err := sess.resolveUncertainWriteInTx(tok)
	if terr := sess.endRecordTx(tok); terr != nil {
		return res, terr
	}
	return res, err
}

var errMQTTResolveFenceBusy = errors.New("another record transaction is open; resolution deferred")

// resolveUncertainWriteInTx is resolveUncertainWrite's body, with the fence held.
func (sess *mqttSession) resolveUncertainWriteInTx(tok recordTx) (mqttUncertainResolution, error) {
	sess.mu.Lock()
	if !sess.seqUncertain {
		sess.mu.Unlock()
		return mqttUncertainOurs, nil
	}
	pw := sess.pendingWrite
	domainTk, idHash := sess.domainTk, sess.idHash
	sess.mu.Unlock()
	if pw == nil {
		// Uncertain with no attempted-write identity: nothing can be classified, and
		// guessing is exactly what this exists to prevent.
		return mqttUncertainUnresolved, fmt.Errorf("session %q is uncertain with no recorded write identity", sess.id)
	}

	smsg, err := sess.jsa.loadSessionMsg(domainTk, idHash)

	// Re-validate the attempt the readback was ABOUT. The load is a network round
	// trip; if the session was resolved and made uncertain again meanwhile, this
	// result describes a different attempt and applying it would be a guess.
	//
	// DEFENSIVE, deliberately. The fence serializes resolvers, so with foreign
	// disposition now ownership-gated, identity-checked and idempotent there is no
	// known state transition this prevents — a second verdict on a stale attempt has
	// nothing left to corrupt. It is kept because the cost is one comparison and the
	// alternative is applying a readback to a question nobody asked; a mutation of it
	// therefore SURVIVES, which is the honest signal that it guards nothing reachable
	// today rather than something untested.
	sess.mu.Lock()
	stillOurs := sess.seqUncertain && sess.pendingWrite == pw
	sess.mu.Unlock()
	if !stillOurs {
		return mqttUncertainOurs, nil
	}

	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return mqttUncertainUnresolved, fmt.Errorf("unable to read back the record of session %q: %w", sess.id, err)
		}
		// The record is GONE. Our write cannot have landed and then vanished on its
		// own, so somebody released this session (a teardown, an expiry). Its state
		// is not ours to rebuild.
		return mqttUncertainForeign, nil
	}

	switch {
	case smsg.Sequence == pw.baseSeq:
		// Still exactly the record we CAS'd against: our write did not land, and
		// sess.seq already says so. Every mutation KEPT on the strength of "it may
		// have landed" is now undone — the record holds the state that preceded it.
		//
		// Uncertainty is cleared first so a continuation that must PERSIST its undo is
		// not refused at entry, and both happen under the fence this resolver holds,
		// so no other writer can observe or store the intermediate state.
		sess.clearUncertaintyLocked(pw.baseSeq)
		sess.runUncertainReverts(tok)
		return mqttUncertainOurs, nil
	case smsg.Sequence > pw.baseSeq && mqttRecordDigest(smsg.Data) == pw.digest:
		// Byte-identical to what we tried to store, at a newer sequence: it landed,
		// and only the response was lost. Adopt the sequence it landed at and run the
		// LANDED continuations — memory already matches the record, but every action a
		// caller withheld for want of proof is now authorized.
		sess.clearUncertaintyLocked(smsg.Sequence)
		sess.commitUncertainWrites(tok)
		return mqttUncertainOurs, nil
	default:
		// A newer record with different content, or a sequence below our base. Either
		// way it is not the write we attempted, so it belongs to another owner. Do
		// NOT adopt its sequence: doing so would turn our stale snapshot into a valid
		// CAS and overwrite whatever that owner has since persisted.
		return mqttUncertainForeign, nil
	}
}

// clearUncertainty marks the session resolved at seq.
func (sess *mqttSession) clearUncertaintyLocked(seq uint64) {
	sess.mu.Lock()
	sess.seqUncertain = false
	sess.pendingWrite = nil
	sess.seq = seq
	sess.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Reconciling the state an ambiguous write left behind
// ---------------------------------------------------------------------------

// uncertainOutcomeActions is what a caller owes the readback after keeping a
// mutation whose write MAY have landed.
//
// Keeping it is only half an answer. The write either landed — in which case memory
// already matches the record, but any action the caller deferred because it could
// not prove durability is now owed — or it did not, in which case the record still
// holds the previous state and memory has to be put back. Nobody can tell which at
// failure time, which is the whole reason the state was kept; the readback decides,
// so BOTH continuations have to be handed to it rather than guessed at.
//
// Registering only the undo is how "keep it on saveMaybeLanded" turns into "assume
// it landed" in one direction and "stall forever" in the other: a drain claim whose
// removal never reached the stream gets acked as though it had (I1), while one that
// did land is never acked at all and its drain is never torn down.
//
// Both run with sess.mu NOT held.
// Each receives the RESOLVER'S record-transaction token. A continuation that needs
// to persist must write under it: opening a transaction of its own would deadlock
// against the resolver holding the fence, and would also let another writer
// interleave between the resolution and the reconciliation.
type uncertainOutcomeActions struct {
	// onNotLanded undoes the mutation: the record is still at the base sequence.
	onNotLanded func(recordTx)
	// onLanded performs whatever was deferred for want of proof: the record holds
	// the mutation, so the irreversible follow-up is now authorized. May be nil.
	onLanded func(recordTx)
}

// resolveUncertainNow attempts the readback immediately, for an off-readLoop writer
// whose ambiguous save would otherwise sit unresolved.
//
// The periodic pass does handle bound sessions, but only on its own schedule, so an
// ambiguity left for it sits unresolved in the meantime: the drain's claim neither
// restored nor acked, and every further save on the session refused. Resolving at the
// point the ambiguity arises shortens that window to nothing in the common case.
//
// Best effort by design: an unreadable record leaves the session unresolved for the
// periodic pass (or the next resume) to retry, which is the same posture as every
// other reader here. A foreign record is reported so the caller can stop.
//
// sess.mu must not be held.
func (sess *mqttSession) resolveUncertainNow(c *client) mqttUncertainResolution {
	res, err := sess.resolveUncertainWrite()
	if err != nil && c != nil {
		c.Debugf("MQTT: could not resolve the uncertain record write of session %q: %v", sess.id, err)
	}
	if res == mqttUncertainForeign {
		// Somebody else owns this session now. Disposition goes through the ownership
		// gate, not inline: doing it here would race a CONNECT that has already
		// evicted this object and installed a replacement. See
		// disposeForeignSessionLocked.
		sess.disposeForeignSession(c)
	}
	return res
}

// disposeForeignSessionLocked is the SINGLE foreign disposition: another owner holds
// this session, so every obligation — the expiry, the Will, the durables — is theirs.
//
// It detaches the stale client, drops the deferred close, evicts the session and
// CLOSES the connection. All four belong together:
//
//   - detaching without closing leaves a client still holding c.mqtt.sess, free to
//     publish, subscribe and ack against an evicted session while another broker is
//     authoritative;
//   - cancelling the expiry timer before the removal is known to apply disarms a
//     replacement session's deadline: the periodic sweep re-arms it eventually, but
//     not before that session has outlived the interval it was promised;
//   - evicting without an identity check deletes by client ID, so a CONNECT that has
//     already installed a restored replacement has that replacement removed instead;
//   - and doing any of it outside the ownership gate races that CONNECT directly.
//
// The caller must hold the session in asm.sessLocked, which is what serializes this
// against a concurrent CONNECT. Only the client this disposition observed is closed,
// so a client that rebound in the meantime is left alone.
//
// No lock held on entry (other than the ownership gate).
func (sess *mqttSession) disposeForeignSessionLocked(asm *mqttAccountSessionManager, c *client) {
	// The identity check comes BEFORE any mutation. A stale caller that discarded the
	// deferred close and only then noticed the binding had changed would have
	// destroyed the CURRENT session's obligation — the Will it still owes, the expiry
	// deadline it is still counting — on its way to deciding it had no business here.
	sess.mu.Lock()
	stale := sess.c
	if c != nil && stale != c {
		// Somebody rebound while this disposition was being decided. The gate means
		// that cannot be a CONNECT in flight, but it can be one that completed before
		// we took it — in which case the session is theirs and not ours to tear down.
		sess.mu.Unlock()
		return
	}
	sess.mu.Unlock()

	sess.takeDeferredDisconnect()
	sess.mu.Lock()
	sess.c = nil
	sess.seqUncertain, sess.pendingWrite = false, nil
	sess.mu.Unlock()

	if asm != nil {
		// Removal and expiry cancellation happen together, under asm.mu, and only if
		// this object is still the resident one: the timer is keyed by client hash, so
		// cancelling it unconditionally would disarm a REPLACEMENT session's deadline.
		asm.removeSessionIfCurrent(sess)
	}
	if stale != nil {
		stale.Warnf("MQTT: session %q is now owned elsewhere; closing this connection", sess.id)
		stale.mqttEnqueueDisconnect(mqttReasonSessionTakenOver, mqttReasonStrSessionTakenOver)
		go stale.closeConnection(DuplicateClientID)
	}
}

// disposeForeignSession is disposeForeignSessionLocked for a caller that does not
// already hold the ownership gate. Failing to take it means a CONNECT is in
// progress: it wins, and it will rebuild from the authoritative record itself.
//
// No lock held on entry.
func (sess *mqttSession) disposeForeignSession(c *client) {
	asm := sess.asm
	if asm == nil {
		return
	}
	sess.mu.Lock()
	owner := sess.c
	sess.mu.Unlock()
	if err := asm.lockSession(sess, owner); err != nil {
		return
	}
	defer asm.unlockSession(sess)
	sess.disposeForeignSessionLocked(asm, c)
}

// retainOnUncertain registers the two continuations for a mutation that was KEPT
// because its write may have landed. See uncertainOutcomeActions.
//
// sess.mu must not be held.
func (sess *mqttSession) retainOnUncertain(onNotLanded, onLanded func(recordTx)) {
	if onNotLanded == nil && onLanded == nil {
		return
	}
	sess.mu.Lock()
	sess.uncertainReverts = append(sess.uncertainReverts, uncertainOutcomeActions{onNotLanded: onNotLanded, onLanded: onLanded})
	sess.mu.Unlock()
}

// takeUncertainActions removes and returns the registered continuations.
func (sess *mqttSession) takeUncertainActions() []uncertainOutcomeActions {
	sess.mu.Lock()
	r := sess.uncertainReverts
	sess.uncertainReverts = nil
	sess.mu.Unlock()
	return r
}

// commitUncertainWrites runs the LANDED continuations, oldest first: the record
// holds every retained mutation, so the follow-ups they deferred are now owed.
func (sess *mqttSession) commitUncertainWrites(tok recordTx) {
	for _, a := range sess.takeUncertainActions() {
		if a.onLanded != nil {
			a.onLanded(tok)
		}
	}
}

// runUncertainReverts undoes every retained mutation, most recent first: the write
// did NOT land, so the record still holds the state that preceded it.
func (sess *mqttSession) runUncertainReverts(tok recordTx) {
	r := sess.takeUncertainActions()
	for i := len(r) - 1; i >= 0; i-- {
		if r[i].onNotLanded != nil {
			r[i].onNotLanded(tok)
		}
	}
}

// deferDisconnect records the MQTT obligations a close could not discharge because
// the session was unresolved, and detaches the client so nothing further can act on
// the stale state. Returns false if an action was already recorded (the first one
// holds the original deadlines and must win).
//
// No lock held on entry.
func (sess *mqttSession) deferDisconnect(action *mqttDeferredDisconnect) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.deferredDisconnect != nil {
		return false
	}
	sess.deferredDisconnect = action
	sess.c = nil
	return true
}

// takeDeferredDisconnect removes and returns the pending deferred action, if any.
func (sess *mqttSession) takeDeferredDisconnect() *mqttDeferredDisconnect {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	action := sess.deferredDisconnect
	sess.deferredDisconnect = nil
	return action
}

// expiryDeadline is the absolute Unix time this session expires, and whether it has
// one at all. A zero interval is "ends now"; mqttSessionNeverExpire has no deadline.
func (a *mqttDeferredDisconnect) expiryDeadline() (int64, bool) {
	if a.expiryInterval == 0 || a.expiryInterval == mqttSessionNeverExpire {
		return 0, false
	}
	return a.disconnectedAt + int64(a.expiryInterval), true
}

// endsAtDisconnect reports whether this session was to be discarded at connection
// close (a deferred clear()).
func (a *mqttDeferredDisconnect) endsAtDisconnect() bool { return a.expiryInterval == 0 }

// expiredBy reports whether the session's expiry deadline has already passed at
// now, so resolution must discard it immediately rather than arm a fresh interval.
func (a *mqttDeferredDisconnect) expiredBy(now time.Time) bool {
	deadline, ok := a.expiryDeadline()
	return ok && now.Unix() >= deadline
}

// willDue reports whether the Will's absolute deadline has passed at now. A
// reconnect arriving before it cancels the Will; after it, the Will is still owed.
func (a *mqttDeferredDisconnect) willDue(now time.Time) bool {
	return a.willDeadline > 0 && now.UnixNano() >= a.willDeadline
}

// mqttDeferDisconnect captures everything a close would have done and parks it on
// the session, then detaches the client. Nothing is written and nothing is
// deleted: the session's record state is unresolved, so no teardown decision can
// be made from it yet.
//
// The Will is captured HERE rather than re-derived on resolution, for two reasons:
// the closing client's CONNECT packet is about to become unreachable, and its
// publish permission is not serializable, so it has to be evaluated while the
// client still exists (a denied Will is never captured, exactly as mqttHandleWill
// never enqueues one).
//
// Runs from the client's readLoop. No lock held on entry.
func (s *Server) mqttDeferDisconnect(c *client, asm *mqttAccountSessionManager, sess *mqttSession) {
	now := time.Now()
	sess.mu.Lock()
	action := &mqttDeferredDisconnect{
		disconnectedAt: now.Unix(),
		expiryInterval: sess.expiryInterval,
		willSessionSeq: sess.seq,
		willGen:        sess.gen,
	}
	sess.mu.Unlock()
	action.willSessEpoch = asm.sessStreamEpoch

	c.mu.Lock()
	// Consume it: from here this deferred action owns the Will, and no other
	// teardown path may publish it as well. See mqttTakeWill.
	if will, disposition := c.mqttTakeWillLocked(); will != nil {
		// Same rule as mqttHandleWill: a v5 Will Delay on a session that survives
		// the connection defers publication, capped at the Session Expiry Interval;
		// anything else is due immediately. The close already classified it unless
		// it somehow arrived here still armed.
		delayed := disposition == mqttWillOwedDelayed
		if disposition == mqttWillArmed {
			delayed = mqttWillIsDelayed(c.mqtt.proto, will, action.expiryInterval)
		}
		if !c.pubAllowedFullCheck(string(will.subject), false, true) {
			// Denied: never captured, so it can never be published later either.
			will = nil
		}
		if will != nil {
			action.will = will
			action.willProps = mqttWillForwardProps(will.props)
			action.willUser = c.opts.Username
			action.willDelayed = delayed
			if delayed {
				effDelay := will.props.willDelay
				if action.expiryInterval < effDelay {
					effDelay = action.expiryInterval
				}
				action.willDeadline = now.Add(time.Duration(effDelay) * time.Second).UnixNano()
			} else {
				action.willDeadline = now.UnixNano()
			}
		}
	}
	c.mu.Unlock()

	if !sess.deferDisconnect(action) {
		return // an earlier close already parked its (original) deadlines
	}
	asm.removeSessFromFlappers(sess.id)
	c.Warnf("MQTT: session %q has an unresolved record write; deferring its disconnect handling until it resolves", sess.id)
}

// resolveUncertainSession classifies a parked session's ambiguous write and
// discharges whatever the close deferred. It is the single place that turns
// "unresolved" into an outcome, so CONNECT and the retry pass both go through it
// and exactly one of them acts.
//
// The caller must hold the session in asm.sessLocked (lockSession), which is what
// serializes it against a concurrent CONNECT.
//
// Returns the resolution. On mqttUncertainUnresolved nothing was written, nothing
// was deleted, and the deferred action is still parked.
//
// No lock held on entry.
func (s *Server) resolveUncertainSession(asm *mqttAccountSessionManager, sess *mqttSession) mqttUncertainResolution {
	res, err := sess.resolveUncertainWrite()
	switch res {
	case mqttUncertainUnresolved:
		s.Debugf("MQTT: session %q is still unresolved: %v", sess.id, err)
		return res
	case mqttUncertainForeign:
		// Another owner holds this session. One disposition path for both resolvers,
		// so a session found foreign by the periodic pass has its stale connection
		// CLOSED too — this pass now examines bound sessions, and detaching one
		// without closing it leaves a live client acting on an evicted session. The
		// caller already holds the ownership gate.
		sess.disposeForeignSessionLocked(asm, nil)
		return res
	}

	// Ours. Discharge the deferred close, if the session is still unbound: a
	// reconnect that got here first owns the session now and its CONNECT applies
	// the reconnect rules (cancelling or retaining the Will) itself.
	sess.mu.Lock()
	rebound := sess.c != nil
	sess.mu.Unlock()
	if rebound {
		return res
	}
	if action := sess.takeDeferredDisconnect(); action != nil {
		s.applyDeferredDisconnect(asm, sess, action)
	}
	return res
}

// applyDeferredDisconnect performs the close work that was parked, against the
// ORIGINAL deadlines: the session gets the lifetime it actually earned, not a
// fresh interval starting whenever JetStream came back.
//
// No lock held on entry; the session is in the locked map.
func (s *Server) applyDeferredDisconnect(asm *mqttAccountSessionManager, sess *mqttSession, action *mqttDeferredDisconnect) {
	now := time.Now()

	switch {
	case action.endsAtDisconnect(), action.expiredBy(now):
		// The session ended at connection close, or its deadline passed while the
		// record was unresolved. Either way it is discarded now, not given a fresh
		// interval.
		asm.removeSession(sess, true)
		if err := sess.clear(true); err != nil {
			s.Warnf("MQTT: deferred discard of session %q failed: %v", sess.id, err)
		}
	default:
		// Persist the ORIGINAL disconnect time and arm only what remains of the
		// interval. A never-expiring session gets no timer, as usual.
		sess.mu.Lock()
		sess.disconnectedAt = action.disconnectedAt
		sess.mu.Unlock()
		tok := sess.beginRecordTx()
		if err := sess.save(tok); err != nil {
			s.Debugf("MQTT: unable to persist the deferred disconnect of session %q: %v", sess.id, err)
		}
		if terr := sess.endRecordTx(tok); terr != nil {
			s.Errorf("MQTT: %v", terr)
		}
		if deadline, ok := action.expiryDeadline(); ok {
			remaining := time.Until(time.Unix(deadline, 0))
			if remaining < 0 {
				remaining = 0
			}
			asm.scheduleSessionExpiry(s, sess.idHash, remaining)
		}
	}

	if action.will == nil {
		return
	}

	// A Will that is already DUE — one with no delay, or a delayed one whose
	// original deadline passed while the record was unresolved — is published
	// directly rather than enqueued. That is what makes the reconnect rule hold:
	// a reconnect arriving BEFORE the original deadline cancels the Will through
	// the outbox as usual, but one arriving AFTER it must not, because the Will was
	// already owed. Enqueuing a past-due Will instead would race the reconnect's
	// own cancel scan and could silently suppress it.
	//
	// The publishing client is long gone, so this goes through the outbox's
	// client-free publisher rather than the immediate path (which writes into
	// c.mqtt.pp). Same publish core, same bounding.
	if !action.willDelayed || action.willDue(now) {
		rec := &mqttWillOutbox{
			ClientIDHash: sess.idHash,
			Gen:          action.willGen,
			Topic:        string(action.will.topic),
			Msg:          action.will.message,
			QoS:          action.will.qos,
			Retain:       action.will.retain,
			Props:        action.willProps,
			User:         action.willUser,
		}
		if err := asm.deliverWillRecord(s, rec); err != nil {
			s.Warnf("MQTT: deferred Will for session %q could not be published: %v", sess.id, err)
		}
		return
	}

	// Still within the Will Delay: hand it to the durable outbox with its ORIGINAL
	// absolute FireAt, so the delay is the one the client configured rather than
	// one restarted by the outage, and a timely reconnect can still cancel it.
	sess.mu.Lock()
	seq := action.willSessionSeq
	if seq == 0 {
		seq = sess.seq
	}
	sess.mu.Unlock()
	_ = asm.enqueueDelayedWill(s, sess.idHash, action.willGen, action.will, action.willProps,
		action.willUser, action.willDeadline, seq, action.willSessEpoch)
}

// retryUncertainSessions is the periodic pass over RESIDENT unresolved sessions.
//
// It cannot be folded into sweepSessionExpiries: that walks persisted RECORDS and
// skips ExpiryInterval == 0 or mqttSessionNeverExpire — exactly the two cases that
// carry a deferred clear() or a deferred Will, so the sessions that most need
// resolving are the ones it would never look at.
//
// It is serialized against CONNECT by the same ownership gate expiry uses
// (lockSession with a nil client, which fails while a client is bound or the ID is
// mid-CONNECT), so a reconnect simply wins and resolves the session itself.
//
// Runs from the periodic sweep goroutine. No lock held on entry.
func (as *mqttAccountSessionManager) retryUncertainSessions(s *Server) {
	// Snapshot under the manager lock and release it before any JetStream call.
	as.mu.RLock()
	candidates := make([]*mqttSession, 0, len(as.sessions))
	for _, sess := range as.sessions {
		candidates = append(candidates, sess)
	}
	as.mu.RUnlock()

	for _, sess := range candidates {
		if !sess.isSeqUncertain() {
			continue
		}
		// A BOUND session must be resolved too, and it is the harder case: nothing
		// else will do it. Its own writers cannot — every save is refused while the
		// session is unresolved — and the connection can stay open indefinitely, so
		// "wait for the disconnect" is not a bound on anything. Left alone, such a
		// session stops being able to persist at all: an UNSUBSCRIBE whose response
		// was lost, a reserved-namespace purge, an affine unwind.
		//
		// The gate is taken as the BOUND CLIENT rather than as nobody, which is the
		// same ownership claim its own readLoop would make: it still fails while a
		// CONNECT is in progress (sessLocked) or if the client changed underneath us,
		// so a reconnect continues to win. resolveUncertainSession itself re-checks
		// for a rebind before discharging any deferred close, and its foreign branch
		// detaches and evicts — which is exactly right for a bound client holding a
		// session somebody else now owns.
		sess.mu.Lock()
		owner := sess.c
		txOpen := sess.recordTxOpen
		sess.mu.Unlock()
		if txOpen {
			// A writer is mid-transaction. save() publishes seqUncertain BEFORE it
			// returns, so resolving now would settle an ambiguity whose writer has not
			// yet classified it or registered its continuations: the writer would then
			// read its own error as DEFINITE and roll back a mutation that landed, or
			// register a continuation into an already-consumed list. Both are silent.
			// The fence is exactly the interval in which mark-classify-register is
			// atomic, so wait for the next pass. See classifySave.
			continue
		}
		if err := as.lockSession(sess, owner); err != nil {
			continue // a CONNECT is in progress, or the binding changed: it wins
		}
		// Re-validate AFTER taking the gate: a CONNECT may have resolved this
		// session between the snapshot and here.
		if sess.isSeqUncertain() {
			s.resolveUncertainSession(as, sess)
		}
		as.unlockSession(sess)
	}
}

// ---------------------------------------------------------------------------
// Classifying a failed record write for the caller's IN-MEMORY state
// ---------------------------------------------------------------------------

// saveOutcome says what a failed save means for the state the caller mutated
// before it.
//
// The distinction is not cosmetic. An ambiguous write may have landed, and which way
// memory has to be reconciled depends entirely on which — so committing to "it did
// not land" at failure time is a guess made at the one moment nobody can know. Guess
// wrong and the next save overwrites what did land: a drain, a claim, a membership,
// an attempt counter.
//
// (The attempted write's identity is safe either way — its digest is snapshotted into
// pendingWrite when the failure is recorded, so later memory changes cannot alter what
// the readback compares against. What is at stake is only the reconciliation.)
//
// So: roll back only a DEFINITE failure. Keep the attempted state when the write may
// have landed, and hand the readback both continuations — see
// uncertainOutcomeActions, because keeping it without saying how to undo or commit it
// is just "assume it landed" with extra steps.
type saveOutcome uint8

const (
	// saveLanded: the write is durable.
	saveLanded saveOutcome = iota
	// saveDefinitelyFailed: nothing was stored (a refused fence, a rejected CAS,
	// a refusal at entry). sess.seq is still right and the caller must undo its
	// mutation — inside the still-open transaction, or another writer can persist
	// the state it is undoing.
	saveDefinitelyFailed
	// saveMaybeLanded: the request went out and its outcome is unknown. The caller
	// must KEEP its mutation; the session is unresolved until a readback classifies
	// it, and no further write is permitted meanwhile.
	saveMaybeLanded
)

// classifySave turns a save error into an outcome. wasUncertain is
// isSeqUncertain() sampled BEFORE the write: without it, a save refused at entry
// because an EARLIER write is unresolved would be misread as this one having
// possibly landed, and the caller would keep a mutation that never left the
// process.
func (sess *mqttSession) classifySave(wasUncertain bool, err error) saveOutcome {
	if err == nil {
		return saveLanded
	}
	if !wasUncertain && sess.isSeqUncertain() {
		return saveMaybeLanded
	}
	return saveDefinitelyFailed
}

// saveClassified is save() plus the classification, for the common case where the
// caller does its own write.
func (sess *mqttSession) saveClassified(tok recordTx) (saveOutcome, error) {
	wasUncertain := sess.isSeqUncertain()
	err := sess.save(tok)
	return sess.classifySave(wasUncertain, err), err
}
