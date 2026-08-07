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
	"sync/atomic"
	"time"
)

// mqttPendingWrite is the identity of an attempted record write whose outcome is
// unknown: the sequence it CAS'd against, and a digest of the exact record bytes
// it tried to store. Both are needed — the sequence alone cannot tell our write
// apart from another broker's, which is the whole point of the classification.
type mqttPendingWrite struct {
	baseSeq uint64
	digest  [sha256.Size]byte
	// voidedRegsRev is the void-register revision the attempt MARSHALLED, so a
	// readback that proves those exact bytes durable can credit exactly the
	// verdicts they carried — no more, whatever has been staged since. Every
	// landed write credits the revision it marshalled, wherever the proof comes
	// from; this is that revision travelling with the proof. See
	// voidRegNonceLocked and releaseSettledVoidReservationsLocked.
	voidedRegsRev uint64
}

// mqttUncertainResolution is how a readback classified an ambiguous write.
type mqttUncertainResolution int

const (
	// mqttUncertainUnresolved: this session's durable state is not known, so
	// nothing may be written or deleted and the retry pass tries again. Two ways
	// to get here, and callers treat them identically: the readback FAILED, so
	// nothing was written and any deferred action is still parked; or the readback
	// succeeded and the work it authorized — a continuation's re-save, a discharged
	// deferred disconnect — left the session uncertain AGAIN, so the deferred action
	// was consumed but a NEW attempt is now outstanding. See
	// resolutionAfterContinuations.
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
// voidedRev is the void-register revision the attempt marshalled, carried so a
// landed readback can credit it.
//
// sess.mu must be held.
func (sess *mqttSession) markWriteUncertainLocked(baseSeq uint64, body []byte, voidedRev uint64) {
	sess.seqUncertain = true
	sess.pendingWrite = &mqttPendingWrite{
		baseSeq: baseSeq, digest: mqttRecordDigest(body), voidedRegsRev: voidedRev,
	}
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

// errMQTTResolveFenceBusy covers both refusal causes of the non-blocking acquisition:
// another record transaction is open, or the fence is free but reserved for an
// announced blocking writer under waiter priority (see sess_recordtx.go). The caller's
// arm is the same either way — not now, retry on the periodic pass — which is why one
// error serves both.
var errMQTTResolveFenceBusy = errors.New("the record fence is unavailable (a transaction is open, or a blocking writer is queued for it); resolution deferred")

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
		// Nothing landed, so nothing is credited: the void-register verdicts this
		// attempt marshalled are still durable nowhere, and the continuation that
		// re-saves them is what carries them (voidedRegsUncertainAction).
		return sess.resolutionAfterContinuations(mqttUncertainOurs), nil
	case smsg.Sequence > pw.baseSeq && mqttRecordDigest(smsg.Data) == pw.digest:
		// Byte-identical to what we tried to store, at a newer sequence: it landed,
		// and only the response was lost. Adopt the sequence it landed at and run the
		// LANDED continuations — memory already matches the record, but every action a
		// caller withheld for want of proof is now authorized.
		sess.clearUncertaintyLocked(smsg.Sequence)
		// These exact bytes are durable, so the void-register verdicts they
		// marshalled are too — credited here for the same reason a confirmed save
		// credits its own: the proof is the durability boundary, not the intent of
		// whoever wrote. See creditVoidedRegsLocked.
		sess.mu.Lock()
		sess.creditVoidedRegsLocked(pw.voidedRegsRev)
		sess.mu.Unlock()
		sess.commitUncertainWrites(tok)
		return sess.resolutionAfterContinuations(mqttUncertainOurs), nil
	default:
		// A newer record with different content, or a sequence below our base. Either
		// way it is not the write we attempted, so it belongs to another owner. Do
		// NOT adopt its sequence: doing so would turn our stale snapshot into a valid
		// CAS and overwrite whatever that owner has since persisted.
		return mqttUncertainForeign, nil
	}
}

// resolutionAfterContinuations downgrades a resolved verdict to UNRESOLVED when the
// continuations this resolution just ran left the session uncertain AGAIN.
//
// A continuation may write — that is the point of handing it the resolver's token —
// and its write can itself come back ambiguous, which re-arms seqUncertain and
// registers a fresh continuation for the NEW attempt. Reporting the original verdict
// then tells the caller the session is writable when its next write will be refused,
// and for CONNECT that is destructive rather than merely stale: it binds, its accept
// save is refused, and mqtt.go's removeSession evicts the only object holding the new
// continuation — so a deferred disconnect's deadline and its void-register verdicts
// are lost with no crash anywhere.
//
// Unresolved is the honest answer and every caller already handles it: nothing is
// discharged, nothing evicted, CONNECT rejects and the client retries, and the
// periodic pass keeps watching a session that is once again in its candidate set.
//
// No lock held on entry.
func (sess *mqttSession) resolutionAfterContinuations(res mqttUncertainResolution) mqttUncertainResolution {
	if res == mqttUncertainOurs && sess.isSeqUncertain() {
		return mqttUncertainUnresolved
	}
	return res
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
	// An UNBOUND session with a PARKED teardown has to resolve and discharge as
	// one step. Clearing the uncertainty first leaves a window in which a
	// reconnect binds, and that reconnect skips resolution precisely BECAUSE the
	// session is no longer uncertain — so nothing ever discharges the action and
	// it is stranded for good, taking its expiry and its Will with it.
	//
	// The ownership gate is what makes the pair atomic. Failing to take it is not
	// a problem to work around: it means a CONNECT is already in flight, and that
	// CONNECT still sees an uncertain session, so it resolves and discharges
	// through resolveUncertainSession itself. Reporting unresolved and leaving it
	// alone is therefore the correct outcome, not a lost update.
	// gated records that THIS call holds the ownership gate, which changes how a
	// foreign verdict must be disposed below: the wrapper form takes the gate
	// itself and would fail against the one already held here.
	gated := false
	if asm := sess.asm; asm != nil {
		// Claimed as the session's CURRENT owner — the bound client, or nobody for
		// an unbound one — which is the same claim reconcileResidentRecordState makes.
		// Sampling it cannot go stale unnoticed: lockSession re-checks the owner
		// under its own lock and fails if it moved.
		//
		// Taken UNCONDITIONALLY, not only when something is already parked.
		// Whether a teardown needs discharging is not knowable before the
		// resolution: a close holding this same gate can park one (and unbind the
		// session) at any point, so a decision made from a prior sample is a
		// check-then-act. The gate is what serializes this against that close.
		//
		// It never blocks — lockSession fails fast — and a failure means another
		// ownership-gated operation holds it, which for a CONNECT means the
		// session is still uncertain when it runs and it resolves and discharges
		// itself. Reporting unresolved is then the correct outcome.
		sess.mu.Lock()
		owner := sess.c
		sess.mu.Unlock()
		if err := asm.lockSession(sess, owner); err != nil {
			return mqttUncertainUnresolved
		}
		defer asm.unlockSession(sess)
		gated = true
	}
	res, err := sess.resolveUncertainWrite()
	if err != nil && c != nil {
		c.Debugf("MQTT: could not resolve the uncertain record write of session %q: %v", sess.id, err)
	}
	if res == mqttUncertainForeign {
		// Somebody else owns this session now. Disposition happens under the
		// ownership gate — doing it ungated would race a CONNECT that has already
		// evicted this object and installed a replacement — but this call already
		// holds that gate, so it must NOT go through the wrapper: lockSession fails
		// fast on a gate it finds held, including its own, which would silently
		// skip the disposition and leave a stale client bound to a session another
		// broker owns. See disposeForeignSessionLocked.
		if gated {
			sess.disposeForeignSessionLocked(sess.asm, c)
		} else {
			sess.disposeForeignSession(c)
		}
		return res
	}
	if res == mqttUncertainOurs {
		// Resolved as ours — which is exactly when a teardown parked by an earlier
		// close becomes discharge-able, and exactly when nothing else will do it:
		// clearing the uncertainty removes this session from the periodic pass, and
		// the next CONNECT skips resolution for the same reason. Only
		// resolveUncertainSession used to discharge one, and only the paths that go
		// through it ever reached that. The gate taken above (when there was
		// anything to discharge) is still held, so no reconnect can have bound
		// since the resolution.
		sess.dischargeDeferredIfUnbound(c)
		// The discharge's own record write can be ambiguous, which leaves this
		// session unresolved again however cleanly the previous attempt classified.
		// Reported for the same reason resolveUncertainSession reports it: a caller
		// told "ours" would act on a session whose next write is refused. This site's
		// production callers ignore the value, so the honest return is what a test
		// discriminates rather than a behavior change here.
		return sess.resolutionAfterContinuations(res)
	}
	return res
}

// dischargeDeferredIfUnbound applies a parked deferred disconnect once its
// session's record state is known again, provided nobody has rebound in the
// meantime — a reconnect owns the decision itself and applies the reconnect
// rules (cancelling or retaining the Will), which is why resolveUncertainSession
// makes the same check.
//
// The caller must already hold the ownership gate for an unbound session with a
// parked action — resolveUncertainNow takes it precisely so that the resolution
// and this discharge cannot be split by a reconnect.
//
// No lock held on entry (other than that gate).
func (sess *mqttSession) dischargeDeferredIfUnbound(c *client) {
	asm := sess.asm
	if asm == nil || c == nil || c.srv == nil {
		return
	}
	sess.mu.Lock()
	bound, parked := sess.c != nil, sess.deferredDisconnect != nil
	sess.mu.Unlock()
	if bound || !parked {
		return
	}
	if action := sess.takeDeferredDisconnect(); action != nil {
		c.srv.applyDeferredDisconnect(asm, sess, action)
	}
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
// The takeover DISCONNECT is sent only to a connection that has actually been
// acknowledged. CONNACK must be the first packet a client sees
// ([MQTT-3.2.0-1]), so a connection whose success CONNACK was never written —
// one refused inside its own CONNECT, which mqttAbortAcceptedConnect leaves
// bound until the readLoop's close bookkeeping detaches it — must be closed
// silently. That is read from the connection itself rather than passed in by
// the caller: a resolver reaching a foreign verdict from the periodic pass has
// no idea which connection it is about to close, and got this wrong.
//
// The state disposal and the notify/close tail are split apart because they belong on
// different goroutines for different callers, NOT because either is optional: every caller
// that disposes state runs the tail. An off-readLoop caller (this one) must not close
// synchronously — it holds the ownership gate, and closeConnection unsubscribes and can
// drain — while the bound settlement on a session's own readLoop must, so that the
// connection is shut before its parser can look at another packet. See
// mqttGateDispatchOnRejection.
//
// No lock held on entry (other than the ownership gate).
func (sess *mqttSession) disposeForeignSessionLocked(asm *mqttAccountSessionManager, c *client) {
	stale, disposed := sess.disposeForeignStateLocked(asm, c)
	if !disposed || stale == nil {
		return
	}
	stale.mqttNotifyForeignDisposal(sess.id)
	go stale.closeConnection(DuplicateClientID)
}

// disposeForeignStateLocked is the STATE half of the foreign disposition: it detaches the
// stale client, drops the deferred close and evicts the session. It reports the client it
// observed bound and whether the disposal APPLIED, so a caller cannot run the notify/close
// tail against a session it turned out to have no business disposing of.
//
// No lock held on entry (other than the ownership gate).
func (sess *mqttSession) disposeForeignStateLocked(asm *mqttAccountSessionManager, c *client) (*client, bool) {
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
		return nil, false
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
	return stale, true
}

// mqttNotifyForeignDisposal tells a disposed connection why it is about to be closed. The
// close itself is the caller's: it is what differs between the off-readLoop callers and the
// settlement running on this connection's own readLoop.
//
// The notification itself is mqttNotifyTakenOver's, the one tail every displacement path
// shares: the CONNACK gate that used to be checked here lives there now, alongside the
// claim that keeps this from being the second DISCONNECT on the socket.
//
// No lock held on entry.
func (c *client) mqttNotifyForeignDisposal(id string) {
	c.Warnf("MQTT: session %q is now owned elsewhere; closing this connection", id)
	c.mqttNotifyTakenOver()
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
//
// Both walkers take the whole list and run it to the end, which is correct only while
// at most ONE continuation per pending write can re-arm uncertainty (today the
// void-verdict re-save, which registers itself for the new attempt and is the last
// thing to run). Register a second re-arming continuation and these walkers must stop
// at the first re-arm and put the unprocessed tail back: continuations that follow it
// would write under a token whose session is uncertain again, and every such write is
// refused at entry — silently, since neither walker looks at an outcome.
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
	parked, _ := sess.deferDisconnectIfUncertain(action, false)
	return parked
}

// deferDisconnectIfUncertain is deferDisconnect for a close that does NOT hold
// the ownership gate, so the uncertainty it is parking against can be resolved
// out from under it. The check and the park are ONE sess.mu critical section:
// checked separately, a gate-holding resolver can clear the uncertainty and run
// its final discharge scan between the two, and an action parked after that
// scan is stranded forever — the session is no longer uncertain, so neither the
// periodic pass nor the next CONNECT will ever resolve (and discharge) again.
//
// Returns (parked, stillUncertain). Not parked because the session resolved
// meanwhile (stillUncertain false) means the close must go back to taking the
// gate and performing an ordinary teardown; not parked because an action
// already exists (stillUncertain true) keeps the first action's deadlines, as
// always. requireUncertain false is the gated caller, for whom the check is
// the caller's own (it holds the gate, so nothing can resolve concurrently).
//
// No lock held on entry.
func (sess *mqttSession) deferDisconnectIfUncertain(action *mqttDeferredDisconnect, requireUncertain bool) (bool, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if requireUncertain && !sess.seqUncertain {
		return false, false
	}
	if sess.deferredDisconnect != nil {
		return false, true
	}
	sess.deferredDisconnect = action
	sess.c = nil
	return true, true
}

// retargetDeferredLifetime updates a PARKED deferred disconnect's Session
// Expiry Interval, for the one case that can invalidate it: a lifetime grant
// whose write was unresolved when the connection closed, and which a later
// readback proves LANDED.
//
// The close path snapshots the lifetime it can see, which while unresolved is
// deliberately the last CONFIRMED one — that is what keeps a grant that never
// landed from discarding the session. When the readback proves the opposite,
// the snapshot is the stale half: applyDeferredDisconnect decides discard and
// timer behavior from the action, not from the session, so without this the
// outcome would depend on whether the close or the readback ran first.
//
// prior is whatever was parked when the grant was attempted, and it is what
// makes this SAFE: an action is only retargeted when it is not that one. A
// deferred disconnect carries no owner identity, and one parked by an EARLIER
// connection can outlive its resolution — resolveUncertainNow classifies a write
// as ours without discharging a parked action, after which the periodic pass
// skips the session because it is no longer uncertain, and the next CONNECT
// skips resolution for the same reason. Retargeting blindly would then rewrite
// a previous connection's teardown terms with this one's. Pointer identity is
// the exact predicate: the close path installs one action and refuses to
// replace it, so a different pointer means this connection's close parked it.
//
// Only the interval moves. The action's Will is untouched and cannot be
// inconsistent with it here: a CONNECT refused before its CONNACK has already
// discarded its Will (mqttAbortAcceptedConnect), so a parked action from that
// path carries none.
//
// No lock held on entry.
func (sess *mqttSession) retargetDeferredLifetime(prior *mqttDeferredDisconnect, expiry uint32) {
	sess.mu.Lock()
	if cur := sess.deferredDisconnect; cur != nil && cur != prior {
		cur.expiryInterval = expiry
	}
	sess.mu.Unlock()
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
	s.mqttDeferDisconnectEx(c, asm, sess, false)
}

// mqttDeferDisconnectEx is mqttDeferDisconnect with the park made conditional
// on the session still being uncertain, for a close that could not take the
// ownership gate. Returns whether the teardown is now someone's responsibility
// (parked here, or already parked by an earlier close); false means the session
// resolved while this close was building the action, so the caller must retake
// the gate and tear down ordinarily — and the Will this function consumed has
// been put back exactly as it was, or that ordinary teardown would publish
// nothing.
func (s *Server) mqttDeferDisconnectEx(c *client, asm *mqttAccountSessionManager, sess *mqttSession, requireUncertain bool) bool {
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
	// teardown path may publish it as well. See mqttTakeWill. taken/takenDisp
	// remember the pre-take state so a REFUSED park can undo the consumption.
	var taken *mqttWill
	var takenDisp mqttWillDisposition
	if will, disposition := c.mqttTakeWillLocked(); will != nil {
		taken, takenDisp = will, disposition
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

	parked, stillUncertain := sess.deferDisconnectIfUncertain(action, requireUncertain)
	if !parked {
		if !stillUncertain {
			// Resolved while the action was being built: nothing may be parked
			// (nothing would ever discharge it), and the Will goes back where it
			// was so the ordinary teardown the caller now performs can publish it.
			if taken != nil {
				c.mu.Lock()
				if c.mqtt != nil && c.mqtt.cp != nil {
					c.mqtt.cp.will = taken
					c.mqtt.willDisposition = takenDisp
				}
				c.mu.Unlock()
			}
			return false
		}
		return true // an earlier close already parked its (original) deadlines
	}
	asm.removeSessFromFlappers(sess.id)
	c.Warnf("MQTT: session %q has an unresolved record write; deferring its disconnect handling until it resolves", sess.id)
	return true
}

// resolveUncertainSession classifies a parked session's ambiguous write and
// discharges whatever the close deferred. It is the single place that turns
// "unresolved" into an outcome, so CONNECT and the retry pass both go through it
// and exactly one of them acts.
//
// The caller must hold the session in asm.sessLocked (lockSession), which is what
// serializes it against a concurrent CONNECT.
//
// Returns the resolution. mqttUncertainUnresolved has the two meanings the enum
// states: the readback failed, in which case nothing was written, nothing was
// deleted and the deferred action is still parked; or the DISCHARGE re-armed
// uncertainty, in which case the action was applied and its own record write is now
// the outstanding one. Callers must treat both the same way — the session's durable
// state is unknown either way — and no caller may bind, resume or discard on it.
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
		// The discharge writes the record ITSELF, outside resolveUncertainWriteInTx,
		// so it can leave the session uncertain again after the readback classified
		// the previous attempt as ours. Re-checking here is what stops a CONNECT
		// from binding on a stale "ours" and then evicting the session its refused
		// accept save runs into — taking the deferred deadline and the void-register
		// verdicts the discharge just staged with it.
		return sess.resolutionAfterContinuations(res)
	}
	return res
}

// applyDeferredDisconnect performs the close work that was parked, against the
// ORIGINAL deadlines: the session gets the lifetime it actually earned, not a
// fresh interval starting whenever JetStream came back.
//
// It is called with the action already CONSUMED, which is what makes the
// unresolved re-check below load-bearing rather than defensive: nothing here can
// land while the record state is unresolved — the surviving branch's save is
// refused at entry and clear() refuses at entry too — and there would be nothing
// left to retry the close with.
//
// No lock held on entry; the session is in the locked map.
func (s *Server) applyDeferredDisconnect(asm *mqttAccountSessionManager, sess *mqttSession, action *mqttDeferredDisconnect) {
	now := time.Now()

	// The resolution that authorized this discharge released the record fence before
	// getting here, and holding the ownership gate does not exclude an off-readLoop
	// writer: a register store settling meanwhile takes the fence with tryBeginRecordTx
	// and can leave its own write AMBIGUOUS. Put the action back untouched rather than
	// applying a close whose every write is refused — the session is uncertain again, so
	// the periodic pass and the next CONNECT resolve and discharge it, with its ORIGINAL
	// deadlines still intact. The surviving branch re-checks once more under its own
	// fence, which is the sound test; this one is the cheap early-out that also covers
	// the discard branch, whose clear() opens a fence of its own.
	if sess.isSeqUncertain() {
		s.reparkDeferredDisconnect(sess, action)
		return
	}
	// Test seam, fired in the window between that check and the fence the surviving branch
	// takes below: an ambiguity armed here is invisible to the check above, which is what
	// makes the in-fence re-check the sound one rather than a belt-and-braces copy.
	if h := mqttTestDischargeWindowHookV.Load(); h != nil {
		(*h)(sess)
	}

	switch {
	case action.endsAtDisconnect(), action.expiredBy(now):
		// The session ended at connection close, or its deadline passed while the
		// record was unresolved. Either way it is discarded now, not given a fresh
		// interval.
		//
		// clear() must own its own transaction — it ENDS the record generation — so this
		// branch cannot hold the fence across it the way the surviving branch does. What
		// makes the discard safe is the in-fence refusal: an ambiguity armed after the
		// entry check makes clear() fail before its first deletion rather than tear down
		// on a stale sequence. The recovery is then ours, because the action is already
		// consumed: put it back, with the session still RESIDENT, so the next resolution
		// discharges the discard and its Will.
		//
		// Keyed on the SENTINEL, never on "is the session uncertain now": any other
		// failure means a lost teardown claim or a partly-done teardown, and re-parking
		// there would resurrect an obligation that is either another owner's or already
		// half-discharged. Those keep the historical outcome — the object goes.
		if err := sess.clear(true); err != nil {
			if errors.Is(err, errMQTTClearRefusedUnresolved) {
				s.reparkDeferredDisconnect(sess, action)
				return
			}
			s.Warnf("MQTT: deferred discard of session %q failed: %v", sess.id, err)
		}
		asm.removeSession(sess, true)
	default:
		// The session SURVIVES, so this is the deferred form of the ordinary surviving
		// close and owes everything that close owes: the ORIGINAL disconnect time
		// persisted, only what remains of the interval armed, and the shared-group
		// pending deliveries purged so a resume cannot NAK-steal messages AckWait would
		// redeliver to other members and so the packet identifiers come back
		// (Spec5 [4.8.2], [4.9]).
		//
		// ONE record transaction and ONE classified write for both. The purge is not a
		// separate operation that happens to run first: when both register-stream rungs
		// fail it falls back to the session record, and an independent save of its own
		// can land ambiguously — after which every later write on this session is
		// refused, including the disconnect time's, with the parked action already
		// consumed and nothing left to retry it. The deadline would then exist in memory
		// only while the landed record said DisconnectedAt=0, i.e. connected: the local
		// timer fires, expireSession finds the record bumped past its stale sequence and
		// delegates to the record, which reads "connected, nothing due" and drops the
		// expiry — leaking the record until a reconnect that may never come.
		//
		// So the fence opens BEFORE the first mutation, the purge only STAGES (its
		// fallback verdicts go into cancelledRegNonces, its register-stream rungs need
		// no record), and one save carries the disconnect time and the verdicts
		// together.
		tok := sess.beginRecordTx()
		// Re-checked with the fence HELD, before the first mutation, and this is the check
		// that is sound: the early-out above is a check-then-act, because the fence is what
		// serializes writers and another one can go ambiguous while this call is still
		// waiting for it. Uncertainty cannot appear from here on — creating it requires a
		// save, and a save requires this fence.
		if sess.isSeqUncertain() {
			if terr := sess.endRecordTx(tok); terr != nil {
				s.Errorf("MQTT: %v", terr)
			}
			s.reparkDeferredDisconnect(sess, action)
			return
		}
		sess.mu.Lock()
		sess.disconnectedAt = action.disconnectedAt
		sess.mu.Unlock()
		if owed := sess.purgeSharedPendingStaged(nil); owed {
			s.Debugf("MQTT: the deferred close of session %q carries its void-register verdicts on the session record", sess.id)
		}
		outcome, err := sess.saveClassified(tok)
		switch outcome {
		case saveMaybeLanded:
			// Ambiguous: the deadline and the verdicts are both in memory and both ride
			// the continuation's re-save, so restart-survival is restored the moment the
			// readback says the write did not land. A landed one needs nothing — the
			// resolver credits what those bytes carried.
			sess.retainOnUncertain(sess.voidedRegsUncertainAction(nil), nil)
			s.Debugf("MQTT: the deferred disconnect of session %q was written ambiguously; the readback will settle it: %v", sess.id, err)
		case saveDefinitelyFailed:
			// The in-memory purge stands, the verdicts stay staged for the next
			// convergence point, and the timer below is still armed. Only
			// restart-survival of the deadline is lost — the same posture as the
			// ordinary close's failed disconnect save, and worth a warning because a
			// lost purge verdict is not a routine debug event.
			s.Warnf("MQTT: unable to persist the deferred disconnect of session %q; its deadline will not survive a restart: %v", sess.id, err)
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

	// RE-SAMPLE the clock. The branch above did JetStream work — the purge's register-stream
	// rungs and the classified save — and a Will deadline that passed during it must not be
	// read from the entry sample as still in the future: that would enqueue an already-DUE
	// Will, and an enqueued record is cancellable, so a reconnect arriving after the deadline
	// could suppress a Will that was already owed. The direct publication below is exactly
	// what makes that reconnect rule hold, so the decision has to be made on the time as it
	// is now, not as it was before the work.
	now = time.Now()

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

// mqttTestDischargeWindowHookV fires after applyDeferredDisconnect's entry check and before
// the surviving branch opens its record transaction. See applyDeferredDisconnect.
var mqttTestDischargeWindowHookV atomic.Pointer[func(sess *mqttSession)]

func setMQTTTestDischargeWindowHook(h func(sess *mqttSession)) func() {
	prev := mqttTestDischargeWindowHookV.Load()
	if h == nil {
		mqttTestDischargeWindowHookV.Store(nil)
	} else {
		mqttTestDischargeWindowHookV.Store(&h)
	}
	return func() { mqttTestDischargeWindowHookV.Store(prev) }
}

// reparkDeferredDisconnect puts a consumed action back, for a discharge that found the
// record state unresolved again. Nothing is undone because nothing was applied, and the
// action still carries the ORIGINAL deadlines — which is the whole reason it is re-parked
// rather than rebuilt.
//
// A refusal means an action is already parked, which cannot happen for a session nobody
// can bind (the ownership gate is held), so it is reported rather than handled: the first
// action holds the earlier deadlines and must win, as always.
//
// No lock held on entry.
func (s *Server) reparkDeferredDisconnect(sess *mqttSession, action *mqttDeferredDisconnect) {
	if !sess.deferDisconnect(action) {
		s.Warnf("MQTT: the deferred disconnect of session %q could not be re-parked; another action is already pending", sess.id)
		return
	}
	s.Debugf("MQTT: the deferred disconnect of session %q stays parked; its record state is unresolved again", sess.id)
}

// reparkRefusedDiscard is the recovery for a discard that clear() REFUSED with
// errMQTTClearRefusedUnresolved — the ordinary clean close and session expiry both take it.
// Neither may simply abandon the session: the refusal deleted nothing, so the record, the
// consumers, the shared group durables the discard owed a release, and the generation's
// in-flight registers are all still there.
//
// It parks the discard as a deferred disconnect with expiryInterval 0, so endsAtDisconnect()
// is true and the discharge takes applyDeferredDisconnect's discard branch — the same
// clear(), performed by a caller that has just proved the record state resolved. Then it
// restores RESIDENCY, which is what every resolver needs: reconcileResidentRecordState and CONNECT
// both scan as.sessions, and for an expiry-0 record sweepSessionExpiries looks at nothing at
// all. Park first, publish second — the obligation must exist before the object is reachable.
//
// No WILL goes into the action, deliberately. A parked Will is memory-only and
// non-transferable: a remote takeover evicts the action under as.mu alone and nothing
// publishes it, which would regress a Will today's broker delivers. The close publishes its
// own by falling through to mqttHandleWill; expiry has none, its Will was discharged by the
// close that started the deadline.
//
// The caller must hold the ownership gate, and must key this on errMQTTClearRefusedUnresolved
// ALONE. Any other clear() failure means a lost teardown claim or a partly-done teardown,
// where re-parking would resurrect an obligation that is another owner's or already half
// discharged.
//
// Returns whether the discard is now someone's responsibility. False leaves the caller its
// historical outcome: the session stays evicted and the caller logs.
//
// No lock held on entry.
func (s *Server) reparkRefusedDiscard(asm *mqttAccountSessionManager, sess *mqttSession, disconnectedAt int64) bool {
	parked, stillUncertain := sess.deferDisconnectIfUncertain(
		&mqttDeferredDisconnect{disconnectedAt: disconnectedAt, expiryInterval: 0}, true)
	switch {
	case !parked && !stillUncertain:
		// UNREACHABLE while the gate is held: seqUncertain is cleared only by
		// clearUncertaintyLocked — reached through resolveUncertainNow, which takes the
		// gate itself, or resolveUncertainSession, whose caller holds it — and by
		// disposeForeignSessionLocked, which also requires it. Reported rather than
		// handled, and returning false is what makes a future violation of that invariant
		// benign (the caller's historical outcome) rather than silent.
		s.Errorf("MQTT: the refused discard of session %q cannot be parked: the session resolved while the ownership gate was held", sess.id)
		return false
	case !parked:
		// An action is already parked, so this session already owes an end. This helper
		// ARBITRATES NOTHING. It recognises only the shape it would have written itself —
		// ends at disconnect AND carries no Will — and in that case the discard IS that
		// obligation, already recorded: the action is neither mutated nor replaced.
		//
		// Every other shape declines. A parked action carries no owner identity, so
		// nothing here can prove whose deadline or whose Will a retarget would overwrite;
		// declining keeps today's evict-and-log, which is provably no worse than today
		// because it IS today. Reachability of those shapes at this point is unknown and
		// stated as such in docs/CONFORMANCE.md.
		//
		// Read under sess.mu, as every other access to this field is.
		sess.mu.Lock()
		existing := sess.deferredDisconnect
		discardOnly := existing != nil && existing.endsAtDisconnect() && existing.will == nil
		sess.mu.Unlock()
		if !discardOnly {
			return false
		}
		s.Debugf("MQTT: the refused discard of session %q is already parked as a discard-only obligation", sess.id)
	}
	// RESIDENT again, so a resolver can find it. The caller removed it before attempting
	// the discard, and for the close that removal is what makes the failure a leak: an
	// expiry-0 record with no resident object has no reaper at all.
	asm.addSession(sess, true)
	return true
}

// reconcileResidentRecordState is the periodic pass over RESIDENT sessions whose
// record state is not settled. It resolves an ambiguous write, settles a definite
// rejection, and back-stops the void-verdict release handoff — three ways for a
// resident session's record state to need work that no traffic will bring.
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
func (as *mqttAccountSessionManager) reconcileResidentRecordState(s *Server) {
	// Snapshot under the manager lock and release it before any JetStream call.
	as.mu.RLock()
	candidates := make([]*mqttSession, 0, len(as.sessions))
	for _, sess := range as.sessions {
		candidates = append(candidates, sess)
	}
	as.mu.RUnlock()

	for _, sess := range candidates {
		sess.mu.Lock()
		uncertain := sess.seqUncertain
		owner := sess.c
		txOpen := sess.recordTxOpen
		// Sampled here and RE-READ inside the fence by the probe itself; this is only
		// the candidate test.
		rejected := sess.recordRejectionPendingLocked()
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
		if uncertain {
			// A BOUND session must be resolved too, and it is the harder case: nothing
			// else will do it. Its own writers cannot — every save is refused while the
			// session is unresolved — and the connection can stay open indefinitely, so
			// "wait for the disconnect" is not a bound on anything. Left alone, such a
			// session stops being able to persist at all: an UNSUBSCRIBE whose response
			// was lost, a reserved-namespace purge, an affine unwind.
			//
			// The gate is taken as the BOUND CLIENT rather than as nobody, which is the
			// same ownership claim its own readLoop would make: it still fails while a
			// CONNECT is in progress (sessLocked) or if the client changed underneath
			// us, so a reconnect continues to win. resolveUncertainSession itself
			// re-checks for a rebind before discharging any deferred close, and its
			// foreign branch detaches and evicts — which is exactly right for a bound
			// client holding a session somebody else now owns.
			if err := as.lockSession(sess, owner); err != nil {
				continue // a CONNECT is in progress, or the binding changed: it wins
			}
			// Re-validate AFTER taking the gate: a CONNECT may have resolved this
			// session between the snapshot and here.
			if sess.isSeqUncertain() {
				s.resolveUncertainSession(as, sess)
			}
			as.unlockSession(sess)
			continue
		}
		if rejected {
			as.settleRejectionFromSweep(s, sess)
			continue
		}
		// Nothing outstanding on the record, but possibly still owing a void-register
		// verdict: the BACKSTOP is what carries the release handoff's liveness past a
		// definite store failure. Not unconditional — the attempt it re-arms enters the
		// same pending predicate, so it defers again while a blocking writer is announced
		// for the fence (see sess_recordtx.go), and that waiter's own release is then the
		// next trigger.
		as.flushVoidVerdictGap(sess)
	}
}

// settleRejectionFromSweep probes one UNBOUND session's outstanding record rejection
// and follows the verdict.
//
// The gate is the sole unboundness check on this path: a bound session's marker simply has
// its claim refused and waits for the connection to end, at which point this same pass
// settles it. See lockSessionForRecordRejection.
//
// Runs from the periodic sweep goroutine. No lock held on entry.
func (as *mqttAccountSessionManager) settleRejectionFromSweep(s *Server, sess *mqttSession) {
	if err := as.lockSessionForRecordRejection(sess); err != nil {
		return
	}
	defer as.unlockSession(sess)
	switch sess.settleRecordRejection(as) {
	case mqttRejectionUnresolved:
		// The probe's own write is ambiguous. Resolve it in THIS iteration with the gate
		// still held — that is resolveUncertainSession's contract, and the wrapper form
		// would claim the same gate and fail against our own hold.
		s.resolveUncertainSession(as, sess)
	case mqttRejectionOurs:
		// The probe IS the backstop: every landed save credits the void-register
		// revision it marshalled, so a gap it covered is closed by it, and a nonce
		// staged after its marshal is the release handoff's to converge.
	case mqttRejectionForeign:
		// Disposed. Nothing else may touch this session, backstop included.
	default: // mqttRejectionKeep
		// Ownership was not established, so the backstop's save would be the same doomed
		// CAS. The marker stays armed and the next pass asks again.
	}
}

// flushVoidVerdictGap is the periodic BACKSTOP for the release handoff.
//
// The handoff rides lock releases, and one attempt per newly staged revision is what keeps
// it from retrying in a storm — but the same watermark means a DEFINITE store failure ends
// the sequence: the revision is no longer above it, so no release triggers again. An
// unbound, never-expiring session has no traffic and no timer either, so the verdict would
// stay memory-only until something else happened to stage a nonce, which may be never; a
// crash then restores the register as an ordinary live delivery, holding its identifier and
// its Receive Maximum slot. A dependency outage is exactly when that is likeliest.
//
// So the periodic pass re-arms it: the watermark is put back below the revision and the
// ordinary attempt runs, under the same ownership gate and the same trampoline as every
// other trigger. Re-arming rather than bypassing keeps ONE path to the store; the sweep
// interval is the retry cadence, which is the right one for a failure that is by definition
// not transient-looking.
//
// Runs from the periodic sweep goroutine, for sessions that are not unresolved (an
// unresolved one is the resolver's, and its own releases re-check afterwards). No lock held
// on entry.
func (as *mqttAccountSessionManager) flushVoidVerdictGap(sess *mqttSession) {
	sess.mu.Lock()
	// The pending condition minus the watermark: that is the conjunct being re-armed.
	//
	// Minus the WAITER conjunct too, deliberately, and that is load-bearing: re-arming
	// while a blocking writer is announced for the fence is exactly what lets that
	// waiter's eventual release trigger the handoff. Completing the predicate here —
	// adding recordTxWaiters.Load() == 0 to owed — would leave the watermark burned, so
	// the release finds nothing above it and the retry waits a whole further sweep
	// interval, lengthening the memory-only crash window for no gain. The attempt this
	// re-arm drives still tests the waiter conjunct, so nothing barges.
	owed := !sess.seqUncertain && sess.c == nil && !sess.recordTxOpen && !sess.voidHandoffActive &&
		sess.voidedRegsRev > sess.voidedRegsSavedRev
	if owed {
		sess.voidedRegsHandoffRev = sess.voidedRegsSavedRev
	}
	sess.mu.Unlock()
	if !owed {
		return
	}
	sess.attemptVoidVerdictHandoff(nil)
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

// saveClassifiedDetail is saveClassified plus the attempted write's raw
// identity, for the one caller that must turn a DEFINITE CAS rejection into a
// decision by kind — 10071 (taken over) versus 10164 (contention) — and retry
// or read back the exact bytes it sent. save() itself re-formats the store
// error and destroys the kind, which is why the detail is captured at the
// boundary. See sess_resume_reconcile.go.
func (sess *mqttSession) saveClassifiedDetail(tok recordTx) (saveOutcome, mqttSessSaveDetail, error) {
	wasUncertain := sess.isSeqUncertain()
	detail, err := sess.saveWithDetail(tok)
	return sess.classifySave(wasUncertain, err), detail, err
}
