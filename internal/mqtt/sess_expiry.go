// Copyright 2020-2026 The NATS Authors
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

// Session expiry: the client-side expiry and lifetime helpers, remote expiry reconciliation,
// expiry scheduling and firing, the pending-discard worker, and the periodic expiry sweep.

package mqtt

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"
)

// mqttSessionExpiry reads THIS connection's effective Session Expiry Interval —
// deliberately its own copy and not the session's, which a replacement
// connection may already have overwritten (see mqtt.sessionExpiry). 0 for a
// connection that never completed CONNECT, which also means "the session ends
// when this connection closes", so its Will is due immediately: the right
// default.
//
// No lock held on entry.
func (c *client) mqttSessionExpiry() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mqtt == nil {
		return 0
	}
	return c.mqtt.sessionExpiry
}

// mqttSessionIsCleanForExpiry reports whether a session with this Session Expiry
// Interval ends at connection close. It is the ONE definition of the derivation
// Spec5 [3.1.2.11.2] fixes: clean and the interval are one fact, not two, and a
// writer that moves one without the other leaves a record whose resume decision
// and whose expiry sweep disagree (F-041).
func mqttSessionIsCleanForExpiry(expiry uint32) bool { return expiry == 0 }

// mqttLifetimeOverride reads the lifetime a v5 DISCONNECT on THIS connection
// instructed the session to end on, or nil when it carried no Session Expiry
// Interval property. See mqtt.lifetimeOverride: it is an obligation of an
// accepted connection, and deliberately not the same fact as sessionExpiry
// above, which also carries a CONNECT's merely REQUESTED terms.
//
// No lock held on entry.
func (c *client) mqttLifetimeOverride() *uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mqtt == nil {
		return nil
	}
	return c.mqtt.lifetimeOverride
}

// reconcileRemoteSessionExpiry reconciles this replica's local session-expiry
// timer from the authoritative persisted record after the session-record watch
// observed a remote session persist for a client this replica does not own
// locally. It runs on a spawned, admitted worker (never inline in the delivery
// callback) because loadSessionMsg is a blocking JSA request.
//
// st is the *sessionReconcileState the callback incremented for idHash; it is
// passed in (not re-fetched) so the inFlight decrement targets exactly that entry.
// observedSeq is the PubAck sequence the callback saw. All timer decisions are made
// atomically under as.mu: a newer observation (watermark) or a live local session
// (ownership) makes this worker leave the timer untouched, because expireSessionFired's
// in-memory path does not re-validate the deadline and could otherwise clear a
// newer generation early. Spec5 [3.1.2.11.2].
//
// No lock held on entry.
func (as *mqttAccountSessionManager) reconcileRemoteSessionExpiry(s *Server, idHash string, observedSeq uint64, st *sessionReconcileState) {
	// Lifecycle-tied prune: decrement the exact state the callback incremented, then
	// prune through the ONE helper. Deleting on `inFlight == 0` alone — which this
	// path used to do — is what let a no-local worker erase a RESIDENT session's
	// undecided eviction trigger, since the two kinds share one entry. Runs on every
	// exit path, so a worker parked before mu keeps its own entry alive and can never
	// resume into a pruned-away watermark. See pruneReconcileEntryLocked.
	defer func() {
		as.mu.Lock()
		if st.inFlight > 0 {
			st.inFlight--
		}
		as.pruneReconcileEntryLocked(idHash, st)
		as.mu.Unlock()
	}()

	// Re-read the authoritative record. Missing/undecodable leaves nothing to
	// reconcile (mirrors loadExpiringSessionRecord's warning behavior).
	smsg, err := as.jsa.loadSessionMsg(as.domainTk, idHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			s.Warnf("MQTT remote session-expiry reconciliation: unable to load session record: %v", err)
		}
		return
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		s.Warnf("MQTT remote session-expiry reconciliation: unable to decode session record at sequence %v: %v", smsg.Sequence, err)
		return
	}
	// Defensive: never arm from a snapshot older than the write we observed. In
	// practice loadSessionMsg returns the subject's last message, whose sequence is
	// >= observedSeq unless the record was deleted (handled above as not-found), so
	// this is a guard rather than a live path.
	if smsg.Sequence < observedSeq {
		return
	}

	// Test seam: park after the load, before the decision lock, so worker-ordering
	// tests can race a newer worker or a local reconnect/re-disconnect ahead.
	as.mu.RLock()
	pause := as.testReconcilePause
	as.mu.RUnlock()
	if pause != nil {
		pause(idHash)
	}

	as.mu.Lock()
	defer as.mu.Unlock()
	// Shutdown: do not install new timers.
	if as.pendingExpiries == nil {
		return
	}
	// Worker-ordering guard: a newer observation for this client supersedes us. Equal
	// is fine — a duplicate observation applies the same decision idempotently.
	if observedSeq < st.latest {
		return
	}
	// Ownership guard: a local session appeared after the callback handed work off (a
	// reconnect, possibly followed by a local re-disconnect that already armed the
	// correct later timer). Its lifecycle owns the timer; leave it untouched.
	if _, ok := as.sessByHash[idHash]; ok {
		return
	}
	// Decide from the authoritative record.
	if ps.DisconnectedAt == 0 || ps.ExpiryInterval == 0 || ps.ExpiryInterval == mqttSessionNeverExpire {
		// Connected, zero-expiry, or never-expire: nothing should expire this
		// session; drop any local timer armed from a superseded record.
		as.cancelSessionExpiryLocked(idHash)
		return
	}
	if remaining := time.Until(mqttSessionExpiryDeadline(ps.DisconnectedAt, ps.ExpiryInterval)); remaining > 0 {
		as.scheduleSessionExpiryLocked(s, idHash, remaining)
		return
	}
	// Already due: arm a 0-delay timer that fires on its own goroutine into the
	// record-based cleanup path (its own connecting guard + sequence-targeted
	// delete). Do not call expireSessionFromRecord inline — it would re-enter mu and
	// issue JSA requests under the lock.
	as.scheduleSessionExpiryLocked(s, idHash, 0)
}

// mqttSessionExpiryDeadline is the first instant at which a finite Session Expiry Interval is
// PROVEN to have elapsed since the disconnect: the stamp is whole Unix seconds, so the
// disconnect itself lies somewhere in [stamp, stamp+1), and stamp+interval would end a session
// up to a second before its interval has passed. One second later than that is never early,
// and at most a second late measured from the sampled stamp. Spec5 [3.1.2.11.2]. Every
// decision and every timer that dates a session's expiry from its stamp asks this; none adds
// the interval to the stamp itself.
func mqttSessionExpiryDeadline(disconnectedAt int64, interval uint32) time.Time {
	return time.Unix(disconnectedAt+int64(interval)+1, 0)
}

// mqttSessionOverdue reports whether a disconnected session's finite interval has elapsed at
// now. A connected session (no stamp), a zero interval (ended at the close by its own route)
// and a session that never expires are never overdue here.
func mqttSessionOverdue(disconnectedAt int64, interval uint32, now time.Time) bool {
	return disconnectedAt > 0 && interval > 0 && interval != mqttSessionNeverExpire &&
		!now.Before(mqttSessionExpiryDeadline(disconnectedAt, interval))
}

// mqttTestExpiryNotDueHookV observes expireSessionFired returning on a deadline that has not
// passed, by client ID hash. Nil in production.
var mqttTestExpiryNotDueHookV atomic.Pointer[func(idHash string)]

// scheduleSessionExpiry arms a timer to clean up a disconnected client's session
// once its MQTT 5.0 Session Expiry Interval elapses. The timer is keyed by the
// client ID hash (the same token used in the session-persist stream) so both a
// local reconnect and an observed remote record write can cancel it via
// cancelSessionExpiry / cancelSessionExpiryLocked. Spec5 [3.1.2.11.2].
//
// Deadlines survive a restart via the persisted disconnect time and
// sweepSessionExpiries. A surviving replica also re-arms the timer when it
// observes the persisted disconnect: the session-record watch reconciles the
// deadline for a session it does not own locally, so the session no longer
// lingers until a reconnect or the periodic sweep if the owning broker dies.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) scheduleSessionExpiry(s *Server, idHash string, delay time.Duration) {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.scheduleSessionExpiryLocked(s, idHash, delay)
}

// scheduleSessionExpiryLocked is scheduleSessionExpiry for callers already holding
// as.mu (e.g. the remote-expiry reconciliation worker, which must decide and arm
// atomically under the lock). Spec5 [3.1.2.11.2].
func (as *mqttAccountSessionManager) scheduleSessionExpiryLocked(s *Server, idHash string, delay time.Duration) {
	// nil map => account manager is shutting down; do not schedule.
	if as.pendingExpiries == nil {
		return
	}
	// Defensive: replace any existing timer for this client ID hash.
	if old, ok := as.pendingExpiries[idHash]; ok {
		old.timer.Stop()
	}
	// The callback captures ITS OWN INSTALL TOKEN, so expireSessionFired can prove the fire
	// it is servicing is the one still installed. Without that, a callback that already
	// fired and then lost as.mu to a cancel-and-re-arm would consume the REPLACEMENT's
	// entry and act on whatever session is current by then — a stale expiry aliased
	// onto a fresh incarnation (F-036 unit 1).
	//
	// A token rather than the *time.Timer the closure would otherwise have to read:
	// AfterFunc can fire before its own assignment completes, so capturing the timer
	// variable is a data race on the install itself.
	token := as.nextExpiryToken()
	as.pendingExpiries[idHash] = mqttPendingExpiry{
		timer: time.AfterFunc(delay, func() { as.expireSessionFired(s, idHash, token) }),
		token: token,
	}
}

// mqttPendingExpiry is one armed session-expiry timer plus the INSTALL TOKEN its
// callback captured. The token is what lets a fired callback prove the entry it
// finds under its client ID hash is its own: the map is keyed by hash, and a
// reconnect-and-redisconnect replaces the entry, so presence alone would let a
// stale callback consume the replacement's and expire a session it never named
// (F-036 unit 1).
type mqttPendingExpiry struct {
	timer *time.Timer
	token uint64
}

// nextExpiryToken mints an install token. Monotonic per account manager and never
// reused, so no two installs can be confused for one another.
//
// as.mu must be held.
func (as *mqttAccountSessionManager) nextExpiryToken() uint64 {
	as.expiryTokens++
	return as.expiryTokens
}

// cancelSessionExpiry stops and removes a pending session-expiry timer for the
// given client ID hash, if any. Called when a new connection resumes the session
// before the interval elapses. Returns true if one was cancelled. Idempotent.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) cancelSessionExpiry(idHash string) bool {
	as.mu.Lock()
	defer as.mu.Unlock()
	return as.cancelSessionExpiryLocked(idHash)
}

// cancelSessionExpiryLocked is cancelSessionExpiry for callers already holding
// as.mu (e.g. the session-persist callback, which cancels on an observed remote
// record write — the record moved, whoever moved it).
func (as *mqttAccountSessionManager) cancelSessionExpiryLocked(idHash string) bool {
	t, ok := as.pendingExpiries[idHash]
	if !ok {
		return false
	}
	t.timer.Stop()
	delete(as.pendingExpiries, idHash)
	return true
}

// expireSessionFired removes and clears a session whose Session Expiry Interval has
// elapsed, unless it was cancelled (reconnect/takeover) or the account manager is
// shutting down, through the same removeSession + clear path as a clean disconnect;
// a session not in the in-memory maps is re-verified against the persisted record
// first. Spec5 [3.1.2.11.2]. It carries the install token of the timer that fired so
// the identity check can prove it is still the installed one (F-036 unit 1). A zero
// token is a direct caller and skips the check.
func (as *mqttAccountSessionManager) expireSessionFired(s *Server, idHash string, fired uint64) {
	as.mu.Lock()
	// nil map => account manager shutdown; the entry (if any) is being torn down.
	if as.pendingExpiries == nil {
		as.mu.Unlock()
		return
	}
	t, ok := as.pendingExpiries[idHash]
	if !ok {
		// Cancelled between the timer firing and acquiring the lock.
		as.mu.Unlock()
		return
	}
	// IDENTITY, not just presence: the map is keyed by client ID hash and a
	// reconnect-and-redisconnect replaces the entry, so an already-fired callback
	// that lost this lock would otherwise consume the REPLACEMENT's entry. Compared
	// and deleted in one critical section (F-036 unit 1). A zero token is a direct
	// caller and keeps the presence-only behaviour.
	//
	// This is a narrowing, NOT the whole guard: a callback that passes here can still
	// be delayed past a reconnect and a later disconnect, and what stops it acting on
	// that newer deadline is the re-read below, under the gate.
	if fired != 0 && t.token != fired {
		as.mu.Unlock()
		return
	}
	// We own this expiry now; drop the timer entry so a concurrent reconnect's
	// cancelSessionExpiry becomes a no-op.
	t.timer.Stop()
	delete(as.pendingExpiries, idHash)
	sess, inMem := as.sessByHash[idHash]
	as.mu.Unlock()
	if !inMem {
		as.expireSessionFromRecord(s, idHash)
		return
	}

	// ACQUIRE BEFORE THE GATE (F-036 unit 1). The lease proves this fire is acting
	// for the incarnation it resolved, and taking it first is what keeps a fire
	// against a RETIRING incarnation from holding the client-ID gate while a
	// retirement waits: a refused acquisition re-arms and returns without ever
	// touching the gate. tryAcquire never parks, for the same reason.
	//
	// The tracker pointer is READ UNDER sess.mu: setIncarnationLocked replaces it
	// there, so sampling it unlocked is a data race with a concurrent re-mint — and
	// would defeat the very boundary this acquisition is.
	sess.mu.Lock()
	tr := sess.leases
	sess.mu.Unlock()
	selfLease, ok := tr.acquire()
	if !ok {
		// The incarnation is retiring or retired: this expiry is not its business.
		//
		// Re-arm ONLY if this fire's own view is still current — the session object
		// is still the resident one AND its tracker is still the one we captured, so
		// no re-mint happened underneath us — and only if no timer is installed. A
		// bare re-arm here is a fresh session's data loss, not a nicety: a callback
		// descheduled across a clean-start re-mint would otherwise REPLACE the new
		// incarnation's legitimate deadline with a one-second one, and the retry would
		// then expire a session that had just connected. Where the check refuses, the
		// deadline is not lost: the periodic sweep re-arms a finite expiry from the
		// persisted record, and a reopened incumbent arms its own on its next close.
		as.rearmExpiryIfViewCurrent(s, idHash, sess, tr)
		return
	}
	// DEFERRED IMMEDIATELY, before any exit below can be taken: an owner that leaks
	// its lease leaves the incarnation permanently undrainable, so every later
	// retirement of this session would refuse. Promotion makes this idempotent, so
	// the retirement path below is unaffected.
	defer selfLease.release()
	// Guard against a concurrent reconnect: lockSession(sess, nil) fails if a
	// CONNECT is mid-processing or a client is bound — the reconnect then owns it.
	if err := as.lockSession(sess, nil); err != nil {
		return
	}
	// Re-check that no client re-bound before we acquired the session lock. The
	// binding is the ownership gate's business, which this path holds, so it is
	// decided here rather than inside the record transaction below.
	sess.mu.Lock()
	rebound := sess.c != nil
	sess.mu.Unlock()
	if rebound {
		as.unlockSession(sess)
		return
	}
	// If the persisted record was bumped past our sequence, our in-memory copy is
	// stale: a resume (and possibly a re-disconnect) happened on another server and
	// its best-effort cancel to this server may have been lost. Do NOT act on our
	// stale state — drop the local copy and let the record-based path decide from
	// the authoritative record: it skips a still-connected or not-yet-due session
	// (re-arming as needed) and cleans up a due one using the record's own sequence
	// and consumers. A record that is not newer (or absent from a failed
	// best-effort disconnect save) leaves our in-memory view authoritative, so the
	// fired timer still cleans up and finite expiry is never silently skipped.
	// Spec5 [3.1.2.11.2].
	//
	// A higher record sequence is NOT evidence of a remote owner while this session's
	// own write is unresolved: an ambiguous write that actually LANDED with its
	// response lost leaves sess.seq stale and the record's sequence higher, so this
	// condition would be true for our own write and would evict the object — with its
	// pendingWrite, its retained continuations and its staged void-register verdicts —
	// before clear() could refuse. Sequence is not identity (see sess_uncertain.go,
	// which classifies by DIGEST); so while unresolved, fall through to the clear()
	// attempt, which refuses with the sentinel and parks the discard, and let
	// resolution decide ours-versus-foreign with the test that can actually tell them
	// apart. A foreign verdict evicts and drops the obligation; an "ours" verdict
	// adopts the landed sequence and discharges the discard against it.
	//
	// ONE uninterrupted record transaction covers the load, the sess.seq read and the
	// decision. A mutex sample alone is check-then-act — the fence is what serializes
	// record writers — and arming uncertainty requires a save, which requires this
	// fence, so nothing can change the decision's inputs underneath it. The seq read
	// belongs INSIDE for the same reason: a save landing between an outside sample and
	// the fence advances the real sequence and yields a false "superseded". See
	// sess_recordtx.go.
	tok := sess.beginRecordTx()
	var loadedSeq uint64
	if smsg, err := as.jsa.loadSessionMsg(as.domainTk, idHash); err == nil {
		loadedSeq = smsg.Sequence
	}
	sess.mu.Lock()
	seq := sess.seq
	uncertain := sess.seqUncertain
	sess.mu.Unlock()
	superseded := !uncertain && loadedSeq > seq
	// Test seam, fired with the fence still HELD and the load already done, carrying the
	// sequences the decision was made from: that pair is what states "one uninterrupted
	// transaction" as an assertion instead of a race.
	if h := mqttTestExpireDecisionHookV.Load(); h != nil {
		(*h)(sess, loadedSeq, seq, superseded)
	}
	// Released WITHOUT ending the generation — nothing was mutated — and before clear(),
	// which opens a transaction of its own because it ends the generation. An ambiguity
	// armed in that gap lands on clear()'s entry or in-fence refusal, so both branches
	// converge on the sentinel and the recovery below.
	if terr := sess.endRecordTx(tok); terr != nil {
		s.Errorf("MQTT: %v", terr)
	}
	if superseded {
		// Our copy is stale: drop it and hand off to the record-based path, which
		// guards against a concurrent CONNECT itself (its connecting check and a
		// sequence-CAS'd delete), so we release the session lock before delegating.
		as.removeSession(sess, true)
		as.unlockSession(sess)
		as.expireSessionFromRecord(s, idHash)
		return
	}

	// THE DEADLINE IS RE-READ HERE, and the placement is the point: past the
	// superseded handoff above, this object's own copy IS the authority, so refusing
	// on it cannot swallow a record-driven decision. A callback that passed its token
	// check can still be delayed while the client reconnects and disconnects again on
	// this broker: the reconnect resumes this same object and saves, so sess.seq
	// adopts the new sequence and nothing above distinguishes the newer incarnation of
	// the deadline from the one this fire was armed for. Expiring it then would
	// destroy a session whose lifetime has not elapsed, its subscriptions and queued
	// QoS state with it. The token identity narrows that window; the re-read closes it
	// (F-036 unit 1).
	sess.mu.Lock()
	notDue := sess.expiryInterval > 0 && sess.expiryInterval != mqttSessionNeverExpire &&
		sess.disconnectedAt > 0 &&
		time.Now().Before(mqttSessionExpiryDeadline(sess.disconnectedAt, sess.expiryInterval))
	sess.mu.Unlock()
	if notDue {
		if h := mqttTestExpiryNotDueHookV.Load(); h != nil {
			(*h)(idHash)
		}
		as.unlockSession(sess)
		return
	}

	// Authoritative cleanup: hold the session lock through clear() so a concurrent
	// CONNECT cannot restore the (possibly stale) persisted record and race the
	// teardown. Delete the session state (consumers, record), matching the
	// clean-disconnect path in mqttHandleClosedClient.
	defer as.unlockSession(sess)
	as.removeSession(sess, true)
	// This fire's own lease is PROMOTED by the retirement below, so the drain does
	// not wait on the caller performing it; the deferred release above then becomes
	// an idempotent no-op (F-036 unit 1).
	if err := sess.clearInner(true, selfLease, nil, nil); err != nil {
		if errors.Is(err, errMQTTClearDrainTimeout) {
			// An admitted route or a queued effect of this incarnation is still
			// outstanding, so nothing was deleted and the object is admissible again
			// (the tracker was reopened). Restore residency and re-arm: the deadline
			// is still owed and a later fire retries the whole discard.
			as.addSession(sess, true)
			as.scheduleSessionExpiry(s, idHash, mqttExpiryRetryAfterRetiring)
			s.Warnf("MQTT session expiry for %q deferred until its incarnation drains", sess.id)
			return
		}
		// The refusal deleted nothing, and the evicted object would take the unresolved
		// write's pendingWrite, its retained continuations and its staged void-register
		// verdicts with it — all of them lost outright, since only this object can classify
		// that write. This path has NO early uncertainty check, so the write is often
		// already unresolved when the timer fires rather than going ambiguous under us;
		// either way the sentinel says nothing was deleted, which is what makes the discard
		// recoverable. Park it with the deadline the client actually earned and restore
		// residency; the deferred as.unlockSession above then releases the gate and runs the
		// void-verdict handoff for a session that is resident again. Keyed on the SENTINEL
		// alone. See reparkRefusedDiscard.
		// TERMINAL on a superseded teardown, in its own branch ahead of the recovery:
		// the base this expiry claimed is not current, so the record it was going to
		// expire is not the one it can speak for and there is nothing left to expire. A
		// normal outcome, so it is not warned about — and not re-parked, which would
		// re-arm an expiry deadline against a record this broker may no longer write.
		if errors.Is(err, errMQTTClearSuperseded) {
			s.Debugf("MQTT session expiry for %q stopped: %v", sess.id, err)
			return
		}
		sess.mu.Lock()
		disconnectedAt := sess.disconnectedAt
		sess.mu.Unlock()
		if errors.Is(err, errMQTTClearRefusedUnresolved) && s.reparkRefusedDiscard(as, sess, disconnectedAt) {
			s.Warnf("MQTT session expiry for %q found the record state unresolved; the discard stays parked until it is classified", sess.id)
			return
		}
		s.Warnf("MQTT session expiry cleanup for %q failed: %v", sess.id, err)
	}
}

// pendingDiscardFinisher builds the finisher for a clean-session discard whose drain
// ceiling fired (F-036 unit 1). It is invoked by the eagerly-admitted worker once the
// captured close cycle has drained.
//
// It RETRIES the client-ID gate until it claims the obligation or sees another
// claimant consume it. A failed acquisition does not mean a CONNECT will finish this:
// the close path that parked the obligation holds the SAME gate across its own
// clearInner, so the common case is meeting the gate held by the parker. Returning
// there would strand a discard [MQTT-3.1.2-6] requires and no sweep reaches — the
// expiry sweep skips expiry-zero records. The loop holds no lock between attempts. Its quitCh
// exit is for a server stopped without the lifecycle; during shutdown quitCh closes only after
// Phase 3 has waited for this worker's WaitGroup, so there the loop ends when lockSession
// succeeds once Phase 1 has drained the clients, or when the obligation is gone.
func (s *Server) pendingDiscardFinisher(asm *mqttAccountSessionManager,
	sess *mqttSession, captured *mqttIncarnationTracker) func() {

	return func() {
		for {
			if !captured.isRetirementPending() {
				return
			}
			if err := asm.lockSession(sess, nil); err == nil {
				s.finishPendingDiscard(asm, sess, captured)
				asm.unlockSession(sess)
				return
			}
			select {
			case <-time.After(mqttPendingDiscardRetryItvl.Get()):
			case <-s.quitCh:
				return
			}
		}
	}
}

// mqttPendingDiscardRetryItvl is how long the pending-discard finisher waits before
// re-attempting the client-ID gate. Short, because the common case is the parking
// caller's own gate hold, which ends within one bounded teardown.
var mqttPendingDiscardRetryItvl = newMQTTKnob(25 * time.Millisecond)

// finishPendingDiscard claims a parked retirement-pending discard and completes it.
//
// THE CLAIM IS IDENTITY-CHECKED, and the identity is the PAIR {session object, its
// tracker} rather than the client ID. A client-ID claim is not enough: while this
// finisher waited, an uncertainty resolver could evict this object as foreign and a
// CONNECT could install a REPLACEMENT session under the same ID — and completing the
// discard against the stale object would then remove the replacement's map entries
// and leave its live connection bound to a session nothing can resolve. So both
// manager indexes must still name THIS object and the object must still carry the
// tracker whose obligation this is; otherwise the discard is terminally superseded
// and there is nothing left to complete. The final eviction is
// removeSessionIfCurrent for the same reason.
//
// Caller holds the client-ID gate.
func (s *Server) finishPendingDiscard(asm *mqttAccountSessionManager, sess *mqttSession,
	captured *mqttIncarnationTracker) mqttPendingDiscardOutcome {

	sess.mu.Lock()
	current := sess.leases
	sess.mu.Unlock()
	if current != captured {
		return mqttPendingDiscardSuperseded
	}
	asm.mu.Lock()
	resident := asm.sessions[sess.id] == sess && asm.sessByHash[sess.idHash] == sess
	asm.mu.Unlock()
	if !resident {
		// Superseded: this object is no longer the client ID's session, so its
		// discard is not ours to make and nothing here was ours to delete.
		if _, ok := captured.takeRetirementPending(); ok {
			s.Debugf("MQTT: the parked discard of session %q is superseded: the object "+
				"is no longer resident", sess.id)
			// THE EVICTED OBJECT'S RETIREMENT IS DECIDED, and nothing else will decide it
			// (F-105 8a). It was evicted — a foreign record found by the uncertainty resolution
			// before this CONNECT's join could run — and will never reach destroy, whose defer
			// is the only other resolution, so a handler parked on the tracker's decision would
			// wait forever. Resolving here declares no owner drained: every path into this
			// function arrives only after the tracker's drained signal (the retirement worker
			// waits on it; the CONNECT join calls this only after it joined).
			captured.resolveRetired()
		}
		return mqttPendingDiscardSuperseded
	}
	if _, ok := captured.takeRetirementPending(); !ok {
		// Another claimant took it; whatever it decided is that claimant's outcome,
		// and this one has nothing left to do.
		return mqttPendingDiscardSuperseded
	}
	p, err := sess.prepareClear()
	if err != nil {
		// The uncertainty refusal: the discard is still owed but cannot run on an
		// unclassified write. Hand the obligation to the EXISTING uncertainty
		// machinery — reparkRefusedDiscard restores residency and parks a
		// discard-only deferred action whose resolution re-drives clear(); the
		// tracker stays CLOSING, so a resume attempt keeps refusing until then.
		if !s.reparkRefusedDiscard(asm, sess, time.Now().Unix()) {
			s.Errorf("MQTT: pending discard of session %q could not be re-parked: %v", sess.id, err)
		}
		// STILL OWED, and the object is still the resident one carrying the
		// obligation and the unclassified write. A caller that went on to restore
		// over it would orphan both.
		return mqttPendingDiscardReparked
	}
	sess.mu.Lock()
	permit, perr := sess.retireIncarnationLocked()
	sess.mu.Unlock()
	if perr != nil {
		p.abort()
		s.Errorf("MQTT: pending discard of session %q: %v", sess.id, perr)
		return mqttPendingDiscardReparked
	}
	if derr := p.destroy(permit, true); derr != nil && !errors.Is(derr, errMQTTClearSuperseded) {
		s.Warnf("MQTT: pending discard of session %q: %v", sess.id, derr)
	}
	asm.removeSessionIfCurrent(sess)
	return mqttPendingDiscardCompleted
}

// mqttPendingDiscardOutcome is what a claim of a parked retirement-pending discard
// resolved to. The distinction matters to a JOINING CONNECT: a REPARKED outcome means
// the object is still resident and still carries both the obligation and the
// unclassified write that refused it, so restoring over it would orphan them — and
// for a session whose disconnect required a discard, could report Session Present and
// resume state the client asked to have destroyed.
type mqttPendingDiscardOutcome int

const (
	// mqttPendingDiscardCompleted: the discard ran; the session is gone.
	mqttPendingDiscardCompleted mqttPendingDiscardOutcome = iota
	// mqttPendingDiscardSuperseded: this object is no longer the client ID's
	// session, or another claimant owns the obligation. Nothing was ours to delete.
	mqttPendingDiscardSuperseded
	// mqttPendingDiscardReparked: still owed, and the object must be left alone.
	mqttPendingDiscardReparked
)

// mqttTestExpireDecisionHookV fires inside expireSessionFired's record transaction, immediately
// after the superseded decision is computed and while the fence is still held. See
// expireSessionFired.
var mqttTestExpireDecisionHookV atomic.Pointer[func(sess *mqttSession, loadedSeq, seq uint64, superseded bool)]

// loadExpiringSessionRecord loads and decodes the persisted record for idHash
// and reports whether its Session Expiry deadline has elapsed (due). A resume
// anywhere rewrites the record with DisconnectedAt=0, so a stale timer reading it
// finds nothing due — the record is authoritative. If the deadline has not yet
// passed (a newer disconnect refreshed it) it re-arms the timer and returns
// due=false. Returns the decoded record and its stream sequence for the caller.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) loadExpiringSessionRecord(s *Server, idHash string) (ps *mqttPersistedSession, seq uint64, due bool) {
	smsg, err := as.jsa.loadSessionMsg(as.domainTk, idHash)
	if err != nil {
		// Not found => the session was cleared or resumed-then-cleaned; done.
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			s.Warnf("MQTT session expiry: unable to load session record: %v", err)
		}
		return nil, 0, false
	}
	ps = &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		s.Warnf("MQTT session expiry: unable to decode session record at sequence %v: %v", smsg.Sequence, err)
		return nil, 0, false
	}
	// A connected client (or a session that no longer expires) leaves nothing to do.
	if ps.DisconnectedAt == 0 || ps.ExpiryInterval == 0 || ps.ExpiryInterval == mqttSessionNeverExpire {
		return ps, smsg.Sequence, false
	}
	// Not due yet (e.g. the record was refreshed by a newer disconnect after this
	// timer was armed): re-arm for the remainder instead of dropping the cleanup.
	if remaining := time.Until(mqttSessionExpiryDeadline(ps.DisconnectedAt, ps.ExpiryInterval)); remaining > 0 {
		as.scheduleSessionExpiry(s, idHash, remaining)
		return ps, smsg.Sequence, false
	}
	return ps, smsg.Sequence, true
}

// expireSessionFromRecord handles an expiry timer firing for a session not in
// the in-memory maps (re-armed by the startup sweep). The persisted record is
// authoritative: a reconnect anywhere rewrites it with a zero disconnect time,
// so re-reading it right before acting keeps a stale timer harmless.
//
// No lock held on entry.
// mqttTestExpiryFireHookV, when it returns false for an idHash, parks that session's expiry
// before it reads or tears anything down. nil in production.
var mqttTestExpiryFireHookV atomic.Pointer[func(idHash string) bool]

func (as *mqttAccountSessionManager) expireSessionFromRecord(s *Server, idHash string) {
	if h := mqttTestExpiryFireHookV.Load(); h != nil && !(*h)(idHash) {
		return
	}
	ps, seq, due := as.loadExpiringSessionRecord(s, idHash)
	if !due {
		return
	}
	// Skip if a CONNECT for this client ID is being processed on this server; its
	// save will reset the disconnect time (and the record delete below is CAS-ed
	// on the sequence we just read, so a concurrent remote resume wins).
	as.mu.Lock()
	_, connecting := as.sessLocked[ps.ID]
	as.mu.Unlock()
	if connecting {
		return
	}

	// A claimed teardown is FINISHED, never re-claimed. Unlike clear() — whose CAS
	// against a stale in-memory sequence is itself the guard — this path reads the
	// record fresh, so its claim WOULD succeed and would overwrite a peer's marker
	// with our nonce, leaving the peer unable to recognize its own release. See
	// sess_teardown.go.
	// Same rule as the CONNECT restore: nothing may build a name or a purge filter
	// from an unvalidated stored generation. See mqttSessGenFromRecord.
	recGen, gerr := mqttSessGenFromRecord(ps.Gen)
	if gerr != nil {
		s.Warnf("MQTT session expiry: refusing to act on %q: %v", ps.ID, gerr)
		return
	}
	ps.Gen = recGen
	// Same rule again, and for the sharper reason: this path CLAIMS the record and
	// deletes it. A shared QoS 2 membership with no affine generation is a record
	// the broker cannot interpret, and expiring it would destroy the state an
	// assignment owed to a started delivery is arbitrated from.
	// See mqttValidateRecordAffinePair.
	if aerr := mqttValidateRecordAffinePair(ps, idHash); aerr != nil {
		s.Warnf("MQTT session expiry: refusing to act on %q: %v", ps.ID, aerr)
		return
	}
	// And a shared group consumer in Cons, for the same reason as the affine pair above but
	// one step sharper: this path builds a transient session from ps and hands it to
	// clear(), which walks Cons. A record whose shape this broker does not write is not one
	// it may claim, rewrite or delete. See mqttSharedConsInRecord.
	if sid, found := mqttSharedConsInRecord(ps); found {
		s.Warnf("MQTT session expiry: refusing to act on %q: %v", ps.ID, mqttSharedConsRecordErr(ps.ID, sid))
		return
	}

	if ps.Teardown != nil {
		if err := as.finishAdoptedTeardown(ps, idHash, seq, s.getOpts()); err != nil {
			// TERMINAL on a superseded adoption, and explicitly so: a newer record holds
			// the subject, which means this expiry has nothing to expire. The historical
			// nil return already behaved this way; what changes is only that the outcome
			// is now named, so it must not fall into the warning arm below.
			if errors.Is(err, errMQTTTeardownAdoptionSuperseded) {
				s.Debugf("MQTT session expiry: the claimed teardown of %q was superseded; nothing was left to finish", ps.ID)
				return
			}
			s.Warnf("MQTT session expiry: finishing the claimed teardown of %q failed: %v", ps.ID, err)
		}
		return
	}

	// Build a transient session from the record — enough state for clear() to
	// delete the consumers and the record itself — and clean up.
	sess := mqttSessionCreate(&as.jsa, ps.ID, idHash, ps.Gen, seq, ps.WID, ps.PWID, s.getOpts())
	sess.asm = as
	sess.domainTk = as.domainTk
	sess.clean = ps.Clean
	sess.subs = ps.Subs
	sess.sharedSubs = ps.SharedSubs
	sess.cons = ps.Cons
	// The restored lists came OUT of the record, so they are already durable there; only
	// a later change reopens the gap.
	sess.voidedRegNonces = mqttNonceSet(ps.VoidedRegs)
	sess.acqReleases = mqttAcqReleaseSet(ps.AcqReleases)
	sess.voidedRegsRev, sess.voidedRegsSavedRev, sess.voidedRegsHandoffRev = 0, 0, 0
	sess.restoreDrainsFromRecord(ps)
	sess.pubRelConsumer = ps.PubRel
	sess.q2AffConsumer = ps.Q2Aff
	sess.q2AffGen = ps.Q2AffGen
	// clear()'s late-uncertainty refusal is UNREACHABLE here, so no recovery is written
	// for it: seqUncertain is armed only by a failed save on this very object
	// (saveWithDetail), and this object is transient — mqttSessionCreate starts no
	// goroutine, it is registered nowhere, and nothing writes through it before the
	// clear() below. If that proof were ever wrong the refusal is still fail-closed:
	// the session is not destroyed on a sequence nobody classified, only left for the
	// next sweep.
	if err := sess.clear(true); err != nil {
		// TERMINAL on a superseded teardown: a newer record holds the subject, so this
		// record-driven expiry has nothing left to expire. Its own branch — a warning
		// here would report a failure on an outcome that is ordinary cross-replica
		// arbitration.
		if errors.Is(err, errMQTTClearSuperseded) {
			s.Debugf("MQTT session expiry for %q stopped: %v", ps.ID, err)
			return
		}
		s.Warnf("MQTT session expiry cleanup for %q failed: %v", ps.ID, err)
	}
}

// sweepSessionExpiries re-arms expiry timers from the persisted session records,
// so deadlines survive a restart; overdue sessions get an immediate timer.
// expireSessionFired re-verifies each record before deleting anything, so racing a
// reconnect or another server's sweep is safe. Runs from a go routine at account
// session manager creation (the first MQTT connect for the account after a
// restart) and then periodically. Spec5 [3.1.2.11.2].
//
// This is fallback and startup recovery, not the normal cross-replica handoff:
// when a broker dies with the cluster still running, the session-record watch's
// remote-expiry reconciliation re-arms surviving replicas near the original
// deadline. The sweep still catches sessions no replica observed (e.g. a persist
// that landed before an account manager existed on any survivor).
//
// No lock held on entry.
func (as *mqttAccountSessionManager) sweepSessionExpiries(s *Server) {
	if h := mqttTestSessExpirySweepPassHookV.Load(); h != nil {
		defer (*h)(as)
	}
	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	var swept int
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			// No (more) messages, or the stream/server is going away: done.
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT session expiry sweep ended: %v", err)
			}
			break
		}
		seq = smsg.Sequence + 1
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil {
			s.Warnf("MQTT session expiry sweep: unable to decode session record at sequence %v, skipping: %v", smsg.Sequence, err)
			continue
		}
		// Only sessions with a finite interval can expire.
		if ps.ExpiryInterval == 0 || ps.ExpiryInterval == mqttSessionNeverExpire {
			continue
		}
		idHash := getHash(ps.ID)
		// A record this broker has said it cannot interpret is not one it may
		// REWRITE. The stamp below is a full record write, and arming a timer on it
		// schedules a claim and a delete of the same record — F-058's rule is that a
		// refused record is never claimed, published, cancelled or deleted. Skip it
		// whole: nothing reclaims it until the streams are recreated.
		// See mqttValidateRecordAffinePair.
		if aerr := mqttValidateRecordAffinePair(ps, idHash); aerr != nil {
			s.Warnf("MQTT session expiry sweep: refusing to act on the record of %q: %v", ps.ID, aerr)
			continue
		}
		// Same rule, same reason as the affine pair: the stamp below is a full record
		// write, and writing an unsupported shape NORMALIZES it — the record would come
		// back looking like something this broker authored. See mqttSharedConsInRecord.
		if sid, found := mqttSharedConsInRecord(ps); found {
			s.Warnf("MQTT session expiry sweep: refusing to act on the record of %q: %v",
				ps.ID, mqttSharedConsRecordErr(ps.ID, sid))
			continue
		}
		if ps.DisconnectedAt == 0 {
			// The record claims a connected client. If we own the record and
			// have no session for it in memory, the owner (us) went down while
			// the client was connected and its disconnect was never recorded:
			// stamp it as of now so the finite interval eventually applies.
			// The CAS write defers to any concurrent reconnect; an in-memory
			// session (bound or freshly disconnected) manages its own state.
			// The ownership gate applies only in a cluster (where every node
			// sweeps, and server names are explicit): standalone, a generated
			// node name changes across restarts and would never match.
			if s.JetStreamIsClustered() && ps.Origin != s.NodeName() {
				continue
			}
			as.mu.RLock()
			_, inMem := as.sessByHash[idHash]
			as.mu.RUnlock()
			if inMem {
				continue
			}
			ps.DisconnectedAt = time.Now().Unix()
			// A raw record write, so it carries a lineage like any other. ps was
			// decoded from the loaded record, so its CURRENT WID is this write's
			// parent — set PWID from it BEFORE minting the new one, or the write
			// parents itself. See sess_fork_lineage.go.
			ps.PWID, ps.WID = ps.WID, mqttNewRecordWID()
			if err := as.storeSessionRecordCAS(ps, idHash, smsg.Sequence); err != nil {
				continue
			}
		}
		remaining := time.Until(mqttSessionExpiryDeadline(ps.DisconnectedAt, ps.ExpiryInterval))
		if remaining < 0 {
			remaining = 0
		}
		// Arm only if absent: a timer armed by a live disconnect (or an earlier
		// sweep) is at least as up to date as this record snapshot.
		as.mu.Lock()
		if as.pendingExpiries != nil {
			if _, exists := as.pendingExpiries[idHash]; !exists {
				token := as.nextExpiryToken()
				as.pendingExpiries[idHash] = mqttPendingExpiry{
					timer: time.AfterFunc(remaining, func() { as.expireSessionFired(s, idHash, token) }),
					token: token,
				}
				swept++
			}
		}
		as.mu.Unlock()
	}
	if swept > 0 {
		s.Debugf("MQTT session expiry sweep re-armed %v timer(s)", swept)
	}
}

// armPendingDiscardWorker arms a retirement-pending discard and starts the ONE
// tracked worker that will complete it (F-036 unit 1).
//
// Admission is EAGER — taken here, in the goroutine that arms the obligation, before
// this returns — so the worker is inside the server's shutdown lifecycle by
// construction. A release then only closes the tracker's drained channel; it never
// schedules work, which is what removes the window where shutdown had already waited
// for its workers and a late release started a teardown behind it.
//
// Admission refusal is FAIL-CLOSED rather than hopeful: it cannot be recovered from
// the record, because a clean session's record carries expiry interval zero and the
// expiry sweep skips exactly those. The refusal arm therefore completes the discard
// SYNCHRONOUSLY on this goroutine once the tracker drains, bounded by the same
// ceiling. In production the refusal is unreachable — this path runs on the read loop
// and shutdown waits for the client WaitGroup before closing worker admission — and
// the arm exists so that ordering is not the only thing standing between a required
// discard and being dropped.
func (s *Server) armPendingDiscardWorker(t *mqttIncarnationTracker, finisher func()) {
	drained, armed := t.armRetirementPending(finisher)
	if !armed {
		return
	}
	if !s.admitWillWorker() {
		ceiling := time.NewTimer(mqttIncarnationDrainTimeout.Get())
		defer ceiling.Stop()
		select {
		case <-drained:
			finisher()
		case <-ceiling.C:
			s.Errorf("MQTT: a clean-session discard could not be completed before " +
				"shutdown: its incarnation did not drain")
		}
		return
	}
	go func() {
		defer s.willWorkerWG.Done()
		<-drained
		finisher()
	}()
}

// rearmExpiryIfViewCurrent re-arms a short expiry retry only when the caller's view
// of the session is still the current one: the same resident object, still carrying
// the tracker the caller captured, and with no timer already installed. Any of those
// failing means somebody else owns this client ID's deadline now — a re-minted
// incarnation, or a fresh timer armed by a newer close — and overwriting it would
// hand a new session a deadline it never asked for (F-036 unit 1).
func (as *mqttAccountSessionManager) rearmExpiryIfViewCurrent(s *Server, idHash string,
	sess *mqttSession, captured *mqttIncarnationTracker) {

	sess.mu.Lock()
	current := sess.leases
	sess.mu.Unlock()
	if current != captured {
		return
	}
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.sessByHash[idHash] != sess {
		return
	}
	if _, exists := as.pendingExpiries[idHash]; exists {
		return
	}
	as.scheduleSessionExpiryLocked(s, idHash, mqttExpiryRetryAfterRetiring)
}
