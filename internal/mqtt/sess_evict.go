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

// Eviction of a resident session when a remote broker persists the same client's record.

package mqtt

import (
	"crypto/sha256"
	"time"
)

// evictOutcome is what one decision attempt of the eviction worker concluded.
type evictOutcome uint8

const (
	// evictNoSession: no resident object for the client — terminal. An observation
	// that declined to spawn while ANY object was resident is consumed against the
	// object now resident; one that arrived with none resident took the no-local
	// branch.
	evictNoSession evictOutcome = iota
	// evictStale: nothing moved strictly past the resident session's acknowledged
	// record — terminal. Covers the equality/own-record case: no remote write can
	// land AT our acknowledged sequence within a stream epoch, so equality is
	// never a takeover.
	evictStale
	// evictBusy: the ownership gate or the record fence is held; the caller
	// retries on a bounded budget, then exits accepting a lost eviction.
	evictBusy
	// evictFresher: a strictly newer trigger arrived past this iteration's
	// knowledge; the caller re-loads.
	evictFresher
	// evictApplied: the session was evicted; the caller runs the notify/close and
	// (takeover only) Will-cancel tail.
	evictApplied
	// evictOwnLanding: the record carries the bytes of THIS session's own
	// unresolved attempt — a store that landed with its response lost. Nothing is
	// evicted and nothing is decided: the ambiguity resolver owns that write, and
	// once it adopts the sequence the same trigger reads stale. Terminal for this
	// attempt, and deliberately NOT a consumed decision, so the record-state
	// backstop re-fires if the resolver never gets there.
	//
	// Without this arm the record would be classified by LINEAGE against the
	// acknowledged pair, which is stale precisely because the attempt is
	// unresolved: our own write {c,a} reads unrelated to acknowledged {a,p} and
	// evicts the rightful client over its own write. The digest is the
	// only thing that identifies an attempt as ours (sess_fork_lineage.go), which
	// is why the comparison is the resolver's own, not a lineage question.
	evictOwnLanding
)

// evictDecision carries one decision attempt's outcome and, on evictApplied, the
// verdict's vocabulary and the state captured for the tail that runs after the
// locks are released.
type evictDecision struct {
	out  evictOutcome
	sess *mqttSession
	// tok/tokHeld: the record transaction still OPEN when the decision section
	// returned. The caller must release it with the full endRecordTx AFTER this
	// decision's as.mu section has returned — the full release can synchronously
	// run the void-verdict handoff, which reacquires as.mu.
	tok     recordTx
	tokHeld bool
	voc     mqttDisposalVocabulary
	forked  bool
	// displacedGen/displacedSeq identify the displaced INCARNATION. They scoped a
	// Will-outbox cancel that this route no longer performs on any verdict; what reads
	// them now is the tail's own Will discharge, which fences the enqueue against the
	// incarnation that actually disconnected.
	displacedGen string
	displacedSeq uint64
	ec           *client
	// action is what the tail needs to discharge a parked deferred close. This route
	// detaches the client inside the as.mu section and then closes it, and a detached
	// client's close publishes only an IMMEDIATE Will (mqttPublishOwedWill), so before
	// F-084 an armed or owed-DELAYED Will simply vanished here.
	//
	// The Session Expiry Interval is deliberately NOT carried: it is connection-owned and
	// stays mutable, so the tail reads it under the lock that consumes the Will instead.
	action *mqttDeferredDisconnect
}

// evictDecideOnce is the eviction worker's decision: ONE as.mu critical section
// with nothing blocking inside. It resolves the resident object FRESH (the worker
// is per-client, not per-object, so a replacement installed mid-run is simply the
// object decided against), OBSERVES the ownership gate without ever claiming it,
// proves adoption quiescent through the record fence, and only then classifies the
// decoded record against the freshest acknowledged pair.
//
// Holding as.mu across the section is what makes the gate observation sound:
// lockSession itself requires as.mu, so no gate can be NEWLY taken while this runs;
// a gate ALREADY held defers the worker (evictBusy) — a deliberate narrowing versus
// the old synchronous handler, which barged into a gate-held probe.
//
// usable=false is the license path: the observation licensed the eviction and the
// record could not narrow it (absent, undecodable, load error, or below the
// trigger), so decisionSeq is the OBSERVATION level and the verdict concedes as
// ownership-UNSETTLED — never a takeover, which no record shape proves (F-084).
// The freshness arm still applies on this path — a trigger that
// advanced past the observation this iteration answers is delivered knowledge,
// not a lost observation, and the re-load may find the record readable at the
// new level.
//
// No lock held on entry.
// loaded is the raw record this iteration read, or nil when the load failed or
// found nothing. It is carried alongside the decoded ps because the digest arm
// compares BYTES — the decoded form cannot answer "are these the bytes we sent".
func (as *mqttAccountSessionManager) evictDecideOnce(cIDHash string, st *sessionReconcileState,
	usable bool, ps *mqttPersistedSession, decisionSeq uint64, loaded *StoredMsg) (d evictDecision) {

	as.mu.Lock()
	defer as.mu.Unlock()
	sess, ok := as.sessByHash[cIDHash]
	if !ok {
		d.out = evictNoSession
		return d
	}
	if _, held := as.sessLocked[sess.id]; held {
		// A CONNECT, close, probe, SUBSCRIBE or UNSUBSCRIBE owns the session's
		// record state; its writes are CAS-arbitrated, so deferring costs at most
		// a lost eviction, which the observation contract already allows.
		d.out = evictBusy
		return d
	}
	// COMPLETION OWNERSHIP, per decision attempt (F-036 unit 1). This worker is
	// spawned by the record-watch delivery and outlives the callback's own lease, so
	// it owns the incarnation for the extent of THIS decision and no longer. A
	// retirement in progress refuses it — never parks it, because it holds as.mu
	// here — and the observation converges on the watch's next pass or the periodic
	// backstop, which is the same disposition the gate-held case above takes.
	//
	// The gate check above already excludes every retirement, each of which holds
	// the client-ID gate for its whole transition; this makes the refusal a fact
	// about the INCARNATION rather than about who happens to hold a lock, so a
	// future retirement that does not take the gate is still refused here.
	workerLease, admitted := sess.leases.acquire()
	if !admitted {
		d.out = evictBusy
		return d
	}
	defer workerLease.release()
	tok, acquired := sess.tryBeginRecordTx()
	if !acquired {
		d.out = evictBusy
		return d
	}
	d.sess, d.tok, d.tokHeld = sess, tok, true
	// The abnormal-exit fence guard (LIFO: runs before the as.mu unlock above). A
	// panic or Goexit escaping the seam below must release the fence, and the full
	// endRecordTx is illegal here — it can synchronously run the void-verdict
	// handoff, which reacquires as.mu. abandonRecordTx takes only sess.mu (the
	// established as.mu → sess.mu order) and skips the handoff, whose skipped
	// liveness edge the periodic backstop (reconcileResidentRecordState →
	// flushVoidVerdictGap) owns. Every NORMAL return hands the open token to the
	// caller instead, which releases it fully after as.mu.
	normalReturn := false
	defer func() {
		if !normalReturn {
			_ = sess.abandonRecordTx(tok)
			d.tokHeld = false
		}
	}()
	// Test seam: fired with as.mu held and the record fence open — the window an
	// abnormal exit must clean up from. Handlers may panic or Goexit; they must
	// not block (they pin as.mu).
	if h := as.testEvictPostFence; h != nil {
		h(cIDHash)
	}
	// Fence acquired proves adoption QUIESCENT: every sess.seq/lineage adoption on
	// a resident, concurrently reachable object happens inside the writer's open
	// recordTx, so no adoption is in flight and a DELAYED acknowledgment (store
	// returned, tx still open until it adopts) cannot be raced past — which is
	// what retires the ordering hazard where a remote observation is handled
	// before the local write's own PubAck adopted its sequence. What it does NOT
	// prove is that no AMBIGUOUS attempt is outstanding: a save whose store landed
	// but whose response was lost arms seqUncertain, closes its transaction, and
	// leaves this pair stale behind a free fence. That staleness only ever NARROWS
	// the verdict — a sibling of the unadopted attempt reads unrelated against the
	// stale pair and CONCEDES (the stated one-hop recognition miss; what it concedes
	// TO is the unsettled vocabulary, never a takeover claim — F-084), while a sibling of the
	// acknowledged record itself still classifies forked (a true positive). No
	// record shape can manufacture a false forked verdict from a stale pair, so
	// the eviction proceeds; the unresolved attempt's own convergence stays with
	// the resolver and the rejection probe. See sess_persist_evict_test.go's
	// unresolved-attempt pair test and docs/CONFORMANCE.md.
	//
	// The one shape that staleness does NOT merely narrow is our own unresolved
	// attempt arriving back as a delivered record: that is not a sibling of
	// anything, it IS our write, and only its digest says so. Sampled in the same
	// sess.mu section as the pair, so no resolver can adopt between the two reads
	// and leave the arm deciding on a mismatched snapshot.
	sess.mu.Lock()
	seqNow, wid, pwid := sess.seq, sess.wid, sess.pwid
	uncertain, pw := sess.seqUncertain, sess.pendingWrite
	var pwBase uint64
	var pwDigest [sha256.Size]byte
	if pw != nil {
		pwBase, pwDigest = pw.baseSeq, pw.digest
	}
	sess.mu.Unlock()
	// Strict: eviction requires the record to have moved PAST us.
	if decisionSeq <= seqNow {
		d.out = evictStale
		normalReturn = true
		return d
	}
	// The digest arm, ahead of every classification: while a write of ours is
	// unresolved, a record carrying its exact bytes is that write landing with its
	// response lost — the resolver's own identity test (sess_uncertain.go), reused
	// verbatim so the two cannot drift. Deciding it here would be the false
	// eviction the old SP path avoided by never delivering our own writes at all.
	// A NON-matching record under seqUncertain keeps today's behavior exactly: it
	// falls through to lineage against the stale pair, which can only narrow.
	if uncertain && pw != nil && loaded != nil &&
		loaded.Sequence > pwBase && mqttRecordDigest(loaded.Data) == pwDigest {
		d.out = evictOwnLanding
		normalReturn = true
		return d
	}
	// The SAME refusal when there are no bytes to compare. An unusable load — absent,
	// unreadable, undecodable, or below the trigger — normally concedes to the
	// UNSETTLED vocabulary and decides at the observation level, which is right when
	// the observation can only have come from a peer. It is NOT right while a write
	// of ours is unresolved and could itself be what was observed: the trigger can be
	// our own landed-but-unacknowledged write (the backstop reaches this path with no
	// record at all, deliberately, so the Origin filter cannot screen it), and
	// conceding would evict the rightful client over its own write on the one
	// occasion the digest cannot speak.
	//
	// Refusing costs only latency: the periodic pass settles the ambiguity before it
	// ever reaches the ownership backstop again, and the undecided trigger is
	// re-fired afterwards against an adopted sequence, where staleness or lineage
	// decides it properly.
	if uncertain && pw != nil && !usable && decisionSeq > pwBase {
		d.out = evictOwnLanding
		normalReturn = true
		return d
	}
	// A strictly newer trigger than this iteration's own knowledge deserves a
	// fresh read — on the unusable-load path included: an observation that
	// advanced past the trigger this iteration answers is DELIVERED knowledge,
	// and a record unreadable at the old level can be readable (and classifiable)
	// at the new one, so conceding here would dispose a session on knowledge already
	// superseded (holistic round 2). Requiring the advance to pass decisionSeq
	// (= max(target, smsg.Sequence) when the record was usable, the observation
	// level otherwise) bounds the loop by observation count.
	if st.evictLatest > decisionSeq {
		d.out = evictFresher
		normalReturn = true
		return d
	}
	// Classify HERE, at decision time, from the freshest pair — never from a pair
	// sampled at load time, which a local save adopting between the load and this
	// section would falsify.
	//
	// An UNUSABLE load concedes to the unsettled vocabulary, not the takeover one. It used
	// to concede to the takeover, which was the strongest claim available on the weakest
	// evidence there is: a record we could not read, that was gone, or that sat below the
	// target says only that this observation cannot be classified. F-084.
	voc, forked := mqttDisposalOwnershipUnsettled, false
	if usable {
		voc, forked = disposalForRemotePersist(ps, wid, pwid)
	}
	// The subject has moved past our base, so this broker's expiry timer must not fire and
	// delete state it no longer speaks for. (Not "the remote record owns the session": the
	// observation identifies no owner — F-084.) Spec5 [3.1.2.11.2].
	as.cancelSessionExpiryLocked(cIDHash)
	as.removeSession(sess, false)
	sess.mu.Lock()
	d.displacedGen, d.displacedSeq = sess.gen, sess.seq
	ec := sess.c
	if ec != nil {
		as.addSessToFlappersLocked(sess.id)
		ec.Warnf("MQTT: session %q %s", sess.id, voc.logReason)
		// Disassociate the client from the session so that on client close,
		// nothing will be done with regards to cleaning up the session, such as
		// deleting stream, etc..
		sess.c = nil
	}
	// The parked deferred action travels in the capsule so the tail can discharge the Will
	// this route used to drop on the floor. The expiry does NOT travel with it: it is
	// connection-owned and stays mutable through an MQTT 5 DISCONNECT override, so the tail
	// reads it under the same lock that consumes the Will rather than from a value sampled
	// here. Reading sess.expiryInterval would be worse still — a v5 DISCONNECT updates the
	// connection's copy and the session's in SEPARATE critical sections, the second only
	// while sess.c == c, and this route holds no ownership gate and can detach between them.
	d.action = sess.takeDeferredDisconnectLocked()
	sess.mu.Unlock()
	d.voc, d.forked, d.ec = voc, forked, ec
	if d.forked {
		// The persist-observation route's mint, and the ONE site that must not take as.mu:
		// this whole decision runs under it. Carried as an explicit BIT from the
		// classification rather than recovered from voc.reason, because the forked and the
		// unsettled vocabularies both carry 0x89 — a reason-code test would mint a grant for
		// a record that established nothing to anchor one to. See sess_fork_repair.go and
		// F-084.
		as.mintForkedGrantLocked(sess, wid, pwid)
	}
	d.out = evictApplied
	normalReturn = true
	return d
}

// evictDisplacedOnRemotePersist evicts the resident local session after
// the session-record watch observed a record write at-or-past its acknowledged
// record — classifying the surviving record FIRST, so a survivor whose body CLAIMS our own
// parent is reported as a FORK while everything else is reported as ownership UNSETTLED.
// Both carry 0x89, inviting the reconnect that resolves the ambiguity; the difference is
// the Reason String and, decisively, whether a repair grant is minted. No arm here reports
// a takeover: this route learns of the displacement from a record, and no record shape
// proves a successor. F-084.
//
// Two corrections to what this comment used to say. It is NOT the leader-transition
// fork: that route is closed for two broker-authored conditional writes (the same-base
// boundary, docs/CONFORMANCE.md), and what remains reachable is an unconditional foreign
// write. And THIS path cancels no delayed Will on either verdict — see the block below,
// where that was deliberately removed in favour of the claimant deciding against the
// permanent acceptance facts. The verdict's Will consequence lives on the BOUND
// REJECTION path (sess_rejected_write.go) — where, since F-084, BOTH arms consume the
// Will: neither demonstrates a successor, and only a displacement this broker holds both
// ends of may suppress one.
//
// It is per-CLIENT, not per-object: at most one runs per client (st.evictRunning),
// and each iteration resolves the resident object fresh under as.mu, so a
// replacement installed mid-run is simply the object the next iteration decides
// against. This is RECOGNITION plus in-process coalescing; the durable
// arbitrations remain the record CAS and the rejection probe, and a lost
// observation is never replayed.
//
// st is the *sessionReconcileState the callback registered for cIDHash; it is
// passed in (not re-fetched) so the cleanup targets exactly that entry.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) evictDisplacedOnRemotePersist(s *Server, cIDHash string, st *sessionReconcileState) {
	applied := false
	completed := false
	// cleanup clears the coalescing flag, decrements the exact state the callback
	// incremented, and prunes through the one helper — which keeps the entry alive
	// while a session is still resident, because an undecided trigger left by a
	// refusal is precisely what the record-state backstop re-fires. See
	// pruneReconcileEntryLocked.
	cleanup := func() {
		as.mu.Lock()
		st.evictRunning = false
		if st.inFlight > 0 {
			st.inFlight--
		}
		as.pruneReconcileEntryLocked(cIDHash, st)
		as.mu.Unlock()
	}
	// observe fires the completion seam AFTER cleanup, on the normal and abnormal
	// paths both, carrying whether an eviction was applied — so a test awaiting a
	// "no eviction" outcome asserts on the worker's DECISION, not its absence.
	observe := func() {
		as.mu.RLock()
		h := as.testEvictComplete
		as.mu.RUnlock()
		if h != nil {
			h(cIDHash, applied)
		}
	}
	// Abnormal exits only (a panic or Goexit escaping a test seam): the normal
	// path funnels through the in-loop terminal cleanup, whose atomic re-check
	// keeps a late trigger from stranding; this guard accepts trigger loss to keep
	// the flag and counter from leaking.
	defer func() {
		if completed {
			return
		}
		cleanup()
		observe()
	}()

	// consumed is the trigger level this worker's last iteration decided against;
	// the terminal cleanup re-checks st.latest against it under as.mu.
	var consumed uint64
	for {
		as.mu.RLock()
		// The trigger this iteration answers. Read fresh each iteration: a newer
		// resident-delivered observation only advances it (no-resident
		// observations advance only the reconcile watermark, never this).
		target := st.evictLatest
		pauseBefore := as.testEvictPauseBeforeLoad
		as.mu.RUnlock()
		if pauseBefore != nil {
			pauseBefore(cIDHash)
		}

		// ONE read per iteration; the observation is at-most-once and the read is
		// bounded, so JetStream unavailability delays only this eviction.
		smsg, err := as.jsa.loadSessionMsg(as.domainTk, cIDHash)
		if err != nil && isErrorOtherThan(err, JSNoMessageFoundErr) {
			s.Warnf("MQTT remote-persist eviction: unable to load session record: %v", err)
		}
		// The license rule: the eviction's LICENSE is the observation — a remote
		// write happened — and the read can only NARROW the claim, never revoke
		// the license. Absent, undecodable, a load error, or a record BELOW the
		// trigger (older than the write that licensed the eviction — classifying
		// it could answer FORKED for a write it cannot speak for, and mint a repair
		// grant with it) all concede: the decision runs at the OBSERVATION level with
		// the UNSETTLED vocabulary. Decode outside any lock.
		var ps *mqttPersistedSession
		usable := false
		decisionSeq := target
		// The raw record travels separately from the decoded one and is kept even
		// when the record is BELOW the trigger or undecodable: the digest arm asks
		// whether these are the bytes this session sent, which is a question about
		// the load, not about the license.
		var loaded *StoredMsg
		if err == nil && smsg != nil {
			loaded = smsg
			if smsg.Sequence >= target {
				if ps = mqttDecodeSessionRecord(smsg); ps != nil {
					usable = true
					decisionSeq = smsg.Sequence
				}
			}
		}

		as.mu.RLock()
		pauseAfter := as.testEvictPauseAfterLoad
		as.mu.RUnlock()
		if pauseAfter != nil {
			pauseAfter(cIDHash)
		}

		// The decision, on a bounded busy-retry budget. Each attempt re-resolves
		// residency fresh; the sleeps run outside every lock.
		var d evictDecision
		for attempt := 0; ; attempt++ {
			d = as.evictDecideOnce(cIDHash, st, usable, ps, decisionSeq, loaded)
			if d.tokHeld {
				// Full release, strictly AFTER the decision's as.mu section
				// returned: endRecordTx can synchronously run the void-verdict
				// handoff, which takes as.mu; the other order self-deadlocks.
				if rerr := d.sess.endRecordTx(d.tok); rerr != nil {
					s.Warnf("MQTT remote-persist eviction: unable to release record transaction: %v", rerr)
				}
			}
			if d.out != evictBusy || attempt+1 >= mqttTeardownLockAttempts {
				break
			}
			time.Sleep(mqttTeardownLockItvl)
		}
		// consumed is what this iteration actually DECIDED against. A busy
		// refusal and a no-resident exit decided nothing: crediting them with the
		// opportunistically loaded record's sequence would let an observation at
		// exactly that level — delivered after the last failed attempt — read as
		// already consumed at cleanup, stranding a genuine trigger behind the
		// clearing flag. They consume only the trigger they answered.
		consumed = decisionSeq
		if d.out == evictBusy || d.out == evictNoSession {
			consumed = target
		}
		// evictDecided is the OTHER half of that accounting, and it is what the
		// record-state backstop reads. `consumed` governs this worker's own loop;
		// evictDecided says whether anything ever reached a verdict for this client.
		// A refusal — busy, or our own landing deferred to the resolver — leaves it
		// behind evictLatest deliberately, which is the re-fire signal. Advanced
		// only where a decision really happened.
		decided := d.out == evictApplied || d.out == evictStale || d.out == evictNoSession
		if decided {
			as.mu.Lock()
			if decisionSeq > st.evictDecided {
				st.evictDecided = decisionSeq
			}
			as.mu.Unlock()
		}

		switch d.out {
		case evictApplied:
			applied = true
			// The displaced connection's Will, BEFORE the notify and the close. This route
			// detaches inside the as.mu section and then closes, and a detached client's
			// close publishes only an IMMEDIATE Will (mqttPublishOwedWill), so an armed or
			// owed-DELAYED one simply vanished here. It runs in the tail because the tail is
			// where every lock and the record token are already released. F-084.
			//
			// The epoch is read from the manager rather than carried in the capsule: it is
			// resolved once at initialization and immutable after, so a late read cannot skew.
			// The SAME probe the gated paths fire, because this route does NOT go through
			// mqttForeignDisposition.applyWill and would otherwise be the hole in a claim
			// that every disposal is observed. Its CONTRACT is the inverse of theirs: the
			// record token and every lock are released by here, and no ownership gate is
			// held at all — see TestF084RemotePersistWillPhaseIsUngated.
			if h := mqttTestWillPhaseProbeV.Load(); h != nil && (d.action != nil || d.ec != nil) {
				(*h)(d.sess, as)
			}
			if d.action != nil {
				s.dischargeDeferredWill(as, d.sess, d.action)
			} else if d.ec != nil {
				// nil expiry: read from the connection under the lock that consumes the
				// Will. This route detaches inside its as.mu section and runs the Will in a
				// tail, so any value sampled at decision time can be stale by then — an
				// MQTT 5 DISCONNECT override moves it. F-084.
				s.mqttHandleWill(d.ec, nil, d.displacedGen, d.displacedSeq, as.sessStreamEpoch)
			}
			if d.ec != nil {
				// Notify and close AFTER every lock release: the notify writes to
				// the socket under c.mu, bounded by mqttWriteDeadline. The shared
				// tail claims the connection's one DISCONNECT and honors the
				// CONNACK gate, so this cannot append a second DISCONNECT to one a
				// racing displacement already sent; 3.1.1 closes silently. The
				// object is already deresidented, so a replacement CONNECT proceeds
				// regardless of this write's duration. Spec5 [3.1.4].
				d.ec.mqttNotifyDisplaced(d.voc)
				go d.ec.closeConnection(d.voc.closeReason)
			}
			// No delayed-Will cancel here any more, on either verdict. The
			// displaced incarnation's Will is decided where every other one is:
			// by the claimant, when it becomes due, against the permanent
			// acceptance facts. A cancel issued here could only say "somebody
			// reconnected", which is not the question — a reconnect after the
			// Will Delay has elapsed leaves the Will owed ([MQTT-3.1.2-8]) — and
			// it had to be scoped by generation AND origin sequence precisely
			// because it could otherwise land after a LATER incarnation's
			// enqueue and destroy that Will instead. Both problems are the same
			// problem: an undated cancel cannot name the Will it is entitled to.
			// See sess_accept_fact.go.
		case evictFresher:
			continue
		}

		// Terminal cleanup, in-loop: clearing the coalescing flag and consuming a
		// late trigger must be ONE as.mu decision, or an observation that declined
		// to spawn (flag still true) while this worker was past its last check
		// would strand. If a resident object exists and a strictly newer trigger
		// arrived, this same worker loops — with a fresh busy budget, possibly
		// against a replacement object — instead of exiting.
		if h := func() func(string) {
			as.mu.RLock()
			defer as.mu.RUnlock()
			return as.testEvictCleanupEntry
		}(); h != nil {
			h(cIDHash)
		}
		as.mu.Lock()
		_, resident := as.sessByHash[cIDHash]
		if resident && st.evictLatest > consumed {
			as.mu.Unlock()
			continue
		}
		st.evictRunning = false
		if st.inFlight > 0 {
			st.inFlight--
		}
		as.pruneReconcileEntryLocked(cIDHash, st)
		as.mu.Unlock()
		break
	}
	completed = true
	observe()
}
