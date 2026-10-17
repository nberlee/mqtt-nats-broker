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

// Clearing and updating a session: clear and its self-lease, reset for reuse, prepared clear
// and destruction, and record updates.

package mqtt

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// errMQTTClearRefusedUnresolved is the sentinel clear returns when it refused BEFORE its
// first deletion because the record state is unresolved. What it encodes is
// RECOVERABILITY, not timing: nothing was deleted, claimed or reset, so a caller holding an
// obligation can put it back untouched and a caller holding a client can simply reject.
//
// It deliberately says nothing about WHEN the write became unresolved, because both are
// real. The in-fence refusal is genuinely late — an off-readLoop writer went ambiguous
// while this call waited for the fence. The entry refusal is not necessarily late at all:
// expireSession has no early uncertainty check, so it can arrive with the write already
// unresolved before its timer ever fired.
//
// It is a distinct error precisely so that recovery is never inferred from an arbitrary
// clear() failure — a lost teardown claim or a JetStream error means the teardown is no
// longer ours or is partly done, and re-parking there would resurrect an obligation nobody
// owes.
var errMQTTClearRefusedUnresolved = errors.New("clear refused before deletion: the record state is unresolved")

// mqttTestClearWindowHookV fires at each of clear's two decision points, named by the
// window: BEFORE its entry check, and after that check but before it takes the record
// fence. Both are windows in which another writer's ambiguity is still invisible to the
// check that follows, and a test arms one positively there instead of sleeping.
var mqttTestClearWindowHookV atomic.Pointer[func(sess *mqttSession, window string)]

const (
	mqttClearWindowPreEntry = "pre-entry"
	mqttClearWindowPreFence = "pre-fence"
)

func setMQTTTestClearWindowHook(h func(sess *mqttSession, window string)) func() {
	prev := mqttTestClearWindowHookV.Load()
	if h == nil {
		mqttTestClearWindowHookV.Store(nil)
	} else {
		mqttTestClearWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestClearWindowHookV.Store(prev) })
}

func (sess *mqttSession) clearWindowHook(window string) {
	if h := mqttTestClearWindowHookV.Load(); h != nil {
		(*h)(sess, window)
	}
}

// Clear the session: discard everything one session incarnation owns and remove
// its record.
//
// The destructive half runs under a DURABLE TEARDOWN CLAIM, not under the local
// locks: lockSession and the record transaction fence are in-process, so they
// arbitrate nothing against another broker writing the same record. A lost claim means
// the claim is not ours and NOTHING is deleted. A release that removes no
// record is classified by re-reading rather than assumed to be ours. And every
// resource named here is generation-scoped. Generation scoping protects the next
// incarnation; a resume preserves the generation, so it never protected a peer still
// serving the retiring one. What protects that peer's inbound QoS 2 state is the bound
// each hold removal carries (F-062); $MQTT_inflight now carries the same kind of bound,
// its reap's own enumeration watermark (F-034). What neither bound does is arbitrate a
// peer that is genuinely still serving the retiring incarnation — that needs an ownership
// predicate, which is F-036's. See sess_teardown.go.
//
// It runs in a record transaction of the OLD generation — its record removal must
// validate against the epoch it opened with — and ends that generation as it
// releases the fence, so every token minted before it is dead. That covers the two
// paths that hand this very object back to be reused as a fresh session: they
// write normally at the new epoch while any pre-clear() writer is refused. See
// sess_recordtx.go.
//
// BOTH uncertainty refusals are UNCONDITIONAL, and both carry
// errMQTTClearRefusedUnresolved. The in-fence one re-checks with the fence HELD, which is
// the sound check — the entry check below is a check-then-act, because the fence is
// what serializes writers and an off-readLoop one (a register store settling) can go
// ambiguous while this call is still waiting for it. It was once opt-in, for the
// deferred discharge alone, because no other caller could act on the sentinel; each
// reachable caller now acts on the sentinel, though the shared recovery may decline, so
// the flag was a mechanism that said "true" at every
// site it could reach. The two callers built from a record just read
// (expireSessionFromRecord, createOrRestoreSession's overdue discard) cannot arm
// uncertainty at all — see the proofs beside them — and refuse rather than destroy if
// that proof were ever wrong.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
//
// Since F-036 unit 1 this is a COMPOSITION: prepareClear (the non-mutating
// refusals and the record fence), the retirement-permit mint, and destroy (the
// destructive half). The caller must have CLOSED the incarnation tracker and
// drained it — the mint fails otherwise, which is the mechanical enforcement —
// and on a refusal the caller reopens the tracker before re-parking, so the
// re-parked object is resolvable again. The CONNECT clean-start arm does not use
// this composition: it interleaves the capsule capture and the detach between
// preparation and destruction (see mqttProcessConnect).
func (sess *mqttSession) clear(noWait bool) error {
	return sess.clearWithSelfLease(noWait, nil)
}

// errMQTTClearDrainTimeout reports that a discard's incarnation tracker did not
// drain within the ceiling: an admitted route or a queued effect of the retiring
// incarnation is still outstanding. Nothing was mutated. Each caller applies its
// own ceiling disposition (leave resident and retry, re-arm the timer, re-park),
// per F-036 unit 1 §3.7.
var errMQTTClearDrainTimeout = errors.New("incarnation did not drain before the discard ceiling")

// mqttExpiryRetryAfterRetiring is how soon a session-expiry fire re-arms when it
// meets an incarnation that is retiring: short, because the retirement either
// completes (and the expiry is moot) or reopens (and the deadline is still owed),
// and neither outcome should wait out another full interval.
var mqttExpiryRetryAfterRetiring = time.Second

// mqttIncarnationDrainTimeout bounds the wait for the retiring incarnation's
// admitted routes and queued effects to complete. It is on the order of the JS API
// timeout: a wedged owner turns into a bounded discard ceiling, never an unbounded
// wait or a disposal under a live owner.
var mqttIncarnationDrainTimeout = 2 * mqttDefaultJSAPITimeout

// clearWithSelfLease is clear() for a caller that holds its own lease on the
// incarnation being discarded (expireSession fires holding its fire-time lease):
// that lease is PROMOTED so the drain does not wait on the caller itself. A nil
// selfLease is the ordinary case.
//
// It closes the incarnation tracker, drains it under the ceiling, then runs the
// non-mutating preparation, mints the retirement permit and destroys. On a drain
// ceiling or a preparation refusal it REOPENS the tracker before returning, so the
// object is admissible again for the caller's ceiling disposition.
func (sess *mqttSession) clearWithSelfLease(noWait bool, selfLease *mqttLease) error {
	return sess.clearInner(noWait, selfLease, nil, nil)
}

// clearInner is the shared close→drain→prepare→mint→destroy body. onCeiling, when
// non-nil, is installed as the tracker's retirement-pending finisher IF the drain
// ceiling fires: the tracker stays CLOSING and the finisher re-runs the discard
// when the last owner releases, or immediately if the drain has already completed.
// This is the clean-session discard's ceiling disposition, which may not abandon
// the discard ([MQTT-3.1.2-6]). A nil onCeiling reopens the tracker on ceiling,
// leaving the object admissible for the caller to retry.
func (sess *mqttSession) clearInner(noWait bool, selfLease *mqttLease,
	onCeiling func(), onCeilingSchedule func(*mqttIncarnationTracker, func())) error {
	sess.mu.Lock()
	t := sess.leases
	sess.mu.Unlock()
	t.close()
	if selfLease != nil {
		t.promote(selfLease)
	}
	ceiling := time.NewTimer(mqttIncarnationDrainTimeout)
	drained := t.waitDrained(ceiling.C)
	ceiling.Stop()
	if !drained {
		if onCeiling != nil {
			// The discard is still OWED, so the obligation is armed and WATCHED by a
			// worker admitted right here — before this call returns — rather than
			// scheduled later by whichever release happens to drain the tracker.
			// Eager admission is what keeps it inside the server's shutdown
			// lifecycle; a late spawn could start a teardown after shutdown had
			// already waited for its workers.
			onCeilingSchedule(t, onCeiling)
		} else {
			t.reopen()
		}
		return errMQTTClearDrainTimeout
	}
	p, err := sess.prepareClear()
	if err != nil {
		t.reopen()
		return err
	}
	sess.mu.Lock()
	permit, perr := sess.retireIncarnationLocked()
	sess.mu.Unlock()
	if perr != nil {
		p.abort()
		t.reopen()
		return fmt.Errorf("refusing to discard session %q: %w", sess.id, perr)
	}
	return p.destroy(permit, noWait)
}

// mqttPreparedClear owns the record transaction prepareClear opened. Exactly one
// of abort or destroy consumes it.
// resetForReuseLocked re-mints this object as a FRESH incarnation for a caller that
// REUSES it as a new session: it empties every per-incarnation map and verdict, re-mints
// the generation under the retirement permit, and installs freshGen and newSeq as the
// base the next write is conditioned on. Factored out of destroy so the reuse paths
// cannot drift: destroy re-mints at newSeq == 0, because the record it just deleted
// leaves nothing to displace; the clean-start HANDOFF re-mints at newSeq == the
// predecessor record's sequence, because its accepted batch DISPLACES that record in
// place rather than deleting it (F-105 row-9 ordering, Codex consult 6). The lineage is
// broken either way (wid/pwid zeroed), so the first post-re-mint write is a parentless
// create-shaped body whatever base it lands at.
//
// sess.mu must be held and is NOT released here.
func (sess *mqttSession) resetForReuseLocked(permit mqttRetirementPermit, freshGen string, newSeq uint64) {
	sess.subs = nil
	sess.noLocalSubs = nil
	sess.rapSubs = nil
	sess.acceptFact = mqttAcceptFactRef{}
	sess.subIDs = nil
	sess.sharedSubs = nil
	sess.sharedCharged = nil
	sess.consCharged = nil
	// With drains nil no new finalizer can even start, and the generation bump on
	// fence release stops any that is already in flight from writing (I9).
	sess.drains = nil
	sess.drainByDur = nil
	// Queued disposals go with them. The teardown below deletes this incarnation's drain
	// durables outright, so nothing is owed; and both paths that REUSE this object as a
	// fresh session would otherwise have the next fence release issue a retired
	// generation's deletes. Anything the teardown misses is the orphan sweep's, which is
	// the disposition already stated for an unconfirmed disposal.
	sess.pendingDrainDisposals = nil
	sess.pendingPublish = nil
	sess.pendingPubRel = nil
	sess.refusedCarriers = nil
	sess.cpending = nil
	sess.deferred = nil
	// Retired sources belonged to the discarded incarnation's consumers, which are being
	// deleted: nothing can redeliver them, so nothing needs settling.
	sess.srcAckOwed = nil
	sess.terminalSources = nil
	sess.terminalRetries = nil
	// The discarded generation's ACK-only bindings are unreachable from the fresh one (ACK
	// routing keys them by generation), and their only other removal — observing their exact
	// ended exchange — is not something a discard establishes: kept, they would accumulate
	// across every abandonment and clean start (checkpoint review WIP-2).
	sess.abandonedAcks = nil
	sess.withdrawOwed = nil // the discarded generation's owed withdrawals (F-088)
	sess.admissionContinuity = nil
	sess.discardDisposalsLocked()
	// These verdicts are per-incarnation and they only ever reach durable state through
	// the session record (VoidedRegs on the way out, mqttNonceSet on the way back in),
	// and a discard DELETES that record — so no later reader can obtain a retired
	// generation's verdicts, whatever survives on $MQTT_inflight. That is the reason, and
	// it deliberately does NOT rest on the teardown having removed every register: since
	// F-034 the reap spares anything above its enumeration watermark and refuses outright
	// on an unreadable one, and clear()'s no-record branch has no marker to re-drive it,
	// so survivors are possible. They are the orphan sweep's, and the sweep reads durable
	// state rather than this object.
	// The handoff watermark goes with them, and for the same reason the two paths that
	// REUSE this object as a fresh session need it: an old generation's value left
	// standing would suppress the new generation's handoffs until its revisions caught
	// up. See voidVerdictHandoffDueLocked.
	sess.voidedRegNonces = nil
	sess.settledVoidReservations = nil
	sess.voidedRegsRev, sess.voidedRegsSavedRev, sess.voidedRegsHandoffRev = 0, 0, 0
	// A retired generation's rejected CAS is not a fresh incarnation's work to settle.
	// Cross-generation false equality is structurally excluded anyway — $MQTT_sess
	// sequences only grow, so a new incarnation's sess.seq can never equal a prior
	// generation's base — but the reset states the intent instead of leaning on stream
	// arithmetic. See sess_rejected_write.go.
	//
	// DISARMING is the load-bearing half now, not the sequence reset: this re-mints at
	// seq = 0, and a base-0 rejection is a real one since F-029, so a marker left armed
	// here would match the fresh session's own sequence and gate its serving forever.
	sess.recordRejectedSeq, sess.recordRejectedArmed = 0, false
	// The identifier space of the RETIRING incarnation is gone with the maps above, so
	// the restriction that protected it must go too. Two paths REUSE the session object
	// as a fresh session, and a fresh one reports Session Present false, so it never
	// runs the resume barrier that would otherwise clear this — left set, bumpPI would
	// return 0 for the whole life of the new session and it could never deliver a
	// single outbound QoS 1/2 message. Safe because the new incarnation has a new
	// generation: nothing the old one could not account for can name a subject the new
	// one uses. See inflight_register.go (R3).
	sess.piSpaceIncomplete = false
	// Incarnation disposal, the retention lifecycle's last step: the old incarnation is
	// irrevocably retired and cannot resume, so nothing it was holding open is owed. An
	// ABORTED clear never reaches here, which is what preserves the obligations it was
	// about to discharge.
	sess.adoptedDrains = nil
	sess.pubRelConsumer = nil
	sess.pubRelVerified = false
	// The PUBREL delivery subject is RE-MINTED below with the new incarnation, so a
	// flag that means "already subscribed" would refer to a subscription on the
	// RETIRED subject. Left set, a reused session object skips processSub and then
	// creates a consumer delivering to a subject nothing is listening on — and
	// because $MQTT_out is interest-retained, every PUBREL it stores is dropped and
	// the client can never complete a QoS 2 exchange.
	//
	// The CONNECT reuse path happens to clear this again after calling clear(), so
	// the hazard is not reachable through it today; that is a caller compensating
	// for state clear() left inconsistent, in one of its seven call sites. The
	// invariant belongs here, beside the re-mint that breaks it — exactly as the
	// affine path already does with q2AffSubscribed below.
	sess.pubRelSubscribed = false
	sess.q2AffConsumer = nil
	sess.q2AffGen = _EMPTY_
	sess.q2AffSubscribed = false
	sess.q2AffDeliverySubject = _EMPTY_
	sess.q2AffDeliverySubjectB = nil
	// Reopen the move fence. clear() is not only a teardown: the expired-session
	// path reuses this very object as a fresh session, and a permanently closed
	// fence would make every later shared QoS 2 subscribe on it fail. Safe here
	// because the fence only has to hold across the drain above — from this point
	// on there is no generation and no readiness, so beginQ2AffOp admits nothing
	// regardless.
	sess.q2AffClosing = false
	// Re-mint the incarnation generation. clear() is not only a teardown: two paths
	// REUSE this very object as a fresh session (the clean-start discard and the
	// overdue-record path), and both must provision under a generation that the
	// deletions below cannot name — including when one of them lands late, after a
	// peer has finished the teardown and a new session exists. That is what makes a
	// late delete harmless TO THE NEXT INCARNATION by construction rather than merely
	// unlikely. Naming alone does not make one harmless to a peer still serving the
	// RETIRING generation, which a resume preserves; for the inbound QoS 2 holds the
	// purge bound does, and it is what a late walk is limited by: see F-062. A legacy
	// session with no generation is re-minted here too, which is the only way it
	// leaves the exposed hash-only naming. See sess_incarnation.go.
	sess.setIncarnationLocked(freshGen, permit)
	sess.seq = newSeq
	// The lineage goes with the sequence: we are acknowledged for nothing once the
	// retiring record is deleted, and six sites read wid as exactly that. Zeroed AFTER
	// retiringWID was captured above.
	sess.wid, sess.pwid = _EMPTY_, _EMPTY_
	// The chain breaks here with the sequence, and the reused object's first write is a
	// parentless create like every other. It used to keep a pointer at the retiring
	// record so a clean-start sibling stayed recognisable; the write-lineage audit
	// removed that, because a pointer at a record whose interval has ended is what every
	// documented FALSE fork was built from. retiringWID is still captured above, for the
	// teardown MARKER's parent and for the retirement precondition below — neither of which
	// is about the next write's ancestry.
	//
	// The GRANT chain breaks here too, and always did: nothing of the retired incarnation
	// may be RE-ASSERTED into the fresh one — that is the resurrection this repair is
	// forbidden to perform. With both bodies nil the first post-re-mint save mints
	// nothing. See sess_fork_repair.go.
	sess.grantBody, sess.grantBodyParent = nil, nil
	sess.tmaxack = 0
	// Discarded session: reset the PI counter too so a reused session object
	// does not inherit the previous session's identifiers. Spec [MQTT-3.1.2-6].
	sess.last_pi = 0
	// sendsInFlight deliberately SURVIVES the discard (F-087 WP5, class F2). With the
	// counter back at zero a reused session object can mint PI 1 for the new incarnation
	// while the displaced connection's PI 1 write is still blocked in a synchronous
	// socket write — the displaced connection is closed on another goroutine, so the
	// write outlives this call. The registry is what holds those identifiers until each
	// write returns; endSend removes the entry, and on nilled maps that removal is the
	// whole continuation. A scan of the entries here could not do the same job: a write
	// whose PUBLISH entry a successful PUBREC already replaced has no entry to find.
	// Hygiene, not a drain. Holds that reserve nothing are dropped so the registry stays
	// bounded; the ones that still reserve something are left exactly as they are. This
	// deliberately does NOT retire a PUBREL's acknowledgement debt: sess.c is untouched
	// here, so on the clean-start path the displaced client is still bound and its debt
	// is still holding an identifier the client itself still holds (F-089).
	// The predicate consults sess.retiringOwnerCID as well as the bound client, so a
	// retirement that detached before this ran still reads the displaced connection's
	// debts as LIVE and does not free an identifier it still holds (F-089).
	sess.sweepDeadSendHoldsLocked()
}

type mqttPreparedClear struct {
	sess *mqttSession
	tok  recordTx
	// auth is the authority as the discard boundary LAST read it, after settling any proposal
	// that stood (F-105 8a). Every later decision of this discard — destroy's retirement
	// receipt, the clean-start handoff-versus-destroy choice — reads THIS load and never a
	// fresher one: a later read could see a proposal that appeared after the boundary and
	// build the receipt from a state the claim did not see. authRead is false when no read
	// was made (a session with no record, or no account).
	auth     mqttAuthorityLoad
	authRead bool
}

// prepareClear runs everything in a discard that precedes the first mutation: the
// entry uncertainty refusal, the record fence acquisition, and the in-fence
// uncertainty refusal. Nothing is mutated on any path through it.
func (sess *mqttSession) prepareClear() (*mqttPreparedClear, error) {
	// REFUSE AT ENTRY while an earlier write is unresolved, BEFORE any consumer or
	// record deletion. sess.seq may be stale, and this path deletes the shared
	// durables and the record before a stale CAS could fail — so it must not run on
	// a snapshot nobody has classified. The caller defers the discard instead; see
	// sess_uncertain.go.
	sess.clearWindowHook(mqttClearWindowPreEntry)
	if sess.isSeqUncertain() {
		// The SENTINEL belongs here too, not only on the in-fence refusal, because what it
		// encodes is that nothing has been deleted — which is equally true here. Two states
		// reach this check: a caller whose own uncertainty check ran before this call cannot
		// cover the interval since (the two are not under one fence), so an ambiguity armed
		// in between arrives HERE; and expireSession has no early check at all, so it
		// arrives with the write already unresolved. Returning a plain error would have
		// either caller treat it as a partly-done teardown and drop an obligation it could
		// have put back.
		return nil, fmt.Errorf("refusing to discard session %q: %w", sess.id, errMQTTClearRefusedUnresolved)
	}
	sess.clearWindowHook(mqttClearWindowPreFence)

	tok := sess.beginRecordTx()
	if sess.isSeqUncertain() {
		p := &mqttPreparedClear{sess: sess, tok: tok}
		p.abort()
		return nil, fmt.Errorf("refusing to discard session %q: %w", sess.id, errMQTTClearRefusedUnresolved)
	}
	p := &mqttPreparedClear{sess: sess, tok: tok}
	p.auth, p.authRead = sess.settleDiscardAuthority()
	return p, nil
}

// settleDiscardAuthority is the discard boundary's ONE authority read, and it SETTLES rather
// than refuses (F-105 8a, consult 119). A discard that meets a reservation or preparation some
// acceptance left standing settles it first — finalize, cancel or close as its verdict licenses
// — so the discard then sees only states every caller already handles: SERVING at this
// generation (a receipted discard), ABSENT or CLOSED (receiptless, as always), or a record a
// committed proposal already moved (the claim supersedes). Refusing instead would hand every
// discard caller a new outcome to recover from, and some of them — a pending-discard finisher
// that has already taken its obligation — have nowhere to put it.
//
// Placed after the record fence and the in-fence uncertainty check, holding neither sess.mu nor
// asm.mu. It needs no client-ID gate: it touches no binding and no local state, and the
// proposal's receipt slot and the authority CAS arbitrate every race durably. The sequence is
// read under sess.mu and released before any request.
//
// THE RESIDUE, stated rather than hidden: a settlement still unresolved when its bound of one
// request timeout runs out — the store cannot answer — leaves the proposal standing, and the
// discard proceeds exactly as it did before this boundary existed: a receiptless claim, no
// authorized discarding closure, no enrolment. The record CAS still arbitrates that claim
// against the proposal's own metadata commit. docs/CONFORMANCE.md carries it as a
// release-blocking gap.
func (sess *mqttSession) settleDiscardAuthority() (mqttAuthorityLoad, bool) {
	if sess.asm == nil {
		return mqttAuthorityLoad{}, false
	}
	sess.mu.Lock()
	seq := sess.seq
	sess.mu.Unlock()
	if seq == 0 {
		// No record: nothing to arbitrate, and destroy reads no authority for it either.
		return mqttAuthorityLoad{}, false
	}
	b, berr := mqttNewOwnershipBudget(time.Now(), sess.jsa.boundarySettlementTotal())
	if berr != nil {
		return sess.jsa.loadOutboundAuthority(sess.id, sess.asm.sessStreamEpoch), true
	}
	allow := func() error { return mqttOwnershipStepAllowed(b, time.Now(), "discard-boundary settlement") }
	res, load, err := sess.jsa.settleStandingProposal(sess.id, sess.asm.sessStreamEpoch, sess.domainTk,
		b, time.Now, allow, newMQTTConnectOwnershipState())
	sess.asm.mu.RLock()
	hook := sess.asm.testBoundarySettle
	sess.asm.mu.RUnlock()
	if hook != nil {
		hook(sess.id, res.Steps)
	}
	if (err != nil || res.Outcome != mqttOwnershipSettled) && sess.jsa.c != nil {
		reason := res.Reason
		if err != nil {
			reason = err.Error()
		}
		sess.jsa.c.Warnf("MQTT: the discard of session %q met an ownership proposal it could not settle (%s); "+
			"it proceeds without a retirement receipt", sess.id, reason)
	}
	return load, true
}

// abort releases the record fence WITHOUT ending the generation: nothing has been
// mutated, this session is not being discarded after all, and the next resolution
// decides again. endRecordTxNewGeneration would bump recordEpoch and kill every
// token minted before it, for a discard that is not happening.
func (p *mqttPreparedClear) abort() {
	if terr := p.sess.endRecordTx(p.tok); terr != nil {
		p.sess.jsa.c.Errorf("MQTT: %v", terr)
	}
}

// destroy is the destructive half of a discard, under the retirement permit. The
// old tracker's resolution is armed by defer ON ENTRY, so success, a superseded
// claim, an incomplete teardown and every post-reset error all resolve parked
// ingress exactly once — a displaced connection parked at the packet gate is never
// stranded by a failure arm (F-036 unit 1).
//
// EVERY destroy CLOSES the retiring generation's authority with an authorized discarding
// closure before it deletes the record when the boundary's load reads it SERVING at that exact
// generation — the standalone-teardown half of F-105 row 8, Gap 1: a session that ends with no
// displacing successor still has to close its own authority, or a reconnect finds SERVING with
// no record and cannot reserve over it. (The Clean-Start handoff, whose closure the
// finalization writes, does not destroy at all.) Over any other load there is no receipt, and
// the closing driver's licence decides whether the shared closing may run (F-105 8b).
func (p *mqttPreparedClear) destroy(permit mqttRetirementPermit, noWait bool) error {
	sess, tok := p.sess, p.tok
	defer permit.tr.resolveRetired()
	// Released on EVERY exit path, including the error returns below, after which
	// the in-memory state is already reset and old writers must be dead anyway.
	defer func() {
		if err := sess.endRecordTxNewGeneration(tok); err != nil {
			sess.jsa.c.Errorf("MQTT: %v", err)
		}
	}()

	var durs []string
	var pubRelDur string

	// Close the session-affine QoS 2 fence FIRST and wait for every in-flight move
	// to finish. A move captures readiness under the lock and then performs
	// JetStream round trips, so without this a delayed claim CAS could land after
	// the scan below and leave a `claimed` record naming a generation that no
	// longer exists — which contenders would honor forever, stalling a live
	// message on a dead session. See qos2_shared_affine.go.
	q2AffGen, q2AffDur := sess.closeQ2AffAndDrain()

	// Same fence for the INBOUND QoS 2 release, and for the same reason: a PUBREL
	// admitted before this point performs JetStream round trips between its claim and
	// its fan-out, so without this it could deliver a message out of this session
	// after the discard completed. Its durable claim arbitrates that against a peer
	// broker; here it can simply be prevented. See qos2_release_claim.go.
	//
	// Reopened on EVERY exit path, because two callers reuse this object as a fresh
	// session; the re-mint below is what makes that safe.
	sess.closeQoS2ReleaseAndDrain()
	defer sess.reopenQoS2ReleaseFence()

	sess.mu.Lock()
	id := sess.id
	idHash := sess.idHash
	// The RETIRING incarnation. Everything this teardown deletes must be named
	// after it, and the session is re-minted below so a reused object provisions
	// under a generation nothing here can name. See sess_incarnation.go.
	retiringGen := sess.gen
	seq := sess.seq
	// The identity of the record the teardown marker will be derived from, captured
	// BEFORE the reset below for the same reason retiringGen and seq are. It is
	// threaded to claimTeardown explicitly rather than read off the snapshot: the
	// snapshot comes from buildPersistedSessionLocked, which deliberately mints no
	// lineage, so deriving the parent there would stamp an empty one. See
	// sess_fork_lineage.go.
	retiringWID := sess.wid
	// The record as it stands, captured BEFORE the reset below: the teardown marker
	// preserves it so any broker can finish this teardown, and so the orphan sweep
	// keeps reading the durables it names as referenced.
	ps := sess.buildPersistedSessionLocked()
	if l := len(sess.cons); l > 0 {
		durs = make([]string, 0, l)
	}
	for sid, cc := range sess.cons {
		delete(sess.cons, sid)
		// A shared group consumer is not owned by this session alone; it is
		// released (deleted only if no other member remains) after our record is
		// gone, below. Spec5 [4.8.2].
		if cc.DeliverGroup != _EMPTY_ {
			continue
		}
		durs = append(durs, cc.Durable)
	}
	// A draining consumer is this session's too: it survived its membership's removal only
	// because started deliveries were still owed, and the session ending discharges
	// that obligation. See unsub_drain.go.
	durs = append(durs, sess.drainDurablesLocked()...)
	// The shared group durables are NOT a teardown target. Every acquisition this generation
	// holds is listed by its acquisition ledger on $MQTT_shared, and the closing releases
	// them from that ledger — sealing it first, so no late join can add to it — before the
	// record is deleted (closeSharedGeneration). No name derived from this session's own
	// membership decides another member's consumer. Spec5 [4.8.2].
	if sess.pubRelConsumer != nil {
		pubRelDur = sess.pubRelConsumer.Durable
	}

	sess.resetForReuseLocked(permit, mqttNewSessGen(), 0)
	sess.mu.Unlock()

	target := &mqttTeardownTarget{
		id: id, idHash: idHash, gen: retiringGen,
		durs: durs, pubRelDur: pubRelDur,
		q2AffGen: q2AffGen, q2AffDur: q2AffDur,
	}

	// THE DISCARD AUTHORIZATION, captured while the record still stands and the authority
	// still serves this generation (F-105, row 8, Gap 1). When present, the claim below
	// commits a durable teardown receipt in the same atomic batch as the marker, and the
	// close after it ends this generation's authority discarding — so a reconnect finds
	// CLOSED and reserves over it rather than SERVING with a deleted record and no way in.
	//
	// It is built from the loaded authority's tenure and owning transition, never a reload
	// at close time: the receipt names the EXACT ownership being retired, and the close
	// validates the standing authority against it. The targets are the ones just captured
	// under the fence — the affine consumer, its generation, and the PUBREL proof source —
	// which is the only moment they are still readable, since the record that named them is
	// about to go.
	var receiptAuth *mqttRetirementRequest
	var closePrev *mqttOutboundAuthority
	var closePrevSeq uint64
	if seq != 0 && sess.asm != nil && p.authRead {
		// The boundary's own load, never a fresher read (see mqttPreparedClear.auth).
		al := p.auth
		if al.status == mqttOutboundValid && al.rec != nil &&
			al.rec.State == mqttAuthorityServing && al.rec.Gen == retiringGen {
			receiptAuth = &mqttRetirementRequest{
				Gen: retiringGen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
				SessWID: retiringWID, SessSeq: seq,
				Reason: mqttRetireSessionEnd, Discard: true,
			}
			receiptAuth.AffineDurable, receiptAuth.AffineGen, receiptAuth.PubRelDurable =
				mqttAffineRetireDescriptor(q2AffDur, q2AffGen, pubRelDur)
			// Carried into the close's first-attempt fast path: the authority just read and the
			// sequence to CAS against. The claim below writes only the marker and the receipt on
			// $MQTT_sess, never the authority on $MQTT_inflight, so this sequence is still current
			// when the close commits.
			closePrev, closePrevSeq = al.rec, al.seq
		}
	}

	// No persisted record means there is nothing to arbitrate and nothing to
	// release: the only resources in scope are ones this object created, under a
	// generation no other broker can name — and no shared acquisition, since acquiring
	// needs the tenure an accepted CONNECT's record write mints. Delete them and stop.
	//
	// A SERVING authority with seq == 0 would be the forbidden SERVING-with-no-record
	// state, and receiptAuth is nil here by construction (the gate above requires seq != 0),
	// so this arm never discards an authorized generation on the strength of an absent
	// record. Resolving that state is proposal recovery's job (Gap 2), not this arm's.
	if seq == 0 {
		return sess.deleteTeardownTargets(target, noWait)
	}

	// CLAIM. Losing it means the base we claimed is no longer current, so nothing is
	// deleted — not the record, not one consumer. That is a COMPLETE outcome for this
	// teardown, and it is reported as errMQTTClearSuperseded rather than as success:
	// a caller that read it as success went on to write at the sequence this call had
	// just reset to 0, which is how a superseded broker erased the winner's record.
	// Four callers are terminal on it and two re-drive. An indeterminate claim is
	// different again and does fail, so the caller fails closed rather than proceeding
	// as though a teardown had run. See sess_teardown.go and sess_takeover_redrive.go.
	markerSeq, nonce, claim, err := sess.claimTeardown(tok, &ps, seq, retiringWID, receiptAuth)
	switch claim {
	case mqttTeardownClaimWon:
	case mqttTeardownClaimSuperseded:
		return fmt.Errorf("discarding session %q: %w", id, errMQTTClearSuperseded)
	default:
		return err
	}

	// RECORD THE RETIREMENT, before the first deletion and on a subject a resume
	// cannot evict. The marker alone cannot carry this fact: it shares the record's
	// subject, so an unconditional foreign write replaces it and the deletions below
	// become invisible — a CAS excludes a competing CONDITIONAL write off our base (the
	// same-base boundary, docs/CONFORMANCE.md) but not one carrying no expectation. A failure here aborts the teardown with
	// NOTHING deleted, leaving the record marked — the already-handled self-healing
	// state, which the next CONNECT, the expiry path or the sweep finishes, each
	// recording the retirement first. See sess_teardown.go.
	//
	// retiringWID rides along, captured above BEFORE the reset for the same reason the
	// generation and the sequence were. It is no longer STORED — the watermark's rwid field
	// was removed, because a write identity names a record's bytes rather than one
	// occurrence of them — but it is still the PRECONDITION: a record carrying no identity
	// cannot be retired by name, so recordGenRetirement fails the teardown closed before
	// any deletion. Reading sess.wid here would read the zero the reset just wrote.
	if err := sess.recordGenRetirement(target, nonce, retiringWID, markerSeq); err != nil {
		return err
	}
	// CLOSE THE AUTHORITY, discarding, BEFORE the record is deleted and before the shared
	// acquisitions are released (F-105, row 8, Gap 1). The receipt committed with the marker
	// authorizes it; the close ends this generation with a permanent discarding closure that
	// seals the inventory, enrols the affine consumer for its recovery owner and roots the
	// recovery pass. A failure leaves the record MARKED with the receipt beside it, which is
	// the self-healing state: an adopter loads the same receipt and finishes the same close.
	// Ordering is the point — the close commits before the record delete, so a crash between
	// them leaves CLOSED authority and a marker an adopter finishes, never a lost closure.
	if receiptAuth != nil {
		if err := sess.jsa.closeTeardownGenerationFast(id, sess.asm.sessStreamEpoch, sess.domainTk,
			retiringGen, closePrev, closePrevSeq, receiptAuth); err != nil {
			return fmt.Errorf("discarding session %q: %w", id, err)
		}
		// THE DISCARDED GENERATION'S RETIREMENT STARTS HERE, before the shared closing and
		// independently of it (F-105 7d): the closure just committed is the licence of every
		// step, and the prompt drive visits its root with the recovery executor itself.
		sess.asm.kickRecoveryRoot(id, sess.asm.sessStreamEpoch, retiringGen)
	}
	// CLOSE THE SHARED ACQUISITIONS of the RETIRING generation, before the first deletion and
	// before the record is released, through the ONE closing driver (F-105 8b): only once the
	// generation's closure record — or an authority read proving it never served — proves nothing
	// can be admitted under it any more, seal its acquisition ledger (from here no join of this
	// generation can land), release every acquisition it lists (one arbitrated ledger+group
	// CAS each, retiring an incarnation whose count reaches zero), and refuse to go on until
	// the ledger reads sealed and empty. Without that licence — a receiptless teardown over an
	// authority that can still admit, the boundary's settlement residue — it withholds. Every
	// failure leaves the record MARKED, so the adoption, which settles first, repeats exactly
	// this from the durable ledger. Named by retiringGen — sess.gen was just re-minted above.
	if sess.asm != nil {
		neverAuthorized := p.authRead && mqttAuthorityNeverServed(p.auth, retiringGen)
		if err := sess.jsa.closeRetiringGeneration(id, sess.asm.sessStreamEpoch, retiringGen, neverAuthorized); err != nil {
			return fmt.Errorf("discarding session %q: %w", id, err)
		}
	} else if len(ps.SharedSubs) > 0 {
		return fmt.Errorf("discarding session %q: it holds shared acquisitions and no account manager can close them", id)
	}

	// DELETE this incarnation's resources. A nonce re-read before the batch would
	// only be an early-out, never what makes this safe — the read and the JetStream
	// delete cannot be atomic. Generation-scoped naming is what keeps these deletes off
	// the NEXT incarnation's resources; it does not establish that no peer is still
	// serving this one. For the inbound QoS 2 holds that is covered by the bound each
	// removal carries (F-062); for $MQTT_inflight it is the bound F-034 added, its reap's
	// own enumeration watermark. Neither bound arbitrates a genuinely live owner of the
	// same record — that is F-036's predicate. The hook here is how a test parks a broker
	// in exactly that window.
	if h := mqttTestTeardownClaimedHookV.Load(); h != nil {
		(*h)(sess, nonce, markerSeq)
	}
	// WHO OWNS THIS GENERATION'S CLEANUP, decided before the first deletion (F-105, row 8), on
	// the owner's own path exactly as on the adopted one. An indeterminate reading refuses with
	// nothing deleted.
	var own mqttTeardownOwnership
	if sess.asm != nil {
		var cerr error
		own, cerr = sess.asm.classifyTeardownGeneration(id, sess.asm.sessStreamEpoch, retiringGen, target)
		if cerr != nil {
			return fmt.Errorf("discarding session %q: %w", id, cerr)
		}
		own.transfers(target)
		sess.asm.reportUnenrolledTargets(id, retiringGen, own)
	}
	if err := sess.deleteTeardownTargets(target, noWait); err != nil {
		return err
	}
	if sess.asm != nil {
		sess.asm.finishAffinePrerequisites(sess.asm.server(), id, sess.asm.sessStreamEpoch,
			retiringGen, own, target)
	}

	// THE METADATA RELEASE IS GATED ON DURABLE FACTS ABOUT THE CLOSED GENERATION, on the
	// owner's own path exactly as on the adopted one (F-105, row 8). Releasing the record is
	// what lets a fresh acceptance in; if the old generation still has duties nothing will
	// traverse, this release makes that permanent. The gate is VACUOUS for a generation with
	// neither ledger nor closure — the unconverted paths that write no receipt — and it
	// stands here for the ones that do: once this teardown closed the authority discarding,
	// the gate refuses the release until the closure is sealed and its surviving work is
	// enrolled for recovery or certified complete.
	if receiptAuth != nil && sess.asm != nil {
		if facts, ferr := sess.asm.gatherAdoptedReleaseFacts(id, sess.asm.sessStreamEpoch, retiringGen); ferr != nil {
			return fmt.Errorf("discarding session %q: %w", id, ferr)
		} else if gerr := mqttAdoptedReleaseAuthorized(id, retiringGen, facts); gerr != nil {
			return fmt.Errorf("discarding session %q: %w", id, gerr)
		}
	}

	// RELEASE. Always confirmed, even on the no-wait path — see releaseTeardown. The shared
	// acquisitions were released from the sealed ledger ABOVE, before this; nothing after
	// the record's release reads it. Spec5 [4.8.2].
	switch res, cur, curSeq, rerr := sess.releaseTeardown(tok, target, nonce, markerSeq); res {
	case mqttTeardownReleased:
		// Ours, and gone. The shared durables are now safe to consider.
	case mqttTeardownSuperseded:
		// The record moved past the base this teardown claimed. Whatever wrote it counts
		// as a holder of the membership, so the shared durables are not ours to release
		// either — which is the operative fact, and does not require naming an owner the
		// sentinel cannot name. Same sentinel as the claim arm, and it says the same
		// thing: the base is not current. It does NOT say
		// nothing was deleted — the deletions above already ran, legitimately, because
		// the claim was won.
		//
		// That is the claim-won deletion window, and this is where it is REPAIRED rather
		// than only reported: when the survivor carries the generation we just retired,
		// it names resources that no longer exist, so it is condemned with a fresh
		// teardown marker before this returns. The sentinel and every caller's
		// disposition are unchanged. See sess_teardown.go.
		sess.condemnRetiredSurvivor(tok, target, cur, curSeq)
		return fmt.Errorf("discarding session %q: %w", id, errMQTTClearSuperseded)
	default:
		return rerr
	}

	// The shared group durables were released BEFORE the record went, from the sealed
	// acquisition ledger (closeSharedGeneration above); nothing is left to decide here.
	return nil
}

// This will update the session record for this client in the account's MQTT
// sessions stream if the session had any change in the subscriptions.
//
// tok identifies the caller's open record transaction: the membership mutations
// below and the save that persists them are one transaction, so no other writer
// can observe (or persist) a half-applied membership change. Callers that are
// themselves part of a larger packet-scoped transaction pass its token down.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
// update persists this packet's membership changes. Callers that must know whether a
// failure was a per-subject CAS REJECTION use updateWithDetail instead: the error returned
// here cannot answer that, because save() re-formats the store error with %v and destroys
// its NATS identity.
func (sess *mqttSession) update(tok recordTx, filters []*mqttFilter, add bool) error {
	_, err := sess.updateWithDetail(tok, filters, add)
	return err
}

// updateWithDetail is update with the store-boundary detail preserved.
//
// It exists for the early per-filter membership save, which must distinguish a CAS
// rejection — evidence that the record MOVED — from a local failure or an ambiguity, which
// are evidence of neither. saveDefinitelyFailed cannot make that distinction: it
// deliberately merges a CAS rejection with a local failure, a refused transaction, and a
// refusal caused by an earlier uncertainty. See mqttSubPacketFacts.recordCASRejected.
func (sess *mqttSession) updateWithDetail(tok recordTx, filters []*mqttFilter, add bool) (mqttSessSaveDetail, error) {
	return sess.updateWithDetailConverting(tok, filters, add, nil)
}

// updateWithDetailConverting is updateWithDetail with a hook that runs INSIDE the
// membership mutations' critical section, after them and before the lock is released.
//
// It exists for one caller: a SUBSCRIBE packet that must turn a QoS 0 downgrade's consumer
// into a drain in the SAME record that carries the downgraded membership (I4, the atomicity
// the UNSUBSCRIBE drain already has). Two things make that boundary the right one:
//
//   - The lock. The membership maps and sess.cons are read under sess.mu by the delivery
//     callbacks, so applying the downgrade and moving the consumer under one acquisition is
//     what stops a callback observing the two apart. The record fence is the packet's
//     transaction, not this lock — the lock's claim is READER VISIBILITY.
//   - needUpdate. A conversion must never be left in memory with nothing persisted, so the
//     hook's return value ORs into it. Its own nil return proves nothing: this function
//     returns a nil error when it writes nothing at all.
//
// Every other caller passes nil and is unaffected.
func (sess *mqttSession) updateWithDetailConverting(tok recordTx, filters []*mqttFilter, add bool,
	inLock func() bool) (mqttSessSaveDetail, error) {
	// Evaluate if we need to persist anything.
	var needUpdate bool
	// The membership maps are read under sess.mu by the delivery callbacks, so the
	// mutations below must hold it. Callers are serialized against each other by
	// the session's locked-map entry, but that says nothing to a concurrent READER
	// — which is a genuine unsynchronized access, not merely a test artifact. The
	// lock is released before save(), which takes it itself.
	sess.mu.Lock()
	for _, f := range filters {
		// Shared subscriptions live in a separate map keyed by session key; plain
		// subscriptions in subs keyed by their NATS subject. rapSubs/subIDs are
		// shared but keyed by session key (collision-free). Spec5 [4.8.2].
		key := f.sessionKey()
		if add {
			if f.qos >= mqttSubAckFailure {
				continue
			}
			if f.shared {
				// The MEMBERSHIP is the filter's: on the acquire arm the acquisitions J1 minted,
				// on the refresh and restore arms the standing references with this packet's
				// QoS. A shared filter with none is a programming error, refused rather than
				// persisted as a membership no ledger entry backs.
				if f.sharedM == nil {
					sess.mu.Unlock()
					return mqttSessSaveDetail{}, fmt.Errorf("shared subscription %q reached the record with no acquisition references", key)
				}
				if cur, ok := sess.sharedSubs[key]; !ok || cur != *f.sharedM {
					if sess.sharedSubs == nil {
						sess.sharedSubs = make(map[string]mqttSharedMembership)
					}
					sess.sharedSubs[key] = *f.sharedM
					needUpdate = true
				}
			} else if qos, ok := sess.subs[key]; !ok || qos != f.qos {
				if sess.subs == nil {
					sess.subs = make(map[string]byte)
				}
				sess.subs[key] = f.qos
				needUpdate = true
			}
			// Track No Local and Retain As Published in separate sets so the
			// persisted QoS byte stays a valid 0..2 for every reader of the record.
			// No Local never applies to shared subscriptions. Spec5 [3.8.3.1].
			var changed bool
			if !f.shared {
				if sess.noLocalSubs, changed = mqttUpdateFilterOptSet(sess.noLocalSubs, key, f.noLocal); changed {
					needUpdate = true
				}
			}
			if sess.rapSubs, changed = mqttUpdateFilterOptSet(sess.rapSubs, key, f.retainAsPublished); changed {
				needUpdate = true
			}
			// f.subID == 0 clears any prior identifier: a re-SUBSCRIBE without one
			// removes it. Spec5 [3.8.2.1.2].
			if sess.subIDs, changed = mqttUpdateFilterSubID(sess.subIDs, key, f.subID); changed {
				needUpdate = true
			}
		} else {
			if f.shared {
				// THE RELEASE VERDICT rides the write that removes the membership: the
				// acquisitions the membership named are owed a release, and this record is
				// what makes that recoverable by any later reader. A filter whose join
				// acquired and never committed (its membership is not in the map) carries its
				// acquisitions itself, and they are owed a verdict just the same.
				if m, ok := sess.sharedSubs[key]; ok {
					delete(sess.sharedSubs, key)
					sess.addAcqReleaseVerdictsLocked(key, m)
					needUpdate = true
				} else if f.sharedM != nil {
					sess.addAcqReleaseVerdictsLocked(key, *f.sharedM)
					needUpdate = true
				}
			} else if _, ok := sess.subs[key]; ok {
				delete(sess.subs, key)
				needUpdate = true
			}
			var changed bool
			if sess.noLocalSubs, changed = mqttUpdateFilterOptSet(sess.noLocalSubs, key, false); changed {
				needUpdate = true
			}
			if sess.rapSubs, changed = mqttUpdateFilterOptSet(sess.rapSubs, key, false); changed {
				needUpdate = true
			}
			if sess.subIDs, changed = mqttUpdateFilterSubID(sess.subIDs, key, 0); changed {
				needUpdate = true
			}
		}
	}
	// The conversion runs HERE: after the membership this record will carry, before the
	// lock that makes the pair atomic to every reader is released, and before the save.
	if inLock != nil && inLock() {
		needUpdate = true
	}
	sess.mu.Unlock()

	var detail mqttSessSaveDetail
	var err error
	if needUpdate {
		detail, err = sess.saveWithDetail(tok)
	}
	return detail, err
}
