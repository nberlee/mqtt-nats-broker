// Reaping orphaned QoS 2 state, and finishing the teardowns that strand it.
//
// Three durable structures outlive the session that created them when a broker
// dies in the wrong window, and nothing else ever revisits them:
//
//   - an inbound QoS 2 hold record on $MQTT_qos2in whose session record is gone,
//     together with the sibling release claim that arbitrates it;
//   - a session-affine $MQTT_Q2AFF_* consumer on $MQTT_out whose session record is
//     gone (an older broker in a mixed-version window ignores Q2Aff entirely); and
//   - an assignment claim on $MQTT_qos2assign whose winner generation no longer
//     exists and which no contender happens to revisit.
//
// None of them loses a message — a stalled claim is honored, never released — but
// all three accumulate. This file converges them, and it converges the fourth
// thing that would otherwise pin the other three in place: a teardown marker whose
// broker died after claiming the record. sess_teardown.go promises such a marker is
// finished by the next CONNECT that can interpret the record or by the orphan sweep —
// and for a reserved-namespace record, which every CONNECT refuses, this pass is the
// ONLY finisher (a record with no valid generation is finished by neither: the
// adoption below validates before it deletes) — and a marked record
// deliberately reads as a REFERENCE to everything it names, so without the adoption
// pass below a never-returning client's QoS 2 state could never become eligible.
//
// # What makes a deletion legal here
//
// Ownership is the (client-ID hash, generation) pair carried in the subject or the
// durable name, never the hash alone — the hash is reused by every later session of
// the same client ID. A resource is an orphan only when its generation is
// AUTHORITATIVELY absent from a COMPLETE session-record walk: a failed lookup, a
// partial scan, an undecodable record or an unvalidatable generation all abort the
// destructive pass rather than being read as absence (invariant 1). Candidates are
// listed BEFORE that walk, so a resource created between the two is never
// classified from a snapshot that predates it, and local in-memory sessions count
// as references so the create-before-save window is covered on this broker.
//
// # Nothing here subtracts two timestamps
//
// There is no clock to compare against. A JetStream store time is assigned by
// whichever server led that stream's Raft group at that instant; four streams are
// four groups, and a single stream spans several clocks across a leadership change.
// Subtracting one from another says nothing about elapsed time. Comparing either
// against this process's time.Now() is worse, and for the reason the release claim's
// self-reported At was rejected: the bridge is an external client on its own host.
//
// So the grace is not an age. It is a STABLE-OBSERVATION requirement, measured on
// the one clock this process can trust — its own monotonic one. A candidate becomes
// eligible only when it has been seen, unchanged and authoritatively orphaned,
// across a span of local monotonic time at least as long as the grace, and never on
// the pass that first saw it. Stored timestamps are diagnostic; only IDENTITY is
// arithmetic here (a stream sequence, a consumer's Created used purely as a
// fingerprint, a teardown nonce), and any change to it restarts the wait. See
// mqttSweepQuarantine.
//
// The costs are honest: a broker restarting more often than the grace never reaps,
// and convergence takes several passes. Both err towards leaving state alone.
//
// # Two grace policies, because they protect different things
//
// mqttOrphanSweepGrace (five minutes) covers construction windows: a resource
// created just before the record save that references it. mqttQoS2ReleaseOrphanGrace
// (one hour) covers release claims, which arbitrate against a process that may be
// paused after loading a payload and before delivering it. Coupling the second to
// the first would retire an arbitration record on a construction-window timescale.
// Both are MINIMUM observation spans, not deadlines: cleanup may land on any later
// pass, and any change to a record restarts its wait.
//
// # Ordering: the hold dies before its fence
//
// The single most load-bearing rule here is that an aged release claim is deleted
// only AFTER its hold record is confirmed gone (invariant 10). A `discarded` marker
// is normally reaped by nobody — it is the only durable proof that a late releaser
// must lose — and destroying the hold first is the sole thing that licenses
// retiring it. A releaser that preloaded the payload and resumes afterwards creates
// a fresh `releasing` claim against an empty subject, then performs its mandatory
// authoritative re-read (verifyQoS2HoldForRelease) and gets mqttQoS2VerifyGone, so
// it answers the client and delivers nothing. Reverse the two steps and that same
// re-read returns mqttQoS2VerifyHeld and the message is fanned out a second time
// from a session that no longer exists.
//
// The affine side has the mirror-image rule: an assignment is committed Discarded
// BEFORE its consumer is deleted (invariant 5). $MQTT_out is interest-retained, so
// deleting the consumer first would drop the copies it still held while the claims
// still described a live delivery — turning a session discard that MQTT 5.0 [4.8.2]
// permits into a recorded Lost message.
//
// # What this file deliberately does not do
//
// It never manufactures affine completion, never releases an assignment back to its
// shared group, and never deletes an assignment through anything but
// reapQ2AffClaim, whose reapable() predicate already refuses a claim marked Lost.
// It adds no MaxAge to any stream: JetStream age eviction cannot perform the
// ordering or the cross-stream proofs above, so protocol state stays explicitly
// reconciled.

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// mqttOrphanSweepRuns counts completed orphan-sweep entries. Test-only: it is the
// only way to observe that the STARTUP call site fired, since a fresh account
// manager begins sweeping before a test can reach into it.
var mqttOrphanSweepRuns atomic.Int64

var (
	// mqttQoS2ReleaseOrphanGrace is how old an inbound QoS 2 RELEASE CLAIM must be
	// before the sweep may retire it. Deliberately far longer than
	// mqttOrphanSweepGrace and than the 300 ms discard wait, and long enough to span
	// several default sweep intervals: a claim arbitrates against a releaser that may
	// be paused between loading a payload and delivering it, which is not the same
	// timescale as a consumer that was created moments before its record save. A var
	// so tests can shorten it.
	mqttQoS2ReleaseOrphanGrace = time.Hour

	// Discovery budgets for the inbound walk, per tick. The message-get API returns
	// the complete record, so scanning a hold transfers its PUBLISH payload even
	// though ownership is entirely in the subject; without a byte bound a single pass
	// over a large leaked history could move gigabytes. Both are checked AFTER the
	// record is accounted, so the byte cap may be exceeded by at most one
	// max_payload-sized hold. Vars so tests can shrink them.
	mqttQoS2SweepRecordBudget = 512
	mqttQoS2SweepByteBudget   = int64(64 << 20)

	// mqttQ2AffSweepMutationBudget bounds assignment CAS transitions per tick. It
	// bounds MUTATIONS only: the inventory and session-reference scans that authorize
	// a consumer deletion are never capped (invariant 13).
	mqttQ2AffSweepMutationBudget = 256
)

// sweepOrphanedQoS2State reconciles abandoned teardowns and the durable QoS 2 state
// whose owning session generation is authoritatively absent.
//
// Best effort at the top level: a failure logs and retries on the next tick. Within
// a pass an indeterminate prerequisite stops the destructive phase it protects, and
// never merely the one record that could not be resolved.
//
// Runs from the account session manager's sweep goroutine, beside
// sweepSessionExpiries and sweepOrphanedConsumers.
func (as *mqttAccountSessionManager) sweepOrphanedQoS2State(s *Server) {
	// Counts every COMPLETED pass, including the one the account manager runs at
	// startup. Deferred rather than incremented on entry: an observer waiting on this
	// needs to know the pass has finished touching state, not that it began. Atomic
	// and package-level because it is observed across account managers, whose sweep
	// goroutines run concurrently with any test watching them.
	defer mqttOrphanSweepRuns.Add(1)

	// (1) Finish abandoned teardowns FIRST, on their own session snapshot, which is
	// then discarded. A marked record preserves references precisely so recovery does
	// not race ahead of teardown, so classifying orphans against a pre-adoption view
	// would protect resources the adoption has just deleted.
	as.adoptAbandonedTeardowns(s)

	// (2) Candidates BEFORE references (invariant 3). A candidate the later walk does
	// not reference was already unreferenced when it was listed; the reverse order
	// would classify a resource created after the walk.
	inbound, ierr := as.collectInboundQoS2Candidates(s)
	inflight, flerr := as.collectInflightCandidates(s)
	inventory, cerr := as.collectQ2AffInventory()
	consumers, conserr := as.jsa.consumerList(mqttOutStreamName)

	// (3) One authoritative, COMPLETE reference set. Anything indeterminate here
	// aborts every destructive phase below, because all of them act on absence.
	refs, rerr := as.buildSessionRefs()
	if rerr != nil {
		s.Debugf("MQTT orphaned QoS 2 sweep ended: %v", rerr)
		return
	}

	if ierr != nil {
		s.Debugf("MQTT orphaned QoS 2 sweep: inbound scan ended: %v", ierr)
	} else {
		as.reconcileInboundQoS2(s, inbound, refs)
	}

	// Outbound in-flight registers. Ordered here rather than with the affine phases
	// because it shares nothing with them: it needs no inventory, no second barrier
	// and no ordering against a consumer delete. It does share the mutation budget,
	// so one class cannot starve the others.
	inflightBudget := mqttQ2AffSweepMutationBudget
	if flerr != nil {
		s.Debugf("MQTT orphaned QoS 2 sweep: in-flight register scan ended: %v", flerr)
	} else {
		as.reconcileOrphanInflight(s, inflight, refs, &inflightBudget)
	}

	if cerr != nil {
		// A malformed claim aborts the whole inventory, which is what protects every
		// affine consumer from deletion during this pass.
		s.Warnf("MQTT orphaned QoS 2 sweep: assignment inventory incomplete, skipping affine reconciliation: %v", cerr)
		return
	}
	budget := mqttQ2AffSweepMutationBudget
	as.reconcileOrphanQ2AffClaims(s, inventory, refs, &budget)

	if conserr != nil {
		s.Debugf("MQTT orphaned QoS 2 sweep: unable to list $MQTT_out consumers: %v", conserr)
		return
	}
	as.deleteOrphanQ2AffConsumers(s, inventory, consumers, refs, &budget)
}

// ---------------------------------------------------------------------------
// Age
// ---------------------------------------------------------------------------

// The three knobs, resolved per pass so a test can scope them to one account
// manager rather than racing the sweep goroutine through a package-level variable.
func (as *mqttAccountSessionManager) orphanGrace() time.Duration {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if as.testOrphanGrace != nil {
		return *as.testOrphanGrace
	}
	return mqttOrphanSweepGrace
}

func (as *mqttAccountSessionManager) qos2ReleaseGrace() time.Duration {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if as.testQoS2ReleaseGrace != nil {
		return *as.testQoS2ReleaseGrace
	}
	return mqttQoS2ReleaseOrphanGrace
}

func (as *mqttAccountSessionManager) qos2SweepByteLimit() int64 {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if as.testQoS2SweepByteLimit != nil {
		return *as.testQoS2SweepByteLimit
	}
	return mqttQoS2SweepByteBudget
}

func (as *mqttAccountSessionManager) qos2SweepRecordLimit() int {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if as.testQoS2SweepRecordLimit != nil {
		return *as.testQoS2SweepRecordLimit
	}
	return mqttQoS2SweepRecordBudget
}

// mqttSweepQuarantine answers "has this exact resource been continuously orphaned
// long enough to act on?" without ever subtracting two wall-clock timestamps.
//
// THERE IS NO SINGLE CLOCK TO SUBTRACT AGAINST. A JetStream store time is assigned
// by whichever server led that stream's Raft group at that moment, and a consumer's
// Created by whichever led the consumer's. Four streams are four Raft groups, and
// even one stream spans several clocks across a leadership change — so a fast
// leader's timestamp minus a slow leader's says nothing about elapsed time, and the
// difference can exceed the grace while no time has passed at all. Comparing either
// against this process's time.Now() is worse still, for the reason the release
// claim's self-reported `At` was rejected: the bridge is an external client on its
// own host.
//
// What this process CAN measure honestly is a duration on its own monotonic clock.
// So the grace stops being an age and becomes a STABLE-OBSERVATION requirement: the
// resource must be seen, unchanged and authoritatively orphaned, across a span of
// local monotonic time at least as long as the grace. A JetStream timestamp is never
// arithmetic here; only the record's IDENTITY is (a stream sequence, or a consumer's
// Created used purely as a fingerprint), and any change to it restarts the wait.
//
// This is strictly stronger than an age test. It says the resource has actually been
// observed to be an orphan for that long, rather than that it was created that long
// ago by somebody else's clock. The costs are honest and bounded: a broker that
// restarts more often than the grace never reaps (its observations start over, which
// is the safe direction), and convergence takes at least two passes instead of one.
type mqttSweepQuarantine struct {
	mu      sync.Mutex
	entries map[string]*mqttQuarantineEntry
	max     int
	stale   time.Duration
}

type mqttQuarantineEntry struct {
	fingerprint string    // identity of the exact record; a change restarts the wait
	since       time.Time // first observation, monotonic
	lastSeen    time.Time // for bounding the map, monotonic
}

const (
	mqttSweepQuarantineMax = 8192
	// How long an unrevisited entry may hold its slot. Generous on purpose: a bounded
	// walk cycles the whole stream well inside this, so anything older is a resource
	// that disappeared without the sweep observing it.
	mqttSweepQuarantineStale = 24 * time.Hour
)

func newMQTTSweepQuarantine() *mqttSweepQuarantine {
	return &mqttSweepQuarantine{
		entries: make(map[string]*mqttQuarantineEntry),
		max:     mqttSweepQuarantineMax,
		stale:   mqttSweepQuarantineStale,
	}
}

// observedOrphanFor records that key is orphaned right now, carrying this exact
// fingerprint, and reports whether that has been continuously true for grace.
//
// A first sighting, or a sighting with a different fingerprint, records the identity
// and authorizes NOTHING — even at a zero grace. One observation can never be
// enough: eligibility requires a later, independent authoritative check, so a
// candidate glimpsed once mid-construction cannot be acted on by the pass that
// glimpsed it.
// When the table is full it REFUSES new keys rather than evicting to make room.
// Evicting is what a cache does, and it is exactly wrong here: a deterministic scan
// revisits keys in the same order, so each newly admitted key would evict the one
// due to be revisited next, every call would look like a first sighting, and NOTHING
// in a backlog larger than the cap could ever accumulate two observations. Refusing
// instead keeps a stable resident cohort that ages, qualifies, is acted on, and is
// forgotten — which frees its slot for the next cohort. Progress is slower on a huge
// backlog, but it is progress rather than a livelock.
func (q *mqttSweepQuarantine) observedOrphanFor(key, fingerprint string, grace time.Duration) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	if e := q.entries[key]; e != nil {
		if e.fingerprint == fingerprint {
			e.lastSeen = now
			return now.Sub(e.since) >= grace
		}
		// The identity changed: replace in place, no capacity question to answer.
		q.entries[key] = &mqttQuarantineEntry{fingerprint: fingerprint, since: now, lastSeen: now}
		return false
	}
	if len(q.entries) >= q.max {
		q.pruneStaleLocked(now)
		if len(q.entries) >= q.max {
			return false // no slot; this key waits for the resident cohort to clear
		}
	}
	q.entries[key] = &mqttQuarantineEntry{fingerprint: fingerprint, since: now, lastSeen: now}
	return false
}

// pruneStaleLocked drops entries nothing has revisited in a very long time. Without
// it a cohort of resources that vanished without the sweep noticing would hold its
// slots forever and the table could never admit anyone else. The threshold is far
// beyond any legitimate revisit interval — a bounded walk cycles the whole stream
// long before this — so a pruned entry is one whose resource is gone or unreachable.
func (q *mqttSweepQuarantine) pruneStaleLocked(now time.Time) {
	for k, e := range q.entries {
		if now.Sub(e.lastSeen) > q.stale {
			delete(q.entries, k)
		}
	}
}

// forget restarts key's wait. Called the moment a resource is observed to be
// REFERENCED again: a session that came back must not inherit the quarantine time
// accumulated while it looked absent.
func (q *mqttSweepQuarantine) forget(key string) {
	q.mu.Lock()
	delete(q.entries, key)
	q.mu.Unlock()
}

// quarantine returns this account manager's observation table, creating it on first
// use.
func (as *mqttAccountSessionManager) quarantine() *mqttSweepQuarantine {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.sweepQuarantine == nil {
		as.sweepQuarantine = newMQTTSweepQuarantine()
	}
	return as.sweepQuarantine
}

// Quarantine keys. Each names one resource; the fingerprint that accompanies it
// names the exact record or consumer currently occupying it.
func mqttQuarantineHoldKey(subject string) string     { return "hold|" + subject }
func mqttQuarantineClaimKey(subject string) string    { return "claim|" + subject }
func mqttQuarantineTeardownKey(idHash string) string  { return "teardown|" + idHash }
func mqttQuarantineAssignKey(subject string) string   { return "assign|" + subject }
func mqttQuarantineConsumerKey(durable string) string { return "q2aff|" + durable }
func mqttQuarantineInflightKey(subject string) string { return "inflight|" + subject }

func mqttQuarantineSeqFP(seq uint64) string { return strconv.FormatUint(seq, 10) }

// forgetIfQ2AffClaimGone releases a claim's quarantine slot once the record is
// authoritatively absent. Only absence frees it: a claim that is merely retained
// (its source can still redeliver) must keep the wait it has accumulated.
func (as *mqttAccountSessionManager) forgetIfQ2AffClaimGone(subject string) {
	cur, _, err := as.loadQ2AffClaim(subject)
	if err == nil && cur == nil {
		as.quarantine().forget(mqttQuarantineAssignKey(subject))
	}
}

// ---------------------------------------------------------------------------
// Teardown marker adoption
// ---------------------------------------------------------------------------

// adoptAbandonedTeardowns finishes teardowns whose broker committed the marker and
// then died. Without it a marked record is a permanent reference to every resource
// it names, so that session's QoS 2 state could never become orphan-eligible.
//
// Eligibility is a stable observation, not an age: no stored timestamp participates,
// neither the record's JetStream store time nor Teardown.StartedAt. The fingerprint
// is the record's sequence plus the teardown nonce, so a resave or a peer's teardown
// restarts the wait — which is what "unchanged" has to mean for a marker.
//
// A young marker, a malformed record, a failed re-read or a failed adoption all
// leave the record exactly as it was, and it continues to protect everything it
// names. Concurrent adoption — by the original broker waking up, by another
// sweeper, or by a CONNECT — is safe under the existing teardown release
// reconciliation: both delete the same retired, generation-scoped names, one wins
// the record delete, and the loser's classifying re-read reports released or
// superseded.
func (as *mqttAccountSessionManager) adoptAbandonedTeardowns(s *Server) {
	grace, q := as.orphanGrace(), as.quarantine()
	opts := s.getOpts()
	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT teardown adoption sweep ended: %v", err)
			}
			return
		}
		seq = smsg.Sequence + 1

		idHash, perr := mqttSessIDHashFromSubject(smsg.Subject, as.domainTk)
		if perr != nil {
			// Not ours to interpret, and not ours to adopt.
			continue
		}
		ps := &mqttPersistedSession{}
		if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
			continue
		}
		if ps.Teardown == nil || ps.Teardown.Nonce == _EMPTY_ {
			continue
		}
		// Every deletion target is validated for the SAME reason the generation is, and it is
		// easy to miss because the generation check looks like it covers the record.
		// mqttTeardownTargetFromRecord copies ps.Q2Aff.Durable VERBATIM and the adoption
		// deletes that consumer — so a record naming a durable that is not this
		// session's would delete another session's $MQTT_out consumer, taking its
		// interest-retained copies with it. A record is not a trusted input here any
		// more than it is in buildSessionRefs, which applies exactly these checks.
		if verr := mqttValidateTeardownTargets(ps, idHash); verr != nil {
			s.Warnf("MQTT teardown adoption: refusing to adopt the teardown on %q: %v", smsg.Subject, verr)
			continue
		}
		// The marker must have been seen, unchanged, for the whole grace. A resave
		// moves the record's sequence and a peer's teardown changes the nonce; either
		// restarts the wait, which is exactly what "unchanged" has to mean here.
		fp := mqttQuarantineSeqFP(smsg.Sequence) + "|" + ps.Teardown.Nonce
		if !q.observedOrphanFor(mqttQuarantineTeardownKey(idHash), fp, grace) {
			continue
		}

		// Re-read: the snapshot is older than the record, and an adoption must act on
		// the marker it verified. A resave (or a peer's completed teardown) moves or
		// removes it, and finishAdoptedTeardown releases at an exact sequence.
		cur, curSeq, rerr := as.loadFenceSession(idHash)
		if rerr != nil || cur == nil || cur.Teardown == nil ||
			cur.Teardown.Nonce != ps.Teardown.Nonce || curSeq != smsg.Sequence {
			continue
		}
		if err := as.finishAdoptedTeardown(cur, idHash, curSeq, opts); err != nil {
			if !errors.Is(err, errMQTTTeardownAdoptionSuperseded) {
				// Everything this record names stays referenced until a later pass.
				s.Warnf("MQTT teardown adoption: unable to finish the abandoned teardown of session %q: %v", idHash, err)
				continue
			}
			// TERMINAL, and a success for this sweep: a newer record holds the subject,
			// so the teardown this pass quarantined was not abandoned after all —
			// somebody finished it. Given its own branch because the quarantine entry
			// must still be FORGOTTEN: left standing it would keep re-observing a marker
			// that no longer exists, and every later pass would re-derive the same
			// verdict. The warning arm would skip exactly that.
			s.Debugf("MQTT teardown adoption: the teardown of session %q was superseded; nothing was left to finish", idHash)
			q.forget(mqttQuarantineTeardownKey(idHash))
			continue
		}
		s.Debugf("MQTT teardown adoption: finished the abandoned teardown of session %q", idHash)
		q.forget(mqttQuarantineTeardownKey(idHash))
	}
}

// ---------------------------------------------------------------------------
// The authoritative session-reference snapshot
// ---------------------------------------------------------------------------

// mqttSessRefs is every session identity a live record or a local in-memory session
// claims. Absence from it is the ONLY thing that makes a resource an orphan, so it
// is built completely or not at all.
type mqttSessRefs struct {
	inbound     map[string]struct{} // (idHash, incarnation generation)
	affine      map[string]struct{} // (idHash, Q2Aff generation)
	affDurables map[string]struct{}
}

func mqttSessRefKey(idHash, gen string) string { return idHash + "\x00" + gen }

func (r *mqttSessRefs) hasInbound(idHash, gen string) bool {
	_, ok := r.inbound[mqttSessRefKey(idHash, gen)]
	return ok
}

func (r *mqttSessRefs) hasAffine(idHash, gen string) bool {
	_, ok := r.affine[mqttSessRefKey(idHash, gen)]
	return ok
}

// buildSessionRefs walks every session record in the account and returns the
// identities they reference, plus the equivalent references from local in-memory
// sessions.
//
// Local sessions are included because their in-memory state may be NEWER than their
// persisted record: a save can be in flight, and a resource created just before it
// would otherwise look unreferenced. That covers the create-before-save window on
// this broker; the age grace covers it on every other one.
//
// Every failure is fatal to the caller's destructive pass. An unattributable
// subject, an undecodable record, a client ID that does not hash back to its subject
// token, or a generation that cannot be validated could each hide a live reference,
// and a hidden reference is a deleted live resource. A record carrying a teardown
// marker still counts: its preserved content exists so recovery and orphan
// reconciliation do not race ahead of the teardown.
func (as *mqttAccountSessionManager) buildSessionRefs() (*mqttSessRefs, error) {
	refs := &mqttSessRefs{
		inbound:     make(map[string]struct{}),
		affine:      make(map[string]struct{}),
		affDurables: make(map[string]struct{}),
	}

	as.mu.RLock()
	sesss := make([]*mqttSession, 0, len(as.sessions))
	for _, sess := range as.sessions {
		sesss = append(sesss, sess)
	}
	as.mu.RUnlock()
	for _, sess := range sesss {
		sess.mu.Lock()
		idHash, gen, affGen := sess.idHash, sess.gen, sess.q2AffGen
		var affDur string
		if sess.q2AffConsumer != nil {
			affDur = sess.q2AffConsumer.Durable
		}
		sess.mu.Unlock()
		if idHash == _EMPTY_ {
			continue
		}
		if gen != _EMPTY_ {
			refs.inbound[mqttSessRefKey(idHash, gen)] = struct{}{}
		}
		if affGen != _EMPTY_ {
			refs.affine[mqttSessRefKey(idHash, affGen)] = struct{}{}
			_, _, dur := mqttQ2AffineNames(idHash, affGen)
			refs.affDurables[dur] = struct{}{}
		}
		if affDur != _EMPTY_ {
			refs.affDurables[affDur] = struct{}{}
		}
	}

	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return nil, fmt.Errorf("session record walk failed before reaching the end: %w", err)
			}
			return refs, nil
		}
		seq = smsg.Sequence + 1

		idHash, perr := mqttSessIDHashFromSubject(smsg.Subject, as.domainTk)
		if perr != nil {
			return nil, fmt.Errorf("session record on %q cannot be attributed: %w", smsg.Subject, perr)
		}
		ps := &mqttPersistedSession{}
		if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
			return nil, fmt.Errorf("session record on %q at sequence %d is undecodable: %w", smsg.Subject, smsg.Sequence, uerr)
		}
		if verr := mqttValidateSessionRecord(ps, idHash); verr != nil {
			return nil, fmt.Errorf("session record on %q: %w", smsg.Subject, verr)
		}
		refs.inbound[mqttSessRefKey(idHash, ps.Gen)] = struct{}{}
		if ps.Q2AffGen != _EMPTY_ {
			refs.affine[mqttSessRefKey(idHash, ps.Q2AffGen)] = struct{}{}
			_, _, dur := mqttQ2AffineNames(idHash, ps.Q2AffGen)
			refs.affDurables[dur] = struct{}{}
			if ps.Q2Aff != nil {
				refs.affDurables[ps.Q2Aff.Durable] = struct{}{}
			}
		}
	}
}

// mqttValidateRecordAffinePair checks a persisted record's affine generation and
// consumer config against each other and against the session they claim to belong
// to. Shared deliberately: the reference walk needs it so a malformed pair cannot
// hide a live reference, and teardown adoption needs the SAME check because it
// deletes the durable the record names. Splitting them once let adoption delete a
// consumer the reference walk would have refused to interpret.
func mqttValidateRecordAffinePair(ps *mqttPersistedSession, idHash string) error {
	if ps.Q2AffGen == _EMPTY_ {
		if ps.Q2Aff != nil {
			// A consumer config with no generation to scope it: the pair is malformed,
			// and neither half can be trusted to say which durable is referenced.
			return fmt.Errorf("record carries an affine consumer config with no affine generation")
		}
		return nil
	}
	if !mqttValidQ2AffGen(ps.Q2AffGen) {
		return fmt.Errorf("record carries a malformed affine generation %q", ps.Q2AffGen)
	}
	if ps.Q2Aff != nil {
		if verr := mqttValidateQ2AffineNames(ps.Q2Aff, idHash, ps.Q2AffGen); verr != nil {
			return fmt.Errorf("record carries an affine consumer config that does not match its generation: %w", verr)
		}
	}
	return nil
}

// mqttValidateSessionRecord is the single definition of "a session record this
// sweep can interpret". Every path that reads a record shares it.
//
// Sharing is the point. The reference walk fails closed on a record it cannot
// interpret, but the post-snapshot RE-READS are what actually authorize a mutation —
// and each had grown its own hand-rolled subset of the checks, so the authoritative
// step was weaker than the snapshot it was re-verifying. A record whose ID does not
// hash back to its subject, or whose affine pair is inconsistent, would then take
// the "valid record for a different generation" path and be read as PROOF OF
// ABSENCE: a live assignment discarded, or a referenced consumer deleted with its
// interest-retained copies. Uninterpretable must mean indeterminate everywhere.
func mqttValidateSessionRecord(ps *mqttPersistedSession, idHash string) error {
	if getHash(ps.ID) != idHash {
		return fmt.Errorf("record names a client ID that does not hash back to %q", idHash)
	}
	if _, gerr := mqttSessGenFromRecord(ps.Gen); gerr != nil {
		return gerr
	}
	return mqttValidateRecordAffinePair(ps, idHash)
}

// mqttValidateTeardownTargets checks that every durable a teardown adoption would
// DELETE actually belongs to the session whose record names it.
//
// mqttTeardownTargetFromRecord copies `Cons`, `Drains` and `PubRel` durable names
// verbatim, exactly as it does the affine one, and deleteTeardownTargets deletes all
// of them. A record naming another live session's durable — malformed, tampered, or
// written by a future broker — would therefore have that consumer deleted, and on
// the interest-retained streams its undelivered messages go with it. Validating only
// the affine pair closed one field of four.
//
// Every name here is bound to its session: a plain session durable is
// "<idHash>_<nuid>", the PUBREL durable is fully derived from (idHash, generation),
// and the affine one likewise. The shared group durables are NOT checked here and do
// not need to be — they are derived from the record's own SharedSubs keys rather
// than copied, and releaseSharedTeardownDurables already refuses to delete one another
// session still holds.
func mqttValidateTeardownTargets(ps *mqttPersistedSession, idHash string) error {
	if verr := mqttValidateSessionRecord(ps, idHash); verr != nil {
		return verr
	}
	durPrefix := idHash + "_"
	for sid, cc := range ps.Cons {
		if cc == nil {
			continue
		}
		if !strings.HasPrefix(cc.Durable, durPrefix) {
			return fmt.Errorf("record names consumer %q for subscription %q, which is not this session's", cc.Durable, sid)
		}
	}
	for sid, pd := range ps.Drains {
		if pd == nil || pd.Cons == nil {
			continue
		}
		if !strings.HasPrefix(pd.Cons.Durable, durPrefix) {
			return fmt.Errorf("record names drain consumer %q for subscription %q, which is not this session's", pd.Cons.Durable, sid)
		}
	}
	if ps.PubRel != nil {
		if _, _, want := mqttPubRelNames(idHash, ps.Gen); ps.PubRel.Durable != want {
			return fmt.Errorf("record names PUBREL consumer %q (want %q)", ps.PubRel.Durable, want)
		}
	}
	return nil
}

// mqttSessIDHashFromSubject extracts the client-ID hash token from a session
// subject, requiring the exact shape this account writes.
//
// Derived from the SUBJECT rather than from record content, which is not a trusted
// input: a record can be written by a future broker or hand-edited, and a hash taken
// from its body would let one session's record claim another's identity.
func mqttSessIDHashFromSubject(subject, domainTk string) (string, error) {
	prefix := mqttSessStreamSubjectPrefix + domainTk
	if !strings.HasPrefix(subject, prefix) {
		return _EMPTY_, fmt.Errorf("subject does not belong to this account/domain")
	}
	hash := subject[len(prefix):]
	if !mqttValidIDHashToken(hash) {
		return _EMPTY_, fmt.Errorf("subject carries a malformed client-ID hash token %q", hash)
	}
	return hash, nil
}

// mqttValidIDHashToken reports whether s is a well-formed client-ID hash: exactly
// sysHashLen characters from getHashSize's base-62 alphabet. Same reasoning as
// mqttValidSessGen — a token of the right length from a foreign alphabet is not one
// of ours, and a name built from it would address something we do not own.
func mqttValidIDHashToken(s string) bool {
	if len(s) != sysHashLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Inbound QoS 2: subject parsing
// ---------------------------------------------------------------------------

// mqttQoS2InName is a parsed $MQTT_qos2in subject. Both record kinds share one
// identity — the (idHash, generation, packet identifier) triple — so a claim always
// knows its hold and vice versa, and neither has to be reconstructed by string
// surgery at a call site.
type mqttQoS2InName struct {
	isClaim bool
	idHash  string
	gen     string
	hold    string
	claim   string
}

// mqttQoS2InCandidate is one hold identity the walk discovered, with the SEQUENCES of
// whichever of its two records the walk happened to see.
//
// Sequences, not times: they are the quarantine fingerprint. Recording them here lets
// a candidate that cannot act yet be skipped without paying a direct lookup — and in
// particular without dragging a live PUBLISH payload across the JS API to learn it.
// Everything that authorizes a mutation is re-read directly.
type mqttQoS2InCandidate struct {
	name      *mqttQoS2InName
	holdSeen  bool
	holdSeq   uint64
	claimSeen bool
	claimSeq  uint64
}

// notYetQuarantined records this pass's observation of whichever records the walk
// saw, and reports whether one of them already rules every disposition out.
//
// An unsatisfied CLAIM rules out all of them: every branch that removes a claim
// needs it settled, every branch that destroys a claimed hold needs it settled, and
// the unclaimed-hold branch cannot apply while a claim is sitting there. An
// unsatisfied HOLD rules out every branch that would destroy it, and no branch
// removes a claim while its hold survives.
//
// Calling this IS the observation, so it must happen on every pass for the wait to
// accumulate. It runs before the direct lookups purely so a candidate that cannot
// act yet does not drag its PUBLISH payload across the JS API to learn that.
func (c *mqttQoS2InCandidate) notYetQuarantined(q *mqttSweepQuarantine, holdGrace, claimGrace time.Duration) bool {
	// Deliberately not short-circuited: both records this pass saw must be observed,
	// or the second one's wait would only start on the pass after the first one's
	// finished — which serializes the pair's quarantine for no benefit.
	blocked := c.claimSeen &&
		!q.observedOrphanFor(mqttQuarantineClaimKey(c.name.claim), mqttQuarantineSeqFP(c.claimSeq), claimGrace)
	if c.holdSeen && !q.observedOrphanFor(mqttQuarantineHoldKey(c.name.hold), mqttQuarantineSeqFP(c.holdSeq), holdGrace) {
		blocked = true
	}
	return blocked
}

// mqttParseQoS2InSubject parses a subject on $MQTT_qos2in strictly, in the exact
// shapes this broker writes and no others.
//
// Strictness is not tidiness. A subject that cannot be attributed to a specific
// (idHash, generation) is retained rather than swept, so a lenient parse that
// guessed would be guessing about which session owns a payload. The packet
// identifier is required to be CANONICAL decimal (no leading zeros, 1..65535)
// because mqttQoS2HoldSubject formats it with strconv.FormatUint: "01" is a subject
// this broker can never have written, so it is foreign, not ours-with-odd-spelling.
func mqttParseQoS2InSubject(subject string) (*mqttQoS2InName, error) {
	if !strings.HasPrefix(subject, mqttQoS2IncomingMsgsStreamSubjectPrefix) {
		return nil, fmt.Errorf("subject is not on the inbound QoS 2 prefix")
	}
	rest := subject[len(mqttQoS2IncomingMsgsStreamSubjectPrefix):]
	tokens := strings.Split(rest, ".")
	n := &mqttQoS2InName{}
	switch {
	case len(tokens) == 3:
	case len(tokens) == 4 && tokens[3] == mqttQoS2ClaimSubjectToken:
		n.isClaim = true
	default:
		return nil, fmt.Errorf("subject has %d tokens past the prefix (want 3, or 4 ending in %q)",
			len(tokens), mqttQoS2ClaimSubjectToken)
	}
	if !mqttValidIDHashToken(tokens[0]) {
		return nil, fmt.Errorf("malformed client-ID hash token %q", tokens[0])
	}
	if !mqttValidSessGen(tokens[1]) {
		return nil, fmt.Errorf("malformed incarnation generation token %q", tokens[1])
	}
	pi, err := strconv.ParseUint(tokens[2], 10, 16)
	if err != nil || pi == 0 || strconv.FormatUint(pi, 10) != tokens[2] {
		return nil, fmt.Errorf("malformed packet identifier token %q", tokens[2])
	}
	n.idHash, n.gen = tokens[0], tokens[1]
	n.hold = mqttQoS2HoldSubject(n.idHash, n.gen, uint16(pi))
	n.claim = mqttQoS2ClaimSubjectFor(n.hold)
	return n, nil
}

// ---------------------------------------------------------------------------
// Inbound QoS 2: bounded candidate discovery
// ---------------------------------------------------------------------------

// collectInboundQoS2Candidates walks $MQTT.qos2.in.> once and returns the distinct
// HOLD identities it saw, hold records and release claims alike.
//
// One walk rather than two filters: the two record kinds are told apart by
// mqttQoS2IsClaimSubject, which is the idiom arbitrateQoS2ReleaseClaims already
// uses, and a single pass observes both in one stream order instead of pairing
// across two independently-timed scans.
//
// The walk is DISCOVERY ONLY. It is bounded, so it is expected to be partial, and a
// partial batch can never prove a counterpart absent — every disposition below
// re-reads both records directly. STREAM.INFO's subject-detail map would avoid the
// payload transfer but cannot be used: jsa.streamSubjects is unpaged and truncates
// past JSMaxSubjectDetails, and a truncated map read as complete is exactly the
// partial-view-as-absence mistake.
func (as *mqttAccountSessionManager) collectInboundQoS2Candidates(s *Server) ([]*mqttQoS2InCandidate, error) {
	si, err := as.jsa.lookupStream(mqttQoS2IncomingMsgsStreamName)
	if err != nil {
		if isErrorOtherThan(err, JSStreamNotFoundErr) {
			return nil, err
		}
		return nil, nil
	}
	if si == nil || si.State.Msgs == 0 || si.State.LastSeq < si.State.FirstSeq {
		return nil, nil
	}
	start := as.qos2SweepStart(si.State.FirstSeq, si.State.LastSeq)
	recordLimit, byteLimit := as.qos2SweepRecordLimit(), as.qos2SweepByteLimit()

	filter := mqttQoS2IncomingMsgsStreamSubjectPrefix + ">"
	seen := make(map[string]*mqttQoS2InCandidate)
	out := make([]*mqttQoS2InCandidate, 0, 16)
	records, bytes := 0, int64(0)
	last := start

	// [start, LastSeq] then [FirstSeq, start): one wrap, stopping before the start
	// point so a full pass covers the stream exactly once.
	spans := [][2]uint64{{start, si.State.LastSeq}}
	if start > si.State.FirstSeq {
		spans = append(spans, [2]uint64{si.State.FirstSeq, start - 1})
	}
	exhausted := false
	for _, span := range spans {
		if exhausted {
			break
		}
		for seq := span[0]; seq <= span[1]; {
			smsg, err := as.jsa.loadNextMsgFromSeq(mqttQoS2IncomingMsgsStreamName, filter, seq)
			if err != nil {
				if isErrorOtherThan(err, JSNoMessageFoundErr) {
					return nil, err
				}
				break // no more records in this span
			}
			if smsg.Sequence > span[1] {
				break
			}
			seq = smsg.Sequence + 1
			last = smsg.Sequence

			records++
			bytes += int64(len(smsg.Subject) + len(smsg.Header) + len(smsg.Data))

			if name, perr := mqttParseQoS2InSubject(smsg.Subject); perr != nil {
				// Retained, never swept and never widened into a wildcard: an
				// unattributable subject has no provable owner.
				s.Warnf("MQTT orphaned QoS 2 sweep: retaining an unattributable record on %q at sequence %d: %v",
					smsg.Subject, smsg.Sequence, perr)
			} else {
				cand := seen[name.hold]
				if cand == nil {
					cand = &mqttQoS2InCandidate{name: name}
					seen[name.hold] = cand
					out = append(out, cand)
				}
				if name.isClaim {
					cand.claimSeen, cand.claimSeq = true, smsg.Sequence
				} else {
					cand.holdSeen, cand.holdSeq = true, smsg.Sequence
				}
			}

			if records >= recordLimit || bytes >= byteLimit {
				exhausted = true
				break
			}
		}
	}

	// Resume where this pass stopped. Losing this cursor costs coverage latency and
	// nothing else, which is why it is plain scheduling state.
	next := last + 1
	if next > si.State.LastSeq {
		next = si.State.FirstSeq
	}
	as.mu.Lock()
	as.qos2SweepCursor = next
	as.mu.Unlock()

	s.Debugf("MQTT orphaned QoS 2 sweep: inbound scan visited %d record(s), %d byte(s), %d candidate hold subject(s)",
		records, bytes, len(out))
	return out, nil
}

// qos2SweepStart returns this pass's starting sequence, seeding the cursor on first
// use and re-seeding it only when the stream can no longer contain it.
//
// The seed is pseudo-random so a broker that restarts more often than it completes a
// full walk does not rescan the same prefix forever; from there the cursor advances
// SEQUENTIALLY, so coverage inside one process is linear rather than a coupon
// collector's. Retention advancing FirstSeq normalizes the cursor forward rather
// than reseeding — that is ordinary progress, not an unusable cursor.
func (as *mqttAccountSessionManager) qos2SweepStart(firstSeq, lastSeq uint64) uint64 {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.qos2SweepSeeded && as.qos2SweepCursor >= firstSeq && as.qos2SweepCursor <= lastSeq {
		return as.qos2SweepCursor
	}
	if as.qos2SweepSeeded && as.qos2SweepCursor < firstSeq {
		// Retention moved past us. Keep walking forward from the new floor.
		as.qos2SweepCursor = firstSeq
		return firstSeq
	}
	span := lastSeq - firstSeq + 1
	var start uint64
	if seed := as.testQoS2SweepSeed; seed != nil {
		start = seed(firstSeq, lastSeq)
		if start < firstSeq || start > lastSeq {
			start = firstSeq
		}
	} else {
		start = firstSeq + uint64(rand.Int63n(int64(min(span, 1<<62))))
	}
	as.qos2SweepCursor, as.qos2SweepSeeded = start, true
	return start
}

// ---------------------------------------------------------------------------
// Inbound QoS 2: reconciliation
// ---------------------------------------------------------------------------

// reconcileInboundQoS2 resolves each candidate hold identity whose incarnation is
// authoritatively absent.
//
// The candidate list is discovery; every decision below is made from a DIRECT
// lookup of both the hold subject and its sibling claim, because the bounded walk
// may have seen one without the other. Live generations are filtered out first, so
// an account whose holds mostly belong to live sessions costs no extra lookups at
// all.
func (as *mqttAccountSessionManager) reconcileInboundQoS2(s *Server, candidates []*mqttQoS2InCandidate, refs *mqttSessRefs) {
	holdGrace, claimGrace := as.orphanGrace(), as.qos2ReleaseGrace()
	q := as.quarantine()
	var purged, claimed, reaped, deferred int
	for _, cand := range candidates {
		n := cand.name
		if refs.hasInbound(n.idHash, n.gen) {
			// A live incarnation owns this subject. Drop any wait accumulated while it
			// looked absent: a session that came back must not inherit it.
			q.forget(mqttQuarantineHoldKey(n.hold))
			q.forget(mqttQuarantineClaimKey(n.claim))
			continue
		}
		if cand.notYetQuarantined(q, holdGrace, claimGrace) {
			// Provably ineligible from what the walk already fetched: do not pay a
			// lookup — and in particular do not pull a live payload — to re-learn it.
			continue
		}
		switch as.reconcileOneInboundQoS2(s, n, q, holdGrace, claimGrace) {
		case mqttQoS2SweepPurged:
			purged++
		case mqttQoS2SweepClaimed:
			claimed++
		case mqttQoS2SweepReaped:
			reaped++
		case mqttQoS2SweepDeferred:
			deferred++
		}
	}
	if purged+claimed+reaped+deferred > 0 {
		s.Debugf("MQTT orphaned QoS 2 sweep: inbound reconciliation fenced %d, purged %d, reaped %d claim(s), deferred %d",
			claimed, purged, reaped, deferred)
	}
}

type mqttQoS2SweepOutcome int

const (
	mqttQoS2SweepNothing mqttQoS2SweepOutcome = iota
	mqttQoS2SweepClaimed                      // a discard marker now fences the hold
	mqttQoS2SweepPurged                       // the hold record is gone
	mqttQoS2SweepReaped                       // the claim record is gone
	mqttQoS2SweepDeferred
)

// reconcileOneInboundQoS2 applies the disposition table to one hold identity.
//
// The orderings that matter, in the order they matter:
//
//  1. A hold is never destroyed without a claim standing on its sibling subject.
//     For an unclaimed hold that means creating the `discarded` marker first
//     (discardQoS2Hold does exactly this, and it is the same arbitration a session
//     teardown runs). For an already-claimed one the existing claim IS the fence.
//  2. A claim is never deleted while its hold survives. Deleting it first would let
//     a paused releaser win a fresh claim and then read its hold as still Held.
//  3. Every delete targets the EXACT sequence just observed, so a delayed pass
//     cannot remove a later exchange's record on a reused packet identifier.
func (as *mqttAccountSessionManager) reconcileOneInboundQoS2(s *Server, n *mqttQoS2InName,
	q *mqttSweepQuarantine, holdGrace, claimGrace time.Duration) mqttQoS2SweepOutcome {
	rec, holdSeq, herr := as.loadQoS2Record(n.hold)
	if herr != nil {
		return mqttQoS2SweepDeferred // indeterminate: never "absent"
	}
	claim, claimMsg, cerr := as.loadQoS2RelClaim(n.claim)
	if cerr != nil {
		return mqttQoS2SweepDeferred
	}

	switch {
	case rec == nil && claimMsg == nil:
		// Nothing left on either subject. Release the slots so a large backlog's
		// resident cohort clears and later candidates can be admitted.
		q.forget(mqttQuarantineHoldKey(n.hold))
		q.forget(mqttQuarantineClaimKey(n.claim))
		return mqttQoS2SweepNothing

	case claimMsg == nil:
		// A hold with no claim. Arbitrate for it exactly as a discard does: the marker
		// is created BEFORE the purge, which is what makes this a competitor rather
		// than a destroyer, and it is left behind for a later, age-qualified pass.
		if !q.observedOrphanFor(mqttQuarantineHoldKey(n.hold), mqttQuarantineSeqFP(holdSeq), holdGrace) {
			return mqttQoS2SweepNothing
		}
		// An expired deadline: a claim that races in as `releasing` is left alone for
		// the next tick rather than blocking the sweep goroutine on a poll loop.
		if err := as.discardQoS2Hold(s, n.hold, time.Now()); err != nil {
			s.Warnf("MQTT orphaned QoS 2 sweep: unable to fence the orphaned hold on %q: %v", n.hold, err)
			return mqttQoS2SweepDeferred
		}
		return mqttQoS2SweepClaimed

	default:
		// SOME record occupies the claim subject — `discarded`, `releasing`, or one
		// that cannot be read at all. All three retire the same way, and the reason
		// they can share a path is that all three are equally effective FENCES: the
		// claim write is a create-only CAS, which fails while any record is there,
		// readable or not. So the fence stays in place while the payload is destroyed,
		// and is removed only once the hold is provably gone.
		//
		// An unreadable record must NOT be deleted first and replaced with a fresh
		// marker. That empties the subject while the hold still exists, and a releaser
		// that preloaded the payload can win a claim in the gap, re-read its hold as
		// still Held, and fan out a message from a session that no longer exists —
		// exactly what invariant 10 exists to prevent. It is retired on the
		// RELEASE-claim grace, not the shorter construction grace a discard would
		// apply to it.
		//
		// For `releasing` this does not pretend a paused process can never resume. It
		// preserves the ordering that already governs one: a releaser which has not yet
		// passed its authoritative re-read delivers nothing, and one already beyond it
		// is inside the documented post-claim residual.
		if !q.observedOrphanFor(mqttQuarantineClaimKey(n.claim), mqttQuarantineSeqFP(claimMsg.Sequence), claimGrace) {
			return mqttQoS2SweepNothing
		}
		if claim == nil {
			s.Warnf("MQTT orphaned QoS 2 sweep: retiring an aged unusable QoS 2 release claim (%s)",
				mqttQoS2CorruptDetail(n.claim, claimMsg, "malformed claim record, or no usable claim identity"))
		}
		if rec != nil {
			if !q.observedOrphanFor(mqttQuarantineHoldKey(n.hold), mqttQuarantineSeqFP(holdSeq), holdGrace) {
				return mqttQoS2SweepNothing
			}
			if err := as.purgeQoS2Hold(n.hold); err != nil {
				s.Warnf("MQTT orphaned QoS 2 sweep: unable to purge the orphaned hold on %q: %v", n.hold, err)
				return mqttQoS2SweepDeferred
			}
			// purgeQoS2Hold is already a confirmed purge; re-reading is defensive depth,
			// because this is the step that licenses removing the fence.
			if after, _, aerr := as.loadQoS2Record(n.hold); aerr != nil || after != nil {
				return mqttQoS2SweepDeferred
			}
		}
		// Only now, and only if nothing replaced the claim we reasoned about.
		if !as.qos2ClaimUnchanged(n.claim, claim, claimMsg.Sequence) {
			return mqttQoS2SweepDeferred
		}
		if st, rerr := as.releaseQoS2Record(claimMsg.Sequence); st != mqttQoS2Released {
			s.Warnf("MQTT orphaned QoS 2 sweep: unable to retire the %s release claim on %q: %v",
				mqttQoS2RelClaimLabel(claim), n.claim, rerr)
			return mqttQoS2SweepDeferred
		}
		s.Debugf("MQTT orphaned QoS 2 sweep: retired the %s release claim on %q",
			mqttQoS2RelClaimLabel(claim), n.claim)
		q.forget(mqttQuarantineHoldKey(n.hold))
		q.forget(mqttQuarantineClaimKey(n.claim))
		return mqttQoS2SweepReaped
	}
}

// ---------------------------------------------------------------------------
// Outbound in-flight registers
// ---------------------------------------------------------------------------

// mqttInflightCandidate is one in-flight register the walk discovered, with the
// SEQUENCE of the record it saw — the quarantine fingerprint, for the same reason the
// inbound candidates carry theirs.
type mqttInflightCandidate struct {
	name *mqttInflightName
	seq  uint64
}

// collectInflightCandidates walks $MQTT.inflight.> once and returns every register it
// could attribute to an (idHash, generation) pair.
//
// Bounded by the same per-tick record budget as the inbound walk, and with the same
// wrapping cursor, for the same reason: an account with a large leaked history must
// not move it all in one pass. There is no byte budget because a register is a small
// fixed-shape record — unlike a hold, which carries a PUBLISH payload.
func (as *mqttAccountSessionManager) collectInflightCandidates(s *Server) ([]*mqttInflightCandidate, error) {
	si, err := as.jsa.lookupStream(mqttInflightStreamName)
	if err != nil {
		if isErrorOtherThan(err, JSStreamNotFoundErr) {
			return nil, err
		}
		return nil, nil
	}
	if si == nil || si.State.Msgs == 0 || si.State.LastSeq < si.State.FirstSeq {
		return nil, nil
	}
	start := as.inflightSweepStart(si.State.FirstSeq, si.State.LastSeq)
	recordLimit := as.qos2SweepRecordLimit()

	filter := mqttInflightStreamSubjectPrefix + ">"
	out := make([]*mqttInflightCandidate, 0, 16)
	records := 0
	last := start

	// [start, LastSeq] then [FirstSeq, start): one wrap, so a full pass covers the
	// stream exactly once.
	spans := [][2]uint64{{start, si.State.LastSeq}}
	if start > si.State.FirstSeq {
		spans = append(spans, [2]uint64{si.State.FirstSeq, start - 1})
	}
	exhausted := false
	for _, span := range spans {
		if exhausted {
			break
		}
		for seq := span[0]; seq <= span[1]; {
			smsg, err := as.jsa.loadNextMsgFromSeq(mqttInflightStreamName, filter, seq)
			if err != nil {
				if isErrorOtherThan(err, JSNoMessageFoundErr) {
					return nil, err
				}
				break
			}
			if smsg.Sequence > span[1] {
				break
			}
			seq = smsg.Sequence + 1
			last = smsg.Sequence
			records++

			if name, perr := mqttParseInflightSubject(smsg.Subject); perr != nil {
				// Retained, never swept: an unattributable subject has no provable owner,
				// so nothing here may decide it is an orphan.
				s.Warnf("MQTT orphaned QoS 2 sweep: retaining an unattributable in-flight register on %q at sequence %d: %v",
					smsg.Subject, smsg.Sequence, perr)
			} else {
				out = append(out, &mqttInflightCandidate{name: name, seq: smsg.Sequence})
			}

			if records >= recordLimit {
				exhausted = true
				break
			}
		}
	}

	next := last + 1
	if next > si.State.LastSeq {
		next = si.State.FirstSeq
	}
	as.mu.Lock()
	as.inflightSweepCursor = next
	as.mu.Unlock()

	s.Debugf("MQTT orphaned QoS 2 sweep: in-flight register scan visited %d record(s), %d candidate(s)",
		records, len(out))
	return out, nil
}

// inflightSweepStart returns this pass's starting sequence in $MQTT_inflight, seeded
// and advanced exactly as the inbound cursor is (qos2SweepStart), and scheduling
// state only: every candidate is authoritatively revalidated before deletion.
func (as *mqttAccountSessionManager) inflightSweepStart(firstSeq, lastSeq uint64) uint64 {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.inflightSweepSeeded && as.inflightSweepCursor >= firstSeq && as.inflightSweepCursor <= lastSeq {
		return as.inflightSweepCursor
	}
	if as.inflightSweepSeeded && as.inflightSweepCursor < firstSeq {
		as.inflightSweepCursor = firstSeq
		return firstSeq
	}
	span := lastSeq - firstSeq + 1
	var start uint64
	if seed := as.testQoS2SweepSeed; seed != nil {
		start = seed(firstSeq, lastSeq)
		if start < firstSeq || start > lastSeq {
			start = firstSeq
		}
	} else {
		start = firstSeq + uint64(rand.Int63n(int64(min(span, 1<<62))))
	}
	as.inflightSweepCursor, as.inflightSweepSeeded = start, true
	return start
}

// reconcileOrphanInflight deletes in-flight registers whose owning session
// incarnation is authoritatively absent.
//
// This is the simplest class in the sweep, and deliberately so. A register has no
// second party — nothing claims it, waits on it, or reads it as proof of anything —
// so there is no ordering to preserve and no fence to keep in place: once the
// incarnation that owned it provably does not exist, the record is inert bookkeeping
// about an identifier space nobody will ever consult again. What it shares with every
// other class is the part that matters: absence is proven by the complete session
// walk, never inferred, and eligibility is a STABLE OBSERVATION on this process's
// monotonic clock rather than any stored timestamp.
//
// The construction grace applies (not the longer release-claim one): a register is
// written moments before the delivery it names, so the window it has to survive is the
// ordinary create-before-save one.
func (as *mqttAccountSessionManager) reconcileOrphanInflight(s *Server, candidates []*mqttInflightCandidate, refs *mqttSessRefs, budget *int) {
	grace, q := as.orphanGrace(), as.quarantine()
	var deleted int
	for _, cand := range candidates {
		n := cand.name
		if refs.hasInbound(n.idHash, n.gen) {
			// A live incarnation owns this identifier. Drop any wait accumulated while
			// it looked absent: a session that came back must not inherit it.
			q.forget(mqttQuarantineInflightKey(n.subject))
			continue
		}
		if !q.observedOrphanFor(mqttQuarantineInflightKey(n.subject), mqttQuarantineSeqFP(cand.seq), grace) {
			continue
		}
		if *budget <= 0 {
			return
		}
		// Revalidated at the moment of the mutation: the snapshot is older than the
		// record, and a register rewritten since (a later exchange on the same
		// identifier, of a generation that has since come back) must not be deleted on
		// the strength of a stale read. The exact sequence is what makes that safe (R4).
		cur, smsg, lerr := as.loadInflightRecord(n.subject)
		if lerr != nil || smsg == nil || smsg.Sequence != cand.seq {
			continue
		}
		_ = cur // readability is irrelevant here: an unreadable orphan is still an orphan
		*budget--
		as.deleteInflightRegister(cand.seq)
		q.forget(mqttQuarantineInflightKey(n.subject))
		deleted++
	}
	if deleted > 0 {
		s.Debugf("MQTT orphaned QoS 2 sweep: deleted %d orphaned in-flight register(s)", deleted)
	}
}

// mqttQoS2RelClaimLabel names a claim for a log line, including the one that could
// not be decoded — a nil claim is a state this path handles, not an impossibility.
func mqttQoS2RelClaimLabel(c *mqttQoS2RelClaim) string {
	if c == nil {
		return "unreadable"
	}
	return c.State
}

// qos2ClaimUnchanged re-reads a claim and reports whether it is still the exact
// record the caller decided about: same sequence, same readability, same state and
// same generation.
//
// The caller's view is always older than the record. A replacement claim on a
// reused packet identifier, or a CAS that landed in between, must defer the pair
// rather than have its record deleted on the strength of a stale read.
func (as *mqttAccountSessionManager) qos2ClaimUnchanged(claimSubject string, want *mqttQoS2RelClaim, wantSeq uint64) bool {
	cur, smsg, err := as.loadQoS2RelClaim(claimSubject)
	if err != nil || smsg == nil || smsg.Sequence != wantSeq {
		return false
	}
	if want == nil || cur == nil {
		return want == nil && cur == nil
	}
	return cur.State == want.State && cur.Gen == want.Gen
}

// ---------------------------------------------------------------------------
// Affine assignments
// ---------------------------------------------------------------------------

// mqttQ2AffInvEntry is one assignment claim as the inventory saw it.
type mqttQ2AffInvEntry struct {
	subject string
	claim   *mqttQ2AffClaim
	seq     uint64
	stored  time.Time
}

// validateOrphanQ2AffClaim is the SWEEP-ONLY validator: every identity a claim
// carries must be derivable from the two things that key it, so the sweep can trust
// a claim's copy subject and associate it with a consumer.
//
// Deliberately not part of mqttQ2AffClaimIdentityOK. That predicate gates a session
// resume and clear(), where an abort has account-wide consequences; these checks
// are derivations from naming helpers, so a future change to mqttQ2AffineNames or
// mqttQ2AffCopySubject would retroactively invalidate every stored claim. Inside the
// sweep that costs one deferred pass and protects every affine consumer from
// deletion; inside clear() it would be an account-wide teardown outage. Any change
// to those naming helpers must be coordinated with this validator.
func validateOrphanQ2AffClaim(subject string, c *mqttQ2AffClaim) error {
	if want := mqttQ2AffClaimSubject(c.GroupDurable, c.SourceSeq); subject != want {
		return fmt.Errorf("claim on %q does not sit on its derived subject %q", subject, want)
	}
	if want := mqttQ2AffClaimHash(c.GroupDurable, c.SourceSeq); c.ClaimHash != want {
		return fmt.Errorf("claim on %q carries claim hash %q (want %q)", subject, c.ClaimHash, want)
	}
	if want := mqttQ2AffCopySubject(c.Winner, c.Gen, c.ClaimHash); c.CopySubject != want {
		return fmt.Errorf("claim on %q names copy subject %q (want %q)", subject, c.CopySubject, want)
	}
	if _, _, want := mqttQ2AffineNames(c.Winner, c.Gen); c.AffDurable != want {
		return fmt.Errorf("claim on %q names affine durable %q (want %q)", subject, c.AffDurable, want)
	}
	switch c.Phase {
	case mqttQ2AffClaimed, mqttQ2AffMaterialized:
	default:
		return fmt.Errorf("claim on %q carries an unknown phase %q", subject, c.Phase)
	}
	if c.Lost && c.AffineDone {
		// Mutually exclusive by construction (mergeProven). A record asserting both
		// says the message was delivered AND never delivered.
		return fmt.Errorf("claim on %q asserts both AffineDone and Lost", subject)
	}
	if !mqttValidIDHashToken(c.Winner) {
		return fmt.Errorf("claim on %q carries a malformed winner hash %q", subject, c.Winner)
	}
	return nil
}

// collectQ2AffInventory returns EVERY assignment claim in the account, or an error.
// Never partial and never capped: a consumer deletion is authorized by the absence
// of an unreconciled claim, and absence taken from a capped view is not absence
// (invariant 13).
func (as *mqttAccountSessionManager) collectQ2AffInventory() ([]*mqttQ2AffInvEntry, error) {
	var out []*mqttQ2AffInvEntry
	err := as.scanQ2AffClaimRecords(func(claim *mqttQ2AffClaim, smsg *StoredMsg) error {
		if verr := validateOrphanQ2AffClaim(smsg.Subject, claim); verr != nil {
			return verr
		}
		out = append(out, &mqttQ2AffInvEntry{
			subject: smsg.Subject, claim: claim, seq: smsg.Sequence, stored: smsg.Time,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// reconcileOrphanQ2AffClaims marks the assignments of absent winners discarded, and
// lets the existing two-sided predicate decide whether they may then be deleted.
//
// A discarded assignment is NEVER released back to its shared group: MQTT 5.0
// [4.8.2] permits the delivery to die with the session that owned it and forbids
// handing a started delivery to a second member. So the claim is retained whenever
// the source may still be re-offered, and a later contender honors it, proves the
// group ack, and reaps it then.
func (as *mqttAccountSessionManager) reconcileOrphanQ2AffClaims(s *Server, inv []*mqttQ2AffInvEntry,
	refs *mqttSessRefs, budget *int) {

	grace, q := as.orphanGrace(), as.quarantine()
	var discarded, lost, retained, deferred int
	for _, e := range inv {
		if refs.hasAffine(e.claim.Winner, e.claim.Gen) {
			q.forget(mqttQuarantineAssignKey(e.subject))
			continue
		}
		// Terminal and retained claims are resolved BEFORE any quarantine observation,
		// and release whatever slot they hold. Neither needs a wait — a wait exists to
		// keep a mid-construction resource from being acted on, and these are already
		// durably terminal — while both are exactly the state that accumulates. `Lost`
		// is permanent by design; a `Discarded` claim is retained until its source is
		// proven closed. Letting them occupy quarantine slots would let a single
		// account fill the shared table and permanently refuse every new hold, claim,
		// consumer and teardown key: the sweep would switch itself off for good.
		if e.claim.Lost {
			// The only durable evidence that a QoS 2 message the broker accepted was
			// never delivered. Surface it; never reap it (reapable() refuses it
			// unconditionally, so there is nothing to attempt either).
			q.forget(mqttQuarantineAssignKey(e.subject))
			lost++
			s.Errorf("MQTT orphaned QoS 2 sweep: shared QoS 2 assignment %q is recorded LOST for session %q and is retained for inspection",
				e.subject, e.claim.Winner)
			continue
		}
		if e.claim.Discarded {
			// Already terminal on the discard axis: no transition left to write, only
			// the reap to retry until the source side is proven closed. reapQ2AffClaim
			// re-reads authoritatively and applies the two-sided predicate itself, which
			// is why it needs no quarantine gate — contenders call it with no grace at
			// all. Charging the mutation budget here would be a no-op write's worth of
			// starvation: an inventory headed by retained-by-design claims would consume
			// the whole budget on the same prefix every pass.
			q.forget(mqttQuarantineAssignKey(e.subject))
			as.reapQ2AffClaim(s, e.subject)
			retained++
			continue
		}
		if !q.observedOrphanFor(mqttQuarantineAssignKey(e.subject), mqttQuarantineSeqFP(e.seq), grace) {
			continue
		}
		if *budget <= 0 {
			deferred++
			continue
		}
		switch as.discardAbsentWinnerClaim(s, e) {
		case mqttQoS2SweepClaimed:
			// Charged only where a transition was actually written.
			*budget--
			discarded++
		case mqttQoS2SweepDeferred:
			deferred++
		}
	}
	if discarded+lost+retained+deferred > 0 {
		s.Debugf("MQTT orphaned QoS 2 sweep: assignment reconciliation discarded %d, retried %d already-discarded, retained %d lost, deferred %d",
			discarded, retained, lost, deferred)
	}
}

// discardAbsentWinnerClaim proves the winner absent and commits Discarded.
//
// "Absent" is a high bar and the verification order matters. Any backend error
// defers. A record whose affine generation MATCHES this claim defers, whether or not
// the rest of it validates: a matching generation means either a live winner or a
// pair we cannot interpret, and neither authorizes discarding its assignment. Only
// an authoritatively absent record, or a valid record for a DIFFERENT generation,
// proves this winner is gone.
func (as *mqttAccountSessionManager) discardAbsentWinnerClaim(s *Server, e *mqttQ2AffInvEntry) mqttQoS2SweepOutcome {
	ps, _, err := as.loadFenceSession(e.claim.Winner)
	if err != nil {
		return mqttQoS2SweepDeferred
	}
	if ps != nil {
		if ps.Q2AffGen == e.claim.Gen {
			return mqttQoS2SweepDeferred // live, or indeterminate: either way, not ours to discard
		}
		// A record written since the reference snapshot has not been through that
		// walk's validation, and this re-read is what authorizes the discard. Only a
		// fully interpretable record for a DIFFERENT generation proves this winner's
		// incarnation is over; anything else is indeterminate.
		if verr := mqttValidateSessionRecord(ps, e.claim.Winner); verr != nil {
			return mqttQoS2SweepDeferred
		}
	}

	// Discarded ONLY. Not AffineDone, which would assert a delivery that never
	// happened, and not SourceAcked, which would let a source redelivery elect a
	// fresh winner if the group ack never actually committed.
	updated, _, aerr := as.advanceQ2AffClaim(e.subject, e.claim.Winner, e.claim.Gen,
		func(c *mqttQ2AffClaim) { c.Discarded = true })
	if aerr != nil || updated == nil {
		return mqttQoS2SweepDeferred
	}
	// May prove SourceAcked from an absent source or group consumer, and completes
	// nothing on the affine side. Retains the claim whenever the source may still be
	// re-offered.
	as.reapQ2AffClaim(s, e.subject)
	as.forgetIfQ2AffClaimGone(e.subject)
	return mqttQoS2SweepClaimed
}

// ---------------------------------------------------------------------------
// Affine consumers, last
// ---------------------------------------------------------------------------

// deleteOrphanQ2AffConsumers removes $MQTT_Q2AFF_* consumers on $MQTT_out that no
// session record references — but only once every assignment they own is durably
// terminal.
//
// The ordering is the central acceptance condition of this whole file. $MQTT_out is
// interest-retained, so deleting the consumer drops the copies it still holds.
// Doing that while a claim still described a live delivery would convert a session
// discard MQTT 5.0 [4.8.2] permits into a recorded Lost message — laundering a
// permitted death into a compliance failure, and in the other direction erasing the
// distinction the write order of AffineDone exists to preserve.
func (as *mqttAccountSessionManager) deleteOrphanQ2AffConsumers(s *Server, inv []*mqttQ2AffInvEntry,
	consumers []*ConsumerInfo, refs *mqttSessRefs, budget *int) {

	type affCand struct {
		durable string
		idHash  string
		gen     string
		created time.Time // the exact consumer instance quarantine observed
	}
	grace, q := as.orphanGrace(), as.quarantine()
	var candidates []affCand
	for _, ci := range consumers {
		if ci == nil || ci.Config == nil {
			continue
		}
		if !strings.HasPrefix(ci.Name, mqttQ2AffineConsumerDurablePrefix) {
			continue // not ours to judge (and deliberately NOT $MQTT_PUBREL_*)
		}
		idHash, gen, perr := mqttParseQ2AffDurable(ci.Name)
		if perr != nil {
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining affine consumer %q: %v", ci.Name, perr)
			continue
		}
		if verr := mqttValidateQ2AffineNames(ci.Config, idHash, gen); verr != nil {
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining affine consumer %q: %v", ci.Name, verr)
			continue
		}
		if refs.hasAffine(idHash, gen) {
			q.forget(mqttQuarantineConsumerKey(ci.Name))
			continue
		}
		if _, ok := refs.affDurables[ci.Name]; ok {
			q.forget(mqttQuarantineConsumerKey(ci.Name))
			continue
		}
		// Created is a FINGERPRINT here, never an age: a recreated consumer of the same
		// name carries a different one and restarts the wait.
		fp := strconv.FormatInt(ci.Created.UnixNano(), 10)
		if !q.observedOrphanFor(mqttQuarantineConsumerKey(ci.Name), fp, grace) {
			continue
		}
		candidates = append(candidates, affCand{durable: ci.Name, idHash: idHash, gen: gen, created: ci.Created})
	}

	for _, cand := range candidates {
		// (1) Every assignment this generation owns, from the COMPLETE inventory.
		owned := make([]*mqttQ2AffInvEntry, 0, 4)
		young := false
		for _, e := range inv {
			if !e.claim.ownedBy(cand.idHash, cand.gen) {
				continue
			}
			// The owned assignment must itself be settled; reconcileOrphanQ2AffClaims
			// observed it earlier in this same pass.
			if !q.observedOrphanFor(mqttQuarantineAssignKey(e.subject), mqttQuarantineSeqFP(e.seq), grace) {
				young = true
				break
			}
			owned = append(owned, e)
		}
		if young {
			continue // a young assignment protects its otherwise-old consumer
		}

		// (2) Commit Discarded on each, recording a positive result per exact identity.
		// An exhausted budget or an unresolved transition retains the consumer: "not
		// visited this tick" may never become "owns no unreconciled claim".
		stalled := false
		for _, e := range owned {
			if e.claim.Discarded || e.claim.Lost {
				continue
			}
			if *budget <= 0 {
				stalled = true
				break
			}
			*budget--
			if _, _, aerr := as.advanceQ2AffClaim(e.subject, cand.idHash, cand.gen,
				func(c *mqttQ2AffClaim) { c.Discarded = true }); aerr != nil {
				stalled = true
				break
			}
		}
		if stalled {
			continue
		}

		// (3) A second COMPLETE inventory as a verification barrier — not as a source
		// of absence from a capped view. A claim created since step 1, or one still not
		// durably discarded, retains the consumer.
		verify, verr := as.collectQ2AffInventory()
		if verr != nil {
			s.Debugf("MQTT orphaned QoS 2 sweep: verification scan failed, retaining affine consumer %q: %v", cand.durable, verr)
			continue
		}
		if !mqttQ2AffGenerationSettled(verify, cand.idHash, cand.gen) {
			continue
		}

		// (4) Final revalidation: a session that reappeared owns its consumer again.
		ps, _, serr := as.loadFenceSession(cand.idHash)
		if serr != nil {
			continue
		}
		if ps != nil {
			if ps.Q2AffGen == cand.gen {
				continue // returned, or indeterminate
			}
			// As in discardAbsentWinnerClaim: a record that appeared after the reference
			// walk has not been validated by it, and this re-read is what authorizes the
			// delete. A malformed-but-LIVE record may well reference this consumer, so it
			// must never be read as evidence that nothing does.
			if verr := mqttValidateSessionRecord(ps, cand.idHash); verr != nil {
				continue
			}
			if ps.Q2Aff != nil && ps.Q2Aff.Durable == cand.durable {
				continue
			}
		}

		// (5) Re-verify the consumer is still the INSTANCE quarantine observed. A
		// durable deleted and recreated under the same name since the listing is a
		// different consumer holding different copies — and recreation is precisely
		// what the grace exists to protect, since a peer provisions the affine consumer
		// BEFORE saving the record that references it. Deleting by name alone would
		// destroy that peer's fresh consumer inside the very window the wait covers,
		// because the candidate still carries the previous instance's fingerprint.
		ci, lerr := as.jsa.lookupConsumer(mqttOutStreamName, cand.durable)
		if lerr != nil || ci == nil || ci.Config == nil {
			continue // gone, or unreadable: nothing to delete, and absence is not proven
		}
		if !ci.Created.Equal(cand.created) {
			// A different instance. Its own wait starts from the next listing.
			continue
		}
		if verr := mqttValidateQ2AffineNames(ci.Config, cand.idHash, cand.gen); verr != nil {
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining affine consumer %q: %v", cand.durable, verr)
			continue
		}

		// (6) Only now.
		if _, derr := as.jsa.deleteConsumer(mqttOutStreamName, cand.durable, false); isErrorOtherThan(derr, JSConsumerNotFoundErr) {
			s.Warnf("MQTT orphaned QoS 2 sweep: unable to delete orphaned affine consumer %q: %v", cand.durable, derr)
			continue
		}
		s.Debugf("MQTT orphaned QoS 2 sweep: deleted orphaned affine consumer %q", cand.durable)
		q.forget(mqttQuarantineConsumerKey(cand.durable))

		// (7) The copies are gone with the consumer, so the assignments that were
		// waiting only on an authoritatively absent copy can finish now.
		for _, e := range owned {
			as.reapQ2AffClaim(s, e.subject)
			as.forgetIfQ2AffClaimGone(e.subject)
		}
	}
}

// mqttQ2AffGenerationSettled reports whether every assignment the verification scan
// attributes to this generation is durably terminal — Discarded, or the Lost record
// that is terminal and permanently retained.
func mqttQ2AffGenerationSettled(inv []*mqttQ2AffInvEntry, idHash, gen string) bool {
	for _, e := range inv {
		if !e.claim.ownedBy(idHash, gen) {
			continue
		}
		if !e.claim.Discarded && !e.claim.Lost {
			return false
		}
	}
	return true
}

// mqttParseQ2AffDurable splits an affine consumer durable back into the identity it
// was built from ("$MQTT_Q2AFF_<idHash>_<generation>"), requiring the exact shape
// mqttQ2AffineNames produces. The separator is "_", which neither a hash nor a
// generation can contain, so the split is unambiguous.
func mqttParseQ2AffDurable(durable string) (idHash, gen string, err error) {
	rest := strings.TrimPrefix(durable, mqttQ2AffineConsumerDurablePrefix)
	if len(rest) != sysHashLen+1+mqttQ2AffGenLen || rest[sysHashLen] != '_' {
		return _EMPTY_, _EMPTY_, fmt.Errorf("durable does not have the affine consumer shape")
	}
	idHash, gen = rest[:sysHashLen], rest[sysHashLen+1:]
	if !mqttValidIDHashToken(idHash) {
		return _EMPTY_, _EMPTY_, fmt.Errorf("durable carries a malformed client-ID hash token %q", idHash)
	}
	if !mqttValidQ2AffGen(gen) {
		return _EMPTY_, _EMPTY_, fmt.Errorf("durable carries a malformed affine generation token %q", gen)
	}
	return idHash, gen, nil
}
