// Reaping orphaned QoS 2 state, and finishing the teardowns that strand it.
//
// Three durable structures outlive the session that created them when a broker
// dies in the wrong window, and nothing else ever revisits them:
//
//   - an inbound QoS 2 hold record on $MQTT_qos2in whose session record is gone,
//     together with the sibling release claim that arbitrates it;
//   - an assignment claim on $MQTT_qos2assign whose winner generation no longer
//     exists and which no contender happens to revisit; and
//   - an orphaned $MQTT_PUBREL_* consumer on $MQTT_out.
//
// A session-affine $MQTT_Q2AFF_* consumer is deliberately NOT in that list. It
// accumulates too, and this file used to collect it, but no positive fact can
// authorize the delete: the retirement watermark records the SESSION generation while
// the consumer is keyed by an AFFINE generation that outlives incarnations, and
// $MQTT_out is interest-retained, so a wrong delete destroys acknowledged QoS 2
// payloads. F-036 course E removed that route rather than patching it; the leak is a
// stated residual in docs/CONFORMANCE.md.
//
// None of them loses a message BY ACCUMULATING — a stalled claim is honored, never
// released. A stale `discarded` marker among them no longer fences a LATER exchange's
// hold either: the marker records the fence its writer was entitled to destroy, so a
// PUBREL whose hold sits above it reaps the marker and proceeds (F-062). This file's own
// retirement of a marker (hold first, then the marker) is likewise bounded to the exact
// record it observed. What follows converges the accumulation; the one direction it still
// does not cover is a live remote generation whose record this pass never sees (F-036).
// All three accumulate. This file converges them, and it converges the fourth
// thing that would otherwise pin the other three in place: a teardown marker whose
// broker died after claiming the record. sess_teardown.go promises such a marker is
// finished by the next CONNECT that can interpret the record or by the orphan sweep —
// and for a reserved-namespace record, which every CONNECT refuses, this pass is the
// ONLY finisher (a record with no valid generation is finished by neither: the
// adoption below validates before it deletes) — and a marked record
// deliberately reads as a REFERENCE to everything it names, so without the adoption
// pass below a never-returning client's QoS 2 state could never become eligible.
//
// # What "AUTHORITATIVE" means in this file, and what it does not
//
// Read this before any other use of the word below. Every "authoritative" claim here —
// the reference set, the record walk, the quarantine's grace, each revalidation — means
// authoritative over TWO things: the persisted session records, and this broker's own
// resident sessions. That is the whole of the evidence available to a sweeper.
//
// It does NOT observe a session that is live on ANOTHER broker whose persisted record is
// absent. A resume preserves the incarnation generation, so such a session keeps using
// the very (hash, generation) names this file condemns, and a teardown elsewhere can be
// what removed its record. Absence from the reference set is therefore not proof that a
// generation is dead; it is proof that this broker cannot see an owner.
//
// That gap is **F-036**'s open false-absence inference, whose scope was widened for it by
// F-032's audit (docs/findings/ha-arbitration.md). It is not restated at each site below:
// the word "authoritative" throughout this file carries exactly the bound stated here, and
// the consequences per resource class belong to the findings that own those classes —
// F-034 and F-036 — not to this comment. ($MQTT_qos2assign's class was F-033, whose audit
// closed on 2026-08-18. What it found in THIS file is F-064, named at its site below
// because it is a defect of ours rather than F-036's premise restated. F-033's other
// addition — that the affine consumer delete's instance fingerprint does not discriminate
// a session that ADOPTS the consumer — is HISTORICAL: that delete no longer exists, so the
// bound stated here no longer has an affine-consumer consumer at all.)
//
// # What makes a deletion legal here
//
// Ownership is the (client-ID hash, generation) pair carried in the subject or the
// durable name, never the hash alone — the hash is reused by every later session of
// the same client ID. A resource is an orphan only when its generation is
// AUTHORITATIVELY absent (in the bounded sense above) from a COMPLETE session-record
// walk: a failed lookup, a partial scan, an undecodable record or an unvalidatable
// generation all abort the destructive pass rather than being read as absence
// (invariant 1). Candidates are
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
// is reaped by no discard — it is the only durable proof that a late releaser must lose —
// and destroying the hold first is the sole thing that licenses THIS path retiring it. (A
// releaser reaps one only where its hold proves the marker never fenced its exchange: F-062.
// That is a different licence from this one and does not weaken the ordering below.) A releaser that preloaded the payload and resumes afterwards creates
// a fresh `releasing` claim against an empty subject, then performs its mandatory
// authoritative re-read (verifyQoS2HoldForRelease) and gets mqttQoS2VerifyGone, so
// it answers the client and delivers nothing. Reverse the two steps and that same
// re-read returns mqttQoS2VerifyHeld and the message is fanned out a second time
// from a session that no longer exists.
//
// The affine side has NO mirror-image rule, and that is the point. Invariant 5 used to
// be "an assignment is committed Discarded BEFORE its consumer is deleted", ordering two
// steps of a delete this pass no longer performs: F-036 course E removed the affine
// consumer delete outright, so the invariant it needed is GONE rather than reordered.
// $MQTT_out is interest-retained, which is why: deleting a consumer drops the copies it
// holds, and no positive fact available here can authorize that — the retirement
// watermark records the SESSION generation while the consumer is keyed by an AFFINE one
// that outlives incarnations. What survives is the claim side alone, where
// discardAbsentWinnerClaim may commit Discarded with NO delete following it, and the
// retained consumer plus its copies are the accepted leak in docs/CONFORMANCE.md.
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
	"sort"
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

// errMQTTSessRecordUninterpretable marks a reference-walk failure caused by a
// RECORD rather than by the transport. The distinction is operator-facing: a record
// this broker cannot interpret is permanent and its recovery is recreating the
// bridge-owned streams, whereas a scan that failed on a timeout, a leader change or
// a shutdown resolves itself on a later pass. Prescribing the destructive recovery
// for the second class would be actively harmful.
var errMQTTSessRecordUninterpretable = errors.New("session record cannot be interpreted")

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
	// a claim transition are never capped (invariant 13).
	mqttQ2AffSweepMutationBudget = 256

	// mqttPubRelRevalidateBudget bounds how many aged PUBREL consumer candidates ONE
	// pass revalidates. Each revalidation costs a session-record read, a consumer
	// lookup and a retirement-watermark read, and since F-036 unit 2 a candidate with
	// no matching retirement is RETAINED — permanently, and in unbounded number. So
	// without a bound the per-pass JetStream traffic grows with the accumulated leak and
	// eventually monopolises the sweep goroutine, which is a cost the leak's acceptance
	// did not include.
	//
	// The cursor below makes the bound a ROTATION rather than a truncation: each pass
	// starts strictly after the last durable NAME it processed, so every candidate is
	// still revisited, just not every candidate on every pass. Convergence latency grows
	// with the backlog; convergence itself does not stop, and the supported claim is
	// eventual visitation of a PERSISTENT candidate under a finite or serviceable churn
	// rate — not fairness against arrivals that permanently exceed the budget.
	//
	// What this bounds is the per-candidate REVALIDATION round trips and nothing else.
	// The consumer listing, the candidate collection and the sort still grow with the
	// $MQTT_out population, which is this sweep's pre-existing shape (shared with the
	// affine leak course E accepted) and is filed as its own roadmap item rather than
	// held against F-036.
	mqttPubRelRevalidateBudget = 128
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

	// (1b) Then condemn the survivors of the claim-won deletion window: records whose
	// generation is durably RETIRED but which carry no marker, because an unconditional
	// foreign write evicted it (the same-base boundary, docs/CONFORMANCE.md). Ordered AFTER adoption deliberately — a marker this pass
	// writes must be finished by a LATER pass, so the record stays visibly MOVED for at
	// least one backstop cadence rather than vanishing moments after it was marked.
	// See condemnRetiredSurvivors.
	as.condemnRetiredSurvivors(s)

	// (2) Candidates BEFORE references (invariant 3). A candidate the later walk does
	// not reference was already unreferenced when it was listed; the reverse order
	// would classify a resource created after the walk.
	inbound, ierr := as.collectInboundQoS2Candidates(s)
	inflight, markers, flerr := as.collectInflightCandidates(s)
	inventory, cerr := as.collectQ2AffInventory()
	consumers, conserr := as.jsa.consumerList(mqttOutStreamName)

	// (3) One authoritative, COMPLETE reference set. Anything indeterminate here
	// aborts every destructive phase below, because all of them act on absence.
	refs, affRefsBad, rerr := as.buildSessionRefs()
	if rerr != nil {
		// A record-shaped failure is PERMANENT — nothing rewrites such a record and
		// expiry refuses it too, so every later pass aborts here — and it has an
		// operator action, so it warns and names it. A transport failure resolves on
		// the next pass and must NOT carry that guidance: telling an operator to
		// recreate the streams because a scan timed out would be worse than silence.
		if errors.Is(rerr, errMQTTSessRecordUninterpretable) {
			s.Warnf("MQTT orphaned QoS 2 sweep ABORTED and nothing was reclaimed: %v; "+
				"this will not clear on its own — stop this account's brokers and recreate the "+
				"bridge-owned JetStream state (the $MQTT_* streams)", rerr)
		} else {
			s.Debugf("MQTT orphaned QoS 2 sweep ended: %v", rerr)
		}
		return
	}

	if ierr != nil {
		s.Debugf("MQTT orphaned QoS 2 sweep: inbound scan ended: %v", ierr)
	} else {
		as.reconcileInboundQoS2(s, inbound, refs)
	}

	// Outbound in-flight registers. Ordered here rather than with the affine claim
	// pass because it shares nothing with it: it needs no inventory and no second
	// barrier. It does share the mutation budget, so one class cannot starve the
	// others.
	inflightBudget := mqttQ2AffSweepMutationBudget
	if flerr != nil {
		s.Debugf("MQTT orphaned QoS 2 sweep: in-flight register scan ended: %v", flerr)
	} else {
		as.reconcileOrphanInflight(s, inflight, refs, &inflightBudget)
		as.reconcileOrphanMarkers(s, markers, &inflightBudget)
	}

	// affineUsable suppresses exactly ONE pass, reconcileOrphanQ2AffClaims, on either
	// of two causes: a malformed claim that aborted the assignment INVENTORY, or a
	// session record whose affine pair cannot be interpreted. It is a conservative
	// suppression rather than a positive authorization — since F-036 course E removed
	// the affine consumer delete, no complete inventory is needed to license anything,
	// and what the gate buys is that a per-claim transition is never written from a
	// partial view. Note the scope precisely: the retired affine REGISTER arm in
	// reconcileOrphanInflight can still mutate claims and is not behind this gate.
	// It is deliberately not an early return: the phases that share nothing with the
	// inventory keep running, or one persistent corrupt claim would disable them
	// account-wide, forever, on evidence about a different resource class.
	// Unlike an incomplete inventory an uninterpretable affine pair is PERMANENT
	// (expiry refuses such a record too), so the warning names the operator action.
	affineUsable := cerr == nil && affRefsBad == nil
	if cerr != nil {
		s.Warnf("MQTT orphaned QoS 2 sweep: assignment inventory incomplete, skipping affine reconciliation: %v", cerr)
	}
	if affRefsBad != nil {
		s.Warnf("MQTT orphaned QoS 2 sweep: skipping affine reconciliation because a session record's "+
			"affine pair cannot be interpreted, so nothing proves which affine generations are still "+
			"referenced: %v; this will not clear on its own — stop this account's brokers and recreate "+
			"the bridge-owned JetStream state (the $MQTT_* streams). Every other class is unaffected "+
			"and keeps being reclaimed", affRefsBad)
	}
	budget := mqttQ2AffSweepMutationBudget
	if affineUsable {
		as.reconcileOrphanQ2AffClaims(s, inventory, refs, &budget)
	}

	if conserr != nil {
		// Fatal to the PUBREL phase, which is the only remaining one that judges a
		// consumer against a listing, and there is none.
		s.Debugf("MQTT orphaned QoS 2 sweep: unable to list $MQTT_out consumers: %v", conserr)
		return
	}

	// PUBREL durables, from the same $MQTT_out listing. Nothing else revisits one: a
	// consumer created just before the record save that names it is referenced by
	// nothing at all if its broker dies in between, and it holds its interest-retained
	// PUBRELs forever.
	//
	// It needs no assignment inventory: eligibility rests on its own complete reference
	// snapshot, this listing, the grace, the final record re-read and the instance
	// re-lookup — so an unreadable affine claim is not evidence about it.
	as.deleteOrphanPubRelConsumers(s, consumers, refs)
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
func mqttQuarantineHoldKey(subject string) string      { return "hold|" + subject }
func mqttQuarantineClaimKey(subject string) string     { return "claim|" + subject }
func mqttQuarantineTeardownKey(idHash string) string   { return "teardown|" + idHash }
func mqttQuarantineRetirementKey(idHash string) string { return "retire|" + idHash }
func mqttQuarantinePubRelKey(durable string) string    { return "pubrel|" + durable }
func mqttQuarantineAssignKey(subject string) string    { return "assign|" + subject }
func mqttQuarantineInflightKey(subject string) string  { return "inflight|" + subject }

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
// sweeper, or by a CONNECT — converges on the RECORD under the existing teardown release
// (the inbound hold walk included: a second run that meets the first adopter's
// `discarded` marker purges to the fence THAT marker recorded, so a later exchange is
// spared rather than destroyed — F-062 arm A2, which also states what a subject the first
// run never reached does instead. The `$MQTT_inflight` purge in the same teardown stays
// bounded since F-034 by the enumeration watermark of the pass that removes it)
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
// Retirement condemnation
// ---------------------------------------------------------------------------

// condemnRetiredSurvivors marks the records that survived a teardown's deletions:
// unmarked records whose generation the durable $MQTT_teardown watermark says was
// RETIRED. It is the reader of last resort for that window — the winner's own
// release-superseded arm covers the case where the winner is alive, and the CONNECT
// restore covers a client that comes back, but a winner that crashed after recording
// the retirement leaves a hollow record that nothing else revisits.
//
// It walks the watermarks rather than the records: the watermark IS the fact, one per
// client, and the walk is bounded by the client-ID population exactly as the
// will-cancel markers are.
//
// MARK ONLY, and NOTHING is ever reaped here. Two things follow from that, both
// load-bearing:
//
//   - The finish belongs to the marker-adoption class on a LATER pass. Releasing the
//     record in the same pass would remove it moments after the marker landed, and a
//     survivor broker whose lossy record watch dropped that brief observation would
//     face an ABSENT record — the documented F-036 shape, where a bound Keep Alive 0
//     client strands. Deferring narrows that route to the case where the survivor's
//     committed-read backstop does not reach the record in time; it does not close it,
//     and a CONNECT that adopts the marker immediately does not wait at all
//     (docs/findings/ha-arbitration.md#f-036).
//   - A watermark is permanent, so there is no reap step, no reap fingerprint, and no
//     GC choice that could widen the escape it fences. An older-than-latest retirement
//     is the stated residual instead.
//
// Eligibility is a stable observation of BOTH identities — the record's sequence and
// the watermark's — so a resave, a peer's teardown or a newer retirement restarts the
// wait. A malformed watermark, an unreadable record or a record whose names are not
// this session's mutates nothing for that client: a marker licenses its adopters to
// delete every durable the record names, which is exactly why it is not written onto a
// record whose names cannot be accounted for.
func (as *mqttAccountSessionManager) condemnRetiredSurvivors(s *Server) {
	grace, q := as.orphanGrace(), as.quarantine()
	opts := s.getOpts()
	filter := mqttTeardownSubjectPrefix + as.domainTk + ">"
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttTeardownStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT retirement condemnation sweep ended: %v", err)
			}
			return
		}
		seq = smsg.Sequence + 1
		wmSeq := smsg.Sequence

		idHash, perr := mqttTeardownIDHashFromSubject(smsg.Subject, as.domainTk)
		if perr != nil {
			continue // not ours to interpret, and not ours to act on
		}
		r := &mqttGenRetirement{}
		if uerr := json.Unmarshal(smsg.Data, r); uerr != nil {
			s.Warnf("MQTT retirement condemnation: retirement watermark on %q is undecodable: %v", smsg.Subject, uerr)
			continue
		}
		if verr := mqttValidateGenRetirement(r, idHash); verr != nil {
			s.Warnf("MQTT retirement condemnation: refusing to act on the watermark on %q: %v", smsg.Subject, verr)
			continue
		}

		ps, curSeq, lerr := as.loadFenceSession(idHash)
		if lerr != nil || ps == nil {
			// Unreadable is not absence, and absence needs no repair: the retired
			// generation's record is already gone.
			continue
		}
		if ps.Gen != r.Gen {
			continue // a fresh generation owns the subject; the watermark simply stays
		}
		if ps.Teardown != nil {
			continue // marked already: the adoption class above owns it
		}
		if verr := mqttValidateTeardownTargets(ps, idHash); verr != nil {
			s.Warnf("MQTT retirement condemnation: refusing to condemn the record of %q: %v", idHash, verr)
			continue
		}

		fp := mqttQuarantineSeqFP(curSeq) + "|" + mqttQuarantineSeqFP(wmSeq)
		if !q.observedOrphanFor(mqttQuarantineRetirementKey(idHash), fp, grace) {
			continue
		}

		// Act-time re-validation at the SAME identities, as every mutation here gets:
		// the snapshot above is older than both records, and the claim CAS below must
		// land on the record this pass verified.
		cur, reSeq, rerr := as.loadFenceSession(idHash)
		if rerr != nil || cur == nil || reSeq != curSeq || cur.Gen != r.Gen || cur.Teardown != nil {
			continue
		}
		if verr := mqttValidateTeardownTargets(cur, idHash); verr != nil {
			continue
		}
		rr, reWmSeq, rstatus := as.jsa.loadGenRetirement(as.domainTk, idHash)
		if rstatus != mqttRetirementValid || reWmSeq != wmSeq || rr.Gen != r.Gen || rr.MarkerSeq != r.MarkerSeq {
			continue
		}

		// A transient session on the RETIRING generation, exactly as
		// finishAdoptedTeardown builds one: the marker is a record write, so it needs a
		// record transaction and a lineage parented to the record it is derived from.
		sess := mqttSessionCreate(&as.jsa, cur.ID, idHash, cur.Gen, reSeq, cur.WID, cur.PWID, opts)
		sess.asm = as
		sess.domainTk = as.domainTk
		tok := sess.beginRecordTx()
		_, _, outcome, cerr := sess.claimTeardown(tok, cur, reSeq, cur.WID, nil)
		if terr := sess.endRecordTx(tok); terr != nil {
			s.Errorf("MQTT: %v", terr)
		}
		if outcome != mqttTeardownClaimWon {
			// The subject moved again, or the outcome is unknown. The watermark is
			// permanent, so the next pass tries again from a fresh observation.
			s.Debugf("MQTT retirement condemnation: could not condemn the record of %q (%v): %v", idHash, outcome, cerr)
			continue
		}
		s.Warnf("MQTT retirement condemnation: condemned the record of session %q, whose generation %q was torn down",
			cur.ID, r.Gen)
		// The record now carries a marker, so this class will skip it from here on; the
		// slot is released so the adoption class's own wait is the only one left.
		q.forget(mqttQuarantineRetirementKey(idHash))
	}
}

// ---------------------------------------------------------------------------
// The authoritative session-reference snapshot
// ---------------------------------------------------------------------------

// mqttSessRefs is every session identity a live record or a local in-memory session
// claims. Absence from it is the ONLY thing that makes a resource an orphan, so it
// is built completely or not at all.
type mqttSessRefs struct {
	inbound map[string]struct{} // (idHash, incarnation generation)
	affine  map[string]struct{} // (idHash, Q2Aff generation)
	// pubRelDurables is the PUBREL consumer names a record or a local session
	// claims: the persisted name verbatim, and — for a LOCAL session — the name
	// derived from its (idHash, generation) unconditionally, because the consumer is
	// created before the record save that names it and the derived name is the only
	// thing that covers that window on this broker.
	pubRelDurables map[string]struct{}
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
// Failures come in TWO classes, and the difference is which references the record
// can still be trusted for.
//
// FATAL, returned as an error: an unattributable subject, an undecodable record, a
// client ID that does not hash back to its subject token, or a generation that
// cannot be validated. Each of those makes EVERY class underivable — the inbound
// and PUBREL references are built from `Gen` and the id hash — so any of them could
// hide a live reference in any class, and a hidden reference is a deleted live
// resource.
//
// AFFINE-INDETERMINATE, returned as the non-fatal second value (nil when every
// record validated): a record whose
// affine PAIR does not validate. Its `Gen` and id hash are sound, so the inbound,
// in-flight and PUBREL references it contributes are complete; only the affine ones
// are not, because such a record either names an affine generation that cannot be
// interpreted or (the G7 shape) carries qos-2 memberships while naming none at all,
// so a session that owns an affine consumer and outstanding claims would contribute
// no reference to them. The caller therefore skips the affine CLAIM pass and runs the
// rest — the same rule the affine claim INVENTORY already follows below, and for
// the reason stated there: otherwise one persistent bad record disables every other
// class account-wide, forever, on evidence about a different resource class. That
// matters here more than for the inventory, because a record refused this way is
// also refused by expiry, so nothing ever removes it.
//
// A record carrying a teardown marker still counts: its preserved content exists so
// recovery and orphan reconciliation do not race ahead of the teardown.
func (as *mqttAccountSessionManager) buildSessionRefs() (refs *mqttSessRefs, affineIndeterminate, err error) {
	refs = &mqttSessRefs{
		inbound:        make(map[string]struct{}),
		affine:         make(map[string]struct{}),
		pubRelDurables: make(map[string]struct{}),
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
		var pubRelDur string
		if sess.pubRelConsumer != nil {
			pubRelDur = sess.pubRelConsumer.Durable
		}
		sess.mu.Unlock()
		if idHash == _EMPTY_ {
			continue
		}
		if gen != _EMPTY_ {
			refs.inbound[mqttSessRefKey(idHash, gen)] = struct{}{}
			// UNCONDITIONALLY, whether or not this session has created its PUBREL
			// consumer yet: the create precedes the record save that names it, so the
			// derived name is what covers that window on this broker.
			_, _, dur := mqttPubRelNames(idHash, gen)
			refs.pubRelDurables[dur] = struct{}{}
		}
		if pubRelDur != _EMPTY_ {
			refs.pubRelDurables[pubRelDur] = struct{}{}
		}
		if affGen != _EMPTY_ {
			refs.affine[mqttSessRefKey(idHash, affGen)] = struct{}{}
		}
	}

	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	for seq := uint64(1); ; {
		smsg, lerr := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if lerr != nil {
			if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				return nil, nil, fmt.Errorf("session record walk failed before reaching the end: %w", lerr)
			}
			return refs, affineIndeterminate, nil
		}
		seq = smsg.Sequence + 1

		idHash, perr := mqttSessIDHashFromSubject(smsg.Subject, as.domainTk)
		if perr != nil {
			return nil, nil, fmt.Errorf("%w: session record on %q cannot be attributed: %w",
				errMQTTSessRecordUninterpretable, smsg.Subject, perr)
		}
		ps := &mqttPersistedSession{}
		if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
			return nil, nil, fmt.Errorf("%w: session record on %q at sequence %d is undecodable: %w",
				errMQTTSessRecordUninterpretable, smsg.Subject, smsg.Sequence, uerr)
		}
		// The identity half is fatal; the affine half only disables the affine claim pass.
		if getHash(ps.ID) != idHash {
			return nil, nil, fmt.Errorf("%w: session record on %q names a client ID that does not hash back to it",
				errMQTTSessRecordUninterpretable, smsg.Subject)
		}
		if _, gerr := mqttSessGenFromRecord(ps.Gen); gerr != nil {
			return nil, nil, fmt.Errorf("%w: session record on %q: %w",
				errMQTTSessRecordUninterpretable, smsg.Subject, gerr)
		}
		if averr := mqttValidateRecordAffinePair(ps, idHash); averr != nil {
			// Its inbound and PUBREL references below are still sound, so they are
			// collected; only the affine ones cannot be trusted.
			if affineIndeterminate == nil {
				affineIndeterminate = fmt.Errorf("session record on %q: %w", smsg.Subject, averr)
			}
			refs.inbound[mqttSessRefKey(idHash, ps.Gen)] = struct{}{}
			if ps.PubRel != nil {
				refs.pubRelDurables[ps.PubRel.Durable] = struct{}{}
			}
			continue
		}
		refs.inbound[mqttSessRefKey(idHash, ps.Gen)] = struct{}{}
		// The persisted PUBREL durable VERBATIM, as for the affine one: the record is
		// the authority on what it claims, and mqttValidateSessionRecord above has
		// already refused a record whose names are not this session's.
		if ps.PubRel != nil {
			refs.pubRelDurables[ps.PubRel.Durable] = struct{}{}
		}
		if ps.Q2AffGen != _EMPTY_ {
			refs.affine[mqttSessRefKey(idHash, ps.Q2AffGen)] = struct{}{}
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
	// A persisted shared QoS byte outside 0..2 is malformed — every writer here
	// stores the value AFTER processSubs has bounded it — and it is not harmlessly
	// malformed. processSubs CLAMPS a filter above QoS 2 down to 2 while restoring,
	// and the resume feeds these values in verbatim, so a record carrying 3 becomes a
	// live shared QoS 2 membership and provisions an affine path. A rule that matched
	// only the literal 2 would let exactly that record through the check below and
	// straight into the destructive readers it exists to stop. Refused, not clamped:
	// this broker did not write it.
	for key, m := range ps.SharedSubs {
		if m.QoS > 2 {
			return fmt.Errorf("record carries a shared subscription %q with an invalid QoS %d", key, m.QoS)
		}
		// AND ITS ACQUISITION REFERENCES: a membership with none, or with a level-up
		// acquisition its key does not call for, is a shape this broker never writes. The
		// pre-transfer shape (a bare QoS byte) does not even decode; this refuses what does.
		if verr := mqttValidateSharedMembership(key, m); verr != nil {
			return fmt.Errorf("record carries an unsupported shared membership: %w", verr)
		}
	}
	if ps.Q2AffGen == _EMPTY_ {
		if ps.Q2Aff != nil {
			// A consumer config with no generation to scope it: the pair is malformed,
			// and neither half can be trusted to say which durable is referenced.
			return fmt.Errorf("record carries an affine consumer config with no affine generation")
		}
		// A qos-2 shared membership IMPLIES an affine generation. No writer in this
		// tree can separate them: the affine path is provisioned in the SUBSCRIBE's
		// first filter loop, before the membership is persisted, and
		// buildPersistedSessionLocked writes SharedSubs, Q2Aff and Q2AffGen in one
		// record. A record that does separate them predates the affine path, and
		// under the compatibility boundary an unsupported persisted shape is REFUSED
		// rather than accommodated.
		//
		// Refusing it is not bookkeeping. Q2AffGen is what materializeQ2AffOnBehalf
		// compares a claim against to conclude the winner is gone, so an empty one
		// mismatches EVERY claim: a record read instead of refused turns a malformed
		// shape into proof of absence and discards an assignment owed to a delivery
		// already started — the redistribution [MQTT-4.8.2-5] forbids.
		if mqttRecordHasSharedQoS2(ps) {
			return fmt.Errorf("record carries a shared QoS 2 subscription with no affine generation")
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
// ABSENCE, and a live assignment would be discarded on it. (It could once also delete a
// referenced consumer with its interest-retained copies; F-036 course E removed that
// route.) Uninterpretable must mean indeterminate everywhere.
func mqttValidateSessionRecord(ps *mqttPersistedSession, idHash string) error {
	if getHash(ps.ID) != idHash {
		return fmt.Errorf("record names a client ID that does not hash back to %q", idHash)
	}
	if _, gerr := mqttSessGenFromRecord(ps.Gen); gerr != nil {
		return gerr
	}
	// A shared group consumer config in Cons is a shape this broker never writes, and every
	// reader reached through here acts DESTRUCTIVELY on what a record says: the reference
	// walk decides what may be deleted, and discardAbsentWinnerClaim's call authorizes a
	// Discarded transition. A record that cannot be accounted for must not license either.
	// See mqttSharedConsInRecord.
	if sid, found := mqttSharedConsInRecord(ps); found {
		return mqttSharedConsRecordErr(ps.ID, sid)
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
// not need to be — a teardown deletes none of them by name: the generation's ACQUISITIONS
// are released from its sealed acquisition ledger, and the group record's arbitrated
// count decides whether an incarnation's consumer goes.
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
//  3. Every CLAIM delete targets the EXACT sequence just observed, so a delayed pass
//     cannot remove a later exchange's claim on a reused packet identifier. The HOLD
//     removal is deliberately different: purgeQoS2Hold purges by SUBJECT, because it
//     must remove whatever occupies it including a record it cannot read. It is still
//     bounded to the sequence this pass loaded and re-confirmed, so a delayed pass
//     spares a later exchange's hold rather than destroying it (F-062). What that
//     bound does NOT close is the case where a live remote generation's record is
//     absent from this pass's view altogether (F-036).
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
		// THE RETIREMENT PROOF (F-036 site 3). Writing the `discarded` marker is
		// destructive in itself — it is the fence a later releaser must lose to — and
		// the purge behind it destroys the payload. Absence establishes that no record
		// and no LOCAL resident names this generation; it cannot distinguish that from
		// a session live on another broker whose record is absent, which still uses
		// this very subject. So a matching retirement watermark is required, and
		// without one the hold is RETAINED, whatever its age: a grace never arbitrated
		// ownership. See orphan_retire_proof.go for the leak this accepts and why no
		// bounded variant exists.
		if as.retirementProvenFor(n.idHash, n.gen) != mqttRetireProven {
			return mqttQoS2SweepDeferred
		}
		// An expired deadline: a claim that races in as `releasing` is left alone for
		// the next tick rather than blocking the sweep goroutine on a poll loop.
		//
		// The purge bound is holdSeq, the sequence of the record this pass loaded and
		// re-confirmed through observedOrphanFor — the only sequence this pass has any
		// evidence about. It is also the fence the fresh marker records, so a later
		// exchange above it can prove the marker is not about it.
		if err := as.discardQoS2Hold(s, n.hold, holdSeq, time.Now()); err != nil {
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
			// THE RETIREMENT PROOF, and the split that keeps this from being a blanket
			// retain (F-036 site 3). What needs a licence is destroying a NONTERMINAL
			// hold — a payload some session may still be entitled to release. A
			// terminal tombstone is not that: the exchange it belongs to already
			// delivered onward and committed, so removing it destroys nothing, and the
			// branch that answers from it (T, in mqttProcessPubRel) answers from its
			// ABSENCE identically once it is gone.
			//
			// The tombstone's removal is therefore allowed without proof, but only in
			// the order below: an EXACT-SEQUENCE CONFIRMED delete first, then an
			// authoritative read proving the subject empty.
			//
			// BOTH steps earn their place, and the reason is clustered JetStream. A
			// no-wait delete returns once the operation is queued or proposed, so the
			// read that follows can precede its Raft application: the pair is still
			// FAIL-SAFE either way — an unapplied delete makes the read see the record
			// and defers the whole disposition — but with an unconfirmed delete this
			// pass makes no progress in one pass and reports no error at the site that
			// caused it. The confirmation is what turns "retry next pass" into "this
			// failed, here". The absence read is separately load-bearing: it is what
			// licenses removing the claim behind the payload.
			if rec.state == mqttQoS2Done {
				if st, derr := as.releaseQoS2Record(holdSeq); st != mqttQoS2Released {
					s.Warnf("MQTT orphaned QoS 2 sweep: unable to remove the terminal QoS 2 tombstone on %q: %v", n.hold, derr)
					return mqttQoS2SweepDeferred
				}
				if after, _, aerr := as.loadQoS2Record(n.hold); aerr != nil || after != nil {
					return mqttQoS2SweepDeferred
				}
			} else {
				if as.retirementProvenFor(n.idHash, n.gen) != mqttRetireProven {
					return mqttQoS2SweepDeferred
				}
				// Bounded to the exact sequence this pass proved aged and orphaned, NOT to any
				// fence an adopted claim carries: this arm's entitlement is independently
				// established (reference walk, grace, re-confirmed sequence), and clamping it to a
				// stale low fence would make it purge nothing and defer forever.
				if err := as.purgeQoS2Hold(n.hold, holdSeq); err != nil {
					s.Warnf("MQTT orphaned QoS 2 sweep: unable to purge the orphaned hold on %q: %v", n.hold, err)
					return mqttQoS2SweepDeferred
				}
				// purgeQoS2Hold is already a confirmed purge; re-reading is defensive
				// depth, because this is the step that licenses removing the fence.
				if after, _, aerr := as.loadQoS2Record(n.hold); aerr != nil || after != nil {
					return mqttQoS2SweepDeferred
				}
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
	// rec is the DECODED register, carried because the discovery walk already read
	// these bytes to attribute the subject, so decoding here costs nothing and saves
	// a per-candidate load in the live-generation arm below. Nil when the body could
	// not be decoded, which is not fatal: such a candidate reaches only the arms that
	// need no body (the header comment on this walk explains why an unattributable
	// SUBJECT is retained rather than swept, and an unreadable BODY is the same
	// judgement one level down).
	rec *mqttInflightRecord
	// decodeErr is WHY rec is nil, carried for the same reason a stored PUBREL's verdict
	// travels with the record: an unreadable body still has to be REPORTED. Without it the
	// affine arm below — which needs a body — skipped an unsupported shape in silence, and
	// its own comment is the argument against that: nothing else revisits such a register
	// while its generation is referenced, so silence hides continuing accumulation from the
	// operator who has to act on it. Nil when the body decoded.
	decodeErr error
}

// mqttInflightMarkerCandidate is one UNSENT MARKER the discovery walk saw: the register
// it is the sibling of, its exact sequence and its body — a CAPTURE, in the boundary's
// sense, because the sibling is read only afterwards (reconcileOrphanMarkers).
type mqttInflightMarkerCandidate struct {
	name *mqttInflightName
	seq  uint64
	load mqttMarkerLoad
}

// mqttQ2AffSettleableRegister reports whether a register is one the live-generation
// affine arm may act on at all. Every clause refuses a shape no supported writer emits,
// and under the compatibility boundary meeting one means the state is not interpretable
// rather than that it needs handling.
//
// Retired is the clause the whole arm's licence rests on: a NORMAL pre-PUBREC affine
// register is non-retired and already carries Q2AffClaim and Q2AffCopy, so without it
// the arm would record AffineDone and delete a copy the client has not completed.
func mqttQ2AffSettleableRegister(rec *mqttInflightRecord, name *mqttInflightName) error {
	if rec == nil || name == nil {
		return fmt.Errorf("no register to settle")
	}
	if !mqttInflightRecordIdentityOK(rec) {
		return fmt.Errorf("register carries no usable identity")
	}
	if rec.PI != name.pi {
		// The body names an identifier other than its own subject's. Recovery already
		// refuses this; the arm would otherwise act on a register it cannot attribute.
		return fmt.Errorf("register body names packet identifier %d on the subject of %d", rec.PI, name.pi)
	}
	if !rec.Retired {
		return fmt.Errorf("register is not retired, so it is a live PUBLISH-phase delivery")
	}
	if rec.Q2AffClaim == _EMPTY_ || rec.Q2AffCopy == _EMPTY_ {
		return fmt.Errorf("register carries no affine assignment reference")
	}
	if rec.QoS != 2 {
		// A retired exchange is past PUBREC, which QoS 1 has no phase for.
		return fmt.Errorf("retired affine register carries QoS %d", rec.QoS)
	}
	// Cancelled ALONGSIDE Retired is a supported shape, not a contradiction, and the
	// precedence rule decides it: Retired dominates (F-087 WP3). A cancellation racing a
	// retirement leaves both bits durably, and refusing here would retain such a register
	// unsettled for the life of a long-lived generation. The retirement is backed by the
	// register's own PUBREC evidence — which is exactly what this settle consumes — while
	// the cancellation over a transmitted delivery is F-088's known-false write.
	if mqttInflightSourceStream(rec.Dur) != mqttOutStreamName {
		// An affine pair on a non-affine durable names a source on the wrong stream.
		return fmt.Errorf("affine register names non-affine durable %q", rec.Dur)
	}
	return nil
}

// collectInflightCandidates walks $MQTT.inflight.> once and returns every register it
// could attribute to an (idHash, generation) pair.
//
// Bounded by the same per-tick record budget as the inbound walk, and with the same
// wrapping cursor, for the same reason: an account with a large leaked history must
// not move it all in one pass. There is no byte budget because a register is a small
// fixed-shape record — unlike a hold, which carries a PUBLISH payload.
func (as *mqttAccountSessionManager) collectInflightCandidates(s *Server) ([]*mqttInflightCandidate, []*mqttInflightMarkerCandidate, error) {
	si, err := as.jsa.lookupStream(mqttInflightStreamName)
	if err != nil {
		if isErrorOtherThan(err, JSStreamNotFoundErr) {
			return nil, nil, err
		}
		return nil, nil, nil
	}
	if si == nil || si.State.Msgs == 0 || si.State.LastSeq < si.State.FirstSeq {
		return nil, nil, nil
	}
	start := as.inflightSweepStart(si.State.FirstSeq, si.State.LastSeq)
	recordLimit := as.qos2SweepRecordLimit()

	filter := mqttInflightStreamSubjectPrefix + ">"
	out := make([]*mqttInflightCandidate, 0, 16)
	var markers []*mqttInflightMarkerCandidate
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
					return nil, nil, err
				}
				break
			}
			if smsg.Sequence > span[1] {
				break
			}
			seq = smsg.Sequence + 1
			last = smsg.Sequence
			records++

			if mqttIsF105Subject(smsg.Subject) {
				// F-105's ARBITRATION RECORDS, which share this stream with the registers
				// because a JetStream atomic batch is per stream and the authority must
				// commit with the exchange mutation it authorizes. They are a different
				// record class with their own permanence: the authority, the permanent
				// generation closures and the permanent transition outcomes are never
				// deleted during the account's recovery epoch, and nothing in this sweep
				// may decide otherwise.
				//
				// Skipped rather than retained-and-logged: warning about each one as an
				// unattributable register would be false, and they are never swept.
				//
				// They ARE charged to the record budget, and the earlier version of this
				// skip that refunded them was wrong: the budget is what bounds the pass,
				// and a class that costs nothing to walk makes a stream full of permanent
				// outcomes an unbounded one. The cost is that a client with many outcomes
				// reaches its registers over more passes, which is the bounded behaviour
				// the sweep is built on.
				if records >= recordLimit {
					exhausted = true
					break
				}
				continue
			}
			if regName, merr := mqttParseInflightMarkerSubject(smsg.Subject); merr == nil {
				// An UNSENT MARKER, which shares this prefix with the registers and is a
				// different record class (inflight_marker.go). Parsed as one, never logged as
				// an unattributable register; its body is decoded here because the bytes are
				// in hand, and the read counts as the marker's CAPTURE.
				m, derr := mqttDecodeInflightMarker(smsg.Data)
				markers = append(markers, &mqttInflightMarkerCandidate{
					name: regName, seq: smsg.Sequence,
					load: mqttMarkerLoad{kind: mqttInflightResponsePresent, seq: smsg.Sequence, marker: m, decodeErr: derr},
				})
			} else if name, perr := mqttParseInflightSubject(smsg.Subject); perr != nil {
				// Retained, never swept: an unattributable subject has no provable owner,
				// so nothing here may decide it is an orphan.
				s.Warnf("MQTT orphaned QoS 2 sweep: retaining an unattributable in-flight register on %q at sequence %d: %v",
					smsg.Subject, smsg.Sequence, perr)
			} else {
				// Decoded here because these bytes are already in hand. A decode failure
				// leaves rec nil rather than dropping the candidate: the orphan arm judges
				// by the SUBJECT and says so ("readability is irrelevant here: an
				// unreadable orphan is still an orphan"), while the live-generation affine
				// arm needs a body and simply never matches a nil one.
				rec, derr := mqttDecodeInflightRecord(smsg.Data)
				if derr != nil {
					// An ILLEGAL SHAPE is unreadable for the same reason malformed JSON is,
					// and it lands in the same place: nil, with the REASON kept. The orphan
					// arm is unaffected, and nothing acts on the body — which is the
					// conservative direction, since acting on a half-written affine
					// reference is exactly what one decoder exists to prevent.
					rec = nil
				}
				out = append(out, &mqttInflightCandidate{
					name: name, seq: smsg.Sequence, rec: rec, decodeErr: derr,
				})
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

	s.Debugf("MQTT orphaned QoS 2 sweep: in-flight register scan visited %d record(s), %d candidate(s), %d unsent marker(s)",
		records, len(out), len(markers))
	return out, markers, nil
}

// reconcileOrphanMarkers is the sweep's arm of operation "reap" for unsent markers
// (N-9.3): for each marker the walk CAPTURED — sequence and body — the sibling register is
// read authoritatively and both go to the boundary, which decides rows 24-28. A marker
// whose sibling is absent, or names another exchange, is deleted at exactly the captured
// sequence; one beside its own live pair, an unreadable sibling or an unresolved read is
// preserved, and said so.
//
// A marker of a LIVE incarnation is treated the same way — a marker alone reserves nothing
// (N-9.6), so the only guard it needs is the sibling read — but only after the same
// quarantine grace a register gets, so a pair mid-construction is never met by a stale
// capture. The retirement proof is not required: it protects a REGISTER whose owner may be
// live elsewhere, and a marker whose sibling is present is preserved by the table itself.
//
// Shares the mutation budget with the register arm, so neither class starves the other.
func (as *mqttAccountSessionManager) reconcileOrphanMarkers(s *Server, candidates []*mqttInflightMarkerCandidate, budget *int) {
	if len(candidates) == 0 {
		return
	}
	grace := as.orphanGrace()
	q := as.quarantine()
	var deleted, preserved int
	for _, cand := range candidates {
		n := cand.name
		key := mqttQuarantineInflightKey(n.subject + "." + mqttInflightMarkerSuffix)
		if !q.observedOrphanFor(key, mqttQuarantineSeqFP(cand.seq), grace) {
			continue
		}
		if *budget <= 0 {
			return
		}
		mk := cand.load.observe(mqttMarkerCaptured, _EMPTY_)
		sibling := as.siblingObservation(n.subject, cand.load.nonceOf())
		switch as.executeReap(as.jsa.c, mqttMarkerCallerSweep, n.subject, sibling, mk, false) {
		case mqttMarkerDeleteMarker:
			*budget--
			deleted++
			q.forget(key)
		case mqttMarkerPreserveMarker:
			preserved++
			if sibling.kind != mqttRegisterSameExchange {
				// Kept for a reason that is not "its own pair is live": the operator may
				// need to know a marker accumulates beside a sibling nothing can read.
				s.Warnf("MQTT orphaned QoS 2 sweep: preserving the unsent marker beside %q at sequence %d: its sibling is %s",
					n.subject, cand.seq, mqttRegisterKindName(sibling.kind))
			}
			q.forget(key)
		}
	}
	if deleted+preserved > 0 {
		s.Debugf("MQTT orphaned QoS 2 sweep: reaped %d unsent marker(s), preserved %d", deleted, preserved)
	}
}

func mqttRegisterKindName(k mqttRegisterKind) string {
	switch k {
	case mqttRegisterAbsent:
		return "absent"
	case mqttRegisterSameExchange:
		return "its own exchange"
	case mqttRegisterDifferentExchange:
		return "another exchange"
	case mqttRegisterUnreadable:
		return "present but unreadable"
	case mqttRegisterIndeterminate:
		return "unreadable at this moment"
	}
	return "unobserved"
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
// This class has no ordering for the SWEEP to preserve: it competes with no claim record
// and no marker, so there is no local fence to keep in place.
//
// That is not the same as "a register has no second party", which this comment used to
// say and which the code contradicts: recovery reads registers as durable authority, and the
// slot-only custody contest (claimRecoveredInflight) REWRITES one to make the recovery-versus-
// close decision durable, invalidating every sequence an older session object held (an ordinary
// register's ownership is its exchange's entitlement since F-105 7b, and nothing rewrites it to
// claim it). A register is a takeover fence for whoever is resuming. The actor doing that is exactly what the header
// says this sweep cannot observe when its record is absent.
//
// What does NOT follow, and what this comment previously asserted: that the owning
// incarnation provably does not exist, that the register is inert, or that nobody will
// consult that identifier space again. Absence here means absent from the complete
// session walk and this broker's residents, in the bounded sense the file header defines
// — which does not observe a session live on another broker whose record was removed, a
// resume having preserved its generation. F-034 AUDITED what deleting a register in that
// state costs, and the answer splits: the deletion here is already conditioned on a
// re-read at the exact candidate sequence, so it cannot destroy a REPLACEMENT — and F-034
// refuted adding an identity comparison on top, because within one stream incarnation no
// mismatching record can occupy the sequence being deleted, so the comparison could not be
// mutation-proved. What remains is contention with a genuinely LIVE owner of the same
// register, which naming cannot arbitrate: that is F-036's predicate, and this sweep's
// share of it is recorded there rather than assumed away here.
//
// What it does share with every other class: absence is established by the complete
// session walk rather than inferred from a partial one, and eligibility is a STABLE
// OBSERVATION on this process's monotonic clock rather than any stored timestamp.
//
// The construction grace applies (not the longer release-claim one): a register is
// written moments before the delivery it names, so the window it has to survive is the
// ordinary create-before-save one.
func (as *mqttAccountSessionManager) reconcileOrphanInflight(s *Server, candidates []*mqttInflightCandidate, refs *mqttSessRefs, budget *int) {
	grace, q := as.orphanGrace(), as.quarantine()
	var deleted int
	// Sources for every settleable retired affine candidate, classified in ONE batch
	// through the same cache the resume path uses — so the source state and the
	// pair-proven-by-source predicate come from one definition, and the pass costs a
	// handful of round trips rather than two per record.
	var affine []*mqttInflightCandidate
	for _, cand := range candidates {
		if refs.hasInbound(cand.name.idHash, cand.name.gen) &&
			mqttQ2AffSettleableRegister(cand.rec, cand.name) == nil {
			affine = append(affine, cand)
		}
	}
	var srcCache *mqttInflightSourceCache
	if len(affine) > 0 {
		srcCache = as.newInflightSourceCache()
		recs := make([]*mqttInflightRecord, 0, len(affine))
		for _, cand := range affine {
			recs = append(recs, cand.rec)
		}
		srcCache.resolve(recs)
	}
	for _, cand := range candidates {
		n := cand.name
		if refs.hasInbound(n.idHash, n.gen) {
			// A live incarnation owns this identifier. Drop any wait accumulated while
			// it looked absent: a session that came back must not inherit it.
			q.forget(mqttQuarantineInflightKey(n.subject))
			// A live record protects a register through the INBOUND key, which is a
			// different key from the claim's affine one — so a remedy written for claims
			// alone leaves retired affine registers accumulating here forever for a
			// session that never resumes. That is F-064's accumulation half, and this is
			// the arm that collects them together with the copy and the claim they name.
			serr := mqttQ2AffSettleableRegister(cand.rec, n)
			switch {
			case serr == nil && srcCache != nil:
				as.settleRetiredAffineRegister(s, cand, srcCache, budget)
			case cand.rec == nil && cand.decodeErr != nil:
				// UNREADABLE, which since the schema was closed is where a half-written
				// affine reference lands: the decoder refuses it, so no arm can inspect the
				// state it names. Retained and SAID SO, for exactly the reason the arm
				// below is said out loud — this register accumulates, nothing else will
				// revisit it, and the operator is the only one who can act.
				s.Warnf("MQTT orphaned QoS 2 sweep: retaining the in-flight register on %q at sequence %d: "+
					"its body is not a shape this broker writes, so nothing may act on it: %v",
					n.subject, cand.seq, cand.decodeErr)
			case serr != nil && cand.rec != nil && (cand.rec.Q2AffClaim != _EMPTY_ || cand.rec.Q2AffCopy != _EMPTY_):
				// A register that CARRIES an affine reference and is nonetheless not
				// settleable is retained — and SAID SO. The state is permanent and does
				// not self-heal: nothing else revisits such a register while its session
				// generation is referenced, so silence would hide continuing accumulation
				// and an unsupported persisted shape from the operator who has to act on
				// it. Registers carrying no affine reference at all are the ordinary case
				// and are skipped without a word.
				s.Warnf("MQTT orphaned QoS 2 sweep: retaining the in-flight register on %q at sequence %d: "+
					"it names affine QoS 2 state but is not settleable: %v", n.subject, cand.seq, serr)
			}
			continue
		}
		if !q.observedOrphanFor(mqttQuarantineInflightKey(n.subject), mqttQuarantineSeqFP(cand.seq), grace) {
			continue
		}
		// THE RETIREMENT PROOF (F-036 site 3). This arm is reached because no record and
		// no local resident names the register's generation — which does not distinguish
		// a session that ended from one live on ANOTHER broker whose record is absent,
		// and such a session still holds this identifier and its Receive Maximum slot.
		// Deleting the register there restores it, on a crash, as an ordinary live
		// delivery for an exchange that has moved on. So a matching retirement watermark
		// is required and the register is otherwise RETAINED. See orphan_retire_proof.go.
		if as.retirementProvenFor(n.idHash, n.gen) != mqttRetireProven {
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
// settleRetiredAffineRegister collects a RETIRED affine in-flight register of a LIVE
// session generation, together with the copy and the assignment claim it names.
//
// This is F-064's accumulation half for the register class. On the successful-PUBREC
// path, if the incarnation-fenced AffineDone write fails, the copy stays unacknowledged
// and the register is durably rewritten retired; the immediate source probe sees the
// copy still live and removes neither. A live record then preserves the claim through
// its affine reference and the register through its INBOUND one, so for a session that
// never resumes all three accumulate, one triple per failed exchange.
//
// Two arms, on what the register's own source proves. For an affine delivery the source
// IS the copy, so the source classification is the copy's state.
//
// Nothing here takes a quarantine observation: a grace exists to keep a
// mid-construction resource from being acted on, and both arms act on positive,
// freshly revalidated evidence under a matching live record.
func (as *mqttAccountSessionManager) settleRetiredAffineRegister(s *Server, cand *mqttInflightCandidate,
	srcCache *mqttInflightSourceCache, budget *int) {

	rec, shape := cand.rec, q2AffShapePrecheck(cand.rec.Q2AffClaim)

	switch srcCache.state(rec) {
	case mqttInflightSourceComplete:
		// The copy was acknowledged, or is authoritatively absent while its consumer
		// still holds interest, so the exchange is over. This mirrors
		// resolveRetiredSource minus its in-memory half.
		//
		// It does NOT rely on the record-before-ack invariant to conclude the claim is
		// terminal. A Complete copy under a nonterminal claim of this assignment is a
		// state no supported path produces, so it is treated as a CONTRADICTION and
		// everything is retained.
		claim, cerr := as.q2AffClaimForCarriedCopy(rec.Q2AffClaim, rec.Q2AffCopy)
		switch {
		case errors.Is(cerr, errQ2AffCarriedCopySuperseded):
			// A POSITIVE fact about a record just read: the claim subject now holds a
			// LATER incarnation, so this register's carried copy can never be authorized
			// by anybody. The successor made no delivery and nothing is written to it.
			// Without this arm the old register accumulates for as long as that successor
			// stays nonterminal, which is the very accumulation this item closes.
			if *budget <= 0 {
				return
			}
			if as.disposeSettledAffineRegister(cand.name, rec, cand.seq) {
				*budget--
			}
		case cerr != nil:
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining retired affine register on %q: %v", rec.Q2AffClaim, cerr)
		case claim == nil:
			// The claim is already reaped, which IS the fact this register's deletion
			// waits on.
			if *budget <= 0 {
				return
			}
			if as.disposeSettledAffineRegister(cand.name, rec, cand.seq) {
				*budget--
			}
		case shape(claim) != nil:
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining retired affine register on %q: its claim's shape is not one this broker writes", rec.Q2AffClaim)
		case claim.terminal():
			if *budget <= 0 {
				return
			}
			*budget--
			// Only a PROVEN reap licenses deleting the register: it is the record that
			// carries this exchange's only remaining reference to the claim, so an
			// indeterminate outcome must retain it for a later pass.
			if as.reapQ2AffClaimChecked(s, rec.Q2AffClaim, shape) == mqttQ2AffReapIndeterminate {
				s.Warnf("MQTT orphaned QoS 2 sweep: retaining retired affine register on %q: its claim's reap was indeterminate", rec.Q2AffClaim)
				return
			}
			as.disposeSettledAffineRegister(cand.name, rec, cand.seq)
		default:
			s.Errorf("MQTT orphaned QoS 2 sweep: retaining retired affine register on %q: its copy is complete while the claim is NONTERMINAL, "+
				"which no supported path produces", rec.Q2AffClaim)
		}

	case mqttInflightSourceLive:
		// The register is durable evidence that this exchange passed a SUCCESSFUL
		// PUBREC — markInflightRegisterRetired is only reached from secureRetiredSource,
		// itself gated on one — and the live message at rec.Seq sitting on rec.Q2AffCopy
		// binds that evidence to THIS copy. That is a positive fact in hand, which is
		// what separates this write from the refuted designs that inferred an outcome
		// from an absence.
		observed, proven := srcCache.affinePairProvenBySource(rec)
		if !proven {
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining retired affine register on %q: its own source is %q rather than the copy it names",
				rec.Q2AffClaim, observed)
			return
		}
		if *budget <= 0 {
			return
		}
		// Charged on ENTRY to the write, after every refusal that precedes it. A no-op
		// or a precheck refusal inside is therefore over-charged, which defers work to
		// the next pass; under-charging would remove the bound.
		*budget--

		// (1) The snapshot is older than the record, so re-read at the EXACT sequence and
		// re-establish the shape against what the arm will act on.
		cur, smsg, lerr := as.loadInflightRecord(cand.name.subject)
		if lerr != nil || smsg == nil || smsg.Sequence != cand.seq ||
			mqttQ2AffSettleableRegister(cur, cand.name) != nil ||
			cur.Q2AffClaim != rec.Q2AffClaim || cur.Q2AffCopy != rec.Q2AffCopy {
			return
		}
		// (2) proven above. (3) Record AffineDone, through the ONE definition of that
		// transition, with a retry-time veto that refuses an unsupported shape and
		// requires the phase a retired register implies. A retired register is evidence
		// of a PUBREC, and the copy cannot have reached the client before its claim
		// became materialized — a cross-record fact neither record proves alone.
		//
		// pubRecProven FALSE: a retired register proves the SOURCE is past PUBREC, not
		// that a PUBREL bound to THIS claim incarnation is durable. Only a resolved
		// carrier read off such a PUBREL licenses that fact.
		if as.proveQ2AffCopyDelivered(rec.Q2AffClaim, rec.Q2AffCopy,
			func(c *mqttQ2AffClaim) error {
				if verr := shape(c); verr != nil {
					return verr
				}
				if c.Phase != mqttQ2AffMaterialized {
					return fmt.Errorf("%w: a retired register implies a materialized claim, and this one is %q",
						errQ2AffPrecheckRefused, c.Phase)
				}
				return nil
			}, s.Warnf, false) == nil {
			return
		}
		// (4) Delete the copy at its EXACT sequence and CONFIRM it gone. Not
		// fire-and-forget: the steps after this depend on it, and a delete whose failure
		// is only logged would let them proceed on an unproven premise.
		if derr := as.jsa.deleteMsg(mqttOutStreamName, rec.Seq, true); derr != nil &&
			isErrorOtherThan(derr, JSSequenceNotFoundErrF) &&
			!isJSErrCausedBy(derr, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
			// Indeterminate: re-read, because a response is not a verdict in either
			// direction ("did not land" is not something a failed response establishes).
			if st, _ := as.loadQ2AffCopy(rec.Q2AffCopy); st != mqttQ2AffCopyAbsent {
				s.Warnf("MQTT orphaned QoS 2 sweep: recorded the completion of %q but could not prove its copy gone, retaining the register: %v",
					rec.Q2AffClaim, derr)
				return
			}
		}
		// (5) Reap, and (6) delete the register LAST, because it is the only proof of
		// step 3's licence.
		//
		// Step 6 requires `reaped` SPECIFICALLY, and that is stricter than arm (a) on
		// purpose. Arm (a) reaches its delete on a legitimate `retained` too, because
		// there the exchange is already over and the claim is retained for a reason that
		// has nothing to do with this register. Here the register is the only remaining
		// carrier of a claim this pass has just written to, so anything short of a proven
		// deletion leaves it for the next pass.
		if as.reapQ2AffClaimChecked(s, rec.Q2AffClaim, shape) != mqttQ2AffReaped {
			return
		}
		as.disposeSettledAffineRegister(cand.name, rec, cand.seq)

	default:
		// Unknown, or Lost. Neither settles anything: an unreadable source is not an
		// absent one, and a consumer that is knowably GONE means the delivery was lost
		// rather than finished — which this arm must never launder into a completion.
	}
}

// disposeSettledAffineRegister removes a settled affine register and the in-memory
// retirement behind it, and reports whether it MUTATED anything.
//
// One route for all four of the arm's delete branches, so the two rules below hold at
// every one of them rather than at whichever was last reviewed.
//
// The verdict rule: a register carrying a cancellation is deleted through the session's
// own verdict property, which releases the verdict against the PROOF of the deletion.
// The Retired-dominates precedence is what makes such a register reachable here at all
// (F-087 WP3), and deleting it fire-and-forget would leave the verdict naming a register
// that no longer exists — an unmatchable entry for the life of the session.
//
// The retention rule: when NO resident, identity-fenced session can durably forget that
// verdict, the register is RETAINED rather than deleted. The register is the verdict's
// rightful carrier: once it is gone a resume has nothing left by which to identify the
// verdict, so the only remover would be generation retirement, over an arbitrarily
// long-lived generation, accumulating one nonce per race. Retaining is also the safe
// direction — a retained retirement suppresses and frees nothing — and its convergence
// edge is real: the session's next resume recovers it as a retirement and the
// session-path reaper runs the delete-first discipline, or the generation retires and
// reaps record and registers together.
//
// Reporting the mutation is what keeps a stable prefix of retained candidates from
// consuming the sweep's per-tick budget: the cursor advances past the scanned batch, so
// a charged no-op would starve every actionable candidate behind it.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) disposeSettledAffineRegister(name *mqttInflightName, rec *mqttInflightRecord, seq uint64) bool {
	if name == nil || rec == nil || seq == 0 {
		return false
	}
	as.mu.RLock()
	sess := as.sessByHash[name.idHash]
	as.mu.RUnlock()
	if sess != nil {
		sess.mu.Lock()
		fenced := sess.gen == name.gen
		sess.mu.Unlock()
		if !fenced {
			sess = nil // a different incarnation's state; not ours to touch
		}
	}
	if sess == nil {
		if rec.Cancelled {
			return false // retained: nothing here can forget the verdict this register carries
		}
		// A verdict that exists ONLY in a non-resident session's persisted CancelledRegs
		// is invisible from here — this arm reads the register, not the record — so such
		// a register is still deleted and its verdict stranded. Pre-existing (the sweep
		// deleted unconditionally before) and filed as F-090, docs/ROADMAP.md §P.
		//
		// NOT "bounded stale metadata", which this comment used to claim and which is
		// false: the residue is one nonce per occurrence and CUMULATIVE over a generation,
		// emptied only when the generation retires with its record. Bounded per occurrence
		// is not a bound on the aggregate.
		//
		// PHASE-GATED even with no resident session (census C19), and the absence of a
		// LOCAL session is exactly why. It does not prove the absence of an exchange: a
		// legal retired slot in an active PUBREL state can be owned by a session live on
		// ANOTHER BROKER, and deleting it here would vacate that exchange's only durable
		// claimant from underneath it. F-090's stranded verdict is a separate concern and
		// does not license bypassing the fence.
		//
		// The gate is applied against the record this sweep has already read, so it costs
		// no extra round trip. Retired is source-side suppression, not a second phase: a
		// genuinely past-PUBREC owner is protected by its explicit slot state.
		//
		// THROUGH THE SHARED ADMISSION RULE, not a phase test of its own (holistic review
		// round 4, finding 3). A phase test alone admitted a `publish` slot carrying a REMOTE
		// holder's live action token — a shape that did not exist while PUBLISH actions lived
		// in process memory, and one this sweep would have deleted out from under the broker
		// still writing it. The shared rule is also what makes the documented claim true for
		// every destructive edge, resident or not.
		// THE ONE ENUMERATED EXCEPTION TO C-IV (F-098, consult D-2's Option A). This arm
		// keeps HEAD's behaviour deliberately: it applies the phase, counter and
		// live-action guards, and it does NOT spend a release authority, so the affine
		// disposer still decides its own release.
		//
		// The population makes that containable rather than a hole: this route is reached
		// only for a RETIRED AFFINE register, the affine pair is written only with the
		// experimental shared-QoS-2 gate on, and the production command fixes that gate
		// off — so a supported broker cannot create the state this arm disposes of. Its
		// consolidation belongs to docs/ROADMAP.md section P, which owns that feature.
		//
		// Naming the exception does not make its release sound. It makes the
		// supported-boundary claim auditable, which is the whole of what is claimed.
		// NO RELEASE TOKEN IS MINTED HERE, and that is D8's requirement rather than an
		// oversight: asserting proven-unsent for a RETIRED record would be false — the
		// client answered PUBREC under that identifier — and a false assertion is not made
		// acceptable by documenting it. The exception spends no authority and keeps every
		// other guard.
		if !mqttSlotGuardsAdmitted(rec, mqttSlotRefFromRecord(name.subject, seq, rec),
			mqttSlotDeleteComplete) {
			return false
		}
		as.deleteInflightRegister(seq)
		return true
	}
	// Census C19: PHASE-GATED. Orphan processing may dispose of a settled affine register,
	// but it may not vacate a slot that is mid-exchange on a resident session — the whole
	// point of the durable owner slot is that only its own exchange retires it. The
	// reference is built from the record this sweep just read, so the gate re-proves the
	// state at the point of the delete rather than trusting the sweep's earlier read.
	// No licence: an affine (slot-only) register never takes the unsent-obligation ending.
	if !sess.reapRegisterWithVerdictGated(seq, rec.Nonce, false,
		mqttSlotRefFromRecord(name.subject, seq, rec), mqttSlotDeleteComplete, mqttDestroyLicence{}) {
		return false
	}
	as.forgetResidentRetirement(name, rec)
	return true
}

// forgetResidentRetirement drops the IN-MEMORY retirement a settled register belonged
// to, on this broker, identity-fenced to the exact exchange.
//
// It exists because settling the durable triple otherwise leaves the resident half
// behind FOREVER, and this arm is what makes that permanent. The in-memory entry is
// created by the same failed-AffineDone path this item targets; nothing re-examines it
// afterwards — both resolveRetiredSource callers run at the moment a retirement is
// CREATED, not periodically — so the only thing that used to clear it was a later
// redelivery of the copy reaching settleRetiredAffineSource. Deleting that copy removes
// exactly that route. Without this, a session that stays resident accumulates one map
// entry per such exchange, which is the same growth law the durable cleanup just closed,
// moved into the heap.
//
// Three fences, because clearing another exchange's retirement would resurrect a delivery
// this broker still owes: the session must be the one the register's SUBJECT names, its
// generation must match that subject's, and the entry's nonce must be the register's own.
// A session that is not resident here has no in-memory half to clear, and a peer that
// holds it will clear its own on the same evidence.
func (as *mqttAccountSessionManager) forgetResidentRetirement(name *mqttInflightName, rec *mqttInflightRecord) {
	if name == nil || rec == nil || rec.Dur == _EMPTY_ || rec.Seq == 0 {
		return
	}
	as.mu.RLock()
	sess := as.sessByHash[name.idHash]
	as.mu.RUnlock()
	if sess == nil {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.gen != name.gen {
		return // a different incarnation's map; not ours to touch
	}
	key := mqttDeferredKey{rec.Dur, rec.Seq}
	if cur := sess.srcAckOwed[key]; cur != nil && cur.nonce == rec.Nonce {
		delete(sess.srcAckOwed, key)
	}
}

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
// a claim's copy subject.
//
// Deliberately not part of mqttQ2AffClaimIdentityOK. That predicate gates a session
// resume and clear(), where an abort has account-wide consequences; these checks
// are derivations from naming helpers, so a future change to mqttQ2AffineNames or
// mqttQ2AffCopySubject would retroactively invalidate every stored claim. Inside the
// sweep that costs one deferred pass and keeps a claim transition from being written
// on an unverifiable identity; inside clear() it would be an account-wide teardown
// outage. Any change
// to those naming helpers must be coordinated with this validator.
func validateOrphanQ2AffClaim(subject string, c *mqttQ2AffClaim) error {
	if want := mqttQ2AffClaimSubject(c.GroupDurable, c.SourceSeq); subject != want {
		return fmt.Errorf("claim on %q does not sit on its derived subject %q", subject, want)
	}
	if want := mqttQ2AffClaimHash(c.GroupDurable, c.SourceSeq); c.ClaimHash != want {
		return fmt.Errorf("claim on %q carries claim hash %q (want %q)", subject, c.ClaimHash, want)
	}
	// Derived WITH the incarnation nonce, so a claim whose CopySubject is valid for
	// some OTHER incarnation is refused rather than acted on: the reaper and the
	// recovery arms read CopySubject to decide, and a subject that does not follow
	// from this record's own fields makes them inspect a copy the record does not own
	// (F-065).
	if want := mqttQ2AffCopySubject(c.Winner, c.Gen, c.ClaimHash, c.Nonce); c.CopySubject != want {
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
		// says the message was proven delivered AND could not be proven delivered.
		return fmt.Errorf("claim on %q asserts both AffineDone and Lost", subject)
	}
	// The same construction argument for the PUBREC proof, which mergeProven resolves
	// to exactly one shape: PubRecProven implies AffineDone and implies not Lost. A
	// record asserting either contradiction is one this broker does not write, so it
	// is REFUSED rather than normalised by the next merge that touches it.
	if c.PubRecProven && c.Lost {
		return fmt.Errorf("claim on %q asserts both a proven PUBREC and Lost", subject)
	}
	if c.PubRecProven && !c.AffineDone {
		return fmt.Errorf("claim on %q asserts a proven PUBREC without AffineDone", subject)
	}
	if !mqttValidIDHashToken(c.Winner) {
		return fmt.Errorf("claim on %q carries a malformed winner hash %q", subject, c.Winner)
	}
	return nil
}

// collectQ2AffInventory returns EVERY assignment claim in the account, or an error.
// Never partial and never capped: absence taken from a capped view is not absence
// (invariant 13). It no longer authorizes a consumer deletion — F-036 course E removed
// that route — but a per-claim transition must still not be written from a partial view.
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
		// A live generation reference makes this pass skip EVERY claim of that
		// generation, and that is deliberate rather than a gap this sweep should
		// close: it has no payload, so the only way it could resolve such a claim is
		// by asserting a fact it cannot prove.
		//
		// The STALL that used to leave behind is closed elsewhere, and
		// owner-independently: a group redelivery does carry the payload, so a
		// contender restores the winner's destroyed consumer and materializes onto it
		// (materializeQ2AffOnBehalf). The materialized-copy-gone population is still
		// owner-dependent (F-064's residual 8). See docs/findings/ha-arbitration.md.
		//
		// What this pass DOES now do under a live reference is reap the claims that are
		// already durably TERMINAL, which is the accumulation half of F-064.
		if refs.hasAffine(e.claim.Winner, e.claim.Gen) {
			q.forget(mqttQuarantineAssignKey(e.subject))
			switch {
			case e.claim.Lost:
				// UNTOUCHED, and not even attempted: reapable() refuses Lost
				// unconditionally, so a call here would be a pure round trip. The
				// absent-winner path below logs it once; this branch adds nothing.
			case e.claim.Discarded || e.claim.AffineDone:
				// Durably terminal with the reap unperformed — the shape a crash or a
				// lost ack response leaves, and which nothing else revisits while the
				// reference is live: recoverQ2AffClaims runs only on the owner's own
				// resume. No quarantine observation, because the claim is already
				// terminal rather than mid-construction, and NO budget charge, because
				// the reap may write nothing and an inventory headed by
				// retained-by-design claims would otherwise consume the whole budget on
				// the same prefix every pass.
				//
				// The CHECKED reaper: between the inventory's validation and the
				// reaper's own fresh load, a same-copy unsupported rewrite can appear,
				// and the plain reaper would delete it or normalise it through its
				// SourceAcked advance.
				as.reapQ2AffClaimChecked(s, e.subject, q2AffShapePrecheck(e.subject))
				retained++
			}
			// Everything else of a live generation is left exactly as it was, and the
			// NONTERMINAL population is deliberate rather than an omission: no fact this
			// pass could write is both true and terminal for it. See
			// settleSourcelessQ2AffClaim's own comment for why the sweep does not call
			// that rule, and F-064's bullet for the population it leaves open.
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
			// The only durable evidence that a QoS 2 message the broker accepted could
			// not be proven delivered. Surface it; never reap it (reapable() refuses
			// it unconditionally, so there is nothing to attempt either). The label is
			// not proof of loss — see F-064's residual 1 — but it is the only record
			// that anything happened at all.
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

// discardAbsentWinnerClaim establishes the winner absent FROM THIS SWEEP'S EVIDENCE and
// commits Discarded.
//
// "Absent" is a high bar and the verification order matters. Any backend error
// defers. A record whose affine generation MATCHES this claim defers, whether or not
// the rest of it validates: a matching generation means either a live winner or a
// pair we cannot interpret, and neither authorizes discarding its assignment. Only
// an authoritatively absent record, or a valid record for a DIFFERENT generation,
// gets past this bar.
//
// That bar is the strongest one available here; it is NOT a proof that the winner is
// gone, and neither of the two ways past it is. "Authoritatively absent" carries the
// bound the file header states — persisted records and this broker's residents — so a
// winner still serving on another broker with no record passes it. A valid record for a
// DIFFERENT generation establishes that ownership MOVED, which is not the same fact: F-036
// is precisely a previous owner continuing to serve after that. F-033's audit has since
// answered what committing Discarded costs, and the two readings do NOT get the same
// answer. Discarding on an ABSENT record rests on F-036's own falsified premise and is
// recorded as F-036 population. The DIFFERENT-generation reading is settled on the
// clean-start route and rests on no false premise: teardown marks every assignment of the
// retiring generation Discarded before deleting its consumer, so the session really has
// ended and [MQTT-4.8.2-5] permits the delivery to die with it. Only an obsolete previous
// owner still serving after ownership moved belongs to F-036. Either way this function is
// not where it is fixed. Note what the audit did NOT find here — the transition itself is
// CAS-conditioned at a base read in the same attempt and asserts the winner's identity, so
// no lineage is owed. See F-033, "What the audit established", in
// docs/findings/ha-arbitration.md; that section is authoritative.
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
		// fully interpretable record for a DIFFERENT generation clears this bar for the
		// winner's incarnation; anything else is indeterminate. (Clearing the bar is not
		// proof it is over — see this function's doc and the file header.)
		if verr := mqttValidateSessionRecord(ps, e.claim.Winner); verr != nil {
			return mqttQoS2SweepDeferred
		}
	}

	// Discarded ONLY. Not AffineDone, which would assert a delivery that never
	// happened, and not SourceAcked, which would let a source redelivery elect a
	// fresh winner if the group ack never actually committed.
	// No nonce: this rests on the WINNER's disposition, a session fact.
	updated, _, aerr := as.advanceQ2AffClaim(e.subject, e.claim.Winner, e.claim.Gen, _EMPTY_, _EMPTY_,
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
// PUBREL durables
// ---------------------------------------------------------------------------

// deleteOrphanPubRelConsumers removes $MQTT_PUBREL_* consumers on $MQTT_out that no
// session record and no local session references.
//
// Nothing else has ever revisited one. sweepOrphanedConsumers lists $MQTT_msgs only,
// this file no longer judges affine consumers at all (F-036 course E), and the
// reference snapshot had no PUBREL class at all — so a consumer created just before
// the record save that names it (mqtt.go, ensurePubRelConsumerSubscription) is
// unreferenced forever if its broker dies in between, and it holds the
// interest-retained PUBRELs of a session that no longer exists.
//
// Deleting one DROPS those PUBRELs, which is correct in every state these checks can
// observe: after a complete authoritative reference walk, a full grace of stable
// observation and the final record re-read below, no observable session exists that
// could deliver them, and the packet identifiers they carry died with their session.
//
// Two windows are NARROWED and not closed, because a JetStream consumer delete is by
// name and unconditioned: a record save can land after the final re-read, and the
// durable can be deleted and recreated after the instance re-lookup. Both are stated in
// docs/CONFORMANCE.md; the resume-side verify-and-recreate
// (ensurePubRelConsumerSubscription) bounds the damage to the PUBRELs the consumer
// held.
func (as *mqttAccountSessionManager) deleteOrphanPubRelConsumers(s *Server, consumers []*ConsumerInfo, refs *mqttSessRefs) {
	type pubRelCand struct {
		durable string
		idHash  string
		gen     string
		created time.Time // the exact consumer instance quarantine observed
	}
	grace, q := as.orphanGrace(), as.quarantine()
	var candidates []pubRelCand
	for _, ci := range consumers {
		if ci == nil || ci.Config == nil {
			continue
		}
		if !strings.HasPrefix(ci.Name, mqttPubRelConsumerDurablePrefix) {
			continue // not ours to judge
		}
		idHash, gen, perr := mqttParsePubRelDurable(ci.Name)
		if perr != nil {
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining PUBREL consumer %q: %v", ci.Name, perr)
			continue
		}
		// A consumer whose config does not match its own name is not ours to judge, at
		// this site or the pre-delete one below. The name is what attributes it, so a
		// mismatch means we cannot say whose delivery path this is. (The removed affine
		// class had the same rule for the same reason; this one no longer mirrors it.)
		if verr := mqttValidatePubRelNames(ci.Config, idHash, gen); verr != nil {
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining PUBREL consumer %q: %v", ci.Name, verr)
			continue
		}
		if _, ok := refs.pubRelDurables[ci.Name]; ok {
			q.forget(mqttQuarantinePubRelKey(ci.Name))
			continue
		}
		// Created is a FINGERPRINT here, never an age: a recreated consumer of the same
		// name carries a different one and restarts the wait.
		fp := strconv.FormatInt(ci.Created.UnixNano(), 10)
		if !q.observedOrphanFor(mqttQuarantinePubRelKey(ci.Name), fp, grace) {
			continue
		}
		candidates = append(candidates, pubRelCand{durable: ci.Name, idHash: idHash, gen: gen, created: ci.Created})
	}

	// The ROTATING BOUND, keyed by the durable NAME rather than by a slice index.
	//
	// A numeric cursor modulo the current length was the first attempt and it is wrong
	// under CHURN: insertions and deletions before the index shift every candidate's
	// identity, so a sustained arrival and departure rate can keep one candidate outside
	// every selected window forever — a starvation that turns a bounded-cost fix into a
	// second leak. A name is stable across both, so "start strictly after the last name
	// processed" means the same thing whatever happened to the set in between.
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].durable < candidates[j].durable })
	total := len(candidates)
	if total > mqttPubRelRevalidateBudget {
		as.mu.Lock()
		after := as.pubRelSweepAfter
		as.mu.Unlock()
		// Strictly after the last processed name, wrapping ONCE to the start — so a
		// cursor naming a candidate that has since gone simply resumes at the next
		// surviving name rather than restarting the rotation.
		start := sort.Search(total, func(i int) bool { return candidates[i].durable > after })
		if start >= total {
			start = 0
		}
		selected := make([]pubRelCand, 0, mqttPubRelRevalidateBudget)
		for i := 0; i < mqttPubRelRevalidateBudget; i++ {
			selected = append(selected, candidates[(start+i)%total])
		}
		as.mu.Lock()
		as.pubRelSweepAfter = selected[len(selected)-1].durable
		as.mu.Unlock()
		candidates = selected
		s.Debugf("MQTT orphaned QoS 2 sweep: revalidating %d of %d aged PUBREL consumer candidate(s) this pass",
			len(candidates), total)
	}
	// The pass-scoped NEGATIVE memo. Many retained candidates share a client, and within
	// one pass an unproven answer cannot change; across passes it must be re-read, which
	// is exactly the scope this has. See orphan_retire_proof.go.
	proofPass := newRetireProofPass()

	for _, cand := range candidates {
		// (1) A final AUTHORITATIVE record re-read. A record saved after the reference
		// walk names this durable and has not been validated by that walk, and a
		// delayed save does not change ConsumerInfo.Created — so the instance re-lookup
		// below cannot catch it and this read is the only thing that can.
		ps, _, serr := as.loadFenceSession(cand.idHash)
		if serr != nil {
			continue // unreadable is not absence
		}
		if ps != nil {
			if verr := mqttValidateSessionRecord(ps, cand.idHash); verr != nil {
				// A malformed but LIVE record may well reference this consumer, so it
				// must never be read as evidence that nothing does.
				continue
			}
			if ps.Gen == cand.gen {
				continue // the generation came back and owns its delivery path again
			}
			if ps.PubRel != nil && ps.PubRel.Durable == cand.durable {
				continue
			}
			// Falling through means the record names a DIFFERENT generation, which
			// establishes that ownership moved — not that this generation stopped serving,
			// which F-036 permits it to keep doing. Deleting an interest-retained PUBREL
			// durable drops the PUBRELs it holds, so what that costs in the
			// live-previous-owner state is that class's own consequence question and is
			// deliberately not answered here (see the file header's bound).
		}

		// (2) Still the INSTANCE quarantine observed? A durable deleted and recreated
		// under the same name since the listing is a different consumer holding
		// different copies — and recreation is precisely what the grace protects, since
		// the consumer is provisioned BEFORE the record that references it.
		ci, lerr := as.jsa.lookupConsumer(mqttOutStreamName, cand.durable)
		if lerr != nil || ci == nil || ci.Config == nil {
			continue // gone, or unreadable: nothing to delete, and absence is not proven
		}
		if !ci.Created.Equal(cand.created) {
			continue // a different instance; its own wait starts from the next listing
		}
		if verr := mqttValidatePubRelNames(ci.Config, cand.idHash, cand.gen); verr != nil {
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining PUBREL consumer %q: %v", cand.durable, verr)
			continue
		}

		// (3) THE RETIREMENT PROOF (F-036 site 3), and it goes here rather than beside
		// the two refs.hasInbound arms because this class is keyed differently: it is
		// attributed by parsing its own durable name and gated by refs.pubRelDurables
		// plus the record re-read above, so the generation to prove only exists after
		// that parse. Both of the states this arm can reach — no record at all, and a
		// record naming a DIFFERENT generation — establish that ownership MOVED, which
		// F-036 is precisely the finding that ownership moving does not stop the
		// previous owner serving. Deleting an interest-retained PUBREL durable drops
		// every PUBREL it holds, so a matching retirement watermark is required and the
		// consumer is otherwise RETAINED with its copies. See orphan_retire_proof.go.
		if as.retirementProvenForInPass(proofPass, cand.idHash, cand.gen) != mqttRetireProven {
			continue
		}

		// (3b) OWNERSHIP AUTHORIZES THE PHYSICAL DELETE (F-105 §5.1 point 5, Codex consult 12).
		// The retirement watermark above proves the GENERATION's ownership moved, but a watermark
		// cannot substitute for the physical name's own use-arbiter: only a RETIRED name-ownership
		// record spends the name, and RETIRED is reached SOLELY through opAuthorizeDeletion's
		// atomic batch — which in the same commit authorizes the resource's deletion — so a RETIRED
		// ownership transitively carries the matching resource authorization. LIVE means an
		// obligation may still depend on the name (F-036 lets the previous owner keep serving), so
		// the PUBREL consumer and its stored PUBRELs are RETAINED; a MISSING record is withheld,
		// never read as permission; an unreadable record is not proven absent, so like every other
		// arm in this per-candidate loop it retains rather than deleting.
		own, _, oerr := as.jsa.loadNameOwnershipPhysical(as.sessStreamEpoch, mqttOwnedStreamOut, cand.durable)
		if oerr != nil {
			s.Warnf("MQTT orphaned QoS 2 sweep: retaining PUBREL consumer %q: unable to read its name ownership: %v", cand.durable, oerr)
			continue
		}
		if own == nil || own.State != mqttNameOwnRetired {
			continue // missing (withheld) or LIVE (may still be depended on): not deletable.
		}

		// (4) Only now.
		if _, derr := as.jsa.deleteConsumer(mqttOutStreamName, cand.durable, false); isErrorOtherThan(derr, JSConsumerNotFoundErr) {
			s.Warnf("MQTT orphaned QoS 2 sweep: unable to delete orphaned PUBREL consumer %q: %v", cand.durable, derr)
			continue
		}
		s.Debugf("MQTT orphaned QoS 2 sweep: deleted orphaned PUBREL consumer %q", cand.durable)
		q.forget(mqttQuarantinePubRelKey(cand.durable))
	}
}

// mqttParsePubRelDurable splits a PUBREL consumer durable back into the identity it
// was built from ("$MQTT_PUBREL_<idHash>_<generation>"), requiring the exact shape
// mqttPubRelNames produces. The separator is "_", which neither a hash nor a
// generation can contain, so the split is unambiguous. Anything else is not ours
// to judge.
func mqttParsePubRelDurable(durable string) (idHash, gen string, err error) {
	rest := strings.TrimPrefix(durable, mqttPubRelConsumerDurablePrefix)
	if len(rest) != sysHashLen+1+mqttSessGenLen || rest[sysHashLen] != '_' {
		return _EMPTY_, _EMPTY_, fmt.Errorf("durable does not have the PUBREL consumer shape")
	}
	idHash, gen = rest[:sysHashLen], rest[sysHashLen+1:]
	if !mqttValidIDHashToken(idHash) {
		return _EMPTY_, _EMPTY_, fmt.Errorf("durable carries a malformed client-ID hash token %q", idHash)
	}
	if !mqttValidSessGen(gen) {
		return _EMPTY_, _EMPTY_, fmt.Errorf("durable carries a malformed incarnation generation token %q", gen)
	}
	return idHash, gen, nil
}
