// Reclamation of a held identifier exclusion: the fourth route that lifts one.
//
// F-098 made identifier EXCLUSION complete. Every site that removes local tracking while
// the durable slot's release is unconfirmed hands the IDENTIFIER's exclusion to `slotOwed`,
// the sixth liveness term on the send hold, and all 27 of them reach one function. What it
// left incomplete, deliberately, was RECLAMATION: three routes lift an exclusion —
// recoverPublishTerminalSlot, finalizeTerminalSlot and settleRecoveryDropExclusion — and
// every one is an edge some OTHER operation happens to cross. An exclusion no such edge
// reaches again is never re-examined.
//
// The cost of that is not capacity. An exclusion charges no Receive Maximum, so the
// client's in-flight window never shrinks and nothing warns. It is IDENTIFIERS: bumpPI
// consults the send registry, so an excluded identifier is skipped at allocation, and
// accumulated exclusions eventually cover the space. bumpPI then returns 0, the caller
// treats that exactly as the in-flight cap, and QoS 1/2 delivery to that session stops
// outright — with AckWait re-offering forever and no event left that can free one.
//
// THIS PASS SPENDS THE EXISTING LICENCE A FOURTH TIME. It proves nothing new, writes
// nothing durable and acquires no authority: it calls confirmSlotReleased and lifts only on
// mqttReleaseEstablishedAbsent, through liftIdentifierExclusionLocked, with the same
// capture-before-read instance protocol the other three use. What it adds is a TRIGGER, so
// the proof is attempted on a bounded cycle while connected rather than only when an
// unrelated operation happens to cross the edge.
//
// WHY A LOCAL OBSERVATION IS NOT ENOUGH, and why this is not one. Four earlier plan-review
// rounds refuted a per-site release licence: an observation goes stale between the
// observing and the act, and acquirePublishAction can acquire an ordinary PUBLISH record
// carrying Cancelled. The release edge that works is a CONFIRMED DURABLE ABSENCE, and that
// is the only edge this pass uses.
//
// WHAT THE ABSENCE LICENSES, exactly, because two plan revisions claimed more and were
// refuted. The read-to-lift window is NOT fenced against a peer: the instance counter
// advances only inside this session object, and confirmSlotReleased observes without
// locking the subject against a later creation. Two writers can create on an absent
// subject — markInflightRegisterCancelledPending's create-only tombstone CAS, and the
// allocation pair — and neither needs to own the connection at the moment it lands. The
// argument is therefore not about WHO can create but about WHAT a post-absence create can
// DESCRIBE: a record's existence does not put a packet identifier in the client's hands, a
// TRANSMISSION does. The tombstone is Cancelled by construction, so it describes a delivery
// that was never transmitted; the allocation create is untransmitted at create time, and
// beginSendLocked requires local pending state and connection ownership before anything
// reaches the wire. Neither establishes an outstanding client exchange, which is why an
// absence read at an instant remains a sound licence — and why the three routes above,
// which inherit the identical window, are sound for the same reason.
//
// Session lock ordering here is as.mu -> sess.mu, and worker admission is taken outside
// both.

package mqtt

import (
	"slices"
	"time"
)

const (
	// mqttExclusionReclaimDelay is the first retry, matching the reconciliation delay the
	// action continuations use.
	mqttExclusionReclaimDelay = mqttActionReconcileDelay
	// mqttExclusionReclaimMaxDelay caps the backoff. A permanently stuck exclusion — a
	// withheld PUBREL, a preserved retired register — then costs two subject reads a
	// minute rather than one every two seconds.
	mqttExclusionReclaimMaxDelay = 30 * time.Second
	// mqttExclusionReclaimReadBudget bounds a pass in SUBJECT READS, not in identifiers.
	// Distinct restrictions accumulate WITHIN one identifier, so an entry count bounds the
	// wrong quantity: 64 identifiers holding four binds each is 256 reads, not 64.
	mqttExclusionReclaimReadBudget = 64
)

// mqttExclusionReclaimCandidate is one identifier's exclusion, snapshotted for reads taken
// outside the session lock.
type mqttExclusionReclaimCandidate struct {
	pi uint16
	// inst is the exclusion instance AT THE SNAPSHOT, which is what the lift re-checks. A
	// restriction installed or reasserted while the reads are out advances the session
	// counter past it, and the lift then refuses this stale answer.
	inst uint64
	// binds is a COPY. The session's own slice can be appended to while the reads are out,
	// and a lift computed over a slice that grew under it would discharge a conjunction it
	// never proved.
	binds []mqttSlotExclusionBind
}

// armExclusionReclaimLocked schedules the reclamation continuation, unless one is already
// scheduled.
//
// ONE CONTINUATION PER SESSION, never one per identifier: a per-identifier timer count
// would track the very residual this pass exists to shrink.
//
// Under sess.mu this may only manipulate TIMER STATE. It must not resolve the server or
// take worker admission — admitWillWorker takes willDispatchMu, and acquiring that under
// the session lock would invert the established order. Both happen inside the callback,
// where no session lock is held. time.AfterFunc itself runs nothing synchronously, so
// arming under the lock is safe.
//
// Session lock held on entry.
func (sess *mqttSession) armExclusionReclaimLocked() {
	if sess.exclReclaimArmed {
		return
	}
	sess.exclReclaimArmed = true
	time.AfterFunc(mqttExclusionReclaimDelay, func() {
		sess.runExclusionReclaim(mqttExclusionReclaimDelay)
	})
}

// armExclusionReclaimIfOwedLocked arms only when something is actually held.
//
// This is the RE-ENROLMENT half, and it exists because nonresidency is not terminal. A
// session object removed from the registry can be restored — the expiry fire restores this
// same object on a drain timeout, the clean-disconnect path does the same, and
// refused-discard recovery restores residency too. A pass that observed the removal has
// already cleared the armed flag and returned, and a restoration installs no new exclusion,
// so without this the standing exclusions come back resident with NO TIMER and reclamation
// is silently lost for the rest of the session.
//
// It is called from addSession — the one function every restoration path funnels through —
// rather than at each of those sites, for the same reason the install-side arm lives inside
// excludeIdentifierLocked. It also arms a resumed session whose recovery restored
// exclusions, which is a gain rather than a cost.
//
// Session lock held on entry.
func (sess *mqttSession) armExclusionReclaimIfOwedLocked() {
	if sess.anyExclusionHeldLocked() {
		sess.armExclusionReclaimLocked()
	}
}

// anyExclusionHeldLocked reports whether this session holds any identifier exclusion at all.
//
// ONE reader of that fact. Both questions that need it — should a restoration re-enrol the
// continuation, and should a finished pass re-arm — are the same question, and asking it in
// two loops is how the two answers drift apart.
//
// It enumerates EXCLUSIONS, which is deliberately not the question sendReservedLocked
// answers. "Is this identifier reserved" is that function's and stays that function's; this
// asks whether any restriction remains to be re-tested, over no identifier in particular.
//
// Session lock held on entry.
func (sess *mqttSession) anyExclusionHeldLocked() bool {
	for _, h := range sess.sendsInFlight {
		if slotExclusionHeldLocked(h) {
			return true
		}
	}
	return false
}

// disarmExclusionReclaimLocked records that no continuation is scheduled, so the next
// install — or the next restoration — arms a fresh one.
//
// Session lock held on entry.
func (sess *mqttSession) disarmExclusionReclaimLocked() {
	sess.exclReclaimArmed = false
}

// runExclusionReclaim is one pass: snapshot, confirm, lift, reschedule.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runExclusionReclaim(delay time.Duration) {
	as := sess.asm
	if as == nil {
		sess.mu.Lock()
		sess.disarmExclusionReclaimLocked()
		sess.mu.Unlock()
		return
	}
	// RESIDENCY FIRST, because a self-rescheduling timer holds the session object: without
	// this, an object that is no longer the live session for its client ID is pinned for as
	// long as any exclusion stands, which on a discarded session is forever. The predicate
	// is the registry's own identity comparison, the same one the lease and rebind paths
	// use, and it distinguishes THIS object from a replacement rather than asking whether
	// some session exists for the ID.
	//
	// It prevents INDEFINITE self-rescheduling of a removed object. It does not promise no
	// timer ever outlives residency: removal can happen after this check or after the
	// reschedule below, so one outstanding callback may still run and then terminate.
	if sess.disarmIfNonresident(as) {
		return
	}
	srv := sess.serverFor(nil)
	if srv == nil || !srv.admitWillWorker() {
		// Shutdown has closed the dispatch gate, or there is no server to admit against.
		// NOTHING IS OWED: this is availability work, not an obligation, which is why it
		// does not go to the server's owed-terminal registry the way a terminal release
		// does. The exclusion simply stands, which is the conservative direction.
		sess.mu.Lock()
		sess.disarmExclusionReclaimLocked()
		sess.mu.Unlock()
		return
	}
	defer srv.willWorkerWG.Done()

	cands, budgetMet := sess.snapshotExclusionReclaimLocked_()
	for _, cand := range cands {
		// SHUTDOWN IS CHECKED BEFORE EVERY SUBJECT READ, below. A pass holds its worker
		// token across every read it makes, and runShutdown waits on willWorkerWG before
		// closing the request path — so against an unresponsive backend a budget of reads
		// at the request timeout would hold graceful shutdown open for minutes, charging
		// that delay to terminal drain work that CANNOT be abandoned. This work can:
		// nothing durable is owed, the exclusions simply stand, and the next install arms
		// a fresh continuation.
		//
		// The partial evidence is discarded rather than applied. An `absent` map assembled
		// across a shutdown boundary is a conjunction whose remaining constituents were
		// never read, and lifting on it would be the false lift every other rule here
		// exists to prevent.
		absent := make(map[string]bool, len(cand.binds))
		for _, b := range cand.binds {
			// AND BEFORE EACH BIND, not only before each entry. One identifier's bind count
			// is explicitly unbounded, so a per-entry check alone still lets a single
			// many-bind entry hold the worker token for as long as its reads take.
			if !srv.willDispatchOpen() {
				sess.mu.Lock()
				sess.disarmExclusionReclaimLocked()
				sess.mu.Unlock()
				return
			}
			if b.subject == _EMPTY_ {
				continue
			}
			// THE BIND'S OWN SUBJECT, never one recomputed from the session's current
			// generation. The subject carries the generation, so an exclusion retained
			// from an older incarnation must be confirmed against the older subject; an
			// absence in the current generation says nothing about it.
			//
			// The comparison record is built from the bind alone, and that is sound for an
			// ABSENCE-ONLY decision: confirmSlotReleased returns EstablishedAbsent on an
			// empty subject read BEFORE it compares anything, and a load error or an
			// unreadable record returns Unresolved ahead of that. So this record cannot
			// manufacture an absence. What it does lose is the exact refutation class —
			// our own record reads as EstablishedOther rather than Refuted, because a
			// valid record carries a non-empty Dur and a non-zero Seq — and that costs
			// nothing here, since both outcomes mean retain and retry.
			//
			// IT MUST NEVER BE USED TO RETIRE A VERDICT ON EstablishedOther. That is the
			// one way this shortcut becomes unsound: the class is not trustworthy, only
			// the absence is.
			if sess.confirmSlotReleased(b.subject,
				&mqttInflightRecord{Nonce: b.nonce}) == mqttReleaseEstablishedAbsent {
				absent[b.subject] = true
			}
		}
		sess.mu.Lock()
		// The snapshot instance, never the current one. liftIdentifierExclusionLocked
		// re-checks it and requires every bound subject in `absent`, because one
		// constituent's absence never discharges a conjunction.
		sess.liftIdentifierExclusionLocked(cand.pi, cand.inst, absent)
		sess.mu.Unlock()
	}

	// Residency is re-checked before rescheduling, and the gate was re-checked between
	// entries above, so a long pass cannot hold willWorkerWG outstanding across a shutdown
	// that began after it started.
	if sess.disarmIfNonresident(as) {
		return
	}
	next := mqttExclusionReclaimMaxDelay
	if budgetMet {
		// A pass that met its budget has more to do NOW. Sleeping the full backoff between
		// batches would make a large set drain at one batch per cap interval; retrying at
		// the short delay bounds throughput without stranding the tail.
		next = mqttExclusionReclaimDelay
	} else if delay < mqttExclusionReclaimMaxDelay {
		if next = delay * 2; next > mqttExclusionReclaimMaxDelay {
			next = mqttExclusionReclaimMaxDelay
		}
	}
	sess.mu.Lock()
	if !sess.anyExclusionHeldLocked() {
		// Nothing left: disarm, so the next install arms a fresh continuation rather than
		// this one polling an empty set forever.
		sess.disarmExclusionReclaimLocked()
		sess.mu.Unlock()
		return
	}
	// STILL ARMED, and the flag is deliberately not cleared and re-set: clearing it here
	// would open a window in which an install saw an armed flag for a continuation that no
	// longer existed, and nothing would ever run again.
	sess.mu.Unlock()
	time.AfterFunc(next, func() { sess.runExclusionReclaim(next) })
}

// disarmIfNonresident stops the continuation when this object is no longer the live session
// for its client ID, and reports whether it did.
//
// THE OBSERVATION AND THE DISARM ARE ONE CRITICAL SECTION, under as.mu, and splitting them
// loses reclamation permanently. With the lock dropped in between:
//
//  1. the callback observes nonresidency and releases as.mu;
//  2. addSession restores THIS SAME object, takes sess.mu, finds exclReclaimArmed still
//     true, and therefore schedules nothing — correctly, by its own lights;
//  3. the callback then clears the flag and returns.
//
// The session is now resident with exclusions standing, no timer, and an armed flag saying
// one exists. Nothing runs again until an unrelated install happens to arm a fresh
// continuation, which is exactly the silent loss the re-enrolment exists to prevent —
// and on a session that is merely holding exclusions rather than allocating, no such
// install is coming.
//
// Holding as.mu across both closes it: addSession blocks until the flag is clear, and then
// its own re-enrolment sees an unarmed session and schedules. A READ lock is enough, since
// what must be excluded is addSession's writer.
//
// Session lock NOT held on entry. Takes as.mu then sess.mu, the established order.
func (sess *mqttSession) disarmIfNonresident(as *mqttAccountSessionManager) bool {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if as.sessions[sess.id] == sess {
		return false
	}
	// THE WINDOW THIS SEAM NAMES is the one between the observation and the disarm. It is
	// empty here by construction — as.mu is still held — and a fixture proves that by
	// attempting a restoration from inside it: under this code the restoration blocks and
	// then finds an unarmed session, while an implementation that released the lock lets it
	// through to find an armed one and schedule nothing. Without the seam the two orders
	// are a scheduling accident and the regression is untestable.
	if hook := mqttTestExclusionReclaimDisarmHook; hook != nil {
		hook()
	}
	sess.mu.Lock()
	sess.disarmExclusionReclaimLocked()
	sess.mu.Unlock()
	return true
}

// mqttTestExclusionReclaimDisarmHook runs inside disarmIfNonresident's critical section,
// between the nonresident observation and the disarm. Nil in production.
var mqttTestExclusionReclaimDisarmHook func()

// snapshotExclusionReclaimLocked_ takes the next batch of exclusions to confirm, and reports
// whether the read budget was met — which is what tells the caller to retry at the short
// delay rather than backing off.
//
// WHOLE ENTRIES ONLY. An identifier's binds are never split across passes: partial evidence
// is invalid the moment the instance can move, so a half-read entry would have to discard
// what it gathered. The budget is therefore met, not respected exactly, and a final entry of
// k binds overruns it by at most k-1. k itself is unbounded, so this bounds the OVERRUN and
// not the absolute size of a pass.
//
// THE CURSOR IS A PACKET IDENTIFIER VALUE, and the selection is its strict successor with
// wrap. An index into the sorted set would not be stable under mutation: with a budget of
// one and entries [L, T], index 0 visits L; replacing L with a higher H gives [T, H] and
// index 1 visits H; replacing H with L repeats the cycle, and the persistent T is never
// visited at all. Anchoring on the value cannot starve that way.
//
// Session lock NOT held on entry.
func (sess *mqttSession) snapshotExclusionReclaimLocked_() (cands []mqttExclusionReclaimCandidate, budgetMet bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()

	held := make([]uint16, 0, len(sess.sendsInFlight))
	for pi, h := range sess.sendsInFlight {
		if slotExclusionHeldLocked(h) {
			held = append(held, pi)
		}
	}
	if len(held) == 0 {
		return nil, false
	}
	slices.Sort(held)

	// The strict successor of the last VISITED identifier, wrapping to the front. Starting
	// at the successor rather than at the cursor itself is what advances the traversal even
	// when the visited entry was lifted and is no longer in `held`.
	start := 0
	for i, pi := range held {
		if pi > sess.exclReclaimCursor {
			start = i
			break
		}
		if i == len(held)-1 {
			start = 0
		}
	}
	// AT MOST ONE VISIT PER ENTRY PER CALLBACK. Without this bound the wrap would let a
	// small set be read repeatedly inside one pass until the budget was met, which is
	// wasted reads rather than progress.
	reads := 0
	for n := 0; n < len(held); n++ {
		pi := held[(start+n)%len(held)]
		h := sess.sendsInFlight[pi]
		if !slotExclusionHeldLocked(h) {
			continue
		}
		binds := make([]mqttSlotExclusionBind, len(h.slotOwed.binds))
		copy(binds, h.slotOwed.binds)
		cands = append(cands, mqttExclusionReclaimCandidate{pi: pi, inst: h.slotOwed.inst, binds: binds})
		// Advanced from the last VISITED entry, so a lift cannot rewind the traversal.
		sess.exclReclaimCursor = pi
		reads += len(binds)
		if reads >= mqttExclusionReclaimReadBudget {
			budgetMet = true
			break
		}
	}
	return cands, budgetMet
}
