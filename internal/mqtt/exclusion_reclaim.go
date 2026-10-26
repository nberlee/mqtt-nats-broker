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
	"strings"
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
	// mqttExclusionReclaimReadBudget bounds a pass in REQUESTS, not in identifiers. Distinct
	// restrictions accumulate WITHIN one identifier, so an entry count bounds the wrong
	// quantity: 64 identifiers holding four binds each is 256 reads, not 64. Every
	// pending-fence step's requests are charged against it as issued (F-088 WP3 rule 6 (a)).
	mqttExclusionReclaimReadBudget = 64
)

// mqttExclReclaimSeams instruments one session's exclusion-reclaim passes (F-088 T36 rows
// 18-18d). Nil in production; a fixture installs it before the session holds any exclusion.
type mqttExclReclaimSeams struct {
	// budget replaces the pass's request budget at the fence-step stop, given the pass's
	// snapshot charge (the batch sizing keeps the production constant).
	budget func(snapshotCharge int) int
	// passStart observes each pass's first candidate.
	passStart func(firstPI uint16)
	// fenceFound runs after a bind's base read found a pending fence, before its step: the
	// pause point between the register load and the fence step's first request.
	fenceFound func(pi uint16, subject string)
	// fenceStep observes each fence step's outcome.
	fenceStep func(pi uint16, subject string, out mqttReclaimFenceOutcome)
	// charged observes every ADDED fence request as it is charged.
	charged func()
	// lift observes every lift invocation.
	lift func(pi uint16)
	// passEnd observes the pass's snapshot charge and its final request account.
	passEnd func(snapshotCharge, spent int)
	// reschedule, when set, decides whether the pass re-arms its own timer.
	reschedule func(next time.Duration) bool
}

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
	// excl is the exclusion OBJECT the snapshot was taken from, by pointer: the pending-fence
	// rotation's cursor is written back to it only while it is still the live exclusion, so a
	// write-back never seeds a replacement (F-088 WP3 rule 6 (b)). fenceCursor is its value at
	// the snapshot.
	excl        *mqttSlotExclusion
	fenceCursor string
}

// mqttReclaimBindOrder is the order a candidate's binds are visited in: by subject VALUE, starting
// at the strict successor of the exclusion's fenceCursor, with wrap (the first subject when the
// cursor is empty). Value anchored, so a bind added or removed between visits cannot make the
// rotation skip or repeat one (F-088 WP3 rule 6 (b)).
func mqttReclaimBindOrder(binds []mqttSlotExclusionBind, cursor string) []mqttSlotExclusionBind {
	out := make([]mqttSlotExclusionBind, len(binds))
	copy(out, binds)
	slices.SortStableFunc(out, func(a, b mqttSlotExclusionBind) int { return strings.Compare(a.subject, b.subject) })
	if cursor == _EMPTY_ {
		return out
	}
	start := 0
	for i, b := range out {
		if b.subject > cursor {
			start = i
			break
		}
		if i == len(out)-1 {
			start = 0
		}
	}
	return append(out[start:], out[:start]...)
}

// mqttReclaimFenceOutcome is what one pending-fence step established.
type mqttReclaimFenceOutcome uint8

const (
	// mqttReclaimFenceKept: no confirmed absence (the door refused, a delete failed or its
	// outcome is unknown, or a successor stands); the subject stays out of `absent`.
	mqttReclaimFenceKept mqttReclaimFenceOutcome = iota
	// mqttReclaimFenceAbsent: the door deleted exactly the observed fence and the subject was then
	// CONFIRMED absent.
	mqttReclaimFenceAbsent
	// mqttReclaimFenceShutdown: the dispatch gate closed before a request; nothing more was issued.
	mqttReclaimFenceShutdown
)

// advanceExclusionFenceCursorLocked moves pi's exclusion's fence cursor to subject, but only while
// excl is still THE live exclusion object of pi (pointer identity, never the instance): a
// write-back never seeds a replacement exclusion (F-088 WP3 rule 6 (b)). Scheduling state only.
//
// Session lock held on entry.
func (sess *mqttSession) advanceExclusionFenceCursorLocked(pi uint16, excl *mqttSlotExclusion, subject string) {
	if h := sess.sendsInFlight[pi]; h != nil && h.slotOwed != nil && h.slotOwed == excl {
		excl.fenceCursor = subject
	}
}

// reclaimPendingFenceStep is the exclusion-reclaim pass's ONE added step (F-088 WP3 rule 6): the
// subject's base read found a positively identified pending cancellation fence, which is disposed
// of THROUGH THE ONE DOOR under the dedicated rule — the fence authority, exactly the observed
// sequence, a confirmed delete (and the door's own confirmed-absence retirement of a verdict that
// names it, unchanged from HEAD) — then a post-delete confirmation. The pass's dispatch gate is
// threaded into the door and asked before EVERY request, and every request it allows is charged
// to the pass (charge). Only the confirmation's ESTABLISHED ABSENCE reports the subject absent; a
// successor (another sequence or nonce) is preserved and keeps the identifier excluded.
//
// Session lock NOT held on entry.
func (sess *mqttSession) reclaimPendingFenceStep(subject string, seq uint64, cur *mqttInflightRecord,
	lic mqttDestroyLicence, open func() bool, charge func()) mqttReclaimFenceOutcome {

	shut := false
	gate := mqttDoorGate(func() bool {
		if !open() {
			shut = true
			return false
		}
		charge()
		return true
	})
	// A fence's disposal ends no obligation (endUnsentBeforeDestroy does not apply to
	// mqttSlotDeletePendingFence), but a serving-generation fence goes only behind an authority advance
	// under the serving connection's licence (F-119), whose every request the gate admits and charges.
	out := sess.reapLoadedRegister(subject, seq, cur, mqttSlotDeletePendingFence, lic, gate)
	switch {
	case shut:
		return mqttReclaimFenceShutdown
	case out != mqttReapDeleted:
		return mqttReclaimFenceKept
	}
	if !gate.open() {
		return mqttReclaimFenceShutdown
	}
	if sess.confirmSlotReleased(subject, cur) != mqttReleaseEstablishedAbsent {
		return mqttReclaimFenceKept
	}
	return mqttReclaimFenceAbsent
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

	sess.mu.Lock()
	seams := sess.testExclReclaim
	sess.mu.Unlock()
	if seams == nil {
		seams = &mqttExclReclaimSeams{}
	}
	cands, budgetMet := sess.snapshotExclusionReclaimLocked_()
	// THE PASS'S REQUEST ACCOUNT (F-088 WP3 rule 6 (a)): the snapshot pre-charged one base read
	// per bind; every request a pending-fence step actually issues is added here as it is issued.
	spent := 0
	for _, cand := range cands {
		spent += len(cand.binds)
	}
	snapshotCharge, budget := spent, mqttExclusionReclaimReadBudget
	if seams.budget != nil {
		budget = seams.budget(snapshotCharge)
	}
	if seams.passEnd != nil {
		defer func() { seams.passEnd(snapshotCharge, spent) }()
	}
	if seams.passStart != nil && len(cands) > 0 {
		seams.passStart(cands[0].pi)
	}
	charge := func() {
		spent++
		if seams.charged != nil {
			seams.charged()
		}
	}
	open := srv.willDispatchOpen
	// The serving connection's licence, captured once for the pass: a pending fence of the serving
	// generation is disposed of only behind an authority advance under it (F-119), admitted as a
	// background attempt so it yields to announced foreground operations.
	sess.mu.Lock()
	lic := mqttLicenceOf(sess.c).withAdmission(sess.admitBackgroundAuthority)
	sess.mu.Unlock()
	fenceSteps := 0
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
		started, stopped := false, false
		for _, b := range mqttReclaimBindOrder(cand.binds, cand.fenceCursor) {
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
			// THE BASE READ, ABSENCE-ONLY: an empty subject read is an absence, exactly as
			// confirmSlotReleased decides it BEFORE it compares anything; a load error or an
			// unreadable record is not. Nothing else this read returns is evidence of a release
			// — and IT MUST NEVER RETIRE A VERDICT on what it finds. What it adds (F-088 WP3
			// rule 6) is recognising a positively identified PENDING FENCE on the subject, which
			// is disposed of by its own rule before the absence can hold.
			cur, smsg, lerr := sess.asm.loadInflightRecord(b.subject)
			if lerr == nil && smsg == nil {
				absent[b.subject] = true
				continue
			}
			if lerr != nil || cur == nil || !mqttValidPendingFence(b.subject, cur) {
				continue
			}
			// THE BUDGET STOP (rule 6 (b)): checked before STARTING a fence step; the first
			// fence step of a pass is always allowed, and a started step always completes.
			if spent >= budget && fenceSteps > 0 {
				stopped = true
				break
			}
			started = true
			if seams.fenceFound != nil {
				seams.fenceFound(cand.pi, b.subject)
			}
			out := sess.reclaimPendingFenceStep(b.subject, smsg.Sequence, cur, lic, open, charge)
			fenceSteps++
			if seams.fenceStep != nil {
				seams.fenceStep(cand.pi, b.subject, out)
			}
			// The rotation's cursor moves to this step's subject — on the SAME exclusion object
			// only, whatever the step's outcome, so a fence that keeps failing recurs once per
			// complete traversal and blocks nothing after it.
			sess.mu.Lock()
			sess.advanceExclusionFenceCursorLocked(cand.pi, cand.excl, b.subject)
			sess.mu.Unlock()
			switch out {
			case mqttReclaimFenceShutdown:
				sess.mu.Lock()
				sess.disarmExclusionReclaimLocked()
				sess.mu.Unlock()
				return
			case mqttReclaimFenceAbsent:
				absent[b.subject] = true
			}
		}
		if stopped {
			// The candidate's partial evidence is discarded: no lift. The traversal resumes AT
			// a candidate whose fence work did not start (the only rewind), and past one whose
			// did — its remaining fences resume from its exclusion's cursor next visit.
			sess.mu.Lock()
			if started {
				sess.exclReclaimCursor = cand.pi
			} else {
				sess.exclReclaimCursor = cand.pi - 1
			}
			sess.mu.Unlock()
			budgetMet = true
			break
		}
		if seams.lift != nil {
			seams.lift(cand.pi)
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
	if seams.reschedule != nil && !seams.reschedule(next) {
		return
	}
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
	if hook := mqttTestExclusionReclaimDisarmHook.Get(); hook != nil {
		hook(sess)
	}
	sess.mu.Lock()
	sess.disarmExclusionReclaimLocked()
	sess.mu.Unlock()
	return true
}

// mqttTestExclusionReclaimDisarmHook runs inside disarmIfNonresident's critical section,
// between the nonresident observation and the disarm, for the session being disarmed. It is
// package-wide, so a hook acts only for the session its test owns. Nil in production.
var mqttTestExclusionReclaimDisarmHook = new(mqttKnob[func(*mqttSession)])

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
		cands = append(cands, mqttExclusionReclaimCandidate{pi: pi, inst: h.slotOwed.inst, binds: binds,
			excl: h.slotOwed, fenceCursor: h.slotOwed.fenceCursor})
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
