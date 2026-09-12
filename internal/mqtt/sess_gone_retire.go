// Retiring a BOUND session whose record was deleted out from under it.
//
// The session-record watch converges every MOVED record, bound sessions included. A
// DELETED one on a BOUND session was the shape it could not finish, and that is F-036's
// site 1. reconcileResidentOwnership already recognises it — a resident session holding a
// non-zero sequence whose subject is authoritatively empty — and ARMS the ownership
// marker, because absence IS an ownership fact: only a party that first CLAIMED the record
// at the sequence it believed current can remove it.
//
// But nothing consumed that marker for a bound, silent client. The bound half of the
// settlement runs PRE-DISPATCH on the connection's own readLoop
// (mqttGateDispatchOnRejection), because a QoS 1 PUBLISH served from a displaced
// connection is acked and unretractable; and the unbound half claims the client-ID gate as
// NOBODY (lockSessionForRecordRejection), so it is refused while a client is attached. A
// client that negotiated Keep Alive 0 and then goes quiet satisfies neither: it sends no
// packet to drive the first and stays bound against the second, so it held a session
// another broker had torn down for the whole remaining life of the connection — the
// split-brain [MQTT-3.1.4-3] forbids.
//
// THE LICENCE, and it needs no new durable state. A valid retirement watermark whose
// MarkerSeq exceeds our own acknowledged sequence, plus the record authoritatively absent,
// plus a full in-fence revalidation, positively proves a teardown marker landed on this
// client's record subject AFTER the record this broker is acknowledged for. Each conjunct
// earns its place: a claim marker is a CAS against the sequence the claiming party
// believed current; a failed recordGenRetirement aborts before any deletion and leaves the
// record MARKED rather than absent; a landed local write moves sess.seq while an ambiguous
// one arms seqUncertain; and an apply-time watermark regression can only cause a false
// NEGATIVE. Absence ALONE is not the licence — that is the inference F-036's site 3 is a
// finding about.
//
// WHY THE DISPOSAL CANNOT SIMPLY CALL clear(). Since F-036 unit 1 the re-mint requires a
// permit that only a closed, drained tracker over sess.c == nil can mint, so a bound
// disposal goes through the retirement sequence — close admission, drain the incarnation's
// owners, capture the Will fence, prepare, PROVE, detach, mint, destroy — and the proof
// runs BEFORE the detach, because afterwards there is no binding left to check.
//
// FAILURE IS REFUSAL, NEVER DISPOSAL UNDER A LIVE OWNER. Every arm before the detach
// reopens the tracker and leaves the incumbent bound with its Will, socket, expiry and
// disconnect state untouched. The marker stays armed and a later pass asks again.
//
// THE WILL IS NOT SUPPRESSED. A retirement watermark proves a TEARDOWN, not a successor
// connection, so the disposal is unlicensed (F-084): the remote teardown owned the durable
// session retirement, not this broker's TCP connection or its connection-local Will.
// Suppressing it here would report a takeover no record shape can demonstrate.
//
// WHAT THIS DOES NOT CLOSE, and it is a stated residual in docs/CONFORMANCE.md: a record
// GONE with an ABSENT, corrupt, indeterminate or insufficient watermark (MarkerSeq at or
// below our sequence) has no licence, so the marker stays armed and the client may strand
// until it sends traffic — which then drives the pre-dispatch settlement as before.
//
// See docs/ARCHITECTURE.md, "The incarnation completion lease" and "The
// generation-retirement watermark", and docs/findings/ha-arbitration.md F-036.

package mqtt

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// errMQTTGoneRetireNoLicence is the refusal every arm of the proof shares: nothing about
// this session was established, so the incumbent keeps serving and the marker stays armed.
var errMQTTGoneRetireNoLicence = errors.New("no retirement licence for the deleted record")

// mqttTestGoneRetireIdentityHookV fires inside the pre-detach proof, immediately BEFORE
// its final atomic identity re-check. It is the seam that makes removing that re-check
// mutation-provable: a test alters the binding or a manager index here and the proof must
// then refuse.
//
// It is deliberately NOT a pre-DISPOSAL hook. The roadmap's owed item (c) describes one,
// because disposeForeignStateFenced decides `applied == false` internally — but
// disposeCaptured reports applied: true unconditionally and performs no identity check at
// all, so on this route there is no such outcome to observe. The only point where an
// identity check exists to defeat is here. Nil in production.
var mqttTestGoneRetireIdentityHookV atomic.Pointer[func(sess *mqttSession)]

// mqttTestGoneRetireProofEntryHookV fires at the ENTRY of the in-fence proof, before its
// durable reads — the BETWEEN-READ seam the roadmap's owed item (d) calls for.
//
// It is a different seam from the identity hook and cannot be replaced by it: this one
// runs before the watermark and record reads, so a test can make the record REAPPEAR or
// the watermark change in exactly the window between the off-gate prefilter's
// observation and the proof's own. Without it, removing either in-fence durable re-read
// is unprovable, because the prefilter's reads alone would still satisfy every fixture.
// Nil in production.
var mqttTestGoneRetireProofEntryHookV atomic.Pointer[func(sess *mqttSession)]

// setMQTTTestGoneRetireProofEntryHook installs (or, with nil, clears) the proof-entry
// hook and returns a function that restores the previous value.
func setMQTTTestGoneRetireProofEntryHook(h func(sess *mqttSession)) func() {
	prev := mqttTestGoneRetireProofEntryHookV.Load()
	if h == nil {
		mqttTestGoneRetireProofEntryHookV.Store(nil)
	} else {
		mqttTestGoneRetireProofEntryHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestGoneRetireProofEntryHookV.Store(prev) })
}

// setMQTTTestGoneRetireIdentityHook installs (or, with nil, clears) the pre-recheck hook
// and returns a function that restores the previous value.
func setMQTTTestGoneRetireIdentityHook(h func(sess *mqttSession)) func() {
	prev := mqttTestGoneRetireIdentityHookV.Load()
	if h == nil {
		mqttTestGoneRetireIdentityHookV.Store(nil)
	} else {
		mqttTestGoneRetireIdentityHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestGoneRetireIdentityHookV.Store(prev) })
}

// lockSessionForGoneRetirement takes the client-ID gate as the EXACT resident object and
// the EXACT bound client, in one as.mu critical section.
//
// Neither existing form fits. lockSession checks the binding but neither manager index, so
// it would admit a claim against an object no lookup finds any more. And
// lockSessionForRecordRejection requires sess.c == nil, which is the reason this whole
// route exists.
//
// BOTH indexes are required, which is the identity set finishPendingDiscard already
// demands: as.sessions alone is weaker, and the record watch's eviction worker resolves
// through as.sessByHash — so checking only the first would let this route and that worker
// each believe it owns the object. (removeSessionIfCurrent is deliberately NOT the
// precedent here: it requires only as.sessions identity and removes as.sessByHash
// conditionally.)
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSessionForGoneRetirement(sess *mqttSession, owner *client) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	fail := as.sessions[sess.id] != sess || as.sessByHash[sess.idHash] != sess
	if !fail {
		if _, fail = as.sessLocked[sess.id]; !fail {
			sess.mu.Lock()
			fail = sess.c != owner || owner == nil
			sess.mu.Unlock()
		}
	}
	if fail {
		return fmt.Errorf("session %q is not this pass's to retire", sess.id)
	}
	as.sessLocked[sess.id] = struct{}{}
	return nil
}

// mqttGoneRetireLicence is what the off-gate prefilter captured, and what the in-fence
// proof must find UNCHANGED.
//
// It carries the observation, not a decision: every field is re-established inside the
// fence, and this struct exists so the re-check compares against the exact facts the
// prefilter acted on rather than against whatever is current.
type mqttGoneRetireLicence struct {
	owner     *client // the bound connection the prefilter saw
	seq       uint64  // our acknowledged record sequence at prefilter time
	markerSeq uint64  // the watermark's marker sequence at prefilter time
	gen       string  // the incarnation the prefilter saw
}

// retireGoneBoundSession is the licence-backed bound retirement: the consumer this route
// adds for an armed ownership marker whose client will never send another packet.
//
// Called from reconcileResidentOwnership's GONE arm, on the periodic sweep goroutine, with
// no lock and no gate held. Reports whether the session was disposed of.
func (as *mqttAccountSessionManager) retireGoneBoundSession(s *Server, sess *mqttSession, observedSeq uint64) bool {
	// 1. THE OFF-GATE PREFILTER. Cheap, and it refuses the ordinary case without ever
	//    contending for the gate: most resident sessions are neither bound nor gone. It
	//    establishes nothing on its own — every fact is re-proved inside the fence — so
	//    its only job is to avoid paying for a gate hold and a drain per pass.
	lic, ok := as.prefilterGoneRetirement(sess, observedSeq)
	if !ok {
		return false
	}

	// 2. THE GATE, as the exact resident object and the exact bound client.
	if err := as.lockSessionForGoneRetirement(sess, lic.owner); err != nil {
		return false // a CONNECT, a close, or another gated pass wins
	}
	gateHeld := true
	defer func() {
		if gateHeld {
			as.unlockSession(sess)
		}
	}()

	// 3. THE RETIREMENT, with the proof running after the record fence is open and
	//    before the detach. Every error arm here has mutated nothing durable and has
	//    left the incumbent bound.
	capsule, prepared, cerr := as.retireBoundIncarnationChecked(sess, func(sess *mqttSession) error {
		return as.proveGoneRetirement(sess, lic)
	})
	if cerr != nil {
		if !errors.Is(cerr, errMQTTGoneRetireNoLicence) {
			// A drain ceiling or an unresolved-record refusal. Both mean the incumbent
			// still has work or an unclassified write outstanding, and both are ordinary
			// outcomes of a pass that declines to act rather than faults.
			s.Debugf("MQTT deleted-record retirement: leaving session %q bound: %v", sess.id, cerr)
		}
		return false
	}

	// 4. PAST THE DETACH, AND EVERY OBLIGATION FROM HERE IS ARMED BY DEFER FIRST.
	//
	//    There is no incumbent left to leave alone: the object is about to be reset and
	//    re-minted, and the capsule is the only place the displaced client, its parked
	//    action and the retiring incarnation's Will baseline still exist. So the
	//    remaining work is armed as DEFERRED WRAPPERS over mutable variables — the same
	//    construction mqttProcessConnect uses — rather than run in straight-line code.
	//
	//    Straight-line code was WRONG here, and the failure is silent: a panic or
	//    runtime.Goexit escaping destroy (which contains test seams) or the Will phase
	//    would let destroy's own defers close the record transaction and retire the
	//    tracker, and the outer defer release the gate, while the disposition, the Will,
	//    the notification and the socket close were all skipped. The client would stay
	//    connected indefinitely against a RETIRED tracker with its Will never
	//    discharged — a stranding strictly worse than the one this route exists to fix.
	//
	//    A `defer` of a VARIABLE captures its value at the defer statement, so each
	//    wrapper reads the variable at execution time. Each is TAKEN AND CLEARED BEFORE
	//    it is invoked, never after: clearing afterwards cannot prevent a second
	//    application when the action itself panics.
	//
	//    LIFO order, which is the order these are declared in reverse: the record fence
	//    is closed by destroy's own defers, then the Will action, then the gate release,
	//    then the notification and close.
	voc := mqttDisposalOwnershipUnsettled
	var (
		connTail func()
		d        mqttForeignDisposition
		disposed bool
		willDone bool
	)
	// The four wrappers, declared in the order that gives the LIFO execution below.
	// Outermost declared first, so it runs LAST.
	//
	//   4. the connection tail  — notify and close, only after the gate is released
	//   3. the gate release
	//   2. the WILL BACKSTOP    — retries the Will if attempt 1 died part-way
	//   1. dispose + the Will   — the ordinary attempt
	defer func() {
		if tail := connTail; tail != nil {
			connTail = nil
			tail()
		}
	}()
	defer func() {
		if gateHeld {
			gateHeld = false
			as.unlockSession(sess)
		}
	}()
	// THE WILL BACKSTOP, and holistic review round 2 is why it exists. Arming the
	// connection tail before the Will guarantees the socket is closed, and that alone
	// is NOT enough: if an abnormal exit escapes applyWill before the Will is consumed,
	// the tail closes an already-detached connection whose read-loop teardown takes the
	// rebound path through mqttPublishOwedWill — which publishes ONLY an immediate Will.
	// A Will carrying a nonzero Will Delay Interval is mqttWillOwedDelayed there, so it
	// is neither published nor enqueued, and it is lost SILENTLY.
	//
	// ITS CLAIM IS EXACTLY THIS AND NO MORE: recovery from an exit BEFORE
	// mqttTakeWillLocked consumed the live Will. It is NOT general idempotence across
	// partial application, and round 3 was right to reject that reading. After the take
	// it is a no-op, because the take is exactly-once under the client lock — so a
	// retry finds nothing and returns. The deferred-action arm cannot be reached at all,
	// since the in-fence proof now REFUSES a session with a parked action.
	//
	// What is therefore NOT covered, and is a stated residual: an exit strictly BETWEEN
	// the take and the durable handoff. Production has no post-take Goexit source on
	// this route, and a genuine uncaught panic terminates the process — so the window is
	// a crash residual of the same class the codebase already carries for a parked Will
	// (reparkRefusedDiscard) and for a lost eviction trigger. It is deliberately not
	// closed with a claimed/completed handoff, which would be a fourth variant of this
	// mechanism for a window no comparable route survives either.
	defer func() {
		if disposed && !willDone {
			d.applyWill(sess, as)
		}
	}()
	defer func() {
		if !disposed {
			disposed = true
			d = as.disposeCaptured(sess, capsule, false)
			// The TAIL IS ARMED BEFORE THE WILL RUNS, not after: applyWill publishes or
			// enqueues, so an abnormal exit from inside it would otherwise leave
			// connTail unassigned and the displaced connection open against an
			// incarnation that no longer exists.
			if d.stale != nil {
				stale := d.stale
				connTail = func() {
					stale.mqttNotifyForeignDisposal(sess.id, voc)
					go stale.closeConnection(voc.closeReason)
				}
			}
		}
		// Unlicensed: a watermark proves a teardown, never a successor, so the Will is
		// discharged or consumed rather than discarded (F-084).
		d.applyWill(sess, as)
		// Set AFTER the call, so a death inside it leaves the backstop armed.
		willDone = true
	}()

	if derr := prepared.destroy(capsule.permit, true); derr != nil {
		if errors.Is(derr, errMQTTClearSuperseded) {
			// THE EXPECTED OUTCOME on this route, not a failure. The record is gone, so
			// the discard's own claim cannot land against it — which is the very fact
			// that licensed the retirement. The in-memory incarnation has been reset
			// either way.
			s.Debugf("MQTT deleted-record retirement: session %q retired over an absent record", sess.id)
		} else {
			// Post-reset, and unrecoverable: the object's subscriptions, consumers and
			// registers are erased and its generation re-minted, so the incumbent cannot
			// be put back. The client is told to try again later rather than left bound
			// to a wiped object.
			s.Warnf("MQTT deleted-record retirement: session %q was retired but its teardown did not complete: %v", sess.id, derr)
			voc = mqttDisposalTeardownIncomplete
		}
	}
	return true
}

// prefilterGoneRetirement is the off-gate observation.
//
// It reads the durable watermark, which is why it runs before the gate rather than inside
// it: a JetStream round trip under the client-ID gate would hold off every CONNECT for
// this client for its duration, once per pass, for a session that in the common case is
// not eligible at all.
//
// No lock held on entry; performs one JetStream read.
func (as *mqttAccountSessionManager) prefilterGoneRetirement(sess *mqttSession, observedSeq uint64) (mqttGoneRetireLicence, bool) {
	sess.mu.Lock()
	lic := mqttGoneRetireLicence{owner: sess.c, seq: sess.seq, gen: sess.gen}
	uncertain, txOpen, armed := sess.seqUncertain, sess.recordTxOpen, sess.recordRejectionGatesServingLocked()
	idHash := sess.idHash
	sess.mu.Unlock()
	switch {
	case lic.owner == nil:
		return lic, false // the unbound settlement already owns this shape
	case uncertain || txOpen:
		return lic, false // a write of ours is in flight or unclassified; not our question yet
	case !armed:
		return lic, false // no ownership question outstanding
	case lic.seq == 0 || lic.seq != observedSeq:
		return lic, false // the sequence moved since the observation that armed the marker
	}
	r, _, status := as.jsa.loadGenRetirement(as.domainTk, idHash)
	if status != mqttRetirementValid || r.MarkerSeq <= lic.seq {
		// Absent, unreadable, indeterminate, or a marker that does not reach past our own
		// record: no licence. The marker stays armed and traffic still drives the
		// pre-dispatch settlement. Stated residual.
		return lic, false
	}
	lic.markerSeq = r.MarkerSeq
	return lic, true
}

// proveGoneRetirement is the in-fence proof, run with the record transaction open, the
// tracker closed and drained, and the client-ID gate held — and BEFORE the detach.
//
// It re-establishes every fact the prefilter observed, because the prefilter took none of
// those locks and its watermark read predates all of them. The ORDER is prescribed: the
// durable reads run with neither as.mu nor sess.mu held, and only then is the in-memory
// identity re-checked under as.mu with sess.mu nested — the established lock order, and
// one acquisition rather than two, because two facts read under two acquisitions are not
// one observation.
//
// The watermark is read BEFORE the final record-absence read, so the last fallible durable
// observation is the one establishing absence. The retirement fact is monotone even if its
// single slot is replaced afterwards; absence is not, so absence goes last.
func (as *mqttAccountSessionManager) proveGoneRetirement(sess *mqttSession, lic mqttGoneRetireLicence) error {
	if h := mqttTestGoneRetireProofEntryHookV.Load(); h != nil {
		(*h)(sess)
	}
	sess.mu.Lock()
	idHash := sess.idHash
	sess.mu.Unlock()

	// Durable read 1: the watermark must still be valid and must still name a marker
	// beyond the sequence this session is acknowledged for.
	r, _, status := as.jsa.loadGenRetirement(as.domainTk, idHash)
	if status != mqttRetirementValid {
		return fmt.Errorf("%w: the watermark is %s", errMQTTGoneRetireNoLicence, mqttRetirementStatusLabel(status))
	}
	if r.MarkerSeq <= lic.seq {
		return fmt.Errorf("%w: the watermark's marker sequence %d does not exceed our record at %d",
			errMQTTGoneRetireNoLicence, r.MarkerSeq, lic.seq)
	}

	// Durable read 2, LAST: the record is still authoritatively absent. An unreadable
	// answer establishes nothing in either direction and refuses, as every other reader
	// of this record does.
	smsg, err := as.jsa.loadSessionMsg(as.domainTk, idHash)
	if err != nil && isErrorOtherThan(err, JSNoMessageFoundErr) {
		return fmt.Errorf("%w: the session record could not be read: %v", errMQTTGoneRetireNoLicence, err)
	}
	if smsg != nil {
		return fmt.Errorf("%w: a session record is present again at sequence %d",
			errMQTTGoneRetireNoLicence, smsg.Sequence)
	}

	if h := mqttTestGoneRetireIdentityHookV.Load(); h != nil {
		(*h)(sess)
	}

	// The in-memory identity, as ONE observation: as.mu then sess.mu, both indexes, the
	// owner, the sequence, the generation, the armed marker and the uncertainty flag.
	as.mu.Lock()
	resident := as.sessions[sess.id] == sess && as.sessByHash[sess.idHash] == sess
	sess.mu.Lock()
	same := sess.c == lic.owner && sess.seq == lic.seq && sess.gen == lic.gen &&
		!sess.seqUncertain && sess.recordRejectionGatesServingLocked() &&
		// NO PARKED ACTION. A capsule carrying BOTH a parked action and a bound client
		// is a state disposeCaptured cannot dispose correctly: it prioritises the
		// action, so the live client's Will is never consumed, and the tail then closes
		// a connection whose own teardown publishes only an IMMEDIATE Will — losing a
		// delayed one silently. The state is REACHABLE (dischargeDeferredIfUnbound
		// tests for exactly it), so this conjunct makes it unreachable ON THIS ROUTE
		// rather than handled here, which is the disposition every other pre-detach
		// refusal already takes.
		//
		// And it cannot reappear between this proof and the capsule capture: the
		// client-ID gate excludes gated parking, ungated parking requires
		// seqUncertain, and the conjunct above rejects that — so a nil action stays
		// nil for the rest of the retirement.
		sess.deferredDisconnect == nil
	sess.mu.Unlock()
	as.mu.Unlock()
	if !resident {
		return fmt.Errorf("%w: this object is no longer the client ID's session", errMQTTGoneRetireNoLicence)
	}
	if !same {
		return fmt.Errorf("%w: the session's binding, sequence, generation or record state changed", errMQTTGoneRetireNoLicence)
	}
	return nil
}
