// Arbitrating two frontends taking over one session from the same record base.
//
// JetStream per-subject CAS is the only durable arbitration this broker has, and
// nothing HERE can prevent a second write landing on one base — which remains true for
// the reason that survives: a CAS excludes a competing CONDITIONAL write off our base
// (the same-base boundary, docs/CONFORMANCE.md), and cannot exclude an UNCONDITIONAL
// one at all. This file is the arbitration either way, and it is still required for the
// ordinary case it was built for: a definite CAS rejection, and a superseded teardown. What it delivers is the part that can be delivered without new durable state:
// every interleaving converges on one owner, and each client is told something true.
//
// Two mechanisms, and they are one design.
//
// EVERY RECORD WRITE IS CONDITIONAL, base 0 included (mqttFrameRecordWrite). A
// first-ever create used to carry no header and therefore landed on top of whatever
// was there — so a clean-start CONNECT on broker B could erase the record broker A
// was acknowledged for, inside a single leader term, with no rejection anywhere. The
// header at 0 means "the subject must currently be EMPTY", which turns that erasure
// into a rejection.
//
// A SUPERSEDED TEARDOWN CLAIM RE-DRIVES rather than proceeding. clear() used to
// return nil when its claim (or its release) was superseded, and the CONNECT read
// that as a completed discard and carried on to its accept save — at base 0, because
// clear() had reset the sequence. The two halves closed the same stomp from opposite
// ends: the write is now refused, and the path that produced it no longer runs.
//
// THE LOOP IS THE CONTRACT, not any enumeration of processConnect's branches. Three
// review rounds each found one more reachable path through that control flow, so the
// design deliberately does not depend on having enumerated them:
//
//	R1  Any outcome meaning "the base I was going to write at is not current" — a
//	    superseded claim, a superseded release (including one reached inside
//	    finishAdoptedTeardown), or a definite 10071-class rejection — re-enters the
//	    re-drive at the authoritative restore, from the top. No path continues to a
//	    write on a base it learned was stale.
//	R2  Every write's base and parent come from the SAME authoritative read. The base
//	    is what the re-evaluated clean-start decision produces from the record just
//	    read; the parent is that record's identity (parentForNextWriteLocked), and where
//	    the read found NO record — an empty subject, or an adopted teardown marker whose
//	    release deletes it — the write is parentless. The adopted-marker exception that
//	    used to parent a successor from the marker's PWID was deleted by the
//	    write-lineage audit, so the rule now has no exception to name.
//	R3  The loop is bounded by the cycle cap, the create-retry cap and the admission
//	    deadline, and EVERY exit leaves nothing resident at seq == 0.
//
// R3's second half is not housekeeping. clear() resets the object to seq = 0 and
// deliberately does not detach sess.c, and reconcileResidentOwnership returns early on
// ourSeq == 0 (an absent subject is the ordinary state of a session whose first record
// has not landed). So a reset, still-bound, still-resident object is unreachable by
// every reaper this broker has — permanent split-brain, not a leak. That is why each
// transition below disposes BEFORE anything fallible.
//
// WHAT THIS DOES NOT CLOSE, both filed as docs/ROADMAP.md §0.2 blockers: the window in
// which a teardown that WON its claim has already deleted the consumers a surviving
// foreign record still names, and the recognition of two parentless first-creates
// racing for one empty subject. Both need durable state this slice does
// not introduce.
//
// See docs/ARCHITECTURE.md and docs/findings/ha-arbitration.md#f-029.

package mqtt

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// errMQTTClearSuperseded is what clear() returns from BOTH of its superseded arms —
// the claim and the release.
//
// What it encodes, worded so it is true at both: the base this teardown claimed is no
// longer current; the subject may be held by another record or may be absent
// entirely; this session's own local state is reset and its generation re-minted; and
// there is no further record work for us to do. The "or absent" half is not
// hair-splitting — claimTeardown reports superseded when the record is GONE as well as
// when it moved (sess_teardown.go), so a sentinel promising "another owner holds it"
// would be false on a reachable path.
//
// Three things it deliberately does NOT say:
//
//   - Not "nothing was deleted". The release arm runs after deleteTeardownTargets, so
//     this incarnation's consumers and durables are already gone — legitimately, since
//     the claim was won.
//   - Not "this session is detached". clear() resets state but does not clear sess.c,
//     which is exactly why the re-drive has to dispose the binding itself.
//   - Not "the winner's named state is intact". It is not, in one window; see the
//     claim-won deletion window in docs/ROADMAP.md §0.2.
//
// It is deliberately distinct from errMQTTClearRefusedUnresolved, which encodes
// recoverability of an UNCLASSIFIED state and must never enter the re-drive loop.
var errMQTTClearSuperseded = errors.New("teardown stopped: the base it claimed is no longer current")

// errMQTTTeardownAdoptionSuperseded is finishAdoptedTeardown's superseded release,
// surfaced instead of swallowed.
//
// It used to return nil there, after which createOrRestoreSession handed back a fresh
// base-0 object without re-reading the record that superseded it — a write on a base
// the code had authoritatively been told was stale, which is the one place the
// production path violated R1. Only the CONNECT caller acts on it; the expiry and
// sweep callers are terminal and are given that branch explicitly, because inheriting
// an error arm would log a normal outcome as a failure.
var errMQTTTeardownAdoptionSuperseded = errors.New("teardown adoption stopped: a newer record holds the subject")

// The two bounds, and they are two because there are two loops.
const (
	// mqttRecordReDriveCycles bounds full restore/claim/delete/release/create rounds.
	mqttRecordReDriveCycles = 3
	// mqttRecordCreateRetries bounds 10164 contention retries of ONE write, which
	// restore and dispose nothing. Deliberately unequal to the cycle cap: with the two
	// equal, a shared counter stops after the same number of attempts as two separate
	// ones and the mutation that merges them cannot be killed by any test.
	mqttRecordCreateRetries   = 5
	mqttRecordCreateRetryWait = 100 * time.Millisecond
	// mqttRecordReDriveDeadline is an ADMISSION bound, not a ceiling: no new top-level
	// re-drive cycle and no new accept-save retry is admitted after it, but an attempt
	// already in progress runs to mqttJSA.timeout, and the nested retry loops inside
	// claimTeardown and releaseTeardown are not deadline-checked. Threading a remaining
	// deadline into those is the stronger form and is deliberately not attempted; what
	// matters for termination is admission of new top-level work.
	mqttRecordReDriveDeadline = 5 * time.Second
)

// The three exhaustion outcomes, distinct so a caller — and a test — can tell which
// bound stopped the CONNECT.
var (
	errMQTTReDriveCyclesExhausted = fmt.Errorf(
		"the session record moved under this connection %d times running", mqttRecordReDriveCycles)
	errMQTTReDriveCreateRetriesExhausted = fmt.Errorf(
		"the session record write contended %d times running", mqttRecordCreateRetries)
	errMQTTReDriveDeadlineReached = fmt.Errorf(
		"no further session record attempt was admitted within %v", mqttRecordReDriveDeadline)
)

// mqttTestTeardownAdoptionWindowHookV fires inside finishAdoptedTeardown, between its
// deletions and the release that classifies them — the window in which a peer's record
// is still invisible to every check that ran before it. It is the only way to reach the
// superseded release deterministically: the sweep's own verifying re-read excludes a
// record that moved BEFORE the adoption starts, so the shape has to be created inside
// it. Same pattern as clear's two-window hook. Nil in production.
var mqttTestTeardownAdoptionWindowHookV atomic.Pointer[func(clientID, idHash string, markerSeq uint64)]

func setMQTTTestTeardownAdoptionWindowHook(h func(clientID, idHash string, markerSeq uint64)) func() {
	prev := mqttTestTeardownAdoptionWindowHookV.Load()
	if h == nil {
		mqttTestTeardownAdoptionWindowHookV.Store(nil)
	} else {
		mqttTestTeardownAdoptionWindowHookV.Store(&h)
	}
	return func() { mqttTestTeardownAdoptionWindowHookV.Store(prev) }
}

// mqttTestReDriveClockV replaces the re-drive's clock so the deadline can be driven
// without sleeping. Nil in production.
var mqttTestReDriveClockV atomic.Pointer[func() time.Time]

func setMQTTTestReDriveClock(h func() time.Time) func() {
	prev := mqttTestReDriveClockV.Load()
	if h == nil {
		mqttTestReDriveClockV.Store(nil)
	} else {
		mqttTestReDriveClockV.Store(&h)
	}
	return func() { mqttTestReDriveClockV.Store(prev) }
}

// mqttRecordReDrive is ONE CONNECT's budget for re-driving its record work.
//
// Both counters are per-CONNECT and persist across T0/T1/T2 re-entry rather than
// resetting per transition, and the INITIAL attempt of each kind is not counted: what
// they bound is repetition, not work. It is threaded as a parameter — including into
// createOrRestoreSession — precisely so a re-drive that re-enters the restore cannot
// silently start a fresh budget.
//
// Not safe for concurrent use, and does not need to be: one CONNECT owns it, on its
// own readLoop, under the client-ID ownership gate.
type mqttRecordReDrive struct {
	now      func() time.Time
	deadline time.Time
	cycles   int
	creates  int
}

func mqttNewRecordReDrive() *mqttRecordReDrive {
	now := time.Now
	if h := mqttTestReDriveClockV.Load(); h != nil {
		now = *h
	}
	return &mqttRecordReDrive{now: now, deadline: now().Add(mqttRecordReDriveDeadline)}
}

// admitCycle admits one more full re-drive cycle, or reports which bound refused it.
func (rd *mqttRecordReDrive) admitCycle() error {
	if !rd.now().Before(rd.deadline) {
		return errMQTTReDriveDeadlineReached
	}
	if rd.cycles >= mqttRecordReDriveCycles {
		return errMQTTReDriveCyclesExhausted
	}
	rd.cycles++
	return nil
}

// admitCreateRetry admits one more retry of a contended record write. It does NOT
// consume cycle budget: a 10164 repeats one write, not one takeover.
func (rd *mqttRecordReDrive) admitCreateRetry() error {
	if !rd.now().Before(rd.deadline) {
		return errMQTTReDriveDeadlineReached
	}
	if rd.creates >= mqttRecordCreateRetries {
		return errMQTTReDriveCreateRetriesExhausted
	}
	rd.creates++
	return nil
}

// discardTakenOverClient is the tail every [MQTT-3.1.4-2] displacement of a
// still-attached client runs: the Will drop, the flappers entry, the one DISCONNECT
// this connection is owed, and the close.
//
// Extracted so the ordinary takeover in processConnect and the re-drive's T1b cannot
// drift apart — they are the same event, and a client displaced by a re-drive is
// entitled to the same explanation as one displaced by an ordinary reconnect. The
// vocabulary is deliberately TAKEN OVER, not foreign-disposed: another connection for
// this client ID really is replacing it, so DuplicateClientID / 0x8E is the truth.
//
// c is the connecting client, for the log line only. No lock held on entry.
func (asm *mqttAccountSessionManager) discardTakenOverClient(c *client, ec *client, cid string) {
	asm.beforeTakeoverDiscard(cid)
	// Remove the "will" of the existing client before closing it — unless that
	// connection already closed and owes an immediate Will, which this takeover is too
	// late to cancel. See mqttDiscardWillOnTakeover.
	ec.mqttDiscardWillOnTakeover()
	asm.mu.Lock()
	asm.addSessToFlappers(cid)
	asm.mu.Unlock()
	c.Warnf("Replacing old client %q since both have the same client ID %q", ec, cid)
	// Tell the evicted v5 client why before the close (no-op for 3.1.1), through the
	// tail every displacement path shares: it claims the connection's one DISCONNECT
	// and stays silent for a connection displaced before its own CONNACK. Spec5 [3.1.4].
	ec.mqttNotifyTakenOver()
	go ec.closeConnection(DuplicateClientID)
}

// abandonReDrivenSession is transition T1: a PRE-BIND superseded discard, on a session
// object that is resident in the maps.
//
// Resident does not imply bound — a record-restored session is inserted with no client,
// a disconnected persistent session stays resident after its client detaches, and the
// uncertainty re-restore produces the same shape — so it branches on what is actually
// attached, exactly as the ordinary takeover tail does:
//
//	T1a  resident and unbound: evict, and tell nobody. There is no connection to
//	     notify, no Will to discard and no flapper entry to add.
//	T1b  an old client attached: evict, and run the full takeover tail on it.
//
// It runs BEFORE the caller's authoritative restore, and that ordering is what makes
// the reset-resident state unreachable rather than merely unlikely: no failure exit
// after this point can leave a seq == 0 object bound in the maps.
//
// The identity check is deliberately WAIVED (nil, not the sampled client): this
// CONNECT holds the client-ID ownership gate, and the object is being abandoned
// whatever is bound to it. Passing a client could only decline the disposal — and
// declining is precisely what would leave the object resident at seq == 0.
//
// No lock held on entry (other than the ownership gate).
func (asm *mqttAccountSessionManager) abandonReDrivenSession(c *client, es *mqttSession) {
	ec, _ := es.disposeForeignStateLocked(asm, nil)
	if ec == nil {
		return
	}
	asm.discardTakenOverClient(c, ec, es.id)
}

// detachConnectingClientForReDrive is transition T2: a POST-BIND rejection, where the
// bound client is the one we are in the middle of accepting.
//
// It must be SILENT. Nothing is notified, nothing is closed, no flapper entry is added
// and no Will is discarded — from the client's point of view its CONNECT is still in
// progress, and it is about to be re-bound to the object restored from the record that
// won. This is why the re-drive could not be one disposal primitive: the takeover tail
// would kill the very connection it is trying to accept, and — if the replacement
// write still completed — leave this broker the durable owner with no serving client
// at all.
//
// The detach is identity-checked, because here an identity mismatch means the object
// is genuinely not ours to detach; the eviction is not, for the same reason as T1.
//
// No lock held on entry (other than the ownership gate).
func (asm *mqttAccountSessionManager) detachConnectingClientForReDrive(es *mqttSession, c *client) {
	es.mu.Lock()
	if es.c == c {
		es.c = nil
	}
	es.mu.Unlock()
	asm.removeSessionIfCurrent(es)
}
