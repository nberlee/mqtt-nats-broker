// Copyright 2026 The mqtt-nats-broker Authors
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

package mqtt

import (
	"errors"
	"sync"
	"time"
)

// The incarnation completion lease (F-036, unit 1).
//
// A session object is REUSED across incarnations: clear() re-mints its generation
// and two paths hand the same object back as a fresh session. Property P — the
// incarnation generation changes only while no client is bound to the object — was
// proved sound by F-036's third attempt and violated by exactly one call site, the
// CONNECT clean-start arm, which re-minted while the displaced client was still
// bound and admitted. The observable cost was single-broker silent loss of an
// acknowledged QoS 2 message: a displaced PUBREL derived its hold subject under the
// FRESH generation, found nothing, and answered a success-shaped PUBCOMP for a
// payload the teardown then destroyed.
//
// The lease is the ownership ledger that makes P enforceable rather than asserted:
// every asynchronous ingress that can mutate session state — a packet route, a
// delivery callback, a timer fire — ACQUIRES the incarnation's tracker before its
// first mutable observation and releases it when its synchronous extent ends,
// helper tails and response enqueue included. clear()'s destructive half — and with
// it the re-mint — runs only under a retirement permit proving the tracker was
// closed and drained to zero. A queued transport effect is NOT an owner: see
// mqttLease. Socket close and unsubscribe are cancellation, never completion evidence;
// disposal is not quiescence, detach is not quiescence, sess.c == nil is not
// quiescence (fifth attempt, binding).
//
// All of this is process memory. No persisted shape changes; a crash discards the
// ledger with the process and recovery semantics are exactly what they were.

const (
	trackerOpen uint8 = iota
	trackerClosing
	trackerRetired
)

var (
	// errMQTTTrackerRetired reports an acquisition against an incarnation that has
	// been retired: the caller's session view is of an object that has since been
	// re-minted (or discarded), and nothing may be served under it.
	errMQTTTrackerRetired = errors.New("session incarnation retired")

	// errMQTTTrackerNotDrained reports a retirement permit requested while an owner
	// remains. It is a programming-error guard, not a runtime disposition: every
	// caller drains (or promotes) before minting.
	errMQTTTrackerNotDrained = errors.New("incarnation tracker not drained")
)

// mqttIncarnationTracker is the completion-ownership ledger of ONE session
// incarnation. Identity is this object, captured by each owner at acquisition, so
// a late release always lands on the incarnation that admitted it and can never
// decrement the fresh incarnation's count. Every state, count and channel
// operation runs under mu.
//
// The channels are PER CLOSE CYCLE: reopen() recreates all three, so a waiter that
// captured a previous cycle's channel observes that cycle's resolution and
// re-evaluates — a closed channel from an aborted transition can never satisfy a
// later transition's drain (plan round 1, finding 1).
type mqttIncarnationTracker struct {
	mu    sync.Mutex
	state uint8
	// owners counts every live lease, promoted ones included; promoted counts the
	// self-retiring callers' own leases, which the drain must not wait on. The
	// drain predicate is owners-promoted == 0: a sole promoted owner reads (1,1)
	// and drains, and the permit consumes both counts (plan round 5, finding 1).
	owners   int
	promoted int
	// closing is closed by close(): in-route bounded waits (the QoS 1/2
	// transmission and phase gates) select on it so retirement is never held
	// behind a 30s gate wait.
	closing chan struct{}
	// drained is closed when owners-promoted reaches 0 while closing.
	drained chan struct{}
	// decided is closed when the transition resolves; reopened carries the value.
	decided  chan struct{}
	reopened bool
	// onReopen, when set, runs (on its own goroutine, after the tracker's lock is released)
	// each time a closing transition is ABORTED and the incarnation re-admits: work a closing
	// tracker refused to start — the custody dispatcher (F-097 WP4) — is re-armed by it.
	// Set once at construction by the session that owns the tracker.
	onReopen func()

	// retirementPending marks a clean-session discard whose drain ceiling fired:
	// the discard is still OWED ([MQTT-3.1.2-6]) and the session is non-resumable
	// until a finisher completes it. Installed and inspected in the same mu
	// section as drained, so the last release and the mark cannot miss each other
	// (plan round 4, finding 2). The value is the finisher scheduled when drained
	// closes (or by the installer itself when it already has).
	retirementPending func()
}

// mqttLease is the CAPABILITY an admitted owner holds — always a pointer, with a
// released bit checked and changed under the SAME tracker mutex that changes
// owners, so a retained copy cannot resurrect ownership after the drain proof
// exists. release is idempotent. Nothing outside this file mints a lease or touches
// the counts.
//
// There is deliberately no way to hand ownership to a QUEUED effect. Scope consult 3
// settled the boundary: what unit 1 owns is LOCAL EXECUTION that can outlive the
// retirement and then observe the re-minted session, mutate the reused object, derive
// a name from the fresh incarnation, or emit the old route's protocol result. A
// transport entry whose subject, reply, headers and body were fully captured while
// the parent lease was held is none of those — its safety is a property of its
// TARGET (incarnation-scoped, per-consumer-unique, an exact sequence, a CAS'd record
// write, a group-record-arbitrated shared incarnation, or a user subject), and those rules are
// authoritative and unchanged. See docs/ARCHITECTURE.md, "The incarnation completion
// lease".
type mqttLease struct {
	tr       *mqttIncarnationTracker
	released bool
}

// mqttRetirementPermit proves the holder observed sess.c == nil and a closed,
// drained tracker inside one critical section. setIncarnationLocked requires one,
// which is what makes property P mechanical rather than asserted. The zero value
// is invalid; only retireIncarnationLocked and the constructor mint one.
type mqttRetirementPermit struct {
	tr *mqttIncarnationTracker
}

func newIncarnationTracker() *mqttIncarnationTracker {
	return &mqttIncarnationTracker{
		state:   trackerOpen,
		closing: make(chan struct{}),
		drained: make(chan struct{}),
		decided: make(chan struct{}),
	}
}

// drainedLocked evaluates the drain predicate and closes the cycle's drained channel
// when it holds. Called by every operation that changes either count while closing,
// and by close() itself.
//
// A release does nothing but CLOSE THAT CHANNEL. It never schedules work: a
// retirement-pending obligation is watched by a worker admitted EAGERLY when the
// obligation was installed, so shutdown cannot pass its WaitGroup wait and then have
// a late release start a teardown behind it.
func (t *mqttIncarnationTracker) drainedLocked() {
	if t.state != trackerClosing || t.owners-t.promoted != 0 {
		return
	}
	select {
	case <-t.drained:
	default:
		close(t.drained)
	}
}

// acquire admits one owner when the tracker is open. It NEVER parks: a closing or
// retired tracker refuses, and the caller decides — the packet gate parks on the
// cycle's decided channel and retries, a delivery wrapper skips, a timer re-arms.
// The returned lease is the capability everything downstream of the admission uses
// to transfer ownership into queued effects.
func (t *mqttIncarnationTracker) acquire() (*mqttLease, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != trackerOpen {
		return nil, false
	}
	t.owners++
	return &mqttLease{tr: t}, true
}

// closingAndDecided returns the CURRENT cycle's closing state and decided channel,
// for a refused acquirer that wants to park until the transition resolves. The
// channel identifies the cycle: after a reopen the returned channel is already
// closed with reopened=true, so a parked caller re-evaluates against the new cycle
// rather than sleeping on a stale one.
func (t *mqttIncarnationTracker) closingAndDecided() (isClosing bool, decided chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == trackerClosing, t.decided
}

// wasReopened reports the resolution the given cycle's decided channel announced.
// Valid only after that channel is closed.
func (t *mqttIncarnationTracker) wasReopened(decided chan struct{}) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	// decided is only replaced by reopen(), which closes the old one first with
	// reopened=true; if the caller's channel is the current one the stored value
	// answers, and if it is stale the answer for that cycle was "reopened".
	if decided != t.decided {
		return true
	}
	return t.reopened
}

// release ends one admitted owner. Idempotent, and REVOKING: a released lease can
// never mint a child again, which is what keeps owners at zero once the drain
// proof exists (plan round 3, finding 1).
func (l *mqttLease) release() {
	t := l.tr
	t.mu.Lock()
	if l.released {
		t.mu.Unlock()
		return
	}
	l.released = true
	t.owners--
	t.drainedLocked()
	t.mu.Unlock()
}

// promote converts a self-retiring caller's own lease into tracker-only promoted
// debt, in ONE mu operation that REVOKES the lease first: released = true (so
// child() refuses from this point), promoted++ — owners is NOT decremented, so the
// drain predicate owners-promoted == 0 reads a sole promoted owner as (1,1) and
// drains (plan round 5, finding 1). The caller's deferred release() is a harmless
// idempotent no-op; retireIncarnationLocked consumes both counts at the mint.
func (t *mqttIncarnationTracker) promote(l *mqttLease) {
	t.mu.Lock()
	if !l.released {
		l.released = true
		t.promoted++
	}
	t.drainedLocked()
	t.mu.Unlock()
}

// close declares retirement intent: open → closing, closing the cycle's closing
// channel so in-route bounded waits cancel promptly. Closing an already-closing
// tracker is a programming error everywhere but the pending-discard JOIN, which
// deliberately does not call close.
func (t *mqttIncarnationTracker) close() {
	t.mu.Lock()
	if t.state != trackerOpen {
		t.mu.Unlock()
		return
	}
	t.state = trackerClosing
	close(t.closing)
	t.drainedLocked()
	t.mu.Unlock()
}

// reopen aborts the transition and re-admits: closing → open, FRESH channels for
// the next cycle, and the old cycle's decided closed with reopened=true so every
// parked acquirer wakes and retries against the open tracker. Owners that were
// admitted during closing (children of still-admitted parents) stay counted; a
// later close waits for them.
func (t *mqttIncarnationTracker) reopen() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != trackerClosing {
		return
	}
	t.state = trackerOpen
	t.reopened = true
	close(t.decided)
	t.closing = make(chan struct{})
	t.drained = make(chan struct{})
	t.decided = make(chan struct{})
	t.reopened = false
	t.retirementPending = nil
	if cb := t.onReopen; cb != nil {
		go cb()
	}
}

// resolveRetired finishes the transition: closing → retired, decided closed with
// reopened=false. Parked packet acquirers wake and refuse; the displaced readLoop
// then closes its connection. Armed by destroy()'s entry defer so EVERY
// destructive outcome — success, superseded, teardown-incomplete, post-reset
// error — resolves parked ingress exactly once (plan round 1, finding 3).
func (t *mqttIncarnationTracker) resolveRetired() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == trackerRetired {
		return
	}
	t.state = trackerRetired
	t.reopened = false
	select {
	case <-t.decided:
	default:
		close(t.decided)
	}
}

// armRetirementPending parks a discard obligation on a tracker whose drain ceiling
// fired, and returns THIS CYCLE's drained channel so the caller's eagerly-admitted
// worker watches the same cycle it armed. The obligation's identity is the pair
// {session object, this tracker}, which is what the claim below verifies: a client ID
// alone would let a finisher act on a REPLACEMENT session installed while it waited.
//
// Returns false when the tracker is no longer closing — the transition resolved while
// the caller decided, so nothing is owed through it.
func (t *mqttIncarnationTracker) armRetirementPending(finisher func()) (<-chan struct{}, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != trackerClosing {
		return nil, false
	}
	t.retirementPending = finisher
	return t.drained, true
}

// takeRetirementPending claims the parked obligation, exactly once. The
// pending-discard finishers — a CONNECT that found the mark, and the
// drain-triggered worker — arbitrate through this under the exclusive client-ID
// gate; the loser finds nothing and returns.
func (t *mqttIncarnationTracker) takeRetirementPending() (func(), bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.retirementPending
	t.retirementPending = nil
	return f, f != nil
}

// isRetirementPending reports whether a parked discard obligation exists. A
// CONNECT meeting the mark completes the discard before resume or clean-start
// proceeds; the mark also makes the session non-resumable.
func (t *mqttIncarnationTracker) isRetirementPending() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.retirementPending != nil
}

// retireIncarnationLocked mints the retirement permit: sess.c must be nil
// (property P) and the current tracker closing with owners-promoted == 0. It
// consumes the promoted debt — the self-retiring caller's own lease dies with the
// incarnation it retires. After the mint no owner can appear: acquire refuses on
// closing, and child() needs an unreleased parent, of which none exists at drain
// zero.
//
// sess.mu must be held.
func (sess *mqttSession) retireIncarnationLocked() (mqttRetirementPermit, error) {
	if sess.c != nil {
		return mqttRetirementPermit{}, errors.New("retiring an incarnation with a client bound")
	}
	t := sess.leases
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != trackerClosing || t.owners-t.promoted != 0 {
		return mqttRetirementPermit{}, errMQTTTrackerNotDrained
	}
	t.owners -= t.promoted
	t.promoted = 0
	return mqttRetirementPermit{tr: t}, nil
}

// waitDrained blocks on the current cycle's drained channel up to the given
// ceiling, holding NO locks. It returns true when the drain completed.
func (t *mqttIncarnationTracker) waitDrained(ceiling <-chan time.Time) bool {
	t.mu.Lock()
	drained := t.drained
	t.mu.Unlock()
	select {
	case <-drained:
		return true
	case <-ceiling:
		// Re-check: the drain may have completed as the ceiling fired.
		select {
		case <-drained:
			return true
		default:
			return false
		}
	}
}

// acquirePacketLease acquires this packet's completion lease on the incarnation
// this connection was bound to — c.mqtt.tracker, captured at the bind, NOT the
// session's current tracker. That is the whole discrimination: after a re-mint the
// displaced connection still names the RETIRED tracker and is refused, while the
// replacement names the fresh one (F-036 unit 1).
//
// While the tracker is CLOSING the caller parks on that cycle's decision rather
// than refusing outright: a transition that reopens — a drain ceiling, or an
// unresolved-state refusal — leaves the incumbent bound and still owed service, and
// a completed one refuses. Parking happens before any mutable session observation,
// so nothing is half-applied across it.
func (c *client) acquirePacketLease() (*mqttLease, bool) {
	t := c.mqtt.tracker
	if t == nil {
		// No session bound yet (CONNECT itself, and the pre-CONNACK window): there is
		// no incarnation to own, and CONNECT is the one route that must not hold a
		// lease on the object it may be about to retire.
		return nil, true
	}
	for {
		if l, ok := t.acquire(); ok {
			return l, true
		}
		isClosing, decided := t.closingAndDecided()
		if !isClosing {
			return nil, false
		}
		<-decided
		if !t.wasReopened(decided) {
			return nil, false
		}
	}
}

// mqttRetiredBinding is what a bound incarnation's retirement CAPTURES before the
// detach, so no disposition arm afterwards has to rediscover the displaced client
// through sess.c — which is exactly what the fourth attempt showed strands it: with
// the client nilled early, disposeForeignStateLocked's `stale := sess.c` yields nil
// and the connection is never closed, never notified, its Will never discharged,
// no disconnect stamped and no expiry arranged.
//
// It carries IDENTITY, not policy. The Session Expiry Interval is deliberately
// absent: it is connection-owned and stays mutable, so it is read under the lock
// that consumes the Will (see mqttWillFence, which makes the same distinction).
type mqttRetiredBinding struct {
	// client is the displaced connection, captured under sess.mu with the detach.
	client *client
	// action is the parked deferred-disconnect obligation, TAKEN atomically with
	// the detach: it can hold the only copy of the Will together with the deadline,
	// generation, sequence and epoch it was parked with, so every arm consumes it
	// exactly once rather than leaving it on an object about to be re-minted.
	action *mqttDeferredDisconnect
	// fence is the delayed-Will baseline of the RETIRING incarnation.
	fence *mqttWillFence
	// permit authorizes the re-mint. Minted in the same critical section as the
	// detach, over a closed and drained tracker.
	permit mqttRetirementPermit
}

// retireBoundIncarnation performs the retirement of an incarnation that still has a
// client bound — the one call site property P was violated at (the CONNECT
// clean-start arm). It closes admission, drains the displaced incarnation's owners
// under the ceiling, captures the retired-binding capsule, runs the non-mutating
// preparation, detaches, and mints the permit.
//
// On EVERY error arm nothing has been mutated and the incumbent is left bound: the
// tracker is reopened, so its parked routes resume serving it, and the caller
// refuses the contending CONNECT. That is the disposition the fourth attempt could
// not supply — a timeout that neither races the outstanding operation nor loses the
// displaced client's close, notification, Will, disconnect stamp or expiry.
//
// The caller holds the client-ID gate and no session lock, no record fence and no
// QoS fence.
func (asm *mqttAccountSessionManager) retireBoundIncarnation(sess *mqttSession) (
	*mqttRetiredBinding, *mqttPreparedClear, error) {

	return asm.retireBoundIncarnationChecked(sess, nil)
}

// retireBoundIncarnationChecked is retireBoundIncarnation with a PRE-DETACH proof.
//
// The callback runs at the one point where a caller's own licence can still be
// revalidated and still be abandoned safely: after prepareClear has opened the record
// transaction, and BEFORE the detach. A caller that checked afterwards would be checking
// nothing — sess.c is nil and the permit is minted by then, which is what makes the
// obvious ordering (retire, then verify) impossible rather than merely weak.
//
// A callback returning an error is one more PRE-MUTATION refusal arm, indistinguishable
// in disposition from the entry refusal or the drain ceiling: the transaction is aborted,
// the tracker reopened, and the incumbent left bound with its Will, socket, expiry and
// disconnect state untouched.
//
// The callback's own obligations, because they cannot be delegated to it:
//
//   - Read every in-memory fact under sess.mu; an OPEN record transaction does not leave
//     that mutex held, so an open transaction is not a substitute for taking it.
//   - Do the DURABLE reads with neither asm.mu nor sess.mu held, then take asm.mu and
//     nest sess.mu for the final in-memory identity check — that lock ORDER is the
//     established one, and two facts read under two separate acquisitions are not one
//     observation.
//
// "Nothing mutated" on the refusal arms is a claim about PROTOCOL state, not a literal
// one: closing and reopening the tracker replaces its channels, and opening then aborting
// a transaction advances internal bookkeeping. Neither is durable and neither is
// observable to a client.
//
// The caller holds the client-ID gate and no session lock, no record fence and no QoS
// fence.
func (asm *mqttAccountSessionManager) retireBoundIncarnationChecked(sess *mqttSession,
	proof func(*mqttSession, *mqttPreparedClear) error) (*mqttRetiredBinding, *mqttPreparedClear, error) {

	sess.mu.Lock()
	t := sess.leases
	sess.mu.Unlock()

	// 1. CLOSE ADMISSION. From here the displaced connection's next packet parks,
	//    its in-route bounded waits cancel, and no new owner is admitted.
	t.close()

	// 2. DRAIN, holding nothing.
	ceiling := time.NewTimer(mqttIncarnationDrainTimeout)
	drained := t.waitDrained(ceiling.C)
	ceiling.Stop()
	if !drained {
		t.reopen()
		return nil, nil, errMQTTClearDrainTimeout
	}

	// 3. CAPTURE, after the drain: a DISCONNECT that was already admitted has by now
	//    finished, so the Will and the parked action this reads are the current ones
	//    rather than a snapshot the incumbent went on to change.
	fence := sess.mqttCaptureWillFence()

	// 4. PREPARE — the entry refusal, the record fence, the in-fence refusal. Still
	//    nothing mutated, so a refusal returns the incumbent untouched.
	p, err := sess.prepareClear()
	if err != nil {
		t.reopen()
		return nil, nil, err
	}

	// 4a. THE CALLER'S PRE-DETACH PROOF, with the record transaction open and the
	//     tracker closed and drained — the only point where a licence can still be
	//     revalidated against state nothing can now move, and still be abandoned with
	//     the incumbent intact. Guarded against an abnormal exit HERE rather than in the
	//     caller: the caller has not received p yet, so a panic or runtime.Goexit
	//     escaping the callback cannot be cleaned up by anything but this frame.
	if proof != nil {
		if perr := asm.runRetirementProof(sess, t, p, proof); perr != nil {
			return nil, nil, perr
		}
	}

	// 5. DETACH and MINT, one critical section. The action is taken WITH the detach
	//    so no concurrent disposal can observe a half-prepared object and claim the
	//    same Will, and the permit is validated against the still-closing, still
	//    drained tracker.
	sess.mu.Lock()
	capsule := &mqttRetiredBinding{client: sess.c, fence: fence}
	capsule.action = sess.takeDeferredDisconnectLocked()
	// The detached connection stays the AUTHORITY for its own outstanding send-hold
	// debts across the re-mint: it is closed on another goroutine, so its write can
	// outlive this discard and the identifier is still its (F-089, F-087 WP5).
	if sess.c != nil {
		sess.retiringOwnerCID = sess.c.cid
	}
	sess.c = nil
	permit, perr := sess.retireIncarnationLocked()
	if perr != nil {
		// Put the incumbent back exactly as it was: this arm has mutated the object,
		// so it is the one place that restores rather than merely reopening.
		sess.c = capsule.client
		sess.deferredDisconnect = capsule.action
		sess.retiringOwnerCID = 0
		sess.mu.Unlock()
		p.abort()
		t.reopen()
		return nil, nil, perr
	}
	capsule.permit = permit
	sess.mu.Unlock()
	return capsule, p, nil
}

// runRetirementProof invokes a pre-detach proof under an abnormal-exit guard.
//
// The guard is here and not in the caller for a reason that is easy to get wrong: at this
// point retireBoundIncarnationChecked has not returned the prepared clear, so a panic or
// runtime.Goexit escaping the callback would leave the record transaction open and the
// tracker closed with NOBODY holding a reference to either — a session that refuses every
// write and every packet for the rest of the process's life. The deferred arm therefore
// abandons the transaction (never the void-verdict handoff form, which is not this
// frame's to run) and reopens the tracker, then lets the exit continue.
//
// The caller holds the client-ID gate.
func (asm *mqttAccountSessionManager) runRetirementProof(sess *mqttSession,
	t *mqttIncarnationTracker, p *mqttPreparedClear, proof func(*mqttSession, *mqttPreparedClear) error) (err error) {

	settled := false
	defer func() {
		if settled {
			return
		}
		// Abnormal exit: unwind exactly what this frame established, in the reverse
		// order it was established.
		p.abort()
		t.reopen()
	}()
	err = proof(sess, p)
	settled = true
	if err != nil {
		p.abort()
		t.reopen()
		return err
	}
	return nil
}

// disposeCaptured is disposeForeignStateFenced for a caller that has ALREADY
// detached the displaced client and holds it in a capsule (F-036 unit 1). It
// performs no identity check and no detach — both already happened, atomically,
// inside retireBoundIncarnation — and it sources the displaced client and the
// parked action from the capsule rather than from the session, which by now names
// a fresh incarnation and a nil client.
//
// The Will disposition is the same rule the fenced form applies, with the same
// F-084 ordering: a licensed disposal (a successor held in hand) discards, a parked
// action outranks a live Will because parking is what MOVED the Will off the
// connection, and otherwise a live connection's Will is consumed.
//
// The caller holds the client-ID gate; sess.mu is not held.
func (asm *mqttAccountSessionManager) disposeCaptured(sess *mqttSession,
	capsule *mqttRetiredBinding, licensed bool) mqttForeignDisposition {

	sess.mu.Lock()
	// The uncertainty state belongs to the incarnation being retired; the fresh one
	// starts clean. Mirrors disposeForeignStateFenced, whose clearing this replaces
	// on the retirement path.
	sess.seqUncertain, sess.pendingWrite = false, nil
	sess.mu.Unlock()

	asm.removeSessionIfCurrent(sess)

	d := mqttForeignDisposition{
		applied: true,
		stale:   capsule.client,
		action:  capsule.action,
		fence:   capsule.fence,
	}
	switch {
	case licensed && capsule.client != nil:
		d.will = mqttWillActionSettleOnTakeover
	case capsule.action != nil:
		d.will = mqttWillActionDischargeDeferred
	case capsule.client != nil:
		d.will = mqttWillActionConsumeLive
	}
	return d
}

// leasedHandler wraps a session delivery callback in its incarnation's completion
// ownership (F-036 unit 1). The tracker is CAPTURED AT INSTALL, so a callback that
// is dispatched after its incarnation was retired acquires the retired tracker and
// is refused — it cannot reach the reused object's fresh state, which is the class
// that defeated the second, third and fourth attempts: an ordinary QoS 1/2 delivery
// passing its own `sess.c == cc` check and then lazily RECREATING a map clear()
// nilled (srcAckOwed, refusedCarriers, voidedRegNonces, deferred).
//
// A refusal SKIPS WITHOUT ACKING, so JetStream re-offers the delivery to whoever
// owns the consumer next, or the teardown deletes it. It does not ack, does not
// answer, and does not touch session state — except one bookkeeping write, and only
// while the tracker is still CLOSING:
//
// recordWithheld asks for the withheld-delivery record a QoS 1/2 skip owes, which
// keeps the eventual redelivery a FIRST transmission for DUP purposes. It is taken
// only after revalidating, under sess.mu, that the captured tracker is still this
// session's current tracker and still closing — so the write lands on the retiring
// object and before any re-mint. On a RETIRED tracker it is skipped entirely:
// recordWithheldDeliveryLocked deliberately admits sess.c == nil and recreates
// sess.deferred, so recording there would be exactly the stale write this boundary
// exists to prevent (plan round 6, finding 1).
func (sess *mqttSession) leasedHandler(h msgHandler, recordWithheld bool) msgHandler {
	tr := sess.leases
	return func(sub *subscription, pc *client, acc *Account, subject, reply string, msg []byte) {
		l, ok := tr.acquire()
		if !ok {
			if !recordWithheld {
				return
			}
			sess.mu.Lock()
			// Both conditions, in the one critical section that can settle them: the
			// tracker must still be this session's, and still be closing.
			if sess.leases == tr {
				if isClosing, _ := tr.closingAndDecided(); isClosing {
					sess.recordWithheldDeliveryLocked(sub.client, sub, reply)
				}
			}
			sess.mu.Unlock()
			return
		}
		defer l.release()
		h(sub, pc, acc, subject, reply, msg)
	}
}

// retirementPendingArmed reports whether a discard obligation is still parked on this
// session's CURRENT incarnation. The drain-triggered finisher polls it so it stops as
// soon as another claimant — a CONNECT that found the mark — consumed the obligation.
func (sess *mqttSession) retirementPendingArmed() bool {
	sess.mu.Lock()
	tr := sess.leases
	sess.mu.Unlock()
	return tr.isRetirementPending()
}

// closingSignal is the CURRENT cycle's closing channel, for an in-route bounded wait
// that must cancel when its incarnation starts retiring. A nil tracker yields nil,
// which in a select is simply an arm that never fires.
func (t *mqttIncarnationTracker) closingSignal() <-chan struct{} {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closing
}
