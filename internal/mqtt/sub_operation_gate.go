// Copyright 2025 The NATS Authors
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
	"sync/atomic"
	"time"
)

// Withholding a live SUBSCRIBE's deliveries until its shared interest is arbitrated.
//
// THE FINDING (F-078). A live SUBSCRIBE that installs or revives shared group delivery
// interest serves under a membership it never revalidated. The consequence is F-077's, and
// "serves" includes ACKNOWLEDGING: the QoS 1/2 callback acks and drops on a reserved-subject
// violation, on a subscribe deny, on No Local suppression, and delivers-and-acks a shared
// QoS 0 member. Each destroys a group message for the members that legitimately hold the
// membership.
//
// F-077 closed the CONNECT/resume path with one classified record write. That path needs no
// transmission gate because the RESTORE gate already withholds everything until the CONNACK.
// A live connection has no such window — its restore gate is already open — so the ownership
// write needs a gate of its own, scoped to the packet that owes it.
//
// WHY NOT A PER-SUBSCRIPTION FLAG. A shared QoS 2 SUBSCRIBE provisions the PUBREL and affine
// subscriptions BEFORE processSharedJSConsumer, and ensurePubRelConsumerSubscription installs
// its delivery subscription with a bare c.processSub whose NATS handler goes live BEFORE
// processSub returns. There is no subscription to hang a flag on. That is F-050's first
// refutation and it applies here unchanged; do not re-propose an mqttSub field.
//
// WHY NOT THE RESTORE GATE'S SHAPE. Two differences are load-bearing:
//
//   - The restore gate may be left INSTALLED after a refusal, because every accepted CONNECT
//     unconditionally replaces it. A packet gate is re-armed only by a later effectful shared
//     SUBSCRIBE, so an abandoned one would outlive its packet AND its connection. This gate is
//     therefore scoped to the CONNECTION that armed it and to a per-session GENERATION: a wait
//     whose connection is not the gate's owner reads Open, which makes that leak unreachable by
//     construction rather than by discipline.
//   - The restore gate stores `opened` and only then wins the `resolved` CAS. Its two terminal
//     transitions are exclusive by construction, so that ordering is safe there. This gate has
//     more terminal paths, so a losing opener could otherwise flip a withheld verdict to open.
//     It uses ONE CAS-controlled terminal state instead, and the channel close follows the
//     winning CAS — which is also what keeps a double close, a broker PANIC, unreachable.
//
// See docs/ARCHITECTURE.md, "A live SUBSCRIBE arbitrates its shared interest before it serves".

// mqttSubArbitration is what the live SUBSCRIBE's arbitration tail did that its CALLER must
// know about, and it exists because a bare error cannot express it.
//
// The readLoop has three distinct behaviours and `error` only distinguishes two: a non-nil
// error produces the v5 disconnect footer, a nil one sends the SUBACK. The third — this
// connection has already been notified and closed, so say nothing more — has no encoding at
// all, and reaching for the footer there would put it in a race with the displacer for the
// REASON the client is left with. The connection's one DISCONNECT claim refuses whichever
// attempt is second, so the cost of speaking twice is a WRONG reason, not a second packet.
// mqttGateDispatchOnRejection solves the identical problem one packet earlier by returning
// (false, nil) and having mqttParse return nil; this is the same shape.
type mqttSubArbitration struct {
	// disposed: the tail has notified this connection and closed it. mqttParse must
	// return nil — no SUBACK, and no footer.
	disposed bool
	// settledOwnAmbiguity: the tail resolved the ambiguity its OWN ownership proof
	// created, under the ownership gate it was already holding. The readLoop must then
	// NOT call mqttResolveAfterPacket: a second attempt is at best a no-op, and at worst
	// hands the generic resolver — whose forked arm DISPOSES — a session the live matrix
	// says must not be disposed. Set on every arm the tail took, including the one that
	// classified nothing, because in that case the tail's own verdict is "nothing
	// classified" and the UNBOUND machinery takes it after the close.
	settledOwnAmbiguity bool
}

// mqttFiltersCarryShared reports whether this packet has any shared filter at all.
//
// The operation gate is armed only for such a packet. A purely plain SUBSCRIBE installs
// nothing on a group's behalf and can acknowledge nothing for other members, so gating it
// would withhold its own traffic to protect nobody.
//
// Read from the PARSED filters before anything is installed, which is what lets the arm
// happen before ensurePubRelConsumerSubscription. A filter already marked failed at parse
// time still counts: it costs only a gate that opens on the quiet path, and excluding it
// would make the arming decision depend on per-filter outcomes that are not known yet.
func mqttFiltersCarryShared(filters []*mqttFilter) bool {
	for _, f := range filters {
		if f != nil && f.shared {
			return true
		}
	}
	return false
}

// mqttSubPacketFacts is what ONE processSubs invocation did, aggregated on its caller's
// stack and returned to it. Never a field on mqttSession: a fact parked there outlives the
// operation that raised it and would govern the next one.
type mqttSubPacketFacts struct {
	// interestRestored is F-077's arbitration trigger, inherited unchanged: this packet
	// installed, repaired, adopted or REFRESHED shared delivery interest. Every one of
	// those re-authorises delivery under a membership the operation has not revalidated,
	// which is why the trigger is "interest was touched" rather than "a durable was
	// created".
	interestRestored bool
	// recordCASRejected is true when a session-record write in this packet was rejected by
	// its PER-SUBJECT CAS.
	//
	// It exists because that failure cannot be recovered from the returned error, and a
	// rule written against `err` alone reproduces the defect it is meant to close. The
	// early per-filter membership save converts a definite rejection into a per-filter
	// SUBACK code and CONTINUES, so a record that demonstrably MOVED leaves both the
	// trigger and the returned error empty — and the packet would then take the "quiet,
	// nothing owed" path, open the operation gate and SUBACK, while asynchronous delivery
	// callbacks immediately serve the session's SURVIVING shared subscriptions under a
	// membership this broker can no longer speak for. The rejection marker does arm, but
	// mqttGateDispatchOnRejection only settles it before the NEXT inbound packet, which is
	// far too late for a callback already running.
	//
	// A CAS REJECTION, AND NOTHING ELSE. The predicate was briefly "any failed record
	// write", and that was wrong in a way only the full suite could show: it converted an
	// ordinary per-filter failure — an affine provisioning save that could not persist —
	// into a connection-level refusal, breaking contracts owned by the affine items rather
	// than by F-078. The distinction is evidential:
	//
	//   - a CAS rejection means the expected-sequence write was REFUSED, which is positive
	//     evidence that the subject moved;
	//   - a definite local failure means only that this write did not land. It moved
	//     nothing and re-authorised nothing;
	//   - an ambiguous write is our OWN bytes maybe-landing. If they landed they landed
	//     under our CAS at our sequence, which is evidence we owned the record, not that
	//     someone else does — and `seqUncertain` refuses every later save until the
	//     ordinary machinery settles it.
	//
	// Read from mqttSessSaveDetail.rawErr, never from the outward error: persistRecord
	// formats the store error with %v and destroys its NATS identity. And never from
	// saveDefinitelyFailed, which deliberately merges a CAS rejection with a local failure,
	// a refused transaction, and a refusal caused by an earlier uncertainty.
	//
	// Set, never cleared, and preserved across every per-filter `continue`.
	//
	// ALL FOUR record writes reachable from a SUBSCRIBE packet feed it, and each needed its
	// own classification because none of them reports one by default:
	//
	//   - the early per-filter membership save, through updateWithDetail;
	//   - the affine provisioning save, which now returns its own CAS verdict rather than
	//     the bare error covering four operations that touch no record at all;
	//   - the affine-unwind save, through saveClassifiedDetail;
	//   - the void-register store, through the classified siblings of BOTH chains that
	//     reach it — secureRegCancellationsClassified and
	//     deleteInflightRegistersClassified, the latter also via
	//     execDeferredConsumerDeleteClassified.
	//
	// The void-register route is the subtle one, and it is a live hole without this: a
	// mixed packet can arm the gate through a failed shared filter, downgrade a plain
	// consumer SUCCESSFULLY, and then meet a competing remote write during that late
	// record write. With no trigger committed and the rejection swallowed by a bare bool,
	// the packet would look quiet, the gate would open, and the session's SURVIVING shared
	// subscriptions would deliver and acknowledge under a membership this broker can no
	// longer speak for.
	recordCASRejected bool
}

// ownershipQuestionRaised reports whether this packet must prove it still owns the
// membership before anything it touched may serve.
//
// The rule is deliberately one predicate rather than two: the gate opens without a proof
// ONLY for a packet that committed no trigger AND none of whose record writes was rejected
// by its per-subject CAS. "Every shared filter failed" is NOT a safe class on its own — see
// recordCASRejected.
//
// A LOCAL or AMBIGUOUS write failure alone does NOT raise the question, and an earlier
// revision of this comment claimed the opposite. Neither is evidence about ownership: a
// local failure stored nothing and moved nothing, and an ambiguity is this connection's own
// bytes maybe-landing, which if they landed landed under its own CAS. Treating either as
// evidence converted an ordinary per-filter failure into a connection-level refusal and
// broke 1352 tests in the ported suite — a contract the affine paths own, not this one.
//
// AN AMBIGUITY DOES NOT BLOCK THE QUIET BRANCH EITHER, and a review round read that as a
// defect. The reasoning and the residual are at the quiet branch in mqttProcessSubs: a quiet
// packet installed nothing, so its open releases only traffic backed by interest predating
// the packet, and withholding that would break the affine items' per-filter contract.
//
// LIVE SUBSCRIBE ONLY. The CONNECT restore path deliberately keeps F-077's narrower trigger
// (interestRestored alone): a record write that fails there already refuses the CONNECT —
// through the returned error, or through mqttFirstUnrestoredFilter for the arm that converts
// it to a per-filter code — so nothing is served meanwhile and the wider predicate would only
// add a write to a connection that is already being torn down. The live path has no such
// backstop, which is the whole reason this predicate exists.
func (f mqttSubPacketFacts) ownershipQuestionRaised() bool {
	return f.interestRestored || f.recordCASRejected
}

// mqttSubGateState is the gate's terminal state, and the ONLY thing a terminal transition
// writes. TimedOut is deliberately absent: a timeout is a property of one waiter's bounded
// wait, not of the gate, and recording it here would let one waiter's deadline decide
// another's.
type mqttSubGateState int32

const (
	// mqttSubGatePending: the packet has not yet resolved its ownership question.
	mqttSubGatePending mqttSubGateState = iota
	// mqttSubGateOpen: a landed CAS proved this broker still owns the membership, or the
	// packet was provenly quiet and owed no write at all. Transmission is allowed.
	mqttSubGateOpen
	// mqttSubGateWithheld: the packet resolved WITHOUT establishing ownership. Nothing it
	// installed may transmit or acknowledge, and the connection is closing.
	mqttSubGateWithheld
)

// mqttSubGate is one shared-SUBSCRIBE packet's "nothing this packet touched may be
// transmitted yet" verdict.
//
// owner and gen together are the scope. owner keeps an abandoned verdict from crossing into
// the next CONNECT's connection; gen keeps a waiter captured on one packet's gate from
// escaping the next packet's, which a bare pointer comparison cannot do because a waiter that
// reloads after its own gate opened would otherwise never look again.
//
// THE BOUNDARY IS THE WAITER'S FINAL READ, not the send. A gate published before that read is
// observed by it; a waiter already past it is the documented final-check residual, which the
// generation does not reach and does not claim to. Stated because the paired test asserted the
// stronger thing and was flaky in consequence — see mqttTestGateAboutToWaitV.
type mqttSubGate struct {
	done  chan struct{}
	state atomic.Int32
	// owner is the CONNECTION ID whose readLoop armed this gate. A wait from any other
	// connection treats it as Open: the gate says nothing about a connection it does not
	// belong to.
	//
	// A SCALAR, deliberately, not a *client. Nothing clears sess.subGate — a CONNECT
	// restore does not replace it either, unlike the restore gate — so a strong pointer
	// here would let a persistent session that disconnected after a shared SUBSCRIBE root
	// its own dead connection indefinitely: the client's MQTT state, its parser buffers,
	// its socket and its NATS connection, until another live shared SUBSCRIBE happened to
	// replace the gate or the session expired. A never-expiring session would retain one
	// such client each. The identity is all this gate needs, and c.cid is immutable for a
	// connection's life, so the scalar is strictly better than clearing the pointer on
	// teardown: it cannot be forgotten on a path nobody thought about.
	owner uint64
	// gen is monotonic per session, minted at arm time.
	gen uint64
}

// armSubGate installs a fresh verdict for one SUBSCRIBE packet and returns it.
//
// Called from mqttProcessSubs BEFORE processSubs, and therefore before
// ensurePubRelConsumerSubscription and ensureQ2AffineConsumerSubscription install anything.
// That is the whole point: at the instant a gate is armed, the packet has installed NO
// interest yet, which is what bounds the residual in docs/CONFORMANCE.md to deliveries whose
// interest predates the packet.
//
// The caller must hold the returned pointer and resolve THAT INSTANCE. Resolving whatever
// sess.subGate currently holds would let a delayed defer from generation N resolve
// generation N+1's gate.
//
// No lock held on entry: the pointer is published atomically, and the session's ownership
// gate serialises this against every other packet for the session.
func (sess *mqttSession) armSubGate(c *client) *mqttSubGate {
	g := &mqttSubGate{
		done:  make(chan struct{}),
		owner: c.cid,
		gen:   sess.subGateGen.Add(1),
	}
	sess.subGate.Store(g)
	return g
}

// resolveSubGate performs this gate's ONE terminal transition, on the exact instance the
// packet armed.
//
// Idempotent, and it has to be: mqttProcessSubs installs it as a defer so that every error
// path AND an abnormal unwind resolves the gate — a test hook calling t.Fatal unwinds with
// runtime.Goexit, which no explicit return statement covers.
//
// The CAS is what makes a second call a no-op rather than a second close(). A double close
// panics the broker.
//
// No lock held on entry.
func (g *mqttSubGate) resolve(state mqttSubGateState) {
	if g == nil {
		return
	}
	if g.state.CompareAndSwap(int32(mqttSubGatePending), int32(state)) {
		close(g.done)
	}
}

// subGateFor returns the gate this connection must satisfy, or nil when there is none.
//
// A gate belonging to another connection is NOT this connection's to wait on: the packet that
// armed it is serving a client this delivery has nothing to do with, and its verdict would
// otherwise block a session that legitimately rebound.
func (sess *mqttSession) subGateFor(cc *client) *mqttSubGate {
	g := sess.subGate.Load()
	if g == nil || cc == nil || g.owner != cc.cid {
		return nil
	}
	return g
}

// awaitTransmissionGates is the whole of a delivery callback's dealing with BOTH verdicts:
// the connection-scoped restore gate and the packet-scoped operation gate.
//
// ONE ABSOLUTE DEADLINE covers both, and every re-check. Two sequential
// mqttPhaseGateWaitFor waits would permit nearly twice the intended cap, and the cap exists
// because past it JetStream re-pushes the message anyway — so waiting longer buys nothing and
// a dead packet must not park a delivery goroutine.
//
// THE RELOAD IS THE POINT. Packets are serialized on the readLoop but delivery callbacks are
// not, so a callback can observe generation N open and then find generation N+1 armed. Each
// time round the loop it re-reads the CURRENT gate rather than the one it first captured, and
// it only returns Open once nothing installed for this connection is pending. The deadline is
// checked at the top of every iteration, including iterations whose channel is already
// closed, so generation churn cannot extend the budget.
//
// WHAT IT CANNOT CLOSE, stated rather than implied: a callback that has passed this check and
// is then descheduled before its irreversible send cannot observe a gate armed in between.
// That escape is bounded to interest that PREDATES the packet — arming happens before the
// packet installs anything — so it never covers interest the packet is about to install,
// which is the only thing F-078 authorises. docs/CONFORMANCE.md carries it with a worked
// interleaving. Closing it entirely needs a real callback-admission boundary, which F-050
// already declined to justify and whose cheap form it refuted outright.
//
// Callers must hold NO session lock: the packet's goroutine needs sess.mu to finish, and
// every condition the caller checked before this must be re-evaluated afterwards.
//
// QoS 0 does not call this — it has no redelivery, so parking a NATS delivery goroutine for
// it would buy nothing; it consults transmissionBlocksQoS0 instead.
func (sess *mqttSession) awaitTransmissionGates(cc *client) mqttTransmissionVerdict {
	deadline := time.Now().Add(mqttPhaseGateWaitFor(cc))
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	// RETIREMENT CANCELS THE WAIT. This wait is bounded by the phase-gate cap, which
	// is up to 30 seconds — far past the incarnation drain ceiling — so without this
	// arm a delivery admitted just before a clean-start takeover would hold its lease
	// long enough to make the takeover time out and refuse, repeatedly, for a reason
	// that has nothing to do with the contender. Closing is the incarnation this
	// CONNECTION was admitted under, so the answer is the same one an armed gate
	// gives: WITHHELD, which the caller records without acking, keeping the eventual
	// redelivery a first transmission (F-036 unit 1).
	var closing <-chan struct{}
	if cc != nil && cc.mqtt != nil && cc.mqtt.tracker != nil {
		closing = cc.mqtt.tracker.closingSignal()
	}

	for {
		// Re-read BOTH on every pass. A gate that resolved while we waited on the other
		// one must be seen here, and a gate armed while we waited must be met.
		rg := sess.restoreGate.Load()
		sg := sess.subGateFor(cc)

		restoreOpen := rg == nil || rg.opened.Load()
		restoreResolved := rg != nil && rg.resolved.Load()
		var subState mqttSubGateState
		if sg != nil {
			subState = mqttSubGateState(sg.state.Load())
		} else {
			subState = mqttSubGateOpen
		}

		// A resolved-but-not-open verdict is definite on either gate, and it dominates:
		// the caller must not transmit, and waiting for the other gate would only delay
		// the bookkeeping that verdict owes.
		if (restoreResolved && !restoreOpen) || subState == mqttSubGateWithheld {
			return mqttTransmissionWithheld
		}
		if restoreOpen && subState == mqttSubGateOpen {
			return mqttTransmissionOpen
		}

		// Deadline first, so a channel that is already closed cannot spin the loop past
		// the budget.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return mqttTransmissionTimedOut
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(remaining)

		// Wait on whichever gate is still pending. Waking early on the wrong one is
		// harmless: the loop re-reads both.
		var pending <-chan struct{}
		switch {
		case !restoreOpen && !restoreResolved && rg != nil:
			pending = rg.done
		case subState == mqttSubGatePending && sg != nil:
			pending = sg.done
		default:
			// Neither is pending yet both tests above failed, which means the state
			// moved between reads. Re-read rather than blocking on nothing.
			continue
		}

		mqttTestGateAboutToWaitHook(sess)

		select {
		case <-pending:
		case <-closing:
			return mqttTransmissionWithheld
		case <-timer.C:
			return mqttTransmissionTimedOut
		}
	}
}

// mqttTestGateAboutToWaitV pauses a waiter after it has read the current gates and chosen
// which one to block on, but BEFORE it blocks. That is the only point at which a test can
// deterministically publish a later generation into the window the reload exists to cover.
//
// Both seamless versions of that test are unmeasurable, in opposite directions. Resolving
// generation N and then arming N+1 races the waiter: a CORRECT waiter may wake, reload while
// N is still the current gate, and return Open — and the earlier form of the test called that
// a defect and failed about one run in eight under -count. Arming N+1 first lets a
// single-load waiter read N+1 straight away and pass. Pausing here fixes both: the test
// resolves N and arms N+1 while the waiter is held, so the wait returns at once and the
// reload is the only thing that can produce the right answer.
//
// Keyed on the session so a background goroutine from another fixture cannot enter it.
// Nil in production.
var mqttTestGateAboutToWaitV atomic.Pointer[func(sess *mqttSession)]

func mqttTestGateAboutToWaitHook(sess *mqttSession) {
	if h := mqttTestGateAboutToWaitV.Load(); h != nil {
		(*h)(sess)
	}
}

// subGateBlocksQoS0 reports whether a SHARED QoS 0 delivery must be DROPPED because this
// packet's ownership question is unresolved or resolved against us.
//
// QoS 0 has no redelivery, so there is nothing to wait for and nothing to record: a message
// selected for this member while the gate is closed reaches no member at all. That is the
// restore gate's existing residual widened to the live SUBSCRIBE window, and
// docs/CONFORMANCE.md states it.
//
// It deliberately checks ONLY the operation gate. The restore gate is consulted earlier in
// the callback, before sub.mqtt is touched at all, because a delivery can arrive there with
// no per-subscription metadata attached; `shared` is only knowable after that, which is why
// the two checks sit at different points rather than in one helper.
//
// Only SHARED QoS 0 calls this. A plain QoS 0 subscription cannot acknowledge a stored group
// message, so gating it would drop messages while destroying nothing for other members.
func (sess *mqttSession) subGateBlocksQoS0(cc *client) bool {
	if g := sess.subGateFor(cc); g != nil {
		return mqttSubGateState(g.state.Load()) != mqttSubGateOpen
	}
	return false
}

// mqttConsumeWillUnderGate discharges this connection's Will while the session's ownership
// gate is still held, for a NON-FOREIGN live-arbitration close.
//
// WHY IT CANNOT WAIT FOR THE ORDINARY CLOSE. A record-observation eviction can detach the
// session between this verdict and mqttHandleClosedClient. Once detached, the close takes
// its rebound path and calls mqttPublishOwedWill, which publishes only an ALREADY-OWED
// IMMEDIATE Will — its own comment says "Only an immediate Will is published here" — so a
// DELAYED Will would never reach the durable $MQTT_willq outbox at all.
//
// WHY mqttHandleWill AND NOT AN INLINE PUBLISH. It is the one carrier that handles both
// kinds: it consumes the Will through mqttTakeWillLocked, then publishes an immediate one or
// defers a delayed one onto the outbox. Deciding by kind here would rebuild exactly the
// asymmetry mqttPublishOwedWill has.
//
// WHY A SECOND DISCHARGE IS SAFE. mqttTakeWillLocked consumes the disposition EXACTLY ONCE,
// which mqttHandleWill already relies on ("this runs twice for a graceful DISCONNECT ... and
// only the first invocation may act on it"). So the ordinary close that follows finds the
// Will gone and its own mqttHandleWill is inert. Nothing else about that close becomes a
// no-op: it still detaches, clears or purges, saves the disconnect time and schedules
// expiry.
//
// THE FOREIGN ARM USES THIS TOO, since F-084. It used to discard instead, on the reasoning
// that a takeover owns the Will's fate — but that arm demonstrates no takeover, only that
// the record is not ours, so suppressing there was silent loss. The discard is now reached
// only from a displacement this broker holds both ends of, and only for an armed Will
// (mqttSettleWillOnTakeover).
//
// The ownership gate is held on entry; sess.mu is not.
func (c *client) mqttConsumeWillUnderGate(asm *mqttAccountSessionManager, sess *mqttSession) {
	if c == nil || sess == nil || asm == nil || c.srv == nil {
		return
	}
	sess.mu.Lock()
	sessGen := sess.gen
	originSessionSeq := sess.seq
	sess.mu.Unlock()
	// The acknowledged record sequence is a sufficient baseline: the accepted-CONNECT
	// atomic batch stages {acceptance fact, session record} in that order and commits them
	// together, so this connection's OWN acceptance fact sorts strictly below the record
	// sequence and cannot suppress the Will it is owed. Suppression reads acceptance facts
	// only (mqttAcceptFactQualifies / scanAcceptFacts) and no session record, so an
	// ordinary disconnect save — which creates no fact — cannot move this baseline against
	// us either.
	// nil expiry: read from the CONNECTION, under the same lock that consumes the Will. The
	// interval is connection-owned and stays mutable through an MQTT 5 DISCONNECT override,
	// so a value sampled here could be stale by the time the Will is claimed — and it
	// decides whether the Will is due now or after a delay, and whether that delay is
	// capped. Reading sess.expiryInterval was doubly wrong: a replacement connection may
	// already have overwritten it (see mqttSessionExpiry). F-084.
	c.srv.mqttHandleWill(c, nil, sessGen, originSessionSeq, asm.sessStreamEpoch)
}

// mqttWillFence is the delayed-Will baseline captured BEFORE an operation that resets the
// session object, for a disposal whose Will action runs after it.
//
// It exists for exactly one caller and the reason is specific: the post-reset
// teardown-incomplete arm disposes an object whose clear() has already re-minted its
// generation and set its sequence to 0. Sampling the session there would hand
// enqueueDelayedWill a zero origin sequence, which its validation refuses — so the delay
// would be sacrificed and the Will published immediately, for a session that had merely
// failed to tear itself down. The fence carries the RETIRING incarnation's identity, which
// is the one the Will actually belongs to. F-084.
//
// It carries the IDENTITY only. The Session Expiry Interval is deliberately not part of it:
// that is connection-owned and stays mutable, so it is read under the lock that consumes
// the Will rather than snapshotted here. What clear() destroys is the generation and the
// sequence, and those are what the fence preserves.
type mqttWillFence struct {
	gen string
	seq uint64
}

// mqttCaptureWillFenceLocked samples the fence. Must be called before the reset.
//
// sess.mu must NOT be held.
func (sess *mqttSession) mqttCaptureWillFence() *mqttWillFence {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return &mqttWillFence{gen: sess.gen, seq: sess.seq}
}

// mqttConsumeWillWithFence is mqttConsumeWillUnderGate against an explicit baseline. A nil
// fence means "sample the session now", which is right everywhere the session still carries
// the incarnation the Will belongs to.
//
// The ownership gate is held on entry; sess.mu is not.
func (c *client) mqttConsumeWillWithFence(asm *mqttAccountSessionManager, sess *mqttSession,
	f *mqttWillFence) {

	if f == nil {
		c.mqttConsumeWillUnderGate(asm, sess)
		return
	}
	if c == nil || sess == nil || asm == nil || c.srv == nil {
		return
	}
	c.srv.mqttHandleWill(c, nil, f.gen, f.seq, asm.sessStreamEpoch)
}

// mqttSettleLiveArbitration is the EVENTUAL half of the live SUBSCRIBE's tail: it resolves an
// ambiguity this packet's own ownership proof created, and applies the live outcome matrix to
// the result.
//
// It runs from a defer ordered so that the packet's record transaction is already CLOSED and
// the session's ownership gate is still HELD. Both matter. The resolution takes a record
// fence of its own and the fence cannot be acquired twice on one goroutine, so the packet's
// transaction must be gone first. And nothing else may classify this session's uncertainty
// meanwhile: the periodic pass resolves BOUND uncertain sessions too, and its generic policy
// DISPOSES on a forked verdict — which for a live connection would evict the session and
// consume the parked teardown, losing the Will.
//
// It deliberately does NOT call resolveUncertainNow. That wrapper takes the ownership gate
// through lockSession, which fails fast against a gate it finds held — including its own —
// and would silently skip the disposition entirely.
//
// gateState and tail are the caller's, written here so its own defers publish them.
func (c *client) mqttSettleLiveArbitration(asm *mqttAccountSessionManager, sess *mqttSession,
	arb *mqttSubArbitration, gateState *mqttSubGateState, tail *func()) {

	// Set on EVERY arm below, including the one that classifies nothing: this tail owned
	// the disposition of its own ambiguity either way, so mqttResolveAfterPacket must not
	// run a second attempt.
	arb.settledOwnAmbiguity = true

	tok := sess.beginRecordTx()
	res, rerr := sess.resolveUncertainWriteInTx(tok)
	if rerr != nil {
		c.Debugf("MQTT: could not resolve the ownership write of session %q: %v", sess.id, rerr)
	}

	switch res {
	case mqttUncertainOurs:
		// The proof's exact bytes are durable, so ownership rests on a LANDED CAS —
		// either the original landed and its bytes were recognised, or the byte-identical
		// re-attempt landed. Discharge what the landed settlement owes, exactly as
		// resolveUncertainNow would have.
		if err := sess.endRecordTx(tok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
		sess.dischargeDeferredIfUnbound(c)
		*gateState = mqttSubGateOpen
		c.mqttConsumeWillUnderGate(asm, sess)

	case mqttUncertainForeign:
		// The resolver found a record that is not ours. Same DISPOSITION as the direct arm
		// and for the same reason — this object may not write and must not serve — but the
		// Will is CONSUMED, not discarded: nothing here demonstrated a successor, which is
		// exactly what the forked arm below already says of itself. F-084.
		//
		// Phase 1, then the transaction, then the Will: the consume does JetStream work and
		// the fence cannot be held across it, and the ownership gate is still held, which is
		// what mqttConsumeWillUnderGate requires.
		d := sess.disposeForeignStateLocked(asm, c, false)
		if err := sess.endRecordTx(tok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
		d.applyWill(sess, asm)
		if d.applied && d.stale != nil {
			voc, stale := mqttDisposalForLineage(res == mqttUncertainForked), d.stale
			arb.disposed = true
			*tail = func() {
				stale.mqttNotifyForeignDisposal(sess.id, voc)
				stale.closeConnection(voc.closeReason)
			}
		}

	case mqttUncertainForked:
		// A body CLAIMING our parent: our write was erased by something not demonstrably
		// our successor, and NOBODY was demonstrated to own the session. So this arm does
		// not dispose and does not discard the Will — the generic resolver's forked arm
		// does dispose, and following it here would evict a live session on evidence that
		// establishes no other owner.
		//
		// The uncertainty is SETTLED WITHOUT DISPOSING, which is what keeps the ordinary
		// close on its normal branch instead of parking a teardown that a later generic
		// forked disposal would consume.
		sess.settleForkedWithoutDisposal()
		if err := sess.endRecordTx(tok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
		c.mqttConsumeWillUnderGate(asm, sess)

	default:
		// UNCLASSIFIED, and there are two shapes.
		//
		// (i) An ordinary resolution failure — JetStream unreachable at resolve time —
		// which returns a NON-NIL error. Nothing was classified, so the pending write's
		// effects are retained and the generic machinery keeps it.
		//
		// (ii) Unresolved returned with a NIL error. That uniquely identifies a LANDED
		// settlement whose continuation re-armed uncertainty: resolutionAfterContinuations
		// downgraded an Ours verdict because a continuation created a NEW ambiguity. The
		// settlement already happened — adoption, revision credit and continuations all
		// ran — so this must NOT re-settle, and the newly current pending write is owned by
		// the ordinary uncertainty machinery from here.
		//
		// Either way nothing proved ownership, so the gate stays WITHHELD; and either way
		// this is a non-Foreign close, so the Will is consumed under the gate.
		if err := sess.endRecordTx(tok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
		c.mqttConsumeWillUnderGate(asm, sess)
	}
}

// settleForkedWithoutDisposal retires an ambiguous ownership write that resolved FORKED, on
// a live connection, without evicting the session or touching its Will.
//
// WHY THIS EXISTS. The generic resolver disposes on a forked verdict, and for an unbound
// session that is right. On a live SUBSCRIBE it is not: a forked verdict says our write was
// erased by something not demonstrably our successor and that NOBODY was demonstrated to own
// the session, which licenses telling the client 0x89 "reconnect" — not evicting it. But
// merely DECLINING to dispose is not enough, and this is the trap:
//
//	the resolver's forked arm mints the fork grant and leaves seqUncertain set. The
//	connection then closes; mqttHandleClosedClient takes its UNRESOLVED branch and PARKS the
//	teardown, Will included. The periodic pass reaches the uncertainty branch first — not the
//	rejection branch — classifies forked again, and disposes. disposeForeignStateLocked then
//	consumes the parked teardown, and the Will is silently lost.
//
// So the uncertainty is retired here, which puts the ordinary close back on its normal
// branch. That is truthful rather than convenient: at forked, classifyAttemptReadback has
// PROVEN a sibling body sits above our base, so our attempted bytes will never land and a
// re-attempt would be a CAS at a stale base. The ambiguity really is resolved — as "did not
// land, and superseded".
//
// WHAT IT RETIRES, and each for its own reason:
//
//   - seqUncertain and pendingWrite: the two assignments disposeForeignStateLocked makes,
//     minus the eviction and minus takeDeferredDisconnect.
//   - the retained continuations, DISCARDED and never run. retainOnLanded parks work owed
//     only once those exact bytes are proven durable, and settleLandedAttempt — the Ours
//     path — is the only thing that runs them. Today a forked verdict is always followed by
//     eviction, so the list dies with the object and nobody had to think about it; keeping
//     the object alive would leave stale callbacks to fire during a LATER, unrelated
//     resolution, because commitUncertainWrites drains an unkeyed session-wide list. A
//     SUBSCRIBE really registers these — the affine unwind and the affine provisioning both
//     do.
//   - the void-register revision is left UNCREDITED. Crediting is settleLandedAttempt's act
//     and its premise is durability, which is false here.
//
// WHAT IT DELIBERATELY DOES NOT TOUCH: the sequence, the lineage and the record body. For
// Ours those bytes ARE the record, which is why clearUncertaintyLocked adopts them; at forked
// they demonstrably are not, so the session stays at its acknowledged base while the
// authoritative record remains the sibling's. Any independent rejection marker is likewise
// left armed — this classification did not settle it.
//
// The ownership gate and the record fence are both held on entry.
func (sess *mqttSession) settleForkedWithoutDisposal() {
	// Taken and dropped: their premise — that these exact bytes became durable — is now
	// proven false.
	_ = sess.takeUncertainActions()
	sess.mu.Lock()
	sess.seqUncertain, sess.pendingWrite = false, nil
	sess.mu.Unlock()
}
