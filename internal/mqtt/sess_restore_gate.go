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
)

// The success CONNACK follows a complete restoration, or the CONNECT is refused.
//
// A resuming CONNECT used to acknowledge the session and only THEN restore its
// subscriptions, so every per-filter restore failure was silently dropped: the
// record kept the membership, no queue subscription and no group consumer
// existed, and the client — which is not required to re-subscribe — held a
// Session-Present connection whose subscription delivered nothing (F-050).
//
// The fix moves restoration to the other side of the CONNACK, which makes it one
// more thing that can refuse the CONNECT through the pre-CONNACK abort
// discipline. That reorder needs one new fact, because [MQTT-3.2.0-1] requires
// the CONNACK to be the FIRST packet the client sees and restoration installs
// live delivery interest:
//
//	nothing may be transmitted on this connection until its CONNACK is written.
//
// That is a property of the CONNECTION, not of any subscription, and the two
// reasons are structural rather than stylistic:
//
//   - a per-subscription flag cannot be armed before the subscription exists,
//     and ensurePubRelConsumerSubscription installs its delivery subscription
//     with a bare c.processSub whose NATS handler goes live BEFORE processSub
//     returns — so a queued PUBREL can enter its callback before any
//     per-subscription metadata has been attached at all;
//   - a per-subscription gate that has been RELEASED is indistinguishable from
//     one that never existed, so a refusal cannot express "woken, and still not
//     allowed to transmit" that way.
//
// So the fact lives on the session as one atomically-published verdict that
// every delivery callback consults BEFORE it touches sub.mqtt. The
// per-subscription phaseGate of the resume reconciliation phase is untouched and
// keeps its own meaning; a delivery that wakes from that gate then meets this
// one.
//
// See docs/ARCHITECTURE.md "The restore gate".

// mqttRestoreGate is one CONNECT's "nothing may be transmitted yet" verdict.
//
// done is closed exactly once, by openRestoreGate or abandonRestoreGate, from the
// CONNECT's own goroutine. opened is stored true STRICTLY BEFORE that close and
// only on the success path, so a waiter released by the close is guaranteed to
// read the final value: the store happens-before the close, which happens-before
// the receive.
//
// A waiter whose bound expires instead may legitimately observe opened false
// while the CONNECT is still working. That is not a stale read — the delivery
// really has not been allowed out yet — and it is treated as withheld, which is
// the same disposition the reconciliation phase's own gate timeout takes.
//
// resolved is what makes the two terminal transitions safe rather than merely
// unreachable together. They live on different code paths — one after a confirmed
// success CONNACK, one on every refusal — and a channel close that runs twice
// PANICS THE BROKER. Today the paths are exclusive; a CAS is what keeps that from
// being an invariant a later caller can break silently, and it is the same posture
// mqttReleasePhaseGateLocked takes with its nil check.
type mqttRestoreGate struct {
	done     chan struct{}
	opened   atomic.Bool
	resolved atomic.Bool
}

// armRestoreGate installs a fresh verdict for this CONNECT.
//
// Called BEFORE the first subscription the CONNECT installs — before the resume
// reconciliation phase, not merely before subscription restoration. The phase's
// own subscriptions are installed closed and behind their own gate, so arming
// this early is defence in depth rather than a behaviour a test can distinguish;
// what it buys is that no subscription installed anywhere inside the CONNECT can
// be outside the verdict.
//
// UNCONDITIONAL on every accepted CONNECT, including a clean start and a resume
// whose record names no filters, because an abandoned verdict is deliberately LEFT
// INSTALLED (see abandonRestoreGate) and the next arm is the only thing that
// replaces it.
//
// That unconditionality is DEFENCE IN DEPTH rather than a proved behaviour, and it
// is recorded as such because an earlier revision claimed more. A mutation making
// the arm conditional on the record naming at least one filter SURVIVES: to strand
// a session you would need an abandoned verdict on a resident object followed by a
// successful CONNECT with no filters on that same object, and a refusal leaves the
// record's filters exactly as it found them, so the next CONNECT always has some.
// The reachable-today cost of a conditional arm is therefore nil; it is kept
// unconditional so no future path has to re-derive that argument.
//
// No lock held on entry: the pointer is published atomically, and asm.sessLocked
// serialises this against every other CONNECT for the session.
func (sess *mqttSession) armRestoreGate() {
	g := &mqttRestoreGate{done: make(chan struct{})}
	sess.restoreGate.Store(g)
}

// openRestoreGate releases every delivery this CONNECT withheld, for transmission.
//
// Called once, immediately after a CONFIRMED success CONNACK — never before it,
// which is the whole point. The store precedes the close so a woken waiter reads
// the verdict rather than racing it.
//
// No lock held on entry.
func (sess *mqttSession) openRestoreGate() {
	g := sess.restoreGate.Load()
	if g == nil {
		return
	}
	g.opened.Store(true)
	if g.resolved.CompareAndSwap(false, true) {
		close(g.done)
	}
}

// abandonRestoreGate wakes every withheld delivery WITHOUT allowing any of them
// out, because this CONNECT is being refused.
//
// The verdict is not cleared, and that is the load-bearing part: a delivery that
// arrives AFTER the abandon and before the client detaches must still be
// blocked, and mqttAbortAcceptedConnect deliberately leaves c.mqtt.sess in place,
// so sess.c still names this connection. A cleared pointer would read as "no
// restore in progress" and transmit on a connection that was never acknowledged.
//
// Waking them matters as much as blocking them: a parked delivery would otherwise
// wait out its whole bound, find the session detached, and no longer be able to
// record itself as never transmitted — so its redelivery would reach the client
// marked DUP.
//
// No lock held on entry.
func (sess *mqttSession) abandonRestoreGate() {
	g := sess.restoreGate.Load()
	if g == nil {
		return
	}
	if g.resolved.CompareAndSwap(false, true) {
		close(g.done)
	}
}

// mqttTransmissionVerdict is what a delivery callback learns by consulting the
// verdict.
type mqttTransmissionVerdict int

const (
	// mqttTransmissionOpen: no restore is in progress, or this one succeeded.
	// Transmission is allowed.
	mqttTransmissionOpen mqttTransmissionVerdict = iota
	// mqttTransmissionWithheld: the restore RESOLVED without opening — the CONNECT
	// was refused. The caller must not send the delivery, and — where its message
	// class carries DUP — must record it as withheld so the attempt that finally
	// goes out is still a first transmission [MQTT-3.3.1-1].
	mqttTransmissionWithheld
	// mqttTransmissionTimedOut: the wait's bound expired while the restore was still
	// working, so nothing is decided yet.
	//
	// Distinct from Withheld because the two owe JetStream different things. The
	// bound is min(AckWait, mqttPhaseGateWaitMax), and the comment that it is "the
	// deadline past which JetStream re-pushes the message anyway" holds only while
	// AckWait is inside the cap. With a configured AckWait ABOVE it, the wait ends
	// long before any redelivery — so a class that carries no withheld bookkeeping
	// must NAK, or an owed PUBREL or affine copy sits unsent for the rest of that
	// AckWait even though the resume went on to SUCCEED. Found by holistic round 6.
	mqttTransmissionTimedOut
)

// A delivery callback does NOT consult this gate on its own. It calls
// awaitTransmissionGates, which waits on this verdict and the live SUBSCRIBE packet's
// together under one absolute deadline (F-078). Two sequential waits would permit nearly
// twice the intended cap, and a callback that satisfied only one of them could transmit
// under interest the other had not authorised.
//
// The single-gate wait this file used to export was deleted when that combined wait landed:
// it had no production caller left, and two mechanisms stating "may this connection transmit
// yet" is exactly the redundancy that grows a seam between them. See sub_operation_gate.go.

// mqttFirstUnrestoredFilter reports the first filter whose restoration reported a
// failure, or nil when none did.
//
// processSubs already records every per-filter outcome in the filter itself — it
// has to, because on a SUBSCRIBE those bytes become the SUBACK — so this reads a
// value the restore path was always required to set rather than adding
// bookkeeping. On the CONNECT restore path there is no SUBACK to carry it, which
// is why nothing read it before.
func mqttFirstUnrestoredFilter(filters []*mqttFilter) *mqttFilter {
	for _, f := range filters {
		if f != nil && f.qos >= mqttSubAckFailure {
			return f
		}
	}
	return nil
}

// restoreExitFail is the per-exit failure seam for the CONNECT restore path.
//
// It returns nil on a SUBSCRIBE and whenever no fixture installed it, and it is
// consulted only AFTER the real call it stands beside has succeeded — so a test
// proves the production route ran and then failed it, rather than replacing it.
// See the field's comment for why the injection is per exit.
func (as *mqttAccountSessionManager) restoreExitFail(fromSubProto bool, exit, sid string) error {
	if fromSubProto {
		return nil
	}
	as.mu.RLock()
	h := as.testRestoreFilterFail
	as.mu.RUnlock()
	if h == nil {
		return nil
	}
	return h(exit, sid)
}

// mqttSharedCharge is one group durable and the MaxAckPending a single
// processSharedJSConsumer call added to tmaxack for it.
type mqttSharedCharge struct {
	durable string
	amt     int
}

func appendSharedCharge(dst []mqttSharedCharge, durable string, amt int) []mqttSharedCharge {
	if amt == 0 || durable == _EMPTY_ {
		return dst
	}
	return append(dst, mqttSharedCharge{durable: durable, amt: amt})
}

// mqttSharedNamesDurable is mqttSharedNames' durable alone.
func mqttSharedNamesDurable(group, subject string) string {
	_, _, durable := mqttSharedNames(group, subject)
	return durable
}

// refundSharedCharges gives back exactly what one filter's group-consumer joins
// added to tmaxack, and only on the CONNECT restore path.
//
// It exists because that path's per-filter failure no longer disposes of the
// consumer: the CONNECT is refused, so the durable the record names is left alone
// for the next attempt — but the charge is in memory on a session object that
// SURVIVES the refusal, and addJSConsToSess runs only once the whole filter
// succeeds. Without this the charge outlives its config, the next resume sees no
// cons entry, charges again, and tmaxack double-counts until the session ends.
//
// A SUBSCRIBE (fromSubProto) is untouched: it disposes of the consumer through
// releaseSharedConsumer, which refunds as part of the release.
//
// No lock held on entry.
func (sess *mqttSession) refundSharedCharges(fromSubProto bool, charges []mqttSharedCharge) {
	if fromSubProto || len(charges) == 0 {
		return
	}
	sess.mu.Lock()
	for _, ch := range charges {
		if have, ok := sess.sharedCharged[ch.durable]; ok && have == ch.amt {
			delete(sess.sharedCharged, ch.durable)
			sess.tmaxack -= ch.amt
			if sess.tmaxack < 0 {
				sess.tmaxack = 0
			}
		}
	}
	sess.mu.Unlock()
}

// recordWithheldDelivery records that a QoS 1/2 delivery reached its callback and
// was NOT transmitted, so the attempt that finally goes out is a first send even
// though JetStream has counted more than one ([MQTT-3.3.1-1]) — and so a freed
// slot can NAK it forward instead of waiting out AckWait.
//
// One helper for both withholding reasons, deliberately: the reconciliation
// phase's own gate and this connection's restore verdict owe the client exactly
// the same thing, and an earlier revision of this change lost the property by
// recording it in only one of the two places.
//
// No lock held on entry.
func (sess *mqttSession) recordWithheldDelivery(cc *client, sub *subscription, reply string) {
	sess.mu.Lock()
	sess.recordWithheldDeliveryLocked(cc, sub, reply)
	sess.mu.Unlock()
}

// recordWithheldDeliveryLocked is recordWithheldDelivery for a caller already
// holding the session lock.
//
// sess.c == nil is the refused-CONNECT case: the connection was detached before
// this delivery woke, and the message is still owed to the SESSION, so the marker
// belongs to it. A session rebound to a DIFFERENT client is that connection's to
// account for, not this stale callback's.
//
// **That boundary is deliberately best-effort, and this change does not try to make
// it more.** A refusal wakes its callbacks and then returns, releasing the ownership
// gate, so a rebind can win that race on ordinary scheduler delay and the woken
// callback is then stale — the delivery stays owed, nothing records it, and its
// redelivery both waits out AckWait instead of being NAKed forward and reaches the
// client with DUP set on a first sight.
//
// Two remedies were built and withdrawn, across three holistic rounds that each
// faulted the previous one:
//
//   - DRAINING the woken callbacks before the refusal returns. A counter cannot be
//     an admission barrier: a callback can be descheduled between loading the
//     verdict and registering, or arrive after the drain has observed zero, and
//     delivery interest stays live until teardown.
//   - Deciding on a positive fact about the DURABLE instead of the client. That
//     removes the rebind race but not the LATENESS — a descheduled callback can
//     still record after the redelivery has gone out — and the set of places a
//     durable can live is neither complete (a record-sourced shared membership's
//     durable exists only through `sharedSubs` and an in-flight config, because
//     `addJSConsToSess` runs after the whole filter succeeds) nor precise (a drain
//     proves the durable survives, not that a given sequence is owed).
//
// So the guarantee is withdrawn rather than approximated a third time. HEAD already
// treats this boundary as "defensive breadth rather than a pinned behavior" and
// names the same consequence, the population that reaches it through a REFUSED
// resume is new to this change, and the DUP derivation itself is F-072's — an open
// alpha blocker for deriving the MQTT flag from the JetStream delivery count. The
// lost prompt NAK is not F-072's, so it is stated as this change's own residual in
// `docs/CONFORMANCE.md`. Closing it needs a real admission boundary, which is a
// mechanism this item does not justify.
//
// **A marker is written only for a delivery that is still OWED**, and the
// predicate is `!closed || phaseOwned`. Both halves are load-bearing, and this
// function is the single home for the rule because the connection verdict outlives
// every per-subscription one:
//
//   - `!closed` covers a live delivery path — an ordinary restored subscription,
//     and a reconciliation subscription that restoration has ADOPTED (adoption
//     clears both `closed` and `phaseOwned`). Its durable survives, so the marker
//     has a first transmission left to protect.
//   - `phaseOwned` covers a reconciliation subscription that has not been adopted
//     yet: still closed, still owed.
//   - Everything else is DOOMED, and must record nothing. A phase subscription
//     whose durable is being deleted is DISOWNED before its gate opens
//     (`unsubscribePhase`'s doomed arm), and an ordinary subscription that was
//     torn down by a QoS downgrade or an UNSUBSCRIBE was never owed this message
//     at all. Writing a marker for either strands an entry nothing can consume:
//     trackPublish clears it only when that durable delivers again, which it never
//     will.
//
// The doomed case is not hypothetical, and it is why the rule lives here rather
// than at the call sites. The reconciliation phase's `undo` purges markers for the
// durables it deleted, but the CONNECTION-scoped verdict is not released by that
// undo — a delivery parked on it wakes at the refusal, strictly AFTER that purge —
// so a caller that recorded unconditionally would leave exactly the stranded
// marker the undo exists to prevent.
//
// sess.mu held on entry.
func (sess *mqttSession) recordWithheldDeliveryLocked(cc *client, sub *subscription, reply string) {
	if sess.c != cc && sess.c != nil {
		return
	}
	if sub == nil || sub.mqtt == nil || sub.mqtt.jsDur == _EMPTY_ {
		return
	}
	if sub.mqtt.closed && !sub.mqtt.phaseOwned {
		return
	}
	sseq, _, _, _, _ := ackReplyInfo(reply)
	if sseq == 0 {
		return
	}
	if sess.deferred == nil {
		sess.deferred = make(map[mqttDeferredKey]string)
	}
	sess.deferred[mqttDeferredKey{sub.mqtt.jsDur, sseq}] = reply
}

// mqttTestRestoreGateBlockedV counts, per message class, the deliveries the
// restore verdict blocked. Tests assert the block POSITIVELY: "the client received
// nothing" also passes when nothing was ever delivered, which is how a mutation
// that removes a gate consultation survives.
var mqttTestRestoreGateBlockedV atomic.Pointer[func(kind string)]

func setMQTTTestRestoreGateBlocked(h func(kind string)) func() {
	prev := mqttTestRestoreGateBlockedV.Load()
	if h == nil {
		mqttTestRestoreGateBlockedV.Store(nil)
	} else {
		mqttTestRestoreGateBlockedV.Store(&h)
	}
	return func() { mqttTestRestoreGateBlockedV.Store(prev) }
}

func mqttTestRestoreGateBlockedHook(kind string) {
	if h := mqttTestRestoreGateBlockedV.Load(); h != nil {
		(*h)(kind)
	}
}

// mqttTestRestorePauseV pauses a CONNECT at one named boundary of the restore, so
// a test can act while the verdict is in a specific state. The boundaries are the
// ones mutations move code across:
//
//	"before-connack"  after the whole pre-CONNACK restore, verdict armed
//	"before-open"     after a confirmed success CONNACK, verdict not yet opened
//	"filter:<sid>"    immediately after one filter's installs, later filters pending
var mqttTestRestorePauseV atomic.Pointer[func(at string)]

func setMQTTTestRestorePause(h func(at string)) func() {
	prev := mqttTestRestorePauseV.Load()
	if h == nil {
		mqttTestRestorePauseV.Store(nil)
	} else {
		mqttTestRestorePauseV.Store(&h)
	}
	return func() { mqttTestRestorePauseV.Store(prev) }
}

func mqttTestRestorePauseHook(at string) {
	if h := mqttTestRestorePauseV.Load(); h != nil {
		(*h)(at)
	}
}

// restoreGateBlocksQoS0 reports whether a QoS 0 delivery must be DROPPED because
// this connection has not been acknowledged yet.
//
// Dropping is the right disposition and it is not a new loss for a plain filter:
// in this window today no raw subscription exists at all, so the message is
// discarded either way. For a SHARED filter it is a real, accepted cost — the
// restored queue subscription already holds group interest, so it can be selected
// for a message another live member would otherwise have received. QoS 0 is
// at-most-once and best-effort, the window is one resume's restoration, and the
// alternatives were worse: buffering is an unbounded new mechanism, a NATS
// subscription's queue group cannot be joined late, and installing the raw
// subscription outside the verdict makes a QoS 0 PUBLISH the client's first
// packet. Stated in docs/CONFORMANCE.md.
func (sess *mqttSession) restoreGateBlocksQoS0() bool {
	g := sess.restoreGate.Load()
	return g != nil && !g.opened.Load()
}
