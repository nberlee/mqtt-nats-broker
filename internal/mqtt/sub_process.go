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

// SUBSCRIBE: the subscription shape, shared-subscription names, processing subscriptions,
// deferred consumer deletion, parsing SUBSCRIBE, and SUBACK.

package mqtt

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// mqttSub contains fields associated with a MQTT subscription, and is added to
// the main subscription struct for MQTT message delivery subscriptions. The
// delivery callbacks may get invoked before sub.mqtt is set up, so they should
// acquire either sess.mu or sess.subsMu before accessing it.
type mqttSub struct {
	// The sub's QOS and the JS durable name. They can change when
	// re-subscribing, and are used in the delivery callbacks. They can be
	// quickly accessed using sess.subsMu.RLock, or under the main session lock.
	qos   byte
	jsDur string

	// shared marks this subscription as belonging to an MQTT 5.0 Shared
	// Subscription group. Read in the delivery callbacks like qos. When set on a
	// QoS0 (raw-subject) sub, the callback must not deliver stored (QoS 1/2
	// published) messages — those arrive through the shared JS consumer instead.
	// Spec5 [4.8.2].
	shared bool

	// closed marks the subscription as barriered so QoS 1/2 delivery callbacks stop
	// tracking new messages for it: a QoS downgrade to 0 or an unsubscribe. It does NOT
	// imply the subscription is going away — a downgrade that converts its consumer into
	// a drain reopens this same object once the record is confirmed, because the drain
	// still owes what it transmitted. Guarded like qos/jsDur (sess.mu or sess.subsMu). Unlike clearing
	// sub.mqtt, this keeps the struct valid for an in-flight enqueue.
	closed bool

	// phaseOwned marks a delivery subscription installed CLOSED by the resume
	// reconciliation phase and not yet adopted by subscription restoration.
	// Restoration adopts it IN PLACE (clearing closed and this marker together,
	// so interest never lapses); a marker still standing after processSubs means
	// restoration skipped the filter and the sweep must reclaim the
	// subscription, or it would hold consumer interest forever while delivering
	// nothing. Guarded like closed. See sess_resume_reconcile.go.
	phaseOwned bool

	// phaseGate is closed when the reconciliation phase releases this
	// subscription — adopted by restoration, or reclaimed by the sweep/undo. A
	// QoS 1/2 delivery that arrives while it is still open WAITS for it rather
	// than being skipped: the interest is installed before the CONNACK, so
	// JetStream pushes the session's whole offline backlog into that window, and
	// skipping it would hold every one of those messages until AckWait (30s by
	// default) — turning an immediate resume delivery into a half-minute stall —
	// and then re-send them with DUP set, on their FIRST transmission to the
	// client. Waiting keeps them first deliveries, in order, at adoption speed.
	// Bounded by mqttPhaseGateWait so a CONNECT that dies without releasing
	// cannot park a delivery goroutine; past it the delivery falls back to the
	// closed-subscription skip and AckWait. nil once released (and on every
	// subscription the phase did not install). Guarded like closed.
	phaseGate chan struct{}

	// Pending serialization of retained messages to be sent when subscription
	// is registered. The sub's delivery callbacks must wait until `prm` is
	// ready (can block on sess.mu for that, too).
	//
	// Each frame carries the packet identifier it was framed under, because a QoS 1/2
	// retained send reserves that identifier from allocation until the frame is written,
	// and whoever writes it has to know which reservations it is discharging (F-091).
	//
	// The slice is also the unit of DETACHMENT: whoever takes it under c.mu takes all of it
	// and nils it, so each frame has exactly one disposition — written, or detached and then
	// TRANSFERRED to a replacement, or detached and then ABANDONED. Detaching is not
	// releasing: a same-sid replacement detaches and hands the frames and their reservations
	// on intact, because a duplicate-filter SUBSCRIBE still owes that replay.
	prm []mqttRetainedFrame
	// replay is this subscription lineage's outstanding retained replay (F-105 7c): the items
	// already published to the session's driver and the GATE live QoS 0 waits on. Inherited by a
	// same-sid replacement, marked departed when the sid leaves. Guarded by the connection's c.mu.
	replay *mqttRetainedReplay

	// If this subscription needs to be checked for being reserved. E.g. '#' or
	// '*' or '*/'.  It is set up at the time of subscription and is immutable
	// after that.
	reserved bool

	// noLocal is the v5 No Local subscription option as the CURRENT subscription
	// states it. Only retained replay reads it — a fresh forwarding under the
	// subscription being created. The delivery callbacks never do: whether an owned
	// message reaches this subscription was decided when the server took ownership,
	// and is carried by the message's No Local stamp (see filterKey, nolocal_stamp.go).
	// Spec5 [3.8.3.1], [MQTT-4.5.0-1].
	noLocal bool

	// filterKey is the session key of the filter this subscription delivers for
	// (mqttFilter.sessionKey), which is what a No Local stamp's tags name. A level-up
	// companion carries its MAIN filter's key, not its own shortened subject, and so does
	// every JetStream delivery subscription of the filter, including a restored one and a
	// DRAIN's: a drain never decides No Local while it drains, but a re-SUBSCRIBE adopts its
	// subscription as ordinary membership in place, so it must already carry the key.
	// Empty only where No Local can never be decided: shared and affine installs. Set at
	// install and immutable for the subscription's life (a same-sid re-SUBSCRIBE is the
	// same filter). Spec5 [3.8.3.1].
	filterKey string

	// retainAsPublished is the v5 Retain As Published subscription option: when
	// set, a live-forwarded message keeps the RETAIN flag it was published with
	// instead of having it cleared. Read in the delivery callbacks like noLocal.
	// Spec5 [3.8.3.1].
	retainAsPublished bool

	// subID is the v5 Subscription Identifier for this subscription (0 = none).
	// The server injects it into every PUBLISH delivered because of this sub.
	// Read in the delivery callbacks like noLocal. Spec5 [3.8.2.1.2].
	subID int
}

// mqttSharedSubPrefix is the MQTT 5.0 Shared Subscription topic filter prefix
// ("$share/{ShareName}/{filter}"). Spec5 [4.8.2].
var mqttSharedSubPrefix = []byte("$share/")

// mqttSharedNames returns the cluster-wide names of one PHYSICAL incarnation of a shared
// subscription group on a given underlying NATS subject: the queue-group name (also the JS
// consumer DeliverGroup), the consumer's deliver subject, and the durable consumer name.
//
// The queue is the LOGICAL identity and depends on the group key alone: authorization
// recovers it from the durable and raw QoS 0 queue subscriptions share it. The deliver
// subject and the durable are PHYSICAL and carry the incarnation ordinal the group record
// mounted — "<qkey>_<inc>" in ONE token, so "$MQTT.qsub.*" in an ACL still covers the
// deliver subject. A consumer for incarnation X pushes to qsub.<qkey>_X and one for Y to
// qsub.<qkey>_Y, so a delete of X can never reach a successor a joiner mounted meanwhile.
// The key is a 16-character hash of ShareName + subject, so all members converge on one
// group record and an accidental collision (which would merge two groups) is negligible.
// Spec5 [4.8.2].
func mqttSharedNames(group, subject string, inc uint64) (queue, deliver, durable string) {
	qkey := mqttSharedGroupKey(group, subject)
	queue = mqttSharedQueuePrefix + qkey
	tail := qkey + "_" + mqttSharedIncToken(inc)
	deliver = mqttSharedSubDeliverPrefix + tail
	durable = mqttSharedConsumerDurablePrefix + tail
	return queue, deliver, durable
}

// mqttTestSharedQoS0OutputHook runs between a shared QoS 0 offer's pre-output HandBack check and its
// frame write (the window docs/CONFORMANCE.md states for consult 84). Nil in production.
var mqttTestSharedQoS0OutputHook atomic.Pointer[func(jsDur, reply string)]

// mqttSharedQueueName is the LOGICAL queue-group name alone, for the callers that need no
// incarnation: the raw QoS 0 queue subscription and the consumer's DeliverGroup.
func mqttSharedQueueName(group, subject string) string {
	return mqttSharedQueuePrefix + mqttSharedGroupKey(group, subject)
}

type mqttFilter struct {
	filter string
	qos    byte
	// v5 Retain Handling subscription option (bits 4-5 of the options byte):
	// 0 = send retained at subscribe, 1 = send only if the subscription is new,
	// 2 = never. Zero value (0) matches 3.1.1 behavior. Spec5 [3.8.3.1].
	retainHandling byte
	// v5 No Local subscription option (bit 2): when set, a message must not be
	// delivered back to the connection that published it. Spec5 [3.8.3.1].
	noLocal bool
	// v5 Retain As Published subscription option (bit 3): when set, a
	// live-forwarded message keeps the RETAIN flag it was published with. Spec5
	// [3.8.3.1].
	retainAsPublished bool
	// v5 Subscription Identifier from the SUBSCRIBE properties block (0 = none).
	// The packet's single identifier applies to every filter it carries. Spec5
	// [3.8.2.1.2].
	subID int
	// v5 (UN)SUBACK reason code for this filter (zero value is success).
	reason byte
	// v5 human-readable failure detail, set where a filter is rejected. The
	// (UN)SUBACK packet-level Reason String carries the first failed filter's
	// text. Spec5 [3.9.2.2.2].
	reasonStr string
	// shared is set when the filter is an MQTT 5.0 Shared Subscription
	// ("$share/{ShareName}/{filter}", v5 only). group holds the ShareName. When
	// shared, filter holds the NATS subject of the underlying topic filter only
	// (the "$share/" prefix and ShareName are stripped), so all existing
	// per-subject logic applies unchanged. Spec5 [4.8.2].
	shared bool
	group  string
	// sharedM is the MEMBERSHIP this shared filter carries into the record: on the acquire
	// arm the acquisitions J1 minted before the eager save, on the refresh and restore arms
	// the standing membership with this packet's QoS. update() reads it for a shared filter
	// and refuses a shared filter that has none, so no membership is ever persisted without
	// its acquisition references. See mqttSharedMembership.
	sharedM *mqttSharedMembership
	// Used only for tracing and should not be used after parsing of (un)sub protocols.
	ttopic []byte
}

// sessionKey returns the key used for this filter in the session's subscription
// maps and as the NATS subscription sid. A shared subscription embeds its
// ShareName so distinct groups (and a plain subscription) on the same subject do
// not collide; a plain subscription just uses its NATS subject. Spec5 [4.8.2].
//
// A plain sid IS NOT distinguishable from a shared one by prefix, and nothing may test it
// that way. This comment used to claim a plain sid "never contains '/'", which is false:
// mqttToNATSSubjectConversion encodes a literal '.' as "//", so the ordinary plain filter
// "$share.foo" has the sid "$share//foo" and satisfies HasPrefix(sid, "$share/"). The
// authorities are f.shared, set at parse time, and cc.DeliverGroup on a resident config.
// F-083.
func (f *mqttFilter) sessionKey() string {
	if f.shared {
		return string(mqttSharedSubPrefix) + f.group + "/" + f.filter
	}
	return f.filter
}

// Helper to set the sub's mqtt fields and possibly serialize (pre-loaded)
// retained messages.
//
// Session lock held on entry. Acquires the subs lock and holds it for
// the duration. Non-MQTT messages coming into mqttDeliverMsgCbQoS0 will be
// waiting.
func (sess *mqttSession) processQOS12Sub(
	c *client, // subscribing client.
	subject, sid, queue []byte, filterKey string, isReserved bool, qos byte, noLocal, retainAsPublished, shared bool, subID int, jsDurName string, h msgHandler, // subscription parameters.
) (*subscription, error) {
	if hook := mqttTestQOS12SubFailHookV.Load(); hook != nil {
		if err := (*hook)(jsDurName); err != nil {
			return nil, err
		}
	}
	return sess.processSub(c, subject, sid, queue, filterKey, isReserved, qos, noLocal, retainAsPublished, shared, subID, jsDurName, h, false, nil, false, nil)
}

func (sess *mqttSession) processSub(
	c *client, // subscribing client.
	subject, sid, queue []byte, filterKey string, isReserved bool, qos byte, noLocal, retainAsPublished, shared bool, subID int, jsDurName string, h msgHandler, // subscription parameters.
	initShadow bool, // do we need to scan for shadow subscriptions? (not for QOS1+)
	rms map[string]*mqttRetainedMsg, // preloaded rms (can be empty, or missing items if errors)
	trace bool, // trace serialized retained messages in the log?
	as *mqttAccountSessionManager, // needed only for rms serialization.
) (*subscription, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if elapsed > mqttProcessSubTooLong {
			c.Warnf("Took too long to process subscription for %q: %v", subject, elapsed)
		}
	}()

	// Hold subsMu to prevent QOS0 messages callback from doing anything until
	// the (MQTT) sub is initialized.
	sess.subsMu.Lock()
	defer sess.subsMu.Unlock()

	// COMPLETION OWNERSHIP, installed here rather than at each registration: every
	// session-scoped delivery callback in the package reaches the NATS subscription
	// through this one wrapper, so the boundary covers the class by construction
	// instead of by a list (F-036 unit 1). Only the QoS 1/2 handler owes the
	// withheld-delivery record on a refusal; the others simply skip without acking.
	h = sess.leasedHandler(h, jsDurName != _EMPTY_)
	sub, displacedFrames, err := c.processSub(subject, queue, sid, h, false)
	if err != nil {
		// c.processSub already called c.Errorf(), so no need here.
		return nil, err
	}
	subs := []*subscription{sub}
	if initShadow {
		subs = append(subs, sub.shadow...)
	}
	for _, ss := range subs {
		if ss.mqtt == nil {
			// reserved is set only once and once the subscription has been
			// created it can be considered immutable.
			ss.mqtt = &mqttSub{
				reserved: isReserved,
			}
		}
		// QOS and jsDurName can be changed on an existing subscription, so
		// accessing it later requires a lock.
		ss.mqtt.qos = qos
		ss.mqtt.noLocal = noLocal
		ss.mqtt.filterKey = filterKey
		ss.mqtt.retainAsPublished = retainAsPublished
		ss.mqtt.subID = subID
		ss.mqtt.jsDur = jsDurName
		ss.mqtt.shared = shared
		// A (re)configured subscription is live; clear any prior teardown mark.
		ss.mqtt.closed = false
	}

	// A same-sid re-SUBSCRIBE DISPLACED a subscription that may still have been holding
	// buffered retained frames, and those frames are still owed to the client: a v5
	// SUBSCRIBE may legitimately carry one filter twice, the first entry asking for the
	// replay. So they are MOVED onto the replacement — which is the entry every later flush
	// and every later teardown will find — rather than released here.
	//
	// Moving rather than releasing is what keeps this out of the reservation vocabulary
	// entirely: the identifiers stay reserved by the frames that still carry them, the
	// written release discharges them when the flush happens, and the abandoned release
	// discharges them if the replacement is torn down. FIRST, so the displaced frames
	// precede any this packet is about to serialize, preserving arrival order (F-091).
	// AND THE REPLAY LINEAGE (F-105 7c): a replacement INHERITS the displaced subscription's
	// outstanding replay object — the items the driver is executing and the gate its live
	// callbacks wait on — so the replacement neither loses nor duplicates them, and this packet's
	// own snapshot is staged after them.
	if sub.mqtt != nil && (len(displacedFrames.frames) > 0 || displacedFrames.replay != nil) {
		c.mu.Lock()
		if r := displacedFrames.replay; r != nil {
			r.handing = false
			r.sub = sub
			sub.mqtt.replay = r
		}
		sub.mqtt.prm = append(displacedFrames.frames, sub.mqtt.prm...)
		c.mu.Unlock()
	}

	if len(rms) > 0 {
		// Only deal with retained messages for the normal subscription,
		// not the shadow one (which is for a different account and subject).
		as.serializeRetainedMsgsForSub(rms, sess, c, sub, trace)
	}

	return sub, nil
}

// mqttUpdateFilterOptSet adds or removes filter from a per-option filter set to
// match want, allocating the set on first add. Returns the (possibly new) set
// and whether it changed, so the caller can flag the session for persistence.
func mqttUpdateFilterOptSet(set map[string]struct{}, filter string, want bool) (map[string]struct{}, bool) {
	if _, has := set[filter]; want && !has {
		if set == nil {
			set = make(map[string]struct{})
		}
		set[filter] = struct{}{}
		return set, true
	} else if !want && has {
		delete(set, filter)
		return set, true
	}
	return set, false
}

// mqttUpdateFilterSubID records (or, when id is 0, removes) the v5 Subscription
// Identifier for filter, allocating the map on first add. Returns the (possibly
// new) map and whether it changed, so the caller can flag the session for
// persistence.
func mqttUpdateFilterSubID(m map[string]int, filter string, id int) (map[string]int, bool) {
	if id != 0 {
		if cur, has := m[filter]; !has || cur != id {
			if m == nil {
				m = make(map[string]int)
			}
			m[filter] = id
			return m, true
		}
	} else if _, has := m[filter]; has {
		delete(m, filter)
		return m, true
	}
	return m, false
}

// mqttFilterSetToList returns a subscription-filter set as a slice for
// persistence (nil when empty, to omit the JSON field). Used for the per-option
// filter sets (No Local, Retain As Published).
func mqttFilterSetToList(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	l := make([]string, 0, len(set))
	for f := range set {
		l = append(l, f)
	}
	return l
}

// mqttFilterSetFromList rebuilds a subscription-filter set from a persisted
// slice (nil when empty).
func mqttFilterSetFromList(l []string) map[string]struct{} {
	if len(l) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(l))
	for _, f := range l {
		set[f] = struct{}{}
	}
	return set
}

// mqttShouldSendRetained reports whether retained messages must be replayed for
// a new subscription with the given v5 Retain Handling option. Spec5 [3.8.3.1]:
// 0 always sends, 1 sends only when the filter has no current subscription, 2
// never sends. subs is the session's pre-SUBSCRIBE filter set.
func mqttShouldSendRetained(retainHandling byte, subs map[string]byte, filter string) bool {
	switch retainHandling {
	case 2:
		return false
	case 1:
		_, existed := subs[filter]
		return !existed
	default:
		return true
	}
}

// mqttDeferredDel is one consumer teardown a SUBSCRIBE packet deferred to its confirmed
// save, together with everything that deferral staged — so the whole staging can be
// WITHDRAWN as a unit when a later occurrence of the same filter in the same packet
// re-establishes QoS > 0.
//
// The three facts lived in three parallel stack slices with three per-filter boundaries,
// over three different cardinalities: one staging contributes at most one closed
// subscription and zero or more nonces. Three boundaries over three cardinalities cannot
// express "withdraw the staging for THIS sid", because a mid-slice removal invalidates the
// later filters' boundaries. Collapsing them into one entry per staging is what makes the
// revocation expressible at all.
//
// Package-level, rather than local to processSubs as it once was, because the readiness
// predicate below is the part a unit test must reach: an external test can neither
// construct a function-local type nor invoke a closure over one.
type mqttDeferredDel struct {
	sid string
	cc  *ConsumerConfig
	// The delivery barrier this staging lowered, nil when there was none to lower.
	closed *subscription
	// The verdicts it recorded on TRANSMITTED registers. Those deliveries stay live and
	// still owe their identifiers, so a withdrawal has to withdraw these too.
	voided []string
	// A later occurrence of this sid re-established QoS > 0, so the teardown must not
	// run. The entry STAYS in the packet's list: a per-filter cancellation still has to
	// find it, to re-stage the downgrade when the reviving occurrence itself fails.
	revoked bool
	// convert says this consumer is ELIGIBLE to become a drain instead of being destroyed,
	// because the stream permits draining at all (drainingAllowed, checked once per
	// packet). It says nothing about whether the consumer holds started deliveries —
	// stageDrain decides that, once, at the conversion boundary, so there is exactly one
	// "started deliveries" predicate. A candidate's staging cancels the not-yet-transmitted
	// allocations but does NOT void the transmitted ones, because those are the exchanges
	// the conversion preserves. See convertDowngradeCandidatesLocked.
	convert bool
	// converted says the conversion actually happened, so the confirmed save must NOT run
	// the irreversible teardown for this entry: the durable is a drain now, named in the
	// record's Drains, and its started deliveries are still owed to the client.
	converted bool
	// consReleased says the final-update boundary took this consumer out of sess.cons, so
	// the record that carries the QoS 0 membership does not name a durable this packet is
	// about to delete. A definite save failure must put it back before the rollback
	// enumerates sess.cons looking for the consumers this packet created.
	consReleased bool
}

// revokeLocked undoes this staging because a LATER occurrence of the same filter in the
// same packet re-established QoS > 0. sess.update applies the last occurrence, so the
// record ends at that QoS and the only coherent end state is the consumer alive and
// delivering under THIS occurrence's terms. [MQTT-3.8.4-3], [MQTT-3.8.4-4].
//
// It REFUSES rather than grants when delivery readiness cannot be established. The saved
// subscription pointer proves only what was true at staging time; granting a QoS the broker
// cannot serve is the defect this repair exists to remove, not a lesser form of it. The
// refusal fails the filter 0x80 and leaves the deferral standing, so the packet still ends
// coherent. That predicate is a defensive assertion rather than a reachable branch — for a
// live entry created by this packet the pair "cons[sid] present, its delivery subscription
// closed" has no other producer — which is why its test synthesizes the state.
//
// What it deliberately does NOT undo is the staged CANCELLATIONS. The staging destroyed
// the not-yet-transmitted allocations outright: the pending slot is a cancelled stub and
// the ack subject is parked in sess.deferred. Their tombstones must still ride the packet's
// write, or a register store still in flight lands with nothing to explain it and holds an
// identifier and a Receive Maximum slot for the rest of the session. Their withheld markers
// stay in sess.deferred for the same reason, and a resumed delivery consumes one normally.
//
// For a CONVERSION CANDIDATE there is also nothing to un-void: its staging recorded no
// verdict on a transmitted register, precisely because those exchanges were to be drained. The message
// itself is untransmitted and unacked in JetStream, so it is re-offered through the
// subscription this revocation just restored and trackPublish allocates a fresh identifier.
//
// sess.mu must be held for the WHOLE of it — the barrier, the terms and the un-void are one
// state change. It is the SPLIT that is the defect: a delivery dispatched between a reopen
// and a later refresh would be sent at the PRE-PACKET QoS, so a session this packet
// downgrades 2 -> 1 would transmit a QoS 2 PUBLISH under a QoS 1 grant [MQTT-3.8.4-8]. The
// order of the assignments WITHIN the section is not a correctness constraint; every reader
// holds one of the two locks taken here.
func (dd *mqttDeferredDel) revokeLocked(c *client, sess *mqttSession,
	qos byte, noLocal, retainAsPublished bool, subID int) error {

	if c == nil || dd.cc == nil || sess.cons[dd.sid] != dd.cc {
		return fmt.Errorf("mqtt: filter %q no longer holds the consumer its downgrade staged", dd.sid)
	}
	if dd.closed == nil || dd.closed.mqtt == nil {
		return fmt.Errorf("mqtt: filter %q has no delivery subscription to restore", dd.sid)
	}
	// Taken under c.mu exactly as closeSharedDeliveryLocked takes it: the pointer we hold
	// is only evidence if it is STILL the installed one.
	c.mu.Lock()
	installed := c.subs[dd.cc.DeliverSubject]
	c.mu.Unlock()
	if installed != dd.closed {
		return fmt.Errorf("mqtt: filter %q no longer delivers through the subscription its downgrade closed", dd.sid)
	}
	// The QoS 1/2 callback reads these under sess.mu and the QoS 0 shadow callback under
	// subsMu, so write under both, as processSub and the existing-consumer refresh do.
	sess.subsMu.Lock()
	dd.closed.mqtt.qos = qos
	dd.closed.mqtt.noLocal = noLocal
	dd.closed.mqtt.retainAsPublished = retainAsPublished
	dd.closed.mqtt.subID = subID
	dd.closed.mqtt.closed = false
	sess.subsMu.Unlock()
	sess.unvoidRegNoncesLocked(dd.voided)
	dd.revoked = true
	return nil
}

// Process subscriptions for the given session/client.
//
// When `fromSubProto` is false, it means that this is invoked from the CONNECT
// protocol, when restoring subscriptions that were saved for this session.
// In that case, there is no need to update the session record.
//
// When `fromSubProto` is true, it means that this call is invoked from the
// processing of the SUBSCRIBE protocol, which means that the session needs to
// be updated. It also means that if a subscription on same subject with same
// QoS already exist, we should not be recreating the subscription/JS durable,
// since it was already done when processing the CONNECT protocol.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
// tok identifies the record transaction this whole packet runs in: membership
// changes, the affine provisioning save, the per-filter rollbacks and the final
// update are one transaction, so no other writer can persist a half-applied
// SUBSCRIBE. The CONNECT restore path passes its own transaction's token down.
// See sess_recordtx.go.
// sharedVerdicts is the CONNECT restore's batched group-durable classification, keyed by
// sid, and nil on the SUBSCRIBE path. It is passed explicitly rather than parked on the
// session so its lifetime is one restore and nothing can read a previous connection's
// judgement. See shared_group_verify.go.
// The second result is F-077's arbitration trigger: this operation installed, repaired,
// adopted or refreshed shared group delivery interest. It is a RETURN VALUE rather than a
// session field on purpose — see mqttSharedJoinResult.installed.
//
// BOTH callers owe a proof, and both make one. The CONNECT caller proves ownership before
// the CONNACK. mqttProcessSubs proves it as the packet's last record write and withholds
// every delivery the packet could authorise until that proof lands, through a packet-scoped
// transmission gate — which the CONNECT path needs no equivalent of, because its restore
// gate is already withholding everything until the CONNACK.
//
// The paths were asymmetric until F-078: mqttProcessSubs used to DISCARD this fact, which is
// what left live interest serving unarbitrated. See sub_operation_gate.go.
func (as *mqttAccountSessionManager) processSubs(sess *mqttSession, c *client, tok recordTx,
	filters []*mqttFilter, fromSubProto, trace bool,
	sharedVerdicts map[string]mqttSharedGroupVerdict) ([]*subscription, mqttSubPacketFacts, error) {

	// Hold an entry in the subs barrier for the whole SUBSCRIBE (or CONNECT session
	// restore) processing: a QoS 1/2 store that samples the barrier while this entry
	// is live waits in mqttInitiateMsgDeliveryJSA until the JetStream consumers
	// created here have registered interest with the messages stream, so a racing
	// publish is never discarded by interest-based retention. A store that sampled
	// before this entry does not wait for it. Deadlock-free: nothing
	// reachable from here stores to $MQTT_msgs, and a publisher blocked in
	// wait() holds no locks.
	barrierEntry := as.jsa.subsBarrier.enter()
	defer as.jsa.subsBarrier.leave(barrierEntry)

	// Test seam, fired with the barrier HELD and before any consumer exists — the exact
	// window the barrier is for. Blocking here lets a test prove the guarantee positively:
	// a QoS 1/2 store issued while this is held must not complete, and must complete and
	// deliver once it is released. That is what distinguishes a real barrier violation from
	// a test whose publish merely raced ahead of SUBSCRIBE processing, which a sleep-based
	// stagger cannot tell apart. See mqttTestSubsBarrierHeldHookV.
	if h := mqttTestSubsBarrierHeldHookV.Load(); h != nil {
		(*h)(filters)
	}

	// Helper to determine if we need to create a separate top-level
	// subscription for a wildcard. sid is derived from the passed-in sid (which
	// for a shared subscription embeds the ShareName) rather than from the
	// subject, so the level-up sid stays in the same namespace as its parent.
	fwc := func(subject, sid string) (bool, string, string) {
		if !mqttNeedSubForLevelUp(subject) {
			return false, _EMPTY_, _EMPTY_
		}
		// Say subject is "foo.>", remove the ".>" so that it becomes "foo"
		fwcsubject := subject[:len(subject)-2]
		// Change the sid to "foo fwc" (for a shared sub, "$share/g/foo fwc")
		fwcsid := sid[:len(sid)-2] + mqttMultiLevelSidSuffix

		return true, fwcsubject, fwcsid
	}

	rmSubjects := map[string]uint64{}
	// Whether this session already had a session-affine QoS 2 delivery path, taken
	// BEFORE the loop below can provision one. It is captured here rather than
	// alongside the subscription snapshot further down because the loop provisions
	// it, and a rollback must be able to tell "this packet built it" from "it was
	// already there". Spec5 [4.8.2].
	// facts is this packet's own account of what it did, aggregated on THIS STACK and
	// nowhere else: a fact parked on mqttSession would outlive the operation that raised it
	// and govern the next one. interestRestored is committed per filter only at the
	// whole-filter success boundary below, so a filter whose level-up companion fails is
	// rolled back out of it exactly as it is rolled back out of everything else. See
	// mqttSubPacketFacts for why recordCASRejected cannot be recovered from err.
	var facts mqttSubPacketFacts

	var q2AffPrePacket bool
	if fromSubProto {
		sess.mu.Lock()
		q2AffPrePacket = sess.q2AffGen != _EMPTY_
		sess.mu.Unlock()
	}
	// Preload retained messages for all requested subscriptions.  Also, since
	// it's the first iteration over the filter list, do some cleanup.
	for _, f := range filters {
		// A filter flagged with a failure reason at parse time (e.g. a shared
		// subscription) is not processed; the code becomes the SUBACK byte.
		if f.reason >= mqttSubAckFailure {
			f.qos = f.reason
			continue
		}
		if f.qos > 2 {
			f.qos = 2
		}
		// A QUARANTINED drain refuses the WHOLE FILTER, checked here because the filter —
		// not the sid — is the atomicity boundary.
		//
		// A wildcard such as `foo/#` owns a main sid AND a level-up companion, and the
		// per-sid refusal downstream processed them independently: a usable main drain was
		// ADOPTED into sess.cons and only the companion then failed. The per-filter failure
		// cleanup removes subscriptions and returned consumers but does not put an adopted
		// drain back, and the persisted record omits a consumer with no sess.subs
		// membership — so the next successful save dropped that drain's Started obligations
		// and its durable identity outright.
		//
		// Refused up front, before anything is adopted, so there is no partial state to
		// roll back. This is the same property three earlier rounds converged on, applied
		// at the boundary that actually holds it.
		if f.qos > 0 && !f.shared {
			held, hold := _EMPTY_, mqttDrainNotHeld
			sess.mu.Lock()
			for _, sid := range mqttUnsubSids(f.sessionKey()) {
				if h := sess.drainSidHoldLocked(sid); h != mqttDrainNotHeld {
					held, hold = sid, h
					break
				}
			}
			sess.mu.Unlock()
			if held != _EMPTY_ {
				// The reason travels with the refusal. Both holds refuse identically, but
				// they converge differently — one on an operator, one on the drained
				// exchange completing — and a message that names the wrong one sends the
				// operator looking for a record that is doing nothing wrong.
				c.Errorf("MQTT: refusing filter %q of client %q: its drained delivery on %q %s",
					f.filter, sess.id, held, hold.reason())
				f.qos = mqttSubAckFailure
				continue
			}
		}
		if f.shared && f.qos == 2 && !c.mqtt.experimentalSharedQoS2 {
			c.Warnf("Downgrading shared subscription QoS2 to QoS1 for %q; shared QoS2 is experimental", f.filter)
			f.qos = 1
		}

		// Deny the whole reserved "$MQTT.>" namespace, not just the delivery
		// subjects. Upstream rejected only $MQTT.sub., $MQTT.deliver.pubrel. and
		// $MQTT.qsub., with a TODO asking why it bothered when "one can subscribe
		// to `#` and it'll include everything". The answer: a wildcard filter
		// cannot reach a $-topic ([MQTT-4.7.2-1], enforced at delivery by
		// mqttMustIgnoreForReservedSub), but an *explicit* "$MQTT/..." filter is
		// not "reserved" by that rule and is delivered normally.
		//
		// That matters here because the broker's own state moves as ordinary NATS
		// publishes on these subjects: every QoS 1/2 PUBLISH is stored to
		// $MQTT.msgs.<subject> and every retained message to $MQTT.rmsgs.<key>,
		// alongside $MQTT.sess.*, $MQTT.accept.*, $MQTT.JSA.*, $MQTT.willq.*,
		// $MQTT.qos2.in.* and $MQTT.out.*. So a client subscribing to
		// "$MQTT/msgs/#" would read every QoS 1/2 message in the account
		// regardless of its own per-topic NATS permissions -- a read-side bypass
		// of its subscribe ACL. Deny the prefix outright; the three delivery
		// subjects are covered by it.
		//
		// Defense in depth: the NATS account should ALSO deny subscribe on
		// "$MQTT.>" (the broker's internal subscriptions live on the service
		// connection, so that deny is safe -- see processSub). The broker must not
		// depend on that config being correct.
		//
		// The restore path cannot reach this: a record carrying such a filter is
		// refused before it becomes a session (mqttReservedMembershipInRecord), so
		// this denial only ever answers a live SUBSCRIBE.
		if strings.HasPrefix(f.filter, mqttPrefix) {
			f.qos = mqttSubAckFailure
			f.reasonStr = "cannot subscribe to internal server subject"
			continue
		}

		if f.qos == 2 {
			err := sess.ensurePubRelConsumerSubscription(c)
			if err == nil {
				err = as.filterExitFail(fromSubProto, "pubrel", f.sessionKey())
			}
			if err != nil {
				c.Errorf("failed to initialize PUBREL processing: %v", err)
				f.qos = mqttSubAckFailure
				f.reasonStr = err.Error()
				continue
			}
			// A SHARED QoS 2 subscription additionally needs this session's affine
			// delivery path, because a started delivery is moved off the group
			// durable onto it. Provisioned only for shared filters: a plain QoS 2
			// subscription never moves anything and must not be charged an extra
			// consumer. Unlike the PUBREL consumer above, it participates in this
			// packet's rollback (q2AffPrePacket): it costs a consumer, a
			// MaxAckPending charge and a persisted generation, none of which a
			// SUBSCRIBE that never took effect should leave behind. Spec5 [4.8.2];
			// see qos2_shared_affine.go.
			if f.shared {
				affCAS, err := sess.ensureQ2AffineConsumerSubscription(c, tok)
				if affCAS {
					// Only the provisioning save's own CAS rejection, never the
					// rest of this arm's error surface. See
					// mqttSubPacketFacts.recordCASRejected.
					facts.recordCASRejected = true
				}
				if err == nil {
					err = as.filterExitFail(fromSubProto, "affine", f.sessionKey())
				}
				if err != nil {
					// Deliberately NOT marked. This arm's error covers consumer
					// creation, the delivery subscription, validation and the
					// restore-exit seam as well as the provisioning record save, and
					// none of those except the save touches the session record at all.
					// Marking the whole arm converted an ordinary per-filter failure
					// into a connection-level refusal, which is a contract owned by the
					// affine items rather than by F-078. The provisioning save's own CAS
					// rejection HAS its own narrower signal: the helper returns it
					// separately, and it is ORed into the fact above.
					c.Errorf("failed to initialize session-affine QoS 2 delivery: %v", err)
					f.qos = mqttSubAckFailure
					f.reasonStr = err.Error()
					continue
				}
			}
		}

		// Shared subscriptions never receive retained messages. Spec5 [4.8.2].
		if f.shared {
			continue
		}
		// Find retained messages. The v5 Retain Handling option controls whether
		// they are replayed at subscribe time. sess.subs still holds the
		// pre-SUBSCRIBE state here (sess.update runs at the end), so it is the
		// right source for the "subscription already exists" test. Spec5 [3.8.3.1].
		if fromSubProto && mqttShouldSendRetained(f.retainHandling, sess.subs, f.filter) {
			as.addRetainedSubjectsForSubject(rmSubjects, f.filter)
			if need, subject, _ := fwc(f.filter, f.filter); need {
				as.addRetainedSubjectsForSubject(rmSubjects, subject)
			}
			// A literal (non-wildcard) filter names one exact subject: force it
			// into rmSubjects even if the in-memory retained-subjects index
			// (as.retmsgs, populated asynchronously by processRetainedMsg
			// reacting to the JetStream store) doesn't have it yet, so
			// loadRetainedMessages' authoritative "last message for subject"
			// stream query below still runs for it. Without this, a retained
			// message published shortly before this SUBSCRIBE can race ahead of
			// that index and be missed entirely. Wildcard filters still rely on
			// the index — there is no cheap "list subjects" stream query to
			// enumerate matches against. Spec5 [3.8.3.1].
			if _, ok := rmSubjects[f.filter]; !ok && !strings.ContainsAny(f.filter, "*>") {
				rmSubjects[f.filter] = 0
			}
		}
	}

	var rms map[string]*mqttRetainedMsg
	if len(rmSubjects) > 0 {
		// Make the best effort to load retained messages.
		rms = as.loadRetainedMessages(rmSubjects, c)
	}

	// Small helper to add the consumer config to the session.
	//
	// Under sess.mu, like every other writer of cons: the map is read under that
	// lock by paths that run concurrently with a SUBSCRIBE or a CONNECT restore
	// — the orphan sweep, the record builder, the resume reconciliation phase —
	// so an unlocked write here is a data race on session state, not merely a
	// stale read. Never called with the lock already held (the two call sites sit
	// between the per-filter processSub calls, which take it internally).
	addJSConsToSess := func(sid string, cc *ConsumerConfig) {
		if cc == nil {
			return
		}
		sess.mu.Lock()
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[sid] = cc
		sess.mu.Unlock()
	}

	// Snapshot pre-SUBSCRIBE membership AND consumer entries so a final-save
	// failure can restore both exactly (only meaningful when persisting, i.e.
	// fromSubProto). Spec5 [4.8.2].
	var snap *mqttSubSnapshot
	var consSnap map[string]*ConsumerConfig
	if fromSubProto {
		snap = sess.snapshotSubs()
		sess.mu.Lock()
		consSnap = make(map[string]*ConsumerConfig, len(sess.cons))
		for k, v := range sess.cons {
			consSnap[k] = v
		}
		sess.mu.Unlock()
	}

	// reclaimDrainDelivery gives a drain whose delivery subscription the resume could
	// not install one now, and adopts it, before provisioning runs. Without it
	// processJSConsumer adopts the drain and its existing-consumer arm reads the entry
	// as proof of a delivery subscription, so the filter is SUBACKed successfully and
	// then delivers nothing for the rest of the connection. See unsub_drain.go.
	//
	// Live SUBSCRIBE only: the CONNECT restore path installs for an existing entry
	// through processJSConsumer's own fall-through. Plain filters only: draining is a
	// plain-consumer mechanism. And positive QoS only, because the reclaim ADOPTS and a
	// QoS 0 re-SUBSCRIBE deliberately does not — the drain keeps draining independently
	// while the raw subscription delivers new messages at QoS 0.
	reclaimDrainDelivery := func(f *mqttFilter, subject, sid string) error {
		if !fromSubProto || f.shared || f.qos == 0 || f.qos >= mqttSubAckFailure {
			return nil
		}
		sess.mu.Lock()
		d, err := sess.reclaimDrainDeliveryLocked(c, subject, sid, f.qos, f.noLocal, f.retainAsPublished, f.subID)
		if d != nil {
			// Recorded as a state the rollback must RESTORE, not undo. See
			// retainRepairedDrain.
			snap.retainRepairedDrain(d)
		}
		sess.mu.Unlock()
		return err
	}

	// Teardown of consumers removed by a QoS downgrade in this packet is deferred until
	// the final session save is confirmed, so a failed save can restore the pre-packet
	// consumer instead of having lost its durable/cursor. Spec5 [4.8.2].
	//
	// And it is irreversible only where the consumer is DESTROYED. One that still holds
	// started deliveries becomes a DRAIN instead, at the final-update boundary: the
	// client's exchange survives the downgrade because [MQTT-3.8.4-4] forbids
	// interrupting the flow of publications. See convertDowngradeCandidatesLocked.
	//
	// Each entry also carries what its deferral staged — the barrier it lowered, the
	// verdicts it recorded, and its DISPOSITION (destroy, or convert into a drain) — so
	// the list is the SINGLE authority on "this consumer is being removed by this packet
	// and what becomes of it". See mqttDeferredDel for why the two parallel
	// slices that used to hold those facts are gone.
	var deferredDels []*mqttDeferredDel
	// Registrations this packet withdraws, cancelled inside its own transaction. Unlike
	// the barrier and the verdicts these are NOT withdrawn by a revocation: the
	// allocation is already destroyed, so its tombstone still owes the packet's write.
	var stagedCancels []mqttRegCancellation
	// The sids the CURRENT filter revoked, reset at the top of each iteration alongside
	// filterDefStart. A wildcard filter can revoke on both halves, so it holds up to two.
	// Its consumer is the per-filter cancellation, which owes each of them a fresh
	// staging: the reviving occurrence failed, so the earlier occurrence's QoS 0
	// membership stands and the downgrade is owed again.
	var revokedThisFilter []string

	// liveDeferredDel is the packet's staged-downgrade lookup for one sid, and the reason
	// at most one LIVE entry can exist per sid: a staging consults it first and stages
	// nothing when one is already there. That is a lookup, not a deduction — the barrier
	// argument alone would be too strong, because a second voidRegistrationsLocked WOULD
	// still find the transmitted entries and re-record verdicts already recorded.
	//
	// It also removes a pre-existing double CONSUMER.DELETE for [{foo,0},{foo,0}], where
	// two entries for one config ran the irreversible teardown twice.
	liveDeferredDel := func(sid string) *mqttDeferredDel {
		for _, dd := range deferredDels {
			if dd.sid == sid && !dd.revoked {
				return dd
			}
		}
		return nil
	}
	// mayDrain answers "may this packet convert a downgraded consumer into a drain at all",
	// and it is the ELIGIBILITY half of that question only. Evaluated ONCE per packet and
	// lazily, on the first staging: the precondition is a property of the messages stream,
	// not of a filter, and drainingAllowed does a stream lookup with no lock held. The
	// UNSUBSCRIBE path checks it once per packet for the same reason.
	//
	// False means the stream imposes a consumer inactivity limit (or it cannot be read), so
	// a drain would be reaped with its started messages: the downgrade falls back to today's
	// destructive teardown and says so, which is a stated residual in docs/CONFORMANCE.md.
	mayDrainKnown, mayDrainOK := false, false
	mayDrain := func() bool {
		if !mayDrainKnown {
			mayDrainOK, mayDrainKnown = sess.drainingAllowed(c), true
		}
		return mayDrainOK
	}
	// stageDeferredDel records one deferred teardown and everything it stages, or reports
	// that this packet has already staged this sid.
	stageDeferredDel := func(sid string, cc *ConsumerConfig) {
		if liveDeferredDel(sid) != nil {
			return
		}
		dd := &mqttDeferredDel{sid: sid, cc: cc, convert: mayDrain()}
		// The teardown itself is deferred to the confirmed save, but the CANCELLATION
		// is not: it belongs in the write that downgrades this filter, or the
		// identifier it withdraws has nothing to recover it. The delivery barrier
		// comes first, so nothing allocates a further identifier behind the snapshot;
		// a rollback — or a revocation — reopens it, because the QoS 1/2 subscription
		// is then still the client's.
		sess.mu.Lock()
		cancels, sub, sent := sess.stageDeferredDeleteLocked(c, cc, dd.convert)
		sess.mu.Unlock()
		dd.closed, dd.voided = sub, sent
		stagedCancels = append(stagedCancels, cancels...)
		deferredDels = append(deferredDels, dd)
		mqttTestSubPauseHook("staged:" + sid)
	}

	// revokeStagedDowngrade undoes a downgrade THIS packet staged for sid, when this
	// later occurrence of the same filter re-establishes QoS > 0. Called at the same two
	// points as reclaimDrainDelivery and for the same reason: it answers the same shape
	// of question — may this filter be granted delivery it cannot actually serve? — and
	// processJSConsumer, which cannot see the packet's stack, is where the grant is
	// otherwise made blind.
	//
	// The two never apply to one sid at the same moment, and the invariant is TEMPORAL
	// rather than I7's structural one: a staged downgrade holds its cons entry for the
	// whole filter loop, and the conversion that turns it into a drain runs after the last
	// occurrence — so no revocation can follow it. Their order here is a readability
	// choice. (I7 alone used to be the argument, and a downgrade that becomes a drain
	// makes one sid reachable through both maps over the packet's lifetime.)
	//
	// The QoS condition lives at the CALL SITE only. A second copy inside would be a
	// guard no test can pin: a QoS 0 occurrence that revoked would immediately re-stage
	// through the same site, leaving nothing observable.
	revokeStagedDowngrade := func(f *mqttFilter, sid string) error {
		if !fromSubProto || f.shared || f.qos == 0 || f.qos >= mqttSubAckFailure {
			return nil
		}
		sess.mu.Lock()
		dd := liveDeferredDel(sid)
		var err error
		if dd != nil {
			err = dd.revokeLocked(c, sess, f.qos, f.noLocal, f.retainAsPublished, f.subID)
		}
		sess.mu.Unlock()
		if dd != nil && err == nil {
			revokedThisFilter = append(revokedThisFilter, sid)
			// Before processJSConsumer's refresh arm re-applies the same four fields,
			// so a test can tell the revocation's write from the refresh's.
			mqttTestSubPauseHook("revoked:" + sid)
		}
		return err
	}

	var err error
	subs := make([]*subscription, 0, len(filters))
	for _, f := range filters {
		// Skip what's already been identified as a failure (0x80 or any v5
		// error reason code).
		if f.qos >= mqttSubAckFailure {
			continue
		}
		subject := f.filter
		bsubject := []byte(subject)
		sid := f.sessionKey()
		bsid := []byte(sid)
		isReserved := isMQTTReservedSubscription(subject)

		// For a shared subscription the raw QoS0 subscription is a NATS queue
		// subscription so a QoS0-published message reaches exactly one group
		// member. The queue name is derived (with the level-up subject handled
		// below) and doubles as the JS consumer DeliverGroup. Spec5 [4.8.2].
		var queue []byte
		if f.shared {
			queue = []byte(mqttSharedQueueName(f.group, subject))
		}

		// Retain Handling is per-subscription, but the preloaded rms is shared
		// across all filters in this SUBSCRIBE and matched against each sub's
		// subject at serialization time. Gating only the preload would still leak
		// a retained message into an RH=1/2 filter when an overlapping filter
		// (e.g. a wildcard with RH=0) caused that subject to be loaded. So pass
		// rms only to filters that must replay; nil otherwise. sess.subs is
		// unchanged until sess.update at the end, so this matches the preload
		// decision. Shared subscriptions never replay retained. Spec5 [3.8.3.1], [4.8.2].
		subRMS := rms
		if f.shared || !mqttShouldSendRetained(f.retainHandling, sess.subs, f.filter) {
			subRMS = nil
		}

		// For a shared subscription persist the membership — the acquisition references
		// J1 reserved on $MQTT_shared — BEFORE creating the group consumer, so a restore
		// after a crash here finds what to repair and a rollback finds what to release.
		// Snapshot the prior state so a later failure of this filter restores it rather
		// than leaving a phantom member. Spec5 [4.8.2].
		var priorM mqttSharedMembership
		var priorExisted, priorRap bool
		var priorSubID int
		if fromSubProto && f.shared {
			priorM, priorExisted = sess.sharedSubs[sid]
			if priorExisted {
				_, priorRap = sess.rapSubs[sid]
				priorSubID = sess.subIDs[sid]
			}
			// ACQUIRE OR REUSE. A key with a standing membership REUSES its acquisition
			// references — a re-SUBSCRIBE changes the QoS or the options, never the
			// incarnation it holds — so the ledger and the group count do not move. A key
			// without one ACQUIRES: J0 admits the whole filter against the charge ceiling,
			// then J1 reserves one acquisition per consumer the filter needs on
			// $MQTT_shared under this CONNECT's tenure, BEFORE the membership save below
			// (J2) commits them to the record. The record must carry the acquisition
			// references before anything is materialized, so a restore after a crash finds
			// what to repair and a rollback finds what to release.
			if priorExisted {
				reused := priorM
				reused.QoS = f.qos
				f.sharedM = &reused
			} else {
				acquired, aerr := sess.acquireSharedFilter(c, f, subject)
				if aerr != nil {
					f.qos = mqttSubAckFailure
					f.reasonStr = aerr.Error()
					if acquired != nil {
						// Something LANDED and the filter still fails: the acquisition is owed a
						// release verdict, written and executed by the same compensation a
						// failed membership save takes (the membership is not in the record,
						// so the verdict is derived from the filter's own references).
						f.sharedM = acquired
						if _, rbCAS, rerr := sess.rollbackSharedSub(c, tok, f, fromSubProto, false, mqttSharedMembership{}, false, 0); rbCAS || rerr != nil {
							if rbCAS {
								facts.recordCASRejected = true
							}
							if rerr != nil {
								return subs, facts, rerr
							}
						}
					}
					continue
				}
				f.sharedM = acquired
			}
			wasUncertain := sess.isSeqUncertain()
			detail, err := sess.updateWithDetail(tok, []*mqttFilter{f}, true)
			if err != nil {
				// A CAS REJECTION here is the one failure that carries ownership
				// evidence: the expected-sequence write was refused, so the record
				// demonstrably MOVED. It is recorded BEFORE the arms below diverge,
				// because the definite arm turns the failure into a per-filter code and
				// continues — after which neither the trigger nor `err` carries any
				// trace of it.
				//
				// Read from the DETAIL, never from `err`: persistRecord formats the
				// store error with %v and destroys its NATS identity. And not from
				// saveDefinitelyFailed, which merges a CAS rejection with a local
				// failure and a refused transaction. See
				// mqttSubPacketFacts.recordCASRejected.
				if mqttIsRecordCASRejection(detail.rawErr) {
					facts.recordCASRejected = true
				}
				if sess.classifySave(wasUncertain, err) != saveDefinitelyFailed {
					// POSSIBLY LANDED. Continuing would be worse than failing: the final
					// update() skips filters marked failed, so the client would get a
					// failure code for a membership that is durably stored, and the rest
					// of the packet would run on a session whose record state is unknown
					// (every later save is refused while it is unresolved). Abandon the
					// packet with the attempted state intact — that is what the resolver
					// re-attempts and proves durable — and let the connection close. The
					// membership then simply stands, SUBACK or not, which is the outcome a
					// landed original would have produced anyway; nothing was deferred for
					// want of proof, so there is no landed continuation to register.
					// Spec5 [4.8.2]. See sess_uncertain.go.
					c.Errorf("MQTT: shared membership write for %q is unresolved; abandoning the SUBSCRIBE with its state intact: %v", sess.id, err)
					return nil, facts, err
				}
				f.qos = mqttSubAckFailure
				f.reasonStr = err.Error()
				// update() mutated the in-memory maps before its save failed; revert
				// them so a later save in this packet cannot persist a phantom
				// membership. Spec5 [4.8.2].
				sess.restoreSharedKeyInMemory(sid, priorExisted, priorM, priorRap, priorSubID)
				continue
			}
		}

		var jscons *ConsumerConfig
		var jssub *subscription

		// Note that if a subscription already exists on this subject, the
		// existing sub is returned. Need to update the qos.
		var sub *subscription
		var err error

		// Group-consumer charges THIS filter added to tmaxack, so a per-filter
		// failure on the CONNECT restore path refunds exactly them. Recorded here
		// rather than derived from the returned config, because that config comes
		// back whether this call charged or merely reused a resident entry, and
		// refunding by durable would erase an EARLIER connection's charge and leave
		// tmaxack permanently short. See sess_restore_gate.go.
		var filterCharged []mqttSharedCharge

		// Whether THIS filter touched shared delivery interest, in either half. Promoted
		// to the packet aggregate only once the whole filter succeeds.
		var filterInstalled bool

		// Deferred consumer deletes recorded by THIS filter, so a later per-filter
		// failure can cancel them (restore the sess.cons entry, keep the durable).
		// One boundary now, because each entry carries what its deferral staged: the
		// delivery barrier it lowered and the verdicts it recorded. A filter that FAILS
		// keeps its original QoS 1/2 membership — the final update simply skips it — so
		// a barrier left down would silently stop that subscription delivering until the
		// client reconnects.
		filterDefStart := len(deferredDels)
		revokedThisFilter = revokedThisFilter[:0]
		cancelFilterDeferred := func() {
			// The sess.cons entries are still there — the release runs at the
			// packet's final-update boundary, which is later than any per-filter
			// cancellation can be — so cancelling this filter's own stagings is just
			// dropping their descriptors and undoing what they staged. Both undos are
			// idempotent, so a revoked entry passing through either is inert.
			var reopen []*subscription
			var unvoid []string
			for _, dd := range deferredDels[filterDefStart:] {
				if dd.closed != nil {
					reopen = append(reopen, dd.closed)
				}
				unvoid = append(unvoid, dd.voided...)
			}
			sess.reopenTeardownDelivery(reopen)
			sess.unvoidRegNonces(unvoid)
			deferredDels = deferredDels[:filterDefStart]
			// And every downgrade this filter REVOKED is owed again: the revival
			// failed, so sess.update skips it and the earlier occurrence's QoS 0
			// membership stands. Re-staged through the same helper rather than by
			// restoring the saved barrier and verdicts, because between the revocation
			// and this failure the barrier was OPEN and the durable live — new
			// deliveries may have been tracked and new identifiers allocated, and only a
			// fresh withdrawal takes them into the packet's own membership-removal write.
			//
			// WHICH withdrawal depends on the disposition, and the helper decides: a
			// consumer being DESTROYED takes voidRegistrationsLocked, verdicts and all,
			// while a conversion CANDIDATE takes the cancel arm alone and leaves its
			// transmitted registers untouched, because those exchanges are about to be
			// drained rather than abandoned.
			for _, sid := range revokedThisFilter {
				dd := func() *mqttDeferredDel {
					for _, d := range deferredDels {
						if d.sid == sid && d.revoked {
							return d
						}
					}
					return nil
				}()
				if dd == nil {
					continue
				}
				dd.revoked = false
				sess.mu.Lock()
				cancels, sub, sent := sess.stageDeferredDeleteLocked(c, dd.cc, dd.convert)
				sess.mu.Unlock()
				dd.closed, dd.voided = sub, sent
				stagedCancels = append(stagedCancels, cancels...)
			}
			revokedThisFilter = revokedThisFilter[:0]
		}

		const processShadowSubs = true

		as.mu.Lock()
		sess.mu.Lock()
		sub, err = sess.processSub(c,
			bsubject, bsid, queue, mqttNoLocalFilterKey(f), isReserved, f.qos, f.noLocal, f.retainAsPublished, f.shared, f.subID, // main subject
			_EMPTY_, mqttDeliverMsgCbQoS0, // no jsDur for QOS0
			processShadowSubs,
			subRMS, trace, as)
		sess.mu.Unlock()
		as.mu.Unlock()

		if err == nil {
			err = as.filterExitFail(fromSubProto, "raw", sid)
		}
		if err != nil {
			f.qos = mqttSubAckFailure
			f.reasonStr = err.Error()
			cancelFilterDeferred()
			doCleanup, rbCAS, rerr := sess.rollbackSharedSub(c, tok, f, fromSubProto, priorExisted, priorM, priorRap, priorSubID)
			if rbCAS {
				facts.recordCASRejected = true
			}
			if rerr != nil {
				// Rollback persistence not confirmed: hard-error WITHOUT releasing
				// resources, so we never delete a group durable the record may still
				// claim membership in. Spec5 [4.8.2].
				return subs, facts, rerr
			}
			if doCleanup {
				if sess.cleanupFailedSubClassified(c, tok, fromSubProto, sub, jscons, jssub) {
					facts.recordCASRejected = true
				}
			}
			sess.refundSharedCharges(fromSubProto, filterCharged)
			continue
		}

		// This will create (if not already exist) a JS consumer for
		// subscriptions of QoS >= 1. But if a JS consumer already exists and
		// the subscription for same subject is now a QoS==0, then the JS
		// consumer will be deleted. Shared subscriptions always join the group
		// consumer (even at QoS0, so stored messages are delivered at QoS0).
		if f.shared {
			var jr mqttSharedJoinResult
			jr, err = sess.processSharedJSConsumer(c, f.group, subject, sid, f.sharedM.Main, f.qos, f.retainAsPublished, f.subID, fromSubProto, sharedVerdicts)
			jscons, jssub = jr.cc, jr.sub
			filterCharged = appendSharedCharge(filterCharged, mqttSharedNamesDurable(f.group, subject, f.sharedM.Main.Inc), jr.charged)
			filterInstalled = filterInstalled || jr.installed
		} else {
			var dd *ConsumerConfig
			// A failed revocation or reclaim SKIPS provisioning and carries its error
			// into the per-filter arm below: by here the raw QoS 0 subscription is
			// already installed, so a bare continue would strand it.
			if err = revokeStagedDowngrade(f, sid); err == nil {
				if err = reclaimDrainDelivery(f, subject, sid); err == nil {
					jscons, jssub, dd, err = sess.processJSConsumer(c, tok, subject, sid, mqttNoLocalFilterKey(f), f.qos, f.noLocal, f.retainAsPublished, f.subID, fromSubProto)
				}
			}
			if dd != nil {
				stageDeferredDel(sid, dd)
			}
		}
		if err == nil {
			err = as.filterExitFail(fromSubProto, "consumer", sid)
		}
		if err != nil {
			f.qos = mqttSubAckFailure
			f.reasonStr = err.Error()
			cancelFilterDeferred()
			doCleanup, rbCAS, rerr := sess.rollbackSharedSub(c, tok, f, fromSubProto, priorExisted, priorM, priorRap, priorSubID)
			if rbCAS {
				facts.recordCASRejected = true
			}
			if rerr != nil {
				// Rollback persistence not confirmed: hard-error WITHOUT releasing
				// resources, so we never delete a group durable the record may still
				// claim membership in. Spec5 [4.8.2].
				return subs, facts, rerr
			}
			if doCleanup {
				if sess.cleanupFailedSubClassified(c, tok, fromSubProto, sub, jscons, jssub) {
					facts.recordCASRejected = true
				}
			}
			sess.refundSharedCharges(fromSubProto, filterCharged)
			continue
		}

		// Process the wildcard subject if needed.
		if need, fwcsubject, fwcsid := fwc(subject, sid); need {
			var fwjscons *ConsumerConfig
			var fwjssub *subscription
			var fwcsub *subscription
			var fwcqueue []byte
			if f.shared {
				fwcqueue = []byte(mqttSharedQueueName(f.group, fwcsubject))
			}

			// See note above about existing subscription.
			as.mu.Lock()
			sess.mu.Lock()
			fwcsub, err = sess.processSub(c,
				[]byte(fwcsubject), []byte(fwcsid), fwcqueue, mqttNoLocalFilterKey(f), isReserved, f.qos, f.noLocal, f.retainAsPublished, f.shared, f.subID, // FWC (top-level wildcard) subject: the MAIN filter's key
				_EMPTY_, mqttDeliverMsgCbQoS0, // no jsDur for QOS0
				processShadowSubs,
				subRMS, trace, as)
			sess.mu.Unlock()
			as.mu.Unlock()
			if err == nil {
				err = as.filterExitFail(fromSubProto, "raw-fwc", fwcsid)
			}
			if err != nil {
				// c.processSub already called c.Errorf(), so no need here.
				f.qos = mqttSubAckFailure
				f.reasonStr = err.Error()
				cancelFilterDeferred()
				doCleanup, rbCAS, rerr := sess.rollbackSharedSub(c, tok, f, fromSubProto, priorExisted, priorM, priorRap, priorSubID)
				if rbCAS {
					facts.recordCASRejected = true
				}
				if rerr != nil {
					return subs, facts, rerr
				}
				if doCleanup {
					if sess.cleanupFailedSubClassified(c, tok, fromSubProto, sub, jscons, jssub) {
						facts.recordCASRejected = true
					}
				}
				sess.refundSharedCharges(fromSubProto, filterCharged)
				continue
			}

			if f.shared {
				var fwjr mqttSharedJoinResult
				fwjr, err = sess.processSharedJSConsumer(c, f.group, fwcsubject, fwcsid, *f.sharedM.Up, f.qos, f.retainAsPublished, f.subID, fromSubProto, sharedVerdicts)
				fwjscons, fwjssub = fwjr.cc, fwjr.sub
				filterCharged = appendSharedCharge(filterCharged, mqttSharedNamesDurable(f.group, fwcsubject, f.sharedM.Up.Inc), fwjr.charged)
				filterInstalled = filterInstalled || fwjr.installed
			} else {
				var fwdd *ConsumerConfig
				// Same revocation and reclaim for the level-up companion, which owns a
				// drain and a staged downgrade of its own. Its failure arm cleans BOTH
				// raw subscriptions and any main consumer this filter created, which is
				// why the error is carried there rather than short-circuiting.
				if err = revokeStagedDowngrade(f, fwcsid); err == nil {
					if err = reclaimDrainDelivery(f, fwcsubject, fwcsid); err == nil {
						fwjscons, fwjssub, fwdd, err = sess.processJSConsumer(c, tok, fwcsubject, fwcsid, mqttNoLocalFilterKey(f), f.qos, f.noLocal, f.retainAsPublished, f.subID, fromSubProto)
					}
				}
				if fwdd != nil {
					// The same staging as the main half, through the same helper: the
					// companion is a consumer in its own right and owes exactly what it
					// does.
					stageDeferredDel(fwcsid, fwdd)
				}
			}
			if err == nil {
				err = as.filterExitFail(fromSubProto, "consumer-fwc", fwcsid)
			}
			if err != nil {
				// c.processSub already called c.Errorf(), so no need here.
				f.qos = mqttSubAckFailure
				f.reasonStr = err.Error()
				cancelFilterDeferred()
				doCleanup, rbCAS, rerr := sess.rollbackSharedSub(c, tok, f, fromSubProto, priorExisted, priorM, priorRap, priorSubID)
				if rbCAS {
					facts.recordCASRejected = true
				}
				if rerr != nil {
					return subs, facts, rerr
				}
				if doCleanup {
					if sess.cleanupFailedSubClassified(c, tok, fromSubProto, sub, jscons, jssub) {
						facts.recordCASRejected = true
					}
					if sess.cleanupFailedSubClassified(c, tok, fromSubProto, fwcsub, fwjscons, fwjssub) {
						facts.recordCASRejected = true
					}
				}
				sess.refundSharedCharges(fromSubProto, filterCharged)
				continue
			}

			subs = append(subs, fwcsub)
			addJSConsToSess(fwcsid, fwjscons)
		}

		subs = append(subs, sub)
		addJSConsToSess(sid, jscons)
		// THE COMMIT BOUNDARY. Both halves are installed and every per-filter failure exit
		// above has been passed, so this filter's interest is the client's.
		facts.interestRestored = facts.interestRestored || filterInstalled
		if fromSubProto {
			// The LIVE counterpart, and it exists for one thing a test cannot otherwise
			// schedule: a packet carrying the same filter twice, where a delivery must be
			// transmitted through the consumer the FIRST occurrence created before the
			// second occurrence downgrades it. Without that transmission the second
			// occurrence has nothing started, so no conversion happens and a test named
			// after the conversion would pass against a broker that never converts.
			mqttTestSubPauseHook("provisioned:" + sid)
		}
		if !fromSubProto {
			// One filter's installs are complete and later filters are not started.
			// The seam is here rather than at the end of the restore because that is
			// the only place a test can tell "installed behind the connection's
			// restore verdict" from "installed live" for a SPECIFIC subscription.
			mqttTestRestorePauseHook("filter:" + sid)
		}
	}

	if fromSubProto {
		wasUncertain := sess.isSeqUncertain()
		// The QoS 0 downgrades this packet staged become DRAINS here rather than being
		// destroyed, inside the same critical section that applies the membership and
		// carried by the same record: either the filter is at QoS 0 and its drain exists,
		// or neither is persisted. [MQTT-3.8.4-3] requires the replacement to be accepted
		// without interrupting the flow of publications, so an exchange already
		// transmitted must still complete. See unsub_drain.go.
		var conv mqttDowngradeConversion
		convert := func() bool {
			conv = sess.convertDowngradeCandidatesLocked(c, deferredDels)
			// And the downgrades that are NOT becoming drains give up their cons entry
			// here, so this record does not name a durable the teardown below deletes.
			// Its result is ORed into the return: the membership change usually forces
			// the save on its own, but a QoS 0 re-SUBSCRIBE over a filter ALREADY at
			// QoS 0 changes no membership, and without this the entry would leave
			// memory while the record kept naming it. See
			// releaseDowngradedConsumersLocked.
			released := sess.releaseDowngradedConsumersLocked(deferredDels)
			return conv.any() || released
		}
		// The fallback arm's tombstones ride the packet's own securing below, so they are
		// appended once the conversion has run.
		//
		// Assigned unconditionally, in HEAD's shape. A review round read the conditional
		// form as able to leave a stale per-filter error standing into a SUCCESSFUL save;
		// it cannot, because every per-filter failure arm assigns an err SHADOWED inside
		// the loop body (declared above the provisioning block) and the shared
		// early-persist arm's comes from its own `:=`, so this outer err is always nil
		// here. Kept unconditional anyway: the equivalence is a scoping accident, and one
		// that a later edit moving a failure arm outside the shadow would silently
		// destroy.
		_, err = sess.updateWithDetailConverting(tok, filters, true, convert)
		stagedCancels = append(stagedCancels, conv.cancels...)
		if err != nil {
			// The session record could not be persisted. Rather than leave a
			// partially-applied SUBSCRIBE live with no SUBACK sent, tear down every
			// subscription and consumer newly created in this packet, restore the
			// pre-packet membership and consumer entries (deferred downgrade deletes
			// were never executed, so their durables are intact, and any conversion is
			// un-converted first so the enumeration below can see it), then return the
			// error so the connection closes. Spec5 [4.8.2].
			//
			// ONLY when the write definitely did not land. An ambiguous one may have
			// stored exactly the state this packet built, so the packet's state is KEPT
			// and the resolver settles it by re-attempting those very bytes — the
			// subscriptions stand, with or without a SUBACK, exactly as they would have
			// had the original landed with its response lost. Nothing is torn down
			// meanwhile either: the consumers and subscriptions this packet created may
			// be precisely what the stored record references. The connection closes
			// regardless, and the session stays unresolved until the resolver classifies
			// it. See sess_uncertain.go.
			if sess.classifySave(wasUncertain, err) == saveMaybeLanded {
				// Not resolved here: the packet's transaction is still open and the
				// resolver must hold the fence. The readLoop resolves after it closes.
				// See mqttResolveAfterPacket.
				//
				// The converted drains stay STAGED and their delivery stays closed:
				// nothing is committed, reopened or deleted until those exact bytes are
				// proven durable. settleLandedAttempt runs this under the resolver's own
				// token; a FORKED verdict discards it, which is right, because a
				// continuation's premise is that its write landed.
				if conv.any() {
					sess.retainOnLanded(func(recordTx) { sess.finishDowngradeDrains(c, conv) })
				}
				// The retention's settlement obligation travels with the attempt, not with
				// this arm: the record MAY hold this packet's membership, so releasing now
				// would drop a retention the stored bytes never accounted for, and holding
				// it forever leaks the object. It settles when those exact bytes are proven
				// (retention lifecycle step 4), fenced by the incarnation the continuation
				// runs under.
				sess.retainOnLanded(func(recordTx) {
					sess.mu.Lock()
					sess.settleAdoptedRetentionsLocked()
					sess.mu.Unlock()
				})
				// A RELEASED destructive downgrade owes its MaxAckPending charge back, and
				// this is the only arm where nothing else gives it: the teardown below is
				// never reached, and the sweep that reaps the durable has no session to
				// refund. Refund only — the cons entry stays gone, because re-persisting it
				// under the QoS 0 membership is the defect this item closed. See
				// refundReleasedDowngrades.
				if refunds := mqttReleasedDowngradeDurables(deferredDels); len(refunds) > 0 {
					sess.retainOnLanded(func(recordTx) { sess.refundReleasedDowngrades(refunds) })
				}
				c.Errorf("MQTT: SUBSCRIBE record write for %q is unresolved; keeping the packet's state: %v", sess.id, err)
				return nil, facts, err
			}
			var reopen []*subscription
			var unvoid []string
			for _, dd := range deferredDels {
				if dd.closed != nil {
					reopen = append(reopen, dd.closed)
				}
				unvoid = append(unvoid, dd.voided...)
			}
			sess.reopenTeardownDelivery(reopen)
			sess.unvoidRegNonces(unvoid)
			// BEFORE the rollback: it discovers this packet's own consumers by walking
			// sess.cons, and a converted one has left that map — so without this a
			// consumer the packet CREATED and then converted is neither restored nor
			// released, and its durable ends up named nowhere. See
			// unconvertDowngradeDrainsLocked.
			sess.mu.Lock()
			sess.unconvertDowngradeDrainsLocked(conv)
			// Same reason, other half: the rollback finds this packet's own consumers by
			// walking sess.cons, and a released downgrade has left it, so a durable this
			// packet CREATED and then downgraded would be named nowhere in the
			// rolled-back record and released by nobody.
			sess.restoreDowngradedConsumersLocked(deferredDels)
			sess.mu.Unlock()
			sess.rollbackSubscribePacket(c, tok, subs, consSnap, snap, q2AffPrePacket)
			sess.secureRegCancellations(c, tok, stagedCancels, mqttLicenceOf(c))
		} else {
			// Save confirmed: now run the irreversible teardown of the consumers that
			// this packet downgraded to QoS 0 and did NOT convert into drains.
			// Durable in the packet's record already; this lets the registers go now
			// rather than at the next resume.
			//
			// Both record writes below are reached on an OTHERWISE-SUCCESSFUL packet,
			// so a CAS rejection in either is precisely the swallowed evidence that
			// would let the gate open as "quiet" while the session's surviving shared
			// subscriptions serve under a membership this broker can no longer speak for.
			// The
			// classified forms exist so the bare bool — which merges a CAS rejection
			// with a local failure and an ambiguity — cannot hide it. See
			// mqttSubPacketFacts.recordCASRejected.
			if _, casRejected := sess.secureRegCancellationsClassified(c, tok, stagedCancels, mqttLicenceOf(c)); casRejected {
				facts.recordCASRejected = true
			}
			for _, dd := range deferredDels {
				// A REVOKED staging has no teardown left to run: a later occurrence of
				// this filter re-established QoS > 0, so the record ends naming this
				// durable and the client is being served by it. The entry stays in the
				// list rather than being removed, so a per-filter cancellation can
				// still find it. This loop runs after the confirmed save, on the read
				// loop, so the flag needs no lock of its own.
				//
				// A CONVERTED staging likewise has no teardown left: its durable is a
				// drain now, the record names it in Drains, and its started deliveries
				// are still owed to the client. Running the teardown here would delete
				// the very consumer the conversion preserved.
				if dd.revoked || dd.converted {
					continue
				}
				if sess.execDeferredConsumerDeleteClassified(c, tok, dd.sid, dd.cc, mqttDisposalDowngraded) {
					facts.recordCASRejected = true
				}
			}
			// The drains this packet created are durable now, so they may be committed
			// and their delivery reopened. Inline here; the ambiguous arm above defers the
			// same call to the resolver instead.
			sess.finishDowngradeDrains(c, conv)
			// The packet's record is CONFIRMED, so every retained adoption it accounted
			// for can be settled: a sid that gained membership is named in Cons, one that
			// became a drain is named in Drains, and one whose consumer was deleted or
			// replaced by this packet is committed. Only here — an unconfirmed outcome
			// leaves the obligation standing, which is what lets a definite failure
			// restore the consumer with its retention intact.
			sess.mu.Lock()
			sess.settleAdoptedRetentionsLocked()
			sess.mu.Unlock()
			// The affine path is provisioned in the first loop, before any filter is
			// known to succeed, so a packet whose shared QoS 2 filters all failed
			// per-filter (a failed membership persist, a failed group-consumer
			// create, a failed subscription) would otherwise keep a consumer, a
			// generation and a MaxAckPending charge for a subscription that does not
			// exist. Only ever unwinds a path THIS packet built. Spec5 [4.8.2].
			//
			// Not while the session is UNRESOLVED. The confirming save below is
			// refused at entry then, so the unwind would terminate the generation's
			// assignments — which nothing undoes — and then restore a path it never
			// managed to remove. The write that left the session unresolved is the
			// provisioning save itself, and it registered a continuation that owns
			// this reclaim and runs it under the resolver's token once the record is
			// proven. See unwindProvisionedQ2AffineOnLanded.
			if !q2AffPrePacket && !sess.isSeqUncertain() && !sess.hasSharedQoS2Sub() {
				// Same commit order as the rollback path: detach, persist a record
				// that no longer names it, and only then delete.
				// disposeNow: this site's failure semantics are unchanged — a failed
				// claim cleanup aborts the unwind and leaves the path usable. Moving it
				// to dispose-after-confirm is filed as its own §0 item, because the
				// rollback around it has distinct failure semantics of its own.
				if u := sess.prepareQ2AffineUnwind(c, true); u.prepared {
					outcome, udetail, serr := sess.saveClassifiedDetail(tok)
					// serr never travels outward from here — every arm below logs it
					// and returns normally — so the packet records the fact itself, and
					// only for the failure that carries ownership evidence. See
					// mqttSubPacketFacts.recordCASRejected.
					if mqttIsRecordCASRejection(udetail.rawErr) {
						facts.recordCASRejected = true
					}
					switch {
					case serr == nil:
						sess.finishQ2AffineUnwind(c, u)
					case outcome == saveDefinitelyFailed:
						// Nothing stored, so the session must go back to describing the
						// resources that are still live.
						sess.restoreQ2AffineUnwind(u)
						c.Errorf("MQTT: unable to persist the removal of an unused affine QoS 2 path for %q: %v", sess.id, serr)
					default:
						// Ambiguous: the record may already omit the path. Keep the
						// detached view (it matches what may have landed) but delete
						// nothing — the resources stay live until the resolver proves the
						// record no longer names them, which is what authorizes the
						// delete.
						sess.retainOnLanded(func(recordTx) { sess.finishQ2AffineUnwind(c, u) })
						c.Errorf("MQTT: the removal of an unused affine QoS 2 path for %q is unresolved: %v", sess.id, serr)
						sess.resolveUncertainNow(c)
					}
				}
			}
		}
	}

	if !fromSubProto && len(deferredDels) > 0 {
		if cerr := sess.settleRestoredDowngrades(c, tok, deferredDels, stagedCancels, &facts); cerr != nil {
			return nil, facts, cerr
		}
	}

	return subs, facts, err
}
func (c *client) mqttParseSubs(r *mqttReader, b byte, pl int) (uint16, []*mqttFilter, error) {
	return c.mqttParseSubsOrUnsubs(r, b, pl, true)
}

func (c *client) mqttParseSubsOrUnsubs(r *mqttReader, b byte, pl int, sub bool) (uint16, []*mqttFilter, error) {
	var expectedFlag byte
	var action string
	if sub {
		expectedFlag = mqttSubscribeFlags
	} else {
		expectedFlag = mqttUnsubscribeFlags
		action = "un"
	}
	// Spec [MQTT-3.8.1-1], [MQTT-3.10.1-1]
	if rf := b & 0xf; rf != expectedFlag {
		return 0, nil, fmt.Errorf("%w: wrong %ssubscribe reserved flags: %x", errMQTTMalformedPacket, action, rf)
	}
	pi, err := mqttParsePIPacket(r)
	if err != nil {
		return 0, nil, err
	}
	// end is the absolute end of this packet. It is unaffected by the v5
	// properties block below, which only advances r.pos within [r.pos, end).
	end := r.pos + (pl - 2)
	// MQTT 5.0: a properties block follows the packet identifier in both
	// SUBSCRIBE and UNSUBSCRIBE. Spec5 [3.8.2.1], [3.10.2.1].
	// subID is the packet's single v5 Subscription Identifier (0 = none); it
	// applies to every filter the packet carries. Spec5 [3.8.2.1.2].
	var subID int
	if c.mqtt.proto == mqttProtoLevel5 {
		ctx := mqttPacketSub
		if !sub {
			ctx = mqttPacketUnsub
		}
		props, perr := r.readProperties(ctx)
		if perr != nil {
			return 0, nil, perr
		}
		// readProperties accepts repeated Subscription Identifiers because a
		// delivered PUBLISH may legitimately carry several; in a SUBSCRIBE more
		// than one is a Protocol Error. A value of 0 is already rejected there.
		// Spec5 [MQTT-3.8.2.1.2].
		if sub && props != nil {
			if len(props.subIDs) > 1 {
				return 0, nil, fmt.Errorf("%w: more than one subscription identifier in SUBSCRIBE", errMQTTProtocolError)
			}
			if len(props.subIDs) == 1 {
				subID = props.subIDs[0]
			}
		}
	}
	var filters []*mqttFilter
	for r.pos < end {
		// Don't make a copy now because, this will happen during conversion
		// or when processing the sub.
		topic, err := r.readBytes("topic filter", false)
		if err != nil {
			return 0, nil, err
		}
		if len(topic) == 0 {
			return 0, nil, errMQTTTopicFilterCannotBeEmpty
		}
		// Spec [MQTT-3.8.3-1], [MQTT-3.10.3-1]
		if err := mqttValidateTopic(topic, "topic filter"); err != nil {
			return 0, nil, err
		}
		// MQTT 5.0 Shared Subscription: "$share/{ShareName}/{filter}". v5 only;
		// for 3.1.1 "$share/..." has no special meaning and is treated as a
		// literal topic filter. Detect and strip the prefix + ShareName before
		// conversion so all downstream per-subject logic sees only the underlying
		// filter. Applies to both SUBSCRIBE and UNSUBSCRIBE. Spec5 [4.8.2].
		var shared bool
		var shareName []byte
		innerTopic := topic
		if c.mqtt.proto == mqttProtoLevel5 && bytes.HasPrefix(topic, mqttSharedSubPrefix) {
			rest := topic[len(mqttSharedSubPrefix):]
			idx := bytes.IndexByte(rest, mqttTopicLevelSep)
			// ShareName must be non-empty, must not contain '+' or '#', and must
			// be followed by a non-empty topic filter. Any violation is a
			// Malformed Packet (fail the whole packet). Spec5 [MQTT-4.8.2-1],
			// [MQTT-4.8.2-2].
			if idx <= 0 || idx == len(rest)-1 ||
				bytes.IndexByte(rest[:idx], mqttSingleLevelWC) >= 0 ||
				bytes.IndexByte(rest[:idx], mqttMultiLevelWC) >= 0 {
				return 0, nil, fmt.Errorf("%w: %q", errMQTTMalformedSharedSub, topic)
			}
			shared = true
			shareName = rest[:idx]
			innerTopic = rest[idx+1:]
		}
		var qos, reason byte
		var reasonStr string
		var retainHandling byte
		var noLocal bool
		var retainAsPublished bool
		// We are going to report if we had an error during the conversion,
		// but we don't fail the parsing. When processing the sub, we will
		// have an error then, and the processing of subs code will send
		// the proper mqttSubAckFailure flag for this given subscription.
		filter, err := mqttFilterToNATSSubject(innerTopic)
		if err != nil {
			c.Errorf("invalid topic %q: %v", topic, err)
			reason = mqttSubAckFailure
			reasonStr = err.Error()
		}
		if sub {
			if c.mqtt.proto == mqttProtoLevel5 {
				// v5 replaces the single QoS byte with a Subscription Options
				// byte: bits 0-1 QoS, 2 No Local, 3 Retain As Published, 4-5
				// Retain Handling, 6-7 reserved. Spec5 [3.8.3.1].
				var opts byte
				opts, err = r.readByte("subscription options")
				if err != nil {
					return 0, nil, err
				}
				// Reserved bits set is a Malformed Packet [MQTT-3-8.3-5].
				if opts&0xC0 != 0 {
					return 0, nil, fmt.Errorf("%w: reserved bits set in options byte 0x%x", errMQTTMalformedSubOption, opts)
				}
				retainHandling = (opts >> 4) & 0x03
				// Retain Handling 3 is reserved; sending it is a Protocol Error.
				// Checked before the No Local / Retain As Published rejection so a
				// reserved value is always classified as a Protocol Error, even
				// when those bits are also set. Spec5 [3.8.3.1].
				if retainHandling == 3 {
					return 0, nil, fmt.Errorf("%w: retain handling value 3 is reserved", errMQTTProtocolError)
				}
				noLocal = opts&0x04 != 0
				retainAsPublished = opts&0x08 != 0
				// No Local on a Shared Subscription is a Protocol Error
				// [MQTT-3.8.3-4]: fail the whole packet.
				if noLocal && shared {
					return 0, nil, fmt.Errorf("%w: No Local set on a shared subscription", errMQTTProtocolError)
				}
				qos = opts & 0x03
			} else {
				qos, err = r.readByte("QoS")
				if err != nil {
					return 0, nil, err
				}
			}
			// Spec [MQTT-3-8.3-4].
			if qos > 2 {
				return 0, nil, fmt.Errorf("%w: subscribe QoS value must be 0, 1 or 2, got %v", errMQTTMalformedSubOption, qos)
			}
		}
		f := &mqttFilter{ttopic: topic, filter: string(filter), qos: qos, retainHandling: retainHandling, noLocal: noLocal, retainAsPublished: retainAsPublished, subID: subID, reason: reason, reasonStr: reasonStr, shared: shared, group: string(shareName)}
		filters = append(filters, f)
	}
	// Spec [MQTT-3.8.3-3], [MQTT-3.10.3-2]
	if len(filters) == 0 {
		return 0, nil, fmt.Errorf("%w: %ssubscribe protocol must contain at least 1 topic filter", errMQTTProtocolError, action)
	}
	return pi, filters, nil
}

func mqttSubscribeTrace(pi uint16, filters []*mqttFilter) string {
	var sep string
	sb := &strings.Builder{}
	sb.WriteString("[")
	for i, f := range filters {
		sb.WriteString(sep)
		sb.Write(f.ttopic)
		sb.WriteString(" (")
		sb.WriteString(f.filter)
		sb.WriteString(") QoS=")
		sb.WriteString(fmt.Sprintf("%v", f.qos))
		if i == 0 {
			sep = ", "
		}
	}
	sb.WriteString(fmt.Sprintf("] pi=%v", pi))
	return sb.String()
}

// The results are NAMED, and that is load-bearing rather than a style choice. The
// arbitration tail runs in a DEFER — it has to, because it must observe the packet
// transaction closed and the ownership gate still held — and an unnamed result is COPIED at
// the return statement, before any defer runs. With unnamed results everything the tail
// decides is invisible to the caller: `settledOwnAmbiguity` never suppresses
// mqttResolveAfterPacket, so the generic resolver gets a second attempt and can apply its
// DISPOSING forked policy to an ambiguity this tail deliberately retained; and an eventual
// Foreign disposal never reports `disposed`, so mqttParse appends its own footer instead of
// staying silent for the DISCONNECT already sent — a second one on the same socket, which
// [MQTT-3.14.4-1] forbids. Named results give the defer the real return slot.
func (c *client) mqttProcessSubs(filters []*mqttFilter) (subs []*subscription, arb mqttSubArbitration, err error) {
	// Those things are immutable, but since processing subs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	trace := c.trace
	c.mu.Unlock()

	// The notify/close tail is registered FIRST so LIFO runs it LAST — after the record
	// transaction has ended AND after the ownership gate has been released. Notifying or
	// closing under the gate is what disposeForeignSessionLocked has to dodge by
	// dispatching its close to another goroutine; here the tail runs on this readLoop,
	// which is what mqttGateDispatchOnRejection does for the same reason.
	var tail func()
	defer func() {
		if tail != nil {
			tail()
		}
	}()

	if lerr := asm.lockSession(sess, c); lerr != nil {
		return nil, arb, lerr
	}
	defer asm.unlockSession(sess)

	// The operation gate is armed BEFORE processSubs, and therefore before
	// ensurePubRelConsumerSubscription and ensureQ2AffineConsumerSubscription install
	// anything — which is what bounds the residual to deliveries whose interest predates
	// this packet. Only for a packet that actually carries a shared filter: a purely plain
	// SUBSCRIBE authorises nothing on a group's behalf and must not gate its own traffic.
	//
	// The resolve is a DEFER on the exact instance armed here, so every error path and an
	// abnormal unwind resolves it — a test hook calling t.Fatal unwinds with
	// runtime.Goexit, which no explicit return covers. Its terminal state defaults to
	// WITHHELD: a packet that dies without establishing ownership must not leave its
	// deliveries free.
	var gate *mqttSubGate
	gateState := mqttSubGateWithheld
	if mqttFiltersCarryShared(filters) {
		gate = sess.armSubGate(c)
		defer func() { gate.resolve(gateState) }()
	}

	// The whole SUBSCRIBE is one record transaction: its membership changes, its
	// per-filter rollbacks and its final persist either all become visible or none
	// do. Outermost readLoop entry point, so it takes the blocking form.
	tok := sess.beginRecordTx()
	// Registered AFTER unlockSession and BEFORE endRecordTx, so LIFO gives exactly
	// endRecordTx -> this -> unlockSession -> tail: the eventual resolution runs with the
	// packet transaction CLOSED (it takes a fence of its own, and the fence cannot be
	// acquired twice on one goroutine) and the ownership gate STILL HELD, so nothing else
	// may classify this session's uncertainty meanwhile. See sub_operation_gate.go.
	var resolveEventually bool
	defer func() {
		if resolveEventually {
			c.mqttSettleLiveArbitration(asm, sess, &arb, &gateState, &tail)
		}
	}()
	// The DIRECT foreign arm's Will action, registered in the same slot and for the same
	// reason: it must observe the packet transaction CLOSED (mqttConsumeWillUnderGate does
	// JetStream work and the fence cannot be held across it) and the ownership gate STILL
	// HELD (its own precondition). LIFO puts it after endRecordTx and before unlockSession.
	// F-084.
	var willPhase func()
	defer func() {
		if willPhase != nil {
			willPhase()
		}
	}()
	defer func() {
		if terr := sess.endRecordTx(tok); terr != nil {
			c.Errorf("MQTT: %v", terr)
		}
	}()

	var facts mqttSubPacketFacts
	subs, facts, err = asm.processSubs(sess, c, tok, filters, true, trace, nil)
	if err != nil {
		// The packet already failed and its own error is what closes the connection. It
		// owes no ownership proof, and the gate stays WITHHELD so nothing it installed
		// serves on the way out.
		return subs, arb, err
	}

	// ARBITRATE. Everything above may have installed or revived shared group delivery
	// interest; nothing yet proved this broker still OWNS the membership it was keyed on
	// (F-078). One classified record write does, and only a write can: a load cannot
	// distinguish "the record did not move" from "a contender's proposal has not committed
	// yet".
	//
	// It is the LAST record write of the packet — after sess.update's final save and after
	// the optional affine unwind — so its CAS linearizes at the sequence the packet left.
	if !facts.ownershipQuestionRaised() {
		// PROVENLY QUIET: no trigger committed AND no record write CAS-rejected, so this
		// packet owes no proof and inherits the previous operation's landed ownership CAS.
		// Both halves are required — see mqttSubPacketFacts.ownershipQuestionRaised.
		//
		// THIS PRECEDES THE isSeqUncertain CHECK BELOW, DELIBERATELY, and a review round
		// read the ordering as a defect: a packet whose ancillary save went AMBIGUOUS
		// opens the gate while the session carries an unresolved write, and an unresolved
		// write is an outcome nobody read, so it includes "CAS-rejected, response lost".
		// The ordering stands because a quiet packet installed NOTHING — no trigger means
		// no shared interest was created or revived — so what this open releases is
		// traffic backed by interest that PREDATES the packet, authorised by whatever
		// operation installed it. Withholding it would give one packet type a stronger
		// delivery guarantee than the broker provides for any other ambiguous record
		// write, and it would convert an ambiguous affine provisioning failure into a
		// connection-level refusal — which is the contract the affine items own, the same
		// one the wider trigger predicate already broke once. That population is the
		// point-in-time-ownership convergence window in docs/CONFORMANCE.md, and F-078
		// makes no stronger guarantee for it. Mutating this order is M34.
		gateState = mqttSubGateOpen
		return subs, arb, nil
	}

	// A session already carrying an unresolved write cannot prove anything: the proof is
	// refused at entry. That is NOT a permission to serve, so it takes the Unresolved arm
	// and the tail resolves it under the still-held gate.
	if sess.isSeqUncertain() {
		resolveEventually = true
		return subs, arb, fmt.Errorf("session %q: %w", sess.id, errMQTTSessionOwnershipUnsettled)
	}

	v := sess.proveSharedMembershipOwnership(tok)
	mqttTestSharedArbitratePause("after-ownership-cas")
	switch v.outcome {
	case mqttGroupOwnershipOurs:
		// Still ours at the write's linearization point. Everything this packet installed
		// may serve, and the SUBACK follows.
		gateState = mqttSubGateOpen
		return subs, arb, nil

	case mqttGroupOwnershipForeign:
		// The ownership write was REJECTED by a record that is not ours. That establishes we
		// may not serve this group and must not write again; it does NOT establish who took
		// it, so the Will is CONSUMED here rather than discarded. Discarding was the old
		// behavior and it suppressed a Will on evidence that demonstrated no successor —
		// the same silent loss the Forked and Local arms below already refuse. F-084.
		//
		// Phase 1 prepares; the Will action runs in phase 2, after this packet's record
		// transaction is closed by the caller's defer and while the ownership gate is still
		// held, which is what mqttConsumeWillUnderGate requires.
		d := sess.disposeForeignStateLocked(asm, c, false)
		willPhase = func() { d.applyWill(sess, asm) }
		if d.applied && d.stale != nil {
			voc, stale := v.voc, d.stale
			arb.disposed = true
			tail = func() {
				stale.mqttNotifyForeignDisposal(sess.id, voc)
				stale.closeConnection(voc.closeReason)
			}
			return subs, arb, nil
		}
		// The identity check refused: something rebound while this verdict was being
		// reached, so the disposal — and the notification — are not ours to make.
		return subs, arb, fmt.Errorf("session %q: %w", sess.id, errMQTTSessionOwnershipUnsettled)

	case mqttGroupOwnershipUnresolved:
		// The write may have landed. The packet's state is KEPT — that is what the
		// resolver re-attempts and proves durable — and the tail owns the resolution.
		resolveEventually = true
		return subs, arb, fmt.Errorf("session %q: %w", sess.id, errMQTTSessionOwnershipUnsettled)

	default:
		// FORKED and LOCAL. Neither demonstrates another owner, so neither disposes and
		// neither discards the Will: suppressing it here would be silent loss. Both are
		// non-Foreign closes, so both CONSUME the Will under the still-held gate rather
		// than leaving it to a later close a record-observation eviction can pre-empt.
		c.mqttConsumeWillUnderGate(asm, sess)
		return subs, arb, fmt.Errorf("session %q: %w", sess.id, errMQTTSessionOwnershipUnsettled)
	}
}

// mqttEnqueueSubAck enqueues a SUBACK and reports whether it was sent. A v5
// SUBACK whose reason-code payload alone exceeds the client's Maximum Packet Size
// is suppressed (returns false); the 3.1.1 path always sends.
func (c *client) mqttEnqueueSubAck(pi uint16, filters []*mqttFilter) bool {
	w := newMQTTWriter(7 + len(filters))
	w.WriteByte(mqttPacketSubAck)
	// The per-filter byte (f.qos) is a granted QoS (0/1/2) or a failure reason
	// code (0x80, or a more specific v5 code such as 0x9E); these are valid
	// SUBACK reason codes, so the payload byte is identical for both protocol
	// levels. v5 only adds a properties block before the reason codes.
	if c.mqtt.proto == mqttProtoLevel5 {
		// Carry the first failed filter's detail as the packet-level Reason
		// String, when the client permits it. Spec5 [3.9.2.2.2].
		var reasonStr string
		for _, f := range filters {
			if f.qos >= mqttSubAckFailure && f.reasonStr != _EMPTY_ {
				reasonStr = f.reasonStr
				break
			}
		}
		props := c.mqttReasonProps(mqttPacketSubAck, reasonStr)
		frame := func() []byte {
			var vh mqttWriter
			vh.WriteUint16(pi)
			vh.writeProperties(props)
			for _, f := range filters {
				vh.WriteByte(f.qos)
			}
			w.Reset()
			w.WriteByte(mqttPacketSubAck)
			w.WriteVarInt(vh.Len())
			w.Write(vh.Bytes())
			return w.Bytes()
		}
		buf := frame()
		// Drop the Reason String (only) if it overflows the client's max.
		if props != nil && !c.mqttFitsClientMax(len(buf)) {
			props = nil
			buf = frame()
		}
		// If the reason-code payload alone still overflows, the SUBACK cannot be
		// sent [MQTT-3.1.2-24]; the subscription side effects stand.
		return c.mqttEnqueueFittingProto(buf)
	}
	// packet length is 2 (for packet identifier) and 1 byte per filter.
	w.WriteVarInt(2 + len(filters))
	w.WriteUint16(pi)
	for _, f := range filters {
		w.WriteByte(f.qos)
	}
	c.mu.Lock()
	_ = c.writeActionlessFrameBy(w.Bytes(), time.Time{})
	c.mu.Unlock()
	return true
}
