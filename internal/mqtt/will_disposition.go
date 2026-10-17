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

// The Will: its disposition states, owing, discarding and settling it on takeover, and
// publishing an owed Will.

package mqtt

import (
	"time"

	"github.com/nats-io/nuid"
)

// mqttWillDisposition is what is owed to one connection's Will Message. (Not to
// be confused with mqttWillState, the durable state of an outbox record.)
//
// A Will starts armed: the connection is up, nothing is owed yet, and a
// same-client-ID CONNECT taking the session over discards it (a takeover is not
// one of the events that publishes a Will, and it is the takeover itself that
// closes the old connection). Observing the connection close is what makes the
// Will owed, and an owed Will is never a takeover's to destroy: an owed immediate
// Will has no delay left in which a reconnect could cancel it, and an owed delayed
// one is suppressed only by an acceptance dated before its deadline, which the
// outbox's acceptance-fact scan decides (mqttSettleWillOnTakeover).
// Spec5 [3.1.2.5], [3.14.4], [MQTT-3.1.2-8].
type mqttWillDisposition uint8

const (
	// mqttWillArmed is a Will on a live connection: publishable if the connection
	// drops, discardable by a normal DISCONNECT or by a takeover.
	mqttWillArmed mqttWillDisposition = iota
	// mqttWillOwedNow is a Will whose connection has closed with no Will Delay
	// Interval left to run. It must be published; a reconnect cannot cancel it.
	mqttWillOwedNow
	// mqttWillOwedDelayed is a Will whose connection has closed with a Will Delay
	// Interval to run. It is enqueued on the durable outbox -- by the teardown, a parked
	// action, or a takeover tail -- and a reconnect accepted before its deadline
	// suppresses it there.
	mqttWillOwedDelayed
	// mqttWillDischarged is terminal: the Will has been published, enqueued or
	// discarded, and must not be acted on again.
	mqttWillDischarged
)

// mqttWillIsDelayed reports whether a Will's publication has to wait out a Will
// Delay Interval instead of happening when the connection closes. The interval
// only defers a Will while the session outlives the connection: a zero Session
// Expiry Interval ends the session at close, and the session ending is itself a
// publication trigger. Spec5 [3.1.3.2.2], [MQTT-3.1.2-8].
func mqttWillIsDelayed(proto byte, will *mqttWill, sessionExpiry uint32) bool {
	return proto == mqttProtoLevel5 && will != nil && will.props != nil &&
		will.props.willDelay > 0 && sessionExpiry > 0
}

// mqttOweWill records that this connection's close has been observed, moving an
// armed Will to owed. sessionExpiry is the session's effective Session Expiry
// Interval, read by the caller while the session is still unambiguously this
// connection's; it decides whether the Will is due now or after its delay.
//
// A no-op unless the Will is still armed, which is what keeps a
// takeover-initiated close silent: the takeover discarded the armed Will before
// closing the connection it displaces.
//
// No lock held on entry.
func (c *client) mqttOweWill(sessionExpiry uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mqtt == nil || c.mqtt.cp == nil || c.mqtt.cp.will == nil ||
		c.mqtt.willDisposition != mqttWillArmed {
		return
	}
	// The close instant is stamped on BOTH arms, not only the delayed one. This is
	// already the site that records that the close has been observed, and stamping
	// only where the value is currently read would make its reach something every
	// later reader has to re-derive.
	//
	// The no-op-unless-armed guard above gives the right semantics for free: this
	// runs twice for a graceful DISCONNECT (see mqttHandleClosedClient), and the
	// second call must not overwrite the first observation.
	c.mqtt.closeObservedAt = time.Now().UnixNano()
	if mqttWillIsDelayed(c.mqtt.proto, c.mqtt.cp.will, sessionExpiry) {
		c.mqtt.willDisposition = mqttWillOwedDelayed
	} else {
		c.mqtt.willDisposition = mqttWillOwedNow
	}
}

// mqttDiscardWill drops this connection's Will without publishing it. A normal
// DISCONNECT does this. Spec [MQTT-3.1.2-8].
//
// No lock held on entry.
func (c *client) mqttDiscardWill() {
	c.mu.Lock()
	c.mqttDiscardWillLocked()
	c.mu.Unlock()
}

// Lock held on entry.
func (c *client) mqttDiscardWillLocked() {
	if c.mqtt == nil || c.mqtt.cp == nil {
		return
	}
	c.mqtt.cp.will = nil
	c.mqtt.willDisposition = mqttWillDischarged
	// A discarded Will has no deadline left to anchor, so the observation goes with
	// it. Leaving it set would hand a stale close instant to a later Will on this
	// same connection.
	c.mqtt.closeObservedAt = 0
}

// mqttDiscardArmedWill drops the Will of a connection being displaced by a new one
// with the same client ID, IF its close has not been observed yet. A takeover is not
// a Will publication trigger, and it is the takeover that closes this connection, so
// an armed Will goes away with it. An OWED Will is not the takeover's to destroy: an
// owed-now one has no delay left in which a reconnect could cancel it, and an
// owed-delayed one is decided by the acceptance facts, which alone can say whether a
// reconnect happened before the delay ran out (see mqttSettleWillOnTakeover).
// Spec5 [3.1.2.5], [MQTT-3.1.2-8].
//
// This is the half of the takeover's rule that needs no gate, which is why the
// displaced connection's own rebound arm applies it too. It has no take and no
// producer, so it can only discard an armed Will or do nothing.
//
// No lock held on entry.
func (c *client) mqttDiscardArmedWill() {
	c.mu.Lock()
	if c.mqtt != nil && c.mqtt.willDisposition != mqttWillOwedNow &&
		c.mqtt.willDisposition != mqttWillOwedDelayed {
		c.mqttDiscardWillLocked()
	}
	c.mu.Unlock()
}

// mqttSettleWillOnTakeover is the takeover's whole Will rule for the connection it
// displaces, applied by the party that holds both connections and the ownership gate:
// an armed Will is discarded (mqttDiscardArmedWill), an owed-now one is left for the
// displaced teardown's mqttPublishOwedWill, and an OWED-DELAYED one is consumed into
// the durable outbox with its close-anchored deadline, so the acceptance facts
// arbitrate it.
//
// It used to discard the owed-delayed Will too, on the reasoning that a reconnect
// within the Will Delay cancels it. That is the rule, but this was the wrong place
// to apply it: the reconnect's acceptance is dated by the acceptance fact this
// CONNECT is about to write, and only the fact scan can compare it with the deadline
// (mqttAcceptFactQualifies). Discarding without a date destroyed a Will whose delay
// had already run out, and a Will whose replacing CONNECT was then refused.
//
// The takeover tail is the OWNER of that consumption, not the displaced teardown: the
// teardown may already have returned through its lost-owner exit, which publishes only
// an owed-now Will. The decision and the take are one c.mu critical section, and the
// Will slot is only ever taken at a commit point, so this and the teardown's own
// mqttOweWill are totally ordered: owed first, and this consumes it; this first, and
// the Will was still armed and is discarded.
//
// fence is the RETIRING incarnation's baseline when the session object has been
// re-minted under this takeover (a clean start); nil means sample sess, which still
// carries the displaced connection's incarnation on a resume. Either way the sample
// precedes this CONNECT's acceptance batch, so the successor's fact sorts above it
// and the displaced connection's own fact below.
//
// The ownership gate is held on entry; no lock is.
func (c *client) mqttSettleWillOnTakeover(asm *mqttAccountSessionManager, sess *mqttSession, f *mqttWillFence) {
	if c == nil || asm == nil || sess == nil {
		return
	}
	action := &mqttDeferredDisconnect{willSessEpoch: asm.sessStreamEpoch}
	if f != nil {
		action.willGen, action.willSessionSeq = f.gen, f.seq
	} else {
		sess.mu.Lock()
		action.willGen, action.willSessionSeq = sess.gen, sess.seq
		sess.mu.Unlock()
	}
	now := time.Now()
	c.mu.Lock()
	if c.mqtt == nil {
		c.mu.Unlock()
		return
	}
	switch c.mqtt.willDisposition {
	case mqttWillOwedNow:
		// Too late to cancel and not this tail's to publish: the displaced teardown
		// observed the close and owns the publication.
	case mqttWillOwedDelayed:
		// The displaced connection's OWN lifetime caps the delay -- a DISCONNECT
		// override if it sent one, else its own interval, never the session's, which
		// this takeover has already overwritten.
		action.expiryInterval = c.mqtt.sessionExpiry
		if ov := c.mqtt.lifetimeOverride; ov != nil {
			action.expiryInterval = *ov
		}
		c.mqttTakeWillIntoActionLocked(action, now)
	default:
		c.mqttDiscardWillLocked()
	}
	c.mu.Unlock()
	if action.will != nil && c.srv != nil {
		c.srv.dischargeDeferredWill(asm, sess, action)
	}
}

// mqttTakeWillLocked consumes this connection's Will, returning it together
// with the disposition it was consumed in; a nil Will means there is nothing
// left to do. Every path that discharges a Will performs this transition while
// holding the client lock, so competing close, deferred-disconnect and takeover
// paths cannot claim the same Will twice.
//
// Lock held on entry.
// The close observation is returned WITH the Will and its disposition, and cleared
// with them: they are one fact (see mqtt.closeObservedAt). A caller that took the
// Will without the anchor would have to re-derive the deadline's origin, which is
// the defect this triple exists to prevent.
func (c *client) mqttTakeWillLocked() (*mqttWill, mqttWillDisposition, int64) {
	if c.mqtt == nil || c.mqtt.cp == nil || c.mqtt.cp.will == nil {
		return nil, mqttWillDischarged, 0
	}
	will, disposition := c.mqtt.cp.will, c.mqtt.willDisposition
	closeObservedAt := c.mqtt.closeObservedAt
	c.mqtt.cp.will = nil
	c.mqtt.willDisposition = mqttWillDischarged
	c.mqtt.closeObservedAt = 0
	return will, disposition, closeObservedAt
}

type mqttWill struct {
	topic   []byte
	subject []byte
	mapped  []byte
	message []byte
	qos     byte
	retain  bool
	// MQTT 5.0 Will properties (nil for 3.1.1).
	props *mqttProperties
}

// mqttHandleWill publishes a disconnected client's Will Message, if any. For an
// MQTT 5.0 client that set a Will Delay Interval on a session that survives the
// connection, publication is deferred so a timely reconnect can cancel it. Spec5
// [3.1.3.2.2], [MQTT-3.1.2-8].
// sessionExpiry is a POINTER so a caller can say "read it from the connection, under the
// same lock that consumes the Will" rather than hand over a value sampled earlier.
//
// The distinction is not cosmetic. The interval is CONNECTION-owned and stays mutable
// through an MQTT 5 DISCONNECT override, so any caller that snapshots it and then does work
// — a disposal running a clear(), an eviction crossing its as.mu section — can consume the
// Will against an interval the client has since changed. That decides whether the Will is
// due now or after its delay, and whether the delay is capped, so a stale read can hold a
// due Will back for the full Will Delay. Callers that legitimately own a value (the
// deferred discharge, whose action captured its own) pass it; disposal paths pass nil.
func (s *Server) mqttHandleWill(c *client, sessionExpiry *uint32, sessGen string, originSessionSeq uint64, originSessEpoch string) {
	c.mu.Lock()
	// Consume the Will under the same lock the disposition is transitioned under:
	// this runs twice for a graceful DISCONNECT (see mqttHandleClosedClient), and
	// only the first invocation may act on it.
	will, disposition, closeObservedAt := c.mqttTakeWillLocked()
	expiry := uint32(0)
	switch {
	case sessionExpiry != nil:
		expiry = *sessionExpiry
	case c.mqtt != nil:
		expiry = c.mqtt.sessionExpiry
	}
	if will == nil {
		c.mu.Unlock()
		return
	}
	// MQTT 5.0 Will Delay Interval: defer publication when a delay is set and the
	// session survives the connection. The session survives iff its (effective)
	// Session Expiry Interval is non-zero; a zero interval ends the session at
	// connection close, so the Will is published immediately. The pending Will is
	// tracked on the account manager keyed by client ID (not on the session object,
	// which a clean-start client removes on disconnect) so a reconnect can cancel
	// it. Spec5 [3.1.3.2.2], [MQTT-3.1.2-8].
	//
	// The close classified this already; recompute only for a Will that somehow
	// reached here still armed, so the two can never disagree.
	delayed := disposition == mqttWillOwedDelayed
	if disposition == mqttWillArmed {
		delayed = mqttWillIsDelayed(c.mqtt.proto, will, expiry)
	}
	if delayed {
		asm := c.mqtt.asm
		// Spec5 [3.1.3.2.2]: publish at the Will Delay Interval OR when the session
		// ends, whichever is first, so cap the delay at the Session Expiry Interval.
		// (mqttSessionNeverExpire, being the max uint32, never lowers the delay.)
		// Anchored to the CLOSE, not to this consumption. Spec5 [3.1.3.2.2] measures
		// the Will Delay from the Network Connection ending, and between that and
		// here sit the session gate, a clean-session discard and two JetStream round
		// trips -- so measuring from now restarts the delay and widens the window in
		// which a reconnect can still suppress a Will that was already owed.
		//
		// An OWED Will with a zero anchor is an invariant violation rather than an
		// armed one, and is classified HERE: mqttWillFireAt reads zero as "the
		// caller's own action is the close", which is right for an armed Will and
		// wrong for an owed one, and it cannot tell them apart.
		now := time.Now()
		var fireAt int64
		if closeObservedAt == 0 && disposition != mqttWillArmed {
			// DUE NOW, not now + D. This Will is already owed -- the disposition says
			// so -- and the only thing missing is when. Granting it a fresh full delay
			// would hand back a suppression window it has already spent, on top of the
			// one already lost; marking it due is the smaller error and the only one
			// that cannot suppress a Will the client is owed.
			c.Errorf("MQTT: a Will owed by client %q reached the Will producer with no close observation; "+
				"it is enqueued as DUE NOW, and suppression entitlement for the interval since the true close is not preserved",
				c.mqtt.cid)
			fireAt = now.UnixNano()
		} else {
			fireAt = mqttWillFireAt(will, expiry, closeObservedAt, now)
		}
		props := mqttWillForwardProps(will.props)
		user := c.opts.Username
		// The EXACT client id, read under c.mu with everything else this frame needs:
		// suppression is decided by comparing it against an acceptance fact's client
		// id, and two clients can share a hash.
		clientID := c.mqtt.cid
		// A delayed Will cannot re-check publish permissions at fire time, so the
		// decision taken at CONNECT is what governs -- and it is READ here, not
		// re-derived. By this point the connection is gone and its snapshot has been
		// invalidated, so re-deriving would refuse every Will rather than the denied
		// ones. See the capture in mqttProcessConnect. F-079.
		willAllowed := c.willPublishAuthorized()
		// The authorized DESTINATION, captured under c.mu in the same frame as the
		// client id and the authorization decision. Sampling it after the unlock is
		// the same class of bug as re-deriving willPublishAuthorized there, which
		// F-079 already fixed on this exact frame: by then the connection's snapshot
		// is invalidated. The enqueue compares it against the current service
		// account and refuses a mismatch, which moves deliverWillRecord's own fire-
		// time fence earlier.
		willAccount, _ := c.willDestinationAccount()
		c.mu.Unlock()
		// Record the Will on the durable outbox ($MQTT_willq) so a surviving
		// replica fires it if this broker exits during the delay. Every replica's
		// claim loop drives it to publication once FireAt passes; a reconnect
		// cancels it. See will_outbox.go. Spec5 [3.1.3.2.2], [MQTT-3.1.2-8].
		if asm != nil && willAllowed {
			// The outcome is classified PER EXIT, not by the error value: see
			// mqttWillEnqueueOutcome. Exactly one outcome is retainable -- a refusal
			// that happened before any enqueue effect -- and an unestablished or
			// mismatched destination account is that outcome. It used to be a
			// straight loss here, which contradicted the rule the rest of this code
			// states: an UNKNOWN destination defers a Will, it never destroys one.
			// The identity is allocated by the enqueue and returned through this
			// struct, so a retention below carries exactly the one the refused
			// attempt used rather than a second one minted here.
			identity := &mqttDelayedWillIdentity{}
			outcome, eerr := asm.enqueueDelayedWill(s, clientID, sessGen, will, props, user,
				fireAt, originSessionSeq, originSessEpoch, willAccount, identity)
			if outcome.retainable() {
				// Retained for RE-ENQUEUE, carrying the original FireAt so the delay
				// stays the one the client configured and the acceptance facts still
				// arbitrate it. Not the immediate queue, which publishes on recovery.
				if !s.retainDelayedWillEnqueue(&mqttOwedDelayedWill{
					as: asm, identity: *identity,
					clientID: clientID, sessGen: sessGen, will: will, props: props, user: user,
					fireAt: fireAt, account: willAccount,
					originSessSeq: originSessionSeq, originSessEpoch: originSessEpoch,
				}) {
					s.Errorf("MQTT: the delayed Will of client %q on %q is LOST: its destination was unavailable (%v) and it could not be retained",
						clientID, will.topic, eerr)
				}
			} else if eerr != nil {
				// Terminal. Every remaining outcome either published the Will
				// without its delay or abandoned it, and none may be retried.
				s.Errorf("MQTT: the delayed Will of client %q on %q was not durably enqueued: %v",
					clientID, will.topic, eerr)
			}
		}
		return
	}
	pp := c.mqtt.pp
	pp.topic = will.topic
	pp.subject = will.subject
	pp.mapped = will.mapped
	pp.msg = will.message
	pp.sz = len(will.message)
	pp.pi = 0
	pp.flags = will.qos << 1
	if will.retain {
		pp.flags |= mqttPubFlagRetain
	}
	// pp is the shared publish struct; replace any properties left over from
	// the client's last PUBLISH with the Will's own. Spec5 [3.1.3.2].
	pp.props = mqttWillForwardProps(will.props)
	// The IMMEDIATE Will is not one of the three capture gates and writes no
	// outbox record, so "the record embodies the decision" says nothing about it:
	// it needs the decision read directly. Unlike the other three its core publish
	// does ride c.nrc and so is dropped by NATS -- but its persistence and, for a
	// Sparkplug birth topic, its derived certificate effect do not, which is
	// exactly the bypass. F-079.
	allowed := c.willPublishAuthorized()
	c.mu.Unlock()
	if !allowed {
		return
	}
	// The originating connection publishes this Will -- EXCEPT when that connection
	// is the reason there is a Will at all.
	//
	// A per-client NATS loss is now an unclean disconnect, so it discharges the
	// Will; but the immediate path fans out through c.nrc, which on that trigger is
	// the connection that just died. PublishMsg then fails, processInboundClientMsg
	// returns before the retained hook, and a QoS 0 Will is simply gone -- fired in
	// name only. A QoS 1/2 Will survives because its store carries it, which is why
	// the paired fixture did not catch this.
	//
	// So an unusable connection hands the Will to the client-free route instead: the
	// same one a delayed or owed Will takes, carrying the account its authorization
	// was decided against and checked again there.
	// WHERE AN IDENTITY GOVERNS, the immediate Will ALWAYS takes the client-free
	// route -- there is no "publish it ourselves if the connection still looks
	// alive" branch, because that branch cannot be made safe. Any check of the
	// snapshot has to release c.mu before publishing, and revocation in that
	// interval puts us back where we started: production connections reconnect
	// without limit, so PublishMsg may buffer the Will and send it after
	// REAUTHENTICATION, possibly into another account, or fail and silently lose a
	// QoS 0 Will that has no store to fall back on.
	//
	// So the branch is deleted rather than tightened. One path, carrying the
	// account the decision was made for and re-checked at discharge. The cost is
	// that under auth passthrough an immediate Will is fanned out by the internal
	// client rather than the client's own connection, so NATS no longer filters it
	// -- which is precisely what the broker's own gate now does, and the reason this
	// item exists.
	//
	// With no identity governing there is nothing to cross and no snapshot to
	// revoke, so that mode keeps publishing exactly as it always did. Without this
	// distinction every immediate Will on a broker running -auth-passthrough=false
	// would be diverted, which is what TestMQTTWillRetain caught.
	c.mu.Lock()
	governed := c.mqttAuthzApplies()
	svcAccount, svcOK := c.willDestinationAccount()
	c.mu.Unlock()
	if governed {
		if !svcOK {
			c.Warnf("Not publishing the immediate Will on %q: its authorized destination is not known", will.topic)
			return
		}
		asm := c.mqtt.asm
		if asm == nil {
			c.Errorf("Unable to publish the immediate Will on %q: no account manager is available to carry it", will.topic)
			return
		}
		rec := &mqttWillOutbox{WillID: nuid.Next(), Account: svcAccount}
		willOutboxMessage(rec, will, pp.props, c.opts.Username)
		if derr := asm.deliverWillRecord(s, rec); derr != nil {
			s.retainOrReportOwedWill(asm, rec, derr, "the immediate Will")
		}
		return
	}
	// A Will is server-originated: no No Local origin, matching the delayed Will
	// path so suppression does not depend on the Will Delay Interval. Spec5 [3.8.3.1].
	// The error is not acted on: there is no publisher left to tell, a retained failure
	// is logged where it happened, and a retry could duplicate a fan-out that already
	// went out (the same reasoning as retainOrReportOwedWill).
	s.mqttInitiateMsgDelivery(c, pp, _EMPTY_, _EMPTY_)
	c.flushClients(0)
}

// mqttPublishOwedWill discharges the Will of a connection whose teardown could
// not take the session gate because the binding moved. Nothing else will publish an
// owed-NOW Will: the teardown that would have published it is the one that just gave
// up, and a LICENSED takeover leaves an owed-now Will alone (mqttSettleWillOnTakeover).
//
// Only an immediate Will is published here. A delayed one is the takeover tail's or
// the disposal's to consume: they hold a baseline this exit cannot sample, because the
// session it would read is already the successor's. The tail enqueues it, and the
// acceptance fact the reconnect records decides it at FireAt, suppressing only if that
// acceptance predates the Will falling due. Spec5 [3.1.2.5].
//
// Publication goes through the account's internal client rather than this dying
// one, whose NATS connection the concurrent takeover close may be tearing down
// underneath it (mqttProcessConnect closes the displaced client asynchronously).
//
// No lock held on entry.
func (s *Server) mqttPublishOwedWill(c *client, asm *mqttAccountSessionManager) {
	c.mu.Lock()
	if c.mqtt == nil || c.mqtt.willDisposition != mqttWillOwedNow {
		c.mu.Unlock()
		return
	}
	will, _, _ := c.mqttTakeWillLocked()
	user := c.opts.Username
	// The internal account client publishes with authority of its own, so this
	// Will is governed by the decision captured at CONNECT and read here. Asking
	// now would refuse every Will rather than the denied ones: this runs on the
	// teardown path, where the dying connection's snapshot is already invalid.
	// F-079.
	allowed := will != nil && c.willPublishAuthorized()
	c.mu.Unlock()
	if will == nil || !allowed {
		return
	}
	// Account is the SERVICE account this Will's authorization was decided
	// against, and deliverWillRecord refuses a record naming a different one -- so
	// it must be the same value enqueueDelayedWill records, not the synthetic
	// account this manager is keyed on. An indeterminate service account means we
	// cannot say what we would be authorizing, so the Will is not published.
	// The account this Will's authorization was CAPTURED against, carried from the
	// connection rather than sampled from the broker now. Sampling would bind the
	// record to whatever the service connection happens to be pointing at during a
	// teardown, which is precisely the moment least likely to be settled.
	c.mu.Lock()
	svcAccount, svcOK := c.willDestinationAccount()
	c.mu.Unlock()
	if !svcOK {
		c.Warnf("Not publishing the owed Will: its authorized destination is not known")
		return
	}
	rec := &mqttWillOutbox{WillID: nuid.Next(), Account: svcAccount}
	willOutboxMessage(rec, will, mqttWillForwardProps(will.props), user)
	if err := asm.deliverWillRecord(s, rec); err != nil {
		s.retainOrReportOwedWill(asm, rec, err, "the Will of a session taken over mid-disconnect")
	}
}

// mqttWillForwardProps encodes a Will's properties as the raw block (length
// prefix + body, the pp.props format) to attach to the Will PUBLISH. Will Delay
// Interval is consumed by the server and has no encode case, so it is never
// forwarded. Spec5 [3.1.3.2].
func mqttWillForwardProps(p *mqttProperties) []byte {
	if p == nil {
		return nil
	}
	w := newMQTTWriter(0)
	w.writeProperties(p)
	// Just the zero-length varint: nothing to forward.
	if w.Len() <= 1 {
		return nil
	}
	return w.Bytes()
}
