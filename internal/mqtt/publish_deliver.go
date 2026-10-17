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

// Outbound delivery: the QoS 0, QoS 1/2 and PUBREL delivery callbacks, the prepared publish,
// PUBLISH header writing, and topic-aliased frames.

package mqtt

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// For a MQTT QoS0 subscription, we create a single NATS subscription on the
// actual subject, for instance "foo.bar".
//
// For a MQTT QoS1+ subscription, we create 2 subscriptions, one on "foo.bar"
// (as for QoS0, but sub.mqtt.qos will be 1 or 2), and one on the subject
// "$MQTT.sub.<uid>" which is the delivery subject of the JS durable consumer
// with the filter subject "$MQTT.msgs.foo.bar".
//
// This callback delivers messages to the client as QoS0 messages, either
// because: (a) they have been produced as MQTT QoS0 messages (and therefore
// only this callback can receive them); (b) they are MQTT QoS1+ published
// messages but this callback is for a subscription that is QoS0; or (c) the
// published messages come from (other) NATS publishers on the subject.
//
// This callback must reject a message if it is known to be a QoS1+ published
// message and this is the callback for a QoS1+ subscription because in that
// case, it will be handled by the other callback. This avoid getting duplicate
// deliveries.
func mqttDeliverMsgCbQoS0(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	if pc.kind == JETSTREAM && len(reply) > 0 && strings.HasPrefix(reply, jsAckPre) {
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	// A resuming CONNECT installs this raw subscription before its CONNACK, and
	// [MQTT-3.2.0-1] makes the CONNACK the first packet the client may see. QoS 0
	// is DROPPED rather than waited on: it has no redelivery, so parking a NATS
	// delivery goroutine would buy nothing. For a plain filter this is the same
	// outcome as today, where no raw subscription exists in that window at all;
	// for a shared one it is an accepted cost, stated in docs/CONFORMANCE.md. See
	// sess_restore_gate.go.
	if sess.restoreGateBlocksQoS0() {
		mqttTestRestoreGateBlockedHook("qos0")
		return
	}

	// Lock here, otherwise we may be called with sub.mqtt == nil. Ignore
	// wildcard subscriptions if this subject starts with '$', per Spec
	// [MQTT-4.7.2-1].
	sess.subsMu.RLock()
	subQoS := sub.mqtt.qos
	filterKey := sub.mqtt.filterKey
	retainAsPublished := sub.mqtt.retainAsPublished
	subID := sub.mqtt.subID
	// For a shared subscription the raw-subject sub must deliver only
	// QoS0-published messages; stored (QoS 1/2 published) messages arrive through
	// the shared JS consumer instead, so treat shared like a QoS1+ sub below when
	// deciding to defer to the QoS 1/2 callback. Spec5 [4.8.2].
	shared := sub.mqtt.shared
	ignore := mqttMustIgnoreForReservedSub(sub, subject)
	sess.subsMu.RUnlock()

	if ignore {
		return
	}

	// A live SUBSCRIBE that installed or revived SHARED group interest has not yet proved
	// this broker still owns the membership, and delivering-and-acking a shared QoS 0
	// member destroys that message for the members who legitimately hold it. Dropped
	// rather than waited on, exactly as the restore gate's QoS 0 arm is, and for the same
	// reason. PLAIN QoS 0 is deliberately ungated: it acknowledges nothing on the group's
	// behalf, so gating it would lose messages while protecting nobody. This check sits
	// AFTER the metadata read because `shared` is not knowable before it. See
	// sub_operation_gate.go (F-078).
	if shared && sess.subGateBlocksQoS0(cc) {
		mqttTestRestoreGateBlockedHook("qos0")
		return
	}

	hdr, msg := pc.msgParts(rmsg)
	var topic []byte
	var props []byte
	// origRetain is the original RETAIN flag, for the Retain As Published option.
	// It always comes from the stamped Nmqtt-Ret header (the publisher's live
	// pp.flags RETAIN is cleared before fan-out); parsed only when the sub wants
	// it, since a retained publish is uncommon.
	var origRetain bool
	if pc.isMqtt() {
		// This is an MQTT publisher directly connected to this server.

		// Check the subscription's QoS. If the message was published with a
		// QoS>0 and the sub has the QoS>0 then the message will be delivered by
		// mqttDeliverMsgCbQoS12.
		msgQoS := mqttGetQoS(pc.mqtt.pp.flags)
		if (subQoS > 0 || shared) && msgQoS > 0 {
			return
		}
		topic = pc.mqtt.pp.topic
		props = pc.mqtt.pp.props
		// If the subject is different than the one in pp.subject, then some
		// mapping/transform occurred and we need to recreate the topic.
		if subject != bytesToString(pc.mqtt.pp.subject) {
			topic = natsSubjectStrToMQTTTopic(subject)
		}
		h := mqttParsePublishNATSHeader(hdr)
		// No Local: the same ownership-time decision as every other route, from the
		// stamp the publisher's own PUBLISH carries. Spec5 [3.8.3.1], [MQTT-4.5.0-1].
		if h != nil && mqttNoLocalExcluded(cc.srv.mqtt.originKey, sess.idHash, filterKey, subject, msg, h.noLocal) {
			return
		}
		if retainAsPublished && h != nil {
			origRetain = h.retained
		}

	} else {
		// Non MQTT client, could be NATS publisher, or ROUTER, etc.. In the
		// standalone broker this is also the path for a genuine local MQTT
		// publisher: unlike upstream (where the MQTT connection is an in-process
		// client and pc is that same object for a same-server delivery), every
		// delivery here — including a client's own QoS0 publish echoed back to
		// its own subscription — arrives over the real NATS wire through a
		// synthetic per-subscription client (see (*client).processSub,
		// shim_engine.go), so pc.isMqtt() never reports true for a live message.
		h := mqttParsePublishNATSHeader(hdr)

		// Check the subscription's QoS. If the message was published with a
		// QoS>0 (in the header) and the sub has the QoS>0 then the message will
		// be delivered by mqttDeliverMsgCbQoS12.
		if (subQoS > 0 || shared) && h != nil && h.qos > 0 {
			return
		}

		// No Local: the decision mqttDeliverMsgCbQoS12 makes, from the stamp taken
		// when the server took ownership, never from this subscription's live option.
		// Each tag is an HMAC over (publisher's idHash, filter, subject, payload), so a
		// match proves this subscriber's own session excluded itself on THIS filter
		// without trusting a client-forgeable header value. See nolocal_stamp.go.
		// Spec5 [3.8.3.1], [MQTT-4.5.0-1].
		if h != nil && mqttNoLocalExcluded(cc.srv.mqtt.originKey, sess.idHash, filterKey, subject, msg, h.noLocal) {
			return
		}

		// If size is more than what a MQTT client can handle, we should probably reject,
		// for now just truncate.
		if len(msg) > mqttMaxPayloadSize {
			msg = msg[:mqttMaxPayloadSize]
		}
		topic = natsSubjectStrToMQTTTopic(subject)
		if h != nil {
			props = h.props
			origRetain = h.retained
		}
	}

	// MQTT 5.0 Message Expiry Interval of 0 expires the message immediately, so
	// it must not reach any subscriber, whatever its protocol level. This live
	// path otherwise waits ~0s, so no interval rewrite is needed. Spec5
	// [3.3.2.3.3].
	if len(props) > 0 {
		if _, expired := mqttForwardExpiry(props, 0); expired {
			return
		}
	}

	// RETAINED REPLAY PRECEDES LIVE TRAFFIC on this subscription (F-105 7c): wait for its replay
	// gate, locks released, until the driver has exhausted the replay or the connection closes.
	if !cc.awaitRetainedReplayGate(sub) {
		return
	}

	// Message never has a packet identifier nor is marked as duplicate. A QoS0
	// message dropped for exceeding the client's Maximum Packet Size needs no
	// completion (it is not JetStream-backed here), so the result is ignored.
	pc.mqttEnqueueQoS0PublishMsgTo(cc, sub, retainAsPublished && origRetain, subID, topic, msg, props)
}

// This is the callback attached to a JS durable subscription for a MQTT QoS 1+
// sub. Only JETSTREAM should be sending a message to this subject (the delivery
// subject associated with the JS durable consumer), but in cluster mode, this
// can be coming from a route, gw, etc... We make sure that if this is the case,
// the message contains a NATS/MQTT header that indicates that this is a
// published QoS1+ message.
func mqttDeliverMsgCbQoS12(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	// Message on foo.bar is stored under $MQTT.msgs.foo.bar, so the subject has to be
	// at least as long as the stream subject prefix "$MQTT.msgs.", and after removing
	// the prefix, has to be at least 1 character long.
	if len(subject) < len(mqttStreamSubjectPrefix)+1 {
		return
	}

	hdr, msg := pc.msgParts(rmsg)
	h := mqttParsePublishNATSHeader(hdr)
	if pc.kind != JETSTREAM && (h == nil || h.qos == 0) {
		// MQTT QoS 0 messages must be ignored, they will be delivered by the
		// other callback, the direct NATS subscription. All JETSTREAM messages
		// will have the header.
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	if h := mqttTestDeliveryOfferedHookV.Load(); h != nil {
		(*h)(sub, reply)
	}

	// The connection's own verdict comes FIRST, before any lock and before
	// sub.mqtt is touched: a resuming CONNECT installs live delivery interest
	// before its CONNACK, and [MQTT-3.2.0-1] makes the CONNACK the first packet
	// the client may see. A withheld delivery was never transmitted, so it is
	// recorded the way an in-flight-cap withholding is, which is what makes the
	// attempt that finally goes out a first send [MQTT-3.3.1-1]. See
	// sess_restore_gate.go.
	// Anything but Open blocks, and both blocking verdicts owe the same thing here:
	// the marker is what makes the attempt that finally goes out a first
	// transmission, whether the restore was refused or this wait simply ran out.
	// Comparing against Withheld ALONE is a defect — a timed-out verdict would fall
	// through and transmit before the CONNACK.
	if sess.awaitTransmissionGates(cc) != mqttTransmissionOpen {
		mqttTestRestoreGateBlockedHook("qos12")
		sess.recordWithheldDelivery(cc, sub, reply)
		return
	}

	// Test seams, PAIRED. The pre-lock one is fired before the lock: this is the point a
	// dispatched callback occupies while drain retirement runs, and holding it here is how a
	// test states that retirement excludes such a callback rather than racing it. The done
	// one is deferred from the same place, so it fires when THAT callback returns, whichever
	// of the exits below it takes.
	//
	// The pair is what makes a negative delivery assertion honest. Every other way of
	// deciding "the callback has had its turn" is a disguised timeout: a delivery on another
	// durable is no proof, because processSub gives each durable its own NATS subscription
	// and nats.go runs an independent dispatch goroutine per subscription, so the other one
	// can be tracked while this callback is still merely runnable.
	if h := mqttTestDeliveryPreLockHookV.Load(); h != nil {
		(*h)(sub)
	}
	if dh := mqttTestDeliveryDoneHookV.Load(); dh != nil {
		defer (*dh)(sub)
	}

	// We lock to check some of the subscription's fields and if we need to keep
	// track of pending acks, etc. There is no need to acquire the subsMu RLock
	// since sess.Lock is overarching for modifying subscriptions.
	sess.mu.Lock()
	// A resume-reconciliation subscription holds its consumer's interest before
	// the CONNACK, so JetStream pushes the session's offline backlog into a
	// window in which nothing may be transmitted yet. WAIT for the phase to
	// release it rather than skipping: these are first transmissions, and
	// skipping them would stall each until AckWait and then mark it DUP. The
	// lock is released while waiting — the releasing goroutine needs it — and
	// every check below is re-evaluated afterwards. See sess_resume_reconcile.go.
	if sub.mqtt != nil && sub.mqtt.phaseGate != nil {
		gate := sub.mqtt.phaseGate
		sess.mu.Unlock()
		timer := time.NewTimer(mqttPhaseGateWaitFor(cc))
		// The retirement of THIS connection's incarnation cancels the wait, for the
		// same reason it cancels the transmission gates: this cap is up to 30 seconds,
		// well past the incarnation drain ceiling, so waiting it out would make a
		// clean-start takeover refuse for a reason unrelated to the contender. The
		// re-checks below then take the ordinary withheld path — recorded, not acked —
		// because sub.mqtt.closed or the ownership term will have moved (F-036 unit 1).
		var closing <-chan struct{}
		if cc != nil && cc.mqtt != nil {
			closing = cc.mqtt.tracker.closingSignal()
		}
		select {
		case <-gate:
		case <-closing:
		case <-timer.C:
		}
		timer.Stop()
		sess.mu.Lock()
	}
	if sess.c != cc || sub.mqtt == nil || sub.mqtt.closed {
		// A delivery the phase never released — the wait ran out, or the filter
		// was reclaimed unadopted — was NEVER TRANSMITTED, so its redelivery is a
		// first send however many attempts JetStream has counted. Record it the
		// way an in-flight-cap withholding is recorded, which is what makes
		// trackPublish clear DUP on the attempt that finally goes out
		// [MQTT-3.3.1-1] (and lets a freed slot NAK it forward rather than
		// waiting out AckWait).
		//
		// WHICH deliveries are still owed is not decided here. The owed-delivery
		// predicate — and its sess.c == nil arm, which covers a RACE rather than a
		// state a test can schedule — lives in recordWithheldDeliveryLocked, its
		// single home, shared with the connection-scoped restore verdict so both
		// withholdings owe the client the same thing. This arm restated that rule
		// for its own case and the restatement went stale, which is what a second
		// home for one fact does.
		if h := mqttTestDeliverySkippedHookV.Load(); h != nil {
			(*h)(sub, reply)
		}
		sess.recordWithheldDeliveryLocked(cc, sub, reply)
		sess.mu.Unlock()
		return
	}

	// Suppress before drain dispatch, but retain this exact offer and wake the
	// terminal owner. In particular, suppression must not starve an unresolved
	// drain-removal save of its readback retry.
	offeredSeq, _, _, _, _ := ackReplyInfo(reply)
	if sess.offerDisposalLocked(sub.mqtt.jsDur, reply, offeredSeq) {
		sess.mu.Unlock()
		return
	}
	if sess.terminalSourceHeldLocked(sub.mqtt.jsDur, offeredSeq) {
		var wake []mqttInflightSlotRef
		if mqttSourceAckReplyBinds(reply, mqttInflightSourceStream(sub.mqtt.jsDur),
			sub.mqtt.jsDur, offeredSeq) {
			for key, w := range sess.terminalSources {
				if key.dur == sub.mqtt.jsDur && key.sseq == offeredSeq &&
					w.ref.name.gen == sess.gen && w.epoch == sess.asm.sessStreamEpoch {
					w.reply = reply
					if w.record.State == mqttInflightPublishTerminal {
						wake = append(wake, w.ref)
					}
				}
			}
		}
		sess.mu.Unlock()
		if sess.isSeqUncertain() {
			sess.resolveUncertainNow(cc)
		}
		for _, ref := range wake {
			sess.scheduleTerminalRegisterRelease(cc, ref.name.pi, ref, false)
		}
		return
	}

	// A DRAINED consumer delivers only what it already started. Branch before any
	// of the ordinary checks, and in this order — the record is the authority, not
	// the in-memory tracking maps, which say nothing after a restart. See
	// unsub_drain.go.
	if sess.testDisposalDispatch != nil {
		sess.testDisposalDispatch()
	}
	var drainClaimed bool
	if d := sess.drainByDur[sub.mqtt.jsDur]; d != nil {
		sseq, _, _, _, _ := ackReplyInfo(reply)
		st := d.seqs[sseq]
		switch {
		case st == nil || !st.claimed:
			// A FRESH message on a drained filter: the client unsubscribed, or downgraded
			// the filter to QoS 0 and is served by the raw subscription instead, so it is
			// not owed this at QoS 1/2. While the drain is still staged it is left to AckWait —
			// acking before the packet's save would lose it if that save rolls back.
			//
			// The same applies to a sequence whose removal is UNCONFIRMED: its record
			// state is unknown, so acking would destroy a message the stored record
			// may still claim (I1). Leave it pending; the resolution discharges the
			// ack once it proves the removal durable, and a foreign verdict makes it
			// somebody else's message entirely. See unsub_drain.go.
			unconfirmed := st != nil && st.unclaimUnconfirmed
			// A third reason to leave it pending, and it is the same shape as the other
			// two: the record does not yet say what this drain claims. An unresolved
			// reservation naming THIS delivery means the claim floor could not place it,
			// so its absence from d.seqs is an incomplete view rather than a fact, and
			// acking would destroy an interest-retained message the next resume may
			// legitimately claim. Per SOURCE, not per session — one durable's opacity says
			// nothing about another's. See sourceRestrictedLocked.
			restricted := sess.sourceRestrictedLocked(sub.mqtt.jsDur, sseq)
			staged := d.staged || unconfirmed || restricted
			sess.mu.Unlock()
			if unconfirmed || sess.isSeqUncertain() {
				// This redelivery IS the retry clock. The finalizer's own immediate attempt
				// may itself have failed, and the periodic pass only runs on its own
				// schedule — so retrying here is what keeps an unresolved removal from
				// sitting unacked and unrestored, with every further save on the session
				// refused, for longer than one AckWait. AckWait paces it.
				//
				// KEYED ON THE SESSION'S UNRESOLVED WRITE, not only on an unconfirmed
				// REMOVAL. The removal is not the only write this path can leave
				// unresolved: the drain's persisted RELEASE REQUEST (L4) is saved before
				// the register delete, and an uncertain save there withholds the whole
				// finalize — leaving the claim standing, so `unconfirmed` is false and the
				// one clock that paces this repair never started. The two are the same
				// obligation observed at different points, and a redelivery is the right
				// clock for both.
				sess.resolveUncertainNow(cc)
			}
			if !staged {
				_, _ = sess.submitSourceAck(reply, false, nil)
			}
			return
		case st.phase == drainPubRelConfirmed:
			// The exchange is past PUBREC with a proven PUBREL: never deliver the
			// payload again. Finalize (which acks on a confirmed save).
			sess.mu.Unlock()
			sess.finalizeDrainDelivery(cc, sub.mqtt.jsDur, sseq, reply)
			return
		case st.phase == drainPubRelUncertain:
			// Past PUBREC, PUBREL existence unknown: deliver nothing, ack nothing,
			// finalize nothing. The durable intent resolves this on resume.
			st.ack = reply
			sess.mu.Unlock()
			return
		case st.phase == drainPublish:
			// Claimed and still in the PUBLISH phase: a forced redelivery.
			drainClaimed = true
		default:
			// An unknown phase must never fall through into a delivery. Leave the
			// message pending so AckWait retries it once the state is understood.
			sess.mu.Unlock()
			cc.Errorf("MQTT: drained sequence %v is in an unknown phase %v; withholding delivery", sseq, st.phase)
			return
		}
	}

	// In this callback we handle only QoS-published messages to QoS
	// subscriptions. Ignore if either is 0, will be delivered by the other
	// callback, mqttDeliverMsgCbQos1.
	var qos byte
	if h != nil {
		qos = h.qos
	}
	if qos > sub.mqtt.qos {
		qos = sub.mqtt.qos
	}
	// A QoS0 member of a shared subscription still consumes stored messages
	// through the group consumer; it just delivers them at QoS0 and acks the JS
	// message (handled below after the reserved/deny checks). For a non-shared
	// QoS0 result the direct NATS subscription delivers instead. Spec5 [4.8.2].
	sharedQoS0 := qos == 0 && sub.mqtt.shared
	// A CLAIMED drained sequence is exempt, and that exemption is what lets a drain
	// whose membership went to QoS 0 finish what it started.
	//
	// This test is about a FIRST transmission: min(published, granted) is 0, so the raw
	// QoS 0 subscription delivers instead and this callback has nothing to do. A claimed
	// drained sequence is not a first transmission — it is the completion of a delivery
	// already put on the wire, which [MQTT-3.10.4-3] owes — and the subscription's maximum
	// is an input to a first transmission and to nothing else. The wire QoS comes from the
	// ENTRY in the send section below, never from here.
	//
	// The CONNECT restore's conversion is why this matters: a cold restore has no
	// subscription to copy delivery terms from, so its drain installs at qos 0 and every
	// claim it holds would stop here. An UNSUBSCRIBE- or downgrade-staged drain copies a
	// positive QoS at staging, so for those this changes nothing. Shared consumers are
	// never drained (stageDrain refuses a DeliverGroup), so sharedQoS0 and the affine move
	// below are unreachable for a claimed sequence.
	if qos == 0 && !sharedQoS0 && !drainClaimed {
		sess.mu.Unlock()
		return
	}

	// Check for reserved subject violation. If so, we will send the ack to
	// remove the message, and do nothing else.
	strippedSubj := subject[len(mqttStreamSubjectPrefix):]
	if mqttMustIgnoreForReservedSub(sub, strippedSubj) {
		sess.mu.Unlock()
		_, _ = sess.submitSourceAck(reply, false, nil)
		return
	}

	// A broad wildcard subscription can overlap a subscribe deny clause, and QoS
	// 1/2 delivery rides the SERVICE connection, so NATS does not filter it --
	// this guard is the only thing that does. F-081.
	//
	// The two non-allow verdicts are disposed of DIFFERENTLY, and that asymmetry is
	// the point. A proven deny is a fact about the message: it will never become
	// deliverable to this filter, so it is acknowledged, or the JS consumer
	// redelivers it forever. An indeterminate verdict is a fact about US -- no valid
	// snapshot -- so the message is left unacknowledged and AckWait keeps it for the
	// client's next connection. Acking it would satisfy "not delivered" by DESTROYING
	// it.
	switch cc.checkDenySubVerdict(strippedSubj, string(sub.queue)) {
	case mqttSubDeliverDenied:
		sess.mu.Unlock()
		// TERMINAL only for a subscription nobody else can serve. On a SHARED
		// subscription the group consumer picked this member without consulting its
		// ACL, so acknowledging here would consume the group's message on behalf of
		// members who ARE allowed it -- turning one member's deny into data loss for
		// the whole group. Hand it back instead, paced, so the group can offer it to
		// someone else; upstream NATS gets the same outcome by filtering denied
		// queue members before selection, which this port cannot do because the
		// selection happens on the service connection.
		//
		// Whether it is acked away or handed back, this session is not going to
		// deliver it, so an identifier a PREVIOUS transmission left behind is
		// released here. The guard sits before trackPublish so a refusal costs no
		// NEW identifier -- but a message transmitted before a disconnect leaves a
		// durable register that the resume restores into the tracking maps before
		// any guard runs, and holding it counts against Receive Maximum for the
		// life of the session. Nothing below this point can free it: the deny
		// returns. Note the INDETERMINATE arm deliberately does not do this --
		// it decides nothing, and freeing an identifier is a decision.
		released := sess.releaseTransmittedIdentifier(cc, sub.mqtt.jsDur, reply)
		if released == mqttAbandonPending {
			// The restored delivery's abandonment is not durable yet (refused, vetoed,
			// unresolved, or a send section is open). Nothing was untracked, and the source
			// is NOT acknowledged: its AckWait re-offer re-runs this guard, which retries.
			return
		}
		if sub.mqtt.shared && released == mqttAbandonDone {
			// The shared abandonment's own disposal already handed this very offer back (7a);
			// a second NAK would only re-pace the same delivery.
			return
		}
		if sub.mqtt.shared {
			// Handed back PACED, on the same backoff the affine refusals use, and
			// that shared rule is load-bearing rather than tidiness. When the denied
			// member is the ONLY one, there is no eligible member to hand it to and no
			// event that resolves it, so a fixed delay is not a retry: it is an
			// unbounded loop against `MaxDeliver` of -1, at whatever rate the constant
			// picks, driven by any client permitted to hold such a subscription and
			// multiplied by every message it can accumulate. A permanent refusal is
			// exactly what the cap is for. The delay comes from JetStream's own
			// delivery count for this message, so no per-message state is kept.
			//
			// The price is paid by the ALLOWED members. Each re-offer goes to a queue
			// member nats-server picks at random, without consulting its ACL, so a
			// denied member picked again refuses again at the next, longer delay: an
			// allowed member is served after a geometric number of offers, with no
			// deadline while a denied member stays in the group. The cap bounds each
			// gap, not the wait. Nothing is lost; docs/CONFORMANCE.md residual 11.
			_, _, dc, _, _ := ackReplyInfo(reply)
			sess.jsa.sendMsg(reply, mqttPacedRefusalNak(dc))
			return
		}
		_, _ = sess.submitSourceAck(reply, false, nil)
		return
	case mqttSubDeliverIndeterminate:
		sess.mu.Unlock()
		return
	}

	// No Local: a message the publishing session excluded from this subscription must not be
	// delivered to it. Ack it so the JS consumer does not redeliver it.
	//
	// WHETHER it was excluded was decided when the server took OWNERSHIP of the message, and
	// is read from the stamp the message carries — never from sub.mqtt.noLocal (F-117).
	// [MQTT-4.5.0-1] puts an owned message into the Session State of the subscriptions that
	// match at that moment, and [MQTT-3.8.4-3] makes a re-SUBSCRIBE a NEW subscription for
	// messages owned after it. Reading the live option instead re-judged an owned message
	// whose offer merely arrived after a re-SUBSCRIBE: turned OFF, it forwarded a message
	// the publisher had excluded ([MQTT-3.8.3-3]); turned ON, this arm acked away a message
	// the subscription had admitted, which is a loss [MQTT-3.8.4-4]. Each tag is an HMAC over
	// (publisher's idHash, filter, subject, payload) under the per-run secret, so a match
	// proves authenticity and that THIS filter was excluded; a mismatch, a foreign tag or an
	// absent stamp delivers. See nolocal_stamp.go.
	//
	// NOT for a CLAIMED drained sequence, and the asymmetry is deliberate. No Local decides
	// whether a message is forwarded at all; for a claimed sequence that decision was taken,
	// and acted on, when the delivery was first put on the wire. Applying it again would not
	// withhold a retransmission — this arm ACKS the source and returns, so it would destroy
	// the message while leaving the drain's claim standing, which is loss plus a stuck claim
	// rather than a header defect. A FRESH sequence on a drained durable never reaches this
	// point — the drain block acks it away. Spec5 [3.8.3.1].
	if !drainClaimed {
		var stamp []byte
		if h != nil {
			stamp = h.noLocal
		}
		if mqttNoLocalExcluded(cc.srv.mqtt.originKey, sess.idHash, sub.mqtt.filterKey, strippedSubj, msg, stamp) {
			sess.mu.Unlock()
			_, _ = sess.submitSourceAck(reply, false, nil)
			return
		}
	}

	// Retain As Published: capture the option under the lock; the delivered
	// message keeps the original RETAIN flag (from the stored Nmqtt-Ret header)
	// only when the subscription set it. Spec5 [3.8.3.1].
	retainAsPub := sub.mqtt.retainAsPublished && h != nil && h.retained
	// Subscription Identifier: capture under the lock, injected on enqueue below.
	// Spec5 [3.8.2.1.2].
	subID := sub.mqtt.subID

	if sharedQoS0 {
		// Deliver at QoS0 (no packet identifier, no tracking) and ack the JS
		// message so the group consumer does not redeliver it. Spec5 [4.8.2].
		//
		// UNLESS THIS MEMBER HOLDS A HANDBACK FOR IT (7a, consult 83). A source this session
		// abandoned and handed back to the group is not this member's to deliver again while the
		// binding stands: the check is here, BEFORE output, because the ACK gate below can only
		// turn the acknowledgement into a NAK after a frame is already on the wire. A HandBack
		// installed after this check can still overlap the write; the gate then hands back, and the
		// result is the duplicate docs/CONFORMANCE.md states (consult 84).
		if q0seq, _, _, _, _ := ackReplyInfo(reply); sess.sourceHandBackHeldLocked(sub.mqtt.jsDur, q0seq) {
			sess.handBackLocked(reply)
			sess.mu.Unlock()
			return
		}
		sess.mu.Unlock()
		originalTopic := natsSubjectStrToMQTTTopic(strippedSubj)
		var props []byte
		if h != nil {
			props = h.props
		}
		if len(props) > 0 {
			_, _, _, ts, _ := ackReplyInfo(reply)
			if p, expired := mqttForwardExpiry(props, ts); expired {
				_, _ = sess.submitSourceAck(reply, false, nil)
				return
			} else {
				props = p
			}
		}
		if hook := mqttTestSharedQoS0OutputHook.Load(); hook != nil {
			(*hook)(sub.mqtt.jsDur, reply)
		}
		pc.mqttEnqueueQoS0PublishMsgTo(cc, sub, retainAsPub, subID, originalTopic, msg, props)
		_, _ = sess.submitSourceAck(reply, false, nil)
		return
	}

	// A shared-subscription delivery at effective QoS 2 is MOVED, never tracked
	// here: the exchange has to complete with the client the server chose, and a
	// message pending on the group durable is redelivered to whichever member
	// AckWait reaches next. The move takes durable ownership of it and hands it to
	// this session's own consumer. Spec5 [MQTT-4.8.2-5]; see
	// qos2_shared_affine.go.
	//
	// Unconditional, deliberately: there is no "only if this delivery already
	// started" test, because `started` reads in-memory tracking maps that say
	// nothing after a restart, a failover, or on another broker.
	//
	// Re-checked here because the move is IRREVERSIBLE and takes several round
	// trips: it claims durable ownership of the message for this session. The guard
	// above ran before all of that, so an identity revoked in between would
	// otherwise acquire ownership it is no longer entitled to. An indeterminate
	// answer withholds without acknowledging, exactly as the guard's own
	// indeterminate arm does -- the message must survive for the next connection.
	if sub.mqtt.shared && qos == 2 {
		if cc.checkDenySubVerdict(strippedSubj, string(sub.queue)) != mqttSubDeliverAllow {
			sess.mu.Unlock()
			return
		}
		groupDurable := sub.mqtt.jsDur
		sess.mu.Unlock()
		var props []byte
		if h != nil {
			props = h.props
		}
		sess.moveStartedSharedQoS2(cc.srv, cc.mqtt.asm, &mqttQ2AffMoveInput{
			groupDurable: groupDurable,
			natsSubject:  strippedSubj,
			reply:        reply,
			props:        props,
			msg:          msg,
			retained:     retainAsPub,
			subID:        subID,
			maxPayload:   atomic.LoadInt32(&cc.mpay),
		})
		return
	}

	sseq, _, _, _, _ := ackReplyInfo(reply)
	// A source RETIRED by an unproven acknowledgement is past PUBREC: its payload must
	// never go out again, whatever the identifier bookkeeping now says. Tested BEFORE
	// trackPublish, because after PUBCOMP nothing in those maps remembers the exchange
	// and the payload would simply be re-sent under a fresh identifier. Not applied to a
	// drain-managed sequence: there the drain state machine owns both the decision and
	// the acknowledgement.
	// A SHARED SOURCE THIS MEMBER HANDED BACK is not re-delivered to it while the HandBack stands
	// (7a, consult 83): its abandoned exchange still owns the identifier and its late PUBACK, and a
	// fresh delivery here would be admitted, acknowledged by the client, and then have its source
	// acknowledgement handed back by the gate — forever. So the re-offer is handed back to the group
	// before any tracking. The predicate is the gate's own.
	if sess.sourceHandBackHeldLocked(sub.mqtt.jsDur, sseq) {
		sess.handBackLocked(reply)
		sess.mu.Unlock()
		return
	}
	if !drainClaimed && sess.sourceRetiredLocked(sub.mqtt.jsDur, sseq) {
		sess.mu.Unlock()
		sess.settleRetiredSource(cc, sub.mqtt.jsDur, sseq, reply)
		return
	}

	// A claimed drained sequence on a QoS 0 drain may only be retransmitted where the
	// EXCHANGE's own QoS is known, and where it is not this refuses BEFORE allocating.
	//
	// The exchange's QoS lives on the pending entry, seeded on recovery from the in-flight
	// register. Without one, the value that would reach storeInflightRegister below is
	// min(published, sub.mqtt.qos) — and on this drain sub.mqtt.qos is 0, so it is 0: an
	// invented QoS for an exchange whose own is unrecoverable. The send section already
	// refuses a zero-QoS entry rather than guessing; this is that same refusal moved
	// ahead of trackPublish, so it costs no packet identifier and stores no QoS-0
	// register that the send section would then decline anyway.
	//
	// CONFINED to qos == 0, which is the population this conversion creates. A POSITIVE
	// QoS drain reaching the same state — its finalization released the register and the
	// save that would have cleared the claim definitely failed — has its own grant to
	// register at, which is not an invention, and refusing there would strand a claim
	// nothing can reconstruct.
	//
	// It WITHHOLDS: no acknowledgement, no register release, no claim removal. Missing
	// evidence is not proof that the exchange completed. Two states produce it without
	// any client acknowledgement — the window in which PUBREC has removed the PUBLISH
	// entry but has not yet recorded the drain intent, and a marker-opaque reservation
	// that recovery installed without a cpending mapping — so settling here would destroy
	// an unfinished exchange. The retention this leaves is stated in docs/CONFORMANCE.md
	// and has no automatic repair.
	if drainClaimed && qos == 0 && !sess.claimedExchangeQoSKnownLocked(sub.mqtt.jsDur, sseq) {
		sess.mu.Unlock()
		cc.Warnf("MQTT: withholding the retransmission of drained sequence %d on %q for client %q: "+
			"the exchange carries no recoverable QoS", sseq, sub.mqtt.jsDur, sess.id)
		return
	}

	if !mqttInflightPreSendAdmits(sub.mqtt.jsDur, reply) {
		sess.mu.Unlock()
		return // a test-staged pre-tracking refusal; AckWait re-offers the sequence
	}
	pi, isNew := sess.trackPublish(sub.mqtt.jsDur, reply)
	// The allocation's nonce, read under the same lock that allocated it: it is the
	// identity every later reservation touch is guarded by, and reading it later would
	// race the teardown that replaces the entry with a reservation.
	//
	// And whether the entry is PROVEN-UNSENT and releasable, which gates the expiry discard
	// below: only a PUBLISH the client has never seen may be dropped on expiry
	// ([MQTT-4.4.0-1]). That is the durable pair's to say — the marker still present — never
	// "the entry pre-existed": a recovered entry whose marker survives was registered and
	// not transmitted and is droppable; one whose marker is gone may have reached the
	// client and is not (provenUnsentReleasableLocked).
	var regNonce string
	var expirable bool
	if ack := sess.pendingPublish[pi]; pi != 0 && ack != nil {
		expirable = sess.provenUnsentReleasableLocked(ack)
		if isNew {
			regNonce = ack.regNonce
		}
	}
	// A SHARED delivery's dependency, captured ONCE under the same lock that allocated: the exact
	// acquisition its incarnation is held by (sharedAcqForDeliveryLocked) and the shared tenure this
	// CONNECT acquires under. Neither is recaptured by a ladder retry; the admission re-validates
	// both against the ledger inside every attempt.
	var sharedDep *mqttAdmitShared
	if isNew && pi != 0 && sub.mqtt.shared {
		if acq, ok := sess.sharedAcqForDeliveryLocked(sub.mqtt.jsDur); ok {
			sharedDep = &mqttAdmitShared{Acq: acq, Tenure: sess.sharedTenure}
		}
	}
	if drainClaimed && pi != 0 {
		// The drain record proves the source was claimed, not that its PUBLISH reached
		// the client: whether this is a first transmission is the marker's to say (the
		// recovered entry carries it), so nothing here overrides the entry's DUP state.
		if d := sess.drainByDur[sub.mqtt.jsDur]; d != nil {
			if st := d.seqs[sseq]; st != nil {
				st.ack = reply
				// Normally a reconciliation of equal values: the resume barrier restored
				// the identifier from this delivery's in-flight register before any source
				// interest existed, so trackPublish returned the very identifier the
				// record already names. It still assigns, because where the register was
				// legitimately absent — a claim that never got one, or one dropped as
				// provably complete — a later PUBREL intent must name the identifier
				// actually on the wire.
				st.pi = pi
				// Repair the register sequence from the live entry. A drain record written
				// before RegSeq existed carries none, and a drain that acquired its register
				// through recovery rather than through staging has it only here. The drain
				// releases the register when the exchange completes, so a zero left in place
				// would leak it (finalizeDrainDelivery).
				if ack := sess.pendingPublish[pi]; ack != nil && ack.regSeq != 0 {
					st.regSeq = ack.regSeq
				}
			}
		}
	}
	sess.mu.Unlock()

	if pi == 0 {
		// We have reached max pending, don't send the message now.
		// JS will cause a redelivery and if by then the number of pending
		// messages has fallen below threshold, the message will be resent.
		return
	}

	originalTopic := natsSubjectStrToMQTTTopic(strippedSubj)
	var props []byte
	if h != nil {
		props = h.props
	}
	// MQTT 5.0 Message Expiry Interval: a v5 subscriber must receive the interval
	// reduced by the time the message waited in the server; if it has already
	// elapsed the message must not be delivered and its stored copy dropped. The
	// store timestamp is carried in the JetStream ack reply subject. Expiry is a
	// property of the message, so it equally gates delivery to a 3.1.1
	// subscriber (whose properties are then stripped on the wire).
	// Only a message PROVEN never transmitted may be dropped on expiry; a redelivery of
	// a possibly-sent PUBLISH must be resent, not dropped, and its identifier held.
	// Spec5 [3.3.2.3.3], [MQTT-4.4.0-1].
	if len(props) > 0 && expirable {
		_, _, _, ts, _ := ackReplyInfo(reply)
		var expired bool
		if props, expired = mqttForwardExpiry(props, ts); expired {
			removed := sess.dropProvenUnsentPublishEntry(pi, mqttLicenceOf(cc))
			if !sess.finalizeDrainSource(cc, removed) {
				_, _ = sess.submitSourceAck(reply, false, nil)
			}
			return
		}
	}

	// A newly allocated identifier is not sendable until its durable in-flight
	// register is confirmed (R1, inflight_register.go). Synchronous, as the shared
	// QoS 2 move above already is on this same path: the identifier the client is
	// about to hold has to survive this broker.
	//
	// As late as possible, and after the expiry check above: a message that is
	// discarded rather than sent has no identifier to make durable, so it should not
	// pay a round trip for one. Still strictly BEFORE the enqueue below, which is the
	// send.
	if isNew {
		// EVERY SUPPORTED DELIVERY IS ADMITTED THROUGH THE OUTBOUND OBLIGATION AUTHORITY (F-105).
		// An ordinary QoS 1/2 delivery carries the exchange and the source/PUBREL resource edges
		// beside the register (admitInflightPublish); a shared-group delivery — QoS 1 only, a QoS 2
		// one was diverted to the affine move above — carries the exact acquisition its source
		// depends on instead (admitSharedInflightPublish, 7a). A shared delivery with no held
		// acquisition is refused there, never admitted without one.
		var stored bool
		if sub.mqtt.shared {
			stored = sess.admitSharedInflightPublish(cc, pi, qos, sub.mqtt.jsDur, sseq, regNonce, sharedDep)
		} else {
			stored = sess.admitInflightPublish(cc, pi, qos, sub.mqtt.jsDur, sseq, regNonce, _EMPTY_, _EMPTY_)
		}
		if !stored {
			return
		}
		if h := mqttTestDeliveryAdmittedPauseV.Load(); h != nil {
			(*h)(sess, pi)
		}
	}
	// The SEND SECTION. queueOutbound is a synchronous socket write with no deadline, so
	// between here and its return a teardown, a denial, a recovery pass or a terminal
	// acknowledgement can free this identifier and bumpPI can re-mint it — two PUBLISHes
	// under one identifier, in order, on one socket. Reserved, those paths condemn the
	// entry instead of removing it and the continuation runs at endSend (F-087 WP5).
	//
	// The deferred call is armed IMMEDIATELY, before the enqueue: the registry's "only
	// endSend removes" invariant must not depend on no early return and no recovered
	// panic ever appearing in between.
	sess.mu.Lock()
	// The wire QoS of every PUBLISH admitted here comes from the ENTRY, never from the
	// subscription. sub.mqtt.qos is an input to a message's FIRST transmission and to
	// nothing else: a re-SUBSCRIBE may replace the filter's maximum at any time
	// ([MQTT-3.8.4-3]) without interrupting a publication already in flight, so
	// recomputing it per offer re-derived a started exchange's QoS from whatever the
	// subscription happened to say -- retransmitting a QoS 2 exchange at QoS 1 after a
	// downgrade, whose PUBACK then deletes the owner slot and abandons the handshake.
	//
	// UNCONDITIONAL, with no branch on started or isNew. A first transmission reads
	// back exactly the min(published, granted) it computed, so the rule costs it
	// nothing; and the contested case that MUST take the entry's value -- the
	// same-source replacement, which preserves an occupant's QoS the caller never
	// supplied -- arrives with isNew true and started false, so an "if started" guard
	// would send at one QoS while the register held another.
	//
	// Read HERE, in the send section, because it must be after any register store has
	// settled: the store is synchronous and confirmInflightRegister threads the landed
	// QoS onto the entry and clears regPending before returning.
	if ack := sess.pendingPublish[pi]; ack != nil {
		if ack.qos == 0 {
			// No per-exchange evidence, so there is nothing to transmit AT. Withheld
			// without acknowledging -- the same disposal every indeterminate arm on this
			// callback uses -- and tested BEFORE the send is reserved, so the return
			// leaks no token. Guessing a QoS is the defect this exists to prevent.
			sess.mu.Unlock()
			cc.Warnf("MQTT: withholding delivery %s/%d for client %q: packet identifier %d carries no exchange QoS",
				sub.mqtt.jsDur, sseq, sess.id, pi)
			return
		}
		qos = ack.qos
	}
	sendTok, sendOK := sess.beginSendLocked(cc, pi, sub.mqtt.jsDur, sseq)
	sess.mu.Unlock()
	if !sendOK {
		// The entry is no longer sendable — a concurrent section, a condemnation, or a
		// connection that is no longer this session's. The message stays with JetStream
		// and AckWait re-offers it, the same cost a regPending decline already accepts.
		return
	}
	// The section's three phases, in an order that IS the mechanism: ADMISSION (may this
	// frame reach this receiver at all), then durable INTENT (the marker's confirmed delete,
	// making the exchange possibly-attempted), then the WRITE. Intent before the write, so
	// a crash after it recovers DUP=1 with the identifier held rather than re-offering a
	// possibly-delivered PUBLISH as a first transmission; admission before intent, so a size
	// refusal never leaves a transmitted-looking register for a PUBLISH that never left.
	// wrote is memory-only: true means POSSIBLY written, a partial, errored or timed-out
	// write included.
	// THE SHUTDOWN BARRIER, before anything durable is taken and before the section's own
	// end is armed. A section's durable action is ended by a JetStream write, so one still
	// open when the JSA teardown runs leaves a live token no later start can attribute —
	// and the identifier, its capacity and the session's outbound flow stay blocked. The
	// gate is refused only while shutting down, and a refusal costs a redelivery.
	//
	// REGISTERED FIRST so it runs LAST: defers unwind in reverse, and releasing the
	// barrier before endPublishSend would let the shutdown wait return with this section's
	// drain still in flight, which is the one thing it exists to prevent.
	if !cc.srv.admitPublishSend() {
		sess.endPublishSend(cc, pi, sendTok, mqttIntentNone, false)
		return
	}
	defer cc.srv.publishSendDone()
	intent, wrote := mqttIntentNone, false
	defer func() { sess.endPublishSend(cc, pi, sendTok, intent, wrote) }()
	// THE DURABLE HALF OF THE EXCLUSION, and it comes first. Before F-098 this section's
	// only hold was process memory, which fences no peer: two brokers may legitimately
	// serve one incarnation. Taking it here rather than after admission fences the SIZE
	// REFUSAL too, which is why that path's disposal now waits for the drain.
	if !sess.acquirePublishAction(cc, pi, &sendTok) {
		return // a peer holds the action, or the slot moved; AckWait re-offers
	}
	if !sess.markerSendWindowHook(pi, mqttMarkerSendReserved) {
		return // a test-staged crash after the pair confirmed and BEFORE intent: proven-unsent
	}
	// DUP is the value the section captured under the lock, never a re-read at write
	// time: the entry can be replaced or condemned while this write is out.
	prep, admitted := pc.mqttPreparePublishMsgTo(cc, pi, qos, sendTok.dup, retainAsPub, subID, originalTopic, msg, props)
	if !admitted {
		// Exceeds the client's MQTT 5.0 Maximum Packet Size at every framing. Spec5
		// [3.1.2.11.4]. What may be done about it is the durable state's to say.
		if !sess.oversizeDisposalAllowed(cc, pi, sendTok) {
			return // POSSIBLY-ATTEMPTED: retained whole, nothing acknowledged
		}
		// PROVEN-UNSENT: no PUBLISH was ever attempted under this identifier, so discard
		// it and complete the delivery so JetStream does not redeliver it forever: release
		// the identifier and ack the JS message. The disposal runs BEFORE the deferred
		// endPublishSend: the pair's cleanup is terminal cleanup's.
		//
		// THE DURABLE ACTION IS DRAINED FIRST. This destructive work cannot proceed under
		// a live token — mqttSlotReleaseAuthority withholds on ANY of them, including this
		// broker's own — and draining here rather than relying on the deferred end is what
		// keeps the disposal in this function where its proof lives.
		sess.drainPublishAction(cc, sendTok)
		sendTok.actionHeld = false
		// Through the same removal as the expiry drop above: the identifier's exclusion is
		// handed off before the untrack, because this delete can be refused or fail just as
		// that one can (F-098, holistic review round 3, swept round 4).
		removed := sess.dropProvenUnsentPublishEntry(pi, mqttLicenceOf(cc))
		if !sess.finalizeDrainSource(cc, removed) {
			_, _ = sess.submitSourceAck(reply, false, nil)
		}
		return
	}
	intent = sess.recordSendIntent(cc, pi, sendTok)
	if intent == mqttIntentDefiniteFailure || intent == mqttIntentAmbiguous {
		return // nothing written; the entry keeps (or holds unresolved) its marker; AckWait re-offers
	}
	if !sess.markerSendWindowHook(pi, mqttMarkerSendIntent) {
		return // a test-staged crash AFTER intent and before the write: possibly-attempted
	}
	prep.bindSection(sendTok)
	wrote = pc.mqttEnqueuePreparedPublishMsgTo(cc, sub, prep)
	sess.markerSendWindowHook(pi, mqttMarkerSendWrote)
	// A refused transmission here is unreachable for an admitted frame (admission is
	// monotone) and would in any case be a possibly-attempted exchange: nothing is disposed.
}

func mqttDeliverPubRelCb(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	if sub.client.mqtt == nil || sub.client.mqtt.sess == nil || reply == _EMPTY_ {
		return
	}
	if dh := mqttTestPubRelDeliveryDoneHookV.Load(); dh != nil {
		defer (*dh)(sub, reply)
	}
	if h := mqttTestPubRelDeliveryDelayV.Load(); h != nil {
		(*h)()
	}

	hdr, _ := pc.msgParts(rmsg)
	// ONE decode, before anything is derived from any single header. See
	// mqttDecodeStoredPubRel: the identifier, the exchange owner, the affine reference
	// and the source correlation are one shape, and a record only some of whose headers
	// are legal is evidence of nothing.
	shape, derr := mqttDecodeStoredPubRel(hdr)
	if derr != nil {
		// WITHHELD: return without acking, exactly as the transmission gate does, and
		// JetStream re-offers it.
		//
		// It may not be transmitted — the client would receive a PUBREL for an exchange
		// nothing can attribute — and it may not be acknowledged away either, because
		// acknowledging destroys the only copy of whatever it was. PUBREL carries no DUP
		// ([MQTT-3.6.1-1] fixed-header flags are 0010), so nothing further is owed. An
		// operator or generation cleanup removes it; the cost is stream space rather than
		// an identifier, because the record reserves nothing.
		//
		// This is the structural refusal. Admission against the owner slot follows only for
		// a record whose complete shape decoded successfully.
		pc.Warnf("MQTT: withholding a stored PUBREL: %v", derr)
		return
	}
	pi := shape.pi
	corr := mqttPubRelCorrelation{SrcDur: shape.srcDur, SrcSeq: shape.srcSeq, Ok: shape.srcDur != _EMPTY_}
	affRef := shape.affRef

	// The owner identity is REQUIRED and was decided by the decode above — before the
	// drain interaction, before either pending map is touched, before any
	// acknowledgement and before the send section opens. A stored PUBREL is EVIDENCE
	// about one exchange, not a claim on a packet identifier.
	owner := shape.owner

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	// Before anything, and before sub.mqtt is touched: ensurePubRelConsumerSubscription
	// installs this subscription with a bare c.processSub, whose NATS handler goes
	// live BEFORE processSub returns, so a stored PUBREL can arrive here with no
	// per-subscription metadata attached at all. The connection-scoped verdict is
	// armed before any subscribe, which is what makes it readable here. WITHOUT
	// ACKNOWLEDGING on a withheld verdict, so JetStream redelivers; PUBREL carries
	// no DUP ([MQTT-3.6.1-1] fixed-header flags are 0010), so nothing is owed
	// beyond that. See sess_restore_gate.go.
	switch gv := sess.awaitTransmissionGates(cc); gv {
	case mqttTransmissionWithheld:
		mqttTestRestoreGateBlockedHook("pubrel")
		return
	case mqttTransmissionTimedOut:
		// NAK rather than simply returning: a PUBREL carries no withheld marker, so
		// nothing else would re-drive it, and the wait's bound can be well short of
		// AckWait. See mqttTransmissionTimedOut.
		mqttTestRestoreGateBlockedHook("pubrel")
		sess.jsa.sendMsg(reply, AckNak)
		return
	}

	sess.mu.Lock()
	held := sess.recoveryPIHeldLocked(pi)
	// AN EXCHANGE THIS BROKER HAS DECIDED COMPLETE IS NOT ADMITTED AGAIN. Its terminal
	// reservation stands from the decision (the client's PUBCOMP, or recovery's proof of it)
	// until its terminal cleanup; until the durable advance lands the slot still reads
	// pubrel_ready, so admission alone would acquire a fresh action and the bind would write a
	// PUBREL the client has already answered [MQTT-4.3.3]. WITHHELD, never acknowledged: the
	// record's acknowledgement belongs to the PUBCOMP path, which sends it once the completion is
	// recorded or carries it in the completion worker. AckWait re-offers it until then, and once
	// the slot is pubrel_terminal admission refuses it on its own.
	answered := sess.pendingPubRel[pi].answeredByPubComp(owner.Nonce)
	sess.mu.Unlock()
	if held || answered {
		// Recovery alone may classify a held reservation; an answered one is the PUBCOMP path's.
		// Either way AckWait re-offers.
		return
	}
	mqttRunTestSeam(&mqttTestPubRelBeforeAdmissionV)

	// A PUBREL that completes a DRAINED source is owned end to end here, with ONE
	// ordering on every path — first delivery and replay alike: confirm the phase
	// (the delivery itself is proof the record exists), remove and persist the
	// source claim, ack the source, and only THEN forward.
	//
	// There is deliberately no fast path that forwards first. With one, the client
	// could PUBCOMP, the PUBREL record could be acked away, and a crash before the
	// readLoop's finalize would leave a claimed source sequence whose exchange is
	// already complete — so the next redelivery would send the payload a second
	// time. The cost is one session-record save before a drained PUBREL reaches the
	// client (I10). See unsub_drain.go.
	if corr.Ok {
		// THE DRAIN'S OWN IDENTITY IS PROVEN BEFORE ANYTHING IS FINALIZED, and syntactic
		// validity above is not that proof.
		//
		// The correlation names a SOURCE, and a source can legitimately have more than one
		// stored PUBREL — a superseded broker's late store is a supported shape. Confirming
		// the drain on the correlation alone therefore lets a record belonging to a
		// DIFFERENT exchange remove the claim, acknowledge the source and forward its
		// PUBREL: the client completes an exchange it never started, and the real one's
		// evidence is gone. The resume barrier already refuses that by exact owner match,
		// but PUBREL delivery is asynchronous, so this path can race ahead of the barrier
		// and reach the same state the barrier exists to prevent.
		//
		// A mismatch is WITHHELD rather than acknowledged: the record may be the only copy
		// of whatever it does prove, and AckWait re-offers it. PUBREL carries no DUP
		// ([MQTT-3.6.1-1]), so nothing further is owed.
		if !sess.drainIntentOwnedBy(corr.SrcDur, corr.SrcSeq, owner.Nonce, pi) {
			sess.mu.Lock()
			id := sess.id
			sess.mu.Unlock()
			cc.Warnf("MQTT: withholding a stored PUBREL for packet identifier %d of client %q: it "+
				"names drained source %s/%d but not the exchange and identifier that source's "+
				"intent owns", pi, id, corr.SrcDur, corr.SrcSeq)
			return
		}
		if d, claimed := sess.confirmDrainPubRel(corr.SrcDur, corr.SrcSeq); d != nil && claimed {
			sess.finalizeDrainDelivery(cc, corr.SrcDur, corr.SrcSeq, _EMPTY_)
			sess.mu.Lock()
			unproven := false
			if st := d.seqs[corr.SrcSeq]; st != nil {
				// Still claimed, or claimed-but-unconfirmed: in neither case is the
				// removal known to be in the record, and only a known-durable removal
				// may be followed by forwarding (I10).
				unproven = st.claimed || st.unclaimUnconfirmed
			}
			sess.mu.Unlock()
			if unproven {
				// The claim removal was not persisted (refused fence, failed save), or
				// its outcome is unknown. Forward nothing and ack nothing; AckWait
				// retries the whole step, and a readback resolves the unknown case.
				return
			}
			if h := mqttTestPubRelPreForwardHookV.Load(); h != nil {
				(*h)()
			}
			// The drain path has now settled the SOURCE. It grants no transmission
			// authority: the surviving owner slot and its action CAS still govern PUBREL.
		}
	}

	// THE DURABLE ADMISSION, and this is F-092's actual fix.
	//
	// Nothing below may run — not the opener, not a map mutation, not a duplicate
	// acknowledgement, and above all not the transmission — until this broker holds a
	// non-revocable action on the slot that owns this identifier. The one exception is a
	// live RETAINED exchange, which has no slot (R6) and reaches the transmission below with
	// act == nil, authorized instead by its live in-memory owner in admitStoredPubRel.
	//
	// The discriminator is NOT an inference from the record's fields against the entry
	// that happens to hold the identifier. Five plan revisions tried that and every one
	// was refuted: several unrelated states deliberately carry an empty correlation, an
	// eligibility question exists beside the correlation question, and entries change
	// class IN PLACE after installation so the classification is never closed. There is
	// exactly ONE authoritative claimant of an identifier and it is the durable slot.
	//
	// A refusal returns WITHOUT acknowledging, so AckWait re-offers the record. That is
	// the whole cost of being wrong here, and it is why the refusal is safe to make.
	act, admitted := sess.admitStoredPubRel(cc, pi, owner, reply, corr, affRef)
	if !admitted {
		return
	}
	// THE DRAIN IS ARMED THE INSTANT AN ACTION EXISTS, before any path below can return.
	//
	// Between here and the write there are several ordinary exits — the connection
	// changed, the PUBREL consumer disappeared, duplicate handling declined, the opener
	// refused — and every one of them leaves a DURABLE action live. Arming the drain only
	// at the write meant a routine rebind or an opener refusal permanently reserved the
	// identifier with no storage failure and no crash involved, which is the plan's
	// "every no-write exit drains" contract broken on the most ordinary paths.
	//
	// Registered here, before any later exit. The send token's defer is registered later,
	// so it runs first; this one then drains the durable action exactly once.
	if act != nil {
		defer sess.startActionDrain(cc, act)
	}

	// THE ACCEPTED-INCARNATION FENCE, and its position in the sequence is the property:
	//
	//	action CAS -> acceptance proof -> send bookkeeping -> socket write
	//
	// The action CAS above establishes that no other holder may act on this exchange. It
	// does NOT establish that this broker is still the session's current accepted
	// incarnation: a successor can commit its acceptance while this broker is between
	// acquiring and writing, and the slot arbitrates the action rather than the
	// proposition that its holder is still current. So the proof runs HERE, after the
	// acquisition and before anything is mutated or sent.
	//
	// Placed before the send section deliberately. Run after it — as an earlier revision
	// had it — a refusal would already have opened a send token, filled the entry's
	// acknowledgement subject and performed F-093's handoff, leaving an entry that claims
	// a delivery this broker never transmitted. Here none of that has happened: the drain
	// armed just above returns the action to `pubrel_ready`, no send token exists so no
	// endSend is owed, and AckWait re-offers the record to whoever is current.
	//
	// It NARROWS the acquisition-to-write window; it does not close it, and the
	// documentation says so. A successor accepting between this proof and the socket write
	// still transmits this exchange's own PUBREL under its own identifier, which is a
	// retransmission the client resolves with PUBCOMP — not F-092's hazard of a PUBREL
	// under an identifier a DIFFERENT exchange owns.
	//
	// A retained exchange has no slot and therefore no action (R6); it is authorized by
	// the live in-memory owner identity of its incarnation and by the connection check
	// below, so it is deliberately not fenced here.
	if act != nil {
		if _, out := sess.proveAcceptedIncarnation(cc); out != mqttAcceptanceCurrent {
			return
		}
	}
	mqttRunTestSeam(&mqttTestBeforePubRelPromotionV)

	sess.mu.Lock()
	if sess.c != cc || sess.pubRelConsumer == nil {
		sess.mu.Unlock()
		return
	}
	// A DUPLICATE RECORD for the same identifier — the "a superseded broker stored
	// after a peer recreated the exchange" race, if it outlives deduplication —
	// must be acked and dropped, not forwarded. The client needs exactly one
	// PUBREL, and a duplicate merely left unacked would redeliver forever.
	//
	// The test is on the STREAM SEQUENCE, which identifies the record. It must not
	// be on the ack subject: every AckWait redelivery of the SAME record arrives on
	// a new ack subject, so comparing those would classify ordinary redelivery as
	// duplication and silently ack away a PUBREL the client is still waiting for.
	//
	// Note also that this is a positive test on TRACKED state, not "no pending
	// entry": after a restart there are no pending entries at all, and a genuine
	// pending PUBREL must still be forwarded.
	// Slot-backed exchanges already made the canonical/duplicate decision against durable
	// state in admitStoredPubRel. Reapplying the older in-memory rule after acquiring an
	// action can contradict the slot: it could acknowledge the canonical record merely
	// because a stale entry remembers another sequence, leaving the slot naming a deleted
	// canonical forever. The in-memory rule remains only for retained exchanges, which
	// have no slot or action from which to make that decision.
	if existing, ok := sess.pendingPubRel[pi]; act == nil && ok && existing != nil && existing.jsAckSubject != _EMPTY_ {
		if sseq, _, _, _, _ := ackReplyInfo(reply); sseq != 0 && existing.sseq != 0 && sseq != existing.sseq {
			// Read UNDER the lock. pubRelConsumer is written under sess.mu by teardown
			// and by consumer recreation, so reading it after the unlock below is a data
			// race — and one that would key this record's bookkeeping by an empty or
			// replacement durable rather than by the record's own.
			dupDur := sess.pubRelDurable()
			sess.mu.Unlock()
			// The duplicate is DISCHARGED before it is acknowledged, not simply
			// discarded. Several PUBREL records under one identifier is a supported
			// stale-broker shape, and this one may carry the only reference to an
			// assignment DIFFERENT from the tracked entry's — acking it away would
			// destroy that proof and let a later resume infer a loss for a message the
			// client answered. Run outside sess.mu, exactly as the PUBCOMP arm does,
			// and on a refusal leave the record for AckWait to re-offer.
			if !sess.dischargeQ2AffPubRelProof(cc, pi, mqttDeferredKey{dupDur, sseq}, affRef) {
				return
			}
			_, _ = sess.submitSourceAck(reply, false, nil)
			return
		}
	}
	// THE SEND SECTION. mqttEnqueuePubResponse is a synchronous socket write with no
	// deadline, so between here and its return a PUBCOMP for an EARLIER copy of this
	// same PUBREL can free the identifier and bumpPI can re-mint it — a new PUBLISH
	// queued under an identifier whose PUBREL bytes are still going out. Opened in the
	// SAME critical section as the tracking below, so nothing can free the identifier
	// between deciding to transmit and reserving it (F-089).
	sendTok, sendOK := sess.beginPubRelSendLocked(cc, pi)
	if !sendOK {
		// A transmission is already out under this identifier. Return WITHOUT
		// acknowledging: the record stays pending and AckWait re-offers it once that
		// write has returned, the same cost trackPublish's regPending decline accepts.
		//
		// WHICH POPULATION STILL REACHES THIS. A slot-backed exchange no longer does for a
		// live PUBLISH: admission refuses that upstream, on the durable action token or on
		// the hold's pubIdent, and leaves a redrive waiter the transmission's end wakes
		// (F-098). What is left here is the RETAINED exchange, which has no slot (R6) and
		// so no admission to refuse it, and the opener's refusal is not specific to a
		// PUBLISH — it fires for any live transmission under the identifier, a PUBREL's
		// included. A continuation here would need a predicate recognising both kinds, so
		// this population stays on AckWait and is stated as a residual in
		// docs/CONFORMANCE.md rather than left implied by this comment.
		sess.mu.Unlock()
		return
	}
	// Armed immediately after the opener, before any bookkeeping below can panic or
	// return. It was registered after the admission-time action drain, so LIFO execution
	// clears the socket token first and then drains the durable action.
	//
	// THE ATTEMPT'S PUBCOMP UNIT IS GIVEN BACK UNLESS ITS FRAME MAY HAVE LEFT (F-116), stated
	// as a property rather than at each return: possiblyEmitted turns true on the line before
	// a writer is called and back to false only on the writer's own proof that nothing was
	// emitted. Every return before a writer — this function's refusals and any added later —
	// gives the unit back; a panic or a torn write after the flip keeps it. Read through the
	// closure, because a deferred call's arguments are fixed when it is armed.
	possiblyEmitted := false
	defer func() { sess.endPubRelSend(cc, pi, sendTok, possiblyEmitted) }()
	if act != nil {
		// THE ACQUIRED ACTION BECOMES THE ENTRY, in one validated step (pending-entry
		// closure). Not trackAsPubRel-then-attach: that published a replacement and a mapping
		// before anything knew the resident was this exchange's, and built the delivered entry
		// from the resident's (possibly lineage-less) evidence rather than the action's. The
		// stored record's affine reference goes on with it — read off the RECORD, because the
		// connection completing this exchange is usually not the one that stored the PUBREL —
		// and so do the acquired slot and the F-093 handoff. A refusal changes nothing; the
		// defers above end the send token and drain the action, and AckWait re-offers.
		if !sess.bindActionToPubRelLocked(pi, act, reply, affRef) {
			sess.mu.Unlock()
			return
		}
	} else {
		// No acquired action and no slot: nothing may be written (F-105 7c deleted the slot-less
		// retained exchange this branch used to serve).
		sess.mu.Unlock()
		return
	}
	trace := cc.trace
	sess.mu.Unlock()

	// THE ACTION'S DRAIN was armed at admission, for the same reason. The durable claim spans
	// the no-deadline socket write below, so it must be drained however this returns —
	// normally, early, or through a recovered panic. A drain that depended on reaching the
	// end of the function would leave an abandoned action on any of those paths, which is
	// the residual this mechanism exists to bound rather than manufacture.
	//
	// It runs AFTER endSend by defer ordering: the local send token is what the identifier's in-memory
	// reservation keys on, and the durable action must outlive it rather than the reverse.
	// A RETAINED exchange is admitted with NO action, because it has no slot for one to
	// bind to (R6) — so both the drain and the write-entry transition are skipped, and its
	// identifier is held by the retained reservation instead.
	//
	// WRITE ENTRY IS CONDITIONAL (F-105 row 2, consult 28), for both classes: the holder must still
	// be the live, un-released, un-drained holder of its token, and an ordinary one must hold the
	// exact acquisition it landed through the authority. Entry is the atomicity against the drain,
	// taken under the session lock BEFORE the writer's own lock; the writer then checks only the
	// ticket entry returned.
	mqttRunTestSeam(&mqttTestAfterPubRelCommitV)
	var ticket *mqttWriteEntry
	if act != nil {
		sess.mu.Lock()
		// The entry the write goes out through must still be one that ACTS for this exchange
		// (an Ordinary or Affine entry of the action's nonce), checked in the same section
		// that enters the write.
		if !sess.pubRelWriteKindLocked(pi, act) {
			sess.mu.Unlock()
			return // nothing written; the deferred drain gives the acquisition up
		}
		t, entered := sess.enterPubRelWriteLocked(act, cc)
		if entered {
			sess.markPubRelEnteredLocked(pi, sendTok)
		}
		sess.mu.Unlock()
		if !entered {
			return // nothing written; the deferred drain gives the acquisition up
		}
		ticket = t
		// ARMED IMMEDIATELY, before anything else can run: every exit from here that did not
		// consume the acquisition — a refusal, a sealed gate, a panic, Goexit — records it
		// abandoned. By LIFO it runs before the drain armed above.
		defer ticket.relinquish()
	}
	if h := mqttTestPubRelWriteBlockV.Load(); h != nil {
		(*h)()
	}
	switch {
	case ticket != nil && ticket.action != nil:
		// AN ORDINARY PUBREL executes its acquisition through the authorized boundary.
		possiblyEmitted = true
		if cc.mqttWriteAuthorizedPubRel(pi, ticket, trace) == mqttOutputPrevented {
			possiblyEmitted = false
		}
	case act != nil && !mqttDurIsSlotOnlyClass(act.slot.dur):
		// An ordinary holder with no acquisition to consume writes nothing, whatever let it
		// this far: its bytes are exactly the output the authority never authorized.
		return
	case act != nil:
		// A slot-only holder (experimental affine, chapter P) keeps the raw emitter. It reports
		// no result, so its attempt keeps its PUBCOMP unit whatever happened: the conservative
		// side, a reservation rather than a freed identifier (F-116).
		possiblyEmitted = true
		cc.mqttEnqueuePubResponse(mqttPacketPubRel, pi, trace)
	}
}

// mqttWriteAuthorizedPubRel writes one ordinary PUBREL as the execution of its acquisition, and
// reports the writer's three-way result. A frame the client's Maximum Packet Size refuses is
// never handed to the writer, so it is PREVENTED like any other frame no byte of which left —
// which is what lets its caller give back the attempt's PUBCOMP unit (F-116).
func (c *client) mqttWriteAuthorizedPubRel(pi uint16, ticket *mqttWriteEntry, trace bool) mqttOutputResult {
	buf := c.mqttPubResponseFrame(mqttPacketPubRel, pi, mqttReasonSuccess, _EMPTY_)
	if !c.mqttFitsClientMax(len(buf)) {
		c.Debugf("Suppressing MQTT packet of %d bytes exceeding client Maximum Packet Size %d", len(buf), c.mqtt.maxPacketSize)
		return mqttOutputPrevented
	}
	res, _ := c.writeAuthorizedFrame(mqttFrameRequest{
		Action: ticket.action, Binding: c.outBind, Holder: ticket, Parts: [][]byte{buf},
	})
	if res != mqttOutputPrevented {
		c.mqttTracePubResponse(mqttPacketPubRel, pi, mqttReasonSuccess, trace)
	}
	return res
}

// The MQTT Server MUST NOT match Topic Filters starting with a wildcard
// character (# or +) with Topic Names beginning with a $ character, Spec
// [MQTT-4.7.2-1]. We will return true if there is a violation.
//
// Session or subMu lock must be held on entry to protect access to sub.mqtt.
func mqttMustIgnoreForReservedSub(sub *subscription, subject string) bool {
	// If the subject does not start with $ nothing to do here.
	if !sub.mqtt.reserved || len(subject) == 0 || subject[0] != mqttReservedPre {
		return false
	}
	return true
}

// Check if a sub is a reserved wildcard. E.g. '#', '*', or '*/" prefix.
func isMQTTReservedSubscription(subject string) bool {
	if len(subject) == 1 && (subject[0] == fwc || subject[0] == pwc) {
		return true
	}
	// Match "*.<>"
	if len(subject) > 1 && (subject[0] == pwc && subject[1] == btsep) {
		return true
	}
	return false
}

func sparkbReplaceDeathTimestamp(msg []byte) []byte {
	const VARINT = 0
	const TIMESTAMP = 1

	orig := msg
	buf := bytes.NewBuffer(make([]byte, 0, len(msg)+16)) // 16 bytes should be enough if we need to add a timestamp
	writeDeathTimestamp := func() {
		// [tck-id-conformance-mqtt-aware-ndeath-timestamp] A Sparkplug Aware
		// MQTT Server MAY replace the timestamp of NDEATH messages. If it does,
		// it MUST set the timestamp to the UTC time at which it attempts to
		// deliver the NDEATH to subscribed clients
		//
		// sparkB spec: 6.4.1. Google Protocol Buffer Schema
		//      optional uint64 timestamp = 1; // Timestamp at message sending time
		//
		// SparkplugB timestamps are milliseconds since epoch, represented as
		// uint64 in go, transmitted as protobuf varint.
		ts := uint64(time.Now().UnixMilli())
		buf.Write(protoEncodeVarint(TIMESTAMP<<3 | VARINT))
		buf.Write(protoEncodeVarint(ts))
	}

	for len(msg) > 0 {
		fieldNumericID, fieldType, size, err := protoScanField(msg)
		if err != nil {
			return orig
		}
		if fieldType != VARINT || fieldNumericID != TIMESTAMP {
			// Add the field as is
			buf.Write(msg[:size])
			msg = msg[size:]
			continue
		}

		writeDeathTimestamp()

		// Add the rest of the message as is, we are done
		buf.Write(msg[size:])
		return buf.Bytes()
	}

	// Add timestamp if we did not find one.
	writeDeathTimestamp()

	return buf.Bytes()
}

// Common function to mqtt delivery callbacks to serialize and send the message
// to the `cc` client.
// mqttEnqueuePublishMsgTo frames and queues an outbound PUBLISH to cc. It
// returns false without sending when the packet would exceed the client's MQTT
// 5.0 Maximum Packet Size; the caller is then responsible for completing the
// message (for QoS1/2, acking the JS delivery so it is not redelivered).
// subID is the subscription's v5 Subscription Identifier (0 = none), injected
// into the properties block for a v5 receiver. Spec5 [3.8.2.1.2].
// mqttPreparedPublish is an outbound PUBLISH that has passed ADMISSION — framed for this
// receiver, Maximum Packet Size checked against the frame it will actually be sent in —
// and is waiting for TRANSMISSION. The two phases are split so a QoS 1/2 sender can record
// durable send intent between them (inflight_register.go, recordSendIntent): the intent
// record must precede the socket write, and the size refusal must precede the intent
// record, or an oversize refusal would leave a transmitted-looking register for a PUBLISH
// that provably never left this broker.
//
// The admission verdict is MONOTONE, which is what makes the split safe without holding
// cc.mu across the gap: it is true when the plain full-topic frame fits, or when this topic
// is ALREADY bound to an alias whose alias-only frame fits. A binding, once made, is never
// reassigned or evicted for the connection's life (FCFS, table only grows), and a fresh
// binding frame is always LARGER than the plain frame, so nothing a concurrent sender can do
// between admission and transmission turns an admitted frame into a refused one.
type mqttPreparedPublish struct {
	pi     uint16
	qos    byte
	dup    bool
	retain bool
	v5     bool
	// useAlias: the receiver is v5 with an outbound Topic Alias Maximum, so framing and
	// the alias-table commit happen atomically with queueOutbound under cc.mu.
	useAlias bool
	topic    []byte
	msg      []byte // Sparkplug DEATH timestamps already rewritten
	props    []byte // Subscription Identifier already injected
	// flags and header are the admitted plain frame for a receiver that does not alias;
	// an aliasing receiver is framed at transmission, under the lock.
	flags  byte
	header []byte
	// out and requireAction bind the frame to its send section's acquisition (F-105 row 2):
	// requireAction marks every QoS 1/2 frame that is not on a slot-only (experimental affine,
	// chapter P) slot, which is written only through the authorized boundary and only for out's
	// clean acquisition. Unset for QoS 0 and for slot-only QoS 1/2.
	out           *mqttPublishOutput
	requireAction bool
}

// bindSection binds a prepared frame to the section that will transmit it. Whether its QoS 1/2
// frame must consume an acquisition is the slot's CLASS, the one statement every acquisition and
// acknowledgement arrow asks, never a caller's flag.
//
// The class is the WHOLE condition (F-105 WP-F). A QoS 1/2 frame reaches the raw branch of
// mqttEnqueuePreparedPublishMsgTo only on a slot-only slot; any other one with no acquisition,
// including one whose slot was never named, is refused there rather than written raw.
func (p *mqttPreparedPublish) bindSection(tok mqttSendToken) {
	p.out = tok.out
	p.requireAction = p.qos > 0 && !mqttDurIsSlotOnlyClass(tok.slot.dur)
}

// mqttPreparePublishMsgTo performs ADMISSION for an outbound PUBLISH: the Sparkplug and
// Subscription Identifier rewrites, then the Maximum Packet Size check against the frame
// the receiver will actually get. It commits nothing — no alias binding, no bytes — so a
// caller may refuse, record intent, or transmit afterwards. ok is false only when the
// PUBLISH cannot be sent to this receiver at any framing (Spec5 [3.1.2.11.4]).
func (c *client) mqttPreparePublishMsgTo(cc *client, pi uint16, qos byte, dup, retainAsPub bool, subID int, topic, msg, props []byte) (mqttPreparedPublish, bool) {
	// [tck-id-conformance-mqtt-aware-nbirth-mqtt-retain] A Sparkplug Aware
	// MQTT Server MUST make NBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id with
	// the MQTT retain flag set to true
	//
	// [tck-id-conformance-mqtt-aware-dbirth-mqtt-retain] A Sparkplug Aware
	// MQTT Server MUST make DBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/DBIRTH/edge_node_id/device_id
	// with the MQTT retain flag set to true
	//
	// $sparkplug/certificates messages are sent as NATS messages, so we
	// need to add the retain flag when sending them to MQTT clients.

	// retainAsPub keeps the original RETAIN flag for a v5 Retain As Published
	// subscription. Spec5 [3.8.3.1].
	retain := retainAsPub
	isBirth, isDeath, isCertificate := sparkbParseBirthDeathTopic(topic)
	if isBirth && qos == 0 {
		retain = retain || isCertificate
	} else if isDeath && !isCertificate {
		msg = sparkbReplaceDeathTimestamp(msg)
	}

	// MQTT 5.0 Subscription Identifier: inject the subscription's identifier into
	// the delivered PUBLISH's properties. Only for a v5 receiver (a 3.1.1 receiver
	// gets no properties section) and before the size check below so the extra
	// bytes count against Maximum Packet Size. Spec5 [3.8.2.1.2].
	v5 := cc.mqtt.proto == mqttProtoLevel5
	if v5 && subID > 0 {
		props = mqttInjectSubID(props, subID)
	}

	// MQTT 5.0 outbound Topic Alias: for a v5 client that advertised a Topic Alias
	// Maximum, the alias table lookup/assignment and framing must be atomic with
	// queueOutbound so a binding PUBLISH (full topic + alias) is always buffered
	// before any alias-only PUBLISH referencing it. That happens at transmission,
	// under cc.mu; admission only asks the same table whether SOME frame fits.
	// Every other receiver keeps the original path: frame and size-check here.
	// Spec5 [3.3.2.3.4].
	prep := mqttPreparedPublish{pi: pi, qos: qos, dup: dup, retain: retain, v5: v5,
		useAlias: v5 && cc.mqtt.outTopicAliasMax > 0 && len(topic) > 0,
		topic:    topic, msg: msg, props: props}
	if !prep.useAlias {
		prep.flags, prep.header = mqttMakePublishHeader(pi, qos, dup, retain, v5, topic, props, len(msg))
		// MQTT 5.0 Maximum Packet Size: never send a PUBLISH larger than the client
		// is willing to accept. Discard it instead and let the caller complete the
		// delivery. Spec5 [3.1.2.11.4]. mqtt.maxPacketSize is 0 for 3.1.1 or when the
		// client did not advertise a limit.
		if mp := cc.mqtt.maxPacketSize; mp != 0 && len(prep.header)+len(msg) > int(mp) {
			return prep, false
		}
		return prep, true
	}
	cc.mu.Lock()
	_, _, _, _, ok := cc.mqttAliasedPublishFrame(pi, qos, dup, retain, topic, props, len(msg))
	cc.mu.Unlock()
	return prep, ok
}

// mqttEnqueuePreparedPublishMsgTo performs TRANSMISSION of an admitted PUBLISH: the alias
// framing and binding commit (atomic with the queue, under cc.mu), any retained frames
// parked on the subscription, and the write itself. It returns false only if the frame no
// longer fits, which admission's monotonicity makes unreachable for an admitted frame; a
// caller that had recorded intent treats that as a possibly-attempted exchange all the
// same, never as proof nothing was sent.
func (c *client) mqttEnqueuePreparedPublishMsgTo(cc *client, sub *subscription, p mqttPreparedPublish) bool {
	// AN ORDINARY QoS 1/2 FRAME WITHOUT ITS ACQUISITION IS NOT WRITTEN AT ALL — not raw, not
	// partially, not after the retained flush. Refused first, so nothing of this call reaches the
	// transport and the buffered retained frames wait for the next write.
	if p.requireAction && (p.out == nil || p.out.action == nil) {
		return false
	}
	flags, headerBytes := p.flags, p.header
	var alias uint16
	var fresh, prevented bool
	cc.mu.Lock()
	if p.useAlias {
		// Build under the lock so the binding commit and queueOutbound cannot be
		// interleaved by a concurrent delivery on the same topic; a fresh binding is
		// committed only when its frame is actually queued.
		var ok bool
		flags, headerBytes, alias, fresh, ok = cc.mqttAliasedPublishFrame(p.pi, p.qos, p.dup, p.retain, p.topic, p.props, len(p.msg))
		if !ok {
			cc.mu.Unlock()
			return false
		}
	}
	// A fresh alias binding is committed only for a frame that may have reached the client.
	commitAlias := func() {
		if fresh {
			if cc.mqtt.outTopicAliases == nil {
				cc.mqtt.outTopicAliases = make(map[string]uint16)
			}
			cc.mqtt.outTopicAliases[string(p.topic)] = alias
		}
	}
	// No retained flush here any more (F-105 7c): retained replay is the session driver's, and live
	// traffic on a raw subscription waits on its replay gate BEFORE it reaches this writer.
	if p.requireAction {
		// THE AUTHORIZED BOUNDARY: header and payload are ONE execution of the section's clean
		// acquisition, on the connection it was acquired on, consumed once, and only while the
		// section still holds it (outbound_frame_write.go). PREVENTED emitted nothing.
		res, _ := cc.writeAuthorizedFrame(mqttFrameRequest{
			Action: p.out.action, Binding: cc.outBind, Holder: p.out,
			Parts: [][]byte{headerBytes, p.msg},
		})
		if res == mqttOutputPrevented {
			prevented = true
		} else {
			commitAlias()
		}
	} else {
		commitAlias()
		mqttTestObserveMixedSite(cc, mqttMixedSitePublishRaw, headerBytes)
		cc.queueOutbound(headerBytes)
		cc.queueOutbound(p.msg)
	}
	c.addToPCD(cc)
	trace := cc.trace
	cc.mu.Unlock()

	if prevented {
		// Nothing of THIS frame was emitted: the gate was sealed, the socket gone, or the section
		// no longer holds the acquisition.
		return false
	}

	if trace {
		pp := mqttPublish{
			topic: p.topic,
			flags: flags,
			pi:    p.pi,
			sz:    len(p.msg),
		}
		// Trace the resolved full topic (clearer than the empty wire topic) and note
		// the alias when one was used.
		tr := mqttPubTrace(&pp)
		if alias > 0 {
			tr += fmt.Sprintf(" alias=%v", alias)
		}
		cc.traceOutOp("PUBLISH", []byte(tr))
	}
	return true
}

// mqttEnqueueQoS0PublishMsgTo admits and transmits in one call a PUBLISH that bears NO
// packet identifier and so records no durable send intent: QoS 0 and the retained sends.
// The identifier is the literal zero by construction — there is no parameter for one — which
// is what lets the every-send-reserved guard prove this entry point cannot carry an
// identifier outside a send section. A QoS 1/2 sender calls the two phases itself, with
// recordSendIntent between them.
func (c *client) mqttEnqueueQoS0PublishMsgTo(cc *client, sub *subscription, retainAsPub bool, subID int, topic, msg, props []byte) bool {
	prep, ok := c.mqttPreparePublishMsgTo(cc, 0, 0, false, retainAsPub, subID, topic, msg, props)
	if !ok {
		return false
	}
	return c.mqttEnqueuePreparedPublishMsgTo(cc, sub, prep)
}

// Serializes to the given writer the message for the given subject.
func (w *mqttWriter) WritePublishHeader(pi uint16, qos byte, dup, retained, v5 bool, topic, props []byte, msgLen int) byte {
	// Compute len (will have to add packet id if message is sent as QoS>=1)
	pkLen := 2 + len(topic) + msgLen
	var flags byte

	// Set flags for dup/retained/qos1
	if dup {
		flags |= mqttPubFlagDup
	}
	if retained {
		flags |= mqttPubFlagRetain
	}
	if qos > 0 {
		pkLen += 2
		flags |= qos << 1
	}
	// MQTT 5.0 PUBLISH carries a properties section after the packet identifier.
	// props (when set) is the raw block from the publisher, including its length
	// prefix, forwarded verbatim; otherwise a single 0 length byte. For 3.1.1
	// receivers (v5 false) there is no properties section, which is how
	// properties get stripped when delivering to a 3.1.1 subscriber. Spec5 [3.3.2.3].
	if v5 {
		if len(props) > 0 {
			pkLen += len(props)
		} else {
			pkLen++
		}
	}

	w.WriteByte(mqttPacketPub | flags)
	w.WriteVarInt(pkLen)
	w.WriteBytes(topic)
	if qos > 0 {
		w.WriteUint16(pi)
	}
	if v5 {
		if len(props) > 0 {
			w.Write(props)
		} else {
			w.WriteByte(0)
		}
	}

	return flags
}

// Serializes to the given writer the message for the given subject. For v5
// receivers it appends the properties section (props, if any, else empty).
func mqttMakePublishHeader(pi uint16, qos byte, dup, retained, v5 bool, topic, props []byte, msgLen int) (byte, []byte) {
	headerBuf := newMQTTWriter(mqttInitialPubHeader + len(topic) + len(props))
	flags := headerBuf.WritePublishHeader(pi, qos, dup, retained, v5, topic, props, msgLen)
	return flags, headerBuf.Bytes()
}

// mqttAliasedPublishFrame frames an outbound v5 PUBLISH against this connection's
// outbound Topic Alias table WITHOUT mutating it, returning the flags, header, the alias
// used (0 = none/full topic), whether that alias is a FRESH binding the caller must commit
// before queueing the frame, and ok=false when the packet must be dropped because even the
// plain full-topic frame exceeds the client's Maximum Packet Size. cc.mu must be held.
//
// It is a pure decision so that ADMISSION (mqttPreparePublishMsgTo) and TRANSMISSION
// (mqttEnqueuePreparedPublishMsgTo) ask the same question and get the same answer: the
// table read and the caller's commit-plus-queueOutbound stay atomic at transmission, so a
// binding frame is buffered before any alias-only frame that references it, and a fresh
// alias is committed only once its binding frame is actually queued. Aliasing never
// causes an extra drop — the plain frame is always the fallback. Spec5 [3.3.2.3.4].
func (cc *client) mqttAliasedPublishFrame(pi uint16, qos byte, dup, retain bool, topic, props []byte, msgLen int) (flags byte, h []byte, alias uint16, fresh, ok bool) {
	mp := int(cc.mqtt.maxPacketSize)
	fits := func(h []byte) bool { return mp == 0 || len(h)+msgLen <= mp }

	// Already bound: send an empty topic + the alias. The alias-only frame can be
	// larger than the plain frame for a 1-2 byte topic (+3 alias bytes, -len(topic)
	// topic bytes), so it too is size-checked; on overflow fall through to the
	// plain frame while keeping the binding.
	if a, bound := cc.mqtt.outTopicAliases[string(topic)]; bound {
		if ap, ok := mqttInjectTopicAlias(props, a); ok {
			flags, h := mqttMakePublishHeader(pi, qos, dup, retain, true, nil, ap, msgLen)
			if fits(h) {
				return flags, h, a, false, true
			}
		}
	} else if len(cc.mqtt.outTopicAliases) < int(cc.mqtt.outTopicAliasMax) {
		// Unbound with room: the next alias (FCFS, table size + 1, never 0 and never
		// above the client's max since outTopicAliasMax = min(client, server)). The
		// binding frame carries the full topic; the caller commits only if it fits AND
		// it is about to queue this frame.
		a := uint16(len(cc.mqtt.outTopicAliases) + 1)
		if ap, ok := mqttInjectTopicAlias(props, a); ok {
			flags, h := mqttMakePublishHeader(pi, qos, dup, retain, true, topic, ap, msgLen)
			if fits(h) {
				return flags, h, a, true, true
			}
		}
	}

	// Table full, injection failed, or an aliased frame did not fit: send the plain
	// full-topic frame. alias is 0 so the trace does not claim an alias was sent.
	flags, h = mqttMakePublishHeader(pi, qos, dup, retain, true, topic, props, msgLen)
	return flags, h, 0, false, fits(h)
}

// Process the SUBSCRIBE packet.
//
// Process the list of subscriptions and update the given filter
// with the QoS that has been accepted (or failure).
//
// Spec [MQTT-3.8.4-3] says that if an exact same subscription is
// found, it needs to be replaced with the new one (possibly updating
// the qos) and that the flow of publications must not be interrupted,
// which I read as the replacement cannot be a "remove then add" if there
// is a chance that in between the 2 actions, published messages
// would be "lost" because there would not be any matching subscription.
//
// Run from client's readLoop.
// No lock held on entry.
// mqttResolveAfterPacket resolves an ambiguous record write left by the packet that
// just finished, once its record transaction and session lock are both released.
//
// It cannot be done from inside the packet: resolution holds the record fence for
// the whole decide-clear-reconcile transition, so an attempt made while the packet
// still holds its own transaction is simply refused — and blocking instead would
// deadlock the readLoop against itself. Doing it here is what keeps a connected
// client from sitting on a session that can no longer persist anything, since every
// later save is refused while it is unresolved.
//
// Runs from the client's readLoop, with no transaction held.
func (c *client) mqttResolveAfterPacket() {
	c.mu.Lock()
	sess := c.mqtt.sess
	c.mu.Unlock()
	if sess == nil || !sess.isSeqUncertain() {
		return
	}
	sess.resolveUncertainNow(c)
}

// mqttFlushVoidedRegs persists a void-register verdict the packet that just finished may
// have left un-recorded.
//
// It belongs here for the same reason mqttResolveAfterPacket does: an UNSUBSCRIBE that
// cancels a register-pending delivery makes the delivery goroutine own the verdict, and that
// goroutine is refused the fence for as long as the UNSUBSCRIBE holds it — while the
// UNSUBSCRIBE's own record write may already have been marshalled without the entry. This is
// the first moment the fence is free again, so it is where the verdict becomes durable
// rather than waiting for the session's next register store or resume.
//
// Runs from the client's readLoop, with no transaction held.
func (c *client) mqttFlushVoidedRegs() {
	c.mu.Lock()
	sess := c.mqtt.sess
	c.mu.Unlock()
	if sess == nil {
		return
	}
	sess.persistVoidedRegs(c)
}
