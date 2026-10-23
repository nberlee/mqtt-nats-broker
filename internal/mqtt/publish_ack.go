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

// Publish acknowledgements: PUBACK, PUBREC, PUBREL and PUBCOMP frames and responses, and
// processing a client's PUBACK, PUBREC and PUBCOMP.

package mqtt

import (
	"errors"
	"fmt"
	"sync/atomic"
)

func (c *client) mqttEnqueuePubResponse(packetType byte, pi uint16, trace bool) {
	c.mqttEnqueuePubResponseReason(packetType, pi, mqttReasonSuccess, _EMPTY_, trace)
}

func (c *client) mqttEnqueuePubResponseReason(packetType byte, pi uint16, reason byte, reasonStr string, trace bool) {
	buf := c.mqttPubResponseFrame(packetType, pi, reason, reasonStr)

	// Unreachable in practice: a connected client's Maximum Packet Size is bounded
	// below by its success CONNACK (~15+ bytes) so a 4/5-byte response always fits.
	// The check keeps every enqueue site provably compliant. [MQTT-3.1.2-24].
	if !c.mqttEnqueueFittingProto(buf) {
		return
	}
	c.mqttTracePubResponse(packetType, pi, reason, trace)
}

// mqttPubResponseFrame encodes a PUBACK, PUBREC, PUBREL or PUBCOMP, shared by the raw emitter and
// the authorized PUBREL write so both put the same bytes on the wire.
func (c *client) mqttPubResponseFrame(packetType byte, pi uint16, reason byte, reasonStr string) []byte {
	proto := [5]byte{packetType, 0x2, 0, 0, 0}
	proto[2] = byte(pi >> 8)
	proto[3] = byte(pi)
	n := 4
	// A non-success reason needs the longer v5 form: PI + reason code,
	// properties omitted. Spec5 [3.4.2.1]. 3.1.1 has no reason codes.
	if reason != mqttReasonSuccess && c.mqtt.proto == mqttProtoLevel5 {
		proto[1] = 0x3
		proto[4] = reason
		n = 5
	}

	// Bits 3,2,1 and 0 of the fixed header in the PUBREL Control Packet are
	// reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat
	// any other value as malformed and close the Network Connection [MQTT-3.6.1-1].
	if packetType == mqttPacketPubRel {
		proto[0] |= 0x2
	}

	// A failure reason may carry a Reason String when the client permits it and
	// it fits; otherwise fall back to the fixed short form. Spec5 [3.4.2.2].
	buf := proto[:n]
	if n == 5 {
		if rs := c.mqttReasonString(packetType, reasonStr); rs != _EMPTY_ {
			w := newMQTTWriter(0)
			w.WriteByte(proto[0])
			var vh mqttWriter
			vh.WriteUint16(pi)
			vh.WriteByte(reason)
			vh.writeProperties(&mqttProperties{reasonString: rs})
			w.WriteVarInt(vh.Len())
			w.Write(vh.Bytes())
			if c.mqttFitsClientMax(w.Len()) {
				buf = w.Bytes()
			}
		}
	}

	return buf
}

// mqttTracePubResponse traces one acknowledgement-family packet the way the emitter always has.
func (c *client) mqttTracePubResponse(packetType byte, pi uint16, reason byte, trace bool) {
	if !trace {
		return
	}
	name := "(???)"
	switch packetType {
	case mqttPacketPubAck:
		name = "PUBACK"
	case mqttPacketPubRec:
		name = "PUBREC"
	case mqttPacketPubRel:
		name = "PUBREL"
	case mqttPacketPubComp:
		name = "PUBCOMP"
	}
	if reason != mqttReasonSuccess && c.mqtt.proto == mqttProtoLevel5 {
		c.traceOutOp(name, []byte(fmt.Sprintf("pi=%v rc=%v", pi, reason)))
	} else {
		c.traceOutOp(name, []byte(fmt.Sprintf("pi=%v", pi)))
	}
}

// Process a PUBACK (QoS1) or a PUBREC (QoS2) packet, acting as Sender. Set
// isPubRec to false to process as a PUBACK. reason is the MQTT 5.0 reason code
// (Success for 3.1.1 or the short form). A PUBREC carrying a failure code
// (>= 0x80) means the receiver rejected the QoS2 message: per Spec5 [4.3.3] the
// sender MUST NOT send a PUBREL and treats the message as unacknowledged/done,
// so we release the packet identifier and stop redelivery without moving it
// into the PUBREL (awaiting-PUBCOMP) state.
//
// Runs from the client's readLoop. No lock held on entry.
func mqttPublishAckMayReleaseSlot(isPubRec, pubRecFailed, deferredRelease bool) bool {
	// A successful PUBREC has already created the successor exchange even when the
	// canonical readback cannot yet move its slot out of PUBLISH. That route-B shape
	// must keep the same owner until PUBCOMP. The send-settlement path owns every
	// deferred release, so this handler must not race it either.
	return !deferredRelease && (!isPubRec || pubRecFailed)
}

// mqttTestPubRecHandoffV holds a successful PUBREC just after its handoff released the session
// lock, before its durable intent: the exchange is past PUBREC in memory and nothing durable has
// moved. Nil outside tests.
var mqttTestPubRecHandoffV atomic.Pointer[func(pi uint16)]

func (c *client) mqttProcessPublishReceived(pi uint16, isPubRec bool, reason byte) (err error) {
	sess := c.mqtt.sess
	if sess == nil {
		return errMQTTInvalidSession
	}

	pubRecFailed := isPubRec && reason >= 0x80

	var jsAckSubject, q2AffClaim, q2AffCopy, pubRelSubject string
	var removed *mqttPending
	// retainedPubRel: this PUBREC advanced a retained exchange, whose PUBREL is emitted directly.
	var retainedPubRel bool
	defer func() {
		if retainedPubRel && err == nil {
			sess.emitRetainedPubRel(c, pi)
		}
	}()
	// Set when this PUBREC stored a PUBREL for a DRAINED source, whose claim only a
	// confirmed PUBREL may release.
	var drainedPubRel bool
	sess.mu.Lock()
	// Must be the same client, and the session must have been setup for QoS2.
	if sess.c != c {
		sess.mu.Unlock()
		return errMQTTInvalidSession
	}
	if sess.pendingPublish[pi] == nil &&
		!(isPubRec && !pubRecFailed && sess.pendingPubRel[pi] != nil) {
		arrival := mqttAckArrival{Packet: mqttPacketPubAck, Reason: reason}
		if isPubRec {
			arrival.Packet = mqttPacketPubRec
		}
		binding, phase := sess.resolveAbandonedAckLocked(pi, arrival)
		if binding == nil {
			sess.mu.Unlock()
			return nil // No captured obligation: never read an occupant by PI to invent one.
		}
		ack := mqttAckSnapshot(binding)
		if phase != mqttExchangePubRecObserved {
			sess.holdTerminalCleanupLocked(pi, binding.nonce)
			if ack.slot.namesSource() { // a retained abandonment settles no source (F-105 7c)
				sess.holdTerminalSourceLocked(ack.slot)
			}
		}
		sess.mu.Unlock()
		// Terminal completion settles the source from the durable register reply.
		// Successful PUBREC retains its independent intent/materialization owner.
		if phase == mqttExchangePubRecObserved {
			// A DRAINED source records its PUBREL intent BEFORE the exchange moves (I11), exactly
			// as the live-entry path below does: the drain's claim is released only by a
			// CONFIRMED PUBREL, and the canonical PUBREL is delivered only for the exchange the
			// intent names. An AckRoute installed by a drain-owned deny reaches here with the
			// drain still claiming its source (F-088 T29: without the intent the PUBREL was
			// withheld forever and the drain stayed claimed).
			if _, _, drained := sess.drainPubRelState(binding.dur, binding.seq); drained {
				if _, _, ierr := sess.recordPubRelIntent(c, binding.dur, binding.seq, pi, binding.nonce); ierr != nil {
					return ierr
				}
			}
			switch sess.intendPubRelSlot(c, ack) {
			case mqttPubRelIntentUnresolved, mqttPubRelIntentStillOwed:
				sess.scheduleIntentResolution(c, pi, ack)
			case mqttPubRelIntentConfirmed:
				// An ABANDONED RETAINED exchange's advance created its canonical PUBREL (F-105 7c):
				// nothing is materialized, so the PUBREL entry is installed here and emitted from it.
				if binding.payload != _EMPTY_ {
					sess.continueAbandonedRetainedPubRel(c, pi, ack)
				}
			case mqttPubRelIntentNotRecorded:
				// PROVEN NOT RECORDED, on a retained exchange: the client's PUBREC is answered by
				// nothing on this connection — no materializer, no rollback to a PUBLISH — so it is
				// closed, as a live PUBREC whose advance is not recorded is; the resume restores
				// the ACK-only identity and the client's PUBREC is taken again.
				if binding.payload != _EMPTY_ {
					return errMQTTPubRelIntentNotRecorded
				}
			}
		} else {
			sess.completeAcknowledgedPublish(c, pi, ack)
		}
		return nil
	}
	if ack := sess.pendingPublish[pi]; ack != nil && !ack.acts() {
		// A reservation that this acknowledgement cannot legitimately advance, declined
		// before any irreversible work — trackAsPubRel, the PUBLISH untrack, the PUBREL
		// store, the source ack and the register release all happen below this point.
		//
		// A MARKER-OPAQUE reservation is the fourth shape, declined for regForeign's
		// reason: whether its PUBLISH was ever transmitted is unknown, so nothing here may
		// settle the exchange; the next resume reads the pair whole (N-7.2).
		//
		// THREE shapes reach here, and what unifies them is that this handler cannot
		// ADVANCE any of them safely — not that the acknowledgement must be illegitimate.
		// R1 makes the illegitimacy argument hold for an ordinary allocation, whose PUBLISH
		// was never sent. It does NOT hold for one that adopted an occupant: the same-source
		// replacement arm preserves a live occupant's QoS precisely because the client may
		// already be holding that exchange, so its acknowledgement can be entirely
		// legitimate. The decline is still correct, for the mechanical reasons below, and it
		// is not self-healing: nothing replays a discarded acknowledgement. A confirmed
		// store clears regPending and a LATER acknowledgement can proceed, ordinarily after
		// another PUBLISH — conditional progress, not a guarantee.
		//
		//   - regForeign: the register behind this identifier is a record recovery could not
		//     interpret, so the exchange is not this object's to advance.
		//   - regPending: allocated but its register store has not confirmed. Accepting a
		//     PUBREC here would trackAsPubRel a bodyless entry carrying jsDur=pubRelDurable()
		//     and regSeq 0 — indistinguishable from a live retained exchange — tear down the
		//     PUBLISH entry so the store then finds it gone and transmits nothing, and ack
		//     the source away. PUBREL delivery would later admit that entry as retained and
		//     put a PUBREL on the wire with no durable action: F-092's own class plus the
		//     lost source. The failed arm skips the PUBREL but still untracks and acks the
		//     source, the same loss without the stray PUBREL.
		//   - regCancelled: a settled-but-unproven cancellation placeholder is bodyless and
		//     equally never transmitted; a spurious success would convert it into the same
		//     retained-looking PUBREL shape.
		//
		// For regForeign, acting would convert the reservation into durable state for an
		// exchange this object does not own — an UNCORRELATED PUBREL a later resume reads as
		// identifier-first with no source mapping, conflicting with the still-live register.
		// Decline is identical for all three: the reservation stands, nothing durable is
		// written, and a conforming client's real acknowledgement arrives only after the
		// PUBLISH is actually transmitted (regPending cleared) or once recovery has resolved
		// the reservation (regForeign). This is a shared PUBACK/PUBREC handler,
		// so a spurious QoS 1 PUBACK of a never-transmitted PUBLISH is declined the same way,
		// which is equally correct under R1.
		sess.mu.Unlock()
		return nil
	}
	// Captured with the connection/generation check. The session object is reused by
	// clear(), which remints its generation and PUBREL subject; a delayed handler must
	// store this exchange's record on the subject belonging to the slot it removed, not
	// whatever generation the object happens to carry after the lock is dropped.
	pubRelSubject = sess.pubRelSubject
	if isPubRec && !pubRecFailed {
		// A DUPLICATE PUBREC is classified BEFORE either map is touched, and that order is
		// the whole point.
		//
		// The client may retransmit PUBREC ([MQTT-4.3.3]) and the original handoff has then
		// already replaced pendingPublish[pi] with a PUBREL entry. Calling trackAsPubRel
		// first would overwrite that live entry with an empty placeholder, and the rest of
		// this handler would then find nothing to work from — no source, and no exchange
		// identity for the record it is about to store. The store would be refused, the
		// rollback would delete the placeholder, and the handler would return an error that
		// TERMINATES THE CONNECTION, taking the live PUBREL tracking with it. A legitimate
		// duplicate acknowledgement must not do any of that.
		//
		// The PUBLISH entry's absence is what identifies the duplicate: this exchange is
		// already past PUBREC, so there is nothing to hand off and nothing to store. The
		// existing PUBREL entry keeps its own identity and its own ack subject.
		//
		// But it must be ANSWERED, not merely left alone. A client retransmits PUBREC
		// precisely because it did not receive the PUBREL ([MQTT-4.3.3]), so returning
		// silently leaves it waiting for JetStream's AckWait — a configurable interval that
		// can be long — and at a Receive Maximum of 1 that stalls the session's whole
		// outbound QoS 1/2 stream behind one exchange. An earlier revision of this branch
		// did exactly that, and its comment claimed the retransmission was "answered by the
		// exchange that already exists", which nothing performed.
		//
		// The answer is a NAK on the stored PUBREL's own delivery, so JetStream re-offers
		// it AT ONCE and the ordinary delivery path retransmits it — with that path's
		// fencing, its send holds and its acknowledgement debt intact. Writing a PUBREL
		// from this read loop instead would duplicate the send section's job and bypass
		// them. Sent after the lock is released, like every other NAK here.
		//
		// A placeholder entry with no ack subject yet is the window between trackAsPubRel
		// and the first delivery attempt: its original store is still in flight and will
		// deliver, so there is nothing to re-offer and nothing to wait for. That case needs
		// no branch here — the subject is empty and sendMsg declines it.
		if _, hasPub := sess.pendingPublish[pi]; !hasPub {
			if rel, hasRel := sess.pendingPubRel[pi]; hasRel {
				if rel.kind == mqttPendingRetained {
					// A RETAINED exchange's PUBREL has no $MQTT_out delivery to re-offer (F-105 7c):
					// a NEW PUBREL action is acquired on the register and written from the canonical.
					sess.mu.Unlock()
					sess.emitRetainedPubRel(c, pi)
					return nil
				}
				dupNak := _EMPTY_
				if rel.acts() {
					// Only an entry that ACTS for its exchange may have its delivery re-offered;
					// a reservation's ACK subject, if it carries one, authorizes nothing.
					dupNak = rel.jsAckSubject
				}
				sess.mu.Unlock()
				// Unconditionally: sendMsg's own first line returns on an empty subject, so
				// a guard here would be a second mechanism stating one fact — and a
				// mutation proved it unobservable, which is what that always looks like.
				sess.jsa.sendMsg(dupNak, AckNak)
				return nil
			}
		}
		// A section still transmitting this PUBLISH is told FIRST, while the entry it was opened
		// for is still resident to name it: the PUBREC moves the exchange past its PUBLISH, so
		// that section must not now begin its write (F-115).
		sess.answerOpenPublishLocked(pi)
		// The JS ACK subject for the PUBREL will be filled in at the delivery
		// attempt.
		sess.trackAsPubRel(pi, _EMPTY_)
	}
	// An IDENTIFIER-FREEING acknowledgement arriving while this entry's transmission has
	// not returned parks its freeing half instead of performing it (F-087 WP5, class D).
	// TCP ordering proves only that this acknowledgement followed an EARLIER
	// transmission; it says nothing about a concurrent REDELIVERY of the same entry whose
	// write is blocked on this same connection, the read loop and the JetStream
	// dispatcher being different goroutines. Freed here, the identifier is re-minted and
	// the new exchange's PUBLISH queues behind the blocked duplicate under one identifier.
	//
	// A SUCCESSFUL PUBREC is exempt and needs no transfer machinery: it replaces the
	// entry with a PUBREL one, and the identifier is held by the sendsInFlight registry
	// rather than by either entry.
	// canonicalPubRelSeq is the $MQTT_out sequence of the PUBREL record this PUBREC
	// stores, and it is what the slot's canonical tuple names once the phase CAS runs.
	// Zero until the store returns, and zero on every path that stores nothing.
	var canonicalPubRelSeq uint64
	deferredRelease := false
	if !isPubRec || pubRecFailed {
		kind := mqttSendSettleAckComplete
		if pubRecFailed {
			kind = mqttSendSettlePubRecFailed
		}
		// The acknowledged PUBLISH is not written by a section still open for it (F-115); the
		// settle parked below still runs at that section's end.
		sess.answerOpenPublishLocked(pi)
		if sess.condemnSendLocked(pi, kind) {
			// The protocol-visible work below proceeds on a SNAPSHOT — the response
			// packet and the source acknowledgement free no identifier — while the
			// untrack and the register delete wait for endSend.
			snap := *sess.pendingPublish[pi]
			removed, deferredRelease = &snap, true
		}
	}
	// The nonce whose identifier the untrack below holds, if any. Released on return unless
	// the terminal completion took the hold over: arms that never run it — a drained source,
	// an affine arm keeping its register — would otherwise leak the identifier for the life
	// of the session.
	var heldTerminalNonce string
	if !deferredRelease {
		removed = sess.untrackPublishEntry(pi, mqttUntrackAnswered)
		if removed != nil && removed.slot.slotOK() &&
			mqttPublishAckMayReleaseSlot(isPubRec, pubRecFailed, deferredRelease) {
			// The acknowledged exchange's IDENTIFIER stays held, in this same critical
			// section, until its terminal cleanup is confirmed — capacity is freed as the
			// protocol says, the identifier is not (holistic review round 1, finding 5).
			sess.holdTerminalCleanupLocked(pi, removed.slot.nonce)
			heldTerminalNonce = removed.slot.nonce
		}
	}
	if removed != nil {
		jsAckSubject, q2AffClaim, q2AffCopy = removed.jsAckSubject, removed.q2AffClaim, removed.q2AffCopy
		// A retained exchange's acknowledgement settles no source (F-105 7c), so it installs no
		// source owner: its identifier hold above is released by the terminal attempt itself.
		if (!isPubRec || pubRecFailed) && removed.slot.slotOK() && removed.slot.namesSource() &&
			!mqttDurIsSlotOnlyClass(removed.jsDur) {
			sess.holdTerminalSourceLocked(removed.slot)
		}
	}
	// PUBACK / failed PUBREC freed an in-flight slot (a successful PUBREC is
	// net-zero under a Receive Maximum and yields no NAKs): release withheld
	// deliveries promptly. Spec5 [4.9].
	deferredNaks := sess.deferredNaksLocked()
	sess.mu.Unlock()
	if isPubRec && !pubRecFailed {
		if h := mqttTestPubRecHandoffV.Load(); h != nil {
			(*h)(pi)
		}
	}

	// PROTOCOL EVIDENCE (N-8.3 items 1 and 3): the client's acknowledgement proves the
	// PUBLISH was received, so the exchange's unsent marker — if the sender's own delete
	// was lost — is cleaned here, at the sequence the entry captured (row 21). A none-held
	// entry has nothing captured and is met by terminal cleanup's reload instead. Issued
	// outside the lock and never awaited; a register the section still owns is untouched.
	if removed != nil && removed.markerSeq != 0 && removed.slot.name.subject != _EMPTY_ && sess.asm != nil {
		sess.asm.cleanMarkerAfterEvidence(c, removed.slot.name.subject, removed.markerSeq)
	}

	// While the freeing half is parked, the register delete is parked with it: endSend
	// owns both, and issuing the delete here would remove the identity of a PUBLISH still
	// being written.
	terminalOwned := false
	defer func() {
		if heldTerminalNonce != _EMPTY_ && !terminalOwned {
			sess.releaseTerminalCleanupHold(pi, heldTerminalNonce)
		}
	}()
	releaseRegister := func(ack *mqttPending) {
		// ONE GATE FOR THE WHOLE-LIFE RULE. A successful PUBREC creates the PUBREL
		// exchange even when its canonical readback cannot yet re-phase the slot, so a
		// still-PUBLISH shape is route-B authority—not permission to run the old
		// PUBLISH-completion delete. Keeping the rule here means no QoS 2 source arm can
		// accidentally revive that delete by calling this convenience closure.
		if !mqttPublishAckMayReleaseSlot(isPubRec, pubRecFailed, deferredRelease) {
			return
		}
		// THE DURABLE TERMINAL PHASE COMES FIRST, and the release is its consequence
		// rather than its equal. Deleting straight from `publish` is what left terminal
		// authority existing only in this goroutine: a peer, or this broker after a
		// restart, would find an ordinary `publish` slot with nothing positive to
		// release it on.
		//
		// A failed advance withholds the release. The slot, the identifier and its
		// Receive Maximum capacity stay held, and recovery finishes the exchange from
		// the phase it finds — which is exactly the case that must NOT be papered over
		// by deleting anyway.
		//
		// A step that cannot be confirmed NOW is handed to the terminal continuation with the
		// identifier still held, never dropped: before, a failed advance only logged and a
		// failed delete was ignored, and a connected idle session had no continuation at all.
		terminalOwned = true
		if ack.res == nil {
			// No section ever opened for this exchange — a retained send (R6), or an entry
			// recovery installed — so there is no acquisition to wait on and the entry's own
			// captured identity is the binding.
			sess.completeAcknowledgedPublish(c, pi, ack)
			return
		}
		// THROUGH THE CELL, even here (holistic review round 7, finding 1): an acquisition
		// whose outcome is still unresolved may already have advanced the record past this
		// entry's captured counter, and this path is reached AFTER the section closed, which
		// the token-keyed hand-off did not cover.
		sess.mu.Lock()
		run, bound := sess.registerAckCleanupLocked(ack.res, pi)
		sess.mu.Unlock()
		if run {
			sess.completeAcknowledgedPublish(c, pi, &mqttPending{slot: bound})
		}
	}

	if isPubRec && !pubRecFailed {
		// Name the source delivery this PUBREL completes. After a restart nothing
		// else links the two — cpending is memory-only — so without it a redelivery
		// of the source would allocate a fresh identifier and resend the payload of
		// an exchange already past PUBREC. Absent for a delivery we have no pending
		// entry for, which is exactly the uncorrelated form written before this
		// existed. See pubrel_correlation.go.
		//
		// Deliberately NO deterministic Nats-Msg-Id here. On this path the source is
		// acked right after the store, so the only legitimate re-store of the same
		// (durable, sequence) is the post-restart one that follows a redelivery — and
		// a surviving dedupe entry would silently drop it, leaving the client waiting
		// for a PUBREL that will never exist. A deduplication identity is only safe
		// where a presence check backs it up.
		var srcDur string
		var srcSeq uint64
		if removed != nil {
			srcDur, srcSeq = removed.jsDur, removed.sseq
		}
		// A DRAINED source records its intent BEFORE the store (I11): the record
		// either owes a PUBREL — recreate it, under this identifier — or it does
		// not and the payload may be redelivered. Nothing else makes recovery
		// positive. A definite failure rolls the whole PUBREC back so the exchange
		// simply restarts; an ambiguous one leaves the phase uncertain, where
		// neither resending nor finalizing is permitted.
		var msgId string
		if _, _, drained := sess.drainPubRelState(srcDur, srcSeq); drained {
			// The source claim may NOT be released here. A store response is not a
			// record (I12): on the interest-retained $MQTT_out a store with no
			// consumer succeeds, keeps nothing, and reports an ordinary success — so
			// acking the source now would discard the payload for a PUBREL that does
			// not exist, and the client would wait for a PUBCOMP exchange nothing can
			// advance. Only a CONFIRMED PUBREL — delivered, or found present at its
			// sequence by the resume barrier — may finalize it (I2b).
			drainedPubRel = true
			// The intent carries the EXCHANGE identity, not a fresh one: recovery may have
			// to recreate this record, and only the identity the owner slot already names
			// makes the recreate the same exchange rather than a new one.
			var intentOwner string
			if removed != nil {
				intentOwner = removed.regNonce
			}
			attempt, definite, ierr := sess.recordPubRelIntent(c, srcDur, srcSeq, pi, intentOwner)
			if ierr != nil {
				if definite {
					sess.mu.Lock()
					if ack, ok := sess.pendingPubRel[pi]; ok && ack != nil && ack.jsAckSubject == _EMPTY_ {
						delete(sess.pendingPubRel, pi)
						sess.retrackPublishLocked(pi, removed)
					}
					sess.mu.Unlock()
				}
				return ierr
			}
			// Only a drained PUBREL carries a deduplication identity, because only
			// here is it backed by a presence check on recovery. See
			// pubrel_correlation.go.
			msgId = mqttPubRelMsgId(sess.idHash, srcDur, srcSeq, attempt)
		}
		// Name the shared QoS 2 ASSIGNMENT this PUBREL completes, as a bound
		// (claim subject, copy subject) pair. Both are already in hand — they came off
		// the pending entry a few lines above — so this costs no lookup and no
		// ordering.
		//
		// It is the durable, account-wide evidence that the client answered PUBREC for
		// this copy, and it is stored HERE, before mqttFinishQ2AffineDelivery records
		// AffineDone. That order is what creates the window this reference closes: a
		// broker that stops in between, with the affine consumer then destroyed out of
		// band, used to leave `materialized ∧ ¬AffineDone ∧ copy absent` and be reported
		// as a LOST delivery for a message the client had already answered. The PAIR
		// rather than the claim subject alone, because a claim subject is reused by a
		// later incarnation. See pubrel_correlation.go and qos2_shared_affine.go.
		// ownerNonce is the EXCHANGE's identity, taken off the entry this PUBREC removed
		// rather than minted here: the record is evidence about that exchange, and a fresh
		// identity would make it evidence about nothing. It is the same value the durable
		// owner slot carries, so the delivery callback can match the two.
		//
		// A delivery with no entry to take it from reaches this only as a duplicate or
		// stale PUBREC, which the handler declines before here; the builder refuses an
		// invalid nonce as a second line rather than writing an unattributable record.
		var ownerNonce string
		if removed != nil {
			ownerNonce = removed.regNonce
		}
		natsMsg, headerLen := mqttNewCorrelatedPubRel(pi, ownerNonce, srcDur, srcSeq, msgId, q2AffClaim, q2AffCopy)
		if natsMsg == nil {
			// No exchange identity, so nothing may be stored: an unattributable record
			// would be refused by every reader anyway, and storing it would leave stream
			// residue an operator has to remove. Roll the PUBREC back exactly as a failed
			// store does below.
			sess.mu.Lock()
			if ack, ok := sess.pendingPubRel[pi]; ok && ack != nil && ack.jsAckSubject == _EMPTY_ {
				delete(sess.pendingPubRel, pi)
				sess.retrackPublishLocked(pi, removed)
			}
			sess.mu.Unlock()
			return errMQTTInvalidSession
		}
		// THE PUBREC HANDOFF. The slot is no longer DELETED here: it is re-phased to
		// pubrel_ready and carries the exchange for the rest of its life, which is what
		// makes it the identifier's single durable claimant rather than one of two
		// independently changing durable sets nothing could arbitrate between.
		//
		// The store's own sequence is what names the canonical record. That is "the
		// actual message" rather than an inference — never Nats-Msg-Id, never the drain
		// attempt counter, never the source sequence, all three of which name something
		// other than this record's position in its consumer's delivery space.
		//
		// A store response is NOT a presence proof (I12): on the interest-retained
		// $MQTT_out a store with no consumer succeeds, keeps nothing and reports success.
		// The returned sequence is therefore only a candidate. phaseSlotToPubRelReady reads
		// it back and writes the canonical tuple only from a present, matching record; a
		// skipped store leaves the slot in publish for recovery.
		var presp *JSPubAckResponse
		// THE INTENT, BEFORE THE STORE. Batches are per stream, so the record on
		// `$MQTT_out` and the slot on `$MQTT_inflight` can never be written atomically;
		// what CAN be made safe is the ORDER. A crash after this CAS leaves a slot that
		// says "past PUBREC, canonical record owed", which no peer may reap; a crash
		// after the store with the slot still in `publish` is the window this closes.
		//
		// A refusal rolls the whole PUBREC back through the existing arm below, rather
		// than storing under a slot that still reads PUBLISH.
		intent := sess.intendPubRelSlot(c, removed)
		switch intent {
		case mqttPubRelIntentNotRecorded, mqttPubRelIntentEnded, mqttPubRelIntentOther:
			err = errMQTTPubRelIntentNotRecorded
		case mqttPubRelIntentUnresolved, mqttPubRelIntentStillOwed:
			// NEITHER continuation is safe yet, so neither is taken: no store under a slot
			// that may still read PUBLISH, and no rollback over a phase that may be durable.
			// The placeholder stays and an identity-bound resolver decides.
			err = errMQTTPubRelIntentNotRecorded
			sess.scheduleIntentResolution(c, pi, removed)
		default:
			if removed != nil && removed.kind == mqttPendingRetained {
				// A RETAINED exchange's canonical PUBREL was created by the advance itself, on
				// $MQTT_inflight (F-105 7c): nothing is stored on $MQTT_out, and the PUBREL goes
				// out directly once this handler releases the locks.
				retainedPubRel = true
				break
			}
			presp, err = sess.jsa.storeMsg(pubRelSubject, headerLen, natsMsg)
		}
		if presp != nil {
			canonicalPubRelSeq = presp.Sequence
		}
		if err == nil && !retainedPubRel {
			// RE-PHASE IMMEDIATELY from the PROVEN record, and the store-then-prove
			// ordering is a correctness property rather than tidiness.
			//
			// Admission no longer requires a pre-existing `pubrel_ready` state:
			// admitStoredPubRel can atomically take the direct `publish -> pubrel_sending`
			// arrow from a delivered record proven to match the slot. This handoff still
			// runs at the earliest handler-side proof point. If it wins before delivery
			// admission it installs the canonical PUBREL tuple and leaves admission on the
			// ordinary `pubrel_ready -> pubrel_sending` arrow; if delivery wins first, the
			// direct arrow installs that tuple together with the action and this CAS safely
			// loses. Either way no exchange stalls on AckWait, which is what the ported
			// suite's timeouts were before the direct arrow existed.
			//
			// A crash or failed handoff after the store leaves "record stored, slot still
			// publish". A live delivery can admit that shape through the direct arrow, and
			// after a restart recovery's route B completes it. The unsafe reverse is
			// advancing the slot from an unproven store RESPONSE so it names a record that
			// was never retained; storing and then proving the exact record by readback
			// before this CAS is what prevents that.
			sess.phaseSlotToPubRelReady(c, removed, canonicalPubRelSeq)
		}
		if err != nil {
			// The in-memory state was advanced BEFORE the store (it has to be:
			// mqttDeliverPubRelCb calls trackAsPubRel itself, so storing first would
			// race a real ack subject against our empty placeholder and lose it). With
			// the store failed, that leaves the identifier wedged in pendingPubRel
			// forever — nothing will ever fill its ack subject — and the delivery's
			// PUBLISH tracking gone, so it can neither be redelivered nor acked. Roll
			// both back.
			//
			// CONDITIONALLY, though. A storeMsg request can time out while the store
			// LANDED and its PUBREL was already delivered, and the callback's
			// trackAsPubRel(pi, reply) has then filled jsAckSubject. An unconditional
			// rollback would delete that real entry — leaking the identifier and
			// leaving a delivered PUBREL that no PUBCOMP can ack — and reinstate stale
			// PUBLISH tracking. So roll back ONLY while the entry is still our empty
			// placeholder: a filled one is proof the store landed, and the callback
			// owns completion from there.
			//
			// AND NOT AT ALL ONCE THE INTENT IS DURABLE (F-098). `intendPubRelSlot` moves
			// the slot to `pubrel_intended` BEFORE the store, and refuses the store if it
			// cannot — so a slot-backed exchange reaching this failure has the client's
			// PUBREC written down. Rolling it back to PUBLISH would contradict that
			// record: the payload would be re-offered for a step the protocol has moved
			// past, and recovery would meet a durable past-PUBREC phase beside an
			// in-memory PUBLISH entry. What the failed store costs is the RECORD, not the
			// phase, and L1's materialization — enrolled by the same transition that
			// recorded the intent — is the continuation that rebuilds it. The empty
			// PUBREL entry stays and holds the identifier; the materialized record's own
			// delivery fills it through trackAsPubRel.
			//
			// A RETAINED exchange has no slot (R6), so nothing durable records its PUBREC
			// and the rollback remains its only continuation.
			//
			// KEYED ON THE CLASSIFIED INTENT, never on the slot's existence: a slot whose intent
			// CAS definitely failed still reads PUBLISH, and suppressing the rollback there
			// stranded the placeholder with nothing enrolled (holistic review round 1,
			// finding 4). Confirmed and unresolved intents are the two that may be durable.
			sess.mu.Lock()
			if ack, ok := sess.pendingPubRel[pi]; ok && ack != nil && ack.jsAckSubject == _EMPTY_ &&
				!intent.intentRecorded() {
				delete(sess.pendingPubRel, pi)
				sess.retrackPublishLocked(pi, removed)
			}
			sess.mu.Unlock()
			// Failure to send out PUBREL will terminate the connection.
			return err
		}
	}

	// Send the ack to JS to remove the pending message from the consumer. On a
	// failed PUBREC this still applies: the receiver has rejected the message,
	// so it must not be redelivered.
	//
	// A session-affine shared QoS 2 copy takes the confirmed route instead: its
	// assignment claim may only be reaped once the affine side is PROVEN complete,
	// and a queued ack proves nothing. Ordered after the PUBREL store above, so the
	// copy is never released before the exchange has somewhere to continue from.
	// The QoS 2 slot is retained and re-phased through PUBCOMP; QoS 1 and rejected
	// deliveries still release their register here. The per-branch ordering remains
	// important because a source acknowledgement must be confirmed before its durable
	// suppression or retirement evidence can be removed.
	switch {
	case removed != nil && removed.kind == mqttPendingRetained && isPubRec && !pubRecFailed:
		// A RETAINED exchange past PUBREC has no source to acknowledge and nothing to retire
		// (F-105 7c): its PUBREL is the canonical the advance created, emitted on return.
	case removed != nil && (!isPubRec || pubRecFailed) && removed.slot.slotOK() &&
		!mqttDurIsSlotOnlyClass(removed.jsDur):
		// Advance the protocol now; only the terminal owner may settle this source.
		releaseRegister(removed)
		// A RETAINED exchange's ending is the wake for its copy's reclamation (F-105 7c).
		if removed.kind == mqttPendingRetained && removed.retainedInc != _EMPTY_ {
			sess.wakeRetainedReclaim(removed.ev.gen, removed.retainedInc)
		}
	case removed != nil && removed.slot.abandonedAck != nil:
		// C4 source-settlement seam for a live PUBREC that raced durable abandonment.
		// Protocol advancement is independent; its source reply is not ACK authority.
		releaseRegister(removed)
	case q2AffClaim != _EMPTY_:
		// RETIRE FIRST, on a successful PUBREC, and only then attempt the affine side.
		// The order is the correctness property and the reason is the same one the
		// unconfirmed-source-ack arm below states: the in-memory half is what suppresses
		// a redelivery arriving on this broker, and this handler has ALREADY removed the
		// pending entry and its source-to-identifier mapping. The completion below can
		// spend several JetStream load-and-CAS retries failing, and if AckWait expires
		// inside that interval the delivery callback finds no retirement, trackPublish
		// finds no mapping, allocates a FRESH identifier and re-sends a payload the
		// client has already answered with PUBREC. Installing the retirement afterwards
		// leaves exactly that window open.
		//
		// Gated on a SUCCESSFUL PUBREC, and the gate is the point rather than a
		// precaution. This case is selected by q2AffClaim alone, so it also sees a
		// NEGATIVE PUBREC — and there no pendingPubRel entry and no PUBREL record are
		// created, so there is no PUBCOMP and therefore no event at which an unlanded
		// durable mark could ever be retried. Retiring there would install machinery
		// whose retry point does not exist. A rejected delivery keeps its pre-F-065
		// behaviour instead: complete the affine side if it can be, then release the
		// register. Nothing is stranded by that, because with no PUBREL there is nothing
		// for recovery to arbitrate against and the live copy stays independently
		// attributable — its claim from its header, its incarnation from its own subject.
		affineRetired := isPubRec && !pubRecFailed && removed != nil
		var retiredKey mqttDeferredKey
		if affineRetired {
			retiredKey = mqttDeferredKey{removed.jsDur, removed.sseq}
			sess.mu.Lock()
			sess.retireSourceLocked(removed.jsDur, removed.sseq, &mqttRetiredSource{
				pi:         pi,
				regSeq:     removed.regSeq,
				nonce:      removed.regNonce,
				q2AffClaim: q2AffClaim,
				q2AffCopy:  q2AffCopy,
			})
			sess.mu.Unlock()
		}
		if recorded, _ := c.mqttFinishQ2AffineDelivery(q2AffClaim, q2AffCopy, jsAckSubject); recorded {
			// Discharged, so the retirement has nothing left to suppress and no durable
			// mark was ever written for it: drop it here rather than leaving one to
			// outlive its exchange and be re-established by every later resume. The
			// assignment claim and copy are no longer owed.
			if affineRetired {
				sess.mu.Lock()
				delete(sess.srcAckOwed, retiredKey)
				sess.mu.Unlock()
			}
			// The closure is the single whole-life gate: successful PUBREC keeps the
			// slot even if canonical readback left it in PUBLISH for route B; rejected
			// PUBREC created no PUBREL exchange and releases it.
			releaseRegister(removed)
			break
		}
		// NOT recorded. The retirement stays, and now it needs its durable half: the
		// PUBREL is already stored and its delivery is in flight, so a PUBCOMP that found
		// no retirement would acknowledge that PUBREL and remove the last durable
		// evidence that this source is past PUBREC. The register is that retirement's
		// durable half and is deliberately kept.
		if affineRetired {
			// An unproven acknowledgement is ambiguous: read the source first, so a
			// retirement with nothing to suppress is dropped rather than made durable.
			if !sess.resolveRetiredSource(c, retiredKey) {
				sess.secureRetiredSource(c, retiredKey)
			}
			c.Warnf("MQTT: keeping the in-flight register of packet identifier %d for client %q and retiring its source: "+
				"the affine side of assignment %q is unproven", pi, sess.id, q2AffClaim)
			break
		}
		// A rejected delivery: no retirement, and the register is released as it was
		// before F-065. The copy stays unacknowledged, so AckWait re-offers it and the
		// gate re-runs once the claim store heals.
		releaseRegister(removed)
	case drainedPubRel:
		// Deliberately nothing — not even the register delete. See the intent block
		// above: the claim and its JetStream delivery are held until the PUBREL is
		// proven to exist, so a PUBREL that was silently kept nowhere costs a
		// redelivery rather than the message. The register is that redelivery's
		// identity and must outlive an unproven PUBREL. Once the PUBREL is proven, it
		// remains the whole-life owner slot until PUBCOMP; the drain completion settles
		// only the source. A non-drained QoS 2 source still acks here, as it always has.
	case deferredRelease && sess.drainOwnsSource(removed):
		// PARKED WITH THE SETTLEMENT, and so is the source acknowledgement the drain owns.
		//
		// The send section that transmitted this PUBLISH is still open, which is why the
		// register delete was parked on it above ("issuing the delete here would remove the
		// identity of a PUBLISH still being written"). Finalizing the drain here releases
		// that same register, so it has to wait for the same event: run now, it deletes under
		// the writer's live action — the premature release C-IV arm 2 refuses — or, with the
		// arm in force, it is refused — after first PERSISTING a release request for a
		// register the writer still owns, a durable obligation that is known to be doomed.
		// endSendWith's settlement runs the finalize once the section closes, which is the
		// one event that can complete it.
	case sess.finalizeDrainSource(c, removed):
		// A drained source, already resolved (or resolving) by the drain state machine,
		// which owns the ack.
		//
		// The register used to be "ours to remove" here because the identifier's durable
		// authority moved to the PUBREL record. It does not move any more: the slot owns
		// the exchange for its whole life and was re-phased at the store.
		//
		// BUT THE ACKNOWLEDGED EXCHANGE'S OWN TERMINAL CLEANUP STILL RUNS (F-098, holistic
		// review round 7). "The source was handled" is not "the register disappeared": the
		// drain's release legitimately PRESERVES a transmitted register under Reserved
		// authority, and it can equally be refused or come back indeterminate. Skipping the
		// cleanup here left the deferred hold to be dropped with nothing having confirmed
		// the slot gone, so the identifier became allocatable over a surviving register.
		//
		// releaseRegister self-gates on mqttPublishAckMayReleaseSlot, so a successful PUBREC
		// — where the slot owns the PUBREL phase — still does nothing here, exactly as
		// before. What changes is only the case that owed a confirmation and never got one.
		releaseRegister(removed)
	case isPubRec && !pubRecFailed && removed != nil && removed.regSeq != 0:
		// The PUBLISH phase is over and the same durable slot now owns the PUBREL phase
		// (R2). The source ack is CONFIRMED here because its durable suppression state is
		// retained or released on the strength of that acknowledgement.
		//
		// What the confirmation buys is CONTINUITY, not the absence of a duplicate: it
		// keeps the register from being removed over a source that was never actually
		// released, which would cost the redelivery its original identifier. A source
		// ack that never lands means JetStream redelivers regardless — that was true
		// before the register existed.
		if aerr := sess.confirmInflightSourceAck(jsAckSubject); aerr != nil {
			// The ack is unproven, so the source may still be pending and will be
			// re-offered. Two things have to happen, and the second is the one that is
			// easy to miss.
			//
			// The register is KEPT, so a later resume can restore this identifier.
			//
			// And the SOURCE is retired: its payload is past PUBREC and must never be
			// sent again, so the next delivery attempt is acknowledged instead of
			// delivered. Without that, the re-offered source is delivered again — before
			// PUBCOMP under a fresh identifier (the cpending link is gone), and after
			// PUBCOMP under any identifier at all, because the guard that would have
			// suppressed it tests pendingPubRel, which PUBCOMP removes. Retiring the
			// source is keyed by the source rather than by the identifier precisely so it
			// outlives the identifier's bookkeeping. See retireSourceLocked.
			//
			// The source is deliberately left UNACKNOWLEDGED here, with no best-effort
			// retry. A retry that happened to land would settle the message but strand its
			// retirement for the life of the session, because nothing else would ever drain
			// it. Leaving the source unacked instead means JetStream re-offers it exactly
			// once more, and settleRetiredSource then acknowledges that attempt WITH
			// CONFIRMATION — under a fresh reply subject rather than the stale one that just
			// failed — and releases both the retirement and the register.
			//
			// The retirement is recorded in BOTH places, and it has to be. In memory it
			// suppresses every redelivery on this broker. On the register it survives a
			// restart: without the durable half the next resume sees an ordinary
			// PUBLISH-phase register over a live source — indistinguishable from a delivery
			// that never reached PUBREC — and re-sends a payload the client has already
			// completed. Leaving the source deliberately unacked is exactly what makes that
			// window reachable, so the durable mark is not optional.
			// In memory FIRST, then durably. The in-memory half is what suppresses a
			// redelivery arriving on this broker, and the PUBREL whose delivery is already
			// in flight can bring one at any moment — so it must be in place before this
			// path spends a JetStream round trip on the durable half.
			//
			// The durable half is attempted here and, if it does not land, AGAIN at PUBCOMP,
			// which is the moment the alternative evidence disappears: while this exchange's
			// PUBREL record exists a restart is safe anyway, because recovery reads
			// register-plus-PUBREL as "past PUBREC". So this attempt failing is recoverable
			// rather than a residual.
			sess.mu.Lock()
			sess.retireSourceLocked(removed.jsDur, removed.sseq, &mqttRetiredSource{
				pi:         pi,
				regSeq:     removed.regSeq,
				nonce:      removed.regNonce,
				q2AffClaim: removed.q2AffClaim,
				q2AffCopy:  removed.q2AffCopy,
			})
			sess.mu.Unlock()
			// An unproven acknowledgement is AMBIGUOUS, and most often it landed with only
			// its confirmation lost. Read the source before making anything durable: if it
			// is provably complete the retirement has nothing to suppress and is dropped
			// here, which is what keeps a retirement from outliving its exchange and being
			// re-established by every later resume. Only a source that is still live (or
			// unprovable) is worth a durable mark.
			retiredKey := mqttDeferredKey{removed.jsDur, removed.sseq}
			if !sess.resolveRetiredSource(c, retiredKey) {
				sess.secureRetiredSource(c, retiredKey)
			}
			c.Warnf("MQTT: keeping the in-flight register of packet identifier %d for client %q and retiring its source %s/%d: "+
				"the source acknowledgement was not confirmed: %v", pi, sess.id, removed.jsDur, removed.sseq, aerr)
			break
		}
		// The slot was re-phased immediately after the store, which is where it has to
		// happen — see the handoff there. Nothing durable is owed here any more: this arm
		// used to delete the register, and the delete is what the confirmed source
		// acknowledgement above was licensing.
		//
		// A PUBCOMP that raced this confirmation may already have retained the terminal slot
		// on a source it saw live; the confirmation is the settlement that slot waits on.
		sess.mu.Lock()
		sess.wakeRetainedTerminalCleanupLocked(removed.regNonce, removed.jsDur, removed.sseq)
		sess.mu.Unlock()
	default:
		// QoS 1, a failed PUBREC, and any QoS 2 source that carried no register. The
		// register goes first, then the ordinary fire-and-forget ack: a lost ack here
		// costs one duplicate PUBLISH, which QoS 1 permits and which [MQTT-4.4.0-1]
		// does not bind — it governs UNACKNOWLEDGED messages — so a confirmed round
		// trip on the hottest path would buy nothing.
		releaseRegister(removed)
		if !sess.disposalBlocksReply(jsAckSubject) {
			_, _ = sess.submitSourceAck(jsAckSubject, false, nil)
		}
	}
	for _, subj := range deferredNaks {
		sess.jsa.sendMsg(subj, AckNak)
	}
	return nil
}

func (c *client) mqttProcessPubAck(pi uint16) error {
	return c.mqttProcessPublishReceived(pi, false, mqttReasonSuccess)
}

func (c *client) mqttProcessPubRec(pi uint16, reason byte) error {
	return c.mqttProcessPublishReceived(pi, true, reason)
}

// mqttTestPubCompDischargeHookV pauses PUBCOMP after its first recovery check,
// before obligation discharge, with sess.mu released. Nil outside tests.
var mqttTestPubCompDischargeHookV atomic.Pointer[func(sess *mqttSession, pi uint16)]

// Runs from the client's readLoop. No lock held on entry.
func (c *client) mqttProcessPubComp(pi uint16) {
	sess := c.mqtt.sess
	if sess == nil {
		return
	}

	// NOTHING IS REMOVED YET. The obligations below are resolved first, and only a
	// PUBCOMP that will actually acknowledge removes the entry.
	//
	// Untracking up front and withholding afterwards destroys the very evidence the
	// withholding protects: qos12InflightForCap counts pendingPubRel against Receive
	// Maximum, so a removed-then-withheld entry frees its packet identifier, bumpPI
	// re-mints it after a wrap, a different PUBREL is tracked under it, and when the
	// original redelivers mqttDeliverPubRelCb's duplicate branch sees another sequence
	// on that identifier and acknowledges the original away.
	var (
		jsDur      string
		sseq       uint64
		affRef     mqttPubRelAffineRef
		hasPending bool
	)
	sess.mu.Lock()
	if sess.c != c {
		sess.mu.Unlock()
		return
	}
	// The acknowledgement [MQTT-2.3.1-4] frees a PUBREL's identifier at has now been
	// received, so the debt it answers is consumed HERE — before the obligations below,
	// which decide whether the RECORD may be acknowledged and can withhold. Those are
	// different questions: a withheld PUBCOMP costs a PUBREL redelivery, and that
	// redelivery's own send section re-incurs the debt, so the accounting stays exact
	// across any number of retries. Deferring the consume below the gates instead makes
	// a withheld exchange double-count and never release its identifier (F-089).
	sess.consumePubRelAckLocked(c, pi)
	if sess.recoveryPIHeldLocked(pi) {
		sess.mu.Unlock()
		return
	}
	if ack := sess.pendingPubRel[pi]; ack != nil {
		jsDur, sseq, affRef, hasPending = ack.jsDur, ack.sseq, ack.pubRelAffRef, true
	}
	// Does this exchange still hold the ONLY durable evidence that its source is past
	// PUBREC? Looked up before the ack below, because that ack is what destroys it.
	retiredKey, insecure := sess.retiredSourceForLocked(pi)
	sess.mu.Unlock()
	if !hasPending {
		// A stray PUBCOMP may consume owner-scoped send debt, but it owns no stored
		// record or pending entry. In particular it must not continue across this unlock
		// and remove an exchange that a concurrent delivery installs under the same PI.
		return
	}

	// The affine obligation, and the retired-source one below it, are CUMULATIVE: an
	// "acknowledge" verdict from either does not license the ack on its own.
	//
	// A stored PUBREL naming a shared QoS 2 assignment is the durable proof that the
	// client answered PUBREC for that copy, and on this path it can be the ONLY one
	// left — the copy is gone and AffineDone was never recorded. Acknowledging it away
	// with the proof unrecorded is what leaves the owner's next resume free to report a
	// delivery the client completed as LOST. See reconcileQ2AffPubRelProofs.
	if h := mqttTestPubCompDischargeHookV.Load(); h != nil {
		(*h)(sess, pi)
	}
	if hasPending && !sess.dischargeQ2AffPubRelProof(c, pi, mqttDeferredKey{jsDur, sseq}, affRef) {
		return
	}

	// A retired source whose register does not yet record the retirement is protected
	// across a restart only by this exchange's PUBREL record: recovery reads
	// register-plus-PUBREL as "past PUBREC" and suppresses the source, while an unmarked
	// register ALONE looks like an ordinary owed delivery and its payload is re-sent. So
	// the evidence is not destroyed until the retirement is durable — persist before ack,
	// the same rule a drain follows.
	//
	// Withholding the acknowledgement costs a PUBREL redelivery, which [MQTT-4.4.0-1]
	// makes legitimate and which the client answers with another PUBCOMP; that brings us
	// back here to retry. Paced by AckWait, and self-healing.
	if insecure && !sess.secureRetiredSource(c, retiredKey) {
		c.Warnf("MQTT: withholding the acknowledgement of the PUBREL for packet identifier %d of client %q: "+
			"its retired source %s/%d is not durably recorded yet, and this record is the only other evidence of it",
			pi, sess.id, retiredKey.jsDur, retiredKey.sseq)
		return
	}

	// Both obligations discharged, so the entry may go. RE-VALIDATED first, because the
	// discharges above ran without the session lock: a teardown can have cleared
	// pendingPubRel, and a clean start or a takeover can have rebound the session and
	// re-minted this identifier for a NEW incarnation. Untracking blind there hands back
	// an empty ack subject, or removes and acknowledges an exchange that is not the one
	// this PUBCOMP answered.
	//
	// An AckWait redelivery of the SAME record is deliberately still allowed through: it
	// changes the reply subject while (jsDur, sseq) stay put, which is why the subject is
	// taken from untrackPubRel's return rather than from the copy above.
	sess.mu.Lock()
	if sess.c != c {
		sess.mu.Unlock()
		return
	}
	// Scoped to an exchange that WAS tracked when this PUBCOMP started. With no entry
	// at all nothing was ever reserved under this identifier, so there is nothing to
	// lose and the pre-existing behaviour stands.
	ack := sess.pendingPubRel[pi]
	// A RETAINED exchange's PUBREL has no $MQTT_out delivery, so no ACK subject: its evidence is the
	// entry's slot, checked below (F-105 7c).
	if sess.recoveryPIHeldLocked(pi) || ack == nil || (ack.jsAckSubject == _EMPTY_ && ack.kind != mqttPendingRetained) ||
		ack.jsDur != jsDur || ack.sseq != sseq {
		sess.mu.Unlock()
		return
	}
	if ack.regSeq != 0 && (!ack.slot.slotOK() || ack.slot.name.subject == _EMPTY_ ||
		ack.slot.name.pi != pi || ack.slot.seq == 0 || ack.slot.nonce != ack.regNonce) {
		// A source-backed PUBREL must carry the durable slot admission acquired. Treating
		// an incomplete reference as retained would acknowledge its canonical record and
		// free local custody while the whole-life owner remains unterminated.
		sess.mu.Unlock()
		return
	}
	// The slot reference is captured BEFORE the untrack, because the untrack is what
	// drops the entry that carries it. Without this the terminal transition below would
	// have nothing to address, and a sequence-addressed fallback is the addressing F-034
	// forbids.
	termSlot := ack.slot
	// Replace the delivered entry with a terminal reservation under the SAME lock. The
	// duplicate scan and confirmed slot delete below perform round trips, and an outbound
	// callback may allocate concurrently with them; an unlock between removal and this
	// placeholder would let bumpPI re-mint the identifier while its durable slot remained.
	jsAckSubject, terminalReserved := sess.pubCompConvertLocked(pi, ack, termSlot)
	// Read AFTER the untrack, so it sees the Receive Maximum slot this PUBCOMP just
	// completed — and only on this path, because a withheld exchange freed nothing. A
	// terminal cleanup reservation is not client-held protocol debt, but it deliberately
	// continues consuming the conservative local cap until cleanup is confirmed.
	deferredNaks := sess.deferredNaksLocked()
	sess.mu.Unlock()

	// THE TERMINAL RELEASE, which is new here: at HEAD the register was already deleted
	// at the PUBREC handoff, so PUBCOMP had no durable state of its own to settle. Now the
	// slot owns the exchange for its whole life, and this is where that life ends.
	//
	// Ordered AFTER the debt and the obligations above. The delivered entry has already
	// become a terminal placeholder, so the in-memory and durable reservations overlap
	// until cleanup is confirmed; a crash leaves a terminal slot recovery finishes.
	// The tri-state distinguishes a deliberately RETAINED terminal slot (a source that may
	// re-offer) from an INDETERMINATE cleanup (consult 7). Both settled cases free the
	// identifier: the durable store fail-closes over a retained terminal slot, so re-mint
	// cannot erase the retirement, and holding the entry would wrongly consume Receive
	// Maximum for an exchange the client has already completed — stalling a Receive Maximum
	// 1 client until the source proves complete. Only an indeterminate cleanup keeps the
	// reservation and retries.
	res := sess.mqttTerminalizeOnPubComp(c, termSlot)
	// THE ORDINARY CLASS (an exchange stands beside the slot) routes every unsettled cleanup
	// through ONE owner, the completion worker; affine slots keep
	// retainAndReconcileTerminal. The class is mqttDurIsSlotOnlyClass's, stated once.
	slotOnly := mqttDurIsSlotOnlyClass(termSlot.dur)
	if !res.recorded {
		// THE COMPLETION IS UNCONFIRMED, so the canonical PUBREL's JetStream ack is WITHHELD:
		// acking it would remove, from interest-retained $MQTT_out, the one record a retry, a
		// redelivery or a restart needs to finish this exchange. The worker carries the ack and
		// sends it once the completion is recorded.
		if slotOnly {
			sess.retainAndReconcileTerminal(c, termSlot)
			sess.sendPubCompAck(jsAckSubject, false)
		} else {
			sess.enrolPubCompOwed(c, termSlot, jsAckSubject)
		}
		for _, subj := range deferredNaks {
			sess.jsa.sendMsg(subj, AckNak)
		}
		return
	}
	switch res.outcome {
	case mqttTerminalDone:
		if terminalReserved {
			sess.releaseTerminalReservation(termSlot)
		}
	case mqttTerminalRetainedReoffer:
		// The slot is deliberately kept for a source that may re-offer. Release the CAPACITY
		// now — holding the entry would wrongly consume Receive Maximum for an exchange the
		// client already completed (stalling a Receive Maximum 1 client) — while the
		// identifier stays EXCLUDED, because the register still stands. The exclusion and the
		// slot's cleanup owner are installed together; the owner holds the obligation until the
		// slot is gone (retained_terminal_cleanup.go).
		if terminalReserved {
			sess.releaseRetainedTerminalReservation(c, termSlot)
		} else {
			sess.mu.Lock()
			sess.excludeIdentifierLocked(termSlot.name.pi, termSlot.name.subject, termSlot.nonce)
			if w := sess.enrollRetainedTerminalCleanupLocked(c, termSlot); w != nil {
				sess.scheduleRetainedCleanupLocked(w, sess.retainedCleanupDelayLocked(0))
			}
			sess.mu.Unlock()
		}
	case mqttTerminalHeldByAction:
		// The client has completed the exchange, but a PUBREL write under this identifier
		// is still out, so terminal cleanup is the HOLDER'S to finish when its drain ends.
		//
		// The in-memory entry goes ONLY IF this broker's own action is what holds it: its hold
		// then carries the reservation until the write returns, and keeping the entry too would
		// consume Receive Maximum for an exchange the client already completed. A token with NO
		// matching local action is a holder this broker cannot see finish, so the reservation
		// stays and is reconciled — a live token owns the identifier until it drains (C-IV arm
		// 2), and freeing it on a PUBCOMP would be the premature release the arm exists to
		// refuse.
		//
		// For an ordinary exchange "this broker's own action" means the live local action
		// holding EXACTLY the token finalize observed: any live reservation (an exclusion, an
		// older action whose successor holds the durable token) proves no drain will clear it.
		//
		// The delegation also MARKS that action answered before the reservation goes
		// (answerLocalAction): a redelivery that acquired it and has not bound yet would
		// otherwise find no reservation and write.
		heldLocally := sess.answerLocalAction(termSlot.name.pi, res.heldToken, slotOnly)
		switch {
		case heldLocally:
			if terminalReserved {
				sess.releaseTerminalReservation(termSlot)
			}
		case slotOnly:
			sess.retainAndReconcileTerminal(c, termSlot)
		default:
			sess.ownTerminalCleanup(c, termSlot)
		}
	default: // mqttTerminalIndeterminate, with the completion recorded
		// A failed terminal scan or store operation is not proof that the durable owner
		// slot is gone. Keep the identifier unavailable in this process until a retry
		// proves the consequence.
		if slotOnly {
			sess.retainAndReconcileTerminal(c, termSlot)
		} else {
			sess.ownTerminalCleanup(c, termSlot)
		}
	}

	// THE COMPLETION IS RECORDED, so the canonical PUBREL's acknowledgement is safe to send:
	// the terminal slot, not the record, is now the durable evidence (finalize disposes of any
	// record that survives).
	sess.sendPubCompAck(jsAckSubject, false)
	for _, subj := range deferredNaks {
		sess.jsa.sendMsg(subj, AckNak)
	}
}

// noteQ2AffCarrierRefused reports whether the refused-carrier diagnostic is owed for the
// PUBREL RECORD rec, and records that it has been said. False means this same record was
// already reported on this connection.
//
// Keyed by the record — its PUBREL durable and stream sequence — and NOT by the packet
// identifier or the pending entry. Two earlier shapes of this bookkeeping each broke on
// that: an entry-lifetime flag is discarded by every AckWait redelivery, because
// trackAsPubRel replaces the entry, and an identifier-keyed one lets a genuinely
// DIFFERENT record inherit another's suppression on the duplicate-delivery path, where
// several records legitimately share one identifier. The record is the thing the
// diagnostic is about, so it is what the suppression is keyed by.
//
// The set lives until the session's pending state is reset (resetForReuseLocked), not merely
// for the connection: a plain resume keeps it. It
// grows by at most one entry per distinct refused record, which is bounded by the refused
// records that actually exist.
func (sess *mqttSession) noteQ2AffCarrierRefused(rec mqttDeferredKey) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if _, said := sess.refusedCarriers[rec]; said {
		return false
	}
	if sess.refusedCarriers == nil {
		sess.refusedCarriers = make(map[mqttDeferredKey]struct{})
	}
	sess.refusedCarriers[rec] = struct{}{}
	return true
}

// dischargeQ2AffPubRelProof reports whether the PUBREL for pi may be acknowledged away
// as far as its shared QoS 2 assignment reference is concerned. False means WITHHOLD:
// the caller returns without acknowledging and without untracking, so the identifier
// stays reserved and AckWait redelivers the record, bringing the retry back here.
//
// Session lock NOT held on entry: it resolves and writes against JetStream.
func (sess *mqttSession) dischargeQ2AffPubRelProof(c *client, pi uint16, rec mqttDeferredKey,
	ref mqttPubRelAffineRef) bool {
	as := sess.asm
	switch {
	case ref.State == mqttPubRelAffineRefAbsent:
		return true // not an affine exchange; nothing is owed
	case ref.refused():
		// A persisted shape this broker does not write. Refused rather than ignored:
		// the assignment it names cannot be resolved, so acknowledging the record away
		// would destroy a reference nothing can interpret but which may still be the
		// only evidence of a delivery.
		//
		// Reported ONCE per identifier per connection. AckWait re-offers a withheld
		// record forever, so warning on every attempt is unbounded output about one
		// unchanging fact.
		if sess.noteQ2AffCarrierRefused(rec) {
			c.Warnf("MQTT: withholding the acknowledgement of the PUBREL for packet identifier %d of client %q: "+
				"its affine QoS 2 reference (claim %q copy %q) is not a shape this broker writes",
				pi, sess.id, ref.Claim, ref.Copy)
		}
		return false
	case as == nil:
		return false
	}
	claim, cerr := as.q2AffClaimForCarriedCopy(ref.Claim, ref.Copy)
	if cerr != nil {
		if errors.Is(cerr, errQ2AffCarriedCopySuperseded) {
			// POSITIVELY a superseded incarnation: nothing will ever authorize the copy
			// this PUBREL was created for, so there is no proof to preserve.
			return true
		}
		c.Warnf("MQTT: withholding the acknowledgement of the PUBREL for packet identifier %d of client %q: "+
			"its affine QoS 2 assignment %q could not be resolved: %v", pi, sess.id, ref.Claim, cerr)
		return false
	}
	if claim == nil {
		// The claim is gone. Acknowledging is right, and the argument is POSITIVE rather
		// than "absence is a fact": with no record there is nothing for a later resume to
		// report untruthfully, and withholding would wedge this exchange forever on
		// something that can never reappear.
		return true
	}
	// The shape check runs BEFORE the already-proven short circuit, for the same reason
	// the resume pass runs it there: a record this broker does not write must be refused
	// rather than believed, and believing its PubRecProven would release the last
	// evidence of a delivery on the strength of a shape that cannot be interpreted.
	if verr := q2AffShapePrecheck(ref.Claim)(claim); verr != nil {
		c.Warnf("MQTT: withholding the acknowledgement of the PUBREL for packet identifier %d of client %q: "+
			"its affine QoS 2 assignment %q is not a shape this broker writes: %v", pi, sess.id, ref.Claim, verr)
		return false
	}
	if claim.PubRecProven {
		return true // already durable
	}
	if as.proveQ2AffCopyDelivered(ref.Claim, ref.Copy, q2AffShapePrecheck(ref.Claim), c.Warnf, true) == nil {
		c.Warnf("MQTT: withholding the acknowledgement of the PUBREL for packet identifier %d of client %q: "+
			"it is the only evidence that the client answered PUBREC for affine QoS 2 assignment %q, and "+
			"recording that fact did not land", pi, sess.id, ref.Claim)
		return false
	}
	as.reapQ2AffClaim(c.srv, ref.Claim)
	return true
}
