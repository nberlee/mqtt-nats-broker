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

// Packet-identifier allocation and in-flight tracking: bumpPI, tracking and untracking QoS 1
// and QoS 2 deliveries, retired sources, redelivery, PUBREL tracking, and consumer charges.

package mqtt

import (
	"errors"
	"slices"
)

func (sess *mqttSession) bumpPI() uint16 {
	if sess.piSpaceIncomplete {
		// The durable identifier space could not be fully reconstructed on resume, so
		// some identifiers the client may be holding are unknown to us. Allocating one
		// could collide with a register or an unreplayed PUBREL that still owns it
		// ([MQTT-2.3.1-4]). Withhold instead: the caller treats 0 exactly as it treats
		// the in-flight cap, and AckWait re-offers the delivery. See
		// inflight_register.go (R3).
		return 0
	}
	if sess.custody.withholdsAllocation() {
		// The custody ledger's reconstruction is incomplete, or an exchange of this
		// generation could not be read and its identifier is therefore unknown: any
		// allocation could collide with it. Withheld exactly like the case above, and a
		// re-observation pass is asked for so the withholding converges.
		sess.kickCustodyPassLocked()
		return 0
	}
	// A released delivery nobody live can answer reserves nothing any more.
	sess.retireReleasedDeliveriesLocked()
	var avail, custodyHeld bool
	next := sess.last_pi
	for i := 0; i < 0xFFFF; i++ {
		next++
		if next == 0 {
			next = 1
		}

		_, usedInPublish := sess.pendingPublish[next]
		_, usedInPubRel := sess.pendingPubRel[next]
		// ALWAYS consulted, not only after a discard: an identifier a transmission still
		// reserves may have no entry at all — a successful PUBREC replaces the PUBLISH
		// entry with a PUBREL one, clear() removes both while the socket write is still
		// blocked, and a PUBCOMP untracks the PUBREL entry while a duplicate copy is
		// still unanswered — so the registry, not the maps, is what says an identifier
		// is still in use (F-087 WP5, F-089).
		usedInSend := sess.sendReservedLocked(next)
		// And the EXCHANGE's custody: an admitted exchange holds its identifier until it
		// durably ends, whatever the maps say — a metadata disposition or a destruction
		// without evidence removes the entry while the client may still hold the
		// identifier. Checked here, at the allocator, so every exclusion-lift route and
		// every map removal is covered at once (F-105 row 5).
		usedInCustody := sess.custody.holdsPI(next)
		custodyHeld = custodyHeld || usedInCustody
		if !usedInPublish && !usedInPubRel && !usedInSend && !usedInCustody && !sess.disposalPILocked(next) {
			sess.last_pi = next
			avail = true
			break
		}
	}
	if !avail {
		// Exhausted. Where the ledger holds any identifier, a hold may belong to an exchange
		// that has since ended and whose one observation failed: the capacity gate need not
		// be at zero (no Receive Maximum), so this withholding asks for the pass itself, or
		// nothing would ever re-read it and the space would stay closed.
		if custodyHeld {
			sess.kickCustodyPassLocked()
		}
		return 0
	}
	// A withholding gate's latched demand is met once an allocation succeeds with nothing left
	// deferred (F-097 WP4).
	sess.noteAllocationSucceededLocked()
	return sess.last_pi
}

// deferredNaksLocked removes and returns up to capacityAvailableLocked
// deferred ack subjects in ascending stream-sequence
// order, so NAK-forced redeliveries preserve publication order
// [MQTT-4.6.0-1]. The caller must send AckNak on each subject AFTER releasing
// the session lock. A NAK'd message that arrives while the session is again at
// the cap simply re-enters deferred via trackPublish.
//
// Lock held on entry
func (sess *mqttSession) deferredNaksLocked() []string {
	if len(sess.deferred) == 0 {
		return nil
	}
	capacity := sess.capacityAvailableLocked()
	if capacity <= 0 {
		return nil
	}
	type deferredNak struct {
		key        mqttDeferredKey
		ackSubject string
	}
	all := make([]deferredNak, 0, len(sess.deferred))
	for k, subj := range sess.deferred {
		all = append(all, deferredNak{k, subj})
	}
	slices.SortFunc(all, func(a, b deferredNak) int {
		switch {
		case a.key.sseq < b.key.sseq:
			return -1
		case a.key.sseq > b.key.sseq:
			return 1
		default:
			return 0
		}
	})
	if len(all) > capacity {
		all = all[:capacity]
	}
	subjects := make([]string, 0, len(all))
	for _, n := range all {
		delete(sess.deferred, n.key)
		subjects = append(subjects, n.ackSubject)
	}
	return subjects
}

// trackPublishRetained is invoked when a retained (QoS) message is published. It
// allocates a new PI and adds it to pendingPublish with an empty value (no
// JetStream backing). Spec5 [3.3.4].
//
// qos is the effective QoS the caller selected for this replay, and it is the one
// piece of per-exchange evidence a retained send ever has: there is no register to
// recover it from (R6), so the entry's own qos field is seeded here or nowhere.
// A retained frame is serialized under this same lock and flushed later, so it is
// never a JetStream redelivery and never reaches the started path -- the seed is
// for coherence with every other exchange, not because a re-offer will read it.
//
// Lock held on entry
func (sess *mqttSession) trackPublishRetained(qos byte) uint16 {
	// Make sure we initialize the tracking maps.
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	// Honor the effective in-flight cap (MaxAckPending capped by the client's
	// Receive Maximum). At the limit, return 0 so the caller downgrades this
	// retained message to QoS0: it is still delivered, but does not count as an
	// unacknowledged QoS1/2 PUBLISH. Spec5 [3.3.4].
	if sess.capacityAvailableLocked() <= 0 {
		// A charge may be held for an exchange that has since ended, or the census may be
		// incomplete: ask for the custody pass exactly as the delivery gate does. Retained-only
		// traffic never reaches that gate, so without this the downgrade would be permanent.
		sess.kickCustodyPassLocked()
		return 0
	}

	pi := sess.bumpPI()
	if pi == 0 {
		return 0
	}
	// The exchange nonce is minted HERE even though a retained send has no register (R6).
	//
	// A retained delivery's PUBREC stores a PUBREL like any other, and that record carries a
	// MANDATORY exchange identity — so an entry with no nonce cannot produce a storable
	// record at all. A review found this the hard way: without the mint, every outbound
	// retained QoS 2 replay stalled at PUBREC and could terminate the connection, because
	// the builder refused the empty owner and the handler returned an error.
	//
	// Minted rather than derived: there is no register to take an identity from, which is
	// exactly why retained is the one exchange whose identity lives only in memory. Its
	// authority is this entry, so a retained record surviving a restart has none.
	// A HOLD until the admission lands (F-105 7c): the identifier is reserved across the
	// materialization and the admission, and acts for nothing until the landing promotes it to a
	// register-backed Retained entry the ledger charges.
	sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectEmpty(),
		mqttNewRetainedAllocation(mqttNewInflightNonce(), qos, sess.allocationFenceLocked()))

	return pi
}

// trackPublish is invoked when a (QoS) PUBLISH message is to be delivered. It
// detects an untracked (new) message based on its sequence extracted from its
// delivery-time jsAckSubject, and adds it to the tracking maps. Returns a PI to
// use for the message (new, or previously used), and whether this is a
// duplicate delivery attempt.
//
// Lock held on entry
// The returned started flag is true when the PI was already tracked, i.e. this
// is a redelivery of a message whose onward delivery already began.
//
// isNew is true only where this call ALLOCATED the identifier, which is exactly the
// set of deliveries owing a durable in-flight register (R1/R6). Such an entry is
// returned marked regPending and MUST NOT be transmitted until
// storeInflightRegister has confirmed it — the caller either confirms it or removes
// it. See inflight_register.go.
func (sess *mqttSession) trackPublish(jsDur, jsAckSubject string) (packetID uint16, isNew bool) {
	var pi uint16

	if jsAckSubject == _EMPTY_ || jsDur == _EMPTY_ {
		return 0, false
	}

	// Make sure we initialize the tracking maps.
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	// The stream sequence from the ack reply subject. Its delivery COUNT is deliberately
	// not read: a count above one says the consumer offered this sequence before, not
	// that a PUBLISH reached the client — every declined offer raises it. DUP is a
	// property of the entry (mqttPending.transmitted), derived from the durable pair.
	sseq, _, _, _, _ := ackReplyInfo(jsAckSubject)
	if sess.disposalSourceLocked(jsDur, sseq) {
		return 0, false
	}
	// AN ABANDONED ADMISSION OF THIS VERY DELIVERY MAY HAVE LANDED (F-097 WP5): the offer is
	// withheld, never admitted a second time; the continuity record re-offers it (NAK, with this
	// latest reply) once its first admission is decided.
	if c := sess.admissionContinuityLocked(jsDur, sseq); c != nil {
		if !sess.continuityRestoredLocked(c) {
			c.reply = jsAckSubject
			return 0, false
		}
		// A resume restored the allocation's own entry: it serves this offer below.
		sess.retireAdmissionContinuityLocked(c)
	}

	var ack *mqttPending
	pi, _ = sess.cpending.lookup(jsDur, sseq)

	if pi != 0 {
		// There is a possible race between a PUBLISH re-delivery calling us,
		// and a PUBREC received already having submitting a PUBREL into JS . If
		// so, indicate no need for (re-)delivery by returning a PI of 0.
		_, usedForPubRel := sess.pendingPubRel[pi]
		if usedForPubRel {
			return 0, false
		}

		// We should have a pending JS ACK for this PI. This is a redelivery of a
		// message already counted against the in-flight cap, so it is not gated
		// again here. NOTE: if a persistent session accumulated more unacked
		// messages than a newly-lowered Receive Maximum, redeliveries of those
		// pre-existing pending messages are not additionally paced on the new
		// connection (a known limitation; new deliveries below still are, and
		// the session drains to the cap as PUBACKs arrive).
		ack = sess.pendingPublish[pi]
		// A register store is in flight for this identifier, so its FIRST
		// transmission has not happened yet (R1). Decline: acting here would either
		// put the payload on the wire before its identity is durable, or — if the
		// store then fails definitively and untracks the identifier — send under an
		// identifier no record names. AckWait re-offers the sequence, by which time
		// the owner has resolved it either way.
		if ack != nil && ack.regPending {
			return 0, false
		}
		// A RECOVERY RESERVATION (regForeign) declines exactly as regPending does, and for
		// a stronger reason: the record on this subject is one this object may not
		// interpret, so nothing here may transmit under it and nothing may settle it. The
		// identifier stays reserved until recovery resolves it — a successful resume's
		// reconstruction may release, reactivate or hand it off — or the generation is
		// discarded.
		if ack != nil && ack.regForeign {
			return 0, false
		}
		// A MARKER-OPAQUE reservation declines for the same reason: whether this PUBLISH
		// was ever transmitted is unknown, so neither DUP value is honest and nothing may
		// go out under the identifier until a resume reads the pair whole.
		if ack != nil && ack.markerOpaque {
			return 0, false
		}
		// An active send section declines too: this entry's payload is already on its
		// way out on a write that has not returned, and re-entering the send path would
		// queue a second PUBLISH behind it under one identifier (F-087 WP5, class E).
		if ack != nil && ack.sendNonce != _EMPTY_ {
			return 0, false
		}
		if ack != nil && !ack.acts() {
			// A reservation of any kind is not a delivery this redelivery may be tracked onto:
			// it acts for nothing, whatever its other fields say.
			return 0, false
		}
	} else {
		// A SOURCE WHOSE EXCHANGE IS PAST PUBREC IS NOT ADMITTED AGAIN (F-115). The PUBREC handoff
		// removed this source's link above and installed its PUBREL-phase entry in the same
		// critical section, so a re-offer arriving after it — an AckWait redelivery racing the
		// handler, or one JetStream dispatched before it processed the source acknowledgement —
		// lands here, and allocating would deliver the payload a second time under a fresh
		// identifier. Declined exactly as usedForPubRel declines a mapped one: nothing is
		// acknowledged, because the source's acknowledgement belongs to the PUBREC handler, its
		// retirement, the drain or recovery — never to this offer.
		if sess.sourcePendingPubRelLocked(jsDur, sseq) {
			return 0, false
		}
		// Gate new deliveries at the effective cap: MaxAckPending capped by the
		// client's MQTT 5.0 Receive Maximum. QoS 2 messages awaiting PUBCOMP are
		// counted too when a Receive Maximum applies (Spec5 [4.9]).
		if sess.capacityAvailableLocked() <= 0 {
			// A charge may be held for an exchange that has since ended: ask the custody
			// observer for a bounded re-observation pass, which converges it.
			sess.kickCustodyPassLocked()
			// At the in-flight cap: withhold the message and remember its ack
			// subject so a freed slot can NAK it for prompt redelivery
			// (deferredNaksLocked); AckWait remains the fallback. Overwrite on
			// re-deferral: JS ignores acks for superseded delivery attempts, so
			// only the latest subject is NAK-able. Spec5 [4.9].
			if sess.deferred == nil {
				sess.deferred = make(map[mqttDeferredKey]string)
			}
			sess.deferred[mqttDeferredKey{jsDur, sseq}] = jsAckSubject
			return 0, false
		}

		// A withheld sequence's NAK subject is consumed with the offer that admits it;
		// the marker says nothing about DUP — the fresh allocation's entry starts
		// transmitted=false whatever the delivery count says.
		delete(sess.deferred, mqttDeferredKey{jsDur, sseq})

		pi = sess.bumpPI()
		if pi == 0 {
			return 0, false
		}

		sess.cpending.put(jsDur, sseq, pi)
		isNew = true
	}

	if ack == nil {
		// A source mapped to an identifier with no entry behind it gets a reservation that
		// acts for nothing: no nonce, no evidence, no transmission state.
		entry := mqttNewUnmintedHold(jsDur, sseq, jsAckSubject)
		if isNew {
			// This allocation owes a register, and nothing may send under the identifier
			// until it is confirmed (R1). The register's identity is minted WITH the
			// allocation, not with the store, so a cancellation that happens while the store
			// is in flight has something immutable to name it by. Neither the identifier nor
			// the source works there: the cancellation frees the one and the other is
			// re-offered. The fence records what custody looked like as the identifier was
			// taken (markerOpaqueEligibleLocked).
			entry = mqttNewAllocation(jsDur, sseq, jsAckSubject, mqttNewInflightNonce(), sess.allocationFenceLocked())
		}
		if !sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectEmpty(), entry) {
			return 0, false
		}
	} else {
		ack.jsAckSubject = jsAckSubject
		ack.sseq = sseq
		ack.jsDur = jsDur
	}

	return pi, isNew
}

// Stops a PI from being tracked as a PUBLISH. It can still be in use for a
// pending PUBREL.
//
// Also returns the affine assignment claim this delivery came from, when it came
// from one: every terminal path for a session-affine shared QoS 2 delivery
// (PUBREC, expiry, Maximum Packet Size discard) has to record the affine side as
// complete on that claim, and the claim identity is only reachable through the
// pending entry being removed here. Empty for every ordinary delivery. See
// qos2_shared_affine.go.
//
// Lock held on entry
func (sess *mqttSession) untrackPublish(pi uint16, why mqttUntrackReason) (jsAckSubject, q2AffClaim, q2AffCopy string) {
	ack := sess.untrackPublishEntry(pi, why)
	if ack == nil {
		return _EMPTY_, _EMPTY_, _EMPTY_
	}
	return ack.jsAckSubject, ack.q2AffClaim, ack.q2AffCopy
}

// untrackPublishEntry is untrackPublish returning the whole removed entry, so a
// caller that may have to PUT IT BACK can. The PUBREC path needs that: it
// untracks before storing the PUBREL, and a failed store must leave the session
// exactly as it was rather than with the identifier wedged and its delivery
// tracking gone. It is also how a caller learns the delivery's (durable,
// sequence), which nothing else exposes once the entry is removed.
//
// Returns nil when the identifier was not tracked as a PUBLISH.
//
// THE REASON IS MANDATORY (F-088 WP2): an UNANSWERED removal of a transmitted entry installs its
// AckRoute binding here, before the entry goes and under the same lock, so the client's late
// acknowledgement still finds the exchange it answers. See outbound_ack_route.go.
//
// Lock held on entry
func (sess *mqttSession) untrackPublishEntry(pi uint16, why mqttUntrackReason) *mqttPending {
	ack, ok := sess.pendingPublish[pi]
	if !ok {
		return nil
	}
	sess.installAckRouteForRemovalLocked(ack, why)

	delete(sess.pendingPublish, pi)
	// Do NOT reset last_pi here: it is a monotonic rolling counter (see bumpPI) so
	// a just-freed id is not reused within a delivery burst, which a client would
	// read as a duplicate. Spec [MQTT-2.3.1-4].

	if ack.jsDur != _EMPTY_ {
		sess.cpending.deleteSeq(ack.jsDur, ack.sseq)
	}

	return ack
}

// mqttRetiredSource is one QoS 2 source that is past PUBREC while its acknowledgement is
// unproven: its payload must never be sent again, and its next delivery attempt is to be
// acknowledged rather than delivered.
//
// It carries what is needed to make the fact DURABLE later as well as to act on it now.
// durable says whether the register already records the retirement; while it is false the
// exchange's PUBREL record is the only cross-restart evidence, so that PUBREL must not be
// acknowledged away (see mqttProcessPubComp).
type mqttRetiredSource struct {
	pi         uint16 // the identifier whose exchange retired this source
	regSeq     uint64 // the register's current sequence: CAS base while unmarked, delete target once settled
	nonce      string // the register's identity, so a later CAS can prove the record is ours
	q2AffClaim string // preserved across the rewrite; empty for an ordinary delivery
	q2AffCopy  string // the copy SUBJECT this delivery carried, which binds the claim reference to it (F-065)
	durable    bool
}

// retireSourceLocked records that a QoS 2 source delivery is past PUBREC while its
// acknowledgement is NOT proven: its payload must never be sent again, and the next
// delivery attempt is to be acknowledged rather than delivered.
//
// Keyed by the SOURCE, deliberately, and not by the packet identifier. The obvious
// alternative — restoring the source's cpending link so the usedForPubRel guard in
// trackPublish suppresses the redelivery — works only until PUBCOMP: untrackPubRel
// removes the pendingPubRel entry the guard tests, and the very next redelivery then
// re-sends the payload of an exchange the client has already COMPLETED. A fact about
// the source has to outlive the identifier's bookkeeping, which is the same shape the
// affine path already uses (its claim's AffineDone flag) and for the same reason.
//
// The set is drained by mqttSourceRetired, which is why it cannot grow with traffic: an
// entry costs one map slot until the first delivery attempt whose acknowledgement is
// CONFIRMED, and it is only ever created when an acknowledgement could not be proven.
//
// Idempotent: a same-broker reconnect re-runs the resume barrier while an existing
// retirement is still in place, and re-recording one must not undo what is already known
// about it (in particular that it is already durable).
//
// Lock held on entry.
func (sess *mqttSession) retireSourceLocked(jsDur string, sseq uint64, rs *mqttRetiredSource) {
	if jsDur == _EMPTY_ || sseq == 0 || rs == nil {
		return
	}
	if sess.srcAckOwed == nil {
		sess.srcAckOwed = make(map[mqttDeferredKey]*mqttRetiredSource)
	}
	key := mqttDeferredKey{jsDur, sseq}
	if cur := sess.srcAckOwed[key]; cur != nil {
		if rs.durable {
			cur.durable = true
			cur.regSeq = rs.regSeq
		}
		return
	}
	sess.srcAckOwed[key] = rs
}

// retiredSourceForLocked finds the retirement an exchange's packet identifier created, so
// PUBCOMP can secure it before destroying the PUBREL that also evidences it. The map holds
// one entry per unproven acknowledgement, so the scan is over a handful of entries at most.
//
// Lock held on entry.
func (sess *mqttSession) retiredSourceForLocked(pi uint16) (mqttDeferredKey, bool) {
	for key, rs := range sess.srcAckOwed {
		if rs != nil && rs.pi == pi && !rs.durable {
			return key, true
		}
	}
	return mqttDeferredKey{}, false
}

// sourceRetiredLocked reports whether a delivery attempt belongs to a source that is
// already past PUBREC with an unproven acknowledgement. Such an attempt must not be
// tracked and must not be delivered; the caller settles it with settleRetiredSource
// once it has released the lock.
//
// Lock held on entry.
func (sess *mqttSession) sourceRetiredLocked(jsDur string, sseq uint64) bool {
	if sess.terminalSourceHeldLocked(jsDur, sseq) || sess.abandonedSourceHeldLocked(jsDur, sseq) {
		return true
	}
	if len(sess.srcAckOwed) == 0 {
		return false
	}
	_, owed := sess.srcAckOwed[mqttDeferredKey{jsDur, sseq}]
	return owed
}

// settleRetiredAffineSource is settleRetiredSource for a SESSION-AFFINE shared QoS 2
// copy, and it exists because that copy's claim owes an affine-side completion that
// an ordinary source ack does not discharge.
//
// The route here is the one F-065's review found unowned. A successful PUBREC stores
// the correlated PUBREL BEFORE it records AffineDone, so a crash between the two
// leaves both the PUBREL and the register; on restart the PUBREL wins over
// pendingPublish, the live register is classified RETIRED rather than restored, and
// its claim reference lands in mqttRetiredSource. Nothing then discharged the affine
// side: the affine delivery callback went straight to trackPublish, which returns
// identifier zero because the restored PUBREL already reserves that source, and
// returned without acknowledging the copy — so the copy NAK-looped and the claim
// never completed, which is F-065's own harm re-entered through recovery.
//
// Three dispositions, and the ORDER inside each is the correctness property:
//
//   - The claim record is gone or unreadable: acknowledge NOTHING and keep the
//     retirement. An absent claim is not a fact (feedback_absence_is_not_a_fact), so
//     the copy NAKs and a later attempt resolves it.
//   - The claim names a DIFFERENT incarnation: that is a positive fact — a live
//     record naming another copy subject — so this copy can never be delivered by
//     anybody. Confirm-ack it and clear the retirement, and write NOTHING onto the
//     replacement: it made no delivery and must not be credited with one.
//   - The claim is this copy's own incarnation: record AffineDone FIRST, fenced to
//     that incarnation, then confirm the copy ack, then clear the retirement and its
//     register. A failure at either of the first two steps retains BOTH cleanup
//     records so the next redelivery retries — persist before ack, ack before
//     forget, exactly as mqttFinishQ2AffineDelivery and secureRetiredSource require.
//
// Lock NOT held on entry: every step issues a JetStream request.
func (sess *mqttSession) settleRetiredAffineSource(c *client, jsDur string, sseq uint64,
	reply, deliveredCopy, claimSubject, carriedCopy string) {

	as := sess.asm
	if as == nil {
		return
	}
	as.mu.RLock()
	ackHook := as.testQ2AffCopyAck
	as.mu.RUnlock()
	confirm := func(reply string) error {
		_, err := sess.submitSourceAck(reply, true, nil)
		return err
	}
	if ackHook != nil {
		confirm = ackHook
	}
	// The pair comes from the RETIREMENT's own durable record, not from arguments derived
	// at this delivery: the register is what survived the restart, and the delivered
	// subject only says which copy is in hand. Falling back to the caller's values would
	// make the persisted pair dead data — which is how a mutation removing it survived.
	sess.mu.Lock()
	if rs := sess.srcAckOwed[mqttDeferredKey{jsDur, sseq}]; rs != nil {
		if rs.q2AffClaim != _EMPTY_ {
			claimSubject = rs.q2AffClaim
		}
		if rs.q2AffCopy != _EMPTY_ {
			carriedCopy = rs.q2AffCopy
		}
	}
	sess.mu.Unlock()

	// The copy IN HAND is bound to the carrier FIRST, before any arm that can acknowledge
	// one, because what an acknowledgement destroys is the copy in hand and not the
	// carrier. Every disposition below proves something about carriedCopy; only this
	// equality makes that proof a statement about the message actually being settled.
	//
	// Review found the previous order wrong for exactly that reason: the superseded arm
	// proved the CARRIER belonged to an older incarnation and then acknowledged whatever
	// was delivered, so a third, cross-assignment copy passed the successor guard and was
	// destroyed — a live assignment's message, lost, with its claim stranded.
	//
	// Reaching this refusal means the message at this retirement's own source sequence is
	// not the copy the carrier names, which the pair's source binding refuses on recovery
	// and which no in-process path can produce (the retirement carries the very subject its
	// delivery was handed). A stream sequence names one immutable message, so the safe
	// direction is to acknowledge nothing.
	if deliveredCopy != _EMPTY_ && deliveredCopy != carriedCopy {
		c.Warnf("MQTT: refusing to settle retired affine QoS 2 source %s/%d: the copy in hand is %q while the "+
			"retirement carries %q, so nothing here may acknowledge either", jsDur, sseq, deliveredCopy, carriedCopy)
		sess.refuseQ2AffCopy(reply)
		return
	}

	// ONE helper turns the carried pair back into a claim, so this route is bound exactly
	// as the live one is: attributable record, and it must name the copy the carrier was
	// created for.
	claim, cerr := as.q2AffClaimForCarriedCopy(claimSubject, carriedCopy)
	switch {
	case errors.Is(cerr, errQ2AffCarriedCopySuperseded):
		// A SUPERSEDED INCARNATION of this claim subject: the live record is attributable
		// and names a different copy, so nothing will ever authorize the one this
		// retirement carries. Disposing of it is what keeps the redelivery from NAK-looping
		// for the life of the session, and the equality above is what makes the disposal
		// safe — the copy in hand IS the carrier this arm just proved superseded, so it is
		// neither the successor's own nor some third assignment's.
		//
		// A caller with NO copy in hand (a settlement driven from the retirement alone)
		// reaches here having proven nothing about what an acknowledgement would destroy,
		// so disposal is refused: the superseded copy is collected by the delivery gate's
		// own terminal arm on its next redelivery, where a delivered subject exists.
		if deliveredCopy == _EMPTY_ {
			c.Warnf("MQTT: refusing to settle retired affine QoS 2 source %s/%d: its carrier %q is superseded but "+
				"no copy is in hand, so nothing here may acknowledge one", jsDur, sseq, carriedCopy)
			return
		}
		if aerr := confirm(reply); aerr != nil {
			c.Warnf("MQTT: could not acknowledge a superseded affine QoS 2 copy for retired source %s/%d: %v", jsDur, sseq, aerr)
			return
		}
		sess.clearRetiredSource(c, jsDur, sseq)
		return
	case cerr != nil:
		// Unattributable or unreadable: this route both acknowledges and mutates, so it
		// refuses like every other direct decision path. Nothing is acked and the
		// retirement is kept for a later attempt.
		c.Warnf("MQTT: refusing to settle retired affine QoS 2 source %s/%d: %v", jsDur, sseq, cerr)
		sess.refuseQ2AffCopy(reply)
		return
	case claim == nil:
		sess.refuseQ2AffCopy(reply) // absent: never guess
		return
	}
	// The SAME nonce-fenced finish path the live delivery uses — record AffineDone,
	// then confirm the copy ack, then reap — rather than a second transition site of its
	// own. The plan requires that reuse, and the reason is not tidiness: an independent
	// CAS here would be an eleventh claim-transition call site with its own
	// load-to-transition window, needing its own fence and its own mutation to prove it.
	// A failure inside it acknowledges nothing, which is exactly what must happen: the
	// retirement then stays for the next redelivery to retry.
	// Only a CONFIRMED copy acknowledgement licenses forgetting the retirement: it is
	// what suppresses redelivery, so clearing it on an unproven ack would leave the copy
	// re-offerable with the state that governs it already gone. A recorded-but-unacked
	// outcome therefore leaves the retirement for the next redelivery to retry.
	if _, acked := c.mqttFinishQ2AffineDelivery(claimSubject, carriedCopy, reply); !acked {
		return
	}
	sess.clearRetiredSource(c, jsDur, sseq)
}

// clearRetiredSource drops a settled SOURCE retirement. It may reap an ordinary
// PUBLISH-phase register whose source is complete, but an explicitly past-PUBREC
// whole-life owner remains for PUBCOMP's terminal cleanup. Retired describes the source
// and is deliberately not treated as a second slot phase. Split out of
// settleRetiredSource so the affine route above reaches the identical cleanup after its
// own, longer proof.
func (sess *mqttSession) clearRetiredSource(c *client, jsDur string, sseq uint64) {
	sess.mu.Lock()
	rs := sess.srcAckOwed[mqttDeferredKey{jsDur, sseq}]
	sess.mu.Unlock()
	sess.clearRetiredSourceMatching(c, jsDur, sseq, rs)
}

func (sess *mqttSession) clearRetiredSourceMatching(c *client, jsDur string, sseq uint64, expected *mqttRetiredSource) {
	key := mqttDeferredKey{jsDur, sseq}
	sess.mu.Lock()
	if expected == nil || sess.srcAckOwed[key] != expected {
		sess.mu.Unlock()
		return
	}
	var regSeq uint64
	var nonce string
	var retiredPI uint16
	if rs := sess.srcAckOwed[key]; rs != nil {
		regSeq, nonce, retiredPI = rs.regSeq, rs.nonce, rs.pi
		// The settled source's retained terminal slot is handed on BEFORE the retirement
		// that owned this source leaves (retained_terminal_cleanup.go).
		sess.wakeRetainedTerminalCleanupLocked(rs.nonce, jsDur, sseq)
	}
	delete(sess.srcAckOwed, key)
	sess.mu.Unlock()
	if regSeq != 0 && sess.asm != nil {
		// Through the verdict property: this settle holds the exchange nonce, so a
		// void verdict naming the register it deletes is released against the
		// proof of that deletion rather than stranded (F-087 WP3).
		//
		// PHASE-GATED (census C08). Clearing the retired SOURCE is what this settle
		// discharges; the exchange's owner slot is a different fact, and past PUBREC that
		// slot is the identifier's only claimant. The reference is built from the
		// retirement, which carries both the identifier and the exchange nonce.
		var slot *mqttInflightSlotRef
		sess.mu.Lock()
		idHash, gen := sess.idHash, sess.gen
		sess.mu.Unlock()
		if idHash != _EMPTY_ && gen != _EMPTY_ && retiredPI != 0 {
			if nm, perr := mqttParseInflightSubject(mqttInflightSubject(idHash, gen, retiredPI)); perr == nil {
				slot = &mqttInflightSlotRef{
					name: *nm, seq: regSeq, nonce: nonce,
					state: mqttInflightPublish, dur: jsDur, sseq: sseq,
				}
			}
		}
		// No licence: a retired register is past PUBREC, never an unsent obligation.
		sess.reapRegisterWithVerdictGated(regSeq, nonce, false, slot, mqttSlotDeleteComplete, mqttDestroyLicence{})
	}
	c.Debugf("MQTT: settled retired QoS 2 source %s/%d for client %q", jsDur, sseq, sess.id)
}

// settleRetiredSource acknowledges a retired source's delivery attempt with
// CONFIRMATION, using THIS attempt's reply subject — never the stale one whose
// acknowledgement could not be proven — and releases the entry only once that lands. A
// failure leaves the entry in place so the next redelivery retries; nothing is
// delivered either way, which is the whole point.
//
// Lock NOT held on entry: this issues a JetStream request.
func (sess *mqttSession) settleRetiredSource(c *client, jsDur string, sseq uint64, reply string) {
	sess.mu.Lock()
	terminal := sess.terminalSourceHeldLocked(jsDur, sseq) || sess.disposalSourceLocked(jsDur, sseq)
	retired := sess.srcAckOwed[mqttDeferredKey{jsDur, sseq}]
	sess.mu.Unlock()
	if terminal {
		return // The terminal continuation owns settlement, including an unresolved ACK advance.
	}
	if !mqttDurIsSlotOnlyClass(jsDur) && !mqttSourceAckReplyBinds(reply, mqttInflightSourceStream(jsDur), jsDur, sseq) {
		return
	}
	if _, err := sess.submitSourceAck(reply, true, nil); err != nil {
		c.Warnf("MQTT: discarded a redelivery of retired QoS 2 source %s/%d for client %q, but its acknowledgement is still unproven: %v",
			jsDur, sseq, sess.id, err)
		return
	}
	// The source is provably gone, so its suppression can be removed. The slot is a
	// separate obligation: if its slot has durably crossed PUBREC it remains the
	// identifier's owner until PUBCOMP, and clearRetiredSource's phase gate preserves it.
	sess.clearRetiredSourceMatching(c, jsDur, sseq, retired)
}

// retrackPublishLocked reinstates an entry removed by untrackPublishEntry, under
// the SAME packet identifier, restoring both maps. Used to roll back a PUBREC
// whose PUBREL store failed: without it the identifier stays wedged in
// pendingPubRel with no ack subject anything will ever fill, and the delivery's
// tracking is gone, so it can neither be redelivered nor acknowledged.
//
// Lock held on entry.
func (sess *mqttSession) retrackPublishLocked(pi uint16, ack *mqttPending) {
	if ack == nil {
		return
	}
	if binding := ack.slot.abandonedAck; binding != nil {
		// Rollback cannot turn abandonment into permission to deliver PUBLISH.
		sess.installAbandonedAckLocked(binding)
		return
	}
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	// The PUBREC that this rollback answers is the client's own proof of receipt, so the
	// entry is TRANSMITTED whatever the section that sent it recorded: its next send is a
	// retransmission and carries DUP=1 (N-8.3 item 2). The marker sequence travels with
	// the restored entry; its clean-up ran when the PUBREC was processed.
	ack.transmitted = true
	// ANY: this is the exact entry the caller removed, put back. Its kind and evidence are the
	// ones its constructor gave it, and the writer's gate asks the question the reinstallation
	// depends on — does custody still hold its lineage; if not, the exchange ended meanwhile and
	// nothing is reinstalled.
	if !sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectAny(), ack) {
		return
	}
	if ack.jsDur == _EMPTY_ {
		return
	}
	sess.cpending.put(ack.jsDur, ack.sseq, pi)
}

// resendPendingRedelivery re-sends the session's unacknowledged QoS 1/2 PUBLISH
// and PUBREL packets by NAK'ing each on its JS ack subject, forcing immediate
// redelivery (with the DUP flag set) on session resume as required by spec
// [MQTT-4.4.0-1], instead of waiting for the JS consumer's AckWait interval.
// NAKs are ordered by stream sequence so redeliveries preserve original order
// [MQTT-4.6.0-1], and re-use the original packet identifiers.
//
// The NAK — the PROMPTNESS of the redelivery — is a same-server-reconnect
// optimization: it needs a live per-attempt JetStream ack subject, and an entry
// rebuilt by the resume barrier from its durable in-flight register has none until
// its first redelivery arrives, so after a restart or a failover redelivery falls
// back to the AckWait interval. The packet IDENTIFIER no longer depends on any of
// this: it is restored from the register itself, before any source interest exists.
// See inflight_register.go.
//
// resumePubs/resumeRels are the pending identities captured when the session was
// attached to this connection, BEFORE any subscription was restored. Only those
// are eligible: restoring subscriptions re-establishes interest, so by the time
// this runs the consumer may already have pushed brand new deliveries into the
// pending maps, and NAK'ing one of those redelivers a message that is on its way
// to the client for the first time — a duplicate the client never asked for.
//
// If the disconnect lasted longer than AckWait, the consumer's own pending
// check may already redeliver a message as interest is re-established; the NAK
// then produces one extra copy. That is harmless (same packet identifier, DUP
// set, so the client dedups) and does not happen on a timely reconnect.
func (sess *mqttSession) resendPendingRedelivery(c *client, resumePubs, resumeRels map[mqttPendingRef]struct{}) {
	type pendingNak struct {
		pi         uint16
		sseq       uint64
		ackSubject string
		// jsDur is which consumer the sequence belongs to. Required, not
		// decorative: the reap reconciliation below checks sequences against
		// $MQTT_msgs, and a session-affine shared QoS 2 entry's sequence is a
		// $MQTT_out sequence in a completely different sequence space. Checking one
		// against the other would misread a live copy as reaped, release its packet
		// identifier and skip its redelivery. See qos2_shared_affine.go.
		jsDur string
	}
	bySeq := func(a, b pendingNak) int {
		switch {
		case a.sseq < b.sseq:
			return -1
		case a.sseq > b.sseq:
			return 1
		default:
			return 0
		}
	}
	// Keep only what was pending when the session was resumed. processSubs has
	// already run, so the maps below can also hold deliveries this connection
	// started moments ago; those are being transmitted for the first time and a
	// NAK would duplicate them. The resume snapshot is the discriminator — the
	// maps themselves cannot tell the two apart. PUBLISH and PUBREL live on
	// different streams (distinct sequence spaces) and are paced differently, so
	// collect them separately.
	sess.mu.Lock()
	jsa := sess.jsa
	pubNaks := make([]pendingNak, 0, len(sess.pendingPublish))
	for pi, ack := range sess.pendingPublish {
		if _, atResume := resumePubs[mqttPendingRef{pi, ack.sseq, ack.jsDur}]; !atResume {
			continue
		}
		if !mqttPendingRedeliverable(ack) {
			continue
		}
		if ack.jsAckSubject != _EMPTY_ {
			pubNaks = append(pubNaks, pendingNak{pi, ack.sseq, ack.jsAckSubject, ack.jsDur})
		}
	}
	relNaks := make([]pendingNak, 0, len(sess.pendingPubRel))
	for pi, ack := range sess.pendingPubRel {
		if _, atResume := resumeRels[mqttPendingRef{pi, ack.sseq, ack.jsDur}]; !atResume {
			continue
		}
		if !ack.acts() {
			continue // a reservation's delivery is not this session's to re-offer
		}
		if ack.jsAckSubject != _EMPTY_ {
			relNaks = append(relNaks, pendingNak{pi, ack.sseq, ack.jsAckSubject, _EMPTY_})
		}
	}
	// The affine QoS 2 delivery path has to be re-established on resume for the
	// same reason the PUBREL one does: its consumer may hold a started delivery
	// this session owns, and nothing else recreates the delivery subscription when
	// the resumed session restores no shared QoS 2 subscription.
	q2AffGen := sess.q2AffGen
	q2AffDur := _EMPTY_
	if sess.q2AffConsumer != nil {
		q2AffDur = sess.q2AffConsumer.Durable
	}
	// Any pending PUBREL (including a placeholder created at PUBREC before its
	// delivery recorded an ack subject) needs the PUBREL delivery subscription.
	hasPendingPubRel := len(sess.pendingPubRel) > 0
	// Deferred (withheld, never transmitted) deliveries from before the
	// disconnect should not wait an AckWait either; drain them here too.
	//
	// The entry is NAK'd but KEPT: it is the only record that this message has
	// never been on the wire, and the NAK's own redelivery arrives with a
	// JetStream delivery count above one, so dropping it here would mark that
	// delivery DUP on the client's FIRST sight of it — the very property this
	// mechanism is documented to preserve ([MQTT-3.3.1-1]; see the prompt-release
	// note in docs/CONFORMANCE.md). trackPublish removes the entry when the
	// message actually goes out, which is the point at which it stops being
	// never-transmitted; a re-send after that is a genuine DUP.
	deferredNaks := make([]pendingNak, 0, len(sess.deferred))
	for k, subj := range sess.deferred {
		deferredNaks = append(deferredNaks, pendingNak{0, k.sseq, subj, k.jsDur})
	}
	sess.mu.Unlock()

	// Reconcile against the messages stream: a QoS1/2 message whose Message Expiry
	// TTL reaped it while it was delivered-but-unacked is gone from the stream, so it
	// can neither be redelivered nor PUBACK'd. Such an entry has nothing left to NAK.
	// Whether its packet identifier may be RELEASED with it is a different question,
	// and the source cannot answer it: the message vanishing is a fact about what this
	// broker can still do, not about what the client is holding. Only a PROVEN-UNSENT
	// exchange — the durable pair still carrying its unsent marker — was demonstrably
	// never seen, and only that one is released here. A POSSIBLY-ATTEMPTED one may have
	// reached the receiver under this identifier, so it stays RESERVED: identifier and
	// Receive Maximum slot held, out of the NAK set, nothing written.
	//
	// This is the same split recovery applies to a finished source, through the same
	// predicate the two Message Expiry paths and the drain's withdrawal ask
	// (provenUnsentReleasableLocked). Releasing on source absence alone freed a slot the
	// client was still spending, and durable per-identifier occupancy does not protect
	// that: a later allocation of a DIFFERENT identifier transmits (F-100).
	//
	// The lookups run outside the session lock. Spec5 [3.3.2.3.3], [MQTT-2.3.1-4].
	if si, err := jsa.lookupStream(mqttStreamName); err == nil && si != nil {
		firstSeq := si.State.FirstSeq
		kept := make([]pendingNak, 0, len(pubNaks))
		var gone []pendingNak
		for _, n := range pubNaks {
			if n.jsDur == q2AffDur && q2AffDur != _EMPTY_ {
				// A session-affine QoS 2 copy: its sequence is a $MQTT_out sequence,
				// so the $MQTT_msgs checks below say nothing about it. NAK it directly
				// (JetStream ignores an ack for a delivery it does not recognize).
				kept = append(kept, n)
				continue
			}
			reaped := n.sseq < firstSeq
			if !reaped {
				if _, err := jsa.loadMsg(mqttStreamName, n.sseq); err != nil && !isErrorOtherThan(err, JSNoMessageFoundErr) {
					reaped = true
				}
			}
			if reaped {
				gone = append(gone, n)
			} else {
				kept = append(kept, n)
			}
		}
		mqttTestRestorePauseHook("reap-scanned")
		if len(gone) > 0 {
			sess.mu.Lock()
			for _, n := range gone {
				// DECIDED under this final lock, never by the scan above: the scan ran,
				// the lock was released for the stream lookups, and the entry can have
				// been acknowledged, replaced, cancelled or entered a send section in
				// between.
				//
				// Identity first. A packet identifier is not a delivery — identifiers
				// are released and re-minted — so an entry whose (durable, sequence) no
				// longer matches what the scan classified belongs to a delivery this
				// pass never looked at, and untracking it would destroy a live one.
				ack := sess.pendingPublish[n.pi]
				if ack == nil || ack.sseq != n.sseq || ack.jsDur != n.jsDur {
					continue
				}
				if !sess.provenUnsentReleasableLocked(ack) {
					// POSSIBLY-ATTEMPTED. Reserved, not released: the receiver may hold
					// this identifier from an attempt whose PUBREC or PUBACK was lost.
					// Its source mapping is KEPT, deliberately — and "unlike recovery"
					// now holds only for recovery's COMPLETE arm. Recovery drops the
					// mapping for a finished source because it runs before any source
					// interest exists and nothing can offer that source again; it KEEPS
					// it for a merely LOST one, for the same reason this site does. Here
					// a callback may already hold this payload, and with the mapping gone
					// trackPublish would allocate it a SECOND identifier for the same
					// source. The entry ends by the client's own acknowledgement, the
					// session's end, or a consumer teardown's purge.
					c.Warnf("MQTT: reserving packet identifier %d for client %q: its PUBLISH may have been transmitted and its source %s/%d is gone, so it is held until the client acknowledges it or the session ends",
						n.pi, sess.id, n.jsDur, n.sseq)
					continue
				}
				// PROVEN UNSENT. The client has never seen this PUBLISH and never will,
				// so the identifier and its slot are free. No send section can be open —
				// the predicate refuses one, under this same uninterrupted lock — so
				// there is no continuation to condemn.
				//
				// "Proven unsent" here is a LOCAL classification, which is an observation
				// rather than exclusion against the next acquisition: the durable slot may
				// survive, and a peer may acquire it between this decision and its delete.
				// The entry's removal and its capacity release are unchanged; the identifier
				// is excluded until a confirmed absence settles it (F-098).
				var exNonce string
				if ack := sess.pendingPublish[n.pi]; ack != nil {
					exNonce = ack.regNonce
				}
				sess.excludeIdentifierLocked(n.pi, sess.inflightSubjectLocked(n.pi), exNonce)
				sess.untrackPublish(n.pi, mqttUntrackUnanswered) // proven unsent: no binding
			}
			sess.mu.Unlock()
		}
		pubNaks = kept
	}

	// Replay pending PUBRELs: they complete already-started QoS2 handshakes (the
	// client answers PUBCOMP, freeing its Receive Maximum slot) rather than
	// starting new deliveries. The PUBREL delivery subscription is only (re)created
	// by the QoS2 subscribe path, so ensure it exists here whenever any PUBREL is
	// pending (a resume may restore no QoS2 subscription, and a placeholder created
	// at PUBREC may not yet carry an ack subject). Spec5 [MQTT-4.4.0-1].
	if q2AffGen != _EMPTY_ {
		// FIRST, and before the consumer work below. Advance every assignment this
		// incarnation's stored PUBRELs prove the client answered PUBREC for, so the
		// reconciliation that follows cannot report one of them as a LOST delivery. It
		// runs here rather than beside recoverQ2AffClaims because it must run even when
		// affine restoration fails — that is the path on which the reap has no other
		// owner. See reconcileQ2AffPubRelProofs.
		proofEvidence := sess.reconcileQ2AffPubRelProofs(c)
		// Standalone caller, so it opens its own record transaction and passes the
		// token down. See sess_recordtx.go.
		tok := sess.beginRecordTx()
		_, err := sess.ensureQ2AffineConsumerSubscription(c, tok)
		if terr := sess.endRecordTx(tok); terr != nil {
			c.Errorf("MQTT: %v", terr)
		}
		if err != nil {
			c.Errorf("Unable to re-establish session-affine QoS 2 delivery on resume: %v", err)
		} else if sess.asm != nil {
			// Reconcile the assignments this generation still owns: complete the ones
			// whose copy is already gone, and reap the ones whose source message no
			// longer exists. Both would otherwise sit forever, and a stalled claim
			// makes every contender wait on a winner with nothing left to deliver.
			sess.asm.recoverQ2AffClaims(sess.idHash, q2AffGen, proofEvidence)
		}
	}

	if hasPendingPubRel {
		if err := sess.ensurePubRelConsumerSubscription(c); err != nil {
			c.Errorf("Unable to re-establish PUBREL delivery on resume: %v", err)
		}
		slices.SortFunc(relNaks, bySeq)
		for _, n := range relNaks {
			jsa.sendMsg(n.ackSubject, AckNak)
		}
	}

	// PUBLISH redeliveries reuse their packet identifiers and bypass the in-flight
	// gate, so respect the client's Receive Maximum: NAK at most the effective cap,
	// in stream order; the JS consumer re-offers the rest via AckWait. Deliveries
	// already in flight and not part of this NAK set hold slots of their own, so
	// they come off the budget first. When a Receive Maximum applies, in-flight
	// PUBRELs count against it too, so subtract them; without one the cap is the JS
	// MaxAckPending, which PUBRELs have already acked out of. Spec5 [MQTT-3.3.4-9].
	//
	// EVERY TERM IS READ HERE, under one lock, and not at the top of the pass. Between
	// the two points this function reserves reaped-source entries, replays PUBRELs and
	// restores the affine path, and each of those moves a term: a reserved entry leaves
	// the NAK set while still holding its slot, and a PUBREC replaces a PUBLISH with a
	// PUBREL. Refreshing one term against a stale other is worse than refreshing none —
	// the PUBLISH leaves the fresh count while its replacement is missing from the old
	// one — so the whole budget is observed at the point it is used.
	//
	// The NAK set is filtered here too, and unconditionally: an entry acknowledged or
	// replaced since the scan is neither NAK'd as that old delivery nor subtracted as
	// one. Identity is (durable, sequence), never the identifier alone.
	//
	// This is coherent accounting AT THE CAPTURE POINT, not atomic admission through the
	// sends below. This pass runs synchronously at the end of CONNECT processing, so
	// acknowledgements on the resuming connection are serialized against it; an
	// independent callback is not, and that residual is pre-existing.
	sess.mu.Lock()
	live := pubNaks[:0]
	for _, n := range pubNaks {
		if ack := sess.pendingPublish[n.pi]; ack != nil && ack.sseq == n.sseq && ack.jsDur == n.jsDur {
			live = append(live, n)
		}
	}
	pubNaks = live
	// Everything in flight that is NOT being NAK'd still occupies a slot at the client:
	// a fresh delivery, a pre-resume entry with no ack subject to NAK, every exchange the
	// custody ledger charges, and — since F-100 — an entry this pass reserved because its
	// source is gone but the receiver may hold its identifier. All of them come off the
	// redelivery budget. The replayed entries themselves are ALREADY counted, so exactly
	// their counts come back out (replayAllowanceLocked): budgeting them as new work would
	// replay nothing at saturation, and the acknowledgements that free the capacity would
	// never come.
	replay := make(map[uint16]*mqttPending, len(pubNaks))
	for _, n := range pubNaks {
		replay[n.pi] = sess.pendingPublish[n.pi]
	}
	capacity := sess.replayAllowanceLocked(replay)
	replayed := min(len(pubNaks), capacity)
	deferredCap := sess.resumeDeferredAllowanceLocked(capacity, replayed)
	sess.mu.Unlock()
	slices.SortFunc(pubNaks, bySeq)
	if len(pubNaks) > capacity {
		pubNaks = pubNaks[:capacity]
	}
	for _, n := range pubNaks {
		jsa.sendMsg(n.ackSubject, AckNak)
	}

	// Deferred (withheld, never transmitted) deliveries fill the capacity left
	// after pending redeliveries; the rest re-offer via AckWait and re-enter
	// deferred through trackPublish.
	if rem := deferredCap; rem > 0 {
		slices.SortFunc(deferredNaks, bySeq)
		if len(deferredNaks) > rem {
			deferredNaks = deferredNaks[:rem]
		}
		for _, n := range deferredNaks {
			jsa.sendMsg(n.ackSubject, AckNak)
		}
	}
	// The pass is over. A test asserting an ABSENCE — that a reserved entry spent the
	// capacity a redelivery would otherwise have used — has no arrival to wait on, and
	// the CONNACK is no boundary at all: it precedes this whole function.
	mqttTestRestorePauseHook("resume-naks-done")
}

// trackAsPubRel is invoked in 2 cases: (a) when we receive a PUBREC and we need
// to change from tracking the PI as a PUBLISH to a PUBREL; and (b) when we
// attempt to deliver the PUBREL to record the JS ack subject for it.
//
// Lock held on entry
// pubRelDurable is the PUBREL consumer's durable name, or empty when none is installed.
// sess.mu must be held.
func (sess *mqttSession) pubRelDurable() string {
	if sess.pubRelConsumer == nil {
		return _EMPTY_
	}
	return sess.pubRelConsumer.Durable
}

func (sess *mqttSession) trackAsPubRel(pi uint16, jsAckSubject string) {
	if sess.recoveryPIHeldLocked(pi) {
		return
	}
	if sess.pubRelConsumer == nil {
		// The cosumer MUST be set up already.
		return
	}
	jsDur := sess.pubRelConsumer.Durable

	if sess.pendingPubRel == nil {
		sess.pendingPubRel = make(map[uint16]*mqttPending)
	}

	// THE EXCHANGE'S DURABLE IDENTITY SURVIVES THIS BOOKKEEPING, and it has to.
	//
	// This function REPLACES the entry — that is deliberate, because a redelivery must
	// pick up a fresh acknowledgement subject — but the slot reference, its sequence and
	// the exchange nonce are facts about the EXCHANGE, not about the delivery attempt.
	// Losing them here silently disarms the terminal release: PUBCOMP addresses the slot
	// through this entry, and with a zero reference it does nothing at all, so the slot
	// stays past PUBREC forever and its identifier is reserved for the life of the
	// session. That defect appeared twice during F-092's implementation, at two different
	// sites, before being fixed here where every caller inherits it.
	//
	// AND SO DOES ITS AUTHORITY, and no more of it: the successor inherits the predecessor's
	// kind and evidence (mqttNewPubRelSuccessor) and never mints any. Prefer an existing
	// pendingPubRel: on record redelivery it is already the PUBREL-side authority, while a
	// resident pendingPublish entry may be the stale half F-093's ordered handoff is about.
	// The first PUBREC handoff runs before untrackPublishEntry, so the PUBLISH entry supplies
	// the identity there — including for RETAINED QoS 2, where there is deliberately no
	// durable slot to recover it from (retainedPubRelAdmittedLocked_ finds the Retained kind),
	// and the custody charge, which the PUBREC keeps (Spec5 [4.9]) and the ledger already
	// charges. A Hold predecessor (the scan's placeholder) yields a Hold: only an acquired
	// action promotes it (bindActionToPubRelLocked).
	prev, pub := sess.pendingPubRel[pi], sess.pendingPublish[pi]
	var sseq uint64
	if jsAckSubject != _EMPTY_ {
		sseq, _, _, _, _ = ackReplyInfo(jsAckSubject)
	}
	next := mqttNewPubRelSuccessor(prev, pub, jsDur, sseq, jsAckSubject)
	if !sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectCaptured(prev), next) {
		// An Ordinary successor whose lineage custody no longer holds: the exchange ended,
		// and its projection may not be re-created.
		return
	}
	if jsAckSubject == _EMPTY_ {
		return
	}
	sess.cpending.put(jsDur, sseq, pi)
}

// Stops a PI from being tracked as a PUBREL.
//
// Lock held on entry
func (sess *mqttSession) untrackPubRel(pi uint16) (jsAckSubject string) {
	ack, ok := sess.pendingPubRel[pi]
	if !ok {
		return _EMPTY_
	}

	delete(sess.pendingPubRel, pi)

	if sess.pubRelConsumer != nil {
		sess.cpending.deleteSeq(ack.jsDur, ack.sseq)
	}

	return ack.jsAckSubject
}

// chargeConsumerLocked records that this process added amt to tmaxack for durable,
// so exactly that much is refunded exactly once when it goes away.
//
// Lock held on entry.
func (sess *mqttSession) chargeConsumerLocked(durable string, amt int) {
	if amt == 0 {
		return
	}
	if sess.consCharged == nil {
		sess.consCharged = make(map[string]int)
	}
	sess.consCharged[durable] += amt
	sess.tmaxack += amt
}

// refundConsumerChargeLocked subtracts what was charged for durable, once. A
// durable with no entry was never charged by this process (it was restored from
// the record), so nothing is refunded.
//
// Lock held on entry.
func (sess *mqttSession) refundConsumerChargeLocked(durable string) {
	amt, ok := sess.consCharged[durable]
	if !ok {
		return
	}
	delete(sess.consCharged, durable)
	sess.tmaxack -= amt
	if sess.tmaxack < 0 {
		sess.tmaxack = 0
	}
}
