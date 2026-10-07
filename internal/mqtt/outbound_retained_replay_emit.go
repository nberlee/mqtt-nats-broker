// Copyright 2026 The mqtt-nats-bridge Authors
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
	"strings"
	"sync/atomic"
	"time"
)

// The durable half of a retained replay item (F-105 7c): materialize, admit, then transmit through
// the same send section an ordinary delivery uses.

// mqttRetainedStep is what one durable step established.
type mqttRetainedStep uint8

const (
	// mqttRetainedStepLanded: the step's batch is durable.
	mqttRetainedStepLanded mqttRetainedStep = iota + 1
	// mqttRetainedStepRefused: definitively not landed, every attempt proven never landed.
	mqttRetainedStepRefused
	// mqttRetainedStepEnded: the captured entitlement no longer serves, or the subscription
	// departed before an attempt: nothing further is this producer's to do.
	mqttRetainedStepEnded
	// mqttRetainedStepUnresolved: an attempt may have landed and its handle could not be settled
	// within the bound: the copy may stand, so its owner is the reclaimer and the connection is
	// closed (CONNECT discovery owns it after).
	mqttRetainedStepUnresolved
)

// runRetainedStep runs one durable step to a positive answer. A failed or expired wait is an
// AMBIGUOUS outcome of that attempt, settled from its frozen handle (resolve, abort, resolve) and
// never treated as a refusal; the step is re-run only after its attempt is proven never landed, and
// a step re-refused that way mqttRetainedStepAttempts times is a definitive refusal.
func (sess *mqttSession) runRetainedStep(c *client, r *mqttRetainedReplay, op func() (mqttOpOutcome, error)) mqttRetainedStep {
	refused := 0
	for attempt := 0; ; attempt++ {
		if c.retainedReplayDeparted(r) {
			return mqttRetainedStepEnded
		}
		// ONE ADMITTED ATTEMPT per gate (outbound_foreground_priority.go).
		if !sess.admitBackgroundAuthority() {
			return mqttRetainedStepEnded
		}
		out, err := op()
		if err == nil {
			return mqttRetainedStepLanded
		}
		if errors.Is(err, errMQTTEntitlementEnded) && out.Handle == nil {
			return mqttRetainedStepEnded
		}
		if out.Handle != nil {
			v := sess.settleRetainedHandle(c, r, out.Handle)
			switch v {
			case mqttTransitionLanded, mqttTransitionSuperseded:
				return mqttRetainedStepLanded
			case mqttTransitionUnresolved:
				return mqttRetainedStepUnresolved
			}
		}
		refused++
		if refused >= mqttRetainedStepAttempts {
			c.Warnf("MQTT: retained replay step for client %q refused: %v", sess.id, err)
			return mqttRetainedStepRefused
		}
		time.Sleep(mqttRetainedStepBackoff(attempt))
	}
}

// settleRetainedHandle settles one attempt's handle, retrying an unresolved answer with capped
// backoff, bounded. A DEPARTURE DOES NOT END IT: the attempt may have landed whatever the
// subscription did, so only a settled verdict, or the bound, returns.
func (sess *mqttSession) settleRetainedHandle(c *client, r *mqttRetainedReplay, h *mqttTransitionHandle) mqttTransitionVerdict {
	for attempt := 0; ; attempt++ {
		v := sess.settleCustodyHandle(h)
		if v != mqttTransitionUnresolved || attempt+1 >= mqttRetainedStepAttempts {
			return v
		}
		time.Sleep(mqttRetainedStepBackoff(attempt))
	}
}

// replayDurableItem materializes, admits and transmits one QoS 1/2 retained snapshot.
func (sess *mqttSession) replayDurableItem(c *client, r *mqttRetainedReplay, f mqttRetainedFrame) {
	// abandon releases the allocation on its proof: no admission was attempted under it, or every
	// attempt was proven never landed, so no PUBLISH can be owed under it.
	abandon := func() { sess.abandonRetainedSends(c, []mqttRetainedRef{{pi: f.pi, nonce: f.nonce}}) }
	asm, jsa := sess.asm, sess.jsa
	if asm == nil || jsa == nil {
		abandon()
		return
	}
	epoch := asm.sessStreamEpoch
	ent := f.ent
	mat := f.mat
	maxPayload := 0
	if c.srv != nil {
		maxPayload = int(c.srv.getOpts().MaxPayload)
	}
	slices, perr := mqttRetainedChunkPlan(mat.Data, maxPayload)
	if perr != nil {
		c.Warnf("MQTT: skipping retained message %q for client %q: %v", mat.Topic, sess.id, perr)
		abandon()
		return
	}
	payload, inc := mqttNewInflightNonce(), mqttNewInflightNonce()
	// unfinished ends a materialization before its admission: the copy is closed, its
	// reclamation owned by the reclaimer whatever the close did, and the allocation released.
	// An unresolved step may have landed a copy nothing here can name, so the connection is
	// closed as well and the next CONNECT's discovery finds it.
	var m *mqttRetainedManifest
	unfinished := func(step mqttRetainedStep) {
		sess.closeAbandonedMaterial(c, m, ent.Gen, inc)
		abandon()
		if step == mqttRetainedStepUnresolved {
			sess.oweRetainedClose(c, f.pi, f.nonce, "materialization")
		}
	}

	// 1. RESERVE.
	if step := sess.runRetainedStep(c, r, func() (mqttOpOutcome, error) {
		got, out, err := jsa.opReserveRetainedPayload(sess.id, epoch, mqttNewInflightNonce(), "op-retained-reserve",
			ent.Gen, ent.Tenure, inc, payload, mat, uint64(len(slices)))
		if got != nil {
			m = got
		}
		return out, err
	}); step != mqttRetainedStepLanded {
		if step == mqttRetainedStepRefused || step == mqttRetainedStepEnded {
			// Every attempt proven never landed (or none sent): no copy stands.
			abandon()
			return
		}
		unfinished(step)
		return
	}
	// 2. CHUNKS and 3. SEAL, each against the manifest as it stands.
	reload := func() (uint64, bool) {
		cur, seq, err := jsa.loadRetainedManifest(sess.id, epoch, ent.Gen, payload)
		if err != nil {
			return 0, false
		}
		m = cur
		return seq, true
	}
	for len(slices) > 0 && m.Written < m.Chunks {
		seq, ok := reload()
		if !ok {
			unfinished(mqttRetainedStepRefused)
			return
		}
		batch := slices[m.Written:]
		if len(batch) > 8 {
			batch = batch[:8]
		}
		if step := sess.runRetainedStep(c, r, func() (mqttOpOutcome, error) {
			_, out, err := jsa.opWriteRetainedChunks(mqttNewInflightNonce(), "op-retained-chunks", ent.Tenure, m, seq, batch)
			return out, err
		}); step != mqttRetainedStepLanded {
			unfinished(step)
			return
		}
		if _, ok := reload(); !ok {
			unfinished(mqttRetainedStepRefused)
			return
		}
	}
	seq, ok := reload()
	if ok && m.State == mqttManifestProvisioning {
		if step := sess.runRetainedStep(c, r, func() (mqttOpOutcome, error) {
			_, out, err := jsa.opSealRetainedPayload(mqttNewInflightNonce(), "op-retained-seal", ent.Tenure, m, seq)
			return out, err
		}); step != mqttRetainedStepLanded {
			unfinished(step)
			return
		}
		seq, ok = reload()
	}
	if !ok || m.State != mqttManifestReady {
		unfinished(mqttRetainedStepRefused)
		return
	}

	// 4. ADMIT, under the allocation's admission custody, exactly as an ordinary delivery. An
	// identifier whose register subject still holds another record is RE-ALLOCATED (consult 102):
	// only once every attempt of the old allocation is proven never landed, under sess.mu, keeping
	// the occupant's exclusions, bounded; the conflicting register is never reaped.
	var l mqttLandedRegister
	admitted := false
	for collision := 0; collision < mqttRetainedCollisionAttempts; collision++ {
		var occupied bool
		var committed bool
		l, admitted, occupied, committed = sess.admitRetainedCopy(c, r, f, m, seq)
		if committed {
			// LANDED BUT NOT PROMOTED: the exchange stands durably, so no pre-admission disposition
			// applies. The connection is closed and the reconnect's resume restores it.
			sess.oweRetainedClose(c, f.pi, f.nonce, "admitted PUBLISH")
			return
		}
		if admitted || !occupied {
			break
		}
		nf, ok := sess.reallocateRetained(c, f)
		if !ok {
			break
		}
		f = nf
		abandon = func() { sess.abandonRetainedSends(c, []mqttRetainedRef{{pi: f.pi, nonce: f.nonce}}) }
		if seq, ok = reload(); !ok || !mqttManifestAdmits(m) || m.Admitted != _EMPTY_ {
			break
		}
	}
	if !admitted {
		unfinished(mqttRetainedStepRefused)
		return
	}
	sess.mu.Lock()
	afterAdmit := sess.testRetainedAfterAdmit
	sess.mu.Unlock()
	if afterAdmit != nil {
		afterAdmit(f.pi)
	}
	// DEPARTED AFTER THE ADMISSION, BEFORE A BYTE: the client left, the exchange is proven unsent
	// (its marker stands), and withdrawing it is the departure disposition — not a loss. A
	// departure is not an ended entitlement: the withdrawal is owned until it is confirmed, or
	// the connection closes (outbound_retained_withdraw.go).
	if c.retainedReplayDeparted(r) {
		sess.withdrawDepartedRetained(c, f.pi, f.nonce)
		return
	}
	// 5. TRANSMIT, re-attempted while this connection still serves the subscription: a copy that
	// could not be acquired has no source redelivery to retry it. Exhausted, the connection is
	// closed and the reconnect's resume replays the exchange.
	for attempt := 0; ; attempt++ {
		if sess.transmitRetained(c, r, f, m, l, true) {
			return
		}
		if attempt+1 >= mqttRetainedOwedAttempts {
			sess.oweRetainedClose(c, f.pi, f.nonce, "PUBLISH")
			return
		}
		time.Sleep(mqttRetainedStepBackoff(attempt))
		sess.mu.Lock()
		current := sess.c == c
		sess.mu.Unlock()
		if !current || c.isClosed() {
			return
		}
		if c.retainedReplayDeparted(r) {
			sess.withdrawDepartedRetained(c, f.pi, f.nonce)
			return
		}
	}
}

// closeAbandonedMaterial closes a copy that will never be admitted, the first act of its
// reclamation, and hands the copy to the reclaimer WHATEVER the close did: the reclaimer's own
// first step is the same close, so an unreadable copy or a refused close is its to retry.
func (sess *mqttSession) closeAbandonedMaterial(c *client, m *mqttRetainedManifest, gen, inc string) {
	defer sess.wakeRetainedReclaim(gen, inc)
	if m == nil || sess.jsa == nil || !sess.admitBackgroundAuthority() {
		return
	}
	cur, seq, err := sess.jsa.loadRetainedManifest(m.ID, m.Epoch, m.Gen, m.Payload)
	if err != nil || cur.State == mqttManifestClosed || cur.Admitted != _EMPTY_ {
		return
	}
	if _, cerr := sess.jsa.opCloseRetainedManifest(mqttNewInflightNonce(), "op-retained-close", cur, seq); cerr != nil {
		c.Warnf("MQTT: closing abandoned retained copy %q for client %q: %v", m.Payload, sess.id, cerr)
	}
}

// admitRetainedCopy admits the copy and promotes the allocation. It returns the landed register.
func (sess *mqttSession) admitRetainedCopy(c *client, r *mqttRetainedReplay, f mqttRetainedFrame,
	m *mqttRetainedManifest, mSeq uint64) (l mqttLandedRegister, admitted, occupied, committed bool) {

	l, admitted, committed = sess.admitRetainedCopyOnce(c, r, f, m, mSeq)
	if admitted || committed || c.retainedReplayDeparted(r) {
		return l, admitted, false, committed
	}
	// Every attempt was proven never landed (an unresolved one was settled before giving up), so
	// a record standing on the subject that is not this allocation's is an occupant.
	cur, smsg, lerr := sess.asm.loadInflightRecord(mqttInflightSubject(sess.idHash, f.ent.Gen, f.pi))
	return l, false, lerr == nil && smsg != nil && (cur == nil || cur.Nonce != f.nonce), false
}

// mqttRetainedCollisionAttempts bounds re-allocations for one retained item.
const mqttRetainedCollisionAttempts = 3

// reallocateRetained releases a never-admitted allocation on its proof and takes a fresh one for
// the same item, under sess.mu, validating the captured generation, connection and nonce first.
// No QoS 0 fallback: an identifier that cannot be allocated ends the item (the F-4 skip).
func (sess *mqttSession) reallocateRetained(c *client, f mqttRetainedFrame) (mqttRetainedFrame, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.c != c || sess.gen != f.ent.Gen {
		return f, false
	}
	if p := sess.pendingPublish[f.pi]; p == nil || !p.regPending || p.regNonce != f.nonce {
		return f, false
	}
	sess.releaseRetainedSendAbandonedLocked(c, f.pi, f.nonce)
	pi := sess.trackPublishRetained(f.qos)
	if pi == 0 {
		return f, false
	}
	p := sess.pendingPublish[pi]
	if p == nil || !sess.beginRetainedSendLocked(c, pi) {
		sess.untrackPublish(pi)
		return f, false
	}
	nf := f
	nf.pi, nf.nonce = pi, p.regNonce
	return nf, true
}

// admitRetainedCopyOnce runs one allocation's admission attempts.
//
// committed reports an admission KNOWN to have landed whose register or marker could not be read
// back to promote the allocation: it is never an unadmitted copy, so its caller must not take any
// pre-admission disposition over it.
func (sess *mqttSession) admitRetainedCopyOnce(c *client, r *mqttRetainedReplay, f mqttRetainedFrame,
	m *mqttRetainedManifest, mSeq uint64) (l mqttLandedRegister, admitted, committed bool) {

	sess.mu.Lock()
	gen, regClaim, regClaimSeq := sess.gen, sess.regClaim, sess.seq
	opened := gen == f.ent.Gen && sess.openAdmissionCustodyLocked(gen, f.pi, f.nonce)
	sess.mu.Unlock()
	if !opened {
		return mqttLandedRegister{}, false, false
	}
	defer sess.finishAdmissionCustody(gen, f.nonce)
	reg := &mqttInflightRecord{
		PI: f.pi, QoS: f.qos, Nonce: f.nonce, Lin: f.nonce, State: mqttInflightPublish,
		Claim: regClaim, ClaimSeq: regClaimSeq, Payload: m.Payload, Digest: m.Digest,
	}
	subject := mqttInflightSubject(sess.idHash, gen, f.pi)
	landed := false
	for attempt := 0; attempt < mqttRetainedStepAttempts && !landed; attempt++ {
		if c.retainedReplayDeparted(r) {
			return mqttLandedRegister{}, false, false
		}
		if !sess.admitBackgroundAuthority() {
			return mqttLandedRegister{}, false, false
		}
		out, err := sess.jsa.opAdmitRetained(&mqttAdmitRetainedRequest{
			ID: sess.id, Epoch: sess.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-admit-retained",
			Gen: f.ent.Gen, Tenure: f.ent.Tenure, Register: reg, Manifest: m, ManifestSeq: mSeq,
		})
		if sess.recordAdmissionAttempt(gen, f.nonce, reg.Lin, out, err) {
			landed = true
			break
		}
		if errors.Is(err, errMQTTEntitlementEnded) && out.Handle == nil {
			return mqttLandedRegister{}, false, false
		}
		// EVERY VERDICT IS CLASSIFIED: only a positively never-landed attempt may be re-submitted or
		// lead to a pre-admission disposition. A landing the custody settlement could not see is
		// still a landing.
		if out.Handle != nil {
			v := sess.jsa.resolveTransitionHandle(out.Handle)
			if v == mqttTransitionUnresolved {
				// AMBIGUOUS: never re-admitted, never released, resolved on THIS allocation.
				v = sess.settleRetainedHandle(c, r, out.Handle)
			}
			if v == mqttTransitionLanded || v == mqttTransitionSuperseded {
				sess.mu.Lock()
				sess.custody.attemptLanded(gen, f.nonce, out.Handle, reg.Lin)
				sess.mu.Unlock()
				landed = true
				break
			}
			if v == mqttTransitionUnresolved {
				// Still unresolved past the bound: possibly landed, so never unadmitted.
				return mqttLandedRegister{}, false, true
			}
		}
		time.Sleep(mqttRetainedStepBackoff(attempt))
		// The copy may have moved (another admission is impossible; a close is not): re-read. A
		// copy naming THIS exchange as admitted is the durable proof that an attempt landed.
		cur, seq, lerr := sess.jsa.loadRetainedManifest(m.ID, m.Epoch, m.Gen, m.Payload)
		if lerr == nil && cur.Admitted == reg.Lin {
			sess.mu.Lock()
			sess.custody.attemptLanded(gen, f.nonce, out.Handle, reg.Lin)
			sess.mu.Unlock()
			landed = true
			break
		}
		if lerr != nil {
			// Unreadable is not "not admitted": an earlier attempt may have landed.
			return mqttLandedRegister{}, false, true
		}
		if !mqttManifestAdmits(cur) || cur.Admitted != _EMPTY_ {
			return mqttLandedRegister{}, false, false
		}
		m, mSeq = cur, seq
	}
	if !landed {
		return mqttLandedRegister{}, false, false
	}
	// LANDED: the readback that promotes the allocation is re-attempted, never read as "not
	// admitted" — a failed read here says nothing about the durable exchange, which stands.
	for attempt := 0; attempt < mqttRetainedStepAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(mqttRetainedStepBackoff(attempt - 1))
		}
		cur, smsg, lerr := sess.asm.loadInflightRecord(subject)
		if lerr != nil || smsg == nil || cur == nil || cur.Nonce != f.nonce {
			continue
		}
		l = mqttLandedRegister{gen: gen, pi: f.pi, regSeq: smsg.Sequence, nonce: f.nonce, lin: reg.Lin, qos: f.qos}
		load := sess.asm.loadInflightMarker(mqttMarkerCallerStore, subject)
		if load.kind != mqttInflightResponsePresent || load.marker == nil || load.marker.Nonce != f.nonce {
			continue
		}
		slot := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
		if slot == nil {
			continue
		}
		sess.mu.Lock()
		prev := sess.pendingPublish[f.pi]
		ok := prev != nil && prev.regPending && prev.regNonce == f.nonce &&
			sess.setPendingLocked(mqttPendingPublishMap, f.pi, mqttExpectExactly(prev),
				mqttPromoteRetainedLanded(prev, l, load.seq, *slot, m.Inc))
		sess.mu.Unlock()
		return l, ok, !ok
	}
	return l, false, true
}

// transmitRetained puts the admitted copy on the wire through the ordinary send section: the
// section, the durable action, the marker's confirmed delete as intent, then the authorized write.
//
// It reports whether the PUBLISH is SETTLED for this connection: written, handed to its
// disposition's owner, the allocation moved on to another owner, or the server shutting down
// (the connection closes). EVERY other exit reports false — still owed here, nothing written —
// and its caller re-attempts it and, exhausted, closes the connection: a retained copy has no
// source redelivery to retry it. background marks the driver's own sections
// (outbound_foreground_priority.go).
func (sess *mqttSession) transmitRetained(c *client, r *mqttRetainedReplay, f mqttRetainedFrame,
	m *mqttRetainedManifest, l mqttLandedRegister, background bool) bool {

	// JUDGED BEFORE THE SEND SECTION: an acquisition under this connection's tenure would make the
	// delivery this connection's quota, which no abandonment may then release (outbound_retained_abandon.go).
	verdict, reason := c.retainedReplayVerdict(f, m)
	if h := sess.testRetainedVerdict.Load(); h != nil {
		verdict, reason = (*h)(verdict, reason)
	}
	sess.mu.Lock()
	// NOT THIS ALLOCATION'S ANY MORE (answered, withdrawn, abandoned, replaced): settled here, so a
	// refusal below can be treated as retryable without retrying an exchange that moved on.
	if p := sess.pendingPublish[f.pi]; p == nil || p.regNonce != f.nonce ||
		(p.kind != mqttPendingRetained && !p.regPending) {
		sess.mu.Unlock()
		return true
	}
	// THE RESERVATION IS HANDED FROM THE SNAPSHOT'S RETAINED HOLD TO THE SEND SECTION in one
	// critical section: the retained hold would make the section refuse, and dropping it first
	// would leave the identifier unreserved in between.
	sess.handRetainedHoldToSectionLocked(f.pi)
	if verdict != mqttRetainedReplayServable {
		if verdict != mqttRetainedReplayRefused {
			// INDETERMINATE decides nothing: the hold goes back and the PUBLISH stays owed.
			sess.beginRetainedSendLocked(c, f.pi)
			sess.mu.Unlock()
			return false
		}
		sess.mu.Unlock()
		sess.disposeUnservableRetained(c, f.pi, f.nonce, m, reason, background)
		return true
	}
	sendTok, sendOK := sess.beginSendLocked(c, f.pi, _EMPTY_, 0)
	if !sendOK {
		sess.beginRetainedSendLocked(c, f.pi)
	}
	sess.mu.Unlock()
	if !sendOK {
		return false // a live section on the identifier: still owed, retried
	}
	if background {
		sendTok.admit = sess.admitBackgroundAuthority
	}
	if !c.srv.admitPublishSend() {
		sess.endPublishSend(c, f.pi, sendTok, mqttIntentNone, false)
		return true // the server is shutting down: the resume replays it
	}
	defer c.srv.publishSendDone()
	intent, wrote := mqttIntentNone, false
	defer func() { sess.endPublishSend(c, f.pi, sendTok, intent, wrote) }()
	if !sess.acquirePublishAction(c, f.pi, &sendTok) {
		return false
	}
	prep, fits := c.mqttPrepareRetainedPublish(f, m, sendTok.dup)
	if !fits {
		return false // judged before the section, so unreachable; nothing written, still owed
	}
	intent = sess.recordSendIntent(c, f.pi, sendTok)
	if intent == mqttIntentDefiniteFailure || intent == mqttIntentAmbiguous {
		return false
	}
	prep.bindSection(sendTok)
	var sub *subscription
	if r != nil {
		sub = r.sub
	}
	wrote = c.mqttEnqueuePreparedPublishMsgTo(c, sub, prep)
	// A write the connection refused while it stays open wrote nothing: still owed.
	return wrote || c.isClosed()
}

// mqttPrepareRetainedPublish builds a retained PUBLISH for this connection from the copy's fields
// and the snapshot's bytes: RETAIN set, full topic (never an alias), the remaining Message Expiry
// Interval from the absolute deadline, the delivery's own Subscription Identifier, and the Maximum
// Packet Size checked on the frame as built.
func (cc *client) mqttPrepareRetainedPublish(f mqttRetainedFrame, m *mqttRetainedManifest, dup bool) (mqttPreparedPublish, bool) {
	v5 := cc.mqtt.proto == mqttProtoLevel5
	var props []byte
	if v5 {
		props = m.Props
		if m.ExpiresAt != 0 {
			rem := uint32(time.Until(time.Unix(0, m.ExpiresAt)).Seconds())
			if rem < 1 {
				rem = 1
			}
			props = mqttSetMessageExpiry(m.Props, rem)
		}
		if m.SubID > 0 {
			props = mqttInjectSubID(props, int(m.SubID))
		}
	}
	prep := mqttPreparedPublish{pi: f.pi, qos: f.qos, dup: dup, retain: true, v5: v5,
		topic: []byte(m.Topic), msg: f.mat.Data, props: props}
	prep.flags, prep.header = mqttMakePublishHeader(f.pi, f.qos, dup, true, v5, prep.topic, props, len(prep.msg))
	if mp := cc.mqtt.maxPacketSize; mp != 0 && len(prep.header)+len(prep.msg) > int(mp) {
		return prep, false
	}
	return prep, true
}

// wakeRetainedReclaim schedules a payload's reclamation (outbound_retained_reclaim.go).
func (sess *mqttSession) wakeRetainedReclaim(gen, inc string) {
	sess.retainedReclaim.wake(sess, gen, inc)
}

// continueAbandonedRetainedPubRel hands an ABANDONED retained exchange's confirmed PUBREC intent to
// its PUBREL (F-105 7c): its advance created the canonical, nothing is materialized, so the PUBREL
// entry is installed from the ACK-only snapshot and emitted. Shared by the PUBREC handler and the
// asynchronous intent resolver, so a confirmation reached either way owes the same continuation.
// An installation that is refused is OWNED, never dropped: a refusal proves nothing about the
// PUBREL the client is now owed.
//
// No lock held on entry.
func (sess *mqttSession) continueAbandonedRetainedPubRel(c *client, pi uint16, ack *mqttPending) {
	installed, owned := sess.installAbandonedRetainedPubRel(c, pi, ack)
	switch {
	case installed:
		sess.emitRetainedPubRel(c, pi)
	case owned:
		snap := *ack
		sess.oweRetained(&mqttRetainedOwed{c: c, pi: pi, nonce: ack.regNonce, kind: mqttRetainedOwedInstall, snap: &snap})
	}
}

// installAbandonedRetainedPubRel installs an abandoned retained exchange's PUBREL entry for c.
// installed reports the entry standing (installed now, or already this exchange's); owned reports
// a refusal that leaves the PUBREL owed here. Neither: the exchange is not c's any more — another
// incarnation, another connection (whose resume restored or quarantined it), or a quarantine of
// this allocation, which authorizes nothing (consult 107).
//
// No lock held on entry.
func (sess *mqttSession) installAbandonedRetainedPubRel(c *client, pi uint16, ack *mqttPending) (installed, owned bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if ack == nil || ack.ev.gen != sess.gen || sess.c != c || sess.retainedQuarantinedLocked(ack.regNonce) {
		return false, false
	}
	if cur := sess.pendingPubRel[pi]; cur != nil {
		if cur.kind == mqttPendingRetained && cur.regNonce == ack.regNonce {
			return true, false
		}
		return false, true
	}
	if h := sess.testRetainedInstallRefused.Load(); h != nil && (*h)() {
		return false, true
	}
	if sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectEmpty(), mqttNewAbandonedRetainedPubRel(ack)) {
		return true, false
	}
	return false, true
}

// emitRetainedPubRel transmits a retained exchange's PUBREL DIRECTLY, after its PUBREC advance
// created the canonical record on $MQTT_inflight (F-105 7c): there is no $MQTT_out delivery to
// carry it, so this is the delivery callback's acquire, bind, enter and write, from the register.
// An emission that could not complete is OWNED by the continuation worker while this connection
// lives; an ended entitlement writes nothing, and the successor's resume replays the PUBREL from
// the canonical.
//
// No lock held on entry.
func (sess *mqttSession) emitRetainedPubRel(c *client, pi uint16) {
	sess.mu.Lock()
	var nonce string
	if rel := sess.pendingPubRel[pi]; rel != nil {
		nonce = rel.regNonce
	}
	sess.mu.Unlock()
	if !sess.emitRetainedPubRelOnce(c, pi, nonce, nil) {
		sess.ownRetainedOwed(c, pi, nonce, mqttRetainedOwedPubRel)
	}
}

// emitRetainedPubRelOnce makes one emission and reports whether the PUBREL is SETTLED for this
// connection: written, owed by nobody here (no retained PUBREL entry of this allocation, a register
// past or not at pubrel_ready), or owed to a successor (the entitlement ended). Every other exit —
// an unreadable register, an unattributable one, an action not acquired, a live section, a refused
// binding (custody without the lineage), a refused write entry — reports false: still owed. admit,
// when set, admits each acquisition attempt (a background caller, outbound_foreground_priority.go).
//
// No lock held on entry.
func (sess *mqttSession) emitRetainedPubRelOnce(c *client, pi uint16, nonce string, admit func() bool) bool {
	as := sess.asm
	if as == nil || c == nil {
		return true
	}
	sess.mu.Lock()
	rel := sess.pendingPubRel[pi]
	sess.mu.Unlock()
	if rel == nil || rel.kind != mqttPendingRetained || rel.ev.gen == _EMPTY_ || rel.regNonce != nonce {
		return true // not this allocation's PUBREL entry: its PUBCOMP or its replacement owns it
	}
	subject := mqttInflightSubject(sess.idHash, rel.ev.gen, pi)
	cur, smsg, lerr := as.loadInflightRecord(subject)
	if lerr != nil {
		return false
	}
	if smsg == nil || cur == nil || !cur.retainedSource() || cur.State != mqttInflightPubRelReady ||
		cur.Lin != rel.ev.lin {
		return true
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	if ref == nil {
		return false
	}
	act, ended := sess.mqttAcquirePubRelAction(c, *ref, cur.Lin, 0, admit)
	if act == nil {
		return ended
	}
	defer sess.startActionDrain(c, act)
	sess.mu.Lock()
	sendTok, sendOK := sess.beginPubRelSendLocked(c, pi)
	if !sendOK {
		// A live local section on the identifier — a PUBLISH section still releasing its token
		// after its durable drain — is not a settlement: the PUBREL is still owed. Retried.
		sess.mu.Unlock()
		return false
	}
	defer sess.endSend(c, pi, sendTok)
	if !sess.bindRetainedPubRelLocked(pi, act) || !sess.pubRelWriteKindLocked(pi, act) {
		sess.mu.Unlock()
		return false
	}
	ticket, entered := sess.enterPubRelWriteLocked(act, c)
	trace := c.trace
	sess.mu.Unlock()
	if !entered {
		return false
	}
	defer ticket.relinquish()
	if h := mqttTestRetainedPubRelEnteredV.Load(); h != nil {
		(*h)(act)
	}
	c.mqttWriteAuthorizedPubRel(pi, ticket, trace)
	return true
}

// mqttTestRetainedPubRelEnteredV runs on a retained PUBREL after its write entry succeeded and its
// relinquish is armed, before the authorized write, so a test can capture the exact holder that
// entered. nil in production.
var mqttTestRetainedPubRelEnteredV atomic.Pointer[func(act *mqttSendAction)]

// bindRetainedPubRelLocked makes an acquired PUBREL action the retained exchange's PUBREL entry:
// the resident must be this exchange's Retained entry, custody must hold its lineage, and nothing
// source-shaped is mapped — a retained PUBREL has no consumer delivery and no cpending entry.
//
// Lock held on entry.
func (sess *mqttSession) bindRetainedPubRelLocked(pi uint16, act *mqttSendAction) bool {
	prev := sess.pendingPubRel[pi]
	if act == nil || prev == nil || prev.kind != mqttPendingRetained || prev.regNonce != act.slot.nonce ||
		!sess.ordinaryHeldLocked(mqttEvidenceFromAction(act)) {
		return false
	}
	return sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectExactly(prev), mqttPromoteRetainedPubRelAction(prev, act))
}

// disposeRetainedRel removes a terminal retained exchange's canonical PUBREL by its exact sequence,
// after reading it back as THIS exchange's record (identity, nonce, identifier). An absent record
// is the store's positive answer that it is gone; an unreadable or foreign one keeps the slot.
func (sess *mqttSession) disposeRetainedRel(cur *mqttInflightRecord) bool {
	if cur == nil || cur.RelSubj == _EMPTY_ || sess.asm == nil {
		return cur != nil && cur.RelSubj == _EMPTY_ && cur.QoS == 1
	}
	smsg, err := sess.jsa.loadLastMsgFor(mqttInflightStreamName, cur.RelSubj)
	if err != nil {
		// The store's positive "no message", seen through the loader's wrapping.
		ae := jsAPIError(err)
		return ae != nil && IsNatsErr(ae, JSNoMessageFoundErr)
	}
	name, perr := mqttParseInflightSubject(mqttInflightSubject(sess.idHash, sess.genOfRel(cur), cur.PI))
	if perr != nil {
		return false
	}
	rel, derr := mqttDecodeRetainedRel(smsg.Data, sess.id, sess.asm.sessStreamEpoch, name.gen, cur.Lin)
	if derr != nil || rel.Identity != cur.RelID || rel.PI != cur.PI {
		return false
	}
	return sess.jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true) == nil
}

// genOfRel is the generation token a retained canonical subject names.
func (sess *mqttSession) genOfRel(cur *mqttInflightRecord) string {
	toks := strings.Split(cur.RelSubj[len(mqttF105SubjectPrefix()):], ".")
	if len(toks) != 4 {
		return _EMPTY_
	}
	return toks[2]
}

// wakeRetainedReclaimForPayload wakes the reclamation of the copy a retained exchange depended on,
// found through its manifest. An unreadable manifest is read again, capped, while this object is
// resident — the copy's ending is its only wake, so a failed read is never the end of it.
func (sess *mqttSession) wakeRetainedReclaimForPayload(gen, payload string) {
	if sess.asm == nil || payload == _EMPTY_ {
		return
	}
	if sess.wakeRetainedReclaimForPayloadOnce(gen, payload) {
		return
	}
	go func() {
		for attempt := 0; sess.resident(); attempt++ {
			time.Sleep(mqttRetainedStepBackoff(attempt))
			if sess.wakeRetainedReclaimForPayloadOnce(gen, payload) {
				return
			}
		}
	}()
}

// wakeRetainedReclaimForPayloadOnce reports whether the manifest was read and the copy woken; a
// copy positively absent needs no reclamation.
func (sess *mqttSession) wakeRetainedReclaimForPayloadOnce(gen, payload string) bool {
	m, _, err := sess.jsa.loadRetainedManifest(sess.id, sess.asm.sessStreamEpoch, gen, payload)
	if err != nil {
		// The store's positive "no message", seen through the loader's wrapping.
		ae := jsAPIError(err)
		return ae != nil && IsNatsErr(ae, JSNoMessageFoundErr)
	}
	sess.wakeRetainedReclaim(gen, m.Inc)
	return true
}
