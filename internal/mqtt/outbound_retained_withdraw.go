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

// Withdrawing an admitted, proven-unsent retained exchange (F-105 7c).
//
// THE ENTRY IS THE OBLIGATION UNTIL THE WITHDRAWAL IS CONFIRMED. The ordinary proven-unsent door
// (dropProvenUnsentPublishEntry) untracks first and ignores the delete's outcome, because a
// source-backed exchange has a redelivery to repair a delete that failed. A retained exchange has
// none: an untrack before the confirmation would leave its register standing at PUBLISH with no
// owner and its identifier excluded against an absence that never comes. So here the entry stays
// tracked — the identifier reserved, the exchange still this connection's — across the gated
// delete (the exchange withdrawn first, each attempt admitted on its own), and it goes only on the
// delete's classified outcome, the exclusion handed over in the same critical section and lifted
// only by a subject-confirmed release. A refused delete is retried by its owner; exhausted, the
// connection closes and the resume restores the exchange from its records.

// withdrawDepartedRetained withdraws an admitted retained exchange whose subscription departed
// before a byte was written, owning the withdrawal until it is confirmed or the connection closes.
//
// No lock held on entry.
func (sess *mqttSession) withdrawDepartedRetained(c *client, pi uint16, nonce string) {
	sess.withdrawRetainedUnsent(c, pi, nonce, true)
}

// withdrawRetainedUnsent begins the withdrawal of a proven-unsent retained allocation: the entry's
// identity is captured and its identifier EXCLUDED first — so whatever removes the entry from now
// on (the custody observer does, the moment the withdrawal lands and ends the exchange) the
// identifier stays reserved over the register — then one attempt is made, and an unsettled
// withdrawal is handed to the continuation worker with that captured identity.
//
// No lock held on entry.
func (sess *mqttSession) withdrawRetainedUnsent(c *client, pi uint16, nonce string, background bool) {
	sess.mu.Lock()
	p := sess.pendingPublish[pi]
	if p == nil || p.kind != mqttPendingRetained || p.regNonce != nonce || p.transmitted || p.sendNonce != _EMPTY_ ||
		!p.slot.slotOK() || p.slot.name.subject == _EMPTY_ {
		// Answered, abandoned, replaced, or possibly sent through a live section: whichever path
		// moved it owns what remains. (An entry with no register reference is a never-admitted
		// allocation, which is not withdrawn here.)
		sess.mu.Unlock()
		return
	}
	snap := *p
	sess.excludeIdentifierLocked(pi, snap.slot.name.subject, nonce)
	sess.mu.Unlock()
	if !sess.withdrawRetainedOnce(c, &snap, background) {
		sess.oweRetained(&mqttRetainedOwed{c: c, pi: pi, nonce: nonce, kind: mqttRetainedOwedWithdraw, snap: &snap})
	}
}

// withdrawRetainedOnce makes one withdrawal attempt of the captured exchange and reports whether
// it is SETTLED: its register's delete confirmed — deleted, already absent, or preserved for
// another occupant or mover. false: the withdrawal or the delete was refused or could not be read,
// and the register still stands, owed. The entry's own presence decides nothing: the obligation is
// the REGISTER, named by the snapshot.
//
// background admits each of the gated door's authority attempts (outbound_foreground_priority.go).
//
// No lock held on entry.
func (sess *mqttSession) withdrawRetainedOnce(c *client, snap *mqttPending, background bool) bool {
	if sess.asm == nil {
		return false
	}
	// THE REFERENCE IS RE-READ ON EVERY ATTEMPT. A withdrawal that landed rewrote the register as
	// OUR cancellation tombstone before a delete that then failed; the retry must consume that
	// tombstone at its own sequence, not read the rewrite as another owner's move.
	slot := snap.slot
	cur, smsg, lerr := sess.asm.loadInflightRecord(slot.name.subject)
	switch {
	case lerr != nil:
		return false
	case smsg != nil && cur != nil && cur.Nonce == snap.regNonce && cur.Lin == snap.ev.lin:
		ref := mqttSlotRefFromRecord(slot.name.subject, smsg.Sequence, cur)
		if ref == nil {
			return false
		}
		slot = *ref
	}
	seq := slot.seq
	if seq == 0 {
		seq = snap.regSeq
	}
	var out mqttReapOutcome
	if background {
		out = sess.reapRegisterGatedOutcome(seq, snap.regNonce, true, &slot, mqttSlotDeleteComplete,
			mqttLicenceOf(c).withAdmission(sess.admitBackgroundAuthority))
	} else {
		out = sess.reapRegisterGatedOutcome(seq, snap.regNonce, true, &slot, mqttSlotDeleteComplete, mqttLicenceOf(c))
	}
	switch out {
	case mqttReapRefused:
		return false
	case mqttReapDeleted, mqttReapAbsent:
		// The register half is proven gone, which is the marker reap's precondition.
		sess.reapWithdrawnRetainedMarker(snap)
	}
	// Settled: the entry goes if it still stands (the exclusion was handed over before the first
	// attempt), and the exclusion is lifted only by a subject-confirmed release.
	pi := slot.name.pi
	sess.mu.Lock()
	if p := sess.pendingPublish[pi]; p != nil && p.regNonce == snap.regNonce {
		sess.untrackPublishEntry(pi, mqttUntrackEnded) // its withdrawal settled the exchange
	}
	sess.mu.Unlock()
	sess.settleRecoveryDropExclusion(&slot, snap.regNonce, snap.jsDur, snap.sseq)
	sess.wakeRetainedReclaim(snap.ev.gen, snap.retainedInc)
	return true
}

// reapWithdrawnRetainedMarker removes a withdrawn exchange's marker once its register is proven
// gone, as deleteInflightRegisterFor does for the ordinary door; a marker it cannot name is left to
// the sweep.
func (sess *mqttSession) reapWithdrawnRetainedMarker(p *mqttPending) {
	if sess.asm == nil {
		return
	}
	var c *client
	if sess.jsa != nil {
		c = sess.jsa.c
	}
	switch {
	case p.markerSeq != 0:
		sess.asm.reapMarkerCaptured(c, mqttMarkerCallerCleanup, p.slot.name.subject, p.regNonce, p.markerSeq)
	case p.transmitted:
		sess.asm.reapMarkerNoneHeld(c, mqttMarkerCallerCleanup, p.slot.name.subject)
	}
}
