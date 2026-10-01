//go:build mqttport_fulltests

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
	"fmt"
	"testing"
	"time"
)

// THE RETAINED OWNERSHIP ORACLE (consult 106). One statement of the property every retained fault
// test asserts at its quiescence points, so a path that drops an obligation fails it whichever
// path that is — the per-path assertions beside it are supplements:
//
//   - every retained register standing at PUBLISH or pubrel_ready — OUR cancellation tombstone
//     included — has an OWNER on this session: its pending entry (by nonce), its abandoned-ACK
//     binding, a continuation-worker obligation naming its allocation, an abandonment decision
//     naming it — or its connection is closed, and the resume owns it. An identifier exclusion
//     is a restriction, never an owner, so it satisfies nothing here — the ONE exception is the
//     positive-store-answer recovery quarantine (consult 107), asserted separately: a recorded
//     quarantine of this exact generation, register and allocation with its reason, the
//     identifier reserved, and nothing acting for it — or, its second form, a register whose
//     persisted shape this broker refuses, held by the walk's unreadable-register reservation;
//   - every copy whose exchange has ended is reclaimed (CleanupComplete) or held by the
//     reclaimer (testRetainedMaterialOwned).

// testRetainedOwnershipViolations reports every retained register in [1, maxPI] of the rig's
// generation that breaks the property; closed is whether the connection that owed them is closed.
func testRetainedOwnershipViolations(r *testRetainedRig, closed bool, maxPI uint16) []string {
	var out []string
	for pi := uint16(1); pi <= maxPI; pi++ {
		subject := mqttInflightSubject(r.idHash, r.gen, pi)
		rec, smsg, err := r.asm.loadInflightRecord(subject)
		var shape *mqttInflightShapeError
		// A refused shape loads as a PRESENT message with no record (loadInflightRecord), never as
		// an error; either form is the exception's second form and is checked, never skipped.
		if errors.As(err, &shape) || (err == nil && smsg != nil && rec == nil) {
			// The exception's second form: a persisted shape this broker refuses (the compatibility
			// boundary), held by the walk's unreadable-register reservation — the identifier
			// reserved, nothing acting, allocation withheld.
			if v := testUnsupportedShapeViolation(r, pi); v != _EMPTY_ {
				out = append(out, fmt.Sprintf("pi %d: an unsupported register shape (%v): %s", pi, err, v))
			}
			continue
		}
		if err != nil {
			out = append(out, fmt.Sprintf("pi %d: the register is unreadable to the oracle: %v", pi, err))
			continue
		}
		if smsg == nil || rec == nil || !rec.retainedSource() {
			continue
		}
		owing := rec.State == mqttInflightPublish || rec.State == mqttInflightPubRelReady
		if !owing || closed {
			continue
		}
		if testRetainedRegisterOwned(r.sess, pi, rec) {
			continue
		}
		// THE ONE NAMED EXCEPTION, asserted on its own terms and never inferred from "unowned and
		// reserved": the positive-store-answer quarantine of THIS register's allocation.
		if q := testRetainedQuarantineViolation(r, pi, subject, rec); q != _EMPTY_ {
			out = append(out, fmt.Sprintf("pi %d: register %s (nonce %s, cancelled %v) stands with no owner: %s",
				pi, rec.State, rec.Nonce, rec.Cancelled, q))
		}
	}
	return out
}

// testRetainedRegisterOwned is the owner half of the property for one register.
func testRetainedRegisterOwned(sess *mqttSession, pi uint16, rec *mqttInflightRecord) bool {
	sess.mu.Lock()
	gen := sess.gen
	p, rel := sess.pendingPublish[pi], sess.pendingPubRel[pi]
	b := sess.abandonedAcks[mqttAbandonedAckIndex{gen: gen, pi: pi}]
	sess.mu.Unlock()
	if (p != nil && p.regNonce == rec.Nonce) || (rel != nil && rel.regNonce == rec.Nonce) ||
		(b != nil && b.nonce == rec.Nonce) {
		return true
	}
	d := &sess.retainedDriver
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, o := range d.owed {
		if o.pi == pi && o.nonce == rec.Nonce {
			return true
		}
	}
	for _, a := range d.abandons {
		if a.ev.PI == pi && a.ev.Nonce == rec.Nonce {
			return true
		}
	}
	return false
}

// testRetainedOwnershipHolds fails t with every violation, waiting up to d for a transient
// handoff (an owner being installed) to complete: quiescence, never a terminal-only check.
func testRetainedOwnershipHolds(t *testing.T, r *testRetainedRig, closed bool, maxPI uint16, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		v := testRetainedOwnershipViolations(r, closed, maxPI)
		if len(v) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for _, s := range v {
				t.Error(s)
			}
			t.FailNow()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// testRetainedMaterialOwned is the material half of the property for one copy whose exchange has
// ended: it is reclaimed, or the reclaimer holds its key.
func testRetainedMaterialOwned(r *testRetainedRig, inc string) bool {
	res, _, _ := r.asm.jsa.loadOutboundResource(r.id, r.epoch, r.gen, inc)
	if res != nil && res.State == mqttResourceCleanupComplete {
		return true
	}
	rc := &r.sess.retainedReclaim
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.pending[mqttRetainedReclaimKey{r.gen, inc}] != nil
}

// testRetainedQuarantineViolation reports why an unowned register is NOT a valid quarantine, or
// empty when it is: the resume recorded this exact subject and allocation with a named reason, the
// identifier is reserved, and nothing acts for the exchange — no acting entry, no continuation
// obligation, no abandonment decision.
func testRetainedQuarantineViolation(r *testRetainedRig, pi uint16, subject string, rec *mqttInflightRecord) string {
	sess := r.sess
	sess.mu.Lock()
	var q *mqttRetainedQuarantined
	for i := range sess.retainedQuarantined {
		if cur := &sess.retainedQuarantined[i]; cur.subject == subject && cur.nonce == rec.Nonce {
			q = cur
		}
	}
	p := sess.pendingPublish[pi]
	reserved := sess.sendReservedLocked(pi) || (sess.custody != nil && sess.custody.holdsPI(pi)) ||
		(p != nil && !p.acts())
	acting := (p != nil && p.acts()) || (sess.pendingPubRel[pi] != nil && sess.pendingPubRel[pi].acts())
	sess.mu.Unlock()
	switch {
	case q == nil:
		return "no quarantine names this allocation"
	case q.reason == _EMPTY_:
		return "its quarantine names no reason"
	case !reserved:
		return "its quarantine left the identifier unreserved"
	case acting:
		return "an entry acts for a quarantined exchange"
	}
	d := &sess.retainedDriver
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, o := range d.owed {
		if o.pi == pi && o.nonce == rec.Nonce {
			return "a continuation acts for a quarantined exchange"
		}
	}
	for _, a := range d.abandons {
		if a.ev.PI == pi && a.ev.Nonce == rec.Nonce {
			return "an abandonment decides a quarantined exchange"
		}
	}
	return _EMPTY_
}

// testUnsupportedShapeViolation reports why a register of a refused shape is not validly held, or
// empty when it is: its identifier is reserved, nothing acts for it, and allocation is withheld.
func testUnsupportedShapeViolation(r *testRetainedRig, pi uint16) string {
	sess := r.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	p := sess.pendingPublish[pi]
	rel := sess.pendingPubRel[pi]
	switch {
	case (p != nil && p.acts()) || (rel != nil && rel.acts()):
		return "an entry acts for it"
	case !(sess.sendReservedLocked(pi) || (p != nil && !p.acts()) || (sess.custody != nil && sess.custody.holdsPI(pi))):
		return "its identifier is not reserved"
	case !sess.piSpaceIncomplete:
		return "allocation is not withheld"
	}
	return _EMPTY_
}
