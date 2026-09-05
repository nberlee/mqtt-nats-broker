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
	"slices"
	"testing"
)

// F-094 at fork repair's preflight, which is the SECOND caller of adoptDrainLocked.
//
// THE TWO HOLDS REFUSE DIFFERENT ROWS, and these three tests pin the split in both
// directions, because getting it wrong in either direction is a real defect that a plan
// review round actually caught:
//
//   - refuse too little and the `absent` row drops a survivor drain that still owes a
//     PUBREL, erasing the same intent adoption would have;
//   - refuse too much and an applicable no-op repair is abandoned, so a fork stays
//     unrepaired for a drain nothing was going to touch.
//
// The `absent` row is NOT exempt, and the reasoning that it is — "our record's omission IS
// the acknowledged completion of the last claim" — is refuted by this file's own terms.
// mqttSurvivorDecidedGrant compares drain PRESENCE via shape(), never the sequences or
// their intents; sess_fork_repair.go's header disclaims that the survivor descended from our
// baseline; and our omission can equally come from recoverDrains pruning a source it proved
// absent. So the omission proves nothing about the survivor's obligations.

// heldSurvivorDrainSession builds a session holding one drain under filter, carrying a
// claimed past-PUBREC intent, as a restore from the SURVIVOR's record leaves it.
func heldSurvivorDrainSession(t *testing.T, filter, dur string) (*mqttSession, *mqttDrain) {
	t.Helper()
	d := mqttDrainFromPersisted(testPersistedDrain(dur,
		mqttPersistedDrainSeq{Seq: 21, PubRel: true, PI: 4,
			OwnerNonce: "xxxxxxxxxxxxxxxxxxxxxx"}))
	if d == nil {
		t.Fatal("the drain must restore")
	}
	sess := &mqttSession{
		drains:     map[string]*mqttDrain{filter: d},
		drainByDur: map[string]*mqttDrain{dur: d},
		subs:       map[string]byte{},
	}
	sess.mu.Lock()
	held := sess.drainSidHoldLocked(filter)
	sess.mu.Unlock()
	if held != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("the fixture must be held by its INTENT, not by an unusable identity: %v", held)
	}
	return sess, d
}

// TestForkRepairRefusesAdoptionOfAHeldSid is the `draining -> live` row: our branch
// re-SUBSCRIBEd, so applying the change would adopt the survivor's drain and erase its
// intent.
func TestForkRepairRefusesAdoptionOfAHeldSid(t *testing.T) {
	t.Parallel()
	const filter, dur = "f/live", "held-live-dur"
	sess, d := heldSurvivorDrainSession(t, filter, dur)

	ours := &mqttGrantEntry{
		mqttGrantTerms: mqttGrantTerms{present: true, qos: 1},
		sids:           map[string]mqttSidState{filter: {con: testConsumerConfig(dur)}},
	}
	survivor := &mqttGrantEntry{
		sids: map[string]mqttSidState{filter: {drain: testPersistedDrain(dur)}},
	}

	sess.mu.Lock()
	applied := sess.applyGrantChangeLocked(mqttGrantChange{filter: filter, ours: ours}, survivor)
	stillDraining := sess.drains[filter]
	_, live := sess.cons[filter]
	_, inSubs := sess.subs[filter]
	sess.mu.Unlock()

	if applied {
		t.Error("the adopting row must be REFUSED for a sid held by a past-PUBREC intent")
	}
	if stillDraining != d {
		t.Errorf("the refused repair adopted the drain away: %v", stillDraining)
	}
	if live {
		t.Error("a refused repair must install no consumer")
	}
	if inSubs {
		t.Error("a refused repair must not leave the filter in the membership map: repair " +
			"mutates membership FIRST, so the accept save would persist a live filter whose " +
			"drain obligations it no longer carries")
	}
}

// TestForkRepairRefusesDroppingAHeldSid is the `draining -> absent` row, and the case a
// plan revision got wrong by treating our omission as proof of completion.
func TestForkRepairRefusesDroppingAHeldSid(t *testing.T) {
	t.Parallel()
	const filter, dur = "f/absent", "held-absent-dur"
	sess, d := heldSurvivorDrainSession(t, filter, dur)

	// Our record says the filter is gone entirely — the acknowledged-UNSUBSCRIBE shape.
	ours := &mqttGrantEntry{}
	survivor := &mqttGrantEntry{
		sids: map[string]mqttSidState{filter: {drain: testPersistedDrain(dur)}},
	}
	if ours.sidState(filter).kind() != mqttSidAbsent {
		t.Fatal("the fixture must exercise the ABSENT row")
	}

	sess.mu.Lock()
	applied := sess.applyGrantChangeLocked(mqttGrantChange{filter: filter, ours: ours}, survivor)
	stillDraining := sess.drains[filter]
	sess.mu.Unlock()

	if applied {
		t.Error("the dropping row must be REFUSED for a sid held by a past-PUBREC intent: " +
			"dropDrainLocked erases the intent exactly as adoption would")
	}
	if stillDraining != d {
		t.Errorf("the refused repair dropped the survivor's drain: %v", stillDraining)
	}
	if st := d.seqs[21]; st == nil || !st.intent || !st.claimed {
		t.Errorf("the survivor's intent must be untouched, got %+v", st)
	}
}

// TestForkRepairAppliesAnEqualDrainingRowOverAHeldSid is the other direction, and it is
// what stops the preflight being widened back to an unconditional refusal.
//
// The `draining -> draining, equal` row preserves the survivor's drain either way, so a
// hold is no reason to abandon the repair. It must SUCCEED — a differing-drains case would
// return false under both the correct implementation and the blanket-refusal mutation, and
// so could not tell them apart.
func TestForkRepairAppliesAnEqualDrainingRowOverAHeldSid(t *testing.T) {
	t.Parallel()
	const filter, dur = "f/equal", "held-equal-dur"
	sess, d := heldSurvivorDrainSession(t, filter, dur)

	// Identical persisted drains on both sides: the no-op row.
	pd := testPersistedDrain(dur)
	ours := &mqttGrantEntry{sids: map[string]mqttSidState{filter: {drain: pd}}}
	survivor := &mqttGrantEntry{sids: map[string]mqttSidState{filter: {drain: pd}}}
	if !mqttPersistedDrainsEqual(survivor.sidState(filter).drain, ours.sidState(filter).drain) {
		t.Fatal("the fixture must exercise the EQUAL row, or it proves nothing")
	}

	sess.mu.Lock()
	applied := sess.applyGrantChangeLocked(mqttGrantChange{filter: filter, ours: ours}, survivor)
	stillDraining := sess.drains[filter]
	sess.mu.Unlock()

	if !applied {
		t.Error("a row that preserves the drain must still APPLY over a held sid: refusing " +
			"every row abandons repairs for a drain nothing was going to touch")
	}
	if stillDraining != d {
		t.Errorf("the equal row must leave the drain exactly where it was: %v", stillDraining)
	}
}

// TestForkRepairStillRefusesEveryRowForAnUnusableIdentity keeps the OTHER hold
// unconditional. Nothing may act on an unattributable intent and nothing may take its
// place, so even the dropping row is refused — and the row split must not have relaxed
// that by accident.
func TestForkRepairStillRefusesEveryRowForAnUnusableIdentity(t *testing.T) {
	t.Parallel()
	const filter, dur = "f/unusable", "unusable-dur"
	d := mqttDrainFromPersisted(testPersistedDrain(dur,
		// Past PUBREC with NO identifier: quarantined on restore.
		mqttPersistedDrainSeq{Seq: 22, PubRel: true, PI: 0,
			OwnerNonce: "xxxxxxxxxxxxxxxxxxxxxx"}))
	if d == nil {
		t.Fatal("the drain must restore")
	}
	sess := &mqttSession{
		drains:     map[string]*mqttDrain{filter: d},
		drainByDur: map[string]*mqttDrain{dur: d},
		subs:       map[string]byte{},
	}
	sess.mu.Lock()
	held := sess.drainSidHoldLocked(filter)
	sess.mu.Unlock()
	if held != mqttDrainHeldUnusableIdentity {
		t.Fatalf("the fixture must be held by an UNUSABLE IDENTITY, got %v", held)
	}

	// The equal row, which a past-PUBREC intent would allow through.
	pd := testPersistedDrain(dur)
	ours := &mqttGrantEntry{sids: map[string]mqttSidState{filter: {drain: pd}}}
	survivor := &mqttGrantEntry{sids: map[string]mqttSidState{filter: {drain: pd}}}

	sess.mu.Lock()
	applied := sess.applyGrantChangeLocked(mqttGrantChange{filter: filter, ours: ours}, survivor)
	sess.mu.Unlock()

	if applied {
		t.Error("an unusable identity must refuse EVERY row, including ones that preserve " +
			"the drain: nothing may act on it and nothing may take its place")
	}
}

// TestForkRepairRefusedFilterIsCountedAsAConflictAndTheMergeCompletes pins the RESIDUAL
// rather than a hoped-for convergence, and it exists because an earlier plan revision
// claimed the opposite: that "repair is a convergence step, so the next resume tries again".
//
// That is false, and the reason is this control flow: mergeForkedGrantLocked appends a
// refused change to tally.conflicts and CONTINUES. The merge therefore completes and is
// applied by the client's own accept save, after which the capsule's anchor no longer
// authorizes another merge — so the refused filter is never re-attempted through it, and the
// drain completing later restores nothing.
//
// Asserting the tally is what makes this evidence rather than a restatement: a merge that
// ABORTED on the refusal would leave the capsule unconsumed and could legitimately be
// retried, and that is the shape this test rules out. The second filter is there so
// "completed" is observable — a merge that stopped at the refusal would never apply it.
func TestForkRepairRefusedFilterIsCountedAsAConflictAndTheMergeCompletes(t *testing.T) {
	t.Parallel()
	const held, dur = "f/held", "notretried-dur"
	const other = "f/other"
	sess, _ := heldSurvivorDrainSession(t, held, dur)

	survivorSnap := &mqttGrantSnap{filters: map[string]mqttGrantEntry{
		held:  {sids: map[string]mqttSidState{held: {drain: testPersistedDrain(dur)}}},
		other: {},
	}}

	// EVERY change carries a BASE equal to the survivor's own entry, and that is
	// load-bearing rather than fixture noise. mqttSurvivorDecidedGrant counts a filter as a
	// conflict whenever the survivor's grant differs from the base, BEFORE
	// applyGrantChangeLocked is ever called — so a nil base makes the held filter a conflict
	// for a reason that has nothing to do with the hold, and the test then passes against a
	// build whose preflight refuses nothing at all. A mutation caught exactly that.
	heldBase := &mqttGrantEntry{sids: map[string]mqttSidState{held: {drain: testPersistedDrain(dur)}}}
	otherBase := &mqttGrantEntry{}
	g := &mqttForkedGrant{changes: []mqttGrantChange{
		// Refused: the adopting row over a held sid, with the survivor undecided about it.
		{filter: held, base: heldBase, ours: &mqttGrantEntry{
			mqttGrantTerms: mqttGrantTerms{present: true, qos: 1},
			sids:           map[string]mqttSidState{held: {con: testConsumerConfig(dur)}},
		}},
		// Independently applicable, and nothing about it touches a drain.
		{filter: other, base: otherBase, ours: &mqttGrantEntry{
			mqttGrantTerms: mqttGrantTerms{present: true, qos: 1},
			sids:           map[string]mqttSidState{other: {con: testConsumerConfig("other-dur")}},
		}},
	}}

	// The premise, asserted rather than assumed: neither filter may be counted a conflict by
	// the survivor-decided check, or the tally below says nothing about the hold.
	heldSurvivor := survivorSnap.filters[held]
	otherSurvivor := survivorSnap.filters[other]
	if mqttSurvivorDecidedGrant(&heldSurvivor, heldBase) {
		t.Fatal("the survivor must be UNDECIDED about the held filter, or its conflict count " +
			"comes from the grant comparison instead of from the refusal under test")
	}
	if mqttSurvivorDecidedGrant(&otherSurvivor, otherBase) {
		t.Fatal("the survivor must be UNDECIDED about the other filter, or it can never apply")
	}

	sess.mu.Lock()
	tally := sess.mergeForkedGrantLocked(g, survivorSnap)
	_, heldInSubs := sess.subs[held]
	_, otherInSubs := sess.subs[other]
	sess.mu.Unlock()

	if !slices.Contains(tally.conflicts, held) {
		t.Errorf("the refused filter must be COUNTED as a conflict, got conflicts=%v", tally.conflicts)
	}
	if !slices.Contains(tally.applied, other) {
		t.Errorf("the merge must COMPLETE and apply the other filter — a merge that aborted "+
			"on the refusal would leave the capsule unconsumed and retryable, which is "+
			"precisely the convergence this residual says does not exist. applied=%v",
			tally.applied)
	}
	if heldInSubs {
		t.Error("the refused filter must not reach the membership map")
	}
	if !otherInSubs {
		t.Error("the applicable filter must reach the membership map")
	}
}
