// Copyright 2024 The NATS Authors
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
	"encoding/json"
	"testing"
)

// The adopted-drain naming invariant (I8) and the machinery that keeps it: a durable a
// re-SUBSCRIBE adopted is named by EVERY record written while its sid holds a consumer and
// no membership key.
//
// The defect these cover is not stranding, it is loss, and it needs no crash: between the
// adoption and the packet's final update the record names the durable nowhere, and a sibling
// broker's orphan sweep deletes it — with its started deliveries, on an interest-retained
// stream — while the session is live here.

// testPersistedRecord decodes the stored session record, for assertions that must be about
// the RECORD rather than about the in-memory maps: the maps are what the loss leaves intact
// while the record is what a sibling sweep reads.
func testPersistedRecord(t *testing.T, sess *mqttSession) *mqttPersistedSession {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	smsg, err := sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil || smsg == nil {
		t.Fatalf("loading the persisted session record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decoding the persisted session record: %v", err)
	}
	return ps
}

// testRecordNamesDurable reports where the stored record references a durable: in Cons, in
// Drains, or nowhere. "Nowhere" is exactly the state the orphan sweep deletes.
func testRecordNamesDurable(t *testing.T, sess *mqttSession, durable string) (inCons, inDrains bool) {
	t.Helper()
	ps := testPersistedRecord(t, sess)
	for _, cc := range ps.Cons {
		if cc != nil && cc.Durable == durable {
			inCons = true
		}
	}
	for _, pd := range ps.Drains {
		if pd != nil && pd.Cons != nil && pd.Cons.Durable == durable {
			inDrains = true
		}
	}
	return inCons, inDrains
}

// testAdoptedDrain drives a real adoption: a drain with a started delivery, then
// adoptDrainLocked, which is the production entry point every re-SUBSCRIBE of a QoS > 0
// filter reaches.
func testAdoptedDrain(t *testing.T, sess *mqttSession, sid, durable, filter string) *mqttDrain {
	t.Helper()
	d := addTestDrain(sess, sid, durable, filter, 1, map[uint64]*drainSeq{
		41: {claimed: true, pi: 7},
	})
	sess.mu.Lock()
	adopted := sess.adoptDrainLocked(sid)
	held := sess.adoptedDrains[sid]
	inCons := sess.cons[sid] == d.cc
	sess.mu.Unlock()
	if !adopted {
		t.Fatal("the fixture must reach a real adoption, or nothing below is about adoption")
	}
	if held != d || !inCons {
		t.Fatalf("adoption must move the durable into cons and RETAIN the drain: held=%v inCons=%v", held, inCons)
	}
	return d
}

// TestAdoptedDrainIsNamedWhileItsMembershipIsAbsent is the naming invariant itself, and it
// is the assertion the sibling sweep's contract turns on: the sweep deletes any consumer no
// record references, so a record naming the durable in NEITHER field is the loss.
func TestAdoptedDrainIsNamedWhileItsMembershipIsAbsent(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-named")
	const sid, dur = "foo.bar", "dur-adopted-named"

	testAdoptedDrain(t, sess, sid, dur, "foo.bar")

	// No membership yet: this is the window every mid-packet write lands in.
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("save inside the adoption window: %v", err)
	}
	inCons, inDrains := testRecordNamesDurable(t, sess, dur)
	if inCons {
		t.Fatal("with no membership key the consumer is not persisted in Cons")
	}
	if !inDrains {
		t.Fatal("a record written inside the adoption window must name the durable in Drains: " +
			"naming it nowhere is what lets a sibling broker's sweep delete it, with its " +
			"started deliveries, while this session is live")
	}
}

// TestAdoptedDrainMovesToConsOnceMembershipLands is the settlement half. Once the packet's
// record is confirmed the durable belongs in Cons, and the retention has to go — a retention
// that outlived its settlement would keep naming a durable in Drains that the record already
// names in Cons.
func TestAdoptedDrainMovesToConsOnceMembershipLands(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-settled")
	const sid, dur = "foo.bar", "dur-adopted-settled"

	testAdoptedDrain(t, sess, sid, dur, "foo.bar")

	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	sess.subs[sid] = 1 // the membership the confirmed final update installs
	sess.settleAdoptedRetentionsLocked()
	retained := len(sess.adoptedDrains)
	sess.mu.Unlock()

	if retained != 0 {
		t.Fatalf("a settled adoption releases its retention, got %d still held", retained)
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("save after settlement: %v", err)
	}
	inCons, inDrains := testRecordNamesDurable(t, sess, dur)
	if !inCons || inDrains {
		t.Fatalf("a settled adoption is named in Cons ONLY: inCons=%v inDrains=%v", inCons, inDrains)
	}
}

// TestRetentionIsReleasedOnACommittedDeletion is the settlement branch that is NOT about
// membership.
//
// A retention has to outlive every tentative shape its sid passes through — staging removes
// cons[sid] for a while, a mid-packet write proves nothing — but once a confirmed outcome has
// committed the consumer's deletion or replacement, the retention describes a consumer this
// session no longer holds. Keeping it there would go on naming a durable in Drains that
// nothing owns, so the record would claim a reference the broker cannot honour.
//
// Asserted on the map directly, because a leak here is invisible in the record: the entry
// simply keeps being written.
func TestRetentionIsReleasedOnACommittedDeletion(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-committed-deletion")
	const sid, dur = "foo.bar", "dur-adopted-deleted"

	testAdoptedDrain(t, sess, sid, dur, "foo.bar")

	// Still held while the sid's disposition is unsettled: no membership, and the consumer
	// entry still the one the retention describes.
	sess.mu.Lock()
	sess.settleAdoptedRetentionsLocked()
	heldWhileUnsettled := sess.adoptedDrains[sid] != nil
	// The committed teardown: the consumer entry is gone from the map, and this outcome is
	// confirmed.
	delete(sess.cons, sid)
	sess.settleAdoptedRetentionsLocked()
	afterDeletion := sess.adoptedDrains[sid] != nil
	sess.mu.Unlock()

	if !heldWhileUnsettled {
		t.Fatal("an unsettled sid keeps its retention, or a definite failure restores the " +
			"consumer with nothing left to name its durable")
	}
	if afterDeletion {
		t.Fatal("a COMMITTED deletion settles the retention: leaving it would keep naming a " +
			"durable in Drains that this session no longer holds under that sid")
	}
}

// TestFailedFilterLeavesTheAdoptedDrainNamed is the bullet's own case: the filter is answered
// 0x80 after the adoption stands, so the membership key never arrives. The record must keep
// naming the durable permanently, not just for the packet.
func TestFailedFilterLeavesTheAdoptedDrainNamed(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-failed-filter")
	const sid, dur = "foo.bar", "dur-adopted-failed"

	testAdoptedDrain(t, sess, sid, dur, "foo.bar")

	// The filter failed, so no membership is ever installed. Settlement runs at the
	// packet's confirmed outcome and must NOT release a retention nothing accounted for.
	sess.mu.Lock()
	sess.settleAdoptedRetentionsLocked()
	stillHeld := sess.adoptedDrains[sid] != nil
	sess.mu.Unlock()
	if !stillHeld {
		t.Fatal("a failed filter accounts for nothing, so its retention must survive settlement")
	}
	// A later, unrelated save must still name it.
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("a later save: %v", err)
	}
	if _, inDrains := testRecordNamesDurable(t, sess, dur); !inDrains {
		t.Fatal("after a failed filter the durable stays named in Drains for every later record")
	}
}

// TestQoS0MembershipKeepsItsConsEntry is the exclusion, and it is why the naming rule reads
// CURRENT STATE rather than what the packet did: a sid that already carries a QoS 0
// membership keeps its subs key, so its cons entry is retained and the durable is referenced
// without any retained-drain naming at all.
func TestQoS0MembershipKeepsItsConsEntry(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-qos0-key")
	const sid, dur = "foo.bar", "dur-adopted-qos0"

	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	sess.subs[sid] = 0 // the QoS 0 membership that survives the adoption
	sess.mu.Unlock()

	testAdoptedDrain(t, sess, sid, dur, "foo.bar")

	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("save: %v", err)
	}
	inCons, inDrains := testRecordNamesDurable(t, sess, dur)
	if !inCons {
		t.Fatal("a surviving membership key keeps the cons entry, so the durable is named in Cons")
	}
	if inDrains {
		t.Fatal("no Drains entry is owed where the cons entry already names the durable: " +
			"two references to one fact is one too many")
	}
}

// TestMarkedDrainRefusesItsFilterThroughTheSharedHold is the ambiguous-removal window.
//
// While a reference-removal write is unresolved the omission MAY be durable, so the durable
// must not be reused: an adoption there would leave a live consumer that no record names,
// which is precisely what the sibling sweep acts on. The refusal goes through the SHARED
// hold, so it produces the same filter-level refusal every other hold does.
//
// The property asserted is that the old durable is never REUSED — deliberately not that the
// record still names it, because after an ambiguous omission it may already not.
func TestMarkedDrainRefusesItsFilterThroughTheSharedHold(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-marked-hold")
	const sid, dur = "foo.bar", "dur-marked-hold"

	d := addTestDrain(sess, sid, dur, "foo.bar", 1, map[uint64]*drainSeq{})

	sess.mu.Lock()
	before := sess.drainSidHoldLocked(sid)
	d.removalMarked = true
	after := sess.drainSidHoldLocked(sid)
	adopted := sess.adoptDrainLocked(sid)
	reused := sess.cons[sid] != nil
	sess.mu.Unlock()

	if before != mqttDrainNotHeld {
		t.Fatalf("fixture: an unmarked empty drain is not held, got %v", before)
	}
	if after != mqttDrainHeldRemovalOutstanding {
		t.Fatalf("a marked drain must be held through the SHARED hold, got %v", after)
	}
	if adopted || reused {
		t.Fatal("a drain with an outstanding reference removal must never have its durable " +
			"reused: its omission may already be durable, and the consumer would then be " +
			"live with no record naming it")
	}

	// A definite failure clears the mark, and the filter is servable again.
	sess.mu.Lock()
	sess.clearRemovalMarkLocked(d)
	cleared := sess.drainSidHoldLocked(sid)
	sess.mu.Unlock()
	if cleared != mqttDrainNotHeld {
		t.Fatalf("clearing the mark must release the hold, got %v", cleared)
	}
}

// TestEligibilityRefusesWhileAnUnresolvedReservationNamesTheDurable is the cross-broker half
// of the eligibility gate.
//
// The known claim set emptying is not the same as nothing being owed. An unresolved
// reservation can still name this durable, and once a save has omitted the entry a sibling
// sweep deletes the consumer that obligation belongs to — which no local decision to
// postpone retirement reaches. So the mark must be refused, and the save must RETAIN the
// empty reference.
func TestEligibilityRefusesWhileAnUnresolvedReservationNamesTheDurable(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-eligibility")
	const dur = "dur-eligibility"

	d := addTestDrain(sess, "foo.bar", dur, "foo.bar", 1, map[uint64]*drainSeq{})

	sess.mu.Lock()
	// A completed apply, so uncertainty is attributed per source rather than session-wide.
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	// An unresolved reservation that KEEPS its source identity, which is the shape an
	// opaque recovered register is installed with: no cpending mapping, jsDur and sseq
	// intact.
	sess.pendingPublish = map[uint16]*mqttPending{
		9: {jsDur: dur, sseq: 55, markerOpaque: true},
	}
	restricted := sess.durableRestrictedLocked(dur)
	marked := sess.markRemovalIfEligibleLocked(d, nil)
	sess.mu.Unlock()

	if !restricted {
		t.Fatal("an unresolved reservation naming this durable restricts it")
	}
	if marked {
		t.Fatal("eligibility must refuse while the durable is restricted: marking here omits " +
			"the reference, and a sibling sweep then deletes a durable that still owes an exchange")
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, inDrains := testRecordNamesDurable(t, sess, dur); !inDrains {
		t.Fatal("an ineligible drain keeps its reference: the save clears claims, it does not un-name")
	}

	// With the reservation resolved, the same drain becomes eligible. Without this arm the
	// test passes for a gate that refuses everything.
	sess.mu.Lock()
	sess.pendingPublish = nil
	nowMarked := sess.markRemovalIfEligibleLocked(d, nil)
	sess.mu.Unlock()
	if !nowMarked {
		t.Fatal("with nothing restricting the durable the mark must be granted, or the gate " +
			"is refusing unconditionally and proves nothing")
	}
}

// TestEligibilityTreatsThisAttemptsOwnRemovalAsLanded is the clause that makes the pre-save
// mark reachable at all.
//
// The owning transition sets unclaimUnconfirmed on the claim it is removing BEFORE it
// classifies its own save — the guard has to go on with the mutation, or a delivery arriving
// during the save reads a removal whose outcome is unknown as durable. HEAD's retirable()
// rejects exactly that state, so asking it here always answers "not retirable": the gate
// would never open and no ordinary drain would ever have its reference omitted.
//
// So this attempt's own removals are discounted, and NOTHING else is: another claim, or
// another sequence whose removal is still unconfirmed, still refuses.
func TestEligibilityTreatsThisAttemptsOwnRemovalAsLanded(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-own-removal")
	const dur = "dur-own-removal"

	d := addTestDrain(sess, "foo.bar", dur, "foo.bar", 1, map[uint64]*drainSeq{
		41: {claimed: true, pi: 7},
	})

	sess.mu.Lock()
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	// Exactly the state finalizeDrainDelivery is in when it decides: claim released, guard
	// on, outcome not yet known.
	d.seqs[41].claimed = false
	d.seqs[41].unclaimUnconfirmed = true
	plainPredicate := d.retirable()
	marked := sess.markRemovalIfEligibleLocked(d, map[uint64]bool{41: true})
	sess.mu.Unlock()

	if plainPredicate {
		t.Fatal("fixture: HEAD's predicate must REJECT this state, or the discounting below " +
			"is untested")
	}
	if !marked {
		t.Fatal("the gate must discount THIS attempt's own removal: asking the plain " +
			"predicate never opens it, so an ordinary drain's reference is never omitted and " +
			"its durable is never deleted")
	}

	// A second sequence whose removal this attempt does NOT own still refuses, so the
	// discount is scoped rather than a blanket bypass.
	sess.mu.Lock()
	d.removalMarked = false
	d.seqs[42] = &drainSeq{claimed: false, unclaimUnconfirmed: true, pi: 8}
	scoped := sess.markRemovalIfEligibleLocked(d, map[uint64]bool{41: true})
	sess.mu.Unlock()
	if scoped {
		t.Fatal("only this attempt's removals are discounted: another unconfirmed removal " +
			"still refuses, or the discount has become a bypass of the guard itself")
	}
}

// TestFailedRemovalClearsTheMarkBeforeReleasingTheFence is a holistic-review finding, and it
// is about ORDERING rather than about the eventual state.
//
// On a definite failure the reference stands, so the mark has to come off — but clearing it
// AFTER the transaction is released leaves a window in which another writer acquires the
// fence and persists the very omission this attempt just failed to make, and the
// transaction's own verdict handoff can write in it too. The mark would then be cleared over
// a record that no longer names the durable, adoption would be permitted, and a sibling
// sweep could delete the reused consumer.
//
// The release seam is what makes the ordering observable: by the time the fence is being
// released, the mark must already be gone.
func TestFailedRemovalClearsTheMarkBeforeReleasingTheFence(t *testing.T) {
	// NOT t.Parallel(): this installs PROCESS-GLOBAL hooks (the store-result rewrite and
	// the transaction-release seam), so running beside anything else would let it corrupt
	// or be corrupted by that test. TestParallelTestsTouchNoProcessGlobalState enforces it.
	_, c, sess, _ := drainTestSession(t, "adopted-removal-ordering")
	const dur = "dur-removal-ordering"

	d := addTestDrain(sess, "foo.bar", dur, "foo.bar", 1, map[uint64]*drainSeq{})
	sess.mu.Lock()
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	sess.mu.Unlock()

	// A DEFINITE failure, and it has to be a REJECTED CAS specifically: only that proves
	// the server answered and nothing was stored, so the session stays resolved. A bare
	// error is an ambiguous write, where the mark must instead be KEPT — the opposite arm.
	restoreStore := setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		return resp, ApiErrors[JSStreamWrongLastSequenceErrF]
	})
	defer restoreStore()

	var markedAtRelease, sawRelease bool
	restoreSeam := setMQTTTestRecordTxPostReleaseHook(func(hs *mqttSession) {
		if hs != sess {
			return
		}
		sawRelease = true
		hs.mu.Lock()
		markedAtRelease = d.removalMarked
		hs.mu.Unlock()
	})
	defer restoreSeam()

	removed := sess.removeEmptyDrainReference(c, d)
	restoreSeam()
	restoreStore()

	if removed {
		t.Fatal("a refused write removes nothing")
	}
	if sess.isSeqUncertain() {
		t.Fatal("fixture: this must be a DEFINITE failure — an ambiguous one keeps the mark, " +
			"so the ordering asserted below would be the wrong property")
	}
	if !sawRelease {
		t.Fatal("the release seam never fired, so this test never reached its schedule")
	}
	if markedAtRelease {
		t.Fatal("the mark must be cleared BEFORE the fence is released: while it is set the " +
			"builder omits the reference, so another writer taking the fence in that window " +
			"persists an omission this attempt failed to make")
	}
	sess.mu.Lock()
	stillMarked := d.removalMarked
	hold := sess.drainSidHoldLocked("foo.bar")
	sess.mu.Unlock()
	if stillMarked {
		t.Fatal("a definite failure leaves the drain fully live, mark and all")
	}
	if hold != mqttDrainNotHeld {
		t.Fatalf("and servable again, got hold %v", hold)
	}
	// Deliberately no later save asserted here: a rejected CAS arms this session's
	// rejection handling, so a further write is legitimately refused. That the RECORD keeps
	// naming an unmarked drain is TestPersistedRecordOmitsOnlyAMarkedDrain's property; what
	// this test owns is the ordering, and asserting both here would only make the fixture
	// fight the rejection machinery.
}

// TestUnconfirmedOutcomesNeverReleaseARetention is the property three review rounds moved
// around and one of my own fixes broke: a retention may be released ONLY by a confirmed
// outcome.
//
// Settling earlier looks harmless and is message loss. If the packet's save then definitely
// fails, the rollback restores the consumer maps but nothing restores the retention — so
// the next record omits the durable, and a sibling broker's sweep deletes it with its
// outstanding deliveries. The helper is therefore only ever reached from a proven record,
// and this test pins the two shapes that must survive an unsettled one.
func TestUnconfirmedOutcomesNeverReleaseARetention(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-unconfirmed")
	const sid, dur = "foo.bar", "dur-unconfirmed"

	d := testAdoptedDrain(t, sess, sid, dur, "foo.bar")

	// Staging legitimately removes cons[sid] for a while. That is a settlement CANDIDATE,
	// not a settlement: if the packet then fails, rollbackStagedDrains puts the consumer
	// back and the retention has to still be there to name the durable.
	sess.mu.Lock()
	delete(sess.cons, sid)
	// The adoption nils sess.drains when it empties it, so staging re-creates the map.
	if sess.drains == nil {
		sess.drains = make(map[string]*mqttDrain, 1)
	}
	sess.drains[sid] = d
	heldMidStaging := sess.adoptedDrains[sid] != nil
	// Put the pre-packet shape back, as a definite failure's rollback does.
	sess.drains = nil
	sess.cons = map[string]*ConsumerConfig{sid: d.cc}
	heldAfterRollback := sess.adoptedDrains[sid] != nil
	sess.mu.Unlock()

	if !heldMidStaging {
		t.Fatal("a sid whose consumer entry is only temporarily gone is a settlement " +
			"CANDIDATE: releasing there loses the naming a rolled-back packet still needs")
	}
	if !heldAfterRollback {
		t.Fatal("and it must survive the rollback itself, or the next save omits the durable " +
			"and a sibling sweep deletes its outstanding deliveries")
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("save after the rollback: %v", err)
	}
	if _, inDrains := testRecordNamesDurable(t, sess, dur); !inDrains {
		t.Fatal("the rolled-back view still names the durable")
	}
}

// TestUnrelatedOpacityRestrictsOnlyItsOwnDurable is the direction that stops the restriction
// becoming a session-wide latch.
//
// One durable's unresolved reservation says nothing about another durable, and nothing about
// a different sequence on its own durable. Treating it as if it did withheld unrelated
// messages and blocked unrelated retirement, which is progress lost for no safety gained.
func TestUnrelatedOpacityRestrictsOnlyItsOwnDurable(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-attribution")
	const durA, durB = "dur-attr-a", "dur-attr-b"

	sess.mu.Lock()
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	sess.pendingPublish = map[uint16]*mqttPending{
		9: {jsDur: durA, sseq: 55, markerOpaque: true},
	}
	aSameSeq := sess.sourceRestrictedLocked(durA, 55)
	aOtherSeq := sess.sourceRestrictedLocked(durA, 56)
	bRestricted := sess.durableRestrictedLocked(durB)
	aRestricted := sess.durableRestrictedLocked(durA)
	sess.mu.Unlock()

	if !aSameSeq {
		t.Fatal("the reservation's OWN source is restricted: the floor could not enrol it")
	}
	if !aRestricted {
		t.Fatal("retirement aggregates by durable, so A is restricted")
	}
	if aOtherSeq {
		t.Fatal("a different sequence on the same durable is not the one the floor missed")
	}
	if bRestricted {
		t.Fatal("an unrelated durable must not be restricted: one durable's opacity is not " +
			"session-wide doubt, and treating it so withholds unrelated messages and blocks " +
			"unrelated retirement")
	}
}

// TestFailedReconstructionRestrictsTheWholeSession is the opposing arm, and the reason the
// per-source rule needs recovery-outcome provenance at all.
//
// After an attempt that never applied, an unknown population may have been missed entirely —
// so a reservation that LOOKS fully attributable proves nothing about the rest, and the
// restriction has to be session-wide. piSpaceIncomplete alone cannot express this: it is set
// in both histories.
func TestFailedReconstructionRestrictsTheWholeSession(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-provenance")
	const durA, durB = "dur-prov-a", "dur-prov-b"

	sess.mu.Lock()
	// A completed walk left an attributable reservation for A...
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	sess.pendingPublish = map[uint16]*mqttPending{
		9: {jsDur: durA, sseq: 55, markerOpaque: true},
	}
	bBefore := sess.durableRestrictedLocked(durB)
	// ...then a further attempt failed before its apply phase, so the completed-walk
	// number falls behind.
	sess.inflightWalks = 2
	bAfter := sess.durableRestrictedLocked(durB)
	complete := sess.applyProvenanceCompleteLocked()
	sess.mu.Unlock()

	if bBefore {
		t.Fatal("after a COMPLETED apply, an unrelated durable is unrestricted")
	}
	if complete {
		t.Fatal("fixture: the latest attempt must look un-applied, or the arm below is vacuous")
	}
	if !bAfter {
		t.Fatal("after an attempt that never applied, the restriction is session-wide: an " +
			"unknown population may have been missed, so an apparently attributable " +
			"reservation says nothing about anything else")
	}
}

// TestIncarnationRebasesTheApplyProvenance is why that provenance is incarnation-scoped.
//
// clear() preserves the walk counter while discarding the pending maps and re-minting the
// generation, and the fresh session does not run recovery at all — Session Present is false.
// Without a rebase a failed predecessor's mismatch would impose its session-wide restriction
// on that new incarnation, and a NEW attributable reservation, which sets piSpaceIncomplete
// without advancing either counter, would be misread as that same failure.
func TestIncarnationRebasesTheApplyProvenance(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "adopted-incarnation")
	const dur = "dur-incarnation"

	sess.mu.Lock()
	// (completed, walks) = (1, 2): a completed walk, then one that failed before applying.
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 2
	staleRestricted := sess.durableRestrictedLocked(dur)
	sess.mu.Unlock()

	// The re-mint, through the production door: setIncarnationLocked is the single home for
	// every generation-scoped fact and is what clear() calls. The permit is what property P
	// requires, so the tracker is closed and drained first, exactly as clear() leaves it.
	sess.leases.close()
	sess.mu.Lock()
	permit, perr := sess.retireIncarnationLocked()
	if perr != nil {
		sess.mu.Unlock()
		t.Fatalf("fixture: the retirement permit must be grantable, got %v", perr)
	}
	sess.setIncarnationLocked(mqttNewSessGen(), permit)
	rebasedWalks := sess.inflightWalks
	freshRestricted := sess.durableRestrictedLocked(dur)
	sess.mu.Unlock()

	if !staleRestricted {
		t.Fatal("fixture: the predecessor's failed attempt must restrict, or the rebase below proves nothing")
	}
	if rebasedWalks != 2 {
		t.Fatalf("the walk counter is NOT reset — the opaque reservations' own stamp reads it — got %d", rebasedWalks)
	}
	if freshRestricted {
		t.Fatal("a fresh incarnation starts unrestricted: it never ran recovery, so a " +
			"predecessor's failure is not its uncertainty")
	}
}
