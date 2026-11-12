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

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// testRetirableLifecycle is testTransition's authority in the shape lifecycle retirement takes:
// closed, its last generation retired, nothing pending.
func testRetirableLifecycle() *mqttOutboundAuthority {
	a := testTransition().Prev
	a.State = mqttAuthorityClosed
	a.Life.Phase, a.Life.Pending, a.Life.Outstanding, a.Life.Anchors = mqttLifecycleClosed, nil, nil, nil
	return a
}

// T-anchor (issue #133): a lifecycle holding a Will anchor, ARMED or OWED, is not retirable, so
// a delayed Will's claimant always finds its anchor to decide from. Mutation M4: the anchors
// ignored by the retirement predicate.
func TestALifecycleHoldingAWillAnchorIsNotRetired(t *testing.T) {
	if !mqttLifecycleRetirable(testRetirableLifecycle()) {
		t.Fatal("fixture: the bare closed lifecycle is not retirable")
	}
	for _, st := range []mqttAnchorState{mqttAnchorArmed, mqttAnchorOwed} {
		a := testRetirableLifecycle()
		a.Life.Anchors = []mqttWillAnchor{{Ord: 1, AcceptOrd: 0, State: st, FireAt: 5}}
		if mqttLifecycleRetirable(a) {
			t.Fatalf("a lifecycle holding an anchor in state %d reads retirable", st)
		}
		next := *a
		nl := *a.Life
		nl.Phase = mqttLifecycleRetiring
		next.Life = &nl
		tr := &mqttAuthorityTransition{Prev: a, Auth: &next, Attempt: &mqttTransitionAttempt{ID: testID},
			Registry: &mqttTransitionMember{subj: mqttLifecycleRegistrySubject(mqttLifecycleRegistryShardFor(getHash(testID)))}}
		if err := mqttCheckRetireLifecycleRules(tr); err == nil {
			t.Fatalf("a lifecycle fence over an anchor in state %d passed its rule", st)
		}
	}
}

// T-anchor-cap-close: with every anchor slot taken, a delay-capable acceptance is refused, one
// without a delayed Will is not, and a close (ARMED to OWED) needs no slot of its own: anchor
// space is reserved at acceptance. Mutation M36: anchor space allocated at the close.
func TestAnchorCapacityIsReservedAtAcceptance(t *testing.T) {
	pl := testServingLife()
	for i := uint64(1); i <= mqttMaxWillAnchors; i++ {
		pl.Anchors = append(pl.Anchors, mqttWillAnchor{Ord: i, AcceptOrd: 0, State: mqttAnchorArmed})
	}
	pl.NextAnchorOrd = mqttMaxWillAnchors + 1
	if _, _, _, err := mqttAcceptAnchors(pl, 1, 10, true); err == nil {
		t.Fatal("a delay-capable acceptance at the anchor cap was admitted")
	}
	if _, _, own, err := mqttAcceptAnchors(pl, 1, 10, false); err != nil || own != nil {
		t.Fatalf("an acceptance without a delayed Will at the cap was refused or allocated: %v %+v", err, own)
	}
	prev := testTransition().Prev
	prev.Life = pl
	for i := uint64(1); i <= mqttMaxWillAnchors; i++ {
		if _, err := mqttComposeAnchorTransition(prev, 11, "txn-close", i, mqttAnchorOwe, 99); err != nil {
			t.Fatalf("closing the connection of anchor %d at the cap was refused: %v", i, err)
		}
	}
}

// T-gen-reuse: an acceptance installs exactly its lifecycle's next generation; one naming an
// earlier (L, GenOrd) is refused. Mutation M16: the NextGenOrd rule skipped.
func TestAnInstallNamingAnEarlierGenerationIsRefused(t *testing.T) {
	pl := testServingLife()
	pl.NextGenOrd = 3
	l := pl.L
	for ord, want := range map[uint64]bool{0: false, 2: false, 3: true} {
		gen, err := mqttGenToken(l, ord)
		if err != nil {
			t.Fatal(err)
		}
		nl := *pl
		nl.NextGenOrd = 4
		next := &mqttOutboundAuthority{Gen: gen, Life: &nl}
		if got := mqttCheckInstallsNextGeneration(next, pl) == nil; got != want {
			t.Fatalf("an install of ordinal %d with the allocator at 3 admitted=%v, want %v", ord, got, want)
		}
	}
}

// T-outcome-scope: an attempt's outcome scope is its kind's: a generation-scoped kind carrying a
// lifecycle scope, or a lifecycle-scoped kind carrying a generation scope, is refused before
// anything is written. Mutation M22: the scope check skipped.
func TestAnAttemptsScopeIsItsKinds(t *testing.T) {
	l, _, _ := mqttParseGenToken(testGen)
	life, err := mqttLifecycleScope(l)
	if err != nil {
		t.Fatal(err)
	}
	gen := testAuthorityAttempt() // admit-exchange: generation-scoped
	gen.Scope = life
	if err := mqttValidateTransitionAttempt(gen); err == nil || !strings.Contains(err.Error(), "is generation-scoped") {
		t.Fatalf("a generation-scoped attempt with a lifecycle scope was admitted: %v", err)
	}
	lc := testAuthorityAttempt()
	lc.Kind = mqttTransitionWillAnchor
	lc.Scope = testGen
	if err := mqttValidateTransitionAttempt(lc); err == nil || !strings.Contains(err.Error(), "is lifecycle-scoped") {
		t.Fatalf("a lifecycle-scoped attempt with a generation scope was admitted: %v", err)
	}
	lc.Scope = life
	if err := mqttValidateTransitionAttempt(lc); err != nil {
		t.Fatalf("a lifecycle-scoped attempt with its lifecycle's scope was refused: %v", err)
	}
}

// T-admit-on-retiring: every acceptance over a retiring authority is refused at the write
// boundary, isolated from the registry and the composer. Mutation M3b: the retiring check dropped.
func TestNoAcceptanceLandsOnARetiringLifecycle(t *testing.T) {
	for _, kind := range []mqttTransitionKind{mqttTransitionAcceptHandoff, mqttTransitionAcceptReopen,
		mqttTransitionAcceptDisplace, mqttTransitionBind, mqttTransitionAcceptAbandon} {
		tr := testTransition()
		tr.Attempt.Kind = kind
		tr.Attempt.Scope = mqttLifeScope(tr.Prev.Life)
		tr.Prev.Life.Phase = mqttLifecycleRetiring
		if err := mqttCheckGenerationBoundary(tr); err == nil || !strings.Contains(err.Error(), "which is retiring") {
			t.Fatalf("a %s over a retiring lifecycle was not refused by the phase rule: %v", kind, err)
		}
	}
}

// The capacity proof (plan section 9): the authority at its caps, every field at its widest, and
// a registry shard at its entry cap with every entry retiring at its widest, each serialize
// within their budgets. A cap raised without its budget fails here.
func TestTheWorstCaseLifecycleRecordsFitTheirBudgets(t *testing.T) {
	const maxU = ^uint64(0)
	const maxI = int64(^uint64(0) >> 1)
	nonce := strings.Repeat("N", 22)
	a := testTransition().Prev
	a.ID, a.Tenure, a.Txn, a.OwnerTxn = strings.Repeat("c", 256), nonce, nonce, nonce
	a.Rev = maxU
	life := &mqttLifecycleState{L: mqttMaxLifecycle, Phase: mqttLifecycleRetiring, NextGenOrd: maxU, GenAcqs: true,
		Accept:        mqttAcceptance{Ord: maxU, At: maxI, Tenure: nonce, Nonce: nonce},
		NextAcceptOrd: maxU, NextAnchorOrd: maxU,
		Pending: &mqttPendingAcceptance{Kind: mqttPendingResume, AcceptOrd: maxU, Nonce: nonce, At: maxI,
			Prior: &mqttPriorLifetime{ExpiryInterval: ^uint32(0)}}}
	for i := 0; i < mqttMaxGenerationSlots; i++ {
		life.Outstanding = append(life.Outstanding, mqttOutstandingGen{Ord: maxU, Phase: mqttOutstandingBounded, Bound: maxU,
			Acqs: true, Teardown: mqttOutstandingTeardown{Kind: mqttTeardownAbandon, Nonce: nonce, MarkerWID: nonce,
				Pending: true, Displaced: true}})
	}
	for i := 0; i < mqttMaxWillAnchors; i++ {
		life.Anchors = append(life.Anchors, mqttWillAnchor{Ord: maxU, AcceptOrd: maxU, State: mqttAnchorOwed, FireAt: maxI, MinAt: maxI})
	}
	a.Life = life
	b, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > mqttLifecycleAuthorityBudget {
		t.Fatalf("the worst-case authority serializes to %d bytes, over its %d budget", len(b), mqttLifecycleAuthorityBudget)
	}

	shard := &mqttLifecycleRegistryShard{Epoch: strings.Repeat("e", 64), Shard: mqttLifecycleRegistryShards - 1,
		Next: mqttMaxLifecycle, Active: map[string]uint64{}, Retiring: map[string]mqttRetiringLifecycle{}}
	for i := 0; i < mqttLifecycleRegistryShardMaxEntries; i++ {
		shard.Retiring[getHash(strings.Repeat("x", 4)+string(rune('a'+i%26))+strconv.Itoa(i))] =
			mqttRetiringLifecycle{L: mqttMaxLifecycle, Phase: mqttLifecycleBounded, Bound: maxU}
	}
	sb, serr := json.Marshal(shard)
	if serr != nil {
		t.Fatal(serr)
	}
	if len(sb) > mqttLifecycleRegistryShardBudget {
		t.Fatalf("a registry shard at its %d-entry cap serializes to %d bytes, over its %d budget",
			mqttLifecycleRegistryShardMaxEntries, len(sb), mqttLifecycleRegistryShardBudget)
	}
	// AND THE CAP IS ENFORCED: no room at the cap, room below it.
	if mqttRegistryShardHasRoom(shard) {
		t.Fatal("a registry shard at its entry cap reports room for a creation")
	}
	delete(shard.Retiring, getHash("xxxxa0"))
	if !mqttRegistryShardHasRoom(shard) {
		t.Fatal("a registry shard below its entry cap reports no room")
	}
}

// A resume abandonment writes the session back on the PAIR its pending acceptance recorded as the
// prior lifetime, expiry and clean flag together (issue #133): a record member carrying either
// one changed is refused at the boundary, whatever the composer did. Mutation: the boundary
// checking the expiry alone.
func TestAResumeAbandonmentRestoresThePriorLifetimePair(t *testing.T) {
	prev := testTransition().Prev
	life := *prev.Life
	life.Pending = &mqttPendingAcceptance{Kind: mqttPendingResume, AcceptOrd: life.Accept.Ord, Nonce: life.Accept.Nonce,
		At: 7e9, Prior: &mqttPriorLifetime{ExpiryInterval: 300}}
	prev.Life = &life
	rec := &mqttPersistedSession{ID: prev.ID, Gen: prev.Gen, Clean: true, ExpiryInterval: 0}
	tr, err := mqttComposeResumeAbandon(prev, 5, "txn-abandon", "sess-subject", 9, rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := mqttCheckResumeAbandonRules(tr); err != nil {
		t.Fatalf("the composed abandonment was refused: %v", err)
	}
	for name, forge := range map[string]func(r *mqttPersistedSession){
		"the requested clean flag kept": func(r *mqttPersistedSession) { r.Clean = true },
		"the requested expiry kept":     func(r *mqttPersistedSession) { r.ExpiryInterval = 0 },
	} {
		var body mqttPersistedSession
		if err := json.Unmarshal(tr.Record.body, &body); err != nil {
			t.Fatal(err)
		}
		forge(&body)
		b, _ := json.Marshal(&body)
		forged := *tr
		forged.Record = &mqttTransitionMember{subj: tr.Record.subj, expect: tr.Record.expect, body: b}
		if err := mqttCheckResumeAbandonRules(&forged); err == nil {
			t.Fatalf("an abandonment record with %s passed the boundary", name)
		}
	}
}
