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

//go:build mqtt_fulltests

package mqtt

import (
	"testing"
	"time"
)

// testWalkHolders runs the identifier walk for pi and returns the phases of the exchanges it
// found holding it.
func testWalkHolders(t *testing.T, e *testF105Env, pi uint16) []mqttExchangePhase {
	t.Helper()
	w, err := mqttTraverseInventoryForIdentifiers(&e.asm.jsa, e.id, e.epoch, testF105Gen,
		map[uint16]struct{}{pi: {}})
	if err != nil || w == nil {
		t.Fatalf("the identifier walk failed: %v", err)
	}
	if len(w.uncertain) != 0 {
		t.Fatalf("the identifier walk left %v uncertain", w.uncertain)
	}
	var phases []mqttExchangePhase
	for _, x := range w.holders {
		if x.PI == pi {
			phases = append(phases, x.Phase)
		}
	}
	return phases
}

// testCachedPositions returns the generation's cached ordinals.
func testCachedPositions(e *testF105Env) map[uint64]mqttDutyIdentity {
	c := &e.asm.jsa.dutyIdentities
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[uint64]mqttDutyIdentity{}
	if g := c.gens[mqttDutyIdentityKey(e.id, e.epoch, testF105Gen)]; g != nil {
		for ord, d := range g.pos {
			out[ord] = d
		}
	}
	return out
}

// A WALK READS EVERY POSITION THE CACHE DOES NOT HOLD, holes below the cached range included: a
// position dropped from the cache while its duty and a live holder of the asked identifier stand
// is read again, and the holder found.
//
// Mutation M-L5a: read only positions above the highest cached one -> the dropped position is
// skipped and the holder missed.
func TestIdentifierWalkReadsOnlyUnseenPositions(t *testing.T) {
	e, done := testF105NewEnv(t, "dutyid-unseen")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	for i, pi := range []uint16{7, 8, 9} {
		testF105AdmitOn(t, e, "txn-adm-"+string(rune('a'+i)), pi, "inc-src")
	}
	if got := testWalkHolders(t, e, 8); len(got) != 1 {
		t.Fatalf("the first walk found %d holder(s) of identifier 8, want 1", len(got))
	}
	cached := testCachedPositions(e)
	if len(cached) != 3 {
		t.Fatalf("the walk cached %d position(s), want all 3 it validated", len(cached))
	}
	var hole uint64
	for ord, d := range cached {
		if d.pi == 8 {
			hole = ord
		}
	}
	if hole == 0 || hole == 3 {
		t.Fatalf("fixture: identifier 8's position %d is not below the highest cached one", hole)
	}
	c := &e.asm.jsa.dutyIdentities
	c.mu.Lock()
	delete(c.gens[mqttDutyIdentityKey(e.id, e.epoch, testF105Gen)].pos, hole)
	c.mu.Unlock()
	if got := testWalkHolders(t, e, 8); len(got) != 1 {
		t.Fatalf("the walk over a dropped position found %d holder(s) of identifier 8, want 1", len(got))
	}
	if _, ok := testCachedPositions(e)[hole]; !ok {
		t.Fatal("the re-read position was not cached again")
	}
}

// THE CACHE HOLDS IDENTITY, NEVER STATE: after an exchange the warmed cache names moves through a
// legal phase transition, the next walk reports the phase it stands at now.
//
// Mutation M-L5b: cache the exchange with the identity -> the walk reports the stale phase.
func TestIdentifierWalkCacheHoldsIdentityOnly(t *testing.T) {
	e, done := testF105NewEnv(t, "dutyid-state")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	if got := testWalkHolders(t, e, 7); len(got) != 1 || got[0] != mqttExchangePublish {
		t.Fatalf("the warming walk found %v, want one holder at Publish", got)
	}
	if _, err := testAdvanceLive(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, nonce, mqttExchangePubRecObserved); err != nil {
		t.Fatalf("advancing the holder: %v", err)
	}
	if got := testWalkHolders(t, e, 7); len(got) != 1 || got[0] != mqttExchangePubRecObserved {
		t.Fatalf("the walk over a cached position found %v, want the holder at PUBREC-observed", got)
	}
}

// PRUNED BELOW THE FLOOR, SCOPED, BOUNDED, AND NEVER REFILLED ACROSS A PRUNE OR AN EVICTION.
//
// Mutation M-L5c: no prune -> positions below the floor stay cached.
func TestIdentifierWalkCachePrunes(t *testing.T) {
	var c mqttDutyIdentityCache
	now := time.Now()
	d := func(pi uint16) *mqttDutyEntry { return &mqttDutyEntry{Kind: mqttDutyExchange, Ref: "x", PI: pi} }

	w := c.begin("c", "ep", "g", 1, 5, now)
	for ord := uint64(1); ord <= 5; ord++ {
		w.offer(ord, d(uint16(ord)))
	}
	stale := c.begin("c", "ep", "g", 1, 5, now) // a walk in flight across the prune below
	stale.offer(1, d(1))
	c.finish(w)
	if n := len(c.gens[mqttDutyIdentityKey("c", "ep", "g")].pos); n != 5 {
		t.Fatalf("%d positions cached, want 5", n)
	}

	// The floor moved to 3: positions 1 and 2 go, and the in-flight walk's fill is dropped.
	w2 := c.begin("c", "ep", "g", 3, 5, now)
	if len(w2.known) != 3 {
		t.Fatalf("the walk above the floor knows %d positions, want 3", len(w2.known))
	}
	c.finish(stale)
	for _, ord := range []uint64{1, 2} {
		if _, ok := c.gens[mqttDutyIdentityKey("c", "ep", "g")].pos[ord]; ok {
			t.Fatalf("position %d below the floor is cached", ord)
		}
	}

	// Another epoch or generation of the client is another scope.
	if w3 := c.begin("c", "ep2", "g", 1, 5, now); len(w3.known) != 0 {
		t.Fatalf("another epoch's walk knows %d positions", len(w3.known))
	}

	// Idle generations are evicted, and a walk that began before the eviction cannot refill.
	inFlight := c.begin("c", "ep", "g", 3, 5, now)
	inFlight.offer(6, d(6))
	c.begin("other", "ep", "g", 1, 1, now.Add(mqttDutyIdentityIdle+time.Minute))
	if _, ok := c.gens[mqttDutyIdentityKey("c", "ep", "g")]; ok {
		t.Fatal("an idle generation was not evicted")
	}
	c.finish(inFlight)
	if _, ok := c.gens[mqttDutyIdentityKey("c", "ep", "g")]; ok {
		t.Fatal("a walk that began before the eviction repopulated the generation")
	}

	// At most mqttDutyIdentityGens generations; the least recently walked goes first.
	var b mqttDutyIdentityCache
	for i := 0; i <= mqttDutyIdentityGens; i++ {
		b.begin("c", "ep", string(rune('A'+i)), 1, 1, now.Add(time.Duration(i)*time.Second))
	}
	if len(b.gens) != mqttDutyIdentityGens {
		t.Fatalf("%d generations cached, want the bound %d", len(b.gens), mqttDutyIdentityGens)
	}
	if _, ok := b.gens[mqttDutyIdentityKey("c", "ep", "A")]; ok {
		t.Fatal("the least recently walked generation was not the one evicted")
	}
}
