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

//go:build mqttport_fulltests

package mqtt

import "testing"

// TestF105NameOwnershipTransitionLegality pins the whole legality matrix of the logical-use
// fence: a name goes absent -> LIVE(owner) -> RETIRED(owner) and nowhere else. It is the
// pure invariant, exercised without a store so every arm is exact.
func TestF105NameOwnershipTransitionLegality(t *testing.T) {
	live := func(gen, inc string) *mqttNameOwnership {
		return &mqttNameOwnership{ID: "c", Epoch: "e", Stream: mqttOwnedStreamMsgs, Name: "n",
			State: mqttNameOwnLive, OwnerGen: gen, OwnerInc: inc}
	}
	retired := func(gen, inc string) *mqttNameOwnership {
		o := live(gen, inc)
		o.State = mqttNameOwnRetired
		return o
	}

	// A first claim of an absent name must be LIVE, and by anyone.
	if err := mqttNameOwnershipTransitionLegal(nil, live("g1", "i1")); err != nil {
		t.Fatalf("a first LIVE claim was refused: %v", err)
	}
	if err := mqttNameOwnershipTransitionLegal(nil, retired("g1", "i1")); err == nil {
		t.Fatal("a first claim installing RETIRED was accepted")
	}

	// Same owner: LIVE->LIVE (idempotent) and LIVE->RETIRED both stand.
	if err := mqttNameOwnershipTransitionLegal(live("g1", "i1"), live("g1", "i1")); err != nil {
		t.Fatalf("an idempotent same-owner re-claim was refused: %v", err)
	}
	if err := mqttNameOwnershipTransitionLegal(live("g1", "i1"), retired("g1", "i1")); err != nil {
		t.Fatalf("the owner retiring its own name was refused: %v", err)
	}

	// A DIFFERENT owner may neither re-claim nor retire a LIVE name.
	if err := mqttNameOwnershipTransitionLegal(live("g1", "i1"), live("g2", "i2")); err == nil {
		t.Fatal("a second owner claimed a LIVE name")
	}
	if err := mqttNameOwnershipTransitionLegal(live("g1", "i1"), retired("g2", "i2")); err == nil {
		t.Fatal("a non-owner retired a LIVE name")
	}

	// A RETIRED name never revives, and is never re-retired into a new owner.
	if err := mqttNameOwnershipTransitionLegal(retired("g1", "i1"), live("g1", "i1")); err == nil {
		t.Fatal("a RETIRED name was revived to LIVE")
	}
	if err := mqttNameOwnershipTransitionLegal(retired("g1", "i1"), retired("g1", "i1")); err == nil {
		t.Fatal("a RETIRED name accepted a further transition")
	}

	// The name a transition is about may not change under it.
	renamed := live("g1", "i1")
	renamed.Name = "other"
	if err := mqttNameOwnershipTransitionLegal(live("g1", "i1"), renamed); err == nil {
		t.Fatal("an ownership transition changed the name it was about")
	}
}

// TestF105NameOwnershipBoundaryEnforcesTheFence drives the real write boundary: provisioning a
// source consumer claims its name LIVE, a second owner of the same physical name is refused,
// and a stray ownership member on a kind that neither provisions nor disposes is refused.
func TestF105NameOwnershipBoundaryEnforcesTheFence(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-name-ownership")
	defer done()
	jsa := &e.asm.jsa
	e.bootstrap(t, "boot")

	const name = "$MQTT_src_ownership"

	// (1) Provisioning a source consumer claims its physical name LIVE, keyed by the name and
	// owned by this resource incarnation.
	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-p1", "op-p1",
		testF105Gen, "tenure-boot", "inc-a", name, mqttResourceSourceConsumer); err != nil {
		t.Fatalf("provisioning the source consumer was refused: %v", err)
	}
	own, _, oerr := jsa.loadNameOwnership(e.id, e.epoch, mqttOwnedStreamMsgs, name)
	if oerr != nil || own == nil {
		t.Fatalf("the name's ownership did not land (%v, %v)", own, oerr)
	}
	if own.State != mqttNameOwnLive || own.OwnerGen != testF105Gen || own.OwnerInc != "inc-a" {
		t.Fatalf("the ownership record is %+v", own)
	}

	// (2) A SECOND owner claiming the SAME physical name is refused — a replacement must use a
	// fresh name, never relive a standing one.
	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-p2", "op-p2",
		testF105Gen, "tenure-boot", "inc-b", name, mqttResourceSourceConsumer); err == nil {
		t.Fatal("a second owner claimed a name already held LIVE")
	}

	// (3) A stray ownership member on a kind that neither provisions nor disposes is refused at
	// the boundary, keyed on the member and not trusted to the composer.
	stray, merr := mqttNameOwnershipMember(e.id, e.epoch, mqttOwnedStreamMsgs, "$MQTT_src_stray",
		mqttNameOwnLive, testF105Gen, "inc-c", 0)
	if merr != nil {
		t.Fatalf("composing the stray member failed: %v", merr)
	}
	if err := jsa.checkNameOwnershipMembers(
		&mqttTransitionAttempt{ID: e.id, Epoch: e.epoch, Txn: "txn-x", Kind: mqttTransitionAcquireAction},
		[]mqttTransitionMember{stray},
	); err == nil {
		t.Fatal("an ownership member rode a kind that neither provisions nor disposes")
	}
}
