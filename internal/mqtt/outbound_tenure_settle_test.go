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

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// F-105 7f: a serving generation settles its displaced tenures.

// testResumeUnderNewTenure is a successor tenure's accept on the rig: the authority moves to a
// fresh tenure of the SAME generation, and the rig's connection now holds it, as the accepted
// resume's connection does. The session stays resident.
func testResumeUnderNewTenure(t *testing.T, e *testInflightEnv) mqttOutboundEntitlement {
	t.Helper()
	testSupersedeTenure(t, e)
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		t.Fatalf("no authority after the supersession: %+v", al)
	}
	ent := mqttOutboundEntitlement{Gen: al.rec.Gen, Tenure: al.rec.Tenure}
	e.c.setOutboundEntitlement(ent)
	e.sess.mu.Lock()
	e.sess.boundEnt, e.sess.boundEntOK = ent, true
	e.sess.mu.Unlock()
	e.asm.mu.Lock()
	e.asm.sessions[e.sess.id] = e.sess
	e.asm.mu.Unlock()
	return ent
}

// testSettleClasses runs one settlement job to completion and returns every classification it
// reported, by consumer name.
func testSettleClasses(t *testing.T, e *testInflightEnv, ent mqttOutboundEntitlement) map[string]string {
	t.Helper()
	var mu sync.Mutex
	got := map[string]string{}
	h := func(name, class string) {
		mu.Lock()
		got[name] = class
		mu.Unlock()
	}
	e.sess.testSettleClassified.Store(&h)
	defer e.sess.testSettleClassified.Store(nil)
	done := make(chan struct{})
	go func() {
		e.sess.runTenureSettlement(ent)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the settlement job never completed its visit")
	}
	mu.Lock()
	defer mu.Unlock()
	out := make(map[string]string, len(got))
	for k, v := range got {
		out[k] = v
	}
	return out
}

func testLoadResource(t *testing.T, e *testInflightEnv, gen, inc string) *mqttOutboundResource {
	t.Helper()
	r, _, err := e.asm.jsa.loadOutboundResource(e.sess.id, e.asm.sessStreamEpoch, gen, inc)
	if err != nil || r == nil {
		t.Fatalf("reading resource %q: %+v (%v)", inc, r, err)
	}
	return r
}

// testAwaitRetired waits for the owner to finish a retirement — its last step resolves the duties
// after the resource reads CleanupComplete — and checks the decision's reason and tenure.
func testAwaitRetired(t *testing.T, e *testInflightEnv, gen, inc, name, reason, tenure string) {
	t.Helper()
	jsa := &e.asm.jsa
	checkFor(t, 15*time.Second, 25*time.Millisecond, func() error {
		r, _, err := jsa.loadOutboundResource(e.sess.id, e.asm.sessStreamEpoch, gen, inc)
		if err != nil || r == nil || r.State != mqttResourceCleanupComplete {
			return fmt.Errorf("not yet retired: %+v (%v)", r, err)
		}
		for _, kind := range []mqttDutyKind{mqttDutyProvision, mqttDutyCleanup} {
			ord, oerr := jsa.resourceDutyOrdinal(e.sess.id, e.asm.sessStreamEpoch, gen, inc, kind)
			if oerr != nil {
				return oerr
			}
			d, _, derr := jsa.loadDutyEntryAt(e.sess.id, e.asm.sessStreamEpoch, gen, ord)
			if derr != nil || d == nil || d.State != mqttDutyTerminal {
				return fmt.Errorf("the %s duty is not yet terminal", kind)
			}
		}
		return nil
	})
	testRequireSourceRetired(t, e, gen, inc, name)
	r := testLoadResource(t, e, gen, inc)
	if r.Disposal.Reason != reason || r.Disposal.Tenure != tenure {
		t.Fatalf("the decision is %q under %q, want %q under %q", r.Disposal.Reason, r.Disposal.Tenure, reason, tenure)
	}
}

// T1 (M11): provisioning stamps the serving tenure, read back from the store; the schema
// requires it on the logical-use kinds only and the step rule keeps it.
func TestF105ProvisioningStampsItsTenure(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t1")
	ent := testServingEntitlement(t, e.sess)
	cl := testClaimedSourceConsumer(t, e, "settle.t1")
	if r := testLoadResource(t, e, cl.gen, cl.inc); r.ProvTenure != ent.Tenure {
		t.Fatalf("the source resource carries provisioning tenure %q, want the serving %q", r.ProvTenure, ent.Tenure)
	}
	src := testLoadResource(t, e, cl.gen, cl.inc)
	bare := *src
	bare.ProvTenure = _EMPTY_
	if err := mqttValidateOutboundResource(&bare, src.ID, src.Epoch, src.Gen, src.Inc); err == nil {
		t.Fatal("a source resource without its provisioning tenure must be an unsupported shape")
	}
	payload := *src
	payload.Kind = mqttResourceRetainedPayload
	if err := mqttValidateOutboundResource(&payload, src.ID, src.Epoch, src.Gen, src.Inc); err == nil {
		t.Fatal("a payload resource carrying a provisioning tenure must be refused")
	}
	moved := *src
	moved.ProvTenure = "another-tenure"
	a := &mqttTransitionAttempt{Txn: "txn-t1"}
	if err := mqttCheckResourceStep(a, src, &moved, nil, nil, nil, nil, nil, nil); err == nil ||
		!strings.Contains(err.Error(), "provisioning tenure") {
		t.Fatalf("a step rewriting the provisioning tenure must be refused, got %v", err)
	}
}

// T2 (M3b): the creation boundary refuses a provisioned resource stamped with a tenure that does
// not serve, and admits the serving one.
func TestF105CreationBoundaryProvesTheProvisioningTenure(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t2")
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	member := func(tenure string) []mqttTransitionMember {
		r := &mqttOutboundResource{ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Gen: al.rec.Gen, Inc: "inc-t2",
			Kind: mqttResourceSourceConsumer, Name: "$MQTT_t2", State: mqttResourceLive,
			Prov: mqttProvisioningPending, Deps: []mqttResourceEdge{}, ProvTenure: tenure}
		b, _ := json.Marshal(r)
		return []mqttTransitionMember{{subj: mqttResourceSubject(getHash(e.sess.id), al.rec.Gen, "inc-t2"), body: b}}
	}
	a := &mqttTransitionAttempt{ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Txn: "txn-t2",
		Kind: mqttTransitionProvisionResource}
	if err := e.asm.jsa.checkCreatedResources(a, al.rec, member("a-foreign-tenure")); err == nil {
		t.Fatal("a resource provisioned under a tenure that does not serve must be refused at creation")
	}
	if err := e.asm.jsa.checkCreatedResources(a, al.rec, member(al.rec.Tenure)); err != nil {
		t.Fatalf("the serving tenure must be admitted (positive control): %v", err)
	}
}

// testSettleDisposeBatch submits a hand-built disposal of resource inc at the CURRENT authority's CAS
// expectations, deciding it under (gen, tenure) for reason.
func testSettleDisposeBatch(t *testing.T, e *testInflightEnv, gen, inc, reason, tenure string) error {
	t.Helper()
	jsa := &e.asm.jsa
	auth, authSeq, err := jsa.requireAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if err != nil || auth == nil {
		t.Fatalf("authority: %+v (%v)", auth, err)
	}
	cur, curSeq, rerr := jsa.loadOutboundResource(e.sess.id, e.asm.sessStreamEpoch, gen, inc)
	if rerr != nil || cur == nil {
		t.Fatalf("resource: %+v (%v)", cur, rerr)
	}
	next := *cur
	txn := mqttNewInflightNonce()
	next.Disposal = &mqttResourceDisposal{Txn: txn, Reason: reason, Gen: gen, Tenure: tenure}
	_, cerr := jsa.commitResourceStep(auth, authSeq, txn, "op-test-dispose", &next, curSeq)
	return cerr
}

// T2b (M3) and T10 (M4): the disposal boundary's two rules for the settlement reasons, each
// isolated from the other.
func TestF105SettlementReasonsAtTheBoundary(t *testing.T) {
	t.Parallel()
	t.Run("a tenure's own claim is not superseded", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "settle-t2b-own")
		ent := testServingEntitlement(t, e.sess)
		cl := testClaimedSourceConsumer(t, e, "settle.t2b.own")
		err := testSettleDisposeBatch(t, e, cl.gen, cl.inc, mqttDisposalTenureSuperseded, ent.Tenure)
		if err == nil || !strings.Contains(err.Error(), "the tenure that provisioned it") {
			t.Fatalf("a superseded decision by the provisioning tenure must be refused, got %v", err)
		}
	})
	t.Run("a PUBREL consumer is never decided by a settlement reason", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "settle-t2b-pubrel")
		e.subscribePubRel(t)
		ent := testServingEntitlement(t, e.sess)
		_, _, dur := mqttPubRelNames(e.sess.idHash, e.sess.gen)
		own, _, _ := e.asm.jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamOut, dur)
		if own == nil {
			t.Fatal("fixture: the PUBREL name must be claimed")
		}
		err := testSettleDisposeBatch(t, e, own.OwnerGen, own.OwnerInc, mqttDisposalRepairReplaced, ent.Tenure)
		if err == nil || !strings.Contains(err.Error(), "subscription's source consumer") {
			t.Fatalf("a repair decision about the PUBREL consumer must be refused, got %v", err)
		}
	})
	t.Run("a displaced tenure's claim is superseded by the serving one", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "settle-t2b-positive")
		cl := testClaimedSourceConsumer(t, e, "settle.t2b.pos")
		ent := testResumeUnderNewTenure(t, e)
		if err := testSettleDisposeBatch(t, e, cl.gen, cl.inc, mqttDisposalTenureSuperseded, ent.Tenure); err != nil {
			t.Fatalf("positive control: %v", err)
		}
	})
	// T10: provisioned under T0, decided under the displaced T1 while T2 serves. The provisioning
	// tenure differs from the deciding one, so only the serving-entitlement rule can refuse it.
	t.Run("a decision under a displaced tenure", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "settle-t10")
		cl := testClaimedSourceConsumer(t, e, "settle.t10")
		t1 := testResumeUnderNewTenure(t, e)
		testResumeUnderNewTenure(t, e)
		err := testSettleDisposeBatch(t, e, cl.gen, cl.inc, mqttDisposalTenureSuperseded, t1.Tenure)
		if err == nil || !strings.Contains(err.Error(), "not the serving one") {
			t.Fatalf("a decision under a displaced tenure must be refused by the serving rule, got %v", err)
		}
	})
}

// T3a/T3b: a claim whose SUBSCRIBE never named it — before and after its physical CREATE — is
// settled by the next tenure: decided `tenure-superseded` under it, its name retired, the
// consumer deleted, both duties resolved.
func TestF105SupersededTenureSettlesAnUnnamedClaim(t *testing.T) {
	t.Parallel()
	for _, created := range []bool{false, true} {
		created := created
		t.Run(fmt.Sprintf("created=%v", created), func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, fmt.Sprintf("settle-t3-%v", created))
			var cl *mqttSourceClaim
			if created {
				cl = testClaimedSourceConsumer(t, e, "settle.t3")
			} else {
				name := e.sess.idHash + "_t3claimonly"
				var err error
				cl, err = e.sess.claimSourceConsumerName(testServingEntitlement(t, e.sess), "settle.t3", name)
				if err != nil {
					t.Fatalf("claim: %v", err)
				}
			}
			ent := testResumeUnderNewTenure(t, e)
			classes := testSettleClasses(t, e, ent)
			if classes[cl.name] != mqttSettleSupersededUndecide {
				t.Fatalf("the unnamed claim of the displaced tenure must classify superseded, got %q", classes[cl.name])
			}
			testAwaitRetired(t, e, cl.gen, cl.inc, cl.name, mqttDisposalTenureSuperseded, ent.Tenure)
		})
	}
}

// T5 (M1): a displaced tenure's consumer that a subscription or a drain still names is spared.
func TestF105SettlementSparesNamedConsumers(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t5")
	sub := testClaimedSourceConsumer(t, e, "settle.t5.sub")
	drn := testClaimedSourceConsumer(t, e, "settle.t5.drain")
	e.sess.mu.Lock()
	if e.sess.cons == nil {
		e.sess.cons = map[string]*ConsumerConfig{}
	}
	e.sess.cons["settle.t5.sub"] = &ConsumerConfig{Durable: sub.name}
	if e.sess.drains == nil {
		e.sess.drains = map[string]*mqttDrain{}
	}
	e.sess.drains["settle.t5.drain"] = &mqttDrain{cc: &ConsumerConfig{Durable: drn.name}}
	e.sess.mu.Unlock()
	ent := testResumeUnderNewTenure(t, e)
	classes := testSettleClasses(t, e, ent)
	for _, cl := range []*mqttSourceClaim{sub, drn} {
		if classes[cl.name] != mqttSettleNamed {
			t.Fatalf("%q is named and must classify named, got %q", cl.name, classes[cl.name])
		}
		if r := testLoadResource(t, e, cl.gen, cl.inc); r.Disposal != nil {
			t.Fatalf("a named consumer was decided: %+v", r.Disposal)
		}
	}
	e.sess.mu.Lock()
	delete(e.sess.drains, "settle.t5.drain")
	e.sess.mu.Unlock()
}

// T6 (M2): the classifier never supersedes a consumer the serving tenure itself provisioned.
func TestF105SettlementLeavesItsOwnTenuresClaims(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t6")
	ent := testResumeUnderNewTenure(t, e)
	cl := testClaimedSourceConsumer(t, e, "settle.t6")
	class, hand, err := e.sess.classifyForSettlement(ent, testLoadResource(t, e, cl.gen, cl.inc))
	if err != nil || class != mqttSettleOwnTenure || hand != _EMPTY_ {
		t.Fatalf("an unnamed claim of the serving tenure must classify own-tenure and hand nothing, got %q/%q (%v)",
			class, hand, err)
	}
}

// T7: a route's hand-off lost after its save removed the name is settled by the next tenure.
func TestF105SettlementCoversALostRouteHandOff(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t7")
	cl := testClaimedSourceConsumer(t, e, "settle.t7")
	var site atomic.Value
	h := func(s, _ string) { site.Store(s) }
	e.sess.testSourceDisposalSite.Store(&h)
	e.sess.testSourceDisposalDrop.Store(true)
	e.sess.disposeSourceConsumer(e.c, &ConsumerConfig{Durable: cl.name}, mqttDisposalUnsubscribed, "test-unsubscribe")
	if site.Load() != "test-unsubscribe" {
		t.Fatal("fixture: the route's hand-off must be reached and dropped")
	}
	e.sess.testSourceDisposalDrop.Store(false)
	ent := testResumeUnderNewTenure(t, e)
	if c := testSettleClasses(t, e, ent)[cl.name]; c != mqttSettleSupersededUndecide {
		t.Fatalf("got %q", c)
	}
	testAwaitRetired(t, e, cl.gen, cl.inc, cl.name, mqttDisposalTenureSuperseded, ent.Tenure)
}

// T8a (M6) and T8b: a decision that stands is finished by the next tenure from whatever state it
// stood in, keeping the original decision.
func TestF105SettlementFinishesAStandingDecision(t *testing.T) {
	t.Parallel()
	t.Run("decided, not authorized", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "settle-t8a")
		cl := testClaimedSourceConsumer(t, e, "settle.t8a")
		t0 := testServingEntitlement(t, e.sess)
		if _, err := e.asm.jsa.opRequestDisposal(&mqttRequestDisposalRequest{ID: e.sess.id,
			Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Gen: cl.gen, Inc: cl.inc,
			Reason: mqttDisposalUnsubscribed, Entitlement: t0}); err != nil {
			t.Fatalf("the route's decision: %v", err)
		}
		ent := testResumeUnderNewTenure(t, e)
		if c := testSettleClasses(t, e, ent)[cl.name]; c != mqttSettleDecided {
			t.Fatalf("got %q", c)
		}
		testAwaitRetired(t, e, cl.gen, cl.inc, cl.name, mqttDisposalUnsubscribed, t0.Tenure)
	})
	t.Run("authorized, consumer already deleted", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "settle-t8b")
		cl := testClaimedSourceConsumer(t, e, "settle.t8b")
		t0 := testServingEntitlement(t, e.sess)
		jsa := &e.asm.jsa
		if _, err := jsa.opRequestDisposal(&mqttRequestDisposalRequest{ID: e.sess.id,
			Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Gen: cl.gen, Inc: cl.inc,
			Reason: mqttDisposalUnsubscribed, Entitlement: t0}); err != nil {
			t.Fatalf("decision: %v", err)
		}
		if _, err := jsa.opSealProvisioning(e.sess.id, e.asm.sessStreamEpoch, mqttNewInflightNonce(),
			"op-seal", cl.gen, cl.inc); err != nil {
			t.Logf("seal: %v", err)
		}
		if _, err := jsa.opAuthorizeDeletion(e.sess.id, e.asm.sessStreamEpoch, mqttNewInflightNonce(),
			"op-retire-source", cl.gen, cl.inc); err != nil {
			t.Fatalf("authorize: %v", err)
		}
		if _, err := jsa.deleteConsumer(mqttStreamName, cl.name, false); err != nil {
			t.Fatalf("the sweep's physical delete: %v", err)
		}
		ent := testResumeUnderNewTenure(t, e)
		if c := testSettleClasses(t, e, ent)[cl.name]; c != mqttSettleDecided {
			t.Fatalf("got %q", c)
		}
		testAwaitRetired(t, e, cl.gen, cl.inc, cl.name, mqttDisposalUnsubscribed, t0.Tenure)
	})
}

// T8c (M6b): a displaced tenure's item pending ahead of the successor's no longer swallows it.
// Both items are queued while the owner is held on an unrelated step, the displaced one FIRST.
func TestF105DisplacedItemDoesNotSwallowTheSuccessors(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t8c")
	e.asm.mu.Lock()
	e.asm.sessions[e.sess.id] = e.sess
	e.asm.mu.Unlock()
	blocker := testClaimedSourceConsumer(t, e, "settle.t8c.blocker")
	target := testClaimedSourceConsumer(t, e, "settle.t8c.target")
	t0 := testServingEntitlement(t, e.sess)
	hold := make(chan struct{})
	entered := make(chan struct{}, 1)
	var once sync.Once
	stepHook := func(k mqttRetainedReclaimKey, _ mqttReclaimStep, _ error) {
		if k.src && k.inc == blocker.inc {
			once.Do(func() {
				entered <- struct{}{}
				<-hold
			})
		}
	}
	e.sess.testReclaimAfterStep.Store(&stepHook)
	e.sess.handSourceRetirement(&mqttSourceRetirement{gen: blocker.gen, name: blocker.name, inc: blocker.inc,
		reason: mqttDisposalUnsubscribed, ent: t0})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the owner never stepped the blocker")
	}
	ent := testResumeUnderNewTenure(t, e)
	e.sess.handSourceRetirement(&mqttSourceRetirement{gen: target.gen, name: target.name, inc: target.inc,
		reason: mqttDisposalUnsubscribed, ent: t0})
	e.sess.handSourceRetirement(&mqttSourceRetirement{gen: target.gen, name: target.name, inc: target.inc,
		reason: mqttDisposalTenureSuperseded, ent: ent})
	close(hold)
	testAwaitRetired(t, e, target.gen, target.inc, target.name, mqttDisposalTenureSuperseded, ent.Tenure)
}

// T11: a settled consumer still held by an obligation is decided but not authorized.
func TestF105SettledHeldConsumerWaitsOnItsHolds(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t11")
	cl := testClaimedSourceConsumer(t, e, "settle.t11")
	// The hold, planted raw: an edge only an admission writes. This test is about what the owner
	// does with one, not about how it got there.
	jsa := &e.asm.jsa
	cur, curSeq, _ := jsa.loadOutboundResource(e.sess.id, e.asm.sessStreamEpoch, cl.gen, cl.inc)
	held := *cur
	held.Deps = []mqttResourceEdge{{Nonce: "lin-held", Role: mqttRoleSourceReplay}}
	b, _ := json.Marshal(&held)
	msg, hdr := mqttFrameBatchWrite(curSeq, b, _EMPTY_, 0, false)
	if _, err := jsa.storeMsg(mqttResourceSubject(getHash(e.sess.id), cl.gen, cl.inc), hdr, msg); err != nil {
		t.Fatalf("planting the hold: %v", err)
	}
	ent := testResumeUnderNewTenure(t, e)
	if c := testSettleClasses(t, e, ent)[cl.name]; c != mqttSettleSupersededUndecide {
		t.Fatalf("got %q", c)
	}
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		r := testLoadResource(t, e, cl.gen, cl.inc)
		if r.Disposal == nil {
			return errors.New("not yet decided")
		}
		return nil
	})
	time.Sleep(500 * time.Millisecond)
	if r := testLoadResource(t, e, cl.gen, cl.inc); r.State != mqttResourceLive || len(r.Deps) != 1 {
		t.Fatalf("a held consumer must stay LIVE and held, got %+v", r)
	}
	if !testConsumerPresent(t, e, cl.name) {
		t.Fatal("a held consumer was deleted")
	}
}

// T15 (M5): the settlement never touches the generation's PUBREL consumer.
func TestF105SettlementSkipsThePubRelConsumer(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t15")
	e.subscribePubRel(t)
	_, _, dur := mqttPubRelNames(e.sess.idHash, e.sess.gen)
	ent := testResumeUnderNewTenure(t, e)
	if c := testSettleClasses(t, e, ent)[dur]; c != mqttSettleKindSkipped {
		t.Fatalf("the PUBREL consumer must classify kind-skipped, got %q", c)
	}
	own, _, _ := e.asm.jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamOut, dur)
	if own == nil || own.State != mqttNameOwnLive {
		t.Fatalf("the PUBREL name must stay LIVE: %+v", own)
	}
	if r := testLoadResource(t, e, own.OwnerGen, own.OwnerInc); r.Disposal != nil {
		t.Fatalf("the PUBREL consumer was decided: %+v", r.Disposal)
	}
}

// T17 (M14): a SUBSCRIBE's claim is made under its connection's entitlement, so a connection a
// takeover displaced claims nothing under its successor's tenure.
func TestF105DisplacedSubscribeClaimsNothing(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t17")
	displaced := testServingEntitlement(t, e.sess)
	testSupersedeTenure(t, e)
	name := e.sess.idHash + "_t17"
	if _, err := e.sess.claimSourceConsumerName(displaced, "settle.t17", name); err == nil {
		t.Fatal("a claim under a displaced entitlement must be refused")
	}
	if own, _, _ := e.asm.jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamMsgs, name); own != nil {
		t.Fatalf("a displaced connection's claim landed: %+v", own)
	}
}

// T18 (M15, M15c) and T18b (M15b): the resource walk follows every page, survives an insertion
// before its offset, and retries a failed read without another CONNECT.
func TestF105SettlementWalkIsComplete(t *testing.T) {
	t.Parallel()
	t.Run("pages and an insertion before the offset", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "settle-t18")
		a := testClaimedSourceConsumer(t, e, "settle.t18.a")
		b := testClaimedSourceConsumer(t, e, "settle.t18.b")
		c := testClaimedSourceConsumer(t, e, "settle.t18.c")
		ent := testResumeUnderNewTenure(t, e)
		filter := mqttResourceWalkFilter(getHash(e.sess.id), ent.Gen)
		all, _, err := e.asm.jsa.streamSubjects(mqttInflightStreamName, filter)
		if err != nil || len(all) != 3 {
			t.Fatalf("fixture: three resources expected, got %v (%v)", all, err)
		}
		var sorted []string
		for s := range all {
			sorted = append(sorted, s)
		}
		for i := range sorted {
			for j := i + 1; j < len(sorted); j++ {
				if sorted[j] < sorted[i] {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}
		// Pages of two. Between the first and second page a subject sorting FIRST is inserted,
		// shifting every later entry by one: the second page re-reads one, never skips one.
		var calls atomic.Int32
		obs := func(f string, offset int) (map[string]uint64, int, error) {
			n := calls.Add(1)
			list := sorted
			if n > 1 {
				list = append([]string{filter[:len(filter)-1] + "0000inserted"}, sorted...)
			}
			page := map[string]uint64{}
			for i := offset; i < len(list) && i < offset+2; i++ {
				page[list[i]] = 1
			}
			return page, len(list), nil
		}
		e.asm.jsa.resourceSubjectsPageObs.Store(&obs)
		defer e.asm.jsa.resourceSubjectsPageObs.Store(nil)
		classes := testSettleClasses(t, e, ent)
		for _, cl := range []*mqttSourceClaim{a, b, c} {
			if classes[cl.name] != mqttSettleSupersededUndecide {
				t.Fatalf("%q was not classified across the pages: %v", cl.name, classes)
			}
		}
		if calls.Load() < 2 {
			t.Fatal("the walk must have followed more than one page")
		}
	})
	t.Run("a failed read is retried by the job", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "settle-t18b")
		cl := testClaimedSourceConsumer(t, e, "settle.t18b")
		ent := testResumeUnderNewTenure(t, e)
		var calls atomic.Int32
		obs := func(f string, offset int) (map[string]uint64, int, error) {
			if calls.Add(1) == 1 {
				return nil, 0, errors.New("injected: the listing failed")
			}
			e.asm.jsa.resourceSubjectsPageObs.Store(nil)
			return e.asm.jsa.streamSubjectsPage(mqttInflightStreamName, f, offset)
		}
		e.asm.jsa.resourceSubjectsPageObs.Store(&obs)
		if c := testSettleClasses(t, e, ent)[cl.name]; c != mqttSettleSupersededUndecide {
			t.Fatalf("got %q after a failed first listing", c)
		}
	})
}

// T4: the settlement runs on whichever broker accepts the next tenure. A second account manager on
// the same JetStream restores the session, takes the next tenure, and settles the first broker's
// unnamed claim.
func TestF105SettlementRunsOnTheNextBroker(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-t4")
	cl := testClaimedSourceConsumer(t, e, "settle.t4")
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the first broker's session: %v", err)
	}
	asm2, sess2, c2 := e.peer(t, "node2", "settle-t4")
	e2 := &testInflightEnv{s: e.s, asm: asm2, sess: sess2, c: c2}
	ent := testResumeUnderNewTenure(t, e2)
	if c := testSettleClasses(t, e2, ent)[cl.name]; c != mqttSettleSupersededUndecide {
		t.Fatalf("got %q", c)
	}
	testAwaitRetired(t, e2, cl.gen, cl.inc, cl.name, mqttDisposalTenureSuperseded, ent.Tenure)
}

// testPubRelDurable is the rig's generation-scoped PUBREL consumer name.
func testPubRelDurable(e *testInflightEnv) string {
	_, _, dur := mqttPubRelNames(e.sess.idHash, e.sess.gen)
	return dur
}

func testPubRelPresent(t *testing.T, e *testInflightEnv) bool {
	t.Helper()
	ci, err := e.asm.jsa.lookupConsumer(mqttOutStreamName, testPubRelDurable(e))
	if err != nil && isErrorOtherThan(err, JSConsumerNotFoundErr) {
		t.Fatalf("looking up the PUBREL consumer: %v", err)
	}
	return ci != nil
}

// testPubRelRepairReasons records every repair decision while the test runs. Not parallel: the
// seam is package-global.
func testPubRelRepairReasons(t *testing.T) func() []string {
	t.Helper()
	var mu sync.Mutex
	var got []string
	h := func(reason string) {
		mu.Lock()
		got = append(got, reason)
		mu.Unlock()
	}
	mqttTestPubRelRepairV.Store(&h)
	t.Cleanup(func() { mqttTestPubRelRepairV.Store(nil) })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...)
	}
}

// T12 (M8): an enrolled re-materialisation continuation whose PUBREL consumer is deleted before
// its first attempt recreates the consumer and rebinds, without another CONNECT.
func TestF105RematerializationRecreatesAnAbsentPubRelConsumer(t *testing.T) {
	reasons := testPubRelRepairReasons(t)
	e, _, subject, _, canon := testPubRelReadyWorld(t, "settle-t12")
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, canon, true); err != nil {
		t.Fatalf("delete the canonical PUBREL: %v", err)
	}
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	// The continuation's first attempt is scheduled; the consumer goes before it runs.
	if _, err := e.asm.jsa.deleteConsumer(mqttOutStreamName, testPubRelDurable(e), false); err != nil {
		t.Fatalf("deleting the PUBREL consumer: %v", err)
	}
	testAwaitRebind(t, e, subject, canon)
	repaired := false
	for _, r := range reasons() {
		if r == mqttPubRelRepaired {
			repaired = true
		}
	}
	if !repaired {
		t.Fatalf("the rebind must have been reached through the repair arm, got %v", reasons())
	}
	if !testPubRelPresent(t, e) {
		t.Fatal("the PUBREL consumer must exist again")
	}
}

// T13 and T14 (M10): the repair arm and the ensure refuse every not-repairable cause, each
// reporting why, and repair the one repairable one.
func TestF105PubRelRepairRefusesWhatItMayNotRepair(t *testing.T) {
	reasons := testPubRelRepairReasons(t)
	e := testNewInflightEnv(t, "settle-t13")
	e.subscribePubRel(t)
	epoch, gen, dur := e.asm.sessStreamEpoch, e.sess.gen, testPubRelDurable(e)

	// Present: nothing to repair, nothing created.
	e.sess.repairAbsentPubRelConsumer(e.c, epoch, gen, dur)
	if got := reasons(); len(got) != 1 || got[0] != mqttPubRelRepairPresent {
		t.Fatalf("a present consumer must be declined as present, got %v", got)
	}

	// Absent under a LIVE, undecided resource: repaired (the positive control for both).
	if _, err := e.asm.jsa.deleteConsumer(mqttOutStreamName, dur, false); err != nil {
		t.Fatalf("delete: %v", err)
	}
	e.sess.repairAbsentPubRelConsumer(e.c, epoch, gen, dur)
	if got := reasons(); got[len(got)-1] != mqttPubRelRepaired || !testPubRelPresent(t, e) {
		t.Fatalf("an absent consumer under a serving resource must be recreated, got %v", got)
	}

	// Absent under a LIVE, undecided resource, on a stream that imposes a consumer inactivity
	// limit: declined on the STREAM, and nothing created. The server substitutes the stream's limit
	// for a consumer requesting none, so a recreated consumer would be reapable under the very
	// obligation it is repaired for; the consumer's absence says nothing about the stream's
	// suitability. Lifting the limit makes the same absence repairable again, so the limit is the
	// only cause of the refusal.
	si, serr := e.asm.jsa.lookupStream(mqttOutStreamName)
	if serr != nil || si == nil {
		t.Fatalf("lookupStream(%q): %v", mqttOutStreamName, serr)
	}
	limited := si.Config
	limited.ConsumerLimits.InactiveThreshold = 300 * time.Millisecond
	if _, err := e.asm.jsa.updateStream(&limited); err != nil {
		t.Fatalf("could not impose a consumer inactivity limit on %q: %v", mqttOutStreamName, err)
	}
	if _, err := e.asm.jsa.deleteConsumer(mqttOutStreamName, dur, false); err != nil {
		t.Fatalf("delete: %v", err)
	}
	e.sess.repairAbsentPubRelConsumer(e.c, epoch, gen, dur)
	if got := reasons(); got[len(got)-1] != mqttPubRelRepairStreamLimit || testPubRelPresent(t, e) {
		t.Fatalf("an absent consumer on a stream imposing an inactivity limit must be declined as %q "+
			"and nothing created, got %v (present=%v)", mqttPubRelRepairStreamLimit, got, testPubRelPresent(t, e))
	}
	unlimited := si.Config
	unlimited.ConsumerLimits.InactiveThreshold = 0
	if _, err := e.asm.jsa.updateStream(&unlimited); err != nil {
		t.Fatalf("could not lift the consumer inactivity limit on %q: %v", mqttOutStreamName, err)
	}
	e.sess.repairAbsentPubRelConsumer(e.c, epoch, gen, dur)
	if got := reasons(); got[len(got)-1] != mqttPubRelRepaired || !testPubRelPresent(t, e) {
		t.Fatalf("with the stream limit lifted the same absence must be repaired, got %v", got)
	}

	// Decided: the repair declines on the resource, and the ensure itself refuses to recreate.
	own, _, _ := e.asm.jsa.loadNameOwnershipPhysical(epoch, mqttOwnedStreamOut, dur)
	if _, err := e.asm.jsa.opRequestDisposal(&mqttRequestDisposalRequest{ID: e.sess.id, Epoch: epoch,
		Txn: mqttNewInflightNonce(), Gen: own.OwnerGen, Inc: own.OwnerInc, Reason: mqttDisposalUnsubscribed,
		Entitlement: testServingEntitlement(t, e.sess)}); err != nil {
		t.Fatalf("deciding the PUBREL resource: %v", err)
	}
	if _, err := e.asm.jsa.deleteConsumer(mqttOutStreamName, dur, false); err != nil {
		t.Fatalf("delete: %v", err)
	}
	e.sess.repairAbsentPubRelConsumer(e.c, epoch, gen, dur)
	if got := reasons(); got[len(got)-1] != mqttPubRelRepairResource || testPubRelPresent(t, e) {
		t.Fatalf("a decided resource must be declined and nothing created, got %v", got)
	}
	e.sess.mu.Lock()
	e.sess.pubRelVerified = false
	e.sess.mu.Unlock()
	if err := e.sess.ensurePubRelConsumerSubscription(e.c); err == nil {
		t.Fatal("the ensure must refuse to rematerialize a decided PUBREL resource")
	}
	if testPubRelPresent(t, e) {
		t.Fatal("the ensure recreated a decided consumer")
	}
}

// H-1: a handoff that PREPARES over the settling tenure and is then CANCELLED does not abandon the
// settlement. The owner's disposal meets PREPARING, is withheld rather than dropped, and lands
// under the same tenure once the cancellation restores it — with no further CONNECT.
func TestF105CancelledHandoffDoesNotAbandonTheSettlement(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "settle-cancel")
	cl := testClaimedSourceConsumer(t, e, "settle.cancel")
	ent := testResumeUnderNewTenure(t, e)
	jsa := &e.asm.jsa
	if _, err := jsa.opPrepareHandoff(e.sess.id, e.asm.sessStreamEpoch, "txn-prep-cancel", &mqttHandoffIntent{
		ID: "intent-cancel", Gen: ent.Gen, Tenure: mqttNewInflightNonce(),
		PrevSessWID: "wid-0", PrevSessSeq: 12,
	}); err != nil {
		t.Fatalf("a remote CONNECT's preparation: %v", err)
	}
	var withheld atomic.Int32
	stepHook := func(k mqttRetainedReclaimKey, _ mqttReclaimStep, err error) {
		if k.src && errors.Is(err, errMQTTDisposalEntitlementPreparing) {
			withheld.Add(1)
		}
	}
	e.sess.testReclaimAfterStep.Store(&stepHook)
	if c := testSettleClasses(t, e, ent)[cl.name]; c != mqttSettleSupersededUndecide {
		t.Fatalf("the settlement must classify through a preparation, got %q", c)
	}
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if withheld.Load() == 0 {
			return errors.New("the owner has not met the preparation yet")
		}
		return nil
	})
	if r := testLoadResource(t, e, cl.gen, cl.inc); r.Disposal != nil {
		t.Fatalf("a decision landed during the preparation: %+v", r.Disposal)
	}
	// The remote CONNECT's metadata half is positively ABORTED, which is what licenses the cancel.
	pl := jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	prop, perr := mqttProposalFromHandoff(pl.rec.Intent)
	if perr != nil {
		t.Fatalf("viewing the intent as a proposal: %v", perr)
	}
	if v := jsa.settleProposal(_EMPTY_, e.sess.id, e.asm.sessStreamEpoch, prop); v != mqttMetadataAborted {
		t.Fatalf("settling the remote proposal returned %v, want ABORTED", v)
	}
	if _, err := jsa.opCancelHandoff(e.sess.id, e.asm.sessStreamEpoch, "txn-cancel", "intent-cancel"); err != nil {
		t.Fatalf("cancelling the preparation: %v", err)
	}
	testAwaitRetired(t, e, cl.gen, cl.inc, cl.name, mqttDisposalTenureSuperseded, ent.Tenure)
}
