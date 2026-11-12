//go:build mqtt_fulltests

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
	"testing"
	"time"
)

// testAcceptRecordMember is a session record member for an acceptance test: the boundary
// binds the subject, and the record's contents are the store boundary's, not the shape's.
func testAcceptRecordMember(e *testOutboundEnv, expect uint64, gen string) *mqttTransitionMember {
	return &mqttTransitionMember{
		subj:   mqttSessSubject(e.asm.domainTk, getHash(e.id)),
		expect: expect,
		body:   []byte(`{"id":"` + e.id + `","gen":"` + gen + `"}`),
	}
}

func testCommitAcceptance(t *testing.T, e *testOutboundEnv, r mqttAcceptRequest, recExpect uint64) (*mqttComposedAcceptance, uint64) {
	t.Helper()
	jsa := &e.asm.jsa
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	reg := jsa.loadLifecycleRegistryShard(e.epoch, mqttLifecycleRegistryShardFor(getHash(e.id)))
	// The record standing at the subject, if the environment wrote one: the composer is told, as
	// a CONNECT tells it from the record it restored.
	smsg, lerr := jsa.loadSessionMsg(e.asm.domainTk, getHash(e.id))
	r.RecordExists = lerr == nil && smsg != nil
	ca, err := jsa.composeAcceptance(r, al, reg)
	if err != nil {
		t.Fatalf("composing the acceptance: %v", err)
	}
	if recExpect == 0 && r.RecordExists {
		recExpect = smsg.Sequence
	}
	ca.T.Record = testAcceptRecordMember(e, recExpect, ca.Gen)
	seq, err := jsa.commitAuthorityTransition(ca.T)
	if err != nil {
		t.Fatalf("committing the %s acceptance: %v", ca.T.Attempt.Kind, err)
	}
	return ca, seq
}

func testAcceptReq(e *testOutboundEnv, n string, clean bool) mqttAcceptRequest {
	return mqttAcceptRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-" + n, Tenure: "tenure-" + n, Nonce: "nonce-" + n,
		At: time.Now().UnixNano(), CleanStart: clean, PriorExpiry: 60,
	}
}

// One batch per acceptance shape: the lifecycle is allocated with the creation, a resume keeps
// the generation, a Clean Start installs the next one and makes its predecessor outstanding,
// and a bind clears exactly the pending acceptance.
func TestAcceptanceShapesCommitOwnershipAndRecordAsOneBatch(t *testing.T) {
	e, done := testNewEnv(t, "accept-shapes")
	defer done()
	jsa := &e.asm.jsa

	ca, aseq := testCommitAcceptance(t, e, testAcceptReq(e, "1", false), 0)
	if ca.T.Attempt.Kind != mqttTransitionAcceptCreate {
		t.Fatalf("the first acceptance composed %s, want a creation", ca.T.Attempt.Kind)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.seq != aseq {
		t.Fatalf("authority after creation: %v at %d, the commit reported %d", al.status, al.seq, aseq)
	}
	l, ord, ok := mqttParseGenToken(al.rec.Gen)
	if !ok || l != al.rec.Life.L || ord != 0 || al.rec.Life.Pending == nil {
		t.Fatalf("creation installed generation %q (%d/%d) with lifecycle %+v", al.rec.Gen, l, ord, al.rec.Life)
	}
	reg := jsa.loadLifecycleRegistryShard(e.epoch, mqttLifecycleRegistryShardFor(getHash(e.id)))
	if reg.status != mqttOutboundValid || reg.rec.Active[getHash(e.id)] != l || reg.rec.Next != l+1 {
		t.Fatalf("registry after creation: %v %+v", reg.status, reg.rec)
	}
	recSeq := aseq + 1

	// A replay of the same creation is refused: the registry moved and the authority exists.
	if _, err := jsa.commitAuthorityTransition(ca.T); err == nil {
		t.Fatal("a replayed creation landed a second time")
	}

	// THE BIND clears exactly the pending acceptance.
	bt, err := mqttComposeBind(al.rec, al.seq, "txn-bind1", "nonce-1")
	if err != nil {
		t.Fatal(err)
	}
	bt.Record = testAcceptRecordMember(e, recSeq, ca.Gen)
	bseq, err := jsa.commitAuthorityTransition(bt)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	recSeq = bseq + 1

	// A RESUME keeps the generation.
	ca2, aseq2 := testCommitAcceptance(t, e, testAcceptReq(e, "2", false), recSeq)
	al = jsa.loadOutboundAuthority(e.id, e.epoch)
	if ca2.T.Attempt.Kind != mqttTransitionAcceptHandoff || al.rec.Gen != ca.Gen || al.rec.Life.Accept.Ord != 1 ||
		al.rec.Life.Pending.Kind != mqttPendingResume || al.seq != aseq2 {
		t.Fatalf("resume installed %+v / %+v", al.rec, al.rec.Life)
	}
	bt, _ = mqttComposeBind(al.rec, al.seq, "txn-bind2", "nonce-2")
	bt.Record = testAcceptRecordMember(e, aseq2+1, ca.Gen)
	bseq, err = jsa.commitAuthorityTransition(bt)
	if err != nil {
		t.Fatalf("bind after resume: %v", err)
	}

	// A CLEAN START installs the next generation and enrols its predecessor, discarded.
	ca3, _ := testCommitAcceptance(t, e, testAcceptReq(e, "3", true), bseq+1)
	al = jsa.loadOutboundAuthority(e.id, e.epoch)
	_, ord3, _ := mqttParseGenToken(al.rec.Gen)
	if ca3.Gen == ca.Gen || ord3 != 1 || len(al.rec.Life.Outstanding) != 1 || al.rec.Life.Outstanding[0].Ord != 0 ||
		al.rec.Life.Outstanding[0].Teardown.Kind != mqttTeardownHandoffDiscard {
		t.Fatalf("clean start installed %q with %+v", al.rec.Gen, al.rec.Life)
	}
	c, _, st := jsa.loadGenClosure(e.id, e.epoch, ca.Gen)
	if st != mqttOutboundValid {
		t.Fatalf("the predecessor's closure reads %v %+v", st, c)
	}
}

// A delay-capable acceptance allocates its Will's anchor ARMED; every later acceptance lowers the
// anchor's minAt to its own time and never raises it; the anchor's own transitions owe it with a
// fire time and remove it, and change nothing else.
func TestWillAnchorsTrackLaterAcceptances(t *testing.T) {
	e, done := testNewEnv(t, "will-anchors")
	defer done()
	jsa := &e.asm.jsa

	r1 := testAcceptReq(e, "1", false)
	r1.WillDelayCapable = true
	ca, aseq := testCommitAcceptance(t, e, r1, 0)
	if ca.Anchor == nil || ca.Anchor.State != mqttAnchorArmed {
		t.Fatalf("a delay-capable acceptance allocated no armed anchor: %+v", ca.Anchor)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	bt, _ := mqttComposeBind(al.rec, al.seq, "txn-b1", "nonce-1")
	bt.Record = testAcceptRecordMember(e, aseq+1, ca.Gen)
	bseq, err := jsa.commitAuthorityTransition(bt)
	if err != nil {
		t.Fatal(err)
	}

	// A later acceptance at a known time lowers minAt; a second one later still does not raise it.
	r2 := testAcceptReq(e, "2", false)
	r2.At = 1000
	_, aseq2 := testCommitAcceptance(t, e, r2, bseq+1)
	al = jsa.loadOutboundAuthority(e.id, e.epoch)
	if a := al.rec.Life.Anchors; len(a) != 1 || a[0].MinAt != 1000 {
		t.Fatalf("the later acceptance did not lower the anchor's minAt to its time: %+v", a)
	}
	bt, _ = mqttComposeBind(al.rec, al.seq, "txn-b2", "nonce-2")
	bt.Record = testAcceptRecordMember(e, aseq2+1, ca.Gen)
	bseq, err = jsa.commitAuthorityTransition(bt)
	if err != nil {
		t.Fatal(err)
	}
	r3 := testAcceptReq(e, "3", false)
	r3.At = 2000
	testCommitAcceptance(t, e, r3, bseq+1)
	al = jsa.loadOutboundAuthority(e.id, e.epoch)
	if a := al.rec.Life.Anchors; len(a) != 1 || a[0].MinAt != 1000 {
		t.Fatalf("a later acceptance raised the anchor's minAt: %+v", a)
	}

	// OWE, then REMOVE, each changing exactly the anchor.
	ot, err := mqttComposeAnchorTransition(al.rec, al.seq, "txn-owe", ca.Anchor.Ord, mqttAnchorOwe, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jsa.commitAuthorityTransition(ot); err != nil {
		t.Fatalf("owing the anchor: %v", err)
	}
	al = jsa.loadOutboundAuthority(e.id, e.epoch)
	if a := al.rec.Life.Anchors; len(a) != 1 || a[0].State != mqttAnchorOwed || a[0].FireAt != 5000 {
		t.Fatalf("the anchor was not owed: %+v", a)
	}
	bad := *ot
	bauth := *ot.Auth
	bauth.Tenure = "tenure-moved"
	bad.Auth = &bauth
	if err := mqttCheckAnchorTransitionRules(&bad); err == nil {
		t.Fatal("a will-anchor transition moved the tenure")
	}
	rt, err := mqttComposeAnchorTransition(al.rec, al.seq, "txn-rm", ca.Anchor.Ord, mqttAnchorRemove, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jsa.commitAuthorityTransition(rt); err != nil {
		t.Fatalf("removing the anchor: %v", err)
	}
	if al = jsa.loadOutboundAuthority(e.id, e.epoch); len(al.rec.Life.Anchors) != 0 || al.rec.Life.NextAnchorOrd != ca.Anchor.Ord+1 {
		t.Fatalf("the removed anchor still stands, or its ordinal was reissued: %+v", al.rec.Life)
	}
}

// testLifecycleAuthority is an authority of lifecycle 1 at generation ord, in state, with n
// outstanding generations below it, for a composer-level test that reads nothing else.
func testLifecycleAuthority(t *testing.T, state mqttAuthorityState, phase mqttLifecyclePhase, ord uint64, n int) *mqttOutboundAuthority {
	t.Helper()
	life := &mqttLifecycleState{
		L: 1, Phase: phase, NextGenOrd: ord + 1, NextAcceptOrd: 1, NextAnchorOrd: 1,
		Accept: mqttAcceptance{Ord: 0, At: 1, Tenure: "tenure-a", Nonce: "nonce-a"},
	}
	for i := 0; i < n; i++ {
		life.Outstanding = append(life.Outstanding, mqttOutstandingGen{
			Ord: uint64(i), Phase: mqttOutstandingOpen,
			Teardown: mqttOutstandingTeardown{Kind: mqttTeardownMarker, Nonce: "n" + string(rune('a'+i%26)) + "x", MarkerWID: "w"},
		})
	}
	return &mqttOutboundAuthority{
		ID: testID, Epoch: testEpoch, Gen: testLifecycleGen(t, 1, ord), Tenure: "tenure-a",
		State: state, Rev: 3, Txn: "txn-a", OwnerTxn: "txn-a", Life: life,
	}
}

func testComposeOver(prev *mqttOutboundAuthority, r mqttAcceptRequest) (*mqttComposedAcceptance, error) {
	r.ID, r.Epoch, r.Txn, r.Tenure, r.Nonce, r.At = testID, testEpoch, "txn-b", "tenure-b", "nonce-b", time.Now().UnixNano()
	return (&mqttJSA{}).composeAcceptance(r, mqttAuthorityLoad{rec: prev, seq: 7, status: mqttOutboundValid}, mqttRegistryLoad{})
}

// T-third-shape's composer arm (M24): the reopen composer, isolated from the boundary, composes
// nothing over a RETIRING lifecycle, and an acceptance over a SERVING authority whose record is
// absent is Inconsistent. A closed, released lifecycle with its record gone reopens, and a release
// whose delete landed but whose lifecycle-scoped half did not is discharged by the reopen.
func TestReopenComposerReadsTheLifecyclePhase(t *testing.T) {
	if _, err := testComposeOver(testLifecycleAuthority(t, mqttAuthorityClosed, mqttLifecycleRetiring, 0, 0), mqttAcceptRequest{}); err == nil {
		t.Fatal("a reopen was composed over a retiring lifecycle")
	}
	if _, err := testComposeOver(testLifecycleAuthority(t, mqttAuthorityServing, mqttLifecycleServing, 0, 0), mqttAcceptRequest{}); err == nil {
		t.Fatal("an acceptance was composed over a serving authority whose record is absent")
	}
	ca, err := testComposeOver(testLifecycleAuthority(t, mqttAuthorityClosed, mqttLifecycleClosed, 0, 0), mqttAcceptRequest{})
	if err != nil || ca.T.Attempt.Kind != mqttTransitionAcceptReopen {
		t.Fatalf("a released lifecycle with its record gone must reopen: %v", err)
	}
	pending := testLifecycleAuthority(t, mqttAuthorityClosed, mqttLifecycleServing, 0, 1)
	pending.Life.Outstanding[0].Teardown.Pending = true
	ca, err = testComposeOver(pending, mqttAcceptRequest{})
	if err != nil || ca.T.Attempt.Kind != mqttTransitionAcceptReopen {
		t.Fatalf("a reopen over a landed delete must compose: %v", err)
	}
	if ca.T.Auth.Life.Outstanding[0].Teardown.Pending {
		t.Fatal("the reopen left the release pending although its create-only record proves the record gone")
	}
}

// T-cap-reserve (M23): with K-1 generations outstanding beside the current one, a Clean Start is
// refused, because its result holds K+1; the invariant is checked on the RESULT, never the
// predecessor (which holds K and would pass).
func TestCleanStartIsRefusedAtTheGenerationCap(t *testing.T) {
	prev := testLifecycleAuthority(t, mqttAuthorityServing, mqttLifecycleServing, mqttMaxGenerationSlots-1, mqttMaxGenerationSlots-1)
	if _, err := testComposeOver(prev, mqttAcceptRequest{CleanStart: true, RecordExists: true}); err == nil {
		t.Fatal("a clean start was composed whose result exceeds the generation slots")
	}
	if err := mqttCheckGenerationSlots(len(prev.Life.Outstanding), true); err != nil {
		t.Fatalf("fixture: the predecessor itself must fit (%v), or this does not discriminate M23", err)
	}
}
