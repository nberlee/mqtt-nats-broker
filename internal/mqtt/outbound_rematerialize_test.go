// Copyright 2025 The mqtt-nats-broker Authors
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
	"encoding/json"
	"testing"
	"time"
)

// A VANISHED CANONICAL PUBREL IS RE-MATERIALISED BY REBIND (F-105 7b, plan-7b §3.8, T16). An
// ordinary pubrel_ready slot whose exchange still reads PubRecObserved and whose named canonical
// record is gone is never re-phased: a new canonical record is published and the slot's tuple is
// rebound onto it in one authority batch. Nothing else about the obligation changes.

// testPubRelReadyWorld drives an admitted QoS 2 delivery to pubrel_ready through route B and
// returns it with its canonical sequence.
func testPubRelReadyWorld(t *testing.T, clientID string) (*testInflightEnv, uint16, string, string, uint64) {
	t.Helper()
	e, pi, subject, lin := testAdmittedPublishWorld(t, clientID, testRealPubRelSrcSeq)
	canon := testStoreCanonical(t, e, pi, e.nonceOf(pi), testRealPubRelSrcSeq)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("route B: %v", err)
	}
	if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPubRelReady || slot.PubRelStreamSeq != canon {
		t.Fatalf("fixture: want pubrel_ready naming %d, got %+v", canon, slot)
	}
	return e, pi, subject, lin, canon
}

// T16 (M23): the rebound slot names a readable canonical record; the lineage, the exchange's
// phase, the inventory's Hi and the custody charge are all unchanged.
func TestF105VanishedCanonicalPubRelIsRebound(t *testing.T) {
	t.Parallel()
	e, pi, subject, lin, canon := testPubRelReadyWorld(t, "remat-rebind")
	led, _, err := e.asm.jsa.loadGenLedger(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen)
	if err != nil || led == nil {
		t.Fatalf("loading the ledger: %v", err)
	}
	hiBefore := led.Hi
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, canon, true); err != nil {
		t.Fatalf("delete the canonical PUBREL: %v", err)
	}
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	var slot *mqttInflightRecord
	deadline := time.Now().Add(10 * time.Second)
	for {
		slot = testSlotAt(t, e, subject)
		if slot != nil && slot.PubRelStreamSeq != canon && slot.PubRelStreamSeq != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the vanished canonical PUBREL was never re-materialised: %+v", slot)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if slot.State != mqttInflightPubRelReady || slot.Lin != lin {
		t.Fatalf("the rebind changes the tuple only: %+v", slot)
	}
	rec, err := e.asm.jsa.loadMsg(mqttOutStreamName, slot.PubRelStreamSeq)
	if err != nil || rec == nil {
		t.Fatalf("the rebound tuple must name a readable canonical record: %v", err)
	}
	if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePubRecObserved {
		t.Fatalf("the exchange must still read PUBREC-observed, got %d", got)
	}
	led, _, err = e.asm.jsa.loadGenLedger(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen)
	if err != nil || led == nil || led.Hi != hiBefore {
		t.Fatalf("re-materialisation must allocate no ordinal: Hi %d -> %+v (%v)", hiBefore, led, err)
	}
	e.sess.mu.Lock()
	held, charged := e.sess.custody.holdsPI(pi), e.sess.custody.chargesPI(pi)
	e.sess.mu.Unlock()
	if !held || !charged {
		t.Fatalf("the obligation's custody must be unchanged: held=%v charged=%v", held, charged)
	}
}

// testRemat builds the re-materialisation of the slot on subject, as recovery enrols it, under
// lic.
func testRemat(t *testing.T, e *testInflightEnv, subject string, lic mqttDestroyLicence) *mqttRematerialization {
	t.Helper()
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the slot: %v", err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	if ref == nil {
		t.Fatalf("no slot reference for %+v", cur)
	}
	return &mqttRematerialization{slot: *ref, oldSeq: cur.PubRelStreamSeq, lin: cur.Lin, lic: lic}
}

// testPubRelRecords counts the records standing on the session's PUBREL subject.
func testPubRelRecords(t *testing.T, e *testInflightEnv) int {
	t.Helper()
	n := 0
	for seq := uint64(1); ; {
		smsg, err := e.asm.jsa.loadNextMsgFromSeq(mqttOutStreamName, e.sess.pubRelSubject, seq)
		if err != nil {
			return n
		}
		seq = smsg.Sequence + 1
		n++
	}
}

// testDropCanonicalEdge rewrites the PUBREL resource without this exchange's canonical-pubrel
// edge: the resource no longer carries the exchange, while the physical consumer stands.
func testDropCanonicalEdge(t *testing.T, e *testInflightEnv, lin string) {
	t.Helper()
	epoch, gen := e.asm.sessStreamEpoch, e.sess.gen
	_, _, dur := mqttPubRelNames(e.sess.idHash, gen)
	inc, ok := e.sess.readyDependencyInc(e.c, mqttOwnedStreamOut, mqttOutStreamName, dur, epoch, gen)
	if !ok {
		t.Fatal("fixture: the PUBREL resource must be ready")
	}
	r, _, err := e.asm.jsa.loadOutboundResource(e.sess.id, epoch, gen, inc)
	if err != nil || r == nil {
		t.Fatalf("loading the PUBREL resource: %v", err)
	}
	next := *r
	next.Deps = nil
	for _, d := range r.Deps {
		if !(d.Role == mqttRoleCanonicalPubRel && d.Nonce == lin) {
			next.Deps = append(next.Deps, d)
		}
	}
	body, _ := json.Marshal(&next)
	if _, err := e.asm.jsa.storeMsg(mqttResourceSubject(e.sess.idHash, gen, inc), -1, body); err != nil {
		t.Fatalf("rewriting the resource: %v", err)
	}
}

// T16 (M25, M26): without the resource carrying this exchange, nothing is published — the
// physical consumer still standing is not eligibility — and the slot is never re-phased: it stays
// pubrel_ready on its old tuple, which is what keeps the ordinary PUBCOMP arm applicable whenever
// the client's answer arrives.
func TestF105RematerializationWithheldWithoutTheResourceEdge(t *testing.T) {
	t.Parallel()
	e, pi, subject, lin, canon := testPubRelReadyWorld(t, "remat-withheld")
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, canon, true); err != nil {
		t.Fatalf("delete the canonical PUBREL: %v", err)
	}
	testDropCanonicalEdge(t, e, lin)
	before := testPubRelRecords(t, e)
	if got := e.sess.rematerializeOnce(e.c, testRemat(t, e, subject, mqttLicenceOf(e.c))); got != mqttRematerializeRetry {
		t.Fatalf("a resource that does not carry the exchange must withhold, got %d", got)
	}
	if after := testPubRelRecords(t, e); after != before {
		t.Fatalf("a withheld re-materialisation must publish nothing: %d -> %d records", before, after)
	}
	if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPubRelReady || slot.PubRelStreamSeq != canon {
		t.Fatalf("the slot must stay pubrel_ready on its old tuple: %+v", slot)
	}
	_ = pi
}

// T16 (race): two materializers racing land ONE rebind; the loser's record is disposed.
func TestF105RacingMaterializersLandOneRebind(t *testing.T) {
	t.Parallel()
	e, _, subject, _, canon := testPubRelReadyWorld(t, "remat-race")
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, canon, true); err != nil {
		t.Fatalf("delete the canonical PUBREL: %v", err)
	}
	a, b := testRemat(t, e, subject, mqttLicenceOf(e.c)), testRemat(t, e, subject, mqttLicenceOf(e.c))
	done := make(chan mqttRematerializeOutcome, 2)
	go func() { done <- e.sess.rematerializeOnce(e.c, a) }()
	go func() { done <- e.sess.rematerializeOnce(e.c, b) }()
	<-done
	<-done
	slot := testSlotAt(t, e, subject)
	if slot == nil || slot.PubRelStreamSeq == canon || slot.PubRelStreamSeq == 0 {
		t.Fatalf("one rebind must land: %+v", slot)
	}
	if n := testPubRelRecords(t, e); n != 1 {
		t.Fatalf("exactly the canonical record must stand, got %d records", n)
	}
	if rec, err := e.asm.jsa.loadMsg(mqttOutStreamName, slot.PubRelStreamSeq); err != nil || rec == nil {
		t.Fatalf("the rebound tuple must name the standing record: %v", err)
	}
}

// T16 (M22): a materializer whose connection was superseded does not rebind: the successor's
// recovery owns the decision. Its published candidate is disposed and the slot is unchanged.
func TestF105StaleMaterializerDoesNotRebind(t *testing.T) {
	t.Parallel()
	e, _, subject, _, canon := testPubRelReadyWorld(t, "remat-stale")
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, canon, true); err != nil {
		t.Fatalf("delete the canonical PUBREL: %v", err)
	}
	stale := mqttLicenceOf(e.c)
	testSupersedeTenure(t, e)
	if got := e.sess.rematerializeOnce(e.c, testRemat(t, e, subject, stale)); got != mqttRematerializeDone {
		t.Fatalf("a superseded materializer must stand down, got %d", got)
	}
	if slot := testSlotAt(t, e, subject); slot == nil || slot.PubRelStreamSeq != canon {
		t.Fatalf("the stale materializer must not rebind: %+v", slot)
	}
	if n := testPubRelRecords(t, e); n != 0 {
		t.Fatalf("the stale materializer's candidate must be disposed, got %d records", n)
	}
}

// T16 (M29b): candidate cleanup never deletes the canonical record. Called with a candidate the
// register currently names, the cleanup must leave it standing.
func TestF105CandidateCleanupSparesTheCanonicalRecord(t *testing.T) {
	t.Parallel()
	e, _, subject, _, canon := testPubRelReadyWorld(t, "remat-cleanup")
	e.sess.disposeRematerializeCandidate(subject, e.sess.pubRelSubject, canon)
	if rec, err := e.asm.jsa.loadMsg(mqttOutStreamName, canon); err != nil || rec == nil {
		t.Fatalf("the canonical record the register names must survive candidate cleanup: %v", err)
	}
}

// T16 (M28): a PUBREL callback for the OLD record, paused after its canonical check while the
// rebind lands, is refused its acquisition: the delivered tuple is carried into the acquisition,
// and every refreshed base must still name it. The rebound record's own acquisition lands.
func TestF105AcquisitionCarriesTheDeliveredCanonicalTuple(t *testing.T) {
	t.Parallel()
	e, _, subject, lin, canon := testPubRelReadyWorld(t, "remat-tuple")
	// The callback for the old record captured its slot reference and passed its canonical check.
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil {
		t.Fatalf("loading the slot: %v", err)
	}
	stale := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	// Then the old record vanished and the rebind landed.
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, canon, true); err != nil {
		t.Fatalf("delete the canonical PUBREL: %v", err)
	}
	if got := e.sess.rematerializeOnce(e.c, testRemat(t, e, subject, mqttLicenceOf(e.c))); got != mqttRematerializeDone {
		t.Fatalf("fixture: the rebind must land, got %d", got)
	}
	rebound := testSlotAt(t, e, subject)
	if rebound == nil || rebound.PubRelStreamSeq == canon {
		t.Fatalf("fixture: the slot must be rebound: %+v", rebound)
	}
	if act, _ := e.sess.mqttAcquirePubRelAction(e.c, *stale, lin, canon); act != nil {
		t.Fatal("an acquisition for the old canonical record must be refused after the rebind")
	}
	if got := testSlotAt(t, e, subject); got == nil || got.ActionToken != _EMPTY_ || got.PubRelStreamSeq != rebound.PubRelStreamSeq {
		t.Fatalf("the refused acquisition must leave the rebound slot untouched: %+v", got)
	}
	_, nseq, _ := e.asm.loadInflightRecord(subject)
	fresh := mqttSlotRefFromRecord(subject, nseq.Sequence, rebound)
	if act, _ := e.sess.mqttAcquirePubRelAction(e.c, *fresh, lin, rebound.PubRelStreamSeq); act == nil {
		t.Fatal("the rebound record's own acquisition must land")
	}
}

// testAwaitRebind waits for the slot on subject to name a readable canonical record other than old.
func testAwaitRebind(t *testing.T, e *testInflightEnv, subject string, old uint64) *mqttInflightRecord {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		slot := testSlotAt(t, e, subject)
		if slot != nil && slot.PubRelStreamSeq != old && slot.PubRelStreamSeq != 0 {
			if rec, err := e.asm.jsa.loadMsg(mqttOutStreamName, slot.PubRelStreamSeq); err == nil && rec != nil {
				return slot
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the vanished canonical PUBREL was never rebound onto a readable record: %+v", slot)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// H7b-2 finding 2, the resident form: a SAME-OBJECT reconnect keeps the slot's PUBREL entry, and
// the vanished canonical record is re-materialised all the same.
//
// Mutation M-enrol-entry: enrolment only where a NEW entry is installed -> never rebound.
func TestF105ResidentReconnectRematerializesTheCanonicalPubRel(t *testing.T) {
	t.Parallel()
	e, pi, subject, _, canon := testPubRelReadyWorld(t, "remat-resident")
	e.sess.mu.Lock()
	resident := e.sess.pendingPubRel[pi] != nil
	e.sess.mu.Unlock()
	if !resident {
		t.Fatal("fixture: route B must leave the PUBREL entry resident")
	}
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, canon, true); err != nil {
		t.Fatalf("delete the canonical PUBREL: %v", err)
	}
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	testAwaitRebind(t, e, subject, canon)
}

// H7b-2 finding 2, the crash-cut form: a candidate record was published and the broker died
// before the rebind. The candidate is an identity match at ANOTHER sequence, which is not the
// named canonical record; the slot ends naming a readable record.
//
// This fixture converges by TWO routes, so it does not discriminate the resume's presence check
// alone: M-identity-presence (presence judged by identity) SURVIVES here, because the candidate's
// own PUBREL delivery meets the live admission, whose canonical-sequence check re-materialises
// from there. The test pins the outcome both routes owe.
func TestF105CandidateBeforeRebindCrashStillRematerializes(t *testing.T) {
	t.Parallel()
	e, pi, subject, _, canon := testPubRelReadyWorld(t, "remat-crash-cut")
	testStoreCanonical(t, e, pi, e.nonceOf(pi), testRealPubRelSrcSeq) // the orphaned candidate
	if err := e.asm.jsa.deleteMsg(mqttOutStreamName, canon, true); err != nil {
		t.Fatalf("delete the canonical PUBREL: %v", err)
	}
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	testAwaitRebind(t, e, subject, canon)
}
