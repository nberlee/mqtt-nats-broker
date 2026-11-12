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
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// issue #107 8b through actual CONNECTs and teardowns: one tenure per acceptance, installation on
// every acceptance, the closing driver's order and withhold, the adoption's settlement and its
// receiptless continuation, and P2. Mutation ledger: mutation-ledger-8b.md.

// test8bTenureOf returns the resident session's tenure, the order its acceptance holds in the
// lifecycle read independently from the authority (issue #133: the acceptance ordinal plus one),
// its entitlement, and its record sequence.
func test8bTenureOf(t *testing.T, s *Server, cid string) (mqttSharedTenure, uint64, mqttOutboundEntitlement, uint64) {
	t.Helper()
	asm, sess := testResidentSession(t, s, cid)
	sess.mu.Lock()
	tenure, ent, recSeq := sess.sharedTenure, sess.boundEnt, sess.seq
	sess.mu.Unlock()
	al := asm.jsa.loadOutboundAuthority(cid, testAccountEpoch(asm))
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
		t.Fatalf("reading the acceptance back from the authority: %v", al.status)
	}
	return tenure, al.rec.Life.Accept.Ord + 1, ent, recSeq
}

// test8bAssertOneTenure asserts the consolidation: the ledger's standing tenure is the
// session's, its ID is the authority tenure the connection was accepted under, and its order is
// the acceptance's own place in its lifecycle — never the session record's sequence.
func test8bAssertOneTenure(t *testing.T, s *Server, asm *mqttAccountSessionManager, cid, epoch string) {
	t.Helper()
	tenure, acceptOrder, ent, recSeq := test8bTenureOf(t, s, cid)
	if tenure.ID != ent.Tenure {
		t.Fatalf("the ledger tenure's ID %q is not the authority tenure %q the connection was accepted under", tenure.ID, ent.Tenure)
	}
	if tenure.Seq != acceptOrder {
		t.Fatalf("the ledger tenure's order %d is not the acceptance's order %d", tenure.Seq, acceptOrder)
	}
	if tenure.Seq == recSeq {
		t.Fatalf("the ledger tenure's order %d is the session record's sequence", tenure.Seq)
	}
	// The ledger is lazy (issue #133): the first acquisition installs it, and the install's
	// authority fence admits only the current acceptance's tenure. Install the session's own.
	if _, err := asm.jsa.installSharedTenure(cid, epoch, ent.Gen, tenure); err != nil {
		t.Fatalf("installing the session's tenure %+v: %v", tenure, err)
	}
	l := asm.jsa.loadSharedLedger(cid, epoch, ent.Gen)
	if l.status != mqttOutboundValid || l.rec.Tenure != tenure {
		t.Fatalf("the generation's ledger must stand under the connection's tenure %+v, it reads %v %+v", tenure, l.status, l.rec)
	}
}

// T1: ONE tenure per acceptance, installed, for a fresh client and for its resume. Mutations S1a
// (the ledger ID minted as its own nonce) and S1b (the order taken from the session record).
func Test8bOneTenurePerAcceptance(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t1"
	ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testAccountEpoch(asm)
	test8bAssertOneTenure(t, s, asm, cid, epoch)
	first, _, _, _ := test8bTenureOf(t, s, cid)
	c.Close()
	time.Sleep(150 * time.Millisecond)
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	test8bAssertOneTenure(t, s, asm, cid, epoch)
	second, _, _, _ := test8bTenureOf(t, s, cid)
	if second.Seq <= first.Seq || second.ID == first.ID {
		t.Fatalf("the resume's tenure %+v must succeed the first %+v", second, first)
	}
}

// T2: after a LOST RESPONSE to the metadata commit — the batch landed, its acknowledgement did
// not, and the acceptance adopted it from the resolved commit — the tenure is still the
// acceptance fact's. Mutation S1b.
func Test8bTenureSurvivesALostResponse(t *testing.T) {
	const cid = "f105-8b-t2"
	s, asm, epoch, _, done := testCleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	var armed atomic.Bool
	armed.Store(true)
	restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
		if obs.Phase == mqttAcceptStoreAcked && armed.CompareAndSwap(true, false) {
			return errors.New("f105 8b test: the metadata batch landed and its acknowledgement was lost")
		}
		return nil
	})
	defer restore()
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	if armed.Load() {
		t.Fatal("the lost response was never injected, so nothing here is tested")
	}
	test8bAssertOneTenure(t, s, asm, cid, epoch)
}

// T3: an acceptance of a generation that never acquired installs NOTHING (issue #133, the lazy
// shared ledger): no SUBSCRIBE runs, so no ledger stands and the authority records that the
// generation did not acquire; its first shared acquisition will create the ledger. Mutation: the
// installation run at CONNECT over absence (a ledger appears).
func Test8bAnAcceptanceThatNeverAcquiredHasNoLedger(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t3"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testAccountEpoch(asm)
	auth := testServingAt(t, asm, cid, epoch)
	if l := asm.jsa.loadSharedLedger(cid, epoch, auth.Gen); l.status != mqttOutboundAbsent {
		t.Fatalf("an acceptance that never acquired left a shared ledger reading %v %+v", l.status, l.rec)
	}
	if auth.Life.GenAcqs {
		t.Fatal("an acceptance that never acquired records that its generation acquired")
	}
}

// T7, the order witness for S2: on the owner's teardown of a SERVING generation the closing
// driver reaches its seal only once the generation's closure stands — the admission fence lands
// before the join fence, so no admission can win in an interval with the ledger sealed and the
// authority still serving. Checked from the durable records at the driver's fence point, before
// the closer is let go. Mutation S2 (the driver run before the receipted close).
func Test8bClosureLandsBeforeTheSeal(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t7"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testAccountEpoch(asm)
	gen := testServingAt(t, asm, cid, epoch).Gen
	// The ledger is lazy: only a generation that acquired has one to seal.
	testInstallFixtureTenure(t, testMQTTResidentSession(t, s, cid))
	var reached atomic.Bool
	var verdict atomic.Pointer[string]
	// Observed at EVERY seal composition, wherever it is composed: the property is about the
	// seal, not about the driver function that is meant to compose it.
	hook := func(shape mqttSharedShape, _ string) {
		if shape != mqttSharedShapeSeal || !reached.CompareAndSwap(false, true) {
			return
		}
		msg := "ok"
		if !testClosureDiscards(asm, cid, epoch, gen) {
			msg = "the ledger seal was composed before the generation's discarding closure stood"
		} else if al := asm.jsa.loadOutboundAuthority(cid, epoch); al.status == mqttOutboundValid && al.rec != nil &&
			al.rec.State == mqttAuthorityServing && al.rec.Gen == gen {
			msg = "the ledger seal was composed with the authority still serving the generation"
		}
		verdict.Store(&msg)
	}
	mqttTestSharedComposeHookV.Store(&hook)
	defer mqttTestSharedComposeHookV.Store(nil)
	c.Close()
	checkFor(t, 3*asm.jsa.timeout, 25*time.Millisecond, func() error {
		if !testRecordGone(asm, cid) {
			return errors.New("the session record still stands")
		}
		return nil
	})
	if !reached.Load() {
		t.Fatal("the teardown never composed a seal")
	}
	if v := verdict.Load(); v == nil || *v != "ok" {
		t.Fatal(*v)
	}
}

// test8bMarked reports the client's record as a teardown marker, with its sequence.
func test8bMarked(asm *mqttAccountSessionManager, cid string) (*mqttPersistedSession, uint64, bool) {
	smsg, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(cid)))
	if err != nil || smsg == nil {
		return nil, 0, false
	}
	ps := &mqttPersistedSession{}
	if json.Unmarshal(smsg.Data, ps) != nil || ps.Teardown == nil {
		return nil, 0, false
	}
	return ps, smsg.Sequence, true
}

// test8bDivergeAffine makes the resident object's in-memory affine target differ from what
// its record names, the divergence P2 exists for.
func test8bDivergeAffine(t *testing.T, asm *mqttAccountSessionManager, cid string) (string, string) {
	t.Helper()
	sess := testResidentObject(asm, cid)
	if sess == nil {
		t.Fatalf("the session for %q must be resident", cid)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.q2AffGen = mqttNewSessGen()
	sess.q2AffConsumer = &ConsumerConfig{Durable: mqttQ2AffineConsumerDurablePrefix + "memoryonly"}
	return sess.q2AffGen, sess.q2AffConsumer.Durable
}

// T15: P2. A clean start over a resident object whose memory names an affine target its record
// does not: the finalization's discarding closure enrols what the DISPLACED RECORD names, and the
// CONNECT succeeds. Mutation P2 (the target taken from memory).
func Test8bCleanStartRetiresWhatTheRecordNames(t *testing.T) {
	const cid = "f105-8b-t15"
	s, asm, epoch, oldGen, done := testCleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	testHoldResident(t, asm, cid)
	memGen, memDur := test8bDivergeAffine(t, asm, cid)
	var retire atomic.Pointer[mqttRetirementRequest]
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Retire != nil && tr.Retire.Gen == oldGen {
			r := *tr.Retire
			retire.Store(&r)
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	if rec := testServingAt(t, asm, cid, epoch); rec.Gen == oldGen {
		t.Fatalf("the clean start must hand off to a fresh generation")
	}
	r := retire.Load()
	if r == nil {
		t.Fatal("the clean start's handoff carried no retirement")
	}
	if r.AffineGen == memGen || r.AffineDurable == memDur {
		t.Fatalf("the retirement named the in-memory affine target %q/%q, not the displaced record's", memGen, memDur)
	}
}

// T16: a record that MOVED between the discard boundary and P2's read proposes no handoff from
// the stale object: the clean start takes the discard arm, whose superseded claim re-drives
// against the record that won, and the CONNECT is accepted (the re-drive machinery itself is
// TestSessSupersededDiscardReDrivesTheCleanStart's). Here: the handoff that finally lands names
// the winner's targets, never the stale object's.
func Test8bMovedDisplacedRecordProposesNoStaleHandoff(t *testing.T) {
	const cid = "f105-8b-t16"
	s, asm, epoch, oldGen, done := testCleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	testHoldResident(t, asm, cid)
	memGen, memDur := test8bDivergeAffine(t, asm, cid)
	var moved atomic.Bool
	settle := func(id string, _ int) {
		if id != cid || !moved.CompareAndSwap(false, true) {
			return
		}
		smsg, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(cid)))
		if err != nil {
			t.Errorf("reading the record to move it: %v", err)
			return
		}
		if _, err := mqttStoreSessionRecordCASBody(&asm.jsa, asm.domainTk, smsg.Data, getHash(cid), smsg.Sequence); err != nil {
			t.Errorf("moving the record: %v", err)
		}
	}
	testArmBoundarySettle(t, asm, settle)
	var stale atomic.Bool
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Retire != nil &&
			(tr.Retire.AffineGen == memGen || tr.Retire.AffineDurable == memDur) {
			stale.Store(true)
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	if !moved.Load() {
		t.Fatal("the record was never moved, so nothing here is tested")
	}
	if stale.Load() {
		t.Fatal("a handoff was proposed carrying the stale object's in-memory targets")
	}
	if rec := testServingAt(t, asm, cid, epoch); rec.Gen == oldGen {
		t.Fatal("the clean start must end serving a fresh generation")
	}
}

// T15b (H-1): an UNREADABLE displaced record is not confirmed displacement. With memory and
// record diverging, P2's read fails once: the clean start refuses BEFORE the detach (the
// incumbent is untouched and nothing is claimed or retired), and the client's retry hands off
// naming the record's targets. Falling through to the discard instead would let its claim win at
// the unmoved sequence and retire what memory names. Mutation P2-unreadable (a read error treated
// as a moved record).
func Test8bUnreadableDisplacedRecordRefusesBeforeDetach(t *testing.T) {
	const cid = "f105-8b-t15b"
	s, asm, epoch, oldGen, done := testCleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	testHoldResident(t, asm, cid)
	memGen, memDur := test8bDivergeAffine(t, asm, cid)
	sessSubj := mqttSessSubject(asm.domainTk, getHash(cid))
	var armed, dropped atomic.Bool
	drop := func(_ string, body []byte) bool {
		if armed.Load() && strings.Contains(string(body), sessSubj) && dropped.CompareAndSwap(false, true) {
			armed.Store(false)
			return true
		}
		return false
	}
	asm.jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() { asm.jsa.testDropRequest.Store(nil) })
	var settled atomic.Bool
	testArmBoundarySettle(t, asm, func(id string, _ int) {
		if id == cid && settled.CompareAndSwap(false, true) {
			armed.Store(true)
		}
	})
	var retired atomic.Pointer[mqttRetirementRequest]
	var claimed atomic.Bool
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionTeardownClaim {
			claimed.Store(true)
		}
		if tr.Retire != nil && tr.Retire.Gen == oldGen {
			r := *tr.Retire
			retired.Store(&r)
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
	ci := &mqttV5ConnInfo{clientID: cid, cleanStart: true}
	testRefusedConnectV5(t, o, ci)
	if !dropped.Load() {
		t.Fatal("the P2 read was never failed, so nothing here is tested")
	}
	if claimed.Load() || retired.Load() != nil {
		t.Fatal("a clean start whose displaced record could not be read went on to retire the generation")
	}
	if rec := testServingAt(t, asm, cid, epoch); rec.Gen != oldGen {
		t.Fatalf("the refused clean start moved the authority to %q", rec.Gen)
	}
	if _, _, marked := test8bMarked(asm, cid); marked {
		t.Fatal("the refused clean start claimed the record")
	}
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	r := retired.Load()
	if r == nil {
		t.Fatal("the retry's handoff carried no retirement")
	}
	if r.AffineGen == memGen || r.AffineDurable == memDur {
		t.Fatalf("the retry retired the in-memory affine target %q/%q", memGen, memDur)
	}
}
