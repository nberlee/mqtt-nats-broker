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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// F-105 8b through actual CONNECTs and teardowns: one tenure per acceptance, installation on
// every acceptance, the closing driver's order and withhold, the adoption's settlement and its
// receiptless continuation, and P2. Mutation ledger: mutation-ledger-8b.md.

// testF105_8bTenureOf returns the resident session's tenure, its acceptance fact's stream
// sequence read independently by the fact's own subject, its entitlement, and its record
// sequence.
func testF105_8bTenureOf(t *testing.T, s *Server, cid string) (mqttSharedTenure, uint64, mqttOutboundEntitlement, uint64) {
	t.Helper()
	asm, sess := testResidentSession(t, s, cid)
	sess.mu.Lock()
	tenure, ref, ent, recSeq := sess.sharedTenure, sess.acceptFact, sess.boundEnt, sess.seq
	sess.mu.Unlock()
	if ref.Subj == _EMPTY_ {
		t.Fatal("the accepted session recorded no acceptance fact")
	}
	smsg, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, ref.Subj)
	if err != nil || smsg == nil {
		t.Fatalf("reading the acceptance fact back: %v", err)
	}
	return tenure, smsg.Sequence, ent, recSeq
}

// testF105_8bAssertOneTenure asserts the consolidation: the ledger's standing tenure is the
// session's, its ID is the authority tenure the connection was accepted under, and its order is
// the acceptance fact's own sequence — never the session record's.
func testF105_8bAssertOneTenure(t *testing.T, s *Server, asm *mqttAccountSessionManager, cid, epoch string) {
	t.Helper()
	tenure, factSeq, ent, recSeq := testF105_8bTenureOf(t, s, cid)
	if tenure.ID != ent.Tenure {
		t.Fatalf("the ledger tenure's ID %q is not the authority tenure %q the connection was accepted under", tenure.ID, ent.Tenure)
	}
	if tenure.Seq != factSeq {
		t.Fatalf("the ledger tenure's order %d is not the acceptance fact's sequence %d", tenure.Seq, factSeq)
	}
	if tenure.Seq == recSeq {
		t.Fatalf("the ledger tenure's order %d is the session record's sequence", tenure.Seq)
	}
	l := asm.jsa.loadSharedLedger(cid, epoch, ent.Gen)
	if l.status != mqttOutboundValid || l.rec.Tenure != tenure {
		t.Fatalf("the generation's ledger must stand under the connection's tenure %+v, it reads %v %+v", tenure, l.status, l.rec)
	}
}

// T1: ONE tenure per acceptance, installed, for a fresh client and for its resume. Mutations S1a
// (the ledger ID minted as its own nonce) and S1b (the order taken from the session record).
func TestF105_8bOneTenurePerAcceptance(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t1"
	ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testF105AccountEpoch(asm)
	testF105_8bAssertOneTenure(t, s, asm, cid, epoch)
	first, _, _, _ := testF105_8bTenureOf(t, s, cid)
	c.Close()
	time.Sleep(150 * time.Millisecond)
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	testF105_8bAssertOneTenure(t, s, asm, cid, epoch)
	second, _, _, _ := testF105_8bTenureOf(t, s, cid)
	if second.Seq <= first.Seq || second.ID == first.ID {
		t.Fatalf("the resume's tenure %+v must succeed the first %+v", second, first)
	}
}

// T2: after a LOST RESPONSE to the metadata commit — the batch landed, its acknowledgement did
// not, and the acceptance adopted it from the resolved commit — the tenure is still the
// acceptance fact's. Mutation S1b.
func TestF105_8bTenureSurvivesALostResponse(t *testing.T) {
	const cid = "f105-8b-t2"
	s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
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
	testF105_8bAssertOneTenure(t, s, asm, cid, epoch)
}

// T3: EVERY acceptance installs before it serves — a client with no record included. No
// SUBSCRIBE runs, so the installation is the only thing that can have created the ledger.
// Mutation M-install (the installation gated on a restored record).
func TestF105_8bEveryAcceptanceInstalls(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t3"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testF105AccountEpoch(asm)
	gen := testF105ServingAt(t, asm, cid, epoch).Gen
	l := asm.jsa.loadSharedLedger(cid, epoch, gen)
	if l.status != mqttOutboundValid || l.rec.State != mqttSharedLedgerOpen || len(l.rec.Acqs) != 0 {
		t.Fatalf("a fresh acceptance must install an open, empty ledger before it serves, it reads %v %+v", l.status, l.rec)
	}
}

// T7, the order witness for S2: on the owner's teardown of a SERVING generation the closing
// driver reaches its seal only once the generation's closure stands — the admission fence lands
// before the join fence, so no admission can win in an interval with the ledger sealed and the
// authority still serving. Checked from the durable records at the driver's fence point, before
// the closer is let go. Mutation S2 (the driver run before the receipted close).
func TestF105_8bClosureLandsBeforeTheSeal(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t7"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testF105AccountEpoch(asm)
	gen := testF105ServingAt(t, asm, cid, epoch).Gen
	var reached atomic.Bool
	var verdict atomic.Pointer[string]
	// Observed at EVERY seal composition, wherever it is composed: the property is about the
	// seal, not about the driver function that is meant to compose it.
	hook := func(shape mqttSharedShape, _ string) {
		if shape != mqttSharedShapeSeal || !reached.CompareAndSwap(false, true) {
			return
		}
		msg := "ok"
		if !testF105ClosureDiscards(asm, cid, epoch, gen) {
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
		if !testF105RecordGone(asm, cid) {
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

// testF105_8bMarked reports the client's record as a teardown marker, with its sequence.
func testF105_8bMarked(asm *mqttAccountSessionManager, cid string) (*mqttPersistedSession, uint64, bool) {
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

func testF105_8bReceipt(asm *mqttAccountSessionManager, cid, gen string) (*mqttSessTeardownReceipt, mqttOutboundLoadStatus) {
	r, _, st := asm.jsa.loadTeardownReceipt(asm.domainTk, getHash(cid), gen)
	return r, st
}

func testF105_8bLedgerSealed(asm *mqttAccountSessionManager, cid, epoch, gen string) bool {
	l := asm.jsa.loadSharedLedger(cid, epoch, gen)
	return l.status == mqttOutboundValid && l.rec.State == mqttSharedLedgerSealed
}

// testF105_8bResidue drives the T22 schedule on a session with the given expiry: an unresolved
// foreign preparation stands, its receipt subject cannot be answered, and a teardown runs —
// the disconnect for a zero expiry, the expiry itself otherwise. It returns the account, epoch,
// the retiring generation and the proposal, with the seam still armed.
func testF105_8bResidue(t *testing.T, s *Server, cid string, expiry uint32) (*mqttAccountSessionManager, string, string, *mqttHandoffIntent) {
	t.Helper()
	o := s.getOpts()
	ci := &mqttV5ConnInfo{clientID: cid}
	if expiry != 0 {
		ci.props = mqttV5ConnPropsSessionExpiry(expiry)
	}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testF105AccountEpoch(asm)
	gen := testF105ServingAt(t, asm, cid, epoch).Gen
	i := testF105PlantUnresolved(t, asm, cid, epoch, "8b-"+cid)
	receipt := mqttHandoffOutcomeSubject(_EMPTY_, getHash(cid), i.ID)
	drop := func(_ string, body []byte) bool { return strings.Contains(string(body), receipt) }
	asm.jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() { asm.jsa.testDropRequest.Store(nil) })
	c.Close()
	wait := 3 * asm.jsa.timeout
	if expiry != 0 {
		wait += time.Duration(expiry) * time.Second
	}
	checkFor(t, wait, 50*time.Millisecond, func() error {
		if _, _, marked := testF105_8bMarked(asm, cid); !marked {
			return errors.New("the record is not marked yet")
		}
		return nil
	})
	return asm, epoch, gen, i
}

// T9: THE RESIDUE, as consult 121 reshaped it. A store that cannot answer the boundary's
// settlement leaves the proposal standing; the teardown claims receiptless, and the closing
// driver has no licence: the record stays MARKED, no discarding closure, the ledger NOT sealed,
// the proposal still stands. Mutation M-seal-unlicensed.
func TestF105_8bReceiptlessTeardownWithholds(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t9"
	asm, epoch, gen, i := testF105_8bResidue(t, s, cid, 0)
	time.Sleep(2 * asm.jsa.timeout)
	if _, _, marked := testF105_8bMarked(asm, cid); !marked {
		t.Fatal("the withheld teardown must leave the record MARKED")
	}
	if _, st := testF105_8bReceipt(asm, cid, gen); st != mqttOutboundAbsent {
		t.Fatalf("the residue's teardown carries no receipt, it reads %v", st)
	}
	if testF105ClosureDiscards(asm, cid, epoch, gen) {
		t.Fatal("the residue must leave NO discarding closure")
	}
	if testF105_8bLedgerSealed(asm, cid, epoch, gen) {
		t.Fatal("an unlicensed closing SEALED the ledger with the generation still able to admit")
	}
	if standing, _ := testF105StandingProposal(t, asm, cid, epoch); standing != i.ID {
		t.Fatalf("the unsettled proposal %q must still stand, got %q", i.ID, standing)
	}
}

// T10: the receiptless continuation, reached by an EXPIRY adopter with no CONNECT ahead of it.
// The residue is created by the first expiry teardown; the store recovers; the record-based
// expiry adoption settles the preparation (it cancels back to SERVING at the retiring
// generation), authors the receipt with a CAS of the marker, closes discarding, seals and
// releases the record. Mutation M-cont (no continuation: the adoption stalls).
func TestF105_8bExpiryAdoptionContinuesReceiptless(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t10"
	asm, epoch, gen, i := testF105_8bResidue(t, s, cid, 1)
	if _, st := testF105_8bReceipt(asm, cid, gen); st != mqttOutboundAbsent {
		t.Fatalf("the first expiry teardown must be receiptless, the receipt reads %v", st)
	}
	if standing, _ := testF105StandingProposal(t, asm, cid, epoch); standing != i.ID {
		t.Fatalf("the proposal must still stand after the first teardown, got %q", standing)
	}
	if obj := testF105ResidentObject(asm, cid); obj != nil {
		asm.cancelSessionExpiry(getHash(cid))
		asm.removeSession(obj, true)
	}
	// The observation is armed BEFORE the store recovers: a background adopter (the re-armed
	// expiry, the orphan sweep) may reach the continuation first, and that is a legitimate run
	// of the same path.
	var adopted atomic.Bool
	ahook := func(id string) {
		if id == cid {
			adopted.Store(true)
		}
	}
	mqttTestReceiptContinuationHookV.Store(&ahook)
	defer mqttTestReceiptContinuationHookV.Store(nil)
	asm.jsa.testDropRequest.Store(nil)
	asm.expireSessionFromRecord(s, getHash(cid))
	if !adopted.Load() {
		t.Fatal("the record-based expiry adoption never reached the receiptless continuation")
	}
	checkFor(t, 3*asm.jsa.timeout, 50*time.Millisecond, func() error {
		if !testF105RecordGone(asm, cid) {
			return errors.New("the continued adoption did not release the record")
		}
		return nil
	})
	if r, st := testF105_8bReceipt(asm, cid, gen); st != mqttOutboundValid || !r.Retire.Discard {
		t.Fatalf("the continuation must author a discarding receipt, it reads %v", st)
	}
	if !testF105ClosureDiscards(asm, cid, epoch, gen) {
		t.Fatal("the continued adoption did not close the generation discarding")
	}
	if !testF105_8bLedgerSealed(asm, cid, epoch, gen) {
		t.Fatal("the continued adoption did not seal the ledger")
	}
}

// T12: the continuation is ARBITRATED by the marker, not deduplicated by the receipt slot: an
// adoption paused before its batch, whose marker is then displaced, writes NO receipt. Mutation
// M-cont-cas (the receipt written create-only without the marker CAS member).
func TestF105_8bContinuationLosesToADisplacedMarker(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t12"
	asm, _, gen, _ := testF105_8bResidue(t, s, cid, 0)
	if obj := testF105ResidentObject(asm, cid); obj != nil {
		asm.removeSession(obj, true)
	}
	asm.jsa.testDropRequest.Store(nil)
	ps, seq, marked := testF105_8bMarked(asm, cid)
	if !marked {
		t.Fatal("no marker to adopt")
	}
	hook := func(id string) {
		if id != cid {
			return
		}
		// DISPLACE the marker: a re-claim by another teardown, a byte-different marker at the
		// next sequence.
		cur, cseq, ok := testF105_8bMarked(asm, cid)
		if !ok {
			t.Errorf("the marker vanished before the displacement")
			return
		}
		next := *cur
		next.Teardown = &mqttSessTeardown{Origin: "peer", Nonce: "nonce-peer", StartedAt: time.Now().Unix()}
		next.WID = mqttNewRecordWID()
		body, _ := json.Marshal(&next)
		if _, err := mqttStoreSessionRecordCASBody(&asm.jsa, asm.domainTk, body, getHash(cid), cseq); err != nil {
			t.Errorf("displacing the marker: %v", err)
		}
	}
	mqttTestReceiptContinuationHookV.Store(&hook)
	defer mqttTestReceiptContinuationHookV.Store(nil)
	err := asm.finishAdoptedTeardown(ps, getHash(cid), seq, s.getOpts())
	if !errors.Is(err, errMQTTTeardownAdoptionSuperseded) {
		t.Fatalf("an adoption whose marker was displaced must be superseded, got %v", err)
	}
	if _, st := testF105_8bReceipt(asm, cid, gen); st != mqttOutboundAbsent {
		t.Fatalf("the losing continuation wrote a receipt (%v): the receipt slot deduplicated, it did not arbitrate", st)
	}
}

// T10b: a RECEIPTED marker over a crash-left PREPARING is finished by an adopter with no CONNECT
// ahead of it, because every adoption settles first. Mutation M-settle-receipted (settle only
// when receiptless).
func TestF105_8bReceiptedAdoptionSettlesFirst(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8b-t10b"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testF105AccountEpoch(asm)
	gen := testF105ServingAt(t, asm, cid, epoch).Gen
	// The owner's teardown claims WITH a receipt and then cannot close: a PREPARING planted
	// between its claim and its close makes the close refuse.
	var planted atomic.Bool
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionCloseTeardown && planted.CompareAndSwap(false, true) {
			testF105PlantUnresolved(t, asm, cid, epoch, "8b-t10b")
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
	c.Close()
	checkFor(t, 3*asm.jsa.timeout, 50*time.Millisecond, func() error {
		if !planted.Load() {
			return errors.New("the owner's close never reached its send")
		}
		if _, _, marked := testF105_8bMarked(asm, cid); !marked {
			return errors.New("the record is not marked")
		}
		return nil
	})
	asm.jsa.testTransitionPresend.Store(nil)
	time.Sleep(asm.jsa.timeout)
	ps, seq, marked := testF105_8bMarked(asm, cid)
	if !marked {
		t.Skip("the owner's close won its race with the planted preparation; nothing to adopt")
	}
	if _, st := testF105_8bReceipt(asm, cid, gen); st != mqttOutboundValid {
		t.Fatalf("the owner's claim must carry a receipt, it reads %v", st)
	}
	if obj := testF105ResidentObject(asm, cid); obj != nil {
		asm.removeSession(obj, true)
	}
	if err := asm.finishAdoptedTeardown(ps, getHash(cid), seq, s.getOpts()); err != nil {
		t.Fatalf("the adopter could not finish a receipted teardown over a PREPARING: %v", err)
	}
	if !testF105RecordGone(asm, cid) || !testF105ClosureDiscards(asm, cid, epoch, gen) {
		t.Fatal("the adoption did not close discarding and release the record")
	}
}

// testF105_8bDivergeAffine makes the resident object's in-memory affine target differ from what
// its record names, the divergence P2 exists for.
func testF105_8bDivergeAffine(t *testing.T, asm *mqttAccountSessionManager, cid string) (string, string) {
	t.Helper()
	sess := testF105ResidentObject(asm, cid)
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
func TestF105_8bCleanStartRetiresWhatTheRecordNames(t *testing.T) {
	const cid = "f105-8b-t15"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	testF105HoldResident(t, asm, cid)
	memGen, memDur := testF105_8bDivergeAffine(t, asm, cid)
	var retire atomic.Pointer[mqttRetirementRequest]
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Auth != nil && tr.Auth.Intent != nil && tr.Auth.Intent.Retire != nil && tr.Auth.Intent.Retire.Gen == oldGen {
			r := *tr.Auth.Intent.Retire
			retire.Store(&r)
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	if rec := testF105ServingAt(t, asm, cid, epoch); rec.Gen == oldGen {
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
func TestF105_8bMovedDisplacedRecordProposesNoStaleHandoff(t *testing.T) {
	const cid = "f105-8b-t16"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	testF105HoldResident(t, asm, cid)
	memGen, memDur := testF105_8bDivergeAffine(t, asm, cid)
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
	testF105ArmBoundarySettle(t, asm, settle)
	var stale atomic.Bool
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Auth != nil && tr.Auth.Intent != nil && tr.Auth.Intent.Retire != nil &&
			(tr.Auth.Intent.Retire.AffineGen == memGen || tr.Auth.Intent.Retire.AffineDurable == memDur) {
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
	if rec := testF105ServingAt(t, asm, cid, epoch); rec.Gen == oldGen {
		t.Fatal("the clean start must end serving a fresh generation")
	}
}

// T15b (H-1): an UNREADABLE displaced record is not confirmed displacement. With memory and
// record diverging, P2's read fails once: the clean start refuses BEFORE the detach (the
// incumbent is untouched and nothing is claimed or retired), and the client's retry hands off
// naming the record's targets. Falling through to the discard instead would let its claim win at
// the unmoved sequence and retire what memory names. Mutation P2-unreadable (a read error treated
// as a moved record).
func TestF105_8bUnreadableDisplacedRecordRefusesBeforeDetach(t *testing.T) {
	const cid = "f105-8b-t15b"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	testF105HoldResident(t, asm, cid)
	memGen, memDur := testF105_8bDivergeAffine(t, asm, cid)
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
	testF105ArmBoundarySettle(t, asm, func(id string, _ int) {
		if id == cid && settled.CompareAndSwap(false, true) {
			armed.Store(true)
		}
	})
	var retired atomic.Pointer[mqttRetirementRequest]
	var claimed atomic.Bool
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionCloseTeardown {
			claimed.Store(true)
		}
		if tr.Auth != nil && tr.Auth.Intent != nil && tr.Auth.Intent.Retire != nil && tr.Auth.Intent.Retire.Gen == oldGen {
			r := *tr.Auth.Intent.Retire
			retired.Store(&r)
		}
	}
	asm.jsa.testTransitionPresend.Store(&pre)
	t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
	ci := &mqttV5ConnInfo{clientID: cid, cleanStart: true}
	testF105RefusedConnectV5(t, o, ci)
	if !dropped.Load() {
		t.Fatal("the P2 read was never failed, so nothing here is tested")
	}
	if claimed.Load() || retired.Load() != nil {
		t.Fatal("a clean start whose displaced record could not be read went on to retire the generation")
	}
	if rec := testF105ServingAt(t, asm, cid, epoch); rec.Gen != oldGen {
		t.Fatalf("the refused clean start moved the authority to %q", rec.Gen)
	}
	if _, _, marked := testF105_8bMarked(asm, cid); marked {
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

// T15c (H-2, consult 123): a clean start whose DISCARD-BOUNDARY settlement cannot answer — a
// preparation planted after the CONNECT's own top settlement with its receipt unanswerable, or
// the authority itself unreadable — REFUSES before the detach. Taking the discard arm would mark
// the record from memory, and the adopter's continuation would then retire what memory names
// for a generation that may still serve. After the store recovers, the retry hands off naming
// EXACTLY the displaced record's targets. Mutation P2-unresolved (an unresolved boundary load
// falls through to the discard arm).
func TestF105_8bUnresolvedBoundaryRefusesTheCleanStart(t *testing.T) {
	for _, variant := range []string{"preparing", "indeterminate"} {
		t.Run(variant, func(t *testing.T) {
			cid := "f105-8b-t15c-" + variant
			s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
			defer done()
			o := s.getOpts()
			testF105HoldResident(t, asm, cid)
			testF105_8bDivergeAffine(t, asm, cid)
			obj := testF105ResidentObject(asm, cid)
			before, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(cid)))
			if err != nil {
				t.Fatal(err)
			}
			recorded := &mqttPersistedSession{}
			if err := json.Unmarshal(before.Data, recorded); err != nil {
				t.Fatal(err)
			}
			want := mqttTeardownTargetFromRecord(recorded, getHash(cid))
			var drop atomic.Pointer[string]
			dropFn := func(_ string, body []byte) bool {
				d := drop.Load()
				return d != nil && strings.Contains(string(body), *d)
			}
			asm.jsa.testDropRequest.Store(&dropFn)
			t.Cleanup(func() { asm.jsa.testDropRequest.Store(nil) })
			var armed atomic.Bool
			restore := setMQTTTestClearWindowHook(func(sess *mqttSession, window string) {
				if sess != obj || window != mqttClearWindowPreFence || !armed.CompareAndSwap(false, true) {
					return
				}
				if variant == "preparing" {
					i := testF105PlantUnresolved(t, asm, cid, epoch, "8b-t15c")
					subj := mqttHandoffOutcomeSubject(_EMPTY_, getHash(cid), i.ID)
					drop.Store(&subj)
					return
				}
				subj := mqttOutboundAuthoritySubject(getHash(cid))
				drop.Store(&subj)
			})
			defer restore()
			var retired atomic.Pointer[mqttRetirementRequest]
			pre := func(tr *mqttAuthorityTransition) {
				if tr.Auth != nil && tr.Auth.Intent != nil && tr.Auth.Intent.Retire != nil && tr.Auth.Intent.Retire.Gen == oldGen {
					r := *tr.Auth.Intent.Retire
					retired.Store(&r)
				}
			}
			asm.jsa.testTransitionPresend.Store(&pre)
			t.Cleanup(func() { asm.jsa.testTransitionPresend.Store(nil) })
			ci := &mqttV5ConnInfo{clientID: cid, cleanStart: true}
			testF105RefusedConnectV5(t, o, ci)
			if !armed.Load() {
				t.Fatal("the discard boundary was never reached, so nothing here is tested")
			}
			drop.Store(nil)
			if _, _, marked := testF105_8bMarked(asm, cid); marked {
				t.Fatal("the clean start marked the record from memory behind an unresolved boundary")
			}
			after, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(cid)))
			if err != nil || after.Sequence != before.Sequence {
				t.Fatalf("the refused clean start moved the record: %d -> %v (%v)", before.Sequence, after, err)
			}
			obj.mu.Lock()
			objGen := obj.gen
			obj.mu.Unlock()
			if objGen != oldGen {
				t.Fatalf("the refused clean start re-minted the incumbent: %q -> %q", oldGen, objGen)
			}
			tok := obj.beginRecordTx()
			if terr := obj.endRecordTx(tok); terr != nil {
				t.Fatalf("the refused clean start left the record transaction held: %v", terr)
			}
			if _, st := testF105_8bReceipt(asm, cid, oldGen); st != mqttOutboundAbsent {
				t.Fatalf("the refused clean start authored a teardown receipt (%v)", st)
			}
			c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			testMQTTReadConnAckV5(t, r2)
			r := retired.Load()
			if r == nil {
				t.Fatal("the retry's handoff carried no retirement")
			}
			if r.AffineGen != want.q2AffGen || r.AffineDurable != want.q2AffDur || r.PubRelDurable != want.pubRelDur {
				t.Fatalf("the retry retired %q/%q/%q, not the displaced record's %q/%q/%q",
					r.AffineGen, r.AffineDurable, r.PubRelDurable, want.q2AffGen, want.q2AffDur, want.pubRelDur)
			}
		})
	}
}

// T15d: the disposition's classification, row by row (consult 123). Mutations P2-unresolved and
// P2-other-gen.
func TestF105_8bCleanStartDispositionTable(t *testing.T) {
	const cid = "f105-8b-t15d"
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	asm, sess := testResidentSession(t, s, cid)
	epoch := testF105AccountEpoch(asm)
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	at := func(st mqttAuthorityState, g string) mqttAuthorityLoad {
		return mqttAuthorityLoad{status: mqttOutboundValid, rec: &mqttOutboundAuthority{State: st, Gen: g}}
	}
	type outcome int
	const (
		handoff outcome = iota
		discard
		refuse
	)
	for name, row := range map[string]struct {
		p    *mqttPreparedClear
		want outcome
	}{
		"not read":              {&mqttPreparedClear{authRead: false}, refuse},
		"indeterminate":         {&mqttPreparedClear{authRead: true, auth: mqttAuthorityLoad{status: mqttOutboundIndeterminate}}, refuse},
		"absent":                {&mqttPreparedClear{authRead: true, auth: mqttAuthorityLoad{status: mqttOutboundAbsent}}, discard},
		"reserving":             {&mqttPreparedClear{authRead: true, auth: at(mqttAuthorityReserving, "gen-other")}, refuse},
		"preparing at this":     {&mqttPreparedClear{authRead: true, auth: at(mqttAuthorityPreparing, gen)}, refuse},
		"closed":                {&mqttPreparedClear{authRead: true, auth: at(mqttAuthorityClosed, gen)}, discard},
		"serving another":       {&mqttPreparedClear{authRead: true, auth: at(mqttAuthorityServing, "gen-other")}, discard},
		"serving this (proven)": {&mqttPreparedClear{authRead: true, auth: asm.jsa.loadOutboundAuthority(cid, epoch)}, handoff},
	} {
		row.p.sess = sess
		_, ok, err := sess.displacedRetireTargets(row.p)
		got := discard
		switch {
		case err != nil:
			got = refuse
		case ok:
			got = handoff
		}
		if got != row.want {
			t.Errorf("%s: disposition %d (ok=%v err=%v), want %d", name, got, ok, err, row.want)
		}
	}
	// A record with NO write identity, stored at the session's position and matched by the
	// object, so the record-identity comparison passes and only the explicit refusal can refuse.
	smsg, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(cid)))
	if err != nil {
		t.Fatal(err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatal(err)
	}
	ps.WID = _EMPTY_
	body, _ := json.Marshal(ps)
	wseq, err := mqttStoreSessionRecordCASBody(&asm.jsa, asm.domainTk, body, getHash(cid), smsg.Sequence)
	if err != nil {
		t.Fatalf("storing the WID-less record: %v", err)
	}
	sess.mu.Lock()
	sess.wid, sess.seq = _EMPTY_, wseq
	sess.mu.Unlock()
	if _, ok, err := sess.displacedRetireTargets(&mqttPreparedClear{sess: sess, authRead: true, auth: asm.jsa.loadOutboundAuthority(cid, epoch)}); ok || err == nil {
		t.Errorf("a SERVING record with no write identity must refuse, got ok=%v err=%v", ok, err)
	}
}
