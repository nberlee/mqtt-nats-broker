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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// F-105 8a: a proposal some acceptance left standing is SETTLED by the next one, through a
// real CONNECT, and never mistaken for that acceptance's own.
//
// The residue each test meets is left by a REAL accepting CONNECT stopped at the point a
// stopped process would stop it — the step-entry seam fails a step before it acts, the
// before-metadata seam fails one after its reservation or preparation landed — so no in-place
// settlement runs and the durable state is exactly a crashed acceptance's. A proposal made by
// one CONNECT is foreign to the next: it carries another intent id, made under another
// CONNECT's state.

// testF105ArmStepEntry installs the step-entry seam for one test.
func testF105ArmStepEntry(t *testing.T, asm *mqttAccountSessionManager, f func(mqttConnectOwnershipAction) error) {
	t.Helper()
	asm.mu.Lock()
	asm.testOwnershipStepEntry = f
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testOwnershipStepEntry = nil
		asm.mu.Unlock()
	})
}

// testF105ArmBeforeMetadata installs the before-metadata seam for one test.
func testF105ArmBeforeMetadata(t *testing.T, asm *mqttAccountSessionManager, f func(mqttConnectOwnershipAction) error) {
	t.Helper()
	asm.mu.Lock()
	asm.testOwnershipBeforeMetadata = f
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testOwnershipBeforeMetadata = nil
		asm.mu.Unlock()
	})
}

// testF105StopOnce fails the named action once, as a process stopping there would.
func testF105StopOnce(action mqttConnectOwnershipAction) func(mqttConnectOwnershipAction) error {
	var armed atomic.Bool
	armed.Store(true)
	return func(a mqttConnectOwnershipAction) error {
		if a == action && armed.CompareAndSwap(true, false) {
			return fmt.Errorf("f105 8a test: the accepting process stopped at %v", a)
		}
		return nil
	}
}

// testF105TransitionLog records every committed authority transition in order: its kind and
// the proposal it acts on. It is the oracle for WHICH proposal a settlement finalized, cancelled
// or closed, and in what order relative to other events.
type testF105TransitionLog struct {
	mu   sync.Mutex
	evts []string
}

func (l *testF105TransitionLog) add(e string) {
	l.mu.Lock()
	l.evts = append(l.evts, e)
	l.mu.Unlock()
}

func (l *testF105TransitionLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.evts...)
}

func (l *testF105TransitionLog) has(e string) bool {
	return testF105IndexOf(l.snapshot(), e) >= 0
}

func testF105IndexOf(evts []string, e string) int {
	for i, x := range evts {
		if x == e {
			return i
		}
	}
	return -1
}

// testF105ProposalOf names the proposal a transition acts on, read from what it displaces.
func testF105ProposalOf(tr *mqttAuthorityTransition) string {
	if tr.Prev == nil {
		return _EMPTY_
	}
	switch {
	case tr.Prev.Intent != nil:
		return tr.Prev.Intent.ID
	case tr.Prev.Creation != nil:
		return tr.Prev.Creation.ID
	}
	return _EMPTY_
}

func testF105LogTransitions(t *testing.T, jsa *mqttJSA) *testF105TransitionLog {
	t.Helper()
	l := &testF105TransitionLog{}
	testArmTransitionSeam(t, jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err == nil && tr.Attempt != nil {
			l.add(string(tr.Attempt.Kind) + ":" + testF105ProposalOf(tr))
		}
		return seq, err
	})
	return l
}

// testF105StandingProposal returns the proposal the authority carries, or "".
func testF105StandingProposal(t *testing.T, asm *mqttAccountSessionManager, id, epoch string) (string, mqttAuthorityState) {
	t.Helper()
	al := asm.jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		return _EMPTY_, 0
	}
	switch {
	case al.rec.State == mqttAuthorityPreparing && al.rec.Intent != nil:
		return al.rec.Intent.ID, al.rec.State
	case al.rec.State == mqttAuthorityReserving && al.rec.Creation != nil:
		return al.rec.Creation.ID, al.rec.State
	}
	return _EMPTY_, al.rec.State
}

func testF105AccountEpoch(asm *mqttAccountSessionManager) string {
	asm.mu.RLock()
	defer asm.mu.RUnlock()
	return asm.sessStreamEpoch
}

// testF105RefusedConnectV5 connects and expects the CONNECT to be refused.
func testF105RefusedConnectV5(t *testing.T, o *Options, ci *mqttV5ConnInfo) {
	t.Helper()
	cc, _ := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTExpectDisconnect(t, cc)
	cc.Close()
}

// testF105ServingAt asserts the authority serves and returns it.
func testF105ServingAt(t *testing.T, asm *mqttAccountSessionManager, id, epoch string) *mqttOutboundAuthority {
	t.Helper()
	al := asm.jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the authority must serve, got status %v rec %+v", al.status, al.rec)
	}
	return al.rec
}

// T1: a crash-left COMMITTED CREATION is finalized with ITS id — not the new acceptance's —
// and the new acceptance then serves under its OWN tenure. Mutations M1, M2, M3, M18.
func TestF105CrashLeftCommittedCreationIsFinalizedAsProposed(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	// A first CONNECT to create the account manager, on another client id.
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f105-8a-t1-warm"}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	defer cw.Close()
	const cid = "f105-8a-t1"
	asm := testMQTTGetAccountSessionManager(t, s, "f105-8a-t1-warm")
	epoch := testF105AccountEpoch(asm)

	testF105ArmStepEntry(t, asm, testF105StopOnce(mqttOwnershipFinalizeCreation))
	persistent := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
	testF105RefusedConnectV5(t, o, persistent)
	crashed, state := testF105StandingProposal(t, asm, cid, epoch)
	if crashed == _EMPTY_ || state != mqttAuthorityReserving {
		t.Fatalf("the crashed acceptance must leave its reservation standing, got %q in state %v", crashed, state)
	}
	if v := asm.jsa.resolveMetadataAttempt(_EMPTY_, cid, epoch, testF105CreationProposal(t, asm, cid, epoch)); v != mqttMetadataCommitted {
		t.Fatalf("the crashed reservation's metadata half must stand COMMITTED, got %v", v)
	}
	log := testF105LogTransitions(t, &asm.jsa)

	c2, r2 := testMQTTConnectV5(t, persistent, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)

	if !log.has(string(mqttTransitionFinalizeCreation) + ":" + crashed) {
		t.Fatalf("the crashed reservation %q was not finalized under its own id: %v", crashed, log.snapshot())
	}
	rec := testF105ServingAt(t, asm, cid, epoch)
	ent := c2Entitlement(t, s, cid)
	if rec.Tenure != ent.Tenure {
		t.Fatalf("the accepted connection must serve under its own tenure %q, the authority carries %q", ent.Tenure, rec.Tenure)
	}
}

// testF105CreationProposal reads the standing reservation's proposal.
func testF105CreationProposal(t *testing.T, asm *mqttAccountSessionManager, id, epoch string) mqttOwnershipProposal {
	t.Helper()
	al := asm.jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Creation == nil {
		t.Fatalf("no reservation stands: %+v", al.rec)
	}
	p, err := mqttProposalFromCreation(al.rec.Creation)
	if err != nil {
		t.Fatalf("the standing reservation is uninterpretable: %v", err)
	}
	return p
}

// testF105IntentProposal reads the standing preparation's proposal.
func testF105IntentProposal(t *testing.T, asm *mqttAccountSessionManager, id, epoch string) mqttOwnershipProposal {
	t.Helper()
	al := asm.jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Intent == nil {
		t.Fatalf("no preparation stands: %+v", al.rec)
	}
	p, err := mqttProposalFromHandoff(al.rec.Intent)
	if err != nil {
		t.Fatalf("the standing preparation is uninterpretable: %v", err)
	}
	return p
}

// c2Entitlement is the resident session's bound entitlement — the tenure the accepted
// connection serves under.
func c2Entitlement(t *testing.T, s *Server, cid string) mqttOutboundEntitlement {
	t.Helper()
	_, sess := testResidentSession(t, s, cid)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !sess.boundEntOK {
		t.Fatalf("the accepted session carries no entitlement")
	}
	return sess.boundEnt
}

// T2: a crash-left COMMITTED HANDOFF is finalized as proposed, then the CONNECT takes its own
// tenure — at the committed generation for a resume, at a fresh one for a clean start.
func TestF105CrashLeftCommittedHandoffIsFinalizedAsProposed(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(fmt.Sprintf("clean=%v", clean), func(t *testing.T) {
			cid := fmt.Sprintf("f105-8a-t2-%v", clean)
			s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
			defer done()
			o := s.getOpts()

			testF105ArmStepEntry(t, asm, testF105StopOnce(mqttOwnershipFinalizeHandoff))
			testF105RefusedConnectV5(t, o, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
				props: mqttV5ConnPropsSessionExpiry(3600)})
			crashed, state := testF105StandingProposal(t, asm, cid, epoch)
			if crashed == _EMPTY_ || state != mqttAuthorityPreparing {
				t.Fatalf("the crashed acceptance must leave its preparation standing, got %q %v", crashed, state)
			}
			committed := testF105IntentProposal(t, asm, cid, epoch)
			log := testF105LogTransitions(t, &asm.jsa)

			c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: clean,
				props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			testMQTTReadConnAckV5(t, r2)

			// (i) the intermediate boundary: the crashed proposal was finalized under its own id.
			if !log.has(string(mqttTransitionFinalize) + ":" + crashed) {
				t.Fatalf("the crashed handoff %q was not finalized as proposed: %v", crashed, log.snapshot())
			}
			// (ii) the end: this CONNECT's own tenure serves.
			rec := testF105ServingAt(t, asm, cid, epoch)
			if ent := c2Entitlement(t, s, cid); rec.Tenure != ent.Tenure || rec.Tenure == committed.Tenure {
				t.Fatalf("the CONNECT must serve under its own tenure, authority %q entitlement %q committed %q",
					rec.Tenure, ent.Tenure, committed.Tenure)
			}
			if clean && rec.Gen == committed.Gen {
				t.Fatalf("a clean start must hand off to a FRESH generation, still the committed %q", committed.Gen)
			}
			if !clean && rec.Gen != committed.Gen {
				t.Fatalf("a resume must serve the committed generation %q, serves %q", committed.Gen, rec.Gen)
			}
		})
	}
}

// T2 resident: the committed foreign handoff moved the record past a RESIDENT object; the
// CONNECT disposes the stale object and restores from the moved record. Mutations M4a, M21,
// M26, M28.
func TestF105CrashLeftCommittedHandoffUnderAResidentObject(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(fmt.Sprintf("clean=%v", clean), func(t *testing.T) {
			cid := fmt.Sprintf("f105-8a-t2r-%v", clean)
			s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
			defer done()
			o := s.getOpts()
			before := testF105ResidentObject(asm, cid)
			if before == nil {
				t.Fatal("the persistent session must be resident for this test to mean anything")
			}

			testF105HoldResident(t, asm, cid)
			// Another acceptance's clean start, crashed after its metadata committed: planted
			// through the same ops a peer broker runs.
			e := &testF105Env{asm: asm, id: cid, epoch: epoch}
			i := testF105Intent(t, e, "t2r")
			testF105Prepare(t, e, i)
			if _, err := asm.jsa.commitMetadataAttempt(testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))); err != nil {
				t.Fatalf("committing the peer's metadata half: %v", err)
			}
			if i.Gen == oldGen {
				t.Fatal("the planted handoff must change the generation for this test to mean anything")
			}
			log := testF105LogTransitions(t, &asm.jsa)

			c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: clean,
				props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			testMQTTReadConnAckV5(t, r2)

			if !log.has(string(mqttTransitionFinalize) + ":" + i.ID) {
				t.Fatalf("the peer's committed handoff %q was not finalized: %v", i.ID, log.snapshot())
			}
			if after := testF105ResidentObject(asm, cid); after == before {
				t.Fatal("the stale resident object is still the client's session")
			}
			rec := testF105ServingAt(t, asm, cid, epoch)
			if !clean && rec.Gen != i.Gen {
				t.Fatalf("a resume must serve the committed generation %q, serves %q", i.Gen, rec.Gen)
			}
		})
	}
}

// testF105ResidentObject is the session object the account holds for cid, bound or not.
func testF105ResidentObject(asm *mqttAccountSessionManager, cid string) *mqttSession {
	asm.mu.RLock()
	defer asm.mu.RUnlock()
	return asm.sessions[cid]
}

// testF105HoldResident keeps a resident object resident while a fixture plants a foreign write
// under it: this broker would otherwise evict it asynchronously when it observes the remote
// persist (or its ownership backstop does), and the CONNECT under test would sometimes meet no
// resident object at all. That is the state a delayed notification leaves — the stale resident
// object the CONNECT's settlement must dispose of itself.
func testF105HoldResident(t *testing.T, asm *mqttAccountSessionManager, cid string) {
	t.Helper()
	h := getHash(cid)
	asm.mu.Lock()
	asm.testSessPersistDrop = func(x string) bool { return x == h }
	asm.testSessRecordBackstopOff = true
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testSessPersistDrop = nil
		asm.testSessRecordBackstopOff = false
		asm.mu.Unlock()
	})
}

// T3: a crash-left UNRESOLVED preparation is aborted, cancelled — its predecessor restored —
// and the CONNECT proceeds. Mutations M1, M2.
func TestF105CrashLeftUnresolvedPreparationIsCancelled(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(fmt.Sprintf("clean=%v", clean), func(t *testing.T) {
			cid := fmt.Sprintf("f105-8a-t3-%v", clean)
			s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
			defer done()
			o := s.getOpts()

			testF105ArmBeforeMetadata(t, asm, testF105StopOnce(mqttOwnershipPrepareHandoff))
			testF105RefusedConnectV5(t, o, &mqttV5ConnInfo{clientID: cid, cleanStart: clean,
				props: mqttV5ConnPropsSessionExpiry(3600)})
			crashed, state := testF105StandingProposal(t, asm, cid, epoch)
			if crashed == _EMPTY_ || state != mqttAuthorityPreparing {
				t.Fatalf("the crashed acceptance must leave its preparation standing, got %q %v", crashed, state)
			}
			p := testF105IntentProposal(t, asm, cid, epoch)
			if v := asm.jsa.resolveMetadataAttempt(_EMPTY_, cid, epoch, p); v != mqttMetadataUnresolved {
				t.Fatalf("the crashed preparation's slot must be empty, got %v", v)
			}
			log := testF105LogTransitions(t, &asm.jsa)

			c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: clean,
				props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			testMQTTReadConnAckV5(t, r2)

			if !log.has(string(mqttTransitionCancelIntent) + ":" + crashed) {
				t.Fatalf("the crashed preparation %q was not cancelled: %v", crashed, log.snapshot())
			}
			if v := asm.jsa.resolveMetadataAttempt(_EMPTY_, cid, epoch, p); v != mqttMetadataAborted {
				t.Fatalf("the crashed preparation's slot must stand ABORTED, got %v", v)
			}
			rec := testF105ServingAt(t, asm, cid, epoch)
			if !clean && rec.Gen != oldGen {
				t.Fatalf("a resume over the restored predecessor must keep generation %q, got %q", oldGen, rec.Gen)
			}
		})
	}
}

// T4: a crash-left UNRESOLVED reservation is aborted, its generation closed, and the CONNECT
// reserves over the closure. Mutations M1, M6.
func TestF105CrashLeftUnresolvedReservationIsClosed(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f105-8a-t4-warm"}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	defer cw.Close()
	const cid = "f105-8a-t4"
	asm := testMQTTGetAccountSessionManager(t, s, "f105-8a-t4-warm")
	epoch := testF105AccountEpoch(asm)

	testF105ArmBeforeMetadata(t, asm, testF105StopOnce(mqttOwnershipReserve))
	persistent := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
	testF105RefusedConnectV5(t, o, persistent)
	crashed, state := testF105StandingProposal(t, asm, cid, epoch)
	if crashed == _EMPTY_ || state != mqttAuthorityReserving {
		t.Fatalf("the crashed acceptance must leave its reservation standing, got %q %v", crashed, state)
	}
	crashedGen := asm.jsa.loadOutboundAuthority(cid, epoch).rec.Gen
	log := testF105LogTransitions(t, &asm.jsa)

	c2, r2 := testMQTTConnectV5(t, persistent, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)

	if cl, _, st := asm.jsa.loadGenClosure(cid, epoch, crashedGen); st != mqttOutboundValid || cl == nil {
		t.Fatalf("the crashed reservation's generation %q must be closed, got status %v", crashedGen, st)
	}
	if !log.has(string(mqttTransitionReserveCreation)+":") && len(log.snapshot()) == 0 {
		t.Fatalf("no transition was observed: %v", log.snapshot())
	}
	if rec := testF105ServingAt(t, asm, cid, epoch); rec.Gen == crashedGen {
		t.Fatalf("the CONNECT must serve a generation other than the closed %q", crashedGen)
	}
}

// T5: a crash-left proposal positively ABORTED by somebody else does not make the next
// acceptance a loser. Mutations M1, M18.
func TestF105ForeignAbortedProposalDoesNotMakeTheAcceptanceALoser(t *testing.T) {
	t.Run("preparation", func(t *testing.T) {
		const cid = "f105-8a-t5-prep"
		s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
		defer done()
		o := s.getOpts()
		testF105ArmBeforeMetadata(t, asm, testF105StopOnce(mqttOwnershipPrepareHandoff))
		ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
		testF105RefusedConnectV5(t, o, ci)
		p := testF105IntentProposal(t, asm, cid, epoch)
		if v := asm.jsa.settleProposal(_EMPTY_, cid, epoch, p); v != mqttMetadataAborted {
			t.Fatalf("aborting the crashed preparation's slot returned %v", v)
		}
		c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		testMQTTReadConnAckV5(t, r2)
		testF105ServingAt(t, asm, cid, epoch)
	})
	t.Run("reservation", func(t *testing.T) {
		o := testMQTTDefaultOptionsV5()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)
		cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f105-8a-t5-warm"}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, rw)
		defer cw.Close()
		const cid = "f105-8a-t5-res"
		asm := testMQTTGetAccountSessionManager(t, s, "f105-8a-t5-warm")
		epoch := testF105AccountEpoch(asm)
		testF105ArmBeforeMetadata(t, asm, testF105StopOnce(mqttOwnershipReserve))
		ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
		testF105RefusedConnectV5(t, o, ci)
		p := testF105CreationProposal(t, asm, cid, epoch)
		if v := asm.jsa.settleProposal(_EMPTY_, cid, epoch, p); v != mqttMetadataAborted {
			t.Fatalf("aborting the crashed reservation's slot returned %v", v)
		}
		c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		testMQTTReadConnAckV5(t, r2)
		testF105ServingAt(t, asm, cid, epoch)
	})
}

// T6: a lost response to the metadata commit — the batch landed, its acknowledgement did not
// — is resolved IN PLACE: the first CONNECT is accepted. Bug v's evidence: an ambiguous
// ownership commit is resolved from its receipt slot, never handed to the record uncertainty
// resolver.
func TestF105LostResponseMetadataCommitFinalizesInPlace(t *testing.T) {
	const cid = "f105-8a-t6"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	var armed atomic.Bool
	armed.Store(true)
	restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
		if obs.Phase == mqttAcceptStoreAcked && armed.CompareAndSwap(true, false) {
			return errors.New("f105 8a test: the metadata batch landed and its acknowledgement was lost")
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
	if rec := testF105ServingAt(t, asm, cid, epoch); rec.Gen == oldGen {
		t.Fatalf("the clean start must serve a fresh generation, still %q", oldGen)
	}
	_, sess := testResidentSession(t, s, cid)
	if sess.isSeqUncertain() {
		t.Fatal("an ambiguous ownership commit must be resolved from its receipt, not left to the record uncertainty resolver")
	}
}

// T18: two settlers. Our settlement selects a foreign proposal; before its act, the competing
// settler acts on it. Ours finds it gone, reloads, and the CONNECT is accepted; the proposal
// was acted on exactly once. Mutation M22.
func TestF105CompetingSettlerActsFirst(t *testing.T) {
	const cid = "f105-8a-t18"
	s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	testF105ArmStepEntry(t, asm, testF105StopOnce(mqttOwnershipFinalizeHandoff))
	ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
	testF105RefusedConnectV5(t, o, ci)
	crashed, _ := testF105StandingProposal(t, asm, cid, epoch)
	if crashed == _EMPTY_ {
		t.Fatal("no crashed proposal stands")
	}
	var finalized atomic.Int32
	var raced atomic.Bool
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionFinalize && testF105ProposalOf(tr) == crashed {
			if raced.CompareAndSwap(false, true) {
				// THE COMPETITOR: finalize the same proposal first, then let ours run.
				// Its own commit passes through this seam too, and is counted there.
				if _, err := asm.jsa.opFinalizeHandoff(cid, epoch, "txn-competitor", crashed); err != nil {
					t.Errorf("the competing settler's finalize: %v", err)
				}
			}
			seq, err := send()
			if err == nil {
				finalized.Add(1)
			}
			return seq, err
		}
		return send()
	})
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	if !raced.Load() {
		t.Fatal("the competitor never raced, so nothing here is tested")
	}
	if n := finalized.Load(); n != 1 {
		t.Fatalf("the proposal must be finalized exactly once, was %d times", n)
	}
	testF105ServingAt(t, asm, cid, epoch)
}

// testF105ArmBudget shrinks the account's CONNECT ownership budget for one test.
func testF105ArmBudget(t *testing.T, asm *mqttAccountSessionManager, d time.Duration) {
	t.Helper()
	asm.mu.Lock()
	asm.testOwnershipBudgetTotal = d
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testOwnershipBudgetTotal = 0
		asm.mu.Unlock()
	})
}

// testF105AbandonGate gates the owed-abandonment worker: it records each launch, and lets it
// act only when release is closed; proceed=false suppresses it.
type testF105AbandonGate struct {
	launched atomic.Int32
	release  chan struct{}
	proceed  atomic.Bool
}

func testF105ArmAbandonGate(t *testing.T, asm *mqttAccountSessionManager) *testF105AbandonGate {
	t.Helper()
	g := &testF105AbandonGate{release: make(chan struct{})}
	g.proceed.Store(true)
	asm.mu.Lock()
	asm.testAbandonOwn = func(string, mqttOwnershipProposal) bool {
		g.launched.Add(1)
		<-g.release
		return g.proceed.Load()
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testAbandonOwn = nil
		asm.mu.Unlock()
	})
	return g
}

// testF105OwnAbortedByPeer arms the before-metadata seam so that, the moment this
// acceptance's own reservation or preparation lands, a PEER settles its slot ABORTED — the
// acceptance has lost — and every attempt to abandon it (close or cancel) is held off by the
// transition seam while blockAbandon is set, sleeping until the budget is gone when expire is
// set. It returns the proposal id it aborted.
func testF105OwnAbortedByPeer(t *testing.T, asm *mqttAccountSessionManager, cid, epoch string,
	action mqttConnectOwnershipAction, blockAbandon *atomic.Bool, expireAfter time.Duration) *atomic.Value {
	t.Helper()
	var own atomic.Value
	var armed atomic.Bool
	armed.Store(true)
	testF105ArmBeforeMetadata(t, asm, func(a mqttConnectOwnershipAction) error {
		if a != action || !armed.CompareAndSwap(true, false) {
			return nil
		}
		al := asm.jsa.loadOutboundAuthority(cid, epoch)
		var p mqttOwnershipProposal
		var err error
		if al.rec != nil && al.rec.Creation != nil {
			p, err = mqttProposalFromCreation(al.rec.Creation)
		} else if al.rec != nil && al.rec.Intent != nil {
			p, err = mqttProposalFromHandoff(al.rec.Intent)
		}
		if err != nil || p.ID == _EMPTY_ {
			t.Errorf("no own proposal stands to abort: %+v %v", al.rec, err)
			return nil
		}
		own.Store(p.ID)
		if v := asm.jsa.settleProposal(_EMPTY_, cid, epoch, p); v != mqttMetadataAborted {
			t.Errorf("the peer's abort returned %v", v)
		}
		return nil
	})
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		id, _ := own.Load().(string)
		if tr.Attempt != nil && id != _EMPTY_ && testF105ProposalOf(tr) == id && blockAbandon.Load() &&
			(tr.Attempt.Kind == mqttTransitionCancelIntent || tr.Attempt.Kind == mqttTransitionCloseAdmit) {
			if expireAfter > 0 {
				time.Sleep(expireAfter)
			}
			return 0, errors.New("f105 8a test: the abandonment is held off")
		}
		return send()
	})
	return &own
}

// T8 / T19: this acceptance's OWN proposal is aborted by a peer and its in-place abandonment
// does not confirm. The CONNECT is refused and the owed-abandonment worker abandons exactly
// that proposal afterwards — with budget remaining (T19: the executor error arm) and at expiry
// (T8d: the deadline cut the cleanup off; the observation carried the ABORTED verdict).
// Variant (c): the worker never acts, and the next acceptance settles the proposal as foreign.
// Mutations M7, M20.
func TestF105OwnAbortedProposalIsAbandonedAfterTheRefusal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expire   bool
		suppress bool
	}{{"budget-remaining", false, false}, {"at-expiry", true, false}, {"worker-suppressed", false, true}} {
		t.Run("preparation/"+tc.name, func(t *testing.T) {
			cid := "f105-8a-t8p-" + tc.name
			s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
			defer done()
			o := s.getOpts()
			before := testF105ServingAt(t, asm, cid, epoch)
			var expire time.Duration
			if tc.expire {
				testF105ArmBudget(t, asm, 400*time.Millisecond)
				expire = 500 * time.Millisecond
			}
			var block atomic.Bool
			block.Store(true)
			own := testF105OwnAbortedByPeer(t, asm, cid, epoch, mqttOwnershipPrepareHandoff, &block, expire)
			gate := testF105ArmAbandonGate(t, asm)
			ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
			testF105RefusedConnectV5(t, o, ci)
			id, _ := own.Load().(string)
			if id == _EMPTY_ {
				t.Fatal("no own proposal was aborted, so nothing here is tested")
			}
			if standing, _ := testF105StandingProposal(t, asm, cid, epoch); standing != id {
				t.Fatalf("the aborted own preparation %q must still stand after the refusal, got %q", id, standing)
			}
			block.Store(false)
			checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
				if gate.launched.Load() == 0 {
					return errors.New("the owed abandonment was never launched")
				}
				return nil
			})
			if tc.suppress {
				gate.proceed.Store(false)
			}
			close(gate.release)
			if tc.suppress {
				// The next acceptance settles it as foreign.
				c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
				defer c2.Close()
				testMQTTReadConnAckV5(t, r2)
				return
			}
			checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
				al := asm.jsa.loadOutboundAuthority(cid, epoch)
				if al.rec == nil || al.rec.State != mqttAuthorityServing || al.rec.Gen != oldGen ||
					al.rec.Tenure != before.Tenure {
					return fmt.Errorf("the predecessor is not restored yet: %+v", al.rec)
				}
				return nil
			})
		})
	}
	t.Run("reservation/budget-remaining", func(t *testing.T) {
		o := testMQTTDefaultOptionsV5()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)
		cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f105-8a-t8r-warm"}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, rw)
		defer cw.Close()
		const cid = "f105-8a-t8r"
		asm := testMQTTGetAccountSessionManager(t, s, "f105-8a-t8r-warm")
		epoch := testF105AccountEpoch(asm)
		var block atomic.Bool
		block.Store(true)
		own := testF105OwnAbortedByPeer(t, asm, cid, epoch, mqttOwnershipReserve, &block, 0)
		gate := testF105ArmAbandonGate(t, asm)
		testF105RefusedConnectV5(t, o, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)})
		id, _ := own.Load().(string)
		if standing, _ := testF105StandingProposal(t, asm, cid, epoch); id == _EMPTY_ || standing != id {
			t.Fatalf("the aborted own reservation %q must still stand after the refusal, got %q", id, standing)
		}
		gen := asm.jsa.loadOutboundAuthority(cid, epoch).rec.Gen
		block.Store(false)
		close(gate.release)
		checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
			if cl, _, st := asm.jsa.loadGenClosure(cid, epoch, gen); st != mqttOutboundValid || cl == nil {
				return fmt.Errorf("the reservation's generation %q is not closed yet (%v)", gen, st)
			}
			return nil
		})
	})
}

// T9: this acceptance's own proposal COMMITTED, and the budget runs out before finalization:
// the CONNECT is refused, NOTHING is abandoned — the owed-abandonment worker is never even
// launched — and the next acceptance finalizes it as proposed. (b): the expiry falls between
// the COMMITTED resolution and the adoption readback. Mutation M9, M20.
func TestF105ExpiredCommittedProposalStandsForTheNextAcceptance(t *testing.T) {
	const cid = "f105-8a-t9"
	s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	testF105ArmBudget(t, asm, 400*time.Millisecond)
	var armed atomic.Bool
	armed.Store(true)
	testF105ArmStepEntry(t, asm, func(a mqttConnectOwnershipAction) error {
		if a == mqttOwnershipFinalizeHandoff && armed.CompareAndSwap(true, false) {
			time.Sleep(500 * time.Millisecond)
		}
		return nil
	})
	gate := testF105ArmAbandonGate(t, asm)
	close(gate.release)
	ci := &mqttV5ConnInfo{clientID: cid, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(3600)}
	testF105RefusedConnectV5(t, o, ci)
	standing, state := testF105StandingProposal(t, asm, cid, epoch)
	if standing == _EMPTY_ || state != mqttAuthorityPreparing {
		t.Fatalf("the committed proposal must still stand PREPARING, got %q %v", standing, state)
	}
	if v := asm.jsa.resolveMetadataAttempt(_EMPTY_, cid, epoch, testF105IntentProposal(t, asm, cid, epoch)); v != mqttMetadataCommitted {
		t.Fatalf("the proposal must stand COMMITTED, got %v", v)
	}
	if n := gate.launched.Load(); n != 0 {
		t.Fatalf("a COMMITTED proposal owes no abandonment, but the worker was launched %d time(s)", n)
	}
	testF105ArmBudget(t, asm, 0)
	log := testF105LogTransitions(t, &asm.jsa)
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	if !log.has(string(mqttTransitionFinalize) + ":" + standing) {
		t.Fatalf("the next acceptance did not finalize the committed proposal %q: %v", standing, log.snapshot())
	}
}

// T10: the ownership deadline caps the REQUESTS, not only when a step starts. With the
// per-request timeout at its 5s default and the budget at 300ms, a store that never answers
// refuses the CONNECT well inside a second. (a) the authority reads; (b) the metadata batch.
// Mutations M10a, M10b.
func TestF105OwnershipDeadlineBoundsABlockedStore(t *testing.T) {
	for _, tc := range []struct {
		name string
		drop func(cidHash string) func(subject string, body []byte) bool
	}{
		{"authority-read", func(h string) func(string, []byte) bool {
			subj := mqttOutboundAuthoritySubject(h)
			return func(_ string, body []byte) bool { return bytesContains(body, subj) }
		}},
		{"metadata-batch", func(h string) func(string, []byte) bool {
			subj := mqttSessSubject(_EMPTY_, h)
			return func(apiSubj string, _ []byte) bool { return apiSubj == subj }
		}},
		// (c) NESTED: the batch AND every receipt-slot read of the settlement that follows it —
		// the resolution a failed commit runs must honour the same deadline.
		{"nested-receipt-resolution", func(h string) func(string, []byte) bool {
			subj := mqttSessSubject(_EMPTY_, h)
			receipts := mqttHandoffClientFilter(_EMPTY_, h)
			receipts = receipts[:len(receipts)-1]
			return func(apiSubj string, body []byte) bool {
				return apiSubj == subj || strings.Contains(string(body), receipts)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cid := "f105-8a-t10-" + tc.name
			s, asm, _, _, done := testF105CleanStartEnv(t, cid)
			defer done()
			o := s.getOpts()
			testF105ArmBudget(t, asm, 300*time.Millisecond)
			if asm.jsa.timeout < 2*time.Second {
				t.Fatalf("the per-request timeout %v is too short for this test to discriminate", asm.jsa.timeout)
			}
			drop := tc.drop(getHash(cid))
			asm.jsa.testDropRequest.Store(&drop)
			t.Cleanup(func() { asm.jsa.testDropRequest.Store(nil) })
			start := time.Now()
			testF105RefusedConnectV5(t, o, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
				props: mqttV5ConnPropsSessionExpiry(3600)})
			if el := time.Since(start); el > 1500*time.Millisecond {
				t.Fatalf("the CONNECT took %v to be refused against a 300ms ownership budget: a request "+
					"outlived the deadline", el)
			}
		})
	}
}

func bytesContains(b []byte, s string) bool {
	return len(s) > 0 && len(b) >= len(s) && strings.Contains(string(b), s)
}

// testF105Persistent connects a persistent v5 session with the given expiry, subscribes, and
// closes it, returning its account, epoch and serving generation.
func testF105Persistent(t *testing.T, s *Server, cid string, expiry uint32) (*mqttAccountSessionManager, string, string) {
	t.Helper()
	o := s.getOpts()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid,
		props: mqttV5ConnPropsSessionExpiry(expiry)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "foo", opts: 1}})
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testF105AccountEpoch(asm)
	gen := testF105ServingAt(t, asm, cid, epoch).Gen
	c.Close()
	time.Sleep(150 * time.Millisecond)
	return asm, epoch, gen
}

// testF105MakeNonResident evicts the resident object from the account's maps WITHOUT discarding
// it and disarms its expiry timer, leaving only the record — the state a restarted broker meets.
func testF105MakeNonResident(t *testing.T, asm *mqttAccountSessionManager, cid string) {
	t.Helper()
	sess := testF105ResidentObject(asm, cid)
	if sess == nil {
		t.Fatal("no resident object to evict")
	}
	asm.cancelSessionExpiry(getHash(cid))
	asm.removeSession(sess, true)
}

// testF105PlantUnresolved plants a foreign clean-start preparation, its metadata slot empty: an
// acceptance on another broker stopped between its prepare and its metadata commit.
func testF105PlantUnresolved(t *testing.T, asm *mqttAccountSessionManager, cid, epoch, tag string) *mqttHandoffIntent {
	t.Helper()
	e := &testF105Env{asm: asm, id: cid, epoch: epoch}
	i := testF105Intent(t, e, tag)
	testF105Prepare(t, e, i)
	return i
}

// testF105ClosureDiscards reports whether gen carries a DISCARDING closure.
func testF105ClosureDiscards(asm *mqttAccountSessionManager, cid, epoch, gen string) bool {
	cl, _, st := asm.jsa.loadGenClosure(cid, epoch, gen)
	return st == mqttOutboundValid && cl != nil && cl.Discard
}

// testF105RecordGone reports whether the client's session record is gone.
func testF105RecordGone(asm *mqttAccountSessionManager, cid string) bool {
	smsg, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(cid)))
	return err != nil || smsg == nil || mqttDecodeSessionRecord(smsg) == nil
}

// T15 / T15b(a): an OVERDUE NON-RESIDENT predecessor with a crash-left UNRESOLVED preparation.
// (T15) the CONNECT's own settlement cancels it BEFORE the restore is entered, and the
// restore's discard finds nothing left to settle. (T15b-a) a preparation planted AFTER that
// settlement, just before the restore, is settled at the DISCARD BOUNDARY instead, and the
// discard proceeds RECEIPTED — the predecessor generation ends with an authorized discarding
// closure. Mutations M5, M17.
func TestF105OverdueNonResidentPredecessorSettlesBeforeTheDiscard(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprintf("late=%v", late), func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			cid := fmt.Sprintf("f105-8a-t15-%v", late)
			asm, epoch, gen := testF105Persistent(t, s, cid, 1)
			testF105MakeNonResident(t, asm, cid)
			var early *mqttHandoffIntent
			if !late {
				early = testF105PlantUnresolved(t, asm, cid, epoch, "t15")
			}
			time.Sleep(1200 * time.Millisecond) // overdue
			log := testF105LogTransitions(t, &asm.jsa)
			var boundarySteps atomic.Int32
			var lateIntent atomic.Value
			asm.mu.Lock()
			asm.testRestoreEntry = func(id string) {
				if id != cid {
					return
				}
				log.add("restore-entry")
				if late && lateIntent.Load() == nil {
					lateIntent.Store(testF105PlantUnresolved(t, asm, cid, epoch, "t15b"))
				}
			}
			asm.testBoundarySettle = func(id string, steps int) {
				if id == cid {
					boundarySteps.Add(int32(steps))
				}
			}
			asm.mu.Unlock()
			t.Cleanup(func() {
				asm.mu.Lock()
				asm.testRestoreEntry, asm.testBoundarySettle = nil, nil
				asm.mu.Unlock()
			})

			c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid,
				props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			testMQTTReadConnAckV5(t, r2)

			evts := log.snapshot()
			if !late {
				cancel := testF105IndexOf(evts, string(mqttTransitionCancelIntent)+":"+early.ID)
				entry := testF105IndexOf(evts, "restore-entry")
				if cancel < 0 || entry < 0 || cancel > entry {
					t.Fatalf("the CONNECT's settlement must cancel %q BEFORE the restore is entered: %v", early.ID, evts)
				}
				if n := boundarySteps.Load(); n != 0 {
					t.Fatalf("the discard boundary settled %d step(s): the CONNECT's own settlement must "+
						"have left nothing standing", n)
				}
			} else {
				li, _ := lateIntent.Load().(*mqttHandoffIntent)
				if li == nil {
					t.Fatal("the late preparation was never planted")
				}
				if !log.has(string(mqttTransitionCancelIntent) + ":" + li.ID) {
					t.Fatalf("the discard boundary did not cancel the late preparation %q: %v", li.ID, evts)
				}
				if boundarySteps.Load() == 0 {
					t.Fatal("the late preparation was not settled at the discard boundary")
				}
			}
			if !testF105ClosureDiscards(asm, cid, epoch, gen) {
				t.Fatalf("the overdue predecessor generation %q must end with an authorized DISCARDING "+
					"closure — a receiptless discard leaves it serving", gen)
			}
		})
	}
}

// T17: a RESIDENT clean start meets a preparation planted between the CONNECT's settlement and
// its retirement. The clean-start retirement's own discard boundary settles it, the handoff
// proceeds from the boundary's load, and the CONNECT is accepted. Mutation M17.
func TestF105CleanStartRetirementSettlesALateProposal(t *testing.T) {
	const cid = "f105-8a-t17"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	var planted atomic.Value
	asm.mu.Lock()
	asm.testRestoreEntry = func(id string) {
		if id == cid && planted.Load() == nil {
			planted.Store(testF105PlantUnresolved(t, asm, cid, epoch, "t17"))
		}
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testRestoreEntry = nil
		asm.mu.Unlock()
	})
	log := testF105LogTransitions(t, &asm.jsa)
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	li, _ := planted.Load().(*mqttHandoffIntent)
	if li == nil {
		t.Fatal("the late preparation was never planted")
	}
	if !log.has(string(mqttTransitionCancelIntent) + ":" + li.ID) {
		t.Fatalf("the late preparation %q was not settled: %v", li.ID, log.snapshot())
	}
	if rec := testF105ServingAt(t, asm, cid, epoch); rec.Gen == oldGen {
		t.Fatalf("the clean start must serve a fresh generation, still %q", oldGen)
	}
}

// T20: a disconnect whose session ends at once (expiry 0), over a crash-left foreign
// preparation and with no CONNECT to follow: the disconnect's own discard settles it at the
// boundary and completes RECEIPTED. Mutation M17.
func TestF105DisconnectDiscardSettlesAStandingProposal(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8a-t20"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "foo", opts: 1}})
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testF105AccountEpoch(asm)
	gen := testF105ServingAt(t, asm, cid, epoch).Gen
	i := testF105PlantUnresolved(t, asm, cid, epoch, "t20")
	c.Close()
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if !testF105RecordGone(asm, cid) {
			return errors.New("the session record still stands")
		}
		if !testF105ClosureDiscards(asm, cid, epoch, gen) {
			return fmt.Errorf("generation %q has no discarding closure yet", gen)
		}
		return nil
	})
	if v := asm.jsa.resolveMetadataAttempt(_EMPTY_, cid, epoch, testF105Prop(t, i)); v != mqttMetadataAborted {
		t.Fatalf("the crash-left preparation's slot must stand ABORTED, got %v", v)
	}
}

// T21: NONRESIDENT RECORD EXPIRY after a restart, over an overdue record and a crash-left
// UNRESOLVED preparation, and no CONNECT: the record-expiry sweep's transient object settles at
// the boundary and completes RECEIPTED. Mutation M17.
func TestF105RecordExpirySettlesAStandingProposal(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8a-t21"
	asm, epoch, gen := testF105Persistent(t, s, cid, 1)
	testF105MakeNonResident(t, asm, cid)
	i := testF105PlantUnresolved(t, asm, cid, epoch, "t21")
	time.Sleep(1200 * time.Millisecond)
	asm.expireSessionFromRecord(s, getHash(cid))
	if !testF105RecordGone(asm, cid) {
		t.Fatal("the overdue record still stands")
	}
	if !testF105ClosureDiscards(asm, cid, epoch, gen) {
		t.Fatalf("generation %q must end with an authorized discarding closure", gen)
	}
	if v := asm.jsa.resolveMetadataAttempt(_EMPTY_, cid, epoch, testF105Prop(t, i)); v != mqttMetadataAborted {
		t.Fatalf("the crash-left preparation's slot must stand ABORTED, got %v", v)
	}
}

// T21b / T2-order: the crash-left proposal is COMMITTED and its successor record is itself
// OVERDUE. The discard boundary FINALIZES it, and the transient object — built from that
// successor — then claims and discards the successor; "a finalize happened" is not taken as
// proof of supersession. With a CONNECT (T2-order), the settlement's finalize precedes the
// first discard entry, and the discard then completes. Mutations M24, M4b.
func TestF105CommittedOverdueSuccessorIsFinalizedThenDiscarded(t *testing.T) {
	for _, viaConnect := range []bool{false, true} {
		t.Run(fmt.Sprintf("connect=%v", viaConnect), func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			cid := fmt.Sprintf("f105-8a-t21b-%v", viaConnect)
			asm, epoch, _ := testF105Persistent(t, s, cid, 3600)
			testF105MakeNonResident(t, asm, cid)
			e := &testF105Env{asm: asm, id: cid, epoch: epoch}
			i := testF105Intent(t, e, "t21b")
			testF105Prepare(t, e, i)
			// The committed successor record is OVERDUE: disconnected ten seconds ago with a
			// one-second expiry.
			fact := &mqttAcceptFact{ClientID: cid, SessEpoch: epoch, AcceptID: mqttNewAcceptID(), At: time.Now().UnixNano()}
			fb, _ := json.Marshal(fact)
			wid := mqttNewInflightNonce()
			rec, _ := json.Marshal(&mqttPersistedSession{ID: cid, Gen: i.Gen, WID: wid, Origin: "o",
				ExpiryInterval: 1, DisconnectedAt: time.Now().Add(-10 * time.Second).Unix()})
			f, ferr := mqttFreezeMetadataAttempt(cid, epoch, _EMPTY_, testF105Prop(t, i), wid,
				mqttAcceptSubject(_EMPTY_, getHash(cid), epoch, fact.AcceptID), fb, testF105RecordSeq(t, e), rec)
			if ferr != nil {
				t.Fatalf("freezing the successor: %v", ferr)
			}
			if _, err := asm.jsa.commitMetadataAttempt(f); err != nil {
				t.Fatalf("committing the successor: %v", err)
			}
			log := testF105LogTransitions(t, &asm.jsa)
			hook := func(sess *mqttSession, window string) {
				if sess.id == cid && window == mqttClearWindowPreEntry {
					log.add("discard-entry")
				}
			}
			mqttTestClearWindowHookV.Store(&hook)
			t.Cleanup(func() { mqttTestClearWindowHookV.Store(nil) })

			if viaConnect {
				c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid,
					props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
				defer c2.Close()
				testMQTTReadConnAckV5(t, r2)
			} else {
				asm.expireSessionFromRecord(s, getHash(cid))
			}
			// The server's own record-expiry sweep meets the same overdue record and may be the
			// discard that wins the claim, so the outcome is awaited rather than read at once.
			checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
				if !testF105ClosureDiscards(asm, cid, epoch, i.Gen) {
					return fmt.Errorf("the finalized successor generation %q has no discarding closure yet: %v",
						i.Gen, log.snapshot())
				}
				return nil
			})
			evts := log.snapshot()
			fin := testF105IndexOf(evts, string(mqttTransitionFinalize)+":"+i.ID)
			entry := testF105IndexOf(evts, "discard-entry")
			if fin < 0 {
				t.Fatalf("the committed proposal %q was never finalized: %v", i.ID, evts)
			}
			if entry < 0 {
				t.Fatalf("the overdue successor was never discarded: %v", evts)
			}
			if viaConnect && fin > entry {
				t.Fatalf("the finalize must precede the first discard entry: %v", evts)
			}
		})
	}
}

// T22 (THE RESIDUE) moved to TestF105_8bReceiptlessTeardownWithholds: since F-105 8b the
// residue's teardown withholds the record's release instead of releasing it receiptless
// (consult 121), and its continuation is TestF105_8bExpiryAdoptionContinuesReceiptless.

// T11 / T12: a refused CONNECT that wrote no success CONNACK is certified closed AT its refusal
// — the certificate names the refusal, not the later teardown — and a finalization another
// acceptance completes afterwards cannot make that socket emit. Mutation M11.
func TestF105RefusedConnectIsCertifiedAndStaysSilent(t *testing.T) {
	const cid = "f105-8a-t11"
	s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	type certified struct {
		c    *client
		cert *mqttCloseCert
	}
	var got []certified
	var mu sync.Mutex
	asm.mu.Lock()
	asm.testCertified = func(c *client, cert *mqttCloseCert) {
		mu.Lock()
		got = append(got, certified{c, cert})
		mu.Unlock()
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testCertified = nil
		asm.mu.Unlock()
	})
	// Refused with its committed proposal left standing (the T9 schedule).
	testF105ArmBudget(t, asm, 400*time.Millisecond)
	var armed atomic.Bool
	armed.Store(true)
	testF105ArmStepEntry(t, asm, func(a mqttConnectOwnershipAction) error {
		if a == mqttOwnershipFinalizeHandoff && armed.CompareAndSwap(true, false) {
			time.Sleep(500 * time.Millisecond)
		}
		return nil
	})
	ci := &mqttV5ConnInfo{clientID: cid, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(3600)}
	testF105RefusedConnectV5(t, o, ci)
	mu.Lock()
	refusals := append([]certified(nil), got...)
	mu.Unlock()
	if len(refusals) != 1 || refusals[0].cert == nil ||
		!strings.HasPrefix(refusals[0].cert.Reason, "connect refused") {
		t.Fatalf("the refused CONNECT must be certified at its refusal, got %+v", refusals)
	}
	refused := refusals[0].c
	// T12: another acceptance finalizes the committed proposal.
	testF105ArmBudget(t, asm, 0)
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	testF105ServingAt(t, asm, cid, epoch)
	if refused.outBind.mayWrite() {
		t.Fatal("the refused connection's output gate reopened")
	}
	if cert := refused.outBind.closeCert(); cert == nil || !strings.HasPrefix(cert.Reason, "connect refused") {
		t.Fatalf("the refused connection's certificate must be its refusal's, got %+v", cert)
	}
	if err := refused.writeSocket([]byte{0xd0, 0x00}, time.Time{}); err != nil {
		t.Fatalf("a write on the certified connection must be declined silently, got %v", err)
	}
}

// T13: an ordinary close certifies the connection through the external protocol. Mutation M13.
func TestF105TeardownCertifiesTheConnection(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8a-t13"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)},
		o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	_, sess := testResidentSession(t, s, cid)
	sess.mu.Lock()
	srvClient := sess.c
	sess.mu.Unlock()
	if srvClient == nil || srvClient.outBind == nil {
		t.Fatal("the connection has no binding to certify")
	}
	if srvClient.outBind.closeCert() != nil {
		t.Fatal("a live connection must carry no certificate")
	}
	c.Close()
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		cert := srvClient.outBind.closeCert()
		if cert == nil || cert.Reason != "connection closed" {
			return fmt.Errorf("no teardown certificate yet: %+v", cert)
		}
		return nil
	})
}

// testF105ArmBoundarySettle installs the discard-boundary observation seam for one test.
func testF105ArmBoundarySettle(t *testing.T, asm *mqttAccountSessionManager, f func(id string, steps int)) {
	t.Helper()
	asm.mu.Lock()
	asm.testBoundarySettle = f
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testBoundarySettle = nil
		asm.mu.Unlock()
	})
}

// T15b(b)/(c): a proposal B appears AFTER the discard boundary's read. (b) the discard's claim
// wins the shared record CAS: B's metadata commit is refused predecessor-stale; the discard's
// RECEIPT — built from the boundary's own load, not a later read — names exactly the retiring
// ownership; the close cannot land while B stands, so the receipt-marked record is RETAINED;
// once B is cancelled, the adopter route finishes with a discarding closure. (c) B's commit
// wins: the discard's claim is superseded and deletes nothing of the successor's. Mutation M19.
func TestF105ProposalAfterTheBoundaryReadIsArbitratedByTheRecord(t *testing.T) {
	t.Run("claim-wins", func(t *testing.T) {
		o := testMQTTDefaultOptionsV5()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)
		const cid = "f105-8a-t15bb"
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		asm := testMQTTGetAccountSessionManager(t, s, cid)
		epoch := testF105AccountEpoch(asm)
		retiring := testF105ServingAt(t, asm, cid, epoch)
		e := &testF105Env{asm: asm, id: cid, epoch: epoch}
		var b atomic.Value
		var baseSeq atomic.Uint64
		testF105ArmBoundarySettle(t, asm, func(id string, _ int) {
			if id != cid || b.Load() != nil {
				return
			}
			baseSeq.Store(testF105RecordSeq(t, e))
			i := testF105Intent(t, e, "t15bb")
			testF105Prepare(t, e, i)
			b.Store(i)
		})
		c.Close() // expiry 0: the close discards
		checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
			if b.Load() == nil {
				return errors.New("B was never planted")
			}
			ps, _ := testF105StandingSessionOrNil(asm, cid)
			if ps == nil || ps.Teardown == nil {
				return errors.New("the discard's teardown marker is not standing yet")
			}
			return nil
		})
		i := b.Load().(*mqttHandoffIntent)
		rcpt, _, st := asm.jsa.loadTeardownReceipt(asm.domainTk, getHash(cid), retiring.Gen)
		if st != mqttOutboundValid || rcpt == nil {
			t.Fatalf("the discard must carry a retirement receipt built from the boundary's load, got status %v", st)
		}
		if rcpt.Retire.Gen != retiring.Gen || rcpt.Retire.Tenure != retiring.Tenure ||
			rcpt.Retire.OwnerTxn != retiring.OwnerTxn || !rcpt.Retire.Discard {
			t.Fatalf("the receipt must name exactly the retiring ownership %+v, got %+v", retiring, rcpt.Retire)
		}
		if _, err := asm.jsa.commitMetadataAttempt(testF105FreezeAttempt(t, e, i, baseSeq.Load())); err == nil {
			t.Fatal("B's metadata commit must lose to the discard's claim on the shared record")
		}
		if testF105ClosureDiscards(asm, cid, epoch, retiring.Gen) {
			t.Fatal("the close cannot have landed while B's preparation stands")
		}
		testF105ResolveStanding(t, e, i) // B settles itself: aborted, cancelled
		ps, seq := testF105StandingSessionOrNil(asm, cid)
		if ps == nil || ps.Teardown == nil {
			t.Fatal("the receipt-marked record must be retained until the close lands")
		}
		if err := asm.finishAdoptedTeardown(ps, getHash(cid), seq, s.getOpts()); err != nil {
			t.Fatalf("the adopter route: %v", err)
		}
		if !testF105ClosureDiscards(asm, cid, epoch, retiring.Gen) {
			t.Fatalf("the adopter must finish with a discarding closure of %q", retiring.Gen)
		}
	})
	t.Run("commit-wins", func(t *testing.T) {
		o := testMQTTDefaultOptionsV5()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)
		const cid = "f105-8a-t15bc"
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "foo", opts: 1}})
		asm := testMQTTGetAccountSessionManager(t, s, cid)
		epoch := testF105AccountEpoch(asm)
		e := &testF105Env{asm: asm, id: cid, epoch: epoch}
		var successorWID atomic.Value
		testF105ArmBoundarySettle(t, asm, func(id string, _ int) {
			if id != cid || successorWID.Load() != nil {
				return
			}
			i := testF105Intent(t, e, "t15bc")
			testF105Prepare(t, e, i)
			f := testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))
			if _, err := asm.jsa.commitMetadataAttempt(f); err != nil {
				t.Errorf("B's metadata commit: %v", err)
			}
			ps, _ := testF105StandingSessionOrNil(asm, cid)
			if ps != nil {
				successorWID.Store(ps.WID)
			}
		})
		c.Close()
		checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
			if successorWID.Load() == nil {
				return errors.New("B's successor was never committed")
			}
			return nil
		})
		time.Sleep(200 * time.Millisecond)
		ps, _ := testF105StandingSessionOrNil(asm, cid)
		if ps == nil || ps.WID != successorWID.Load().(string) || ps.Teardown != nil {
			t.Fatalf("the discard's claim must lose to B's committed successor and touch nothing of it, got %+v", ps)
		}
	})
}

// testF105StandingSessionOrNil reads the client's standing session record, or nil.
func testF105StandingSessionOrNil(asm *mqttAccountSessionManager, cid string) (*mqttPersistedSession, uint64) {
	smsg, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(cid)))
	if err != nil || smsg == nil {
		return nil, 0
	}
	return mqttDecodeSessionRecord(smsg), smsg.Sequence
}

// T16: ONE foreign target per CONNECT, across a full re-drive. The CONNECT settles foreign
// proposal A at its top, is forced into a full re-drive (a rival record write moves its
// predecessor), and meets a SECOND foreign proposal B planted as its own failed attempt is
// cleaned up. It refuses rather than settle B, and B is untouched. Mutation M14.
func TestF105OneForeignTargetPerConnectAcrossARedrive(t *testing.T) {
	const cid = "f105-8a-t16"
	s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
	// A: a crash-left unresolved preparation.
	testF105ArmBeforeMetadata(t, asm, testF105StopOnce(mqttOwnershipPrepareHandoff))
	testF105RefusedConnectV5(t, o, ci)
	a, _ := testF105StandingProposal(t, asm, cid, epoch)
	if a == _EMPTY_ {
		t.Fatal("A was never left standing")
	}
	e := &testF105Env{asm: asm, id: cid, epoch: epoch}
	var rival, plantedB atomic.Bool
	var b atomic.Value
	// The re-drive: once THIS CONNECT's own preparation lands, a rival rewrites the record.
	testF105ArmBeforeMetadata(t, asm, func(act mqttConnectOwnershipAction) error {
		if act != mqttOwnershipPrepareHandoff || !rival.CompareAndSwap(false, true) {
			return nil
		}
		ps, _ := testF105StandingSessionOrNil(asm, cid)
		if ps == nil {
			t.Error("no record to rewrite")
			return nil
		}
		ps.WID = mqttNewInflightNonce()
		body, _ := json.Marshal(ps)
		if _, err := asm.jsa.storeSessionMsg(asm.domainTk, getHash(cid), 0, body); err != nil {
			t.Errorf("the rival write: %v", err)
		}
		return nil
	})
	// B: planted the moment this CONNECT's own failed preparation is cancelled.
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err == nil && rival.Load() && tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionCancelIntent &&
			testF105ProposalOf(tr) != a && plantedB.CompareAndSwap(false, true) {
			i := testF105Intent(t, e, "t16-b")
			if _, perr := asm.jsa.opPrepareHandoff(cid, epoch, "txn-prep-b", i); perr != nil {
				t.Errorf("planting B: %v", perr)
			}
			b.Store(i)
		}
		return seq, err
	})
	testF105RefusedConnectV5(t, o, ci)
	bi, _ := b.Load().(*mqttHandoffIntent)
	if bi == nil {
		t.Fatal("the re-drive never happened, so B was never planted and nothing here is tested")
	}
	if standing, state := testF105StandingProposal(t, asm, cid, epoch); standing != bi.ID || state != mqttAuthorityPreparing {
		t.Fatalf("B must be untouched by a CONNECT that already settled A, got %q %v", standing, state)
	}
	if v := asm.jsa.resolveMetadataAttempt(_EMPTY_, cid, epoch, testF105Prop(t, bi)); v != mqttMetadataUnresolved {
		t.Fatalf("B's slot must be untouched, got %v", v)
	}
}

// M8 integration: the owed-abandonment worker acts on its CAPTURED proposal only. The captured
// preparation is cancelled and an ABORTED replacement R — one the cancellation boundary would
// accept — stands in its place before the worker runs; R stays standing.
func TestF105AbandonmentWorkerNeverRetargets(t *testing.T) {
	const cid = "f105-8a-m8"
	s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	var block atomic.Bool
	block.Store(true)
	own := testF105OwnAbortedByPeer(t, asm, cid, epoch, mqttOwnershipPrepareHandoff, &block, 0)
	gate := testF105ArmAbandonGate(t, asm)
	testF105RefusedConnectV5(t, o, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)})
	id, _ := own.Load().(string)
	if id == _EMPTY_ {
		t.Fatal("no own proposal was aborted")
	}
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if gate.launched.Load() == 0 {
			return errors.New("the worker was never launched")
		}
		return nil
	})
	block.Store(false)
	// Somebody else cancels the captured preparation and an ABORTED replacement R stands.
	if _, err := asm.jsa.opCancelHandoff(cid, epoch, "txn-other-cancel", id); err != nil {
		t.Fatalf("cancelling the captured preparation: %v", err)
	}
	e := &testF105Env{asm: asm, id: cid, epoch: epoch}
	rep := testF105Intent(t, e, "m8-r")
	if _, err := asm.jsa.opPrepareHandoff(cid, epoch, "txn-prep-r", rep); err != nil {
		t.Fatalf("standing R: %v", err)
	}
	if v := asm.jsa.settleProposal(_EMPTY_, cid, epoch, testF105Prop(t, rep)); v != mqttMetadataAborted {
		t.Fatalf("aborting R's slot returned %v", v)
	}
	close(gate.release)
	time.Sleep(500 * time.Millisecond)
	if standing, state := testF105StandingProposal(t, asm, cid, epoch); standing != rep.ID || state != mqttAuthorityPreparing {
		t.Fatalf("the worker retargeted onto the replacement: R %q must still stand, got %q %v", rep.ID, standing, state)
	}
}

// M25: a RESIDENT object whose retirement is pending, with an owner still holding it, under a
// committed foreign handoff: it is NOT foreign-disposed (that would leave its parked obligation
// to nobody and declare nothing about its owners); the CONNECT's existing join waits for the
// owner to drain and finishes the discard, whose claim the moved record supersedes; the CONNECT
// then restores from the record and is accepted.
func TestF105RetirementPendingResidentTakesTheJoin(t *testing.T) {
	const cid = "f105-8a-m25"
	s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	es := testF105ResidentObject(asm, cid)
	if es == nil {
		t.Fatal("no resident object")
	}
	es.mu.Lock()
	tr := es.leases
	es.mu.Unlock()
	lease, ok := tr.acquire()
	if !ok {
		t.Fatal("the tracker must admit an owner")
	}
	tr.close()
	if _, armed := tr.armRetirementPending(func() {}); !armed {
		t.Fatal("a closing tracker must accept the obligation")
	}
	testF105HoldResident(t, asm, cid)
	e := &testF105Env{asm: asm, id: cid, epoch: epoch}
	i := testF105Intent(t, e, "m25")
	testF105Prepare(t, e, i)
	if _, err := asm.jsa.commitMetadataAttempt(testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))); err != nil {
		t.Fatalf("committing the foreign handoff: %v", err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		lease.release()
	}()
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	if tr.isRetirementPending() {
		t.Fatal("the pending discard was never claimed: the join did not run")
	}
	if rec := testF105ServingAt(t, asm, cid, epoch); rec.Gen != i.Gen {
		t.Fatalf("the resume must serve the committed generation %q, got %q", i.Gen, rec.Gen)
	}
}

// M27: an object evicted while its retirement is pending — a foreign record found before any
// CONNECT's join ran — is never destroyed, so its finisher, which runs only after the owners
// drained, resolves the tracker: a handler parked on its decision returns.
func TestF105EvictedPendingDiscardResolvesItsTracker(t *testing.T) {
	e := testNewInflightEnv(t, "f105-8a-m27")
	e.sess.mu.Lock()
	e.sess.c = nil
	stale := e.sess
	captured := stale.leases
	e.sess.mu.Unlock()
	captured.close()
	if _, armed := captured.armRetirementPending(func() {}); !armed {
		t.Fatal("a closing tracker must accept the obligation")
	}
	closing, decided := captured.closingAndDecided()
	if !closing {
		t.Fatal("the tracker must be closing")
	}
	// Evicted: the object is no longer the client's session.
	e.asm.removeSession(stale, true)
	e.s.finishPendingDiscard(e.asm, stale, captured)
	select {
	case <-decided:
	case <-time.After(2 * time.Second):
		t.Fatal("a handler parked on the evicted object's tracker never returns: nothing resolved it")
	}
	if captured.wasReopened(decided) {
		t.Fatal("the evicted object's retirement is final, not a reopen")
	}
}

// T18r: a RESIDENT G0 object under a COMMITTED generation-changing foreign handoff to G1, and a
// competing settler finalizes it while our settlement is reading its slot — before our reload.
// Our CONNECT executed no finalize of its own; the resident object is stale all the same, and
// the trigger is the STATE (the authority serves another generation), not our act. Mutation M28.
func TestF105CompetitorFinalizesUnderAResidentObject(t *testing.T) {
	const cid = "f105-8a-t18r"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	testF105HoldResident(t, asm, cid)
	e := &testF105Env{asm: asm, id: cid, epoch: epoch}
	i := testF105Intent(t, e, "t18r")
	testF105Prepare(t, e, i)
	if _, err := asm.jsa.commitMetadataAttempt(testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))); err != nil {
		t.Fatalf("committing the foreign handoff: %v", err)
	}
	if i.Gen == oldGen {
		t.Fatal("the foreign handoff must change the generation")
	}
	receipt := mqttHandoffOutcomeSubject(_EMPTY_, getHash(cid), i.ID)
	var raced atomic.Bool
	hook := func(stream, subject string) error {
		if subject == receipt && raced.CompareAndSwap(false, true) {
			if _, err := asm.jsa.opFinalizeHandoff(cid, epoch, "txn-competitor", i.ID); err != nil {
				t.Errorf("the competitor's finalize: %v", err)
			}
		}
		return nil
	}
	mqttTestLoadLastMsgHookV.Store(&hook)
	t.Cleanup(func() { mqttTestLoadLastMsgHookV.Store(nil) })
	before := testF105ResidentObject(asm, cid)
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	if !raced.Load() {
		t.Fatal("the competitor never raced, so nothing here is tested")
	}
	if after := testF105ResidentObject(asm, cid); after == before {
		t.Fatal("the stale resident object is still the client's session")
	}
	if rec := testF105ServingAt(t, asm, cid, epoch); rec.Gen != i.Gen {
		t.Fatalf("the resume must serve the committed generation %q, got %q", i.Gen, rec.Gen)
	}
}

// T2 resident, BOUND incumbent: the stale resident object still has its connection, which
// armed a Will. Disposing it as foreign closes that connection and CONSUMES its Will — no
// successor of it was established by this CONNECT, so its Will is not a takeover's to discard
// (F-084). A disposal that did nothing would leave the object to an ordinary takeover, which
// discards the Will. Mutation M26.
func TestF105StaleResidentIncumbentIsDisposedAsForeign(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const cid = "f105-8a-t2b"
	const willTopic = "f105/8a/will"
	w, wr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f105-8a-t2b-watch"}, o.MQTT.Host, o.MQTT.Port)
	defer w.Close()
	testMQTTReadConnAckV5(t, wr)
	testMQTTSubV5(t, w, wr, 1, []mqttV5SubFilter{{topic: willTopic, opts: 0}})

	c1, r1 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600),
		will: &mqttWill{topic: []byte(willTopic), message: []byte("gone")}}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTReadConnAckV5(t, r1)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	epoch := testF105AccountEpoch(asm)
	testF105HoldResident(t, asm, cid)
	e := &testF105Env{asm: asm, id: cid, epoch: epoch}
	i := testF105Intent(t, e, "t2b")
	testF105Prepare(t, e, i)
	if _, err := asm.jsa.commitMetadataAttempt(testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))); err != nil {
		t.Fatalf("committing the foreign handoff: %v", err)
	}

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	// The incumbent's Will is CONSUMED: published, since it carries no delay.
	testMQTTReadPublishV5(t, wr, willTopic, []byte("gone"))
	// The incumbent is told, with the foreign-disposal notice — not a takeover's.
	if reason, _ := testMQTTReadDisconnectV5(t, r1); reason == mqttReasonSessionTakenOver {
		t.Fatalf("the incumbent was disposed as a TAKEOVER (reason %#x); nothing established a successor", reason)
	}
	if rec := testF105ServingAt(t, asm, cid, epoch); rec.Gen != i.Gen {
		t.Fatalf("the resume must serve the committed generation %q, got %q", i.Gen, rec.Gen)
	}
}

// H-1: WHO ABORTED THE PROPOSAL decides whether the CONNECT may propose again. The acceptance's
// preparation lands, then its predecessor record moves (a rival's write) so its metadata commit
// is refused predecessor-stale. (peer) If another party had ALREADY aborted the proposal's slot,
// the acceptance has lost: it cleans up and REFUSES, with exactly one proposal made. (self) If
// the slot was empty, this acceptance's own settlement aborts it; that is only the lost record
// race, and the CONNECT re-drives against the winner and is accepted. Mutation M29.
func TestF105PeerAbortedProposalIsNotReproposedByARedrive(t *testing.T) {
	for _, peer := range []bool{true, false} {
		t.Run(fmt.Sprintf("peer=%v", peer), func(t *testing.T) {
			cid := fmt.Sprintf("f105-8a-h1-%v", peer)
			s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
			defer done()
			o := s.getOpts()
			var armed atomic.Bool
			armed.Store(true)
			testF105ArmBeforeMetadata(t, asm, func(a mqttConnectOwnershipAction) error {
				if a != mqttOwnershipPrepareHandoff || !armed.CompareAndSwap(true, false) {
					return nil
				}
				if peer {
					p := testF105IntentProposal(t, asm, cid, epoch)
					if v := asm.jsa.settleProposal(_EMPTY_, cid, epoch, p); v != mqttMetadataAborted {
						t.Errorf("the peer's abort returned %v", v)
					}
				}
				ps, _ := testF105StandingSessionOrNil(asm, cid)
				if ps == nil {
					t.Error("no record to rewrite")
					return nil
				}
				ps.WID = mqttNewInflightNonce()
				body, _ := json.Marshal(ps)
				if _, err := asm.jsa.storeSessionMsg(asm.domainTk, getHash(cid), 0, body); err != nil {
					t.Errorf("the rival write: %v", err)
				}
				return nil
			})
			var prepares atomic.Int32
			testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				seq, err := send()
				if err == nil && tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionPrepareIntent {
					prepares.Add(1)
				}
				return seq, err
			})
			ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
			if peer {
				testF105RefusedConnectV5(t, o, ci)
				if n := prepares.Load(); n != 1 {
					t.Fatalf("a CONNECT whose proposal another party aborted must not propose again: %d proposals", n)
				}
				testF105ServingAt(t, asm, cid, epoch) // its preparation was cancelled back
				return
			}
			c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			testMQTTReadConnAckV5(t, r2)
			if n := prepares.Load(); n < 2 {
				t.Fatalf("a lost record race must re-drive against the winner: %d proposals", n)
			}
		})
	}
}

// H-2 (a): an empty or unreadable slot read proves nothing about who aborted. A peer aborts
// this acceptance's proposal at the very read the settlement makes first, and that read is
// made to fail: the acceptance's own create-only abort write then fails too, so it cannot show
// the abort was its own — it LOST, refuses, and proposes exactly once. Mutation M30.
func TestF105UnprovenSelfAbortIsALoss(t *testing.T) {
	const cid = "f105-8a-h2a"
	s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	var receipt atomic.Value
	var armed atomic.Bool
	armed.Store(true)
	testF105ArmBeforeMetadata(t, asm, func(a mqttConnectOwnershipAction) error {
		if a != mqttOwnershipPrepareHandoff || !armed.CompareAndSwap(true, false) {
			return nil
		}
		p := testF105IntentProposal(t, asm, cid, epoch)
		receipt.Store(p)
		// The record race this acceptance then loses: a rival's write moves its predecessor.
		ps, _ := testF105StandingSessionOrNil(asm, cid)
		ps.WID = mqttNewInflightNonce()
		body, _ := json.Marshal(ps)
		if _, err := asm.jsa.storeSessionMsg(asm.domainTk, getHash(cid), 0, body); err != nil {
			t.Errorf("the rival write: %v", err)
		}
		return nil
	})
	var peerDone atomic.Bool
	hook := func(stream, subject string) error {
		p, ok := receipt.Load().(mqttOwnershipProposal)
		if !ok || subject != mqttHandoffOutcomeSubject(_EMPTY_, getHash(cid), p.ID) || !peerDone.CompareAndSwap(false, true) {
			return nil
		}
		if err := asm.jsa.abortMetadataAttempt(_EMPTY_, cid, epoch, p); err != nil {
			t.Errorf("the peer's abort: %v", err)
		}
		return errors.New("f105 8a test: the slot read failed")
	}
	mqttTestLoadLastMsgHookV.Store(&hook)
	t.Cleanup(func() { mqttTestLoadLastMsgHookV.Store(nil) })
	var prepares atomic.Int32
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err == nil && tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionPrepareIntent {
			prepares.Add(1)
		}
		return seq, err
	})
	testF105RefusedConnectV5(t, o, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)})
	if !peerDone.Load() {
		t.Fatal("the peer's abort never ran at the settlement's read, so nothing here is tested")
	}
	if n := prepares.Load(); n != 1 {
		t.Fatalf("an acceptance that cannot prove its own abort must not propose again: %d proposals", n)
	}
}

// H-2 (b): an ABORTED verdict observed by the metadata-contention path is a loss even when the
// cleanup that follows fails with a retryable store error: the returned error must not be
// classifiable as that contention, and the observation must record the loss, which the accept
// loop checks before any retry arm. Mutation M31.
func TestF105ObservedLossSurvivesARetryableCleanupError(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-8a-h2b")
	defer done()
	i := testF105Intent(t, e, "h2b")
	testF105Prepare(t, e, i)
	p := testF105Prop(t, i)
	if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, p); v != mqttMetadataAborted {
		t.Fatalf("the peer's abort returned %v", v)
	}
	detail := mqttSessSaveDetail{ownFrozen: testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))}
	contention := ApiErrors[JSStreamWrongLastSequenceConstantErr]
	obs := &mqttOwnObservation{}
	obs.noteOwn(p, mqttAuthorityPreparing) // the preparation landed
	sess := testReconcileSession(e)
	_, err := sess.resolveOrRetryMetadataCommit(&e.asm.jsa, obs, p, detail, contention, _EMPTY_,
		mqttOwnershipStep{Action: mqttOwnershipPrepareHandoff},
		func() error { return errors.Join(errors.New("cleanup contended"), contention) },
		func() error { return fmt.Errorf("cleanup: %w", contention) },
		func(*mqttForeignRecovery) mqttConnectOwnershipInput { return mqttConnectOwnershipInput{} })
	if err == nil {
		t.Fatal("a proposal another party aborted must be refused")
	}
	if ae := jsAPIError(err); ae != nil && IsNatsErr(ae, JSStreamWrongLastSequenceConstantErr) {
		t.Fatalf("the loss must not be classifiable as a retryable contention: %v", err)
	}
	if !obs.forbidsRedrive() {
		t.Fatal("the observation must forbid a re-drive: an own proposal aborted by another party")
	}
}

// H-3: the settlement cannot read the proposal's slot at all. A peer has aborted it, the
// metadata commit is refused predecessor-stale, BOTH reads of the slot fail and the
// acceptance's own abort write fails because the peer owns the slot. Nothing proves a self-abort,
// so the CONNECT refuses with exactly one proposal; once reads recover, the proposal stands for
// the next acceptance to settle. Mutation M33.
func TestF105UnreadableSettlementIsNotARedrive(t *testing.T) {
	const cid = "f105-8a-h3"
	s, asm, epoch, _, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	var prop atomic.Value
	var armed, failing atomic.Bool
	armed.Store(true)
	testF105ArmBeforeMetadata(t, asm, func(a mqttConnectOwnershipAction) error {
		if a != mqttOwnershipPrepareHandoff || !armed.CompareAndSwap(true, false) {
			return nil
		}
		p := testF105IntentProposal(t, asm, cid, epoch)
		if v := asm.jsa.settleProposal(_EMPTY_, cid, epoch, p); v != mqttMetadataAborted {
			t.Errorf("the peer's abort returned %v", v)
		}
		prop.Store(p)
		failing.Store(true)
		ps, _ := testF105StandingSessionOrNil(asm, cid)
		ps.WID = mqttNewInflightNonce()
		body, _ := json.Marshal(ps)
		if _, err := asm.jsa.storeSessionMsg(asm.domainTk, getHash(cid), 0, body); err != nil {
			t.Errorf("the rival write: %v", err)
		}
		return nil
	})
	var failed atomic.Int32
	hook := func(stream, subject string) error {
		p, ok := prop.Load().(mqttOwnershipProposal)
		if !ok || !failing.Load() || subject != mqttHandoffOutcomeSubject(_EMPTY_, getHash(cid), p.ID) {
			return nil
		}
		// Exactly the settlement's two reads fail; reads then RECOVER, so a re-drive would meet
		// a readable ABORTED slot and could propose again — which is what must not happen.
		if failed.Add(1) >= 2 {
			failing.Store(false)
		}
		return errors.New("f105 8a test: the slot cannot be read")
	}
	mqttTestLoadLastMsgHookV.Store(&hook)
	t.Cleanup(func() { mqttTestLoadLastMsgHookV.Store(nil) })
	var prepares atomic.Int32
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err == nil && tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionPrepareIntent {
			prepares.Add(1)
		}
		return seq, err
	})
	ci := &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)}
	testF105RefusedConnectV5(t, o, ci)
	failing.Store(false)
	if failed.Load() < 2 {
		t.Fatalf("both settlement reads must have failed for this test to mean anything, %d did", failed.Load())
	}
	if n := prepares.Load(); n != 1 {
		t.Fatalf("an acceptance that could not settle its proposal must not propose again: %d proposals", n)
	}
	p := prop.Load().(mqttOwnershipProposal)
	if standing, _ := testF105StandingProposal(t, asm, cid, epoch); standing != p.ID {
		t.Fatalf("the unsettled proposal %q must stand for the next acceptance, got %q", p.ID, standing)
	}
}

// Consult 120: the accept loop's gate decides from the drive's OBSERVATION, not from the error.
// The acceptance's preparation lands and the step then fails with each retryable identity the
// accept loop knows; the observation holds an own, unsettled proposal, so the CONNECT refuses
// with exactly one proposal whatever the error claims. Mutation M32.
func TestF105RedriveIsLicensedByTheObservationNotTheError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"10164", ApiErrors[JSStreamWrongLastSequenceConstantErr]},
		{"record-cas", ApiErrors[JSStreamWrongLastSequenceErrF]},
		{"predecessor-stale", fmt.Errorf("test: %w", errMQTTHandoffPredecessorStale)},
		{"fork-fence", fmt.Errorf("test: %w", errMQTTForkFenceMoved)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cid := "f105-8a-gate-" + tc.name
			s, asm, _, _, done := testF105CleanStartEnv(t, cid)
			defer done()
			o := s.getOpts()
			testF105ArmBeforeMetadata(t, asm, func() func(mqttConnectOwnershipAction) error {
				var armed atomic.Bool
				armed.Store(true)
				return func(a mqttConnectOwnershipAction) error {
					if a == mqttOwnershipPrepareHandoff && armed.CompareAndSwap(true, false) {
						return tc.err
					}
					return nil
				}
			}())
			var prepares atomic.Int32
			testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				seq, err := send()
				if err == nil && tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionPrepareIntent {
					prepares.Add(1)
				}
				return seq, err
			})
			testF105RefusedConnectV5(t, o, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)})
			if n := prepares.Load(); n != 1 {
				t.Fatalf("a %s error over an unsettled own proposal must not license a re-drive: %d proposals", tc.name, n)
			}
		})
	}
}

// Consult 120, observation completeness: a preparation whose transition LANDED but cannot be
// resolved (its response lost, its outcome unreadable) may stand, so it is this acceptance's own
// proposal for the gate. The step fails with a retryable identity; the CONNECT still refuses
// with exactly one proposal. Mutation M34.
func TestF105UnresolvedPreparationCountsAsOwn(t *testing.T) {
	const cid = "f105-8a-m34"
	s, asm, _, _, done := testF105CleanStartEnv(t, cid)
	defer done()
	o := s.getOpts()
	var prepares atomic.Int32
	var txn atomic.Value
	var armed atomic.Bool
	armed.Store(true)
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err != nil || tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionPrepareIntent {
			return seq, err
		}
		prepares.Add(1)
		if armed.CompareAndSwap(true, false) {
			txn.Store(tr.Attempt.Txn)
			return 0, ApiErrors[JSStreamWrongLastSequenceConstantErr] // landed; its response lost
		}
		return seq, err
	})
	hook := func(stream, subject string) error {
		if x, ok := txn.Load().(string); ok && subject == mqttTransitionOutcomeSubject(getHash(cid), x) {
			return errors.New("f105 8a test: the transition outcome cannot be read")
		}
		return nil
	}
	mqttTestLoadLastMsgHookV.Store(&hook)
	t.Cleanup(func() { mqttTestLoadLastMsgHookV.Store(nil) })
	testF105RefusedConnectV5(t, o, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(3600)})
	if txn.Load() == nil {
		t.Fatal("the preparation's response was never lost, so nothing here is tested")
	}
	if n := prepares.Load(); n != 1 {
		t.Fatalf("a preparation that may stand is the CONNECT's own: it must not propose again (%d proposals)", n)
	}
}
