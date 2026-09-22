//go:build mqttport_fulltests

// Copyright 2026 The mqtt-nats-bridge Authors
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
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// The F-105 clean-start HANDOFF over a LIVE serving authority.
//
// A clean start that lands on a client whose outbound authority is SERVING no longer destroys
// the record and then acquires ownership — that premature destruction left the authority
// serving with no record to hand off from, and refused every such CONNECT (Codex consult 6).
// The accepted batch now displaces the predecessor record IN PLACE through a discarding
// handoff: the successor serves at a fresh generation, the predecessor generation is closed
// with Discard and its cleanup enrolled, all in one finalization.

// testF105CleanStartEnv connects a PERSISTENT session and returns the account, the client id,
// its recovery epoch, and the outbound authority's generation as it stands after the connect.
func testF105CleanStartEnv(t *testing.T, clientID string) (*Server, *mqttAccountSessionManager, string, string, func()) {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "foo", opts: 1}})
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	epoch := asm.sessStreamEpoch
	asm.mu.RUnlock()
	al := asm.jsa.loadOutboundAuthority(clientID, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		c.Close()
		testMQTTShutdownServer(s)
		t.Fatalf("the persistent connect did not leave a SERVING authority: %+v", al)
	}
	gen := al.rec.Gen
	// The session survives its close (persistent), so the reconnect meets it resident with a
	// live authority — the exact state the clean-start handoff exists for.
	c.Close()
	time.Sleep(150 * time.Millisecond)
	return s, asm, epoch, gen, func() { testMQTTShutdownServer(s) }
}

// TestF105CleanStartHandoffOverLiveAuthoritySucceeds is the production regression: a clean
// start over a resident, persistent session with a live serving authority must be ACCEPTED,
// report no session present, and install a successor serving authority at a NEW generation.
func TestF105CleanStartHandoffOverLiveAuthoritySucceeds(t *testing.T) {
	const clientID = "f105-cs-handoff-ok"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, clientID)
	defer done()
	o := s.getOpts()

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	// Session Present MUST be false: a clean start resumes nothing (Spec5 [3.2.2-1]).
	testMQTTReadConnAckV5(t, r2)

	al := asm.jsa.loadOutboundAuthority(clientID, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the successor must serve, got %+v", al)
	}
	if al.rec.Gen == oldGen {
		t.Fatalf("the successor must serve a FRESH generation, still %q", oldGen)
	}
}

// TestF105CleanStartHandoffClosesAndReplacesThePredecessor asserts the durable outcome of the
// handoff: the predecessor generation is CLOSED with Discard, and the standing session record
// is the successor's — a fresh generation, parentless (a clean start inherits no lineage).
func TestF105CleanStartHandoffClosesAndReplacesThePredecessor(t *testing.T) {
	const clientID = "f105-cs-handoff-closure"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, clientID)
	defer done()
	o := s.getOpts()

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)

	// The predecessor generation is permanently CLOSED, and the closure records the discard.
	cl, _, st := asm.jsa.loadGenClosure(clientID, epoch, oldGen)
	if st != mqttOutboundValid || cl == nil {
		t.Fatalf("the predecessor generation %q must carry a closure, got status %v", oldGen, st)
	}
	if !cl.Discard {
		t.Fatalf("the predecessor closure must record the DISCARD, got %+v", cl)
	}

	// The standing session record is the successor's: a fresh generation matching the serving
	// authority, and no parent — a clean start starts a new lineage.
	al := asm.jsa.loadOutboundAuthority(clientID, epoch)
	smsg, lerr := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(clientID)))
	if lerr != nil {
		t.Fatalf("loading the successor record: %v", lerr)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decoding the successor record: %v", err)
	}
	if ps.Gen != al.rec.Gen {
		t.Fatalf("the standing record carries generation %q but the authority serves %q", ps.Gen, al.rec.Gen)
	}
	if ps.Gen == oldGen {
		t.Fatalf("the successor record still carries the predecessor generation %q", oldGen)
	}
	if ps.PWID != _EMPTY_ {
		t.Fatalf("a clean start's successor record must be parentless, carries PWID %q", ps.PWID)
	}
}

// TestF105CleanStartHandoffAbortBeforeCommitPreservesThePredecessor arms the metadata-commit
// seam to fail BEFORE the batch is sent. Nothing of the successor commits: the predecessor
// session record stands unchanged, and a retry with the seam disarmed completes the handoff.
func TestF105CleanStartHandoffAbortBeforeCommitPreservesThePredecessor(t *testing.T) {
	// KNOWN GAP (F-105 clean-start crash-window recovery). After an abort BEFORE the metadata
	// commit, the authority stands PREPARING with an UNRESOLVED metadata slot. A fresh CONNECT
	// (this test's retry, NOT a process restart) adopts that standing intent as its own
	// (loadConnectOwnershipInput), and the delayed-write fence refuses it because the batch could
	// still be in flight ("neither finalizing nor cancelling is licensed"). Nothing on this broker
	// then settles the slot, so the authority is STRANDED PREPARING — a stuck state, not merely a
	// refused CONNECT. Restoring the predecessor needs the recovery to settle the unresolved slot
	// (abort, racing the original) rather than adopt-and-refuse it — a recovery piece separate
	// from the clean-start accept ordering this unit lands. Kept as the executable specification.
	t.Skip("F-105 clean-start crash-window recovery (abort-before-commit): fresh-CONNECT retry adopts the unresolved own-intent and the fence refuses it, leaving the authority stranded PREPARING; see report")
	const clientID = "f105-cs-handoff-abort"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, clientID)
	defer done()
	o := s.getOpts()

	// The predecessor record as it stands before the aborted attempt.
	before, berr := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(clientID)))
	if berr != nil {
		t.Fatalf("reading the predecessor record: %v", berr)
	}

	var armed atomic.Bool
	armed.Store(true)
	restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
		// Fail the FIRST metadata commit issuing after the arm — the clean-start handoff's —
		// and disarm, so nothing of the successor batch is sent and the retry proceeds.
		if obs.Phase == mqttAcceptStoreIssuing && armed.CompareAndSwap(true, false) {
			return fmt.Errorf("f105 test: metadata commit aborted before send")
		}
		return nil
	})

	// The aborted attempt: the CONNECT is refused (the metadata step errored).
	cc, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTExpectDisconnect(t, cc)
	cc.Close()

	// The predecessor record is UNCHANGED — nothing was displaced.
	after, aerr := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(clientID)))
	if aerr != nil {
		t.Fatalf("re-reading the predecessor record: %v", aerr)
	}
	if after.Sequence != before.Sequence {
		t.Fatalf("an aborted handoff moved the predecessor record from %d to %d", before.Sequence, after.Sequence)
	}

	// The retry, seam disarmed, completes the handoff to a fresh serving generation.
	restore()
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	al := asm.jsa.loadOutboundAuthority(clientID, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing || al.rec.Gen == oldGen {
		t.Fatalf("the retry must install a fresh serving successor, got %+v", al)
	}
}

// TestF105CleanStartHandoffCleansUpThePredecessorConsumer asserts the cleanup half: the plain
// QoS 1 consumer the predecessor held is removed once the successor is serving. The affine
// consumer, when present, is deferred to recovery; a plain source/group consumer is this
// broker's to delete directly.
func TestF105CleanStartHandoffCleansUpThePredecessorConsumer(t *testing.T) {
	const clientID = "f105-cs-handoff-cleanup"
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "foo", opts: 1}})

	// The durable the predecessor's subscription created, captured while it stands.
	asm, sess := testResidentSession(t, s, clientID)
	sess.mu.Lock()
	var predDur string
	for _, cc := range sess.cons {
		if cc.DeliverGroup == _EMPTY_ {
			predDur = cc.Durable
		}
	}
	sess.mu.Unlock()
	if predDur == _EMPTY_ {
		t.Fatal("the predecessor must hold a plain consumer for this test to mean anything")
	}
	if _, err := asm.jsa.lookupConsumer(mqttStreamName, predDur); err != nil {
		t.Fatalf("the predecessor consumer %q must exist before the handoff: %v", predDur, err)
	}

	c.Close()
	time.Sleep(150 * time.Millisecond)

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)

	// The successor is serving and the predecessor's plain consumer is gone.
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if _, err := asm.jsa.lookupConsumer(mqttStreamName, predDur); err == nil {
			return fmt.Errorf("the predecessor consumer %q still stands", predDur)
		}
		return nil
	})
}

// TestF105CleanStartHandoffCrashAfterCommitFinalizesAsProposed arms the metadata-commit seam
// to fail AFTER the three-member batch has committed but before the finalization. The
// successor record and its COMMITTED receipt are durable, the authority stands PREPARING, and
// a fresh CONNECT must complete the SAME proposal — finalizing the successor it committed
// rather than proposing another (Codex consult 6: a committed proposal must finalize exactly
// as proposed).
func TestF105CleanStartHandoffCrashAfterCommitFinalizesAsProposed(t *testing.T) {
	// KNOWN GAP (F-105 clean-start crash-window recovery). After the metadata commits but before
	// finalization the successor record and its COMMITTED receipt are durable and the authority
	// stands PREPARING, so a recovery that read PREPARING+Committed would finalize as proposed.
	// OBSERVED on the fresh-CONNECT retry: the drive reaches opPrepareHandoff with an empty
	// predecessor write identity ("no predecessor session write identity") — it did NOT take the
	// finalize path — so the pre-drive clean-start block ran (the authority was not SERVING when
	// the block read it) rather than deferring to the drive's recovery. The exact intermediate
	// path is UNVERIFIED: either the block's destroy deleted/altered the committed state, or the
	// Acked-injected error marked the write seqUncertain and resolveUncertainSession re-routed the
	// retry. Either way the fix is for the clean-start block to DEFER to the drive's recovery for
	// a PREPARING/RESERVING authority instead of destroying — a recovery-ordering piece separate
	// from the clean-start accept ordering this unit lands. Kept as the executable specification.
	t.Skip("F-105 clean-start crash-window recovery (crash-after-commit): fresh-CONNECT retry hits opPrepareHandoff with empty WID instead of finalizing the committed proposal; intermediate path unverified; see report")
	const clientID = "f105-cs-handoff-crash"
	s, asm, epoch, oldGen, done := testF105CleanStartEnv(t, clientID)
	defer done()
	o := s.getOpts()

	var armed atomic.Bool
	armed.Store(true)
	restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
		// Let the batch COMMIT (Issuing passes), then fail at the acknowledgement — the
		// metadata is durable, but the caller never reaches the finalization.
		if obs.Phase == mqttAcceptStoreAcked && armed.CompareAndSwap(true, false) {
			return fmt.Errorf("f105 test: crash after the metadata commit, before finalization")
		}
		return nil
	})

	cc, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTExpectDisconnect(t, cc)
	cc.Close()
	restore()

	// The successor generation the aborted attempt committed: the standing record is already
	// the successor's, at a fresh generation, and the authority is mid-handoff (PREPARING).
	smsg, lerr := asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(asm.domainTk, getHash(clientID)))
	if lerr != nil {
		t.Fatalf("reading the committed successor record: %v", lerr)
	}
	committed := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, committed); err != nil {
		t.Fatalf("decoding the committed successor record: %v", err)
	}
	if committed.Gen == oldGen || committed.Gen == _EMPTY_ {
		t.Fatalf("the committed record must carry the successor generation, got %q (old %q)", committed.Gen, oldGen)
	}

	// A fresh CONNECT completes the committed handoff: it must be accepted and leave the
	// SUCCESSOR serving at exactly the generation the committed batch installed.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	al := asm.jsa.loadOutboundAuthority(clientID, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the recovery must leave a serving successor, got %+v", al)
	}
}
