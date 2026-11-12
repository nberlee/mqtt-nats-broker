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
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// The issue #107 clean-start HANDOFF over a LIVE serving authority.
//
// A clean start that lands on a client whose outbound authority is SERVING no longer destroys
// the record and then acquires ownership — that premature destruction left the authority
// serving with no record to hand off from, and refused every such CONNECT (Codex consult 6).
// The accepted batch now displaces the predecessor record IN PLACE through a discarding
// handoff: the successor serves at a fresh generation, the predecessor generation is closed
// with Discard and its cleanup enrolled, all in one finalization.

// testCleanStartEnv connects a PERSISTENT session and returns the account, the client id,
// its recovery epoch, and the outbound authority's generation as it stands after the connect.
func testCleanStartEnv(t *testing.T, clientID string) (*Server, *mqttAccountSessionManager, string, string, func()) {
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

// TestCleanStartHandoffOverLiveAuthoritySucceeds is the production regression: a clean
// start over a resident, persistent session with a live serving authority must be ACCEPTED,
// report no session present, and install a successor serving authority at a NEW generation.
func TestCleanStartHandoffOverLiveAuthoritySucceeds(t *testing.T) {
	const clientID = "f105-cs-handoff-ok"
	s, asm, epoch, oldGen, done := testCleanStartEnv(t, clientID)
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

// TestCleanStartHandoffClosesAndReplacesThePredecessor asserts the durable outcome of the
// handoff: the predecessor generation is CLOSED with Discard, and the standing session record
// is the successor's — a fresh generation, parentless (a clean start inherits no lineage).
func TestCleanStartHandoffClosesAndReplacesThePredecessor(t *testing.T) {
	const clientID = "f105-cs-handoff-closure"
	s, asm, epoch, oldGen, done := testCleanStartEnv(t, clientID)
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

// TestCleanStartHandoffAbortBeforeCommitPreservesThePredecessor arms the metadata-commit
// seam to fail BEFORE the batch is sent. Nothing of the successor commits: the predecessor
// session record stands unchanged, and a retry with the seam disarmed completes the handoff.
func TestCleanStartHandoffAbortBeforeCommitPreservesThePredecessor(t *testing.T) {
	const clientID = "f105-cs-handoff-abort"
	s, asm, epoch, oldGen, done := testCleanStartEnv(t, clientID)
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
		// Fail EVERY acceptance batch issuing while armed — the clean-start handoff's and each
		// re-send of it within the CONNECT (issue #133: an acceptance whose batch never left is
		// re-sent within the per-CONNECT allowance) — so nothing of the successor batch is sent.
		// The seam is disarmed before the retry below.
		if obs.Phase == mqttAcceptStoreIssuing && armed.Load() {
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
	armed.Store(false)
	restore()
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	al := asm.jsa.loadOutboundAuthority(clientID, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing || al.rec.Gen == oldGen {
		t.Fatalf("the retry must install a fresh serving successor, got %+v", al)
	}
}

// TestCleanStartHandoffCleansUpThePredecessorConsumer asserts the cleanup half: the plain
// QoS 1 consumer the predecessor held is removed once the successor is serving. The affine
// consumer, when present, is deferred to recovery; a plain source/group consumer is this
// broker's to delete directly.
func TestCleanStartHandoffCleansUpThePredecessorConsumer(t *testing.T) {
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

// TestCleanStartHandoffLeavesAnUnreadablePredecessorRegisterToTheSweep is the clean-start
// predecessor cleanup's row of docs/CONFORMANCE.md's teardown-reap table: the cleanup's register
// reap REFUSES an in-bound register it cannot read, the refusal does not itself refuse the
// CONNECT, and nothing adopts a retry, since the metadata batch displaced the predecessor record
// and the finalization closed its generation discarding. What converges the register is the
// orphan sweep, licensed by that discarding closure alone.
func TestCleanStartHandoffLeavesAnUnreadablePredecessorRegisterToTheSweep(t *testing.T) {
	const clientID = "cs-handoff-unreadable-register"
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "foo", opts: 1}})
	asm, sess := testResidentSession(t, s, clientID)
	sess.mu.Lock()
	hash, predGen := sess.idHash, sess.gen
	sess.mu.Unlock()

	// One QoS 1 delivery the predecessor never acknowledges: its register stands.
	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTReadConnAckV5(t, pr)
	testMQTTPublishV5(t, pc, 1, 1, "foo", []byte("unacked"))
	testMQTTReadPubAckV5(t, pr, 1)
	_, pi := testMQTTReadPublishV5(t, r, "foo", []byte("unacked"))
	regSubj := mqttInflightSubject(hash, predGen, pi)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if smsg, err := asm.jsa.loadLastMsgFor(mqttSessStreamName, regSubj); err != nil || smsg == nil {
			return fmt.Errorf("the predecessor's register on %q does not stand: %v", regSubj, err)
		}
		return nil
	})

	// The session persists its close with a SERVING authority: the state the handoff is for. The
	// disconnect is settled before anything is planted: the client is gone and its persisted record
	// carries the disconnect at the predecessor generation.
	c.Close()
	testWaitSessionRecordSettled(t, s, asm, clientID)
	if ps := testLoadPersistedSession(t, asm, clientID); ps.Gen != predGen {
		t.Fatalf("fixture: the settled record carries generation %q, not the predecessor's %q", ps.Gen, predGen)
	}
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		al := asm.jsa.loadOutboundAuthority(clientID, asm.sessStreamEpoch)
		if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing || al.rec.Gen != predGen {
			return fmt.Errorf("the predecessor authority does not serve %q: %+v", predGen, al)
		}
		return nil
	})

	// A body no broker in this tree writes, at the register's own subject.
	corrupt := []byte(`{"pi":`)
	ack, err := asm.jsa.storeMsg(regSubj, 0, corrupt)
	if err != nil {
		t.Fatalf("planting the unreadable register: %v", err)
	}
	corruptSeq := ack.Sequence
	if smsg, lerr := asm.jsa.loadLastMsgFor(mqttSessStreamName, regSubj); lerr != nil || smsg == nil ||
		smsg.Sequence != corruptSeq || string(smsg.Data) != string(corrupt) {
		t.Fatalf("fixture: the planted body is not what stands on %q: %+v, %v", regSubj, smsg, lerr)
	}
	if _, derr := mqttDecodeInflightRecord(corrupt); derr == nil {
		t.Fatal("fixture: the planted body decodes, so nothing here is unreadable")
	}

	var reached atomic.Bool
	testSetTeardownDeletesHook(t, func(tg *mqttTeardownTarget) {
		if tg.idHash == hash && tg.gen == predGen {
			reached.Store(true)
		}
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if sessp, rc, _ := testMQTTReadConnAckV5(t, r2); rc != mqttConnAckRCConnectionAccepted || sessp {
		t.Fatalf("the clean start was answered rc=%#x sessionPresent=%v", rc, sessp)
	}

	if !reached.Load() {
		t.Fatal("the clean-start predecessor cleanup never reached the predecessor generation's reap")
	}
	if cl, _, st := asm.jsa.loadGenClosure(clientID, asm.sessStreamEpoch, predGen); st != mqttOutboundValid || cl == nil {
		t.Fatalf("the predecessor generation must be closed discarding, got %+v (status %v)", cl, st)
	}
	// The reap refused rather than deleted: the unreadable register still stands at its sequence.
	if smsg, lerr := asm.jsa.loadLastMsgFor(mqttSessStreamName, regSubj); lerr != nil || smsg == nil || smsg.Sequence != corruptSeq {
		t.Fatalf("the unreadable register on %q did not survive the cleanup: %+v, %v", regSubj, smsg, lerr)
	}

	// Only now may the sweep qualify it: the background sweep runs at the default grace.
	testOrphanSetGraces(t, asm, 0, 0)
	testOrphanSettle(asm, s)
	if smsg, lerr := asm.jsa.loadLastMsgFor(mqttSessStreamName, regSubj); lerr == nil && smsg != nil {
		t.Fatalf("the orphan sweep retained the unreadable register on %q at sequence %d", regSubj, smsg.Sequence)
	}
}
