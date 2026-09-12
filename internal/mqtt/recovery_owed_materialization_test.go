// Copyright 2025 The mqtt-nats-bridge Authors
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

	"github.com/nats-io/nuid"
)

// RECOVERY ENROLS AN OWED RECORD (F-098 L1, holistic review round 1, finding 3; C13 order 4).
//
// The crash cut: the intent CAS landed, the PUBREL store did not, and the process died
// before — or with — the live path's own materialization timer. After the restart nothing is
// in memory and no PUBREL record exists for admission to act on. Recovery must enrol the slot
// itself; the pre-fix recovery installed a non-actionable reservation and nothing ever
// rebuilt the record, so the client waited for a PUBREL that never came.

// testSeedOwedSlot drives a real QoS 2 register to `pubrel_intended` under a PREDECESSOR's
// custody, with no PUBREL record stored, and forgets everything in memory — a restart.
func testSeedOwedSlot(t *testing.T, e *testInflightEnv, token string) (pi uint16, subject string) {
	t.Helper()
	// AN ACCEPTED RECORD, so custody can be CONTESTED rather than refused: recovery may take
	// a predecessor's slot only against a claim provably older than this session's own
	// acceptance, and with no acceptance there is nothing to order the two by. Established
	// the way testReadyForPubRelDelivery does it, minus that helper's in-memory slot store —
	// these gates run against real JetStream.
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	testConnIdentity(t, e, e.c)
	// The session must be BOUND to this connection before its record is saved, or the
	// acceptance proof has no connection to be current for.
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	testPersistAcceptedRecord(t, e)
	// TWICE, so the acceptance sequence leaves room below it: a first save on a fresh stream
	// lands at 1, and a predecessor's claim has to be STRICTLY older and still a real claim.
	testPersistAcceptedRecord(t, e)
	e.sess.mu.Lock()
	ourSeq := e.sess.seq
	e.sess.mu.Unlock()
	if ourSeq < 2 {
		t.Fatalf("the fixture needs an acceptance sequence above 1 to seed an older predecessor, got %d", ourSeq)
	}
	e.subscribePubRel(t)
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	pi = e.track(t, dur, sseq, 2)
	rec, msg := e.loadRegister(t, pi)
	if rec == nil || msg == nil {
		t.Fatal("the register must exist")
	}
	owed := *rec
	owed.State = mqttInflightPubRelIntended
	owed.Claim, owed.ClaimSeq = "a-predecessor-incarnations-claim", ourSeq-1
	if token != _EMPTY_ {
		owed.ActionToken, owed.ActionClaim, owed.ActionClaimSeq = token, owed.Claim, owed.ClaimSeq
	}
	subject = mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	if _, err := e.asm.jsa.storeInflightCAS(subject, msg.Sequence, &owed); err != nil {
		t.Fatalf("seeding the owed slot: %v", err)
	}
	e.forgetInMemory()
	return pi, subject
}

func testPubRelRecordStored(e *testInflightEnv) bool {
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttOutStreamName, e.sess.pubRelSubject)
	return err == nil && smsg != nil
}

// Mutation M102: drop recovery's enrolment of owed-record reservations.
func TestMQTTRecoveryMaterializesAnOwedPubRelRecord(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "recover-owed")
	pi, subject := testSeedOwedSlot(t, e, _EMPTY_)
	if testPubRelRecordStored(e) {
		t.Fatal("the fixture must start with NO PUBREL record, or it is the stored-record case")
	}

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	e.sess.mu.Lock()
	_, reserved := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if !reserved {
		t.Fatalf("recovery must reserve packet identifier %d for the owed exchange", pi)
	}

	// NO OTHER TRAFFIC: nothing is delivered and no client acts. Only recovery's own
	// enrolment can rebuild the record.
	deadline := time.Now().Add(15 * time.Second)
	for {
		cur, _, _ := e.asm.jsa.loadInflightRecord(subject)
		if testPubRelRecordStored(e) && cur != nil && cur.State == mqttInflightPubRelReady {
			if cur.Claim != e.sess.regClaim {
				t.Fatalf("materialization must bind under THIS incarnation's custody, got claim %q", cur.Claim)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the owed record was never materialized: stored=%v slot=%+v", testPubRelRecordStored(e), cur)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// An owed slot whose action is LIVE is not this pass's to act on. Recovery keeps the
// reservation and materializes nothing while the holder may still be acting; the enrolment
// retries rather than giving up, which the main case above proves converges once it can.
func TestMQTTRecoveryDoesNotMaterializeUnderALiveAction(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "recover-owed-held")
	pi, subject := testSeedOwedSlot(t, e, mqttNewInflightNonce())

	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	e.sess.mu.Lock()
	_, reserved := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if !reserved {
		t.Fatalf("a live-action owed slot must keep its reservation for packet identifier %d", pi)
	}
	// Longer than the materialization delay, so an enrolled attempt has certainly run.
	time.Sleep(mqttActionReconcileDelay + time.Second)
	if testPubRelRecordStored(e) {
		t.Fatal("a PUBREL record was materialized under a live action")
	}
	cur, _, _ := e.asm.jsa.loadInflightRecord(subject)
	if cur == nil || cur.State != mqttInflightPubRelIntended || cur.ActionToken == _EMPTY_ {
		t.Fatalf("the held slot must be untouched: %+v", cur)
	}
}
