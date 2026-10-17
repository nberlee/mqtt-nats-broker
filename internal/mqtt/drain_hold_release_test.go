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

package mqtt

import (
	"testing"

	"github.com/nats-io/nuid"
)

// A DRAINED SOURCE'S ACKNOWLEDGEMENT RELEASES THE IDENTIFIER HOLD (F-098). The PUBACK untrack
// holds an acknowledged exchange's identifier until its terminal cleanup is confirmed, and the
// terminal completion is what releases it. A DRAINED source never runs that completion — the
// drain's own finalize releases the register — so the hold must be released when the handler
// returns, or the identifier is reserved for the life of the session with nothing to free it.
// Mutation M111: drop the on-return release.
func TestMQTTDrainedAcknowledgementReleasesTheIdentifierHold(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "drain-hold")
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess, proto: mqttProtoLevel5}
	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	testPersistAcceptedRecord(t, e)

	dur := e.sess.idHash + "_" + nuid.Next()
	// A REAL delivery and its real reply: the terminal settlement's acknowledgement is confirmed,
	// and the server does not confirm one for a sequence it never delivered (consult 111).
	sseq, reply := e.publishSourceDelivered(t, dur)
	pi := e.trackAdmitted(t, dur, sseq, 1, reply)
	const sid = "drainhold/topic"
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	e.sess.mu.Lock()
	d := &mqttDrain{cc: cc, qos: 1, seqs: map[uint64]*drainSeq{sseq: {claimed: true, pi: pi}}}
	e.sess.drains = map[string]*mqttDrain{sid: d}
	e.sess.drainByDur = map[string]*mqttDrain{dur: d}
	e.sess.mu.Unlock()

	if err := e.c.mqttProcessPublishReceived(pi, false, 0); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}

	e.sess.mu.Lock()
	reserved := e.sess.sendReservedLocked(pi)
	var claimed bool
	if cur := e.sess.drainByDur[dur]; cur != nil && cur.seqs[sseq] != nil {
		claimed = cur.seqs[sseq].claimed
	}
	e.sess.mu.Unlock()
	if claimed {
		t.Fatal("the fixture must reach the drain's finalize: the claim is still standing")
	}
	if reserved {
		t.Fatalf("packet identifier %d is still held after its drained exchange completed", pi)
	}
}
