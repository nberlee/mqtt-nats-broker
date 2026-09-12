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

// A FINALIZE THAT LOSES THE RECORD FENCE IS RETRIED (F-098). "Retried on redelivery" is not
// true of every finalize: one driven by the client's acknowledgement — or by the send
// settlement that acknowledgement parks — runs ONCE. Two drains settling on two delivery
// goroutines contend for the fence, and the loser used to withhold with no clock at all, so
// its drain was never retired (TestMQTTDrainLevelUpCoversBothConsumers caught it 2 runs in 5).
//
// This stages the loss deterministically: the fence is HELD while the finalize runs, on a
// claimed drained sequence with no register, which goes straight to the fence. While the fence
// is held the claim must stand — withholding is right — and once it is released the claim must
// be removed with no further event. Mutation M109: drop the retry from the fence arm.
func TestMQTTDrainFinalizeRetriesAfterLosingTheRecordFence(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "drain-fence")
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	testConnIdentity(t, e, e.c)
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	testPersistAcceptedRecord(t, e)

	dur := e.sess.idHash + "_" + nuid.Next()
	const sid, sseq = "fence/topic", uint64(7)
	cc := &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()}
	st := &drainSeq{claimed: true} // pi 0: no register, so nothing to release first
	e.sess.mu.Lock()
	d := &mqttDrain{cc: cc, qos: 1, seqs: map[uint64]*drainSeq{sseq: st}}
	e.sess.drains = map[string]*mqttDrain{sid: d}
	e.sess.drainByDur = map[string]*mqttDrain{dur: d}
	e.sess.mu.Unlock()

	claimed := func() bool {
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		cur := e.sess.drainByDur[dur]
		if cur == nil {
			return false
		}
		s := cur.seqs[sseq]
		return s != nil && s.claimed
	}

	held := e.sess.beginRecordTx()
	if !e.sess.finalizeDrainDelivery(e.c, dur, sseq, _EMPTY_) {
		t.Fatal("a drained sequence is the drain's to finalize")
	}
	time.Sleep(100 * time.Millisecond) // longer than the prompt retry delay: it has run, and lost again
	if !claimed() {
		t.Fatal("the claim was removed while the record fence was held by someone else")
	}
	if err := e.sess.endRecordTx(held); err != nil {
		t.Fatalf("releasing the fence: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for claimed() {
		if time.Now().After(deadline) {
			t.Fatal("the finalize that lost the fence was never retried: the claim stands with no " +
				"event left to remove it, so its drain is never retired")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
