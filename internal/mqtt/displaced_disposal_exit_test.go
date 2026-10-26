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

// A displaced disposal ends only on two positive facts: its tenure no longer serves, and its
// source is settled. Each refusal below is a case where one of them is missing or merely
// inferred, and the one acceptance is where both are read.

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// displacedDisposalRig installs a disposal on the real-store abandonment fixture whose evidence
// names a tenure other than the serving one (or the serving one, when displaced is false).
func displacedDisposalRig(t *testing.T, clientID string, displaced bool) (*testAbandonFix, *mqttDisposal) {
	t.Helper()
	f := testAbandonAdmitted(t, clientID, 1)
	e := f.e
	ev := testAbandonEvidence(t, e, f.pi, f.subject)
	e.sess.mu.Lock()
	w := e.sess.installDisposalLocked(e.c, ev)
	if w != nil {
		w.busy = true // no step of its own runs while the test drives the exit
		if displaced {
			// What displacement looks like to an installed owner: the serving tenure is no
			// longer the one its evidence names.
			w.evidence.Tenure += "-displaced"
		}
	}
	e.sess.mu.Unlock()
	if w == nil {
		t.Fatal("fixture: no disposal installed")
	}
	return f, w
}

func disposalIndexed(sess *mqttSession, w *mqttDisposal) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.disposals[w.key] == w
}

// settleRealSource acknowledges deliveries of the fixture's SRC consumer until its ack floor
// passes seq, as the deciding owner (or a group member) would.
func settleRealSource(t *testing.T, e *testInflightEnv, seq uint64) {
	t.Helper()
	nc := e.s.nrc
	sub, err := nc.SubscribeSync("f105.real.source.delivery")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, testRealPubRelSrcDur)
		if err == nil && ci != nil && ci.AckFloor.Stream >= seq {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("could not settle the source: %+v %v", ci, err)
		}
		if err := nc.Publish(mqttStreamSubjectPrefix+"f092.source", []byte("settle")); err != nil {
			t.Fatal(err)
		}
		for {
			m, err := sub.NextMsg(100 * time.Millisecond)
			if err != nil {
				break
			}
			_ = m.AckSync(nats.AckWait(time.Second))
		}
	}
}

func TestDisplacedDisposalEndsOnlyOnBothFacts(t *testing.T) {
	t.Parallel()

	t.Run("serving tenure unchanged", func(t *testing.T) {
		t.Parallel()
		f, w := displacedDisposalRig(t, "displaced-exit-same", false)
		settleRealSource(t, f.e, w.key.seq)
		if w.endIfDisplacedAndSourceSettled() || !disposalIndexed(f.e.sess, w) {
			t.Fatal("an owner whose tenure still serves was ended")
		}
	})

	t.Run("displaced, source unsettled, then settled", func(t *testing.T) {
		t.Parallel()
		f, w := displacedDisposalRig(t, "displaced-exit-settle", true)
		e := f.e
		ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, testRealPubRelSrcDur)
		if err != nil || ci == nil || ci.AckFloor.Stream >= w.key.seq {
			t.Fatalf("fixture: the source must be unsettled: %+v %v", ci, err)
		}
		if w.endIfDisplacedAndSourceSettled() || !disposalIndexed(e.sess, w) {
			t.Fatal("the disposal ended on displacement alone, with its source still unsettled")
		}
		settleRealSource(t, e, w.key.seq)
		if !w.endIfDisplacedAndSourceSettled() || disposalIndexed(e.sess, w) {
			t.Fatal("the disposal did not end with its tenure displaced and its source settled")
		}
		// Only the denial owner went: the identifier stays reserved with its entry.
		e.sess.mu.Lock()
		_, entry := e.sess.pendingPublish[f.pi]
		e.sess.mu.Unlock()
		if !entry {
			t.Fatal("ending the obsolete denial owner released the identifier's pending entry")
		}
	})

	t.Run("recovery-staged", func(t *testing.T) {
		t.Parallel()
		f, w := displacedDisposalRig(t, "displaced-exit-recovery", true)
		settleRealSource(t, f.e, w.key.seq)
		f.e.sess.mu.Lock()
		w.recovery = &mqttDisposalRecoveryHold{}
		f.e.sess.mu.Unlock()
		if w.endIfDisplacedAndSourceSettled() || !disposalIndexed(f.e.sess, w) {
			t.Fatal("a recovery-staged owner was ended outside its synchronous resume")
		}
	})

	t.Run("no serving authority", func(t *testing.T) {
		t.Parallel()
		f, w := displacedDisposalRig(t, "displaced-exit-noauth", true)
		e := f.e
		settleRealSource(t, e, w.key.seq)
		al := e.asm.jsa.loadOutboundAuthority(e.sess.id, w.key.epoch)
		if al.status != mqttOutboundValid || al.seq == 0 {
			t.Fatalf("fixture: no authority to remove: %+v", al)
		}
		if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, al.seq, true); err != nil {
			t.Fatal(err)
		}
		if w.endIfDisplacedAndSourceSettled() || !disposalIndexed(e.sess, w) {
			t.Fatal("an owner was ended without a serving authority to show its tenure moved")
		}
	})

	t.Run("shared consumer absent but not retired", func(t *testing.T) {
		t.Parallel()
		f, w := displacedDisposalRig(t, "displaced-exit-shared", true)
		e := f.e
		// A well-formed group durable that no consumer and no group record stand behind.
		e.sess.mu.Lock()
		w.key.dur = mqttSharedConsumerDurablePrefix + "AbCdEfGhIjKlMnOp_1"
		e.sess.mu.Unlock()
		// The settlement predicate itself: rewriting the key unindexes the owner, so the exit's
		// removal would refuse for an unrelated reason and prove nothing here.
		if w.sourceSettled() {
			t.Fatal("an absent shared consumer with no retirement record was taken as settled")
		}
	})

	t.Run("ordinary consumer absent but not retired", func(t *testing.T) {
		t.Parallel()
		f, w := displacedDisposalRig(t, "displaced-exit-absent", true)
		e := f.e
		if _, err := e.asm.jsa.deleteConsumer(mqttStreamName, testRealPubRelSrcDur, false); err != nil {
			t.Fatal(err)
		}
		if w.endIfDisplacedAndSourceSettled() || !disposalIndexed(e.sess, w) {
			t.Fatal("an absent ordinary consumer whose resource is not retired was taken as settled")
		}
	})
}
