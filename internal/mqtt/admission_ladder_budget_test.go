//go:build mqttport_fulltests

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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The delivery admission ladder is a FOREGROUND authority operation announced for its whole span,
// so only background attempts admitted before the announcement can still commit inside it, at most
// one per counted writer. With three attempts, three such commits abandoned a valid delivery for
// its AckWait re-offer. Its budget is now one more than the writers, and an attempt that reached
// the commit is spaced from the next by the shared contention wait. These tests force the losses
// at the commit seam instead of waiting for the interleaving.

// testLadderCountWaits counts the requested contention waits instead of sleeping them.
func testLadderCountWaits(t *testing.T, rig *testAcquireRig) *atomic.Int32 {
	t.Helper()
	var waits atomic.Int32
	rig.asm.mu.Lock()
	rig.asm.testAuthorityContentionWait = func(time.Duration) { waits.Add(1) }
	rig.asm.mu.Unlock()
	t.Cleanup(func() {
		rig.asm.mu.Lock()
		rig.asm.testAuthorityContentionWait = nil
		rig.asm.mu.Unlock()
	})
	return &waits
}

// testLadderLoseAtCommit refuses the first lose admission commits of the rig's client with the
// production CAS rejection, and counts every admission commit.
func testLadderLoseAtCommit(t *testing.T, rig *testAcquireRig, lose int) *atomic.Int32 {
	t.Helper()
	var attempts atomic.Int32
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt == nil || at.Attempt.ID != rig.id || at.Attempt.Op != "op-admit-publish" {
			return send()
		}
		if int(attempts.Add(1)) <= lose {
			return 0, ApiErrors[JSStreamWrongLastSequenceErrF]
		}
		return send()
	}
	rig.asm.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { rig.asm.jsa.testTransitionCommit.Store(nil) })
	return &attempts
}

// TestMQTTAdmissionLadderOutlastsTheBackgroundWriters: the admission survives one lost attempt per
// counted background writer and transmits on the first offer, each re-attempt preceded by one
// contention wait; one loss more than that exhausts the first offer, and the source's prompt re-offer
// transmits.
func TestMQTTAdmissionLadderOutlastsTheBackgroundWriters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lose  int
		inBud bool
	}{
		{name: "one loss per background writer", lose: mqttBackgroundAuthorityWriters, inBud: true},
		{name: "one loss more", lose: mqttBackgroundAuthorityWriters + 1, inBud: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := testAcquireRigUp(t, "ladder-bud", 2*time.Second)
			attempts := testLadderLoseAtCommit(t, rig, tc.lose)
			waits := testLadderCountWaits(t, rig)

			start := time.Now()
			testMQTTPubV5Props(t, rig.cp, rig.rp, 1, false, 1, rig.topic, []byte("live"), nil)
			if tc.inBud {
				testMQTTReadPubV5Props(t, rig.c, rig.r, rig.topic, []byte("live"))
				if el := time.Since(start); el >= rig.o.MQTT.AckWait {
					t.Fatalf("the delivery took %v, which is the redelivery's (AckWait %v), not the first offer's",
						el, rig.o.MQTT.AckWait)
				}
				if n := attempts.Load(); int(n) != tc.lose+1 {
					t.Fatalf("the admission made %d attempts, want %d (%d lost, then one landed)", n, tc.lose+1, tc.lose)
				}
				if w := waits.Load(); int(w) != tc.lose {
					t.Fatalf("%d contention waits before %d re-attempts", w, tc.lose)
				}
				return
			}
			// EXHAUSTED, the source is deferred and NAKed when the admission's custody reservation is
			// released, so the re-offer follows at once rather than at AckWait. What separates the
			// budget is the count: the first offer spends exactly its budget, every attempt lost and
			// each re-attempt spaced, and the re-offer's first attempt lands unspaced. A larger budget
			// would have landed the extra attempt inside the first offer after one more wait; a
			// smaller one spends offers, and their waits, until the losses run out.
			testMQTTReadPubV5Props(t, rig.c, rig.r, rig.topic, []byte("live"))
			if n := attempts.Load(); int(n) != tc.lose+1 {
				t.Fatalf("%d admission attempts across the offers, want %d (%d lost, then one landed)", n, tc.lose+1, tc.lose)
			}
			if w := waits.Load(); int(w) != mqttForegroundAcquireAttempts-1 {
				t.Fatalf("%d contention waits, want %d: one before each re-attempt of a first offer that spent its budget of %d",
					w, mqttForegroundAcquireAttempts-1, mqttForegroundAcquireAttempts)
			}
		})
	}
}

// TestMQTTAdmissionLadderSpacesOnlyCommittedAttempts: an attempt refused before its operation
// reached the commit carries no handle and is re-attempted without a wait; the same number of
// losses AT the commit is spaced, one wait each. Both arms still transmit on the first offer.
func TestMQTTAdmissionLadderSpacesOnlyCommittedAttempts(t *testing.T) {
	const refused = mqttForegroundAcquireAttempts - 1
	errRefusedBeforeCommit := errors.New("refused before the commit")
	for _, tc := range []struct {
		name      string
		atCommit  bool
		wantWaits int
	}{
		{name: "refused before the commit", atCommit: false, wantWaits: 0},
		{name: "lost at the commit", atCommit: true, wantWaits: refused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := testAcquireRigUp(t, "ladder-space", 2*time.Second)
			waits := testLadderCountWaits(t, rig)
			var writes atomic.Int32
			if tc.atCommit {
				testLadderLoseAtCommit(t, rig, refused)
			} else {
				rig.asm.mu.Lock()
				rig.asm.testAdmitWrite = func(sess *mqttSession, _ bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
					if sess.id != rig.id || int(writes.Add(1)) > refused {
						return write()
					}
					return mqttOpOutcome{}, errRefusedBeforeCommit
				}
				rig.asm.mu.Unlock()
				t.Cleanup(func() {
					rig.asm.mu.Lock()
					rig.asm.testAdmitWrite = nil
					rig.asm.mu.Unlock()
				})
			}

			start := time.Now()
			testMQTTPubV5Props(t, rig.cp, rig.rp, 1, false, 1, rig.topic, []byte("live"), nil)
			testMQTTReadPubV5Props(t, rig.c, rig.r, rig.topic, []byte("live"))
			if el := time.Since(start); el >= rig.o.MQTT.AckWait {
				t.Fatalf("the delivery took %v, which is the redelivery's (AckWait %v), not the first offer's",
					el, rig.o.MQTT.AckWait)
			}
			if !tc.atCommit && int(writes.Load()) != refused+1 {
				t.Fatalf("fixture: %d admission writes, want %d refused before the commit and then one", writes.Load(), refused)
			}
			if w := waits.Load(); int(w) != tc.wantWaits {
				t.Fatalf("%d contention waits after %d failed attempts %s, want %d", w, refused, tc.name, tc.wantWaits)
			}
		})
	}
}

// TestMQTTFailedReplacementCreatesAtTheIncomingQoS: a same-delivery replacement preserves the
// OCCUPANT's QoS on the record it writes, and when that attempt fails and the reread finds the
// subject empty, the ladder's next write is a CREATE of a fresh exchange whose dependencies were
// assembled for the INCOMING QoS. The create must carry that QoS, not the failed replacement's.
//
// Reached the way TestF105FailedRegisterOnlyAttemptRetriesOnAFreshLineage reaches its create: a
// possibly-attempted QoS 2 occupant, a QoS 1 re-offer of the same delivery on its identifier, and
// the occupant's register removed under the register-only attempt so it is refused and the subject
// is empty. Every create composed afterwards is read at the commit seam. The occupant's exchange
// still stands, so the fresh obligation is not admitted beside it; what is asserted is what the
// create would write. Mutation: the reset beside the lineage reset removed, and the creates carry
// QoS 2.
func TestMQTTFailedReplacementCreatesAtTheIncomingQoS(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "ladder-qos-reset", 2)
	e, sess := f.e, f.e.sess
	testW2PossiblyAttempted(t, f)
	occupant, occSeq := f.register(t)
	if occupant == nil || occupant.QoS != 2 {
		t.Fatalf("fixture: no QoS 2 occupant register: %+v", occupant)
	}
	nonce := testW2InstallSuccessorEntry(f)

	var mu sync.Mutex
	var refused, inRegisterOnly, subjectEmpty bool
	var refusal error
	var createQoS []byte
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(_ *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		mu.Lock()
		first := registerOnly && !refused
		mu.Unlock()
		if !first {
			return write()
		}
		if derr := e.asm.jsa.deleteMsg(mqttInflightStreamName, occSeq, true); derr != nil {
			return mqttOpOutcome{}, derr
		}
		mu.Lock()
		inRegisterOnly = true
		mu.Unlock()
		out, err := write()
		cur, _, lerr := e.asm.loadInflightRecord(f.subject)
		mu.Lock()
		inRegisterOnly, refused, refusal = false, true, err
		subjectEmpty = lerr == nil && cur == nil
		mu.Unlock()
		return out, err
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testAdmitWrite = nil
		e.asm.mu.Unlock()
	})
	testSetTransitionCommit(t, e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		mu.Lock()
		after := refused && !inRegisterOnly
		mu.Unlock()
		if !after || tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionAdmitExchange {
			return send()
		}
		for _, m := range tr.Extra {
			if m.subj != f.subject || m.expect != 0 {
				continue
			}
			var reg mqttInflightRecord
			if err := json.Unmarshal(m.body, &reg); err != nil {
				t.Errorf("decoding the create's register member: %v", err)
				break
			}
			mu.Lock()
			createQoS = append(createQoS, reg.QoS)
			mu.Unlock()
		}
		return send()
	})

	sess.admitInflightPublish(e.c, f.pi, 1, testRealPubRelSrcDur, f.sseq, nonce, _EMPTY_, _EMPTY_)

	mu.Lock()
	defer mu.Unlock()
	// ROUTE WITNESS: the register-only attempt was reached and refused, leaving the subject empty.
	if !refused || refusal == nil || !subjectEmpty {
		t.Fatalf("fixture: register-only attempt reached=%v refused with %v, subject empty after it=%v; want all",
			refused, refusal, subjectEmpty)
	}
	if len(createQoS) == 0 {
		t.Fatalf("no create reached the commit after the refused replacement (%v), so the reset was never exercised", refusal)
	}
	for _, q := range createQoS {
		if q != 1 {
			t.Fatalf("a create after the failed QoS 2 replacement writes register QoS %d, want the incoming QoS 1 (creates: %v)",
				q, createQoS)
		}
	}
}
