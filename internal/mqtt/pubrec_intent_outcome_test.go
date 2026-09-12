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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// THE PUBREC INTENT OUTCOME IS CLASSIFIED (F-098, holistic review round 1, finding 4; C13
// order 3). A slot's EXISTENCE proves nothing about whether the intent CAS landed, and the
// pre-fix rollback gate read it as proof: a definitely-failed intent kept its empty
// placeholder, rolled nothing back and enrolled nothing.

// testIntentEnv drives a real QoS 2 delivery to the point where its PUBREC can arrive, and
// wraps the slot's store and load so a test can shape the intent CAS's outcome.
type testIntentEnv struct {
	*testInflightEnv
	pi        uint16
	slotSubj  string
	intentCAS func(write func() (uint64, error)) (uint64, error)
	failLoads atomic.Bool
	loadsOK   atomic.Int32
}

func testNewIntentEnv(t *testing.T, clientID string) *testIntentEnv {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	e.subscribePubRel(t) // $MQTT_out is interest-retained: a store with no consumer keeps nothing
	dur := e.sess.idHash + "_" + nuid.Next()
	pi := e.track(t, dur, 31, 2)
	ie := &testIntentEnv{testInflightEnv: e, pi: pi, slotSubj: mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)}
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		write := func() (uint64, error) { return e.asm.jsa.storeInflightCAS(subject, expect, r) }
		if subject == ie.slotSubj && r.State == mqttInflightPubRelIntended && ie.intentCAS != nil {
			return ie.intentCAS(write)
		}
		return write()
	}
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if subject == ie.slotSubj && ie.failLoads.Load() {
			return nil, nil, errors.New("test: the resolving read is unavailable")
		}
		if subject == ie.slotSubj {
			ie.loadsOK.Add(1)
		}
		return e.asm.jsa.loadInflightRecord(subject)
	}
	e.asm.mu.Unlock()
	e.c.mqtt = &mqtt{sess: e.sess, proto: mqttProtoLevel5}
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	t.Cleanup(func() {
		e.sess.mu.Lock()
		e.sess.c = nil
		e.sess.mu.Unlock()
	})
	return ie
}

func (ie *testIntentEnv) maps() (pubrel *mqttPending, published bool) {
	ie.sess.mu.Lock()
	defer ie.sess.mu.Unlock()
	_, published = ie.sess.pendingPublish[ie.pi]
	return ie.sess.pendingPubRel[ie.pi], published
}

func (ie *testIntentEnv) pubRelStored(t *testing.T) bool {
	t.Helper()
	smsg, err := ie.asm.jsa.loadLastMsgFor(mqttOutStreamName, ie.sess.pubRelSubject)
	return err == nil && smsg != nil
}

// (a) DEFINITELY NOT RECORDED: the intent write is refused without landing. Nothing may be
// stored under a slot that still reads PUBLISH, and the PUBREC rolls back — the placeholder
// goes and the PUBLISH tracking returns. Mutation M98: key the rollback on slot existence.
func TestMQTTPubRecDefinitelyUnrecordedIntentRollsBack(t *testing.T) {
	t.Parallel()
	ie := testNewIntentEnv(t, "intent-definite")
	ie.intentCAS = func(func() (uint64, error)) (uint64, error) {
		return 0, errors.New("test: the intent CAS was rejected")
	}
	if err := ie.c.mqttProcessPublishReceived(ie.pi, true, 0); !errors.Is(err, errMQTTPubRelIntentNotRecorded) {
		t.Fatalf("a definitely unrecorded intent must abort the PUBREC, got %v", err)
	}
	rel, published := ie.maps()
	if rel != nil || !published {
		t.Fatalf("the PUBREC must roll back: placeholder=%+v publish-tracked=%v", rel, published)
	}
	if ie.pubRelStored(t) {
		t.Fatal("a PUBREL was stored under a slot that still reads PUBLISH")
	}
	if cur, _, _ := ie.asm.jsa.loadInflightRecord(ie.slotSubj); cur == nil || cur.State != mqttInflightPublish {
		t.Fatalf("the slot must still read PUBLISH: %+v", cur)
	}
}

// (b) LANDED, RESPONSE LOST: the reread proves the intent durable, so the store proceeds and
// nothing rolls back. Mutation M99: never return Confirmed from the resolution.
func TestMQTTPubRecLandedIntentWithALostResponseProceeds(t *testing.T) {
	t.Parallel()
	ie := testNewIntentEnv(t, "intent-landed")
	var lost atomic.Bool
	ie.intentCAS = func(write func() (uint64, error)) (uint64, error) {
		seq, err := write()
		if err == nil && lost.CompareAndSwap(false, true) {
			return 0, errors.New("test: the intent landed but its response was lost")
		}
		return seq, err
	}
	if err := ie.c.mqttProcessPublishReceived(ie.pi, true, 0); err != nil {
		t.Fatalf("a landed intent must let the PUBREC complete, got %v", err)
	}
	if !lost.Load() {
		t.Fatal("the fixture never lost the intent response")
	}
	rel, published := ie.maps()
	if rel == nil || published {
		t.Fatalf("the exchange must be past PUBREC: placeholder=%+v publish-tracked=%v", rel, published)
	}
	if !ie.pubRelStored(t) {
		t.Fatal("the PUBREL record must have been stored")
	}
}

// (c) UNRESOLVED: the intent landed and the resolving read failed too. Neither continuation
// is safe, so neither is taken — no store, no rollback, the placeholder kept — and the
// resolver, once reads recover, proves the intent and L1 rebuilds the record. Mutation M100:
// let an unresolved intent take the rollback.
func TestMQTTPubRecUnresolvedIntentIsResolvedLater(t *testing.T) {
	t.Parallel()
	ie := testNewIntentEnv(t, "intent-unresolved")
	var lost atomic.Bool
	ie.intentCAS = func(write func() (uint64, error)) (uint64, error) {
		seq, err := write()
		if err == nil && lost.CompareAndSwap(false, true) {
			ie.failLoads.Store(true)
			return 0, errors.New("test: the intent landed but its response was lost")
		}
		return seq, err
	}
	if err := ie.c.mqttProcessPublishReceived(ie.pi, true, 0); !errors.Is(err, errMQTTPubRelIntentNotRecorded) {
		t.Fatalf("an unresolved intent must not complete the PUBREC, got %v", err)
	}
	rel, published := ie.maps()
	if rel == nil || published {
		t.Fatalf("an unresolved intent must KEEP the placeholder and not roll back: placeholder=%+v "+
			"publish-tracked=%v", rel, published)
	}
	if ie.pubRelStored(t) {
		t.Fatal("nothing may be stored while the intent is unresolved")
	}

	ie.failLoads.Store(false)
	deadline := time.Now().Add(15 * time.Second)
	for !ie.pubRelStored(t) {
		if time.Now().After(deadline) {
			t.Fatal("the resolver never proved the landed intent, so L1 never rebuilt the record")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, published := ie.maps(); published {
		t.Fatal("a resolved-landed intent must never be rolled back to PUBLISH")
	}
}

// (d) The resolver's rollback is guarded exactly as the live one: a placeholder a PUBREL
// delivery FILLED in the meantime is never undone. The positive control proves the same
// resolver does roll back an unfilled one, so "kept" below is not a resolver that never ran.
// Mutation M101: drop the resolver's empty-placeholder guard.
func TestMQTTPubRecIntentResolverNeverUndoesAFilledEntry(t *testing.T) {
	t.Parallel()
	for _, filled := range []bool{false, true} {
		filled := filled
		name := "an unfilled placeholder rolls back"
		if filled {
			name = "a filled entry is preserved"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ie := testNewIntentEnv(t, "intent-filled")
			ie.intentCAS = func(func() (uint64, error)) (uint64, error) {
				// NOT landed, and the reread is unavailable: unresolved, and when reads
				// recover the resolver finds the slot still in PUBLISH.
				ie.failLoads.Store(true)
				return 0, errors.New("test: the intent CAS outcome is unknown")
			}
			if err := ie.c.mqttProcessPublishReceived(ie.pi, true, 0); !errors.Is(err, errMQTTPubRelIntentNotRecorded) {
				t.Fatalf("an unresolved intent must not complete the PUBREC, got %v", err)
			}
			if filled {
				ie.sess.mu.Lock()
				ie.sess.pendingPubRel[ie.pi].jsAckSubject = "$JS.ACK.filled.by.a.delivery"
				ie.sess.mu.Unlock()
			}
			before := ie.loadsOK.Load()
			ie.failLoads.Store(false)
			deadline := time.Now().Add(10 * time.Second)
			for ie.loadsOK.Load() == before {
				if time.Now().After(deadline) {
					t.Fatal("the resolver never reread the slot")
				}
				time.Sleep(20 * time.Millisecond)
			}
			deadline = time.Now().Add(5 * time.Second)
			for {
				rel, published := ie.maps()
				if filled {
					if rel == nil || rel.jsAckSubject == _EMPTY_ || published {
						t.Fatalf("the resolver undid a filled PUBREL entry: placeholder=%+v publish-tracked=%v",
							rel, published)
					}
					if time.Now().After(deadline) {
						return
					}
				} else if rel == nil && published {
					return // rolled back: the resolver ran and chose the rollback
				} else if time.Now().After(deadline) {
					t.Fatalf("the resolver never rolled back an unfilled placeholder: placeholder=%+v "+
						"publish-tracked=%v", rel, published)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}
