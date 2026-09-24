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
	"fmt"
	"sync"
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
// wraps the intent's write and the slot's load so a test can shape the intent's outcome.
//
// The obligation is ADMITTED, as production admits it, so the intent is the ATOMIC
// exchange-and-slot advance — which the register store seam never sees. The intent hook sits on
// the advance's own seam (testAckAdvance), and fired proves each scenario reached it.
type testIntentEnv struct {
	*testInflightEnv
	pi        uint16
	slotSubj  string
	intentCAS func(write func() (uint64, error)) (uint64, error)
	fired     atomic.Int64
	failLoads atomic.Bool
	loadsOK   atomic.Int32
}

func testNewIntentEnv(t *testing.T, clientID string) *testIntentEnv {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	e.subscribePubRel(t) // $MQTT_out is interest-retained: a store with no consumer keeps nothing
	dur := e.sess.idHash + "_" + nuid.Next()
	pi := e.trackAdmitted(t, dur, 31, 2)
	ie := &testIntentEnv{testInflightEnv: e, pi: pi, slotSubj: mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)}
	e.asm.mu.Lock()
	e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.To != mqttExchangePubRecObserved || req.Register == nil ||
			mqttInflightSubject(e.sess.idHash, req.Gen, req.Register.PI) != ie.slotSubj || ie.intentCAS == nil {
			return write()
		}
		ie.fired.Add(1)
		var out mqttOpOutcome
		seq, err := ie.intentCAS(func() (uint64, error) {
			o, werr := write()
			out = o
			return o.Seq, werr
		})
		out.Seq = seq
		return out, err
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
	if ie.fired.Load() == 0 {
		t.Fatal("the fixture never reached the intent's write, so no refusal was injected")
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
			if ie.fired.Load() == 0 {
				t.Fatal("the fixture never reached the intent's write, so nothing was injected")
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

// (e) AN ACKNOWLEDGED ADVANCE IS THE PROOF. The batch lands and its success is returned, and only
// then do slot reads fail: the PUBREC completes — the PUBREL is stored, nothing rolls back —
// because no read stands between an acknowledged intent and the store. Requiring a readback here
// would turn a durable intent into a closed connection. Mutation M6: return the resolving
// re-read's outcome instead of Confirmed after an acknowledged batch.
func TestMQTTPubRecAcknowledgedAdvanceProceedsDespiteAFailedReadback(t *testing.T) {
	t.Parallel()
	ie := testNewIntentEnv(t, "intent-acknowledged")
	ie.intentCAS = func(write func() (uint64, error)) (uint64, error) {
		seq, err := write()
		if err == nil {
			ie.failLoads.Store(true) // every slot read from here on fails
		}
		return seq, err
	}
	err := ie.c.mqttProcessPublishReceived(ie.pi, true, 0)
	ie.failLoads.Store(false)
	if err != nil {
		t.Fatalf("an acknowledged intent must let the PUBREC complete, got %v", err)
	}
	if ie.fired.Load() == 0 {
		t.Fatal("the fixture never reached the intent's write")
	}
	rel, published := ie.maps()
	if rel == nil || published {
		t.Fatalf("the exchange must be past PUBREC: placeholder=%+v publish-tracked=%v", rel, published)
	}
	if !ie.pubRelStored(t) {
		t.Fatal("the PUBREL record must have been stored")
	}
}

// testPubRecWaits arms the backoff seam: every wait the advance would sleep is recorded instead,
// and run (when set) coordinates a real mutation between two attempts.
func testPubRecWaits(ie *testIntentEnv, run func(n int)) *[]time.Duration {
	var mu sync.Mutex
	waits := &[]time.Duration{}
	ie.asm.mu.Lock()
	ie.asm.testPubRecAdvanceWait = func(d time.Duration) {
		mu.Lock()
		*waits = append(*waits, d)
		n := len(*waits)
		mu.Unlock()
		if run != nil {
			run(n)
		}
	}
	ie.asm.mu.Unlock()
	return waits
}

// testCheckPubRecWaits requires every recorded wait inside the advance's backoff bounds.
func testCheckPubRecWaits(t *testing.T, waits []time.Duration, want int) {
	t.Helper()
	if len(waits) != want {
		t.Fatalf("the advance waited %d times, want %d", len(waits), want)
	}
	for i, d := range waits {
		if d < mqttPubRecAdvanceWaitMin || d >= mqttPubRecAdvanceWaitMin+mqttPubRecAdvanceWaitJitter {
			t.Fatalf("wait %d was %v, outside [%v, %v)", i, d, mqttPubRecAdvanceWaitMin,
				mqttPubRecAdvanceWaitMin+mqttPubRecAdvanceWaitJitter)
		}
	}
}

// A SUCCESSFUL PUBREC OUTLASTS A CONTENTION BURST (consult 22). Its advance is refused N times —
// the send's own token drain, a resuming CONNECT's successive authority writes — and lands on
// attempt N+1, spaced by one bounded wait per refusal and none on a first-attempt success. N = 3
// is the case the old three-attempt bound lost, rolling the acknowledgement back and closing a
// healthy connection. Mutation: restore the three-attempt bound, and N = 3 and N = 7 fail.
func TestMQTTPubRecAdvanceReattemptsAfterRefusals(t *testing.T) {
	t.Parallel()
	for _, refusals := range []int{0, 1, 3, 7} {
		refusals := refusals
		t.Run(fmt.Sprintf("refused %d", refusals), func(t *testing.T) {
			t.Parallel()
			ie := testNewIntentEnv(t, fmt.Sprintf("pubrec-refused-%d", refusals))
			waits := testPubRecWaits(ie, nil)
			ie.intentCAS = func(write func() (uint64, error)) (uint64, error) {
				if int(ie.fired.Load()) <= refusals {
					return 0, errors.New("test: the advance was refused")
				}
				return write()
			}
			if err := ie.c.mqttProcessPublishReceived(ie.pi, true, 0); err != nil {
				t.Fatalf("the PUBREC must complete after %d refusals, got %v", refusals, err)
			}
			if got := int(ie.fired.Load()); got != refusals+1 {
				t.Fatalf("the advance was attempted %d times, want %d", got, refusals+1)
			}
			testCheckPubRecWaits(t, *waits, refusals)
			if rel, published := ie.maps(); rel == nil || published {
				t.Fatalf("the exchange must be past PUBREC: placeholder=%+v publish-tracked=%v", rel, published)
			}
			if !ie.pubRelStored(t) {
				t.Fatal("the PUBREL record must have been stored")
			}
			if cur, _, _ := ie.asm.jsa.loadInflightRecord(ie.slotSubj); cur == nil || !mqttInflightPastPubRec(cur.State) {
				t.Fatalf("the slot must be past PUBREC: %+v", cur)
			}
		})
	}
}

// THE BUDGET IS EXACT. Eight refusals: eight attempts, seven waits — no ninth attempt and no wait
// after the last — and the resolving re-read, finding the slot still PUBLISH, rolls the PUBREC
// back exactly as before. Mutation: loop on `<=`, and a ninth attempt is made.
func TestMQTTPubRecAdvanceExhaustsAfterEightAttempts(t *testing.T) {
	t.Parallel()
	ie := testNewIntentEnv(t, "pubrec-exhausted")
	waits := testPubRecWaits(ie, nil)
	ie.intentCAS = func(func() (uint64, error)) (uint64, error) {
		return 0, errors.New("test: the advance was refused")
	}
	if err := ie.c.mqttProcessPublishReceived(ie.pi, true, 0); !errors.Is(err, errMQTTPubRelIntentNotRecorded) {
		t.Fatalf("an exhausted, unrecorded intent must abort the PUBREC, got %v", err)
	}
	if got := int(ie.fired.Load()); got != mqttPubRecAdvanceAttempts {
		t.Fatalf("the advance was attempted %d times, want exactly %d", got, mqttPubRecAdvanceAttempts)
	}
	testCheckPubRecWaits(t, *waits, mqttPubRecAdvanceAttempts-1)
	if rel, published := ie.maps(); rel != nil || !published {
		t.Fatalf("the PUBREC must roll back: placeholder=%+v publish-tracked=%v", rel, published)
	}
}

// EXHAUSTION CONCLUDES NOTHING BY ITSELF. The LAST attempt commits and its response is lost: the
// resolving re-read finds the intent durable and the PUBREC completes. With the re-read also
// unavailable the outcome is Unresolved — nothing stored, nothing rolled back.
func TestMQTTPubRecAdvanceLastAttemptIsResolvedNotAssumed(t *testing.T) {
	t.Parallel()
	for _, readable := range []bool{true, false} {
		readable := readable
		name := "landed, response lost"
		if !readable {
			name = "landed, response lost, re-read unavailable"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ie := testNewIntentEnv(t, "pubrec-last-attempt")
			testPubRecWaits(ie, nil)
			ie.intentCAS = func(write func() (uint64, error)) (uint64, error) {
				if int(ie.fired.Load()) < mqttPubRecAdvanceAttempts {
					return 0, errors.New("test: the advance was refused")
				}
				if _, err := write(); err != nil {
					t.Errorf("the last attempt's real commit failed: %v", err)
				}
				if !readable {
					ie.failLoads.Store(true)
				}
				return 0, errors.New("test: the last attempt landed but its response was lost")
			}
			err := ie.c.mqttProcessPublishReceived(ie.pi, true, 0)
			ie.failLoads.Store(false)
			rel, published := ie.maps()
			if readable {
				if err != nil {
					t.Fatalf("a landed last attempt must let the PUBREC complete, got %v", err)
				}
				if rel == nil || published || !ie.pubRelStored(t) {
					t.Fatalf("the PUBREC must complete: placeholder=%+v publish-tracked=%v", rel, published)
				}
				return
			}
			if !errors.Is(err, errMQTTPubRelIntentNotRecorded) {
				t.Fatalf("an unresolved last attempt must not complete the PUBREC, got %v", err)
			}
			if rel == nil || published {
				t.Fatalf("an unresolved intent must KEEP the placeholder and not roll back: placeholder=%+v "+
					"publish-tracked=%v", rel, published)
			}
		})
	}
}

// EVERY ATTEMPT READS FRESH. A real write lands between two attempts — during the wait — once on
// the slot (a register CAS conflict) and once on the authority (the pre-submit conflict a
// resuming CONNECT causes); the next attempt must reload and land on what now stands, not resend
// the stale read. Mutation: hoist the slot read above the loop, and the slot case never lands.
func TestMQTTPubRecAdvanceReadsFreshAfterEachWait(t *testing.T) {
	t.Parallel()
	for _, what := range []string{"slot", "authority"} {
		what := what
		t.Run(what, func(t *testing.T) {
			t.Parallel()
			ie := testNewIntentEnv(t, "pubrec-fresh-"+what)
			var otherSubj string
			var otherSeq uint64
			var other *mqttInflightRecord
			if what == "authority" {
				// A second obligation whose own acknowledgement moves the authority.
				dur := ie.sess.idHash + "_" + nuid.Next()
				opi := ie.trackAdmitted(t, dur, 32, 1)
				otherSubj = mqttInflightSubject(ie.sess.idHash, ie.sess.gen, opi)
				cur, smsg, err := ie.asm.jsa.loadInflightRecord(otherSubj)
				if err != nil || cur == nil || smsg == nil {
					t.Fatalf("the second obligation's register: %+v %v", cur, err)
				}
				other, otherSeq = cur, smsg.Sequence
			}
			ie.intentCAS = func(write func() (uint64, error)) (uint64, error) {
				if ie.fired.Load() == 1 {
					// The conflicting write lands between this attempt's read and its commit.
					switch what {
					case "slot":
						cur, smsg, err := ie.asm.jsa.loadInflightRecord(ie.slotSubj)
						if err != nil || cur == nil || smsg == nil {
							t.Errorf("reading the slot to move it: %v", err)
							break
						}
						same := *cur
						if _, werr := ie.asm.jsa.storeInflightCAS(ie.slotSubj, smsg.Sequence, &same); werr != nil {
							t.Errorf("moving the slot's sequence: %v", werr)
						}
					case "authority":
						// The operation reads the authority INSIDE write(), so the conflicting
						// authority write must land between its composition and its commit: the
						// transition seam, armed for this one attempt and guarded against the
						// moving operation's own transition.
						var moving atomic.Bool
						hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
							if moving.Load() {
								return send()
							}
							moving.Store(true)
							_, aerr := ie.sess.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
								ID: ie.sess.id, Epoch: ie.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(),
								Op: "op-ack-advance", Gen: ie.sess.gen, Nonce: other.Lin, To: mqttExchangeTerminal,
								Acq: other.Acq, Register: other, RegisterSeq: otherSeq,
							})
							if aerr != nil {
								t.Errorf("moving the authority: %v", aerr)
							}
							return send()
						}
						ie.sess.jsa.testTransitionCommit.Store(&hook)
						defer ie.sess.jsa.testTransitionCommit.Store(nil)
					}
					if _, err := write(); err == nil {
						t.Errorf("the first attempt committed over a write that moved its %s", what)
					}
					return 0, errors.New("test: the first attempt lost its CAS")
				}
				return write()
			}
			waits := testPubRecWaits(ie, nil)
			if err := ie.c.mqttProcessPublishReceived(ie.pi, true, 0); err != nil {
				t.Fatalf("the PUBREC must complete on a fresh read after a %s conflict, got %v", what, err)
			}
			if got := ie.fired.Load(); got != 2 {
				t.Fatalf("the advance was attempted %d times, want 2", got)
			}
			testCheckPubRecWaits(t, *waits, 1)
			if !ie.pubRelStored(t) {
				t.Fatal("the PUBREL record must have been stored")
			}
		})
	}
}

// The backoff's endpoints, without real time.
func TestMQTTPubRecAdvanceBackoffBounds(t *testing.T) {
	t.Parallel()
	if got := mqttPubRecAdvanceBackoff(0); got != mqttPubRecAdvanceWaitMin {
		t.Fatalf("backoff(0) = %v, want %v", got, mqttPubRecAdvanceWaitMin)
	}
	top := mqttPubRecAdvanceWaitMin + mqttPubRecAdvanceWaitJitter
	if got := mqttPubRecAdvanceBackoff(0.999999); got >= top || got < top-time.Millisecond {
		t.Fatalf("backoff(~1) = %v, want just under %v", got, top)
	}
}
