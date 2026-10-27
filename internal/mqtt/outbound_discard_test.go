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
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

// F-105 7d: the two acts a discarded generation's retirement takes under its closure, at the write
// boundary (plan-7d T5, T14, T16).

// testDiscardPlantClosure plants generation gen's closure, discarding or not. Direct, because what
// is under test is the boundary's READ of the licence; the transitions that write one are tested
// where they live.
func testDiscardPlantClosure(t *testing.T, jsa *mqttJSA, id, epoch, gen string, discard bool) {
	t.Helper()
	body, err := json.Marshal(&mqttGenClosure{
		ID: id, Epoch: epoch, Gen: gen, Txn: "txn-planted-closure", Discard: discard,
		RootShard: mqttDirShardFor(getHash(id)), RootOrd: 1,
	})
	if err != nil {
		t.Fatalf("marshalling the closure: %v", err)
	}
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, serr := jsa.storeMsg(mqttGenClosureSubject(getHash(id), gen), hdr, msg); serr != nil {
		t.Fatalf("planting the closure: %v", serr)
	}
}

func testDiscardRefused(t *testing.T, err error, why string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the batch was ACCEPTED; want it refused by %q", why)
	}
	if !strings.Contains(err.Error(), why) {
		t.Fatalf("refused for another reason: %v (want %q)", err, why)
	}
}

// testDiscardHeldQoS2 admits one QoS 2 obligation holding a source and a PUBREL consumer.
func testDiscardHeldQoS2(t *testing.T, e *testF105Env) *mqttOutboundExchange {
	t.Helper()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	testF105PlantResource(t, e, "inc-rel", mqttResourcePubRelConsumer)
	nonce, _, _, _ := testF105AdmitQoS(t, e, "txn-adm", 7, 2, "inc-src", "inc-rel")
	x, _ := testF105LoadExchange(t, e, testF105Gen, nonce)
	testRequireHolds(t, &e.asm.jsa, x, true)
	return x
}

// The positive: under a committed discarding closure, every phase that has not released its holds
// is discarded, and the holds go in the same batch.
func TestF105DiscardEndsAnObligationOfADiscardedGeneration(t *testing.T) {
	for name, steps := range map[string][]mqttExchangePhase{
		"publish":                nil,
		"pubrec observed":        {mqttExchangePubRecObserved},
		"terminal pending drain": {mqttExchangePubRecObserved, mqttExchangeTerminalPendingDrain},
		// No "rejected pending drain" row: no batch records that phase (a refusal over a live
		// PUBLISH token waits for the drain and lands as Rejected beside its slot), so
		// there is no stored exchange of that phase for a discard to meet.
	} {
		t.Run(name, func(t *testing.T) {
			e, done := testF105NewEnv(t, "f105-discard-"+strings.ReplaceAll(name, " ", "-"))
			defer done()
			x := testDiscardHeldQoS2(t, e)
			for _, to := range steps {
				if _, err := testAdvanceLive(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, x.Nonce, to); err != nil {
					t.Fatalf("advancing to %d: %v", to, err)
				}
			}
			testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
			if _, err := e.asm.jsa.opDiscardExchange(e.id, e.epoch, "txn-discard", testF105Gen, x.Nonce); err != nil {
				t.Fatalf("the discard was refused: %v", err)
			}
			end, _ := testF105LoadExchange(t, e, testF105Gen, x.Nonce)
			if end.Phase != mqttExchangeDiscarded {
				t.Fatalf("the exchange stands at %d, want Discarded", end.Phase)
			}
			testRequireHolds(t, &e.asm.jsa, end, false)
		})
	}
}

// The licence and the confinement, each submitted DIRECTLY to the write boundary with an
// otherwise-valid batch, so a composer's own refusal cannot stand in for the boundary's.
func TestF105DiscardBoundaryRefusals(t *testing.T) {
	compose := func(t *testing.T, e *testF105Env, x *mqttOutboundExchange, releases bool) *mqttAuthorityTransition {
		t.Helper()
		cur, curSeq := testF105LoadExchange(t, e, testF105Gen, x.Nonce)
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		tr, err := mqttDiscardExchangeTransition(al.rec, al.seq, "txn-d", "op-d", cur, curSeq)
		if err != nil {
			t.Fatalf("composing the discard: %v", err)
		}
		if releases {
			tr = testF105WithHoldReleases(t, e, tr, cur, mqttExchangeDiscarded)
		}
		return tr
	}

	t.Run("no closure", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-discard-b-none")
		defer done()
		x := testDiscardHeldQoS2(t, e)
		_, err := e.asm.jsa.commitAuthorityTransition(compose(t, e, x, true))
		testDiscardRefused(t, err, "only a committed discarding closure licenses")
	})
	t.Run("a preserving closure", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-discard-b-preserve")
		defer done()
		x := testDiscardHeldQoS2(t, e)
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, false)
		_, err := e.asm.jsa.commitAuthorityTransition(compose(t, e, x, true))
		testDiscardRefused(t, err, "preserving its obligations")
	})
	t.Run("another generation's closure", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-discard-b-other")
		defer done()
		x := testDiscardHeldQoS2(t, e)
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen2, true)
		_, err := e.asm.jsa.commitAuthorityTransition(compose(t, e, x, true))
		testDiscardRefused(t, err, "only a committed discarding closure licenses")
	})
	t.Run("without its hold releases", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-discard-b-norelease")
		defer done()
		x := testDiscardHeldQoS2(t, e)
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
		_, err := e.asm.jsa.commitAuthorityTransition(compose(t, e, x, false))
		testDiscardRefused(t, err, "without releasing its hold")
	})
	t.Run("an ended obligation rewritten", func(t *testing.T) {
		// The composer refuses this itself (T16), so the batch is built by hand: the standing
		// record is an ending, the proposed one Discarded.
		e, done := testF105NewEnv(t, "f105-discard-b-ended")
		defer done()
		x := testDiscardHeldQoS2(t, e)
		if _, err := testAdvanceLive(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, x.Nonce, mqttExchangeRejected); err != nil {
			t.Fatalf("ending the obligation: %v", err)
		}
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
		cur, curSeq := testF105LoadExchange(t, e, testF105Gen, x.Nonce)
		pending := *cur
		pending.Phase = mqttExchangePublish
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		tr, err := mqttDiscardExchangeTransition(al.rec, al.seq, "txn-d", "op-d", &pending, curSeq)
		if err != nil {
			t.Fatalf("composing over the pretended phase: %v", err)
		}
		_, err = e.asm.jsa.commitAuthorityTransition(tr)
		testDiscardRefused(t, err, "an ending is preserved, never rewritten")
	})
	t.Run("Discarded written by another kind", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-discard-b-kind")
		defer done()
		x := testDiscardHeldQoS2(t, e)
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
		tr := compose(t, e, x, true)
		tr.Attempt.Kind = mqttTransitionAdvanceExchange
		_, err := e.asm.jsa.commitAuthorityTransition(tr)
		testDiscardRefused(t, err, "only a discard-exchange transition writes Discarded")
	})
	t.Run("a discard carrying a register", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-discard-b-register")
		defer done()
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		nonce, subj, reg, regSeq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
		x, _ := testF105LoadExchange(t, e, testF105Gen, nonce)
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
		tr := compose(t, e, x, true)
		b, _ := json.Marshal(reg)
		tr.Extra = append(tr.Extra, mqttTransitionMember{subj: subj, expect: regSeq, body: b})
		_, err := e.asm.jsa.commitAuthorityTransition(tr)
		testDiscardRefused(t, err, "neither the exchange it discards nor one of that exchange's hold releases")
	})
}

// T16: the composer preserves an ending rather than composing over it, and nothing is written.
func TestF105DiscardPreservesAnEnding(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-ended")
	defer done()
	x := testDiscardHeldQoS2(t, e)
	if _, err := testAdvanceLive(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, x.Nonce, mqttExchangeRejected); err != nil {
		t.Fatalf("ending the obligation: %v", err)
	}
	testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
	_, before := testF105LoadExchange(t, e, testF105Gen, x.Nonce)
	_, err := e.asm.jsa.opDiscardExchange(e.id, e.epoch, "txn-discard", testF105Gen, x.Nonce)
	if !errors.Is(err, errMQTTExchangeAlreadyReleased) {
		t.Fatalf("discarding an ended obligation returned %v, want errMQTTExchangeAlreadyReleased", err)
	}
	end, after := testF105LoadExchange(t, e, testF105Gen, x.Nonce)
	if end.Phase != mqttExchangeRejected || after != before {
		t.Fatalf("the ending was touched: phase %d, sequence %d -> %d", end.Phase, before, after)
	}
}

// T5, the abandonment: a DeliveryAbandoned obligation is discarded with its immutable abandonment
// fact preserved, and its holds — which the abandonment kept — are released.
func TestF105DiscardPreservesTheAbandonmentFact(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		f := testAbandonAdmitted(t, "f105-discard-abandoned-q"+string('0'+qos), qos)
		f.deny()
		f.requireAbandoned(t)
		x := f.exchange(t)
		testRequireHolds(t, &f.e.asm.jsa, x, true)
		jsa, id, epoch := &f.e.asm.jsa, f.e.sess.id, f.e.asm.sessStreamEpoch
		testDiscardPlantClosure(t, jsa, id, epoch, f.gen, true)
		if _, err := jsa.opDiscardExchange(id, epoch, "txn-discard", f.gen, f.lin); err != nil {
			t.Fatalf("QoS %d: discarding the abandoned obligation was refused: %v", qos, err)
		}
		end := f.exchange(t)
		if end.Phase != mqttExchangeDiscarded || end.Abandoned == nil || *end.Abandoned != *x.Abandoned {
			t.Fatalf("QoS %d: the discard lost or changed the abandonment: phase %d, %+v", qos, end.Phase, end.Abandoned)
		}
		testRequireHolds(t, jsa, end, false)
	}
}

// T5, the closure-licensed disposal.
func TestF105GenerationDiscardedDisposal(t *testing.T) {
	setup := func(t *testing.T, name string) *testF105Env {
		e, done := testF105NewEnv(t, "f105-discard-disp-"+name)
		t.Cleanup(done)
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		return e
	}
	direct := func(t *testing.T, e *testF105Env, d *mqttResourceDisposal) error {
		t.Helper()
		cur, curSeq := testF105LoadResource(t, e, "inc-src")
		next := *cur
		next.Disposal = d
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		_, err := e.asm.jsa.commitResourceStep(al.rec, al.seq, d.Txn, "op-direct", &next, curSeq)
		return err
	}

	t.Run("licensed by a discarding closure, with no tenure", func(t *testing.T) {
		e := setup(t, "ok")
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
		if _, err := e.asm.jsa.opDisposeDiscarded(e.id, e.epoch, "txn-disp", testF105Gen, "inc-src"); err != nil {
			t.Fatalf("the discard's disposal was refused: %v", err)
		}
		r, _ := testF105LoadResource(t, e, "inc-src")
		if r.Disposal == nil || r.Disposal.Reason != mqttDisposalGenerationDiscarded ||
			r.Disposal.Tenure != _EMPTY_ || r.Disposal.Gen != testF105Gen {
			t.Fatalf("the standing decision is not the discard's: %+v", r.Disposal)
		}
		// IDEMPOTENT, and a standing decision is never overwritten.
		if _, err := e.asm.jsa.opDisposeDiscarded(e.id, e.epoch, "txn-disp-2", testF105Gen, "inc-src"); err != nil {
			t.Fatalf("a repeated disposal was refused: %v", err)
		}
		if again, _ := testF105LoadResource(t, e, "inc-src"); again.Disposal.Txn != "txn-disp" {
			t.Fatalf("the standing decision was replaced: %+v", again.Disposal)
		}
	})
	t.Run("without a closure", func(t *testing.T) {
		e := setup(t, "none")
		err := direct(t, e, &mqttResourceDisposal{Txn: "txn-x", Reason: mqttDisposalGenerationDiscarded, Gen: testF105Gen})
		testDiscardRefused(t, err, "only a committed discarding closure licenses")
	})
	t.Run("under a preserving closure", func(t *testing.T) {
		e := setup(t, "preserve")
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, false)
		err := direct(t, e, &mqttResourceDisposal{Txn: "txn-x", Reason: mqttDisposalGenerationDiscarded, Gen: testF105Gen})
		testDiscardRefused(t, err, "preserving its obligations")
	})
	t.Run("naming another generation", func(t *testing.T) {
		e := setup(t, "othergen")
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen2, true)
		err := direct(t, e, &mqttResourceDisposal{Txn: "txn-x", Reason: mqttDisposalGenerationDiscarded, Gen: testF105Gen2})
		testDiscardRefused(t, err, "a discard decides only its own generation's consumers")
	})
	t.Run("carrying a tenure", func(t *testing.T) {
		e := setup(t, "tenure")
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		err := direct(t, e, &mqttResourceDisposal{Txn: "txn-x", Reason: mqttDisposalGenerationDiscarded,
			Gen: testF105Gen, Tenure: al.rec.Tenure})
		testDiscardRefused(t, err, "a discard is decided by the closure")
	})
	t.Run("another reason without a tenure", func(t *testing.T) {
		e := setup(t, "notenure")
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
		d := &mqttResourceDisposal{Txn: "txn-x", Reason: mqttDisposalUnsubscribed, Gen: testF105Gen}
		// The licence refuses it at the boundary (no serving entitlement has an empty tenure), and
		// the closed schema refuses it on its own, which is the reason-specific rule under test.
		testDiscardRefused(t, direct(t, e, d), "which is not the serving one")
		testDiscardRefused(t, mqttValidateResourceDisposal(d), "malformed tenure")
	})
}

// T14 at the predicate surface: Discarded releases holds and ends the duty, and is NOT an ending —
// no identifier, no acquisition, no captured acknowledgement binding follows it.
func TestF105DiscardedIsNotAnIdentifierEnding(t *testing.T) {
	if !mqttValidExchangePhase(mqttExchangeDiscarded) || !mqttExchangeReleasesHolds(mqttExchangeDiscarded) ||
		!mqttExchangeDeliveryDutyEnded(mqttExchangeDiscarded) {
		t.Fatal("Discarded must be a valid phase that releases holds and ends the duty")
	}
	if mqttExchangePhaseEnded(mqttExchangeDiscarded) {
		t.Fatal("Discarded is an identifier ending: a discard would manufacture drain and discharge evidence")
	}
	x := &mqttOutboundExchange{Phase: mqttExchangeDiscarded}
	if d, _ := mqttCustodyDisposition(x); d == mqttCustodyEnded {
		t.Fatal("custody reads a discarded obligation as ENDED, releasing its identifier")
	}
	if c, err := mqttExchangeCharge(x, true); err != nil || c != mqttChargeHeldPendingDrain {
		t.Fatalf("a discarded obligation with output outstanding is charged %v (%v), want held pending drain", c, err)
	}
	x.Abandoned = &mqttExchangeAbandonment{}
	if (&mqttAllocationAckBinding{role: mqttAckRoleHandBack}).endedBy(nil, x) {
		t.Fatal("a discard ended a captured abandoned-ACK binding")
	}
	v, _ := mqttCompletionAllowed(mqttCompletionInput{Op: mqttOpAcquireMore,
		Auth: &mqttOutboundAuthority{State: mqttAuthorityServing}, Exchange: &mqttOutboundExchange{Phase: mqttExchangeDiscarded, QoS: 2}})
	if v != mqttCompletionRefuse {
		t.Fatalf("a discarded obligation may acquire more output: verdict %v", v)
	}
}

// T4c: a client answer and the generation's discard race on ONE exchange, both orders, as REAL
// store contention: the winner commits in the loser's presend window — after every one of the
// loser's boundary reads passed — so what refuses the loser is the store's own sequence CAS on its
// batch, never a boundary re-read (testTransitionPresend).
//
// ACK wins: the answer stands, Terminal, its hold released by its own ending, and a re-attempted
// discard finds the ending preserved. DISCARD wins: no answer is recorded over the discarded
// obligation.
func TestF105AckAndDiscardRaceOnOneExchange(t *testing.T) {
	setup := func(t *testing.T, name string) (*testF105Env, string, *mqttInflightRecord, uint64) {
		e, done := testF105NewEnv(t, "f105-discard-race-"+name)
		t.Cleanup(done)
		e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		nonce, _, reg, regSeq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
		testDiscardPlantClosure(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, true)
		return e, nonce, reg, regSeq
	}
	ack := func(e *testF105Env, nonce string, reg *mqttInflightRecord, regSeq uint64) error {
		_, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
			ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
			Gen: testF105Gen, Nonce: nonce, To: mqttExchangeTerminal, Acq: reg.Acq,
			Register: reg, RegisterSeq: regSeq,
		})
		return err
	}
	// race lands win() in the presend window of the transition whose txn is loser.
	race := func(t *testing.T, e *testF105Env, loser string, win func() error) *error {
		var winErr error
		var fired atomic.Bool
		hook := func(tr *mqttAuthorityTransition) {
			if tr.Attempt != nil && tr.Attempt.Txn == loser && fired.CompareAndSwap(false, true) {
				winErr = win()
			}
		}
		e.asm.jsa.testTransitionPresend.Store(&hook)
		t.Cleanup(func() { e.asm.jsa.testTransitionPresend.Store(nil) })
		return &winErr
	}
	storeCAS := func(t *testing.T, err error, who string) {
		t.Helper()
		if err == nil {
			t.Fatalf("the %s committed over the winner", who)
		}
		if ae := jsAPIError(err); ae == nil || !isQoS2CASRejection(ae) {
			t.Fatalf("the %s was refused by something other than the store's sequence CAS: %v", who, err)
		}
	}

	t.Run("the acknowledgement wins", func(t *testing.T) {
		e, nonce, reg, regSeq := setup(t, "ack")
		winErr := race(t, e, "txn-discard", func() error { return ack(e, nonce, reg, regSeq) })
		_, derr := e.asm.jsa.opDiscardExchange(e.id, e.epoch, "txn-discard", testF105Gen, nonce)
		if *winErr != nil {
			t.Fatalf("fixture: the acknowledgement did not commit in the discard's window: %v", *winErr)
		}
		storeCAS(t, derr, "discard")
		x, _ := testF105LoadExchange(t, e, testF105Gen, nonce)
		if x.Phase != mqttExchangeTerminal {
			t.Fatalf("the exchange stands at %d, want the answer's Terminal", x.Phase)
		}
		// The answer's ending leaves the holds release-owed (outbound_release_holds.go).
		testRequireHolds(t, &e.asm.jsa, x, true)
		e.asm.jsa.testTransitionPresend.Store(nil)
		if _, err := e.asm.jsa.opDiscardExchange(e.id, e.epoch, "txn-discard-2", testF105Gen, nonce); !errors.Is(err, errMQTTExchangeAlreadyReleased) {
			t.Fatalf("the re-attempted discard returned %v, want the ending preserved", err)
		}
	})

	t.Run("the discard wins", func(t *testing.T) {
		e, nonce, reg, regSeq := setup(t, "discard")
		winErr := race(t, e, "txn-ack", func() error {
			_, err := e.asm.jsa.opDiscardExchange(e.id, e.epoch, "txn-discard", testF105Gen, nonce)
			return err
		})
		aerr := ack(e, nonce, reg, regSeq)
		if *winErr != nil {
			t.Fatalf("fixture: the discard did not commit in the acknowledgement's window: %v", *winErr)
		}
		storeCAS(t, aerr, "acknowledgement")
		x, _ := testF105LoadExchange(t, e, testF105Gen, nonce)
		if x.Phase != mqttExchangeDiscarded {
			t.Fatalf("the exchange stands at %d, want Discarded", x.Phase)
		}
		testRequireHolds(t, &e.asm.jsa, x, false)
		// And a fresh acknowledgement, composed against the discarded record, is refused outright.
		e.asm.jsa.testTransitionPresend.Store(nil)
		if err := ack(e, nonce, reg, regSeq); err == nil {
			t.Fatal("a PUBACK was recorded over a discarded obligation")
		}
	})
}
