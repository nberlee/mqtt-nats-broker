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

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// THE CONNECT OWNERSHIP STEPS RECONCILE A LOST OR REFUSED ATTEMPT BEFORE REPEATING IT (consult 21).
//
// The successful-PUBREC advance is a standing-authority writer, so a resuming CONNECT's ownership
// steps now contend with the old connection's acknowledgements. A step that loses is resolved
// from the handle its operation froze — landed, superseded, or settled never-landed by taking its
// outcome subject — and only a positively never-landed attempt is repeated, within one allowance
// per CONNECT and the ownership deadline.

// testArmTransitionSeam installs the transition seam for one test and removes it after.
func testArmTransitionSeam(t *testing.T, jsa *mqttJSA,
	hook func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error)) {
	t.Helper()
	jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { jsa.testTransitionCommit.Store(nil) })
}

// testReconcileSession is the minimal session the ownership helper and executor steps act
// through: its client identity and the account's JetStream assistant.
func testReconcileSession(e *testF105Env) *mqttSession {
	return &mqttSession{id: e.id, jsa: &e.asm.jsa, asm: e.asm}
}

// testCountingAllow is an allowance of n re-attempts, counting how many it admitted.
func testCountingAllow(n int) (func() error, *int) {
	used := 0
	return func() error {
		if used >= n {
			return errors.New("test: the re-attempt allowance is spent")
		}
		used++
		return nil
	}, &used
}

// C2: A LOST RESPONSE IS RESOLVED, NEVER RE-ISSUED — for every authority step the executor runs.
// The seam performs the real commit and reports it lost; the helper must conclude from the
// retained handle that the transition happened, return its transaction, and never issue a second
// one (which each operation would refuse, being no idempotent retry). Mutation M-C2: treat every
// handle error as never-landed, and the re-attempt either appears or is refused.
func TestF105OwnershipStepLostResponseIsResolved(t *testing.T) {
	type step struct {
		name  string
		kind  mqttTransitionKind
		setup func(t *testing.T, e *testF105Env) func(txn string) (mqttOpOutcome, error)
	}
	steps := []step{
		{"claim", mqttTransitionReserveCreation, func(t *testing.T, e *testF105Env) func(string) (mqttOpOutcome, error) {
			p := testF105CreationProp(t, "c2-claim")
			return func(txn string) (mqttOpOutcome, error) {
				return e.asm.jsa.opClaimClient(e.id, e.epoch, txn, p.Gen, p.Tenure, p.ID)
			}
		}},
		{"finalize claim", mqttTransitionFinalizeCreation, func(t *testing.T, e *testF105Env) func(string) (mqttOpOutcome, error) {
			p := testF105CreationProp(t, "c2-fin-claim")
			testF105StandCreation(t, e, p)
			f := testF105FreezeProposal(t, e, p, testF105RecordSeq(t, e))
			if _, err := e.asm.jsa.commitMetadataAttempt(f); err != nil {
				t.Fatalf("committing the creation's metadata: %v", err)
			}
			return func(txn string) (mqttOpOutcome, error) {
				return e.asm.jsa.opFinalizeClaim(e.id, e.epoch, txn, p.ID)
			}
		}},
		{"prepare", mqttTransitionPrepareIntent, func(t *testing.T, e *testF105Env) func(string) (mqttOpOutcome, error) {
			i := testF105Intent(t, e, "c2-prep")
			return func(txn string) (mqttOpOutcome, error) {
				return e.asm.jsa.opPrepareHandoff(e.id, e.epoch, txn, i)
			}
		}},
		{"finalize handoff", mqttTransitionFinalize, func(t *testing.T, e *testF105Env) func(string) (mqttOpOutcome, error) {
			i := testF105Intent(t, e, "c2-fin")
			testF105Prepare(t, e, i)
			if _, err := e.asm.jsa.commitMetadataAttempt(testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))); err != nil {
				t.Fatalf("committing the handoff's metadata: %v", err)
			}
			return func(txn string) (mqttOpOutcome, error) {
				return e.asm.jsa.opFinalizeHandoff(e.id, e.epoch, txn, i.ID)
			}
		}},
		{"cancel handoff", mqttTransitionCancelIntent, func(t *testing.T, e *testF105Env) func(string) (mqttOpOutcome, error) {
			i := testF105Intent(t, e, "c2-cancel")
			testF105Prepare(t, e, i)
			if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataAborted {
				t.Fatalf("settling the preparation returned %v, want ABORTED", v)
			}
			return func(txn string) (mqttOpOutcome, error) {
				return e.asm.jsa.opCancelHandoff(e.id, e.epoch, txn, i.ID)
			}
		}},
		{"close abandoned reservation", _EMPTY_, func(t *testing.T, e *testF105Env) func(string) (mqttOpOutcome, error) {
			p := testF105CreationProp(t, "c2-close")
			testF105StandCreation(t, e, p)
			if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, p); v != mqttMetadataAborted {
				t.Fatalf("settling the reservation returned %v, want ABORTED", v)
			}
			al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
			if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityReserving {
				t.Fatalf("no standing reservation to close: %+v", al)
			}
			g, tn, ow := al.rec.Gen, al.rec.Tenure, al.rec.OwnerTxn
			return func(txn string) (mqttOpOutcome, error) {
				return e.asm.jsa.opCloseGeneration(e.id, e.epoch, txn, g, tn, ow)
			}
		}},
	}
	for _, st := range steps {
		st := st
		t.Run(st.name, func(t *testing.T) {
			e, done := testF105NewEnv(t, "c2-"+strings.ReplaceAll(st.name, " ", "-"))
			defer done()
			op := st.setup(t, e)
			var calls, lost atomic.Int32
			testArmTransitionSeam(t, &e.asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				if st.kind != _EMPTY_ && tr.Attempt.Kind != st.kind {
					return send()
				}
				calls.Add(1)
				seq, err := send()
				if err == nil && lost.CompareAndSwap(0, 1) {
					return 0, errors.New("test: the step committed but its response was lost")
				}
				return seq, err
			})
			allow, used := testCountingAllow(mqttOwnershipStepReattempts)
			txn, err := testReconcileSession(e).reconcileOwnershipStep(op, allow)
			if err != nil {
				t.Fatalf("a committed step whose response was lost must resolve as landed, got %v", err)
			}
			if lost.Load() != 1 {
				t.Fatal("the fixture never committed and lost the step, so nothing here is tested")
			}
			if n := calls.Load(); n != 1 {
				t.Fatalf("the step was issued %d times; a landed transition is never re-issued", n)
			}
			if *used != 0 {
				t.Fatalf("a landed step consumed %d re-attempts", *used)
			}
			if txn == _EMPTY_ {
				t.Fatal("the helper must return the landed attempt's transaction")
			}
			if o, _, status := e.asm.jsa.loadTransitionOutcome(e.id, e.epoch, txn); status != mqttOutboundValid ||
				o.Verdict != mqttOutcomeCommitted {
				t.Fatalf("the returned transaction %q is not the committed one: %+v (%v)", txn, o, status)
			}
		})
	}
}

// C4 (a, b): THE HELPER IS BOUNDED BY ITS ALLOWANCE, and makes exactly one attempt once the
// allowance or the deadline refuses. Every attempt is refused before any batch is sent, so each is
// settled never-landed and admits one re-attempt only through allow. Mutation M-C4a: stop
// consulting allow, and the attempt count exceeds the bound.
func TestF105OwnershipStepReattemptsAreBounded(t *testing.T) {
	for _, allowance := range []int{mqttOwnershipStepReattempts, 0} {
		allowance := allowance
		t.Run(fmt.Sprintf("allowance %d", allowance), func(t *testing.T) {
			e, done := testF105NewEnv(t, fmt.Sprintf("c4-bound-%d", allowance))
			defer done()
			p := testF105CreationProp(t, fmt.Sprintf("c4-%d", allowance))
			var calls atomic.Int32
			testArmTransitionSeam(t, &e.asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				if calls.Add(1) > 50 {
					return send() // a runaway loop under mutation ends instead of hanging the test
				}
				return 0, errors.New("test: refused before any batch was sent")
			})
			allow, used := testCountingAllow(allowance)
			_, err := testReconcileSession(e).reconcileOwnershipStep(func(txn string) (mqttOpOutcome, error) {
				return e.asm.jsa.opClaimClient(e.id, e.epoch, txn, p.Gen, p.Tenure, p.ID)
			}, allow)
			if err == nil {
				t.Fatal("a step refused on every attempt must fail once the allowance is spent")
			}
			if n := int(calls.Load()); n != allowance+1 {
				t.Fatalf("the step was attempted %d times, want %d (one plus the allowance)", n, allowance+1)
			}
			if *used != allowance {
				t.Fatalf("the allowance admitted %d re-attempts, want %d", *used, allowance)
			}
		})
	}
}

// C3: A PINNED FINALIZE NEVER RETARGETS — through the executor's own target capture. The
// executor pins P1 before its first attempt; inside that attempt a PEER finalizes P1, prepares P2
// and commits P2's metadata, so the attempt's real pre-submit check refuses. The re-attempt must
// still name P1 — and be refused, P1 no longer standing — leaving P2 PREPARING. Mutation M-C3:
// re-discover the standing proposal inside the retry, and P2 is finalized by an acceptance that
// never proposed it.
func TestF105OwnershipFinalizeTargetIsPinned(t *testing.T) {
	e, done := testF105NewEnv(t, "c3-pinned")
	defer done()
	p1 := testF105Intent(t, e, "c3-p1")
	testF105Prepare(t, e, p1)
	if _, err := e.asm.jsa.commitMetadataAttempt(testF105FreezeAttempt(t, e, p1, testF105RecordSeq(t, e))); err != nil {
		t.Fatalf("committing P1's metadata: %v", err)
	}
	var p2 *mqttHandoffIntent
	var peer, raced atomic.Bool
	testArmTransitionSeam(t, &e.asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if peer.Load() || tr.Attempt.Kind != mqttTransitionFinalize || !raced.CompareAndSwap(false, true) {
			return send()
		}
		peer.Store(true)
		if _, err := e.asm.jsa.opFinalizeHandoff(e.id, e.epoch, "txn-peer-fin", p1.ID); err != nil {
			t.Errorf("the peer's finalize of P1: %v", err)
		}
		p2 = testF105Intent(t, e, "c3-p2")
		testF105Prepare(t, e, p2)
		if _, err := e.asm.jsa.commitMetadataAttempt(testF105FreezeAttempt(t, e, p2, testF105RecordSeq(t, e))); err != nil {
			t.Errorf("committing P2's metadata: %v", err)
		}
		peer.Store(false)
		return send() // the executor's P1 attempt, over an authority that moved
	})
	allow, _ := testCountingAllow(mqttOwnershipStepReattempts)
	sess := testReconcileSession(e)
	exec := sess.newAcceptOwnershipExecutor(recordTx{}, e.id, e.epoch, p1.Gen, p1.Tenure, p1.ID,
		true, nil, _EMPTY_, 0, func() error { return errors.New("test: no metadata resend") }, allow)
	if _, err := exec(mqttOwnershipStep{Action: mqttOwnershipFinalizeHandoff}); err == nil {
		t.Fatal("finalizing a P1 that no longer stands must fail")
	}
	if !raced.Load() || p2 == nil {
		t.Fatal("the peer never raced the executor's finalize, so nothing here is tested")
	}
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityPreparing || al.rec.Intent == nil ||
		al.rec.Intent.ID != p2.ID {
		t.Fatalf("P2 must still stand PREPARING, un-finalized by this executor: %+v", al.rec)
	}
}

// C5: A PREPARATION THAT IS NOT THIS ACCEPTANCE'S IS NEVER GIVEN ITS METADATA. The executor's
// prepare commits and its response is lost; before the executor reads the authority back, its
// proposal is settled aborted and cancelled and ANOTHER acceptance's preparation stands. The step
// must refuse without even entering the fenced metadata commit. Mutation M-C5: drop the intent
// identity check, and the metadata commit is entered for the foreign proposal.
func TestF105OwnershipPrepareRefusesAForeignPreparation(t *testing.T) {
	e, done := testF105NewEnv(t, "c5-foreign")
	defer done()
	own := testF105Intent(t, e, "c5-own")
	var entered atomic.Int32
	e.asm.mu.Lock()
	e.asm.testOwnershipMetadataEntry = func() { entered.Add(1) }
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testOwnershipMetadataEntry = nil
		e.asm.mu.Unlock()
	})
	var foreign *mqttHandoffIntent
	var peer, raced atomic.Bool
	testArmTransitionSeam(t, &e.asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if peer.Load() || tr.Attempt.Kind != mqttTransitionPrepareIntent || !raced.CompareAndSwap(false, true) {
			return send()
		}
		seq, err := send()
		if err != nil {
			t.Errorf("the executor's prepare did not commit: %v", err)
			return seq, err
		}
		peer.Store(true)
		if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, testF105Prop(t, own)); v != mqttMetadataAborted {
			t.Errorf("settling the executor's proposal returned %v, want ABORTED", v)
		}
		if _, cerr := e.asm.jsa.opCancelHandoff(e.id, e.epoch, "txn-peer-cancel", own.ID); cerr != nil {
			t.Errorf("cancelling the executor's preparation: %v", cerr)
		}
		foreign = testF105Intent(t, e, "c5-foreign")
		testF105Prepare(t, e, foreign)
		peer.Store(false)
		return 0, errors.New("test: the prepare committed but its response was lost")
	})
	allow, _ := testCountingAllow(mqttOwnershipStepReattempts)
	sess := testReconcileSession(e)
	exec := sess.newAcceptOwnershipExecutor(recordTx{}, e.id, e.epoch, own.Gen, own.Tenure, own.ID,
		own.CleanReq, own.Retire, own.PrevSessWID, own.PrevSessSeq,
		func() error { return errors.New("test: no metadata resend") }, allow)
	if _, err := exec(mqttOwnershipStep{Action: mqttOwnershipPrepareHandoff}); err == nil {
		t.Fatal("a step whose standing preparation is another acceptance's must refuse")
	}
	if !raced.Load() || foreign == nil {
		t.Fatal("the foreign preparation never replaced the executor's, so nothing here is tested")
	}
	if n := entered.Load(); n != 0 {
		t.Fatalf("the fenced metadata commit was entered %d time(s) for a proposal that is not this acceptance's", n)
	}
}

// C1: THE REPORTED RACE, end to end. A client holds an admitted QoS 2 obligation and resumes; the
// resuming CONNECT's prepare is composed, and before it commits the obligation's real
// successful-PUBREC advance lands, moving the standing authority. The first prepare is refused by
// the real pre-submit check with its real plain error; the next is composed over the fresh
// authority under the SAME proposal; the CONNECT is accepted. Mutation M-C1: return the error on
// a never-landed attempt instead of re-attempting, and the CONNECT is refused.
func TestF105ResumeOwnershipSurvivesAPubRecAdvance(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "c1-resume-race"
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "foo", qos: 2}}, []byte{2})
	testMQTTFlush(t, sub, nil, subr)

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte("payload"))
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte("payload"))

	asm := testMQTTGetAccountSessionManager(t, s, cid)
	asm.mu.RLock()
	sess := asm.sessions[cid]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("no resident session for the subscriber")
	}
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	subj := mqttInflightSubject(getHash(cid), gen, pi)

	var (
		mu       sync.Mutex
		prepares []*mqttAuthorityTransition
		firstErr error
		inner    atomic.Bool
	)
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if inner.Load() || tr.Attempt.ID != cid || tr.Attempt.Kind != mqttTransitionPrepareIntent {
			return send()
		}
		mu.Lock()
		prepares = append(prepares, tr)
		n := len(prepares)
		mu.Unlock()
		if n != 1 {
			return send()
		}
		// THE RACE: the old connection's successful-PUBREC advance lands between this prepare's
		// read of the authority and its commit.
		inner.Store(true)
		reg, smsg, lerr := asm.jsa.loadInflightRecord(subj)
		if lerr != nil || reg == nil || smsg == nil {
			t.Errorf("reading the obligation's slot: %v", lerr)
		} else if _, aerr := asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
			ID: cid, Epoch: asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
			Gen: gen, Nonce: reg.Lin, To: mqttExchangePubRecObserved, Acq: reg.Acq,
			Register: reg, RegisterSeq: smsg.Sequence,
		}); aerr != nil {
			t.Errorf("landing the PUBREC advance: %v", aerr)
		}
		inner.Store(false)
		seq, err := send()
		mu.Lock()
		firstErr = err
		mu.Unlock()
		return seq, err
	})

	sub2, subr2 := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer sub2.Close()
	testMQTTCheckConnAck(t, subr2, mqttConnAckRCConnectionAccepted, true)

	mu.Lock()
	defer mu.Unlock()
	if len(prepares) < 2 {
		t.Fatalf("the refused prepare was not re-attempted: %d prepare(s)", len(prepares))
	}
	if firstErr == nil || !strings.Contains(firstErr.Error(), "is conditioned on the authority at sequence") {
		t.Fatalf("the first prepare must meet the real pre-submit refusal, got %v", firstErr)
	}
	a, b := prepares[0], prepares[1]
	if a.PrevSeq == b.PrevSeq {
		t.Fatalf("the re-attempt reused the stale authority sequence %d", a.PrevSeq)
	}
	if a.Auth.Intent == nil || b.Auth.Intent == nil || a.Auth.Intent.ID != b.Auth.Intent.ID {
		t.Fatal("the re-attempt must carry the SAME proposal as the refused attempt")
	}
}

// C4 (c): THE ALLOWANCE IS PER CONNECT, across steps and across a restoration re-drive. The first
// drive spends two re-attempts on its prepare and then commits it; its metadata commit is refused
// as a record-CAS loss, which re-drives the whole CONNECT from `restoreSession`; every prepare of
// the re-drive is refused, so it may spend only the two re-attempts left. Six prepares in all.
// Mutation M-C4b: initialise the allowance below `restoreSession`, and the re-drive gets a fresh
// four — eight prepares.
func TestF105OwnershipAllowanceSurvivesARedrive(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const cid = "c4-redrive"
	first, fr := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, fr, mqttConnAckRCConnectionAccepted, false)
	testMQTTFlush(t, first, nil, fr)

	asm := testMQTTGetAccountSessionManager(t, s, cid)
	var prepares atomic.Int32
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.ID != cid || tr.Attempt.Kind != mqttTransitionPrepareIntent {
			return send()
		}
		switch n := prepares.Add(1); {
		case n <= 2:
			return 0, errors.New("test: refused before any batch was sent")
		case n == 3:
			return send()
		case n > 20:
			return send() // a runaway under mutation ends instead of hanging the test
		default:
			return 0, errors.New("test: refused before any batch was sent")
		}
	})
	recSubj := mqttSessSubject(asm.domainTk, getHash(cid))
	var rejected atomic.Bool
	restore := asm.jsa.setAcceptStoreObserver(func(obs mqttAcceptStoreObs) error {
		if obs.Phase != mqttAcceptStoreIssuing || obs.RecSubj != recSubj || !rejected.CompareAndSwap(false, true) {
			return nil
		}
		return ApiErrors[JSStreamWrongLastSequenceErrF]
	})
	defer restore()

	c2, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: cid, cleanSess: false},
		o.MQTT.Host, o.MQTT.Port, 0)
	if c2 != nil {
		defer c2.Close()
	}
	if !rejected.Load() {
		t.Fatal("the first drive's metadata commit was never refused, so no re-drive happened")
	}
	if got, want := int(prepares.Load()), 3+1+2; got != want {
		t.Fatalf("the CONNECT issued %d prepares, want %d: the allowance of %d re-attempts is per "+
			"CONNECT and must not be replenished by the re-drive (err=%v)", got, want, mqttOwnershipStepReattempts, err)
	}
}
