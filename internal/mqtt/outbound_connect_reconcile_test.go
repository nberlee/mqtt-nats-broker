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

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// C1: THE REPORTED RACE, end to end. A client holds an admitted QoS 2 obligation and resumes; the
// resuming CONNECT's prepare is composed, and before it commits the obligation's real
// successful-PUBREC advance lands. The advance is authority-free (mqttAttemptAuthorityFree), so it
// no longer moves the standing authority: the race is closed by construction, and the prepare
// lands on its FIRST attempt over the authority sequence it was composed against. Mutation
// M-L1a (the advance writes the authority again): the first prepare is refused by the pre-submit
// check and re-attempted. The re-attempt on a never-landed prepare (M-C1) is witnessed by the C4
// allowance tests, whose refusals are staged at the seam.
func TestResumeOwnershipSurvivesAPubRecAdvance(t *testing.T) {
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
	// THE SLOT MUST BE QUIESCENT before the race is staged: the old connection's send section
	// drains its PUBLISH output token after the frame reaches the client, so the PUBLISH arriving
	// here does not mean the slot has stopped moving. An advance composed against a slot still
	// being drained is refused for that, not for the race this test stages.
	quiet := time.Now().Add(5 * time.Second)
	for {
		reg, _, lerr := asm.jsa.loadInflightRecord(subj)
		if lerr == nil && reg != nil && reg.ActionToken == _EMPTY_ {
			break
		}
		if time.Now().After(quiet) {
			t.Fatalf("the delivered slot never went quiescent: %+v (%v)", reg, lerr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	var (
		mu       sync.Mutex
		prepares []*mqttAuthorityTransition
		firstErr error
		advLin   string
		inner    atomic.Bool
	)
	testArmTransitionSeam(t, &asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if inner.Load() || tr.Attempt.ID != cid || tr.Attempt.Kind != mqttTransitionAcceptHandoff {
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
		before := asm.jsa.loadOutboundAuthority(cid, asm.sessStreamEpoch)
		inner.Store(true)
		reg, smsg, lerr := asm.jsa.loadInflightRecord(subj)
		if reg != nil {
			advLin = reg.Lin
		}
		if lerr != nil || reg == nil || smsg == nil {
			t.Errorf("reading the obligation's slot: %v", lerr)
		} else if _, aerr := asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
			ID: cid, Epoch: asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
			Gen: gen, Nonce: reg.Lin, To: mqttExchangePubRecObserved, Acq: reg.Acq,
			Register: reg, RegisterSeq: smsg.Sequence,
		}); aerr != nil {
			t.Errorf("landing the PUBREC advance: %v", aerr)
		} else if after := asm.jsa.loadOutboundAuthority(cid, asm.sessStreamEpoch); after.seq != before.seq {
			t.Errorf("the PUBREC advance moved the authority from sequence %d to %d", before.seq, after.seq)
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
	if len(prepares) != 1 || firstErr != nil {
		t.Fatalf("the prepare met the PUBREC advance as a contender: %d prepare(s), first answered %v",
			len(prepares), firstErr)
	}
	if x, _, xerr := asm.jsa.loadOutboundExchange(cid, asm.sessStreamEpoch, gen, advLin); xerr != nil || x == nil ||
		x.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the PUBREC advance did not land: %+v (%v)", x, xerr)
	}
}

// C4 (c): THE ALLOWANCE IS PER CONNECT, across steps and across a restoration re-drive. The first
// drive spends two re-attempts on its prepare and then commits it; its metadata commit is refused
// as a record-CAS loss, which re-drives the whole CONNECT from `restoreSession`; every prepare of
// the re-drive is refused, so it may spend only the two re-attempts left. Six prepares in all.
// Mutation M-C4b: initialise the allowance below `restoreSession`, and the re-drive gets a fresh
// four — eight prepares.
func TestOwnershipAllowanceSurvivesARedrive(t *testing.T) {
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
		if tr.Attempt.ID != cid || tr.Attempt.Kind != mqttTransitionAcceptHandoff {
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
