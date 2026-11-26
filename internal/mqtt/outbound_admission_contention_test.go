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
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A delivery whose admission loses only to stale expectations keeps its place in the
// subscription's order: it is re-attempted past the ladder's budget, on the same callback, until
// it lands, until something other than contention refuses it, or until the incarnation closes.

// testAdmissionContentionRig is a broker, a persistent QoS 1 subscriber on "ac/t", and a publisher.
type testAdmissionContentionRig struct {
	sess   *mqttSession
	sc, pc net.Conn
	sr, pr *mqttReader
	waits  *atomic.Int32
	extend atomic.Int32
}

func newTestAdmissionContentionRig(t *testing.T, id string) *testAdmissionContentionRig {
	t.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rig := &testAdmissionContentionRig{}
	rig.sc, rig.sr = testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.sc.Close() })
	testMQTTCheckConnAck(t, rig.sr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, rig.sc, rig.sr, []*mqttFilter{{filter: "ac/t", qos: 1}}, []byte{1})
	testMQTTFlush(t, rig.sc, nil, rig.sr)
	rig.pc, rig.pr = testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.pc.Close() })
	testMQTTCheckConnAck(t, rig.pr, mqttConnAckRCConnectionAccepted, false)
	rig.sess = testMQTTGetClient(t, s, id).mqtt.sess
	rig.waits = testCountContentionWaits(t, rig.sess)
	mqttTestAdmissionExtendedHook.Set(func(sess *mqttSession, _ int) {
		if sess == rig.sess {
			rig.extend.Add(1)
		}
	})
	t.Cleanup(func() { mqttTestAdmissionExtendedHook.Set(nil) })
	return rig
}

// bumpAuthority commits one write of the session's authority record at the sequence it reads, as
// any of the session's own acknowledgements does. tenure, when set, also moves the tenure: a
// takeover's shape.
func (rig *testAdmissionContentionRig) bumpAuthority(tenure string) error {
	jsa, as := rig.sess.jsa, rig.sess.asm
	al := jsa.loadOutboundAuthority(rig.sess.id, as.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		return fmt.Errorf("the authority reads %v", al.status)
	}
	next := *al.rec
	next.Rev++
	next.Txn = mqttNewInflightNonce()
	if tenure != _EMPTY_ {
		next.Tenure = tenure
	}
	b, err := json.Marshal(&next)
	if err != nil {
		return err
	}
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	_, err = jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(rig.sess.id)), hdr, msg)
	return err
}

// contendAdmissions defeats the first n admission attempts of the subscriber's deliveries, each by
// one authority write committed after the attempt's reads and before its batch. At attempt
// tenureAt (0: never) the write also moves the tenure.
func (rig *testAdmissionContentionRig) contendAdmissions(t *testing.T, n, tenureAt int32) *atomic.Int32 {
	t.Helper()
	var seen atomic.Int32
	testPresendFor(t, rig.sess, "op-admit-publish", _EMPTY_, func(*mqttAuthorityTransition) {
		k := seen.Add(1)
		if k > n {
			return
		}
		tenure := _EMPTY_
		if k == tenureAt {
			tenure = mqttNewInflightNonce()
		}
		if err := rig.bumpAuthority(tenure); err != nil {
			t.Errorf("contending admission attempt %d: %v", k, err)
		}
	})
	return &seen
}

func (rig *testAdmissionContentionRig) publish(t *testing.T, i int) {
	t.Helper()
	testMQTTPublish(t, rig.pc, rig.pr, 1, false, false, "ac/t", uint16(i+1), []byte(fmt.Sprintf("m%d", i)))
}

// TestAdmissionContentionKeepsTheSubscriptionsOrder: an admission that loses ten consecutive
// attempts — more than its budget of mqttForegroundAcquireAttempts — to the session's own authority
// writes is still delivered, in publish order, without a JetStream re-offer.
func TestAdmissionContentionKeepsTheSubscriptionsOrder(t *testing.T) {
	rig := newTestAdmissionContentionRig(t, "ac-order")
	const lost = 10
	if lost <= mqttForegroundAcquireAttempts {
		t.Fatalf("fixture: %d lost attempts do not exceed the budget of %d", lost, mqttForegroundAcquireAttempts)
	}
	seen := rig.contendAdmissions(t, lost, 0)
	for i := 0; i < 3; i++ {
		rig.publish(t, i)
	}
	for i := 0; i < 3; i++ {
		testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ac/t", 2, []byte(fmt.Sprintf("m%d", i)))
	}
	if got := seen.Load(); got < lost+1 {
		t.Fatalf("fixture: %d admission attempts were made, want more than the %d defeated", got, lost)
	}
	if n := rig.extend.Load(); n < lost-mqttForegroundAcquireAttempts {
		t.Fatalf("the admission was extended %d time(s), want at least %d", n, lost-mqttForegroundAcquireAttempts)
	}
	if n := rig.waits.Load(); n < lost {
		t.Fatalf("%d contention waits spaced %d lost attempts", n, lost)
	}
}

// testMoveLedgerInsideTheBoundary rewrites the generation ledger of sess's first n admission
// attempts of operation op, at the sequence it stands at and with the same body, AFTER the
// attempt's boundary reads were prefetched and before any check runs: the identifier census then
// reads the prefetched ledger and passes, and the allocation's second read sees it moved. An
// attempt that creates its generation's ledger is left alone. Every attempt's commit outcome of
// that operation is collected by a commit wrapper.
func testMoveLedgerInsideTheBoundary(t *testing.T, sess *mqttSession, op string, n int32) (*atomic.Int32, *[]error, *sync.Mutex) {
	t.Helper()
	jsa := sess.jsa
	admission := func(at *mqttAuthorityTransition) bool {
		return at.Attempt != nil && at.Attempt.ID == sess.id && at.Attempt.Op == op
	}
	var moved atomic.Int32
	window := func(at *mqttAuthorityTransition) {
		if !admission(at) || moved.Load() >= n {
			return
		}
		var ledger *mqttTransitionMember
		for i := range at.Extra {
			if mqttIsLedgerSubject(at.Extra[i].subj) {
				ledger = &at.Extra[i]
			}
		}
		if ledger == nil {
			t.Errorf("fixture: admission %q carries no ledger member", at.Attempt.Txn)
			return
		}
		if ledger.expect == 0 {
			return // the generation's first allocation creates its ledger
		}
		prefetched := false
		set := mqttBoundaryPrefetchSet(at.Attempt.ID, at.Extra)
		for _, subj := range set {
			prefetched = prefetched || subj == ledger.subj
		}
		if !prefetched || len(set) < 2 {
			t.Errorf("fixture: the ledger is not among the prefetched reads %v", set)
			return
		}
		// THE ORIGINAL STORE, not the attempt's prefetched view, so the census' one cached answer
		// is left for the census.
		smsg, err := jsa.loadLastMsgFor(mqttSessStreamName, ledger.subj)
		if err != nil || smsg == nil || smsg.Sequence != ledger.expect {
			t.Errorf("fixture: the ledger reads %v at the attempt's window (%v), want sequence %d", smsg, err, ledger.expect)
			return
		}
		msg, hdr := mqttFrameBatchWrite(smsg.Sequence, smsg.Data, _EMPTY_, 0, false)
		if _, err := jsa.storeMsg(ledger.subj, hdr, msg); err != nil {
			t.Errorf("moving the ledger: %v", err)
			return
		}
		moved.Add(1)
	}
	jsa.testTransitionPrefetched.Store(&window)
	t.Cleanup(func() { jsa.testTransitionPrefetched.Store(nil) })

	var mu sync.Mutex
	var outcomes []error
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if admission(at) {
			mu.Lock()
			outcomes = append(outcomes, err)
			mu.Unlock()
		}
		return seq, err
	}
	jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { jsa.testTransitionCommit.Store(nil) })
	return &moved, &outcomes, &mu
}

// testRequireMovedLedgerRefusals requires every collected refusal to be a stale expectation, and
// exactly want of them to be the allocation's moved-ledger refusal. Other stale refusals are the
// session's own concurrent writers (the store's CAS, or a census fence), which a backlog produces
// on its own; a moved ledger refused by anything but the allocation's read is not counted, so a
// fixture whose write lands before the prefetch fails here.
func testRequireMovedLedgerRefusals(t *testing.T, outcomes *[]error, mu *sync.Mutex, want int) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	refused := 0
	for _, err := range *outcomes {
		if err == nil {
			continue
		}
		if !mqttIsStaleExpectation(err) {
			t.Fatalf("an admission attempt was refused by something other than contention: %v", err)
		}
		if strings.Contains(err.Error(), "high-water that has moved") {
			refused++
		}
	}
	if refused != want {
		t.Fatalf("%d attempt(s) were refused by the allocation's read of the moved ledger, want %d", refused, want)
	}
}

// TestAdmissionKeepsItsOrderWhenTheLedgerMovesInsideTheBoundary: a single admission whose
// generation ledger moves between the boundary's prefetch and the allocation's own read — more
// times than its budget — is refused as a stale expectation each time and extended rather than
// abandoned, so it is delivered before anything published after it.
func TestAdmissionKeepsItsOrderWhenTheLedgerMovesInsideTheBoundary(t *testing.T) {
	rig := newTestAdmissionContentionRig(t, "ac-ledger")
	// A WARM-UP DELIVERY creates the generation's ledger, so every admission below CASes it at a
	// nonzero expectation.
	rig.publish(t, 0)
	testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ac/t", 2, []byte("m0"))
	const lost = 10
	if lost <= mqttForegroundAcquireAttempts {
		t.Fatalf("fixture: %d lost attempts do not exceed the budget of %d", lost, mqttForegroundAcquireAttempts)
	}
	moved, outcomes, mu := testMoveLedgerInsideTheBoundary(t, rig.sess, "op-admit-publish", lost)
	// ONE OFFER AT A TIME, so each is admitted singly: offers pushed together are admitted in a
	// batch (the batched route's own test below).
	for i := 1; i < 4; i++ {
		rig.publish(t, i)
		testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ac/t", 2, []byte(fmt.Sprintf("m%d", i)))
	}
	if got := moved.Load(); got != lost {
		t.Fatalf("fixture: the ledger was moved inside %d single admission attempt(s), want %d", got, lost)
	}
	testRequireMovedLedgerRefusals(t, outcomes, mu, lost)
	if n := rig.extend.Load(); n < lost-mqttForegroundAcquireAttempts {
		t.Fatalf("the admission was extended %d time(s), want at least %d", n, lost-mqttForegroundAcquireAttempts)
	}
}

// TestAdmissionBatchKeepsItsOrderWhenTheLedgerMovesInsideTheBoundary: the batched route's version.
// A batch of several deliveries whose ledger moves inside the boundary more times than its budget
// is refused as a stale expectation each time, re-attempted with the same deliveries, and the
// whole backlog arrives in publish order.
func TestAdmissionBatchKeepsItsOrderWhenTheLedgerMovesInsideTheBoundary(t *testing.T) {
	rig := newTestBatchRig(t, "ab-ledger", 1, nil)
	sess := rig.sess()
	testCountContentionWaits(t, sess)
	var extended atomic.Int32
	mqttTestAdmissionExtendedHook.Set(func(s *mqttSession, _ int) {
		if s.id == sess.id {
			extended.Add(1)
		}
	})
	t.Cleanup(func() { mqttTestAdmissionExtendedHook.Set(nil) })
	evMu, evs := testRecordBatchOrder(t, nil)
	lose := int32(mqttForegroundAcquireAttempts + 3)
	moved, outcomes, mu := testMoveLedgerInsideTheBoundary(t, sess, "op-admit-publishes", lose)
	const n = 3 * mqttAdmissionBatchMax
	rig.backlog(n)
	rig.readInOrder(n)
	if got := moved.Load(); got != lose {
		t.Fatalf("fixture: the ledger was moved inside %d batch attempt(s), want %d", got, lose)
	}
	testRequireMovedLedgerRefusals(t, outcomes, mu, int(lose))
	if extended.Load() == 0 {
		t.Fatalf("the batch was not extended past its budget of %d", mqttForegroundAcquireAttempts)
	}
	// EVERY REFUSED ATTEMPT CARRIED SEVERAL DELIVERIES, and the batch that lost more than its
	// budget was re-attempted with the same ones.
	evMu.Lock()
	defer evMu.Unlock()
	var parts []string
	refused := 0
	for _, e := range *evs {
		if !e.attempt || e.err == nil {
			continue
		}
		if len(e.nonces) < 2 {
			t.Fatalf("fixture: a refused batch attempt carried %d delivery(ies)", len(e.nonces))
		}
		if parts == nil {
			parts = e.nonces
		}
		if strings.Join(e.nonces, ",") == strings.Join(parts, ",") {
			refused++
		}
	}
	if refused <= mqttForegroundAcquireAttempts {
		t.Fatalf("the contended batch was refused %d time(s), not past its budget of %d", refused, mqttForegroundAcquireAttempts)
	}
}

// TestAdmissionExtensionEndsOnAMovedTenure: a write that moves the tenure turns the next attempt
// into a refusal, which is not contention, so the admission is not extended past its budget and is
// abandoned as it always was.
func TestAdmissionExtensionEndsOnAMovedTenure(t *testing.T) {
	rig := newTestAdmissionContentionRig(t, "ac-tenure")
	rig.contendAdmissions(t, 1000, 2)
	rig.publish(t, 0)
	if _, _, ok := testMQTTReadPacketReady(rig.sr, 2*time.Second); ok {
		t.Fatalf("a delivery was transmitted after its connection's tenure moved")
	}
	if n := rig.extend.Load(); n != 0 {
		t.Fatalf("an admission refused for a moved tenure was extended %d time(s)", n)
	}
}

// TestAdmissionExtensionEndsWhenItsConnectionCloses: an admission that would be contended forever
// stops when its connection closes, and its delivery callback returns.
func TestAdmissionExtensionEndsWhenItsConnectionCloses(t *testing.T) {
	rig := newTestAdmissionContentionRig(t, "ac-close")
	rig.contendAdmissions(t, 1<<30, 0)
	returned := make(chan struct{}, 1)
	done := func(sub *subscription) {
		if sub != nil && sub.client != nil && sub.client.mqtt != nil && sub.client.mqtt.sess == rig.sess {
			select {
			case returned <- struct{}{}:
			default:
			}
		}
	}
	mqttTestDeliveryDoneHookV.Store(&done)
	t.Cleanup(func() { mqttTestDeliveryDoneHookV.Store(nil) })
	rig.publish(t, 0)
	deadline := time.Now().Add(10 * time.Second)
	for rig.extend.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("the admission was never extended (%d)", rig.extend.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-returned:
		t.Fatalf("the delivery callback returned while its admission was still being extended")
	default:
	}
	rig.sc.Close()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatalf("the delivery callback did not return after its connection closed (%d extensions)",
			rig.extend.Load())
	}
}

// TestStaleExpectationClassification: only a positively stale expectation qualifies.
func TestStaleExpectationClassification(t *testing.T) {
	cas := ApiErrors[JSStreamWrongLastSequenceErrF]
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"the boundary's stale sequence":      {fmt.Errorf("transition %q mutates resource: %w", "t", errMQTTStaleExpectation), true},
		"the store's CAS rejection, wrapped": {fmt.Errorf("committing: %w", cas), true},
		"a read failure":                     {errors.New("the authority could not be read"), false},
		"a moved entitlement":                {errors.New("the standing authority carries another tenure"), false},
		"a timeout":                          {fmt.Errorf("committing: %w", errors.New("timeout")), false},
		"nothing":                            {nil, false},
		"the boundary's helper":              {mqttStaleExpectationf("transition %q moved", "t"), true},
		"the helper, wrapped":                {fmt.Errorf("committing: %w", mqttStaleExpectationf("moved")), true},
	} {
		if got := mqttIsStaleExpectation(tc.err); got != tc.want {
			t.Fatalf("%s: classified %v, want %v", name, got, tc.want)
		}
	}
	// AN ABSENCE IS THE STORE'S 404, seen through the wrapping a loader adds; anything else is not.
	notFound := ApiErrors[JSNoMessageFoundErr]
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"the store's 404":         {notFound, true},
		"the 404, wrapped":        {fmt.Errorf("exchange %q could not be read: %w", "x", notFound), true},
		"another JetStream error": {fmt.Errorf("reading: %w", cas), false},
		"a transport failure":     {errors.New("timeout"), false},
		"nothing":                 {nil, false},
	} {
		if got := mqttReadIsAbsence(tc.err); got != tc.want {
			t.Fatalf("%s: read as absence %v, want %v", name, got, tc.want)
		}
	}
	// AND EVERY ATTEMPT MUST QUALIFY: one attempt that failed otherwise disqualifies the extension.
	x := &mqttDeliveryContentionExtension{life: &mqttDeliveryLifecycle{sess: &mqttSession{}, c: &client{}}}
	x.noteWrite(errMQTTStaleExpectation)
	x.noteWrite(errors.New("a read failure"))
	if x.extend(2, errMQTTStaleExpectation) {
		t.Fatalf("an admission whose attempts did not all lose to contention was extended")
	}
}
