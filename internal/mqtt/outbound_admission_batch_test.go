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
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A resource step that adds several dependencies at once is accepted only as holds of a name-owned
// consumer, each added edge judged exactly as a single one is; every other shape keeps the
// single-edge rule.

func testBatchStepResource(kind mqttResourceKind, deps ...mqttResourceEdge) *mqttOutboundResource {
	return &mqttOutboundResource{ID: "c", Epoch: "e", Gen: "g", Inc: "inc-1", Kind: kind, Name: "$MQTT_dur",
		State: mqttResourceLive, Deps: deps}
}

func testBatchStep(cur *mqttOutboundResource, add []mqttResourceEdge, created ...string) error {
	next := *cur
	next.Deps = append(append([]mqttResourceEdge{}, cur.Deps...), add...)
	createdHere := make(map[string]struct{}, len(created))
	for _, n := range created {
		createdHere[n] = struct{}{}
	}
	ownedLive := func(*mqttOutboundResource) error { return nil }
	return mqttCheckResourceStep(&mqttTransitionAttempt{Txn: "txn-b"}, cur, &next, createdHere, nil, nil, nil, ownedLive, nil)
}

func TestAdmissionBatchResourceStepAddsSeveralHolds(t *testing.T) {
	holds := []mqttResourceEdge{
		{Nonce: "x1", Role: mqttRoleSourceReplay},
		{Nonce: "x2", Role: mqttRoleSourceReplay},
		{Nonce: "x3", Role: mqttRoleSourceReplay},
	}
	src := testBatchStepResource(mqttResourceSourceConsumer)
	if err := testBatchStep(src, holds, "x1", "x2", "x3"); err != nil {
		t.Fatalf("three source-replay holds, each of an exchange this batch creates: %v", err)
	}
	// ONE INVALID EDGE AMONG VALID ONES is refused, wherever it stands.
	for i := range holds {
		created := []string{}
		for j, h := range holds {
			if j != i {
				created = append(created, h.Nonce)
			}
		}
		if err := testBatchStep(src, holds, created...); err == nil ||
			!strings.Contains(err.Error(), "does not admit") {
			t.Fatalf("edge %d names an exchange the batch does not create, but the step was not refused: %v", i, err)
		}
	}
	disposed := testBatchStepResource(mqttResourceSourceConsumer)
	disposed.Disposal = &mqttResourceDisposal{Txn: "txn-d", Reason: "test"}
	if err := testBatchStep(disposed, holds, "x1", "x2", "x3"); err == nil ||
		!strings.Contains(err.Error(), "was disposed") {
		t.Fatalf("several holds on a disposed consumer must be refused, got %v", err)
	}
	// THE NAME MUST BE LIVE AND OWNED, judged for the batch's edges as for one.
	next := *src
	next.Deps = append([]mqttResourceEdge{}, holds...)
	notOwned := errors.New("the name is retired")
	err := mqttCheckResourceStep(&mqttTransitionAttempt{Txn: "txn-b"}, src, &next,
		map[string]struct{}{"x1": {}, "x2": {}, "x3": {}}, nil, nil, nil,
		func(*mqttOutboundResource) error { return notOwned }, nil)
	if !errors.Is(err, notOwned) {
		t.Fatalf("several holds on a name that is not live and owned must be refused, got %v", err)
	}
}

func TestAdmissionBatchResourceStepKeepsTheSingleEdgeRuleElsewhere(t *testing.T) {
	// A NON-HOLD ROLE never adds two edges in one act.
	payload := testBatchStepResource(mqttResourceRetainedPayload)
	material := []mqttResourceEdge{
		{Nonce: "x1", Role: mqttRoleRetainedMaterial},
		{Nonce: "x2", Role: mqttRoleRetainedMaterial},
	}
	if err := testBatchStep(payload, material, "x1", "x2"); err == nil ||
		!strings.Contains(err.Error(), "more than one dependency admitted at once") {
		t.Fatalf("two retained-material edges in one act must be refused, got %v", err)
	}
	// AND A RESOURCE THAT IS NOT LOGICAL-USE takes no two holds in one act either.
	holdsOnPayload := []mqttResourceEdge{
		{Nonce: "x1", Role: mqttRoleSourceReplay},
		{Nonce: "x2", Role: mqttRoleSourceReplay},
	}
	if err := testBatchStep(payload, holdsOnPayload, "x1", "x2"); err == nil ||
		!strings.Contains(err.Error(), "more than one dependency admitted at once") {
		t.Fatalf("two holds on a resource that is not a name-owned consumer must be refused, got %v", err)
	}
}

// TestAdmissionBatchResourceStepRefusesAReleaseBesideAdditions: a non-hold release changes nothing
// else in its act, however many dependencies the act would add beside it.
func TestAdmissionBatchResourceStepRefusesAReleaseBesideAdditions(t *testing.T) {
	cur := testBatchStepResource(mqttResourceSourceConsumer, mqttResourceEdge{Nonce: "old", Role: mqttRoleRetainedMaterial})
	next := *cur
	next.Deps = []mqttResourceEdge{
		{Nonce: "x1", Role: mqttRoleSourceReplay},
		{Nonce: "x2", Role: mqttRoleSourceReplay},
	}
	err := mqttCheckResourceStep(&mqttTransitionAttempt{Txn: "txn-b"}, cur, &next,
		map[string]struct{}{"x1": {}, "x2": {}}, nil, func(string, string) error { return nil }, nil,
		func(*mqttOutboundResource) error { return nil }, nil)
	if err == nil {
		t.Fatalf("a non-hold release beside two added holds must be refused")
	}
	if !strings.Contains(err.Error(), "both released") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// testBatchRig is a broker, a persistent subscriber on "ab/t" and a publisher.
type testBatchRig struct {
	t      *testing.T
	s      *Server
	o      *Options
	id     string
	sc, pc net.Conn
	sr, pr *mqttReader
	qos    byte
}

func newTestBatchRig(t *testing.T, id string, qos byte, tune func(o *Options)) *testBatchRig {
	t.Helper()
	o := testMQTTDefaultOptions()
	if tune != nil {
		tune(o)
	}
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rig := &testBatchRig{t: t, s: s, o: o, id: id, qos: qos}
	rig.connectSubscriber(false)
	testMQTTSub(t, 1, rig.sc, rig.sr, []*mqttFilter{{filter: "ab/t", qos: qos}}, []byte{qos})
	testMQTTFlush(t, rig.sc, nil, rig.sr)
	rig.pc, rig.pr = testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.pc.Close() })
	testMQTTCheckConnAck(t, rig.pr, mqttConnAckRCConnectionAccepted, false)
	return rig
}

func (rig *testBatchRig) connectSubscriber(present bool) {
	rig.t.Helper()
	rig.sc, rig.sr = testMQTTConnect(rig.t, &mqttConnInfo{clientID: rig.id, cleanSess: false}, rig.o.MQTT.Host, rig.o.MQTT.Port)
	sc := rig.sc
	rig.t.Cleanup(func() { sc.Close() })
	testMQTTCheckConnAck(rig.t, rig.sr, mqttConnAckRCConnectionAccepted, present)
}

func (rig *testBatchRig) sess() *mqttSession {
	return testMQTTGetClient(rig.t, rig.s, rig.id).mqtt.sess
}

func (rig *testBatchRig) publish(qos byte, i int) {
	rig.t.Helper()
	testMQTTPublish(rig.t, rig.pc, rig.pr, qos, false, false, "ab/t", uint16(i%60000+1), []byte(fmt.Sprintf("m%d", i)))
}

// backlog disconnects the subscriber, publishes n messages to it, and reconnects it, so the
// session's consumer pushes them all at once.
func (rig *testBatchRig) backlog(n int) {
	rig.t.Helper()
	rig.detach()
	for i := 0; i < n; i++ {
		rig.publish(rig.qos, i)
	}
	rig.connectSubscriber(true)
}

// detach closes the subscriber's connection and waits until its session no longer serves it.
func (rig *testBatchRig) detach() {
	rig.t.Helper()
	sess := rig.sess()
	rig.sc.Close()
	checkFor(rig.t, 10*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != nil {
			return fmt.Errorf("the session still serves a connection")
		}
		return nil
	})
}

// readInOrder reads n first transmissions in publish order and acknowledges each.
func (rig *testBatchRig) readInOrder(n int) {
	rig.t.Helper()
	for i := 0; i < n; i++ {
		testMQTTCheckPubMsg(rig.t, rig.sc, rig.sr, "ab/t", rig.qos<<1, []byte(fmt.Sprintf("m%d", i)))
	}
}

// testBatchRecorder observes batched admission: each settled attempt's size and verdict, and the
// most offers any subscription held at once.
type testBatchRecorder struct {
	mu       sync.Mutex
	sizes    []int
	verdicts []mqttTransitionVerdict
	held     int
	maxHeld  int
	singles  int
}

func testRecordBatches(t *testing.T) *testBatchRecorder {
	r := &testBatchRecorder{}
	mqttTestAdmissionBatchSettledHook.Set(func(_ *mqttSession, nonces []string, v mqttTransitionVerdict) {
		r.mu.Lock()
		r.sizes = append(r.sizes, len(nonces))
		r.verdicts = append(r.verdicts, v)
		r.mu.Unlock()
	})
	mqttTestHeldOffersHook.Set(func(_ *subscription, held int) {
		r.mu.Lock()
		r.held++
		if held > r.maxHeld {
			r.maxHeld = held
		}
		r.mu.Unlock()
	})
	mqttTestAdmissionBatchSingleHook.Set(func(*mqttSession, string) {
		r.mu.Lock()
		r.singles++
		r.mu.Unlock()
	})
	t.Cleanup(func() {
		mqttTestAdmissionBatchSettledHook.Set(nil)
		mqttTestHeldOffersHook.Set(nil)
		mqttTestAdmissionBatchSingleHook.Set(nil)
	})
	return r
}

// landedBatches counts the settled attempts that carried more than one delivery and LANDED.
func (r *testBatchRecorder) landedBatches() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for i, sz := range r.sizes {
		if sz > 1 && (r.verdicts[i] == mqttTransitionLanded || r.verdicts[i] == mqttTransitionSuperseded) {
			n++
		}
	}
	return n
}

// TestAdmissionBatchAdmitsABacklogTogether: a backlog pushed to a reconnecting subscriber is
// admitted several deliveries per committed batch, and reaches the client once each, in publish
// order, as first transmissions.
func TestAdmissionBatchAdmitsABacklogTogether(t *testing.T) {
	rig := newTestBatchRig(t, "ab-backlog", 1, nil)
	rec := testRecordBatches(t)
	const n = 40
	rig.backlog(n)
	rig.readInOrder(n)
	if _, _, ok := testMQTTReadPacketReady(rig.sr, 300*time.Millisecond); ok {
		t.Fatalf("a packet followed the %d deliveries", n)
	}
	if rec.landedBatches() == 0 {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		t.Fatalf("no committed batch carried more than one delivery (sizes %v, verdicts %v)", rec.sizes, rec.verdicts)
	}
}

// TestAdmissionBatchDoesNotHoldAnOfferAtRest: a delivery with nothing queued behind it is
// processed by the callback that received it: nothing is held, and it reaches the client at once.
func TestAdmissionBatchDoesNotHoldAnOfferAtRest(t *testing.T) {
	rig := newTestBatchRig(t, "ab-rest", 1, nil)
	rec := testRecordBatches(t)
	for i := 0; i < 5; i++ {
		rig.publish(1, i)
		b, pl, ok := testMQTTReadPacketReady(rig.sr, 3*time.Second)
		if !ok {
			t.Fatalf("delivery %d did not reach the client within 3s at rest", i)
		}
		if pt := b & mqttPacketMask; pt != mqttPacketPub {
			t.Fatalf("expected a PUBLISH, got %x", pt)
		}
		flags, pi, _, _ := testMQTTGetPubMsgExEx(t, nil, rig.sr, b, pl, "ab/t", []byte(fmt.Sprintf("m%d", i)))
		if flags != 2 {
			t.Fatalf("delivery %d carried flags %x", i, flags)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.held != 0 {
		t.Fatalf("%d offers were held at rest", rec.held)
	}
}

// TestAdmissionBatchHoldsAtMostTheBatchBound: however large the backlog, a subscription holds at
// most the batch bound less the offer that processes them, and no batch exceeds the bound.
func TestAdmissionBatchHoldsAtMostTheBatchBound(t *testing.T) {
	rig := newTestBatchRig(t, "ab-bound", 1, nil)
	rec := testRecordBatches(t)
	const n = 100
	rig.backlog(n)
	rig.readInOrder(n)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.maxHeld == 0 {
		t.Fatalf("fixture: no offer was ever held under a backlog of %d", n)
	}
	if rec.maxHeld > mqttAdmissionBatchMax-1 {
		t.Fatalf("a subscription held %d offers, above the bound of %d", rec.maxHeld, mqttAdmissionBatchMax-1)
	}
	for _, sz := range rec.sizes {
		if sz > mqttAdmissionBatchMax {
			t.Fatalf("a batch carried %d deliveries, above the bound of %d", sz, mqttAdmissionBatchMax)
		}
	}
}

// TestAdmissionBatchHeldOffersDieWithTheirConnection: offers held when the subscriber's connection
// closes are neither allocated nor acknowledged; the next connection receives every message once,
// in order.
func TestAdmissionBatchHeldOffersDieWithTheirConnection(t *testing.T) {
	rig := newTestBatchRig(t, "ab-close", 1, func(o *Options) { o.MQTT.AckWait = time.Second })
	rec := testRecordBatches(t)
	const n = 30
	// The connection closes as soon as the first offers are held.
	closed := make(chan struct{})
	var once sync.Once
	mqttTestHeldOffersHook.Set(func(_ *subscription, held int) {
		rec.mu.Lock()
		rec.held++
		rec.mu.Unlock()
		if held >= 3 {
			once.Do(func() { close(closed) })
		}
	})
	rig.detach()
	for i := 0; i < n; i++ {
		rig.publish(1, i)
	}
	rig.connectSubscriber(true)
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatalf("fixture: no offers were held")
	}
	rig.detach()
	mqttTestHeldOffersHook.Set(nil)
	rig.connectSubscriber(true)
	// Every message arrives; those the first connection sent before it closed come again with DUP.
	seen := map[string]int{}
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < n && time.Now().Before(deadline) {
		flags, pi, _, payload := testMQTTGetPubMsgEx(t, rig.sc, rig.sr, "ab/t", nil)
		seen[string(payload)]++
		_ = flags
		testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
	}
	for i := 0; i < n; i++ {
		if seen[fmt.Sprintf("m%d", i)] == 0 {
			t.Fatalf("message m%d never reached the second connection (%d distinct seen)", i, len(seen))
		}
	}
}

// testBatchEvent is one step of a subscription's batched admission, in the order it happened.
type testBatchEvent struct {
	attempt bool // an attempt submitted (else a verdict applied)
	nonces  []string
	err     error
	verdict mqttTransitionVerdict
}

// testRecordBatchOrder records every batch attempt and every verdict applied, in order. check, when
// set, runs inside the settled hook, immediately after the verdict was applied.
func testRecordBatchOrder(t *testing.T, check func(nonces []string, v mqttTransitionVerdict)) (*sync.Mutex, *[]testBatchEvent) {
	var mu sync.Mutex
	var evs []testBatchEvent
	mqttTestAdmissionBatchAttemptHook.Set(func(_ *mqttSession, nonces []string, err error) {
		mu.Lock()
		evs = append(evs, testBatchEvent{attempt: true, nonces: append([]string(nil), nonces...), err: err})
		mu.Unlock()
	})
	mqttTestAdmissionBatchSettledHook.Set(func(_ *mqttSession, nonces []string, v mqttTransitionVerdict) {
		mu.Lock()
		evs = append(evs, testBatchEvent{nonces: append([]string(nil), nonces...), verdict: v})
		mu.Unlock()
		if check != nil {
			check(nonces, v)
		}
	})
	t.Cleanup(func() {
		mqttTestAdmissionBatchAttemptHook.Set(nil)
		mqttTestAdmissionBatchSettledHook.Set(nil)
	})
	return &mu, &evs
}

// testCustodyAfterSettlement checks, immediately after a verdict was applied, that every
// participant's attempt is settled and, for a landing, that its exchange is charged.
func testCustodyAfterSettlement(t *testing.T, sess *mqttSession, nonces []string, landed bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	l := sess.custody
	for _, n := range nonces {
		if adm := l.heldAdmissions[n]; adm != nil && len(adm.unsettled) != 0 {
			t.Errorf("participant %q still has %d unsettled attempt(s) after the verdict", n, len(adm.unsettled))
		}
		if landed {
			if _, ok := l.heldExchanges[n]; !ok {
				t.Errorf("participant %q landed but its exchange carries no charge", n)
			}
		}
	}
}

// bumpAuthority commits one write of the session's authority record at the sequence it reads.
func testBumpSessionAuthority(sess *mqttSession) error {
	jsa, as := sess.jsa, sess.asm
	al := jsa.loadOutboundAuthority(sess.id, as.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		return fmt.Errorf("the authority reads %v", al.status)
	}
	next := *al.rec
	next.Rev++
	next.Txn = mqttNewInflightNonce()
	b, err := json.Marshal(&next)
	if err != nil {
		return err
	}
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	_, err = jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(sess.id)), hdr, msg)
	return err
}

// testOnFirstBatchCommit wraps the first committed batch of the session (op-admit-publishes).
func testOnFirstBatchCommit(t *testing.T, sess *mqttSession,
	wrap func(send func() (uint64, error)) (uint64, error)) *atomic.Int32 {
	var n atomic.Int32
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt == nil || at.Attempt.ID != sess.id || at.Attempt.Op != "op-admit-publishes" {
			return send()
		}
		if n.Add(1) == 1 {
			return wrap(send)
		}
		return send()
	}
	sess.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { sess.jsa.testTransitionCommit.Store(nil) })
	return &n
}

// testNothingBeforeTheVerdict fails if any attempt carrying a participant of attempt i was
// submitted after it and before its verdict.
func testNothingBeforeTheVerdict(t *testing.T, evs []testBatchEvent, i int) {
	t.Helper()
	parts := map[string]bool{}
	for _, n := range evs[i].nonces {
		parts[n] = true
	}
	for j := i + 1; j < len(evs); j++ {
		if !evs[j].attempt {
			return
		}
		for _, n := range evs[j].nonces {
			if parts[n] {
				t.Fatalf("participant %q was submitted again (event %d) before attempt %d's verdict", n, j, i)
			}
		}
	}
}

// TestAdmissionBatchLostResponseSettlesEveryParticipantOnce: the first batch's commit lands but its
// response is lost. Nothing is submitted for any participant until the one receipt is resolved;
// every participant settles LANDED from it, at once; and no reservation outlives the batch.
func TestAdmissionBatchLostResponseSettlesEveryParticipantOnce(t *testing.T) {
	rig := newTestBatchRig(t, "ab-lost", 1, nil)
	sess := rig.sess()
	var checked atomic.Int32
	mu, evs := testRecordBatchOrder(t, func(nonces []string, v mqttTransitionVerdict) {
		if checked.Add(1) == 1 {
			testCustodyAfterSettlement(t, sess, nonces, true)
		}
	})
	testOnFirstBatchCommit(t, sess, func(send func() (uint64, error)) (uint64, error) {
		if _, err := send(); err != nil {
			return 0, err
		}
		return 0, errors.New("the batch's response was lost")
	})
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	mu.Lock()
	got := append([]testBatchEvent(nil), (*evs)...)
	mu.Unlock()
	first := -1
	for i, e := range got {
		if e.attempt && e.err != nil && len(e.nonces) > 1 {
			first = i
			break
		}
	}
	if first < 0 {
		t.Fatalf("fixture: no batch attempt lost its response (%d events)", len(got))
	}
	testNothingBeforeTheVerdict(t, got, first)
	if v := got[first+1]; v.attempt || v.verdict != mqttTransitionLanded {
		t.Fatalf("the lost batch was settled %v, want landed", v.verdict)
	}
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if k := len(sess.custody.heldAdmissions); k != 0 {
			return fmt.Errorf("%d admission reservations remain", k)
		}
		return nil
	})
}

// TestAdmissionBatchSupersededReceiptIsALanding: the lost batch's receipt is resolved after a later
// authority revision; SUPERSEDED means it committed, so every participant is charged at once and
// none is submitted again.
func TestAdmissionBatchSupersededReceiptIsALanding(t *testing.T) {
	rig := newTestBatchRig(t, "ab-super", 1, nil)
	sess := rig.sess()
	var checked atomic.Int32
	mu, evs := testRecordBatchOrder(t, func(nonces []string, v mqttTransitionVerdict) {
		if checked.Add(1) == 1 {
			testCustodyAfterSettlement(t, sess, nonces, true)
		}
	})
	testOnFirstBatchCommit(t, sess, func(send func() (uint64, error)) (uint64, error) {
		if _, err := send(); err != nil {
			return 0, err
		}
		if err := testBumpSessionAuthority(sess); err != nil {
			t.Errorf("revising the authority: %v", err)
		}
		return 0, errors.New("the batch's response was lost")
	})
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	mu.Lock()
	got := append([]testBatchEvent(nil), (*evs)...)
	mu.Unlock()
	for i, e := range got {
		if e.attempt && e.err != nil && len(e.nonces) > 1 {
			testNothingBeforeTheVerdict(t, got, i)
			if v := got[i+1]; v.verdict != mqttTransitionSuperseded {
				t.Fatalf("the lost batch resolved %v, want superseded", v.verdict)
			}
			return
		}
	}
	t.Fatalf("fixture: no batch attempt lost its response")
}

// TestAdmissionBatchNeverLandedIsSettledBeforeItsRetry: a batch refused at the store after its
// handle was frozen is settled NEVER LANDED by its abort before the batch is attempted again, and
// the retry lands; nothing is delivered twice.
func TestAdmissionBatchNeverLandedIsSettledBeforeItsRetry(t *testing.T) {
	rig := newTestBatchRig(t, "ab-never", 1, nil)
	sess := rig.sess()
	testCountContentionWaits(t, sess)
	mu, evs := testRecordBatchOrder(t, nil)
	testOnFirstBatchCommit(t, sess, func(func() (uint64, error)) (uint64, error) {
		return 0, ApiErrors[JSStreamWrongLastSequenceErrF]
	})
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	if _, _, ok := testMQTTReadPacketReady(rig.sr, 300*time.Millisecond); ok {
		t.Fatalf("a packet followed the %d deliveries", n)
	}
	mu.Lock()
	got := append([]testBatchEvent(nil), (*evs)...)
	mu.Unlock()
	for i, e := range got {
		if e.attempt && e.err != nil && len(e.nonces) > 1 {
			testNothingBeforeTheVerdict(t, got, i)
			if v := got[i+1]; v.verdict != mqttTransitionNeverLanded {
				t.Fatalf("the refused batch settled %v, want never landed", v.verdict)
			}
			if len(got) < i+3 || !got[i+2].attempt {
				t.Fatalf("the refused batch was not attempted again")
			}
			return
		}
	}
	t.Fatalf("fixture: no batch attempt was refused")
}

// TestAdmissionBatchContentionKeepsTheWholeBatch: a batch refused only by contention, on more
// attempts than the ladder's budget, is extended and re-attempted with the same participants until
// it lands; nothing is abandoned and publish order holds.
func TestAdmissionBatchContentionKeepsTheWholeBatch(t *testing.T) {
	rig := newTestBatchRig(t, "ab-stale", 1, nil)
	sess := rig.sess()
	testCountContentionWaits(t, sess)
	var extended atomic.Int32
	mqttTestAdmissionExtendedHook.Set(func(s *mqttSession, _ int) {
		if s == sess {
			extended.Add(1)
		}
	})
	t.Cleanup(func() { mqttTestAdmissionExtendedHook.Set(nil) })
	mu, evs := testRecordBatchOrder(t, nil)
	lose := int32(mqttForegroundAcquireAttempts + 3)
	var lost atomic.Int32
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt != nil && at.Attempt.ID == sess.id && at.Attempt.Op == "op-admit-publishes" && lost.Add(1) <= lose {
			return 0, ApiErrors[JSStreamWrongLastSequenceErrF]
		}
		return send()
	}
	sess.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { sess.jsa.testTransitionCommit.Store(nil) })
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	if extended.Load() == 0 {
		t.Fatalf("the batch was not extended past its budget of %d", mqttForegroundAcquireAttempts)
	}
	mu.Lock()
	got := append([]testBatchEvent(nil), (*evs)...)
	mu.Unlock()
	var parts []string
	for _, e := range got {
		if !e.attempt || len(e.nonces) < 2 {
			continue
		}
		if parts == nil {
			parts = e.nonces
		}
		if e.err == nil {
			break
		}
		if strings.Join(e.nonces, ",") != strings.Join(parts, ",") {
			t.Fatalf("a contended batch was re-attempted with other participants: %v, then %v", parts, e.nonces)
		}
	}
	if parts == nil {
		t.Fatalf("fixture: no batch attempt was made")
	}
}

// TestDeliveryOfferHasOneUnflushedExit is the census of the delivery callback's decision region:
// mqttDeliverQoS12Offer releases the session lock plainly exactly once — the exit of a fresh
// delivery that joins the collector — and every other exit goes through the collector's
// unlockAndFlush. Nor does it submit a source acknowledgement while the lock is held.
func TestDeliveryOfferHasOneUnflushedExit(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "publish_deliver.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body *ast.BlockStmt
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "mqttDeliverQoS12Offer" && fd.Recv == nil {
			body = fd.Body
		}
	}
	if body == nil {
		t.Fatal("mqttDeliverQoS12Offer is not in publish_deliver.go")
	}
	plain, flushing, lockedAcks := 0, 0, 0
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "Unlock":
			if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "mu" {
				if id, ok := inner.X.(*ast.Ident); ok && id.Name == "sess" {
					plain++
				}
			}
		case "unlockAndFlush":
			flushing++
		case "submitSourceAckLocked":
			lockedAcks++
		}
		return true
	})
	if plain != 1 {
		t.Fatalf("mqttDeliverQoS12Offer releases the session lock plainly %d times, want exactly the collect exit", plain)
	}
	if flushing == 0 {
		t.Fatalf("mqttDeliverQoS12Offer has no flushing exit")
	}
	if lockedAcks != 0 {
		t.Fatalf("mqttDeliverQoS12Offer submits %d source acknowledgement(s) under the session lock", lockedAcks)
	}
}

// TestAdmissionBatchRefusedAfterATakeoverBeforeItsCommit: a successor tenure installed after the
// batch captured its connection's entitlement, immediately before the batch reads the standing
// authority, refuses the batch: none of its registers exists and nothing reaches the displaced
// connection.
func TestAdmissionBatchRefusedAfterATakeoverBeforeItsCommit(t *testing.T) {
	rig := newTestBatchRig(t, "ab-takeover", 1, nil)
	var moved atomic.Int32
	var participants []string
	var pmu sync.Mutex
	mqttTestAdmitBatchBeforeAuthorityHook.Set(func(r *mqttAdmitPublishesRequest) {
		if moved.Add(1) != 1 {
			return
		}
		pmu.Lock()
		for _, e := range r.Entries {
			participants = append(participants, mqttInflightSubject(getHash(r.ID), r.Gen, e.Register.PI))
		}
		pmu.Unlock()
		sess := testMQTTGetClient(t, rig.s, rig.id).mqtt.sess
		jsa, as := sess.jsa, sess.asm
		al := jsa.loadOutboundAuthority(sess.id, as.sessStreamEpoch)
		if al.rec == nil {
			t.Errorf("the authority could not be read")
			return
		}
		next := *al.rec
		next.Rev++
		next.Txn = mqttNewInflightNonce()
		next.Tenure = mqttNewInflightNonce()
		b, _ := json.Marshal(&next)
		msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
		if _, err := jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(sess.id)), hdr, msg); err != nil {
			t.Errorf("installing a successor tenure: %v", err)
		}
	})
	t.Cleanup(func() { mqttTestAdmitBatchBeforeAuthorityHook.Set(nil) })
	rig.backlog(10)
	if _, _, ok := testMQTTReadPacketReady(rig.sr, 2*time.Second); ok {
		t.Fatalf("a delivery reached the connection whose tenure was displaced")
	}
	pmu.Lock()
	defer pmu.Unlock()
	if len(participants) < 2 {
		t.Fatalf("fixture: no batch reached its authority read (%d)", len(participants))
	}
	sess := testMQTTGetClient(t, rig.s, rig.id).mqtt.sess
	for _, subj := range participants {
		if _, smsg, err := sess.asm.loadInflightRecord(subj); err != nil || smsg != nil {
			t.Fatalf("register %q of the refused batch stands (err %v)", subj, err)
		}
	}
}

// TestAdmissionBatchHeldOffersKeepTheirOwnHeaders: held offers whose NATS headers differ in length
// (retained and not) are each decoded with their own, and delivered byte-exact.
func TestAdmissionBatchHeldOffersKeepTheirOwnHeaders(t *testing.T) {
	rig := newTestBatchRig(t, "ab-hdr", 1, nil)
	rec := testRecordBatches(t)
	rig.detach()
	const n = 24
	for i := 0; i < n; i++ {
		testMQTTPublish(t, rig.pc, rig.pr, 1, false, i%2 == 0, "ab/t", uint16(i+1),
			[]byte(fmt.Sprintf("m%d-%s", i, strings.Repeat("x", i*7))))
	}
	rig.connectSubscriber(true)
	for i := 0; i < n; i++ {
		testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 2, []byte(fmt.Sprintf("m%d-%s", i, strings.Repeat("x", i*7))))
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.maxHeld == 0 {
		t.Fatalf("fixture: no offer was held")
	}
}

// TestAdmissionBatchSharedSubscriptionNeverHolds: a shared subscription under a backlog never holds
// an offer, and still delivers every message.
func TestAdmissionBatchSharedSubscriptionNeverHolds(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rec := testRecordBatches(t)
	sc, sr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ab-shared", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { sc.Close() })
	testMQTTReadConnAckV5(t, sr)
	testMQTTSubV5(t, sc, sr, 1, []mqttV5SubFilter{{topic: "$share/g/ab/t", opts: 1}})
	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ab-shared-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { pc.Close() })
	testMQTTReadConnAckV5(t, pr)
	const n = 30
	for i := 0; i < n; i++ {
		testMQTTPubV5Props(t, pc, pr, 1, false, uint16(i+1), "ab/t", []byte(fmt.Sprintf("m%d", i)), nil)
	}
	seen := map[string]bool{}
	for len(seen) < n {
		payload, pi, _ := testBatchReadPublishV5(t, sr)
		seen[string(payload)] = true
		testMQTTSendPIPacket(mqttPacketPubAck, t, sc, pi)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.held != 0 {
		t.Fatalf("a shared subscription held %d offers", rec.held)
	}
}

// TestAdmissionBatchRetransmissionFollowsTheCollectedDeliveries: a retransmission processed after a
// fresh delivery in the same callback is sent after it, as offered: the collector is flushed before
// any delivery it did not admit is sent.
func TestAdmissionBatchRetransmissionFollowsTheCollectedDeliveries(t *testing.T) {
	rig := newTestBatchRig(t, "ab-retx", 1, func(o *Options) { o.MQTT.AckWait = time.Second })
	// A is sent and left unanswered, so its AckWait re-offer is a retransmission.
	rig.publish(1, 0)
	testMQTTCheckPubMsgNoAck(t, rig.sc, rig.sr, "ab/t", 2, []byte("m0"))
	// X is processed alone, and its processing is held until B and A's re-offer are both queued
	// behind it, so the next run is [B, A's re-offer]: a fresh delivery, then a retransmission.
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	offered := func(sub *subscription, reply string) {
		if seq, _, _, _, _ := ackReplyInfo(reply); seq == 2 {
			once.Do(func() {
				close(entered)
				<-release
			})
		}
	}
	mqttTestDeliveryOfferedHookV.Store(&offered)
	t.Cleanup(func() { mqttTestDeliveryOfferedHookV.Store(nil) })
	rig.publish(1, 1) // X, stream sequence 2
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture: X was never processed")
	}
	rig.publish(1, 2)                   // B, stream sequence 3
	time.Sleep(1500 * time.Millisecond) // A's AckWait passes: its re-offer queues behind B
	close(release)
	testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 2, []byte("m1"))
	testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 2, []byte("m2"))
	// A's retransmission carries DUP.
	testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 2|mqttPubFlagDup, []byte("m0"))
}

// TestAdmissionBatchSplitsAtAnOccupiedSlot: a register with no exchange occupies one participant's
// identifier subject, so the store refuses the batch as stale (its create-only member). The batch
// splits at that participant: the ones before it are admitted as a batch, the occupant's delivery
// takes the single ladder, and the rest are admitted after it, in that order.
func TestAdmissionBatchSplitsAtAnOccupiedSlot(t *testing.T) {
	rig := newTestBatchRig(t, "ab-split", 1, nil)
	sess := rig.sess()
	testCountContentionWaits(t, sess)
	mu, evs := testRecordBatchOrder(t, nil)
	var singles []string
	mqttTestAdmissionBatchSingleHook.Set(func(_ *mqttSession, nonce string) {
		mu.Lock()
		singles = append(singles, nonce)
		*evs = append(*evs, testBatchEvent{attempt: true, nonces: []string{"single:" + nonce}})
		mu.Unlock()
	})
	t.Cleanup(func() { mqttTestAdmissionBatchSingleHook.Set(nil) })
	// The first batch's third register subject is occupied, between the batch's reads and its
	// commit, by a register naming another delivery of the same durable (one that does not exist).
	var placed atomic.Bool
	testPresendFor(t, sess, "op-admit-publishes", _EMPTY_, func(at *mqttAuthorityTransition) {
		var regs []string
		for _, m := range at.Extra {
			if name, err := mqttParseInflightSubject(m.subj); err == nil && name != nil && m.expect == 0 {
				regs = append(regs, m.subj)
			}
		}
		if len(regs) < 4 || !placed.CompareAndSwap(false, true) {
			return
		}
		name, _ := mqttParseInflightSubject(regs[2])
		dur := _EMPTY_
		sess.mu.Lock()
		claim, claimSeq := sess.regClaim, sess.seq
		for _, cc := range sess.cons {
			if cc != nil && cc.Durable != _EMPTY_ {
				dur = cc.Durable
			}
		}
		sess.mu.Unlock()
		const foreignSeq = 999999
		nonce := mqttNewInflightNonce()
		occ := &mqttInflightRecord{PI: name.pi, Dur: dur, Seq: foreignSeq, QoS: 1, State: mqttInflightPublish,
			Nonce: nonce, Lin: nonce, Claim: claim, ClaimSeq: claimSeq,
			SourceAckReply: fmt.Sprintf("$JS.ACK.%s.%s.1.%d.1.0.0", mqttStreamName, dur, foreignSeq)}
		if verr := mqttValidateInflightShape(occ); verr != nil {
			t.Errorf("fixture: the occupying register is not a supported shape: %v", verr)
			return
		}
		body, _ := json.Marshal(occ)
		msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
		if _, err := sess.jsa.storeMsg(regs[2], hdr, msg); err != nil {
			t.Errorf("occupying %q: %v", regs[2], err)
		}
	})
	const n = 10
	rig.backlog(n)
	// Everything but the occupied identifier's delivery arrives in order; that one may follow later.
	got := map[string]bool{}
	deadline := time.Now().Add(20 * time.Second)
	for len(got) < n-1 && time.Now().Before(deadline) {
		_, pi, _, payload := testMQTTGetPubMsgEx(t, rig.sc, rig.sr, "ab/t", nil)
		got[string(payload)] = true
		testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
	}
	mu.Lock()
	defer mu.Unlock()
	var failed []string
	at := -1
	for i, e := range *evs {
		if e.attempt && e.err != nil && mqttIsStaleExpectation(e.err) && len(e.nonces) > 3 {
			failed, at = e.nonces, i
			break
		}
	}
	if at < 0 {
		t.Fatalf("fixture: no batch was refused as stale (events %d)", len(*evs))
	}
	if len(singles) == 0 {
		t.Fatalf("no participant took the single ladder after the split")
	}
	k := -1
	for i, nn := range failed {
		if nn == singles[0] {
			k = i
		}
	}
	if k <= 0 {
		t.Fatalf("the single-ladder participant is at position %d of the refused batch, want after its head", k)
	}
	// After the refusal: the prefix as a batch (landed), then the single, then the suffix.
	// Consecutive attempts of the same participants are one batch re-attempted on contention,
	// the refused batch itself included: a stale refusal is re-attempted whole while its slots
	// read empty, and the split follows the attempt that finds the occupant.
	whole := strings.Join(failed, ",")
	var order []string
	for _, e := range (*evs)[at+1:] {
		if !e.attempt {
			continue
		}
		a := strings.Join(e.nonces, ",")
		if (len(order) == 0 && a == whole) || (len(order) > 0 && order[len(order)-1] == a) {
			continue
		}
		order = append(order, a)
	}
	want := []string{strings.Join(failed[:k], ","), "single:" + failed[k]}
	if k == 1 {
		// A prefix of one delivery is admitted by the single ladder too.
		want[0] = "single:" + failed[0]
	}
	if len(order) < 2 || order[0] != want[0] || order[1] != want[1] {
		t.Fatalf("after the stale refusal the attempts were %v, want the prefix %v then the single %q",
			order, failed[:k], failed[k])
	}
	if k+1 < len(failed) {
		// A suffix of one delivery is admitted by the single ladder.
		if len(order) < 3 || !strings.HasPrefix(strings.TrimPrefix(order[2], "single:"), failed[k+1]) {
			t.Fatalf("the suffix was not attempted after the single: %v", order)
		}
	}
}

// testBatchReadPublishV5 reads one v5 PUBLISH and returns its payload, identifier and Message
// Expiry Interval.
func testBatchReadPublishV5(t *testing.T, r *mqttReader) ([]byte, uint16, uint32) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketPub {
		t.Fatalf("expected a PUBLISH, got %x", pt)
	}
	start := r.pos
	qos := mqttGetQoS(b & mqttPacketFlagMask)
	if _, err := r.readBytes("topic", false); err != nil {
		t.Fatal(err)
	}
	var pi uint16
	if qos > 0 {
		var err error
		if pi, err = r.readUint16("pi"); err != nil {
			t.Fatal(err)
		}
	}
	props, err := r.readProperties(mqttPropsContextPubOut)
	if err != nil {
		t.Fatal(err)
	}
	n := pl - (r.pos - start)
	payload := append([]byte(nil), r.buf[r.pos:r.pos+n]...)
	r.pos += n
	var me uint32
	if props != nil {
		me = props.messageExpiry
	}
	return payload, pi, me
}

// testBatchExpiryRig is a v5 persistent subscriber on "ae/t" whose session holds a backlog of n
// messages published with Message Expiry Interval expiry, aged by age before it reconnects.
func testBatchExpiryRig(t *testing.T, id string, n int, expiry uint32, age time.Duration) (net.Conn, *mqttReader) {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	ci := &mqttV5ConnInfo{clientID: id, cleanStart: false, props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ae/t", opts: 1}})
	sess := testMQTTGetClient(t, s, id).mqtt.sess
	c.Close()
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != nil {
			return fmt.Errorf("still attached")
		}
		return nil
	})
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { cp.Close() })
	testMQTTReadConnAckV5(t, rp)
	for i := 0; i < n; i++ {
		testMQTTPubV5Props(t, cp, rp, 1, false, uint16(i+1), "ae/t", []byte(fmt.Sprintf("m%d", i)), mqttV5PropsWithExpiry(expiry))
	}
	time.Sleep(age)
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	testMQTTReadConnAckV5(t, r2)
	return c2, r2
}

// TestAdmissionBatchExpiryIsJudgedAtTheFlush: a collected delivery whose Message Expiry elapses
// before its flush is dropped there, proven unsent, and never reaches the client.
func TestAdmissionBatchExpiryIsJudgedAtTheFlush(t *testing.T) {
	// Every flush waits until the backlog's 2s Message Expiry has passed since the first one began.
	var mu sync.Mutex
	var until time.Time
	mqttTestDeliveryFlushHook.Set(func(_ *mqttSession, n int) {
		mu.Lock()
		if until.IsZero() {
			until = time.Now().Add(2500 * time.Millisecond)
		}
		d := time.Until(until)
		mu.Unlock()
		if d > 0 {
			time.Sleep(d)
		}
	})
	t.Cleanup(func() { mqttTestDeliveryFlushHook.Set(nil) })
	_, r := testBatchExpiryRig(t, "ae-expired", 12, 2, 0)
	if _, _, ok := testMQTTReadPacketReady(r, 3*time.Second); ok {
		t.Fatalf("a delivery whose Message Expiry elapsed before its flush reached the client")
	}
}

// TestAdmissionBatchExpiryIsReducedOnce: a collected delivery's remaining Message Expiry is the
// ORIGINAL interval less the time the message waited, computed once at the flush.
func TestAdmissionBatchExpiryIsReducedOnce(t *testing.T) {
	var once sync.Once
	mqttTestDeliveryFlushHook.Set(func(_ *mqttSession, n int) {
		if n > 1 {
			once.Do(func() { time.Sleep(time.Second) })
		}
	})
	t.Cleanup(func() { mqttTestDeliveryFlushHook.Set(nil) })
	c, r := testBatchExpiryRig(t, "ae-once", 12, 10, 3*time.Second)
	payload, pi, me := testBatchReadPublishV5(t, r)
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	// Aged about 3s before reconnecting and about 1s more at the flush: about 6s remain. A second
	// reduction of the already-reduced interval would leave about 3.
	if me < 5 || me > 7 {
		t.Fatalf("%s arrived with a Message Expiry Interval of %d, want about 6 (10 less one wait of about 4)", payload, me)
	}
}

// testRunAfterX arranges a run of the subscriber's callback that processes A (fresh) and then B:
// X is published and processed alone, its processing held until queued() reports that wait has
// passed, so A and whatever B is (published by prepare) queue behind it. beforeB runs inside B's
// offer, before anything of B is decided, when B's stream sequence is bSeq.
func (rig *testBatchRig) runAfterX(prepare func(), bSeq uint64, beforeB func()) {
	rig.t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var xOnce, bOnce sync.Once
	offered := func(sub *subscription, reply string) {
		seq, _, _, _, _ := ackReplyInfo(reply)
		switch {
		case seq == 1:
			xOnce.Do(func() {
				close(entered)
				<-release
			})
		case seq == bSeq && beforeB != nil:
			bOnce.Do(beforeB)
		}
	}
	mqttTestDeliveryOfferedHookV.Store(&offered)
	rig.t.Cleanup(func() { mqttTestDeliveryOfferedHookV.Store(nil) })
	rig.publish(1, 0) // X, stream sequence 1
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		rig.t.Fatal("fixture: X was never processed")
	}
	prepare()
	close(release)
}

// testFlushOrder records, in order, "admitted:<pi>" when a collected delivery reaches its send
// section and whatever else the caller appends.
type testFlushOrder struct {
	mu  sync.Mutex
	evs []string
}

func (o *testFlushOrder) add(e string) {
	o.mu.Lock()
	o.evs = append(o.evs, e)
	o.mu.Unlock()
}

func (o *testFlushOrder) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.evs...)
}

func (o *testFlushOrder) index(prefix string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i, e := range o.evs {
		if strings.HasPrefix(e, prefix) {
			return i
		}
	}
	return -1
}

// testRecordAdmittedSends records "admitted:<stream sequence>" as each admitted delivery reaches
// its send section.
func testRecordAdmittedSends(t *testing.T, o *testFlushOrder) {
	admitted := func(sess *mqttSession, pi uint16) {
		sess.mu.Lock()
		var seq uint64
		if p := sess.pendingPublish[pi]; p != nil {
			seq = p.sseq
		}
		sess.mu.Unlock()
		o.add(fmt.Sprintf("admitted:%d", seq))
	}
	mqttTestDeliveryAdmittedPauseV.Store(&admitted)
	t.Cleanup(func() { mqttTestDeliveryAdmittedPauseV.Store(nil) })
}

// TestAdmissionBatchFlushesBeforeAWithheldOffer: an offer whose transmission gate is already
// resolved Withheld when its processing begins records its withholding only after the delivery
// collected ahead of it was flushed.
func TestAdmissionBatchFlushesBeforeAWithheldOffer(t *testing.T) {
	rig := newTestBatchRig(t, "ab-withheld", 1, nil)
	sess := rig.sess()
	order := &testFlushOrder{}
	testRecordAdmittedSends(t, order)
	blocked := func(kind string) { order.add("withheld:" + kind) }
	mqttTestRestoreGateBlockedV.Store(&blocked)
	t.Cleanup(func() { mqttTestRestoreGateBlockedV.Store(nil) })
	var prev *mqttRestoreGate
	rig.runAfterX(func() {
		rig.publish(1, 1) // A, stream sequence 2
		rig.publish(1, 2) // B, stream sequence 3
	}, 3, func() {
		g := &mqttRestoreGate{done: make(chan struct{})}
		g.resolved.Store(true)
		close(g.done)
		prev = sess.restoreGate.Swap(g)
	})
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if order.index("withheld:qos12") < 0 {
			return fmt.Errorf("B was not withheld yet: %v", order.snapshot())
		}
		return nil
	})
	sess.restoreGate.Store(prev)
	a, w := order.index("admitted:2"), order.index("withheld:qos12")
	if a < 0 || a > w {
		t.Fatalf("B recorded its withholding before A was flushed: %v", order.snapshot())
	}
}

// TestAdmissionBatchFlushesBeforeAGateWait: an offer whose transmission gate is still pending when
// its processing begins waits only after the delivery collected ahead of it was flushed.
func TestAdmissionBatchFlushesBeforeAGateWait(t *testing.T) {
	rig := newTestBatchRig(t, "ab-gatewait", 1, nil)
	sess := rig.sess()
	order := &testFlushOrder{}
	testRecordAdmittedSends(t, order)
	g := &mqttRestoreGate{done: make(chan struct{})}
	var prev *mqttRestoreGate
	rig.runAfterX(func() {
		rig.publish(1, 1) // A
		rig.publish(1, 2) // B
	}, 3, func() {
		prev = sess.restoreGate.Swap(g)
	})
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if order.index("admitted:2") < 0 {
			return fmt.Errorf("A was not flushed while B waited on its gate: %v", order.snapshot())
		}
		return nil
	})
	g.opened.Store(true)
	g.resolved.Store(true)
	close(g.done)
	sess.restoreGate.Store(prev)
	testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 2, []byte("m0"))
	testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 2, []byte("m1"))
	testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 2, []byte("m2"))
}

// TestAdmissionBatchFlushesBeforeADeclinedReofferIsAcknowledged: a re-offer of a source the session
// already acknowledged is re-acknowledged only after the delivery collected ahead of it was
// flushed.
func TestAdmissionBatchFlushesBeforeADeclinedReofferIsAcknowledged(t *testing.T) {
	rig := newTestBatchRig(t, "ab-declined", 1, func(o *Options) { o.MQTT.AckWait = time.Second })
	sess := rig.sess()
	order := &testFlushOrder{}
	// B (stream sequence 1) is a source the session's record says it acknowledged while JetStream
	// still holds it: its first offer is refused before tracking, and its acknowledgement recorded as
	// submitted, so its AckWait re-offer reaches the acked-source decline arm.
	var refused atomic.Bool
	restorePre := setMQTTTestInflightPreSendHook(func(dur, reply string) bool {
		// Called by the delivery body under the session lock.
		if seq, _, _, _, _ := ackReplyInfo(reply); seq == 1 && refused.CompareAndSwap(false, true) {
			sess.ackedSources.record(dur, 1)
			return false
		}
		return true
	})
	t.Cleanup(restorePre)
	var declined atomic.Bool
	mqttTestAckedReofferDeclinedHook.Set(func(_ string, seq uint64) {
		if seq == 1 {
			declined.Store(true)
		}
	})
	t.Cleanup(func() { mqttTestAckedReofferDeclinedHook.Set(nil) })
	mqttTestSourceAckDropHook.Set(func(reply string) bool {
		if seq, _, _, _, _ := ackReplyInfo(reply); seq == 1 {
			order.add("acked:B")
		}
		return false
	})
	t.Cleanup(func() { mqttTestSourceAckDropHook.Set(nil) })
	testMQTTPublish(t, rig.pc, rig.pr, 1, false, false, "ab/t", 1, []byte("b"))
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		if !refused.Load() {
			return fmt.Errorf("B's first offer was not refused")
		}
		return nil
	})
	testRecordAdmittedSends(t, order)
	// X (stream sequence 2) is held until A (3) and B's re-offer are queued behind it.
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	offered := func(sub *subscription, reply string) {
		if seq, _, _, _, _ := ackReplyInfo(reply); seq == 2 {
			once.Do(func() {
				close(entered)
				<-release
			})
		}
	}
	mqttTestDeliveryOfferedHookV.Store(&offered)
	t.Cleanup(func() { mqttTestDeliveryOfferedHookV.Store(nil) })
	rig.publish(1, 1) // X
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture: X was never processed")
	}
	rig.publish(1, 2) // A
	time.Sleep(1500 * time.Millisecond)
	close(release)
	testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 2, []byte("m1"))
	testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 2, []byte("m2"))
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if !declined.Load() || order.index("acked:B") < 0 {
			return fmt.Errorf("B's re-offer was not declined and re-acknowledged: %v", order.snapshot())
		}
		return nil
	})
	if a, k := order.index("admitted:3"), order.index("acked:B"); a < 0 || a > k {
		t.Fatalf("B's re-offer was acknowledged before A was flushed: %v", order.snapshot())
	}
}

// testBatchReadAll reads PUBLISHes until every one of the n payloads m0..m<n-1> arrived, answering
// each, and fails if any payload arrived twice as a first transmission (DUP clear).
func (rig *testBatchRig) readAllOnce(n int, within time.Duration) {
	rig.t.Helper()
	first := map[string]int{}
	seen := map[string]bool{}
	deadline := time.Now().Add(within)
	for len(seen) < n {
		if time.Now().After(deadline) {
			rig.t.Fatalf("only %d of %d messages arrived within %v", len(seen), n, within)
		}
		b, pl, ok := testMQTTReadPacketReady(rig.sr, time.Until(deadline))
		if !ok {
			continue
		}
		if pt := b & mqttPacketMask; pt != mqttPacketPub {
			rig.t.Fatalf("expected a PUBLISH, got %x", pt)
		}
		flags, pi, _, payload := testMQTTGetPubMsgExEx(rig.t, nil, rig.sr, b, pl, "ab/t", nil)
		seen[string(payload)] = true
		if flags&mqttPubFlagDup == 0 {
			first[string(payload)]++
			if first[string(payload)] > 1 {
				rig.t.Fatalf("%s was transmitted twice as a first transmission", payload)
			}
		}
		testMQTTSendPIPacket(mqttPacketPubAck, rig.t, rig.sc, pi)
	}
}

// TestAdmissionBatchUnresolvedOutcomeAbandonsEveryParticipant: a batch whose receipt cannot be
// resolved (nor settled by its abort) concludes nothing: every participant is abandoned as the
// ladder abandons on exhaustion, none is retried or split, and every message still reaches the
// client, none twice as a first transmission.
func TestAdmissionBatchUnresolvedOutcomeAbandonsEveryParticipant(t *testing.T) {
	rig := newTestBatchRig(t, "ab-unres", 1, nil)
	sess := rig.sess()
	mu, evs := testRecordBatchOrder(t, nil)
	var lostHandle atomic.Pointer[mqttTransitionHandle]
	testOnFirstBatchCommit(t, sess, func(send func() (uint64, error)) (uint64, error) {
		if _, err := send(); err != nil {
			return 0, err
		}
		return 0, errors.New("the batch's response was lost")
	})
	var forced atomic.Int32
	sess.asm.mu.Lock()
	sess.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if lostHandle.Load() == nil {
			lostHandle.Store(h)
		}
		if h == lostHandle.Load() && forced.Add(1) <= 2 {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	sess.asm.mu.Unlock()
	t.Cleanup(func() {
		sess.asm.mu.Lock()
		sess.asm.testCustodyResolve = nil
		sess.asm.mu.Unlock()
	})
	const n = 12
	rig.backlog(n)
	rig.readAllOnce(n, 60*time.Second)
	mu.Lock()
	defer mu.Unlock()
	for i, e := range *evs {
		if !e.attempt && e.verdict == mqttTransitionUnresolved {
			for j := i + 1; j < len(*evs); j++ {
				for _, nn := range (*evs)[j].nonces {
					for _, p := range e.nonces {
						if nn == p {
							t.Fatalf("participant %q of the unresolved batch was submitted again", p)
						}
					}
				}
			}
			return
		}
	}
	t.Fatalf("fixture: no batch settled unresolved (%d events)", len(*evs))
}

// TestAdmissionBatchFailedReadBackTakesTheLadder: a landed batch whose register read-back fails for
// one participant settles that participant through the single ladder, which adopts its standing
// register by nonce; it is delivered in its position.
func TestAdmissionBatchFailedReadBackTakesTheLadder(t *testing.T) {
	rig := newTestBatchRig(t, "ab-readback", 1, nil)
	sess := rig.sess()
	var target atomic.Value // the register subject whose read-back fails once
	var failed atomic.Bool
	var singles atomic.Int32
	mqttTestAdmissionBatchSettledHook.Set(func(s *mqttSession, nonces []string, v mqttTransitionVerdict) {
		if s != sess || len(nonces) < 3 || v != mqttTransitionLanded || target.Load() != nil {
			return
		}
		s.mu.Lock()
		for pi, p := range s.pendingPublish {
			if p.regNonce == nonces[1] {
				target.Store(mqttInflightSubject(s.idHash, s.gen, pi))
			}
		}
		s.mu.Unlock()
	})
	mqttTestAdmissionBatchSingleHook.Set(func(*mqttSession, string) { singles.Add(1) })
	t.Cleanup(func() {
		mqttTestAdmissionBatchSettledHook.Set(nil)
		mqttTestAdmissionBatchSingleHook.Set(nil)
	})
	sess.asm.mu.Lock()
	sess.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if tg, _ := target.Load().(string); tg == subject && failed.CompareAndSwap(false, true) {
			return nil, nil, errors.New("the register could not be read back")
		}
		return sess.asm.jsa.loadInflightRecord(subject)
	}
	sess.asm.mu.Unlock()
	t.Cleanup(func() {
		sess.asm.mu.Lock()
		sess.asm.testInflightLoad = nil
		sess.asm.mu.Unlock()
	})
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	if !failed.Load() {
		t.Fatalf("fixture: no read-back failed")
	}
	if singles.Load() == 0 {
		t.Fatalf("the participant whose read-back failed did not take the single ladder")
	}
}

// TestAdmissionBatchExpiredDeliveryWaitsForTheLiveOnesAheadOfIt: a flush holding a live delivery
// and, after it, one whose Message Expiry has elapsed disposes of the expired one only after the
// live one ahead of it was admitted and sent.
func TestAdmissionBatchExpiredDeliveryWaitsForTheLiveOnesAheadOfIt(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	ci := &mqttV5ConnInfo{clientID: "ae-mixed", cleanStart: false, props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ae/t", opts: 1}})
	sess := testMQTTGetClient(t, s, "ae-mixed").mqtt.sess
	c.Close()
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != nil {
			return fmt.Errorf("still attached")
		}
		return nil
	})
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ae-mixed-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { cp.Close() })
	testMQTTReadConnAckV5(t, rp)
	// Stream sequences 1..4 live (60 s), 5 expiring (2 s), 6..8 live.
	for i := 0; i < 8; i++ {
		exp := uint32(60)
		if i == 4 {
			exp = 2
		}
		testMQTTPubV5Props(t, cp, rp, 1, false, uint16(i+1), "ae/t", []byte(fmt.Sprintf("m%d", i)), mqttV5PropsWithExpiry(exp))
	}
	order := &testFlushOrder{}
	testRecordAdmittedSends(t, order)
	mqttTestSourceAckDropHook.Set(func(reply string) bool {
		if seq, _, _, _, _ := ackReplyInfo(reply); seq == 5 {
			order.add("disposed:5")
		}
		return false
	})
	t.Cleanup(func() { mqttTestSourceAckDropHook.Set(nil) })
	var mu sync.Mutex
	var until time.Time
	mqttTestDeliveryFlushHook.Set(func(_ *mqttSession, n int) {
		mu.Lock()
		if until.IsZero() {
			until = time.Now().Add(2500 * time.Millisecond)
		}
		d := time.Until(until)
		mu.Unlock()
		if d > 0 {
			time.Sleep(d)
		}
	})
	t.Cleanup(func() { mqttTestDeliveryFlushHook.Set(nil) })
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	testMQTTReadConnAckV5(t, r2)
	for _, want := range []string{"m0", "m1", "m2", "m3", "m5", "m6", "m7"} {
		payload, pi, _ := testBatchReadPublishV5(t, r2)
		if string(payload) != want {
			t.Fatalf("received %s, want %s", payload, want)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
	}
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if order.index("disposed:5") < 0 {
			return fmt.Errorf("the expired delivery was not disposed of: %v", order.snapshot())
		}
		return nil
	})
	evs := order.snapshot()
	d := order.index("disposed:5")
	for _, seq := range []int{1, 2, 3, 4} {
		if a := order.index(fmt.Sprintf("admitted:%d", seq)); a < 0 || a > d {
			t.Fatalf("the expired delivery 5 was disposed of before live delivery %d ahead of it was sent: %v", seq, evs)
		}
	}
}

// testBatchPeers is two frontends sharing one JetStream (the supported multi-frontend topology),
// a persistent subscriber on A with a backlog of n messages about to be pushed, and a publisher.
type testBatchPeers struct {
	oA, oB *Options
	sA, sB *Server
	cid    string
}

func newTestBatchPeers(t *testing.T, cid string, n int) *testBatchPeers {
	t.Helper()
	p := &testBatchPeers{cid: cid}
	p.oA = testMQTTDefaultOptions()
	p.oA.MQTT.AckWait = time.Second
	p.sA = testMQTTRunServer(t, p.oA)
	t.Cleanup(func() { testMQTTShutdownServer(p.sA) }) // owns the shared nats-server: last
	p.oB = testMQTTDefaultOptions()
	p.oB.MQTT.AckWait = time.Second
	testSetStoreDir(p.oB, testStoreDir(p.oA))
	p.sB = testMQTTRunServer(t, p.oB)
	t.Cleanup(func() { p.sB.Shutdown() })
	sc, sr := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, p.oA.MQTT.Host, p.oA.MQTT.Port)
	testMQTTCheckConnAck(t, sr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sc, sr, []*mqttFilter{{filter: "ab/t", qos: 1}}, []byte{1})
	testMQTTFlush(t, sc, nil, sr)
	sess := testMQTTGetClient(t, p.sA, cid).mqtt.sess
	sc.Close()
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != nil {
			return fmt.Errorf("still attached")
		}
		return nil
	})
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, p.oA.MQTT.Host, p.oA.MQTT.Port)
	t.Cleanup(func() { pc.Close() })
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	for i := 0; i < n; i++ {
		testMQTTPublish(t, pc, pr, 1, false, false, "ab/t", uint16(i+1), []byte(fmt.Sprintf("m%d", i)))
	}
	return p
}

// takeOver resumes the subscriber on B while A is held, releasing A once B has accepted the
// CONNECT (or after a bound, if B's acceptance waits on A).
func (p *testBatchPeers) takeOver(t *testing.T, release func()) (net.Conn, *mqttReader) {
	t.Helper()
	type conn struct {
		c net.Conn
		r *mqttReader
	}
	got := make(chan conn, 1)
	go func() {
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: p.cid, cleanSess: false}, p.oB.MQTT.Host, p.oB.MQTT.Port)
		got <- conn{c, r}
	}()
	var cB conn
	select {
	case cB = <-got:
		release()
	case <-time.After(5 * time.Second):
		release()
		cB = <-got
	}
	t.Cleanup(func() { cB.c.Close() })
	testMQTTCheckConnAck(t, cB.r, mqttConnAckRCConnectionAccepted, true)
	return cB.c, cB.r
}

// readAllOnceOn reads PUBLISHes on one connection until all n payloads arrived, answering each, and
// fails if any payload arrives twice as a first transmission.
func testBatchReadAllOnceOn(t *testing.T, c net.Conn, r *mqttReader, n int, within time.Duration) {
	t.Helper()
	first := map[string]int{}
	seen := map[string]bool{}
	deadline := time.Now().Add(within)
	for len(seen) < n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d messages arrived within %v", len(seen), n, within)
		}
		b, pl, ok := testMQTTReadPacketReady(r, time.Until(deadline))
		if !ok {
			continue
		}
		if pt := b & mqttPacketMask; pt != mqttPacketPub {
			t.Fatalf("expected a PUBLISH, got %x", pt)
		}
		flags, pi, _, payload := testMQTTGetPubMsgExEx(t, nil, r, b, pl, "ab/t", nil)
		seen[string(payload)] = true
		if flags&mqttPubFlagDup == 0 {
			if first[string(payload)]++; first[string(payload)] > 1 {
				t.Fatalf("%s was transmitted twice as a first transmission", payload)
			}
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	}
}

// TestAdmissionBatchDisplacedBrokersBatchAdmitsNothing: broker A's batch has captured its
// connection's entitlement and is about to read the authority when the client resumes on broker B,
// which is then offered the same sources and allocates its own identifiers for them. A's batch is
// refused at the entitlement, so no two exchanges stand for one identifier and no source is
// delivered under two identifiers: every message reaches the client on B once as a first
// transmission.
func TestAdmissionBatchDisplacedBrokersBatchAdmitsNothing(t *testing.T) {
	const n = 16
	p := newTestBatchPeers(t, "ab-peers-pre", n)
	var tenureA atomic.Value
	held, release := make(chan struct{}), make(chan struct{})
	var once, rel sync.Once
	mqttTestAdmitBatchBeforeAuthorityHook.Set(func(r *mqttAdmitPublishesRequest) {
		if r.ID != p.cid {
			return
		}
		if tenureA.Load() == nil {
			tenureA.Store(r.Tenure)
		}
		if r.Tenure == tenureA.Load().(string) {
			once.Do(func() {
				close(held)
				<-release
			})
		}
	})
	t.Cleanup(func() { mqttTestAdmitBatchBeforeAuthorityHook.Set(nil) })
	t.Cleanup(func() { rel.Do(func() { close(release) }) })
	cA, rA := testMQTTConnect(t, &mqttConnInfo{clientID: p.cid, cleanSess: false}, p.oA.MQTT.Host, p.oA.MQTT.Port)
	t.Cleanup(func() { cA.Close() })
	testMQTTCheckConnAck(t, rA, mqttConnAckRCConnectionAccepted, true)
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("fixture: A never reached a batch's authority read")
	}
	cB, rB := p.takeOver(t, func() { rel.Do(func() { close(release) }) })
	testBatchReadAllOnceOn(t, cB, rB, n, 60*time.Second)
}

// TestAdmissionBatchDisplacedBrokerSendsNothingItAdmitted: broker A's batch has COMMITTED and is
// about to send when the client resumes on broker B. A sends nothing (its connection no longer
// serves the session); the admitted obligations are B's to deliver, each once as a first
// transmission.
func TestAdmissionBatchDisplacedBrokerSendsNothingItAdmitted(t *testing.T) {
	const n = 16
	p := newTestBatchPeers(t, "ab-peers-post", n)
	held, release := make(chan struct{}), make(chan struct{})
	var once, rel sync.Once
	var sessA atomic.Pointer[mqttSession]
	admitted := func(sess *mqttSession, pi uint16) {
		if sess.id != p.cid || (sessA.Load() != nil && sessA.Load() != sess) {
			return
		}
		sessA.CompareAndSwap(nil, sess)
		once.Do(func() {
			close(held)
			<-release
		})
	}
	mqttTestDeliveryAdmittedPauseV.Store(&admitted)
	t.Cleanup(func() { mqttTestDeliveryAdmittedPauseV.Store(nil) })
	t.Cleanup(func() { rel.Do(func() { close(release) }) })
	cA, rA := testMQTTConnect(t, &mqttConnInfo{clientID: p.cid, cleanSess: false}, p.oA.MQTT.Host, p.oA.MQTT.Port)
	t.Cleanup(func() { cA.Close() })
	testMQTTCheckConnAck(t, rA, mqttConnAckRCConnectionAccepted, true)
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("fixture: A never reached a send section")
	}
	cB, rB := p.takeOver(t, func() { rel.Do(func() { close(release) }) })
	testBatchReadAllOnceOn(t, cB, rB, n, 60*time.Second)
	if _, _, ok := testMQTTReadPacketReady(rA, 500*time.Millisecond); ok {
		t.Fatalf("the displaced connection on A received a packet after its batch was paused")
	}
}

// TestAdmissionBatchRefusalHandsItsHeadToTheLadder: a batch refused for a reason that is not
// contention is settled, its first delivery taken by the single ladder, and the rest attempted
// again as a batch, in that order.
func TestAdmissionBatchRefusalHandsItsHeadToTheLadder(t *testing.T) {
	rig := newTestBatchRig(t, "ab-refused", 1, nil)
	sess := rig.sess()
	mu, evs := testRecordBatchOrder(t, nil)
	mqttTestAdmissionBatchSingleHook.Set(func(_ *mqttSession, nonce string) {
		mu.Lock()
		*evs = append(*evs, testBatchEvent{attempt: true, nonces: []string{"single:" + nonce}})
		mu.Unlock()
	})
	t.Cleanup(func() { mqttTestAdmissionBatchSingleHook.Set(nil) })
	testOnFirstBatchCommit(t, sess, func(func() (uint64, error)) (uint64, error) {
		return 0, errors.New("refused for a reason that is not contention")
	})
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	mu.Lock()
	defer mu.Unlock()
	for i, e := range *evs {
		if !e.attempt || e.err == nil || len(e.nonces) < 3 {
			continue
		}
		if mqttIsStaleExpectation(e.err) {
			t.Fatalf("fixture: the refusal classified as contention: %v", e.err)
		}
		var after []string
		for _, f := range (*evs)[i+1:] {
			if f.attempt {
				after = append(after, strings.Join(f.nonces, ","))
			}
		}
		if len(after) < 2 || after[0] != "single:"+e.nonces[0] || !strings.HasPrefix(after[1], e.nonces[1]) {
			t.Fatalf("after the refusal the attempts were %v, want the head %q singly then the rest from %q",
				after, e.nonces[0], e.nonces[1])
		}
		return
	}
	t.Fatalf("fixture: no batch was refused")
}

// TestAdmissionBatchFlushesBeforeADrainedOffer: an offer whose durable has a drain when its
// processing begins acts only after the delivery collected ahead of it was flushed.
func TestAdmissionBatchFlushesBeforeADrainedOffer(t *testing.T) {
	rig := newTestBatchRig(t, "ab-drained", 1, nil)
	sess := rig.sess()
	order := &testFlushOrder{}
	testRecordAdmittedSends(t, order)
	mqttTestSourceAckDropHook.Set(func(reply string) bool {
		if seq, _, _, _, _ := ackReplyInfo(reply); seq == 3 {
			order.add("acked:B")
		}
		return false
	})
	t.Cleanup(func() { mqttTestSourceAckDropHook.Set(nil) })
	var dur string
	rig.runAfterX(func() {
		rig.publish(1, 1) // A, stream sequence 2
		rig.publish(1, 2) // B, stream sequence 3
	}, 3, func() {
		// A drain stands on the durable when B is processed: an unstaged drain acknowledges a
		// fresh offer away.
		sess.mu.Lock()
		for _, cc := range sess.cons {
			if cc != nil && cc.Durable != _EMPTY_ {
				dur = cc.Durable
			}
		}
		if sess.drainByDur == nil {
			sess.drainByDur = map[string]*mqttDrain{}
		}
		sess.drainByDur[dur] = &mqttDrain{seqs: map[uint64]*drainSeq{}}
		sess.mu.Unlock()
	})
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if order.index("acked:B") < 0 {
			return fmt.Errorf("B was not disposed of by the drain: %v", order.snapshot())
		}
		return nil
	})
	sess.mu.Lock()
	delete(sess.drainByDur, dur)
	sess.mu.Unlock()
	if a, k := order.index("admitted:2"), order.index("acked:B"); a < 0 || a > k {
		t.Fatalf("B was acted on by the drain before A was flushed: %v", order.snapshot())
	}
}

// TestAdmissionBatchIdentifierIsExclusiveForEveryParticipant: a batch whose LATER participant
// claims an identifier a standing obligation still holds is refused whole; the identifier walk
// judges every identifier the batch claims, not its first.
func TestAdmissionBatchIdentifierIsExclusiveForEveryParticipant(t *testing.T) {
	e, done := testF105NewEnv(t, "batch-identifier-exclusive")
	defer done()
	jsa := &e.asm.jsa
	e.bootstrap(t, "txn-1")
	// A registerless obligation stands on identifier 11.
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
		PI: 11, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: "txn-standing", Tenure: al.rec.Tenure,
	}
	tr, terr := testF105AdmitExchangeTr(t, e, al.rec, al.seq, "txn-standing", "op-standing", x)
	if terr != nil {
		t.Fatalf("assembling the standing obligation: %v", terr)
	}
	if _, err := jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("admitting the standing obligation: %v", err)
	}
	batch := func(txn string, pis ...uint16) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		var entries []mqttAdmitPublishesEntry
		for i, pi := range pis {
			entries = append(entries, mqttAdmitPublishesEntry{Register: freshRegisterLineage(&mqttInflightRecord{
				State: mqttInflightPublish, PI: pi, Dur: "$MQTT_src", Seq: uint64(100 + i), QoS: 1,
				Nonce: mqttNewInflightNonce(),
			})})
		}
		_, err := jsa.opAdmitPublishes(&mqttAdmitPublishesRequest{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			Entries: entries,
		})
		return err
	}
	if err := batch("txn-b1", 10, 11); err == nil {
		t.Fatalf("a batch claiming identifier 11 as its second participant was admitted while an obligation holds it")
	} else if !strings.Contains(err.Error(), "still holds at phase") {
		t.Fatalf("the batch was refused for another reason than the standing obligation: %v", err)
	}
	if err := batch("txn-b2", 10, 12); err != nil {
		t.Fatalf("a batch on free identifiers was refused: %v", err)
	}
}

// testNothingBeforeItsOwnVerdict fails if, for any failed batch attempt, an attempt carrying one of
// its participants was submitted before the verdict for that attempt's participants was applied.
// Attempts of other subscriptions may interleave freely.
func testNothingBeforeItsOwnVerdict(t *testing.T, evs []testBatchEvent) {
	t.Helper()
	key := func(n []string) string { return strings.Join(n, ",") }
	for i, e := range evs {
		if !e.attempt || e.err == nil {
			continue
		}
		parts := map[string]bool{}
		for _, n := range e.nonces {
			parts[n] = true
		}
		for j := i + 1; j < len(evs); j++ {
			f := evs[j]
			if !f.attempt && key(f.nonces) == key(e.nonces) {
				break
			}
			if f.attempt {
				for _, n := range f.nonces {
					if parts[n] {
						t.Fatalf("participant %q was submitted again (event %d) before attempt %d's verdict", n, j, i)
					}
				}
			}
		}
	}
}

// TestAdmissionBatchCompetingBatchesOfOneSession: two subscriptions of one session admit their
// backlogs concurrently, both under the connection's entitlement, and their first batches are held
// at the store until both are there, so they race on the session's authority and ledger. The loser
// is settled before it is attempted again, every message reaches the client once and in each
// topic's order, and no custody reservation outlives the run.
func TestAdmissionBatchCompetingBatchesOfOneSession(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	const id = "ab-compete"
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "ab/x", qos: 1}, {filter: "ab/y", qos: 1}}, []byte{1, 1})
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTGetClient(t, s, id).mqtt.sess
	c.Close()
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != nil {
			return fmt.Errorf("still attached")
		}
		return nil
	})
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { pc.Close() })
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	const n = 16
	for i := 0; i < n; i++ {
		testMQTTPublish(t, pc, pr, 1, false, false, "ab/x", uint16(2*i+1), []byte(fmt.Sprintf("x%d", i)))
		testMQTTPublish(t, pc, pr, 1, false, false, "ab/y", uint16(2*i+2), []byte(fmt.Sprintf("y%d", i)))
	}
	testCountContentionWaits(t, sess)
	mu, evs := testRecordBatchOrder(t, nil)
	// THE BARRIER: the first two batches to reach the store wait for each other, then both send.
	var arrived atomic.Int32
	both := make(chan struct{})
	var bothOnce sync.Once
	var stale atomic.Int32
	testPresendFor(t, sess, "op-admit-publishes", _EMPTY_, func(*mqttAuthorityTransition) {
		k := arrived.Add(1)
		if k > 2 {
			return
		}
		if k == 2 {
			bothOnce.Do(func() { close(both) })
		}
		select {
		case <-both:
		case <-time.After(10 * time.Second):
		}
	})
	mqttTestAdmissionBatchAttemptHook.Set(func(_ *mqttSession, nonces []string, err error) {
		mu.Lock()
		*evs = append(*evs, testBatchEvent{attempt: true, nonces: append([]string(nil), nonces...), err: err})
		mu.Unlock()
		if mqttIsStaleExpectation(err) {
			stale.Add(1)
		}
	})
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	next := map[string]int{"x": 0, "y": 0}
	for next["x"] < n || next["y"] < n {
		_, pi, topic, payload := testMQTTGetPubMsgEx(t, c2, r2, _EMPTY_, nil)
		k := topic[len(topic)-1:]
		if want := fmt.Sprintf("%s%d", k, next[k]); string(payload) != want {
			t.Fatalf("on %s received %s, want %s", topic, payload, want)
		}
		next[k]++
		testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
	}
	if arrived.Load() < 2 {
		t.Fatalf("fixture: only %d batch(es) reached the store", arrived.Load())
	}
	if stale.Load() == 0 {
		t.Fatalf("fixture: the two held batches did not contend")
	}
	mu.Lock()
	got := append([]testBatchEvent(nil), (*evs)...)
	mu.Unlock()
	testNothingBeforeItsOwnVerdict(t, got)
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if k := len(sess.custody.heldAdmissions); k != 0 {
			return fmt.Errorf("%d admission reservations remain", k)
		}
		if k := sess.receiveMaxOccupancyLocked(); k != 0 {
			return fmt.Errorf("%d Receive Maximum charges remain", k)
		}
		return nil
	})
}

// TestAdmissionBatchReceiveMaximumWithholdsAndReleases: under Receive Maximum 2 a backlog of five is
// admitted two at a time; the rest is withheld, re-offered as capacity is released, and every
// message arrives once; nothing stays charged afterwards.
func TestAdmissionBatchReceiveMaximumWithholdsAndReleases(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 2 * time.Second
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	ci := &mqttV5ConnInfo{clientID: "ab-rmax", cleanStart: false, props: mqttV5PropsSessionExpiryReceiveMax(300, 2)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ab/t", opts: 1}})
	sess := testMQTTGetClient(t, s, "ab-rmax").mqtt.sess
	c.Close()
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != nil {
			return fmt.Errorf("still attached")
		}
		return nil
	})
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ab-rmax-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { cp.Close() })
	testMQTTReadConnAckV5(t, rp)
	const n = 5
	for i := 0; i < n; i++ {
		testMQTTPubV5Props(t, cp, rp, 1, false, uint16(i+1), "ab/t", []byte(fmt.Sprintf("m%d", i)), nil)
	}
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	testMQTTReadConnAckV5(t, r2)
	// Two arrive; nothing more until one is answered.
	var held []uint16
	seen := map[string]int{}
	for i := 0; i < 2; i++ {
		payload, pi, _ := testBatchReadPublishV5(t, r2)
		seen[string(payload)]++
		held = append(held, pi)
	}
	if _, _, ok := testMQTTReadPacketReady(r2, 500*time.Millisecond); ok {
		t.Fatalf("a third PUBLISH was sent under Receive Maximum 2")
	}
	for _, pi := range held {
		testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
	}
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < n && time.Now().Before(deadline) {
		payload, pi, _ := testBatchReadPublishV5(t, r2)
		seen[string(payload)]++
		testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
	}
	for i := 0; i < n; i++ {
		if k := seen[fmt.Sprintf("m%d", i)]; k != 1 {
			t.Fatalf("m%d arrived %d times", i, k)
		}
	}
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if k := len(sess.custody.heldAdmissions); k != 0 {
			return fmt.Errorf("%d admission reservations remain", k)
		}
		if k := sess.receiveMaxOccupancyLocked(); k != 0 {
			return fmt.Errorf("%d Receive Maximum charges remain", k)
		}
		return nil
	})
}

// TestAdmissionBatchMixedQoSBatchTakesEachObligationsHolds: a QoS 2 subscription receiving QoS 1
// and QoS 2 messages commits batches mixing both: every participant holds the source consumer,
// only the QoS 2 ones hold the PUBREL consumer, and every exchange completes.
func TestAdmissionBatchMixedQoSBatchTakesEachObligationsHolds(t *testing.T) {
	rig := newTestBatchRig(t, "ab-mixed", 2, nil)
	sess := rig.sess()
	var mixed atomic.Int32
	var bad atomic.Value
	mqttTestAdmissionBatchSettledHook.Set(func(s *mqttSession, nonces []string, v mqttTransitionVerdict) {
		if s != sess || len(nonces) < 2 || v != mqttTransitionLanded {
			return
		}
		s.mu.Lock()
		gen, epoch := s.gen, s.asm.sessStreamEpoch
		s.mu.Unlock()
		q := map[byte]int{}
		for _, nn := range nonces {
			x, _, err := s.jsa.loadOutboundExchange(s.id, epoch, gen, nn)
			if err != nil || x == nil {
				bad.Store(fmt.Sprintf("exchange %s unreadable: %v", nn, err))
				return
			}
			q[x.QoS]++
			src, rel := false, false
			for _, h := range x.Holds {
				src = src || h.Role == mqttRoleSourceReplay
				rel = rel || h.Role == mqttRoleCanonicalPubRel
			}
			if !src || rel != (x.QoS == 2) {
				bad.Store(fmt.Sprintf("QoS %d exchange %s holds %+v", x.QoS, nn, x.Holds))
			}
		}
		if q[1] > 0 && q[2] > 0 {
			mixed.Add(1)
		}
	})
	t.Cleanup(func() { mqttTestAdmissionBatchSettledHook.Set(nil) })
	rig.detach()
	const n = 20
	for i := 0; i < n; i++ {
		testMQTTPublish(t, rig.pc, rig.pr, byte(1+i%2), false, false, "ab/t", uint16(i+1), []byte(fmt.Sprintf("m%d", i)))
	}
	rig.connectSubscriber(true)
	seen := map[string]bool{}
	owed := map[uint16]bool{}
	deadline := time.Now().Add(30 * time.Second)
	for (len(seen) < n || len(owed) > 0) && time.Now().Before(deadline) {
		b, pl := testMQTTReadPacket(t, rig.sr)
		switch b & mqttPacketMask {
		case mqttPacketPub:
			flags, pi, _, payload := testMQTTGetPubMsgExEx(t, nil, rig.sr, b, pl, "ab/t", nil)
			seen[string(payload)] = true
			if mqttGetQoS(flags) == 2 {
				owed[pi] = true
				testMQTTSendPIPacket(mqttPacketPubRec, t, rig.sc, pi)
			} else {
				testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
			}
		case mqttPacketPubRel:
			pi, err := rig.sr.readUint16("pi")
			if err != nil {
				t.Fatal(err)
			}
			delete(owed, pi)
			testMQTTSendPIPacket(mqttPacketPubComp, t, rig.sc, pi)
		default:
			t.Fatalf("unexpected packet %x", b)
		}
	}
	if len(seen) < n || len(owed) > 0 {
		t.Fatalf("%d of %d delivered, %d QoS 2 exchanges unfinished", len(seen), n, len(owed))
	}
	if v, _ := bad.Load().(string); v != "" {
		t.Fatal(v)
	}
	if mixed.Load() == 0 {
		t.Fatalf("fixture: no committed batch mixed QoS 1 and QoS 2 deliveries")
	}
}
