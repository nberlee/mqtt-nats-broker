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

//go:build mqttport_fulltests

package mqtt

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// A long-lived connection keeps no execution history per transmission (F-105 integrated gate, round
// 1): the consume-once fact lives on each acquisition's holder, so once every exchange a connection
// carried has ended, nothing reachable from the connection and its session grows with how many it
// carried. The schedules that end an entered PUBREL unwritten (S6, S8) repeat in a sibling test on the
// same harness: since F-116 an attempt whose frame was provably not emitted gives its PUBCOMP unit back,
// so each such exchange ends with the one PUBCOMP its written retransmission is owed. A sibling rather
// than two more schedules here only for the run's length: every exchange costs the store a little more
// than the last, so one connection carrying all eight takes about as long as the two tests together
// and fits no shard's budget. Each is also its own single-shot test below.

const (
	testLongLivedPhase1 = 10
	testLongLivedPhase2 = 100
	// testRetainedContainersBudget bounds the oracle's walk: past it the walk fails rather than
	// reporting a partial census.
	testRetainedContainersBudget = 10000
)

// testLongLived is the one connection and session every exchange runs on.
type testLongLived struct {
	e    *testInflightEnv
	tc   *testFrameConn
	gen  string
	sseq uint64
	// goBase is the highest goroutine id before the fixture existed: only goroutines created after
	// it can be continuations of the driven calls. Earlier ones belong to other tests of the same
	// binary, such as parallel tests parked at t.Parallel while this one runs.
	goBase int
}

// nextSeq is a fresh source sequence: every exchange is its own delivery of the source.
func (l *testLongLived) nextSeq() uint64 {
	l.sseq++
	return l.sseq
}

// testLongLivedMaxPacketSize sets the connection's negotiated Maximum Packet Size under the
// connection lock. Scoped by the caller, which restores the previous value.
func testLongLivedMaxPacketSize(c *client, mp uint32) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev := c.mqtt.maxPacketSize
	c.mqtt.maxPacketSize = mp
	return prev
}

// awaitEnded waits on the exchange's own completion: its register gone from the store, no PUBLISH
// or PUBREL entry left for its identifier, and NO send hold at all for it.
func (l *testLongLived) awaitEnded(t *testing.T, pi uint16, subject, what string) {
	t.Helper()
	ended := func() (bool, string) {
		cur, smsg, err := l.e.asm.jsa.loadInflightRecord(subject)
		l.e.sess.mu.Lock()
		defer l.e.sess.mu.Unlock()
		pub, rel, hold := l.e.sess.pendingPublish[pi], l.e.sess.pendingPubRel[pi], l.e.sess.sendsInFlight[pi]
		if err == nil && smsg == nil && pub == nil && rel == nil && hold == nil {
			return true, _EMPTY_
		}
		return false, fmt.Sprintf("register=%+v (present=%v, err=%v) publish=%+v pubrel=%+v hold=%+v",
			cur, smsg != nil, err, pub, rel, hold)
	}
	deadline := time.Now().Add(testRow2Timeout)
	for {
		ok, state := ended()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the exchange on %d did not end: %s", what, pi, state)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// requireCell asserts the acquisition (gen, lin, acq) reads want on the connection's cells.
func (l *testLongLived) requireCell(t *testing.T, what, lin string, acq uint64, want mqttFrameState) {
	t.Helper()
	if st, ok := testRow2GuardState(l.e.c, l.gen, lin, acq); !ok || st != want {
		t.Fatalf("%s: acquisition %s/%s/%d reads %v (present=%v), want %v", what, l.gen, lin, acq, st, ok, want)
	}
}

// publishSection opens an ordinary send section on pi the way the delivery callback does.
func (l *testLongLived) publishSection(t *testing.T, pi uint16, sseq uint64) mqttSendToken {
	t.Helper()
	l.e.sess.mu.Lock()
	tok, ok := l.e.sess.beginSendLocked(l.e.c, pi, testRealPubRelSrcDur, sseq)
	l.e.sess.mu.Unlock()
	if !ok || tok.out == nil {
		t.Fatalf("the send section for %d did not open with its output object: ok=%v", pi, ok)
	}
	if !l.e.sess.acquirePublishAction(l.e.c, pi, &tok) {
		t.Fatalf("the ordinary acquisition for %d did not land", pi)
	}
	if tok.out.action == nil || tok.out.action.Kind != mqttActionPublish {
		t.Fatalf("the landed acquisition is not the section's output: %+v", tok.out)
	}
	return tok
}

// s3s1 is ONE QoS 1 exchange carrying S3 then S1: a section acquired and ended UNWRITTEN (its
// drain records the acquisition ABANDONED), then a section that executes the next acquisition,
// and the client's PUBACK ends the exchange. They share an exchange so that S3's delivery is
// still completed: an abandoned section leaves the delivery owed.
func (l *testLongLived) s3s1(t *testing.T, first bool) {
	t.Helper()
	e := l.e
	sseq := l.nextSeq()
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, sseq, 1)
	subject := mqttInflightSubject(e.sess.idHash, l.gen, pi)

	// S3: acquired, never written; its end drains it.
	before := len(l.tc.written)
	tok := l.publishSection(t, pi, sseq)
	lin, acq3 := tok.out.lin, tok.out.acq
	if !e.c.srv.admitPublishSend() {
		t.Fatal("S3: the publish-send barrier refused a running server")
	}
	e.sess.endPublishSend(e.c, pi, tok, mqttIntentNone, false)
	e.c.srv.publishSendDone()
	if first {
		l.requireCell(t, "S3 (unwritten section drained)", lin, acq3, mqttFrameAbandoned)
		if len(l.tc.written) != before {
			t.Fatalf("S3: an unwritten section put %x on the wire", l.tc.written[before:])
		}
	}

	// S1: executed, as the delivery callback's section does it.
	tok = l.publishSection(t, pi, sseq)
	acq1 := tok.out.acq
	if acq1 != acq3+1 {
		t.Fatalf("S1: the retransmission acquired counter %d, want %d", acq1, acq3+1)
	}
	if !e.c.srv.admitPublishSend() {
		t.Fatal("S1: the publish-send barrier refused a running server")
	}
	intent, wrote := mqttIntentNone, false
	func() {
		defer e.c.srv.publishSendDone()
		defer func() { e.sess.endPublishSend(e.c, pi, tok, intent, wrote) }()
		prep, ok := e.c.mqttPreparePublishMsgTo(e.c, pi, 1, tok.dup, false, 0, []byte("foo"), []byte("payload"), nil)
		if !ok {
			t.Fatal("S1: the PUBLISH was not admitted for framing")
		}
		intent = e.sess.recordSendIntent(e.c, pi, tok)
		if intent == mqttIntentDefiniteFailure || intent == mqttIntentAmbiguous {
			t.Fatalf("S1: the send intent was not recorded: %v", intent)
		}
		prep.bindSection(tok)
		if !prep.requireAction {
			t.Fatal("S1: an ordinary QoS 1 frame must require its acquisition")
		}
		wrote = e.c.mqttEnqueuePreparedPublishMsgTo(e.c, testRow2Sub(), prep)
		if !wrote {
			t.Fatal("S1: the authorized PUBLISH was not written")
		}
	}()
	if first {
		l.requireCell(t, "S1 (executed PUBLISH)", lin, acq1, mqttFrameFinished)
		if len(l.tc.written) == before {
			t.Fatal("S1: the executed PUBLISH wrote nothing")
		}
	}
	if err := e.c.mqttProcessPubAck(pi); err != nil {
		t.Fatalf("S1: the PUBACK for %d was refused: %v", pi, err)
	}
	l.awaitEnded(t, pi, subject, "S3/S1")
}

// s4s2 is ONE QoS 2 exchange at pubrel_ready carrying S4 then S2: a PUBREL acquired and drained
// before entry (the drain records it ABANDONED), then one acquired, entered, written, relinquished
// and drained, and the client's PUBCOMP ends the exchange.
func (l *testLongLived) s4s2(t *testing.T, first bool) {
	t.Helper()
	e := l.e
	seed := testSeedRealPubRelReadyAt(t, e, l.nextSeq())
	before := len(l.tc.written)

	// S4.
	act, cur := testRow2AcquiredPubRel(t, e, seed)
	e.sess.startActionDrain(e.c, act)
	if first {
		l.requireCell(t, "S4 (PUBREL drained before entry)", cur.Lin, cur.Acq+1, mqttFrameAbandoned)
		if len(l.tc.written) != before {
			t.Fatalf("S4: a PUBREL drained before entry put %x on the wire", l.tc.written[before:])
		}
	}

	// S2, step by step in the delivery callback's own order: the send section, the action bound as
	// the PUBREL entry (so the PUBCOMP can answer the stored record), entry, the authorized write,
	// and the exits in the callback's LIFO order — relinquish, endSend, then the drain.
	act, cur2 := testRow2AcquiredPubRel(t, e, seed)
	func() {
		defer e.sess.startActionDrain(e.c, act)
		e.sess.mu.Lock()
		sendTok, sendOK := e.sess.beginPubRelSendLocked(e.c, seed.pi)
		if !sendOK {
			e.sess.mu.Unlock()
			t.Fatal("S2: the PUBREL send section did not open")
		}
		emitted := false
		defer func() { e.sess.endPubRelSend(e.c, seed.pi, sendTok, emitted) }()
		if !e.sess.bindActionToPubRelLocked(seed.pi, act, seed.reply(1), mqttPubRelAffineRef{}) ||
			!e.sess.pubRelWriteKindLocked(seed.pi, act) {
			e.sess.mu.Unlock()
			t.Fatal("S2: the acquired action did not become the PUBREL entry")
		}
		ticket, ok := e.sess.enterPubRelWriteLocked(act, e.c)
		if ok {
			e.sess.markPubRelEnteredLocked(seed.pi, sendTok)
		}
		e.sess.mu.Unlock()
		if !ok {
			t.Fatal("S2: the live PUBREL holder did not enter its write")
		}
		defer ticket.relinquish()
		emitted = e.c.mqttWriteAuthorizedPubRel(seed.pi, ticket, false) != mqttOutputPrevented
	}()
	if first {
		l.requireCell(t, "S2 (executed PUBREL)", cur2.Lin, cur2.Acq+1, mqttFrameFinished)
		if !bytes.Equal(l.tc.written[before:], testRow2PubRelFrame(seed.pi)) {
			t.Fatalf("S2: the wire carries %x, want one PUBREL", l.tc.written[before:])
		}
	}
	e.c.mqttProcessPubComp(seed.pi)
	l.awaitEnded(t, seed.pi, seed.subject, "S4/S2")
}

// s5 is ONE QoS 2 exchange at pubrel_ready carrying S5: a stored PUBREL delivered through the
// delivery callback executes its acquisition, and the client's PUBCOMP ends the exchange.
func (l *testLongLived) s5(t *testing.T, first bool) {
	t.Helper()
	e := l.e
	seed := testSeedRealPubRelReadyAt(t, e, l.nextSeq())
	before := len(l.tc.written)
	_, cur, _ := testRow2Ref(t, e, seed.subject)
	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelCorrelated(t, e, seed.pi, seed.reply(1), seed.nonce, testRealPubRelSrcDur, seed.sseq)
	})
	if first {
		if writes != 1 {
			t.Fatalf("S5: the delivery entered its write %d times, want once", writes)
		}
		l.requireCell(t, "S5 (executed PUBREL via delivery)", cur.Lin, cur.Acq+1, mqttFrameFinished)
		if !bytes.Equal(l.tc.written[before:], testRow2PubRelFrame(seed.pi)) {
			t.Fatalf("S5: the wire carries %x, want one PUBREL", l.tc.written[before:])
		}
	}
	e.c.mqttProcessPubComp(seed.pi)
	l.awaitEnded(t, seed.pi, seed.subject, "S5")
}

// testRetainedQoS2Replayed replays ONE retained QoS 2 PUBLISH on e's connection through the driver's
// own item routine (materialize, admit, transmit), with the allocation made as the SUBSCRIBE path
// makes it, and returns the exchange's identifier, register subject, lineage and counter.
func testRetainedQoS2Replayed(t *testing.T, e *testInflightEnv, tc *testFrameConn, gen string) (pi uint16,
	subject, lin string, acq uint64) {
	t.Helper()
	sess := e.sess
	ent, ok := e.c.outboundEntitlement()
	if !ok {
		t.Fatal("the connection carries no entitlement to replay under")
	}
	sess.mu.Lock()
	pi = sess.trackPublishRetained(2)
	var nonce string
	if p := sess.pendingPublish[pi]; pi != 0 && p != nil {
		nonce = p.regNonce
	}
	reserved := pi != 0 && sess.beginRetainedSendLocked(e.c, pi)
	sess.mu.Unlock()
	if !reserved || nonce == _EMPTY_ {
		t.Fatalf("no retained QoS 2 allocation (pi=%d nonce=%q reserved=%v)", pi, nonce, reserved)
	}
	f := mqttRetainedFrame{pi: pi, nonce: nonce, qos: 2, ent: ent,
		mat: &mqttRetainedMaterial{Topic: "rt/longlived", QoS: 2, Data: []byte("retained")}}
	r := newMQTTRetainedReplay(&subscription{client: e.c, mqtt: &mqttSub{}})
	before := len(tc.written)
	sess.replayDurableItem(e.c, r, f)
	subject = mqttInflightSubject(sess.idHash, gen, pi)
	pub, _, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || pub == nil || !pub.retainedSource() || pub.State != mqttInflightPublish {
		t.Fatalf("the replayed retained exchange is %+v (%v)", pub, err)
	}
	if len(tc.written) == before {
		t.Fatal("the retained PUBLISH was not written")
	}
	return pi, subject, pub.Lin, pub.Acq
}

// s7 is ONE retained QoS 2 exchange carrying S7: the client's PUBREC emits the PUBREL through
// emitRetainedPubRelOnce, which executes its acquisition, and the client's PUBCOMP ends the exchange.
func (l *testLongLived) s7(t *testing.T, first bool) {
	t.Helper()
	e := l.e
	pi, subject, lin, pubAcq := testRetainedQoS2Replayed(t, e, l.tc, l.gen)
	before := len(l.tc.written)
	if err := e.c.mqttProcessPublishReceived(pi, true, mqttReasonSuccess); err != nil {
		t.Fatalf("S7: the retained PUBREC was refused: %v", err)
	}
	if first {
		l.requireCell(t, "S7 (retained PUBREL executed)", lin, pubAcq+1, mqttFrameFinished)
		if !bytes.Equal(l.tc.written[before:], testRow2PubRelFrame(pi)) {
			t.Fatalf("S7: the wire carries %x, want one PUBREL", l.tc.written[before:])
		}
	}
	e.c.mqttProcessPubComp(pi)
	l.awaitEnded(t, pi, subject, "S7")
}

// iteration runs every repeated execution schedule once.
func (l *testLongLived) iteration(t *testing.T, first bool) {
	t.Helper()
	l.s3s1(t, first)
	l.s4s2(t, first)
	l.s5(t, first)
	l.s7(t, first)
}

// refusedIteration runs every repeated schedule that ends an entered PUBREL unwritten once (F-116).
func (l *testLongLived) refusedIteration(t *testing.T, first bool) {
	t.Helper()
	l.s6(t, first)
	l.s8(t, first)
}

// s6 is ONE QoS 2 exchange at pubrel_ready carrying S6 then S5: a stored PUBREL delivered through
// the delivery callback enters its write and is refused by the client's Maximum Packet Size, then
// its redelivery executes the next acquisition, and the client's ONE PUBCOMP ends the exchange. The
// refused attempt's unit was given back (F-116), so that one PUBCOMP is all the identifier owes.
func (l *testLongLived) s6(t *testing.T, first bool) {
	t.Helper()
	e := l.e
	seed := testSeedRealPubRelReadyAt(t, e, l.nextSeq())
	before := len(l.tc.written)
	prev := testLongLivedMaxPacketSize(e.c, 3)
	testDeliverPubRelCorrelated(t, e, seed.pi, seed.reply(1), seed.nonce, testRealPubRelSrcDur, seed.sseq)
	testLongLivedMaxPacketSize(e.c, prev)
	if len(l.tc.written) != before {
		t.Fatalf("S6: a PUBREL refused for size put %x on the wire", l.tc.written[before:])
	}
	testActionDrained(t, e, seed.pi)
	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelCorrelated(t, e, seed.pi, seed.reply(2), seed.nonce, testRealPubRelSrcDur, seed.sseq)
	})
	if writes != 1 || !bytes.Equal(l.tc.written[before:], testRow2PubRelFrame(seed.pi)) {
		t.Fatalf("S6: the redelivery entered its write %d times and the wire carries %x, want one PUBREL",
			writes, l.tc.written[before:])
	}
	e.c.mqttProcessPubComp(seed.pi)
	l.awaitEnded(t, seed.pi, seed.subject, "S6")
}

// s8 is ONE retained QoS 2 exchange carrying S8 then S7: the client's PUBREC emits the PUBREL, which
// enters its write and is refused by the client's Maximum Packet Size, then a re-emission executes
// the next acquisition, and the client's ONE PUBCOMP ends the exchange (F-116, as in s6).
func (l *testLongLived) s8(t *testing.T, first bool) {
	t.Helper()
	e := l.e
	pi, subject, _, _ := testRetainedQoS2Replayed(t, e, l.tc, l.gen)
	before := len(l.tc.written)
	prev := testLongLivedMaxPacketSize(e.c, 3)
	err := e.c.mqttProcessPublishReceived(pi, true, mqttReasonSuccess)
	testLongLivedMaxPacketSize(e.c, prev)
	if err != nil {
		t.Fatalf("S8: the retained PUBREC was refused: %v", err)
	}
	if len(l.tc.written) != before {
		t.Fatalf("S8: a PUBREL refused for size put %x on the wire", l.tc.written[before:])
	}
	e.sess.mu.Lock()
	var nonce string
	if rel := e.sess.pendingPubRel[pi]; rel != nil {
		nonce = rel.regNonce
	}
	e.sess.mu.Unlock()
	testRow2Eventually(t, testRow2Timeout, "S8: the retained PUBREL re-emitted", func() bool {
		e.sess.emitRetainedPubRelOnce(e.c, pi, nonce, nil)
		return len(l.tc.written) > before
	})
	if !bytes.Equal(l.tc.written[before:], testRow2PubRelFrame(pi)) {
		t.Fatalf("S8: the wire carries %x, want one PUBREL", l.tc.written[before:])
	}
	e.c.mqttProcessPubComp(pi)
	l.awaitEnded(t, pi, subject, "S8")
}

// testRetainedContainers is the census of what the connection and its session retain: the length
// of every map, slice and channel reachable from c's value fields through exactly two pointer edges
// (c.mqtt and c.mqtt.sess), recursing into struct fields held by value. A map or slice element that
// is a pointer to a struct of this package is traversed one level by the same rule, its containers
// summed over the elements under field[*].sub. No other pointer, interface or func is followed.
//
// Read under sess.mu, c.mu and c.wmu, taken in production's order (sess.mu -> c.mu, as
// unsub_drain.go documents; c.mu -> c.wmu, as mqttEnqueuePreparedPublishMsgTo takes them).
func testRetainedContainers(t *testing.T, c *client) map[string]int {
	t.Helper()
	sess := c.mqtt.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wmu.Lock()
	defer c.wmu.Unlock()

	w := &testContainerWalk{t: t, out: map[string]int{}, pkg: reflect.TypeOf(client{}).PkgPath()}
	w.walk("c", reflect.ValueOf(c).Elem(), false)
	return w.out
}

type testContainerWalk struct {
	t       *testing.T
	out     map[string]int
	pkg     string
	visited int
}

// walk records v's containers under path. elem marks a value reached through a container element:
// it follows no pointer at all.
func (w *testContainerWalk) walk(path string, v reflect.Value, elem bool) {
	w.visited++
	if w.visited > testRetainedContainersBudget {
		w.t.Fatalf("the retained-container walk visited more than %d values (at %s)", testRetainedContainersBudget, path)
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			w.walk(path+"."+v.Type().Field(i).Name, v.Field(i), elem)
		}
	case reflect.Array:
		if k := v.Type().Elem().Kind(); k == reflect.Struct || k == reflect.Array {
			for i := 0; i < v.Len(); i++ {
				w.walk(path+"[]", v.Index(i), elem)
			}
		}
	case reflect.Chan:
		if !v.IsNil() {
			w.out[path] += v.Len()
		} else {
			w.out[path] += 0
		}
	case reflect.Map, reflect.Slice:
		n := 0
		if !v.IsNil() {
			n = v.Len()
		}
		w.out[path] += n
		if elem || n == 0 {
			return
		}
		// THE ELEMENTS, one level: a pointer to a struct of this package is dereferenced once
		// and walked by value with no further pointer; an element that is itself held by value
		// (a nested map, slice or channel — cpending's per-durable maps — or a struct) is walked
		// in the same element mode, so a container nested inside another is never invisible.
		et := v.Type().Elem()
		if !w.packageStructPtr(et) && !testContainerByValue(et.Kind()) {
			return
		}
		each := func(ev reflect.Value) {
			if w.packageStructPtr(et) {
				if !ev.IsNil() {
					w.walk(path+"[*]", ev.Elem(), true)
				}
				return
			}
			w.walk(path+"[*]", ev, true)
		}
		if v.Kind() == reflect.Map {
			it := v.MapRange()
			for it.Next() {
				each(it.Value())
			}
			return
		}
		for i := 0; i < n; i++ {
			each(v.Index(i))
		}
	case reflect.Ptr:
		// EXACTLY TWO pointer edges.
		if !elem && !v.IsNil() && (path == "c.mqtt" || path == "c.mqtt.sess") {
			w.walk(path, v.Elem(), false)
		}
	}
}

// testContainerByValue reports a kind whose value an element holds directly and the walk can
// enter without following a pointer.
func testContainerByValue(k reflect.Kind) bool {
	switch k {
	case reflect.Map, reflect.Slice, reflect.Chan, reflect.Struct, reflect.Array:
		return true
	}
	return false
}

func (w *testContainerWalk) packageStructPtr(t reflect.Type) bool {
	return t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct && t.Elem().PkgPath() == w.pkg
}

// testExecCellEnv is one accepted connection over the real store, ready for ordinary PUBLISH and
// PUBREL exchanges, with a recording transport.
func testExecCellEnv(t *testing.T, clientID string) (*testInflightEnv, *testFrameConn) {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, e)
	testInstallPassThroughSeams(e)
	testStampAcceptedConnection(t, e)
	// JetStream's confirmation of a source acknowledgement, and nothing else: every source sequence
	// here names a message that was never stored, which no consumer delivery can confirm, so without
	// the seam each PUBREC waits out the confirmation timeout and retires its source instead. The
	// HandBack gate still runs first (confirmInflightSourceAckFor).
	e.asm.mu.Lock()
	e.asm.testInflightSourceAck = func(string) error { return nil }
	e.asm.mu.Unlock()
	return e, testRow2Wire(e)
}

// TestF105ALongLivedConnectionRetainsNoTransmissionHistory drives complete exchanges of the repeated
// execution schedules (S1, S2, S3, S4, S5, S7) on ONE connection, phase 1 then a further, ten times
// longer phase 2, and requires that every exchange ends with nothing left for its identifier and
// that nothing reachable from the connection and its session is longer after phase 2 than after
// phase 1.
func TestF105ALongLivedConnectionRetainsNoTransmissionHistory(t *testing.T) {
	testLongLivedRun(t, "f105-longlived", (*testLongLived).iteration)
}

// TestF116ALongLivedConnectionEndsRefusedPubRelsWithOnePubComp is the same for S6 and S8, the
// schedules that end an entered PUBREL unwritten and then complete with ONE PUBCOMP: before F-116 each
// refusal left a send hold {no token, no action, pubCompOwed 1}, two per round, 220 after 110.
func TestF116ALongLivedConnectionEndsRefusedPubRelsWithOnePubComp(t *testing.T) {
	testLongLivedRun(t, "f116-longlived", (*testLongLived).refusedIteration)
}

// testLongLivedRun is the two-phase body both long-lived tests share.
func testLongLivedRun(t *testing.T, clientID string, iterate func(*testLongLived, *testing.T, bool)) {
	t.Helper()
	goBase := testLongLivedMaxGoroutineID()
	e, tc := testExecCellEnv(t, clientID)
	l := &testLongLived{e: e, tc: tc, gen: testRow2Gen(e), sseq: 1000, goBase: goBase}

	for i := 0; i < testLongLivedPhase1; i++ {
		iterate(l, t, i == 0)
	}
	l.requireNoLiveHolder(t, "phase 1")
	l.awaitQuiescent(t, "phase 1")
	p1 := testRetainedContainers(t, e.c)

	for i := 0; i < testLongLivedPhase2; i++ {
		iterate(l, t, false)
	}
	l.requireNoLiveHolder(t, "phase 2")
	l.awaitQuiescent(t, "phase 2")
	p2 := testRetainedContainers(t, e.c)

	t.Logf("the census recorded %d paths after phase 1 and %d after phase 2", len(p1), len(p2))
	var grew []string
	for path, n2 := range p2 {
		n1, seen := p1[path]
		if !seen || n2 > n1 {
			grew = append(grew, path)
		}
	}
	sort.Strings(grew)
	for _, path := range grew {
		n1, seen := p1[path]
		t.Errorf("%s grew from %d (present=%v) after phase 1 to %d after phase 2: the connection retains "+
			"something per transmission", path, n1, seen, p2[path])
	}
}

// THE COMPLETION BARRIER. Every driven call has returned; what may still be running is an
// asynchronous continuation one of them started, and each is waited on through its OWN completion
// signal before a sample is taken. The driven calls are those of S1-S8: the PUBLISH section
// (beginSendLocked, acquirePublishAction, recordSendIntent, mqttEnqueuePreparedPublishMsgTo,
// endPublishSend), the PUBREL acquisition, entry, write and drain (mqttAcquirePubRelAction,
// enterPubRelWriteLocked, mqttWriteAuthorizedPubRel, startActionDrain), mqttDeliverPubRelCb, the
// retained replay (replayDurableItem) and its PUBREC emission (emitRetainedPubRel ->
// emitRetainedPubRelOnce), and the PUBACK / PUBREC / PUBCOMP handlers. The continuations they reach:
//
//   - outbound_source_completion.go:459, the terminal retry (time.AfterFunc), armed by
//     holdTerminalSourceLocked (outbound_source_completion.go:151) from the PUBACK/PUBCOMP
//     handlers: done when its key leaves sess.terminalRetries; its source owner leaves
//     sess.terminalSources.
//   - exclusion_reclaim.go:102 / :281, the exclusion reclaim pass, armed by excludeIdentifierLocked
//     (inflight_register.go:8052) when a drained PUBREL action retires without completing the
//     exchange (pubrel_action.go retireActionLocked_): done when sess.exclReclaimArmed is false.
//   - outbound_retained_reclaim.go:247, the retained copy reclaimer (go runRetainedReclaim), woken
//     by the retained exchange's terminal cleanup (pubrel_action.go:2227 via
//     outbound_retained_replay_emit.go:553): done when retainedReclaim is not running and has
//     nothing pending. Its retry goroutine (outbound_retained_replay_emit.go:741) starts only on
//     an unreadable manifest, and the goroutine scan below refuses it.
//   - outbound_custody_session.go:299 / :323, the custody observer (go runCustodyObserver), hinted
//     by the acknowledgement advances (outbound_ack_advance.go:452, :578): done when
//     sess.custodyObserving is false and no hint is left.
//   - retained_terminal_cleanup.go:115 (a terminal slot over a live source),
//     pubrel_action.go:1355 / :1423 / :1822 (action reconcile, owed materialization, slot-only
//     terminal), inflight_register.go:7502 (an unresolved PUBLISH drain), pubrel_intent_resolution.go:55
//     (an unresolved PUBREC intent), pubcomp_completion.go:136 (an owed PUBCOMP completion),
//     outbound_disposal.go:263 (a disposal), outbound_pubrel_rematerialize.go:160 (a missing
//     canonical), outbound_retained_replay.go:236 / :302 / :320 (an owed retained emission, close or
//     abandonment): each is armed only on a failed, refused or unresolved step, and each registers
//     itself before arming — sess.retainedCleanups, s.owedActions, s.owedDrains, s.owedIntents,
//     sess.pubCompOwed, sess.disposals / s.owedDisposals, sess.retainedDriver.owed / .abandons —
//     so the registries reading empty is the evidence none is outstanding.
//   - the endPublishSend / finishActionAndWake wake (pubrel_action.go:3026
//     wakePubRelRedriveIfClear) runs synchronously inside the driven call and acts only on a
//     registered redrive waiter (sess.pubRelRedrive, required empty); the PUBREL consumer has no
//     delivery subscription (testReadyForRealPubRelDelivery), so no NAK it could send re-enters
//     the delivery callback.
//
// Every other `go` / time.AfterFunc site in the package is on a path no driven call reaches —
// CONNECT, takeover, resume and recovery, Will, session expiry, shutdown, the shared group, the
// unsubscribe drain — and the goroutine scan is the backstop: at the sample no goroutine may have
// a frame in this package except the account manager's four standing loops, parked.
func (l *testLongLived) awaitQuiescent(t *testing.T, phase string) {
	t.Helper()
	sess, s := l.e.sess, l.e.s
	busy := func() string {
		sess.mu.Lock()
		var why []string
		for name, n := range map[string]int{
			"sess.terminalRetries": len(sess.terminalRetries), "sess.terminalSources": len(sess.terminalSources),
			"sess.retainedCleanups": len(sess.retainedCleanups), "sess.custodyHints": len(sess.custodyHints),
			"sess.pubRelRedrive": len(sess.pubRelRedrive), "sess.pubCompOwed": len(sess.pubCompOwed),
			"sess.disposals": len(sess.disposals), "sess.srcAckOwed": len(sess.srcAckOwed),
		} {
			if n != 0 {
				why = append(why, fmt.Sprintf("%s=%d", name, n))
			}
		}
		if sess.exclReclaimArmed {
			why = append(why, "sess.exclReclaimArmed")
		}
		if sess.custodyObserving {
			why = append(why, "sess.custodyObserving")
		}
		sess.mu.Unlock()
		sess.retainedDriver.mu.Lock()
		if n := len(sess.retainedDriver.owed) + len(sess.retainedDriver.abandons); n != 0 {
			why = append(why, fmt.Sprintf("sess.retainedDriver owed+abandons=%d", n))
		}
		sess.retainedDriver.mu.Unlock()
		sess.retainedReclaim.mu.Lock()
		if sess.retainedReclaim.running || len(sess.retainedReclaim.pending) != 0 {
			why = append(why, fmt.Sprintf("sess.retainedReclaim running=%v pending=%d",
				sess.retainedReclaim.running, len(sess.retainedReclaim.pending)))
		}
		sess.retainedReclaim.mu.Unlock()
		for name, n := range map[string]func() int{
			"s.owedDrains":    func() int { s.owedDrainsMu.Lock(); defer s.owedDrainsMu.Unlock(); return len(s.owedDrains) },
			"s.owedTerminals": func() int { s.owedTerminalsMu.Lock(); defer s.owedTerminalsMu.Unlock(); return len(s.owedTerminals) },
			"s.owedIntents":   func() int { s.owedIntentsMu.Lock(); defer s.owedIntentsMu.Unlock(); return len(s.owedIntents) },
			"s.owedDisposals": func() int { s.owedDisposalsMu.Lock(); defer s.owedDisposalsMu.Unlock(); return len(s.owedDisposals) },
			"s.owedActions":   func() int { s.owedActionsMu.Lock(); defer s.owedActionsMu.Unlock(); return len(s.owedActions) },
			"s.owedRetained":  func() int { s.owedRetainedMu.Lock(); defer s.owedRetainedMu.Unlock(); return len(s.owedRetained) },
		} {
			if v := n(); v != 0 {
				why = append(why, fmt.Sprintf("%s=%d", name, v))
			}
		}
		why = append(why, testLongLivedForeignGoroutines(l.goBase)...)
		sort.Strings(why)
		return strings.Join(why, "; ")
	}
	deadline := time.Now().Add(5*mqttActionReconcileDelay + testRow2Timeout)
	for {
		why := busy()
		if why == _EMPTY_ {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("before the %s sample, continuations are still outstanding: %s", phase, why)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// testLongLivedStandingLoops are the account manager's own loops, started by the fixture's account
// creation and not by any driven call; each must be parked at its select.
var testLongLivedStandingLoops = []string{
	"(*mqttAccountSessionManager).willClaimLoop(",
	"(*mqttAccountSessionManager).sendJSAPIrequests(",
	"(*mqttAccountSessionManager).cleanupRetainedMessageCache(",
	"(*mqttAccountSessionManager).retainedObserverLoop(",
	"(*Server).mqttCreateAccountSessionManager.func4()",
}

// testLongLivedGoroutineID parses the id from a goroutine dump's header line ("goroutine N [...]:").
func testLongLivedGoroutineID(header string) int {
	var id int
	if _, err := fmt.Sscanf(header, "goroutine %d ", &id); err != nil {
		return 0
	}
	return id
}

// testLongLivedMaxGoroutineID is the highest id among the goroutines alive now.
func testLongLivedMaxGoroutineID() int {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]
	max := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if id := testLongLivedGoroutineID(g); id > max {
			max = id
		}
	}
	return max
}

// testLongLivedForeignGoroutines lists every goroutine created after goBase with a frame in this
// package that is neither the test itself nor a parked standing loop.
func testLongLivedForeignGoroutines(goBase int) []string {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		if testLongLivedGoroutineID(g) <= goBase {
			continue
		}
		if !strings.Contains(g, "internal/mqtt.") || strings.Contains(g, "TestF105ALongLivedConnectionRetainsNoTransmissionHistory") {
			continue
		}
		lines := strings.SplitN(g, "\n", 3)
		if len(lines) < 2 {
			continue
		}
		standing := false
		for _, loop := range testLongLivedStandingLoops {
			if strings.Contains(lines[1], loop) && strings.Contains(lines[0], "[select") {
				standing = true
			}
		}
		if !standing {
			out = append(out, "goroutine "+lines[0]+" "+lines[1])
		}
	}
	return out
}

// requireNoLiveHolder is the lifecycle check: every exchange driven has ended, so no PUBLISH entry,
// no PUBREL entry and no send hold of any kind remains on the session.
func (l *testLongLived) requireNoLiveHolder(t *testing.T, phase string) {
	t.Helper()
	sess := l.e.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if n := len(sess.pendingPublish); n != 0 {
		t.Errorf("after %s %d PUBLISH entries remain: %v", phase, n, sess.pendingPublish)
	}
	if n := len(sess.pendingPubRel); n != 0 {
		t.Errorf("after %s %d PUBREL entries remain", phase, n)
	}
	for pi, h := range sess.sendsInFlight {
		t.Errorf("after %s identifier %d still has a send hold: %+v", phase, pi, h)
	}
}

// testEnteredHolder captures, at the write hook, the identifier's REGISTERED PUBREL holder: read
// under sess.mu, required to be Writing with the expected identifier, lineage and counter, and kept
// by pointer so the test reads that exact holder's cell afterwards. Problems are recorded rather than
// raised, since the hook runs inside the production call.
type testEnteredHolder struct {
	fired int
	held  *mqttSendAction
	bad   []string
}

func (h *testEnteredHolder) capture(sess *mqttSession, pi uint16, lin string, acq uint64, entered *mqttSendAction) {
	h.fired++
	sess.mu.Lock()
	defer sess.mu.Unlock()
	var act *mqttSendAction
	if hold := sess.sendsInFlight[pi]; hold != nil {
		act = hold.action
	}
	switch {
	case act == nil:
		h.bad = append(h.bad, fmt.Sprintf("no holder is registered for %d at the write hook", pi))
		return
	case entered != nil && act != entered:
		h.bad = append(h.bad, "the registered holder is not the one that entered")
	case act.state != mqttSendActionWriting:
		h.bad = append(h.bad, fmt.Sprintf("the registered holder is in state %v, want Writing", act.state))
	case act.slot.name.pi != pi || act.outAction == nil || act.outAction.PI != pi ||
		act.outAction.Nonce != lin || act.outAction.Acq != acq:
		h.bad = append(h.bad, fmt.Sprintf("the registered holder names %d/%+v, want %d/%s/%d",
			act.slot.name.pi, act.outAction, pi, lin, acq))
	}
	h.held = act
}

// requireRefusedAfterEntry is what both single-shot tests assert once the production call returned:
// entry happened once, nothing reached the wire, the EXACT holder's cell is ABANDONED at the
// acquisition's key, its action and token drained on the store, and the attempt's PUBCOMP unit was
// given back (F-116): the refusal is a provably prevented write, so no PUBCOMP is owed for it.
//
// NOT asserted: that the identifier is free. The exchange is still at pubrel_ready and its PUBREL
// is still owed, so the identifier stays the exchange's; s6 and s8 carry it on to its PUBCOMP.
func (h *testEnteredHolder) requireRefusedAfterEntry(t *testing.T, e *testInflightEnv, tc *testFrameConn,
	subject, gen, lin string, acq uint64) {
	t.Helper()
	for _, b := range h.bad {
		t.Error(b)
	}
	if h.fired != 1 || h.held == nil {
		t.Fatalf("the write hook fired %d times (holder captured=%v), want once: entry was not reached",
			h.fired, h.held != nil)
	}
	if len(tc.written) != 0 {
		t.Fatalf("a PUBREL refused for size put %x on the wire", tc.written)
	}
	if st, ok := testRow2CellState(h.held.exec, gen, lin, acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the entered holder's cell reads %v (present=%v) at %s/%s/%d, want ABANDONED", st, ok, gen, lin, acq)
	}
	actionAndTokenDrained := func() bool {
		e.sess.mu.Lock()
		drained := h.held.drained
		e.sess.mu.Unlock()
		cur, smsg, err := e.asm.jsa.loadInflightRecord(subject)
		return drained && err == nil && smsg != nil && cur != nil && cur.Acq == acq &&
			cur.ActionToken == _EMPTY_ && cur.State == mqttInflightPubRelReady
	}
	testRow2Eventually(t, testRow2Timeout, "the refused acquisition's action and token to drain", actionAndTokenDrained)
	pi := h.held.slot.name.pi
	e.sess.mu.Lock()
	var owed int
	if hold := e.sess.sendsInFlight[pi]; hold != nil {
		owed = hold.pubCompOwed
	}
	e.sess.mu.Unlock()
	if owed != 0 {
		t.Fatalf("the size-refused attempt kept %d PUBCOMP unit(s) on %d: nothing was emitted, so none is owed", owed, pi)
	}
}

// S6. An ordinary PUBREL delivered through mqttDeliverPubRelCb enters its write and is refused by
// the client's Maximum Packet Size before consuming its acquisition: relinquish records the exact
// holder's acquisition ABANDONED, nothing is written, and the action drains.
func TestF105AnEnteredPubRelRefusedBySizeIsAbandonedOnItsHolder(t *testing.T) {
	e, tc := testExecCellEnv(t, "f105-s6-size")
	seed := testSeedRealPubRelReady(t, e)
	gen := testRow2Gen(e)
	_, cur, _ := testRow2Ref(t, e, seed.subject)
	lin, acq := cur.Lin, cur.Acq+1
	if n := len(testRow2PubRelFrame(seed.pi)); n <= 3 {
		t.Fatalf("the fixture's limit must be below the %d-byte PUBREL", n)
	}

	var h testEnteredHolder
	restore := setMQTTTestPubRelWriteBlock(func() { h.capture(e.sess, seed.pi, lin, acq, nil) })
	prev := testLongLivedMaxPacketSize(e.c, 3)
	testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
	testLongLivedMaxPacketSize(e.c, prev)
	restore()

	h.requireRefusedAfterEntry(t, e, tc, seed.subject, gen, lin, acq)
}

// S8. A retained exchange's PUBREL, emitted through emitRetainedPubRelOnce by the client's PUBREC,
// enters its write and is refused by the client's Maximum Packet Size before consuming its
// acquisition: relinquish records the exact holder's acquisition ABANDONED, nothing is written, and
// the action drains.
func TestF105AnEnteredRetainedPubRelRefusedBySizeIsAbandonedOnItsHolder(t *testing.T) {
	e, tc := testExecCellEnv(t, "f105-s8-size")
	gen := testRow2Gen(e)
	pi, subject, lin, pubAcq := testRetainedQoS2Replayed(t, e, tc, gen)
	acq := pubAcq + 1
	tc.written = nil // the retained PUBLISH; from here nothing may be written

	var h testEnteredHolder
	hook := func(act *mqttSendAction) { h.capture(e.sess, pi, lin, acq, act) }
	prevHook := mqttTestRetainedPubRelEnteredV.Swap(&hook)
	prev := testLongLivedMaxPacketSize(e.c, 3)
	err := e.c.mqttProcessPublishReceived(pi, true, mqttReasonSuccess)
	testLongLivedMaxPacketSize(e.c, prev)
	mqttTestRetainedPubRelEnteredV.Store(prevHook)
	if err != nil {
		t.Fatalf("the retained PUBREC was refused: %v", err)
	}

	h.requireRefusedAfterEntry(t, e, tc, subject, gen, lin, acq)
}
