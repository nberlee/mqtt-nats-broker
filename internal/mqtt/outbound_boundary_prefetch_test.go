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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The write boundary's prefetch (outbound_boundary_prefetch.go): one pipelined request for the
// records the comparisons re-read, the first read of each answered from it.

// testPrefetchGetSubject is the subject a message GET request names, or empty.
func testPrefetchGetSubject(ev mqttJSARequestEvent) string {
	if ev.Kind != mqttJSARequestSent || !strings.Contains(ev.Subject, ".MSG.GET.") {
		return _EMPTY_
	}
	var req JSApiMsgGetRequest
	if json.Unmarshal(ev.Body, &req) != nil {
		return _EMPTY_
	}
	return req.LastFor
}

// testPrefetchGets counts the message GETs jsa sends, by the subject each names.
func testPrefetchGets(t *testing.T, jsa *mqttJSA) func() map[string]int {
	t.Helper()
	var mu sync.Mutex
	got := map[string]int{}
	f := func(ev mqttJSARequestEvent) {
		if s := testPrefetchGetSubject(ev); s != _EMPTY_ {
			mu.Lock()
			got[s]++
			mu.Unlock()
		}
	}
	jsa.testObserveRequest.Store(&f)
	t.Cleanup(func() { jsa.testObserveRequest.Store(nil) })
	return func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string]int, len(got))
		for k, v := range got {
			out[k] = v
		}
		return out
	}
}

func testPrefetchStore(t *testing.T, jsa *mqttJSA, subject string, body []byte) uint64 {
	t.Helper()
	ack, err := jsa.storeMsg(subject, 0, body)
	if err != nil {
		t.Fatalf("storing %q: %v", subject, err)
	}
	return ack.Sequence
}

// TestBoundaryPrefetchSetIsWhatTheComparisonsReRead: every member carried at a nonzero
// expectation and the exchange each action names, deduplicated, in member order, and nothing on
// another stream; a create-only member is not in it.
func TestBoundaryPrefetchSetIsWhatTheComparisonsReRead(t *testing.T) {
	const id = "prefetch-set"
	h := getHash(id)
	g1 := mqttNewSessGen()
	reg := mqttInflightSubject(h, g1, 3)
	marker := mqttInflightMarkerSubject(h, g1, 3)
	created := mqttInflightSubject(h, g1, 4)
	act := mqttActionSubject(h, g1, "nonceA", 1)
	xch := mqttExchangeSubject(h, g1, "nonceA")
	other := mqttActionSubject(h, g1, "nonceB", 2)
	otherXch := mqttExchangeSubject(h, g1, "nonceB")
	extra := []mqttTransitionMember{
		{subj: reg, expect: 7},
		{subj: created, expect: 0},
		{subj: act, expect: 0},
		{subj: marker, expect: 9},
		{subj: xch, expect: 11},
		{subj: other, expect: 0},
		{subj: "$MQTT.sess.elsewhere", expect: 5},
	}
	got := mqttBoundaryPrefetchSet(id, extra)
	want := []string{reg, xch, marker, otherXch}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("prefetch set\n got %v\nwant %v", got, want)
	}
	if one := mqttBoundaryPrefetchSet(id, extra[:1]); len(one) != 1 {
		t.Fatalf("a one-member transition's set is %v", one)
	}
}

// TestBoundaryPrefetchAnswersEachSubjectOnce: the first read of a prefetched subject is answered
// from the prefetch without a request; the next read reaches the store and sees a write that
// landed after the prefetch (the inventory walk's re-read of the ledger depends on it).
func TestBoundaryPrefetchAnswersEachSubjectOnce(t *testing.T) {
	e := testNewInflightEnv(t, "prefetch-once")
	jsa := &e.asm.jsa
	g1 := mqttNewSessGen()
	a := mqttInflightSubject(e.sess.idHash, g1, 1)
	b := mqttInflightSubject(e.sess.idHash, g1, 2)
	seqA := testPrefetchStore(t, jsa, a, []byte(`"v1"`))
	seqB := testPrefetchStore(t, jsa, b, []byte(`"v1"`))
	view := jsa.withBoundaryPrefetch(e.sess.id, []mqttTransitionMember{{subj: a, expect: seqA}, {subj: b, expect: seqB}})
	if view == jsa {
		t.Fatalf("a two-subject set installed no view")
	}
	seqA2 := testPrefetchStore(t, jsa, a, []byte(`"v2"`))
	gets := testPrefetchGets(t, jsa)

	first, err := view.loadLastMsgFor(mqttInflightStreamName, a)
	if err != nil || first == nil || first.Sequence != seqA {
		t.Fatalf("the first read was not the prefetched record: %v %v", first, err)
	}
	if n := gets()[a]; n != 0 {
		t.Fatalf("the first read of a prefetched subject sent %d request(s)", n)
	}
	second, err := view.loadLastMsgFor(mqttInflightStreamName, a)
	if err != nil || second == nil || second.Sequence != seqA2 {
		t.Fatalf("the second read did not reach the store: got %v (want sequence %d) %v", second, seqA2, err)
	}
	if n := gets()[a]; n != 1 {
		t.Fatalf("the second read sent %d request(s), want 1", n)
	}
}

// TestBoundaryPrefetchAnswersAsTheStoreWould: a prefetched absence and a prefetched unreadable
// record are answered exactly as the direct reads answer them, so every refusal built on them is
// unchanged.
func TestBoundaryPrefetchAnswersAsTheStoreWould(t *testing.T) {
	e := testNewInflightEnv(t, "prefetch-same")
	jsa := &e.asm.jsa
	g1 := mqttNewSessGen()
	absent := mqttInflightSubject(e.sess.idHash, g1, 5)
	bad := mqttInflightSubject(e.sess.idHash, g1, 6)
	badSeq := testPrefetchStore(t, jsa, bad, []byte("not a register"))

	dMsg, dErr := jsa.loadLastMsgFor(mqttInflightStreamName, absent)
	dRec, dSmsg, dRErr := jsa.loadInflightRecord(bad)

	view := jsa.withBoundaryPrefetch(e.sess.id, []mqttTransitionMember{{subj: absent, expect: 3}, {subj: bad, expect: badSeq}})
	if view == jsa {
		t.Fatalf("no view installed")
	}
	vMsg, vErr := view.loadLastMsgFor(mqttInflightStreamName, absent)
	if (vMsg == nil) != (dMsg == nil) || fmt.Sprint(vErr) != fmt.Sprint(dErr) || vErr == nil {
		t.Fatalf("absent subject: view (%v, %v), store (%v, %v)", vMsg, vErr, dMsg, dErr)
	}
	vRec, vSmsg, vRErr := view.loadInflightRecord(bad)
	if vRec != nil || dRec != nil || vSmsg == nil || dSmsg == nil || vSmsg.Sequence != dSmsg.Sequence || vRErr != nil || dRErr != nil {
		t.Fatalf("unreadable record: view (%v, %v, %v), store (%v, %v, %v)", vRec, vSmsg, vRErr, dRec, dSmsg, dRErr)
	}
}

// TestBoundaryPrefetchKeepsAPartialBatch: a request timeout with some replies received keeps the
// answers it got (their first reads send nothing) and reads the missing subject from the store; a
// prefetch that got no reply at all installs no view.
func TestBoundaryPrefetchKeepsAPartialBatch(t *testing.T) {
	e := testNewInflightEnv(t, "prefetch-partial")
	jsa := &e.asm.jsa
	g1 := mqttNewSessGen()
	got := mqttInflightSubject(e.sess.idHash, g1, 7)
	lost := mqttInflightSubject(e.sess.idHash, g1, 8)
	gotSeq := testPrefetchStore(t, jsa, got, []byte(`"kept"`))
	lostSeq := testPrefetchStore(t, jsa, lost, []byte(`"read later"`))

	var dropping atomic.Bool
	dropping.Store(true)
	drop := func(subject string, body []byte) bool {
		return dropping.Load() && strings.Contains(subject, ".MSG.GET.") && strings.Contains(string(body), lost)
	}
	jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() { jsa.testDropRequest.Store(nil) })
	short := *jsa
	short.deadline = time.Now().Add(500 * time.Millisecond)
	view := short.withBoundaryPrefetch(e.sess.id, []mqttTransitionMember{{subj: got, expect: gotSeq}, {subj: lost, expect: lostSeq}})
	if view == &short {
		t.Fatalf("a partial batch installed no view")
	}
	dropping.Store(false)
	view.deadline = time.Time{}
	gets := testPrefetchGets(t, jsa)
	if m, err := view.loadLastMsgFor(mqttInflightStreamName, got); err != nil || m == nil || m.Sequence != gotSeq {
		t.Fatalf("the received reply was not kept: %v %v", m, err)
	}
	if m, err := view.loadLastMsgFor(mqttInflightStreamName, lost); err != nil || m == nil || m.Sequence != lostSeq {
		t.Fatalf("the missing subject was not read from the store: %v %v", m, err)
	}
	if g := gets(); g[got] != 0 || g[lost] != 1 {
		t.Fatalf("requests after the partial batch: kept %d (want 0), missing %d (want 1)", g[got], g[lost])
	}

	dropping.Store(true)
	none := *jsa
	none.deadline = time.Now().Add(300 * time.Millisecond)
	drop2 := func(subject string, body []byte) bool { return strings.Contains(subject, ".MSG.GET.") }
	jsa.testDropRequest.Store(&drop2)
	if v := none.withBoundaryPrefetch(e.sess.id, []mqttTransitionMember{{subj: got, expect: gotSeq}, {subj: lost, expect: lostSeq}}); v != &none {
		t.Fatalf("a prefetch with no reply installed a view")
	}
}

// TestBoundaryPrefetchStillMeetsTheReadFaultSeam: an injected read failure reaches a subject the
// view would have answered. Serial: the seam is package-wide.
func TestBoundaryPrefetchStillMeetsTheReadFaultSeam(t *testing.T) {
	e := testNewInflightEnv(t, "prefetch-seam")
	jsa := &e.asm.jsa
	g1 := mqttNewSessGen()
	a := mqttInflightSubject(e.sess.idHash, g1, 9)
	b := mqttInflightSubject(e.sess.idHash, g1, 10)
	seqA := testPrefetchStore(t, jsa, a, []byte(`"a"`))
	seqB := testPrefetchStore(t, jsa, b, []byte(`"b"`))
	view := jsa.withBoundaryPrefetch(e.sess.id, []mqttTransitionMember{{subj: a, expect: seqA}, {subj: b, expect: seqB}})
	injected := errors.New("injected read failure")
	h := func(stream, subject string) error {
		if subject == a {
			return injected
		}
		return nil
	}
	mqttTestLoadLastMsgHookV.Store(&h)
	t.Cleanup(func() { mqttTestLoadLastMsgHookV.Store(nil) })
	if _, err := view.loadLastMsgFor(mqttInflightStreamName, a); !errors.Is(err, injected) {
		t.Fatalf("the fault seam did not reach a prefetched subject: %v", err)
	}
}

// TestBoundaryPrefetchAcquisitionReadsTogether: a batched acquisition's re-reads (each
// participant's register, each action's exchange) are all sent while every one of their replies is
// held, so none of them waits for another, and none is read a second time by its comparisons.
func TestBoundaryPrefetchAcquisitionReadsTogether(t *testing.T) {
	rig := newTestBatchRig(t, "prefetch-acq", 1, nil)
	sess := rig.sess()
	o := testObserveJSA(t, sess.jsa)
	var done atomic.Bool
	var mu sync.Mutex
	var failures []string
	fail := func(format string, args ...any) {
		mu.Lock()
		failures = append(failures, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt == nil || at.Attempt.ID != sess.id || at.Attempt.Kind != mqttTransitionAcquireAction || done.Load() {
			return send()
		}
		// The expectation is derived here, from the members, not from the set under test: each
		// replaced register and each exchange an action is acquired for.
		want := map[string]bool{}
		for _, m := range at.Extra {
			if _, err := mqttParseInflightSubject(m.subj); err == nil && m.expect != 0 {
				want[m.subj] = true
			}
			if cls, ok := mqttF105ObligationClass(m.subj); ok && cls == mqttActionClassTk {
				toks := strings.Split(m.subj[len(mqttF105SubjectPrefix()):], ".")
				want[mqttExchangeSubject(getHash(at.Attempt.ID), toks[2], toks[3])] = true
			}
		}
		if len(want) < 4 || !done.CompareAndSwap(false, true) {
			return send()
		}
		isWanted := func(s string) bool { return want[s] }
		h := o.hold(isWanted)
		m := o.mark()
		type result struct {
			seq uint64
			err error
		}
		out := make(chan result, 1)
		go func() {
			seq, err := send()
			out <- result{seq, err}
		}()
		if !o.waitSends(m, len(want), isWanted, 10*time.Second) {
			fail("only %d of the %d re-reads were sent while their replies were held: the reads wait for each other",
				o.sends(m, isWanted), len(want))
		}
		o.release(h, nil)
		r := <-out
		per := map[string]int{}
		for _, s := range o.sentSubjects(m, isWanted) {
			per[s]++
		}
		for s := range want {
			if per[s] != 1 {
				fail("%q was requested %d time(s) in the commit (want 1)", s, per[s])
			}
		}
		return r.seq, r.err
	}
	sess.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { sess.jsa.testTransitionCommit.Store(nil) })

	const n = 24
	rig.backlog(n)
	rig.readInOrder(n)
	if !done.Load() {
		t.Fatalf("no batched acquisition of two or more participants was observed")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, f := range failures {
		t.Error(f)
	}
}

// TestBoundaryPrefetchRacingWriteIsRefusedBySubmission: a register rewritten after an acquisition
// prefetched it, before its batch is sent, is refused by the batch's own expectation, never
// committed over; the flush re-attempts and every delivery arrives exactly once.
func TestBoundaryPrefetchRacingWriteIsRefusedBySubmission(t *testing.T) {
	rig := newTestBatchRig(t, "prefetch-race", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var raced atomic.Bool
	presend := func(at *mqttAuthorityTransition) {
		if at.Attempt == nil || at.Attempt.ID != sess.id || at.Attempt.Kind != mqttTransitionAcquireAction {
			return
		}
		if len(mqttBoundaryPrefetchSet(at.Attempt.ID, at.Extra)) < 4 || !raced.CompareAndSwap(false, true) {
			return
		}
		for _, m := range at.Extra {
			if _, err := mqttParseInflightSubject(m.subj); err != nil || m.expect == 0 {
				continue
			}
			cur, err := sess.jsa.loadLastMsgFor(mqttInflightStreamName, m.subj)
			if err != nil || cur == nil {
				rec.fail("reading %q to race it: %v", m.subj, err)
				return
			}
			// The same body, rewritten: the register's sequence moves, its content does not.
			if _, err := sess.jsa.storeMsg(m.subj, 0, cur.Data); err != nil {
				rec.fail("racing write on %q: %v", m.subj, err)
			}
			return
		}
	}
	sess.jsa.testTransitionPresend.Store(&presend)
	t.Cleanup(func() { sess.jsa.testTransitionPresend.Store(nil) })

	const n = 24
	rig.backlog(n)
	rig.readInOrder(n)
	if _, _, ok := testMQTTReadPacketReady(rig.sr, 300*time.Millisecond); ok {
		t.Fatalf("a packet followed the %d deliveries: a delivery was sent twice", n)
	}
	rec.check(t)
	if !raced.Load() {
		t.Fatalf("no batched acquisition reached the racing write")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	refused := false
	for _, a := range rec.attempts {
		if len(a.pis) > 1 && a.v == mqttTransitionNeverLanded {
			refused = true
		}
	}
	if !refused {
		t.Fatalf("the raced batch was not refused at submission: attempts %+v", rec.attempts)
	}
}
