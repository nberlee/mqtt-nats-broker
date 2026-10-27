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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The outbound delivery benchmark measures how fast stored QoS 1 and QoS 2 messages reach
// subscribers: a burst is published (each publish acknowledged) before the subscribers read, so
// the whole burst is backlog, and the subscribers then read and acknowledge as fast as they can.
// It is the measurement behind docs/CONFORMANCE.md's per-subscriber delivery ceiling, and a test
// rather than a Go benchmark because the cluster harness is *testing.T-only and because a number
// is reported only for a run whose deliveries checked out.
//
// A row counts only if its oracle holds for every subscriber: every published message delivered,
// the first delivery of each in publish order (MQTT-4.6.0-6, non-shared subscriptions), every
// QoS 2 flow completed. Duplicates are counted and reported, not failed: at-least-once permits
// them, and how many arrive is part of what is measured.
//
// Opt-in: MQTTNB_OUTBOUND_DELIVERY_BENCH=1 (make bench-outbound). MQTTNB_OUTBOUND_DELIVERY_BENCH_OUT
// names a CSV file the rows are appended to, MQTTNB_OUTBOUND_DELIVERY_BENCH_LABEL labels them, and
// MQTTNB_OUTBOUND_DELIVERY_BENCH_CASES=<a,b> selects cases by name.

const (
	outboundDeliveryBenchEnv      = "MQTTNB_OUTBOUND_DELIVERY_BENCH"
	outboundDeliveryBenchOutEnv   = "MQTTNB_OUTBOUND_DELIVERY_BENCH_OUT"
	outboundDeliveryBenchLabelEnv = "MQTTNB_OUTBOUND_DELIVERY_BENCH_LABEL"
	outboundDeliveryBenchCasesEnv = "MQTTNB_OUTBOUND_DELIVERY_BENCH_CASES"

	outboundDeliveryBenchTimeout = 20 * time.Minute
	outboundDeliveryBenchWindow  = 50
)

type outboundDeliveryBenchCase struct {
	name  string
	qos   byte
	subs  int
	burst int
	// idle, when set, first delivers a burst of `burst`, waits `idle`, and then measures only a
	// further `after` messages: delivery to a session that carries unreclaimed history at rest.
	idle  time.Duration
	after int
}

var outboundDeliveryBenchMatrix = []outboundDeliveryBenchCase{
	{name: "qos1_subs1_burst500", qos: 1, subs: 1, burst: 500},
	{name: "qos2_subs1_burst200", qos: 2, subs: 1, burst: 200},
	{name: "qos1_subs4_burst200", qos: 1, subs: 4, burst: 200},
	{name: "qos1_subs1_idle", qos: 1, subs: 1, burst: 300, idle: 45 * time.Second, after: 20},
}

func TestMQTTOutboundDeliveryThroughput(t *testing.T) {
	if os.Getenv(outboundDeliveryBenchEnv) != "1" {
		t.Skipf("set %s=1 to run the outbound delivery benchmark", outboundDeliveryBenchEnv)
	}
	selected := map[string]bool{}
	for _, c := range strings.Split(os.Getenv(outboundDeliveryBenchCasesEnv), ",") {
		if c = strings.TrimSpace(c); c != _EMPTY_ {
			selected[c] = true
		}
	}
	rigs := []struct {
		name  string
		start func(t *testing.T) inboundAckBenchRig
	}{
		{"standalone", inboundAckBenchStandalone},
		{"r3", inboundAckBenchR3},
	}
	for _, rig := range rigs {
		for _, bc := range outboundDeliveryBenchMatrix {
			if len(selected) > 0 && !selected[bc.name] {
				continue
			}
			t.Run(rig.name+"/"+bc.name, func(t *testing.T) {
				r := rig.start(t)
				res := runOutboundDeliveryBenchCase(t, r, bc)
				outboundDeliveryBenchReport(t, rig.name, bc, res)
			})
		}
	}
}

// outboundDeliveryBenchSub is one subscriber's connection and what it received. Its reader reads
// only while its gate is open, so a burst is backlog before anything is read.
type outboundDeliveryBenchSub struct {
	id        string
	idHash    string
	c         net.Conn
	br        *bufio.Reader
	mu        sync.Mutex
	gate      chan struct{} // closed: reading; open channel: parked before the next read
	parked    bool
	trace     []string // payloads, in arrival order, duplicates included
	at        []time.Time
	pis       map[uint16]string // QoS 2: the payload each identifier last carried
	completed map[string]bool   // QoS 2: payloads whose PUBREL was answered with PUBCOMP
	firstPI   map[string]uint16 // the identifier each payload first arrived under
	fresh     int               // repeats under a DIFFERENT identifier: a new delivery of it
	resent    int               // repeats under the same identifier with DUP: a retransmission
	err       error
}

type outboundDeliveryBenchResult struct {
	unique, dup   int
	fresh, resent int
	elapsed       time.Duration
	windows       []float64 // msg/s per outboundDeliveryBenchWindow first deliveries, first subscriber
	gaps          []time.Duration
	requests      map[string]int
	waits         int64
}

func runOutboundDeliveryBenchCase(t *testing.T, r inboundAckBenchRig, bc outboundDeliveryBenchCase) outboundDeliveryBenchResult {
	t.Helper()
	topic := "obench/" + bc.name
	subs := make([]*outboundDeliveryBenchSub, bc.subs)
	for i := range subs {
		id := fmt.Sprintf("obench-%s-%d", bc.name, i)
		c, mr := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: false}, r.host, r.port)
		t.Cleanup(func() { c.Close() })
		testMQTTCheckConnAck(t, mr, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c, mr, []*mqttFilter{{filter: topic, qos: bc.qos}}, []byte{bc.qos})
		testMQTTFlush(t, c, nil, mr)
		subs[i] = &outboundDeliveryBenchSub{id: id, c: c, br: bufio.NewReader(c), gate: make(chan struct{}),
			pis: map[uint16]string{}, completed: map[string]bool{}, firstPI: map[string]uint16{}}
	}
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "obench-pub-" + bc.name, cleanSess: true}, r.host, r.port)
	t.Cleanup(func() { pc.Close() })
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	publish := func(from, n int) {
		for i := from; i < from+n; i++ {
			testMQTTPublish(t, pc, pr, bc.qos, false, false, topic, uint16(i%65535+1), []byte(fmt.Sprintf("m%d", i)))
		}
	}
	for _, s := range subs {
		s.idHash = testMQTTGetClient(t, r.broker, s.id).mqtt.sess.idHash
		go s.read()
	}
	open := func() time.Time {
		at := time.Now()
		for _, s := range subs {
			s.mu.Lock()
			close(s.gate)
			s.mu.Unlock()
		}
		return at
	}
	// park stops every reader before its next read; nothing is in flight when it is called.
	park := func() {
		for _, s := range subs {
			s.mu.Lock()
			s.gate = make(chan struct{})
			s.mu.Unlock()
		}
		deadline := time.Now().Add(10 * time.Second)
		for _, s := range subs {
			for {
				s.mu.Lock()
				parked := s.parked
				s.mu.Unlock()
				if parked {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("subscriber %s did not park", s.id)
				}
				_ = s.c.SetReadDeadline(time.Now())
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
	await := func(n int) {
		deadline := time.Now().Add(outboundDeliveryBenchTimeout)
		for {
			done := true
			for _, s := range subs {
				s.mu.Lock()
				if s.err != nil {
					s.mu.Unlock()
					t.Fatalf("subscriber %s: %v", s.id, s.err)
				}
				if outboundDeliveryBenchUnique(s.trace) < n || (bc.qos == 2 && len(s.completed) < n) {
					done = false
				}
				s.mu.Unlock()
			}
			if done {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("the deliveries did not complete within %v", outboundDeliveryBenchTimeout)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	sess := testMQTTGetClient(t, r.broker, subs[0].id).mqtt.sess
	waits := testCountContentionWaitsSleeping(t, sess)
	measured, from := bc.burst, 0
	if bc.idle > 0 {
		publish(0, bc.burst)
		open()
		await(bc.burst)
		outboundDeliveryBenchSettled(t, r, subs)
		time.Sleep(bc.idle)
		park()
		measured, from = bc.after, bc.burst
		for _, s := range subs {
			s.mu.Lock()
			s.trace, s.at, s.pis, s.completed = nil, nil, map[uint16]string{}, map[string]bool{}
			s.firstPI, s.fresh, s.resent = map[string]uint16{}, 0, 0
			s.mu.Unlock()
		}
	}
	// THE BURST IS PUBLISHED, every publish acknowledged, before any subscriber reads: the whole
	// burst is backlog. The clock runs from the first read to the last completed delivery; the quiet
	// period that counts late duplicates is outside it.
	publish(from, measured)
	census := startOutboundDeliveryCensus(sess)
	defer census.stop()
	waits.reset()
	start := open()
	await(measured)
	elapsed := time.Since(start)
	time.Sleep(2 * time.Second)
	outboundDeliveryBenchSettled(t, r, subs)

	res := outboundDeliveryBenchResult{elapsed: elapsed, requests: census.stop(), waits: waits.count()}
	for i, s := range subs {
		s.mu.Lock()
		unique, dup, err := outboundDeliveryBenchOracle(from, measured, s.trace, s.completed, bc.qos == 2)
		at := append([]time.Time(nil), s.at...)
		fresh, resent := s.fresh, s.resent
		s.mu.Unlock()
		if err != nil {
			t.Fatalf("subscriber %s: %v", s.id, err)
		}
		res.unique += unique
		res.dup += dup
		res.fresh += fresh
		res.resent += resent
		if i == 0 {
			res.windows, res.gaps = outboundDeliveryBenchShape(start, at)
		}
	}
	return res
}

// read handles the subscriber's side of every delivery until its connection closes: PUBACK for
// QoS 1, PUBREC then PUBCOMP for QoS 2. It reads only while its gate is open.
func (s *outboundDeliveryBenchSub) read() {
	fail := func(err error) {
		s.mu.Lock()
		if s.err == nil && !errors.Is(err, net.ErrClosed) {
			s.err = err
		}
		s.mu.Unlock()
	}
	for {
		s.mu.Lock()
		gate := s.gate
		s.parked = true
		s.mu.Unlock()
		<-gate
		s.mu.Lock()
		s.parked = false
		s.mu.Unlock()
		_ = s.c.SetReadDeadline(time.Time{})
		b, body, err := inboundAckBenchReadPacket(s.br)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			continue // parked between phases, with nothing in flight
		}
		if err != nil {
			fail(err)
			return
		}
		switch b & mqttPacketMask {
		case mqttPacketPub:
			if len(body) < 2 {
				fail(fmt.Errorf("short PUBLISH"))
				return
			}
			tl := int(binary.BigEndian.Uint16(body))
			rest := body[2+tl:]
			pqos := (b & mqttPubFlagQoS) >> 1
			var pi uint16
			if pqos > 0 {
				pi = binary.BigEndian.Uint16(rest)
				rest = rest[2:]
			}
			s.mu.Lock()
			if first, seen := s.firstPI[string(rest)]; seen {
				if first == pi && b&mqttPubFlagDup != 0 {
					s.resent++
				} else {
					s.fresh++
				}
			} else {
				s.firstPI[string(rest)] = pi
			}
			s.trace = append(s.trace, string(rest))
			s.at = append(s.at, time.Now())
			if pqos == 2 {
				s.pis[pi] = string(rest)
			}
			s.mu.Unlock()
			switch pqos {
			case 1:
				_, err = s.c.Write([]byte{mqttPacketPubAck, 2, byte(pi >> 8), byte(pi)})
			case 2:
				_, err = s.c.Write([]byte{mqttPacketPubRec, 2, byte(pi >> 8), byte(pi)})
			}
		case mqttPacketPubRel:
			pi := binary.BigEndian.Uint16(body)
			if _, err = s.c.Write([]byte{mqttPacketPubComp, 2, body[0], body[1]}); err == nil {
				s.mu.Lock()
				if p, ok := s.pis[pi]; ok {
					s.completed[p] = true
				}
				s.mu.Unlock()
			}
		}
		if err != nil {
			fail(err)
			return
		}
	}
}

// outboundDeliveryBenchUnique counts the distinct payloads in a trace.
func outboundDeliveryBenchUnique(trace []string) int {
	seen := map[string]struct{}{}
	for _, p := range trace {
		seen[p] = struct{}{}
	}
	return len(seen)
}

// outboundDeliveryBenchOracle checks one subscriber's trace against messages m<from>..m<from+n-1>:
// every one delivered, the FIRST delivery of each in publish order, nothing else delivered, and for
// QoS 2 every one's exchange completed (its PUBREL answered). It returns the unique and duplicate
// counts.
func outboundDeliveryBenchOracle(from, n int, trace []string, completed map[string]bool, qos2 bool) (unique, dup int, err error) {
	seen := map[string]struct{}{}
	next := from
	for _, p := range trace {
		if _, ok := seen[p]; ok {
			dup++
			continue
		}
		seen[p] = struct{}{}
		if want := fmt.Sprintf("m%d", next); p != want {
			return 0, 0, fmt.Errorf("first delivery %d is %q, want %q: delivery order is not publish order", next-from, p, want)
		}
		next++
	}
	if next != from+n {
		return 0, 0, fmt.Errorf("%d of %d messages delivered", next-from, n)
	}
	if qos2 {
		for i := from; i < from+n; i++ {
			if p := fmt.Sprintf("m%d", i); !completed[p] {
				return 0, 0, fmt.Errorf("the QoS 2 exchange of %q never completed: no PUBREL was answered", p)
			}
		}
	}
	return n, dup, nil
}

// outboundDeliveryBenchSettled requires the broker to have settled every subscriber's deliveries:
// nothing pending or awaiting acknowledgement on its source consumers, nor on its PUBREL consumer.
func outboundDeliveryBenchSettled(t *testing.T, r inboundAckBenchRig, subs []*outboundDeliveryBenchSub) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		unsettled := _EMPTY_
		for _, stream := range []string{mqttStreamName, mqttOutStreamName} {
			for ci := range r.js.ConsumersInfo(stream) {
				for _, s := range subs {
					if strings.Contains(ci.Name, s.idHash) && (ci.NumAckPending != 0 || ci.NumPending != 0) {
						unsettled = fmt.Sprintf("%s on %s: %d pending, %d awaiting acknowledgement",
							ci.Name, stream, ci.NumPending, ci.NumAckPending)
					}
				}
			}
		}
		if unsettled == _EMPTY_ {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the broker did not settle the deliveries: %s", unsettled)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// outboundDeliveryBenchShape returns the rate per window of first deliveries and the gaps between
// consecutive arrivals.
func outboundDeliveryBenchShape(start time.Time, at []time.Time) ([]float64, []time.Duration) {
	var windows []float64
	var gaps []time.Duration
	prev := start
	wstart := start
	for i, a := range at {
		gaps = append(gaps, a.Sub(prev))
		prev = a
		if (i+1)%outboundDeliveryBenchWindow == 0 {
			windows = append(windows, float64(outboundDeliveryBenchWindow)/a.Sub(wstart).Seconds())
			wstart = a
		}
	}
	return windows, gaps
}

// outboundDeliveryCensus counts the requests the account's store client sends, by class: verb
// and, for a message GET on the in-flight stream, the record class it reads.
type outboundDeliveryCensus struct {
	jsa *mqttJSA
	mu  sync.Mutex
	n   map[string]int
}

func startOutboundDeliveryCensus(sess *mqttSession) *outboundDeliveryCensus {
	c := &outboundDeliveryCensus{jsa: sess.jsa, n: map[string]int{}}
	obs := func(ev mqttJSARequestEvent) {
		if ev.Kind != mqttJSARequestSent {
			return
		}
		cls := outboundDeliveryCensusClass(ev.Subject, ev.Body)
		c.mu.Lock()
		c.n[cls]++
		c.mu.Unlock()
	}
	c.jsa.testObserveRequest.Store(&obs)
	return c
}

func (c *outboundDeliveryCensus) stop() map[string]int {
	c.jsa.testObserveRequest.Store(nil)
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for k, v := range c.n {
		out[k] = v
	}
	return out
}

func outboundDeliveryCensusClass(subject string, body []byte) string {
	tk := strings.Split(subject, ".")
	if !strings.HasPrefix(subject, "$JS.API.") {
		if len(tk) > 3 {
			tk = tk[:3]
		}
		return "publish " + strings.Join(tk, ".")
	}
	verb := strings.Join(tk[2:min(len(tk), 4)], ".")
	if verb == "STREAM.MSG" && len(tk) > 4 {
		verb = "STREAM.MSG." + tk[4]
	}
	if strings.HasPrefix(verb, "STREAM.MSG.GET") {
		b := string(body)
		if i := strings.Index(b, mqttF105SubjectPrefix()); i >= 0 {
			rest := b[i+len(mqttF105SubjectPrefix()):]
			if j := strings.IndexAny(rest, ".\""); j > 0 {
				rest = rest[:j]
			}
			return verb + " " + rest
		}
		if strings.Contains(b, mqttInflightStreamSubjectPrefix) {
			return verb + " register/marker"
		}
	}
	return verb
}

// testContentionWaitCounter counts the requested authority-contention waits and still sleeps them.
type testContentionWaitCounter struct {
	mu sync.Mutex
	n  int64
}

func (w *testContentionWaitCounter) reset() { w.mu.Lock(); w.n = 0; w.mu.Unlock() }
func (w *testContentionWaitCounter) count() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

func testCountContentionWaitsSleeping(t *testing.T, sess *mqttSession) *testContentionWaitCounter {
	t.Helper()
	w := &testContentionWaitCounter{}
	as := sess.asm
	as.mu.Lock()
	as.testAuthorityContentionWait = func(d time.Duration) {
		w.mu.Lock()
		w.n++
		w.mu.Unlock()
		time.Sleep(d)
	}
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		as.testAuthorityContentionWait = nil
		as.mu.Unlock()
	})
	return w
}

func outboundDeliveryBenchReport(t *testing.T, rig string, bc outboundDeliveryBenchCase, res outboundDeliveryBenchResult) {
	t.Helper()
	sort.Slice(res.gaps, func(i, j int) bool { return res.gaps[i] < res.gaps[j] })
	pct := func(p float64) time.Duration {
		if len(res.gaps) == 0 {
			return 0
		}
		return res.gaps[min(len(res.gaps)-1, int(p*float64(len(res.gaps))))]
	}
	rate := float64(res.unique) / res.elapsed.Seconds()
	var windows []string
	for _, w := range res.windows {
		windows = append(windows, strconv.FormatFloat(w, 'f', 1, 64))
	}
	total := 0
	for _, v := range res.requests {
		total += v
	}
	perDelivery := float64(total) / float64(max(res.unique, 1))
	row := []string{os.Getenv(outboundDeliveryBenchLabelEnv), rig, bc.name, strconv.Itoa(res.unique),
		fmt.Sprintf("%d (fresh %d, retransmitted %d)", res.dup, res.fresh, res.resent), strconv.FormatFloat(rate, 'f', 1, 64), pct(0.5).String(), pct(0.9).String(),
		strconv.FormatFloat(perDelivery, 'f', 1, 64), strconv.FormatInt(res.waits, 10), strings.Join(windows, " ")}
	t.Logf("label=%s rig=%s case=%s unique=%s dup=%s rate=%s/s gap_p50=%s gap_p90=%s requests_per_delivery=%s "+
		"contention_waits=%s windows=[%s]", row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
		row[9], row[10])
	classes := make([]string, 0, len(res.requests))
	for k := range res.requests {
		classes = append(classes, k)
	}
	sort.Strings(classes)
	for _, k := range classes {
		t.Logf("requests %s: %.1f per delivery", k, float64(res.requests[k])/float64(max(res.unique, 1)))
	}
	if out := os.Getenv(outboundDeliveryBenchOutEnv); out != _EMPTY_ {
		f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatalf("bench output: %v", err)
		}
		defer f.Close()
		fmt.Fprintln(f, strings.Join(row, ","))
	}
}

// TestMQTTOutboundDeliveryBenchOracleDiscriminates: the oracle rejects each failure it exists for,
// alone, and accepts duplicates.
func TestMQTTOutboundDeliveryBenchOracleDiscriminates(t *testing.T) {
	all := map[string]bool{"m0": true, "m1": true, "m2": true}
	for name, tc := range map[string]struct {
		trace     []string
		qos2      bool
		completed map[string]bool
		ok        bool
		dup       int
	}{
		"complete and in order":         {[]string{"m0", "m1", "m2"}, false, nil, true, 0},
		"a duplicate":                   {[]string{"m0", "m1", "m1", "m2", "m0"}, false, nil, true, 2},
		"a message missing":             {[]string{"m0", "m2"}, false, nil, false, 0},
		"a message overtaken":           {[]string{"m0", "m2", "m1"}, false, nil, false, 0},
		"a message never received":      {[]string{"m0", "m1"}, false, nil, false, 0},
		"a foreign message":             {[]string{"m0", "m1", "x", "m2"}, false, nil, false, 0},
		"QoS 2, every exchange done":    {[]string{"m0", "m1", "m2"}, true, all, true, 0},
		"QoS 2, an exchange unfinished": {[]string{"m0", "m1", "m2"}, true, map[string]bool{"m0": true, "m2": true}, false, 0},
	} {
		_, dup, err := outboundDeliveryBenchOracle(0, 3, tc.trace, tc.completed, tc.qos2)
		if tc.ok && err != nil {
			t.Fatalf("%s: refused: %v", name, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("%s: accepted", name)
		}
		if tc.ok && dup != tc.dup {
			t.Fatalf("%s: %d duplicate(s) counted, want %d", name, dup, tc.dup)
		}
	}
}
