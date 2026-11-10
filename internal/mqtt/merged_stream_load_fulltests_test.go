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
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nuid"
)

// The merged-stream load measurement runs, at once and against one broker, the four
// activities whose interaction the session stream now carries in one place: QoS 1 traffic
// (in-flight register writes), fresh-identifier CONNECT churn (session records, acceptance
// facts, receipts and authority transitions), delayed Wills coming due (the acceptance-fact
// scan and its cutoff), and the sweep tick driven back to back (the session-record walk among
// it). It reports CONNECT latency, sweep pass duration, how late each Will was published
// after it fell due, and how far the stream moved while one session-record walk ran (the tail
// that walk must cover).
//
// A measurement, not a pass/fail test: the same file runs on two trees and the rows are
// compared. It asserts only that the four activities actually overlapped and that every
// observation was made, so a quiet run cannot pass for a loaded one.
//
// Opt-in: MQTTNB_MERGED_LOAD=1. MQTTNB_MERGED_LOAD_OUT names a CSV the rows are appended to,
// MQTTNB_MERGED_LOAD_LABEL labels them, MQTTNB_MERGED_LOAD_SECONDS sets the run (default 45).

const (
	mergedLoadEnv      = "MQTTNB_MERGED_LOAD"
	mergedLoadOutEnv   = "MQTTNB_MERGED_LOAD_OUT"
	mergedLoadLabelEnv = "MQTTNB_MERGED_LOAD_LABEL"
	mergedLoadSecsEnv  = "MQTTNB_MERGED_LOAD_SECONDS"
	mergedLoadWillWait = 1 // the Will Delay Interval, seconds
)

// mergedLoadSeries is one measured distribution, in milliseconds.
type mergedLoadSeries struct {
	mu sync.Mutex
	ms []float64
}

func (s *mergedLoadSeries) add(d time.Duration) {
	s.mu.Lock()
	s.ms = append(s.ms, float64(d.Microseconds())/1000)
	s.mu.Unlock()
}

func (s *mergedLoadSeries) row(label, metric string) string {
	s.mu.Lock()
	v := append([]float64(nil), s.ms...)
	s.mu.Unlock()
	if len(v) == 0 {
		return fmt.Sprintf("%s,%s,0,,,", label, metric)
	}
	sort.Float64s(v)
	q := func(p float64) float64 { return v[min(len(v)-1, int(p*float64(len(v))))] }
	return fmt.Sprintf("%s,%s,%d,%.1f,%.1f,%.1f", label, metric, len(v), q(0.5), q(0.95), v[len(v)-1])
}

func (s *mergedLoadSeries) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ms)
}

// mergedLoadClient dials and connects one MQTT 5.0 client.
func mergedLoadClient(t *testing.T, addr string, cp *paho.Connect,
	onPub func(paho.PublishReceived) (bool, error)) (*paho.Client, net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, nil, err
	}
	cc := paho.ClientConfig{Conn: conn}
	if onPub != nil {
		cc.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPub}
	}
	c := paho.NewClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca, err := c.Connect(ctx, cp)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if ca.ReasonCode != 0 {
		conn.Close()
		return nil, nil, fmt.Errorf("CONNACK reason %d", ca.ReasonCode)
	}
	return c, conn, nil
}

func TestMQTTMergedStreamLoad(t *testing.T) {
	if os.Getenv(mergedLoadEnv) != "1" {
		t.Skipf("set %s=1 to run the merged-stream load measurement", mergedLoadEnv)
	}
	secs := 45
	if v, err := strconv.Atoi(os.Getenv(mergedLoadSecsEnv)); err == nil && v > 0 {
		secs = v
	}
	// The sweep ticker is pushed out so this test drives the tick itself, back to back, and
	// times each pass; the Will claim loop runs at its own default cadence, as in production.
	restoreSweep := setMQTTSessionExpirySweepInterval(time.Hour)
	t.Cleanup(restoreSweep)

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	addr := fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port)
	expiry := uint32(3600)
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	var wg sync.WaitGroup
	running := func() bool { return time.Now().Before(deadline) }

	var connectLat, sweepPass, walkSpan, willLate mergedLoadSeries
	var delivered, published atomic.Int64

	// The Will watcher: a Will's lateness is its arrival past the moment it fell due.
	var dueMu sync.Mutex
	due := map[string]time.Time{}
	watcher, _, err := mergedLoadClient(t, addr, &paho.Connect{ClientID: "ml-watch", CleanStart: true, KeepAlive: 60},
		func(pr paho.PublishReceived) (bool, error) {
			id := string(pr.Packet.Payload)
			dueMu.Lock()
			at, ok := due[id]
			delete(due, id)
			dueMu.Unlock()
			if ok {
				willLate.add(time.Since(at))
			}
			return true, nil
		})
	if err != nil {
		t.Fatalf("watcher: %v", err)
	}
	defer func() { _ = watcher.Disconnect(&paho.Disconnect{}) }()
	ctx := context.Background()
	if _, err := watcher.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "ml/will/#", QoS: 1}}}); err != nil {
		t.Fatalf("watcher subscribe: %v", err)
	}

	// QoS 1 traffic: persistent subscribers and publishers waiting for each PUBACK.
	for i := 0; i < 4; i++ {
		sub, _, err := mergedLoadClient(t, addr, &paho.Connect{ClientID: fmt.Sprintf("ml-sub-%d", i), CleanStart: true,
			KeepAlive: 60, Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry}},
			func(paho.PublishReceived) (bool, error) { delivered.Add(1); return true, nil })
		if err != nil {
			t.Fatalf("subscriber %d: %v", i, err)
		}
		defer func() { _ = sub.Disconnect(&paho.Disconnect{}) }()
		if _, err := sub.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: fmt.Sprintf("ml/q/%d", i), QoS: 1}}}); err != nil {
			t.Fatalf("subscriber %d subscribe: %v", i, err)
		}
	}
	for i := 0; i < 4; i++ {
		pub, _, err := mergedLoadClient(t, addr, &paho.Connect{ClientID: fmt.Sprintf("ml-pub-%d", i), CleanStart: true, KeepAlive: 60}, nil)
		if err != nil {
			t.Fatalf("publisher %d: %v", i, err)
		}
		defer func() { _ = pub.Disconnect(&paho.Disconnect{}) }()
		wg.Add(1)
		go func(i int, pub *paho.Client) {
			defer wg.Done()
			for running() {
				pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, err := pub.Publish(pctx, &paho.Publish{Topic: fmt.Sprintf("ml/q/%d", i), QoS: 1, Payload: []byte("x")})
				cancel()
				if err == nil {
					published.Add(1)
				}
			}
		}(i, pub)
	}

	// Fresh-identifier CONNECT churn, the production prober's shape: clean session, no
	// subscription, a graceful DISCONNECT.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for running() {
			start := time.Now()
			c, _, err := mergedLoadClient(t, addr, &paho.Connect{ClientID: "ml-probe-" + nuid.Next(), CleanStart: true, KeepAlive: 30}, nil)
			if err != nil {
				continue
			}
			connectLat.add(time.Since(start))
			_ = c.Disconnect(&paho.Disconnect{})
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Delayed Wills coming due: a client with a Will Delay connects and drops its socket, so
	// the Will falls due mergedLoadWillWait seconds later and the claimant scans for it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		delay := uint32(mergedLoadWillWait)
		willExpiry := uint32(30)
		for running() {
			id := "ml-will-" + nuid.Next()
			c, conn, err := mergedLoadClient(t, addr, &paho.Connect{ClientID: id, CleanStart: true, KeepAlive: 30,
				Properties:     &paho.ConnectProperties{SessionExpiryInterval: &willExpiry},
				WillMessage:    &paho.WillMessage{Topic: "ml/will/" + id, QoS: 1, Payload: []byte(id)},
				WillProperties: &paho.WillProperties{WillDelayInterval: &delay}}, nil)
			if err != nil {
				continue
			}
			_ = c
			dueMu.Lock()
			due[id] = time.Now().Add(time.Duration(mergedLoadWillWait) * time.Second)
			dueMu.Unlock()
			conn.Close()
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// The sweep tick, back to back, and one session-record walk timed with the stream's
	// movement across it.
	s.mqtt.sessmgr.mu.RLock()
	asm := s.mqtt.sessmgr.sessions[globalAccountName]
	s.mqtt.sessmgr.mu.RUnlock()
	if asm == nil {
		t.Fatal("no account session manager")
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for running() {
			start := time.Now()
			asm.reconcileResidentRecordState(s)
			asm.sweepSessionExpiries(s)
			asm.sweepOrphanedConsumers(s)
			asm.sweepSharedGroupRetirements(s)
			asm.sweepAcqReleaseVerdicts(s)
			asm.sweepOrphanedQoS2State(s)
			asm.sweepOutboundRecovery(s, func() bool { return false })
			sweepPass.add(time.Since(start))

			before, err := asm.sessLastSeq()
			if err != nil {
				continue
			}
			if werr := asm.walkSessionRecords(func(*StoredMsg) error { return nil }); werr != nil {
				continue
			}
			after, err := asm.sessLastSeq()
			if err == nil && after >= before {
				// Recorded as a count, through the same series: one "ms" is one sequence.
				walkSpan.add(time.Duration(after-before) * time.Millisecond)
			}
		}
	}()

	wg.Wait()
	// Let the last Wills fall due and arrive.
	time.Sleep(time.Duration(mergedLoadWillWait)*time.Second + 35*time.Second)

	dueMu.Lock()
	missing := len(due)
	dueMu.Unlock()
	label := os.Getenv(mergedLoadLabelEnv)
	rows := []string{
		connectLat.row(label, "connect_latency_ms"),
		sweepPass.row(label, "sweep_pass_ms"),
		willLate.row(label, "will_lateness_ms"),
		walkSpan.row(label, "walk_tail_sequences"),
		fmt.Sprintf("%s,qos1_published,%d,,,", label, published.Load()),
		fmt.Sprintf("%s,qos1_delivered,%d,,,", label, delivered.Load()),
		fmt.Sprintf("%s,wills_unpublished,%d,,,", label, missing),
	}
	for _, r := range rows {
		t.Log(r)
	}
	if out := os.Getenv(mergedLoadOutEnv); out != _EMPTY_ {
		fh, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatalf("output: %v", err)
		}
		defer fh.Close()
		for _, r := range rows {
			fmt.Fprintln(fh, r)
		}
	}
	// The activities must have overlapped, or the rows describe a quiet broker.
	if published.Load() == 0 || connectLat.count() == 0 || sweepPass.count() == 0 || willLate.count() == 0 || walkSpan.count() == 0 {
		t.Fatalf("an activity made no observation: published=%d connects=%d sweeps=%d wills=%d walks=%d",
			published.Load(), connectLat.count(), sweepPass.count(), willLate.count(), walkSpan.count())
	}
}
