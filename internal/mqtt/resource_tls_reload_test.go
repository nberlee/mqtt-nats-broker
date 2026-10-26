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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/nberlee/mqtt-nats-broker/internal/tlsreload"
)

// tlsReloadCapture holds what the soak observed of each published TLS material without
// holding the material: weak pointers to its per-handshake configuration and its client-CA
// pool, and its generation.
type tlsReloadCapture struct {
	cfgs  []weak.Pointer[tls.Config]
	pools []weak.Pointer[x509.CertPool]
	gens  []uint64
}

// awaitPublished polls until the source has published generation want, then records it.
// Polling Active keeps every test frame off the watcher's stack.
func (c *tlsReloadCapture) awaitPublished(t *testing.T, src *tlsreload.Source, want uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		cfg, pool, gen := src.Active()
		if gen == want {
			if cfg == nil || pool == nil {
				t.Fatalf("generation %d published a nil configuration (%v) or pool (%v)", gen, cfg, pool)
			}
			c.cfgs = append(c.cfgs, weak.Make(cfg))
			c.pools = append(c.pools, weak.Make(pool))
			c.gens = append(c.gens, gen)
			return
		}
		if gen > want {
			t.Fatalf("generation %d published while waiting for %d: a publication was not observed", gen, want)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the watcher never published generation %d (active %d)", want, gen)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// awaitSuperseded requires every captured configuration and pool older than the active
// generation to become unreachable within d.
func (c *tlsReloadCapture) awaitSuperseded(t *testing.T, block int, active uint64, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		runtime.GC()
		runtime.GC()
		var alive []string
		for i, g := range c.gens {
			if g >= active {
				continue
			}
			if c.cfgs[i].Value() != nil {
				alive = append(alive, fmt.Sprintf("config of generation %d", g))
			}
			if c.pools[i].Value() != nil {
				alive = append(alive, fmt.Sprintf("client-CA pool of generation %d", g))
			}
		}
		if len(alive) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("block %d: superseded TLS material still reachable:\n%s", block, strings.Join(alive, "\n"))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// tlsReloadWatchers counts goroutines rooted in the TLS source's package, through the
// soak's own classifier, so a classifier that cannot see that package counts zero.
func tlsReloadWatchers() int {
	n := 0
	for root, k := range brokerGoroutines() {
		if strings.Contains(root, tlsReloadPkgFrame) {
			n += k
		}
	}
	return n
}

// tlsReloadCycle is one renewal under traffic: a fresh listener pair and a fresh client CA
// are projected, the watcher applies them, an mTLS client issued by the new CA connects and
// publishes, and a refused update (mismatched key) follows. Its locals end with it, so no
// reference to a client or its connection state outlives the cycle.
func tlsReloadCycle(t *testing.T, o *Options, src *tlsreload.Source, proj *tlsProjection, capture *tlsReloadCapture, block, i int) {
	g, other := newTLSGeneration(t), newTLSGeneration(t)
	proj.swap(g.cert, g.key, g.ca.CertPEM)
	capture.awaitPublished(t, src, capture.gens[len(capture.gens)-1]+1)

	tag := fmt.Sprintf("tls-b%d-%d", block, i)
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: tag, cleanSess: true, tls: true, tlsc: tlsReloadClient(&g.client, g)},
		o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, c, r, 1, false, false, "soak/tls/"+tag, 1, []byte("renewed"))
	testMQTTDisconnect(t, c, nil)
	c.Close()

	refusals := src.Refusals()
	proj.swap(g.cert, other.key, g.ca.CertPEM)
	deadline := time.Now().Add(10 * time.Second)
	for src.Refusals() == refusals {
		if time.Now().After(deadline) {
			t.Fatal("the watcher never refused a mismatched key")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestResourceTLSReloadChurn: repeated renewals of the listener certificate AND the client
// CA, under connection churn, with a refused update between each, leave nothing behind:
// every superseded configuration and pool is collected once its connections are gone, the
// watcher is the only goroutine the source runs, broker goroutines return to the idle set,
// descriptors and broker-allocated heap do not grow, and Stop leaves no watcher.
func TestResourceTLSReloadChurn(t *testing.T) {
	requireResourceSoak(t)
	prevRate := runtime.MemProfileRate
	runtime.MemProfileRate = 4096
	t.Cleanup(func() { runtime.MemProfileRate = prevRate })
	// quiesce waits out two periods of each sweep; shorten them as the single-broker soak does.
	prevJail, prevFlap, prevTTL := mqttSessJailDur, mqttFlapCleanItvl, mqttRetainedCacheTTL
	mqttSessJailDur, mqttFlapCleanItvl, mqttRetainedCacheTTL = 50*time.Millisecond, 250*time.Millisecond, 500*time.Millisecond
	t.Cleanup(func() { mqttSessJailDur, mqttFlapCleanItvl, mqttRetainedCacheTTL = prevJail, prevFlap, prevTTL })

	first := newTLSGeneration(t)
	proj := newTLSProjection(t, first.cert, first.key, first.ca.CertPEM)
	src, err := tlsreload.Load(proj.files(true), nil)
	if err != nil {
		t.Fatal(err)
	}
	o := testMQTTDefaultOptions()
	o.MQTT.TLSConfig = src.ServerConfig()
	o.MQTT.TLSTimeout = 1
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	src.Start(50 * time.Millisecond)
	stopped := false
	defer func() {
		if !stopped {
			src.Stop()
		}
	}()

	capture := &tlsReloadCapture{}
	capture.awaitPublished(t, src, 1)
	as := testMQTTGetGlobalASM(t, s)
	quiesce(t, s, as, nil)
	baseline := awaitIdleGoroutines(t)
	if n := tlsReloadWatchers(); n != 1 {
		t.Fatalf("%d goroutines rooted in the TLS source while its watcher runs, want 1", n)
	}

	const blocks, perBlock = 4, 25 // block 0 is the warm-up
	var brokerHeaps []int64
	var fds []int
	for b := 0; b < blocks; b++ {
		for i := 0; i < perBlock; i++ {
			tlsReloadCycle(t, o, src, proj, capture, b, i)
		}
		quiesce(t, s, as, nil)
		_, _, active := src.Active()
		if want := uint64(1 + (b+1)*perBlock); active != want || uint64(len(capture.gens)) != want {
			t.Fatalf("block %d: active generation %d with %d captured, want %d of each", b, active, len(capture.gens), want)
		}
		for i := 1; i < len(capture.gens); i++ {
			if capture.gens[i] != capture.gens[i-1]+1 {
				t.Fatalf("block %d: generations %v are not consecutive", b, capture.gens)
			}
			if capture.pools[i] == capture.pools[i-1] {
				t.Fatalf("block %d: generations %d and %d share a client-CA pool", b, capture.gens[i-1], capture.gens[i])
			}
		}
		capture.awaitSuperseded(t, b, active, resourceSoakQuiesce)
		if b == 0 {
			continue
		}
		deadline := time.Now().Add(resourceSoakQuiesce)
		for {
			d := goroutineDiff(baseline, brokerGoroutines())
			if d == "" {
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("block %d: broker goroutines did not return to the idle set:\n%s", b, d)
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		if n := tlsReloadWatchers(); n != 1 {
			t.Errorf("block %d: %d goroutines rooted in the TLS source, want its one watcher", b, n)
		}
		brokerHeaps = append(brokerHeaps, brokerHeapInuse())
		fds = append(fds, openFDs(t))
		t.Logf("block %d: generation %d, %d refusals, broker heap in use ~%d bytes, %d descriptors",
			b, active, src.Refusals(), brokerHeaps[len(brokerHeaps)-1], fds[len(fds)-1])
	}
	const heapAllowance, fdAllowance = 8 << 20, 16
	if grew := brokerHeaps[len(brokerHeaps)-1] - brokerHeaps[0]; grew > heapAllowance {
		t.Errorf("broker-allocated heap in use grew ~%d bytes across %d blocks of reloads (allowance %d)", grew, blocks-2, heapAllowance)
	}
	if grew := fds[len(fds)-1] - fds[0]; grew > fdAllowance {
		t.Errorf("open descriptors grew by %d across %d blocks (allowance %d)", grew, blocks-2, fdAllowance)
	}

	src.Stop()
	stopped = true
	deadline := time.Now().Add(10 * time.Second)
	for tlsReloadWatchers() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("a goroutine rooted in the TLS source outlived Stop")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
