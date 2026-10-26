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
	"sync"
	"testing"
	"time"
)

// A graceful shutdown joins the closure-certificate writes before Phase 4 stops the
// JetStream request path, so a certificate claimed before shutdown is written rather than
// lost under it. Not parallel: the persistence and claim seams are process-wide.

// testHoldCertWrite parks b's certificate write at the persistence seam until the returned
// release runs; every other binding's write proceeds untouched. held fires when it parks.
func testHoldCertWrite(t *testing.T, b *mqttConnBinding) (held <-chan struct{}, release func()) {
	t.Helper()
	ch := make(chan struct{}, 1)
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	h := func(c *mqttDurableCloseCert) bool {
		if c.Run != b.run || c.Conn != b.conn {
			return true
		}
		ch <- struct{}{}
		<-gate
		return true
	}
	prev := mqttTestCloseCertPersistV.Swap(&h)
	t.Cleanup(func() { mqttTestCloseCertPersistV.Store(prev) })
	return ch, release
}

// testObserveCertJoin reports when s reaches joinCloseCertWrites with its gate closed.
func testObserveCertJoin(t *testing.T, s *Server) <-chan struct{} {
	t.Helper()
	ch := make(chan struct{}, 1)
	h := func(srv *Server) {
		if srv == s {
			ch <- struct{}{}
		}
	}
	prev := mqttTestShutdownCertJoinV.Swap(&h)
	t.Cleanup(func() { mqttTestShutdownCertJoinV.Store(prev) })
	return ch
}

func testCloseCertInFlight(s *Server) int {
	s.closeCertMu.Lock()
	defer s.closeCertMu.Unlock()
	return s.closeCertInFlight
}

func testClosedChan(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestGracefulShutdownJoinsTheClosureCertificateWrite: a connection that acquired is
// certified and its certificate write is held; shutdown must reach the join and wait there,
// with quitCh still open, until the write is released, and the certificate must then stand.
// Without the join, shutdown closes quitCh with the write outstanding, which is the loss the
// join exists to prevent; nothing here is timed, every step is a fence.
func TestGracefulShutdownJoinsTheClosureCertificateWrite(t *testing.T) {
	sec := testRow2OpenSection(t, "cert-shutdown", 1)
	sec.acquire(t)
	e := sec.e
	b := e.c.outBind
	epoch, gen := e.asm.sessStreamEpoch, e.sess.gen

	held, release := testHoldCertWrite(t, b)
	joined := testObserveCertJoin(t, e.s)
	mqttCertifyClosed(e.c, b, "closed by the shutdown under test")
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the certificate write never started")
	}

	done := e.s.BeginShutdown()
	e.s.mu.Lock()
	quitCh := e.s.quitCh
	e.s.mu.Unlock()
	select {
	case <-joined:
	case <-quitCh:
		t.Fatal("shutdown crossed Phase 4 with a certificate write outstanding")
	case <-done:
		t.Fatal("shutdown finished with a certificate write outstanding")
	case <-time.After(30 * time.Second):
		t.Fatal("shutdown never reached the certificate join")
	}
	if testClosedChan(quitCh) || testClosedChan(done) {
		t.Fatal("shutdown went past the join while the certificate write was held")
	}

	release()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("shutdown did not complete once the certificate write was released")
	}

	asm2, _, _ := e.peer(t, "node2", "cert-shutdown")
	c, err := asm2.jsa.loadDurableCloseCert(e.sess.id, epoch, gen, b.run, b.conn)
	if err != nil || c == nil {
		t.Fatalf("the certificate of %s/%s does not stand after a graceful shutdown: %+v (%v)", b.run, b.conn, c, err)
	}
}

// TestCertificateJoinCoversAClaimedButUnlaunchedWrite parks a certifier between claiming its
// persistence attempt and launching it. The join's token must already exist there, so the
// join cannot return until that write has run: a token taken at launch instead would leave a
// claimed write no join can see, and every other certifier finds the attempt already claimed.
func TestCertificateJoinCoversAClaimedButUnlaunchedWrite(t *testing.T) {
	sec := testRow2OpenSection(t, "cert-claim-gap", 1)
	sec.acquire(t)
	e := sec.e
	b := e.c.outBind

	parked := make(chan struct{}, 1)
	gate := make(chan struct{})
	var once sync.Once
	releaseClaim := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(releaseClaim)
	claimed := func(cb *mqttConnBinding) {
		if cb != b {
			return
		}
		parked <- struct{}{}
		<-gate
	}
	prev := mqttTestCertClaimedV.Swap(&claimed)
	t.Cleanup(func() { mqttTestCertClaimedV.Store(prev) })
	held, releaseWrite := testHoldCertWrite(t, b)
	joinEntered := testObserveCertJoin(t, e.s)

	go mqttCertifyClosed(e.c, b, "parked between claim and launch")
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the certifier never claimed the persistence attempt")
	}
	if n := testCloseCertInFlight(e.s); n != 1 {
		t.Fatalf("%d certificate writes are joinable while the claimed one is parked before launch, want 1", n)
	}

	joinDone := make(chan struct{})
	go func() {
		e.s.joinCloseCertWrites()
		close(joinDone)
	}()
	select {
	case <-joinEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the join never closed its gate")
	}
	if testClosedChan(joinDone) {
		t.Fatal("the join returned while a claimed write had not even launched")
	}
	releaseClaim()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the released certifier never launched its write")
	}
	if testClosedChan(joinDone) {
		t.Fatal("the join returned while the write was still held")
	}
	releaseWrite()
	select {
	case <-joinDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the join did not return once the write ended")
	}
	if n := testCloseCertInFlight(e.s); n != 0 {
		t.Fatalf("%d certificate writes still counted after the join returned", n)
	}
	testAwaitDurableCert(t, e, b)
}
