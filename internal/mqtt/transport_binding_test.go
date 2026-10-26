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
	"net"
	"sync"
	"testing"
	"time"
)

// TestF105SealIsOneWayAndIdempotent pins that output permission, once withdrawn, stays
// withdrawn, and that only the first sealer is the one that establishes the fact.
//
// A gate that could reopen would make a certificate a statement about a moment rather than
// about the rest of the connection's life, and every reader would have to re-check — the
// read-then-act shape this item exists to remove.
func TestF105SealIsOneWayAndIdempotent(t *testing.T) {
	b := mqttNewConnBinding("run-1", "conn-1")
	if !b.mayWrite() {
		t.Fatalf("a fresh binding refuses output")
	}
	if b.closeCert() != nil {
		t.Fatalf("a fresh binding already carries a closure certificate")
	}

	if !b.gate.seal() {
		t.Fatalf("the first seal did not report itself as the sealer")
	}
	if b.gate.seal() {
		t.Fatalf("a second seal reported itself as the sealer, so two callers would each " +
			"believe they owed the certificate")
	}
	if b.mayWrite() {
		t.Fatalf("output is still permitted after sealing")
	}
}

// TestF105CertificateNamesItsOwnConnection pins that a certificate is checked against the
// binding it is claimed for. Accepting any certificate would let one connection's closure be
// read as another's.
func TestF105CertificateNamesItsOwnConnection(t *testing.T) {
	b := mqttNewConnBinding("run-1", "conn-1")
	cert := mqttCertifyClosed(nil, b, "test")
	if cert == nil {
		t.Fatalf("certifying a binding with no client produced no certificate")
	}
	if !mqttBindingCertifies(b, cert) {
		t.Fatalf("a binding does not accept its own certificate")
	}
	for name, other := range map[string]*mqttConnBinding{
		"another connection": mqttNewConnBinding("run-1", "conn-2"),
		"another broker run": mqttNewConnBinding("run-2", "conn-1"),
	} {
		if mqttBindingCertifies(other, cert) {
			t.Fatalf("%s accepted a certificate that does not name it", name)
		}
	}
	if mqttBindingCertifies(b, nil) {
		t.Fatalf("a nil certificate was accepted: absence is not closure")
	}

	// Idempotent: a second call returns the established certificate rather than a second,
	// weaker one.
	again := mqttCertifyClosed(nil, b, "test-again")
	if again == nil || again.Reason != "test" {
		t.Fatalf("a second certification replaced the established certificate: %+v", again)
	}
}

// TestF105CertificationCrossesTheWriterBarrier is the test the whole ordering exists for.
//
// Sealing the gate stops writers from BEGINNING. It says nothing about one already inside a
// write. Certification must therefore cross the writer's own lock, and it must close the
// transport OUTSIDE that lock — a blocked writer holds it, so closing under it would
// deadlock against the very writer being stopped.
//
// The fixture holds the write lock for a bounded time and asserts that certification cannot
// complete until it is released, which is exactly the property "no write is in progress"
// depends on.
func TestF105CertificationCrossesTheWriterBarrier(t *testing.T) {
	c := &client{}
	b := mqttNewConnBinding("run-1", "conn-1")

	held := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.wmu.Lock()
		close(held)
		<-release
		c.wmu.Unlock()
	}()
	<-held

	done := make(chan *mqttCloseCert, 1)
	go func() { done <- mqttCertifyClosed(c, b, "barrier") }()

	select {
	case <-done:
		t.Fatalf("certification completed while a writer still held the write lock: it " +
			"would be certifying that no write is in progress while one is")
	case <-time.After(150 * time.Millisecond):
	}

	// The gate is sealed already, though: no NEW writer may begin even while the barrier
	// is still being crossed.
	if b.mayWrite() {
		t.Fatalf("output was still permitted while certification was in progress")
	}

	close(release)
	wg.Wait()
	select {
	case cert := <-done:
		if !mqttBindingCertifies(b, cert) {
			t.Fatalf("certification produced a certificate for another binding")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("certification never completed after the writer released the lock")
	}
}

// TestF105ConcurrentCertifiersAllWaitForTheFact pins that a caller which loses the seal race
// still does not return before the certificate is true. Otherwise the loser could act on
// "someone is sealing" while a write was still in flight.
func TestF105ConcurrentCertifiersAllWaitForTheFact(t *testing.T) {
	c := &client{}
	b := mqttNewConnBinding("run-1", "conn-1")

	held := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.wmu.Lock()
		close(held)
		<-release
		c.wmu.Unlock()
	}()
	<-held

	const callers = 4
	done := make(chan *mqttCloseCert, callers)
	for range callers {
		go func() { done <- mqttCertifyClosed(c, b, "race") }()
	}
	select {
	case <-done:
		t.Fatalf("a certifier returned while a writer held the write lock")
	case <-time.After(150 * time.Millisecond):
	}

	close(release)
	wg.Wait()
	for i := range callers {
		select {
		case cert := <-done:
			if cert == nil {
				t.Fatalf("certifier %d returned no certificate", i)
			}
			if !mqttBindingCertifies(b, cert) {
				t.Fatalf("certifier %d returned a certificate for another binding", i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("certifier %d never returned", i)
		}
	}
}

// TestF105CertificationClosesTheTransportOutsideTheWriteLock is the ordering test that needs
// a writer genuinely blocked INSIDE a socket write, which no lock-holding fixture can model.
//
// net.Pipe is synchronous and unbuffered, so a Write blocks until the peer reads and returns
// only when the connection is closed. That is the production shape: writeSocket holds wmu
// for the whole of its Write, so closing the transport under that lock would deadlock
// against the very writer being stopped, and certification would never complete.
//
// The assertion is a bounded wait rather than an unbounded one precisely because the failure
// mode is a hang.
func TestF105CertificationClosesTheTransportOutsideTheWriteLock(t *testing.T) {
	srv, _ := net.Pipe() // no reader on the peer: a Write blocks until Close
	c := &client{nc: srv, rawConn: srv}
	b := mqttNewConnBinding("run-1", "conn-1")

	writing := make(chan struct{})
	wrote := make(chan error, 1)
	go func() {
		close(writing)
		// Blocks inside c.nc.Write holding wmu, exactly as a real slow consumer does.
		wrote <- c.writeSocket([]byte("a blocked frame"), time.Time{})
	}()
	<-writing
	// Give the writer time to enter Write and take wmu.
	time.Sleep(100 * time.Millisecond)

	done := make(chan *mqttCloseCert, 1)
	go func() { done <- mqttCertifyClosed(c, b, "blocked-writer") }()

	select {
	case cert := <-done:
		if !mqttBindingCertifies(b, cert) {
			t.Fatalf("certification produced a certificate for another binding")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("certification never completed against a writer blocked in a socket write: " +
			"the transport must be closed OUTSIDE the write lock, or closing deadlocks " +
			"against the writer it is meant to stop")
	}

	select {
	case err := <-wrote:
		if err == nil {
			t.Fatalf("the blocked write reported success after its transport was closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the blocked write never returned after certification closed the transport")
	}
}

// TestF105EveryConnectionGetsItsOwnBinding pins that the binding names the connection it
// belongs to.
//
// It caught a real defect: the mint was first placed at the client struct literal, where the
// cid has not been assigned yet, so every client on the broker shared the binding {run, "0"}.
// Sealing one would then have reported every other connection as unable to write.
func TestF105EveryConnectionGetsItsOwnBinding(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	seen := map[string]string{}
	for _, id := range []string{"bind-a", "bind-b", "bind-c"} {
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

		mc := testMQTTGetClient(t, s, id)
		if mc.outBind == nil {
			t.Fatalf("client %q has no output binding", id)
		}
		if mc.outBind.conn == "0" {
			t.Fatalf("client %q was bound before its connection id existed", id)
		}
		if mc.outBind.run != mqttBrokerRunID {
			t.Fatalf("client %q is bound to run %q, not this run %q", id, mc.outBind.run, mqttBrokerRunID)
		}
		if prev, dup := seen[mc.outBind.conn]; dup {
			t.Fatalf("clients %q and %q share the binding %q: sealing one would report the "+
				"other unable to write", prev, id, mc.outBind.conn)
		}
		seen[mc.outBind.conn] = id
	}
}

// TestF105SealedGateStopsRealOutput pins that the gate is consulted by the actual writer,
// not only by the tests. Until this, mayWrite had no production caller and the claim that
// writeSocket checks it under wmu was unverified.
func TestF105SealedGateStopsRealOutput(t *testing.T) {
	srv, peer := net.Pipe()
	defer peer.Close()
	c := &client{nc: srv, rawConn: srv, outBind: mqttBindConnection(42)}

	// Sealed: the write must not reach the transport at all. With an unsealed gate this
	// same call would block on the unbuffered pipe, which is what makes the assertion mean
	// something.
	c.outBind.gate.seal()
	done := make(chan error, 1)
	go func() { done <- c.writeSocket([]byte("must not be written"), time.Time{}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a write through a sealed gate reported an error rather than simply "+
				"not happening: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("a write through a sealed gate reached the transport and blocked on it")
	}
}
