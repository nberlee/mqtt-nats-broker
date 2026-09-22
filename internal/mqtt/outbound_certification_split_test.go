//go:build mqttport_fulltests

// Copyright 2026 The mqtt-nats-bridge Authors
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
	"sync"
	"testing"
	"time"
)

// TestF105SealedGateStillCertifies is the regression for the certificate-completion deadlock
// (plan §4.4). A torn writer seals the gate under wmu via sealOutput and NOTHING else — it may
// crash, or simply not have reached its own completeCertification yet. An external certifier
// (a teardown) meeting that already-sealed gate must still COMPLETE the certificate, not block
// forever waiting for a completion the bare seal never owed. The old mqttCertifyClosed hung on
// <-b.established here.
func TestF105SealedGateStillCertifies(t *testing.T) {
	c := &client{}
	b := mqttNewConnBinding("run-1", "conn-1")

	// The writer's half, in isolation: seal, and stop (no completion).
	sealOutput(b)
	if b.mayWrite() {
		t.Fatalf("sealOutput did not withdraw output permission")
	}

	done := make(chan *mqttCloseCert, 1)
	go func() { done <- mqttCertifyClosed(c, b, "external") }()

	select {
	case cert := <-done:
		if cert == nil || !mqttBindingCertifies(b, cert) {
			t.Fatalf("external certifier produced no certificate for its binding: %+v", cert)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("external certifier hung on a gate a bare sealOutput had sealed: this is the " +
			"deadlock the seal/complete split removes")
	}
	if b.closeCert() == nil {
		t.Fatalf("no certificate was recorded")
	}
}

// TestF105WriterAndExternalCertifierConverge pins that the torn-writer completion and an
// external certifier converge on ONE certificate rather than one blocking on the other:
// whichever crosses the barrier first records it, the other adopts the same one.
func TestF105WriterAndExternalCertifierConverge(t *testing.T) {
	c := &client{}
	b := mqttNewConnBinding("run-1", "conn-1")

	sealOutput(b) // the writer sealed under wmu; both callers now complete after the barrier

	var wg sync.WaitGroup
	certs := make([]*mqttCloseCert, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		mqttCrossWriterBarrier(c)
		certs[0] = completeCertification(b, "writer")
	}()
	go func() {
		defer wg.Done()
		certs[1] = mqttCertifyClosed(c, b, "external")
	}()

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatalf("a certifier never returned: the two must converge, not wait on each other")
	}

	if certs[0] == nil || certs[1] == nil {
		t.Fatalf("a certifier returned no certificate: %+v %+v", certs[0], certs[1])
	}
	// EXACTLY ONE record: completeCertification returns the standing certificate under the
	// lock, so both callers hold the very same pointer.
	if certs[0] != certs[1] {
		t.Fatalf("the two certifiers recorded different certificates (%p vs %p): exactly one "+
			"must be established", certs[0], certs[1])
	}
	if certs[0] != b.closeCert() {
		t.Fatalf("the returned certificate is not the one standing on the binding")
	}
}

// TestF105CompleteCertificationIsIdempotent pins that a second completion neither replaces the
// certificate nor double-closes `established`.
func TestF105CompleteCertificationIsIdempotent(t *testing.T) {
	b := mqttNewConnBinding("run-1", "conn-1")

	first := completeCertification(b, "first")
	if first == nil || first.Reason != "first" {
		t.Fatalf("first completion did not record its certificate: %+v", first)
	}
	// `established` must be closed once a certificate stands.
	select {
	case <-b.established:
	default:
		t.Fatalf("established was not closed when the certificate was recorded")
	}

	second := completeCertification(b, "second")
	if second != first {
		t.Fatalf("a second completion replaced the certificate (%p -> %p)", first, second)
	}
	if b.closeCert().Reason != "first" {
		t.Fatalf("a second completion overwrote the recorded reason to %q", b.closeCert().Reason)
	}
}
