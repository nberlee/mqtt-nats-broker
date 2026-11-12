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

// Delayed-Will outbox fixtures shared by the tagged and untagged Will suites.

package mqtt

import (
	"sync/atomic"
	"testing"
	"time"
)

// testWillFor builds a valid delayed-Will outbox record for a client.
func testWillFor(asm *mqttAccountSessionManager, clientID, willID string, fireAt int64, originSeq uint64) *mqttWillOutbox {
	return &mqttWillOutbox{
		WillID:           willID,
		ClientIDHash:     getHash(clientID),
		ClientID:         clientID,
		Gen:              mqttNewSessGen(),
		Topic:            "will/fence",
		Msg:              []byte(willID),
		FireAt:           fireAt,
		State:            mqttWillPending,
		OriginSessionSeq: originSeq,
		OriginSessEpoch:  asm.sessStreamEpoch,
	}
}

// testSeedWill stores a record on its own subject and returns both.
func testSeedWill(t testing.TB, asm *mqttAccountSessionManager, rec *mqttWillOutbox) (string, uint64) {
	t.Helper()
	subject := mqttWillQSubject(asm.domainTk, rec.ClientIDHash, rec.WillID)
	seq, err := asm.jsa.storeWillOutboxCAS(subject, 0, rec)
	if err != nil {
		t.Fatalf("seed the outbox record: %v", err)
	}
	return subject, seq
}

// testCapturePublishes replaces the delayed-Will publication with a counter, so a
// test asserts on the EXTERNAL act rather than on whatever the record ended up
// saying. Returns the counter.
func testCapturePublishes(t testing.TB, asm *mqttAccountSessionManager) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	asm.mu.Lock()
	asm.testPublishWill = func(*mqttWillOutbox) error {
		n.Add(1)
		return nil
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testPublishWill = nil
		asm.mu.Unlock()
	})
	return &n
}

// testReloadWill returns the current record on a subject.
func testReloadWill(t testing.TB, asm *mqttAccountSessionManager, subject string) (*mqttWillOutbox, uint64) {
	t.Helper()
	rec, seq, err := asm.jsa.loadWillOutbox(asm.domainTk, subject)
	if err != nil {
		t.Fatalf("reload the outbox record on %q: %v", subject, err)
	}
	return rec, seq
}

// testLicenseWill puts a record into a LICENSED shape: the decision and the cutoff
// it was taken under, alongside the state. The legal matrix refuses a terminal
// record that carries no decision, so a fixture that only set the state would be
// refused at decode rather than acted on.
func testLicenseWill(rec *mqttWillOutbox, state mqttWillState, dec mqttWillDecision, cutoff uint64) {
	rec.State = state
	rec.Decision = dec
	c := cutoff
	rec.DecisionCutoff = &c
}

// testFarFuture is a FireAt no scan will ever call due.
func testFarFuture() int64 { return time.Now().Add(time.Hour).UnixNano() }

// testShortenWillLease shortens ONE account manager's claim lease (and with it the
// derived renewal cadence and scan budget).
//
// Instance-scoped deliberately: the package variable is read by every live claim
// loop on its own goroutine, so a test that assigned it would be racing every other
// test's broker rather than configuring its own.
func testShortenWillLease(t testing.TB, asm *mqttAccountSessionManager, lease time.Duration) func() {
	t.Helper()
	asm.mu.Lock()
	prev := asm.testClaimLease
	asm.testClaimLease = lease
	asm.mu.Unlock()
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		asm.mu.Lock()
		asm.testClaimLease = prev
		asm.mu.Unlock()
	}
	t.Cleanup(restore)
	return restore
}

// withLongWillScan pins the claim-scan interval far above any test's duration, so no
// tick falls due while the test runs - a time-bound assumption (30s), not a disabled
// loop. It must be called before the manager starts: the loop samples the interval
// once, after its initial pass, so changing it later leaves a tick phase in the test.
// Pair it with testAwaitInitialWillScan, which covers the pass that runs before the
// ticker exists.
//
// It also pins the lease above the interval, as production does; a test that needs a
// short lease sets its own manager's with testShortenWillLease, which takes precedence.
//
// mqttWillClaimInterval and mqttWillClaimLease are package globals (the tagged build's
// init() drops them to 50ms and 2s), so a caller must not be a parallel test. The
// parallel tests in this package run after every serial one and never call it.
func withLongWillScan(t testing.TB) {
	t.Helper()
	prevInterval, prevLease := mqttWillClaimInterval.Get(), mqttWillClaimLease.Get()
	mqttWillClaimInterval.Set(30 * time.Second)
	mqttWillClaimLease.Set(60 * time.Second)
	t.Cleanup(func() {
		mqttWillClaimInterval.Set(prevInterval)
		mqttWillClaimLease.Set(prevLease)
	})
}
