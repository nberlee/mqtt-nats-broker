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
	"strings"
	"sync/atomic"
	"testing"
)

// A write-boundary refusal is marked stale exactly when the store's CAS on the same member would
// have refused, and where the member stands is compared before anything the standing record holds
// (errMQTTStaleExpectation). Driven DIRECTLY at the checks, so the store's CAS cannot supply the
// refusal, and on a stationary store, so each case differs from its control in one record.

// testWriteLedgerRaw stores body on the generation's ledger subject at whatever stands there,
// bypassing the write boundary, and returns the sequence it landed at.
func testWriteLedgerRaw(t *testing.T, e *testOutboundEnv, body []byte) uint64 {
	t.Helper()
	subj := mqttGenLedgerSubject(getHash(e.id), testGen)
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, subj)
	if err != nil || smsg == nil {
		t.Fatalf("reading the ledger before rewriting it: %v", err)
	}
	msg, hdr := mqttFrameBatchWrite(smsg.Sequence, body, _EMPTY_, 0, false)
	resp, serr := e.asm.jsa.storeMsg(subj, hdr, msg)
	if serr != nil {
		t.Fatalf("rewriting the ledger: %v", serr)
	}
	return resp.Sequence
}

// TestBoundaryMarksOnlyTheMovedLedgerExpectation: the allocation's ledger check compares the
// member's expectation first. A moved ledger is refused marked whatever it now holds — another
// floor or watermark, an undecodable body — and an undecodable ledger at exactly the expected
// sequence is refused unmarked.
func TestBoundaryMarksOnlyTheMovedLedgerExpectation(t *testing.T) {
	e, done := testNewEnv(t, "stale-ledger-expect")
	defer done()
	e.bootstrap(t, "txn-1")
	if err := testSubmitInventory(t, e, "txn-one",
		testLedgerMember(t, e, 0, testInventoryLedger(e, mqttGenLedgerOpen, 1)),
		testDutyMember(t, e, 0, testDuty(e, 1, mqttDutyProvision, "inc-xa", mqttDutyPending)),
	); err != nil {
		t.Fatalf("the allocation was refused: %v", err)
	}
	led, s0, lerr := e.asm.jsa.loadGenLedger(e.id, e.epoch, testGen)
	if lerr != nil || led == nil {
		t.Fatalf("the ledger does not load back: %v", lerr)
	}
	attempt := &mqttTransitionAttempt{ID: e.id, Epoch: e.epoch, Txn: "txn-direct", Op: "op-direct"}
	check := func(expect uint64) error {
		return e.asm.jsa.checkInventoryMembers(attempt, []mqttTransitionMember{testLedgerMember(t, e, expect, led)})
	}

	// THE POSITIVE CONTROL: the ledger at its standing sequence, unchanged, validates.
	if err := check(s0); err != nil {
		t.Fatalf("a ledger member at the standing sequence was refused: %v", err)
	}

	// MOVED, SAME BODY: the production witness, a ledger another writer committed in between.
	body, _ := json.Marshal(led)
	s1 := testWriteLedgerRaw(t, e, body)
	if err := check(s0); err == nil || !mqttIsStaleExpectation(err) ||
		!strings.Contains(err.Error(), "high-water that has moved") {
		t.Fatalf("a ledger that moved under its expectation was refused as %v, want the marked moved-ledger refusal", err)
	}

	// MOVED TO ANOTHER WATERMARK: refused as moved, not as a floor or watermark the batch never saw.
	moved := *led
	moved.Lo, moved.Del = 2, 2
	mbody, _ := json.Marshal(&moved)
	s2 := testWriteLedgerRaw(t, e, mbody)
	if err := check(s1); err == nil || !mqttIsStaleExpectation(err) || strings.Contains(err.Error(), "watermark") {
		t.Fatalf("a ledger moved to another floor was refused as %v, want the marked moved-ledger refusal", err)
	}

	// AN UNDECODABLE BODY: at a moved sequence it is the moved expectation; at exactly the
	// expected sequence it is a ledger this broker cannot interpret, which is not contention.
	s3 := testWriteLedgerRaw(t, e, []byte(`{"not":"a ledger"`))
	if err := check(s2); err == nil || !mqttIsStaleExpectation(err) {
		t.Fatalf("an undecodable ledger at a moved sequence was refused as %v, want marked", err)
	}
	if err := check(s3); err == nil || mqttIsStaleExpectation(err) {
		t.Fatalf("an undecodable ledger at the expected sequence was refused as %v, want an unmarked refusal", err)
	}
}

// TestIdentifierCensusComparesTheLedgerBeforeItsFailure: the identifier census reports a ledger
// that moved under the batch's expectation as moved, even when the moved ledger cannot be
// interpreted and the walk therefore fails.
func TestIdentifierCensusComparesTheLedgerBeforeItsFailure(t *testing.T) {
	e, done := testNewEnv(t, "stale-census-ledger")
	defer done()
	auth, authSeq := e.bootstrap(t, "txn-boot")
	first, err := mqttAdmitPublishTransition(testAdmission(e, auth, authSeq, "txn-first", 7, nil))
	if err != nil {
		t.Fatalf("assembling the first admission: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(first); err != nil {
		t.Fatalf("the first admission was refused: %v", err)
	}
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	second, err := mqttAdmitPublishTransition(testAdmission(e, al.rec, al.seq, "txn-second", 8, nil))
	if err != nil {
		t.Fatalf("assembling the second admission: %v", err)
	}

	// THE POSITIVE CONTROL: the census over the ledger the admission was composed against.
	if err := e.asm.jsa.checkIdentifierExclusivity(second.Attempt, second.Extra); err != nil {
		t.Fatalf("the census over the standing ledger refused: %v", err)
	}
	testWriteLedgerRaw(t, e, []byte(`{"not":"a ledger"`))
	if err := e.asm.jsa.checkIdentifierExclusivity(second.Attempt, second.Extra); err == nil ||
		!mqttIsStaleExpectation(err) {
		t.Fatalf("the census over an undecodable ledger at a moved sequence refused as %v, want marked", err)
	}
}

// TestIdentifierCensusExhaustionIsNotContention: an identifier walk the floor keeps crossing gives
// up after its restarts, and that is reported as an unfinished walk, unmarked, even though the
// ledger its last pass read is not the one the batch expects: the walk did not finish, so it is
// not the store's CAS refusal taken early.
//
// In two halves, because the identifier walk reads with no session manager and so has no seam to
// cross it mid-pass: the traversal's exhaustion is driven for real through the census, which
// shares the restart loop, and the boundary's decision over a walk is driven directly.
func TestIdentifierCensusExhaustionIsNotContention(t *testing.T) {
	e, done := testFloorEnv(t, "stale-census-exhausted")
	defer done()
	jsa := &e.asm.jsa
	// ENDED OBLIGATIONS, one more than the restarts, then one kept: every pass reads them, and a
	// floor advanced one position after each duty read crosses the pass once.
	for i := 0; i <= mqttTraversalRestarts+1; i++ {
		testEndAndResolve(t, e, testFloorAdmit(t, e, uint16(10+i)))
	}
	testFloorAdmit(t, e, 2)

	var advances atomic.Int32
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		resps, err := e.asm.jsa.loadLastMsgForMulti(mqttSessStreamName, subjects)
		if len(subjects) > 0 && mqttIsDutySubject(subjects[0]) {
			led, _ := testFloorLedger(t, e)
			testAdvanceTo(t, e, led.Lo+1)
			if now, _ := testFloorLedger(t, e); now.Lo > led.Lo {
				advances.Add(1)
			}
		}
		return resps, err
	}
	e.asm.mu.Unlock()
	census, err := mqttTraverseGenerationInventory(jsa, e.asm, e.id, e.epoch, testGen, nil)
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = nil
	e.asm.mu.Unlock()
	if advances.Load() <= int32(mqttTraversalRestarts) {
		t.Fatalf("fixture: the floor crossed the traversal %d time(s), want more than its %d restarts",
			advances.Load(), mqttTraversalRestarts)
	}
	if err == nil || !errors.Is(err, errMQTTTraversalExhausted) || census == nil {
		t.Fatalf("the traversal did not report its exhausted restarts as such: %v", err)
	}

	// THE BOUNDARY'S DECISION over such a walk: the ledger of its last pass is not the batch's,
	// and still nothing is marked.
	_, expect := testFloorLedger(t, e)
	walk := &mqttIdentifierWalk{ledgerSeq: census.ledgerSeq}
	if mqttIdentifierWalkMovedFrom(walk, err, census.ledgerSeq+1) {
		t.Fatalf("an exhausted walk was judged a moved ledger")
	}
	// THE CONTROLS: the same walk finished, or failed otherwise, over a moved ledger is moved;
	// over the expected ledger, or with no observation at all, it is not.
	if !mqttIdentifierWalkMovedFrom(walk, nil, census.ledgerSeq+1) ||
		!mqttIdentifierWalkMovedFrom(walk, errors.New("an undecodable ledger"), census.ledgerSeq+1) {
		t.Fatalf("a finished or failed walk over a moved ledger was not judged moved")
	}
	if mqttIdentifierWalkMovedFrom(walk, nil, census.ledgerSeq) || mqttIdentifierWalkMovedFrom(nil, err, expect) {
		t.Fatalf("a walk over the expected ledger, or with no observation, was judged moved")
	}
}
