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
	"testing"
	"time"
)

// Fixtures that put a receipted generation attempt into the state a production caller would leave
// it in the run's receipt ledger (receipt_ledger.go): stamped before its handle is frozen, and a
// reply lost in transit recorded as a sent, ambiguous submission rather than as an attempt this
// run never sent.
// testLoseSubmission records in the run's receipt ledger what a send whose reply was lost leaves
// there: the submission registered, then returned ambiguous, so the attempt stays undecided with
// its fence raised and only the store can decide it. It is for a commit seam that reports a lost
// reply WITHOUT calling send: a seam that returns before send otherwise leaves an attempt this run
// never sent, which the ledger settles as never landed. Nothing for an attempt the ledger does not
// track.
func testLoseSubmission(t *testing.T, jsa *mqttJSA, tr *mqttAuthorityTransition) {
	t.Helper()
	l := jsa.receipts()
	rec := l.record(tr.Attempt)
	if rec == nil {
		return
	}
	if err := l.register(rec, tr.PrevSeq); err != nil {
		t.Errorf("registering the lost submission of %q: %v", tr.Attempt.Txn, err)
		return
	}
	l.returned(rec, true, mqttSubmissionAmbiguous, tr.PrevSeq, 0)
}

// testLoseAcknowledgement parks the reply to the next batch PUBLISHED to subject (a batch's reply
// is its commit member's) and never delivers it, so a send made while it is armed lands but times
// out, and the ledger records an ambiguous submission exactly as for a reply lost in transit. A
// read of the same record is published to the message-get API and is not parked. The returned
// function disarms it; the parked reply is never delivered.
func testLoseAcknowledgement(t *testing.T, jsa *mqttJSA, subject string) (disarm func()) {
	t.Helper()
	obs := testObserveJSA(t, jsa)
	h := newMQTTJSAReplyHold(func(reply string) bool {
		obs.mu.Lock()
		defer obs.mu.Unlock()
		return obs.api[reply] == subject
	})
	jsa.testHoldReply.Store(h)
	return func() { jsa.testHoldReply.CompareAndSwap(h, nil) }
}

// testFrozenHandle is commitOp without the send: the attempt stamped into its receipt batch in
// place, then its handle frozen over the stamped scope and pointed at the attempt's one record,
// as every production handle of a receipted generation attempt is.
func testFrozenHandle(t *testing.T, jsa *mqttJSA, tr *mqttAuthorityTransition) *mqttTransitionHandle {
	t.Helper()
	rec, err := jsa.receipts().prepare(tr)
	if err != nil {
		t.Fatalf("stamping transition %q: %v", tr.Attempt.Txn, err)
	}
	h, err := mqttNewTransitionHandle(tr)
	if err != nil {
		t.Fatalf("freezing the handle of %q: %v", tr.Attempt.Txn, err)
	}
	h.rec = rec
	return h
}

// testCommitLosingAck commits tr through commitOp, the production path, while the batch's
// acknowledgement (its commit member is the authority) is parked: the batch lands in the store, the
// send's wait is cut by a short ownership deadline, and the ledger records an ambiguous submission.
// The returned handle carries its record, undecided, so its first resolution decides it from the
// store. tr must write the authority.
func testCommitLosingAck(t *testing.T, jsa *mqttJSA, tr *mqttAuthorityTransition) *mqttTransitionHandle {
	t.Helper()
	a := tr.Attempt
	disarm := testLoseAcknowledgement(t, jsa, mqttOutboundAuthoritySubject(getHash(a.ID)))
	out, err := jsa.withDeadline(time.Now().Add(500 * time.Millisecond)).commitOp(tr)
	disarm()
	if err == nil || mqttClassifySubmission(err) != mqttSubmissionAmbiguous {
		t.Fatalf("fixture: the commit of %q did not end ambiguous: %v", a.Txn, err)
	}
	if out.Handle == nil || out.Handle.rec == nil {
		t.Fatalf("fixture: the commit of %q returned no handle on its record", a.Txn)
	}
	if v, decided := jsa.receipts().verdict(out.Handle.rec); decided {
		t.Fatalf("fixture: the commit of %q is already decided %v", a.Txn, v)
	}
	if al := jsa.loadOutboundAuthority(a.ID, a.Epoch); al.status != mqttOutboundValid || al.rec.Txn != a.Txn {
		t.Fatalf("fixture: the commit of %q did not land", a.Txn)
	}
	return out.Handle
}
