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
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// The receipt ledger's state machine against a scripted store (issue #192). The ledger is the real
// one; the store is a model of what this design relies on: one authority sequence every receipted
// submission CASes, one create-only outcome subject per attempt, a queue of sent batches that apply,
// are refused, or wait, and acknowledgements the network may drop. The driver step is the model of
// driveReceipt's order (resolve first; abort while undecided at the fence; a progress write once
// decided at the fence), and the resolver applies main's rules to the model.

// receiptModelSubmission is one sent batch the model store has not answered.
type receiptModelSubmission struct {
	rec      *mqttReceiptAttempt
	commit   bool
	exp      uint64
	dropAck  bool
	answered bool
}

// receiptModel is the scripted store and the history it records.
type receiptModel struct {
	t       *testing.T
	l       *mqttReceiptLedger
	seq     uint64
	outcome map[string]string // txn -> "committed" | "aborted"
	queue   []*receiptModelSubmission
	purged  map[string]bool // batch token -> purged
	recs    []*mqttReceiptAttempt
	gen     string
	retired bool
	now     time.Time
}

func newReceiptModel(t *testing.T) *receiptModel {
	m := &receiptModel{
		t: t, l: newMQTTReceiptLedger(), seq: 10, outcome: map[string]string{}, purged: map[string]bool{},
		gen: testGen, now: time.Unix(1_000_000, 0),
	}
	m.l.now = func() time.Time { return m.now }
	return m
}

// prepare stamps a fresh receipted attempt over the authority as it stands now.
func (m *receiptModel) prepare(txn string) (*mqttReceiptAttempt, error) {
	a := &mqttTransitionAttempt{
		ID: testID, Epoch: testEpoch, Txn: txn, Scope: m.gen, Op: "op-" + txn,
		Kind: mqttTransitionAdmitExchange, PrevTxn: "own-0", Rev: 2, Gen: m.gen, Tenure: "tenure-0", OwnerTxn: "own-0",
	}
	auth := &mqttOutboundAuthority{ID: testID, Epoch: testEpoch, Gen: m.gen, Tenure: "tenure-0", State: mqttAuthorityServing,
		Rev: 2, Txn: txn, OwnerTxn: "own-0"}
	rec, err := m.l.prepare(&mqttAuthorityTransition{Attempt: a, Prev: auth, PrevSeq: m.seq, Auth: auth})
	if err == nil {
		m.recs = append(m.recs, rec)
	}
	return rec, err
}

// send registers and queues one submission of rec at the current authority sequence (an abort) or
// at its own expectation (the commit).
func (m *receiptModel) send(rec *mqttReceiptAttempt, commit, dropAck bool) bool {
	exp := m.seq
	if commit {
		exp = rec.authSeq
	}
	if m.purged[rec.batch.token] {
		m.t.Fatalf("a submission of %q was sent into purged batch %q", rec.attempt.Txn, rec.batch.token)
	}
	if err := m.l.register(rec, exp); err != nil {
		if commit {
			m.l.unsent(rec)
		}
		return false
	}
	m.queue = append(m.queue, &receiptModelSubmission{rec: rec, commit: commit, exp: exp, dropAck: dropAck})
	return true
}

// apply lets the store answer queued submission i: applied when the authority stands at its
// expectation and the outcome subject is free, refused otherwise.
func (m *receiptModel) apply(i int) {
	s := m.queue[i]
	m.queue = append(m.queue[:i], m.queue[i+1:]...)
	txn := s.rec.attempt.Txn
	landed := s.exp == m.seq && m.outcome[txn] == "" && !m.retired
	if landed {
		if m.purged[s.rec.batch.token] {
			m.t.Fatalf("P1: a submission of %q landed in purged batch %q", txn, s.rec.batch.token)
		}
		m.seq++
		if s.commit {
			m.outcome[txn] = "committed"
		} else {
			m.outcome[txn] = "aborted"
		}
	}
	if s.answered {
		return
	}
	switch {
	case s.dropAck:
		m.l.returned(s.rec, s.commit, mqttSubmissionAmbiguous, s.exp, 0)
	case landed:
		m.l.returned(s.rec, s.commit, mqttSubmissionLanded, s.exp, m.seq)
	default:
		m.l.returned(s.rec, s.commit, mqttSubmissionRefused, s.exp, 0)
	}
}

// timeout answers queued submission i as ambiguous while leaving it queued: it may still apply.
func (m *receiptModel) timeout(i int) {
	s := m.queue[i]
	if s.answered {
		return
	}
	s.answered = true
	m.l.returned(s.rec, s.commit, mqttSubmissionAmbiguous, s.exp, 0)
}

// write is an unrelated authority write.
func (m *receiptModel) write() { m.seq++ }

// resolve is main's resolution over the model, returned through the record's cell.
func (m *receiptModel) resolve(rec *mqttReceiptAttempt) mqttTransitionVerdict {
	if v, ok := m.l.verdict(rec); ok {
		return v
	}
	txn := rec.attempt.Txn
	v := mqttTransitionUnresolved
	switch {
	case m.retired:
		v = mqttTransitionRetired
	case m.outcome[txn] == "committed":
		v = mqttTransitionLanded
	case m.outcome[txn] == "aborted":
		v = mqttTransitionNeverLanded
	case m.seq > rec.authSeq && !m.purged[rec.batch.token]:
		// provedNeverLandedBySequence: authority past the expectation, the receipt absent, the
		// scope live.
		v = mqttTransitionNeverLanded
	}
	if v == mqttTransitionRetired {
		m.l.observe(rec, 0, true)
	}
	return m.l.decide(rec, v)
}

// driverPass is driveReceipt's order over every due record, then the purges.
func (m *receiptModel) driverPass(healthy bool) {
	w, _ := m.l.work(testEpoch, 1<<20)
	for _, rec := range w.settle {
		m.resolve(rec)
		m.l.observe(rec, m.seq, m.retired)
		m.l.mu.Lock()
		settled, decided, fence := rec.settled, rec.decided, rec.fence
		m.l.mu.Unlock()
		if settled || m.seq > fence {
			continue
		}
		if !decided {
			m.send(rec, false, false)
		} else {
			// The progress write: the authority alone, at the sequence read.
			m.seq++
			m.l.observe(rec, m.seq, false)
		}
		if healthy {
			for len(m.queue) > 0 {
				m.apply(0)
			}
		}
	}
	for _, b := range w.purge {
		if m.l.beginPurge(b) {
			for _, s := range m.queue {
				if s.rec.batch == b && s.exp >= m.seq {
					m.t.Fatalf("P1: batch %q purged while a submission of %q at %d can still land (authority %d)",
						b.token, s.rec.attempt.Txn, s.exp, m.seq)
				}
			}
			m.purged[b.token] = true
			m.l.forget(b)
		}
	}
}

// checkVerdicts is P2: every decided verdict is the true history.
func (m *receiptModel) checkVerdicts() {
	for _, rec := range m.recs {
		v, ok := m.l.verdict(rec)
		if !ok {
			continue
		}
		truth := m.outcome[rec.attempt.Txn]
		switch {
		case v == mqttTransitionRetired:
		case v == mqttTransitionLanded && truth != "committed":
			m.t.Fatalf("P2: %q decided Landed, but its commit never applied", rec.attempt.Txn)
		case v == mqttTransitionNeverLanded && truth == "committed":
			m.t.Fatalf("P2: %q decided NeverLanded, but its commit applied", rec.attempt.Txn)
		}
	}
}

// TestReceiptLedgerPropertiesHoldOverGeneratedSchedules drives the ledger through generated
// schedules of commits, delayed and lost submissions, unrelated writes, holder resolutions, driver
// passes and the clock, then a healthy store, and checks: P1 no purge while a submission into the
// batch can land, P2 every verdict is the true history, P3 every record settles and every batch is
// purged once the store is healthy, P4 the budget equals the attempts in unpurged batches.
func TestReceiptLedgerPropertiesHoldOverGeneratedSchedules(t *testing.T) {
	for seed := int64(0); seed < 400; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			r := rand.New(rand.NewSource(seed))
			m := newReceiptModel(t)
			for step := 0; step < 200; step++ {
				switch r.Intn(8) {
				case 0, 1:
					if rec, err := m.prepare(fmt.Sprintf("txn-%d", step)); err == nil {
						m.send(rec, true, r.Intn(4) == 0)
					}
				case 2:
					if len(m.queue) > 0 {
						m.apply(r.Intn(len(m.queue)))
					}
				case 3:
					if len(m.queue) > 0 {
						m.timeout(r.Intn(len(m.queue)))
					}
				case 4:
					m.write()
				case 5:
					if len(m.recs) > 0 {
						m.resolve(m.recs[r.Intn(len(m.recs))])
					}
				case 6:
					m.driverPass(false)
				case 7:
					m.now = m.now.Add(time.Duration(r.Intn(15)) * time.Second)
				}
				m.checkVerdicts()
			}
			// THE STORE RECOVERS: every queued submission answers, the clock passes every batch's
			// age, and the driver runs with no unrelated write.
			for len(m.queue) > 0 {
				m.apply(0)
			}
			m.now = m.now.Add(time.Hour)
			for i := 0; i < 50; i++ {
				m.now = m.now.Add(mqttReceiptRetryMax)
				m.driverPass(true)
			}
			m.checkVerdicts()
			if batches, unsettled := m.l.pending(testEpoch); batches != 0 || unsettled != 0 {
				t.Fatalf("P3: after recovery %d batch(es) and %d unsettled record(s) remain", batches, unsettled)
			}
			for _, ks := range m.l.keys {
				t.Fatalf("P4: a forgotten generation still charges %d attempt(s)", ks.charged)
			}
		})
	}
}

// TestALaterAbortInvalidatesOlderFenceEvidence is plan round 1's first schedule: evidence read past
// an ambiguous commit's fence does not cover an abort registered afterwards at a newer expectation.
func TestALaterAbortInvalidatesOlderFenceEvidence(t *testing.T) {
	m := newReceiptModel(t)
	rec, err := m.prepare("txn-1")
	if err != nil {
		t.Fatal(err)
	}
	m.send(rec, true, false)
	m.timeout(0)
	m.write()
	m.l.observe(rec, m.seq, false)
	// The holder aborts at the newer expectation; the abort's answer is lost and it stays queued.
	m.send(rec, false, false)
	m.timeout(1)
	// The holder decides NeverLanded by the sequence proof, and the driver reads the authority
	// still standing at the abort's expectation.
	if v := m.resolve(rec); v != mqttTransitionNeverLanded {
		t.Fatalf("resolution: %v", v)
	}
	m.l.observe(rec, m.seq, false)
	m.l.mu.Lock()
	settled := rec.settled
	m.l.mu.Unlock()
	if settled {
		t.Fatalf("the record settled on evidence older than its abort's expectation: the abort could still land after the purge")
	}
}

// TestTheDriverResolvesBeforeItAbortsAgain is plan round 1's second schedule: a delayed commit lands
// under the driver's abort, which is refused create-only; the next pass decides from the receipt
// and settles by a progress write, with no further abort.
func TestTheDriverResolvesBeforeItAbortsAgain(t *testing.T) {
	m := newReceiptModel(t)
	rec, err := m.prepare("txn-1")
	if err != nil {
		t.Fatal(err)
	}
	m.send(rec, true, false)
	m.timeout(0)
	m.now = m.now.Add(time.Hour)
	m.driverPass(false) // resolves Unresolved, sends the abort at the commit's own expectation
	if len(m.queue) != 2 {
		t.Fatalf("the driver did not abort the undecided attempt: %d queued", len(m.queue))
	}
	m.apply(0) // the delayed commit lands
	m.apply(0) // the abort is refused: its expectation was taken by the commit
	m.now = m.now.Add(time.Hour)
	m.driverPass(true)
	if v, _ := m.l.verdict(rec); v != mqttTransitionLanded {
		t.Fatalf("verdict %v, want Landed", v)
	}
	m.l.mu.Lock()
	settled, sends := rec.settled, rec.sends
	m.l.mu.Unlock()
	if !settled || sends != 2 {
		t.Fatalf("settled %v after %d submissions: the driver should settle with no further abort", settled, sends)
	}
}

// TestARefusedAbortDecidesNothing pins that only an abort that LANDED decides NeverLanded.
func TestARefusedAbortDecidesNothing(t *testing.T) {
	l := newMQTTReceiptLedger()
	m := &receiptModel{t: t, l: l, seq: 10, outcome: map[string]string{}, purged: map[string]bool{}, gen: testGen, now: time.Now()}
	rec, err := m.prepare("txn-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.register(rec, rec.authSeq); err != nil {
		t.Fatal(err)
	}
	l.returned(rec, true, mqttSubmissionAmbiguous, rec.authSeq, 0)
	if err := l.register(rec, rec.authSeq); err != nil {
		t.Fatal(err)
	}
	l.returned(rec, false, mqttSubmissionRefused, rec.authSeq, 0)
	if v, ok := l.verdict(rec); ok {
		t.Fatalf("a refused abort decided %v: its subject may hold the commit", v)
	}
}

// TestADecidedAttemptRefusesAnotherSubmission pins that nothing is submitted for a decided attempt:
// a submission registered after the decision could land behind the batch's purge.
func TestADecidedAttemptRefusesAnotherSubmission(t *testing.T) {
	m := newReceiptModel(t)
	rec, err := m.prepare("txn-1")
	if err != nil {
		t.Fatal(err)
	}
	m.send(rec, true, false)
	m.timeout(0)
	m.l.decide(rec, mqttTransitionLanded)
	if err := m.l.register(rec, m.seq); !errors.Is(err, errMQTTReceiptUnregistered) {
		t.Fatalf("a decided attempt registered another submission: %v", err)
	}
}

// TestAResolutionReturnsTheCellOverTheStore pins (b'): a resolver that read an absent receipt after
// the attempt was decided Landed and its batch purged still returns Landed.
func TestAResolutionReturnsTheCellOverTheStore(t *testing.T) {
	l := newMQTTReceiptLedger()
	m := &receiptModel{t: t, l: l, seq: 10, outcome: map[string]string{}, purged: map[string]bool{}, gen: testGen, now: time.Now()}
	rec, err := m.prepare("txn-1")
	if err != nil {
		t.Fatal(err)
	}
	l.decide(rec, mqttTransitionLanded)
	if v := l.decide(rec, mqttTransitionNeverLanded); v != mqttTransitionLanded {
		t.Fatalf("a later store-derived NeverLanded overrode the published Landed: %v", v)
	}
	if v := l.decide(rec, mqttTransitionUnresolved); v != mqttTransitionLanded {
		t.Fatalf("an Unresolved read was returned over the published Landed: %v", v)
	}
}

// TestTheBudgetRefusesNewAttemptsAndOnlyAPurgeReturnsIt pins the budget: a spent budget refuses a
// new attempt before anything is stamped, settlement does not return it, and a confirmed purge
// does.
func TestTheBudgetRefusesNewAttemptsAndOnlyAPurgeReturnsIt(t *testing.T) {
	m := newReceiptModel(t)
	for i := 0; i < mqttReceiptBudget; i++ {
		rec, err := m.prepare(fmt.Sprintf("txn-%d", i))
		if err != nil {
			t.Fatalf("attempt %d refused under the budget: %v", i, err)
		}
		m.l.unsent(rec)
	}
	if _, err := m.prepare("txn-over"); !errors.Is(err, errMQTTReceiptBudget) {
		t.Fatalf("an attempt past the budget was admitted: %v", err)
	}
	// Every record is settled (never sent), but nothing is purged yet: still spent.
	if _, err := m.prepare("txn-over"); !errors.Is(err, errMQTTReceiptBudget) {
		t.Fatalf("settlement returned the budget before any purge: %v", err)
	}
	m.now = m.now.Add(time.Hour)
	m.driverPass(true)
	if _, err := m.prepare("txn-after"); err != nil {
		t.Fatalf("a confirmed purge did not return the budget: %v", err)
	}
}

// TestABatchClosesByCountAndByAge pins the batch rotation: a new batch after
// mqttReceiptBatchAttempts attempts, and after mqttReceiptBatchAge.
func TestABatchClosesByCountAndByAge(t *testing.T) {
	m := newReceiptModel(t)
	first, _ := m.prepare("txn-0")
	for i := 1; i < mqttReceiptBatchAttempts; i++ {
		rec, _ := m.prepare(fmt.Sprintf("txn-%d", i))
		if rec.batch != first.batch {
			t.Fatalf("attempt %d left the batch before it was full", i)
		}
	}
	second, _ := m.prepare("txn-full")
	if second.batch == first.batch {
		t.Fatalf("a full batch took another attempt")
	}
	m.now = m.now.Add(mqttReceiptBatchAge)
	third, _ := m.prepare("txn-aged")
	if third.batch == second.batch {
		t.Fatalf("an aged batch took another attempt")
	}
	if third.attempt.Scope != mqttBatchScope(testGen, third.batch.token) {
		t.Fatalf("the attempt was stamped %q, not into its batch", third.attempt.Scope)
	}
}

// TestPositiveRetirementSettlesWithoutTheAuthority pins that a retired scope settles a record the
// authority can no longer fence (lifecycle retirement deletes it), and that a missing authority
// alone does not.
func TestPositiveRetirementSettlesWithoutTheAuthority(t *testing.T) {
	m := newReceiptModel(t)
	rec, _ := m.prepare("txn-1")
	m.send(rec, true, false)
	m.timeout(0)
	m.l.observe(rec, 0, false)
	m.l.mu.Lock()
	settled := rec.settled
	m.l.mu.Unlock()
	if settled {
		t.Fatalf("a record settled with no evidence")
	}
	m.l.observe(rec, 0, true)
	if v, _ := m.l.verdict(rec); v != mqttTransitionRetired {
		t.Fatalf("an undecided record under a retired scope decided %v, want Retired", v)
	}
	m.queue = nil // the store purged the scope; the queued commit can never land
	m.l.mu.Lock()
	settled = rec.settled
	m.l.mu.Unlock()
	if !settled {
		t.Fatalf("positive retirement did not settle the record")
	}
}

// TestReceiptScopeShapes pins the scope contract: a receipted generation kind's receipt names its
// batch, a receipt-free kind's attempt carries none, and the batch is not part of the generation.
func TestReceiptScopeShapes(t *testing.T) {
	scope := mqttBatchScope(testGen, testReceiptBatch)
	if _, gen, ok := mqttParseOutcomeScope(scope); !ok || gen != testGen {
		t.Fatalf("a batch scope parsed to %q, %v", gen, ok)
	}
	if b := mqttOutcomeScopeBatch(scope); b != testReceiptBatch {
		t.Fatalf("the batch of %q read %q", scope, b)
	}
	if err := mqttCheckReceiptScope(mqttTransitionAdmitExchange, testGen); err == nil {
		t.Fatalf("a generation receipt with no batch was admitted")
	}
	if err := mqttCheckOutcomeScope(mqttTransitionAdvanceExchange, scope); err == nil {
		t.Fatalf("a receipt-free kind carried a receipt batch")
	}
	if !mqttIsBatchReceiptSubject(mqttTransitionOutcomeSubject("h", scope, "txn")) {
		t.Fatalf("a batch receipt subject was not recognized")
	}
	if mqttIsBatchReceiptSubject(mqttTransitionOutcomeSubject("h", "L.0000000002", "txn")) {
		t.Fatalf("a lifecycle receipt subject was taken for a batch receipt")
	}
	for _, k := range []mqttTransitionKind{mqttTransitionAdvanceExchange, mqttTransitionReleaseHolds, mqttTransitionAdvanceFloor} {
		if mqttKindWritesReceipt(k) {
			t.Fatalf("%s writes a receipt", k)
		}
	}
}

// TestAReceiptOutsideTheLedgerIsRefused pins the structural registration: a generation receipt
// whose batch this run did not stamp the attempt into is refused at the commit (prepare) and at
// the abort's fenced create, before anything is sent.
func TestAReceiptOutsideTheLedgerIsRefused(t *testing.T) {
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}
	a := testAuthorityAttempt()
	auth := &mqttOutboundAuthority{ID: testID, Epoch: testEpoch}
	if _, err := jsa.receipts().prepare(&mqttAuthorityTransition{Attempt: a, Auth: auth, PrevSeq: 3}); !errors.Is(err, errMQTTReceiptUnregistered) {
		t.Fatalf("a commit naming a batch the ledger never stamped was admitted: %v", err)
	}
	subj := mqttTransitionOutcomeSubject(getHash(testID), a.Scope, a.Txn)
	if err := jsa.createBesideAuthority(testID, mqttAuthorityLoad{status: mqttOutboundValid, rec: auth, seq: 3}, subj,
		[]byte("{}"), a.Txn, nil); !errors.Is(err, errMQTTReceiptUnregistered) {
		t.Fatalf("an abort's receipt in a batch the ledger does not track was admitted: %v", err)
	}
}

// TestNoRecordSettlesWithASubmissionInFlight pins the in-flight guard: the driver reads the
// authority past every ambiguous fence while a holder's abort, registered after the driver
// collected the record, is still in flight at the authority's current sequence. Settling then
// would purge the batch under an abort that can still land.
func TestNoRecordSettlesWithASubmissionInFlight(t *testing.T) {
	m := newReceiptModel(t)
	rec, err := m.prepare("txn-1")
	if err != nil {
		t.Fatal(err)
	}
	m.send(rec, true, false)
	m.timeout(0)
	m.apply(0)                // the delayed commit lands: the authority moves to 11
	m.send(rec, false, false) // the holder's abort at 11, in flight
	m.l.decide(rec, mqttTransitionLanded)
	m.l.observe(rec, m.seq+1, false)
	m.l.mu.Lock()
	settled := rec.settled
	m.l.mu.Unlock()
	if settled {
		t.Fatalf("the record settled while its abort was still in flight")
	}
}

// TestAFailedPurgeIsRetriedUntilConfirmed pins that a batch whose purge failed after it moved to
// purging is purged again on a later pass, its budget still charged, and returns the budget only
// when a purge is confirmed (holistic round 1).
func TestAFailedPurgeIsRetriedUntilConfirmed(t *testing.T) {
	m := newReceiptModel(t)
	rec, err := m.prepare("txn-1")
	if err != nil {
		t.Fatal(err)
	}
	m.l.unsent(rec)
	m.now = m.now.Add(time.Hour)
	w, _ := m.l.work(testEpoch, 64)
	if len(w.purge) != 1 || !m.l.beginPurge(w.purge[0]) {
		t.Fatalf("the settled batch was not offered for its purge: %d", len(w.purge))
	}
	b := w.purge[0]
	// The purge fails: the batch stays purging.
	m.l.retryPurgeLater(b)
	if w, _ := m.l.work(testEpoch, 64); len(w.purge) != 0 {
		t.Fatalf("a failed purge was retried before its backoff")
	}
	m.now = m.now.Add(mqttReceiptRetryMax)
	w, _ = m.l.work(testEpoch, 64)
	if len(w.purge) != 1 || w.purge[0] != b {
		t.Fatalf("a batch left purging by a failed purge was never offered again")
	}
	if m.l.keys[mqttReceiptKey{epoch: testEpoch, id: testID, gen: testGen}].charged != 1 {
		t.Fatalf("a failed purge returned the budget")
	}
	m.l.forget(b)
	if _, ok := m.l.keys[mqttReceiptKey{epoch: testEpoch, id: testID, gen: testGen}]; ok {
		t.Fatalf("a confirmed purge did not return the budget")
	}
}
