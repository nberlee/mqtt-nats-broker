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
	"sync"
	"time"
)

// The receipt ledger (issue #192): how a serving generation's transition receipts are reclaimed
// while the generation serves, by the run that wrote them.
//
// A GENERATION RECEIPT HAS ONE READER: handles of the process that submitted it. A handle is
// built only from an in-memory transition (mqttNewTransitionHandle) and an outcome is read only
// under one (resolveTransitionHandle) or by the abort that settles it, so a process that dies
// takes every reader of its receipts with it. And every receipted generation attempt writes the
// authority at a nonzero expectation, as does every abort: once the authority's sequence stands
// past a submission's expectation, that submission can never land.
//
// So the run alone decides when a receipt it wrote is no longer needed. Each receipted generation
// attempt is stamped into a RECEIPT BATCH, a token the run mints and never reuses
// (ob.txn.<idHash>.<gen>.<batch>.<txn>), and owned by one RECORD here that every handle of the
// attempt points at. The record holds the attempt's one decision cell, and counts the attempt's
// submissions, the commit and every abort. A batch whose every record is decided, and whose every
// submission can no longer land, has no reader left and no writer to come: the run purges it with
// one filtered purge and forgets it. Nothing about a batch is persisted but its receipts.
//
// What a run that dies leaves behind is its unpurged batches, reclaimed by the generation's purge
// (docs/CONFORMANCE.md). Scope retirement, the generation's and the lifecycle's purges, still
// removes any run's receipts, by its own rules.

const (
	// mqttReceiptBatchAttempts closes a batch after this many attempts.
	mqttReceiptBatchAttempts = 256
	// mqttReceiptBatchAge closes a batch this long after it opened, so an idle generation's last
	// receipts are purged too.
	mqttReceiptBatchAge = 10 * time.Second
	// mqttReceiptBudget bounds the receipted attempts of one generation in batches not yet purged:
	// unsettled records, settled neighbours their batch keeps, and settled batches a failing purge
	// has not yet removed.
	mqttReceiptBudget = 4096
	// mqttReceiptDriverReads bounds the authority reads of one driver pass.
	mqttReceiptDriverReads = 64
	// mqttReceiptRetryMin and mqttReceiptRetryMax space a record's retries when a pass could not
	// settle it.
	mqttReceiptRetryMin = time.Second
	mqttReceiptRetryMax = 30 * time.Second
	// mqttReceiptShutdownBudget bounds shutdown's settlement and purge.
	mqttReceiptShutdownBudget = 10 * time.Second
)

var (
	// errMQTTReceiptBudget refuses a new receipted attempt of a generation whose budget is spent:
	// definite, nothing was sent.
	errMQTTReceiptBudget = errors.New("the generation's receipt budget is spent until its settled batches are purged")
	// errMQTTReceiptLedgerClosed refuses a new receipted attempt once the server is shutting down:
	// definite, nothing was sent.
	errMQTTReceiptLedgerClosed = errors.New("the receipt ledger is closed: the server is shutting down")
	// errMQTTReceiptUnregistered refuses a receipt write the ledger did not register: a scope
	// naming a batch this run did not stamp the attempt into, or a submission of an attempt that
	// is already decided.
	errMQTTReceiptUnregistered = errors.New("the receipt write is not registered in this run's receipt ledger")
)

// mqttReceiptKey names one generation of one client in one account recovery epoch.
type mqttReceiptKey struct {
	epoch, id, gen string
}

// mqttReceiptBatchState is a batch's place in its life: attempts are stamped only into the open
// batch, and only a closed batch whose every record is settled is purged.
type mqttReceiptBatchState uint8

const (
	mqttReceiptBatchOpen mqttReceiptBatchState = iota
	mqttReceiptBatchClosed
	mqttReceiptBatchPurging
)

// mqttReceiptBatch is one batch of one generation's receipts. Guarded by the ledger's lock.
type mqttReceiptBatch struct {
	key       mqttReceiptKey
	token     string
	opened    time.Time
	state     mqttReceiptBatchState
	records   []*mqttReceiptAttempt
	unsettled int
	// nextTry and backoff space the retries of a purge that failed or was ambiguous: the batch
	// stays purging, its budget charged, until a fresh listing finds it empty.
	nextTry time.Time
	backoff time.Duration
}

// mqttSubmissionResult is what one submission's own return established.
type mqttSubmissionResult uint8

// A submission refused before it reached the store is never returned: it was never registered
// (mqttReceiptLedger.unsent settles a call that sent nothing).
const (
	// mqttSubmissionLanded: the store acknowledged the batch.
	mqttSubmissionLanded mqttSubmissionResult = iota + 1
	// mqttSubmissionRefused: the store refused the batch at a record CAS. The batch is never
	// applied, and that is ALL it establishes: not that another copy of the attempt did not land.
	mqttSubmissionRefused
	// mqttSubmissionAmbiguous: anything else. The batch may still land while the authority stands
	// at its expectation.
	mqttSubmissionAmbiguous
)

// mqttReceiptAttempt is the one record of one receipted generation attempt, created before its
// handle (mqttReceiptLedger.prepare) and owned by the ledger, not by any handle: a caller that
// drops its handle loses nothing, and the ledger settles the record by itself.
type mqttReceiptAttempt struct {
	// Frozen at prepare: the stamped attempt, its digest and the authority sequence its batch
	// expects (never zero).
	batch   *mqttReceiptBatch
	attempt mqttTransitionAttempt
	digest  string
	authSeq uint64

	// Guarded by the ledger's lock.
	//
	// sends counts every submission ever registered and active those not yet returned.
	// commitAmbiguous: some commit submission returned ambiguous, so a later definite refusal of
	// another proves nothing about the attempt. fence is the highest expectation of a submission
	// that returned ambiguous: what could still land lands only while the authority stands at or
	// below it. evidence is the highest authority sequence observed since (an acknowledgement or a
	// read), kept as a SEQUENCE and compared with the CURRENT fence at settlement, so a later
	// submission is never covered by older evidence. retired: the attempt's scope is positively
	// retired.
	sends, active   int
	commitAmbiguous bool
	fence, evidence uint64
	retired         bool
	decided         bool
	verdict         mqttTransitionVerdict
	settled         bool
	nextTry         time.Time
	backoff         time.Duration
}

// mqttReceiptKeyState is one generation's batches in this run.
type mqttReceiptKeyState struct {
	open    *mqttReceiptBatch
	batches map[string]*mqttReceiptBatch
	charged int
}

// mqttReceiptLedger is one account's receipt ledger.
type mqttReceiptLedger struct {
	mu       sync.Mutex
	keys     map[mqttReceiptKey]*mqttReceiptKeyState
	byTxn    map[string]*mqttReceiptAttempt
	terminal bool
	kick     func()
	// now is the clock, replaced by a test. Nil in production.
	now func() time.Time
}

func newMQTTReceiptLedger() *mqttReceiptLedger {
	return &mqttReceiptLedger{
		keys:  map[mqttReceiptKey]*mqttReceiptKeyState{},
		byTxn: map[string]*mqttReceiptAttempt{},
	}
}

func (l *mqttReceiptLedger) clock() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

// receipts is this account's receipt ledger, created on first use.
func (jsa *mqttJSA) receipts() *mqttReceiptLedger {
	if jsa == nil || jsa.mqttJSACore == nil {
		return nil
	}
	jsa.receiptsOnce.Do(func() { jsa.receiptLedger = newMQTTReceiptLedger() })
	return jsa.receiptLedger
}

// kickReceipts wakes the account's receipt driver, if it runs.
func (l *mqttReceiptLedger) kickDriver() {
	l.mu.Lock()
	kick := l.kick
	l.mu.Unlock()
	if kick != nil {
		kick()
	}
}

// prepare stamps a receipted generation attempt into its generation's open batch and creates its
// record, BEFORE the attempt's digest is frozen and its handle built: the digest binds the scope,
// and every handle of the attempt points at this one record. A transition already stamped is a
// resend of the same attempt and returns the record it was stamped with; nothing reconstructs a
// record, so no attempt ever has two decision cells. Nil for an attempt that writes no generation
// receipt. A refusal is definite: nothing was stamped or sent.
func (l *mqttReceiptLedger) prepare(t *mqttAuthorityTransition) (*mqttReceiptAttempt, error) {
	if l == nil || t == nil || t.Attempt == nil || !mqttKindNeedsReceiptBatch(t.Attempt.Kind) {
		return nil, nil
	}
	a := t.Attempt
	l.mu.Lock()
	defer l.mu.Unlock()
	if mqttOutcomeScopeBatch(a.Scope) != _EMPTY_ {
		rec := l.byTxn[a.Txn]
		if rec == nil || rec.attempt.Scope != a.Scope {
			return nil, fmt.Errorf("transition %q: %w", a.Txn, errMQTTReceiptUnregistered)
		}
		return rec, nil
	}
	_, gen, ok := mqttParseOutcomeScope(a.Scope)
	if !ok || gen == _EMPTY_ {
		return nil, fmt.Errorf("transition %q carries the malformed generation scope %q", a.Txn, a.Scope)
	}
	// THE FENCE IS DEFINED: a receipted generation attempt writes the authority at a nonzero
	// expectation, which is what makes its late landing impossible once the authority moved.
	if t.Auth == nil || t.PrevSeq == 0 {
		return nil, fmt.Errorf("a %s attempt writes its generation receipt without the authority at a nonzero "+
			"expectation, so nothing could fence a late copy of it", a.Kind)
	}
	if _, dup := l.byTxn[a.Txn]; dup {
		return nil, fmt.Errorf("transition %q is already registered under another scope: %w", a.Txn, errMQTTReceiptUnregistered)
	}
	if l.terminal {
		return nil, errMQTTReceiptLedgerClosed
	}
	key := mqttReceiptKey{epoch: a.Epoch, id: a.ID, gen: gen}
	ks := l.keys[key]
	if ks == nil {
		ks = &mqttReceiptKeyState{batches: map[string]*mqttReceiptBatch{}}
		l.keys[key] = ks
	}
	if ks.charged >= mqttReceiptBudget {
		return nil, fmt.Errorf("generation %q of %q: %w", gen, a.ID, errMQTTReceiptBudget)
	}
	now := l.clock()
	if b := ks.open; b != nil && (len(b.records) >= mqttReceiptBatchAttempts || now.Sub(b.opened) >= mqttReceiptBatchAge) {
		l.closeLocked(ks)
	}
	if ks.open == nil {
		b := &mqttReceiptBatch{key: key, token: mqttNewInflightNonce(), opened: now, state: mqttReceiptBatchOpen}
		ks.open, ks.batches[b.token] = b, b
	}
	b := ks.open
	a.Scope = mqttBatchScope(gen, b.token)
	digest, derr := mqttTransitionMutationDigest(t)
	if derr != nil {
		a.Scope = gen
		return nil, derr
	}
	rec := &mqttReceiptAttempt{batch: b, attempt: *a, digest: digest, authSeq: t.PrevSeq}
	b.records = append(b.records, rec)
	b.unsettled++
	ks.charged++
	l.byTxn[a.Txn] = rec
	return rec, nil
}

// record returns the record of a stamped attempt, or nil.
func (l *mqttReceiptLedger) record(a *mqttTransitionAttempt) *mqttReceiptAttempt {
	if l == nil || a == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec := l.byTxn[a.Txn]; rec != nil && rec.attempt.Scope == a.Scope {
		return rec
	}
	return nil
}

// register admits one submission of rec at authority expectation exp, immediately before it is
// sent. Refused once the attempt is decided or settled, or its batch is being purged: nothing is
// left to submit for, and a submission registered then could land behind the purge.
func (l *mqttReceiptLedger) register(rec *mqttReceiptAttempt, exp uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec.decided || rec.settled || rec.batch.state == mqttReceiptBatchPurging || exp == 0 {
		return fmt.Errorf("transition %q: %w", rec.attempt.Txn, errMQTTReceiptUnregistered)
	}
	rec.sends++
	rec.active++
	return nil
}

// returned records what one registered submission's own return established. commit says whether it
// was the attempt's commit (else its abort); exp is its expectation, ackSeq the sequence of an
// acknowledged batch.
func (l *mqttReceiptLedger) returned(rec *mqttReceiptAttempt, commit bool, res mqttSubmissionResult, exp, ackSeq uint64) {
	l.mu.Lock()
	rec.active--
	switch res {
	case mqttSubmissionLanded:
		if commit {
			l.decideLocked(rec, mqttTransitionLanded)
		} else {
			l.decideLocked(rec, mqttTransitionNeverLanded)
		}
		rec.evidence = max(rec.evidence, ackSeq)
	case mqttSubmissionRefused:
		// A refused COMMIT never lands; the attempt never landed only if no other copy of it can.
		if commit && !rec.commitAmbiguous && rec.active == 0 {
			l.decideLocked(rec, mqttTransitionNeverLanded)
		}
	case mqttSubmissionAmbiguous:
		rec.fence = max(rec.fence, exp)
		if commit {
			rec.commitAmbiguous = true
		}
	}
	kick := l.settleLocked(rec)
	l.mu.Unlock()
	if kick {
		l.kickDriver()
	}
}

// unsent records a commit call that returned without ever registering a submission: an attempt
// never sent never landed, unless an earlier submission of it was.
func (l *mqttReceiptLedger) unsent(rec *mqttReceiptAttempt) {
	if rec == nil {
		return
	}
	l.mu.Lock()
	if rec.sends == 0 {
		l.decideLocked(rec, mqttTransitionNeverLanded)
	}
	kick := l.settleLocked(rec)
	l.mu.Unlock()
	if kick {
		l.kickDriver()
	}
}

// decide stores v in rec's cell if the cell is empty, and returns the cell's value: the CANONICAL
// verdict every resolver returns, Unresolved included, never its own computation. A resolver that
// read an absent receipt after another participant decided Landed and the batch was purged
// therefore still returns Landed.
func (l *mqttReceiptLedger) decide(rec *mqttReceiptAttempt, v mqttTransitionVerdict) mqttTransitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.decideLocked(rec, v)
}

func (l *mqttReceiptLedger) decideLocked(rec *mqttReceiptAttempt, v mqttTransitionVerdict) mqttTransitionVerdict {
	if !rec.decided && v != mqttTransitionUnresolved {
		rec.decided, rec.verdict = true, v
		if v == mqttTransitionRetired {
			rec.retired = true
		}
	}
	if !rec.decided {
		return mqttTransitionUnresolved
	}
	return rec.verdict
}

// verdict returns rec's decided verdict, if any.
func (l *mqttReceiptLedger) verdict(rec *mqttReceiptAttempt) (mqttTransitionVerdict, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return rec.verdict, rec.decided
}

// observe records fence evidence: an authority read at sequence seq, or positive retirement of the
// attempt's scope.
func (l *mqttReceiptLedger) observe(rec *mqttReceiptAttempt, seq uint64, retired bool) {
	l.mu.Lock()
	rec.evidence = max(rec.evidence, seq)
	if retired {
		rec.retired = true
		l.decideLocked(rec, mqttTransitionRetired)
	}
	kick := l.settleLocked(rec)
	l.mu.Unlock()
	if kick {
		l.kickDriver()
	}
}

// settleLocked settles rec if it can be: decided, no submission in flight, and no submission that
// could still land (the evidence stands past the current fence, or the scope is retired). Reports
// whether its batch became purgeable.
func (l *mqttReceiptLedger) settleLocked(rec *mqttReceiptAttempt) bool {
	if rec.settled || !rec.decided || rec.active != 0 {
		return false
	}
	if rec.fence != 0 && rec.evidence <= rec.fence && !rec.retired {
		return false
	}
	rec.settled = true
	delete(l.byTxn, rec.attempt.Txn)
	b := rec.batch
	b.unsettled--
	return b.unsettled == 0 && b.state == mqttReceiptBatchClosed
}

// closeLocked closes ks's open batch: nothing more is stamped into it.
func (l *mqttReceiptLedger) closeLocked(ks *mqttReceiptKeyState) {
	if b := ks.open; b != nil {
		b.state = mqttReceiptBatchClosed
		ks.open = nil
	}
}

// close closes every open batch (shutdown) and returns whether any batch is now purgeable.
func (l *mqttReceiptLedger) closeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ks := range l.keys {
		l.closeLocked(ks)
	}
}

// setTerminal refuses every new receipted attempt from here on (shutdown).
func (l *mqttReceiptLedger) setTerminal() {
	l.mu.Lock()
	l.terminal = true
	l.mu.Unlock()
}

// mqttReceiptWork is what one driver pass found to do.
type mqttReceiptWork struct {
	settle []*mqttReceiptAttempt
	purge  []*mqttReceiptBatch
}

// work closes aged batches and collects, for account epoch epoch, the records a pass should drive
// (unsettled, nothing in flight, due) and the batches it should purge (closed, every record
// settled), round-robin over keys and records. It also reports the earliest time something not
// collected becomes due, so the driver can sleep until then.
func (l *mqttReceiptLedger) work(epoch string, limit int) (mqttReceiptWork, time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock()
	var w mqttReceiptWork
	var next time.Time
	due := func(t time.Time) {
		if next.IsZero() || t.Before(next) {
			next = t
		}
	}
	for key, ks := range l.keys {
		if key.epoch != epoch {
			continue
		}
		if b := ks.open; b != nil {
			if now.Sub(b.opened) >= mqttReceiptBatchAge {
				l.closeLocked(ks)
			} else {
				due(b.opened.Add(mqttReceiptBatchAge))
			}
		}
		for _, b := range ks.batches {
			// A batch whose purge failed is PURGING, and is purged again: a purge is idempotent, and
			// only a confirmed one returns its budget.
			if (b.state == mqttReceiptBatchClosed || b.state == mqttReceiptBatchPurging) && b.unsettled == 0 {
				if now.Before(b.nextTry) {
					due(b.nextTry)
				} else {
					w.purge = append(w.purge, b)
				}
				continue
			}
			for _, rec := range b.records {
				// A record never sent is its caller's, which settles it when its call returns.
				if rec.settled || rec.active != 0 || rec.sends == 0 {
					continue
				}
				if now.Before(rec.nextTry) {
					due(rec.nextTry)
					continue
				}
				if len(w.settle) < limit {
					w.settle = append(w.settle, rec)
				} else {
					due(now)
				}
			}
		}
	}
	return w, next
}

// retryLater spaces rec's next attempt after a pass that could not settle it.
func (l *mqttReceiptLedger) retryLater(rec *mqttReceiptAttempt) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec.backoff == 0 {
		rec.backoff = mqttReceiptRetryMin
	} else {
		rec.backoff = min(2*rec.backoff, mqttReceiptRetryMax)
	}
	rec.nextTry = l.clock().Add(rec.backoff)
}

// retryPurgeLater spaces b's next purge after one that failed or was ambiguous.
func (l *mqttReceiptLedger) retryPurgeLater(b *mqttReceiptBatch) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b.backoff == 0 {
		b.backoff = mqttReceiptRetryMin
	} else {
		b.backoff = min(2*b.backoff, mqttReceiptRetryMax)
	}
	b.nextTry = l.clock().Add(b.backoff)
}

// beginPurge moves b to purging, refusing every submission into it from here on; false if b is not
// a closed batch whose every record is settled.
func (l *mqttReceiptLedger) beginPurge(b *mqttReceiptBatch) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b.state == mqttReceiptBatchPurging {
		return true
	}
	if b.state != mqttReceiptBatchClosed || b.unsettled != 0 {
		return false
	}
	b.state = mqttReceiptBatchPurging
	return true
}

// forget drops a batch whose purge is confirmed and returns its attempts to the budget.
func (l *mqttReceiptLedger) forget(b *mqttReceiptBatch) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ks := l.keys[b.key]
	if ks == nil || ks.batches[b.token] != b {
		return
	}
	delete(ks.batches, b.token)
	ks.charged -= len(b.records)
	if ks.open == nil && len(ks.batches) == 0 {
		delete(l.keys, b.key)
	}
}

// pending reports what is still unpurged for account epoch epoch: batches, and unsettled records.
func (l *mqttReceiptLedger) pending(epoch string) (batches, unsettled int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, ks := range l.keys {
		if key.epoch != epoch {
			continue
		}
		for _, b := range ks.batches {
			batches++
			unsettled += b.unsettled
		}
	}
	return batches, unsettled
}

// leftovers lists, for account epoch epoch, every batch still unpurged, for shutdown's report.
func (l *mqttReceiptLedger) leftovers(epoch string) []mqttReceiptBatch {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []mqttReceiptBatch
	for key, ks := range l.keys {
		if key.epoch != epoch {
			continue
		}
		for _, b := range ks.batches {
			out = append(out, mqttReceiptBatch{key: b.key, token: b.token, state: b.state, unsettled: b.unsettled})
		}
	}
	return out
}
