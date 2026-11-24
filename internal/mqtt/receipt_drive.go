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
	"fmt"
	"time"
)

// The receipt ledger's driver (issue #192): it settles what no holder settles and purges what is
// settled, on its own clock. A record is settled by the store alone: resolved first (a receipt at
// an equal fence decides from its body), then, while undecided with the authority still at its
// fence, by its own fenced abort; once decided with the authority still at its fence (an abort
// whose acknowledgement was lost, on an idle session), by a PROGRESS WRITE, the authority rewritten
// unchanged at the sequence it reads, which no late copy can survive. None of it charges the
// receipt budget, so a spent budget never blocks the work that returns it.

// runReceiptDriver is the account's receipt driver: a pass on every kick and at the earliest
// deadline the ledger reports (a batch's age, a record's retry), never later than its own retry
// ceiling.
func (as *mqttAccountSessionManager) runReceiptDriver(quit, closeCh chan struct{}) {
	l := as.jsa.receipts()
	kick := make(chan struct{}, 1)
	l.mu.Lock()
	l.kick = func() {
		select {
		case kick <- struct{}{}:
		default:
		}
	}
	l.mu.Unlock()
	timer := time.NewTimer(mqttReceiptRetryMax)
	defer timer.Stop()
	for {
		select {
		case <-kick:
		case <-timer.C:
		case <-quit:
			return
		case <-closeCh:
			return
		}
		next := as.receiptPass(false)
		wait := mqttReceiptRetryMax
		if !next.IsZero() {
			wait = max(min(time.Until(next), mqttReceiptRetryMax), 10*time.Millisecond)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
	}
}

// receiptPass drives one bounded pass: the due records, then the purgeable batches. shutdown drives
// with no admission: every client connection is closed by then (Phase 1), so no foreground work can
// be announced. It returns when the next pass is due.
func (as *mqttAccountSessionManager) receiptPass(shutdown bool) time.Time {
	jsa := as.jsa
	l := jsa.receipts()
	w, next := l.work(as.sessStreamEpoch, mqttReceiptDriverReads)
	for _, rec := range w.settle {
		if shutdown {
			jsa.driveReceipt(rec, nil)
		} else {
			as.driveReceiptAdmitted(rec)
		}
	}
	for _, b := range w.purge {
		if err := jsa.purgeReceiptBatch(b); err != nil {
			jsa.receipts().retryPurgeLater(b)
			if jsa.c != nil {
				jsa.c.Debugf("MQTT: purging receipt batch %q of generation %q of %q: %v", b.token, b.key.gen, b.key.id, err)
			}
		}
	}
	if len(w.settle) != 0 || len(w.purge) != 0 {
		// Whatever this pass settled or purged may have made more due: look again at once.
		return time.Now()
	}
	return next
}

// driveReceiptAdmitted is driveReceipt as a registered background actor of the record's client, each
// authority write admitted only while no foreground work of the client is announced.
func (as *mqttAccountSessionManager) driveReceiptAdmitted(rec *mqttReceiptAttempt) {
	id := rec.attempt.ID
	deregister := as.registerForegroundActor(id)
	defer deregister()
	admit := func() bool {
		_, _, idle := as.admitForegroundOrIdle(id)
		return idle == nil
	}
	as.jsa.driveReceipt(rec, admit)
}

// mqttReceiptHandle is a handle over rec, for the ledger's own resolution: it shares rec's cell.
func mqttReceiptHandle(rec *mqttReceiptAttempt) *mqttTransitionHandle {
	return &mqttTransitionHandle{attempt: rec.attempt, digest: rec.digest, authSeq: rec.authSeq, rec: rec}
}

// driveReceipt takes one step toward settling rec.
func (jsa *mqttJSA) driveReceipt(rec *mqttReceiptAttempt, admit func() bool) {
	l := jsa.receipts()
	a := &rec.attempt
	// RESOLVE FIRST, whatever the authority's position: a receipt standing at an equal fence decides
	// from its body, where a second abort would only be refused create-only, deciding nothing.
	jsa.resolveTransitionHandle(mqttReceiptHandle(rec))
	al := jsa.loadOutboundAuthority(a.ID, a.Epoch)
	switch al.status {
	case mqttOutboundValid:
		l.observe(rec, al.seq, false)
	case mqttOutboundAbsent:
		// A MISSING AUTHORITY PROVES NOTHING; only positive retirement of the scope settles here.
		if jsa.outcomeScopeRetired(a) {
			l.observe(rec, 0, true)
		}
	}
	l.mu.Lock()
	settled, decided, fence := rec.settled, rec.decided, rec.fence
	l.mu.Unlock()
	if settled {
		return
	}
	if al.status == mqttOutboundValid && al.seq <= fence {
		if !decided {
			// UNDECIDED, the authority still where a copy could land: the attempt's own fenced abort,
			// which contends with that copy for the outcome subject. Its return is recorded by the
			// abort funnel; the next pass resolves again.
			_ = jsa.abortTransition(a, admit)
		} else {
			if admit != nil && !admit() {
				l.retryLater(rec)
				return
			}
			_ = jsa.progressAuthority(rec)
		}
	}
	l.mu.Lock()
	settled = rec.settled
	l.mu.Unlock()
	if !settled {
		l.retryLater(rec)
	}
}

// progressAuthority moves the authority past rec's fence when nothing else does: the authority
// rewritten unchanged, alone, at the sequence just read, the abort's fence without its receipt. It
// writes no receipt and moves no revision; it is refused where the abort would be (another
// lifecycle, a generation outside its boundary), and contends with foreground work like one.
//
// One attempt, admitted by its caller immediately before it.
func (jsa *mqttJSA) progressAuthority(rec *mqttReceiptAttempt) error {
	a := &rec.attempt
	al := jsa.loadOutboundAuthority(a.ID, a.Epoch)
	if al.status != mqttOutboundValid || al.seq == 0 {
		return fmt.Errorf("moving the authority of %q past transition %q: it reads %v", a.ID, a.Txn, al.status)
	}
	l := jsa.receipts()
	l.mu.Lock()
	fence := rec.fence
	l.mu.Unlock()
	if al.seq > fence {
		l.observe(rec, al.seq, false)
		return nil
	}
	if lc, _, _ := mqttParseOutcomeScope(a.Scope); al.rec.Life == nil || al.rec.Life.L != lc {
		return fmt.Errorf("moving the authority past transition %q: its scope %q is not the standing lifecycle's", a.Txn, a.Scope)
	}
	if berr := mqttCheckGenerationBoundary(&mqttAuthorityTransition{
		Attempt: a, Prev: al.rec, PrevSeq: al.seq, Auth: al.rec,
	}); berr != nil {
		return fmt.Errorf("moving the authority past transition %q: %w", a.Txn, berr)
	}
	authBody, merr := json.Marshal(al.rec)
	if merr != nil {
		return merr
	}
	jsa.mu.Lock()
	batchID := jsa.nuid.Next()
	jsa.mu.Unlock()
	am, ah := mqttFrameBatchWrite(al.seq, authBody, batchID, 1, true)
	bi, err := jsa.newAtomicBatchRequest(mqttJSAMsgStore, []mqttBatchMsg{
		{subj: mqttOutboundAuthoritySubject(getHash(a.ID)), hdr: ah, msg: am},
	})
	if err != nil {
		return err
	}
	resp := bi.(*JSPubAckResponse)
	if aerr := resp.ToError(); aerr != nil {
		return aerr
	}
	if cerr := mqttCheckTransitionBatchAck(resp, batchID, 1, "progress-"+a.Txn); cerr != nil {
		return cerr
	}
	l.observe(rec, resp.Sequence, false)
	return nil
}

// mqttReceiptBatchFilter is the subject filter of one receipt batch.
func mqttReceiptBatchFilter(idHash, gen, batch string) string {
	return mqttTransitionOutcomeSubject(idHash, mqttBatchScope(gen, batch), ">")
}

// purgeReceiptBatch purges a closed batch whose every record is settled, and forgets it once a
// fresh listing finds nothing under its filter. Every submission into the batch can no longer land,
// so nothing is written under it again; the cutoff is the stream's last sequence read after the
// batch moved to purging, past every receipt it holds. A failed or ambiguous purge leaves the batch
// for the next pass, its budget still charged.
func (jsa *mqttJSA) purgeReceiptBatch(b *mqttReceiptBatch) error {
	l := jsa.receipts()
	if !l.beginPurge(b) {
		return nil
	}
	filter := mqttReceiptBatchFilter(getHash(b.key.id), b.key.gen, b.token)
	subs, last, err := jsa.streamSubjects(mqttSessStreamName, filter)
	if err != nil {
		return err
	}
	if len(subs) != 0 {
		if perr := jsa.purgeStream(mqttSessStreamName, filter, last+1, false); perr != nil {
			return perr
		}
		if subs, _, err = jsa.streamSubjects(mqttSessStreamName, filter); err != nil {
			return err
		}
		if len(subs) != 0 {
			return fmt.Errorf("%d receipt(s) remain under %q after its purge", len(subs), filter)
		}
	}
	l.forget(b)
	return nil
}

// settleReceiptsAtShutdown is a graceful shutdown's part of the receipt ledger, run while the store
// still answers (before Phase 4): no new receipted attempt from here on, every open batch closed,
// and the ledger driven, its own aborts and progress writes unrefused, until everything is purged or
// the deadline passes. What is left is reported: it stays until the generation's purge, exactly as
// after a crash.
func (as *mqttAccountSessionManager) settleReceiptsAtShutdown(deadline time.Time) {
	l := as.jsa.receipts()
	l.setTerminal()
	l.closeAll()
	for time.Now().Before(deadline) {
		if batches, _ := l.pending(as.sessStreamEpoch); batches == 0 {
			return
		}
		next := as.receiptPass(true)
		if wait := time.Until(next); wait > 0 {
			time.Sleep(min(wait, 50*time.Millisecond, time.Until(deadline)))
		}
	}
	if as.jsa.c == nil || as.jsa.c.srv == nil {
		return
	}
	for _, b := range l.leftovers(as.sessStreamEpoch) {
		as.jsa.c.srv.Warnf("MQTT: shutting down with receipt batch %q of generation %q of client %q unpurged "+
			"(%d unsettled attempt(s)): its receipts stay until the generation retires", b.token, b.key.gen, b.key.id, b.unsettled)
	}
}

// settleReceiptsAtShutdown runs every account's receipt shutdown under one deadline.
func (s *Server) settleReceiptsAtShutdown() {
	s.mqtt.sessmgr.mu.RLock()
	managers := make([]*mqttAccountSessionManager, 0, len(s.mqtt.sessmgr.sessions))
	for _, as := range s.mqtt.sessmgr.sessions {
		managers = append(managers, as)
	}
	s.mqtt.sessmgr.mu.RUnlock()
	deadline := time.Now().Add(mqttReceiptShutdownBudget)
	for _, as := range managers {
		as.settleReceiptsAtShutdown(deadline)
	}
}
