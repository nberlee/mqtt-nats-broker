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
	"fmt"
)

// The generation rules of the write boundary (issue #133), stated over the authority a
// transition CASes, so they hold for every kind rather than for the kinds that remember them.
//
//   - A transition scoped to a generation needs that generation CURRENT, or outstanding at
//     phase open: once its retirement fences it, nothing more is written for it.
//   - A retiring lifecycle takes no transition: its authority's last write is its retirement.
//   - Every noncurrent generation is a discarded one: a transition that makes the predecessor's
//     generation noncurrent closes it and enrols it outstanding, in the same batch.

// mqttCheckGenerationBoundary applies the three rules to a transition that writes an authority
// over a predecessor. A first creation has no predecessor and an authority-free kind writes no
// authority; neither is reached here.
func mqttCheckGenerationBoundary(t *mqttAuthorityTransition) error {
	a, prev := t.Attempt, t.Prev
	if prev == nil || t.Auth == nil {
		return nil
	}
	if prev.Life == nil {
		return fmt.Errorf("transition %q writes over an authority with no lifecycle", a.Txn)
	}
	if prev.Life.Phase == mqttLifecycleRetiring {
		return fmt.Errorf("transition %q writes the authority of lifecycle %d, which is retiring: "+
			"its last write is its retirement", a.Txn, prev.Life.L)
	}
	if _, gen, _ := mqttParseOutcomeScope(a.Scope); gen != _EMPTY_ {
		s, entry, _ := mqttGenStandingIn(mqttAuthorityLoad{rec: prev, status: mqttOutboundValid}, gen)
		switch {
		case s == mqttGenCurrent:
		case s == mqttGenOutstanding && entry.Phase == mqttOutstandingOpen:
		default:
			return fmt.Errorf("transition %q is scoped to generation %q, which reads %v under the "+
				"authority it writes: only a current or open generation takes a transition", a.Txn, gen, s)
		}
	}
	return mqttCheckNoncurrencyIsADiscard(t)
}

// mqttCheckNoncurrencyIsADiscard refuses a transition that ends the predecessor's current
// generation without closing it and enrolling it outstanding.
func mqttCheckNoncurrencyIsADiscard(t *mqttAuthorityTransition) error {
	prev, next := t.Prev, t.Auth
	wasCurrent := prev.State == mqttAuthorityServing
	stillCurrent := next.State == mqttAuthorityServing && next.Gen == prev.Gen
	if !wasCurrent || stillCurrent {
		return nil
	}
	if t.Closure == nil || t.Closure.Gen != prev.Gen {
		return fmt.Errorf("transition %q makes generation %q noncurrent without closing it: a "+
			"noncurrent generation is a discarded one", t.Attempt.Txn, prev.Gen)
	}
	_, ord, _ := mqttParseGenToken(prev.Gen)
	if next.Life == nil || mqttOutstandingIndex(next.Life, ord) < 0 || mqttOutstandingIndex(prev.Life, ord) >= 0 {
		return fmt.Errorf("transition %q makes generation %q noncurrent without enrolling it "+
			"outstanding in the same batch", t.Attempt.Txn, prev.Gen)
	}
	return nil
}

// mqttGenRecordWindow is when a generation's own evidence record may be created: the records
// the boundary cannot see a kind for, written outside any transition.
type mqttGenRecordWindow uint8

const (
	// mqttGenRecordWhileOpen: the generation is noncurrent and outstanding at phase open — its
	// completion certificate, which certifies a retirement already under way.
	mqttGenRecordWhileOpen mqttGenRecordWindow = iota + 1
	// mqttGenRecordWhileLive: the generation is current under any tenure, or outstanding at
	// phase open — affine readiness. (A connection-close certificate is NOT fenced: it is written
	// at every close, publishDurableCloseCert.)
	mqttGenRecordWhileLive
)

// errMQTTFencedRecordNotAdmitted: a background writer of a fenced generation record was refused
// admission because foreground work of the client is announced. Nothing was sent.
var errMQTTFencedRecordNotAdmitted = errors.New("the fenced record's write was not admitted: foreground work is announced")

// createFencedGenRecord creates one of generation gen's evidence records, create-only, in a batch
// with the authority rewritten as read: the same fence as an abort, so a record of a generation
// that is fenced, retired or of a retiring lifecycle is never written, and a writer paused across
// any authority write is refused by the CAS. An authority that moved under it is contention and
// is retried, bounded; a conflict with the record's subject already taken is returned for the
// caller's read-back to decide. admit, when set, is asked before every attempt: each one rewrites
// the authority, so a background writer yields to announced foreground work per attempt.
func (jsa *mqttJSA) createFencedGenRecord(id, epoch, gen, subj string, body []byte, w mqttGenRecordWindow, admit func() bool) error {
	var err error
	for i := 0; i < mqttAuthorityFenceAttempts; i++ {
		if admit != nil && !admit() {
			return errMQTTFencedRecordNotAdmitted
		}
		if err = jsa.createFencedGenRecordOnce(id, epoch, gen, subj, body, w); err == nil || !mqttIsRecordCASRejection(err) {
			return err
		}
		if smsg, lerr := jsa.loadLastMsgFor(mqttSessStreamName, subj); lerr != nil || smsg != nil {
			return err
		}
	}
	return err
}

func (jsa *mqttJSA) createFencedGenRecordOnce(id, epoch, gen, subj string, body []byte, w mqttGenRecordWindow) error {
	al := jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid || al.rec.Life == nil {
		return fmt.Errorf("writing %q: the authority it is fenced by reads %v", subj, al.status)
	}
	if al.rec.Life.Phase == mqttLifecycleRetiring {
		return fmt.Errorf("writing %q: lifecycle %d is retiring", subj, al.rec.Life.L)
	}
	s, entry, _ := mqttGenStandingIn(al, gen)
	open := s == mqttGenOutstanding && entry.Phase == mqttOutstandingOpen
	if !open && !(w == mqttGenRecordWhileLive && s == mqttGenCurrent) {
		return fmt.Errorf("writing %q: generation %q reads %v, outside the window its record is written in",
			subj, gen, s)
	}
	return jsa.createBesideAuthority(id, al, subj, body, subj)
}

// createBesideAuthority creates subj create-only in one batch with the authority al read,
// rewritten unchanged at the sequence it was read at: a fence that moves no revision. label names
// the write in an acknowledgement mismatch.
func (jsa *mqttJSA) createBesideAuthority(id string, al mqttAuthorityLoad, subj string, body []byte, label string) error {
	authBody, merr := json.Marshal(al.rec)
	if merr != nil {
		return merr
	}
	if h := mqttTestBesideAuthorityPresendV.Load(); h != nil {
		(*h)(subj)
	}
	jsa.mu.Lock()
	batchID := jsa.nuid.Next()
	jsa.mu.Unlock()
	rm, rh := mqttFrameBatchWrite(0, body, batchID, 1, false)
	am, ah := mqttFrameBatchWrite(al.seq, authBody, batchID, 2, true)
	bi, err := jsa.newAtomicBatchRequest(mqttJSAMsgStore, []mqttBatchMsg{
		{subj: subj, hdr: rh, msg: rm},
		{subj: mqttOutboundAuthoritySubject(getHash(id)), hdr: ah, msg: am},
	})
	if err != nil {
		return err
	}
	resp := bi.(*JSPubAckResponse)
	if aerr := resp.ToError(); aerr != nil {
		return aerr
	}
	return mqttCheckTransitionBatchAck(resp, batchID, 2, label)
}
