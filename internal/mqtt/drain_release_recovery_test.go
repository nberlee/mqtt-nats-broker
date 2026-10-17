// Copyright 2025 The mqtt-nats-broker Authors
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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// A RECOVERED RELEASE REQUEST SETTLES WITHOUT A DELIVERY (F-098 L4, holistic review round 3,
// finding 2). A drained PUBACK persists the request and the process stops before the claim
// removal lands. On the next resume the record still carries the claim and the request — and
// every event that drives a finalize is SOURCE-side: a redelivery, a PUBREL correlation, or an
// in-memory retry chain the restart lost. With the source expired and the consumer gone, the
// obligation had no clock at all and the drain stayed outstanding for the life of the session.
//
// The fixture is that session: a restored drain with a valid request, NO delivery index and no
// consumer, and nothing else to happen.
//
// Mutation M120: drop the enrolment.
func TestMQTTRecoveredDrainReleaseSettlesWithoutADelivery(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "rel-recovered")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	cur, _, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || cur == nil {
		t.Fatalf("the fixture needs a planted register: %+v %v", cur, err)
	}
	rel := &mqttPersistedDrainRelease{Subject: subject, Nonce: cur.Nonce, Acq: cur.Acq, Attempt: 1}

	e.sess.mu.Lock()
	d := &mqttDrain{
		cc:   &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()},
		qos:  1,
		seqs: map[uint64]*drainSeq{sseq: {claimed: true, pi: pi, rel: rel}},
	}
	e.sess.drains = map[string]*mqttDrain{"relrec/topic": d}
	// DE-INDEXED, which is the consumer-gone shape: restoreDrains removes the delivery
	// index when it cannot install a subscription, and a release obligation must still
	// reach its target.
	e.sess.drainByDur = nil
	e.sess.c = e.c
	e.sess.mu.Unlock()

	e.sess.enrolRecoveredDrainReleases(e.c)

	deadline := time.Now().Add(15 * time.Second)
	for {
		settled := func() error {
			if occ, _, lerr := e.asm.jsa.loadInflightRecord(subject); lerr != nil || occ != nil {
				return fmt.Errorf("the register still holds identifier %d (err %v)", pi, lerr)
			}
			e.sess.mu.Lock()
			defer e.sess.mu.Unlock()
			if st := d.seqs[sseq]; st != nil && (st.claimed || st.rel != nil) {
				return fmt.Errorf("the obligation is still outstanding: claimed=%v rel=%+v", st.claimed, st.rel)
			}
			return nil
		}()
		if settled == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a recovered release request never settled on a session with no deliveries: %v", settled)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// THE RESUME ENROLS THEM, and this pins the CALL SITE rather than the helper: a helper nothing
// calls is a continuation that never runs, and the fixture above drives it directly.
func TestResumeEnrolsRecoveredDrainReleases(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	calls := func(fn *ast.FuncDecl, name string) bool {
		found := false
		ast.Inspect(fn, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == name {
				found = true
			}
			return !found
		})
		return found
	}
	var restorers []string
	for _, name := range censusProductionFiles(t) {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !calls(fn, "restoreDrains") {
				continue
			}
			restorers = append(restorers, fn.Name.Name)
			if !calls(fn, "enrolRecoveredDrainReleases") {
				t.Errorf("%s (%s) restores drains but never enrols their recovered release requests: "+
					"an obligation whose source is gone would then have no clock at all (L4)", fn.Name.Name, name)
			}
		}
	}
	if len(restorers) == 0 {
		t.Fatal("no production function restores drains: this tripwire is checking a predicate nothing satisfies")
	}
}

// A WITHHELD CLAIM REMOVAL KEEPS THE REQUEST'S IDENTITY (F-098 L4, holistic review round 4,
// finding 1). The release settles Rel(k) against a successor at k+1: the successor is PRESERVED
// and the request is discharged. But the claim removal that consumes it can still fail — a fence
// refusal, or a definitely-failed save — and if the request were cleared before that, the retry
// would find no request, bind to whatever occupies the subject (the successor this very call
// preserved) and delete it.
//
// Mutation M121: clear the request before the transaction, as round 4 found it.
func TestMQTTDrainFinalizeKeepsItsRequestAcrossAFailedClaimRemoval(t *testing.T) {
	// NOT parallel: the save hook below is process-global, and a hook that outlives its test
	// or runs beside another one fails the whole binary rather than one case.
	e := testNewInflightEnv(t, "rel-rebind")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	cur, smsg, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || cur == nil {
		t.Fatalf("the fixture needs a planted register: %+v %v", cur, err)
	}
	// The subject holds a SUCCESSOR of the transmission the request names.
	rel := &mqttPersistedDrainRelease{Subject: subject, Nonce: cur.Nonce, Acq: cur.Acq, Attempt: 1}
	succ := *cur
	succ.Acq = cur.Acq + 1
	if _, werr := e.asm.jsa.storeInflightCAS(subject, smsg.Sequence, &succ); werr != nil {
		t.Fatalf("installing the successor: %v", werr)
	}

	e.sess.mu.Lock()
	d := &mqttDrain{
		cc:   &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()},
		qos:  1,
		seqs: map[uint64]*drainSeq{sseq: {claimed: true, pi: pi, rel: rel}},
	}
	e.sess.drains = map[string]*mqttDrain{"relrebind/topic": d}
	e.sess.drainByDur = map[string]*mqttDrain{dur: d}
	e.sess.c = e.c
	e.sess.mu.Unlock()

	// ONLY the claim-removal save fails, so the release's own prerequisite saves land and the
	// finalize reaches the transaction it is being tested on (C15's rule).
	restore := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != e.sess {
			return nil
		}
		hs.mu.Lock()
		removing := false
		for _, dr := range hs.drainByDur {
			for _, st := range dr.seqs {
				if st != nil && !st.claimed && st.unclaimUnconfirmed {
					removing = true
				}
			}
		}
		hs.mu.Unlock()
		if !removing {
			return nil
		}
		return errDrainTestSaveFailed
	})
	t.Cleanup(restore)

	for attempt := 1; attempt <= 2; attempt++ {
		if !e.sess.finalizeDrainDelivery(e.c, dur, sseq, _EMPTY_) {
			t.Fatalf("attempt %d: the finalize did not handle the sequence", attempt)
		}
		after, _, lerr := e.asm.jsa.loadInflightRecord(subject)
		if lerr != nil || after == nil {
			t.Fatalf("attempt %d: the SUCCESSOR was deleted on evidence about its predecessor: %+v %v",
				attempt, after, lerr)
		}
		if after.Acq != succ.Acq {
			t.Fatalf("attempt %d: the occupant is %+v, want the untouched successor at Acq %d",
				attempt, after, succ.Acq)
		}
		e.sess.mu.Lock()
		st := d.seqs[sseq]
		held := st != nil && st.rel != nil && st.rel.Acq == rel.Acq && st.rel.Subject == rel.Subject
		claimed := st != nil && st.claimed
		e.sess.mu.Unlock()
		if !held {
			t.Fatalf("attempt %d: the request was consumed while its claim removal was unproven, "+
				"so the next attempt binds to whatever occupies the subject", attempt)
		}
		if !claimed {
			t.Fatalf("attempt %d: a definitely-failed removal must leave the claim standing", attempt)
		}
	}
}

// A DEFINITELY-FAILED CLAIM REMOVAL SETTLES ITSELF (F-098 L4, holistic review round 5,
// finding 2). The finalize released its register, took the record fence, and its claim-removal
// save definitely failed: claim and request are restored. "The message stays pending and is
// redelivered" is the recovery route — but only while the source can still be delivered. With
// the register already gone and the source expired, nothing redelivers, and retirement refuses
// while a request stands, so the drain would sit outstanding for the life of the session.
//
// The fixture removes every other clock: no consumer, no source delivery, one refusal.
//
// Mutation M125: drop the retry after a definitely-failed claim removal.
func TestMQTTDrainFinalizeSettlesItselfAfterADefiniteRemovalFailure(t *testing.T) {
	// NOT parallel: the save hook below is process-global.
	e := testNewInflightEnv(t, "rel-definite")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	cur, _, err := e.asm.jsa.loadInflightRecord(subject)
	if err != nil || cur == nil {
		t.Fatalf("the fixture needs a planted register: %+v %v", cur, err)
	}
	rel := &mqttPersistedDrainRelease{Subject: subject, Nonce: cur.Nonce, Acq: cur.Acq, Attempt: 1}

	e.sess.mu.Lock()
	d := &mqttDrain{
		cc:   &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + nuid.Next()},
		qos:  1,
		seqs: map[uint64]*drainSeq{sseq: {claimed: true, pi: pi, rel: rel}},
	}
	e.sess.drains = map[string]*mqttDrain{"reldefinite/topic": d}
	e.sess.drainByDur = map[string]*mqttDrain{dur: d}
	e.sess.c = e.c
	e.sess.mu.Unlock()

	// ONE refusal, aimed at the claim-removal save: the retry must then be able to finish.
	var refused atomic.Int64
	restore := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != e.sess {
			return nil
		}
		hs.mu.Lock()
		removing := false
		for _, dr := range hs.drainByDur {
			for _, st := range dr.seqs {
				if st != nil && !st.claimed && st.unclaimUnconfirmed {
					removing = true
				}
			}
		}
		hs.mu.Unlock()
		if !removing || !refused.CompareAndSwap(0, 1) {
			return nil
		}
		return errDrainTestSaveFailed
	})
	t.Cleanup(restore)

	if !e.sess.finalizeDrainDelivery(e.c, dur, sseq, _EMPTY_) {
		t.Fatal("the finalize did not handle the sequence")
	}
	if refused.Load() == 0 {
		t.Fatal("the fixture never refused a claim-removal save, so nothing here is tested")
	}

	// NOTHING else is invoked from here: no delivery, no second finalize call.
	deadline := time.Now().Add(20 * time.Second)
	for {
		e.sess.mu.Lock()
		st := d.seqs[sseq]
		outstanding := st != nil && (st.claimed || st.rel != nil)
		e.sess.mu.Unlock()
		if !outstanding {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("a definitely-failed claim removal left the drain outstanding with no clock")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
