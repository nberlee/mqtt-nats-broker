//go:build mqttport_fulltests

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
	"strings"
	"sync/atomic"
	"testing"
)

// An ordinary admission racing its generation's closure (F-105 WP-F W2, row C1/M1).
//
// The closure is permanent: once it commits, nothing new may be admitted under the generation it
// closed, because the closure's enrolment is the complete inventory recovery will work from. An
// admission that composed against the serving authority and its open ledger, and lost the race to
// the closure, must therefore leave NOTHING - no register, marker, exchange, resource edge or duty
// - and must release its custody reservation, so the session's Receive Maximum is not charged for
// an obligation that does not exist.

// testAdmitLosesToClosureRig is one ordinary QoS 1 allocation, tracked and ready to admit at
// Receive Maximum 1, on a connection whose transport records every byte.
type testAdmitLosesToClosureRig struct {
	e     *testInflightEnv
	pi    uint16
	nonce string
	gen   string
	tc    *testFrameConn
}

func testNewAdmitLosesToClosureRig(t *testing.T, clientID string) *testAdmitLosesToClosureRig {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, e)
	testCustodyRM1(t, e.sess)
	// The source claimed by name through the authority, as production claims it before CREATE:
	// without it readiness refuses before the ladder and no race is ever staged.
	e.admissionPrerequisites(t, testRealPubRelSrcDur, 1)
	tc := &testFrameConn{}
	if e.c.nc != nil {
		t.Fatal("fixture: the rig connection already has a transport")
	}
	e.c.nc, e.c.rawConn = tc, tc
	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 1))
	gen := e.sess.gen
	e.sess.mu.Unlock()
	if pi == 0 {
		t.Fatal("fixture: no identifier was allocated")
	}
	e.acceptUnderServingAuthority(t)
	return &testAdmitLosesToClosureRig{e: e, pi: pi, nonce: e.nonceOf(pi), gen: gen, tc: tc}
}

// closeGeneration commits the production closure of the generation the admission was decided
// under, through the bounded closing episode.
func (r *testAdmitLosesToClosureRig) closeGeneration(t *testing.T) error {
	jsa := &r.e.asm.jsa
	al := jsa.loadOutboundAuthority(r.e.sess.id, r.e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		t.Errorf("fixture: no authority to close: %+v", al)
		return nil
	}
	_, _, pubRelDur := mqttPubRelNames(r.e.sess.idHash, r.gen)
	return jsa.closeGenerationEnrolled(r.e.sess.id, r.e.asm.sessStreamEpoch, al.rec.Gen, al.rec.Tenure,
		al.rec.OwnerTxn, false, mqttClosingTargets{PubRelDurable: pubRelDur})
}

func (r *testAdmitLosesToClosureRig) admit() bool {
	return r.e.sess.admitInflightPublish(r.e.c, r.pi, 1, testRealPubRelSrcDur, testRealPubRelSrcSeq,
		r.nonce, _EMPTY_, _EMPTY_)
}

// requireNothingAdmitted asserts that no member of the admission stands, nothing was written, and
// the custody reservation is gone.
func (r *testAdmitLosesToClosureRig) requireNothingAdmitted(t *testing.T) {
	t.Helper()
	e := r.e
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch
	if _, smsg := e.loadRegister(t, r.pi); smsg != nil {
		t.Fatalf("a register stands for identifier %d after its admission lost to the closure", r.pi)
	}
	if _, smsg := e.loadMarker(t, r.pi); smsg != nil {
		t.Fatalf("an unsent marker stands for identifier %d after its admission lost to the closure", r.pi)
	}
	if xm, err := jsa.loadLastMsgFor(mqttInflightStreamName, mqttExchangeSubject(getHash(e.sess.id), r.gen, r.nonce)); xm != nil ||
		!IsNatsErr(err, JSNoMessageFoundErr) {
		t.Fatalf("an exchange stands for %q after its admission lost to the closure: %+v (%v)", r.nonce, xm, err)
	}
	res, unreadable, err := e.asm.enumerateGenerationResources(e.sess.id, epoch, r.gen)
	if err != nil || unreadable != 0 {
		t.Fatalf("enumerating the generation's resources: %v (%d unreadable)", err, unreadable)
	}
	if len(res) == 0 {
		t.Fatal("fixture: the generation enumerates no resource, so the edge assertion is vacuous")
	}
	for _, rr := range res {
		for _, d := range rr.Deps {
			if d.Nonce == r.nonce {
				t.Fatalf("resource %q carries an edge for %q after its admission lost to the closure: %+v",
					rr.Inc, r.nonce, d)
			}
		}
	}
	led, _, lerr := jsa.loadGenLedger(e.sess.id, epoch, r.gen)
	if lerr != nil || led == nil {
		t.Fatalf("reading the generation ledger: %+v (%v)", led, lerr)
	}
	for ord := uint64(1); ord <= led.Hi; ord++ {
		d, _, derr := jsa.loadDutyEntryAt(e.sess.id, epoch, r.gen, ord)
		if derr != nil {
			t.Fatalf("reading duty %d: %v", ord, derr)
		}
		if d != nil && d.Ref == r.nonce {
			t.Fatalf("duty %d stands for %q after its admission lost to the closure: %+v", ord, r.nonce, d)
		}
	}
	if len(r.tc.written) != 0 {
		t.Fatalf("the connection carried %q for an admission that lost to the closure", string(r.tc.written))
	}
	if testCustodyHasAdmission(e.sess, r.nonce) {
		t.Fatal("the custody reservation of an admission that lost to the closure survived the ladder")
	}
}

// TestF105OrdinaryAdmissionLosesToAClosure pins the race both ways the store can decide it.
func TestF105OrdinaryAdmissionLosesToAClosure(t *testing.T) {
	// The closure commits in the admission's presend window: after every one of the admission's
	// boundary reads passed, so only the store's own sequence CAS on its batch (the authority's
	// revision and the generation ledger the closure sealed) can refuse it.
	t.Run("the closure commits after every boundary check", func(t *testing.T) {
		t.Parallel()
		r := testNewAdmitLosesToClosureRig(t, "w2-admit-closure-presend")
		jsa := &r.e.asm.jsa
		var fired atomic.Bool
		var closeErr error
		pre := func(tr *mqttAuthorityTransition) {
			// The closure's own commit re-enters this seam; only the admission's first attempt is
			// the window.
			if tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionAdmitExchange && fired.CompareAndSwap(false, true) {
				closeErr = r.closeGeneration(t)
			}
		}
		jsa.testTransitionPresend.Store(&pre)
		t.Cleanup(func() { jsa.testTransitionPresend.Store(nil) })
		var admitErr error
		var observed atomic.Bool
		commit := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			seq, err := send()
			if tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionAdmitExchange && observed.CompareAndSwap(false, true) {
				admitErr = err
			}
			return seq, err
		}
		jsa.testTransitionCommit.Store(&commit)
		t.Cleanup(func() { jsa.testTransitionCommit.Store(nil) })

		admitted := r.admit()
		if !fired.Load() {
			t.Fatal("fixture: the admission never reached its presend window, so no race was staged")
		}
		if closeErr != nil {
			t.Fatalf("fixture: the closure did not commit in the admission's window: %v", closeErr)
		}
		if admitted {
			t.Fatal("an ordinary admission was admitted over its generation's closure")
		}
		if admitErr == nil {
			t.Fatal("the admission's batch committed over the closure")
		}
		if ae := jsAPIError(admitErr); ae == nil || !isQoS2CASRejection(ae) {
			t.Fatalf("the admission was refused by something other than the store's sequence CAS: %v", admitErr)
		}
		r.requireNothingAdmitted(t)
	})

	// The closure commits after the admission composed but before its store comparisons: the
	// pre-submit comparison of the captured predecessor with the stored one refuses it, before any
	// batch reaches the store.
	t.Run("the closure commits before the store comparisons", func(t *testing.T) {
		t.Parallel()
		r := testNewAdmitLosesToClosureRig(t, "w2-admit-closure-precompare")
		jsa := &r.e.asm.jsa
		var fired atomic.Bool
		var closeErr, admitErr error
		commit := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionAdmitExchange && fired.CompareAndSwap(false, true) {
				closeErr = r.closeGeneration(t)
				seq, err := send()
				admitErr = err
				return seq, err
			}
			return send()
		}
		jsa.testTransitionCommit.Store(&commit)
		t.Cleanup(func() { jsa.testTransitionCommit.Store(nil) })

		admitted := r.admit()
		if !fired.Load() {
			t.Fatal("fixture: the admission never reached its commit, so no race was staged")
		}
		if closeErr != nil {
			t.Fatalf("fixture: the closure did not commit before the admission's comparisons: %v", closeErr)
		}
		if admitted {
			t.Fatal("an ordinary admission was admitted over its generation's closure")
		}
		if admitErr == nil {
			t.Fatal("the admission's batch committed over the closure")
		}
		// THE PRE-SUBMIT COMPARE, not the store: a refusal by the store's CAS here means the
		// captured predecessor reached the stream unchecked against the one standing.
		if ae := jsAPIError(admitErr); ae != nil && isQoS2CASRejection(ae) {
			t.Fatalf("the admission reached the store's CAS: the pre-submit comparison let a stale "+
				"predecessor through: %v", admitErr)
		}
		if !strings.Contains(admitErr.Error(), "is conditioned on the authority at sequence") {
			t.Fatalf("the admission was refused by something other than the pre-submit comparison "+
				"of its predecessor with the stored authority: %v", admitErr)
		}
		r.requireNothingAdmitted(t)
	})
}
