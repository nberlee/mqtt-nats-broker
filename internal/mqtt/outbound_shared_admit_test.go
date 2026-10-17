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
	"sync/atomic"
	"testing"
)

// testSharedAdmitRig is one session joined to one shared group, with a source message published
// through the group consumer, ready to admit its delivery.
type testSharedAdmitRig struct {
	e    *testInflightEnv
	key  string
	dur  string
	sseq uint64
	acq  mqttSharedAcqRef
}

func newTestSharedAdmitRig(t *testing.T, clientID string) *testSharedAdmitRig {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	// A group name per client, so a process-wide test observer can tell this rig's source
	// replies from a parallel test's.
	group, filter := "g"+getHashSize(clientID, 8), "foo.bar"
	key := "$share/" + group + "/" + filter
	_, _, dur := mqttSharedNames(group, filter, 1)
	sseq := e.publishSourceGroup(t, dur, group, filter, filter)
	acq := e.joinSharedForTest(t, key, group, filter, false)
	e.installServingAuthority(t)
	e.acceptUnderServingAuthority(t)
	return &testSharedAdmitRig{e: e, key: key, dur: dur, sseq: sseq, acq: acq}
}

// admit allocates and admits the rig's delivery under dep, and reports whether it landed.
func (r *testSharedAdmitRig) admit(t *testing.T, dep *mqttAdmitShared) (uint16, bool) {
	t.Helper()
	e := r.e
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(r.dur, testInflightAckSubject(r.dur, r.sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d isNew=%v", pi, isNew)
	}
	return pi, e.sess.admitSharedInflightPublish(e.c, pi, 1, r.dur, r.sseq, e.nonceOf(pi), dep)
}

func (r *testSharedAdmitRig) dep() *mqttAdmitShared {
	r.e.sess.mu.Lock()
	defer r.e.sess.mu.Unlock()
	return &mqttAdmitShared{Acq: r.acq, Tenure: r.e.sess.sharedTenure}
}

// exchangeOf reads the exchange standing beside the register on pi, if any.
func (r *testSharedAdmitRig) exchangeOf(t *testing.T, pi uint16) (*mqttInflightRecord, *mqttOutboundExchange) {
	t.Helper()
	e := r.e
	reg, smsg, err := e.asm.loadInflightRecord(mqttInflightSubject(e.sess.idHash, e.sess.gen, pi))
	if err != nil {
		t.Fatalf("loading the register: %v", err)
	}
	if smsg == nil || reg == nil {
		return nil, nil
	}
	x, _, xerr := e.sess.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, reg.Lin)
	if xerr != nil {
		t.Fatalf("loading the exchange: %v", xerr)
	}
	return reg, x
}

// T1: a shared QoS 1 delivery is admitted through the authority — register, marker and exchange in
// one batch — and the exchange names the EXACT acquisition its incarnation is held by.
func TestSharedQoS1AdmitsThroughTheAuthority(t *testing.T) {
	t.Parallel()
	r := newTestSharedAdmitRig(t, "shared-admit-t1")
	pi, ok := r.admit(t, r.dep())
	if !ok {
		t.Fatal("a shared delivery with a held acquisition was refused")
	}
	reg, x := r.exchangeOf(t, pi)
	if reg == nil || x == nil {
		t.Fatalf("the admission left register=%v exchange=%v", reg != nil, x != nil)
	}
	if x.SharedAcq == nil || *x.SharedAcq != r.acq {
		t.Fatalf("the exchange depends on %+v, want the joined acquisition %+v", x.SharedAcq, r.acq)
	}
	if x.QoS != 1 || x.Phase != mqttExchangePublish || x.PI != pi {
		t.Fatalf("exchange = %+v", x)
	}
	if err := mqttCheckSharedAcqPair(reg, x); err != nil {
		t.Fatalf("the admitted pair violates the pair rule: %v", err)
	}
}

// T1 (refusals at the entrance): no dependency, or QoS 2, never reaches a write.
func TestSharedAdmissionRefusesWithoutADependencyOrAtQoS2(t *testing.T) {
	t.Parallel()
	r := newTestSharedAdmitRig(t, "shared-admit-entrance")
	if pi, ok := r.admit(t, nil); ok {
		t.Fatal("a shared delivery with no acquisition was admitted")
	} else if reg, _ := r.exchangeOf(t, pi); reg != nil {
		t.Fatal("a refused shared admission wrote a register")
	}
	e := r.e
	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(r.dur, testInflightAckSubject(r.dur, r.sseq+1, 1))
	e.sess.mu.Unlock()
	if e.sess.admitSharedInflightPublish(e.c, pi, 2, r.dur, r.sseq+1, e.nonceOf(pi), r.dep()) {
		t.Fatal("a shared delivery was admitted at QoS 2")
	}
}

// T2: an acquisition the ledger no longer lists is not a dependency anything may take.
func TestSharedAdmissionRefusesAReleasedAcquisition(t *testing.T) {
	t.Parallel()
	r := newTestSharedAdmitRig(t, "shared-admit-t2")
	dep := r.dep()
	e := r.e
	// A SECOND holder of the same incarnation, so the release leaves the group serving it: only
	// the ledger's listing of THIS acquisition can refuse the admission.
	group := "g" + getHashSize("shared-admit-t2", 8)
	if _, out, err := e.sess.acquireShared(e.c, group, "foo.bar", 1024); err != nil || out != mqttSharedAcquired {
		t.Fatalf("acquiring a second holder: %v %v", out, err)
	}
	if done, err := e.sess.jsa.executeSharedRelease(e.sess.sharedEpoch(), e.sess.id, e.sess.gen, r.acq); err != nil || !done {
		t.Fatalf("releasing the acquisition: done=%v err=%v", done, err)
	}
	pi, ok := r.admit(t, dep)
	if ok {
		t.Fatal("a delivery was admitted against an acquisition the ledger no longer lists")
	}
	if _, x := r.exchangeOf(t, pi); x != nil {
		t.Fatal("a refused admission left an exchange")
	}
}

// T3: the ledger still lists the acquisition, but the group no longer serves its incarnation.
func TestSharedAdmissionRefusesARetiringIncarnation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		mount func(g *mqttSharedGroupRecord, inc uint64)
	}{
		{"another incarnation is live", func(g *mqttSharedGroupRecord, inc uint64) {
			g.Live = &mqttSharedLive{Inc: inc + 1, Holders: 1}
			g.Next = inc + 2
		}},
		{"this incarnation is retiring", func(g *mqttSharedGroupRecord, inc uint64) {
			g.Live = &mqttSharedLive{Inc: inc + 1, Holders: 1}
			g.Retiring = append(g.Retiring, inc)
			g.Next = inc + 2
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newTestSharedAdmitRig(t, "shared-admit-t3-"+map[bool]string{true: "a", false: "b"}[tc.name == "another incarnation is live"])
			e := r.e
			g := e.sess.jsa.loadSharedGroup(e.sess.sharedEpoch(), r.acq.Group)
			if g.status != mqttOutboundValid {
				t.Fatalf("the group record reads %v", g.status)
			}
			next := *g.rec
			tc.mount(&next, r.acq.Inc)
			body, err := json.Marshal(&next)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := e.asm.jsa.storeMsg(mqttSharedGroupSubject(r.acq.Group), -1, body); err != nil {
				t.Fatalf("mounting the group record: %v", err)
			}
			if pi, ok := r.admit(t, r.dep()); ok {
				t.Fatal("a delivery was admitted against an incarnation the group does not serve")
			} else if _, x := r.exchangeOf(t, pi); x != nil {
				t.Fatal("a refused admission left an exchange")
			}
		})
	}
}

// T4: a delivery captured under a shared tenure the ledger no longer carries is refused.
func TestSharedAdmissionRefusesAForeignLedgerTenure(t *testing.T) {
	t.Parallel()
	r := newTestSharedAdmitRig(t, "shared-admit-t4")
	dep := r.dep()
	dep.Tenure = mqttSharedTenure{Seq: dep.Tenure.Seq + 1, ID: mqttNewInflightNonce()}
	if pi, ok := r.admit(t, dep); ok {
		t.Fatal("a delivery captured under another shared tenure was admitted")
	} else if _, x := r.exchangeOf(t, pi); x != nil {
		t.Fatal("a refused admission left an exchange")
	}
}

// T5 + T6: the readiness is taken INSIDE each attempt, AFTER its authority read. An authority bump
// between the two makes the attempt's batch lose its CAS; the retry re-runs the WHOLE check, so the
// seam is reached once per attempt. A readiness computed once before the ladder (or before the
// authority read) is reached once.
func TestSharedAdmissionReadinessFollowsTheAuthorityRead(t *testing.T) {
	r := newTestSharedAdmitRig(t, "shared-admit-t5")
	e := r.e
	var calls, bumped int32
	hook := func(id string) {
		if id != e.sess.id {
			return
		}
		if atomic.AddInt32(&calls, 1) != 1 {
			return
		}
		// Advance the authority's revision between the attempt's authority read and its ledger
		// read: the attempt's batch CASes the revision it read, which no longer stands.
		al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
		if al.status != mqttOutboundValid || al.rec == nil {
			return
		}
		next := *al.rec
		next.Rev++
		next.Txn = mqttNewInflightNonce()
		b, _ := json.Marshal(&next)
		msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
		if _, err := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(e.sess.id)), hdr, msg); err == nil {
			atomic.StoreInt32(&bumped, 1)
		}
	}
	mqttTestSharedReadinessHook.Store(&hook)
	defer mqttTestSharedReadinessHook.Store(nil)

	pi, ok := r.admit(t, r.dep())
	if atomic.LoadInt32(&bumped) != 1 {
		t.Fatal("fixture error: the authority was never bumped inside the readiness window")
	}
	if n := atomic.LoadInt32(&calls); n < 2 {
		t.Fatalf("the readiness ran %d time(s): a retry after the authority moved must re-run it", n)
	}
	if !ok {
		t.Fatal("the retry against the moved authority should have landed")
	}
	if _, x := r.exchangeOf(t, pi); x == nil || x.SharedAcq == nil {
		t.Fatal("the landed retry left no exchange naming the acquisition")
	}
}

// T6's other half: a ledger change inside the readiness window is SEEN by that attempt — the
// check is not a stale observation made before the ladder.
func TestSharedAdmissionRetryReReadsReadiness(t *testing.T) {
	r := newTestSharedAdmitRig(t, "shared-admit-t6")
	e := r.e
	var calls int32
	hook := func(id string) {
		if id != e.sess.id || atomic.AddInt32(&calls, 1) != 1 {
			return
		}
		_, _ = e.sess.jsa.executeSharedRelease(e.sess.sharedEpoch(), e.sess.id, e.sess.gen, r.acq)
	}
	mqttTestSharedReadinessHook.Store(&hook)
	defer mqttTestSharedReadinessHook.Store(nil)
	if pi, ok := r.admit(t, r.dep()); ok {
		t.Fatal("a release inside the readiness window did not refuse the admission")
	} else if _, x := r.exchangeOf(t, pi); x != nil {
		t.Fatal("a refused admission left an exchange")
	}
	if atomic.LoadInt32(&calls) == 0 {
		t.Fatal("fixture error: the readiness seam was never reached")
	}
}
