// Copyright 2026 The mqtt-nats-bridge Authors
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
	"github.com/nats-io/nuid"
	"sync/atomic"
	"testing"
)

// Fixtures for the ORDINARY PUBLISH acquisition on the real store (F-105 row 2, consult 29).
//
// An ordinary acquisition is ONE authority batch — the register's token and the action record
// together — so a fixture that wants to intercept it intercepts that batch
// (jsa.testTransitionCommit), not the register store the slot machine's own CAS used to go
// through. The drain still clears its token through the register store, so hooks on a DRAIN stay
// where they were.

// testStampAcceptedConnection gives the fixture connection what an accepted CONNECT gives a real
// one: its connection identity and output binding (createMQTTClient), and the entitlement the
// accept loop recorded — the standing serving authority's generation and tenure.
func testStampAcceptedConnection(t *testing.T, e *testInflightEnv) {
	t.Helper()
	if e.c.cid == 0 {
		testConnIdentity(t, e, e.c)
	}
	if e.c.outBind == nil {
		e.c.outBind = mqttBindConnection(e.c.cid)
	}
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the fixture has no serving authority to accept the connection under: %+v", al)
	}
	e.c.setOutboundEntitlement(mqttOutboundEntitlement{Gen: al.rec.Gen, Tenure: al.rec.Tenure})
	if ent, ok := e.c.outboundEntitlement(); !ok || ent.Gen != al.rec.Gen || ent.Tenure != al.rec.Tenure {
		t.Fatalf("the fixture connection carries entitlement %+v (ok=%v), want the serving %s/%s",
			ent, ok, al.rec.Gen, al.rec.Tenure)
	}
}

// testInstallPassThroughSeams installs register load and store seams that go straight to the real
// store, so a test can chain its own faults onto them. They model nothing: every read and write
// is JetStream's.
func testInstallPassThroughSeams(e *testInflightEnv) {
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		return e.asm.jsa.storeInflightCAS(subject, expect, r)
	}
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		return e.asm.jsa.loadInflightRecord(subject)
	}
	e.asm.mu.Unlock()
}

// testSetTransitionCommit installs a commit seam for the rest of the test.
func testSetTransitionCommit(t *testing.T, e *testInflightEnv,
	hook func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error)) {
	t.Helper()
	e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionCommit.Store(nil) })
}

// testAcquisitionTokenOf reports the output-custody token an acquire-action transition installs on
// its register member, or empty for any other transition.
func testAcquisitionTokenOf(tr *mqttAuthorityTransition) string {
	if tr == nil || tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionAcquireAction {
		return _EMPTY_
	}
	for _, m := range tr.Extra {
		if mqttIsF105Subject(m.subj) || m.expect == 0 {
			continue
		}
		if _, perr := mqttParseInflightSubject(m.subj); perr != nil {
			continue
		}
		if rec, derr := mqttDecodeInflightRecord(m.body); derr == nil {
			return rec.ActionToken
		}
	}
	return _EMPTY_
}

// testRequireLandedAcquisition asserts, from the store, that an acquire-action transition that
// reported success landed BOTH carriers of one acquisition: the register carries its token at
// the counter the action record names, and the record exists for that lineage and counter.
func testRequireLandedAcquisition(t *testing.T, e *testInflightEnv, tr *mqttAuthorityTransition) {
	t.Helper()
	for _, m := range tr.Extra {
		if mqttIsF105Subject(m.subj) || m.expect == 0 {
			continue
		}
		name, perr := mqttParseInflightSubject(m.subj)
		if perr != nil {
			continue
		}
		want, derr := mqttDecodeInflightRecord(m.body)
		if derr != nil {
			t.Errorf("the acquisition's token member does not decode: %v", derr)
			return
		}
		cur, smsg, lerr := e.asm.jsa.loadInflightRecord(m.subj)
		if lerr != nil || cur == nil || smsg == nil || cur.ActionToken != want.ActionToken || cur.Acq != want.Acq {
			t.Errorf("the landed acquisition's register does not carry its token at counter %d: %+v (%v)",
				want.Acq, cur, lerr)
			return
		}
		asubj := mqttActionSubject(getHash(e.sess.id), name.gen, cur.Lin, cur.Acq)
		amsg, aerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, asubj)
		if aerr != nil || amsg == nil {
			t.Errorf("the landed acquisition has no action record on %q: %v", asubj, aerr)
			return
		}
		a, adec := mqttDecodeOutboundAction(amsg.Data, e.sess.id, e.asm.sessStreamEpoch, name.gen, cur.Lin, cur.Acq)
		if adec != nil || a.PI != cur.PI || a.Kind != mqttActionPublish && a.Kind != mqttActionPubRel {
			t.Errorf("the landed action record does not describe the token's acquisition: %+v (%v)", a, adec)
		}
		return
	}
	t.Errorf("the acquire-action transition carried no register token member")
}

// testLoseNextAcquisitionResponse makes the next ACQUISITION batch land and then report failure —
// the lost-response shape — once it has proven both carriers landed. Every other transition, and
// an acquisition batch the store itself refused, is untouched.
func testLoseNextAcquisitionResponse(t *testing.T, e *testInflightEnv) *atomic.Bool {
	t.Helper()
	lost := &atomic.Bool{}
	testSetTransitionCommit(t, e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if lost.Load() || testAcquisitionTokenOf(tr) == _EMPTY_ {
			return send()
		}
		seq, err := send()
		if err != nil {
			return seq, err // did not land: not the shape this seam injects
		}
		testRequireLandedAcquisition(t, e, tr)
		lost.Store(true)
		return 0, errors.New("test: the acquisition landed but its response was lost")
	})
	return lost
}

// testPublishOutputFor is the output object beginSendLocked creates for a section on this
// connection; a test that builds its token by hand builds this beside it.
func testPublishOutputFor(e *testInflightEnv, ref mqttInflightSlotRef, token string) *mqttPublishOutput {
	return &mqttPublishOutput{guard: &e.c.outFrames, gen: ref.name.gen, pi: ref.name.pi, token: token}
}

// testAcquireSlotOnlyEnv is the pre-row-2 acquisition fixture on a SHARED-group slot: the map
// store, a quiescent PUBLISH slot, no exchange. A slot-only class keeps the slot machine's own
// CAS until 7a, so a property of that CAS — its internal base refresh — is still tested here, on
// the class that still takes it (consult 29's rule; the ordinary route's equivalent is
// TestMQTTPublishActionRecordsTheCounterItLandedOnAnOrdinarySlot).
func testAcquireSlotOnlyEnv(t *testing.T, clientID string, pi uint16) (*testInflightEnv, string, mqttSendToken) {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForPubRelDelivery(t, e)
	subject := testVetoSeedSlot(t, e, pi, mqttInflightPublish, mqttNewInflightNonce())
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the seeded slot: rec=%+v err=%v", cur, err)
	}
	quiet := *cur
	quiet.ActionToken, quiet.ActionClaim, quiet.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
	quiet.Dur = mqttSharedConsumerDurablePrefix + e.sess.idHash + "_" + nuid.Next()
	testRegisterSourceReply(&quiet)
	seq, serr := e.asm.testInflightStore(subject, smsg.Sequence, &quiet)
	if serr != nil {
		t.Fatalf("quiescing the slot: %v", serr)
	}
	if !mqttDurIsSlotOnlyClass(quiet.Dur) {
		t.Fatalf("the fixture must be slot-only, got %q", quiet.Dur)
	}
	ref := mqttSlotRefFromRecord(subject, seq, &quiet)
	if ref == nil {
		t.Fatal("the seeded slot must yield a reference")
	}
	return e, subject, mqttSendToken{token: mqttNewInflightNonce(), slot: *ref}
}
