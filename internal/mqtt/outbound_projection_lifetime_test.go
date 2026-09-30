// Copyright 2025 The mqtt-nats-bridge Authors
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

//go:build mqttport_fulltests

package mqtt

import (
	"sync/atomic"
	"testing"
)

// A PROJECTION LIVES ONLY WHILE CUSTODY HOLDS ITS LINEAGE (F-105 7b, plan-7b §3.7, T15). An
// ordinary pending projection that the resume installs, retains or reactivates exists only while
// the custody ledger holds its lineage, and it is removed when the ledger releases that lineage.
// Two points enforce it, both under the session lock, so for any install and any release one
// precedes the other: release first, the install gate refuses; install first, the release
// removes it. This replaces the claim rewrite that used to move a register's sequence out from
// under a paused terminal delete.

// testEndExchangeRemotely ends an admitted obligation DURABLY without touching this session's
// memory, as a completion on another broker does: the withdrawal op runs at the store layer, with
// no local hint and no local projection change.
func testEndExchangeRemotely(t *testing.T, f *testWithdrawFix) {
	t.Helper()
	e := f.e
	if _, err := e.sess.jsa.opWithdrawUnsent(&mqttWithdrawUnsentRequest{
		ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-withdraw-unsent",
		Gen: f.gen, Subject: f.subject, Nonce: f.nonce, Lin: f.lin, Licence: mqttLicenceOf(e.c),
	}); err != nil {
		t.Fatalf("ending the exchange durably: %v", err)
	}
	if x := f.exchange(t); x == nil || !mqttExchangePhaseEnded(x.Phase) {
		t.Fatalf("fixture: the exchange must have ended: %+v", x)
	}
}

// testRestoredProjection resumes after a restart and returns whether the delivery was installed.
func testRestoredProjection(t *testing.T, f *testWithdrawFix) bool {
	t.Helper()
	testRestart(f.e)
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	return testProjectionPresent(f)
}

func testProjectionPresent(f *testWithdrawFix) bool {
	f.e.sess.mu.Lock()
	defer f.e.sess.mu.Unlock()
	ack := f.e.sess.pendingPublish[f.pi]
	return ack != nil && ack.lin == f.lin
}

// T15 (M20a, M20d): a release AFTER the install removes the projection, through each of the
// three routes that apply an exchange observation.
func TestMQTTRecoveryProjectionLivesOnlyWhileCustodyHoldsItsLineage(t *testing.T) {
	t.Parallel()
	for _, route := range []struct {
		name    string
		observe func(t *testing.T, f *testWithdrawFix)
	}{
		{"target observation", func(t *testing.T, f *testWithdrawFix) {
			f.e.sess.observeCustodyTarget(f.gen, mqttCustodyTarget{kind: mqttCustodyTargetExchange, key: f.lin})
		}},
		{"census", func(t *testing.T, f *testWithdrawFix) {
			f.e.sess.rebuildCustodyForResume(f.gen)
		}},
		{"uncertain re-read", nil}, // staged below: the fault must be installed before the ending
	} {
		t.Run(route.name, func(t *testing.T) {
			t.Parallel()
			f := testWithdrawAdmitted(t, "lifetime-"+route.name, 1)
			if !testRestoredProjection(t, f) {
				t.Fatal("fixture: the resume must install the delivery's projection")
			}
			if route.observe == nil {
				// The exchange becomes uncertain first, then ends while unreadable; the re-read
				// that finally decodes it observes the ending.
				xsubj := mqttExchangeSubject(f.e.sess.idHash, f.gen, f.lin)
				var fault atomic.Bool
				fault.Store(true)
				f.e.asm.mu.Lock()
				f.e.asm.testCustodyDecodeFault = func(subject string) bool { return fault.Load() && subject == xsubj }
				f.e.asm.mu.Unlock()
				f.e.sess.rebuildCustodyForResume(f.gen)
				testEndExchangeRemotely(t, f)
				fault.Store(false)
				testCustodyKick(f.e.sess)
				testCustodyEventually(t, "the re-read removed the projection", func() bool {
					return !testProjectionPresent(f)
				})
				return
			}
			testEndExchangeRemotely(t, f)
			if !testProjectionPresent(f) {
				t.Fatal("fixture: nothing local may observe the remote ending before the route under test")
			}
			route.observe(t, f)
			if testProjectionPresent(f) {
				t.Fatalf("the %s released the lineage but its projection survived", route.name)
			}
		})
	}
}

// T15 (M31): a release BEFORE apply — the exchange ends, and is observed, between the resume's walk
// and its apply — leaves no projection: the install gate refuses a lineage the ledger no longer
// holds, and the identifier is free for new work.
func TestMQTTRecoveryInstallGateRefusesAReleasedLineage(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "lifetime-before-apply", 1)
	testCustodyRM1(t, f.e.sess)
	testRestart(f.e)
	var loads atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testResumeAuthorityLoad = func(id string) mqttAuthorityLoad {
		// The SECOND authority read is apply's: end the exchange just before it, and observe it.
		if loads.Add(1) == 2 {
			testEndExchangeRemotely(t, f)
			f.e.sess.observeCustodyTarget(f.gen, mqttCustodyTarget{kind: mqttCustodyTargetExchange, key: f.lin})
		}
		return f.e.asm.jsa.loadOutboundAuthority(id, f.e.asm.sessStreamEpoch)
	}
	f.e.asm.mu.Unlock()
	t.Cleanup(func() { f.e.asm.mu.Lock(); f.e.asm.testResumeAuthorityLoad = nil; f.e.asm.mu.Unlock() })
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if loads.Load() < 2 {
		t.Fatal("fixture: apply's authority read was never reached")
	}
	if testProjectionPresent(f) {
		t.Fatal("the install gate must refuse a lineage the ledger released before apply")
	}
	if got := testCustodyMints(f.e.sess, f.pi); got != f.pi {
		t.Fatalf("the ended exchange's identifier must be reusable by new work (maxp=1), got %d", got)
	}
}

// T15g (M34): the gate at its boundary. A lineage-less record never satisfies it and is never an
// exemption: refused, nothing installed, no mapping changed. A resume cannot reach this state (the
// shape validator rejects a lineage-less ordinary register first), so it is tested directly.
func TestMQTTInstallGateRefusesAnEmptyLineage(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "lifetime-empty-lineage", 1)
	cur, _ := f.register(t)
	rec := *cur
	rec.Lin = _EMPTY_
	p := &mqttPending{jsDur: rec.Dur, sseq: rec.Seq, regNonce: rec.Nonce}
	f.e.sess.mu.Lock()
	beforeMap := len(f.e.sess.cpending[rec.Dur])
	ok := f.e.sess.installOrdinaryProjectionLocked(f.gen, &rec, p)
	afterMap := len(f.e.sess.cpending[rec.Dur])
	f.e.sess.mu.Unlock()
	if ok {
		t.Fatal("a lineage-less record must never satisfy the install gate")
	}
	if p.lin != _EMPTY_ || beforeMap != afterMap {
		t.Fatalf("a refused install changes nothing: lin=%q mappings %d -> %d", p.lin, beforeMap, afterMap)
	}
}

// An UNREADABLE exchange is not an ended one (consult 93's oracle). A resume whose census cannot
// read a delivery's exchange must neither lose the delivery nor keep it past its ending: the
// uncertainty's RESOLUTION drives the projection. Resolved LIVE, the projection is present with
// its source mapping, and the source's redelivery returns the ORIGINAL identifier (a new one would
// admit a second exchange for one source: a duplicate, and an exchange nothing ends). Resolved
// ENDED, the projection is gone and the identifier free. Measured at 34c50ef: the restart form
// restored the delivery and the redelivery returned identifier 1.
func TestMQTTRecoveryUncertainExchangeKeepsItsDelivery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		restart bool
		ended   bool
	}{
		{"restart, resolves live", true, false},
		{"restart, resolves ended", true, true},
		{"same object, resolves live", false, false},
		{"same object, resolves ended", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := testWithdrawAdmitted(t, "uncertain-delivery", 1)
			lostSourceHook(f.e, mqttInflightSourceLive)
			if tc.restart {
				testRestart(f.e)
			}
			xsubj := mqttExchangeSubject(f.e.sess.idHash, f.gen, f.lin)
			var fault atomic.Bool
			fault.Store(true)
			f.e.asm.mu.Lock()
			f.e.asm.testCustodyDecodeFault = func(s string) bool { return fault.Load() && s == xsubj }
			f.e.asm.mu.Unlock()
			if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}
			if tc.ended {
				testEndExchangeRemotely(t, f)
			}
			fault.Store(false)
			testCustodyKick(f.e.sess)
			testCustodyEventually(t, "the uncertainty resolved", func() bool {
				f.e.sess.mu.Lock()
				defer f.e.sess.mu.Unlock()
				_, still := f.e.sess.custody.uncertainSubjects[xsubj]
				return !still
			})
			f.e.sess.mu.Lock()
			present := f.e.sess.pendingPublish[f.pi] != nil && f.e.sess.pendingPublish[f.pi].lin == f.lin
			_, mapped := f.e.sess.cpending[testRealPubRelSrcDur][f.sseq]
			f.e.sess.mu.Unlock()
			if tc.ended {
				if present || mapped {
					t.Fatalf("resolved ended: the projection must be gone: present=%v mapped=%v", present, mapped)
				}
				if got := testCustodyMints(f.e.sess, f.pi); got != f.pi {
					t.Fatalf("resolved ended: the identifier must be free, got %d", got)
				}
				return
			}
			if !present || !mapped {
				t.Fatalf("resolved live: the delivery must be restored with its mapping: present=%v mapped=%v", present, mapped)
			}
			f.e.sess.mu.Lock()
			pi, isNew := f.e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, f.sseq, 2))
			f.e.sess.mu.Unlock()
			if pi != f.pi || isNew {
				t.Fatalf("resolved live: the redelivery must return the original identifier %d, got %d (new=%v)", f.pi, pi, isNew)
			}
		})
	}
}

// testBreakDuty makes the delivery's own duty ordinal undecodable and returns a restorer: the
// DUTY-hole form of an undecided inventory, which names no lineage at all.
func testBreakDuty(t *testing.T, f *testWithdrawFix) (subject string, restore func()) {
	t.Helper()
	ord := testDutyOrdinalOf(t, f.e, f.gen, f.lin)
	subject = mqttDutySubject(f.e.sess.idHash, f.gen, ord)
	original, err := f.e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	if err != nil || original == nil {
		t.Fatalf("loading the duty: %v", err)
	}
	if _, err := f.e.asm.jsa.storeMsg(subject, -1, []byte("not a duty")); err != nil {
		t.Fatalf("break the duty: %v", err)
	}
	return subject, func() {
		if _, err := f.e.asm.jsa.storeMsg(subject, -1, original.Data); err != nil {
			t.Fatalf("restore the duty: %v", err)
		}
	}
}

// testObserveAllTargets runs one synchronous custody pass over every target.
func testObserveAllTargets(f *testWithdrawFix) {
	f.e.sess.mu.Lock()
	targets := f.e.sess.custody.targets(f.gen, 1<<10)
	f.e.sess.mu.Unlock()
	for _, tg := range targets {
		f.e.sess.observeCustodyTarget(f.gen, tg)
	}
}

// The DUTY-hole form of TestMQTTRecoveryUncertainExchangeKeepsItsDelivery (consult 93): a restart
// whose inventory cannot read the delivery's own duty holds the lineage provisionally, and the
// hole's resolution drives the projection. LIVE keeps the delivery and its identifier; ENDED
// removes both once the remaining uncertainty clears.
//
// Mutation M-seed-duty: a duty hole not counted as undecided -> the LIVE arm loses the delivery.
func TestMQTTRecoveryDutyHoleKeepsItsDelivery(t *testing.T) {
	t.Parallel()
	for _, ended := range []bool{false, true} {
		name := "resolves live"
		if ended {
			name = "resolves ended"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := testWithdrawAdmitted(t, "duty-hole-delivery", 1)
			dutySubject, restore := testBreakDuty(t, f)
			testRestart(f.e)
			if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}
			if !testProjectionPresent(f) {
				t.Fatal("an undecided inventory must keep the delivery, held provisionally")
			}
			if ended {
				testEndExchangeRemotely(t, f)
			}
			restore()
			testCustodyKick(f.e.sess)
			testCustodyEventually(t, "the duty hole resolved", func() bool {
				f.e.sess.mu.Lock()
				defer f.e.sess.mu.Unlock()
				return !f.e.sess.custody.subjectUncertain(f.gen, dutySubject)
			})
			testObserveAllTargets(f)
			f.e.sess.mu.Lock()
			_, mapped := f.e.sess.cpending[testRealPubRelSrcDur][f.sseq]
			f.e.sess.mu.Unlock()
			present := testProjectionPresent(f)
			if ended {
				if present || mapped {
					t.Fatalf("resolved ended: the projection must be gone: present=%v mapped=%v", present, mapped)
				}
				if got := testCustodyMints(f.e.sess, f.pi); got != f.pi {
					t.Fatalf("resolved ended: the identifier must be free, got %d", got)
				}
				return
			}
			if !present || !mapped {
				t.Fatalf("resolved live: present=%v mapped=%v", present, mapped)
			}
			f.e.sess.mu.Lock()
			pi, isNew := f.e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, f.sseq, 2))
			f.e.sess.mu.Unlock()
			if pi != f.pi || isNew {
				t.Fatalf("resolved live: the redelivery must return identifier %d, got %d (new=%v)", f.pi, pi, isNew)
			}
		})
	}
}

// An ending ACCEPTED during the resume vetoes the provisional holder (consult 93): the walk
// captured the register while it was live, the exchange then ended and its ending was observed
// (the duty hole resolved to an ended exchange) before apply, and apply must not recreate a
// holder from the stale capture. The release precedes the install.
//
// Mutation M-veto: the ended-during-resume check removed -> apply reseeds, and the ended delivery
// is restored as work.
func TestMQTTRecoveryAcceptedEndingVetoesAProvisionalHold(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "veto-provisional", 1)
	_, restore := testBreakDuty(t, f)
	testRestart(f.e)
	var loads atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testResumeAuthorityLoad = func(id string) mqttAuthorityLoad {
		if loads.Add(1) == 2 {
			// Between the walk and apply: the exchange ends, the duty reads again, and the
			// re-read observes the ending.
			testEndExchangeRemotely(t, f)
			restore()
			testObserveAllTargets(f)
		}
		return f.e.asm.jsa.loadOutboundAuthority(id, f.e.asm.sessStreamEpoch)
	}
	f.e.asm.mu.Unlock()
	t.Cleanup(func() { f.e.asm.mu.Lock(); f.e.asm.testResumeAuthorityLoad = nil; f.e.asm.mu.Unlock() })
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if loads.Load() < 2 {
		t.Fatal("fixture: apply's authority read was never reached")
	}
	if testProjectionPresent(f) {
		t.Fatal("an ending accepted during the resume must veto the provisional holder")
	}
	f.e.sess.mu.Lock()
	held := f.e.sess.custody.holdsPI(f.pi)
	f.e.sess.mu.Unlock()
	if held {
		t.Fatal("the ended exchange's identifier must not be held by a reseeded holder")
	}
}

// T20: a delivery the client answered with PUBREC while the resume was between its walk and its
// apply is never re-offered as a PUBLISH. The walk captured the register at publish; the successful
// PUBREC advance moved the register and the exchange together; apply must not install the captured
// PUBLISH as sendable work.
func TestMQTTRecoveryNeverRetransmitsPastAPubRecLandedDuringTheWalk(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "pubrec-during-walk", 2)
	f.e.transmit(t, f.pi, testRealPubRelSrcDur, f.sseq, true)
	testRestart(f.e)
	var loads atomic.Int32
	var advanced error
	f.e.asm.mu.Lock()
	f.e.asm.testResumeAuthorityLoad = func(id string) mqttAuthorityLoad {
		if loads.Add(1) == 2 {
			// As the PUBREC handler does: the canonical PUBREL is stored before the advance.
			testStoreCanonical(t, f.e, f.pi, f.nonce, f.sseq)
			cur, seq := f.register(t)
			_, advanced = f.e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
				ID: f.e.sess.id, Epoch: f.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-ack",
				Gen: f.gen, Nonce: f.lin, To: mqttExchangePubRecObserved, Acq: cur.Acq,
				Register: cur, RegisterSeq: seq,
			})
		}
		return f.e.asm.jsa.loadOutboundAuthority(id, f.e.asm.sessStreamEpoch)
	}
	f.e.asm.mu.Unlock()
	t.Cleanup(func() { f.e.asm.mu.Lock(); f.e.asm.testResumeAuthorityLoad = nil; f.e.asm.mu.Unlock() })
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
		t.Logf("recoverInflightRegisters: %v", err)
	}
	if loads.Load() < 2 {
		t.Fatal("fixture: apply's authority read was never reached")
	}
	if advanced != nil {
		t.Fatalf("fixture: the PUBREC advance must land during the walk: %v", advanced)
	}
	if cur, _ := f.register(t); cur == nil || !mqttInflightPastPubRec(cur.State) {
		t.Fatalf("fixture: the register must be past PUBREC: %+v", cur)
	}
	// The whole send path, not the local gate alone: a retransmission must also acquire the
	// durable slot, which it can only do against the register it names.
	f.e.sess.mu.Lock()
	ack := f.e.sess.pendingPublish[f.pi]
	var tok mqttSendToken
	var opened bool
	if ack != nil && ack.regNonce == f.nonce {
		tok, opened = f.e.sess.beginSendLocked(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq)
	}
	f.e.sess.mu.Unlock()
	if opened {
		acquired := f.e.sess.acquirePublishAction(f.e.c, f.pi, &tok)
		f.e.sess.endPublishSend(f.e.c, f.pi, tok, mqttIntentNone, false)
		if acquired {
			t.Fatal("a delivery answered with PUBREC must never be re-offered as a PUBLISH: the durable acquisition landed")
		}
	}
	if cur, _ := f.register(t); cur == nil || !mqttInflightPastPubRec(cur.State) || cur.ActionToken != _EMPTY_ {
		t.Fatalf("the past-PUBREC slot must be untouched by the stale PUBLISH: %+v", cur)
	}
	// Liveness: any stale PUBLISH projection continues the exchange's OWN lineage, so the
	// exchange's ending (the PUBREL delivered from the canonical record, then PUBCOMP) removes it
	// through the release path, and it never outlives the obligation.
	f.e.sess.mu.Lock()
	stale := f.e.sess.pendingPublish[f.pi]
	f.e.sess.mu.Unlock()
	if stale != nil && stale.regNonce == f.nonce && stale.lin != f.lin {
		t.Fatalf("a stale PUBLISH projection must carry the exchange's lineage %q, got %q", f.lin, stale.lin)
	}
}

// T15n (M34 behavioural half, consult 90's no-new-population obligation): after a resume over the
// §3.2 populations together — an admitted live delivery, a pubrel_ready slot, an unreached
// register, a pre-admission tombstone, a PUBREL record alone, and a withdrawn exchange — every
// ORDINARY entry the resume leaves actionable carries a lineage the custody ledger holds.
// Non-actionable reservations (placeholder, opaque, foreign) confer nothing and are skipped; the
// lineage-less placeholder promotion is the next checkpoint's.
func TestMQTTRecoveryLeavesNoUnheldActionableEntry(t *testing.T) {
	t.Parallel()
	e, _, relSubject, _, _ := testPubRelReadyWorld(t, "no-new-population")
	_ = relSubject
	live := testWithdrawAdmitMore(t, e, testRealPubRelSrcSeq+1, 1)
	gone := testWithdrawAdmitMore(t, e, testRealPubRelSrcSeq+2, 1)
	testEndExchangeRemotely(t, gone)
	gen := e.sess.gen
	// An unreached register and a PUBREL record alone.
	if _, err := e.asm.jsa.storeInflightCAS(mqttInflightSubject(e.sess.idHash, gen, 40), 0, freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 40, Dur: testRealPubRelSrcDur, Seq: testRealPubRelSrcSeq + 9, QoS: 1, Nonce: mqttNewInflightNonce(),
	})); err != nil {
		t.Fatalf("plant the unreached register: %v", err)
	}
	testStoreCanonical(t, e, 41, mqttNewInflightNonce(), testRealPubRelSrcSeq+10)
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	check := func(kind string, m map[uint16]*mqttPending) {
		for pi, p := range m {
			if p == nil || p.regRecoveryPlaceholder || p.regRecoveryOpaque || p.regForeign || p.markerOpaque ||
				mqttDurIsSlotOnlyClass(p.jsDur) {
				continue
			}
			if p.lin == _EMPTY_ || !e.sess.custody.hasExchange(gen, p.lin) {
				t.Errorf("%s entry %d is actionable ordinary work with an unheld lineage %q: %+v", kind, pi, p.lin, p)
			}
		}
	}
	check("PUBLISH", e.sess.pendingPublish)
	check("PUBREL", e.sess.pendingPubRel)
	if p := e.sess.pendingPublish[live.pi]; p == nil || p.lin != live.lin {
		t.Errorf("control: the live admitted delivery must be restored: %+v", p)
	}
}

// T15 (M20b): a completion on ANOTHER broker after the resume, with no local callback for the old
// delivery, is observed by the gate-driven pass alone: new traffic at Receive Maximum 1 is
// withheld, the withholding asks for a pass, the pass re-reads the HELD target, and capacity and
// the identifier come back.
func TestMQTTRecoveryGateDrivenPassObservesARemoteCompletion(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "lifetime-remote-completion", 1)
	testCustodyRM1(t, f.e.sess)
	if !testRestoredProjection(t, f) {
		t.Fatal("fixture: the resume must install the delivery")
	}
	testEndExchangeRemotely(t, f)
	if got := testCustodyCapacity(f.e.sess); got != 0 {
		t.Fatalf("fixture: nothing local observed the remote ending, capacity must still be 0, got %d", got)
	}
	// New traffic: the allocator withholds and asks for a pass.
	f.e.sess.mu.Lock()
	pi, _ := f.e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, f.sseq+50, 1))
	f.e.sess.mu.Unlock()
	if pi != 0 {
		t.Fatalf("fixture: at Receive Maximum 1 with the old charge held, new traffic must be withheld, got %d", pi)
	}
	testCustodyEventually(t, "the gate-driven pass observed the remote completion", func() bool {
		return testCustodyCapacity(f.e.sess) == 1 && !testProjectionPresent(f)
	})
	if got := testCustodyMints(f.e.sess, f.pi); got != f.pi {
		t.Fatalf("the completed exchange's identifier must be reusable, got %d", got)
	}
}

// T15 (M20c): the release selects projections by LINEAGE alone, never by packet identifier. An
// ended observation of L2 removes L2's projection (the inherited-lineage replacement is the
// positive control) and leaves a fresh-lineage projection L1 on the SAME identifier standing.
func TestMQTTReleaseSelectsByLineageNotIdentifier(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "lifetime-lineage-selection")
	gen := e.sess.gen
	const pi = uint16(7)
	l1, l2 := mqttNewInflightNonce(), mqttNewInflightNonce()
	release := func() {
		t.Helper()
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		x := &mqttOutboundExchange{Nonce: l2, PI: pi, Phase: mqttExchangeWithdrawnUnsent}
		at, ok := e.sess.custody.beginObservation(gen, l2)
		if !ok {
			t.Fatal("fixture: the ledger must hold L2")
		}
		e.sess.acceptExchangeObservationLocked(gen, []*mqttOutboundExchange{x}, func() bool {
			return e.sess.custody.applyExchangeObservation(gen, l2, at, x)
		})
	}

	// The fresh-lineage projection on the identifier; the ledger holds L2 on the same identifier.
	e.sess.mu.Lock()
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: "SRC", sseq: 1, regNonce: l1, lin: l1}
	e.sess.custody.seedProvisionalExchange(gen, l2, pi)
	e.sess.mu.Unlock()
	release()
	e.sess.mu.Lock()
	survivor := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if survivor == nil || survivor.lin != l1 {
		t.Fatalf("a fresh-lineage projection on the same identifier must survive L2's ending: %+v", survivor)
	}

	// Positive control: a projection that continues L2 is removed by the same ending.
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi] = &mqttPending{jsDur: "SRC", sseq: 1, regNonce: mqttNewInflightNonce(), lin: l2}
	e.sess.custody.seedProvisionalExchange(gen, l2, pi)
	e.sess.mu.Unlock()
	release()
	e.sess.mu.Lock()
	gone := e.sess.pendingPublish[pi] == nil
	e.sess.mu.Unlock()
	if !gone {
		t.Fatal("the projection continuing L2 must be removed when L2 ends")
	}
}
