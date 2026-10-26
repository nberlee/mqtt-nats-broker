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

//go:build mqtt_fulltests

package mqtt

import (
	"testing"
)

// F-088 fix I and II on the in-process peer rig: a RECOVERED OUTPUT TOKEN beside a standing unsent
// marker is outstanding output authority, so recovery classifies the register POSSIBLY ATTEMPTED
// and keeps the marker as deletion debt; and a successful acting restore supersedes the
// allocation's void verdict. The token is a real acquisition (acquirePublishAction) by a
// section left open on A's own connection — the holder that may still write — and B is a distinct
// account session manager on the same JetStream (testF088ResumeOnPeer). These are ROUTING and
// BOUNDARY witnesses: the cross-broker wire schedules are in f088_cross_tenure_witness_e2e_test.go.

// testF088AcquireLeaving opens a send section for f on A's own connection and ACQUIRES its output
// token, leaving the section open and the marker standing: outstanding output authority for B's
// recovery to read.
func testF088AcquireLeaving(t *testing.T, f *testWithdrawFix) mqttSendToken {
	t.Helper()
	f.e.bind()
	tok := f.e.openSection(t, f.pi, testRealPubRelSrcDur, f.sseq)
	if !f.e.sess.acquirePublishAction(f.e.c, f.pi, &tok) {
		t.Fatalf("fixture: A's section must acquire the output token for %d", f.pi)
	}
	if cur, _ := f.register(t); cur == nil || cur.ActionToken == _EMPTY_ {
		t.Fatalf("fixture: the register must carry A's output token, got %+v", cur)
	}
	if !f.markerStands() {
		t.Fatal("fixture: the unsent marker must stand beside the token")
	}
	return tok
}

// requireRecoveredPossiblyAttempted asserts B restored f's allocation ACTING and possibly attempted,
// with the marker kept as debt and the register and marker left in place.
func requireRecoveredPossiblyAttempted(t *testing.T, label string, f *testWithdrawFix, p *testF088Peer) {
	t.Helper()
	ent := p.entry(f.pi)
	if ent == nil || ent.regNonce != f.nonce || !ent.acts() || !ent.transmitted || ent.markerSeq == 0 {
		t.Errorf("%s: B must restore the token-bearing register ACTING, possibly attempted, with its marker as debt, got %+v",
			label, ent)
	}
	if cur, _ := f.register(t); cur == nil || cur.Nonce != f.nonce {
		t.Errorf("%s: the register must not be dropped, got %+v", label, cur)
	}
	if !f.markerStands() {
		t.Errorf("%s: the marker must be kept as deletion debt, not deleted by recovery", label)
	}
}

// M-F2a's witness. A register carrying a recovered output token AND a standing marker AND a durable
// void verdict is RESTORED, not dropped as a genuine cancellation: the holder may still
// write, so the verdict cannot have proved non-transmission. The restoration supersedes the
// verdict (fix II, the standing-marker case P8-1 decided).
//
// Input: a real acquisition left open on A; the verdict staged through the production entry and
// made durable by the resume's save (seeded persisted state). Not exercised: the wire.
// Mutation M-F2a (the genuine-cancellation arm reverted to markerSeq != 0): the register is dropped
// by nonce and nothing is restored.
func TestF088RecoveredTokenWithVerdictAndStandingMarkerIsRestored(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f088-mf2a", 1)
	tok := testF088AcquireLeaving(t, f)
	f.e.sess.mu.Lock()
	staged := f.e.sess.stageVoidVerdictLocked(mqttTestLicenceLocked(f.e.sess, nil), f.nonce)
	f.e.sess.mu.Unlock()
	if !staged {
		t.Fatal("fixture: the verdict must be staged under the accepted entitlement")
	}
	p := testF088ResumeOnPeer(t, f, "node2-mf2a")
	met := false
	for _, s := range p.cancellations() {
		met = met || (s.pi == f.pi && s.nonce == f.nonce && s.verdict && s.markerSeq != 0)
	}
	if !met {
		t.Fatalf("witness: recovery's classification must meet the verdict with the marker standing, saw %+v", p.cancellations())
	}
	requireRecoveredPossiblyAttempted(t, "M-F2a", f, p)
	p.sess.mu.Lock()
	_, held := p.sess.voidedRegNonces[f.nonce]
	p.sess.mu.Unlock()
	if held {
		t.Error("M-F2a: the token-derived restoration must retire the superseded verdict")
	}
	f.e.sess.endPublishSend(f.e.c, f.pi, tok, mqttIntentNone, false)
}

// M-F2b's witness. A register carrying a recovered output token and a standing marker over a source
// nothing can re-offer (its consumer deleted: Lost) is RESTORED, not consumed as a proven-unsent
// lost-delete register: the marker licenses nothing while its holder may still write.
//
// Input: a real acquisition left open on A; the source consumer really deleted before the resume.
// Mutation M-F2b (the non-reofferable-source arm reverted to markerSeq != 0): the register is
// drop-consumed and nothing is restored.
func TestF088RecoveredTokenOverLostSourceIsRestored(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f088-mf2b", 1)
	tok := testF088AcquireLeaving(t, f)
	if ci, err := f.e.asm.jsa.lookupConsumer(mqttStreamName, testRealPubRelSrcDur); err != nil || ci == nil {
		t.Fatalf("fixture: the source consumer must exist before it is deleted: %v", err)
	}
	if _, err := f.e.asm.jsa.deleteConsumer(mqttStreamName, testRealPubRelSrcDur, false); err != nil {
		t.Fatalf("fixture: deleting the source consumer: %v", err)
	}
	p := testF088ResumeOnPeer(t, f, "node2-mf2b")
	requireRecoveredPossiblyAttempted(t, "M-F2b", f, p)
	f.e.sess.endPublishSend(f.e.c, f.pi, tok, mqttIntentNone, false)
}

// W-STAND. A GENUINE marker-standing cancellation — no output token — is still proven-unsent: B's
// recovery meets it on its verdict with the marker standing and takes the cancellation's own
// disposition (nothing restored acting), and an in-session verdict still fences a send
// (beginSendLocked's nonce fence, kept unconditional by fix II).
func TestF088GenuineMarkerStandingCancellationStaysFenced(t *testing.T) {
	t.Parallel()
	t.Run("recovery", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "f088-wstand-rec", 1)
		if cur, _ := f.register(t); cur == nil || cur.ActionToken != _EMPTY_ {
			t.Fatalf("fixture: the register must carry no output token, got %+v", cur)
		}
		f.e.sess.mu.Lock()
		staged := f.e.sess.stageVoidVerdictLocked(mqttTestLicenceLocked(f.e.sess, nil), f.nonce)
		f.e.sess.mu.Unlock()
		if !staged {
			t.Fatal("fixture: the verdict must be staged")
		}
		p := testF088ResumeOnPeer(t, f, "node2-wstand")
		met := false
		for _, s := range p.cancellations() {
			met = met || (s.pi == f.pi && s.nonce == f.nonce && s.verdict && s.markerSeq != 0)
		}
		if !met {
			t.Fatalf("witness: recovery must meet the genuine cancellation with its marker standing, saw %+v", p.cancellations())
		}
		if ent := p.entry(f.pi); ent != nil && ent.regNonce == f.nonce && ent.acts() {
			t.Fatalf("a genuine cancellation must not be restored acting, got %+v", ent)
		}
	})
	t.Run("fence", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "f088-wstand-fence", 1)
		f.e.bind()
		f.e.sess.mu.Lock()
		ent := f.e.sess.pendingPublish[f.pi]
		sendable := ent != nil && ent.acts() && ent.markerSeq != 0 && ent.sendNonce == _EMPTY_
		staged := f.e.sess.stageVoidVerdictLocked(mqttTestLicenceLocked(f.e.sess, nil), f.nonce)
		f.e.sess.c = f.e.c
		_, opened := f.e.sess.beginSendLocked(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq)
		f.e.sess.mu.Unlock()
		if !sendable || !staged {
			t.Fatalf("fixture: an acting, marker-standing entry and a staged verdict are required (sendable %v staged %v)",
				sendable, staged)
		}
		if opened {
			t.Fatal("the nonce verdict must fence the send of a genuinely cancelled allocation")
		}
	})
}

// testF088RecoveredUntracked builds the W-ACKRT/W-VETO state: a register B recovered POSSIBLY
// ATTEMPTED over A's outstanding token, A's section then ended without intent (its token drained),
// and B's entry removed UNANSWERED. It returns the peer and a copy of the entry as it stood before
// the removal.
//
// A BOUNDARY TEST, NOT PRODUCTION REACH: the removal is a direct untrackPublishEntry(...,
// mqttUntrackUnanswered). The non-drain routes do not remove this entry — the consumer-teardown
// purge RETAINS a possibly-attempted entry (purgeConsumerPendingModeLocked's `continue`) and
// Message Expiry drops only a proven-unsent one — and the drain-owned ACL deny
// (releaseTransmittedIdentifier) is keyed on the source mapping, which this rig's recovered entry
// does not carry (its source classifies complete), so it removes nothing here.
//
// drainHolder decides whether A's section ends (draining its token without intent) BEFORE the
// removal; when false the token still stands on return, and endHolder ends it.
func testF088RecoveredUntracked(t *testing.T, clientID string, drainHolder bool) (*testWithdrawFix, *testF088Peer, mqttPending, func()) {
	t.Helper()
	f := testWithdrawAdmitted(t, clientID, 1)
	tok := testF088AcquireLeaving(t, f)
	p := testF088ResumeOnPeer(t, f, "node2-"+clientID)
	requireRecoveredPossiblyAttempted(t, "fixture", f, p)
	ended := false
	endHolder := func() {
		if ended {
			return
		}
		ended = true
		f.e.sess.endPublishSend(f.e.c, f.pi, tok, mqttIntentNone, false)
		if cur, _ := f.register(t); cur == nil || cur.ActionToken != _EMPTY_ {
			t.Fatalf("fixture: A's section must drain its token, got %+v", cur)
		}
	}
	if drainHolder {
		endHolder()
	}
	ent := p.entry(f.pi)
	if ent == nil || mqttEntrySendEvidence(ent) != mqttSendEvidencePossiblyAttempted {
		t.Fatalf("fixture: B's entry must carry possibly-attempted evidence, got %+v", ent)
	}
	before := *ent
	p.sess.mu.Lock()
	removed := p.sess.untrackPublishEntry(f.pi, mqttUntrackUnanswered)
	p.sess.mu.Unlock()
	if removed == nil || removed.regNonce != f.nonce {
		t.Fatalf("fixture: the unanswered removal must take B's entry, got %+v", removed)
	}
	return f, p, before, endHolder
}

func testF088PeerBinding(p *testF088Peer, gen string, pi uint16) *mqttAllocationAckBinding {
	p.sess.mu.Lock()
	defer p.sess.mu.Unlock()
	return p.sess.abandonedAcks[mqttAbandonedAckIndex{gen: gen, pi: pi}]
}

// W-FORK. A fork repair's UNION re-asserts a verdict our erased record carried, and it runs before
// recovery (the accept block's reassertForkedGrantInto precedes recoverInflightRegisters); recovery
// must retire it again when it restores the allocation possibly attempted, and keep doing so when
// the verdict comes back once more.
//
// Input: an ordinary allocation TRANSMITTED on the session (its marker deleted by intent), a verdict
// for it staged on OUR record, which a sibling then erases (a real FORKED rejection, the capsule
// minted); the reconnect restores the survivor and applies the capsule behind a record transaction,
// exactly as the accept block does (testForkRepairEnv.reconnect). Recovery then runs on that object.
// The repeat re-applies the verdict through voidRegNonceLocked — the one helper the union itself
// uses — standing in for a second resurrection, and recovers again. Each retirement is asserted with
// 12.3's revision and persistence checks.
func TestF088ForkUnionResurrectedVerdictIsRetiredAgain(t *testing.T) {
	t.Parallel()
	const cid = "f088-wfork"
	e := testNewRejectionEnv(t, cid)
	e.dropPersistObservations(t, e.idHash)
	testReadyForRealPubRelDelivery(t, e.testInflightEnv)
	f := testWithdrawAdmitMore(t, e.testInflightEnv, testRealPubRelSrcSeq, 1)
	e.testInflightEnv.bind()
	e.transmit(t, f.pi, testRealPubRelSrcDur, f.sseq, true)
	if f.markerStands() {
		t.Fatal("fixture: the transmission must have consumed the unsent marker")
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("fixture: persist the PARENT record: %v", err)
	}
	e.sess.mu.Lock()
	parentRec := e.sess.buildPersistedSessionLocked()
	staged := e.sess.stageVoidVerdictLocked(mqttTestLicenceLocked(e.sess, nil), f.nonce)
	e.sess.mu.Unlock()
	if !staged {
		t.Fatal("fixture: the verdict must be staged on our branch")
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("fixture: persist OUR record (the one the fork erases): %v", err)
	}
	testLandSibling(t, e.testInflightEnv, func(ps *mqttPersistedSession) { testCopyGrantInto(ps, &parentRec) })
	e.rejectSave(t, e.sess)
	if got := e.sess.settleRecordRejection(e.asm); got != mqttRejectionForked {
		t.Fatalf("fixture: the rejection must classify FORKED, got %v", got)
	}
	fe := &testForkRepairEnv{testRejectionEnv: e, parentRec: parentRec}
	carried := false
	for _, n := range fe.capsule(t).regNonces {
		carried = carried || n == f.nonce
	}
	if !carried {
		t.Fatal("fixture: the fork capsule must carry our verdict")
	}

	sess := fe.reconnect(t)
	c := e.s.createInternalAccountClient()
	testAcceptRigConnection(t, e.asm, sess, c)
	recoverOnce := func(round string) {
		t.Helper()
		sess.mu.Lock()
		_, held := sess.voidedRegNonces[f.nonce]
		rev, saved := sess.voidedRegsRev, sess.voidedRegsSavedRev
		resident := sess.pendingPublish[f.pi]
		sess.mu.Unlock()
		// Which arm the retirement takes is recorded: a RESIDENT already-transmitted entry is Site 4
		// (iii), none is the fresh install.
		t.Logf("W-FORK %s: same object as before the fork %v; resident entry before recovery %v (transmitted %v)",
			round, sess == e.sess, resident != nil, resident != nil && resident.transmitted)
		if !held {
			t.Fatalf("fixture (%s): the verdict must stand (resurrected) before recovery", round)
		}
		if rev != saved {
			if !sess.persistVoidedRegs(nil) {
				t.Fatalf("fixture (%s): the resurrected verdict must be credited before recovery (rev %d saved %d)", round, rev, saved)
			}
			sess.mu.Lock()
			rev, saved = sess.voidedRegsRev, sess.voidedRegsSavedRev
			sess.mu.Unlock()
		}
		v := &testF088VerdictRevisions{seen: true, rev: rev, savedRev: saved}
		if _, err := sess.recoverInflightRegisters(c); err != nil {
			t.Fatalf("recovery (%s): %v", round, err)
		}
		sess.mu.Lock()
		ent := sess.pendingPublish[f.pi]
		ok := ent != nil && ent.regNonce == f.nonce && ent.acts() && ent.transmitted
		sess.c = c
		sess.mu.Unlock()
		if !ok {
			t.Fatalf("CONTRACT (%s): recovery must restore the allocation acting and possibly attempted, got %+v", round, ent)
		}
		requireVerdictRetired(t, "W-FORK "+round, v, sess, e.asm, cid, f.nonce)
	}
	recoverOnce("after the union")
	sess.mu.Lock()
	sess.voidRegNonceLocked(f.nonce)
	sess.mu.Unlock()
	recoverOnce("after a repeat resurrection")
}

// W-ACKRT. A RECOVERED possible attempt removed UNANSWERED installs the allocation-bound AckRoute
// (its evidence is PossiblyAttempted, fix I), and the client's late PUBACK completes the exchange
// through the resolver.
//
// Mutation M-F5 (installAckRouteForRemovalLocked returns false): no binding, the PUBACK completes
// nothing and the exchange stays at Publish.
func TestF088RecoveredPossibleAttemptUnansweredRemovalRoutesLateAck(t *testing.T) {
	t.Parallel()
	f, p, _, _ := testF088RecoveredUntracked(t, "f088-ackrt", true)
	if b := testF088PeerBinding(p, f.gen, f.pi); b == nil || b.role != mqttAckRoleAckRoute || b.ev.lin != f.lin {
		t.Errorf("the unanswered removal of a recovered possible attempt must install its AckRoute, got %+v", b)
	}
	if err := p.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK on B: %v", err)
	}
	testCustodyEventually(t, "the late PUBACK ends the exchange through the AckRoute", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
}

// W-VETO. The AckRoute of a recovered possible attempt keeps its identity rules: a restoration of the
// SAME allocation hands it off (removed, the acting entry owns the acknowledgement); a binding for a
// DIFFERENT allocation on the identifier is refused while it stands; and once the lineage has ENDED
// no AckRoute is installed for it again.
func TestF088RecoveredPossibleAttemptAckRouteVetoes(t *testing.T) {
	t.Parallel()
	// A's token stands through the second recovery, so that recovery again reads the register
	// possibly attempted (once A drains without intent and the marker still stands, a fresh
	// classification correctly reads it proven-unsent).
	f, p, before, endHolder := testF088RecoveredUntracked(t, "f088-veto", false)
	b := testF088PeerBinding(p, f.gen, f.pi)
	if b == nil || b.role != mqttAckRoleAckRoute {
		t.Fatalf("fixture: the AckRoute must stand, got %+v", b)
	}
	// A DIFFERENT allocation on the same identifier is refused while the AckRoute stands.
	other := before
	other.regNonce = mqttNewInflightNonce()
	other.slot.nonce = other.regNonce
	other.ev.lin, other.ev.nonce = mqttNewInflightNonce(), other.regNonce
	p.sess.mu.Lock()
	installed := p.sess.installAckRouteForRemovalLocked(&other, mqttUntrackUnanswered)
	p.sess.mu.Unlock()
	if installed {
		t.Error("an AckRoute for a different allocation must be refused while this allocation's stands")
	}
	if cur := testF088PeerBinding(p, f.gen, f.pi); cur == nil || cur.ev.lin != f.lin {
		t.Errorf("the standing AckRoute must be untouched, got %+v", cur)
	}
	// The SAME allocation restored acting hands the AckRoute off.
	if _, err := p.sess.recoverInflightRegisters(p.c); err != nil {
		t.Fatalf("B's second recovery: %v", err)
	}
	if ent := p.entry(f.pi); ent == nil || ent.regNonce != f.nonce || !ent.acts() || !ent.transmitted {
		t.Fatalf("the second recovery must restore the allocation acting and possibly attempted, got %+v", ent)
	}
	if cur := testF088PeerBinding(p, f.gen, f.pi); cur != nil && cur.role == mqttAckRoleAckRoute {
		t.Errorf("the restoration must hand the AckRoute off, got %+v", cur)
	}
	// The lineage ENDS, and no AckRoute is installed for it again.
	endHolder()
	if err := p.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK on B: %v", err)
	}
	testCustodyEventually(t, "the PUBACK ends the lineage", func() bool {
		p.sess.mu.Lock()
		defer p.sess.mu.Unlock()
		return p.sess.custody.lineageEnded(f.gen, f.lin)
	})
	p.sess.mu.Lock()
	reinstalled := p.sess.installAckRouteForRemovalLocked(&before, mqttUntrackUnanswered)
	p.sess.mu.Unlock()
	if reinstalled {
		t.Error("an ended lineage's AckRoute must be vetoed")
	}
}
