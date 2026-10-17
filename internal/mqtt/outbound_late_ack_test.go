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

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// F-088 / F-097 WP2 (plan rev 20): a late acknowledgement is BOUND before it arrives. These tests
// witness the AckRoute binding — installed only by an UNANSWERED removal of a transmitted entry —
// routing the client's answer to the existing completion owners.

// testLateAckFix is an ordinary admitted obligation that was TRANSMITTED (a real send section on
// the session's own connection), with a drain claiming its delivery so an ACL deny takes the
// drain-owned untrack rather than an abandonment.
type testLateAckFix struct {
	*testWithdrawFix
	reply string
}

func testLateAckTransmitted(t *testing.T, clientID string, qos byte) *testLateAckFix {
	t.Helper()
	f := testWithdrawAdmitted(t, clientID, qos)
	f.e.bind()
	f.e.transmit(t, f.pi, testRealPubRelSrcDur, f.sseq, true)
	f.e.sess.mu.Lock()
	ent := f.e.sess.pendingPublish[f.pi]
	f.e.sess.mu.Unlock()
	if mqttEntrySendEvidence(ent) != mqttSendEvidencePossiblyAttempted {
		t.Fatalf("fixture: the entry must record its transmission: %+v", ent)
	}
	testCustodyRM1(t, f.e.sess)
	return &testLateAckFix{testWithdrawFix: f, reply: testInflightAckSubject(testRealPubRelSrcDur, f.sseq, 2)}
}

// claimByDrain restores a drain that claims the delivery, as UNSUBSCRIBE's drain does.
func (f *testLateAckFix) claimByDrain(t *testing.T) {
	t.Helper()
	sess := f.e.sess
	sess.mu.Lock()
	sess.restoreDrainLocked("sid-late-ack", mqttDrainFromPersisted(&mqttPersistedDrain{
		Cons: &ConsumerConfig{Durable: testRealPubRelSrcDur}, QoS: 1,
		Started: []mqttPersistedDrainSeq{{Seq: f.sseq, PI: f.pi}},
	}))
	st := sess.drainSeqLocked(testRealPubRelSrcDur, f.sseq)
	sess.mu.Unlock()
	if st == nil || !st.claimed {
		t.Fatal("fixture: the drain must claim the delivery")
	}
}

// untrackByDeny is the drain-owned ACL deny: the transmitted entry is untracked UNANSWERED.
func (f *testLateAckFix) untrackByDeny(t *testing.T) {
	t.Helper()
	f.claimByDrain(t)
	if r := f.e.sess.releaseTransmittedIdentifier(f.e.c, testRealPubRelSrcDur, f.reply); r != mqttAbandonNone {
		t.Fatalf("a drain-claimed deny must take the untrack, got %v", r)
	}
}

func (f *testLateAckFix) binding() *mqttAllocationAckBinding {
	f.e.sess.mu.Lock()
	defer f.e.sess.mu.Unlock()
	return f.e.sess.abandonedAcks[mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}]
}

func (f *testLateAckFix) entry() *mqttPending {
	f.e.sess.mu.Lock()
	defer f.e.sess.mu.Unlock()
	return f.e.sess.pendingPublish[f.pi]
}

// T4. The drain-owned deny untracks a TRANSMITTED entry: its AckRoute is installed in the same
// section, its register is preserved at publish, and the client's PUBACK on the same connection
// ends the exchange (Terminal), cleans the register and releases the charge.
//
// Mutation M5 (Unanswered skips install): no binding -> the PUBACK is dropped, the exchange stays
// Publish.
func TestF088UntrackedTransmittedDeliveryLatePubAckEndsTheExchange(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f088-t4", 1)
	f.untrackByDeny(t)
	if f.entry() != nil {
		t.Fatal("fixture: the deny must untrack the entry")
	}
	b := f.binding()
	// Non-fatal: the install is the mechanism; the target (M5) is the late PUBACK ending the
	// exchange, evaluated below either way.
	if b == nil || b.role != mqttAckRoleAckRoute {
		t.Errorf("an UNANSWERED removal of a transmitted entry must install its AckRoute, got %+v", b)
	}
	if cur, _ := f.register(t); cur == nil || cur.State != mqttInflightPublish {
		t.Fatalf("the transmitted exchange's register must be preserved at publish, got %+v", cur)
	}
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("fixture: the exchange must still wait for its answer, phase %d", x.Phase)
	}
	if testCustodyCapacity(f.e.sess) != 0 {
		t.Fatal("the unanswered exchange must keep its charge")
	}
	// The ACK reaches no entry: only the binding can route it (the resolver hit).
	if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	testCustodyEventually(t, "the late PUBACK ends the exchange", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
	testCustodyEventually(t, "the terminal register is cleaned", func() bool {
		cur, _ := f.register(t)
		return cur == nil
	})
	testCustodyEventually(t, "the exchange's charge is released", func() bool {
		return testCustodyCapacity(f.e.sess) == 1
	})
	testCustodyEventually(t, "the binding retires on the ending", func() bool { return f.binding() == nil })
}

// T5. T4 to the untrack; broker A stops; a cold COMPLETE resume on peer B: recovery restores an
// ACTING entry for p (its marker is gone: possibly attempted), and the client's PUBACK on B ends the
// exchange through the ordinary path.
func TestF088PreservedRegisterLatePubAckAfterColdPeerResume(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f088-t5", 1)
	f.untrackByDeny(t)
	if b := f.binding(); b == nil || b.role != mqttAckRoleAckRoute {
		t.Fatalf("fixture: the AckRoute must stand on A, got %+v", b)
	}
	if err := testSessSave(t, f.e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	_, peer, pc := f.e.peer(t, "node2", f.e.sess.id)
	if _, err := peer.recoverInflightRegisters(pc); err != nil {
		t.Fatalf("B's recovery: %v", err)
	}
	peer.mu.Lock()
	ent := peer.pendingPublish[f.pi]
	incomplete := peer.piSpaceIncomplete
	peer.c = pc
	peer.mu.Unlock()
	if incomplete {
		t.Fatal("fixture: B's resume must be COMPLETE")
	}
	if ent == nil || !ent.acts() {
		t.Fatalf("B's recovery must restore an ACTING entry for the preserved register, got %+v", ent)
	}
	if err := pc.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK on B: %v", err)
	}
	testCustodyEventually(t, "the PUBACK on B ends the exchange", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
}

// T9 (R1-F1). L1 on p ENDS (acknowledged): its removal is Ended and installs nothing. p is then
// re-allocated to L2, which is transmitted and untracked UNANSWERED: its AckRoute names L2 (the
// stale L1 is vetoed as an ended lineage), and the PUBACK ends L2.
func TestF088EndedLineageIsNotReboundOnIdentifierReuse(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f088-t9", 1)
	l1 := f.lin
	if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("L1's PUBACK: %v", err)
	}
	testCustodyEventually(t, "L1 ends and its charge comes back", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal && testCustodyCapacity(f.e.sess) == 1
	})
	testCustodyEventually(t, "L1's register is cleaned", func() bool { cur, _ := f.register(t); return cur == nil })
	if f.binding() != nil {
		t.Fatal("an answered and ended exchange installs no binding")
	}
	f.e.sess.mu.Lock()
	ended := f.e.sess.custody.lineageEnded(f.gen, l1)
	f.e.sess.last_pi = f.pi - 1
	f.e.sess.mu.Unlock()
	if !ended {
		t.Fatal("fixture: the ledger must know L1 ended (the veto's input)")
	}
	f2 := testWithdrawAdmitMore(t, f.e, f.sseq+1, 1)
	if f2.pi != f.pi || f2.lin == l1 {
		t.Fatalf("fixture: L2 must reuse identifier %d under a new lineage, got %d / %q", f.pi, f2.pi, f2.lin)
	}
	f.e.transmit(t, f2.pi, testRealPubRelSrcDur, f2.sseq, true)
	l2 := &testLateAckFix{testWithdrawFix: f2, reply: testInflightAckSubject(testRealPubRelSrcDur, f2.sseq, 2)}
	l2.untrackByDeny(t)
	b := l2.binding()
	if b == nil || b.role != mqttAckRoleAckRoute || b.ev.lin != f2.lin {
		t.Fatalf("L2's AckRoute must name L2, got %+v", b)
	}
	if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("L2's PUBACK: %v", err)
	}
	testCustodyEventually(t, "L2 ends", func() bool {
		x := l2.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
}

// T9, the LEDGER-ENDED schedule (M8's target). In T9 above L1's PUBACK is taken by A's handler,
// whose ANSWERED untrack removes L1's entry before any ending is observed, and a completion run on
// A itself retires any binding of the allocation it completed - so no stale binding can form
// there. Here L1 is completed by ANOTHER session object of the same generation (peer B, which
// restored L1 acting and took the client's PUBACK) while A's entry for L1 STILL STANDS; A's
// ledger then observes the ending and removes the entry (removeLineageProjectionsLocked, ENDED).
// That removal must install nothing (Ended, and the ended-lineage veto); then p is re-allocated on
// A to L2, which is transmitted and untracked UNANSWERED: its AckRoute names L2 and the PUBACK
// ends L2.
func TestF088EndedLineageObservedWhileTrackedIsNotRebound(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f088-t9-ledger", 1)
	// THE FLOOR UNIT IS HELD (F-112): once L1's projection and register are both gone its
	// ended-lineage entry is legitimately pruned, and this test reads that entry as its proof that
	// A observed the ending. The prune's own conditions are TestF112TheEndedLineageVetoExits...'s.
	f.e.sess.mu.Lock()
	if f.e.sess.testCustodyDriver == nil {
		f.e.sess.testCustodyDriver = &mqttCustodyDriverSeams{}
	}
	f.e.sess.testCustodyDriver.floorHeld = func(string) bool { return true }
	f.e.sess.mu.Unlock()
	l1 := f.lin
	ack := f.entry()
	if mqttEntrySendEvidence(ack) != mqttSendEvidencePossiblyAttempted {
		t.Fatalf("fixture: L1's entry must be transmitted and acting, got %+v", ack)
	}
	if err := testSessSave(t, f.e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	_, peer, pc := f.e.peer(t, "node2", f.e.sess.id)
	if _, err := peer.recoverInflightRegisters(pc); err != nil {
		t.Fatalf("B's recovery: %v", err)
	}
	peer.mu.Lock()
	pent := peer.pendingPublish[f.pi]
	peer.c = pc
	peer.mu.Unlock()
	if pent == nil || !pent.acts() {
		t.Fatalf("fixture: B must restore L1 acting, got %+v", pent)
	}
	if err := pc.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("L1's PUBACK on B: %v", err)
	}
	testCustodyEventually(t, "B ends L1 and cleans its register", func() bool {
		x := f.exchange(t)
		cur, _ := f.register(t)
		return x != nil && x.Phase == mqttExchangeTerminal && cur == nil
	})
	if f.entry() != ack {
		t.Fatalf("fixture: A's entry for L1 must still stand after B's completion, got %+v", f.entry())
	}
	f.e.sess.hintCustody(f.gen, l1)
	// REACH: A's ledger observed L1's ending and removed A's still-standing entry.
	testCustodyEventually(t, "the ledger's observation of L1's ending removes L1's entry", func() bool {
		f.e.sess.mu.Lock()
		defer f.e.sess.mu.Unlock()
		return f.e.sess.pendingPublish[f.pi] == nil && f.e.sess.custody.lineageEnded(f.gen, l1)
	})
	// Non-fatal: the direct observable of the removal's reason; the target is L2's ending below.
	if b := f.binding(); b != nil {
		t.Errorf("an ended lineage's removal must install no binding, got %+v (lineage %q)", b, b.ev.lin)
	}
	f.e.sess.mu.Lock()
	f.e.sess.last_pi = f.pi - 1
	f.e.sess.mu.Unlock()
	f2 := testWithdrawAdmitMore(t, f.e, f.sseq+1, 1)
	if f2.pi != f.pi || f2.lin == l1 {
		t.Fatalf("fixture: L2 must reuse identifier %d under a new lineage, got %d / %q", f.pi, f2.pi, f2.lin)
	}
	f.e.transmit(t, f2.pi, testRealPubRelSrcDur, f2.sseq, true)
	l2 := &testLateAckFix{testWithdrawFix: f2, reply: testInflightAckSubject(testRealPubRelSrcDur, f2.sseq, 2)}
	if b := l2.binding(); b != nil {
		t.Logf("binding standing at L2's install: lineage %q (L1 %q)", b.ev.lin, l1)
	}
	l2.untrackByDeny(t)
	if b := l2.binding(); b == nil || b.role != mqttAckRoleAckRoute || b.ev.lin != f2.lin {
		t.Errorf("L2's AckRoute must name L2, got %+v", b)
	}
	if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("L2's PUBACK: %v", err)
	}
	// TARGET (M8).
	testCustodyEventually(t, "L2 ends (Terminal)", func() bool {
		x := l2.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
}

// T9b. A NEVER-SENT acting entry removed by the non-drain UNSUBSCRIBE teardown's proven-unsent arm
// installs no binding (its marker stands: nothing to answer), and the teardown's ladder then
// withdraws it (WithdrawnUnsent).
func TestF088MarkerStandingRemovalInstallsNoBinding(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f088-t9b", 1)
	f.e.sess.mu.Lock()
	ent := f.e.sess.pendingPublish[f.pi]
	f.e.sess.mu.Unlock()
	if mqttEntrySendEvidence(ent) != mqttSendEvidenceMarkerStanding {
		t.Fatalf("fixture: a never-sent entry holding its marker is required, got %+v", ent)
	}
	f.e.sess.mu.Lock()
	regs, _ := f.e.sess.purgeConsumerPendingLocked(testRealPubRelSrcDur, mqttTestLicenceLocked(f.e.sess, nil))
	_, kept := f.e.sess.pendingPublish[f.pi]
	bound := f.e.sess.abandonedAcks[mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}]
	f.e.sess.mu.Unlock()
	if kept {
		t.Fatal("fixture: the proven-unsent arm must remove the entry")
	}
	if bound != nil {
		t.Fatalf("a marker-standing removal must install no binding for %d, got %+v", f.pi, bound)
	}
	f.e.sess.voidRegistersOnStream(f.e.c, regs, mqttTestLicence(f.e.sess, nil))
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("the teardown's ladder must withdraw the never-sent obligation, got %+v", x)
	}
}

// T9b, the DRAIN route (M7's target). The removal above is purgeConsumerPendingLocked's direct
// delete (T13's census class "proven-unsent purge"), which never reaches the binding install. The
// removal that DOES carry a marker-standing entry into the install is the UNSUBSCRIBE drain's
// withdraw arm (stageDrain, drainClaimWithdraw): it untracks the never-sent entry UNANSWERED, and
// the evidence enum must keep MarkerStanding from installing an AckRoute.
func TestF088MarkerStandingDrainWithdrawInstallsNoBinding(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f088-t9b-drain", 1)
	sess := f.e.sess
	const sid = "sid-t9b-drain"
	cc := &ConsumerConfig{Durable: testRealPubRelSrcDur}
	sess.mu.Lock()
	ent := sess.pendingPublish[f.pi]
	mapped := sess.cpending[testRealPubRelSrcDur][f.sseq] == f.pi
	sess.mu.Unlock()
	if mqttEntrySendEvidence(ent) != mqttSendEvidenceMarkerStanding || !mapped {
		t.Fatalf("fixture: a never-sent entry holding its marker, mapped for its source, is required (mapped %v): %+v",
			mapped, ent)
	}
	sess.mu.Lock()
	sess.cons = map[string]*ConsumerConfig{sid: cc}
	disposition := sess.drainClaimDispositionLocked(ent)
	sess.stageDrain(sid, cc, nil, mqttTestLicenceLocked(sess, nil))
	_, kept := sess.pendingPublish[f.pi]
	bound := sess.abandonedAcks[mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}]
	sess.mu.Unlock()
	// REACH: the drain classified the entry for withdrawal and untracked it (the Unanswered
	// removal of a marker-standing entry).
	if disposition != drainClaimWithdraw || kept {
		t.Fatalf("witness: the drain's withdraw arm must untrack the never-sent entry (disposition %v, kept %v)",
			disposition, kept)
	}
	// TARGET (M7).
	if bound != nil {
		t.Fatalf("a marker-standing removal must install no binding for %d, got %+v", f.pi, bound)
	}
}

// T13b. The door REFUSES a vanished marker: the marker is deleted after the door's evidence was
// minted and before the withdrawal op's marker read; the op reports no marker, and the register
// survives (no destruction on a cancellation's or a tombstone's say-so).
func TestF088DoorRefusesAVanishedMarker(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f088-t13b", 1)
	rec, seq := f.register(t)
	var vanished bool
	var opErr error
	f.e.asm.mu.Lock()
	f.e.asm.testWithdrawUnsent = func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if !vanished {
			// After the door minted its evidence on the standing marker, BEFORE the op's own read.
			vanished = true
			ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
			if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
				t.Errorf("deleting the marker mid-door: %v", err)
			}
		}
		out, err := write()
		opErr = err
		return out, err
	}
	f.e.asm.mu.Unlock()
	if out := f.e.sess.reapLoadedRegister(f.subject, seq, rec, mqttSlotDeleteNeverTransmitted,
		mqttLicenceOf(f.e.c), nil); out != mqttReapRefused {
		t.Fatalf("the door must refuse once the marker vanished, got %v", out)
	}
	if !vanished {
		t.Fatal("fixture: the marker was never removed inside the door")
	}
	if opErr == nil {
		t.Fatal("the withdrawal op must report the missing marker, not commit")
	}
	t.Logf("witness: the op refused with %v", opErr)
	if cur, _ := f.register(t); cur == nil || cur.Nonce != f.nonce {
		t.Fatalf("the register must survive, got %+v", cur)
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("no withdrawal may be manufactured, got %+v", x)
	}
}

// testLateAckUnitSess is a session wired far enough for the binding index alone.
func testLateAckUnitSess() *mqttSession {
	return &mqttSession{
		asm: &mqttAccountSessionManager{sessStreamEpoch: "epoch-1"}, id: "late-ack-unit", idHash: "hhhhhhhh",
		gen: "gen-1", custody: newMQTTCustodyLedger("gen-1"),
	}
}

func testLateAckUnitBinding(sess *mqttSession, role mqttAllocationAckBindingRole, pi uint16, nonce, lin, dur string) *mqttAllocationAckBinding {
	return &mqttAllocationAckBinding{
		role: role, sess: sess, epoch: sess.asm.sessStreamEpoch,
		name:  mqttInflightName{idHash: sess.idHash, gen: sess.gen, pi: pi, subject: mqttInflightSubject(sess.idHash, sess.gen, pi)},
		nonce: nonce, ev: mqttPendingEvidence{gen: sess.gen, lin: lin, nonce: nonce}, dur: dur, seq: 7, qos: 1, acq: 1,
	}
}

// T7. A QoS 1 AckRoute is answered by the WRONG packet (PUBREC): mqttPhaseForAck refuses it and
// the exchange stays Publish; the right packet (PUBACK) then ends it (positive control).
func TestF088BindingRefusesTheWrongAcknowledgement(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f088-t7", 1)
	f.untrackByDeny(t)
	if b := f.binding(); b == nil || b.role != mqttAckRoleAckRoute {
		t.Fatalf("fixture: the AckRoute must stand, got %+v", b)
	}
	_ = f.e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess)
	testCustodyStaysFor(t, "a PUBREC never advances a QoS 1 exchange", 200*time.Millisecond, func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangePublish
	})
	if f.binding() == nil {
		t.Fatal("a refused acknowledgement must not consume the binding")
	}
	if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	testCustodyEventually(t, "the right acknowledgement ends it", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
}

// T8a/T8b. The install boundary refuses a binding of another generation, or of another stream
// epoch; the same binding with this session's generation and epoch installs.
func TestF088BindingRefusesAnotherGeneration(t *testing.T) {
	t.Parallel()
	sess := testLateAckUnitSess()
	b := testLateAckUnitBinding(sess, mqttAckRoleAckRoute, 5, "n5", "n5", "dur")
	other := *b
	other.name.gen = "gen-0"
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.installAbandonedAckLocked(&other) {
		t.Fatal("a binding of another generation must be refused")
	}
	if !sess.installAbandonedAckLocked(b) {
		t.Fatal("positive control: this generation's binding installs")
	}
}

func TestF088BindingRefusesAnotherEpoch(t *testing.T) {
	t.Parallel()
	sess := testLateAckUnitSess()
	b := testLateAckUnitBinding(sess, mqttAckRoleAckRoute, 5, "n5", "n5", "dur")
	other := *b
	other.epoch = "epoch-0"
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.installAbandonedAckLocked(&other) {
		t.Fatal("a binding of another stream epoch must be refused")
	}
	if !sess.installAbandonedAckLocked(b) {
		t.Fatal("positive control: this epoch's binding installs")
	}
}

// T18. A HandBack installs DESPITE the ended-lineage veto (it reconstructs a terminal abandonment
// whose exchange already ended), and a shared one keeps the source gate closed until its register
// is proven deleted; an AckRoute of an ended lineage is refused.
func TestF088TerminalHandBackStillReconstructs(t *testing.T) {
	t.Parallel()
	sess := testLateAckUnitSess()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.custody.noteEnded("lin-ended", 1)
	route := testLateAckUnitBinding(sess, mqttAckRoleAckRoute, 6, "n6", "lin-ended", "dur")
	if sess.installAbandonedAckLocked(route) {
		t.Fatal("an AckRoute for an ended lineage must be vetoed")
	}
	shared := mqttSharedConsumerDurablePrefix + "group"
	hb := testLateAckUnitBinding(sess, mqttAckRoleHandBack, 6, "n6", "lin-ended", shared)
	if !sess.installAbandonedAckLocked(hb) {
		t.Fatal("a HandBack is exempt from the ended-lineage veto")
	}
	if !sess.sourceHandBackHeldLocked(shared, 7) {
		t.Fatal("the shared HandBack must keep the source gate closed")
	}
	if sess.removeAbandonedAckBindingLocked(hb, mqttBindingReleaseEnded) {
		t.Fatal("an ending alone must not remove a shared HandBack")
	}
	if !sess.sourceHandBackHeldLocked(shared, 7) {
		t.Fatal("the gate must stay closed until the register is proven deleted")
	}
	if !sess.removeAbandonedAckBindingLocked(hb, mqttBindingReleaseEndedAndDeleted) {
		t.Fatal("ended AND deleted removes the shared HandBack")
	}
	if sess.sourceHandBackHeldLocked(shared, 7) {
		t.Fatal("the gate must open once the HandBack is removed")
	}
}

// T19. A SHARED AckRoute holds no source gate: only a HandBack does.
func TestF088SharedAckRouteDoesNotGateTheSource(t *testing.T) {
	t.Parallel()
	sess := testLateAckUnitSess()
	shared := mqttSharedConsumerDurablePrefix + "group"
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !sess.installAbandonedAckLocked(testLateAckUnitBinding(sess, mqttAckRoleAckRoute, 8, "n8", "n8", shared)) {
		t.Fatal("fixture: the shared AckRoute must install")
	}
	if sess.sourceHandBackHeldLocked(shared, 7) {
		t.Fatal("an AckRoute must not hold the source-ACK gate")
	}
}

// T19b. An ORDINARY AckRoute is removed by its exchange's ordinary Terminal observation (no
// abandonment fact), matched by lineage.
func TestF088AckRouteEndsByLineage(t *testing.T) {
	t.Parallel()
	sess := testLateAckUnitSess()
	b := testLateAckUnitBinding(sess, mqttAckRoleAckRoute, 9, "n9", "lin9", "dur")
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !sess.installAbandonedAckLocked(b) {
		t.Fatal("fixture: the AckRoute must install")
	}
	x := &mqttOutboundExchange{ID: sess.id, Epoch: b.epoch, Gen: sess.gen, Nonce: "lin9", PI: 9, QoS: 1,
		Phase: mqttExchangeTerminal}
	other := *x
	other.Nonce = "lin-other"
	sess.observeAbandonedAckEndLocked(&other)
	if sess.abandonedAcks[b.index()] == nil {
		t.Fatal("another lineage's ending must not remove the binding")
	}
	sess.observeAbandonedAckEndLocked(x)
	if sess.abandonedAcks[b.index()] != nil {
		t.Fatal("the AckRoute must be removed by its own lineage's Terminal observation")
	}
}

// T19c. An abandonment's HandBack UPGRADES the same allocation's AckRoute; a binding of a
// different allocation on the same identifier is never overwritten, and an AckRoute never
// replaces a HandBack.
func TestF088AbandonmentUpgradesAnAckRoute(t *testing.T) {
	t.Parallel()
	sess := testLateAckUnitSess()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	route := testLateAckUnitBinding(sess, mqttAckRoleAckRoute, 10, "n10", "lin10", "dur")
	if !sess.installAbandonedAckLocked(route) {
		t.Fatal("fixture: the AckRoute must install")
	}
	foreign := testLateAckUnitBinding(sess, mqttAckRoleHandBack, 10, "n-other", "lin-other", "dur")
	if sess.installAbandonedAckLocked(foreign) || sess.abandonedAcks[route.index()] != route {
		t.Fatal("another allocation's HandBack must never overwrite the binding")
	}
	hb := testLateAckUnitBinding(sess, mqttAckRoleHandBack, 10, "n10", "lin10", "dur")
	if !sess.installAbandonedAckLocked(hb) || sess.abandonedAcks[route.index()] != hb {
		t.Fatal("the same allocation's HandBack must replace its AckRoute")
	}
	if sess.installAbandonedAckLocked(route) || sess.abandonedAcks[route.index()] != hb {
		t.Fatal("an AckRoute must never replace a HandBack")
	}
}

// T11. Arm 4 is MARKER-ONLY: the cancellation bit is not evidence of non-transmission. Matrix
// marker x Cancelled over a `publish` register bound to this allocation.
func TestMQTTSlotAuthorityCancellationIsNotNonTransmission(t *testing.T) {
	t.Parallel()
	subject := mqttInflightSubject("hhhhhhhh", "gen-1", 3)
	for _, tc := range []struct {
		name      string
		cancelled bool
		marker    mqttSlotMarkerProof
		want      mqttSlotAuthority
	}{
		{"marker standing", false, mqttSlotMarkerPresent, mqttSlotProvenUnsentAuthority},
		{"Cancelled, marker standing", true, mqttSlotMarkerPresent, mqttSlotProvenUnsentAuthority},
		{"marker absent", false, mqttSlotMarkerAbsent, mqttSlotReservedAuthority},
		{"Cancelled, marker absent -> reserved", true, mqttSlotMarkerAbsent, mqttSlotReservedAuthority},
		{"marker indeterminate", false, mqttSlotMarkerIndeterminate, mqttSlotWithhold},
		{"Cancelled, marker indeterminate", true, mqttSlotMarkerIndeterminate, mqttSlotWithhold},
	} {
		rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 3, Dur: "dur", Seq: 4, QoS: 2,
			Nonce: "n3", Acq: 1})
		if tc.cancelled {
			rec.Cancelled, rec.CancelOrigin = true, mqttCancelOriginOccupant
		}
		ev := mqttSlotEvidence{readable: true, rec: rec, marker: tc.marker,
			bound: mqttSlotIdentity{subject: subject, nonce: rec.Nonce, acq: rec.Acq}}
		if got := mqttSlotReleaseAuthority(ev); got != tc.want {
			t.Errorf("%s: authority %d, want %d", tc.name, got, tc.want)
		}
	}

	// "verdict, marker absent -> reserved" (M2's target): a DURABLY COVERED cancellation verdict
	// for the register's nonce (cancelledRegNonces insertRev <= voidedRegsSavedRev, both seeded)
	// is no evidence of non-transmission. Through the evidence constructor itself
	// (mqttSlotEvidenceFromRecord, the one path a verdict reader would sit on), with the marker
	// authoritatively absent.
	t.Run("verdict, marker absent -> reserved", func(t *testing.T) {
		sess := testLateAckUnitSess()
		sess.asm.testInflightMarkerLoad = func(string) (*StoredMsg, error) { return nil, nil } // absent
		vsubject := mqttInflightSubject(sess.idHash, sess.gen, 3)
		rec := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 3, Dur: "dur", Seq: 4, QoS: 1,
			Nonce: "n3v", Acq: 1})
		sess.mu.Lock()
		sess.cancelledRegNonces = map[string]uint64{rec.Nonce: 5}
		sess.voidedRegsSavedRev = 7
		covered := sess.cancelledRegNonces[rec.Nonce] <= sess.voidedRegsSavedRev
		sess.mu.Unlock()
		if !covered {
			t.Fatal("fixture: the verdict must be durably covered")
		}
		ev := sess.mqttSlotEvidenceFromRecord(vsubject, rec, mqttSlotIdentityOf(vsubject, rec))
		if !ev.readable || ev.rec != rec {
			t.Fatalf("witness: the evidence constructor must read the record in hand, got %+v", ev)
		}
		if got := mqttSlotReleaseAuthority(ev); got != mqttSlotReservedAuthority {
			t.Fatalf("a covered verdict with the marker absent must reserve: authority %d (marker %v), want %d",
				got, ev.marker, mqttSlotReservedAuthority)
		}
	})
}

// T11b `recogniser` and `evidence`: the acknowledgement arrow admits a body that clears a
// known-false OCCUPANT cancellation (both members together), and the boundary's evidence half
// requires the marker to read authoritatively absent.
func TestMQTTAckArrowClearsAKnownFalseCancellation(t *testing.T) {
	t.Parallel()
	cur := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 3, Dur: "dur", Seq: 4, QoS: 1,
		Nonce: "n3", Acq: 1, Cancelled: true, CancelOrigin: mqttCancelOriginOccupant})
	t.Run("recogniser", func(t *testing.T) {
		cleared := *cur
		cleared.State, cleared.Cancelled, cleared.CancelOrigin = mqttInflightPublishTerminal, false, _EMPTY_
		if !mqttIsAckPhaseAdvance(cur, &cleared) {
			t.Fatal("the acknowledgement arrow must admit the cleared body")
		}
		half := cleared
		half.CancelOrigin = mqttCancelOriginOccupant
		if mqttIsAckPhaseAdvance(cur, &half) {
			t.Fatal("clearing Cancelled while keeping its provenance is no recognised arrow")
		}
		fence := *cur
		fence.CancelOrigin = mqttCancelOriginPendingCreate
		if mqttClearsCancellation(&fence, &cleared) {
			t.Fatal("a pending fence's cancellation is never cleared")
		}
	})
	t.Run("evidence", func(t *testing.T) {
		cleared := *cur
		cleared.State, cleared.Cancelled, cleared.CancelOrigin = mqttInflightPublishTerminal, false, _EMPTY_
		for _, tc := range []struct {
			name string
			load mqttMarkerLoad
			want bool
		}{
			{"absent", mqttMarkerLoad{kind: mqttInflightResponseAbsent}, true},
			{"standing", mqttMarkerLoad{kind: mqttInflightResponsePresent, marker: &mqttInflightMarker{Nonce: "n3"}}, false},
			{"indeterminate", mqttMarkerLoad{kind: mqttInflightResponseIndeterminate}, false},
		} {
			if got := mqttRefutedCancellation(cur, &cleared, tc.load); got != tc.want {
				t.Errorf("marker %s: refuted=%v, want %v", tc.name, got, tc.want)
			}
		}
	})
	t.Run("builder", func(t *testing.T) {
		f := testLateAckTransmitted(t, "f088-t11b", 1)
		// The known-false write: an OCCUPANT cancellation over the transmitted register (marker
		// gone), written at the raw boundary as the copy-and-CAS fallback would write it.
		rec, seq := f.register(t)
		tomb := *rec
		tomb.Cancelled, tomb.CancelOrigin = true, mqttCancelOriginOccupant
		if _, err := f.e.asm.jsa.storeInflightCAS(f.subject, seq, &tomb); err != nil {
			t.Fatalf("fixture: writing the occupant cancellation: %v", err)
		}
		var advanced *mqttInflightRecord
		hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			for _, m := range tr.Extra {
				if m.subj == f.subject {
					if r, derr := mqttDecodeInflightRecord(m.body); derr == nil && r.State != mqttInflightPublish {
						advanced = r
					}
				}
			}
			return send()
		}
		f.e.asm.jsa.testTransitionCommit.Store(&hook)
		t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
		if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
			t.Fatalf("PUBACK: %v", err)
		}
		testCustodyEventually(t, "the acknowledged exchange ends", func() bool {
			x := f.exchange(t)
			return x != nil && x.Phase == mqttExchangeTerminal
		})
		if advanced == nil || advanced.Cancelled || advanced.CancelOrigin != _EMPTY_ {
			t.Fatalf("the advance must write the register with BOTH cancellation members cleared, got %+v", advanced)
		}
	})
}

// ---------------------------------------------------------------------------
// Cold resume on a peer: recovery's cancellation classification (WP3 edit 4)
// ---------------------------------------------------------------------------

// testF088CancelSeen is one firing of recovery's cancellation-classification seam.
type testF088CancelSeen struct {
	pi           uint16
	nonce        string
	recCancelled bool
	verdict      bool
	markerSeq    uint64
}

// testF088Peer is broker B after a cold COMPLETE resume of the fixture's session.
type testF088Peer struct {
	asm  *mqttAccountSessionManager
	sess *mqttSession
	c    *client
	mu   sync.Mutex
	seen []testF088CancelSeen
}

func (p *testF088Peer) cancellations() []testF088CancelSeen {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]testF088CancelSeen(nil), p.seen...)
}

func (p *testF088Peer) entry(pi uint16) *mqttPending {
	p.sess.mu.Lock()
	defer p.sess.mu.Unlock()
	return p.sess.pendingPublish[pi]
}

// testF088ResumeOnPeer persists A's session record (A stops serving here: nothing further is
// driven on it), restores the session on peer B and runs B's recovery with the cancellation
// seam observing it. The resume must be COMPLETE.
func testF088ResumeOnPeer(t *testing.T, f *testWithdrawFix, node string) *testF088Peer {
	t.Helper()
	if err := testSessSave(t, f.e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	asm, sess, c := f.e.peer(t, node, f.e.sess.id)
	p := &testF088Peer{asm: asm, sess: sess, c: c}
	asm.mu.Lock()
	asm.testRecoverCancellation = func(pi uint16, nonce string, recCancelled, verdict bool, markerSeq uint64) {
		p.mu.Lock()
		p.seen = append(p.seen, testF088CancelSeen{pi, nonce, recCancelled, verdict, markerSeq})
		p.mu.Unlock()
	}
	asm.mu.Unlock()
	if _, err := sess.recoverInflightRegisters(c); err != nil {
		t.Fatalf("B's recovery: %v", err)
	}
	sess.mu.Lock()
	incomplete := sess.piSpaceIncomplete
	sess.c = c
	sess.mu.Unlock()
	if incomplete {
		t.Fatal("fixture: B's resume must be COMPLETE")
	}
	return p
}

// requireMarkerlessCancellationMet is the REACH witness, asserted before any outcome: recovery's
// cancellation classification met p, by the cancellation fact the test names, with the marker
// authoritatively ABSENT (no marker sequence on a decided send state).
func (p *testF088Peer) requireMarkerlessCancellationMet(t *testing.T, pi uint16, nonce string, byRecord, byVerdict bool) {
	t.Helper()
	for _, s := range p.cancellations() {
		if s.pi != pi || s.nonce != nonce {
			continue
		}
		if s.recCancelled != byRecord || s.verdict != byVerdict {
			t.Fatalf("witness: the classification met %d with Cancelled=%v verdict=%v, want %v/%v",
				pi, s.recCancelled, s.verdict, byRecord, byVerdict)
		}
		if s.markerSeq != 0 {
			t.Fatalf("witness: the classification met %d with a standing marker (seq %d), want absent", pi, s.markerSeq)
		}
		return
	}
	t.Fatalf("witness: recovery's cancellation classification never met packet identifier %d (seen %+v)",
		pi, p.cancellations())
}

// captureAdvance records the register member every advance-exchange transition on B writes for
// subject, so the test can read whether the acknowledgement cleared the cancellation.
func (p *testF088Peer) captureAdvance(t *testing.T, subject string) func() *mqttInflightRecord {
	t.Helper()
	var mu sync.Mutex
	var advanced *mqttInflightRecord
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionAdvanceExchange {
			for _, m := range tr.Extra {
				if m.subj != subject {
					continue
				}
				if r, derr := mqttDecodeInflightRecord(m.body); derr == nil && r.State != mqttInflightPublish {
					mu.Lock()
					advanced = r
					mu.Unlock()
				}
			}
		}
		return send()
	}
	p.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { p.asm.jsa.testTransitionCommit.Store(nil) })
	return func() *mqttInflightRecord {
		mu.Lock()
		defer mu.Unlock()
		return advanced
	}
}

// T5d. A TRANSMITTED ordinary exchange (marker consumed by its send intent) whose register carries
// an OCCUPANT cancellation, cold-resumed on peer B. Recovery's cancellation classification meets
// it with the marker ABSENT (witnessed first), so edit 4 routes it as the possibly-attempted
// register it is: B restores it ACTING, the client's PUBACK ends the exchange, and the advance
// clears BOTH cancellation members (edit 3's refuted clearance).
//
// INPUT, STATED (R8-F2): the cancellation is written by the PRODUCTION writer
// (markInflightRegisterCancelled, its copy-and-CAS fallback) invoked DIRECTLY over the transmitted
// register. Its production SCHEDULE (PS-1: a cancellation decided over a possibly-transmitted
// register) is not demonstrated here: the consumer teardown's purge `continue`s past a transmitted
// entry, so no teardown hands a transmitted register to this writer. Boundary-hardening and M6's
// evidence only.
//
// Mutation M6 (edit 4 reverted): the markerless cancellation keeps the pre-edit cancellation
// disposition -> p is not restored acting.
func TestF088MarkerlessCancelledRegisterRestoresActingOnColdResume(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f088-t5d", 1)
	if f.markerStands() {
		t.Fatal("fixture: the transmission must have consumed the unsent marker")
	}
	_, seq := f.register(t)
	out, err := f.e.sess.markInflightRegisterCancelled(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq, seq, f.nonce,
		mqttLicenceOf(f.e.c))
	if err != nil || out != mqttRegCancelStored {
		t.Fatalf("fixture: the production cancellation writer must store its tombstone: %v (%v)", out, err)
	}
	if cur, _ := f.register(t); cur == nil || !cur.Cancelled || cur.CancelOrigin != mqttCancelOriginOccupant {
		t.Fatalf("fixture: the register must carry an occupant cancellation, got %+v", cur)
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("fixture: a markerless cancellation must not end the exchange, got %+v", x)
	}

	p := testF088ResumeOnPeer(t, f.testWithdrawFix, "node2-t5d")
	p.requireMarkerlessCancellationMet(t, f.pi, f.nonce, true, false)

	ent := p.entry(f.pi)
	if ent == nil || !ent.acts() {
		t.Fatalf("B's recovery must restore an ACTING entry for the markerless cancelled register, got %+v", ent)
	}
	t.Logf("observed: restored entry transmitted=%v (a retransmission carries DUP=1)", ent.transmitted)
	advanced := p.captureAdvance(t, f.subject)
	if err := p.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK on B: %v", err)
	}
	testCustodyEventually(t, "the PUBACK on B ends the exchange", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
	if a := advanced(); a == nil || a.Cancelled || a.CancelOrigin != _EMPTY_ {
		t.Fatalf("the advance must clear BOTH cancellation members, got %+v", a)
	}
}

// T5e. As T5d, but the register itself is NOT cancelled: the session record carries a DURABLY
// COVERED verdict naming its nonce (staged, then serialized by a landed save). Recovery's
// classification meets it on the verdict with the marker absent (witnessed first); B restores it
// acting and the client's PUBACK ends it.
//
// INPUT, STATED (R8-F2): the verdict is staged through the production entry
// (stageCancellationVerdictLocked) invoked DIRECTLY over a transmitted allocation, and made
// durable by a real save. PS-2's production schedule (a covered verdict over a possibly-transmitted
// register) is not demonstrated here. Boundary-hardening; recorded separately from M6's T5d.
func TestF088CoveredVerdictRegisterRestoresActingOnColdResume(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f088-t5e", 1)
	if f.markerStands() {
		t.Fatal("fixture: the transmission must have consumed the unsent marker")
	}
	f.e.sess.mu.Lock()
	staged := f.e.sess.stageCancellationVerdictLocked(mqttTestLicenceLocked(f.e.sess, nil), f.nonce)
	f.e.sess.mu.Unlock()
	if !staged {
		t.Fatal("fixture: the verdict must be staged under the accepted entitlement")
	}
	if err := testSessSave(t, f.e.sess); err != nil {
		t.Fatalf("fixture: the save that covers the verdict: %v", err)
	}
	f.e.sess.mu.Lock()
	insertRev, held := f.e.sess.cancelledRegNonces[f.nonce]
	covered := held && insertRev <= f.e.sess.voidedRegsSavedRev
	f.e.sess.mu.Unlock()
	if !covered {
		t.Fatalf("fixture: the verdict must be DURABLY covered (held %v, insert %d, saved %d)",
			held, insertRev, f.e.sess.voidedRegsSavedRev)
	}
	if cur, _ := f.register(t); cur == nil || cur.Cancelled {
		t.Fatalf("fixture: the register itself must not be cancelled, got %+v", cur)
	}

	p := testF088ResumeOnPeer(t, f.testWithdrawFix, "node2-t5e")
	p.requireMarkerlessCancellationMet(t, f.pi, f.nonce, false, true)

	if ent := p.entry(f.pi); ent == nil || !ent.acts() {
		t.Fatalf("B's recovery must restore an ACTING entry for the verdict-named register, got %+v", ent)
	}
	if err := p.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK on B: %v", err)
	}
	testCustodyEventually(t, "the PUBACK on B ends the exchange", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
}

// T6. The QoS 2 analogue of T5: a transmitted QoS 2 exchange untracked UNANSWERED on A (its
// AckRoute stands there), cold-resumed on peer B, which restores it acting. `refused`: a failed
// PUBREC on B ends it Rejected. `completed`: a successful PUBREC on B records the intent and the
// canonical PUBREL, the PUBREL is delivered and transmitted on B, and the client's PUBCOMP ends
// it Terminal.
func TestF088PreservedQoS2CompletesAcrossColdResume(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T, clientID string) (*testLateAckFix, *testF088Peer) {
		f := testLateAckTransmitted(t, clientID, 2)
		f.untrackByDeny(t)
		if b := f.binding(); b == nil || b.role != mqttAckRoleAckRoute {
			t.Fatalf("fixture: the AckRoute must stand on A, got %+v", b)
		}
		p := testF088ResumeOnPeer(t, f.testWithdrawFix, "node2-"+clientID)
		if ent := p.entry(f.pi); ent == nil || !ent.acts() {
			t.Fatalf("B's recovery must restore an ACTING entry for the preserved QoS 2 register, got %+v", ent)
		}
		return f, p
	}
	t.Run("refused", func(t *testing.T) {
		t.Parallel()
		f, p := setup(t, "f088-t6-refused")
		_ = p.c.mqttProcessPubRec(f.pi, 0x80)
		testCustodyEventually(t, "the failed PUBREC ends the exchange Rejected", func() bool {
			x := f.exchange(t)
			return x != nil && x.Phase == mqttExchangeRejected
		})
		// Rejected with NO PUBREL: the register never reaches the PUBREL side.
		if cur, _ := f.register(t); cur != nil && (mqttInflightPastPubRec(cur.State) || cur.PubRelStreamSeq != 0) {
			t.Fatalf("a refused PUBREC must materialize no PUBREL, got %+v", cur)
		}
	})
	t.Run("completed", func(t *testing.T) {
		t.Parallel()
		f, p := setup(t, "f088-t6-completed")
		// B as a real broker's connection would be: an output identity, the accepted record, the
		// PUBREL consumer (no delivery subscription: the PUBREL is delivered by hand below), and a
		// recording transport.
		pe := &testInflightEnv{s: p.c.srv, asm: p.asm, sess: p.sess, c: p.c}
		testConnIdentity(t, pe, p.c)
		p.sess.mu.Lock()
		p.sess.pubRelSubscribed = true
		p.sess.mu.Unlock()
		testPersistAcceptedRecord(t, pe)
		pe.subscribePubRel(t)
		testRow2Wire(pe)

		if err := p.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
			t.Fatalf("PUBREC on B: %v", err)
		}
		var rec *mqttInflightRecord
		testCustodyEventually(t, "the successful PUBREC materializes the canonical PUBREL", func() bool {
			rec, _ = f.register(t)
			return rec != nil && rec.State == mqttInflightPubRelReady && rec.PubRelStreamSeq != 0
		})
		if x := f.exchange(t); x == nil || x.Phase != mqttExchangePubRecObserved {
			t.Fatalf("the PUBREC must be observed on the exchange, got %+v", x)
		}
		p.sess.mu.Lock()
		_, _, pubRelDur := mqttPubRelNames(p.sess.idHash, p.sess.gen)
		p.sess.mu.Unlock()
		testDeliverPubRelCorrelated(t, pe, f.pi, testInflightAckSubject(pubRelDur, rec.PubRelStreamSeq, 1),
			rec.Nonce, rec.Dur, rec.Seq)
		pe.s.finishOwedActions()
		testCustodyEventually(t, "the PUBREL action drains", func() bool {
			cur, _ := f.register(t)
			return cur != nil && cur.ActionToken == _EMPTY_
		})
		p.c.mqttProcessPubComp(f.pi)
		pe.s.finishOwedTerminals()
		testCustodyEventually(t, "the PUBCOMP on B ends the exchange", func() bool {
			x := f.exchange(t)
			return x != nil && x.Phase == mqttExchangeTerminal
		})
	})
}

// TestF088RecoveryHandoffRetiresTheAckRoute backs T13c's recovery-handoff reason: a resident
// CONNECT's recovery on the SAME session object restores an acting entry for the allocation an
// AckRoute names, and the AckRoute is removed (HANDOFF): the restored entry now owns the
// acknowledgement, and the PUBACK ends the exchange through it.
func TestF088RecoveryHandoffRetiresTheAckRoute(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f088-handoff", 1)
	f.untrackByDeny(t)
	if b := f.binding(); b == nil || b.role != mqttAckRoleAckRoute {
		t.Fatalf("fixture: the AckRoute must stand, got %+v", b)
	}
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if ent := f.entry(); ent == nil || !ent.acts() {
		t.Fatalf("the resident recovery must restore an ACTING entry, got %+v", ent)
	}
	if b := f.binding(); b != nil {
		t.Fatalf("the restoration must hand the AckRoute off (removed), got %+v", b)
	}
	if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	testCustodyEventually(t, "the restored entry's PUBACK ends the exchange", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
}

// T3b. The DIFFERENT-SOURCE arm, reached through the ordinary admission (admitInflightPublish):
// a fresh allocation meets an occupant naming ANOTHER delivery whose source is provably complete
// but whose unsent marker is gone (possibly attempted). Source completion is not exchange
// completion: the arm RESERVES (the occupant is preserved at its own sequence) and ABANDONS this
// allocation (the identifier excluded, the source deferred).
//
// INPUT, STATED: the occupant is seeded (a real admission batch landed directly on the identifier
// with its marker then deleted); a fresh allocation reaching an identifier still holding another
// delivery's register is the lost-delete schedule the arm exists for.
//
// Mutation M3a (the arm treats sourceComplete as proven-unsent): the occupant is reaped -> fails.
func TestF097DifferentSourceArmReservesAPossiblyAttemptedOccupant(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "f097-t3b")
	dur := e.sess.idHash + "_" + nuid.Next()
	e.sess.mu.Lock()
	e.sess.last_pi = 10
	e.sess.mu.Unlock()
	occ := e.landAdmission(t, 11, 1, dur, 7, mqttNewInflightNonce())
	_, occMsg := e.loadRegister(t, 11)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, 11)
	ml := e.asm.jsa.loadOccupantMarkerLoad(subject)
	if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, subject, ml.seq, true); err != nil {
		t.Fatalf("fixture: consuming the occupant's marker: %v", err)
	}
	if load := e.asm.loadInflightMarker(mqttMarkerCallerStore, subject); load.kind != mqttInflightResponseAbsent {
		t.Fatalf("fixture: the occupant must be possibly attempted (no marker), got kind %v", load.kind)
	}
	if st := e.asm.inflightSourceState(occ); st != mqttInflightSourceComplete {
		t.Fatalf("fixture: the occupant's source must be provably complete, got %v", st)
	}

	// REACH WITNESS: the different-source arm is the admission ladder's only reader of a source's
	// state, and it asks about the OCCUPANT's delivery. The seam answers what the real read above
	// answered (complete), so it observes without changing the arm's input.
	var askedOccupant atomic.Bool
	e.asm.mu.Lock()
	e.asm.testInflightSourceState = func(r *mqttInflightRecord) mqttInflightSourceState {
		if r != nil && r.Dur == dur && r.Seq == 7 {
			askedOccupant.Store(true)
			return mqttInflightSourceComplete
		}
		return mqttInflightSourceUnknown
	}
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, 600, 1))
	e.sess.mu.Unlock()
	if pi != 11 || !isNew {
		t.Fatalf("fixture: the allocation must land on the occupied identifier 11, got %d (new %v)", pi, isNew)
	}
	if e.sess.admitInflightPublish(e.c, pi, 1, dur, 600, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatal("an allocation over a possibly-attempted occupant of another delivery must not be admitted")
	}
	if !askedOccupant.Load() {
		t.Fatal("witness: the different-source arm never asked for the occupant's source state")
	}
	rec, smsg := e.loadRegister(t, 11)
	if rec == nil || rec.Nonce != occ.Nonce || smsg == nil || smsg.Sequence != occMsg.Sequence {
		t.Fatalf("the possibly-attempted occupant must be preserved at its own sequence %d, got %+v", occMsg.Sequence, rec)
	}
	e.sess.mu.Lock()
	ent := e.sess.pendingPublish[11]
	inst := e.sess.slotExclusionInstLocked(11)
	e.sess.mu.Unlock()
	if ent != nil {
		t.Fatalf("the allocation must be abandoned (no entry left), got %+v", ent)
	}
	if inst == 0 {
		t.Fatal("the abandoned identifier must stay excluded (reserved), not freed")
	}
}

// T11c. A token ACQUISITION over a GENUINE cancellation (an occupant cancellation whose unsent
// marker STANDS) passes beginSendLocked, reaches the durable boundary with a member that clears the
// cancellation, and is REFUSED there; the register keeps its cancellation, its counter and no
// token. Positive control `refuted`: with the marker authoritatively absent the same acquisition
// lands and clears both members.
func TestMQTTAcquisitionOverAGenuineCancellationIsRefused(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, clientID string, consumeMarker bool) {
		f := testWithdrawAdmitted(t, clientID, 1)
		cur, seq := f.register(t)
		tomb := *cur
		tomb.Cancelled, tomb.CancelOrigin = true, mqttCancelOriginOccupant
		if _, err := f.e.asm.storeInflightCAS(f.subject, seq, &tomb); err != nil {
			t.Fatalf("fixture: writing the occupant cancellation: %v", err)
		}
		if consumeMarker {
			ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
			if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
				t.Fatalf("fixture: consuming the marker: %v", err)
			}
		} else if !f.markerStands() {
			t.Fatal("fixture: a GENUINE cancellation keeps its marker standing")
		}
		before, beforeSeq := f.register(t)
		var mu sync.Mutex
		var clearing bool
		var boundaryErr error
		hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionAcquireAction {
				return send()
			}
			for _, m := range tr.Extra {
				if m.subj != f.subject {
					continue
				}
				if r, derr := mqttDecodeInflightRecord(m.body); derr == nil && !r.Cancelled && r.CancelOrigin == _EMPTY_ {
					mu.Lock()
					clearing = true
					mu.Unlock()
				}
			}
			s, err := send()
			mu.Lock()
			boundaryErr = err
			mu.Unlock()
			return s, err
		}
		f.e.asm.jsa.testTransitionCommit.Store(&hook)
		t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
		f.e.bind()
		tok := f.e.openSection(t, f.pi, testRealPubRelSrcDur, f.sseq)
		acquired := f.e.sess.acquirePublishAction(f.e.c, f.pi, &tok)
		mu.Lock()
		reached, berr := clearing, boundaryErr
		mu.Unlock()
		if !reached {
			t.Fatal("witness: the acquisition never reached the durable boundary with a clearing member")
		}
		after, afterSeq := f.register(t)
		if consumeMarker {
			if !acquired || berr != nil {
				t.Fatalf("positive control: a refuted cancellation's acquisition must land (%v)", berr)
			}
			if after == nil || after.Cancelled || after.CancelOrigin != _EMPTY_ || after.ActionToken == _EMPTY_ ||
				after.Acq != before.Acq+1 {
				t.Fatalf("positive control: the acquisition must clear both members and install its token, got %+v", after)
			}
			return
		}
		if acquired {
			t.Fatal("an acquisition over a genuine cancellation must be refused")
		}
		if berr == nil || !strings.Contains(berr.Error(), "clears the cancellation") {
			t.Fatalf("the refusal must be the boundary's evidence check, got %v", berr)
		}
		if after == nil || afterSeq != beforeSeq || !after.Cancelled || after.CancelOrigin != mqttCancelOriginOccupant ||
			after.ActionToken != _EMPTY_ || after.Acq != before.Acq {
			t.Fatalf("the refused acquisition must leave the register whole at %d, got %+v at %d", beforeSeq, after, afterSeq)
		}
		if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
			t.Fatalf("the exchange must stay at Publish, got %+v", x)
		}
	}
	t.Run("genuine", func(t *testing.T) { t.Parallel(); run(t, "f088-t11c", false) })
	t.Run("refuted", func(t *testing.T) { t.Parallel(); run(t, "f088-t11c-ctl", true) })
}

// T16 (Q4). A verdict naming a TRANSMITTED allocation (marker gone) is no destruction licence: the
// register is present and arm 4 reserves it before the late acknowledgement. The client's PUBACK
// ends the exchange, terminal cleanup deletes the register through the door, and the door's
// confirmed deletion RETIRES the verdict.
//
// INPUT, STATED: the verdict is staged directly through the production entry
// (stageCancellationVerdictLocked) over the transmitted allocation and saved; no P-b producer
// schedule is claimed (M1b's condition).
func TestF088VerdictRetiredByTerminalCleanup(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f088-t16", 1)
	f.e.sess.mu.Lock()
	staged := f.e.sess.stageCancellationVerdictLocked(mqttTestLicenceLocked(f.e.sess, nil), f.nonce)
	f.e.sess.mu.Unlock()
	if !staged {
		t.Fatal("fixture: the verdict must be staged")
	}
	if err := testSessSave(t, f.e.sess); err != nil {
		t.Fatalf("fixture: saving the verdict: %v", err)
	}
	rec, seq := f.register(t)
	if rec == nil {
		t.Fatal("the register must be present before the late ACK")
	}
	if rel := f.e.sess.mintSlotRelease(f.subject, rec, seq); rel.auth != mqttSlotReservedAuthority {
		t.Fatalf("arm 4 must reserve a verdict-named register whose marker is gone, got %d", rel.auth)
	}
	verdicted := func() bool {
		f.e.sess.mu.Lock()
		defer f.e.sess.mu.Unlock()
		_, ok := f.e.sess.cancelledRegNonces[f.nonce]
		return ok
	}
	if !verdicted() {
		t.Fatal("fixture: the verdict must stand before the late ACK")
	}
	if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	testCustodyEventually(t, "the late PUBACK ends the exchange", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
	testCustodyEventually(t, "terminal cleanup deletes the register", func() bool {
		cur, _ := f.register(t)
		return cur == nil
	})
	testCustodyEventually(t, "the confirmed deletion retires the verdict", func() bool { return !verdicted() })
}

// T17 (WP3 edit 5, STAGED: the boundary form). A REGISTER-ONLY REPLACEMENT lands over an inherited
// same-delivery occupant whose PUBLISH may be on the wire (its marker is gone), and the allocation
// is cancelled between the replacement's CAS and its confirmation. confirmInflightRegister's
// cancelled arm (transmitted=true) then destroys and tombstones NOTHING: the replacement register
// survives uncancelled at Publish, only OUR cancelled placeholder goes, and the identifier stays
// excluded.
//
// INPUT, STATED: the inherited occupant is SEEDED (an admission batch landed directly on the
// identifier the next allocation takes, its marker then deleted); W5c's production schedule is NOT
// reached by this test, so the edit's acting-entry reconstruction and drain re-enrolment stay
// behind W5c and are not asserted. The cancellation is the consumer teardown's purge of the
// pending allocation, run inside the write seam after the replacement landed; the teardown's
// separate securing pass is not run.
func TestF088InheritedReplacementCancellationEnrolsTheDrain(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "f088-t17")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	e.sess.mu.Lock()
	e.sess.last_pi = 10
	e.sess.mu.Unlock()
	occ := e.landAdmission(t, 11, 1, dur, sseq, mqttNewInflightNonce())
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, 11)
	ml := e.asm.jsa.loadOccupantMarkerLoad(subject)
	if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, subject, ml.seq, true); err != nil {
		t.Fatalf("fixture: consuming the inherited occupant's marker: %v", err)
	}
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if pi != 11 || !isNew {
		t.Fatalf("fixture: the re-offer must allocate the inherited identifier 11, got %d (new %v)", pi, isNew)
	}
	nonce := e.nonceOf(pi)
	var deletes atomic.Int32
	var registerOnly, cancelled atomic.Bool
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(seq uint64) error {
		deletes.Add(1)
		return e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
	}
	e.asm.testAdmitWrite = func(s *mqttSession, ro bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, err := write()
		if s == e.sess && ro && err == nil && cancelled.CompareAndSwap(false, true) {
			registerOnly.Store(true)
			// Between the replacement's landing and its confirmation: the teardown cancels the
			// pending allocation (its placeholder replaces the entry, its verdict is staged).
			s.mu.Lock()
			s.purgeConsumerPendingLocked(dur, mqttTestLicenceLocked(s, nil))
			s.mu.Unlock()
		}
		return out, err
	}
	e.asm.mu.Unlock()
	e.sess.admitInflightPublish(e.c, pi, 1, dur, sseq, nonce, _EMPTY_, _EMPTY_)
	if !registerOnly.Load() {
		t.Fatal("witness: the admission never landed a register-only replacement over the inherited occupant")
	}
	rec, _ := e.loadRegister(t, 11)
	if rec == nil || rec.Nonce != nonce || rec.Lin != occ.Lin || rec.State != mqttInflightPublish {
		t.Fatalf("the landed replacement (inherited lineage %q) must survive at publish, got %+v", occ.Lin, rec)
	}
	if rec.Cancelled || rec.CancelOrigin != _EMPTY_ {
		t.Fatalf("the cancellation must tombstone nothing over a possibly-transmitted inheritance, got %+v", rec)
	}
	if n := deletes.Load(); n != 0 {
		t.Fatalf("nothing may be destroyed, but %d register delete(s) were issued", n)
	}
	x, _, err := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, occ.Lin)
	if err != nil || x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("the inherited exchange must stay at Publish, got %+v (%v)", x, err)
	}
	e.sess.mu.Lock()
	ent := e.sess.pendingPublish[11]
	inst := e.sess.slotExclusionInstLocked(11)
	e.sess.mu.Unlock()
	if ent != nil {
		t.Fatalf("only OUR cancelled placeholder goes, and it must be gone, got %+v", ent)
	}
	if inst == 0 {
		t.Fatal("the identifier must stay excluded")
	}
}

// ---------------------------------------------------------------------------
// Progress controls: a MARKER-BACKED cancellation is still withdrawn and cleaned
// ---------------------------------------------------------------------------

// T12. Marker-only arm 4 must not wedge the genuine population: a cancellation whose unsent marker
// STANDS is still withdrawn (WithdrawnUnsent) and its register cleaned, for every class.
//   - `tombstone-then-door`: the production cancellation writer over a marker-standing ordinary
//     register withdraws in the same batch; the door then finishes the tombstone (exchange ended).
//   - `teardown-ladder`: the consumer teardown's register ladder (purge, then voidRegistersOnStream)
//     withdraws and deletes the never-sent obligation, register and marker both.
//   - `affine`: an EXPERIMENTAL AFFINE (slot-only) pair with its marker standing is removed whole by
//     the same ladder; nothing is fabricated for a class with no exchange.
func TestF088MarkerBackedCancellationStillWithdraws(t *testing.T) {
	t.Parallel()
	t.Run("tombstone-then-door", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "f088-t12-tomb", 1)
		_, seq := f.register(t)
		out, err := f.e.sess.markInflightRegisterCancelled(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq, seq, f.nonce,
			mqttLicenceOf(f.e.c))
		if err != nil || out != mqttRegCancelStored {
			t.Fatalf("the marker-backed cancellation must store: %v (%v)", out, err)
		}
		if x := f.exchange(t); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
			t.Fatalf("a marker-backed cancellation must withdraw the obligation, got %+v", x)
		}
		tomb, tseq := f.register(t)
		if tomb == nil || !tomb.Cancelled {
			t.Fatalf("the withdrawal's tombstone must stand, got %+v", tomb)
		}
		if !f.e.sess.reapObservedRegister(f.e.c, f.subject, tseq, tomb, mqttSlotDeleteNeverTransmitted) {
			t.Fatal("the door must finish the withdrawn obligation's tombstone")
		}
		f.requireWithdrawnAndGone(t)
		testCustodyEventually(t, "the withdrawn obligation's charge is released", func() bool {
			return !testCustodyHasExchange(f.e.sess, f.lin)
		})
	})
	t.Run("teardown-ladder", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "f088-t12-ladder", 1)
		f.e.sess.mu.Lock()
		regs, _ := f.e.sess.purgeConsumerPendingLocked(testRealPubRelSrcDur, mqttTestLicenceLocked(f.e.sess, nil))
		f.e.sess.mu.Unlock()
		if len(regs) != 1 || regs[0].pi != f.pi {
			t.Fatalf("fixture: the teardown must void exactly the never-sent obligation, got %+v", regs)
		}
		if unproven := f.e.sess.voidRegistersOnStream(f.e.c, regs, mqttLicenceOf(f.e.c)); unproven {
			t.Fatal("the marker-backed void must be proven on the register stream")
		}
		f.requireWithdrawnAndGone(t)
		testCustodyEventually(t, "the marker follows its register", func() bool { return !f.markerStands() })
		testCustodyEventually(t, "the withdrawn obligation's charge is released", func() bool {
			return !testCustodyHasExchange(f.e.sess, f.lin)
		})
	})
	t.Run("affine", func(t *testing.T) {
		t.Parallel()
		a := newT36Affine(t, "f088-t12-affine")
		if !a.e.sess.storeInflightRegister(a.e.c, a.pi, a.qos, a.dur, a.sseq, a.nonce, _EMPTY_, _EMPTY_) {
			t.Fatal("fixture: the affine pair must land")
		}
		marker, _ := a.e.loadMarker(t, a.pi)
		if rec, _ := a.register(t); rec == nil || marker == nil || !mqttDurIsSlotOnlyClass(rec.Dur) {
			t.Fatalf("fixture: a slot-only pair with its marker standing is required, got %+v / %+v", rec, marker)
		}
		a.e.sess.mu.Lock()
		regs, _ := a.e.sess.purgeConsumerPendingLocked(a.dur, mqttTestLicenceLocked(a.e.sess, nil))
		a.e.sess.mu.Unlock()
		if len(regs) != 1 || regs[0].pi != a.pi {
			t.Fatalf("fixture: the teardown must void exactly the never-sent affine pair, got %+v", regs)
		}
		if unproven := a.e.sess.voidRegistersOnStream(a.e.c, regs, mqttLicenceOf(a.e.c)); unproven {
			t.Fatal("the marker-backed affine void must be proven on the register stream")
		}
		if rec, smsg := a.register(t); smsg != nil {
			t.Fatalf("the marker-backed affine register must be removed, got %+v", rec)
		}
		testCustodyEventually(t, "the affine marker follows its register", func() bool {
			m, _ := a.e.loadMarker(t, a.pi)
			return m == nil
		})
		if x := a.exchange(); x != nil {
			t.Fatalf("an affine delivery has no exchange, so none may be fabricated: %+v", x)
		}
	})
}

// T15 (Q0). EVERY DESTRUCTIVE ROUTE INTO THE DOOR ENDS THE EXCHANGE OR PRESERVES THE REGISTER: over
// an un-ended ordinary Publish exchange, each door entrance, under each destructive disposition,
// either refuses with the register and the exchange intact, or ends the exchange (WithdrawnUnsent)
// before the register goes. A possibly-attempted register (marker gone) is always PRESERVED; a
// never-sent one (marker standing) under a never-transmitted or complete disposition is withdrawn
// (positive control).
//
// The matrix is over the door's ENTRANCES: every production route reaches the door through one of
// them, and the routes into the two inner entrances are frozen below. Which functions may call the
// door's own delete is TestF105WithdrawDestructionHasOneDoor's census, not this test's.
func TestF088EveryDestructiveRouteEndsOrPreserves(t *testing.T) {
	t.Parallel()
	callers := testProductionCallers(t, map[string]bool{"reapLoadedRegister": true, "reapRegisterGatedOutcome": true})
	for callee, want := range map[string][]string{
		"reapLoadedRegister":       {"reapObservedRegister", "reclaimPendingFenceStep", "releaseBoundInflightRegister"},
		"reapRegisterGatedOutcome": {"reapRegisterWithVerdictGated", "recoverPublishTerminalSlot", "withdrawRetainedOnce"},
	} {
		if strings.Join(callers[callee], ",") != strings.Join(want, ",") {
			t.Errorf("the door entrance %s is reached from %v, frozen %v: a new route into the door must join "+
				"this matrix's evidence", callee, callers[callee], want)
		}
	}

	type entrance struct {
		name string
		call func(f *testWithdrawFix, cur *mqttInflightRecord, seq uint64, d mqttInflightDisposition) string
	}
	entrances := []entrance{
		{"reapLoadedRegister", func(f *testWithdrawFix, cur *mqttInflightRecord, seq uint64, d mqttInflightDisposition) string {
			return itoa(int(f.e.sess.reapLoadedRegister(f.subject, seq, cur, d, mqttLicenceOf(f.e.c), nil)))
		}},
		{"reapObservedRegister", func(f *testWithdrawFix, cur *mqttInflightRecord, seq uint64, d mqttInflightDisposition) string {
			if f.e.sess.reapObservedRegister(f.e.c, f.subject, seq, cur, d) {
				return "true"
			}
			return "false"
		}},
		{"reapRegisterWithVerdictGated", func(f *testWithdrawFix, cur *mqttInflightRecord, seq uint64, d mqttInflightDisposition) string {
			ref := mqttSlotRefFromRecord(f.subject, seq, cur)
			if f.e.sess.reapRegisterWithVerdictGated(seq, f.nonce, true, ref, d, mqttLicenceOf(f.e.c)) {
				return "true"
			}
			return "false"
		}},
		{"reapRegisterGatedOutcome", func(f *testWithdrawFix, cur *mqttInflightRecord, seq uint64, d mqttInflightDisposition) string {
			ref := mqttSlotRefFromRecord(f.subject, seq, cur)
			return itoa(int(f.e.sess.reapRegisterGatedOutcome(seq, f.nonce, true, ref, d, mqttLicenceOf(f.e.c))))
		}},
	}
	dispositions := []struct {
		name string
		d    mqttInflightDisposition
	}{
		{"never-transmitted", mqttSlotDeleteNeverTransmitted},
		{"complete", mqttSlotDeleteComplete},
		{"publish-terminal", mqttSlotDeletePublishTerminal},
		{"terminal", mqttSlotDeleteTerminal},
		{"pending-fence", mqttSlotDeletePendingFence},
	}
	for _, en := range entrances {
		en := en
		t.Run(en.name, func(t *testing.T) {
			t.Parallel()
			var e *testInflightEnv
			sseq := testRealPubRelSrcSeq
			for _, ds := range dispositions {
				for _, markerStanding := range []bool{true, false} {
					var f *testWithdrawFix
					if e == nil {
						f = testWithdrawAdmitted(t, "f088-t15-"+en.name, 1)
						e = f.e
					} else {
						sseq++
						f = testWithdrawAdmitMore(t, e, sseq, 1)
					}
					cell := ds.name + "/marker-absent"
					if markerStanding {
						cell = ds.name + "/marker-standing"
					} else {
						ml := e.asm.jsa.loadOccupantMarkerLoad(f.subject)
						if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
							t.Fatalf("%s: fixture: consuming the marker: %v", cell, err)
						}
					}
					cur, seq := f.register(t)
					got := en.call(f, cur, seq, ds.d)
					after, afterSeq := f.register(t)
					x := f.exchange(t)
					ended := x != nil && mqttExchangePhaseEnded(x.Phase)
					switch {
					case after == nil && !ended:
						t.Errorf("%s: the register was destroyed while its exchange is un-ended (%+v); entrance said %s",
							cell, x, got)
					case !markerStanding && (after == nil || afterSeq != seq || after.Nonce != f.nonce || ended):
						t.Errorf("%s: a possibly-attempted register must be preserved whole at %d with its exchange at "+
							"Publish, got %+v at %d, exchange %+v; entrance said %s", cell, seq, after, afterSeq, x, got)
					case after != nil && (afterSeq != seq || after.Nonce != f.nonce) && !ended:
						t.Errorf("%s: a refused destruction must leave the register whole at %d, got %+v at %d",
							cell, seq, after, afterSeq)
					}
					if markerStanding && (ds.d == mqttSlotDeleteNeverTransmitted || ds.d == mqttSlotDeleteComplete) &&
						(after != nil || x == nil || x.Phase != mqttExchangeWithdrawnUnsent) {
						t.Errorf("%s: positive control: a never-sent obligation must be withdrawn and its register "+
							"deleted, got register %+v, exchange %+v; entrance said %s", cell, after, x, got)
					}
					outcome := "preserved"
					if after == nil {
						outcome = "ended-then-deleted"
					}
					t.Logf("%s: entrance=%s outcome=%s", cell, got, outcome)
				}
			}
		})
	}
}

// T20. AN OWED WITHDRAWAL ACROSS A SAME-OBJECT RECONNECT. The consumer teardown's ladder decides
// a never-sent obligation's cancellation under connection A's entitlement, and its withdrawal's
// landing is UNKNOWN (seam): the item is owed under A's entitlement and the verdict staged. The
// client reconnects to the SAME session object under a new tenure. The dispatcher's retry of the
// stale item is refused at the boundary (mqttWithdrawUnlicensed) and the item compare-deleted;
// nothing is withdrawn under the stale licence.
//   - `successor-survives`: a successor item for the same obligation, recorded under the new
//     licence while the stale unit runs, is NOT retired by the stale unit (pointer identity), and
//     itself withdraws the obligation under the new licence.
//   - `connect-recovery`: the reconnect's recovery withdraws the obligation under the NEW licence
//     (the genuine-cancellation arm: the verdict names it and its marker stands).
func TestF088WithdrawalDebtAcrossSameObjectReconnect(t *testing.T) {
	t.Parallel()
	type world struct {
		f          *testWithdrawFix
		ent1, ent2 mqttOutboundEntitlement
		c2         *client
		pass       atomic.Bool
		mu         sync.Mutex
		staleErrs  []error
		landedEnts []mqttOutboundEntitlement
		onStale    func()
		onNew      func()
	}
	eventually := func(t *testing.T, what string, cond func() bool) {
		t.Helper()
		// The dispatcher's backoff after the unresolved attempts can approach its 5 s cap.
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("not within 20s: %s", what)
	}
	setup := func(t *testing.T, clientID string) *world {
		w := &world{f: testWithdrawAdmitted(t, clientID, 1)}
		f := w.f
		f.e.bind()
		w.ent1, _ = f.e.c.outboundEntitlement()
		f.e.asm.mu.Lock()
		f.e.asm.testWithdrawUnsent = func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if req.Subject != f.subject {
				return write()
			}
			if !w.pass.Load() {
				// The landing can never be read: UNRESOLVED.
				return mqttOpOutcome{Handle: &mqttTransitionHandle{}}, errors.New("test: the response was lost")
			}
			if req.Licence.ent == w.ent1 {
				if w.onStale != nil {
					w.onStale()
				}
				out, err := write()
				w.mu.Lock()
				w.staleErrs = append(w.staleErrs, err)
				w.mu.Unlock()
				return out, err
			}
			if w.onNew != nil {
				w.onNew()
			}
			out, err := write()
			if err == nil {
				w.mu.Lock()
				w.landedEnts = append(w.landedEnts, req.Licence.ent)
				w.mu.Unlock()
			}
			return out, err
		}
		f.e.asm.mu.Unlock()

		f.e.sess.mu.Lock()
		regs, _ := f.e.sess.purgeConsumerPendingLocked(testRealPubRelSrcDur, mqttTestLicenceLocked(f.e.sess, nil))
		f.e.sess.mu.Unlock()
		if !f.e.sess.voidRegistersOnStream(f.e.c, regs, mqttLicenceOf(f.e.c)) {
			t.Fatal("fixture: an unresolved withdrawal must leave the void unproven")
		}
		f.e.sess.mu.Lock()
		item := f.e.sess.withdrawOwed[f.lin]
		_, verdict := f.e.sess.cancelledRegNonces[f.nonce]
		f.e.sess.mu.Unlock()
		if item == nil || item.lic.ent != w.ent1 || item.nonce != f.nonce {
			t.Fatalf("fixture: the unresolved withdrawal must be OWED under A's entitlement, got %+v", item)
		}
		if !verdict {
			t.Fatal("fixture: the cancellation's verdict must be staged")
		}

		// The same-object reconnect: a new tenure serves, and a new connection is accepted on the
		// SAME session object.
		testRewriteAuthority(t, f.e, func(a *mqttOutboundAuthority) { a.Tenure, a.OwnerTxn = mqttNewInflightNonce(), a.Txn })
		w.c2 = f.e.s.createInternalAccountClient()
		testAcceptRigConnection(t, f.e.asm, f.e.sess, w.c2)
		w.ent2, _ = w.c2.outboundEntitlement()
		if w.ent2 == w.ent1 || w.ent2.isZero() {
			t.Fatalf("fixture: the reconnect must serve under a NEW tenure (%+v vs %+v)", w.ent2, w.ent1)
		}
		f.e.sess.mu.Lock()
		f.e.sess.c = w.c2
		f.e.sess.mu.Unlock()
		return w
	}
	staleRefused := func(w *world) bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		for _, err := range w.staleErrs {
			if errors.Is(err, errMQTTEntitlementEnded) || errors.Is(err, errMQTTNoDestroyLicence) {
				return true
			}
		}
		return false
	}
	owed := func(w *world) *mqttWithdrawOwed {
		w.f.e.sess.mu.Lock()
		defer w.f.e.sess.mu.Unlock()
		return w.f.e.sess.withdrawOwed[w.f.lin]
	}

	t.Run("successor-survives", func(t *testing.T) {
		t.Parallel()
		w := setup(t, "f088-t20-successor")
		f := w.f
		var successor atomic.Pointer[mqttWithdrawOwed]
		release := make(chan struct{})
		var once sync.Once
		w.onStale = func() {
			once.Do(func() {
				_, seq := f.register(t)
				f.e.sess.recordWithdrawOwed(f.gen, f.subject, f.nonce, f.lin, seq, mqttLicenceOf(w.c2))
				successor.Store(owed(w))
			})
		}
		var newReached atomic.Bool
		w.onNew = func() {
			newReached.Store(true)
			<-release
		}
		w.pass.Store(true)
		eventually(t, "the stale item's retry is refused at the boundary", func() bool { return staleRefused(w) })
		s := successor.Load()
		if s == nil || s.lic.ent != w.ent2 {
			t.Fatalf("fixture: the successor item must be recorded under the new licence, got %+v", s)
		}
		// The stale unit's disposition is decided either way: the successor's own unit runs (it
		// survived), or the successor item is gone (the stale unit retired it).
		eventually(t, "the stale unit's disposition is decided", func() bool { return newReached.Load() || owed(w) != s })
		// TARGET (M20).
		if cur := owed(w); cur != s {
			t.Fatalf("the stale unit must never retire the successor item: recorded %p, now %p", s, cur)
		}
		if !newReached.Load() {
			t.Fatal("the successor item's own unit must run")
		}
		if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
			t.Fatalf("nothing may be withdrawn under the stale licence, got %+v", x)
		}
		close(release)
		eventually(t, "the successor withdraws the obligation under the new licence", func() bool {
			x := f.exchange(t)
			return x != nil && x.Phase == mqttExchangeWithdrawnUnsent
		})
		eventually(t, "the landed successor item retires", func() bool { return owed(w) == nil })
		w.mu.Lock()
		for _, ent := range w.landedEnts {
			if ent != w.ent2 {
				w.mu.Unlock()
				t.Fatalf("a withdrawal landed under %+v, not the new licence", ent)
			}
		}
		w.mu.Unlock()
		testDriverP2(t, f.e.sess, f.e.s)
	})
	t.Run("connect-recovery", func(t *testing.T) {
		t.Parallel()
		w := setup(t, "f088-t20-recovery")
		f := w.f
		w.pass.Store(true)
		eventually(t, "the stale item's retry is refused at the boundary", func() bool { return staleRefused(w) })
		eventually(t, "the refused stale item is compare-deleted", func() bool { return owed(w) == nil })
		if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
			t.Fatalf("nothing may be withdrawn under the stale licence, got %+v", x)
		}
		if cur, _ := f.register(t); cur == nil || !f.markerStands() {
			t.Fatalf("the never-sent pair must still stand for the successor's recovery, got %+v", cur)
		}
		if _, err := f.e.sess.recoverInflightRegisters(w.c2); err != nil {
			t.Fatalf("the reconnect's recovery: %v", err)
		}
		eventually(t, "the reconnect's recovery withdraws the obligation", func() bool {
			x := f.exchange(t)
			return x != nil && x.Phase == mqttExchangeWithdrawnUnsent
		})
		eventually(t, "the withdrawn register is deleted", func() bool { cur, _ := f.register(t); return cur == nil })
		w.mu.Lock()
		landed := append([]mqttOutboundEntitlement(nil), w.landedEnts...)
		w.mu.Unlock()
		if len(landed) == 0 {
			t.Fatal("witness: no withdrawal landed through the seam")
		}
		for _, ent := range landed {
			if ent != w.ent2 {
				t.Fatalf("the recovery's withdrawal landed under %+v, not the new licence %+v", ent, w.ent2)
			}
		}
		testDriverP2(t, f.e.sess, f.e.s)
	})
}

// ---------------------------------------------------------------------------
// F-097 residual preservation (C2 requirement 5): a resume that leaves the identifier space
// INCOMPLETE stays conservative, and only the next complete resume decides the opaque register
// ---------------------------------------------------------------------------

// testF097ResumeOnPeer persists A's session record, restores it on peer B and runs B's recovery
// with B's BATCHED register/marker read routed through multi (nil: the real store). Unlike
// testF088ResumeOnPeer it does not require the resume to be complete: the residual tests build
// exactly the incomplete one. The resume's marker phase and its pre-apply reread both read
// through the batched seam (testInflightLoadMulti), never through the point-read marker seam.
func testF097ResumeOnPeer(t *testing.T, f *testWithdrawFix, node string,
	multi func(asm *mqttAccountSessionManager) func(subjects []string) ([]*JSApiMsgGetResponse, error)) *testF088Peer {
	t.Helper()
	if err := testSessSave(t, f.e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	asm, sess, c := f.e.peer(t, node, f.e.sess.id)
	p := &testF088Peer{asm: asm, sess: sess, c: c}
	if multi != nil {
		asm.mu.Lock()
		asm.testInflightLoadMulti = multi(asm)
		asm.mu.Unlock()
	}
	if _, err := sess.recoverInflightRegisters(c); err != nil {
		t.Fatalf("B's recovery: %v", err)
	}
	sess.mu.Lock()
	sess.c = c
	sess.mu.Unlock()
	return p
}

// testF097OpaqueMarkerSeam answers B's batched read of q's unsent marker with a marker naming
// ANOTHER allocation (every other subject reads the store as it stands): the marker phase then
// cannot read q's marker as q's or as absent, and recovery reserves q MARKER-OPAQUE. patched
// records that the seam actually served q's marker (the route witness).
func testF097OpaqueMarkerSeam(t *testing.T, regSubject string, patched *atomic.Bool) func(asm *mqttAccountSessionManager) func([]string) ([]*JSApiMsgGetResponse, error) {
	t.Helper()
	markerSubject := regSubject + "." + mqttInflightMarkerSuffix
	foreign, ferr := mqttBuildInflightMarker(mqttNewInflightNonce())
	if ferr != nil {
		t.Fatalf("fixture: building a foreign marker body: %v", ferr)
	}
	return func(asm *mqttAccountSessionManager) func([]string) ([]*JSApiMsgGetResponse, error) {
		return func(subjects []string) ([]*JSApiMsgGetResponse, error) {
			resps, err := asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
			if err != nil {
				return resps, err
			}
			for i, s := range subjects {
				if s != markerSubject || i >= len(resps) || resps[i] == nil || resps[i].Message == nil {
					continue
				}
				cp := *resps[i]
				m := *cp.Message
				m.Data = foreign
				cp.Message = &m
				resps[i] = &cp
				patched.Store(true)
			}
			return resps, nil
		}
	}
}

// requireStaysConservative is T27's common half: with the seam gone, a SUCCESSFUL point read of
// q's marker and a FULL dispatcher pass leave piSpaceIncomplete set and allocation withheld (WP4
// never clears it: only the next complete resume does), and the connection is still open (no
// server DISCONNECT was produced by any of it).
func (p *testF088Peer) requireStaysConservative(t *testing.T, subject string) {
	t.Helper()
	p.asm.mu.Lock()
	p.asm.testInflightLoadMulti = nil
	p.asm.mu.Unlock()
	if load := p.asm.loadInflightMarker(mqttMarkerCallerEvidence, subject); load.kind == mqttInflightResponseIndeterminate {
		t.Fatalf("fixture: the point read of q's marker must succeed, got %+v", load)
	}
	rec := testDriverInstall(p.sess)
	rec.mu.Lock()
	before := rec.iterations
	rec.mu.Unlock()
	testCustodyKick(p.sess)
	testDriverWait(t, "a full dispatcher pass runs", testCustodyBound, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.iterations > before
	})
	testCustodyQuiet(t, p.sess)
	// REACH, made visible: which dispatcher unit kinds the pass ran (M34 needs a unit that reads
	// q's marker; a pass with none cannot tell).
	rec.mu.Lock()
	kinds := make([]mqttCustodyUnitKind, 0, len(rec.units))
	for _, u := range rec.units {
		kinds = append(kinds, u.kind)
	}
	iterations := rec.iterations
	rec.mu.Unlock()
	t.Logf("observed dispatcher pass: %d iteration(s) since the kick, unit kinds %v", iterations-before, kinds)
	p.sess.mu.Lock()
	incomplete := p.sess.piSpaceIncomplete
	allocated := p.sess.bumpPI()
	p.sess.mu.Unlock()
	if !incomplete {
		t.Fatal("a point read and a dispatcher pass must leave piSpaceIncomplete set: only a complete resume clears it")
	}
	if allocated != 0 {
		t.Fatalf("allocation must stay withheld while the space is incomplete, bumpPI gave %d", allocated)
	}
	if p.c.isClosed() {
		t.Fatal("the incomplete recovery must produce no server DISCONNECT")
	}
}

// T27. THE TWO POPULATIONS NOTHING ON THIS CONNECTION MAY ANSWER (WP2 owner table), each resumed
// cold on peer B with the identifier space left incomplete:
//   - `opaque`: a never-sent obligation q whose unsent marker B cannot read as q's (seam: the
//     batched marker read answers a foreign allocation's marker). Recovery reserves q
//     marker-opaque; the client's PUBACK for q is DECLINED (HEAD behaviour: whether the PUBLISH
//     was sent is unknown) and the reservation, the register and the exchange are kept.
//   - `reread`: a TRANSMITTED obligation q (marker consumed), whose pre-apply reread of its own
//     register fails as a batch (seam: the second batched read naming q's register). Recovery
//     installs NO entry for q; the PUBACK reaches no entry and no binding and is DROPPED.
//
// In both, a successful point read of q's marker and a full dispatcher pass leave
// piSpaceIncomplete set (requireStaysConservative), and nothing disconnects. Backs T13's
// recovery-reservation removal class (f088_entry_removal_census_test.go).
//
// Mutation M34 (a dispatcher unit clears piSpaceIncomplete after a successful point read): the
// flag is cleared by the pass -> fails.
func TestF097OpaqueAndRereadFailureStayConservative(t *testing.T) {
	t.Parallel()
	t.Run("opaque", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "f097-t27-opaque", 1)
		var patched atomic.Bool
		p := testF097ResumeOnPeer(t, f, "node2-t27-opaque", testF097OpaqueMarkerSeam(t, f.subject, &patched))
		if !patched.Load() {
			t.Fatal("witness: B's marker phase never read q's marker through the seam")
		}
		p.sess.mu.Lock()
		ent := p.sess.pendingPublish[f.pi]
		incomplete := p.sess.piSpaceIncomplete
		p.sess.mu.Unlock()
		if !incomplete {
			t.Fatal("fixture: a marker-opaque register must leave B's identifier space incomplete")
		}
		if ent == nil || ent.acts() || !ent.markerOpaque {
			t.Fatalf("fixture: B must hold q as a non-acting marker-opaque reservation, got %+v", ent)
		}
		_, seqBefore := f.register(t)
		if err := p.c.mqttProcessPubAck(f.pi); err != nil {
			t.Fatalf("the declined PUBACK must not be an error (an error closes the connection): %v", err)
		}
		if got := p.entry(f.pi); got != ent {
			t.Fatalf("the declined PUBACK must keep the reservation, got %+v", got)
		}
		testCustodyStaysFor(t, "the declined PUBACK advances nothing", 200*time.Millisecond, func() bool {
			x := f.exchange(t)
			cur, seq := f.register(t)
			return x != nil && x.Phase == mqttExchangePublish && cur != nil && seq == seqBefore
		})
		p.requireStaysConservative(t, f.subject)
		if got := p.entry(f.pi); got == nil || got.acts() {
			t.Fatalf("the reservation must still stand after the pass, got %+v", got)
		}
	})
	t.Run("reread", func(t *testing.T) {
		t.Parallel()
		f := testLateAckTransmitted(t, "f097-t27-reread", 1)
		if f.markerStands() {
			t.Fatal("fixture: the transmission must have consumed q's marker (the reread is asked only then)")
		}
		var reads atomic.Int32
		var failed atomic.Bool
		p := testF097ResumeOnPeer(t, f.testWithdrawFix, "node2-t27-reread",
			func(asm *mqttAccountSessionManager) func([]string) ([]*JSApiMsgGetResponse, error) {
				return func(subjects []string) ([]*JSApiMsgGetResponse, error) {
					names := false
					for _, s := range subjects {
						names = names || s == f.subject
					}
					// The first batch naming q's register is the walk's own read; the second is the
					// pre-apply reread of a register whose marker is absent.
					if names && reads.Add(1) == 2 {
						failed.Store(true)
						return nil, errors.New("test: the pre-apply reread failed")
					}
					return asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
				}
			})
		if !failed.Load() {
			t.Fatalf("witness: the pre-apply reread was never reached (%d batched reads of q's register)", reads.Load())
		}
		p.sess.mu.Lock()
		ent := p.sess.pendingPublish[f.pi]
		incomplete := p.sess.piSpaceIncomplete
		p.sess.mu.Unlock()
		if !incomplete {
			t.Fatal("fixture: a failed reread must leave B's identifier space incomplete")
		}
		if ent != nil {
			t.Fatalf("a failed reread installs no entry for q, got %+v", ent)
		}
		_, seqBefore := f.register(t)
		if err := p.c.mqttProcessPubAck(f.pi); err != nil {
			t.Fatalf("the dropped PUBACK must not be an error (an error closes the connection): %v", err)
		}
		testCustodyStaysFor(t, "the dropped PUBACK advances nothing", 200*time.Millisecond, func() bool {
			x := f.exchange(t)
			cur, seq := f.register(t)
			return x != nil && x.Phase == mqttExchangePublish && cur != nil && seq == seqBefore
		})
		if got := p.entry(f.pi); got != nil {
			t.Fatalf("no binding or entry may be invented for q by its PUBACK, got %+v", got)
		}
		p.requireStaysConservative(t, f.subject)
	})
}

// ---------------------------------------------------------------------------
// WP3 edit 7: the admission classifier probes a CANCELLED occupant's marker before it classifies
// ---------------------------------------------------------------------------

// testF088CancelledOccupant is T17's inherited same-delivery occupant on identifier 11, carrying an
// OCCUPANT cancellation. INPUT, STATED: the occupant is seeded (an admission batch landed directly,
// its cancellation then written at the raw register boundary as the copy-and-CAS fallback writes
// it); markerAbsent consumes its unsent marker first (possibly attempted). The next allocation of
// the same delivery is tracked onto identifier 11 and returned with its nonce.
type testF088CancelledOcc struct {
	e       *testInflightEnv
	dur     string
	sseq    uint64
	subject string
	occ     *mqttInflightRecord // the cancelled occupant as stored
	occSeq  uint64
	pi      uint16
	nonce   string
	probes  atomic.Int32 // point reads of identifier 11's unsent marker (the classifier's probe)
	deletes atomic.Int32 // exact-sequence register deletes
	mu      sync.Mutex
	events  []string
}

func (o *testF088CancelledOcc) note(ev string) {
	o.mu.Lock()
	o.events = append(o.events, ev)
	o.mu.Unlock()
}

func (o *testF088CancelledOcc) log() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.events...)
}

func testF088CancelledOccupantOn(t *testing.T, e *testInflightEnv, dur string, sseq uint64, qos byte, landed *mqttInflightRecord,
	markerAbsent bool) *testF088CancelledOcc {
	t.Helper()
	o := &testF088CancelledOcc{e: e, dur: dur, sseq: sseq, subject: mqttInflightSubject(e.sess.idHash, e.sess.gen, 11)}
	if markerAbsent {
		ml := e.asm.jsa.loadOccupantMarkerLoad(o.subject)
		if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, o.subject, ml.seq, true); err != nil {
			t.Fatalf("fixture: consuming the occupant's marker: %v", err)
		}
	}
	_, smsg := e.loadRegister(t, 11)
	tomb := *landed
	tomb.Cancelled, tomb.CancelOrigin = true, mqttCancelOriginOccupant
	if _, err := e.asm.storeInflightCAS(o.subject, smsg.Sequence, &tomb); err != nil {
		t.Fatalf("fixture: writing the occupant cancellation: %v", err)
	}
	cur, cmsg := e.loadRegister(t, 11)
	if cur == nil || !cur.Cancelled || cur.CancelOrigin != mqttCancelOriginOccupant || cur.Retired {
		t.Fatalf("fixture: the occupant must carry an occupant cancellation, got %+v", cur)
	}
	o.occ, o.occSeq = cur, cmsg.Sequence
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 2))
	e.sess.mu.Unlock()
	if pi != 11 || !isNew {
		t.Fatalf("fixture: the next allocation must take the occupied identifier 11, got %d (new %v)", pi, isNew)
	}
	o.pi, o.nonce = pi, e.nonceOf(pi)
	markerSubject := o.subject + "." + mqttInflightMarkerSuffix
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(seq uint64) error {
		o.deletes.Add(1)
		o.note("delete:" + itoa(int(seq)))
		return e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
	}
	e.asm.testInflightMarkerLoad = func(subject string) (*StoredMsg, error) {
		if subject == markerSubject {
			o.probes.Add(1)
			// Class tag: the occupant the probe is asked about, read beside it.
			tag := "probe"
			if r, _, _ := e.asm.jsa.loadInflightRecord(o.subject); r != nil && r.Cancelled {
				if mqttDurIsSlotOnlyClass(r.Dur) {
					tag = "probe(slot-only cancelled)"
				} else {
					tag = "probe(ordinary cancelled)"
				}
			}
			o.note(tag)
		}
		return e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	}
	e.asm.mu.Unlock()
	return o
}

// testF088OrdinaryCancelledOccupant seeds the ordinary occupant exactly as T17 does.
func testF088OrdinaryCancelledOccupant(t *testing.T, clientID string, markerAbsent bool) *testF088CancelledOcc {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	e.sess.mu.Lock()
	e.sess.last_pi = 10
	e.sess.mu.Unlock()
	occ := e.landAdmission(t, 11, 1, dur, sseq, mqttNewInflightNonce())
	return testF088CancelledOccupantOn(t, e, dur, sseq, 1, occ, markerAbsent)
}

// T22 (WP3 edit 7). A same-delivery occupant carrying an OCCUPANT cancellation whose marker is
// AUTHORITATIVELY ABSENT (possibly attempted): the ladder probes the marker BEFORE classifying
// (witness: one point read of the occupant's marker, reused by the replacement arm, before the
// register-only write), the refuted cancellation falls through to mqttSlotDispReplace, and the
// register-only replacement lands over it with BOTH Cancelled and CancelOrigin cleared (revision
// 12's explicit authorization), inheriting the occupant's lineage. Settlement confirms the entry
// transmitted, so the next section carries DUP=1.
//
// Subtest `pending-fence`: the same register-only replacement over a PENDING FENCE predecessor
// (`pending-create`, written by the real producer) is refused at the boundary, by the fence rule.
//
// Mutation M26 (the pre-classification probe removed): the classifier's cancelMarker stays at its
// fail-closed zero value -> abandon, no register-only replacement lands -> fails.
func TestF088MarkerlessCancelledOccupantIsReplacedRegisterOnly(t *testing.T) {
	t.Parallel()
	t.Run("occupant", func(t *testing.T) {
		t.Parallel()
		o := testF088OrdinaryCancelledOccupant(t, "f088-t22", true)
		e := o.e
		var registerOnly atomic.Bool
		var probesAtWrite atomic.Int32
		probesAtWrite.Store(-1)
		e.asm.mu.Lock()
		e.asm.testAdmitWrite = func(s *mqttSession, ro bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if s == e.sess && ro {
				probesAtWrite.CompareAndSwap(-1, o.probes.Load())
			}
			out, err := write()
			if s == e.sess && ro && err == nil {
				registerOnly.Store(true)
			}
			return out, err
		}
		e.asm.mu.Unlock()
		// Non-fatal: the admission's verdict follows from the replacement below, which is the target
		// (M26).
		if !e.sess.admitInflightPublish(e.c, o.pi, 1, o.dur, o.sseq, o.nonce, _EMPTY_, _EMPTY_) {
			t.Errorf("the allocation over a refuted cancellation must be admitted (events %v)", o.log())
		}
		if !registerOnly.Load() {
			t.Fatalf("the register-only replacement must land over the refuted cancellation (events %v)", o.log())
		}
		if n := probesAtWrite.Load(); n != 1 {
			t.Fatalf("the occupant's marker must be probed exactly once before the register-only write "+
				"(the pre-classification probe, reused by the replacement arm), got %d (events %v)", n, o.log())
		}
		rec, _ := e.loadRegister(t, 11)
		if rec == nil || rec.Nonce != o.nonce || rec.Lin != o.occ.Lin || rec.State != mqttInflightPublish || rec.QoS != o.occ.QoS {
			t.Fatalf("the replacement must carry our nonce and the occupant's lineage %q at publish, got %+v", o.occ.Lin, rec)
		}
		if rec.Cancelled || rec.CancelOrigin != _EMPTY_ {
			t.Fatalf("the replacement must clear BOTH Cancelled and CancelOrigin, got %+v", rec)
		}
		if present, transmitted, opaque, markerSeq := e.entryState(11); !present || !transmitted || opaque || markerSeq != 0 {
			t.Fatalf("settlement must confirm the entry TRANSMITTED (present=%v transmitted=%v opaque=%v marker=%d)",
				present, transmitted, opaque, markerSeq)
		}
		if tok := e.openSection(t, 11, o.dur, o.sseq); !tok.dup {
			t.Fatalf("the continued exchange's next transmission must carry DUP=1, got %+v", tok)
		}
		x, _, err := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, o.occ.Lin)
		if err != nil || x == nil || x.Phase != mqttExchangePublish {
			t.Fatalf("the continued exchange must stay at Publish, got %+v (%v)", x, err)
		}
		if n := o.deletes.Load(); n != 0 {
			t.Fatalf("nothing may be destroyed, but %d register delete(s) were issued", n)
		}
	})
	t.Run("pending-fence", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "f088-t22-fence")
		e.acceptUnderServingAuthority(t)
		ent, _ := e.c.outboundEntitlement()
		dur := e.sess.idHash + "_" + nuid.Next()
		e.sess.mu.Lock()
		pi := e.sess.bumpPI()
		e.sess.mu.Unlock()
		fence, fseq := testF088WriteFence(t, e, pi, dur, 6, mqttNewInflightNonce())
		nonce := mqttNewInflightNonce()
		lin, _ := mqttLineageForWrite(mqttLineageWriteRegisterOnly, fence, nonce)
		repl := &mqttInflightRecord{State: mqttInflightPublish, PI: pi, Dur: fence.Dur, Seq: fence.Seq, QoS: fence.QoS,
			Nonce: nonce, Lin: lin, Claim: fence.Claim, ClaimSeq: fence.ClaimSeq, SourceAckReply: fence.SourceAckReply}
		_, err := e.asm.jsa.opReplacePublish(&mqttReplacePublishRequest{
			ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-replace-publish",
			Gen: ent.Gen, Tenure: ent.Tenure, RegisterOnly: true, Occupant: fence, OccupantSeq: fseq, Register: repl,
		})
		if err == nil || !strings.Contains(err.Error(), "pending cancellation fence") {
			t.Fatalf("a register-only replacement over a pending fence must be refused by the fence rule, got %v", err)
		}
		if rec, smsg := e.loadRegister(t, pi); rec == nil || smsg.Sequence != fseq ||
			rec.CancelOrigin != mqttCancelOriginPendingCreate {
			t.Fatalf("the fence must stand untouched at %d, got %+v", fseq, rec)
		}
	})
}

// T22b (WP3 edit 7). The same cancelled occupant, but its marker cannot be read (every point read
// of it fails): nothing is established, so the allocation ABANDONS with the identifier excluded,
// and the occupant is neither reaped nor replaced.
func TestF088CancelledOccupantMarkerIndeterminateAbandons(t *testing.T) {
	t.Parallel()
	o := testF088OrdinaryCancelledOccupant(t, "f088-t22b", false)
	e := o.e
	markerSubject := o.subject + "." + mqttInflightMarkerSuffix
	var anyReplacement atomic.Bool
	e.asm.mu.Lock()
	e.asm.testInflightMarkerLoad = func(subject string) (*StoredMsg, error) {
		if subject == markerSubject {
			o.probes.Add(1)
			return nil, errors.New("test: the marker read did not resolve")
		}
		return e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	}
	e.asm.testAdmitWrite = func(s *mqttSession, ro bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if s == e.sess && ro {
			anyReplacement.Store(true)
		}
		return write()
	}
	e.asm.mu.Unlock()
	if e.sess.admitInflightPublish(e.c, o.pi, 1, o.dur, o.sseq, o.nonce, _EMPTY_, _EMPTY_) {
		t.Fatal("an allocation over a cancelled occupant whose marker is indeterminate must not be admitted")
	}
	if o.probes.Load() == 0 {
		t.Fatal("witness: the classifier never probed the cancelled occupant's marker")
	}
	if anyReplacement.Load() {
		t.Fatal("no replacement may be attempted over an undecided cancelled occupant")
	}
	if n := o.deletes.Load(); n != 0 {
		t.Fatalf("the occupant must not be reaped, but %d register delete(s) were issued", n)
	}
	rec, smsg := e.loadRegister(t, 11)
	if rec == nil || smsg == nil || smsg.Sequence != o.occSeq || rec.Nonce != o.occ.Nonce || !rec.Cancelled {
		t.Fatalf("the occupant must be preserved whole at %d, got %+v", o.occSeq, rec)
	}
	e.sess.mu.Lock()
	ent := e.sess.pendingPublish[11]
	inst := e.sess.slotExclusionInstLocked(11)
	e.sess.mu.Unlock()
	if ent != nil {
		t.Fatalf("the allocation must be abandoned (no entry left), got %+v", ent)
	}
	if inst == 0 {
		t.Fatal("the abandoned identifier must stay excluded")
	}
}

// T22c (WP3 edit 7, all classes: C4 section 4). An EXPERIMENTAL AFFINE (slot-only) occupant with
// an occupant cancellation and its unsent marker STANDING: the pre-classification probe runs for
// this class too (witness, class-tagged, before any delete), the genuine cancellation is reaped
// through the door at its exact sequence, and the create is retried and lands this allocation's
// pair.
//
// INPUT, STATED: the affine occupant pair is seeded through the slot-only pair writer and its
// cancellation written at the raw register boundary (no in-memory entry for it).
func TestF088AffineCancelledOccupantMarkerBackedCleanup(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "f088-t22c")
	affDur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_" + nuid.Next()[:6]
	const sseq = 9
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, 11)
	occ := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 11, Dur: affDur, Seq: sseq, QoS: 2,
		Nonce: mqttNewInflightNonce()})
	if _, err := e.asm.storeInflightPair(subject, 0, 0, occ); err != nil {
		t.Fatalf("fixture: seeding the affine occupant pair: %v", err)
	}
	if m, _ := e.loadMarker(t, 11); m == nil || m.Nonce != occ.Nonce {
		t.Fatalf("fixture: the occupant's marker must stand, got %+v", m)
	}
	e.sess.mu.Lock()
	e.sess.last_pi = 10
	e.sess.mu.Unlock()
	o := testF088CancelledOccupantOn(t, e, affDur, sseq, 2, occ, false)
	if !mqttDurIsSlotOnlyClass(o.occ.Dur) {
		t.Fatalf("fixture: the occupant must be slot-only, got %+v", o.occ)
	}
	if !e.sess.storeInflightRegister(e.c, o.pi, 2, affDur, sseq, o.nonce, _EMPTY_, _EMPTY_) {
		t.Fatalf("the create must be retried and land after the marker-backed cleanup (events %v)", o.log())
	}
	events := o.log()
	firstProbe, firstDelete := -1, -1
	for i, ev := range events {
		if firstProbe < 0 && ev == "probe(slot-only cancelled)" {
			firstProbe = i
		}
		if firstDelete < 0 && ev == "delete:"+itoa(int(o.occSeq)) {
			firstDelete = i
		}
	}
	if firstProbe < 0 {
		t.Fatalf("witness: the pre-classification probe never ran for the slot-only cancelled occupant (events %v)", events)
	}
	if firstDelete < 0 || firstDelete < firstProbe {
		t.Fatalf("the occupant must be reaped at its exact sequence %d after the probe (events %v)", o.occSeq, events)
	}
	rec, _ := e.loadRegister(t, 11)
	if rec == nil || rec.Nonce != o.nonce || rec.Cancelled || !mqttDurIsSlotOnlyClass(rec.Dur) {
		t.Fatalf("the retried create must land this allocation's register, got %+v", rec)
	}
	if m, _ := e.loadMarker(t, 11); m == nil || m.Nonce != o.nonce {
		t.Fatalf("the retried create must land this allocation's marker, got %+v", m)
	}
	t.Logf("observed events: %v", events)
}

// T1 (F-097 takeover stranding, memory half). The client PUBACKs L on broker A; A's in-call
// acknowledgement advances all fail and its terminal retry is held (barrier). The client resumes on peer
// B, which takes a NEW tenure over the same generation: B's recovery restores L, its ledger holds
// L's charge, and at Receive Maximum 1 B withholds a further delivery (the demand latch). After a B
// dispatcher unit has READ L at Publish (the barrier's witness), A's retry is released and lands
// (standing authority finishes an admitted obligation), with B's register still at A's acquisition.
// TARGET: B's ledger drops L and releases its charge - durable AND memory release - with no
// further offer, publish, PUBACK or resume, before AckWait; the withheld delivery is then admitted.
//
// IN-PROCESS RIG, STATED: two session managers sharing one JetStream (e.peer), the tenure takeover
// written at the authority record, the PUBACK delivered to A's handler and "the deferred delivery
// leaves" represented by B's trackPublish admitting it. No real client: the client-visible half is
// not on the wire.
//
// Mutation M12 (B's demand latch cleared by a Publish read): B never re-reads L after A lands, so
// its charge waits for AckWait -> fails.
func TestF097TakeoverFailedAdvanceReleasesOnThePeerWithoutTraffic(t *testing.T) {
	t.Parallel()
	f := testLateAckTransmitted(t, "f097-t1", 1)
	cur, _ := f.register(t)
	if cur == nil {
		t.Fatal("fixture: L's register must stand")
	}
	k := cur.Acq
	var advances atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseA := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseA)
	var landedAcq atomic.Int64
	landedAcq.Store(-1)
	var advErr atomic.Value
	f.e.asm.mu.Lock()
	f.e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		// A's acknowledgement re-attempts a contended advance within its own call (D3), so EVERY
		// in-call attempt fails: only then is the advance A's terminal retry, which is held.
		if advances.Add(1) <= mqttForegroundAcquireAttempts {
			return mqttOpOutcome{}, errors.New("test: A's in-call acknowledgement advances fail")
		}
		<-release
		if r, _, lerr := f.e.asm.jsa.loadInflightRecord(f.subject); lerr == nil && r != nil {
			landedAcq.Store(int64(r.Acq))
		}
		out, err := write()
		if err != nil {
			advErr.Store(err.Error())
		}
		return out, err
	}
	f.e.asm.mu.Unlock()
	if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK on A: %v", err)
	}
	if advances.Load() == 0 {
		t.Fatal("fixture: A's acknowledgement advance never ran")
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("fixture: A's failed advance must leave L at Publish, got %+v", x)
	}

	// THE TAKEOVER: B takes a new tenure over the same generation and resumes cold.
	if err := testSessSave(t, f.e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	testRewriteAuthority(t, f.e, func(a *mqttOutboundAuthority) { a.Tenure, a.OwnerTxn = mqttNewInflightNonce(), a.Txn })
	_, peer, pc := f.e.peer(t, "node2", f.e.sess.id)
	if _, err := peer.recoverInflightRegisters(pc); err != nil {
		t.Fatalf("B's recovery: %v", err)
	}
	peer.mu.Lock()
	peer.c = pc
	incomplete := peer.piSpaceIncomplete
	peer.mu.Unlock()
	if incomplete {
		t.Fatal("fixture: B's resume must be COMPLETE")
	}
	if !testCustodyHasExchange(peer, f.lin) {
		t.Fatal("fixture: B's ledger must hold L's charge after its recovery")
	}
	var umu sync.Mutex
	var lUnits int
	peer.mu.Lock()
	peer.testCustodyDriver = &mqttCustodyDriverSeams{unit: func(gen string, u mqttCustodyUnit) {
		if u.kind == mqttCustodyUnitTarget && u.target.kind == mqttCustodyTargetExchange && u.target.key == f.lin {
			umu.Lock()
			lUnits++
			umu.Unlock()
		}
	}}
	peer.mu.Unlock()
	testCustodyRM1(t, peer)
	bReply := testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq+1, 1)
	peer.mu.Lock()
	bPI, _ := peer.trackPublish(testRealPubRelSrcDur, bReply)
	peer.mu.Unlock()
	if bPI != 0 {
		t.Fatalf("fixture: B must withhold a further delivery while L holds the only slot, got identifier %d", bPI)
	}
	// BARRIER: a B dispatcher unit has read L - at Publish, its charge still held - and the run is
	// idle again, BEFORE A's retry may land.
	testDriverWait(t, "a B dispatcher unit reads L at Publish", 10*time.Second, func() bool {
		umu.Lock()
		n := lUnits
		umu.Unlock()
		peer.mu.Lock()
		idle := !peer.custodyObserving
		peer.mu.Unlock()
		return n > 0 && idle
	})
	if !testCustodyHasExchange(peer, f.lin) || testCustodyCapacity(peer) != 0 {
		t.Fatal("witness: B's read of L must find it at Publish, charge held")
	}
	releaseA()
	testDriverWait(t, "A's retried advance lands L Terminal", 15*time.Second, func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
	if e, _ := advErr.Load().(string); e != _EMPTY_ {
		t.Fatalf("A's retried advance reported %s", e)
	}
	if got := landedAcq.Load(); got != int64(k) {
		t.Fatalf("witness: B's register must still be at A's acquisition %d when A lands, got %d", k, got)
	}
	start := time.Now()
	// TARGET (M12): B's ledger drops L and releases its charge, with no further offer, publish,
	// acknowledgement or resume, before AckWait (30 s default on B).
	released := false
	for deadline := start.Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if !testCustodyHasExchange(peer, f.lin) && testCustodyCapacity(peer) == 1 {
			released = true
			break
		}
	}
	umu.Lock()
	n := lUnits
	umu.Unlock()
	if !released {
		t.Fatalf("not within 15s: B's ledger drops L and releases its charge without traffic (B units for L: %d)", n)
	}
	t.Logf("B released L's charge %v after A's landing (B units for L: %d)", time.Since(start), n)
	peer.mu.Lock()
	bPI, bNew := peer.trackPublish(testRealPubRelSrcDur, bReply)
	peer.mu.Unlock()
	if bPI == 0 || !bNew {
		t.Fatalf("the withheld delivery must be admitted once L's charge is released, got %d (new=%v)", bPI, bNew)
	}
}

// T10 (live-token arm). Broker A's writer is blocked INSIDE the socket write of L's PUBLISH,
// holding wmu, with L's output token on the register. Certification of A's connection cannot
// complete (it crosses the writer barrier, and this transport ignores the close certification
// performs first). The client resumes on peer B under a NEW tenure: after B's pre-pass (which
// clears only CERTIFIED holders' tokens) the token is still present, and the client's PUBACK on
// B must leave L at Publish while the token stands. A's write is then released, A's holder
// drains, and B's retry ends L Terminal.
//
// IN-PROCESS RIG, STATED: A's delivery runs through the real callback (mqttDeliverMsgCbQoS12) on
// the env's connection with a recording transport whose Write blocks (onWrite) and whose Close
// does not unblock it; B is e.peer under a rewritten tenure.
//
// Mutation M10 (combined: the terminal release ignores the token AND the advance builder's token
// refusal removed): B's advance lands under the live token -> "Publish while the token stands"
// fails.
func TestF088LateAckWaitsForAnUncertifiedHoldersDrain(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "f088-t10")
	testReadyForRealPubRelDelivery(t, e)
	e.admissionPrerequisites(t, testRealPubRelSrcDur, 1)
	e.acceptUnderServingAuthority(t)
	wire := testRow2Wire(e)
	inWrite, unblock := make(chan struct{}), make(chan struct{})
	var once, unblockOnce sync.Once
	releaseWriter := func() { unblockOnce.Do(func() { close(unblock) }) }
	t.Cleanup(releaseWriter)
	e.c.mu.Lock()
	wire.onWrite = func() {
		once.Do(func() {
			close(inWrite)
			<-unblock // inside the socket write, wmu held; a Close does not end it
		})
	}
	e.c.mu.Unlock()
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.rmax = 5
	e.sess.mu.Unlock()
	reply := testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 1)
	hdr := []byte("NATS/1.0\r\n" + mqttNatsHeader + ":1\r\n\r\n")
	rmsg := append(append([]byte(nil), hdr...), "t10-payload"...)
	pcA := &client{kind: JETSTREAM}
	pcA.pa.hdr = len(hdr)
	sub := &subscription{client: e.c, mqtt: &mqttSub{qos: 1, jsDur: testRealPubRelSrcDur}}
	cbDone := make(chan struct{})
	go func() {
		defer close(cbDone)
		mqttDeliverMsgCbQoS12(sub, pcA, nil, mqttStreamSubjectPrefix+"f092.source", reply, rmsg)
	}()
	select {
	case <-inWrite:
	case <-cbDone:
		t.Fatal("fixture: A's delivery returned without reaching the socket write")
	case <-time.After(10 * time.Second):
		t.Fatal("fixture: A's delivery never reached the socket write")
	}
	e.sess.mu.Lock()
	pi := e.sess.cpending[testRealPubRelSrcDur][testRealPubRelSrcSeq]
	gen := e.sess.gen
	e.sess.mu.Unlock()
	if pi == 0 {
		t.Fatal("fixture: A's delivery must be tracked")
	}
	subject := mqttInflightSubject(e.sess.idHash, gen, pi)
	loadRegisterL := func() *mqttInflightRecord {
		r, _, err := e.asm.jsa.loadInflightRecord(subject)
		if err != nil {
			t.Fatalf("loading L's register: %v", err)
		}
		return r
	}
	l := loadRegisterL()
	if l == nil || l.ActionToken == _EMPTY_ {
		t.Fatalf("fixture: L's register must carry A's live output token while the write is held, got %+v", l)
	}
	lin, token := l.Lin, l.ActionToken
	exchange := func() *mqttOutboundExchange {
		x, _, err := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, gen, lin)
		if err != nil {
			t.Fatalf("loading L's exchange: %v", err)
		}
		return x
	}

	// Certification of A's connection cannot complete: it crosses the writer barrier.
	certDone := make(chan *mqttCloseCert, 1)
	go func() { certDone <- mqttCertifyClosed(e.c, e.c.outBind, "test: the client resumed elsewhere") }()
	select {
	case <-certDone:
		releaseWriter()
		t.Fatal("certification of A's connection completed while its writer was inside the socket write")
	case <-time.After(300 * time.Millisecond):
	}

	// THE TAKEOVER: B under a new tenure, cold recovery.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	testRewriteAuthority(t, e, func(a *mqttOutboundAuthority) { a.Tenure, a.OwnerTxn = mqttNewInflightNonce(), a.Txn })
	_, peer, pc := e.peer(t, "node2", e.sess.id)
	if _, err := peer.recoverInflightRegisters(pc); err != nil {
		t.Fatalf("B's recovery: %v", err)
	}
	peer.mu.Lock()
	peer.c = pc
	pent := peer.pendingPublish[pi]
	peer.mu.Unlock()
	if r := loadRegisterL(); r == nil || r.ActionToken != token {
		t.Fatalf("witness: A's uncertified token must survive B's pre-pass, got %+v", r)
	}
	if pent == nil || !pent.acts() {
		t.Fatalf("witness: B must restore L ACTING (otherwise the PUBACK is declined, not refused on the token), got %+v", pent)
	}
	var bAdvances atomic.Int32
	var bErrs []string
	var bmu sync.Mutex
	peer.asm.mu.Lock()
	peer.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		bAdvances.Add(1)
		out, err := write()
		if err != nil {
			bmu.Lock()
			bErrs = append(bErrs, err.Error())
			bmu.Unlock()
		}
		return out, err
	}
	peer.asm.mu.Unlock()
	defer func() {
		bmu.Lock()
		defer bmu.Unlock()
		t.Logf("B's advance attempts: %d, refusals: %q", bAdvances.Load(), bErrs)
	}()
	if err := pc.mqttProcessPubAck(pi); err != nil {
		t.Fatalf("PUBACK on B: %v", err)
	}
	peer.mu.Lock()
	retries := len(peer.terminalRetries)
	peer.mu.Unlock()
	// Witness: B took the acknowledgement into its owned completion path (the terminal retry is
	// registered), so what holds L at Publish below is the token refusal, not a decline.
	if retries == 0 {
		t.Fatal("witness: B's PUBACK must be owned by B's terminal retry")
	}
	// TARGET (M10): Publish while the token stands - across B's first attempt and a retry.
	testCustodyStaysFor(t, "L stays at Publish while A's uncertified token stands", 2500*time.Millisecond, func() bool {
		x := exchange()
		r := loadRegisterL()
		return x != nil && x.Phase == mqttExchangePublish && r != nil && r.ActionToken == token
	})
	releaseWriter()
	select {
	case <-cbDone:
	case <-time.After(10 * time.Second):
		t.Fatal("A's delivery did not return after its write was released")
	}
	testDriverWait(t, "A's holder drains its token", 10*time.Second, func() bool {
		r := loadRegisterL()
		return r == nil || r.ActionToken == _EMPTY_
	})
	testDriverWait(t, "B's retry ends L Terminal", 15*time.Second, func() bool {
		x := exchange()
		return x != nil && x.Phase == mqttExchangeTerminal
	})
	select {
	case <-certDone:
	case <-time.After(5 * time.Second):
		t.Fatal("certification of A's connection did not complete after its writer returned")
	}
}

// T26 (C2 requirement 5). A BOUND exchange completes while allocation stays closed. Peer B resumes
// cold with q (never sent) marker-opaque (seam), so B's identifier space is incomplete; a
// different exchange p (transmitted on A) is restored ACTING. The client PUBACKs p; its first
// acknowledgement advance fails (seam), and the terminal-retry worker carries it: p reaches
// Terminal and its custody charge is released with no further offer and no resume, while
// piSpaceIncomplete stays set and bumpPI allocates nothing throughout.
//
// Mutation M35 (the ACK handler or terminal attempt returns early while piSpaceIncomplete): p never
// reaches Terminal -> fails.
func TestF097BoundExchangeCompletesWhileAllocationStaysClosed(t *testing.T) {
	t.Parallel()
	pf := testWithdrawAdmitted(t, "f097-t26", 1)
	qf := testWithdrawAdmitMore(t, pf.e, testRealPubRelSrcSeq+1, 1)
	pf.e.bind()
	pf.e.transmit(t, pf.pi, testRealPubRelSrcDur, pf.sseq, true)
	if pf.markerStands() || !qf.markerStands() {
		t.Fatal("fixture: p must be transmitted (marker consumed) and q never sent (marker standing)")
	}
	var patched atomic.Bool
	p := testF097ResumeOnPeer(t, pf, "node2-t26", testF097OpaqueMarkerSeam(t, qf.subject, &patched))
	if !patched.Load() {
		t.Fatal("witness: B's marker phase never read q's marker through the seam")
	}
	closed := func() bool {
		p.sess.mu.Lock()
		defer p.sess.mu.Unlock()
		return p.sess.piSpaceIncomplete && p.sess.bumpPI() == 0
	}
	if !closed() {
		t.Fatal("fixture: q's marker-opaque reservation must leave B's allocation closed")
	}
	if q := p.entry(qf.pi); q == nil || q.acts() || !q.markerOpaque {
		t.Fatalf("fixture: q must be a marker-opaque reservation on B, got %+v", q)
	}
	if ent := p.entry(pf.pi); ent == nil || !ent.acts() || !ent.transmitted {
		t.Fatalf("fixture: p must be restored ACTING (transmitted) on B, got %+v", ent)
	}
	if !testCustodyHasExchange(p.sess, pf.lin) {
		t.Fatal("fixture: B's custody must hold p's exchange")
	}
	var attempts atomic.Int32
	p.asm.mu.Lock()
	p.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.Nonce == pf.lin && attempts.Add(1) == 1 {
			return mqttOpOutcome{}, errors.New("test: the first acknowledgement advance failed")
		}
		return write()
	}
	p.asm.mu.Unlock()
	if err := p.c.mqttProcessPubAck(pf.pi); err != nil {
		t.Fatalf("PUBACK of p on B: %v", err)
	}
	deadline := time.Now().Add(testCustodyBound)
	for {
		if !closed() {
			t.Fatal("allocation must stay closed throughout: only a complete resume reopens it")
		}
		x := pf.exchange(t)
		if x != nil && x.Phase == mqttExchangeTerminal && !testCustodyHasExchange(p.sess, pf.lin) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("p must reach Terminal and release its charge without another offer or resume "+
				"(exchange %+v, charged %v, advance attempts %d)", x, testCustodyHasExchange(p.sess, pf.lin), attempts.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if attempts.Load() < 2 {
		t.Fatalf("witness: the completion must have come through the retry after the failed advance, attempts %d",
			attempts.Load())
	}
	if !closed() {
		t.Fatal("piSpaceIncomplete must still be set after p completed")
	}
	if x := qf.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("q is untouched by p's completion, got %+v", x)
	}
	if p.c.isClosed() {
		t.Fatal("no server DISCONNECT")
	}
}

// testF097OpaqueQOnPeer is T27's `opaque` state: q never sent, resumed cold on peer B as a
// marker-opaque reservation (seam), the identifier space incomplete; the seam is then removed so
// the NEXT resume reads the store as it stands.
func testF097OpaqueQOnPeer(t *testing.T, clientID string, qos ...byte) (*testWithdrawFix, *testF088Peer) {
	t.Helper()
	q := byte(1)
	if len(qos) != 0 {
		q = qos[0]
	}
	f := testWithdrawAdmitted(t, clientID, q)
	var patched atomic.Bool
	p := testF097ResumeOnPeer(t, f, "node2-"+clientID, testF097OpaqueMarkerSeam(t, f.subject, &patched))
	p.sess.mu.Lock()
	ent := p.sess.pendingPublish[f.pi]
	incomplete := p.sess.piSpaceIncomplete
	p.sess.mu.Unlock()
	if !patched.Load() || !incomplete || ent == nil || ent.acts() || !ent.markerOpaque {
		t.Fatalf("fixture: q must be a marker-opaque reservation on an incomplete B (patched %v, incomplete %v, %+v)",
			patched.Load(), incomplete, ent)
	}
	p.asm.mu.Lock()
	p.asm.testInflightLoadMulti = nil
	p.asm.mu.Unlock()
	return f, p
}

// T28a. After T27's `opaque` state, q's marker is AUTHORITATIVELY ABSENT at the next resume (it was
// consumed: the PUBLISH may be on the wire). That resume - B's recovery re-run on the same session
// object, as a CONNECT on B runs it - completes: it restores q ACTING, its retransmission carries
// DUP=1, the client's PUBACK ends it (Terminal), piSpaceIncomplete is cleared by that recovery, and
// a fresh delivery allocates.
func TestF097NextCompleteResumeRetransmitsAMarkerAbsentRegister(t *testing.T) {
	t.Parallel()
	f, p := testF097OpaqueQOnPeer(t, "f097-t28a")
	ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
		t.Fatalf("fixture: consuming q's marker: %v", err)
	}
	if f.markerStands() {
		t.Fatal("fixture: q's marker must be absent")
	}
	if _, err := p.sess.recoverInflightRegisters(p.c); err != nil {
		t.Fatalf("the next resume's recovery: %v", err)
	}
	p.sess.mu.Lock()
	incomplete := p.sess.piSpaceIncomplete
	p.sess.mu.Unlock()
	if incomplete {
		t.Fatal("the next resume reads q whole and must clear piSpaceIncomplete")
	}
	ent := p.entry(f.pi)
	if ent == nil || !ent.acts() || !ent.transmitted || ent.markerOpaque {
		t.Fatalf("the next resume must restore q ACTING and transmitted, got %+v", ent)
	}
	pe := &testInflightEnv{s: p.c.srv, asm: p.asm, sess: p.sess, c: p.c}
	testConnIdentity(t, pe, p.c)
	if tok := pe.transmit(t, f.pi, testRealPubRelSrcDur, f.sseq, true); !tok.dup {
		t.Fatalf("q's retransmission must carry DUP=1, got %+v", tok)
	}
	if err := p.c.mqttProcessPubAck(f.pi); err != nil {
		t.Fatalf("PUBACK of q on B: %v", err)
	}
	testCustodyEventually(t, "the client's PUBACK ends q", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
	p.sess.mu.Lock()
	allocated := p.sess.bumpPI()
	p.sess.mu.Unlock()
	if allocated == 0 {
		t.Fatal("a fresh delivery must allocate once the resume completed")
	}
}

// T28b. The same, with q's marker STANDING at the next resume (the PUBLISH never went out): the
// complete recovery withdraws q (WithdrawnUnsent), no PUBLISH of q is put on B's wire, the flag is
// cleared and a fresh delivery allocates.
//
// CONDITION, OBSERVED: recovery drops a marker-standing, uncancelled register only in its
// routed-source arm (the source classified Complete or Lost: nothing can re-offer it). In this env
// B classifies q's source SRC/41 as gone, which is what makes the withdrawal happen; over a LIVE
// source the same register is restored as an unsent acting entry instead, and the source's
// re-offer sends it DUP=0. The row holds over a gone source, not unconditionally.
func TestF097NextCompleteResumeWithdrawsAMarkerStandingRegister(t *testing.T) {
	t.Parallel()
	f, p := testF097OpaqueQOnPeer(t, "f097-t28b")
	if !f.markerStands() {
		t.Fatal("fixture: q's marker must stand")
	}
	pe := &testInflightEnv{s: p.c.srv, asm: p.asm, sess: p.sess, c: p.c}
	wire := testRow2Wire(pe)
	if _, err := p.sess.recoverInflightRegisters(p.c); err != nil {
		t.Fatalf("the next resume's recovery: %v", err)
	}
	p.sess.mu.Lock()
	incomplete := p.sess.piSpaceIncomplete
	p.sess.mu.Unlock()
	if incomplete {
		t.Fatal("the next resume reads q whole and must clear piSpaceIncomplete")
	}
	ent := p.entry(f.pi)
	x := f.exchange(t)
	t.Logf("observed after the complete resume: entry %+v, exchange %+v", ent, x)
	testCustodyEventually(t, "the complete recovery withdraws q", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeWithdrawnUnsent
	})
	if ent := p.entry(f.pi); ent != nil && ent.acts() {
		t.Fatalf("a withdrawn q must leave no acting entry that could send it, got %+v", ent)
	}
	p.c.mu.Lock()
	written := append([]byte(nil), wire.written...)
	p.c.mu.Unlock()
	if len(written) != 0 && written[0]&0xF0 == mqttPacketPub {
		t.Fatalf("no PUBLISH of q may be sent, the wire carries %x", written)
	}
	p.sess.mu.Lock()
	allocated := p.sess.bumpPI()
	p.sess.mu.Unlock()
	if allocated == 0 {
		t.Fatal("a fresh delivery must allocate once the resume completed")
	}
}

// testF097TerminalRetryFix is T30's construction: a transmitted ordinary exchange whose PUBACK is
// accepted into the terminal-retry worker at acq == k because its first advance fails (seam); the
// retry's register read is observed (the instance load seam, a passthrough) and the second
// advance attempt is counted.
type testF097TerminalRetryFix struct {
	*testLateAckFix
	k        uint64
	advances atomic.Int32
	// armedAdvances counts the advances attempted AFTER armed: every advance before it is refused,
	// so the acknowledgement's whole in-call budget is spent and the retry worker owns it (D3).
	armedAdvances atomic.Int32
	mu            sync.Mutex
	reads         []uint64 // Acq of every register read of the subject after armed
	armed         atomic.Bool
}

func testF097AcceptedIntoRetry(t *testing.T, clientID string) *testF097TerminalRetryFix {
	t.Helper()
	r := &testF097TerminalRetryFix{testLateAckFix: testLateAckTransmitted(t, clientID, 1)}
	cur, _ := r.register(t)
	if cur == nil {
		t.Fatal("fixture: the transmitted register must stand")
	}
	// k is whatever acquisition counter the transmission left (this send path leaves 0); the retry
	// captured it in its reference, and a successor is any later value.
	r.k = cur.Acq
	e := r.e
	e.asm.mu.Lock()
	e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.Nonce == r.lin {
			r.advances.Add(1)
			if !r.armed.Load() {
				return mqttOpOutcome{}, errors.New("test: the acknowledgement advance failed until the retry is armed")
			}
			r.armedAdvances.Add(1)
		}
		return write()
	}
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		rec, smsg, err := e.asm.jsa.loadInflightRecord(subject)
		if subject == r.subject && r.armed.Load() && rec != nil {
			r.mu.Lock()
			r.reads = append(r.reads, rec.Acq)
			r.mu.Unlock()
		}
		return rec, smsg, err
	}
	e.asm.mu.Unlock()
	if err := e.c.mqttProcessPubAck(r.pi); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	if r.advances.Load() != mqttForegroundAcquireAttempts {
		t.Fatalf("fixture: the acknowledgement's whole in-call budget (%d advances) must have been attempted "+
			"and failed, attempts %d", mqttForegroundAcquireAttempts, r.advances.Load())
	}
	if x := r.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("fixture: the failed advance must leave the exchange at Publish, got %+v", x)
	}
	if r.entry() != nil {
		t.Fatal("fixture: the accepted PUBACK must have taken the entry (the retry worker owns it now)")
	}
	return r
}

// reconnect is the same-generation reconnect: a new tenure serves and a new connection is accepted
// on the SAME session object (T20's construction).
func (r *testF097TerminalRetryFix) reconnect(t *testing.T) *client {
	t.Helper()
	e := r.e
	ent1, _ := e.c.outboundEntitlement()
	testRewriteAuthority(t, e, func(a *mqttOutboundAuthority) { a.Tenure, a.OwnerTxn = mqttNewInflightNonce(), a.Txn })
	c2 := e.s.createInternalAccountClient()
	testAcceptRigConnection(t, e.asm, e.sess, c2)
	ent2, _ := c2.outboundEntitlement()
	if ent2 == ent1 || ent2.isZero() {
		t.Fatalf("fixture: the reconnect must serve under a NEW tenure (%+v vs %+v)", ent2, ent1)
	}
	e.sess.mu.Lock()
	e.sess.c = c2
	gen := e.sess.gen
	e.sess.mu.Unlock()
	if gen != r.gen {
		t.Fatalf("fixture: the reconnect must keep the generation, %q vs %q", gen, r.gen)
	}
	return c2
}

func (r *testF097TerminalRetryFix) readAcqs() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.reads...)
}

// T30a. The PUBACK accepted into the terminal-retry worker at acq == k (its in-call advances failed);
// the client reconnects to the same session object (same generation, new tenure) before the
// retry; no new acquisition happens in between (the retry's own read finds Acq == k); the retry
// completes the SAME acquisition (Terminal).
func TestF097AcceptedAckCompletesTheSameAcquisitionAcrossReconnect(t *testing.T) {
	t.Parallel()
	r := testF097AcceptedIntoRetry(t, "f097-t30a")
	r.reconnect(t)
	r.armed.Store(true)
	testDriverWait(t, "the retry completes the acknowledged exchange", 10*time.Second, func() bool {
		x := r.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
	reads := r.readAcqs()
	if len(reads) == 0 || reads[0] != r.k {
		t.Fatalf("witness: the retry's register read must find the same acquisition %d, got %v", r.k, reads)
	}
	if r.armedAdvances.Load() < 1 {
		t.Fatalf("the completion must come from the retry's advance, armed attempts %d", r.armedAdvances.Load())
	}
	testCustodyEventually(t, "the terminal register is cleaned", func() bool { cur, _ := r.register(t); return cur == nil })
}

// T30b. As T30a, but a SUCCESSOR acquisition k+1 of the same allocation lands before the retry.
// The retry's read finds cur.Acq == k+1 != ref.acq (witness: the instance load seam) and takes
// doneOverSurvivor: no advance from the old PUBACK, the successor keeps its token and Acq k+1,
// the exchange stays at Publish, and the identifier is handed off as excluded (doneOverSurvivor's
// own effect). Subtest `drained`: the successor has ALSO drained (token clear) before the retry, so
// only the captured-k comparison stands between the old PUBACK and a terminal advance.
//
// INPUT, STATED: the successor acquisition (and its drain) are written at the raw register
// boundary in the acquisition's shape (Acq+1 with a token, then the token cleared).
//
// Mutation M36 (the retry re-reads acq from the register instead of the captured ref): `drained`
// terminalizes the successor -> fails.
func TestF097AcceptedAckDoesNotTerminalizeASuccessor(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, clientID string, drained bool) {
		r := testF097AcceptedIntoRetry(t, clientID)
		r.reconnect(t)
		cur, seq := r.register(t)
		succ := *cur
		succ.Acq = r.k + 1
		succ.ActionToken, succ.ActionClaim, succ.ActionClaimSeq = mqttNewInflightNonce(), cur.Claim, cur.ClaimSeq
		seq2, err := r.e.asm.jsa.storeInflightCAS(r.subject, seq, &succ)
		if err != nil {
			t.Fatalf("fixture: landing the successor acquisition: %v", err)
		}
		if drained {
			done := succ
			done.ActionToken, done.ActionClaim, done.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
			if _, err := r.e.asm.jsa.storeInflightCAS(r.subject, seq2, &done); err != nil {
				t.Fatalf("fixture: draining the successor: %v", err)
			}
		}
		r.e.sess.mu.Lock()
		excludedBefore := r.e.sess.slotExclusionInstLocked(r.pi)
		r.e.sess.mu.Unlock()
		if excludedBefore != 0 {
			t.Fatalf("fixture: the identifier must not be excluded before the retry (instance %d), or the "+
				"doneOverSurvivor witness below proves nothing", excludedBefore)
		}
		r.armed.Store(true)
		testDriverWait(t, "the retry reads the register", 10*time.Second, func() bool { return len(r.readAcqs()) > 0 })
		if reads := r.readAcqs(); reads[0] != r.k+1 {
			t.Fatalf("witness: the retry must read the successor's acquisition %d, got %v", r.k+1, reads)
		}
		testDriverWait(t, "the retry discharges over the survivor", 10*time.Second, func() bool {
			r.e.sess.mu.Lock()
			defer r.e.sess.mu.Unlock()
			return len(r.e.sess.terminalRetries) == 0 && r.e.sess.slotExclusionInstLocked(r.pi) != 0
		})
		testCustodyStaysFor(t, "the old PUBACK advances nothing", 300*time.Millisecond, func() bool {
			x := r.exchange(t)
			return x != nil && x.Phase == mqttExchangePublish && r.armedAdvances.Load() == 0
		})
		after, _ := r.register(t)
		if after == nil || after.Acq != r.k+1 || after.State != mqttInflightPublish {
			t.Fatalf("the successor must keep Acq %d at publish, got %+v", r.k+1, after)
		}
		if !drained && after.ActionToken != succ.ActionToken {
			t.Fatalf("the successor must keep its token until its own drain, got %+v", after)
		}
	}
	t.Run("live", func(t *testing.T) { t.Parallel(); run(t, "f097-t30b", false) })
	t.Run("drained", func(t *testing.T) { t.Parallel(); run(t, "f097-t30b-drained", true) })
}

// testF097ResolverSeen is one firing of the resolver's AckRoute publish-arm decision hook, tagged
// by caller context: FOREGROUND inside the client's PUBREC handler, WORKER otherwise (the intent
// resolution worker's attempt, which runs on its own timer after the handler returned).
type testF097ResolverSeen struct {
	worker             bool
	curAcq, bindingAcq uint64
}

// testF097QoS2AckRoute is T29's base: a TRANSMITTED QoS 2 exchange untracked UNANSWERED by the
// drain-owned deny (T4's base), its AckRoute standing; A records its wire, and the decision hook
// (package-level: the caller is not parallel) and a register-read seam (instance) are installed.
// The seam fails the first failReads reads of the register after arm(), then reads through.
type testF097QoS2AckRoute struct {
	*testLateAckFix
	b          *mqttAllocationAckBinding
	wire       *testFrameConn
	foreground atomic.Bool
	armed      atomic.Bool
	failReads  int32
	reads      atomic.Int32
	failed     atomic.Int32
	mu         sync.Mutex
	seen       []testF097ResolverSeen
}

func testF097QoS2AckRouteFix(t *testing.T, clientID string, failReads int32) *testF097QoS2AckRoute {
	t.Helper()
	f := testLateAckTransmitted(t, clientID, 2)
	f.untrackByDeny(t)
	b := f.binding()
	if b == nil || b.role != mqttAckRoleAckRoute || b.qos != 2 {
		t.Fatalf("fixture: a QoS 2 AckRoute must stand, got %+v", b)
	}
	r := &testF097QoS2AckRoute{testLateAckFix: f, b: b, failReads: failReads}
	r.wire = testRow2Wire(f.e)
	mqttTestResolverAckRoutePublish = func(alloc string, curAcq, bindingAcq uint64) {
		if alloc != b.nonce {
			return
		}
		r.mu.Lock()
		r.seen = append(r.seen, testF097ResolverSeen{worker: !r.foreground.Load(), curAcq: curAcq, bindingAcq: bindingAcq})
		r.mu.Unlock()
	}
	t.Cleanup(func() { mqttTestResolverAckRoutePublish = nil })
	f.e.asm.mu.Lock()
	f.e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if subject == f.subject && r.armed.Load() {
			if n := r.reads.Add(1); n <= r.failReads {
				r.failed.Add(1)
				return nil, nil, errors.New("test: the register read did not resolve")
			}
		}
		return f.e.asm.jsa.loadInflightRecord(subject)
	}
	f.e.asm.mu.Unlock()
	return r
}

func (r *testF097QoS2AckRoute) decisions() []testF097ResolverSeen {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]testF097ResolverSeen(nil), r.seen...)
}

func (r *testF097QoS2AckRoute) intentOwed() bool {
	s := r.e.s
	s.owedIntentsMu.Lock()
	defer s.owedIntentsMu.Unlock()
	for k := range s.owedIntents {
		if k.sess == r.e.sess && k.name.pi == r.pi {
			return true
		}
	}
	return false
}

// pubRec is the client's PUBREC, on the session's connection, as the FOREGROUND.
func (r *testF097QoS2AckRoute) pubRec(t *testing.T, reason byte) {
	t.Helper()
	r.armed.Store(true)
	r.foreground.Store(true)
	err := r.e.c.mqttProcessPubRec(r.pi, reason)
	r.foreground.Store(false)
	if err != nil {
		t.Fatalf("PUBREC: %v", err)
	}
}

func (r *testF097QoS2AckRoute) wrote(frame []byte) bool {
	r.e.c.mu.Lock()
	defer r.e.c.mu.Unlock()
	return strings.Contains(string(r.wire.written), string(frame))
}

// T29 (QoS 2 residual by phase).
//   - `pubrec-ackroute` (R6-F1; M22/M37's target): the client's successful PUBREC resolves the
//     AckRoute. BOTH foreground reads of the register fail (intendPubRelSlot's first read, then its
//     resolver's), so the foreground outcome is Unresolved and the intent resolution worker owns
//     the obligation (witness: two failed reads, no FOREGROUND decision, the worker enrolled). The
//     WORKER's resolver finds the same allocation at publish with acq == k (witness: the
//     WORKER-tagged decision hook, cur.Acq == binding.acq) and returns StillOwed; the retry records
//     the intent and the canonical PUBREL is materialized, delivered and emitted with no further
//     client traffic; the client's PUBCOMP ends the exchange Terminal.
//   - `pubrec-ackroute-successor` (M38's target): the same, with a successor acquisition k+1 landing
//     after the foreground returned Unresolved and before the worker's first read (ordered by the
//     worker's reconcile delay: the successor is written as soon as the handler returns). The
//     WORKER-tagged hook records (cur.Acq = k+1, binding.acq = k); the resolver's disposition is
//     Other: the binding retires and no advance comes from the old PUBREC.
//   - `refused-pubrec`: a refused PUBREC on an established binding ends Rejected with no PUBREL.
//   - `awaiting-pubcomp-actionable`: a past-PUBREC exchange with an ACTIONABLE PUBREL entry while
//     piSpaceIncomplete is set for an unrelated reason (seeded through setPISpaceIncomplete):
//     PUBCOMP is served by mqttProcessPubComp and the exchange ends.
//
// INPUT, STATED: the successor acquisition is written at the raw register boundary in the
// acquisition's shape (T30b's construction).
//
// NOT PARALLEL: the resolver decision hook is package-level.
//
// Mutations: M22 (an AckRoute's successful PUBREC routed to completeAcknowledgedPublish) ends the
// exchange Rejected; M37 (the publish arm keeps HEAD's Unresolved for an AckRoute) never emits
// the PUBREL; M38 (StillOwed without the acquisition comparison) advances the old PUBREC over
// the successor.
func TestF097QoS2ResidualByPhase(t *testing.T) {
	t.Run("pubrec-ackroute", func(t *testing.T) {
		r := testF097QoS2AckRouteFix(t, "f097-t29-ackroute", 2)
		r.pubRec(t, mqttReasonSuccess)
		if n := r.failed.Load(); n != 2 {
			t.Fatalf("witness: both foreground reads of the register must fail, %d did", n)
		}
		for _, s := range r.decisions() {
			if !s.worker {
				t.Fatalf("witness: the foreground reached the publish-arm decision (%+v): its outcome was not Unresolved", s)
			}
		}
		if !r.intentOwed() {
			t.Fatal("witness: the foreground's Unresolved must hand the obligation to the intent resolution worker")
		}
		testDriverWait(t, "the WORKER's resolver meets the bound allocation at publish", 10*time.Second, func() bool {
			return len(r.decisions()) > 0
		})
		if s := r.decisions()[0]; !s.worker || s.curAcq != s.bindingAcq {
			t.Fatalf("witness: the WORKER's first decision must find acq == k (StillOwed's fixture condition), got %+v", s)
		}
		var rec *mqttInflightRecord
		materialized := false
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if rec, _ = r.register(t); rec != nil && rec.State == mqttInflightPubRelReady && rec.PubRelStreamSeq != 0 {
				materialized = true
				break
			}
		}
		// Non-fatal: the materialization is a step on the way to the target (the PUBREL on the
		// wire, M37), which is evaluated below either way.
		if !materialized {
			t.Errorf("not within 10s: the owed successful PUBREC materializes the canonical PUBREL (register %+v)", rec)
		} else {
			if x := r.exchange(t); x == nil || x.Phase != mqttExchangePubRecObserved {
				t.Fatalf("the PUBREC must be observed on the exchange, not routed to a terminal ending, got %+v", x)
			}
			r.e.sess.mu.Lock()
			_, _, pubRelDur := mqttPubRelNames(r.e.sess.idHash, r.e.sess.gen)
			r.e.sess.mu.Unlock()
			testDeliverPubRelCorrelated(t, r.e, r.pi, testInflightAckSubject(pubRelDur, rec.PubRelStreamSeq, 1),
				rec.Nonce, rec.Dur, rec.Seq)
			r.e.s.finishOwedActions()
		}
		// TARGET (M37): the PUBREL reaches the wire with no further client traffic.
		testCustodyEventually(t, "the PUBREL is emitted on the wire with no further client traffic", func() bool {
			return r.wrote(testRow2PubRelFrame(r.pi))
		})
		testCustodyEventually(t, "the PUBREL action drains", func() bool {
			cur, _ := r.register(t)
			return cur != nil && cur.ActionToken == _EMPTY_
		})
		r.e.c.mqttProcessPubComp(r.pi)
		r.e.s.finishOwedTerminals()
		testCustodyEventually(t, "the client's PUBCOMP ends the exchange", func() bool {
			x := r.exchange(t)
			return x != nil && x.Phase == mqttExchangeTerminal
		})
	})
	// `pubrec-ackroute-foreground` (M22's route): the same AckRoute base with NO read fault, so the
	// handler's own intendPubRelSlot succeeds. M22 diverts the successful PUBREC away from
	// intendPubRelSlot altogether, so the shared construction's witnesses above (both FOREGROUND
	// reads of intendPubRelSlot failed) are not on its route; here the route witness is the AckRoute
	// resolver hit (the handler's binding branch read p's register) and the target is that the
	// exchange records the PUBREC instead of ending.
	t.Run("pubrec-ackroute-foreground", func(t *testing.T) {
		r := testF097QoS2AckRouteFix(t, "f097-t29-foreground", 0)
		r.pubRec(t, mqttReasonSuccess)
		if r.reads.Load() == 0 {
			t.Fatal("witness: the AckRoute resolver hit must act on p's register (no read of it after the PUBREC)")
		}
		var x *mqttOutboundExchange
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if x = r.exchange(t); x == nil || x.Phase != mqttExchangePublish {
				break
			}
		}
		// TARGET (M22): the successful PUBREC is observed, never routed to a terminal ending.
		if x == nil || x.Phase == mqttExchangeRejected || x.Phase != mqttExchangePubRecObserved {
			t.Fatalf("the successful PUBREC must be observed on the exchange, not Rejected or ended, got %+v", x)
		}
		testCustodyEventually(t, "the canonical PUBREL is materialized", func() bool {
			rec, _ := r.register(t)
			return rec != nil && rec.State == mqttInflightPubRelReady && rec.PubRelStreamSeq != 0
		})
	})
	t.Run("pubrec-ackroute-successor", func(t *testing.T) {
		r := testF097QoS2AckRouteFix(t, "f097-t29-successor", 2)
		k := r.b.acq
		r.pubRec(t, mqttReasonSuccess)
		if n := r.failed.Load(); n != 2 || !r.intentOwed() {
			t.Fatalf("witness: the foreground must end Unresolved with the worker owning it (failed reads %d)", n)
		}
		if len(r.decisions()) != 0 {
			t.Fatalf("fixture: the successor must land BEFORE the worker's first decision, seen %+v", r.decisions())
		}
		cur, seq := r.register(t)
		if cur == nil || cur.Acq != k || cur.State != mqttInflightPublish {
			t.Fatalf("fixture: the register must still be the bound transmission at publish, got %+v", cur)
		}
		succ := *cur
		succ.Acq = k + 1
		succ.ActionToken, succ.ActionClaim, succ.ActionClaimSeq = mqttNewInflightNonce(), cur.Claim, cur.ClaimSeq
		if _, err := r.e.asm.jsa.storeInflightCAS(r.subject, seq, &succ); err != nil {
			t.Fatalf("fixture: landing the successor acquisition: %v", err)
		}
		testDriverWait(t, "the WORKER's resolver meets the successor", 10*time.Second, func() bool {
			return len(r.decisions()) > 0
		})
		if s := r.decisions()[0]; !s.worker || s.curAcq != k+1 || s.bindingAcq != k {
			t.Fatalf("witness: the WORKER's first decision must see (cur.Acq %d, binding.acq %d), got %+v", k+1, k, s)
		}
		testDriverWait(t, "the resolver's Other retires the binding", 10*time.Second, func() bool { return r.binding() == nil })
		testCustodyStaysFor(t, "no advance comes from the old PUBREC", 300*time.Millisecond, func() bool {
			x := r.exchange(t)
			cur, _ := r.register(t)
			return x != nil && x.Phase == mqttExchangePublish && cur != nil && cur.State == mqttInflightPublish &&
				cur.Acq == k+1 && cur.ActionToken == succ.ActionToken
		})
		if r.wrote(testRow2PubRelFrame(r.pi)) {
			t.Fatal("no PUBREL may be emitted for the old PUBREC over a successor")
		}
	})
	t.Run("refused-pubrec", func(t *testing.T) {
		r := testF097QoS2AckRouteFix(t, "f097-t29-refused", 0)
		r.pubRec(t, 0x80)
		testCustodyEventually(t, "the refused PUBREC ends the exchange Rejected", func() bool {
			x := r.exchange(t)
			return x != nil && x.Phase == mqttExchangeRejected
		})
		if cur, _ := r.register(t); cur != nil && (mqttInflightPastPubRec(cur.State) || cur.PubRelStreamSeq != 0) {
			t.Fatalf("a refused PUBREC must materialize no PUBREL, got %+v", cur)
		}
		if r.wrote(testRow2PubRelFrame(r.pi)) {
			t.Fatal("a refused PUBREC must put no PUBREL on the wire")
		}
	})
	t.Run("awaiting-pubcomp-actionable", func(t *testing.T) {
		f := testLateAckTransmitted(t, "f097-t29-pubcomp", 2)
		if err := f.e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
			t.Fatalf("PUBREC: %v", err)
		}
		var rec *mqttInflightRecord
		testCustodyEventually(t, "the PUBREC materializes the canonical PUBREL", func() bool {
			rec, _ = f.register(t)
			return rec != nil && rec.State == mqttInflightPubRelReady && rec.PubRelStreamSeq != 0
		})
		f.e.sess.mu.Lock()
		_, _, pubRelDur := mqttPubRelNames(f.e.sess.idHash, f.e.sess.gen)
		f.e.sess.mu.Unlock()
		testDeliverPubRelCorrelated(t, f.e, f.pi, testInflightAckSubject(pubRelDur, rec.PubRelStreamSeq, 1),
			rec.Nonce, rec.Dur, rec.Seq)
		f.e.s.finishOwedActions()
		testCustodyEventually(t, "the PUBREL action drains", func() bool {
			cur, _ := f.register(t)
			return cur != nil && cur.ActionToken == _EMPTY_
		})
		// An UNRELATED identifier keeps the space incomplete (seeded: the flag is per session, the
		// PUBCOMP gate is per identifier).
		f.e.sess.setPISpaceIncomplete(f.e.c, "test: an unrelated identifier could not be read")
		f.e.sess.mu.Lock()
		rel := f.e.sess.pendingPubRel[f.pi]
		held := f.e.sess.recoveryPIHeldLocked(f.pi)
		incomplete := f.e.sess.piSpaceIncomplete
		f.e.sess.mu.Unlock()
		if rel == nil || held || !incomplete {
			t.Fatalf("fixture: an actionable PUBREL entry under an incomplete space (entry %+v, held %v, incomplete %v)",
				rel, held, incomplete)
		}
		f.e.c.mqttProcessPubComp(f.pi)
		f.e.s.finishOwedTerminals()
		testCustodyEventually(t, "PUBCOMP ends the exchange whatever the unrelated identifier says", func() bool {
			x := f.exchange(t)
			return x != nil && x.Phase == mqttExchangeTerminal
		})
		f.e.sess.mu.Lock()
		stillIncomplete := f.e.sess.piSpaceIncomplete
		f.e.sess.mu.Unlock()
		if !stillIncomplete {
			t.Fatal("the PUBCOMP must not clear the unrelated incompleteness")
		}
	})
	t.Run("awaiting-pubrec-opaque", func(t *testing.T) {
		f, p := testF097OpaqueQOnPeer(t, "f097-t29-pubrec-opaque", 2)
		ent := p.entry(f.pi)
		_, seqBefore := f.register(t)
		if err := p.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
			t.Fatalf("the declined PUBREC must not be an error: %v", err)
		}
		if got := p.entry(f.pi); got != ent {
			t.Fatalf("the declined PUBREC must keep the opaque reservation, got %+v", got)
		}
		testCustodyStaysFor(t, "the declined PUBREC advances nothing", 200*time.Millisecond, func() bool {
			x := f.exchange(t)
			cur, seq := f.register(t)
			return x != nil && x.Phase == mqttExchangePublish && cur != nil && seq == seqBefore
		})
		// The next complete resume, with q's marker consumed (the PUBLISH may be on the wire).
		ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
		if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
			t.Fatalf("fixture: consuming q's marker: %v", err)
		}
		if _, err := p.sess.recoverInflightRegisters(p.c); err != nil {
			t.Fatalf("the next resume's recovery: %v", err)
		}
		if e := p.entry(f.pi); e == nil || !e.acts() || !e.transmitted {
			t.Fatalf("the next complete resume must restore q ACTING and transmitted, got %+v", e)
		}
		pe, wire := testF097PeerPubRelEnv(t, p)
		if tok := pe.transmit(t, f.pi, testRealPubRelSrcDur, f.sseq, true); !tok.dup {
			t.Fatalf("q's retransmission must carry DUP=1, got %+v", tok)
		}
		if err := p.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
			t.Fatalf("PUBREC on B: %v", err)
		}
		testF097PeerPubRelThroughPubComp(t, f, p, pe, wire)
	})
	t.Run("awaiting-pubcomp-opaque", func(t *testing.T) {
		f := testLateAckTransmitted(t, "f097-t29-pubcomp-opaque", 2)
		if err := f.e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
			t.Fatalf("PUBREC: %v", err)
		}
		var rec *mqttInflightRecord
		testCustodyEventually(t, "the PUBREC materializes the canonical PUBREL", func() bool {
			rec, _ = f.register(t)
			return rec != nil && rec.State == mqttInflightPubRelReady && rec.PubRelStreamSeq != 0
		})
		f.e.sess.mu.Lock()
		_, _, pubRelDur := mqttPubRelNames(f.e.sess.idHash, f.e.sess.gen)
		f.e.sess.mu.Unlock()
		testDeliverPubRelCorrelated(t, f.e, f.pi, testInflightAckSubject(pubRelDur, rec.PubRelStreamSeq, 1),
			rec.Nonce, rec.Dur, rec.Seq)
		f.e.s.finishOwedActions()
		testCustodyEventually(t, "A's PUBREL action drains", func() bool {
			cur, _ := f.register(t)
			return cur != nil && cur.ActionToken == _EMPTY_
		})
		// Cold resume on B whose walk reads the past-PUBREC register UNREADABLE (seam).
		//
		// ROUTE, OBSERVED (the plan names recoveryPIHeldLocked as the decline): for an ORDINARY QoS 2
		// exchange neither available construction reaches it. A cold resume installs a
		// regRecoveryPlaceholder PUBREL entry (no acknowledgement subject), which recoveryPIHeldLocked
		// does not count; a resident resume keeps the resident actionable PUBREL entry (in-memory
		// state wins), so PUBCOMP is served there. Demotion of a PUBREL entry happens only for a
		// quarantined pair (inflight_register.go, the apply phase's quarantine loop), a class this
		// ordinary exchange is not. The residual property is asserted on the cold construction:
		// PUBCOMP is DECLINED while the reservation stands, and the next complete resume serves it.
		var patched atomic.Bool
		p := testF097ResumeOnPeer(t, f.testWithdrawFix, "node2-t29-pubcomp-opaque",
			func(asm *mqttAccountSessionManager) func([]string) ([]*JSApiMsgGetResponse, error) {
				return func(subjects []string) ([]*JSApiMsgGetResponse, error) {
					resps, err := asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
					if err != nil {
						return resps, err
					}
					for i, s := range subjects {
						if s == f.subject && i < len(resps) && resps[i] != nil && resps[i].Message != nil {
							cp := *resps[i]
							m := *cp.Message
							m.Data = []byte("{unreadable")
							cp.Message = &m
							resps[i] = &cp
							patched.Store(true)
						}
					}
					return resps, nil
				}
			})
		p.sess.mu.Lock()
		held := p.sess.recoveryPIHeldLocked(f.pi)
		incomplete := p.sess.piSpaceIncomplete
		pubEnt, relEnt := p.sess.pendingPublish[f.pi], p.sess.pendingPubRel[f.pi]
		p.sess.mu.Unlock()
		if !patched.Load() || !incomplete {
			t.Fatalf("fixture: B's walk must read the register unreadable and leave the space incomplete "+
				"(patched %v, incomplete %v)", patched.Load(), incomplete)
		}
		t.Logf("observed gate on B: recoveryPIHeldLocked=%v, publish entry %+v, pubrel entry %+v", held, pubEnt, relEnt)
		p.c.mqttProcessPubComp(f.pi)
		p.c.srv.finishOwedTerminals()
		testCustodyStaysFor(t, "the PUBCOMP is declined while the reservation stands", 200*time.Millisecond, func() bool {
			x := f.exchange(t)
			return x != nil && x.Phase == mqttExchangePubRecObserved
		})
		// The next complete resume reads the register whole.
		p.asm.mu.Lock()
		p.asm.testInflightLoadMulti = nil
		p.asm.mu.Unlock()
		if _, err := p.sess.recoverInflightRegisters(p.c); err != nil {
			t.Fatalf("the next resume's recovery: %v", err)
		}
		p.sess.mu.Lock()
		incomplete = p.sess.piSpaceIncomplete
		p.sess.mu.Unlock()
		if incomplete {
			t.Fatal("the next resume must complete")
		}
		pe, wire := testF097PeerPubRelEnv(t, p)
		testF097PeerPubRelThroughPubComp(t, f.testWithdrawFix, p, pe, wire)
	})
}

// testF097PeerPubRelEnv makes peer B's connection able to emit a PUBREL and records its wire (T6's
// `completed` construction: an output identity, the accepted record, the PUBREL consumer, no
// delivery subscription - the PUBREL is delivered by hand).
func testF097PeerPubRelEnv(t *testing.T, p *testF088Peer) (*testInflightEnv, *testFrameConn) {
	t.Helper()
	pe := &testInflightEnv{s: p.c.srv, asm: p.asm, sess: p.sess, c: p.c}
	if p.c.cid == 0 {
		testConnIdentity(t, pe, p.c)
	}
	p.sess.mu.Lock()
	p.sess.pubRelSubscribed = true
	p.sess.mu.Unlock()
	testPersistAcceptedRecord(t, pe)
	pe.subscribePubRel(t)
	return pe, testRow2Wire(pe)
}

// testF097PeerPubRelThroughPubComp waits for B's canonical PUBREL, delivers it by hand, requires
// the PUBREL (and no PUBLISH) on B's wire, then the client's PUBCOMP ends the exchange Terminal.
func testF097PeerPubRelThroughPubComp(t *testing.T, f *testWithdrawFix, p *testF088Peer, pe *testInflightEnv, wire *testFrameConn) {
	t.Helper()
	var rec *mqttInflightRecord
	testCustodyEventually(t, "the canonical PUBREL stands", func() bool {
		rec, _ = f.register(t)
		return rec != nil && rec.State == mqttInflightPubRelReady && rec.PubRelStreamSeq != 0
	})
	p.sess.mu.Lock()
	_, _, pubRelDur := mqttPubRelNames(p.sess.idHash, p.sess.gen)
	p.sess.mu.Unlock()
	testDeliverPubRelCorrelated(t, pe, f.pi, testInflightAckSubject(pubRelDur, rec.PubRelStreamSeq, 2),
		rec.Nonce, rec.Dur, rec.Seq)
	pe.s.finishOwedActions()
	frame := testRow2PubRelFrame(f.pi)
	testCustodyEventually(t, "the PUBREL is put on B's wire", func() bool {
		p.c.mu.Lock()
		defer p.c.mu.Unlock()
		return strings.Contains(string(wire.written), string(frame))
	})
	p.c.mu.Lock()
	written := append([]byte(nil), wire.written...)
	p.c.mu.Unlock()
	if len(written) == 0 || written[0]&mqttPacketMask != mqttPacketPubRel {
		t.Fatalf("B must retransmit the PUBREL, not a PUBLISH: wire %x", written)
	}
	testCustodyEventually(t, "B's PUBREL action drains", func() bool {
		cur, _ := f.register(t)
		return cur != nil && cur.ActionToken == _EMPTY_
	})
	p.c.mqttProcessPubComp(f.pi)
	pe.s.finishOwedTerminals()
	testCustodyEventually(t, "the client's PUBCOMP ends the exchange", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal
	})
}

// ---------------------------------------------------------------------------
// End to end: a real server and real MQTT 5 clients
// ---------------------------------------------------------------------------

// testF097Pkt is one packet a client read, parsed far enough for these tests.
type testF097Pkt struct {
	ptype, flags byte
	pi           uint16
	topic        string
	payload      string
}

// testF097ReadPkt reads ONE packet within timeout (ok false on a timeout), consuming it whole.
func testF097ReadPkt(t *testing.T, r *mqttReader, timeout time.Duration) (testF097Pkt, bool) {
	t.Helper()
	b, pl, ok := testMQTTReadPacketReady(r, timeout)
	if !ok {
		return testF097Pkt{}, false
	}
	start := r.pos
	p := testF097Pkt{ptype: b & mqttPacketMask, flags: b & mqttPacketFlagMask}
	switch p.ptype {
	case mqttPacketPub:
		topic, err := r.readBytes("topic", false)
		if err != nil {
			t.Fatalf("reading a PUBLISH topic: %v", err)
		}
		p.topic = string(topic)
		if mqttGetQoS(p.flags) > 0 {
			if p.pi, err = r.readUint16("pi"); err != nil {
				t.Fatalf("reading a PUBLISH identifier: %v", err)
			}
		}
		if _, err := r.readProperties(mqttPropsContextPubOut); err != nil {
			t.Fatalf("reading PUBLISH properties: %v", err)
		}
		p.payload = string(r.buf[r.pos : start+pl])
	default:
		if pl >= 2 {
			pi, err := r.readUint16("pi")
			if err != nil {
				t.Fatalf("reading a packet identifier: %v", err)
			}
			p.pi = pi
		}
	}
	r.pos = start + pl
	return p, true
}

// T3 (production regression). A QoS 2 subscriber withholds its PUBREC for p: the live entry keeps
// p (witness). The message's Message Expiry Interval elapses (its stored copy's JetStream TTL is
// real) while AckWait re-offers meet the entry. A second message is delivered under ANOTHER
// identifier (p is never reused while its exchange is unanswered); the late PUBREC is answered by
// PUBREL p and the client's PUBCOMP completes the exchange.
//
// Environment: a real server and real MQTT 5 clients. NOT claimed to reach the admission's
// different-source arm (T3b is that boundary's test).
func TestF097LostPubRecSourceExpiryNeverReusesTheIdentifier(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const sub, topic = "f097-t3-sub", "f097t3/x"

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: sub, cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: topic, opts: 2}})
	testMQTTFlush(t, cs, nil, rs)

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f097-t3-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 2, false, 1, topic, []byte("first"), mqttV5PropsWithExpiry(1))

	first, ok := testF097ReadPkt(t, rs, 5*time.Second)
	if !ok || first.ptype != mqttPacketPub || first.payload != "first" || mqttGetQoS(first.flags) != 2 {
		t.Fatalf("the subscriber must receive the first message at QoS 2, got %+v (ok %v)", first, ok)
	}
	p := first.pi
	// PUBREC withheld. Witness: the live entry holds p (transmitted once its send section closed).
	testCustodyEventually(t, "the live entry holds p as transmitted", func() bool {
		ack, tracked := testInflightEntry(t, s, sub, p)
		return tracked && ack.transmitted
	})
	// The Message Expiry Interval (1 s) elapses; AckWait (500 ms) re-offers meet the entry.
	time.Sleep(2500 * time.Millisecond)
	if _, tracked := testInflightEntry(t, s, sub, p); !tracked {
		t.Fatalf("the unanswered exchange's entry must still hold %d after its source expired", p)
	}

	testMQTTPubV5Props(t, cp, rp, 1, false, 2, topic, []byte("second"), nil)
	var second testF097Pkt
	deadline := time.Now().Add(10 * time.Second)
	for {
		pkt, ok := testF097ReadPkt(t, rs, time.Until(deadline))
		if !ok {
			t.Fatal("the second message was never delivered")
		}
		if pkt.ptype == mqttPacketDisconnect {
			t.Fatal("the server sent DISCONNECT")
		}
		if pkt.ptype == mqttPacketPub && pkt.payload == "first" {
			if pkt.pi != p {
				t.Fatalf("a re-offer of the first message must keep identifier %d, got %d", p, pkt.pi)
			}
			continue
		}
		if pkt.ptype == mqttPacketPub && pkt.payload == "second" {
			second = pkt
			break
		}
		t.Fatalf("unexpected packet before the second message: %+v", pkt)
	}
	if second.pi == p {
		t.Fatalf("the second message reused identifier %d while its exchange is unanswered", p)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, cs, second.pi)

	// The late PUBREC: PUBREL p, then the client's PUBCOMP completes it.
	testMQTTSendPIPacket(mqttPacketPubRec, t, cs, p)
	deadline = time.Now().Add(10 * time.Second)
	for {
		pkt, ok := testF097ReadPkt(t, rs, time.Until(deadline))
		if !ok {
			t.Fatalf("no PUBREL answered the late PUBREC for %d", p)
		}
		if pkt.ptype == mqttPacketPubRel && pkt.pi == p {
			break
		}
		if pkt.ptype == mqttPacketPub && (pkt.payload == "first" && pkt.pi == p || pkt.payload == "second" && pkt.pi == second.pi) {
			continue // a re-offer crossing the answer on the wire
		}
		t.Fatalf("unexpected packet while waiting for PUBREL %d: %+v", p, pkt)
	}
	testMQTTSendPIPacket(mqttPacketPubComp, t, cs, p)
	asm := testMQTTGetAccountSessionManager(t, s, sub)
	asm.mu.RLock()
	sess := asm.sessions[sub]
	asm.mu.RUnlock()
	testCustodyEventually(t, "the PUBCOMP completes the exchange", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.pendingPublish[p] == nil && sess.pendingPubRel[p] == nil
	})
}

// T31 (residual preservation). During an INCOMPLETE recovery (the T26/T27 shape, reached on a real
// server's CONNECT): no server DISCONNECT, already reserved QoS 1 work continues, QoS 0
// deliveries flow. The subscriber receives two QoS 1 messages under identifiers a and b and
// answers neither, then reconnects to its session; the resume's pre-apply reread of a's register
// is answered unreadable for a alone (instance seam on the account manager), so a is reserved
// as undecided and the identifier space is left incomplete (allocation withheld).
//
// OBSERVED SHAPE (a RESIDENT reconnect, the only one a single server offers): the resident entries
// win over the walk's reread failure ("in-memory state wins"), so a stays ACTING here rather than
// becoming the cold resume's marker-opaque reservation (T26/T27 build that one on a peer); the
// incompleteness withholds fresh allocation only.
//
// Witnessed on the wire: CONNACK; the reserved exchanges' retransmissions (DUP=1, same
// identifiers) arrive and the client's PUBACKs end them; a QoS 0 message is delivered; a NEW QoS 1
// message is withheld (allocation closed: the stated residual, recorded, not a defect); no
// DISCONNECT and the connection answers PINGREQ. Not compared against a HEAD build in this run:
// the behaviour is recorded as observed.
func TestF097NoNewDisconnectOrQoS0Degradation(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	const sub, q1, q0 = "f097-t31-sub", "f097t31/q1", "f097t31/z"
	connect := func() (net.Conn, *mqttReader) {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: sub, cleanStart: false,
			props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		return c, r
	}
	cs, rs := connect()
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: q1, opts: 1}, {topic: q0, opts: 0}})
	testMQTTFlush(t, cs, nil, rs)

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f097-t31-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 1, false, 1, q1, []byte("a"), nil)
	testMQTTPubV5Props(t, cp, rp, 1, false, 2, q1, []byte("b"), nil)
	pis := map[string]uint16{}
	for len(pis) < 2 {
		pkt, ok := testF097ReadPkt(t, rs, 5*time.Second)
		if !ok || pkt.ptype != mqttPacketPub {
			t.Fatalf("the subscriber must receive both QoS 1 messages, got %+v (ok %v)", pkt, ok)
		}
		pis[pkt.payload] = pkt.pi
	}
	cs.Close()

	asm := testMQTTGetAccountSessionManager(t, s, sub)
	asm.mu.RLock()
	sess := asm.sessions[sub]
	asm.mu.RUnlock()
	sess.mu.Lock()
	subjA := mqttInflightSubject(sess.idHash, sess.gen, pis["a"])
	sess.mu.Unlock()
	var readsA atomic.Int32
	var patched atomic.Bool
	asm.mu.Lock()
	asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		resps, err := asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
		if err != nil {
			return resps, err
		}
		for i, sj := range subjects {
			// The first batch naming a's register is the walk's read; the second is the
			// pre-apply reread of a register whose marker is absent.
			if sj == subjA && readsA.Add(1) == 2 && i < len(resps) {
				resps[i] = indeterminateResp()
				patched.Store(true)
			}
		}
		return resps, nil
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testInflightLoadMulti = nil
		asm.mu.Unlock()
	})

	cs, rs = connect()
	defer cs.Close()
	if !patched.Load() {
		t.Fatalf("witness: the resume's reread of a's register was never reached (%d reads)", readsA.Load())
	}
	sess.mu.Lock()
	incomplete := sess.piSpaceIncomplete
	entA, entB := sess.pendingPublish[pis["a"]], sess.pendingPublish[pis["b"]]
	sess.mu.Unlock()
	desc := func(p *mqttPending) string {
		if p == nil {
			return "nil"
		}
		return fmt.Sprintf("acts=%v markerOpaque=%v transmitted=%v", p.acts(), p.markerOpaque, p.transmitted)
	}
	if !incomplete || entA == nil || entB == nil || !entB.acts() {
		t.Fatalf("fixture: the resume must be incomplete with both identifiers reserved (incomplete %v, a %s, b %s)",
			incomplete, desc(entA), desc(entB))
	}
	t.Logf("observed after the incomplete resident resume: a %s, b %s", desc(entA), desc(entB))

	testMQTTPubV5Props(t, cp, rp, 0, false, 0, q0, []byte("zero"), nil)
	testMQTTPubV5Props(t, cp, rp, 1, false, 3, q1, []byte("c"), nil)
	gotRetx := map[string]bool{}
	var gotZero bool
	wantRetx := []string{"b"}
	if entA.acts() {
		wantRetx = append(wantRetx, "a")
	}
	done := func() bool {
		for _, m := range wantRetx {
			if !gotRetx[m] {
				return false
			}
		}
		return gotZero
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !done() {
		pkt, ok := testF097ReadPkt(t, rs, time.Until(deadline))
		if !ok {
			break
		}
		switch {
		case pkt.ptype == mqttPacketDisconnect:
			t.Fatal("the incomplete recovery must produce no server DISCONNECT")
		case pkt.ptype == mqttPacketPub && (pkt.payload == "a" || pkt.payload == "b"):
			if !entA.acts() && pkt.payload == "a" {
				t.Fatalf("a non-acting, undecided a must not be retransmitted on this connection: %+v", pkt)
			}
			if pkt.pi != pis[pkt.payload] || pkt.flags&mqttPubFlagDup == 0 {
				t.Fatalf("%s's retransmission must keep identifier %d with DUP=1, got %+v", pkt.payload, pis[pkt.payload], pkt)
			}
			gotRetx[pkt.payload] = true
			testMQTTSendPIPacket(mqttPacketPubAck, t, cs, pkt.pi)
		case pkt.ptype == mqttPacketPub && pkt.payload == "zero":
			gotZero = true
		case pkt.ptype == mqttPacketPub && pkt.payload == "c":
			t.Fatalf("a NEW QoS 1 delivery was allocated while the identifier space is incomplete: %+v", pkt)
		default:
			t.Fatalf("unexpected packet: %+v", pkt)
		}
	}
	if !done() {
		t.Fatalf("reserved QoS 1 work and QoS 0 must keep flowing (retransmitted %v of %v, QoS 0 delivered %v)",
			gotRetx, wantRetx, gotZero)
	}
	for _, m := range wantRetx {
		pi := pis[m]
		testCustodyEventually(t, m+"'s PUBACK ends its exchange", func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			return sess.pendingPublish[pi] == nil
		})
	}
	// Still open: PINGREQ is answered, and nothing else arrives (no DISCONNECT, no c).
	if _, err := testMQTTWrite(cs, []byte{mqttPacketPing, 0}); err != nil {
		t.Fatalf("PINGREQ: %v", err)
	}
	pkt, ok := testF097ReadPkt(t, rs, 2*time.Second)
	if !ok || pkt.ptype != mqttPacketPingResp {
		t.Fatalf("the connection must stay open and answer PINGREQ, got %+v (ok %v)", pkt, ok)
	}
	sess.mu.Lock()
	incomplete = sess.piSpaceIncomplete
	sess.mu.Unlock()
	if !incomplete {
		t.Fatal("observed: allocation reopened without a complete resume (the stated residual says it stays closed)")
	}
}
