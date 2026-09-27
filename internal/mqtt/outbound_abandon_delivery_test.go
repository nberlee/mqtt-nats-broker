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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"sync/atomic"
	"testing"
)

// A restored delivery denied on resume is abandoned durably (F-105 row 5, plan-row5.md rev 9
// §3.13). Every test starts from an ordinary delivery ADMITTED through the authority and then
// TRANSMITTED (its unsent marker deleted, as a send's intent delete does), with the restored
// entry and source mapping standing — the population the deny guard meets on a resume.

type testAbandonFix struct {
	*testWithdrawFix
	reply string
}

func testAbandonAdmitted(t *testing.T, clientID string, qos byte) *testAbandonFix {
	t.Helper()
	f := testWithdrawAdmitted(t, clientID, qos)
	ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
		t.Fatalf("deleting the marker (the transmission's intent delete): %v", err)
	}
	f.e.sess.mu.Lock()
	ent := f.e.sess.pendingPublish[f.pi]
	mapped := f.e.sess.cpending[testRealPubRelSrcDur][f.sseq] == f.pi
	f.e.sess.mu.Unlock()
	if ent == nil || ent.lin != f.lin || !mapped {
		t.Fatalf("fixture: the restored entry and mapping must stand: %+v mapped=%v", ent, mapped)
	}
	testCustodyRM1(t, f.e.sess)
	return &testAbandonFix{testWithdrawFix: f, reply: testInflightAckSubject(testRealPubRelSrcDur, f.sseq, 2)}
}

func (f *testAbandonFix) deny() mqttAbandonResult {
	return f.e.sess.releaseTransmittedIdentifier(f.e.c, testRealPubRelSrcDur, f.reply)
}

func (f *testAbandonFix) requireAbandoned(t *testing.T) {
	t.Helper()
	if x := f.exchange(t); x.Phase != mqttExchangeDeliveryAbandoned {
		t.Fatalf("the exchange stands at phase %d, want DeliveryAbandoned", x.Phase)
	}
	cur, _ := f.register(t)
	if cur == nil || cur.State != mqttInflightDeliveryAbandoned || cur.AbandonReason != mqttAbandonReasonACLDeny ||
		cur.AbandonTenure == _EMPTY_ || cur.Nonce != f.nonce {
		t.Fatalf("the register is not the abandoned disposal record: %+v", cur)
	}
}

// T25. The deny abandons durably, releases the charge (B admitted at Receive Maximum 1), keeps
// the identifier, untracks the entry and suppresses the source.
//
// Mutation M25a: the ordinary arm skipped (HEAD untrack only) -> the exchange stays Publish.
func TestF105AbandonDeniedDeliveryReleasesTheChargeNotTheIdentifier(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-basic", 1)
	if r := f.deny(); r != mqttAbandonDone {
		t.Fatalf("the deny established %v, want the abandonment", r)
	}
	f.requireAbandoned(t)
	sess := f.e.sess
	sess.mu.Lock()
	_, ent := sess.pendingPublish[f.pi]
	_, mapped := sess.cpending[testRealPubRelSrcDur][f.sseq]
	suppressed := sess.sourceRetiredLocked(testRealPubRelSrcDur, f.sseq)
	sess.mu.Unlock()
	if ent || mapped || !suppressed {
		t.Fatalf("memory after the abandonment: entry=%v mapping=%v suppressed=%v", ent, mapped, suppressed)
	}
	testCustodyEventually(t, "B admitted once the abandonment is observed", func() bool {
		return testCustodyCapacity(sess) == 1
	})
	if got := testCustodyMints(sess, f.pi); got == f.pi {
		t.Fatalf("the abandoned delivery's identifier %d was handed out", f.pi)
	}
}

// T26. Cold reconstruction on a fresh session object: the abandoned exchange holds its
// identifier only.
//
// Mutation M25b: the observation removes the entry (identifier released with the charge).
// Mutation M26: the census adopts no DeliveryAbandoned exchange.
func TestF105AbandonedIdentifierSurvivesColdReconstruction(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-cold", 1)
	if r := f.deny(); r != mqttAbandonDone {
		t.Fatalf("deny: %v", r)
	}
	testCustodyEventually(t, "live: the charge released", func() bool { return testCustodyCapacity(f.e.sess) == 1 })
	if got := testCustodyMints(f.e.sess, f.pi); got == f.pi {
		t.Fatal("live: the identifier was handed out")
	}
	restored, _, err := f.e.asm.createOrRestoreSession(f.e.sess.id, f.e.s.getOpts())
	if err != nil || restored == f.e.sess {
		t.Fatalf("createOrRestoreSession must build a FRESH object: %v", err)
	}
	testCustodyRM1(t, restored)
	if _, err := restored.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	restored.mu.Lock()
	holds := restored.custody.holdsPI(f.pi)
	charges := restored.custody.charges()
	_, ent := restored.pendingPublish[f.pi]
	restored.mu.Unlock()
	if !holds || charges != 0 || ent {
		t.Fatalf("cold: identifier held=%v charges=%d entry=%v, want held, 0, none", holds, charges, ent)
	}
	if got := testCustodyMints(restored, f.pi); got == f.pi {
		t.Fatal("cold: the abandoned identifier was handed out")
	}
}

// T27. A QoS 2 restored PUBLISH whose PUBREC was lost is abandoned the same way and its
// identifier is held.
//
// Mutation M27: releasing the charge also drops the identifier hold -> handed out.
func TestF105AbandonedQoS2IdentifierIsHeld(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-qos2", 2)
	if r := f.deny(); r != mqttAbandonDone {
		t.Fatalf("deny: %v", r)
	}
	f.requireAbandoned(t)
	testCustodyEventually(t, "the charge released", func() bool { return testCustodyCapacity(f.e.sess) == 1 })
	if got := testCustodyMints(f.e.sess, f.pi); got == f.pi {
		t.Fatalf("the QoS 2 abandoned identifier %d was handed out", f.pi)
	}
}

// T28. A delivery last acquired (and drained) under the CURRENT connection's tenure is not
// abandoned: its charge is this connection's quota. Pending, source not acknowledged.
//
// Mutation M28: the tenure veto removed -> abandoned.
func TestF105AbandonVetoesACurrentConnectionAcquisition(t *testing.T) {
	t.Parallel()
	e, subject, tok, pi := testAcquireEnv(t, "abandon-veto")
	if !e.sess.acquirePublishAction(e.c, pi, &tok) {
		t.Fatal("fixture: the acquisition under the current tenure must land")
	}
	if !e.sess.drainPublishAction(e.c, tok) {
		t.Fatal("fixture: the drain must clear the token")
	}
	cur, _, _ := e.asm.loadInflightRecord(subject)
	if cur == nil || cur.ActionToken != _EMPTY_ || cur.Acq == 0 {
		t.Fatalf("fixture: a drained, acquired publish slot: %+v", cur)
	}
	e.sess.mu.Lock()
	if e.sess.cpending == nil {
		e.sess.cpending = map[string]map[uint64]uint16{}
	}
	if e.sess.cpending[testRealPubRelSrcDur] == nil {
		e.sess.cpending[testRealPubRelSrcDur] = map[uint64]uint16{}
	}
	e.sess.cpending[testRealPubRelSrcDur][testRealPubRelSrcSeq] = pi
	e.sess.mu.Unlock()
	r := e.sess.releaseTransmittedIdentifier(e.c, testRealPubRelSrcDur,
		testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 2))
	if r != mqttAbandonPending {
		t.Fatalf("a delivery acquired under this connection's tenure was %v, want pending", r)
	}
	if _, verr := e.sess.abandonCurrentOutputVeto(testAbandonEvidence(t, e, pi, subject)); !errors.Is(verr, errMQTTAbandonCurrentOutput) {
		t.Fatalf("the refusal is not the current-connection veto: %v", verr)
	}
	if after, _, _ := e.asm.loadInflightRecord(subject); after == nil || after.State != mqttInflightPublish {
		t.Fatalf("the vetoed delivery's register moved: %+v", after)
	}
}

// T29. A register rewrite between the evidence read and the commit (the authority untouched)
// fails the batch; every re-attempt re-reads, and while the register keeps moving nothing is
// abandoned.
//
// Mutation M29: a blind register-sequence refresh after the evidence -> the batch lands over the
// moved register.
func TestF105AbandonIsFencedByTheRegister(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-fence", 1)
	var moved atomic.Bool
	var attempts atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		attempts.Add(1)
		req.afterEvidence = func() {
			moved.Store(true)
			cur, smsg, err := f.e.asm.jsa.loadInflightRecord(f.subject)
			if err != nil || cur == nil || smsg == nil {
				t.Errorf("reading the register to move: %v", err)
				return
			}
			same := *cur
			if _, werr := f.e.asm.jsa.storeInflightCAS(f.subject, smsg.Sequence, &same); werr != nil {
				t.Errorf("moving the register: %v", werr)
			}
		}
		return write()
	}
	f.e.asm.mu.Unlock()
	if r := f.deny(); r != mqttAbandonPending {
		t.Fatalf("an abandonment over a moved register returned %v, want pending", r)
	}
	if !moved.Load() {
		t.Fatal("the fixture never moved the register")
	}
	if attempts.Load() < 2 {
		t.Fatalf("a batch lost to contention was not re-attempted with fresh evidence (%d attempt(s))", attempts.Load())
	}
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("the exchange moved to %d over a stale register", x.Phase)
	}
}

// T30. Forged abandon-delivery batches, each refused at its own first enforcement point.
func TestF105AbandonBoundaryRefusesForgedBatches(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, want string
		mutate     func(tr *mqttAuthorityTransition, reg *mqttInflightRecord, x *mqttOutboundExchange) (regBody, xBody bool)
		dropX      bool
	}{
		{name: "no exchange member", want: "carries 1 register(s) and 0 exchange(s)", dropX: true},
		{name: "source changed", want: "changes more than its disposition",
			mutate: func(_ *mqttAuthorityTransition, reg *mqttInflightRecord, _ *mqttOutboundExchange) (bool, bool) {
				reg.Seq++
				testRegisterSourceReply(reg)
				return true, false
			}},
		{name: "lineage changed", want: "describe ONE delivery",
			mutate: func(_ *mqttAuthorityTransition, reg *mqttInflightRecord, _ *mqttOutboundExchange) (bool, bool) {
				reg.Lin = mqttNewInflightNonce()
				return true, false
			}},
		{name: "cancelled on the successor", want: "cancellation or retirement mark",
			mutate: func(_ *mqttAuthorityTransition, reg *mqttInflightRecord, _ *mqttOutboundExchange) (bool, bool) {
				reg.Cancelled = true
				return true, false
			}},
		{name: "unknown reason", want: "no supported reason",
			mutate: func(_ *mqttAuthorityTransition, reg *mqttInflightRecord, _ *mqttOutboundExchange) (bool, bool) {
				reg.AbandonReason = "whim"
				return true, false
			}},
		{name: "tenure mismatch", want: "the decision's binding is the committing authority's",
			mutate: func(_ *mqttAuthorityTransition, reg *mqttInflightRecord, _ *mqttOutboundExchange) (bool, bool) {
				reg.AbandonTenure = mqttNewInflightNonce()
				return true, false
			}},
		// Unit C1: the exchange's historical fact is bound to exactly the register this batch
		// abandons and to the committing tenure.
		{name: "fact bound to another acquisition", want: "without the abandonment of its obligation",
			mutate: func(_ *mqttAuthorityTransition, _ *mqttInflightRecord, x *mqttOutboundExchange) (bool, bool) {
				x.Abandoned.Acq++
				return false, true
			}},
		{name: "fact under another tenure", want: "without the abandonment of its obligation",
			mutate: func(_ *mqttAuthorityTransition, _ *mqttInflightRecord, x *mqttOutboundExchange) (bool, bool) {
				x.Abandoned.Tenure = mqttNewInflightNonce()
				return false, true
			}},
		{name: "fact removed", want: "without its abandonment fact",
			mutate: func(_ *mqttAuthorityTransition, _ *mqttInflightRecord, x *mqttOutboundExchange) (bool, bool) {
				x.Abandoned = nil
				return false, true
			}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := testAbandonAdmitted(t, "abandon-forged-"+strings.ReplaceAll(tc.name, " ", "-"), 1)
			jsa := &f.e.asm.jsa
			auth, authSeq, err := jsa.servingAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
			if err != nil {
				t.Fatalf("no serving authority: %v", err)
			}
			reg, regSeq := f.register(t)
			x, xSeq, _ := jsa.loadOutboundExchange(f.e.sess.id, f.e.asm.sessStreamEpoch, f.gen, f.lin)
			ev := mqttDenyEvidence{Gen: auth.Gen, Tenure: auth.Tenure, Subject: f.subject, Dur: reg.Dur, Seq: reg.Seq,
				Nonce: reg.Nonce, Lin: reg.Lin, PI: reg.PI, Reason: mqttAbandonReasonACLDeny,
				Reply: testInflightAckSubject(reg.Dur, reg.Seq, 2)}
			txn := mqttNewInflightNonce()
			tr, terr := mqttAbandonDeliveryTransition(auth, authSeq, txn, "op-"+txn, ev, reg, regSeq, x, xSeq)
			if terr != nil {
				t.Fatalf("composing: %v", terr)
			}
			if tc.dropX {
				tr.Extra = tr.Extra[:1]
			}
			if tc.mutate != nil {
				var next mqttInflightRecord
				if err := json.Unmarshal(tr.Extra[0].body, &next); err != nil {
					t.Fatal(err)
				}
				var nx mqttOutboundExchange
				if err := json.Unmarshal(tr.Extra[1].body, &nx); err != nil {
					t.Fatal(err)
				}
				rb, xb := tc.mutate(tr, &next, &nx)
				if rb {
					body, _ := json.Marshal(&next)
					tr.Extra[0].body = body
				}
				if xb {
					body, _ := json.Marshal(&nx)
					tr.Extra[1].body = body
				}
			}
			_, cerr := jsa.commitAuthorityTransition(tr)
			if cerr == nil || !strings.Contains(cerr.Error(), tc.want) {
				t.Fatalf("the forged batch was answered %v, want a refusal naming %q", cerr, tc.want)
			}
		})
	}
}

// T31. A genuinely unresolved attempt (resolve and abort both faulted over a batch that never
// left) is pending: the entry and mapping stay and the caller does not acknowledge.
//
// Mutation M31: unresolved treated as abandoned -> the entry is untracked.
func TestF105AbandonUnresolvedIsPending(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-unresolved", 1)
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAbandonDelivery {
			return 0, errors.New("test: the batch's response was lost")
		}
		return send()
	}
	f.e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
		return mqttTransitionUnresolved, true
	}
	f.e.asm.mu.Unlock()
	if r := f.deny(); r != mqttAbandonPending {
		t.Fatalf("an unresolved abandonment returned %v, want pending", r)
	}
	f.e.sess.mu.Lock()
	_, ent := f.e.sess.pendingPublish[f.pi]
	_, mapped := f.e.sess.cpending[testRealPubRelSrcDur][f.sseq]
	f.e.sess.mu.Unlock()
	if !ent || !mapped {
		t.Fatalf("an unresolved abandonment untracked the delivery: entry=%v mapping=%v", ent, mapped)
	}
}

// T32. The batch landed, then the broker stopped before memory was settled: (a) a cold resume
// installs no entry and no mapping and suppresses the source; (b) a same-object resume removes
// the resident entry and mapping.
//
// Mutation M32a: the recovery arm skipped -> restored as an ordinary delivery.
// Mutation M32b: the suppression not installed.
// Mutation M32c: the same-object removal skipped.
func TestF105AbandonedDeliveryRecovery(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-recovery", 1)
	// The batch lands, but memory is left exactly as it was (the crash after the commit).
	ev := mqttDenyEvidence{Gen: f.gen, Subject: f.subject, Dur: testRealPubRelSrcDur, Seq: f.sseq, Nonce: f.nonce,
		Lin: f.lin, PI: f.pi, Reason: mqttAbandonReasonACLDeny, Reply: f.reply}
	ent, ok := f.e.c.outboundEntitlement()
	if !ok {
		t.Fatal("fixture: the connection carries no entitlement")
	}
	ev.Tenure = ent.Tenure
	if r, _, _ := f.e.sess.abandonDeniedDelivery(ev, nil); r != mqttAbandonDone {
		t.Fatalf("the abandonment did not land: %v", r)
	}
	f.requireAbandoned(t)

	// (b) same object.
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("same-object recovery: %v", err)
	}
	f.e.sess.mu.Lock()
	_, entB := f.e.sess.pendingPublish[f.pi]
	_, mappedB := f.e.sess.cpending[testRealPubRelSrcDur][f.sseq]
	supB := f.e.sess.sourceRetiredLocked(testRealPubRelSrcDur, f.sseq)
	f.e.sess.mu.Unlock()
	if entB || mappedB || !supB {
		t.Fatalf("same-object recovery: entry=%v mapping=%v suppressed=%v", entB, mappedB, supB)
	}

	// (a) cold.
	restored, _, err := f.e.asm.createOrRestoreSession(f.e.sess.id, f.e.s.getOpts())
	if err != nil || restored == f.e.sess {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	if _, err := restored.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("cold recovery: %v", err)
	}
	restored.mu.Lock()
	_, entA := restored.pendingPublish[f.pi]
	_, mappedA := restored.cpending[testRealPubRelSrcDur][f.sseq]
	supA := restored.sourceRetiredLocked(testRealPubRelSrcDur, f.sseq)
	restored.mu.Unlock()
	if entA || mappedA || !supA {
		t.Fatalf("cold recovery: entry=%v mapping=%v suppressed=%v", entA, mappedA, supA)
	}
}

// T33. The controls: an indeterminate deny, a shared (slot-only) source, and a consult-32 door
// destruction write no abandonment.
//
// Mutation M33: abandonment invoked from deleteInflightRegisterFor.
func TestF105AbandonmentOnlyForTheOrdinaryTerminalDeny(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "abandon-controls", 1)
	ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	_ = f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true)
	cur, seq := f.register(t)
	tomb := *cur
	tomb.Cancelled = true
	nseq, err := f.e.asm.storeInflightCAS(f.subject, seq, &tomb)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	if !f.e.sess.reapObservedRegister(f.e.c, f.subject, nseq, &tomb, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("the door refused the tombstone destruction")
	}
	if x := f.exchange(t); x.Phase == mqttExchangeDeliveryAbandoned {
		t.Fatal("a door destruction without evidence wrote an abandonment")
	}
	// The GENERIC register delete (source completion's route, among others), driven for a
	// transmitted ordinary delivery with its connection bound: it deletes, it never abandons.
	g := testAbandonAdmitted(t, "abandon-controls-generic", 1)
	g.e.sess.mu.Lock()
	g.e.sess.c = g.e.c
	ent := g.e.sess.pendingPublish[g.pi]
	g.e.sess.mu.Unlock()
	g.e.sess.deleteInflightRegisterFor(ent, mqttSlotDeleteComplete)
	if x := g.exchange(t); x.Phase == mqttExchangeDeliveryAbandoned {
		t.Fatal("the generic register delete wrote an abandonment")
	}
	// A shared source takes the slot-only path: no evidence is built at all.
	e2 := testNewInflightEnv(t, "abandon-controls-shared")
	dur := mqttSharedConsumerDurablePrefix + e2.sess.idHash + "_g"
	e2.sess.mu.Lock()
	e2.sess.pendingPublish = map[uint16]*mqttPending{7: {jsDur: dur, sseq: 5, lin: mqttNewInflightNonce(), regNonce: mqttNewInflightNonce()}}
	e2.sess.cpending = map[string]map[uint64]uint16{dur: {5: 7}}
	e2.sess.mu.Unlock()
	if r := e2.sess.releaseTransmittedIdentifier(e2.c, dur, testInflightAckSubject(dur, 5, 1)); r != mqttAbandonNone {
		t.Fatalf("a shared delivery's deny returned %v, want the slot-only disposal", r)
	}
}

// T34a. A send section open at the deny: pending, entry intact, nothing written.
//
// Mutation M34: condemn-and-ack as HEAD -> the entry is untracked.
func TestF105AbandonWaitsForAnOpenSection(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-section", 1)
	f.e.sess.mu.Lock()
	f.e.sess.pendingPublish[f.pi].sendNonce = mqttNewInflightNonce()
	f.e.sess.mu.Unlock()
	if r := f.deny(); r != mqttAbandonPending {
		t.Fatalf("a deny over an open section returned %v, want pending", r)
	}
	f.e.sess.mu.Lock()
	_, ent := f.e.sess.pendingPublish[f.pi]
	f.e.sess.mu.Unlock()
	if !ent {
		t.Fatal("the entry was untracked while its section is open")
	}
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("an abandonment was written over an open section: phase %d", x.Phase)
	}
	f.e.sess.mu.Lock()
	f.e.sess.pendingPublish[f.pi].sendNonce = _EMPTY_
	f.e.sess.mu.Unlock()
	if r := f.deny(); r != mqttAbandonDone {
		t.Fatalf("after the section closed without an acquisition the deny returned %v", r)
	}
}

// T35. The identifier predicates never adopted the duty predicate.
//
// Mutation M35: exclusivity switched to mqttExchangeDeliveryDutyEnded.
func TestF105DutyPredicateStaysOutOfIdentifierRules(t *testing.T) {
	t.Parallel()
	forbidden := map[string]bool{"checkIdentifierExclusivity": true, "mqttIdentifierReleaseAllowed": true,
		"composeReplacement": true, "mqttComposeReplacement": true}
	fset := token.NewFileSet()
	for _, name := range []string{"outbound_transition.go", "outbound_custody.go", "outbound_replace_compose.go"} {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil || !forbidden[fd.Name.Name] {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == "mqttExchangeDeliveryDutyEnded" {
					t.Errorf("%s: %s reads the duty predicate", name, fd.Name.Name)
				}
				return true
			})
		}
	}
	if mqttExchangePhaseEnded(mqttExchangeDeliveryAbandoned) {
		t.Fatal("an abandoned delivery reads as an identifier-releasing ending")
	}
}

// T36. The gate admits from delivery_abandoned EXACTLY the two acknowledgement dispositions (unit
// C2, plan-unitC rev 3 §4.3: abandonment is absorbing for PUBLISH delivery, not for a client's
// acknowledgement) and refuses every other one, and the classifier holds it first.
//
// Mutation M36a: MarkRetired admitted from it. Mutation M36b: the classifier arm moved below
// the own-nonce arm. Mutation M-C11: any non-acknowledgement disposition admitted.
func TestF105AbandonedRegisterIsClosedToEveryDisposition(t *testing.T) {
	t.Parallel()
	for d := mqttSlotDeleteComplete; d <= mqttSlotAdvanceAcknowledgedTerminal; d++ {
		ack := d == mqttSlotAdvancePhase || d == mqttSlotAdvanceAcknowledgedTerminal
		if got := mqttInflightTransitionAllowed(d, mqttInflightDeliveryAbandoned); got != ack {
			t.Errorf("disposition %d from delivery_abandoned: admitted %v, want %v", d, got, ack)
		}
	}
	nonce := mqttNewInflightNonce()
	rec := &mqttInflightRecord{PI: 3, Dur: "SRC", Seq: 1, QoS: 1, Nonce: nonce, Lin: nonce,
		SourceAckReply: testInflightAckSubject("SRC", 1, 1),
		Claim:          "c", State: mqttInflightDeliveryAbandoned, AbandonReason: mqttAbandonReasonACLDeny, AbandonTenure: "t"}
	v := mqttClassifyInflightSlot(mqttSlotObservation{resp: mqttSlotSubjectPresent, occupant: rec, seq: 9,
		subjectNamesOccupant: true, occupantIsOurs: true}, mqttAllocationIntent{nonce: nonce})
	if v.disposition != mqttSlotDispAbandonedHeld {
		t.Fatalf("the classifier answered %d for an abandoned register with our own nonce", v.disposition)
	}
}

// T37. A register-only replacement keeps the continued obligation's acquisition history, and
// the veto sees the acquisition through it.
//
// Mutation M37a: the composer's carry removed -> the replacement is refused.
// Mutation M37b: the boundary rule removed while a forged batch resets Acq -> it lands.
func TestF105RegisterOnlyReplacementKeepsItsAcquisitionHistory(t *testing.T) {
	t.Parallel()
	e, subject, tok, pi := testAcquireEnv(t, "abandon-acq-carry")
	if !e.sess.acquirePublishAction(e.c, pi, &tok) || !e.sess.drainPublishAction(e.c, tok) {
		t.Fatal("fixture: acquire and drain under the current tenure")
	}
	// The send's intent delete: an authoritatively absent marker is what makes the occupant
	// POSSIBLY ATTEMPTED, the population a register-only replacement continues.
	ml := e.asm.jsa.loadOccupantMarkerLoad(subject)
	if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, subject, ml.seq, true); err != nil {
		t.Fatalf("deleting the marker: %v", err)
	}
	cur, curSeq := testAbandonRegisterAt(t, e, subject)
	auth, _, _ := e.asm.jsa.servingAuthority(e.sess.id, e.asm.sessStreamEpoch)
	fresh := *cur
	fresh.Nonce = mqttNewInflightNonce()
	fresh.Acq = 0
	// A FORGED batch that resets the counter is refused at the boundary (M37b), whatever the
	// composer does.
	authSeq := uint64(0)
	if _, aseq, aerr := e.asm.jsa.servingAuthority(e.sess.id, e.asm.sessStreamEpoch); aerr == nil {
		authSeq = aseq
	}
	ftxn := mqttNewInflightNonce()
	forged, ferr := mqttReplacePublishTransition(&mqttReplacingAdmission{
		Auth: auth, AuthSeq: authSeq, Txn: ftxn, Op: "op-" + ftxn, Mode: mqttReplaceRegisterOnly,
		RegSubj: subject, Occupant: cur, OccupantSeq: curSeq, Register: &fresh,
	})
	if ferr != nil {
		t.Fatalf("composing the forged replacement: %v", ferr)
	}
	for i, m := range forged.Extra {
		if m.subj != subject {
			continue
		}
		var reg mqttInflightRecord
		if err := json.Unmarshal(m.body, &reg); err != nil {
			t.Fatal(err)
		}
		reg.Acq = 0
		body, _ := json.Marshal(&reg)
		forged.Extra[i].body = body
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(forged); cerr == nil ||
		!strings.Contains(cerr.Error(), "keeps its transmission history") {
		t.Fatalf("a register-only replacement resetting the counter was answered %v", cerr)
	}
	if _, err := e.asm.jsa.opReplacePublish(&mqttReplacePublishRequest{
		ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-replace-publish",
		Gen: auth.Gen, Tenure: auth.Tenure, RegisterOnly: true,
		Occupant: cur, OccupantSeq: curSeq, Register: &fresh,
	}); err != nil {
		t.Fatalf("the register-only replacement must land: %v", err)
	}
	after, _ := testAbandonRegisterAt(t, e, subject)
	if after.Nonce != fresh.Nonce || after.Acq != cur.Acq {
		t.Fatalf("after the replacement: nonce %q acq %d, want %q and the occupant's %d", after.Nonce, after.Acq, fresh.Nonce, cur.Acq)
	}
	// The deny now meets the continued obligation: the entry names the new allocation, so the
	// only thing that can refuse is the veto, reading the carried acquisition.
	e.sess.mu.Lock()
	e.sess.pendingPublish[pi].regNonce = fresh.Nonce
	e.sess.pendingPublish[pi].lin = after.Lin
	if e.sess.cpending[testRealPubRelSrcDur] == nil {
		e.sess.cpending[testRealPubRelSrcDur] = map[uint64]uint16{}
	}
	e.sess.cpending[testRealPubRelSrcDur][testRealPubRelSrcSeq] = pi
	e.sess.mu.Unlock()
	if r := e.sess.releaseTransmittedIdentifier(e.c, testRealPubRelSrcDur,
		testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 2)); r != mqttAbandonPending {
		t.Fatalf("the deny after a register-only replacement returned %v: the current-tenure acquisition "+
			"was lost with the counter", r)
	}
	if _, verr := e.sess.abandonCurrentOutputVeto(testAbandonEvidence(t, e, pi, subject)); !errors.Is(verr, errMQTTAbandonCurrentOutput) {
		t.Fatalf("after the replacement the refusal is not the current-connection veto: %v", verr)
	}
}

func testAbandonRegisterAt(t *testing.T, e *testInflightEnv, subject string) (*mqttInflightRecord, uint64) {
	t.Helper()
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("no register on %q: %v", subject, err)
	}
	return rec, smsg.Sequence
}

// T38. An abandoned exchange's duty resolves (the classifier and the ending boundary both read
// the delivery-duty predicate) while its identifier stays held.
//
// Mutation M38: the classifier keeps its enumerated list -> Withheld.
func TestF105AbandonedExchangeDutyResolves(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-duty", 1)
	if r := f.deny(); r != mqttAbandonDone {
		t.Fatalf("deny: %v", r)
	}
	v := f.e.asm.jsa.classifyExchangeDuty(f.e.sess.id, f.e.asm.sessStreamEpoch, f.gen,
		&mqttDutyEntry{Kind: mqttDutyExchange, Ref: f.lin})
	if v.Disposition != mqttDutyMayResolve {
		t.Fatalf("the abandoned exchange's duty classified %v (%s), want MayResolve", v.Disposition, v.Why)
	}
	a := &mqttTransitionAttempt{ID: f.e.sess.id, Epoch: f.e.asm.sessStreamEpoch, Txn: "t"}
	if err := f.e.asm.jsa.checkDutyEndingIsEarned(a, f.gen, &mqttDutyEntry{Kind: mqttDutyExchange, Ref: f.lin}, nil); err != nil {
		t.Fatalf("the boundary refused the abandoned exchange's duty ending: %v", err)
	}
	f.e.sess.mu.Lock()
	held := f.e.sess.custody.holdsPI(f.pi)
	f.e.sess.mu.Unlock()
	if !held {
		t.Fatal("the identifier was released with the duty")
	}
}

// T39/T40/T41. The ledger's split: a stale Publish read or census never recharges an abandoned
// entry; an uncertain subject resolving to DeliveryAbandoned becomes an identifier-only entry;
// replay counts nothing for an identifier-only, non-custody member.
//
// M39: chargeReleased cleared by a live read. M40: the uncertainty dropped with no entry.
// M41: identifier ownership counted as a charge in replay.
func TestF105CustodySplitIsMonotoneAndComplete(t *testing.T) {
	t.Parallel()
	gen := mqttNewSessGen()
	l := newMQTTCustodyLedger(gen)
	testCustodyEnsureExchange(l, gen, "a", 4)
	at, _ := l.beginObservation(gen, "a")
	if !l.applyExchangeObservation(gen, "a", at, &mqttOutboundExchange{Nonce: "a", PI: 4, QoS: 1, Phase: mqttExchangeDeliveryAbandoned}) {
		t.Fatal("observing the abandonment did not release the charge")
	}
	at2, _ := l.beginObservation(gen, "a")
	l.applyExchangeObservation(gen, "a", at2, &mqttOutboundExchange{Nonce: "a", PI: 4, QoS: 1, Phase: mqttExchangePublish})
	l.mergeCensus(gen, []*mqttOutboundExchange{{Nonce: "a", PI: 4, QoS: 1, Phase: mqttExchangePublish}}, nil, true)
	if l.charges() != 0 || !l.holdsPI(4) || l.chargesPI(4) {
		t.Fatalf("T39: charges=%d holds=%v chargesPI=%v after stale reads", l.charges(), l.holdsPI(4), l.chargesPI(4))
	}
	l.mergeCensus(gen, nil, []string{"subj-b"}, true)
	l.resolveUncertain(gen, "subj-b", &mqttOutboundExchange{Nonce: "b", PI: 5, QoS: 1, Phase: mqttExchangeDeliveryAbandoned})
	if !l.holdsPI(5) || l.chargesPI(5) || l.charges() != 0 {
		t.Fatalf("T40: holds=%v chargesPI=%v charges=%d", l.holdsPI(5), l.chargesPI(5), l.charges())
	}
	s := &mqttSession{gen: gen, maxp: 100, rmax: 1, custody: l,
		pendingPublish: map[uint16]*mqttPending{4: {}}, pendingPubRel: map[uint16]*mqttPending{}}
	if a := s.replayAllowanceLocked(map[uint16]*mqttPending{4: s.pendingPublish[4]}); a != 1 {
		t.Fatalf("T41: an identifier-only member inflated the replay allowance to %d", a)
	}
}

// testAbandonEvidence builds the deny evidence for the entry resident on pi, as the deny would.
func testAbandonEvidence(t *testing.T, e *testInflightEnv, pi uint16, subject string) mqttDenyEvidence {
	t.Helper()
	ent, ok := e.c.outboundEntitlement()
	if !ok {
		t.Fatal("the connection carries no entitlement")
	}
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	p := e.sess.pendingPublish[pi]
	if p == nil {
		t.Fatalf("no entry on %d", pi)
	}
	return mqttDenyEvidence{Gen: e.sess.gen, Tenure: ent.Tenure, Subject: subject, Dur: p.jsDur, Seq: p.sseq,
		Nonce: p.regNonce, Lin: p.lin, PI: pi, Reason: mqttAbandonReasonACLDeny,
		Reply: testInflightAckSubject(p.jsDur, p.sseq, 2)}
}

// T43 (consult 35). A restored delivery a drain already CLAIMS is excluded from abandonment: it
// keeps the pre-row-5 disposal, its exchange stays Publish, and the ledger holds its charge and
// identifier — live, with the drain known only through sess.drains, and restored from the record
// before the deny; and across a further recovery after the register is gone.
//
// Mutation M43a: the exclusion removed -> an abandonment is written.
// Mutation M43b: the lookup reads the delivery index (drainByDur) only -> the drains-only case
// is abandoned.
func TestF105DrainClaimedDeliveryIsNotAbandoned(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		drainsOnly bool
		drainQoS   byte
	}{{name: "live", drainQoS: 1}, {name: "drains-only", drainsOnly: true, drainQoS: 1}, {name: "qos0-drain", drainQoS: 0}} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := testAbandonAdmitted(t, "abandon-drain-"+tc.name, 1)
			sess := f.e.sess
			sess.mu.Lock()
			sess.restoreDrainLocked("sid-"+tc.name, mqttDrainFromPersisted(&mqttPersistedDrain{
				Cons: &ConsumerConfig{Durable: testRealPubRelSrcDur}, QoS: tc.drainQoS,
				Started: []mqttPersistedDrainSeq{{Seq: f.sseq, PI: f.pi}},
			}))
			if tc.drainsOnly {
				delete(sess.drainByDur, testRealPubRelSrcDur)
			}
			st := sess.drainSeqLocked(testRealPubRelSrcDur, f.sseq)
			sess.mu.Unlock()
			if st == nil || !st.claimed {
				t.Fatal("fixture: the drain must claim the restored delivery")
			}
			if r := f.deny(); r != mqttAbandonNone {
				t.Fatalf("a drain-claimed delivery's deny returned %v, want the pre-row-5 disposal", r)
			}
			testAbandonRequireHeldNotAbandoned(t, f)
		})
	}
	t.Run("restored-before-deny", func(t *testing.T) {
		t.Parallel()
		f := testAbandonAdmitted(t, "abandon-drain-cold", 1)
		restored, _, err := f.e.asm.createOrRestoreSession(f.e.sess.id, f.e.s.getOpts())
		if err != nil || restored == f.e.sess {
			t.Fatalf("createOrRestoreSession: %v", err)
		}
		restored.restoreDrainsFromRecord(&mqttPersistedSession{Drains: map[string]*mqttPersistedDrain{
			"sid-cold": {Cons: &ConsumerConfig{Durable: testRealPubRelSrcDur}, QoS: 1,
				Started: []mqttPersistedDrainSeq{{Seq: f.sseq, PI: f.pi}}},
		}})
		if _, err := restored.recoverInflightRegisters(f.e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		testCustodyRM1(t, restored)
		restored.mu.Lock()
		restored.c = f.e.c
		restored.mu.Unlock()
		if r := restored.releaseTransmittedIdentifier(f.e.c, testRealPubRelSrcDur, f.reply); r != mqttAbandonNone {
			t.Fatalf("a record-restored drain claim was abandoned: %v", r)
		}
		if x := f.exchange(t); x.Phase != mqttExchangePublish {
			t.Fatalf("the exchange moved to %d", x.Phase)
		}
	})
}

func testAbandonRequireHeldNotAbandoned(t *testing.T, f *testAbandonFix) {
	t.Helper()
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("a drain-claimed delivery's exchange moved to phase %d", x.Phase)
	}
	if cur, _ := f.register(t); cur != nil && cur.State == mqttInflightDeliveryAbandoned {
		t.Fatal("a drain-claimed delivery's register was abandoned")
	}
	if c := testCustodyCapacity(f.e.sess); c != 0 {
		t.Fatalf("the Publish exchange's charge was released: capacity %d", c)
	}
	if got := testCustodyMints(f.e.sess, f.pi); got == f.pi {
		t.Fatalf("the Publish exchange's identifier %d was handed out", f.pi)
	}
	// And across a further recovery, with its register gone: the ledger still holds it.
	restored, _, err := f.e.asm.createOrRestoreSession(f.e.sess.id, f.e.s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}
	testCustodyRM1(t, restored)
	if _, err := restored.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if !testCustodyHasExchange(restored, f.lin) || testCustodyCapacity(restored) != 0 {
		t.Fatal("after recovery the Publish exchange no longer holds its charge")
	}
}

// T44. While an abandonment is in flight, no drain may claim the delivery.
//
// Mutation M44: drainClaimEligibleLocked ignores the in-flight mark -> eligible.
func TestF105AbandoningDeliveryIsNotDrainEligible(t *testing.T) {
	t.Parallel()
	gen := mqttNewSessGen()
	s := &mqttSession{gen: gen, custody: newMQTTCustodyLedger(gen),
		pendingPublish: map[uint16]*mqttPending{3: {abandonInFlight: true}}, pendingPubRel: map[uint16]*mqttPending{}}
	if _, eligible, knownPubRel := s.drainClaimEligibleLocked(3); eligible || knownPubRel {
		t.Fatalf("a delivery whose abandonment is in flight was eligible=%v knownPubRel=%v for a drain claim",
			eligible, knownPubRel)
	}
}

// T32c. The ACTUAL delivery callback, re-offering an abandoned source under an ALLOWING identity
// (no authorization configured): it is suppressed — no identifier allocated, no entry tracked.
//
// Mutation M32d: the callback's suppression check removed -> a fresh identifier is allocated.
func TestF105AbandonedSourceReofferIsSuppressedAtTheCallback(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-callback", 1)
	if r := f.deny(); r != mqttAbandonDone {
		t.Fatalf("deny: %v", r)
	}
	sess := f.e.sess
	sess.mu.Lock()
	sess.c = f.e.c
	sess.rmax = 5
	before := len(sess.pendingPublish)
	lastPI := sess.last_pi
	sess.mu.Unlock()
	hdr := []byte("NATS/1.0\r\n" + mqttNatsHeader + ":1\r\n\r\n")
	rmsg := append(append([]byte(nil), hdr...), "abandoned"...)
	pc := &client{kind: JETSTREAM}
	pc.pa.hdr = len(hdr)
	sub := &subscription{client: f.e.c, mqtt: &mqttSub{qos: 1, jsDur: testRealPubRelSrcDur}}
	mqttDeliverMsgCbQoS12(sub, pc, nil, mqttStreamSubjectPrefix+"foo.bar", f.reply, rmsg)
	sess.mu.Lock()
	after := len(sess.pendingPublish)
	afterPI := sess.last_pi
	_, mapped := sess.cpending[testRealPubRelSrcDur][f.sseq]
	sess.mu.Unlock()
	if after != before || afterPI != lastPI || mapped {
		t.Fatalf("the abandoned source was re-offered into a delivery: entries %d->%d, last_pi %d->%d, mapped=%v",
			before, after, lastPI, afterPI, mapped)
	}
}

func testAbandonDrainEligible(sess *mqttSession, pi uint16) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	_, eligible, _ := sess.drainClaimEligibleLocked(pi)
	return eligible
}

// T45 (H-2). The drain fence is held for the whole abandonment and ends only where it is
// settled: (a) in flight the delivery is not drain-eligible, and a settled abandonment leaves no
// entry to claim; (b) an abandonment whose landing is UNKNOWN keeps the fence until a later
// attempt settles it; (c) a second deny meeting an attempt in flight writes nothing.
//
// Mutation M45a: the in-flight mark not set -> eligible while the write is in flight.
// Mutation M45b: an unresolved outcome lifts the fence -> eligible after it.
// Mutation M45c: a second deny ignores the in-flight mark -> two writes.
func TestF105AbandonmentFencesDrainStagingUntilSettled(t *testing.T) {
	t.Parallel()
	t.Run("in-flight-then-settled", func(t *testing.T) {
		t.Parallel()
		f := testAbandonAdmitted(t, "abandon-fence-settled", 1)
		var eligibleInFlight atomic.Bool
		f.e.asm.mu.Lock()
		f.e.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			out, err := write()
			eligibleInFlight.Store(testAbandonDrainEligible(f.e.sess, f.pi))
			return out, err
		}
		f.e.asm.mu.Unlock()
		if r := f.deny(); r != mqttAbandonDone {
			t.Fatalf("deny: %v", r)
		}
		if eligibleInFlight.Load() {
			t.Fatal("the delivery was drain-eligible while its abandonment was in flight")
		}
		f.e.sess.mu.Lock()
		_, resident := f.e.sess.pendingPublish[f.pi]
		f.e.sess.mu.Unlock()
		if resident {
			t.Fatal("a settled abandonment left its entry for a drain to claim")
		}
	})
	t.Run("unresolved-keeps-the-fence", func(t *testing.T) {
		t.Parallel()
		f := testAbandonAdmitted(t, "abandon-fence-unresolved", 1)
		var fault atomic.Bool
		fault.Store(true)
		hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt.Kind == mqttTransitionAbandonDelivery && fault.Load() {
				return 0, errors.New("test: the batch's response was lost")
			}
			return send()
		}
		f.e.asm.jsa.testTransitionCommit.Store(&hook)
		t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
		f.e.asm.mu.Lock()
		f.e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
			if fault.Load() {
				return mqttTransitionUnresolved, true
			}
			return 0, false
		}
		f.e.asm.mu.Unlock()
		if r := f.deny(); r != mqttAbandonPending {
			t.Fatalf("an unresolved abandonment returned %v to the deny, want pending", r)
		}
		if testAbandonDrainEligible(f.e.sess, f.pi) {
			t.Fatal("an abandonment whose landing is unknown lifted the drain fence")
		}
		fault.Store(false)
		if r := f.deny(); r != mqttAbandonDone {
			t.Fatalf("the settling retry returned %v", r)
		}
		f.e.sess.mu.Lock()
		_, resident := f.e.sess.pendingPublish[f.pi]
		f.e.sess.mu.Unlock()
		if resident {
			t.Fatal("the settled retry left the entry")
		}
	})
	t.Run("overlapping-deny", func(t *testing.T) {
		t.Parallel()
		f := testAbandonAdmitted(t, "abandon-fence-overlap", 1)
		entered := make(chan struct{})
		release := make(chan struct{})
		var writes atomic.Int32
		f.e.asm.mu.Lock()
		f.e.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if writes.Add(1) == 1 {
				close(entered)
				<-release
			}
			return write()
		}
		f.e.asm.mu.Unlock()
		done := make(chan mqttAbandonResult, 1)
		go func() { done <- f.deny() }()
		<-entered
		if r := f.deny(); r != mqttAbandonPending {
			close(release)
			t.Fatalf("a second deny during an abandonment in flight returned %v, want pending", r)
		}
		close(release)
		if r := <-done; r != mqttAbandonDone {
			t.Fatalf("the first deny returned %v", r)
		}
		if n := writes.Load(); n != 1 {
			t.Fatalf("%d abandonment writes for one delivery, want 1", n)
		}
	})
}

// T46 (H-3). An abandonment that COMMITTED behind a lost response is settled by its own handle,
// never by a later attempt's reads: a retry whose evidence read fails keeps the delivery fenced
// from drain staging, and the fence ends only when the earlier handle positively resolves.
//
// Mutation M46: the retry ignores the retained handles (the H-2 tree) -> the failed evidence
// read lifts the fence while the committed abandonment is still unsettled.
// Mutation M46b: a still-unknown retained handle is dropped -> the same.
// Mutation M46c: a LANDED retained handle is not taken as the answer -> the retry issues a new op.
func TestF105UnsettledAbandonmentOutlivesAFailedRetry(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "abandon-unsettled-retry", 1)
	var unresolved, readFault atomic.Bool
	unresolved.Store(true)
	var commits atomic.Int32
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind != mqttTransitionAbandonDelivery {
			return send()
		}
		commits.Add(1)
		if _, err := send(); err != nil {
			return 0, err
		}
		return 0, errors.New("test: the committed batch's response was lost")
	}
	f.e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
		if unresolved.Load() {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	var ops atomic.Int32
	f.e.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		ops.Add(1)
		return write()
	}
	f.e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if readFault.Load() && subject == f.subject {
			return nil, nil, errors.New("test: the register read failed")
		}
		return f.e.asm.jsa.loadInflightRecord(subject)
	}
	f.e.asm.mu.Unlock()

	if r := f.deny(); r != mqttAbandonPending {
		t.Fatalf("the committed-but-unresolved abandonment returned %v, want pending", r)
	}
	if n := commits.Load(); n != 1 {
		t.Fatalf("fixture: %d abandonment commits, want 1", n)
	}
	if x := f.exchange(t); x.Phase != mqttExchangeDeliveryAbandoned {
		t.Fatalf("fixture: the abandonment must have committed, exchange at phase %d", x.Phase)
	}
	if testAbandonDrainEligible(f.e.sess, f.pi) {
		t.Fatal("a committed abandonment with an unknown landing lifted the drain fence")
	}

	readFault.Store(true)
	if r := f.deny(); r == mqttAbandonDone {
		t.Fatalf("the faulted retry returned done")
	}
	if testAbandonDrainEligible(f.e.sess, f.pi) {
		t.Fatal("a retry's failed evidence read lifted the fence over an unsettled committed abandonment")
	}
	f.e.sess.mu.Lock()
	_, resident := f.e.sess.pendingPublish[f.pi]
	f.e.sess.mu.Unlock()
	if !resident {
		t.Fatal("fixture: the unsettled abandonment must leave its entry resident")
	}

	readFault.Store(false)
	unresolved.Store(false)
	opsBefore := ops.Load()
	if r := f.deny(); r != mqttAbandonDone {
		t.Fatalf("the settling retry returned %v", r)
	}
	if n := ops.Load() - opsBefore; n != 0 {
		t.Fatalf("the settling retry issued %d abandonment ops: the retained handle's landing IS the answer", n)
	}
	if n := commits.Load(); n != 1 {
		t.Fatalf("the settling retry wrote again: %d commits, want 1", n)
	}
	f.requireAbandoned(t)
	f.e.sess.mu.Lock()
	_, resident = f.e.sess.pendingPublish[f.pi]
	f.e.sess.mu.Unlock()
	if resident {
		t.Fatal("the settled abandonment left its entry")
	}
}
