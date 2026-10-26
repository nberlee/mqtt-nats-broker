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
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
)

// An unsent obligation is ended, not forgotten (F-105, the prerequisite of row 5; plan-withdraw.md
// rev 6 §6). Every test below starts from an ordinary obligation ADMITTED through the authority —
// an exchange at Publish beside its register and unsent marker — so the route under test is
// reached with the population it exists for.

type testWithdrawFix struct {
	e       *testInflightEnv
	pi      uint16
	subject string
	gen     string
	lin     string
	nonce   string
	sseq    uint64
}

// testWithdrawAdmitted admits one ordinary, never-transmitted obligation on the real store.
func testWithdrawAdmitted(t *testing.T, clientID string, qos byte) *testWithdrawFix {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, e)
	return testWithdrawAdmitMore(t, e, testRealPubRelSrcSeq, qos)
}

// testWithdrawAdmitMore admits a further obligation in the same env.
func testWithdrawAdmitMore(t *testing.T, e *testInflightEnv, sseq uint64, qos byte) *testWithdrawFix {
	t.Helper()
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, sseq, qos)
	e.sess.mu.Lock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	gen := e.sess.gen
	e.sess.mu.Unlock()
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || cur.State != mqttInflightPublish || cur.Lin == _EMPTY_ || mqttDurIsSlotOnlyClass(cur.Dur) {
		t.Fatalf("the fixture must be an ordinary admitted publish register: %+v (%v)", cur, err)
	}
	f := &testWithdrawFix{e: e, pi: pi, subject: subject, gen: gen, lin: cur.Lin, nonce: cur.Nonce, sseq: sseq}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("the fixture's obligation is not admitted at Publish: %+v", x)
	}
	if !f.markerStands() {
		t.Fatal("the fixture's unsent marker does not stand, so it is not a proven-unsent obligation")
	}
	return f
}

func (f *testWithdrawFix) exchange(t *testing.T) *mqttOutboundExchange {
	t.Helper()
	x, _, err := f.e.asm.jsa.loadOutboundExchange(f.e.sess.id, f.e.asm.sessStreamEpoch, f.gen, f.lin)
	if err != nil {
		t.Fatalf("loading the exchange of %q: %v", f.lin, err)
	}
	return x
}

func (f *testWithdrawFix) register(t *testing.T) (*mqttInflightRecord, uint64) {
	t.Helper()
	cur, smsg, err := f.e.asm.loadInflightRecord(f.subject)
	if err != nil {
		t.Fatalf("loading the register on %q: %v", f.subject, err)
	}
	if smsg == nil {
		return nil, 0
	}
	return cur, smsg.Sequence
}

func (f *testWithdrawFix) markerStands() bool {
	return mqttMarkerNamesAllocation(f.e.asm.jsa.loadOccupantMarkerLoad(f.subject), f.nonce)
}

func (f *testWithdrawFix) ref(t *testing.T) *mqttInflightSlotRef {
	t.Helper()
	cur, seq := f.register(t)
	if cur == nil {
		t.Fatal("no register to reference")
	}
	return mqttSlotRefFromRecord(f.subject, seq, cur)
}

// requireEnded asserts the obligation reached the withdrawal's durable ending and its register is
// gone.
func (f *testWithdrawFix) requireWithdrawnAndGone(t *testing.T) {
	t.Helper()
	if x := f.exchange(t); x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("the unsent obligation reads phase %d, want WithdrawnUnsent (%d)", x.Phase, mqttExchangeWithdrawnUnsent)
	}
	if cur, _ := f.register(t); cur != nil {
		t.Fatalf("the register survived its destruction: %+v", cur)
	}
}

// commitWithdrawal hands a hand-composed withdrawal batch to the write boundary, bypassing the op.
func (f *testWithdrawFix) commitWithdrawal(t *testing.T, mutate func(tr *mqttAuthorityTransition)) error {
	t.Helper()
	jsa := &f.e.asm.jsa
	auth, authSeq, err := jsa.requireAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
	if err != nil || auth == nil {
		t.Fatalf("no authority: %v", err)
	}
	reg, regSeq := f.register(t)
	x, xSeq, xerr := jsa.loadOutboundExchange(f.e.sess.id, f.e.asm.sessStreamEpoch, f.gen, f.lin)
	if reg == nil || xerr != nil {
		t.Fatalf("the fixture lost its register or exchange: %v", xerr)
	}
	txn := mqttNewInflightNonce()
	tr, terr := mqttWithdrawUnsentTransition(auth, authSeq, txn, "op-"+txn, f.subject, reg, regSeq, x, xSeq)
	if terr != nil {
		t.Fatalf("composing the withdrawal: %v", terr)
	}
	if mutate != nil {
		mutate(tr)
	}
	_, cerr := jsa.commitAuthorityTransition(tr)
	return cerr
}

// testRewriteAuthority rewrites the standing authority directly (a takeover, a closure), at its
// exact sequence, with the revision advanced.
func testRewriteAuthority(t *testing.T, e *testInflightEnv, mutate func(a *mqttOutboundAuthority)) {
	t.Helper()
	auth, seq, err := e.asm.jsa.requireAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if err != nil || auth == nil {
		t.Fatalf("no authority to rewrite: %v", err)
	}
	next := *auth
	next.Rev++
	next.Txn = mqttNewInflightNonce()
	mutate(&next)
	b, _ := json.Marshal(&next)
	msg, hdr := mqttFrameBatchWrite(seq, b, _EMPTY_, 0, false)
	if _, serr := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(e.sess.id)), hdr, msg); serr != nil {
		t.Fatalf("rewriting the authority: %v", serr)
	}
}

// T1/T2 (route: the shared proven-unsent disposal both Message Expiry and the oversize disposal
// use). The obligation is ended before its register is destroyed.
//
// Mutation M1: endUnsentBeforeDestroy disabled -> the exchange stays Publish.
func TestF105WithdrawProvenUnsentDisposalEndsTheObligation(t *testing.T) {
	t.Parallel()
	for _, qos := range []byte{1, 2} {
		f := testWithdrawAdmitted(t, "withdraw-drop-q"+string('0'+qos), qos)
		if removed := f.e.sess.dropProvenUnsentPublishEntry(f.pi, mqttTestLicence(f.e.sess, nil)); removed == nil {
			t.Fatalf("QoS %d: the disposal found no entry to remove", qos)
		}
		f.requireWithdrawnAndGone(t)
	}
}

// T2 (oversize order). The oversize disposal drains the section's acquisition BEFORE it disposes,
// so the withdrawal meets no token; a withdrawal attempted under the live token is refused.
func TestF105WithdrawOversizeAfterTheDrainEndsTheObligation(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "withdraw-oversize", 2)
	f := &testWithdrawFix{e: s.e, pi: s.pi, subject: s.subject}
	cur, _ := f.register(t)
	f.gen, f.lin, f.nonce = s.tok.slot.name.gen, cur.Lin, cur.Nonce
	s.acquire(t)
	if got := s.e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(s.e.sess, nil), nil); got != mqttWithdrawNoEvidence {
		t.Fatalf("a withdrawal under the section's live token reported %v, want no evidence", got)
	}
	s.e.sess.drainPublishAction(s.e.c, s.tok)
	s.tok.actionHeld = false
	if removed := s.e.sess.dropProvenUnsentPublishEntry(s.pi, mqttTestLicence(s.e.sess, nil)); removed == nil {
		t.Fatal("the oversize disposal found no entry")
	}
	f.requireWithdrawnAndGone(t)
}

// T3 (both door entrances). The gated entrance (reapRegisterWithVerdictGated, used by the
// teardown's verdict reap and deleteInflightRegisterFor) and the loaded entrance
// (reapObservedRegister / reapLoadedRegister, used by admission's reaps and directly by the drain
// finalizer's releaseBoundInflightRegister) both end the obligation first.
//
// Mutation M3: the step present only in reapRegisterGatedOutcome -> the loaded case fails.
func TestF105WithdrawAtBothDoorEntrances(t *testing.T) {
	t.Parallel()
	t.Run("gated", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "withdraw-door-gated", 1)
		if !f.e.sess.reapRegisterWithVerdictGated(f.ref(t).seq, f.nonce, true, f.ref(t), mqttSlotDeleteNeverTransmitted, mqttTestLicence(f.e.sess, nil)) {
			t.Fatal("the gated door refused a proven-unsent destruction")
		}
		f.requireWithdrawnAndGone(t)
	})
	t.Run("loaded", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "withdraw-door-loaded", 2)
		cur, seq := f.register(t)
		if !f.e.sess.reapObservedRegister(f.e.c, f.subject, seq, cur, mqttSlotDeleteNeverTransmitted) {
			t.Fatal("the loaded door refused a proven-unsent destruction")
		}
		f.requireWithdrawnAndGone(t)
	})
}

// T3 (the cancellation fallback). A cancellation's tombstone, for an ordinary obligation with its
// evidence standing, is the withdrawal's: the exchange ends in the same batch.
func TestF105WithdrawCancellationTombstoneEndsTheObligation(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-cancel-tomb", 1)
	_, seq := f.register(t)
	out, err := f.e.sess.markInflightRegisterCancelled(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq, seq, f.nonce, mqttTestLicence(f.e.sess, nil))
	if err != nil || out != mqttRegCancelStored {
		t.Fatalf("the cancellation did not store: %v (%v)", out, err)
	}
	if x := f.exchange(t); x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("the cancelled obligation reads phase %d, want WithdrawnUnsent", x.Phase)
	}
	if cur, _ := f.register(t); cur == nil || !cur.Cancelled {
		t.Fatalf("the register is not the tombstone: %+v", cur)
	}
}

// The cancellation fallback WITHOUT the marker evidence stores the standalone tombstone exactly as
// before and leaves the exchange at Publish (consult 32): the ordinary real-store equivalent of the
// placeholder rig's "a landed CAS is STORED" arm.
func TestF105CancellationWithoutEvidenceStoresTheStandaloneTombstone(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-cancel-noev", 1)
	ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
		t.Fatalf("deleting the marker: %v", err)
	}
	_, seq := f.register(t)
	out, err := f.e.sess.markInflightRegisterCancelled(f.e.c, f.pi, testRealPubRelSrcDur, f.sseq, seq, f.nonce, mqttTestLicence(f.e.sess, nil))
	if err != nil || out != mqttRegCancelStored {
		t.Fatalf("the cancellation did not store: %v (%v)", out, err)
	}
	if cur, _ := f.register(t); cur == nil || !cur.Cancelled {
		t.Fatalf("the register is not the standalone tombstone: %+v", cur)
	}
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("a cancellation without marker evidence ended the obligation at phase %d", x.Phase)
	}
}

// T4b (boundary, token). A hand-composed batch over a token-bearing predecessor is refused.
// Regression-only: the withdrawal rule's token clause and checkActionTokenMembers each refuse;
// M4 removes both (a double mutation).
func TestF105WithdrawBoundaryRefusesALiveToken(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-bnd-token", 1)
	cur, seq := f.register(t)
	tok := *cur
	tok.ActionToken, tok.ActionClaim, tok.ActionClaimSeq = mqttNewInflightNonce(), cur.Claim, cur.ClaimSeq
	if _, err := f.e.asm.storeInflightCAS(f.subject, seq, &tok); err != nil {
		t.Fatalf("installing the fixture token: %v", err)
	}
	testRequireRefusal(t, f.commitWithdrawal(t, nil), "output token", "installs an output-custody token")
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("the refused withdrawal moved the exchange to %d", x.Phase)
	}
}

// T5 (boundary, marker). A hand-composed, otherwise valid batch is refused when the marker of the
// allocation is absent, names another allocation, or cannot be read.
//
// Mutation M5: the boundary's marker rule removed -> all three land.
func TestF105WithdrawBoundaryValidatesTheMarkerItself(t *testing.T) {
	t.Parallel()
	t.Run("absent", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "withdraw-bnd-mk-absent", 1)
		ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
		if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
			t.Fatalf("deleting the marker: %v", err)
		}
		testRequireRefusal(t, f.commitWithdrawal(t, nil), "no unsent marker naming")
	})
	t.Run("other-allocation", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "withdraw-bnd-mk-other", 1)
		testRewriteMarker(t, f, mqttNewInflightNonce())
		testRequireRefusal(t, f.commitWithdrawal(t, nil), "no unsent marker naming")
	})
	t.Run("unreadable", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "withdraw-bnd-mk-bad", 1)
		testRewriteMarkerRaw(t, f, []byte("{not a marker"))
		testRequireRefusal(t, f.commitWithdrawal(t, nil), "no unsent marker naming")
	})
}

func testRewriteMarker(t *testing.T, f *testWithdrawFix, nonce string) {
	t.Helper()
	b, err := mqttBuildInflightMarker(nonce)
	if err != nil {
		t.Fatalf("building a marker: %v", err)
	}
	testRewriteMarkerRaw(t, f, b)
}

func testRewriteMarkerRaw(t *testing.T, f *testWithdrawFix, body []byte) {
	t.Helper()
	ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	msg, hdr := mqttFrameBatchWrite(ml.seq, body, _EMPTY_, 0, false)
	if _, err := f.e.asm.jsa.storeMsg(f.subject+"."+mqttInflightMarkerSuffix, hdr, msg); err != nil {
		t.Fatalf("rewriting the marker: %v", err)
	}
}

// T6 (op-level interleaving, regression-only). The writer acquires between the op's evidence reads
// and its commit: the register moved, the commit is refused, and the door refuses rather than
// deleting a register that now carries a live holder.
func TestF105WithdrawAcquisitionInTheReadToCommitWindowWins(t *testing.T) {
	t.Parallel()
	s := testRow2OpenSection(t, "withdraw-window", 1)
	f := &testWithdrawFix{e: s.e, pi: s.pi, subject: s.subject, gen: s.tok.slot.name.gen}
	cur, seq := f.register(t)
	f.lin, f.nonce = cur.Lin, cur.Nonce
	var fired atomic.Bool
	s.e.asm.mu.Lock()
	s.e.asm.testWithdrawAfterEvidence = func() {
		if fired.CompareAndSwap(false, true) {
			s.acquire(t)
		}
	}
	s.e.asm.mu.Unlock()
	if s.e.sess.reapObservedRegister(s.e.c, f.subject, seq, cur, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("the door deleted a register whose holder acquired inside the withdrawal's window")
	}
	if !fired.Load() {
		t.Fatal("the window seam never fired, so nothing here is tested")
	}
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("the exchange moved to %d although its PUBLISH may now be written", x.Phase)
	}
	if after, _ := f.register(t); after == nil || after.ActionToken == _EMPTY_ {
		t.Fatalf("the acquiring holder's register is gone or tokenless: %+v", after)
	}
}

// T7 (synthetic boundary coverage, nonce binding). The standing marker names a DIFFERENT
// allocation of the same obligation; the withdrawal is refused by op and boundary alike.
//
// Mutation M7: mqttMarkerNamesAllocation accepts any nonce -> lands.
func TestF105WithdrawRequiresTheMarkerOfThisAllocation(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-nonce", 1)
	testRewriteMarker(t, f, mqttNewInflightNonce())
	if got := f.e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(f.e.sess, nil), nil); got != mqttWithdrawNoMarker {
		t.Fatalf("the op answered %v for another allocation's marker, want no marker", got)
	}
	testRequireRefusal(t, f.commitWithdrawal(t, nil), "no unsent marker naming")
}

// T8a/T8b/T8c. The withdrawal is the SERVING connection's decision (F-105 7b, plan-7b §3.3: an
// owner-dependent mutation, converted in 7b), and it keeps the generation exception: an earlier
// generation's unsent obligation is withdrawn by the connection the later generation serves,
// changing nothing on the authority but its revision. Under a CLOSED authority no connection's
// entitlement serves, so nothing is withdrawn (no production route withdraws for a generation
// that is no longer served: plan §3.4's Closure deviation); a G1 connection's stale licence is
// refused. A batch that changes the authority's state is refused.
//
// Mutations: M8a (the entitlement comparison removed -> the closed and stale withdrawals land),
// M8b (the generation exception not extended -> G2's withdrawal of G1's obligation is refused),
// M8c (the withdrawal arm skips the shared authority-preservation rule).
func TestF105WithdrawUnderTheStandingAuthority(t *testing.T) {
	t.Parallel()
	t.Run("closed", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "withdraw-closed", 1)
		testRewriteAuthority(t, f.e, func(a *mqttOutboundAuthority) { a.State = mqttAuthorityClosed })
		if got := f.e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(f.e.sess, nil), nil); got != mqttWithdrawUnlicensed {
			t.Fatalf("a withdrawal under a closed authority must be refused as unlicensed, reported %v", got)
		}
		auth, _, _ := f.e.asm.jsa.requireAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
		if auth.State != mqttAuthorityClosed {
			t.Fatalf("the refused withdrawal moved the authority to state %d", auth.State)
		}
		if x := f.exchange(t); x.Phase != mqttExchangePublish {
			t.Fatalf("phase %d, want the obligation untouched at Publish", x.Phase)
		}
	})
	t.Run("displaced-generation", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "withdraw-g1", 1)
		stale := mqttTestLicence(f.e.sess, nil)
		var g2, tenure string
		testRewriteAuthority(t, f.e, func(a *mqttOutboundAuthority) {
			a.Gen, a.Tenure, a.OwnerTxn = mqttNewSessGen(), mqttNewInflightNonce(), a.Txn
			g2, tenure = a.Gen, a.Tenure
		})
		if got := f.e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, stale, nil); got != mqttWithdrawUnlicensed {
			t.Fatalf("G1's stale connection must be refused, reported %v", got)
		}
		serving := mqttEntitlementLicence(mqttOutboundEntitlement{Gen: g2, Tenure: tenure}, true)
		if got := f.e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, serving, nil); got != mqttWithdrawLanded {
			t.Fatalf("a G1 withdrawal by the connection G2 serves reported %v", got)
		}
		auth, _, _ := f.e.asm.jsa.requireAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
		if auth.Gen != g2 || auth.Tenure != tenure || auth.State != mqttAuthorityServing {
			t.Fatalf("the withdrawal moved G2's authority: %+v", auth)
		}
		if x := f.exchange(t); x.Phase != mqttExchangeWithdrawnUnsent {
			t.Fatalf("phase %d, want WithdrawnUnsent", x.Phase)
		}
	})
	t.Run("state-changing-body", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "withdraw-closing-body", 1)
		err := f.commitWithdrawal(t, func(tr *mqttAuthorityTransition) {
			tr.Auth.State = mqttAuthorityClosed
			tr.Auth.Intent, tr.Auth.Creation = nil, nil
		})
		testRequireRefusal(t, err, "moves the authority from state")
	})
}

// T9 (structural). The only production callers of reapRegisterWithVerdict are the door's two
// entrances, and the only other register deletes are the generation-level ones, each for a stated
// reason.
//
// Mutation M9: a direct reapRegisterWithVerdict call added elsewhere -> this fails.
func TestF105WithdrawDestructionHasOneDoor(t *testing.T) {
	t.Parallel()
	callers := testProductionCallers(t, map[string]bool{
		"reapRegisterWithVerdict": true, "deleteInflightRegister": true, "deleteInflightRegisterConfirmed": true,
		"deleteInflightRegistersPipelined": true,
	})
	want := map[string][]string{
		// The door: both entrances, after endUnsentBeforeDestroy.
		"reapRegisterWithVerdict": {"reapLoadedRegister", "reapRegisterGatedOutcome"},
		// The door's own delete, and two generation-level deletes: the orphan sweep reconciles
		// registers of generations no session references, and the settled affine register is
		// slot-only (7a).
		"deleteInflightRegister": {"disposeSettledAffineRegister", "reapRegisterWithVerdict", "reconcileOrphanInflight"},
		// The door's confirmed delete (gated or not: F-088 WP3 rule 6 (c) threads the
		// exclusion-reclaim continuation's dispatch gate THROUGH the door, never beside it), and the
		// incarnation reap's pipelined delete of a discarded generation.
		"deleteInflightRegisterConfirmed":  {"deleteInflightRegistersPipelined", "reapRegisterWithVerdict"},
		"deleteInflightRegistersPipelined": {"reapIncarnationRegisters"},
	}
	for callee, fns := range want {
		got := callers[callee]
		if strings.Join(got, ",") != strings.Join(fns, ",") {
			t.Errorf("%s is called from %v, want exactly %v: a new caller destroys an ordinary register "+
				"without passing the door that ends its obligation", callee, got, fns)
		}
	}
}

// testProductionCallers maps each named callee to the sorted, de-duplicated names of the non-test
// package functions that call it (by selector or plain name).
func testProductionCallers(t *testing.T, callees map[string]bool) map[string][]string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	seen := map[string]map[string]bool{}
	for _, fn := range files {
		if strings.HasSuffix(fn, "_test.go") {
			continue
		}
		src, rerr := os.ReadFile(fn)
		if rerr != nil {
			t.Fatal(rerr)
		}
		file, perr := parser.ParseFile(fset, fn, src, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", fn, perr)
		}
		for _, d := range file.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				ce, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var name string
				switch f := ce.Fun.(type) {
				case *ast.SelectorExpr:
					name = f.Sel.Name
				case *ast.Ident:
					name = f.Name
				}
				if callees[name] {
					if seen[name] == nil {
						seen[name] = map[string]bool{}
					}
					seen[name][fd.Name.Name] = true
				}
				return true
			})
		}
	}
	out := map[string][]string{}
	for callee, set := range seen {
		for fn := range set {
			out[callee] = append(out[callee], fn)
		}
		sort.Strings(out[callee])
	}
	return out
}

// T10. A crash after the withdrawal landed and before the delete leaves the tombstone beside the
// ended exchange; the next destruction finishes it through the door ("already ended").
//
// Mutation M10: the door refuses an already-ended exchange -> the tombstone is left.
func TestF105WithdrawLandedBeforeACrashIsFinishedByTheNextPass(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-crash", 1)
	if got := f.e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(f.e.sess, nil), nil); got != mqttWithdrawLanded {
		t.Fatalf("the withdrawal reported %v", got)
	}
	cur, seq := f.register(t)
	if cur == nil || !cur.Cancelled {
		t.Fatalf("the landed withdrawal left no tombstone: %+v", cur)
	}
	// "Crash": nothing deleted it. The next pass destroys it through the door.
	if !f.e.sess.reapObservedRegister(f.e.c, f.subject, seq, cur, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("the next pass refused to destroy a withdrawn obligation's tombstone")
	}
	f.requireWithdrawnAndGone(t)
}

// T11a. A withdrawal whose landing cannot be resolved refuses the destruction: register kept.
//
// Mutation M11a: UNRESOLVED treated as proceed -> the register is destroyed with its ending unknown.
func TestF105WithdrawUnresolvedRefusesTheDestruction(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-unresolved", 1)
	f.e.asm.mu.Lock()
	f.e.asm.testWithdrawUnsent = func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		// A handle whose outcome can never be read: the resolver answers UNRESOLVED, and the abort
		// it attempts cannot be written for it either.
		return mqttOpOutcome{Handle: &mqttTransitionHandle{}}, errors.New("test: the response was lost")
	}
	f.e.asm.mu.Unlock()
	cur, seq := f.register(t)
	if f.e.sess.reapObservedRegister(f.e.c, f.subject, seq, cur, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("the door destroyed a register whose withdrawal is unresolved")
	}
	if after, _ := f.register(t); after == nil {
		t.Fatal("the register is gone although its ending was never established")
	}
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("phase %d, want Publish", x.Phase)
	}
}

// T11b. A positively aborted attempt is re-read and re-attempted once.
//
// Mutation M11b: no retry -> the door refuses a still-eligible obligation.
func TestF105WithdrawNeverLandedIsReattempted(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-never", 1)
	var calls atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testWithdrawUnsent = func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if calls.Add(1) == 1 {
			// Never sent: build the handle the op would have, and report the send failed.
			jsa := &f.e.asm.jsa
			auth, authSeq, _ := jsa.requireAuthority(req.ID, req.Epoch)
			reg, rmsg, _ := jsa.loadInflightRecord(req.Subject)
			x, xSeq, _ := jsa.loadOutboundExchange(req.ID, req.Epoch, req.Gen, reg.Lin)
			tr, err := mqttWithdrawUnsentTransition(auth, authSeq, req.Txn, req.Op, req.Subject, reg, rmsg.Sequence, x, xSeq)
			if err != nil {
				t.Errorf("composing: %v", err)
				return mqttOpOutcome{}, err
			}
			h, _ := mqttNewTransitionHandle(tr)
			return mqttOpOutcome{Handle: h}, errors.New("test: the send failed")
		}
		return write()
	}
	f.e.asm.mu.Unlock()
	cur, seq := f.register(t)
	if !f.e.sess.reapObservedRegister(f.e.c, f.subject, seq, cur, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("the door gave up on a never-landed withdrawal whose evidence still stands")
	}
	if calls.Load() != 2 {
		t.Fatalf("the withdrawal was attempted %d times, want 2", calls.Load())
	}
	f.requireWithdrawnAndGone(t)
}

// T11c. A withdrawal that LANDED while its response was lost, and was then superseded by a later
// authority revision, is re-observed by identity: our tombstone is destroyed; a successor that has
// since taken the subject is left alone.
//
// Mutation M11d: a superseded withdrawal deletes whatever occupies the subject -> (ii) fails.
func TestF105WithdrawSupersededIsReobservedByIdentity(t *testing.T) {
	t.Parallel()
	successorNonce := mqttNewInflightNonce()
	run := func(t *testing.T, clientID string, successor bool) (*testWithdrawFix, bool) {
		f := testWithdrawAdmitted(t, clientID, 1)
		other := testWithdrawAdmitMore(t, f.e, testRealPubRelSrcSeq+7, 1)
		f.e.asm.mu.Lock()
		f.e.asm.testWithdrawUnsent = func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if req.Subject != f.subject {
				return write()
			}
			out, err := write()
			if err != nil {
				return out, err
			}
			// A later authority revision lands before this caller resolves: another obligation's
			// withdrawal.
			if got := f.e.sess.withdrawUnsent(other.gen, other.subject, other.nonce, other.lin, mqttTestLicence(f.e.sess, nil), nil); got != mqttWithdrawLanded {
				t.Errorf("the superseding withdrawal reported %v", got)
			}
			if successor {
				tomb, seq := f.register(t)
				succ := *tomb
				succ.Cancelled, succ.Nonce, succ.Lin = false, successorNonce, successorNonce
				succ.CancelOrigin = _EMPTY_ // F-088: an uncancelled successor carries no provenance
				if _, serr := f.e.asm.storeInflightCAS(f.subject, seq, &succ); serr != nil {
					t.Errorf("installing the successor: %v", serr)
				}
			}
			return out, errors.New("test: the response was lost")
		}
		f.e.asm.mu.Unlock()
		cur, seq := f.register(t)
		return f, f.e.sess.reapObservedRegister(f.e.c, f.subject, seq, cur, mqttSlotDeleteNeverTransmitted)
	}
	t.Run("our-tombstone", func(t *testing.T) {
		f, ok := run(t, "withdraw-sup-ours", false)
		if !ok {
			t.Fatal("the door refused although our superseded withdrawal landed")
		}
		f.requireWithdrawnAndGone(t)
	})
	t.Run("successor", func(t *testing.T) {
		f, ok := run(t, "withdraw-sup-succ", true)
		if ok {
			t.Fatal("the door reported destroying a subject a successor now occupies")
		}
		if cur, _ := f.register(t); cur == nil || cur.Nonce != successorNonce {
			t.Fatalf("the successor was destroyed: %+v", cur)
		}
	})
}

// T12e. A withdrawal's register body keeps the occupant whole: one that changes its acquisition
// counter is refused. (T12a-d are refused by overlapping fences and are regression-only.)
//
// Mutation M12e: the body rule dropped -> lands.
func TestF105WithdrawBoundaryKeepsTheOccupantWhole(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-bnd-body", 1)
	err := f.commitWithdrawal(t, func(tr *mqttAuthorityTransition) {
		for i, m := range tr.Extra {
			if _, perr := mqttParseInflightSubject(m.subj); perr != nil {
				continue
			}
			rec, derr := mqttDecodeInflightRecord(m.body)
			if derr != nil {
				t.Fatalf("decoding the tombstone: %v", derr)
			}
			rec.Acq++
			b, _ := json.Marshal(rec)
			tr.Extra[i].body = b
		}
	})
	testRequireRefusal(t, err, "changes more than its cancellation")
	t.Run("regression-only", func(t *testing.T) {
		g := testWithdrawAdmitted(t, "withdraw-bnd-noreg", 1)
		testRequireRefusal(t, g.commitWithdrawal(t, func(tr *mqttAuthorityTransition) { tr.Extra = tr.Extra[1:] }),
			"register(s) and")
	})
}

// T13 (regression-only). Nothing is acquired at a withdrawn obligation.
func TestF105WithdrawnObligationAdmitsNoAcquisition(t *testing.T) {
	t.Parallel()
	for _, k := range []mqttActionKind{mqttActionPublish, mqttActionPubRel} {
		if mqttActionPermittedAtPhase(2, mqttExchangeWithdrawnUnsent, k) == nil {
			t.Fatalf("a %q may be acquired at a withdrawn obligation", k)
		}
	}
}

// T14. A tombstone from a pre-existing cancellation, beside a Publish exchange whose marker still
// stands, is withdrawn when it is destroyed.
//
// Mutation M14: a tombstone predecessor refused by the withdrawal -> the exchange stays Publish.
func TestF105WithdrawAPreexistingCancellationTombstone(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-pre-tomb", 1)
	cur, seq := f.register(t)
	tomb := *cur
	tomb.Cancelled, tomb.CancelOrigin = true, mqttCancelOriginOccupant // F-088: an occupant CAS
	nseq, err := f.e.asm.storeInflightCAS(f.subject, seq, &tomb)
	if err != nil {
		t.Fatalf("writing the pre-existing tombstone: %v", err)
	}
	if !f.e.sess.reapObservedRegister(f.e.c, f.subject, nseq, &tomb, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("the door refused the tombstone's destruction")
	}
	f.requireWithdrawnAndGone(t)
}

// T15. Admission is pinned to the connection's OWN accepted entitlement: after a takeover rewrote
// the authority's tenure, a delivery on the displaced connection is refused and admits nothing.
//
// Mutation M15: the serving authority read restored at the admission site -> it admits under the
// successor's tenure.
func TestF105AdmissionIsPinnedToTheConnectionsEntitlement(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "admit-pin", 1)
	testRewriteAuthority(t, f.e, func(a *mqttOutboundAuthority) {
		a.Tenure, a.OwnerTxn = mqttNewInflightNonce(), a.Txn
	})
	e := f.e
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, f.sseq+11, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d new=%v", pi, isNew)
	}
	if e.sess.admitInflightPublish(e.c, pi, 1, testRealPubRelSrcDur, f.sseq+11, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatal("the displaced connection admitted a delivery under its successor's tenure")
	}
	e.sess.mu.Lock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	e.sess.mu.Unlock()
	if cur, smsg, _ := e.asm.loadInflightRecord(subject); smsg != nil {
		t.Fatalf("the refused admission left a register: %+v", cur)
	}
}

// T16 (consult 32's seam; re-expressed by F-088). A cancellation tombstone WITHOUT the marker
// evidence is no destruction licence: the door preserves the register and the exchange stays at
// Publish, which still refuses a competing durable admission on its identifier, and no withdrawal
// is manufactured.
//
// Mutation M16 (double): the op's marker check AND the boundary's marker rule removed -> the
// exchange is withdrawn without evidence.
func TestF105WithdrawNeverManufacturedWithoutEvidence(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-no-evidence", 1)
	ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
		t.Fatalf("deleting the marker: %v", err)
	}
	cur, seq := f.register(t)
	tomb := *cur
	tomb.Cancelled, tomb.CancelOrigin = true, mqttCancelOriginOccupant // F-088: an occupant CAS
	nseq, err := f.e.asm.storeInflightCAS(f.subject, seq, &tomb)
	if err != nil {
		t.Fatalf("writing the tombstone: %v", err)
	}
	// RE-EXPRESSED by F-097/F-088 (plan rev 20, seven-red-tests table). This test asserted that the
	// door destroys the register "on the tombstone's own proof", the deleted premise: a cancellation
	// bit is a decision, not a proof of non-transmission, and with the marker gone the PUBLISH may
	// be on the wire. What it pinned and still pins is the other half: no withdrawal is ever
	// manufactured without marker evidence. The expected result is now PRESERVATION of both the
	// register (the only carrier of what a late acknowledgement completes) and the Publish exchange.
	if f.e.sess.reapObservedRegister(f.e.c, f.subject, nseq, &tomb, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("the door destroyed a possibly-attempted register on a cancellation bit alone")
	}
	if after, smsg := f.register(t); after == nil || smsg != nseq || after.Nonce != tomb.Nonce {
		t.Fatalf("the register must survive at sequence %d, got %+v at %d", nseq, after, smsg)
	}
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("an obligation with no marker evidence was ended at phase %d", x.Phase)
	}
	// The surviving Publish exchange still holds its identifier against a durable admission.
	auth, _, _ := f.e.asm.jsa.requireAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
	competitor := mqttNewInflightNonce()
	rec := &mqttInflightRecord{
		Dur: testRealPubRelSrcDur, Seq: f.sseq + 99, PI: f.pi, QoS: 1, State: mqttInflightPublish,
		Nonce: competitor, Lin: competitor, Claim: tomb.Claim, ClaimSeq: tomb.ClaimSeq,
		SourceAckReply: testInflightAckSubject(testRealPubRelSrcDur, f.sseq+99, 1),
	}
	if _, aerr := f.e.asm.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: f.e.sess.id, Epoch: f.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-admit-publish",
		Gen: auth.Gen, Tenure: auth.Tenure, Register: rec,
	}); aerr == nil || !strings.Contains(aerr.Error(), "still holds") {
		t.Fatalf("a competing admission was not refused by the identifier the Publish exchange still holds: %v", aerr)
	}
}

// T17. Our own admission landed, its response was lost, and another continuation withdrew the
// obligation before the ladder re-read: the landed write is not adopted. The entry is abandoned
// (its sequence deferred), the tombstone is destroyed through the door, and nothing is confirmed
// for an exchange no acquisition may serve.
//
// Mutation M17: the adopt arm ignores the tombstone -> the ended write is adopted and confirmed.
func TestF105OwnWriteEndedIsNotAdopted(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		lost bool
	}{
		// The admission's response is lost: the ladder re-reads and meets our own tombstone.
		{name: "lost-response", lost: true},
		// H-1 f2: the response ARRIVES, and the withdrawal lands before the register read-back —
		// the read-back must not confirm a tombstone as a settlement.
		{name: "successful-response", lost: false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testOwnWriteEnded(t, "withdraw-own-ended-"+tc.name, tc.lost)
		})
	}
}

func testOwnWriteEnded(t *testing.T, clientID string, lost bool) {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, e)
	// A first admission provisions the source and the authority the second one needs.
	testWithdrawAdmitMore(t, e, testRealPubRelSrcSeq, 1)
	sseq := testRealPubRelSrcSeq + 3
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, sseq, 1))
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	gen := e.sess.gen
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish: pi=%d new=%v", pi, isNew)
	}
	admNonce := e.nonceOf(pi)
	var withdrew atomic.Bool
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind != mqttTransitionAdmitExchange || withdrew.Load() {
			return send()
		}
		seq, err := send()
		if err != nil {
			return 0, err
		}
		withdrew.Store(true)
		if got := e.sess.withdrawUnsent(gen, subject, admNonce, admNonce, mqttTestLicence(e.sess, nil), nil); got != mqttWithdrawLanded {
			t.Errorf("the concurrent withdrawal reported %v", got)
		}
		if lost {
			return 0, errors.New("test: the admission's response was lost")
		}
		return seq, nil
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionCommit.Store(nil) })
	if e.sess.admitInflightPublish(e.c, pi, 1, testRealPubRelSrcDur, sseq, admNonce, _EMPTY_, _EMPTY_) {
		t.Fatal("the ladder adopted a landed write whose obligation had since been ended")
	}
	if !withdrew.Load() {
		t.Fatal("the fixture never withdrew the landed admission, so nothing here is tested")
	}
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[pi]
	_, deferred := e.sess.deferred[mqttDeferredKey{testRealPubRelSrcDur, sseq}]
	e.sess.mu.Unlock()
	if ack != nil {
		t.Fatalf("an entry stands for the ended obligation: %+v", ack)
	}
	if !deferred {
		t.Fatal("the abandoned allocation's source was not deferred for re-offer")
	}
	if cur, smsg, _ := e.asm.loadInflightRecord(subject); smsg != nil {
		t.Fatalf("the ended obligation's tombstone was not destroyed: %+v", cur)
	}
}

// T20 (H-1 f1). The withdrawal ends the allocation its caller validated, or nothing: a caller
// holding a stale view (allocation N while another now stands on the subject) must not end the
// obligation that stands there, whatever evidence that one has.
//
// Mutation M20: the op's allocation binding removed -> the standing obligation is withdrawn.
func TestF105WithdrawEndsOnlyTheValidatedAllocation(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-stale-caller", 1)
	stale := mqttNewInflightNonce()
	for _, v := range []struct{ nonce, lin string }{{stale, f.lin}, {f.nonce, stale}, {stale, stale}} {
		if got := f.e.sess.withdrawUnsent(f.gen, f.subject, v.nonce, v.lin, mqttTestLicence(f.e.sess, nil), nil); got != mqttWithdrawNoEvidence {
			t.Fatalf("a withdrawal validated for %s/%s ended what stands on the subject: %v", v.nonce, v.lin, got)
		}
	}
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("the standing obligation was ended at phase %d by a caller that validated another", x.Phase)
	}
	if cur, _ := f.register(t); cur == nil || cur.Cancelled || cur.Nonce != f.nonce {
		t.Fatalf("the standing register was touched: %+v", cur)
	}
}

// T19 (regression-only). Under the acquire-action kind a tombstone register member is refused.
func TestF105WithdrawTombstoneIsTheWithdrawalKindsOnly(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "withdraw-kind", 1)
	err := f.commitWithdrawal(t, func(tr *mqttAuthorityTransition) {
		tr.Attempt.Kind = mqttTransitionAcquireAction
		tr.Extra = tr.Extra[:1]
	})
	testRequireRefusal(t, err, "not a token acquisition", "cancellation tombstone")
}

// testRequireRefusal requires err to be a refusal carrying one of the named reasons: a batch
// refused for some other reason proves nothing about the rule under test.
func testRequireRefusal(t *testing.T, err error, reasons ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the batch landed; want a refusal naming one of %q", reasons)
	}
	for _, r := range reasons {
		if strings.Contains(err.Error(), r) {
			return
		}
	}
	t.Fatalf("refused for another reason than %q: %v", reasons, err)
}
