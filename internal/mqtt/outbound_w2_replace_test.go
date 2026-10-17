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
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Same-identifier replacements through the production admission (F-105 WP-F W2, rows C7 and C8).
//
// A same-delivery re-offer that finds its own earlier allocation on the identifier replaces it:
// a PAIR replacement over a proven-unsent occupant admits a fresh obligation and supersedes the
// occupant's, and a REGISTER-ONLY replacement over a possibly-attempted occupant continues the
// occupant's obligation under a fresh transmission nonce. Both are reached the way T9
// (TestF105CustodyPairReplacementMovesOneCharge) reaches them: the successor's scheduling entry is
// installed on the occupant's identifier, which is the state the ladder meets on a re-offer.

// testW2InstallSuccessorEntry puts the re-offered allocation's scheduling entry on the occupant's
// identifier and returns its nonce.
func testW2InstallSuccessorEntry(f *testWithdrawFix) string {
	sess := f.e.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.rmax, sess.maxp = 2, 100
	delete(sess.pendingPublish, f.pi)
	nonce := mqttNewInflightNonce()
	sess.pendingPublish[f.pi] = &mqttPending{jsDur: testRealPubRelSrcDur, sseq: f.sseq,
		jsAckSubject: testInflightAckSubject(testRealPubRelSrcDur, f.sseq, 2), regPending: true, regNonce: nonce}
	return nonce
}

// testW2SourceDeps is the dependency set the source consumer's resource carries now.
func testW2SourceDeps(t *testing.T, e *testInflightEnv) []mqttResourceEdge {
	t.Helper()
	own, _, err := e.sess.jsa.loadNameOwnershipPhysical(e.asm.sessStreamEpoch, mqttOwnedStreamMsgs, testRealPubRelSrcDur)
	if err != nil || own == nil {
		t.Fatalf("reading the source's name ownership: %+v (%v)", own, err)
	}
	return testLoadResource(t, e, own.OwnerGen, own.OwnerInc).Deps
}

func testW2Charges(sess *mqttSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.custody.charges()
}

// testW2AcquireAndDrain takes one real PUBLISH action on the fixture's register and gives it
// back, so the occupant carries acquisition HISTORY (Acq > 0) and no live action.
func testW2AcquireAndDrain(t *testing.T, f *testWithdrawFix) {
	t.Helper()
	e := f.e
	testStampAcceptedConnection(t, e)
	cur, seq := f.register(t)
	if cur == nil {
		t.Fatal("fixture: no register to acquire on")
	}
	ref := mqttSlotRefFromRecord(f.subject, seq, cur)
	if ref == nil {
		t.Fatal("fixture: the register yields no slot reference")
	}
	token := mqttNewInflightNonce()
	tok := mqttSendToken{token: token, slot: *ref, out: testPublishOutputFor(e, *ref, token)}
	if !e.sess.acquirePublishAction(e.c, f.pi, &tok) || !e.sess.drainPublishAction(e.c, tok) {
		t.Fatal("fixture: acquire and drain under the current tenure")
	}
}

// testW2PossiblyAttempted makes the fixture's occupant POSSIBLY ATTEMPTED - the send's intent
// delete of its unsent marker - the population a register-only replacement continues.
func testW2PossiblyAttempted(t *testing.T, f *testWithdrawFix) {
	t.Helper()
	ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
		t.Fatalf("deleting the occupant's marker: %v", err)
	}
	if f.markerStands() {
		t.Fatal("fixture: the occupant's marker still stands, so it is not possibly attempted")
	}
}

// testW2LandedLineage is the lineage the landed settlement installed on the identifier's entry.
func testW2LandedLineage(sess *mqttSession, pi uint16) (lin, nonce string, ok bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	p := sess.pendingPublish[pi]
	if p == nil {
		return _EMPTY_, _EMPTY_, false
	}
	return p.ev.lin, p.ev.nonce, true
}

// TestF105RegisterOnlyReplacementChargesOnce: a register-only replacement CONTINUES the occupant's
// obligation. It writes the heir register under a fresh transmission nonce on the occupant's
// lineage and acquisition history, and nothing else: the occupant's exchange is the obligation, so
// it stands unchanged at PUBLISH, no second exchange is created for the heir's nonce, the custody
// ledger keeps exactly the one charge it held, no resource takes a new edge, and the landed
// settlement names the lineage the durable register carries.
//
// The occupant has acquired and drained an action, so its Acq is NONZERO (an inherited 0 would make
// the acquisition-history check vacuous) and no live ActionToken stands to refuse the replacement
// on its own; both are preconditions.
//
// Mutations (C8, W3 plan section 6): L1 mqttLineageForWrite's register-only Lin returns the nonce
// (L1b with the boundary's lineage rule disabled), target heir.Lin == E1; L2 its register-only Acq
// returns 0 (L2b with the boundary's acquisition rule disabled), target heir.Acq == occupant.Acq;
// H1 the orchestrator hands custody another lineage, target custody.charges() == 1 and no charge at
// the heir's nonce; H2 the composer's agreement refusal removed and the ladder's derivation
// bypassed, target one charge on E1 and the settlement lineage equal to the durable register's.
func TestF105RegisterOnlyReplacementChargesOnce(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "w2-register-only", 1)
	e, sess := f.e, f.e.sess
	testW2AcquireAndDrain(t, f)
	occupant, _ := f.register(t)
	if occupant == nil || occupant.Lin != f.lin {
		t.Fatalf("fixture: no occupant register on lineage %q: %+v", f.lin, occupant)
	}
	// PRECONDITIONS: acquisition history, and no live action.
	if occupant.Acq == 0 {
		t.Fatalf("fixture: the occupant carries no acquisition history (Acq 0), so the inherited-Acq check is vacuous: %+v", occupant)
	}
	if occupant.ActionToken != _EMPTY_ {
		t.Fatalf("fixture: the occupant holds a live action %q, which refuses the replacement on its own", occupant.ActionToken)
	}
	testW2PossiblyAttempted(t, f)
	x0, x0seq, err := e.asm.jsa.loadOutboundExchange(sess.id, e.asm.sessStreamEpoch, f.gen, f.lin)
	if err != nil || x0.Phase != mqttExchangePublish {
		t.Fatalf("fixture: E1 is not at PUBLISH: %+v (%v)", x0, err)
	}
	deps0 := testW2SourceDeps(t, e)
	if len(deps0) == 0 {
		t.Fatal("fixture: the source carries no edge, so the dependency assertion is vacuous")
	}
	nonce := testW2InstallSuccessorEntry(f)
	if c := testW2Charges(sess); c != 1 {
		t.Fatalf("fixture: %d charges before the replacement, want the occupant's one", c)
	}

	var mu sync.Mutex
	var modes []bool
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(_ *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		mu.Lock()
		modes = append(modes, registerOnly)
		mu.Unlock()
		return write()
	}
	e.asm.mu.Unlock()

	if !sess.admitInflightPublish(e.c, f.pi, 1, testRealPubRelSrcDur, f.sseq, nonce, _EMPTY_, _EMPTY_) {
		t.Fatal("the register-only replacement was refused")
	}
	// ROUTE WITNESS: the replacement arm wrote register-only, and that was the ladder's last
	// write. (The ladder's first attempt is the create-only admission, which the occupant's
	// register refuses; registerOnly is false there and says nothing about the replacement.)
	mu.Lock()
	last := len(modes) > 0 && modes[len(modes)-1]
	got := append([]bool(nil), modes...)
	mu.Unlock()
	if !last {
		t.Fatalf("fixture: the admission's writes were %v, want a register-only replacement to have landed last", got)
	}

	heir, _ := f.register(t)
	if heir == nil || heir.PI != f.pi {
		t.Fatalf("no heir register on identifier %d: %+v", f.pi, heir)
	}
	if heir.Nonce != nonce || heir.Nonce == occupant.Nonce {
		t.Fatalf("the heir's transmission nonce is %q, want the fresh %q (occupant %q)", heir.Nonce, nonce, occupant.Nonce)
	}
	// TARGET (L1): the heir continues E1.
	if heir.Lin != f.lin {
		t.Fatalf("the register-only heir is on lineage %q, want the continued obligation E1 %q", heir.Lin, f.lin)
	}
	// TARGET (L2): and E1's acquisition history, nonzero by precondition.
	if heir.Acq != occupant.Acq {
		t.Fatalf("the heir's acquisition history is %d, want the occupant's %d", heir.Acq, occupant.Acq)
	}
	x1, x1seq, err := e.asm.jsa.loadOutboundExchange(sess.id, e.asm.sessStreamEpoch, f.gen, f.lin)
	if err != nil || x1.Nonce != x0.Nonce || x1.Phase != mqttExchangePublish || x1seq != x0seq {
		t.Fatalf("E1 changed under a register-only replacement: %+v at %d (was %+v at %d) err=%v", x1, x1seq, x0, x0seq, err)
	}
	if xm, xerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttExchangeSubject(sess.idHash, f.gen, heir.Nonce)); xm != nil ||
		!IsNatsErr(xerr, JSNoMessageFoundErr) {
		t.Fatalf("an exchange was created for the heir's transmission nonce %q: %+v (%v)", heir.Nonce, xm, xerr)
	}
	// TARGET (H1, H2): one charge, E1's.
	if c := testW2Charges(sess); c != 1 {
		t.Fatalf("the register-only replacement left %d custody charges, want exactly E1's one", c)
	}
	sess.mu.Lock()
	heirCharged := sess.custody.hasExchange(f.gen, heir.Nonce)
	sess.mu.Unlock()
	if heirCharged {
		t.Fatalf("the custody ledger charges the heir's transmission nonce %q as an exchange", heir.Nonce)
	}
	if !testCustodyHasExchange(sess, f.lin) {
		t.Fatal("the continued obligation E1 lost its charge")
	}
	if deps1 := testW2SourceDeps(t, e); len(deps1) != len(deps0) {
		t.Fatalf("the source's dependency set moved from %v to %v under a register-only replacement", deps0, deps1)
	}
	// TARGET (H2): the landed settlement names the lineage the durable register carries.
	if lin, ln, ok := testW2LandedLineage(sess, f.pi); !ok || lin != heir.Lin || ln != heir.Nonce {
		t.Fatalf("the landed settlement installed lineage %q / nonce %q (entry present %v), want the durable register's %q / %q",
			lin, ln, ok, heir.Lin, heir.Nonce)
	}
}

// TestF105FailedRegisterOnlyAttemptRetriesOnAFreshLineage: a register-only attempt that FAILS sends
// the ladder back to a create, and the create starts a FRESH obligation - its lineage is its own
// nonce, never the occupant's that the failed attempt carried (W3 plan section 6, L4).
//
// The register-only attempt is refused at the boundary because the occupant's register is removed
// under it, so the reread finds the subject EMPTY while the occupant's exchange E1 still stands at
// PUBLISH: exactly the state in which a stale inherited lineage would name E1 from a create. Every
// create the ladder composes afterwards is read at the commit seam, where its register member is
// what the batch would write.
//
// Mutations: L4 the ladder's reset arm keeps the stale occupant's Lin - opAdmitPublish's lineage
// agreement refuses before composing, so no create reaches the seam. L4b, the same with that
// agreement removed, does NOT reach the seam either: the stale-lineage create names E1's
// dependency edge, which already stands on the source resource, so mqttResourceTakeDependency
// refuses it by edge identity ("dependency source-replay:... already stands on resource") before
// any commit. This test therefore does not kill L4b; the lineage property on a raw create is
// killed by TestMQTTFailedInheritingReplacementDoesNotCarryItsLineage (inflight_lineage_test.go),
// on the shared ladder's create-only fallback.
func TestF105FailedRegisterOnlyAttemptRetriesOnAFreshLineage(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "w2-l4-retry", 1)
	e, sess := f.e, f.e.sess
	testW2PossiblyAttempted(t, f)
	occupant, occSeq := f.register(t)
	if occupant == nil || occupant.Lin != f.lin {
		t.Fatalf("fixture: no occupant register on lineage %q: %+v", f.lin, occupant)
	}
	nonce := testW2InstallSuccessorEntry(f)

	type createSeen struct{ lin, nonce string }
	var mu sync.Mutex
	var refused, inRegisterOnly bool
	var refusal error
	var subjectEmpty, e1Standing bool
	var creates []createSeen
	var createErrs []string
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(_ *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		mu.Lock()
		first, after := registerOnly && !refused, refused
		mu.Unlock()
		if !first {
			out, err := write()
			if after && !registerOnly && err != nil {
				mu.Lock()
				createErrs = append(createErrs, err.Error())
				mu.Unlock()
			}
			return out, err
		}
		// The occupant's register goes, so the replacement - which CASes at the occupant's
		// observed sequence and is re-derived by the boundary from the occupant it reads - fails.
		if derr := e.asm.jsa.deleteMsg(mqttInflightStreamName, occSeq, true); derr != nil {
			return mqttOpOutcome{}, derr
		}
		mu.Lock()
		inRegisterOnly = true
		mu.Unlock()
		out, err := write()
		cur, _, lerr := e.asm.loadInflightRecord(f.subject)
		x, _, xerr := e.asm.jsa.loadOutboundExchange(sess.id, e.asm.sessStreamEpoch, f.gen, f.lin)
		mu.Lock()
		inRegisterOnly, refused, refusal = false, true, err
		subjectEmpty = lerr == nil && cur == nil
		e1Standing = xerr == nil && x != nil && x.Phase == mqttExchangePublish
		mu.Unlock()
		return out, err
	}
	e.asm.mu.Unlock()
	testSetTransitionCommit(t, e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		mu.Lock()
		after := refused && !inRegisterOnly
		mu.Unlock()
		if !after || tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionAdmitExchange {
			return send()
		}
		for _, m := range tr.Extra {
			if m.subj != f.subject || m.expect != 0 {
				continue
			}
			var reg mqttInflightRecord
			if err := json.Unmarshal(m.body, &reg); err != nil {
				t.Errorf("decoding the create's register member: %v", err)
				break
			}
			mu.Lock()
			creates = append(creates, createSeen{lin: reg.Lin, nonce: reg.Nonce})
			mu.Unlock()
		}
		return send()
	})

	admitted := sess.admitInflightPublish(e.c, f.pi, 1, testRealPubRelSrcDur, f.sseq, nonce, _EMPTY_, _EMPTY_)

	mu.Lock()
	defer mu.Unlock()
	// ROUTE WITNESS: the register-only attempt was reached and failed, leaving an empty subject
	// beside a standing E1.
	if !refused || refusal == nil {
		t.Fatalf("fixture: no register-only attempt was refused (reached %v, err %v)", refused, refusal)
	}
	if !subjectEmpty || !e1Standing {
		t.Fatalf("fixture: after the refused attempt the subject was empty=%v and E1 standing=%v, want both", subjectEmpty, e1Standing)
	}
	// TARGET: the ladder reset to a create, and every create carried a FRESH lineage.
	if len(creates) == 0 {
		t.Fatalf("no create reached the commit after the refused register-only attempt (%v); the creates were answered %q",
			refusal, createErrs)
	}
	for _, c := range creates {
		if c.nonce != nonce || c.lin != c.nonce {
			t.Fatalf("a create after the failed register-only attempt carries lineage %q for nonce %q (occupant E1 %q), want its own nonce %q",
				c.lin, c.nonce, f.lin, nonce)
		}
	}
	// E1 still holds the identifier, so the fresh obligation cannot be admitted beside it, and
	// nothing the ladder did charged a second obligation.
	if admitted {
		t.Fatal("a create was admitted on an identifier the unended E1 still holds")
	}
	if xm, xerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttExchangeSubject(sess.idHash, f.gen, nonce)); xm != nil ||
		!IsNatsErr(xerr, JSNoMessageFoundErr) {
		t.Fatalf("an exchange was created for %q beside the standing E1: %+v (%v)", nonce, xm, xerr)
	}
	sess.mu.Lock()
	charges, heirCharged := sess.custody.charges(), sess.custody.hasExchange(f.gen, nonce)
	sess.mu.Unlock()
	if charges != 1 || heirCharged {
		t.Fatalf("after the ladder: %d charges (want E1's one), heir nonce charged=%v", charges, heirCharged)
	}
	t.Logf("route: %d create attempt(s) after the refused register-only attempt (%v), answered %q", len(creates), refusal, createErrs)
}

// TestF105RegisterOnlyLostResponseChargesOnce: a register-only replacement whose commit LANDS but
// whose response is lost - an error WITH its handle - is still charged once, on the continued
// obligation E1 (W3 plan section 6, H3). The attempt is recorded with the lineage the ladder froze
// and settled later, and that lineage, not the heir's nonce, is what custody charges.
//
// The two settlement paths are run separately and named: "immediate" is recordAdmissionAttempt's
// own settlement; "reconciled" holds every resolution unresolved through the abort, so only the
// custody reconciler's replay of the STORED attempt can settle it.
//
// Mutation H3: the ambiguous attempt record stores the heir's nonce as its lineage. Target: exactly
// one charge, on E1, and none at the heir's nonce.
func TestF105RegisterOnlyLostResponseChargesOnce(t *testing.T) {
	for _, path := range []string{"immediate", "reconciled"} {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			f := testWithdrawAdmitted(t, "w2-h3-"+path, 1)
			e, sess := f.e, f.e.sess
			testW2AcquireAndDrain(t, f)
			testW2PossiblyAttempted(t, f)
			occupant, _ := f.register(t)
			if occupant == nil || occupant.Lin != f.lin || occupant.Acq == 0 || occupant.ActionToken != _EMPTY_ {
				t.Fatalf("fixture: want a quiescent occupant on E1 %q with acquisition history: %+v", f.lin, occupant)
			}
			nonce := testW2InstallSuccessorEntry(f)

			var inRegisterOnly, lost, handled atomic.Bool
			var fault atomic.Bool
			fault.Store(path == "reconciled")
			e.asm.mu.Lock()
			e.asm.testAdmitWrite = func(_ *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				inRegisterOnly.Store(registerOnly)
				out, err := write()
				inRegisterOnly.Store(false)
				if registerOnly && err != nil && out.Handle != nil {
					handled.Store(true)
				}
				return out, err
			}
			e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
				if fault.Load() {
					return mqttTransitionUnresolved, true
				}
				return 0, false
			}
			e.asm.mu.Unlock()
			testSetTransitionCommit(t, e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				if !inRegisterOnly.Load() || lost.Load() || tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionAdmitExchange {
					return send()
				}
				seq, err := send()
				if err != nil {
					return seq, err
				}
				lost.Store(true)
				return 0, errors.New("test: the register-only replacement's response was lost")
			})

			if !sess.admitInflightPublish(e.c, f.pi, 1, testRealPubRelSrcDur, f.sseq, nonce, _EMPTY_, _EMPTY_) {
				t.Fatal("the register-only replacement whose response was lost was not adopted")
			}
			if !lost.Load() || !handled.Load() {
				t.Fatalf("fixture: the landed register-only commit lost its response=%v, with a handle=%v", lost.Load(), handled.Load())
			}
			if path == "reconciled" {
				// Nothing has settled the stored attempt yet: its reservation stands.
				if !testCustodyHasAdmission(sess, nonce) {
					t.Fatal("fixture: the unresolved register-only attempt was settled before the reconciler ran")
				}
				fault.Store(false)
				testCustodyKick(sess)
				testCustodyEventually(t, "the reconciler settled the stored register-only attempt", func() bool {
					return !testCustodyHasAdmission(sess, nonce)
				})
				t.Logf("path: the custody reconciler's replay of the stored attempt consumed its lineage")
			} else {
				if testCustodyHasAdmission(sess, nonce) {
					t.Fatal("the immediately resolvable attempt was left for the reconciler")
				}
				t.Logf("path: recordAdmissionAttempt's immediate settlement consumed the attempt's lineage")
			}

			heir, _ := f.register(t)
			if heir == nil || heir.Nonce != nonce || heir.Lin != f.lin || heir.Acq != occupant.Acq {
				t.Fatalf("the landed heir is %+v, want nonce %q on E1 %q with the occupant's Acq %d", heir, nonce, f.lin, occupant.Acq)
			}
			// TARGET: exactly one charge, E1's; none at the heir's nonce.
			sess.mu.Lock()
			charges, heirCharged := sess.custody.charges(), sess.custody.hasExchange(f.gen, nonce)
			sess.mu.Unlock()
			if charges != 1 || heirCharged {
				t.Fatalf("the lost-response register-only replacement left %d charges (want E1's one), heir nonce charged=%v", charges, heirCharged)
			}
			if !testCustodyHasExchange(sess, f.lin) {
				t.Fatal("the continued obligation E1 lost its charge")
			}
			if xm, xerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttExchangeSubject(sess.idHash, f.gen, nonce)); xm != nil ||
				!IsNatsErr(xerr, JSNoMessageFoundErr) {
				t.Fatalf("an exchange was created for the heir's transmission nonce %q: %+v (%v)", nonce, xm, xerr)
			}
		})
	}
}

// TestF105AReplacementRespectsAThirdStandingExchange: a pair replacement claims the identifier for
// its fresh obligation, and that claim is checked against the RESULTING state - the batch overlaid
// on what already stands. An unended exchange C that is neither the occupant's nor the heir's,
// standing on the same (generation, identifier), refuses the replacement whole: no heir register,
// the occupant's exchange left at PUBLISH, C unchanged, and no exchange created.
//
// BOUNDARY HARDENING: C is PLANTED by a raw store write. On the production route a third unended
// exchange cannot stand on an identifier whose occupant is unended - one unended obligation per
// (gen, PI), enforced by checkIdentifierExclusivity at every admission and by the allocator's
// bumpPI/holdsPI - so this pins the replacement-scoped form of the exclusivity check, not a
// reachable schedule. Mutations (C7): skip the claim for standing subjects; a replacement-scoped
// early return from the exclusivity check.
func TestF105AReplacementRespectsAThirdStandingExchange(t *testing.T) {
	// The marker disposition seam is process-wide: not parallel.
	var mu sync.Mutex
	var pairDecided bool
	var writes []bool
	var pairErrs []string
	t.Cleanup(setMQTTTestMarkerDispositionHook(func(caller string, v mqttMarkerVerdict) {
		if caller == mqttMarkerCallerReplace && v.disposition == mqttMarkerReplacePair {
			mu.Lock()
			pairDecided = true
			mu.Unlock()
		}
	}))
	f := testWithdrawAdmitted(t, "w2-replace-third", 1)
	e, sess := f.e, f.e.sess
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch
	occupant, occSeq := f.register(t)
	if occupant == nil || !f.markerStands() {
		t.Fatalf("fixture: the occupant must be proven unsent (marker present): %+v", occupant)
	}
	xA0, xA0seq, err := jsa.loadOutboundExchange(sess.id, epoch, f.gen, f.lin)
	if err != nil || xA0.Phase != mqttExchangePublish {
		t.Fatalf("fixture: A's exchange is not at PUBLISH: %+v (%v)", xA0, err)
	}

	// THE PLANTED THIRD OBLIGATION on A's identifier.
	auth, _, aerr := jsa.servingAuthority(sess.id, epoch)
	if aerr != nil || auth == nil {
		t.Fatalf("fixture: no serving authority: %v", aerr)
	}
	cNonce := mqttNewInflightNonce()
	body, merr := json.Marshal(&mqttOutboundExchange{
		ID: sess.id, Epoch: epoch, Gen: f.gen, Nonce: cNonce, PI: f.pi, QoS: 1,
		Phase: mqttExchangePublish, AdmitTxn: mqttNewInflightNonce(), Tenure: auth.Tenure,
	})
	if merr != nil {
		t.Fatalf("encoding the planted exchange: %v", merr)
	}
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, err := jsa.storeMsg(mqttExchangeSubject(sess.idHash, f.gen, cNonce), hdr, msg); err != nil {
		t.Fatalf("planting the third exchange: %v", err)
	}
	// AND ITS DUTY, as an admission writes it: every exchange enumeration is the inventory (F-112).
	testF112PlantAdmittedDuty(t, jsa, sess.id, epoch, f.gen, cNonce)
	xC0, xC0seq, err := jsa.loadOutboundExchange(sess.id, epoch, f.gen, cNonce)
	if err != nil || xC0.Phase != mqttExchangePublish || xC0.PI != f.pi {
		t.Fatalf("fixture: the planted exchange does not read back as an unended obligation on %d: %+v (%v)", f.pi, xC0, err)
	}
	exchanges0 := testCustodyCountExchanges(t, e, f.gen)

	nonce := testW2InstallSuccessorEntry(f)
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(_ *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		mu.Lock()
		// A write counts for the replacement arm only once the ladder has decided a pair
		// replacement: the ladder's first attempt is the create-only admission, also
		// registerOnly=false, which the occupant's register refuses.
		pair := !registerOnly && pairDecided
		writes = append(writes, !pair)
		mu.Unlock()
		out, err := write()
		if pair && err != nil {
			mu.Lock()
			pairErrs = append(pairErrs, err.Error())
			mu.Unlock()
		}
		return out, err
	}
	e.asm.mu.Unlock()

	admitted := sess.admitInflightPublish(e.c, f.pi, 1, testRealPubRelSrcDur, f.sseq, nonce, _EMPTY_, _EMPTY_)
	// ROUTE WITNESS: the ladder decided a pair replacement and its replacement arm wrote.
	mu.Lock()
	decided, w := pairDecided, append([]bool(nil), writes...)
	mu.Unlock()
	pairWrote := false
	for _, ro := range w {
		pairWrote = pairWrote || !ro
	}
	if !decided || !pairWrote {
		t.Fatalf("fixture: the replacement arm was not reached (pair decided=%v, writes=%v)", decided, w)
	}
	// TARGET: refused, and nothing moved.
	if admitted {
		t.Fatal("a pair replacement was admitted on an identifier a third unended exchange holds")
	}
	reg, regSeq := f.register(t)
	if reg == nil || reg.Nonce != occupant.Nonce || reg.Lin != occupant.Lin || regSeq != occSeq {
		t.Fatalf("the occupant's register was replaced by a refused replacement: %+v at %d (was %+v at %d)", reg, regSeq, occupant, occSeq)
	}
	if xA, xseq, lerr := jsa.loadOutboundExchange(sess.id, epoch, f.gen, f.lin); lerr != nil ||
		xA.Phase != mqttExchangePublish || xseq != xA0seq {
		t.Fatalf("A's exchange moved under a refused replacement: %+v at %d (was %d) err=%v", xA, xseq, xA0seq, lerr)
	}
	if xC, xseq, lerr := jsa.loadOutboundExchange(sess.id, epoch, f.gen, cNonce); lerr != nil ||
		xC.Phase != mqttExchangePublish || xseq != xC0seq {
		t.Fatalf("the third exchange moved under a refused replacement: %+v at %d (was %d) err=%v", xC, xseq, xC0seq, lerr)
	}
	if xm, xerr := jsa.loadLastMsgFor(mqttInflightStreamName, mqttExchangeSubject(sess.idHash, f.gen, nonce)); xm != nil ||
		!IsNatsErr(xerr, JSNoMessageFoundErr) {
		t.Fatalf("the refused replacement created an exchange for %q: %+v (%v)", nonce, xm, xerr)
	}
	if n := testCustodyCountExchanges(t, e, f.gen); n != exchanges0 {
		t.Fatalf("the generation holds %d exchanges after a refused replacement, was %d", n, exchanges0)
	}
	// And what refused it was the third obligation's claim on the identifier, not another fence.
	mu.Lock()
	errs := append([]string(nil), pairErrs...)
	mu.Unlock()
	namesC := false
	for _, es := range errs {
		namesC = namesC || strings.Contains(es, cNonce)
	}
	if !namesC {
		t.Fatalf("no replacement attempt was refused for the third exchange %q: %q", cNonce, errs)
	}
}
