//go:build mqttport_fulltests

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
	"encoding/json"
	"strings"
	"sync"
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

// TestF105RegisterOnlyReplacementChargesOnce: a register-only replacement CONTINUES the occupant's
// obligation. It writes the heir register under a fresh transmission nonce on the occupant's
// lineage and allocation history, and nothing else: the occupant's exchange is the obligation, so
// it stands unchanged at PUBLISH, no second exchange is created for the heir's nonce, the custody
// ledger keeps exactly the one charge it held, and no resource takes a new edge.
//
// Mutations (C8): the composer starts a fresh lineage (rec.Lin = rec.Nonce) - the boundary's
// resulting-state check is its fence - alone and together with that boundary; the ladder starts a
// fresh lineage. Targets: the committed heir.Lin == E1 (composer forms) and custody.charges() == 1
// (ladder).
func TestF105RegisterOnlyReplacementChargesOnce(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "w2-register-only", 1)
	e, sess := f.e, f.e.sess
	occupant, _ := f.register(t)
	if occupant == nil || occupant.Lin != f.lin {
		t.Fatalf("fixture: no occupant register on lineage %q: %+v", f.lin, occupant)
	}
	// The send's intent delete: an authoritatively absent marker makes the occupant POSSIBLY
	// ATTEMPTED, the population a register-only replacement continues.
	ml := e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
		t.Fatalf("deleting the occupant's marker: %v", err)
	}
	if f.markerStands() {
		t.Fatal("fixture: the occupant's marker still stands, so it is not possibly attempted")
	}
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
	// TARGET (composer forms): the heir continues E1.
	if heir.Lin != f.lin {
		t.Fatalf("the register-only heir is on lineage %q, want the continued obligation E1 %q", heir.Lin, f.lin)
	}
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
	// TARGET (ladder): one charge, E1's.
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
