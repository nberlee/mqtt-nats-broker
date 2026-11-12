//go:build mqtt_fulltests

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
	"fmt"
	"strings"
	"testing"
	"time"
)

// testRewriteOutboundAuthority rewrites the env's authority as mutate shapes it, at the
// sequence it stands at: the store state a generation's retirement steps leave, staged.
func testRewriteOutboundAuthority(t *testing.T, e *testOutboundEnv, mutate func(a *mqttOutboundAuthority)) {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("no authority to rewrite: %v", al.status)
	}
	next := *al.rec
	life := *al.rec.Life
	life.Outstanding = append([]mqttOutstandingGen(nil), al.rec.Life.Outstanding...)
	next.Life = &life
	next.Rev++
	next.Txn = mqttNewInflightNonce()
	mutate(&next)
	b, _ := json.Marshal(&next)
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(e.id)), hdr, msg); err != nil {
		t.Fatalf("rewriting the authority: %v", err)
	}
	if rl := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); rl.status != mqttOutboundValid {
		t.Fatalf("the rewritten authority does not read back: %v", rl.status)
	}
}

// An abort is fenced by the authority it rides with: it lands while its generation is open
// without moving the authority's revision, is refused once the generation is fenced (which then
// resolves Unresolved, never Retired), and a generation the authority no longer carries
// resolves Retired, its abort refused. Mutations: the abort's fence removed (the fenced abort
// lands); the resolver's retired arm removed (the retired attempt reads Unresolved).
func TestAnAbortIsFencedByItsGenerationsPhase(t *testing.T) {
	e, done := testNewEnv(t, "abort-fence")
	defer done()
	base, baseSeq := e.bootstrap(t, "txn-1")
	attempts := map[string]*mqttTransitionHandle{}
	for _, txn := range []string{"txn-open", "txn-fenced", "txn-retired"} {
		h, err := mqttNewTransitionHandle(testAdmitNothing(e, base, baseSeq, txn))
		if err != nil {
			t.Fatal(err)
		}
		attempts[txn] = h
	}
	// The generation is made noncurrent by its claim, and a successor serves.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if _, err := e.asm.jsa.commitAuthorityTransition(testClosure(e, al.rec, al.seq, "txn-close")); err != nil {
		t.Fatalf("closing the generation: %v", err)
	}
	e.accept(t, "txn-g2", testGen2, "tenure-g2", true, mqttClosingTargets{})
	_, ord, _ := mqttParseGenToken(testGen)

	// OPEN: the abort lands and no revision moves.
	before := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if err := e.asm.jsa.abortTransition(&attempts["txn-open"].attempt, nil); err != nil {
		t.Fatalf("an abort scoped to an open generation was refused: %v", err)
	}
	after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if after.rec.Rev != before.rec.Rev || after.rec.Txn != before.rec.Txn || after.seq == before.seq {
		t.Fatalf("the abort moved the authority from %d/%q to %d/%q (seq %d -> %d): want the body "+
			"rewritten as read", before.rec.Rev, before.rec.Txn, after.rec.Rev, after.rec.Txn, before.seq, after.seq)
	}
	if v := e.asm.jsa.resolveTransitionHandle(attempts["txn-open"]); v != mqttTransitionNeverLanded {
		t.Fatalf("an aborted attempt resolved %v, want NEVER LANDED", v)
	}

	// FENCED: refused by the phase rule, and the scope is not retired.
	testRewriteOutboundAuthority(t, e, func(a *mqttOutboundAuthority) {
		i := mqttOutstandingIndex(a.Life, ord)
		if i < 0 {
			t.Fatalf("fixture: generation %q is not outstanding", testGen)
		}
		a.Life.Outstanding[i].Phase = mqttOutstandingFenced
	})
	err := e.asm.jsa.abortTransition(&attempts["txn-fenced"].attempt, nil)
	if err == nil || !strings.Contains(err.Error(), "only a current or open generation") {
		t.Fatalf("an abort scoped to a fenced generation was not refused by the phase rule: %v", err)
	}
	if v := e.asm.jsa.resolveTransitionHandle(attempts["txn-fenced"]); v != mqttTransitionUnresolved {
		t.Fatalf("an attempt of a fenced generation resolved %v, want UNRESOLVED", v)
	}
	// AND NO EVIDENCE RECORD OF A FENCED GENERATION IS WRITTEN: its completion certificate, which
	// is written only while it is open, is refused and not created.
	if err := e.asm.jsa.publishGenCompletionCert(&mqttGenCompletionCertificate{
		ID: e.id, Epoch: e.epoch, Gen: testGen, SealedHi: 1,
	}, nil); err == nil || !strings.Contains(err.Error(), "outside the window") {
		t.Fatalf("a completion certificate of a fenced generation was not refused by the fence: %v", err)
	}
	if c, _ := e.asm.jsa.loadGenCompletionCert(e.id, e.epoch, testGen); c != nil {
		t.Fatalf("a completion certificate of a fenced generation was written: %+v", c)
	}

	// RETIRED: the authority no longer carries the generation below its allocator.
	testRewriteOutboundAuthority(t, e, func(a *mqttOutboundAuthority) {
		a.Life.Outstanding = nil
	})
	if v := e.asm.jsa.resolveTransitionHandle(attempts["txn-retired"]); v != mqttTransitionRetired {
		t.Fatalf("an attempt of a retired generation resolved %v, want RETIRED", v)
	}
	if err := e.asm.jsa.abortTransition(&attempts["txn-retired"].attempt, nil); err == nil {
		t.Fatal("an abort scoped to a retired generation landed")
	}
	// AND THE HISTORICAL READER SAYS RETIRED, not an ending the floor proved and not "nothing
	// landed": an absent exchange of the generation, with or without a landing proof.
	for _, proof := range []mqttLandingProof{mqttLandingUnproven, mqttLandingDecodedRead} {
		if v := e.asm.jsa.classifyExchangeAbsence(e.id, e.epoch, testGen, "lin-gone", proof); v != mqttAbsenceRetired {
			t.Fatalf("an absent exchange of a retired generation (proof %v) classified %v, want RETIRED", proof, v)
		}
	}
	// AND A DECIDED ATTEMPT STAYS DECIDED: its outcome stands in the retired scope until the
	// purge reclaims it, and the verdict is the outcome's.
	if v := e.asm.jsa.resolveTransitionHandle(attempts["txn-open"]); v != mqttTransitionNeverLanded {
		t.Fatalf("an aborted attempt of a retired generation resolved %v, want NEVER LANDED", v)
	}
}

// The authority-free hold release is fenced by terminal state (issue #133): over a resource that
// lists no hold of an ended exchange and a duty already terminal — the state retirement's
// prerequisites leave — the composer emits no batch at all. Composer-level, isolated from the
// boundary: the transition seam counts what reaches the writer.
func TestNoHoldReleaseIsComposedFromRetiredState(t *testing.T) {
	e, done := testNewEnv(t, "terminal-fence-holds")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testPlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := mqttNewInflightNonce()
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: testGen, Nonce: nonce, PI: 7, QoS: 1,
		Phase: mqttExchangeTerminal, AdmitTxn: "txn-adm", Tenure: auth.Tenure,
		Holds: []mqttExchangeHold{{Inc: "inc-src", Role: mqttRoleSourceReplay}},
	}
	resolved := testDuty(e, 1, mqttDutyExchange, nonce, mqttDutyTerminal)
	testPlantNewDuty(t, e, resolved)
	sent := 0
	testArmTransitionSeam(t, &e.asm.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		sent++
		return send()
	})
	_, err := e.asm.jsa.releaseHoldsOnce(e.id, e.epoch, testGen, auth,
		map[string]*mqttOutboundExchange{nonce: x}, []string{nonce},
		[]mqttSweptDuty{{Ord: 1, Entry: resolved}}, mqttNewInflightNonce())
	if sent != 0 {
		t.Fatalf("a hold release was composed from retired state (%d batch(es) sent, err %v)", sent, err)
	}
}

// A connection-close certificate is written at every close, so it does NOT ride beside the
// authority (consult 11): it lands create-only and leaves the authority's sequence and body
// exactly as they stood, so a transition composed across a close meets no contention from it.
// Mutation: the certificate written through the authority fence (the authority moves).
func TestACloseCertificateLeavesTheAuthorityUntouched(t *testing.T) {
	e, done := testNewEnv(t, "close-cert-unfenced")
	defer done()
	e.bootstrap(t, "txn-1")
	before := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	cert := &mqttDurableCloseCert{ID: e.id, Epoch: e.epoch, Gen: testGen, Run: "run-1", Conn: "conn-1"}
	if err := e.asm.jsa.publishDurableCloseCert(cert); err != nil {
		t.Fatalf("persisting the closure certificate: %v", err)
	}
	if got, err := e.asm.jsa.loadDurableCloseCert(e.id, e.epoch, testGen, "run-1", "conn-1"); err != nil || got == nil {
		t.Fatalf("the certificate does not read back: %+v (%v)", got, err)
	}
	after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if after.seq != before.seq || after.rec.Rev != before.rec.Rev || after.rec.Txn != before.rec.Txn {
		t.Fatalf("the certificate moved the authority from %d to %d", before.seq, after.seq)
	}
}

// A background abort is admitted before EVERY attempt, not once (consult 12): its first write
// loses its CAS to an authority that moved, and the retry asks the holder's admission again. A
// refused admission writes nothing and leaves the attempt unresolved. Mutation: admission asked
// only before the first attempt (the second admission is never asked).
func TestEveryAbortAttemptIsAdmitted(t *testing.T) {
	e, done := testNewEnv(t, "abort-admitted")
	defer done()
	base, baseSeq := e.bootstrap(t, "txn-1")
	moved := func(t *testing.T) func() {
		var once bool
		h := func(subj string) {
			if once {
				return
			}
			once = true
			testRewriteOutboundAuthority(t, e, func(a *mqttOutboundAuthority) {})
		}
		mqttTestBesideAuthorityPresendV.Store(&h)
		return func() { mqttTestBesideAuthorityPresendV.Store(nil) }
	}

	// ADMITTED TWICE, landed on the second.
	x := testAdmitNothing(e, base, baseSeq, "txn-admitted")
	asked := 0
	undo := moved(t)
	err := e.asm.jsa.abortTransition(x.Attempt, func() bool { asked++; return true })
	undo()
	if err != nil || asked != 2 {
		t.Fatalf("an abort whose first write lost its CAS returned %v after %d admission(s), want landed after 2", err, asked)
	}
	if v := e.asm.jsa.resolveTransition(x); v != mqttTransitionNeverLanded {
		t.Fatalf("the admitted abort resolved %v, want NEVER LANDED", v)
	}

	// REFUSED ON THE RETRY: nothing written, the attempt still unresolved.
	y := testAdmitNothing(e, base, baseSeq, "txn-refused")
	asked = 0
	undo = moved(t)
	err = e.asm.jsa.abortTransition(y.Attempt, func() bool { asked++; return asked == 1 })
	undo()
	if !errors.Is(err, errMQTTAbortNotAdmitted) || asked != 2 {
		t.Fatalf("an abort refused admission on its retry returned %v after %d admission(s)", err, asked)
	}
	if v := e.asm.jsa.resolveTransition(y); v != mqttTransitionUnresolved {
		t.Fatalf("an abort refused admission resolved %v, want UNRESOLVED", v)
	}
}

// A generation is fenced only once its work is finished, read from the store at the write
// boundary: a hand-built control is refused while its teardown release is pending, while no
// completion certificate stands, and while its recovery root stands; with all three it lands.
// Mutations: each arm of checkGenerationFenceLicence removed.
func TestTheGenerationFenceIsLicensedByFinishedWork(t *testing.T) {
	e, done := testNewEnv(t, "fence-licence")
	defer done()
	auth, seq := e.bootstrap(t, "txn-1")
	if _, err := e.asm.jsa.commitAuthorityTransition(testClosure(e, auth, seq, "txn-close")); err != nil {
		t.Fatalf("closing the generation: %v", err)
	}
	_, ord, _ := mqttParseGenToken(testGen)
	fence := func(want string) {
		t.Helper()
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		tr, err := mqttComposeRetireGenerationStep(al.rec, al.seq, mqttNewInflightNonce(), ord, 0)
		if err != nil || tr == nil {
			t.Fatalf("composing the fence: %v", err)
		}
		_, cerr := e.asm.jsa.commitAuthorityTransition(tr)
		if want == _EMPTY_ {
			if cerr != nil {
				t.Fatalf("the licensed fence was refused: %v", cerr)
			}
			return
		}
		if cerr == nil || !strings.Contains(cerr.Error(), want) {
			t.Fatalf("the fence was not refused for %q: %v", want, cerr)
		}
	}
	fence("fenced with its teardown release pending")
	testRewriteOutboundAuthority(t, e, func(a *mqttOutboundAuthority) {
		a.Life.Outstanding[mqttOutstandingIndex(a.Life, ord)].Teardown.Pending = false
	})
	fence("no completion certificate standing")
	led, _, _ := e.asm.jsa.loadGenLedger(e.id, e.epoch, testGen)
	if err := e.asm.jsa.publishGenCompletionCert(&mqttGenCompletionCertificate{
		ID: e.id, Epoch: e.epoch, Gen: testGen, SealedHi: led.SealedHi,
	}, nil); err != nil {
		t.Fatalf("certifying the generation: %v", err)
	}
	fence("recovery root still stands")
	closed, _, _ := e.asm.jsa.loadGenClosure(e.id, e.epoch, testGen)
	_, rootSeq, rerr := e.asm.jsa.loadRecoveryRoot(closed.RootShard, closed.RootOrd)
	if rerr != nil || rootSeq == 0 {
		t.Fatalf("fixture: the closure's root does not stand: %v", rerr)
	}
	if err := e.asm.jsa.deleteMsg(mqttSessStreamName, rootSeq, false); err != nil {
		t.Fatalf("retiring the root: %v", err)
	}
	fence(_EMPTY_)
}

// A lifecycle's fence carries the registry shard exactly as it stands with this client moved
// from active to retiring: a forged body (another client moved too) is refused at the write
// boundary, and the honest one lands. Mutation: the store comparison removed.
func TestTheLifecycleFenceWritesOnlyItsOwnRegistryMove(t *testing.T) {
	e, done := testNewEnv(t, "lifecycle-fence-registry")
	defer done()
	e.bootstrap(t, "txn-1")
	testRewriteOutboundAuthority(t, e, func(a *mqttOutboundAuthority) {
		a.State, a.Life.Phase, a.Life.Pending, a.Life.Outstanding, a.Life.Anchors =
			mqttAuthorityClosed, mqttLifecycleClosed, nil, nil, nil
	})
	compose := func() *mqttAuthorityTransition {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		reg := e.asm.jsa.loadLifecycleRegistryShard(e.epoch, mqttLifecycleRegistryShardFor(getHash(e.id)))
		tr, err := mqttComposeRetireLifecycle(al.rec, al.seq, reg, mqttNewInflightNonce())
		if err != nil {
			t.Fatalf("composing the lifecycle fence: %v", err)
		}
		return tr
	}
	forged := compose()
	var body mqttLifecycleRegistryShard
	if err := json.Unmarshal(forged.Registry.body, &body); err != nil {
		t.Fatal(err)
	}
	body.Retiring["zzzzzzzz"] = mqttRetiringLifecycle{L: 99, Phase: mqttLifecycleFenced}
	forged.Registry.body, _ = json.Marshal(&body)
	if _, err := e.asm.jsa.commitAuthorityTransition(forged); err == nil ||
		!strings.Contains(err.Error(), "writes a registry shard other than") {
		t.Fatalf("a forged registry move was not refused by the registry comparison: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(compose()); err != nil {
		t.Fatalf("the honest lifecycle fence was refused: %v", err)
	}
	if al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); al.rec.Life.Phase != mqttLifecycleRetiring {
		t.Fatalf("the fence left the lifecycle in phase %d", al.rec.Life.Phase)
	}
}

// The shared ledger is fenced by the authority (shared_ledger_fence.go): once its generation is
// discarded, neither a join nor the ledger's creation lands for it, so an absent ledger of a
// discarded generation that never acquired stays absent, which is what makes that absence mean
// "no acquisitions". Mutation: the fence's standing check removed.
func TestNoSharedLedgerWorkLandsForADiscardedGeneration(t *testing.T) {
	e, done := testNewEnv(t, "shared-fence")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	gen := auth.Gen
	tenure := testTenure(auth.Life.Accept.Ord + 1)
	testAcceptSharedTenure(t, &e.asm.jsa, e.id, e.epoch, tenure)
	testDiscardClose(t, e)

	inst, err := mqttComposeSharedInstall(e.epoch, e.id, gen, tenure, mqttSharedLedgerLoad{status: mqttOutboundAbsent})
	if err != nil {
		t.Fatalf("composing the late installation: %v", err)
	}
	if res := e.asm.jsa.commitSharedLifecycleBatch(inst); res.Outcome == mqttSharedWriteLanded ||
		!errors.Is(res.Err, errMQTTSharedNotCurrent) {
		t.Fatalf("a ledger was created for the discarded generation, or refused for another reason: %v %v", res.Outcome, res.Err)
	}
	if l := e.asm.jsa.loadSharedLedger(e.id, e.epoch, gen); l.status != mqttOutboundAbsent {
		t.Fatalf("the discarded generation's ledger reads %v, want absent", l.status)
	}
	if acquired, aerr := e.asm.jsa.generationAcquired(e.id, e.epoch, gen); aerr != nil || acquired {
		t.Fatalf("the discarded generation reads acquired=%v (%v)", acquired, aerr)
	}
	if err := e.asm.jsa.sharedClosingComplete(e.id, e.epoch, gen); err != nil {
		t.Fatalf("the closing of a generation that never acquired is not complete: %v", err)
	}
}

// testRetireDiscardedGeneration releases the env's retirement hold and drives recovery and the
// lifecycle pass until generation gen is retired: the real controls and the real purge.
func testRetireDiscardedGeneration(t *testing.T, e *testOutboundEnv, gen string) {
	t.Helper()
	e.asm.testHoldGenerationRetirement.Store(false)
	checkFor(t, 30*time.Second, 50*time.Millisecond, func() error {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
		e.asm.sweepLifecycles(e.asm.server())
		if s, _ := e.asm.genStanding(e.id, gen); s != mqttGenRetired {
			return fmt.Errorf("generation %q reads %v", gen, s)
		}
		return nil
	})
}

// T-delayed-abort and T-historical-readers, across a real purge (issue #133): an attempt scoped
// to a generation, frozen before that generation retired, resolves RETIRED afterwards; its delayed
// abort is refused and recreates no outcome in the purged scope; an absent exchange of the
// generation classifies RETIRED; its teardown classifies obsolete; and a close certificate a
// paused certifier writes afterwards lands inert, the authority untouched (consult 11).
// Mutations: M5 (the abort without its authority fence) recreates the outcome; M27 (the absence
// classifier without its retirement check) reads the purged inventory as a proved ending.
func TestAFrozenAttemptOfARetiredGenerationStaysTerminal(t *testing.T) {
	e, done := testNewEnv(t, "retired-frozen")
	defer done()
	base, baseSeq := e.bootstrap(t, "txn-1")
	gen := base.Gen
	frozen := testAdmitNothing(e, base, baseSeq, "txn-frozen")
	h, err := mqttNewTransitionHandle(frozen)
	if err != nil {
		t.Fatal(err)
	}
	testDiscardClose(t, e)
	testRetireDiscardedGeneration(t, e, gen)

	if v := e.asm.jsa.resolveTransitionHandle(h); v != mqttTransitionRetired {
		t.Fatalf("an attempt of the retired generation resolved %v, want RETIRED", v)
	}
	if err := e.asm.jsa.abortTransition(frozen.Attempt, nil); err == nil {
		t.Fatal("a delayed abort of the retired generation landed")
	}
	if _, _, st := e.asm.jsa.loadTransitionOutcome(e.id, e.epoch, frozen.Attempt.Scope, frozen.Attempt.Txn); st != mqttOutboundAbsent {
		t.Fatalf("the delayed abort recreated an outcome in the purged scope (status %v)", st)
	}
	if v := e.asm.jsa.classifyExchangeAbsence(e.id, e.epoch, gen, "lin-gone", mqttLandingDecodedRead); v != mqttAbsenceRetired {
		t.Fatalf("an absent exchange of the retired generation classified %v, want RETIRED", v)
	}
	own, cerr := e.asm.classifyTeardownGeneration(e.id, e.epoch, gen, nil)
	if cerr != nil || own.Class != mqttTeardownOwnerObsolete {
		t.Fatalf("the retired generation's teardown classified %v (%v), want obsolete", own.Class, cerr)
	}
	before := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if err := e.asm.jsa.publishDurableCloseCert(&mqttDurableCloseCert{ID: e.id, Epoch: e.epoch, Gen: gen, Run: "run-late", Conn: "conn-late"}); err != nil {
		t.Fatalf("the paused certifier's late certificate was refused: %v", err)
	}
	if after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); after.seq != before.seq {
		t.Fatalf("the late certificate moved the authority from %d to %d", before.seq, after.seq)
	}
	if s, _ := e.asm.genStanding(e.id, gen); s != mqttGenRetired {
		t.Fatalf("the late certificate revived generation %q: %v", gen, s)
	}
}

// A teardown that meets a generation whose lifecycle retirement is under way, or whose registry
// cannot be read, after the authority was purged, finds no closure, no ledger and no authority:
// that absence is purged state, not work never admitted, so the classification withholds rather
// than licensing the vacuous deletion. Mutation: unresolved standing classified vacuous.
func TestAnUnresolvedStandingWithholdsTheTeardown(t *testing.T) {
	retiringWithoutAuthority := func(t *testing.T, e *testOutboundEnv) string {
		t.Helper()
		auth, _ := e.bootstrap(t, "txn-1")
		gen := auth.Gen
		testDiscardClose(t, e)
		testRetireDiscardedGeneration(t, e, gen)
		idHash := getHash(e.id)
		// The crashed lifecycle retirement: the registry fenced it retiring, the authority is
		// purged, and the registry's entry was never completed.
		shard := mqttLifecycleRegistryShardFor(idHash)
		rl := e.asm.jsa.loadLifecycleRegistryShard(e.epoch, shard)
		if rl.status != mqttOutboundValid {
			t.Fatalf("fixture: the registry shard reads %v", rl.status)
		}
		l, _, _ := mqttParseGenToken(gen)
		reg := mqttRegistryWithRetiring(rl.rec, idHash, mqttRetiringLifecycle{L: l, Phase: mqttLifecycleBounded, Bound: 1})
		body, err := json.Marshal(reg)
		if err != nil {
			t.Fatal(err)
		}
		msg, hdr := mqttFrameBatchWrite(rl.seq, body, _EMPTY_, 0, false)
		if _, err := e.asm.jsa.storeMsg(mqttLifecycleRegistrySubject(shard), hdr, msg); err != nil {
			t.Fatalf("fixture: fencing the lifecycle retiring: %v", err)
		}
		if err := e.asm.jsa.purgeStream(mqttSessStreamName, mqttOutboundAuthoritySubject(idHash), 0, false); err != nil {
			t.Fatalf("fixture: purging the authority: %v", err)
		}
		return gen
	}
	t.Run("lifecycle retiring, authority purged", func(t *testing.T) {
		e, done := testNewEnv(t, "unresolved-retiring")
		defer done()
		gen := retiringWithoutAuthority(t, e)
		if s, _ := e.asm.genStanding(e.id, gen); s != mqttGenUnresolved {
			t.Fatalf("fixture: generation %q reads %v, want unresolved", gen, s)
		}
		if own, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, gen, nil); err == nil {
			t.Fatalf("a teardown over a half-retired lifecycle classified %v with no error", own.Class)
		}
	})
	t.Run("registry unreadable", func(t *testing.T) {
		e, done := testNewEnv(t, "unresolved-registry")
		defer done()
		gen := retiringWithoutAuthority(t, e)
		regSubj := mqttLifecycleRegistrySubject(mqttLifecycleRegistryShardFor(getHash(e.id)))
		restore := setMQTTTestLoadLastMsgHook(func(_, subject string) error {
			if subject == regSubj {
				return errors.New("injected: the registry read is indeterminate")
			}
			return nil
		})
		defer restore()
		if own, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, gen, nil); err == nil {
			t.Fatalf("a teardown over an unreadable registry classified %v with no error", own.Class)
		}
	})
}

// A fenced generation record (the completion certificate, the affine readiness) written by a
// background actor is admitted before every attempt: each rewrites the authority as its fence.
// Refused, it sends nothing: no record and the authority unmoved. Mutation: the admit ignored.
func TestAFencedRecordWriteIsAdmittedPerAttempt(t *testing.T) {
	e, done := testNewEnv(t, "fenced-admit")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	before := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	subj := mqttGenCertSubject(getHash(e.id), auth.Gen)
	asked := 0
	err := e.asm.jsa.createFencedGenRecord(e.id, e.epoch, auth.Gen, subj, []byte(`{}`), mqttGenRecordWhileLive,
		func() bool { asked++; return false })
	if !errors.Is(err, errMQTTFencedRecordNotAdmitted) || asked != 1 {
		t.Fatalf("a refused admission returned %v after %d ask(s), want errMQTTFencedRecordNotAdmitted after 1", err, asked)
	}
	if smsg, _ := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, subj); smsg != nil {
		t.Fatal("a refused fenced write left its record")
	}
	if after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); after.seq != before.seq {
		t.Fatalf("a refused fenced write moved the authority from %d to %d", before.seq, after.seq)
	}
}

// T-gen-crash (issue #133): a retirement that crashed after a partial purge is finished by a
// later broker from the authority's Outstanding phase alone: the remaining records are purged
// and only then is the entry removed. Mutation M7: the entry removed before the purge (records
// survive a removed entry).
func TestAPartiallyPurgedGenerationIsFinishedFromItsPhase(t *testing.T) {
	e, done := testNewEnv(t, "gen-crash")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	gen := auth.Gen
	testDiscardClose(t, e)
	// Recovery finishes the generation's work (held short of retirement by the env).
	checkFor(t, 30*time.Second, 50*time.Millisecond, func() error {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
		if !testDiscardCertified(e, gen) {
			return fmt.Errorf("generation %q is not certified", gen)
		}
		return nil
	})
	_, ord, _ := mqttParseGenToken(gen)
	idHash := getHash(e.id)
	// The crashed driver: fenced, bounded, and one filter of the purge done.
	for _, bound := range []uint64{0, 0} {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		if o := al.rec.Life.Outstanding[mqttOutstandingIndex(al.rec.Life, ord)]; o.Phase == mqttOutstandingFenced {
			_, last, err := e.asm.jsa.streamSubjects(mqttSessStreamName, mqttOutboundAuthoritySubject(idHash))
			if err != nil {
				t.Fatal(err)
			}
			bound = last
		}
		tr, err := mqttComposeRetireGenerationStep(al.rec, al.seq, mqttNewInflightNonce(), ord, bound)
		if err != nil || tr == nil {
			t.Fatalf("composing the crashed driver's step: %v", err)
		}
		if _, cerr := e.asm.jsa.commitAuthorityTransition(tr); cerr != nil {
			t.Fatalf("the crashed driver's step was refused: %v", cerr)
		}
	}
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	o := al.rec.Life.Outstanding[mqttOutstandingIndex(al.rec.Life, ord)]
	if o.Phase != mqttOutstandingBounded {
		t.Fatalf("fixture: generation %q stands at phase %d, want bounded", gen, o.Phase)
	}
	filters := mqttGenerationPurgeFilters(idHash, gen)
	// The crash came mid-filter: the first filter's records below its newest one are gone.
	firstSubjects, _, lerr := e.asm.jsa.streamSubjects(mqttSessStreamName, filters[0])
	if lerr != nil {
		t.Fatal(lerr)
	}
	newest := uint64(0)
	for subj := range firstSubjects {
		smsg, err := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, subj)
		if err != nil {
			t.Fatalf("reading %q: %v", subj, err)
		}
		newest = max(newest, smsg.Sequence)
	}
	if err := e.asm.jsa.purgeStream(mqttSessStreamName, filters[0], min(newest, o.Bound+1), false); err != nil {
		t.Fatalf("the crashed driver's partial purge: %v", err)
	}
	remaining := func() int {
		n := 0
		for _, f := range filters {
			subjects, _, err := e.asm.jsa.streamSubjects(mqttSessStreamName, f)
			if err != nil {
				t.Fatalf("listing %q: %v", f, err)
			}
			for _, c := range subjects {
				n += int(c)
			}
		}
		return n
	}
	if remaining() == 0 {
		t.Fatal("fixture: the partial purge left nothing for the next driver to finish")
	}
	// The next broker: the lifecycle sweep, from the recorded phase, no longer held.
	e.asm.testHoldGenerationRetirement.Store(false)
	e.asm.sweepLifecycles(e.asm.server())
	if s, _ := e.asm.genStanding(e.id, gen); s != mqttGenRetired {
		t.Fatalf("the restarted driver left generation %q %v", gen, s)
	}
	if n := remaining(); n != 0 {
		t.Fatalf("%d record(s) of the retired generation %q survive its retirement", n, gen)
	}
}

// T-recreated-outbox (issue #133): an outbox record standing after its Will anchor was
// discharged (a late enqueue, or one recreated after its terminal delete) is cancelled, never
// published: a discharged anchor is not an empty minAt. Mutation M14: an absent anchor read as
// nothing to suppress (the record publishes).
func TestAWillRecordOutlivingItsDischargedAnchorIsCancelled(t *testing.T) {
	e, done := testNewEnv(t, "recreated-outbox")
	defer done()
	e.bootstrap(t, "txn-1")
	ref := testAcceptWithWillAnchor(t, e.asm, e.id)
	rec := &mqttWillOutbox{ClientID: e.id, WillID: ref.willID(), FireAt: time.Now().Add(time.Hour).UnixNano()}
	if v, _ := e.asm.decideFromAnchor(rec); v != mqttAnchorPublish {
		t.Fatalf("a record of a standing anchor with no later acceptance decided %v, want publish", v)
	}
	e.asm.dropWillAnchor(e.id, ref)
	// The removal is the manager's asynchronous retry loop.
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if n := len(testWillAnchorsOf(t, e.asm, e.id)); n != 0 {
			return fmt.Errorf("fixture: %d anchor(s) still stand after the discharge", n)
		}
		return nil
	})
	if v, _ := e.asm.decideFromAnchor(rec); v != mqttAnchorCancel {
		t.Fatalf("a record outliving its discharged anchor decided %v, want cancel", v)
	}
}

// T-obsolete-readers (issue #133): the readers a paused job resumes into after its generation
// retired and was purged see an empty or absent generation and end obsolete work: the discard's
// exact-delete read finds nothing to delete, the resource and obligation listings are empty with
// nothing unreadable, and the tenure settlement ends its visit. None manufactures work from the
// absence (the completion proof, plan 6.3b).
func TestReadersOfARetiredGenerationEndObsoleteWork(t *testing.T) {
	e, done := testNewEnv(t, "obsolete-readers")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	gen := auth.Gen
	testDiscardClose(t, e)
	testRetireDiscardedGeneration(t, e, gen)

	st, err := e.asm.jsa.loadDiscardCleanup(e.id, e.epoch, gen, &mqttOutboundExchange{Nonce: "nonce-gone", PI: 7})
	if err != nil || st.rel != nil || st.reg != nil || st.relSeq != 0 || st.regSeq != 0 {
		t.Fatalf("the discard's cleanup read of a retired generation found %+v (%v), want nothing to delete", st, err)
	}
	res, unreadable, rerr := e.asm.enumerateGenerationResources(e.id, e.epoch, gen)
	if rerr != nil || unreadable != 0 || len(res) != 0 {
		t.Fatalf("the resource listing of a retired generation returned %d (%d unreadable, %v)", len(res), unreadable, rerr)
	}
	obl, oerr := e.asm.enumerateGenerationObligations(e.id, e.epoch, gen)
	if oerr != nil || len(obl.exchanges) != 0 || len(obl.actions) != 0 {
		t.Fatalf("the obligation listing of a retired generation returned %+v (%v)", obl, oerr)
	}
	sess, _, serr := e.asm.createOrRestoreSession(e.id, e.asm.server().getOpts())
	if serr != nil {
		t.Fatal(serr)
	}
	if !sess.settleTenureVisit(mqttOutboundEntitlement{Gen: gen, Tenure: auth.Tenure}, map[string]struct{}{}) {
		t.Fatal("the tenure settlement of a retired generation did not end its visit")
	}
}

// T-discharge-crash (issue #133): a terminal Will record is deleted only after its anchor is
// discharged. A failed discharge keeps the record, and the next pass finishes; a pass that
// crashed after the discharge and before the delete is finished by the next one, the removal of
// an already-discharged anchor being a success. Mutation M13: the record deleted before the
// discharge (the failed arm loses the record while its anchor stands).
func TestATerminalWillIsDeletedOnlyAfterItsAnchorIsDischarged(t *testing.T) {
	e, done := testNewEnv(t, "discharge-crash")
	defer done()
	e.bootstrap(t, "txn-1")
	idHash := getHash(e.id)
	plant := func(t *testing.T) (string, *mqttWillAnchorRef) {
		ref := testArmWillAnchor(t, e.asm, e.id)
		subject := mqttWillQSubject(e.asm.domainTk, idHash, ref.willID())
		rec := &mqttWillOutbox{WillID: ref.willID(), ClientIDHash: idHash, ClientID: e.id, Gen: testGen,
			FireAt: time.Now().Add(-time.Second).UnixNano(), OriginSessionSeq: 1, OriginSessEpoch: e.epoch}
		testLicenseWill(rec, mqttWillDone, mqttWillDecisionPublish, 1)
		if _, err := e.asm.jsa.storeWillOutboxCAS(subject, 0, rec); err != nil {
			t.Fatalf("planting the terminal Will: %v", err)
		}
		return subject, ref
	}
	standing := func(subject string) bool {
		rec, _, err := e.asm.jsa.loadWillOutbox(e.asm.domainTk, subject)
		return err == nil && rec != nil
	}
	anchored := func(ref *mqttWillAnchorRef) bool {
		for _, a := range testWillAnchorsOf(t, e.asm, e.id) {
			if a.Ord == ref.Ord {
				return true
			}
		}
		return false
	}

	// A FAILED DISCHARGE keeps the record.
	subject, ref := plant(t)
	e.asm.mu.Lock()
	e.asm.testAnchorTransitionErr = func(string, mqttAnchorOp) error { return errors.New("test: the discharge failed") }
	e.asm.mu.Unlock()
	e.asm.scanWillOutbox(e.asm.server())
	if !standing(subject) || !anchored(ref) {
		t.Fatalf("a failed discharge: record standing=%v, anchor standing=%v; want both", standing(subject), anchored(ref))
	}
	e.asm.mu.Lock()
	e.asm.testAnchorTransitionErr = nil
	e.asm.mu.Unlock()
	e.asm.scanWillOutbox(e.asm.server())
	if standing(subject) || anchored(ref) {
		t.Fatalf("the next pass did not finish: record standing=%v, anchor standing=%v", standing(subject), anchored(ref))
	}

	// A CRASH between the discharge and the delete.
	subject, ref = plant(t)
	if err := e.asm.runAnchorTransition(e.id, *ref, mqttAnchorRemove, 0, nil); err != nil || anchored(ref) {
		t.Fatalf("fixture: the anchor was not discharged: %v", err)
	}
	e.asm.scanWillOutbox(e.asm.server())
	if standing(subject) {
		t.Fatal("a terminal record whose anchor was already discharged was left for ever")
	}
}

// T-anchor-after-purge (issue #133): a Will anchor is the LIFECYCLE's, so it outlives the
// generation that armed it: after a Clean Start successor discarded that generation and it was
// fenced and purged, the old connection's anchor transition (ARMED to OWED) still lands, and
// nothing of the successor moves. Mutation M18: the anchor transition requiring its generation
// open (the late transition is refused and the Will is lost).
func TestAWillAnchorOutlivesItsGeneration(t *testing.T) {
	e, done := testNewEnv(t, "anchor-after-purge")
	defer done()
	e.bootstrap(t, "txn-1")
	ref := testAcceptWithWillAnchor(t, e.asm, e.id)
	old := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch).rec.Gen
	testDiscardClose(t, e)
	testRetireDiscardedGeneration(t, e, old)
	before := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch).rec
	fireAt := time.Now().Add(time.Minute).UnixNano()
	if err := e.asm.runAnchorTransition(e.id, *ref, mqttAnchorOwe, fireAt, nil); err != nil {
		t.Fatalf("the old connection's anchor transition was refused after its generation's purge: %v", err)
	}
	after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch).rec
	i, ok := mqttAnchorAt(after.Life.Anchors, ref.Ord)
	if !ok || after.Life.Anchors[i].State != mqttAnchorOwed || after.Life.Anchors[i].FireAt != fireAt {
		t.Fatalf("the anchor did not become owed: %+v", after.Life.Anchors)
	}
	if after.Gen != before.Gen || after.Tenure != before.Tenure || after.State != before.State ||
		after.Life.Accept != before.Life.Accept {
		t.Fatalf("the late anchor transition moved the successor: %+v -> %+v", before, after)
	}
}

// A teardown release whose record delete landed but whose authority half did not is finished by
// the lifecycle sweep from the positive entry and the record's absence (plan 5.2), not left to
// pin the generation until the client returns. Mutation: the sweep skipping a pending entry.
func TestTheSweepRecordsAReleaseThatDidNotLand(t *testing.T) {
	e, done := testNewEnv(t, "release-unlanded")
	defer done()
	auth, seq := e.bootstrap(t, "txn-1")
	if _, err := e.asm.jsa.commitAuthorityTransition(testClosure(e, auth, seq, "txn-close")); err != nil {
		t.Fatalf("claiming the teardown: %v", err)
	}
	_, ord, _ := mqttParseGenToken(auth.Gen)
	entry := func() mqttOutstandingGen {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		return al.rec.Life.Outstanding[mqttOutstandingIndex(al.rec.Life, ord)]
	}
	if !entry().Teardown.Pending {
		t.Fatal("fixture: the claim left no pending release")
	}
	// The release's delete landed; its authority half never ran.
	if err := e.asm.jsa.deleteMsg(mqttSessStreamName, testStandingRecordSeq(t, e.asm, e.id), false); err != nil {
		t.Fatalf("deleting the marked record: %v", err)
	}
	e.asm.sweepLifecycles(e.asm.server())
	if entry().Teardown.Pending {
		t.Fatal("the sweep left the release of a deleted record unrecorded")
	}
}
