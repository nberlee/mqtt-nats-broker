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
	"strconv"
	"testing"
)

// The whole claim of F-105, exercised end to end against a real store.
//
// Every other test here proves one rule. This one proves they COMPOSE, which is the only
// thing that matters to a client: an obligation admitted before a retirement survives the
// retirement, keeps the resources it needs to finish, finishes, and only then lets those
// resources go. Each earlier lane in this subsystem had correct pieces and no demonstration
// that the pieces added up.

// TestF105ObligationSurvivesRetirementAndCompletes walks one QoS 2 exchange from admission,
// through a closure that would previously have destroyed its completion route, to settlement
// and authorized cleanup.
func TestF105ObligationSurvivesRetirementAndCompletes(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-lifecycle")
	defer done()

	// (1) A serving authority, and the two resources a QoS 2 delivery needs: the source
	// consumer it is delivered from, and the PUBREL consumer whose interest-retained
	// records carry the client's PUBREC evidence. The second is the one a teardown deletes
	// today.
	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	rel, relSeq := testF105PlantResource(t, e, "inc-rel", mqttResourcePubRelConsumer)

	// (2) ADMIT the obligation, with its dependencies, in one transaction.
	adm := testF105Admission(e, auth, authSeq, "txn-adm", 7, []mqttResourceDependency{
		{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay},
		{Resource: rel, Seq: relSeq, Role: mqttRoleCanonicalPubRel},
	})
	atr, err := mqttAdmitPublishTransition(adm)
	if err != nil {
		t.Fatalf("assembling the admission failed: %v", err)
	}
	admSeq, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, atr))
	if err != nil {
		t.Fatalf("the admission was refused: %v", err)
	}

	// (3) ACQUIRE an action on a specific connection. Its binding is what a completion
	// resolves against later, and it is never rewritten.
	bind := mqttBindConnection(4242)
	act := &mqttOutboundAction{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Nonce: adm.Exchange.Nonce, Acq: 1,
		Kind: mqttActionPublish, PI: 7,
		Run: bind.run, Conn: bind.conn, Tenure: auth.Tenure, AcqTxn: "txn-acq",
	}
	acq, err := mqttAcquireActionTransition(atr.Auth, admSeq, "txn-acq", "op-acq", act)
	if err != nil {
		t.Fatalf("assembling the acquisition failed: %v", err)
	}
	acqSeq, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, acq))
	if err != nil {
		t.Fatalf("the acquisition was refused: %v", err)
	}

	// (4) RETIREMENT WINS. The generation is closed while the obligation is outstanding and
	// its action is still executing. This is the moment that used to destroy the completion
	// route.
	closing := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: auth.OwnerTxn, Rev: acq.Auth.Rev + 1,
			Gen: testF105Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: acq.Auth, PrevSeq: acqSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Tenure: auth.Tenure,
			State: mqttAuthorityClosed, Rev: acq.Auth.Rev + 1, Txn: "txn-close",
			OwnerTxn: auth.OwnerTxn,
		},
		Closure: &mqttGenClosure{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Txn: "txn-close"},
	}
	closeSeq, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, closing))
	if err != nil {
		t.Fatalf("the closure was refused: %v", err)
	}

	// (5) THE OBLIGATION AND ITS RESOURCES SURVIVED. This is the assertion the whole
	// finding is about.
	obs, err := e.asm.enumerateGenerationObligations(e.id, e.epoch, testF105Gen)
	if err != nil {
		t.Fatalf("enumerating the closed generation's obligations failed: %v", err)
	}
	if obs.unreadableExchanges != 0 || obs.unreadableOther != 0 {
		t.Fatalf("the closed generation reports %d unreadable exchanges and %d other "+
			"unreadable records", obs.unreadableExchanges, obs.unreadableOther)
	}
	if len(obs.exchanges) != 1 || obs.exchanges[0].Nonce != adm.Exchange.Nonce {
		t.Fatalf("the admitted exchange did not survive the closure: %d found", len(obs.exchanges))
	}
	if len(obs.actions) != 1 || obs.actions[0].Acq != 1 {
		t.Fatalf("the acquired action did not survive the closure: %d found", len(obs.actions))
	}
	for _, inc := range []string{"inc-src", "inc-rel"} {
		rmsg, rerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
			mqttResourceSubject(getHash(e.id), testF105Gen, inc))
		if rerr != nil {
			t.Fatalf("resource %q did not survive the closure: %v", inc, rerr)
		}
		got, _ := mqttDecodeOutboundResource(rmsg.Data, e.id, e.epoch, testF105Gen, inc)
		if len(got.Deps) != 1 || got.Deps[0].Nonce != adm.Exchange.Nonce {
			t.Fatalf("resource %q is needed by %v after the closure, want the admitted "+
				"obligation", inc, got.Deps)
		}
		if mqttResourceDeletable(got) {
			t.Fatalf("resource %q became deletable while the obligation still depends on it: "+
				"this is the teardown that drops the client's PUBREC evidence", inc)
		}
	}

	// (6) COMPLETION STILL WORKS under the closed authority. New work does not.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityClosed {
		t.Fatalf("the authority is not closed (status %v)", al.status)
	}
	xch := obs.exchanges[0]
	for _, op := range []mqttCompletionOp{mqttOpExecuteAcquired, mqttOpObserveAck} {
		v, cerr := mqttCompletionAllowed(mqttCompletionInput{Op: op, Auth: al.rec, Exchange: xch})
		if cerr != nil || v != mqttCompletionProceed {
			t.Fatalf("operation %d on a surviving obligation was %d (%v), want PROCEED", op, v, cerr)
		}
	}
	if v, _ := mqttCompletionAllowed(mqttCompletionInput{
		Op: mqttOpAcquireMore, Auth: al.rec, Exchange: xch,
	}); v != mqttCompletionRefuse {
		t.Fatalf("new work was permitted under a closed authority (%d)", v)
	}

	// (7) THE PROTOCOL RUNS TO ITS END, DURABLY, under the closed authority.
	//
	// Each move is an arbitrated transition that CASes the exchange record, not a field a
	// handler edits: PUBREC transfers ownership to the client, the release becomes owed, and
	// PUBCOMP arrives while the action acquired in (3) can still execute. Advancing in
	// memory would have proved only that this test can assign to a struct — and it would
	// have skipped the two intermediate phases entirely, which is exactly the move the
	// successor relation refuses.
	for _, step := range []struct {
		to  mqttExchangePhase
		why string
	}{
		{mqttExchangePubRecObserved, "PUBREC"},
		{mqttExchangeTerminalPendingDrain, "PUBCOMP with output still outstanding"},
	} {
		cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
		authNow := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		if authNow.status != mqttOutboundValid || authNow.rec.State != mqttAuthorityClosed {
			t.Fatalf("the authority stopped being closed before %s", step.why)
		}
		adv, aerr := mqttAdvanceExchangeTransition(authNow.rec, authNow.seq,
			"txn-adv-"+strconv.Itoa(int(step.to)), "op-adv", cur, curSeq, step.to)
		if aerr != nil {
			t.Fatalf("assembling the advance for %s failed: %v", step.why, aerr)
		}
		if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, adv)); cerr != nil {
			t.Fatalf("the advance for %s was refused under the closed authority: %v", step.why, cerr)
		}
		got, _ := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
		if got.Phase != step.to {
			t.Fatalf("after %s the exchange stands at phase %d, want %d", step.why, got.Phase, step.to)
		}
	}
	// A SECOND PUBCOMP CHANGES NOTHING. The relation is strictly forward, so the exchange
	// cannot be moved back to an earlier phase to be re-charged and re-emitted.
	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	authNow := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if _, aerr := mqttAdvanceExchangeTransition(authNow.rec, authNow.seq, "txn-adv-back", "op-adv",
		cur, curSeq, mqttExchangePublish); aerr == nil {
		t.Fatalf("a terminal-pending-drain exchange was moved back to its publish phase")
	}
	xch = cur

	// The action is still outstanding, so terminal evidence does NOT release yet.
	if v, _ := mqttCompletionAllowed(mqttCompletionInput{
		Op: mqttOpReleaseTerminal, Auth: al.rec, Exchange: xch, Outstanding: true,
	}); v != mqttCompletionAwaitDrain {
		t.Fatalf("a terminal exchange with outstanding output was %d, want AWAIT DRAIN", v)
	}

	// (8) SETTLE the action positively, then release.
	if !mqttActionDrained(obs.actions[0], true, nil) {
		t.Fatalf("a settled action was not reported drained")
	}
	if v, _ := mqttCompletionAllowed(mqttCompletionInput{
		Op: mqttOpReleaseTerminal, Auth: al.rec, Exchange: xch, Outstanding: false,
	}); v != mqttCompletionProceed {
		t.Fatalf("a drained terminal exchange was not releasable under a closed authority (%d)", v)
	}

	// (9) ONLY NOW may the resources go: the dependency is cleared, and the disposal is
	// authorized under the CLOSED authority, which is when this work actually runs.
	// THE DRAIN IS RECORDED DURABLY FIRST. The exchange stands at terminal-pending-drain,
	// and a release names an obligation that ENDED — so the last phase move is part of the
	// lifecycle rather than a thing this test asserts about itself.
	cur, curSeq = testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	authNow = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	drained, derr := mqttAdvanceExchangeTransition(authNow.rec, authNow.seq, "txn-drained",
		"op-drained", cur, curSeq, mqttExchangeTerminal)
	if derr != nil {
		t.Fatalf("assembling the drain completion failed: %v", derr)
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, drained)); cerr != nil {
		t.Fatalf("recording the completed drain was refused: %v", cerr)
	}

	authNow = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	for _, inc := range []string{"inc-src", "inc-rel"} {
		rmsg, _ := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
			mqttResourceSubject(getHash(e.id), testF105Gen, inc))
		got, _ := mqttDecodeOutboundResource(rmsg.Data, e.id, e.epoch, testF105Gen, inc)
		cleared, derr := mqttResourceReleaseDependency(got, testF105EdgeFor(got, adm.Exchange.Nonce))
		if derr != nil {
			t.Fatalf("releasing %q's dependency failed: %v", inc, derr)
		}
		dispose, aerr := mqttAuthorizeDeletionTransition(authNow.rec, authNow.seq,
			"txn-disp-"+inc, "op-disp-"+inc, cleared, rmsg.Sequence)
		if aerr != nil {
			t.Fatalf("assembling %q's disposal failed: %v", inc, aerr)
		}
		if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, dispose)); cerr != nil {
			t.Fatalf("authorizing %q's deletion after completion was refused: %v", inc, cerr)
		}
		authNow = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		if authNow.status != mqttOutboundValid {
			t.Fatalf("the authority does not load back after %q's disposal", inc)
		}
		if authNow.rec.State != mqttAuthorityClosed {
			t.Fatalf("disposing %q reopened the authority to state %d", inc, authNow.rec.State)
		}
		fmsg, _ := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
			mqttResourceSubject(getHash(e.id), testF105Gen, inc))
		fin, _ := mqttDecodeOutboundResource(fmsg.Data, e.id, e.epoch, testF105Gen, inc)
		if !mqttResourceDeletable(fin) {
			t.Fatalf("resource %q is not deletable after its obligation completed: state %d", inc, fin.State)
		}
	}

	_ = closeSeq
	_ = json.Marshal
}

// TestF105ClosureBeforeAdmissionRefusesTheObligation is the other ordering, and it must come
// out the opposite way: an admission that arrives after closure wins nothing.
//
// Together with the test above this is the whole boundary — admitted-before survives,
// admitted-after is refused — and it is the pair that makes "no admission after closure"
// mean something rather than being asserted.
func TestF105ClosureBeforeAdmissionRefusesTheObligation(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-lifecycle-race")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)

	// The admission is composed against the authority as it stands...
	adm := testF105Admission(e, auth, authSeq, "txn-late", 7, []mqttResourceDependency{
		{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay},
	})
	late, err := mqttAdmitPublishTransition(adm)
	if err != nil {
		t.Fatalf("assembling the admission failed: %v", err)
	}

	// ...and the closure lands first.
	closing := testF105Closure(e, auth, authSeq, "txn-close")
	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, closing)); err != nil {
		t.Fatalf("the closure was refused: %v", err)
	}

	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, late)); err == nil {
		t.Fatalf("an obligation was admitted AFTER its generation closed")
	}

	// And it left nothing: no exchange, no register, no marker, and no dependency the
	// cleanup would then wait on forever.
	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 7)
	for name, subj := range map[string]string{
		"register": regSubj,
		"marker":   regSubj + "." + mqttInflightMarkerSuffix,
		"exchange": mqttExchangeSubject(getHash(e.id), testF105Gen, adm.Exchange.Nonce),
	} {
		if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj); err == nil {
			t.Fatalf("the refused post-closure admission installed its %s", name)
		}
	}
	rmsg, _ := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttResourceSubject(getHash(e.id), testF105Gen, "inc-src"))
	got, _ := mqttDecodeOutboundResource(rmsg.Data, e.id, e.epoch, testF105Gen, "inc-src")
	if len(got.Deps) != 0 {
		t.Fatalf("the refused admission left %v depending on the resource, which would block "+
			"cleanup forever for an obligation that does not exist", got.Deps)
	}
}

// testF105LoadExchange reads one exchange as it durably stands, with the sequence an advance
// must CAS against. Read back rather than remembered: a phase advance conditioned on a
// sequence a caller held from earlier would overwrite whatever happened in between.
func testF105LoadExchange(t *testing.T, e *testF105Env, gen, nonce string) (*mqttOutboundExchange, uint64) {
	t.Helper()
	msg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttExchangeSubject(getHash(e.id), gen, nonce))
	if err != nil {
		t.Fatalf("loading exchange %q: %v", nonce, err)
	}
	x, derr := mqttDecodeOutboundExchange(msg.Data, e.id, e.epoch, gen, nonce)
	if derr != nil {
		t.Fatalf("decoding exchange %q: %v", nonce, derr)
	}
	return x, msg.Sequence
}

// TestF105ATruncatedCensusIsNotACensus is round I-6's finding applied to the obligation census.
//
// Capacity counts custody from this enumeration. A first page read as the whole answer
// UNDER-counts, and an under-count admits a delivery past Receive Maximum while the client still
// holds the exchange this broker never saw — the fail-open direction of the same class the
// discharge proof already closed.
func TestF105ATruncatedCensusIsNotACensus(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-census-truncated")
	defer done()
	e.bootstrap(t, "txn-1")

	truncated := func(string) (map[string]uint64, int, error) {
		return map[string]uint64{}, 100_001, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&truncated)
	if _, err := e.asm.enumerateGenerationObligations(e.id, e.epoch, testF105Gen); err == nil {
		t.Fatalf("a first page was read as this generation's whole obligation census")
	}

	complete := func(string) (map[string]uint64, int, error) {
		return map[string]uint64{}, 0, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&complete)
	if _, err := e.asm.enumerateGenerationObligations(e.id, e.epoch, testF105Gen); err != nil {
		t.Fatalf("a complete answer holding nothing was refused: %v", err)
	}
	e.asm.jsa.exchangeSubjectsObs.Store(nil)
}
