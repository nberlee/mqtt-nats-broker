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

// testF105PlantResource stores a live resource and returns it with its sequence.
func testF105PlantResource(t *testing.T, e *testF105Env, inc string, kind mqttResourceKind) (*mqttOutboundResource, uint64) {
	t.Helper()
	r := &mqttOutboundResource{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Inc: inc,
		Kind: kind, Name: "$MQTT_" + inc, State: mqttResourceLive, Deps: []string{},
	}
	b, _ := json.Marshal(r)
	msg, hdr := mqttFrameBatchWrite(0, b, _EMPTY_, 0, false)
	resp, err := e.asm.jsa.storeMsg(mqttResourceSubject(getHash(e.id), testF105Gen, inc), hdr, msg)
	if err != nil {
		t.Fatalf("planting resource %q: %v", inc, err)
	}
	return r, resp.Sequence
}

func testF105Admission(e *testF105Env, auth *mqttOutboundAuthority, authSeq uint64,
	txn string, pi uint16, deps []mqttResourceDependency) *mqttPublishAdmission {
	reg := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: "$MQTT_src", Seq: 1, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	return &mqttPublishAdmission{
		Auth: auth, AuthSeq: authSeq, Txn: txn, Op: "op-" + txn,
		Exchange: &mqttOutboundExchange{
			// THE EXCHANGE NONCE IS THE REGISTER'S LINEAGE, not an independent mint: Lin is
			// per-obligation where the register's own Nonce is per-allocation.
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Nonce: reg.Lin,
			PI: pi, QoS: 2, Phase: mqttExchangePublish,
			AdmitTxn: txn, Tenure: auth.Tenure,
		},
		Register: reg,
		RegSubj:  mqttInflightSubject(getHash(e.id), testF105Gen, pi),
		Deps:     deps,
	}
}

// TestF105PublishAdmissionIsOneTransaction is the checkpoint Codex named: the ordinary QoS 2
// admission carrying its register, its marker, its exchange and its dependency increments in
// ONE transition.
//
// Today the register lands on its own, so a teardown can delete the consumer carrying an
// obligation's PUBREC evidence without ever knowing the obligation depended on it. Here the
// dependency is incremented in the same batch that admits the obligation, so there is no
// interval in which one exists without the other.
func TestF105PublishAdmissionIsOneTransaction(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-admitpub")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	rel, relSeq := testF105PlantResource(t, e, "inc-rel", mqttResourcePubRelConsumer)

	adm := testF105Admission(e, auth, authSeq, "txn-p1", 7, []mqttResourceDependency{
		{Resource: src, Seq: srcSeq}, {Resource: rel, Seq: relSeq},
	})
	tr, err := mqttAdmitPublishTransition(adm)
	if err != nil {
		t.Fatalf("assembling the admission failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("the publish admission was refused: %v", err)
	}

	// All four landed: register, marker, exchange, and both dependency counts.
	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 7)
	if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, regSubj); err != nil {
		t.Fatalf("the register does not load back: %v", err)
	}
	if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, regSubj+"."+mqttInflightMarkerSuffix); err != nil {
		t.Fatalf("the unsent marker does not load back: %v", err)
	}
	xmsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttExchangeSubject(getHash(e.id), testF105Gen, adm.Exchange.Nonce))
	if err != nil {
		t.Fatalf("the exchange does not load back: %v", err)
	}
	if _, derr := mqttDecodeOutboundExchange(xmsg.Data, e.id, e.epoch, testF105Gen, adm.Exchange.Nonce); derr != nil {
		t.Fatalf("the exchange does not decode: %v", derr)
	}
	for _, inc := range []string{"inc-src", "inc-rel"} {
		rmsg, rerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
			mqttResourceSubject(getHash(e.id), testF105Gen, inc))
		if rerr != nil {
			t.Fatalf("resource %q does not load back: %v", inc, rerr)
		}
		got, derr := mqttDecodeOutboundResource(rmsg.Data, e.id, e.epoch, testF105Gen, inc)
		if derr != nil {
			t.Fatalf("resource %q does not decode: %v", inc, derr)
		}
		if len(got.Deps) != 1 {
			t.Fatalf("resource %q is needed by %v after one admission, want exactly one", inc, got.Deps)
		}
		if mqttResourceDeletable(got) {
			t.Fatalf("resource %q is deletable while an obligation depends on it", inc)
		}
	}
}

// TestF105PublishAdmissionLosesWhole is the half that matters: when the admission loses, it
// must leave NOTHING behind — no register, no marker, no exchange, and no dependency
// increment.
//
// Each member would have succeeded on its own subject, so only the shared transaction makes
// this true. A register landing alone is the state that produces an obligation nothing
// authorized; a dependency increment landing alone would block a cleanup forever for an
// obligation that does not exist.
func TestF105PublishAdmissionLosesWhole(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-admitpub-lose")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)

	// Something else advances the authority first, so the admission below is stale.
	bump := testF105Admission(e, auth, authSeq, "txn-bump", 9, nil)
	btr, err := mqttAdmitPublishTransition(bump)
	if err != nil {
		t.Fatalf("assembling the bump failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(btr); err != nil {
		t.Fatalf("the bump admission was refused: %v", err)
	}

	stale := testF105Admission(e, auth, authSeq, "txn-stale", 7, []mqttResourceDependency{
		{Resource: src, Seq: srcSeq},
	})
	str, err := mqttAdmitPublishTransition(stale)
	if err != nil {
		t.Fatalf("assembling the stale admission failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(str); err == nil {
		t.Fatalf("a publish was admitted against a superseded authority")
	}

	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 7)
	for name, subj := range map[string]string{
		"register": regSubj,
		"marker":   regSubj + "." + mqttInflightMarkerSuffix,
		"exchange": mqttExchangeSubject(getHash(e.id), testF105Gen, stale.Exchange.Nonce),
	} {
		if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj); err == nil {
			t.Fatalf("the refused admission still installed its %s: that is an obligation no "+
				"authority admitted", name)
		}
	}
	rmsg, _ := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttResourceSubject(getHash(e.id), testF105Gen, "inc-src"))
	got, _ := mqttDecodeOutboundResource(rmsg.Data, e.id, e.epoch, testF105Gen, "inc-src")
	if len(got.Deps) != 0 {
		t.Fatalf("the refused admission still left %v depending on the resource: a cleanup "+
			"would be blocked forever for an obligation that does not exist", got.Deps)
	}
}

// TestF105PublishAdmissionRefusesWithoutAuthority pins that there is NO missing-authority
// fallback. A fallback is how a path quietly keeps admitting work the authority never
// authorized, which is the state this mechanism exists to make unreachable.
func TestF105PublishAdmissionRefusesWithoutAuthority(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-admitpub-noauth")
	defer done()

	if _, err := mqttAdmitPublishTransition(&mqttPublishAdmission{Txn: "t", Op: "o"}); err == nil {
		t.Fatalf("a publish was admitted with no authority at all")
	}

	auth, authSeq := e.bootstrap(t, "txn-1")

	// A CLOSED authority admits nothing, and the refusal is at assembly rather than at the
	// store: an admission that reached the network would have raced a closure it should
	// never have contended with.
	closed := *auth
	closed.State = mqttAuthorityClosed
	if _, err := mqttAdmitPublishTransition(testF105Admission(e, &closed, authSeq, "txn-c", 7, nil)); err == nil {
		t.Fatalf("a publish was admitted under a CLOSED authority")
	}

	// The exchange and its register must describe one obligation.
	bad := testF105Admission(e, auth, authSeq, "txn-m", 7, nil)
	bad.Exchange.PI = 8
	if _, err := mqttAdmitPublishTransition(bad); err == nil {
		t.Fatalf("an exchange and register allocating different identifiers were admitted")
	}
	bad = testF105Admission(e, auth, authSeq, "txn-q", 7, nil)
	bad.Exchange.QoS = 1
	if _, err := mqttAdmitPublishTransition(bad); err == nil {
		t.Fatalf("an exchange and register carrying different QoS were admitted")
	}
}

// TestF105PublishAdmissionNeverReplacesARegister pins that an admission CREATES the pair and
// cannot displace an occupant.
//
// Which occupant may be replaced by which allocation is the slot state machine's decision
// (inflight_slot_state.go): whether the occupant is the same delivery, what its disposition
// is, whether custody permits the identifier to move. Offering an expectation here would
// decide that a second time, and offering one WITHOUT deciding it is worse — an absent unsent
// marker is normal after transmission, so the marker's create-only member does not stand in
// for the occupant check, and a fresh PUBLISH allocation would overwrite a live past-PUBREC
// register the machine would have refused.
//
// So replacement is not an operation this mechanism supports. Both halves of the pair are
// create-only, and the refusal reaches a hand-composed member too, not only the admission
// argument that no longer exists.
func TestF105PublishAdmissionNeverReplacesARegister(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-admitpub-noreplace")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")

	// An existing register on the identifier, with NO marker beside it — the shape that
	// looks free to anything reasoning from the marker's absence.
	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 7)
	prior := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	priorSeq, err := e.asm.jsa.storeInflightCAS(regSubj, 0, prior)
	if err != nil {
		t.Fatalf("planting the prior register failed: %v", err)
	}
	if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, regSubj+"."+mqttInflightMarkerSuffix); err == nil {
		t.Fatalf("the fixture planted a marker it was supposed to leave absent")
	}

	// The admission is create-only on both members, so it loses to the occupant.
	adm := testF105Admission(e, auth, authSeq, "txn-repl", 7, nil)
	tr, err := mqttAdmitPublishTransition(adm)
	if err != nil {
		t.Fatalf("assembling the admission failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("an admission landed on an identifier a register already occupies")
	}

	// AND THE REFUSAL IS THE BOUNDARY'S, not just the store's: a member composed by hand at
	// the occupant's real sequence — which the store WOULD accept — is refused for naming a
	// replacement at all.
	hand := testF105AdmitNothing(e, auth, authSeq, "txn-hand")
	body, merr := json.Marshal(freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 2, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	}))
	if merr != nil {
		t.Fatalf("marshalling the replacement register failed: %v", merr)
	}
	hand.Extra = []mqttTransitionMember{{subj: regSubj, expect: priorSeq, body: body}}
	if _, err := e.asm.jsa.commitAuthorityTransition(hand); err == nil {
		t.Fatalf("a hand-composed member replaced a live register at its own sequence")
	}
	rmsg, rerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, regSubj)
	if rerr != nil || rmsg.Sequence != priorSeq {
		t.Fatalf("the refused replacement moved the standing register")
	}

	// The marker stays create-only on an EMPTY identifier, which is what makes a standing
	// marker mean "nothing has been transmitted under this pair".
	free := testF105Admission(e, auth, authSeq, "txn-free", 8, nil)
	ftr, ferr := mqttAdmitPublishTransition(free)
	if ferr != nil {
		t.Fatalf("assembling the admission onto a free identifier failed: %v", ferr)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(ftr); err != nil {
		t.Fatalf("an admission onto a free identifier was refused: %v", err)
	}
	freeSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 8)
	if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, freeSubj+"."+mqttInflightMarkerSuffix); err != nil {
		t.Fatalf("the admission did not install its unsent marker: %v", err)
	}
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	again := testF105Admission(e, al.rec, al.seq, "txn-again", 8, nil)
	atr, aerr := mqttAdmitPublishTransition(again)
	if aerr != nil {
		t.Fatalf("assembling the second admission failed: %v", aerr)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(atr); err == nil {
		t.Fatalf("an admission landed onto an identifier whose marker already stood: the " +
			"create-only marker is what makes a standing marker mean anything")
	}
}

// TestF105ExchangeNonceIsTheRegisterLineage pins the binding implementation review 4
// required, and the reason it matters is not tidiness.
//
// Lin is per-OBLIGATION where the register's own Nonce is per-ALLOCATION. Minting the
// exchange identity independently would give one obligation two identities that agree only
// by luck — and after an allocation replacement nothing could reconstruct which exchange a
// register belonged to, because the identifier is reusable and matching on it proves
// nothing.
func TestF105ExchangeNonceIsTheRegisterLineage(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-lineage-bind")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")

	ok := testF105Admission(e, auth, authSeq, "txn-p", 7, nil)
	if ok.Exchange.Nonce != ok.Register.Lin {
		t.Fatalf("the fixture no longer binds the exchange to the register's lineage")
	}
	if _, err := mqttAdmitPublishTransition(ok); err != nil {
		t.Fatalf("a correctly bound admission was refused: %v", err)
	}

	// An independently minted exchange identity is refused.
	loose := testF105Admission(e, auth, authSeq, "txn-q", 8, nil)
	loose.Exchange.Nonce = mqttNewInflightNonce()
	if _, err := mqttAdmitPublishTransition(loose); err == nil {
		t.Fatalf("an exchange minted independently of its register's lineage was admitted: " +
			"after an allocation replacement nothing could say which exchange that register " +
			"belonged to")
	}

	// And so is one that borrows the register's per-ALLOCATION nonce, which is the
	// available near-miss: that value changes when the identifier is reallocated, so an
	// obligation keyed on it would not survive a replacement.
	// A REPLACEMENT register is the shape where the two differ: the allocation is re-minted
	// while the obligation is carried, so Nonce moves and Lin does not. Built explicitly
	// rather than skipped, because a skipped case is a case nobody checks.
	wrong := testF105Admission(e, auth, authSeq, "txn-r", 9, nil)
	carried := wrong.Register.Lin
	wrong.Register.Nonce = mqttNewInflightNonce()
	wrong.Register.Lin = carried
	if wrong.Register.Nonce == wrong.Register.Lin {
		t.Fatalf("the fixture failed to build a register whose allocation differs from its obligation")
	}
	wrong.Exchange.Nonce = wrong.Register.Nonce
	if _, err := mqttAdmitPublishTransition(wrong); err == nil {
		t.Fatalf("an exchange keyed on the register's per-ALLOCATION nonce was admitted: that " +
			"value is re-minted by a replacement, so the obligation would lose its identity " +
			"exactly when the identifier moves")
	}
	// The same register bound correctly is admitted, so the refusal above is about the
	// binding and not about the replacement shape.
	wrong.Exchange.Nonce = carried
	if _, err := mqttAdmitPublishTransition(wrong); err != nil {
		t.Fatalf("a replacement register bound to its carried lineage was refused: %v", err)
	}
}

// testF105AdmitOn admits one obligation that depends on the named resources, through the
// OPERATION surface, and returns its exchange nonce.
//
// The operation reads the authority and every named resource itself, which is what a
// converted caller will do: this fixture supplies the register the delivery path would have
// built and the incarnations the obligation needs, and nothing else.
func testF105AdmitOn(t *testing.T, e *testF105Env, txn string, pi uint16, incs ...string) string {
	t.Helper()
	reg := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: "$MQTT_src", Seq: 1, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority does not load back: status %v", al.status)
	}
	if _, err := e.asm.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
		Gen: al.rec.Gen, Tenure: al.rec.Tenure,
		Register: reg, DepIncs: incs,
	}); err != nil {
		t.Fatalf("admission %s was refused: %v", txn, err)
	}
	return reg.Lin
}

// testF105FinishExchange walks one QoS 2 exchange to TERMINAL durably, through the operation
// surface.
//
// Every step is an arbitrated transition. A test that reached terminal by writing the record
// would be asserting the very thing a dependency release is required to PROVE.
func testF105FinishExchange(t *testing.T, e *testF105Env, nonce string) {
	t.Helper()
	for i, to := range []mqttExchangePhase{
		mqttExchangePubRecObserved, mqttExchangePubRelOwed, mqttExchangeTerminal,
	} {
		if _, err := e.asm.jsa.opAdvanceExchange(e.id, e.epoch,
			"txn-fin-"+nonce+"-"+strconv.Itoa(i), "op-fin", testF105Gen, nonce, to); err != nil {
			t.Fatalf("advancing %q to %d was refused: %v", nonce, to, err)
		}
	}
}

// testF105ReleaseDependency releases ONE named edge from a resource, through the operation
// surface.
func testF105ReleaseDependency(t *testing.T, e *testF105Env, inc, nonce, txn string) error {
	t.Helper()
	_, err := e.asm.jsa.opReleaseDependency(e.id, e.epoch, txn, "op-"+txn, testF105Gen, inc, nonce)
	return err
}
