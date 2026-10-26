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
	"strings"
	"testing"
)

// The three batch shapes only a deleted composer ever wrote: an exchange admitted with no
// register, an exchange advanced with no slot member, and a cleanup completed with no duty ending.
// Each test submits an otherwise valid batch with ONLY the companion removed, asserts the rule's
// own refusal, and lands the companion-carrying batch as the positive control.

// testWithoutSlotMembers drops every in-flight register and unsent-marker member from a
// batch, leaving its F-105 members (exchange, ledger, duties, resources) untouched.
func testWithoutSlotMembers(tr *mqttAuthorityTransition) *mqttAuthorityTransition {
	out := *tr
	out.Extra = nil
	for _, m := range tr.Extra {
		if _, err := mqttParseInflightSubject(m.subj); err == nil {
			continue
		}
		if _, err := mqttParseInflightMarkerSubject(m.subj); err == nil {
			continue
		}
		out.Extra = append(out.Extra, m)
	}
	return &out
}

// TestExchangeAdmittedOnlyWithItsRegister is Rule A. The negative carries NEITHER register
// NOR marker: a marker without its register is already refused, so removing only the register
// would be refused for that reason and prove nothing about this rule.
func TestExchangeAdmittedOnlyWithItsRegister(t *testing.T) {
	e, done := testF105NewEnv(t, "companion-register")
	defer done()
	auth, authSeq := e.bootstrap(t, "txn-1")
	jsa := &e.asm.jsa

	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Nonce: mqttNewInflightNonce(),
		PI: 7, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: "txn-adm", Tenure: auth.Tenure,
	}
	full, err := testAdmitPublishTr(t, jsa, auth, authSeq, "txn-adm", "op-adm", x)
	if err != nil {
		t.Fatalf("composing the admission: %v", err)
	}
	bare := testWithoutSlotMembers(full)
	if len(bare.Extra) >= len(full.Extra) {
		t.Fatalf("fixture: no register or marker member was removed (%d of %d)", len(bare.Extra), len(full.Extra))
	}
	if _, err := jsa.commitAuthorityTransition(bare); err == nil ||
		!strings.Contains(err.Error(), "with no register beside it") {
		t.Fatalf("an exchange admitted with no register was not refused by its own rule: %v", err)
	}
	if xr, _, _ := jsa.loadOutboundExchange(e.id, e.epoch, auth.Gen, x.Nonce); xr != nil {
		t.Fatalf("the refused admission installed its exchange: %+v", xr)
	}
	// The control: the same admission WITH its register and marker lands.
	if _, err := jsa.commitAuthorityTransition(full); err != nil {
		t.Fatalf("the admission with its register was refused: %v", err)
	}
}

// TestExchangeAdvancedOnlyWithItsSlot is Rule B: the acknowledgement batch with its slot
// re-phase removed is exactly what the deleted exchange-only advance wrote.
func TestExchangeAdvancedOnlyWithItsSlot(t *testing.T) {
	e, done := testF105NewEnv(t, "companion-slot")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
	jsa := &e.asm.jsa

	compose := func() *mqttAuthorityTransition {
		t.Helper()
		cur, curSeq, lerr := jsa.loadOutboundExchange(e.id, e.epoch, auth.Gen, nonce)
		if lerr != nil {
			t.Fatalf("reading the exchange: %v", lerr)
		}
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		tr, terr := mqttAdvanceExchangeAckTransition(al.rec, al.seq, &mqttAdvanceExchangeAckRequest{
			ID: e.id, Epoch: e.epoch, Txn: mqttNewInflightNonce(), Op: "op-ack",
			Gen: auth.Gen, Nonce: nonce, To: mqttExchangeTerminal, Acq: reg.Acq,
			Register: reg, RegisterSeq: seq,
		}, cur, curSeq)
		if terr != nil {
			t.Fatalf("composing the acknowledgement: %v", terr)
		}
		if herr := jsa.appendHoldReleases(tr, cur, mqttExchangeTerminal); herr != nil {
			t.Fatalf("composing the hold releases: %v", herr)
		}
		return tr
	}
	full := compose()
	bare := testWithoutSlotMembers(full)
	if len(bare.Extra) >= len(full.Extra) {
		t.Fatalf("fixture: no slot member was removed (%d of %d)", len(bare.Extra), len(full.Extra))
	}
	if _, err := jsa.commitAuthorityTransition(bare); err == nil ||
		!strings.Contains(err.Error(), "with no slot member beside it") {
		t.Fatalf("an exchange advanced with no slot member was not refused by its own rule: %v", err)
	}
	if x, _ := testF105LoadExchange(t, e, auth.Gen, nonce); x.Phase != mqttExchangePublish {
		t.Fatalf("the refused advance moved the exchange to phase %d", x.Phase)
	}
	// The control: the same acknowledgement WITH its slot re-phase lands.
	if _, err := jsa.commitAuthorityTransition(compose()); err != nil {
		t.Fatalf("the acknowledgement with its slot was refused: %v", err)
	}
}

// testAuthorizedResource provisions a source-consumer resource through the surface and
// drives it to DELETE_AUTHORIZED, returning the ordinal of its predeclared cleanup duty.
func testAuthorizedResource(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority, inc string) uint64 {
	t.Helper()
	jsa := &e.asm.jsa
	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-prov-"+inc, "op-prov", auth.Gen, auth.Tenure,
		inc, "$MQTT_"+inc, mqttResourceSourceConsumer); err != nil {
		t.Fatalf("provisioning %q: %v", inc, err)
	}
	if _, err := jsa.opSealProvisioning(e.id, e.epoch, "txn-seal-"+inc, "op-seal", auth.Gen, inc); err != nil {
		t.Fatalf("sealing %q: %v", inc, err)
	}
	if _, err := jsa.opRequestDisposal(&mqttRequestDisposalRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-disp-" + inc, Gen: auth.Gen, Inc: inc,
		Reason: mqttDisposalUnsubscribed, Entitlement: mqttOutboundEntitlement{Gen: auth.Gen, Tenure: auth.Tenure},
	}); err != nil {
		t.Fatalf("disposing of %q: %v", inc, err)
	}
	if _, err := jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth-"+inc, "op-auth", auth.Gen, inc); err != nil {
		t.Fatalf("authorizing the deletion of %q: %v", inc, err)
	}
	ord, found, err := jsa.findDutyInInventory(e.id, e.epoch, auth.Gen, func(d *mqttDutyEntry) bool {
		return d.Kind == mqttDutyCleanup && d.Ref == inc
	})
	if err != nil || !found {
		t.Fatalf("no cleanup duty stands for %q: %v", inc, err)
	}
	return ord
}

// testCleanupCompletionBatch composes resource r's cleanup completion as completeCleanupAndResolve
// does, with the duty member supplied by the caller (nil for none).
func testCleanupCompletionBatch(t *testing.T, e *testF105Env, r *mqttOutboundResource, rseq uint64,
	duty *mqttTransitionMember) *mqttAuthorityTransition {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	auth := al.rec
	next := *r
	next.State, next.Outcome = mqttResourceCleanupComplete, mqttCleanupGone
	rbody, err := json.Marshal(&next)
	if err != nil {
		t.Fatalf("marshalling the completion: %v", err)
	}
	txn := mqttNewInflightNonce()
	extra := []mqttTransitionMember{{subj: mqttResourceSubject(getHash(e.id), r.Gen, r.Inc), expect: rseq, body: rbody}}
	if duty != nil {
		extra = append(extra, *duty)
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionDisposeResource, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: auth.ID, Epoch: auth.Epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: auth.State, Rev: auth.Rev + 1, Txn: txn, OwnerTxn: auth.OwnerTxn,
			Intent: auth.Intent, Creation: auth.Creation,
		},
		Extra: extra,
	}
}

// testTerminalDutyMember is the member that ends the duty at ord in gen.
func testTerminalDutyMember(t *testing.T, e *testF105Env, gen string, ord uint64) mqttTransitionMember {
	t.Helper()
	cur, curSeq, err := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, ord)
	if err != nil || cur == nil {
		t.Fatalf("reading duty %d of %q: %v", ord, gen, err)
	}
	body, berr := mqttResolvedDutyBody(cur, string(mqttCleanupGone))
	if berr != nil {
		t.Fatalf("composing the duty's ending: %v", berr)
	}
	return mqttTransitionMember{subj: mqttDutySubject(getHash(e.id), gen, ord), expect: curSeq, body: body}
}

// TestCleanupCompletedOnlyWithItsDuty is Rule C, with three isolated negatives. A terminal
// cleanup duty already requires its OWN resource cleanup-complete (in the batch or in storage),
// so the unrelated duty in (2) names a resource that is itself made cleanup-complete first: that
// duty then passes every existing check, and only Rule C can refuse the batch.
func TestCleanupCompletedOnlyWithItsDuty(t *testing.T) {
	e, done := testF105NewEnv(t, "companion-duty")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	jsa := &e.asm.jsa
	ordA := testAuthorizedResource(t, e, auth, "inc-a")
	ordB := testAuthorizedResource(t, e, auth, "inc-b")

	load := func(inc string) (*mqttOutboundResource, uint64) {
		t.Helper()
		r, seq, err := jsa.loadOutboundResource(e.id, e.epoch, auth.Gen, inc)
		if err != nil {
			t.Fatalf("reading %q: %v", inc, err)
		}
		return r, seq
	}
	refused := func(name string, tr *mqttAuthorityTransition) {
		t.Helper()
		if _, err := jsa.commitAuthorityTransition(tr); err == nil ||
			!strings.Contains(err.Error(), "with no terminal cleanup duty naming it") {
			t.Fatalf("%s: the completion was not refused by its own rule: %v", name, err)
		}
		if r, _ := load("inc-a"); r.State != mqttResourceDeleteAuthorized {
			t.Fatalf("%s: the refused completion moved the resource to state %d", name, r.State)
		}
	}

	// (1) No duty member at all: the shape the deleted opCompleteCleanup wrote.
	ra, sa := load("inc-a")
	refused("missing duty", testCleanupCompletionBatch(t, e, ra, sa, nil))

	// (2) Another resource's terminal cleanup duty, same generation. inc-b's resource is made
	// cleanup-complete in storage (its duty left standing), so that duty's own ending is legal.
	rb, sb := load("inc-b")
	doneB := *rb
	doneB.State, doneB.Outcome = mqttResourceCleanupComplete, mqttCleanupGone
	bb, _ := json.Marshal(&doneB)
	bmsg, bhdr := mqttFrameBatchWrite(sb, bb, _EMPTY_, 0, false)
	if _, err := jsa.storeMsg(mqttResourceSubject(getHash(e.id), auth.Gen, "inc-b"), bhdr, bmsg); err != nil {
		t.Fatalf("fixture: completing inc-b's resource in storage: %v", err)
	}
	other := testTerminalDutyMember(t, e, auth.Gen, ordB)
	ra, sa = load("inc-a")
	refused("another resource's duty", testCleanupCompletionBatch(t, e, ra, sa, &other))

	// (3) A terminal cleanup duty naming the same incarnation string in ANOTHER generation. It
	// is planted beside a resource of that name in that generation, itself cleanup-complete, so
	// the duty answers for that resource and not this one.
	const otherGen = "otherGenerationXYZabcd"
	// The real authorized record of inc-a, moved to the other generation and completed there: a
	// well-formed record of that generation, so the duty naming it passes its own checks.
	ra, _ = load("inc-a")
	ghost := *ra
	ghost.Gen = otherGen
	ghost.State, ghost.Outcome = mqttResourceCleanupComplete, mqttCleanupGone
	gb, _ := json.Marshal(&ghost)
	gmsg, ghdr := mqttFrameBatchWrite(0, gb, _EMPTY_, 0, false)
	if _, err := jsa.storeMsg(mqttResourceSubject(getHash(e.id), otherGen, "inc-a"), ghdr, gmsg); err != nil {
		t.Fatalf("fixture: planting the other generation's resource: %v", err)
	}
	gd := &mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: otherGen, Ord: ordA, Kind: mqttDutyCleanup,
		Ref: "inc-a", State: mqttDutyNotYetActionable}
	testF105PlantNewDuty(t, e, gd)
	foreign := testTerminalDutyMember(t, e, otherGen, ordA)
	ra, sa = load("inc-a")
	refused("the same incarnation in another generation", testCleanupCompletionBatch(t, e, ra, sa, &foreign))

	// The control: its own cleanup duty's ending beside it, and the completion lands.
	own := testTerminalDutyMember(t, e, auth.Gen, ordA)
	ra, sa = load("inc-a")
	if _, err := jsa.commitAuthorityTransition(testCleanupCompletionBatch(t, e, ra, sa, &own)); err != nil {
		t.Fatalf("the completion with its own duty was refused: %v", err)
	}
	if r, _ := load("inc-a"); r.State != mqttResourceCleanupComplete {
		t.Fatalf("the completion with its own duty left the resource at state %d", r.State)
	}
}
