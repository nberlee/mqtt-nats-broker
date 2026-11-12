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
	"strings"
	"testing"
	"time"
)

// testWithClaimedTeardown is t with the teardown of the Outstanding entry the claim adds replaced
// by edit, everything else shared: the forged entry a hand-built claim could carry.
func testWithClaimedTeardown(t *mqttAuthorityTransition, edit func(*mqttOutstandingTeardown)) *mqttAuthorityTransition {
	f := *t
	auth := *t.Auth
	life := *t.Auth.Life
	life.Outstanding = append([]mqttOutstandingGen(nil), t.Auth.Life.Outstanding...)
	edit(&life.Outstanding[len(life.Outstanding)-1].Teardown)
	auth.Life = &life
	f.Auth = &auth
	return &f
}

// The teardown claim's boundary reads the teardown from the Outstanding entry the batch adds
// (issue #133): the entry is the claim's only durable record of what it bound, so a claim whose
// entry names another kind, another marker, an abandonment with no fresh acceptance pending, or
// no marker identity at all is refused at the write boundary. Mutations: drop the entry's kind
// check; drop the marker nonce comparison; drop the marker WID comparison; drop the abandonment
// eligibility check; drop the identity well-formedness check.
func TestTheTeardownClaimIsCheckedAgainstTheEntryItAdds(t *testing.T) {
	e, done := testNewEnv(t, "claim-entry-rules")
	defer done()
	auth, seq := e.bootstrap(t, "txn-1")
	claim, err := testComposeTeardownClaim(e.asm, e.id, auth, seq, mqttNewInflightNonce(),
		mqttRetirementRequest{Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn}, nil)
	if err != nil {
		t.Fatalf("composing the claim: %v", err)
	}
	if err := mqttCheckTeardownClaimRules(claim); err != nil {
		t.Fatalf("the production claim was refused: %v", err)
	}
	for _, c := range []struct {
		name string
		edit func(*mqttOutstandingTeardown)
		want string
	}{
		{"a handoff-discard entry", func(td *mqttOutstandingTeardown) { td.Kind = mqttTeardownHandoffDiscard }, "enrols no marker teardown"},
		{"another marker's nonce", func(td *mqttOutstandingTeardown) { td.Nonce = mqttNewInflightNonce() }, "is not its bound marker"},
		{"another marker's write identity", func(td *mqttOutstandingTeardown) { td.MarkerWID = mqttNewRecordWID() }, "is not its bound marker"},
		{"an abandonment with nothing pending", func(td *mqttOutstandingTeardown) { td.Kind = mqttTeardownAbandon }, "finds no fresh pending acceptance"},
		{"no marker identity", func(td *mqttOutstandingTeardown) { td.Nonce, td.MarkerWID = _EMPTY_, _EMPTY_ }, "enrols no marker teardown"},
	} {
		err := mqttCheckTeardownClaimRules(testWithClaimedTeardown(claim, c.edit))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("a claim whose entry carries %s was not refused by its rule (%q): %v", c.name, c.want, err)
		}
	}
}

// A Clean Start's boundary checks the retirement it carries directly against the predecessor it
// displaces (issue #133): the exact ownership, the clean-start reason. Mutations: drop the
// reason check; drop the ownership comparison.
func TestACleanStartCarriesItsPredecessorsRetirement(t *testing.T) {
	e, done := testNewEnv(t, "clean-start-retire-rules")
	defer done()
	e.bootstrap(t, "txn-1")
	txn := mqttNewInflightNonce()
	ca, err := e.asm.jsa.composeAcceptance(mqttAcceptRequest{
		ID: e.id, Epoch: e.epoch, Txn: txn, Tenure: mqttNewInflightNonce(), Nonce: "intent-" + txn,
		At: time.Now().UnixNano(), CleanStart: true, RecordExists: true,
	}, e.asm.jsa.loadOutboundAuthority(e.id, e.epoch), mqttRegistryLoad{})
	if err != nil {
		t.Fatalf("composing the clean start: %v", err)
	}
	if ca.T.Attempt.Kind != mqttTransitionAcceptHandoff || ca.T.Retire == nil {
		t.Fatalf("fixture: the clean start composed %s with retirement %+v", ca.T.Attempt.Kind, ca.T.Retire)
	}
	if err := mqttCheckAcceptShapeRules(ca.T); err != nil {
		t.Fatalf("the production clean start was refused: %v", err)
	}
	for _, c := range []struct {
		name string
		edit func(*mqttRetirementRequest)
	}{
		{"a session-end reason", func(r *mqttRetirementRequest) { r.Reason = mqttRetireSessionEnd }},
		{"another tenure", func(r *mqttRetirementRequest) { r.Tenure = mqttNewInflightNonce() }},
		{"another owner transition", func(r *mqttRetirementRequest) { r.OwnerTxn = mqttNewInflightNonce() }},
	} {
		f := *ca.T
		ret := *ca.T.Retire
		c.edit(&ret)
		f.Retire = &ret
		err := mqttCheckAcceptShapeRules(&f)
		if err == nil || !strings.Contains(err.Error(), "carries no retirement of its predecessor's ownership") {
			t.Fatalf("a clean start carrying %s was not refused by its rule: %v", c.name, err)
		}
	}
}
