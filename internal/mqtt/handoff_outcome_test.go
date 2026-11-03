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
	"testing"
)

// TestHandoffSubjectIsNotASessionRecord is the tripwire for the third subject space.
//
// Every session walk in this package filters "sess.<domainTk>>" and decodes what it gets as
// a session record, which is exactly why the acceptance facts are top-level. A handoff
// outcome nested under that prefix would be read as a malformed session, and the reader
// would be drawing conclusions about a client from a record about a handoff.
func TestHandoffSubjectIsNotASessionRecord(t *testing.T) {
	const domainTk = ""
	subj := mqttHandoffOutcomeSubject(domainTk, testIDHash, "intent-1")

	if testSubjectMatchesPrefix(subj, mqttSessStreamSubjectPrefix) {
		t.Fatalf("the handoff subject %q sits under the session-record prefix, where every "+
			"session walk would decode it as a session", subj)
	}
	if testSubjectMatchesPrefix(subj, mqttAcceptSubjectPrefix) {
		t.Fatalf("the handoff subject %q sits under the acceptance-fact prefix", subj)
	}
	if !mqttIsHandoffSubject(subj) {
		t.Fatalf("the handoff subject %q is not classified as one", subj)
	}
	// And the classification must not claim the other two spaces.
	if mqttIsHandoffSubject(mqttSessStreamSubjectPrefix + domainTk + testIDHash) {
		t.Fatalf("a session record subject is classified as a handoff outcome")
	}
	if mqttIsHandoffSubject(mqttAcceptSubject(domainTk, testIDHash, "ep", "aid")) {
		t.Fatalf("an acceptance fact subject is classified as a handoff outcome")
	}
}

// TestHandoffOutcomeDecodeIsVerdictScoped pins that an aborted metadata half cannot
// describe a session write it did not perform, and that the verdict itself is mandatory.
func TestHandoffOutcomeDecodeIsVerdictScoped(t *testing.T) {
	committed := testHandoffJSON(t, mqttHandoffCommitted)
	if _, err := mqttDecodeHandoffOutcome(committed, testID, testEpoch, "intent-1"); err != nil {
		t.Fatalf("a well-formed committed handoff outcome was refused: %v", err)
	}
	aborted := testHandoffJSON(t, mqttHandoffAborted)
	if _, err := mqttDecodeHandoffOutcome(aborted, testID, testEpoch, "intent-1"); err != nil {
		t.Fatalf("a well-formed aborted handoff outcome was refused: %v", err)
	}

	for name, body := range map[string]string{
		"aborted with a write identity":  testWith(t, aborted, "sess_wid", `"wid-1"`),
		"aborted with a proposal":        testWith(t, aborted, "proposed_gen", `"`+testGen2+`"`),
		"verdict absent":                 testWithout(t, committed, "verdict"),
		"verdict null":                   testWith(t, committed, "verdict", "null"),
		"verdict unsupported":            testWith(t, committed, "verdict", "9"),
		"committed without a write id":   testWithout(t, committed, "sess_wid"),
		"committed without a proposal":   testWithout(t, committed, "proposed_gen"),
		"an unknown member":              testWith(t, committed, "extra", `"x"`),
		"a predecessor at sequence zero": testWith(t, committed, "prev_sess_seq", "0"),
	} {
		if _, err := mqttDecodeHandoffOutcome([]byte(body), testID, testEpoch, "intent-1"); err == nil {
			t.Fatalf("a handoff outcome with %s was admitted", name)
		}
	}

	if mqttHandoffVerdict(0) == mqttHandoffCommitted {
		t.Fatalf("the zero handoff verdict names COMMITTED, so a body missing the member " +
			"would read as the permissive value")
	}
}

// TestCommittedHandoffCarriesOnlyWhatIsKnownBeforePublication pins the correction that
// a sequence cannot be in this record at all.
//
// The body is assembled BEFORE its own batch commits, the batch API returns the commit
// member's sequence only afterwards, and substituting a body after publication would
// destroy the create-only permanence the whole protocol rests on. So the receipt carries a
// write IDENTITY, which the writer mints itself — and which is what a repair needs anyway,
// since a replacement moves the position while the identity says whether the record is
// still ours.
func TestCommittedHandoffCarriesOnlyWhatIsKnownBeforePublication(t *testing.T) {
	o := testHandoffOutcome(mqttHandoffCommitted)
	if err := mqttValidateHandoffOutcome(o, testID, testEpoch, "intent-1"); err != nil {
		t.Fatalf("a well-formed committed outcome was refused: %v", err)
	}

	for name, mutate := range map[string]func(*mqttHandoffOutcome){
		"no write identity":              func(m *mqttHandoffOutcome) { m.SessWID = _EMPTY_ },
		"the predecessor's own identity": func(m *mqttHandoffOutcome) { m.SessWID = m.PrevSessWID },
		"no proposed generation":         func(m *mqttHandoffOutcome) { m.ProposedGen = _EMPTY_ },
		"no proposed tenure":             func(m *mqttHandoffOutcome) { m.ProposedTenure = _EMPTY_ },
	} {
		m := *o
		mutate(&m)
		if err := mqttValidateHandoffOutcome(&m, testID, testEpoch, "intent-1"); err == nil {
			t.Fatalf("a committed handoff outcome with %s was admitted", name)
		}
	}
}

// TestHandoffOutcomeMustMatchItsIntent pins that an outcome sharing a subject is not
// thereby an answer: one conditioned on a different predecessor describes a different
// handoff, and finalizing on it would displace ownership nobody checked.
func TestHandoffOutcomeMustMatchItsIntent(t *testing.T) {
	i := testPreparingAuthority().Intent
	p, perr := mqttProposalFromHandoff(i)
	if perr != nil {
		t.Fatalf("the fixture handoff intent was refused: %v", perr)
	}
	o := testHandoffOutcome(mqttHandoffCommitted)
	o.Intent = i.ID
	if err := mqttOutcomeAnswersProposal(o, p); err != nil {
		t.Fatalf("an outcome answering its own intent was rejected: %v", err)
	}

	for name, mutate := range map[string]func(*mqttHandoffOutcome){
		"proposed generation":    func(m *mqttHandoffOutcome) { m.ProposedGen = testGen },
		"proposed tenure":        func(m *mqttHandoffOutcome) { m.ProposedTenure = "tenure-9" },
		"intent":                 func(m *mqttHandoffOutcome) { m.Intent = "intent-9" },
		"predecessor generation": func(m *mqttHandoffOutcome) { m.PrevGen = testGen2 },
		"predecessor tenure":     func(m *mqttHandoffOutcome) { m.PrevTenure = "tenure-9" },
		"predecessor ownership":  func(m *mqttHandoffOutcome) { m.PrevOwnerTxn = "own-9" },
		"predecessor write id":   func(m *mqttHandoffOutcome) { m.PrevSessWID = "wid-9" },
		"predecessor sequence":   func(m *mqttHandoffOutcome) { m.PrevSessSeq = 99 },
		// THE SHAPE is part of the answer. One subject carries one proposal's verdict, so a
		// creation receipt standing where a handoff's is sought is a contradiction and must
		// never be read as an answer — a handoff finalized on it would install ownership
		// with no predecessor displaced.
		"proposal kind": func(m *mqttHandoffOutcome) { m.Kind = mqttProposalCreation },
	} {
		m := *o
		mutate(&m)
		if err := mqttOutcomeAnswersProposal(&m, p); err == nil {
			t.Fatalf("an outcome differing in its %s was reported as answering this intent", name)
		}
	}
}

// TestOutcomeShapesAreDistinct pins that the receipt's required members follow the
// PROPOSAL KIND, in both directions.
//
// One reader for two shapes was the alternative to two protocols, and it is only safe if the
// shape is mandatory and enforced: a reader that guessed would either demand a predecessor a
// first creation never had — refusing every legitimate creation receipt — or admit a handoff
// naming none, losing the binding that stops a delayed acceptance retargeting a successor.
func TestOutcomeShapesAreDistinct(t *testing.T) {
	handoff := testHandoffJSON(t, mqttHandoffCommitted)
	creation := testCreationJSON(t, mqttHandoffCommitted)
	abortedCreation := testCreationJSON(t, mqttHandoffAborted)

	for name, body := range map[string][]byte{
		"a committed creation": creation,
		"an aborted creation":  abortedCreation,
	} {
		if _, err := mqttDecodeHandoffOutcome(body, testID, testEpoch, "intent-1"); err != nil {
			t.Fatalf("a well-formed receipt for %s was refused: %v", name, err)
		}
	}

	// THE VALIDATOR IS THE BACKSTOP, and it is checked SEPARATELY from the decoder. A
	// receipt assembled in memory never passes the member check at all — the freeze and
	// abort paths build one from a proposal and validate it directly — so if only the
	// decoder enforced the shape, a hand-built proposal carrying both a creation kind and a
	// predecessor would be published as a receipt describing a displacement that never
	// happened.
	for name, mutate := range map[string]func(*mqttHandoffOutcome){
		"a predecessor generation":      func(m *mqttHandoffOutcome) { m.PrevGen = testGen },
		"a predecessor tenure":          func(m *mqttHandoffOutcome) { m.PrevTenure = "tenure-0" },
		"a predecessor ownership":       func(m *mqttHandoffOutcome) { m.PrevOwnerTxn = "own-0" },
		"a predecessor write identity":  func(m *mqttHandoffOutcome) { m.PrevSessWID = "wid-0" },
		"a predecessor record sequence": func(m *mqttHandoffOutcome) { m.PrevSessSeq = 12 },
	} {
		m := *testCreationOutcome(mqttHandoffCommitted)
		mutate(&m)
		if err := mqttValidateHandoffOutcome(&m, testID, testEpoch, "intent-1"); err == nil {
			t.Fatalf("a creation receipt carrying %s was admitted", name)
		}
	}
	// A handoff receipt that dropped one is refused by the same validator, so the check is
	// not merely "creations are emptier".
	for name, mutate := range map[string]func(*mqttHandoffOutcome){
		"no predecessor generation":      func(m *mqttHandoffOutcome) { m.PrevGen = _EMPTY_ },
		"no predecessor record sequence": func(m *mqttHandoffOutcome) { m.PrevSessSeq = 0 },
	} {
		m := *testHandoffOutcome(mqttHandoffCommitted)
		mutate(&m)
		if err := mqttValidateHandoffOutcome(&m, testID, testEpoch, "intent-1"); err == nil {
			t.Fatalf("a handoff receipt with %s was admitted", name)
		}
	}
	// And an unsupported kind is refused by the validator too, not only by the decoder.
	m := *testHandoffOutcome(mqttHandoffCommitted)
	m.Kind = "transfer"
	if err := mqttValidateHandoffOutcome(&m, testID, testEpoch, "intent-1"); err == nil {
		t.Fatalf("a receipt carrying an unsupported proposal kind was admitted")
	}

	for name, body := range map[string]string{
		// A creation that names a predecessor describes a displacement that never happened.
		"a creation naming a predecessor generation": testWith(t, creation, "prev_gen", `"`+testGen+`"`),
		"a creation naming a predecessor sequence":   testWith(t, creation, "prev_sess_seq", "12"),
		// And a handoff that names none has lost the binding it exists for.
		"a handoff without its predecessor generation": testWithout(t, handoff, "prev_gen"),
		"a handoff without its predecessor sequence":   testWithout(t, handoff, "prev_sess_seq"),
		// The kind itself is mandatory and closed.
		"no proposal kind":          testWithout(t, handoff, "kind"),
		"an unsupported kind":       testWith(t, handoff, "kind", `"transfer"`),
		"a null kind":               testWith(t, handoff, "kind", "null"),
		"an empty kind":             testWith(t, handoff, "kind", `""`),
		"a creation with a kind of": testWith(t, creation, "kind", `"handoff"`),
	} {
		if _, err := mqttDecodeHandoffOutcome([]byte(body), testID, testEpoch, "intent-1"); err == nil {
			t.Fatalf("a receipt with %s was admitted", name)
		}
	}
}

// TestProposalViewsCarryTheirOwnShape pins that the KIND comes from the constructor and
// that a creation view carries no predecessor for a caller to fill in later.
func TestProposalViewsCarryTheirOwnShape(t *testing.T) {
	h, err := mqttProposalFromHandoff(testPreparingAuthority().Intent)
	if err != nil {
		t.Fatalf("the fixture handoff intent was refused: %v", err)
	}
	if h.Kind != mqttProposalHandoff {
		t.Fatalf("a handoff intent viewed as a %q proposal", h.Kind)
	}
	if h.PrevGen == _EMPTY_ || h.PrevSessSeq == 0 {
		t.Fatalf("a handoff proposal lost the predecessor it is conditioned on")
	}

	c, cerr := mqttProposalFromCreation(&mqttCreationIntent{
		ID: "intent-1", Gen: testGen, Tenure: "tenure-1",
	})
	if cerr != nil {
		t.Fatalf("a well-formed creation intent was refused: %v", cerr)
	}
	if c.Kind != mqttProposalCreation {
		t.Fatalf("a creation intent viewed as a %q proposal", c.Kind)
	}
	if c.PrevGen != _EMPTY_ || c.PrevTenure != _EMPTY_ || c.PrevOwnerTxn != _EMPTY_ ||
		c.PrevSessWID != _EMPTY_ || c.PrevSessSeq != 0 {
		t.Fatalf("a creation proposal names a predecessor it never displaced")
	}

	// A malformed intent of either shape produces no proposal at all, so no caller can
	// reach the metadata half with one.
	if _, err := mqttProposalFromHandoff(nil); err == nil {
		t.Fatalf("an absent handoff intent produced a proposal")
	}
	if _, err := mqttProposalFromCreation(&mqttCreationIntent{ID: "intent-1"}); err == nil {
		t.Fatalf("a creation intent proposing no generation produced a proposal")
	}
}

func testHandoffOutcome(v mqttHandoffVerdict) *mqttHandoffOutcome {
	o := &mqttHandoffOutcome{
		ID: testID, Epoch: testEpoch, Intent: "intent-1", Verdict: v,
		Kind:    mqttProposalHandoff,
		PrevGen: testGen, PrevTenure: "tenure-1", PrevOwnerTxn: "own-1",
		PrevSessWID: "wid-0", PrevSessSeq: 12,
	}
	if v == mqttHandoffCommitted {
		o.SessWID = "wid-1"
		o.ProposedGen, o.ProposedTenure = testGen2, "tenure-2"
	}
	return o
}

func testHandoffJSON(t *testing.T, v mqttHandoffVerdict) []byte {
	t.Helper()
	b, err := json.Marshal(testHandoffOutcome(v))
	if err != nil {
		t.Fatalf("marshalling the fixture handoff outcome: %v", err)
	}
	return b
}

// testCreationOutcome is the OTHER shape: a first creation displaces nothing, so it
// names no predecessor at all.
func testCreationOutcome(v mqttHandoffVerdict) *mqttHandoffOutcome {
	o := &mqttHandoffOutcome{
		ID: testID, Epoch: testEpoch, Intent: "intent-1", Verdict: v,
		Kind: mqttProposalCreation,
	}
	if v == mqttHandoffCommitted {
		o.SessWID = "wid-1"
		o.ProposedGen, o.ProposedTenure = testGen2, "tenure-2"
	}
	return o
}

func testCreationJSON(t *testing.T, v mqttHandoffVerdict) []byte {
	t.Helper()
	b, err := json.Marshal(testCreationOutcome(v))
	if err != nil {
		t.Fatalf("marshalling the fixture creation outcome: %v", err)
	}
	return b
}

func testSubjectMatchesPrefix(subject, prefix string) bool {
	return len(subject) >= len(prefix) && subject[:len(prefix)] == prefix
}
