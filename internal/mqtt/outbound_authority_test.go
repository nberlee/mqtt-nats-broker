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

// Syntactically valid tokens: a generation is mqttSessGenLen alphanumerics, a client-ID
// hash is sysHashLen of them.
const (
	// A substrate fixture's first two generations: lifecycle 2 (the harness retired the minting
	// CONNECT's lifecycle 1, testClearMintedAuthority), ordinals 0 and 1 (mqttGenToken).
	testGen    = "0000000002000000000000"
	testGen2   = "0000000002000000000001"
	testIDHash = "abcd1234"
	testID     = "client-a"
	testEpoch  = "epoch-1"
)

// TestSubjectsAreUnreachableByTheRegisterMachinery is the tripwire the class token
// exists for.
//
// The issue #107 record classes share a stream with the outbound registers, because
// JetStream's atomic batches are per stream and the authority has to commit with the
// exchange mutation it authorizes. Sharing the stream means sharing the subject space with
// a reap that deletes what its enumeration returns, so the separation is asserted here
// rather than left to the reader of a subject builder.
//
// Note which guard does the work for each class. A closure carries four tokens past the
// prefix, an outcome five or six (its scope is a generation or L.<L>), and the register parser
// wants exactly three, so they are excluded structurally. THE AUTHORITY SUBJECT HAS EXACTLY THREE, so only token validation excludes
// it — TestClassTokenCannotBeMistakenForAnIdentity is the test that actually protects
// that one, and an earlier comment in this file claimed the count guard covered all three.
func TestSubjectsAreUnreachableByTheRegisterMachinery(t *testing.T) {
	subjects := map[string]string{
		"authority": mqttOutboundAuthoritySubject(testIDHash),
		"closure":   mqttGenClosureSubject(testIDHash, testGen),
		"outcome":   mqttTransitionOutcomeSubject(testIDHash, testGen, "txn-1"),
		// The inventory classes are asserted here explicitly rather than inherited from the
		// prefix argument. A ledger carries four tokens and a duty five, so both are excluded
		// twice over — but "excluded twice over" is a claim about this code as it stands, and
		// a class that silently began parsing as a register would be destroyed by a reap that
		// believed it had named what it deleted. Named sites are a sample; these are instances.
		"ledger": mqttGenLedgerSubject(testIDHash, testGen),
		"duty":   mqttDutySubject(testIDHash, testGen, 1),
	}

	for name, subj := range subjects {
		if _, err := mqttParseInflightSubject(subj); err == nil {
			t.Fatalf("the %s subject %q parses as an in-flight register, so a bounded reap "+
				"would believe it had named what it deleted", name, subj)
		}
		if _, err := mqttParseInflightMarkerSubject(subj); err == nil {
			t.Fatalf("the %s subject %q parses as an unsent marker, which would let marker "+
				"recovery read it as proof about a register", name, subj)
		}
		if !mqttIsOutboundRecordSubject(subj) {
			t.Fatalf("the %s subject %q is not recognised as an arbitration record, so the "+
				"transition writer would accept it as a caller member", name, subj)
		}
	}

	regFilter := mqttInflightWalkFilter(testIDHash, testGen)
	markFilter := mqttInflightMarkerWalkFilter(testIDHash, testGen)
	for name, subj := range subjects {
		if testAuthoritySubjectMatchesFilter(subj, regFilter) {
			t.Fatalf("the %s subject %q is enumerated by the register walk %q", name, subj, regFilter)
		}
		if testAuthoritySubjectMatchesFilter(subj, markFilter) {
			t.Fatalf("the %s subject %q is enumerated by the marker walk %q", name, subj, markFilter)
		}
	}
}

// TestClassTokenCannotBeMistakenForAnIdentity guards what actually excludes the
// authority subject, whose token count matches a register's. Pinning it here means a future
// change to either token length fails this test rather than silently making a class subject
// attributable to a client.
func TestClassTokenCannotBeMistakenForAnIdentity(t *testing.T) {
	if mqttValidIDHashToken(mqttOutboundRecordClassTk) {
		t.Fatalf("the class token %q validates as a client-ID hash, so a class subject "+
			"could be attributed to a client", mqttOutboundRecordClassTk)
	}
	if mqttValidSessGen(mqttOutboundRecordClassTk) {
		t.Fatalf("the class token %q validates as an incarnation generation", mqttOutboundRecordClassTk)
	}
	for _, cls := range []string{mqttAuthorityClassTk, mqttClosureClassTk, mqttOutcomeClassTk} {
		if mqttValidIDHashToken(cls) {
			t.Fatalf("the per-class token %q validates as a client-ID hash", cls)
		}
		if mqttValidSessGen(cls) {
			t.Fatalf("the per-class token %q validates as an incarnation generation", cls)
		}
	}
}

// TestWalkFiltersAreClassScoped asserts each class walk names its own class and one
// client, so an enumeration cannot return another class's records and spend a bounded
// budget on them.
func TestWalkFiltersAreClassScoped(t *testing.T) {
	closures := mqttGenClosureWalkFilter(testIDHash)
	outcomes := mqttTransitionOutcomeWalkFilter(testIDHash, testGen)

	if !testAuthoritySubjectMatchesFilter(mqttGenClosureSubject(testIDHash, testGen), closures) {
		t.Fatalf("the closure walk %q does not enumerate its own client's closure", closures)
	}
	if !testAuthoritySubjectMatchesFilter(mqttTransitionOutcomeSubject(testIDHash, testGen, "txn-1"), outcomes) {
		t.Fatalf("the outcome walk %q does not enumerate its own client's outcome", outcomes)
	}
	if testAuthoritySubjectMatchesFilter(mqttTransitionOutcomeSubject(testIDHash, testGen, "txn-1"), closures) {
		t.Fatalf("the closure walk %q enumerates a transition outcome", closures)
	}
	if testAuthoritySubjectMatchesFilter(mqttGenClosureSubject(testIDHash, testGen), outcomes) {
		t.Fatalf("the outcome walk %q enumerates a generation closure", outcomes)
	}
	if testAuthoritySubjectMatchesFilter(mqttGenClosureSubject("zzzz9999", testGen), closures) {
		t.Fatalf("the closure walk %q enumerates another client's closure", closures)
	}
	// A LIFECYCLE-SCOPED OUTCOME IS NO GENERATION'S RECORD: a generation's walk, which is what
	// reclaims it, must not reach one of the lifecycle the generation belongs to.
	l, _, _ := mqttParseGenToken(testGen)
	life, err := mqttLifecycleScope(l)
	if err != nil {
		t.Fatal(err)
	}
	if testAuthoritySubjectMatchesFilter(mqttTransitionOutcomeSubject(testIDHash, life, "txn-1"), outcomes) {
		t.Fatalf("the generation's outcome walk %q enumerates a lifecycle-scoped outcome", outcomes)
	}
}

// ---------------------------------------------------------------------------
// Raw-byte schema refusals
// ---------------------------------------------------------------------------
//
// These go through the DECODER on persisted bytes, which is the only thing that can
// establish the compatibility boundary. A struct-level test cannot: the defect being
// guarded against is a missing or null member decoding to a zero value that happens to name
// a real, permissive state, and a Go struct literal can never express "member absent".

// TestAuthorityDecodeRefusesUnsupportedBytes pins that an authority body this broker
// does not write is refused rather than normalised.
func TestAuthorityDecodeRefusesUnsupportedBytes(t *testing.T) {
	good := testServingAuthorityJSON(t)
	if _, err := mqttDecodeOutboundAuthority(good, testID, testEpoch); err != nil {
		t.Fatalf("a well-formed authority was refused: %v", err)
	}

	for name, body := range map[string]string{
		"state absent":     testWithout(t, good, "state"),
		"state null":       testWith(t, good, "state", "null"),
		"state zero":       testWith(t, good, "state", "0"),
		"rev absent":       testWithout(t, good, "rev"),
		"rev null":         testWith(t, good, "rev", "null"),
		"owner_txn absent": testWithout(t, good, "owner_txn"),
		"unknown member":   testWith(t, good, "unexpected", `"x"`),
		"duplicate member": strings.Replace(string(good), `{`, `{"rev":9,`, 1),
	} {
		if _, err := mqttDecodeOutboundAuthority([]byte(body), testID, testEpoch); err == nil {
			t.Fatalf("an authority with %s was admitted; the decode is the compatibility "+
				"boundary and a typed decode would have normalised it", name)
		}
	}

	// A body whose zero-valued state would have read as SERVING is the sharpest case: the
	// constants start at 1 precisely so that even a bypassed decode cannot serve it.
	if mqttAuthorityState(0) == mqttAuthorityServing {
		t.Fatalf("the zero authority state names SERVING, so an unsupported body would read " +
			"as the most permissive state available")
	}
	if mqttOutcomeVerdict(0) == mqttOutcomeCommitted {
		t.Fatalf("the zero outcome verdict names COMMITTED")
	}
}

// TestOutcomeDecodeIsVerdictScoped pins that the legal member set follows the verdict:
// an aborted outcome carrying ownership members is contradictory, not merely
// over-specified, and admitting it would let a caller read ownership out of a transition
// that never happened.
func TestOutcomeDecodeIsVerdictScoped(t *testing.T) {
	committed := testCommittedOutcomeJSON(t)
	if _, err := mqttDecodeTransitionOutcome(committed, testID, testEpoch, "txn-1"); err != nil {
		t.Fatalf("a well-formed committed outcome was refused: %v", err)
	}
	aborted := testAbortedOutcomeJSON(t)
	if _, err := mqttDecodeTransitionOutcome(aborted, testID, testEpoch, "txn-1"); err != nil {
		t.Fatalf("a well-formed aborted outcome was refused: %v", err)
	}

	for name, body := range map[string]string{
		"aborted with rev":                            testWith(t, aborted, "rev", "7"),
		"aborted with gen":                            testWith(t, aborted, "gen", `"`+testGen+`"`),
		"aborted with tenure":                         testWith(t, aborted, "tenure", `"tenure-1"`),
		"aborted with owner_txn":                      testWith(t, aborted, "owner_txn", `"own-1"`),
		"aborted with digest":                         testWith(t, aborted, "digest", `"d"`),
		"verdict absent":                              testWithout(t, committed, "verdict"),
		"verdict null":                                testWith(t, committed, "verdict", "null"),
		"verdict unsupported":                         testWith(t, committed, "verdict", "9"),
		"committed without rev":                       testWithout(t, committed, "rev"),
		"committed without digest":                    testWithout(t, committed, "digest"),
		"scope absent":                                testWithout(t, committed, "scope"),
		"lifecycle scope on a generation-scoped kind": testWith(t, committed, "scope", `"L.0000000002"`),
	} {
		if _, err := mqttDecodeTransitionOutcome([]byte(body), testID, testEpoch, "txn-1"); err == nil {
			t.Fatalf("an outcome with %s was admitted", name)
		}
	}
}

// TestDecodeAttributesByIdentityNotHash pins that a record is attributed by its exact
// client and epoch. A hash routes a lookup; attributing on it alone would let a collision
// be served as the requested client, and a foreign epoch's record be read as this
// account's.
func TestDecodeAttributesByIdentityNotHash(t *testing.T) {
	good := testServingAuthorityJSON(t)
	if _, err := mqttDecodeOutboundAuthority(good, "client-b", testEpoch); err == nil {
		t.Fatalf("an authority naming %q was admitted for client-b", testID)
	}
	if _, err := mqttDecodeOutboundAuthority(good, testID, "epoch-2"); err == nil {
		t.Fatalf("an authority from another recovery epoch was admitted")
	}
}

// ---------------------------------------------------------------------------
// Semantic agreement
// ---------------------------------------------------------------------------

// TestAttemptPredecessorMatchesRevision pins that only a first admission has no
// predecessor. Any later revision displaces something, and an attempt that cannot name what
// it displaces could finalize against whatever it found.
func TestAttemptPredecessorMatchesRevision(t *testing.T) {
	first := testAuthorityAttempt()
	first.Rev = 1
	first.PrevTxn = _EMPTY_
	if err := mqttValidateTransitionAttempt(first); err != nil {
		t.Fatalf("a well-formed first admission was refused: %v", err)
	}

	later := testAuthorityAttempt()
	later.Rev = 2
	later.PrevTxn = "own-0"
	if err := mqttValidateTransitionAttempt(later); err != nil {
		t.Fatalf("a well-formed later attempt was refused: %v", err)
	}

	bad := testAuthorityAttempt()
	bad.Rev = 2
	bad.PrevTxn = _EMPTY_
	if err := mqttValidateTransitionAttempt(bad); err == nil {
		t.Fatalf("an attempt at revision 2 with no predecessor was admitted")
	}

	bad = testAuthorityAttempt()
	bad.Rev = 1
	bad.PrevTxn = "own-0"
	if err := mqttValidateTransitionAttempt(bad); err == nil {
		t.Fatalf("a first admission naming a predecessor was admitted")
	}
}

// TestOutcomeMustMatchTheAttemptItAnswers pins the check that stops a receipt found at
// a transition id from being read as the answer to a different question. Without it a
// caller could commit ownership for one tenure while permanently recording a receipt naming
// another, and resolution would still report LANDED.
func TestOutcomeMustMatchTheAttemptItAnswers(t *testing.T) {
	a := testAuthorityAttempt()
	o := testCommittedOutcomeFor(a)
	if err := mqttOutcomeMatchesAttempt(o, a); err != nil {
		t.Fatalf("an outcome describing its own attempt was rejected: %v", err)
	}

	for name, mutate := range map[string]func(*mqttTransitionOutcome){
		"operation":   func(o *mqttTransitionOutcome) { o.Op = "other-op" },
		"kind":        func(o *mqttTransitionOutcome) { o.Kind = mqttTransitionTeardownClaim },
		"predecessor": func(o *mqttTransitionOutcome) { o.PrevTxn = "own-9" },
		"revision":    func(o *mqttTransitionOutcome) { o.Rev = 99 },
		"generation":  func(o *mqttTransitionOutcome) { o.Gen = testGen2 },
		"tenure":      func(o *mqttTransitionOutcome) { o.Tenure = "other-tenure" },
		"ownership":   func(o *mqttTransitionOutcome) { o.OwnerTxn = "other-own" },
	} {
		m := testCommittedOutcomeFor(a)
		mutate(m)
		if err := mqttOutcomeMatchesAttempt(m, a); err == nil {
			t.Fatalf("an outcome describing a different %s was reported as this attempt's answer", name)
		}
	}
}

// TestDigestBindsTheWholeMutation pins that the receipt describes the batch actually
// sent. A digest over the attempt alone would be satisfied by any batch sharing a
// transition id.
func TestDigestBindsTheWholeMutation(t *testing.T) {
	a := testAuthorityAttempt()
	auth := []byte(`{"rev":2}`)
	base := mqttTransitionDigest(a, 7, auth, nil)

	if base == mqttTransitionDigest(a, 8, auth, nil) {
		t.Fatalf("the digest ignores the authority expectation")
	}
	if base == mqttTransitionDigest(a, 7, []byte(`{"rev":3}`), nil) {
		t.Fatalf("the digest ignores the authority body")
	}
	withMember := mqttTransitionDigest(a, 7, auth, []mqttTransitionMember{
		{subj: "x.y", expect: 1, body: []byte("b")},
	})
	if base == withMember {
		t.Fatalf("the digest ignores caller members")
	}
	if withMember == mqttTransitionDigest(a, 7, auth, []mqttTransitionMember{
		{subj: "x.y", expect: 2, body: []byte("b")},
	}) {
		t.Fatalf("the digest ignores a member's expectation")
	}
	if withMember == mqttTransitionDigest(a, 7, auth, []mqttTransitionMember{
		{subj: "x.y", expect: 1, body: []byte("c")},
	}) {
		t.Fatalf("the digest ignores a member's body")
	}

	// Member ORDER must not change the digest — the batch's content is what is being
	// described — while member IDENTITY must.
	two := []mqttTransitionMember{
		{subj: "x.y", expect: 1, body: []byte("b")},
		{subj: "x.z", expect: 2, body: []byte("c")},
	}
	rev := []mqttTransitionMember{two[1], two[0]}
	if mqttTransitionDigest(a, 7, auth, two) != mqttTransitionDigest(a, 7, auth, rev) {
		t.Fatalf("the digest depends on the order the caller assembled its members in")
	}

	b := testAuthorityAttempt()
	b.OwnerTxn = "own-other"
	if base == mqttTransitionDigest(b, 7, auth, nil) {
		t.Fatalf("the digest ignores the attempt's proposed ownership")
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func testAuthorityAttempt() *mqttTransitionAttempt {
	return &mqttTransitionAttempt{
		ID: testID, Epoch: testEpoch, Txn: "txn-1", Scope: testGen, Op: "op-1",
		Kind: mqttTransitionAdmitExchange, PrevTxn: "own-0",
		Rev: 2, Gen: testGen, Tenure: "tenure-0", OwnerTxn: "own-0",
	}
}

func testCommittedOutcomeFor(a *mqttTransitionAttempt) *mqttTransitionOutcome {
	return &mqttTransitionOutcome{
		ID: a.ID, Epoch: a.Epoch, Txn: a.Txn, Scope: a.Scope, Op: a.Op, Kind: a.Kind,
		Verdict: mqttOutcomeCommitted, PrevTxn: a.PrevTxn,
		Rev: a.Rev, Gen: a.Gen, Tenure: a.Tenure, OwnerTxn: a.OwnerTxn, Digest: "d1",
	}
}

func testServingAuthorityJSON(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(&mqttOutboundAuthority{
		ID: testID, Epoch: testEpoch, Gen: testGen, Tenure: "tenure-1",
		State: mqttAuthorityServing, Rev: 2, Txn: "txn-1", OwnerTxn: "own-1",
	})
	if err != nil {
		t.Fatalf("marshalling the fixture authority: %v", err)
	}
	return b
}

func testCommittedOutcomeJSON(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(testCommittedOutcomeFor(testAuthorityAttempt()))
	if err != nil {
		t.Fatalf("marshalling the fixture outcome: %v", err)
	}
	return b
}

func testAbortedOutcomeJSON(t *testing.T) []byte {
	t.Helper()
	a := testAuthorityAttempt()
	b, err := json.Marshal(&mqttTransitionOutcome{
		ID: a.ID, Epoch: a.Epoch, Txn: a.Txn, Scope: a.Scope, Op: a.Op, Kind: a.Kind,
		Verdict: mqttOutcomeAborted, PrevTxn: a.PrevTxn,
	})
	if err != nil {
		t.Fatalf("marshalling the fixture outcome: %v", err)
	}
	return b
}

// testWith returns body with member set to raw, added if absent. Operating on the
// PERSISTED BYTES is the point: a struct literal cannot express a member that is absent or
// explicitly null, which is exactly the shape the decoder must refuse.
func testWith(t *testing.T, body []byte, member, raw string) string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decoding the fixture: %v", err)
	}
	m[member] = json.RawMessage(raw)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("re-encoding the fixture: %v", err)
	}
	return string(out)
}

// testWithout returns body with member removed.
func testWithout(t *testing.T, body []byte, member string) string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decoding the fixture: %v", err)
	}
	delete(m, member)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("re-encoding the fixture: %v", err)
	}
	return string(out)
}

// testAuthoritySubjectMatchesFilter reports whether subject is matched by a NATS subject filter
// built from literal tokens plus the single-token wildcard. The classes here never use the
// multi-token wildcard, and a filter that grew one would not be matched correctly by this
// helper — which is the point: it refuses rather than quietly approximating.
func testAuthoritySubjectMatchesFilter(subject, filter string) bool {
	if strings.Contains(filter, ">") {
		panic("issue #107 subject filters never use the multi-token wildcard")
	}
	sTok := strings.Split(subject, ".")
	fTok := strings.Split(filter, ".")
	if len(sTok) != len(fTok) {
		return false
	}
	for i := range fTok {
		if fTok[i] == "*" {
			continue
		}
		if fTok[i] != sTok[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// The transition write boundary
// ---------------------------------------------------------------------------
//
// Every refusal below returns before the batch reaches the network, so these run against a
// bare mqttJSA. That is deliberate: the boundary's whole claim is that an unsupported
// transition is never SENT, and a test that needed a stream to establish it would be
// testing the stream instead.

// TestTransitionRefusesForeignMembers pins the boundary Codex's implementation review
// named: atomicity commits what it is given, so a caller member naming a closure or another
// transition's outcome would be written faithfully and the permanence these records rest on
// would be a caller convention rather than a property.
func TestTransitionRefusesForeignMembers(t *testing.T) {
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}
	idHash := getHash(testID)

	for name, subj := range map[string]string{
		"a generation closure":       mqttGenClosureSubject(idHash, testGen),
		"another transition outcome": mqttTransitionOutcomeSubject(idHash, testGen, "txn-9"),
		"this transition's outcome":  mqttTransitionOutcomeSubject(idHash, testGen, "txn-1"),
		"the authority itself":       mqttOutboundAuthoritySubject(idHash),
	} {
		tr := testTransition()
		tr.Extra = []mqttTransitionMember{{subj: subj, expect: 0, body: []byte("{}")}}
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition carrying %s as a caller member was sent", name)
		}
	}

	// A duplicate ordinary subject is refused too: the digest sorts members, so two
	// members on one subject would otherwise be indistinguishable from one.
	tr := testTransition()
	tr.Extra = []mqttTransitionMember{
		{subj: "$MQTT.other.a", expect: 0, body: []byte("{}")},
		{subj: "$MQTT.other.a", expect: 1, body: []byte("{}")},
	}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a transition naming one subject twice was sent")
	}

	// The guard must not be so broad that it refuses the ordinary members the transition
	// exists to carry. An exchange subject is not an arbitration record, and a guard that
	// said otherwise would refuse every real transition while still passing the cases
	// above.
	if mqttIsOutboundRecordSubject(mqttInflightSubject(idHash, testGen, 1)) {
		t.Fatalf("an ordinary in-flight register subject is classified as an arbitration record")
	}
}

// TestTransitionRefusesAnAuthorityItsAttemptDidNotDescribe pins that the permanent
// receipt cannot describe a mutation that never happened: the installed body must be the
// ownership the attempt proposed.
func TestTransitionRefusesAnAuthorityItsAttemptDidNotDescribe(t *testing.T) {
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}

	for name, mutate := range map[string]func(*mqttAuthorityTransition){
		"generation": func(tr *mqttAuthorityTransition) { tr.Auth.Gen = testGen2 },
		"tenure":     func(tr *mqttAuthorityTransition) { tr.Auth.Tenure = "tenure-other" },
		"ownership":  func(tr *mqttAuthorityTransition) { tr.Auth.OwnerTxn = "own-other" },
		"revision":   func(tr *mqttAuthorityTransition) { tr.Auth.Rev = 9 },
		"transition": func(tr *mqttAuthorityTransition) { tr.Auth.Txn = "txn-other" },
	} {
		tr := testTransition()
		mutate(tr)
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition installing a different %s from its attempt was sent", name)
		}
	}
}

func testTransition() *mqttAuthorityTransition {
	a := testAuthorityAttempt()
	return &mqttAuthorityTransition{
		Attempt: a,
		Prev: &mqttOutboundAuthority{
			ID: testID, Epoch: testEpoch, Gen: testGen, Tenure: "tenure-0",
			State: mqttAuthorityServing, Rev: 1, Txn: "txn-0", OwnerTxn: "own-0",
			Life: testServingLife(),
		},
		// An admit-exchange preserves its predecessor's ownership, which is what makes it
		// work admitted UNDER a tenure rather than a move of one.
		PrevSeq: 11,
		Auth: &mqttOutboundAuthority{
			ID: a.ID, Epoch: a.Epoch, Gen: a.Gen, Tenure: a.Tenure,
			State: mqttAuthorityServing, Rev: a.Rev, Txn: a.Txn, OwnerTxn: a.OwnerTxn,
			Life: testServingLife(),
		},
	}
}

// testServingLife is the lifecycle of testGen, serving it as its first generation.
func testServingLife() *mqttLifecycleState {
	l, _, _ := mqttParseGenToken(testGen)
	return &mqttLifecycleState{
		L: l, Phase: mqttLifecycleServing, NextGenOrd: 1, NextAcceptOrd: 1, NextAnchorOrd: 1,
		Accept: mqttAcceptance{Ord: 0, At: 1, Tenure: "tenure-0", Nonce: "nonce-0"},
	}
}

// TestAbortedOutcomeValidatorRefusesOwnership reaches the SEMANTIC validator directly,
// which the decoder tests cannot.
//
// mqttDecodeTransitionOutcome refuses an aborted body carrying ownership members at the
// member-set level, before mqttValidateTransitionOutcome's own check runs — so a mutation
// deleting that check survives every decoder-driven fixture. The check is not redundant:
// abortTransition builds its outcome IN MEMORY and validates it before writing, so this is
// the layer that stops a malformed abort being published to a permanent subject.
func TestAbortedOutcomeValidatorRefusesOwnership(t *testing.T) {
	a := testAuthorityAttempt()
	base := &mqttTransitionOutcome{
		ID: a.ID, Epoch: a.Epoch, Txn: a.Txn, Scope: a.Scope, Op: a.Op, Kind: a.Kind,
		Verdict: mqttOutcomeAborted, PrevTxn: a.PrevTxn,
	}
	if err := mqttValidateTransitionOutcome(base, a.ID, a.Epoch, a.Txn); err != nil {
		t.Fatalf("a well-formed aborted outcome was refused: %v", err)
	}

	for name, mutate := range map[string]func(*mqttTransitionOutcome){
		"a revision":   func(o *mqttTransitionOutcome) { o.Rev = 7 },
		"a generation": func(o *mqttTransitionOutcome) { o.Gen = testGen },
		"a tenure":     func(o *mqttTransitionOutcome) { o.Tenure = "tenure-1" },
		"an ownership": func(o *mqttTransitionOutcome) { o.OwnerTxn = "own-1" },
		"a digest":     func(o *mqttTransitionOutcome) { o.Digest = "d1" },
	} {
		o := *base
		mutate(&o)
		if err := mqttValidateTransitionOutcome(&o, a.ID, a.Epoch, a.Txn); err == nil {
			t.Fatalf("an aborted outcome carrying %s was admitted, so a caller could read "+
				"ownership out of a transition that never landed", name)
		}
	}
}

// TestTransitionRefusesAnUnsupportedPredecessor pins that a predecessor is validated,
// not merely present. Without it a caller could hand in any struct and the
// consecutive-revision test would pass against it.
func TestTransitionRefusesAnUnsupportedPredecessor(t *testing.T) {
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}

	for name, mutate := range map[string]func(*mqttOutboundAuthority){
		"another client":       func(p *mqttOutboundAuthority) { p.ID = "client-b" },
		"another epoch":        func(p *mqttOutboundAuthority) { p.Epoch = "epoch-9" },
		"no revision":          func(p *mqttOutboundAuthority) { p.Rev = 0 },
		"an unsupported state": func(p *mqttOutboundAuthority) { p.State = mqttAuthorityState(9) },
	} {
		tr := testTransition()
		mutate(tr.Prev)
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition over a predecessor naming %s was sent", name)
		}
	}
}

// TestDigestCoversTheClosure pins the omission Codex's second review found: the closure
// is a batch member, so a transition carrying one and a transition without must not produce
// the same receipt.
func TestDigestCoversTheClosure(t *testing.T) {
	bare := testTransition()
	bare.Attempt.Kind = mqttTransitionTeardownClaim
	bare.Auth.State = mqttAuthorityClosed

	withClosure := testTransition()
	withClosure.Attempt.Kind = mqttTransitionTeardownClaim
	withClosure.Auth.State = mqttAuthorityClosed
	withClosure.Closure = &mqttGenClosure{
		ID: testID, Epoch: testEpoch, Gen: testGen, Txn: withClosure.Attempt.Txn,
	}

	bd, err := mqttTransitionMutationDigest(bare)
	if err != nil {
		t.Fatalf("digesting the bare transition: %v", err)
	}
	cd, err := mqttTransitionMutationDigest(withClosure)
	if err != nil {
		t.Fatalf("digesting the closing transition: %v", err)
	}
	if bd == cd {
		t.Fatalf("a transition with a closure and one without produce the same digest")
	}

	// The closure's own content must move it too: a closure recording another root address
	// is another closure.
	flipped := *withClosure
	c := *withClosure.Closure
	c.RootOrd++
	flipped.Closure = &c
	fd, err := mqttTransitionMutationDigest(&flipped)
	if err != nil {
		t.Fatalf("digesting the re-addressed transition: %v", err)
	}
	if fd == cd {
		t.Fatalf("closures at two root addresses produce the same digest")
	}

	other := *withClosure
	c2 := *withClosure.Closure
	c2.Gen = testGen2
	other.Closure = &c2
	od, err := mqttTransitionMutationDigest(&other)
	if err != nil {
		t.Fatalf("digesting the other-generation transition: %v", err)
	}
	if od == cd {
		t.Fatalf("closures for two generations produce the same digest")
	}
}

// TestMembersAreConfinedToTheirOwnStreamAndClient pins the confinement Codex's third
// review found still open.
//
// A JetStream batch is per stream and newAtomicBatchRequest publishes each subject
// separately, so a member naming another stream is not in the transaction at all: it is
// routed elsewhere or nowhere, and the acknowledgement cannot say which. Beyond that, a
// member must be a record class this transition may write, for THIS client and generation,
// in a shape the register boundary would accept — because atomicity validates ordering and
// never meaning.
func TestMembersAreConfinedToTheirOwnStreamAndClient(t *testing.T) {
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}
	idHash := getHash(testID)

	for name, subj := range map[string]string{
		"a session record subject":  mqttSessStreamSubjectPrefix + idHash,
		"the retained-message tree": mqttRetainedMsgsStreamSubject,
		"a wildcard":                mqttInflightStreamSubjectPrefix + idHash + "." + testGen + ".*",
		"another client's register": mqttInflightSubject("zzzz9999", testGen, 3),
		"another generation":        mqttInflightSubject(idHash, testGen2, 3),
		"an unparseable subject":    mqttInflightStreamSubjectPrefix + "nonsense",
	} {
		tr := testTransition()
		tr.Extra = []mqttTransitionMember{{subj: subj, expect: 0, body: []byte("{}")}}
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition carrying a member on %s was sent", name)
		}
	}

	// A register body goes through the same shape matrix every other register writer uses.
	tr := testTransition()
	tr.Extra = []mqttTransitionMember{{
		subj:   mqttInflightSubject(idHash, testGen, 3),
		expect: 0,
		body:   []byte(`{"not":"a register"}`),
	}}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a transition carrying an unsupported register shape was sent")
	}
}

// TestKindRulesRefuseContradictoryStates reaches the per-kind rules directly, through
// the pure rule function, because several of them refuse a transition that would otherwise
// be a perfectly valid store write — so a fixture driving the writer would be refused by
// the store for an unrelated reason and prove nothing about the rule.
func TestKindRulesRefuseContradictoryStates(t *testing.T) {
	// A FIRST ACCEPTANCE MAY NOT CLOSE: it would install an authority and a permanent closure for
	// the generation it is creating, in one batch. It is the one attempt with NO predecessor, which
	// is what makes it create-only, and it installs its acceptance PENDING (issue #133).
	admit := testFirstAcceptance(t)
	if err := mqttCheckTransitionKindRules(admit); err != nil {
		t.Fatalf("a well-formed first acceptance was refused: %v", err)
	}
	unbound := *admit
	uauth := *admit.Auth
	ul := *admit.Auth.Life
	ul.Pending = nil
	uauth.Life = &ul
	unbound.Auth = &uauth
	if err := mqttCheckTransitionKindRules(&unbound); err == nil {
		t.Fatalf("a first acceptance installed a serving authority with no pending acceptance to bind")
	}
	withClosure := *admit
	withClosure.Closure = &mqttGenClosure{
		ID: testID, Epoch: testEpoch, Gen: admit.Auth.Gen, Txn: admit.Attempt.Txn,
	}
	if err := mqttCheckTransitionKindRules(&withClosure); err == nil {
		t.Fatalf("a first acceptance carrying a generation closure was admitted")
	}

	// A first acceptance's ownership is its OWN identity: there is no predecessor whose ownership
	// it could inherit, so ownership it did not mint is ownership nothing established.
	borrowed := *admit
	bauth := *admit.Auth
	bauth.OwnerTxn = "own-borrowed"
	borrowed.Auth = &bauth
	if err := mqttCheckTransitionKindRules(&borrowed); err == nil {
		t.Fatalf("a first acceptance installing ownership it did not mint was admitted")
	}

}

// TestAdmissionRequiresAServingPredecessor is the rule the whole item exists for, at
// the layer that enforces it: new outbound work may not be admitted over an authority that
// has closed admission. A closed enum with no rules attached would let exactly that
// through, which is admission-after-closure.
func TestAdmissionRequiresAServingPredecessor(t *testing.T) {
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}

	for _, kind := range []mqttTransitionKind{mqttTransitionAdmitExchange, mqttTransitionAcquireAction} {
		for name, state := range map[string]mqttAuthorityState{
			"closed": mqttAuthorityClosed,
		} {
			tr := testTransition()
			tr.Attempt.Kind = kind
			tr.Prev.State = state
			if _, err := jsa.commitAuthorityTransition(tr); err == nil {
				t.Fatalf("a %s transition over a %s predecessor was sent: that is admission "+
					"after admission closed", kind, name)
			}
		}

		// It must also not move ownership: this authorizes work UNDER the standing tenure.
		tr := testTransition()
		tr.Attempt.Kind = kind
		tr.Attempt.Tenure = "tenure-moved"
		tr.Auth.Tenure = "tenure-moved"
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a %s transition moved ownership", kind)
		}

		// And it must not write a closure.
		tr = testTransition()
		tr.Attempt.Kind = kind
		tr.Closure = &mqttGenClosure{
			ID: testID, Epoch: testEpoch, Gen: testGen, Txn: tr.Attempt.Txn,
		}
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a %s transition wrote a generation closure", kind)
		}
	}
}

// TestEveryAuthorityShapeSurvivesItsOwnDecoder closes the class the retirement-request
// member set was one instance of.
//
// The defect was not a typo: mqttRetirementRequest gained sess_wid and sess_seq and its
// closed member set did not, so a preparing authority carrying a retirement request was
// WRITABLE AND UNREADABLE — the record landed, and the broker's own decoder then reported it
// corrupt, which for an arbitration record means the client is refused rather than served by
// the wrong tenure. No test caught it because none loaded such an authority back.
//
// A per-field fix would leave the next added member free to do the same, so what is pinned
// here is the PROPERTY: every authority shape this broker composes round-trips through the
// decoder that reads it. A new member that is not in its set fails here rather than in
// recovery.
func TestEveryAuthorityShapeSurvivesItsOwnDecoder(t *testing.T) {
	serving := &mqttOutboundAuthority{
		ID: testID, Epoch: testEpoch, Gen: testGen, Tenure: "tenure-1",
		State: mqttAuthorityServing, Rev: 1, Txn: "txn-1", OwnerTxn: "txn-1",
	}
	withLife := testFirstAcceptance(t).Auth
	resumed := *withLife
	rl := *withLife.Life
	rl.Pending = &mqttPendingAcceptance{Kind: mqttPendingResume, AcceptOrd: rl.Accept.Ord, Nonce: rl.Accept.Nonce,
		At: rl.Accept.At, Prior: &mqttPriorLifetime{ExpiryInterval: 60}}
	rl.Anchors = []mqttWillAnchor{{Ord: 0, AcceptOrd: 0, State: mqttAnchorArmed}}
	rl.NextAnchorOrd = 1
	resumed.Life = &rl
	closed := &mqttOutboundAuthority{
		ID: testID, Epoch: testEpoch, Gen: testGen, Tenure: "tenure-1",
		State: mqttAuthorityClosed, Rev: 2, Txn: "txn-2", OwnerTxn: "txn-1",
	}

	for name, a := range map[string]*mqttOutboundAuthority{
		"a serving authority":                        serving,
		"a first acceptance, its lifecycle pending":  withLife,
		"a resumed acceptance holding a Will anchor": &resumed,
		"a closed authority":                         closed,
	} {
		if verr := mqttValidateOutboundAuthority(a, testID, testEpoch); verr != nil {
			t.Fatalf("%s is not a shape this broker composes: %v", name, verr)
		}
		body, merr := json.Marshal(a)
		if merr != nil {
			t.Fatalf("marshalling %s: %v", name, merr)
		}
		back, derr := mqttDecodeOutboundAuthority(body, testID, testEpoch)
		if derr != nil {
			t.Fatalf("%s is writable and unreadable: its own decoder refuses it: %v", name, derr)
		}
		if back.State != a.State || back.Gen != a.Gen || back.Tenure != a.Tenure ||
			back.OwnerTxn != a.OwnerTxn || back.Rev != a.Rev {
			t.Fatalf("%s did not round-trip: %+v became %+v", name, a, back)
		}
		ab, _ := json.Marshal(a.Life)
		bb, _ := json.Marshal(back.Life)
		if string(ab) != string(bb) {
			t.Fatalf("%s lost its lifecycle section in the round trip: %s became %s", name, ab, bb)
		}
	}
}

// TestTransitionEnforcesItsPredecessor pins that a revision is not admitted merely
// because it satisfies a storage expectation. The sequence moves for completions that
// change no ownership, so without this a transition could install any revision over any
// predecessor.
func TestTransitionEnforcesItsPredecessor(t *testing.T) {
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}

	for name, mutate := range map[string]func(*mqttAuthorityTransition){
		"no predecessor at all":          func(tr *mqttAuthorityTransition) { tr.Prev = nil },
		"a predecessor at sequence zero": func(tr *mqttAuthorityTransition) { tr.PrevSeq = 0 },
		"a non-consecutive revision": func(tr *mqttAuthorityTransition) {
			tr.Prev.Rev = 7
		},
		"a predecessor owning something else": func(tr *mqttAuthorityTransition) {
			tr.Prev.OwnerTxn = "own-other"
		},
	} {
		tr := testTransition()
		mutate(tr)
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition with %s was sent", name)
		}
	}

	// A FIRST ACCEPTANCE is the one attempt with no predecessor, and naming one is refused from
	// the other side.
	tr := testFirstAcceptance(t)
	tr.Prev, tr.PrevSeq = testTransition().Prev, 3
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a first acceptance naming a predecessor authority was sent")
	}
}

// testFirstAcceptance composes a creation the way an accepted CONNECT does, against no store: an
// absent authority, an empty registry shard, and a record member over no record.
func testFirstAcceptance(t *testing.T) *mqttAuthorityTransition {
	t.Helper()
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}
	reg := mqttRegistryLoad{rec: mqttNewLifecycleRegistryShard(testEpoch, mqttLifecycleRegistryShardFor(getHash(testID))),
		status: mqttOutboundAbsent}
	ca, err := jsa.composeAcceptance(mqttAcceptRequest{
		ID: testID, Epoch: testEpoch, Txn: "txn-first", Tenure: "tenure-first", Nonce: "intent-first", At: 1,
	}, mqttAuthorityLoad{status: mqttOutboundAbsent}, reg)
	if err != nil {
		t.Fatalf("composing a first acceptance: %v", err)
	}
	ca.T.Record = &mqttTransitionMember{subj: mqttSessSubject(_EMPTY_, getHash(testID)), body: []byte(`{}`)}
	return ca.T
}
