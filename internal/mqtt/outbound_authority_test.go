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
	"testing"
)

// Syntactically valid tokens: a generation is mqttSessGenLen alphanumerics, a client-ID
// hash is sysHashLen of them.
const (
	testF105Gen    = "0123456789abcdefghijkl"
	testF105Gen2   = "0123456789abcdefghijkX"
	testF105IDHash = "abcd1234"
	testF105ID     = "client-a"
	testF105Epoch  = "epoch-1"
)

// TestF105SubjectsAreUnreachableByTheRegisterMachinery is the tripwire the class token
// exists for.
//
// The F-105 record classes share $MQTT_inflight with the outbound registers, because
// JetStream's atomic batches are per stream and the authority has to commit with the
// exchange mutation it authorizes. Sharing the stream means sharing the subject space with
// a reap that deletes what its enumeration returns, so the separation is asserted here
// rather than left to the reader of a subject builder.
//
// Note which guard does the work for each class. Closure and outcome subjects carry four
// tokens past the prefix and the register parser wants exactly three, so they are excluded
// structurally. THE AUTHORITY SUBJECT HAS EXACTLY THREE, so only token validation excludes
// it — TestF105ClassTokenCannotBeMistakenForAnIdentity is the test that actually protects
// that one, and an earlier comment in this file claimed the count guard covered all three.
func TestF105SubjectsAreUnreachableByTheRegisterMachinery(t *testing.T) {
	subjects := map[string]string{
		"authority": mqttOutboundAuthoritySubject(testF105IDHash),
		"closure":   mqttGenClosureSubject(testF105IDHash, testF105Gen),
		"outcome":   mqttTransitionOutcomeSubject(testF105IDHash, "txn-1"),
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
		if !mqttIsF105Subject(subj) {
			t.Fatalf("the %s subject %q is not recognised as an arbitration record, so the "+
				"transition writer would accept it as a caller member", name, subj)
		}
	}

	regFilter := mqttInflightWalkFilter(testF105IDHash, testF105Gen)
	markFilter := mqttInflightMarkerWalkFilter(testF105IDHash, testF105Gen)
	for name, subj := range subjects {
		if testF105SubjectMatchesFilter(subj, regFilter) {
			t.Fatalf("the %s subject %q is enumerated by the register walk %q", name, subj, regFilter)
		}
		if testF105SubjectMatchesFilter(subj, markFilter) {
			t.Fatalf("the %s subject %q is enumerated by the marker walk %q", name, subj, markFilter)
		}
	}
}

// TestF105ClassTokenCannotBeMistakenForAnIdentity guards what actually excludes the
// authority subject, whose token count matches a register's. Pinning it here means a future
// change to either token length fails this test rather than silently making a class subject
// attributable to a client.
func TestF105ClassTokenCannotBeMistakenForAnIdentity(t *testing.T) {
	if mqttValidIDHashToken(mqttF105ClassTk) {
		t.Fatalf("the class token %q validates as a client-ID hash, so a class subject "+
			"could be attributed to a client", mqttF105ClassTk)
	}
	if mqttValidSessGen(mqttF105ClassTk) {
		t.Fatalf("the class token %q validates as an incarnation generation", mqttF105ClassTk)
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

// TestF105WalkFiltersAreClassScoped asserts each class walk names its own class and one
// client, so an enumeration cannot return another class's records and spend a bounded
// budget on them.
func TestF105WalkFiltersAreClassScoped(t *testing.T) {
	closures := mqttGenClosureWalkFilter(testF105IDHash)
	outcomes := mqttTransitionOutcomeWalkFilter(testF105IDHash)

	if !testF105SubjectMatchesFilter(mqttGenClosureSubject(testF105IDHash, testF105Gen), closures) {
		t.Fatalf("the closure walk %q does not enumerate its own client's closure", closures)
	}
	if !testF105SubjectMatchesFilter(mqttTransitionOutcomeSubject(testF105IDHash, "txn-1"), outcomes) {
		t.Fatalf("the outcome walk %q does not enumerate its own client's outcome", outcomes)
	}
	if testF105SubjectMatchesFilter(mqttTransitionOutcomeSubject(testF105IDHash, "txn-1"), closures) {
		t.Fatalf("the closure walk %q enumerates a transition outcome", closures)
	}
	if testF105SubjectMatchesFilter(mqttGenClosureSubject(testF105IDHash, testF105Gen), outcomes) {
		t.Fatalf("the outcome walk %q enumerates a generation closure", outcomes)
	}
	if testF105SubjectMatchesFilter(mqttGenClosureSubject("zzzz9999", testF105Gen), closures) {
		t.Fatalf("the closure walk %q enumerates another client's closure", closures)
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

// TestF105AuthorityDecodeRefusesUnsupportedBytes pins that an authority body this broker
// does not write is refused rather than normalised.
func TestF105AuthorityDecodeRefusesUnsupportedBytes(t *testing.T) {
	good := testF105ServingAuthorityJSON(t)
	if _, err := mqttDecodeOutboundAuthority(good, testF105ID, testF105Epoch); err != nil {
		t.Fatalf("a well-formed authority was refused: %v", err)
	}

	for name, body := range map[string]string{
		"state absent":     testF105Without(t, good, "state"),
		"state null":       testF105With(t, good, "state", "null"),
		"state zero":       testF105With(t, good, "state", "0"),
		"rev absent":       testF105Without(t, good, "rev"),
		"rev null":         testF105With(t, good, "rev", "null"),
		"owner_txn absent": testF105Without(t, good, "owner_txn"),
		"unknown member":   testF105With(t, good, "unexpected", `"x"`),
		"duplicate member": strings.Replace(string(good), `{`, `{"rev":9,`, 1),
	} {
		if _, err := mqttDecodeOutboundAuthority([]byte(body), testF105ID, testF105Epoch); err == nil {
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

// TestF105OutcomeDecodeIsVerdictScoped pins that the legal member set follows the verdict:
// an aborted outcome carrying ownership members is contradictory, not merely
// over-specified, and admitting it would let a caller read ownership out of a transition
// that never happened.
func TestF105OutcomeDecodeIsVerdictScoped(t *testing.T) {
	committed := testF105CommittedOutcomeJSON(t)
	if _, err := mqttDecodeTransitionOutcome(committed, testF105ID, testF105Epoch, "txn-1"); err != nil {
		t.Fatalf("a well-formed committed outcome was refused: %v", err)
	}
	aborted := testF105AbortedOutcomeJSON(t)
	if _, err := mqttDecodeTransitionOutcome(aborted, testF105ID, testF105Epoch, "txn-1"); err != nil {
		t.Fatalf("a well-formed aborted outcome was refused: %v", err)
	}

	for name, body := range map[string]string{
		"aborted with rev":         testF105With(t, aborted, "rev", "7"),
		"aborted with gen":         testF105With(t, aborted, "gen", `"`+testF105Gen+`"`),
		"aborted with tenure":      testF105With(t, aborted, "tenure", `"tenure-1"`),
		"aborted with owner_txn":   testF105With(t, aborted, "owner_txn", `"own-1"`),
		"aborted with digest":      testF105With(t, aborted, "digest", `"d"`),
		"verdict absent":           testF105Without(t, committed, "verdict"),
		"verdict null":             testF105With(t, committed, "verdict", "null"),
		"verdict unsupported":      testF105With(t, committed, "verdict", "9"),
		"committed without rev":    testF105Without(t, committed, "rev"),
		"committed without digest": testF105Without(t, committed, "digest"),
	} {
		if _, err := mqttDecodeTransitionOutcome([]byte(body), testF105ID, testF105Epoch, "txn-1"); err == nil {
			t.Fatalf("an outcome with %s was admitted", name)
		}
	}
}

// TestF105DecodeAttributesByIdentityNotHash pins that a record is attributed by its exact
// client and epoch. A hash routes a lookup; attributing on it alone would let a collision
// be served as the requested client, and a foreign epoch's record be read as this
// account's.
func TestF105DecodeAttributesByIdentityNotHash(t *testing.T) {
	good := testF105ServingAuthorityJSON(t)
	if _, err := mqttDecodeOutboundAuthority(good, "client-b", testF105Epoch); err == nil {
		t.Fatalf("an authority naming %q was admitted for client-b", testF105ID)
	}
	if _, err := mqttDecodeOutboundAuthority(good, testF105ID, "epoch-2"); err == nil {
		t.Fatalf("an authority from another recovery epoch was admitted")
	}
}

// ---------------------------------------------------------------------------
// Semantic agreement
// ---------------------------------------------------------------------------

// TestF105HandoffIntentMustAgreeWithItsAuthority pins that a preparing authority's intent
// names that authority's OWN ownership as its predecessor. An intent naming something else
// is not a weaker intent: finalizing it would displace ownership nobody checked.
func TestF105HandoffIntentMustAgreeWithItsAuthority(t *testing.T) {
	base := testF105PreparingAuthority()
	if err := mqttValidateOutboundAuthority(base, testF105ID, testF105Epoch); err != nil {
		t.Fatalf("a well-formed preparing authority was refused: %v", err)
	}

	for name, mutate := range map[string]func(*mqttOutboundAuthority){
		"generation": func(a *mqttOutboundAuthority) { a.Intent.PrevGen = testF105Gen2 },
		"tenure":     func(a *mqttOutboundAuthority) { a.Intent.PrevTenure = "other" },
		"ownership":  func(a *mqttOutboundAuthority) { a.Intent.PrevOwnerTxn = "other" },
	} {
		a := testF105PreparingAuthority()
		mutate(a)
		if err := mqttValidateOutboundAuthority(a, testF105ID, testF105Epoch); err == nil {
			t.Fatalf("a preparing authority whose intent names a different predecessor %s "+
				"was admitted", name)
		}
	}

	// A retirement request must name the same predecessor the intent does, or a stale
	// callback could acquire a privilege it never won.
	a := testF105PreparingAuthority()
	a.Intent.Retire.Gen = testF105Gen2
	if err := mqttValidateOutboundAuthority(a, testF105ID, testF105Epoch); err == nil {
		t.Fatalf("a retirement request naming a different generation from its intent was admitted")
	}

	// All five components of the predecessor are compared, not just the three that name
	// ownership: a retirement matching on ownership alone would be satisfied by a
	// predecessor whose session-record half had since been replaced.
	for name, mutate := range map[string]func(*mqttRetirementRequest){
		"session write identity": func(r *mqttRetirementRequest) { r.SessWID = "wid-9" },
		"session sequence":       func(r *mqttRetirementRequest) { r.SessSeq = 99 },
	} {
		a := testF105PreparingAuthority()
		mutate(a.Intent.Retire)
		if err := mqttValidateOutboundAuthority(a, testF105ID, testF105Epoch); err == nil {
			t.Fatalf("a retirement request naming a different %s was admitted", name)
		}
	}

	// The reason, the CONNECT mode and the discard disposition are three statements about
	// one request; a body where they disagree describes no supported operation.
	for name, mutate := range map[string]func(*mqttHandoffIntent){
		"an unsupported reason": func(i *mqttHandoffIntent) { i.Retire.Reason = "because" },
		"expiry on a clean start": func(i *mqttHandoffIntent) {
			i.Retire.Reason = mqttRetireExpiry
		},
		"a clean start that does not discard": func(i *mqttHandoffIntent) { i.Retire.Discard = false },
	} {
		a := testF105PreparingAuthority()
		mutate(a.Intent)
		if err := mqttValidateOutboundAuthority(a, testF105ID, testF105Epoch); err == nil {
			t.Fatalf("a retirement request with %s was admitted", name)
		}
	}

	// A clean start must actually retire what it displaces, or the old generation stays
	// admitted and nothing ever resolves its obligations.
	a = testF105PreparingAuthority()
	a.Intent.Retire = nil
	if err := mqttValidateOutboundAuthority(a, testF105ID, testF105Epoch); err == nil {
		t.Fatalf("a clean-start intent requesting no retirement was admitted")
	}
}

// TestF105IntentCleanFlagAndGenerationAgree pins the pairing: a clean start discards the
// session and so proposes a fresh generation, a resume preserves it. Either reversed is a
// body whose halves disagree about what it asks for.
func TestF105IntentCleanFlagAndGenerationAgree(t *testing.T) {
	a := testF105PreparingAuthority()
	a.Intent.Gen = a.Intent.PrevGen // clean start, but preserving the generation
	if err := mqttValidateOutboundAuthority(a, testF105ID, testF105Epoch); err == nil {
		t.Fatalf("a clean-start intent preserving its predecessor's generation was admitted")
	}

	a = testF105PreparingAuthority()
	a.Intent.CleanReq = false
	a.Intent.Retire.Reason = mqttRetireTakeover
	a.Intent.Retire.Discard = false
	// still proposing a fresh generation, which a resume may not do
	if err := mqttValidateOutboundAuthority(a, testF105ID, testF105Epoch); err == nil {
		t.Fatalf("a resume intent proposing a fresh generation was admitted")
	}
}

// TestF105AttemptPredecessorMatchesRevision pins that only a first admission has no
// predecessor. Any later revision displaces something, and an attempt that cannot name what
// it displaces could finalize against whatever it found.
func TestF105AttemptPredecessorMatchesRevision(t *testing.T) {
	first := testF105Attempt()
	first.Rev = 1
	first.PrevTxn = _EMPTY_
	if err := mqttValidateTransitionAttempt(first); err != nil {
		t.Fatalf("a well-formed first admission was refused: %v", err)
	}

	later := testF105Attempt()
	later.Rev = 2
	later.PrevTxn = "own-0"
	if err := mqttValidateTransitionAttempt(later); err != nil {
		t.Fatalf("a well-formed later attempt was refused: %v", err)
	}

	bad := testF105Attempt()
	bad.Rev = 2
	bad.PrevTxn = _EMPTY_
	if err := mqttValidateTransitionAttempt(bad); err == nil {
		t.Fatalf("an attempt at revision 2 with no predecessor was admitted")
	}

	bad = testF105Attempt()
	bad.Rev = 1
	bad.PrevTxn = "own-0"
	if err := mqttValidateTransitionAttempt(bad); err == nil {
		t.Fatalf("a first admission naming a predecessor was admitted")
	}
}

// TestF105OutcomeMustMatchTheAttemptItAnswers pins the check that stops a receipt found at
// a transition id from being read as the answer to a different question. Without it a
// caller could commit ownership for one tenure while permanently recording a receipt naming
// another, and resolution would still report LANDED.
func TestF105OutcomeMustMatchTheAttemptItAnswers(t *testing.T) {
	a := testF105Attempt()
	o := testF105CommittedOutcomeFor(a)
	if err := mqttOutcomeMatchesAttempt(o, a); err != nil {
		t.Fatalf("an outcome describing its own attempt was rejected: %v", err)
	}

	for name, mutate := range map[string]func(*mqttTransitionOutcome){
		"operation":   func(o *mqttTransitionOutcome) { o.Op = "other-op" },
		"kind":        func(o *mqttTransitionOutcome) { o.Kind = mqttTransitionCloseAdmit },
		"predecessor": func(o *mqttTransitionOutcome) { o.PrevTxn = "own-9" },
		"revision":    func(o *mqttTransitionOutcome) { o.Rev = 99 },
		"generation":  func(o *mqttTransitionOutcome) { o.Gen = testF105Gen2 },
		"tenure":      func(o *mqttTransitionOutcome) { o.Tenure = "other-tenure" },
		"ownership":   func(o *mqttTransitionOutcome) { o.OwnerTxn = "other-own" },
	} {
		m := testF105CommittedOutcomeFor(a)
		mutate(m)
		if err := mqttOutcomeMatchesAttempt(m, a); err == nil {
			t.Fatalf("an outcome describing a different %s was reported as this attempt's answer", name)
		}
	}
}

// TestF105DigestBindsTheWholeMutation pins that the receipt describes the batch actually
// sent. A digest over the attempt alone would be satisfied by any batch sharing a
// transition id.
func TestF105DigestBindsTheWholeMutation(t *testing.T) {
	a := testF105Attempt()
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

	b := testF105Attempt()
	b.OwnerTxn = "own-other"
	if base == mqttTransitionDigest(b, 7, auth, nil) {
		t.Fatalf("the digest ignores the attempt's proposed ownership")
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func testF105Attempt() *mqttTransitionAttempt {
	return &mqttTransitionAttempt{
		ID: testF105ID, Epoch: testF105Epoch, Txn: "txn-1", Op: "op-1",
		Kind: mqttTransitionAdmitExchange, PrevTxn: "own-0",
		Rev: 2, Gen: testF105Gen, Tenure: "tenure-0", OwnerTxn: "own-0",
	}
}

func testF105CommittedOutcomeFor(a *mqttTransitionAttempt) *mqttTransitionOutcome {
	return &mqttTransitionOutcome{
		ID: a.ID, Epoch: a.Epoch, Txn: a.Txn, Op: a.Op, Kind: a.Kind,
		Verdict: mqttOutcomeCommitted, PrevTxn: a.PrevTxn,
		Rev: a.Rev, Gen: a.Gen, Tenure: a.Tenure, OwnerTxn: a.OwnerTxn, Digest: "d1",
	}
}

func testF105PreparingAuthority() *mqttOutboundAuthority {
	return &mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-1",
		State: mqttAuthorityPreparing, Rev: 2, Txn: "txn-2", OwnerTxn: "own-1",
		Intent: &mqttHandoffIntent{
			ID: "intent-1", PrevGen: testF105Gen, PrevTenure: "tenure-1",
			PrevOwnerTxn: "own-1", PrevSessWID: "wid-0", PrevSessSeq: 12,
			Tenure: "tenure-2", Gen: testF105Gen2, CleanReq: true,
			Retire: &mqttRetirementRequest{
				Gen: testF105Gen, Tenure: "tenure-1", OwnerTxn: "own-1",
				SessWID: "wid-0", SessSeq: 12,
				Reason: mqttRetireCleanStart, Discard: true,
			},
		},
	}
}

func testF105ServingAuthorityJSON(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(&mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-1",
		State: mqttAuthorityServing, Rev: 2, Txn: "txn-1", OwnerTxn: "own-1",
	})
	if err != nil {
		t.Fatalf("marshalling the fixture authority: %v", err)
	}
	return b
}

func testF105CommittedOutcomeJSON(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(testF105CommittedOutcomeFor(testF105Attempt()))
	if err != nil {
		t.Fatalf("marshalling the fixture outcome: %v", err)
	}
	return b
}

func testF105AbortedOutcomeJSON(t *testing.T) []byte {
	t.Helper()
	a := testF105Attempt()
	b, err := json.Marshal(&mqttTransitionOutcome{
		ID: a.ID, Epoch: a.Epoch, Txn: a.Txn, Op: a.Op, Kind: a.Kind,
		Verdict: mqttOutcomeAborted, PrevTxn: a.PrevTxn,
	})
	if err != nil {
		t.Fatalf("marshalling the fixture outcome: %v", err)
	}
	return b
}

// testF105With returns body with member set to raw, added if absent. Operating on the
// PERSISTED BYTES is the point: a struct literal cannot express a member that is absent or
// explicitly null, which is exactly the shape the decoder must refuse.
func testF105With(t *testing.T, body []byte, member, raw string) string {
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

// testF105Without returns body with member removed.
func testF105Without(t *testing.T, body []byte, member string) string {
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

// testF105SubjectMatchesFilter reports whether subject is matched by a NATS subject filter
// built from literal tokens plus the single-token wildcard. The classes here never use the
// multi-token wildcard, and a filter that grew one would not be matched correctly by this
// helper — which is the point: it refuses rather than quietly approximating.
func testF105SubjectMatchesFilter(subject, filter string) bool {
	if strings.Contains(filter, ">") {
		panic("F-105 subject filters never use the multi-token wildcard")
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

// TestF105TransitionRefusesForeignMembers pins the boundary Codex's implementation review
// named: atomicity commits what it is given, so a caller member naming a closure or another
// transition's outcome would be written faithfully and the permanence these records rest on
// would be a caller convention rather than a property.
func TestF105TransitionRefusesForeignMembers(t *testing.T) {
	jsa := &mqttJSA{}
	idHash := getHash(testF105ID)

	for name, subj := range map[string]string{
		"a generation closure":       mqttGenClosureSubject(idHash, testF105Gen),
		"another transition outcome": mqttTransitionOutcomeSubject(idHash, "txn-9"),
		"this transition's outcome":  mqttTransitionOutcomeSubject(idHash, "txn-1"),
		"the authority itself":       mqttOutboundAuthoritySubject(idHash),
	} {
		tr := testF105Transition()
		tr.Extra = []mqttTransitionMember{{subj: subj, expect: 0, body: []byte("{}")}}
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition carrying %s as a caller member was sent", name)
		}
	}

	// A duplicate ordinary subject is refused too: the digest sorts members, so two
	// members on one subject would otherwise be indistinguishable from one.
	tr := testF105Transition()
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
	if mqttIsF105Subject(mqttInflightSubject(idHash, testF105Gen, 1)) {
		t.Fatalf("an ordinary in-flight register subject is classified as an arbitration record")
	}
}

// TestF105TransitionEnforcesItsPredecessor pins that a revision is not admitted merely
// because it satisfies a storage expectation. The sequence moves for completions that
// change no ownership, so without this a transition could install any revision over any
// predecessor.
func TestF105TransitionEnforcesItsPredecessor(t *testing.T) {
	jsa := &mqttJSA{}

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
		tr := testF105Transition()
		mutate(tr)
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition with %s was sent", name)
		}
	}

	// A FIRST CLAIM is the one attempt with no predecessor, and naming one is refused from
	// the other side.
	tr := testF105Transition()
	tr.Attempt.Kind = mqttTransitionReserveCreation
	tr.Attempt.Rev, tr.Attempt.PrevTxn = 1, _EMPTY_
	tr.Attempt.OwnerTxn = tr.Attempt.Txn
	tr.Auth.Rev, tr.Auth.OwnerTxn = 1, tr.Attempt.Txn
	tr.Auth.State = mqttAuthorityReserving
	tr.Auth.Creation = &mqttCreationIntent{
		ID: "intent-first", Gen: tr.Auth.Gen, Tenure: tr.Auth.Tenure,
	}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a first claim naming a predecessor authority was sent")
	}
}

// TestF105TransitionRefusesAnAuthorityItsAttemptDidNotDescribe pins that the permanent
// receipt cannot describe a mutation that never happened: the installed body must be the
// ownership the attempt proposed.
func TestF105TransitionRefusesAnAuthorityItsAttemptDidNotDescribe(t *testing.T) {
	jsa := &mqttJSA{}

	for name, mutate := range map[string]func(*mqttAuthorityTransition){
		"generation": func(tr *mqttAuthorityTransition) { tr.Auth.Gen = testF105Gen2 },
		"tenure":     func(tr *mqttAuthorityTransition) { tr.Auth.Tenure = "tenure-other" },
		"ownership":  func(tr *mqttAuthorityTransition) { tr.Auth.OwnerTxn = "own-other" },
		"revision":   func(tr *mqttAuthorityTransition) { tr.Auth.Rev = 9 },
		"transition": func(tr *mqttAuthorityTransition) { tr.Auth.Txn = "txn-other" },
	} {
		tr := testF105Transition()
		mutate(tr)
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition installing a different %s from its attempt was sent", name)
		}
	}
}

// TestF105TransitionRefusesAForeignClosure pins that a closure written by a transition
// names that transition, so the closure and its receipt can be reconciled without either
// being inferred from the other.
func TestF105TransitionRefusesAForeignClosure(t *testing.T) {
	jsa := &mqttJSA{}

	// A CLOSE-AUTHORIZED baseline. An admit-exchange rejects every closure before reaching
	// closure identity at all, so using one here would mask the property this test names.
	closing := func() *mqttAuthorityTransition {
		tr := testF105Transition()
		tr.Attempt.Kind = mqttTransitionCloseAdmit
		tr.Auth.State = mqttAuthorityClosed
		tr.Closure = &mqttGenClosure{
			ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Txn: tr.Attempt.Txn,
		}
		return tr
	}
	// The baseline is checked through the pure rule function rather than the writer: a
	// VALID transition would otherwise reach the network, which a bare mqttJSA cannot do.
	// The point of the probe is that the baseline is close-authorized, so the two failures
	// below are about closure IDENTITY and not about the kind refusing every closure.
	if err := mqttCheckTransitionKindRules(closing()); err != nil {
		t.Fatalf("the close-authorized baseline was refused by its kind rule: %v", err)
	}

	tr := closing()
	tr.Closure.Txn = "txn-other"
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a transition writing a closure naming another transition was sent")
	}

	tr = closing()
	tr.Closure.ID = "client-b"
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a transition writing another client's closure was sent")
	}
}

func testF105Transition() *mqttAuthorityTransition {
	a := testF105Attempt()
	return &mqttAuthorityTransition{
		Attempt: a,
		Prev: &mqttOutboundAuthority{
			ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-0",
			State: mqttAuthorityServing, Rev: 1, Txn: "txn-0", OwnerTxn: "own-0",
		},
		// An admit-exchange preserves its predecessor's ownership, which is what makes it
		// work admitted UNDER a tenure rather than a move of one.
		PrevSeq: 11,
		Auth: &mqttOutboundAuthority{
			ID: a.ID, Epoch: a.Epoch, Gen: a.Gen, Tenure: a.Tenure,
			State: mqttAuthorityServing, Rev: a.Rev, Txn: a.Txn, OwnerTxn: a.OwnerTxn,
		},
	}
}

// TestF105AbortedOutcomeValidatorRefusesOwnership reaches the SEMANTIC validator directly,
// which the decoder tests cannot.
//
// mqttDecodeTransitionOutcome refuses an aborted body carrying ownership members at the
// member-set level, before mqttValidateTransitionOutcome's own check runs — so a mutation
// deleting that check survives every decoder-driven fixture. The check is not redundant:
// abortTransition builds its outcome IN MEMORY and validates it before writing, so this is
// the layer that stops a malformed abort being published to a permanent subject.
func TestF105AbortedOutcomeValidatorRefusesOwnership(t *testing.T) {
	a := testF105Attempt()
	base := &mqttTransitionOutcome{
		ID: a.ID, Epoch: a.Epoch, Txn: a.Txn, Op: a.Op, Kind: a.Kind,
		Verdict: mqttOutcomeAborted, PrevTxn: a.PrevTxn,
	}
	if err := mqttValidateTransitionOutcome(base, a.ID, a.Epoch, a.Txn); err != nil {
		t.Fatalf("a well-formed aborted outcome was refused: %v", err)
	}

	for name, mutate := range map[string]func(*mqttTransitionOutcome){
		"a revision":   func(o *mqttTransitionOutcome) { o.Rev = 7 },
		"a generation": func(o *mqttTransitionOutcome) { o.Gen = testF105Gen },
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

// TestF105AdmissionRequiresAServingPredecessor is the rule the whole item exists for, at
// the layer that enforces it: new outbound work may not be admitted over an authority that
// has closed admission. A closed enum with no rules attached would let exactly that
// through, which is admission-after-closure.
func TestF105AdmissionRequiresAServingPredecessor(t *testing.T) {
	jsa := &mqttJSA{}

	for _, kind := range []mqttTransitionKind{mqttTransitionAdmitExchange, mqttTransitionAcquireAction} {
		for name, state := range map[string]mqttAuthorityState{
			"closed":    mqttAuthorityClosed,
			"preparing": mqttAuthorityPreparing,
		} {
			tr := testF105Transition()
			tr.Attempt.Kind = kind
			tr.Prev.State = state
			if state == mqttAuthorityPreparing {
				tr.Prev.Intent = testF105PreparingAuthority().Intent
			}
			if _, err := jsa.commitAuthorityTransition(tr); err == nil {
				t.Fatalf("a %s transition over a %s predecessor was sent: that is admission "+
					"after admission closed", kind, name)
			}
		}

		// It must also not move ownership: this authorizes work UNDER the standing tenure.
		tr := testF105Transition()
		tr.Attempt.Kind = kind
		tr.Attempt.Tenure = "tenure-moved"
		tr.Auth.Tenure = "tenure-moved"
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a %s transition moved ownership", kind)
		}

		// And it must not write a closure.
		tr = testF105Transition()
		tr.Attempt.Kind = kind
		tr.Closure = &mqttGenClosure{
			ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Txn: tr.Attempt.Txn,
		}
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a %s transition wrote a generation closure", kind)
		}
	}
}

// TestF105CloseAdmissionWritesBothHalves pins that the closed state and its permanent
// record land in one batch for one generation. Either without the other would leave the two
// halves of a single fact able to disagree, and the permanent record is the authoritative
// one.
func TestF105CloseAdmissionWritesBothHalves(t *testing.T) {
	jsa := &mqttJSA{}

	tr := testF105Transition()
	tr.Attempt.Kind = mqttTransitionCloseAdmit
	tr.Auth.State = mqttAuthorityClosed
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a close-admission transition with no permanent closure was sent")
	}

	tr = testF105Transition()
	tr.Attempt.Kind = mqttTransitionCloseAdmit
	tr.Auth.State = mqttAuthorityClosed
	tr.Closure = &mqttGenClosure{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen2, Txn: tr.Attempt.Txn,
	}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a close-admission transition closed a generation other than its predecessor's")
	}

	tr = testF105Transition()
	tr.Attempt.Kind = mqttTransitionCloseAdmit
	tr.Closure = &mqttGenClosure{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Txn: tr.Attempt.Txn,
	}
	// Auth left SERVING: the closure would stand while the arbitration point still admitted.
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a close-admission transition left its authority serving")
	}
}

// TestF105TransitionRefusesAnUnsupportedPredecessor pins that a predecessor is validated,
// not merely present. Without it a caller could hand in any struct and the
// consecutive-revision test would pass against it.
func TestF105TransitionRefusesAnUnsupportedPredecessor(t *testing.T) {
	jsa := &mqttJSA{}

	for name, mutate := range map[string]func(*mqttOutboundAuthority){
		"another client":       func(p *mqttOutboundAuthority) { p.ID = "client-b" },
		"another epoch":        func(p *mqttOutboundAuthority) { p.Epoch = "epoch-9" },
		"no revision":          func(p *mqttOutboundAuthority) { p.Rev = 0 },
		"an unsupported state": func(p *mqttOutboundAuthority) { p.State = mqttAuthorityState(9) },
	} {
		tr := testF105Transition()
		mutate(tr.Prev)
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition over a predecessor naming %s was sent", name)
		}
	}
}

// TestF105DigestCoversTheClosure pins the omission Codex's second review found: the closure
// is a batch member, so a transition carrying one and a transition without must not produce
// the same receipt.
func TestF105DigestCoversTheClosure(t *testing.T) {
	bare := testF105Transition()
	bare.Attempt.Kind = mqttTransitionCloseAdmit
	bare.Auth.State = mqttAuthorityClosed

	withClosure := testF105Transition()
	withClosure.Attempt.Kind = mqttTransitionCloseAdmit
	withClosure.Auth.State = mqttAuthorityClosed
	withClosure.Closure = &mqttGenClosure{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Txn: withClosure.Attempt.Txn,
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

	// The closure's own content must move it too: a discarding closure and a
	// non-discarding one authorize different things.
	flipped := *withClosure
	c := *withClosure.Closure
	c.Discard = true
	flipped.Closure = &c
	fd, err := mqttTransitionMutationDigest(&flipped)
	if err != nil {
		t.Fatalf("digesting the discarding transition: %v", err)
	}
	if fd == cd {
		t.Fatalf("a discarding closure and a non-discarding one produce the same digest")
	}

	other := *withClosure
	c2 := *withClosure.Closure
	c2.Gen = testF105Gen2
	other.Closure = &c2
	od, err := mqttTransitionMutationDigest(&other)
	if err != nil {
		t.Fatalf("digesting the other-generation transition: %v", err)
	}
	if od == cd {
		t.Fatalf("closures for two generations produce the same digest")
	}
}

// TestF105FinalizeBindsItsPendingIntent pins what stops a delayed acceptance installing a
// tenure over a successor: a finalization must find its predecessor still PREPARING and
// carrying the intent being finalized, and may install only what that intent proposed.
//
// A finalize that accepted any predecessor with any intent would be the retarget the whole
// handoff structure exists to prevent.
func TestF105FinalizeBindsItsPendingIntent(t *testing.T) {
	jsa := &mqttJSA{}

	// A well-formed finalization: the predecessor is preparing, and the installed body is
	// exactly what its intent proposed.
	good := func() *mqttAuthorityTransition {
		prep := testF105PreparingAuthority()
		prep.Rev = 1
		tr := testF105Transition()
		tr.Prev = prep
		tr.Attempt.Kind = mqttTransitionFinalize
		tr.Attempt.PrevTxn = prep.OwnerTxn
		tr.Attempt.Gen = prep.Intent.Gen
		tr.Attempt.Tenure = prep.Intent.Tenure
		tr.Attempt.OwnerTxn = tr.Attempt.Txn
		tr.Auth = &mqttOutboundAuthority{
			ID: testF105ID, Epoch: testF105Epoch,
			Gen: prep.Intent.Gen, Tenure: prep.Intent.Tenure,
			State: mqttAuthorityServing, Rev: tr.Attempt.Rev,
			Txn: tr.Attempt.Txn, OwnerTxn: tr.Attempt.Txn,
		}
		// This intent clean-starts and discards, so the finalization closes the
		// predecessor's generation in the same batch.
		tr.Closure = &mqttGenClosure{
			ID: testF105ID, Epoch: testF105Epoch, Gen: prep.Intent.PrevGen,
			Txn: tr.Attempt.Txn, Discard: true,
		}
		return tr
	}

	for name, mutate := range map[string]func(*mqttAuthorityTransition){
		"a serving predecessor": func(tr *mqttAuthorityTransition) {
			tr.Prev.State = mqttAuthorityServing
			tr.Prev.Intent = nil
		},
		"a closed predecessor": func(tr *mqttAuthorityTransition) {
			tr.Prev.State = mqttAuthorityClosed
			tr.Prev.Intent = nil
		},
		"a generation its intent did not propose": func(tr *mqttAuthorityTransition) {
			tr.Attempt.Gen, tr.Auth.Gen = testF105Gen, testF105Gen
		},
		"a tenure its intent did not propose": func(tr *mqttAuthorityTransition) {
			tr.Attempt.Tenure, tr.Auth.Tenure = "tenure-elsewhere", "tenure-elsewhere"
		},
		"ownership that is not its own identity": func(tr *mqttAuthorityTransition) {
			tr.Attempt.OwnerTxn, tr.Auth.OwnerTxn = "own-borrowed", "own-borrowed"
		},
		"no closure for the generation it discards": func(tr *mqttAuthorityTransition) {
			tr.Closure = nil
		},
		"a closure for the wrong generation": func(tr *mqttAuthorityTransition) {
			tr.Closure.Gen = testF105Gen2
		},
		"a non-discarding closure": func(tr *mqttAuthorityTransition) {
			tr.Closure.Discard = false
		},
	} {
		tr := good()
		mutate(tr)
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a finalize transition with %s was sent", name)
		}
	}
}

// TestF105KindRulesRefuseContradictoryStates reaches the per-kind rules directly, through
// the pure rule function, because several of them refuse a transition that would otherwise
// be a perfectly valid store write — so a fixture driving the writer would be refused by
// the store for an unrelated reason and prove nothing about the rule.
func TestF105KindRulesRefuseContradictoryStates(t *testing.T) {
	// A FIRST CLAIM MAY NOT CLOSE. Otherwise it installs an authority and a permanent
	// closure for the generation it is claiming, in one batch — after which a finalization
	// makes that generation servable while closure already says it is closed. That is
	// admission-after-closure reached through a different kind.
	//
	// It is also the one attempt with NO predecessor, which is what makes it create-only,
	// and there is no longer any kind that reaches SERVING in one step: the claim below
	// installs a RESERVING authority, and only a metadata commit licenses the rest.
	admit := testF105Transition()
	admit.Attempt.Kind = mqttTransitionReserveCreation
	admit.Attempt.Rev, admit.Attempt.PrevTxn = 1, _EMPTY_
	admit.Attempt.OwnerTxn = admit.Attempt.Txn
	admit.Auth.Rev, admit.Auth.OwnerTxn = 1, admit.Attempt.Txn
	admit.Auth.State = mqttAuthorityReserving
	admit.Auth.Creation = &mqttCreationIntent{
		ID: "intent-first", Gen: admit.Auth.Gen, Tenure: admit.Auth.Tenure,
	}
	admit.Prev, admit.PrevSeq = nil, 0
	if err := mqttCheckTransitionKindRules(admit); err != nil {
		t.Fatalf("a well-formed first claim was refused: %v", err)
	}
	serving := *admit
	sauth := *admit.Auth
	sauth.State, sauth.Creation = mqttAuthorityServing, nil
	serving.Auth = &sauth
	if err := mqttCheckTransitionKindRules(&serving); err == nil {
		t.Fatalf("a first claim installed a SERVING authority with no metadata half behind it")
	}
	withClosure := *admit
	withClosure.Closure = &mqttGenClosure{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Txn: admit.Attempt.Txn,
	}
	if err := mqttCheckTransitionKindRules(&withClosure); err == nil {
		t.Fatalf("a first claim carrying a generation closure was admitted")
	}

	// A first claim's ownership is its OWN identity: there is no predecessor whose
	// ownership it could inherit, so ownership it did not mint is ownership nothing
	// established.
	borrowed := *admit
	ba := *admit.Attempt
	ba.OwnerTxn = "own-borrowed"
	borrowed.Attempt = &ba
	bauth := *admit.Auth
	bauth.OwnerTxn = "own-borrowed"
	borrowed.Auth = &bauth
	if err := mqttCheckTransitionKindRules(&borrowed); err == nil {
		t.Fatalf("a first claim installing ownership it did not mint was admitted")
	}

	// CLOSING ADMISSION MAY NOT DISPLACE AN UNRESOLVED PREPARATION. A handoff that is
	// neither finalized nor cancelled would otherwise be discarded silently, and the
	// metadata half it may already have committed would have nothing to answer to.
	closing := testF105Transition()
	closing.Attempt.Kind = mqttTransitionCloseAdmit
	closing.Auth.State = mqttAuthorityClosed
	closing.Closure = &mqttGenClosure{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Txn: closing.Attempt.Txn,
	}
	if err := mqttCheckTransitionKindRules(closing); err != nil {
		t.Fatalf("a well-formed close-admission was refused: %v", err)
	}
	overPreparing := *closing
	prev := testF105PreparingAuthority()
	prev.Rev = 1
	prev.Gen, prev.Tenure, prev.OwnerTxn = closing.Prev.Gen, closing.Prev.Tenure, closing.Prev.OwnerTxn
	prev.Intent.PrevGen, prev.Intent.PrevTenure, prev.Intent.PrevOwnerTxn = prev.Gen, prev.Tenure, prev.OwnerTxn
	prev.Intent.Retire.Gen, prev.Intent.Retire.Tenure, prev.Intent.Retire.OwnerTxn = prev.Gen, prev.Tenure, prev.OwnerTxn
	overPreparing.Prev = prev
	if err := mqttCheckTransitionKindRules(&overPreparing); err == nil {
		t.Fatalf("a close-admission displaced a preparing authority with an unresolved handoff")
	}

	// And it may not move ownership: ending a generation is not transferring it.
	moved := *closing
	ma := *closing.Auth
	ma.Tenure = "tenure-moved"
	moved.Auth = &ma
	if err := mqttCheckTransitionKindRules(&moved); err == nil {
		t.Fatalf("a close-admission moved ownership")
	}
}

// TestF105MembersAreConfinedToTheirOwnStreamAndClient pins the confinement Codex's third
// review found still open.
//
// A JetStream batch is per stream and newAtomicBatchRequest publishes each subject
// separately, so a member naming another stream is not in the transaction at all: it is
// routed elsewhere or nowhere, and the acknowledgement cannot say which. Beyond that, a
// member must be a record class this transition may write, for THIS client and generation,
// in a shape the register boundary would accept — because atomicity validates ordering and
// never meaning.
func TestF105MembersAreConfinedToTheirOwnStreamAndClient(t *testing.T) {
	jsa := &mqttJSA{}
	idHash := getHash(testF105ID)

	for name, subj := range map[string]string{
		"another stream entirely":   mqttSessStreamSubjectPrefix + idHash,
		"the retained-message tree": mqttRetainedMsgsStreamSubject,
		"a wildcard":                mqttInflightStreamSubjectPrefix + idHash + "." + testF105Gen + ".*",
		"another client's register": mqttInflightSubject("zzzz9999", testF105Gen, 3),
		"another generation":        mqttInflightSubject(idHash, testF105Gen2, 3),
		"an unparseable subject":    mqttInflightStreamSubjectPrefix + "nonsense",
	} {
		tr := testF105Transition()
		tr.Extra = []mqttTransitionMember{{subj: subj, expect: 0, body: []byte("{}")}}
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition carrying a member on %s was sent", name)
		}
	}

	// A register body goes through the same shape matrix every other register writer uses.
	tr := testF105Transition()
	tr.Extra = []mqttTransitionMember{{
		subj:   mqttInflightSubject(idHash, testF105Gen, 3),
		expect: 0,
		body:   []byte(`{"not":"a register"}`),
	}}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a transition carrying an unsupported register shape was sent")
	}
}

// TestF105IntentIsInsideTheCommittedBinding reaches two checks the wire tests cannot.
//
// A finalize or cancel-intent transition is refused by its KIND RULE if the intent it names
// disagrees with its predecessor's, so no committable pair of transitions differs in the
// intent alone — which means the wire tests can never exercise the digest and the matching
// on that field. Both are reached here directly, because "the receipt records WHICH handoff
// it answered" is exactly the kind of binding that is invisible until it is missing.
func TestF105IntentIsInsideTheCommittedBinding(t *testing.T) {
	base := testF105Attempt()
	base.Kind = mqttTransitionFinalize
	base.Intent = "intent-1"
	if err := mqttValidateTransitionAttempt(base); err != nil {
		t.Fatalf("a well-formed finalize attempt was refused: %v", err)
	}

	other := *base
	other.Intent = "intent-2"

	// THE DIGEST. Two attempts differing only in the handoff they answer must not produce
	// one receipt, or a finalization of one handoff could be read as the answer to another.
	auth := []byte(`{"rev":2}`)
	if mqttTransitionDigest(base, 7, auth, nil) == mqttTransitionDigest(&other, 7, auth, nil) {
		t.Fatalf("the mutation digest ignores the handoff intent the transition names")
	}

	// THE MATCHING. Same, at the point a resolution decides whether a standing receipt
	// answers this attempt.
	o := testF105CommittedOutcomeFor(base)
	o.Intent = base.Intent
	if err := mqttOutcomeMatchesAttempt(o, base); err != nil {
		t.Fatalf("an outcome answering its own attempt was rejected: %v", err)
	}
	o.Intent = "intent-2"
	if err := mqttOutcomeMatchesAttempt(o, base); err == nil {
		t.Fatalf("an outcome answering a different handoff was reported as this attempt's answer")
	}

	// And the receipt's intent must agree with its kind, exactly as the attempt's does: a
	// kind that resolves no handoff may not name one, and one that does must.
	bad := testF105CommittedOutcomeFor(base)
	bad.Intent = _EMPTY_
	if err := mqttValidateTransitionOutcome(bad, base.ID, base.Epoch, base.Txn); err == nil {
		t.Fatalf("a finalize outcome naming no handoff intent was admitted")
	}
	stray := testF105CommittedOutcomeFor(testF105Attempt())
	stray.Intent = "intent-1"
	if err := mqttValidateTransitionOutcome(stray, testF105ID, testF105Epoch, "txn-1"); err == nil {
		t.Fatalf("an admit-exchange outcome naming a handoff intent was admitted")
	}
}

// TestF105EveryAuthorityShapeSurvivesItsOwnDecoder closes the class the retirement-request
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
func TestF105EveryAuthorityShapeSurvivesItsOwnDecoder(t *testing.T) {
	serving := &mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-1",
		State: mqttAuthorityServing, Rev: 1, Txn: "txn-1", OwnerTxn: "txn-1",
	}
	preparing := testF105PreparingAuthority()
	preparingNoRetire := testF105PreparingAuthority()
	noRetire := *preparingNoRetire.Intent
	noRetire.Retire = nil
	noRetire.CleanReq = false
	noRetire.Gen = preparingNoRetire.Gen
	preparingNoRetire.Intent = &noRetire
	reserving := &mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-1",
		State: mqttAuthorityReserving, Rev: 1, Txn: "txn-1", OwnerTxn: "txn-1",
		Creation: &mqttCreationIntent{ID: "intent-1", Gen: testF105Gen, Tenure: "tenure-1"},
	}
	closed := &mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-1",
		State: mqttAuthorityClosed, Rev: 2, Txn: "txn-2", OwnerTxn: "txn-1",
	}

	for name, a := range map[string]*mqttOutboundAuthority{
		"a serving authority":                           serving,
		"a preparing authority with a retirement":       preparing,
		"a preparing authority resuming, no retirement": preparingNoRetire,
		"a reserving authority":                         reserving,
		"a closed authority":                            closed,
	} {
		if verr := mqttValidateOutboundAuthority(a, testF105ID, testF105Epoch); verr != nil {
			t.Fatalf("%s is not a shape this broker composes: %v", name, verr)
		}
		body, merr := json.Marshal(a)
		if merr != nil {
			t.Fatalf("marshalling %s: %v", name, merr)
		}
		back, derr := mqttDecodeOutboundAuthority(body, testF105ID, testF105Epoch)
		if derr != nil {
			t.Fatalf("%s is writable and unreadable: its own decoder refuses it: %v", name, derr)
		}
		if back.State != a.State || back.Gen != a.Gen || back.Tenure != a.Tenure ||
			back.OwnerTxn != a.OwnerTxn || back.Rev != a.Rev {
			t.Fatalf("%s did not round-trip: %+v became %+v", name, a, back)
		}
		if (a.Intent == nil) != (back.Intent == nil) || (a.Creation == nil) != (back.Creation == nil) {
			t.Fatalf("%s lost its pending proposal in the round trip", name)
		}
		if a.Intent != nil && a.Intent.Retire != nil {
			if back.Intent.Retire == nil {
				t.Fatalf("%s lost its retirement request", name)
			}
			// The two members whose omission from the set caused the defect.
			if back.Intent.Retire.SessWID != a.Intent.Retire.SessWID ||
				back.Intent.Retire.SessSeq != a.Intent.Retire.SessSeq {
				t.Fatalf("%s lost the session identity its retirement names", name)
			}
		}
	}
}
