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

//go:build mqttport_fulltests

package mqtt

import (
	"strings"
	"testing"
)

// TestF105DischargeRequiresEveryAcquisition is the boundary that replaces two prose contracts.
//
// A connection knows which acquisitions IT consumed and finished. It knows nothing about ones
// made on a connection it cannot reach, and nothing at all after a restart. Releasing an
// identifier or freeing a charge on a connection-local answer frees something another connection
// may still emit under.
//
// The aggregate enumerates the exchange's own durable ACTION records, so the constituent set
// comes from the store rather than from anyone's memory — and UNKNOWN is not DISCHARGED.
func TestF105DischargeRequiresEveryAcquisition(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discharge")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	acquire := func(txn string, acq uint64) {
		t.Helper()
		if _, _, err := e.asm.jsa.opAcquireAction(testF105WithRegister(t, e, 7, &mqttAcquireActionRequest{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Gen: auth.Gen, Tenure: auth.Tenure,
			Nonce: nonce, Acq: acq, Kind: mqttActionPublish,
			// Outstanding is left FALSE deliberately: it is the connection-local answer this
			// file exists to stop trusting, and the fixture needs the second action record
			// precisely to show that one finished execution does not discharge the exchange.
			Binding: mqttBindConnection(4242),
		})); err != nil {
			t.Fatalf("acquiring %s was refused: %v", txn, err)
		}
	}

	// NO ACTIONS YET: a complete enumeration that found nothing IS discharged. Nothing was
	// ever acquired, so nothing can emit.
	none := func(string, string, uint64) bool { return false }
	p, err := e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, none)
	if err != nil {
		t.Fatalf("proving: %v", err)
	}
	if !p.Complete || !p.Discharged() {
		t.Fatalf("an exchange with no acquisitions was not discharged: %+v", p)
	}

	// ACQUIRE ONE. With a local oracle that cannot establish it finished, the exchange is NOT
	// discharged, and the refusal names the constituent.
	acquire("txn-acq", 1)
	p, err = e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, none)
	if err != nil {
		t.Fatalf("proving: %v", err)
	}
	if len(p.Constituents) != 1 {
		t.Fatalf("the proof enumerated %d constituents, want the one acquisition: %+v",
			len(p.Constituents), p)
	}
	if p.Discharged() {
		t.Fatalf("an exchange with an unestablished acquisition was discharged")
	}
	if !strings.Contains(p.Blocker(), "outstanding") {
		t.Fatalf("the blocker did not name the outstanding acquisition: %s", p.Blocker())
	}

	// THE LOCAL ORACLE IS WHAT ESTABLISHES FINISHED, and only for what this process saw.
	all := func(string, string, uint64) bool { return true }
	p, err = e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, all)
	if err != nil {
		t.Fatalf("proving: %v", err)
	}
	if !p.Discharged() {
		t.Fatalf("an exchange whose only acquisition finished was not discharged: %s", p.Blocker())
	}

	// AND A SECOND ACQUISITION the oracle cannot establish blocks it again — which is the
	// retransmission case: one finished execution does not discharge the exchange.
	acquire("txn-acq2", 2)
	onlyFirst := func(_, _ string, acq uint64) bool { return acq == 1 }
	p, err = e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, onlyFirst)
	if err != nil {
		t.Fatalf("proving: %v", err)
	}
	if p.Discharged() {
		t.Fatalf("one finished acquisition discharged an exchange holding two")
	}
}

// TestF105AnIncompleteEnumerationIsNeverDischarged keeps two failures apart that both block
// release.
//
// "All of them are done" and "all the ones I saw are done" are different claims. A proof whose
// enumeration failed has no specific blocker to name — reporting one would describe a
// constituent that may not exist — and it must still refuse.
func TestF105AnIncompleteEnumerationIsNeverDischarged(t *testing.T) {
	incomplete := mqttDischargeProof{
		Gen: testF105Gen, Nonce: "xch-a",
		Constituents: []mqttActionDischarge{{Acq: 1, State: mqttDischargeFinished}},
	}
	if incomplete.Discharged() {
		t.Fatalf("a proof whose enumeration failed was discharged on the constituents it did see")
	}
	if !strings.Contains(incomplete.Blocker(), "never saw") {
		t.Fatalf("an incomplete proof named a specific blocker: %s", incomplete.Blocker())
	}

	complete := incomplete
	complete.Complete = true
	if !complete.Discharged() {
		t.Fatalf("a complete proof with every constituent finished was not discharged")
	}
	if complete.Blocker() != _EMPTY_ {
		t.Fatalf("a discharged proof named a blocker: %s", complete.Blocker())
	}
}

// TestF105ProvingDischargeRequiresAnOracle refuses a proof that could only ever say no.
//
// The exchange is REAL here, and that is the whole design of the fixture: proving a nonce
// nothing admitted would be refused by the anchor, so a refusal would say nothing about the
// oracle. Every other refusal in this file has to be reached the same way — through a state
// where the rule under test is the only one with an objection.
func TestF105ProvingDischargeRequiresAnOracle(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discharge-oracle")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	_, err := e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, nil)
	if err == nil {
		t.Fatalf("a discharge proof was built with no oracle, so it could never establish anything")
	}
	if !strings.Contains(err.Error(), "drain oracle") {
		t.Fatalf("the refusal was not the missing oracle: %v", err)
	}
	// The positive control: the same exchange, with an oracle, proves.
	if _, perr := e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce,
		func(string, string, uint64) bool { return true }); perr != nil {
		t.Fatalf("the same exchange could not be proven with an oracle: %v", perr)
	}
}

// TestF105DischargeRefusesATokenThatIsNotOne keeps a malformed identity out of the subject
// filter.
//
// The enumeration's reach IS the proof's claim. A token carrying a separator or a wildcard
// builds a filter over a different subject space, and a complete enumeration of the wrong space
// reports a discharged exchange as confidently as a right one would.
func TestF105DischargeRefusesATokenThatIsNotOne(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discharge-tokens")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	all := func(string, string, uint64) bool { return true }

	for name, c := range map[string]struct{ gen, nonce string }{
		"a wildcard generation":    {"*", nonce},
		"a wildcard nonce":         {auth.Gen, ">"},
		"a nonce with a separator": {auth.Gen, "a.b"},
		"an empty nonce":           {auth.Gen, _EMPTY_},
		"an empty generation":      {_EMPTY_, nonce},
	} {
		_, err := e.asm.proveExchangeDischarged(e.id, e.epoch, c.gen, c.nonce, all)
		if err == nil {
			t.Fatalf("%s built a discharge proof, so the enumeration covered a subject space "+
				"that is not this exchange's", name)
		}
		// THE REFUSAL MUST BE THE TOKEN RULE'S. Reaching the store with a malformed token and
		// being refused THERE is a different mechanism and a weaker one: a wildcard subject is
		// something the store will happily answer, so a proof that got that far has already
		// asked the wrong question. Asserting the text is what keeps the anchor from standing
		// in for this rule.
		if !strings.Contains(err.Error(), "subject separator") {
			t.Fatalf("%s was refused by something other than the token rule: %v", name, err)
		}
	}

	// THE POSITIVE CONTROL: well-formed tokens still prove, so the refusals above are the
	// guard and not a fixture that cannot reach the function.
	if _, err := e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, all); err != nil {
		t.Fatalf("a well-formed exchange could not be proven: %v", err)
	}
}

// testF105PlantActionBytes writes raw bytes at an action subject of one exchange.
//
// Deliberately raw: the point of these cases is a record the proof CANNOT interpret, and one
// written through the operation surface would be interpretable by construction.
func testF105PlantActionBytes(t *testing.T, e *testF105Env, gen, nonce, acqTk string, body []byte) {
	t.Helper()
	subject := mqttF105SubjectPrefix() + mqttActionClassTk + "." + getHash(e.id) + "." + gen +
		"." + nonce + "." + acqTk
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(subject, hdr, msg); err != nil {
		t.Fatalf("planting the action record on %q: %v", subject, err)
	}
}

// TestF105AnUninterpretableActionIsOutstanding covers the three ways a record can defeat the
// enumeration without being absent.
//
// All three have the same shape: this broker found something at an address that belongs to the
// exchange and could not say what it was. Skipping any of them would drop a constituent from a
// proof that then reports every constituent finished — a release licensed by what could not be
// read. The blockers are asserted apart from each other because a single "outstanding" answer
// would let one rule stand in for the other two.
func TestF105AnUninterpretableActionIsOutstanding(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discharge-unreadable")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	all := func(string, string, uint64) bool { return true }

	pi := uint16(6)
	for name, c := range map[string]struct {
		acqTk string
		body  string
		want  string
	}{
		// A BODY THIS BROKER CANNOT INTERPRET may be an acquisition of exactly this exchange,
		// and it certainly is not one that can be called finished.
		"a body that will not decode": {"3", `{"nonsense":true}`, "cannot be interpreted"},
		// A SUBJECT IT CANNOT PLACE is not one it may skip either: the acquisition token is
		// what says WHICH acquisition, so a token with no reading leaves an unidentified one.
		"an acquisition token that is not a number": {"abc", `{}`, "cannot be placed"},
		// AND A PADDED TOKEN IS A SECOND SPELLING of a number that already has one. Accepting
		// it would make two subjects name one acquisition, so a proof reading one address
		// would answer for the other.
		"a zero-padded acquisition token": {"007", `{}`, "cannot be placed"},
	} {
		pi++
		nonce := testF105AdmitOn(t, e, "txn-adm-"+c.acqTk, pi, "inc-src")
		testF105PlantActionBytes(t, e, auth.Gen, nonce, c.acqTk, []byte(c.body))
		p, err := e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, all)
		if err != nil {
			t.Fatalf("%s: proving: %v", name, err)
		}
		if p.Discharged() {
			t.Fatalf("%s was discharged, so a record this broker could not read counted as "+
				"finished", name)
		}
		if !strings.Contains(p.Blocker(), c.want) {
			t.Fatalf("%s blocked on %q, want it to name %q", name, p.Blocker(), c.want)
		}
	}
}

// TestF105ProvingDischargeRequiresTheExchange refuses a proof about an obligation nothing
// admitted.
//
// This is the finding that makes an empty enumeration safe: without it, a nonce nothing ever
// admitted matches no subject, enumerates cleanly, and comes back DISCHARGED with no
// constituents — which is the strongest possible answer built from the weakest possible
// evidence. Acquisition already refuses on an exchange that does not exist; proving one
// discharged is the same claim read the other way round, and it is the one that releases.
func TestF105ProvingDischargeRequiresTheExchange(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discharge-anchor")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	all := func(string, string, uint64) bool { return true }

	if _, err := e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen,
		mqttNewInflightNonce(), all); err == nil {
		t.Fatalf("an exchange nothing admitted was proven discharged")
	}
	// THE RIGHT NONCE UNDER THE WRONG GENERATION is the same hole wearing a valid identity:
	// the exchange exists, just not as this generation's work.
	const otherGen = "zyxwvu9876543210lkjihg"
	if otherGen == testF105Gen {
		t.Fatalf("the fixture's second generation is the one the exchange was admitted under")
	}
	if _, err := e.asm.proveExchangeDischarged(e.id, e.epoch, otherGen, nonce, all); err == nil {
		t.Fatalf("an exchange was proven discharged under a generation it does not belong to")
	}
	// The positive control: the admitted exchange under its own generation does prove.
	p, err := e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, all)
	if err != nil {
		t.Fatalf("the admitted exchange could not be proven: %v", err)
	}
	if !p.Discharged() {
		t.Fatalf("an admitted exchange with no acquisitions was not discharged: %s", p.Blocker())
	}
}

// TestF105ATruncatedEnumerationIsNotADischarge is the review's first finding.
//
// A stream-info subject list is capped by the server at 100,000 and the true total comes back in
// the envelope, so a caller reading the map alone cannot tell EVERY action from a FIRST PAGE.
// The proof's whole claim is completeness, and a first page whose members all finished would
// report a discharged exchange with an outstanding acquisition sitting just past the cut —
// which is the release this boundary exists to withhold.
func TestF105ATruncatedEnumerationIsNotADischarge(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discharge-truncated")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	all := func(string, string, uint64) bool { return true }

	// A TRUNCATED ANSWER: a page this process can establish finished, and a total saying there
	// are more than it was shown.
	truncated := func(string) (map[string]uint64, int, error) {
		return map[string]uint64{}, 100_001, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&truncated)
	p, err := e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, all)
	if err != nil {
		t.Fatalf("proving against a truncated answer: %v", err)
	}
	if p.Discharged() {
		t.Fatalf("an exchange was discharged on a first page")
	}
	if !strings.Contains(p.Blocker(), "never saw") {
		t.Fatalf("the truncation was not reported as an incomplete enumeration: %s", p.Blocker())
	}

	// THE SAME EMPTY PAGE, REPORTED COMPLETE, discharges — so what refused above is the
	// truncation and not the emptiness.
	complete := func(string) (map[string]uint64, int, error) {
		return map[string]uint64{}, 0, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&complete)
	p, err = e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, all)
	if err != nil {
		t.Fatalf("proving against a complete answer: %v", err)
	}
	if !p.Discharged() {
		t.Fatalf("a complete answer holding nothing did not discharge: %s", p.Blocker())
	}
	e.asm.jsa.exchangeSubjectsObs.Store(nil)
}

// TestF105ActionAcquisitionInstallsTokenAtomically pins §4.3 point 1: the output-custody TOKEN and
// the durable action RECORD are committed as ONE authority batch, so neither is an independent
// acquisition. The register CAS installs the token bound to the slot's existing custody and
// advances Acq, beside the create-only action record for the same acquisition.
func TestF105ActionAcquisitionInstallsTokenAtomically(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-action-token")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	regSubj := mqttInflightSubject(getHash(e.id), auth.Gen, 7)
	occ, smsg, lerr := e.asm.jsa.loadInflightRecord(regSubj)
	if lerr != nil || occ == nil || smsg == nil {
		t.Fatalf("the admitted register does not load back: %v", lerr)
	}
	if occ.ActionToken != _EMPTY_ {
		t.Fatalf("a freshly admitted register must carry no token")
	}

	token := mqttNewInflightNonce()
	if _, _, err := e.asm.jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq", Op: "op-acq",
		Gen: auth.Gen, Tenure: auth.Tenure,
		Nonce: nonce, Acq: occ.Acq + 1, Kind: mqttActionPublish,
		Binding:  mqttBindConnection(4242),
		Register: occ, RegisterSeq: smsg.Sequence, Token: token,
	}); err != nil {
		t.Fatalf("the atomic token+record acquisition was refused: %v", err)
	}

	cur, _, _ := e.asm.jsa.loadInflightRecord(regSubj)
	if cur == nil || cur.ActionToken != token {
		t.Fatalf("the token was not installed on the register: %+v", cur)
	}
	if cur.Acq != occ.Acq+1 {
		t.Fatalf("the acquisition counter did not advance: %d", cur.Acq)
	}
	if cur.ActionClaim != occ.Claim || cur.ActionClaimSeq != occ.ClaimSeq {
		t.Fatalf("the token was not bound to the register's own custody")
	}
	p, err := e.asm.proveExchangeDischarged(e.id, e.epoch, auth.Gen, nonce, func(string, string, uint64) bool { return false })
	if err != nil {
		t.Fatalf("proving: %v", err)
	}
	if len(p.Constituents) != 1 {
		t.Fatalf("the action record was not created beside the token: %+v", p)
	}
}
