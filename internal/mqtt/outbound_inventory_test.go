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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// testF105Ledger builds a ledger body for the fixture generation.
func testF105Ledger(e *testF105Env, state mqttGenLedgerState, hi uint64) *mqttGenLedger {
	l := &mqttGenLedger{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, State: state, Hi: hi}
	if state == mqttGenLedgerSealed {
		l.SealedHi = hi
	}
	return l
}

// testF105Duty builds a duty entry body at one ordinal.
func testF105Duty(e *testF105Env, ord uint64, kind mqttDutyKind, ref string,
	state mqttDutyState) *mqttDutyEntry {

	d := &mqttDutyEntry{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: ord, Kind: kind, Ref: ref, State: state,
	}
	if state == mqttDutyTerminal {
		d.Outcome = "completed"
	}
	return d
}

// testF105SubmitInventory submits one batch of ledger and duty members straight to the write
// boundary, which is where these rules live.
//
// DIRECT, deliberately. The composers that will emit inventory members do not exist yet, and a
// rule proven only through a composer is one the boundary never had to make — which is the shape
// that let a mutation survive earlier in this work package.
func testF105SubmitInventory(t *testing.T, e *testF105Env, txn string,
	members ...mqttTransitionMember) error {
	t.Helper()
	return testF105SubmitInventoryAs(t, e, mqttTransitionDisposeResource, txn, members...)
}

// testF105SubmitInventoryAs is the same, under a named transition kind.
//
// The kind matters because each one declares which member classes it may carry, and those rules
// run BEFORE the inventory boundary. A batch carrying an exchange has to go in under a kind that
// admits exchanges, or the refusal comes from the member classifier and the rule under test is
// never reached — which is a fixture not reaching its route.
func testF105SubmitInventoryAs(t *testing.T, e *testF105Env, kind mqttTransitionKind, txn string,
	members ...mqttTransitionMember) error {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority does not load back: status %v", al.status)
	}
	_, err := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: kind, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: mqttAuthorityServing, Rev: al.rec.Rev + 1, Txn: txn, OwnerTxn: al.rec.OwnerTxn,
		},
		Extra: members,
	})
	return err
}

// testF105SubmitInventoryHolding is testF105SubmitInventory for a generation that is already
// CLOSED: it PRESERVES the authority's state instead of asserting SERVING.
//
// Every production caller that touches inventory after a closure does this — opResolveDuty,
// completeCleanupAndResolve and the seal all carry `State: auth.State` — and a fixture that moved
// a closed authority back to serving would be refused by the state rule before reaching whatever
// inventory rule it meant to test.
func testF105SubmitInventoryHolding(t *testing.T, e *testF105Env, txn string,
	members ...mqttTransitionMember) error {

	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority does not load back: status %v", al.status)
	}
	_, err := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionDisposeResource, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: al.rec.State, Rev: al.rec.Rev + 1, Txn: txn, OwnerTxn: al.rec.OwnerTxn,
		},
		Extra: members,
	})
	return err
}

func testF105LedgerMember(t *testing.T, e *testF105Env, expect uint64, l *mqttGenLedger) mqttTransitionMember {
	t.Helper()
	body, err := json.Marshal(l)
	if err != nil {
		t.Fatalf("marshalling the ledger: %v", err)
	}
	return mqttTransitionMember{
		subj: mqttGenLedgerSubject(getHash(e.id), testF105Gen), expect: expect, body: body,
	}
}

func testF105DutyMember(t *testing.T, e *testF105Env, expect uint64, d *mqttDutyEntry) mqttTransitionMember {
	t.Helper()
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshalling the duty entry: %v", err)
	}
	return mqttTransitionMember{
		subj: mqttDutySubject(getHash(e.id), testF105Gen, d.Ord), expect: expect, body: body,
	}
}

// testF105PlantDuty overwrites a duty record IN THE STORE, at the sequence it stands.
//
// For fixtures whose subject is what a READER does with a record, not what a writer may submit.
// A duty reaches TERMINAL in production through a continuation whose ending the boundary
// requires to be earned; an allocation may never start there, which is a write rule these
// fixtures are not about and which now refuses them. Planting the body directly keeps the
// reader's scenario — a terminal record standing in the inventory — without asking the write
// boundary to admit a batch no writer is allowed to compose.
func testF105PlantDuty(t *testing.T, e *testF105Env, d *mqttDutyEntry) {
	t.Helper()
	subj := mqttDutySubject(getHash(e.id), testF105Gen, d.Ord)
	cur, cerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if cerr != nil || cur == nil {
		t.Fatalf("reading duty %d before planting over it: %v", d.Ord, cerr)
	}
	body, merr := json.Marshal(d)
	if merr != nil {
		t.Fatalf("marshalling the duty entry: %v", merr)
	}
	msg, hdr := mqttFrameBatchWrite(cur.Sequence, body, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(subj, hdr, msg); err != nil {
		t.Fatalf("planting duty %d: %v", d.Ord, err)
	}
}

// testF105LoadDutyIfAny is testF105LoadDuty for a fixture asserting that NOTHING was written.
func testF105LoadDutyIfAny(t *testing.T, e *testF105Env, ord uint64) (*mqttDutyEntry, uint64) {
	t.Helper()
	subj := mqttDutySubject(getHash(e.id), testF105Gen, ord)
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if err != nil || smsg == nil {
		return nil, 0
	}
	d, derr := mqttDecodeDutyEntry(smsg.Data, e.id, e.epoch, testF105Gen, ord)
	if derr != nil {
		t.Fatalf("a record stands at duty %d that cannot be decoded: %v", ord, derr)
	}
	return d, smsg.Sequence
}

// testF105LoadDuty reads one duty entry back with its sequence.
func testF105LoadDuty(t *testing.T, e *testF105Env, ord uint64) (*mqttDutyEntry, uint64) {
	t.Helper()
	subj := mqttDutySubject(getHash(e.id), testF105Gen, ord)
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if err != nil || smsg == nil {
		t.Fatalf("reading duty %d: %v", ord, err)
	}
	d, derr := mqttDecodeDutyEntry(smsg.Data, e.id, e.epoch, testF105Gen, ord)
	if derr != nil {
		t.Fatalf("decoding duty %d: %v", ord, derr)
	}
	return d, smsg.Sequence
}

// TestF105OrdinalTokensOrderAndRoundTrip pins the encoding the whole cursor rests on.
//
// The ordinal is a SUBJECT TOKEN rather than a field because a record's JetStream sequence moves
// when it is updated and its name does not — so an entry updated behind a cursor is still found
// where the cursor expects it. That only works if lexical subject order IS ordinal order, which
// is what fixed-width zero padding buys and what this checks rather than assumes.
func TestF105OrdinalTokensOrderAndRoundTrip(t *testing.T) {
	prev := ""
	for _, ord := range []uint64{1, 2, 9, 10, 11, 99, 100, 1_000_000, 1 << 32, 1<<64 - 1} {
		tk := mqttFormatOrdinal(ord)
		if len(tk) != mqttOrdinalWidth {
			t.Fatalf("ordinal %d rendered %d characters wide", ord, len(tk))
		}
		// LEXICAL ORDER IS ORDINAL ORDER. Without this a cursor walking subjects would visit
		// 10 before 9 and a bounded batch would silently skip work.
		if prev != "" && !(prev < tk) {
			t.Fatalf("ordinal %d renders %q, which does not sort after %q", ord, tk, prev)
		}
		prev = tk
		back, err := mqttParseOrdinal(tk)
		if err != nil || back != ord {
			t.Fatalf("ordinal %d did not round trip: %v %v", ord, back, err)
		}
	}

	for name, tk := range map[string]string{
		"too short":            "1",
		"too long":             strings.Repeat("0", mqttOrdinalWidth+1),
		"not a number":         strings.Repeat("x", mqttOrdinalWidth),
		"negative":             "-" + strings.Repeat("0", mqttOrdinalWidth-1),
		"ordinal zero":         strings.Repeat("0", mqttOrdinalWidth),
		"padded with a space":  strings.Repeat(" ", mqttOrdinalWidth-1) + "1",
		"a plausible overflow": strings.Repeat("9", mqttOrdinalWidth),
	} {
		if _, err := mqttParseOrdinal(tk); err == nil {
			t.Fatalf("%s (%q) parsed as an ordinal", name, tk)
		}
	}
}

// TestF105InventoryRecordsRefuseUnsupportedBodies pins the two record shapes.
func TestF105InventoryRecordsRefuseUnsupportedBodies(t *testing.T) {
	e := &testF105Env{id: testF105ID, epoch: testF105Epoch}

	t.Run("ledger", func(t *testing.T) {
		for name, tc := range map[string]struct {
			l  *mqttGenLedger
			ok bool
		}{
			"an open ledger":              {testF105Ledger(e, mqttGenLedgerOpen, 0), true},
			"an open ledger with entries": {testF105Ledger(e, mqttGenLedgerOpen, 7), true},
			"a sealed ledger":             {testF105Ledger(e, mqttGenLedgerSealed, 7), true},

			// AN OPEN LEDGER HAS NO BOUND: a bound exists only once the shape is fixed, and
			// one carried here would let a sweep believe it had something to cover.
			"an open ledger carrying a bound": {
				&mqttGenLedger{ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
					State: mqttGenLedgerOpen, Hi: 3, SealedHi: 3}, false},
			// A SEALED BOUND BELOW THE HIGH-WATER claims coverage of an inventory that already
			// reaches past it — the exact under-coverage the retirement proof exists to stop.
			"a sealed bound below the high-water": {
				&mqttGenLedger{ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
					State: mqttGenLedgerSealed, Hi: 7, SealedHi: 5}, false},
			"a sealed bound above the high-water": {
				&mqttGenLedger{ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
					State: mqttGenLedgerSealed, Hi: 5, SealedHi: 7}, false},
			"a sealed ledger with no bound": {
				&mqttGenLedger{ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
					State: mqttGenLedgerSealed, Hi: 5}, false},
			"an unsupported state": {
				&mqttGenLedger{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, State: 9, Hi: 1}, false},
			"no state at all": {
				&mqttGenLedger{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Hi: 1}, false},
		} {
			err := mqttValidateGenLedger(tc.l, e.id, e.epoch, testF105Gen)
			if tc.ok && err != nil {
				t.Fatalf("%s was refused: %v", name, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("%s was accepted", name)
			}
		}
	})

	t.Run("duty", func(t *testing.T) {
		for name, tc := range map[string]struct {
			d  *mqttDutyEntry
			ok bool
		}{
			"a pending exchange duty": {testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending), true},
			"a predeclared cleanup":   {testF105Duty(e, 1, mqttDutyCleanup, "inc-1", mqttDutyNotYetActionable), true},
			"a terminal provisioning": {testF105Duty(e, 1, mqttDutyProvision, "inc-1", mqttDutyTerminal), true},
			"an unsupported kind":     {&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1, Kind: "other", Ref: "x", State: mqttDutyPending}, false},
			"an unsupported state":    {&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1, Kind: mqttDutyExchange, Ref: "x", State: 9}, false},
			"ordinal zero":            {&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 0, Kind: mqttDutyExchange, Ref: "x", State: mqttDutyPending}, false},
			"a malformed reference":   {&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1, Kind: mqttDutyExchange, Ref: "not a token", State: mqttDutyPending}, false},

			// TERMINAL WITHOUT AN OUTCOME is an ending with nothing to say about itself, and
			// "classified" is the word that makes a duty terminal at all.
			"terminal with no outcome": {
				&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
					Kind: mqttDutyExchange, Ref: "xch-a", State: mqttDutyTerminal}, false},
			// AN OUTCOME BEFORE THE END describes a classified result for work still owed.
			"pending with an outcome": {
				&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
					Kind: mqttDutyExchange, Ref: "xch-a", State: mqttDutyPending,
					Outcome: "completed"}, false},
			// A CLAIM COORDINATES WORK, so an ended duty holds none.
			"terminal and still claimed": {
				&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
					Kind: mqttDutyExchange, Ref: "xch-a", State: mqttDutyTerminal,
					Outcome: "completed", Claim: "broker-1", LeaseUntil: 5}, false},
			// A LEASE WITHOUT A CLAIMANT NAMES NOBODY; a claimant without a lease never
			// expires. Either alone is a record no reader can act on.
			"a lease with no claimant": {
				&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
					Kind: mqttDutyExchange, Ref: "xch-a", State: mqttDutyPending,
					LeaseUntil: 5}, false},
			"a claimant with no lease": {
				&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
					Kind: mqttDutyExchange, Ref: "xch-a", State: mqttDutyPending,
					Claim: "broker-1"}, false},
			"a claim and a lease together": {
				&mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
					Kind: mqttDutyExchange, Ref: "xch-a", State: mqttDutyPending,
					Claim: "broker-1", LeaseUntil: 5}, true},
		} {
			err := mqttValidateDutyEntry(tc.d, e.id, e.epoch, testF105Gen, tc.d.Ord)
			if tc.ok && err != nil {
				t.Fatalf("%s was refused: %v", name, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("%s was accepted", name)
			}
		}
	})
}

// TestF105NotYetActionableIsNotResolved is consult 5's requirement, pinned on its own.
//
// "A predeclared entry must remain unresolved while its conditional future work is still
// possible; otherwise a cursor can pass a 'finished' entry that later becomes owed." So a
// cleanup duty whose resource still has dependents is NOT resolved, and neither is a claimed
// one, and neither is one whose claim expired. Only a classified ending is.
func TestF105NotYetActionableIsNotResolved(t *testing.T) {
	for name, tc := range map[string]struct {
		state    mqttDutyState
		resolved bool
	}{
		"pending":            {mqttDutyPending, false},
		"not yet actionable": {mqttDutyNotYetActionable, false},
		"terminal":           {mqttDutyTerminal, true},
	} {
		if got := mqttDutyResolved(tc.state); got != tc.resolved {
			t.Fatalf("%s reported resolved=%v", name, got)
		}
	}
}

// TestF105AllocationRequiresItsLedger is rule 1, and the shape an earlier design let through.
//
// The earlier rules were conditioned on the ledger member's CONTENTS, so a batch that simply
// omitted the ledger escaped them entirely. Here the absence IS the violation.
func TestF105AllocationRequiresItsLedger(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-alloc-ledger")
	defer done()
	e.bootstrap(t, "txn-1")

	// NO LEDGER MEMBER AT ALL.
	err := testF105SubmitInventory(t, e, "txn-bare",
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)))
	if err == nil {
		t.Fatalf("a duty entry was allocated with no ledger member")
	}
	if !strings.Contains(err.Error(), "no generation ledger member") {
		t.Fatalf("the refusal did not come from the allocation rule: %v", err)
	}

	// THE POSITIVE CONTROL: the same entry, with the ledger CAS that allocates its ordinal.
	if err := testF105SubmitInventory(t, e, "txn-first",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)),
	); err != nil {
		t.Fatalf("a properly ledgered allocation was refused: %v", err)
	}
	if d, _ := testF105LoadDuty(t, e, 1); d.Ref != "xch-a" {
		t.Fatalf("the allocated duty did not land: %v", d)
	}
}

// TestF105AllocationIsExactlyTheDeclaredInterval is the contiguity rule, and it is ENFORCED
// rather than inferred from CAS contention.
//
// An earlier version of this design argued that contention on the ledger made holes impossible.
// It does not: contention decides WHO writes, not WHAT they write, so nothing stopped a composer
// from stepping the high-water by three and writing entries at two of the ordinals. The gap is
// then a duty no sweep will visit, because a cursor reaching it cannot tell a hole from the end
// of the inventory.
func TestF105AllocationIsExactlyTheDeclaredInterval(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-alloc-interval")
	defer done()
	e.bootstrap(t, "txn-1")

	// A GAP WITH THE RIGHT COUNT. This shape matters and the first version of this fixture
	// missed it: moving the high-water by three and supplying two entries is caught by the
	// COUNT, so it proved nothing about the interval. Here the count is exactly right — three
	// entries for three ordinals — and one of them sits outside the interval, leaving ordinal 3
	// unoccupied. A cursor reaching 3 cannot tell that hole from the end of the inventory, so
	// the duty at 4 is one no sweep will ever visit.
	err := testF105SubmitInventory(t, e, "txn-gap",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 3)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 2, mqttDutyExchange, "xch-b", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 4, mqttDutyExchange, "xch-d", mqttDutyPending)))
	if err == nil {
		t.Fatalf("an allocation left a hole in the inventory")
	}
	if !strings.Contains(err.Error(), "a gap is a duty no sweep will visit") {
		t.Fatalf("the refusal did not come from the interval rule: %v", err)
	}

	// AND THE COUNT RULE IS SEPARATE, with its own shape: fewer entries than the high-water
	// declares. Kept apart from the gap above so a mutation of either is a real kill rather
	// than one the other masks.
	if err := testF105SubmitInventory(t, e, "txn-short",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 3)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 2, mqttDutyExchange, "xch-b", mqttDutyPending)),
	); err == nil {
		t.Fatalf("fewer entries were allocated than the ledger's high-water declares")
	}

	// MORE ENTRIES THAN THE LEDGER DECLARES.
	if err := testF105SubmitInventory(t, e, "txn-over",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 2, mqttDutyExchange, "xch-b", mqttDutyPending)),
	); err == nil {
		t.Fatalf("more entries were allocated than the ledger's high-water declares")
	}

	// A LEDGER THAT MOVES WITH NO ENTRIES AT ALL: the ordinals would be consumed and nothing
	// would occupy them, which is the same hole from the other side.
	if err := testF105SubmitInventory(t, e, "txn-empty",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 2)),
	); err == nil {
		t.Fatalf("the high-water moved with no entries allocated")
	}

	// THE POSITIVE CONTROL: a whole consecutive interval, allocated at once, which is what an
	// admission predeclaring a resource's provisioning and cleanup duties actually does.
	if err := testF105SubmitInventory(t, e, "txn-three",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 3)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 2, mqttDutyProvision, "inc-1", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 3, mqttDutyCleanup, "inc-1", mqttDutyNotYetActionable)),
	); err != nil {
		t.Fatalf("a whole consecutive interval was refused: %v", err)
	}

	// AND THE NEXT ALLOCATION CONTINUES FROM THE STANDING HIGH-WATER, not from 1.
	_, ledgerSeq := testF105LoadLedger(t, e)
	if err := testF105SubmitInventory(t, e, "txn-next",
		testF105LedgerMember(t, e, ledgerSeq, testF105Ledger(e, mqttGenLedgerOpen, 4)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 4, mqttDutyExchange, "xch-d", mqttDutyPending)),
	); err != nil {
		t.Fatalf("the following allocation was refused: %v", err)
	}
}

// testF105LoadLedger reads the fixture generation's ledger back with its sequence.
func testF105LoadLedger(t *testing.T, e *testF105Env) (*mqttGenLedger, uint64) {
	t.Helper()
	l, seq, err := e.asm.jsa.loadGenLedger(e.id, e.epoch, testF105Gen)
	if err != nil || l == nil {
		t.Fatalf("reading the generation ledger: %v %v", l, err)
	}
	return l, seq
}

// TestF105SealingForbidsAllocationAndIsNotRevocable is the fence, and the whole reason closure
// does not have to rewrite anything.
func TestF105SealingForbidsAllocationAndIsNotRevocable(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-fence")
	defer done()
	e.bootstrap(t, "txn-1")

	// A REAL OBLIGATION, admitted through the surface, because completing its duty at the end
	// of this test now requires the exchange to have actually ended — a duty's ending is proved
	// against the thing it is about, not asserted.
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-one", 7, "inc-src")
	led, seq := testF105LoadLedger(t, e)
	if led.Hi != 1 {
		t.Fatalf("the admission allocated %d ordinals, want 1", led.Hi)
	}
	// THE SEAL GOES THROUGH A CLOSING BATCH, because sealing IS the closing act: a seal with no
	// closure leaves a generation that can neither close (the composers refuse an already-sealed
	// ledger) nor be recovered (no root was allocated against the seal). Holistic round 10 found
	// that this fixture had been sealing by the bypass. The fence it tests is unchanged.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority does not load back: status %v", al.status)
	}
	shard := mqttDirShardFor(getHash(e.id))
	hdr := &mqttDirShard{Shard: shard, Hi: 1, Next: 1, Cycle: 1, Horizon: 1}
	root := &mqttRecoveryRoot{
		Shard: shard, Ord: 1, ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
		SweepStart: 1, Resume: 1, Bound: 1,
	}
	if err := testF105ClosingBatch(t, e, al.rec, al.seq, "txn-seal",
		testF105ClosureRecord(e, "txn-seal", false, shard, 1),
		testF105LedgerMember(t, e, seq, testF105Ledger(e, mqttGenLedgerSealed, 1)),
		testF105Member(t, mqttShardSubject(shard), 0, hdr),
		testF105Member(t, mqttRootSubject(root.Shard, root.Ord), 0, root),
	); err != nil {
		t.Fatalf("sealing the inventory through its closing batch was refused: %v", err)
	}

	// ALLOCATION IS WHAT THE SEAL FORBIDS, and it is the only thing it has to forbid.
	_, sealedSeq := testF105LoadLedger(t, e)
	err := testF105SubmitInventoryHolding(t, e, "txn-late",
		testF105LedgerMember(t, e, sealedSeq, testF105Ledger(e, mqttGenLedgerSealed, 2)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 2, mqttDutyExchange, "xch-b", mqttDutyPending)))
	if err == nil {
		t.Fatalf("a duty was allocated in a sealed generation")
	}
	// The refusal may now name the CLOSURE rather than the seal, and that is not a weaker
	// assertion: since holistic round 10, a ledger is sealed only inside the batch that closes
	// its generation, so the closure's "last act able to take an ordinal" IS the seal's
	// permanence. What is still required is a refusal that names that permanence — an
	// unrelated refusal (a state rule, a member classifier) must still fail this test.
	if !strings.Contains(err.Error(), "was sealed at") &&
		!strings.Contains(err.Error(), "sealing fixes the bound") &&
		!strings.Contains(err.Error(), "closed permanently") {
		t.Fatalf("the refusal did not come from the seal or its closure: %v", err)
	}

	// UNSEALING IS REFUSED. Everything completing after a seal relies on it, so it is not
	// revocable — the same rule the resource's provisioning seal carries, for the same reason.
	if err := testF105SubmitInventoryHolding(t, e, "txn-reopen",
		testF105LedgerMember(t, e, sealedSeq, testF105Ledger(e, mqttGenLedgerOpen, 1)),
	); err == nil {
		t.Fatalf("a sealed inventory was reopened")
	}

	// AND COMPLETION STILL RUNS. This is the point of the whole design: the seal fixes the
	// inventory's SHAPE, and the states of its entries keep moving.
	testF105FinishExchange(t, e, nonce)
	d, dseq := testF105LoadDuty(t, e, 1)
	ended := *d
	ended.State, ended.Outcome = mqttDutyTerminal, "completed"
	if err := testF105SubmitInventoryHolding(t, e, "txn-finish",
		testF105DutyMember(t, e, dseq, &ended)); err != nil {
		t.Fatalf("a duty could not be completed after its generation was sealed: %v", err)
	}
}

// TestF105DutyContinuationsPreserveIdentityAndTerminality is rule 2 and the invariant the
// recovery cursor rests on.
//
// The bounded sweep advances past every ordinal it has established terminal. If terminality
// could reverse, that advance would step over work that came back, and no cursor discipline
// could recover it — the sweep would have no reason to look again.
func TestF105DutyContinuationsPreserveIdentityAndTerminality(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-duty-continue")
	defer done()
	e.bootstrap(t, "txn-1")

	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-one", 7, "inc-src")
	d, dseq := testF105LoadDuty(t, e, 1)

	// A CONTINUATION OVER NOTHING would invent the duty it claims to advance.
	if err := testF105SubmitInventory(t, e, "txn-ghost",
		testF105DutyMember(t, e, 99, testF105Duty(e, 7, mqttDutyExchange, "xch-z", mqttDutyPending)),
	); err == nil {
		t.Fatalf("a continuation advanced a duty that does not exist")
	}

	// A MOVED SEQUENCE: the continuation was decided against a record that is no longer there.
	if err := testF105SubmitInventory(t, e, "txn-stale",
		testF105DutyMember(t, e, dseq+1000, d)); err == nil {
		t.Fatalf("a continuation landed against a sequence nothing stands at")
	}

	// RE-IDENTIFYING an entry would let a batch repurpose an ordinal into a duty nothing
	// reserved — the allocation rule evaded rather than obeyed.
	moved := *d
	moved.Kind, moved.Ref = mqttDutyCleanup, "inc-9"
	if err := testF105SubmitInventory(t, e, "txn-repoint",
		testF105DutyMember(t, e, dseq, &moved)); err == nil {
		t.Fatalf("a duty entry was re-identified to another duty")
	}

	// THE POSITIVE CONTROL: an ordinary advance to terminal, with its classified outcome —
	// after the obligation itself has ended, which is what earns it.
	testF105FinishExchange(t, e, nonce)
	d, dseq = testF105LoadDuty(t, e, 1)
	ended := *d
	ended.State, ended.Outcome = mqttDutyTerminal, "completed"
	if err := testF105SubmitInventory(t, e, "txn-end",
		testF105DutyMember(t, e, dseq, &ended)); err != nil {
		t.Fatalf("a duty could not reach its ending: %v", err)
	}
	_, endedSeq := testF105LoadDuty(t, e, 1)

	// AND IT NEVER OWES WORK AGAIN.
	reopened := ended
	reopened.State, reopened.Outcome = mqttDutyPending, _EMPTY_
	err := testF105SubmitInventory(t, e, "txn-reopen",
		testF105DutyMember(t, e, endedSeq, &reopened))
	if err == nil {
		t.Fatalf("a terminal duty was reopened")
	}
	if !strings.Contains(err.Error(), "may never owe work again") {
		t.Fatalf("the refusal did not come from the terminality rule: %v", err)
	}

	// NOR IS ITS ENDING REWRITTEN. An ending is recorded once.
	relabelled := ended
	relabelled.Outcome = "something else"
	if err := testF105SubmitInventory(t, e, "txn-relabel",
		testF105DutyMember(t, e, endedSeq, &relabelled)); err == nil {
		t.Fatalf("a terminal duty's classified outcome was rewritten")
	}
}

// TestF105DutyContinuationExpectationNamesTheInspectedEntry is the inventory's version of
// consult 2's item 7, and it exists because two mutations survived without it.
//
// The STORE's CAS establishes "the entry at commit stands at the supplied expectation". This
// validator establishes "the supplied expectation names the entry whose BODY this validator
// inspected". Those are different facts, and the difference is the read-to-commit interval: the
// identity and terminality rules above are decided against a body read HERE, so an expectation
// that named a different record would have them validating one entry and committing over
// another.
//
// Driven DIRECTLY at checkInventoryMembers, because through commitAuthorityTransition the store
// supplies the refusal and the check is unreachable — which is precisely why MUT-D7 and MUT-D8
// survived their first run.
func TestF105DutyContinuationExpectationNamesTheInspectedEntry(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-duty-expect")
	defer done()
	e.bootstrap(t, "txn-1")

	if err := testF105SubmitInventory(t, e, "txn-one",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)),
	); err != nil {
		t.Fatalf("the allocation was refused: %v", err)
	}
	d, dseq := testF105LoadDuty(t, e, 1)
	attempt := &mqttTransitionAttempt{ID: e.id, Epoch: e.epoch, Txn: "txn-direct", Op: "op-direct"}

	// THE POSITIVE CONTROL: the true sequence validates, so the refusals below are about the
	// expectation and not about this shape being rejected wholesale.
	if err := e.asm.jsa.checkInventoryMembers(attempt,
		[]mqttTransitionMember{testF105DutyMember(t, e, dseq, d)}); err != nil {
		t.Fatalf("a continuation at the standing sequence was refused: %v", err)
	}

	// A SEQUENCE NOTHING STANDS AT. The store would refuse this too — but it would refuse it
	// AFTER this validator had already decided identity and terminality against whatever body
	// the caller supplied, which is the decision this check is protecting.
	if err := e.asm.jsa.checkInventoryMembers(attempt,
		[]mqttTransitionMember{testF105DutyMember(t, e, dseq+1000, d)}); err == nil {
		t.Fatalf("a continuation validated against a sequence nothing stands at")
	} else if !strings.Contains(err.Error(), "no longer there") {
		t.Fatalf("the refusal did not come from the expectation rule: %v", err)
	}

	// AN ENTRY THAT IS NOT THERE AT ALL. Ordinal 7 was never allocated, so a continuation over
	// it would invent the duty it claims to advance — and inventing one is how an ordinal
	// nothing reserved acquires an occupant.
	ghost := testF105Duty(e, 7, mqttDutyExchange, "xch-z", mqttDutyPending)
	if err := e.asm.jsa.checkInventoryMembers(attempt,
		[]mqttTransitionMember{testF105DutyMember(t, e, 5, ghost)}); err == nil {
		t.Fatalf("a continuation validated over an entry that does not exist")
	} else if !strings.Contains(err.Error(), "nothing stands there") {
		t.Fatalf("the refusal did not come from the absent-entry rule: %v", err)
	}
}

// testF105CoverExchanges appends the inventory that a batch's create-only EXCHANGE members owe.
//
// Rule 3 makes inventory coverage mandatory for every admitted obligation, and most fixtures in
// this package build their exchange members by hand because they are testing something else —
// identifier exclusivity, batch atomicity, a member-shape rule. Making each of them assemble a
// ledger CAS and a duty entry by hand would bury what they are actually about, and a fixture
// that got the interval wrong would fail for a reason unrelated to its subject.
//
// It reads the STANDING ledger and allocates from it, so a batch covered this way is the same
// shape the composers produce rather than a hand-waved one — which is what keeps this a fixture
// and not an exemption. A test specifically about coverage does NOT use it; it omits the
// inventory on purpose and asserts the refusal.
func testF105CoverExchanges(t *testing.T, e *testF105Env, gen string,
	members []mqttTransitionMember) []mqttTransitionMember {
	t.Helper()

	idHash := getHash(e.id)
	var decls []mqttDutyDeclaration
	for _, m := range members {
		if m.expect != 0 {
			continue
		}
		cls, ok := mqttF105ObligationClass(m.subj)
		if !ok || cls != mqttExchangeClassTk {
			continue
		}
		toks := strings.Split(m.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 || toks[1] != idHash {
			continue
		}
		decls = append(decls, mqttDutyDeclaration{
			Kind: mqttDutyExchange, Ref: toks[3], State: mqttDutyPending,
		})
	}
	if len(decls) == 0 {
		return members
	}
	cur, curSeq, err := e.asm.jsa.loadGenLedger(e.id, e.epoch, gen)
	if err != nil {
		t.Fatalf("reading the ledger of generation %q: %v", gen, err)
	}
	inv, ierr := mqttAllocateInventoryMembers(e.id, e.epoch, gen, cur, curSeq, decls)
	if ierr != nil {
		t.Fatalf("allocating inventory for %d exchange(s): %v", len(decls), ierr)
	}
	return append(append([]mqttTransitionMember{}, members...), inv...)
}

// testF105AdmitExchangeTr composes a bare exchange admission with the inventory it owes.
//
// mqttAdmitExchangeTransition needs the standing ledger, because an admission allocates its
// obligation's duty ordinal from it. Reading it at every call site would be noise in tests that
// are about something else entirely, so it is read once here.
func testF105AdmitExchangeTr(t *testing.T, e *testF105Env, prev *mqttOutboundAuthority,
	prevSeq uint64, txn, op string, x *mqttOutboundExchange) (*mqttAuthorityTransition, error) {
	t.Helper()
	led, ledSeq, err := e.asm.jsa.loadGenLedger(e.id, e.epoch, x.Gen)
	if err != nil {
		t.Fatalf("reading the ledger of generation %q: %v", x.Gen, err)
	}
	return mqttAdmitExchangeTransition(prev, prevSeq, txn, op, x, led, ledSeq)
}

// TestF105CreationsOweTheirInventoryCoverage is rule 3, and it is the one the other two cannot
// state.
//
// Rules 1 and 2 are about batches that DO carry inventory members: an allocation must match its
// ledger, a continuation must match its standing entry. A caller that simply omitted the
// inventory would satisfy both and still install an obligation nothing can discover — the
// failure arrived at by NOT PARTICIPATING rather than by participating wrongly. Consult 5 put it
// as "a resource creation cannot become permitted merely because the caller labels it
// completion".
//
// Every case here is submitted straight to the boundary with the coverage deliberately left out,
// which is the one place in this package a fixture does that on purpose.
func TestF105CreationsOweTheirInventoryCoverage(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-coverage")
	defer done()
	e.bootstrap(t, "txn-1")

	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	nonce := mqttNewInflightNonce()
	// The exchange NAMES ITS ADMITTING TRANSITION, so the member is built per txn: an obligation
	// carrying somebody else's transition id is refused for that reason instead, and the
	// coverage rule would never be reached.
	xchMember := func(txn string) mqttTransitionMember {
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: nonce, PI: 7, QoS: 2,
			Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		xbody, err := json.Marshal(x)
		if err != nil {
			t.Fatalf("marshalling the exchange: %v", err)
		}
		return mqttTransitionMember{
			subj: mqttExchangeSubject(getHash(e.id), al.rec.Gen, nonce), expect: 0, body: xbody,
		}
	}

	// AN OBLIGATION WITH NO ENTRY NAMING IT.
	if err := testF105SubmitInventoryAs(t, e, mqttTransitionAdmitExchange, "txn-uncovered", xchMember("txn-uncovered")); err == nil {
		t.Fatalf("an obligation was admitted with no duty entry naming it")
	} else if !strings.Contains(err.Error(), "no duty entry naming it") {
		t.Fatalf("the refusal did not come from the coverage rule: %v", err)
	}

	// AN ENTRY NAMING SOMETHING ELSE is not coverage either. This is the case a rule counting
	// entries rather than matching them would miss: the batch carries a properly ledgered
	// allocation, so rules 1 and 2 are satisfied, and it still describes a different duty.
	if err := testF105SubmitInventoryAs(t, e, mqttTransitionAdmitExchange, "txn-mismatched",
		xchMember("txn-mismatched"),
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-somebody-else",
			mqttDutyPending)),
	); err == nil {
		t.Fatalf("an entry naming another obligation was accepted as coverage")
	}

	// THE POSITIVE CONTROL: the same obligation, with the entry that names it.
	if err := testF105SubmitInventoryAs(t, e, mqttTransitionAdmitExchange, "txn-covered",
		xchMember("txn-covered"),
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, nonce, mqttDutyPending)),
	); err != nil {
		t.Fatalf("a covered admission was refused: %v", err)
	}

	// AND A RESOURCE OWES TWO. The cleanup entry is the one that is easy to forget and
	// impossible to add later: it becomes actionable when the last dependent goes, which may be
	// after closure, when allocating an ordinal is exactly what the seal forbids.
	e2, done2 := testF105NewEnv(t, "f105-coverage-res")
	defer done2()
	e2.bootstrap(t, "txn-1")
	al2 := e2.asm.jsa.loadOutboundAuthority(e2.id, e2.epoch)
	r := &mqttOutboundResource{
		ID: e2.id, Epoch: e2.epoch, Gen: al2.rec.Gen, Inc: "inc-cov",
		Kind: mqttResourceSourceConsumer, Name: "$MQTT_src", State: mqttResourceLive,
		Prov: mqttProvisioningPending, Deps: []mqttResourceEdge{},
	}
	rbody, rerr := json.Marshal(r)
	if rerr != nil {
		t.Fatalf("marshalling the resource: %v", rerr)
	}
	resMember := mqttTransitionMember{
		subj: mqttResourceSubject(getHash(e2.id), al2.rec.Gen, "inc-cov"), expect: 0, body: rbody,
	}
	// PROVISIONING ONLY: the cleanup entry is missing, and that is the refusal.
	if err := testF105SubmitInventoryAs(t, e2, mqttTransitionProvisionResource, "txn-half",
		resMember,
		testF105LedgerMember(t, e2, 0, testF105Ledger(e2, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e2, 0, testF105Duty(e2, 1, mqttDutyProvision, "inc-cov",
			mqttDutyPending)),
	); err == nil {
		t.Fatalf("a resource was provisioned with no cleanup duty reserved")
	} else if !strings.Contains(err.Error(), "no cleanup duty") {
		t.Fatalf("the refusal did not name the missing cleanup duty: %v", err)
	}

	// AND THE OTHER HALF. Reserving the cleanup but not the provisioning is the mirror case,
	// and it needs its own fixture: with only the "missing cleanup" case above, a rule that
	// checked the cleanup entry and silently skipped the provisioning one passed every
	// assertion. A mutation showed exactly that.
	if err := testF105SubmitInventoryAs(t, e2, mqttTransitionProvisionResource, "txn-otherhalf",
		resMember,
		testF105LedgerMember(t, e2, 0, testF105Ledger(e2, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e2, 0, testF105Duty(e2, 1, mqttDutyCleanup, "inc-cov",
			mqttDutyNotYetActionable)),
	); err == nil {
		t.Fatalf("a resource was provisioned with no provisioning duty reserved")
	} else if !strings.Contains(err.Error(), "no provisioning duty") {
		t.Fatalf("the refusal did not name the missing provisioning duty: %v", err)
	}

	// THE POSITIVE CONTROL for a resource: both duties, one batch.
	if err := testF105SubmitInventoryAs(t, e2, mqttTransitionProvisionResource, "txn-both",
		resMember,
		testF105LedgerMember(t, e2, 0, testF105Ledger(e2, mqttGenLedgerOpen, 2)),
		testF105DutyMember(t, e2, 0, testF105Duty(e2, 1, mqttDutyProvision, "inc-cov",
			mqttDutyPending)),
		testF105DutyMember(t, e2, 0, testF105Duty(e2, 2, mqttDutyCleanup, "inc-cov",
			mqttDutyNotYetActionable)),
	); err != nil {
		t.Fatalf("a fully covered provisioning was refused: %v", err)
	}
}

// TestF105ADutyEndingMustBeEarned is round I-2's second finding.
//
// Immutability of a terminal duty preserves an ending; it says NOTHING about whether the ending
// was earned. Before this rule a batch could move a PENDING or NOT_YET_ACTIONABLE duty straight
// to TERMINAL with an outcome of its choosing, never touching the exchange or resource the duty
// is about — and then every sweep passes that ordinal forever and the generation can retire while
// the obligation is still owed.
//
// It is "a count can be decremented by a transition that ended nothing", one level up from the
// count that sentence was first written about.
func TestF105ADutyEndingMustBeEarned(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-duty-earned")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	d, dseq := testF105LoadDuty(t, e, 1)
	if d.Ref != nonce {
		t.Fatalf("the admission's duty names %q, not its obligation %q", d.Ref, nonce)
	}

	// THE OBLIGATION IS STILL LIVE, so its duty has not ended however firmly the batch says so.
	ended := *d
	ended.State, ended.Outcome = mqttDutyTerminal, "completed"
	err := testF105SubmitInventory(t, e, "txn-claim", testF105DutyMember(t, e, dseq, &ended))
	if err == nil {
		t.Fatalf("a duty was declared complete while its obligation was still live")
	}
	if !strings.Contains(err.Error(), "is not an ending") &&
		!strings.Contains(err.Error(), "stands at phase") {
		t.Fatalf("the refusal did not come from the earned-ending rule: %v", err)
	}

	// A PENDING-DRAIN PHASE IS NOT AN ENDING EITHER, which is the case a coarser rule would
	// have let through: the exchange has reached its protocol conclusion but still holds its
	// identifier and may still emit.
	for i, step := range []mqttExchangePhase{
		mqttExchangePubRecObserved, mqttExchangeTerminalPendingDrain,
	} {
		if _, aerr := e.asm.jsa.opAdvanceExchange(e.id, e.epoch,
			fmt.Sprintf("txn-walk-%d", i), "op-walk", testF105Gen, nonce, step); aerr != nil {
			t.Fatalf("advancing the obligation to phase %d: %v", int(step), aerr)
		}
	}
	d, dseq = testF105LoadDuty(t, e, 1)
	ended = *d
	ended.State, ended.Outcome = mqttDutyTerminal, "completed"
	if err := testF105SubmitInventory(t, e, "txn-drain",
		testF105DutyMember(t, e, dseq, &ended)); err == nil {
		t.Fatalf("a duty ended while its obligation was still draining")
	}

	// THE POSITIVE CONTROL: once the obligation truly ends, the duty may. Advanced the last
	// step directly rather than through testF105FinishExchange, which walks from the start and
	// would be refused for moving backwards from the drain this test parked it in.
	if _, aerr := e.asm.jsa.opAdvanceExchange(e.id, e.epoch, "txn-end-x", "op-end",
		testF105Gen, nonce, mqttExchangeTerminal); aerr != nil {
		t.Fatalf("ending the obligation: %v", aerr)
	}
	d, dseq = testF105LoadDuty(t, e, 1)
	ended = *d
	ended.State, ended.Outcome = mqttDutyTerminal, "completed"
	if err := testF105SubmitInventory(t, e, "txn-done",
		testF105DutyMember(t, e, dseq, &ended)); err != nil {
		t.Fatalf("a duty whose obligation ended could not end: %v", err)
	}
}

// TestF105ADutyIsNeverAllocatedTerminal is the same defect reached from the other side.
//
// If an allocation could start a duty at TERMINAL, a batch would cover a freshly created
// obligation with an ending nobody earned — and rule 3's coverage check would be satisfied by an
// entry that every sweep immediately passes.
func TestF105ADutyIsNeverAllocatedTerminal(t *testing.T) {
	_, err := mqttAllocateInventoryMembers(testF105ID, testF105Epoch, testF105Gen, nil, 0,
		[]mqttDutyDeclaration{{
			Kind: mqttDutyExchange, Ref: "xch-a", State: mqttDutyTerminal,
		}})
	if err == nil {
		t.Fatalf("a duty was allocated already terminal")
	}
	if !strings.Contains(err.Error(), "has not ended") {
		t.Fatalf("the refusal did not come from the allocation rule: %v", err)
	}
	// THE POSITIVE CONTROL, so the rule is about the STATE and not about allocation failing.
	if _, err := mqttAllocateInventoryMembers(testF105ID, testF105Epoch, testF105Gen, nil, 0,
		[]mqttDutyDeclaration{{
			Kind: mqttDutyExchange, Ref: "xch-a", State: mqttDutyPending,
		}}); err != nil {
		t.Fatalf("an ordinary allocation was refused: %v", err)
	}
}

// TestF105CoverageMustNameTheGenerationItCovers is round I-2's first finding.
//
// Coverage used to be keyed only by Kind and Ref. A batch admitting exchange E in one generation
// could therefore carry an OPEN ledger and a consecutive duty naming E in ANOTHER — and every
// other rule passes, because the ledger and the duty agree with each other and the interval is
// exact. The coverage LOOKS present. What actually happens is that the admitting generation has
// no inventory entry for E at all, so no sweep of it will ever resume the obligation, while a
// sweep of the other generation resolves the reference under the wrong one.
func TestF105CoverageMustNameTheGenerationItCovers(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-coverage-gen")
	defer done()
	e.bootstrap(t, "txn-1")

	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	nonce := mqttNewInflightNonce()
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: nonce, PI: 7, QoS: 2,
		Phase: mqttExchangePublish, AdmitTxn: "txn-elsewhere", Tenure: al.rec.Tenure,
	}
	xbody, err := json.Marshal(x)
	if err != nil {
		t.Fatalf("marshalling the exchange: %v", err)
	}

	// The ledger and the duty are internally consistent — with EACH OTHER — but they belong to
	// a different generation than the obligation being admitted.
	other := testF105Gen2
	led := &mqttGenLedger{
		ID: e.id, Epoch: e.epoch, Gen: other, State: mqttGenLedgerOpen, Hi: 1,
	}
	lbody, lerr := json.Marshal(led)
	if lerr != nil {
		t.Fatalf("marshalling the ledger: %v", lerr)
	}
	duty := &mqttDutyEntry{
		ID: e.id, Epoch: e.epoch, Gen: other, Ord: 1,
		Kind: mqttDutyExchange, Ref: nonce, State: mqttDutyPending,
	}
	dbody, derr := json.Marshal(duty)
	if derr != nil {
		t.Fatalf("marshalling the duty: %v", derr)
	}

	if err := testF105SubmitInventoryAs(t, e, mqttTransitionAdmitExchange, "txn-elsewhere",
		mqttTransitionMember{
			subj: mqttExchangeSubject(getHash(e.id), al.rec.Gen, nonce), expect: 0, body: xbody,
		},
		mqttTransitionMember{
			subj: mqttGenLedgerSubject(getHash(e.id), other), expect: 0, body: lbody,
		},
		mqttTransitionMember{
			subj: mqttDutySubject(getHash(e.id), other, 1), expect: 0, body: dbody,
		},
	); err == nil {
		t.Fatalf("an obligation was covered by a duty entry filed under another generation")
	} else if !strings.Contains(err.Error(), "no duty entry naming it THERE") {
		t.Fatalf("the refusal did not come from the coverage rule: %v", err)
	}
}

// TestF105AnAllocationNeverStartsTerminal is the write boundary's half of a rule that existed
// only in the composer.
//
// The composer refuses to allocate a duty already terminal, with the right reason: the thing it
// is predeclared for is created by the same batch and cannot have ended. But a composer is not a
// boundary. A member assembled any other way reaches the store without meeting it, and the
// continuation rule that would have caught an unearned ending cannot see this one, because it
// runs only where a duty is being CONTINUED. The result is an entry the bounded sweep advances
// past forever while the obligation it names is still owed — the same defect as asserting an
// ending later, reached by allocating it instead.
func TestF105AnAllocationNeverStartsTerminal(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-alloc-terminal")
	defer done()
	e.bootstrap(t, "txn-1")

	err := testF105SubmitInventory(t, e, "txn-alloc-terminal",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyTerminal)),
	)
	if err == nil {
		t.Fatalf("a batch allocated a duty that had already ended")
	}
	if !strings.Contains(err.Error(), "already terminal") {
		t.Fatalf("the refusal did not come from the allocation's state: %v", err)
	}

	// AND NOTHING WAS WRITTEN. A refusal that had already taken the ordinal would leave the
	// inventory carrying the very entry the rule exists to keep out.
	if d, _ := testF105LoadDutyIfAny(t, e, 1); d != nil {
		t.Fatalf("the refused allocation left duty 1 standing: %+v", d)
	}
	led, _, lerr := e.asm.jsa.loadGenLedger(e.id, e.epoch, testF105Gen)
	if lerr != nil {
		t.Fatalf("reading the ledger after the refusal: %v", lerr)
	}
	if led != nil {
		t.Fatalf("the refused allocation created a ledger at high-water %d", led.Hi)
	}

	// AND THE SAME BATCH WITHOUT THE ENDING IS ADMITTED, which is what makes the arm above a
	// statement about the STATE rather than about inventory members in general.
	if oerr := testF105SubmitInventory(t, e, "txn-alloc-pending",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)),
	); oerr != nil {
		t.Fatalf("the same allocation with a live duty was refused: %v", oerr)
	}
}

// TestF105AClosedGenerationTakesNoFurtherOrdinal covers the half of the seal the seal cannot
// cover itself.
//
// A sealed ledger refuses further allocation, but it only speaks for a generation that HAS one.
// A generation closed before it ever allocated anything has none — so a later disposal, which is
// deliberately permitted to name an earlier generation, could create its ledger create-only and
// take an ordinal in a generation whose permanent closure says it admits nothing.
func TestF105AClosedGenerationTakesNoFurtherOrdinal(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-closed-no-ordinal")
	defer done()
	al, _ := e.bootstrap(t, "txn-1")

	// CLOSE IT CARRYING NOTHING, so the generation ends with no ledger at all.
	if _, err := e.asm.jsa.opCloseGeneration(e.id, e.epoch, "txn-close",
		al.Gen, al.Tenure, al.OwnerTxn); err != nil {
		t.Fatalf("closing the generation: %v", err)
	}
	// AND A SUCCESSOR SERVES, which is the only way the late batch is reachable at all: a
	// disposal naming an earlier generation runs under the CURRENT authority, and that is the
	// permission this rule has to survive.
	cl := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	e.claim(t, cl.rec, cl.seq, "txn-2", testF105Gen2, "tenure-2")

	err := testF105SubmitInventory(t, e, "txn-late-alloc",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyCleanup, "inc-late",
			mqttDutyNotYetActionable)),
	)
	if err == nil {
		t.Fatalf("a batch allocated an ordinal in a permanently closed generation")
	}
	if !strings.Contains(err.Error(), "closed permanently") {
		t.Fatalf("the refusal did not come from the closure: %v", err)
	}
	// THE CLOSURE'S OWN SEALED LEDGER IS STILL THERE AND STILL SEALED. Every closure seals its
	// inventory, so the question is no longer whether a ledger exists — it is whether the refused
	// allocation MOVED one, which is what would have taken an ordinal in a generation whose
	// permanent closure says it admits nothing.
	led, _, lerr := e.asm.jsa.loadGenLedger(e.id, e.epoch, testF105Gen)
	if lerr != nil || led == nil {
		t.Fatalf("the closed generation's sealed ledger is gone: %+v (%v)", led, lerr)
	}
	if led.State != mqttGenLedgerSealed || led.Hi != 0 || led.SealedHi != 0 {
		t.Fatalf("the refused allocation moved the ledger of a closed generation: %+v", led)
	}
}
