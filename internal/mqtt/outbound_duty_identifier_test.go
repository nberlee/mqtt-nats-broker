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
	"sync"
	"testing"
)

// An exchange duty names the packet identifier its exchange holds. Identifier exclusivity reads
// only the exchanges whose duties name a claimed identifier, so the identifier on the duty is
// load-bearing: these tests pin that it is required, bound to the exchange where both are
// created, never moved by a continuation or a relocation, and that the walk reads what it must
// and nothing else.

// TestDutyEntryIdentifierShape: an exchange duty names an identifier, in every state; no other
// kind names one.
func TestDutyEntryIdentifierShape(t *testing.T) {
	base := func(kind mqttDutyKind, pi uint16, state mqttDutyState) *mqttDutyEntry {
		d := &mqttDutyEntry{ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Ord: 1,
			Kind: kind, Ref: "ref-a", PI: pi, State: state}
		switch state {
		case mqttDutyTerminal:
			d.Outcome = "completed"
		case mqttDutyRelocated:
			d.To = 2
		}
		return d
	}
	for name, tc := range map[string]struct {
		d  *mqttDutyEntry
		ok bool
	}{
		"a pending exchange duty naming its identifier":  {base(mqttDutyExchange, 7, mqttDutyPending), true},
		"a terminal exchange duty naming its identifier": {base(mqttDutyExchange, 7, mqttDutyTerminal), true},
		"a relocation marker naming its identifier":      {base(mqttDutyExchange, 7, mqttDutyRelocated), true},
		"an exchange duty naming no identifier":          {base(mqttDutyExchange, 0, mqttDutyPending), false},
		"a relocation marker naming no identifier":       {base(mqttDutyExchange, 0, mqttDutyRelocated), false},
		"a provision duty naming an identifier":          {base(mqttDutyProvision, 7, mqttDutyPending), false},
		"a cleanup duty naming an identifier":            {base(mqttDutyCleanup, 7, mqttDutyNotYetActionable), false},
		"a provision duty naming none":                   {base(mqttDutyProvision, 0, mqttDutyPending), true},
	} {
		err := mqttValidateDutyEntry(tc.d, testF105ID, testF105Epoch, testF105Gen, 1)
		if tc.ok && err != nil {
			t.Fatalf("%s was refused: %v", name, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
	// AND THE STORED SHAPE: a body without the member decodes as unsupported, never as "any
	// identifier".
	body, _ := json.Marshal(base(mqttDutyExchange, 0, mqttDutyPending))
	if _, err := mqttDecodeDutyEntry(body, testF105ID, testF105Epoch, testF105Gen, 1); err == nil {
		t.Fatalf("an exchange duty stored without its identifier decoded")
	}
}

// TestAllocationBindsDutyIdentifierToExchange: the boundary binds an allocated exchange duty to
// the exchange created beside it, identifier included.
func TestAllocationBindsDutyIdentifierToExchange(t *testing.T) {
	e, done := testF105NewEnv(t, "duty-identifier-binding")
	defer done()
	jsa := &e.asm.jsa
	e.bootstrap(t, "txn-1")

	compose := func(txn string, pi uint16) (*mqttAuthorityTransition, *mqttOutboundExchange) {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
			PI: pi, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		tr, err := testF105AdmitExchangeTr(t, e, al.rec, al.seq, txn, "op-"+txn, x)
		if err != nil {
			t.Fatalf("composing the admission: %v", err)
		}
		return tr, x
	}

	// A DUTY NAMING ANOTHER IDENTIFIER than its exchange holds.
	tr, _ := compose("txn-wrong-pi", 7)
	rewrote := false
	for i, m := range tr.Extra {
		if !mqttIsDutySubject(m.subj) || m.expect != 0 {
			continue
		}
		var d mqttDutyEntry
		if err := json.Unmarshal(m.body, &d); err != nil {
			t.Fatalf("reading the composed duty: %v", err)
		}
		if d.Kind != mqttDutyExchange || d.PI != 7 {
			t.Fatalf("fixture: the composed exchange duty is %+v, want one naming identifier 7", d)
		}
		d.PI = 8
		tr.Extra[i].body, _ = json.Marshal(&d)
		rewrote = true
	}
	if !rewrote {
		t.Fatalf("fixture: the admission composed no exchange duty")
	}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("an exchange duty naming identifier 8 was allocated beside an exchange holding 7")
	} else if !strings.Contains(err.Error(), "which holds 7") {
		t.Fatalf("the refusal did not come from the identifier binding: %v", err)
	}

	// A DUTY NAMING AN EXCHANGE THE BATCH DOES NOT CREATE.
	if err := testF105SubmitInventory(t, e, "txn-orphan-duty",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 1)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-nowhere", mqttDutyPending)),
	); err == nil {
		t.Fatalf("an exchange duty was allocated for an exchange no batch creates")
	} else if !strings.Contains(err.Error(), "does not create") {
		t.Fatalf("the refusal did not come from the identifier binding: %v", err)
	}

	// THE POSITIVE CONTROL: the composed admission, untouched.
	tr, _ = compose("txn-right-pi", 7)
	if _, err := jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("an admission whose duty names its exchange's identifier was refused: %v", err)
	}
}

// TestDutyContinuationKeepsIdentifier: a continuation is the same duty, identifier included.
func TestDutyContinuationKeepsIdentifier(t *testing.T) {
	e, done := testF112Env(t, "duty-identifier-continuation")
	defer done()
	nonce := testF112Admit(t, e, 5)
	ord, found, err := e.asm.jsa.findDutyInInventory(e.id, e.epoch, testF105Gen,
		func(d *mqttDutyEntry) bool { return d.Ref == nonce })
	if err != nil || !found {
		t.Fatalf("finding the admitted duty: %v", err)
	}
	d, dseq := testF105LoadDuty(t, e, ord)
	if d.PI != 5 {
		t.Fatalf("fixture: the admitted duty names identifier %d, want 5", d.PI)
	}
	moved := *d
	moved.PI = 6
	if err := testF105SubmitInventory(t, e, "txn-repoint", testF105DutyMember(t, e, dseq, &moved)); err == nil {
		t.Fatalf("a continuation moved an exchange duty from identifier 5 to 6")
	} else if !strings.Contains(err.Error(), "re-identifies duty") {
		t.Fatalf("the refusal did not come from the continuation identity: %v", err)
	}
}

// TestFloorRelocationKeepsIdentifier: a relocation carries the identifier unchanged, on the
// marker and on the representative.
func TestFloorRelocationKeepsIdentifier(t *testing.T) {
	e, done := testF112Env(t, "duty-identifier-relocation")
	defer done()
	jsa := &e.asm.jsa

	testF112Admit(t, e, 1) // outstanding: its position is relocated, not reclaimed
	led, ledSeq := testF112Ledger(t, e)
	d, seq, err := jsa.loadDutyEntryAt(e.id, e.epoch, testF105Gen, 1)
	if err != nil || d == nil {
		t.Fatalf("reading position 1: %v", err)
	}
	f := &mqttFloorAdvance{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ledger: led, LedgerSeq: ledSeq,
		Positions: []mqttFloorPosition{{Ord: 1, Entry: d, Seq: seq, Class: mqttFloorRelocate}}}

	tamper := func(marker bool) []mqttTransitionMember {
		members, merr := mqttComposeFloorMembers(f)
		if merr != nil {
			t.Fatalf("composing the relocation: %v", merr)
		}
		hit := false
		for i, m := range members {
			if !mqttIsDutySubject(m.subj) || (m.expect != 0) != marker {
				continue
			}
			var md mqttDutyEntry
			_ = json.Unmarshal(m.body, &md)
			if md.PI != 1 {
				t.Fatalf("fixture: the composed member names identifier %d, want 1", md.PI)
			}
			md.PI = 2
			members[i].body, _ = json.Marshal(&md)
			hit = true
		}
		if !hit {
			t.Fatalf("fixture: no member to tamper (marker=%v)", marker)
		}
		return members
	}
	if err := testF112CommitFloorMembers(t, e, tamper(true)); err == nil ||
		!strings.Contains(err.Error(), "re-identifies position") {
		t.Fatalf("a relocation marker moved the identifier: %v", err)
	}
	if err := testF112CommitFloorMembers(t, e, tamper(false)); err == nil ||
		!strings.Contains(err.Error(), "different state") {
		t.Fatalf("a representative moved the identifier: %v", err)
	}
	// THE POSITIVE CONTROL: the composed relocation, untouched.
	if err := jsa.opAdvanceFloor(f, mqttNewInflightNonce()); err != nil {
		t.Fatalf("a relocation carrying the identifier unchanged was refused: %v", err)
	}
}

// TestIdentifierExclusivityReadsOnlyClaimedIdentifiers: an admission reads the exchanges whose
// duties name the identifier it claims, and none of the others, while a live, an abandoned or an
// unreadable obligation on that identifier still refuses it.
func TestIdentifierExclusivityReadsOnlyClaimedIdentifiers(t *testing.T) {
	e, done := testF112Env(t, "duty-identifier-walk")
	defer done()
	jsa := &e.asm.jsa

	// History on identifiers 20..27, ended but not reclaimed (the floor has not run): every one
	// of them is inside [Lo, Hi], which is what a walk reading every exchange used to read.
	history := map[string]struct{}{}
	for pi := uint16(20); pi < 28; pi++ {
		n := testF112Admit(t, e, pi)
		testF112EndAndResolve(t, e, n)
		history[n] = struct{}{}
	}
	// An ended obligation on the claimed identifier 7: read, and found ended.
	endedSeven := testF112Admit(t, e, 7)
	testF112EndAndResolve(t, e, endedSeven)

	var mu sync.Mutex
	read := map[string]int{}
	obs := func(ev mqttJSARequestEvent) {
		if ev.Kind != mqttJSARequestSent || !strings.Contains(ev.Subject, ".MSG.GET.") {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for n := range history {
			if strings.Contains(string(ev.Body), mqttExchangeSubject(getHash(e.id), testF105Gen, n)) {
				read["history"]++
			}
		}
		if strings.Contains(string(ev.Body), mqttExchangeSubject(getHash(e.id), testF105Gen, endedSeven)) {
			read["seven"]++
		}
	}
	jsa.testObserveRequest.Store(&obs)
	defer jsa.testObserveRequest.Store(nil)

	live := testF112Admit(t, e, 7)
	mu.Lock()
	got := map[string]int{"history": read["history"], "seven": read["seven"]}
	mu.Unlock()
	if got["history"] != 0 {
		t.Fatalf("an admission on identifier 7 read %d exchange(s) whose duties name other identifiers",
			got["history"])
	}
	if got["seven"] == 0 {
		t.Fatalf("an admission on identifier 7 did not read the ended exchange whose duty names 7")
	}

	admit := func(txn string, pi uint16) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
			PI: pi, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		tr, terr := testF105AdmitExchangeTr(t, e, al.rec, al.seq, txn, "op-"+txn, x)
		if terr != nil {
			t.Fatalf("composing the admission at %d: %v", pi, terr)
		}
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}
	// A LIVE OBLIGATION ON THE CLAIMED IDENTIFIER STILL REFUSES.
	if err := admit("txn-second-seven", 7); err == nil {
		t.Fatalf("a second obligation was admitted on identifier 7 while %q holds it", live)
	} else if !strings.Contains(err.Error(), "still holds at phase") {
		t.Fatalf("the refusal did not come from the standing obligation: %v", err)
	}

	// AN ABANDONED OBLIGATION KEEPS ITS IDENTIFIER, so its duty is read and refuses.
	// Planted: the abandonment route runs on a live session, and what this pins is only what
	// the walk does with the phase it reads.
	abandoned := testF112Admit(t, e, 9)
	ax, _, aerr := jsa.loadOutboundExchange(e.id, e.epoch, testF105Gen, abandoned)
	if aerr != nil {
		t.Fatalf("reading the obligation on 9: %v", aerr)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	ax.Phase = mqttExchangeDeliveryAbandoned
	ax.Abandoned = &mqttExchangeAbandonment{Reason: mqttAbandonReasonACLDeny, Tenure: al.rec.Tenure,
		Alloc: abandoned, Acq: 1, Dur: "$MQTT_src", Seq: 1, Reply: testInflightAckSubject("$MQTT_src", 1, 1)}
	abody, _ := json.Marshal(ax)
	if _, err := jsa.storeMsg(mqttExchangeSubject(getHash(e.id), testF105Gen, abandoned), -1, abody); err != nil {
		t.Fatalf("planting the abandoned phase: %v", err)
	}
	if err := admit("txn-over-abandoned", 9); err == nil {
		t.Fatalf("identifier 9 was claimed while an abandoned delivery still holds it")
	} else if !strings.Contains(err.Error(), "still holds at phase 10") {
		t.Fatalf("the refusal did not come from the abandoned obligation: %v", err)
	}

	// AN UNREADABLE EXCHANGE WHOSE DUTY NAMES THE CLAIMED IDENTIFIER REFUSES.
	if _, err := jsa.storeMsg(mqttExchangeSubject(getHash(e.id), testF105Gen, live), -1,
		[]byte(`{"id":"nonsense"`)); err != nil {
		t.Fatalf("corrupting the live exchange: %v", err)
	}
	if err := admit("txn-over-unreadable", 7); err == nil {
		t.Fatalf("identifier 7 was claimed while the exchange whose duty names it cannot be read")
	} else if !strings.Contains(err.Error(), "unreadable position") {
		t.Fatalf("the refusal did not come from the unreadable obligation: %v", err)
	}
}
