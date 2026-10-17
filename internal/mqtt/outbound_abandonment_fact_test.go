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
	"fmt"
	"strings"
	"testing"
)

// F-105 row 5, unit C1: the durable abandonment fact on the exchange (plan-unitC rev 3 §3).

const testFactDur, testFactSeq = "SRC", uint64(41)

func testFactExchange(phase mqttExchangePhase, qos byte, fact *mqttExchangeAbandonment) *mqttOutboundExchange {
	return &mqttOutboundExchange{
		ID: "client", Epoch: "epoch", Gen: "gen", Nonce: "ybCTyzmflUIkM0L4h8ABwI", PI: 7, QoS: qos,
		Phase: phase, AdmitTxn: "ybCTyzmflUIkM0L4h8AGMo", Tenure: "ybCTyzmflUIkM0L4h8AKdm", Abandoned: fact,
	}
}

func testFact() *mqttExchangeAbandonment {
	return &mqttExchangeAbandonment{
		Reason: mqttAbandonReasonACLDeny, Tenure: "ybCTyzmflUIkM0L4h8AOzW", Alloc: "ybCTyzmflUIkM0L4h8ATGU",
		Acq: 3, Dur: testFactDur, Seq: testFactSeq, Reply: testInflightAckSubject(testFactDur, testFactSeq, 2),
	}
}

func testFactDecode(t *testing.T, x *mqttOutboundExchange) error {
	t.Helper()
	b, err := json.Marshal(x)
	if err != nil {
		t.Fatal(err)
	}
	_, derr := mqttDecodeOutboundExchange(b, x.ID, x.Epoch, x.Gen, x.Nonce)
	return derr
}

// T-C1 (substrate). The fact's closed schema and the phases that may carry it.
//
// Mutation M-C1: DeliveryAbandoned accepted without a fact -> the "abandoned without fact" leg.
// Mutation M-C26: the reply bound by sequence alone -> the wrong-stream and wrong-consumer legs.
func TestUnitCAbandonmentFactShape(t *testing.T) {
	t.Parallel()
	for _, p := range []mqttExchangePhase{mqttExchangeDeliveryAbandoned, mqttExchangePubRecObserved,
		mqttExchangeTerminalPendingDrain, mqttExchangeTerminal, mqttExchangeRejected} {
		if err := testFactDecode(t, testFactExchange(p, 2, testFact())); err != nil {
			t.Errorf("phase %d with a valid fact refused: %v", p, err)
		}
	}
	if err := testFactDecode(t, testFactExchange(mqttExchangeTerminal, 1, testFact())); err != nil {
		t.Errorf("a QoS 1 terminal with a valid fact refused: %v", err)
	}
	// Without a fact: an ordinary exchange decodes and carries none.
	if err := testFactDecode(t, testFactExchange(mqttExchangePublish, 1, nil)); err != nil {
		t.Errorf("an ordinary exchange refused: %v", err)
	}
	v2 := testFact()
	v2.Reply = fmt.Sprintf("$JS.ACK.dom.acchash.%s.%s.2.%d.%d.0.0.rnd", mqttStreamName, testFactDur, testFactSeq, testFactSeq)
	if err := testFactDecode(t, testFactExchange(mqttExchangeDeliveryAbandoned, 1, v2)); err != nil {
		t.Errorf("a v2 acknowledgement subject refused: %v", err)
	}

	refused := []struct {
		name string
		x    *mqttOutboundExchange
		want string
	}{
		{"abandoned without fact", testFactExchange(mqttExchangeDeliveryAbandoned, 1, nil), "without its abandonment fact"},
		{"fact on a fresh obligation", testFactExchange(mqttExchangePublish, 1, testFact()), "which no abandonment reaches"},
		{"fact on a withdrawal", testFactExchange(mqttExchangeWithdrawnUnsent, 1, testFact()), "which no abandonment reaches"},
		{"fact on a supersession", testFactExchange(mqttExchangeSupersededUnsent, 1, testFact()), "which no abandonment reaches"},
	}
	for _, mut := range []struct {
		name string
		f    func(*mqttExchangeAbandonment)
		want string
	}{
		{"unknown reason", func(f *mqttExchangeAbandonment) { f.Reason = "whim" }, "does not abandon for"},
		{"malformed tenure", func(f *mqttExchangeAbandonment) { f.Tenure = "" }, "malformed tenure"},
		{"malformed allocation", func(f *mqttExchangeAbandonment) { f.Alloc = "" }, "malformed allocation"},
		// The ordinary arm's source members are omitted when zero since F-105 7c (the retained arm
		// carries none), so a zero source is an ABSENT member: the closed schema refuses it before
		// the semantic check, naming the member.
		{"no source", func(f *mqttExchangeAbandonment) { f.Seq = 0 }, "member seq is mandatory and absent"},
		{"wrong stream", func(f *mqttExchangeAbandonment) {
			f.Reply = strings.Replace(f.Reply, mqttStreamName, "OTHER", 1)
		}, "is not an acknowledgement of"},
		{"wrong consumer", func(f *mqttExchangeAbandonment) {
			f.Reply = testInflightAckSubject("ELSE", testFactSeq, 2)
		}, "is not an acknowledgement of"},
		{"wrong sequence", func(f *mqttExchangeAbandonment) {
			f.Reply = testInflightAckSubject(testFactDur, testFactSeq+1, 2)
		}, "is not an acknowledgement of"},
		{"no reply", func(f *mqttExchangeAbandonment) { f.Reply = "" }, "member reply is mandatory and absent"},
	} {
		f := testFact()
		mut.f(f)
		refused = append(refused, struct {
			name string
			x    *mqttOutboundExchange
			want string
		}{mut.name, testFactExchange(mqttExchangeDeliveryAbandoned, 1, f), mut.want})
	}
	for _, tc := range refused {
		if err := testFactDecode(t, tc.x); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: decoded as %v, want a refusal naming %q", tc.name, err, tc.want)
		}
	}

	// Raw shapes the typed encoder cannot produce: null, partial, and an unknown member.
	good, _ := json.Marshal(testFactExchange(mqttExchangeDeliveryAbandoned, 1, testFact()))
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(good, &raw); err != nil {
		t.Fatal(err)
	}
	var fact map[string]json.RawMessage
	if err := json.Unmarshal(raw["abandoned"], &fact); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(map[string]json.RawMessage, map[string]json.RawMessage)
		want string
	}{
		{"null fact", func(r, _ map[string]json.RawMessage) { r["abandoned"] = json.RawMessage("null") }, "a JSON object"},
		{"partial fact", func(_, f map[string]json.RawMessage) { delete(f, "reply") }, "mandatory and absent"},
		{"unknown fact member", func(_, f map[string]json.RawMessage) { f["why"] = json.RawMessage(`"x"`) }, "not a field this broker writes"},
		{"null fact member", func(_, f map[string]json.RawMessage) { f["acq"] = json.RawMessage("null") }, "mandatory and null"},
	} {
		r := map[string]json.RawMessage{}
		for k, v := range raw {
			r[k] = v
		}
		f := map[string]json.RawMessage{}
		for k, v := range fact {
			f[k] = v
		}
		tc.edit(r, f)
		if _, ok := r["abandoned"]; ok && string(r["abandoned"]) != "null" {
			fb, _ := json.Marshal(f)
			r["abandoned"] = fb
		}
		b, _ := json.Marshal(r)
		if _, err := mqttDecodeOutboundExchange(b, "client", "epoch", "gen", "ybCTyzmflUIkM0L4h8ABwI"); err == nil ||
			!strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: decoded as %v, want a refusal naming %q", tc.name, err, tc.want)
		}
	}
}

// T-C1 (substrate). Durable records compare by value, the fact included.
func TestUnitCExchangeEqualityIsByValue(t *testing.T) {
	t.Parallel()
	a, b := testFactExchange(mqttExchangeTerminal, 1, testFact()), testFactExchange(mqttExchangeTerminal, 1, testFact())
	if a.Abandoned == b.Abandoned {
		t.Fatal("fixture: the two facts must be distinct allocations")
	}
	if !mqttExchangeEqual(a, b) {
		t.Fatal("two reads of one record compared unequal")
	}
	b.Abandoned.Acq++
	if mqttExchangeEqual(a, b) {
		t.Fatal("a changed fact compared equal")
	}
	if mqttExchangeEqual(a, testFactExchange(mqttExchangeTerminal, 1, nil)) {
		t.Fatal("a removed fact compared equal")
	}
}

// T-C2 (substrate). The charge follows the FACT, the identifier the ending: an abandoned
// exchange whose phase a client acknowledgement advanced stays charge-released in every phase a
// census may read, and holds its identifier until the exchange ends.
//
// Mutation M-C2: charge release read from the DeliveryAbandoned phase alone -> the
// PubRecObserved and pending-drain legs recharge.
func TestUnitCAbandonmentChargeFollowsTheFact(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		phase  mqttExchangePhase
		qos    byte
		fact   bool
		charge mqttChargeDisposition
		disp   mqttCustodyDisp
	}{
		{mqttExchangeDeliveryAbandoned, 2, true, mqttChargeReleasable, mqttCustodyAbandoned},
		{mqttExchangePubRecObserved, 2, true, mqttChargeReleasable, mqttCustodyAbandoned},
		{mqttExchangeTerminalPendingDrain, 2, true, mqttChargeReleasable, mqttCustodyAbandoned},
		{mqttExchangeTerminal, 2, true, mqttChargeReleasable, mqttCustodyEnded},
		{mqttExchangeRejected, 2, true, mqttChargeReleasable, mqttCustodyEnded},
		// Controls without the fact: the phase decides as before.
		{mqttExchangePubRecObserved, 2, false, mqttChargeHeld, mqttCustodyHeld},
		{mqttExchangeTerminalPendingDrain, 2, false, mqttChargeHeldPendingDrain, mqttCustodyHeld},
		{mqttExchangeTerminal, 1, false, mqttChargeReleasable, mqttCustodyEnded},
	} {
		var f *mqttExchangeAbandonment
		if tc.fact {
			f = testFact()
		}
		x := testFactExchange(tc.phase, tc.qos, f)
		// The fact legs pass OUTSTANDING output: the fact releases the charge even while a PUBREL
		// action may still drain. The controls pass none, so their answer is the phase's own.
		c, err := mqttExchangeCharge(x, tc.fact)
		if err != nil || c != tc.charge {
			t.Errorf("phase %d fact %v: charge %v (%v), want %v", tc.phase, tc.fact, c, err, tc.charge)
		}
		if d, _ := mqttCustodyDisposition(x); d != tc.disp {
			t.Errorf("phase %d fact %v: custody %v, want %v", tc.phase, tc.fact, d, tc.disp)
		}
	}
	// A cold ledger adopting the census read of an abandoned exchange past PUBREC holds its
	// identifier and never charges it.
	l := newMQTTCustodyLedger("gen")
	x := testFactExchange(mqttExchangePubRecObserved, 2, testFact())
	if d := l.adoptReadExchange(x); d != mqttCustodyAbandoned {
		t.Fatalf("adopted as %v", d)
	}
	if !l.holdsPI(x.PI) || l.chargesPI(x.PI) {
		t.Fatalf("a cold ledger holds %v charges %v for an abandoned exchange past PUBREC", l.holdsPI(x.PI), l.chargesPI(x.PI))
	}
}
