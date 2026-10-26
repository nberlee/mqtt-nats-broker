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

//go:build mqtt_fulltests

package mqtt

import (
	"strings"
	"testing"
)

// TestF105ARetainedSourceMayNotFabricateADelivery is the defect the source union removes.
//
// Today the retained path fabricates a `Dur` and a `Seq` so it fits the shape an ordinary
// delivery uses. Those values then flow into every reader that believes it is looking at a
// delivery: a source-completion classifier asks the consumer about a sequence that was never
// delivered, and gets an answer about something else entirely.
//
// A fabricated value is WORSE than a missing one, because a missing one refuses and a fabricated
// one is believed. So the union forbids them rather than leaving them empty by convention — a
// convention is something a future writer breaks without anything noticing.
func TestF105ARetainedSourceMayNotFabricateADelivery(t *testing.T) {
	ordinary := &mqttObligationSource{
		Kind: mqttSourceOrdinary, Stream: "$MQTT_msgs", Inc: "inc-src", Dur: "$MQTT_src", Seq: 7,
	}
	retained := &mqttObligationSource{
		Kind: mqttSourceRetained, Payload: "pay-1", Digest: "d",
	}
	// THE POSITIVE CONTROLS, one per arm, without which every refusal below would be satisfied
	// by a validator that refused everything.
	if err := mqttValidateObligationSource(ordinary); err != nil {
		t.Fatalf("an ordinary source was refused: %v", err)
	}
	if err := mqttValidateObligationSource(retained); err != nil {
		t.Fatalf("a retained source was refused: %v", err)
	}
	// Which source is admitted against a manifest is the write boundary's rule
	// (checkRetainedAdmission), pinned by TestF105RetainedAdmissionBoundaryRefusals.

	for name, tc := range map[string]struct {
		s    *mqttObligationSource
		want string
	}{
		// EACH OF THE THREE FABRICATIONS SEPARATELY, because a rule that checked only one would
		// pass a record carrying the other two.
		"a retained source with a fabricated durable": {
			&mqttObligationSource{Kind: mqttSourceRetained, Payload: "pay-1", Digest: "d",
				Dur: "$MQTT_src"}, "fabricating one makes every reader"},
		"a retained source with a fabricated sequence": {
			&mqttObligationSource{Kind: mqttSourceRetained, Payload: "pay-1", Digest: "d",
				Seq: 7}, "fabricating one makes every reader"},
		"a retained source with a fabricated stream": {
			&mqttObligationSource{Kind: mqttSourceRetained, Payload: "pay-1", Digest: "d",
				Stream: "$MQTT_msgs"}, "fabricating one makes every reader"},

		// AND THE OTHER DIRECTION: an ordinary source may not carry retained material either.
		"an ordinary source carrying a payload": {
			&mqttObligationSource{Kind: mqttSourceOrdinary, Stream: "s", Inc: "i", Dur: "d",
				Seq: 1, Payload: "pay-1"}, "material comes from one place"},

		// A CONSUMER NAME CAN BE RECREATED. Without the incarnation, a recreated source with
		// the same coordinates is indistinguishable from the original — the same mistake as
		// treating a subject as an identity, which this project has made before.
		"an ordinary source with no incarnation": {
			&mqttObligationSource{Kind: mqttSourceOrdinary, Stream: "s", Dur: "d", Seq: 1},
			"a consumer name can be recreated"},
		"a retained source with a fabricated incarnation": {
			&mqttObligationSource{Kind: mqttSourceRetained, Payload: "pay-1", Digest: "d",
				Inc: "inc-src"}, "fabricating one makes every reader"},

		"a retained source with no payload": {
			&mqttObligationSource{Kind: mqttSourceRetained, Digest: "d"}, "material identity"},
		"a retained source with no digest": {
			&mqttObligationSource{Kind: mqttSourceRetained, Payload: "pay-1"}, "no digest"},
		"an ordinary source with no durable": {
			&mqttObligationSource{Kind: mqttSourceOrdinary, Stream: "s", Inc: "i", Seq: 1},
			"comes from a consumer on a stream"},
		"an ordinary source at sequence zero": {
			&mqttObligationSource{Kind: mqttSourceOrdinary, Stream: "s", Inc: "i", Dur: "d"},
			"unset value and not a delivery"},
		"a source with no kind": {&mqttObligationSource{}, "unsupported kind"},
	} {
		err := mqttValidateObligationSource(tc.s)
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s was refused for the wrong reason: %v", name, err)
		}
	}
}

// TestF105ACanonicalReferenceIsAKindNotAValue pins the second union.
//
// An ordinary exchange's release lives on $MQTT_out at a sequence; a retained one has no such
// consumer, so its canonical record lives at a unique exchange-specific subject on
// $MQTT_inflight. Those are different KINDS of reference, and the identity beside the in-flight
// subject is what makes the difference safe: a subject locates an ADDRESS, and this project has
// been bitten before by treating an address as an identity.
func TestF105ACanonicalReferenceIsAKindNotAValue(t *testing.T) {
	if err := mqttValidateCanonicalRef(&mqttCanonicalRef{
		Kind: mqttCanonicalConsumer, Consumer: "$MQTT_pubrel", Seq: 3,
	}); err != nil {
		t.Fatalf("a consumer reference was refused: %v", err)
	}
	if err := mqttValidateCanonicalRef(&mqttCanonicalRef{
		Kind: mqttCanonicalInflight, Subject: "$MQTT.inflight.f105.rel.x", Identity: "rel-1",
	}); err != nil {
		t.Fatalf("an in-flight reference was refused: %v", err)
	}

	for name, r := range map[string]*mqttCanonicalRef{
		// A SUBJECT WITHOUT AN IDENTITY locates an address. A record read back from a rewritten
		// subject is a different record, and only the identity says so.
		"an in-flight reference with no identity": {
			Kind: mqttCanonicalInflight, Subject: "$MQTT.inflight.f105.rel.x"},
		"an in-flight reference with no subject": {
			Kind: mqttCanonicalInflight, Identity: "rel-1"},
		"an in-flight reference carrying consumer coordinates": {
			Kind: mqttCanonicalInflight, Subject: "s", Identity: "rel-1",
			Consumer: "$MQTT_pubrel", Seq: 3},
		"a consumer reference carrying an in-flight subject": {
			Kind: mqttCanonicalConsumer, Consumer: "c", Seq: 1, Subject: "s"},
		"a consumer reference at sequence zero": {
			Kind: mqttCanonicalConsumer, Consumer: "c"},
		"a reference with no kind": {},
	} {
		if err := mqttValidateCanonicalRef(r); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}
