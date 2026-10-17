//go:build mqttport_fulltests

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

import "testing"

// Which obligation an acknowledgement answers (F-105, row 3).
//
// The two refuted readings are each given a case, because both were plausible enough to be
// written down and one of them was in this plan for two review rounds.

func TestF105AckBindingIsCapturedNotLookedUp(t *testing.T) {
	var b mqttAckBindings
	first := mqttAckBinding{Gen: testF105Gen, Nonce: "E1", PI: 7, Acq: 1, QoS: 2}
	testF105Capture(t, &b, first)

	// THE POSITIVE CONTROL: an acknowledgement for 7 answers E1.
	got, err := mqttResolveAck(&b, 7, testF105PubRec())
	if err != nil || !got.Resolved || got.Binding.Nonce != "E1" {
		t.Fatalf("the captured binding did not resolve: %+v %v", got, err)
	}
	if got.Phase != mqttExchangePubRecObserved {
		t.Fatalf("a successful PUBREC derived phase %d", got.Phase)
	}

	// THE REUSE CASE, which is the whole reason this is a capture. A handler took the
	// resolution above, then the identifier moved to E2 — as a proven-unsent replacement
	// legitimately does. The handler's captured identity must be UNAFFECTED, because a lookup
	// taken now would select E2 and complete the wrong obligation.
	testF105Capture(t, &b, mqttAckBinding{Gen: testF105Gen, Nonce: "E2", PI: 7, Acq: 1, QoS: 2})
	if got.Binding.Nonce != "E1" {
		t.Fatalf("a captured resolution followed a later allocation to %q", got.Binding.Nonce)
	}
	// A NEW acknowledgement arriving after the move answers E2, which is equally required:
	// the capture is per dispatch, not a freeze of the identifier forever.
	next, err := mqttResolveAck(&b, 7, testF105PubRec())
	if err != nil || next.Binding.Nonce != "E2" {
		t.Fatalf("an acknowledgement after the move did not answer the new obligation: %+v", next)
	}
}

func TestF105AckBindingRefusesWhatItCannotAttribute(t *testing.T) {
	var b mqttAckBindings
	testF105Capture(t, &b, mqttAckBinding{Gen: testF105Gen, Nonce: "E1", PI: 7, Acq: 1, QoS: 2})

	// An identifier this connection never told the peer about resolves to NOTHING. A refusal,
	// never a licence to pick the nearest obligation: an acknowledgement nobody can attribute
	// must not complete one chosen by proximity.
	if got, err := mqttResolveAck(&b, 9, testF105PubRec()); err != nil || got.Resolved {
		t.Fatalf("an unattributable acknowledgement resolved to an obligation: %+v %v", got, err)
	}
	if _, err := mqttResolveAck(nil, 7, testF105PubRec()); err == nil {
		t.Fatalf("an acknowledgement resolved with no bindings at all")
	}
}

func TestF105AckBindingSurvivesAResume(t *testing.T) {
	// A RESUMED CLIENT ACKNOWLEDGING ON A NEW CONNECTION IS LEGITIMATE MQTT. Gating on the
	// action's original connection would break QoS 1/2 resumption outright, which is why the
	// new connection ADOPTS bindings for the exchanges that survived rather than refusing
	// anything it did not itself dispatch.
	var old mqttAckBindings
	testF105Capture(t, &old, mqttAckBinding{Gen: testF105Gen, Nonce: "E1", PI: 7, Acq: 1, QoS: 2})

	var resumed mqttAckBindings
	if got, err := mqttResolveAck(&resumed, 7, testF105PubComp()); err != nil || got.Resolved {
		t.Fatalf("a fresh connection resolved an identifier it never dispatched: %+v %v", got, err)
	}
	survived, _ := old.resolve(7)
	resumed.adopt([]mqttAckBinding{survived})

	got, err := mqttResolveAck(&resumed, 7, testF105PubComp())
	if err != nil || !got.Resolved || got.Binding.Nonce != "E1" {
		t.Fatalf("the resumed connection could not answer for a surviving exchange: %+v %v", got, err)
	}
}

func TestF105AckBindingIsReleasedOnlyByItsOwnObligation(t *testing.T) {
	var b mqttAckBindings
	testF105Capture(t, &b, mqttAckBinding{Gen: testF105Gen, Nonce: "E1", PI: 7, Acq: 1, QoS: 2})

	// A release naming a DIFFERENT obligation leaves it alone. Otherwise a terminal outcome
	// for a successor would drop the binding a predecessor's retransmission still needs.
	b.release(7, "E2")
	if _, ok := b.resolve(7); !ok {
		t.Fatalf("a foreign obligation's release dropped this binding")
	}
	b.release(7, "E1")
	if _, ok := b.resolve(7); ok {
		t.Fatalf("the obligation's own release did not drop its binding")
	}
}

// TestF105AckBindingSuppliesTheReleasePrecondition is where row 3 meets the substrate's custody
// rules: mqttIdentifierReleaseAllowed requires bindingsDischarged, and this is what answers it.
//
// The pairing matters because the two halves fail differently. A releasable CHARGE says the
// obligation ended; an undischarged BINDING says a captured handler can still name it on its
// original connection. Releasing the identifier on the first alone lets a late acknowledgement
// complete an obligation that no longer holds it.
func TestF105AckBindingSuppliesTheReleasePrecondition(t *testing.T) {
	var b mqttAckBindings
	testF105Capture(t, &b, mqttAckBinding{Gen: testF105Gen, Nonce: "E1", PI: 7, Acq: 1, QoS: 2})

	// Terminal AND drained, so the charge itself is releasable — the strongest evidence a
	// completion can carry.
	ev := mqttReleaseEvidence{Terminal: true, Drained: true}
	if ok, why := mqttIdentifierReleaseAllowed(ev, b.dischargedFor(testF105Gen, "E1")); ok {
		t.Fatalf("the identifier was released while a captured binding still named its exchange: %s", why)
	}

	// An unrelated exchange's binding does not hold THIS one's identifier.
	if !b.dischargedFor(testF105Gen, "E-other") {
		t.Fatalf("an unrelated exchange read as undischarged")
	}

	// Once the obligation's own binding is released, both halves are satisfied.
	b.release(7, "E1")
	if ok, why := mqttIdentifierReleaseAllowed(ev, b.dischargedFor(testF105Gen, "E1")); !ok {
		t.Fatalf("a terminal, drained, fully discharged obligation was refused release: %s", why)
	}
}

// testF105PubRec and testF105PubComp are the arrivals these fixtures used to express as a
// chosen phase. Written as PACKETS because that is what a dispatch site has.
func testF105PubRec() mqttAckArrival {
	return mqttAckArrival{Packet: mqttPacketPubRec, Reason: mqttReasonSuccess}
}

func testF105PubComp() mqttAckArrival {
	return mqttAckArrival{Packet: mqttPacketPubComp, Reason: mqttReasonSuccess}
}

// testF105Capture captures a binding and fails the test if the binding is one this connection
// could not have dispatched.
func testF105Capture(t *testing.T, b *mqttAckBindings, bind mqttAckBinding) {
	t.Helper()
	if err := b.capture(bind); err != nil {
		t.Fatalf("capturing identifier %d for %q: %v", bind.PI, bind.Nonce, err)
	}
}
