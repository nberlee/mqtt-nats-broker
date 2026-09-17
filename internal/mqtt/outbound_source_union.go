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

import "fmt"

// Two validated tagged unions (F-105, row 7; design D4).
//
// # Why the source shape has to become one
//
// An ordinary delivery has a source consumer and a sequence. A RETAINED send does not — there is
// no consumer it was delivered from — and today the code that handles both fabricates a `Dur` and
// a `Seq` for the retained case so the shared shape fits. Those fabricated values then flow into
// every reader that thinks it is looking at a delivery: a source-completion classifier asks the
// consumer about a sequence that was never delivered, and gets an answer about something else
// entirely.
//
// A fabricated value is worse than a missing one, because a missing one refuses and a fabricated
// one is believed. So the source becomes a union whose retained arm FORBIDS `Dur` and `Seq`
// rather than leaving them empty by convention — a convention is something a future writer can
// break without anything noticing.
//
// PI, the exchange, QoS, phase and transmission identity stay mandatory for BOTH arms. What
// varies is only where the material comes from.
//
// # And the canonical reference is a union too, for the same reason
//
// An ordinary QoS 2 exchange stores its canonical PUBREL as a consumer and a sequence on
// `$MQTT_out`. A retained one has no such consumer, so its canonical record lives at a unique
// exchange-specific subject on `$MQTT_inflight` — a different KIND of reference, not a different
// value of the same kind. Recovery reads the exchange's canonical reference DIRECTLY; it never
// creates an exchange from an ownerless PUBREL and never consults the current retained slot,
// because the slot may long since have been rewritten.

// mqttSourceKind is the closed set of places an obligation's material comes from.
type mqttSourceKind string

const (
	// mqttSourceOrdinary: delivered from a source consumer at a sequence.
	mqttSourceOrdinary mqttSourceKind = "ordinary"
	// mqttSourceRetained: replayed from durable material identified by a manifest. There is no
	// consumer and no sequence, and inventing them is the defect this union removes.
	mqttSourceRetained mqttSourceKind = "retained"
)

// mqttObligationSource is where one obligation's material comes from.
type mqttObligationSource struct {
	Kind mqttSourceKind `json:"kind"`

	// Stream, Inc, Dur and Seq belong to the ORDINARY arm and are forbidden on the retained one.
	//
	// Inc is the source consumer's INCARNATION, and it is required for the reason this project
	// has learned twice: a consumer NAME can be recreated. Stream, durable and sequence together
	// are coordinates that a recreated source reuses, so without the incarnation a completion or
	// replay reader cannot tell the original source from a successor that happens to occupy the
	// same address — which is the same mistake as treating a subject as an identity.
	Stream string `json:"stream,omitempty"`
	Inc    string `json:"inc,omitempty"`
	Dur    string `json:"dur,omitempty"`
	Seq    uint64 `json:"seq,omitempty"`

	// Payload and Digest belong to the RETAINED arm and are forbidden on the ordinary one.
	Payload string `json:"payload,omitempty"`
	Digest  string `json:"digest,omitempty"`
}

// mqttValidateObligationSource refuses a source this broker cannot act on.
//
// Both directions are checked. Requiring the arm's own members catches a source that says nothing;
// FORBIDDING the other arm's members is what catches the fabricated value — a retained source
// carrying a Dur is exactly the shape that makes a completion classifier ask a consumer about a
// delivery that never happened.
func mqttValidateObligationSource(s *mqttObligationSource) error {
	if s == nil {
		return fmt.Errorf("obligation source is absent")
	}
	switch s.Kind {
	case mqttSourceOrdinary:
		if s.Stream == _EMPTY_ || s.Dur == _EMPTY_ {
			return fmt.Errorf("an ordinary source names stream %q and durable %q: a delivery "+
				"comes from a consumer on a stream", s.Stream, s.Dur)
		}
		if !mqttF105IdentTk(s.Inc) {
			return fmt.Errorf("an ordinary source names durable %q with incarnation %q: a "+
				"consumer name can be recreated, so coordinates alone do not distinguish the "+
				"source this obligation was delivered from", s.Dur, s.Inc)
		}
		if s.Seq == 0 {
			return fmt.Errorf("an ordinary source carries sequence 0, which is the unset value " +
				"and not a delivery")
		}
		if s.Payload != _EMPTY_ || s.Digest != _EMPTY_ {
			return fmt.Errorf("an ordinary source carries retained material %q/%q: material comes "+
				"from one place, and a record describing two is one no reader can act on",
				s.Payload, s.Digest)
		}
	case mqttSourceRetained:
		if !mqttF105IdentTk(s.Payload) {
			return fmt.Errorf("a retained source names payload %q, which is not a material "+
				"identity", s.Payload)
		}
		if s.Digest == _EMPTY_ {
			return fmt.Errorf("a retained source carries no digest, so nothing reassembled from " +
				"its material can be checked")
		}
		// THE WHOLE POINT OF THE UNION. A fabricated Dur or Seq flows into every reader that
		// believes it is looking at a delivery — a source-completion classifier asks the
		// consumer about a sequence that was never delivered and gets an answer about something
		// else. A missing value refuses; a fabricated one is believed.
		if s.Stream != _EMPTY_ || s.Inc != _EMPTY_ || s.Dur != _EMPTY_ || s.Seq != 0 {
			return fmt.Errorf("a retained source carries delivery coordinates %q/%q/%q/%d: there "+
				"is no consumer it was delivered from, and fabricating one makes every reader "+
				"that trusts them wrong", s.Stream, s.Inc, s.Dur, s.Seq)
		}
	default:
		return fmt.Errorf("obligation source carries an unsupported kind %q", s.Kind)
	}
	return nil
}

// mqttCanonicalKind is the closed set of places a canonical PUBREL record lives.
type mqttCanonicalKind string

const (
	// mqttCanonicalConsumer: an ordinary exchange's release, on $MQTT_out at a sequence.
	mqttCanonicalConsumer mqttCanonicalKind = "consumer"
	// mqttCanonicalInflight: a retained exchange's release, at a unique exchange-specific
	// subject on $MQTT_inflight. A DIFFERENT KIND of reference, not a different value.
	mqttCanonicalInflight mqttCanonicalKind = "inflight"
)

// mqttCanonicalRef locates one exchange's canonical PUBREL record.
type mqttCanonicalRef struct {
	Kind mqttCanonicalKind `json:"kind"`

	// Consumer and Seq belong to the consumer arm.
	Consumer string `json:"consumer,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`

	// Subject and Identity belong to the in-flight arm. Identity is carried BESIDE the subject
	// because a subject can be rewritten: reading the record back and finding a different
	// identity is what distinguishes "my record" from "a record at the address mine used to
	// have".
	Subject  string `json:"subject,omitempty"`
	Identity string `json:"identity,omitempty"`
}

// mqttValidateCanonicalRef refuses a reference this broker cannot resolve.
func mqttValidateCanonicalRef(r *mqttCanonicalRef) error {
	if r == nil {
		return fmt.Errorf("canonical reference is absent")
	}
	switch r.Kind {
	case mqttCanonicalConsumer:
		if r.Consumer == _EMPTY_ {
			return fmt.Errorf("a consumer canonical reference names no consumer")
		}
		if r.Seq == 0 {
			return fmt.Errorf("a consumer canonical reference carries sequence 0, which is the " +
				"unset value and not a record")
		}
		if r.Subject != _EMPTY_ || r.Identity != _EMPTY_ {
			return fmt.Errorf("a consumer canonical reference carries an in-flight subject %q: "+
				"a record lives in one place", r.Subject)
		}
	case mqttCanonicalInflight:
		if r.Subject == _EMPTY_ {
			return fmt.Errorf("an in-flight canonical reference names no subject")
		}
		// THE IDENTITY IS NOT OPTIONAL. A subject alone locates an ADDRESS, and this mechanism
		// has been bitten before by treating an address as an identity — a record read back
		// from a rewritten subject is a different record, and only the identity says so.
		if !mqttF105IdentTk(r.Identity) {
			return fmt.Errorf("an in-flight canonical reference carries identity %q: a subject "+
				"locates an address and only the identity says the record there is the one "+
				"this exchange means", r.Identity)
		}
		if r.Consumer != _EMPTY_ || r.Seq != 0 {
			return fmt.Errorf("an in-flight canonical reference carries consumer coordinates "+
				"%q/%d: a retained exchange has no consumer its release was delivered from",
				r.Consumer, r.Seq)
		}
	default:
		return fmt.Errorf("canonical reference carries an unsupported kind %q", r.Kind)
	}
	return nil
}

// mqttSourceNeedsManifest reports whether admitting against this source requires a READY
// manifest.
//
// Separated so the rule has ONE home: an admission whose material is retained is admitted only
// against an exact READY manifest, and a caller deciding that for itself would be a second table
// to keep in step with this union.
func mqttSourceNeedsManifest(s *mqttObligationSource) bool {
	return s != nil && s.Kind == mqttSourceRetained
}
