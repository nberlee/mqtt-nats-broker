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
	"reflect"
	"strconv"
	"strings"
)

// The outbound exchange and its actions (F-105).
//
// An EXCHANGE is the client-facing obligation: one outbound message this broker owes a
// client an outcome for. Its identity is (account epoch, client, generation, exchange
// nonce), and it is preserved across transfer and across allocation replacement. A packet
// identifier is an ALLOCATION belonging to an exchange, never its identity — which is why an
// identifier can be reserved while its exchange is still outstanding, and why two exchanges
// on one identifier at different times are different obligations.
//
// An ACTION is one acquired operation for an exchange: a PUBLISH to emit, a PUBREL to emit.
// It names the exchange, its own acquisition counter, and — this is the part that matters —
// the ORIGINAL tenure and connection that acquired it. A transfer moves the serving pointer
// and does not rewrite actions, because an action's completion authority is a different
// question from the authority to begin another one. An action acquired before a retirement
// may still execute afterwards, on its original connection; what it may not do is be
// redirected through a rebound one.
//
// Both records live on $MQTT_inflight, and both are installed by the SAME atomic batch that
// contends on the outbound authority. That is the admission boundary: an exchange that
// existed without an authority transition admitting it would be an obligation nothing
// authorized, which is precisely the state F-105 exists to make unreachable.

// The record-class tokens for the two classes this file owns. As with the arbitration
// records, a leading class token keeps them structurally out of the register machinery's
// reach: neither can satisfy mqttValidIDHashToken or mqttValidSessGen.
const (
	mqttExchangeClassTk = "xch"
	mqttActionClassTk   = "act"
)

// mqttExchangeSubject returns the durable subject of one exchange.
func mqttExchangeSubject(idHash, gen, nonce string) string {
	return mqttF105SubjectPrefix() + mqttExchangeClassTk + "." + idHash + "." + gen + "." + nonce
}

// mqttActionSubject returns the durable subject of one acquired action.
func mqttActionSubject(idHash, gen, nonce string, acq uint64) string {
	return mqttF105SubjectPrefix() + mqttActionClassTk + "." + idHash + "." + gen + "." +
		nonce + "." + strconv.FormatUint(acq, 10)
}

// mqttActionWalkFilter enumerates ONE exchange's acquired actions, and only those.
//
// Bounded by construction for the same reason the exchange filter is: an exchange holds one
// action per acquisition, and a retransmission is a new acquisition rather than an unbounded
// rewrite. It lives here, beside the subject it mirrors, because a caller assembling the
// pattern itself is a second statement of the layout that nothing keeps in step with the first.
func mqttActionWalkFilter(idHash, gen, nonce string) string {
	return mqttF105SubjectPrefix() + mqttActionClassTk + "." + idHash + "." + gen + "." +
		nonce + ".*"
}

// mqttParseOrdinalToken reads an acquisition token from an action subject.
//
// Separate from mqttParseOrdinal because the two encodings are different: a duty ordinal is
// fixed-width and zero-padded so lexical order IS ordinal order, while an acquisition counter is
// written plainly. Sharing one parser would silently accept either form in both places, which is
// how a subject space stops being a check on anything.
func mqttParseOrdinalToken(tk string) (uint64, error) {
	v, err := strconv.ParseUint(tk, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed acquisition token %q: %w", tk, err)
	}
	if strconv.FormatUint(v, 10) != tk {
		return 0, fmt.Errorf("acquisition token %q does not round trip", tk)
	}
	return v, nil
}

// mqttExchangePhase is where an exchange stands in its protocol.
//
// Numbered from 1 for the reason every other F-105 enum is: a missing or null member decodes
// to zero, and a zero naming a real phase would make an unsupported record read as the most
// permissive one.
type mqttExchangePhase uint8

const (
	// mqttExchangePublish is an outbound PUBLISH whose acknowledgement has not been
	// observed. For QoS 1 that is the whole exchange; for QoS 2 it precedes PUBREC.
	mqttExchangePublish mqttExchangePhase = 1
	// mqttExchangePubRecObserved records a successful PUBREC. From here no further PUBLISH
	// may be acquired: the client has taken ownership of the message and what is owed is
	// the release.
	mqttExchangePubRecObserved mqttExchangePhase = 2
	// 3 is RESERVED. It named a "PUBREL owed" phase that stated nothing PUBREC-observed does
	// not: every consumer treated the two alike, and the canonical PUBREL's identity is the
	// SLOT's fact, bound by the rebind's readback proof. Nothing ever wrote it. A persisted 3
	// is an unsupported shape and is REFUSED by mqttValidExchangePhase — never read as
	// PUBREC-observed, and never renumbered away (F-105 PUBCOMP arm, consult 23).
	// mqttExchangeTerminalPendingDrain is terminal evidence observed while output is still
	// outstanding.
	//
	// It is a phase of its own rather than "terminal", deliberately. Terminal evidence
	// seen during outstanding output does NOT release the exchange: a write acquired
	// earlier may still be executing, and releasing here would let a late first
	// transmission escape after its own release boundary. The drain, or a closure
	// certificate for the action's connection, is what moves it on.
	mqttExchangeTerminalPendingDrain mqttExchangePhase = 4
	// mqttExchangeTerminal is protocol completion with no outstanding output.
	mqttExchangeTerminal mqttExchangePhase = 5
	// mqttExchangeRejectedPendingDrain is a REFUSED PUBREC observed while output is still
	// outstanding. It stands to terminal-pending-drain as rejection stands to completion.
	mqttExchangeRejectedPendingDrain mqttExchangePhase = 6
	// mqttExchangeRejected is a QoS 2 exchange the receiver REFUSED.
	//
	// A PUBREC carrying a reason code >= 0x80 says the client will not take the message:
	// per [MQTT-4.3.3] the sender must NOT send a PUBREL and the exchange is over. That is
	// an ending, but it is NOT the ending mqttExchangeTerminal records — no ownership was
	// transferred and no release was ever owed — so it is its own phase rather than a
	// terminal reached by pretending the PUBREC succeeded. Routing a rejection through
	// PUBREC-observed would put a permanent record on the store claiming the client holds a
	// message it refused, and would oblige a release nothing may emit.
	mqttExchangeRejected mqttExchangePhase = 7
	// mqttExchangeSupersededUnsent is a proven-unsent PUBLISH obligation ended because its
	// packet identifier was re-allocated to a fresh delivery in a PAIR replacement (F-105 step
	// 5, Codex consult 15). It is its OWN absorbing ending, distinct from every acknowledgement
	// phase: no PUBACK, PUBREC or refusal was ever observed — the marker naming the occupant
	// proved the PUBLISH never left the broker — so recording completion (Terminal) or rejection
	// (Rejected) would fabricate a client answer that never came. It exists for both QoS values,
	// and it is reached ONLY through a validated proven-unsent replacement, never through the
	// generic phase-successor table. Absorbing: nothing may be acquired at it, and like every
	// ending it releases the identifier its predecessor held.
	mqttExchangeSupersededUnsent mqttExchangePhase = 8
	// mqttExchangeWithdrawnUnsent is an admitted PUBLISH obligation ended because its delivery
	// was destroyed WITHOUT EVER BEING TRANSMITTED — expired, oversize, cancelled, drained — and
	// no replacement takes its identifier. It is SupersededUnsent's standalone sibling, and a
	// phase of its own for the same reason: no client answer was ever observed, so recording
	// completion or rejection would fabricate one.
	//
	// Reached ONLY by a withdraw-unsent transition (opWithdrawUnsent), never through the
	// successor table, and only on positive whole-exchange evidence the write boundary checks
	// itself: the register at publish with no output token, and the unsent marker naming that
	// allocation still standing. Every ordinary write follows that marker's durable absence, so
	// a standing marker with no token is proof no PUBLISH of it ever reached the client.
	// Absorbing: nothing may be acquired at it.
	mqttExchangeWithdrawnUnsent mqttExchangePhase = 9
	// mqttExchangeDeliveryAbandoned records that the broker PERMANENTLY ABANDONED delivery of
	// a PUBLISH obligation a previous connection may have transmitted and the client never
	// acknowledged: a restored delivery denied by the authorization policy on re-offer (F-105
	// row 5, consult 34). Absorbing FOR DELIVERY — nothing may be acquired at it and it has no
	// successor — but it is NOT an ending in mqttExchangePhaseEnded's sense: no client answer
	// was observed, so the identifier the client may still hold under §4.3 is NOT released by
	// it (it is held until the generation is discarded). What it releases is the Receive
	// Maximum charge, which Spec5 §4.9 makes connection-local: the resumed connection will
	// never send it. Reached ONLY by opAbandonDeniedDelivery.
	mqttExchangeDeliveryAbandoned mqttExchangePhase = 10
	// mqttExchangeDiscarded records that the obligation's GENERATION was discarded before the
	// exchange ended (F-105 7d): a session ended, so nothing will ever deliver, release or
	// drain it. Reached ONLY by a discard-exchange transition, licensed by the generation's
	// committed DISCARDING closure, from any phase that does not already release its holds.
	//
	// It releases the obligation's HOLDS and ends its DUTY — the two questions a discarded
	// generation's retirement asks — and it is deliberately NOT an ending in
	// mqttExchangePhaseEnded's sense. No client answer was observed and no output was proven
	// drained, so it is never evidence that an identifier, an acquired action or a captured
	// acknowledgement binding of the original connection may be released: a discard
	// manufactures no drain and no handler discharge. An exchange that already ended keeps its
	// own ending and is never rewritten to this phase. Absorbing: nothing may be acquired at it.
	mqttExchangeDiscarded mqttExchangePhase = 11
)

func mqttValidExchangePhase(p mqttExchangePhase) bool {
	switch p {
	case mqttExchangePublish, mqttExchangePubRecObserved,
		mqttExchangeTerminalPendingDrain, mqttExchangeTerminal,
		mqttExchangeRejectedPendingDrain, mqttExchangeRejected,
		mqttExchangeSupersededUnsent, mqttExchangeWithdrawnUnsent, mqttExchangeDeliveryAbandoned,
		mqttExchangeDiscarded:
		return true
	}
	return false
}

// mqttExchangeReleasesHolds reports whether a phase has released the obligation's holds on its
// logical-use resources: an ending, or the generation's discard (F-105 7d). It answers the HOLD
// question only — which consumers an obligation may still need — and is the predicate every
// hold-release rule reads, so a discard releases holds without becoming an identifier ending.
func mqttExchangeReleasesHolds(p mqttExchangePhase) bool {
	return mqttExchangePhaseEnded(p) || p == mqttExchangeDiscarded
}

// mqttExchangePhaseEnded reports whether a phase is an ENDING with no outstanding output —
// the phases at which an obligation may be released and its identifier reclaimed.
//
// There are two, because an exchange ends either by completing or by being refused, and a
// predicate is what keeps every consumer from enumerating one of them and forgetting the
// other.
func mqttExchangePhaseEnded(p mqttExchangePhase) bool {
	return p == mqttExchangeTerminal || p == mqttExchangeRejected ||
		p == mqttExchangeSupersededUnsent || p == mqttExchangeWithdrawnUnsent
}

// mqttExchangeDeliveryDutyEnded reports whether an exchange owes no further DELIVERY: it ended
// (mqttExchangePhaseEnded), the broker abandoned its delivery (DeliveryAbandoned), or its
// generation was discarded (Discarded, F-105 7d). It answers
// the DUTY question only — whether recovery owes this exchange anything further — and is
// deliberately NOT the identifier question: identifier exclusivity, replacement and
// mqttIdentifierReleaseAllowed keep reading mqttExchangePhaseEnded, so an abandoned exchange's
// identifier stays closed.
func mqttExchangeDeliveryDutyEnded(p mqttExchangePhase) bool {
	return mqttExchangePhaseEnded(p) || p == mqttExchangeDeliveryAbandoned || p == mqttExchangeDiscarded
}

// mqttExchangePhaseEndedPendingDrain reports whether a phase records an ending whose output
// has not drained. The evidence is durable; the release is not yet permitted.
func mqttExchangePhaseEndedPendingDrain(p mqttExchangePhase) bool {
	return p == mqttExchangeTerminalPendingDrain || p == mqttExchangeRejectedPendingDrain
}

// mqttOutboundExchange is the durable obligation.
type mqttOutboundExchange struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	// Nonce is the exchange's identity within its generation, minted at admission and never
	// rewritten. It survives an allocation replacement, which is what lets the identifier
	// move while the obligation does not.
	Nonce string `json:"nonce"`

	// PI is the identifier currently ALLOCATED to this exchange. It is allocation, not
	// identity: two exchanges may hold one identifier at different times and are different
	// obligations.
	PI    uint16            `json:"pi"`
	QoS   byte              `json:"qos"`
	Phase mqttExchangePhase `json:"phase"`

	// AdmitTxn is the transition that admitted this exchange, so an exchange found in
	// recovery can be tied back to the authority that authorized it. An exchange naming no
	// admission is an obligation nothing authorized and is refused.
	AdmitTxn string `json:"admit_txn"`
	// Tenure is the tenure under which it was admitted. It is NOT rewritten by a transfer:
	// ownership of the exchange follows the authority pointer, and this records where it
	// came from.
	Tenure string `json:"tenure"`

	// Abandoned is the HISTORICAL fact that this broker abandoned the exchange's PUBLISH
	// delivery (F-105 row 5, unit C1): written once, by the abandonment's own transition, and
	// carried unchanged through every later phase. The phase says where the protocol stands;
	// this says the delivery was abandoned, which a later client acknowledgement advancing the
	// phase must not erase — charge reconstruction reads it (a successful PUBREC after an
	// abandonment does not recharge the exchange). Absent — not null — when there is none.
	Abandoned *mqttExchangeAbandonment `json:"abandoned,omitempty"`

	// SharedAcq is the exact shared-group ACQUISITION this obligation's source delivery depends
	// on: the generation's ledger entry that keeps the group incarnation's consumer — and with it
	// the interest-retained source message — alive (outbound_shared_acq.go). Written once by the
	// admission, carried unchanged through every later phase, never re-derived. Absent, not null,
	// for an exchange whose source is not a shared group consumer.
	SharedAcq *mqttSharedAcqRef `json:"shared_acq,omitempty"`

	// Holds names the logical-use resources — the source consumer and, for QoS 2, the canonical
	// PUBREL consumer — this obligation holds an edge on (F-105 7c-lc). It is the REVERSE INDEX of
	// those resources' Deps: a resource is keyed (gen, inc) and names its dependents, so without
	// this the batch that ENDS the obligation could not name the records it must release without
	// walking the generation. Written once, by the admitting batch, equal to exactly the edges that
	// batch adds for this nonce in those two roles (the write boundary re-derives both directions);
	// carried unchanged through every later phase. The resources are in the exchange's own
	// generation. Absent, not null, when there is none.
	Holds []mqttExchangeHold `json:"holds,omitempty"`

	// RetainedOwner is a RETAINED copy's owner: the payload resource incarnation whose
	// retained-material edge this obligation holds (F-112). Written once, by the retained
	// admission batch, bound there to the admitted manifest's own `inc` in both directions
	// (checkRetainedAdmission, checkRetainedAdmissionCoverage), carried unchanged through every
	// later phase. It is the walk-free address the floor's reclamation predicate reads to prove
	// the material edge released; without it, a completed copy whose register is gone names its
	// owner nowhere. Absent, not null, for a non-retained exchange.
	RetainedOwner string `json:"retained_owner,omitempty"`
}

// mqttExchangeHold is one logical-use resource an exchange holds an edge on, and the role.
type mqttExchangeHold struct {
	Inc  string           `json:"inc"`
	Role mqttResourceRole `json:"role"`
}

var mqttExchangeHoldMembers = map[string]struct{}{"inc": {}, "role": {}}

// mqttHoldRole reports whether an edge role is one an exchange's Holds index: the two roles whose
// discharge is the obligation's own ending and whose resource is a name-owned consumer.
func mqttHoldRole(r mqttResourceRole) bool {
	return r == mqttRoleSourceReplay || r == mqttRoleCanonicalPubRel
}

// mqttValidateExchangeHolds is the closed schema of the index: present means non-empty, each hold
// a well-formed incarnation in a hold role, no hold twice, and the canonical PUBREL only at QoS 2.
func mqttValidateExchangeHolds(e *mqttOutboundExchange) error {
	if e.Holds == nil {
		return nil
	}
	if len(e.Holds) == 0 {
		return fmt.Errorf("outbound exchange carries an empty holds index: absent, not empty, when there is none")
	}
	seen := make(map[mqttExchangeHold]struct{}, len(e.Holds))
	for _, h := range e.Holds {
		if !mqttF105IdentTk(h.Inc) {
			return fmt.Errorf("outbound exchange holds a malformed resource incarnation %q", h.Inc)
		}
		if !mqttHoldRole(h.Role) {
			return fmt.Errorf("outbound exchange holds %q in role %q, which the holds index does not carry", h.Inc, h.Role)
		}
		if h.Role == mqttRoleCanonicalPubRel && e.QoS != 2 {
			return fmt.Errorf("a QoS %d outbound exchange holds a canonical PUBREL consumer", e.QoS)
		}
		if _, dup := seen[h]; dup {
			return fmt.Errorf("outbound exchange holds %q in role %q twice", h.Inc, h.Role)
		}
		seen[h] = struct{}{}
	}
	return nil
}

// mqttHoldsEqual compares two holds indexes as sets.
func mqttHoldsEqual(a, b []mqttExchangeHold) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[mqttExchangeHold]int, len(a))
	for _, h := range a {
		m[h]++
	}
	for _, h := range b {
		if m[h] == 0 {
			return false
		}
		m[h]--
	}
	return true
}

// mqttExchangeAbandonment is the durable binding of one abandonment decision: why and under
// which tenure it was taken, the exact allocation and acquisition it abandoned, the source
// delivery it disposes of, and the source's JetStream acknowledgement subject captured at the
// denial, so an owner reconstructed after a restart can settle a still-live source without
// waiting for a redelivery (a durable and sequence alone are not an acknowledgement handle).
//
// The fact is a UNION by source, like the register (F-105 7c): an ORDINARY abandonment names the
// source delivery it disposes of (Dur, Seq, Reply); a RETAINED one names the materialization
// (Payload) and FORBIDS the delivery coordinates, because a retained delivery was never a source
// delivery and a fabricated one would be settled by a reader that believes it.
type mqttExchangeAbandonment struct {
	Reason  string `json:"reason"`
	Tenure  string `json:"tenure"`
	Alloc   string `json:"alloc"`
	Acq     uint64 `json:"acq"`
	Dur     string `json:"dur,omitempty"`
	Seq     uint64 `json:"seq,omitempty"`
	Reply   string `json:"reply,omitempty"`
	Payload string `json:"payload,omitempty"`
}

// mqttExchangeAbandonmentMembers is every member either arm may carry; which are REQUIRED depends
// on the arm (mqttExchangeAbandonmentRequired).
var mqttExchangeAbandonmentMembers = map[string]struct{}{
	"reason": {}, "tenure": {}, "alloc": {}, "acq": {}, "dur": {}, "seq": {}, "reply": {},
	"payload": {},
}

var (
	mqttExchangeAbandonmentOrdinaryRequired = map[string]struct{}{
		"reason": {}, "tenure": {}, "alloc": {}, "acq": {}, "dur": {}, "seq": {}, "reply": {},
	}
	mqttExchangeAbandonmentRetainedRequired = map[string]struct{}{
		"reason": {}, "tenure": {}, "alloc": {}, "acq": {}, "payload": {},
	}
)

// retained reports whether the fact is the retained arm.
func (f *mqttExchangeAbandonment) retained() bool { return f != nil && f.Payload != _EMPTY_ }

// mqttValidateExchangeAbandonment is the closed schema of the fact: every member present and
// meaningful, the reason in the closed set, and the reply an acknowledgement subject for
// exactly the named source message.
func mqttValidateExchangeAbandonment(f *mqttExchangeAbandonment) error {
	switch {
	case f == nil:
		return fmt.Errorf("the abandonment fact is absent")
	case f.retained():
		return mqttValidateRetainedAbandonment(f)
	case !mqttValidAbandonReason(f.Reason):
		return fmt.Errorf("the abandonment fact carries reason %q, which this broker does not abandon for", f.Reason)
	case !mqttF105IdentTk(f.Tenure):
		return fmt.Errorf("the abandonment fact carries a malformed tenure %q", f.Tenure)
	case !mqttF105IdentTk(f.Alloc):
		return fmt.Errorf("the abandonment fact carries a malformed allocation %q", f.Alloc)
	case f.Dur == _EMPTY_ || f.Seq == 0:
		return fmt.Errorf("the abandonment fact names no source delivery")
	case !mqttSourceAckReplyBinds(f.Reply, mqttStreamName, f.Dur, f.Seq):
		return fmt.Errorf("the abandonment fact's reply %q is not an acknowledgement of %q sequence %d", f.Reply, f.Dur, f.Seq)
	}
	return nil
}

// mqttExchangeEqual compares two exchanges by value, the fact, the shared reference and the holds
// index included. Deep equality is value equality here: every pointer member is an optional
// record compared by its contents, and an absent holds index differs from any present one.
func mqttExchangeEqual(a, b *mqttOutboundExchange) bool {
	if a == nil || b == nil {
		return a == b
	}
	return reflect.DeepEqual(a, b)
}

// mqttExchangeFactPhase reports whether an exchange may carry the abandonment fact in phase p:
// the abandonment itself, exactly the phases a client acknowledgement advances it to, and the
// generation's discard, which preserves the immutable fact through its ending (F-105 7d).
func mqttExchangeFactPhase(p mqttExchangePhase) bool {
	switch p {
	case mqttExchangeDeliveryAbandoned, mqttExchangePubRecObserved, mqttExchangeTerminalPendingDrain,
		mqttExchangeTerminal, mqttExchangeRejected, mqttExchangeDiscarded:
		return true
	}
	return false
}

// mqttOutboundAction is one acquired operation for an exchange.
type mqttOutboundAction struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	Nonce string `json:"nonce"`
	// Acq is this action's acquisition counter within its exchange, so a retransmission is
	// a distinct action rather than an overwrite of the first.
	Acq uint64 `json:"acq"`

	// Kind is the packet this action emits.
	Kind mqttActionKind `json:"kind"`
	// PI is the identifier the emitted packet carries, frozen here: an action must not
	// follow a later allocation replacement, or the bytes on the wire would disagree with
	// what the client was told.
	PI uint16 `json:"pi"`

	// THE ORIGINAL BINDING. Run and Conn name the connection that acquired this action, and
	// Tenure the tenure it was acquired under. None is ever rewritten — not by a transfer,
	// not by a resident reconnect. A completion resolved against "the session's current
	// connection" would follow a rebind and answer for a successor; these fields are what a
	// captured handler resolves against instead.
	Run    string `json:"run"`
	Conn   string `json:"conn"`
	Tenure string `json:"tenure"`

	// AcqTxn is the transition that acquired it.
	AcqTxn string `json:"acq_txn"`
}

// mqttActionKind is the closed set of packets an action may emit.
type mqttActionKind string

const (
	mqttActionPublish mqttActionKind = "publish"
	mqttActionPubRel  mqttActionKind = "pubrel"
)

func mqttValidActionKind(k mqttActionKind) bool {
	return k == mqttActionPublish || k == mqttActionPubRel
}

var mqttExchangeMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "nonce": {}, "pi": {}, "qos": {}, "phase": {},
	"admit_txn": {}, "tenure": {},
}

// mqttExchangeAllowedMembers is every member an exchange may carry: the mandatory ones, the
// optional abandonment fact and the optional shared acquisition.
var mqttExchangeAllowedMembers = func() map[string]struct{} {
	m := map[string]struct{}{"abandoned": {}, "shared_acq": {}, "holds": {}, "retained_owner": {}}
	for k := range mqttExchangeMembers {
		m[k] = struct{}{}
	}
	return m
}()

var mqttActionMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "nonce": {}, "acq": {}, "kind": {}, "pi": {},
	"run": {}, "conn": {}, "tenure": {}, "acq_txn": {},
}

// mqttDecodeOutboundExchange decodes and fully validates one exchange body.
func mqttDecodeOutboundExchange(data []byte, wantID, wantEpoch, wantGen, wantNonce string) (*mqttOutboundExchange, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttExchangeAllowedMembers, "an outbound exchange"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttExchangeMembers, "an outbound exchange"); err != nil {
		return nil, err
	}
	// The optional fact is absent or a complete object: never null, never partial, never
	// carrying a member this broker does not write.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if fact, ok := raw["abandoned"]; ok {
		if err := mqttJSONMembersAreExactAndUnique(fact, mqttExchangeAbandonmentMembers, "an abandonment fact"); err != nil {
			return nil, err
		}
		req := mqttExchangeAbandonmentOrdinaryRequired
		var arm map[string]json.RawMessage
		if err := json.Unmarshal(fact, &arm); err != nil {
			return nil, err
		}
		if _, ok := arm["payload"]; ok {
			req = mqttExchangeAbandonmentRetainedRequired
		}
		if err := mqttJSONMembersArePresent(fact, req, "an abandonment fact"); err != nil {
			return nil, err
		}
	}
	if ref, ok := raw["shared_acq"]; ok {
		if err := mqttJSONMembersAreExactAndUnique(ref, mqttSharedAcqRefMembers, "a shared acquisition reference"); err != nil {
			return nil, err
		}
		if err := mqttJSONMembersArePresent(ref, mqttSharedAcqRefMembers, "a shared acquisition reference"); err != nil {
			return nil, err
		}
	}
	if ro, ok := raw["retained_owner"]; ok && string(ro) == "null" {
		return nil, fmt.Errorf("an outbound exchange's retained owner is null: it is absent or names an incarnation")
	}
	if hs, ok := raw["holds"]; ok {
		var items []json.RawMessage
		if err := json.Unmarshal(hs, &items); err != nil || items == nil {
			return nil, fmt.Errorf("an outbound exchange's holds index is not an array")
		}
		for _, it := range items {
			if err := mqttJSONMembersAreExactAndUnique(it, mqttExchangeHoldMembers, "an exchange hold"); err != nil {
				return nil, err
			}
			if err := mqttJSONMembersArePresent(it, mqttExchangeHoldMembers, "an exchange hold"); err != nil {
				return nil, err
			}
		}
	}
	e := &mqttOutboundExchange{}
	if err := json.Unmarshal(data, e); err != nil {
		return nil, err
	}
	if err := mqttValidateOutboundExchange(e, wantID, wantEpoch, wantGen, wantNonce); err != nil {
		return nil, err
	}
	return e, nil
}

// mqttDecodeOutboundAction decodes and fully validates one action body.
func mqttDecodeOutboundAction(data []byte, wantID, wantEpoch, wantGen, wantNonce string, wantAcq uint64) (*mqttOutboundAction, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttActionMembers, "an outbound action"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttActionMembers, "an outbound action"); err != nil {
		return nil, err
	}
	a := &mqttOutboundAction{}
	if err := json.Unmarshal(data, a); err != nil {
		return nil, err
	}
	if err := mqttValidateOutboundAction(a, wantID, wantEpoch, wantGen, wantNonce, wantAcq); err != nil {
		return nil, err
	}
	return a, nil
}

// mqttValidateOutboundExchange refuses a body this broker does not support.
func mqttValidateOutboundExchange(e *mqttOutboundExchange, wantID, wantEpoch, wantGen, wantNonce string) error {
	if e == nil {
		return fmt.Errorf("outbound exchange is absent")
	}
	if e.ID != wantID {
		return fmt.Errorf("outbound exchange names client %q, not %q", e.ID, wantID)
	}
	if e.Epoch != wantEpoch {
		return fmt.Errorf("outbound exchange carries recovery epoch %q, not this account's %q", e.Epoch, wantEpoch)
	}
	if e.Gen != wantGen {
		return fmt.Errorf("outbound exchange names generation %q on the subject of %q", e.Gen, wantGen)
	}
	if e.Nonce != wantNonce {
		return fmt.Errorf("outbound exchange names nonce %q on the subject of %q", e.Nonce, wantNonce)
	}
	if !mqttF105IdentTk(e.Nonce) {
		return fmt.Errorf("outbound exchange carries a malformed nonce %q", e.Nonce)
	}
	if e.PI == 0 {
		return fmt.Errorf("outbound exchange carries packet identifier zero")
	}
	if e.QoS != 1 && e.QoS != 2 {
		return fmt.Errorf("outbound exchange carries QoS %d: only 1 and 2 owe an acknowledgement", e.QoS)
	}
	if !mqttValidExchangePhase(e.Phase) {
		return fmt.Errorf("outbound exchange carries an unsupported phase %d", e.Phase)
	}
	// A QoS 1 exchange has no release, so the PUBREL-side phase describes an exchange that
	// cannot exist.
	//
	// The rejection phases are equally QoS 2's: they record a REFUSED PUBREC, and a QoS 1
	// exchange has no PUBREC to refuse. A failed PUBACK ends a QoS 1 exchange at terminal,
	// where nothing about ownership is claimed either way.
	if e.QoS == 1 && (e.Phase == mqttExchangePubRecObserved ||
		e.Phase == mqttExchangeRejectedPendingDrain || e.Phase == mqttExchangeRejected) {
		return fmt.Errorf("a QoS 1 outbound exchange carries phase %d, which belongs to QoS 2", e.Phase)
	}
	if !mqttF105IdentTk(e.AdmitTxn) {
		return fmt.Errorf("outbound exchange carries a malformed admitting transition %q", e.AdmitTxn)
	}
	if !mqttF105IdentTk(e.Tenure) {
		return fmt.Errorf("outbound exchange carries a malformed tenure %q", e.Tenure)
	}
	// THE FACT AND ITS PHASES. An abandonment is written only with its fact, and the fact
	// survives only into the phases a client acknowledgement advances an abandonment to: a
	// fresh obligation, a withdrawal or a supersession carrying it describes a history that
	// cannot have happened.
	if e.Abandoned != nil {
		if err := mqttValidateExchangeAbandonment(e.Abandoned); err != nil {
			return fmt.Errorf("outbound exchange: %w", err)
		}
		if !mqttExchangeFactPhase(e.Phase) {
			return fmt.Errorf("outbound exchange carries the abandonment fact in phase %d, which no abandonment reaches", e.Phase)
		}
	} else if e.Phase == mqttExchangeDeliveryAbandoned {
		return fmt.Errorf("outbound exchange is DeliveryAbandoned without its abandonment fact")
	}
	// THE SHARED DEPENDENCY'S SHAPE. Whether a reference is REQUIRED depends on the register's
	// durable, which this body does not carry: that is the pair rule's (mqttCheckSharedAcqPair).
	// Supported shared delivery is QoS 1 alone.
	if e.SharedAcq != nil {
		if err := mqttValidateSharedAcqRef(e.SharedAcq); err != nil {
			return fmt.Errorf("outbound exchange: %w", err)
		}
		if e.QoS != 1 {
			return fmt.Errorf("outbound exchange depends on a shared acquisition at QoS %d; supported shared delivery is QoS 1", e.QoS)
		}
	}
	if e.RetainedOwner != _EMPTY_ && !mqttF105IdentTk(e.RetainedOwner) {
		return fmt.Errorf("outbound exchange carries a malformed retained owner %q", e.RetainedOwner)
	}
	return mqttValidateExchangeHolds(e)
}

// mqttValidateOutboundAction refuses a body this broker does not support.
func mqttValidateOutboundAction(a *mqttOutboundAction, wantID, wantEpoch, wantGen, wantNonce string, wantAcq uint64) error {
	if a == nil {
		return fmt.Errorf("outbound action is absent")
	}
	if a.ID != wantID {
		return fmt.Errorf("outbound action names client %q, not %q", a.ID, wantID)
	}
	if a.Epoch != wantEpoch {
		return fmt.Errorf("outbound action carries recovery epoch %q, not this account's %q", a.Epoch, wantEpoch)
	}
	if a.Gen != wantGen {
		return fmt.Errorf("outbound action names generation %q on the subject of %q", a.Gen, wantGen)
	}
	if a.Nonce != wantNonce {
		return fmt.Errorf("outbound action names exchange %q on the subject of %q", a.Nonce, wantNonce)
	}
	if a.Acq != wantAcq {
		return fmt.Errorf("outbound action names acquisition %d on the subject of %d", a.Acq, wantAcq)
	}
	if !mqttValidActionKind(a.Kind) {
		return fmt.Errorf("outbound action carries an unsupported kind %q", a.Kind)
	}
	if a.PI == 0 {
		return fmt.Errorf("outbound action carries packet identifier zero")
	}
	// THE ORIGINAL BINDING IS MANDATORY. An action that cannot name the connection it was
	// acquired on has no completion route that survives a rebind, which is the whole reason
	// the binding is carried rather than looked up.
	if a.Run == _EMPTY_ || a.Conn == _EMPTY_ {
		return fmt.Errorf("outbound action names no originating connection")
	}
	if !mqttF105IdentTk(a.Tenure) {
		return fmt.Errorf("outbound action carries a malformed tenure %q", a.Tenure)
	}
	if !mqttF105IdentTk(a.AcqTxn) {
		return fmt.Errorf("outbound action carries a malformed acquiring transition %q", a.AcqTxn)
	}
	return nil
}

// mqttActionBoundTo reports whether an action was acquired on the connection b names.
//
// This is the check a captured completion handler makes instead of consulting the session's
// current connection: a resident reconnect rebinds that, and a handler resolved against it
// would answer for a successor's state.
func mqttActionBoundTo(a *mqttOutboundAction, b *mqttConnBinding) bool {
	return a != nil && b != nil && a.Run == b.run && a.Conn == b.conn
}

// mqttActionPermittedAtPhase reports whether an exchange standing at this phase may emit this
// packet at all.
//
// The completion rules answer a different question — whether MORE work may be acquired, and
// whether it must wait for a drain — and they leave the choice of packet to the caller. This
// is the choice: a PUBLISH is the delivery itself, so it may only be acquired while the
// exchange is still at its publish; a PUBREL is the release, so it may only be acquired once
// a successful PUBREC has transferred ownership and until it has been discharged.
//
// Stated once, here, because both halves are protocol facts about the phase and a second
// statement of them at an acquisition site is how a retransmission after PUBREC or a release
// with nothing behind it reaches the wire.
func mqttActionPermittedAtPhase(qos byte, phase mqttExchangePhase, kind mqttActionKind) error {
	if !mqttValidExchangePhase(phase) {
		return fmt.Errorf("an exchange at unsupported phase %d emits nothing", phase)
	}
	switch kind {
	case mqttActionPublish:
		if phase != mqttExchangePublish {
			return fmt.Errorf("an exchange at phase %d may not emit another PUBLISH: the "+
				"delivery is answered for", phase)
		}
		return nil
	case mqttActionPubRel:
		if qos != 2 {
			return fmt.Errorf("a QoS %d exchange owes no release", qos)
		}
		if phase != mqttExchangePubRecObserved {
			return fmt.Errorf("an exchange at phase %d owes no release", phase)
		}
		return nil
	}
	return fmt.Errorf("an exchange was asked to emit an unsupported packet %q", kind)
}

// mqttAcquireActionTransition assembles the transition that acquires one action.
func mqttAcquireActionTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	a *mqttOutboundAction) (*mqttAuthorityTransition, error) {
	if prev == nil {
		return nil, fmt.Errorf("acquiring an action requires the authority it is acquired under")
	}
	if a == nil {
		return nil, fmt.Errorf("acquiring an action requires the action")
	}
	if a.AcqTxn != txn {
		return nil, fmt.Errorf("the action names acquiring transition %q, but this transition is %q",
			a.AcqTxn, txn)
	}
	if a.Gen != prev.Gen {
		return nil, fmt.Errorf("the action is acquired in generation %q but its authority carries %q",
			a.Gen, prev.Gen)
	}
	body, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionAcquireAction, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityServing, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
		// Create-only on the acquisition counter, so a retransmission is a NEW action rather
		// than an overwrite of the one that may still be executing.
		Extra: []mqttTransitionMember{{
			subj:   mqttActionSubject(getHash(prev.ID), a.Gen, a.Nonce, a.Acq),
			expect: 0,
			body:   body,
		}},
	}, nil
}

// mqttExchangePhaseSucceeds reports whether one phase may follow another for this QoS.
//
// The relation is enumerated rather than derived from the ordering, because "greater than"
// is not the protocol: a QoS 1 exchange has no PUBREC to observe and no release to owe, and
// an ordering check would let a PUBACK be recorded as ownership transfer. The relation is
// also acyclic and strictly forward, which is what makes an exchange unable to leave a
// terminal phase — a resurrected obligation would re-charge Receive Maximum and re-authorize
// output for a message the client already finished with.
func mqttExchangePhaseSucceeds(qos byte, from, to mqttExchangePhase) error {
	if !mqttValidExchangePhase(from) || !mqttValidExchangePhase(to) {
		return fmt.Errorf("an exchange cannot move between unsupported phases %d and %d", from, to)
	}
	if from == to {
		return fmt.Errorf("an exchange already stands at phase %d, so advancing to it records nothing", to)
	}
	switch qos {
	case 1:
		// PUBACK ends a QoS 1 exchange outright. Whether it lands terminal or
		// terminal-pending-drain is the OUTPUT's question, not the protocol's, and both are
		// reachable here for that reason.
		if from == mqttExchangePublish &&
			(to == mqttExchangeTerminal || to == mqttExchangeTerminalPendingDrain) {
			return nil
		}
		// A PUBACK ends an ABANDONED QoS 1 delivery too (unit C2). An abandoned exchange has no
		// output that may still execute, so its ending is never pending-drain.
		if from == mqttExchangeDeliveryAbandoned && to == mqttExchangeTerminal {
			return nil
		}
	case 2:
		switch from {
		case mqttExchangePublish:
			// PUBREC is the ownership transfer. A QoS 2 exchange may NOT go from a publish
			// straight to terminal: there is no acknowledgement that ends it there, and
			// admitting one would discard a release the client is still owed.
			if to == mqttExchangePubRecObserved {
				return nil
			}
			// A REFUSED PUBREC ends it here instead, and only here: the refusal is the
			// answer to the PUBLISH, so an exchange that has already transferred ownership
			// or owes a release cannot be talked back into a rejection. Which of the two
			// rejection phases it lands in is the OUTPUT's question, exactly as for QoS 1.
			if to == mqttExchangeRejected || to == mqttExchangeRejectedPendingDrain {
				return nil
			}
		case mqttExchangePubRecObserved:
			// PUBCOMP ENDS IT, whatever its reason code (mqttPhaseForAck). Terminal when no
			// PUBREL output is outstanding; terminal-pending-drain when a PUBREL action is still
			// live, so the release boundary waits for that action's own drain.
			if to == mqttExchangeTerminal || to == mqttExchangeTerminalPendingDrain {
				return nil
			}
		case mqttExchangeRejectedPendingDrain:
			// The drain never changes WHICH ending was observed, so a refused exchange
			// drains to refused and never to terminal.
			if to == mqttExchangeRejected {
				return nil
			}
		case mqttExchangeDeliveryAbandoned:
			// The client's answer to a copy of an ABANDONED delivery (unit C2): a successful
			// PUBREC still transfers ownership and still owes the release (the PUBREL is sent and
			// completed as for any other), a refused one still ends it. No output is outstanding
			// on an abandoned exchange, so the refusal is never pending-drain.
			if to == mqttExchangePubRecObserved || to == mqttExchangeRejected {
				return nil
			}
		}
	default:
		return fmt.Errorf("an exchange at QoS %d has no phase protocol", qos)
	}
	// The drain completing is the one move common to both, and it is the ONLY way out of
	// either pending-drain phase: they exist precisely because the ending was observed while
	// output could still execute. A drain never changes WHICH ending was observed.
	if from == mqttExchangeTerminalPendingDrain && to == mqttExchangeTerminal {
		return nil
	}
	return fmt.Errorf("a QoS %d exchange may not move from phase %d to phase %d", qos, from, to)
}

// mqttAdvanceExchangeTransition assembles the durable advance of one exchange's phase.
//
// THE PHASE IS A DURABLE FACT, not a field a handler edits in memory. Before this the only
// way an exchange reached PUBREC-observed or terminal was for a caller to write the whole
// record itself, which meant nothing checked the move: a PUBCOMP handler could put a QoS 2
// exchange straight from its publish to terminal and the release the client was owed would
// simply vanish, and no authority would have arbitrated any of it.
//
// It CASes the exchange at the sequence it was read, so two brokers observing the same
// acknowledgement produce one advance, and it contends on the authority in the same batch, so
// the advance is an act of the current owner even when the exchange belongs to an older
// generation the owner has already closed.
func mqttAdvanceExchangeTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	cur *mqttOutboundExchange, curSeq uint64, to mqttExchangePhase) (*mqttAuthorityTransition, error) {

	if prev == nil {
		return nil, fmt.Errorf("advancing an exchange requires the authority it is advanced under")
	}
	if cur == nil {
		return nil, fmt.Errorf("advancing an exchange requires the exchange as it stands")
	}
	// THE READ SEQUENCE IS MANDATORY. Zero would frame the member create-only, which for an
	// exchange that already exists is not an advance at all — it would be refused by the
	// store, but stating it here says why rather than leaving a caller to read a CAS error.
	if curSeq == 0 {
		return nil, fmt.Errorf("advancing exchange %q requires the sequence it was read at", cur.Nonce)
	}
	if verr := mqttValidateOutboundExchange(cur, prev.ID, prev.Epoch, cur.Gen, cur.Nonce); verr != nil {
		return nil, fmt.Errorf("refusing to advance an unsupported exchange: %w", verr)
	}
	if err := mqttExchangePhaseSucceeds(cur.QoS, cur.Phase, to); err != nil {
		return nil, err
	}

	// ONLY THE PHASE MOVES. The advance is built by copying the record as read, so an
	// identifier reallocation, a tenure or an admitting transition cannot ride along with a
	// completion — each of those is a different operation with a different authorization.
	next := *cur
	next.Phase = to
	body, err := json.Marshal(&next)
	if err != nil {
		return nil, err
	}

	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionAdvanceExchange, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		// The authority is reinstalled exactly as it stood, pending proposals included: an
		// advance is not an ownership event, and dropping a reservation or a preparation
		// here would abandon a proposal nobody resolved.
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: prev.State, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
			Intent: prev.Intent, Creation: prev.Creation,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttExchangeSubject(getHash(prev.ID), cur.Gen, cur.Nonce),
			expect: curSeq,
			body:   body,
		}},
	}, nil
}

// mqttSourceAckReplyBinds reports whether reply is a JetStream acknowledgement subject for
// exactly message seq of durable consumer dur on stream — both the v1 layout
// ($JS.ACK.<stream>.<consumer>.<dc>.<sseq>.<dseq>.<ts>.<pending>) and the v2 layout, which
// prefixes the domain and account hash. A matching sequence alone is not a binding: the
// stream and the consumer must be the delivery's own.
func mqttSourceAckReplyBinds(reply, stream, dur string, seq uint64) bool {
	if reply == _EMPTY_ || stream == _EMPTY_ || dur == _EMPTY_ || seq == 0 {
		return false
	}
	rs, rd, rseq, ok := mqttParseSourceAckReply(reply)
	return ok && rs == stream && rd == dur && rseq == seq
}

// mqttParseSourceAckReply is THE parser of a JetStream acknowledgement subject: the stream, the
// consumer and the stream sequence it acknowledges, in both the v1 and the v2 layout. ok is false
// for anything that is not a well-formed acknowledgement subject.
func mqttParseSourceAckReply(reply string) (stream, dur string, seq uint64, ok bool) {
	toks := strings.Split(reply, ".")
	if len(toks) < 2 || toks[0] != "$JS" || toks[1] != "ACK" {
		return _EMPTY_, _EMPTY_, 0, false
	}
	var off int
	switch {
	case len(toks) == expectedNumReplyTokensV1:
		off = 2
	case len(toks) >= expectedNumReplyTokensV2:
		off = 4
	default:
		return _EMPTY_, _EMPTY_, 0, false
	}
	sseq, _, _, _, _ := ackReplyInfo(reply)
	if toks[off] == _EMPTY_ || toks[off+1] == _EMPTY_ || sseq == 0 {
		return _EMPTY_, _EMPTY_, 0, false
	}
	return toks[off], toks[off+1], sseq, true
}
