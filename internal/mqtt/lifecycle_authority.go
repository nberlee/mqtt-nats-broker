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
	"bytes"
	"encoding/json"
	"fmt"
)

// What the per-client authority carries about its LIFECYCLE (issue #133, plan 4.2).
//
// The authority already arbitrates ownership of the current generation. Reclamation needs it
// to say more, in the same record and therefore in the same compare-and-swap: which
// generations have stopped being current and are not yet retired (Outstanding), which
// acceptance is current (Accept) and which is not yet bound to a live connection (Pending),
// and every delayed Will a later acceptance could still suppress (Anchors). Each list is bounded by admission, never by
// eviction: a transition whose result would exceed a cap is refused, and every completion step
// fits because the work it completes reserved its room when it was admitted (plan section 9).

// mqttLifecyclePhase is the lifecycle's own phase, distinct from the current generation's
// ownership state.
type mqttLifecyclePhase uint8

const (
	// mqttLifecycleServing: the lifecycle admits acceptances.
	mqttLifecycleServing mqttLifecyclePhase = 1
	// mqttLifecycleClosed: the last generation's release completed with no generation
	// current. Retirable once nothing is outstanding.
	mqttLifecycleClosed mqttLifecyclePhase = 2
	// mqttLifecycleRetiring: the authority's last write. The boundary refuses every
	// transition of an authority in this phase.
	mqttLifecycleRetiring mqttLifecyclePhase = 3
)

// mqttOutstandingPhase is a noncurrent generation's retirement progress (plan 6.2).
type mqttOutstandingPhase uint8

const (
	mqttOutstandingOpen    mqttOutstandingPhase = 1
	mqttOutstandingFenced  mqttOutstandingPhase = 2
	mqttOutstandingBounded mqttOutstandingPhase = 3
)

// mqttTeardownKind is how a generation stopped being current (plan 5.2's variants).
type mqttTeardownKind uint8

const (
	// mqttTeardownMarker: a teardown claim marked the record.
	mqttTeardownMarker mqttTeardownKind = 1
	// mqttTeardownHandoffDiscard: a Clean Start acceptance replaced the record.
	mqttTeardownHandoffDiscard mqttTeardownKind = 2
	// mqttTeardownAbandon: a pending fresh acceptance was abandoned and its record marked.
	mqttTeardownAbandon mqttTeardownKind = 3
)

// mqttOutstandingTeardown is the authority's fixed-size record of a generation's teardown: its
// kind, the marker it binds, and whether its release is pending. The retirement request itself
// is checked at the claim's write boundary and enrolled through the closing members; no reader
// needs it afterwards, so it is not persisted.
type mqttOutstandingTeardown struct {
	Kind      mqttTeardownKind `json:"kind"`
	Nonce     string           `json:"nonce,omitempty"`
	MarkerWID string           `json:"marker_wid,omitempty"`
	// Pending is true until the record holding the marker is released (deleted) or
	// displaced; generation fencing requires it false.
	Pending bool `json:"pending"`
	// Displaced records that a successor's acceptance replaced the marker, which discharged
	// the release: the subject will never again hold this generation's marker.
	Displaced bool `json:"displaced,omitempty"`
}

// mqttOutstandingGen is one generation made noncurrent and not yet retired.
type mqttOutstandingGen struct {
	Ord      uint64                  `json:"ord"`
	Phase    mqttOutstandingPhase    `json:"phase"`
	Bound    uint64                  `json:"bound,omitempty"`
	Acqs     bool                    `json:"acqs,omitempty"`
	Teardown mqttOutstandingTeardown `json:"teardown"`
}

// mqttAcceptance is the current acceptance: what the shared tenure is ordered by and what a
// claimant's suppression evidence is dated by.
type mqttAcceptance struct {
	Ord    uint64 `json:"ord"`
	At     int64  `json:"at"`
	Tenure string `json:"tenure"`
	Nonce  string `json:"nonce"`
}

// mqttPendingAcceptKind says what abandoning a pending acceptance may do (plan 5.1).
type mqttPendingAcceptKind uint8

const (
	// mqttPendingFresh: the acceptance installed its generation, which holds nothing a
	// client was ever told about; abandonment discards it.
	mqttPendingFresh mqttPendingAcceptKind = 1
	// mqttPendingResume: the acceptance preserved an established session; abandonment
	// restores the session's prior lifetime and leaves the generation current.
	mqttPendingResume mqttPendingAcceptKind = 2
)

// mqttPriorLifetime is a resumed session's lifetime before the pending CONNECT: its expiry and
// whether it was a clean session, the PAIR the CONNECT's own lifetime save replaces (a requested
// expiry of zero persists a clean session), so an abandonment restores both.
type mqttPriorLifetime struct {
	ExpiryInterval uint32 `json:"expiry"`
	Clean          bool   `json:"clean,omitempty"`
}

// mqttPendingAcceptance is an acceptance not yet bound to a live connection. Bind and
// abandonment compete on it with mutually exclusive predicates.
type mqttPendingAcceptance struct {
	Kind      mqttPendingAcceptKind `json:"kind"`
	AcceptOrd uint64                `json:"accept_ord"`
	Nonce     string                `json:"nonce"`
	At        int64                 `json:"at"`
	Prior     *mqttPriorLifetime    `json:"prior,omitempty"`
}

// mqttAnchorState is a Will anchor's state (plan 5.3).
type mqttAnchorState uint8

const (
	// mqttAnchorArmed: the acceptance that allocated it is the connection's; its Will is
	// not yet owed.
	mqttAnchorArmed mqttAnchorState = 1
	// mqttAnchorOwed: the connection closed and its delayed Will is owed at FireAt.
	mqttAnchorOwed mqttAnchorState = 2
)

// mqttWillAnchor is the suppression history of one delay-capable Will.
type mqttWillAnchor struct {
	Ord       uint64          `json:"ord"`
	AcceptOrd uint64          `json:"accept_ord"`
	State     mqttAnchorState `json:"state"`
	FireAt    int64           `json:"fire_at,omitempty"`
	// MinAt is the earliest acceptance time of every acceptance later than the anchor's own,
	// or zero for none. Every accept batch lowers it; nothing raises it.
	MinAt int64 `json:"min_at,omitempty"`
}

// The caps (plan section 9). Entries are fixed-size, so the worst-case serialized authority is
// a function of these counts; TestTheWorstCaseLifecycleRecordsFitTheirBudgets keeps it under
// the budget.
const (
	mqttMaxGenerationSlots = 32 // K: outstanding generations plus the current one
	mqttMaxWillAnchors     = 32 // A
	// mqttLifecycleAuthorityBudget bounds the serialized authority well under the default
	// max_payload, leaving the rest of every batch its room.
	mqttLifecycleAuthorityBudget = 64 * 1024 //nolint:unused // proven against the caps by TestTheWorstCaseLifecycleRecordsFitTheirBudgets, never checked per write
)

// mqttCheckGenerationSlots is THE capacity invariant, checked on the RESULT of a transition:
// every generation, current or outstanding, already owns the slot its discard will occupy.
func mqttCheckGenerationSlots(outstanding int, hasCurrent bool) error {
	n := outstanding
	if hasCurrent {
		n++
	}
	if n > mqttMaxGenerationSlots {
		return fmt.Errorf("the lifecycle would hold %d generations, beyond its %d slots", n, mqttMaxGenerationSlots)
	}
	return nil
}

// mqttValidateLifecycleLists refuses lifecycle lists this broker would not write.
func mqttValidateLifecycleLists(outstanding []mqttOutstandingGen, nextGenOrd uint64, anchors []mqttWillAnchor,
	nextAnchorOrd, nextAcceptOrd uint64) error {

	seen := make(map[uint64]struct{}, len(outstanding))
	for _, o := range outstanding {
		if o.Ord >= nextGenOrd {
			return fmt.Errorf("outstanding generation %d was never allocated (next %d)", o.Ord, nextGenOrd)
		}
		if _, dup := seen[o.Ord]; dup {
			return fmt.Errorf("outstanding generation %d is listed twice", o.Ord)
		}
		seen[o.Ord] = struct{}{}
		switch o.Phase {
		case mqttOutstandingOpen, mqttOutstandingFenced:
			if o.Bound != 0 {
				return fmt.Errorf("outstanding generation %d carries a bound before its bounded phase", o.Ord)
			}
		case mqttOutstandingBounded:
			if o.Bound == 0 {
				return fmt.Errorf("bounded generation %d carries no bound", o.Ord)
			}
		default:
			return fmt.Errorf("outstanding generation %d carries an unsupported phase %d", o.Ord, o.Phase)
		}
		if o.Phase != mqttOutstandingOpen && o.Teardown.Pending {
			return fmt.Errorf("generation %d is fenced with its teardown release pending", o.Ord)
		}
		switch o.Teardown.Kind {
		case mqttTeardownMarker, mqttTeardownAbandon:
			if !mqttOutboundIdentTk(o.Teardown.Nonce) || !mqttOutboundIdentTk(o.Teardown.MarkerWID) {
				return fmt.Errorf("generation %d's marker teardown names no marker", o.Ord)
			}
			if o.Teardown.Displaced && o.Teardown.Pending {
				return fmt.Errorf("generation %d's displaced marker is still pending release", o.Ord)
			}
		case mqttTeardownHandoffDiscard:
			if o.Teardown.Pending || o.Teardown.Displaced || o.Teardown.Nonce != _EMPTY_ || o.Teardown.MarkerWID != _EMPTY_ {
				return fmt.Errorf("generation %d's handoff discard carries marker state", o.Ord)
			}
		default:
			return fmt.Errorf("generation %d carries an unsupported teardown kind %d", o.Ord, o.Teardown.Kind)
		}
	}
	if len(anchors) > mqttMaxWillAnchors {
		return fmt.Errorf("the lifecycle holds %d Will anchors, beyond its %d", len(anchors), mqttMaxWillAnchors)
	}
	aseen := make(map[uint64]struct{}, len(anchors))
	for _, a := range anchors {
		if a.Ord >= nextAnchorOrd {
			return fmt.Errorf("the Will anchor %d was never allocated (next %d)", a.Ord, nextAnchorOrd)
		}
		if _, dup := aseen[a.Ord]; dup {
			return fmt.Errorf("the Will anchor %d is listed twice", a.Ord)
		}
		aseen[a.Ord] = struct{}{}
		if a.AcceptOrd >= nextAcceptOrd {
			return fmt.Errorf("the Will anchor %d names acceptance %d, never allocated", a.Ord, a.AcceptOrd)
		}
		switch a.State {
		case mqttAnchorArmed:
			if a.FireAt != 0 {
				return fmt.Errorf("an armed Will anchor %d carries a fire time", a.Ord)
			}
		case mqttAnchorOwed:
			if a.FireAt == 0 {
				return fmt.Errorf("an owed Will anchor %d carries no fire time", a.Ord)
			}
		default:
			return fmt.Errorf("the Will anchor %d carries an unsupported state %d", a.Ord, a.State)
		}
	}
	return nil
}

// mqttLifecycleState is the lifecycle section of a client's authority: one member, so the
// whole of what reclamation adds to the authority is validated and decoded as a unit.
type mqttLifecycleState struct {
	L             uint64                 `json:"l"`
	Phase         mqttLifecyclePhase     `json:"phase"`
	NextGenOrd    uint64                 `json:"next_gen"`
	GenAcqs       bool                   `json:"gen_acqs,omitempty"`
	Outstanding   []mqttOutstandingGen   `json:"outstanding,omitempty"`
	Accept        mqttAcceptance         `json:"accept"`
	NextAcceptOrd uint64                 `json:"next_accept"`
	Pending       *mqttPendingAcceptance `json:"pending,omitempty"`
	Anchors       []mqttWillAnchor       `json:"anchors,omitempty"`
	NextAnchorOrd uint64                 `json:"next_anchor"`
}

var mqttLifecycleStateMembers = map[string]struct{}{
	"l": {}, "phase": {}, "next_gen": {}, "gen_acqs": {}, "outstanding": {}, "accept": {},
	"next_accept": {}, "pending": {}, "anchors": {}, "next_anchor": {},
}

var mqttLifecycleStateRequired = map[string]struct{}{
	"l": {}, "phase": {}, "next_gen": {}, "accept": {}, "next_accept": {}, "next_anchor": {},
}

// mqttValidateLifecycleState refuses a lifecycle section this broker would not write. gen and
// serving are the authority's current generation and whether it is current: a current
// generation is one this lifecycle allocated, and is never also outstanding.
func mqttValidateLifecycleState(s *mqttLifecycleState, gen string, serving bool) error {
	if s == nil {
		return fmt.Errorf("the authority carries no lifecycle section")
	}
	if s.L == 0 || s.L > mqttMaxLifecycle {
		return fmt.Errorf("the authority names lifecycle %d outside the token range", s.L)
	}
	switch s.Phase {
	case mqttLifecycleServing, mqttLifecycleClosed, mqttLifecycleRetiring:
	default:
		return fmt.Errorf("the authority carries an unsupported lifecycle phase %d", s.Phase)
	}
	if s.NextGenOrd == 0 || s.NextAcceptOrd == 0 {
		return fmt.Errorf("the authority's allocators are unset")
	}
	if s.Accept.Ord >= s.NextAcceptOrd || !mqttOutboundIdentTk(s.Accept.Tenure) ||
		!mqttOutboundIdentTk(s.Accept.Nonce) || s.Accept.At == 0 {
		return fmt.Errorf("the authority's current acceptance is malformed")
	}
	if p := s.Pending; p != nil {
		if p.AcceptOrd != s.Accept.Ord || p.Nonce != s.Accept.Nonce {
			return fmt.Errorf("the pending acceptance %d is not the current one %d", p.AcceptOrd, s.Accept.Ord)
		}
		switch p.Kind {
		case mqttPendingFresh:
			if p.Prior != nil {
				return fmt.Errorf("a fresh pending acceptance carries a prior lifetime")
			}
		case mqttPendingResume:
			if p.Prior == nil {
				return fmt.Errorf("a resumed pending acceptance carries no prior lifetime")
			}
		default:
			return fmt.Errorf("the pending acceptance carries an unsupported kind %d", p.Kind)
		}
	}
	gl, gord, ok := mqttParseGenToken(gen)
	if !ok || gl != s.L || gord >= s.NextGenOrd {
		return fmt.Errorf("the authority's generation %q is not one lifecycle %d allocated", gen, s.L)
	}
	if err := mqttValidateLifecycleLists(s.Outstanding, s.NextGenOrd, s.Anchors, s.NextAnchorOrd, s.NextAcceptOrd); err != nil {
		return err
	}
	for _, o := range s.Outstanding {
		if serving && o.Ord == gord {
			return fmt.Errorf("the current generation %d is also outstanding", gord)
		}
	}
	if serving && s.Phase != mqttLifecycleServing {
		return fmt.Errorf("a lifecycle in phase %d has a current generation", s.Phase)
	}
	if s.Phase == mqttLifecycleRetiring && (len(s.Outstanding) != 0 || len(s.Anchors) != 0 || s.Pending != nil) {
		return fmt.Errorf("a retiring lifecycle still holds work")
	}
	return mqttCheckGenerationSlots(len(s.Outstanding), serving)
}

// mqttCheckLifecycleSection applies the closed schema to an authority's lifecycle section, if
// it carries one: the section's own members exactly, and every nested entry decoded with no
// member this broker does not write.
func mqttCheckLifecycleSection(authority []byte) error {
	raw := struct {
		Life json.RawMessage `json:"life"`
	}{}
	if err := json.Unmarshal(authority, &raw); err != nil {
		return err
	}
	if len(raw.Life) == 0 {
		return nil
	}
	if err := mqttJSONMembersAreExactAndUnique(raw.Life, mqttLifecycleStateMembers, "a lifecycle section"); err != nil {
		return err
	}
	if err := mqttJSONMembersArePresent(raw.Life, mqttLifecycleStateRequired, "a lifecycle section"); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw.Life))
	dec.DisallowUnknownFields()
	var s mqttLifecycleState
	if err := dec.Decode(&s); err != nil {
		return mqttShapeErr(fmt.Sprintf("a lifecycle section carries a member this broker does not write: %v", err))
	}
	return nil
}
