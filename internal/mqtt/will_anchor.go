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
)

// Will anchors (issue #133, plan 5.3).
//
// A delayed Will is suppressed by a timely reconnect: an acceptance of the same client before
// the Will's fire time. The acceptance facts answered that by being scanned, and could never be
// reclaimed because a deleted fact reads exactly like no reconnect. An anchor answers it from
// one record instead. Every delay-capable connection's acceptance allocates an anchor in its own
// batch, and every LATER acceptance lowers the anchor's minAt to its own time in its batch, so
// the claimant decides from one read: suppressed iff some later acceptance came before FireAt.
//
// The anchor is allocated ARMED at acceptance, not at close. Allocating it at close would lose
// an acceptance that arrived between the close and the allocation, would let the lifecycle
// retire under a broker still holding the Will, and would spend capacity no admission reserved.

const (
	// mqttTransitionWillAnchor changes exactly one Will anchor and nothing else. It is the one
	// lifecycle transition that needs no ownership of the session: the broker that holds a Will's
	// disposition completes its anchor whatever has since happened to the generation or tenure.
	mqttTransitionWillAnchor mqttTransitionKind = "will-anchor"
)

// mqttAnchorOp is what a will-anchor transition does to its anchor.
type mqttAnchorOp uint8

const (
	// mqttAnchorOwe: ARMED to OWED, with the Will's fire time.
	mqttAnchorOwe mqttAnchorOp = iota + 1
	// mqttAnchorRemove: the anchor's Will will never be published (discarded, immediate, refused
	// before CONNACK, or published by the fail-safe).
	mqttAnchorRemove
)

// mqttWillToken is THE outbox identity of the Will an anchor protects: deterministic, so every
// attempt to enqueue that Will writes the same subject, and a record that lands after its
// anchor's discharge is recognised as the anchor's.
func mqttWillToken(l, anchorOrd uint64) (string, error) { return mqttGenToken(l, anchorOrd) }

// mqttParseWillToken is mqttWillToken's inverse.
func mqttParseWillToken(willID string) (l, anchorOrd uint64, ok bool) {
	return mqttParseGenToken(willID)
}

// mqttAnchorAt finds an anchor by ordinal.
func mqttAnchorAt(anchors []mqttWillAnchor, ord uint64) (int, bool) {
	for i, a := range anchors {
		if a.Ord == ord {
			return i, true
		}
	}
	return -1, false
}

// mqttAcceptAnchors is the anchor list an acceptance at time at, ordinal acceptOrd, installs:
// every standing anchor's minAt lowered to at (all of them belong to earlier acceptances), and,
// for a delay-capable connection, a new ARMED anchor of its own.
func mqttAcceptAnchors(pl *mqttLifecycleState, acceptOrd uint64, at int64, delayCapable bool) ([]mqttWillAnchor, uint64, *mqttWillAnchor, error) {
	var anchors []mqttWillAnchor
	next := uint64(1)
	if pl != nil {
		next = pl.NextAnchorOrd
		for _, a := range pl.Anchors {
			if a.MinAt == 0 || at < a.MinAt {
				a.MinAt = at
			}
			anchors = append(anchors, a)
		}
	}
	if !delayCapable {
		return anchors, next, nil, nil
	}
	if len(anchors) >= mqttMaxWillAnchors {
		return nil, 0, nil, fmt.Errorf("the lifecycle holds %d Will anchors, its limit", len(anchors))
	}
	own := mqttWillAnchor{Ord: next, AcceptOrd: acceptOrd, State: mqttAnchorArmed}
	anchors = append(anchors, own)
	return anchors, next + 1, &own, nil
}

// mqttCheckAcceptAnchors is the boundary's half of mqttAcceptAnchors: the installed list is the
// predecessor's with minAt lowered exactly to the acceptance's time, plus at most one new ARMED
// anchor of this acceptance.
func mqttCheckAcceptAnchors(pl, nl *mqttLifecycleState) error {
	var prev []mqttWillAnchor
	prevNext := uint64(1)
	if pl != nil {
		prev, prevNext = pl.Anchors, pl.NextAnchorOrd
	}
	want, _, _, err := mqttAcceptAnchors(pl, nl.Accept.Ord, nl.Accept.At, false)
	if err != nil {
		return err
	}
	extra := len(nl.Anchors) - len(prev)
	switch {
	case extra == 0 && nl.NextAnchorOrd == prevNext:
	case extra == 1 && nl.NextAnchorOrd == prevNext+1:
		own := nl.Anchors[len(nl.Anchors)-1]
		if own != (mqttWillAnchor{Ord: prevNext, AcceptOrd: nl.Accept.Ord, State: mqttAnchorArmed}) {
			return fmt.Errorf("an acceptance allocates anchor %+v, not an armed anchor of its own", own)
		}
	default:
		return fmt.Errorf("an acceptance changes the anchor list from %d to %d entries", len(prev), len(nl.Anchors))
	}
	for i, w := range want {
		if nl.Anchors[i] != w {
			return fmt.Errorf("an acceptance installs anchor %+v where its predecessor's lowered is %+v", nl.Anchors[i], w)
		}
	}
	return nil
}

// mqttComposeAnchorTransition changes one anchor of prev's lifecycle.
func mqttComposeAnchorTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn string, anchorOrd uint64,
	op mqttAnchorOp, fireAt int64) (*mqttAuthorityTransition, error) {

	if prev == nil || prev.Life == nil {
		return nil, fmt.Errorf("no lifecycle holds Will anchor %d", anchorOrd)
	}
	if prev.Life.Phase == mqttLifecycleRetiring {
		return nil, fmt.Errorf("the lifecycle holding Will anchor %d is retiring", anchorOrd)
	}
	i, ok := mqttAnchorAt(prev.Life.Anchors, anchorOrd)
	if !ok {
		return nil, fmt.Errorf("the Will anchor %d is not standing", anchorOrd)
	}
	nl := *prev.Life
	nl.Anchors = append([]mqttWillAnchor(nil), prev.Life.Anchors...)
	switch op {
	case mqttAnchorOwe:
		if nl.Anchors[i].State != mqttAnchorArmed || fireAt == 0 {
			return nil, fmt.Errorf("the Will anchor %d is not armed, or no fire time was given", anchorOrd)
		}
		nl.Anchors[i].State, nl.Anchors[i].FireAt = mqttAnchorOwed, fireAt
	case mqttAnchorRemove:
		nl.Anchors = append(nl.Anchors[:i], nl.Anchors[i+1:]...)
		if len(nl.Anchors) == 0 {
			nl.Anchors = nil
		}
	default:
		return nil, fmt.Errorf("unsupported anchor operation %d", op)
	}
	next := *prev
	next.Life, next.Rev, next.Txn = &nl, prev.Rev+1, txn
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Scope: mqttLifeScope(prev.Life), Op: "op-" + txn,
			Kind:    mqttTransitionWillAnchor,
			PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1, Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq, Auth: &next,
	}, nil
}

// mqttCheckAnchorTransitionRules is the boundary's rule for will-anchor: everything but the
// revision, the producing transition and ONE anchor is the predecessor's, and that anchor moved
// ARMED to OWED with a fire time, or was removed.
func mqttCheckAnchorTransitionRules(t *mqttAuthorityTransition) error {
	prev, next := t.Prev, t.Auth
	if prev == nil || prev.Life == nil || next == nil || next.Life == nil {
		return fmt.Errorf("a will-anchor transition has no lifecycle on both sides")
	}
	if prev.Life.Phase == mqttLifecycleRetiring {
		return fmt.Errorf("a will-anchor transition writes a retiring lifecycle")
	}
	if t.Closure != nil || len(t.Extra) != 0 {
		return fmt.Errorf("a will-anchor transition carries members")
	}
	pa, na := prev.Life.Anchors, next.Life.Anchors
	changed := -1
	switch {
	case len(na) == len(pa):
		for i := range pa {
			if pa[i] != na[i] {
				if changed != -1 {
					return fmt.Errorf("a will-anchor transition changes more than one anchor")
				}
				changed = i
			}
		}
		if changed == -1 {
			return fmt.Errorf("a will-anchor transition changes no anchor")
		}
		p, n := pa[changed], na[changed]
		if p.State != mqttAnchorArmed || n.State != mqttAnchorOwed || n.FireAt == 0 ||
			n.Ord != p.Ord || n.AcceptOrd != p.AcceptOrd || n.MinAt != p.MinAt {
			return fmt.Errorf("a will-anchor transition moves anchor %+v to %+v", p, n)
		}
	case len(na) == len(pa)-1:
		for i, j := 0, 0; i < len(pa); i++ {
			if j < len(na) && pa[i] == na[j] {
				j++
				continue
			}
			if changed != -1 {
				return fmt.Errorf("a will-anchor transition removes more than one anchor")
			}
			changed = i
		}
	default:
		return fmt.Errorf("a will-anchor transition changes the anchor count from %d to %d", len(pa), len(na))
	}
	want := *prev
	wl := *prev.Life
	wl.Anchors = na
	want.Life, want.Rev, want.Txn = &wl, t.Attempt.Rev, t.Attempt.Txn
	got, _ := json.Marshal(next)
	exp, _ := json.Marshal(&want)
	if string(got) != string(exp) {
		return fmt.Errorf("a will-anchor transition changes more than its anchor")
	}
	return nil
}
