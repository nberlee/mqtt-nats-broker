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
	"fmt"
	"strings"
)

// The outcome scope is checked against what the batch writes (issue #133).
//
// A generation-scoped outcome is reclaimed with its generation's records, so it must be ONE
// generation's: a batch writing another generation's records under it would leave a receipt
// that survives, or dies with, the wrong records. A lifecycle-scoped outcome outlives every
// generation of its lifecycle, so its members may be any generation of THAT lifecycle (the
// clean-start handoff's closing members are the predecessor's), and of no other.

// mqttLifeScope is the lifecycle outcome scope of an authority's lifecycle, or empty when it
// has none, which the attempt validation refuses: a composer never invents a scope.
func mqttLifeScope(life *mqttLifecycleState) string {
	if life == nil {
		return _EMPTY_
	}
	scope, err := mqttLifecycleScope(life.L)
	if err != nil {
		return _EMPTY_
	}
	return scope
}

// mqttMemberGeneration returns the generation a member subject is attributed to, and whether
// it is attributed to one at all. Every outbound record class is named ob.<cls>.<idHash>.<gen>
// except the ones listed here, which are per client, per shard or per physical name; the
// in-flight register and its marker carry the generation in their own name.
func mqttMemberGeneration(subj string) (string, bool, error) {
	if mqttIsOutboundRecordSubject(subj) {
		toks := strings.Split(subj[len(mqttOutboundRecordSubjectPrefix()):], ".")
		switch toks[0] {
		case mqttAuthorityClassTk, mqttLifecycleRegistryClassTk, mqttShardClassTk, mqttRootClassTk,
			mqttNameOwnClassTk, mqttOutcomeClassTk:
			return _EMPTY_, false, nil
		}
		if len(toks) < 3 {
			return _EMPTY_, false, fmt.Errorf("member %q names no generation", subj)
		}
		if _, _, ok := mqttParseGenToken(toks[2]); !ok {
			return _EMPTY_, false, fmt.Errorf("member %q names the malformed generation %q", subj, toks[2])
		}
		return toks[2], true, nil
	}
	if name, err := mqttParseInflightSubject(subj); err == nil {
		return name.gen, true, nil
	}
	if name, err := mqttParseInflightMarkerSubject(subj); err == nil {
		return name.gen, true, nil
	}
	return _EMPTY_, false, nil
}

// mqttCheckOutcomeScopeMembers refuses a transition whose outcome scope disagrees with the
// authority it writes or with any generation-attributed record it carries.
func mqttCheckOutcomeScopeMembers(t *mqttAuthorityTransition, members []mqttTransitionMember) error {
	a := t.Attempt
	scopeL, scopeGen, ok := mqttParseOutcomeScope(a.Scope)
	if !ok {
		return fmt.Errorf("transition %q carries a malformed outcome scope %q", a.Txn, a.Scope)
	}
	// THE SCOPE IS THE AUTHORITY'S LIFECYCLE. A client's lifecycle changes only by retiring its
	// authority, so no transition writing this authority is about another lifecycle's records.
	if t.Auth != nil {
		if t.Auth.Life == nil {
			return fmt.Errorf("transition %q writes an authority with no lifecycle, which no outcome "+
				"scope can name", a.Txn)
		}
		if t.Auth.Life.L != scopeL {
			return fmt.Errorf("transition %q is scoped to lifecycle %d and writes the authority of "+
				"lifecycle %d", a.Txn, scopeL, t.Auth.Life.L)
		}
	}
	for _, e := range members {
		gen, attributed, err := mqttMemberGeneration(e.subj)
		if err != nil {
			return fmt.Errorf("transition %q: %w", a.Txn, err)
		}
		if !attributed {
			continue
		}
		if scopeGen != _EMPTY_ {
			if gen != scopeGen {
				return fmt.Errorf("transition %q is scoped to generation %q and carries member %q of "+
					"generation %q", a.Txn, scopeGen, e.subj, gen)
			}
			continue
		}
		if l, _, _ := mqttParseGenToken(gen); l != scopeL {
			return fmt.Errorf("transition %q is scoped to lifecycle %d and carries member %q of "+
				"another lifecycle's generation %q", a.Txn, scopeL, e.subj, gen)
		}
	}
	return nil
}
