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

// An ending releases its holds in its own batch (F-105 7c-lc).
//
// An admitted obligation takes an edge on its source consumer (and, at QoS 2, on the canonical
// PUBREL consumer), and that edge is what keeps the consumer from being retired while the
// obligation may still need it. The edge is discharged by the obligation's ENDING — terminal,
// rejected, superseded unsent, withdrawn unsent — and by nothing else, so the release rides the
// batch that writes the ending. Committed apart, a crash between the two would leave an ended
// obligation pinning a consumer that no later act is entitled to free; committed together, a lost
// response leaves both or neither, and the caller resolves it by reading the exchange.
//
// The write boundary enforces it in both directions (checkResourceOverwrites): an ending without
// its releases is refused, and a hold release without its ending is refused. A producer this file
// is not called from is therefore refused rather than silently leaking, which is the point of
// stating it as a property.
//
// An abandonment is NOT an ending (mqttExchangePhaseEnded excludes it) and releases nothing; a
// pending-drain phase releases nothing either — the drain completion that moves it to terminal
// carries the release.

// appendHoldReleases adds to tr the release of every hold x has, for an ending of x to next.
//
// MERGED, not appended, where the transition already proposes the resource: a pair replacement
// adds its fresh obligation's edge to the same source resource the superseded predecessor holds,
// and a second member on one subject is refused at the boundary. The release then edits THAT
// member's proposed body — its additions and its expectation preserved — and a new member is
// appended only for a resource the transition does not already carry.
//
// A held resource that cannot be read makes the ending uncomposable: the caller's own retry
// applies, as for an unreadable exchange.
func (jsa *mqttJSA) appendHoldReleases(tr *mqttAuthorityTransition, x *mqttOutboundExchange,
	next mqttExchangePhase) error {

	if tr == nil || x == nil || len(x.Holds) == 0 {
		return nil
	}
	if !mqttExchangeReleasesHolds(next) {
		return nil
	}
	idHash := getHash(x.ID)
	loaded := jsa.loadHeldResources(tr, x)
	for _, h := range x.Holds {
		subj := mqttResourceSubject(idHash, x.Gen, h.Inc)
		edge := mqttResourceEdge{Nonce: x.Nonce, Role: h.Role}
		merged := false
		for i := range tr.Extra {
			if tr.Extra[i].subj != subj {
				continue
			}
			proposed, derr := mqttDecodeOutboundResource(tr.Extra[i].body, x.ID, x.Epoch, x.Gen, h.Inc)
			if derr != nil {
				return fmt.Errorf("releasing obligation %q's hold on %q: the transition's own member "+
					"is unsupported: %w", x.Nonce, h.Inc, derr)
			}
			rel, rerr := mqttResourceReleaseDependency(proposed, edge)
			if rerr != nil {
				return fmt.Errorf("releasing obligation %q's hold on %q: %w", x.Nonce, h.Inc, rerr)
			}
			body, merr := json.Marshal(rel)
			if merr != nil {
				return merr
			}
			tr.Extra[i].body = body
			merged = true
			break
		}
		if merged {
			continue
		}
		ld := loaded[subj]
		cur, curSeq, lerr := ld.res, ld.seq, ld.err
		if lerr != nil {
			return fmt.Errorf("releasing obligation %q's hold on %q: %w", x.Nonce, h.Inc, lerr)
		}
		if cur == nil {
			return fmt.Errorf("obligation %q holds resource %q, which does not stand: an obligation's "+
				"hold is never on an absent resource, and its ending is refused rather than composed "+
				"over one", x.Nonce, h.Inc)
		}
		rel, rerr := mqttResourceReleaseDependency(cur, edge)
		if rerr != nil {
			return fmt.Errorf("releasing obligation %q's hold on %q: %w", x.Nonce, h.Inc, rerr)
		}
		body, merr := json.Marshal(rel)
		if merr != nil {
			return merr
		}
		tr.Extra = append(tr.Extra, mqttTransitionMember{subj: subj, expect: curSeq, body: body})
	}
	return nil
}

// mqttHeldResourceRead is one held resource as appendHoldReleases reads it.
type mqttHeldResourceRead struct {
	res *mqttOutboundResource
	seq uint64
	err error
}

// loadHeldResources reads, ahead of the release loop, every held resource that loop will read:
// each hold whose subject no member of the transition (nor an earlier hold's appended member)
// already proposes. Two or more are read in ONE round, and the loop judges them in hold order as
// it always has, so a refusal names the same hold; an ending that refuses at its first resource
// has read the rest (docs/ARCHITECTURE.md, "Admission readiness in two rounds" states the cost
// rule). One is read alone, exactly as before.
func (jsa *mqttJSA) loadHeldResources(tr *mqttAuthorityTransition, x *mqttOutboundExchange) map[string]mqttHeldResourceRead {
	idHash := getHash(x.ID)
	proposed := make(map[string]bool, len(tr.Extra)+len(x.Holds))
	for _, m := range tr.Extra {
		proposed[m.subj] = true
	}
	var incs, subjects []string
	for _, h := range x.Holds {
		subj := mqttResourceSubject(idHash, x.Gen, h.Inc)
		if proposed[subj] {
			continue
		}
		proposed[subj] = true
		incs = append(incs, h.Inc)
		subjects = append(subjects, subj)
	}
	out := make(map[string]mqttHeldResourceRead, len(subjects))
	if len(subjects) == 1 {
		res, seq, err := jsa.loadOutboundResource(x.ID, x.Epoch, x.Gen, incs[0])
		out[subjects[0]] = mqttHeldResourceRead{res, seq, err}
		return out
	}
	round := make([]mqttJSARoundRequest, len(subjects))
	for i, subj := range subjects {
		round[i] = mqttLastForRead(mqttInflightStreamName, subj)
	}
	resps, rerr := jsa.requestRound(round)
	for i, subj := range subjects {
		smsg, merr := mqttRoundMsgGet(resps, i, rerr)
		res, seq, err := mqttDecodeOutboundResourceRead(smsg, merr, x.ID, x.Epoch, x.Gen, incs[i])
		out[subj] = mqttHeldResourceRead{res, seq, err}
	}
	return out
}
