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
	"time"
)

// A FROZEN BATCH IS RE-SENT ONLY WHILE WHAT IT EXPECTS STILL STANDS (issue #186).
//
// Two batch families carry a member on a subject OTHER CLIENTS write: every closing batch (the
// teardown claim, the Clean Start handoff) carries its directory shard, whose header any closing in
// the shard and every broker's recovery pass move, and a creation carries its lifecycle registry
// shard, which every creation and lifecycle retirement in the shard moves. Neither subject is ever
// deleted or purged, so a moved shard never returns to its expectation: a frozen batch expecting it
// can never land, and re-sending it only spends the allowance on identical refusals.
//
// Movement proves the future, not the past. Once a shared member reads moved, the frozen batch
// cannot commit AFTER that read, but an earlier ambiguous send may have committed BEFORE it. So a
// resolver that sees movement re-reads the batch's landed witness after the shared read — the
// authority's acceptance nonce, the record's marker nonce — and recomposes only on a READABLE
// witness that is not ours. An unreadable witness licenses nothing.
//
// Per-client closing members (the ledger, the root, the affine enrolment) move only with the
// client's authority, which the resolvers already re-read. The session record does not, and the
// resolvers keep their own record re-read for it.

// sharedMembersMoved reports whether a member the frozen batch t expects on a subject other clients
// write — the directory shard of a closing, the lifecycle registry shard of a creation — no longer
// stands at its expectation. It reads the stored subjects through this view, never a send's
// prefetch, so the answer is never the header the batch was composed against.
func (jsa *mqttJSA) sharedMembersMoved(t *mqttAuthorityTransition) (bool, error) {
	var shared []mqttTransitionMember
	if t.Registry != nil {
		shared = append(shared, *t.Registry)
	}
	for _, e := range t.Extra {
		if mqttIsShardSubject(e.subj) {
			shared = append(shared, e)
		}
	}
	for _, m := range shared {
		var stored uint64
		smsg, err := jsa.loadLastMsgFor(mqttSessStreamName, m.subj)
		switch {
		case err != nil && isErrorOtherThan(err, JSNoMessageFoundErr):
			return false, fmt.Errorf("reading %q to resolve a refused batch: %w", m.subj, err)
		case err == nil && smsg != nil:
			stored = smsg.Sequence
		}
		if stored != m.expect {
			return true, nil
		}
	}
	return false, nil
}

// mqttSharedShardAllowance is one foreground act's patience with shared-shard movement: a refusal
// caused only by another client's write (or a recovery pass) on a shared shard spends no counted
// attempt while it stands, because the counted budgets were derived from this client's own writers
// and do not count anyone else's. Once it is spent such a refusal counts again, so every loop still
// ends. Same shape as a closing's contention allowance (issue #178).
type mqttSharedShardAllowance struct {
	deadline time.Time
}

// mqttTestSharedShardAllowanceV, when positive, replaces the allowance.
var mqttTestSharedShardAllowanceV = new(mqttKnob[time.Duration])

// mqttTestSharedShardClockV, when set, replaces the allowance's clock.
var mqttTestSharedShardClockV = new(mqttKnob[func() time.Time])

func mqttSharedShardNow() time.Time {
	if now := mqttTestSharedShardClockV.Get(); now != nil {
		return now()
	}
	return time.Now()
}

// newMqttSharedShardAllowance starts an allowance of one JetStream API request's time.
func newMqttSharedShardAllowance() *mqttSharedShardAllowance {
	d := time.Duration(mqttDefaultJSAPITimeout)
	if t := mqttTestSharedShardAllowanceV.Get(); t > 0 {
		d = t
	}
	return &mqttSharedShardAllowance{deadline: mqttSharedShardNow().Add(d)}
}

// absorbs reports whether a shared-shard refusal is still free of a counted attempt.
func (a *mqttSharedShardAllowance) absorbs() bool {
	return a != nil && mqttSharedShardNow().Before(a.deadline)
}
