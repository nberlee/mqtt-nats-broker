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

// The generation-ledger SEAL a closing batch carries (F-105).
//
// This file used to enrol a retiring generation's shared group durables as per-generation
// cleanup resources, and to answer the deferred last-holder question for them with an
// account-wide walk of the session records. Both are gone with the shared-group lifecycle
// transfer: a group durable is held by ARBITRATION on its own group record, and a
// generation's shared acquisitions are released by the closing from the generation's own
// acquisition ledger (shared_lifecycle_ops.go). A per-generation resource could never express
// a consumer several generations hold, and a walk that decides by absence across a record set
// could never fence a concurrent join.
//
// What survives is the one thing the closing composer needs from here and that has nothing to
// do with shared durables: the ledger member that SEALS a generation's inventory as the same
// batch closes it.

// mqttSealGenLedgerMember composes the ledger member that seals generation gen's inventory at
// its standing high-water.
//
// cur is that generation's standing ledger, nil when it has none, and curSeq the sequence it was
// read at. The member SEALS in the one act, because that is what makes the enrolment's
// completeness establishable at all: after the seal no further ordinal can be taken, so the set
// of duties naming this generation is fixed by the same batch that wrote them. An already-sealed
// ledger is refused: the seal is the closing act, and a second seal would be a second closing.
func mqttSealGenLedgerMember(id, epoch, gen string, cur *mqttGenLedger, curSeq uint64) (mqttTransitionMember, error) {
	if !mqttValidSessGen(gen) {
		return mqttTransitionMember{}, fmt.Errorf("sealing the inventory of malformed generation %q", gen)
	}
	// THE SEAL CARRIES THE FLOOR AND THE WATERMARK UNCHANGED (F-112): the floor freezes with the
	// shape, and deletions already authorized below it still finish.
	hi, lo, del := uint64(0), uint64(1), uint64(1)
	if cur != nil {
		lo, del = cur.Lo, cur.Del
		if cur.State == mqttGenLedgerSealed {
			return mqttTransitionMember{}, fmt.Errorf("generation %q's inventory is already sealed at %d, so no "+
				"closing batch can seal it again", gen, cur.SealedHi)
		}
		hi = cur.Hi
	}
	if cur == nil && curSeq != 0 {
		return mqttTransitionMember{}, fmt.Errorf("sealing generation %q against sequence %d with no ledger "+
			"standing there", gen, curSeq)
	}
	if cur != nil && curSeq == 0 {
		return mqttTransitionMember{}, fmt.Errorf("sealing generation %q requires the sequence its ledger was "+
			"read at", gen)
	}
	next := &mqttGenLedger{
		ID: id, Epoch: epoch, Gen: gen, State: mqttGenLedgerSealed, Hi: hi, SealedHi: hi,
		Lo: lo, Del: del,
	}
	body, err := json.Marshal(next)
	if err != nil {
		return mqttTransitionMember{}, err
	}
	return mqttTransitionMember{subj: mqttGenLedgerSubject(getHash(id), gen), expect: curSeq, body: body}, nil
}
