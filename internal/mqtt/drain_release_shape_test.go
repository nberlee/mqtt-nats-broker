// Copyright 2025 The mqtt-nats-bridge Authors
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
	"strings"
	"testing"
)

// AN INCOMPLETE RELEASE REQUEST REFUSES THE WHOLE RECORD (F-098 L4, holistic review round 5,
// finding 4). Every member of a persisted Rel is mandatory, and the one that typed decoding
// silently supplies is the transmission counter: an omitted `acq` arrives as zero, which is a
// REAL counter — a register never re-acquired carries it — so nothing downstream can tell the
// fabricated identity from a genuine one. Settlement would then read any real occupant as
// superseding it and consume the request without resolving its target.
//
// The refusal therefore has to happen where the difference between "absent" and "zero" still
// exists, and it refuses the RECORD, on F-057's precedent: a later save would otherwise
// normalize the missing field, which is migration.
//
// Mutation M127: decode the request without checking member presence.
func TestPersistedDrainReleaseRefusesAnIncompleteShape(t *testing.T) {
	t.Parallel()
	const subject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
	nonce := mqttNewInflightNonce()
	ps := &mqttPersistedSession{
		Drains: map[string]*mqttPersistedDrain{
			"drain/topic": {
				Cons: &ConsumerConfig{Durable: "$MQTT_abc"},
				Started: []mqttPersistedDrainSeq{{
					Seq: 41, PI: 7,
					Rel: &mqttPersistedDrainRelease{Subject: subject, Nonce: nonce, Acq: 7},
				}},
			},
		},
	}
	b, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshalling the fixture record: %v", err)
	}
	complete := string(b)
	if !strings.Contains(complete, `"acq":7`) {
		t.Fatalf("the fixture must carry an explicit counter to remove: %s", complete)
	}

	for _, tc := range []struct {
		name     string
		body     string
		wantNil  bool
		wantRel  bool
		wantAcq  uint64
		whyClaim string
	}{
		{
			name:     "an omitted counter refuses the record",
			body:     strings.Replace(complete, `,"acq":7`, "", 1),
			wantNil:  true,
			whyClaim: "an absent member names no transmission, and zero is not a substitute",
		},
		{
			name:     "an explicit null counter refuses the record",
			body:     strings.Replace(complete, `"acq":7`, `"acq":null`, 1),
			wantNil:  true,
			whyClaim: "null is absence spelled differently",
		},
		{
			name:     "an omitted subject refuses the record",
			body:     strings.Replace(complete, `"subject":"`+subject+`"`, `"subject":""`, 1),
			wantNil:  true,
			whyClaim: "a request with no target names no exchange",
		},
		{
			name:     "an explicit ZERO counter is accepted",
			body:     strings.Replace(complete, `"acq":7`, `"acq":0`, 1),
			wantRel:  true,
			wantAcq:  0,
			whyClaim: "zero is the counter of a register that was never re-acquired",
		},
		{
			name:    "the complete request decodes unchanged",
			body:    complete,
			wantRel: true,
			wantAcq: 7,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mqttDecodeSessionRecord(&StoredMsg{Data: []byte(tc.body)})
			if tc.wantNil {
				if got != nil {
					t.Fatalf("the record was accepted: %s", tc.whyClaim)
				}
				return
			}
			if got == nil {
				t.Fatal("a supported record was refused")
			}
			d := got.Drains["drain/topic"]
			if d == nil {
				t.Fatal("the decoded record lost its drain")
			}
			if len(d.Started) != 1 {
				t.Fatalf("the decoded drain carries %d started sequence(s), want 1", len(d.Started))
			}
			if d.Started[0].Rel == nil {
				t.Fatal("the decoded sequence lost its release request")
			}
			rel := d.Started[0].Rel
			if !rel.valid() || rel.Acq != tc.wantAcq || rel.Subject != subject || rel.Nonce != nonce {
				t.Fatalf("decoded request = %+v, want the fixture's at counter %d", rel, tc.wantAcq)
			}
		})
	}
}
