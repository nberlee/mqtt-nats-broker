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
	"errors"
	"testing"
)

// What a closing may treat as contention is decided over the write boundary's ACTUAL refusal arms
// and the store's real CAS errors, never over errors a test fabricates in their likeness.
func TestSharedContentionIsOnlyAStalePriorOrACASRejection(t *testing.T) {
	rejected := func(err error) mqttSharedWriteResult {
		return mqttSharedWriteResult{Outcome: mqttSharedWriteRejected, Err: err}
	}
	cases := []struct {
		name string
		res  mqttSharedWriteResult
		want bool
	}{
		{"prior vanished", rejected(mqttSharedPriorMatches(mqttOutboundAbsent, 0, true, 7, "ledger")), true},
		{"prior appeared", rejected(mqttSharedPriorMatches(mqttOutboundValid, 8, false, 0, "ledger")), true},
		{"prior moved", rejected(mqttSharedPriorMatches(mqttOutboundValid, 8, true, 7, "ledger")), true},
		{"prior corrupt", rejected(mqttSharedPriorMatches(mqttOutboundCorrupt, 8, true, 7, "ledger")), false},
		{"prior indeterminate", rejected(mqttSharedPriorMatches(mqttOutboundIndeterminate, 0, true, 7, "ledger")), false},
		{"10164", rejected(ApiErrors[JSStreamWrongLastSequenceConstantErr]), true},
		{"10071", rejected(ApiErrors[JSStreamWrongLastSequenceErrF]), true},
		{"other store refusal", rejected(ApiErrors[JSStreamNotFoundErr]), false},
		{"ambiguous 10164", mqttSharedWriteResult{Outcome: mqttSharedWriteAmbiguous, Err: ApiErrors[JSStreamWrongLastSequenceConstantErr]}, false},
		{"refused shape", mqttSharedWriteResult{Err: errors.New("refusing to send an unsupported batch")}, false},
		{"landed", mqttSharedWriteResult{Outcome: mqttSharedWriteLanded}, false},
	}
	for _, tc := range cases {
		if got := mqttIsSharedContention(tc.res); got != tc.want {
			t.Errorf("%s: contention %v, want %v (%v)", tc.name, got, tc.want, tc.res.Err)
		}
	}
}
