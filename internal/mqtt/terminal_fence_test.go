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

import "testing"

// The authority-free advance is fenced by terminal state (issue #133): no transition can be
// composed from an exchange retirement left terminal, whatever phase it is asked to reach. This
// is the composer-level arm, isolated from the write boundary, which refuses the same move
// again (checkExchangeOverwrites). Mutation M20: a composer that accepts a terminal target.
func TestNoAdvanceIsComposedFromATerminalExchange(t *testing.T) {
	prev := testTransition().Prev
	terminal := 0
	for _, qos := range []byte{1, 2} {
		for from := mqttExchangePhase(1); from < 64; from++ {
			if !mqttValidExchangePhase(from) || !mqttExchangeDeliveryDutyEnded(from) {
				continue
			}
			terminal++
			for to := mqttExchangePhase(1); to < 64; to++ {
				cur := &mqttOutboundExchange{
					ID: testID, Epoch: testEpoch, Gen: testGen, Nonce: "nonce-terminal", PI: 7, QoS: qos,
					Phase: from, AdmitTxn: "txn-admit", Tenure: "tenure-0",
				}
				if mqttValidateOutboundExchange(cur, testID, testEpoch, testGen, cur.Nonce) != nil {
					continue // a phase this QoS never stands at
				}
				if tr, err := mqttAdvanceExchangeTransition(prev, 0, "txn-late", "op-late", cur, 3, to); err == nil {
					t.Fatalf("an advance of a QoS %d exchange from terminal phase %d to %d was composed: %+v",
						qos, from, to, tr.Attempt)
				}
			}
		}
	}
	if terminal == 0 {
		t.Fatal("fixture: no terminal phase was exercised")
	}
}
