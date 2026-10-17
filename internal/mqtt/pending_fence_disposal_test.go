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

// The slot-only (experimental affine) class keeps its plain fence disposal: it has no authority to
// advance, needs no licence, and its cancellation durability is chapter P's (F-119). An ordinary
// fence with no store to advance against is KEPT, never disposed of by default.
func TestPendingFenceDisposalKeepsTheSlotOnlyClassPlain(t *testing.T) {
	sess := &mqttSession{}
	var decided bool
	h := func(string, bool, bool) { decided = true }
	mqttTestPendingFenceDisposalHookV.Store(&h)
	t.Cleanup(func() { mqttTestPendingFenceDisposalHookV.Store(nil) })
	affine := &mqttInflightRecord{Dur: mqttQ2AffineConsumerDurablePrefix + "x", Nonce: "n"}
	if !sess.fenceBeforePendingFenceDisposal("$MQTT.inflight.h.g.1", affine, mqttDestroyLicence{}, nil) {
		t.Fatal("a slot-only fence is disposed of with no licence and no advance")
	}
	if decided {
		t.Fatal("a slot-only fence must not reach the authority-class decision at all")
	}
	ordinary := &mqttInflightRecord{Dur: "ordinary", Nonce: "n"}
	if sess.fenceBeforePendingFenceDisposal("$MQTT.inflight.h.g.1", ordinary, mqttDestroyLicence{}, nil) {
		t.Fatal("an ordinary fence with no store to advance against must be kept")
	}
}
