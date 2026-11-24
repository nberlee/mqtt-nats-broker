//go:build mqtt_fulltests && mqtt_experimental_shared_qos2

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
	"strings"
	"testing"
)

// TestExperimentalAffineCreateUnansweredCloses: the session-affine QoS 2 path a shared QoS 2
// filter provisions has its consumer create go unanswered; the filter's refusal is classified in
// the first pass's window and the packet closes (sub_jetstream_unanswered.go).
func TestExperimentalAffineCreateUnansweredCloses(t *testing.T) {
	testUnansweredClosesV5(t, "unanswered-affine", []mqttV5SubFilter{{topic: "$share/ga/affine/topic", opts: 2}},
		func(_ *mqttAccountSessionManager, sess *mqttSession) func(string, []byte) bool {
			prefix := fmt.Sprintf(JSApiDurableCreateT, mqttOutStreamName, _EMPTY_)
			return func(subject string, body []byte) bool {
				if !strings.HasPrefix(subject, prefix) {
					return false
				}
				var req CreateConsumerRequest
				if json.Unmarshal(body, &req) != nil {
					return false
				}
				// The path's generation is minted by this provisioning, so the durable is matched
				// by the session's affine prefix.
				return strings.HasPrefix(req.Config.Durable, mqttQ2AffineConsumerDurablePrefix+sess.idHash+"_")
			}
		})
}
