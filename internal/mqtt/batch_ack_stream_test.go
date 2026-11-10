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
	"strings"
	"testing"
)

// TestBatchAcksMustNameTheSessionStream pins that an authority transition and an in-flight
// pair write accept an acknowledgement only from the stream their records live on, now that
// the in-flight space is part of $MQTT_sess.
//
// Each row varies the stream alone, with the batch id and member count right, so a check
// that compared the stream against nothing (or against the former $MQTT_inflight) could not
// pass through the batch-id refusal instead. Driven on the checks themselves, because the
// server answers on the stream it stored to, so no live send can produce a foreign one.
func TestBatchAcksMustNameTheSessionStream(t *testing.T) {
	t.Parallel()
	ack := func(t *testing.T, js string) *JSPubAckResponse {
		t.Helper()
		r := &JSPubAckResponse{}
		if err := json.Unmarshal([]byte(js), r); err != nil {
			t.Fatalf("decoding the acknowledgement: %v", err)
		}
		return r
	}
	const sent = "batch-we-sent"
	on := func(stream string, count int) string {
		b, _ := json.Marshal(map[string]any{"stream": stream, "seq": 9, "batch": sent, "count": count})
		return string(b)
	}

	t.Run("an authority transition", func(t *testing.T) {
		t.Parallel()
		for _, stream := range []string{"OTHER", mqttRetiredInflightStreamName} {
			err := mqttCheckTransitionBatchAck(ack(t, on(stream, 3)), sent, 3, "txn-x")
			if err == nil || !strings.Contains(err.Error(), "acknowledged on stream") {
				t.Fatalf("an acknowledgement on %s was not refused for its stream: %v", stream, err)
			}
		}
		if err := mqttCheckTransitionBatchAck(ack(t, on(mqttSessStreamName, 3)), sent, 3, "txn-x"); err != nil {
			t.Fatalf("an acknowledgement naming this batch on %s was refused: %v", mqttSessStreamName, err)
		}
	})

	t.Run("an in-flight pair", func(t *testing.T) {
		t.Parallel()
		for _, stream := range []string{"OTHER", mqttRetiredInflightStreamName} {
			err := mqttCheckInflightPairBatchAck(ack(t, on(stream, mqttInflightPairBatchSize)), sent, "subj")
			if err == nil || !strings.Contains(err.Error(), "acknowledged on stream") {
				t.Fatalf("an acknowledgement on %s was not refused for its stream: %v", stream, err)
			}
		}
		if err := mqttCheckInflightPairBatchAck(ack(t, on(mqttSessStreamName, mqttInflightPairBatchSize)), sent, "subj"); err != nil {
			t.Fatalf("an acknowledgement naming this batch on %s was refused: %v", mqttSessStreamName, err)
		}
	})
}
