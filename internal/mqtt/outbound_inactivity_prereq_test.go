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

//go:build mqttport_fulltests

package mqtt

import (
	"testing"
	"time"
)

// TestF105InactivityPrerequisite pins the F-105 §5.1 rule that a depended-on source/PUBREL
// consumer is safe only when its EFFECTIVE inactivity threshold is zero — both the stream's
// substituted limit and the consumer's own request — and that an unverifiable stream is unsafe.
// It exercises the pure decision core so no embedded JetStream is needed; the store wrapper is
// only the lookup drainingAllowed already models.
func TestF105InactivityPrerequisite(t *testing.T) {
	const nonzero = 30 * time.Second

	cases := []struct {
		name        string
		streamLimit time.Duration
		consumerReq time.Duration
		verified    bool
		wantSafe    bool
	}{
		{"clean stream, consumer requests none", 0, 0, true, true},
		{"stream imposes a limit", nonzero, 0, true, false},
		{"consumer requests a threshold, clean stream", 0, nonzero, true, false},
		{"both set", nonzero, nonzero, true, false},
		{"stream unverifiable", 0, 0, false, false},
		{"stream unverifiable even with a clean-looking limit arg", nonzero, 0, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mqttEffectiveInactivitySafe(tc.streamLimit, tc.consumerReq, tc.verified)
			if tc.wantSafe && err != nil {
				t.Fatalf("expected SAFE, got refusal: %v", err)
			}
			if !tc.wantSafe {
				if err == nil {
					t.Fatalf("expected a refusal (unsafe), got safe")
				}
				if _, ok := err.(mqttInactivityUnsafe); !ok {
					t.Fatalf("expected a mqttInactivityUnsafe refusal, got %T: %v", err, err)
				}
			}
		})
	}
}

// TestF105ClearConsumerInactivity pins the safe-config half: the helper zeroes a consumer's
// requested threshold and tolerates a nil config.
func TestF105ClearConsumerInactivity(t *testing.T) {
	cfg := &ConsumerConfig{InactiveThreshold: 30 * time.Second}
	mqttClearConsumerInactivity(cfg)
	if cfg.InactiveThreshold != 0 {
		t.Fatalf("expected the requested inactivity threshold cleared, got %s", cfg.InactiveThreshold)
	}
	mqttClearConsumerInactivity(nil) // must not panic
}
