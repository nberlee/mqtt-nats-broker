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

//go:build mqtt_fulltests

package mqtt

import (
	"testing"
	"time"
)

// TestTheOwnershipBudgetIsOneAbsoluteInterval pins the two properties a per-operation timeout
// would not have.
//
// A protocol that gave each CAS its own timeout lets a retry loop extend the total without bound,
// which is the failure a deadline exists to prevent. And deriving the remaining budget from the
// ANCHOR rather than decrementing it as steps report back is what makes a step that HANGS still
// spend it — a decremented counter measures the work that came back, and the client is waiting on
// the wall clock.
func TestTheOwnershipBudgetIsOneAbsoluteInterval(t *testing.T) {
	anchor := time.Now()
	b, err := mqttNewOwnershipBudget(anchor, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("anchoring the budget: %v", err)
	}

	if got := b.remaining(anchor); got != 100*time.Millisecond {
		t.Fatalf("a fresh budget has %v left, want the whole 100ms", got)
	}
	if got := b.remaining(anchor.Add(30 * time.Millisecond)); got != 70*time.Millisecond {
		t.Fatalf("after 30ms the budget has %v left, want 70ms", got)
	}
	// A RETRY CONSUMES IT LIKE ANYTHING ELSE: the remaining budget is a function of the clock,
	// so nothing a caller does refreshes it.
	if !b.expired(anchor.Add(101 * time.Millisecond)) {
		t.Fatalf("the budget survived its own total")
	}
	if got := b.remaining(anchor.Add(time.Hour)); got != 0 {
		t.Fatalf("an exhausted budget reported %v left", got)
	}

	// STEPS ARE GATED BEFORE THEY BEGIN. A step admitted with no budget left finishes after the
	// client has already been failed, and the budget is about what the client experiences.
	if err := mqttOwnershipStepAllowed(b, anchor.Add(10*time.Millisecond), "read ownership"); err != nil {
		t.Fatalf("a step inside the budget was refused: %v", err)
	}
	if err := mqttOwnershipStepAllowed(b, anchor.Add(200*time.Millisecond), "finalize"); err == nil {
		t.Fatalf("a step began with no budget left")
	}
	// AND NO BUDGET AT ALL IS A REFUSAL, never an unbounded step.
	if err := mqttOwnershipStepAllowed(nil, anchor, "read ownership"); err == nil {
		t.Fatalf("an ownership step ran with no budget bounding it")
	}

	// THE ANCHOR IS REQUIRED. A budget that started at the first ownership WRITE would leave the
	// reads and the recovery before it uncovered, which is the gap this exists to close.
	if _, err := mqttNewOwnershipBudget(time.Time{}, time.Second); err == nil {
		t.Fatalf("a budget was anchored nowhere")
	}
	if _, err := mqttNewOwnershipBudget(anchor, 0); err == nil {
		t.Fatalf("a budget of zero was accepted")
	}
}
