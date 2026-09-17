// Copyright 2026 The mqtt-nats-bridge Authors
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
	"strings"
	"testing"
	"time"
)

// TestF105TheOwnershipBudgetIsOneAbsoluteInterval pins the two properties a per-operation timeout
// would not have.
//
// A protocol that gave each CAS its own timeout lets a retry loop extend the total without bound,
// which is the failure a deadline exists to prevent. And deriving the remaining budget from the
// ANCHOR rather than decrementing it as steps report back is what makes a step that HANGS still
// spend it — a decremented counter measures the work that came back, and the client is waiting on
// the wall clock.
func TestF105TheOwnershipBudgetIsOneAbsoluteInterval(t *testing.T) {
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

// TestF105ExpiryDispositionsAreNotInterchangeable is the heart of row 9's deadline.
//
// Three outcomes, two of which look alike and mean opposite things. Getting them wrong is not a
// timing bug — it is either closing a generation whose metadata actually committed, or serving a
// client whose proposal actually lost.
func TestF105ExpiryDispositionsAreNotInterchangeable(t *testing.T) {
	// ABORTED: the loser closes its generation and refuses. It does NOT mint a replacement.
	aborted, err := mqttDisposeExpiredOwnership(mqttProposalAborted)
	if err != nil {
		t.Fatalf("disposing an aborted proposal: %v", err)
	}
	if !aborted.RefuseConnect || !aborted.CloseGeneration || aborted.LeaveSettleable {
		t.Fatalf("an aborted proposal disposed %+v, want refuse + close and NOT settleable", aborted)
	}

	// COMMITTED BUT NOT FINALIZED: not a refusal of the protocol, only of this connection. A
	// committed slot is never aborted — that is the one act the create-only fence forbids.
	committed, err := mqttDisposeExpiredOwnership(mqttProposalCommitted)
	if err != nil {
		t.Fatalf("disposing a committed proposal: %v", err)
	}
	if !committed.RefuseConnect || committed.CloseGeneration || !committed.LeaveSettleable {
		t.Fatalf("a committed proposal disposed %+v, want refuse + settleable and NOT closed",
			committed)
	}

	// UNRESOLVED: the third class. Absence of a committed verdict is NOT proof of
	// non-commitment, so nothing is aborted.
	unresolved, err := mqttDisposeExpiredOwnership(mqttProposalUnresolved)
	if err != nil {
		t.Fatalf("disposing an unresolved proposal: %v", err)
	}
	if !unresolved.RefuseConnect || unresolved.CloseGeneration || !unresolved.LeaveSettleable {
		t.Fatalf("an unresolved proposal disposed %+v, want refuse + settleable and NOT closed",
			unresolved)
	}
	if !strings.Contains(unresolved.Why, "not proof of non-commitment") {
		t.Fatalf("the unresolved disposition did not state why it is not an abort: %s",
			unresolved.Why)
	}

	// CLOSING AND LEAVING SETTLEABLE ARE MUTUALLY EXCLUSIVE, which is the property that keeps
	// the aborted case apart from the other two.
	for name, d := range map[string]mqttExpiryDisposition{
		"aborted": aborted, "committed": committed, "unresolved": unresolved,
	} {
		if d.CloseGeneration && d.LeaveSettleable {
			t.Fatalf("%s both closed its generation and left the proposal settleable", name)
		}
		if !d.RefuseConnect {
			t.Fatalf("%s did not refuse the connection", name)
		}
	}

	// A FINALIZED PROPOSAL HAS A SERVABLE AUTHORITY, so asking what its expiry means is asking
	// the wrong question — and it is refused rather than answered with a plausible default.
	if _, err := mqttDisposeExpiredOwnership(mqttProposalFinalized); err == nil {
		t.Fatalf("a finalized proposal was given an expiry disposition")
	}
	if _, err := mqttDisposeExpiredOwnership(mqttProposalState(99)); err == nil {
		t.Fatalf("an unsupported proposal state was given an expiry disposition")
	}
}
