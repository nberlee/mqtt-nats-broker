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
	"errors"
	"strings"
	"testing"
	"time"
)

// testDriveClock advances a fixed amount on every read, so a test can spend a budget without
// sleeping and without the result depending on how fast the machine is.
func testDriveClock(anchor time.Time, step time.Duration) func() time.Time {
	n := 0
	return func() time.Time {
		t := anchor.Add(time.Duration(n) * step)
		n++
		return t
	}
}

func testServingLoad() mqttAuthorityLoad {
	return mqttAuthorityLoad{
		status: mqttOutboundValid,
		rec: &mqttOutboundAuthority{
			ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-1",
			State: mqttAuthorityServing, Rev: 1, Txn: "txn-1", OwnerTxn: "txn-1",
		},
	}
}

// TestF105OwnershipStopsAtAServableAuthority is the ordinary path, and it pins the ORDER that
// matters most: servable is checked BEFORE the budget.
//
// A protocol that has already succeeded is not failed by its clock running out afterwards. The
// disposition function refuses the question for a finalized proposal precisely so this order is
// forced rather than assumed.
func TestF105OwnershipStopsAtAServableAuthority(t *testing.T) {
	b, err := mqttNewOwnershipBudget(time.Now(), time.Millisecond)
	if err != nil {
		t.Fatalf("anchoring: %v", err)
	}
	// A clock already far past the budget: if the budget were consulted first, this would
	// expire instead of succeeding.
	clock := testDriveClock(b.Anchor.Add(time.Hour), 0)

	res, derr := mqttDriveOwnership(mqttConnectOwnershipInput{Load: testServingLoad()},
		"tenure-1", b, clock, func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			t.Fatal("a servable authority executed a step")
			return mqttConnectOwnershipInput{}, nil
		})
	if derr != nil {
		t.Fatalf("driving: %v", derr)
	}
	if res.Outcome != mqttOwnershipServable || res.Steps != 0 {
		t.Fatalf("a servable authority drove to %v after %d steps", res.Outcome, res.Steps)
	}
}

// TestF105OwnershipExpiryCarriesTheProposalsDisposition is the whole point of the budget living
// in the driver rather than at the call site.
//
// The connection is refused in every case. What differs is what happens to the PROPOSAL, and
// the driver is the only thing that knows which of the three states the run reached.
func TestF105OwnershipExpiryCarriesTheProposalsDisposition(t *testing.T) {
	absent := mqttAuthorityLoad{status: mqttOutboundAbsent}

	// UNRESOLVED: no verdict was established. Nothing is aborted — absence of a committed
	// verdict is not proof of non-commitment.
	b, _ := mqttNewOwnershipBudget(time.Now(), 10*time.Millisecond)
	res, err := mqttDriveOwnership(mqttConnectOwnershipInput{Load: absent}, "tenure-mine", b,
		testDriveClock(b.Anchor, 20*time.Millisecond),
		func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			return mqttConnectOwnershipInput{Load: absent}, nil
		})
	if err != nil {
		t.Fatalf("driving: %v", err)
	}
	if res.Outcome != mqttOwnershipExpired {
		t.Fatalf("an exhausted budget drove to %v: %s", res.Outcome, res.Reason)
	}
	if !res.Disposition.RefuseConnect || res.Disposition.AbandonOwn ||
		!res.Disposition.LeaveSettleable {
		t.Fatalf("an unresolved expiry disposed %+v", res.Disposition)
	}

	// COMMITTED but not finalized: the metadata half is permanent, so another acceptance
	// finalizes it exactly as proposed. Aborting here is the one act the create-only fence
	// forbids.
	b2, _ := mqttNewOwnershipBudget(time.Now(), 10*time.Millisecond)
	res2, err2 := mqttDriveOwnership(
		mqttConnectOwnershipInput{Load: absent, IntentID: "intent-1",
			Metadata: mqttMetadataCommitted},
		"tenure-mine", b2, testDriveClock(b2.Anchor, 20*time.Millisecond),
		func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			return mqttConnectOwnershipInput{Load: absent}, nil
		})
	if err2 != nil {
		t.Fatalf("driving: %v", err2)
	}
	if res2.Outcome != mqttOwnershipExpired {
		t.Fatalf("an exhausted budget drove to %v", res2.Outcome)
	}
	if res2.Disposition.AbandonOwn || !res2.Disposition.LeaveSettleable {
		t.Fatalf("a committed proposal's expiry disposed %+v, want settleable and NOT closed",
			res2.Disposition)
	}

	// THE TWO DISPOSITIONS CARRY THE SAME FLAGS, AND THAT IS CORRECT. Both refuse this
	// connection and leave the proposal standing; they differ in what is KNOWN, not in what is
	// done. The operational difference belongs to the NEXT acceptance — a committed proposal
	// must be finalized exactly as proposed, while an unresolved one may still be settled
	// either way — and that is mqttDecideConnectOwnership's decision, not this one's.
	//
	// So the reason is the only thing that separates them here, and a mutation collapsing the
	// committed case into the unresolved one survived every flag assertion above until this
	// was added.
	if !strings.Contains(res2.Reason, "no budget left") {
		t.Fatalf("the committed expiry did not report the budget: %s", res2.Reason)
	}
	if !strings.Contains(res2.Disposition.Why, "committed permanently") {
		t.Fatalf("a committed proposal's expiry was explained as %q, which does not say the "+
			"metadata half is permanent", res2.Disposition.Why)
	}
	if !strings.Contains(res.Disposition.Why, "not proof of non-commitment") {
		t.Fatalf("an unresolved expiry was explained as %q, which does not say why it is not "+
			"an abort", res.Disposition.Why)
	}
}

// TestF105OwnershipBindsToOneForeignProposal is the abort-war bound, and it lives here because
// no single decision can hold it.
//
// An acceptance free to chase whatever foreign proposal it finds on each reload would abort a
// succession of them. The decision function is stateless and would happily name a new target
// every time; what stops it is that the driver carries the first one and refuses a second.
func TestF105OwnershipBindsToOneForeignProposal(t *testing.T) {
	first := &mqttForeignRecovery{
		Proposal:  mqttOwnershipProposal{Kind: mqttProposalCreation, ID: "intent-A"},
		AuthSeq:   1,
		AuthState: mqttAuthorityReserving,
	}
	second := &mqttForeignRecovery{
		Proposal:  mqttOwnershipProposal{Kind: mqttProposalCreation, ID: "intent-B"},
		AuthSeq:   2,
		AuthState: mqttAuthorityReserving,
	}
	// The SAME proposal seen again through a fresh struct is not a second target.
	if !mqttSameForeignTarget(first, &mqttForeignRecovery{Proposal: first.Proposal, AuthSeq: 99}) {
		t.Fatalf("the same proposal read twice was treated as two targets")
	}
	if mqttSameForeignTarget(first, second) {
		t.Fatalf("two different proposals were treated as one target")
	}
	// A matching id under a DIFFERENT KIND is a different proposal: a creation and a handoff
	// are not interchangeable and a recovery must not silently retarget between them.
	if mqttSameForeignTarget(first, &mqttForeignRecovery{
		Proposal: mqttOwnershipProposal{Kind: mqttProposalHandoff, ID: "intent-A"},
	}) {
		t.Fatalf("a creation and a handoff sharing an id were treated as one proposal")
	}
	if mqttSameForeignTarget(nil, first) || mqttSameForeignTarget(first, nil) {
		t.Fatalf("a missing target matched")
	}
}

// TestF105OwnershipBoundsWorkAsWellAsTime pins what the step bound ACTUALLY guarantees.
//
// An earlier version of this test claimed the step bound is what makes an unconverging protocol
// terminate, and demonstrated it by removing the bound and watching the test hang. That was a
// FROZEN TEST CLOCK, not a production failure: the wall-clock budget is finite and absolute, so
// with a clock that advances, a run of instant retries expires on its own.
//
// The bound's real guarantee is about WORK, not time. Each step is a durable transition against
// contended records, so without it a run could attempt unboundedly many of them inside the time
// the budget allows — hammering the store and contending with the brokers it is racing.
//
// Both halves are asserted here, because the first is what stops the second being overclaimed.
func TestF105OwnershipBoundsWorkAsWellAsTime(t *testing.T) {
	absent := mqttAuthorityLoad{status: mqttOutboundAbsent}

	// (1) TIME ALONE TERMINATES IT. A generous step allowance and an ADVANCING clock: the run
	// ends by expiry, and the step bound is not what stopped it.
	b, _ := mqttNewOwnershipBudget(time.Now(), 10*time.Millisecond)
	calls := 0
	res, err := mqttDriveOwnership(mqttConnectOwnershipInput{Load: absent}, "tenure-mine", b,
		testDriveClock(b.Anchor, 3*time.Millisecond),
		func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			calls++
			return mqttConnectOwnershipInput{Load: absent}, nil
		})
	if err != nil {
		t.Fatalf("driving: %v", err)
	}
	if res.Outcome != mqttOwnershipExpired {
		t.Fatalf("an advancing clock drove to %v, want expiry: %s", res.Outcome, res.Reason)
	}
	if calls >= mqttOwnershipMaxSteps {
		t.Fatalf("the run reached the step bound (%d calls) before the clock stopped it, so this "+
			"case does not show what it claims", calls)
	}

	// (2) WORK IS BOUNDED TOO. A budget large enough never to expire here, so only the step
	// bound can cap the durable transitions attempted.
	b2, _ := mqttNewOwnershipBudget(time.Now(), time.Hour)
	calls2 := 0
	res2, err2 := mqttDriveOwnership(mqttConnectOwnershipInput{Load: absent}, "tenure-mine", b2,
		testDriveClock(b2.Anchor, time.Millisecond),
		func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			calls2++
			return mqttConnectOwnershipInput{Load: absent}, nil
		})
	if err2 != nil {
		t.Fatalf("driving: %v", err2)
	}
	if res2.Outcome != mqttOwnershipRefused {
		t.Fatalf("an unconverging protocol drove to %v", res2.Outcome)
	}
	if !strings.Contains(res2.Reason, "contention rather than progress") {
		t.Fatalf("the refusal did not come from the step bound: %s", res2.Reason)
	}
	if calls2 != mqttOwnershipMaxSteps {
		t.Fatalf("the protocol attempted %d transitions, want the bound of %d",
			calls2, mqttOwnershipMaxSteps)
	}
}

// TestF105OwnershipRequiresItsClockAndExecutor pins the two refusals that keep an unbounded run
// from existing at all.
func TestF105OwnershipRequiresItsClockAndExecutor(t *testing.T) {
	b, _ := mqttNewOwnershipBudget(time.Now(), time.Second)
	if _, err := mqttDriveOwnership(mqttConnectOwnershipInput{}, "tenure-mine", b, time.Now, nil); err == nil {
		t.Fatalf("ownership was driven with no executor")
	}
	if _, err := mqttDriveOwnership(mqttConnectOwnershipInput{}, "tenure-mine", b, nil,
		func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			return mqttConnectOwnershipInput{}, nil
		}); err == nil {
		t.Fatalf("ownership was driven with no clock")
	}
}

// TestF105OwnershipDeadlineViewCapsEveryRequest is consult 6's first WP-E.2 concern, as F-105 8a
// answers it.
//
// Checking a deadline BEFORE starting a step does not bound a step that then BLOCKS, and neither
// does handing the step a duration: a step is several requests, and each told "the remaining
// budget" could spend all of it. So the deadline is ONE INSTANT carried by the assistant view the
// protocol's every request runs on: each wait is the per-request timeout capped at that instant,
// and a request after it is refused before anything is queued — the certain shape, which says
// nothing was sent.
func TestF105OwnershipDeadlineViewCapsEveryRequest(t *testing.T) {
	core := &mqttJSA{mqttJSACore: &mqttJSACore{timeout: 5 * time.Second}}

	if w, err := core.requestWait(); err != nil || w != 5*time.Second {
		t.Fatalf("a view with no deadline must wait the per-request timeout, got %v, %v", w, err)
	}
	near := core.withDeadline(time.Now().Add(200 * time.Millisecond))
	if near.mqttJSACore != core.mqttJSACore {
		t.Fatal("a deadline view must share the assistant's core, or it is a different assistant")
	}
	w, err := near.requestWait()
	if err != nil || w <= 0 || w > 200*time.Millisecond {
		t.Fatalf("a view 200ms from its deadline must wait at most 200ms, got %v, %v", w, err)
	}
	far := core.withDeadline(time.Now().Add(time.Hour))
	if w, err := far.requestWait(); err != nil || w != 5*time.Second {
		t.Fatalf("a deadline beyond the per-request timeout must not extend it, got %v, %v", w, err)
	}
	past := core.withDeadline(time.Now().Add(-time.Millisecond))
	if _, err := past.requestWait(); !errors.Is(err, errMQTTOwnershipDeadline) || !errors.Is(err, errMQTTRequestNotSent) {
		t.Fatalf("a request after the deadline must be refused before it is queued, as a certain "+
			"not-sent outcome, got %v", err)
	}
	// A wait that ENDED on the deadline is still ambiguous — the request was queued — and is
	// marked as the deadline's so the protocol disposes it as an expiry.
	if err := past.timeoutError(errors.New("timeout")); !errors.Is(err, errMQTTOwnershipDeadline) ||
		errors.Is(err, errMQTTRequestNotSent) {
		t.Fatalf("a reply wait cut off by the deadline must carry the deadline and NOT claim the "+
			"request was unsent, got %v", err)
	}
	b, _ := mqttNewOwnershipBudget(time.Unix(1000, 0), 20*time.Second)
	if got := b.deadline(); !got.Equal(time.Unix(1020, 0)) {
		t.Fatalf("the budget's deadline is its anchor plus its total, got %v", got)
	}
}

// TestF105OwnershipDoesNotStopAtSomebodyElsesTenure is round I-4's first finding.
//
// The servable check used to accept ANY serving authority, which is the
// finalization-versus-activation confusion in its most damaging form: an ordinary reconnect
// finding another broker's tenure returned SERVABLE having executed zero steps, skipping the
// handoff preparation entirely. The same happened after recovering a committed FOREIGN
// proposal — its tenure becomes serving while this acceptance has acquired nothing.
//
// A caveat in a file header does not repair a skipped transition.
func TestF105OwnershipDoesNotStopAtSomebodyElsesTenure(t *testing.T) {
	// A serving authority whose tenure belongs to somebody else.
	foreign := testServingLoad()
	b, _ := mqttNewOwnershipBudget(time.Now(), time.Hour)

	steps := 0
	res, err := mqttDriveOwnership(mqttConnectOwnershipInput{Load: foreign}, "tenure-mine", b,
		testDriveClock(b.Anchor, time.Millisecond),
		func(s mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			steps++
			// The decision for a serving predecessor is to PREPARE A HANDOFF, which is
			// precisely the transition the old early return skipped.
			if s.Action != mqttOwnershipPrepareHandoff {
				t.Fatalf("step %d was %v, want a handoff preparation against a foreign tenure",
					steps, s.Action)
			}
			return mqttConnectOwnershipInput{Load: foreign}, nil
		})
	if err != nil {
		t.Fatalf("driving: %v", err)
	}
	if steps == 0 {
		t.Fatalf("a foreign serving tenure drove to %v with NO steps: the handoff was skipped",
			res.Outcome)
	}
	if res.Outcome == mqttOwnershipServable {
		t.Fatalf("somebody else's tenure was reported as this acceptance's ownership")
	}

	// THE POSITIVE CONTROL: the same authority, when the tenure IS this acceptance's, is
	// servable immediately and executes nothing.
	mine := testServingLoad()
	res2, err2 := mqttDriveOwnership(mqttConnectOwnershipInput{Load: mine}, mine.rec.Tenure, b,
		testDriveClock(b.Anchor, time.Millisecond),
		func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			t.Fatal("this acceptance's own serving tenure executed a step")
			return mqttConnectOwnershipInput{}, nil
		})
	if err2 != nil {
		t.Fatalf("driving: %v", err2)
	}
	if res2.Outcome != mqttOwnershipServable || res2.Steps != 0 {
		t.Fatalf("this acceptance's own tenure drove to %v after %d steps",
			res2.Outcome, res2.Steps)
	}

	// AND WITHOUT ITS OWN TENURE the driver cannot tell the two apart, so it refuses rather
	// than guessing.
	if _, err := mqttDriveOwnership(mqttConnectOwnershipInput{Load: mine}, _EMPTY_, b, time.Now,
		func(mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			return mqttConnectOwnershipInput{}, nil
		}); err == nil {
		t.Fatalf("ownership was driven with no tenure of its own to compare against")
	}
}

// TestF105OwnershipLoserStopsAcrossAReload is round I-4's second finding.
//
// The loser bound was latched only when the decision returned CloseAbandoned — immediately
// before returning — so it never protected a SUBSEQUENT read. If a peer aborted and CLOSED this
// acceptance's reservation before its next reload, the input arrived as ABORTED over a closed
// authority, the decision returned ReserveOverClosed, and the driver minted a REPLACEMENT
// proposal: the abort war the bound exists to prevent, entered through the loser's own retry.
//
// The verdict is now latched from the READ, so the run stops whatever the next decision offers.
func TestF105OwnershipLoserStopsAcrossAReload(t *testing.T) {
	b, _ := mqttNewOwnershipBudget(time.Now(), time.Hour)
	closed := mqttAuthorityLoad{
		status: mqttOutboundValid,
		rec: &mqttOutboundAuthority{
			ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-old",
			State: mqttAuthorityClosed, Rev: 2, Txn: "txn-2", OwnerTxn: "txn-2",
		},
	}
	// The first read has this acceptance's own proposal ABORTED. A peer has already closed the
	// authority, so the stateless decision would happily offer a fresh reservation over it.
	first := mqttConnectOwnershipInput{
		Load: closed, IntentID: "intent-mine", Metadata: mqttMetadataAborted,
	}

	steps := 0
	res, err := mqttDriveOwnership(first, "tenure-mine", b,
		testDriveClock(b.Anchor, time.Millisecond),
		func(s mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
			steps++
			if s.Action == mqttOwnershipReserve || s.Action == mqttOwnershipReserveOverClosed {
				t.Fatalf("the losing acceptance minted a replacement proposal (%v) after its "+
					"own was aborted", s.Action)
			}
			// Whatever it did, the next read still says aborted over a closed authority.
			return first, nil
		})
	if err != nil {
		t.Fatalf("driving: %v", err)
	}
	if res.Outcome != mqttOwnershipRefused {
		t.Fatalf("a losing acceptance drove to %v, want refused: %s", res.Outcome, res.Reason)
	}
	if steps > 1 {
		t.Fatalf("the losing acceptance executed %d steps: it closes its generation once and "+
			"then stops", steps)
	}
	if !strings.Contains(res.Reason, "rather than minting a replacement") {
		t.Fatalf("the refusal did not come from the loser bound: %s", res.Reason)
	}
}
