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

package mqtt

import (
	"errors"
	"fmt"
	"time"
)

// Settling a standing ownership proposal before anything is destroyed (F-105 8a).
//
// An acceptance that crashed — or one still running on another broker — can leave its
// reservation or preparation standing, its metadata half committed, aborted or unresolved. Two
// places meet such a proposal before they may safely act, and both SETTLE it rather than read
// around it:
//
//   - the CONNECT, before its restore, its uncertain-record resolution, its retirement and its
//     clean-start block: a proposal destroyed over is a committed successor lost, or a
//     predecessor cancelled back onto a record that is gone;
//   - every discard, at prepareClear: the CONNECT's own read is not a fence, and disconnect,
//     expiry and gone-retirement never pass through a CONNECT at all.
//
// Settling is the SETTLEMENT ARMS only — settle the slot, then finalize, cancel or close the
// named proposal as its verdict licenses — never a reservation, a preparation, a metadata commit
// or a record adoption. It touches no binding and no local session state, so it needs no
// client-ID gate: the proposal's create-only receipt slot and the authority CAS arbitrate every
// race with another settler or with the proposal's own delayed batch durably.

// settleStandingProposal settles whatever ownership proposal stands for one client, bounded by
// budget, and returns the run's result and the authority as it read last. A result other than
// mqttOwnershipSettled means a proposal still stands (unresolved, contended, unreadable or out of
// budget): the caller decides what that refuses.
//
// st carries the CONNECT's one-foreign-target bound and its own earlier intents; a caller with no
// CONNECT passes a fresh one.
func (jsa *mqttJSA) settleStandingProposal(id, epoch, domainTk string, budget *mqttOwnershipBudget,
	now func() time.Time, allow func() error, st *mqttConnectOwnershipState) (mqttOwnershipResult, mqttAuthorityLoad, error) {
	view := jsa.withDeadline(budget.deadline())
	reconcile := func(op func(txn string) (mqttOpOutcome, error)) (string, error) {
		return mqttReconcileOwnershipStep(view, op, allow)
	}
	var last mqttAuthorityLoad
	load := func(rec *mqttForeignRecovery) mqttConnectOwnershipInput {
		in := view.loadConnectOwnershipInput(id, epoch, domainTk, mqttOwnIdentity{Earlier: st.ownIntents}, rec)
		last = in.Load
		return in
	}
	exec := func(step mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
		if step.Recovery == nil {
			// A settle-only run owns no proposal, so every act it is offered names one it is
			// settling. Anything else is a decision this run must not execute.
			return mqttConnectOwnershipInput{}, fmt.Errorf("a settlement was offered %v on no named proposal", step.Action)
		}
		if err := view.runOwnershipSettlementArm(id, epoch, domainTk, step, step.Recovery.Proposal.ID, reconcile); err != nil {
			if errors.Is(err, errMQTTOwnershipTargetMoved) {
				return load(step.Recovery), err
			}
			return mqttConnectOwnershipInput{}, err
		}
		return load(step.Recovery), nil
	}
	res, err := mqttDriveOwnershipRun(load(nil), _EMPTY_, budget, now, exec, st, nil, true)
	return res, last, err
}

// boundarySettlementTotal bounds a discard boundary's settlement of a standing proposal:
// one per-request timeout, from the moment the boundary begins. A discard is not a CONNECT and
// has no client waiting on a CONNACK; what it owes is not to hold its record fence open
// indefinitely, and a store that cannot answer within one request timeout leaves the discard on
// its receiptless residue (see prepareClear).
func (jsa *mqttJSA) boundarySettlementTotal() time.Duration { return jsa.timeout }

// abandonOwnProposal is the owed abandonment of an accepting CONNECT's OWN proposal that was
// positively aborted and whose abandonment did not confirm — the in-place cleanup failed, or the
// deadline cut it off (F-105 8a). The CONNECT has already been refused and certified; this runs
// after it, off the CONNECT path, ONE attempt under its own deadline of one request timeout.
//
// It acts on the CAPTURED target only: close for a reservation, cancel for a preparation, and
// only while that exact proposal still stands with a permanent ABORTED verdict. It never
// rediscovers a target, so a proposal that replaced this one is never touched; a target that no
// longer stands — the cleanup did land after all, or another acceptance settled it — leaves it
// nothing to do. If the attempt fails, the durable fallback is the next acceptance, which meets
// an ABORTED proposal it did not make and settles it with the same continuation.
func (as *mqttAccountSessionManager) abandonOwnProposal(id string, o mqttOwnObservation) {
	if o.Own == nil || o.Verdict != mqttMetadataAborted || o.AbandonConfirmed {
		return
	}
	target := *o.Own
	action := mqttOwnershipCloseAbandoned
	switch o.OwnState {
	case mqttAuthorityReserving:
	case mqttAuthorityPreparing:
		action = mqttOwnershipCancelHandoff
	default:
		return
	}
	as.mu.RLock()
	hook := as.testAbandonOwn
	as.mu.RUnlock()
	if hook != nil && !hook(id, target) {
		return
	}
	epoch, domainTk := as.sessStreamEpoch, as.domainTk
	go func() {
		jsa := as.jsa.withDeadline(time.Now().Add(as.jsa.timeout))
		if jsa.resolveMetadataAttempt(domainTk, id, epoch, target) != mqttMetadataAborted {
			return
		}
		noReattempt := func() error { return errors.New("the owed abandonment makes one attempt") }
		reconcile := func(op func(txn string) (mqttOpOutcome, error)) (string, error) {
			return mqttReconcileOwnershipStep(jsa, op, noReattempt)
		}
		step := mqttOwnershipStep{Action: action,
			Recovery: &mqttForeignRecovery{Proposal: target, AuthState: o.OwnState}}
		if err := jsa.runOwnershipSettlementArm(id, epoch, domainTk, step, target.ID, reconcile); err != nil &&
			!errors.Is(err, errMQTTOwnershipTargetMoved) {
			if as.jsa.c != nil {
				as.jsa.c.Debugf("MQTT: the owed abandonment of client %q's proposal %q did not land: %v; "+
					"the next acceptance settles it", id, target.ID, err)
			}
		}
	}()
}
