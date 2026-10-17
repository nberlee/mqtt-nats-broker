//go:build mqttport_fulltests

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

func testF105Load(status mqttOutboundLoadStatus, rec *mqttOutboundAuthority) mqttAuthorityLoad {
	return mqttAuthorityLoad{rec: rec, seq: 9, status: status}
}

func testF105Reserving(intentID string) *mqttOutboundAuthority {
	c := &mqttCreationIntent{ID: intentID, Gen: testF105Gen, Tenure: "tenure-new"}
	return &mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch, Gen: c.Gen, Tenure: c.Tenure,
		State: mqttAuthorityReserving, Rev: 1, Txn: "txn-res", OwnerTxn: "txn-res",
		Creation: c,
	}
}

func testF105Preparing(intentID string) *mqttOutboundAuthority {
	a := testF105PreparingAuthority()
	a.Intent.ID = intentID
	return a
}

// TestF105UnreadableAuthorityRefusesTheConnect pins the rule most available to get wrong:
// an unreadable authority is NOT an absent one.
//
// Treating either corrupt or indeterminate as "no authority" would let a first-creation
// reservation be claimed over a record this broker simply could not read — and the
// reservation is create-only precisely so that exactly one claimant holds it.
func TestF105UnreadableAuthorityRefusesTheConnect(t *testing.T) {
	for name, status := range map[string]mqttOutboundLoadStatus{
		"corrupt":       mqttOutboundCorrupt,
		"indeterminate": mqttOutboundIndeterminate,
	} {
		d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{
			Load: testF105Load(status, nil),
		})
		got, why := d.Action, d.Reason
		if got != mqttOwnershipRefuse {
			t.Fatalf("a %s authority produced action %d (%q), want REFUSE", name, got, why)
		}
		if why == _EMPTY_ {
			t.Fatalf("a %s refusal carried no reason", name)
		}
	}
}

// TestF105AbsentAuthorityReservesRatherThanServes pins that "no ownership recorded" licenses
// a reservation and nothing stronger. Serving immediately would owe a client something before
// its ownership was established.
func TestF105AbsentAuthorityReservesRatherThanServes(t *testing.T) {
	d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{
		Load: testF105Load(mqttOutboundAbsent, nil),
	})
	got, why := d.Action, d.Reason
	if got != mqttOwnershipReserve {
		t.Fatalf("an absent authority produced action %d (%q), want RESERVE", got, why)
	}
}

// TestF105AForeignProposalIsSettledNotAdopted pins the two halves of the recovery, and the
// line between them.
//
// A reservation and a preparation are resolvable ONLY by the acceptance that made them —
// adopting one would finalize ownership this acceptance never proposed. But a bare refusal
// was the defect: the proposal is create-only and never deleted, so a claimant that died
// between writing it and resolving its metadata half leaves the client permanently
// unservable. What another acceptance may do is SETTLE it: take its permanent metadata slot,
// which the original claimant's own commit is racing for, and then act on the verdict.
func TestF105AForeignProposalIsSettledNotAdopted(t *testing.T) {
	for name, tc := range map[string]struct {
		rec   *mqttOutboundAuthority
		mine  string
		state mqttAuthorityState
	}{
		"a first-creation reservation": {testF105Reserving("create-1"), "create-1", mqttAuthorityReserving},
		"a pending handoff":            {testF105Preparing("intent-1"), "intent-1", mqttAuthorityPreparing},
	} {
		// ANOTHER CLAIMANT'S PROPOSAL IS SETTLED, whatever THIS acceptance's own metadata
		// verdict says — that verdict answers this acceptance's proposal and is simply not
		// about the foreign one.
		for _, v := range []mqttMetadataVerdict{
			mqttMetadataCommitted, mqttMetadataAborted, mqttMetadataUnresolved, mqttMetadataForeign,
		} {
			d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{
				Load: testF105Load(mqttOutboundValid, tc.rec), IntentID: "someone-else", Metadata: v,
			})
			if d.Action != mqttOwnershipRecoverForeign {
				t.Fatalf("%s held by another claimant produced action %d (%q) at metadata %d, "+
					"want RECOVER FOREIGN", name, d.Action, d.Reason, v)
			}
			// THE TARGET CARRIES EVIDENCE, not an id. An id names a subject and says nothing
			// about what stands there, and the abort it would drive is permanent.
			if d.Recovery == nil {
				t.Fatalf("%s produced a recovery with no target", name)
			}
			if d.Recovery.Proposal.ID != tc.mine {
				t.Fatalf("%s produced a recovery for proposal %q, want %q",
					name, d.Recovery.Proposal.ID, tc.mine)
			}
			if d.Recovery.AuthState != tc.state {
				t.Fatalf("%s produced a recovery at authority state %d, want %d",
					name, d.Recovery.AuthState, tc.state)
			}
			// The sequence is what every authority mutation of the recovery CASes against,
			// so a recovery composed over ownership that has since moved loses.
			if d.Recovery.AuthSeq != 9 {
				t.Fatalf("%s produced a recovery at sequence %d, want the observed 9",
					name, d.Recovery.AuthSeq)
			}
			// It is never adopted as this acceptance's own.
			if d.Recovery.Proposal.ID == "someone-else" {
				t.Fatalf("%s adopted the foreign proposal as this acceptance's own", name)
			}
		}

		// ONE TARGET PER ACCEPTANCE. An acceptance already settling one proposal that finds
		// a different one standing is refused rather than retargeted: chasing successive
		// foreign proposals is an abort war in which nobody ever serves.
		bound := &mqttForeignRecovery{
			Proposal:  mqttOwnershipProposal{Kind: mqttProposalCreation, ID: "some-earlier-one"},
			AuthSeq:   3,
			AuthState: mqttAuthorityReserving,
		}
		d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{
			Load: testF105Load(mqttOutboundValid, tc.rec), IntentID: "someone-else",
			Recovering: bound,
		})
		if d.Action != mqttOwnershipRefuse {
			t.Fatalf("%s let an acceptance already settling %q retarget onto %q (action %d)",
				name, bound.Proposal.ID, tc.mine, d.Action)
		}
		// And an acceptance re-entering for the SAME target still settles it, because a
		// settlement takes more than one round: CAS retries are not new targets.
		same := &mqttForeignRecovery{
			Proposal: mqttOwnershipProposal{Kind: mqttProposalCreation, ID: tc.mine}, AuthSeq: 3,
		}
		d = mqttDecideConnectOwnership(mqttConnectOwnershipInput{
			Load: testF105Load(mqttOutboundValid, tc.rec), IntentID: "someone-else",
			Recovering: same,
		})
		if d.Action != mqttOwnershipRecoverForeign {
			t.Fatalf("%s refused an acceptance re-entering for its own recovery target (action %d, %q)",
				name, d.Action, d.Reason)
		}

		// Its own claimant, with the metadata half UNRESOLVED: still refused. The batch may
		// be in flight, so neither finishing nor abandoning is licensed.
		d = mqttDecideConnectOwnership(mqttConnectOwnershipInput{
			Load: testF105Load(mqttOutboundValid, tc.rec), IntentID: tc.mine,
			Metadata: mqttMetadataUnresolved,
		})
		if d.Action != mqttOwnershipRefuse {
			t.Fatalf("%s with an unresolved metadata half produced action %d (%q), want REFUSE",
				name, d.Action, d.Reason)
		}
	}
}

// TestF105ForeignRecoveryContinuationFollowsTheSettledVerdict pins what a settled foreign
// proposal licenses, including the two things it deliberately does NOT license.
func TestF105ForeignRecoveryContinuationFollowsTheSettledVerdict(t *testing.T) {
	reserving := &mqttForeignRecovery{
		Proposal:  mqttOwnershipProposal{Kind: mqttProposalCreation, ID: "create-1"},
		AuthState: mqttAuthorityReserving, AuthSeq: 9,
	}
	preparing := &mqttForeignRecovery{
		Proposal:  mqttOwnershipProposal{Kind: mqttProposalHandoff, ID: "intent-1"},
		AuthState: mqttAuthorityPreparing, AuthSeq: 9,
	}

	for name, tc := range map[string]struct {
		r    *mqttForeignRecovery
		v    mqttMetadataVerdict
		want mqttConnectOwnershipAction
	}{
		// COMMITTED means the original claimant WON. It is finalized exactly as proposed.
		"a committed foreign reservation": {reserving, mqttMetadataCommitted, mqttOwnershipFinalizeCreation},
		"a committed foreign handoff":     {preparing, mqttMetadataCommitted, mqttOwnershipFinalizeHandoff},
		// An abandoned reservation CLOSES its generation; the reservation is never deleted.
		"an aborted foreign reservation": {reserving, mqttMetadataAborted, mqttOwnershipCloseAbandoned},
		// A cancelled handoff RESTORES its predecessor. It does NOT close it: the
		// predecessor's generation was never displaced, and closing it because somebody
		// else's handoff failed would destroy a session nothing retired.
		"an aborted foreign handoff": {preparing, mqttMetadataAborted, mqttOwnershipCancelHandoff},
		// Neither an unresolved slot nor a contradictory receipt licenses anything.
		"an unresolved foreign reservation": {reserving, mqttMetadataUnresolved, mqttOwnershipRefuse},
		"an unresolved foreign handoff":     {preparing, mqttMetadataUnresolved, mqttOwnershipRefuse},
		"a contradictory receipt":           {preparing, mqttMetadataForeign, mqttOwnershipRefuse},
		"no target at all":                  {nil, mqttMetadataAborted, mqttOwnershipRefuse},
	} {
		got, why := mqttForeignRecoveryContinuation(tc.r, tc.v)
		if got != tc.want {
			t.Fatalf("%s produced action %d (%q), want %d", name, got, why, tc.want)
		}
		if why == _EMPTY_ {
			t.Fatalf("%s carried no reason", name)
		}
	}

	// A SERVING or CLOSED authority carries no proposal, so no verdict about one licenses a
	// continuation over it. This is the arm a caller reaches by reloading after the
	// authority moved under it, and retargeting there is exactly what must not happen.
	for _, st := range []mqttAuthorityState{mqttAuthorityServing, mqttAuthorityClosed} {
		moved := &mqttForeignRecovery{
			Proposal: preparing.Proposal, AuthState: st, AuthSeq: 9,
		}
		for _, v := range []mqttMetadataVerdict{mqttMetadataCommitted, mqttMetadataAborted} {
			if got, why := mqttForeignRecoveryContinuation(moved, v); got != mqttOwnershipRefuse {
				t.Fatalf("a %d verdict over authority state %d produced action %d (%q), want REFUSE",
					v, st, got, why)
			}
		}
	}
}

// TestF105ResolvedIntentsTakeTheirOwnRoute pins the four resolutions, including the one that
// is easy to state wrongly: an ABORTED first creation is abandoned by READMITTING a fresh
// generation, never by deleting the reservation, because deleting it would make the next
// claimant's create-only write succeed against an absence.
func TestF105ResolvedIntentsTakeTheirOwnRoute(t *testing.T) {
	for name, tc := range map[string]struct {
		rec      *mqttOutboundAuthority
		mine     string
		verdict  mqttMetadataVerdict
		expected mqttConnectOwnershipAction
	}{
		"a committed creation": {testF105Reserving("c"), "c", mqttMetadataCommitted, mqttOwnershipFinalizeCreation},
		"an aborted creation":  {testF105Reserving("c"), "c", mqttMetadataAborted, mqttOwnershipCloseAbandoned},
		"a committed handoff":  {testF105Preparing("i"), "i", mqttMetadataCommitted, mqttOwnershipFinalizeHandoff},
		"an aborted handoff":   {testF105Preparing("i"), "i", mqttMetadataAborted, mqttOwnershipCancelHandoff},
	} {
		d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{
			Load: testF105Load(mqttOutboundValid, tc.rec), IntentID: tc.mine, Metadata: tc.verdict,
		})
		got, why := d.Action, d.Reason
		if got != tc.expected {
			t.Fatalf("%s produced action %d (%q), want %d", name, got, why, tc.expected)
		}
	}

	// A serving authority is handed off; a closed one is re-admitted beside its closure.
	serving := &mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-0",
		State: mqttAuthorityServing, Rev: 1, Txn: "t", OwnerTxn: "t",
	}
	if d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{
		Load: testF105Load(mqttOutboundValid, serving),
	}); d.Action != mqttOwnershipPrepareHandoff {
		t.Fatalf("a serving authority produced action %d (%q), want PREPARE HANDOFF",
			d.Action, d.Reason)
	}
	closed := *serving
	closed.State = mqttAuthorityClosed
	if d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{
		Load: testF105Load(mqttOutboundValid, &closed),
	}); d.Action != mqttOwnershipReserveOverClosed {
		t.Fatalf("a closed authority produced action %d (%q), want RESERVE OVER CLOSED: a "+
			"direct readmission installs SERVING outright, with no frozen proposal and no "+
			"metadata half, which reintroduces the bootstrap gap on the second attempt",
			d.Action, d.Reason)
	}
}

// TestF105OnlyServingAdmitsDelivery pins the single-state answer to "may new outbound work
// be admitted under this". A second reading that admitted a reservation or a preparation
// would be an admission before the metadata half committed, or during a frozen handoff.
func TestF105OnlyServingAdmitsDelivery(t *testing.T) {
	serving := &mqttOutboundAuthority{State: mqttAuthorityServing}
	if !mqttOwnershipAdmitsDelivery(serving) {
		t.Fatalf("a serving authority does not admit delivery")
	}
	for name, st := range map[string]mqttAuthorityState{
		"reserving": mqttAuthorityReserving,
		"preparing": mqttAuthorityPreparing,
		"closed":    mqttAuthorityClosed,
	} {
		if mqttOwnershipAdmitsDelivery(&mqttOutboundAuthority{State: st}) {
			t.Fatalf("a %s authority admits delivery", name)
		}
	}
	if mqttOwnershipAdmitsDelivery(nil) {
		t.Fatalf("a missing authority admits delivery: absence is not permission")
	}
}

// TestF105AbortedReservationClosesBeforeItClaimsAgain pins the two-step abandonment
// implementation review 4 found collapsed into one.
//
// An aborted reservation returned READMIT, but readmit-tenure requires a CLOSED predecessor
// and an aborted reservation is not one — so the route did not exist. Worse, a direct
// readmission installs SERVING outright with no frozen proposal and no metadata half, which
// is the bootstrap gap reappearing on the second attempt after a failed first creation.
func TestF105AbortedReservationClosesBeforeItClaimsAgain(t *testing.T) {
	rec := testF105Reserving("c")

	d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{
		Load: testF105Load(mqttOutboundValid, rec), IntentID: "c", Metadata: mqttMetadataAborted,
	})
	got, why := d.Action, d.Reason
	if got != mqttOwnershipCloseAbandoned {
		t.Fatalf("an aborted reservation produced action %d (%q), want CLOSE ABANDONED: "+
			"readmission requires a CLOSED predecessor, which an aborted reservation is not", got, why)
	}

	// And once it IS closed, the next step is a reservation over that closure — never a
	// direct serving authority.
	closed := &mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-new",
		State: mqttAuthorityClosed, Rev: 2, Txn: "txn-close", OwnerTxn: "txn-res",
	}
	d = mqttDecideConnectOwnership(mqttConnectOwnershipInput{
		Load: testF105Load(mqttOutboundValid, closed),
	})
	got, why = d.Action, d.Reason
	if got != mqttOwnershipReserveOverClosed {
		t.Fatalf("a closed authority produced action %d (%q), want RESERVE OVER CLOSED", got, why)
	}
}
