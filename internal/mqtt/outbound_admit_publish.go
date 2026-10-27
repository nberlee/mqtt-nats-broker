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
)

// Admitting one outbound PUBLISH (F-105).
//
// This is what the ordinary QoS 1/2 delivery route will call in place of the independent
// register-and-marker write it performs today. It is deliberately ONE transition carrying
// everything that admission means:
//
//   - the AUTHORITY, so the admission is arbitrated rather than assumed;
//   - the EXCHANGE, the client-facing obligation the identifier is allocated to;
//   - the REGISTER and its UNSENT MARKER, which are the existing durable pair the send path
//     and recovery already read — they keep their current shapes and their current meaning;
//   - the RESOURCE dependency increments for everything this obligation will need to
//     complete, which is what stops a later teardown deleting the consumer that carries its
//     PUBREC evidence.
//
// All of it commits or none of it does. The failure this closes is not hypothetical: today
// the register lands on its own, so a source consumer or the PUBREL consumer can be deleted
// by a teardown that never knew an obligation depended on it, and the register survives to
// prove nothing.
//
// NOT WIRED. mqttDeliverMsgCbQoS12 still calls storeInflightRegister directly; this exists so
// the route can be converted against a mechanism that has already been exercised against a
// real store. There is deliberately NO missing-authority fallback: a caller without an
// authority is refused, because a fallback is how a path quietly keeps admitting work the
// authority never authorized.

// mqttPublishAdmission is one outbound PUBLISH's admission, assembled by its caller.
type mqttPublishAdmission struct {
	// Auth is the authority this admission is made under, and AuthSeq the sequence it was
	// read at.
	Auth    *mqttOutboundAuthority
	AuthSeq uint64

	// Txn and Op identify the attempt.
	Txn string
	Op  string

	// Exchange is the obligation being admitted. Its nonce is minted by the caller and its
	// AdmitTxn must be Txn.
	Exchange *mqttOutboundExchange

	// Register is the durable in-flight register, in its existing shape, and RegSubj the
	// subject it is created on. Both keep the meanings inflight_register.go gives them;
	// this transition changes WHEN they are written, not WHAT they say.
	//
	// THERE IS NO EXPECTATION FIELD, deliberately. An admission CREATES a register; it
	// never replaces one. Which occupant may be displaced by which allocation is the slot
	// state machine's decision (inflight_slot_state.go), and offering an expectation here
	// would be this mechanism deciding it a second time — silently, since an absent unsent
	// marker is normal after transmission and proves nothing about the identifier being
	// free. Replacement becomes supported when the ordinary-PUBLISH path is converted, by
	// REUSING that machine's decision inside this batch.
	Register *mqttInflightRecord
	RegSubj  string

	// Deps are the resources this obligation will need in order to complete, each with the
	// sequence it was read at. Their counts are incremented in this same batch.
	Deps []mqttResourceDependency

	// Ledger is the generation's inventory ledger as it stands, and LedgerSeq the sequence it
	// was read at. Nil when the generation has allocated nothing yet, which is a positive
	// answer rather than a failure: the first allocation creates the ledger create-only.
	//
	// The admission ALLOCATES its obligation's duty ordinal from here, in this same batch. That
	// is what makes the work discoverable at the moment it exists rather than at the moment it
	// succeeds — a duty that only became discoverable once it had done its work would be
	// undiscoverable in exactly the case recovery is for.
	Ledger    *mqttGenLedger
	LedgerSeq uint64
}

// mqttResourceDependency names one resource an admission depends on, and the ROLE it needs it
// in.
//
// The role is not optional and has no default. A dependency taken without saying why would be a
// bare nonce again, and the release would then have nothing to prove.
type mqttResourceDependency struct {
	Resource *mqttOutboundResource
	Seq      uint64
	// Role is why this admission needs the resource. With the obligation's nonce it is the
	// edge's whole identity: no live role names a constituent, so an edge carries no Ref.
	Role mqttResourceRole
}

// mqttHoldsForDeps is the holds index an admission's dependencies imply: every logical-use
// resource taken in a hold role. Nil when there is none, so the member is absent.
func mqttHoldsForDeps(deps []mqttResourceDependency) []mqttExchangeHold {
	var holds []mqttExchangeHold
	for _, d := range deps {
		if d.Resource != nil && mqttHoldRole(d.Role) && mqttResourceLogicalUseKind(d.Resource.Kind) {
			holds = append(holds, mqttExchangeHold{Inc: d.Resource.Inc, Role: d.Role})
		}
	}
	return holds
}

// edgeFor builds this dependency's edge for one obligation.
func (d mqttResourceDependency) edgeFor(nonce string) mqttResourceEdge {
	return mqttResourceEdge{Nonce: nonce, Role: d.Role}
}

// mqttAdmitPublishTransition assembles the whole admission.
//
// The register and marker are carried as ordinary members, so they land under exactly the
// expectations storeInflightPair would have used: the register at its observed sequence, the
// marker create-only. What changes is that they can no longer land without the authority and
// the exchange landing too.
func mqttAdmitPublishTransition(a *mqttPublishAdmission) (*mqttAuthorityTransition, error) {
	if a == nil || a.Auth == nil {
		return nil, fmt.Errorf("admitting a publish requires the authority it is admitted under")
	}
	if a.Exchange == nil || a.Register == nil {
		return nil, fmt.Errorf("admitting a publish requires both its exchange and its register")
	}
	if a.AuthSeq == 0 {
		return nil, fmt.Errorf("admitting a publish requires the sequence its authority was read at")
	}
	if a.Auth.State != mqttAuthorityServing {
		return nil, fmt.Errorf("an authority in state %d admits no new obligation", a.Auth.State)
	}
	// THE EXCHANGE IS ADMITTED INTO THE AUTHORITY'S OWN GENERATION. Without this an
	// admission under a successor's authority could install an obligation in a generation
	// that is already closed — an obligation admitted after closure, reached by naming the
	// old generation rather than by racing the closure.
	if a.Exchange.Gen != a.Auth.Gen {
		return nil, fmt.Errorf("the exchange is admitted into generation %q but its authority serves %q",
			a.Exchange.Gen, a.Auth.Gen)
	}
	if a.Exchange.Tenure != a.Auth.Tenure {
		return nil, fmt.Errorf("the exchange names tenure %q but its authority carries %q",
			a.Exchange.Tenure, a.Auth.Tenure)
	}
	if a.Exchange.AdmitTxn != a.Txn {
		return nil, fmt.Errorf("the exchange names admitting transition %q, but this transition is %q",
			a.Exchange.AdmitTxn, a.Txn)
	}
	// The exchange and the register describe ONE obligation, so the identifier and the QoS
	// they carry must agree. They are two records because recovery and the send path already
	// read the register; they are not two opinions.
	if a.Exchange.PI != a.Register.PI {
		return nil, fmt.Errorf("the exchange allocates identifier %d but its register carries %d",
			a.Exchange.PI, a.Register.PI)
	}
	if a.Exchange.QoS != a.Register.QoS {
		return nil, fmt.Errorf("the exchange is QoS %d but its register is QoS %d",
			a.Exchange.QoS, a.Register.QoS)
	}
	// THE EXCHANGE NONCE IS THE REGISTER'S LINEAGE, not a second identity minted beside it.
	//
	// Lin is per-OBLIGATION where the register's own Nonce is per-ALLOCATION, so the
	// exchange — which is the obligation — is identified by exactly that value. Minting E
	// independently would give one obligation two identities that agree only by luck, and
	// after an allocation replacement nothing could reconstruct which exchange a register
	// belonged to: the identifier is reusable, so matching on it proves nothing.
	if a.Exchange.Nonce != a.Register.Lin {
		return nil, fmt.Errorf("the exchange is %q but its register's lineage is %q: the "+
			"exchange nonce IS the obligation lineage", a.Exchange.Nonce, a.Register.Lin)
	}
	if a.RegSubj == _EMPTY_ {
		return nil, fmt.Errorf("admitting a publish requires the register's subject")
	}
	if verr := mqttValidateInflightShape(a.Register); verr != nil {
		return nil, fmt.Errorf("refusing to admit an unsupported register shape: %w", verr)
	}

	regBody, err := json.Marshal(a.Register)
	if err != nil {
		return nil, err
	}
	markerBody, merr := mqttBuildInflightMarker(a.Register.Nonce)
	if merr != nil {
		return nil, fmt.Errorf("refusing to admit a publish whose marker cannot be built: %w", merr)
	}
	// THE HOLDS INDEX IS DERIVED HERE, from the edges this batch adds, and from nothing the caller
	// says: the ending that releases them reads it, and the write boundary refuses an admission
	// whose index differs from its added edges in either direction (F-105 7c-lc).
	a.Exchange.Holds = mqttHoldsForDeps(a.Deps)
	xchBody, xerr := json.Marshal(a.Exchange)
	if xerr != nil {
		return nil, xerr
	}

	idHash := getHash(a.Auth.ID)
	members := []mqttTransitionMember{
		{subj: a.RegSubj, expect: 0, body: regBody},
		{subj: a.RegSubj + "." + mqttInflightMarkerSuffix, expect: 0, body: markerBody},
		{subj: mqttExchangeSubject(idHash, a.Exchange.Gen, a.Exchange.Nonce), expect: 0, body: xchBody},
	}

	// EVERY DEPENDENCY IS INCREMENTED IN THIS BATCH, at the sequence it was read. That is
	// what makes it impossible for a deletion authorization to win on a count taken before
	// this obligation existed: both write the resource record, so one of them loses.
	for _, d := range a.Deps {
		if d.Resource == nil {
			return nil, fmt.Errorf("admitting a publish requires each named dependency")
		}
		if d.Seq == 0 {
			return nil, fmt.Errorf("dependency %q carries no read sequence", d.Resource.Inc)
		}
		if d.Resource.Gen != a.Exchange.Gen {
			return nil, fmt.Errorf("dependency %q belongs to generation %q, not this exchange's %q",
				d.Resource.Inc, d.Resource.Gen, a.Exchange.Gen)
		}
		// NAMED BY THE OBLIGATION, not counted. The write boundary then requires this
		// exchange to be created by this very batch, so the edge is derived from an
		// admission rather than asserted by a caller.
		next, derr := mqttResourceTakeDependency(d.Resource, d.edgeFor(a.Exchange.Nonce))
		if derr != nil {
			return nil, fmt.Errorf("admitting a publish: %w", derr)
		}
		body, berr := json.Marshal(next)
		if berr != nil {
			return nil, berr
		}
		members = append(members, mqttTransitionMember{
			subj:   mqttResourceSubject(idHash, next.Gen, next.Inc),
			expect: d.Seq,
			body:   body,
		})
	}

	// THE OBLIGATION'S DUTY ORDINAL, allocated in this same batch.
	//
	// One entry, for the exchange. The resources it depends on already carry their own
	// provisioning and cleanup entries, reserved by the transition that provisioned them — an
	// admission never allocates for a resource, which is what keeps "every duty is allocated
	// when the thing it concerns is admitted" a rule with one meaning rather than two.
	inv, ierr := mqttAllocateInventoryMembers(a.Auth.ID, a.Auth.Epoch, a.Auth.Gen,
		a.Ledger, a.LedgerSeq, []mqttDutyDeclaration{
			{Kind: mqttDutyExchange, Ref: a.Exchange.Nonce, PI: a.Exchange.PI, State: mqttDutyPending},
		})
	if ierr != nil {
		return nil, fmt.Errorf("admitting a publish: %w", ierr)
	}
	members = append(members, inv...)

	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: a.Auth.ID, Epoch: a.Auth.Epoch, Txn: a.Txn, Op: a.Op,
			Kind: mqttTransitionAdmitExchange, PrevTxn: a.Auth.OwnerTxn, Rev: a.Auth.Rev + 1,
			Gen: a.Auth.Gen, Tenure: a.Auth.Tenure, OwnerTxn: a.Auth.OwnerTxn,
		},
		Prev: a.Auth, PrevSeq: a.AuthSeq,
		Auth: &mqttOutboundAuthority{
			ID: a.Auth.ID, Epoch: a.Auth.Epoch, Gen: a.Auth.Gen, Tenure: a.Auth.Tenure,
			State: mqttAuthorityServing, Rev: a.Auth.Rev + 1, Txn: a.Txn, OwnerTxn: a.Auth.OwnerTxn,
		},
		Extra: members,
	}, nil
}
