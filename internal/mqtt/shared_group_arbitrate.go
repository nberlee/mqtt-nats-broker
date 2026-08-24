// Copyright 2025 The NATS Authors
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

// Proving this broker still owns a shared membership before it serves the group.
//
// THE FINDING (F-077). No route that restores shared group delivery interest revalidates
// that this broker still owns the record the membership was read from. Broker A holds a
// resident session; B accepts a newer CONNECT for the same client ID and its path removes
// the membership; B's last-holder scan deletes the group durable; A creates it again and
// CONNACKs. A then serves group deliveries under a membership the authoritative record no
// longer carries — and "serves" includes ACKNOWLEDGING: mqttDeliverMsgCbQoS12 acks and
// drops on a reserved-subject violation, on a subscribe deny, on No Local suppression, and
// delivers-and-acks a shared QoS 0 member. Each of those destroys a group message for the
// members that legitimately hold the membership.
//
// F-076 proved the durable's CONFIGURATION on resume. This proves OWNERSHIP, which is a
// different fact and needs a different instrument.
//
// WHY A WRITE AND NOT A READ. A read cannot arbitrate. JetStream returns 10164 from the
// clustered pre-proposal check while a write for the subject is in process, so "the record
// did not move" is not provable from a load — the contender's proposal can commit a moment
// later. Only a landed per-subject CAS establishes ownership, and it establishes it at its
// own linearization point. See docs/ARCHITECTURE.md, "A definite record-CAS rejection is
// outstanding work", and feedback_durable_arbitration_not_dedup.
//
// WHAT THIS DOES NOT PROVE. Ownership of the RECORD, never presence of the CONSUMER.
// CONSUMER.DELETE carries no precondition field at all, so no finite check binds the
// durable's existence to the record's ownership: the durable may vanish at any point after
// the last verification or write, this CAS included. That is F-076's stated residual and
// docs/CONFORMANCE.md carries it.
//
// SCOPE. This file is the CONNECT/resume half. A live SUBSCRIBE installs and revives the
// same interest with no arbitration at all, which is F-078: it needs a packet-scoped
// transmission gate this path does not, because the restore gate already withholds
// everything until the CONNACK.
//
// See docs/ARCHITECTURE.md, "A restored shared membership is arbitrated against the
// record".
package mqtt

import "sync/atomic"

// mqttSharedJoinResult is what ONE processSharedJSConsumer invocation did.
//
// It is a struct rather than a widening result list because three of its facts were
// previously inferred from one another and each inference was wrong in a different way:
//
//   - `charged` used to stand in for "this call created the durable". It cannot: the S1
//     repair creates a durable whose charge this session object already holds, so it
//     creates with charged == 0. A verify create gated on charged would then be skipped
//     for exactly the population that just recreated a durable.
//   - `created` in turn cannot stand in for the arbitration trigger. A resume that finds
//     every durable present-and-canonical creates nothing and still installs delivery
//     interest under a membership it has not revalidated — that is F-077's R1.
//   - and `charged` must remain the refund's fact alone, because processSharedJSConsumer
//     returns cc whether or not THIS call charged, so refunding by durable would erase an
//     earlier connection's charge and leave tmaxack permanently short.
//
// So the three are separate fields and each has exactly one consumer.
type mqttSharedJoinResult struct {
	cc  *ConsumerConfig
	sub *subscription
	// charged is the MaxAckPending this invocation added to tmaxack, and 0 when it reused
	// a resident charge. The per-filter refund reads this and nothing else.
	charged int
	// created is true when this invocation issued a group-durable create — the join arm,
	// or the S1 repair. The post-subscribe verify create reads this.
	created bool
	// installed is the ARBITRATION TRIGGER: this invocation installed, repaired, adopted
	// or refreshed shared delivery interest. Every one of those re-authorises delivery
	// under a membership this operation has not revalidated, which is why the trigger is
	// "interest was touched" rather than "a durable was created".
	//
	// It is aggregated on the CALLER'S STACK, never on the session: a fact parked on
	// mqttSession outlives the operation that raised it and would govern the next one,
	// which is exactly the stale-generation defect F-078 has to solve for its packet gate.
	installed bool
}

// mqttGroupOwnership is what one ownership write established.
//
// It is a distinct enum rather than a reuse of mqttRepairSaveVerdict because the two
// rejection KINDS reach it by different routes that must not be collapsed: 10071 disposes
// without consulting the resolver and classifies its own lineage, while 10164 goes through
// resolveContendedRecordSave. A single verdict type is what lets the caller act on the
// outcome without knowing which route produced it.
type mqttGroupOwnership int

const (
	// mqttGroupOwnershipOurs: a CAS landed, so this broker still owned the record at the
	// write's linearization point. The only outcome that permits serving.
	mqttGroupOwnershipOurs mqttGroupOwnership = iota
	// mqttGroupOwnershipForeign: another owner holds the record. Demonstrated takeover.
	mqttGroupOwnershipForeign
	// mqttGroupOwnershipForked: the record claims our parent — our write was erased by
	// something not demonstrably a successor of ours, and NOBODY was demonstrated to own
	// the session. Distinct from Foreign because it licenses a different thing to SAY
	// (0x89, "reconnect", rather than 0x8E, "stop").
	mqttGroupOwnershipForked
	// mqttGroupOwnershipUnresolved: the write's outcome is unknown. The caller KEEPS its
	// mutation and performs no further write; the resolver settles it.
	mqttGroupOwnershipUnresolved
	// mqttGroupOwnershipLocal: a definite local failure. Nothing was stored.
	mqttGroupOwnershipLocal
)

// mqttGroupOwnershipVerdict is one ownership write's outcome, plus what it licenses the
// broker to say about it.
//
// voc travels WITH the outcome rather than being derived by the caller, because the two
// routes to Foreign/Forked compute it differently — disposalForDirectRejection reads the
// record for the 10071 arm, mqttDisposalForRepairSave maps the resolver's verdict for the
// 10164 arm — and a caller re-deriving it would have to know which route ran.
type mqttGroupOwnershipVerdict struct {
	outcome mqttGroupOwnership
	// voc is set on Foreign and Forked, and is the zero vocabulary otherwise.
	voc mqttDisposalVocabulary
	// err is the store-boundary error, for the caller's log and refusal text. nil on Ours.
	err error
}

// proveSharedMembershipOwnership writes the session record under its per-subject CAS and
// classifies the result, so a caller that has just restored shared group interest can tell
// whether this broker still owns the membership that interest is keyed on.
//
// THE WRITE IS THE POINT. Its body is the record the ordinary path would marshal, and on a
// resume that is normally the same membership that was read — so this looks like a no-op
// rewrite and is not one. persistRecord stamps a fresh WID and parents it to the current
// lineage at the store boundary, so every ownership write advances the record's lineage
// identity, and the CAS at sess.seq is what no read could be.
//
// The verdict switch is deliberately the same shape as the repair save's in
// sess_resume_reconcile.go, and the two rules it inherits are not re-derived here:
//
//   - 10071 (the single-writer check) means another broker wrote after our accept save.
//     It disposes without ever consulting the resolver, so this is the ONLY place a
//     sibling lineage can be recognised on that arm — disposalForDirectRejection reads the
//     record once, classifies it, and MINTS THE FORK GRANT. Collapsing it to a bare
//     takeover would assert 0x8E where the record supports only 0x89, and would lose the
//     grant entirely. See sess_fork_lineage.go.
//   - 10164 signals clustered contention, not a lost race, and gets the bounded
//     re-read/retry treatment. See resolveContendedRecordSave.
//
// tok must be the CALLER'S OWN open transaction — acceptTok on the CONNECT path. Both
// helpers this delegates to state that requirement: storeFramedSessionMsg's raw re-send is
// safe without re-validating the fence precisely because the caller holds a transaction on
// this goroutine, and mintForkedGrant's contract requires the classification to happen
// inside it. Opening a fresh transaction here instead would satisfy neither
// (feedback_fence_before_mutation).
//
// No lock held on entry. Performs JetStream requests.
func (sess *mqttSession) proveSharedMembershipOwnership(tok recordTx) mqttGroupOwnershipVerdict {
	outcome, detail, err := sess.saveClassifiedDetail(tok)
	if err == nil {
		return mqttGroupOwnershipVerdict{outcome: mqttGroupOwnershipOurs}
	}

	if outcome == saveMaybeLanded {
		// The record may already carry this write. The caller KEEPS its mutation — that
		// is what the resolver re-attempts and proves durable — and no further write is
		// permitted until it settles.
		return mqttGroupOwnershipVerdict{outcome: mqttGroupOwnershipUnresolved, err: err}
	}

	switch {
	case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceErrF):
		// Definite 10071. One load, on the rejection path of a pre-CONNACK write, never on
		// a healthy connection; it narrows the verdict and mints the grant when the record
		// is a sibling rather than a successor.
		voc := sess.disposalForDirectRejection(&detail)
		out := mqttGroupOwnershipForeign
		if voc == mqttDisposalForked {
			out = mqttGroupOwnershipForked
		}
		return mqttGroupOwnershipVerdict{outcome: out, voc: voc, err: err}

	case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceConstantErr):
		switch v := sess.resolveContendedRecordSave(&detail); v {
		case mqttRepairSaveLanded:
			return mqttGroupOwnershipVerdict{outcome: mqttGroupOwnershipOurs}
		case mqttRepairSaveForeign:
			return mqttGroupOwnershipVerdict{
				outcome: mqttGroupOwnershipForeign, voc: mqttDisposalForRepairSave(v), err: err}
		case mqttRepairSaveForked:
			return mqttGroupOwnershipVerdict{
				outcome: mqttGroupOwnershipForked, voc: mqttDisposalForRepairSave(v), err: err}
		case mqttRepairSaveUnresolved:
			// The pending-write state is already established by the resolver — a CAS
			// rejection is definite, so save() did not mark it uncertain, and without that
			// the resolver would have nothing to classify or re-attempt later.
			return mqttGroupOwnershipVerdict{outcome: mqttGroupOwnershipUnresolved, err: err}
		default: // mqttRepairSaveLocalDefinite
			return mqttGroupOwnershipVerdict{outcome: mqttGroupOwnershipLocal, err: err}
		}

	default:
		// A definite non-CAS failure: a refused fence, a refusal at entry, a test seam.
		// Nothing was stored and sess.seq is right.
		return mqttGroupOwnershipVerdict{outcome: mqttGroupOwnershipLocal, err: err}
	}
}

// mqttTestSharedArbitratePauseV pauses the restore at one named boundary of the shared
// group arbitration, so a test can act while the broker is between two specific steps.
//
// The boundaries are anchored to the CREATES, not to the ownership CAS, and that is the
// whole reason the seam is shaped this way: a pause point that moved with the code a
// mutation moves would discriminate nothing. A mutation that relocates the CAS relative to
// mqtt.go's second group-durable create is observable only because these two boundaries
// stay where the creates are.
//
//	"after-group-create:<sid>"  immediately after the join arm's create
//	"after-group-verify:<sid>"  immediately after the post-subscribe verify create
//	"after-ownership-cas"       after the verdict, before the CONNACK
//
// Nil in production.
var mqttTestSharedArbitratePauseV atomic.Pointer[func(string)]

func mqttTestSharedArbitratePause(boundary string) {
	if h := mqttTestSharedArbitratePauseV.Load(); h != nil {
		(*h)(boundary)
	}
}
