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
	"encoding/json"
	"fmt"
	"strings"
)

// Resource retirement (F-105).
//
// THE DEFECT THIS CLOSES, stated plainly: today a teardown deletes the ordinary source
// consumers and then the PUBREL consumer, and `$MQTT_out` is interest-retained, so deleting
// that consumer drops the records it carried — the only evidence the client answered PUBREC.
// An obligation admitted before that teardown is left with no completion route at all. The
// register may survive and prove nothing.
//
// So destruction stops being something a pass decides and becomes something a resource
// record AUTHORIZES. A resource carries the count of obligations that still depend on it,
// and the transition that authorizes its deletion contends on the resource record itself —
// against the very transitions that would add another dependency. Winning that CAS therefore
// establishes two things at once: the count was zero, and no new dependency can be added
// afterwards. Neither is provable by looking.
//
// Three consequences worth stating because each one is a mistake this project has made:
//
//   - RETIREMENT IS NOT DELETION. Closing a generation to new admission says nothing about
//     whether its obligations finished; only their own evidence says that.
//   - A SUCCESSFUL DELETE IS NOT A COMPLETED CLEANUP. The duty records its classified
//     outcome, and a resource that turned out to be gone, or replaced, is classified rather
//     than counted as removed.
//   - AN ABSENT RESOURCE RECORD IS NOT AN UNUSED ONE. A pass that cannot read a resource
//     leaves it alone; the only thing that licenses deletion is a won authorization.

// mqttResourceClassTk is the record class for a retirable backing resource.
const mqttResourceClassTk = "res"

// mqttResourceKind is the closed set of resources a generation owns and a retirement may
// eventually reclaim.
type mqttResourceKind string

const (
	// mqttResourceSourceConsumer is an ordinary delivery consumer.
	mqttResourceSourceConsumer mqttResourceKind = "source"
	// mqttResourcePubRelConsumer is the PUBREL consumer. Deleting it drops its
	// interest-retained records, which is exactly why it may not be deleted while an
	// exchange still depends on it.
	mqttResourcePubRelConsumer mqttResourceKind = "pubrel"
	// mqttResourceRetainedPayload is durable replay material for a retained send.
	mqttResourceRetainedPayload mqttResourceKind = "payload"
	// mqttResourceSharedGroupDurable is a shared-subscription GROUP consumer, and it is the
	// one kind this generation does not OWN.
	//
	// Its name is DERIVED from the subscription key, so every member of the group derives the
	// same one and several generations hold it at once. Two things follow, and both are why it
	// needs a kind of its own rather than passing as a source consumer:
	//
	//   - it is not PROVISIONED by the generation that enrols it. The durable already exists,
	//     or another member created it; a provisioning duty here would send an executor to
	//     create something that is not this generation's to create.
	//   - its cleanup is CONDITIONAL. Deleting it while another session still holds the group
	//     would take a consumer that is serving somebody else, so the executor re-asks the
	//     last-holder question at execution time and treats an unresolved answer as a refusal.
	//     A source consumer's cleanup is unconditional, so enrolling one of these AS a source
	//     is the last-holder defect reintroduced through the inventory.
	mqttResourceSharedGroupDurable mqttResourceKind = "shared"
)

func mqttValidResourceKind(k mqttResourceKind) bool {
	switch k {
	case mqttResourceSourceConsumer, mqttResourcePubRelConsumer, mqttResourceRetainedPayload,
		mqttResourceSharedGroupDurable:
		return true
	}
	return false
}

// mqttProvisioningState is whether anything can still MATERIALIZE a resource.
//
// Deliberately a separate axis from mqttResourceState. D5 lists "provisioning status" beside
// identity, dependency counts and deletion state precisely because they answer different
// questions, and it makes provisioning sealed a PRECONDITION of deletion authorization.
type mqttProvisioningState uint8

const (
	// mqttProvisioningPending: the record stands for a durable whose creation is not settled.
	// A delayed create may still land, so the name is not free and deletion is not authorized
	// however few dependents remain.
	mqttProvisioningPending mqttProvisioningState = 1
	// mqttProvisioningSealed: settled. Either the durable was confirmed created, or its
	// non-creation was established, and no delayed materialization can follow. This is the
	// POSITIVE fact D5 requires; it is never inferred from a lookup that found nothing.
	mqttProvisioningSealed mqttProvisioningState = 2
)

func mqttValidProvisioningState(s mqttProvisioningState) bool {
	return s == mqttProvisioningPending || s == mqttProvisioningSealed
}

// mqttResourceState is where a resource stands in its retirement.
//
// From 1, as every F-105 enum is: a missing member must not decode as the permissive value,
// and here the permissive value would authorize a deletion. mqttProvisioningState above is
// numbered from 1 for exactly the same reason.
type mqttResourceState uint8

const (
	// mqttResourceLive is in use. Dependencies may still be added.
	mqttResourceLive mqttResourceState = 1
	// mqttResourceDeleteAuthorized records a WON authorization: at the instant of that CAS
	// the dependency count was zero, and no dependency may be added afterwards.
	mqttResourceDeleteAuthorized mqttResourceState = 2
	// mqttResourceCleanupComplete records the classified outcome of the deletion.
	mqttResourceCleanupComplete mqttResourceState = 3
)

func mqttValidResourceState(s mqttResourceState) bool {
	return s == mqttResourceLive || s == mqttResourceDeleteAuthorized || s == mqttResourceCleanupComplete
}

// mqttCleanupOutcome is the classified result of acting on an authorized deletion.
//
// A successful delete is NOT a completed cleanup: an exact-sequence delete succeeds for an
// already-gone sequence, and a peer that acquired the subject rewrites at a new one. The
// classification is what keeps those apart.
type mqttCleanupOutcome string

const (
	mqttCleanupRemoved  mqttCleanupOutcome = "removed"  // this pass removed it
	mqttCleanupGone     mqttCleanupOutcome = "gone"     // already absent when acted on
	mqttCleanupReplaced mqttCleanupOutcome = "replaced" // a different incarnation occupies the name
)

func mqttValidCleanupOutcome(o mqttCleanupOutcome) bool {
	return o == mqttCleanupRemoved || o == mqttCleanupGone || o == mqttCleanupReplaced
}

// mqttResourceSubject returns the durable subject of one resource incarnation.
//
// Keyed by INCARNATION rather than by name. A consumer name can be recreated, and a
// retirement that addressed the name would authorize deleting a successor that merely reuses
// it; the incarnation token is what makes an authorization about the thing it was computed
// for.
func mqttResourceSubject(idHash, gen, inc string) string {
	return mqttF105SubjectPrefix() + mqttResourceClassTk + "." + idHash + "." + gen + "." + inc
}

// mqttResourceWalkFilter enumerates one generation's resources, and only those.
func mqttResourceWalkFilter(idHash, gen string) string {
	return mqttF105SubjectPrefix() + mqttResourceClassTk + "." + idHash + "." + gen + ".*"
}

// mqttResourceRole is the closed set of reasons an obligation needs a resource.
//
// D5 fixes these six and they are not free text: the whole point of a role is that releasing one
// proves THAT need ended and says nothing about the others.
type mqttResourceRole string

const (
	// mqttRoleSourceReplay: the resource is required for permitted replay.
	mqttRoleSourceReplay mqttResourceRole = "source-replay"
	// mqttRoleCanonicalPubRel: the resource carries the release obligation. OWED and STORED are
	// STATES of this one role, not two roles — the need is the same need either way.
	mqttRoleCanonicalPubRel mqttResourceRole = "canonical-pubrel"
	// mqttRoleOutputAction: an acquired output needs the resource until its duty drains.
	mqttRoleOutputAction mqttResourceRole = "output-action"
	// mqttRoleAckHandler: the captured completion route needs it. DISTINCT from the output
	// action even when both are bound to the same acquisition, because their discharge evidence
	// is different — output drains, a handler is discharged.
	mqttRoleAckHandler mqttResourceRole = "ack-handler"
	// mqttRoleSourceSettlement: needed until settlement is durably resolved. DISTINCT from
	// replay for the same reason: different evidence discharges it.
	mqttRoleSourceSettlement mqttResourceRole = "source-settlement"
	// mqttRoleSharedAffine: the exact shared or affine obligation needs it. A role FAMILY, which
	// is why its Ref is required — it never licenses merging every such duty into one edge.
	mqttRoleSharedAffine mqttResourceRole = "shared-affine"
)

func mqttValidResourceRole(r mqttResourceRole) bool {
	switch r {
	case mqttRoleSourceReplay, mqttRoleCanonicalPubRel, mqttRoleOutputAction,
		mqttRoleAckHandler, mqttRoleSourceSettlement, mqttRoleSharedAffine:
		return true
	}
	return false
}

// mqttRoleNeedsRef reports whether a role can have several INDEPENDENTLY OUTSTANDING
// constituents, so that naming the role alone would not name the need.
//
// This is the refinement that stops roles recreating the very collapse they fix. A role is
// enough only when it stands for ONE aggregate obligation for the (exchange, resource) pair.
// An exchange can hold several acquisitions and several captured handlers at once, and a
// shared/affine edge is explicitly one family covering distinct duties — so those three name
// their constituent, and releasing one says nothing about the rest. Collapsing them would be the
// bare-nonce defect one level further in.
func mqttRoleNeedsRef(r mqttResourceRole) bool {
	switch r {
	case mqttRoleOutputAction, mqttRoleAckHandler, mqttRoleSharedAffine:
		return true
	}
	return false
}

// mqttRoleDischargeEvidence is what the boundary must establish before a role's edge is removed.
//
// ROUND I-2's THIRD FINDING, and it is the one that mattered most: adding Role and Ref
// DISTINGUISHES edges but does not ENFORCE their different release evidence. The boundary still
// checked only mqttExchangePhaseEnded — protocol completion with no outstanding output — which is
// not a certificate that source settlement, a captured handler's duty or a shared/affine
// disposition has finished. So an ended exchange could release its source-settlement edge, that
// edge could be the last one, and a provisioning-sealed resource the unfinished duty still needs
// became deletable. F-105's own defect, reached through the mechanism built to prevent it.
//
// The table below is therefore the point of the roles rather than a detail of them.
type mqttRoleDischargeEvidence uint8

const (
	// mqttDischargeByExchangeEnding: the exchange's durable ending IS this role's evidence.
	// True for replay and for the canonical release, because both are things the protocol
	// itself concludes — an exchange at Terminal has nothing left to replay and owes no release.
	mqttDischargeByExchangeEnding mqttRoleDischargeEvidence = 1
	// mqttDischargeNotYetEstablished: this role has its own discharge evidence and this broker
	// cannot yet produce it durably.
	//
	// The edge is therefore NOT RELEASABLE, and that is the correct answer rather than a gap
	// papered over: releasing on the exchange's ending would be exactly the unsound step the
	// finding names. It fails CLOSED — the resource stays undeletable until the evidence exists
	// — which costs reclamation and never costs an obligation.
	mqttDischargeNotYetEstablished mqttRoleDischargeEvidence = 2
)

// mqttRoleDischarge reports what evidence releasing this role requires.
func mqttRoleDischarge(r mqttResourceRole) mqttRoleDischargeEvidence {
	switch r {
	case mqttRoleSourceReplay, mqttRoleCanonicalPubRel:
		return mqttDischargeByExchangeEnding
	}
	// output-action, ack-handler, source-settlement and shared-affine each end on evidence the
	// exchange's phase does not carry: an acquisition's drain, a captured handler's discharge,
	// a settled source ack, an exact shared or affine disposition.
	return mqttDischargeNotYetEstablished
}

// mqttResourceEdge is ONE obligation's need for one resource, in one role.
//
// Identity is the WHOLE triple. Two edges differing only in role are two different needs with
// two different discharges, and the record carries both until both are proven.
type mqttResourceEdge struct {
	// Nonce is the exchange whose obligation needs the resource.
	Nonce string `json:"nonce"`
	// Role is why it needs it.
	Role mqttResourceRole `json:"role"`
	// Ref names the exact constituent for a role that can have several outstanding at once —
	// the acquisition, the captured binding, the exact shared or affine duty. REQUIRED for
	// those roles and FORBIDDEN for the aggregate ones, so a reader never has to wonder whether
	// an empty Ref means "the whole role" or "somebody forgot".
	Ref string `json:"ref,omitempty"`
}

func (e mqttResourceEdge) String() string {
	if e.Ref == _EMPTY_ {
		return string(e.Role) + ":" + e.Nonce
	}
	return string(e.Role) + ":" + e.Nonce + "/" + e.Ref
}

// mqttValidateResourceEdge refuses an edge this broker cannot act on.
func mqttValidateResourceEdge(e mqttResourceEdge) error {
	if !mqttF105IdentTk(e.Nonce) {
		return fmt.Errorf("names a malformed dependent obligation %q", e.Nonce)
	}
	if !mqttValidResourceRole(e.Role) {
		return fmt.Errorf("obligation %q carries an unsupported role %q", e.Nonce, e.Role)
	}
	needs := mqttRoleNeedsRef(e.Role)
	if needs && e.Ref == _EMPTY_ {
		return fmt.Errorf("obligation %q needs this resource in role %q, which can have several "+
			"constituents outstanding at once, so the edge must name the exact one it stands for",
			e.Nonce, e.Role)
	}
	if !needs && e.Ref != _EMPTY_ {
		return fmt.Errorf("obligation %q carries a constituent %q on aggregate role %q: an "+
			"aggregate role is one need for the pair, and a reference here would describe a "+
			"partial discharge the role cannot express", e.Nonce, e.Ref, e.Role)
	}
	if needs && !mqttF105IdentTk(e.Ref) {
		return fmt.Errorf("obligation %q carries a malformed constituent %q", e.Nonce, e.Ref)
	}
	return nil
}

// mqttOutboundResource is a backing resource and what still depends on it.
type mqttOutboundResource struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	// Inc is this resource's incarnation identity, minted when the resource is provisioned
	// and never reused.
	Inc  string           `json:"inc"`
	Kind mqttResourceKind `json:"kind"`
	// Name is the durable name to act on — a consumer name, a payload manifest id.
	Name  string            `json:"name"`
	State mqttResourceState `json:"state"`
	// Prov is whether this resource's PHYSICAL provisioning is settled (F-105, row 8; D5
	// requires it and names it as a precondition of deletion authorization).
	//
	// It is not the same fact as State, and conflating them is the defect it exists to
	// prevent. State says where the resource stands in its RETIREMENT; Prov says whether
	// anything can still MATERIALIZE it. A record is written before the durable it stands for
	// is confirmed to exist — opProvisionResource commits the record, and the CONSUMER.CREATE
	// is a separate act whose outcome may be unknown — so a resource can legitimately be LIVE
	// with its provisioning still in flight.
	//
	// Authorizing deletion in that window is how an orphan is made: the delete finds nothing,
	// the delayed create lands afterwards, and the account now carries a durable that no
	// record names and no sweep will attribute. That is the false-reachability class
	// (feedback_false_reachability_class) reached from the other direction — not deleting
	// something still needed, but deleting the NAME of something not yet made.
	Prov mqttProvisioningState `json:"prov"`
	// Deps NAMES the obligations that still need this resource, each with the ROLE it needs
	// it in.
	//
	// A COUNT WAS NOT ENOUGH, and the difference is the whole authorization. A count can be
	// decremented by a transition that ended nothing: "one fewer" is an assertion, and a
	// caller draining a count one step at a time reaches zero with every obligation still
	// outstanding. A named edge can only be removed by proving that need ended, and it can
	// only be removed once, because the second attempt names something the list no longer
	// holds.
	//
	// A BARE NONCE WAS NOT ENOUGH EITHER, and that is the later correction. One exchange can
	// need one resource in several roles at once — a QoS 2 exchange needs the PUBREL consumer
	// for its canonical record AND for the output that emits it — and with one edge for all of
	// them the edge's release must stand for all of them. What the boundary actually checked
	// was mqttExchangePhaseEnded, and Terminal is PROTOCOL completion with no outstanding
	// output: it is not a certificate that source settlement, captured-handler duties and
	// shared/affine dispositions have finished. So a bare edge released on protocol terminality
	// frees a resource a settlement duty still needs — F-105's own defect, one level down.
	//
	// Kept as a list rather than a record class of its own: the properties that matter — an
	// edge is added only by the admission that creates its exchange, removed only on THAT
	// ROLE's discharge, removed exactly once, and all of it in the same CAS that contends with
	// deletion authorization — are all properties of this record, and a separate class would
	// need its own subject space, decoder, walk filter, sweep exclusion and census row to state
	// the same thing.
	Deps []mqttResourceEdge `json:"deps"`
	// AuthTxn is the transition that authorized deletion, present from
	// DELETE_AUTHORIZED onwards.
	AuthTxn string `json:"auth_txn,omitempty"`
	// Outcome is the classified cleanup result, present at CLEANUP_COMPLETE.
	Outcome mqttCleanupOutcome `json:"outcome,omitempty"`
}

var mqttResourceMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "inc": {}, "kind": {}, "name": {}, "state": {},
	"prov": {}, "deps": {}, "auth_txn": {}, "outcome": {},
}

var mqttResourceRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "inc": {}, "kind": {}, "name": {}, "state": {}, "prov": {},
	"deps": {},
}

var mqttResourceOptional = map[string]struct{}{"auth_txn": {}, "outcome": {}}

// mqttDecodeOutboundResource decodes and fully validates one resource body.
func mqttDecodeOutboundResource(data []byte, wantID, wantEpoch, wantGen, wantInc string) (*mqttOutboundResource, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttResourceMembers, "an outbound resource"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttResourceRequired, "an outbound resource"); err != nil {
		return nil, err
	}
	if err := mqttJSONOptionalMembersNotNull(data, mqttResourceOptional, "an outbound resource"); err != nil {
		return nil, err
	}
	r := &mqttOutboundResource{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}
	if err := mqttValidateOutboundResource(r, wantID, wantEpoch, wantGen, wantInc); err != nil {
		return nil, err
	}
	return r, nil
}

// mqttValidateOutboundResource refuses a body this broker does not support.
func mqttValidateOutboundResource(r *mqttOutboundResource, wantID, wantEpoch, wantGen, wantInc string) error {
	if r == nil {
		return fmt.Errorf("outbound resource is absent")
	}
	if r.ID != wantID {
		return fmt.Errorf("outbound resource names client %q, not %q", r.ID, wantID)
	}
	if r.Epoch != wantEpoch {
		return fmt.Errorf("outbound resource carries recovery epoch %q, not this account's %q", r.Epoch, wantEpoch)
	}
	if r.Gen != wantGen {
		return fmt.Errorf("outbound resource names generation %q on the subject of %q", r.Gen, wantGen)
	}
	if r.Inc != wantInc {
		return fmt.Errorf("outbound resource names incarnation %q on the subject of %q", r.Inc, wantInc)
	}
	if !mqttF105IdentTk(r.Inc) {
		return fmt.Errorf("outbound resource carries a malformed incarnation %q", r.Inc)
	}
	if !mqttValidResourceKind(r.Kind) {
		return fmt.Errorf("outbound resource carries an unsupported kind %q", r.Kind)
	}
	if r.Name == _EMPTY_ {
		return fmt.Errorf("outbound resource carries no durable name")
	}
	if !mqttValidResourceState(r.State) {
		return fmt.Errorf("outbound resource carries an unsupported state %d", r.State)
	}
	if !mqttValidProvisioningState(r.Prov) {
		return fmt.Errorf("outbound resource carries an unsupported provisioning status %d: a "+
			"resource whose materialization is not stated is one whose name cannot be freed",
			r.Prov)
	}
	// AN EMPTY LIST IS WRITTEN AS `[]`, NEVER `null`. A null would decode to a nil slice
	// that reads identically to an empty one, and "no record of any dependent" would then be
	// indistinguishable from "this resource is genuinely free" — which is the
	// absence-as-a-fact inference every other record here refuses.
	if r.Deps == nil {
		return fmt.Errorf("outbound resource carries a null dependent list; an empty one is written as []")
	}
	// IDENTITY IS THE WHOLE TRIPLE. Two edges differing only in role are two different needs
	// with two different discharges, so the duplicate rule is keyed on (nonce, role, ref) and
	// NOT on the nonce — keying it on the nonce would forbid the very thing roles exist to
	// express.
	seen := make(map[mqttResourceEdge]struct{}, len(r.Deps))
	for _, e := range r.Deps {
		if verr := mqttValidateResourceEdge(e); verr != nil {
			return fmt.Errorf("outbound resource %w", verr)
		}
		if _, dup := seen[e]; dup {
			return fmt.Errorf("outbound resource names dependency %s twice: a need is held "+
				"once, and a duplicate would need releasing twice", e)
		}
		seen[e] = struct{}{}
	}
	switch r.State {
	case mqttResourceLive:
		if r.AuthTxn != _EMPTY_ || r.Outcome != _EMPTY_ {
			return fmt.Errorf("a live outbound resource describes a deletion it has not been authorized for")
		}
	case mqttResourceDeleteAuthorized:
		if !mqttF105IdentTk(r.AuthTxn) {
			return fmt.Errorf("an authorized outbound resource carries a malformed authorizing transition %q", r.AuthTxn)
		}
		if r.Outcome != _EMPTY_ {
			return fmt.Errorf("an authorized outbound resource describes a cleanup outcome it has not reached")
		}
		// THE INVARIANT THE AUTHORIZATION MEANS. A won authorization is a statement that
		// the count was zero; a body carrying both is self-contradictory, and admitting it
		// would let a deletion proceed over live dependencies.
		if len(r.Deps) != 0 {
			return fmt.Errorf("an authorized outbound resource still names %d dependent obligations", len(r.Deps))
		}
	case mqttResourceCleanupComplete:
		if !mqttF105IdentTk(r.AuthTxn) {
			return fmt.Errorf("a completed outbound resource carries a malformed authorizing transition %q", r.AuthTxn)
		}
		if !mqttValidCleanupOutcome(r.Outcome) {
			return fmt.Errorf("a completed outbound resource carries an unsupported outcome %q", r.Outcome)
		}
		if len(r.Deps) != 0 {
			return fmt.Errorf("a completed outbound resource still names %d dependent obligations", len(r.Deps))
		}
	}
	return nil
}

// mqttResourceDeletable reports whether a resource may be acted on destructively.
//
// Only a WON authorization licenses it. A live resource with no dependencies is NOT
// deletable: nothing has excluded a dependency arriving between the reading and the acting,
// and that exclusion is the whole content of the authorization.
func mqttResourceDeletable(r *mqttOutboundResource) bool {
	return r != nil && r.State == mqttResourceDeleteAuthorized
}

// mqttSealProvisioningTransition assembles the transition that seals one resource's
// provisioning.
//
// The EVIDENCE is the caller's, exactly as an obligation's terminal evidence is: this broker
// confirmed the durable was created, or established that it never will be. What the transition
// does is make that settled fact durable and irrevocable, so a deletion authorized later cannot
// be racing a create that is still in flight.
//
// It moves nothing else. A seal that also released an edge would carry two proofs under one
// transition, and the boundary refuses exactly that.
func mqttSealProvisioningTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	r *mqttOutboundResource, resSeq uint64) (*mqttAuthorityTransition, error) {

	if prev == nil {
		return nil, fmt.Errorf("sealing provisioning requires the authority it runs under")
	}
	if r == nil {
		return nil, fmt.Errorf("sealing provisioning requires the resource")
	}
	if resSeq == 0 {
		return nil, fmt.Errorf("sealing provisioning requires the sequence the resource was read at")
	}
	if r.State != mqttResourceLive {
		return nil, fmt.Errorf("resource %q is in state %d, so its provisioning is not this "+
			"transition's to seal", r.Inc, r.State)
	}
	// ALREADY SEALED IS REFUSED RATHER THAN TREATED AS DONE. A second seal would be a
	// transition that establishes nothing, and a caller that believes it sealed something it
	// did not is exactly the confusion an idempotent-looking success creates.
	if r.Prov == mqttProvisioningSealed {
		return nil, fmt.Errorf("resource %q is already sealed, so there is nothing to seal", r.Inc)
	}
	next := *r
	next.Prov = mqttProvisioningSealed
	body, err := json.Marshal(&next)
	if err != nil {
		return nil, err
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionDisposeResource, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityServing, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttResourceSubject(getHash(prev.ID), r.Gen, r.Inc),
			expect: resSeq,
			body:   body,
		}},
	}, nil
}

// mqttAuthorizeDeletionTransition assembles the transition that authorizes deleting one
// resource.
//
// It CASes the resource record at the sequence the caller read, which is what makes it
// contend with the transitions that add dependencies: both write this record, so exactly one
// wins. A deletion authorized this way carries the guarantee that the count was zero AND
// that no later dependency can be added, because a dependency addition after this point
// would have to beat a sequence that has already moved.
func mqttAuthorizeDeletionTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	r *mqttOutboundResource, resSeq uint64) (*mqttAuthorityTransition, error) {
	if prev == nil {
		return nil, fmt.Errorf("authorizing a deletion requires the authority it runs under")
	}
	if r == nil {
		return nil, fmt.Errorf("authorizing a deletion requires the resource")
	}
	if r.State != mqttResourceLive {
		return nil, fmt.Errorf("resource %q is in state %d, so its deletion is not this transition's to authorize",
			r.Inc, r.State)
	}
	if len(r.Deps) != 0 {
		return nil, fmt.Errorf("resource %q is still needed by %d obligation(s)", r.Inc, len(r.Deps))
	}
	// SEALED FIRST. Restated here as well as at the boundary because the two answer different
	// callers: this one tells the composer's caller why its authorization cannot be assembled,
	// and the boundary refuses a batch assembled by anything at all. Neither is redundant with
	// the other, which is the distinction the write boundary has had to make repeatedly.
	if r.Prov != mqttProvisioningSealed {
		return nil, fmt.Errorf("resource %q has provisioning status %d, so its deletion is not "+
			"authorizable: until nothing further can materialize it, its name is not free",
			r.Inc, r.Prov)
	}
	if resSeq == 0 {
		return nil, fmt.Errorf("authorizing a deletion requires the sequence the resource was read at")
	}
	next := *r
	next.State, next.AuthTxn = mqttResourceDeleteAuthorized, txn
	body, err := json.Marshal(&next)
	if err != nil {
		return nil, err
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionDisposeResource, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			// The predecessor's state is PRESERVED, not normalised to serving: this runs
			// after closure as often as before it.
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: prev.State, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
			// Both pending proposals ride through untouched: cleanup runs beside a
			// reservation or a preparation and resolves neither. Dropping the creation
			// proposal made the output invalid under RESERVING, which is a body this
			// broker would then refuse to read back.
			Intent: prev.Intent, Creation: prev.Creation,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttResourceSubject(getHash(prev.ID), r.Gen, r.Inc),
			expect: resSeq,
			body:   body,
		}},
	}, nil
}

// mqttResourceTakeDependency adds ONE named edge.
//
// Naming the obligation rather than bumping a number is what makes the increment derived: the
// write boundary requires the named exchange to be created by this same batch, so a caller
// cannot inflate the list and make a resource undeletable forever.
//
// Written as a CAS at the read sequence, deliberately the same contention point the deletion
// authorization uses: that is what makes "nothing depended on it, and nothing can be added"
// a property of the store rather than of the order two passes happened to run in.
func mqttResourceTakeDependency(r *mqttOutboundResource, e mqttResourceEdge) (*mqttOutboundResource, error) {
	if r == nil {
		return nil, fmt.Errorf("taking a dependency requires the resource")
	}
	if r.State != mqttResourceLive {
		return nil, fmt.Errorf("resource %q is in state %d and admits no dependency", r.Inc, r.State)
	}
	if verr := mqttValidateResourceEdge(e); verr != nil {
		return nil, fmt.Errorf("resource %q was offered a dependency that %w", r.Inc, verr)
	}
	for _, have := range r.Deps {
		if have == e {
			return nil, fmt.Errorf("dependency %s already stands on resource %q", e, r.Inc)
		}
	}
	next := *r
	next.Deps = append(append([]mqttResourceEdge{}, r.Deps...), e)
	return &next, nil
}

// mqttResourceReleaseDependency removes ONE named edge.
//
// It refuses an edge the resource does not hold, which is what makes a release consumable
// exactly once: a second attempt names something that is no longer there, rather than
// decrementing a count that cannot tell the two apart.
func mqttResourceReleaseDependency(r *mqttOutboundResource, e mqttResourceEdge) (*mqttOutboundResource, error) {
	if r == nil {
		return nil, fmt.Errorf("releasing a dependency requires the resource")
	}
	if r.State != mqttResourceLive {
		return nil, fmt.Errorf("resource %q is in state %d and admits no dependency change", r.Inc, r.State)
	}
	// RELEASES THE EXACT EDGE, never the obligation. An exchange holding the same resource in
	// two roles keeps the other one, which is the entire reason the role is in the key: a
	// release that matched on the nonce would discharge a need whose evidence nobody produced.
	out := make([]mqttResourceEdge, 0, len(r.Deps))
	found := false
	for _, have := range r.Deps {
		if have == e && !found {
			found = true
			continue
		}
		out = append(out, have)
	}
	if !found {
		return nil, fmt.Errorf("dependency %s does not stand on resource %q, so there is "+
			"nothing to release", e, r.Inc)
	}
	next := *r
	next.Deps = out
	return &next, nil
}

// mqttResourceIsF105Class reports whether a subject is a resource record. Used by the
// transition writer's member classifier, which admits obligation and resource members and
// refuses the arbitration records.
func mqttResourceIsF105Class(subject string) bool {
	return strings.HasPrefix(subject,
		mqttF105SubjectPrefix()+mqttResourceClassTk+".")
}
