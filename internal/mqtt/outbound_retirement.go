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
)

func mqttValidResourceKind(k mqttResourceKind) bool {
	switch k {
	case mqttResourceSourceConsumer, mqttResourcePubRelConsumer, mqttResourceRetainedPayload:
		return true
	}
	return false
}

// mqttResourceState is where a resource stands in its retirement.
//
// From 1, as every F-105 enum is: a missing member must not decode as the permissive value,
// and here the permissive value would authorize a deletion.
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
	// Deps NAMES the admitted obligations that still need this resource, by exchange nonce.
	//
	// A COUNT WAS NOT ENOUGH, and the difference is the whole authorization. A count can be
	// decremented by a transition that ended nothing: "one fewer" is an assertion, and a
	// caller draining a count one step at a time reaches zero with every obligation still
	// outstanding. A named edge can only be removed by proving THAT obligation reached its
	// end, and it can only be removed once, because the second attempt names something the
	// list no longer holds.
	//
	// Kept as a list rather than a record class of its own: the four properties that matter
	// — an edge is added only by the admission that creates its exchange, removed only on
	// that exchange's terminal evidence, removed exactly once, and all of it in the same CAS
	// as the count — are all properties of this record, and a separate class would need its
	// own subject space, decoder, walk filter, sweep exclusion and census row to state the
	// same thing.
	Deps []string `json:"deps"`
	// AuthTxn is the transition that authorized deletion, present from
	// DELETE_AUTHORIZED onwards.
	AuthTxn string `json:"auth_txn,omitempty"`
	// Outcome is the classified cleanup result, present at CLEANUP_COMPLETE.
	Outcome mqttCleanupOutcome `json:"outcome,omitempty"`
}

var mqttResourceMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "inc": {}, "kind": {}, "name": {}, "state": {},
	"deps": {}, "auth_txn": {}, "outcome": {},
}

var mqttResourceRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "inc": {}, "kind": {}, "name": {}, "state": {}, "deps": {},
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
	// AN EMPTY LIST IS WRITTEN AS `[]`, NEVER `null`. A null would decode to a nil slice
	// that reads identically to an empty one, and "no record of any dependent" would then be
	// indistinguishable from "this resource is genuinely free" — which is the
	// absence-as-a-fact inference every other record here refuses.
	if r.Deps == nil {
		return fmt.Errorf("outbound resource carries a null dependent list; an empty one is written as []")
	}
	seen := make(map[string]struct{}, len(r.Deps))
	for _, n := range r.Deps {
		if !mqttF105IdentTk(n) {
			return fmt.Errorf("outbound resource names a malformed dependent obligation %q", n)
		}
		if _, dup := seen[n]; dup {
			return fmt.Errorf("outbound resource names obligation %q twice: an obligation "+
				"depends on a resource once, and a duplicate would need releasing twice", n)
		}
		seen[n] = struct{}{}
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
func mqttResourceTakeDependency(r *mqttOutboundResource, nonce string) (*mqttOutboundResource, error) {
	if r == nil {
		return nil, fmt.Errorf("taking a dependency requires the resource")
	}
	if r.State != mqttResourceLive {
		return nil, fmt.Errorf("resource %q is in state %d and admits no dependency", r.Inc, r.State)
	}
	if !mqttF105IdentTk(nonce) {
		return nil, fmt.Errorf("resource %q was offered a malformed dependent obligation %q", r.Inc, nonce)
	}
	for _, n := range r.Deps {
		if n == nonce {
			return nil, fmt.Errorf("obligation %q already depends on resource %q", nonce, r.Inc)
		}
	}
	next := *r
	next.Deps = append(append([]string{}, r.Deps...), nonce)
	return &next, nil
}

// mqttResourceReleaseDependency removes ONE named edge.
//
// It refuses an edge the resource does not hold, which is what makes a release consumable
// exactly once: a second attempt names something that is no longer there, rather than
// decrementing a count that cannot tell the two apart.
func mqttResourceReleaseDependency(r *mqttOutboundResource, nonce string) (*mqttOutboundResource, error) {
	if r == nil {
		return nil, fmt.Errorf("releasing a dependency requires the resource")
	}
	if r.State != mqttResourceLive {
		return nil, fmt.Errorf("resource %q is in state %d and admits no dependency change", r.Inc, r.State)
	}
	out := make([]string, 0, len(r.Deps))
	found := false
	for _, n := range r.Deps {
		if n == nonce && !found {
			found = true
			continue
		}
		out = append(out, n)
	}
	if !found {
		return nil, fmt.Errorf("obligation %q does not depend on resource %q, so there is "+
			"nothing to release", nonce, r.Inc)
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
