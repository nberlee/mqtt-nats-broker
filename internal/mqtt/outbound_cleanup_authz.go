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
	"fmt"
	"strings"
)

// Destruction is authorized by the resource, not by the teardown (F-105, row 8).
//
// This is the defect F-105 was filed for, in its most direct form. deleteTeardownTargets
// removes the ordinary source consumers and then the PUBREL consumer, and because $MQTT_out is
// interest-retained, deleting that consumer drops the records it carried — which are the only
// evidence the client answered PUBREC. The register may survive and prove nothing. A teardown
// destroys the completion route of an obligation admitted before it, and never knew.
//
// # Why a count was not enough, restated where the deletion happens
//
// A resource NAMES the obligations depending on it, by exchange nonce. A count can be
// decremented by a transition that ended nothing — "one fewer" is an assertion — so a caller
// stepping it down reaches zero with every obligation still outstanding, and then authorizes
// deleting the consumer whose interest-retained records are those obligations' only evidence.
// A named edge is DERIVED at both ends: added only by the admission that creates its exchange
// in the same batch, removed only against that exchange's durable terminal phase.
//
// # And a live resource with no dependents is deliberately not deletable
//
// Winning the authorizing CAS on the resource record is what establishes BOTH that nothing
// depended on it and that nothing can be added afterwards. Reading "no dependents" and then
// deleting is the read-then-act shape this mechanism exists to remove: nothing excluded a
// dependency arriving in between.
//
// # What this function is, at this point in the conversion
//
// The gate, not yet the whole protocol. It refuses a destruction that the resource records
// forbid; it does not yet run the three-stage retirement, which is row 8's own work. Until an
// obligation is admitted through the authority there are no resource records for a client, and
// this reports "no dependency forbids it" — so it changes nothing for an unconverted path,
// which is exactly what lets it land before the production flip.

// mqttCleanupBlock names one resource whose dependents forbid its destruction.
type mqttCleanupBlock struct {
	// Inc is the resource incarnation, Name the durable it stands for, and Deps the
	// obligations that still need it. The obligations are named rather than counted,
	// because that is what makes the refusal checkable rather than asserted.
	Inc  string
	Name string
	Deps []string
}

func (b mqttCleanupBlock) Error() string {
	return fmt.Sprintf("resource %q (%s) still carries %d obligation(s): %v",
		b.Inc, b.Name, len(b.Deps), b.Deps)
}

// mqttEdgeNames renders a resource's standing needs for a refusal message.
//
// Rendered rather than counted, and each one carries its ROLE, because "still needed" is not
// actionable and "still needed for its canonical PUBREL" is.
func mqttEdgeNames(deps []mqttResourceEdge) []string {
	out := make([]string, 0, len(deps))
	for _, e := range deps {
		out = append(out, e.String())
	}
	return out
}

// mqttDestructionBlockedBy reports which of a generation's resources forbid destroying the
// named durables.
//
// Resources are matched by NAME, because that is what a teardown knows: it holds consumer
// names, not incarnations. A resource naming a durable that is not being destroyed is not
// consulted, and a durable with no resource record is not protected — nothing has been admitted
// against it.
//
// An UNREADABLE resource blocks. A record this broker cannot interpret may be the one naming an
// obligation that depends on this consumer, and treating it as absent is the inference the
// whole mechanism removes.
func mqttDestructionBlockedBy(resources []*mqttOutboundResource, unreadable int,
	durables []string) []mqttCleanupBlock {

	if len(durables) == 0 {
		return nil
	}
	want := make(map[string]struct{}, len(durables))
	for _, d := range durables {
		if d != _EMPTY_ {
			want[d] = struct{}{}
		}
	}
	var blocks []mqttCleanupBlock
	if unreadable > 0 {
		blocks = append(blocks, mqttCleanupBlock{
			Inc:  "<unreadable>",
			Name: "<unknown>",
			Deps: []string{fmt.Sprintf("%d resource record(s) this broker cannot read", unreadable)},
		})
	}
	for _, r := range resources {
		if r == nil {
			continue
		}
		if _, ok := want[r.Name]; !ok {
			continue
		}
		if len(r.Deps) == 0 {
			// No obligation names it. That is not a licence to delete it HERE either — the
			// authorizing CAS is what excludes a dependency arriving next — but it is not
			// this gate's refusal, and saying so is what keeps the two rules distinct.
			continue
		}
		blocks = append(blocks, mqttCleanupBlock{Inc: r.Inc, Name: r.Name, Deps: mqttEdgeNames(r.Deps)})
	}
	return blocks
}

// checkDestructionAuthorized reports whether any resource record FORBIDS destroying these
// durables. Its nil is a refusal that did not fire; it is NOT authorization to delete.
//
// The name says "authorized" because it is the authorization boundary's refusal half, and that
// is exactly why the distinction is spelled out here rather than only in the file header: a
// caller reading `if err := checkDestructionAuthorized(...); err == nil { delete() }` has built
// the read-then-act shape this whole mechanism exists to remove. Nothing excluded a dependency
// arriving between the read and the delete.
//
// Three of its nils carry no information at all, which is the clearest way to see it: a nil
// manager, an empty generation and an empty durable list all return nil, and none of them is a
// statement about any resource. Deletion is authorized only by winning the CAS on the resource
// record itself, LIVE -> DELETE_AUTHORIZED, which is what establishes both that the dependency
// list was empty and that no dependency can be added afterwards — and that CAS is WP-C's, not
// this function's.
//
// Reads the generation's resources itself rather than taking them from the caller, because the
// question is about the state at the moment of destruction and a caller's snapshot is exactly
// the read-then-act shape being removed.
func (as *mqttAccountSessionManager) checkDestructionAuthorized(id, epoch, gen string,
	durables []string) error {

	if as == nil || gen == _EMPTY_ || len(durables) == 0 {
		return nil
	}
	resources, unreadable, rerr := as.enumerateGenerationResources(id, epoch, gen)
	if rerr != nil {
		// AN UNRESOLVED ENUMERATION IS NOT AN EMPTY ONE. The teardown does not proceed on a
		// read it could not complete.
		return fmt.Errorf("the resources of generation %q could not be enumerated, so no "+
			"destruction is authorized: %w", gen, rerr)
	}
	if blocks := mqttDestructionBlockedBy(resources, unreadable, durables); len(blocks) > 0 {
		return fmt.Errorf("destroying the backing resources of generation %q is not authorized: %v",
			gen, blocks)
	}
	return nil
}

// enumerateGenerationResources reads one generation's backing resources.
//
// Mirrors enumerateGenerationObligations, including the part that matters most: a record that
// cannot be decoded is counted as UNREADABLE rather than skipped, because a resource this
// broker cannot interpret may be the one naming an obligation that depends on the consumer
// about to be destroyed.
func (as *mqttAccountSessionManager) enumerateGenerationResources(id, epoch, gen string) ([]*mqttOutboundResource, int, error) {
	idHash := getHash(id)
	var out []*mqttOutboundResource
	unreadable := 0

	// COMPLETENESS-AWARE, because this enumeration is what a destruction check asks about
	// DEPENDENCIES. The server caps a subject list at 100,000 and reports the true total in the
	// envelope, so a first page missing the one resource that still names a dependent would
	// return no block at all — an absence read as a fact, which is the whole shape this lane
	// exists to remove.
	subjects, whole, err := as.jsa.streamSubjectsComplete(mqttInflightStreamName,
		mqttResourceWalkFilter(idHash, gen))
	if err != nil {
		if isErrorOtherThan(err, JSStreamNotFoundErr) {
			return nil, 0, fmt.Errorf("enumerating the backing resources of %q/%q: %w", id, gen, err)
		}
		return nil, 0, nil
	}
	if !whole {
		return nil, 0, fmt.Errorf("the backing resources of %q/%q came back as a first page "+
			"rather than the whole answer, so a dependent this broker never saw would read as "+
			"absent", id, gen)
	}
	for subject := range subjects {
		toks := strings.Split(subject[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			unreadable++
			continue
		}
		smsg, lerr := as.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if lerr != nil {
			if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				return nil, 0, fmt.Errorf("loading the backing resource on %q: %w", subject, lerr)
			}
			// Gone between the enumeration and the load: a later pass re-enumerates, and
			// nothing is inferred from its absence here.
			continue
		}
		r, derr := mqttDecodeOutboundResource(smsg.Data, id, epoch, gen, toks[3])
		if derr != nil {
			unreadable++
			continue
		}
		out = append(out, r)
	}
	return out, unreadable, nil
}
