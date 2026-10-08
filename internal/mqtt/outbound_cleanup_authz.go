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
// removed the ordinary source consumers and then the PUBREL consumer, and because $MQTT_out is
// interest-retained, deleting that consumer dropped the records it carried — which are the only
// evidence the client answered PUBREC. The register may survive and prove nothing. A teardown
// destroyed the completion route of an obligation admitted before it, and never knew.
//
// # Why a count was not enough
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
// Winning the authorizing CAS on the resource record (opAuthorizeDeletion, LIVE ->
// DELETE_AUTHORIZED, which retires the consumer name's ownership in the same batch) is what
// establishes BOTH that nothing depended on it and that nothing can be added afterwards.
// Reading "no dependents" and then deleting is the read-then-act shape this mechanism exists to
// remove: nothing excluded a dependency arriving in between. So no read-side check gates a
// destruction here. A discarding teardown TRANSFERS its logical-use consumers instead of
// deleting them (classifyTeardownGeneration sets transferLogical, which deleteTeardownTargets
// honours), and the recovery executor retires each through that CAS.
//
// What this file keeps is the enumeration the teardown classification and the recovery executor
// share.

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
	// DEPENDENCIES. The server answers a subject list in pages of at most 100,000, so a walk
	// missing the page that holds the one resource still naming a dependent would return no block
	// at all — an absence read as a fact, which is the whole shape this lane exists to remove.
	subjects, whole, err := as.jsa.streamSubjectsComplete(mqttInflightStreamName,
		mqttResourceWalkFilter(idHash, gen))
	if err != nil {
		if isErrorOtherThan(err, JSStreamNotFoundErr) {
			return nil, 0, fmt.Errorf("enumerating the backing resources of %q/%q: %w", id, gen, err)
		}
		return nil, 0, nil
	}
	if !whole {
		return nil, 0, fmt.Errorf("the backing resources of %q/%q could not be enumerated "+
			"completely, so a dependent this broker never saw would read as absent", id, gen)
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
