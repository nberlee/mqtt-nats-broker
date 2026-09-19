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

import "fmt"

// Who owns a retiring generation's cleanup: the teardown, or recovery (F-105, row 8).
//
// # Exactly one mechanism owns each durable, and a DURABLE FACT selects which
//
// The teardown classifies the generation BEFORE it deletes anything, and carries that
// classification through to the release. It is never recomputed afterwards: the release deletes
// the session record, so a second look would be asking about records that have since gone.
//
//	VACUOUS       closure and ledger BOTH positively absent. The generation admitted nothing
//	              through the authority, so there is no duty a release could strand and nothing
//	              for these facts to be about. Today's behaviour exactly, byte for byte.
//	ENROLLED      either present. Its cleanup is transferred only where the generation's own
//	              closure says DISCARD and its affine target is enrolled with both a resource
//	              record and a cleanup duty; the shared group durables stay with the synchronous
//	              helper for every population.
//	INDETERMINATE anything unreadable, inconsistent or incomplete. REFUSED — never classified as
//	              vacuous, because "I could not read it" is not "there is nothing there", and the
//	              vacuous arm is the one that licenses deleting everything.
//
// # Why the disposition is the selector and not the presence of a record
//
// Selecting on "is there a resource record with a cleanup duty" is contradictory: removing either
// record would then silently make a transferred target synchronous, while the release gate must
// REFUSE exactly that state. The closure's `Discard` member is immutable, written by the act that
// had the protocol event in front of it, and it partitions the population without overlap: a
// discarding closure REQUIRES the enrolment and refuses without it; a non-discarding one enrols
// no affine target at all and refuses nothing.

// mqttTeardownClass is who owns this generation's transferable cleanup.
type mqttTeardownClass uint8

const (
	// mqttTeardownOwnerUnknown is the zero value and a REFUSAL, so a path that forgets to
	// classify stalls rather than deleting.
	mqttTeardownOwnerUnknown mqttTeardownClass = iota
	// mqttTeardownOwnerVacuous: nothing was admitted through the authority.
	mqttTeardownOwnerVacuous
	// mqttTeardownOwnerPreserving: an inventory exists and its closure does not discard, so
	// the affine consumer was never transferred.
	mqttTeardownOwnerPreserving
	// mqttTeardownOwnerDiscarding: an inventory exists, its closure discards, and the affine
	// target is enrolled. Its physical deletion belongs to the recovery pass.
	mqttTeardownOwnerDiscarding
)

func (c mqttTeardownClass) String() string {
	switch c {
	case mqttTeardownOwnerVacuous:
		return "vacuous"
	case mqttTeardownOwnerPreserving:
		return "enrolled, preserving"
	case mqttTeardownOwnerDiscarding:
		return "enrolled, discarding"
	}
	return "indeterminate"
}

// mqttTeardownOwnership is the classification and what it found.
type mqttTeardownOwnership struct {
	Class mqttTeardownClass
	// Affine is the enrolled affine resource, present only for the discarding class.
	Affine *mqttOutboundResource
}

// classifyTeardownGeneration decides who owns this generation's transferable cleanup.
//
// It runs BEFORE the first physical deletion. The existing release gate asks its question after
// `deleteTeardownTargets` has already run, which is the right place for a release decision and the
// wrong one for a deletion decision: by then the affine consumer would be gone whatever the
// answer.
func (as *mqttAccountSessionManager) classifyTeardownGeneration(id, epoch, gen string,
	t *mqttTeardownTarget) (mqttTeardownOwnership, error) {

	var out mqttTeardownOwnership

	closed, _, status := as.jsa.loadGenClosure(id, epoch, gen)
	switch status {
	case mqttOutboundValid, mqttOutboundAbsent:
	default:
		// UNREADABLE IS NOT ABSENT. Proceeding would take the vacuous path and delete exactly
		// what the classification exists to protect.
		return out, fmt.Errorf("the closure record of generation %q could not be resolved "+
			"(status %v), so this broker cannot say who owns its cleanup", gen, status)
	}
	led, _, lerr := as.jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil {
		return out, fmt.Errorf("the inventory ledger of generation %q could not be read: %w", gen, lerr)
	}

	if closed == nil && led == nil {
		out.Class = mqttTeardownOwnerVacuous
		return out, nil
	}
	if closed == nil {
		// AN INVENTORY WITH NO CLOSURE. Not vacuous — there is an inventory — and not enrolled
		// either, because nothing says the generation ended or what its disposition was. The
		// release gate refuses this too, and so does this.
		return out, fmt.Errorf("generation %q of client %q has an inventory and no closure "+
			"record, so nothing says whether its obligations were abandoned or preserved", gen, id)
	}
	if !closed.Discard {
		out.Class = mqttTeardownOwnerPreserving
		return out, nil
	}

	// A DISCARDING CLOSURE REQUIRES ITS ENROLMENT. This is the "required enrolment missing" case,
	// and it refuses HERE, at classification, before any downstream work — a later refusal at the
	// release gate would leave the deletions already done.
	if t == nil || t.q2AffDur == _EMPTY_ {
		// Nothing affine to transfer. The discarding generation may legitimately have held no
		// affine consumer at all.
		out.Class = mqttTeardownOwnerDiscarding
		return out, nil
	}
	res, unreadable, rerr := as.enumerateGenerationResources(id, epoch, gen)
	if rerr != nil {
		return out, fmt.Errorf("the backing resources of generation %q could not be enumerated, "+
			"so this broker cannot say whether its affine consumer is enrolled: %w", gen, rerr)
	}
	if unreadable > 0 {
		return out, fmt.Errorf("%d backing resource(s) of generation %q could not be interpreted, "+
			"and one of them may be the affine enrolment this teardown is asking about",
			unreadable, gen)
	}
	for i := range res {
		r := res[i]
		if r.Kind != mqttResourceAffineConsumer || r.Name != t.q2AffDur {
			continue
		}
		// AND ITS DUTY, because a resource with no duty entry is a durable nothing will come
		// back for: the entry is what a sweep reaches.
		has, derr := as.jsa.cleanupDutyStandsFor(id, epoch, gen, r.Inc)
		if derr != nil {
			return out, fmt.Errorf("the cleanup duty of affine consumer %q in generation %q "+
				"could not be read: %w", r.Inc, gen, derr)
		}
		if !has {
			return out, fmt.Errorf("affine consumer %q of generation %q is enrolled with no "+
				"cleanup duty, so nothing will come back for it", r.Inc, gen)
		}
		out.Class, out.Affine = mqttTeardownOwnerDiscarding, r
		return out, nil
	}
	return out, fmt.Errorf("generation %q of client %q discards its obligations and holds affine "+
		"consumer %q, which no resource record names: its deletion would be transferred to a duty "+
		"that does not exist", gen, id, t.q2AffDur)
}

// finishAffinePrerequisites writes the readiness assertion and seals the enrolled affine resource.
//
// Called by the teardown AFTER `deleteTeardownTargets` has secured the PUBREC proofs and
// established the assignments' discard disposition, and BEFORE the metadata release. The pass can
// do this too, and does when no teardown ever runs; both write the same create-only record, so
// whichever arrives second reads it as done.
//
// A failure here is NOT fatal to the teardown. The assertion and the seal are what make the
// cleanup ACTIONABLE, and the pass finishes them from the same durable evidence if this does not:
// what the teardown must not do is delete the consumer, and it has already not done that.
func (as *mqttAccountSessionManager) finishAffinePrerequisites(s *Server, id, epoch, gen string,
	own mqttTeardownOwnership, t *mqttTeardownTarget) {

	if own.Class != mqttTeardownOwnerDiscarding || own.Affine == nil {
		return
	}
	if rerr := as.jsa.publishAffineReadiness(&mqttAffineReadiness{
		ID: id, Epoch: epoch, Gen: gen,
		AffGen: own.Affine.AffGen, Durable: own.Affine.Name, PubRelDur: own.Affine.PubRelDur,
	}); rerr != nil {
		s.Debugf("MQTT teardown: asserting the affine readiness of %q/%q: %v", id, gen, rerr)
		return
	}
	r, rseq, lerr := as.jsa.loadOutboundResource(id, epoch, gen, own.Affine.Inc)
	if lerr != nil || r == nil || r.Prov == mqttProvisioningSealed {
		return
	}
	txn := mqttNewInflightNonce()
	root := &mqttRecoveryRoot{ID: id, Epoch: epoch, Gen: gen}
	if serr := as.jsa.opSealAffineProvisioning(root, r, rseq, txn); serr != nil {
		s.Debugf("MQTT teardown: sealing the provisioning of %q/%q/%q: %v", id, gen, r.Inc, serr)
	}
}
