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

import "fmt"

// What a recovery pass owes an outstanding duty (F-105, row 6).
//
// The sweep establishes WHICH ordinals are outstanding. This establishes WHAT each one owes, and
// it is deliberately a classifier rather than an executor — the same split, and for the same
// reason, as the in-flight slot classifier: a function that read records AND acted on them could
// not be tested against a corpus without performing the actions, so the decision would only ever
// be exercised through its own effects.
//
// # State reconciliation is the default route, and a descriptor is the exception
//
// Every continuation here reads the AUTHORITATIVE RECORDS and derives what is owed. None of them
// retains a description of what a previous run intended. The design permits exactly one retained
// descriptor — an ownership proposal whose metadata verdict is unresolved, because there the next
// decision depends on whether ONE EXACT ATTEMPT committed and absence is not proof of
// non-commitment — and that one belongs to CONNECT rather than here.
//
// The earlier drafts of this design got that wrong twice, and both errors were the same shape:
// they reached for a journal when what was missing was a FENCE. Re-creating a manifest is not a
// fence. A fresh nonce is not a fence. Naming the wrong source of safety is how a mechanism ends
// up storing intentions.
//
// # A duty entry's existence is itself evidence, and it removes a case
//
// The duty entry and the thing it is about are written in the SAME atomic batch. So a sweep that
// can read the entry knows the admission landed — "an admission whose CAS result was unresolved"
// is a question for the CALLER that submitted it, fenced by the shared authority expectation,
// and never a question for a pass that is holding the entry in its hand. That is why nothing
// here tries to decide whether an admission happened.
//
// # And reading is never permission to act
//
// Every verdict below says what is OWED, not that this broker may do it. That is not a promise
// this file can keep on its own, so it is worth naming WHAT keeps it: each work kind routes
// through an act that re-derives its own authorization, and none of them consults a verdict.
//
//	deliver             -> action acquisition: serving authority, phase permits a PUBLISH, and a
//	                       conflicting-output drain; then writeAuthorizedFrame, which consumes the
//	                       acquisition under wmu and refuses a binding that is not the
//	                       connection's own
//	release             -> the same acquisition path, with mqttActionPermittedAtPhase admitting a
//	                       PUBREL only past a successful PUBREC
//	drain               -> nothing is emitted at all; the drain's own completion is the act
//	provision           -> opProvisionResource, which requires a SERVING authority and commits
//	                       create-only
//	authorize-deletion  -> the resource CAS LIVE -> DELETE_AUTHORIZED, which is what establishes
//	                       both that the dependency list was empty and that nothing can be added
//	                       afterwards, and which additionally requires provisioning SEALED
//	cleanup             -> the classified-outcome transition, which requires the standing
//	                       authorization it completes under
//
// So a future executor that treated mqttDutyOwesWork as permission would still be refused by the
// act it attempted. The verdict is advisory by CONSTRUCTION rather than by convention, and this
// list is the thing to check if a new work kind is ever added — a kind whose act has no boundary
// of its own would be the gap this comment exists to make visible.

// mqttDutyDisposition is what a pass may conclude about one outstanding duty.
type mqttDutyDisposition uint8

const (
	// mqttDutyMayResolve: the thing this duty is about has reached its end, so the duty may be
	// recorded terminal. It does NOT mean the pass has done so — the ending still has to be
	// written through the boundary, which re-derives it.
	mqttDutyMayResolve mqttDutyDisposition = iota + 1
	// mqttDutyOwesWork: the duty is actionable and names what remains.
	mqttDutyOwesWork
	// mqttDutyWithheld: nothing may be concluded yet. The duty stays outstanding and the sweep
	// comes back for it — which is the honest answer for conditional work that is not yet
	// possible, and for anything this broker could not read.
	mqttDutyWithheld
)

func (d mqttDutyDisposition) String() string {
	switch d {
	case mqttDutyMayResolve:
		return "may-resolve"
	case mqttDutyOwesWork:
		return "owes-work"
	case mqttDutyWithheld:
		return "withheld"
	}
	return "unknown"
}

// mqttDutyWork names the kind of work an outstanding duty owes.
type mqttDutyWork uint8

const (
	mqttDutyWorkNone mqttDutyWork = iota
	// mqttDutyWorkDeliver: the obligation is still at its publish and the client has not
	// answered. What is owed is delivery — under a NEWLY acquired action, never an old one.
	mqttDutyWorkDeliver
	// mqttDutyWorkRelease: the exchange is past PUBREC and owes its release. The canonical
	// PUBREL is the durable identity; a restored broker re-emits under a newly acquired action.
	mqttDutyWorkRelease
	// mqttDutyWorkDrain: the exchange has reached its protocol conclusion but still holds its
	// identifier pending a drain. Nothing is emitted; what is owed is the drain's completion.
	mqttDutyWorkDrain
	// mqttDutyWorkProvision: the resource's materialization is not settled. Owed: provision it,
	// or establish that it never will be, and seal.
	mqttDutyWorkProvision
	// mqttDutyWorkAuthorizeDeletion: the resource is LIVE with nothing depending on it and its
	// provisioning sealed. Owed: the authorizing CAS — which is what establishes both that the
	// dependency list was empty and that nothing can be added afterwards.
	mqttDutyWorkAuthorizeDeletion
	// mqttDutyWorkCleanup: deletion is authorized and the classified outcome is not recorded.
	// Owed: act on the EXACT incarnation and classify the result. An exact-sequence delete
	// returning success is not a completed cleanup.
	mqttDutyWorkCleanup
	// The four kinds of a DISCARDED generation's retirement (F-105 7d), each licensed by the
	// generation's committed discarding closure rather than by an entitlement:
	//
	// mqttDutyWorkDiscardEnd: an obligation that has not released its holds is ended
	// (discard-exchange), its holds released in the same batch.
	mqttDutyWorkDiscardEnd
	// mqttDutyWorkDiscardCleanup: an obligation that released its holds still has its canonical
	// PUBREL (`rel`) or its own register standing; owed: delete them, the rel first.
	mqttDutyWorkDiscardCleanup
	// mqttDutyWorkDispose: a source or PUBREL consumer nothing decided about; owed: the
	// generation-discarded disposal decision.
	mqttDutyWorkDispose
)

func (w mqttDutyWork) String() string {
	switch w {
	case mqttDutyWorkDeliver:
		return "deliver"
	case mqttDutyWorkRelease:
		return "release"
	case mqttDutyWorkDrain:
		return "drain"
	case mqttDutyWorkProvision:
		return "provision"
	case mqttDutyWorkAuthorizeDeletion:
		return "authorize-deletion"
	case mqttDutyWorkCleanup:
		return "cleanup"
	case mqttDutyWorkDiscardEnd:
		return "discard-end"
	case mqttDutyWorkDiscardCleanup:
		return "discard-cleanup"
	case mqttDutyWorkDispose:
		return "dispose"
	}
	return "none"
}

// mqttDutyVerdict is one outstanding duty's classification.
type mqttDutyVerdict struct {
	Disposition mqttDutyDisposition
	Work        mqttDutyWork
	// Why is the fact the verdict turned on, for the operator who has to act on it and for the
	// test that has to distinguish this verdict from a neighbouring one.
	Why string
}

// classifyOutstandingDuty reads the authoritative records for one outstanding duty and reports
// what it owes.
//
// It performs NO writes and acquires nothing. Every read failure is WITHHELD rather than
// resolved: a duty this broker cannot read is one it cannot conclude anything about, and the
// sweep's own rule — a missing or malformed entry is an inconsistency, never completed work —
// applies just as much to the records a duty points at as to the entry itself.
func (jsa *mqttJSA) classifyOutstandingDuty(id, epoch, gen string, d *mqttDutyEntry) mqttDutyVerdict {
	return jsa.classifyOutstandingDutyIn(id, epoch, gen, false, d)
}

// classifyOutstandingDutyIn is the classification for a generation whose closure the caller read.
//
// discarded says that committed closure DISCARDS, read once per visit (F-105 7d): a discarded
// generation owes its retirement rather than its protocol, and a live or preserved one classifies
// exactly as classifyOutstandingDuty does.
func (jsa *mqttJSA) classifyOutstandingDutyIn(id, epoch, gen string, discarded bool, d *mqttDutyEntry) mqttDutyVerdict {
	if d == nil {
		return mqttDutyVerdict{
			Disposition: mqttDutyWithheld,
			Why:         "the entry could not be read, and an unreadable duty concludes nothing",
		}
	}
	switch d.Kind {
	case mqttDutyExchange:
		if discarded {
			return jsa.classifyDiscardedExchangeDuty(id, epoch, gen, d)
		}
		return jsa.classifyExchangeDuty(id, epoch, gen, d)
	case mqttDutyProvision, mqttDutyCleanup:
		return jsa.classifyResourceDuty(id, epoch, gen, discarded, d)
	}
	return mqttDutyVerdict{
		Disposition: mqttDutyWithheld,
		Why:         fmt.Sprintf("this broker does not interpret duties of kind %q", d.Kind),
	}
}

// classifyExchangeDuty covers the obligation continuations: an unknown emission outcome, and an
// exchange past PUBREC with an owed release.
func (jsa *mqttJSA) classifyExchangeDuty(id, epoch, gen string, d *mqttDutyEntry) mqttDutyVerdict {
	x, _, err := jsa.loadOutboundExchange(id, epoch, gen, d.Ref)
	if err != nil || x == nil {
		return mqttDutyVerdict{
			Disposition: mqttDutyWithheld,
			Why: fmt.Sprintf("obligation %q could not be read (%v), and an obligation this "+
				"broker cannot read is one it may neither resume nor call finished", d.Ref, err),
		}
	}
	switch x.Phase {
	case mqttExchangePublish:
		// THE CLIENT HAS NOT ANSWERED. What is owed is delivery, and it is owed under a NEWLY
		// acquired action: an action from a previous run names a connection this broker cannot
		// reach, so its emission outcome is UNKNOWN and Outstanding is answered TRUE for it.
		// Nothing here is ever applied as "sent".
		return mqttDutyVerdict{
			Disposition: mqttDutyOwesWork, Work: mqttDutyWorkDeliver,
			Why: "the obligation stands at its publish, so the delivery is still owed",
		}
	case mqttExchangePubRecObserved:
		// PAST PUBREC. The canonical PUBREL record is the durable identity of the release; a
		// restored broker re-emits under a newly acquired action and never the old one.
		// Readability and a standing dependency are NOT alone permission to emit — acquisition
		// still awaits conflicting output drain and still requires serving authority.
		return mqttDutyVerdict{
			Disposition: mqttDutyOwesWork, Work: mqttDutyWorkRelease,
			Why: "the client answered PUBREC, so the release is owed and the payload is not",
		}
	case mqttExchangeTerminalPendingDrain, mqttExchangeRejectedPendingDrain:
		// STILL HOLDS ITS IDENTIFIER. The protocol is concluded and the duty is not: treating a
		// pending drain as an ending is how an identifier is reused while output can still
		// execute under it.
		return mqttDutyVerdict{
			Disposition: mqttDutyOwesWork, Work: mqttDutyWorkDrain,
			Why: "the obligation is concluded but still holds its identifier pending a drain",
		}
	}
	if mqttExchangeDeliveryDutyEnded(x.Phase) {
		// A durable ending, or a delivery the broker abandoned: either way no delivery is owed.
		// The DUTY question only — the identifier stays closed for an abandoned delivery
		// (mqttExchangePhaseEnded, which identifier exclusivity reads, excludes it).
		return mqttDutyVerdict{
			Disposition: mqttDutyMayResolve,
			Why:         "the obligation owes no further delivery",
		}
	}
	return mqttDutyVerdict{
		Disposition: mqttDutyWithheld,
		Why: fmt.Sprintf("obligation %q stands at phase %d, which this broker does not interpret",
			d.Ref, int(x.Phase)),
	}
}

// classifyResourceDuty covers the provisioning and cleanup continuations.
func (jsa *mqttJSA) classifyResourceDuty(id, epoch, gen string, discarded bool, d *mqttDutyEntry) mqttDutyVerdict {
	r, _, err := jsa.loadOutboundResource(id, epoch, gen, d.Ref)
	if err != nil || r == nil {
		return mqttDutyVerdict{
			Disposition: mqttDutyWithheld,
			Why: fmt.Sprintf("resource %q could not be read (%v), and a resource this broker "+
				"cannot read is never treated as absent", d.Ref, err),
		}
	}

	if d.Kind == mqttDutyProvision {
		if mqttResourceLogicalUseKind(r.Kind) {
			// A LOGICAL-USE kind has no durable materialization to seal (consult 4): its create
			// is unfenceable, so there is no "nothing further can materialize it" fact for
			// recovery to establish, and inventing one from a consumer lookup would be exactly
			// the seal-on-existence the plan forbids. The resource standing IS the claim (the
			// name-ownership went LIVE in the provisioning batch); whether the physical consumer
			// is usable is a LIVE admission gate, not a recovery duty. So the provision duty is
			// resolvable once the resource exists, and nothing here reads the consumer.
			return mqttDutyVerdict{
				Disposition: mqttDutyMayResolve,
				Why: "a source/PUBREL consumer is arbitrated by name ownership, not by a " +
					"materialization seal; the claim rode its provisioning batch and readiness " +
					"for use is a live admission gate",
			}
		}
		if r.Prov == mqttProvisioningSealed {
			return mqttDutyVerdict{
				Disposition: mqttDutyMayResolve,
				Why:         "the resource's provisioning is sealed, so nothing further can materialize it",
			}
		}
		return mqttDutyVerdict{
			Disposition: mqttDutyOwesWork, Work: mqttDutyWorkProvision,
			Why: "the resource's materialization is not settled, so its name is not free",
		}
	}

	// CLEANUP. Its states are walked from the end backwards, because the later ones are the
	// ones that conclude something and the earlier ones are all "not yet".
	switch r.State {
	case mqttResourceCleanupComplete:
		return mqttDutyVerdict{
			Disposition: mqttDutyMayResolve,
			Why:         "the cleanup recorded its classified outcome",
		}
	case mqttResourceDeleteAuthorized:
		return mqttDutyVerdict{
			Disposition: mqttDutyOwesWork, Work: mqttDutyWorkCleanup,
			Why: "deletion is authorized and the classified outcome is not recorded: a delete " +
				"that returned success is not a completed cleanup",
		}
	}

	// LIVE. What is owed depends on whether the resource is authorizable at all, and the two
	// answers are kept apart because only one of them is work.
	if len(r.Deps) > 0 {
		// In a DISCARDED generation too (F-105 7d): its holders are found and ended through their
		// own exchange duties — every admitted obligation has one in its generation, which the
		// boundary's coverage rule enforces, and the census reaches it even once resolved — so the
		// resource waits for them rather than being a second route to them.
		return mqttDutyVerdict{
			Disposition: mqttDutyWithheld,
			Why: fmt.Sprintf("%d obligation(s) still need the resource, so its cleanup is not "+
				"yet actionable", len(r.Deps)),
		}
	}
	if !mqttResourceLogicalUseKind(r.Kind) && r.Prov != mqttProvisioningSealed {
		// NOTHING DEPENDS ON IT AND ITS NAME IS STILL NOT FREE. Withheld rather than
		// authorizable: a delayed create may still land, and authorizing here is how the orphan
		// is made. This holds for the kinds the broker can SEAL; a logical-use kind takes the
		// arm below, because its delayed create is fenced by name ownership rather than by a
		// seal — authorizing deletion RETIRES the name (opAuthorizeDeletion), after which a late
		// create is a harmless orphan no obligation can adopt.
		return mqttDutyVerdict{
			Disposition: mqttDutyWithheld,
			Why: "nothing depends on the resource but its provisioning is unsealed, so a " +
				"delayed create may still land under its name",
		}
	}
	if mqttResourceLogicalUseKind(r.Kind) && r.Disposal == nil && discarded {
		// ENDED WITH ITS GENERATION (F-105 7d). The closure that discarded the generation is the
		// decision no route of it can take any more; recording it is owed.
		return mqttDutyVerdict{
			Disposition: mqttDutyOwesWork, Work: mqttDutyWorkDispose,
			Why: "the source/PUBREL consumer's generation was discarded and nothing recorded its end",
		}
	}
	if mqttResourceLogicalUseKind(r.Kind) && r.Disposal == nil {
		// SERVING, NOT ABANDONED (F-105 7c-lc). Zero dependents is also what an idle subscription
		// looks like between two messages; nothing here may end a consumer no route ended. The
		// disposal decision is the route's, and until it stands the cleanup is not actionable.
		return mqttDutyVerdict{
			Disposition: mqttDutyWithheld,
			Why: "nothing depends on the source/PUBREL consumer, but no route has disposed of it: " +
				"it is serving, and an idle consumer is not an ended one",
		}
	}
	why := "nothing depends on the resource and its provisioning is sealed, so the " +
		"authorizing CAS is owed"
	if mqttResourceLogicalUseKind(r.Kind) {
		why = "a route disposed of the source/PUBREL consumer and nothing depends on it, so " +
			"authorizing deletion — which RETIRES its physical name and makes any late create a " +
			"harmless orphan — is owed"
	}
	return mqttDutyVerdict{
		Disposition: mqttDutyOwesWork, Work: mqttDutyWorkAuthorizeDeletion, Why: why,
	}
}
