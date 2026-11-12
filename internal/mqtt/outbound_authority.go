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
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// The outbound authority (issue #107).
//
// Retirement must not destroy the durable representation, or the completion route, of an
// obligation that was admitted before it. That is the whole invariant, and the reason a
// name cannot carry it: a resume PRESERVES the generation, so a peer may legitimately be
// serving the same subjects, and every earlier attempt to decide entitlement by observing
// durable state went stale between the observation and the act.
//
// So entitlement is arbitrated rather than observed. One per-client AUTHORITY record names
// the tenure currently admitted to create outbound obligations, and every transition that
// creates an obligation, acquires an action, transfers ownership or closes admission
// contends on it — committing in the SAME atomic batch as the exchange mutation it
// authorizes. JetStream's atomic batches are per stream, so the authority shares a stream
// with the exchanges it governs; the session record shares it too ($MQTT_sess), but no
// transition here commits with it.
//
// Three facts are kept apart, because collapsing any two of them is what every prior
// attempt did:
//
//   - ADMISSION is the authority's positive state. A missing authority record does not
//     establish an open generation, and a missing anything never establishes a fact here.
//   - CLOSURE is a per-generation record, AUTHORITATIVE for its generation until that
//     generation retires (issue #133). "Retired" means no new
//     admission; it does NOT mean every old operation finished or every backing resource
//     is disposable.
//   - An OUTCOME is the permanent, immutable verdict of one attempted transition. It
//     exists so that "this transition never landed" is a POSITIVE fact: an undecided
//     attempt is settled by create-only publishing an ABORTED outcome to the very subject
//     the commit batch would have occupied, which races that batch atomically. A caller
//     that merely failed to find a receipt has learned nothing.
//
// Nothing here deletes an outcome or a closure during the account's recovery epoch. Their
// storage cost is explicit and accepted: this design offers no proof that every delayed
// caller has stopped referring to them, and inventing an expiry would rebuild the
// inference from absence that the whole item exists to remove.

// mqttOutboundRecordClassTk is the fixed token that opens every subject class this file owns.
//
// It is what keeps these records out of the register machinery's reach, and the guard is
// TOKEN VALIDATION rather than token count. The distinction matters and an earlier comment
// here got it wrong: an authority subject has exactly three tokens past the prefix, the
// same as a register, so what excludes it is only that this token satisfies neither
// mqttValidIDHashToken (sysHashLen, 8) nor mqttValidSessGen (mqttSessGenLen, 22). Closure
// and outcome subjects carry four tokens and are excluded twice over. The tests assert
// both properties rather than leaving them implied, because a class that silently began
// parsing as a register would be destroyed by a reap that believed it had named what it
// deleted.
const mqttOutboundRecordClassTk = "ob"

// The per-class tokens. Each names the fact its record carries, not the mechanism that
// writes it.
const (
	mqttAuthorityClassTk = "auth"   // the tenure admitted to create outbound obligations
	mqttClosureClassTk   = "closed" // a generation permanently closed to new admission
	mqttOutcomeClassTk   = "txn"    // one attempted transition's permanent verdict
)

// mqttOutboundRecordSubjectPrefix is the common prefix of every class above.
func mqttOutboundRecordSubjectPrefix() string {
	return mqttInflightStreamSubjectPrefix + mqttOutboundRecordClassTk + "."
}

// mqttIsOutboundRecordSubject reports whether a subject belongs to any issue #107 record class.
func mqttIsOutboundRecordSubject(subject string) bool {
	return strings.HasPrefix(subject, mqttOutboundRecordSubjectPrefix())
}

// mqttIsOutboundArbitrationSubject reports whether a subject is one of the records the
// TRANSITION WRITER ITSELF owns: the authority, a generation closure, a transition outcome.
//
// The distinction from the obligation classes matters and is not cosmetic. These three are
// how a transition is arbitrated and recorded, so a caller-supplied member naming one would
// let a transition rewrite the very evidence that says what it did — and atomicity would
// commit it faithfully, because it validates ordering and never meaning. The obligation
// classes an admission installs (an exchange, an action) are the opposite: carrying them is
// the whole point of the batch.
func mqttIsOutboundArbitrationSubject(subject string) bool {
	if !mqttIsOutboundRecordSubject(subject) {
		return false
	}
	rest := subject[len(mqttOutboundRecordSubjectPrefix()):]
	for _, cls := range []string{mqttAuthorityClassTk, mqttClosureClassTk, mqttOutcomeClassTk} {
		if strings.HasPrefix(rest, cls+".") {
			return true
		}
	}
	return false
}

// mqttOutboundObligationClass returns the obligation class a subject names, and whether it is
// one at all. A subject under the issue #107 prefix that is neither an arbitration record nor a
// known obligation class is refused rather than guessed at.
//
// The GENERATION LEDGER and the DUTY INVENTORY are obligation classes, not arbitration records,
// and the distinction is load-bearing. An arbitration record is one the transition writer itself
// owns, so a caller-supplied member naming one could rewrite the evidence of what a transition
// did. These two are the opposite: an admission CARRIES its ledger CAS and its duty entries, in
// the same batch, because allocating an ordinal atomically with the obligation is the whole
// mechanism. Carrying them is the point.
func mqttOutboundObligationClass(subject string) (string, bool) {
	if !mqttIsOutboundRecordSubject(subject) {
		return _EMPTY_, false
	}
	rest := subject[len(mqttOutboundRecordSubjectPrefix()):]
	for _, cls := range []string{mqttExchangeClassTk, mqttActionClassTk, mqttResourceClassTk,
		mqttLedgerClassTk, mqttDutyClassTk, mqttManifestClassTk, mqttChunkClassTk, mqttRelClassTk} {
		if strings.HasPrefix(rest, cls+".") {
			return cls, true
		}
	}
	return _EMPTY_, false
}

// mqttOutboundAuthoritySubject returns the per-client authority subject.
//
// Per CLIENT rather than per generation, deliberately: the record is what arbitrates the
// move from one generation to the next, so a name scoped by generation could not express
// the transition that retires one and admits its successor.
func mqttOutboundAuthoritySubject(idHash string) string {
	return mqttOutboundRecordSubjectPrefix() + mqttAuthorityClassTk + "." + idHash
}

// mqttGenClosureSubject returns the permanent closure subject for one generation.
//
// Permanent, and never deleted while the recovery epoch stands: closure is the fact every
// reader resolves entitlement against, and a closure that could be reaped would let a
// later reader conclude an open generation from its absence.
func mqttGenClosureSubject(idHash, gen string) string {
	return mqttOutboundRecordSubjectPrefix() + mqttClosureClassTk + "." + idHash + "." + gen
}

// mqttTransitionOutcomeSubject returns the permanent outcome subject of one attempted
// transition.
//
// One subject per attempt, and both verdicts compete for it create-only. That mutual
// exclusion IS the protocol: a committed batch carries its COMMITTED member, and a caller
// that cannot decide publishes ABORTED to the same subject, so exactly one of the two can
// ever exist and the loser's whole batch is refused. Two slots, or one overwritable slot,
// would let an abort stand beside a later commit and decide nothing.
//
// SCOPED, ob.txn.<idHash>.<scope>.<txn>: a generation-scoped outcome is one of that
// generation's records and is reclaimed with them by its purge filter; a lifecycle-scoped one
// (L.<L>) outlives every generation of its lifecycle (mqttKindOutcomeScope).
func mqttTransitionOutcomeSubject(idHash, scope, txn string) string {
	return mqttOutboundRecordSubjectPrefix() + mqttOutcomeClassTk + "." + idHash + "." + scope + "." + txn
}

// mqttOutboundIdentTk reports whether s is a nonempty single subject token. Used for the
// identity tokens this file mints rather than derives — a transition id, a tenure id —
// which must not contain a separator or a wildcard, since every name here is built by
// concatenation and a stored value is never a trusted input.
func mqttOutboundIdentTk(s string) bool {
	return s != _EMPTY_ && !strings.ContainsAny(s, ". \t*>")
}

// ---------------------------------------------------------------------------
// The attempt description
// ---------------------------------------------------------------------------

// mqttTransitionKind is the closed set of operations a transition may perform.
//
// Closed rather than a free string, because the kind is part of what an ambiguity
// resolution verifies: a caller resolving an attempt states the operation it expected, and
// a receipt describing a different one is a contradiction rather than a match.
type mqttTransitionKind string

const (
	// There is deliberately NO kind that installs a SERVING authority outright: every route to
	// SERVING is an acceptance shape, which commits the ownership and the session record as one
	// batch (accept_shapes.go, issue #133).
	mqttTransitionAdmitExchange mqttTransitionKind = "admit-exchange"
	mqttTransitionAcquireAction mqttTransitionKind = "acquire-action"
	// mqttTransitionDisposeResource authorizes or completes the cleanup of a backing
	// resource, and is the ONE obligation transition that does not require serving
	// authority.
	//
	// That is not a relaxation, it is the point. Cleanup of an old generation's resources
	// is needed precisely AFTER that generation is closed — closure is what makes the
	// resources reclaimable — so a kind that demanded SERVING would make the work
	// impossible at the only time it matters, and would push a caller towards reopening or
	// borrowing an authority to do it. It moves no ownership, admits no obligation and
	// writes no closure; all it carries is the resource record's own transition.
	mqttTransitionDisposeResource mqttTransitionKind = "dispose-resource"
	// mqttTransitionProvisionResource records a backing resource this generation owns, so
	// that an admission has something to take a dependency on.
	//
	// It is separate from dispose-resource because it is NEW WORK and is governed
	// accordingly: a resource may only be provisioned by a SERVING authority, where cleanup
	// must remain possible after closure. It carries exactly one resource member, always
	// create-only — a provisioning that could overwrite would be a way to reset a resource's
	// dependents to nothing and authorize deleting a consumer obligations still need.
	mqttTransitionProvisionResource mqttTransitionKind = "provision-resource"
	// mqttTransitionMaterializePayload writes retained replay material: a manifest CAS,
	// the chunks that land through it, and the witness of the verification that seals it.
	//
	// It exists because a materialization does not fit either resource kind. Provisioning
	// creates the payload's resource and its manifest in one create-only batch, but material
	// larger than one message needs SEVERAL batches after that, and each carries no new
	// resource; disposal carries the same records but is permitted under a closed authority,
	// which is right for cleanup and wrong for writing material. So this is governed like
	// provisioning — NEW WORK under a SERVING authority — and carries nothing but the
	// materialization's own records.
	//
	// It admits no obligation, moves no ownership and writes no closure. The fence itself is
	// not stated here: it is a rule about manifest and chunk MEMBERS, so it holds for every
	// kind that carries them.
	mqttTransitionMaterializePayload mqttTransitionKind = "materialize-payload"
	// mqttTransitionAdvanceExchange moves ONE exchange along its protocol, durably.
	//
	// It is the other transition that does not require serving authority, and for the same
	// reason dispose-resource does not: an obligation of a CLOSED generation must still be
	// able to END. Terminal evidence for a G1 exchange arrives on G1's connection while G2
	// serves, and a kind that demanded SERVING would leave that generation permanently
	// unfinished — its charge held, its identifier unreclaimable — which is the state the
	// retirement protocol must be able to reach the end of.
	//
	// It admits nothing and moves no ownership. The only thing it may change is the PHASE
	// of an exchange that already exists, and only along the protocol's own successor
	// relation, so it can neither resurrect a terminal obligation nor skip the drain.
	mqttTransitionAdvanceExchange mqttTransitionKind = "advance-exchange"
	// mqttTransitionReleaseHolds releases the hold edges of obligations that already ENDED, and
	// writes terminal the delivery duties that release earns (outbound_release_holds.go). It is
	// AUTHORITY-FREE, always: it ends nothing, admits nothing and moves no ownership; each
	// resource member is a CAS removing exact edges whose exchange the store shows ended, and
	// each duty member is judged by checkDutyEndingIsEarned on the state the batch leaves. Like
	// dispose-resource it may name any generation of the client: finished history needs no
	// serving authority to be released.
	mqttTransitionReleaseHolds mqttTransitionKind = "release-holds"
	// mqttTransitionWithdrawUnsent ends ONE admitted PUBLISH obligation that was never
	// transmitted: its register becomes a cancellation tombstone and its exchange moves
	// Publish -> WithdrawnUnsent, in one batch.
	//
	// Like advance-exchange it ends work already authorised, so it needs no serving authority
	// and may name an earlier generation's exchange; like it, it moves no ownership and admits
	// nothing. It is a kind of its own rather than a variant of the advance because its evidence
	// is not a client answer but the unsent marker, which the write boundary loads and checks
	// itself, and because the advance's slot members are acknowledgement arrows this one never
	// carries.
	mqttTransitionWithdrawUnsent mqttTransitionKind = "withdraw-unsent"
	// mqttTransitionAbandonDelivery records that the broker abandoned delivery of one restored
	// ordinary obligation on a positive authorization deny: its register moves to
	// delivery_abandoned and its exchange Publish -> DeliveryAbandoned, in one batch (issue #107 row
	// 5, consult 34). Unlike an ending it is a DECISION made under a connection's entitlement,
	// so it requires the SERVING authority at exactly that generation and tenure; like an
	// ending it moves no ownership and admits nothing.
	mqttTransitionAbandonDelivery mqttTransitionKind = "abandon-delivery"
	// mqttTransitionTombstoneRegister records that the serving connection CANCELLED one ordinary
	// allocation without whole-exchange evidence (issue #107 7b): the register becomes the cancellation
	// tombstone — an overwrite of its untokened publish occupant with Cancelled set and nothing
	// else, or, for an allocation whose own store is still in flight, a create-only tombstone —
	// and the exchange, if any, is left where it is. Like abandon-delivery it is a DECISION under
	// one connection's entitlement: it requires the SERVING authority at exactly that generation
	// and tenure, so a displaced connection's cancellation is refused, and it moves no ownership.
	mqttTransitionTombstoneRegister mqttTransitionKind = "tombstone-register"
	// mqttTransitionRebindCanonical re-binds an ordinary `pubrel_ready` slot whose canonical
	// PUBREL record vanished for a reason other than PUBCOMP to a freshly published, proven
	// record (issue #107 7b, continuation 3): the register moves `pubrel_ready(old tuple) ->
	// pubrel_ready(new tuple)` with nothing else changed, and its exchange must stand at
	// PubRecObserved. The slot is never re-phased, so a PUBCOMP completes it at any moment by
	// today's arm. Choosing to re-emit is the serving connection's decision, so it requires the
	// SERVING authority at exactly the attempt's generation and tenure and moves no ownership.
	mqttTransitionRebindCanonical mqttTransitionKind = "rebind-canonical"
	// mqttTransitionDiscardExchange ends one obligation of a DISCARDED generation (issue #107 7d): its
	// exchange moves from a phase that does not yet release its holds to Discarded, and the batch
	// releases every hold it has. Its licence is a FACT, not an entitlement: the exchange's own
	// generation carries a committed discarding closure, which is immutable, so a check against it
	// has no window. Like advance-exchange it needs no serving authority and moves no ownership —
	// it runs under the CLOSED predecessor authority, or under a successor's after a Clean-Start
	// handoff — and it carries the exchange and its hold releases and nothing else: no register
	// (the reaper may already have taken it), no duty (the census resolves it later).
	mqttTransitionDiscardExchange mqttTransitionKind = "discard-exchange"
	// mqttTransitionFenceSharedRelease advances a SERVING authority's revision and nothing else
	// (issue #107 7e): the second step of a shared acquisition's release, written after the ledger
	// closed the acquisition, so every shared admission that read readiness before the close —
	// and CASes the authority sequence it read — loses its CAS. It carries no member and changes
	// no state or ownership; its licence (the verdict) lives on the shared ledger, out of this
	// boundary's sight, and its whole cost to a concurrent operation is one CAS re-read.
	mqttTransitionFenceSharedRelease mqttTransitionKind = "fence-shared-release"
	// mqttTransitionFencePendingDisposal advances a SERVING authority's revision and nothing else,
	// under the DECIDING connection's entitlement (issue #121): the step a pending cancellation fence of
	// the serving generation owes before the one door deletes it. Every admission of the fence's
	// nonce prepared before it CASes an authority sequence it moves past, and a displaced connection
	// cannot commit it. It carries no member and changes no state or ownership. See
	// pending_fence_disposal.go.
	mqttTransitionFencePendingDisposal mqttTransitionKind = "fence-pending-disposal"
)

// mqttOutcomeScopeClass is what an outcome's scope names: one generation, or the lifecycle.
type mqttOutcomeScopeClass uint8

const (
	mqttOutcomeScopeGeneration mqttOutcomeScopeClass = iota + 1
	mqttOutcomeScopeLifecycle
)

// mqttKindOutcomeScope is THE scope of each kind. A transition whose business is one
// generation's records is scoped to THAT generation, the one its members belong to and not the
// one the authority serves: old-generation cleanup under a successor is the old generation's.
// A transition that changes lifecycle-level state (an acceptance, the bind and abandonment, the
// teardown claim and release, the Will anchors) is scoped to the lifecycle, so its outcome
// survives the generation it closed or installed.
func mqttKindOutcomeScope(k mqttTransitionKind) mqttOutcomeScopeClass {
	switch k {
	case mqttTransitionTeardownClaim, mqttTransitionTeardownRelease, mqttTransitionRetireGeneration, mqttTransitionRetireLifecycle,
		mqttTransitionAcceptCreate, mqttTransitionAcceptHandoff, mqttTransitionAcceptReopen,
		mqttTransitionAcceptDisplace, mqttTransitionBind, mqttTransitionAcceptAbandon,
		mqttTransitionWillAnchor:
		return mqttOutcomeScopeLifecycle
	}
	return mqttOutcomeScopeGeneration
}

// mqttCheckOutcomeScope reports whether scope is a well-formed scope of kind k's class.
func mqttCheckOutcomeScope(k mqttTransitionKind, scope string) error {
	_, gen, ok := mqttParseOutcomeScope(scope)
	switch {
	case !ok:
		return fmt.Errorf("a %s attempt carries a malformed outcome scope %q", k, scope)
	case mqttKindOutcomeScope(k) == mqttOutcomeScopeGeneration && gen == _EMPTY_:
		return fmt.Errorf("a %s attempt is generation-scoped and carries the lifecycle scope %q", k, scope)
	case mqttKindOutcomeScope(k) == mqttOutcomeScopeLifecycle && gen != _EMPTY_:
		return fmt.Errorf("a %s attempt is lifecycle-scoped and carries the generation scope %q", k, scope)
	}
	return nil
}

// mqttKindResolvesIntent reports whether a kind names the acceptance it answers: the acceptance
// shapes, the bind, and the abandonment of a resume.
func mqttKindResolvesIntent(k mqttTransitionKind) bool {
	return mqttIsAcceptShape(k) || k == mqttTransitionBind || k == mqttTransitionAcceptAbandon
}

// mqttValidTransitionKind reports whether k is one this broker writes.
func mqttValidTransitionKind(k mqttTransitionKind) bool {
	switch k {
	case mqttTransitionTeardownClaim, mqttTransitionTeardownRelease, mqttTransitionRetireGeneration, mqttTransitionRetireLifecycle,
		mqttTransitionAdmitExchange, mqttTransitionAcquireAction,
		mqttTransitionDisposeResource, mqttTransitionProvisionResource,
		mqttTransitionAdvanceExchange, mqttTransitionMaterializePayload,
		mqttTransitionWithdrawUnsent, mqttTransitionAbandonDelivery, mqttTransitionTombstoneRegister,
		mqttTransitionRebindCanonical, mqttTransitionDiscardExchange, mqttTransitionFenceSharedRelease,
		mqttTransitionFencePendingDisposal, mqttTransitionAdvanceFloor, mqttTransitionReleaseHolds,
		mqttTransitionAcceptCreate, mqttTransitionAcceptHandoff, mqttTransitionAcceptReopen, mqttTransitionAcceptDisplace, mqttTransitionBind,
		mqttTransitionAcceptAbandon,
		mqttTransitionWillAnchor:
		return true
	}
	return false
}

// mqttTransitionAttempt is the stable, validated description of ONE attempted transition,
// shared by the three operations that must agree about it: the commit, the abort, and the
// resolution.
//
// It exists because a receipt carrying only a transition id proves the wrong thing. A
// caller resolving an ambiguous attempt knows what it tried to do; if the standing outcome
// describes a different operation, predecessor or resulting ownership, that is a
// CONTRADICTION and must not be read as its own answer. Without this, a caller could commit
// ownership for one tenure while permanently recording a receipt naming another, and the
// resolution would still report LANDED.
type mqttTransitionAttempt struct {
	// ID and Epoch are the exact identities this attempt is about. Neither is a hash: a
	// hash routes a lookup and attributes nothing.
	ID    string
	Epoch string

	// Txn is this ATTEMPT's identity, fresh per attempt, and names the outcome subject.
	Txn string
	// Scope is the outcome's scope (mqttKindOutcomeScope): the generation the members belong to,
	// or the lifecycle token. Part of the outcome subject, so the abort and the resolution that
	// hold only the attempt name the same subject the commit did.
	Scope string
	// Op is the LOGICAL operation, stable across the transport attempts of one transition.
	// A fresh batch id per transport attempt is a transport concern; this is what an
	// ambiguity resolution is about.
	Op   string
	Kind mqttTransitionKind

	// PrevTxn is the ownership transition this attempt expected to displace. Empty only
	// for a first admission, which has no predecessor to name.
	PrevTxn string

	// Intent names the handoff this attempt answers, and is required for exactly the two
	// operations that resolve one: a finalization and a cancellation. Reading whatever
	// intent the predecessor happens to carry is not the same as answering the intent this
	// operation was for — a caller that reloaded and followed a different pending handoff
	// would otherwise be indistinguishable.
	Intent string

	// The ownership this attempt intends to produce.
	Rev      uint64
	Gen      string
	Tenure   string
	OwnerTxn string
}

// mqttValidateTransitionAttempt refuses an attempt description this broker cannot act on.
func mqttValidateTransitionAttempt(a *mqttTransitionAttempt) error {
	if a == nil {
		return fmt.Errorf("transition attempt is absent")
	}
	if a.ID == _EMPTY_ {
		return fmt.Errorf("transition attempt names no client")
	}
	if !mqttOutboundIdentTk(a.Epoch) {
		return fmt.Errorf("transition attempt carries a malformed recovery epoch %q", a.Epoch)
	}
	if !mqttOutboundIdentTk(a.Txn) {
		return fmt.Errorf("transition attempt carries a malformed transition id %q", a.Txn)
	}
	if !mqttOutboundIdentTk(a.Op) {
		return fmt.Errorf("transition attempt carries a malformed operation id %q", a.Op)
	}
	if !mqttValidTransitionKind(a.Kind) {
		return fmt.Errorf("transition attempt carries an unsupported kind %q", a.Kind)
	}
	if serr := mqttCheckOutcomeScope(a.Kind, a.Scope); serr != nil {
		return serr
	}
	if a.PrevTxn != _EMPTY_ && !mqttOutboundIdentTk(a.PrevTxn) {
		return fmt.Errorf("transition attempt names a malformed predecessor transition %q", a.PrevTxn)
	}
	// AN AUTHORITY-FREE KIND proposes no revision and displaces no predecessor; every other kind
	// proposes one.
	if free := mqttAttemptAuthorityFree(a.Kind, a.Rev); free && a.PrevTxn != _EMPTY_ {
		return fmt.Errorf("an authority-free %s attempt names predecessor %q: it displaces none", a.Kind, a.PrevTxn)
	} else if !free && a.Rev == 0 {
		return fmt.Errorf("transition attempt proposes no authority revision")
	}
	if !mqttValidSessGen(a.Gen) {
		return fmt.Errorf("transition attempt proposes a malformed generation %q", a.Gen)
	}
	if !mqttOutboundIdentTk(a.Tenure) {
		return fmt.Errorf("transition attempt proposes a malformed tenure %q", a.Tenure)
	}
	if !mqttOutboundIdentTk(a.OwnerTxn) {
		return fmt.Errorf("transition attempt proposes a malformed ownership transition %q", a.OwnerTxn)
	}
	// A first admission is the only attempt with no predecessor, and it is exactly the
	// attempt that installs revision 1. Any later revision displaces something, and an
	// attempt that cannot name what it displaces could finalize against whatever it found.
	if !mqttAttemptAuthorityFree(a.Kind, a.Rev) && (a.PrevTxn == _EMPTY_) != (a.Rev == 1) {
		return fmt.Errorf("transition attempt at revision %d names predecessor %q: only a first admission has none",
			a.Rev, a.PrevTxn)
	}
	// The intent is required by exactly the kinds that resolve one (the acceptances, the bind and
	// the abandonment of a resume), and refused by the rest: an attempt that carried one where it
	// means nothing would have a field no reader checks, which is how a stale value survives to be
	// believed later.
	needsIntent := mqttKindResolvesIntent(a.Kind)
	if needsIntent && !mqttOutboundIdentTk(a.Intent) {
		return fmt.Errorf("a %s attempt names no handoff intent", a.Kind)
	}
	if !needsIntent && a.Intent != _EMPTY_ {
		return fmt.Errorf("a %s attempt names handoff intent %q, which it does not resolve", a.Kind, a.Intent)
	}
	return nil
}

// ---------------------------------------------------------------------------
// The records
// ---------------------------------------------------------------------------

// mqttAuthorityState is the lifecycle of one client's outbound authority.
//
// The states are deliberately few, and none of them is "unknown": an authority that cannot
// be read is refused by its reader rather than classified here: a record you cannot
// interpret may name a tenure newer than yours.
//
// The constants start at 1. A missing or null `state` member decodes to the zero value,
// and a zero naming a real state would make an unsupported persisted shape read as
// SERVING, the most permissive answer available. The closed-schema decode refuses such a
// body outright; this numbering means a bug that bypassed it still cannot manufacture a
// servable authority.
type mqttAuthorityState uint8

const (
	// mqttAuthorityServing is the ordinary state: Tenure names the tenure admitted to
	// create outbound obligations for Gen.
	mqttAuthorityServing mqttAuthorityState = 1
	// mqttAuthorityClosed records that Gen is permanently closed to new admission. The
	// permanent closure record is the authority for that; this state is the same fact in
	// the place contention happens.
	mqttAuthorityClosed mqttAuthorityState = 3
)

// mqttOutboundAuthority is the per-client arbitration record.
//
// Rev is the authority's OWN monotonic revision, distinct from the JetStream sequence the
// CAS is expressed against and distinct from the tenure identity. It exists because an
// outcome has to be able to say which authority revision a committed transition produced,
// and a storage sequence cannot: a completion that legitimately advances the record moves
// the sequence without moving the ownership those revisions describe.
type mqttOutboundAuthority struct {
	// ID and Epoch are the exact identity this body claims, carried in the body rather
	// than read off the subject: the subject carries a HASH, which routes a lookup and
	// attributes nothing.
	ID    string `json:"id"`
	Epoch string `json:"epoch"`

	Gen    string             `json:"gen"`
	Tenure string             `json:"tenure"`
	State  mqttAuthorityState `json:"state"`
	Rev    uint64             `json:"rev"`

	// Txn names the transition that produced THIS BODY, so a caller holding an ambiguous
	// acknowledgement can decide from the record itself whether its own attempt is the one
	// standing here.
	Txn string `json:"txn"`

	// OwnerTxn names the transition that installed the OWNERSHIP this body describes, and
	// it is preserved across a preparation that does not change owner. The two are
	// different questions — "which write produced these bytes" and "which transition owns
	// this session" — and a handoff names the second, so collapsing them would let a
	// preparation silently retarget the predecessor a finalization must match.
	OwnerTxn string `json:"owner_txn"`

	// Life is the lifecycle this authority belongs to and what reclamation needs it to say
	// (lifecycle_authority.go).
	Life *mqttLifecycleState `json:"life,omitempty"`
}

// mqttRetirementRequest names the exact predecessor a handoff asks to retire.
//
// Naming the predecessor IN FULL is the point, and it must be the same predecessor the
// intent names: a retirement that reloaded the current authority and retargeted whatever it
// found would let a stale expiry or cleanup callback acquire a privilege it never won,
// which is the shape that lets a losing metadata operation destroy the winner's
// obligations.
type mqttRetirementRequest struct {
	Gen      string `json:"gen"`
	Tenure   string `json:"tenure"`
	OwnerTxn string `json:"owner_txn"`
	// The predecessor's session-record identity, carried so the retirement names all five
	// components D2 requires rather than the three that identify ownership alone. A
	// retirement that named fewer could be satisfied by a predecessor whose metadata half
	// had since been replaced.
	SessWID string `json:"sess_wid"`
	SessSeq uint64 `json:"sess_seq"`
	// Reason is one of the supported retirement reasons; a free string would let a
	// contradictory request describe itself into legitimacy.
	Reason mqttRetirementReason `json:"reason"`
	// The predecessor's AFFINE target, carried so the closure that ends its generation can
	// enrol it AFTER the accepted batch has displaced the record that named it (issue #107).
	//
	// Retention, not proof. `$MQTT_sess` carries MaxMsgsPer 1, so the write that displaces a
	// session record EVICTS its body: by the time the finalization writes the closure, the
	// membership it would have read is gone. This request is written while that record still
	// stands, which is what makes the name available at all — and it is the acceptance's
	// assertion that it is the RIGHT name, audited rather than enforced.
	//
	// OPTIONAL, because a retirement that transfers nothing carries none. The shared group
	// durables are deliberately NOT here: a generation's shared ACQUISITIONS are released by the
	// closing from the generation's own acquisition ledger (shared_lifecycle_ops.go), so no
	// retirement request names them and no closure enrols them.
	AffineDurable string `json:"affine_durable,omitempty"`
	AffineGen     string `json:"affine_gen,omitempty"`
	PubRelDurable string `json:"pubrel_durable,omitempty"`
}

// mqttRetirementReason is the closed set of reasons a generation is retired. Every retirement
// DISCARDS the generation it names (issue #133: a noncurrent generation is a discarded one); the
// reason says only what ended it.
type mqttRetirementReason string

const (
	// mqttRetireCleanStart is an explicit client-requested discard, which replaces the
	// generation in the acceptance that displaces it.
	mqttRetireCleanStart mqttRetirementReason = "clean-start"
	// mqttRetireSessionEnd is the OWNING broker discarding its own session record because
	// the session's retention ended: a clean-session close, a zero session-expiry
	// disconnect, or an elapsed expiry that removes the record. Unlike Clean Start it is
	// not driven by a displacing CONNECT and carries no handoff: it is a standalone
	// teardown, authorized at the teardown claim's write boundary.
	mqttRetireSessionEnd mqttRetirementReason = "session-end"
)

// mqttValidRetirementReason reports whether r is one this broker writes.
func mqttValidRetirementReason(r mqttRetirementReason) bool {
	switch r {
	case mqttRetireCleanStart, mqttRetireSessionEnd, mqttRetireAbandoned:
		return true
	}
	return false
}

// mqttGenClosure is the permanent record that one generation is closed to new admission.
//
// EVERY CLOSURE DISCARDS (issue #133). A generation becomes noncurrent only by being discarded:
// a preserving close keeps it current, so there is no closure that ends admission while keeping
// the obligations, and no member saying which of the two a closure is.
type mqttGenClosure struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	// Txn names the transition that closed it, so the closure and the outcome that
	// produced it can be reconciled without either being inferred from the other.
	Txn string `json:"txn"`
	// RootShard and RootOrd are the ADDRESS of the recovery root this closure's own batch
	// allocated. Both are MANDATORY, and they are read rather than searched for: the release
	// gate loads the root at this address instead of enumerating a shard by wildcard, which
	// can return a short answer for a filter with many subjects. An enumeration that misses a
	// root reads as no reachable root, and the gate would then demand a completion certificate
	// for a generation whose duties are in fact enrolled.
	//
	// SHARD ZERO IS A REAL SHARD and ordinal zero is not, so neither is validated by its value
	// alone: the members are required to be PRESENT, which `mqttJSONMembersArePresent` decides
	// on the body rather than on the decoded struct.
	RootShard uint64 `json:"root_shard"`
	RootOrd   uint64 `json:"root_ord"`
}

// mqttOutcomeVerdict is a transition's permanent verdict. There are exactly two, and
// neither can be written over the other. As with the authority state the zero value names
// no verdict, so a body missing the member cannot decode as the permissive one.
type mqttOutcomeVerdict uint8

const (
	mqttOutcomeCommitted mqttOutcomeVerdict = 1
	mqttOutcomeAborted   mqttOutcomeVerdict = 2
)

// mqttTransitionOutcome is one attempted transition's permanent verdict.
//
// Rev, Gen, Tenure and OwnerTxn are meaningful only for a committed outcome, and record
// what the transition PRODUCED. A later authority revision does not retract them: it means
// this transition landed and was subsequently superseded, which is a different answer from
// "this transition never landed" and is exactly the distinction a caller holding a lost
// acknowledgement needs.
type mqttTransitionOutcome struct {
	ID      string             `json:"id"`
	Epoch   string             `json:"epoch"`
	Txn     string             `json:"txn"`
	Scope   string             `json:"scope"`
	Op      string             `json:"op"`
	Kind    mqttTransitionKind `json:"kind"`
	Verdict mqttOutcomeVerdict `json:"verdict"`

	// PrevTxn is the ownership predecessor this transition expected, so an outcome read in
	// isolation still says what it was conditioned on.
	PrevTxn string `json:"prev_txn,omitempty"`

	// Intent names the handoff this transition resolved, for the two kinds that resolve
	// one. It is part of the committed binding: a receipt that did not record WHICH handoff
	// it answered would let a finalization of one be read as the answer to another.
	Intent string `json:"intent,omitempty"`

	// The ownership this transition produced. Present for a committed verdict only.
	Rev      uint64 `json:"rev,omitempty"`
	Gen      string `json:"gen,omitempty"`
	Tenure   string `json:"tenure,omitempty"`
	OwnerTxn string `json:"owner_txn,omitempty"`

	// Digest binds this outcome to the mutation its batch carried — the attempt, the
	// authority bytes and expectation, and every member's subject, expectation and body —
	// so an outcome cannot be read as the receipt of a different batch that happened to
	// share a name.
	Digest string `json:"digest,omitempty"`
}

// ---------------------------------------------------------------------------
// The closed-schema decoders
// ---------------------------------------------------------------------------
//
// Every persisted body is decoded HERE and nowhere else, and the decode is where the
// compatibility boundary lives: a body carrying a member this broker does not write, a
// member twice, or a mandatory member absent or null, is REFUSED. It is never normalized by
// a typed decode and rewritten, which is the repair the boundary forbids — and which would
// silently turn an unsupported shape into a supported one on the next save. A struct-level
// test cannot establish this; only a raw-byte one can.

var mqttAuthorityMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "tenure": {}, "state": {}, "rev": {},
	"txn": {}, "owner_txn": {}, "life": {},
}

var mqttAuthorityRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "tenure": {}, "state": {}, "rev": {},
	"txn": {}, "owner_txn": {},
}

var mqttClosureMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "txn": {},
	"root_shard": {}, "root_ord": {},
}

// The outcome's legal member set DEPENDS ON ITS VERDICT. An aborted outcome carrying
// ownership members — even spelled as explicit zeroes — is contradictory rather than merely
// over-specified, so the two sets are separate and the verdict is read first.
var mqttOutcomeCommonMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "txn": {}, "scope": {}, "op": {}, "kind": {}, "verdict": {}, "prev_txn": {}, "intent": {},
}

var mqttOutcomeCommittedMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "txn": {}, "scope": {}, "op": {}, "kind": {}, "verdict": {}, "prev_txn": {}, "intent": {},
	"rev": {}, "gen": {}, "tenure": {}, "owner_txn": {}, "digest": {},
}

var mqttOutcomeCommonRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "txn": {}, "scope": {}, "op": {}, "kind": {}, "verdict": {},
}

var mqttOutcomeCommittedRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "txn": {}, "scope": {}, "op": {}, "kind": {}, "verdict": {},
	"rev": {}, "gen": {}, "tenure": {}, "owner_txn": {}, "digest": {},
}

// mqttOutcomeCommittedFreeRequired is the committed receipt of a kind that may run authority-free:
// an authority-free attempt produced no revision, so it carries none (mqttAttemptAuthorityFree).
var mqttOutcomeCommittedFreeRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "txn": {}, "scope": {}, "op": {}, "kind": {}, "verdict": {},
	"gen": {}, "tenure": {}, "owner_txn": {}, "digest": {},
}

// mqttJSONOptionalMembersNotNull refuses an explicitly null optional member.
//
// Optional means ABSENT OR A VALID SUPPORTED VALUE, never null. These writers use
// omitempty, so they never emit null; a body that carries one was not written by this
// broker, and letting the typed decode turn it into the zero value is the normalization the
// compatibility boundary forbids. mqttJSONMembersArePresent already makes this refusal for
// mandatory members, and this is the same rule for the rest.
func mqttJSONOptionalMembersNotNull(data []byte, optional map[string]struct{}, noun string) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for key := range optional {
		v, ok := raw[key]
		if !ok {
			continue
		}
		if string(bytes.TrimSpace(v)) == "null" {
			return mqttShapeErr(noun + " member " + key + " is present and null")
		}
	}
	return nil
}

// The optional members of each class, which is what the rule above is applied to.
var (
	mqttAuthorityOptional = map[string]struct{}{"life": {}}
	mqttOutcomeOptional   = map[string]struct{}{
		"prev_txn": {}, "intent": {}, "rev": {}, "gen": {}, "tenure": {}, "owner_txn": {}, "digest": {},
	}
)

// mqttDecodeOutboundAuthority decodes and fully validates one authority body.
//
// wantID and wantEpoch are the EXACT client and account recovery epoch the caller is
// operating as. They are compared against the body rather than against a hash of it: a hash
// routes the lookup, and attributing a record on a hash alone would let a collision be
// served as the requested client.
func mqttDecodeOutboundAuthority(data []byte, wantID, wantEpoch string) (*mqttOutboundAuthority, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttAuthorityMembers, "an outbound authority"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttAuthorityRequired, "an outbound authority"); err != nil {
		return nil, err
	}
	if err := mqttJSONOptionalMembersNotNull(data, mqttAuthorityOptional, "an outbound authority"); err != nil {
		return nil, err
	}
	if err := mqttCheckLifecycleSection(data); err != nil {
		return nil, err
	}
	a := &mqttOutboundAuthority{}
	if err := json.Unmarshal(data, a); err != nil {
		return nil, err
	}
	if err := mqttValidateOutboundAuthority(a, wantID, wantEpoch); err != nil {
		return nil, err
	}
	return a, nil
}

// mqttDecodeGenClosure decodes and fully validates one closure body.
func mqttDecodeGenClosure(data []byte, wantID, wantEpoch, wantGen string) (*mqttGenClosure, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttClosureMembers, "a generation closure"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttClosureMembers, "a generation closure"); err != nil {
		return nil, err
	}
	c := &mqttGenClosure{}
	if err := json.Unmarshal(data, c); err != nil {
		return nil, err
	}
	if err := mqttValidateGenClosure(c, wantID, wantEpoch, wantGen); err != nil {
		return nil, err
	}
	return c, nil
}

// mqttDecodeTransitionOutcome decodes and fully validates one outcome body, choosing the
// legal member set from the verdict the body itself carries.
func mqttDecodeTransitionOutcome(data []byte, wantID, wantEpoch, wantTxn string) (*mqttTransitionOutcome, error) {
	// The verdict decides which members are legal, so it is read before the member set is
	// applied — and it is itself mandatory, so a body without one is refused rather than
	// defaulted to the permissive reading.
	if err := mqttJSONMembersArePresent(data, mqttOutcomeCommonRequired, "a transition outcome"); err != nil {
		return nil, err
	}
	probe := struct {
		Verdict mqttOutcomeVerdict `json:"verdict"`
		Kind    mqttTransitionKind `json:"kind"`
	}{}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, err
	}
	var members, required map[string]struct{}
	switch probe.Verdict {
	case mqttOutcomeCommitted:
		members, required = mqttOutcomeCommittedMembers, mqttOutcomeCommittedRequired
		if mqttKindMayOmitAuthority(probe.Kind) {
			required = mqttOutcomeCommittedFreeRequired
		}
	case mqttOutcomeAborted:
		members, required = mqttOutcomeCommonMembers, mqttOutcomeCommonRequired
	default:
		return nil, mqttShapeErr("a transition outcome carries an unsupported verdict " +
			strconv.FormatUint(uint64(probe.Verdict), 10))
	}
	if err := mqttJSONMembersAreExactAndUnique(data, members, "a transition outcome"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, required, "a transition outcome"); err != nil {
		return nil, err
	}
	if err := mqttJSONOptionalMembersNotNull(data, mqttOutcomeOptional, "a transition outcome"); err != nil {
		return nil, err
	}
	o := &mqttTransitionOutcome{}
	if err := json.Unmarshal(data, o); err != nil {
		return nil, err
	}
	if err := mqttValidateTransitionOutcome(o, wantID, wantEpoch, wantTxn); err != nil {
		return nil, err
	}
	return o, nil
}

// ---------------------------------------------------------------------------
// The semantic validators
// ---------------------------------------------------------------------------

// mqttValidateOutboundAuthority refuses any body this broker does not positively support.
//
// Refused rather than repaired: the compatibility boundary makes an unsupported persisted
// shape a refusal, and filling a missing field from sess.seq, a claim sequence or the
// standing watermark would manufacture exactly the entitlement this record exists to
// arbitrate.
func mqttValidateOutboundAuthority(a *mqttOutboundAuthority, wantID, wantEpoch string) error {
	if a == nil {
		return fmt.Errorf("outbound authority is absent")
	}
	if a.ID != wantID {
		return fmt.Errorf("outbound authority names client %q, not %q", a.ID, wantID)
	}
	if a.Epoch != wantEpoch {
		return fmt.Errorf("outbound authority carries recovery epoch %q, not this account's %q", a.Epoch, wantEpoch)
	}
	if !mqttValidSessGen(a.Gen) {
		return fmt.Errorf("outbound authority carries a malformed generation %q", a.Gen)
	}
	if !mqttOutboundIdentTk(a.Tenure) {
		return fmt.Errorf("outbound authority carries a malformed tenure %q", a.Tenure)
	}
	if !mqttOutboundIdentTk(a.Txn) {
		return fmt.Errorf("outbound authority carries a malformed transition id %q", a.Txn)
	}
	if !mqttOutboundIdentTk(a.OwnerTxn) {
		return fmt.Errorf("outbound authority carries a malformed ownership transition id %q", a.OwnerTxn)
	}
	if a.Rev == 0 {
		return fmt.Errorf("outbound authority carries no revision")
	}
	if a.Life != nil {
		if err := mqttValidateLifecycleState(a.Life, a.Gen, a.State == mqttAuthorityServing); err != nil {
			return fmt.Errorf("outbound authority: %w", err)
		}
	}
	switch a.State {
	case mqttAuthorityServing, mqttAuthorityClosed:
	default:
		return fmt.Errorf("outbound authority carries an unsupported state %d", a.State)
	}
	return nil
}

// mqttValidateAffineDescriptor is the ONE definition of a well-formed affine target, used by every
// record that carries one: the retirement request that selects it, the resource the closing batch
// enrols, and the readiness assertion that licenses its seal.
//
// # Why it is one function
//
// Six holistic rounds found this same class one record at a time — a field checked for SHAPE but
// not against the identity that determines it, or checked at one record and not at the next. The
// failure mode of the spread-out version is not only a missed substitution: a descriptor accepted
// HERE and refused at the resource is an ACCEPTED-BUT-UNFINISHABLE shape, and once the metadata
// half of a handoff commits, cancellation needs an ABORTED verdict, so finalization keeps failing
// on a descriptor that can no longer be changed. Every record therefore asks the same question of
// the same derivation.
//
// gen is the generation the target belongs to — the RETIRING one, which owns the PUBREL consumer
// holding the proofs; affGen is the affine incarnation, which owns the affine durable.
func mqttValidateAffineDescriptor(idHash, gen, affGen, affDurable, pubRelDurable string) error {
	if affGen == _EMPTY_ && affDurable == _EMPTY_ && pubRelDurable == _EMPTY_ {
		return nil // no affine target named at all, which is a supported shape
	}
	// A PARTIALLY NAMED TARGET needs no rule of its own: the derivations below refuse every one of
	// them, because an empty durable is not the name any generation derives and an empty affine
	// generation derives no name that matches a non-empty durable. A separate both-or-none check
	// was written first and DELETED — no input distinguished it, so it was two mechanisms stating
	// one fact (`feedback_delete_redundant_mechanisms`), and a mutation removing it could never
	// have killed.
	if !mqttValidSessGen(affGen) {
		return fmt.Errorf("an affine target carries a malformed affine generation %q, which no "+
			"readiness assertion could name", affGen)
	}
	if _, _, want := mqttQ2AffineNames(idHash, affGen); affDurable != want {
		return fmt.Errorf("an affine target names durable %q, and affine generation %q owns %q",
			affDurable, affGen, want)
	}
	if _, _, want := mqttPubRelNames(idHash, gen); pubRelDurable != want {
		return fmt.Errorf("an affine target names PUBREL consumer %q, and generation %q owns %q: "+
			"the proofs it owes live in the consumer its own generation names",
			pubRelDurable, gen, want)
	}
	return nil
}

// mqttAffineRetireDescriptor is the affine target a retirement names: the affine durable, its
// affine generation and the PUBREL consumer that carries its proofs — all three when the retiring
// generation holds an affine QoS 2 consumer, none when it holds none (issue #115).
//
// The PUBREL durable is the affine target's PROOF SOURCE, never a target of its own: an ordinary
// QoS 2 session's PUBREL consumer is a provisioned resource, enrolled when it was created and
// retired by the recovery executor from that record. Naming it alone produced a half-named
// descriptor that mqttValidateAffineDescriptor refuses by design, and a clean start over any
// ordinary QoS 2 session was refused before CONNACK. A descriptor that is half-named on the
// affine side (a durable without its generation, or the reverse) is passed through unchanged, so
// the validator still refuses it.
func mqttAffineRetireDescriptor(affDur, affGen, pubRelDur string) (string, string, string) {
	if affDur == _EMPTY_ && affGen == _EMPTY_ {
		return _EMPTY_, _EMPTY_, _EMPTY_
	}
	return affDur, affGen, pubRelDur
}

// mqttValidateGenClosure refuses a closure body this broker does not support. A closure
// that cannot be read is an inconsistency, and its readers refuse the decisions that depend
// on it rather than treating the generation as open.
func mqttValidateGenClosure(c *mqttGenClosure, wantID, wantEpoch, wantGen string) error {
	if c == nil {
		return fmt.Errorf("generation closure is absent")
	}
	if c.ID != wantID {
		return fmt.Errorf("generation closure names client %q, not %q", c.ID, wantID)
	}
	if c.Epoch != wantEpoch {
		return fmt.Errorf("generation closure carries recovery epoch %q, not this account's %q", c.Epoch, wantEpoch)
	}
	if c.Gen != wantGen {
		return fmt.Errorf("generation closure names generation %q on the subject of %q", c.Gen, wantGen)
	}
	if !mqttValidSessGen(c.Gen) {
		return fmt.Errorf("generation closure carries a malformed generation %q", c.Gen)
	}
	if !mqttOutboundIdentTk(c.Txn) {
		return fmt.Errorf("generation closure carries a malformed transition id %q", c.Txn)
	}
	if c.RootShard >= mqttDirShards {
		return fmt.Errorf("generation closure names recovery shard %d, outside the %d this broker "+
			"addresses", c.RootShard, mqttDirShards)
	}
	// THE ROOT'S SHARD IS DERIVED FROM THE CLIENT, so a closure naming another one addresses a
	// root no traversal of this client's work would ever reach.
	if got := mqttDirShardFor(getHash(c.ID)); got != c.RootShard {
		return fmt.Errorf("generation closure for client %q names recovery shard %d, but that "+
			"client hashes to %d", c.ID, c.RootShard, got)
	}
	if c.RootOrd == 0 {
		return fmt.Errorf("generation closure names recovery ordinal zero, and ordinals start at 1")
	}
	return nil
}

// mqttValidateTransitionOutcome refuses an outcome body this broker does not support.
//
// A committed verdict must describe the ownership it produced, because that is the only
// thing that distinguishes "landed and was superseded" from "landed and still stands". An
// aborted verdict must describe none of it: an abort produced no ownership, and a body
// claiming otherwise is not a weaker abort but a contradictory one.
func mqttValidateTransitionOutcome(o *mqttTransitionOutcome, wantID, wantEpoch, wantTxn string) error {
	if o == nil {
		return fmt.Errorf("transition outcome is absent")
	}
	if o.ID != wantID {
		return fmt.Errorf("transition outcome names client %q, not %q", o.ID, wantID)
	}
	if o.Epoch != wantEpoch {
		return fmt.Errorf("transition outcome carries recovery epoch %q, not this account's %q", o.Epoch, wantEpoch)
	}
	if o.Txn != wantTxn {
		return fmt.Errorf("transition outcome names transition %q on the subject of %q", o.Txn, wantTxn)
	}
	if !mqttOutboundIdentTk(o.Op) {
		return fmt.Errorf("transition outcome carries a malformed operation id %q", o.Op)
	}
	if !mqttValidTransitionKind(o.Kind) {
		return fmt.Errorf("transition outcome carries an unsupported kind %q", o.Kind)
	}
	if serr := mqttCheckOutcomeScope(o.Kind, o.Scope); serr != nil {
		return serr
	}
	if o.PrevTxn != _EMPTY_ && !mqttOutboundIdentTk(o.PrevTxn) {
		return fmt.Errorf("transition outcome names a malformed predecessor transition %q", o.PrevTxn)
	}
	// The kinds that must NAME the acceptance they answer must record it, so the outcome says
	// which one it was for: the attempt's set (mqttKindResolvesIntent), one definition.
	resolvesHandoff := mqttKindResolvesIntent(o.Kind)
	if resolvesHandoff && !mqttOutboundIdentTk(o.Intent) {
		return fmt.Errorf("a %s outcome names no handoff intent", o.Kind)
	}
	if !resolvesHandoff && o.Intent != _EMPTY_ {
		return fmt.Errorf("a %s outcome names handoff intent %q, which it does not resolve", o.Kind, o.Intent)
	}
	switch o.Verdict {
	case mqttOutcomeCommitted:
		// An authority-free kind produced no revision; every other kind produced exactly one.
		if free := mqttAttemptAuthorityFree(o.Kind, o.Rev); free && o.PrevTxn != _EMPTY_ {
			return fmt.Errorf("a committed authority-free %s outcome names predecessor %q: it displaced none",
				o.Kind, o.PrevTxn)
		} else if !free && o.Rev == 0 {
			return fmt.Errorf("committed outcome carries no authority revision")
		}
		if !mqttValidSessGen(o.Gen) {
			return fmt.Errorf("committed outcome carries a malformed generation %q", o.Gen)
		}
		if !mqttOutboundIdentTk(o.Tenure) {
			return fmt.Errorf("committed outcome carries a malformed tenure %q", o.Tenure)
		}
		if !mqttOutboundIdentTk(o.OwnerTxn) {
			return fmt.Errorf("committed outcome carries a malformed ownership transition %q", o.OwnerTxn)
		}
		if o.Digest == _EMPTY_ {
			return fmt.Errorf("committed outcome carries no mutation digest")
		}
		if !mqttAttemptAuthorityFree(o.Kind, o.Rev) && (o.PrevTxn == _EMPTY_) != (o.Rev == 1) {
			return fmt.Errorf("committed outcome at revision %d names predecessor %q: only a first admission has none",
				o.Rev, o.PrevTxn)
		}
	case mqttOutcomeAborted:
		if o.Rev != 0 || o.Gen != _EMPTY_ || o.Tenure != _EMPTY_ ||
			o.OwnerTxn != _EMPTY_ || o.Digest != _EMPTY_ {
			return fmt.Errorf("aborted outcome describes ownership it did not produce")
		}
	default:
		return fmt.Errorf("transition outcome carries an unsupported verdict %d", o.Verdict)
	}
	return nil
}

// mqttOutcomeMatchesAttempt reports whether a standing outcome describes the attempt the
// caller is resolving.
//
// A resolution that skipped this would answer a question nobody asked: the outcome subject
// is named by the transition id alone, so a receipt found there proves only that SOME batch
// under that id committed. A mismatch on any of these is a CONTRADICTION — the caller
// intended one mutation and the durable record describes another — and must never be
// reported as a match.
func mqttOutcomeMatchesAttempt(o *mqttTransitionOutcome, a *mqttTransitionAttempt) error {
	if o.Op != a.Op {
		return fmt.Errorf("outcome names operation %q, the attempt %q", o.Op, a.Op)
	}
	if o.Kind != a.Kind {
		return fmt.Errorf("outcome names kind %q, the attempt %q", o.Kind, a.Kind)
	}
	if o.Scope != a.Scope {
		return fmt.Errorf("outcome names scope %q, the attempt %q", o.Scope, a.Scope)
	}
	if o.PrevTxn != a.PrevTxn {
		return fmt.Errorf("outcome names predecessor %q, the attempt %q", o.PrevTxn, a.PrevTxn)
	}
	if o.Intent != a.Intent {
		return fmt.Errorf("outcome names handoff intent %q, the attempt %q", o.Intent, a.Intent)
	}
	if o.Verdict == mqttOutcomeAborted {
		return nil
	}
	if o.Rev != a.Rev || o.Gen != a.Gen || o.Tenure != a.Tenure || o.OwnerTxn != a.OwnerTxn {
		return fmt.Errorf("outcome produced %d/%q/%q/%q, the attempt proposed %d/%q/%q/%q",
			o.Rev, o.Gen, o.Tenure, o.OwnerTxn, a.Rev, a.Gen, a.Tenure, a.OwnerTxn)
	}
	return nil
}

// ---------------------------------------------------------------------------
// The mutation digest
// ---------------------------------------------------------------------------

// mqttTransitionDigest binds an outcome to the exact mutation its batch carried: the
// attempt's own description, the authority bytes and expectation, and every caller member's
// subject, expectation and body.
//
// Without it a receipt proves only that SOME batch under this transition id committed. With
// it, a caller resolving an ambiguous attempt can state what it tried to write and detect a
// standing outcome describing something else — a contradiction to refuse, not a match to
// act on.
//
// Members are sorted, so the digest describes the batch's CONTENT rather than the order the
// caller happened to assemble it in; the writer separately refuses duplicate subjects, so
// sorting can never merge two members into one. Every field is length-prefixed, so no pair
// of different field sequences can produce one byte stream.
func mqttTransitionDigest(a *mqttTransitionAttempt, authExpect uint64, authBody []byte, extra []mqttTransitionMember) string {
	h := sha256.New()
	writeField := func(parts ...string) {
		for _, p := range parts {
			h.Write([]byte(strconv.Itoa(len(p))))
			h.Write([]byte(":"))
			h.Write([]byte(p))
		}
	}
	writeField(a.ID, a.Epoch, a.Txn, a.Scope, a.Op, string(a.Kind), a.PrevTxn, a.Gen, a.Tenure, a.OwnerTxn, a.Intent)
	writeField(strconv.FormatUint(a.Rev, 10), strconv.FormatUint(authExpect, 10))
	writeField(string(authBody))

	lines := make([]string, 0, len(extra))
	for _, e := range extra {
		eh := sha256.Sum256(e.body)
		lines = append(lines, e.subj+"\x00"+strconv.FormatUint(e.expect, 10)+"\x00"+
			base64.RawURLEncoding.EncodeToString(eh[:]))
	}
	sort.Strings(lines)
	for _, l := range lines {
		writeField(l)
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// ---------------------------------------------------------------------------
// The classification handle
// ---------------------------------------------------------------------------

// mqttTransitionHandle answers one question and only one: did THIS exact frozen mutation
// commit?
//
// classifyAgainstOutcome answers it from the assembled transition's digest, which a caller holding its
// attempt in memory still has. A caller that has RESTARTED does not: it has neither the
// original authority body nor the original member bytes, so it cannot recompute the digest.
// This is the form that survives, because it retains the answer rather than the inputs — the
// complete immutable attempt identity plus the expected digest, and nothing that would be
// needed only to resend the mutation byte for byte.
//
// THE EXPECTED DIGEST MUST NEVER COME FROM THE RECEIPT BEING CHECKED. A handle built by
// reading the outcome and copying its digest would agree with every receipt, including one
// this broker never authored, and the verification would be a tautology. The constructor
// therefore takes the frozen transition and nothing else; there is deliberately no
// constructor from an outcome.
//
// WHAT THIS IS NOT. Holding a handle is not a restart-recovery protocol. A recovering broker
// does not resolve arbitrary historical transitions at all: it reconciles from durable state
// — the authority, the permanent closures, the exchange and inventory records, and the
// handoff metadata evidence — and needs a handle only where a persisted continuation says its
// next decision depends on whether one exact attempt committed. Placing and populating those
// continuations belongs to the callers WP2 wires, and each must choose state reconciliation
// or exact-attempt continuation and show its crash recovery through that route.
//
// And enumeration is never proof that an unobserved admission did not happen: a pre-crash
// batch can still arrive. Before any incompatible action, a recovering broker positively
// aborts the attempt it knows of, or wins a transition whose expectations fence the delayed
// one. Scan absence authorizes nothing.
type mqttTransitionHandle struct {
	attempt mqttTransitionAttempt
	digest  string
}

// mqttNewTransitionHandle freezes a transition into the handle that can classify it later.
//
// Taking the whole transition rather than the attempt is deliberate: the digest covers the
// authority body, the closure and every member, so a handle built from the attempt alone
// would describe a mutation the writer never sent.
func mqttNewTransitionHandle(t *mqttAuthorityTransition) (*mqttTransitionHandle, error) {
	if t == nil || t.Attempt == nil {
		return nil, fmt.Errorf("unreachable: a transition handle with no attempt")
	}
	if verr := mqttValidateTransitionAttempt(t.Attempt); verr != nil {
		return nil, fmt.Errorf("refusing a handle for an unsupported attempt: %w", verr)
	}
	digest, derr := mqttTransitionMutationDigest(t)
	if derr != nil {
		return nil, derr
	}
	return &mqttTransitionHandle{attempt: *t.Attempt, digest: digest}, nil
}
