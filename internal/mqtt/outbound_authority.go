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
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// The outbound authority (F-105).
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
// authorizes. Because JetStream's atomic batches are per stream, that co-location is why
// these records live on $MQTT_inflight beside the registers they govern rather than on
// $MQTT_sess beside the session record.
//
// Three facts are kept apart, because collapsing any two of them is what every prior
// attempt did:
//
//   - ADMISSION is the authority's positive state. A missing authority record does not
//     establish an open generation, and a missing anything never establishes a fact here.
//   - CLOSURE is a permanent per-generation record. It is AUTHORITATIVE, and the
//     $MQTT_teardown generation-retirement watermark becomes a recoverable index
//     projected from it — never independent closure evidence. "Retired" means no new
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

// mqttF105ClassTk is the fixed token that opens every subject class this file owns.
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
const mqttF105ClassTk = "f105"

// The per-class tokens. Each names the fact its record carries, not the mechanism that
// writes it.
const (
	mqttAuthorityClassTk = "auth"   // the tenure admitted to create outbound obligations
	mqttClosureClassTk   = "closed" // a generation permanently closed to new admission
	mqttOutcomeClassTk   = "txn"    // one attempted transition's permanent verdict
)

// mqttF105SubjectPrefix is the common prefix of every class above.
func mqttF105SubjectPrefix() string {
	return mqttInflightStreamSubjectPrefix + mqttF105ClassTk + "."
}

// mqttIsF105Subject reports whether a subject belongs to any F-105 record class.
func mqttIsF105Subject(subject string) bool {
	return strings.HasPrefix(subject, mqttF105SubjectPrefix())
}

// mqttIsF105ArbitrationSubject reports whether a subject is one of the three records the
// TRANSITION WRITER ITSELF owns: the authority, a permanent generation closure, a permanent
// transition outcome.
//
// The distinction from the obligation classes matters and is not cosmetic. These three are
// how a transition is arbitrated and recorded, so a caller-supplied member naming one would
// let a transition rewrite the very evidence that says what it did — and atomicity would
// commit it faithfully, because it validates ordering and never meaning. The obligation
// classes an admission installs (an exchange, an action) are the opposite: carrying them is
// the whole point of the batch.
func mqttIsF105ArbitrationSubject(subject string) bool {
	if !mqttIsF105Subject(subject) {
		return false
	}
	rest := subject[len(mqttF105SubjectPrefix()):]
	for _, cls := range []string{mqttAuthorityClassTk, mqttClosureClassTk, mqttOutcomeClassTk} {
		if strings.HasPrefix(rest, cls+".") {
			return true
		}
	}
	return false
}

// mqttF105ObligationClass returns the obligation class a subject names, and whether it is
// one at all. A subject under the F-105 prefix that is neither an arbitration record nor a
// known obligation class is refused rather than guessed at.
//
// The GENERATION LEDGER and the DUTY INVENTORY are obligation classes, not arbitration records,
// and the distinction is load-bearing. An arbitration record is one the transition writer itself
// owns, so a caller-supplied member naming one could rewrite the evidence of what a transition
// did. These two are the opposite: an admission CARRIES its ledger CAS and its duty entries, in
// the same batch, because allocating an ordinal atomically with the obligation is the whole
// mechanism. Carrying them is the point.
func mqttF105ObligationClass(subject string) (string, bool) {
	if !mqttIsF105Subject(subject) {
		return _EMPTY_, false
	}
	rest := subject[len(mqttF105SubjectPrefix()):]
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
	return mqttF105SubjectPrefix() + mqttAuthorityClassTk + "." + idHash
}

// mqttGenClosureSubject returns the permanent closure subject for one generation.
//
// Permanent, and never deleted while the recovery epoch stands: closure is the fact every
// reader resolves entitlement against, and a closure that could be reaped would let a
// later reader conclude an open generation from its absence.
func mqttGenClosureSubject(idHash, gen string) string {
	return mqttF105SubjectPrefix() + mqttClosureClassTk + "." + idHash + "." + gen
}

// mqttTransitionOutcomeSubject returns the permanent outcome subject of one attempted
// transition.
//
// One subject per attempt, and both verdicts compete for it create-only. That mutual
// exclusion IS the protocol: a committed batch carries its COMMITTED member, and a caller
// that cannot decide publishes ABORTED to the same subject, so exactly one of the two can
// ever exist and the loser's whole batch is refused. Two slots, or one overwritable slot,
// would let an abort stand beside a later commit and decide nothing.
func mqttTransitionOutcomeSubject(idHash, txn string) string {
	return mqttF105SubjectPrefix() + mqttOutcomeClassTk + "." + idHash + "." + txn
}

// mqttGenClosureWalkFilter enumerates one client's closure records, and only those.
func mqttGenClosureWalkFilter(idHash string) string {
	return mqttF105SubjectPrefix() + mqttClosureClassTk + "." + idHash + ".*"
}

// mqttTransitionOutcomeWalkFilter enumerates one client's transition outcomes, and only
// those.
func mqttTransitionOutcomeWalkFilter(idHash string) string {
	return mqttF105SubjectPrefix() + mqttOutcomeClassTk + "." + idHash + ".*"
}

// mqttF105IdentTk reports whether s is a nonempty single subject token. Used for the
// identity tokens this file mints rather than derives — a transition id, a tenure id —
// which must not contain a separator or a wildcard, since every name here is built by
// concatenation and a stored value is never a trusted input.
func mqttF105IdentTk(s string) bool {
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
	// There is deliberately NO kind that installs a SERVING authority outright.
	//
	// Two once existed — admit-tenure at revision 1 and readmit-tenure over a closure — and
	// both were the bootstrap gap itself: an authority entitled to serve a client with no
	// reservation behind it and no committed metadata half, which is the entitlement F-105
	// exists to make impossible. Every route to SERVING is now reserve-creation, a metadata
	// commit, and finalize-creation, whose verdict check reads the permanent receipt.
	mqttTransitionPrepareIntent mqttTransitionKind = "prepare-intent"
	mqttTransitionFinalize      mqttTransitionKind = "finalize"
	mqttTransitionCloseAdmit    mqttTransitionKind = "close-admission"
	// mqttTransitionCloseTeardown is the AUTHORIZED DISCARDING closure of a standalone
	// teardown: a session that ends with no displacing successor closes its OWN authority
	// and abandons its outbound obligations, which a close-admission may never do.
	//
	// It is close-admission with the one difference that it MAY discard, and the difference
	// is authorized rather than asserted: the operation loads and validates a durable
	// teardown receipt naming the exact retiring ownership before it composes the batch, and
	// carries that receipt's retirement onto the transition so the boundary binds the affine
	// enrolment to the target the receipt selected, exactly as a finalization binds to its
	// intent's. Ending a generation is still not authority to abandon what it owes; the
	// receipt is that authority (F-105, row 8, Gap 1).
	mqttTransitionCloseTeardown mqttTransitionKind = "close-teardown"
	mqttTransitionAdmitExchange mqttTransitionKind = "admit-exchange"
	mqttTransitionAcquireAction mqttTransitionKind = "acquire-action"
	// mqttTransitionCancelIntent returns a PREPARING authority to serving under its
	// ORIGINAL ownership, on proof that the metadata half was positively aborted.
	//
	// Without it a preparation whose metadata half lost is a dead end: admission and
	// preparation both require SERVING, finalization requires the proposed ownership, and
	// closing admission would destroy a session that was never displaced. A broker would
	// have to either delete the authority or wait forever, and the first is the shape this
	// item exists to remove.
	mqttTransitionCancelIntent mqttTransitionKind = "cancel-intent"
	// mqttTransitionReserveCreation claims a client with a proposal that serves nothing.
	//
	// It installs a RESERVING authority, which grants no serving permission: the shape
	// virgin creation lacked. Two predecessors are legal and they are ONE fact, not two
	// kinds — an ABSENT one at revision 1, create-only by virtue of that, and a CLOSED one
	// proposing a fresh generation.
	//
	// The second is what makes a claim after a closure executable at all, and it is the ONLY
	// route there: a kind installing SERVING over a closure would reintroduce the bootstrap
	// gap on the second attempt — after an abandoned first creation, which is precisely when
	// it is reached.
	mqttTransitionReserveCreation mqttTransitionKind = "reserve-creation"
	// mqttTransitionFinalizeCreation turns a reservation into a serving authority, after
	// its metadata half has committed. It may install only what the reservation proposed.
	mqttTransitionFinalizeCreation mqttTransitionKind = "finalize-creation"
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
	// delivery_abandoned and its exchange Publish -> DeliveryAbandoned, in one batch (F-105 row
	// 5, consult 34). Unlike an ending it is a DECISION made under a connection's entitlement,
	// so it requires the SERVING authority at exactly that generation and tenure; like an
	// ending it moves no ownership and admits nothing.
	mqttTransitionAbandonDelivery mqttTransitionKind = "abandon-delivery"
	// mqttTransitionTombstoneRegister records that the serving connection CANCELLED one ordinary
	// allocation without whole-exchange evidence (F-105 7b): the register becomes the cancellation
	// tombstone — an overwrite of its untokened publish occupant with Cancelled set and nothing
	// else, or, for an allocation whose own store is still in flight, a create-only tombstone —
	// and the exchange, if any, is left where it is. Like abandon-delivery it is a DECISION under
	// one connection's entitlement: it requires the SERVING authority at exactly that generation
	// and tenure, so a displaced connection's cancellation is refused, and it moves no ownership.
	mqttTransitionTombstoneRegister mqttTransitionKind = "tombstone-register"
	// mqttTransitionRebindCanonical re-binds an ordinary `pubrel_ready` slot whose canonical
	// PUBREL record vanished for a reason other than PUBCOMP to a freshly published, proven
	// record (F-105 7b, continuation 3): the register moves `pubrel_ready(old tuple) ->
	// pubrel_ready(new tuple)` with nothing else changed, and its exchange must stand at
	// PubRecObserved. The slot is never re-phased, so a PUBCOMP completes it at any moment by
	// today's arm. Choosing to re-emit is the serving connection's decision, so it requires the
	// SERVING authority at exactly the attempt's generation and tenure and moves no ownership.
	mqttTransitionRebindCanonical mqttTransitionKind = "rebind-canonical"
)

// mqttValidTransitionKind reports whether k is one this broker writes.
func mqttValidTransitionKind(k mqttTransitionKind) bool {
	switch k {
	case mqttTransitionPrepareIntent, mqttTransitionFinalize,
		mqttTransitionCloseAdmit, mqttTransitionCloseTeardown,
		mqttTransitionAdmitExchange, mqttTransitionAcquireAction,
		mqttTransitionCancelIntent, mqttTransitionDisposeResource, mqttTransitionProvisionResource,
		mqttTransitionReserveCreation, mqttTransitionFinalizeCreation,
		mqttTransitionAdvanceExchange, mqttTransitionMaterializePayload,
		mqttTransitionWithdrawUnsent, mqttTransitionAbandonDelivery, mqttTransitionTombstoneRegister,
		mqttTransitionRebindCanonical:
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
	if !mqttF105IdentTk(a.Epoch) {
		return fmt.Errorf("transition attempt carries a malformed recovery epoch %q", a.Epoch)
	}
	if !mqttF105IdentTk(a.Txn) {
		return fmt.Errorf("transition attempt carries a malformed transition id %q", a.Txn)
	}
	if !mqttF105IdentTk(a.Op) {
		return fmt.Errorf("transition attempt carries a malformed operation id %q", a.Op)
	}
	if !mqttValidTransitionKind(a.Kind) {
		return fmt.Errorf("transition attempt carries an unsupported kind %q", a.Kind)
	}
	if a.PrevTxn != _EMPTY_ && !mqttF105IdentTk(a.PrevTxn) {
		return fmt.Errorf("transition attempt names a malformed predecessor transition %q", a.PrevTxn)
	}
	if a.Rev == 0 {
		return fmt.Errorf("transition attempt proposes no authority revision")
	}
	if !mqttValidSessGen(a.Gen) {
		return fmt.Errorf("transition attempt proposes a malformed generation %q", a.Gen)
	}
	if !mqttF105IdentTk(a.Tenure) {
		return fmt.Errorf("transition attempt proposes a malformed tenure %q", a.Tenure)
	}
	if !mqttF105IdentTk(a.OwnerTxn) {
		return fmt.Errorf("transition attempt proposes a malformed ownership transition %q", a.OwnerTxn)
	}
	// A first admission is the only attempt with no predecessor, and it is exactly the
	// attempt that installs revision 1. Any later revision displaces something, and an
	// attempt that cannot name what it displaces could finalize against whatever it found.
	if (a.PrevTxn == _EMPTY_) != (a.Rev == 1) {
		return fmt.Errorf("transition attempt at revision %d names predecessor %q: only a first admission has none",
			a.Rev, a.PrevTxn)
	}
	// The intent is required by exactly the two kinds that resolve one, and refused by the
	// rest: an attempt that carried one where it means nothing would have a field no reader
	// checks, which is how a stale value survives to be believed later.
	needsIntent := a.Kind == mqttTransitionFinalize || a.Kind == mqttTransitionCancelIntent ||
		a.Kind == mqttTransitionFinalizeCreation
	if needsIntent && !mqttF105IdentTk(a.Intent) {
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
// be read is refused by its reader rather than classified here, for the reason the
// retirement watermark refuses a corrupt read — a record you cannot interpret may name a
// tenure newer than yours.
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
	// mqttAuthorityPreparing names a handoff intent that has reserved the transition and
	// TEMPORARILY frozen new admission. It is not closure: obligations already admitted
	// continue, and their completion is still permitted.
	mqttAuthorityPreparing mqttAuthorityState = 2
	// mqttAuthorityClosed records that Gen is permanently closed to new admission. The
	// permanent closure record is the authority for that; this state is the same fact in
	// the place contention happens.
	mqttAuthorityClosed mqttAuthorityState = 3
	// mqttAuthorityReserving is a FIRST CREATION that has reserved its arbitration and is
	// not yet serving.
	//
	// It exists because virgin creation had no representation at all: a first admission
	// installs SERVING with no predecessor, while a prepared handoff requires a full
	// one — predecessor generation, tenure, ownership transition, session write identity
	// and a nonzero session sequence — so there was no way to express "this client has no
	// prior ownership and I am claiming it" without either serving before the metadata
	// committed or MANUFACTURING a predecessor from session metadata. The second is
	// forbidden: missing metadata is not evidence that a creation is first, and inventing
	// a predecessor would let a reader believe ownership was displaced that never existed.
	//
	// A reservation grants NO serving permission. It is create-only, so exactly one
	// claimant can hold it, and it is finalized into a serving authority only after the
	// metadata half commits. An abandoned reservation is CLOSED rather than deleted — its
	// generation never served and never will — and a later attempt reserves a fresh
	// generation beside it, through reserve-creation again.
	mqttAuthorityReserving mqttAuthorityState = 4
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

	// Intent is the handoff intent this authority has reserved, present exactly in the
	// preparing state and absent otherwise.
	Intent *mqttHandoffIntent `json:"intent,omitempty"`

	// Creation is the first-creation proposal, present exactly in the reserving state and
	// absent otherwise.
	Creation *mqttCreationIntent `json:"creation,omitempty"`
}

// mqttHandoffIntent is the immutable description of a proposed ownership move.
//
// It names its PREDECESSOR explicitly, and that is what makes a delayed acceptance lose:
// finalization requires both this intent's id and the ownership predecessor it named, so a
// CAS retry may refresh the storage revision it contends against but can never retarget a
// successor that has since been admitted. Another acceptance requires another CONNECT
// transaction, not the revival of this one.
type mqttHandoffIntent struct {
	ID string `json:"id"`

	// The named predecessor, in full. PrevSessSeq is the session record's sequence at the
	// time the predecessor was observed; it is part of the predecessor's identity and is
	// NOT a tenure identity in its own right, since every confirmed save advances it.
	PrevGen      string `json:"prev_gen"`
	PrevTenure   string `json:"prev_tenure"`
	PrevOwnerTxn string `json:"prev_owner_txn"`
	PrevSessWID  string `json:"prev_sess_wid"`
	PrevSessSeq  uint64 `json:"prev_sess_seq"`

	// What the intent proposes.
	Tenure   string `json:"tenure"`
	Gen      string `json:"gen"`
	CleanReq bool   `json:"clean_req"`

	// Retire, when present, is the retirement this handoff also requests.
	Retire *mqttRetirementRequest `json:"retire,omitempty"`
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
	// Discard records that explicit session discard was requested, as Clean Start does.
	// It is a request, re-evaluated against the current authority by the CONNECT
	// transaction; it is never itself authority to discard.
	Discard bool `json:"discard"`
	// The predecessor's AFFINE target, carried so the closure that ends its generation can
	// enrol it AFTER the accepted batch has displaced the record that named it (F-105).
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

// mqttCreationIntent is the immutable proposal of a FIRST creation.
//
// A separate type from mqttHandoffIntent on purpose. That type requires a full predecessor,
// and that requirement is load-bearing — it is what stops a delayed acceptance retargeting a
// successor — so weakening it to accommodate creation would remove the guarantee everywhere
// to serve one case. A creation names no predecessor because there is none, and says so by
// being a different shape rather than by leaving fields empty.
type mqttCreationIntent struct {
	ID     string `json:"id"`
	Gen    string `json:"gen"`
	Tenure string `json:"tenure"`
}

// mqttValidateCreationIntent refuses a creation proposal this broker cannot act on.
func mqttValidateCreationIntent(i *mqttCreationIntent) error {
	if i == nil {
		return fmt.Errorf("creation intent is absent")
	}
	if !mqttF105IdentTk(i.ID) {
		return fmt.Errorf("creation intent carries a malformed id %q", i.ID)
	}
	if !mqttValidSessGen(i.Gen) {
		return fmt.Errorf("creation intent proposes a malformed generation %q", i.Gen)
	}
	if !mqttF105IdentTk(i.Tenure) {
		return fmt.Errorf("creation intent proposes a malformed tenure %q", i.Tenure)
	}
	return nil
}

// mqttRetirementReason is the closed set of reasons a handoff may request retirement.
//
// Closed because the reason is not decoration: it decides which CONNECT mode and which
// discard disposition are legal, and a free string would let a request assert one thing in
// its reason and another in its flags.
type mqttRetirementReason string

const (
	// mqttRetireCleanStart is an explicit client-requested discard. It is the only reason
	// that may carry Discard, and it necessarily replaces the generation.
	mqttRetireCleanStart mqttRetirementReason = "clean-start"
	// mqttRetireExpiry is the session-expiry pass retiring an elapsed session. It never
	// carries Discard: expiry ends the session's entitlement, and what happens to its
	// outstanding obligations is decided by the retirement protocol, not asserted here.
	mqttRetireExpiry mqttRetirementReason = "expiry"
	// mqttRetireTakeover is a successor acceptance displacing a predecessor that keeps its
	// session. It preserves the generation, so it never carries Discard either.
	mqttRetireTakeover mqttRetirementReason = "takeover"
	// mqttRetireSessionEnd is the OWNING broker discarding its own session record because
	// the session's retention ended: a clean-session close, a zero session-expiry
	// disconnect, or an elapsed expiry that removes the record. Unlike Clean Start it is
	// not driven by a displacing CONNECT and carries no handoff — it is a standalone
	// teardown — and unlike a plain expiry/takeover it DOES discard, which is the fact the
	// durable teardown receipt authorizes and the discarding closure records. It exists
	// because Clean Start was the only reason that could discard, and a session that ends
	// with no successor still has to close its own authority (F-105, row 8, Gap 1).
	mqttRetireSessionEnd mqttRetirementReason = "session-end"
)

// mqttValidRetirementReason reports whether r is one this broker writes.
func mqttValidRetirementReason(r mqttRetirementReason) bool {
	switch r {
	case mqttRetireCleanStart, mqttRetireExpiry, mqttRetireTakeover, mqttRetireSessionEnd:
		return true
	}
	return false
}

// mqttGenClosure is the permanent record that one generation is closed to new admission.
type mqttGenClosure struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	// Txn names the transition that closed it, so the closure and the outcome that
	// produced it can be reconciled without either being inferred from the other.
	Txn string `json:"txn"`
	// Discard records whether the closure carried an authorized session discard, which is
	// what distinguishes "this generation ended" from "this generation's obligations were
	// authorized to be abandoned". They are different facts and only one of them licenses
	// disposal.
	Discard bool `json:"discard"`
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
	"txn": {}, "owner_txn": {}, "intent": {}, "creation": {},
}

var mqttAuthorityRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "tenure": {}, "state": {}, "rev": {},
	"txn": {}, "owner_txn": {},
}

var mqttIntentMembers = map[string]struct{}{
	"id": {}, "prev_gen": {}, "prev_tenure": {}, "prev_owner_txn": {}, "prev_sess_wid": {},
	"prev_sess_seq": {}, "tenure": {}, "gen": {}, "clean_req": {}, "retire": {},
}

var mqttIntentRequired = map[string]struct{}{
	"id": {}, "prev_gen": {}, "prev_tenure": {}, "prev_owner_txn": {}, "prev_sess_wid": {},
	"prev_sess_seq": {}, "tenure": {}, "gen": {}, "clean_req": {},
}

// A RETIREMENT REQUEST NAMES ALL FIVE COMPONENTS OF ITS PREDECESSOR, session identity
// included. The set was left at three ownership members when sess_wid and sess_seq were
// added, which made every preparing authority carrying a retirement request WRITABLE and
// UNREADABLE: the record landed, and its own decoder then refused it as corrupt. Nothing
// caught it because no test loaded such an authority back — the closed schema is only a
// schema if the set and the struct are changed together.
var mqttRetireMembers = map[string]struct{}{
	"gen": {}, "tenure": {}, "owner_txn": {}, "sess_wid": {}, "sess_seq": {},
	"reason": {}, "discard": {},
	"affine_durable": {}, "affine_gen": {}, "pubrel_durable": {},
}

// mqttRetireRequired is what a retirement request must SAY, which is not the same set as what it
// may carry. The durable targets are optional: a retirement that transfers nothing names none.
var mqttRetireRequired = map[string]struct{}{
	"gen": {}, "tenure": {}, "owner_txn": {}, "sess_wid": {}, "sess_seq": {},
	"reason": {}, "discard": {},
}

var mqttClosureMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "txn": {}, "discard": {},
	"root_shard": {}, "root_ord": {},
}

// The outcome's legal member set DEPENDS ON ITS VERDICT. An aborted outcome carrying
// ownership members — even spelled as explicit zeroes — is contradictory rather than merely
// over-specified, so the two sets are separate and the verdict is read first.
var mqttOutcomeCommonMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "txn": {}, "op": {}, "kind": {}, "verdict": {}, "prev_txn": {}, "intent": {},
}

var mqttOutcomeCommittedMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "txn": {}, "op": {}, "kind": {}, "verdict": {}, "prev_txn": {}, "intent": {},
	"rev": {}, "gen": {}, "tenure": {}, "owner_txn": {}, "digest": {},
}

var mqttOutcomeCommonRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "txn": {}, "op": {}, "kind": {}, "verdict": {},
}

var mqttOutcomeCommittedRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "txn": {}, "op": {}, "kind": {}, "verdict": {},
	"rev": {}, "gen": {}, "tenure": {}, "owner_txn": {}, "digest": {},
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
	mqttAuthorityOptional = map[string]struct{}{"intent": {}, "creation": {}}
	mqttIntentOptional    = map[string]struct{}{"retire": {}}
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
	raw := struct {
		State  mqttAuthorityState `json:"state"`
		Intent json.RawMessage    `json:"intent"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if raw.State == mqttAuthorityPreparing {
		if len(raw.Intent) == 0 {
			return nil, mqttShapeErr("a preparing outbound authority carries no handoff intent")
		}
		if err := mqttJSONMembersAreExactAndUnique(raw.Intent, mqttIntentMembers, "a handoff intent"); err != nil {
			return nil, err
		}
		if err := mqttJSONMembersArePresent(raw.Intent, mqttIntentRequired, "a handoff intent"); err != nil {
			return nil, err
		}
		if err := mqttJSONOptionalMembersNotNull(raw.Intent, mqttIntentOptional, "a handoff intent"); err != nil {
			return nil, err
		}
		rr := struct {
			Retire json.RawMessage `json:"retire"`
		}{}
		if err := json.Unmarshal(raw.Intent, &rr); err != nil {
			return nil, err
		}
		if len(rr.Retire) != 0 {
			if err := mqttJSONMembersAreExactAndUnique(rr.Retire, mqttRetireMembers, "a retirement request"); err != nil {
				return nil, err
			}
			if err := mqttJSONMembersArePresent(rr.Retire, mqttRetireRequired, "a retirement request"); err != nil {
				return nil, err
			}
		}
	} else if len(raw.Intent) != 0 {
		return nil, mqttShapeErr("a non-preparing outbound authority carries a handoff intent")
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
	}{}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, err
	}
	var members, required map[string]struct{}
	switch probe.Verdict {
	case mqttOutcomeCommitted:
		members, required = mqttOutcomeCommittedMembers, mqttOutcomeCommittedRequired
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
	if !mqttF105IdentTk(a.Tenure) {
		return fmt.Errorf("outbound authority carries a malformed tenure %q", a.Tenure)
	}
	if !mqttF105IdentTk(a.Txn) {
		return fmt.Errorf("outbound authority carries a malformed transition id %q", a.Txn)
	}
	if !mqttF105IdentTk(a.OwnerTxn) {
		return fmt.Errorf("outbound authority carries a malformed ownership transition id %q", a.OwnerTxn)
	}
	if a.Rev == 0 {
		return fmt.Errorf("outbound authority carries no revision")
	}
	// The two intent shapes are present in exactly one state each, and nowhere else. A
	// record carrying both, or carrying one in a state that does not use it, has two places
	// to say what it is doing.
	if a.Creation != nil && a.State != mqttAuthorityReserving {
		return fmt.Errorf("outbound authority in state %d carries a creation intent", a.State)
	}
	switch a.State {
	case mqttAuthorityServing, mqttAuthorityClosed:
		if a.Intent != nil {
			return fmt.Errorf("outbound authority in state %d carries a handoff intent", a.State)
		}
	case mqttAuthorityReserving:
		if a.Intent != nil {
			return fmt.Errorf("a reserving outbound authority carries a handoff intent")
		}
		if err := mqttValidateCreationIntent(a.Creation); err != nil {
			return fmt.Errorf("reserving outbound authority: %w", err)
		}
		// THE RESERVATION DESCRIBES WHAT IT PROPOSES, not something else. A reservation
		// whose body named a different generation or tenure from its own proposal would
		// make the finalization's binding meaningless.
		if a.Gen != a.Creation.Gen || a.Tenure != a.Creation.Tenure {
			return fmt.Errorf("a reserving authority carries %q/%q but proposes %q/%q",
				a.Gen, a.Tenure, a.Creation.Gen, a.Creation.Tenure)
		}
	case mqttAuthorityPreparing:
		if err := mqttValidateHandoffIntentFor(getHash(a.ID), a.Intent); err != nil {
			return fmt.Errorf("preparing outbound authority: %w", err)
		}
		// THE INTENT MUST NAME THIS AUTHORITY'S OWN OWNERSHIP as its predecessor. An
		// intent naming some other predecessor is not a weaker intent: finalizing it would
		// displace ownership nobody checked, which is the retarget this structure exists
		// to prevent.
		if a.Intent.PrevGen != a.Gen || a.Intent.PrevTenure != a.Tenure || a.Intent.PrevOwnerTxn != a.OwnerTxn {
			return fmt.Errorf("handoff intent names predecessor %q/%q/%q but its authority carries %q/%q/%q",
				a.Intent.PrevGen, a.Intent.PrevTenure, a.Intent.PrevOwnerTxn, a.Gen, a.Tenure, a.OwnerTxn)
		}
	default:
		return fmt.Errorf("outbound authority carries an unsupported state %d", a.State)
	}
	return nil
}

// mqttValidateHandoffIntent refuses an intent that cannot name what it proposes to
// displace, or whose retirement names a different predecessor from the intent itself.
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

// mqttValidateHandoffIntentFor is mqttValidateHandoffIntent plus the AFFINE DESCRIPTOR, for the
// callers that are about to PERSIST the intent and therefore know whose it is.
//
// The plain form is also used as a VIEW — mqttProposalFromHandoff reads an intent as a proposal —
// and a view has no client identity to derive names from and is not the gate that must refuse an
// unfinishable shape. The persistence boundaries are: the authority record's own validation, and
// opPrepareHandoff before the record is written.
func mqttValidateHandoffIntentFor(idHash string, i *mqttHandoffIntent) error {
	if err := mqttValidateHandoffIntent(i); err != nil {
		return err
	}
	if i == nil || i.Retire == nil {
		return nil
	}
	r := i.Retire
	if derr := mqttValidateAffineDescriptor(idHash, r.Gen, r.AffineGen, r.AffineDurable,
		r.PubRelDurable); derr != nil {
		return fmt.Errorf("retirement request of generation %q: %w", r.Gen, derr)
	}
	return nil
}

func mqttValidateHandoffIntent(i *mqttHandoffIntent) error {
	if i == nil {
		return fmt.Errorf("handoff intent is absent")
	}
	if !mqttF105IdentTk(i.ID) {
		return fmt.Errorf("handoff intent carries a malformed id %q", i.ID)
	}
	if !mqttValidSessGen(i.PrevGen) {
		return fmt.Errorf("handoff intent names a malformed predecessor generation %q", i.PrevGen)
	}
	if !mqttF105IdentTk(i.PrevTenure) {
		return fmt.Errorf("handoff intent names a malformed predecessor tenure %q", i.PrevTenure)
	}
	if !mqttF105IdentTk(i.PrevOwnerTxn) {
		return fmt.Errorf("handoff intent names a malformed predecessor ownership transition %q", i.PrevOwnerTxn)
	}
	if i.PrevSessWID == _EMPTY_ {
		return fmt.Errorf("handoff intent names no predecessor session write identity")
	}
	// A predecessor that exists has a sequence. Zero is the shape of a record never
	// written, so an intent claiming to displace one cannot carry it.
	if i.PrevSessSeq == 0 {
		return fmt.Errorf("handoff intent names a predecessor session record at sequence zero")
	}
	if !mqttValidSessGen(i.Gen) {
		return fmt.Errorf("handoff intent proposes a malformed generation %q", i.Gen)
	}
	if !mqttF105IdentTk(i.Tenure) {
		return fmt.Errorf("handoff intent proposes a malformed tenure %q", i.Tenure)
	}
	// A clean start DISCARDS the session, so it necessarily proposes a fresh generation; a
	// resume necessarily preserves it. Either pairing reversed is a body whose two halves
	// disagree about what it is asking for.
	if i.CleanReq == (i.Gen == i.PrevGen) {
		return fmt.Errorf("handoff intent requests clean=%t while proposing generation %q against predecessor %q",
			i.CleanReq, i.Gen, i.PrevGen)
	}
	// A CLEAN START DISCARDS THE SESSION, so it must actually retire the predecessor it is
	// displacing. Without this a clean start could replace the generation while leaving the
	// old one admitted, which is the state whose obligations nothing would ever resolve.
	if i.CleanReq && i.Retire == nil {
		return fmt.Errorf("handoff intent clean-starts %q but requests no retirement of it", i.PrevGen)
	}
	if i.Retire != nil {
		r := i.Retire
		// THE FULL PREDECESSOR, all five components. Ownership alone would be satisfied by
		// a predecessor whose session-record half had since been replaced.
		if r.Gen != i.PrevGen || r.Tenure != i.PrevTenure || r.OwnerTxn != i.PrevOwnerTxn ||
			r.SessWID != i.PrevSessWID || r.SessSeq != i.PrevSessSeq {
			return fmt.Errorf("retirement request names %q/%q/%q/%q/%d but its intent's predecessor is %q/%q/%q/%q/%d",
				r.Gen, r.Tenure, r.OwnerTxn, r.SessWID, r.SessSeq,
				i.PrevGen, i.PrevTenure, i.PrevOwnerTxn, i.PrevSessWID, i.PrevSessSeq)
		}
		if !mqttValidRetirementReason(r.Reason) {
			return fmt.Errorf("retirement request carries an unsupported reason %q", r.Reason)
		}
		// The reason, the CONNECT mode and the discard disposition are three statements
		// about one request, and a body where they disagree describes no supported
		// operation. A clean start is exactly the discarding case.
		if (r.Reason == mqttRetireCleanStart) != i.CleanReq {
			return fmt.Errorf("retirement request reasons %q against a clean=%t intent", r.Reason, i.CleanReq)
		}
		if r.Discard != (r.Reason == mqttRetireCleanStart) {
			return fmt.Errorf("retirement request reasons %q with discard=%t", r.Reason, r.Discard)
		}
		// The AFFINE DESCRIPTOR is checked by mqttValidateHandoffIntentFor, which the two
		// PERSISTENCE boundaries call — it needs the client identity the names derive from, and
		// this function is also used as a view where there is none.
	}
	return nil
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
	if !mqttF105IdentTk(c.Txn) {
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
	if !mqttF105IdentTk(o.Op) {
		return fmt.Errorf("transition outcome carries a malformed operation id %q", o.Op)
	}
	if !mqttValidTransitionKind(o.Kind) {
		return fmt.Errorf("transition outcome carries an unsupported kind %q", o.Kind)
	}
	if o.PrevTxn != _EMPTY_ && !mqttF105IdentTk(o.PrevTxn) {
		return fmt.Errorf("transition outcome names a malformed predecessor transition %q", o.PrevTxn)
	}
	// The same three kinds that must NAME the proposal they answer must record it, so the
	// receipt says which one it was for. Keep this set and the attempt's in step: a kind
	// required to name an intent whose receipt may not carry it would be unresolvable.
	resolvesHandoff := o.Kind == mqttTransitionFinalize || o.Kind == mqttTransitionCancelIntent ||
		o.Kind == mqttTransitionFinalizeCreation
	if resolvesHandoff && !mqttF105IdentTk(o.Intent) {
		return fmt.Errorf("a %s outcome names no handoff intent", o.Kind)
	}
	if !resolvesHandoff && o.Intent != _EMPTY_ {
		return fmt.Errorf("a %s outcome names handoff intent %q, which it does not resolve", o.Kind, o.Intent)
	}
	switch o.Verdict {
	case mqttOutcomeCommitted:
		if o.Rev == 0 {
			return fmt.Errorf("committed outcome carries no authority revision")
		}
		if !mqttValidSessGen(o.Gen) {
			return fmt.Errorf("committed outcome carries a malformed generation %q", o.Gen)
		}
		if !mqttF105IdentTk(o.Tenure) {
			return fmt.Errorf("committed outcome carries a malformed tenure %q", o.Tenure)
		}
		if !mqttF105IdentTk(o.OwnerTxn) {
			return fmt.Errorf("committed outcome carries a malformed ownership transition %q", o.OwnerTxn)
		}
		if o.Digest == _EMPTY_ {
			return fmt.Errorf("committed outcome carries no mutation digest")
		}
		if (o.PrevTxn == _EMPTY_) != (o.Rev == 1) {
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
	writeField(a.ID, a.Epoch, a.Txn, a.Op, string(a.Kind), a.PrevTxn, a.Gen, a.Tenure, a.OwnerTxn, a.Intent)
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
// resolveTransition answers it from the assembled transition, which a caller holding its
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

// mqttSameHandoffIntent and mqttSameCreationIntent report whether two pending proposals are
// the same one.
//
// Compared by VALUE rather than by pointer: a transition rebuilds the authority body, so the
// proposal it carries is a different object even when it is the same proposal, and a pointer
// comparison would report every faithful copy as a rewrite.
func mqttSameHandoffIntent(a, b *mqttHandoffIntent) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Retire == nil || b.Retire == nil {
		if a.Retire != nil || b.Retire != nil {
			return false
		}
		return *a == *b || sameIntentBody(a, b)
	}
	return sameIntentBody(a, b) && sameRetirement(a.Retire, b.Retire)
}

// sameRetirement compares two retirement requests member by member.
//
// Written out rather than compared with ==, because the request now carries a SLICE of durable
// names and Go refuses to compare a struct containing one. Spelling it out is also what keeps this
// honest: a member added later and forgotten here would silently make two different retirements
// compare equal, and a preserved-intent check that cannot see a difference is not a check.
func sameRetirement(a, b *mqttRetirementRequest) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Gen != b.Gen || a.Tenure != b.Tenure || a.OwnerTxn != b.OwnerTxn ||
		a.SessWID != b.SessWID || a.SessSeq != b.SessSeq || a.Reason != b.Reason ||
		a.Discard != b.Discard || a.AffineDurable != b.AffineDurable ||
		a.AffineGen != b.AffineGen || a.PubRelDurable != b.PubRelDurable {
		return false
	}
	return true
}

func sameIntentBody(a, b *mqttHandoffIntent) bool {
	return a.ID == b.ID && a.PrevGen == b.PrevGen && a.PrevTenure == b.PrevTenure &&
		a.PrevOwnerTxn == b.PrevOwnerTxn && a.PrevSessWID == b.PrevSessWID &&
		a.PrevSessSeq == b.PrevSessSeq && a.Tenure == b.Tenure && a.Gen == b.Gen &&
		a.CleanReq == b.CleanReq
}

func mqttSameCreationIntent(a, b *mqttCreationIntent) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
