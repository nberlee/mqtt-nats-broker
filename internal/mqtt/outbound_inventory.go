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
	"strconv"
	"strings"
)

// The generation ledger and its duty inventory (F-105, rows 6 and 8).
//
// # Why a walk filter is not a discovery mechanism
//
// Recovery enumerates REGISTERS today and then scans PUBREL records. Three things that cannot do:
//
//  1. It finds RECORDS, not DUTIES. A provisioning operation or a cleanup duty has no record of
//     its own until it materializes, and a duty that becomes discoverable only once it has done
//     its work is not discoverable at all.
//  2. It has no stable order, so a cursor over it must be a JetStream sequence — and a sequence
//     MOVES when a record is updated. An entry updated behind a cursor that already passed its
//     old sequence is lost. The ordinal here is a SUBJECT token for exactly that reason: the
//     record's sequence moves and its name does not.
//  3. Closure must SEAL A BOUND, and a subject-space enumeration has no bound to seal.
//
// # The seal is a FENCE, not a scan
//
// Closing a generation rewrites no exchange. The only operation closure must forbid is ordinal
// ALLOCATION, and allocation is exactly and only the operation that CASes the ledger — so a
// transition that would allocate finds the ledger SEALED and is refused, and a transition that
// only completes existing work never reads it.
//
// That is what makes the four permissions after closure fall out of one fact rather than four
// checks: no new obligation enters the generation; existing completion may still update edges
// and counters; an already-owed resource may be materialized through its PREDECLARED entry; and
// no new serving authority is needed to complete an existing named dependency.
//
// # Every duty is predeclared, and that is a contract rather than a consequence
//
//	Every duty a generation will ever owe is allocated when the thing it concerns is admitted.
//	After closure the inventory's SHAPE is fixed; only the states of its entries move.
//
// The example that forces it: after closure the last dependency on a resource disappears and the
// resource CASes LIVE -> DELETE_AUTHORIZED. That act must ACTIVATE an existing cleanup entry,
// because allocating one against a sealed ledger would fail — which is why a resource's
// admission predeclares its cleanup duty along with its provisioning one.
//
// # An entry is never individually deleted
//
// A resolved duty is updated to its classified terminal result and LEFT STANDING; entries go
// only when the generation's whole root retires. That is what makes "a missing or malformed
// entry is an inconsistency, not completed work" coherent — absence is never legitimate, so it
// is never ambiguous between "done" and "lost". The storage cost is explicit and accepted, the
// same cost the outcome and closure records already carry.

// The record classes this file owns. Both fail mqttValidIDHashToken and mqttValidSessGen the
// same way every other F-105 class does, so neither can ever parse as a register.
const (
	mqttLedgerClassTk = "led"  // one generation's lifecycle state and inventory high-water
	mqttDutyClassTk   = "duty" // one predeclared duty, at a stable ordinal
)

// mqttOrdinalWidth is the fixed width of an ordinal subject token.
//
// FIXED-WIDTH ZERO-PADDED DECIMAL, so lexical subject order IS ordinal order and a cursor can be
// a plain integer. 20 digits because that is what a uint64 needs at its maximum; a narrower
// field would order correctly only until it overflowed, which is the kind of bound that holds
// right up until the day it does not.
const mqttOrdinalWidth = 20

// mqttFormatOrdinal renders an ordinal as its subject token.
func mqttFormatOrdinal(ord uint64) string {
	return fmt.Sprintf("%0*d", mqttOrdinalWidth, ord)
}

// mqttParseOrdinal reads an ordinal subject token back.
//
// EXACT WIDTH AND EXACT ROUND TRIP. A token that is short, long, or carries a leading form this
// broker would not have written is refused rather than coerced: the ordering guarantee is a
// property of the encoding, and a token outside it is one this broker cannot place.
func mqttParseOrdinal(tk string) (uint64, error) {
	if len(tk) != mqttOrdinalWidth {
		return 0, fmt.Errorf("ordinal token %q is %d characters wide (want %d)",
			tk, len(tk), mqttOrdinalWidth)
	}
	ord, err := strconv.ParseUint(tk, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed ordinal token %q: %w", tk, err)
	}
	if mqttFormatOrdinal(ord) != tk {
		return 0, fmt.Errorf("ordinal token %q does not round trip", tk)
	}
	if ord == 0 {
		// ORDINALS START AT 1. Zero is the unset value of every uint64 that carries one, so
		// admitting it as a real ordinal would make "no ordinal" and "the first ordinal"
		// indistinguishable in every struct that holds one.
		return 0, fmt.Errorf("ordinal token %q names ordinal zero, and ordinals start at 1", tk)
	}
	return ord, nil
}

// mqttGenLedgerSubject returns one generation's ledger subject.
func mqttGenLedgerSubject(idHash, gen string) string {
	return mqttF105SubjectPrefix() + mqttLedgerClassTk + "." + idHash + "." + gen
}

// mqttDutySubject returns one duty entry's subject.
func mqttDutySubject(idHash, gen string, ord uint64) string {
	return mqttF105SubjectPrefix() + mqttDutyClassTk + "." + idHash + "." + gen + "." +
		mqttFormatOrdinal(ord)
}

// mqttDutyWalkFilter enumerates one generation's duty entries, and only those.
func mqttDutyWalkFilter(idHash, gen string) string {
	return mqttF105SubjectPrefix() + mqttDutyClassTk + "." + idHash + "." + gen + ".*"
}

// mqttIsLedgerSubject and mqttIsDutySubject are what the transition writer's member classifier
// uses. The ledger is CALLER-CARRIED — an admission's batch includes its CAS — so it is an
// obligation-class member and not an arbitration record.
func mqttIsLedgerSubject(subject string) bool {
	return strings.HasPrefix(subject, mqttF105SubjectPrefix()+mqttLedgerClassTk+".")
}

func mqttIsDutySubject(subject string) bool {
	return strings.HasPrefix(subject, mqttF105SubjectPrefix()+mqttDutyClassTk+".")
}

// ---------------------------------------------------------------------------
// The generation ledger
// ---------------------------------------------------------------------------

// mqttGenLedgerState is whether a generation still admits new duties.
type mqttGenLedgerState uint8

const (
	// mqttGenLedgerOpen: ordinals may still be allocated.
	mqttGenLedgerOpen mqttGenLedgerState = 1
	// mqttGenLedgerSealed: the inventory's shape is fixed. Only the states of existing entries
	// move from here, and SealedHi is the bound a retirement must cover.
	mqttGenLedgerSealed mqttGenLedgerState = 2
)

func mqttValidGenLedgerState(s mqttGenLedgerState) bool {
	return s == mqttGenLedgerOpen || s == mqttGenLedgerSealed
}

// mqttGenLedger is one generation's inventory high-water and lifecycle state.
type mqttGenLedger struct {
	ID    string             `json:"id"`
	Epoch string             `json:"epoch"`
	Gen   string             `json:"gen"`
	State mqttGenLedgerState `json:"state"`
	// Hi is the highest ordinal allocated in this generation. 0 means none yet.
	//
	// Allocation CASes this record, which is what makes ordinals CONTIGUOUS: a batch that loses
	// the CAS lands nothing, so no ordinal is consumed by a refused transition and there is no
	// hole for a cursor to mistake for the end.
	Hi uint64 `json:"hi"`
	// Lo is the FLOOR (F-112): every position below it is RETIRED — reclaimed, or relocated to a
	// representative at or above it — so an entry below Lo may be absent and its absence is
	// legitimate by the floor's own proof. Every position in [Lo, Hi] must exist and decode, or
	// it is an inconsistency, exactly as before the floor existed. 1 <= Lo <= Hi+1; Lo == Hi+1
	// is an inventory with nothing outstanding.
	Lo uint64 `json:"lo"`
	// Del is the physical-deletion watermark (F-112): every position below Del is DELETED, its
	// duty entry and, for a reclaimed exchange duty, its exchange record. 1 <= Del <= Lo. The
	// positions in [Del, Lo) are retired but may still be stored; no reader consults them.
	Del uint64 `json:"del"`
	// SealedHi is the bound at the moment of sealing, present only when SEALED.
	//
	// Kept beside Hi rather than replacing it, because a retirement must compare the bound a
	// SWEEP captured against the bound the generation was actually sealed at — equality is what
	// establishes coverage, and a single field could not express the difference.
	SealedHi uint64 `json:"sealed_hi,omitempty"`
}

var mqttLedgerMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "state": {}, "hi": {}, "lo": {}, "del": {}, "sealed_hi": {},
}

// The floor and the deletion watermark are REQUIRED: a ledger written before the floor existed
// is an unsupported persisted shape and is refused, never read as "floor 1" (no migration).
var mqttLedgerRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "state": {}, "hi": {}, "lo": {}, "del": {},
}

var mqttLedgerOptional = map[string]struct{}{"sealed_hi": {}}

// mqttDecodeGenLedger decodes and fully validates one ledger body.
func mqttDecodeGenLedger(data []byte, wantID, wantEpoch, wantGen string) (*mqttGenLedger, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttLedgerMembers, "a generation ledger"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttLedgerRequired, "a generation ledger"); err != nil {
		return nil, err
	}
	if err := mqttJSONOptionalMembersNotNull(data, mqttLedgerOptional, "a generation ledger"); err != nil {
		return nil, err
	}
	l := &mqttGenLedger{}
	if err := json.Unmarshal(data, l); err != nil {
		return nil, err
	}
	if err := mqttValidateGenLedger(l, wantID, wantEpoch, wantGen); err != nil {
		return nil, err
	}
	return l, nil
}

// mqttValidateGenLedger refuses a ledger body this broker cannot act on.
func mqttValidateGenLedger(l *mqttGenLedger, wantID, wantEpoch, wantGen string) error {
	if l == nil {
		return fmt.Errorf("generation ledger is absent")
	}
	if l.ID != wantID {
		return fmt.Errorf("generation ledger names client %q, not %q", l.ID, wantID)
	}
	if l.Epoch != wantEpoch {
		return fmt.Errorf("generation ledger carries recovery epoch %q, not this account's %q",
			l.Epoch, wantEpoch)
	}
	if l.Gen != wantGen {
		return fmt.Errorf("generation ledger names generation %q on the subject of %q", l.Gen, wantGen)
	}
	if !mqttValidSessGen(l.Gen) {
		return fmt.Errorf("generation ledger carries a malformed generation %q", l.Gen)
	}
	if !mqttValidGenLedgerState(l.State) {
		return fmt.Errorf("generation ledger carries an unsupported state %d", l.State)
	}
	// THE FLOOR SITS INSIDE THE INVENTORY, and deletion never passes the floor. A floor of zero
	// would name ordinal zero, which no allocation takes; one past Hi+1 would claim positions
	// nothing allocated were retired; a watermark past the floor would delete positions whose
	// entries are still the inventory's evidence.
	if l.Lo == 0 || l.Lo > l.Hi+1 {
		return fmt.Errorf("generation ledger carries floor %d against high-water %d: the floor "+
			"lies in 1..Hi+1", l.Lo, l.Hi)
	}
	if l.Del == 0 || l.Del > l.Lo {
		return fmt.Errorf("generation ledger carries deletion watermark %d against floor %d: "+
			"deletion lies in 1..Lo", l.Del, l.Lo)
	}
	switch l.State {
	case mqttGenLedgerOpen:
		if l.SealedHi != 0 {
			return fmt.Errorf("an OPEN generation ledger carries a sealed bound of %d: a bound "+
				"exists only once the inventory's shape is fixed", l.SealedHi)
		}
	case mqttGenLedgerSealed:
		// THE SEAL FREEZES THE BOUND AT THE HIGH-WATER. A sealed bound below Hi would claim
		// coverage of an inventory that already reaches past it, and one above Hi would send
		// every sweep looking for entries nothing ever allocated.
		if l.SealedHi != l.Hi {
			return fmt.Errorf("a SEALED generation ledger carries bound %d against high-water "+
				"%d: sealing fixes the bound AT the high-water", l.SealedHi, l.Hi)
		}
	}
	return nil
}

// mqttLedgerSpan is the number of positions in [Lo, Hi]: the part of the inventory every reader
// must still read. ONE DEFINITION, because ordinals are unsigned and Lo == Hi+1 is a legal,
// fully reclaimed inventory: `Hi - Lo` there underflows into an enormous span.
func mqttLedgerSpan(l *mqttGenLedger) uint64 {
	if l == nil || l.Lo > l.Hi {
		return 0
	}
	return l.Hi - l.Lo + 1
}

// mqttLedgerFloor is the first position a reader must read: the floor of a standing ledger, and 1
// for a generation that has none.
func mqttLedgerFloor(l *mqttGenLedger) uint64 {
	if l == nil {
		return 1
	}
	return l.Lo
}

// ---------------------------------------------------------------------------
// The duty inventory
// ---------------------------------------------------------------------------

// mqttDutyKind is the closed set of things a duty entry stands for.
type mqttDutyKind string

const (
	// mqttDutyExchange is one admitted obligation.
	mqttDutyExchange mqttDutyKind = "exchange"
	// mqttDutyProvision is one resource's materialization — predeclared with the resource,
	// because a provisioning operation that only became discoverable once it had succeeded
	// would be undiscoverable in exactly the case that matters.
	mqttDutyProvision mqttDutyKind = "provision"
	// mqttDutyCleanup is one resource's eventual reclamation. ALSO predeclared with the
	// resource: it becomes actionable when the last dependency goes, which can happen after
	// closure, and allocating an ordinal then would fail against a sealed ledger.
	mqttDutyCleanup mqttDutyKind = "cleanup"
)

func mqttValidDutyKind(k mqttDutyKind) bool {
	switch k {
	case mqttDutyExchange, mqttDutyProvision, mqttDutyCleanup:
		return true
	}
	return false
}

// mqttDutyState is how far one duty has got.
//
// The three are NOT a convenience. Consult 5's requirement is that a predeclared entry stays
// UNRESOLVED while its conditional future work is still possible — "otherwise a cursor can pass
// a 'finished' entry that later becomes owed" — so "not actionable yet" needs a name of its own
// and must never be mistaken for an ending.
type mqttDutyState uint8

const (
	// mqttDutyPending: predeclared and possibly actionable. UNRESOLVED.
	mqttDutyPending mqttDutyState = 1
	// mqttDutyNotYetActionable: predeclared, and its work cannot begin yet — a cleanup whose
	// resource still has dependents. UNRESOLVED, deliberately: it is not finished, and a sweep
	// that counted it as finished would retire a generation still owing the work.
	mqttDutyNotYetActionable mqttDutyState = 2
	// mqttDutyTerminal: the WHOLE duty completed, with its classified result. Irreversible.
	mqttDutyTerminal mqttDutyState = 3
	// mqttDutyRelocated (F-112): this POSITION was retired by a floor advance and the duty it
	// held was carried, unchanged, to the representative at To. It is NOT an ending — the duty
	// lives on at To — and not a duty state anything continues: it is legal only below the
	// ledger's floor, where no reader consults it, until the deletion watermark removes it.
	mqttDutyRelocated mqttDutyState = 4
)

func mqttValidDutyState(s mqttDutyState) bool {
	return s == mqttDutyPending || s == mqttDutyNotYetActionable || s == mqttDutyTerminal ||
		s == mqttDutyRelocated
}

// mqttDutyResolved reports whether a sweep may count this duty as covered.
//
// ONLY terminal. Named as a function rather than compared inline because it is the single
// definition a cursor, a retirement and the boundary all depend on agreeing about — and
// "currently inactive", "claim expired" and "not actionable yet" are each a plausible mistake
// that this makes impossible to write by accident.
func mqttDutyResolved(s mqttDutyState) bool { return s == mqttDutyTerminal }

// mqttDutyEntry is one predeclared duty at a stable ordinal.
type mqttDutyEntry struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	// Ord is this entry's ordinal, and it NEVER changes. It is carried in the body as well as
	// in the subject so a record read by any route can be placed without re-parsing its name.
	Ord  uint64       `json:"ord"`
	Kind mqttDutyKind `json:"kind"`
	// Ref is what the duty concerns: an exchange nonce, or a resource incarnation.
	Ref   string        `json:"ref"`
	State mqttDutyState `json:"state"`
	// Claim and LeaseUntil COORDINATE work and authorize nothing.
	//
	// They exist to stop sixteen goroutines on four brokers doing one duty at once, and that is
	// their whole job. An expired claim proves nothing about what the claimant did; a held claim
	// never authorizes a deletion, which is the resource CAS's business; and a claim never
	// proves completion, which is Outcome's. Every duty execution is therefore written to be
	// SAFE UNDER CONCURRENT EXECUTION, because a paused claimant resuming after its lease
	// expired cannot be excluded — only made rarer.
	Claim      string `json:"claim,omitempty"`
	LeaseUntil int64  `json:"lease_until,omitempty"`
	// Outcome is the whole duty's classified result, present only at TERMINAL.
	Outcome string `json:"outcome,omitempty"`
	// To is a RELOCATION MARKER's representative (F-112): the position the duty was carried
	// to. Present exactly when the state is Relocated, and always above Ord.
	To uint64 `json:"to,omitempty"`
	// From is a REPRESENTATIVE's provenance (F-112): the position a floor advance carried this
	// duty from. Present only on an entry a floor advance created, and always below Ord.
	// Provenance only: no reader decides anything on it but the floor's own write boundary,
	// which binds the copy to the marker it carries.
	From uint64 `json:"from,omitempty"`
}

var mqttDutyMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "ord": {}, "kind": {}, "ref": {}, "state": {},
	"claim": {}, "lease_until": {}, "outcome": {}, "to": {}, "from": {},
}

var mqttDutyRequired = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "ord": {}, "kind": {}, "ref": {}, "state": {},
}

var mqttDutyOptional = map[string]struct{}{
	"claim": {}, "lease_until": {}, "outcome": {}, "to": {}, "from": {},
}

// mqttDecodeDutyEntry decodes and fully validates one duty body.
func mqttDecodeDutyEntry(data []byte, wantID, wantEpoch, wantGen string, wantOrd uint64) (*mqttDutyEntry, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttDutyMembers, "a duty entry"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttDutyRequired, "a duty entry"); err != nil {
		return nil, err
	}
	if err := mqttJSONOptionalMembersNotNull(data, mqttDutyOptional, "a duty entry"); err != nil {
		return nil, err
	}
	d := &mqttDutyEntry{}
	if err := json.Unmarshal(data, d); err != nil {
		return nil, err
	}
	if err := mqttValidateDutyEntry(d, wantID, wantEpoch, wantGen, wantOrd); err != nil {
		return nil, err
	}
	return d, nil
}

// mqttValidateDutyEntry refuses a duty body this broker cannot act on.
func mqttValidateDutyEntry(d *mqttDutyEntry, wantID, wantEpoch, wantGen string, wantOrd uint64) error {
	if d == nil {
		return fmt.Errorf("duty entry is absent")
	}
	if d.ID != wantID {
		return fmt.Errorf("duty entry names client %q, not %q", d.ID, wantID)
	}
	if d.Epoch != wantEpoch {
		return fmt.Errorf("duty entry carries recovery epoch %q, not this account's %q",
			d.Epoch, wantEpoch)
	}
	if d.Gen != wantGen {
		return fmt.Errorf("duty entry names generation %q on the subject of %q", d.Gen, wantGen)
	}
	// THE ORDINAL IN THE BODY IS THE ORDINAL IN THE NAME. An entry whose body disagrees with
	// its subject is one a cursor would place in a position its content does not occupy, which
	// is precisely the loss the ordinal exists to prevent.
	if d.Ord != wantOrd {
		return fmt.Errorf("duty entry carries ordinal %d on the subject of %d", d.Ord, wantOrd)
	}
	if d.Ord == 0 {
		return fmt.Errorf("duty entry carries ordinal zero, and ordinals start at 1")
	}
	if !mqttValidDutyKind(d.Kind) {
		return fmt.Errorf("duty entry carries an unsupported kind %q", d.Kind)
	}
	if !mqttF105IdentTk(d.Ref) {
		return fmt.Errorf("duty entry carries a malformed subject reference %q", d.Ref)
	}
	if !mqttValidDutyState(d.State) {
		return fmt.Errorf("duty entry carries an unsupported state %d", d.State)
	}
	if d.Claim != _EMPTY_ && !mqttF105IdentTk(d.Claim) {
		return fmt.Errorf("duty entry carries a malformed claim %q", d.Claim)
	}
	// A LEASE WITHOUT A CLAIMANT NAMES NOBODY, and a claimant without a lease never expires.
	// Either alone is a record no reader can act on.
	if (d.Claim == _EMPTY_) != (d.LeaseUntil == 0) {
		return fmt.Errorf("duty entry carries claim %q and lease %d: a claim and its lease are "+
			"written together or not at all", d.Claim, d.LeaseUntil)
	}
	if d.LeaseUntil < 0 {
		return fmt.Errorf("duty entry carries a lease deadline of %d", d.LeaseUntil)
	}
	if d.From != 0 && d.From >= d.Ord {
		return fmt.Errorf("duty entry %d was carried from position %d: a floor advance carries "+
			"a duty UP, to a position above Hi", d.Ord, d.From)
	}
	if (d.State == mqttDutyRelocated) != (d.To != 0) {
		return fmt.Errorf("duty entry %d is in state %d with relocation target %d: a relocation "+
			"marker and its target are written together or not at all", d.Ord, d.State, d.To)
	}
	switch d.State {
	case mqttDutyRelocated:
		if d.To <= d.Ord {
			return fmt.Errorf("duty entry %d is relocated to %d: a representative stands above "+
				"the position it was carried from", d.Ord, d.To)
		}
		if d.Outcome != _EMPTY_ || d.Claim != _EMPTY_ || d.From != 0 {
			return fmt.Errorf("relocation marker %d carries an outcome, a claim or a provenance: "+
				"the duty's whole state moved to %d, and the marker names only where", d.Ord, d.To)
		}
	case mqttDutyTerminal:
		// THE OUTCOME IS WHAT MAKES IT TERMINAL. "Calling a result classified is insufficient
		// unless those conditions define terminality" — so an entry claiming an ending with
		// nothing to say about it is refused rather than believed.
		if d.Outcome == _EMPTY_ {
			return fmt.Errorf("duty entry %d is terminal with no classified outcome", d.Ord)
		}
		if d.Claim != _EMPTY_ {
			return fmt.Errorf("duty entry %d is terminal and still claimed by %q: a claim "+
				"coordinates work, so an ended duty holds none", d.Ord, d.Claim)
		}
	default:
		if d.Outcome != _EMPTY_ {
			return fmt.Errorf("duty entry %d is in state %d and records an outcome: an outcome "+
				"is the whole duty's classified result and it has not ended", d.Ord, d.State)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Allocating ordinals inside a batch
// ---------------------------------------------------------------------------

// mqttDutyDeclaration is one duty a batch predeclares.
type mqttDutyDeclaration struct {
	Kind mqttDutyKind
	// Ref is what the duty concerns: an exchange nonce, or a resource incarnation.
	Ref string
	// State is where the duty starts. A duty whose work cannot begin yet starts
	// NOT_YET_ACTIONABLE, which is UNRESOLVED — the cursor must keep coming back for it.
	State mqttDutyState
}

// mqttAllocateInventoryMembers builds the ledger CAS and the duty entries for one allocation.
//
// ONE DEFINITION, used by every composer that allocates. The boundary re-derives all of this
// from the batch and the store, so a bug here cannot land — but two composers each computing
// their own interval would be two chances to compute it differently, and the one that drifted
// would be refused at the boundary with no indication which of them was wrong.
//
// cur is the ledger as it stands, nil when the generation has none yet: a generation's FIRST
// allocation creates its ledger create-only, which is why an absent ledger is a positive answer
// here rather than a failure.
func mqttAllocateInventoryMembers(id, epoch, gen string, cur *mqttGenLedger, curSeq uint64,
	decls []mqttDutyDeclaration) ([]mqttTransitionMember, error) {

	if len(decls) == 0 {
		return nil, nil
	}
	if cur != nil && cur.State != mqttGenLedgerOpen {
		// THE FENCE, refused at the composer as well as at the boundary, and the two are not
		// redundant: this tells its caller why the batch cannot be assembled, the boundary
		// refuses a batch assembled by anything at all.
		return nil, fmt.Errorf("generation %q sealed its inventory at %d, so no further duty is "+
			"allocated in it", gen, cur.SealedHi)
	}
	// AN ALLOCATION MOVES ONLY THE HIGH-WATER. The floor and the deletion watermark are carried
	// unchanged: a generation's first ledger starts both at 1, and only a floor advance
	// (outbound_floor.go) moves them.
	base, lo, del := uint64(0), uint64(1), uint64(1)
	if cur != nil {
		base, lo, del = cur.Hi, cur.Lo, cur.Del
	}
	idHash := getHash(id)
	members := make([]mqttTransitionMember, 0, len(decls)+1)

	next := &mqttGenLedger{
		ID: id, Epoch: epoch, Gen: gen, State: mqttGenLedgerOpen,
		Hi: base + uint64(len(decls)), Lo: lo, Del: del,
	}
	if verr := mqttValidateGenLedger(next, id, epoch, gen); verr != nil {
		return nil, fmt.Errorf("refusing to compose an unsupported generation ledger: %w", verr)
	}
	lbody, err := json.Marshal(next)
	if err != nil {
		return nil, err
	}
	members = append(members, mqttTransitionMember{
		subj: mqttGenLedgerSubject(idHash, gen), expect: curSeq, body: lbody,
	})

	for i, d := range decls {
		ord := base + uint64(i) + 1
		entry := &mqttDutyEntry{
			ID: id, Epoch: epoch, Gen: gen, Ord: ord, Kind: d.Kind, Ref: d.Ref, State: d.State,
		}
		// AN ALLOCATION NEVER STARTS TERMINAL. The thing a duty is predeclared for is being
		// created in this same batch, so it cannot already have ended — and a duty allocated
		// terminal would cover a live obligation with an ending nobody earned, which is the
		// same defect as asserting one later.
		if mqttDutyResolved(d.State) {
			return nil, fmt.Errorf("refusing to allocate duty %d for %s/%q already terminal: "+
				"the thing it is predeclared for is created by this batch and has not ended",
				ord, d.Kind, d.Ref)
		}
		if verr := mqttValidateDutyEntry(entry, id, epoch, gen, ord); verr != nil {
			return nil, fmt.Errorf("refusing to compose an unsupported duty entry at %d: %w", ord, verr)
		}
		body, merr := json.Marshal(entry)
		if merr != nil {
			return nil, merr
		}
		// CREATE-ONLY. An allocation takes an ordinal nothing has occupied, and an expectation
		// here would mean this batch believed it was continuing a duty it is in fact reserving.
		members = append(members, mqttTransitionMember{
			subj: mqttDutySubject(idHash, gen, ord), expect: 0, body: body,
		})
	}
	return members, nil
}
