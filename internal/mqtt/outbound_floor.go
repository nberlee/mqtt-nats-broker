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
	"errors"
	"fmt"
)

// The inventory floor (F-112): a live generation's history is retired behind a PROVEN contiguous
// floor, and physically deleted behind a durable watermark.
//
// # What the floor proves
//
//	Every position below Lo is RETIRED; every duty that still has work or evidence to carry has
//	exactly ONE representative entry in [Lo, Hi].
//
// A position is retired in one of two ways, decided per position by the batch that advances the
// floor: RECLAIMED, when the duty and what it names are finished and nothing can still need them;
// or RELOCATED, when the duty is carried unchanged to a fresh ordinal above Hi in the same batch and
// the old position is left as an inert marker naming where. Relocation is what lets the floor pass
// a long-lived duty — the PUBREL consumer's, a live subscription's, an abandoned exchange's — that
// would otherwise pin it near ordinal 1 for the generation's life.
//
// So "a missing entry is an inconsistency, never completed work" still holds where it is read:
// inside [Lo, Hi]. Below Lo an absence is legitimate by the floor's own proof, and no reader looks.
//
// # What makes the advance sound without reading the exchanges in the batch
//
// Every fact the reclamation predicate reads is ABSORBING: a Terminal duty is immutable, an ended
// exchange phase is absorbing, a resource at CleanupComplete stays there and an edge naming an
// exchange is added only by the batch that admits it, and a register subject that stopped naming a
// lineage never names it again (a register naming L is written only by L's admission or by CAS
// rewrites of a register already naming L). So a fact read at the boundary cannot become false
// before the batch lands; the ledger CAS orders the advance against every admission and every
// other advance, and the marker's entry CAS orders a relocation against a continuation of the duty
// it moves.

// mqttTransitionAdvanceFloor retires inventory positions behind the floor and records physical
// deletion behind the watermark (F-112). It carries the generation ledger and duty members and
// nothing else. Like dispose-resource it is permitted under any authority state and moves no
// ownership: retiring finished history is justified entirely by absorbing, record-bound evidence,
// so it decides nothing for the session and needs no entitlement.
const mqttTransitionAdvanceFloor mqttTransitionKind = "advance-floor"

// The floor's bounds. One batch retires at most mqttFloorBatchPositions positions and relocates
// at most mqttFloorBatchCarry of them; one deletion step removes at most mqttFloorBatchPositions.
// An atomic batch is bounded by the server, and every member here is checked at the boundary by
// reads, so the batch is kept well inside both.
const (
	mqttFloorBatchPositions = 256
	mqttFloorBatchCarry     = 64
	// mqttFloorMinReclaim and the 2x rule are the AMORTIZATION: a horizon is executed only when
	// it reclaims at least max(mqttFloorMinReclaim, 2 * relocations), so the relocation writes
	// are at most half the reclaimed positions and a prefix of long-lived duties is never
	// rewritten for nothing.
	mqttFloorMinReclaim = 32
	// mqttFloorSlack is the span below which a generation's floor is not worth advancing.
	mqttFloorSlack = 256
)

// mqttFloorClass is what a floor advance may do with one position.
type mqttFloorClass uint8

const (
	// mqttFloorReclaim: the duty and what it names are finished and nothing can still need them.
	mqttFloorReclaim mqttFloorClass = iota + 1
	// mqttFloorRelocate: the duty must be carried to a representative above Hi.
	mqttFloorRelocate
	// mqttFloorUndecided: something the decision needs could not be read. The advance stops
	// before it; an undecided position is never relocated or reclaimed.
	mqttFloorUndecided
)

// errMQTTFloorUndecided reports that a position's class could not be decided from the store.
var errMQTTFloorUndecided = errors.New("the position cannot be decided")

// classifyFloorPosition decides one position of generation gen.
//
// ONE DEFINITION, used by the floor unit that composes an advance and by the write boundary that
// admits it, so the two can never disagree about what a position is. Every read is of an
// absorbing fact (see the file comment), which is what lets the boundary's verdict stand until the
// batch lands.
func (jsa *mqttJSA) classifyFloorPosition(id, epoch, gen string, d *mqttDutyEntry) (mqttFloorClass, error) {
	if d == nil {
		return mqttFloorUndecided, fmt.Errorf("%w: no entry", errMQTTFloorUndecided)
	}
	switch d.State {
	case mqttDutyRelocated:
		// A MARKER INSIDE THE INVENTORY is an inconsistency: markers are legal only below Lo.
		return mqttFloorUndecided, fmt.Errorf("%w: position %d is a relocation marker inside the "+
			"inventory", errMQTTFloorUndecided, d.Ord)
	case mqttDutyTerminal:
	default:
		// AN UNRESOLVED DUTY STILL OWES WORK, so its position is carried, never reclaimed.
		return mqttFloorRelocate, nil
	}
	switch d.Kind {
	case mqttDutyExchange:
		return jsa.classifyFloorExchange(id, epoch, gen, d.Ref)
	case mqttDutyProvision, mqttDutyCleanup:
		// A RESOURCE DUTY IS RECLAIMED PER POSITION once the resource's cleanup is COMPLETE. The
		// resource record is permanent and CleanupComplete is absorbing, so this needs no read of
		// the sibling duty — which may itself already be reclaimed and deleted.
		// completeCleanupAndResolve writes CleanupComplete and the cleanup duty's Terminal in ONE
		// batch; a provision duty still open at CleanupComplete never reaches here (unresolved).
		r, _, rerr := jsa.loadOutboundResource(id, epoch, gen, d.Ref)
		if rerr != nil || r == nil {
			return mqttFloorUndecided, fmt.Errorf("%w: resource %q: %v", errMQTTFloorUndecided, d.Ref, rerr)
		}
		if r.State == mqttResourceCleanupComplete {
			return mqttFloorReclaim, nil
		}
		return mqttFloorRelocate, nil
	}
	return mqttFloorUndecided, fmt.Errorf("%w: duty kind %q", errMQTTFloorUndecided, d.Kind)
}

// classifyFloorExchange decides a TERMINAL exchange duty's position: RECLAIMED only when the
// obligation is ended for good and nothing can still need its record.
//
//  1. the exchange is at an IDENTIFIER ending (mqttExchangePhaseEnded). DeliveryAbandoned and
//     Discarded keep the identifier and are never reclaimed;
//  2. no resource still holds an edge naming it: every hold its reverse index names, and a
//     retained copy's owner, either stands at CleanupComplete or lists no edge for this nonce;
//  3. the register subject (gen, PI) no longer names the lineage. This subsumes the canonical
//     PUBREL (disposed before the confirmed register delete), the retained rel, the HandBack and
//     the terminal slot kept for a re-offerable source, and with the write boundary's token rules
//     it subsumes action drain: no batch moves an exchange into an ended phase beside a standing
//     token, and no register naming a lineage is deleted or re-pointed while it holds one — so an
//     ended exchange whose register is gone has no token left that could emit.
func (jsa *mqttJSA) classifyFloorExchange(id, epoch, gen, nonce string) (mqttFloorClass, error) {
	x, _, xerr := jsa.loadOutboundExchange(id, epoch, gen, nonce)
	if xerr != nil || x == nil {
		// INSIDE THE INVENTORY an absent exchange is an inconsistency, never a reclamation.
		return mqttFloorUndecided, fmt.Errorf("%w: exchange %q: %v", errMQTTFloorUndecided, nonce, xerr)
	}
	if !mqttExchangePhaseEnded(x.Phase) {
		return mqttFloorRelocate, nil
	}
	for _, h := range x.Holds {
		cls, err := jsa.classifyFloorEdge(id, epoch, gen, h.Inc, nonce, h.Role)
		if cls != mqttFloorReclaim {
			return cls, err
		}
	}
	if x.RetainedOwner != _EMPTY_ {
		cls, err := jsa.classifyFloorEdge(id, epoch, gen, x.RetainedOwner, nonce, mqttRoleRetainedMaterial)
		if cls != mqttFloorReclaim {
			return cls, err
		}
	}
	rec, smsg, rerr := jsa.loadInflightRecord(mqttInflightSubject(getHash(id), gen, x.PI))
	switch {
	case rerr != nil:
		return mqttFloorUndecided, fmt.Errorf("%w: register %d: %v", errMQTTFloorUndecided, x.PI, rerr)
	case rec == nil && smsg != nil:
		// An UNREADABLE register may be the one naming this lineage.
		return mqttFloorUndecided, fmt.Errorf("%w: register %d is unreadable", errMQTTFloorUndecided, x.PI)
	case rec != nil && rec.Lin == nonce:
		return mqttFloorRelocate, nil
	}
	return mqttFloorReclaim, nil
}

// classifyFloorEdge decides whether resource inc still holds an edge of role naming nonce.
func (jsa *mqttJSA) classifyFloorEdge(id, epoch, gen, inc, nonce string, role mqttResourceRole) (mqttFloorClass, error) {
	r, _, rerr := jsa.loadOutboundResource(id, epoch, gen, inc)
	if rerr != nil || r == nil {
		return mqttFloorUndecided, fmt.Errorf("%w: resource %q: %v", errMQTTFloorUndecided, inc, rerr)
	}
	if r.State == mqttResourceCleanupComplete {
		return mqttFloorReclaim, nil
	}
	for _, e := range r.Deps {
		if e.Nonce == nonce && e.Role == role {
			return mqttFloorRelocate, nil
		}
	}
	return mqttFloorReclaim, nil
}

// mqttFloorPosition is one position a floor advance retires, as the composer read it.
type mqttFloorPosition struct {
	Ord   uint64
	Entry *mqttDutyEntry
	Seq   uint64
	Class mqttFloorClass
}

// mqttFloorAdvance is one batch: the ledger as read, and the contiguous positions [Lo, Lo+len)
// it retires.
type mqttFloorAdvance struct {
	ID, Epoch, Gen string
	Ledger         *mqttGenLedger
	LedgerSeq      uint64
	Positions      []mqttFloorPosition
	// DelTo, when above the ledger's Del, also advances the deletion watermark to it. Every
	// position in [Del, DelTo) must already be physically deleted, and DelTo may not pass the
	// floor the ledger was READ at: a watermark covers only positions an earlier batch retired.
	DelTo uint64
}

// mqttComposeFloorMembers builds the ledger and duty members of one floor advance.
func mqttComposeFloorMembers(f *mqttFloorAdvance) ([]mqttTransitionMember, error) {
	led := f.Ledger
	if led == nil || f.LedgerSeq == 0 {
		return nil, fmt.Errorf("a floor advance of generation %q needs the ledger it was read against", f.Gen)
	}
	idHash := getHash(f.ID)
	next := *led
	members := []mqttTransitionMember{}
	for i, p := range f.Positions {
		if p.Ord != led.Lo+uint64(i) {
			return nil, fmt.Errorf("a floor advance of generation %q retires position %d out of order "+
				"(want %d): the floor is contiguous", f.Gen, p.Ord, led.Lo+uint64(i))
		}
		switch p.Class {
		case mqttFloorReclaim:
		case mqttFloorRelocate:
			next.Hi++
			copyEntry := *p.Entry
			copyEntry.Ord, copyEntry.From, copyEntry.To = next.Hi, p.Ord, 0
			if verr := mqttValidateDutyEntry(&copyEntry, f.ID, f.Epoch, f.Gen, next.Hi); verr != nil {
				return nil, fmt.Errorf("refusing to compose an unsupported representative: %w", verr)
			}
			marker := mqttDutyEntry{
				ID: f.ID, Epoch: f.Epoch, Gen: f.Gen, Ord: p.Ord, Kind: p.Entry.Kind, Ref: p.Entry.Ref,
				State: mqttDutyRelocated, To: next.Hi,
			}
			if verr := mqttValidateDutyEntry(&marker, f.ID, f.Epoch, f.Gen, p.Ord); verr != nil {
				return nil, fmt.Errorf("refusing to compose an unsupported relocation marker: %w", verr)
			}
			cbody, cerr := json.Marshal(&copyEntry)
			if cerr != nil {
				return nil, cerr
			}
			mbody, merr := json.Marshal(&marker)
			if merr != nil {
				return nil, merr
			}
			members = append(members,
				mqttTransitionMember{subj: mqttDutySubject(idHash, f.Gen, next.Hi), expect: 0, body: cbody},
				mqttTransitionMember{subj: mqttDutySubject(idHash, f.Gen, p.Ord), expect: p.Seq, body: mbody},
			)
		default:
			return nil, fmt.Errorf("a floor advance of generation %q cannot retire undecided position %d",
				f.Gen, p.Ord)
		}
	}
	next.Lo = led.Lo + uint64(len(f.Positions))
	if f.DelTo > next.Del {
		next.Del = f.DelTo
	}
	if led.State == mqttGenLedgerSealed {
		// A SEALED ledger keeps its shape: only the watermark moves.
		next.Hi, next.Lo = led.Hi, led.Lo
	}
	if verr := mqttValidateGenLedger(&next, f.ID, f.Epoch, f.Gen); verr != nil {
		return nil, fmt.Errorf("refusing to compose an unsupported floor ledger: %w", verr)
	}
	lbody, err := json.Marshal(&next)
	if err != nil {
		return nil, err
	}
	return append([]mqttTransitionMember{{
		subj: mqttGenLedgerSubject(idHash, f.Gen), expect: f.LedgerSeq, body: lbody,
	}}, members...), nil
}

// composeFloorBatch reads one bounded floor advance of an OPEN generation from its floor up to (not
// including) end: the longest contiguous run of decided positions, at most mqttFloorBatchPositions
// of them and at most mqttFloorBatchCarry relocations, stopping before the first undecided one. A
// nil advance means the ledger is absent or sealed; an advance with no positions means the floor's
// first position is undecided now.
func (jsa *mqttJSA) composeFloorBatch(id, epoch, gen string, end uint64) (*mqttFloorAdvance, error) {
	led, ledSeq, lerr := jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil {
		return nil, lerr
	}
	if led == nil || led.State != mqttGenLedgerOpen {
		return nil, nil
	}
	f := &mqttFloorAdvance{ID: id, Epoch: epoch, Gen: gen, Ledger: led, LedgerSeq: ledSeq}
	carried := 0
	for ord := led.Lo; ord < end && ord <= led.Hi && len(f.Positions) < mqttFloorBatchPositions; ord++ {
		d, dseq, derr := jsa.loadDutyEntryAt(id, epoch, gen, ord)
		if derr != nil || d == nil {
			break
		}
		cls, _ := jsa.classifyFloorPosition(id, epoch, gen, d)
		if cls == mqttFloorUndecided {
			break
		}
		if cls == mqttFloorRelocate {
			if carried == mqttFloorBatchCarry {
				break
			}
			carried++
		}
		f.Positions = append(f.Positions, mqttFloorPosition{Ord: ord, Entry: d, Seq: dseq, Class: cls})
	}
	return f, nil
}

// opAdvanceFloor commits one floor advance under the standing authority, moving nothing on it but
// its revision.
func (jsa *mqttJSA) opAdvanceFloor(f *mqttFloorAdvance, txn string) error {
	auth, authSeq, err := jsa.requireAuthority(f.ID, f.Epoch)
	if err != nil {
		return err
	}
	if auth == nil {
		return fmt.Errorf("client %q has no outbound authority, so no floor of its may advance", f.ID)
	}
	members, merr := mqttComposeFloorMembers(f)
	if merr != nil {
		return merr
	}
	_, cerr := jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: f.ID, Epoch: f.Epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionAdvanceFloor, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: authSeq,
		Auth: &mqttOutboundAuthority{
			ID: auth.ID, Epoch: auth.Epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: auth.State, Rev: auth.Rev + 1, Txn: txn, OwnerTxn: auth.OwnerTxn,
			Intent: auth.Intent, Creation: auth.Creation,
		},
		Extra: members,
	})
	return cerr
}

// mqttCheckAdvanceFloorKind is the kind rule: preserve the authority exactly, carry exactly one
// ledger member and nothing but duty members beside it.
func mqttCheckAdvanceFloorKind(t *mqttAuthorityTransition) error {
	if err := mqttCheckEndingPreservesAuthority(t, "an advance-floor"); err != nil {
		return err
	}
	ledgers := 0
	for _, e := range t.Extra {
		switch {
		case mqttIsLedgerSubject(e.subj):
			ledgers++
		case mqttIsDutySubject(e.subj):
		default:
			return fmt.Errorf("an advance-floor transition carries member %q, which is neither the "+
				"generation ledger nor a duty entry", e.subj)
		}
	}
	if ledgers != 1 {
		return fmt.Errorf("an advance-floor transition carries %d ledger members: the floor is one "+
			"ledger's", ledgers)
	}
	return nil
}

// checkFloorAdvance is the write boundary of an advance-floor batch: every ledger and duty member
// it carries is decided here, against the store, and nowhere else.
//
//   - the ledger is read at the member's expectation; an OPEN ledger may move Lo forward and Hi
//     by exactly the representatives the batch creates; a SEALED one keeps its shape and may move
//     only the deletion watermark;
//   - every position in [Lo, Lo') is RECLAIMED under classifyFloorPosition, re-decided here, or
//     carried by a marker AND its representative in this batch, the representative equal to the
//     standing entry in everything but its ordinal and its provenance;
//   - every position in [Del, Del') is physically deleted, and Del' does not pass the floor the
//     ledger was read at.
func (jsa *mqttJSA) checkFloorAdvance(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	idHash := getHash(a.ID)
	var lm *mqttTransitionMember
	var gen string
	for i := range extra {
		e := &extra[i]
		if !mqttIsLedgerSubject(e.subj) {
			continue
		}
		g, perr := mqttParseLedgerSubject(e.subj, idHash)
		if perr != nil {
			return fmt.Errorf("transition %q carries a ledger member on %q, which this broker cannot "+
				"place: %w", a.Txn, e.subj, perr)
		}
		lm, gen = e, g
	}
	if lm == nil {
		return fmt.Errorf("transition %q advances a floor without its ledger", a.Txn)
	}
	next, derr := mqttDecodeGenLedger(lm.body, a.ID, a.Epoch, gen)
	if derr != nil {
		return fmt.Errorf("transition %q carries an unsupported generation ledger: %w", a.Txn, derr)
	}
	cur, curSeq, lerr := jsa.loadGenLedger(a.ID, a.Epoch, gen)
	if lerr != nil {
		return fmt.Errorf("transition %q advances the floor of generation %q, whose ledger could not "+
			"be read: %w", a.Txn, gen, lerr)
	}
	if cur == nil || lm.expect == 0 || lm.expect != curSeq {
		return fmt.Errorf("transition %q advances the floor of generation %q against ledger sequence %d, "+
			"but it stands at %d: a floor advance continues the ledger it read", a.Txn, gen, lm.expect, curSeq)
	}
	if next.State != cur.State {
		return fmt.Errorf("transition %q moves generation %q's ledger from state %d to %d: a floor "+
			"advance neither seals nor reopens", a.Txn, gen, cur.State, next.State)
	}
	if next.SealedHi != cur.SealedHi {
		return fmt.Errorf("transition %q rewrites generation %q's sealed bound", a.Txn, gen)
	}
	if next.Lo < cur.Lo || next.Del < cur.Del {
		return fmt.Errorf("transition %q moves generation %q's floor or watermark backwards", a.Txn, gen)
	}
	if next.Lo == cur.Lo && next.Del == cur.Del {
		return fmt.Errorf("transition %q advances neither the floor nor the watermark of generation %q",
			a.Txn, gen)
	}
	if cur.State == mqttGenLedgerSealed && (next.Lo != cur.Lo || next.Hi != cur.Hi) {
		// THE FLOOR FREEZES WITH THE SEAL. A sealed inventory's shape is what its recovery root
		// covers; only deletions already authorized below the floor still finish.
		return fmt.Errorf("transition %q moves the floor of generation %q, whose inventory is sealed: "+
			"only the deletion watermark moves after a seal", a.Txn, gen)
	}
	if next.Lo-cur.Lo > mqttFloorBatchPositions || next.Del-cur.Del > mqttFloorBatchPositions {
		return fmt.Errorf("transition %q retires or deletes more than %d positions of generation %q in "+
			"one batch", a.Txn, mqttFloorBatchPositions, gen)
	}

	// THE MEMBERS: create-only representatives above the old Hi, and markers at retired positions.
	copies := map[uint64]*mqttDutyEntry{}
	markers := map[uint64]struct {
		next   *mqttDutyEntry
		expect uint64
	}{}
	for _, e := range extra {
		if !mqttIsDutySubject(e.subj) {
			continue
		}
		dgen, ord, perr := mqttParseDutySubject(e.subj, idHash)
		if perr != nil || dgen != gen {
			return fmt.Errorf("transition %q carries duty member %q outside generation %q", a.Txn, e.subj, gen)
		}
		d, ddErr := mqttDecodeDutyEntry(e.body, a.ID, a.Epoch, gen, ord)
		if ddErr != nil {
			return fmt.Errorf("transition %q carries an unsupported duty entry on %q: %w", a.Txn, e.subj, ddErr)
		}
		if e.expect == 0 {
			copies[ord] = d
			continue
		}
		if d.State != mqttDutyRelocated {
			return fmt.Errorf("transition %q continues duty %d in a floor advance: a floor advance "+
				"writes relocation markers and representatives, and continues no duty", a.Txn, ord)
		}
		markers[ord] = struct {
			next   *mqttDutyEntry
			expect uint64
		}{d, e.expect}
	}
	if len(markers) > mqttFloorBatchCarry {
		return fmt.Errorf("transition %q relocates %d positions in one batch (at most %d)",
			a.Txn, len(markers), mqttFloorBatchCarry)
	}
	if next.Hi != cur.Hi+uint64(len(copies)) {
		return fmt.Errorf("transition %q moves generation %q's high-water from %d to %d while creating %d "+
			"representative(s): a floor advance allocates only the positions it carries duties to",
			a.Txn, gen, cur.Hi, next.Hi, len(copies))
	}
	for ord := cur.Hi + 1; ord <= next.Hi; ord++ {
		if _, ok := copies[ord]; !ok {
			return fmt.Errorf("transition %q allocates %d..%d but carries no representative at %d",
				a.Txn, cur.Hi+1, next.Hi, ord)
		}
	}

	// EVERY RETIRED POSITION, re-decided against the store.
	for ord := cur.Lo; ord < next.Lo; ord++ {
		subj := mqttDutySubject(idHash, gen, ord)
		smsg, rerr := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
		if rerr != nil || smsg == nil {
			return fmt.Errorf("transition %q retires position %d of generation %q, which could not be "+
				"read: a position inside the inventory exists, and retiring one this broker cannot see "+
				"would retire a duty nobody decided (%v)", a.Txn, ord, gen, rerr)
		}
		standing, sderr := mqttDecodeDutyEntry(smsg.Data, a.ID, a.Epoch, gen, ord)
		if sderr != nil {
			return fmt.Errorf("transition %q retires position %d of generation %q, whose entry cannot be "+
				"interpreted: %w", a.Txn, ord, gen, sderr)
		}
		if mk, carried := markers[ord]; carried {
			if mk.expect != smsg.Sequence {
				return fmt.Errorf("transition %q relocates position %d at sequence %d, but it stands at %d",
					a.Txn, ord, mk.expect, smsg.Sequence)
			}
			if standing.State == mqttDutyRelocated {
				return fmt.Errorf("transition %q relocates position %d, which is already a relocation marker",
					a.Txn, ord)
			}
			rep, ok := copies[mk.next.To]
			if !ok {
				return fmt.Errorf("transition %q relocates position %d to %d without creating the "+
					"representative there: a retired position's duty has exactly one representative",
					a.Txn, ord, mk.next.To)
			}
			if mk.next.Kind != standing.Kind || mk.next.Ref != standing.Ref {
				return fmt.Errorf("transition %q re-identifies position %d in its relocation marker", a.Txn, ord)
			}
			want := *standing
			want.Ord, want.From, want.To = rep.Ord, ord, 0
			if !mqttDutyEntryEqual(&want, rep) {
				return fmt.Errorf("transition %q carries position %d's duty to %d with a different "+
					"state: a relocation moves a POSITION and never the duty's state, claim or outcome",
					a.Txn, ord, rep.Ord)
			}
			delete(copies, rep.Ord)
			delete(markers, ord)
			continue
		}
		cls, cerr := jsa.classifyFloorPosition(a.ID, a.Epoch, gen, standing)
		if cls != mqttFloorReclaim {
			return fmt.Errorf("transition %q retires position %d of generation %q without carrying its "+
				"duty, but that position is not reclaimable (%v): a duty that still has work or "+
				"evidence keeps exactly one representative inside the inventory", a.Txn, ord, gen, cerr)
		}
	}
	if len(markers) != 0 || len(copies) != 0 {
		return fmt.Errorf("transition %q carries %d marker(s) and %d representative(s) that do not pair "+
			"with a retired position", a.Txn, len(markers), len(copies))
	}

	// AND THE WATERMARK covers only positions an earlier batch retired, each already deleted.
	if next.Del > cur.Lo {
		return fmt.Errorf("transition %q moves generation %q's deletion watermark to %d past the floor %d "+
			"it read: only positions an earlier batch retired may be recorded deleted",
			a.Txn, gen, next.Del, cur.Lo)
	}
	for ord := cur.Del; ord < next.Del; ord++ {
		smsg, rerr := jsa.loadLastMsgFor(mqttInflightStreamName, mqttDutySubject(idHash, gen, ord))
		if rerr == nil && smsg != nil {
			return fmt.Errorf("transition %q records position %d of generation %q deleted while its entry "+
				"still stands", a.Txn, ord, gen)
		}
		if rerr != nil && isErrorOtherThan(rerr, JSNoMessageFoundErr) {
			return fmt.Errorf("transition %q records position %d of generation %q deleted, which could not "+
				"be read: %w", a.Txn, ord, gen, rerr)
		}
	}
	return nil
}

// mqttDutyEntryEqual compares two duty entries field by field.
func mqttDutyEntryEqual(a, b *mqttDutyEntry) bool {
	return *a == *b
}

// ---------------------------------------------------------------------------
// Physical deletion below the floor
// ---------------------------------------------------------------------------

// deleteRetiredPositionsStep removes the records of at most mqttFloorBatchPositions retired
// positions from the deletion watermark up, then records the watermark. It reports whether
// positions remain to be deleted.
//
// ORDER: for a RECLAIMED exchange duty the exchange record goes first and the duty entry after it,
// so a crash between the two leaves a duty that still names what to delete; a relocation marker
// names no exchange, and only its own entry goes. Every delete is by the EXACT sequence just read,
// and a delete that removes nothing is classified by a re-read rather than assumed: the subjects
// carry the generation and the nonce or ordinal, so a late delete can only name what is gone.
func (jsa *mqttJSA) deleteRetiredPositionsStep(id, epoch, gen string) (bool, error) {
	return jsa.deleteRetiredPositionsUpTo(id, epoch, gen, mqttFloorBatchPositions)
}

// deleteRetiredPositionsUpTo is deleteRetiredPositionsStep bounded to at most limit positions.
func (jsa *mqttJSA) deleteRetiredPositionsUpTo(id, epoch, gen string, limit uint64) (bool, error) {
	led, ledSeq, lerr := jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil {
		return false, lerr
	}
	if led == nil || led.Del >= led.Lo {
		return false, nil
	}
	idHash := getHash(id)
	end := min(led.Lo, led.Del+min(limit, mqttFloorBatchPositions))
	for ord := led.Del; ord < end; ord++ {
		dsubj := mqttDutySubject(idHash, gen, ord)
		smsg, rerr := jsa.loadLastMsgFor(mqttInflightStreamName, dsubj)
		if rerr != nil {
			if isErrorOtherThan(rerr, JSNoMessageFoundErr) {
				return true, fmt.Errorf("reading retired position %d of generation %q: %w", ord, gen, rerr)
			}
			continue // already deleted by an earlier run
		}
		d, derr := mqttDecodeDutyEntry(smsg.Data, id, epoch, gen, ord)
		if derr != nil {
			// A RETIRED POSITION THIS BROKER CANNOT INTERPRET is left standing: a cleanup does not
			// remove what it cannot identify, and the watermark stops before it.
			return true, fmt.Errorf("retired position %d of generation %q cannot be interpreted: %w", ord, gen, derr)
		}
		if d.Kind == mqttDutyExchange && d.State == mqttDutyTerminal {
			if err := jsa.deleteReclaimedExchange(id, epoch, gen, d.Ref); err != nil {
				return true, err
			}
		}
		if err := jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); err != nil {
			if again, aerr := jsa.loadLastMsgFor(mqttInflightStreamName, dsubj); aerr == nil && again != nil {
				return true, fmt.Errorf("deleting retired position %d of generation %q: %w", ord, gen, err)
			} else if aerr != nil && isErrorOtherThan(aerr, JSNoMessageFoundErr) {
				return true, fmt.Errorf("deleting retired position %d of generation %q was unresolved: %w",
					ord, gen, aerr)
			}
		}
	}
	f := &mqttFloorAdvance{ID: id, Epoch: epoch, Gen: gen, Ledger: led, LedgerSeq: ledSeq, DelTo: end}
	if err := jsa.opAdvanceFloor(f, mqttNewInflightNonce()); err != nil {
		return true, err
	}
	return end < led.Lo, nil
}

// deleteReclaimedExchange removes one reclaimed exchange record by the exact sequence it stands at.
func (jsa *mqttJSA) deleteReclaimedExchange(id, epoch, gen, nonce string) error {
	xsubj := mqttExchangeSubject(getHash(id), gen, nonce)
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, xsubj)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return fmt.Errorf("reading reclaimed exchange %q of generation %q: %w", nonce, gen, err)
		}
		return nil
	}
	if derr := jsa.deleteMsg(mqttInflightStreamName, smsg.Sequence, true); derr != nil {
		again, aerr := jsa.loadLastMsgFor(mqttInflightStreamName, xsubj)
		if aerr == nil && again != nil {
			return fmt.Errorf("deleting reclaimed exchange %q of generation %q: %w", nonce, gen, derr)
		}
		if aerr != nil && isErrorOtherThan(aerr, JSNoMessageFoundErr) {
			return fmt.Errorf("deleting reclaimed exchange %q of generation %q was unresolved: %w", nonce, gen, aerr)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Absence below the floor (F-112)
// ---------------------------------------------------------------------------

// mqttLandingProof is how a reader knows the exchange it is asking about LANDED. A typed value,
// never a boolean: an absence is classified as a reclamation only with one of exactly these, and
// "the reader believes so" is not among them.
type mqttLandingProof uint8

const (
	// mqttLandingUnproven: nothing positive. An absence keeps its pre-floor meaning at that site.
	mqttLandingUnproven mqttLandingProof = iota
	// mqttLandingDecodedRead: this process decoded the exchange (a custody holder, a binding
	// built from a read exchange, a continuation that read it).
	mqttLandingDecodedRead
	// mqttLandingCommitted: the attempt resolver returned a COMMITTED verdict — LANDED or
	// SUPERSEDED, the binding and the independently retained digest validated — for an attempt
	// that wrote this lineage. ABORTED and unresolved are not proofs: a permanent aborted
	// outcome is exactly what a never-landed admission leaves.
	mqttLandingCommitted
	// mqttLandingValidatedRegister: a resume read a validated register naming the lineage.
	mqttLandingValidatedRegister
	// mqttLandingAdmittedDuty: this process decoded a duty entry naming the lineage. A duty is
	// allocated only by the batch that admits its exchange (the coverage rule), so a decoded one
	// proves the admission landed.
	mqttLandingAdmittedDuty
)

// mqttLandingProofDecided resolves the attempts that wrote lin and returns a COMMITTED proof if
// any of them committed, and reports whether the answer is DECIDED. Unproven-and-decided means every attempt that wrote lin positively never landed (an
// ABORTED outcome), or none wrote it: nothing of the lineage landed. Unproven-and-undecided means
// some attempt's resolution is UNRESOLVED — an outcome or authority read that failed, or a batch
// still in flight — which proves neither direction and must never be read as "never landed".
func (jsa *mqttJSA) mqttLandingProofDecided(attempts []mqttCustodyAttempt, lin string) (mqttLandingProof, bool) {
	decided := true
	for _, a := range attempts {
		if a.lin != lin {
			continue
		}
		if a.landed {
			return mqttLandingCommitted, true
		}
		if a.handle == nil {
			continue
		}
		switch jsa.resolveTransitionHandle(a.handle) {
		case mqttTransitionLanded, mqttTransitionSuperseded:
			return mqttLandingCommitted, true
		case mqttTransitionNeverLanded:
		default:
			decided = false
		}
	}
	return mqttLandingUnproven, decided
}

// mqttAbsenceVerdict is what an absent exchange read means.
type mqttAbsenceVerdict uint8

const (
	// mqttAbsenceUnproven: no landing proof; the site keeps its pre-floor meaning.
	mqttAbsenceUnproven mqttAbsenceVerdict = iota
	// mqttAbsenceReclaimed: a positive, terminal ending: the floor retired the exchange's
	// position, which it does only for an obligation ended for good.
	mqttAbsenceReclaimed
	// mqttAbsenceUncertain: an inconsistency (a duty inside the inventory still names it) or an
	// inventory that could not be read whole. Proves nothing.
	mqttAbsenceUncertain
)

// classifyExchangeAbsence classifies an exchange that read ABSENT. THE ONE DEFINITION:
//
//	Exchange L read absent. It is RECLAIMED iff the caller holds a landing proof for L AND a
//	COMPLETE traversal of [Lo, Hi], taken after the absent read, finds no duty naming L.
//
// Sound because the floor retires an exchange's position only when the exchange is reclaimable
// (an ending), a relocated exchange keeps exactly one representative naming it, and a lineage
// that never landed has no landing proof. A surviving duty naming L makes the absence an
// inconsistency. An incomplete traversal proves nothing, and neither does an undecodable
// POSITION, which may be the representative; another lineage's unreadable exchange does not
// matter, since its duty decoded and named that lineage. Cost: one traversal per absence, which
// is rare.
func (jsa *mqttJSA) classifyExchangeAbsence(id, epoch, gen, lin string, proof mqttLandingProof) mqttAbsenceVerdict {
	if proof == mqttLandingUnproven {
		return mqttAbsenceUnproven
	}
	census, err := mqttTraverseGenerationInventory(jsa, nil, id, epoch, gen, nil)
	if err != nil || census == nil || !census.complete {
		return mqttAbsenceUncertain
	}
	// AN UNDECODABLE POSITION may be the representative naming lin, so it decides nothing. An
	// uncertain EXCHANGE subject does not: its duty decoded, and named the lineage it reports in
	// census.named, which the check below reads.
	for _, u := range census.uncertain {
		if mqttIsDutySubject(u) {
			return mqttAbsenceUncertain
		}
	}
	if _, named := census.named[lin]; named {
		return mqttAbsenceUncertain
	}
	return mqttAbsenceReclaimed
}
