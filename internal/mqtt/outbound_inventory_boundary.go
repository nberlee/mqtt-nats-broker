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
	"fmt"
	"strings"
)

// The inventory's write boundary (F-105, rows 6 and 8).
//
// # Why these rules are stated over what a batch MUST CONTAIN
//
// An earlier revision of this design proposed two checks: refuse a create-only duty member when
// the batch's ledger member says SEALED, and refuse a duty member above the sealed bound. Both
// were conditioned on a member the caller simply OMITS. A batch carrying a create-only duty and
// no ledger member escaped the first outright, and the second needed authoritative ledger
// information the batch does not necessarily carry.
//
// So the rules are obligations on the batch rather than conditions on its contents:
//
//  1. EVERY ALLOCATION INCLUDES the matching OPEN-ledger CAS and exactly the allocated ordinal
//     interval. A create-only duty member in a batch with no ledger member is refused for that
//     reason alone — the absence IS the violation, not an exemption from the rule.
//  2. EVERY NON-ALLOCATING CONTINUATION updates an existing entry and preserves its immutable
//     identity and its permitted purpose. A continuation cannot repurpose an entry into a duty
//     it was never predeclared for.
//  3. A new exchange or resource CANNOT OMIT its required inventory coverage. (Landed with the
//     composers that produce it; see the note at checkInventoryCoverage.)
//
// # The invariant the recovery cursor rests on
//
// Once a duty is counted as TERMINAL, no permitted transition may make it owe work again. The
// bounded sweep persists "the lowest ordinal I could not finish" and advances past everything
// below it; if terminality could reverse, that advance would step over work that came back. So
// the boundary enforces irreversibility here rather than assuming it, and "currently inactive",
// "claim expired" and "not actionable yet" are each a state that is deliberately NOT terminal.

// mqttBatchLedgerMember is the ledger CAS a batch carries, if it carries one.
type mqttBatchLedgerMember struct {
	subj   string
	expect uint64
	next   *mqttGenLedger
	gen    string
}

// checkInventoryMembers validates every ledger and duty member of one batch.
//
// Unconditional over the members, so no transition kind and no caller can route an allocation
// past it: an allocation is identified by carrying a create-only duty member, which is a
// property of the member and not of the transition's name.
func (jsa *mqttJSA) checkInventoryMembers(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	// A FLOOR ADVANCE IS DECIDED WHOLE BY ITS OWN BOUNDARY (F-112): its create-only members are
	// representatives, not allocations, and its continuations are relocation markers, not duty
	// continuations, so neither generic rule below describes them.
	if a.Kind == mqttTransitionAdvanceFloor {
		return jsa.checkFloorAdvance(a, extra)
	}
	idHash := getHash(a.ID)

	var ledger *mqttBatchLedgerMember
	for _, e := range extra {
		if !mqttIsLedgerSubject(e.subj) {
			continue
		}
		gen, perr := mqttParseLedgerSubject(e.subj, idHash)
		if perr != nil {
			return fmt.Errorf("transition %q carries a ledger member on %q, which this broker "+
				"cannot place: %w", a.Txn, e.subj, perr)
		}
		next, derr := mqttDecodeGenLedger(e.body, a.ID, a.Epoch, gen)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported generation ledger on %q: %w",
				a.Txn, e.subj, derr)
		}
		if ledger != nil {
			// ONE GENERATION'S ALLOCATION PER BATCH. Two ledgers would be two generations
			// allocating in one act, and the interval check below could only be about one of
			// them.
			return fmt.Errorf("transition %q carries ledger members for two generations, %q and "+
				"%q: an allocation belongs to one generation", a.Txn, ledger.gen, gen)
		}
		ledger = &mqttBatchLedgerMember{subj: e.subj, expect: e.expect, next: next, gen: gen}
	}

	allocated := map[uint64]*mqttDutyEntry{}
	for _, e := range extra {
		if !mqttIsDutySubject(e.subj) {
			continue
		}
		gen, ord, perr := mqttParseDutySubject(e.subj, idHash)
		if perr != nil {
			return fmt.Errorf("transition %q carries a duty member on %q, which this broker "+
				"cannot place: %w", a.Txn, e.subj, perr)
		}
		next, derr := mqttDecodeDutyEntry(e.body, a.ID, a.Epoch, gen, ord)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported duty entry on %q: %w",
				a.Txn, e.subj, derr)
		}
		// RELOCATION IS THE FLOOR'S AND ONLY THE FLOOR'S (F-112). A marker, or a newly created
		// representative, written by any other kind would retire a position, or carry a duty, with
		// none of the evidence checkFloorAdvance reads. A representative's later CONTINUATION keeps
		// its provenance, and checkDutyContinuation holds it to exactly that.
		if next.State == mqttDutyRelocated || (e.expect == 0 && next.From != 0) {
			return fmt.Errorf("transition %q of kind %q writes duty %d as a relocation marker or a "+
				"representative: only an advance-floor transition relocates", a.Txn, a.Kind, ord)
		}
		if e.expect == 0 {
			// AN ALLOCATION. Every one of these needs the ledger member checked below; it is
			// collected here and the interval is validated once, against the whole set.
			//
			// AND IT NEVER STARTS TERMINAL. The thing a duty is predeclared for is created by
			// this same batch, so it cannot already have ended. The composer refuses this, but a
			// composer is not a boundary: a member assembled any other way reaches the store
			// without meeting it, and an entry that arrives terminal is one the bounded sweep
			// advances past forever while the obligation it names is still owed. It is the same
			// defect checkDutyEndingIsEarned exists for, arrived at by ALLOCATING the ending
			// instead of asserting it later — and that check cannot see this, because it runs
			// only where a duty is being continued.
			if mqttDutyResolved(next.State) {
				return fmt.Errorf("transition %q allocates duty %d for %s/%q already terminal "+
					"with outcome %q: the thing it is predeclared for is created by this batch "+
					"and has not ended, and a sweep passes a terminal ordinal exactly once",
					a.Txn, ord, next.Kind, next.Ref, next.Outcome)
			}
			if ledger != nil && gen != ledger.gen {
				return fmt.Errorf("transition %q allocates duty %d in generation %q while its "+
					"ledger belongs to %q", a.Txn, ord, gen, ledger.gen)
			}
			if _, dup := allocated[ord]; dup {
				return fmt.Errorf("transition %q allocates ordinal %d twice", a.Txn, ord)
			}
			allocated[ord] = next
			continue
		}
		if err := jsa.checkDutyContinuation(a, e, gen, ord, next, extra); err != nil {
			return err
		}
	}

	if err := jsa.checkInventoryAllocation(a, ledger, allocated); err != nil {
		return err
	}
	if err := mqttCheckInventoryCoverage(a, extra, allocated); err != nil {
		return err
	}
	return mqttCheckExchangeDutyIdentifiers(a, extra, allocated)
}

// mqttCheckExchangeDutyIdentifiers binds every allocated exchange duty to the exchange it names:
// that exchange is CREATED in this same batch, in the duty's generation, and holds exactly the
// identifier the duty names.
//
// Coverage (below) requires the other direction — a created exchange owes a duty — and says
// nothing about what the duty claims. The identifier is the part a reader now trusts without
// reading the exchange: exclusivity skips every exchange whose duty names an identifier it does
// not claim. A duty naming the wrong identifier, or naming an exchange this batch does not
// create, would hide a live obligation from it, so the claim is checked where both are written.
func mqttCheckExchangeDutyIdentifiers(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	allocated map[uint64]*mqttDutyEntry) error {

	idHash := getHash(a.ID)
	for ord, d := range allocated {
		if d.Kind != mqttDutyExchange {
			continue
		}
		subj := mqttExchangeSubject(idHash, d.Gen, d.Ref)
		var created *mqttTransitionMember
		for i := range extra {
			if extra[i].subj == subj && extra[i].expect == 0 {
				created = &extra[i]
				break
			}
		}
		if created == nil {
			return fmt.Errorf("transition %q allocates exchange duty %d for %q, which this batch does "+
				"not create: an exchange's duty is allocated in the batch that creates it, and only "+
				"there can the identifier it names be bound to the exchange", a.Txn, ord, d.Ref)
		}
		x, xerr := mqttDecodeOutboundExchange(created.body, a.ID, a.Epoch, d.Gen, d.Ref)
		if xerr != nil {
			return fmt.Errorf("transition %q creates exchange %q whose body this broker cannot "+
				"interpret, so the identifier its duty names cannot be checked: %w", a.Txn, d.Ref, xerr)
		}
		if x.PI != d.PI {
			return fmt.Errorf("transition %q allocates exchange duty %d naming identifier %d for "+
				"exchange %q, which holds %d", a.Txn, ord, d.PI, d.Ref, x.PI)
		}
	}
	return nil
}

// mqttCheckInventoryCoverage is rule 3: a new exchange or resource cannot omit its inventory.
//
// The rule exists because the OTHER two are about batches that DO carry inventory members. A
// caller that simply omitted them would satisfy both and still install an obligation nothing can
// discover — which is the failure the whole inventory exists to prevent, arrived at by not
// participating rather than by participating wrongly. Consult 5: "a resource creation cannot
// become permitted merely because the caller labels it completion".
//
// WHAT EACH CREATION OWES:
//
//   - an EXCHANGE owes one entry of kind `exchange` naming its nonce;
//   - a RESOURCE owes TWO — `provision` and `cleanup`, both naming its incarnation. The cleanup
//     entry is owed HERE, at provisioning, because it becomes actionable when the last dependent
//     goes, which can happen after closure when allocating an ordinal is exactly what the seal
//     forbids.
//
// An ACTION owes none. It belongs to an exchange that already has an entry, and a duty per
// acquisition would make the inventory grow with retransmissions rather than with obligations.
func mqttCheckInventoryCoverage(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	allocated map[uint64]*mqttDutyEntry) error {

	// KEYED BY GENERATION AS WELL AS KIND AND REF, and the generation is the part that was
	// missing.
	//
	// Without it, a batch admitting exchange E in G1 could carry an OPEN ledger and a
	// consecutive duty naming E in G2. Every other rule passes — the duty and the ledger agree
	// with each other, the interval is exact, the coverage LOOKS present — and yet G1 has no
	// inventory entry for E at all, while a sweep of G2 resolves the reference under the wrong
	// generation. The entries must cover the thing THIS batch created, in the generation it
	// created it in.
	type coverKey struct {
		gen  string
		kind mqttDutyKind
		ref  string
	}
	covered := map[coverKey]struct{}{}
	for _, d := range allocated {
		covered[coverKey{gen: d.Gen, kind: d.Kind, ref: d.Ref}] = struct{}{}
	}
	has := func(gen string, k mqttDutyKind, ref string) bool {
		_, ok := covered[coverKey{gen: gen, kind: k, ref: ref}]
		return ok
	}

	idHash := getHash(a.ID)
	for _, e := range extra {
		// CREATE-ONLY MEMBERS ONLY. A continuation advances something that was covered when it
		// was created, and demanding coverage again would demand a second entry for one duty.
		if e.expect != 0 {
			continue
		}
		cls, ok := mqttF105ObligationClass(e.subj)
		if !ok {
			continue
		}
		rest := e.subj[len(mqttF105SubjectPrefix()):]
		toks := strings.Split(rest, ".")
		switch cls {
		case mqttExchangeClassTk:
			if len(toks) != 4 || toks[1] != idHash {
				continue
			}
			if !has(toks[2], mqttDutyExchange, toks[3]) {
				return fmt.Errorf("transition %q admits obligation %q in generation %q with no "+
					"duty entry naming it THERE: an obligation nothing can discover is one no "+
					"recovery pass will ever resume, and an entry filed under another generation "+
					"covers a different inventory", a.Txn, toks[3], toks[2])
			}
		case mqttResourceClassTk:
			if len(toks) != 4 || toks[1] != idHash {
				continue
			}
			gen, inc := toks[2], toks[3]
			// THE KIND DECIDES WHICH DUTIES ARE OWED, so the body is read rather than assumed.
			//
			// Every kind this generation PROVISIONS owes both entries. (A shared group durable
			// is not a resource of this authority at all: it is held by acquisitions counted in
			// its group record and released by the shared closing, shared_lifecycle_ops.go.)
			r, derr := mqttDecodeOutboundResource(e.body, a.ID, a.Epoch, gen, inc)
			if derr != nil {
				return fmt.Errorf("transition %q creates resource %q whose body this broker "+
					"cannot interpret, so the duties it owes cannot be decided: %w",
					a.Txn, inc, derr)
			}
			// AN AFFINE CONSUMER OWES ONLY ITS CLEANUP, for the same reason a group durable
			// does: it already exists, so it is enrolled rather than provisioned, and a
			// provisioning duty would send an executor to create something that is not this
			// enrolment's to create. It differs in the other direction — its cleanup is
			// unconditional, because no other session can hold it — and that difference lives in
			// the executor, not here.
			if r.Kind == mqttResourceAffineConsumer {
				if has(gen, mqttDutyProvision, inc) {
					return fmt.Errorf("transition %q enrols affine consumer %q with a provisioning "+
						"duty: the consumer already exists, and an entry naming work nobody may "+
						"do is worse than no entry at all", a.Txn, inc)
				}
				if !has(gen, mqttDutyCleanup, inc) {
					return fmt.Errorf("transition %q enrols affine consumer %q with no cleanup "+
						"duty entry: the enrolment exists so that a duty comes back for it after "+
						"the session record is gone, and without the entry it names nothing",
						a.Txn, inc)
				}
				continue
			}
			if !has(gen, mqttDutyProvision, inc) {
				return fmt.Errorf("transition %q provisions resource %q with no provisioning "+
					"duty entry: a provisioning operation that became discoverable only once it "+
					"had succeeded would be undiscoverable in exactly the case recovery is for",
					a.Txn, inc)
			}
			if !has(gen, mqttDutyCleanup, inc) {
				return fmt.Errorf("transition %q provisions resource %q with no cleanup duty "+
					"entry: the cleanup becomes actionable when its last dependent goes, which "+
					"may be after closure — and allocating an ordinal then is exactly what the "+
					"seal forbids, so it is reserved now or never", a.Txn, inc)
			}
		}
	}
	return nil
}

// checkDutyContinuation validates an update to an entry that already exists.
//
// The exact standing entry is RE-READ. A continuation decided against a record this boundary
// never saw is a caller's conclusion, and the expectation alone does not carry what the decision
// turned on — the entry's kind, its subject reference and its state are in the body, not in the
// sequence.
func (jsa *mqttJSA) checkDutyContinuation(a *mqttTransitionAttempt, e mqttTransitionMember,
	gen string, ord uint64, next *mqttDutyEntry, extra []mqttTransitionMember) error {

	// THE THREE READINGS ARE PARTITIONED HERE, and absence is decided by the error CODE rather
	// than by a nil record.
	//
	// loadLastMsgFor reports an absent subject as JSNoMessageFoundErr, so an earlier version's
	// `smsg == nil` branch was unreachable — the error arm caught every absence first and
	// reported it as unreadable. That is the same mutually-covering pair this work package
	// already hit once at the register boundary: two branches, one reachable, and no single
	// mutation able to discriminate them. One condition, one rule.
	//
	// BOTH are refusals. An unreadable entry is not an absent one and neither licenses a
	// rewrite — a continuation names a record it claims to be continuing, so failing to find it
	// is never a reason to treat the member as an allocation.
	smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, e.subj)
	if (lerr != nil && !isErrorOtherThan(lerr, JSNoMessageFoundErr)) || (lerr == nil && smsg == nil) {
		return fmt.Errorf("transition %q continues duty %d on %q at sequence %d, but nothing "+
			"stands there: a duty is allocated create-only and continued thereafter, and a "+
			"continuation over an absent entry would invent the duty it claims to advance",
			a.Txn, ord, e.subj, e.expect)
	}
	if lerr != nil {
		return fmt.Errorf("transition %q continues duty %d on %q, which this broker could not "+
			"read: %w", a.Txn, ord, e.subj, lerr)
	}
	if smsg.Sequence != e.expect {
		return fmt.Errorf("transition %q continues duty %d on %q at sequence %d, but it stands "+
			"at %d: the continuation was decided against a record that is no longer there",
			a.Txn, ord, e.subj, e.expect, smsg.Sequence)
	}
	cur, derr := mqttDecodeDutyEntry(smsg.Data, a.ID, a.Epoch, gen, ord)
	if derr != nil {
		return fmt.Errorf("transition %q continues duty %d on %q, whose standing entry this "+
			"broker cannot interpret: %w", a.Txn, ord, e.subj, derr)
	}

	// A RELOCATION MARKER IS INERT (F-112): the duty it held lives on at its representative, and
	// continuing the marker would give one duty two states.
	if cur.State == mqttDutyRelocated {
		return fmt.Errorf("transition %q continues duty %d, which is a relocation marker to %d: the "+
			"duty is continued at its representative", a.Txn, ord, cur.To)
	}
	// IDENTITY NEVER MOVES. An entry is predeclared for one thing, and a continuation that
	// re-pointed it would let a batch repurpose an ordinal into a duty nothing reserved — which
	// is the allocation rule evaded rather than obeyed. A representative's provenance is part
	// of that identity (F-112).
	// The identifier an exchange duty names is part of that identity: exclusivity skips the
	// exchanges whose duties name another identifier, so re-pointing it would hide one.
	if next.Kind != cur.Kind || next.Ref != cur.Ref || next.PI != cur.PI || next.From != cur.From {
		return fmt.Errorf("transition %q re-identifies duty %d from %s/%q (identifier %d) to %s/%q "+
			"(identifier %d): an entry is predeclared for one duty and continued as that duty",
			a.Txn, ord, cur.Kind, cur.Ref, cur.PI, next.Kind, next.Ref, next.PI)
	}

	// A TERMINAL DUTY IS IMMUTABLE, and this is ONE rule rather than two.
	//
	// The bounded recovery sweep advances past every ordinal it has established terminal. If a
	// terminal duty could be reopened, that advance would step over work that came back, and no
	// cursor discipline could recover it — the sweep would have no reason to look again. The
	// classified outcome is the other half of the same fact: it is what MAKES the duty terminal,
	// so rewriting it re-decides an ending a sweep has already relied on.
	//
	// Written as one condition because an earlier version wrote two and a mutation showed they
	// were not separable. Disabling the reopen check alone killed nothing: a reopened entry must
	// drop its outcome, since a non-terminal entry carrying one is refused by the validator, so
	// the outcome check caught every reopen anyway — and a reader hitting it would have been
	// told "an ending is recorded once" about a batch whose actual defect was reopening the
	// duty. Two mechanisms stating one fact, with the wrong one doing the talking.
	if mqttDutyResolved(cur.State) && (!mqttDutyResolved(next.State) || next.Outcome != cur.Outcome) {
		return fmt.Errorf("transition %q alters duty %d, which ended as %q with state %d -> %d "+
			"and outcome %q: an ending is recorded once, and a duty a recovery pass has already "+
			"advanced past may never owe work again",
			a.Txn, ord, cur.Outcome, cur.State, next.State, next.Outcome)
	}

	// AND REACHING TERMINAL IS PROVED, NOT ASSERTED.
	//
	// This is the rule the immutability above was quietly standing in for, and it does not.
	// Immutability preserves an ending; it says nothing about whether the ending was earned. A
	// batch could move a PENDING or NOT_YET_ACTIONABLE duty straight to TERMINAL with an
	// outcome of its choosing, never touching the exchange or resource the duty is about — and
	// then every sweep passes that ordinal forever and the generation can retire while the
	// obligation is still owed. It is "a count can be decremented by a transition that ended
	// nothing", one level up from the count it was first written about.
	if !mqttDutyResolved(cur.State) && mqttDutyResolved(next.State) {
		if err := jsa.checkDutyEndingIsEarned(a, gen, next, extra); err != nil {
			return err
		}
	}
	return nil
}

// checkDutyEndingIsEarned requires the thing a duty is about to have actually ended.
//
// Each kind has its own evidence and they are not interchangeable:
//
//   - an EXCHANGE duty ends when its obligation reaches a durable ending — the same fact, read
//     the same way, that releasing a dependency edge requires;
//   - a PROVISIONING duty ends when the resource's provisioning is SEALED, which is the positive
//     fact that nothing further can materialize it;
//   - a CLEANUP duty ends when the resource records its classified cleanup outcome. Not when
//     deletion is merely authorized: "a successful delete is not a completed cleanup" is this
//     mechanism's own rule, and CLEANUP_COMPLETE is where the classification lives.
//
// An unreadable or absent subject is a REFUSAL. A duty claiming an ending this broker cannot
// corroborate is exactly the assertion this check exists to replace.
func (jsa *mqttJSA) checkDutyEndingIsEarned(a *mqttTransitionAttempt, gen string,
	d *mqttDutyEntry, extra []mqttTransitionMember) error {

	switch d.Kind {
	case mqttDutyExchange:
		x, _, xerr := jsa.loadOutboundExchange(a.ID, a.Epoch, gen, d.Ref)
		if xerr != nil || x == nil {
			return fmt.Errorf("transition %q ends duty %d, whose obligation %q this broker "+
				"cannot read: an ending nothing corroborates is an assertion (%v)",
				a.Txn, d.Ord, d.Ref, xerr)
		}
		if !mqttExchangeDeliveryDutyEnded(x.Phase) {
			return fmt.Errorf("transition %q ends duty %d while its obligation %q stands at "+
				"phase %d: a duty ends when the thing it is about ends, and a pending-drain "+
				"phase is not an ending", a.Txn, d.Ord, d.Ref, int(x.Phase))
		}
		// AND, AT AN IDENTIFIER ENDING, WHEN NO HOLD OF IT IS LISTED in the state this batch
		// leaves (outbound_release_holds.go). An acknowledgement ending leaves its holds
		// release-owed, and this pending duty is the durable record of that debt: written
		// terminal beside a listed hold, the debt would be forgotten. Judged per hold on the
		// resource as the batch leaves it — its own member, validated as a step by
		// mqttCheckResourceStep, or the stored record — so the batch that removes the last edge
		// may resolve, and one that leaves another hold listed may not. A stored absence is
		// stable: no batch adds an edge for an obligation it does not create.
		if mqttExchangeReleasesHolds(x.Phase) {
			for _, h := range x.Holds {
				r, rerr := jsa.resourceAsBatchLeavesIt(a, gen, h.Inc, extra)
				if rerr != nil || r == nil {
					return fmt.Errorf("transition %q ends duty %d, whose obligation %q holds resource "+
						"%q, which this broker cannot read: the release it owes is not corroborated (%v)",
						a.Txn, d.Ord, d.Ref, h.Inc, rerr)
				}
				if mqttHoldListed(r, d.Ref, h.Role) {
					return fmt.Errorf("transition %q ends duty %d while resource %q still lists obligation "+
						"%q's hold in role %q: an ended obligation's duty ends when its holds are released",
						a.Txn, d.Ord, h.Inc, d.Ref, h.Role)
				}
			}
		}

	case mqttDutyProvision:
		r, _, rerr := jsa.loadOutboundResource(a.ID, a.Epoch, gen, d.Ref)
		if rerr != nil || r == nil {
			return fmt.Errorf("transition %q ends the provisioning duty %d, whose resource %q "+
				"this broker cannot read: %v", a.Txn, d.Ord, d.Ref, rerr)
		}
		// A LOGICAL-USE kind (source, PUBREL) ends its provisioning duty WITHOUT a seal: its
		// create is unfenceable, so there is no "nothing further can materialize it" fact to
		// record, and its name is arbitrated by the ownership record instead (consult 4). The
		// duty is discharged by the claim that rode its provisioning batch; readiness for use is
		// a live admission gate, not this duty. The seal keeps its meaning for every other kind.
		if !mqttResourceLogicalUseKind(r.Kind) && r.Prov != mqttProvisioningSealed {
			return fmt.Errorf("transition %q ends the provisioning duty %d while resource %q "+
				"carries provisioning status %d: the duty ends when nothing further can "+
				"materialize the resource, which is what sealing records",
				a.Txn, d.Ord, d.Ref, r.Prov)
		}

	case mqttDutyCleanup:
		// THE RESOURCE AS THIS BATCH LEAVES IT, not merely as it stands.
		//
		// A batch is ATOMIC, so the completion that earns this ending may be a MEMBER of the same
		// batch: the executor records the classified outcome and ends the duty together,
		// precisely so no crash window sits between a cleanup that is finished and a duty that
		// says otherwise. A rule reading only the stored record would refuse that batch — the
		// stored resource is still DELETE_AUTHORIZED while it is being validated.
		//
		// What the batch's own member may NOT do is invent support: the member is validated as a
		// legal step from the stored record at its CAS expectation by mqttCheckResourceStep, and
		// only after that does it count here. So a terminal duty beside a completion for ANOTHER
		// resource is refused by the binding below — each member is independently legal, and only
		// the pairing is wrong, which is exactly the case the stored-only rule could not see.
		r, rerr := jsa.resourceAsBatchLeavesIt(a, gen, d.Ref, extra)
		if rerr != nil {
			return fmt.Errorf("transition %q ends the cleanup duty %d, whose resource %q this "+
				"broker cannot read: %v", a.Txn, d.Ord, d.Ref, rerr)
		}
		if r == nil {
			return fmt.Errorf("transition %q ends the cleanup duty %d, and neither this batch nor "+
				"the store carries resource %q: an ending nothing corroborates is an assertion",
				a.Txn, d.Ord, d.Ref)
		}
		if r.State != mqttResourceCleanupComplete {
			return fmt.Errorf("transition %q ends the cleanup duty %d while resource %q stands "+
				"in state %d: a successful delete is not a completed cleanup, and the classified "+
				"outcome is what says the duty is done", a.Txn, d.Ord, d.Ref, r.State)
		}
	}
	return nil
}

// checkInventoryAllocation validates the ledger CAS against the ordinals a batch allocates.
//
// This is rule 1, and it is stated in both directions: allocations require the ledger, and a
// ledger that moves its high-water requires the matching allocations. Either alone would leave
// the other free to drift.
func (jsa *mqttJSA) checkInventoryAllocation(a *mqttTransitionAttempt,
	ledger *mqttBatchLedgerMember, allocated map[uint64]*mqttDutyEntry) error {

	if ledger == nil {
		if len(allocated) > 0 {
			// THE ABSENCE IS THE VIOLATION. This is the shape the earlier design let through:
			// a create-only duty member in a batch that simply omits the ledger, so every rule
			// conditioned on the ledger's contents had nothing to read.
			return fmt.Errorf("transition %q allocates %d duty entr(ies) with no generation "+
				"ledger member: an ordinal is allocated BY the ledger CAS, and a batch that "+
				"writes an entry without it has taken a position nothing reserved",
				a.Txn, len(allocated))
		}
		return nil
	}

	// AND AN ALLOCATION NEEDS A GENERATION STILL ABLE TO TAKE ONE.
	//
	// The seal is the usual answer, and it is not the whole answer: it only speaks for a
	// generation that HAS a ledger. A generation closed before it ever allocated anything has
	// none, so a later batch — a disposal, which is permitted to name an earlier generation —
	// could create one create-only and allocate into a generation whose permanent closure says
	// it admits nothing. Closure is the last act able to take an ordinal; that is one fact, so
	// it is stated once, here, where ordinals are taken.
	//
	// The closing batch itself is unaffected: it writes its closure in the same act, so no
	// closure stands while this runs. An unreadable closure REFUSES, for the reason
	// checkProposedGenerationIsFresh refuses: what licenses the allocation is the ABSENCE of a
	// closure, and a failed read establishes no absence.
	if len(allocated) > 0 {
		c, _, cst := jsa.loadGenClosure(a.ID, a.Epoch, ledger.gen)
		switch cst {
		case mqttOutboundAbsent:
		case mqttOutboundValid:
			return fmt.Errorf("transition %q allocates %d duty entr(ies) in generation %q, which "+
				"transition %q closed permanently: closure is the last act able to take an "+
				"ordinal, and an inventory nothing can add to is what makes its completeness a "+
				"fact", a.Txn, len(allocated), ledger.gen, c.Txn)
		default:
			return fmt.Errorf("transition %q allocates in generation %q whose permanent closure "+
				"could not be read: an allocation rests on the ABSENCE of one, which a failed "+
				"read does not establish", a.Txn, ledger.gen)
		}
	}

	cur, curSeq, lerr := jsa.loadGenLedger(a.ID, a.Epoch, ledger.gen)
	if lerr != nil {
		return fmt.Errorf("transition %q carries a ledger CAS for generation %q, which this "+
			"broker could not read: %w", a.Txn, ledger.gen, lerr)
	}
	// ONLY THE FLOOR'S KIND MOVES THE FLOOR OR THE WATERMARK (F-112). An allocation and a seal
	// carry both unchanged; a fresh ledger starts both at 1.
	wantLo, wantDel := uint64(1), uint64(1)
	if cur != nil {
		wantLo, wantDel = cur.Lo, cur.Del
	}
	if ledger.next.Lo != wantLo || ledger.next.Del != wantDel {
		return fmt.Errorf("transition %q of kind %q moves generation %q's floor or deletion watermark "+
			"(%d/%d -> %d/%d): only an advance-floor transition does", a.Txn, a.Kind, ledger.gen,
			wantLo, wantDel, ledger.next.Lo, ledger.next.Del)
	}
	if cur == nil {
		// A generation's FIRST allocation creates its ledger, so a create-only expectation is
		// legitimate here; anything else names a record that is not there.
		if ledger.expect != 0 {
			return fmt.Errorf("transition %q conditions generation %q's ledger on sequence %d, "+
				"but no ledger stands there", a.Txn, ledger.gen, ledger.expect)
		}
		return mqttCheckAllocationInterval(a, 0, ledger.next, allocated)
	}
	if ledger.expect == 0 {
		return fmt.Errorf("transition %q creates a ledger for generation %q create-only while "+
			"one already stands at sequence %d", a.Txn, ledger.gen, curSeq)
	}
	if ledger.expect != curSeq {
		return fmt.Errorf("transition %q conditions generation %q's ledger on sequence %d, but "+
			"it stands at %d: the allocation was computed against a high-water that has moved",
			a.Txn, ledger.gen, ledger.expect, curSeq)
	}
	// SEALING FORBIDS ALLOCATION, and that is the entire fence. No exchange is rewritten by a
	// closure; the only thing it has to stop is an ordinal being taken, and an ordinal is taken
	// only here.
	if cur.State == mqttGenLedgerSealed && len(allocated) > 0 {
		return fmt.Errorf("transition %q allocates %d duty entr(ies) in generation %q, whose "+
			"inventory was sealed at %d: after sealing the inventory's SHAPE is fixed and only "+
			"the states of its entries move", a.Txn, len(allocated), ledger.gen, cur.SealedHi)
	}
	if cur.State == mqttGenLedgerSealed && ledger.next.State != mqttGenLedgerSealed {
		return fmt.Errorf("transition %q reopens the sealed inventory of generation %q: a seal "+
			"is what every completion after it relies on, and it is not revocable",
			a.Txn, ledger.gen)
	}
	return mqttCheckAllocationInterval(a, cur.Hi, ledger.next, allocated)
}

// mqttCheckAllocationInterval requires a batch's allocations to be exactly the consecutive
// interval its ledger declares.
//
// CONTIGUITY IS ENFORCED, NOT ASSUMED. An earlier version of this design argued that CAS
// contention alone made holes impossible; it does not. Contention decides WHO writes, not WHAT
// they write, so nothing stopped a composer from stepping the high-water by three and writing
// entries at two of the ordinals — leaving a gap that a cursor reads as the end of the
// inventory, and a duty that no sweep will ever visit.
func mqttCheckAllocationInterval(a *mqttTransitionAttempt, curHi uint64, next *mqttGenLedger,
	allocated map[uint64]*mqttDutyEntry) error {

	if next.Hi < curHi {
		return fmt.Errorf("transition %q moves generation %q's high-water backwards from %d to "+
			"%d: an ordinal that has been handed out is never handed out again",
			a.Txn, next.Gen, curHi, next.Hi)
	}
	want := next.Hi - curHi
	if uint64(len(allocated)) != want {
		return fmt.Errorf("transition %q moves generation %q's high-water from %d to %d, which "+
			"allocates %d ordinal(s), but the batch carries %d duty entr(ies): the ledger and "+
			"the entries state one fact and they must state the same one",
			a.Txn, next.Gen, curHi, next.Hi, want, len(allocated))
	}
	for ord := curHi + 1; ord <= next.Hi; ord++ {
		if _, ok := allocated[ord]; !ok {
			return fmt.Errorf("transition %q allocates the interval %d..%d but carries no entry "+
				"at ordinal %d: a gap is a duty no sweep will visit, since a cursor reaching it "+
				"cannot tell a hole from the end of the inventory",
				a.Txn, curHi+1, next.Hi, ord)
		}
	}
	return nil
}

// mqttParseLedgerSubject reads a ledger subject's generation, refusing anything else.
func mqttParseLedgerSubject(subject, wantIDHash string) (string, error) {
	rest, ok := strings.CutPrefix(subject, mqttF105SubjectPrefix()+mqttLedgerClassTk+".")
	if !ok {
		return _EMPTY_, fmt.Errorf("subject is not a generation ledger")
	}
	toks := strings.Split(rest, ".")
	if len(toks) != 2 {
		return _EMPTY_, fmt.Errorf("ledger subject has %d tokens past its class (want 2)", len(toks))
	}
	if toks[0] != wantIDHash {
		return _EMPTY_, fmt.Errorf("ledger subject names client hash %q, not %q", toks[0], wantIDHash)
	}
	if !mqttValidSessGen(toks[1]) {
		return _EMPTY_, fmt.Errorf("malformed generation token %q", toks[1])
	}
	return toks[1], nil
}

// mqttParseDutySubject reads a duty subject's generation and ordinal, refusing anything else.
func mqttParseDutySubject(subject, wantIDHash string) (string, uint64, error) {
	rest, ok := strings.CutPrefix(subject, mqttF105SubjectPrefix()+mqttDutyClassTk+".")
	if !ok {
		return _EMPTY_, 0, fmt.Errorf("subject is not a duty entry")
	}
	toks := strings.Split(rest, ".")
	if len(toks) != 3 {
		return _EMPTY_, 0, fmt.Errorf("duty subject has %d tokens past its class (want 3)", len(toks))
	}
	if toks[0] != wantIDHash {
		return _EMPTY_, 0, fmt.Errorf("duty subject names client hash %q, not %q", toks[0], wantIDHash)
	}
	if !mqttValidSessGen(toks[1]) {
		return _EMPTY_, 0, fmt.Errorf("malformed generation token %q", toks[1])
	}
	ord, err := mqttParseOrdinal(toks[2])
	if err != nil {
		return _EMPTY_, 0, err
	}
	return toks[1], ord, nil
}

// loadGenLedger reads one generation's ledger, returning a nil record when none stands.
//
// The three outcomes are kept apart for the reason every read in this package keeps them apart:
// ABSENT is a positive answer here — a generation that has allocated nothing has no ledger — and
// UNREADABLE is not absence but a refusal, because a ledger this broker cannot interpret may be
// the one carrying the high-water an allocation is about to step.
func (jsa *mqttJSA) loadGenLedger(id, epoch, gen string) (*mqttGenLedger, uint64, error) {
	subj := mqttGenLedgerSubject(getHash(id), gen)
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, fmt.Errorf("reading the ledger of generation %q: %w", gen, err)
		}
		return nil, 0, nil
	}
	if smsg == nil {
		return nil, 0, nil
	}
	l, derr := mqttDecodeGenLedger(smsg.Data, id, epoch, gen)
	if derr != nil {
		return nil, 0, fmt.Errorf("the ledger of generation %q cannot be interpreted: %w", gen, derr)
	}
	return l, smsg.Sequence, nil
}

// resourceAsBatchLeavesIt returns the resource this batch RESULTS IN for one incarnation: the
// batch's own member when it carries one for exactly that (generation, incarnation), and the
// stored record otherwise.
//
// The member is not trusted on its own — mqttCheckResourceStep validates it as a legal step from
// the stored record at its CAS expectation — so what this adds is only the binding: which
// resource the batch is actually about. That is the S-xor-Delta property this project already
// names (feedback_batch_is_not_the_whole_state), applied to the one rule that reads a resource to
// judge a duty.
func (jsa *mqttJSA) resourceAsBatchLeavesIt(a *mqttTransitionAttempt, gen, inc string,
	extra []mqttTransitionMember) (*mqttOutboundResource, error) {

	want := mqttResourceSubject(getHash(a.ID), gen, inc)
	for _, e := range extra {
		if e.subj != want {
			continue
		}
		r, derr := mqttDecodeOutboundResource(e.body, a.ID, a.Epoch, gen, inc)
		if derr != nil {
			return nil, fmt.Errorf("this batch carries an unsupported resource on %q: %w", e.subj, derr)
		}
		return r, nil
	}
	r, _, rerr := jsa.loadOutboundResource(a.ID, a.Epoch, gen, inc)
	if rerr != nil {
		return nil, rerr
	}
	return r, nil
}
