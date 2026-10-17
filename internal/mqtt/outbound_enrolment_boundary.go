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

// A closing batch enrols its generation for recovery (F-105, row 6).
//
// # Why this is keyed on the CLOSURE and not on the members present
//
// Every rule here could have been written as "if the batch carries a root, then ...". That shape
// admits exactly the bypasses this file exists to close, and they are all OMISSIONS: a closure
// that carries no ledger member at all, over a generation whose ledger stands OPEN; a first
// enrolment that creates its ledger directly SEALED and allocates no root; a closure whose
// recorded root address names an ordinal nothing in the batch writes. A rule that only inspects
// present members says nothing about any of them.
//
// So the entry point is the closure: a transition that writes one is REQUIRED to seal that
// generation's ledger and to allocate exactly one root for it, and the stored state is re-read to
// decide both. A transition that writes no closure may carry neither.
//
// # What it establishes, and what it leaves to the caller
//
// It establishes the ADDRESS and the SHAPE: the ordinal is the stored header's high-water plus
// one, the root's bound is the sealed high-water this batch installs, the identity is this client
// and this account recovery epoch, the cursor is at birth, and the closure's recorded address is
// the root the batch actually writes. Those are facts about records, and the boundary can decide
// every one of them from the batch and the store.
//
// It does NOT establish that the enrolment is COMPLETE — that every durable the retiring session
// held is named. It cannot: the membership lives in a session record the accepted batch has
// already displaced, and `$MQTT_sess` carries MaxMsgsPer 1, so the predecessor's body is evicted
// by the very write that displaces it. Completeness is therefore the closing caller's assertion,
// audited rather than enforced, and recorded as such under "The trusted producer set" in
// docs/ARCHITECTURE.md.

// mqttBatchRootMember is a root member a batch carries, with what the boundary decoded from it.
type mqttBatchRootMember struct {
	subj   string
	expect uint64
	shard  uint64
	ord    uint64
	root   *mqttRecoveryRoot
}

// checkRecoveryEnrolment enforces that a closing batch enrols its generation, and that nothing
// else carries a directory member at all.
func (jsa *mqttJSA) checkRecoveryEnrolment(a *mqttTransitionAttempt, t *mqttAuthorityTransition) error {
	idHash := getHash(a.ID)

	var shardMember *mqttTransitionMember
	var rootMember *mqttBatchRootMember
	for i := range t.Extra {
		e := t.Extra[i]
		switch {
		case mqttIsShardSubject(e.subj):
			if shardMember != nil {
				return fmt.Errorf("transition %q carries two directory shard members: a closure "+
					"allocates one ordinal, in one shard", a.Txn)
			}
			shardMember = &t.Extra[i]
		case mqttIsRootSubject(e.subj):
			if rootMember != nil {
				return fmt.Errorf("transition %q carries two recovery root members: a closure "+
					"enrols one generation", a.Txn)
			}
			shard, ord, perr := mqttParseRootSubject(e.subj)
			if perr != nil {
				return fmt.Errorf("transition %q carries a root member on %q, which this broker "+
					"cannot place: %w", a.Txn, e.subj, perr)
			}
			root, derr := mqttDecodeRecoveryRoot(e.body, shard, ord)
			if derr != nil {
				return fmt.Errorf("transition %q carries an unsupported recovery root on %q: %w",
					a.Txn, e.subj, derr)
			}
			rootMember = &mqttBatchRootMember{
				subj: e.subj, expect: e.expect, shard: shard, ord: ord, root: root,
			}
		}
	}

	if t.Closure == nil {
		// NO CLOSURE, NO DIRECTORY MEMBERS. The per-member check already refuses them, and this
		// states the same fact at the batch level so the rule reads in one direction as well as
		// the other.
		if shardMember != nil || rootMember != nil {
			return fmt.Errorf("transition %q carries a directory member and writes no closure: a "+
				"generation is enrolled for recovery by the act that closes it", a.Txn)
		}
		// AND NO AFFINE RESOURCE EITHER, which the rules below cannot say because they are keyed
		// on the closure and this batch has none.
		//
		// Without this, an arbitrary `provision-resource` batch creates a LIVE, SEALED affine
		// resource with an open ledger and a cleanup duty, skipping the born-PENDING rule and the
		// discarding-disposition rule together; a later NON-discarding close then roots that
		// inventory, and the pass authorizes and deletes the consumer with no readiness and no
		// discard licence behind it. An affine resource is created by the act that CLOSES its
		// generation and abandons it, and by nothing else.
		if inc, found := mqttCreatedAffineResource(a, t, idHash); found {
			return fmt.Errorf("transition %q creates affine consumer resource %q and writes no "+
				"closure: an affine consumer is enrolled only by the closing transition that "+
				"discards its generation", a.Txn, inc)
		}
		// AND NO SEALED LEDGER, for the same reason and with a worse consequence.
		//
		// Sealing is the closing act: the rules below derive the root's bound from the seal THIS
		// batch installs, so a seal without a closure is a seal no root was allocated against. It
		// also WEDGES the generation permanently — `mqttSealGenLedgerMember` refuses an
		// already-sealed ledger, so the normal composers can no longer close it, the inventory
		// cannot be reopened, and no root will ever exist for the pass to discover. A generation
		// that can neither close nor be recovered is exactly the stranding this enrolment exists
		// to prevent.
		// A FLOOR ADVANCE on an already-sealed ledger rewrites it SEALED with only its deletion
		// watermark moved (F-112); checkFloorAdvance refuses it unless the stored ledger was
		// already sealed, so it seals nothing.
		if gen, found := mqttSealsLedgerOutsideAClosure(t, idHash); found && a.Kind != mqttTransitionAdvanceFloor {
			return fmt.Errorf("transition %q seals the inventory ledger of generation %q and "+
				"writes no closure: the seal is the closing act, and a sealed ledger with no "+
				"root can neither be closed nor recovered", a.Txn, gen)
		}
		return nil
	}

	gen := t.Closure.Gen
	// THE SEAL THIS BATCH INSTALLS, and whether it installs one at all. Read from the members
	// rather than from the caller, because the rule below is about what the batch DOES.
	var sealed *mqttGenLedger
	var sealedSubj string
	for _, e := range t.Extra {
		if !mqttIsLedgerSubject(e.subj) {
			continue
		}
		lgen, perr := mqttParseLedgerSubject(e.subj, idHash)
		if perr != nil || lgen != gen {
			continue
		}
		led, derr := mqttDecodeGenLedger(e.body, a.ID, a.Epoch, gen)
		if derr != nil {
			// checkInventoryMembers reports this in its own words; here it only means the batch
			// has no readable seal for the closing generation.
			continue
		}
		if led.State == mqttGenLedgerSealed {
			sealed, sealedSubj = led, e.subj
		}
	}

	// RULE 1, IN BOTH DIRECTIONS. A closure must seal and must root; a root must accompany a
	// seal. Stated from the closure so that omitting the ledger member entirely is caught.
	if sealed == nil {
		return fmt.Errorf("transition %q closes generation %q without sealing its inventory: a "+
			"closed generation whose ledger can still take an ordinal has no fixed shape for any "+
			"sweep to have covered, and the release gate would withhold it for ever", a.Txn, gen)
	}
	if rootMember == nil || shardMember == nil {
		return fmt.Errorf("transition %q seals generation %q and allocates no recovery root: a "+
			"sealed generation nothing can traverse is exactly the abandoned inventory the root "+
			"exists to exclude", a.Txn, gen)
	}
	_ = sealedSubj

	// The ledger it sealed cannot have been sealed already — mqttAllocateInventoryMembers and
	// checkInventoryAllocation both say so — which is what stops a second closure allocating a
	// second root for one generation.

	// RULE 4: THE ROOT'S ADDRESS, IDENTITY AND CURSOR.
	wantShard := mqttDirShardFor(idHash)
	if rootMember.shard != wantShard {
		return fmt.Errorf("transition %q allocates a root for client %q in shard %d, but that "+
			"client hashes to %d, so no sweep would look for it there",
			a.Txn, a.ID, rootMember.shard, wantShard)
	}
	if rootMember.root.ID != a.ID || rootMember.root.Epoch != a.Epoch || rootMember.root.Gen != gen {
		return fmt.Errorf("transition %q allocates a root naming %q/%q/%q, and closes %q/%q/%q",
			a.Txn, rootMember.root.ID, rootMember.root.Epoch, rootMember.root.Gen,
			a.ID, a.Epoch, gen)
	}
	if rootMember.expect != 0 {
		return fmt.Errorf("transition %q allocates root %d/%d at sequence %d: an ordinal is taken "+
			"create-only, and an expectation here would mean this batch believed it was "+
			"continuing a sweep it is in fact starting",
			a.Txn, rootMember.shard, rootMember.ord, rootMember.expect)
	}
	if rootMember.root.SweepStart != 1 || rootMember.root.Resume != 1 || rootMember.root.Unresolved != 0 {
		return fmt.Errorf("transition %q allocates root %d/%d with a cursor that is not at birth "+
			"(start %d, resume %d, unresolved %d): a first sweep begins at ordinal 1 with nothing "+
			"examined", a.Txn, rootMember.shard, rootMember.ord,
			rootMember.root.SweepStart, rootMember.root.Resume, rootMember.root.Unresolved)
	}
	// THE BOUND IS THE SEAL THIS BATCH INSTALLS, duties this batch allocated included. A bound
	// taken before the batch would send the first sweep home one duty early.
	if rootMember.root.Bound != sealed.SealedHi {
		return fmt.Errorf("transition %q allocates root %d/%d bounded at %d while sealing "+
			"generation %q at %d: a root's bound is the inventory this batch fixes",
			a.Txn, rootMember.shard, rootMember.ord, rootMember.root.Bound, gen, sealed.SealedHi)
	}

	// RULE 4a: THE PROPOSED HEADER BODY, because an arbitrary batch never passes through the
	// allocator. Re-read the stored header and require exactly what allocation may do to it.
	stored, storedSeq, herr := jsa.loadDirShardAt(rootMember.shard)
	if herr != nil {
		return fmt.Errorf("transition %q enrols generation %q and its directory shard could not "+
			"be read: %w", a.Txn, gen, herr)
	}
	proposed, derr := mqttDecodeDirShard(shardMember.body, rootMember.shard)
	if derr != nil {
		return fmt.Errorf("transition %q carries an unsupported directory shard on %q: %w",
			a.Txn, shardMember.subj, derr)
	}
	if shardMember.subj != mqttShardSubject(rootMember.shard) {
		return fmt.Errorf("transition %q carries its shard member on %q, which is not shard %d's "+
			"canonical subject", a.Txn, shardMember.subj, rootMember.shard)
	}
	if stored == nil {
		if shardMember.expect != 0 {
			return fmt.Errorf("transition %q writes shard %d at sequence %d with no header "+
				"standing there", a.Txn, rootMember.shard, shardMember.expect)
		}
		if proposed.Hi != 1 || proposed.Next != 1 || proposed.Cycle != 1 || proposed.Horizon != 1 {
			return fmt.Errorf("transition %q initializes shard %d as hi %d, next %d, cycle %d, "+
				"horizon %d: a first allocation creates ordinal 1 and a first cycle over it",
				a.Txn, rootMember.shard, proposed.Hi, proposed.Next, proposed.Cycle, proposed.Horizon)
		}
		if rootMember.ord != 1 {
			return fmt.Errorf("transition %q allocates ordinal %d in a shard with no header: the "+
				"first ordinal is 1", a.Txn, rootMember.ord)
		}
	} else {
		if shardMember.expect != storedSeq {
			return fmt.Errorf("transition %q writes shard %d at sequence %d, but it stands at %d: "+
				"an allocation is decided against the header it read",
				a.Txn, rootMember.shard, shardMember.expect, storedSeq)
		}
		if stored.Hi == ^uint64(0) {
			return fmt.Errorf("transition %q allocates in shard %d, which has taken its last "+
				"ordinal", a.Txn, rootMember.shard)
		}
		if proposed.Hi != stored.Hi+1 {
			return fmt.Errorf("transition %q moves shard %d's high-water from %d to %d: an "+
				"allocation takes exactly one ordinal", a.Txn, rootMember.shard, stored.Hi, proposed.Hi)
		}
		if rootMember.ord != stored.Hi+1 {
			return fmt.Errorf("transition %q allocates ordinal %d against a high-water of %d: "+
				"ordinals are contiguous because a refused batch takes none",
				a.Txn, rootMember.ord, stored.Hi)
		}
		// ALLOCATION IS NOT TRAVERSAL. Moving the position, the cycle or its horizon here would
		// reposition every walker on this shard from a batch that is closing one client's
		// generation.
		if proposed.Next != stored.Next || proposed.Cycle != stored.Cycle ||
			proposed.Horizon != stored.Horizon {
			return fmt.Errorf("transition %q rewrites shard %d's traversal state (next %d->%d, "+
				"cycle %d->%d, horizon %d->%d): allocation takes an ordinal and moves no walker",
				a.Txn, rootMember.shard, stored.Next, proposed.Next, stored.Cycle, proposed.Cycle,
				stored.Horizon, proposed.Horizon)
		}
	}

	// RULE 5: THE CLOSURE RECORDS THE ADDRESS THE BATCH WROTE. The gate loads the root from
	// here, so a closure naming another address would send it to a record this batch never
	// wrote — or to one belonging to a different generation.
	if t.Closure.RootShard != rootMember.shard || t.Closure.RootOrd != rootMember.ord {
		return fmt.Errorf("transition %q closes generation %q naming root %d/%d and allocates "+
			"%d/%d", a.Txn, gen, t.Closure.RootShard, t.Closure.RootOrd,
			rootMember.shard, rootMember.ord)
	}

	// RULE 7a: AN AFFINE RESOURCE IS BORN PENDING. Its cleanup becomes authorizable only once
	// the provisioning seal records that nothing further can materialize it, and that seal
	// requires the readiness assertion. A create-only member carrying SEALED would skip both.
	for _, e := range t.Extra {
		r := mqttCreatedResourceMember(a, e, idHash)
		if r == nil {
			continue
		}
		if r.Kind == mqttResourceAffineConsumer && r.Prov != mqttProvisioningPending {
			return fmt.Errorf("transition %q enrols affine consumer %q with provisioning status "+
				"%d: an affine consumer is enrolled PENDING, because a delayed create may still "+
				"land under its name, and only the readiness assertion licenses the seal",
				a.Txn, r.Inc, r.Prov)
		}
	}
	return nil
}

// mqttCreatedResourceMember decodes a CREATE-ONLY resource member of this client, or nil.
func mqttCreatedResourceMember(a *mqttTransitionAttempt, e mqttTransitionMember,
	idHash string) *mqttOutboundResource {

	if !strings.HasPrefix(e.subj, mqttF105SubjectPrefix()+mqttResourceClassTk+".") || e.expect != 0 {
		return nil
	}
	toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
	if len(toks) != 4 || toks[1] != idHash {
		return nil
	}
	r, rerr := mqttDecodeOutboundResource(e.body, a.ID, a.Epoch, toks[2], toks[3])
	if rerr != nil {
		return nil // checkTransitionMember reports it in its own words
	}
	return r
}

// mqttCreatedAffineResource reports whether this batch CREATES an affine consumer resource.
func mqttCreatedAffineResource(a *mqttTransitionAttempt, t *mqttAuthorityTransition,
	idHash string) (inc string, found bool) {

	for _, e := range t.Extra {
		if r := mqttCreatedResourceMember(a, e, idHash); r != nil &&
			r.Kind == mqttResourceAffineConsumer {
			return r.Inc, true
		}
	}
	return _EMPTY_, false
}

// mqttParseRootSubject splits a recovery root subject back into its shard and ordinal.
func mqttParseRootSubject(subject string) (shard, ord uint64, err error) {
	if !mqttIsRootSubject(subject) {
		return 0, 0, fmt.Errorf("subject %q is not a recovery root", subject)
	}
	toks := strings.Split(subject[len(mqttF105SubjectPrefix()):], ".")
	if len(toks) != 3 {
		return 0, 0, fmt.Errorf("recovery root subject %q is malformed", subject)
	}
	sh, serr := strconv.ParseUint(toks[1], 10, 64)
	if serr != nil || strconv.FormatUint(sh, 10) != toks[1] {
		return 0, 0, fmt.Errorf("recovery root subject %q carries a malformed shard token", subject)
	}
	if sh >= mqttDirShards {
		return 0, 0, fmt.Errorf("recovery root subject %q names shard %d, outside the %d this "+
			"broker addresses", subject, sh, mqttDirShards)
	}
	o, oerr := mqttParseOrdinal(toks[2])
	if oerr != nil {
		return 0, 0, fmt.Errorf("recovery root subject %q carries a malformed ordinal: %w", subject, oerr)
	}
	return sh, o, nil
}

// mqttSealsLedgerOutsideAClosure reports whether a batch carries a ledger member in the SEALED
// state, whether it creates one that way or moves an OPEN one there.
func mqttSealsLedgerOutsideAClosure(t *mqttAuthorityTransition, idHash string) (gen string,
	found bool) {

	for _, e := range t.Extra {
		if !mqttIsLedgerSubject(e.subj) {
			continue
		}
		l := &mqttGenLedger{}
		if err := json.Unmarshal(e.body, l); err != nil {
			continue // checkTransitionMember reports an unreadable member in its own words
		}
		if l.State == mqttGenLedgerSealed {
			return l.Gen, true
		}
	}
	return _EMPTY_, false
}
