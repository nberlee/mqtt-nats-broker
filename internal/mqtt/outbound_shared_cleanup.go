// Copyright 2026 The mqtt-nats-bridge Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"sort"
	"sync/atomic"
)

// Deferred cleanup of an enrolled shared group durable (F-105, row 8).
//
// # Why the live scan cannot be reused
//
// `sharedDurablesHeldByOthers` answers for a session that is tearing ITSELF down, and two of its
// steps say so: it reads holders out of its own `sharedSubs` map, and it SKIPS every persisted
// record carrying its own client ID, on the stated grounds that the in-memory check above covers
// it.
//
// Both are wrong for a duty executed later. There is no live session, so the in-memory half is
// empty; and a record standing under the same client ID is then not this session's stale copy
// but a SUCCESSOR's — a reconnected client that legitimately holds the group. Skipping it reports
// "no other holder" and deletes a consumer that is serving somebody.
//
// So the deferred scan distinguishes by GENERATION rather than by client: the only record it may
// disregard is the retiring generation's own, and every other record — same client or not — is
// counted as a holder if its membership owns the durable.
//
// # Fail-closed, for the same reason as the live scan
//
// The answer authorizes an IRREVERSIBLE delete of a consumer whose backlog is interest-retained.
// "No other holder" may only be reported out of a record set this walk read IN FULL, so any
// record it cannot read, and any error that is not "no more messages", reports every durable HELD
// and the deletion is withheld. Spec5 [4.8.2].
type mqttSharedHolderScan struct {
	// Held is keyed by durable name. True means somebody may still be using it, which
	// INCLUDES every unresolved reading.
	Held map[string]bool
	// Complete says whether the walk read the whole record set. A false value makes every
	// entry in Held meaningless except as a refusal, and it is carried separately so a caller
	// cannot mistake a fail-closed answer for a measured one.
	Complete bool
}

// sharedDurablesHeldAfterRetirement reports which of durables another session may still hold,
// for a cleanup running after the retiring generation's own record is gone.
//
// retiringGen names the generation being reclaimed. A record of THIS client in THAT generation is
// the one being torn down and is not a holder; a record of this client in any other generation is
// a successor and IS one.
func (jsa *mqttJSA) sharedDurablesHeldAfterRetirement(id, retiringGen string,
	durables []string) mqttSharedHolderScan {

	// COUNTED, so a test can assert that a recovery pass performs NONE of these. The walk reads
	// every persisted session record, and excluding a shared cleanup by KIND before the duty
	// classifier runs is worth nothing unless it is measurable.
	if h := mqttTestSharedHolderScanHookV.Load(); h != nil {
		(*h)()
	}
	scan := mqttSharedHolderScan{Held: make(map[string]bool, len(durables))}
	uniq := make([]string, 0, len(durables))
	for _, d := range durables {
		if _, seen := scan.Held[d]; !seen {
			scan.Held[d] = false
			uniq = append(uniq, d)
		}
	}
	holdAll := func() {
		for _, d := range uniq {
			scan.Held[d] = true
		}
	}
	markOwned := func(key string) {
		for _, d := range uniq {
			if !scan.Held[d] && mqttSharedKeyOwnsDurable(key, d) {
				scan.Held[d] = true
			}
		}
	}
	if len(uniq) == 0 {
		scan.Complete = true
		return scan
	}

	filter := mqttSessStreamSubjectPrefix + jsa.domainToken() + ">"
	for seq := uint64(1); ; {
		smsg, err := jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			// ONLY "no more messages" ends the walk with an answer. Anything else is a reading
			// this broker could not complete, and an incomplete reading of a record set is not
			// a measurement of it.
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				holdAll()
				return scan
			}
			scan.Complete = true
			return scan
		}
		seq = smsg.Sequence + 1
		ps := &mqttPersistedSession{}
		if derr := json.Unmarshal(smsg.Data, ps); derr != nil {
			holdAll()
			return scan
		}
		// THE BODY'S IDENTITY IS THE SUBJECT'S, and that is ONE rule rather than two.
		//
		// The skip below is the only record this cleanup may disregard, and it is decided from
		// the BODY; a record standing on another client's subject whose body claims the retiring
		// identity would be skipped while its SharedSubs named the very durable about to be
		// deleted. Attribution is by subject, the same rule the register boundary applies: a
		// record disagreeing with its own name can be read by nobody, and a holder scan may not
		// resolve that disagreement in the direction that permits a deletion.
		//
		if smsg.Subject != mqttSessStreamSubjectPrefix+jsa.domainToken()+getHash(ps.ID) {
			holdAll()
			return scan
		}
		// AND A DECODE THAT SUCCEEDS IS STILL NOT AN IDENTITY. `null` and `{}` unmarshal without
		// error into a record naming nobody, which contributes no holders while the walk calls
		// itself complete.
		//
		// This check was deleted once, on the reasoning that a body naming nobody cannot be
		// standing at the subject of somebody — and its mutation survived, which looked like
		// confirmation. It is wrong: getHash(_EMPTY_) is a perfectly good subject token, so a
		// record at THAT address passes the comparison above with an empty identity and is
		// counted as read. The two checks answer different questions — one asks whether the
		// record is where its contents say it is, this one asks whether its contents say
		// anything — and the mutation survived only because nothing had supplied the input that
		// separates them.
		if ps.ID == _EMPTY_ {
			holdAll()
			return scan
		}
		// A SHAPE THIS BROKER NEVER WRITES is refused BEFORE any skip, exactly as the live scan
		// does: holders are read out of SharedSubs alone, so a record carrying shared group
		// consumers in Cons hides the very holder this cleanup is about to delete.
		if _, found := mqttSharedConsInRecord(ps); found {
			holdAll()
			return scan
		}
		// THE ONLY RECORD THIS CLEANUP MAY DISREGARD is the one it is cleaning up after.
		if ps.ID == id && ps.Gen == retiringGen {
			continue
		}
		for key := range ps.SharedSubs {
			markOwned(key)
		}
	}
}

// mqttSharedCleanupVerdict says what a shared group durable's cleanup duty may do right now.
type mqttSharedCleanupVerdict uint8

const (
	// mqttSharedCleanupUnknown is the zero value and a refusal: nothing was established.
	mqttSharedCleanupUnknown mqttSharedCleanupVerdict = iota
	// mqttSharedCleanupDelete: no other session holds it and the record set was read in full.
	mqttSharedCleanupDelete
	// mqttSharedCleanupWithhold: somebody may still hold it. The duty stays outstanding.
	mqttSharedCleanupWithhold
)

// classifySharedCleanup decides one enrolled shared durable's cleanup, RE-ASKING the question
// every time it is called.
//
// A teardown's earlier answer never authorizes a later deletion: the group's membership changes
// while the duty waits, and a session that joined after the teardown holds the consumer just as
// firmly as one that was there before it. An unresolved reading WITHHOLDS.
func (jsa *mqttJSA) classifySharedCleanup(id, retiringGen,
	durable string) (mqttSharedCleanupVerdict, string) {

	scan := jsa.sharedDurablesHeldAfterRetirement(id, retiringGen, []string{durable})
	if !scan.Complete {
		return mqttSharedCleanupWithhold, fmt.Sprintf("the session records could not be read in "+
			"full, so nothing can be said about who still holds group durable %q", durable)
	}
	if scan.Held[durable] {
		return mqttSharedCleanupWithhold, fmt.Sprintf("another session still holds group durable "+
			"%q, and deleting it would take a consumer that is serving somebody", durable)
	}
	return mqttSharedCleanupDelete, _EMPTY_
}

// mqttSharedEnrolment is the batch members that enrol one generation's held shared group
// durables, for the transition that CLOSES it.
//
// # Why the closing transition and not the acquisition
//
// The enrolment has to outlive the session record, because the record is what names the durables
// and the metadata release is what deletes it. A cleanup duty is the thing that outlives it —
// and a duty needs an ORDINAL, which only the ledger allocates, and which the SEAL forbids
// allocating after. Closure is the last act that can take one.
//
// # Why the membership is captured rather than re-read
//
// The names come from the persisted record the accepted batch DISPLACED — the last membership the
// retiring session could possibly have written, because the record CAS that displaced it is what
// stops the next one. A fresh read afterwards would be a second act with a window before it; the
// displaced record has none.
type mqttSharedEnrolment struct {
	// Members are the resource, duty and ledger members to add to the closing batch.
	Members []mqttTransitionMember
	// Incs names the resource incarnation minted for each durable, in the same order as the
	// durables given, so a caller can report what it enrolled.
	Incs map[string]string
}

// mqttEnrolSharedDurables builds the enrolment for one closing transition.
//
// gen is the generation being CLOSED — which is not always the transition's own generation, since
// a discarding finalization installs its successor while closing its predecessor. cur is that
// generation's standing ledger, nil when it has none, and curSeq the sequence it was read at.
//
// The ledger member SEALS as it allocates, in the one act, because that is what makes the
// enrolment's completeness establishable: after the seal no further ordinal can be taken, so the
// set of duties naming this generation's durables is fixed by the same batch that wrote them.
func mqttEnrolSharedDurables(id, epoch, gen string, durables []string, cur *mqttGenLedger,
	curSeq uint64) (*mqttSharedEnrolment, error) {

	if !mqttValidSessGen(gen) {
		return nil, fmt.Errorf("enrolling shared durables under malformed generation %q", gen)
	}
	// DEDUPED AND ORDERED, so two members of the same group produce one enrolment and the
	// ordinals a batch takes do not depend on map iteration.
	seen := map[string]struct{}{}
	uniq := make([]string, 0, len(durables))
	for _, d := range durables {
		if d == _EMPTY_ {
			return nil, fmt.Errorf("enrolling an unnamed shared durable in generation %q", gen)
		}
		if _, dup := seen[d]; dup {
			continue
		}
		seen[d] = struct{}{}
		uniq = append(uniq, d)
	}
	sort.Strings(uniq)

	hi := uint64(0)
	if cur != nil {
		if cur.State == mqttGenLedgerSealed {
			return nil, fmt.Errorf("generation %q's inventory is already sealed at %d, so no "+
				"enrolment can take an ordinal in it", gen, cur.SealedHi)
		}
		hi = cur.Hi
	}
	if cur == nil && curSeq != 0 {
		return nil, fmt.Errorf("enrolling in generation %q against sequence %d with no ledger "+
			"standing there", gen, curSeq)
	}
	if cur != nil && curSeq == 0 {
		return nil, fmt.Errorf("enrolling in generation %q requires the sequence its ledger was "+
			"read at", gen)
	}

	idHash := getHash(id)
	out := &mqttSharedEnrolment{Incs: make(map[string]string, len(uniq))}
	for _, dur := range uniq {
		hi++
		inc := mqttNewInflightNonce()
		out.Incs[dur] = inc
		r := &mqttOutboundResource{
			ID: id, Epoch: epoch, Gen: gen, Inc: inc,
			Kind: mqttResourceSharedGroupDurable, Name: dur, State: mqttResourceLive,
			// SEALED AT BIRTH, and truthfully: the consumer already exists, so nothing this
			// generation does will materialize it and there is no window for a delayed create
			// to land in. That is the same fact a provisioning seal records, established here
			// by the thing being enrolled rather than by an act that has to happen later.
			Prov: mqttProvisioningSealed,
			Deps: []mqttResourceEdge{},
		}
		rbody, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		d := &mqttDutyEntry{
			ID: id, Epoch: epoch, Gen: gen, Ord: hi,
			Kind: mqttDutyCleanup, Ref: inc, State: mqttDutyNotYetActionable,
		}
		dbody, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		out.Members = append(out.Members,
			mqttTransitionMember{subj: mqttResourceSubject(idHash, gen, inc), expect: 0, body: rbody},
			mqttTransitionMember{subj: mqttDutySubject(idHash, gen, hi), expect: 0, body: dbody},
		)
	}

	next := &mqttGenLedger{
		ID: id, Epoch: epoch, Gen: gen, State: mqttGenLedgerSealed, Hi: hi, SealedHi: hi,
	}
	lbody, err := json.Marshal(next)
	if err != nil {
		return nil, err
	}
	out.Members = append(out.Members, mqttTransitionMember{
		subj: mqttGenLedgerSubject(idHash, gen), expect: curSeq, body: lbody,
	})
	return out, nil
}

// mqttTestSharedHolderScanHookV fires once per account-wide shared-holder walk.
//
// A package-level seam rather than a field, because the walk belongs to the JSA and the JSA holds
// no back-pointer to its account manager — the same shape every other whole-walk seam here takes.
var mqttTestSharedHolderScanHookV atomic.Pointer[func()]

func setMQTTTestSharedHolderScanHook(h func()) func() {
	prev := mqttTestSharedHolderScanHookV.Load()
	if h == nil {
		mqttTestSharedHolderScanHookV.Store(nil)
	} else {
		mqttTestSharedHolderScanHookV.Store(&h)
	}
	return func() { mqttTestSharedHolderScanHookV.Store(prev) }
}
