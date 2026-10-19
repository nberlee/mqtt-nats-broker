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
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// The authority transition (F-105).
//
// One transition is ONE atomic batch: the permanent outcome that records the attempt's
// verdict, the authority record the transition contends on, and every exchange, dependency
// and inventory mutation the transition authorizes. Each member carries its own
// Nats-Expected-Last-Subject-Sequence, so a failing CAS on any member rejects the whole
// batch and nothing is stored. The binding between members is ATOMICITY, never a
// cross-subject expectation — the same reasoning storeInflightPair and the accept batch
// already rest on, and the reason these records share $MQTT_inflight with the registers.
//
// The outcome member is create-only, and that is what makes an ambiguous attempt decidable.
// An attempt whose acknowledgement is lost cannot be resolved by looking for its effect:
// the authority is a single-slot record, so a later transition overwrites the body the
// first one wrote, and the caller would be reading an absence. Instead the resolver reads
// the attempt's OWN outcome subject, and a caller that still cannot decide publishes an
// ABORTED verdict there create-only. Exactly one of the two writes can win: if the abort
// lands first the original batch's create-only member can never succeed, so the batch is
// refused in its entirety; if the batch landed first the abort fails and the readback
// returns COMMITTED. Either way the answer is a record that EXISTS, never an inference from
// one that does not.
//
// Atomicity commits what it is given. It does not validate MEANING — so this file, not its
// callers, is where every member of every transition is classified and refused.

// mqttOutboundLoadStatus classifies a read of one F-105 record.
//
// Deliberately the same four-way shape the retirement watermark uses, and for the same
// reason: corrupt and indeterminate are not absent. A record this broker cannot interpret
// may describe a tenure newer than the reader's, so every caller refuses the decision that
// depends on it rather than proceeding as though nothing were there.
type mqttOutboundLoadStatus int

const (
	mqttOutboundAbsent        mqttOutboundLoadStatus = iota // no record on the subject
	mqttOutboundValid                                       // decoded and fully validated
	mqttOutboundCorrupt                                     // unsupported shape, or a field that fails validation
	mqttOutboundIndeterminate                               // transient load failure
)

// mqttAuthorityLoad is an authority read together with the sequence a CAS must name.
//
// The sequence is returned on every non-absent outcome because it is available before the
// decode: a writer that must refuse a corrupt body still needs the sequence to say what it
// refused, and a reader that retries needs to contend against the body it actually saw.
type mqttAuthorityLoad struct {
	rec    *mqttOutboundAuthority
	seq    uint64
	status mqttOutboundLoadStatus
}

// loadOutboundAuthority reads one client's authority record.
//
// id and epoch are the EXACT identities the caller is operating as, threaded through to the
// decoder so attribution never rests on the subject's hash alone.
func (jsa *mqttJSA) loadOutboundAuthority(id, epoch string) mqttAuthorityLoad {
	subject := mqttOutboundAuthoritySubject(getHash(id))
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return mqttAuthorityLoad{status: mqttOutboundIndeterminate}
		}
		return mqttAuthorityLoad{status: mqttOutboundAbsent}
	}
	a, derr := mqttDecodeOutboundAuthority(smsg.Data, id, epoch)
	if derr != nil {
		return mqttAuthorityLoad{seq: smsg.Sequence, status: mqttOutboundCorrupt}
	}
	return mqttAuthorityLoad{rec: a, seq: smsg.Sequence, status: mqttOutboundValid}
}

// loadGenClosure reads the permanent closure record for one generation.
//
// ABSENT IS NOT "OPEN". The caller that wants to know whether a generation may still admit
// work asks the authority's positive state and wins a CAS against it; this read answers the
// narrower question of whether closure has been positively established, which is what every
// destructive path needs before it may treat an obligation as ended.
func (jsa *mqttJSA) loadGenClosure(id, epoch, gen string) (*mqttGenClosure, uint64, mqttOutboundLoadStatus) {
	subject := mqttGenClosureSubject(getHash(id), gen)
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, mqttOutboundIndeterminate
		}
		return nil, 0, mqttOutboundAbsent
	}
	c, derr := mqttDecodeGenClosure(smsg.Data, id, epoch, gen)
	if derr != nil {
		return nil, smsg.Sequence, mqttOutboundCorrupt
	}
	return c, smsg.Sequence, mqttOutboundValid
}

// requireDiscardingClosure is the licence of every act a DISCARDED generation's retirement takes
// without an entitlement (F-105 7d): the generation's committed closure, read here, says Discard.
//
// The closure is create-only and its Discard member is immutable, written by the act that ended
// the generation, so a positive answer cannot become false between this read and the write it
// licenses — there is no window to arbitrate. Anything other than a VALID discarding closure
// refuses: an absent closure is a generation still serving or never closed, a non-discarding one
// preserved its obligations, and an unreadable one is not an answer.
func (jsa *mqttJSA) requireDiscardingClosure(id, epoch, gen string) error {
	c, _, st := jsa.loadGenClosure(id, epoch, gen)
	switch {
	case st != mqttOutboundValid:
		return fmt.Errorf("the closure of generation %q reads %v, and only a committed discarding "+
			"closure licenses its retirement", gen, st)
	case !c.Discard:
		return fmt.Errorf("generation %q was closed preserving its obligations, which licenses no "+
			"discard", gen)
	}
	return nil
}

// loadTransitionOutcome reads one attempted transition's permanent verdict.
func (jsa *mqttJSA) loadTransitionOutcome(id, epoch, txn string) (*mqttTransitionOutcome, uint64, mqttOutboundLoadStatus) {
	subject := mqttTransitionOutcomeSubject(getHash(id), txn)
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, mqttOutboundIndeterminate
		}
		return nil, 0, mqttOutboundAbsent
	}
	o, derr := mqttDecodeTransitionOutcome(smsg.Data, id, epoch, txn)
	if derr != nil {
		return nil, smsg.Sequence, mqttOutboundCorrupt
	}
	return o, smsg.Sequence, mqttOutboundValid
}

// mqttTransitionVerdict is what an ambiguity resolution concludes about one attempt.
type mqttTransitionVerdict int

const (
	// mqttTransitionUnresolved is the honest answer when nothing positive could be
	// established, AND the answer to every contradiction. It authorizes NOTHING: a
	// replacement operation taken on the strength of an unresolved attempt is the
	// release-on-absence shape under another name.
	mqttTransitionUnresolved mqttTransitionVerdict = iota
	// mqttTransitionLanded reports a committed attempt whose ownership still stands.
	mqttTransitionLanded
	// mqttTransitionSuperseded reports a committed attempt whose authority revision has
	// since been advanced. The attempt HAPPENED — its exchange mutations are real and its
	// obligations exist — and only the ownership it installed has moved on. Collapsing this
	// into "never landed" is what would strand the obligations it created.
	mqttTransitionSuperseded
	// mqttTransitionNeverLanded reports a positively aborted attempt, which can never land
	// afterwards because its outcome subject is taken.
	mqttTransitionNeverLanded
)

// resolveTransitionHandle classifies a transition from a handle frozen BEFORE the attempt,
// which is what a caller holds once the original mutation inputs are gone.
//
// Identical evidence rules: the same positive-only reads, the same
// contradiction-is-unresolved partition, and the same digest comparison — against the
// handle's INDEPENDENTLY retained expected value, never against the receipt's own.
func (jsa *mqttJSA) resolveTransitionHandle(h *mqttTransitionHandle) mqttTransitionVerdict {
	if h == nil {
		return mqttTransitionUnresolved
	}
	return jsa.classifyAgainstOutcome(&h.attempt, h.digest)
}

// classifyAgainstOutcome is the ONE partition both resolvers use, so they can never disagree
// about what a receipt means. wantDigest is always retained independently of the receipt
// being read.
func (jsa *mqttJSA) classifyAgainstOutcome(a *mqttTransitionAttempt, wantDigest string) mqttTransitionVerdict {
	if verr := mqttValidateTransitionAttempt(a); verr != nil {
		return mqttTransitionUnresolved
	}
	o, _, ostatus := jsa.loadTransitionOutcome(a.ID, a.Epoch, a.Txn)
	if ostatus != mqttOutboundValid {
		// Absent is not "never landed": the abort has not been written, so the original
		// batch may still be in flight and may still commit. The caller settles it by
		// attempting the abort, never by concluding one here.
		return mqttTransitionUnresolved
	}
	if merr := mqttOutcomeMatchesAttempt(o, a); merr != nil {
		return mqttTransitionUnresolved
	}
	if o.Verdict == mqttOutcomeAborted {
		return mqttTransitionNeverLanded
	}
	// THE DIGEST IS VERIFIED, not merely required to exist, and against a value retained
	// independently of this receipt. Without that the receipt says only that some batch
	// under this transition id committed: changing a member's subject, expectation or body
	// changes what was written while leaving every other field of the outcome identical,
	// and the resolution would still answer for it.
	if wantDigest != o.Digest {
		return mqttTransitionUnresolved
	}
	al := jsa.loadOutboundAuthority(a.ID, a.Epoch)
	if al.status != mqttOutboundValid {
		return mqttTransitionUnresolved
	}
	switch {
	case al.rec.Rev > o.Rev:
		return mqttTransitionSuperseded
	case al.rec.Rev == o.Rev:
		// Txn, not only ownership. OwnerTxn deliberately SURVIVES a transition that changes
		// no owner, so it cannot identify the transition that produced this exact revision;
		// and since every transition advances the revision, one revision produced by a
		// different transition is a contradiction rather than a match.
		if al.rec.Txn == o.Txn && al.rec.OwnerTxn == o.OwnerTxn &&
			al.rec.Gen == o.Gen && al.rec.Tenure == o.Tenure {
			return mqttTransitionLanded
		}
		return mqttTransitionUnresolved
	default:
		// The standing authority precedes a revision a committed outcome says it produced.
		return mqttTransitionUnresolved
	}
}

// abortTransition settles an undecided attempt by taking its outcome subject.
//
// Create-only, and that is the whole mechanism: this write and the original batch's own
// outcome member contend for one subject, so the loser is refused.
//
// A FAILED abort is not by itself proof that the batch won. The failure may be a timeout, a
// transport error, or another resolver's ABORTED already standing there; even a subject
// expectation conflict establishes only that the subject is occupied, not by what. The
// caller therefore re-reads and lets the validated outcome decide — which is what
// classifyAgainstOutcome does, and why this function returns the error rather than a verdict.
func (jsa *mqttJSA) abortTransition(a *mqttTransitionAttempt) error {
	if verr := mqttValidateTransitionAttempt(a); verr != nil {
		return fmt.Errorf("refusing to abort an unsupported transition attempt: %w", verr)
	}
	o := &mqttTransitionOutcome{
		ID: a.ID, Epoch: a.Epoch, Txn: a.Txn, Op: a.Op, Kind: a.Kind,
		Verdict: mqttOutcomeAborted, PrevTxn: a.PrevTxn, Intent: a.Intent,
	}
	if verr := mqttValidateTransitionOutcome(o, a.ID, a.Epoch, a.Txn); verr != nil {
		return fmt.Errorf("refusing to abort transition %q with an unsupported outcome: %w", a.Txn, verr)
	}
	body, _ := json.Marshal(o)
	// Create-only: expect the subject's last sequence to be zero. Framed as every other
	// conditional write on this broker is.
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	_, err := jsa.storeMsg(mqttTransitionOutcomeSubject(getHash(a.ID), a.Txn), hdr, msg)
	return err
}

// mqttTransitionMember is one caller-supplied member of a transition batch: its subject,
// its own expected last subject sequence, and its already-marshalled body. The batch
// framing is applied by the writer, so every member of every transition carries the headers
// the commit protocol depends on.
type mqttTransitionMember struct {
	subj   string
	expect uint64
	body   []byte
}

// mqttAuthorityTransition is one attempted transition, assembled by its caller and
// committed here.
//
// Extra carries the exchange, dependency and inventory mutations the transition authorizes.
// They belong in the SAME batch rather than in a follow-up write, because a transition that
// installed its authority and then failed to install what it authorized would leave an
// obligation nothing admitted — the precise state F-105 exists to make unreachable.
type mqttAuthorityTransition struct {
	// Attempt is the description the commit, the abort and the resolution all agree about.
	Attempt *mqttTransitionAttempt

	// Prev is the authority this transition displaces, exactly as it was loaded, and
	// PrevSeq is the sequence it was loaded at. Nil only for a first admission.
	Prev    *mqttOutboundAuthority
	PrevSeq uint64

	// Auth is the body this transition installs.
	Auth *mqttOutboundAuthority

	// Closure, when present, is the permanent generation closure this transition writes.
	// It is a typed field rather than a caller-supplied member because its subject's
	// permanence is the protocol: a raw member could overwrite a standing closure, and
	// atomicity would commit that faithfully.
	Closure *mqttGenClosure

	// Retire is the standing retirement a close-teardown transition carries. A
	// close-admission and a finalization find their retirement on the predecessor's
	// pending intent; a standalone teardown has no such intent, so the operation reads it
	// off the durable receipt and threads it here for the boundary's affine-binding check.
	// It is transient — the permanent record of what was authorized is the discarding
	// closure and the enrolled affine resource — so it is never serialized.
	Retire *mqttRetirementRequest

	Extra []mqttTransitionMember
}

// commitAuthorityTransition writes the transition as one atomic batch and returns the
// AUTHORITY's sequence.
//
// The authority is the commit member, so the acknowledgement names the record a later
// transition must contend against. No other member's sequence is inferred from it: a caller
// needing one loads that subject authoritatively, exactly as the pair writer does for its
// marker.
func (jsa *mqttJSA) commitAuthorityTransition(t *mqttAuthorityTransition) (uint64, error) {
	if t == nil || t.Auth == nil || t.Attempt == nil {
		return 0, fmt.Errorf("unreachable: an authority transition with no attempt or authority")
	}
	a := t.Attempt
	if verr := mqttValidateTransitionAttempt(a); verr != nil {
		return 0, fmt.Errorf("refusing to commit an unsupported transition attempt: %w", verr)
	}
	idHash := getHash(a.ID)

	// THE WRITE BOUNDARY VALIDATES. Every transition funnels through here, so this is the
	// only place the claim "no unsupported shape is ever written" can be true.
	if verr := mqttValidateOutboundAuthority(t.Auth, a.ID, a.Epoch); verr != nil {
		return 0, fmt.Errorf("refusing to commit a transition with an unsupported authority: %w", verr)
	}

	// The authority this transition installs must be the ownership its attempt described,
	// or the permanent receipt would describe a mutation that never happened.
	if t.Auth.Rev != a.Rev || t.Auth.Gen != a.Gen || t.Auth.Tenure != a.Tenure ||
		t.Auth.OwnerTxn != a.OwnerTxn || t.Auth.Txn != a.Txn {
		return 0, fmt.Errorf("transition %q installs %d/%q/%q/%q but its attempt proposed %d/%q/%q/%q",
			a.Txn, t.Auth.Rev, t.Auth.Gen, t.Auth.Tenure, t.Auth.OwnerTxn, a.Rev, a.Gen, a.Tenure, a.OwnerTxn)
	}

	// THE PREDECESSOR IS VALIDATED, not merely present. A body this broker would refuse to
	// load is not a predecessor a transition may displace: without this, a caller could
	// hand in any struct and the consecutive-revision test would pass against it.
	if t.Prev != nil {
		if verr := mqttValidateOutboundAuthority(t.Prev, a.ID, a.Epoch); verr != nil {
			return 0, fmt.Errorf("refusing to commit a transition over an unsupported predecessor: %w", verr)
		}
	}

	// EACH KIND CARRIES ITS OWN RULE. A closed enum with no rules attached would let an
	// admit-exchange transition pass with a CLOSED predecessor, which is precisely the
	// admission-after-closure this item exists to make unreachable.
	if err := mqttCheckTransitionKindRules(t); err != nil {
		return 0, err
	}
	if err := mqttCheckDisposalLicence(t, func(gen string) error {
		return jsa.requireDiscardingClosure(t.Attempt.ID, t.Attempt.Epoch, gen)
	}); err != nil {
		return 0, err
	}

	// THE PREDECESSOR IS ENFORCED, not assumed. A revision that merely happens to satisfy
	// a storage expectation proves nothing about what it displaced: the sequence moves for
	// completions that change no ownership, so without this a transition could install any
	// revision over any predecessor.
	switch a.Rev {
	case 1:
		if t.Prev != nil || t.PrevSeq != 0 {
			return 0, fmt.Errorf("a first admission of %q names a predecessor", a.ID)
		}
	default:
		if t.Prev == nil {
			return 0, fmt.Errorf("transition %q at revision %d names no predecessor authority", a.Txn, a.Rev)
		}
		if t.PrevSeq == 0 {
			return 0, fmt.Errorf("transition %q names a predecessor authority at sequence zero", a.Txn)
		}
		if t.Prev.Rev+1 != a.Rev {
			return 0, fmt.Errorf("transition %q installs revision %d over a predecessor at %d: revisions are consecutive",
				a.Txn, a.Rev, t.Prev.Rev)
		}
		if t.Prev.OwnerTxn != a.PrevTxn {
			return 0, fmt.Errorf("transition %q expects predecessor ownership %q but its loaded predecessor carries %q",
				a.Txn, a.PrevTxn, t.Prev.OwnerTxn)
		}
	}

	// EVERY CALLER MEMBER IS CLASSIFIED HERE. Atomicity commits what it is given, so a
	// member naming a closure or another transition's outcome would be written faithfully
	// and the permanence these records rest on would be a caller convention rather than a
	// property.
	authSubject := mqttOutboundAuthoritySubject(idHash)
	outcomeSubject := mqttTransitionOutcomeSubject(idHash, a.Txn)
	seen := map[string]struct{}{authSubject: {}, outcomeSubject: {}}
	// THE GENERATION A CLOSURE NAMES, which is not always the transition's own: a discarding
	// finalization installs its SUCCESSOR while closing its predecessor, and the enrolment it
	// carries belongs to the predecessor. Empty when this transition writes no closure, so the
	// permission cannot be reached by a transition that is not closing anything.
	closureGen := _EMPTY_
	if t.Closure != nil {
		closureGen = t.Closure.Gen
	}
	for _, e := range t.Extra {
		if err := mqttCheckTransitionMember(a, idHash, closureGen, e, seen); err != nil {
			return 0, err
		}
		seen[e.subj] = struct{}{}
	}

	digest, derr := mqttTransitionMutationDigest(t)
	if derr != nil {
		return 0, derr
	}
	authBody, _ := json.Marshal(t.Auth)

	var closureBody []byte
	if t.Closure != nil {
		if verr := mqttValidateGenClosure(t.Closure, a.ID, a.Epoch, t.Closure.Gen); verr != nil {
			return 0, fmt.Errorf("refusing to commit a transition with an unsupported closure: %w", verr)
		}
		if t.Closure.Txn != a.Txn {
			return 0, fmt.Errorf("transition %q writes a closure naming transition %q", a.Txn, t.Closure.Txn)
		}
		closureBody, _ = json.Marshal(t.Closure)
	}

	// The digest binds the receipt to THIS mutation, closure included, and is computed from
	// the same assembled form the resolver recomputes.
	outcome := &mqttTransitionOutcome{
		ID: a.ID, Epoch: a.Epoch, Txn: a.Txn, Op: a.Op, Kind: a.Kind,
		Verdict: mqttOutcomeCommitted, PrevTxn: a.PrevTxn, Intent: a.Intent,
		Rev: a.Rev, Gen: a.Gen, Tenure: a.Tenure, OwnerTxn: a.OwnerTxn,
		Digest: digest,
	}
	if verr := mqttValidateTransitionOutcome(outcome, a.ID, a.Epoch, a.Txn); verr != nil {
		return 0, fmt.Errorf("refusing to commit a transition with an unsupported outcome: %w", verr)
	}
	outcomeBody, _ := json.Marshal(outcome)

	// AN OWNERSHIP MOVE IS CHECKED AGAINST ITS METADATA HALF, which is the permanent record
	// of whether the proposal it answers won.
	//
	// The kind rules establish that a finalization names its predecessor's pending proposal,
	// and nothing more: they cannot tell a proposal that COMMITTED from one that was
	// ABORTED, because that fact lives on $MQTT_sess. Without this a caller reserves, aborts
	// its own metadata slot, and then finalizes anyway — installing SERVING over a permanent
	// ABORTED verdict — or cancels and closes a proposal whose metadata half had committed,
	// discarding ownership the cluster has already acted on. mqttDecideConnectOwnership
	// enforces the distinction, and a caller that does not consult it is enforced here.
	if err := jsa.checkMetadataVerdict(t); err != nil {
		return 0, err
	}

	// THE STORE COMPARISONS RUN LAST, after every check a transition can make about itself.
	// A transition that is malformed on its face is refused without a read, and the reads
	// below are then about a transition that is otherwise legal.
	// THE PREDECESSOR IS COMPARED WITH THE ONE IN THE STORE, not merely validated as a
	// well-formed body a caller handed in.
	//
	// Every rule above reads t.Prev to decide what this transition may do — which state may
	// be displaced, which ownership is preserved, whether a proposal is pending. All of them
	// are conclusions about STORED state drawn from a caller's struct. Without this a caller
	// could copy the standing SERVING authority, set its State to CLOSED, keep its real
	// sequence, and reserve a fresh generation over it: every rule passes, the CAS passes
	// because the sequence is right, and live ownership is displaced with no handoff and no
	// permanent closure. The same substitution erases a pending proposal by clearing it.
	// THE STORE COMPARISONS AND THE SEND, as one unit a test seam can wrap: it runs after the
	// attempt is composed and its predecessor captured, so a hook can land a racing write before
	// send (the real pre-submit check then refuses with the real error) or report a committed
	// send as lost. Nil in production.
	send := func() (uint64, error) {
		if t.Prev != nil {
			if err := jsa.checkPredecessorAgainstStore(a, t.Prev, t.PrevSeq); err != nil {
				return 0, err
			}
		}

		// EVERY OVERWRITE OF AN EXCHANGE OR A RESOURCE IS CHECKED AGAINST THE STORE, HERE, and
		// not in any builder.
		//
		// Every other rule above is a property of the transition as composed. This one is a
		// relation between what is being written and what already stands, so nothing that reads
		// only the transition can establish it — and a guarantee that lives in an optional
		// builder is a caller convention, not a property. Without this a caller could assemble a
		// legitimate transition and then substitute its exchange body for a terminal-to-publish
		// regression, a QoS 2 publish-to-terminal skip that drops an owed release, or a changed
		// identifier, QoS, tenure or admitting transition: the member's own schema check passes,
		// the CAS passes because the sequence is right, and a completed obligation is
		// resurrected under an authority that may already be closed.
		//
		// KEYED ON THE MEMBER, NOT ON THE KIND. An exchange member at a nonzero expectation IS
		// an advance, whichever kind carries it, and admit-exchange and acquire-action confine
		// their members by nothing at all — so a rule scoped to advance-exchange would leave the
		// identical bypass one kind name away. This is the family, not the instance.
		xchPred, xerr := jsa.checkExchangeOverwrites(a, t.Extra)
		if xerr != nil {
			return 0, xerr
		}
		if err := jsa.checkSupersededPredecessors(a, t.Extra); err != nil {
			return 0, err
		}
		if err := jsa.checkCreatedExchanges(a, t.Extra); err != nil {
			return 0, err
		}
		if err := jsa.checkActionMembers(a, t.Extra); err != nil {
			return 0, err
		}
		if err := jsa.checkIdentifierExclusivity(a, t.Extra); err != nil {
			return 0, err
		}
		if err := jsa.checkCreatedResources(a, t.Prev, t.Extra); err != nil {
			return 0, err
		}
		if err := jsa.checkNameOwnershipMembers(a, t.Extra); err != nil {
			return 0, err
		}
		if err := jsa.checkPairBindings(a, t.Extra); err != nil {
			return 0, err
		}
		// AND EVERY REPLACING REGISTER IS VALIDATED AGAINST ITS EXACT OCCUPANT. Keyed on the
		// member — a nonzero expectation on a register subject IS a replacement, whichever kind
		// carries it — so the two member arms above may return without deciding it.
		if err := jsa.checkRegisterReplacements(a, t.Extra); err != nil {
			return 0, err
		}
		if err := jsa.checkActionTokenMembers(a, t.Extra); err != nil {
			return 0, err
		}
		// AND EVERY SLOT MEMBER OF AN ADVANCE IS BOUND TO ITS EXCHANGE ADVANCE (F-105 rows 3-4).
		// A register member moving along an acknowledgement arrow (publish_terminal,
		// pubrel_intended, pubrel_terminal), or a PUBREL holder's token clearance, records one
		// fact beside the exchange member; this validates it against that member and the
		// exchange's VALIDATED predecessor, and refuses any in-flight member on an advance that
		// is neither.
		if err := jsa.checkAckPhaseAdvanceMembers(a, t.Extra, xchPred); err != nil {
			return 0, err
		}
		// AND EVERY LEDGER AND DUTY MEMBER IS VALIDATED AS AN ALLOCATION OR A CONTINUATION. Keyed
		// on the member too: a create-only duty member IS an allocation and owes its ledger CAS,
		// whatever kind of transition happens to carry it.
		if err := jsa.checkInventoryMembers(a, t.Extra); err != nil {
			return 0, err
		}
		// AND A CLOSING BATCH ENROLS ITS GENERATION FOR RECOVERY, in the same commit. Keyed on the
		// CLOSURE rather than on the members present, because the bypasses this rule exists to close
		// are omissions: a closure that seals without a root, or seals nothing at all.
		if err := jsa.checkRecoveryEnrolment(a, t); err != nil {
			return 0, err
		}
		// AND THE RETAINED MATERIAL'S FENCE. Keyed on the member for the same reason: a chunk IS a
		// fenced write, whatever kind of transition carries it, and a batch that omitted the
		// manifest would otherwise escape every rule conditioned on one.
		if err := jsa.checkManifestMembers(a, t.Prev, t.Extra); err != nil {
			return 0, err
		}
		// AND A RETAINED CANONICAL PUBREL IS CREATED BY EXACTLY ITS ARROW (F-105 7c).
		if err := jsa.checkRetainedRelMembers(a, t.Extra); err != nil {
			return 0, err
		}
		if err := checkRetainedAdmissionCoverage(a, t.Extra); err != nil {
			return 0, err
		}
		// A GENERATION IS CLAIMED ONCE. Every kind that proposes or installs one asks whether it
		// was ever closed, because the kind rules compare only with the immediate predecessor and
		// a permanent closure outlives every predecessor it had.
		switch a.Kind {
		case mqttTransitionReserveCreation, mqttTransitionFinalizeCreation:
			if err := jsa.checkProposedGenerationIsFresh(a, t.Auth.Gen); err != nil {
				return 0, err
			}
		case mqttTransitionPrepareIntent:
			if t.Auth.Intent != nil && t.Auth.Intent.Gen != t.Auth.Gen {
				if err := jsa.checkProposedGenerationIsFresh(a, t.Auth.Intent.Gen); err != nil {
					return 0, err
				}
			}
		case mqttTransitionFinalize:
			// A resume keeps the generation it is already serving, which is not a claim on a
			// fresh one; only a clean start proposes one.
			if t.Prev != nil && t.Auth.Gen != t.Prev.Gen {
				if err := jsa.checkProposedGenerationIsFresh(a, t.Auth.Gen); err != nil {
					return 0, err
				}
			}
		}
		if err := jsa.checkResourceOverwrites(a, t.Extra, xchPred); err != nil {
			return 0, err
		}
		// Every boundary check has passed; only the store's own CAS stands between this batch and
		// the stream. A test that must stage REAL store contention — not a boundary refusal — lands
		// its racing write here. Nil in production.
		if hook := jsa.testTransitionPresend.Load(); hook != nil {
			(*hook)(t)
		}

		jsa.mu.Lock()
		batchID := jsa.nuid.Next()
		jsa.mu.Unlock()

		// Member 1 is the outcome, create-only. Then the closure, also create-only. Then what
		// the transition authorizes. The authority is last and carries the commit flag, so the
		// PubAck names it.
		total := 2 + len(t.Extra)
		if closureBody != nil {
			total++
		}
		msgs := make([]mqttBatchMsg, 0, total)
		seq := 1
		add := func(subj string, expect uint64, body []byte, commit bool) {
			m, h := mqttFrameBatchWrite(expect, body, batchID, seq, commit)
			msgs = append(msgs, mqttBatchMsg{subj: subj, hdr: h, msg: m})
			seq++
		}
		add(outcomeSubject, 0, outcomeBody, false)
		if closureBody != nil {
			add(mqttGenClosureSubject(idHash, t.Closure.Gen), 0, closureBody, false)
		}
		for _, e := range t.Extra {
			add(e.subj, e.expect, e.body, false)
		}
		add(authSubject, t.PrevSeq, authBody, true)

		bi, err := jsa.newAtomicBatchRequest(mqttJSAMsgStore, _EMPTY_, msgs)
		if err != nil {
			return 0, err
		}
		resp := bi.(*JSPubAckResponse)
		if aerr := resp.ToError(); aerr != nil {
			return 0, aerr
		}
		if resp.PubAck == nil {
			return 0, fmt.Errorf("transition %q was acknowledged with no publish acknowledgement", a.Txn)
		}
		if resp.BatchId != batchID || resp.BatchSize != uint64(total) {
			return 0, fmt.Errorf("transition %q was acknowledged as %d message(s) under batch %q, but this broker sent %d under %q",
				a.Txn, resp.BatchSize, resp.BatchId, total, batchID)
		}
		if resp.Stream != mqttInflightStreamName {
			return 0, fmt.Errorf("transition %q was acknowledged on stream %q (want %q)",
				a.Txn, resp.Stream, mqttInflightStreamName)
		}
		return resp.Sequence, nil
	}
	if hook := jsa.testTransitionCommit.Load(); hook != nil {
		return (*hook)(t, send)
	}
	return send()
}

// mqttCheckTransitionKindRules enforces what each operation requires of the authority it
// displaces and the body it installs.
//
// The enum alone establishes nothing: it is the rules here that make "admitted before
// closure" a property of the mechanism rather than of its callers. Every rule is stated as
// what the operation needs, so a kind added without one fails closed at the default.
// mqttCheckDisposalLicence requires every disposal decision this transition introduces to be made
// under the serving entitlement it names (F-105 7c-lc).
//
// A route decides a consumer is ended under the connection that ran it, and records that
// entitlement in the decision. Composing the write later against whatever authority stands would
// let a route paused across a takeover mark the successor's still-serving consumer disposable —
// and with zero dependents it would then be retired at once. So the predecessor this batch
// displaces must be SERVING at exactly the decision's generation and tenure. A decision is
// introduced by the transition it names (the store comparison in mqttCheckResourceStep refuses
// anything else), which is how one carried from an earlier batch is told apart without a read.
//
// ONE DECISION IS LICENSED BY A FACT INSTEAD (F-105 7d): `generation-discarded`, the decision a
// discarded generation's retirement records about its own consumers after the generation has
// stopped serving. No entitlement exists to take it under; its licence is the resource
// generation's committed discarding closure, which discarding() reads and which cannot become
// false later. It carries no tenure, and it is refused for any generation not discarded.
func mqttCheckDisposalLicence(t *mqttAuthorityTransition, discarding func(gen string) error) error {
	for _, e := range t.Extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttResourceClassTk {
			continue
		}
		var r mqttOutboundResource
		if json.Unmarshal(e.body, &r) != nil || r.Disposal == nil || r.Disposal.Txn != t.Attempt.Txn {
			continue
		}
		if t.Attempt.Kind != mqttTransitionDisposeResource {
			return fmt.Errorf("transition %q of kind %q introduces a disposal decision on %q: a disposal "+
				"is a resource transition of its own", t.Attempt.Txn, t.Attempt.Kind, r.Inc)
		}
		if r.Disposal.Reason == mqttDisposalGenerationDiscarded {
			if r.Disposal.Gen != r.Gen {
				return fmt.Errorf("transition %q records the discard of generation %q on resource %q of "+
					"generation %q: a discard decides only its own generation's consumers",
					t.Attempt.Txn, r.Disposal.Gen, r.Inc, r.Gen)
			}
			if err := discarding(r.Gen); err != nil {
				return fmt.Errorf("transition %q disposes of %q as discarded: %w", t.Attempt.Txn, r.Inc, err)
			}
			continue
		}
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing ||
			t.Prev.Gen != r.Disposal.Gen || t.Prev.Tenure != r.Disposal.Tenure {
			return fmt.Errorf("transition %q disposes of %q under the entitlement %q/%q, which is not the "+
				"serving one: a decision taken under an entitlement that has moved on is never "+
				"re-made under its successor", t.Attempt.Txn, r.Inc, r.Disposal.Gen, r.Disposal.Tenure)
		}
		// THE TWO SETTLEMENT REASONS ARE A SOURCE CONSUMER'S ONLY (F-105 7f). The generation's PUBREL
		// consumer is one stable name a successor reuses, so neither a successor's settlement nor a
		// subscription repair ever decides it.
		switch r.Disposal.Reason {
		case mqttDisposalTenureSuperseded, mqttDisposalRepairReplaced:
			if r.Kind != mqttResourceSourceConsumer {
				return fmt.Errorf("transition %q disposes of %q (kind %q) as %q, which decides only a "+
					"subscription's source consumer", t.Attempt.Txn, r.Inc, r.Kind, r.Disposal.Reason)
			}
		}
		// A SUCCESSOR'S DECISION, NEVER A TENURE'S ABOUT ITS OWN CLAIM. The naming half of the
		// settlement's licence is the composer's (the session record is not in this batch); this is
		// the half the record carries: the deciding tenure is not the one that provisioned it.
		if r.Disposal.Reason == mqttDisposalTenureSuperseded && r.ProvTenure == r.Disposal.Tenure {
			return fmt.Errorf("transition %q disposes of %q as superseded under tenure %q, the tenure "+
				"that provisioned it: a tenure's own consumer is ended by the route that ends it",
				t.Attempt.Txn, r.Inc, r.Disposal.Tenure)
		}
	}
	return nil
}

// mqttKindCloses reports whether a transition kind may write a generation closure.
//
// Both of them may also carry the ENROLMENT that closure needs, and nothing else. See
// mqttCheckClosingEnrolment.
func mqttKindCloses(k mqttTransitionKind) bool {
	return k == mqttTransitionCloseAdmit || k == mqttTransitionFinalize ||
		k == mqttTransitionCloseTeardown
}

// mqttCheckClosingEnrolment is the one permission a closing transition has over members.
//
// # What it admits, and why only this
//
// A DISCARDING generation's AFFINE CONSUMER has to be enrolled before its metadata is released,
// because the session record is what names it and the release is what deletes it. The enrolment
// is a cleanup duty, a duty needs an ORDINAL, and only the ledger allocates one — which the SEAL
// forbids after. Closure is the last act able to take one, so the enrolment rides in the batch
// that closes, or it cannot exist at all. (The shared group durables used to be enrolled here
// too; they are no longer resources of this authority — a generation's shared ACQUISITIONS are
// released by the shared closing from its own acquisition ledger, shared_lifecycle_ops.go.)
//
// That is a reason to admit EXACTLY those members and nothing else. A closure carrying an
// exchange would admit work into a generation it is ending; one carrying a source consumer would
// provision under an authority that is about to stop serving; one carrying a continuation would
// advance an entry whose sweep may already have passed it. Each of those is a write no rule about
// a closing kind has ever looked at, which is what the finding underneath this permission was.
func mqttCheckClosingEnrolment(a *mqttTransitionAttempt, t *mqttAuthorityTransition) error {
	// AT MOST ONE affine target: a generation holds one affine consumer, and a second would name
	// one no retirement selected.
	affineSeen := false
	if t.Closure == nil {
		return fmt.Errorf("a %s transition carries %d member(s) without writing a closure: the "+
			"only members a closing kind may carry are the enrolment of the generation it is "+
			"closing, and it is not closing one", a.Kind, len(t.Extra))
	}
	gen := t.Closure.Gen
	idHash := getHash(a.ID)
	// THE ENROLMENT IS ONE-TO-ONE, and the duties are bound to it in BOTH directions. The
	// coverage rule binds a created resource to the duties that must name it; nothing bound a
	// duty to a resource this batch created, so a closure could otherwise allocate an ordinal
	// for any reference it liked — which is the hole this whole permission was carved out of.
	enrolled := map[string]struct{}{}
	for _, e := range t.Extra {
		cls, ok := mqttF105ObligationClass(e.subj)
		if !ok || cls != mqttResourceClassTk || e.expect != 0 {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) == 4 {
			enrolled[toks[3]] = struct{}{}
		}
	}
	for _, e := range t.Extra {
		// THE DIRECTORY CLASSES TRAVEL WITH A CLOSURE, and only with one. They are not obligation
		// classes — no admission may ever carry one — and their whole rule set is the batch-level
		// one in checkRecoveryEnrolment: the ordinal against the stored header, the bound against
		// the ledger this batch seals, the identity, the birth cursor, and that neither may appear
		// without the other. Admitting the class here is not admitting the content.
		if mqttIsShardSubject(e.subj) || mqttIsRootSubject(e.subj) {
			continue
		}
		cls, ok := mqttF105ObligationClass(e.subj)
		if !ok {
			return fmt.Errorf("a %s transition carries member %q, which is no obligation class",
				a.Kind, e.subj)
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		switch cls {
		case mqttResourceClassTk:
			if len(toks) != 4 || toks[1] != idHash || toks[2] != gen {
				return fmt.Errorf("a %s transition enrols resource %q, which is not in the "+
					"generation %q its closure names", a.Kind, e.subj, gen)
			}
			if e.expect != 0 {
				return fmt.Errorf("a %s transition rewrites resource %q at sequence %d: an "+
					"enrolment creates an incarnation and never replaces one",
					a.Kind, e.subj, e.expect)
			}
			r, derr := mqttDecodeOutboundResource(e.body, a.ID, a.Epoch, gen, toks[3])
			if derr != nil {
				return fmt.Errorf("a %s transition carries an unsupported resource on %q: %w",
					a.Kind, e.subj, derr)
			}
			// THE KIND IS THE PERMISSION, and there is exactly one.
			//
			// Everything else a generation owns was provisioned under a serving authority and is
			// already enrolled. An AFFINE CONSUMER is the exception: the session created it, but
			// it is keyed by an affine generation that outlives incarnations, so no
			// session-generation fact could ever authorize deleting one — which is why the
			// closure is where it becomes a duty. (A group durable used to be the second kind
			// admitted here, as one the generation held without provisioning; it is no longer a
			// resource of this authority at all.)
			//
			// AND IT IS ADMITTED ONLY BY A DISCARDING CLOSURE. A closure that
			// preserves the session's obligations leaves its affine consumer to the incarnation
			// that continues; enrolling it there would file a cleanup duty against a consumer
			// that is still in use.
			switch r.Kind {
			case mqttResourceAffineConsumer:
				if t.Closure == nil || !t.Closure.Discard {
					return fmt.Errorf("a %s transition enrols affine consumer %q under a closure "+
						"that does not discard: the consumer belongs to the incarnation that "+
						"continues, and a cleanup duty against it would name a consumer still "+
						"in use", a.Kind, toks[3])
				}
				// AND IT IS THE CONSUMER THE STANDING RETIREMENT NAMED, not merely a
				// well-formed one.
				//
				// The disposition says the generation's obligations are abandoned; it says
				// nothing about WHICH consumer that licenses deleting. Every field the executor
				// acts on is derived from the resource's own AffGen, so a finalization that
				// preserves the closing generation, the PENDING state, the cleanup duty and the
				// root — and substitutes another correctly derived affine target — would send
				// the pass to discard that generation's assignments and delete its consumer,
				// which may be the SUCCESSOR'S. Trusting the acceptance's choice of target does
				// not license changing that choice during finalization.
				if affineSeen {
					return fmt.Errorf("a %s transition enrols two affine consumers: a "+
						"generation holds one, and a second names a consumer no retirement "+
						"selected", a.Kind)
				}
				affineSeen = true
				ret := mqttStandingRetirement(t)
				if ret == nil {
					return fmt.Errorf("a %s transition enrols affine consumer %q with no "+
						"standing retirement to have selected it", a.Kind, toks[3])
				}
				if r.AffGen != ret.AffineGen || r.Name != ret.AffineDurable ||
					r.PubRelDur != ret.PubRelDurable {
					return fmt.Errorf("a %s transition enrols affine consumer %q naming "+
						"generation %q, durable %q and PUBREL durable %q, while the standing "+
						"retirement names %q, %q and %q: the enrolment is bound to the target "+
						"the acceptance selected", a.Kind, toks[3], r.AffGen, r.Name,
						r.PubRelDur, ret.AffineGen, ret.AffineDurable, ret.PubRelDurable)
				}
			default:
				return fmt.Errorf("a %s transition enrols resource %q of kind %q: a closure may "+
					"enrol its generation's own affine consumer, and nothing else", a.Kind, toks[3], r.Kind)
			}
		case mqttDutyClassTk, mqttLedgerClassTk:
			if len(toks) < 3 || toks[1] != idHash || toks[2] != gen {
				return fmt.Errorf("a %s transition carries inventory member %q, which is not in "+
					"the generation %q its closure names", a.Kind, e.subj, gen)
			}
			if cls != mqttDutyClassTk {
				// AND THE LEDGER IT CARRIES IS THE SEALED ONE. The enrolment's whole claim to
				// completeness is that the act which allocates is the act which seals, so no
				// further ordinal can be taken in that generation afterwards. Checking only the
				// ledger's GENERATION left that a property of the composer: a batch carrying an
				// otherwise valid enrolment beside a ledger body still OPEN closes the
				// generation with its inventory unsealed, and then a later disposal — which is
				// permitted to name an earlier generation — allocates into an inventory this
				// act was supposed to have shut. The completeness the release gate reads off
				// that generation would then be a snapshot again.
				led, derr := mqttDecodeGenLedger(e.body, a.ID, a.Epoch, gen)
				if derr != nil {
					return fmt.Errorf("a %s transition carries an unsupported ledger on %q: %w",
						a.Kind, e.subj, derr)
				}
				if led.State != mqttGenLedgerSealed {
					return fmt.Errorf("a %s transition closes generation %q while leaving its "+
						"inventory at state %d: closure is the last act able to take an ordinal, "+
						"so the act that allocates is the act that seals",
						a.Kind, gen, led.State)
				}
				break
			}
			if len(toks) != 4 {
				return fmt.Errorf("a %s transition carries a malformed duty subject %q",
					a.Kind, e.subj)
			}
			// A DUTY A CLOSURE ALLOCATES NAMES A RESOURCE THIS BATCH ENROLLED. Anything else is
			// an ordinal taken for a reference the enrolment is not about, in an inventory this
			// same act is sealing shut behind it.
			ord, oerr := mqttParseOrdinal(toks[3])
			if oerr != nil {
				return fmt.Errorf("a %s transition carries duty member %q with a malformed "+
					"ordinal: %w", a.Kind, e.subj, oerr)
			}
			d, derr := mqttDecodeDutyEntry(e.body, a.ID, a.Epoch, gen, ord)
			if derr != nil {
				return fmt.Errorf("a %s transition carries an unsupported duty on %q: %w",
					a.Kind, e.subj, derr)
			}
			if d.Kind != mqttDutyCleanup {
				return fmt.Errorf("a %s transition allocates a %q duty: a closure's enrolment "+
					"reserves the CLEANUP of the durables it names and nothing else",
					a.Kind, d.Kind)
			}
			if _, ok := enrolled[d.Ref]; !ok {
				return fmt.Errorf("a %s transition allocates a cleanup duty naming %q, which it "+
					"does not enrol: an ordinal taken for a reference this batch is not about "+
					"lands in an inventory the same act seals shut behind it", a.Kind, d.Ref)
			}
		default:
			return fmt.Errorf("a %s transition carries member %q of class %q: a closure carries "+
				"its generation's affine enrolment and its inventory, and nothing else",
				a.Kind, e.subj, cls)
		}
	}
	return nil
}

// mqttKindCarriesMembers reports whether a transition kind may carry obligation members at all.
//
// A CLOSED SET, stated once, because five kinds enumerated what they may carry and five said
// nothing — and a rule written five times and not the sixth is the shape this lane keeps finding.
// The five that carry members are the ones whose PURPOSE is the members: they provision,
// materialize, dispose, admit or advance. The rest install, freeze, cancel or close OWNERSHIP,
// and each of their own comments already says so — "and NOTHING ELSE", "a freeze is not
// closure", "a cancellation ends nothing".
//
// Refusing here rather than in each arm is deliberate: a new kind is refused by default and has
// to be added to this set on purpose, which is the opposite of inheriting a permission by being
// forgotten.
func mqttKindCarriesMembers(k mqttTransitionKind) bool {
	switch k {
	case mqttTransitionAdmitExchange, mqttTransitionAcquireAction, mqttTransitionAdvanceExchange,
		mqttTransitionProvisionResource, mqttTransitionDisposeResource,
		mqttTransitionMaterializePayload, mqttTransitionWithdrawUnsent, mqttTransitionAbandonDelivery,
		mqttTransitionTombstoneRegister, mqttTransitionRebindCanonical, mqttTransitionDiscardExchange,
		mqttTransitionAdvanceFloor:
		return true
	}
	return false
}

// mqttCheckEndingPreservesAuthority is the authority rule of every kind that ENDS work already
// authorised rather than authorising new work: a predecessor is required, and the transition
// changes nothing on the authority but its revision — not its state, not its ownership, not
// either pending proposal, and it writes no closure.
//
// One statement for the kinds that share it (advance-exchange, withdraw-unsent), because a kind
// exempt from the serving requirement that could move ownership or state would be a route for
// taking over a generation under a closed authority, and two copies of the rule are how one of
// them loses a clause.
func mqttCheckEndingPreservesAuthority(t *mqttAuthorityTransition, what string) error {
	if t.Prev == nil {
		return fmt.Errorf("%s transition requires a predecessor", what)
	}
	if t.Auth.State != t.Prev.State {
		return fmt.Errorf("%s transition moves the authority from state %d to %d",
			what, t.Prev.State, t.Auth.State)
	}
	if t.Auth.Gen != t.Prev.Gen || t.Auth.Tenure != t.Prev.Tenure || t.Auth.OwnerTxn != t.Prev.OwnerTxn {
		return fmt.Errorf("%s transition moves ownership, which it may not", what)
	}
	if !mqttSameHandoffIntent(t.Prev.Intent, t.Auth.Intent) {
		return fmt.Errorf("%s transition rewrote the pending handoff intent", what)
	}
	if !mqttSameCreationIntent(t.Prev.Creation, t.Auth.Creation) {
		return fmt.Errorf("%s transition rewrote the pending creation proposal", what)
	}
	if t.Closure != nil {
		return fmt.Errorf("%s transition writes a generation closure", what)
	}
	return nil
}

func mqttCheckTransitionKindRules(t *mqttAuthorityTransition) error {
	a := t.Attempt
	// OWNERSHIP TRANSITIONS CARRY NOTHING, with ONE narrow exception. See
	// mqttKindCarriesMembers and mqttCheckClosingEnrolment.
	if !mqttKindCarriesMembers(a.Kind) && len(t.Extra) != 0 {
		if !mqttKindCloses(a.Kind) {
			return fmt.Errorf("a %s transition carries %d obligation member(s), which its kind "+
				"does not admit: it moves, freezes or cancels ownership, and an obligation "+
				"riding on one of those is a write no rule about that kind has ever looked at",
				a.Kind, len(t.Extra))
		}
		if err := mqttCheckClosingEnrolment(a, t); err != nil {
			return err
		}
	}
	switch a.Kind {
	case mqttTransitionReserveCreation:
		// A CLAIM THAT SERVES NOTHING. It installs a reservation, never a serving authority
		// — the whole point is that nothing may be owed to a client before the metadata
		// half commits — and it may not admit obligations or close anything.
		//
		// TWO PREDECESSORS ARE LEGAL, and they are one fact rather than two kinds:
		//
		//   - NONE, at revision 1, which is create-only by virtue of that. The first claim
		//     on a client with no prior ownership.
		//   - A CLOSED authority, at any revision, proposing a FRESH generation. The claim
		//     after a closure — an abandoned first creation, or a retired generation.
		//
		// The second arm is the ONLY route to a post-closure claim. A kind installing
		// SERVING over a closure — readmit-tenure, deleted — carried no frozen proposal and
		// no metadata half, so it reintroduced the bootstrap gap on the second attempt,
		// which is exactly what this kind was added to close on the first.
		switch {
		case t.Prev == nil:
			if a.Rev != 1 {
				return fmt.Errorf("a reserve-creation transition at revision %d has no predecessor", a.Rev)
			}
		case t.Prev.State == mqttAuthorityClosed:
			// The closure stands: this reserves a successor BESIDE it and never reopens it,
			// so the generation must actually differ and no closure is written here.
			if t.Auth.Gen == t.Prev.Gen {
				return fmt.Errorf("a reserve-creation transition reuses the closed generation %q", t.Prev.Gen)
			}
		default:
			return fmt.Errorf("a reserve-creation transition displaces an authority in state %d; "+
				"only an absent or a CLOSED predecessor may be claimed over", t.Prev.State)
		}
		if t.Auth.State != mqttAuthorityReserving {
			return fmt.Errorf("a reserve-creation transition installs state %d, not reserving", t.Auth.State)
		}
		if t.Auth.OwnerTxn != a.Txn {
			return fmt.Errorf("a reserve-creation transition installs ownership %q rather than its own identity %q",
				t.Auth.OwnerTxn, a.Txn)
		}
		if t.Closure != nil {
			return fmt.Errorf("a reserve-creation transition writes a generation closure")
		}

	case mqttTransitionFinalizeCreation:
		// The reservation becomes a serving authority, and may install ONLY what it
		// proposed. Binding to the named creation intent is what stops a second claimant
		// finalizing somebody else's reservation into its own tenure.
		if t.Prev == nil || t.Prev.State != mqttAuthorityReserving || t.Prev.Creation == nil {
			return fmt.Errorf("a finalize-creation transition requires a reserving predecessor carrying its proposal")
		}
		c := t.Prev.Creation
		if a.Intent != c.ID {
			return fmt.Errorf("a finalize-creation transition answers proposal %q but its predecessor carries %q",
				a.Intent, c.ID)
		}
		if t.Auth.State != mqttAuthorityServing {
			return fmt.Errorf("a finalize-creation transition installs state %d, not serving", t.Auth.State)
		}
		if t.Auth.Gen != c.Gen || t.Auth.Tenure != c.Tenure {
			return fmt.Errorf("a finalize-creation transition installs %q/%q but its proposal was %q/%q",
				t.Auth.Gen, t.Auth.Tenure, c.Gen, c.Tenure)
		}
		// Ownership stays the reservation's: the transition that CLAIMED the client is the
		// one that owns it, and finalization is the same ownership becoming servable.
		if t.Auth.OwnerTxn != t.Prev.OwnerTxn {
			return fmt.Errorf("a finalize-creation transition moves ownership from %q to %q",
				t.Prev.OwnerTxn, t.Auth.OwnerTxn)
		}
		if t.Closure != nil {
			return fmt.Errorf("a finalize-creation transition writes a generation closure")
		}

	case mqttTransitionAdvanceFloor:
		// Retiring finished history behind the floor (F-112): any predecessor state, nothing
		// moved, only the ledger and duty members. checkFloorAdvance decides every member.
		if err := mqttCheckAdvanceFloorKind(t); err != nil {
			return err
		}

	case mqttTransitionDisposeResource:
		// Permitted under ANY predecessor state, including CLOSED, because that is when
		// old-generation cleanup runs. It moves nothing: ownership, generation and tenure
		// are preserved exactly, the authority state is preserved exactly, and no closure
		// is written. Everything it may change lives in its resource members.
		if t.Prev == nil {
			return fmt.Errorf("a dispose-resource transition requires a predecessor")
		}
		if t.Auth.State != t.Prev.State {
			return fmt.Errorf("a dispose-resource transition moves the authority from state %d to %d",
				t.Prev.State, t.Auth.State)
		}
		if t.Auth.Gen != t.Prev.Gen || t.Auth.Tenure != t.Prev.Tenure || t.Auth.OwnerTxn != t.Prev.OwnerTxn {
			return fmt.Errorf("a dispose-resource transition moves ownership, which it may not")
		}
		// A PENDING PROPOSAL IS PRESERVED EXACTLY. Cleanup runs beside a reservation or a
		// preparation, and a resource-only transition that dropped or rewrote one would
		// silently abandon a handoff nobody resolved — the authority would still be in that
		// state with nothing to finalize.
		if !mqttSameHandoffIntent(t.Prev.Intent, t.Auth.Intent) {
			return fmt.Errorf("a dispose-resource transition rewrote the pending handoff intent")
		}
		if !mqttSameCreationIntent(t.Prev.Creation, t.Auth.Creation) {
			return fmt.Errorf("a dispose-resource transition rewrote the pending creation proposal")
		}
		if t.Closure != nil {
			return fmt.Errorf("a dispose-resource transition writes a generation closure")
		}
		if len(t.Extra) == 0 {
			return fmt.Errorf("a dispose-resource transition carries no resource")
		}
		for _, e := range t.Extra {
			// THE INVENTORY TRAVELS WITH THE WORK. A disposal advances the cleanup duty that
			// was predeclared with the resource, and after closure that advance is the ONLY
			// thing left that can happen to it — so refusing inventory members here would make
			// the predeclared entry unreachable by the act it was predeclared for.
			//
			// Admitting the class is not admitting the content: checkInventoryMembers validates
			// every one of them as an allocation or a continuation, against the standing ledger
			// and the standing entry.
			if mqttIsLedgerSubject(e.subj) || mqttIsDutySubject(e.subj) ||
				mqttIsManifestSubject(e.subj) || mqttIsChunkSubject(e.subj) {
				// Retained replay MATERIAL travels with the disposal too: closing a manifest is
				// what a cleanup does FIRST, before anything is removed, so that a delayed chunk
				// writer cannot recreate material already deleted. checkManifestMembers
				// validates the fence; admitting the class is not admitting the content.
				continue
			}
			if mqttIsNameOwnershipSubject(e.subj) {
				// AUTHORIZING DELETION RETIRES THE PHYSICAL NAME, in this same batch, so no
				// obligation may depend on it and no replacement may claim it. checkNameOwnership
				// Members validates the LIVE -> RETIRED transition against the standing record;
				// admitting the class is not admitting the content.
				continue
			}
			if !mqttResourceIsF105Class(e.subj) {
				return fmt.Errorf("a dispose-resource transition carries member %q, which is "+
					"neither a resource nor its inventory", e.subj)
			}
		}

	case mqttTransitionProvisionResource:
		// NEW WORK, so the authority governs it exactly as it governs an admission: a
		// resource is provisioned to be depended on, and a closed generation acquires
		// nothing further. It moves no ownership and writes no closure, and its one member
		// is create-only — a provisioning that could overwrite would reset a live resource's
		// dependents to nothing.
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing {
			return fmt.Errorf("a provision-resource transition requires a serving predecessor")
		}
		if t.Auth.State != mqttAuthorityServing {
			return fmt.Errorf("a provision-resource transition installs state %d, not serving", t.Auth.State)
		}
		if t.Auth.Gen != t.Prev.Gen || t.Auth.Tenure != t.Prev.Tenure || t.Auth.OwnerTxn != t.Prev.OwnerTxn {
			return fmt.Errorf("a provision-resource transition moves ownership, which it may not")
		}
		if t.Closure != nil {
			return fmt.Errorf("a provision-resource transition writes a generation closure")
		}
		// EXACTLY ONE RESOURCE, and the inventory that reserves its duties.
		//
		// The count rule used to be "one member", which said the same thing when a resource had
		// no inventory. It cannot stay that way: provisioning now predeclares the resource's
		// provisioning AND cleanup duties in this same batch, because the cleanup duty becomes
		// actionable only when the last dependent goes — possibly long after closure, when
		// allocating an ordinal is exactly what the seal forbids. So the rule is restated as
		// what it always meant: ONE RESOURCE, created, plus whatever inventory reserves it.
		resources := 0
		for _, e := range t.Extra {
			if mqttIsLedgerSubject(e.subj) || mqttIsDutySubject(e.subj) ||
				mqttIsManifestSubject(e.subj) || mqttIsChunkSubject(e.subj) {
				// A materialization is PROVISIONED like any other resource, and its manifest and
				// chunks are the material that provisioning produces.
				continue
			}
			if mqttIsNameOwnershipSubject(e.subj) {
				// A SOURCE or PUBREL provisioning claims the consumer's physical name LIVE in the
				// same batch. checkNameOwnershipMembers validates the claim against the standing
				// record; admitting the class here is not admitting the content, and the name is
				// not a resource, so it is not counted as one below.
				continue
			}
			if !mqttResourceIsF105Class(e.subj) {
				return fmt.Errorf("a provision-resource transition carries member %q, which is "+
					"neither a resource nor its inventory", e.subj)
			}
			if e.expect != 0 {
				return fmt.Errorf("a provision-resource transition rewrites the resource on %q at sequence %d: "+
					"provisioning creates an incarnation and never replaces one",
					e.subj, e.expect)
			}
			resources++
		}
		if resources != 1 {
			return fmt.Errorf("a provision-resource transition carries %d resource(s), not the "+
				"one it provisions", resources)
		}

	case mqttTransitionMaterializePayload:
		// NEW WORK, so the admission rule applies exactly as it does to provisioning: material
		// is written to be admitted against, and a closed generation materializes nothing. The
		// disposal kind carries the same records under ANY state because CLEANUP must outlive
		// closure; writing material must not.
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing {
			return fmt.Errorf("a materialize-payload transition requires a serving predecessor")
		}
		if t.Auth.State != mqttAuthorityServing {
			return fmt.Errorf("a materialize-payload transition installs state %d, not serving",
				t.Auth.State)
		}
		if t.Auth.Gen != t.Prev.Gen || t.Auth.Tenure != t.Prev.Tenure || t.Auth.OwnerTxn != t.Prev.OwnerTxn {
			return fmt.Errorf("a materialize-payload transition moves ownership, which it may not")
		}
		if !mqttSameHandoffIntent(t.Prev.Intent, t.Auth.Intent) {
			return fmt.Errorf("a materialize-payload transition rewrote the pending handoff intent")
		}
		if !mqttSameCreationIntent(t.Prev.Creation, t.Auth.Creation) {
			return fmt.Errorf("a materialize-payload transition rewrote the pending creation proposal")
		}
		if t.Closure != nil {
			return fmt.Errorf("a materialize-payload transition writes a generation closure")
		}
		if len(t.Extra) == 0 {
			return fmt.Errorf("a materialize-payload transition carries no material")
		}
		// IT CARRIES THE MATERIALIZATION'S OWN RECORDS AND NOTHING ELSE. Without this, a kind
		// whose whole business is writing bytes would be a route for installing resources,
		// registers or exchanges beside them — and the fence rules, which are keyed on manifest
		// and chunk members, would have nothing to say about the rest of the batch.
		for _, e := range t.Extra {
			if !mqttIsManifestSubject(e.subj) && !mqttIsChunkSubject(e.subj) {
				return fmt.Errorf("a materialize-payload transition carries member %q, which is "+
					"not a manifest or a chunk", e.subj)
			}
		}

	case mqttTransitionAdvanceExchange:
		// Permitted under ANY predecessor state, including CLOSED, because an obligation of
		// a closed generation must still be able to end. Like dispose-resource it preserves
		// ownership, state and both pending proposals exactly; unlike it, what it carries is
		// exchanges rather than resources, and it carries at least one.
		if err := mqttCheckEndingPreservesAuthority(t, "an advance-exchange"); err != nil {
			return err
		}
		if len(t.Extra) == 0 {
			return fmt.Errorf("an advance-exchange transition carries no exchange")
		}
		// IT CARRIES EXCHANGES IT ADVANCES, AND — as an acknowledgement (F-105 row 3) — THE
		// SLOT RE-PHASE THAT COMPLETES THE SAME EVENT, AND NOTHING ELSE. Without the "nothing
		// else" the kind that is exempt from the serving requirement would be a route for
		// admitting registers, actions or dependency counts under a closed authority; the
		// acknowledgement re-phase and the PUBREL drain's token clearance are admitted only at the
		// slot states mqttAdvanceKindSlotState names, with their full validation (the binding to
		// the exchange member and its standing phase, the transmission and token rules) in
		// checkAckPhaseAdvanceMembers.
		exchanges := 0
		for _, e := range t.Extra {
			// AND NOTHING IS CREATED. A create-only member here would install an obligation —
			// an exchange or a register nothing admitted — through the one kind that does not
			// need a serving authority.
			// THE RETAINED PUBREC ADVANCE (F-105 7c) carries two more: its canonical PUBREL,
			// create-only (the record the advance installs, bound to its arrow by
			// checkRetainedRelMembers), and its copy's manifest, rewritten unchanged (the fence
			// checkRetainedManifestArbitration admits on this kind only).
			if mqttIsRetainedRelSubject(e.subj) {
				continue
			}
			if mqttIsManifestSubject(e.subj) && e.expect != 0 {
				continue
			}
			if e.expect == 0 {
				return fmt.Errorf("an advance-exchange transition carries member %q create-only, "+
					"which would install an obligation rather than advance one", e.subj)
			}
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttExchangeClassTk {
				exchanges++
				continue
			}
			// A HOLD RELEASE (F-105 7c-lc): an ending releases its source and canonical-PUBREL
			// edges in its own batch. checkResourceOverwrites confines such a member to exactly the
			// release this batch's endings justify.
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttResourceClassTk {
				continue
			}
			if _, perr := mqttParseInflightSubject(e.subj); perr == nil {
				if rec, derr := mqttDecodeInflightRecord(e.body); derr == nil && mqttAdvanceKindSlotState(rec) {
					continue // a slot member of the advance; checkAckPhaseAdvanceMembers validates it
				}
			}
			return fmt.Errorf("an advance-exchange transition carries member %q, which is neither an "+
				"exchange it advances nor an acknowledgement slot re-phase", e.subj)
		}
		if exchanges == 0 {
			return fmt.Errorf("an advance-exchange transition carries no exchange to advance")
		}

	case mqttTransitionDiscardExchange:
		// Permitted under ANY predecessor state: the generation it ends is CLOSED, and after a
		// Clean-Start handoff the authority serves a successor. The licence is the exchange's own
		// generation's discarding closure, which checkExchangeOverwrites reads at the boundary.
		if err := mqttCheckEndingPreservesAuthority(t, "a discard-exchange"); err != nil {
			return err
		}
		// EXACTLY ONE EXCHANGE, AN OVERWRITE, AND ITS HOLD RELEASES, AND NOTHING ELSE. Without
		// "nothing else" the kind exempt from the serving requirement would carry registers,
		// actions, inventory or admissions under a closed authority.
		exchanges := 0
		for _, e := range t.Extra {
			if e.expect == 0 {
				return fmt.Errorf("a discard-exchange transition carries member %q create-only, which "+
					"would install an obligation rather than end one", e.subj)
			}
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttExchangeClassTk {
				exchanges++
				continue
			}
			// The discarded obligation's hold releases, confined by checkResourceOverwrites to
			// exactly the edges this ending releases.
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttResourceClassTk {
				continue
			}
			return fmt.Errorf("a discard-exchange transition carries member %q, which is neither the "+
				"exchange it discards nor one of that exchange's hold releases", e.subj)
		}
		if exchanges != 1 {
			return fmt.Errorf("a discard-exchange transition carries %d exchange(s): it ends exactly one", exchanges)
		}

	case mqttTransitionWithdrawUnsent:
		// Permitted under ANY predecessor state for the advance's reason — an obligation of a
		// closed or displaced generation must still be able to end — and preserving the
		// authority exactly as the advance does: the shared rule, not a restatement of it.
		if err := mqttCheckEndingPreservesAuthority(t, "a withdraw-unsent"); err != nil {
			return err
		}
		// EXACTLY ONE REGISTER AND ONE EXCHANGE, BOTH OVERWRITES, OF ONE OBLIGATION, AND
		// NOTHING ELSE. The advance's member gate admits acknowledgement slot states only and is
		// deliberately not broadened: a withdrawal's register stays at publish. Without "nothing
		// else" the kind exempt from the serving requirement would carry admissions, actions,
		// resources or inventory under a closed authority.
		var regGen, regLin, xGen, xNonce string
		regs, xchs := 0, 0
		for _, e := range t.Extra {
			if e.expect == 0 {
				return fmt.Errorf("a withdraw-unsent transition carries member %q create-only, which "+
					"would install an obligation rather than end one", e.subj)
			}
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttExchangeClassTk {
				toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
				if len(toks) != 4 {
					return fmt.Errorf("a withdraw-unsent transition carries a malformed exchange %q", e.subj)
				}
				xGen, xNonce = toks[2], toks[3]
				xchs++
				continue
			}
			// The withdrawn obligation's hold releases (F-105 7c-lc), confined by
			// checkResourceOverwrites to exactly the edges this ending releases.
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttResourceClassTk {
				continue
			}
			if name, perr := mqttParseInflightSubject(e.subj); perr == nil {
				rec, derr := mqttDecodeInflightRecord(e.body)
				if derr != nil {
					return fmt.Errorf("a withdraw-unsent transition carries an unsupported register on %q: %w",
						e.subj, derr)
				}
				regGen, regLin = name.gen, rec.Lin
				regs++
				continue
			}
			return fmt.Errorf("a withdraw-unsent transition carries member %q, which is neither the "+
				"register nor the exchange of the obligation it ends", e.subj)
		}
		if regs != 1 || xchs != 1 {
			return fmt.Errorf("a withdraw-unsent transition carries %d register(s) and %d exchange(s): "+
				"it ends exactly one obligation, as one register and one exchange", regs, xchs)
		}
		if regGen != xGen || regLin != xNonce {
			return fmt.Errorf("a withdraw-unsent transition pairs the register of %q/%q with the exchange "+
				"of %q/%q: both members describe ONE obligation", regGen, regLin, xGen, xNonce)
		}

	case mqttTransitionAbandonDelivery:
		// A DECISION under one connection's entitlement: the predecessor must be SERVING at
		// exactly the attempt's generation and tenure (a takeover changed the tenure, and the
		// displaced connection's decision is refused here), and the authority is otherwise
		// preserved exactly as an ending preserves it.
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing || t.Prev.Gen != a.Gen || t.Prev.Tenure != a.Tenure {
			return fmt.Errorf("an abandon-delivery transition requires the serving authority of its own "+
				"decision's generation and tenure %q/%q", a.Gen, a.Tenure)
		}
		if err := mqttCheckEndingPreservesAuthority(t, "an abandon-delivery"); err != nil {
			return err
		}
		// EXACTLY ONE REGISTER AND ONE EXCHANGE, BOTH OVERWRITES, OF ONE OBLIGATION OF THE
		// DECIDING GENERATION, AND NOTHING ELSE.
		var regGen, regLin, xGen, xNonce string
		regs, xchs := 0, 0
		for _, e := range t.Extra {
			if e.expect == 0 {
				return fmt.Errorf("an abandon-delivery transition carries member %q create-only, which "+
					"would install an obligation rather than dispose of one", e.subj)
			}
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttExchangeClassTk {
				toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
				if len(toks) != 4 {
					return fmt.Errorf("an abandon-delivery transition carries a malformed exchange %q", e.subj)
				}
				xGen, xNonce = toks[2], toks[3]
				xchs++
				continue
			}
			if name, perr := mqttParseInflightSubject(e.subj); perr == nil {
				rec, derr := mqttDecodeInflightRecord(e.body)
				if derr != nil {
					return fmt.Errorf("an abandon-delivery transition carries an unsupported register on %q: %w",
						e.subj, derr)
				}
				regGen, regLin = name.gen, rec.Lin
				regs++
				continue
			}
			return fmt.Errorf("an abandon-delivery transition carries member %q, which is neither the "+
				"register nor the exchange of the delivery it abandons", e.subj)
		}
		if regs != 1 || xchs != 1 {
			return fmt.Errorf("an abandon-delivery transition carries %d register(s) and %d exchange(s): "+
				"it abandons exactly one delivery, as one register and one exchange", regs, xchs)
		}
		if regGen != xGen || regLin != xNonce || regGen != a.Gen {
			return fmt.Errorf("an abandon-delivery transition pairs the register of %q/%q with the exchange "+
				"of %q/%q under generation %q: both members describe ONE delivery of the deciding generation",
				regGen, regLin, xGen, xNonce, a.Gen)
		}

	case mqttTransitionTombstoneRegister:
		// A DECISION under one connection's entitlement, exactly as abandon-delivery: the
		// predecessor must be SERVING at the attempt's generation and tenure, and the authority is
		// otherwise preserved exactly as an ending preserves it.
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing || t.Prev.Gen != a.Gen || t.Prev.Tenure != a.Tenure {
			return fmt.Errorf("a tombstone-register transition requires the serving authority of its own "+
				"decision's generation and tenure %q/%q", a.Gen, a.Tenure)
		}
		if err := mqttCheckEndingPreservesAuthority(t, "a tombstone-register"); err != nil {
			return err
		}
		// EXACTLY ONE MEMBER: a register of the deciding generation whose body is a cancellation
		// tombstone at its publish. An overwrite is judged against its exact occupant by the
		// register boundary; a create-only one is the pending form, whose CAS proves the subject
		// absent.
		if len(t.Extra) != 1 {
			return fmt.Errorf("a tombstone-register transition carries %d members: it cancels exactly one "+
				"allocation, as one register", len(t.Extra))
		}
		e := t.Extra[0]
		name, perr := mqttParseInflightSubject(e.subj)
		if perr != nil {
			return fmt.Errorf("a tombstone-register transition carries member %q, which is not a register", e.subj)
		}
		if name.gen != a.Gen {
			return fmt.Errorf("a tombstone-register transition cancels a register of generation %q under %q",
				name.gen, a.Gen)
		}
		rec, derr := mqttDecodeInflightRecord(e.body)
		if derr != nil {
			return fmt.Errorf("a tombstone-register transition carries an unsupported register on %q: %w", e.subj, derr)
		}
		// An OVERWRITE may carry Retired: a source retired concurrently is preserved, and
		// retirement dominates the cancellation wherever it is read. The create-only (pending)
		// form is never retired: nothing was ever transmitted under it.
		if !rec.Cancelled || (rec.Retired && e.expect == 0) || rec.State != mqttInflightPublish ||
			rec.ActionToken != _EMPTY_ || rec.PI != name.pi || mqttDurIsSlotOnlyClass(rec.Dur) {
			return fmt.Errorf("a tombstone-register transition writes %q with a body that is not an ordinary "+
				"cancellation tombstone at its publish", e.subj)
		}

	case mqttTransitionRebindCanonical:
		// A DECISION under one connection's entitlement, exactly as tombstone-register.
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing || t.Prev.Gen != a.Gen || t.Prev.Tenure != a.Tenure {
			return fmt.Errorf("a rebind-canonical transition requires the serving authority of its own "+
				"decision's generation and tenure %q/%q", a.Gen, a.Tenure)
		}
		if err := mqttCheckEndingPreservesAuthority(t, "a rebind-canonical"); err != nil {
			return err
		}
		// EXACTLY ONE MEMBER: an OVERWRITE of a register of the deciding generation, whose body is
		// an ordinary untokened pubrel_ready slot. The register boundary judges it against its
		// exact occupant and loads the exchange.
		if len(t.Extra) != 1 || t.Extra[0].expect == 0 {
			return fmt.Errorf("a rebind-canonical transition carries %d members: it re-binds exactly one "+
				"standing slot, as one register overwrite", len(t.Extra))
		}
		e := t.Extra[0]
		name, perr := mqttParseInflightSubject(e.subj)
		if perr != nil || name.gen != a.Gen {
			return fmt.Errorf("a rebind-canonical transition carries member %q, which is not a register of "+
				"generation %q", e.subj, a.Gen)
		}
		rec, derr := mqttDecodeInflightRecord(e.body)
		if derr != nil {
			return fmt.Errorf("a rebind-canonical transition carries an unsupported register on %q: %w", e.subj, derr)
		}
		if rec.State != mqttInflightPubRelReady || rec.ActionToken != _EMPTY_ || rec.PubRelStreamSeq == 0 ||
			mqttDurIsSlotOnlyClass(rec.Dur) {
			return fmt.Errorf("a rebind-canonical transition writes %q with a body that is not an ordinary "+
				"untokened pubrel_ready slot", e.subj)
		}

	case mqttTransitionCancelIntent:
		// A preparation whose metadata half was positively aborted returns to serving under
		// its ORIGINAL ownership. It restores; it never installs the proposal.
		if t.Prev == nil || t.Prev.State != mqttAuthorityPreparing || t.Prev.Intent == nil {
			return fmt.Errorf("a cancel-intent transition requires a preparing predecessor carrying its intent")
		}
		if t.Auth.State != mqttAuthorityServing {
			return fmt.Errorf("a cancel-intent transition installs state %d, not serving", t.Auth.State)
		}
		i := t.Prev.Intent
		if a.Intent != i.ID {
			return fmt.Errorf("a cancel-intent transition answers intent %q but its predecessor carries %q",
				a.Intent, i.ID)
		}
		if t.Auth.Gen != i.PrevGen || t.Auth.Tenure != i.PrevTenure || t.Auth.OwnerTxn != i.PrevOwnerTxn {
			return fmt.Errorf("a cancel-intent transition installs %q/%q/%q rather than restoring its predecessor's %q/%q/%q",
				t.Auth.Gen, t.Auth.Tenure, t.Auth.OwnerTxn, i.PrevGen, i.PrevTenure, i.PrevOwnerTxn)
		}
		if t.Closure != nil {
			return fmt.Errorf("a cancel-intent transition writes a generation closure: a cancellation ends nothing")
		}

	case mqttTransitionFenceSharedRelease:
		// A SERVING authority, preserved exactly but for its revision: the ending kinds' rule plus
		// the serving requirement, since a fence is owed only where admission is possible. No
		// member rides on it (mqttKindCarriesMembers refuses Extra) and it writes no closure.
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing {
			return fmt.Errorf("a fence-shared-release transition requires a serving predecessor")
		}
		if err := mqttCheckEndingPreservesAuthority(t, "a fence-shared-release"); err != nil {
			return err
		}

	case mqttTransitionFencePendingDisposal:
		// The same shape as fence-shared-release: a SERVING authority preserved exactly but for its
		// revision, no member, no closure. The entitlement compare is the op's, made against the
		// predecessor this transition CASes (pending_fence_disposal.go).
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing {
			return fmt.Errorf("a fence-pending-disposal transition requires a serving predecessor")
		}
		if err := mqttCheckEndingPreservesAuthority(t, "a fence-pending-disposal"); err != nil {
			return err
		}

	case mqttTransitionAdmitExchange, mqttTransitionAcquireAction:
		// THE ADMISSION RULE. New outbound work requires a predecessor that is SERVING, and
		// preserves its ownership: these operations authorize work under the standing
		// tenure and never move it.
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing {
			return fmt.Errorf("a %s transition requires a serving predecessor", a.Kind)
		}
		if t.Auth.State != mqttAuthorityServing {
			return fmt.Errorf("a %s transition installs state %d, not serving", a.Kind, t.Auth.State)
		}
		if t.Auth.Gen != t.Prev.Gen || t.Auth.Tenure != t.Prev.Tenure || t.Auth.OwnerTxn != t.Prev.OwnerTxn {
			return fmt.Errorf("a %s transition moves ownership, which it may not", a.Kind)
		}
		if t.Closure != nil {
			return fmt.Errorf("a %s transition writes a generation closure", a.Kind)
		}

	case mqttTransitionPrepareIntent:
		// A preparation freezes admission and reserves a handoff. It preserves ownership —
		// that is what makes it a freeze rather than a transfer — and writes no closure.
		if t.Prev == nil || t.Prev.State != mqttAuthorityServing {
			return fmt.Errorf("a prepare-intent transition requires a serving predecessor")
		}
		if t.Auth.State != mqttAuthorityPreparing {
			return fmt.Errorf("a prepare-intent transition installs state %d, not preparing", t.Auth.State)
		}
		if t.Auth.Gen != t.Prev.Gen || t.Auth.Tenure != t.Prev.Tenure || t.Auth.OwnerTxn != t.Prev.OwnerTxn {
			return fmt.Errorf("a prepare-intent transition moves ownership before it is finalized")
		}
		if t.Closure != nil {
			return fmt.Errorf("a prepare-intent transition writes a generation closure: a freeze is not closure")
		}

	case mqttTransitionFinalize:
		// A finalization installs the ownership its own preparation proposed, and NOTHING
		// ELSE. Binding it to the pending intent is what stops a delayed acceptance
		// retargeting a successor: the predecessor must still be preparing, and the intent
		// standing there must be the one being finalized.
		if t.Prev == nil || t.Prev.State != mqttAuthorityPreparing || t.Prev.Intent == nil {
			return fmt.Errorf("a finalize transition requires a preparing predecessor carrying its intent")
		}
		i := t.Prev.Intent
		// IT MUST NAME THE INTENT IT ANSWERS. Reading whatever intent the predecessor
		// happens to carry is not the same as finalizing the one this operation was for: a
		// caller that reloaded and followed a different pending intent would otherwise be
		// admitted here.
		if a.Intent != i.ID {
			return fmt.Errorf("a finalize transition answers intent %q but its predecessor carries %q",
				a.Intent, i.ID)
		}
		if t.Auth.State != mqttAuthorityServing {
			return fmt.Errorf("a finalize transition installs state %d, not serving", t.Auth.State)
		}
		if t.Auth.Gen != i.Gen || t.Auth.Tenure != i.Tenure {
			return fmt.Errorf("a finalize transition installs %q/%q but its pending intent proposed %q/%q",
				t.Auth.Gen, t.Auth.Tenure, i.Gen, i.Tenure)
		}
		if t.Auth.OwnerTxn != a.Txn {
			return fmt.Errorf("a finalize transition installs ownership %q rather than its own identity %q",
				t.Auth.OwnerTxn, a.Txn)
		}
		// A finalization that discards its predecessor closes that generation in the same
		// batch, and one that resumes must not close anything.
		wantClose := i.Retire != nil && i.Retire.Discard
		if wantClose {
			if t.Closure == nil {
				return fmt.Errorf("a finalize transition discarding %q writes no closure for it", i.PrevGen)
			}
			if t.Closure.Gen != i.PrevGen {
				return fmt.Errorf("a finalize transition closes %q but its intent discards %q",
					t.Closure.Gen, i.PrevGen)
			}
			if !t.Closure.Discard {
				return fmt.Errorf("a finalize transition discarding %q writes a non-discarding closure", i.PrevGen)
			}
		} else if t.Closure != nil {
			return fmt.Errorf("a finalize transition writes a closure its intent did not request")
		}

	case mqttTransitionCloseAdmit:
		// Closing admission installs the closed state AND its permanent record, in one
		// batch, for the generation being closed. Either without the other would leave the
		// two halves of one fact able to disagree.
		if t.Prev == nil {
			return fmt.Errorf("a close-admission transition requires a predecessor")
		}
		if t.Auth.State != mqttAuthorityClosed {
			return fmt.Errorf("a close-admission transition installs state %d, not closed", t.Auth.State)
		}
		if t.Closure == nil {
			return fmt.Errorf("a close-admission transition writes no permanent closure")
		}
		if t.Closure.Gen != t.Prev.Gen || t.Auth.Gen != t.Prev.Gen {
			return fmt.Errorf("a close-admission transition closes %q over a predecessor at %q",
				t.Closure.Gen, t.Prev.Gen)
		}
		// Closing admission ends a generation; it does not move ownership, and it may not
		// silently discard a preparation whose handoff nobody resolved. A PREPARING
		// predecessor is cancelled or finalized first.
		if t.Auth.Tenure != t.Prev.Tenure || t.Auth.OwnerTxn != t.Prev.OwnerTxn {
			return fmt.Errorf("a close-admission transition moves ownership from %q/%q to %q/%q",
				t.Prev.Tenure, t.Prev.OwnerTxn, t.Auth.Tenure, t.Auth.OwnerTxn)
		}
		if t.Prev.State == mqttAuthorityPreparing {
			return fmt.Errorf("a close-admission transition displaces a preparing authority with an unresolved handoff")
		}
		// AND IT DOES NOT DISCARD. The closure's disposition is what a later reader consults to
		// decide whether to SERVE this generation's obligations or DROP them, and a discard is
		// authorized by a retirement intent — which this kind does not carry. A discarding
		// close-admission would be that authorization asserted by whoever assembled the batch,
		// with nothing behind it. Ending a generation and abandoning its obligations are
		// different facts; only one of them has an author here.
		if t.Closure.Discard {
			return fmt.Errorf("a close-admission transition writes a DISCARDING closure for %q: "+
				"ending a generation is not authority to abandon its obligations, and the "+
				"authorization for that is a retirement intent this kind does not carry",
				t.Closure.Gen)
		}
		// A RESERVING predecessor IS closable, and that is the recoverable abort of a first
		// creation: the reservation is never deleted — no arbitration record is — its
		// generation is simply closed, having never served and never able to, and a later
		// attempt reserves a fresh generation beside it, through reserve-creation again.

	case mqttTransitionCloseTeardown:
		// The AUTHORIZED discarding closure of a standalone teardown. It is close-admission
		// with one difference — it MAY discard — and every other structural fact is the
		// same: it closes the generation the predecessor serves, in one batch with the
		// permanent closure, and moves no ownership.
		if t.Prev == nil {
			return fmt.Errorf("a close-teardown transition requires a predecessor")
		}
		// It discards a SERVING session. A reservation aborts non-discardingly through
		// close-admission, and a preparation must be resolved before its generation ends;
		// neither is a teardown-discard target, and both fall to recovery rather than here.
		if t.Prev.State != mqttAuthorityServing {
			return fmt.Errorf("a close-teardown transition displaces an authority in state %d; "+
				"a discarding teardown ends a SERVING session", t.Prev.State)
		}
		if t.Auth.State != mqttAuthorityClosed {
			return fmt.Errorf("a close-teardown transition installs state %d, not closed", t.Auth.State)
		}
		if t.Closure == nil {
			return fmt.Errorf("a close-teardown transition writes no permanent closure")
		}
		if t.Closure.Gen != t.Prev.Gen || t.Auth.Gen != t.Prev.Gen {
			return fmt.Errorf("a close-teardown transition closes %q over a predecessor at %q",
				t.Closure.Gen, t.Prev.Gen)
		}
		if t.Auth.Tenure != t.Prev.Tenure || t.Auth.OwnerTxn != t.Prev.OwnerTxn {
			return fmt.Errorf("a close-teardown transition moves ownership from %q/%q to %q/%q",
				t.Prev.Tenure, t.Prev.OwnerTxn, t.Auth.Tenure, t.Auth.OwnerTxn)
		}
		// AND IT DISCARDS, on the authority of the standing retirement the operation read
		// off the durable receipt. A non-discarding close is close-admission's business;
		// this kind exists only for the discard, and the receipt is what a
		// close-admission lacks. The retirement must be present, discard, and name the
		// exact generation this batch closes — the same binding a finalization's
		// retirement gets — so a receipt about another generation cannot authorize this
		// one. The affine member, when present, is bound to this retirement's targets by
		// mqttCheckClosingEnrolment.
		if !t.Closure.Discard {
			return fmt.Errorf("a close-teardown transition writes a non-discarding closure for %q: "+
				"a non-discarding close is a close-admission, and this kind exists for the discard",
				t.Closure.Gen)
		}
		ret := mqttStandingRetirement(t)
		if ret == nil {
			return fmt.Errorf("a close-teardown transition discards %q with no standing retirement to authorize it",
				t.Closure.Gen)
		}
		if !ret.Discard {
			return fmt.Errorf("a close-teardown transition carries a retirement that does not discard")
		}
		if ret.Gen != t.Closure.Gen {
			return fmt.Errorf("a close-teardown transition closes %q but its retirement names %q",
				t.Closure.Gen, ret.Gen)
		}
		if !mqttValidRetirementReason(ret.Reason) {
			return fmt.Errorf("a close-teardown transition carries a retirement of unsupported reason %q", ret.Reason)
		}

	default:
		return fmt.Errorf("transition kind %q carries no rule", a.Kind)
	}
	return nil
}

// mqttTransitionMutationDigest assembles the transition's complete mutation and digests it.
//
// One function, used by the writer to mint the receipt and by the resolver to verify it, so
// the two can never disagree about what the batch contained. The closure is included: it is
// a batch member, and omitting it made a transition with a closure and one without produce
// the same digest.
func mqttTransitionMutationDigest(t *mqttAuthorityTransition) (string, error) {
	if t == nil || t.Attempt == nil || t.Auth == nil {
		return _EMPTY_, fmt.Errorf("unreachable: digesting a transition with no attempt or authority")
	}
	authBody, err := json.Marshal(t.Auth)
	if err != nil {
		return _EMPTY_, err
	}
	members := make([]mqttTransitionMember, 0, len(t.Extra)+1)
	if t.Closure != nil {
		cb, cerr := json.Marshal(t.Closure)
		if cerr != nil {
			return _EMPTY_, cerr
		}
		members = append(members, mqttTransitionMember{
			subj:   mqttGenClosureSubject(getHash(t.Attempt.ID), t.Closure.Gen),
			expect: 0,
			body:   cb,
		})
	}
	members = append(members, t.Extra...)
	return mqttTransitionDigest(t.Attempt, t.PrevSeq, authBody, members), nil
}

// mqttCheckTransitionMember classifies one caller-supplied batch member and refuses
// anything this writer is not entitled to publish.
//
// The batch is per stream and newAtomicBatchRequest publishes each subject separately, so a
// member naming another stream is not part of any transaction: it is routed elsewhere or
// nowhere, and the acknowledgement cannot establish otherwise. Confinement is therefore a
// correctness property and not tidiness.
//
// Beyond confinement the member must be a record class this transition may write, for this
// client, in a shape the register boundary would accept. Atomicity commits what it is given
// and validates ORDERING, never meaning — so without this the transition is a way to write
// an arbitrary body onto a register subject while bypassing every existing validating
// writer.
func mqttCheckTransitionMember(a *mqttTransitionAttempt, idHash, closureGen string,
	e mqttTransitionMember, seen map[string]struct{}) error {

	if e.subj == _EMPTY_ {
		return fmt.Errorf("transition %q carries a member with no subject", a.Txn)
	}
	if strings.ContainsAny(e.subj, "*> 	") {
		return fmt.Errorf("transition %q carries member %q, which is not a publishable subject", a.Txn, e.subj)
	}
	if _, dup := seen[e.subj]; dup {
		return fmt.Errorf("transition %q names subject %q more than once", a.Txn, e.subj)
	}
	// STREAM CONFINEMENT. A batch is per stream; a foreign subject is not in the
	// transaction at all.
	if !strings.HasPrefix(e.subj, mqttInflightStreamSubjectPrefix) {
		return fmt.Errorf("transition %q carries member %q, which is not on the outbound in-flight stream",
			a.Txn, e.subj)
	}
	if mqttIsF105ArbitrationSubject(e.subj) {
		return fmt.Errorf("transition %q carries member %q, which is an arbitration record this writer owns",
			a.Txn, e.subj)
	}
	// The obligation classes an admission installs are carried BY DESIGN, and are checked
	// against this transition's own identity rather than merely permitted.
	if cls, ok := mqttF105ObligationClass(e.subj); ok {
		return mqttCheckObligationMember(a, idHash, closureGen, cls, e)
	}
	// THE DIRECTORY CLASSES ARE ADMITTED ONLY BY A CLOSING TRANSITION, and only far enough to
	// reach the batch-level rules in checkRecoveryEnrolment, which decide everything about them:
	// the ordinal against the stored header, the bound against the ledger this batch seals, the
	// identity, the birth cursor, and that neither may appear without the other. A transition
	// that writes no closure never gets here, so no admission, disposal or acknowledgement can
	// carry a root — enrolling a generation is an act of closing it and of nothing else.
	if mqttIsShardSubject(e.subj) || mqttIsRootSubject(e.subj) {
		if closureGen == _EMPTY_ {
			return fmt.Errorf("transition %q carries directory member %q, which only a closing "+
				"transition may carry: a generation is enrolled for recovery by the act that "+
				"closes it", a.Txn, e.subj)
		}
		return nil
	}
	// A NAME-OWNERSHIP member is admitted far enough to reach the batch-level rule in
	// checkNameOwnershipMembers, which decides everything about it: the kind that may carry it,
	// its transition legality against the standing record, and that its expectation matches.
	// Here it is only confirmed to be this client's and to decode. It is keyed by the physical
	// NAME and NOT by generation, so — unlike a register — it is not checked against a.Gen.
	if mqttIsNameOwnershipSubject(e.subj) {
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 3 {
			return fmt.Errorf("transition %q carries a malformed ownership subject %q", a.Txn, e.subj)
		}
		// THE CLIENT IS A BODY ATTRIBUTE, NOT A SUBJECT TOKEN (Codex consult 12): the slot is
		// keyed by the physical stream and name so a DELETE issued for that pair — which carries
		// no client — finds the one slot. The body is still validated to be THIS client's, here.
		o, derr := mqttDecodeNameOwnership(e.body, a.ID, a.Epoch)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported ownership body on %q: %w", a.Txn, e.subj, derr)
		}
		// THE BODY'S STREAM AND NAME ARE THE SUBJECT'S. The subject carries only a hash of the
		// name, so a body naming a different consumer would be attributed to the wrong slot;
		// reconstructing the subject from the body is what pins the write to its own name.
		if mqttNameOwnershipSubject(o.Stream, o.Name) != e.subj {
			return fmt.Errorf("transition %q carries ownership member %q whose body names %q/%q, which "+
				"hashes to a different subject", a.Txn, e.subj, o.Stream, o.Name)
		}
		return nil
	}
	if mqttIsF105Subject(e.subj) {
		return fmt.Errorf("transition %q carries member %q, which is under the arbitration "+
			"prefix but is no record class this broker writes", a.Txn, e.subj)
	}

	// The remaining classes on this stream are the register and its unsent marker. Both
	// are attributed to a client and a generation by their own names, and a transition may
	// only carry its OWN client's.
	name, perr := mqttParseInflightSubject(e.subj)
	if perr != nil {
		if reg, merr := mqttParseInflightMarkerSubject(e.subj); merr == nil {
			name = reg
		} else {
			return fmt.Errorf("transition %q carries member %q, which is neither a register nor its marker: %v",
				a.Txn, e.subj, perr)
		}
	}
	if name.idHash != idHash {
		return fmt.Errorf("transition %q carries member %q, which belongs to another client", a.Txn, e.subj)
	}
	if name.gen != a.Gen && !mqttKindEndsEarlierGeneration(a.Kind) {
		return fmt.Errorf("transition %q carries member %q, which names generation %q rather than this transition's %q",
			a.Txn, e.subj, name.gen, a.Gen)
	}
	// AN ADVANCE MAY RE-PHASE AN EARLIER GENERATION'S SLOT, for the same reason it may name an
	// earlier generation's exchange: a G1 obligation must reach its end while G2 serves, and the
	// only authority whose CAS can succeed is the current one. The exemption is safe because
	// checkAckPhaseAdvanceMembers refuses every in-flight member an advance carries that is not a
	// genuine acknowledgement arrow BOUND to a same-generation exchange advance in the batch — so
	// an arbitrary or foreign-generation register cannot reach the store by way of this relaxation.

	// A REGISTER BODY GOES THROUGH THE SAME SHAPE MATRIX every other register writer uses.
	if _, isReg := mqttParseInflightSubject(e.subj); isReg == nil {
		// Through THE ONE decode API, never a local json.Unmarshal: that function is the
		// single place the closed schema and the legal-shape matrix are applied on read, and
		// a second decoder here would be a second definition of what a register is.
		rec, derr := mqttDecodeInflightRecord(e.body)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported register body on %q: %w", a.Txn, e.subj, derr)
		}
		// AND THE BODY'S IDENTIFIER IS THE SUBJECT'S. The subject is where the slot lives and
		// the body is what it says, so a register stored under one identifier while claiming
		// another is a record every reader refuses and no writer can clean up: recovery
		// treats the disagreement as unreadable and retirement declines to delete what it
		// cannot interpret, so the allocation is stranded for the epoch. The operation
		// derives the subject from the body and cannot produce this; a member composed some
		// other way can, which is the whole reason this check is here and not only there.
		if rec.PI != name.pi {
			return fmt.Errorf("transition %q carries a register on %q whose body allocates "+
				"identifier %d: a slot is named by its subject and says what it is in its body, "+
				"and a record disagreeing with its own name can be read by nobody",
				a.Txn, e.subj, rec.PI)
		}
		// A CANCELLATION'S PROVENANCE IS MINTED BY ITS WRITE MODE, and only here can the mode be
		// seen (F-088, consult C4). A pending fence is a CREATE-ONLY tombstone: the create is
		// what proves the subject was empty, so `pending-create` is legal only on a create-only
		// member of a tombstone-register transition. Every cancellation written over an occupant
		// is a CAS, so `occupant` is never legal on a create. A body that disagrees with its own
		// write mode would let a CAS mint a fence (disposable by provenance alone) over an
		// admitted exchange's identity carrier.
		switch rec.CancelOrigin {
		case mqttCancelOriginPendingCreate:
			if a.Kind != mqttTransitionTombstoneRegister || e.expect != 0 {
				return fmt.Errorf("transition %q of kind %q carries a pending cancellation fence on %q at "+
					"expectation %d: a fence is written only by a create-only tombstone-register member",
					a.Txn, a.Kind, e.subj, e.expect)
			}
		case mqttCancelOriginOccupant:
			if e.expect == 0 {
				return fmt.Errorf("transition %q carries an occupant cancellation on %q as a create: "+
					"an occupant cancellation is written over the occupant it cancels", a.Txn, e.subj)
			}
		}
		// A REPLACING REGISTER IS VALIDATED, NOT REFUSED — but not HERE.
		//
		// Until the ordinary-PUBLISH path was converted this arm refused a nonzero
		// expectation outright, because replacement was the slot state machine's decision
		// applied by storeInflightRegister, and validating an expectation here as well would
		// have been a SECOND definition of a legal register step.
		//
		// The conversion removed the first definition instead of adding a second: the
		// machine's decision is now carried out INSIDE the admitting batch. What a
		// replacement turns on — the occupant's delivery, its disposition, its QoS and its
		// lineage, and whether the batch continues or validly ends its obligation — is not
		// in this member, so it cannot be decided from this member. checkRegisterReplacements
		// reads the exact occupant, re-runs the machine's gate and validates the resulting
		// state; it runs unconditionally over every member carrying a nonzero expectation on
		// a register subject, so nothing reaches the store by way of this return.
		return nil
	}

	// AND SO DOES THE MARKER. It was exempt here on the reasoning that its own builder
	// validates it, which is the builder-only guarantee this boundary exists to replace: a
	// member composed by hand reaches the store without ever meeting that builder, and an
	// unattributed marker body is exactly what makes a resumed PUBLISH undecidable between
	// a first transmission and a retransmission.
	if _, derr := mqttDecodeInflightMarker(e.body); derr != nil {
		return fmt.Errorf("transition %q carries an unsupported unsent marker on %q: %w", a.Txn, e.subj, derr)
	}
	// A MARKER IS CREATE-ONLY EXCEPT AS HALF OF A PAIR REPLACEMENT. Its whole meaning is
	// presence: it is minted with the register it names and ENDED by a delete at its own
	// sequence. A free-standing overwrite would let a transmitted allocation be described as
	// unsent again — the DUP flag lying in the direction [MQTT-3.3.1-1] forbids — at a
	// sequence a concurrent transmission's delete is racing.
	//
	// The one legitimate overwrite is the pair replacement, where the occupant's marker is
	// displaced at its OBSERVED sequence by the marker of the allocation replacing it, so no
	// interval shows a bare register or a marker naming a record that is gone. Whether this
	// member is that half cannot be told from this member: it depends on a replacing register
	// standing beside it on the same slot, and on the occupant that register names.
	// checkRegisterReplacements decides it, and refuses a marker overwrite with no replacing
	// register beside it — so nothing reaches the store by way of this return.
	return nil
}

// mqttCheckObligationMember validates an exchange or action member against the transition
// carrying it.
//
// Attribution is by DECODE, not by subject shape: the subject says which client, generation
// and exchange a body claims to be about, and the decoders refuse a body that disagrees with
// its own name. That is what stops a transition installing a well-named obligation whose
// contents belong to something else.
// mqttKindEndsEarlierGeneration reports whether a kind may carry a register or exchange member of
// a generation other than the attempt's: the two kinds that END an existing obligation, because a
// G1 obligation must reach its end while G2 serves and only the current authority's CAS can
// succeed. Each is bound by its own member rules — the advance by checkAckPhaseAdvanceMembers,
// the withdrawal by its kind arm (one register and one exchange of ONE obligation, same
// generation) and by the boundary's withdrawal rule — so no arbitrary earlier-generation member
// rides on the exemption.
func mqttKindEndsEarlierGeneration(k mqttTransitionKind) bool {
	return k == mqttTransitionAdvanceExchange || k == mqttTransitionWithdrawUnsent ||
		k == mqttTransitionDiscardExchange
}

func mqttCheckObligationMember(a *mqttTransitionAttempt, idHash, closureGen, cls string,
	e mqttTransitionMember) error {

	toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
	switch cls {
	case mqttRelClassTk:
		return mqttCheckRetainedRelMember(a, idHash, e)
	case mqttExchangeClassTk:
		// <cls>.<idHash>.<gen>.<nonce>
		if len(toks) != 4 {
			return fmt.Errorf("transition %q carries a malformed exchange subject %q", a.Txn, e.subj)
		}
		if toks[1] != idHash {
			return fmt.Errorf("transition %q carries exchange %q, which belongs to another client", a.Txn, e.subj)
		}
		// AN ADVANCE MAY NAME AN EARLIER GENERATION, for the reason the kind exists: a G1
		// obligation must be able to reach its end while G2 serves, and the only authority
		// whose CAS can succeed is the current one. Every other kind still admits only its
		// own generation — an ADMISSION naming another generation's exchange would install
		// an obligation in a generation that may already be closed.
		if !mqttKindEndsEarlierGeneration(a.Kind) && toks[2] != a.Gen {
			return fmt.Errorf("transition %q carries exchange %q, which names generation %q rather than %q",
				a.Txn, e.subj, toks[2], a.Gen)
		}
		if !mqttValidSessGen(toks[2]) {
			return fmt.Errorf("transition %q carries exchange %q with a malformed generation token",
				a.Txn, e.subj)
		}
		if _, derr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, toks[2], toks[3]); derr != nil {
			return fmt.Errorf("transition %q carries an unsupported exchange on %q: %w", a.Txn, e.subj, derr)
		}
	case mqttActionClassTk:
		// <cls>.<idHash>.<gen>.<nonce>.<acq>
		if len(toks) != 5 {
			return fmt.Errorf("transition %q carries a malformed action subject %q", a.Txn, e.subj)
		}
		if toks[1] != idHash {
			return fmt.Errorf("transition %q carries action %q, which belongs to another client", a.Txn, e.subj)
		}
		if toks[2] != a.Gen {
			return fmt.Errorf("transition %q carries action %q, which names generation %q rather than %q",
				a.Txn, e.subj, toks[2], a.Gen)
		}
		// AN ACTION IS NEVER REWRITTEN, so every action member is create-only, whichever
		// kind carries it. Its Run, Conn and Tenure are the ORIGINAL binding a captured
		// completion handler resolves against; an overwrite would redirect an acquired
		// operation through a rebound connection, which is the one thing the record exists
		// to make impossible. A retransmission is a new acquisition counter, not an
		// overwrite of the attempt that may still be executing.
		if e.expect != 0 {
			return fmt.Errorf("transition %q overwrites action %q, which is never rewritten: "+
				"a retransmission is a new acquisition, not a replacement of the one that may "+
				"still be executing", a.Txn, e.subj)
		}
		acq, cerr := strconv.ParseUint(toks[4], 10, 64)
		if cerr != nil || strconv.FormatUint(acq, 10) != toks[4] {
			return fmt.Errorf("transition %q carries action %q with a malformed acquisition token", a.Txn, e.subj)
		}
		if _, derr := mqttDecodeOutboundAction(e.body, a.ID, a.Epoch, toks[2], toks[3], acq); derr != nil {
			return fmt.Errorf("transition %q carries an unsupported action on %q: %w", a.Txn, e.subj, derr)
		}
	case mqttResourceClassTk:
		// <cls>.<idHash>.<gen>.<inc>
		if len(toks) != 4 {
			return fmt.Errorf("transition %q carries a malformed resource subject %q", a.Txn, e.subj)
		}
		if toks[1] != idHash {
			return fmt.Errorf("transition %q carries resource %q, which belongs to another client", a.Txn, e.subj)
		}
		// A RESOURCE MAY BELONG TO AN EARLIER GENERATION, and for dispose-resource it
		// usually does: cleanup of G1 runs while G2 serves, which is the whole reason that
		// kind exists. Requiring the current generation made deferred cleanup impossible at
		// exactly the moment it is needed — the successor's authority names G2, and
		// retrying under G1's old authority fails its CAS instead.
		//
		// Every other kind still admits only its own generation: an ADMISSION that could
		// name another generation's resource would let a fresh obligation take a dependency
		// on something a closed generation owns.
		// A CLOSING TRANSITION'S ENROLMENT NAMES THE GENERATION ITS CLOSURE NAMES, and that is
		// the only cross-generation permission besides disposal's. It is narrow in both
		// directions: closureGen is empty unless this transition WRITES the closure, and the
		// enrolment's own rules below require the resource to be the affine consumer enrolled
		// there. A finalization that installs a successor may therefore enrol the predecessor's
		// affine consumer, and may do nothing else across the boundary.
		// AND AN ENDING KIND'S HOLD RELEASE names its obligation's own generation, which is an
		// earlier one when a G1 obligation ends while G2 serves (F-105 7c-lc, P-4); what such a
		// member may change is confined by checkResourceOverwrites.
		if a.Kind != mqttTransitionDisposeResource && !mqttKindEndsEarlierGeneration(a.Kind) &&
			toks[2] != a.Gen && !(closureGen != _EMPTY_ && toks[2] == closureGen) {
			return fmt.Errorf("transition %q carries resource %q, which names generation %q rather than %q",
				a.Txn, e.subj, toks[2], a.Gen)
		}
		if !mqttValidSessGen(toks[2]) {
			return fmt.Errorf("transition %q carries resource %q with a malformed generation token",
				a.Txn, e.subj)
		}
		if _, derr := mqttDecodeOutboundResource(e.body, a.ID, a.Epoch, toks[2], toks[3]); derr != nil {
			return fmt.Errorf("transition %q carries an unsupported resource on %q: %w", a.Txn, e.subj, derr)
		}
	case mqttLedgerClassTk, mqttDutyClassTk, mqttManifestClassTk, mqttChunkClassTk:
		// DECIDED BY checkInventoryMembers and checkManifestMembers, NOT HERE, and the delegation is explicit for the
		// same reason the register arms' is. This function sees one member at a time, and every
		// rule that matters about an inventory member is about the batch: an allocation is
		// legal only beside its ledger CAS, the ordinals must be exactly the declared interval,
		// and a continuation is legal only against the entry standing at its expectation. A
		// per-member verdict here could only be a weaker restatement of one of those, and a
		// second statement of a rule is how the two drift apart.

	default:
		return fmt.Errorf("transition %q carries member %q of unknown obligation class %q", a.Txn, e.subj, cls)
	}
	return nil
}

// checkExchangeOverwrites validates every exchange member that overwrites, against the
// exchange as it stands.
//
// The expected sequence is not merely "some sequence": the record read back must BE the one
// at that sequence, or the comparison would be against a body this transition is not
// conditioned on. The CAS would reject such a write anyway, but refusing here says why and
// keeps the check honest — a comparison against the wrong version proves nothing.
//
// ONLY THE PHASE MAY DIFFER, and only along the protocol's own successor relation. Both
// halves matter: the first stops a completion carrying an identifier reallocation or a
// re-attribution, and the second stops a terminal obligation being resurrected or a QoS 2
// release being skipped.
//
// It RETURNS the validated standing phase of every exchange it compared, keyed by subject: the
// slot-member binding needs the exchange's predecessor, and one read and one sequence check are
// the only version of it the batch is conditioned on (a second read could see another).
func (jsa *mqttJSA) checkExchangeOverwrites(a *mqttTransitionAttempt,
	extra []mqttTransitionMember) (map[string]mqttExchangePhase, error) {

	pred := make(map[string]mqttExchangePhase)
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttExchangeClassTk {
			continue
		}
		// A create-only member installs a NEW exchange and has nothing to be compared
		// against. Which kinds may do that is the kind rules' question, not this one's.
		if e.expect == 0 {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			return nil, fmt.Errorf("transition %q carries a malformed exchange subject %q", a.Txn, e.subj)
		}
		gen, nonce := toks[2], toks[3]

		smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, e.subj)
		if lerr != nil {
			return nil, fmt.Errorf("transition %q advances exchange %q, which could not be read: %w",
				a.Txn, nonce, lerr)
		}
		if smsg.Sequence != e.expect {
			return nil, fmt.Errorf("transition %q advances exchange %q at sequence %d but it stands at %d",
				a.Txn, nonce, e.expect, smsg.Sequence)
		}
		cur, derr := mqttDecodeOutboundExchange(smsg.Data, a.ID, a.Epoch, gen, nonce)
		if derr != nil {
			return nil, fmt.Errorf("transition %q advances exchange %q, whose standing record is "+
				"unsupported: %w", a.Txn, nonce, derr)
		}
		next, nerr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, gen, nonce)
		if nerr != nil {
			return nil, fmt.Errorf("transition %q carries an unsupported exchange on %q: %w",
				a.Txn, e.subj, nerr)
		}

		// Compare by rebuilding the standing record with the proposed phase. Anything else
		// that differs then shows up as a mismatch, including a member added later that
		// nobody thought to enumerate here.
		want := *cur
		want.Phase = next.Phase
		// THE ABANDONMENT FACT is introduced by the abandonment's own transition and by nothing
		// else, onto an exchange that has none; every other rewrite carries it exactly (compared
		// by value). The paired register rule proves the introduced fact's binding.
		if a.Kind == mqttTransitionAbandonDelivery && cur.Abandoned == nil && next.Abandoned != nil {
			want.Abandoned = next.Abandoned
		}
		if !mqttExchangeEqual(&want, next) {
			return nil, fmt.Errorf("transition %q rewrites exchange %q beyond its phase: an advance "+
				"carries a completion, never a reallocation, a re-attribution or a changed abandonment", a.Txn, nonce)
		}
		if (next.Phase == mqttExchangeWithdrawnUnsent) != (a.Kind == mqttTransitionWithdrawUnsent) {
			// A WITHDRAWAL IS ITS KIND'S AND ONLY ITS KIND'S. Under any other kind the phase would
			// end a delivery with none of the evidence the withdrawal rule loads; and a
			// withdraw-unsent transition that moves its exchange anywhere else is not one.
			return nil, fmt.Errorf("transition %q of kind %q moves exchange %q to phase %d: only a "+
				"withdraw-unsent transition writes WithdrawnUnsent, and it writes nothing else",
				a.Txn, a.Kind, nonce, next.Phase)
		}
		if (next.Phase == mqttExchangeDeliveryAbandoned) != (a.Kind == mqttTransitionAbandonDelivery) {
			// AN ABANDONMENT IS ITS KIND'S AND ONLY ITS KIND'S, for the withdrawal's reason.
			return nil, fmt.Errorf("transition %q of kind %q moves exchange %q to phase %d: only an "+
				"abandon-delivery transition writes DeliveryAbandoned, and it writes nothing else",
				a.Txn, a.Kind, nonce, next.Phase)
		}
		if (next.Phase == mqttExchangeDiscarded) != (a.Kind == mqttTransitionDiscardExchange) {
			// A DISCARD IS ITS KIND'S AND ONLY ITS KIND'S, for the withdrawal's reason.
			return nil, fmt.Errorf("transition %q of kind %q moves exchange %q to phase %d: only a "+
				"discard-exchange transition writes Discarded, and it writes nothing else",
				a.Txn, a.Kind, nonce, next.Phase)
		}
		if next.Phase == mqttExchangeDiscarded {
			// NOT a protocol advance and absent from the successor table. Only an obligation that
			// has not released its holds is discarded: an ended one keeps its own ending, which is
			// the evidence it already carries (F-105 7d).
			if mqttExchangeReleasesHolds(cur.Phase) {
				return nil, fmt.Errorf("transition %q discards exchange %q from phase %d, which already "+
					"released its holds: an ending is preserved, never rewritten", a.Txn, nonce, cur.Phase)
			}
			// THE LICENCE: this exchange's own generation is committed DISCARDING. The closure is
			// create-only and immutable, so what is read here cannot become false later; an
			// unreadable, absent or non-discarding closure licenses nothing.
			if lerr := jsa.requireDiscardingClosure(a.ID, a.Epoch, gen); lerr != nil {
				return nil, fmt.Errorf("transition %q discards exchange %q: %w", a.Txn, nonce, lerr)
			}
		} else if next.Phase == mqttExchangeDeliveryAbandoned {
			// Absent from the successor table. Only a PUBLISH delivery is abandoned; the paired
			// register rule (checkAbandonmentRegister) proves which delivery and under what binding.
			if cur.Phase != mqttExchangePublish {
				return nil, fmt.Errorf("transition %q abandons exchange %q from phase %d: only a PUBLISH "+
					"delivery can be abandoned", a.Txn, nonce, cur.Phase)
			}
		} else if next.Phase == mqttExchangeWithdrawnUnsent {
			// NOT a protocol advance and absent from the successor table, like the supersession.
			// Only a PUBLISH obligation can have been unsent. The evidence that it WAS — the
			// tokenless register and the standing marker of its allocation — is
			// checkRegisterReplacements' withdrawal rule, over the paired register member.
			if cur.Phase != mqttExchangePublish {
				return nil, fmt.Errorf("transition %q withdraws exchange %q from phase %d: only a "+
					"PUBLISH obligation can have been unsent", a.Txn, nonce, cur.Phase)
			}
		} else if next.Phase == mqttExchangeSupersededUnsent {
			// A PROVEN-UNSENT SUPERSESSION is NOT a generic protocol advance — it is deliberately
			// absent from the successor table (F-105 step 5, Codex consult 15). It ends a PUBLISH
			// obligation whose packet identifier a pair replacement is re-allocating to a fresh
			// delivery. The only legal predecessor state is PUBLISH: nothing past it was ever
			// unsent, so nothing past it may be superseded. That this ending is a REAL replacement
			// — a fresh obligation re-allocates the identifier, never a lone ending that would drop
			// the delivery with no client answer — is proven by checkSupersededPredecessors over
			// the batch's resulting state.
			if cur.Phase != mqttExchangePublish {
				return nil, fmt.Errorf("transition %q supersedes exchange %q from phase %d: only a PUBLISH "+
					"obligation is proven-unsent and may be superseded", a.Txn, nonce, cur.Phase)
			}
		} else if serr := mqttExchangePhaseSucceeds(cur.QoS, cur.Phase, next.Phase); serr != nil {
			return nil, fmt.Errorf("transition %q advances exchange %q illegally: %w", a.Txn, nonce, serr)
		}
		if mqttExchangePhaseEnded(next.Phase) && !mqttExchangePhaseEnded(cur.Phase) &&
			!mqttExchangePhaseEndedPendingDrain(cur.Phase) {
			if err := jsa.checkEndingBesideNoToken(a, extra, gen, cur); err != nil {
				return nil, err
			}
		}
		pred[e.subj] = cur.Phase
	}
	return pred, nil
}

// checkEndingBesideNoToken is the action-drain half of an ending (F-112): no batch moves an
// exchange DIRECTLY into an identifier ending — from a phase that is not pending-drain — while the
// standing register naming its lineage holds an output token.
//
// The pending-drain phases exist so that an ending with output still executing is recorded as
// pending-drain, and their arrow to the ending IS the drain's completion, so it is exempt. Where
// the batch carries the register itself the per-arrow guards decide (the publish terminal arrow,
// PUBCOMP's pending-drain rule, the withdrawal's tokenless occupant, checkAckPhaseAdvanceMembers);
// this covers the producers that carry no register member. Together with "no action is acquired
// at an ended phase" (mqttActionPermittedAtPhase, enforced by checkActionMembers) it is what lets
// the floor's reclamation predicate read "ended, and the register no longer names the lineage" as
// "no action of it can still emit": an action emits only while its token stands.
func (jsa *mqttJSA) checkEndingBesideNoToken(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	gen string, cur *mqttOutboundExchange) error {

	subj := mqttInflightSubject(getHash(a.ID), gen, cur.PI)
	for _, e := range extra {
		if e.subj == subj {
			return nil // the per-arrow register rules decide this batch
		}
	}
	reg, smsg, rerr := jsa.loadInflightRecord(subj)
	switch {
	case rerr != nil:
		return fmt.Errorf("transition %q ends exchange %q, whose register on %q could not be read: %w",
			a.Txn, cur.Nonce, subj, rerr)
	case reg == nil && smsg != nil:
		return fmt.Errorf("transition %q ends exchange %q while the register on %q cannot be interpreted: "+
			"it may name the obligation and hold its token", a.Txn, cur.Nonce, subj)
	case reg != nil && reg.Lin == cur.Nonce && reg.ActionToken != _EMPTY_:
		return fmt.Errorf("transition %q ends exchange %q while its register on %q holds output token %q: "+
			"an ending beside a live holder would let a transmission escape after its own release",
			a.Txn, cur.Nonce, subj, reg.ActionToken)
	}
	return nil
}

// checkSupersededPredecessors requires every proven-unsent supersession to be the predecessor
// half of a pair replacement: its identifier must be re-allocated to a FRESHLY ADMITTED
// obligation in the same batch.
//
// A mqttExchangeSupersededUnsent member standing alone would end a proven-unsent obligation with
// no client answer and no successor — silently dropping the delivery, which is exactly the loss
// this gate refuses. Tying the ending to a co-admitted fresh obligation on the same identifier is
// what makes "reached ONLY through a validated proven-unsent replacement" a boundary rule rather
// than a convention of the one caller that composes it.
func (jsa *mqttJSA) checkSupersededPredecessors(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	freshPI := map[uint16]struct{}{}
	var superseded []*mqttOutboundExchange
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttExchangeClassTk {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			continue
		}
		x, derr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, toks[2], toks[3])
		if derr != nil || x == nil {
			continue // an undecodable member is refused by mqttCheckObligationMember
		}
		if e.expect == 0 {
			// A create-only member ADMITS a fresh obligation; its identifier is the one a
			// supersession may re-allocate.
			freshPI[x.PI] = struct{}{}
			continue
		}
		if x.Phase == mqttExchangeSupersededUnsent {
			superseded = append(superseded, x)
		}
	}
	for _, x := range superseded {
		if _, ok := freshPI[x.PI]; !ok {
			return fmt.Errorf("transition %q supersedes proven-unsent obligation %q on identifier %d "+
				"without admitting a fresh obligation to re-allocate it: a supersession is the "+
				"predecessor half of a pair replacement, never a lone ending that drops the delivery",
				a.Txn, x.Nonce, x.PI)
		}
	}
	return nil
}

// batchExchangeState is every exchange this batch leaves standing, keyed "gen.nonce".
//
// A member-by-member check reads the STORE, which is the state BEFORE this batch — and a batch
// is atomic, so what an action has to be legal against is the state the batch RESULTS IN. The
// two differ exactly when one batch both acquires an action and advances that exchange, which
// is how a PUBLISH could be acquired beside the very advance that ends the obligation: each
// half is legal alone, and together they install emitting output for an exchange the same
// batch made releasable.
func (jsa *mqttJSA) batchExchangeState(a *mqttTransitionAttempt,
	extra []mqttTransitionMember) (map[string]*mqttOutboundExchange, error) {

	out := map[string]*mqttOutboundExchange{}
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttExchangeClassTk {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			return nil, fmt.Errorf("transition %q carries a malformed exchange subject %q", a.Txn, e.subj)
		}
		x, derr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, toks[2], toks[3])
		if derr != nil {
			return nil, fmt.Errorf("transition %q carries an unsupported exchange on %q: %w",
				a.Txn, e.subj, derr)
		}
		out[toks[2]+"."+toks[3]] = x
	}
	return out, nil
}

// checkCreatedExchanges requires every exchange a transition INSTALLS to start at the
// beginning of its protocol, under this transition and this tenure.
//
// A create-only member has nothing to compare against, which is why it was skipped — and that
// is exactly what made it the way round the protocol: an admission whose exchange body says
// TERMINAL commits, because the successor relation is only consulted for an ADVANCE. A
// dependency release then accepts that fabricated ending and authorizes deleting a consumer
// the client's real obligation still needs. The phase relation is only a protocol if the
// first phase is also enforced.
func (jsa *mqttJSA) checkCreatedExchanges(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	// ONE IDENTIFIER, ONE OUTSTANDING OBLIGATION, WITHIN A BATCH.
	//
	// Two exchanges left outstanding at one identifier are two messages a client can only
	// answer ambiguously: an acknowledgement names the identifier and nothing else. Their
	// nonces differ, so their subjects differ and every other rule is satisfied — and a
	// registerless admission has no register to collide with — which makes the combination
	// unsound while every member is legal.
	//
	// Computed from the batch's RESULTING state, so an ADVANCE counts too: a batch that moves
	// exchange A along its protocol while admitting B at A's identifier is the same collision
	// reached through two different kinds of member. An exchange whose resulting phase is an
	// ENDING has given the identifier up and does not collide.
	resulting, rerr := jsa.batchExchangeState(a, extra)
	if rerr != nil {
		return rerr
	}
	allocated := map[string]string{}
	for key, x := range resulting {
		if mqttExchangePhaseEnded(x.Phase) {
			continue
		}
		slot := x.Gen + "." + strconv.FormatUint(uint64(x.PI), 10)
		if prev, dup := allocated[slot]; dup && prev != x.Nonce {
			return fmt.Errorf("transition %q leaves obligations %q and %q outstanding on one "+
				"identifier %d: an acknowledgement names the identifier and nothing else, so "+
				"two outstanding obligations sharing one cannot be answered",
				a.Txn, prev, x.Nonce, x.PI)
		}
		allocated[slot] = x.Nonce
		_ = key
	}
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttExchangeClassTk || e.expect != 0 {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			return fmt.Errorf("transition %q carries a malformed exchange subject %q", a.Txn, e.subj)
		}
		gen, nonce := toks[2], toks[3]
		// AND ONLY THE KIND THAT ADMITS MAY ADMIT. The kind rules confine members for every
		// kind that has a member rule, and the ownership kinds have none — so a
		// close-admission over a reservation whose metadata half was ABORTED could carry a
		// fresh exchange and its action, and a generation that never became SERVING would
		// acquire emitting output in the transaction that permanently closes it.
		if a.Kind != mqttTransitionAdmitExchange {
			return fmt.Errorf("transition %q of kind %q admits the exchange on %q: new work is "+
				"admitted by an admission, under an authority that is SERVING", a.Txn, a.Kind, e.subj)
		}
		x, derr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, gen, nonce)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported exchange on %q: %w", a.Txn, e.subj, derr)
		}
		if x.Phase != mqttExchangePublish {
			return fmt.Errorf("transition %q admits exchange %q at phase %d: an obligation is "+
				"admitted at its publish and reaches every other phase by an advance the "+
				"successor relation licenses", a.Txn, nonce, x.Phase)
		}
		if x.AdmitTxn != a.Txn {
			return fmt.Errorf("transition %q admits exchange %q, which names admitting "+
				"transition %q: an obligation nothing authorized is an obligation nobody owes",
				a.Txn, nonce, x.AdmitTxn)
		}
		if x.Gen != a.Gen || x.Tenure != a.Tenure {
			return fmt.Errorf("transition %q admits exchange %q under %q/%q but the transition "+
				"itself carries %q/%q", a.Txn, nonce, x.Gen, x.Tenure, a.Gen, a.Tenure)
		}

	}
	return nil
}

// checkPairBindings requires an admitted exchange and the register it is admitted with to
// describe ONE obligation.
//
// The builder checks it, which is what made it a caller convention: every binding a batch can
// state about itself still holds for a body substituted after the builder ran. Change only the
// exchange's PI and both records pass their own validation — the exchange's shape is right,
// the register's shape is right — and the batch installs an obligation whose allocation
// disagrees with the register recovery will read. An acquisition then derives its identifier
// from the exchange while recovery derives the slot from the register, so one obligation holds
// two identifiers and neither side can tell.
//
// And it is two-directional: an exchange admitted with no register is refused as surely as a
// register allocated with no exchange.
func (jsa *mqttJSA) checkPairBindings(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	type slot struct {
		reg    *mqttInflightRecord
		marker *mqttInflightMarker
	}
	slots := map[string]*slot{}
	// Keyed by LINEAGE as well, because that is what an exchange is matched by — and a map
	// has no order, so "the first register carrying this lineage" is not a rule at all.
	byLin := map[string]*mqttInflightRecord{}
	// Which of those lineages arrived on a REPLACING register, so the admission requirement
	// below can exempt a continued obligation without exempting a fresh one.
	replaces := map[string]bool{}
	for _, e := range extra {
		if mqttIsF105Subject(e.subj) {
			continue
		}
		if name, perr := mqttParseInflightSubject(e.subj); perr == nil {
			rec, derr := mqttDecodeInflightRecord(e.body)
			if derr != nil {
				return fmt.Errorf("transition %q carries an unsupported register on %q: %w", a.Txn, e.subj, derr)
			}
			key := name.gen + "." + strconv.FormatUint(uint64(name.pi), 10)
			if slots[key] == nil {
				slots[key] = &slot{}
			}
			slots[key].reg = rec
			// ONE ALLOCATION PER OBLIGATION, WITHIN A BATCH. Two registers carrying one
			// lineage at different identifiers are two recovery allocations for a single
			// exchange, and the exchange ledger — and the dependency accounting that rests
			// on it — knows about one. Reached by ADDING a member rather than substituting
			// one, which is why matching the exchange against any single register is not
			// enough.
			if prev, dup := byLin[name.gen+"."+rec.Lin]; dup {
				return fmt.Errorf("transition %q carries two registers for obligation %q, "+
					"allocating identifiers %d and %d: an obligation holds ONE allocation",
					a.Txn, rec.Lin, prev.PI, rec.PI)
			}
			byLin[name.gen+"."+rec.Lin] = rec
			if e.expect != 0 {
				replaces[name.gen+"."+rec.Lin] = true
			}
			continue
		}
		if name, merr := mqttParseInflightMarkerSubject(e.subj); merr == nil {
			m, derr := mqttDecodeInflightMarker(e.body)
			if derr != nil {
				return fmt.Errorf("transition %q carries an unsupported marker on %q: %w", a.Txn, e.subj, derr)
			}
			key := name.gen + "." + strconv.FormatUint(uint64(name.pi), 10)
			if slots[key] == nil {
				slots[key] = &slot{}
			}
			slots[key].marker = m
			continue
		}
	}

	// A MARKER IS MINTED WITH ITS REGISTER AND NEVER ON ITS OWN.
	//
	// The marker is present ⇔ nothing has been transmitted under this pair identity, and a
	// transmission records itself by DELETING it. So a marker written alone is a claim that
	// an allocation is unsent — and after a transmission has deleted one, its subject is
	// free, so a create-only write succeeds and the claim is FALSE: recovery reads a
	// matching marker, calls the delivery a first transmission and puts DUP=0 on a
	// retransmission, which is the one direction [MQTT-3.3.1-1] forbids.
	//
	// Requiring the register in the same batch is what makes the marker's presence mean
	// something, because a register is create-only here too: the pair is minted once, as one
	// act, and the marker's absence afterwards is the transmission's record.
	for key, sl := range slots {
		if sl.marker == nil {
			continue
		}
		if sl.reg == nil {
			return fmt.Errorf("transition %q writes the unsent marker for %q on its own: a "+
				"marker is minted with the register it names, and one written alone claims an "+
				"allocation is unsent after a transmission deleted the claim", a.Txn, key)
		}
		if sl.marker.Nonce != sl.reg.Nonce {
			return fmt.Errorf("transition %q pairs the register on %q with a marker naming "+
				"allocation %q rather than its own %q", a.Txn, key, sl.marker.Nonce, sl.reg.Nonce)
		}
	}

	// AND EVERY REGISTER IS ADMITTED WITH ITS OBLIGATION. The binding was one-directional —
	// an exchange had to agree with the register beside it, and a register alone was nobody's
	// — so a batch carrying only a fresh register and its marker allocated an identifier with
	// no obligation to answer for it, and every create-only expectation succeeded because the
	// standing obligation it collided with had never occupied that subject.
	admitted := map[string]struct{}{}
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttExchangeClassTk || e.expect != 0 {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) == 4 {
			admitted[toks[2]+"."+toks[3]] = struct{}{}
		}
	}
	for gl, rec := range byLin {
		gen := gl[:len(gl)-len(rec.Lin)-1]
		if _, ok := admitted[gen+"."+rec.Lin]; ok {
			continue
		}
		// DELEGATED TO checkRegisterReplacements FOR A REPLACING REGISTER — and delegation
		// is what this is, not an exemption.
		//
		// The rule here is "every allocation has an obligation behind it". For a CREATED
		// register that obligation is admitted in this batch, which this function can see.
		// For a REPLACING one it may be a STANDING obligation the client already holds — a
		// register-only replacement inherits its occupant's lineage precisely because
		// admitting a second one would duplicate its charge and its dependency edges — and
		// whether it exists is a question about the store, which this function does not ask.
		//
		// checkRegisterReplacements discharges the same rule for those members: it runs
		// unconditionally over every member carrying an expectation, re-reads the occupant,
		// requires an inherited lineage to EQUAL the standing record's, and then LOADS that
		// lineage's exchange and requires it to be readable and to agree with the occupant on
		// identifier and QoS. So the obligation is proved, by the function that can see it;
		// what is not done is proving it twice from two different sets of facts.
		if replaces[gl] {
			continue
		}
		// A CANCELLATION TOMBSTONE ALLOCATES NOTHING (F-105 7b). A tombstone-register
		// transition's create-only member records that an allocation whose own store is still in
		// flight was cancelled: it occupies the subject so that store's create-only CAS fails, and
		// no obligation stands behind it by construction. Its kind arm admits exactly that shape.
		if a.Kind == mqttTransitionTombstoneRegister && rec.Cancelled && !rec.Retired {
			continue
		}
		return fmt.Errorf("transition %q allocates identifier %d for obligation %q, which "+
			"it does not admit: an allocation with no obligation behind it answers to "+
			"nothing and excludes nobody", a.Txn, rec.PI, rec.Lin)
	}

	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttExchangeClassTk || e.expect != 0 {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			continue
		}
		x, derr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, toks[2], toks[3])
		if derr != nil {
			continue // its own check reports this
		}
		// AN EXCHANGE IS ADMITTED ONLY WITH ITS REGISTER. The admission allocates the
		// obligation's identifier, and the register is where recovery reads that allocation; an
		// exchange admitted alone would be an obligation no slot answers for, and every
		// acknowledgement route refuses an ordinary slot without its exchange
		// (ackExchangeFor), so neither half could ever be completed. Every production admission
		// composes the pair in one batch (mqttAdmitPublishTransition, the replacement
		// composer); the rule stands here so it holds for a batch assembled by anything at all.
		// Looked up by LINEAGE rather than by identifier, because the identifier is exactly
		// what a substitution changes.
		reg := byLin[x.Gen+"."+x.Nonce]
		// THE EXCHANGE NONCE IS THE REGISTER'S LINEAGE. Lin is per-OBLIGATION where the
		// register's own nonce is per-ALLOCATION, so the obligation is identified by exactly
		// that value — and after an allocation replacement nothing could reconstruct which
		// exchange a register belonged to, because the identifier is reusable.
		if reg == nil {
			if len(slots) == 0 {
				return fmt.Errorf("transition %q admits exchange %q with no register beside it: an "+
					"exchange is admitted only with the register that allocates its identifier",
					a.Txn, x.Nonce)
			}
			return fmt.Errorf("transition %q admits exchange %q beside registers none of "+
				"which carries that lineage: the exchange nonce IS the obligation lineage",
				a.Txn, x.Nonce)
		}
		if x.QoS != reg.QoS {
			return fmt.Errorf("transition %q admits exchange %q at QoS %d beside a register "+
				"at QoS %d", a.Txn, x.Nonce, x.QoS, reg.QoS)
		}
		if reg.PI != x.PI {
			return fmt.Errorf("transition %q admits exchange %q allocating identifier %d "+
				"beside a register allocating %d", a.Txn, x.Nonce, x.PI, reg.PI)
		}
		// AND THE REGISTER IS AT THE BEGINNING TOO. An exchange is admitted at its PUBLISH,
		// so the slot it is admitted with must be at the state that means the same thing. A
		// register already at publish_terminal is one recovery treats as completed and
		// eligible for terminal cleanup, while this mechanism would go on permitting another
		// PUBLISH and holding the dependency edges open; one at a PUBREL state describes an
		// exchange that has already transferred ownership, which an admission has not.
		if reg.State != mqttInflightPublish {
			return fmt.Errorf("transition %q admits exchange %q at its publish beside a "+
				"register in state %q: the pair describes one obligation, and an admission "+
				"is its beginning", a.Txn, x.Nonce, reg.State)
		}
	}
	return nil
}

// checkIdentifierExclusivity requires an identifier a batch claims to be held by NOTHING ELSE
// that is still outstanding — including records this batch does not touch.
//
// The batch's own members are not the whole state. Two REGISTERLESS admissions committed one
// after the other, each legal on its own, each carrying a single exchange, leave two
// obligations outstanding at one identifier: the second batch's collision map contains one
// exchange, and their create-only subjects differ because the nonces differ. That is the
// registerless route specifically — a registered admission is already exclusive, because the
// register's subject IS the identifier and it is written create-only, and the boundary ties
// the exchange to it by lineage, identifier and QoS.
//
// The walk runs for EVERY admission. Scoping it to the registerless form was wrong, and the
// counterexample is short: admit A registerless on identifier 7, then admit B WITH a register
// on identifier 7. A never occupied that register subject, so B's create-only write succeeds,
// B's exchange and marker subjects are fresh too, and both obligations stand on one
// identifier. A create-only member excludes another writer at ITS OWN KEY; it says nothing
// about a different key describing the same thing.
//
// AN UNREADABLE OBLIGATION REFUSES: a record this broker cannot interpret may be the one
// holding the identifier, and treating it as absent is the inference this whole mechanism
// exists to remove. So does an INCOMPLETE enumeration, for the same reason one step further
// out — an obligation the walk never returned is one this claim never saw. The walk is the
// generation's inventory [Lo, Hi] (F-112), fenced by the ledger CAS this same batch carries.
func (jsa *mqttJSA) checkIdentifierExclusivity(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	resulting, rerr := jsa.batchExchangeState(a, extra)
	if rerr != nil {
		return rerr
	}
	claimed := map[string]string{}
	gens := map[string]struct{}{}
	for _, e := range extra {
		cls, ok := mqttF105ObligationClass(e.subj)
		if !ok || cls != mqttExchangeClassTk || e.expect != 0 {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			continue
		}
		x := resulting[toks[2]+"."+toks[3]]
		if x == nil || mqttExchangePhaseEnded(x.Phase) {
			continue
		}
		slot := x.Gen + "." + strconv.FormatUint(uint64(x.PI), 10)
		claimed[slot] = x.Nonce
		gens[x.Gen] = struct{}{}
	}
	if len(claimed) == 0 {
		return nil
	}

	idHash := getHash(a.ID)
	for gen := range gens {
		// THE ENUMERATION IS THE INVENTORY, read at the ledger sequence this batch CASes (F-112).
		// Every exchange's duty is allocated in the batch that creates it, so [Lo, Hi] covers every
		// obligation the floor has not proven ended and reclaimed; and because this batch's own
		// ledger member expects the sequence read here, an admission or a floor advance landing
		// after the read makes this batch lose its CAS. A subject walk could not be fenced that
		// way, and since exchange records are now deleted behind the floor its paging could step
		// over a live subject (a deletion between pages shifts the rest down).
		var ledgerExpect uint64
		ledgerFound := false
		for _, e := range extra {
			if mqttIsLedgerSubject(e.subj) && e.subj == mqttGenLedgerSubject(idHash, gen) {
				ledgerExpect, ledgerFound = e.expect, true
			}
		}
		if !ledgerFound {
			// A CREATED EXCHANGE WITHOUT ITS LEDGER is refused by the coverage rule
			// (checkInventoryMembers, later in this same send): an exchange owes its duty, and its
			// duty owes the ledger CAS. That rule is the one that says why; this check has no
			// fenced inventory to read for it.
			continue
		}
		census, cerr := mqttTraverseGenerationInventory(jsa, nil, a.ID, a.Epoch, gen, nil)
		if cerr != nil {
			return fmt.Errorf("transition %q could not enumerate generation %q's obligations, "+
				"so the identifier it claims cannot be shown to be free: %w", a.Txn, gen, cerr)
		}
		if census.ledgerSeq != ledgerExpect {
			return fmt.Errorf("transition %q enumerated generation %q's obligations at ledger sequence "+
				"%d but CASes %d: the identifier was shown free in an inventory this batch is not "+
				"conditioned on", a.Txn, gen, census.ledgerSeq, ledgerExpect)
		}
		if len(census.uncertain) != 0 {
			// AN OBLIGATION THIS BROKER CANNOT READ may be the one holding the identifier.
			return fmt.Errorf("transition %q found %d unreadable position(s) in generation %q's "+
				"inventory (%s), which may hold the identifier it claims", a.Txn, len(census.uncertain),
				gen, census.uncertain[0])
		}
		for _, x := range census.live {
			// A member of THIS batch is the batch's own statement about that obligation, and
			// the collision rules have already compared them with each other.
			if _, mine := resulting[gen+"."+x.Nonce]; mine {
				continue
			}
			if mqttExchangePhaseEnded(x.Phase) {
				continue
			}
			slot := gen + "." + strconv.FormatUint(uint64(x.PI), 10)
			if mine, dup := claimed[slot]; dup {
				return fmt.Errorf("transition %q admits obligation %q on identifier %d, which "+
					"obligation %q still holds at phase %d: an acknowledgement names the "+
					"identifier and nothing else", a.Txn, mine, x.PI, x.Nonce, x.Phase)
			}
		}
	}
	return nil
}

// checkCreatedResources requires every resource a transition INSTALLS to start LIVE, owing
// nothing and authorizing nothing.
//
// Create-only resource members were skipped for the same reason, with the same consequence: a
// hand-composed provisioning could install a resource already naming a dependent obligation
// that does not exist — which no release can ever discharge, because a release has to prove
// that obligation ended — or install one already DELETE_AUTHORIZED, which is the authorization
// this mechanism exists to make provable.
func (jsa *mqttJSA) checkCreatedResources(a *mqttTransitionAttempt, prev *mqttOutboundAuthority,
	extra []mqttTransitionMember) error {
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttResourceClassTk || e.expect != 0 {
			continue
		}
		// AND ONLY TWO KINDS MAY DO IT. Provisioning is new work and requires a serving
		// authority; disposal is permitted after closure, so a creation carried by a
		// disposal would provision under a closed generation through the other kind.
		//
		// A CLOSING KIND IS THE SECOND, and only for the closure's ENROLMENT: the affine
		// consumer is keyed by a generation that outlives incarnations, and closure is the last
		// act able to allocate the ordinal its cleanup duty needs.
		// mqttCheckClosingEnrolment validates that shape on the same batch — the kind, the
		// generation, and the one-to-one binding to the duties allocated beside it — so this
		// rule admits the kind and that one decides the content.
		if a.Kind != mqttTransitionProvisionResource && !mqttKindCloses(a.Kind) {
			return fmt.Errorf("transition %q of kind %q creates the resource on %q: only a "+
				"provision-resource transition provisions one, and only a closing transition "+
				"enrols one", a.Txn, a.Kind, e.subj)
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			return fmt.Errorf("transition %q carries a malformed resource subject %q", a.Txn, e.subj)
		}
		gen, inc := toks[2], toks[3]
		r, derr := mqttDecodeOutboundResource(e.body, a.ID, a.Epoch, gen, inc)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported resource on %q: %w", a.Txn, e.subj, derr)
		}
		if r.State != mqttResourceLive {
			return fmt.Errorf("transition %q provisions resource %q in state %d: a resource is "+
				"provisioned LIVE and reaches every other state by a step the machine licenses",
				a.Txn, inc, r.State)
		}
		if len(r.Deps) != 0 {
			return fmt.Errorf("transition %q provisions resource %q already needed by %d "+
				"obligation(s): an edge is taken by the admission that creates its obligation, "+
				"and one named here could never be released", a.Txn, inc, len(r.Deps))
		}
		if r.AuthTxn != _EMPTY_ || r.Outcome != _EMPTY_ {
			return fmt.Errorf("transition %q provisions resource %q already carrying an "+
				"authorization or an outcome", a.Txn, inc)
		}
		// NOR A DISPOSAL DECISION (F-105 7c-lc, holistic H-1). A decision is introduced on a LIVE
		// resource by a dispose transition under the serving entitlement it names; a resource
		// CREATED carrying one would bypass both that licence and the step that admits it, and the
		// authorization would then trust a decision no route took.
		if r.Disposal != nil {
			return fmt.Errorf("transition %q provisions resource %q already carrying a disposal "+
				"decision: a consumer is disposed of by the route that ends it, never at its creation",
				a.Txn, inc)
		}
		// THE PROVISIONING TENURE IS THE ONE THAT SERVES (F-105 7f). The schema requires a
		// well-formed one on a logical-use consumer; this proves it is TRUE — the tenure of the
		// serving authority the provisioning displaces — so a hand-built batch cannot stamp a
		// foreign tenure that a later `tenure-superseded` decision would then accept against a
		// consumer its own tenure claimed. Immutability (mqttCheckResourceStep) preserves it.
		if mqttResourceLogicalUseKind(r.Kind) {
			if prev == nil || prev.State != mqttAuthorityServing || prev.Gen != r.Gen ||
				prev.Tenure != r.ProvTenure {
				return fmt.Errorf("transition %q provisions resource %q under tenure %q, which is not "+
					"the tenure serving generation %q", a.Txn, inc, r.ProvTenure, r.Gen)
			}
		}
	}
	return nil
}

// checkProposedGenerationIsFresh refuses a generation that has ever been CLOSED.
//
// Closure is permanent and per generation, and the kind rules compare only with the IMMEDIATE
// predecessor — so after G1 is closed, G2 served and G2 closed, nothing stopped a claim
// reserving G1 again with fresh transition and proposal identities. Its metadata half commits,
// its finalization installs SERVING at G1, and new work enters a generation whose permanent
// closure says it admits none. The next closure then fails outright, because that slot is
// already taken.
//
// An unreadable or indeterminate closure REFUSES. An absence is what licenses the claim, so
// this is exactly the inference that may not be drawn from a failed read.
func (jsa *mqttJSA) checkProposedGenerationIsFresh(a *mqttTransitionAttempt, gen string) error {
	c, _, st := jsa.loadGenClosure(a.ID, a.Epoch, gen)
	switch st {
	case mqttOutboundAbsent:
		return nil
	case mqttOutboundValid:
		return fmt.Errorf("transition %q proposes generation %q, which transition %q closed "+
			"permanently: a closed generation admits nothing, and closure is never reopened",
			a.Txn, gen, c.Txn)
	}
	return fmt.Errorf("transition %q proposes generation %q whose permanent closure could not "+
		"be read: a claim rests on the ABSENCE of one, which a failed read does not establish",
		a.Txn, gen)
}

// checkNameOwnershipMembers re-derives every physical-name ownership member against the
// standing record, so the logical-use fence is a boundary property and not a composer
// convention (F-105 cutover, consult 4).
//
// KEYED ON THE MEMBER, like every rule in this file: an ownership write IS a claim or a
// retirement whatever kind carries it, so a rule scoped to one kind would leave the identical
// bypass one kind name away. What it enforces:
//
//   - the KIND may carry it: a LIVE claim only on provision-resource, a RETIRED retirement
//     only on dispose-resource. No other kind may touch a name's ownership;
//   - the EXPECTATION matches the standing record's sequence (0 when absent), so a composer
//     cannot claim create-only over a name that already stands, nor CAS a stale sequence;
//   - the transition is one mqttNameOwnershipTransitionLegal admits: absent -> LIVE(owner),
//     LIVE(owner) -> LIVE(owner) (idempotent), LIVE(owner) -> RETIRED(owner). A second owner
//     claiming a LIVE name, and any revival of a RETIRED one, are refused against the record
//     as it actually stands in the store, not as the caller described it.
func (jsa *mqttJSA) checkNameOwnershipMembers(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	for _, e := range extra {
		if !mqttIsNameOwnershipSubject(e.subj) {
			continue
		}
		next, derr := mqttDecodeNameOwnership(e.body, a.ID, a.Epoch)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported ownership body on %q: %w", a.Txn, e.subj, derr)
		}
		switch a.Kind {
		case mqttTransitionProvisionResource:
			if next.State != mqttNameOwnLive {
				return fmt.Errorf("transition %q provisions ownership of %q in state %d: a provision claims a "+
					"name LIVE", a.Txn, next.Name, next.State)
			}
		case mqttTransitionDisposeResource:
			if next.State != mqttNameOwnRetired {
				return fmt.Errorf("transition %q disposes ownership of %q in state %d: a disposal retires it",
					a.Txn, next.Name, next.State)
			}
		default:
			return fmt.Errorf("transition %q of kind %q carries an ownership member for %q: only a "+
				"provision-resource (claim LIVE) or a dispose-resource (retire) transition may",
				a.Txn, a.Kind, next.Name)
		}
		prev, prevSeq, lerr := jsa.loadNameOwnership(a.ID, a.Epoch, next.Stream, next.Name)
		if lerr != nil {
			return fmt.Errorf("transition %q: %w", a.Txn, lerr)
		}
		if e.expect != prevSeq {
			return fmt.Errorf("transition %q writes ownership of %q at expectation %d, but it stands at %d: "+
				"the claim must CAS the exact record it read", a.Txn, next.Name, e.expect, prevSeq)
		}
		if terr := mqttNameOwnershipTransitionLegal(prev, next); terr != nil {
			return fmt.Errorf("transition %q: %w", a.Txn, terr)
		}
	}
	return mqttCheckRetirementPairing(a, extra)
}

// mqttCheckRetirementPairing requires a logical-use consumer's deletion authorization and its
// name's retirement to ride ONE batch, in both directions (F-105 7c-lc, Codex consult 14 finding 1).
//
// The two records state one fact — this physical name is spent, and this incarnation's deletion is
// authorized — and the sweeps read the ownership half alone. A resource authorized without its
// name retired would leave a LIVE name over a consumer cleanup is about to delete; a name retired
// without its resource's authorization would let a sweep delete a consumer whose resource still
// says LIVE. The composer pairs them; this refuses a batch assembled by anything else.
func mqttCheckRetirementPairing(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	type ownKey struct {
		stream mqttOwnedStream
		name   string
	}
	retiring := map[ownKey]*mqttNameOwnership{}
	for _, e := range extra {
		if !mqttIsNameOwnershipSubject(e.subj) {
			continue
		}
		o, derr := mqttDecodeNameOwnership(e.body, a.ID, a.Epoch)
		if derr != nil || o.State != mqttNameOwnRetired {
			continue
		}
		retiring[ownKey{o.Stream, o.Name}] = o
	}
	paired := map[ownKey]bool{}
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttResourceClassTk {
			continue
		}
		var r mqttOutboundResource
		if json.Unmarshal(e.body, &r) != nil || !mqttResourceLogicalUseKind(r.Kind) {
			continue
		}
		// An authorization IN THIS BATCH: a completion carries the earlier authorizing transition.
		if r.State != mqttResourceDeleteAuthorized || r.AuthTxn != a.Txn {
			continue
		}
		stream, _ := mqttOwnedStreamForResourceKind(r.Kind)
		k := ownKey{stream, r.Name}
		o := retiring[k]
		if o == nil {
			return fmt.Errorf("transition %q authorizes deleting %s consumer %q without retiring its name "+
				"%q in the same batch", a.Txn, r.Kind, r.Inc, r.Name)
		}
		if o.OwnerGen != r.Gen || o.OwnerInc != r.Inc {
			return fmt.Errorf("transition %q authorizes deleting %q/%q but retires name %q for owner %q/%q",
				a.Txn, r.Gen, r.Inc, r.Name, o.OwnerGen, o.OwnerInc)
		}
		paired[k] = true
	}
	for k, o := range retiring {
		if !paired[k] {
			return fmt.Errorf("transition %q retires name %q of %q/%q without authorizing that resource's "+
				"deletion in the same batch", a.Txn, o.Name, o.OwnerGen, o.OwnerInc)
		}
	}
	return nil
}

// checkActionMembers requires every acquired action to be one its EXCHANGE permits.
//
// Keyed on the member and not the kind, for the reason every check in this file is: an action
// member is an acquisition whichever transition carries it, and a rule scoped to
// acquire-action would leave the identical bypass one kind name away.
//
// What the member alone establishes is its shape and its create-only expectation. What it
// cannot establish is anything about the OBLIGATION it emits for: whether that exchange
// exists, whether it is still at a phase this packet belongs to, and whether the identifier
// the packet will carry is the one the client was told. Without this the operation surface's
// own check would be the only one — a composition guarantee, which is the class this boundary
// exists to remove, and a hand-composed action would put a PUBLISH on the wire for an
// exchange the client already refused.
func (jsa *mqttJSA) checkActionMembers(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	// THE STATE THIS BATCH RESULTS IN, not the one it found. An exchange this batch installs
	// or ADVANCES is read from the batch: a batch is atomic, so an action acquired beside an
	// advance that ends its exchange is emitting output for an obligation that is over the
	// moment both land.
	resulting, berr := jsa.batchExchangeState(a, extra)
	if berr != nil {
		return berr
	}
	// ONE ACQUISITION PER OBLIGATION, WITHIN A BATCH. Two actions for one exchange land
	// together and are therefore executable together, which is exactly the serialization the
	// acquisition rules exist to impose: another transmission WAITS while an acquired action
	// may still emit. Their subjects differ — the acquisition counter is part of the name —
	// so nothing about the members themselves objects, and each is individually legal
	// against the same phase.
	acquired := map[string]uint64{}

	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttActionClassTk {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 5 {
			return fmt.Errorf("transition %q carries a malformed action subject %q", a.Txn, e.subj)
		}
		gen, nonce := toks[2], toks[3]
		acq, perr := strconv.ParseUint(toks[4], 10, 64)
		if perr != nil {
			return fmt.Errorf("transition %q carries action subject %q with no acquisition counter",
				a.Txn, e.subj)
		}
		// AN ACTION IS ACQUIRED BY AN ACQUISITION, for the same reason an exchange is
		// admitted by an admission: the ownership kinds confine their members by nothing, so
		// a closure could otherwise carry emitting output for the generation it is ending.
		if a.Kind != mqttTransitionAcquireAction {
			return fmt.Errorf("transition %q of kind %q acquires the action on %q: emitting "+
				"output is acquired by an acquisition, under an authority that is SERVING",
				a.Txn, a.Kind, e.subj)
		}
		if prev, dup := acquired[gen+"."+nonce]; dup {
			return fmt.Errorf("transition %q acquires two actions for exchange %q, at "+
				"acquisitions %d and %d: an acquisition is serialized against the output it "+
				"would race, and two that land together race each other", a.Txn, nonce, prev, acq)
		}
		acquired[gen+"."+nonce] = acq
		act, derr := mqttDecodeOutboundAction(e.body, a.ID, a.Epoch, gen, nonce, acq)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported action on %q: %w", a.Txn, e.subj, derr)
		}
		// AND IT IS ATTRIBUTED TO THE TRANSITION AND TENURE THAT ACQUIRED IT. Both are
		// immutable evidence a captured completion handler resolves against, and the decoder
		// checks only that they are well-formed tokens: an action naming the tenure a resume
		// displaced would be permanent evidence attributed to ownership that did not
		// authorize it.
		if act.AcqTxn != a.Txn {
			return fmt.Errorf("transition %q carries an action on %q naming acquiring "+
				"transition %q", a.Txn, e.subj, act.AcqTxn)
		}
		if act.Tenure != a.Tenure {
			return fmt.Errorf("transition %q carries an action on %q acquired under tenure %q, "+
				"not this transition's %q", a.Txn, e.subj, act.Tenure, a.Tenure)
		}

		x, ok := resulting[gen+"."+nonce]
		if !ok {
			smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName,
				mqttExchangeSubject(getHash(a.ID), gen, nonce))
			if lerr != nil {
				return fmt.Errorf("transition %q acquires an action for exchange %q, which could "+
					"not be read: %w", a.Txn, nonce, lerr)
			}
			sx, xerr := mqttDecodeOutboundExchange(smsg.Data, a.ID, a.Epoch, gen, nonce)
			if xerr != nil {
				return fmt.Errorf("transition %q acquires an action for exchange %q, whose "+
					"standing record is unsupported: %w", a.Txn, nonce, xerr)
			}
			x = sx
		}
		if perr := mqttActionPermittedAtPhase(x.QoS, x.Phase, act.Kind); perr != nil {
			return fmt.Errorf("transition %q acquires a %q for exchange %q: %w",
				a.Txn, act.Kind, nonce, perr)
		}
		// THE IDENTIFIER IS THE EXCHANGE'S. An action carrying another one would put bytes
		// on the wire that disagree with what the client was told, and the allocation is the
		// exchange's to state.
		if act.PI != x.PI {
			return fmt.Errorf("transition %q acquires an action carrying identifier %d for "+
				"exchange %q, which holds %d", a.Txn, act.PI, nonce, x.PI)
		}
		// AND THE REGISTER IT IS ACQUIRED ON STATES THE EXCHANGE'S SHARED DEPENDENCY, or none: the
		// pair rule, checked here too so a hand-composed batch cannot emit over a contradiction.
		for _, re := range extra {
			if _, perr := mqttParseInflightSubject(re.subj); perr != nil {
				continue
			}
			if rec, derr := mqttDecodeInflightRecord(re.body); derr == nil && rec.Lin == nonce {
				if cerr := mqttCheckSharedAcqPair(rec, x); cerr != nil {
					return fmt.Errorf("transition %q acquires an action for exchange %q: %w", a.Txn, nonce, cerr)
				}
			}
		}
	}
	return nil
}

// checkPredecessorAgainstStore requires the supplied predecessor to BE the standing authority.
//
// Byte equality over the marshalled bodies rather than a field-by-field comparison: the
// record carries nested pending proposals, and a comparison that enumerated fields would
// silently stop covering whatever is added next — which is exactly how a pending proposal
// would become erasable again.
func (jsa *mqttJSA) checkPredecessorAgainstStore(a *mqttTransitionAttempt,
	prev *mqttOutboundAuthority, prevSeq uint64) error {

	smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, mqttOutboundAuthoritySubject(getHash(a.ID)))
	if lerr != nil {
		return fmt.Errorf("transition %q names a predecessor authority that could not be read: %w",
			a.Txn, lerr)
	}
	if smsg.Sequence != prevSeq {
		// A PRE-SUBMIT contention window: the authority moved between the op's read and this
		// check, so the transition's expected-last-sequence no longer holds and no batch is
		// submitted. A frequent authority writer settles it the same way it settles the
		// JetStream 10071 the batch CAS would have returned a moment later: a bounded re-attempt
		// on ANY error after an authoritative re-read (the admission-ladder contract), which is
		// why this needs no distinguishing type.
		return fmt.Errorf("transition %q is conditioned on the authority at sequence %d but it "+
			"stands at %d", a.Txn, prevSeq, smsg.Sequence)
	}
	stored, derr := mqttDecodeOutboundAuthority(smsg.Data, a.ID, a.Epoch)
	if derr != nil {
		return fmt.Errorf("transition %q runs over a standing authority this broker cannot "+
			"interpret: %w", a.Txn, derr)
	}
	want, werr := json.Marshal(stored)
	if werr != nil {
		return werr
	}
	got, gerr := json.Marshal(prev)
	if gerr != nil {
		return gerr
	}
	if !bytes.Equal(want, got) {
		return fmt.Errorf("transition %q describes a predecessor the store does not hold: every "+
			"rule about what this transition may displace is a conclusion about the STANDING "+
			"authority, not about the body a caller supplied", a.Txn)
	}
	return nil
}

// checkResourceOverwrites validates every resource member that overwrites, against the
// resource as it stands.
//
// THE ZERO COUNT MUST BE THE STORED ONE. An authorization is the claim that at the instant of
// this CAS nothing depended on the resource, and a count read out of the caller's own
// proposed body is not that claim at all — a caller could supply DELETE_AUTHORIZED with
// Deps=0 over a resource standing at Deps=1 and authorize deleting the PUBREL consumer an
// outstanding exchange needs to complete. The builder checks this; the builder is optional.
func (jsa *mqttJSA) checkResourceOverwrites(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	xchPred map[string]mqttExchangePhase) error {
	// The exchanges THIS batch installs, so an added edge can be checked against an
	// admission rather than taken on trust; and the exchanges THIS batch moves into an ending
	// (endedHere, keyed "gen.nonce"), computed ONCE from the validated standing phase of each
	// overwritten exchange and the phase the batch proposes for it, so every rule that asks
	// "does this batch end that obligation" asks the same set.
	createdHere := map[string]struct{}{}
	createdX := map[string]*mqttOutboundExchange{}
	endedHere := map[string]*mqttOutboundExchange{}
	for _, e := range extra {
		cls, ok := mqttF105ObligationClass(e.subj)
		if !ok || cls != mqttExchangeClassTk {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			continue
		}
		x, derr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, toks[2], toks[3])
		if derr != nil {
			continue // refused by mqttCheckObligationMember
		}
		if e.expect == 0 {
			createdHere[toks[3]] = struct{}{}
			createdX[toks[3]] = x
			continue
		}
		// A HOLD-RELEASING move: an ending, or the generation's discard (F-105 7d), which releases
		// holds without being an identifier ending.
		if prev, known := xchPred[e.subj]; known && !mqttExchangeReleasesHolds(prev) && mqttExchangeReleasesHolds(x.Phase) {
			endedHere[toks[2]+"."+toks[3]] = x
		}
	}
	// A DISPOSE-RESOURCE BATCH OVERWRITES AT MOST ONE RESOURCE (F-105 7c-lc, closing the H-1..H-3
	// class; consult 112). It is the one kind licensed to seal, authorize, complete or dispose, so it
	// is also the one kind in which an UNRELATED resource change could otherwise ride a legitimate
	// step on another record. Every dispose composer carries at most one resource member (a duty's
	// resolution carries none); more than one is refused. The batch's other members have their own
	// validators: this states the resource cardinality and nothing about them.
	disposed := 0
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttResourceClassTk && e.expect != 0 {
			disposed++
		}
	}
	if a.Kind == mqttTransitionDisposeResource && disposed > 1 {
		return fmt.Errorf("transition %q of kind %q mutates %d resources: a dispose-resource transition "+
			"overwrites at most one", a.Txn, a.Kind, disposed)
	}
	// What each resource member's hold edges did, for the two batch-level properties below.
	addedHolds := map[string][]mqttExchangeHold{}           // nonce -> holds this batch added
	releasedHolds := map[string]map[mqttExchangeHold]bool{} // gen.nonce -> holds this batch released
	// AN ENDING IS ABSORBING, so reading the exchange through the latest record is sound in
	// the only direction that matters: a read that says ended still says ended now, and one
	// that says otherwise refuses a release that can simply be retried. The unsafe
	// direction — concluding "ended" from a stale record — cannot occur.
	//
	// Both endings release: a REFUSED PUBREC ends the obligation as finally as a completed
	// one, and holding the resource for it would keep a consumer alive for a message the
	// client already declined.
	terminal := func(gen, nonce string) error {
		subj := mqttExchangeSubject(getHash(a.ID), gen, nonce)
		smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
		if lerr != nil {
			return fmt.Errorf("its exchange record could not be read: %w", lerr)
		}
		x, xerr := mqttDecodeOutboundExchange(smsg.Data, a.ID, a.Epoch, gen, nonce)
		if xerr != nil {
			return fmt.Errorf("its exchange record is unsupported: %w", xerr)
		}
		if !mqttExchangePhaseEnded(x.Phase) {
			return fmt.Errorf("it stands at phase %d, which is not an ending", x.Phase)
		}
		return nil
	}
	// SEALING A PAYLOAD RESOURCE IS ANSWERABLE, so it is answered rather than asserted.
	//
	// A seal says nothing can materialize this resource any more, and a deletion authorization
	// rests on it. For a consumer that fact settles outside this stream and the caller is the
	// only one who can state it. For replay material it does NOT: the manifest is in this
	// stream, at the address the resource names, and a seal taken while it is still
	// provisioning is a claim the store can refute — this is contract 4's class at the
	// resource boundary, the same shape as a manifest sealed READY over unverified bytes.
	sealable := func(r *mqttOutboundResource) error {
		// AN AFFINE CONSUMER'S SEAL RESTS ON THE READINESS ASSERTION, and on nothing the caller
		// says. Sealing it is what makes its cleanup authorizable, and its cleanup deletes the
		// consumer whose records carry the only evidence that a client answered PUBREC — so the
		// seal may not be written before the proofs are secured and the assignments' discard
		// disposition is established.
		//
		// WHAT THIS ESTABLISHES IS WHOM THE ASSERTION IS ABOUT: the record must decode, be
		// complete, and name exactly this client, this account recovery epoch, this generation,
		// this affine generation and this durable. It does NOT establish that the acts ran —
		// matching identifiers are not evidence of work — so the producers are enumerated and
		// audited under "The trusted producer set" in docs/ARCHITECTURE.md, and a premature
		// record written by a producer that skipped the acts would satisfy this rule. That limit
		// is stated rather than implied.
		if r.Kind == mqttResourceAffineConsumer {
			ready, lerr := jsa.loadAffineReadiness(r.ID, r.Epoch, r.Gen)
			if lerr != nil {
				return fmt.Errorf("its readiness assertion could not be read: %w", lerr)
			}
			if ready == nil {
				return fmt.Errorf("no readiness assertion stands for generation %q, so nothing "+
					"says its PUBREC proofs are secured or its assignments discarded", r.Gen)
			}
			if ready.AffGen != r.AffGen || ready.Durable != r.Name ||
				ready.PubRelDur != r.PubRelDur {
				return fmt.Errorf("the readiness assertion for generation %q covers %q/%q with "+
					"proofs from %q, and this resource is %q/%q with proofs in %q, so it asserts "+
					"nothing about this consumer", r.Gen, ready.AffGen, ready.Durable,
					ready.PubRelDur, r.AffGen, r.Name, r.PubRelDur)
			}
			return nil
		}
		if r.Kind != mqttResourceRetainedPayload {
			return nil
		}
		// THE STORED MATERIAL, deliberately, and NOT what this batch is about to write.
		//
		// A seal must rest on a cessation of materialization that is ALREADY ESTABLISHED. Reading
		// the batch's own manifest member instead would let one act close the material and
		// consume that closure as the seal's evidence — a second fact riding on a seal, which
		// "one act, one fact" already refuses for dependency edges. Consult 8 named that
		// permission and refused it: close the manifest, THEN seal the resource.
		subj := mqttManifestSubject(getHash(a.ID), r.Gen, r.Name)
		smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
		absent := (lerr != nil && !isErrorOtherThan(lerr, JSNoMessageFoundErr)) ||
			(lerr == nil && smsg == nil)
		if lerr != nil && !absent {
			return fmt.Errorf("its material could not be read: %w", lerr)
		}
		if absent {
			// NOTHING STANDS, and the seal is what keeps it that way: material is RESERVED only
			// by a materializing kind, and against an owner that is not yet sealed.
			return nil
		}
		m, derr := mqttDecodeRetainedManifest(smsg.Data, r.ID, r.Epoch, r.Gen, r.Name)
		if derr != nil {
			return fmt.Errorf("its material is described by a record this broker cannot "+
				"interpret: %w", derr)
		}
		// ANOTHER MATERIALIZATION'S RECORD IS NOT EVIDENCE ABOUT OURS, in either direction.
		//
		// Reading it as ours is the class this lane keeps finding — acting on a record that
		// belongs to a different materialization — so it is refused rather than interpreted.
		// The protocol order does not produce this: a resource is sealed before its deletion is
		// authorized, and the name is free for a replacement only long after. The cost of being
		// wrong about that is a payload record that outlives its material; the cost of guessing
		// is a seal taken over material the boundary never looked at.
		if m.Inc != r.Inc {
			return fmt.Errorf("the material standing at %q belongs to materialization %q, so "+
				"this resource's own material cannot be identified there", r.Name, m.Inc)
		}
		// CLOSED, AND ONLY CLOSED (F-105 7c). The recovery order is close the manifest, then seal
		// the resource; a seal over a READY copy would let an admission land against material
		// whose owner already declared it would take nothing further.
		if m.State != mqttManifestClosed {
			return fmt.Errorf("its material is %s, not closed: close the manifest before sealing its "+
				"owner", map[mqttManifestState]string{mqttManifestProvisioning: "still provisioning",
				mqttManifestReady: "READY"}[m.State])
		}
		return nil
	}
	// ownedLive answers whether a resource whose NAME is governed by an ownership record (the
	// source and PUBREL consumers, §5.1) may have a new dependency admitted against it: its
	// name must stand LIVE and be owned by exactly this incarnation. A resource kind whose
	// name the ownership record does not govern (an affine consumer, a payload) passes — its
	// lifecycle is arbitrated elsewhere. The read is client-agnostic-free here: the resource
	// record already names this client (a.ID), so the strict loader is right.
	ownedLive := func(r *mqttOutboundResource) error {
		stream, governed := mqttOwnedStreamForResourceKind(r.Kind)
		if !governed {
			return nil
		}
		o, _, lerr := jsa.loadNameOwnership(a.ID, a.Epoch, stream, r.Name)
		if lerr != nil {
			return fmt.Errorf("its name-ownership could not be read: %w", lerr)
		}
		if o == nil {
			return fmt.Errorf("no name-ownership record stands for %q on %q, so nothing licenses "+
				"depending on it: a dependency is admitted only against a LIVE owned name", r.Name, stream)
		}
		if o.State != mqttNameOwnLive {
			return fmt.Errorf("the name %q on %q is in ownership state %d, not LIVE: a spent name "+
				"admits no new dependency", r.Name, stream, o.State)
		}
		if o.OwnerGen != r.Gen || o.OwnerInc != r.Inc {
			return fmt.Errorf("the name %q on %q is owned by incarnation %q/%q, but this resource is "+
				"%q/%q: a dependency must name the exact live owner", r.Name, stream,
				o.OwnerGen, o.OwnerInc, r.Gen, r.Inc)
		}
		return nil
	}

	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttResourceClassTk {
			continue
		}
		// Create-only provisions a new incarnation and has nothing to compare against.
		if e.expect == 0 {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			return fmt.Errorf("transition %q carries a malformed resource subject %q", a.Txn, e.subj)
		}
		gen, inc := toks[2], toks[3]

		smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, e.subj)
		if lerr != nil {
			return fmt.Errorf("transition %q mutates resource %q, which could not be read: %w",
				a.Txn, inc, lerr)
		}
		if smsg.Sequence != e.expect {
			return fmt.Errorf("transition %q mutates resource %q at sequence %d but it stands at %d",
				a.Txn, inc, e.expect, smsg.Sequence)
		}
		cur, derr := mqttDecodeOutboundResource(smsg.Data, a.ID, a.Epoch, gen, inc)
		if derr != nil {
			return fmt.Errorf("transition %q mutates resource %q, whose standing record is "+
				"unsupported: %w", a.Txn, inc, derr)
		}
		next, nerr := mqttDecodeOutboundResource(e.body, a.ID, a.Epoch, gen, inc)
		if nerr != nil {
			return fmt.Errorf("transition %q carries an unsupported resource on %q: %w",
				a.Txn, e.subj, nerr)
		}
		retained := func(r *mqttOutboundResource, nonce string) error {
			return jsa.retainedMaterialNoLongerNeeded(a, r, nonce)
		}
		if err := mqttCheckResourceStep(a, cur, next, createdHere, endedHere, terminal, sealable, ownedLive, retained); err != nil {
			return err
		}
		if cur.State == mqttResourceDeleteAuthorized && next.State == mqttResourceCleanupComplete {
			if err := mqttRequireCleanupDutyEnding(a, extra, next); err != nil {
				return err
			}
		}
		addedE, removedE, _ := mqttResourceEdgeChanges(cur, next)
		if mqttResourceLogicalUseKind(cur.Kind) {
			for _, ed := range addedE {
				if mqttHoldRole(ed.Role) {
					addedHolds[ed.Nonce] = append(addedHolds[ed.Nonce], mqttExchangeHold{Inc: cur.Inc, Role: ed.Role})
				}
			}
			for _, ed := range removedE {
				if mqttHoldRole(ed.Role) {
					k := cur.Gen + "." + ed.Nonce
					if releasedHolds[k] == nil {
						releasedHolds[k] = map[mqttExchangeHold]bool{}
					}
					releasedHolds[k][mqttExchangeHold{Inc: cur.Inc, Role: ed.Role}] = true
				}
			}
		}
		// OUTSIDE A DISPOSE-RESOURCE TRANSITION, A RESOURCE OVERWRITE IS AN EDGE CHANGE AND NOTHING
		// ELSE (F-105 7c-lc; P-4, holistic H-2 and H-3, closed as a property rather than per kind).
		// Sealing, authorizing, completing and disposing are the dispose-resource kind's; every
		// other kind that carries a resource member carries it to take the edges its own admissions
		// create or to release the holds its own endings end, and the rules above judge those
		// edges. Without this a SEPARATE resource member — another resource sealed with no edge
		// change of its own — could ride an admission, a pair replacement, an advance or a
		// withdrawal, and nothing else would refuse it. The ending kinds also take no edge at all.
		if a.Kind != mqttTransitionDisposeResource {
			if len(addedE) == 0 && len(removedE) == 0 {
				return fmt.Errorf("transition %q of kind %q mutates resource %q without changing its "+
					"dependents: outside a dispose-resource transition a resource member is an edge change",
					a.Txn, a.Kind, cur.Inc)
			}
			if mqttKindEndsEarlierGeneration(a.Kind) && len(addedE) != 0 {
				return fmt.Errorf("transition %q of kind %q mutates resource %q other than by releasing "+
					"the holds of an obligation it ends", a.Txn, a.Kind, cur.Inc)
			}
			want := *cur
			want.Deps = next.Deps
			if !reflect.DeepEqual(&want, next) {
				return fmt.Errorf("transition %q of kind %q changes resource %q beyond its dependents: "+
					"outside a dispose-resource transition a resource member is an edge change", a.Txn, a.Kind, cur.Inc)
			}
		}
	}
	// THE HOLDS INDEX OF EVERY OBLIGATION THIS BATCH ADMITS IS EXACTLY THE HOLD EDGES IT ADDS, in
	// both directions: an index naming an edge that was not taken would make its ending release
	// nothing real, and an edge the index omits could never be released by that ending.
	for nonce, x := range createdX {
		if !mqttHoldsEqual(x.Holds, addedHolds[nonce]) {
			return fmt.Errorf("transition %q admits obligation %q with holds %v, but the batch adds the "+
				"hold edges %v: the index is derived from the edges, never asserted beside them",
				a.Txn, nonce, x.Holds, addedHolds[nonce])
		}
	}
	// AND EVERY OBLIGATION THIS BATCH ENDS RELEASES EVERY HOLD IT HAS, IN THIS BATCH. The ending and
	// the release are one fact: committed apart, a crash between them leaves an ended obligation
	// pinning a consumer no later act is entitled to free (no stored-ended release is admitted).
	for key, x := range endedHere {
		for _, h := range x.Holds {
			if !releasedHolds[key][h] {
				return fmt.Errorf("transition %q ends obligation %q without releasing its hold on %q "+
					"in role %q: an ending releases its holds in the same batch", a.Txn, x.Nonce, h.Inc, h.Role)
			}
		}
	}
	return nil
}

// mqttRequireCleanupDutyEnding requires a cleanup completion to ride with the ending of the
// cleanup duty that names the resource.
//
// A CLEANUP IS COMPLETE ONLY TOGETHER WITH ITS DUTY. Every resource is admitted with its cleanup
// duty predeclared, and completeCleanupAndResolve writes the resource's classified outcome and
// that duty's Terminal in one batch. A completion landing alone would leave a finished cleanup
// beside a duty that still says it is owed, and the next pass would re-execute it; so the batch
// must carry a TERMINAL cleanup-duty entry naming this incarnation in this resource's own
// generation. Both bindings are checked separately: a terminal duty for another resource, or
// for the same incarnation string in another generation, answers for a different resource.
func mqttRequireCleanupDutyEnding(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	r *mqttOutboundResource) error {

	idHash := getHash(a.ID)
	for _, e := range extra {
		if !mqttIsDutySubject(e.subj) {
			continue
		}
		gen, ord, perr := mqttParseDutySubject(e.subj, idHash)
		if perr != nil {
			continue // the duty member's own check refuses it
		}
		if gen != r.Gen {
			continue
		}
		d, derr := mqttDecodeDutyEntry(e.body, a.ID, a.Epoch, gen, ord)
		if derr != nil {
			continue // the duty member's own check refuses it
		}
		if d.Kind != mqttDutyCleanup || d.State != mqttDutyTerminal {
			continue
		}
		if d.Ref != r.Inc {
			continue
		}
		return nil
	}
	return fmt.Errorf("transition %q completes the cleanup of resource %q in generation %q with no "+
		"terminal cleanup duty naming it in the same batch: a cleanup completes only together with "+
		"its duty", a.Txn, r.Inc, r.Gen)
}

// mqttResourceEdgeChanges reports every edge a resource mutation adds and removes. An edge is
// present once or not at all; any other count is refused.
// Which combinations are legitimate is mqttCheckResourceStep's question.
func mqttResourceEdgeChanges(cur, next *mqttOutboundResource) (added, removed []mqttResourceEdge, err error) {
	have := make(map[mqttResourceEdge]int, len(cur.Deps))
	for _, e := range cur.Deps {
		have[e]++
	}
	for _, e := range next.Deps {
		have[e]--
	}
	for e, d := range have {
		switch {
		case d == 1:
			removed = append(removed, e)
		case d == -1:
			added = append(added, e)
		case d != 0:
			return nil, nil, fmt.Errorf("dependency %s appears %d times", e, -d)
		}
	}
	sort.Slice(removed, func(i, j int) bool { return removed[i].String() < removed[j].String() })
	return added, removed, nil
}

// mqttCheckResourceStep is the resource's own state machine, applied to the STORED record and
// the proposed one.
//
// Identity never moves, the retirement runs forward only, and every edge change is DERIVED
// rather than asserted: an edge is added only by the admission that creates its exchange in
// this same batch, and removed only on that exchange's terminal evidence. A count could be
// drained one step at a time by transitions that ended nothing, which is how a PUBREL
// consumer carrying an outstanding exchange's only PUBREC evidence became deletable.
//
// createdHere names the exchanges this batch installs create-only, and terminal reports
// whether an exchange has durably reached its end.
func mqttCheckResourceStep(a *mqttTransitionAttempt, cur, next *mqttOutboundResource,
	createdHere map[string]struct{}, endedHere map[string]*mqttOutboundExchange,
	terminal func(gen, nonce string) error,
	sealable func(r *mqttOutboundResource) error, ownedLive func(r *mqttOutboundResource) error,
	retained func(r *mqttOutboundResource, nonce string) error) error {

	if next.Kind != cur.Kind || next.Name != cur.Name {
		return fmt.Errorf("transition %q re-identifies resource %q: a retirement is about the "+
			"incarnation it was computed for", a.Txn, cur.Inc)
	}
	if next.ProvTenure != cur.ProvTenure {
		return fmt.Errorf("transition %q rewrites resource %q's provisioning tenure: it is the fact a "+
			"successor tenure's settlement is licensed by, fixed when the consumer was claimed", a.Txn, cur.Inc)
	}
	addedAll, removedAll, derr := mqttResourceEdgeChanges(cur, next)
	if derr != nil {
		return fmt.Errorf("transition %q mutates resource %q's dependents illegitimately: %w",
			a.Txn, cur.Inc, derr)
	}
	// A HOLD RELEASE IS ITS EXCHANGE'S ENDING'S, AND ONLY THAT (F-105 7c-lc). The source-replay and
	// canonical-PUBREL edges of a name-owned consumer are released IN THE BATCH that moves their
	// exchange into an ending — never later against a stored ending, never earlier. So these
	// removals are judged against endedHere, the endings THIS batch makes (S-xor-Delta: the batch
	// is atomic, and the stored exchange is the state before it), and several may leave one record
	// in one act: each is its own obligation's ending, proven separately. Every other role keeps
	// the single-edge rule and its own evidence below.
	var holdRemoved, otherRemoved []mqttResourceEdge
	for _, e := range removedAll {
		if mqttHoldRole(e.Role) && mqttResourceLogicalUseKind(cur.Kind) {
			holdRemoved = append(holdRemoved, e)
		} else {
			otherRemoved = append(otherRemoved, e)
		}
	}
	if len(addedAll) > 1 {
		return fmt.Errorf("transition %q mutates resource %q's dependents illegitimately: more than "+
			"one dependency admitted at once", a.Txn, cur.Inc)
	}
	if len(otherRemoved) > 1 {
		return fmt.Errorf("transition %q mutates resource %q's dependents illegitimately: more than "+
			"one dependency released at once", a.Txn, cur.Inc)
	}
	if len(otherRemoved) == 1 && (len(addedAll) == 1 || len(holdRemoved) > 0) {
		return fmt.Errorf("transition %q mutates resource %q's dependents illegitimately: one act "+
			"both released %s and changed another dependency", a.Txn, cur.Inc, otherRemoved[0])
	}
	for _, h := range holdRemoved {
		x := endedHere[cur.Gen+"."+h.Nonce]
		if x == nil {
			return fmt.Errorf("transition %q releases %s from resource %q, but this batch does not "+
				"move that obligation into an ending: a source or canonical-PUBREL edge is released "+
				"by its exchange's ending, in the same batch, and by nothing else", a.Txn, h, cur.Inc)
		}
		held := false
		for _, xh := range x.Holds {
			if xh.Inc == cur.Inc && xh.Role == h.Role {
				held = true
				break
			}
		}
		if !held {
			return fmt.Errorf("transition %q releases %s from resource %q, which that obligation's "+
				"holds index does not name", a.Txn, h, cur.Inc)
		}
	}
	var added, removed *mqttResourceEdge
	if len(addedAll) == 1 {
		added = &addedAll[0]
	}
	if len(otherRemoved) == 1 {
		removed = &otherRemoved[0]
	}
	// THE DISPOSAL FACT IS WRITTEN ONCE AND CARRIED (F-105 7c-lc). Introduced only on a LIVE
	// resource, by a batch that changes nothing else about it, in this transition's own name (the
	// entitlement licence is mqttCheckDisposalLicence's); never cleared, never rewritten.
	switch {
	case cur.Disposal != nil:
		if next.Disposal == nil || *next.Disposal != *cur.Disposal {
			return fmt.Errorf("transition %q rewrites or clears the disposal decision of resource %q: "+
				"a route's decision that a consumer is ended is permanent", a.Txn, cur.Inc)
		}
	case next.Disposal != nil:
		want := *cur
		want.Disposal = next.Disposal
		if cur.State != mqttResourceLive || !reflect.DeepEqual(&want, next) {
			return fmt.Errorf("transition %q introduces a disposal decision on resource %q together "+
				"with another change, or on a resource that is not LIVE: one act, one fact", a.Txn, cur.Inc)
		}
		if next.Disposal.Txn != a.Txn {
			return fmt.Errorf("transition %q introduces a disposal decision of resource %q in the name of %q",
				a.Txn, cur.Inc, next.Disposal.Txn)
		}
	}
	if added != nil && cur.Disposal != nil {
		// A DISPOSED CONSUMER TAKES NO NEW DEPENDENT. The disposal and the admission both CAS this
		// record, so whichever lands second meets the other: admission-first holds the retirement
		// until its ending; disposal-first refuses the admission here.
		return fmt.Errorf("transition %q makes resource %q depend on obligation %q, but the resource "+
			"was disposed (%s): a consumer a route has ended admits no new obligation",
			a.Txn, cur.Inc, added.Nonce, cur.Disposal.Reason)
	}
	if added != nil {
		if _, ok := createdHere[added.Nonce]; !ok {
			return fmt.Errorf("transition %q makes resource %q depend on obligation %q, which "+
				"this transition does not admit: a dependency is taken by the admission that "+
				"creates the obligation, never asserted over one that already exists",
				a.Txn, cur.Inc, added.Nonce)
		}
		// AND, FOR A NAME-OWNED CONSUMER, THE NAME MUST BE LIVE AND OWNED BY THIS INCARNATION
		// (F-105 §5.1 point 3, Codex consult 11). Admission takes a dependency edge; that edge
		// is what stops a teardown deleting the consumer that carries this obligation's PUBREC
		// evidence. But the edge alone says nothing about whether the NAME is still this
		// resource's to depend on: a retirement may have already spent it. Retirement CASes
		// this same resource record, so an ownership read taken here is serialized against it
		// by the resource CAS — the winner is decided on this record, and a retirement that
		// beat us bumps the sequence so our own resource CAS fails. Depending on a RETIRED or
		// foreign-owned name is refused, which is the admission-versus-retirement contention.
		if err := ownedLive(cur); err != nil {
			return fmt.Errorf("transition %q admits a dependency on resource %q, but %w",
				a.Txn, cur.Inc, err)
		}
	}
	if removed != nil {
		// THE EXCHANGE'S ENDING IS A FLOOR, NOT THE PROOF, and this is where that stops being a
		// comment and becomes a rule.
		//
		// The floor is necessary for every role: a need of a LIVE obligation is not discharged.
		// It is SUFFICIENT only for the roles the protocol itself concludes (the two hold roles
		// of a name-owned consumer are judged above, against this batch's own endings).
		// A RETAINED-MATERIAL edge has two further floors (F-105 7c): an exchange carrying the
		// retained abandonment fact for this payload never replays its PUBLISH again, whatever
		// phase a late PUBREC moved it to, and a generation closed with Discard has no serving
		// successor to replay anything. Neither licenses any other role.
		retainedFloor := removed.Role == mqttRoleRetainedMaterial && retained != nil &&
			retained(cur, removed.Nonce) == nil
		if !retainedFloor {
			if terr := terminal(cur.Gen, removed.Nonce); terr != nil {
				return fmt.Errorf("transition %q releases %s from resource %q, whose obligation has "+
					"not ended: %w", a.Txn, removed, cur.Inc, terr)
			}
		}
		if mqttRoleDischarge(removed.Role) != mqttDischargeByExchangeEnding {
			// FAILS CLOSED. Round I-2's finding: distinguishing edges by role does nothing
			// unless the boundary enforces their different evidence. Every supported role is
			// discharged by its exchange's ending and decoding refuses every other role, so
			// this arm is unreachable today; it stays so that a role whose evidence is NOT the
			// ending can never be released on the ending by default — after which the resource
			// would reach zero edges and become deletable while its need still stood.
			//
			// Refusing costs reclamation. Permitting costs the obligation, which is the thing
			// F-105 exists to protect.
			return fmt.Errorf("transition %q releases %s from resource %q on the strength of its "+
				"obligation ending, which is PROTOCOL completion and not this role's discharge: "+
				"that role ends on evidence this broker does not yet record durably, so the edge "+
				"is not releasable and the resource stays needed", a.Txn, removed, cur.Inc)
		}
	}
	edgesChanged := len(addedAll) > 0 || len(removedAll) > 0

	// PROVISIONING RUNS FORWARD ONLY, and on its own axis.
	//
	// Sealing says nothing can MATERIALIZE this resource any more; the retirement states say
	// where it stands in being reclaimed. Unsealing would let a delayed create land after a
	// deletion was authorized on the strength of the seal, which is the whole hazard.
	if cur.Prov == mqttProvisioningSealed && next.Prov != mqttProvisioningSealed {
		return fmt.Errorf("transition %q unseals the provisioning of resource %q: a seal is the "+
			"positive fact that nothing further can materialize it, and it is not revocable",
			a.Txn, cur.Inc)
	}
	sealing := cur.Prov != next.Prov

	switch {
	case cur.State == mqttResourceLive && next.State == mqttResourceLive:
		// A dependency edge, OR a seal. Never both, and nothing else may ride along.
		if next.AuthTxn != _EMPTY_ || next.Outcome != _EMPTY_ {
			return fmt.Errorf("transition %q records an authorization or an outcome on live "+
				"resource %q", a.Txn, cur.Inc)
		}
		// ONE ACT, ONE FACT. A batch that sealed provisioning AND moved an edge would carry
		// two different pieces of evidence under one transition, and a reader could not say
		// which one the boundary had actually checked.
		if sealing && edgesChanged {
			return fmt.Errorf("transition %q both seals the provisioning of resource %q and "+
				"changes its dependents: those are two facts with two proofs, and one act "+
				"establishes one of them", a.Txn, cur.Inc)
		}
		// AND THE SEAL IS CHECKED WHERE IT CAN BE CHECKED.
		if sealing {
			if serr := sealable(cur); serr != nil {
				return fmt.Errorf("transition %q seals the provisioning of resource %q, but %w: "+
					"a seal is the positive fact that nothing further can materialize it, and a "+
					"deletion authorized on the strength of it would free a name that is not "+
					"free", a.Txn, cur.Inc, serr)
			}
		}

	case cur.State == mqttResourceLive && next.State == mqttResourceDeleteAuthorized:
		// PROVISIONING MUST BE SEALED FIRST — D5 names it beside the zero dependency count as
		// a condition of this authorization, and it is a different fact from the count.
		//
		// A resource whose physical creation is still in flight has a name that is not free:
		// the delete finds nothing, the delayed create lands afterwards, and the account keeps
		// a durable that no record names and no sweep will attribute. Both CUR and NEXT are
		// required sealed, so this act cannot supply the seal it is relying on.
		//
		// A LOGICAL-USE kind (source, PUBREL) is EXEMPT, because its create is unfenceable and
		// it can never produce this seal (consult 4). Its name-freedom is arbitrated instead by
		// the name-ownership record, which THIS same batch retires (opAuthorizeDeletion carries
		// the LIVE -> RETIRED member, and checkNameOwnershipMembers enforces it): a delayed
		// create landing after the name is RETIRED is a harmless orphan no obligation can adopt.
		// Requiring a seal here would strand the name for ever. See mqttResourceLogicalUseKind.
		// A LOGICAL-USE KIND IS AUTHORIZED ONLY ON A STANDING DISPOSAL DECISION (F-105 7c-lc): its
		// zero edges are equally what an idle serving subscription looks like.
		if mqttResourceLogicalUseKind(cur.Kind) && cur.Disposal == nil {
			return fmt.Errorf("transition %q authorizes deleting %s consumer %q, which no route has "+
				"disposed of: it is serving, and zero dependents is not the decision that it is ended",
				a.Txn, cur.Kind, cur.Inc)
		}
		if !mqttResourceLogicalUseKind(cur.Kind) &&
			(cur.Prov != mqttProvisioningSealed || next.Prov != mqttProvisioningSealed) {
			return fmt.Errorf("transition %q authorizes deleting resource %q whose provisioning "+
				"is not sealed: until nothing further can materialize it, its name is not free "+
				"and a deletion that found nothing would prove nothing", a.Txn, cur.Inc)
		}
		// THE LIST MUST BE EMPTY WHEN THIS CAS WINS, which is the authorization's entire
		// meaning: nothing depends on the resource and nothing can be added afterwards.
		// Releasing the LAST edge and authorizing in one act is legitimate and is how
		// completion actually runs — and it is now safe, because that release had to prove
		// its obligation terminal above.
		if len(next.Deps) != 0 {
			return fmt.Errorf("transition %q authorizes deleting resource %q while it is still "+
				"needed by %d obligation(s)", a.Txn, cur.Inc, len(next.Deps))
		}
		if next.AuthTxn != a.Txn {
			return fmt.Errorf("transition %q authorizes deleting resource %q in the name of %q",
				a.Txn, cur.Inc, next.AuthTxn)
		}
		if next.Outcome != _EMPTY_ {
			return fmt.Errorf("transition %q authorizes deleting resource %q and records its "+
				"outcome in the same act", a.Txn, cur.Inc)
		}

	case cur.State == mqttResourceDeleteAuthorized && next.State == mqttResourceCleanupComplete:
		if next.AuthTxn != cur.AuthTxn {
			return fmt.Errorf("transition %q completes cleanup of resource %q under authorization "+
				"%q, but it was authorized by %q", a.Txn, cur.Inc, next.AuthTxn, cur.AuthTxn)
		}
		if !mqttValidCleanupOutcome(next.Outcome) {
			return fmt.Errorf("transition %q completes cleanup of resource %q with no classified "+
				"outcome", a.Txn, cur.Inc)
		}

	default:
		// Everything else, named rather than implied: a resurrection to LIVE, a dependency
		// added after authorization, a re-authorization, a second cleanup.
		return fmt.Errorf("transition %q moves resource %q from state %d to %d, which its "+
			"retirement does not permit", a.Txn, cur.Inc, cur.State, next.State)
	}
	return nil
}

// domainToken is the session subject's domain prefix, or empty when there is no domain.
func (jsa *mqttJSA) domainToken() string {
	if jsa.domain == _EMPTY_ {
		return _EMPTY_
	}
	return jsa.domain + "."
}

// checkMetadataVerdict requires the proposal a transition acts on to have the permanent
// verdict that licenses the act.
//
// Reading $MQTT_sess from a transition that writes $MQTT_inflight is sound because the slot
// is CREATE-ONLY and its verdict is permanent: once read it cannot change, so there is no
// window between this read and the authority CAS in which the answer could become wrong. The
// converse — inferring a verdict from the authority record — is what this exists to stop.
//
// UNRESOLVED and FOREIGN both refuse. An unresolved slot may still commit, and a receipt that
// does not answer this proposal is a contradiction; neither licenses an ownership move.
func (jsa *mqttJSA) checkMetadataVerdict(t *mqttAuthorityTransition) error {
	a := t.Attempt
	var (
		p    mqttOwnershipProposal
		want mqttMetadataVerdict
		act  string
		err  error
	)
	switch a.Kind {
	case mqttTransitionFinalizeCreation:
		p, err = mqttProposalFromCreation(t.Prev.Creation)
		want, act = mqttMetadataCommitted, "finalizes"
	case mqttTransitionFinalize:
		p, err = mqttProposalFromHandoff(t.Prev.Intent)
		want, act = mqttMetadataCommitted, "finalizes"
	case mqttTransitionCancelIntent:
		p, err = mqttProposalFromHandoff(t.Prev.Intent)
		want, act = mqttMetadataAborted, "cancels"
	case mqttTransitionCloseAdmit:
		// Only a RESERVING predecessor carries a proposal to close over. A SERVING one has
		// none, and a PREPARING one is refused by the kind rule before reaching here.
		if t.Prev == nil || t.Prev.State != mqttAuthorityReserving {
			return nil
		}
		p, err = mqttProposalFromCreation(t.Prev.Creation)
		want, act = mqttMetadataAborted, "abandons"
	default:
		return nil
	}
	if err != nil {
		return fmt.Errorf("transition %q acts on a proposal this broker cannot interpret: %w", a.Txn, err)
	}
	got := jsa.resolveMetadataAttempt(jsa.domainToken(), a.ID, a.Epoch, p)
	if got != want {
		return fmt.Errorf("transition %q %s proposal %q, whose metadata half resolves %d and "+
			"not the %d that licenses it: an ownership move follows the permanent verdict, "+
			"never the authority record's shape", a.Txn, act, p.ID, got, want)
	}
	return nil
}

// mqttStandingRetirement is the retirement the acceptance selected targets in, or nil.
func mqttStandingRetirement(t *mqttAuthorityTransition) *mqttRetirementRequest {
	if t == nil {
		return nil
	}
	// A standalone teardown carries its retirement on the transition itself, read from the
	// durable receipt the operation validated; a handoff carries it on the predecessor's
	// pending intent. Neither reads whatever a predecessor happens to hold.
	if t.Attempt != nil && t.Attempt.Kind == mqttTransitionCloseTeardown {
		return t.Retire
	}
	if t.Prev == nil || t.Prev.Intent == nil {
		return nil
	}
	return t.Prev.Intent.Retire
}
