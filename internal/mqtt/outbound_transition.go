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
	"encoding/json"
	"fmt"
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

// resolveTransition classifies one attempted transition from positive evidence only.
//
// The attempt is required, not optional: the outcome subject is named by a transition id
// alone, so a receipt found there proves only that SOME batch under that id committed. The
// caller states what it tried to do, and a standing outcome describing anything else is a
// contradiction rather than an answer.
//
// The authority read serves exactly one distinction — landed versus superseded — and never
// decides whether the attempt happened. Its three admissible shapes are: a strictly later
// revision (superseded), the same revision carrying the same ownership (landed), and
// anything else, which is an inconsistency this broker refuses to interpret. An authority
// at an EARLIER revision than a committed outcome is the sharpest of those: it cannot have
// preceded a transition that claims to have produced a later one.
func (jsa *mqttJSA) resolveTransition(t *mqttAuthorityTransition) mqttTransitionVerdict {
	if t == nil || t.Attempt == nil {
		return mqttTransitionUnresolved
	}
	want, derr := mqttTransitionMutationDigest(t)
	if derr != nil {
		return mqttTransitionUnresolved
	}
	return jsa.classifyAgainstOutcome(t.Attempt, want)
}

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
// resolveTransition does, and why this function returns the error rather than a verdict.
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
	for _, e := range t.Extra {
		if err := mqttCheckTransitionMember(a, idHash, e, seen); err != nil {
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
	if err := jsa.checkExchangeOverwrites(a, t.Extra); err != nil {
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
	if err := jsa.checkCreatedResources(a, t.Extra); err != nil {
		return 0, err
	}
	if err := jsa.checkPairBindings(a, t.Extra); err != nil {
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
	if err := jsa.checkResourceOverwrites(a, t.Extra); err != nil {
		return 0, err
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

// mqttCheckTransitionKindRules enforces what each operation requires of the authority it
// displaces and the body it installs.
//
// The enum alone establishes nothing: it is the rules here that make "admitted before
// closure" a property of the mechanism rather than of its callers. Every rule is stated as
// what the operation needs, so a kind added without one fails closed at the default.
func mqttCheckTransitionKindRules(t *mqttAuthorityTransition) error {
	a := t.Attempt
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
		if len(t.Extra) != 0 {
			return fmt.Errorf("a reserve-creation transition carries obligations, which it may not admit")
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
			if !mqttResourceIsF105Class(e.subj) {
				return fmt.Errorf("a dispose-resource transition carries member %q, which is not a resource", e.subj)
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
		if len(t.Extra) != 1 {
			return fmt.Errorf("a provision-resource transition carries %d member(s), not the one resource it provisions",
				len(t.Extra))
		}
		if !mqttResourceIsF105Class(t.Extra[0].subj) {
			return fmt.Errorf("a provision-resource transition carries member %q, which is not a resource",
				t.Extra[0].subj)
		}
		if t.Extra[0].expect != 0 {
			return fmt.Errorf("a provision-resource transition rewrites the resource on %q at sequence %d: "+
				"provisioning creates an incarnation and never replaces one",
				t.Extra[0].subj, t.Extra[0].expect)
		}

	case mqttTransitionAdvanceExchange:
		// Permitted under ANY predecessor state, including CLOSED, because an obligation of
		// a closed generation must still be able to end. Like dispose-resource it preserves
		// ownership, state and both pending proposals exactly; unlike it, what it carries is
		// exchanges rather than resources, and it carries at least one.
		if t.Prev == nil {
			return fmt.Errorf("an advance-exchange transition requires a predecessor")
		}
		if t.Auth.State != t.Prev.State {
			return fmt.Errorf("an advance-exchange transition moves the authority from state %d to %d",
				t.Prev.State, t.Auth.State)
		}
		if t.Auth.Gen != t.Prev.Gen || t.Auth.Tenure != t.Prev.Tenure || t.Auth.OwnerTxn != t.Prev.OwnerTxn {
			return fmt.Errorf("an advance-exchange transition moves ownership, which it may not")
		}
		if !mqttSameHandoffIntent(t.Prev.Intent, t.Auth.Intent) {
			return fmt.Errorf("an advance-exchange transition rewrote the pending handoff intent")
		}
		if !mqttSameCreationIntent(t.Prev.Creation, t.Auth.Creation) {
			return fmt.Errorf("an advance-exchange transition rewrote the pending creation proposal")
		}
		if t.Closure != nil {
			return fmt.Errorf("an advance-exchange transition writes a generation closure")
		}
		if len(t.Extra) == 0 {
			return fmt.Errorf("an advance-exchange transition carries no exchange")
		}
		// IT CARRIES EXCHANGES AND NOTHING ELSE. Without this the kind that is exempt from
		// the serving requirement would be a route for admitting registers, actions or
		// dependency counts under a closed authority.
		for _, e := range t.Extra {
			if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttExchangeClassTk {
				return fmt.Errorf("an advance-exchange transition carries member %q, which is not an exchange",
					e.subj)
			}
			// AND IT NEVER CREATES ONE. A create-only member here would install an exchange
			// — an obligation nothing admitted — through the one kind that does not need a
			// serving authority.
			if e.expect == 0 {
				return fmt.Errorf("an advance-exchange transition carries member %q create-only, "+
					"which would install an exchange rather than advance one", e.subj)
			}
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
		// A RESERVING predecessor IS closable, and that is the recoverable abort of a first
		// creation: the reservation is never deleted — no arbitration record is — its
		// generation is simply closed, having never served and never able to, and a later
		// attempt reserves a fresh generation beside it, through reserve-creation again.

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
func mqttCheckTransitionMember(a *mqttTransitionAttempt, idHash string, e mqttTransitionMember, seen map[string]struct{}) error {
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
		return mqttCheckObligationMember(a, idHash, cls, e)
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
	if name.gen != a.Gen {
		return fmt.Errorf("transition %q carries member %q, which names generation %q rather than this transition's %q",
			a.Txn, e.subj, name.gen, a.Gen)
	}

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
		// A TRANSITION CREATES A REGISTER; IT NEVER REPLACES ONE.
		//
		// Replacing an allocation is governed by the slot state machine in
		// inflight_slot_state.go — which disposition the occupant is in, whether it is the
		// same delivery, whether custody permits the identifier to move — and those rules
		// live there, applied by the writers that own them. Validating a nonzero expectation
		// here would be a SECOND definition of a legal register step, and NOT validating it
		// is worse: an absent unsent marker is normal after transmission, so the marker's
		// create-only member does not stand in for the occupant check, and a hand-composed
		// member could overwrite a live past-PUBREC register with a fresh PUBLISH allocation
		// the machine would have refused.
		//
		// So replacement is simply not an operation this mechanism supports. It becomes one
		// when the ordinary-PUBLISH admission path is converted, by REUSING that machine's
		// decision and committing its write with the admission's other members in the same
		// batch — not by calling an existing writer first and admitting afterwards, which
		// would split the two facts across two transactions.
		if e.expect != 0 {
			return fmt.Errorf("transition %q replaces the register on %q at sequence %d: this "+
				"mechanism creates a register and never replaces one, because replacement is "+
				"the slot state machine's decision and not this boundary's", a.Txn, e.subj, e.expect)
		}
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
	// A MARKER IS ALWAYS CREATE-ONLY. Its whole meaning is presence: it is minted with the
	// register it names and ENDED by a delete at its own sequence, never rewritten. An
	// overwrite would let a transmitted allocation be described as unsent again, which is
	// the DUP flag lying in the direction the specification forbids ([MQTT-3.3.1-1]), and it
	// would do so at a sequence a concurrent transmission's delete is racing.
	if e.expect != 0 {
		return fmt.Errorf("transition %q rewrites the unsent marker on %q at sequence %d: a "+
			"marker is created with its register and deleted by a transmission, never rewritten",
			a.Txn, e.subj, e.expect)
	}
	return nil
}

// mqttCheckObligationMember validates an exchange or action member against the transition
// carrying it.
//
// Attribution is by DECODE, not by subject shape: the subject says which client, generation
// and exchange a body claims to be about, and the decoders refuse a body that disagrees with
// its own name. That is what stops a transition installing a well-named obligation whose
// contents belong to something else.
func mqttCheckObligationMember(a *mqttTransitionAttempt, idHash, cls string, e mqttTransitionMember) error {
	toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
	switch cls {
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
		if a.Kind != mqttTransitionAdvanceExchange && toks[2] != a.Gen {
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
		if a.Kind != mqttTransitionDisposeResource && toks[2] != a.Gen {
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
func (jsa *mqttJSA) checkExchangeOverwrites(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
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
			return fmt.Errorf("transition %q carries a malformed exchange subject %q", a.Txn, e.subj)
		}
		gen, nonce := toks[2], toks[3]

		smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, e.subj)
		if lerr != nil {
			return fmt.Errorf("transition %q advances exchange %q, which could not be read: %w",
				a.Txn, nonce, lerr)
		}
		if smsg.Sequence != e.expect {
			return fmt.Errorf("transition %q advances exchange %q at sequence %d but it stands at %d",
				a.Txn, nonce, e.expect, smsg.Sequence)
		}
		cur, derr := mqttDecodeOutboundExchange(smsg.Data, a.ID, a.Epoch, gen, nonce)
		if derr != nil {
			return fmt.Errorf("transition %q advances exchange %q, whose standing record is "+
				"unsupported: %w", a.Txn, nonce, derr)
		}
		next, nerr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, gen, nonce)
		if nerr != nil {
			return fmt.Errorf("transition %q carries an unsupported exchange on %q: %w",
				a.Txn, e.subj, nerr)
		}

		// Compare by rebuilding the standing record with the proposed phase. Anything else
		// that differs then shows up as a mismatch, including a member added later that
		// nobody thought to enumerate here.
		want := *cur
		want.Phase = next.Phase
		if want != *next {
			return fmt.Errorf("transition %q rewrites exchange %q beyond its phase: an advance "+
				"carries a completion, never a reallocation or a re-attribution", a.Txn, nonce)
		}
		if serr := mqttExchangePhaseSucceeds(cur.QoS, cur.Phase, next.Phase); serr != nil {
			return fmt.Errorf("transition %q advances exchange %q illegally: %w", a.Txn, nonce, serr)
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
// A REGISTERLESS ADMISSION IS STILL SUPPORTED: mqttAdmitExchangeTransition carries an exchange
// and nothing else, and an exchange with no register in its batch has nothing to disagree
// with. The rule binds what is PRESENT.
func (jsa *mqttJSA) checkPairBindings(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	type slot struct {
		reg    *mqttInflightRecord
		marker *mqttInflightMarker
	}
	slots := map[string]*slot{}
	// Keyed by LINEAGE as well, because that is what an exchange is matched by — and a map
	// has no order, so "the first register carrying this lineage" is not a rule at all.
	byLin := map[string]*mqttInflightRecord{}
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
		if _, ok := admitted[gen+"."+rec.Lin]; !ok {
			return fmt.Errorf("transition %q allocates identifier %d for obligation %q, which "+
				"it does not admit: an allocation with no obligation behind it answers to "+
				"nothing and excludes nobody", a.Txn, rec.PI, rec.Lin)
		}
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
		// A REGISTERLESS ADMISSION HAS NOTHING TO DISAGREE WITH, and that route is
		// deliberately supported. But a batch that DOES carry registers is admitting the
		// obligation those registers allocate for, so the exchange must be one of them —
		// looked up by LINEAGE rather than by identifier, because the identifier is exactly
		// what a substitution changes.
		if len(slots) == 0 {
			continue
		}
		reg := byLin[x.Gen+"."+x.Nonce]
		// THE EXCHANGE NONCE IS THE REGISTER'S LINEAGE. Lin is per-OBLIGATION where the
		// register's own nonce is per-ALLOCATION, so the obligation is identified by exactly
		// that value — and after an allocation replacement nothing could reconstruct which
		// exchange a register belonged to, because the identifier is reusable.
		if reg == nil {
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
// out — an obligation the walk never returned is one this claim never saw.
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
		subjects, complete, serr := jsa.streamSubjectsComplete(mqttInflightStreamName,
			mqttExchangeWalkFilter(idHash, gen))
		if serr == nil && !complete {
			// A TRUNCATED ANSWER IS NOT AN ANSWER. The server caps a subject list at
			// JSMaxSubjectDetails and reports the true total beside it; a generation retains
			// its exchange records after they end, so the cap is reachable while the
			// identifier space is not. An obligation outside the first page would be an
			// obligation this claim never saw, and absence is exactly what a claim rests on.
			return fmt.Errorf("transition %q could not enumerate generation %q's obligations "+
				"completely, so the identifier it claims cannot be shown to be free: the "+
				"answer was truncated", a.Txn, gen)
		}
		if serr != nil {
			if isErrorOtherThan(serr, JSStreamNotFoundErr) {
				return fmt.Errorf("transition %q could not enumerate generation %q's obligations, "+
					"so the identifier it claims cannot be shown to be free: %w", a.Txn, gen, serr)
			}
			continue
		}
		for subject := range subjects {
			toks := strings.Split(subject[len(mqttF105SubjectPrefix()):], ".")
			if len(toks) != 4 {
				return fmt.Errorf("transition %q found an uninterpretable obligation subject %q "+
					"in generation %q, which may hold the identifier it claims", a.Txn, subject, gen)
			}
			// A member of THIS batch is the batch's own statement about that obligation, and
			// the collision rules have already compared them with each other.
			if _, mine := resulting[gen+"."+toks[3]]; mine {
				continue
			}
			smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, subject)
			if lerr != nil {
				if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
					return fmt.Errorf("transition %q could not read standing obligation %q: %w",
						a.Txn, subject, lerr)
				}
				continue
			}
			x, derr := mqttDecodeOutboundExchange(smsg.Data, a.ID, a.Epoch, gen, toks[3])
			if derr != nil {
				return fmt.Errorf("transition %q found an unreadable obligation on %q, which may "+
					"hold the identifier it claims: %w", a.Txn, subject, derr)
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
func (jsa *mqttJSA) checkCreatedResources(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttResourceClassTk || e.expect != 0 {
			continue
		}
		// AND ONLY ONE KIND MAY DO IT. Provisioning is new work and requires a serving
		// authority; disposal is permitted after closure, so a creation carried by a
		// disposal would provision under a closed generation through the other kind.
		if a.Kind != mqttTransitionProvisionResource {
			return fmt.Errorf("transition %q of kind %q creates the resource on %q: only a "+
				"provision-resource transition provisions one", a.Txn, a.Kind, e.subj)
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
func (jsa *mqttJSA) checkResourceOverwrites(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	// The exchanges THIS batch installs, so an added edge can be checked against an
	// admission rather than taken on trust.
	createdHere := map[string]struct{}{}
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttExchangeClassTk && e.expect == 0 {
			toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
			if len(toks) == 4 {
				createdHere[toks[3]] = struct{}{}
			}
		}
	}
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
		if err := mqttCheckResourceStep(a, cur, next, createdHere, terminal); err != nil {
			return err
		}
	}
	return nil
}

// mqttResourceEdgeDelta reports the single edge a resource mutation adds or removes.
//
// EXACTLY ONE, OR NONE. A mutation that changed two edges would be two obligations' business
// in one act, and the evidence for each is different; one that swapped an edge for another
// would release something without proving it ended. Both are named here rather than left to
// fall out of a count.
func mqttResourceEdgeDelta(cur, next *mqttOutboundResource) (added, removed string, err error) {
	have := make(map[string]int, len(cur.Deps))
	for _, n := range cur.Deps {
		have[n]++
	}
	for _, n := range next.Deps {
		have[n]--
	}
	for n, d := range have {
		switch {
		case d == 1:
			if removed != _EMPTY_ {
				return _EMPTY_, _EMPTY_, fmt.Errorf("more than one obligation released at once")
			}
			removed = n
		case d == -1:
			if added != _EMPTY_ {
				return _EMPTY_, _EMPTY_, fmt.Errorf("more than one obligation admitted at once")
			}
			added = n
		case d != 0:
			return _EMPTY_, _EMPTY_, fmt.Errorf("obligation %q appears %d times", n, -d)
		}
	}
	if added != _EMPTY_ && removed != _EMPTY_ {
		return _EMPTY_, _EMPTY_, fmt.Errorf("one act both admitted %q and released %q", added, removed)
	}
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
	createdHere map[string]struct{}, terminal func(gen, nonce string) error) error {

	if next.Kind != cur.Kind || next.Name != cur.Name {
		return fmt.Errorf("transition %q re-identifies resource %q: a retirement is about the "+
			"incarnation it was computed for", a.Txn, cur.Inc)
	}
	added, removed, derr := mqttResourceEdgeDelta(cur, next)
	if derr != nil {
		return fmt.Errorf("transition %q mutates resource %q's dependents illegitimately: %w",
			a.Txn, cur.Inc, derr)
	}
	if added != _EMPTY_ {
		if _, ok := createdHere[added]; !ok {
			return fmt.Errorf("transition %q makes resource %q depend on obligation %q, which "+
				"this transition does not admit: a dependency is taken by the admission that "+
				"creates the obligation, never asserted over one that already exists",
				a.Txn, cur.Inc, added)
		}
	}
	if removed != _EMPTY_ {
		if terr := terminal(cur.Gen, removed); terr != nil {
			return fmt.Errorf("transition %q releases resource %q from obligation %q, which has "+
				"not ended: %w", a.Txn, cur.Inc, removed, terr)
		}
	}

	switch {
	case cur.State == mqttResourceLive && next.State == mqttResourceLive:
		// A dependency edge. Nothing else may ride along.
		if next.AuthTxn != _EMPTY_ || next.Outcome != _EMPTY_ {
			return fmt.Errorf("transition %q records an authorization or an outcome on live "+
				"resource %q", a.Txn, cur.Inc)
		}

	case cur.State == mqttResourceLive && next.State == mqttResourceDeleteAuthorized:
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
