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
	"sync/atomic"
)

// Closing a generation: one batch that ends it, enrols it and makes it discoverable (F-105).
//
// # What one closing batch contains
//
// The permanent transition outcome, the generation closure, the enrolment's resource and duty
// members, the SEALED ledger, the directory shard header taking the next ordinal, the recovery
// root at that ordinal, and the authority record last, carrying the commit flag. All of it is on
// $MQTT_inflight and a JetStream atomic batch is per stream, so it commits together or not at all.
//
// # Which durables are enrolled, and the selector is the DISPOSITION
//
// The shared group durables are NOT enrolled. A group consumer is held by several generations
// at once, so no per-generation resource could express it; a generation's ACQUISITIONS of it
// are released by the closing from the generation's own acquisition ledger on $MQTT_shared
// (shared_lifecycle_ops.go, closeSharedGeneration), before its record goes.
//
// The AFFINE consumer is enrolled ONLY when the closure DISCARDS. A closure that preserves the
// session's obligations leaves its affine consumer to the incarnation that continues — the affine
// generation outlives incarnations, which is exactly why no session-generation fact could ever
// authorize deleting one (F-036 course E). `close-admit` cannot carry a discard at all, so a
// non-discarding closed generation is an ordinary population rather than an edge case, and its
// affine consumer stays the TEARDOWN's to delete synchronously, as it is today.
//
// Ownership is therefore per TARGET and the selector is the closure's own immutable `Discard`
// member: exactly one mechanism owns each durable, and which one is a durable fact rather than a
// property of whichever code path happens to run.
//
// # The affine resource carries the PUBREL durable as a FIELD
//
// Securing the PUBREC proofs reads the records the PUBREL consumer carries, so the readiness work
// needs that NAME after every transient teardown object is gone. The PUBREL consumer is not
// enrolled here because it already is: it was provisioned as a resource of its generation, with
// its provisioning and cleanup duties, before its CREATE, and since F-105 7d the recovery executor
// retires it from that record once the generation is discarded. A name is data; nothing has to
// finish a field.

// mqttClosingTargets is what the closing caller says the retiring generation holds.
//
// COMPLETENESS OF THIS IS THE CALLER'S ASSERTION, and the boundary says so rather than pretending
// to check it: the membership lives in a session record the accepted batch has already displaced,
// and `$MQTT_sess` carries MaxMsgsPer 1, so the predecessor's body is evicted by the very write
// that displaces it. What the boundary does enforce is every structural fact about what is here.
type mqttClosingTargets struct {
	// AffineDurable and AffineGen name the session's own affine QoS 2 consumer, empty when the
	// record carried none. PubRelDurable is the proof source the readiness work needs.
	AffineDurable string
	AffineGen     string
	PubRelDurable string
}

// mqttClosingEnrolment is the members a closing batch adds beside its closure.
type mqttClosingEnrolment struct {
	Members []mqttTransitionMember
	// Incs names the resource incarnation minted for each durable, so a caller can report what
	// it enrolled. The affine one is under AffineDurable's name.
	Incs map[string]string
	// RootShard and RootOrd are the address the closure must record.
	RootShard uint64
	RootOrd   uint64
}

// mqttComposeClosingEnrolment builds every member a closing batch carries beside its closure.
//
// gen is the generation being CLOSED, which is not always the transition's own: a discarding
// finalization installs its successor while closing its predecessor. discard is that closure's
// disposition, and it decides whether the affine consumer is enrolled at all.
//
// The ledger member SEALS as it allocates, in the one act, because that is what makes the
// enrolment's completeness establishable at all: after the seal no further ordinal can be taken,
// so the set of duties naming this generation's durables is fixed by the same batch that wrote
// them.
func mqttComposeClosingEnrolment(id, epoch, gen string, discard bool, t mqttClosingTargets,
	cur *mqttGenLedger, curSeq uint64, hdr *mqttDirShard, hdrSeq uint64) (*mqttClosingEnrolment, error) {

	seal, serr := mqttSealGenLedgerMember(id, epoch, gen, cur, curSeq)
	if serr != nil {
		return nil, serr
	}
	out := &mqttClosingEnrolment{Members: []mqttTransitionMember{seal}, Incs: map[string]string{}}

	// THE SEALED LEDGER JUST COMPOSED is the one every later member has to agree with, so it is
	// read back out of the members rather than recomputed. Recomputing it here would be a second
	// definition of the bound, and the two would drift.
	sealedHi, lidx, lerr := mqttSealedHiOf(out.Members, id, epoch, gen)
	if lerr != nil {
		return nil, lerr
	}

	if discard && t.AffineDurable != _EMPTY_ {
		inc := mqttNewInflightNonce()
		sealedHi++
		r := &mqttOutboundResource{
			ID: id, Epoch: epoch, Gen: gen, Inc: inc,
			Kind: mqttResourceAffineConsumer, Name: t.AffineDurable, State: mqttResourceLive,
			// PENDING, and truthfully so: a delayed create may still land under this name. The
			// on-behalf materializer reads a winner's record, checks its teardown marker and
			// creates afterwards, and its own comment says that check does not close the race.
			// The provisioning seal is what records that nothing further can materialize it, and
			// the seal requires the readiness assertion.
			Prov: mqttProvisioningPending, Deps: []mqttResourceEdge{},
			AffGen: t.AffineGen, PubRelDur: t.PubRelDurable,
		}
		if verr := mqttValidateOutboundResource(r, id, epoch, gen, inc); verr != nil {
			return nil, fmt.Errorf("refusing to enrol an unsupported affine consumer: %w", verr)
		}
		rbody, merr := json.Marshal(r)
		if merr != nil {
			return nil, merr
		}
		d := &mqttDutyEntry{
			ID: id, Epoch: epoch, Gen: gen, Ord: sealedHi,
			Kind: mqttDutyCleanup, Ref: inc, State: mqttDutyNotYetActionable,
		}
		dbody, merr := json.Marshal(d)
		if merr != nil {
			return nil, merr
		}
		idHash := getHash(id)
		out.Members = append(out.Members,
			mqttTransitionMember{subj: mqttResourceSubject(idHash, gen, inc), expect: 0, body: rbody},
			mqttTransitionMember{subj: mqttDutySubject(idHash, gen, sealedHi), expect: 0, body: dbody},
		)
		out.Incs[t.AffineDurable] = inc

		// AND THE SEAL MOVES WITH IT. The sealed-ledger member composed above is rewritten to
		// the bound this batch now installs, so exactly one ledger member stands and it names
		// the whole allocation.
		lo, del := uint64(1), uint64(1)
		if cur != nil {
			lo, del = cur.Lo, cur.Del
		}
		led := &mqttGenLedger{
			ID: id, Epoch: epoch, Gen: gen, State: mqttGenLedgerSealed, Hi: sealedHi, SealedHi: sealedHi,
			Lo: lo, Del: del,
		}
		lbody, merr := json.Marshal(led)
		if merr != nil {
			return nil, merr
		}
		out.Members[lidx].body = lbody
	}

	shardMember, rootMember, ord, aerr := mqttAllocateRecoveryRoot(id, epoch, gen, hdr, hdrSeq, sealedHi)
	if aerr != nil {
		return nil, aerr
	}
	out.Members = append(out.Members, shardMember, rootMember)
	out.RootShard, out.RootOrd = shardMember.shardOf(), ord
	return out, nil
}

// shardOf returns the shard a directory member addresses. A member's subject is its address, so
// this reads it back rather than carrying a second copy that could disagree with it.
func (m mqttTransitionMember) shardOf() uint64 {
	shard, _, err := mqttParseRootSubject(m.subj)
	if err == nil {
		return shard
	}
	for i := uint64(0); i < mqttDirShards; i++ {
		if m.subj == mqttShardSubject(i) {
			return i
		}
	}
	return 0
}

// mqttSealedHiOf finds the sealed bound a composed member set installs, and where that member is.
func mqttSealedHiOf(members []mqttTransitionMember, id, epoch, gen string) (uint64, int, error) {
	want := mqttGenLedgerSubject(getHash(id), gen)
	for i, m := range members {
		if m.subj != want {
			continue
		}
		led, derr := mqttDecodeGenLedger(m.body, id, epoch, gen)
		if derr != nil {
			return 0, 0, fmt.Errorf("the composed ledger for generation %q cannot be read back: %w", gen, derr)
		}
		if led.State != mqttGenLedgerSealed {
			return 0, 0, fmt.Errorf("the composed ledger for generation %q is not sealed, so this "+
				"batch does not close it", gen)
		}
		return led.SealedHi, i, nil
	}
	return 0, 0, fmt.Errorf("the composed enrolment for generation %q carries no ledger member", gen)
}

// ---------------------------------------------------------------------------
// Closing a generation, and settling an ambiguous attempt
// ---------------------------------------------------------------------------

// mqttCloseEpisodeAttempts bounds ONE closing episode.
//
// A bound on the EPISODE and not on the client: exhausting it is an error the caller reports, and
// a later caller may enter a fresh episode. Unrelated traffic in one directory shard must never
// become a permanent refusal for a client that happens to hash there.
const mqttCloseEpisodeAttempts = 4

// opCloseTeardownGeneration commits the AUTHORIZED discarding closure of one generation, on
// the authority of its durable teardown receipt (F-105, row 8, Gap 1).
//
// It is opCloseGeneration's discarding sibling, and the difference is where the
// discard's authority comes from. That operation is only ever composed non-discarding by its
// callers; this one LOADS the receipt written atomically with the teardown marker, validates
// it against the standing ownership, and derives the retirement AND the closing targets from
// it. The receipt is what a close-admission lacks — ending a generation is not authority to
// abandon its obligations — so the boundary permits this kind's discard only because the
// operation put an authorizing retirement on the transition for it to bind the affine
// enrolment against.
//
// The standing ownership is validated against the receipt, never reloaded-and-retargeted: a
// resume preserves the generation and changes the tenure, so a receipt captured under one
// tenure must not close a session another tenure is serving. Once the teardown marker is
// claimed no resume can land — a CONNECT that reads a marker adopts it — but the check is the
// same one every closure makes, and it is the fence against a moved ownership.
func (jsa *mqttJSA) opCloseTeardownGeneration(id, epoch, domainTk, txn, gen string) (mqttOpOutcome, error) {
	prev, prevSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if prev == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q carries no authority, so there is no generation to tear down", id)
	}
	rcpt, _, rst := jsa.loadTeardownReceipt(domainTk, getHash(id), gen)
	switch rst {
	case mqttOutboundValid:
	case mqttOutboundAbsent:
		// A SERVING authority with no receipt is not authority to discard. An absence never
		// authorizes abandoning obligations, and inventing a discard from "no receipt" is the
		// exact shape this protocol exists to forbid.
		return mqttOpOutcome{}, fmt.Errorf("discarding %q for %q: no durable teardown receipt authorizes it", gen, id)
	default:
		return mqttOpOutcome{}, fmt.Errorf("discarding %q for %q: its teardown receipt reads %v, which licenses nothing", gen, id, rst)
	}
	return jsa.opCloseTeardownGenerationWith(id, epoch, txn, gen, prev, prevSeq, &rcpt.Retire)
}

// opCloseTeardownGenerationWith composes and commits the discarding closure from an ALREADY
// LOADED authority and an ALREADY CONFIRMED receipt authorization. It is what the owner's
// first-attempt fast path calls: the destroy path loaded the SERVING authority and committed
// the receipt atomically with the marker, so re-reading either is redundant on the first try.
// The authority is still validated against the receipt here, and the boundary re-validates the
// stored predecessor and CAS's at prevSeq — so a peer that moved the authority since it was
// loaded simply fails this attempt's CAS, and the caller falls back to the full re-reading
// episode. Skipping the pre-read never skips the arbitration.
func (jsa *mqttJSA) opCloseTeardownGenerationWith(id, epoch, txn, gen string,
	prev *mqttOutboundAuthority, prevSeq uint64, ret *mqttRetirementRequest) (mqttOpOutcome, error) {

	if prev.State != mqttAuthorityServing {
		return mqttOpOutcome{}, fmt.Errorf("discarding %q for %q: the standing authority is in state %d, "+
			"and a teardown discards a SERVING session; a reservation or preparation is resolved by recovery, not here",
			gen, id, prev.State)
	}
	if prev.Gen != gen {
		return mqttOpOutcome{}, fmt.Errorf("discarding %q for %q: the standing authority serves %q, "+
			"so this teardown belongs to a generation that has moved on", gen, id, prev.Gen)
	}
	if ret.Gen != gen || ret.Tenure != prev.Tenure || ret.OwnerTxn != prev.OwnerTxn {
		return mqttOpOutcome{}, fmt.Errorf("discarding %q/%q/%q for %q: the teardown receipt names %q/%q/%q, "+
			"so it authorizes an ownership that is not the one standing", gen, prev.Tenure, prev.OwnerTxn, id,
			ret.Gen, ret.Tenure, ret.OwnerTxn)
	}
	if !ret.Discard {
		return mqttOpOutcome{}, fmt.Errorf("discarding %q for %q: its teardown receipt does not discard", gen, id)
	}

	targets := mqttClosingTargets{
		AffineDurable: ret.AffineDurable,
		AffineGen:     ret.AffineGen,
		PubRelDurable: ret.PubRelDurable,
	}
	cur, curSeq, lerr := jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	hdr, hdrSeq, herr := jsa.loadDirShardAt(mqttDirShardFor(getHash(id)))
	if herr != nil {
		return mqttOpOutcome{}, herr
	}
	enr, cerr := mqttComposeClosingEnrolment(id, epoch, gen, true, targets, cur, curSeq, hdr, hdrSeq)
	if cerr != nil {
		return mqttOpOutcome{}, cerr
	}

	// The retirement is threaded onto the transition, not a member: the boundary reads it
	// through mqttStandingRetirement to bind the affine enrolment to the target the receipt
	// selected, exactly as a finalization's retirement binds its own. It is transient — the
	// permanent record is the discarding closure and the enrolled resource.
	retCopy := *ret
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: id, Epoch: epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionCloseTeardown, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: id, Epoch: epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityClosed, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
		Closure: &mqttGenClosure{
			ID: id, Epoch: epoch, Gen: prev.Gen, Txn: txn, Discard: true,
			RootShard: enr.RootShard, RootOrd: enr.RootOrd,
		},
		Retire: &retCopy,
		Extra:  enr.Members,
	})
}

// closeTeardownGeneration drives one bounded discarding-teardown closing episode, settling
// ambiguity rather than re-attempting blind.
//
// A standing closure of this generation is this teardown's success only when it actually
// DISCARDS (a Codex correction): "any closure of this generation means success" is too weak
// here — a non-discarding closure would report a teardown that abandoned obligations as done
// while the obligations still stood — so a non-discarding closure of the target generation is a
// contradiction this refuses rather than adopts.
//
// # Why a lost acknowledgement is not a failure
//
// The batch may have committed. Re-composing on that assumption would take a second ordinal, or
// be refused by the create-only closure member and reported as a failure of a close that in fact
// succeeded. So an unresolved send is CLASSIFIED first:
//
//	LANDED / SUPERSEDED -> the closure, the enrolment and the root are permanent. The episode is
//	                       over. SUPERSEDED means a later authority revision stands, which is a
//	                       different fact from "never landed" and is never a reason to repeat.
//	NEVER LANDED        -> a durable ABORTED outcome stands for this exact attempt, so nothing of
//	                       it can ever commit. Re-compose under a NEW identity, against freshly
//	                       read header, authority and ledger.
//	UNRESOLVED          -> nothing is known. abortTransition contends for the attempt's own
//	                       outcome subject with the batch's own member; whoever wins decides it.
//	                       The abort's FAILURE decides nothing either, so the loop re-reads.
//
// # And nothing durable has to remember the attempt
//
// A broker that dies mid-episode leaves no orphaned obligation to rediscover: the CLOSURE is a
// create-only per-generation record, so a later actor reads the state and either finds the
// generation closed — whoever closed it — or closes it under its own identity. A dangling batch
// landing afterwards is refused by that same create-only member or by the header CAS it still
// expects. The frozen handle is an in-process optimization that saves a re-read, never the
// mechanism.
func (jsa *mqttJSA) closeTeardownGeneration(id, epoch, domainTk, gen string) error {
	var lastErr error
	for attempt := 0; attempt < mqttCloseEpisodeAttempts; attempt++ {
		if closed, _, status := jsa.loadGenClosure(id, epoch, gen); status == mqttOutboundValid {
			if closed.Gen == gen {
				if !closed.Discard {
					return fmt.Errorf("tearing down generation %q: a NON-discarding closure already "+
						"stands for it, so this discard did not run and cannot be reported as done", gen)
				}
				return nil
			}
		} else if status != mqttOutboundAbsent {
			lastErr = fmt.Errorf("the closure of generation %q reads %v, which licenses nothing", gen, status)
			continue
		}

		txn := mqttNewInflightNonce()
		out, cerr := jsa.opCloseTeardownGeneration(id, epoch, domainTk, txn, gen)
		if cerr == nil {
			return nil
		}
		lastErr = cerr
		if out.Handle == nil {
			continue
		}
		if done, serr := jsa.settleCloseAttempt(gen, out.Handle, cerr); serr != nil {
			return fmt.Errorf("tearing down generation %q: %w", gen, serr)
		} else if done {
			return nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("tearing down generation %q did not resolve within one episode", gen)
	}
	return fmt.Errorf("tearing down generation %q: %w", gen, lastErr)
}

// closeTeardownGenerationFast is the OWNER's first-attempt fast path (consult 9).
//
// The owner's destroy already loaded the SERVING authority and committed the receipt
// atomically with the marker, so the full episode's pre-read of the closure, the
// re-load of the authority and the re-load of the receipt are all redundant on the first
// try. This carries the exact authority (with its sequence) and the confirmed receipt
// authorization straight into composition, cutting the close from ~6 JetStream round trips
// to ~3 on the happy path — which is what a clean disconnect pays, and what a tight-timed
// concurrent delivery cannot afford to wait behind.
//
// Safety is unchanged, because the ARBITRATION is unchanged: the boundary still validates
// the stored predecessor and the batch still CAS's the authority at prevSeq. A peer that
// moved the authority since destroy loaded it makes this attempt fail its CAS; the attempt
// is then resolved (never recomposed blind on a lost ack), and ANY non-success falls back to
// the full re-reading episode, which reads the durable result and either finds the generation
// already discarded (success) or refuses a moved ownership. The cached inputs are used for
// exactly one attempt and never reused.
func (jsa *mqttJSA) closeTeardownGenerationFast(id, epoch, domainTk, gen string,
	prev *mqttOutboundAuthority, prevSeq uint64, ret *mqttRetirementRequest) error {

	txn := mqttNewInflightNonce()
	out, cerr := jsa.opCloseTeardownGenerationWith(id, epoch, txn, gen, prev, prevSeq, ret)
	if cerr == nil {
		return nil
	}
	// A lost acknowledgement is resolved before anything is recomposed: the batch may have
	// committed. Landed or superseded is done; never-landed hands over to the full episode;
	// anything else is settled with an abort so two attempts can never both be in flight — and
	// an attempt the abort could not settle STOPS here rather than letting the episode compose
	// a second close beside it (F-105 8b, the close-optimization audit).
	if out.Handle != nil {
		if done, serr := jsa.settleCloseAttempt(gen, out.Handle, cerr); serr != nil {
			return fmt.Errorf("tearing down generation %q: %w", gen, serr)
		} else if done {
			return nil
		}
	}
	// The cached authority moved or this attempt was aborted: re-read authority, receipt and
	// closure and drive to a durable result. Never reuse the stale cached inputs beyond here.
	return jsa.closeTeardownGeneration(id, epoch, domainTk, gen)
}

// errMQTTClosingUnlicensed: a teardown reached its shared closing with no evidence that the
// retiring generation can never admit again. The seal is withheld — and with it the record's
// release — so the record stays MARKED and the adopter, which settles first, repeats.
var errMQTTClosingUnlicensed = errors.New("the retiring generation's shared closing is not licensed")

// mqttTestClosingFenceHookV runs inside closeRetiringGeneration between its licence and the
// seal, naming the generation: the interval a test holds a closer in. nil in production.
var mqttTestClosingFenceHookV atomic.Pointer[func(gen string)]

// closeRetiringGeneration is the ONE closing driver of a teardown (F-105 8b), on the owner's
// path and on the adoption: the generation closure on $MQTT_inflight is the ADMISSION fence and
// the ledger seal on $MQTT_shared is the JOIN fence, and the seal is followed by T3, whose
// releases skip the release fence (7e). So T3 may only run where nothing can depend on an
// acquisition it releases — and a dependant needs an admission, which needs the authority
// SERVING at this generation. The caller has already driven its receipted close, if it had a
// receipt. The licence (consult 122):
//
//  1. the ledger is already SEALED: resume T3 and T4. Every production seal was licensed when it
//     was written (this driver, or the recovery executor under a discarding closure), so the
//     seal is that licence made durable — and a closing that crashed after sealing a late join
//     must be finishable from it;
//  2. a DISCARDING closure of this generation stands: the obligations are abandoned by an
//     authorized retirement — seal, release the SEALED set, T4;
//  3. otherwise admission must be positively fenced — a closure of this generation stands
//     (read FIRST), or the generation was NEVER AUTHORIZED (mqttAuthorityNeverServed: the
//     authority read absent, or serving or closed at another generation while no closure of
//     this one exists — a generation the authority ever served ends with a closure, and a
//     generation is never proposed again) — AND the ledger must read absent or list no
//     acquisition, so no acquisition
//     T3 could release has a dependant; a join landing after that observation can be depended on
//     by nothing, is preserved by the seal and is released from the sealed set;
//  4. anything else — SERVING without a receipt, a standing proposal, a preserving closure over a
//     listed acquisition, an indeterminate read — withholds with errMQTTClosingUnlicensed before
//     the seal.
//
// The seal stays its own record: the shl.<idHash>.<gen> delayed-write exclusion is never
// replaced by a lookup of the closure.
func (jsa *mqttJSA) closeRetiringGeneration(id, epoch, gen string, neverAuthorized bool) error {
	closure, _, cst := jsa.loadGenClosure(id, epoch, gen)
	if cst != mqttOutboundValid && cst != mqttOutboundAbsent {
		return fmt.Errorf("%w: the closure of generation %q reads %v", errMQTTClosingUnlicensed, gen, cst)
	}
	closed := cst == mqttOutboundValid && closure != nil && closure.Gen == gen
	l := jsa.loadSharedLedger(id, epoch, gen)
	if l.status != mqttOutboundValid && l.status != mqttOutboundAbsent {
		return fmt.Errorf("%w: the ledger of generation %q reads %v", errMQTTClosingUnlicensed, gen, l.status)
	}
	sealed := l.status == mqttOutboundValid && l.rec.State == mqttSharedLedgerSealed
	empty := l.status == mqttOutboundAbsent || len(l.rec.Acqs) == 0
	switch {
	case sealed:
	case closed && closure.Discard:
	case (closed || (cst == mqttOutboundAbsent && neverAuthorized)) && empty:
	default:
		return fmt.Errorf("%w: generation %q (closure %v, ledger %v with %d acquisition(s), never authorized %v)",
			errMQTTClosingUnlicensed, gen, cst, l.status, mqttSharedLedgerLen(l), neverAuthorized)
	}
	if h := mqttTestClosingFenceHookV.Load(); h != nil {
		(*h)(gen)
	}
	return jsa.closeSharedGeneration(id, epoch, gen)
}

// mqttSharedLedgerLen is how many acquisitions a ledger load lists (zero for anything but a
// valid record).
func mqttSharedLedgerLen(l mqttSharedLedgerLoad) int {
	if l.status != mqttOutboundValid || l.rec == nil {
		return 0
	}
	return len(l.rec.Acqs)
}

// settleCloseAttempt decides a sent close attempt whose send failed, before anything is
// recomposed (consult 9's criterion, enforced since F-105 8b): landed or superseded is done;
// never-landed leaves nothing in flight, so the caller may compose again; anything else is
// aborted and resolved again, and an attempt that is STILL not decisive — the abort failed, or
// the second resolution is unresolved — is returned as an error. Composing a fresh close then
// would put two attempts in flight, and the CAS and the create-only closure converging on one
// of them is not the same as the protocol never issuing the second.
func (jsa *mqttJSA) settleCloseAttempt(gen string, h *mqttTransitionHandle, cerr error) (done bool, err error) {
	switch v := jsa.resolveTransitionHandle(h); v {
	case mqttTransitionLanded, mqttTransitionSuperseded:
		return true, nil
	case mqttTransitionNeverLanded:
		return false, nil
	}
	if aerr := jsa.abortTransition(&h.attempt); aerr != nil {
		return false, fmt.Errorf("the close of generation %q was unresolved (%v) and settling it failed: %w", gen, cerr, aerr)
	}
	switch v2 := jsa.resolveTransitionHandle(h); v2 {
	case mqttTransitionLanded, mqttTransitionSuperseded:
		return true, nil
	case mqttTransitionNeverLanded:
		return false, nil
	default:
		return false, fmt.Errorf("the close of generation %q stays unresolved after its abort (%v)", gen, cerr)
	}
}

// mqttAuthorityNeverServed reports whether a DETERMINATE authority load proves that generation
// gen was never authorized: the authority is absent, or it serves or is closed at ANOTHER
// generation. Combined with the absence of a closure of gen (the closing driver reads that), the
// generation never served: every generation the authority served ends with a permanent closure,
// and a generation, minted fresh per proposal, is never proposed again. A standing proposal
// (RESERVING, PREPARING) or an indeterminate read proves nothing.
func mqttAuthorityNeverServed(al mqttAuthorityLoad, gen string) bool {
	switch al.status {
	case mqttOutboundAbsent:
		return true
	case mqttOutboundValid:
		return al.rec != nil && al.rec.Gen != gen &&
			(al.rec.State == mqttAuthorityServing || al.rec.State == mqttAuthorityClosed)
	default:
		return false
	}
}
