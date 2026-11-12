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

// Closing a generation: one batch that ends it, enrols it and makes it discoverable (issue #107).
//
// # What one closing batch contains
//
// The permanent transition outcome, the generation closure, the enrolment's resource and duty
// members, the SEALED ledger, the directory shard header taking the next ordinal, the recovery
// root at that ordinal, and the authority record last, carrying the commit flag. All of it is in
// the $MQTT.inflight. space of one stream and goes in one atomic batch, so it commits together or
// not at all.
//
// # Which durables are enrolled, and the selector is the DISPOSITION
//
// The shared group durables are NOT enrolled. A group consumer is held by several generations
// at once, so no per-generation resource could express it; a generation's ACQUISITIONS of it
// are released by the closing from the generation's own acquisition ledger in the $MQTT.shared. space
// (shared_lifecycle_ops.go, closeSharedGeneration), before its record goes.
//
// The AFFINE consumer is enrolled by the closure, and every closure DISCARDS (issue #133): a
// generation becomes noncurrent only by being discarded. The affine generation outlives
// incarnations, which is exactly why no session-generation fact could ever authorize deleting one
// (issue #40 course E); the discarding closure that enrols it is that authorization.
//
// Ownership is therefore per TARGET and the selector is the closure itself: exactly one
// mechanism owns each durable, and which one is a durable fact rather than a property of
// whichever code path happens to run.
//
// # The affine resource carries the PUBREL durable as a FIELD
//
// Securing the PUBREC proofs reads the records the PUBREL consumer carries, so the readiness work
// needs that NAME after every transient teardown object is gone. The PUBREL consumer is not
// enrolled here because it already is: it was provisioned as a resource of its generation, with
// its provisioning and cleanup duties, before its CREATE, and since issue #107 7d the recovery executor
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
// finalization installs its successor while closing its predecessor. Every closure discards
// (issue #133), so a named affine target is always enrolled.
//
// The ledger member SEALS as it allocates, in the one act, because that is what makes the
// enrolment's completeness establishable at all: after the seal no further ordinal can be taken,
// so the set of duties naming this generation's durables is fixed by the same batch that wrote
// them.
func mqttComposeClosingEnrolment(id, epoch, gen string, t mqttClosingTargets,
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

	if t.AffineDurable != _EMPTY_ {
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

// closeTeardownGeneration is an adopter's close of a marked generation: the claim batch that
// marked the record wrote the closure in the same act (teardown_claim.go), so the closure
// stands for every marker an adopter can find. A marker whose generation has no closure is
// Inconsistent, never closed here: closing it would author a discard no claim decided.
func (jsa *mqttJSA) closeTeardownGeneration(id, epoch, gen string) error {
	_, _, status := jsa.loadGenClosure(id, epoch, gen)
	switch status {
	case mqttOutboundValid:
		return nil
	case mqttOutboundAbsent:
		return fmt.Errorf("tearing down generation %q: its marker stands with no discarding closure, which no claim writes", gen)
	default:
		return fmt.Errorf("tearing down generation %q: its closure reads %v, which licenses nothing", gen, status)
	}
}

// errMQTTClosingUnlicensed: a teardown reached its shared closing with no evidence that the
// retiring generation can never admit again. The seal is withheld — and with it the record's
// release — so the record stays MARKED and the adopter, which settles first, repeats.
var errMQTTClosingUnlicensed = errors.New("the retiring generation's shared closing is not licensed")

// mqttTestClosingFenceHookV runs inside closeRetiringGeneration between its licence and the
// seal, naming the generation: the interval a test holds a closer in. nil in production.
var mqttTestClosingFenceHookV atomic.Pointer[func(gen string)]

// closeRetiringGeneration is the ONE closing driver of a teardown (issue #107 8b), on the owner's
// path and on the adoption: the generation closure in the $MQTT.inflight. space is the ADMISSION fence and
// the ledger seal in the $MQTT.shared. space is the JOIN fence, and the seal is followed by T3, whose
// releases skip the release fence (7e). So T3 may only run where nothing can depend on an
// acquisition it releases — and a dependant needs an admission, which needs the authority
// SERVING at this generation. The caller has already driven its receipted close, if it had a
// receipt. The licence (consult 122):
//
//  1. the ledger is already SEALED: resume T3 and T4. Every production seal was licensed when it
//     was written (this driver, or the recovery executor under a discarding closure), so the
//     seal is that licence made durable — and a closing that crashed after sealing a late join
//     must be finishable from it;
//  2. a closure of this generation stands: every closure discards (issue #133), so the
//     obligations are abandoned by an authorized retirement — seal, release the SEALED set, T4;
//  3. otherwise the generation was NEVER AUTHORIZED (mqttAuthorityNeverServed: the authority
//     read absent, or serving or closed at another generation while no closure of this one
//     exists — a generation the authority ever served ends with a closure, and a generation is
//     never proposed again) AND the ledger reads absent or lists no acquisition, so no
//     acquisition T3 could release has a dependant; a join landing after that observation can
//     be depended on by nothing, is preserved by the seal and is released from the sealed set;
//  4. anything else — SERVING without a receipt, a standing proposal, an indeterminate read —
//     withholds with errMQTTClosingUnlicensed before the seal.
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
	case closed:
	case cst == mqttOutboundAbsent && neverAuthorized && empty:
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

// mqttAuthorityNeverServed reports whether a DETERMINATE authority load proves that generation
// gen was never authorized: the authority is absent, or it serves or is closed at ANOTHER
// generation. Combined with the absence of a closure of gen (the closing driver reads that), the
// generation never served: every generation the authority served ends with a permanent closure,
// and a generation, allocated once by its lifecycle, is never installed again. An indeterminate
// read proves nothing.
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
