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

// The recovery directory and its sweep cursor (F-105, row 6).
//
// # Who resumes work after a restart: nobody in particular
//
// Any broker serving the account may resume a duty. There is no owner election, because an
// election is an availability dependency on exactly the state being recovered. What makes that
// workable is that a duty's execution is safe under CONCURRENT execution — see the claim's
// comment on the duty entry, which authorizes nothing and proves nothing.
//
// # Why the directory is PAGED, which the first design got wrong
//
// An earlier version put every root for a shard in ONE record per shard, with a fixed shard
// count. With S fixed shards and a growing population of outstanding generations, at least one
// record grows with the account until the first-duty batch can no longer add its root within the
// message limit — and reading that whole record to iterate every root silently multiplies the
// 128-entry work budget by the number of roots it found.
//
// So a shard is a HEADER plus paged ENTRIES, the same shape the generation ledger and its duty
// inventory already have one level down: `shard` carries the high-water and the traversal
// position, `root` is one generation at a stable ordinal.
//
// # The cursor is FOUR fields, and a single accumulated minimum is wrong
//
// A cursor that records "the lowest ordinal I could not finish" and updates it by taking a
// minimum can only ever fall. Ordinal 7 is unresolved in sweep 1, so the mark is 7; it completes
// during sweep 2; the minimum with 7 is still 7, and the generation never retires.
//
// The fix is to keep the PREVIOUS sweep's restart point apart from the CURRENT sweep's
// accumulator, and reset the accumulator when the next sweep begins. A duty resolved this time
// round simply never enters this sweep's accumulator, so the next restart point rises.
//
// # And there is no progress guarantee to claim
//
// A duty blocked on external evidence stays unresolved across arbitrarily many sweeps. What this
// mechanism guarantees is BOUNDED WORK PER PASS and NO PERMANENT SKIP. How long a duty stays
// open is a function of the evidence it waits for, and no cursor discipline changes that.

// The record classes this file owns.
const (
	mqttShardClassTk = "shard" // one directory shard's high-water and traversal position
	mqttRootClassTk  = "root"  // one generation enrolled for recovery, at a stable ordinal
)

// mqttDirShards is the fixed number of directory shards.
//
// Fixed, and that is safe ONLY because the shard is paged: a fixed count with an unbounded
// record per shard was the defect this layout replaces. What the count bounds now is the number
// of independent traversal positions, not the size of anything.
const mqttDirShards = 64

// mqttDirShardFor returns the shard a client's generations are enrolled in.
//
// By CLIENT rather than by generation, so every generation of one client lands in one shard and
// a sweep that reaches a client sees all of its outstanding work together.
func mqttDirShardFor(idHash string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(idHash); i++ {
		h ^= uint64(idHash[i])
		h *= 1099511628211
	}
	return h % mqttDirShards
}

func mqttShardSubject(shard uint64) string {
	return mqttF105SubjectPrefix() + mqttShardClassTk + "." + strconv.FormatUint(shard, 10)
}

func mqttRootSubject(shard, ord uint64) string {
	return mqttF105SubjectPrefix() + mqttRootClassTk + "." + strconv.FormatUint(shard, 10) + "." +
		mqttFormatOrdinal(ord)
}

func mqttIsShardSubject(subject string) bool {
	return strings.HasPrefix(subject, mqttF105SubjectPrefix()+mqttShardClassTk+".")
}

func mqttIsRootSubject(subject string) bool {
	return strings.HasPrefix(subject, mqttF105SubjectPrefix()+mqttRootClassTk+".")
}

// ---------------------------------------------------------------------------
// The shard header
// ---------------------------------------------------------------------------

// mqttDirShard is one shard's root high-water and traversal position.
type mqttDirShard struct {
	Shard uint64 `json:"shard"`
	// Hi is the highest root ordinal allocated in this shard. Enrolling a generation CASes
	// this record, which is what makes root ordinals contiguous for the same reason duty
	// ordinals are: a batch that loses the CAS lands nothing, so no ordinal is consumed by a
	// refused enrolment.
	Hi uint64 `json:"hi"`
	// Next is where the account-wide walk resumes. A RESUMPTION HINT and never a completion
	// record: a pass that ends here starts the next one at Next, and nothing about a root below
	// it is thereby claimed to be finished.
	Next uint64 `json:"next"`
	// Cycle and Horizon are what stop a busy shard from starving an old root.
	//
	// A cycle walks ordinals 1..Horizon, where Horizon was the shard's high-water when the cycle
	// BEGAN. An allocation raising Hi mid-cycle does not move that horizon, so the wrap is a
	// definite position rather than a moving target the walk can never reach. Re-reading a fresh
	// high-water each pass was the defect this pair replaces: with continuous allocation the
	// walk could advance for ever and never revisit ordinal 1.
	//
	// The cycle also decides whose progress a header write may merge. Within one cycle Next is
	// merged as a maximum, so no walker rolls another back; a walker holding an older cycle
	// discards its position entirely, because a position means nothing in a cycle it did not
	// walk.
	Cycle   uint64 `json:"cycle"`
	Horizon uint64 `json:"horizon"`
}

var mqttShardMembers = map[string]struct{}{
	"shard": {}, "hi": {}, "next": {}, "cycle": {}, "horizon": {},
}

var mqttShardRequired = map[string]struct{}{
	"shard": {}, "hi": {}, "next": {}, "cycle": {}, "horizon": {},
}

func mqttDecodeDirShard(data []byte, wantShard uint64) (*mqttDirShard, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttShardMembers, "a directory shard"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttShardRequired, "a directory shard"); err != nil {
		return nil, err
	}
	s := &mqttDirShard{}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	if err := mqttValidateDirShard(s, wantShard); err != nil {
		return nil, err
	}
	return s, nil
}

func mqttValidateDirShard(s *mqttDirShard, wantShard uint64) error {
	if s == nil {
		return fmt.Errorf("directory shard is absent")
	}
	if s.Shard != wantShard {
		return fmt.Errorf("directory shard names %d on the subject of %d", s.Shard, wantShard)
	}
	if s.Shard >= mqttDirShards {
		return fmt.Errorf("directory shard %d is outside the %d this broker addresses",
			s.Shard, mqttDirShards)
	}
	// THE TRAVERSAL POSITION MAY SIT ONE PAST THE HIGH-WATER and no further: Hi+1 is the
	// caught-up position, meaning every root allocated so far has been visited this pass.
	// Anything beyond it names a root nothing ever allocated, which a walk would read as an
	// empty shard and stop.
	if s.Next > s.Hi+1 {
		return fmt.Errorf("directory shard %d resumes at %d with a high-water of %d: a position "+
			"past the high-water names a root nothing allocated", s.Shard, s.Next, s.Hi)
	}
	if s.Next == 0 {
		return fmt.Errorf("directory shard %d resumes at ordinal zero, and ordinals start at 1",
			s.Shard)
	}
	if s.Cycle == 0 {
		return fmt.Errorf("directory shard %d carries cycle zero, and cycles start at 1", s.Shard)
	}
	// THE HORIZON IS A HIGH-WATER THIS SHARD ONCE HAD, so it can never exceed the one it has
	// now: Hi never decreases, and a horizon above it names roots nothing ever allocated.
	if s.Horizon > s.Hi {
		return fmt.Errorf("directory shard %d carries horizon %d above its high-water %d: a "+
			"horizon is a high-water this shard once had, and a high-water is never handed back",
			s.Shard, s.Horizon, s.Hi)
	}
	// AND THE POSITION SITS INSIDE THE CYCLE IT BELONGS TO. Horizon+1 is the completed
	// position, exactly as Hi+1 is the caught-up one.
	if s.Next > s.Horizon+1 {
		return fmt.Errorf("directory shard %d resumes at %d past its cycle horizon of %d: the "+
			"completed position is one past the horizon and never further", s.Shard, s.Next, s.Horizon)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Allocating a root, and advancing the traversal
// ---------------------------------------------------------------------------

// mqttAllocateRecoveryRoot builds the two members a CLOSING batch adds to enrol its generation
// for recovery: the shard header taking the next ordinal, and the root at that ordinal.
//
// # Why this rides the closing batch and not a write of its own
//
// Every F-105 class lives on $MQTT_inflight, and a JetStream atomic batch is per stream, so the
// closure, the enrolment, the sealed ledger, the header and the root commit together or not at
// all. A second write after the closure would leave a crash window in which a sealed, enrolled,
// CLOSED generation has no root — which the release gate refuses for ever, and which could only
// be repaired by discovering unrooted closed generations, the very enumeration the paged
// directory exists to avoid.
//
// The invariant is therefore stated as: CLOSURE COMMITS WITH A REACHABLE ROOT, and thereafter
// recovery ownership stands as that root or as a valid completion certificate. Not "a closed
// generation always has a root" — retirement removes it.
//
// # The bound is the seal this same batch installs
//
// Bound is the generation's SealedHi as the batch leaves it, duties this batch allocates
// included. Capturing the pre-batch high-water would send the first sweep home one duty early,
// and a bound that moved afterwards would let a sweep chase a target it can never finish.
//
// hdr is the shard header as it stands, nil when none does; hdrSeq the sequence it was read at.
func mqttAllocateRecoveryRoot(id, epoch, gen string, hdr *mqttDirShard, hdrSeq uint64,
	sealedHi uint64) (shardMember, rootMember mqttTransitionMember, ord uint64, err error) {

	var zero mqttTransitionMember
	if !mqttValidSessGen(gen) {
		return zero, zero, 0, fmt.Errorf("allocating a recovery root under malformed generation %q", gen)
	}
	if id == _EMPTY_ || epoch == _EMPTY_ {
		return zero, zero, 0, fmt.Errorf("allocating a recovery root requires the client and the " +
			"account recovery epoch")
	}
	shard := mqttDirShardFor(getHash(id))
	next := &mqttDirShard{Shard: shard, Hi: 1, Next: 1, Cycle: 1, Horizon: 1}
	if hdr != nil {
		if hdrSeq == 0 {
			return zero, zero, 0, fmt.Errorf("allocating in shard %d requires the sequence its "+
				"header was read at", shard)
		}
		// ORDINALS ARE CONTIGUOUS BECAUSE THE BATCH IS ATOMIC: a batch that loses this CAS lands
		// nothing, so no ordinal is consumed by a refused enrolment.
		if hdr.Hi == ^uint64(0) {
			return zero, zero, 0, fmt.Errorf("shard %d has allocated its last ordinal", shard)
		}
		// EVERYTHING ELSE IS PRESERVED EXACTLY. Allocation is not traversal: moving Next, the
		// cycle or its horizon here would silently reposition every walker on this shard.
		next = &mqttDirShard{
			Shard: shard, Hi: hdr.Hi + 1, Next: hdr.Next, Cycle: hdr.Cycle, Horizon: hdr.Horizon,
		}
	} else if hdrSeq != 0 {
		return zero, zero, 0, fmt.Errorf("allocating in shard %d against sequence %d with no "+
			"header standing there", shard, hdrSeq)
	}
	if verr := mqttValidateDirShard(next, shard); verr != nil {
		return zero, zero, 0, fmt.Errorf("refusing to compose an unsupported directory shard: %w", verr)
	}
	hbody, herr := json.Marshal(next)
	if herr != nil {
		return zero, zero, 0, herr
	}

	ord = next.Hi
	// A ROOT IS BORN AT THE START OF ITS FIRST SWEEP, and for a generation that allocated
	// nothing that position is already the finished one: SweepStart 1, Resume 1, Bound 0, which
	// mqttSweepFinished reads as complete. Such a root retires on its first visit, which is
	// correct — there is nothing to walk.
	root := &mqttRecoveryRoot{
		Shard: shard, Ord: ord, ID: id, Epoch: epoch, Gen: gen,
		SweepStart: 1, Resume: 1, Bound: sealedHi,
	}
	if sealedHi == ^uint64(0) {
		return zero, zero, 0, fmt.Errorf("generation %q sealed at the maximum ordinal, so the "+
			"finished position one past it cannot be expressed", gen)
	}
	if verr := mqttValidateRecoveryRoot(root, shard, ord); verr != nil {
		return zero, zero, 0, fmt.Errorf("refusing to compose an unsupported recovery root: %w", verr)
	}
	rbody, rerr := json.Marshal(root)
	if rerr != nil {
		return zero, zero, 0, rerr
	}
	return mqttTransitionMember{subj: mqttShardSubject(shard), expect: hdrSeq, body: hbody},
		mqttTransitionMember{subj: mqttRootSubject(shard, ord), expect: 0, body: rbody},
		ord, nil
}

// mqttAdvanceShard applies a walker's traversal position to the header as it now stands.
//
// THE CYCLE DECIDES WHOSE POSITION COUNTS, and the four cases are exhaustive over what a walker
// can hold — a walker's own cycle is only ever one it read from a COMMITTED header, so a stored
// cycle BELOW it is unreachable rather than merely unhandled:
//
//   - same cycle, walker has not finished it: merge Next as a MAXIMUM, so no walker rolls another
//     back, and keep the stored horizon;
//   - same cycle, and the HEADER's position has reached the horizon: wrap, as a CAS from exactly
//     this cycle, capturing the horizon from the high-water as it now stands;
//   - same cycle, the header has NOT reached the horizon although this walker's own walk is done:
//     merge and stop. A cycle ends when the header's progress reaches the horizon, not when one
//     worker personally visited it;
//   - stored cycle newer: DISCARD the local position entirely and adopt what stands. A position
//     means nothing in a cycle this walker did not walk.
//
// Returns nil when there is nothing to write, which is not a failure: it means another walker has
// already recorded at least as much progress.
func mqttAdvanceShard(stored *mqttDirShard, storedSeq uint64, myCycle, myNext uint64) (*mqttDirShard, error) {
	if stored == nil {
		return nil, fmt.Errorf("advancing a shard requires the header it advances")
	}
	if storedSeq == 0 {
		return nil, fmt.Errorf("advancing shard %d requires the sequence its header was read at",
			stored.Shard)
	}
	if myCycle == 0 || myNext == 0 {
		return nil, fmt.Errorf("advancing shard %d requires a cycle and a position, and both "+
			"start at 1", stored.Shard)
	}
	if myCycle > stored.Cycle {
		// A POSITION FROM A CYCLE THE HEADER HAS NEVER REACHED. The walker cannot have read it
		// from a committed header, so it is a proposal that never landed.
		return nil, fmt.Errorf("shard %d stands at cycle %d and a walker offers a position in "+
			"cycle %d, which no committed header ever carried", stored.Shard, stored.Cycle, myCycle)
	}
	if myCycle < stored.Cycle {
		return nil, nil
	}
	if stored.Next > stored.Horizon {
		// THE CYCLE IS COMPLETE — by the header's own progress, whoever made it. Wrap.
		if stored.Cycle == ^uint64(0) {
			return nil, fmt.Errorf("shard %d has exhausted its cycle counter", stored.Shard)
		}
		next := &mqttDirShard{
			Shard: stored.Shard, Hi: stored.Hi, Next: 1,
			Cycle: stored.Cycle + 1, Horizon: stored.Hi,
		}
		if verr := mqttValidateDirShard(next, next.Shard); verr != nil {
			return nil, fmt.Errorf("refusing to wrap shard %d into an unsupported header: %w",
				stored.Shard, verr)
		}
		return next, nil
	}
	if myNext <= stored.Next {
		return nil, nil
	}
	if myNext > stored.Horizon+1 {
		myNext = stored.Horizon + 1
	}
	next := &mqttDirShard{
		Shard: stored.Shard, Hi: stored.Hi, Next: myNext,
		Cycle: stored.Cycle, Horizon: stored.Horizon,
	}
	if verr := mqttValidateDirShard(next, next.Shard); verr != nil {
		return nil, fmt.Errorf("refusing to advance shard %d into an unsupported header: %w",
			stored.Shard, verr)
	}
	return next, nil
}

// persistShard writes a shard header back at the sequence it was read at.
//
// CAS, like every other progress write here: a stale walker is refused rather than overwriting a
// newer cycle, and the loser simply re-reads. NOT AN AUTHORITY TRANSITION, for the same reason
// persistSweep is not — a recovery pass may run on a broker holding no authority for any client
// in this shard, and requiring one would make traversal depend on the entitlement it exists to
// survive.
func (jsa *mqttJSA) persistShard(h *mqttDirShard, expect uint64) (uint64, error) {
	if h == nil || expect == 0 {
		return 0, fmt.Errorf("persisting a shard header requires it and the sequence it was read at")
	}
	if verr := mqttValidateDirShard(h, h.Shard); verr != nil {
		return 0, fmt.Errorf("refusing to persist an unsupported directory shard: %w", verr)
	}
	body, err := json.Marshal(h)
	if err != nil {
		return 0, err
	}
	msg, hdr := mqttFrameBatchWrite(expect, body, _EMPTY_, 0, false)
	resp, serr := jsa.storeMsg(mqttShardSubject(h.Shard), hdr, msg)
	if serr != nil {
		return 0, serr
	}
	return resp.Sequence, nil
}

// loadDirShardAt reads one shard header with the sequence it stands at.
func (jsa *mqttJSA) loadDirShardAt(shard uint64) (*mqttDirShard, uint64, error) {
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, mqttShardSubject(shard))
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, fmt.Errorf("reading directory shard %d: %w", shard, err)
		}
		return nil, 0, nil
	}
	if smsg == nil {
		return nil, 0, nil
	}
	h, derr := mqttDecodeDirShard(smsg.Data, shard)
	if derr != nil {
		return nil, 0, fmt.Errorf("directory shard %d cannot be interpreted: %w", shard, derr)
	}
	return h, smsg.Sequence, nil
}

// ---------------------------------------------------------------------------
// One enrolled generation, and its sweep cursor
// ---------------------------------------------------------------------------

// mqttRecoveryRoot is one generation enrolled for recovery, with the cursor of the sweep
// currently walking it.
type mqttRecoveryRoot struct {
	// Shard and Ord are this root's stable address. The ordinal never changes, which is what
	// lets the shard's traversal position be an integer rather than a set.
	Shard uint64 `json:"shard"`
	Ord   uint64 `json:"ord"`

	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`

	// SweepStart is the ordinal THIS sweep began at, Resume is THE NEXT ORDINAL TO EXAMINE,
	// and Bound is the inventory bound captured when this sweep started.
	//
	// Bound is persisted rather than re-read per batch. A bound limits ONE SWEEP, not the
	// generation: ordinals allocated past it belong to a later sweep, and re-capturing it
	// mid-sweep would let a sweep chase a moving target it can never finish.
	SweepStart uint64 `json:"sweep_start"`
	Resume     uint64 `json:"resume"`
	Bound      uint64 `json:"bound"`

	// Unresolved is THIS sweep's accumulator: the lowest ordinal it has left unresolved, or 0
	// for none yet.
	//
	// Kept APART from SweepStart, and reset when the next sweep begins. A single field updated
	// by taking a minimum can only fall, so a duty that completes after being counted once
	// would hold the cursor at its ordinal forever and the generation would never retire.
	Unresolved uint64 `json:"unresolved,omitempty"`
}

var mqttRootMembers = map[string]struct{}{
	"shard": {}, "ord": {}, "id": {}, "epoch": {}, "gen": {},
	"sweep_start": {}, "resume": {}, "bound": {}, "unresolved": {},
}

var mqttRootRequired = map[string]struct{}{
	"shard": {}, "ord": {}, "id": {}, "epoch": {}, "gen": {},
	"sweep_start": {}, "resume": {}, "bound": {},
}

var mqttRootOptional = map[string]struct{}{"unresolved": {}}

func mqttDecodeRecoveryRoot(data []byte, wantShard, wantOrd uint64) (*mqttRecoveryRoot, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttRootMembers, "a recovery root"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttRootRequired, "a recovery root"); err != nil {
		return nil, err
	}
	if err := mqttJSONOptionalMembersNotNull(data, mqttRootOptional, "a recovery root"); err != nil {
		return nil, err
	}
	r := &mqttRecoveryRoot{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}
	if err := mqttValidateRecoveryRoot(r, wantShard, wantOrd); err != nil {
		return nil, err
	}
	return r, nil
}

func mqttValidateRecoveryRoot(r *mqttRecoveryRoot, wantShard, wantOrd uint64) error {
	if r == nil {
		return fmt.Errorf("recovery root is absent")
	}
	if r.Shard != wantShard || r.Ord != wantOrd {
		return fmt.Errorf("recovery root names %d/%d on the subject of %d/%d",
			r.Shard, r.Ord, wantShard, wantOrd)
	}
	if r.Ord == 0 {
		return fmt.Errorf("recovery root carries ordinal zero, and ordinals start at 1")
	}
	if !mqttValidSessGen(r.Gen) {
		return fmt.Errorf("recovery root carries a malformed generation %q", r.Gen)
	}
	if r.ID == _EMPTY_ || r.Epoch == _EMPTY_ {
		return fmt.Errorf("recovery root names no client or no recovery epoch")
	}
	// THE ROOT IS IN THE SHARD ITS CLIENT HASHES TO. A root filed elsewhere would be
	// unreachable by every sweep that looks for that client's work in the right place.
	if got := mqttDirShardFor(getHash(r.ID)); got != r.Shard {
		return fmt.Errorf("recovery root for client %q sits in shard %d but that client hashes "+
			"to %d, so no sweep would look for it there", r.ID, r.Shard, got)
	}
	if r.SweepStart == 0 || r.Resume == 0 {
		return fmt.Errorf("recovery root carries sweep start %d and resume %d: a sweep begins at "+
			"ordinal 1, never at 0", r.SweepStart, r.Resume)
	}
	if r.Resume < r.SweepStart {
		return fmt.Errorf("recovery root resumes at %d, before its sweep began at %d",
			r.Resume, r.SweepStart)
	}
	// RESUME MAY SIT ONE PAST THE BOUND, and that position means COMPLETED THIS SWEEP.
	// Because Resume names the NEXT ordinal to examine, reaching Bound still leaves that entry
	// unprocessed — the completed position is Bound+1, and an off-by-one here would retire a
	// generation one duty early.
	if r.Resume > r.Bound+1 {
		return fmt.Errorf("recovery root resumes at %d past a captured bound of %d: the completed "+
			"position is one past the bound and never further", r.Resume, r.Bound)
	}
	if r.Unresolved != 0 && (r.Unresolved < r.SweepStart || r.Unresolved >= r.Resume) {
		return fmt.Errorf("recovery root carries unresolved ordinal %d outside the range this "+
			"sweep has examined (%d..%d)", r.Unresolved, r.SweepStart, r.Resume-1)
	}
	return nil
}

// mqttSweepOutcome is what one batch of a sweep established.
type mqttSweepOutcome struct {
	// Examined is how many ordinals this batch looked at, and LowestUnresolved the lowest one
	// it could not count terminal — including any that was MISSING or MALFORMED, which are
	// inconsistencies and never completed work.
	Examined         uint64
	LowestUnresolved uint64
}

// mqttAdvanceSweep applies one batch's outcome to a root's cursor.
//
// Pure, and separated from the pass that reads records, because the cursor arithmetic is the
// part that was wrong in the first design and the part a test can pin exactly.
func mqttAdvanceSweep(cur *mqttRecoveryRoot, out mqttSweepOutcome) (*mqttRecoveryRoot, error) {
	if cur == nil {
		return nil, fmt.Errorf("advancing a sweep requires the root it walks")
	}
	next := *cur
	if out.LowestUnresolved != 0 {
		if out.LowestUnresolved < cur.Resume {
			return nil, fmt.Errorf("a batch reported ordinal %d unresolved, which this sweep "+
				"examined before resuming at %d", out.LowestUnresolved, cur.Resume)
		}
		// THE LOWEST OF THIS SWEEP'S UNRESOLVED ORDINALS. A minimum is right HERE, within one
		// sweep, and wrong across sweeps — which is the distinction the accumulator's reset
		// exists to make.
		if next.Unresolved == 0 || out.LowestUnresolved < next.Unresolved {
			next.Unresolved = out.LowestUnresolved
		}
	}
	next.Resume = cur.Resume + out.Examined
	if next.Resume > cur.Bound+1 {
		next.Resume = cur.Bound + 1
	}
	return &next, nil
}

// mqttSweepFinished reports whether this sweep has examined every ordinal through its bound.
//
// Resume names the NEXT ordinal to examine, so the finished position is one PAST the bound.
func mqttSweepFinished(r *mqttRecoveryRoot) bool {
	return r != nil && r.Resume == r.Bound+1
}

// mqttRollSweep starts the next sweep over a root that has finished this one.
//
// This is where the accumulator is RESET, and it is the whole correction to the first design:
//
//	nextStart = Unresolved,   if this sweep left anything unresolved
//	            Bound + 1,    otherwise
//
// The second case is what a single accumulated minimum could never express. A sweep that
// resolved everything starts the next one PAST its old bound, so the generation moves forward
// instead of re-walking ordinals it has already established terminal.
//
// newBound is the generation's bound as it stands NOW — its sealed high-water if sealed, else
// its current high-water. Captured once, here, at the start of the sweep.
func mqttRollSweep(cur *mqttRecoveryRoot, newBound uint64) (*mqttRecoveryRoot, error) {
	if cur == nil {
		return nil, fmt.Errorf("rolling a sweep requires the root it walks")
	}
	if !mqttSweepFinished(cur) {
		return nil, fmt.Errorf("the sweep over %q/%q resumes at %d against a bound of %d, so it "+
			"has not finished and does not roll", cur.ID, cur.Gen, cur.Resume, cur.Bound)
	}
	nextStart := cur.Bound + 1
	if cur.Unresolved != 0 {
		nextStart = cur.Unresolved
	}
	if newBound+1 < nextStart {
		// THE BOUND WENT BACKWARDS, which nothing may do: a high-water is never handed back and
		// a sealed bound is fixed at it. Refusing is the only safe answer, because starting a
		// sweep past its own bound would report it finished without examining anything.
		return nil, fmt.Errorf("the next sweep over %q/%q would start at %d against a bound of "+
			"%d: a bound does not move backwards", cur.ID, cur.Gen, nextStart, newBound)
	}
	next := *cur
	next.SweepStart, next.Resume, next.Bound, next.Unresolved = nextStart, nextStart, newBound, 0
	return &next, nil
}

// mqttRootRetires reports whether this generation's recovery may end.
//
// FOUR CONDITIONS, and they are a proof rather than a checklist:
//
//  1. every ordinal below SweepStart was previously established terminal, and by the boundary's
//     irreversibility rule cannot reopen;
//  2. this sweep examined every ordinal from SweepStart through Bound;
//  3. its accumulator is empty;
//  4. the authoritative ledger is SEALED with SealedHi == Bound;
//  5. every position the floor retired is physically deleted, Del == Lo (F-112). Recovery is the
//     named discovery owner of deletions already authorized below a sealed floor, so the root
//     may not go while any remain: a retired root would leave them nobody's.
//
// Equality with the sealed bound is NOT sufficient on its own, and neither is a clean sweep. A
// sweep that captured an OPEN bound may still retire — if closure sealed at exactly that bound
// with no further allocation, the four conditions hold and retirement is correct. What excludes
// the unsafe case is the equality itself: an open capture that differs from the eventual
// SealedHi cannot retire, and needs no separate "captured while sealed" flag to say so.
func mqttRootRetires(r *mqttRecoveryRoot, led *mqttGenLedger) bool {
	if r == nil || led == nil {
		return false
	}
	if led.State != mqttGenLedgerSealed {
		return false
	}
	return mqttSweepFinished(r) && r.Unresolved == 0 && led.SealedHi == r.Bound && led.Del == led.Lo
}

// ---------------------------------------------------------------------------
// Walking one generation's inventory
// ---------------------------------------------------------------------------

// mqttSweepBatchMax is how many entries one batch of a sweep reads.
//
// The bound belongs to the BACKGROUND pass and not to pre-CONNACK reconstruction, which must
// establish a complete reservation picture or withhold allocation entirely. Conflating the two
// would either stall every reconnect or hand out an identifier the client is holding.
const mqttSweepBatchMax = 128

// mqttSweptDuty is one entry a batch looked at, with what the sweep could establish about it.
type mqttSweptDuty struct {
	Ord uint64
	// Entry is the decoded duty, or nil when the entry was MISSING or MALFORMED. Nil is not
	// "nothing to do": a missing or unreadable entry is an INCONSISTENCY, never completed work,
	// and it enters the accumulator exactly as an open duty does.
	Entry *mqttDutyEntry
	// Exchange, when set, is the exchange an exchange duty names as its reader already read it:
	// a resolver may use it instead of reading it again (an ending is absorbing). Nil: read it.
	Exchange *mqttOutboundExchange
	// Why records what made this entry unresolved, for the operator who has to act on it. Empty
	// for a resolved one.
	Why string
	// Census marks a RESOLVED exchange duty of a DISCARDED generation, returned so its discard
	// work can be judged (F-105 7d): it is unresolved only if that work remains, which the visit
	// decides when it classifies the entry, never the batch read.
	Census bool
}

// sweepOneBatchCharged is sweepOneBatch with a per-ENTRY charge, which is what makes the pass's
// budget a bound on store operations rather than on invocations.
//
// One call reads up to mqttSweepBatchMax entries. Charging the call rather than the reads bought
// 128 round trips for one unit, so the advertised limit — and with it the shutdown latency the
// pass promises — meant nothing under a large inventory. The charge reports whether the NEXT read
// may happen, and a refusal ends the batch where it stands: the outcome reports only the ordinals
// actually examined, so the cursor advances over exactly what was looked at.
//
// nil means uncharged, which is every caller that is not the recovery pass.
//
// census is set for a root whose generation's closure DISCARDS (F-105 7d): a RESOLVED exchange
// duty is then returned too, flagged Census and kept OUT of LowestUnresolved — whether its
// obligation still owes discard work is the visit's classification to decide, at its own charge,
// and an entry the visit never classifies is never advanced past (visitRecoveryRoot).
func (jsa *mqttJSA) sweepOneBatchCharged(r *mqttRecoveryRoot,
	charge func() bool, census bool) ([]mqttSweptDuty, mqttSweepOutcome, error) {

	if r == nil {
		return nil, mqttSweepOutcome{}, fmt.Errorf("sweeping requires the root it walks")
	}
	if mqttSweepFinished(r) {
		return nil, mqttSweepOutcome{}, nil
	}
	idHash := getHash(r.ID)
	// THE FLOOR (F-112): every position below it is RETIRED, its duty reclaimed or carried to a
	// representative inside [Lo, Hi], so the sweep accounts the whole retired prefix examined and
	// resolved in CONSTANT time, without a read or a charge, and its bounded batch starts at
	// max(Resume, Lo). A sealed generation's floor is frozen, so this is read once per batch.
	floor := uint64(1)
	if led, _, lerr := jsa.loadGenLedger(r.ID, r.Epoch, r.Gen); lerr != nil {
		return nil, mqttSweepOutcome{}, fmt.Errorf("sweeping %q/%q: the ledger could not be read: %w",
			r.ID, r.Gen, lerr)
	} else if led != nil {
		floor = led.Lo
	}
	start := r.Resume
	out := mqttSweepOutcome{}
	if floor > start {
		skip := min(floor, r.Bound+1) - start
		out.Examined += skip
		start += skip
	}
	if start > r.Bound {
		return nil, out, nil
	}
	last := r.Bound
	if span := last - start + 1; span > mqttSweepBatchMax {
		last = start + mqttSweepBatchMax - 1
	}

	swept := make([]mqttSweptDuty, 0, last-start+1)
	for ord := start; ord <= last; ord++ {
		if charge != nil && !charge() {
			// THE BATCH ENDS WHERE THE BUDGET DID. Everything below this ordinal was examined and
			// is reported; this one and everything after it were not read at all, so they are
			// outside Examined and the next pass reads them.
			break
		}
		out.Examined++
		subj := mqttDutySubject(idHash, r.Gen, ord)
		smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
		switch {
		case lerr != nil && isErrorOtherThan(lerr, JSNoMessageFoundErr):
			// AN UNRESOLVED READ IS NOT AN ANSWER. The sweep cannot say whether this ordinal is
			// outstanding, so it says outstanding — and, unlike the two cases below, it stops
			// rather than reporting a partial batch as a whole one, because the ordinals after
			// it were never looked at.
			return nil, mqttSweepOutcome{}, fmt.Errorf("sweeping %q/%q: duty %d could not be "+
				"read: %w", r.ID, r.Gen, ord, lerr)
		case lerr != nil || smsg == nil:
			swept = append(swept, mqttSweptDuty{Ord: ord, Why: "the entry is missing"})
		default:
			d, derr := mqttDecodeDutyEntry(smsg.Data, r.ID, r.Epoch, r.Gen, ord)
			if derr != nil {
				swept = append(swept, mqttSweptDuty{
					Ord: ord, Why: fmt.Sprintf("the entry cannot be interpreted: %v", derr),
				})
				break
			}
			if mqttDutyResolved(d.State) {
				if census && d.Kind == mqttDutyExchange {
					swept = append(swept, mqttSweptDuty{Ord: ord, Entry: d, Census: true})
				}
				continue
			}
			swept = append(swept, mqttSweptDuty{
				Ord: ord, Entry: d,
				Why: fmt.Sprintf("the duty stands at state %d", d.State),
			})
		}
		if out.LowestUnresolved == 0 {
			out.LowestUnresolved = ord
		}
	}
	return swept, out, nil
}

// persistSweep writes a root's advanced cursor back, at the sequence it was read at.
//
// CAS, so a stale worker's write is refused rather than overwriting a newer sweep's state. Two
// brokers sweeping one generation is expected — claims reduce duplicate work and cannot exclude
// it — so the loser here has simply been overtaken and re-reads.
//
// NOT AN AUTHORITY TRANSITION, deliberately. A recovery pass may run on a broker that holds no
// authority for this client, which is the whole point of "any broker serving the account may
// resume a duty"; requiring one would make recovery depend on the very entitlement it exists to
// survive. The cursor authorizes nothing, so it needs no arbitration — and because the root and
// shard classes are not obligation classes, a caller cannot smuggle one into an authority batch
// either.
func (jsa *mqttJSA) persistSweep(r *mqttRecoveryRoot, expect uint64) (uint64, error) {
	if r == nil || expect == 0 {
		return 0, fmt.Errorf("persisting a sweep requires the root and the sequence it was read at")
	}
	if verr := mqttValidateRecoveryRoot(r, r.Shard, r.Ord); verr != nil {
		return 0, fmt.Errorf("refusing to persist an unsupported recovery root: %w", verr)
	}
	body, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}
	msg, hdr := mqttFrameBatchWrite(expect, body, _EMPTY_, 0, false)
	resp, serr := jsa.storeMsg(mqttRootSubject(r.Shard, r.Ord), hdr, msg)
	if serr != nil {
		return 0, serr
	}
	return resp.Sequence, nil
}

// loadRecoveryRoot reads one root with its sequence, returning nil when none stands.
func (jsa *mqttJSA) loadRecoveryRoot(shard, ord uint64) (*mqttRecoveryRoot, uint64, error) {
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, mqttRootSubject(shard, ord))
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, fmt.Errorf("reading recovery root %d/%d: %w", shard, ord, err)
		}
		return nil, 0, nil
	}
	if smsg == nil {
		return nil, 0, nil
	}
	r, derr := mqttDecodeRecoveryRoot(smsg.Data, shard, ord)
	if derr != nil {
		// UNREADABLE IS NOT ABSENT. A root this broker cannot interpret may be the one enrolling
		// a generation with outstanding duties, and reading it as absent would drop that
		// generation out of every future sweep.
		return nil, 0, fmt.Errorf("recovery root %d/%d cannot be interpreted: %w", shard, ord, derr)
	}
	return r, smsg.Sequence, nil
}
