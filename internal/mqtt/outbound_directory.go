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

// mqttRootWalkFilter enumerates one shard's roots, and only those.
func mqttRootWalkFilter(shard uint64) string {
	return mqttF105SubjectPrefix() + mqttRootClassTk + "." + strconv.FormatUint(shard, 10) + ".*"
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
}

var mqttShardMembers = map[string]struct{}{"shard": {}, "hi": {}, "next": {}}

var mqttShardRequired = map[string]struct{}{"shard": {}, "hi": {}, "next": {}}

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
	return nil
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
//  4. the authoritative ledger is SEALED with SealedHi == Bound.
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
	return mqttSweepFinished(r) && r.Unresolved == 0 && led.SealedHi == r.Bound
}

// mqttRootCaughtUp reports that a sweep has examined everything allocated so far without
// resolving the generation.
//
// A DIFFERENT STATE FROM RETIRED, and naming it is the point: an OPEN generation whose sweep has
// finished is caught up, not completed. Its root stays discoverable and is revisited for growth
// or for sealing, and treating the two alike would remove a root that is still accruing work.
func mqttRootCaughtUp(r *mqttRecoveryRoot, led *mqttGenLedger) bool {
	return r != nil && led != nil && mqttSweepFinished(r) && !mqttRootRetires(r, led)
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
	// Why records what made this entry unresolved, for the operator who has to act on it. Empty
	// for a resolved one.
	Why string
}

// sweepOneBatch reads up to mqttSweepBatchMax entries from a root's resume position and reports
// what it established.
//
// It RESOLVES NOTHING. Reading an entry cannot complete a duty — only the duty's own transition
// can — so this establishes which ordinals are still outstanding and hands them back. The caller
// is what acts on them, under whatever concurrency it chooses, and this stays a pure traversal
// that a test can pin without a work executor in the way.
func (jsa *mqttJSA) sweepOneBatch(r *mqttRecoveryRoot) ([]mqttSweptDuty, mqttSweepOutcome, error) {
	if r == nil {
		return nil, mqttSweepOutcome{}, fmt.Errorf("sweeping requires the root it walks")
	}
	if mqttSweepFinished(r) {
		return nil, mqttSweepOutcome{}, nil
	}
	idHash := getHash(r.ID)
	last := r.Bound
	if span := last - r.Resume + 1; span > mqttSweepBatchMax {
		last = r.Resume + mqttSweepBatchMax - 1
	}

	swept := make([]mqttSweptDuty, 0, last-r.Resume+1)
	out := mqttSweepOutcome{Examined: last - r.Resume + 1}
	for ord := r.Resume; ord <= last; ord++ {
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
