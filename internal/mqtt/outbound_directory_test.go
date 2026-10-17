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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"testing"
)

// testF105Root builds a recovery root for the fixture client, in the shard it hashes to.
func testF105Root(start, resume, bound, unresolved uint64) *mqttRecoveryRoot {
	shard := mqttDirShardFor(getHash(testF105ID))
	return &mqttRecoveryRoot{
		Shard: shard, Ord: 1, ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen,
		SweepStart: start, Resume: resume, Bound: bound, Unresolved: unresolved,
	}
}

func testF105SealedLedger(hi uint64) *mqttGenLedger {
	return &mqttGenLedger{
		Lo: 1, Del: 1,
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen,
		State: mqttGenLedgerSealed, Hi: hi, SealedHi: hi,
	}
}

// TestF105SweepCursorClearsAnOrdinalThatLaterCompletes is consult 5's counterexample, and the
// defect the four-field cursor exists to fix.
//
// The first design kept ONE accumulated minimum: "the lowest ordinal I could not finish", updated
// by taking a minimum. Ordinal 7 is unresolved in the first sweep, so the mark is 7; it completes
// during the second sweep; the minimum with the standing 7 is still 7. The mark can only ever
// fall, so the generation never retires and its resources are never reclaimed — a failure that
// looks like conservatism from the outside, which is what makes it dangerous.
//
// With the accumulator kept apart from the restart point and RESET when the next sweep begins, a
// duty resolved this time round simply never enters the accumulator.
func TestF105SweepCursorClearsAnOrdinalThatLaterCompletes(t *testing.T) {
	// Sweep 1 over a generation with ten duties: everything examined, ordinal 7 unresolved.
	r := testF105Root(1, 1, 10, 0)
	r, err := mqttAdvanceSweep(r, mqttSweepOutcome{Examined: 10, LowestUnresolved: 7})
	if err != nil {
		t.Fatalf("advancing the first sweep: %v", err)
	}
	if !mqttSweepFinished(r) {
		t.Fatalf("a sweep that examined every ordinal through its bound is not finished: %+v", r)
	}
	if r.Unresolved != 7 {
		t.Fatalf("the sweep did not record ordinal 7 unresolved: %+v", r)
	}

	// Sweep 2 restarts AT 7, not at 1 and not at the bound.
	r, err = mqttRollSweep(r, 10)
	if err != nil {
		t.Fatalf("rolling into the second sweep: %v", err)
	}
	if r.SweepStart != 7 || r.Resume != 7 {
		t.Fatalf("the second sweep did not restart at the unresolved ordinal: %+v", r)
	}
	if r.Unresolved != 0 {
		t.Fatalf("the accumulator was not reset for the new sweep: %+v", r)
	}

	// THE POINT: 7 completes this time, so nothing enters the accumulator.
	r, err = mqttAdvanceSweep(r, mqttSweepOutcome{Examined: 4})
	if err != nil {
		t.Fatalf("advancing the second sweep: %v", err)
	}
	if r.Unresolved != 0 {
		t.Fatalf("a resolved ordinal entered the accumulator: %+v", r)
	}
	if !mqttRootRetires(r, testF105SealedLedger(10)) {
		t.Fatalf("a clean sweep through a sealed bound did not retire: %+v", r)
	}

	// AND THE THIRD SWEEP, had the generation not retired, would start PAST the old bound
	// rather than re-walking ordinals already established terminal. That branch is what a
	// single accumulated minimum could never express.
	rolled, rerr := mqttRollSweep(r, 12)
	if rerr != nil {
		t.Fatalf("rolling a clean sweep: %v", rerr)
	}
	if rolled.SweepStart != 11 {
		t.Fatalf("a clean sweep restarted at %d, want one past its old bound", rolled.SweepStart)
	}
}

// TestF105SweepCompletesOnePastItsBound pins the off-by-one.
//
// Resume names THE NEXT ORDINAL TO EXAMINE, so reaching Bound still leaves that entry
// unprocessed. The completed position is Bound+1, and treating Bound as completion would retire
// a generation with its last duty never looked at.
func TestF105SweepCompletesOnePastItsBound(t *testing.T) {
	atBound := testF105Root(1, 5, 5, 0)
	if mqttSweepFinished(atBound) {
		t.Fatalf("a sweep resuming AT its bound was called finished, leaving ordinal 5 unexamined")
	}
	if mqttRootRetires(atBound, testF105SealedLedger(5)) {
		t.Fatalf("a generation retired with its last duty unexamined")
	}
	past := testF105Root(1, 6, 5, 0)
	if !mqttSweepFinished(past) {
		t.Fatalf("a sweep one past its bound was not finished: %+v", past)
	}

	// AND ADVANCING NEVER OVERSHOOTS. A batch reporting more examined than remain clamps at the
	// completed position rather than running past it, which would put the cursor in a state its
	// own validator refuses.
	r, err := mqttAdvanceSweep(testF105Root(1, 1, 3, 0), mqttSweepOutcome{Examined: 128})
	if err != nil {
		t.Fatalf("advancing past the bound: %v", err)
	}
	if r.Resume != 4 {
		t.Fatalf("a batch overshot the bound: resume %d, want 4", r.Resume)
	}
}

// TestF105RetirementNeedsTheSealedBoundItActuallySwept is the second cut consult 5 found, and it
// also removes a prohibition revision 2 stated wrongly.
//
// THE UNSAFE CASE: a sweep captures an OPEN high-water of 100, an admission creates 101, closure
// seals at 101, and the sweep finishes through 100 with nothing unresolved. It now observes
// SEALED — and retiring on that would remove a root that never visited 101.
//
// THE CASE REVISION 2 WRONGLY FORBADE: capture an OPEN 100, closure seals at 100 with no further
// allocation, finish through 100. Retirement is CORRECT here, and a categorical "an open capture
// can never retire" would have refused it forever.
//
// What separates them is the EQUALITY, not the state at capture — which is why no "captured
// while sealed" flag exists.
func TestF105RetirementNeedsTheSealedBoundItActuallySwept(t *testing.T) {
	// Swept through 100; the generation sealed at 101. The entry at 101 was never examined.
	short := testF105Root(1, 101, 100, 0)
	if !mqttSweepFinished(short) {
		t.Fatalf("the fixture's sweep is not finished: %+v", short)
	}
	if mqttRootRetires(short, testF105SealedLedger(101)) {
		t.Fatalf("a generation retired on a bound below its sealed high-water, so ordinal 101 " +
			"was never visited")
	}
	// CAUGHT UP, not completed — and it has a name of its own precisely so the two are not
	// confused.
	if !mqttRootCaughtUp(short, testF105SealedLedger(101)) {
		t.Fatalf("a finished sweep that cannot retire was not reported caught up")
	}

	// The same sweep, against a generation that sealed at exactly the captured bound.
	if !mqttRootRetires(short, testF105SealedLedger(100)) {
		t.Fatalf("a clean sweep whose captured bound equals the sealed high-water did not retire")
	}

	// AN OPEN GENERATION NEVER RETIRES, however clean the sweep: nothing has fixed the shape of
	// its inventory, so there is no bound for a coverage proof to be about.
	open := &mqttGenLedger{
		Lo: 1, Del: 1,
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen,
		State: mqttGenLedgerOpen, Hi: 100,
	}
	if mqttRootRetires(short, open) {
		t.Fatalf("an OPEN generation retired")
	}
	if !mqttRootCaughtUp(short, open) {
		t.Fatalf("a finished sweep over an open generation was not reported caught up")
	}

	// AND AN UNRESOLVED ORDINAL BLOCKS RETIREMENT even with the bounds agreeing.
	dirty := testF105Root(1, 101, 100, 42)
	if mqttRootRetires(dirty, testF105SealedLedger(100)) {
		t.Fatalf("a generation retired with ordinal 42 unresolved")
	}
}

// TestF105SweepRollRefusesABoundThatWentBackwards pins the one thing that would make a sweep
// report itself finished without examining anything.
func TestF105SweepRollRefusesABoundThatWentBackwards(t *testing.T) {
	finished := testF105Root(1, 11, 10, 0)
	if _, err := mqttRollSweep(finished, 5); err == nil {
		t.Fatalf("the next sweep started at 11 against a bound of 5, which it would report " +
			"finished without examining anything")
	}
	// AND AN UNFINISHED SWEEP DOES NOT ROLL. Rolling mid-sweep would reset the accumulator and
	// lose every unresolved ordinal the sweep had found so far.
	if _, err := mqttRollSweep(testF105Root(1, 5, 10, 3), 10); err == nil {
		t.Fatalf("a sweep rolled before it finished, discarding its accumulator")
	}
}

// TestF105RecoveryRecordsRefuseUnsupportedBodies pins the two record shapes.
func TestF105RecoveryRecordsRefuseUnsupportedBodies(t *testing.T) {
	shard := mqttDirShardFor(getHash(testF105ID))

	t.Run("shard", func(t *testing.T) {
		for name, tc := range map[string]struct {
			s  *mqttDirShard
			ok bool
		}{
			"an empty shard": {&mqttDirShard{
				Shard: shard, Hi: 0, Next: 1, Cycle: 1, Horizon: 0}, true},
			"a populated shard": {&mqttDirShard{
				Shard: shard, Hi: 5, Next: 3, Cycle: 1, Horizon: 5}, true},
			"caught up": {&mqttDirShard{
				Shard: shard, Hi: 5, Next: 6, Cycle: 2, Horizon: 5}, true},
			// The position is bounded by the CYCLE'S HORIZON, not by the high-water: a cycle
			// walks the roots that existed when it began, and an allocation since then belongs
			// to the next one.
			"past its cycle horizon": {&mqttDirShard{
				Shard: shard, Hi: 9, Next: 7, Cycle: 2, Horizon: 5}, false},
			"past the high-water": {&mqttDirShard{
				Shard: shard, Hi: 5, Next: 7, Cycle: 1, Horizon: 5}, false},
			"resuming at zero": {&mqttDirShard{
				Shard: shard, Hi: 5, Next: 0, Cycle: 1, Horizon: 5}, false},
			"cycle zero": {&mqttDirShard{
				Shard: shard, Hi: 5, Next: 1, Cycle: 0, Horizon: 5}, false},
			// A HORIZON IS A HIGH-WATER THIS SHARD ONCE HAD, so it can never exceed the one it
			// has now: Hi never decreases.
			"horizon above the high-water": {&mqttDirShard{
				Shard: shard, Hi: 5, Next: 1, Cycle: 1, Horizon: 6}, false},
			"an unaddressable shard": {&mqttDirShard{
				Shard: mqttDirShards + 1, Hi: 0, Next: 1, Cycle: 1, Horizon: 0}, false},
		} {
			err := mqttValidateDirShard(tc.s, tc.s.Shard)
			if tc.ok && err != nil {
				t.Fatalf("%s was refused: %v", name, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("%s was accepted", name)
			}
		}
	})

	t.Run("root", func(t *testing.T) {
		for name, tc := range map[string]struct {
			r  *mqttRecoveryRoot
			ok bool
		}{
			"a fresh sweep":    {testF105Root(1, 1, 10, 0), true},
			"a finished sweep": {testF105Root(1, 11, 10, 0), true},
			"one unresolved":   {testF105Root(1, 11, 10, 4), true},

			"resuming before its sweep began":         {testF105Root(5, 3, 10, 0), false},
			"resuming past the completed position":    {testF105Root(1, 12, 10, 0), false},
			"an unresolved ordinal it never examined": {testF105Root(1, 5, 10, 7), false},
			"an unresolved ordinal below its sweep":   {testF105Root(5, 11, 10, 2), false},
			"a sweep starting at zero":                {testF105Root(0, 1, 10, 0), false},
		} {
			err := mqttValidateRecoveryRoot(tc.r, tc.r.Shard, tc.r.Ord)
			if tc.ok && err != nil {
				t.Fatalf("%s was refused: %v", name, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("%s was accepted", name)
			}
		}

		// A ROOT FILED IN THE WRONG SHARD is unreachable by every sweep that looks for that
		// client's work where its hash says it should be. Refused rather than tolerated,
		// because the cost is silent: nothing ever reports the work as missing.
		misfiled := testF105Root(1, 1, 10, 0)
		misfiled.Shard = (misfiled.Shard + 1) % mqttDirShards
		if err := mqttValidateRecoveryRoot(misfiled, misfiled.Shard, misfiled.Ord); err == nil {
			t.Fatalf("a root filed in the wrong shard was accepted")
		}
	})
}

// TestF105DirectoryShardingIsStableAndAddressable pins the two properties a sweep depends on.
func TestF105DirectoryShardingIsStableAndAddressable(t *testing.T) {
	for _, id := range []string{"a", "client-one", testF105ID, "", "a much longer client id"} {
		h := getHash(id)
		first := mqttDirShardFor(h)
		if first >= mqttDirShards {
			t.Fatalf("client %q hashes to shard %d, outside the %d addressed", id, first, mqttDirShards)
		}
		// STABLE, because a client whose shard moved would leave its enrolled roots where no
		// sweep looks for them.
		if again := mqttDirShardFor(h); again != first {
			t.Fatalf("client %q hashed to %d then %d", id, first, again)
		}
	}
}

// testF105PlantRoot enrols a generation for recovery, directly, and returns its sequence.
func testF105PlantRoot(t *testing.T, e *testF105Env, r *mqttRecoveryRoot) uint64 {
	t.Helper()
	body, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshalling the root: %v", err)
	}
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	resp, serr := e.asm.jsa.storeMsg(mqttRootSubject(r.Shard, r.Ord), hdr, msg)
	if serr != nil {
		t.Fatalf("planting the root: %v", serr)
	}
	return resp.Sequence
}

// testF105EnvRoot builds a root for the env's own client, in the shard it hashes to.
func testF105EnvRoot(e *testF105Env, start, resume, bound, unresolved uint64) *mqttRecoveryRoot {
	return &mqttRecoveryRoot{
		Shard: mqttDirShardFor(getHash(e.id)), Ord: 1,
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
		SweepStart: start, Resume: resume, Bound: bound, Unresolved: unresolved,
	}
}

// TestF105SweepCountsMissingAndMalformedEntriesAsOutstanding is D5's rule, which is the one a
// sweep is most tempted to get wrong.
//
// "Missing or malformed entries are INCONSISTENCIES, not completed work." A sweep that skipped
// what it could not read would advance past it and, with nothing else ever revisiting that
// ordinal, would eventually retire a generation whose duty nobody ever established anything
// about. Absence is not evidence.
//
// The other half is equally deliberate: `Resume` MAY pass such an entry while the accumulator
// retains it. Blocking on it instead would let one inconsistency starve every later duty in the
// generation, which is a denial of service the design never asked for.
func TestF105SweepCountsMissingAndMalformedEntriesAsOutstanding(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-sweep-gaps")
	defer done()
	e.bootstrap(t, "txn-1")

	// Four duties: 1 terminal, 2 missing, 3 malformed, 4 still pending.
	if err := testF105SubmitInventory(t, e, "txn-alloc",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 4)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 2, mqttDutyExchange, "xch-b", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 3, mqttDutyExchange, "xch-c", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 4, mqttDutyExchange, "xch-d", mqttDutyPending)),
	); err != nil {
		t.Fatalf("allocating four duties: %v", err)
	}
	// 1 REACHES terminal in the store rather than being allocated there: an allocation may never
	// start terminal, and what this test is about is what the SWEEP does with the record.
	testF105PlantDuty(t, e, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyTerminal))
	// Remove 2 outright and corrupt 3, which is what a sweep must not read as done.
	gone, gerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttDutySubject(getHash(e.id), testF105Gen, 2))
	if gerr != nil || gone == nil {
		t.Fatalf("reading duty 2 before removing it: %v", gerr)
	}
	if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, gone.Sequence, true); err != nil {
		t.Fatalf("removing duty 2: %v", err)
	}
	bad, berr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttDutySubject(getHash(e.id), testF105Gen, 3))
	if berr != nil || bad == nil {
		t.Fatalf("reading duty 3 before corrupting it: %v", berr)
	}
	msg, hdr := mqttFrameBatchWrite(bad.Sequence, []byte(`{"not":"a duty"}`), _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttDutySubject(getHash(e.id), testF105Gen, 3), hdr, msg); err != nil {
		t.Fatalf("corrupting duty 3: %v", err)
	}

	root := testF105EnvRoot(e, 1, 1, 4, 0)
	swept, out, err := e.asm.jsa.sweepOneBatch(root)
	if err != nil {
		t.Fatalf("sweeping: %v", err)
	}
	if out.Examined != 4 {
		t.Fatalf("the batch examined %d ordinals, want 4", out.Examined)
	}
	// 1 is terminal, so the lowest outstanding is the MISSING one.
	if out.LowestUnresolved != 2 {
		t.Fatalf("the lowest unresolved ordinal is %d, want the missing entry at 2", out.LowestUnresolved)
	}
	if len(swept) != 3 {
		t.Fatalf("the batch reported %d outstanding duties, want 2, 3 and 4: %+v", len(swept), swept)
	}
	for _, s := range swept {
		if s.Ord == 1 {
			t.Fatalf("a terminal duty was reported outstanding")
		}
		if s.Why == "" {
			t.Fatalf("an outstanding duty was reported with no reason: %+v", s)
		}
	}
	// The missing and malformed ones carry NO entry, which is what stops a caller acting on a
	// record it never read.
	for _, s := range swept {
		if (s.Ord == 2 || s.Ord == 3) && s.Entry != nil {
			t.Fatalf("duty %d was reported with an entry it does not have", s.Ord)
		}
		if s.Ord == 4 && s.Entry == nil {
			t.Fatalf("the readable pending duty was reported with no entry")
		}
	}

	// AND THE CURSOR PASSES THEM while retaining the mark, so one inconsistency does not starve
	// every later duty.
	next, aerr := mqttAdvanceSweep(root, out)
	if aerr != nil {
		t.Fatalf("advancing: %v", aerr)
	}
	if !mqttSweepFinished(next) {
		t.Fatalf("the sweep did not reach its completed position: %+v", next)
	}
	if next.Unresolved != 2 {
		t.Fatalf("the accumulator lost the missing entry: %+v", next)
	}
}

// TestF105SweepBatchIsBounded pins the 128.
func TestF105SweepBatchIsBounded(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-sweep-bound")
	defer done()
	e.bootstrap(t, "txn-1")

	// A generation claiming far more duties than one batch may read. The entries are not
	// planted: every ordinal then reads as MISSING, which is exactly the outstanding
	// classification this test wants and needs no ceremony to produce.
	root := testF105EnvRoot(e, 1, 1, 1000, 0)
	swept, out, err := e.asm.jsa.sweepOneBatch(root)
	if err != nil {
		t.Fatalf("sweeping: %v", err)
	}
	if out.Examined != mqttSweepBatchMax {
		t.Fatalf("one batch examined %d ordinals, want %d", out.Examined, mqttSweepBatchMax)
	}
	if uint64(len(swept)) != mqttSweepBatchMax {
		t.Fatalf("one batch reported %d duties, want %d", len(swept), mqttSweepBatchMax)
	}
	next, aerr := mqttAdvanceSweep(root, out)
	if aerr != nil {
		t.Fatalf("advancing: %v", aerr)
	}
	if next.Resume != mqttSweepBatchMax+1 {
		t.Fatalf("the cursor resumed at %d after one batch, want %d",
			next.Resume, mqttSweepBatchMax+1)
	}
	// AND IT YIELDS RATHER THAN FINISHING. A bounded pass that reported itself complete would
	// retire a generation on 128 of its 1000 duties.
	if mqttSweepFinished(next) {
		t.Fatalf("a bounded batch reported the whole sweep finished")
	}
}

// TestF105SweepCursorIsPersistedUnderCAS pins the one thing that keeps two brokers from
// overwriting each other's progress.
//
// Claims reduce duplicate work and CANNOT exclude it — a paused claimant resuming after its
// lease expired is always possible — so two brokers sweeping one generation is expected rather
// than exceptional. What must not happen is a stale worker writing its older cursor over a newer
// one, which would re-walk ordinals already established terminal or, worse, move the accumulator
// backwards.
func TestF105SweepCursorIsPersistedUnderCAS(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-sweep-cas")
	defer done()
	e.bootstrap(t, "txn-1")

	root := testF105EnvRoot(e, 1, 1, 10, 0)
	seq := testF105PlantRoot(t, e, root)

	// Broker A advances and persists.
	a, err := mqttAdvanceSweep(root, mqttSweepOutcome{Examined: 5, LowestUnresolved: 3})
	if err != nil {
		t.Fatalf("advancing A: %v", err)
	}
	newSeq, perr := e.asm.jsa.persistSweep(a, seq)
	if perr != nil {
		t.Fatalf("persisting A: %v", perr)
	}

	// Broker B advanced from the SAME read and persists at the sequence it saw. It must lose.
	b, berr := mqttAdvanceSweep(root, mqttSweepOutcome{Examined: 9})
	if berr != nil {
		t.Fatalf("advancing B: %v", berr)
	}
	if _, err := e.asm.jsa.persistSweep(b, seq); err == nil {
		t.Fatalf("a stale sweep overwrote a newer cursor, losing its accumulator")
	}

	// A's cursor stands, accumulator intact.
	got, gotSeq, lerr := e.asm.jsa.loadRecoveryRoot(root.Shard, root.Ord)
	if lerr != nil || got == nil {
		t.Fatalf("reading the root back: %v", lerr)
	}
	if gotSeq != newSeq || got.Resume != 6 || got.Unresolved != 3 {
		t.Fatalf("the standing cursor is not the winner's: %+v", got)
	}
}

// TestF105RecoveryRecordsCannotRideInAnAuthorityBatch pins a separation that is currently true
// by construction, which is exactly why it needs a test.
//
// The shard header and the recovery roots are NOT obligation classes and NOT arbitration
// records. They are recovery bookkeeping, CAS'd directly, because a sweep may run on a broker
// that holds no authority for the client whose work it is resuming — requiring one would make
// recovery depend on the very entitlement it exists to survive.
//
// The other side of that is what this asserts: a caller must not be able to smuggle a cursor
// update into an authority batch. Doing so would let a transition advance a sweep past ordinals
// nobody examined, which loses work silently — the cursor authorizes nothing, so nothing would
// refuse it on authorization grounds.
func TestF105RecoveryRecordsCannotRideInAnAuthorityBatch(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-root-not-a-member")
	defer done()
	e.bootstrap(t, "txn-1")

	root := testF105EnvRoot(e, 1, 1, 10, 0)
	body, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("marshalling the root: %v", err)
	}
	shardBody, serr := json.Marshal(&mqttDirShard{Shard: root.Shard, Hi: 1, Next: 1})
	if serr != nil {
		t.Fatalf("marshalling the shard: %v", serr)
	}

	for name, m := range map[string]mqttTransitionMember{
		"a recovery root": {subj: mqttRootSubject(root.Shard, root.Ord), expect: 0, body: body},
		"a shard header":  {subj: mqttShardSubject(root.Shard), expect: 0, body: shardBody},
	} {
		if err := testF105SubmitInventory(t, e, "txn-smuggle-"+mqttNewInflightNonce(), m); err == nil {
			t.Fatalf("%s was carried inside an authority transition", name)
		}
	}

	// AND THE SAME RECORDS ARE WRITABLE DIRECTLY, which is the point of the separation rather
	// than an accident of it — without this the test would pass against a broker that simply
	// could not write them at all.
	if seq := testF105PlantRoot(t, e, root); seq == 0 {
		t.Fatalf("a recovery root could not be written outside a transition")
	}
}

// TestF105SweepNeverPassesANotYetActionableDuty closes the composition the other tests only
// cover transitively.
//
// mqttDutyResolved says NOT_YET_ACTIONABLE is unresolved, and the sweep reports unresolved
// entries — but nothing pinned the two TOGETHER, and this is the case consult 5 named as the
// reason the state exists: "a predeclared entry must remain unresolved while its conditional
// future work is still possible; otherwise a cursor can pass a 'finished' entry that later
// becomes owed".
//
// A cleanup duty predeclared with its resource is exactly that entry. It is not open work — its
// resource still has dependents — and it is emphatically not finished. A sweep that advanced
// past it would retire a generation still owing the cleanup.
func TestF105SweepNeverPassesANotYetActionableDuty(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-sweep-notyet")
	defer done()
	e.bootstrap(t, "txn-1")

	if err := testF105SubmitInventory(t, e, "txn-alloc",
		testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerOpen, 2)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyPending)),
		testF105DutyMember(t, e, 0, testF105Duty(e, 2, mqttDutyCleanup, "inc-1",
			mqttDutyNotYetActionable)),
	); err != nil {
		t.Fatalf("allocating the pair: %v", err)
	}
	// As above: 1 reaches terminal in the store, because an allocation may never start there.
	testF105PlantDuty(t, e, testF105Duty(e, 1, mqttDutyExchange, "xch-a", mqttDutyTerminal))

	root := testF105EnvRoot(e, 1, 1, 2, 0)
	swept, out, err := e.asm.jsa.sweepOneBatch(root)
	if err != nil {
		t.Fatalf("sweeping: %v", err)
	}
	if out.LowestUnresolved != 2 {
		t.Fatalf("the not-yet-actionable cleanup was not reported outstanding: %+v", out)
	}
	if len(swept) != 1 || swept[0].Ord != 2 || swept[0].Entry == nil {
		t.Fatalf("the sweep did not hand back the cleanup duty: %+v", swept)
	}

	// AND THE CURSOR KEEPS COMING BACK FOR IT. After the sweep finishes, the next one restarts
	// AT the cleanup rather than past it — which is the whole reason the state is not terminal.
	next, aerr := mqttAdvanceSweep(root, out)
	if aerr != nil {
		t.Fatalf("advancing: %v", aerr)
	}
	rolled, rerr := mqttRollSweep(next, 2)
	if rerr != nil {
		t.Fatalf("rolling: %v", rerr)
	}
	if rolled.SweepStart != 2 {
		t.Fatalf("the next sweep starts at %d, past the cleanup it still owes", rolled.SweepStart)
	}
	// AND THE GENERATION DOES NOT RETIRE, even sealed at exactly the swept bound.
	if mqttRootRetires(next, testF105SealedLedger(2)) {
		t.Fatalf("a generation retired while a predeclared cleanup was still owed")
	}
}

// testF105PlantRootAt rewrites a root at its standing sequence.
func testF105PlantRootAt(t *testing.T, e *testF105Env, r *mqttRecoveryRoot) {
	t.Helper()
	_, seq, err := e.asm.jsa.loadRecoveryRoot(r.Shard, r.Ord)
	if err != nil {
		t.Fatalf("reading the root before rewriting it: %v", err)
	}
	if _, perr := e.asm.jsa.persistSweep(r, seq); perr != nil {
		t.Fatalf("rewriting the root: %v", perr)
	}
}

// testF105PlantShard writes a shard header covering ordinals up to hi.
func testF105PlantShard(t *testing.T, e *testF105Env, shard, hi uint64) {
	t.Helper()
	// A HEADER CARRIES ITS CYCLE AND ITS HORIZON, and a planted one is no exception: the record
	// is refused on read without them. The horizon is the high-water this cycle began at, which
	// for a freshly planted header is the high-water it has.
	body, err := json.Marshal(&mqttDirShard{
		Shard: shard, Hi: hi, Next: 1, Cycle: 1, Horizon: hi,
	})
	if err != nil {
		t.Fatalf("marshalling the shard header: %v", err)
	}
	cur, cerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttShardSubject(shard))
	var expect uint64
	if cerr == nil && cur != nil {
		expect = cur.Sequence
	}
	msg, hdr := mqttFrameBatchWrite(expect, body, _EMPTY_, 0, false)
	if _, serr := e.asm.jsa.storeMsg(mqttShardSubject(shard), hdr, msg); serr != nil {
		t.Fatalf("planting the shard header: %v", serr)
	}
}
