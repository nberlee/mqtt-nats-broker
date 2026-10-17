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

import "testing"

// F-116: a PUBREL attempt's PUBCOMP unit is taken before its write and given back only when its
// frame was provably not emitted. The client answers exactly the PUBRELs it received, so a unit
// kept for one it never received holds the identifier for the connection's life.
//
// Every test here drives a process-wide seam, so none is parallel.

// testF116Hold is pi's send hold as the tests read it: the owed count, the open token and the
// open attempt's write-entry fact. present is false when there is no hold at all.
type testF116Hold struct {
	present bool
	owed    int
	token   string
	entered bool
}

func testF116ReadHold(e *testInflightEnv, pi uint16) testF116Hold {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	h := e.sess.sendsInFlight[pi]
	if h == nil {
		return testF116Hold{}
	}
	return testF116Hold{present: true, owed: h.pubCompOwed, token: h.token, entered: h.relEntered}
}

// testF116Owed is pi's owed count, zero with no hold.
func testF116Owed(e *testInflightEnv, pi uint16) int {
	return testF116ReadHold(e, pi).owed
}

// testF116Written delivers seed's canonical PUBREL as its n-th delivery and requires it written
// once: the copy whose PUBCOMP a later step owes or answers.
func testF116Written(t *testing.T, e *testInflightEnv, seed testRealPubRelSeed, n int) {
	t.Helper()
	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelCorrelated(t, e, seed.pi, seed.reply(n), seed.nonce, testRealPubRelSrcDur, seed.sseq)
	})
	if writes != 1 {
		t.Fatalf("delivery %d of the PUBREL entered its write %d times, want once", n, writes)
	}
	testActionDrained(t, e, seed.pi)
}

// testF116Deliver delivers seed's canonical PUBREL as its n-th delivery.
func testF116Deliver(t *testing.T, e *testInflightEnv, seed testRealPubRelSeed, n int) {
	t.Helper()
	testDeliverPubRelCorrelated(t, e, seed.pi, seed.reply(n), seed.nonce, testRealPubRelSrcDur, seed.sseq)
}

// testF116SwapToHold makes the resident PUBREL entry a Hold copy, which write entry refuses, and
// returns the original so the caller can put it back.
//
// Lock held on entry.
func testF116SwapToHoldLocked(e *testInflightEnv, pi uint16) *mqttPending {
	cur := e.sess.pendingPubRel[pi]
	h := *cur
	h.kind = mqttPendingHold
	e.sess.pendingMapLocked(mqttPendingPubRelMap)[pi] = &h
	return cur
}

// testF116IdentifierFree waits until nothing reserves pi: no entry in either map, no live hold.
func testF116IdentifierFree(t *testing.T, e *testInflightEnv, pi uint16, what string) {
	t.Helper()
	testCustodyEventually(t, what, func() bool {
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		_, pub := e.sess.pendingPublish[pi]
		_, rel := e.sess.pendingPubRel[pi]
		return !pub && !rel && !e.sess.sendReservedLocked(pi)
	})
}

// T1 (the original trigger, staged as the roadmap names it). The client's PUBCOMP for a written
// copy completes the exchange while a redelivered copy sits between its commit and its write
// entry; write entry then refuses, nothing is written, and the redelivered attempt's unit is
// given back, so the identifier is free once the exchange's cleanup is done — no second PUBCOMP,
// which the client will never send, is needed.
//
// Mutations M1 (never cancel), M3 (cancel unconditionally: the written copy's unit goes too, and
// its PUBCOMP then finds nothing to consume — caught by the owed reading before it).
func TestF116APubRelRefusedAfterThePubCompOwesNothing(t *testing.T) {
	e, tc := testExecCellEnv(t, "f116-t1")
	seed := testSeedRealPubRelReady(t, e)
	testF116Written(t, e, seed, 1)
	if got := testF116Owed(e, seed.pi); got != 1 {
		t.Fatalf("a written, unanswered PUBREL must owe one PUBCOMP, owes %d", got)
	}
	written := len(tc.written)

	var midOwed int
	restore := mqttSetTestSeam(&mqttTestAfterPubRelCommitV, func() {
		midOwed = testF116Owed(e, seed.pi)
		e.c.mqttProcessPubComp(seed.pi)
	})
	writes := testCountPubRelWrites(t, e, func() { testF116Deliver(t, e, seed, 2) })
	restore()

	if midOwed != 2 {
		t.Fatalf("with the redelivered attempt open the hold must owe 2 (one written, one open), owed %d", midOwed)
	}
	if writes != 0 || len(tc.written) != written {
		t.Fatalf("a PUBREL whose exchange the PUBCOMP completed must write nothing, entered %d, wire %x",
			writes, tc.written[written:])
	}
	if got := testF116Owed(e, seed.pi); got != 0 {
		t.Fatalf("the refused attempt's unit must be given back: owed %d after the callback", got)
	}
	testF116IdentifierFree(t, e, seed.pi, "the completed exchange's identifier is free")
}

// T1, the entry-swap form beside T12's control: a written PUBREL keeps its unit until its PUBCOMP,
// and one refused at write entry (its entry swapped to a Hold) gives its unit back.
//
// Mutations M1 (swapped arm), M3 (control arm).
func TestF116AnUnwrittenPubRelGivesItsUnitBackAndAWrittenOneKeepsIt(t *testing.T) {
	for _, swap := range []bool{false, true} {
		swap := swap
		t.Run(map[bool]string{false: "written", true: "refused at write entry"}[swap], func(t *testing.T) {
			e, _ := testExecCellEnv(t, "f116-t1b")
			seed := testSeedRealPubRelReady(t, e)
			var orig *mqttPending
			restore := mqttSetTestSeam(&mqttTestAfterPubRelCommitV, func() {
				if !swap {
					return
				}
				e.sess.mu.Lock()
				orig = testF116SwapToHoldLocked(e, seed.pi)
				e.sess.mu.Unlock()
			})
			writes := testCountPubRelWrites(t, e, func() { testF116Deliver(t, e, seed, 1) })
			restore()
			want, wantWrites := 1, 1
			if swap {
				want, wantWrites = 0, 0
			}
			if writes != wantWrites {
				t.Fatalf("entered the write %d times, want %d", writes, wantWrites)
			}
			if got := testF116Owed(e, seed.pi); got != want {
				t.Fatalf("owed %d after the callback, want %d", got, want)
			}
			if swap {
				e.sess.mu.Lock()
				e.sess.pendingMapLocked(mqttPendingPubRelMap)[seed.pi] = orig
				e.sess.mu.Unlock()
			}
		})
	}
}

// T4 (negative control). A torn PUBREL write may have put the whole frame on the wire, so the
// attempt keeps its unit: the client may answer it, and freeing the identifier would let it be
// re-minted while that PUBCOMP can still arrive.
//
// Mutations M2 (cancel on Failed), M3.
func TestF116ATornPubRelWriteKeepsItsUnit(t *testing.T) {
	e, tc := testExecCellEnv(t, "f116-t4")
	seed := testSeedRealPubRelReady(t, e)
	tc.failAt = len(tc.written) + 2
	testF116Deliver(t, e, seed, 1)
	if tc.writeErr == nil {
		t.Fatal("fixture: the PUBREL write did not tear")
	}
	if got := testF116Owed(e, seed.pi); got != 1 {
		t.Fatalf("a torn PUBREL write must keep its unit: owed %d", got)
	}
}

// T6. A panic after the writer was called — here in the torn writer's unlocked hook, after the
// bytes went out and wmu was released — keeps the unit: the flag turns before the writer, never
// after it returns.
//
// Mutation M4.
func TestF116APanicAfterTheWriterKeepsTheUnit(t *testing.T) {
	e, tc := testExecCellEnv(t, "f116-t6")
	seed := testSeedRealPubRelReady(t, e)
	tc.failAt = len(tc.written) + 2
	hook := func(*client) { panic("test: after the torn write") }
	mqttTestTornWriterUnlockedHookV.Store(&hook)
	t.Cleanup(func() { mqttTestTornWriterUnlockedHookV.Store(nil) })
	recovered := func() (r any) {
		defer func() { r = recover() }()
		testF116Deliver(t, e, seed, 1)
		return nil
	}()
	mqttTestTornWriterUnlockedHookV.Store(nil)
	if recovered == nil {
		t.Fatal("fixture: the hook did not panic")
	}
	if got := testF116Owed(e, seed.pi); got != 1 {
		t.Fatalf("a panic after the writer was called must keep the unit: owed %d", got)
	}
}

// T5b. Only the refused attempt's unit goes: a written copy still unanswered keeps its own, a
// repeated closer for the refused attempt changes nothing, and the real PUBCOMP then frees the
// identifier.
//
// Mutation M7 (two guarded decrements: owed 1 -> 0).
func TestF116ARefusedAttemptLeavesAnUnansweredCopysUnit(t *testing.T) {
	e, _ := testExecCellEnv(t, "f116-t5b")
	seed := testSeedRealPubRelReady(t, e)
	testF116Written(t, e, seed, 1)

	var orig *mqttPending
	var tok string
	restore := mqttSetTestSeam(&mqttTestAfterPubRelCommitV, func() {
		e.sess.mu.Lock()
		orig = testF116SwapToHoldLocked(e, seed.pi)
		tok = e.sess.sendsInFlight[seed.pi].token
		e.sess.mu.Unlock()
	})
	testF116Deliver(t, e, seed, 2)
	restore()
	if got := testF116Owed(e, seed.pi); got != 1 {
		t.Fatalf("the unanswered written copy's unit must stay: owed %d, want 1", got)
	}
	e.sess.endPubRelSend(e.c, seed.pi, tok, false)
	if got := testF116Owed(e, seed.pi); got != 1 {
		t.Fatalf("a repeated closer must change nothing: owed %d, want 1", got)
	}
	e.sess.mu.Lock()
	e.sess.pendingMapLocked(mqttPendingPubRelMap)[seed.pi] = orig
	e.sess.mu.Unlock()
	testActionDrained(t, e, seed.pi)
	e.c.mqttProcessPubComp(seed.pi)
	if got := testF116Owed(e, seed.pi); got != 0 {
		t.Fatalf("the real PUBCOMP must answer the written copy: owed %d", got)
	}
	testF116IdentifierFree(t, e, seed.pi, "the answered exchange's identifier is free")
}

// T5c. A stale closer — an earlier attempt's token, after a successor's section opened — touches
// neither the successor's token nor its count.
//
// Mutation M5 (the decrement not behind the exact token).
func TestF116AStaleCloserLeavesTheSuccessorsSection(t *testing.T) {
	e, _ := testExecCellEnv(t, "f116-t5c")
	const pi = uint16(9)
	e.sess.mu.Lock()
	tokA, okA := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if !okA {
		t.Fatal("attempt A did not open")
	}
	e.sess.endPubRelSend(e.c, pi, tokA, true)
	e.sess.mu.Lock()
	tokB, okB := e.sess.beginPubRelSendLocked(e.c, pi)
	e.sess.mu.Unlock()
	if !okB {
		t.Fatal("attempt B did not open")
	}
	e.sess.endPubRelSend(e.c, pi, tokA, false)
	if h := testF116ReadHold(e, pi); h.token != tokB || h.owed != 2 {
		t.Fatalf("a stale closer changed the successor's section: %+v, want token %q owed 2", h, tokB)
	}
	e.sess.endPubRelSend(e.c, pi, tokB, true)
}

// T8. relEntered describes exactly the open attempt: set at write entry, cleared with the token,
// and reset by the next opener even when something left it true.
//
// Mutations M12a (not cleared with the token), M12b (not reset by the opener).
func TestF116TheWriteEntryFactBelongsToTheOpenAttempt(t *testing.T) {
	e, _ := testExecCellEnv(t, "f116-t8")
	const pi = uint16(10)
	t.Run("cleared with the token", func(t *testing.T) {
		e.sess.mu.Lock()
		tok, ok := e.sess.beginPubRelSendLocked(e.c, pi)
		if ok {
			e.sess.markPubRelEnteredLocked(pi, tok)
		}
		e.sess.mu.Unlock()
		if h := testF116ReadHold(e, pi); !ok || !h.entered {
			t.Fatalf("write entry must mark the open attempt entered: %+v", h)
		}
		e.sess.endPubRelSend(e.c, pi, tok, true)
		if h := testF116ReadHold(e, pi); h.entered || h.token != _EMPTY_ {
			t.Fatalf("the fact must go with the token: %+v", h)
		}
	})
	t.Run("reset by the opener", func(t *testing.T) {
		e.sess.mu.Lock()
		h := e.sess.sendsInFlight[pi]
		if h == nil || h.pubCompOwed == 0 {
			e.sess.mu.Unlock()
			t.Fatal("fixture: the written attempt's hold must remain")
		}
		h.relEntered = true // seeded: whatever left it true, the next attempt has not entered
		tok, ok := e.sess.beginPubRelSendLocked(e.c, pi)
		e.sess.mu.Unlock()
		if got := testF116ReadHold(e, pi); !ok || got.entered {
			t.Fatalf("a fresh attempt must not read as entered: %+v", got)
		}
		e.sess.endPubRelSend(e.c, pi, tok, true)
	})
}

// testF116ReleaseLineage applies an accepted ending of lin, as acceptExchangeObservationLocked
// does, and reports what that left on pi's PUBREL map.
func testF116ReleaseLineage(e *testInflightEnv, pi uint16, lin string) (released bool, entry *mqttPending) {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	testReleaseLineageLocked(e.sess, lin)
	p := e.sess.pendingPubRel[pi]
	return p != nil && p.awaitingPubCompOwner != 0, p
}

// testF116NoReleasedDelivery requires that pi carries no released delivery: absent, or the
// terminal reservation the worker keeps for a slot cleanup it owns.
func testF116NoReleasedDelivery(t *testing.T, e *testInflightEnv, pi uint16, what string) {
	t.Helper()
	e.sess.mu.Lock()
	p := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if p != nil && p.awaitingPubCompOwner != 0 {
		t.Fatalf("%s: a released delivery awaiting owner %d remains on %d: %+v", what, p.awaitingPubCompOwner, pi, p)
	}
}

// T7. A lineage release while the only open attempt has NOT entered its write makes no released
// delivery: that attempt can no longer emit, so no PUBCOMP will ever answer one.
//
// Mutation M8 (the release counts the unentered unit).
func TestF116ALineageReleaseBeforeEntryMakesNoReleasedDelivery(t *testing.T) {
	e, tc := testExecCellEnv(t, "f116-t7")
	seed := testSeedRealPubRelReady(t, e)
	written := len(tc.written)
	var released bool
	restore := mqttSetTestSeam(&mqttTestAfterPubRelCommitV, func() {
		released, _ = testF116ReleaseLineage(e, seed.pi, seed.lin)
	})
	writes := testCountPubRelWrites(t, e, func() { testF116Deliver(t, e, seed, 1) })
	restore()
	if released {
		t.Fatal("a release before write entry made a released delivery for a PUBREL that cannot be emitted")
	}
	if writes != 0 || len(tc.written) != written {
		t.Fatalf("a released exchange's PUBREL must not be written: entered %d", writes)
	}
	if got := testF116Owed(e, seed.pi); got != 0 {
		t.Fatalf("the unentered attempt's unit must be given back: owed %d", got)
	}
	testF116NoReleasedDelivery(t, e, seed.pi, "after the callback")
}

// T7b. The same release over a written copy still unanswered DOES make a released delivery — a
// real PUBCOMP is owed — and the refused attempt's cancellation leaves it; that PUBCOMP then
// settles through it.
//
// Mutation M8's converse guard: a reader that discounted the written unit too would fail here.
func TestF116ALineageReleaseBeforeEntryKeepsAWrittenCopysReleasedDelivery(t *testing.T) {
	e, _ := testExecCellEnv(t, "f116-t7b")
	seed := testSeedRealPubRelReady(t, e)
	testF116Written(t, e, seed, 1)
	var released bool
	restore := mqttSetTestSeam(&mqttTestAfterPubRelCommitV, func() {
		released, _ = testF116ReleaseLineage(e, seed.pi, seed.lin)
	})
	testF116Deliver(t, e, seed, 2)
	restore()
	if !released {
		t.Fatal("a release over an unanswered written copy must make a released delivery")
	}
	if got := testF116Owed(e, seed.pi); got != 1 {
		t.Fatalf("the written copy's unit must stay: owed %d", got)
	}
	e.sess.mu.Lock()
	p := e.sess.pendingPubRel[seed.pi]
	e.sess.mu.Unlock()
	if p == nil || p.awaitingPubCompOwner != e.c.cid {
		t.Fatalf("the released delivery must survive the refused attempt: %+v", p)
	}
	testActionDrained(t, e, seed.pi)
	e.c.mqttProcessPubComp(seed.pi)
	if got := testF116Owed(e, seed.pi); got != 0 {
		t.Fatalf("the PUBCOMP must answer the written copy: owed %d", got)
	}
	testF116NoReleasedDelivery(t, e, seed.pi, "after the PUBCOMP")
}

// testF116ReleaseAtEntry releases lin inside the write block hook — after write entry, before the
// writer — and limits the client's Maximum Packet Size so the writer refuses the frame. It
// reports whether the release made a released delivery.
func testF116ReleaseAtEntry(t *testing.T, e *testInflightEnv, seed testRealPubRelSeed, n int) (released, entered bool) {
	t.Helper()
	restore := setMQTTTestPubRelWriteBlock(func() {
		entered = testF116ReadHold(e, seed.pi).entered
		released, _ = testF116ReleaseLineage(e, seed.pi, seed.lin)
	})
	prev := testLongLivedMaxPacketSize(e.c, 3)
	testF116Deliver(t, e, seed, n)
	testLongLivedMaxPacketSize(e.c, prev)
	restore()
	return released, entered
}

// T7c. A release after write entry makes a released delivery (the frame could still have gone);
// when the writer then refuses on size, the attempt's unit is given back and, with nothing left
// owed, the released delivery goes with it.
//
// Mutations M9 (the release discounts an ENTERED attempt: no released delivery), M10 (no
// retirement at cancellation).
func TestF116AnEnteredAttemptRefusedAfterAReleaseRetiresItsReleasedDelivery(t *testing.T) {
	e, tc := testExecCellEnv(t, "f116-t7c")
	seed := testSeedRealPubRelReady(t, e)
	written := len(tc.written)
	released, entered := testF116ReleaseAtEntry(t, e, seed, 1)
	if !entered {
		t.Fatal("fixture: the hook must run after write entry")
	}
	if !released {
		t.Fatal("a release after write entry must make a released delivery")
	}
	if len(tc.written) != written {
		t.Fatalf("a PUBREL refused for size put %x on the wire", tc.written[written:])
	}
	if got := testF116Owed(e, seed.pi); got != 0 {
		t.Fatalf("the size-refused attempt's unit must be given back: owed %d", got)
	}
	testF116NoReleasedDelivery(t, e, seed.pi, "after the size refusal")
}

// T7d. The same over a written copy still unanswered: its unit stays, so does the released
// delivery awaiting its PUBCOMP.
//
// Mutation M11 (retirement at cancellation whatever remains owed).
func TestF116AnEnteredAttemptRefusedAfterAReleaseKeepsAWrittenCopysReleasedDelivery(t *testing.T) {
	e, _ := testExecCellEnv(t, "f116-t7d")
	seed := testSeedRealPubRelReady(t, e)
	testF116Written(t, e, seed, 1)
	released, _ := testF116ReleaseAtEntry(t, e, seed, 2)
	if !released {
		t.Fatal("a release after write entry must make a released delivery")
	}
	if got := testF116Owed(e, seed.pi); got != 1 {
		t.Fatalf("the written copy's unit must stay: owed %d", got)
	}
	e.sess.mu.Lock()
	p := e.sess.pendingPubRel[seed.pi]
	e.sess.mu.Unlock()
	if p == nil || p.awaitingPubCompOwner != e.c.cid {
		t.Fatalf("the released delivery awaiting the written copy's PUBCOMP must remain: %+v", p)
	}
}
