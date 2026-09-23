// Copyright 2025 The mqtt-nats-bridge Authors
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
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// A LOST ACQUISITION RESPONSE MUST NOT ORPHAN THE TOKEN (F-098, holistic review round 1,
// finding 2; C13 order 2).
//
// The acquisition CAS can land and lose its response. casInflightSlot then rereads, finds a
// token — ours — and refuses on it, so acquirePublishAction saw only "not acquired". Before the
// fix the section held nothing: its end skipped the drain, no owed-drain entry was recorded,
// and every later acquisition refused the survivor for the life of the session.

// testAcquireEnv seeds a PUBLISH-phase slot under this session's custody and returns the
// send token a section would carry for it.
func testAcquireEnv(t *testing.T, clientID string, pi uint16) (*testInflightEnv, string, mqttSendToken) {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForPubRelDelivery(t, e)
	subject := testVetoSeedSlot(t, e, pi, mqttInflightPublish, mqttNewInflightNonce())
	// testVetoSeedSlot installs a token; an acquisition needs a QUIESCENT slot.
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the seeded slot: rec=%+v err=%v", cur, err)
	}
	quiet := *cur
	quiet.ActionToken, quiet.ActionClaim, quiet.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
	seq, serr := e.asm.testInflightStore(subject, smsg.Sequence, &quiet)
	if serr != nil {
		t.Fatalf("quiescing the slot: %v", serr)
	}
	ref := mqttSlotRefFromRecord(subject, seq, &quiet)
	if ref == nil {
		t.Fatal("the seeded slot must yield a reference")
	}
	return e, subject, mqttSendToken{token: mqttNewInflightNonce(), slot: *ref}
}

// testLoseNextAcquisitionResponse makes the next write that INSTALLS an action land and then
// report failure — the lost-response shape. Every other write is untouched.
func testLoseNextAcquisitionResponse(e *testInflightEnv) *atomic.Bool {
	lost := &atomic.Bool{}
	e.asm.mu.Lock()
	store := e.asm.testInflightStore
	e.asm.testInflightStore = func(subject string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		seq, err := store(subject, expect, r)
		if err == nil && r.ActionToken != _EMPTY_ && lost.CompareAndSwap(false, true) {
			return 0, errors.New("test: the acquisition landed but its response was lost")
		}
		return seq, err
	}
	e.asm.mu.Unlock()
	return lost
}

// Mutation M95: arm tok.actionHeld only after a successful CAS, as before the fix.
func TestMQTTPublishActionLostResponseIsAdoptedAndDrained(t *testing.T) {
	t.Parallel()
	const pi = uint16(8)
	e, subject, tok := testAcquireEnv(t, "acquire-lost", pi)
	lost := testLoseNextAcquisitionResponse(e)

	if e.sess.acquirePublishAction(e.c, pi, &tok, false) {
		t.Fatal("an acquisition whose response was lost must not authorize a transmission")
	}
	if !lost.Load() {
		t.Fatal("the fixture never lost an acquisition response, so nothing here is tested")
	}
	cur, _, _ := e.asm.loadInflightRecord(subject)
	if cur == nil || cur.ActionToken != tok.token {
		t.Fatalf("the landed acquisition must be on the record: %+v", cur)
	}
	if !tok.actionHeld {
		t.Fatal("the section must own the landed token for cleanup, or nothing ever drains it")
	}

	// The section's end drains it...
	if !e.sess.drainPublishAction(e.c, tok) {
		t.Fatal("the adopted token must drain")
	}
	if after, _, _ := e.asm.loadInflightRecord(subject); after == nil || after.ActionToken != _EMPTY_ {
		t.Fatalf("the drain must clear the adopted token: %+v", after)
	}
	// ...and a fresh acquisition proceeds.
	next := mqttSendToken{token: mqttNewInflightNonce(), slot: tok.slot}
	if !e.sess.acquirePublishAction(e.c, pi, &next, false) {
		t.Fatal("after the adopted token drained, the next acquisition must succeed")
	}
}

// The resolving read FAILS too. The mark stays, the section-end drain cannot resolve and
// records a shutdown-visible obligation, and the shutdown's final pass — with storage back —
// clears the token. Mutation M96: withdraw the mark on an indeterminate resolving read.
func TestMQTTPublishActionUnresolvedLostResponseReachesTheFinalPass(t *testing.T) {
	t.Parallel()
	const pi = uint16(9)
	e, subject, tok := testAcquireEnv(t, "acquire-unresolved", pi)
	lost := testLoseNextAcquisitionResponse(e)
	var reading atomic.Bool
	reading.Store(true)
	e.asm.mu.Lock()
	load := e.asm.testInflightLoad
	e.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
		if lost.Load() && reading.Load() {
			return nil, nil, errors.New("test: the resolving read is unavailable")
		}
		return load(subj)
	}
	e.asm.mu.Unlock()

	if e.sess.acquirePublishAction(e.c, pi, &tok, false) {
		t.Fatal("an unresolved acquisition must not authorize a transmission")
	}
	if !tok.actionHeld {
		t.Fatal("an indeterminate resolution must KEEP the section's cleanup ownership")
	}
	if e.sess.drainPublishAction(e.c, tok) {
		t.Fatal("with reads unavailable the drain cannot resolve")
	}
	e.s.owedDrainsMu.Lock()
	owed := len(e.s.owedDrains)
	e.s.owedDrainsMu.Unlock()
	if owed != 1 {
		t.Fatalf("the unresolved drain must be recorded for the shutdown's final pass, got %d", owed)
	}

	reading.Store(false) // storage recovers before the teardown
	e.s.finishOwedDrains()
	if after, _, _ := e.asm.loadInflightRecord(subject); after == nil || after.ActionToken != _EMPTY_ {
		t.Fatalf("the final pass must clear the orphaned token: %+v", after)
	}
}

// A PEER'S token races in between the pre-read and the CAS: a DEFINITE refusal. The section
// must withdraw its mark and must never drain the peer's action.
func TestMQTTPublishActionRefusedAcquisitionLeavesAPeersToken(t *testing.T) {
	t.Parallel()
	const pi = uint16(10)
	e, subject, tok := testAcquireEnv(t, "acquire-peer", pi)
	peer := mqttNewInflightNonce()
	var raced atomic.Bool
	e.asm.mu.Lock()
	store := e.asm.testInflightStore
	e.asm.testInflightStore = func(subj string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		if r.ActionToken == tok.token && raced.CompareAndSwap(false, true) {
			// The peer's acquisition lands first, so ours is rejected by the CAS.
			cur, smsg, _ := e.asm.testInflightLoad(subj)
			theirs := *cur
			theirs.ActionToken = peer
			theirs.ActionClaim, theirs.ActionClaimSeq = theirs.Claim, theirs.ClaimSeq
			theirs.Acq = cur.Acq + 1
			if _, err := store(subj, smsg.Sequence, &theirs); err != nil {
				return 0, err
			}
			return 0, errors.New("test: CAS rejected, the peer acquired first")
		}
		return store(subj, expect, r)
	}
	e.asm.mu.Unlock()

	if e.sess.acquirePublishAction(e.c, pi, &tok, false) {
		t.Fatal("a refused acquisition must not authorize a transmission")
	}
	if tok.actionHeld {
		t.Fatal("a definite refusal must withdraw the section's mark")
	}
	e.sess.drainPublishAction(e.c, tok)
	if after, _, _ := e.asm.loadInflightRecord(subject); after == nil || after.ActionToken != peer {
		t.Fatalf("the peer's action must be untouched: %+v", after)
	}
}

// AN ACQUISITION RECORDS THE COUNTER IT ACTUALLY WROTE (F-098 C-I, holistic review round 4,
// finding 2). casInflightSlot refreshes its CAS base on every internal attempt, and a peer that
// acquired and DRAINED in between leaves custody unchanged and the token clear — so the token
// and custody predicates both pass while the counter has moved. The caller derived its expected
// counter and the entry's recorded one from its OUTER read, so a CAS landing at k+2 while the
// entry says k+1 makes this exchange's own acknowledgement read as a successor's, and the
// terminal cleanup it owes is dropped.
//
// The fixture moves the record from inside the CAS's own load, which is the only window the
// caller's pre-read cannot see.
//
// Mutation M122: drop the acquire arm's counter predicate.
func TestMQTTPublishActionRefusesACounterItDidNotRead(t *testing.T) {
	t.Parallel()
	const pi = uint16(11)
	e, subject, tok := testAcquireEnv(t, "acquire-counter", pi)
	outerAcq := tok.slot.acq
	var loads, moved atomic.Int64
	e.asm.mu.Lock()
	load := e.asm.testInflightLoad
	store := e.asm.testInflightStore
	e.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
		cur, smsg, err := load(subj)
		if subj != subject || err != nil || cur == nil || smsg == nil {
			return cur, smsg, err
		}
		// Load 1 is acquirePublishAction's own read; load 2 is the CAS's base.
		if loads.Add(1) == 2 && moved.CompareAndSwap(0, 1) {
			peer := *cur
			peer.Acq++ // a peer acquired and drained: counter advanced, token clear
			if _, werr := store(subj, smsg.Sequence, &peer); werr != nil {
				t.Errorf("installing the peer's drained acquisition: %v", werr)
				return cur, smsg, err
			}
			return load(subj)
		}
		return cur, smsg, err
	}
	e.asm.mu.Unlock()

	acquired := e.sess.acquirePublishAction(e.c, pi, &tok, false)
	if moved.Load() == 0 {
		t.Fatal("the fixture never reached the CAS's own load, so nothing here is tested")
	}
	if acquired {
		t.Fatal("an acquisition whose base moved under it must not report success: its counter " +
			"would name a transmission the slot never carried")
	}
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil {
		t.Fatalf("reading the slot after the refusal: %+v %v", cur, err)
	}
	if cur.ActionToken != _EMPTY_ {
		t.Fatalf("a refused acquisition left a token on the record: %+v", cur)
	}
	if cur.Acq != outerAcq+1 {
		t.Fatalf("the record's counter is %d, want the peer's %d — this call must not have "+
			"advanced it", cur.Acq, outerAcq+1)
	}
}

// A DELAYED DRAIN MAY NOT LOWER A NEWER TRANSMISSION'S COUNTER (F-098 C-I, holistic review
// round 6, finding 2). Successive transmissions of one allocation deliberately share nonce and
// subject, so those two do not separate an older drain from a newer one. An older drain can
// clear its token, pause before its write-back, and resume after an AckWait delivery has
// acquired and drained the same allocation — putting the EARLIER counter back on the entry,
// after which the next acknowledgement is bound to a transmission the slot has moved past and
// its terminal obligation is discarded as superseded.
//
// Deliberately NOT parallel: the fixture blocks a store hook on this manager.
//
// Mutation M129: drop the monotonic guard on the drain's write-back.
func TestMQTTDelayedDrainDoesNotLowerANewerCounter(t *testing.T) {
	e, subject, tok := testAcquireEnv(t, "acquire-delayed-drain", 12)
	const pi = uint16(12)
	e.sess.mu.Lock()
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending, 1)
	}
	e.sess.pendingPublish[pi] = &mqttPending{slot: tok.slot}
	e.sess.mu.Unlock()
	if !e.sess.acquirePublishAction(e.c, pi, &tok, false) {
		t.Fatal("the first acquisition must succeed")
	}
	first := tok.slot.acq

	// The first drain's CAS lands, then it is HELD before its write-back.
	held := make(chan struct{})
	release := make(chan struct{})
	var blocked atomic.Bool
	e.asm.mu.Lock()
	store := e.asm.testInflightStore
	e.asm.testInflightStore = func(subj string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		seq, err := store(subj, expect, r)
		if subj == subject && err == nil && r.ActionToken == _EMPTY_ && blocked.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
		return seq, err
	}
	e.asm.mu.Unlock()
	done := make(chan bool, 1)
	go func() { done <- e.sess.drainPublishAction(e.c, tok) }()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the first drain never reached its CAS")
	}

	// A NEWER transmission acquires and drains while the older drain is held.
	// Through the manager, because this env's slot lives behind its seams rather than in
	// JetStream.
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("reading the quiesced slot: %+v %v", cur, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	if ref == nil {
		t.Fatal("the quiesced slot must yield a reference")
	}
	newer := mqttSendToken{token: mqttNewInflightNonce(), slot: *ref}
	if !e.sess.acquirePublishAction(e.c, pi, &newer, false) {
		t.Fatal("the newer transmission's acquisition must succeed")
	}
	if newer.slot.acq <= first {
		t.Fatalf("the newer acquisition must advance the counter: %d then %d", first, newer.slot.acq)
	}

	close(release)
	if !<-done {
		t.Fatal("the held drain must still resolve")
	}
	e.sess.mu.Lock()
	entryAcq := e.sess.pendingPublish[pi].slot.acq
	e.sess.mu.Unlock()
	if entryAcq < newer.slot.acq {
		t.Fatalf("the delayed drain put counter %d back on the entry, over the newer %d",
			entryAcq, newer.slot.acq)
	}
}
