// Copyright 2025 The mqtt-nats-broker Authors
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
	"strings"
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

// testAcquireEnv admits an ORDINARY QoS 2 delivery through the outbound authority on the real
// store (F-105 row 2, consult 29) and returns the send token a section would carry for it: the
// register at `publish` beside its exchange, no token on it, the connection accepted under the
// serving entitlement. Pass-through load and store seams stand so a test can chain faults onto
// them; an ACQUISITION is intercepted at the authority commit instead, since it no longer goes
// through the register store.
func testAcquireEnv(t *testing.T, clientID string) (*testInflightEnv, string, mqttSendToken, uint16) {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, e)
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 2)
	testInstallPassThroughSeams(e)
	testStampAcceptedConnection(t, e)
	e.sess.mu.Lock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	e.sess.mu.Unlock()
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the admitted slot: rec=%+v err=%v", cur, err)
	}
	if cur.State != mqttInflightPublish || cur.ActionToken != _EMPTY_ || mqttDurIsSlotOnlyClass(cur.Dur) {
		t.Fatalf("the fixture must be a quiescent ORDINARY publish slot, got %+v", cur)
	}
	if x, _, xerr := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, testGenOf(e), cur.Lin); xerr != nil || x == nil {
		t.Fatalf("the admitted slot must stand beside its exchange: %+v (%v)", x, xerr)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	if ref == nil {
		t.Fatal("the seeded slot must yield a reference")
	}
	token := mqttNewInflightNonce()
	return e, subject, mqttSendToken{token: token, slot: *ref, out: testPublishOutputFor(e, *ref, token)}, pi
}

// testGenOf reads the session's current generation.
func testGenOf(e *testInflightEnv) string {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return e.sess.gen
}

// Mutation M95: arm tok.actionHeld only after a successful CAS, as before the fix.
func TestMQTTPublishActionLostResponseIsAdoptedAndDrained(t *testing.T) {
	t.Parallel()
	e, subject, tok, pi := testAcquireEnv(t, "acquire-lost")
	lost := testLoseNextAcquisitionResponse(t, e)

	if e.sess.acquirePublishAction(e.c, pi, &tok) {
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
	if !e.sess.acquirePublishAction(e.c, pi, &next) {
		t.Fatal("after the adopted token drained, the next acquisition must succeed")
	}
}

// The resolving read FAILS too. The mark stays, the section-end drain cannot resolve and
// records a shutdown-visible obligation, and the shutdown's final pass — with storage back —
// clears the token. Mutation M96: withdraw the mark on an indeterminate resolving read.
func TestMQTTPublishActionUnresolvedLostResponseReachesTheFinalPass(t *testing.T) {
	t.Parallel()
	e, subject, tok, pi := testAcquireEnv(t, "acquire-unresolved")
	lost := testLoseNextAcquisitionResponse(t, e)
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

	if e.sess.acquirePublishAction(e.c, pi, &tok) {
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
	e, subject, tok, pi := testAcquireEnv(t, "acquire-peer")
	peer := mqttNewInflightNonce()
	var raced, oursRefused atomic.Bool
	// The peer's VALID competing acquisition — its own authority batch, token and action record —
	// lands between our batch's composition and its send, so the store refuses ours.
	testSetTransitionCommit(t, e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if testAcquisitionTokenOf(tr) != tok.token || !raced.CompareAndSwap(false, true) {
			return send()
		}
		cur, smsg, lerr := e.asm.jsa.loadInflightRecord(subject)
		al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
		if lerr != nil || cur == nil || smsg == nil || al.rec == nil {
			t.Errorf("reading the slot the peer acquires: %+v %+v (%v)", cur, al, lerr)
			return send()
		}
		if _, _, perr := e.asm.jsa.opAcquireAction(&mqttAcquireActionRequest{
			ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-acquire-peer",
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, Nonce: cur.Lin, Acq: cur.Acq + 1, Kind: mqttActionPublish,
			Binding:  mqttBindConnection(e.c.cid + 1000),
			Register: cur, RegisterSeq: smsg.Sequence, Token: peer,
		}); perr != nil {
			t.Errorf("the peer's competing acquisition must land: %v", perr)
		}
		seq, err := send()
		oursRefused.Store(err != nil)
		return seq, err
	})

	if e.sess.acquirePublishAction(e.c, pi, &tok) {
		t.Fatal("a refused acquisition must not authorize a transmission")
	}
	if tok.actionHeld {
		t.Fatal("a definite refusal must withdraw the section's mark")
	}
	if !raced.Load() || !oursRefused.Load() {
		t.Fatalf("the race never happened (peer acquired=%v, ours refused=%v), so nothing here is tested",
			raced.Load(), oursRefused.Load())
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
	e, subject, tok := testAcquireSlotOnlyEnv(t, "acquire-counter", pi)
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

	acquired := e.sess.acquirePublishAction(e.c, pi, &tok)
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

// THE SAME WINDOW ON AN ORDINARY SLOT (F-105 row 2). An ordinary acquisition is an authority
// batch, and a peer acquiring and draining between its composition and its send makes the store
// refuse it; the bounded re-attempt (consult 22) then reads the moved register and acquires at
// the NEXT counter. What C-I requires survives that: whatever this call reports, the counter it
// records is the one the record carries beside its token, and the action record names it too.
//
// Mutation M122b: record the outer read's counter instead of the landed one.
func TestMQTTPublishActionRecordsTheCounterItLandedOnAnOrdinarySlot(t *testing.T) {
	t.Parallel()
	e, subject, tok, pi := testAcquireEnv(t, "acquire-counter-ordinary")
	outerAcq := tok.slot.acq
	var moved atomic.Int64
	testSetTransitionCommit(t, e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		// The batch is recognised by its ACTION RECORD member, not its token member: a batch that
		// carried the record without the token (mutation A1(a)) is the batch under test and must
		// still be the one the peer races.
		if !testAcquisitionCarriesActionRecord(tr, getHash(e.sess.id)) || !moved.CompareAndSwap(0, 1) {
			return send()
		}
		cur, smsg, lerr := e.asm.jsa.loadInflightRecord(subject)
		if lerr != nil || cur == nil || smsg == nil {
			t.Errorf("reading the slot the peer moves: %+v (%v)", cur, lerr)
			return send()
		}
		peer := *cur
		peer.Acq++ // a peer acquired and drained: counter advanced, token clear
		if _, werr := e.asm.jsa.storeInflightCAS(subject, smsg.Sequence, &peer); werr != nil {
			t.Errorf("installing the peer's drained acquisition: %v", werr)
		}
		return send()
	})
	if !e.sess.acquirePublishAction(e.c, pi, &tok) {
		t.Fatal("the re-attempt over the moved register must acquire")
	}
	if moved.Load() == 0 {
		t.Fatal("the fixture never reached the acquisition batch, so nothing here is tested")
	}
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || cur.ActionToken != tok.token {
		t.Fatalf("the landed acquisition is not on the record: %+v %v", cur, err)
	}
	if cur.Acq != outerAcq+2 || tok.slot.acq != cur.Acq {
		t.Fatalf("the section records counter %d, the record carries %d, want both %d: a counter "+
			"this call did not land", tok.slot.acq, cur.Acq, outerAcq+2)
	}
	if tok.out == nil || tok.out.action == nil || tok.out.action.Acq != cur.Acq || tok.out.acq != cur.Acq {
		t.Fatalf("the acquisition the frame will consume does not name the landed counter: %+v", tok.out)
	}
	if _, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttActionSubject(getHash(e.sess.id), tok.slot.name.gen, cur.Lin, cur.Acq)); lerr != nil {
		t.Fatalf("no action record at the landed counter: %v", lerr)
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
	e, subject, tok, pi := testAcquireEnv(t, "acquire-delayed-drain")
	e.sess.mu.Lock()
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending, 1)
	}
	e.sess.pendingPublish[pi] = &mqttPending{slot: tok.slot}
	e.sess.mu.Unlock()
	if !e.sess.acquirePublishAction(e.c, pi, &tok) {
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
	if !e.sess.acquirePublishAction(e.c, pi, &newer) {
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

// testAcquisitionCarriesActionRecord reports whether an acquisition batch writes an action record
// for the client: the member that identifies an acquisition whichever other member it carries.
func testAcquisitionCarriesActionRecord(tr *mqttAuthorityTransition, idHash string) bool {
	if tr == nil || tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionAcquireAction {
		return false
	}
	prefix := mqttF105SubjectPrefix() + mqttActionClassTk + "." + idHash + "."
	for _, m := range tr.Extra {
		if strings.HasPrefix(m.subj, prefix) {
			return true
		}
	}
	return false
}
