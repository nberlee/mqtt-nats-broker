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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// AN ACKNOWLEDGEMENT'S TERMINAL CLEANUP IS NEVER DROPPED (F-098, holistic review round 1,
// finding 5; C13 order 5). Before the fix a failed `publish -> publish_terminal` advance only
// logged and a failed delete was ignored — after the entry had already been untracked — so a
// connected, idle session had no continuation at all and the identifier's fate was left to a
// later event that never has to come.
//
// The IDENTIFIER is held from the untrack until the delete is confirmed; the CAPACITY is freed
// at once, because the client's acknowledgement ended the exchange for the client.

type testTerminalEnv struct {
	*testInflightEnv
	pi      uint16
	dur     string
	sseq    uint64
	subject string
	failing atomic.Bool
	advance func(r *mqttInflightRecord, write func() (uint64, error)) (uint64, error)
}

func testNewTerminalEnv(t *testing.T, clientID string) *testTerminalEnv {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	te := &testTerminalEnv{testInflightEnv: e}
	te.dur = e.sess.idHash + "_" + nuid.Next()
	te.sseq = e.publishSource(t, te.dur)
	te.pi = e.track(t, te.dur, te.sseq, 1)
	te.subject = mqttInflightSubject(e.sess.idHash, e.sess.gen, te.pi)
	e.asm.mu.Lock()
	e.asm.testInflightStore = func(subject string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		write := func() (uint64, error) { return e.asm.jsa.storeInflightCAS(subject, expect, r) }
		if subject == te.subject && r.State == mqttInflightPublishTerminal && te.advance != nil {
			return te.advance(r, write)
		}
		return write()
	}
	e.asm.mu.Unlock()
	e.c.mqtt = &mqtt{sess: e.sess, proto: mqttProtoLevel5}
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	t.Cleanup(func() {
		e.sess.mu.Lock()
		e.sess.c = nil
		e.sess.mu.Unlock()
	})
	return te
}

func (te *testTerminalEnv) failDeletes() {
	te.asm.mu.Lock()
	te.asm.testInflightDelete = func(seq uint64) error {
		if te.failing.Load() {
			return errors.New("test: the register delete could not be confirmed")
		}
		return te.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
	}
	te.asm.mu.Unlock()
}

// identifierHeld reports the two halves separately: the identifier reserved, and the
// capacity (the pending entry) freed.
func (te *testTerminalEnv) identifierHeld() (reserved, entry bool) {
	te.sess.mu.Lock()
	defer te.sess.mu.Unlock()
	_, entry = te.sess.pendingPublish[te.pi]
	return te.sess.sendReservedLocked(te.pi), entry
}

func (te *testTerminalEnv) slot() *mqttInflightRecord {
	cur, _, _ := te.asm.jsa.loadInflightRecord(te.subject)
	return cur
}

// acknowledge drives the PUBACK through the named path.
func (te *testTerminalEnv) acknowledge(t *testing.T, deferred bool) {
	t.Helper()
	var tok mqttSendToken
	if deferred {
		tok = te.openSection(t, te.pi, te.dur, te.sseq)
	}
	if err := te.c.mqttProcessPublishReceived(te.pi, false, 0); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	if deferred {
		// The acknowledgement parked on the open section; its end performs the settlement.
		te.sess.endPublishSend(te.c, te.pi, tok, mqttIntentNone, false)
	}
}

func (te *testTerminalEnv) waitDischarged(t *testing.T, why string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		reserved, _ := te.identifierHeld()
		if te.slot() == nil && !reserved {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the terminal cleanup never completed on an idle session: slot=%+v reserved=%v",
				why, te.slot(), reserved)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Mutations: M103 (no identifier hold), M104 (no continuation on failure).
func TestMQTTAcknowledgementTerminalCleanupSurvivesFailures(t *testing.T) {
	t.Parallel()
	for _, deferred := range []bool{false, true} {
		for _, fault := range []string{"advance refused", "advance landed, response lost", "delete fails"} {
			deferred, fault := deferred, fault
			path := "immediate"
			if deferred {
				path = "deferred"
			}
			t.Run(path+"/"+fault, func(t *testing.T) {
				t.Parallel()
				te := testNewTerminalEnv(t, "terminal-"+path)
				te.failing.Store(true)
				var lost atomic.Bool
				switch fault {
				case "advance refused":
					te.advance = func(_ *mqttInflightRecord, write func() (uint64, error)) (uint64, error) {
						if te.failing.Load() {
							return 0, errors.New("test: the terminal advance was refused")
						}
						return write()
					}
				case "advance landed, response lost":
					te.advance = func(_ *mqttInflightRecord, write func() (uint64, error)) (uint64, error) {
						seq, err := write()
						if err == nil && lost.CompareAndSwap(false, true) {
							return 0, errors.New("test: the terminal advance landed but its response was lost")
						}
						return seq, err
					}
				case "delete fails":
					te.failDeletes()
				}

				te.acknowledge(t, deferred)

				reserved, entry := te.identifierHeld()
				if !reserved {
					t.Fatalf("the identifier was released while its slot still names the exchange (%s)", fault)
				}
				if entry {
					t.Fatal("the capacity must be freed at the acknowledgement: the client has completed")
				}
				if te.slot() == nil {
					t.Fatalf("the slot must still exist while the %s", fault)
				}

				te.failing.Store(false)
				te.waitDischarged(t, fault)
			})
		}
	}
}

// A LIVE ACTION on the slot waits: no delete under a holder, then cleanup once it clears.
func TestMQTTAcknowledgementTerminalCleanupWaitsForALiveAction(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-held")
	peer := mqttNewInflightNonce()
	te.advance = func(r *mqttInflightRecord, _ func() (uint64, error)) (uint64, error) {
		// The terminal write lands WITH a live action on the record.
		held := *r
		held.ActionToken, held.ActionClaim, held.ActionClaimSeq = peer, held.Claim, held.ClaimSeq
		_, smsg, _ := te.asm.jsa.loadInflightRecord(te.subject)
		return te.asm.jsa.storeInflightCAS(te.subject, smsg.Sequence, &held)
	}
	te.acknowledge(t, false)

	time.Sleep(mqttActionReconcileDelay + time.Second) // an enrolled attempt has certainly run
	cur := te.slot()
	if cur == nil || cur.ActionToken != peer {
		t.Fatalf("terminal cleanup acted under a live action: %+v", cur)
	}
	if reserved, _ := te.identifierHeld(); !reserved {
		t.Fatal("the identifier was released while a holder still acts on the slot")
	}

	// The holder drains.
	te.advance = nil
	cleared := *cur
	cleared.ActionToken, cleared.ActionClaim, cleared.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
	_, smsg, _ := te.asm.jsa.loadInflightRecord(te.subject)
	if _, err := te.asm.jsa.storeInflightCAS(te.subject, smsg.Sequence, &cleared); err != nil {
		t.Fatalf("draining the holder's token: %v", err)
	}
	te.waitDischarged(t, "after the holder drained")
}

// A SUCCESSOR transmission is not this acknowledgement's to finish: the continuation stops,
// leaves it alone, and releases its own hold. Mutation M105: compare only nonce and source.
func TestMQTTAcknowledgementTerminalCleanupLeavesASuccessor(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-successor")
	te.failing.Store(true)
	te.advance = func(_ *mqttInflightRecord, write func() (uint64, error)) (uint64, error) {
		if te.failing.Load() {
			return 0, errors.New("test: the terminal advance was refused")
		}
		return write()
	}
	te.acknowledge(t, false)

	// A successor transmission of the same allocation lands before the retry.
	cur, smsg, _ := te.asm.jsa.loadInflightRecord(te.subject)
	succ := *cur
	succ.Acq = cur.Acq + 1
	if _, err := te.asm.jsa.storeInflightCAS(te.subject, smsg.Sequence, &succ); err != nil {
		t.Fatalf("installing the successor: %v", err)
	}
	te.failing.Store(false)

	deadline := time.Now().Add(10 * time.Second)
	for {
		if reserved, _ := te.identifierHeld(); !reserved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the continuation never discharged its own obligation")
		}
		time.Sleep(20 * time.Millisecond)
	}
	after := te.slot()
	if after == nil || after.Acq != succ.Acq || after.State != mqttInflightPublish {
		t.Fatalf("the successor must be untouched: %+v", after)
	}
}

// WORKER ADMISSION CLOSED: the obligation is handed to the shutdown registry and finished by
// the final pass. Mutation M106: drop the hand-off.
func TestMQTTAcknowledgementTerminalCleanupReachesTheFinalPass(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-shutdown")
	te.failing.Store(true)
	te.advance = func(_ *mqttInflightRecord, write func() (uint64, error)) (uint64, error) {
		if te.failing.Load() {
			return 0, errors.New("test: the terminal advance was refused")
		}
		return write()
	}
	te.s.closeWillDispatch() // shutdown Phase 2: no new workers
	te.acknowledge(t, false)

	te.s.owedTerminalsMu.Lock()
	owed := len(te.s.owedTerminals)
	te.s.owedTerminalsMu.Unlock()
	if owed != 1 {
		t.Fatalf("a refused admission must hand the cleanup to the shutdown registry, got %d", owed)
	}
	te.failing.Store(false)
	te.s.finishOwedTerminals()
	if reserved, _ := te.identifierHeld(); te.slot() != nil || reserved {
		t.Fatalf("the final pass must finish the cleanup: slot=%+v reserved=%v", te.slot(), reserved)
	}
}

// Recovery's enrolment carries NO acknowledgement, so it may never terminalize an ordinary
// PUBLISH slot. Mutation M107: ignore the acknowledged flag.
func TestMQTTTerminalCleanupNeverTerminalizesWithoutEvidence(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-no-evidence")
	cur, smsg, _ := te.asm.jsa.loadInflightRecord(te.subject)
	ref := mqttSlotRefFromRecord(te.subject, smsg.Sequence, cur)
	if !te.sess.attemptTerminalRegisterRelease(te.c, te.pi, *ref, false) {
		t.Fatal("an unacknowledged PUBLISH slot is not this continuation's: the attempt must discharge")
	}
	after := te.slot()
	if after == nil || after.State != mqttInflightPublish {
		t.Fatalf("a PUBLISH slot was terminalized with no acknowledgement behind it: %+v", after)
	}
}

// A RECOVERED TERMINAL RECORD RESERVES ONLY ITS OWN IDENTIFIER, AND ITS CLEANUP RELEASES IT
// (F-098, holistic review round 2, finding 3). Recovery meets a `publish_terminal` register whose
// delete fails. It used to reserve the identifier OPAQUE, which marks the whole identifier space
// incomplete — and the continuation that later confirmed the delete had nothing to undo that
// with, so a finite storage fault withheld every new outbound QoS 1/2 allocation until the next
// reconnect. The reservation is now the identity-bound terminal hold the continuation releases.
//
// Mutations: M114 (no hold installed), M115 (reserve opaque, as round 2 found it), M116 (no
// continuation armed).
func TestMQTTRecoveredTerminalRecordReleasesItsReservation(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-recovered")
	te.transmit(t, te.pi, te.dur, te.sseq, true)
	te.waitMarkerAbsent(t, te.pi)
	// The register as a crash leaves it: the acknowledgement recorded the phase and the delete
	// never landed.
	var cur *mqttInflightRecord
	var smsg *StoredMsg
	deadline := time.Now().Add(5 * time.Second)
	for {
		var err error
		cur, smsg, err = te.asm.jsa.loadInflightRecord(te.subject)
		if err == nil && cur != nil && cur.ActionToken == _EMPTY_ {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the transmission's action never drained: rec=%+v err=%v", cur, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	terminal := *cur
	terminal.State = mqttInflightPublishTerminal
	if _, err := te.asm.jsa.storeInflightCAS(te.subject, smsg.Sequence, &terminal); err != nil {
		t.Fatalf("writing the terminal phase: %v", err)
	}
	te.forgetInMemory()
	te.sess.mu.Lock()
	te.sess.sendsInFlight = nil
	te.sess.mu.Unlock()

	te.failDeletes()
	te.failing.Store(true)
	if _, err := te.sess.recoverInflightRegisters(te.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if te.slot() == nil {
		t.Fatal("the fixture must reach the unresolved branch: the terminal record was deleted")
	}
	te.sess.mu.Lock()
	reserved := te.sess.sendReservedLocked(te.pi)
	next := te.sess.bumpPI()
	te.sess.mu.Unlock()
	// THE TWO HALVES, MEASURED APART. The identifier this record names is not re-mintable, and
	// every OTHER identifier still is: an opaque reservation fails the second, a missing
	// reservation the first.
	if next == te.pi {
		t.Fatalf("allocation minted %d, the identifier its own terminal record still names", te.pi)
	}
	if next == 0 {
		t.Fatal("allocation is withheld for the whole session by one terminal record")
	}
	if !reserved {
		t.Fatalf("identifier %d is not reserved while its terminal record still names the exchange", te.pi)
	}

	te.failing.Store(false)
	te.waitDischarged(t, "recovered terminal record")
	te.sess.mu.Lock()
	incomplete := te.sess.piSpaceIncomplete
	te.sess.mu.Unlock()
	if incomplete {
		t.Fatal("allocation is still withheld after the terminal cleanup was confirmed")
	}
}

// A SUCCESSOR THAT ARRIVES INSIDE THE CAS IS NOT TERMINALIZED (F-098, holistic review round 3,
// finding 1). The acknowledgement's continuation checks the transmission identity on its own
// read, but casInflightSlot refreshes its base on every internal attempt: the caller reads
// transmission k, a peer acquires k+1, and the CAS's own load returns that successor — whose
// identifier, nonce and source all match, because matchesRecord deliberately omits the counter.
// Writing `publish_terminal` there records that the client answered a transmission it never
// saw, and the delete that terminal authority licenses follows.
//
// The fixture installs the successor from inside the CAS's own load, which is the only window
// that matters: TestMQTTAcknowledgementTerminalCleanupLeavesASuccessor installs it before the
// outer read, where the caller's own check already catches it.
//
// Mutation M119: drop the acknowledged arrow's identity predicate.
func TestMQTTAcknowledgementTerminalCleanupLeavesASuccessorRacingTheCAS(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-cas-successor")
	var loads, installed atomic.Int64
	var succAcq uint64
	te.asm.mu.Lock()
	te.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		cur, smsg, err := te.asm.jsa.loadInflightRecord(subject)
		if subject != te.subject || err != nil || cur == nil || smsg == nil {
			return cur, smsg, err
		}
		// Load 1 is the continuation's own identity check; load 2 is the CAS's base.
		if loads.Add(1) == 2 && installed.CompareAndSwap(0, 1) {
			succ := *cur
			succ.Acq = cur.Acq + 1
			succAcq = succ.Acq
			if _, werr := te.asm.jsa.storeInflightCAS(subject, smsg.Sequence, &succ); werr != nil {
				return cur, smsg, err
			}
			return te.asm.jsa.loadInflightRecord(subject)
		}
		return cur, smsg, err
	}
	te.asm.mu.Unlock()

	te.acknowledge(t, false)

	deadline := time.Now().Add(10 * time.Second)
	for {
		if reserved, _ := te.identifierHeld(); !reserved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the continuation never discharged its own obligation")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if installed.Load() == 0 {
		t.Fatal("the fixture never reached the CAS's own load: no successor was installed")
	}
	after := te.slot()
	if after == nil {
		t.Fatal("the successor was DELETED: terminal authority was recorded for a transmission the acknowledgement never named")
	}
	if after.Acq != succAcq || after.State != mqttInflightPublish {
		t.Fatalf("the successor must be untouched, got %+v (want Acq %d, still publish)", after, succAcq)
	}
}

// AN ACKNOWLEDGEMENT AFTER AN ADOPTED RETRANSMISSION STILL CLEANS UP (F-098 C-I, holistic
// review round 5, finding 1). A retransmission acquires the action again — the counter advances
// on the record — and its CAS response is lost. The section adopts the landed token for
// cleanup, but if the ENTRY keeps the pre-acquisition counter, the PUBACK for the outstanding
// PUBLISH stamps its completion evidence with an identity the slot no longer carries: the
// terminal continuation reads its own exchange as a successor, releases the identifier hold and
// drops the cleanup, leaving a nonterminal register with nothing owed to it.
//
// Mutation M124: adopt the landed acquisition without reconciling its counter.
func TestMQTTAcknowledgementAfterAnAdoptedRetransmissionStillCleansUp(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-adopted")
	cur, smsg, err := te.asm.jsa.loadInflightRecord(te.subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("the fixture needs its register: %+v %v", cur, err)
	}
	ref := mqttSlotRefFromRecord(te.subject, smsg.Sequence, cur)
	if ref == nil {
		t.Fatal("the seeded slot must yield a reference")
	}
	before := cur.Acq

	// The retransmission's acquisition LANDS and loses its response.
	tok := mqttSendToken{token: mqttNewInflightNonce(), slot: *ref}
	lost := testLoseNextAcquisitionResponse(te.testInflightEnv)
	if te.sess.acquirePublishAction(te.c, te.pi, &tok) {
		t.Fatal("an acquisition whose response was lost must not authorize a transmission")
	}
	if !lost.Load() {
		t.Fatal("the fixture never lost an acquisition response, so nothing here is tested")
	}
	after, _, _ := te.asm.jsa.loadInflightRecord(te.subject)
	if after == nil || after.Acq != before+1 {
		t.Fatalf("the acquisition must have landed on the record: %+v", after)
	}
	// THE ADOPTION'S OWN HALF, measured before the drain can repair it: the section and its
	// entry must already name the transmission the record carries.
	if tok.slot.acq != before+1 {
		t.Fatalf("the adopted section still names counter %d, want the landed %d", tok.slot.acq, before+1)
	}
	te.sess.mu.Lock()
	entryAcq := uint64(0)
	if ack := te.sess.pendingPublish[te.pi]; ack != nil {
		entryAcq = ack.slot.acq
	}
	te.sess.mu.Unlock()
	if entryAcq != before+1 {
		t.Fatalf("the entry still stamps counter %d, want the landed %d: its own acknowledgement "+
			"would read as a successor's", entryAcq, before+1)
	}
	if !te.sess.drainPublishAction(te.c, tok) {
		t.Fatal("the adopted token must drain at the section's end")
	}

	// And now the client acknowledges the exchange.
	te.acknowledge(t, false)
	te.waitDischarged(t, "acknowledgement after an adopted retransmission")
}

// THE DRAIN RECONCILES WHAT THE ADOPTION COULD NOT (F-098 C-I, holistic review round 5,
// finding 1, second half). When the resolving read is unavailable, the adoption keeps its
// cleanup mark but learns nothing: the section and its entry still name the pre-acquisition
// counter. The section's own drain is then the first reader of the landed one, and it owes the
// same write-back — otherwise the acknowledgement that follows stamps an identity the slot no
// longer carries and its terminal cleanup is dropped.
//
// Mutation M124b: let the drain clear the token without reconciling the counter.
func TestMQTTDrainReconcilesAnUnresolvedAcquisitionsCounter(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-drain-reconcile")
	cur, smsg, err := te.asm.jsa.loadInflightRecord(te.subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("the fixture needs its register: %+v %v", cur, err)
	}
	ref := mqttSlotRefFromRecord(te.subject, smsg.Sequence, cur)
	if ref == nil {
		t.Fatal("the seeded slot must yield a reference")
	}
	before := cur.Acq

	tok := mqttSendToken{token: mqttNewInflightNonce(), slot: *ref}
	lost := testLoseNextAcquisitionResponse(te.testInflightEnv)
	var reading atomic.Bool
	reading.Store(true)
	te.asm.mu.Lock()
	load := te.asm.testInflightLoad
	te.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
		// Only AFTER the acquisition has landed: its own pre-read must succeed.
		if lost.Load() && reading.Load() && subj == te.subject {
			return nil, nil, errors.New("test: the resolving read is unavailable")
		}
		if load != nil {
			return load(subj)
		}
		return te.asm.jsa.loadInflightRecord(subj)
	}
	te.asm.mu.Unlock()

	if te.sess.acquirePublishAction(te.c, te.pi, &tok) {
		t.Fatal("an acquisition whose response was lost must not authorize a transmission")
	}
	if !lost.Load() {
		t.Fatal("the fixture never lost an acquisition response, so nothing here is tested")
	}
	if !tok.actionHeld {
		t.Fatal("an indeterminate resolution must KEEP the section's cleanup ownership")
	}
	if tok.slot.acq != before {
		t.Fatalf("the fixture must leave the section STALE (counter %d), or the drain's half is "+
			"not what is being tested", before)
	}

	reading.Store(false) // storage recovers, and the drain is the first to read the landed counter
	if !te.sess.drainPublishAction(te.c, tok) {
		t.Fatal("the adopted token must drain once storage is back")
	}
	te.sess.mu.Lock()
	entryAcq := uint64(0)
	if ack := te.sess.pendingPublish[te.pi]; ack != nil {
		entryAcq = ack.slot.acq
	}
	te.sess.mu.Unlock()
	if entryAcq != before+1 {
		t.Fatalf("the entry still stamps counter %d, want the landed %d", entryAcq, before+1)
	}

	te.acknowledge(t, false)
	te.waitDischarged(t, "acknowledgement after a drain-reconciled acquisition")
}

// AN ACKNOWLEDGEMENT PARKED ON AN UNRESOLVED SECTION BINDS LATER, NOT WRONGLY (F-098, holistic
// review round 6, finding 1). A retransmission's acquisition lands and loses its response, and
// reads stay unavailable through the section's end: the drain cannot resolve, so the entry the
// settlement untracks may already be one counter behind the record. Binding the acknowledged
// cleanup there stamps an identity the slot has moved past — the continuation then reads its own
// exchange as a successor, releases the identifier hold and drops the obligation, and the later
// drain has no resident entry left to repair.
//
// The obligation therefore rides the TOKEN until whoever resolves it — here the shutdown's own
// final pass — binds it to the record in hand.
//
// Mutation M128: bind the acknowledged cleanup at the settlement regardless of resolution.
func TestMQTTParkedAcknowledgementBindsWhenTheActionResolves(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-unresolved-park")
	tok := te.openSection(t, te.pi, te.dur, te.sseq)
	lost := testLoseNextAcquisitionResponse(te.testInflightEnv)
	var reading atomic.Bool
	reading.Store(true)
	te.asm.mu.Lock()
	load := te.asm.testInflightLoad
	te.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
		// Only AFTER the acquisition has landed: its own pre-read must succeed.
		if lost.Load() && reading.Load() && subj == te.subject {
			return nil, nil, errors.New("test: reads are unavailable")
		}
		if load != nil {
			return load(subj)
		}
		return te.asm.jsa.loadInflightRecord(subj)
	}
	te.asm.mu.Unlock()

	if te.sess.acquirePublishAction(te.c, te.pi, &tok) {
		t.Fatal("an acquisition whose response was lost must not authorize a transmission")
	}
	if !lost.Load() {
		t.Fatal("the fixture never lost an acquisition response, so nothing here is tested")
	}

	// The client acknowledges while the section is still open, so the settlement is parked.
	if err := te.c.mqttProcessPublishReceived(te.pi, false, 0); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	te.sess.endPublishSend(te.c, te.pi, tok, mqttIntentNone, false)

	if reserved, _ := te.identifierHeld(); !reserved {
		t.Fatal("an acknowledgement whose cleanup is not yet bound must keep its identifier")
	}
	if slot := te.slot(); slot == nil {
		t.Fatal("the register must survive an unresolved section: nothing has confirmed its delete")
	}

	reading.Store(false)    // storage recovers...
	te.s.finishOwedDrains() // ...and the final pass resolves the token that carries the obligation
	te.waitDischarged(t, "acknowledgement parked on an unresolved section")
}

// A PRESERVED REWRITE IS NOT A CONFIRMED RELEASE (F-098, holistic review round 6, finding 4).
// recoverPublishTerminalSlot used the BOOLEAN reap wrapper, which reports success for a
// same-identity rewrite — and an exact-sequence delete reads an ALREADY-ABSENT sequence as
// success too, so a rewrite that outran the delete looked identical to a clean one. Reporting
// success there released the identifier hold and scheduled nothing, leaving a `publish_terminal`
// record with no continuation at all.
//
// Nothing is stubbed: the rewrite is a real CAS at a new sequence, and the caller is given the
// sequence it observed BEFORE it — which is exactly the stale view the schedule produces.
//
// Mutation M131: report success from the boolean wrapper again.
func TestMQTTRecoverPublishTerminalSlotRefusesAPreservedRewrite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		rewrite         bool
		rewriteAtDelete bool
		wantClosed      bool
	}{
		{name: "a same-identity rewrite outran the delete", rewrite: true},
		// The one the classified outcome alone cannot catch: the rewrite lands INSIDE the
		// gate, between its load and its delete, so the exact-sequence delete finds its
		// sequence already absent and reports success. Only re-reading the subject tells
		// that apart from a clean release.
		{name: "a rewrite landed between the load and the delete", rewriteAtDelete: true},
		{name: "the subject is genuinely clear", wantClosed: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := testNewTerminalEnv(t, "terminal-rewrite-"+strings.ReplaceAll(tc.name, " ", "")[:12])
			cur, smsg, err := te.asm.jsa.loadInflightRecord(te.subject)
			if err != nil || cur == nil || smsg == nil {
				t.Fatalf("the fixture needs its register: %+v %v", cur, err)
			}
			terminal := *cur
			terminal.State = mqttInflightPublishTerminal
			observed, serr := te.asm.jsa.storeInflightCAS(te.subject, smsg.Sequence, &terminal)
			if serr != nil {
				t.Fatalf("writing the terminal phase: %v", serr)
			}
			if tc.rewrite {
				// A retirement mark or custody write of the SAME exchange lands between the
				// caller's observation and its delete: same identity, new sequence.
				// The same record at a NEW sequence: what a custody write or a mark looks
				// like from here. The body is unchanged on purpose — the identity is what
				// this test is about, not the content.
				moved := terminal
				if _, werr := te.asm.jsa.storeInflightCAS(te.subject, observed, &moved); werr != nil {
					t.Fatalf("installing the rewrite: %v", werr)
				}
			}

			if tc.rewriteAtDelete {
				// The rewrite lands while the gate is between its load and its delete.
				var once atomic.Bool
				te.asm.mu.Lock()
				te.asm.testInflightDelete = func(seq uint64) error {
					if once.CompareAndSwap(false, true) {
						moved := terminal
						if _, werr := te.asm.jsa.storeInflightCAS(te.subject, observed, &moved); werr != nil {
							t.Errorf("installing the interleaved rewrite: %v", werr)
						}
					}
					return te.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
				}
				te.asm.mu.Unlock()
			}

			got := te.sess.recoverPublishTerminalSlot(te.c, te.subject, observed, &terminal)
			if got != tc.wantClosed {
				t.Fatalf("recoverPublishTerminalSlot = %v, want %v: a record still occupying the "+
					"subject under our own identity is not a confirmed release", got, tc.wantClosed)
			}
			after := te.slot()
			if (tc.rewrite || tc.rewriteAtDelete) && after == nil {
				t.Fatal("the rewritten record was deleted on a stale sequence")
			}
			if !tc.rewrite && !tc.rewriteAtDelete && after != nil {
				t.Fatalf("the register survived a confirmed release: %+v", after)
			}
		})
	}
}

// AN UNREADABLE OCCUPANT IS NOT A CLEARED SUBJECT (F-098, holistic review round 7, finding 4).
// The loader reports a present-but-unattributable record as a nil record with a non-nil stored
// message, and the post-delete confirmation read treated that as proof the subject was clear:
// the identifier hold was released and no continuation was scheduled, over a record a rewrite
// may have left racing the delete.
//
// Mutation M133: read an unreadable occupant as a cleared subject again.
func TestMQTTRecoverPublishTerminalSlotRefusesAnUnreadableConfirmation(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "terminal-unreadable")
	cur, smsg, err := te.asm.jsa.loadInflightRecord(te.subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("the fixture needs its register: %+v %v", cur, err)
	}
	terminal := *cur
	terminal.State = mqttInflightPublishTerminal
	observed, serr := te.asm.jsa.storeInflightCAS(te.subject, smsg.Sequence, &terminal)
	if serr != nil {
		t.Fatalf("writing the terminal phase: %v", serr)
	}

	// After the delete lands, the subject reads as PRESENT BUT UNREADABLE.
	var gone atomic.Bool
	te.asm.mu.Lock()
	te.asm.testInflightDelete = func(seq uint64) error {
		derr := te.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
		gone.Store(true)
		return derr
	}
	te.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
		if subj == te.subject && gone.Load() {
			return nil, &StoredMsg{Sequence: observed + 1, Subject: subj}, nil
		}
		return te.asm.jsa.loadInflightRecord(subj)
	}
	te.asm.mu.Unlock()

	if te.sess.recoverPublishTerminalSlot(te.c, te.subject, observed, &terminal) {
		t.Fatal("an unreadable occupant was read as a confirmed release: the identifier hold " +
			"would be freed and no continuation scheduled over a record nothing can interpret")
	}
}

// testUnresolvedSection opens a section, lets its acquisition LAND and lose its response, and
// keeps reads unavailable so the section's own drain cannot resolve it. It returns the token
// and a func that restores reads.
func testUnresolvedSection(t *testing.T, te *testTerminalEnv) (mqttSendToken, func()) {
	t.Helper()
	tok := te.openSection(t, te.pi, te.dur, te.sseq)
	lost := testLoseNextAcquisitionResponse(te.testInflightEnv)
	var reading atomic.Bool
	reading.Store(true)
	te.asm.mu.Lock()
	load := te.asm.testInflightLoad
	te.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
		if lost.Load() && reading.Load() && subj == te.subject {
			return nil, nil, errors.New("test: reads are unavailable")
		}
		if load != nil {
			return load(subj)
		}
		return te.asm.jsa.loadInflightRecord(subj)
	}
	te.asm.mu.Unlock()
	if te.sess.acquirePublishAction(te.c, te.pi, &tok) {
		t.Fatal("an acquisition whose response was lost must not authorize a transmission")
	}
	if !lost.Load() {
		t.Fatal("the fixture never lost an acquisition response, so nothing here is tested")
	}
	return tok, func() { reading.Store(false) }
}

// AN ACKNOWLEDGEMENT ARRIVING AFTER THE SECTION CLOSED IS BOUND BY THE SAME CELL (F-098,
// holistic review round 7, finding 1). The token-keyed hand-off covered only acknowledgements
// parked inside an open section; one arriving afterwards took the immediate path and captured
// the entry's stale counter.
//
// Mutation M134: bypass the cell on the immediate path.
func TestMQTTAcknowledgementAfterSectionEndBindsThroughTheCell(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "cell-after-section")
	tok, restoreReads := testUnresolvedSection(t, te)
	te.sess.endPublishSend(te.c, te.pi, tok, mqttIntentNone, false)

	// The client acknowledges AFTER the section closed, while the acquisition is unresolved.
	if err := te.c.mqttProcessPublishReceived(te.pi, false, 0); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	if slot := te.slot(); slot == nil {
		t.Fatal("nothing may have deleted the register while the acquisition was unresolved")
	}
	restoreReads()
	te.s.finishOwedDrains() // the resolver publishes, and its publication claims the cleanup
	te.waitDischarged(t, "acknowledgement after section end")
}

// A RESOLUTION THAT COMPLETES FIRST IS NOT LOST (F-098, holistic review round 7, finding 2).
// The hand-off had a lost wakeup: the resolver could finish between the failed section-end
// drain and the registration, leaving the registration with nothing to consume it. The cell
// retains the result instead, so the LATE registrant claims the continuation.
//
// Mutation M135: drop the claim from the registration side.
func TestMQTTResolutionBeforeRegistrationStillRunsTheCleanup(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "cell-resolve-first")
	tok, restoreReads := testUnresolvedSection(t, te)
	te.sess.endPublishSend(te.c, te.pi, tok, mqttIntentNone, false)

	// The resolver runs BEFORE any acknowledgement registers.
	restoreReads()
	te.s.finishOwedDrains()

	if err := te.c.mqttProcessPublishReceived(te.pi, false, 0); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	// PROMPTLY, and the bound is what makes this test about the REGISTRANT's claim: the
	// resolver has already run and published, so if registration does not claim the
	// continuation, the only thing left that would is the drain reconciler's next attempt —
	// a whole mqttActionReconcileDelay away. A generous window would accept that and prove
	// nothing about the pair.
	deadline := time.Now().Add(mqttActionReconcileDelay / 2)
	for {
		reserved, _ := te.identifierHeld()
		if te.slot() == nil && !reserved {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the acknowledgement did not claim the continuation its resolution had " +
				"already made runnable: the cleanup waited for another resolver pass")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// THE CONTINUATION IS CLAIMED EXACTLY ONCE (F-098, Codex's design ruling). Both publishers
// write to the same cell, and either order completes the pair — but only one of them may run
// the cleanup, or an acknowledged exchange gets two continuations racing the same delete.
//
// Mutation M138: drop the claimed guard.
func TestAckResolutionClaimsTheContinuationExactlyOnce(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		order []string
	}{
		{name: "resolution then registration", order: []string{"publish", "register", "publish", "register"}},
		{name: "registration then resolution", order: []string{"register", "publish", "register", "publish"}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess := &mqttSession{}
			ref := mqttInflightSlotRef{
				name: mqttInflightName{pi: 7}, nonce: "xxxxxxxxxxxxxxxxxxxxxx",
				dur: "SRC", sseq: 41, acq: 1,
			}
			ref.name.subject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
			res := &mqttAckResolution{ref: ref}
			runs := 0
			sess.mu.Lock()
			const attempt = "this-sections-token"
			sess.markAckResolutionPendingLocked(res, attempt)
			for _, step := range tc.order {
				var run bool
				if step == "publish" {
					run, _, _ = sess.publishAckResolutionLocked(res, ref, true, attempt)
				} else {
					run, _ = sess.registerAckCleanupLocked(res, 7)
				}
				if run {
					runs++
				}
			}
			sess.mu.Unlock()
			if runs != 1 {
				t.Fatalf("the continuation was claimed %d times, want exactly 1", runs)
			}
		})
	}
}

// A FOREIGN SUCCESSOR IS NEVER TERMINALIZED ON OUR ACKNOWLEDGEMENT (F-098 C-I, holistic review
// round 7, finding 3). The drain can find a DIFFERENT token on the slot — a peer acquired it —
// and that observation says nothing about which acquisition we owned. Adopting its counter let
// a predecessor's acknowledgement terminalize the successor; the cleanup must instead discharge
// by releasing its OWN hold, leaving the successor untouched.
//
// Mutation M136: publish the observed identity when the token is foreign.
func TestMQTTForeignSuccessorIsNotTerminalizedByOurAcknowledgement(t *testing.T) {
	t.Parallel()
	for _, drained := range []bool{false, true} {
		drained := drained
		name := "the peer's action is live"
		if drained {
			name = "the peer has already drained"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testForeignSuccessorSurvives(t, drained)
		})
	}
}

// testForeignSuccessorSurvives is the body of the case above, run with the peer's action live
// and with it already DRAINED. The drained variant is the one Codex's ruling turned on: no live
// token is present, so nothing but C-I's evidence binding stands between a predecessor's
// acknowledgement and a successor the client may still owe an answer for — the failed-PUBREC
// schedule.
func testForeignSuccessorSurvives(t *testing.T, peerDrained bool) {
	t.Helper()
	suffix := "live"
	if peerDrained {
		suffix = "drained"
	}
	te := testNewTerminalEnv(t, "cell-foreign-succ-"+suffix)
	tok, restoreReads := testUnresolvedSection(t, te)
	te.sess.endPublishSend(te.c, te.pi, tok, mqttIntentNone, false)
	if err := te.c.mqttProcessPublishReceived(te.pi, false, 0); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}

	// A PEER acquires the same allocation: same subject and nonce, a higher counter, and a
	// token that is not ours.
	cur, smsg, err := te.asm.jsa.loadInflightRecord(te.subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("reading the slot: %+v %v", cur, err)
	}
	peer := *cur
	peer.Acq++
	if peerDrained {
		// ALREADY DRAINED: no token at a higher counter. Nothing but C-I's evidence binding
		// stands between our acknowledgement and this transmission, which is the schedule
		// Codex's ruling turned on — the client may still owe an answer for it.
		peer.ActionToken, peer.ActionClaim, peer.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
	} else {
		peer.ActionToken = mqttNewInflightNonce()
		peer.ActionClaim, peer.ActionClaimSeq = peer.Claim, peer.ClaimSeq
	}
	if _, werr := te.asm.jsa.storeInflightCAS(te.subject, smsg.Sequence, &peer); werr != nil {
		t.Fatalf("installing the peer's acquisition: %v", werr)
	}

	restoreReads()
	te.s.finishOwedDrains() // our drain observes a FOREIGN token and publishes nothing

	deadline := time.Now().Add(10 * time.Second)
	for {
		reserved, _ := te.identifierHeld()
		if !reserved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("our acknowledgement's hold was never discharged against the foreign successor")
		}
		time.Sleep(20 * time.Millisecond)
	}
	after := te.slot()
	if after == nil {
		t.Fatal("the peer's transmission was DELETED on our acknowledgement")
	}
	if after.State != mqttInflightPublish || after.Acq != peer.Acq || after.ActionToken != peer.ActionToken {
		t.Fatalf("the peer's transmission was mutated by our acknowledgement: %+v", after)
	}
}

// A DRAIN PUBLISHES THE IDENTITY IT OBSERVED, NOT ONLY THE ONE IT WROTE (F-098, holistic review
// round 8). Observing our own token on the slot is itself proof of which acquisition we owned.
// If the publication waits for the clearing CAS, a CAS that LANDS and loses its response throws
// that proof away: the next pass sees a cleared token — indistinguishable from a foreign one —
// resolves without publishing, and the acknowledged cleanup then binds to a standing identity
// the record has already moved past, so it reads its own exchange as a successor and drops the
// obligation with storage healthy.
//
// Mutation M139: publish only from the CAS.
func TestMQTTDrainPublishesTheIdentityItObserved(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "cell-observed")
	tok, restoreReads := testUnresolvedSection(t, te)
	te.sess.endPublishSend(te.c, te.pi, tok, mqttIntentNone, false)
	if err := te.c.mqttProcessPublishReceived(te.pi, false, 0); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	restoreReads()

	// The drain's clearing CAS lands, and its response is lost.
	var lostCAS atomic.Bool
	te.asm.mu.Lock()
	store := te.asm.testInflightStore
	te.asm.testInflightStore = func(subj string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		seq, err := store(subj, expect, r)
		if subj == te.subject && err == nil && r.ActionToken == _EMPTY_ &&
			lostCAS.CompareAndSwap(false, true) {
			return 0, errors.New("test: the drain's CAS landed but its response was lost")
		}
		return seq, err
	}
	te.asm.mu.Unlock()

	te.s.finishOwedDrains()
	if !lostCAS.Load() {
		t.Fatal("the fixture never lost the drain's CAS response, so nothing here is tested")
	}
	te.waitDischarged(t, "drain whose clearing CAS response was lost")
}

// A STALE RESOLVER MAY NOT SETTLE A NEWER ATTEMPT (F-098, holistic review round 9, finding 1).
// The cell is shared across sections, so an older section's drain can resume long after a NEW
// section has marked the cell pending for its own acquisition. Clearing `pending` on that older
// proof lets an acknowledgement bind to an identity the newer acquisition has already moved
// past — and once the continuation is claimed, the newer publication cannot restart it.
//
// Mutation M140: clear pending regardless of which attempt published.
func TestAckResolutionIgnoresAStaleResolver(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{}
	ref := mqttInflightSlotRef{
		name: mqttInflightName{pi: 7}, nonce: "xxxxxxxxxxxxxxxxxxxxxx",
		dur: "SRC", sseq: 41, acq: 1,
	}
	ref.name.subject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
	res := &mqttAckResolution{ref: ref}
	const older, newer = "older-sections-token", "newer-sections-token"

	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.markAckResolutionPendingLocked(res, older)
	sess.markAckResolutionPendingLocked(res, newer) // a new section takes over the exchange

	// The OLDER section's drain finally publishes what it saw.
	stale := ref
	stale.acq = 1
	if run, _, _ := sess.publishAckResolutionLocked(res, stale, true, older); run {
		t.Fatal("a stale resolver claimed the continuation")
	}
	if !res.pending {
		t.Fatal("a stale resolver settled the newer attempt: an acknowledgement would now bind " +
			"to an identity that attempt may already have advanced past")
	}
	if run, _ := sess.registerAckCleanupLocked(res, 7); run {
		t.Fatal("the acknowledgement claimed a cleanup while the newer attempt was unresolved")
	}

	// The NEWER attempt resolves, and only then is the cleanup runnable.
	landed := ref
	landed.acq = 2
	run, pi, bound := sess.publishAckResolutionLocked(res, landed, true, newer)
	if !run || pi != 7 {
		t.Fatalf("the newer attempt's publication must claim the waiting cleanup, got run=%v pi=%d", run, pi)
	}
	if bound.acq != landed.acq {
		t.Fatalf("the cleanup is bound to counter %d, want the newer attempt's %d", bound.acq, landed.acq)
	}
}

// A PROVEN ABSENCE RESOLVES THE CELL (F-098, holistic review round 9, finding 2). When the
// register disappears between the acquisition's read and its CAS, the resolving read proves
// absence — sufficient resolution evidence. The arm used to clear the section's cleanup mark and
// return, which ALSO stopped the section's drain (it refuses an unheld token), so nothing was
// enrolled anywhere: an acknowledgement registering against the still-pending cell held its
// identifier with no delivery, timer or shutdown pass able to reach it.
//
// Mutation M141: return from the absence arm without resolving the cell.
func TestMQTTAbsentRegisterResolvesTheAcknowledgementCell(t *testing.T) {
	t.Parallel()
	te := testNewTerminalEnv(t, "cell-absent")
	tok := te.openSection(t, te.pi, te.dur, te.sseq)

	// The register disappears between the acquisition's read and its CAS.
	var vanished atomic.Bool
	te.asm.mu.Lock()
	store := te.asm.testInflightStore
	te.asm.testInflightStore = func(subj string, expect uint64, r *mqttInflightRecord) (uint64, error) {
		if subj == te.subject && r.ActionToken != _EMPTY_ && vanished.CompareAndSwap(false, true) {
			if derr := te.asm.jsa.deleteMsg(mqttInflightStreamName, expect, true); derr != nil {
				t.Errorf("removing the register: %v", derr)
			}
			return 0, errors.New("test: the acquisition met a subject that had just been emptied")
		}
		return store(subj, expect, r)
	}
	te.asm.mu.Unlock()

	if te.sess.acquirePublishAction(te.c, te.pi, &tok) {
		t.Fatal("an acquisition whose subject is empty must not authorize a transmission")
	}
	if !vanished.Load() {
		t.Fatal("the fixture never emptied the subject, so nothing here is tested")
	}
	te.sess.endPublishSend(te.c, te.pi, tok, mqttIntentNone, false)

	// The client acknowledges. Nothing else will run: no delivery, no timer, no shutdown pass.
	if err := te.c.mqttProcessPublishReceived(te.pi, false, 0); err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	deadline := time.Now().Add(mqttActionReconcileDelay / 2)
	for {
		if reserved, _ := te.identifierHeld(); !reserved {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the acknowledgement's identifier is still held although the acquisition was " +
				"PROVEN not to have happened: nothing is left to resolve it")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
