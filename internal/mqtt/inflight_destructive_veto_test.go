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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// C-IV ARM 2 AT THE ONE DESTRUCTIVE DOOR (F-098, holistic review round 1, finding 0).
//
// Any live action withholds every destructive disposition, OUR OWN TOKEN INCLUDED. A holder
// whose write has not returned may still act on the record, so nothing may remove it from
// under that holder; the holder's own drain completes the cleanup from the phase it observes.
// Deleting the `pubrel_terminal_draining` phase without moving this guard to the delete door
// was the regression: PUBCOMP advanced a live-token slot to `pubrel_terminal` and terminal
// cleanup deleted it.
//
// Each row seeds a legal slot for its disposition and asks the gated door to delete it twice:
// with a token (must refuse and leave the slot) and without (must delete). The token-free half
// is what makes the refusal mean something — a fixture whose delete never works at all would
// pass the first half for the wrong reason.
//
// Mutation M91: drop the live-token clause from reapRegisterGatedOutcome. Every "with a token"
// half then deletes.
func TestMQTTInflightDestructiveDispositionsRefuseALiveAction(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		state mqttInflightState
		disp  mqttInflightDisposition
		// unsent seeds this row's slot with its OWN unsent marker, which is what the two
		// `publish`-phase dispositions require before any delete is licensed at all
		// (C-IV arm 4). The terminal rows need none: a durable terminal phase is positive
		// authority in its own right (arm 3) and is reached whatever the marker says.
		unsent bool
	}{
		{name: "terminal cleanup of a PUBREL exchange", state: mqttInflightPubRelTerminal, disp: mqttSlotDeleteTerminal},
		{name: "terminal cleanup of a PUBLISH exchange", state: mqttInflightPublishTerminal, disp: mqttSlotDeletePublishTerminal},
		// RENAMED, because the old name described a slot this broker no longer writes: an
		// ACKNOWLEDGED PUBLISH is recorded `publish_terminal` and released under its
		// terminal disposition, which the row above already covers. `DeleteComplete` keeps
		// its coverage here as what it actually is now — a complete-delete of an exchange
		// PROVEN never to have transmitted.
		{name: "an unsent PUBLISH's complete-delete", state: mqttInflightPublish, disp: mqttSlotDeleteComplete, unsent: true},
		{name: "a never-transmitted release", state: mqttInflightPublish, disp: mqttSlotDeleteNeverTransmitted, unsent: true},
	} {
		// "Ours" and "a peer's" are the SAME shape on the record — the evidence carries no
		// owner term (TestSlotEvidenceCannotIdentifyATokensOwner) — so both rows exist to
		// document that the arm does not care, not because they differ.
		for _, owner := range []string{"our own", "a peer's"} {
			tc, owner := tc, owner
			t.Run(tc.name+" under "+owner+" token", func(t *testing.T) {
				t.Parallel()
				e := testNewInflightEnv(t, "veto")
				pi := uint16(5)
				var subject string
				if tc.state == mqttInflightPublishTerminal {
					// An ordinary terminal slot needs its terminal exchange and a settled
					// source before the live-action guard can be tested independently.
					dur := e.sess.idHash + "_terminal_veto"
					sseq, reply := e.publishOfferedSource(t, dur)
					pi = e.trackAdmitted(t, dur, sseq, 2, reply)
					subject = mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
					r, m := testRegisterAt(t, e, pi)
					seq := testAdvanceTerminalPair(t, e, subject, m.Sequence, r)
					if proof := e.sess.completeSource(subject, seq, r, true); !proof.permits(subject, seq, r) {
						t.Fatal("fixture: terminal source settlement was not confirmed")
					}
					r.ActionToken = mqttNewInflightNonce()
					r.ActionClaim, r.ActionClaimSeq = r.Claim, r.ClaimSeq
					if _, err := e.asm.jsa.storeInflightCAS(subject, seq, r); err != nil {
						t.Fatalf("installing the live terminal action: %v", err)
					}
					testInstallPassThroughSeams(e)
				} else if tc.state == mqttInflightPublish {
					// REBUILT by F-097/F-088 (ruling R-impl-1): the publish rows used to seed an
					// ordinary register with NO exchange, a shape admission never writes and the
					// door now refuses (WP3 edit 2), so the positive control could not reach the
					// delete. The slot is now a REAL admission (register, exchange and unsent
					// marker in one batch) carrying a live token; every assertion is unchanged.
					dur := e.sess.idHash + "_publish_veto"
					sseq, reply := e.publishOfferedSource(t, dur)
					pi = e.trackAdmitted(t, dur, sseq, 2, reply)
					subject = mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
					r, m := testRegisterAt(t, e, pi)
					r.ActionToken = mqttNewInflightNonce()
					r.ActionClaim, r.ActionClaimSeq = r.Claim, r.ClaimSeq
					if _, err := e.asm.jsa.storeInflightCAS(subject, m.Sequence, r); err != nil {
						t.Fatalf("installing the live PUBLISH action: %v", err)
					}
					testInstallPassThroughSeams(e)
				} else {
					testReadyForPubRelDelivery(t, e)
					// The guard cannot distinguish our token from a peer's token.
					subject = testVetoSeedSlot(t, e, pi, tc.state, mqttNewInflightNonce())
				}

				cur, smsg, err := e.asm.loadInflightRecord(subject)
				if err != nil || cur == nil || smsg == nil {
					t.Fatalf("loading the seeded slot: rec=%+v err=%v", cur, err)
				}
				ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
				if ref == nil {
					t.Fatal("the seeded slot must yield a reference")
				}
				if tc.unsent {
					// Keyed by the allocation's NONCE, so it is still this slot's own
					// marker after the token is cleared below — clearing an action does
					// not put a PUBLISH on the wire. Without that retention the positive
					// control would lose its licence along with the token, and the refusal
					// above would prove nothing.
					testPlantUnsentMarkers(e.asm, map[string]string{subject: cur.Nonce})
				}
				if got := e.sess.reapRegisterGatedOutcome(smsg.Sequence, cur.Nonce, true, ref, tc.disp, mqttTestLicence(e.sess, nil)); got != mqttReapRefused {
					t.Fatalf("a live action must withhold %s, got outcome %d", tc.name, got)
				}
				if after, amsg, aerr := e.asm.loadInflightRecord(subject); aerr != nil || amsg == nil || after == nil ||
					after.ActionToken == _EMPTY_ {
					t.Fatalf("the refused delete must leave the slot and its token: rec=%+v err=%v", after, aerr)
				}

				// THE POSITIVE CONTROL: the same slot, token cleared, is deleted by the same call.
				cleared := *cur
				cleared.ActionToken, cleared.ActionClaim, cleared.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
				seq, serr := e.asm.testInflightStore(subject, smsg.Sequence, &cleared)
				if serr != nil {
					t.Fatalf("clearing the token: %v", serr)
				}
				ref2 := mqttSlotRefFromRecord(subject, seq, &cleared)
				if got := e.sess.reapRegisterGatedOutcome(seq, cleared.Nonce, true, ref2, tc.disp, mqttTestLicence(e.sess, nil)); got != mqttReapDeleted {
					t.Fatalf("with no live action the same delete must land, got outcome %d: the fixture "+
						"never reaches the delete, so the refusal above proves nothing", got)
				}
			})
		}
	}
}

// A same-source REPLACEMENT is destructive through a CAS rather than a delete, and the same
// arm binds it. Mutation M92: drop the live-token clause from casInflightSlot.
func TestMQTTInflightReplacementRefusesALiveAction(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "veto-replace")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(6)
	subject := testVetoSeedSlot(t, e, pi, mqttInflightPublish, mqttNewInflightNonce())
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the seeded slot: rec=%+v err=%v", cur, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	if ref == nil {
		t.Fatal("the seeded slot must yield a reference")
	}
	if _, ok := e.sess.casInflightSlot(e.c, *ref, mqttInflightPublish, mqttSlotReplacePublish,
		func(next *mqttInflightRecord) { next.Nonce = mqttNewInflightNonce() }); ok {
		t.Fatal("a replacement must not destroy an occupant whose action is live")
	}
	after, _, _ := e.asm.loadInflightRecord(subject)
	if after == nil || after.Nonce != cur.Nonce || after.ActionToken == _EMPTY_ {
		t.Fatalf("the occupant must be untouched, got %+v", after)
	}
}

// testVetoSeedSlot installs a legal slot in the given phase carrying a live action token, and
// returns its subject. The action's custody equals the slot's, which the validator requires.
func testVetoSeedSlot(t *testing.T, e *testInflightEnv, pi uint16, state mqttInflightState, token string) string {
	t.Helper()
	testSeedPubRelReadySlot(t, e, pi)
	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the ready slot: rec=%+v err=%v", cur, err)
	}
	next := *cur
	next.State = state
	if state == mqttInflightPublish || state == mqttInflightPublishTerminal {
		// PUBLISH-side phases carry no canonical PUBREL identity.
		next.PubRelDurable, next.PubRelStreamSeq = _EMPTY_, 0
	}
	next.ActionToken = token
	next.ActionClaim, next.ActionClaimSeq = next.Claim, next.ClaimSeq
	if verr := mqttValidateInflightShape(&next); verr != nil {
		t.Fatalf("the seeded %s slot is not a shape this broker writes: %v", state, verr)
	}
	if _, serr := e.asm.testInflightStore(subject, smsg.Sequence, &next); serr != nil {
		t.Fatalf("installing the %s slot: %v", state, serr)
	}
	return subject
}

// G1a (C13's gate for finding 0), through the production route: PUBCOMP arrives while a
// PUBREL write is genuinely blocked inside its send section, so the slot carries a LIVE
// action when terminal cleanup would run. The pre-fix tree advanced the slot to
// `pubrel_terminal` and deleted it under the holder; the fixed one advances it, KEEPS it with
// its token, keeps the identifier reserved, and lets the holder's own drain delete it once the
// write returns.
//
// Deliberately NOT parallel: the PUBREL write-block seam is process-global.
//
// Mutation M91 (drop the live-token clause from mqttSlotDestructionAdmitted, and the early
// return in finalizeTerminalSlot) fails the "kept with its token" assertion.
func TestMQTTInflightPubCompUnderALiveActionKeepsTheSlotForItsHolder(t *testing.T) {
	e := testNewInflightEnv(t, "client-terminal-held")
	// ON THE REAL STORE (consult 26): an ordinary exchange's PUBCOMP is one authority batch.
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	pi := seed.pi
	e.sess.mu.Lock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	e.sess.mu.Unlock()

	// Copy 1 goes out and its write returns.
	testDeliverPubRelAs(t, e, pi, seed.reply(1), seed.nonce)

	// Copy 2 blocks INSIDE its send section, holding a durable action on the slot.
	blocked, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	restore := setMQTTTestPubRelWriteBlock(func() {
		close(blocked)
		<-release
	})
	defer restore()
	defer func() { unblock(); <-done }()
	go func() {
		defer close(done)
		testDeliverPubRelAs(t, e, pi, seed.reply(2), seed.nonce)
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the duplicate never reached its send section, so no action was live")
	}
	if cur, _, err := e.asm.loadInflightRecord(subject); err != nil || cur == nil || cur.ActionToken == _EMPTY_ {
		t.Fatalf("the fixture must hold a LIVE durable action while the write is out: rec=%+v err=%v", cur, err)
	}

	// PUBCOMP for copy 1, through the real handler, while the action is live — with every
	// PUBREL-subject scan COUNTED. Terminal cleanup's disposal of canonical and duplicate
	// records, and its affine discharge, all begin with that scan, so a scan here is disposal
	// work started under a holder that may still transmit. The gate would still refuse the
	// final delete; only this observation catches cleanup that runs everything before it
	// (mutation M91b, which the slot assertions below cannot see).
	var disposalScans atomic.Int32
	var watching atomic.Bool
	watching.Store(true)
	restoreScan := setMQTTTestLoadNextMsgHook(func(stream, subject string, _ uint64) error {
		if watching.Load() && stream == mqttOutStreamName && strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			disposalScans.Add(1)
		}
		return nil
	})
	e.c.mqttProcessPubComp(pi)
	watching.Store(false)
	restoreScan()
	if n := disposalScans.Load(); n != 0 {
		t.Fatalf("terminal cleanup scanned the PUBREL records %d time(s) under a live action: disposal "+
			"work may not start before the holder's own drain", n)
	}

	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || smsg == nil || cur == nil {
		t.Fatalf("terminal cleanup deleted the slot under a live action: rec=%+v msg=%v err=%v", cur, smsg, err)
	}
	if cur.State != mqttInflightPubRelTerminal || cur.ActionToken == _EMPTY_ {
		t.Fatalf("the slot must be recorded terminal AND keep its holder's token, got state=%s token=%q",
			cur.State, cur.ActionToken)
	}
	if testMintsPI(t, e.sess, pi) {
		t.Fatalf("packet identifier %d was released while a PUBREL write under it was still out", pi)
	}

	// The write returns: the holder drains its own action and finishes the cleanup itself.
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocked delivery never returned")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, smsg, err := e.asm.loadInflightRecord(subject)
		if err == nil && smsg == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the holder's drain never finished the terminal cleanup: the slot is still present (err=%v)", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
