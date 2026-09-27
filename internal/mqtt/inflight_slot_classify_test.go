//go:build mqttport_fulltests

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
	"testing"

	"github.com/nats-io/nuid"
)

// The slot classifier, arm by arm (F-105, row 1).
//
// The classifier replaces the DECISION half of storeInflightRegister's ladder, so what these
// tests have to pin is not only "each arm answers correctly" but the PRECEDENCE between arms —
// the ladder's order is load-bearing in three places, and an implementation that got every arm
// right individually and the order wrong would be a different protocol.
//
// Pure inputs, so no store and no fixtures: every fact is an argument, which is the property
// that made the extraction worth doing.

const (
	testSlotOurDur   = "$MQTT_ours"
	testSlotOurSeq   = uint64(41)
	testSlotOurNonce = "nonce-ours"
)

func testSlotIntent() mqttAllocationIntent {
	return mqttAllocationIntent{pi: 7, dur: testSlotOurDur, sseq: testSlotOurSeq, nonce: testSlotOurNonce}
}

// testSlotOccupant is a legal same-delivery occupant, which each case then varies in ONE way.
// A shared base is what makes each case's difference the thing under test.
func testSlotOccupant() *mqttInflightRecord {
	return &mqttInflightRecord{
		State: mqttInflightPublish, PI: 7, Dur: testSlotOurDur, Seq: testSlotOurSeq, QoS: 2,
		Nonce: "nonce-occupant", Lin: "nonce-occupant",
		SourceAckReply: testInflightAckSubject(testSlotOurDur, testSlotOurSeq, 1),
	}
}

func testSlotPresent(cur *mqttInflightRecord, ours bool) mqttSlotObservation {
	return mqttSlotObservation{
		resp: mqttSlotSubjectPresent, occupant: cur, seq: 900,
		subjectNamesOccupant: true, occupantIsOurs: ours,
	}
}

func TestF105SlotClassifierAnswersEveryArm(t *testing.T) {
	t.Parallel()
	want := testSlotIntent()

	cases := []struct {
		name string
		obs  mqttSlotObservation
		disp mqttSlotDisposition
	}{
		{
			name: "a free subject creates",
			obs:  mqttSlotObservation{resp: mqttSlotSubjectAbsent},
			disp: mqttSlotDispCreate,
		},
		{
			// AN INDETERMINATE READ IS NOT AN ABSENCE. Classifying it as one would create a
			// pair over an occupant nobody read.
			name: "an unresolved read refuses",
			obs:  mqttSlotObservation{resp: mqttSlotSubjectIndeterminate},
			disp: mqttSlotDispRefuse,
		},
		{
			name: "an undecodable occupant refuses",
			obs:  mqttSlotObservation{resp: mqttSlotSubjectPresent, seq: 900, subjectNamesOccupant: true},
			disp: mqttSlotDispRefuse,
		},
		{
			name: "a body naming another identifier refuses",
			obs: func() mqttSlotObservation {
				o := testSlotPresent(testSlotOccupant(), true)
				o.subjectNamesOccupant = false
				return o
			}(),
			disp: mqttSlotDispRefuse,
		},
		{
			name: "our own nonce, still ours, adopts the landed write",
			obs: func() mqttSlotObservation {
				cur := testSlotOccupant()
				cur.Nonce, cur.Lin = testSlotOurNonce, testSlotOurNonce
				return testSlotPresent(cur, true)
			}(),
			disp: mqttSlotDispAdoptLandedWrite,
		},
		{
			name: "our own nonce, claimed by a peer, refuses",
			obs: func() mqttSlotObservation {
				cur := testSlotOccupant()
				cur.Nonce, cur.Lin = testSlotOurNonce, testSlotOurNonce
				return testSlotPresent(cur, false)
			}(),
			disp: mqttSlotDispRefuseForeignClaim,
		},
		{
			name: "a pure cancellation tombstone is reaped",
			obs: func() mqttSlotObservation {
				cur := testSlotOccupant()
				cur.Cancelled = true
				return testSlotPresent(cur, true)
			}(),
			disp: mqttSlotDispReapCancelledThenRetry,
		},
		{
			name: "a terminal publish for this source is cleaned up and withdrawn",
			obs: func() mqttSlotObservation {
				cur := testSlotOccupant()
				cur.State = mqttInflightPublishTerminal
				return testSlotPresent(cur, true)
			}(),
			disp: mqttSlotDispTerminalCleanupWithdraw,
		},
		{
			name: "a retired past-PUBREC slot refuses",
			obs: func() mqttSlotObservation {
				cur := testSlotOccupant()
				cur.Retired, cur.State = true, mqttInflightPubRelIntended
				return testSlotPresent(cur, true)
			}(),
			disp: mqttSlotDispRefuse,
		},
		{
			name: "a retirement for this source is adopted",
			obs: func() mqttSlotObservation {
				cur := testSlotOccupant()
				cur.Retired = true
				return testSlotPresent(cur, true)
			}(),
			disp: mqttSlotDispAdoptRetirement,
		},
		{
			name: "a same-delivery occupant that is ours is replaced",
			obs:  testSlotPresent(testSlotOccupant(), true),
			disp: mqttSlotDispReplace,
		},
		{
			name: "a same-delivery occupant that is not ours is quarantined",
			obs:  testSlotPresent(testSlotOccupant(), false),
			disp: mqttSlotDispQuarantineForeign,
		},
		{
			name: "another delivery, complete AND proven unsent, is reaped",
			obs: func() mqttSlotObservation {
				cur := testSlotOccupant()
				cur.Dur = "$MQTT_other"
				testRegisterSourceReply(cur)
				o := testSlotPresent(cur, true)
				o.sourceComplete, o.provenUnsent = true, true
				return o
			}(),
			disp: mqttSlotDispReapCompleteThenRetry,
		},
		{
			// COMPLETION ALONE IS NOT AUTHORITY. Without proven-unsent the register may be a
			// live delivery's identity on another broker.
			name: "another delivery, complete but not proven unsent, is abandoned",
			obs: func() mqttSlotObservation {
				cur := testSlotOccupant()
				cur.Dur = "$MQTT_other"
				testRegisterSourceReply(cur)
				o := testSlotPresent(cur, true)
				o.sourceComplete = true
				return o
			}(),
			disp: mqttSlotDispAbandon,
		},
		{
			name: "another delivery, live or unknown, is abandoned",
			obs: func() mqttSlotObservation {
				cur := testSlotOccupant()
				cur.Dur = "$MQTT_other"
				testRegisterSourceReply(cur)
				return testSlotPresent(cur, true)
			}(),
			disp: mqttSlotDispAbandon,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mqttClassifyInflightSlot(tc.obs, want)
			if got.disposition != tc.disp {
				t.Fatalf("classified as %d (%s), want %d", int(got.disposition), got.why, int(tc.disp))
			}
			// THE VERDICT CARRIES THE SEQUENCE IT WAS REACHED AGAINST. Every cleanup and
			// every replacement this verdict licenses addresses that exact sequence, so a
			// verdict that dropped it would license an action at whatever the caller
			// happened to remember.
			if tc.obs.resp == mqttSlotSubjectPresent && got.seq != tc.obs.seq {
				t.Fatalf("the verdict carries sequence %d rather than the observed %d", got.seq, tc.obs.seq)
			}
		})
	}
}

// TestF105SlotClassifierPrecedenceIsTheLadders pins the three orderings that are load-bearing.
//
// Each case is a record that satisfies TWO arms at once. An implementation with every arm
// correct and the order wrong passes the table above and fails here, which is the whole reason
// these are separate.
func TestF105SlotClassifierPrecedenceIsTheLadders(t *testing.T) {
	t.Parallel()
	want := testSlotIntent()

	t.Run("a cancellation tombstone for OUR source is reaped, not replaced", func(t *testing.T) {
		// Satisfies both the cancellation arm and the same-delivery replace arm. The
		// tombstone records a delivery that was never transmitted, so our allocation IS the
		// first transmission its freeing licensed — where replacing it would OVERWRITE the
		// tombstone and destroy that evidence without a confirmed delete.
		cur := testSlotOccupant()
		cur.Cancelled = true
		got := mqttClassifyInflightSlot(testSlotPresent(cur, true), want)
		if got.disposition != mqttSlotDispReapCancelledThenRetry {
			t.Fatalf("a cancellation tombstone for our own source classified as %d (%s)",
				int(got.disposition), got.why)
		}
	})

	t.Run("a RETIRED cancellation tombstone is not reaped", func(t *testing.T) {
		// The precedence rule's other half: a record carrying BOTH was transmitted, so
		// reaping it would free an identifier the client holds.
		cur := testSlotOccupant()
		cur.Cancelled, cur.Retired = true, true
		got := mqttClassifyInflightSlot(testSlotPresent(cur, true), want)
		if got.disposition == mqttSlotDispReapCancelledThenRetry {
			t.Fatalf("a retired tombstone was reaped, freeing an identifier the client holds")
		}
		if got.disposition != mqttSlotDispAdoptRetirement {
			t.Fatalf("a retired tombstone classified as %d (%s)", int(got.disposition), got.why)
		}
	})

	t.Run("a terminal publish is cleaned up even when RETIRED is set", func(t *testing.T) {
		// Satisfies both the terminal arm and the retirement arm. A completed exchange is
		// not a retirement to adopt: adopting it would install a source suppression for an
		// exchange that is already complete.
		cur := testSlotOccupant()
		cur.State, cur.Retired = mqttInflightPublishTerminal, true
		got := mqttClassifyInflightSlot(testSlotPresent(cur, true), want)
		if got.disposition != mqttSlotDispTerminalCleanupWithdraw {
			t.Fatalf("a terminal publish carrying a retirement classified as %d (%s)",
				int(got.disposition), got.why)
		}
	})

	t.Run("our own nonce wins over every body-derived arm", func(t *testing.T) {
		// Satisfies the cancellation arm too. The nonce identifies the ALLOCATION, and
		// markInflightRegisterCancelledPending can write a sparse record carrying our nonce —
		// so the lost-response arm must be reached before the body is classified.
		cur := testSlotOccupant()
		cur.Nonce, cur.Lin, cur.Cancelled = testSlotOurNonce, testSlotOurNonce, true
		got := mqttClassifyInflightSlot(testSlotPresent(cur, true), want)
		if got.disposition != mqttSlotDispAdoptLandedWrite {
			t.Fatalf("our own landed write classified as %d (%s)", int(got.disposition), got.why)
		}
	})
}

// TestF105SlotMarkerIsAskedForExactlyOneDisposition pins which arm needs the second read.
//
// Reading the marker for an arm that does not need it is a wasted round trip; NOT reading it
// for the arm that does is the defect where a batch's shape gets taken for the store's answer,
// which a mutation found in the replacement boundary.
func TestF105SlotMarkerIsAskedForExactlyOneDisposition(t *testing.T) {
	t.Parallel()
	all := []mqttSlotDisposition{
		mqttSlotDispRefuse, mqttSlotDispCreate, mqttSlotDispReplace,
		mqttSlotDispAdoptLandedWrite, mqttSlotDispRefuseForeignClaim,
		mqttSlotDispReapCancelledThenRetry, mqttSlotDispTerminalCleanupWithdraw,
		mqttSlotDispAdoptRetirement, mqttSlotDispQuarantineForeign,
		mqttSlotDispReapCompleteThenRetry, mqttSlotDispAbandon,
	}
	for _, d := range all {
		if got, wantNeeds := mqttSlotDispositionNeedsMarker(d), d == mqttSlotDispReplace; got != wantNeeds {
			t.Fatalf("disposition %d reports needing a marker = %v, want %v", int(d), got, wantNeeds)
		}
	}
}

// TestF105SlotClassifierDrivesTheLiveLadder drives the REAL storeInflightRegister through one
// planted occupant per shape and asserts the observable outcome.
//
// Its predecessor compared the arm the ladder took against the disposition the classifier
// predicted, which is what earned the classifier the right to replace those predicates. Now
// that it HAS replaced them the comparison would be the verdict against itself, so it is gone
// rather than kept as a test that cannot fail. What is left is the part that still carries
// information: the ladder's effect on durable state and on the caller.
//
// Every case asserts the store DECLINED — none of these shapes may transmit — and that the
// planted occupant is untouched where the disposition says it must be.
func TestF105SlotClassifierDrivesTheLiveLadder(t *testing.T) {
	drive := func(t *testing.T, clientID string, plant func(e *testInflightEnv, subject string, pi uint16)) (*testInflightEnv, string, bool) {
		t.Helper()
		e := testNewInflightEnv(t, clientID)
		dur := e.sess.idHash + "_" + nuid.Next()

		e.sess.mu.Lock()
		pi, _ := e.sess.trackPublish(dur, testInflightAckSubject(dur, 7700, 1))
		nonce := e.sess.pendingPublish[pi].regNonce
		e.sess.mu.Unlock()
		if pi == 0 {
			t.Fatalf("the fixture allocated no identifier")
		}
		subject := e.regSubject(pi)
		plant(e, subject, pi)
		return e, subject, e.sess.storeInflightRegister(e.c, pi, 2, dur, 7700, nonce, _EMPTY_, _EMPTY_)
	}

	t.Run("an unreadable occupant is refused and left alone", func(t *testing.T) {
		var planted uint64
		e, subject, ok := drive(t, "slot-live-unreadable", func(e *testInflightEnv, subject string, pi uint16) {
			if _, err := e.asm.jsa.storeMsg(subject, -1, []byte("not a register")); err != nil {
				t.Fatalf("planting the unreadable occupant failed: %v", err)
			}
			smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
			if lerr != nil || smsg == nil {
				t.Fatalf("the planted occupant does not load back: %v", lerr)
			}
			planted = smsg.Sequence
		})
		if ok {
			t.Fatalf("the store transmitted under an identifier an unreadable record occupies")
		}
		// UNTOUCHED. An unreadable record cannot be attributed to any delivery, so it cannot
		// be proven stale — erasing it is how a corrupted record becomes a reused identifier.
		smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if err != nil || smsg == nil || smsg.Sequence != planted {
			t.Fatalf("the refusal moved the unreadable occupant")
		}
	})

	t.Run("a body naming another identifier is refused and left alone", func(t *testing.T) {
		var planted uint64
		e, subject, ok := drive(t, "slot-live-mismatch", func(e *testInflightEnv, subject string, pi uint16) {
			seq, err := e.asm.jsa.storeInflightCAS(subject, 0, freshRegisterLineage(&mqttInflightRecord{
				State: mqttInflightPublish, PI: pi + 1, Dur: e.sess.idHash + "_x", Seq: 7700, QoS: 2,
				Nonce: mqttNewInflightNonce(),
			}))
			if err != nil {
				t.Fatalf("planting the mismatched occupant failed: %v", err)
			}
			planted = seq
		})
		if ok {
			t.Fatalf("the store transmitted under an identifier a mismatched record occupies")
		}
		smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if err != nil || smsg == nil || smsg.Sequence != planted {
			t.Fatalf("the refusal moved the mismatched occupant")
		}
	})

	t.Run("a live occupant for another delivery is refused and never reaped", func(t *testing.T) {
		var planted uint64
		e, subject, ok := drive(t, "slot-live-other", func(e *testInflightEnv, subject string, pi uint16) {
			seq, err := e.asm.jsa.storeInflightCAS(subject, 0, freshRegisterLineage(&mqttInflightRecord{
				State: mqttInflightPublish, PI: pi, Dur: e.sess.idHash + "_live", Seq: 9100, QoS: 2,
				Nonce: mqttNewInflightNonce(),
			}))
			if err != nil {
				t.Fatalf("planting the live occupant failed: %v", err)
			}
			planted = seq
		})
		if ok {
			t.Fatalf("the store transmitted under an identifier another delivery holds")
		}
		// NEVER REAPED: on another broker this record is that delivery's only durable
		// identity, and destroying it strands the identifier the client is holding.
		smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if err != nil || smsg == nil || smsg.Sequence != planted {
			t.Fatalf("a live occupant for another delivery was reaped")
		}
	})
}
