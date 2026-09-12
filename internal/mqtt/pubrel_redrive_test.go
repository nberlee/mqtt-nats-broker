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
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// The PUBREL REDRIVE WAITER (F-098): a stored PUBREL that admission refused because a
// PUBLISH transmission was still live on its slot must be woken by that transmission's own
// end, not left to AckWait.
//
// The refusal itself is correct and is NOT what these tests are about. What they are about
// is the continuation: exclusion without one is indistinguishable from a stall, and the
// stall is 30 seconds by default where a client — or a test — gives up in 10.
//
// Every test here drives registerPubRelRedrive and wakePubRelRedrive directly and observes
// the NAK on the record's OWN delivery subject, which is the only place a redrive is
// visible. The wire-level gates, which prove the windows are reachable at all from a real
// client, are the e2e fixtures in pubrel_transition_e2e_test.go.

// testRedriveAckSubject builds a delivery's reply subject with an EXPLICIT delivery count
// and consumer sequence, so a redelivery of one record is distinguishable from its first
// delivery by recency.
func testRedriveAckSubject(dur string, sseq uint64, dc, dseq int) string {
	return fmt.Sprintf("$JS.ACK.%s.%s.%d.%d.%d.0.0", mqttStreamName, dur, dc, sseq, dseq)
}

// testRedriveObserver subscribes to a delivery's reply subject so a redrive NAK on it can
// be observed — or its ABSENCE established, which is what the retention tests need.
func testRedriveObserver(t *testing.T, e *testInflightEnv, reply string) *nats.Subscription {
	t.Helper()
	obs, serr := e.s.nrc.SubscribeSync(reply)
	if serr != nil {
		t.Fatalf("subscribe to %q: %v", reply, serr)
	}
	t.Cleanup(func() { _ = obs.Unsubscribe() })
	if err := e.s.nrc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return obs
}

// testRequireRedrive requires a NAK on this delivery, which is the redrive.
func testRequireRedrive(t *testing.T, obs *nats.Subscription, why string) {
	t.Helper()
	msg, err := obs.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("%s: no redrive was issued, so the client waits out AckWait (%v)", why, err)
	}
	if !bytes.Equal(msg.Data, AckNak) {
		t.Fatalf("%s: a redrive is a NAK of the exact refused delivery, got %q", why, msg.Data)
	}
}

// testRequireNoRedrive requires that NOTHING was sent on this delivery.
//
// The interval is short deliberately: the registration's reread is synchronous, so a
// wrongly-issued NAK is already sent by the time register returns. This is not a race
// against a slow path, it is a check that a decision already taken was the right one.
func testRequireNoRedrive(t *testing.T, obs *nats.Subscription, why string) {
	t.Helper()
	if msg, err := obs.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatalf("%s: the delivery was answered %q while a blocker still stood, so the waiter "+
			"was consumed and the transmission's end will find nothing to wake", why, msg.Data)
	}
}

// testRedriveWaiters returns the waiter set for one identifier.
// publishTransmissionStandsLocalOnly is the local half alone, for the tests that are about
// the marker rather than about the combined question.
func (sess *mqttSession) publishTransmissionStandsLocalOnly(pi uint16) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.publishSectionLiveLocked(pi)
}

func testRedriveWaiters(e *testInflightEnv, pi uint16) map[mqttPubRelRedriveKey]mqttPubRelRedriveWaiter {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	out := map[mqttPubRelRedriveKey]mqttPubRelRedriveWaiter{}
	for k, v := range e.sess.pubRelRedrive[pi] {
		out[k] = v
	}
	return out
}

// testRedriveSlot seeds the slot the waiter's reread will consult, with an explicit action
// token, and returns its subject.
func testRedriveSlot(t *testing.T, e *testInflightEnv, pi uint16, token string) string {
	t.Helper()
	testSeedPubRelReadySlot(t, e, pi)
	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	if token == _EMPTY_ {
		return subject
	}
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the seeded slot: rec=%v err=%v", cur, err)
	}
	next := *cur
	next.ActionToken = token
	next.ActionClaim, next.ActionClaimSeq = cur.Claim, cur.ClaimSeq
	if _, serr := e.asm.testInflightStore(subject, smsg.Sequence, &next); serr != nil {
		t.Fatalf("installing the action token: %v", serr)
	}
	return subject
}

// R1: the REGISTRATION RACE. The transmission can end between the caller observing it and
// the registration landing, and a waiter registered after the wake has already run would
// never be woken at all — the exact stall the mechanism exists to remove, reached by the
// mechanism itself.
//
// So the state is re-read AFTER registering, and nothing standing means the wake is issued
// right there. Mutation M70: read before registering (or drop the reread) and this delivery
// is answered by nothing.
func TestMQTTPubRelRedriveWakesWhenTheTransmissionAlreadyEnded(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "redrive-r1")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(3)
	const dur = "PUBREL_F089"
	subject := testRedriveSlot(t, e, pi, _EMPTY_) // quiescent: no action token
	reply := testInflightAckSubject(dur, testCanonicalPubRelSeq, 1)
	obs := testRedriveObserver(t, e, reply)

	e.sess.registerPubRelRedrive(e.c, pi, reply, subject, dur)

	testRequireRedrive(t, obs, "the transmission had already ended when the waiter was registered")
	if got := testRedriveWaiters(e, pi); len(got) != 0 {
		t.Fatalf("a woken waiter is consumed, got %+v", got)
	}
}

// R2: EVERY BLOCKER RETAINS THE WAITER, and two of these rows are about what an UNPROVEN
// read means rather than about a blocker at all.
//
// The durable half and the local half are two halves of ONE transmission: endPublishSend
// drains the durable action and then releases the local section, and admission refuses on
// either. A reread that answered "clear" on the local half alone would consume the waiter
// in the middle of that sequence, and the wake at the end would find nothing.
//
// An indeterminate load and a PRESENT-BUT-UNREADABLE record are not absences. The loader
// reports both as a nil record, and only the stored message distinguishes them from the
// proven absence that really has no durable half.
//
// Mutations: M71 (wake on any token that is not the one observed), M72 (ignore the local
// hold), M73 (treat a nil record as quiescent without consulting the stored message).
func TestMQTTPubRelRedriveIsRetainedWhileAnyBlockerStands(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		token string
		hold  bool
		load  func(e *testInflightEnv, prev func(string) (*mqttInflightRecord, *StoredMsg, error)) func(string) (*mqttInflightRecord, *StoredMsg, error)
	}{
		{
			// A REPLACEMENT action. The one this refusal named is gone, but the slot is
			// not quiescent: waking here buys a NAK, an immediate re-offer, a fresh
			// refusal and a fresh registration, for as long as actions rotate.
			name: "a different durable action", token: "replacement-action-token",
		},
		{
			// The local half, after the durable drain landed. This is the ordinary
			// schedule, not an exotic one: endPublishSend drains and THEN releases.
			name: "this broker's own live PUBLISH send", hold: true,
		},
		{
			name: "an indeterminate load",
			load: func(e *testInflightEnv, prev func(string) (*mqttInflightRecord, *StoredMsg, error)) func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
					return nil, nil, errors.New("test: no response")
				}
			},
		},
		{
			name: "a present but unattributable record",
			load: func(e *testInflightEnv, prev func(string) (*mqttInflightRecord, *StoredMsg, error)) func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
					_, smsg, err := prev(subject)
					if smsg == nil || err != nil {
						return nil, smsg, err
					}
					// PRESENT — the stored message is real — and unreadable.
					return nil, smsg, nil
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "redrive-r2")
			testReadyForPubRelDelivery(t, e)
			const pi = uint16(3)
			const dur = "PUBREL_F089"
			subject := testRedriveSlot(t, e, pi, tc.token)
			if tc.hold {
				e.sess.mu.Lock()
				if e.sess.sendsInFlight == nil {
					e.sess.sendsInFlight = map[uint16]*mqttSendHold{}
				}
				e.sess.sendsInFlight[pi] = &mqttSendHold{pubIdent: "live-publish-send"}
				e.sess.mu.Unlock()
			}
			if tc.load != nil {
				e.asm.mu.Lock()
				prev := e.asm.testInflightLoad
				e.asm.testInflightLoad = tc.load(e, prev)
				e.asm.mu.Unlock()
			}
			reply := testInflightAckSubject(dur, testCanonicalPubRelSeq, 1)
			obs := testRedriveObserver(t, e, reply)

			e.sess.registerPubRelRedrive(e.c, pi, reply, subject, dur)

			testRequireNoRedrive(t, obs, tc.name)
			if got := testRedriveWaiters(e, pi); len(got) != 1 {
				t.Fatalf("the waiter must stand while %s, got %+v", tc.name, got)
			}
			// AND THE END STILL WAKES IT. Retention is only correct because the
			// transmission's end is a real continuation; a retained waiter nothing ever
			// wakes is the same stall by another route.
			e.sess.wakePubRelRedrive(pi)
			testRequireRedrive(t, obs, "the transmission ended")
		})
	}
}

// R3: ONE WAITER PER STORED RECORD, and every record keeps one.
//
// Several stored PUBREL records under one identifier is a supported stale-broker shape, so
// two distinct records refused in the same window must BOTH keep a continuation: a single
// slot per identifier silently abandoned one, which is the defect this whole mechanism
// exists to remove, reached one level down.
//
// A record AckWait re-offers while the blocker still stands is the SAME waiter at a NEW
// address, not a second one — and an older delivery's callback, arriving late, must not put
// the stale address back. Mutations: M74 (key by identifier alone), M75 (append every
// reply), M76 (let any registration overwrite the address regardless of delivery sequence).
func TestMQTTPubRelRedriveKeepsOneWaiterPerRecord(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "redrive-r3")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(3)
	const dur = "PUBREL_F089"
	// A LIVE action, so every registration below is retained rather than woken.
	subject := testRedriveSlot(t, e, pi, "live-action-token")

	const otherSeq = uint64(9)
	// A REDELIVERY OF ONE RECORD MOVES THE CONSUMER SEQUENCE, which the shared fixture
	// helper does not model — it writes the stream sequence into both positions, so two
	// deliveries of one record would be indistinguishable by recency and this test would
	// pass whichever address survived. Built explicitly here for that reason.
	first := testRedriveAckSubject(dur, testCanonicalPubRelSeq, 1, 11)
	reoffer := testRedriveAckSubject(dur, testCanonicalPubRelSeq, 2, 12)
	other := testRedriveAckSubject(dur, otherSeq, 1, 13)
	firstObs, reofferObs, otherObs := testRedriveObserver(t, e, first),
		testRedriveObserver(t, e, reoffer), testRedriveObserver(t, e, other)

	e.sess.registerPubRelRedrive(e.c, pi, first, subject, dur)
	e.sess.registerPubRelRedrive(e.c, pi, reoffer, subject, dur)
	e.sess.registerPubRelRedrive(e.c, pi, other, subject, dur)
	// The STALE callback, arriving after the re-offer registered: same record, an OLDER
	// delivery. It must not put the superseded address back.
	e.sess.registerPubRelRedrive(e.c, pi, first, subject, dur)

	waiters := testRedriveWaiters(e, pi)
	if len(waiters) != 2 {
		t.Fatalf("two distinct records must each keep a waiter, and one record's re-offers "+
			"must not add any, got %d: %+v", len(waiters), waiters)
	}
	canonical := waiters[mqttPubRelRedriveKey{dur: dur, sseq: testCanonicalPubRelSeq}]
	if canonical.reply != reoffer {
		t.Fatalf("a re-offered record's waiter must hold the NEWEST delivery's address, got %q",
			canonical.reply)
	}
	if waiters[mqttPubRelRedriveKey{dur: dur, sseq: otherSeq}].reply != other {
		t.Fatalf("the second record's waiter must hold its own address, got %+v", waiters)
	}

	e.sess.wakePubRelRedrive(pi)
	// BOTH records are re-offered, and the superseded address is not.
	testRequireRedrive(t, reofferObs, "the canonical record's newest delivery")
	testRequireRedrive(t, otherObs, "the second record's delivery")
	testRequireNoRedrive(t, firstObs, "a superseded delivery address")
	if got := testRedriveWaiters(e, pi); len(got) != 0 {
		t.Fatalf("a wake consumes every waiter for the identifier, got %+v", got)
	}
}

// R4: A PUBREL ACTION'S OWN END WAKES TOO, and it is a different event from a send
// section's end.
//
// Admission registers a waiter against ANY live action token, a PUBREL's included, and a
// PUBREL action outlives the section that acquired it: endPublishSend never runs for it. A
// waiter left behind one would sit until something unrelated happened to wake it, which is
// the AckWait silence one level over from the one the mechanism removes.
//
// Mutation M82: retire the action without waking (call retireActionLocked_ directly at the
// drain's proven-completion arms).
func TestMQTTPubRelRedriveWakesWhenAPubRelActionDrains(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "redrive-r4")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(25)
	const dur = "PUBREL_F089"
	testSeedPubRelReadySlot(t, e, pi)

	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("loading the ready slot: rec=%+v msg=%+v err=%v", rec, smsg, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, rec)
	if ref == nil {
		t.Fatal("the ready slot must yield a reference")
	}
	act := e.sess.mqttAcquirePubRelAction(e.c, *ref)
	if act == nil {
		t.Fatal("acquiring the fixture's PUBREL action")
	}

	// A SECOND record for this identifier is delivered while that action is live and is
	// refused — the supported stale-broker shape, and the one whose continuation is the
	// action rather than a section.
	reply := testRedriveAckSubject(dur, 9, 1, 11)
	obs := testRedriveObserver(t, e, reply)
	e.sess.registerPubRelRedrive(e.c, pi, reply, subject, dur)
	testRequireNoRedrive(t, obs, "a live PUBREL action")
	if got := testRedriveWaiters(e, pi); len(got) != 1 {
		t.Fatalf("the waiter must stand while the PUBREL action is live, got %+v", got)
	}

	e.sess.startActionDrain(e.c, act)

	testRequireRedrive(t, obs, "the PUBREL action drained")
	if cur, _, lerr := e.asm.loadInflightRecord(subject); lerr != nil || cur == nil ||
		cur.ActionToken != _EMPTY_ {
		t.Fatalf("the drain must have cleared the token before the wake: rec=%+v err=%v", cur, lerr)
	}
}

// R5: A STALE CLOSER CLEARS NOTHING, and pubIdent is the field this is really about.
//
// endSendWith is reached with a token that names a section, and a section that already
// ended — or one that never owned this hold — must not clear the marker naming the PUBLISH
// a LIVE section still has on the wire. Clearing it would tell PUBREL admission that the
// transmission had finished, and the whole local-half refusal would evaporate exactly when
// it is needed.
//
// Mutation M81: clear pubIdent before (or outside) the exact-token match.
func TestMQTTPubRelSendSectionMarkerSurvivesAStaleCloser(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "redrive-r5")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(4)
	const live = "the-live-sections-token"

	e.sess.mu.Lock()
	if e.sess.sendsInFlight == nil {
		e.sess.sendsInFlight = map[uint16]*mqttSendHold{}
	}
	e.sess.sendsInFlight[pi] = &mqttSendHold{token: live, pubIdent: live}
	e.sess.mu.Unlock()

	e.sess.endSendWith(e.c, pi, "a-previous-sections-token", nil)

	e.sess.mu.Lock()
	h := e.sess.sendsInFlight[pi]
	e.sess.mu.Unlock()
	if h == nil || h.token != live || h.pubIdent != live {
		t.Fatalf("a stale closer must clear neither half of the live section, got %+v", h)
	}
	if !e.sess.publishTransmissionStandsLocalOnly(pi) {
		t.Fatal("the live PUBLISH must still be visible to PUBREL admission")
	}

	// AND THE SECTION'S OWN CLOSER CLEARS BOTH.
	e.sess.endSendWith(e.c, pi, live, nil)
	e.sess.mu.Lock()
	after := e.sess.sendsInFlight[pi]
	e.sess.mu.Unlock()
	if after != nil && after.pubIdent != _EMPTY_ {
		t.Fatalf("the section's own closer must clear its marker, got %+v", after)
	}
	if e.sess.publishTransmissionStandsLocalOnly(pi) {
		t.Fatal("no PUBLISH is on the wire once the section closed")
	}
}
