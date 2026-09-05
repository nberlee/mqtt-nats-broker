// Copyright 2025 The NATS Authors
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
	"testing"
)

// F-094: a drain carrying a past-PUBREC intent owns its sid, so no subscription cycle may
// adopt it away.
//
// The defect these pin: adoptDrainLocked removes the drain from BOTH maps and hands its
// consumer back as an ordinary membership. The drain object — and with it every
// per-sequence claim, intent, identifier and owner nonce — is dropped on the floor. The
// still-unacknowledged source then bypasses the delivery callback's drain branch entirely
// (it keys on drainByDur) and is re-offered at AckWait as a FRESH PUBLISH of an exchange
// the client has already answered, on a different packet identifier because PUBREC removed
// the original cpending link.
//
// The predicate is deliberately UNQUALIFIED — st.intent alone, no claim flag. Two narrower
// shapes were refuted during planning and drainSidHoldLocked's own comment records both;
// TestAdoptionRefusedForAFinalizedIntentBesideALiveClaim and
// TestAdoptionRefusedForAFinalizedIntentAsTheLastSequence are their counterexamples, so
// re-narrowing the predicate fails here rather than in production.

// heldDrainFixture builds a session holding one restored drain under sid.
func heldDrainFixture(t *testing.T, sid, dur string, seqs map[uint64]*drainSeq) *mqttSession {
	t.Helper()
	d := &mqttDrain{
		cc:   &ConsumerConfig{Durable: dur, DeliverSubject: mqttSubPrefix + dur},
		qos:  2,
		seqs: seqs,
	}
	return &mqttSession{
		drains:     map[string]*mqttDrain{sid: d},
		drainByDur: map[string]*mqttDrain{dur: d},
	}
}

// TestAdoptionRefusedWhileADrainCarriesAPastPubRecIntent is the ordinary case: the intent
// is claimed and unfinished, which is where a re-SUBSCRIBE lands between the client's
// PUBREC and the stored PUBREL being delivered back.
func TestAdoptionRefusedWhileADrainCarriesAPastPubRecIntent(t *testing.T) {
	t.Parallel()
	const sid, dur = "1", "$MQTT_plain_intent"
	sess := heldDrainFixture(t, sid, dur, map[uint64]*drainSeq{
		41: {claimed: true, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	d := sess.drains[sid]

	if got := sess.drainSidHoldLocked(sid); got != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("a past-PUBREC intent must hold its sid, got %v", got)
	}
	if sess.adoptDrainLocked(sid) {
		t.Fatal("adoption must be refused: it drops the drain and its seqs map, after which " +
			"the unacknowledged source is re-offered on the ordinary path as a fresh PUBLISH")
	}
	if sess.drains[sid] != d || sess.drainByDur[dur] != d {
		t.Fatal("a refused adoption must leave BOTH maps exactly as they were")
	}
	if sess.cons[sid] != nil {
		t.Error("a refused adoption must install no consumer membership")
	}
	// And the record built from this session still carries the intent, which is what a
	// restart reads. Losing it here is what let the source be retransmitted.
	if len(d.startedSeqs()) == 0 {
		t.Fatal("the persisted Started list must still carry the intent")
	}
}

// TestAdoptionRefusedForAFinalizedIntentBesideALiveClaim is the counterexample that killed
// the first narrower predicate, `intent && (claimed || unclaimUnconfirmed)`.
//
// finalizeDrainDelivery clears unclaimUnconfirmed in its saveLanded arm BEFORE it sends the
// source acknowledgement, and that send is fire-and-forget into the JSA queue. So this
// state — intent recorded, claim durably removed, both guards clear — can coexist with a
// source that was never acknowledged. While the drain lives, a re-offer hits its
// `!st.claimed` arm and is acked there; adoption removes exactly that suppression, and the
// live path cannot rebuild it because PUBREC already dropped the cpending link.
func TestAdoptionRefusedForAFinalizedIntentBesideALiveClaim(t *testing.T) {
	t.Parallel()
	const sid, dur = "1", "$MQTT_plain_finalized"
	sess := heldDrainFixture(t, sid, dur, map[uint64]*drainSeq{
		// Finalized: claim gone, guard clear, intent still recorded.
		41: {claimed: false, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
		// Still owed, which is why the drain has not been retired.
		42: {claimed: true, pi: 8},
	})

	if got := sess.drainSidHoldLocked(sid); got != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("a finalized intent whose source ack is unproven must still hold, got %v", got)
	}
	if sess.adoptDrainLocked(sid) {
		t.Fatal("adoption must be refused: the finalized intent's source acknowledgement is " +
			"fire-and-forget, so the drain is the only thing suppressing its re-offer")
	}
}

// TestAdoptionRefusedForAFinalizedIntentAsTheLastSequence is the counterexample that killed
// the second narrower predicate, the same test hoisted to drain level ("an intent anywhere
// plus any outstanding sequence").
//
// Here NOTHING is outstanding: the drain's only sequence is a finalized intent. endRecordTx
// releases before the source ack and the teardown, so a re-SUBSCRIBE lands in that gap and
// the drain-level conjunction would have adopted.
func TestAdoptionRefusedForAFinalizedIntentAsTheLastSequence(t *testing.T) {
	t.Parallel()
	const sid, dur = "1", "$MQTT_plain_last"
	sess := heldDrainFixture(t, sid, dur, map[uint64]*drainSeq{
		41: {claimed: false, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	d := sess.drains[sid]

	// The premise, or this test proves nothing about the conjunction it exists to refute.
	if !d.retirable() {
		t.Fatal("the fixture must have NOTHING outstanding, which is the whole point")
	}
	if got := sess.drainSidHoldLocked(sid); got != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("an intent holds its sid even with nothing outstanding, got %v", got)
	}
	if sess.adoptDrainLocked(sid) {
		t.Fatal("adoption must be refused: the claim flags describe claim-removal DURABILITY, " +
			"not whether the retained consumer can still re-offer the source")
	}
}

// TestAdoptionRefusedWhileAnUnclaimRemainsUnconfirmed covers the saveMaybeLanded window,
// where the removal may or may not be in the record (I1).
func TestAdoptionRefusedWhileAnUnclaimRemainsUnconfirmed(t *testing.T) {
	t.Parallel()
	const sid, dur = "1", "$MQTT_plain_unconfirmed"
	sess := heldDrainFixture(t, sid, dur, map[uint64]*drainSeq{
		41: {claimed: false, unclaimUnconfirmed: true, intent: true, pi: 7,
			ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	d := sess.drains[sid]

	if got := sess.drainSidHoldLocked(sid); got != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("an unconfirmed removal must still hold, got %v", got)
	}
	if sess.adoptDrainLocked(sid) {
		t.Fatal("adoption must be refused while the removal's outcome is unknown")
	}
	// And retirement is refused for the same state, which is a separate rule: claimedCount()
	// alone would read this drain as empty and tear its durable down on a write nobody has
	// proven landed.
	if d.retirable() {
		t.Fatal("an unconfirmed removal must forbid retirement: claimedCount() counts " +
			"st.claimed only, which is exactly why retirable() is not that test")
	}
}

// TestHoldReasonPrefersTheUnusableIdentity pins the precedence, because the two holds
// converge differently — one on an operator, one on the drained exchange completing — and
// the refusal message names which.
//
// Asserted in BOTH insertion orders: Go map traversal order is not insertion order, so a
// single-order test can pass against an implementation that simply returns whichever
// sequence it happened to see first.
func TestHoldReasonPrefersTheUnusableIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		seqs map[uint64]*drainSeq
	}{
		{"unusable first", map[uint64]*drainSeq{
			41: {claimed: true, intent: true, pi: 0, ownerUnusable: true},
			42: {claimed: true, intent: true, pi: 8, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
		}},
		{"intent first", map[uint64]*drainSeq{
			42: {claimed: true, intent: true, pi: 8, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
			41: {claimed: true, intent: true, pi: 0, ownerUnusable: true},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess := heldDrainFixture(t, "1", "$MQTT_plain_both", tc.seqs)
			if got := sess.drainSidHoldLocked("1"); got != mqttDrainHeldUnusableIdentity {
				t.Fatalf("the unusable identity must win regardless of traversal order, got %v", got)
			}
		})
	}

	// The reasons are distinct and each names its own convergence, which is the false-claim
	// this rename exists to stop: before F-094 both refusals told the operator to remove a
	// record that, for an ordinary intent, is doing nothing wrong.
	unusable := mqttDrainHeldUnusableIdentity.reason()
	intent := mqttDrainHeldPastPubRecIntent.reason()
	if unusable == intent || unusable == _EMPTY_ || intent == _EMPTY_ {
		t.Fatalf("each hold must state its own convergence: %q vs %q", unusable, intent)
	}
	if mqttDrainNotHeld.reason() != _EMPTY_ {
		t.Error("an unheld sid has no reason to render")
	}
}

// TestPastPubRecIntentRefusesEverySidOfAWildcardFilter is the paired-sid case at the unit
// level: the FILTER is the atomicity boundary, so a wildcard whose companion holds an
// intent must not have its main sid adopted.
func TestPastPubRecIntentRefusesEverySidOfAWildcardFilter(t *testing.T) {
	t.Parallel()
	const mainSid = "foo.>"
	companion := mainSid[:len(mainSid)-2] + mqttMultiLevelSidSuffix
	if !mqttNeedSubForLevelUp(mainSid) {
		t.Fatalf("%q must own a level-up companion for this test to mean anything", mainSid)
	}

	unheld := &mqttDrain{
		cc: &ConsumerConfig{Durable: "$MQTT_main_plain"}, qos: 2,
		seqs: map[uint64]*drainSeq{41: {claimed: true, pi: 7}},
	}
	held := &mqttDrain{
		cc: &ConsumerConfig{Durable: "$MQTT_comp_intent"}, qos: 2,
		seqs: map[uint64]*drainSeq{42: {claimed: true, intent: true, pi: 8,
			ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"}},
	}
	sess := &mqttSession{
		drains: map[string]*mqttDrain{mainSid: unheld, companion: held},
		drainByDur: map[string]*mqttDrain{
			"$MQTT_main_plain": unheld, "$MQTT_comp_intent": held,
		},
	}

	// The premise both ways, or a refusal proves nothing about which sid caused it.
	if got := sess.drainSidHoldLocked(mainSid); got != mqttDrainNotHeld {
		t.Fatalf("the main drain must be UNHELD, got %v", got)
	}
	if got := sess.drainSidHoldLocked(companion); got != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("the companion must be held by its intent, got %v", got)
	}

	// So the filter-level question is the one that has to be asked. A per-sid walk adopts
	// the main drain before reaching the companion, and the per-filter failure cleanup does
	// not put an adopted drain back.
	anyHeld := false
	for _, sid := range mqttUnsubSids(mainSid) {
		if sess.drainSidHoldLocked(sid) != mqttDrainNotHeld {
			anyHeld = true
		}
	}
	if !anyHeld {
		t.Fatal("the filter must report held through the question every occupying path asks")
	}
}

// TestStagingRefusedOverAHeldSid keeps one owner per sid at the second UNSUBSCRIBE.
//
// Defensive: the completed preflights stop an ordinary subscription cycle producing a live
// consumer alongside a held drain. The fixture therefore builds that state directly, with a
// non-shared config and non-empty cpending so it reaches past stageDrain's early returns.
func TestStagingRefusedOverAHeldSid(t *testing.T) {
	t.Parallel()
	const sid, dur = "1", "$MQTT_plain_intent"
	sess := heldDrainFixture(t, sid, dur, map[uint64]*drainSeq{
		41: {claimed: true, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	d := sess.drains[sid]

	fresh := &ConsumerConfig{Durable: "$MQTT_plain_fresh"}
	sess.cpending = map[string]map[uint64]uint16{fresh.Durable: {99: 8}}

	if sess.stageDrain(sid, fresh, nil) {
		t.Fatal("staging must be refused while a held drain owns this sid: otherwise the new " +
			"drain takes the map slot and the record, serialized by that map, loses the intent")
	}
	if sess.drains[sid] != d {
		t.Fatal("the held drain must keep its sid against a later stage")
	}
	if got := sess.drainSidHoldLocked(sid); got != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("the hold must survive the whole cycle, got %v", got)
	}
}

// TestHeldReasonDoesNotPromiseReleaseOnOneExchange pins the diagnostic against the state
// that makes the obvious wording false.
//
// The hold is unqualified, so a drain can be held by an intent whose OWN exchange is already
// finalized while a different sequence keeps the drain alive. Telling the operator the
// filter returns "once that exchange completes" is then wrong twice: that exchange owes
// nothing, and completing it releases nothing. The true condition is the drain's retirement,
// and a message naming the wrong one sends an operator looking at a healthy exchange.
func TestHeldReasonDoesNotPromiseReleaseOnOneExchange(t *testing.T) {
	t.Parallel()
	const sid, dur = "1", "$MQTT_plain_reason"
	sess := heldDrainFixture(t, sid, dur, map[uint64]*drainSeq{
		41: {claimed: false, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
		42: {claimed: true, pi: 8},
	})
	if got := sess.drainSidHoldLocked(sid); got != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("the fixture must be held by the finalized intent, got %v", got)
	}

	reason := mqttDrainHeldPastPubRecIntent.reason()
	for _, false_ := range []string{"once that exchange completes", "owes its PUBREL"} {
		if strings.Contains(reason, false_) {
			t.Errorf("the refusal must not promise %q: this drain's intent is already "+
				"finalized and a different claim is what holds the sid, so completing that "+
				"exchange releases nothing. Reason was %q", false_, reason)
		}
	}
	if !strings.Contains(reason, "retired") {
		t.Errorf("the refusal must name the condition that actually frees the sid — the "+
			"drain being retired — got %q", reason)
	}
}
