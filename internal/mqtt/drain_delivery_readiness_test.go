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
	"errors"
	"testing"
)

// Delivery readiness on adoption. A drain whose resume could not install a delivery
// subscription is still adoptable, and adoption moves its durable into sess.cons —
// after which processJSConsumer's existing-consumer arm reads that entry as proof a
// delivery subscription exists. The filter is SUBACKed SUCCESSFULLY and then delivers
// nothing for the rest of the connection.
//
// SCOPE, so a later reader does not mistake what these prove. They cover the
// INSTALLATION: a reclaimed drain ends with a usable delivery subscription carrying
// the terms the client asked for, or the filter fails. They deliberately do NOT cover
// what a failed filter does with an adoption that already happened, nor whether an
// already-started QoS 2 exchange still completes at QoS 2 when the re-SUBSCRIBE asks
// for QoS 1. Both are separate defects, reachable without this repair, and both are
// stated in docs/CONFORMANCE.md and filed in docs/ROADMAP.md.

// unindexDrain removes a drain's reverse-index entry, which is the half of the R2
// state a fixture cannot get from addTestDrain. The other half — no delivery
// subscription — is the CALLER's, and the callers below supply subscription-less
// fixtures; this helper does not unsubscribe anything. Together they are what
// restoreDrains leaves when processQOS12Sub fails.
//
// R2 ALONE. The blanket QoS 2 withholding leaves the drain INDEXED with no
// subscription, because restoreDrains skips the durable and the entry
// restoreDrainLocked made at record restore survives — which is why the reclaim's
// guard tests the subscription rather than the index. That producer is driven end to
// end in drain_delivery_readiness_e2e_test.go.
func unindexDrain(sess *mqttSession, d *mqttDrain) {
	sess.mu.Lock()
	delete(sess.drainByDur, d.cc.Durable)
	sess.mu.Unlock()
}

// subForDeliverSubject returns the connection's subscription on a durable's deliver
// subject, which is the thing whose absence IS the defect.
func subForDeliverSubject(c *client, cc *ConsumerConfig) *subscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subs[cc.DeliverSubject]
}

// TestReclaimInstallsDeliveryForASubscriptionLessDrain is the defect itself: before
// the reclaim, adoption produced a consumer entry with no delivery interest.
func TestReclaimInstallsDeliveryForASubscriptionLessDrain(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "reclaim-installs")

	const dur, sid = "dur-reclaim-installs", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	d := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{
		41: {claimed: true},
	})
	sess.mu.Lock()
	d.cc = cfg
	sess.mu.Unlock()
	unindexDrain(sess, d)

	// The premise, or this test passes for the wrong reason.
	if subForDeliverSubject(c, cfg) != nil {
		t.Fatal("the fixture must start with NO delivery subscription")
	}
	if drainRegistered(sess, dur) {
		t.Fatal("the fixture must start un-indexed, which is what a FAILED RESTORE leaves")
	}

	sess.mu.Lock()
	got, err := sess.reclaimDrainDeliveryLocked(c, sid, sid, 1, false, false, 0)
	sess.mu.Unlock()
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if got != d {
		t.Fatalf("the reclaim must report the drain it adopted, got %v", got)
	}

	sub := subForDeliverSubject(c, cfg)
	if sub == nil || sub.mqtt == nil {
		t.Fatal("a reclaimed drain must end with a delivery subscription: without one the " +
			"filter is SUBACKed successfully and then delivers nothing")
	}
	if sub.mqtt.closed {
		t.Fatal("the installed delivery subscription must be open")
	}
	if sub.mqtt.jsDur != dur {
		t.Fatalf("the subscription must name the drain's durable, got %q", sub.mqtt.jsDur)
	}

	sess.mu.Lock()
	live, stillDraining := sess.cons[sid], sess.drains[sid]
	charged := sess.consCharged[dur]
	sess.mu.Unlock()
	if live != cfg {
		t.Fatalf("the drain's durable must be adopted into cons, got %v", live)
	}
	if stillDraining != nil {
		t.Fatal("an adopted drain must leave sess.drains (I7)")
	}
	if charged != 0 {
		t.Fatalf("a durable that predates this call must not be charged, got %d", charged)
	}
	// Adoption detaches the drain OBJECT from the session maps without touching its
	// per-sequence state. Pinned because the reclaim must not quietly clear it on the
	// way past: what re-establishes those claims for the adopted consumer is the durable
	// scans, and they need the object intact to do it.
	if d.seqs[41] == nil {
		t.Fatal("adoption must leave the detached drain object's sequence state intact")
	}
}

// TestReclaimInstallsWithTheRequestedTermsNotTheDrainsTerms is entered DIRECTLY at the
// helper, and the entry point is the test.
//
// Driven through processSubs instead, processJSConsumer's metadata refresh can win the
// lock before any delivery callback classifies, so a delivery-shaped assertion passes
// against the mutation as often as not. Called directly, the installed terms are read
// on return, before any refresh can have run, and the mutation dies every time.
//
// What it guards: installing with the DRAIN's terms and letting the later refresh
// correct them leaves a window in which the callback selects with the OLD terms
// against a backlog this very call made available. A drain with noLocal=true and a
// re-SUBSCRIBE asking noLocal=false is enough — the callback acks the message away
// where the unrepaired broker leaves it pending.
func TestReclaimInstallsWithTheRequestedTermsNotTheDrainsTerms(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "reclaim-terms")

	const dur, sid = "dur-reclaim-terms", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	d := addTestDrain(sess, sid, dur, sid, 2, nil)
	sess.mu.Lock()
	// Every term differs from what the re-SUBSCRIBE will ask for, so no assertion
	// below can pass by coincidence.
	d.cc, d.noLocal, d.rap, d.subID = cfg, true, true, 7
	sess.mu.Unlock()
	unindexDrain(sess, d)

	sess.mu.Lock()
	_, err := sess.reclaimDrainDeliveryLocked(c, sid, sid, 1, false, false, 9)
	sess.mu.Unlock()
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}

	sub := subForDeliverSubject(c, cfg)
	if sub == nil || sub.mqtt == nil {
		t.Fatal("no delivery subscription was installed")
	}
	sess.mu.Lock()
	sess.subsMu.Lock()
	qos, noLocal, rap, subID := sub.mqtt.qos, sub.mqtt.noLocal, sub.mqtt.retainAsPublished, sub.mqtt.subID
	sess.subsMu.Unlock()
	sess.mu.Unlock()

	if qos != 1 {
		t.Fatalf("the install must use the REQUESTED QoS, got %d (the drain's is 2)", qos)
	}
	if noLocal {
		t.Fatal("the install must use the REQUESTED No Local: with the drain's true, the " +
			"callback acks away a message this connection published and is owed")
	}
	if rap {
		t.Fatal("the install must use the REQUESTED Retain As Published, not the drain's")
	}
	if subID != 9 {
		t.Fatalf("the install must use the REQUESTED subscription identifier, got %d", subID)
	}
}

// TestReclaimDerivesTheReservedFlagFromTheFilter pins the one field the later refresh
// can NEVER repair: isReserved is set once at install.
//
// The deliver subject is always "$MQTT.sub.<nuid>", never the client's filter, so
// deriving the flag from it answers a different question entirely and gets this
// fixture wrong: the filter ">" is reserved and the deliver subject is not.
func TestReclaimDerivesTheReservedFlagFromTheFilter(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "reclaim-reserved")

	// An internal filter, for which isMQTTReservedSubscription is actually true. A
	// "$SYS" filter is NOT that fixture.
	const filter = ">"
	const dur, sid = "dur-reclaim-reserved", ">"
	cfg := realDrainConsumer(t, sess, dur, filter)
	d := addTestDrain(sess, sid, dur, filter, 1, nil)
	sess.mu.Lock()
	d.cc = cfg
	sess.mu.Unlock()
	unindexDrain(sess, d)

	if !isMQTTReservedSubscription(filter) {
		t.Fatalf("the fixture filter %q must be reserved, or this proves nothing", filter)
	}

	sess.mu.Lock()
	_, err := sess.reclaimDrainDeliveryLocked(c, filter, sid, 1, false, false, 0)
	sess.mu.Unlock()
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	sub := subForDeliverSubject(c, cfg)
	if sub == nil || sub.mqtt == nil {
		t.Fatal("no delivery subscription was installed")
	}
	if !sub.mqtt.reserved {
		t.Fatal("the reserved flag must come from the FILTER, not the $MQTT.sub.* deliver " +
			"subject; it is set once at install and no refresh can repair it")
	}
}

// TestAFailedReclaimAdoptsNothingAndLeavesNoRegistration is the failure contract: a
// failed reclaim leaves the session exactly as it found it.
//
// Deliberately NOT t.Parallel(): it installs a PROCESS-GLOBAL failure hook, and a
// parallel test that can reach process-global state can corrupt whatever runs beside
// it. Keying the hook by durable makes it harmless in practice, but the invariant
// TestParallelTestsTouchNoProcessGlobalState enforces is about reach, not luck.
func TestAFailedReclaimAdoptsNothingAndLeavesNoRegistration(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "reclaim-fails")

	const dur, sid = "dur-reclaim-fails", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	d := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{
		41: {claimed: true},
	})
	sess.mu.Lock()
	d.cc = cfg
	sess.mu.Unlock()
	unindexDrain(sess, d)

	boom := errors.New("delivery subscription refused")
	restore := setMQTTTestQOS12SubFailHook(func(durable string) error {
		if durable == dur {
			return boom
		}
		return nil
	})
	defer restore()

	sess.mu.Lock()
	got, err := sess.reclaimDrainDeliveryLocked(c, sid, sid, 1, false, false, 0)
	sess.mu.Unlock()
	if !errors.Is(err, boom) {
		t.Fatalf("a failed install must fail the filter, got err=%v", err)
	}
	if got != nil {
		t.Fatalf("a failed reclaim must report no adoption, got %v", got)
	}

	sess.mu.Lock()
	stillDraining, live := sess.drains[sid], sess.cons[sid]
	sess.mu.Unlock()
	if stillDraining != d {
		t.Fatal("a failed reclaim must leave the drain where it found it")
	}
	if live != nil {
		t.Fatal("a failed reclaim must adopt nothing (I7)")
	}
	if st := d.seqs[41]; st == nil || !st.claimed {
		t.Fatalf("a failed reclaim must not touch the drain's claims, got %+v", st)
	}
	if drainRegistered(sess, dur) {
		t.Fatal("a failed reclaim must leave NO reverse-index entry: that is the state a " +
			"failed restore leaves, and what the next resume retries from")
	}
	if subForDeliverSubject(c, cfg) != nil {
		t.Fatal("a failed install must leave no delivery subscription")
	}
}

// TestAHeldDrainIsNeitherReclaimedNorAdopted is F-094's guard against this change, and
// it is entered directly at the helper for the reason
// TestAdoptionRefusalIsReachedBelowTheProcessSubsPreflight records: processSubs refuses
// a held filter before the helper is called, so a mutation removing the helper's own
// check survives every end-to-end test.
//
// The reclaim runs BEFORE adoptDrainLocked, so adoptDrainLocked's refusal does not
// cover it: without its own check the helper would install delivery interest for a
// drain whose whole purpose is to withhold it.
func TestAHeldDrainIsNeitherReclaimedNorAdopted(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "reclaim-held")

	const dur, sid = "dur-reclaim-held", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	d := addTestDrain(sess, sid, dur, sid, 2, map[uint64]*drainSeq{
		41: {claimed: true, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	sess.mu.Lock()
	d.cc = cfg
	held := sess.drainSidHoldLocked(sid)
	sess.mu.Unlock()
	unindexDrain(sess, d)
	if held != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("the fixture must be HELD, or there is no refusal to prove, got %v", held)
	}

	sess.mu.Lock()
	got, err := sess.reclaimDrainDeliveryLocked(c, sid, sid, 2, false, false, 0)
	sess.mu.Unlock()
	if err != nil {
		t.Fatalf("a held drain is declined, not errored: %v", err)
	}
	if got != nil {
		t.Fatalf("a held drain must not be reported as adopted, got %v", got)
	}

	if subForDeliverSubject(c, cfg) != nil {
		t.Fatal("NOTHING may be installed for a held drain: source interest is exactly " +
			"what the hold withholds, and a later refusal cannot take it back")
	}
	sess.mu.Lock()
	stillDraining, live := sess.drains[sid], sess.cons[sid]
	sess.mu.Unlock()
	if stillDraining != d {
		t.Fatal("the held drain must survive untouched")
	}
	if live != nil {
		t.Fatal("a held drain must never be adopted (F-094)")
	}
}

// TestAnAlreadyDeliveringDrainIsLeftToTheOrdinaryAdoption keeps the helper's blast
// radius to the defective state. A healthy drain must reach adoption where it always
// did, inside processJSConsumer — otherwise every re-SUBSCRIBE over any drain would
// take a new code path.
func TestAnAlreadyDeliveringDrainIsLeftToTheOrdinaryAdoption(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "reclaim-healthy")

	const dur, sid = "dur-reclaim-healthy", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	d := addTestDrain(sess, sid, dur, sid, 1, nil)
	sess.mu.Lock()
	d.cc = cfg
	// ACTUALLY delivering, which means a real subscription on the deliver subject.
	// addTestDrain only populates the reverse index, and the index is not evidence of
	// delivery — a drain restored from the record is indexed before anything is
	// installed. A fixture that indexed and stopped would be the DEFECTIVE state
	// wearing the healthy state's name, and this test would then assert that the
	// helper declines to repair the one thing it exists for.
	inbox := cfg.DeliverSubject
	_, serr := sess.processQOS12Sub(c, []byte(inbox), []byte(inbox), nil, _EMPTY_, false, 1, false, false,
		false, 0, dur, mqttDeliverMsgCbQoS12)
	sess.mu.Unlock()
	if serr != nil {
		t.Fatalf("fixture subscription: %v", serr)
	}
	if subForDeliverSubject(c, cfg) == nil {
		t.Fatal("the fixture must be genuinely delivering")
	}

	sess.mu.Lock()
	got, err := sess.reclaimDrainDeliveryLocked(c, sid, sid, 1, false, false, 0)
	stillDraining, live := sess.drains[sid], sess.cons[sid]
	sess.mu.Unlock()
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if got != nil {
		t.Fatalf("an already-delivering drain is not this helper's to adopt, got %v", got)
	}
	if stillDraining != d || live != nil {
		t.Fatal("the helper must leave a healthy drain entirely alone")
	}
}
