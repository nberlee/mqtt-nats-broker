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
	"testing"
	"time"
)

// F-094's release side. The hold is unqualified — every past-PUBREC intent owns its sid —
// so the hold lasts exactly as long as the DRAIN OBJECT does, and these pin that the drain
// can always be retired.
//
// Getting that wrong is not a theoretical risk: revision 2 of the plan shipped the widened
// hold with the old retirement guards, and a review round found a reachable state where the
// sid was refused for the life of the session. restoreDrains deletes the drainByDur entry
// when a delivery subscription fails to restore and KEEPS sess.drains[sid]; both reapers
// keyed on that entry, so nothing could ever retire such a drain.
//
// NOTE ON SCOPE, so a later reader does not mistake what these prove: these are about the
// sid RELEASE, which is F-094's side. The disposal half — the delivery barrier applied with
// the map drop, the terminal per-object flag, and the classified consumer delete — is pinned
// by drain_disposal_test.go, and the guarantee it establishes is scoped to this drain object
// within this resident session object's lifetime (docs/CONFORMANCE.md).

// realDrainConsumer creates an actual durable so recovery's lookup finds it. Without one,
// recoverDrains takes its consumer-absent arm and prunes through p.drop, bypassing the
// ownership guard these tests exist to exercise.
func realDrainConsumer(t *testing.T, sess *mqttSession, durable, filter string) *ConsumerConfig {
	t.Helper()
	cfg := ConsumerConfig{
		Durable:        durable,
		DeliverSubject: mqttSubPrefix + durable,
		FilterSubject:  mqttStreamSubjectPrefix + filter,
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverNew,
		AckWait:        time.Minute,
		MaxAckPending:  1024,
	}
	if _, err := sess.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName, Config: cfg,
	}); err != nil {
		t.Fatalf("createDurableConsumer(%s): %v", durable, err)
	}
	return &cfg
}

// TestRetirementRefusedWhileAnUnclaimIsUnconfirmed drives the real retirement path for a
// drain whose last removal is mid-saveMaybeLanded.
//
// claimedCount() was the test and counts st.claimed alone, so this drain read as empty and
// retirement tore its durable down on a write whose outcome nobody knew (I1 backwards).
func TestRetirementRefusedWhileAnUnclaimIsUnconfirmed(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-retire-unconfirmed")

	const dur = "dur-retire-unconfirmed"
	d := addTestDrain(sess, "sid", dur, "foo", 2, map[uint64]*drainSeq{
		41: {claimed: false, unclaimUnconfirmed: true, intent: true, pi: 7,
			ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})

	sess.finishDrainIfEmpty(c, d)

	if !drainRegistered(sess, dur) {
		t.Fatal("retirement must be refused while a removal is unconfirmed: the record may " +
			"still carry that claim, and the durable would be gone")
	}
	sess.mu.Lock()
	stillHeld := sess.drainSidHoldLocked("sid")
	sess.mu.Unlock()
	if stillHeld != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("the sid must stay held while retirement is refused, got %v", stillHeld)
	}
}

// TestRetirementProceedsWithoutADrainByDurEntry is the state that made revision 2's hold
// unreleasable, and it is the test that proves the hold HAS a release.
//
// The drain is durably empty and still owns its sid, but its drainByDur entry is gone —
// exactly what restoreDrains leaves behind when processQOS12Sub fails. The old guard
// (`sess.drainByDur[dur] != d`) returned early here forever.
func TestRetirementProceedsWithoutADrainByDurEntry(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-retire-nobydur")

	const dur, sid = "dur-retire-nobydur", "sid"
	cfg := realDrainConsumer(t, sess, dur, "foo")

	// Durably empty, with the intent still recorded: claim removed, guard clear.
	d := addTestDrain(sess, sid, dur, "foo", 2, map[uint64]*drainSeq{
		41: {claimed: false, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	sess.mu.Lock()
	d.cc = cfg
	// The restore failure: the reverse index is dropped, the sid mapping is kept.
	delete(sess.drainByDur, dur)
	sess.mu.Unlock()

	// The premise, or this test passes for the wrong reason.
	sess.mu.Lock()
	held, retirable := sess.drainSidHoldLocked(sid), d.retirable()
	sess.mu.Unlock()
	if held != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("the fixture must be HELD, or there is no release to prove, got %v", held)
	}
	if !retirable {
		t.Fatal("the fixture must be durably empty")
	}

	sess.finishDrainIfEmpty(c, d)

	sess.mu.Lock()
	stillThere, nowHeld := sess.drains[sid], sess.drainSidHoldLocked(sid)
	sess.mu.Unlock()
	if stillThere != nil {
		t.Fatal("a durably empty drain owning its sid must retire even with no drainByDur " +
			"entry: otherwise the widened hold refuses that filter for the life of the session")
	}
	if nowHeld != mqttDrainNotHeld {
		t.Fatalf("the sid must be free once the drain is retired, got %v", nowHeld)
	}
}

// TestStaleRetirementDoesNotDeleteAnAdoptedDurable pins what the ownership guard is FOR,
// so replacing the drainByDur test with a sid test does not quietly lose that protection.
//
// The dangerous shape is a retirement call that was already in flight when the drain was
// ADOPTED. Adoption hands the SAME durable to sess.cons[sid] as a live consumer, so a stale
// finishDrainIfEmpty that got past its guard would unsubscribe that consumer's delivery
// subject and delete its durable — destroying a live subscription and its started
// deliveries. The guard stops it because the drain is no longer in sess.drains at all, so
// the durable-to-sid lookup fails.
//
// An earlier version of this test put a DIFFERENT drain under the sid and asserted the
// replacement survived. It passed with the guard deleted, and the mutation run caught that:
// dropDrainLocked resolves the sid by scanning for the durable, so a stale object that is
// not in sess.drains cannot reach any sid entry and the call was a harmless no-op. The
// assertion has to be about the DURABLE, which is the thing a stale call can still destroy.
func TestStaleRetirementDoesNotDeleteAnAdoptedDurable(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-retire-adopted")

	const dur, sid = "dur-retire-adopted", "sid"
	cfg := realDrainConsumer(t, sess, dur, "foo")

	// Durably empty and NOT held, so adoption is allowed: the hold covers intents, and this
	// test is about what happens to an ordinary drain that was adopted away.
	d := addTestDrain(sess, sid, dur, "foo", 2, map[uint64]*drainSeq{
		41: {claimed: false, pi: 7},
	})
	sess.mu.Lock()
	d.cc = cfg
	held := sess.drainSidHoldLocked(sid)
	adopted := sess.adoptDrainLocked(sid)
	consumerCfg := sess.cons[sid]
	sess.mu.Unlock()

	if held != mqttDrainNotHeld {
		t.Fatalf("the fixture must be adoptable, got %v", held)
	}
	if !adopted || consumerCfg == nil || consumerCfg.Durable != dur {
		t.Fatalf("the drain must be adopted into a live consumer keeping its durable: %v %+v",
			adopted, consumerCfg)
	}

	// The stale call, as one already in flight when the adoption landed.
	sess.finishDrainIfEmpty(c, d)

	sess.mu.Lock()
	stillLive := sess.cons[sid]
	sess.mu.Unlock()
	if stillLive == nil || stillLive.Durable != dur {
		t.Fatal("a stale retirement must not disturb the consumer the adoption installed")
	}

	// The durable itself is what a stale call destroys. The sleep is RETAINED even though
	// retirement's delete is now confirmed and synchronous: what this test asserts is that
	// no delete was issued AT ALL, and the guard it exercises returns before
	// disposeRetiredDrainConsumer is reached, so there is no request to wait on and nothing
	// to synchronise against. The window is left open so a build that issued one anyway —
	// through any route, confirmed or not — has time to complete it before the check.
	time.Sleep(500 * time.Millisecond)
	ci, err := sess.jsa.lookupConsumer(mqttStreamName, dur)
	if err != nil || ci == nil {
		t.Fatalf("a stale retirement deleted the ADOPTED consumer's durable (err=%v, ci=%v): "+
			"that destroys a live subscription and every delivery it had started", err, ci)
	}
}

// TestResumeSweepVisitsAnAlreadyEmptyDrain covers the sweep that runs BEFORE reconciliation's
// early return.
//
// A drain that owed nothing on entry produces no pruning plan, so `len(plans) == 0` returns
// before the post-save sweep ever runs. The fixture establishes that zero-plan condition
// positively rather than assuming it, and removes drainByDur too, so it discriminates both
// the missing sweep and a drainByDur-keyed ownership guard.
func TestResumeSweepVisitsAnAlreadyEmptyDrain(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-sweep-empty")

	const dur, sid = "dur-sweep-empty", "sid"
	cfg := realDrainConsumer(t, sess, dur, "foo")

	d := addTestDrain(sess, sid, dur, "foo", 2, map[uint64]*drainSeq{
		41: {claimed: false, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	sess.mu.Lock()
	d.cc = cfg
	delete(sess.drainByDur, dur)
	// Nothing is claimed, so reconciliation has nothing to unclaim and nothing to drop:
	// this is the zero-plan condition, asserted rather than assumed.
	zeroPlan := d.claimedCount() == 0 && d.retirable()
	sess.mu.Unlock()
	if !zeroPlan {
		t.Fatal("the fixture must produce NO reconciliation plan, or it exercises the " +
			"post-save sweep instead of the one under test")
	}

	sess.recoverDrains(c, nil)

	sess.mu.Lock()
	stillThere := sess.drains[sid]
	sess.mu.Unlock()
	if stillThere != nil {
		t.Fatal("a resume must retire a drain that was already empty on entry: it yields no " +
			"plan, so a sweep placed only after the early return never sees it")
	}
}

// TestPostSaveSweepRetiresADrainReconciliationEmptied covers the OTHER sweep, and it is the
// counterexample that stopped the two being collapsed into one relocated call.
//
// Here the drain is still claimed when the pre-return sweep runs, so only a sweep after
// reconciliation's own successful pruning can retire it. Moving the existing collection
// instead of adding a second one silently drops this case, and the sid stays held.
func TestPostSaveSweepRetiresADrainReconciliationEmptied(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-sweep-postsave")

	const dur, sid = "dur-sweep-postsave", "sid"
	// A REAL consumer, deliberately. With none, reconciliation takes its consumer-absent arm
	// and sets p.drop, so the drain is removed by the pruning itself and this test passes
	// with the post-save sweep deleted — which is exactly what the mutation run caught.
	// With a live consumer, p.drop is never set: the claim is UNCLAIMED and the now-empty
	// drain can only be retired by the sweep that runs after the save.
	cfg := realDrainConsumer(t, sess, dur, "foo")
	d := addTestDrain(sess, sid, dur, "foo", 2, map[uint64]*drainSeq{
		// A finalized intent: it keeps the sid HELD but owes nothing.
		41: {claimed: false, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
		// Claimed, PUBLISH-phase, and naming a sequence the stream does not hold, so the
		// exact-lookup arm CONFIRMS its absence and unclaims it.
		999: {claimed: true, pi: 8},
	})
	sess.mu.Lock()
	d.cc = cfg
	sess.mu.Unlock()

	sess.mu.Lock()
	notYetRetirable := !d.retirable()
	held := sess.drainSidHoldLocked(sid)
	sess.mu.Unlock()
	if !notYetRetirable {
		t.Fatal("the fixture must still owe something when the pre-return sweep runs, or it " +
			"tests the already-empty path instead")
	}
	if held != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("the fixture must be held by its intent, got %v", held)
	}

	sess.recoverDrains(c, nil)

	sess.mu.Lock()
	stillThere, nowHeld := sess.drains[sid], sess.drainSidHoldLocked(sid)
	sess.mu.Unlock()
	if stillThere != nil {
		t.Fatal("a drain this pass's own pruning emptied must be retired by the sweep that " +
			"runs after the save: it was still claimed when the earlier sweep ran")
	}
	if nowHeld != mqttDrainNotHeld {
		t.Fatalf("the sid must be free, got %v", nowHeld)
	}
}

// TestSkippedDrainIsNeverRetired keeps the barrier exclusion on BOTH sweeps. A drain whose
// PUBREL state the barrier could not resolve must not be pruned OR retired: its intent may
// be the only evidence the exchange is past PUBREC.
func TestSkippedDrainIsNeverRetired(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-sweep-skipped")

	const dur, sid = "dur-sweep-skipped", "sid"
	addTestDrain(sess, sid, dur, "foo", 2, map[uint64]*drainSeq{
		// Durably empty, so nothing but the skip set stops a sweep retiring it.
		41: {claimed: false, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})

	sess.recoverDrains(c, map[string]bool{dur: true})

	sess.mu.Lock()
	stillThere := sess.drains[sid]
	sess.mu.Unlock()
	if stillThere == nil {
		t.Fatal("a barrier-unresolved drain must not be retired by either sweep: nothing " +
			"about it may be acted on until a resume resolves its PUBREL state")
	}
}

// TestAdoptionRefusalIsReachedBelowTheProcessSubsPreflight enters through
// processJSConsumer, which is the route an ordinary SUBSCRIBE cannot exercise while the
// per-filter preflight stands.
//
// This matters for what a mutation can prove. The end-to-end tests all enter through
// processSubs, so removing only adoptDrainLocked's own hold check leaves every one of them
// passing — the outer preflight refuses first. Fork repair reaches adoption through a
// different door, and so does any future caller, so the guard inside the helper has to be
// pinned where it actually lives.
func TestAdoptionRefusalIsReachedBelowTheProcessSubsPreflight(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "drain-below-preflight")

	const dur, sid = "dur-below-preflight", "sid"
	cfg := realDrainConsumer(t, sess, dur, "foo")
	d := addTestDrain(sess, sid, dur, "foo", 2, map[uint64]*drainSeq{
		41: {claimed: true, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	sess.mu.Lock()
	d.cc = cfg
	sess.mu.Unlock()

	// The provisioning entry point itself, with no processSubs above it.
	cc, sub, _, err := sess.processJSConsumer(c, recordTx{}, "foo", sid, 2, false, false, 0, false)
	if err == nil {
		t.Fatal("provisioning under a held sid must fail: the drain owns it exclusively, and " +
			"a second durable on the same filter would deliver every new message twice")
	}
	if cc != nil || sub != nil {
		t.Fatalf("a refused provision must return nothing, got cc=%v sub=%v", cc, sub)
	}

	sess.mu.Lock()
	stillDraining, live := sess.drains[sid], sess.cons[sid]
	sess.mu.Unlock()
	if stillDraining != d {
		t.Fatal("the held drain must survive a refused provision")
	}
	if live != nil {
		t.Fatal("a refused provision must install no consumer membership (I7)")
	}
	if st := d.seqs[41]; st == nil || !st.intent || !st.claimed {
		t.Fatalf("the past-PUBREC intent must be untouched, got %+v", st)
	}
}
