package mqtt

import (
	"testing"
)

// F-083, and the shared-group lifecycle transfer. An UNSUBSCRIBE owes the same TWO different
// releases a failed shared SUBSCRIBE does, decided by two different instruments.
//
// Whether the GROUP consumer goes is the group record's arbitrated holder count, moved by the
// release of each ACQUISITION the doomed membership held: the removal write carries a release
// verdict per acquisition and the commit executes it. The pending deliveries and their reserved
// identifiers, the ack-budget charge, the deliver-subject subscription and the resident cons
// entries are this session's own, owed whatever the records say — with one exception that must
// never license a local release: THIS session still reaches the durable through another of its
// own keys.
//
// Two things make this caller different from F-082's, and both are re-derived here rather
// than assumed:
//
//   - the COMMIT half runs after sess.update has already removed the filters from sharedSubs,
//     so the live map IS the surviving membership and nothing needs excluding;
//   - the STAGING half runs BEFORE that, so the surviving membership is the live map minus
//     the PACKET's whole doomed set. One packet can leave both overlapping filters, and
//     excluding only the key in hand would let each protect the other.

// testUnsubEnv is a shared-release env plus the UNSUBSCRIBE-side helpers.
type testUnsubEnv struct {
	*testSharedReleaseEnv
}

// testNewUnsubEnv creates the group durables of every given key and returns the env.
//
// The keys are created through the production request builder, exactly as
// testNewSharedReleaseEnv does, so the fixture cannot drift from the names the release
// derives.
func testNewUnsubEnv(t *testing.T, clientID string, keys ...string) *testUnsubEnv {
	t.Helper()
	if len(keys) == 0 {
		t.Fatalf("the fixture needs at least one shared key")
	}
	x := testNewSharedReleaseEnv(t, clientID, keys[0])
	for _, key := range keys[1:] {
		group, subject, ok := mqttParseSharedKey(key)
		if !ok {
			t.Fatalf("the fixture's own key %q is not a shared session key", key)
		}
		for _, s := range testSharedReleaseSubjects(subject) {
			ccr := &CreateConsumerRequest{
				Stream: mqttStreamName,
				Config: buildSharedGroupConsumerConfig(group, s, 1, mqttDefaultAckWait, 1024, false),
			}
			if _, err := x.e.asm.jsa.createDurableConsumer(ccr); err != nil {
				t.Fatalf("creating the group consumer for %q: %v", s, err)
			}
		}
	}
	return &testUnsubEnv{testSharedReleaseEnv: x}
}

// setMembership acquires the session's shared memberships through the production path, which
// is what the surviving-key set is sampled from and what the ledger lists.
func (x *testUnsubEnv) setMembership(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		x.acquire(t, key)
	}
}

// removeMembership drops the keys an UNSUBSCRIBE's record write removes and writes their
// release verdicts, which is the state commitUnsub runs against; it returns the removed
// memberships, which is what the commit is handed.
func (x *testUnsubEnv) removeMembership(t *testing.T, keys ...string) map[string]mqttSharedMembership {
	t.Helper()
	return x.leave(t, keys...)
}

// consSids returns the session's current cons keys, so a test can assert the EXACT surviving
// set rather than the absence of one entry it happened to think of.
func (x *testUnsubEnv) consSids() map[string]struct{} {
	sess := x.e.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	out := make(map[string]struct{}, len(sess.cons))
	for sid := range sess.cons {
		out[sid] = struct{}{}
	}
	return out
}

// assertConsSids fails unless the session's cons keys are EXACTLY want.
//
// The exact set matters and a per-entry check does not substitute for it: the entry this
// pins is the level-up companion of the DOOMED key, whose durable SURVIVES because the plain
// key still holds it. A release keyed on owed durables alone leaves it behind, and every
// other assertion in this file still passes.
func (x *testUnsubEnv) assertConsSids(t *testing.T, what string, want ...string) {
	t.Helper()
	got := x.consSids()
	wantSet := make(map[string]struct{}, len(want))
	for _, sid := range want {
		wantSet[sid] = struct{}{}
	}
	for sid := range got {
		if _, ok := wantSet[sid]; !ok {
			t.Fatalf("%s: cons still holds %q, which no surviving membership names", what, sid)
		}
	}
	for sid := range wantSet {
		if _, ok := got[sid]; !ok {
			t.Fatalf("%s: cons lost %q, which a surviving membership still names", what, sid)
		}
	}
}

// runCommitUnsub drives the commit half through the PRODUCTION method commitUnsub calls,
// with the doomed memberships already removed and persisted as they are by then.
//
// It calls releaseUnsubSharedTargets rather than reimplementing the sequence. An earlier
// revision of this file did reimplement it, and every mutation of the production code passed:
// the fixtures never reached the route they named. A fixture that mirrors the code under test
// proves only that the mirror is self-consistent.
func (x *testUnsubEnv) runCommitUnsub(t *testing.T, tok recordTx, removed map[string]mqttSharedMembership) {
	t.Helper()
	x.e.sess.releaseUnsubSharedTargets(x.c, tok, removed)
}

// TestUnsubKeepsAnOverlappingOwnMembershipAtCommit is T1.
//
// A session holds "$share/g/foo" and "$share/g/foo.>" (the session-key form of the MQTT
// filter "$share/g/foo/#") and unsubscribes only the second. That key joins TWO group
// consumers, and the level-up one IS the plain filter's: same durable, same deliver subject.
// Everything a whole-durable release does would therefore land on a filter the client still
// holds, while the group record withholds only the CONSUMER.DELETE: the parent's count goes
// 2 -> 1 and its incarnation stays live.
//
// The cons assertion is the EXACT set: the "$share/g/foo fwc" entry belongs to the doomed key
// and must go even though its durable survives, which is the one thing an owed-durable-keyed
// sweep gets wrong.
func TestUnsubKeepsAnOverlappingOwnMembershipAtCommit(t *testing.T) {
	const clientID = "unsuboverlap"
	const doomed = "$share/g/foo.>"
	const surviving = "$share/g/foo"

	x := testNewUnsubEnv(t, clientID, doomed)
	x.setMembership(t, surviving, doomed)
	sess := x.e.sess
	sess.mu.Lock()
	doomedM, survM := sess.sharedSubs[doomed], sess.sharedSubs[surviving]
	sess.mu.Unlock()
	targets := mqttSharedGroupNamesFor(doomed, doomedM)
	if len(targets) != 2 {
		t.Fatalf("the fixture's own key %q joins %d group consumers, want 2", doomed, len(targets))
	}
	own, levelUp := targets[0], targets[1]
	if levelUp.durable != mqttSharedGroupNamesFor(surviving, survM)[0].durable {
		t.Fatalf("the fixture's premise is wrong: %q and %q do not share a durable", doomed, surviving)
	}
	for _, tg := range targets {
		if !x.consumerExists(t, tg.durable) {
			t.Fatalf("the fixture did not create the group consumer %q", tg.durable)
		}
	}

	sess.mu.Lock()
	baseline := sess.tmaxack
	sess.mu.Unlock()

	// The doomed key's own two cons entries, and the surviving plain key's.
	ownSub := x.seedJoin(t, own, 5, "n-own", doomed)
	survSub := x.seedJoin(t, levelUp, 6, "n-surv", doomed[:len(doomed)-2]+mqttMultiLevelSidSuffix, surviving)

	// The record write landed: the doomed membership is gone, the surviving one is not, and
	// the verdicts for BOTH of the doomed key's acquisitions are durable.
	removed := x.removeMembership(t, doomed)

	tok := sess.beginRecordTx()
	x.runCommitUnsub(t, tok, removed)
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	// The doomed key's own durable is owed in full, its group retires and its consumer goes.
	x.assertDischarged(t, "T1 doomed target", own, 5, baseline+1024, ownSub)
	x.assertSettled(t, "T1 own group", doomed, mqttSharedMembership{QoS: 1, Main: doomedM.Main}, 0)
	x.assertConsumer(t, own.durable, true)

	// The level-up durable is the SURVIVING plain filter's. Nothing local may be touched,
	// while the doomed key's acquisition on it IS released: the count goes 2 -> 1.
	x.assertUntouched(t, "T1 surviving target", levelUp, 6, survSub)
	x.assertSettled(t, "T1 parent group", surviving, mqttSharedMembership{QoS: 1, Main: *doomedM.Up}, 1)
	x.assertConsumer(t, levelUp.durable, false)

	// Exactly the surviving key's entry remains: both of the doomed key's are gone,
	// including the level-up companion whose durable survived.
	x.assertConsSids(t, "T1", surviving)
}

// TestUnsubDischargesOwnStateUnderARefusal is T5, and it is the pin for the whole property:
// a second holder or a group record that cannot be read withholds the GROUP CONSUMER and
// nothing else.
//
// The third arm is a permanent CONTROL. Without it every assertion here would pass against a
// release that deletes nothing, ever — and the mutation that routes a shared config into the
// plain teardown is caught by the second-holder arm precisely because that arm proves the
// consumer still stands when the count says another member holds it.
func TestUnsubDischargesOwnStateUnderARefusal(t *testing.T) {
	const clientID = "unsubrefusal"
	const key = "$share/g/foo"

	for _, tc := range []struct {
		name        string
		hold        func(t *testing.T, x *testUnsubEnv)
		consSids    []string
		wantHolders uint64
		wantDeleted bool
		withheld    bool
	}{
		{
			name: "a second holder keeps the consumer, not this session's state",
			hold: func(t *testing.T, x *testUnsubEnv) {
				x.foreignHold(t, "othermember", key)
			},
			consSids:    []string{key},
			wantHolders: 1,
			wantDeleted: false,
		},
		{
			// The no-cons population UNSUBSCRIBE's old branch discharged nothing for: a
			// persisted membership with no live join, whose charge, pending state and
			// deliver-subject subscription are all real. The group record cannot be read, so
			// the release cannot be executed and its verdict stands for a later executor.
			name: "an unreadable group record withholds the execution, and there is no cons entry",
			hold: func(t *testing.T, x *testUnsubEnv) {
				x.withholdGroupRecord(t, key)
			},
			wantHolders: 1,
			wantDeleted: false,
			withheld:    true,
		},
		{
			name:        "control, the only holder leaves and the consumer goes",
			consSids:    []string{key},
			wantHolders: 0,
			wantDeleted: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := testNewUnsubEnv(t, clientID, key)
			m := x.acquire(t, key)
			target := mqttSharedGroupNamesFor(key, m)[0]
			if !x.consumerExists(t, target.durable) {
				t.Fatalf("the fixture did not create the group consumer %q", target.durable)
			}

			sess := x.e.sess
			sess.mu.Lock()
			baseline := sess.tmaxack
			sess.mu.Unlock()

			sub := x.seedJoin(t, target, 5, "n-unsub", tc.consSids...)
			if tc.hold != nil {
				tc.hold(t, x)
			}
			removed := x.removeMembership(t, key)

			tok := sess.beginRecordTx()
			x.runCommitUnsub(t, tok, removed)
			if err := sess.endRecordTx(tok); err != nil {
				t.Fatalf("closing the record transaction: %v", err)
			}

			x.assertDischarged(t, tc.name, target, 5, baseline, sub)
			if tc.withheld {
				x.assertVerdictPending(t, tc.name, m)
			} else {
				x.assertSettled(t, tc.name, key, m, tc.wantHolders)
			}
			x.assertConsumer(t, target.durable, tc.wantDeleted)
		})
	}
}

// TestUnsubSweepsEveryConsEntryNamingADoomedDurable is T6.
//
// Neither stale entry may use one of the doomed filter's real sids: those are removed by the
// doomed-config step regardless, so seeding one there would leave a single entry for the
// sweep and a first-match sweep would still pass.
func TestUnsubSweepsEveryConsEntryNamingADoomedDurable(t *testing.T) {
	const clientID = "unsubsweep"
	const key = "$share/g/foo"

	x := testNewUnsubEnv(t, clientID, key)
	m := x.acquire(t, key)
	target := mqttSharedGroupNamesFor(key, m)[0]

	sess := x.e.sess
	sess.mu.Lock()
	baseline := sess.tmaxack
	sess.mu.Unlock()

	// Two entries naming the durable, NEITHER of them one of mqttUnsubSids(key).
	sub := x.seedJoin(t, target, 5, "n-sweep", key+"-stale-a", key+"-stale-b")
	removed := x.removeMembership(t, key)

	tok := sess.beginRecordTx()
	x.runCommitUnsub(t, tok, removed)
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	x.assertDischarged(t, "T6", target, 5, baseline, sub)
	x.assertConsSids(t, "T6")
}

// TestUnsubStagesTheNoConsDurableFully is T7, in three arms.
//
// Asserting the barrier alone leaves the name-keyed void unpinned: the commit half purges the
// same state later, so omitting the void entirely still passes every other test here.
//
// Arm (c) seeds a TRANSMITTED registration (regPending false). A register-pending
// cancellation is deliberately NOT unvoided on rollback, so seeding one would assert the
// opposite of correct behaviour.
func TestUnsubStagesTheNoConsDurableFully(t *testing.T) {
	const clientID = "unsubstage"
	const key = "$share/g/foo"
	const nonce = "n-stage"

	newStaged := func(t *testing.T) (*testUnsubEnv, mqttSharedGroupNames, *subscription,
		[]mqttRegCancellation, []*subscription, []string) {

		t.Helper()
		x := testNewUnsubEnv(t, clientID, key)
		target := mqttSharedGroupNamesFor(key, x.acquire(t, key))[0]
		// No cons entry: the persisted membership with no live join.
		sub := x.seedJoin(t, target, 5, nonce)

		sess := x.e.sess
		sess.mu.Lock()
		cancels, closed, voided := sess.stageUnsubCancellationsLocked(
			x.c, []*mqttFilter{{shared: true, group: "g", filter: "foo"}}, nil)
		sess.mu.Unlock()
		return x, target, sub, cancels, closed, voided
	}

	t.Run("a the barrier is lowered by deliver subject", func(t *testing.T) {
		x, _, sub, _, closed, _ := newStaged(t)
		if sub.mqtt == nil || !sub.mqtt.closed {
			t.Fatalf("T7(a): the no-cons deliver subscription was not closed at staging")
		}
		if len(closed) != 1 || closed[0] != sub {
			t.Fatalf("T7(a): the closed-subscription list does not carry the barriered sub (%d entries)", len(closed))
		}
		_ = x
	})

	t.Run("b the registrations are voided by durable", func(t *testing.T) {
		x, target, _, _, _, voided := newStaged(t)
		found := false
		for _, n := range voided {
			if n == nonce {
				found = true
			}
		}
		if !found {
			t.Fatalf("T7(b): the transmitted nonce %q of %q was not voided at staging", nonce, target.durable)
		}
		// The VERDICT is what the void records for a transmitted registration, and the
		// returned list alone does not prove it was written: voidRegistrationsLocked
		// deliberately leaves cpending and the identifier alone here — the reap frees those,
		// and since F-034 it may decline to. So the durable state is the oracle.
		sess := x.e.sess
		sess.mu.Lock()
		_, voidedDurably := sess.cancelledRegNonces[nonce]
		sess.mu.Unlock()
		if !voidedDurably {
			t.Fatalf("T7(b): the void verdict for %q was not recorded on the session", nonce)
		}
	})

	t.Run("c a definite failure reopens and unvoids", func(t *testing.T) {
		x, _, sub, _, closed, voided := newStaged(t)
		sess := x.e.sess
		// Exactly what undoUnsub does when the record write DEFINITELY did not land.
		sess.reopenTeardownDelivery(closed)
		sess.unvoidRegNonces(voided)

		if sub.mqtt != nil && sub.mqtt.closed {
			t.Fatalf("T7(c): the delivery stayed closed after a packet that provably did not land")
		}
		if len(voided) == 0 {
			t.Fatalf("T7(c): nothing was voided, so the unvoid assertion proves nothing")
		}
		sess.mu.Lock()
		defer sess.mu.Unlock()
		for _, n := range voided {
			if _, stillVoid := sess.cancelledRegNonces[n]; stillVoid {
				t.Fatalf("T7(c): the nonce %q is still voided after the rollback", n)
			}
		}
	})
}

// TestUnsubStagingExcludesEveryDoomedKeyInThePacket is T3.
//
// One packet leaves BOTH overlapping filters. Neither doomed key may protect the other, so
// both durables must be barriered and voided — which an exclusion of only the key in hand
// gets exactly wrong, suppressing every closure.
func TestUnsubStagingExcludesEveryDoomedKeyInThePacket(t *testing.T) {
	const clientID = "unsubbothkeys"
	const multi = "$share/g/foo.>"
	const plain = "$share/g/foo"

	x := testNewUnsubEnv(t, clientID, multi)
	x.setMembership(t, plain)
	targets := mqttSharedGroupNamesFor(multi, x.acquire(t, multi))
	own, levelUp := targets[0], targets[1]

	ownSub := x.seedJoin(t, own, 5, "n-multi")
	levelSub := x.seedJoin(t, levelUp, 6, "n-plain")

	sess := x.e.sess
	sess.mu.Lock()
	_, closed, voided := sess.stageUnsubCancellationsLocked(x.c, []*mqttFilter{
		{shared: true, group: "g", filter: "foo.>"},
		{shared: true, group: "g", filter: "foo"},
	}, nil)
	sess.mu.Unlock()

	if ownSub.mqtt == nil || !ownSub.mqtt.closed {
		t.Fatalf("T3: %q was not barriered — a doomed key protected it", own.durable)
	}
	if levelSub.mqtt == nil || !levelSub.mqtt.closed {
		t.Fatalf("T3: %q was not barriered — the other doomed key protected it", levelUp.durable)
	}
	if len(closed) != 2 {
		t.Fatalf("T3: %d subscriptions barriered, want both", len(closed))
	}
	for _, want := range []string{"n-multi", "n-plain"} {
		found := false
		for _, n := range voided {
			if n == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("T3: the nonce %q was not voided; a doomed key suppressed its own release", want)
		}
	}
}

// TestUnsubBarriersOnlyTheDoomedDurablesAtStaging is T2: the staging half's own fence.
//
// The surviving plain filter's delivery must still be open and its registrations unvoided
// when the doomed multi-level key leaves alone. That is the staging-side twin of T1, and it
// fails separately from T1 so the two fences are pinned independently.
func TestUnsubBarriersOnlyTheDoomedDurablesAtStaging(t *testing.T) {
	const clientID = "unsubstagefence"
	const doomed = "$share/g/foo.>"
	const surviving = "$share/g/foo"

	x := testNewUnsubEnv(t, clientID, doomed)
	x.setMembership(t, surviving)
	targets := mqttSharedGroupNamesFor(doomed, x.acquire(t, doomed))
	own, levelUp := targets[0], targets[1]

	ownSub := x.seedJoin(t, own, 5, "n-doomed")
	survSub := x.seedJoin(t, levelUp, 6, "n-survivor")

	sess := x.e.sess
	sess.mu.Lock()
	_, closed, voided := sess.stageUnsubCancellationsLocked(
		x.c, []*mqttFilter{{shared: true, group: "g", filter: "foo.>"}}, nil)
	sess.mu.Unlock()

	if ownSub.mqtt == nil || !ownSub.mqtt.closed {
		t.Fatalf("T2: the doomed durable %q was not barriered", own.durable)
	}
	if survSub.mqtt != nil && survSub.mqtt.closed {
		t.Fatalf("T2: the SURVIVING filter's delivery was closed at staging")
	}
	if len(closed) != 1 {
		t.Fatalf("T2: %d subscriptions barriered, want only the doomed one", len(closed))
	}
	for _, n := range voided {
		if n == "n-survivor" {
			t.Fatalf("T2: the surviving filter's registration was voided at staging")
		}
	}
	sess.mu.Lock()
	_, survPending := sess.cpending[levelUp.durable]
	sess.mu.Unlock()
	if !survPending {
		t.Fatalf("T2: the surviving filter's pending deliveries were purged at staging")
	}
}

// TestUnsubDischargesLocalStateWithoutACons is T4: the no-cons population at the COMMIT half.
//
// The old branch derived the durables from the key, gated the delete on the scan, and purged
// nothing — so an UNSUBSCRIBE the record says happened left the pending deliveries, their
// identifiers, the charge and the deliver-subject subscription behind.
func TestUnsubDischargesLocalStateWithoutACons(t *testing.T) {
	const clientID = "unsubnocons"
	const key = "$share/g/foo"

	x := testNewUnsubEnv(t, clientID, key)
	m := x.acquire(t, key)
	target := mqttSharedGroupNamesFor(key, m)[0]

	sess := x.e.sess
	sess.mu.Lock()
	baseline := sess.tmaxack
	sess.mu.Unlock()

	sub := x.seedJoin(t, target, 5, "n-nocons") // no cons sids at all
	removed := x.removeMembership(t, key)

	tok := sess.beginRecordTx()
	x.runCommitUnsub(t, tok, removed)
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	x.assertDischarged(t, "T4", target, 5, baseline, sub)
	x.assertSettled(t, "T4", key, m, 0)
	x.assertConsumer(t, target.durable, true)
}

// TestSubscribeRollbackKeepsAnOverlappingOwnMembership is T8, the FOURTH site (F-083's D-d),
// and it drives rollbackSubscribePacket itself so BOTH halves are exercised: the staging
// barrier/void inline in that function, and the release in finishSubscribeRollback.
//
// A session already holds "$share/g/foo". A SUBSCRIBE adds "$share/g/foo.>", whose level-up
// companion joins the durable the plain filter already holds — under a DIFFERENT cons key,
// so rollbackSubscribePacket counts it among what the packet "added". Its save then fails and
// the packet rolls back. Without the fence the rollback closes the surviving filter's
// delivery, withdraws its registrations, purges its pending state and refunds its charge,
// while the group record withholds only the CONSUMER.DELETE.
//
// The authority is the PRE-PACKET snapshot, not the live map: at staging time sess.sharedSubs
// still names the very key being rolled back. The rollback's record write carries the release
// verdicts for the acquisitions the packet made — BOTH of them, the overlapping one included:
// the parent's count goes 2 -> 1 while its local state is untouched.
func TestSubscribeRollbackKeepsAnOverlappingOwnMembership(t *testing.T) {
	const clientID = "subrollbackoverlap"
	const added = "$share/g/foo.>"
	const surviving = "$share/g/foo"

	x := testNewUnsubEnv(t, clientID, added)

	// The pre-packet membership holds ONLY the plain filter; the packet attempted the
	// multi-level one, so the live map names both.
	x.setMembership(t, surviving)
	sess := x.e.sess
	// snapshotSubs takes sess.mu itself, so it is called OUTSIDE the critical section below.
	snap := sess.snapshotSubs()
	sess.mu.Lock()
	consSnap := make(map[string]*ConsumerConfig, len(sess.cons))
	for sid, cfg := range sess.cons {
		consSnap[sid] = cfg
	}
	sess.mu.Unlock()
	// What the attempted packet added: acquired and eagerly saved, as its per-filter J1/J2 do.
	addedM := x.acquire(t, added)
	targets := mqttSharedGroupNamesFor(added, addedM)
	own, levelUp := targets[0], targets[1]
	sess.mu.Lock()
	baseline := sess.tmaxack
	sess.mu.Unlock()

	// What the packet created: its own durable, and the level-up companion whose durable is
	// the SURVIVING plain filter's.
	addedSub := x.seedJoin(t, own, 5, "n-added", added)
	survSub := x.seedJoin(t, levelUp, 6, "n-surv", added[:len(added)-2]+mqttMultiLevelSidSuffix)

	tok := sess.beginRecordTx()
	sess.rollbackSubscribePacket(x.c, tok, nil, consSnap, snap, true)
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	// The packet's OWN durable is released in full, its group retires and its consumer goes.
	x.assertDischarged(t, "T8 packet's own target", own, 5, baseline+1024, addedSub)
	x.assertSettled(t, "T8 own group", added, mqttSharedMembership{QoS: 1, Main: addedM.Main}, 0)
	x.assertConsumer(t, own.durable, true)

	// The pre-existing plain filter is untouched, in BOTH halves: still delivering (staging
	// barrier not lowered) and still holding its pending state and charge (release skipped).
	// Its group's count still moves: the packet's acquisition on it is released, 2 -> 1.
	x.assertUntouched(t, "T8 surviving target", levelUp, 6, survSub)
	x.assertSettled(t, "T8 parent group", surviving, mqttSharedMembership{QoS: 1, Main: *addedM.Up}, 1)
	x.assertConsumer(t, levelUp.durable, false)
}

// TestSubscribeRollbackStillReleasesWhatItCreated is T10, the CONTROL for T8 and the paired
// test for M10.
//
// Without it, a fence that suppresses EVERYTHING passes T8 — T8 only asserts that the
// survivor is untouched. This arm has no overlap at all, so every target the packet created
// is owed and must be released and deleted.
func TestSubscribeRollbackStillReleasesWhatItCreated(t *testing.T) {
	const clientID = "subrollbackplain"
	const added = "$share/g/bar"

	x := testNewUnsubEnv(t, clientID, added)

	// Nothing survives this rollback: the pre-packet membership is empty.
	sess := x.e.sess
	snap := sess.snapshotSubs()
	sess.mu.Lock()
	consSnap := make(map[string]*ConsumerConfig, len(sess.cons))
	for sid, cfg := range sess.cons {
		consSnap[sid] = cfg
	}
	sess.mu.Unlock()
	addedM := x.acquire(t, added)
	target := mqttSharedGroupNamesFor(added, addedM)[0]
	sess.mu.Lock()
	baseline := sess.tmaxack
	sess.mu.Unlock()

	sub := x.seedJoin(t, target, 5, "n-plainadd", added)

	tok := sess.beginRecordTx()
	sess.rollbackSubscribePacket(x.c, tok, nil, consSnap, snap, true)
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	x.assertDischarged(t, "T10", target, 5, baseline, sub)
	x.assertSettled(t, "T10", added, addedM, 0)
	x.assertConsumer(t, target.durable, true)
}

// TestSubscribeRollbackKeepsAPlainDollarShareSubscription pins the rollback half of the
// no-lexical-classification rule, which T12 does not reach: T12 exercises UNSUBSCRIBE, and
// this defect lives in finishSubscribeRollback's existedBefore.
//
// A plain filter "$share.foo" has the sid "$share//foo" (a literal '.' encodes as "//"), so
// classifying the sid by its "$share/" prefix looked the key up in snap.shared alone, found
// nothing, and concluded the subscription did not exist before the packet — unsubscribing a
// subscription the rollback is supposed to RESTORE.
func TestSubscribeRollbackKeepsAPlainDollarShareSubscription(t *testing.T) {
	const clientID = "rollbackplaindollar"
	// The NATS-subject form of the plain MQTT filter "$share.foo".
	const plainSid = "$share//foo"

	x := testNewUnsubEnv(t, clientID, "$share/g/unrelated")
	sess := x.e.sess

	// The subscription predates the packet: it is in the pre-packet membership snapshot.
	sess.mu.Lock()
	sess.subs = map[string]byte{plainSid: 1}
	sess.mu.Unlock()
	snap := sess.snapshotSubs()

	sub := &subscription{client: x.c, subject: []byte(plainSid), sid: []byte(plainSid), mqtt: &mqttSub{}}
	x.c.mu.Lock()
	if x.c.subs == nil {
		x.c.subs = make(map[string]*subscription)
	}
	x.c.subs[plainSid] = sub
	x.c.mu.Unlock()

	sess.mu.Lock()
	consSnap := make(map[string]*ConsumerConfig, len(sess.cons))
	for sid, cfg := range sess.cons {
		consSnap[sid] = cfg
	}
	sess.mu.Unlock()

	tok := sess.beginRecordTx()
	sess.rollbackSubscribePacket(x.c, tok, []*subscription{sub}, consSnap, snap, true)
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	x.c.mu.Lock()
	_, survived := x.c.subs[plainSid]
	x.c.mu.Unlock()
	if !survived {
		t.Fatalf("the rollback unsubscribed the pre-existing plain subscription %q", plainSid)
	}
}
