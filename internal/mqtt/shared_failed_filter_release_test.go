package mqtt

import (
	"testing"
	"time"
)

// F-082. A live shared SUBSCRIBE whose filter failed owes TWO different releases, and the
// last-holder scan answers only one of them.
//
// `held[durable]` authorizes destroying the GROUP consumer, which is other members' business.
// Everything else the loop body does — the resident cons entry, the deliver-subject
// subscription, the pending deliveries and their reserved identifiers, and the ack-budget
// charge — is this session's own state, owed whatever the answer. And the answer merges a
// third fact that must never license a release: that THIS session still reaches the durable
// through another of its own keys, which is the overlapping level-up case.
//
// The fixtures here seed the state a failing packet really leaves. That matters most for the
// shape with NO cons entry, which is the COMMON one: addJSConsToSess runs only once the whole
// filter has succeeded, so the join a failing filter made is invisible to a cons walk while
// its charge, its pending state and its subscription are all real.

// testSharedReleaseEnv is a session with a real JetStream, a client to hang delivery
// subscriptions on, and one group consumer really created on $MQTT_msgs.
type testSharedReleaseEnv struct {
	e   *testOrphanEnv
	c   *client
	key string
}

// testNewSharedReleaseEnv creates the group durables of key and returns the env.
//
// The consumers are created through the production request builder, so the fixture cannot
// drift from the names the release derives.
func testNewSharedReleaseEnv(t *testing.T, clientID, key string) *testSharedReleaseEnv {
	t.Helper()
	e := testNewOrphanEnv(t, clientID)
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		t.Fatalf("the fixture's own key %q is not a shared session key", key)
	}
	// One consumer per target, built exactly as processSharedJSConsumer builds them.
	for _, s := range testSharedReleaseSubjects(subject) {
		ccr := &CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: buildSharedGroupConsumerConfig(group, s, mqttDefaultAckWait, 1024, false),
		}
		if _, err := e.asm.jsa.createDurableConsumer(ccr); err != nil {
			t.Fatalf("creating the group consumer for %q: %v", s, err)
		}
	}
	return &testSharedReleaseEnv{e: e, c: e.asm.jsa.c, key: key}
}

// testSharedReleaseSubjects is the NATS subject(s) a shared key's group consumers filter on:
// the subject itself plus its level-up parent for a multi-level filter.
func testSharedReleaseSubjects(subject string) []string {
	out := []string{subject}
	if mqttNeedSubForLevelUp(subject) {
		out = append(out, subject[:len(subject)-2])
	}
	return out
}

// consumerExists reports whether the group durable is still on $MQTT_msgs.
func (x *testSharedReleaseEnv) consumerExists(t *testing.T, durable string) bool {
	t.Helper()
	cons, err := x.e.asm.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("listing consumers: %v", err)
	}
	for _, ci := range cons {
		if ci != nil && ci.Name == durable {
			return true
		}
	}
	return false
}

// seedJoin seeds everything a shared join leaves on the session for one durable: the
// ack-budget charge, one TRANSMITTED pending delivery holding a packet identifier and a
// register nonce, and the deliver-subject subscription. withCons additionally seeds resident
// cons entries — which a FAILED filter would NOT have, so it is off for the common shape.
func (x *testSharedReleaseEnv) seedJoin(t *testing.T, target mqttSharedGroupNames, pi uint16,
	nonce string, consSids ...string) *subscription {

	t.Helper()
	sess := x.e.sess
	sub := &subscription{client: x.c, subject: []byte(target.deliver), sid: []byte(target.deliver), mqtt: &mqttSub{}}
	x.c.mu.Lock()
	if x.c.subs == nil {
		x.c.subs = make(map[string]*subscription)
	}
	x.c.subs[target.deliver] = sub
	x.c.mu.Unlock()

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.sharedCharged == nil {
		sess.sharedCharged = make(map[string]int)
	}
	sess.sharedCharged[target.durable] = 1024
	sess.tmaxack += 1024
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}
	sess.cpending[target.durable] = map[uint64]uint16{7: pi}
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	// Transmitted, not register-pending: its identifier is the release's to free and its
	// nonce is what the void verdict names.
	sess.pendingPublish[pi] = &mqttPending{regNonce: nonce, regSeq: 0}
	for _, sid := range consSids {
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[sid] = &ConsumerConfig{
			Durable:        target.durable,
			DeliverSubject: target.deliver,
			DeliverGroup:   "g",
		}
	}
	return sub
}

// assertDischarged is the WHOLE-SET oracle for one target: nothing this session held for that
// durable may remain. Asserting the set rather than one field is what stops a partial release
// passing.
func (x *testSharedReleaseEnv) assertDischarged(t *testing.T, what string, target mqttSharedGroupNames,
	pi uint16, baseline int, sub *subscription) {

	t.Helper()
	sess := x.e.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for sid, cfg := range sess.cons {
		if cfg != nil && cfg.Durable == target.durable {
			t.Fatalf("%s: the cons entry %q still names the released durable", what, sid)
		}
	}
	if _, ok := sess.sharedCharged[target.durable]; ok {
		t.Fatalf("%s: the ack-budget charge for %q was not released", what, target.durable)
	}
	if got := sess.tmaxack; got != baseline {
		t.Fatalf("%s: tmaxack = %d, want the pre-join baseline %d", what, got, baseline)
	}
	if _, ok := sess.cpending[target.durable]; ok {
		t.Fatalf("%s: the pending deliveries of %q were not purged", what, target.durable)
	}
	if _, ok := sess.pendingPublish[pi]; ok {
		t.Fatalf("%s: packet identifier %d is still reserved", what, pi)
	}
	if sub.mqtt != nil && !sub.mqtt.closed {
		t.Fatalf("%s: the deliver subscription was not closed", what)
	}
	x.c.mu.Lock()
	_, stillSubscribed := x.c.subs[target.deliver]
	x.c.mu.Unlock()
	if stillSubscribed {
		t.Fatalf("%s: the deliver-subject subscription %q was not removed", what, target.deliver)
	}
}

// assertUntouched is assertDischarged's opposite, for a durable this session still holds
// through another of its own keys. Everything must survive: this is the arm that fails if the
// release ever treats "held" as one fact.
func (x *testSharedReleaseEnv) assertUntouched(t *testing.T, what string, target mqttSharedGroupNames,
	pi uint16, sub *subscription) {

	t.Helper()
	sess := x.e.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if _, ok := sess.sharedCharged[target.durable]; !ok {
		t.Fatalf("%s: the surviving membership's charge for %q was refunded", what, target.durable)
	}
	if _, ok := sess.cpending[target.durable]; !ok {
		t.Fatalf("%s: the surviving membership's pending deliveries for %q were purged", what, target.durable)
	}
	if _, ok := sess.pendingPublish[pi]; !ok {
		t.Fatalf("%s: the surviving membership's packet identifier %d was freed", what, pi)
	}
	if sub.mqtt != nil && sub.mqtt.closed {
		t.Fatalf("%s: the surviving membership's delivery was closed", what)
	}
	x.c.mu.Lock()
	_, stillSubscribed := x.c.subs[target.deliver]
	x.c.mu.Unlock()
	if !stillSubscribed {
		t.Fatalf("%s: the surviving membership's subscription %q was removed", what, target.deliver)
	}
}

// assertConsumer settles the group durable's fate, and the two directions need DIFFERENT
// oracles because the delete is fire-and-forget: releaseSharedGroupTarget issues
// deleteConsumer with noWait, so "gone" arrives asynchronously while "still there" is only
// meaningful if it holds for a while. An immediate check in either direction passes or fails
// on timing — which is how an earlier revision of this file reported a mutation as
// discriminating when it had only been lucky.
func (x *testSharedReleaseEnv) assertConsumer(t *testing.T, durable string, wantDeleted bool) {
	t.Helper()
	if wantDeleted {
		deadline := time.Now().Add(5 * time.Second)
		for x.consumerExists(t, durable) {
			if time.Now().After(deadline) {
				t.Fatalf("the group consumer %q survived although the scan reported it unheld", durable)
			}
			time.Sleep(20 * time.Millisecond)
		}
		return
	}
	// Deleting under a refusal destroys every member's interest-retained backlog, so this
	// direction is held over a window rather than sampled once: a delete in flight when the
	// first look happened would otherwise read as a pass.
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		if !x.consumerExists(t, durable) {
			t.Fatalf("the group consumer %q was DELETED although the scan withheld the answer", durable)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSharedFailedFilterReleaseDischargesOwnStateUnderARefusal is T1-T3: the three
// dispositions of one target, driven through releaseFailedSharedFilter itself.
//
// T3 (nothing planted, so the scan reports unheld) is a permanent CONTROL, not decoration: it
// is the only arm that proves the group consumer is still deleted when it may be. Without it
// every assertion in T1 and T2 would pass against a release that deletes nothing, ever.
func TestSharedFailedFilterReleaseDischargesOwnStateUnderARefusal(t *testing.T) {
	const clientID = "failedfilter"
	const key = "$share/g/foo"
	const nonce = "n-release"

	for _, tc := range []struct {
		name string
		// plant makes the scan answer for the durable; nil leaves it unheld.
		plant func(t *testing.T, e *testOrphanEnv)
		// consSids is empty for the no-cons shape a failed filter really leaves.
		consSids    []string
		wantDeleted bool
	}{
		{
			// T1. A well-formed foreign record naming the durable: another member holds it,
			// so the consumer stays while this session's own state goes. TWO stale cons
			// entries, because a durable can be named by more than one and a first-match
			// sweep would leave the second stranded with no membership to reach it by.
			name: "T1 foreign holder with two stale cons entries",
			plant: func(t *testing.T, e *testOrphanEnv) {
				testPlantSessionRecordJSON(t, e.asm, "othermember", &mqttPersistedSession{
					ID:         "othermember",
					SharedSubs: map[string]byte{key: 1},
				})
			},
			consSids:    []string{key, key + "-stale"},
			wantDeleted: false,
		},
		{
			// T2. The COMMON shape: no cons entry at all, and the scan REFUSES because a
			// record cannot be read. Both halves matter — a cons-keyed release discharges
			// nothing here, and a held-gated one discharges nothing either.
			name: "T2 unreadable record and no cons entry",
			plant: func(t *testing.T, e *testOrphanEnv) {
				testPlantSessionRecord(t, e.asm, "corrupt", []byte("{not json"))
			},
			wantDeleted: false,
		},
		{
			name:        "T3 control, the scan reports unheld",
			wantDeleted: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := testNewSharedReleaseEnv(t, clientID, key)
			target := mqttSharedGroupNamesForKey(key)[0]
			if !x.consumerExists(t, target.durable) {
				t.Fatalf("the fixture did not create the group consumer %q", target.durable)
			}

			x.e.sess.mu.Lock()
			baseline := x.e.sess.tmaxack
			x.e.sess.mu.Unlock()

			sub := x.seedJoin(t, target, 5, nonce, tc.consSids...)
			if tc.plant != nil {
				tc.plant(t, x.e)
			}

			tok := x.e.sess.beginRecordTx()
			x.e.sess.releaseFailedSharedFilter(x.c, tok, &mqttFilter{shared: true, group: "g", filter: "foo"})
			if err := x.e.sess.endRecordTx(tok); err != nil {
				t.Fatalf("closing the record transaction: %v", err)
			}

			x.assertDischarged(t, tc.name, target, 5, baseline, sub)
			x.assertConsumer(t, target.durable, tc.wantDeleted)
		})
	}
}

// TestSharedFailedFilterReleaseKeepsAnOverlappingOwnMembership is T4, and it is the mixed
// target set rather than the overlap alone.
//
// The key "$share/g/foo.>" — the session-key form of the MQTT filter "$share/g/foo/#", whose
// subject part is a NATS subject — joins TWO group consumers: one for "foo.>" and one for its
// level-up parent "foo". That parent IS the consumer of the plain filter "$share/g/foo". So a
// session holding the plain filter whose "/#" SUBSCRIBE fails has one OWED target and one it
// must not touch, in a single call. Asserting only the preserved half would let a whole-filter rule
// ("if any target is own-held, return") pass every other test in this file.
func TestSharedFailedFilterReleaseKeepsAnOverlappingOwnMembership(t *testing.T) {
	const clientID = "overlapfilter"
	const failing = "$share/g/foo.>"
	const surviving = "$share/g/foo"

	x := testNewSharedReleaseEnv(t, clientID, failing)
	targets := mqttSharedGroupNamesForKey(failing)
	if len(targets) != 2 {
		t.Fatalf("the multi-level key joins %d group consumers, want 2: %+v", len(targets), targets)
	}
	owed, ownHeld := targets[0], targets[1]
	if ownHeld.durable != mqttSharedGroupNamesForKey(surviving)[0].durable {
		t.Fatalf("the fixture's premise is wrong: %q does not share a durable with %q", failing, surviving)
	}

	// The surviving membership is the only reason ownHeld must be spared. The failing key is
	// NOT in sharedSubs: its removal has already been proven durable, which is what running
	// this continuation means.
	x.e.sess.mu.Lock()
	if x.e.sess.sharedSubs == nil {
		x.e.sess.sharedSubs = make(map[string]byte)
	}
	x.e.sess.sharedSubs[surviving] = 1
	baseline := x.e.sess.tmaxack
	x.e.sess.mu.Unlock()

	owedSub := x.seedJoin(t, owed, 11, "n-owed")
	keptSub := x.seedJoin(t, ownHeld, 12, "n-kept", surviving)
	// Only the OWED join's charge comes back, so the expected total is the state after both
	// joins minus that one charge — an exact figure, not "zero" and not "less than before".
	x.e.sess.mu.Lock()
	baselineWithKept := x.e.sess.tmaxack - 1024
	x.e.sess.mu.Unlock()
	_ = baseline

	tok := x.e.sess.beginRecordTx()
	x.e.sess.releaseFailedSharedFilter(x.c, tok, &mqttFilter{shared: true, group: "g", filter: "foo.>"})
	if err := x.e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	x.assertDischarged(t, "T4 owed half", owed, 11, baselineWithKept, owedSub)
	x.assertUntouched(t, "T4 preserved half", ownHeld, 12, keptSub)

	// Nothing planted, so the owed target's own consumer really is unheld and must go...
	x.assertConsumer(t, owed.durable, true)
	// ...while the overlapping one is held by this session's surviving membership.
	x.assertConsumer(t, ownHeld.durable, false)
}

// testRollbackSharedFilter drives the production rollback for one failed shared filter and
// returns its verdict, with the record transaction opened and closed the way the packet does.
func (x *testSharedReleaseEnv) testRollbackSharedFilter(t *testing.T, key string, priorExisted bool,
	priorQoS byte) (bool, error) {

	t.Helper()
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		t.Fatalf("the fixture's own key %q is not a shared session key", key)
	}
	f := &mqttFilter{shared: true, group: group, filter: subject, qos: 1}
	tok := x.e.sess.beginRecordTx()
	run, _, err := x.e.sess.rollbackSharedSub(x.c, tok, f, true, priorExisted, priorQoS, false, 0)
	if terr := x.e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("closing the record transaction: %v", terr)
	}
	return run, err
}

// TestSharedFailedFilterRollbackRegistersOnlyWhatIsOwed is T5.
//
// undo() is shared by both branches of rollbackSharedSub, so before F-082 the prior-existing
// branch — whose write is an ADD that KEEPS the membership — registered the same landed
// release as the removal branch. Nothing was owed there, and the only thing standing between
// that continuation and a live membership's group consumers is the own-hold fence inside it.
//
// The positive control is half the test: a fixture that only checked "the restore branch
// registers nothing" would pass against a rollback that registers nothing at all, ever.
func TestSharedFailedFilterRollbackRegistersOnlyWhatIsOwed(t *testing.T) {
	const key = "$share/g/foo"

	for _, tc := range []struct {
		name         string
		priorExisted bool
		wantRetained int
	}{
		{"T5 the restore branch owes no release", true, 0},
		{"T5 control, the removal branch owes one", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := testNewSharedReleaseEnv(t, "rollbackowed", key)
			x.e.sess.mu.Lock()
			if x.e.sess.sharedSubs == nil {
				x.e.sess.sharedSubs = make(map[string]byte)
			}
			x.e.sess.sharedSubs[key] = 1
			x.e.sess.mu.Unlock()

			// The rollback's own save is issued and never reaches the stream, which is the
			// AMBIGUOUS half — the only outcome that registers a landed continuation at all.
			mqttTestSessStoreSuppressV.Store(true)
			t.Cleanup(func() { mqttTestSessStoreSuppressV.Store(false) })

			if _, err := x.testRollbackSharedFilter(t, key, tc.priorExisted, 2); err == nil {
				t.Fatal("the suppressed rollback save reported success, so this case never reached undo()")
			}
			if !x.e.sess.isSeqUncertain() {
				t.Fatal("the suppressed save did not arm uncertainty, so undo() took the DEFINITE arm")
			}
			if got := len(x.e.sess.takeUncertainActions()); got != tc.wantRetained {
				t.Fatalf("%d landed continuation(s) registered, want %d", got, tc.wantRetained)
			}
		})
	}
}

// TestSharedFailedFilterRollbackBarriersTheAbandonedJoin is T6 and T7: the STAGING half.
//
// The barrier has to be down before the register snapshot beside it, or that snapshot is only
// a moment in time and a delivery arriving after it allocates an identifier whose cancellation
// misses the membership-removal write. Before F-082 the barrier was lowered by walking
// sess.cons, so for the abandoned join of a FAILED filter — which the failing attempt has not
// written there — it was never lowered at all.
//
// T7 additionally pins the doomed key's EXCLUSION from the ownership test. This runs BEFORE
// the removal, so sharedSubs still names the failing key; counting it would make every closure
// suppress itself, and since the doomed key here owns BOTH durables, T6 would go with it.
func TestSharedFailedFilterRollbackBarriersTheAbandonedJoin(t *testing.T) {
	const failing = "$share/g/foo.>"
	const surviving = "$share/g/foo"

	for _, tc := range []struct {
		name string
		// alsoHold is a second membership of this session, or empty.
		alsoHold string
		// consFor seeds a resident cons entry for the OWED target, so a cons-keyed barrier
		// would find it. Off for T6, which is the shape that has none.
		consForOwed bool
	}{
		{name: "T6 no cons entry for the abandoned join"},
		{name: "T7 an overlapping own membership survives", alsoHold: surviving, consForOwed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := testNewSharedReleaseEnv(t, "rollbackbarrier", failing)
			targets := mqttSharedGroupNamesForKey(failing)
			if len(targets) != 2 {
				t.Fatalf("the multi-level key joins %d group consumers, want 2", len(targets))
			}
			owed, overlap := targets[0], targets[1]

			x.e.sess.mu.Lock()
			if x.e.sess.sharedSubs == nil {
				x.e.sess.sharedSubs = make(map[string]byte)
			}
			x.e.sess.sharedSubs[failing] = 1
			if tc.alsoHold != _EMPTY_ {
				x.e.sess.sharedSubs[tc.alsoHold] = 1
			}
			x.e.sess.mu.Unlock()

			var owedCons []string
			if tc.consForOwed {
				owedCons = append(owedCons, failing)
			}
			owedSub := x.seedJoin(t, owed, 21, "n-owed", owedCons...)
			overlapSub := x.seedJoin(t, overlap, 22, "n-overlap")

			// Captured AT THE SAVE, not after the call: post-call state cannot tell staging
			// BEFORE the save from staging after it, and the crash property is the ordering.
			var closedAtSave, voidedAtSave bool
			restore := setMQTTTestSessSaveHook(func(sp *mqttSession) error {
				if sp != x.e.sess {
					return nil
				}
				sp.mu.Lock()
				_, voidedAtSave = sp.cancelledRegNonces["n-owed"]
				sp.mu.Unlock()
				sp.subsMu.Lock()
				closedAtSave = owedSub.mqtt.closed
				sp.subsMu.Unlock()
				return nil
			})
			t.Cleanup(restore)

			mqttTestSessStoreSuppressV.Store(true)
			t.Cleanup(func() { mqttTestSessStoreSuppressV.Store(false) })
			if _, err := x.testRollbackSharedFilter(t, failing, false, 1); err == nil {
				t.Fatal("the suppressed rollback save reported success")
			}

			if !closedAtSave {
				t.Fatal("the abandoned join's deliver subscription was NOT closed by the time the " +
					"removal save was issued, so the register snapshot beside it is only a moment in time")
			}
			if !voidedAtSave {
				t.Fatal("the abandoned join's register was NOT voided by the time the removal save " +
					"was issued")
			}
			if tc.alsoHold == _EMPTY_ {
				return
			}
			// The overlapping durable belongs to a membership that SURVIVES this packet, so
			// neither its delivery nor its registrations may be touched.
			x.e.sess.subsMu.Lock()
			overlapClosed := overlapSub.mqtt.closed
			x.e.sess.subsMu.Unlock()
			if overlapClosed {
				t.Fatal("the surviving membership's delivery was closed by the failing filter's rollback")
			}
			x.e.sess.mu.Lock()
			_, overlapVoided := x.e.sess.cancelledRegNonces["n-overlap"]
			x.e.sess.mu.Unlock()
			if overlapVoided {
				t.Fatal("the surviving membership's registrations were voided by the failing filter's rollback")
			}
		})
	}
}

// TestSharedFailedFilterRollbackRestoresADefiniteFailure is T8.
//
// A DEFINITE failure means the record still holds the membership, so everything the staging
// lowered has to go back: the same subscription reopened and the void verdicts withdrawn. The
// barrier is now keyed by deliver subject while the list that reopens it holds pointers, so
// the two could drift apart without any other test noticing — no existing rollback fixture
// seeds a matching delivery subscription at all.
func TestSharedFailedFilterRollbackRestoresADefiniteFailure(t *testing.T) {
	const key = "$share/g/foo"
	x := testNewSharedReleaseEnv(t, "rollbackdefinite", key)
	target := mqttSharedGroupNamesForKey(key)[0]

	x.e.sess.mu.Lock()
	if x.e.sess.sharedSubs == nil {
		x.e.sess.sharedSubs = make(map[string]byte)
	}
	x.e.sess.sharedSubs[key] = 1
	x.e.sess.mu.Unlock()

	sub := x.seedJoin(t, target, 31, "n-definite")

	// A CAS rejection with the record genuinely untouched: the DEFINITE arm, and the only one
	// that must restore.
	stop := testForceContendedRejection(t, x.e.sess)
	if _, err := x.testRollbackSharedFilter(t, key, false, 1); err == nil {
		t.Fatal("the rejected rollback save reported success")
	}
	stop()

	if x.e.sess.isSeqUncertain() {
		t.Fatal("a CAS rejection is DEFINITE: it must not arm uncertainty, or this test is " +
			"exercising the ambiguous arm instead")
	}
	x.e.sess.subsMu.Lock()
	closed := sub.mqtt.closed
	x.e.sess.subsMu.Unlock()
	if closed {
		t.Fatal("the delivery barrier was not reopened after a DEFINITE rollback failure, so the " +
			"membership the record still holds has stopped delivering")
	}
	x.e.sess.mu.Lock()
	_, stillVoided := x.e.sess.cancelledRegNonces["n-definite"]
	_, membershipBack := x.e.sess.sharedSubs[key]
	x.e.sess.mu.Unlock()
	if stillVoided {
		t.Fatal("the void verdict was not withdrawn after a DEFINITE rollback failure")
	}
	if !membershipBack {
		t.Fatal("the membership was not restored after a DEFINITE rollback failure")
	}
}

// TestSharedGroupTargetReleaseIsIdempotentOverLocalState is T9.
//
// The extraction makes the release reachable for a target no config ever existed for, so it
// must be safe with nothing seeded and safe called twice. LOCAL STATE only: it may legitimately
// repeat the last-holder scan and re-issue an idempotent CONSUMER.DELETE, so "no external
// calls" is not the claim.
func TestSharedGroupTargetReleaseIsIdempotentOverLocalState(t *testing.T) {
	const key = "$share/g/foo"
	x := testNewSharedReleaseEnv(t, "releaseidempotent", key)
	target := mqttSharedGroupNamesForKey(key)[0]

	x.e.sess.mu.Lock()
	before := x.e.sess.tmaxack
	x.e.sess.mu.Unlock()

	held := map[string]bool{target.durable: true}
	for i := range 2 {
		tok := x.e.sess.beginRecordTx()
		x.e.sess.releaseSharedGroupTarget(x.c, tok, target.durable, target.deliver, held)
		if err := x.e.sess.endRecordTx(tok); err != nil {
			t.Fatalf("closing the record transaction on call %d: %v", i+1, err)
		}
		x.e.sess.mu.Lock()
		after, charged, pending := x.e.sess.tmaxack, len(x.e.sess.sharedCharged), len(x.e.sess.cpending)
		x.e.sess.mu.Unlock()
		if after != before {
			t.Fatalf("call %d moved tmaxack from %d to %d for a durable this session never charged",
				i+1, before, after)
		}
		if charged != 0 || pending != 0 {
			t.Fatalf("call %d invented state: sharedCharged=%d cpending=%d", i+1, charged, pending)
		}
	}
	// Held throughout, so the release may never have deleted it.
	if !x.consumerExists(t, target.durable) {
		t.Fatal("the group consumer was deleted although the scan answer said held")
	}
}

// TestSharedFailedFilterReleaseConsumesTheScanPerDurable is T10, and it closes the hole the
// second holistic round found: nothing else here proves this caller reads the last-holder
// answer PER DURABLE.
//
// T1-T3 release a single target, so any answer is trivially "the" answer, and T4's mixed set
// pairs own-held with unheld — but own-held targets are removed BEFORE the scan runs, so they
// never appear in its map. Reusing one target's `held` value for every owed target therefore
// passed every other test in this file and the route test as well.
//
// The consequence of that defect is not symmetric, which is why this arm asserts both
// directions. Applying the unheld answer to the foreign-held target DELETES a group consumer
// another session still holds, destroying the interest-retained backlog of every member of
// that group. Applying the foreign-held answer to the unheld one merely retains a consumer
// nothing will reclaim.
func TestSharedFailedFilterReleaseConsumesTheScanPerDurable(t *testing.T) {
	const failing = "$share/g/foo.>"
	const foreignHolds = "$share/g/foo"

	x := testNewSharedReleaseEnv(t, "perdurable", failing)
	targets := mqttSharedGroupNamesForKey(failing)
	if len(targets) != 2 {
		t.Fatalf("the multi-level key joins %d group consumers, want 2", len(targets))
	}
	unheld, foreign := targets[0], targets[1]

	// A DIFFERENT session holds only the level-up filter, so the scan's answer is selective:
	// false for this key's own durable, true for the one it shares with that member.
	testPlantSessionRecordJSON(t, x.e.asm, "othermember", &mqttPersistedSession{
		ID:         "othermember",
		SharedSubs: map[string]byte{foreignHolds: 1},
	})
	// This session holds NO shared membership: both targets are owed, so both reach the scan
	// and the disposition can only come from its per-durable answer.
	x.e.sess.mu.Lock()
	baseline := x.e.sess.tmaxack
	x.e.sess.mu.Unlock()

	unheldSub := x.seedJoin(t, unheld, 41, "n-unheld")
	foreignSub := x.seedJoin(t, foreign, 42, "n-foreign")

	tok := x.e.sess.beginRecordTx()
	x.e.sess.releaseFailedSharedFilter(x.c, tok, &mqttFilter{shared: true, group: "g", filter: "foo.>"})
	if err := x.e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	// Both targets are owed, so BOTH discharge their local state whatever the scan said —
	// that is D1, and it is what distinguishes this arm from a held-gated loop body.
	x.assertDischarged(t, "T10 unheld target", unheld, 41, baseline, unheldSub)
	x.assertDischarged(t, "T10 foreign-held target", foreign, 42, baseline, foreignSub)

	// And only the unheld one is destroyed.
	x.assertConsumer(t, unheld.durable, true)
	x.assertConsumer(t, foreign.durable, false)
}

// TestSharedFailedFilterInlineCleanupKeepsAnOverlappingOwnMembership is T11, found by the
// third holistic round: the CONFIRMED inline half of the failed-filter release had the same
// overlap defect as the landed continuation, and nothing covered it.
//
// The route is real. A session holding "$share/g/foo" SUBSCRIBEs "$share/g/foo.>", whose
// level-up half has its own sid but the SAME group durable and deliver subject as the
// surviving filter. That half takes the create path (its own sid has no cons entry), so a
// failure in its delivery installation returns the config — and with the rollback save
// CONFIRMED, cleanupFailedSubClassified releases by that config. Whole-durable: the surviving
// filter's delivery would be closed and unsubscribed, its pending identifiers purged and its
// ack-budget charge refunded, and the last-holder scan stops none of it, because the scan
// withholds only the consumer DELETE.
//
// T4 covers the continuation and the route test has no overlapping membership, so before this
// arm the inline path was uncovered in both directions.
func TestSharedFailedFilterInlineCleanupKeepsAnOverlappingOwnMembership(t *testing.T) {
	const failing = "$share/g/foo.>"
	const surviving = "$share/g/foo"

	x := testNewSharedReleaseEnv(t, "inlineoverlap", failing)
	// The level-up target: the durable the two keys SHARE.
	overlap := mqttSharedGroupNamesForKey(failing)[1]
	if overlap.durable != mqttSharedGroupNamesForKey(surviving)[0].durable {
		t.Fatalf("the fixture's premise is wrong: %q does not share a durable with %q", failing, surviving)
	}

	// Only the surviving membership is held: the failing filter's own was rolled back before
	// this cleanup runs, which is what "confirmed" means on this path.
	x.e.sess.mu.Lock()
	if x.e.sess.sharedSubs == nil {
		x.e.sess.sharedSubs = make(map[string]byte)
	}
	x.e.sess.sharedSubs[surviving] = 1
	x.e.sess.mu.Unlock()

	sub := x.seedJoin(t, overlap, 51, "n-inline", surviving)

	// Exactly what processSharedJSConsumer hands back when its delivery installation fails
	// after the create: the level-up config.
	cc := &ConsumerConfig{Durable: overlap.durable, DeliverSubject: overlap.deliver, DeliverGroup: "g"}
	tok := x.e.sess.beginRecordTx()
	_ = x.e.sess.cleanupFailedSubClassified(x.c, tok, true, nil, cc, nil)
	if err := x.e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	x.assertUntouched(t, "T11 inline overlap", overlap, 51, sub)
	x.assertConsumer(t, overlap.durable, false)
}
