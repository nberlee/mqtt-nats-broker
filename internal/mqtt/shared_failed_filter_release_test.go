package mqtt

import (
	"testing"
	"time"
)

// F-082, and the shared-group lifecycle transfer. A live shared SUBSCRIBE whose filter failed
// owes TWO different releases, decided by two different instruments.
//
// Whether the GROUP consumer goes is the group record's arbitrated holder count, moved by the
// release of each ACQUISITION the filter held: the compensating record write carries a release
// verdict per acquisition, and executing it is one ledger+group CAS that removes exactly that
// entry. That is other members' business and is decided by the records, never by this session.
// Everything else the release does — the resident cons entry, the deliver-subject
// subscription, the pending deliveries and their reserved identifiers, and the ack-budget
// charge — is this session's own state, owed whatever the records say, with ONE exception that
// must never license a release: THIS session still reaches the durable through another of its
// own keys, which is the overlapping level-up case.
//
// The fixtures here acquire through the production path (J0/J1), so the ledger lists what the
// release must settle and the group record counts it; a membership fabricated in memory would
// make every release settle as a no-op and prove nothing. They then seed the state a failing
// packet really leaves. That matters most for the shape with NO cons entry, which is the COMMON
// one: addJSConsToSess runs only once the whole filter has succeeded, so the join a failing
// filter made is invisible to a cons walk while its charge, its pending state and its
// subscription are all real.

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
			Config: buildSharedGroupConsumerConfig(group, s, 1, mqttDefaultAckWait, 1024, false),
		}
		if _, err := e.asm.jsa.createDurableConsumer(ccr); err != nil {
			t.Fatalf("creating the group consumer for %q: %v", s, err)
		}
	}
	// The deciding connection is a client of its OWN, accepted as a CONNECT would be (F-105 7b):
	// never the manager's request-loop client, whose fields that loop reads unlocked.
	c := e.s.createInternalAccountClient()
	testAcceptRigConnection(t, e.asm, e.sess, c)
	return &testSharedReleaseEnv{e: e, c: c, key: key}
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

// acquire is J0 and J1 for key through the production acquisition path, followed by the
// membership save (J2): the ledger lists the acquisitions and the group record counts them, so
// a release of this membership has something to settle. The fixture's consumer for incarnation
// 1 is what a first mount names, and the fixture asserts that agreement rather than assuming it.
func (x *testSharedReleaseEnv) acquire(t *testing.T, key string) mqttSharedMembership {
	t.Helper()
	return testAcquireShared(t, x.e.sess, x.c, key)
}

// testAcquireShared acquires key for sess as its own SUBSCRIBE would, and installs the
// membership in memory and in the record.
func testAcquireShared(t *testing.T, sess *mqttSession, c *client, key string) mqttSharedMembership {
	t.Helper()
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		t.Fatalf("the fixture's own key %q is not a shared session key", key)
	}
	sess.mu.Lock()
	unminted := sess.sharedTenure.isZero()
	seq := sess.seq
	sess.mu.Unlock()
	if unminted {
		if seq == 0 {
			t.Fatalf("the session %q has no record sequence, so no tenure can be minted: persist it first", sess.id)
		}
		testInstallFixtureTenure(t, sess)
	}
	f := &mqttFilter{shared: true, group: group, filter: subject, qos: 1}
	m, err := sess.acquireSharedFilter(c, f, subject)
	if err != nil {
		t.Fatalf("acquiring %q for %q: %v", key, sess.id, err)
	}
	sess.mu.Lock()
	if sess.sharedSubs == nil {
		sess.sharedSubs = make(map[string]mqttSharedMembership)
	}
	sess.sharedSubs[key] = *m
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the membership of %q: %v", key, err)
	}
	// The fixture reached the records, or it reached nothing.
	epoch := sess.sharedEpoch()
	for _, a := range m.acquisitions() {
		if a.Inc != 1 {
			t.Fatalf("the acquisition of %q mounted incarnation %d; this fixture's consumers are incarnation 1", key, a.Inc)
		}
	}
	l := sess.jsa.loadSharedLedger(sess.id, epoch, sess.gen)
	if l.status != mqttOutboundValid {
		t.Fatalf("the ledger of %q reads %v after an acquisition", sess.id, l.status)
	}
	for _, a := range m.acquisitions() {
		if _, listed := l.rec.lists(a.Acq); !listed {
			t.Fatalf("the ledger of %q does not list the acquisition %q it just made", sess.id, a.Acq)
		}
	}
	return *m
}

// leave removes the memberships of keys and writes their release verdicts, which is what the
// record write of an UNSUBSCRIBE, a forced leave or a failed filter's compensation does
// (sess.update's removal arm). It returns the removed memberships, which is the state the
// release under test runs against.
func (x *testSharedReleaseEnv) leave(t *testing.T, keys ...string) map[string]mqttSharedMembership {
	t.Helper()
	sess := x.e.sess
	removed := make(map[string]mqttSharedMembership, len(keys))
	sess.mu.Lock()
	for _, key := range keys {
		m, ok := sess.sharedSubs[key]
		if !ok {
			sess.mu.Unlock()
			t.Fatalf("the session holds no membership for %q to leave", key)
		}
		delete(sess.sharedSubs, key)
		sess.addAcqReleaseVerdictsLocked(key, m)
		removed[key] = m
	}
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the removal of %v: %v", keys, err)
	}
	return removed
}

// foreignHold makes a SECOND session, id, acquire key through the production path, so the
// group's count is 2 and a release by this session leaves the consumer standing for the
// other member.
func (x *testSharedReleaseEnv) foreignHold(t *testing.T, id, key string) mqttSharedMembership {
	t.Helper()
	other, _, err := x.e.asm.createOrRestoreSession(id, x.e.s.getOpts())
	if err != nil {
		t.Fatalf("creating the second member %q: %v", id, err)
	}
	if err := testSessSave(t, other); err != nil {
		t.Fatalf("persisting the second member %q: %v", id, err)
	}
	return testAcquireShared(t, other, x.c, key)
}

// withholdGroupRecord makes key's own group record read INDETERMINATE, which is the one
// condition under which a release cannot be executed: nothing about the group can be
// concluded, so the verdict stands for a later executor and the consumer is left alone.
func (x *testSharedReleaseEnv) withholdGroupRecord(t *testing.T, key string) {
	t.Helper()
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		t.Fatalf("the fixture's own key %q is not a shared session key", key)
	}
	qkey := mqttSharedGroupKey(group, subject)
	h := func(q string) (mqttSharedGroupLoad, bool) {
		if q != qkey {
			return mqttSharedGroupLoad{}, false
		}
		return mqttSharedGroupLoad{status: mqttOutboundIndeterminate}, true
	}
	mqttTestSharedGroupLoadHookV.Store(&h)
	t.Cleanup(func() { mqttTestSharedGroupLoadHookV.Store(nil) })
}

// groupOf reads key's OWN group record through the production decoder.
func (x *testSharedReleaseEnv) groupOf(t *testing.T, key string) mqttSharedGroupLoad {
	t.Helper()
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		t.Fatalf("the fixture's own key %q is not a shared session key", key)
	}
	return x.e.asm.jsa.loadSharedGroup(x.e.sess.sharedEpoch(), mqttSharedGroupKey(group, subject))
}

// assertSettled is the RECORD-side oracle of a release: the verdicts for m are gone from the
// session, the ledger no longer lists its acquisitions, and key's own group counts
// wantHolders — zero meaning the incarnation was retired and its retirement finished (the
// consumer's deletion classified and the retiring entry cleared).
func (x *testSharedReleaseEnv) assertSettled(t *testing.T, what, key string, m mqttSharedMembership, wantHolders uint64) {
	t.Helper()
	sess := x.e.sess
	sess.mu.Lock()
	for _, a := range m.acquisitions() {
		if _, pending := sess.acqReleases[a.Acq]; pending {
			sess.mu.Unlock()
			t.Fatalf("%s: the release verdict for %q is still pending after the release settled", what, a.Acq)
		}
	}
	sess.mu.Unlock()
	l := sess.jsa.loadSharedLedger(sess.id, sess.sharedEpoch(), sess.gen)
	if l.status != mqttOutboundValid {
		t.Fatalf("%s: the ledger reads %v", what, l.status)
	}
	for _, a := range m.acquisitions() {
		if _, listed := l.rec.lists(a.Acq); listed {
			t.Fatalf("%s: the ledger still lists the released acquisition %q", what, a.Acq)
		}
	}
	g := x.groupOf(t, key)
	if g.status != mqttOutboundValid {
		t.Fatalf("%s: the group record reads %v (%v)", what, g.status, g.err)
	}
	switch {
	case wantHolders == 0 && (g.rec.Live != nil || len(g.rec.Retiring) != 0):
		t.Fatalf("%s: the group should be retired and finished, reads live=%+v retiring=%v", what, g.rec.Live, g.rec.Retiring)
	case wantHolders > 0 && (g.rec.Live == nil || g.rec.Live.Holders != wantHolders):
		t.Fatalf("%s: the group should count %d holder(s), reads live=%+v", what, wantHolders, g.rec.Live)
	}
}

// assertVerdictPending is assertSettled's opposite: the release could not be executed, so
// the verdict stays for a later executor, the ledger still lists the acquisition and the
// group still counts it.
func (x *testSharedReleaseEnv) assertVerdictPending(t *testing.T, what string, m mqttSharedMembership) {
	t.Helper()
	sess := x.e.sess
	sess.mu.Lock()
	for _, a := range m.acquisitions() {
		if _, pending := sess.acqReleases[a.Acq]; !pending {
			sess.mu.Unlock()
			t.Fatalf("%s: the release verdict for %q was dropped although its execution was withheld", what, a.Acq)
		}
	}
	sess.mu.Unlock()
	l := sess.jsa.loadSharedLedger(sess.id, sess.sharedEpoch(), sess.gen)
	if l.status != mqttOutboundValid {
		t.Fatalf("%s: the ledger reads %v", what, l.status)
	}
	for _, a := range m.acquisitions() {
		if _, listed := l.rec.lists(a.Acq); !listed {
			t.Fatalf("%s: the ledger no longer lists %q although its release was withheld", what, a.Acq)
		}
	}
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
	// PROVEN UNSENT, which is the population whose identifier the release is entitled to
	// free and whose nonce the void verdict may name (F-101): the durable pair still holds
	// its unsent marker, so nothing was ever put on the wire under this identifier. The
	// marker sequence is what says so, exactly as a confirmed register store leaves it
	// (storeInflightRegister's step-3 capture).
	//
	// It used to be built as {regNonce, regSeq: 0} under a comment calling it "transmitted",
	// which set neither transmitted nor a marker sequence — a shape no production path
	// produces, and one F-101's gate now reads as POSSIBLY ATTEMPTED and reserves. The
	// assertions below are about the release ladder discharging its own state, so the
	// fixture is corrected to carry the evidence that ladder requires rather than the
	// assertions being weakened to match a fixture that no longer reaches it. A transmitted
	// entry's opposite disposition is asserted separately, below.
	sess.pendingPublish[pi] = &mqttPending{regNonce: nonce, markerSeq: 4242}
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
				t.Fatalf("the group consumer %q survived although its last acquisition was released", durable)
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
			t.Fatalf("the group consumer %q was DELETED although the group still counts a holder, or nothing licensed the delete", durable)
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
// T3 (this session is the only holder) is a permanent CONTROL, not decoration: it is the only
// arm that proves the group consumer is still deleted when the records say it may be. Without
// it every assertion in T1 and T2 would pass against a release that deletes nothing, ever.
func TestSharedFailedFilterReleaseDischargesOwnStateUnderARefusal(t *testing.T) {
	const clientID = "failedfilter"
	const key = "$share/g/foo"
	const nonce = "n-release"

	for _, tc := range []struct {
		name string
		// hold arranges the other side of the release: a second holder, or a group record
		// that cannot be read. nil leaves this session the only holder.
		hold func(t *testing.T, x *testSharedReleaseEnv)
		// consSids is empty for the no-cons shape a failed filter really leaves.
		consSids []string
		// wantHolders is the group's count after the release; 0 means retired.
		wantHolders uint64
		wantDeleted bool
		// withheld: the release could not be executed, so its verdict stands.
		withheld bool
	}{
		{
			// T1. A second member holds the group, so the count goes 2 -> 1 and the consumer
			// stays while this session's own state goes. TWO stale cons entries, because a
			// durable can be named by more than one and a first-match sweep would leave the
			// second stranded with no membership to reach it by.
			name: "T1 a second holder keeps the consumer, with two stale cons entries",
			hold: func(t *testing.T, x *testSharedReleaseEnv) {
				x.foreignHold(t, "othermember", key)
			},
			consSids:    []string{key, key + "-stale"},
			wantHolders: 1,
			wantDeleted: false,
		},
		{
			// T2. The COMMON shape: no cons entry at all, and the group record cannot be read,
			// so the release cannot be executed. Both halves matter — a cons-keyed release
			// discharges nothing here, and one that withheld everything on an unreadable record
			// would strand this session's own state as well. The verdict stays for the next
			// resume or the verdict pass, which is the stated residual.
			name: "T2 an unreadable group record withholds the execution, and there is no cons entry",
			hold: func(t *testing.T, x *testSharedReleaseEnv) {
				x.withholdGroupRecord(t, key)
			},
			wantHolders: 1,
			wantDeleted: false,
			withheld:    true,
		},
		{
			name:        "T3 control, the only holder leaves and the group retires",
			wantHolders: 0,
			wantDeleted: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := testNewSharedReleaseEnv(t, clientID, key)
			m := x.acquire(t, key)
			target := mqttSharedGroupNamesFor(key, m)[0]
			if !x.consumerExists(t, target.durable) {
				t.Fatalf("the fixture did not create the group consumer %q", target.durable)
			}

			x.e.sess.mu.Lock()
			baseline := x.e.sess.tmaxack
			x.e.sess.mu.Unlock()

			sub := x.seedJoin(t, target, 5, nonce, tc.consSids...)
			if tc.hold != nil {
				tc.hold(t, x)
			}
			// The compensating write: the membership is removed and the verdict is durable.
			x.leave(t, key)

			tok := x.e.sess.beginRecordTx()
			x.e.sess.releaseFailedSharedFilter(x.c, tok, &mqttFilter{shared: true, group: "g", filter: "foo", sharedM: &m})
			if err := x.e.sess.endRecordTx(tok); err != nil {
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

// The SAME release, over a POSSIBLY-ATTEMPTED delivery, must not free its identifier — the
// F-101 property asserted at this site rather than only at the purge that implements it,
// because a release edge is where the claim is consumed and a claim proved at one surface
// says nothing about another (feedback_claim_fanout_across_tiers).
//
// Everything else the release owes still happens: the charge is refunded, the cons entries
// are swept, the delivery is closed and the mapping is purged. Only the identifier and its
// capacity charge stay, because a teardown's own ladder never establishes that the CLIENT
// completed.
//
// Mutation check: remove the purge's provenUnsentReleasableLocked gate and the reservation
// and no-verdict assertions here fail while the discharge assertions still pass.
func TestSharedFailedFilterReleaseReservesAPossiblyAttemptedDelivery(t *testing.T) {
	const clientID = "failedfilter-reserved"
	const key = "$share/g/foo"
	const nonce = "n-possibly-attempted"

	x := testNewSharedReleaseEnv(t, clientID, key)
	m := x.acquire(t, key)
	target := mqttSharedGroupNamesFor(key, m)[0]

	x.e.sess.mu.Lock()
	baseline := x.e.sess.tmaxack
	x.e.sess.mu.Unlock()

	sub := x.seedJoin(t, target, 5, nonce)
	// POSSIBLY ATTEMPTED, and stated positively: the marker is gone and the entry is
	// transmitted, which is exactly what endPublishSend leaves after a confirmed intent.
	x.e.sess.mu.Lock()
	ack := x.e.sess.pendingPublish[5]
	ack.markerSeq, ack.transmitted = 0, true
	x.e.sess.mu.Unlock()
	x.leave(t, key)

	tok := x.e.sess.beginRecordTx()
	x.e.sess.releaseFailedSharedFilter(x.c, tok, &mqttFilter{shared: true, group: "g", filter: "foo", sharedM: &m})
	if err := x.e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	sess := x.e.sess
	sess.mu.Lock()
	_, stillReserved := sess.pendingPublish[5]
	_, verdicted := sess.voidedRegNonces[nonce]
	_, charged := sess.sharedCharged[target.durable]
	_, mapped := sess.cpending[target.durable]
	after := sess.tmaxack
	sess.mu.Unlock()

	if !stillReserved {
		t.Fatal("a possibly-attempted delivery's identifier must survive the shared release: the client may hold it")
	}
	if verdicted {
		t.Fatalf("no never-transmitted verdict may name %q: recovery would read it as licence to free the identifier", nonce)
	}
	// The rest of the release is unchanged, so the reservation is not smuggling in a
	// wholesale refusal to discharge.
	if charged || after != baseline {
		t.Fatalf("the ack-budget charge must still be refunded, charged=%v tmaxack=%d want %d", charged, after, baseline)
	}
	if mapped {
		t.Fatal("the source mapping must still go with the consumer being released")
	}
	if sub.mqtt != nil && !sub.mqtt.closed {
		t.Fatal("the deliver subscription must still be closed")
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
	// The surviving membership first: it holds the parent group alone (count 1). The failing
	// key then acquires on its own group AND on the parent (count 2) — two acquisitions, both
	// owed a release, while only ONE durable is owed a local release.
	survM := x.acquire(t, surviving)
	failM := x.acquire(t, failing)
	targets := mqttSharedGroupNamesFor(failing, failM)
	if len(targets) != 2 {
		t.Fatalf("the multi-level key joins %d group consumers, want 2: %+v", len(targets), targets)
	}
	owed, ownHeld := targets[0], targets[1]
	if ownHeld.durable != mqttSharedGroupNamesFor(surviving, survM)[0].durable {
		t.Fatalf("the fixture's premise is wrong: %q does not share a durable with %q", failing, surviving)
	}
	if g := x.groupOf(t, surviving); g.status != mqttOutboundValid || g.rec.Live == nil || g.rec.Live.Holders != 2 {
		t.Fatalf("the parent group should count both acquisitions before the release, reads %+v", g.rec)
	}

	x.e.sess.mu.Lock()
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

	// The failing key is NOT in sharedSubs: its removal has already been proven durable,
	// which is what running this continuation means, and the verdicts ride that write.
	x.leave(t, failing)

	tok := x.e.sess.beginRecordTx()
	x.e.sess.releaseFailedSharedFilter(x.c, tok, &mqttFilter{shared: true, group: "g", filter: "foo.>", sharedM: &failM})
	if err := x.e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	x.assertDischarged(t, "T4 owed half", owed, 11, baselineWithKept, owedSub)
	x.assertUntouched(t, "T4 preserved half", ownHeld, 12, keptSub)

	// BOTH acquisitions are released: the failing key's own group retires...
	x.assertSettled(t, "T4 own group", failing, mqttSharedMembership{QoS: 1, Main: failM.Main}, 0)
	x.assertConsumer(t, owed.durable, true)
	// ...and the parent's count goes back to the surviving membership's one, its consumer
	// standing — held by the RECORD's count, and locally by the surviving key.
	x.assertSettled(t, "T4 parent group", surviving, mqttSharedMembership{QoS: 1, Main: *failM.Up}, 1)
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
	// The ATTEMPTED membership is the one the live map names, exactly as processSubs leaves
	// it on the filter before the rollback runs.
	x.e.sess.mu.Lock()
	cur := x.e.sess.sharedSubs[key]
	x.e.sess.mu.Unlock()
	f := &mqttFilter{shared: true, group: group, filter: subject, qos: 1, sharedM: &cur}
	var priorM mqttSharedMembership
	if priorExisted {
		priorM = cur
		priorM.QoS = priorQoS
	}
	tok := x.e.sess.beginRecordTx()
	run, _, err := x.e.sess.rollbackSharedSub(x.c, tok, f, true, priorExisted, priorM, false, 0)
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
				x.e.sess.sharedSubs = make(map[string]mqttSharedMembership)
			}
			x.e.sess.sharedSubs[key] = testSharedMembership(key, 1)
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
			targets := mqttSharedGroupNamesFor(failing, testSharedMembership(failing, 1))
			if len(targets) != 2 {
				t.Fatalf("the multi-level key joins %d group consumers, want 2", len(targets))
			}
			owed, overlap := targets[0], targets[1]

			x.e.sess.mu.Lock()
			if x.e.sess.sharedSubs == nil {
				x.e.sess.sharedSubs = make(map[string]mqttSharedMembership)
			}
			x.e.sess.sharedSubs[failing] = testSharedMembership(failing, 1)
			if tc.alsoHold != _EMPTY_ {
				x.e.sess.sharedSubs[tc.alsoHold] = testSharedMembership(tc.alsoHold, 1)
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
				_, voidedAtSave = sp.voidedRegNonces["n-owed"]
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
			_, overlapVoided := x.e.sess.voidedRegNonces["n-overlap"]
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
	target := mqttSharedGroupNamesFor(key, testSharedMembership(key, 1))[0]

	x.e.sess.mu.Lock()
	if x.e.sess.sharedSubs == nil {
		x.e.sess.sharedSubs = make(map[string]mqttSharedMembership)
	}
	x.e.sess.sharedSubs[key] = testSharedMembership(key, 1)
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
	_, stillVoided := x.e.sess.voidedRegNonces["n-definite"]
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
// re-read the records and re-execute an already-settled verdict, so "no external calls" is not
// the claim.
func TestSharedGroupTargetReleaseIsIdempotentOverLocalState(t *testing.T) {
	const key = "$share/g/foo"
	x := testNewSharedReleaseEnv(t, "releaseidempotent", key)
	target := mqttSharedGroupNamesFor(key, testSharedMembership(key, 1))[0]

	x.e.sess.mu.Lock()
	before := x.e.sess.tmaxack
	x.e.sess.mu.Unlock()

	for i := range 2 {
		tok := x.e.sess.beginRecordTx()
		x.e.sess.releaseSharedGroupTarget(x.c, tok, target.durable, target.deliver)
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
	// No verdict names it, so nothing licensed a delete.
	if !x.consumerExists(t, target.durable) {
		t.Fatal("the group consumer was deleted although no release verdict named it")
	}
}

// TestSharedFailedFilterReleaseSettlesEachAcquisitionSeparately is T10, and it closes the
// hole the second holistic round found in the scan-era release: nothing else here proves this
// caller settles its targets PER ACQUISITION.
//
// T1-T3 release a single target, so any disposition is trivially "the" disposition, and T4's
// mixed set pairs own-held with sole-held — but own-held targets never reach the local release
// at all. A release that applied one target's outcome to every owed target passed every other
// test in this file and the route test as well.
//
// The consequence of that defect is not symmetric, which is why this arm asserts both
// directions. Retiring the group another session still holds DELETES a group consumer that
// member depends on, destroying the interest-retained backlog of every member of that group.
// Leaving the sole-held one merely retains a consumer nothing will reclaim.
func TestSharedFailedFilterReleaseSettlesEachAcquisitionSeparately(t *testing.T) {
	const failing = "$share/g/foo.>"
	const foreignHolds = "$share/g/foo"

	x := testNewSharedReleaseEnv(t, "perdurable", failing)
	// A DIFFERENT session holds only the level-up filter, so the two acquisitions the failing
	// key makes settle differently: its own group retires, the parent's count goes 2 -> 1.
	x.foreignHold(t, "othermember", foreignHolds)
	failM := x.acquire(t, failing)
	targets := mqttSharedGroupNamesFor(failing, failM)
	if len(targets) != 2 {
		t.Fatalf("the multi-level key joins %d group consumers, want 2", len(targets))
	}
	soleHeld, foreign := targets[0], targets[1]

	// This session holds NO other shared membership: both targets are owed a local release,
	// and the consumers' fates can only come from each acquisition's own settlement.
	x.e.sess.mu.Lock()
	baseline := x.e.sess.tmaxack
	x.e.sess.mu.Unlock()

	soleSub := x.seedJoin(t, soleHeld, 41, "n-sole")
	foreignSub := x.seedJoin(t, foreign, 42, "n-foreign")
	x.leave(t, failing)

	tok := x.e.sess.beginRecordTx()
	x.e.sess.releaseFailedSharedFilter(x.c, tok, &mqttFilter{shared: true, group: "g", filter: "foo.>", sharedM: &failM})
	if err := x.e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("closing the record transaction: %v", err)
	}

	// Both targets are owed, so BOTH discharge their local state whatever the records say —
	// that is D1, and it is what distinguishes this arm from a count-gated loop body.
	x.assertDischarged(t, "T10 sole-held target", soleHeld, 41, baseline, soleSub)
	x.assertDischarged(t, "T10 foreign-held target", foreign, 42, baseline, foreignSub)

	// And only the sole-held one is destroyed: its group retires, the parent still counts
	// the other member.
	x.assertSettled(t, "T10 own group", failing, mqttSharedMembership{QoS: 1, Main: failM.Main}, 0)
	x.assertSettled(t, "T10 parent group", foreignHolds, mqttSharedMembership{QoS: 1, Main: *failM.Up}, 1)
	x.assertConsumer(t, soleHeld.durable, true)
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
// ack-budget charge refunded, and the group record stops none of it, because the record decides
// only the consumer's fate.
//
// T4 covers the continuation and the route test has no overlapping membership, so before this
// arm the inline path was uncovered in both directions.
func TestSharedFailedFilterInlineCleanupKeepsAnOverlappingOwnMembership(t *testing.T) {
	const failing = "$share/g/foo.>"
	const surviving = "$share/g/foo"

	x := testNewSharedReleaseEnv(t, "inlineoverlap", failing)
	// Only the surviving membership is held: the failing filter's own was rolled back before
	// this cleanup runs, which is what "confirmed" means on this path.
	survM := x.acquire(t, surviving)
	// The level-up target: the durable the two keys SHARE.
	overlap := mqttSharedGroupNamesFor(failing, testSharedMembershipAt(failing, 1, 1))[1]
	if overlap.durable != mqttSharedGroupNamesFor(surviving, survM)[0].durable {
		t.Fatalf("the fixture's premise is wrong: %q does not share a durable with %q", failing, surviving)
	}

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
