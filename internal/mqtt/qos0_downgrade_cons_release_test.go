// Copyright 2025 The mqtt-nats-broker Authors
// SPDX-License-Identifier: Apache-2.0

//go:build mqttport_fulltests

package mqtt

import (
	"fmt"
	"testing"
	"time"
)

// The tests here cover one invariant: a QoS 1/2 -> QoS 0 re-SUBSCRIBE whose consumer is
// DESTROYED rather than converted must not leave the record naming that consumer. The
// release happens at the packet's final-update boundary, in the same sess.mu section that
// applies the membership, because the membership and the consumer reference are one fact.
// See releaseDowngradedConsumersLocked.
//
// Every record assertion here uses the TWO-VALUE form on Subs. A missing key also reads as
// zero, and a missing key is a REMOVAL — a different fact, a different code path, and a
// state a broker that dropped the filter outright would produce while satisfying a
// one-value assertion.

// testRecordConsAbsent asserts the record carries the membership PRESENT at QoS 0 and names
// no consumer for sid, which is this item's whole acceptance shape. It also asserts no drain
// is named: omitting the consumer while ALSO failing to name a drain is the defect, and
// asserting only the first two would pass against a build that wrongly drained.
func testRecordConsAbsent(t *testing.T, ps *mqttPersistedSession, key, sid string) {
	t.Helper()
	qos, ok := ps.Subs[key]
	if !ok || qos != 0 {
		t.Fatalf("membership %q must be PRESENT at QoS 0 in the record, got %v (present %v)", key, qos, ok)
	}
	if cc := ps.Cons[sid]; cc != nil {
		t.Fatalf("the record must not name sid %q's consumer after a DESTROYED downgrade, got %+v", sid, cc)
	}
	if d := ps.Drains[sid]; d != nil {
		t.Fatalf("nothing was started, so sid %q must not be named as a drain either, got %+v", sid, d)
	}
}

// TestMQTTQoS0DowngradeReleasesTheConsumerReference is T1: the ordinary single-occurrence
// downgrade, with nothing started, is the population the roadmap bullet named and the one
// that reaches the destructive arm through stageDrain's empty-cpending refusal.
func TestMQTTQoS0DowngradeReleasesTheConsumerReference(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "consrel", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	sess := testMQTTResidentSession(t, s, "consrel")
	dur, sid := testDowngradeDurable(t, sess, "foo")

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, c, nil, r)

	asm := testMQTTGetAccountSessionManager(t, s, "consrel")
	ps, _ := testReconcileRecord(t, asm, "consrel")
	testRecordConsAbsent(t, ps, "foo", sid)

	// The durable really is destroyed, so the omission is truthful rather than premature.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if testSubRevokeConsumerLives(t, sess, dur) {
			return fmt.Errorf("durable %q still exists", dur)
		}
		return nil
	})
	sess.mu.Lock()
	live := sess.cons[sid]
	sess.mu.Unlock()
	if live != nil {
		t.Fatalf("a downgrade that STANDS must leave no cons entry, got %+v", live)
	}
}

// TestMQTTQoS0DowngradeReleasesTheLevelUpCompanion is T3: a multi-level filter owns TWO
// sids, and the companion's key is its PARENT's. A release keyed on the raw sid finds no
// membership for "<X> fwc", skips it, and strands half a wildcard in the record — which is
// why this asserts both sids rather than the count.
func TestMQTTQoS0DowngradeReleasesTheLevelUpCompanion(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "consrelfwc", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})

	sess := testMQTTResidentSession(t, s, "consrelfwc")
	// The membership key for "foo/#" is the NATS-converted "foo.>"; the level-up companion
	// is filed under "foo fwc" and belongs to that same key.
	conv, _ := mqttToNATSSubjectConversion([]byte("foo/#"), true)
	mainSid := string(conv)
	fwcSid := mainSid[:len(mainSid)-2] + mqttMultiLevelSidSuffix
	sess.mu.Lock()
	mainCC, fwcCC := sess.cons[mainSid], sess.cons[fwcSid]
	sess.mu.Unlock()
	if mainCC == nil || fwcCC == nil {
		t.Fatalf("the fixture never reached the route it names: a QoS 1 multi-level filter "+
			"must own BOTH sids (main=%v companion=%v)", mainCC != nil, fwcCC != nil)
	}

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, c, nil, r)

	asm := testMQTTGetAccountSessionManager(t, s, "consrelfwc")
	ps, _ := testReconcileRecord(t, asm, "consrelfwc")
	testRecordConsAbsent(t, ps, mainSid, mainSid)
	if cc := ps.Cons[fwcSid]; cc != nil {
		t.Fatalf("the LEVEL-UP COMPANION's consumer must be released too: its membership key "+
			"is the parent filter's, so a release keyed on the raw sid strands it, got %+v", cc)
	}
}

// TestMQTTQoS0DowngradePersistsAPositiveQoSConsumer is T11, the positive control. A release
// that fired on a POSITIVE membership, or a record builder that stopped persisting plain
// consumers, would satisfy every assertion above.
//
// The oracle is the persisted DURABLE IDENTITY, not that Cons is non-empty: an expected
// value that is merely "something is there" passes against a broker writing the wrong
// consumer.
func TestMQTTQoS0DowngradePersistsAPositiveQoSConsumer(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "conskeep", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{
		{filter: "plain", qos: 2},
		{filter: "wild/#", qos: 2},
	}, []byte{2, 2})
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, "conskeep")
	wconv, _ := mqttToNATSSubjectConversion([]byte("wild/#"), true)
	wildSid := string(wconv)
	fwcSid := wildSid[:len(wildSid)-2] + mqttMultiLevelSidSuffix
	sess.mu.Lock()
	want := map[string]string{}
	for _, sid := range []string{"plain", wildSid, fwcSid} {
		cc := sess.cons[sid]
		if cc == nil {
			sess.mu.Unlock()
			t.Fatalf("the fixture never reached the route it names: no consumer for sid %q", sid)
		}
		want[sid] = cc.Durable
	}
	sess.mu.Unlock()

	asm := testMQTTGetAccountSessionManager(t, s, "conskeep")
	ps, _ := testReconcileRecord(t, asm, "conskeep")
	for sid, dur := range want {
		cc := ps.Cons[sid]
		if cc == nil {
			t.Fatalf("a POSITIVE-QoS membership must keep its consumer in the record; sid %q is missing", sid)
		}
		if cc.Durable != dur {
			t.Fatalf("sid %q must persist its own durable %q, got %q", sid, dur, cc.Durable)
		}
	}
}

// TestMQTTQoS0DowngradeForcesASaveForTheReleaseAlone is T9, and it is the only test that
// discriminates the inLock hook's return value.
//
// updateWithDetailConverting saves only when needUpdate is set, and a membership change
// usually sets it. Not always: a QoS 0 re-SUBSCRIBE over a filter ALREADY at QoS 0 that
// still holds a cons entry changes no membership, no option and no subscription identifier,
// so the hook's own return is the only thing left. Without it the entry leaves memory while
// the record keeps naming it — the defect, inverted.
//
// The preconditions are held explicitly rather than hoped for: an EXISTING QoS 0 membership,
// a retained config, unchanged options, and nothing else in the packet that would make the
// conversion report work of its own.
func TestMQTTQoS0DowngradeForcesASaveForTheReleaseAlone(t *testing.T) {
	t.Parallel()
	s, c, sess, _ := drainTestSession(t, "qos0-release-forces-save")
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}

	cc := &ConsumerConfig{
		Durable:        sess.idHash + "_forcedsave",
		DeliverSubject: mqttSubPrefix + "forcedsave",
		AckPolicy:      AckExplicit,
		FilterSubject:  mqttStreamSubjectPrefix + "foo",
	}
	sess.mu.Lock()
	sess.subs = map[string]byte{"foo": 0}
	sess.cons = map[string]*ConsumerConfig{"foo": cc}
	sess.mu.Unlock()

	// The pre-state must be durable, so the assertion below is about THIS write and not
	// about a record that never named the consumer in the first place.
	tok := sess.beginRecordTx()
	if err := sess.save(tok); err != nil {
		t.Fatalf("seeding the record: %v", err)
	}
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx(seed): %v", err)
	}
	seeded, _ := testReconcileRecord(t, asm, "qos0-release-forces-save")
	if seeded.Cons["foo"] == nil {
		t.Fatal("the fixture never reached the route it names: a cons entry beside a QoS 0 " +
			"membership is exactly the state this test starts from, and the builder must " +
			"still persist it (that is what protects a live ADOPTED durable)")
	}

	dels := []*mqttDeferredDel{{sid: "foo", cc: cc}}
	tok = sess.beginRecordTx()
	// The production closure, in the shape processSubs uses it: the conversion reports
	// nothing here (no candidate is eligible), so the release is the only work.
	var conv mqttDowngradeConversion
	inLock := func() bool {
		conv = sess.convertDowngradeCandidatesLocked(c, dels)
		released := sess.releaseDowngradedConsumersLocked(dels)
		return conv.any() || released
	}
	if _, err := sess.updateWithDetailConverting(tok, []*mqttFilter{{filter: "foo", qos: 0}}, true, inLock); err != nil {
		t.Fatalf("updateWithDetailConverting: %v", err)
	}
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
	if conv.any() {
		t.Fatal("the fixture is not isolating the release: the conversion reported work, so " +
			"needUpdate would have been set whatever the release returned")
	}
	if !dels[0].consReleased {
		t.Fatal("the release did not fire, so this test says nothing about the save it forces")
	}

	ps, _ := testReconcileRecord(t, asm, "qos0-release-forces-save")
	testRecordConsAbsent(t, ps, "foo", "foo")
}

// TestMQTTQoS0DowngradeReleaseKeepsAReplacedConfig is T1's identity sub-arm, and the pair
// for the mutation that relaxes the release's identity check to a mere presence test.
//
// The check is DEFENSIVE: no production route is demonstrated that replaces a staged
// config in place, because a revocation itself requires the same identity. It is exercised
// at the helper because evicting a consumer another occurrence installed would destroy a
// live subscription's delivery, and a defensive guard with no test is a guard that quietly
// stops working.
func TestMQTTQoS0DowngradeReleaseKeepsAReplacedConfig(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "qos0-release-identity")

	staged := &ConsumerConfig{Durable: sess.idHash + "_staged"}
	installed := &ConsumerConfig{Durable: sess.idHash + "_installed"}
	sess.mu.Lock()
	sess.subs = map[string]byte{"foo": 0}
	sess.cons = map[string]*ConsumerConfig{"foo": installed}
	dels := []*mqttDeferredDel{{sid: "foo", cc: staged}}
	released := sess.releaseDowngradedConsumersLocked(dels)
	got := sess.cons["foo"]
	sess.mu.Unlock()

	if released {
		t.Fatal("the release must not report work for a sid whose entry is no longer the one staged")
	}
	if got != installed {
		t.Fatalf("the release evicted a config it never staged: want %p, got %p", installed, got)
	}
	if dels[0].consReleased {
		t.Fatal("a descriptor whose entry was replaced must not be marked released, or the " +
			"rollback would reinstate a config this packet does not own")
	}
}

// TestMQTTQoS0DowngradeReleaseSkipsAnAbsentMembership is the other half of the same
// predicate: a MISSING key is a removal, which is UNSUBSCRIBE's path, and the builder's own
// presence test already omits its consumer. Releasing there would take the entry out from
// under a teardown that has not been staged by this boundary.
func TestMQTTQoS0DowngradeReleaseSkipsAnAbsentMembership(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "qos0-release-absent")

	cc := &ConsumerConfig{Durable: sess.idHash + "_absent"}
	sess.mu.Lock()
	sess.subs = map[string]byte{}
	sess.cons = map[string]*ConsumerConfig{"foo": cc}
	dels := []*mqttDeferredDel{{sid: "foo", cc: cc}}
	released := sess.releaseDowngradedConsumersLocked(dels)
	_, still := sess.cons["foo"]
	sess.mu.Unlock()

	if released || !still {
		t.Fatalf("an ABSENT membership is a removal, not a downgrade: the release must skip it "+
			"(released=%v, entry still present=%v)", released, still)
	}
}

// TestMQTTQoS0DowngradeReleaseSkipsAConvertedDescriptor pins the exclusion that keeps this
// mechanism and the drain conversion apart. A converted descriptor's durable is a drain,
// named in the record's Drains, and stageDrain already took its cons entry; reporting work
// for it would be harmless here but would mark it released, and the rollback would then
// reinstate a cons entry for a durable that is a drain — the shape I7 forbids.
func TestMQTTQoS0DowngradeReleaseSkipsAConvertedDescriptor(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "qos0-release-converted")

	cc := &ConsumerConfig{Durable: sess.idHash + "_converted"}
	sess.mu.Lock()
	sess.subs = map[string]byte{"foo": 0}
	// The conversion has already moved it: cons no longer holds it, which is the state the
	// release must recognise without needing the flag. The flag is asserted separately.
	sess.cons = map[string]*ConsumerConfig{}
	dels := []*mqttDeferredDel{
		{sid: "foo", cc: cc, converted: true},
		{sid: "bar", cc: cc, revoked: true},
	}
	released := sess.releaseDowngradedConsumersLocked(dels)
	sess.mu.Unlock()

	if released {
		t.Fatal("neither a CONVERTED nor a REVOKED descriptor is this boundary's to release")
	}
	if dels[0].consReleased || dels[1].consReleased {
		t.Fatalf("no exclusion may mark a descriptor released (converted=%v revoked=%v)",
			dels[0].consReleased, dels[1].consReleased)
	}
}

// TestMQTTQoS0DowngradeRestoreOnlyFillsAnAbsentSid is the un-remove's guard.
//
// Tracking that we removed an entry does not prove nothing has been installed at that sid
// since, and overwriting a config another path installed would repoint delivery at a
// durable this packet is about to delete. The restore therefore fills only an ABSENT sid,
// and it clears the flag either way so a second pass cannot resurrect anything.
func TestMQTTQoS0DowngradeRestoreOnlyFillsAnAbsentSid(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "qos0-release-restore")

	released := &ConsumerConfig{Durable: sess.idHash + "_released"}
	newer := &ConsumerConfig{Durable: sess.idHash + "_newer"}
	sess.mu.Lock()
	sess.cons = map[string]*ConsumerConfig{"taken": newer}
	dels := []*mqttDeferredDel{
		{sid: "free", cc: released, consReleased: true},
		{sid: "taken", cc: released, consReleased: true},
	}
	sess.restoreDowngradedConsumersLocked(dels)
	free, taken := sess.cons["free"], sess.cons["taken"]
	sess.mu.Unlock()

	if free != released {
		t.Fatalf("an absent sid must get its released config back, got %+v", free)
	}
	if taken != newer {
		t.Fatalf("an occupied sid must keep what is there: want %p, got %p", newer, taken)
	}
	if dels[0].consReleased || dels[1].consReleased {
		t.Fatal("the restore must clear the flag on every descriptor it considered, or a " +
			"later pass reinstates an entry the packet has since given up")
	}
}

// TestMQTTAdoptedDrainIsNamedInAMidPacketRecord is T4, and it is the reason the record
// BUILDER was left alone.
//
// The tempting fix for this item was a positive-QoS predicate in
// buildPersistedSessionLocked. It is wrong, and this is the counterexample. A filter sitting
// at QoS 0 with a drain that still owes a started QoS 1 exchange -- an ordinary d554f13
// result -- is upgraded back to QoS 1 in a packet that also carries a shared filter.
// Adoption moves the durable out of BOTH drain maps and into sess.cons at once, while
// sess.subs still says QoS 0 until the final update; the shared filter's early persist then
// writes a record in between. Under a positive-QoS predicate that record names the durable
// NOWHERE -- not in Cons, not in Drains -- and an orphan sweep on any broker may delete it
// with the client's unacknowledged exchange inside it.
//
// So the oracle is the mid-packet record naming the adopted durable BY IDENTITY. The
// exchange completing afterwards is asserted too, but it is not the discriminator: it
// completes whether or not that record was truthful, as long as nothing swept in between.
//
// MQTT 5 throughout, because a shared subscription is what forces the mid-packet write and
// 3.1.1 does not carry one.
func TestMQTTAdoptedDrainIsNamedInAMidPacketRecord(t *testing.T) {
	// NOT parallel: it installs process-global record hooks.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "adoptmid/topic"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "adoptmid", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK: %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, "adoptmid")
	dur, sid := testDowngradeDurable(t, sess, topic)

	// One started, unacknowledged QoS 1 delivery: this is what the drain preserves and
	// what an early sweep would destroy.
	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "adoptmidpub", cleanStart: true},
		o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTReadConnAckV5(t, pr)
	testMQTTPublishV5(t, pc, 1, 1, topic, []byte("owed"))
	if reason, _ := testMQTTReadPubAckV5(t, pr, 1); reason != mqttReasonSuccess {
		t.Fatalf("PUBACK to the publisher: 0x%x", reason)
	}
	_, _, _, pi, ok := testMQTTTryReadPublishV5(r, 3*time.Second)
	if !ok {
		t.Fatal("fixture: the subscriber never received the QoS 1 delivery, so no started exchange exists")
	}

	// The downgrade converts, which is d554f13's guarantee and this fixture's premise.
	if codes := testMQTTSubV5(t, c, r, 2, []mqttV5SubFilter{{topic: topic, opts: 0}}); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK for the downgrade: %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess.mu.Lock()
	drained := sess.drains[sid] != nil
	membership, present := sess.subs[sid]
	sess.mu.Unlock()
	if !drained || !present || membership != 0 {
		t.Fatalf("the fixture never reached the route it names: the downgrade must leave a "+
			"DRAIN under a QoS 0 membership (drain=%v membership=%v present=%v)",
			drained, membership, present)
	}

	w := &testSubRevokeWrittenRecords{}
	restore := w.armWrittenRecords(sess)
	defer restore()

	// The upgrade adopts the drain; the shared filter that follows it in the SAME packet
	// forces an early membership persist, which is the record this test is about.
	codes := testMQTTSubV5(t, c, r, 3, []mqttV5SubFilter{
		{topic: topic, opts: 1},
		{topic: "$share/adoptmidg/other", opts: 1},
	})
	if len(codes) != 2 || codes[0] != 1 || codes[1] != 1 {
		t.Fatalf("SUBACK for the adopting packet: %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	records, cerr := w.landed()
	if cerr != nil {
		t.Fatalf("record capture: %v", cerr)
	}
	if len(records) == 0 {
		t.Fatal("the fixture never reached the route it names: no record was written during " +
			"the adopting packet, so there is no mid-packet write to judge")
	}
	// Every record written BEFORE the final update must name the adopted durable, so this
	// checks all of them rather than only the first: a broker that got the first right and
	// a later one wrong would leave the same window open.
	var sawMidPacket bool
	for i, ps := range records {
		qos, present := ps.Subs[sid]
		if !present || qos != 0 {
			// The final update or later: the membership has moved to QoS 1 and the consumer
			// is named by the ordinary rule.
			continue
		}
		sawMidPacket = true
		cc := ps.Cons[sid]
		if cc == nil {
			t.Fatalf("record %d carries the PRE-PACKET QoS 0 membership and must still name "+
				"the adopted durable %q in Cons: adoption emptied Drains, so omitting it here "+
				"leaves the durable named NOWHERE and an orphan sweep on any broker may "+
				"delete it with the client's unacknowledged exchange inside it", i, dur)
		}
		if cc.Durable != dur {
			t.Fatalf("record %d names durable %q for sid %q, want the adopted %q", i, cc.Durable, sid, dur)
		}
		if d := ps.Drains[sid]; d != nil {
			t.Fatalf("record %d names sid %q as BOTH a consumer and a drain, which no writer "+
				"may produce: fork repair drops the live config on exactly that basis", i, sid)
		}
	}
	if !sawMidPacket {
		t.Fatalf("no captured record carried the pre-packet QoS 0 membership for sid %q, so the "+
			"mid-packet window this test exists for was never observed (%d records captured)",
			sid, len(records))
	}

	// The exchange the drain preserved still completes, under its original identifier.
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	testMQTTFlush(t, c, nil, r)
}

// TestMQTTQoS0DowngradeReleaseOmitsTheDurableFromBothTeardownDerivations is T6.
//
// clear() derives the durables it deletes from sess.cons (mqttPreparedClear.destroy, sess_clear.go), while a FOREIGN
// broker adopting the teardown marker derives them from the marker's record
// (mqttTeardownTargetFromRecord, sess_teardown.go). The release takes the entry out of the
// map that feeds the first and out of the record that feeds the second, so in the window
// between the confirmed save and the teardown NEITHER names it and the durable is the
// orphan sweep's.
//
// That divergence from HEAD is intended, and this test pins it so a later reader cannot
// "repair" the marker back into naming a durable whose teardown is already owed. Both
// derivations are asserted, because the change moves the entry out of one map that two
// different consumers read.
func TestMQTTQoS0DowngradeReleaseOmitsTheDurableFromBothTeardownDerivations(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "qos0-release-teardown")

	cc := &ConsumerConfig{
		Durable:        sess.idHash + "_teardownderive",
		DeliverSubject: mqttSubPrefix + "teardownderive",
	}
	keep := &ConsumerConfig{
		Durable:        sess.idHash + "_keepderive",
		DeliverSubject: mqttSubPrefix + "keepderive",
	}
	sess.mu.Lock()
	// "released" is at QoS 0 with its entry already gone (the boundary ran); "keep" is an
	// ordinary positive-QoS subscription and is the positive control — a derivation that
	// simply stopped naming anything would satisfy the first assertion alone.
	sess.subs = map[string]byte{"released": 0, "keep": 1}
	sess.cons = map[string]*ConsumerConfig{"keep": keep}
	ps := sess.buildPersistedSessionLocked()
	var localDurs []string
	for _, c := range sess.cons {
		localDurs = append(localDurs, c.Durable)
	}
	sess.mu.Unlock()

	// Derivation 1: clear()'s own list, which reads sess.cons.
	for _, d := range localDurs {
		if d == cc.Durable {
			t.Fatalf("clear() would delete %q: the released entry must be gone from sess.cons", d)
		}
	}
	if len(localDurs) != 1 || localDurs[0] != keep.Durable {
		t.Fatalf("clear() must still delete a POSITIVE-QoS subscription's durable, got %v", localDurs)
	}

	// Derivation 2: the teardown marker a foreign broker adopts.
	target := mqttTeardownTargetFromRecord(&ps, sess.idHash)
	for _, d := range target.durs {
		if d == cc.Durable {
			t.Fatalf("the teardown marker names %q: the record must not offer a durable whose "+
				"teardown this packet already owes", d)
		}
	}
	var sawKeep bool
	for _, d := range target.durs {
		if d == keep.Durable {
			sawKeep = true
		}
	}
	if !sawKeep {
		t.Fatalf("the marker must still name a POSITIVE-QoS subscription's durable %q, got %v",
			keep.Durable, target.durs)
	}
}
