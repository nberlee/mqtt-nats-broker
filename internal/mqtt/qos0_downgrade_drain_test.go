// Copyright 2025 The mqtt-nats-bridge Authors
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"sync/atomic"
	"testing"
)

// TestConversionTransfersTheDurablesLiveWithheldMarkers is the marker handoff, and it is
// the defect two plan-review rounds found in the designs that preceded it.
//
// stageDrain OWNS sess.deferred for its durable: it deletes every entry (I5 — a withheld
// delivery was never transmitted, so it is not drained) and re-installs only the
// cancellations it discovered itself in cpending. A withheld marker with NO registration —
// a delivery withheld at the in-flight cap — is therefore invisible to it and would be
// destroyed. That marker is what makes a later FIRST transmission carry DUP=0
// ([MQTT-3.3.1-1]), since trackPublish otherwise derives dup from JetStream's delivery
// count.
//
// HEAD's deferred QoS downgrade performs no I5 drop at all, so losing it would be a
// REGRESSION introduced by the conversion rather than an inherited drain behavior. Hence
// the transfer, and hence this test.
//
// The oracle is deliberately the marker's PRESENCE plus its ownership by the drain — not
// "the conversion succeeded", which a broker that dropped the marker would also satisfy.
func TestConversionTransfersTheDurablesLiveWithheldMarkers(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-marker-transfer")

	const dur, sid = "dur-qos0-marker", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)

	capWithheld := mqttDeferredKey{jsDur: dur, sseq: 77}
	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = cfg
	sess.subs[sid] = 0 // the membership this packet's update just applied
	// One STARTED delivery, so stageDrain has something to drain.
	sess.cpending = map[string]map[uint64]uint16{dur: {41: 1}}
	sess.pendingPublish = map[uint16]*mqttPending{1: {jsDur: dur, sseq: 41, jsAckSubject: "ack.41", transmitted: true}}
	// And one delivery withheld at the in-flight CAP: a marker with no registration.
	sess.deferred = map[mqttDeferredKey]string{capWithheld: "ack.77"}
	sess.mu.Unlock()

	dd := &mqttDeferredDel{sid: sid, cc: cfg, convert: true}

	sess.mu.Lock()
	conv := sess.convertDowngradeCandidatesLocked(c, []*mqttDeferredDel{dd})
	sess.mu.Unlock()

	if len(conv.drains) != 1 || !dd.converted {
		t.Fatalf("the candidate must convert: it holds a started delivery (drains=%d converted=%v)",
			len(conv.drains), dd.converted)
	}
	d := conv.drains[0].d

	sess.mu.Lock()
	mark, present := sess.deferred[capWithheld]
	_, owned := d.cancelled[capWithheld]
	sess.mu.Unlock()

	if !present {
		t.Fatal("the conversion must preserve a withheld marker that had no registration: " +
			"stageDrain's I5 drop removes it, HEAD's downgrade teardown never did, and " +
			"without it the delivery's eventual FIRST transmission carries DUP=1")
	}
	if mark != "ack.77" {
		t.Fatalf("the marker must keep its ack subject, got %q", mark)
	}
	if !owned {
		t.Fatal("the drain must OWN the transferred marker: d.cancelled is what commitDrains " +
			"retires, so a marker outside it is never dropped when the staging settles")
	}
}

// TestConversionRestoresMarkersEvenWhenStageDrainRefuses is the helper contract for
// stageDrain's third refusal site.
//
// stageDrain returns false in three places and one of them — len(d.seqs) == 0 — is AFTER
// the I5 drop has already run. A transfer that re-installed only on success would lose the
// markers there. There is no production producer for that site (a delivery callback holds
// sess.mu from its closed check through trackPublish, so no fresh allocation lands after
// staging), which is why this fixture SYNTHESIZES the state and asserts a helper contract
// rather than a client-visible defect.
func TestConversionRestoresMarkersEvenWhenStageDrainRefuses(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-marker-refused")

	const dur, sid = "dur-qos0-refused", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)

	withheld := mqttDeferredKey{jsDur: dur, sseq: 88}
	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = cfg
	sess.subs[sid] = 0
	// Every pending entry is regPending, so stageDrain cancels them all and reaches its
	// post-drop refusal with an empty started set.
	sess.cpending = map[string]map[uint64]uint16{dur: {51: 2}}
	sess.pendingPublish = map[uint16]*mqttPending{
		2: {jsDur: dur, sseq: 51, jsAckSubject: "ack.51", regPending: true, regNonce: "nonce-51"},
	}
	sess.deferred = map[mqttDeferredKey]string{withheld: "ack.88"}
	sess.mu.Unlock()

	dd := &mqttDeferredDel{sid: sid, cc: cfg, convert: true}
	sess.mu.Lock()
	conv := sess.convertDowngradeCandidatesLocked(c, []*mqttDeferredDel{dd})
	mark, present := sess.deferred[withheld]
	sess.mu.Unlock()

	if len(conv.drains) != 0 || dd.converted {
		t.Fatalf("nothing was provably started, so no drain may be staged (drains=%d converted=%v)",
			len(conv.drains), dd.converted)
	}
	if !present || mark != "ack.88" {
		t.Fatalf("the markers must survive a refusal taken AFTER the I5 drop, got %q present=%v",
			mark, present)
	}
}

// TestConversionRefusesAnIdentityMismatch is T18: the boundary re-validates that the entry
// is still the one the packet staged.
//
// Defensive rather than reachable — a revoked occurrence clears convert — but a wrongful
// conversion moves a LIVE subscription's consumer into a drain, which stops it delivering.
// The fixture gives the old consumer outstanding work, so removing the check would
// actually convert something.
func TestConversionRefusesAnIdentityMismatch(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-identity")

	const dur, sid = "dur-qos0-identity", "foo"
	staged := realDrainConsumer(t, sess, dur, sid)
	// A DIFFERENT consumer now occupies the sid.
	other := realDrainConsumer(t, sess, dur+"-other", sid)

	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = other
	sess.subs[sid] = 0
	sess.cpending = map[string]map[uint64]uint16{dur: {61: 3}}
	sess.pendingPublish = map[uint16]*mqttPending{3: {jsDur: dur, sseq: 61, jsAckSubject: "ack.61"}}
	sess.mu.Unlock()

	dd := &mqttDeferredDel{sid: sid, cc: staged, convert: true}
	sess.mu.Lock()
	conv := sess.convertDowngradeCandidatesLocked(c, []*mqttDeferredDel{dd})
	stillOther := sess.cons[sid] == other
	sess.mu.Unlock()

	if len(conv.drains) != 0 || dd.converted {
		t.Fatalf("a candidate whose cons entry no longer names it must not convert (drains=%d converted=%v)",
			len(conv.drains), dd.converted)
	}
	if !stillOther {
		t.Fatal("the occupying consumer must be left exactly where it was")
	}
}

// TestConversionRefusesWhenTheMembershipIsNotZero is the other half of the boundary's
// re-validation, and it is why the read uses the TWO-VALUE form: a MISSING key also reads
// as zero, and a missing key is a removal rather than a downgrade.
func TestConversionRefusesWhenTheMembershipIsNotZero(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-membership")

	const dur, sid = "dur-qos0-membership", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)

	for _, tc := range []struct {
		name  string
		apply func()
	}{
		{"membership absent", func() { delete(sess.subs, sid) }},
		{"membership still positive", func() { sess.subs[sid] = 1 }},
	} {
		sess.mu.Lock()
		if sess.subs == nil {
			sess.subs = make(map[string]byte)
		}
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[sid] = cfg
		sess.subs[sid] = 0
		tc.apply()
		sess.cpending = map[string]map[uint64]uint16{dur: {71: 4}}
		sess.pendingPublish = map[uint16]*mqttPending{4: {jsDur: dur, sseq: 71, jsAckSubject: "ack.71"}}
		sess.mu.Unlock()

		dd := &mqttDeferredDel{sid: sid, cc: cfg, convert: true}
		sess.mu.Lock()
		conv := sess.convertDowngradeCandidatesLocked(c, []*mqttDeferredDel{dd})
		sess.mu.Unlock()
		if len(conv.drains) != 0 || dd.converted {
			t.Fatalf("%s: the conversion must refuse (drains=%d converted=%v)",
				tc.name, len(conv.drains), dd.converted)
		}
	}
}

// TestUnconvertRestoresAConvertedConsumerBeforeRollbackEnumerates is the ordering
// guarantee behind unconvertDowngradeDrainsLocked, and it is deliberately SYNTHETIC.
//
// rollbackSubscribePacket discovers the consumers a packet created by walking sess.cons,
// so a converted one has left that map and would be classified as nothing at all:
// neither restored nor released. The un-convert runs first so the existing
// classification can see it.
//
// This is the UNIT half of that guarantee, and it complements rather than replaces
// TestMQTTQoS0DowngradeRollbackReleasesAPacketCreatedDurable, which drives the same
// ordering through a real client and a real rollback. Keeping both is deliberate: this one
// states the map contract in isolation, so a future reader can see what the ordering is
// FOR without reconstructing a four-way schedule.
//
// A note worth keeping, because it cost a review round: this test was briefly the ONLY
// coverage, on the mistaken ground that a consumer created by the packet could never hold
// a transmitted delivery. The subscription barrier does not give that — wait blocks only
// for SUBSCRIBEs already in flight when it sampled, so a publisher that sampled earlier
// can land its store mid-packet. The end-to-end test exists because the population is
// real.
func TestUnconvertRestoresAConvertedConsumerBeforeRollbackEnumerates(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-unconvert")

	const dur, sid = "dur-qos0-unconvert", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)

	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = cfg
	sess.subs[sid] = 0
	sess.cpending = map[string]map[uint64]uint16{dur: {31: 9}}
	sess.pendingPublish = map[uint16]*mqttPending{9: {jsDur: dur, sseq: 31, jsAckSubject: "ack.31", transmitted: true}}
	sess.mu.Unlock()

	dd := &mqttDeferredDel{sid: sid, cc: cfg, convert: true}
	sess.mu.Lock()
	conv := sess.convertDowngradeCandidatesLocked(c, []*mqttDeferredDel{dd})
	// The state the rollback would meet: the drain owns the sid and cons no longer does.
	_, consAfterConvert := sess.cons[sid]
	sess.mu.Unlock()
	if len(conv.drains) != 1 {
		t.Fatalf("the fixture must convert, or there is no ordering to test (drains=%d)", len(conv.drains))
	}
	if consAfterConvert {
		t.Fatal("after conversion the sid must be the drain's, not sess.cons' (I7)")
	}

	sess.mu.Lock()
	sess.unconvertDowngradeDrainsLocked(conv)
	restored, inCons := sess.cons[sid]
	_, stillDrained := sess.drains[sid]
	_, stillIndexed := sess.drainByDur[dur]
	sess.mu.Unlock()

	if !inCons || restored != cfg {
		t.Fatal("the un-convert must put the consumer back in sess.cons under its own sid: " +
			"rollbackSubscribePacket enumerates that map, and a consumer missing from it is " +
			"neither restored nor released")
	}
	if stillDrained || stillIndexed {
		t.Fatalf("the un-convert must drop the drain from BOTH maps (drains=%v drainByDur=%v)",
			stillDrained, stillIndexed)
	}
}

// installClosedDelivery gives a drain the state the conversion leaves behind: a delivery
// subscription that EXISTS and is CLOSED. The reopen is what makes it deliver again, and
// nothing else repairs it — reclaimDrainDeliveryLocked returns immediately when a
// subscription is present, closed or not.
func installClosedDelivery(t *testing.T, c *client, sess *mqttSession, cc *ConsumerConfig, qos byte) *subscription {
	t.Helper()
	sess.mu.Lock()
	sub, err := sess.processQOS12Sub(c, []byte(cc.DeliverSubject), []byte(cc.DeliverSubject), nil,
		false, qos, false, false, false, 0, cc.Durable, mqttDeliverMsgCbQoS12)
	if err == nil && sub != nil && sub.mqtt != nil {
		sess.subsMu.Lock()
		sub.mqtt.closed = true
		sess.subsMu.Unlock()
	}
	sess.mu.Unlock()
	if err != nil {
		t.Fatalf("installing the delivery subscription: %v", err)
	}
	return sub
}

// TestCompletionReopensTheDeliveryItStaged is the other half of the conversion: a
// committed drain whose subscription stays closed can never retransmit, because the
// QoS 1/2 callback tests sub.mqtt.closed BEFORE it looks for a drain.
func TestCompletionReopensTheDeliveryItStaged(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-reopen")

	const dur, sid = "dur-qos0-reopen", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	sub := installClosedDelivery(t, c, sess, cfg, 1)

	d := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{41: {claimed: true}})
	sess.mu.Lock()
	d.cc, d.staged = cfg, true
	sess.mu.Unlock()

	sess.finishDowngradeDrains(c, mqttDowngradeConversion{drains: []mqttConvertedDrain{{d: d, sub: sub}}})

	sess.mu.Lock()
	sess.subsMu.Lock()
	closed, staged := sub.mqtt.closed, d.staged
	sess.subsMu.Unlock()
	sess.mu.Unlock()
	if closed {
		t.Fatal("the completion must reopen the delivery the staging closed: the callback " +
			"tests sub.mqtt.closed before it looks for a drain, so a closed one never retransmits")
	}
	if staged {
		t.Fatal("the completion must COMMIT the drain, or a fresh delivery is left to AckWait forever")
	}
}

// TestCompletionReopensNothingForARetiredDrain and its siblings are the reopen's
// validation. Each disposition is asserted on its own, because an earlier revision of the
// plan demanded they all end in the same repairable state — and they do not: retirement
// has already delete-requested the durable, so there is nothing to reopen and no later
// reclaim.
func TestCompletionReopensNothingForARetiredDrain(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-reopen-retired")

	const dur, sid = "dur-qos0-retired", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	sub := installClosedDelivery(t, c, sess, cfg, 1)

	d := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{42: {claimed: true}})
	sess.mu.Lock()
	d.cc, d.retired = cfg, true
	sess.mu.Unlock()

	sess.finishDowngradeDrains(c, mqttDowngradeConversion{drains: []mqttConvertedDrain{{d: d, sub: sub}}})

	sess.mu.Lock()
	sess.subsMu.Lock()
	closed := sub.mqtt.closed
	sess.subsMu.Unlock()
	sess.mu.Unlock()
	if !closed {
		t.Fatal("a RETIRED drain must be left alone: its durable has been delete-requested, " +
			"so reopening delivery for it would route to a consumer that is going away")
	}
}

// TestCompletionReopensNothingForAForeignDrainObject is the stale-pointer guard: the
// object this conversion staged is no longer the one the maps hold.
func TestCompletionReopensNothingForAForeignDrainObject(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-reopen-foreign")

	const dur, sid = "dur-qos0-foreign", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	sub := installClosedDelivery(t, c, sess, cfg, 1)

	// The maps hold a DIFFERENT drain object for this durable.
	resident := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{43: {claimed: true}})
	sess.mu.Lock()
	resident.cc = cfg
	sess.mu.Unlock()
	stale := &mqttDrain{cc: cfg, staged: true}

	sess.finishDowngradeDrains(c, mqttDowngradeConversion{drains: []mqttConvertedDrain{{d: stale, sub: sub}}})

	sess.mu.Lock()
	sess.subsMu.Lock()
	closed := sub.mqtt.closed
	sess.subsMu.Unlock()
	sess.mu.Unlock()
	if !closed {
		t.Fatal("the reopen must act only on the object this conversion staged and still " +
			"filed in both maps; reopening on a stale pointer is a write against state " +
			"another owner is responsible for")
	}
}

// TestCompletionReopensNothingForADetachedClient is the DISCONNECTED disposition, and it
// is a normal one rather than an anomaly.
//
// An ambiguous write can stay unresolved while its connection closes — the shim clears
// c.subs — and the resident reconciliation pass then resolves it and runs this
// continuation against a client whose subscriptions are gone. Nothing may touch that
// object; the surviving drain takes its source interest from the ordinary resume.
func TestCompletionReopensNothingForADetachedClient(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-reopen-detached")

	const dur, sid = "dur-qos0-detached", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	sub := installClosedDelivery(t, c, sess, cfg, 1)

	d := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{44: {claimed: true}})
	sess.mu.Lock()
	d.cc, d.staged = cfg, true
	sess.mu.Unlock()

	// The connection closed: its subscription map is gone.
	c.mu.Lock()
	c.subs = nil
	c.mu.Unlock()

	// Must not panic, must still COMMIT — the record is durable, so the drain is
	// committed whether or not this connection can serve it.
	sess.finishDowngradeDrains(c, mqttDowngradeConversion{drains: []mqttConvertedDrain{{d: d, sub: sub}}})

	sess.mu.Lock()
	staged := d.staged
	sess.mu.Unlock()
	if staged {
		t.Fatal("a detached client must still let the drain COMMIT: the bytes are durable, " +
			"and leaving it staged means a fresh delivery is never ack-discarded")
	}
}

// TestConversionRefusesWhenAHeldDrainOwnsTheSid is T13, and it keeps the two reasons
// stageDrain refuses APART: "nothing started" is the ordinary drain-versus-delete choice,
// while a HELD drain on the same sid is an ownership refusal that must not be overridden.
//
// The held drain keeps the slot and this consumer takes the ordinary destructive
// teardown — the same disposition draining-not-allowed uses. Overwriting the held drain
// would erase a past-PUBREC intent by routine client behaviour (F-094).
//
// A unit fixture, because the state has no client-reachable producer: it needs a
// quarantined drain under a sid that also carries a live consumer. The consumer is given
// STARTED work, not merely a cancelled registration, or the refusal would never be
// reached at all.
func TestConversionRefusesWhenAHeldDrainOwnsTheSid(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-held")

	const dur, sid = "dur-qos0-held", "foo"
	// The held drain, on a durable of its own.
	held := addTestDrain(sess, sid, dur+"-held", sid, 2, map[uint64]*drainSeq{
		41: {claimed: true, intent: true, pi: 7, ownerNonce: "xxxxxxxxxxxxxxxxxxxxxx"},
	})
	// And a live consumer under the same sid, with started work of its own.
	cfg := realDrainConsumer(t, sess, dur, sid)
	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = cfg
	sess.subs[sid] = 0
	sess.cpending = map[string]map[uint64]uint16{dur: {51: 5}}
	// regNonce AND markerSeq are both required, not decoration. voidRegistrationsLocked
	// records a verdict only for an entry that HAS a nonce, and since F-101 only for one
	// that is PROVEN UNSENT — its durable pair still holding the unsent marker, which is
	// what markerSeq says. Without either, the refusal's void would be vacuously empty and
	// the assertion below would pass against a broker that voided nothing.
	sess.pendingPublish = map[uint16]*mqttPending{
		5: {
			jsDur: dur, sseq: 51, jsAckSubject: "ack.51",
			regNonce: "yyyyyyyyyyyyyyyyyyyyyy", markerSeq: 4242,
		},
	}
	hold := sess.drainSidHoldLocked(sid)
	sess.mu.Unlock()
	if hold != mqttDrainHeldPastPubRecIntent {
		t.Fatalf("the fixture must be HELD, or there is no refusal to prove, got %v", hold)
	}

	dd := &mqttDeferredDel{sid: sid, cc: cfg, convert: true}
	sess.mu.Lock()
	conv := sess.convertDowngradeCandidatesLocked(c, []*mqttDeferredDel{dd})
	stillHeld := sess.drains[sid] == held
	sess.mu.Unlock()

	if len(conv.drains) != 0 || dd.converted {
		t.Fatalf("a held sid must refuse the conversion (drains=%d converted=%v)",
			len(conv.drains), dd.converted)
	}
	// The assertion is on the HELD drain's survival, not on the absence of a new one: an
	// absence would also hold if the conversion had done nothing at all.
	if !stillHeld {
		t.Fatal("the older HELD drain must keep the sid: overwriting it erases a " +
			"past-PUBREC intent that routine client behaviour must not destroy (F-094)")
	}
	// And the ordinary teardown's verdicts ride this record instead.
	if len(conv.voided) == 0 {
		t.Fatal("a refused conversion must void the transmitted registers HERE, so the " +
			"verdicts ride the same record that removes the membership")
	}
}

// TestCommitDropsTheTransferredMarkers is T25: the transferred markers must be RETIRED
// when the staging settles, and the drain owning them is what does it.
//
// The claim is failure to remove them at COMMIT, not that they would otherwise live
// forever — a later adoption, a NAK pass or a teardown can also remove them.
func TestCommitDropsTheTransferredMarkers(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-commit-markers")

	const dur, sid = "dur-qos0-commit", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	installClosedDelivery(t, c, sess, cfg, 1)

	mark := mqttDeferredKey{jsDur: dur, sseq: 99}
	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = cfg
	sess.subs[sid] = 0
	sess.cpending = map[string]map[uint64]uint16{dur: {61: 6}}
	sess.pendingPublish = map[uint16]*mqttPending{6: {jsDur: dur, sseq: 61, jsAckSubject: "ack.61", transmitted: true}}
	sess.deferred = map[mqttDeferredKey]string{mark: "ack.99"}
	sess.mu.Unlock()

	dd := &mqttDeferredDel{sid: sid, cc: cfg, convert: true}
	sess.mu.Lock()
	conv := sess.convertDowngradeCandidatesLocked(c, []*mqttDeferredDel{dd})
	_, transferred := sess.deferred[mark]
	sess.mu.Unlock()
	if len(conv.drains) != 1 || !transferred {
		t.Fatalf("the fixture must convert AND transfer, or the commit has nothing to retire "+
			"(drains=%d transferred=%v)", len(conv.drains), transferred)
	}

	sess.finishDowngradeDrains(c, conv)

	sess.mu.Lock()
	_, stillThere := sess.deferred[mark]
	sess.mu.Unlock()
	if stillThere {
		t.Fatal("the commit must drop the markers the drain owns: the staging is settled, " +
			"so no first transmission of theirs remains to protect")
	}
}

// TestUnconvertIsScopedToThisConversionsDrains is the helper contract for the SCOPED
// un-convert, and it is labelled as a contract rather than a race: on the live path this
// packet's drains are the only staged ones, so the scoping buys provability — a
// per-drain disposition — rather than covering a reachable case.
//
// The whole-session form would unstage a drain this conversion never created.
func TestUnconvertIsScopedToThisConversionsDrains(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "qos0-scoped")

	const mineDur, mineSid = "dur-qos0-scoped-mine", "foo"
	const otherDur, otherSid = "dur-qos0-scoped-other", "bar"

	mine := addTestDrain(sess, mineSid, mineDur, mineSid, 1, map[uint64]*drainSeq{41: {claimed: true}})
	other := addTestDrain(sess, otherSid, otherDur, otherSid, 1, map[uint64]*drainSeq{42: {claimed: true}})
	sess.mu.Lock()
	mine.cc = &ConsumerConfig{Durable: mineDur, DeliverSubject: mqttSubPrefix + mineDur}
	other.cc = &ConsumerConfig{Durable: otherDur, DeliverSubject: mqttSubPrefix + otherDur}
	mine.staged, other.staged = true, true
	sess.mu.Unlock()

	sess.mu.Lock()
	sess.unconvertDowngradeDrainsLocked(mqttDowngradeConversion{drains: []mqttConvertedDrain{{d: mine}}})
	_, mineBack := sess.cons[mineSid]
	_, mineDrained := sess.drains[mineSid]
	otherStill := sess.drains[otherSid] == other
	_, otherInCons := sess.cons[otherSid]
	sess.mu.Unlock()

	if !mineBack || mineDrained {
		t.Fatalf("this conversion's drain must go back to sess.cons (back=%v stillDrained=%v)",
			mineBack, mineDrained)
	}
	if !otherStill || otherInCons {
		t.Fatal("a drain this conversion did not create must be left exactly where it was: " +
			"the whole-session form would unstage it and hand its durable to a rollback " +
			"that never owned it")
	}
}

// TestCompletionReopensNothingForAReplacedSubscription is T15a, and it is a DIFFERENT
// guard from the foreign-drain-object one: here the drain is the right object, still
// filed in both maps, and it is the SUBSCRIPTION on the deliver subject that has been
// replaced since the staging closed it.
//
// The drain maps cannot answer that question — they key on the durable, not on the
// subscription — so the identity has to be carried from the staging. Reopening whatever
// currently occupies the subject would be a write against an object this packet never
// closed and does not own.
func TestCompletionReopensNothingForAReplacedSubscription(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-reopen-replaced")

	const dur, sid = "dur-qos0-replaced", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	staged := installClosedDelivery(t, c, sess, cfg, 1)

	d := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{45: {claimed: true}})
	sess.mu.Lock()
	d.cc, d.staged = cfg, true
	sess.mu.Unlock()

	// A different subscription object now occupies the deliver subject, closed as the
	// staging left it. The drain and both maps are untouched.
	replacement := &subscription{subject: []byte(cfg.DeliverSubject), sid: []byte(cfg.DeliverSubject),
		mqtt: &mqttSub{qos: 1, jsDur: dur, closed: true}}
	c.mu.Lock()
	c.subs[cfg.DeliverSubject] = replacement
	c.mu.Unlock()

	sess.finishDowngradeDrains(c, mqttDowngradeConversion{drains: []mqttConvertedDrain{{d: d, sub: staged}}})

	sess.subsMu.Lock()
	replacementClosed := replacement.mqtt.closed
	sess.subsMu.Unlock()
	if !replacementClosed {
		t.Fatal("the completion must reopen only the subscription its own staging closed: " +
			"a replacement belongs to whoever installed it, and opening it hands a drained " +
			"durable's deliveries to a subscription this packet never closed")
	}
	// The drain still commits: the record is durable either way.
	sess.mu.Lock()
	stillStaged := d.staged
	sess.mu.Unlock()
	if stillStaged {
		t.Fatal("a refused reopen must not block the COMMIT")
	}
}

// TestAmbiguousDowngradeSaveDefersTheCompletionToTheResolver is T8's landed arm, and it
// covers the boundary that justified converting at the final-update save at all: an
// ambiguous write must leave the drain STAGED and its delivery CLOSED until those exact
// bytes are proven durable, then commit and reopen through the retained continuation.
//
// Registering before the outcome is known is correct — that is what retainOnLanded is for.
// What must never happen is EXECUTION on a forked verdict, which the sibling test asserts.
func TestAmbiguousDowngradeSaveDefersTheCompletionToTheResolver(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-ambiguous")

	const dur, sid = "dur-qos0-ambiguous", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	sub := installClosedDelivery(t, c, sess, cfg, 1)

	d := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{41: {claimed: true}})
	sess.mu.Lock()
	d.cc, d.staged = cfg, true
	sess.mu.Unlock()
	conv := mqttDowngradeConversion{drains: []mqttConvertedDrain{{d: d, sub: sub}}}

	// The packet's ambiguous arm registers the completion rather than running it.
	sess.retainOnLanded(func(recordTx) { sess.finishDowngradeDrains(c, conv) })

	// Until the bytes are proven: staged, and closed. Committing here would ack-discard a
	// fresh delivery the rolled-back record may still claim; reopening here would
	// retransmit under a membership that may never have landed.
	sess.mu.Lock()
	sess.subsMu.Lock()
	stagedBefore, closedBefore := d.staged, sub.mqtt.closed
	sess.subsMu.Unlock()
	sess.mu.Unlock()
	if !stagedBefore || !closedBefore {
		t.Fatalf("an ambiguous save must leave the drain staged and its delivery closed "+
			"(staged=%v closed=%v)", stagedBefore, closedBefore)
	}

	// The resolver proves them durable and runs what was retained.
	sess.commitUncertainWrites(recordTx{})

	sess.mu.Lock()
	sess.subsMu.Lock()
	stagedAfter, closedAfter := d.staged, sub.mqtt.closed
	sess.subsMu.Unlock()
	sess.mu.Unlock()
	if stagedAfter {
		t.Fatal("the landed continuation must COMMIT the drain: the record is durable, so a " +
			"fresh delivery is owed an ack-discard rather than being left to AckWait")
	}
	if closedAfter {
		t.Fatal("the landed continuation must reopen delivery, or the drain never " +
			"retransmits what it is holding")
	}
}

// TestForkedDowngradeSaveDiscardsTheCompletion is T8's forked arm and M11's designated
// test. It asserts the continuation is NOT EXECUTED, directly, rather than inferring it
// from an end state a later foreign cleanup could also produce.
func TestForkedDowngradeSaveDiscardsTheCompletion(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-forked")

	const dur, sid = "dur-qos0-forked", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	sub := installClosedDelivery(t, c, sess, cfg, 1)

	d := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{41: {claimed: true}})
	sess.mu.Lock()
	d.cc, d.staged = cfg, true
	sess.mu.Unlock()
	conv := mqttDowngradeConversion{drains: []mqttConvertedDrain{{d: d, sub: sub}}}

	var ran atomic.Bool
	sess.retainOnLanded(func(recordTx) {
		ran.Store(true)
		sess.finishDowngradeDrains(c, conv)
	})

	// A FORKED verdict: these bytes demonstrably did not land, so the continuation's
	// premise is false and it must be dropped rather than run.
	sess.settleForkedWithoutDisposal()
	// And a LATER, unrelated resolution must not fire it either — the retained list is
	// session-wide and unkeyed, which is exactly why the forked path takes it.
	sess.commitUncertainWrites(recordTx{})

	if ran.Load() {
		t.Fatal("a forked verdict must DISCARD the completion: its premise is that those " +
			"exact bytes became durable, and a fork proves they did not — committing the " +
			"drain and reopening delivery would act on a record the sibling owns")
	}
	sess.mu.Lock()
	sess.subsMu.Lock()
	staged, closed := d.staged, sub.mqtt.closed
	sess.subsMu.Unlock()
	sess.mu.Unlock()
	if !staged || !closed {
		t.Fatalf("a discarded completion must leave the state untouched (staged=%v closed=%v)",
			staged, closed)
	}
}

// TestStagingCancelsAnAllocationWhoseStoreIsStillInFlight is T17 and M3's designated
// test: it is why the pending cancellation happens at the FILTER's staging rather than at
// the conversion boundary.
//
// The delivery barrier does not stop a callback that is already past it. beginSendLocked
// tests the pending entry's registration state — regPending, regCancelled, regForeign —
// and never sub.mqtt.closed, so an allocation whose register store is still in flight
// would otherwise confirm and transmit under a membership the packet has removed.
// Cancelling at staging is what refuses it; deferring that to the boundary widens the
// window by the whole filter loop.
//
// The oracle is the SEND GATE's refusal, asserted positively, plus the withheld marker
// that keeps the eventual first transmission's DUP clear.
func TestStagingCancelsAnAllocationWhoseStoreIsStillInFlight(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-inflight-store")

	const dur, sid = "dur-qos0-inflight", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	// OPEN, unlike the completion tests: this fixture starts before the downgrade, and
	// lowering the barrier is one of the things the staging must be observed doing.
	sess.mu.Lock()
	if _, err := sess.processQOS12Sub(c, []byte(cfg.DeliverSubject), []byte(cfg.DeliverSubject), nil,
		false, 1, false, false, false, 0, cfg.Durable, mqttDeliverMsgCbQoS12); err != nil {
		sess.mu.Unlock()
		t.Fatalf("installing the delivery subscription: %v", err)
	}
	sess.mu.Unlock()

	const pi, sseq = uint16(3), uint64(71)
	sess.mu.Lock()
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = cfg
	sess.subs[sid] = 1
	// An allocation whose register store is STILL IN FLIGHT: regPending, with an ack
	// subject, exactly as a callback waiting in storeInflightRegister leaves it.
	sess.cpending = map[string]map[uint64]uint16{dur: {sseq: pi}}
	sess.pendingPublish = map[uint16]*mqttPending{
		pi: {jsDur: dur, sseq: sseq, jsAckSubject: "ack.71", regPending: true, regNonce: "zzzzzzzzzzzzzzzzzzzzzz"},
	}
	// The premise, or this test proves nothing: the entry matches what
	// confirmInflightRegister requires to PROCEED — present, still regPending, and naming
	// this durable and sequence. Its store completing would then clear regPending and the
	// send would follow.
	ack := sess.pendingPublish[pi]
	proceeds := ack != nil && ack.regPending && ack.jsDur == dur && ack.sseq == sseq
	sess.c = c
	sess.mu.Unlock()
	if !proceeds {
		t.Fatal("the fixture must start with an allocation confirmation would PROCEED on, " +
			"or the refusal below proves nothing about the cancellation")
	}

	// The QoS 0 downgrade stages a conversion candidate.
	sess.mu.Lock()
	cancels, closed, voided := sess.stageDeferredDeleteLocked(c, cfg, true)
	sess.mu.Unlock()

	if len(cancels) != 1 {
		t.Fatalf("the staging must cancel the in-flight allocation, got %+v", cancels)
	}
	if closed == nil {
		t.Fatal("the staging must lower the delivery barrier")
	}
	if len(voided) != 0 {
		t.Fatalf("a candidate's staging must record NO verdict on a transmitted register, got %v", voided)
	}

	// Now the store completes. Confirmation must REFUSE — "the allocation was CANCELLED
	// under us … this register describes a delivery that will never be sent" — which is
	// the only thing standing between a completing store and a transmission under a
	// membership the packet has removed. beginSendLocked would not have stopped it:
	// confirmation is what clears regPending, and the gate never inspects
	// sub.mqtt.closed.
	confirmed := sess.confirmInflightRegister(c, mqttLandedRegister{gen: sess.gen, pi: pi, jsDur: dur, sseq: sseq,
		regSeq: 900, nonce: "zzzzzzzzzzzzzzzzzzzzzz", lin: "zzzzzzzzzzzzzzzzzzzzzz", qos: 1}, 901, false)
	sess.mu.Lock()
	marker, marked := sess.deferred[mqttDeferredKey{jsDur: dur, sseq: sseq}]
	sess.mu.Unlock()
	if confirmed {
		t.Fatal("confirmation must refuse a cancelled allocation: admitting it would send a " +
			"PUBLISH under a membership this packet has already removed, and the client " +
			"would hold an identifier for an exchange nothing on this side remembers")
	}
	if !marked || marker != "ack.71" {
		t.Fatalf("the cancellation must leave a withheld marker so the eventual FIRST "+
			"transmission carries DUP=0, got %q present=%v", marker, marked)
	}
}

// TestRollbackKeepsTheTransferredMarkerSoTheFirstSendIsNotDup is T7b and T23 together, at
// the level the guarantee actually lives: a marker survives a definite rollback, and
// trackPublish then clears DUP on the transmission that finally goes out.
//
// Both marker provenances are covered in one fixture, because the transfer does not
// distinguish them and a test that covered only the cancelled one would miss the
// regression this path would otherwise introduce:
//
//   - one CANCELLED at staging, whose sequence is already out of cpending;
//   - one withheld at the in-flight CAP, which never had a registration at all.
//
// The oracle is trackPublish's own dup return, not the marker's presence alone: the marker
// exists to make a first transmission carry DUP=0 ([MQTT-3.3.1-1]), and asserting only
// that the map entry survived would pass against a broker that never consulted it.
func TestRollbackKeepsTheTransferredMarkerSoTheFirstSendIsNotDup(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-marker-rollback")

	const dur, sid = "dur-qos0-mrb", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	sess.mu.Lock()
	if _, err := sess.processQOS12Sub(c, []byte(cfg.DeliverSubject), []byte(cfg.DeliverSubject), nil,
		false, 1, false, false, false, 0, cfg.Durable, mqttDeliverMsgCbQoS12); err != nil {
		sess.mu.Unlock()
		t.Fatalf("installing the delivery subscription: %v", err)
	}
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons[sid] = cfg
	sess.subs[sid] = 1
	const startedSeq, cancelSeq, capSeq = uint64(41), uint64(51), uint64(61)
	sess.cpending = map[string]map[uint64]uint16{dur: {startedSeq: 1, cancelSeq: 2}}
	sess.pendingPublish = map[uint16]*mqttPending{
		// transmitted: the exchange the drain will owe
		1: {jsDur: dur, sseq: startedSeq, jsAckSubject: "ack.41", regNonce: "aaaaaaaaaaaaaaaaaaaaaa", transmitted: true},
		// still storing: the staging cancels this one and leaves its marker
		2: {jsDur: dur, sseq: cancelSeq, jsAckSubject: "ack.51", regPending: true, regNonce: "bbbbbbbbbbbbbbbbbbbbbb"},
	}
	// and one withheld at the cap, with NO registration
	sess.deferred = map[mqttDeferredKey]string{{jsDur: dur, sseq: capSeq}: "ack.61"}
	sess.mu.Unlock()

	// Stage the candidate, then convert, then roll the packet back.
	sess.mu.Lock()
	cancels, _, _ := sess.stageDeferredDeleteLocked(c, cfg, true)
	sess.subs[sid] = 0
	dd := &mqttDeferredDel{sid: sid, cc: cfg, convert: true}
	conv := sess.convertDowngradeCandidatesLocked(c, []*mqttDeferredDel{dd})
	sess.mu.Unlock()
	if len(cancels) != 1 || len(conv.drains) != 1 {
		t.Fatalf("the fixture must cancel one allocation and convert (cancels=%d drains=%d)",
			len(cancels), len(conv.drains))
	}

	sess.mu.Lock()
	sess.unconvertDowngradeDrainsLocked(conv)
	sess.subs[sid] = 1 // the rollback restores the pre-packet membership
	sess.mu.Unlock()

	// Both markers must still be there, and each must clear DUP on its FIRST transmission.
	for _, tc := range []struct {
		name string
		sseq uint64
	}{
		{"cancelled at staging", cancelSeq},
		{"withheld at the in-flight cap", capSeq},
	} {
		sess.mu.Lock()
		_, marked := sess.deferred[mqttDeferredKey{jsDur: dur, sseq: tc.sseq}]
		sess.mu.Unlock()
		if !marked {
			t.Fatalf("%s: the marker must survive a rolled-back conversion, or its eventual "+
				"first transmission arrives with DUP=1 against [MQTT-3.3.1-1]", tc.name)
		}
		// A REDELIVERY of that sequence: JetStream's delivery count is above one, and it
		// says nothing about DUP — a fresh allocation's entry is not transmitted.
		sess.mu.Lock()
		rePI, _ := sess.trackPublish(dur, testInflightAckSubject(dur, tc.sseq, 2))
		transmitted := rePI != 0 && sess.pendingPublish[rePI].transmitted
		sess.mu.Unlock()
		if rePI == 0 || transmitted {
			t.Fatalf("%s: the first transmission must carry DUP=0: nothing was ever sent, "+
				"whatever JetStream's delivery count says (pi=%d transmitted=%v)", tc.name, rePI, transmitted)
		}
	}
}

// TestDrainingAllowedFiresItsEligibilitySeam pins the seam T14b counts. Without it that
// test could pass against a broker that never evaluated the precondition at all, and the
// seam itself would be dead code in the default build.
func TestDrainingAllowedFiresItsEligibilitySeam(t *testing.T) {
	// NOT parallel: it installs a PROCESS-GLOBAL hook, so running beside another test
	// would let each corrupt the other's view of it.
	// TestParallelTestsTouchNoProcessGlobalState enforces this, and the fix is dropping
	// t.Parallel() rather than weakening that guard.
	_, c, sess, _ := drainTestSession(t, "qos0-eligibility-seam")

	var calls atomic.Int32
	restore := setMQTTTestDrainingAllowedHook(func() { calls.Add(1) })
	defer restore()

	sess.drainingAllowed(c)
	sess.drainingAllowed(c)
	if n := calls.Load(); n != 2 {
		t.Fatalf("the eligibility seam must fire once per evaluation, got %d for two calls", n)
	}
}

// TestCompletionReopensNothingWhenNothingWasCaptured is the nil-capture case, and it is a
// distinct guard from the replaced-pointer one.
//
// closeSharedDeliveryLocked returns nil for a subscription that was ALREADY closed as well
// as for an absent one, so a nil capture is not evidence that this packet owns whatever is
// installed now — it is evidence that this packet closed nothing. Exempting nil from the
// identity comparison would let the completion open a subscription some other owner closed.
func TestCompletionReopensNothingWhenNothingWasCaptured(t *testing.T) {
	t.Parallel()
	_, c, sess, _ := drainTestSession(t, "qos0-reopen-nilcapture")

	const dur, sid = "dur-qos0-nilcapture", "foo"
	cfg := realDrainConsumer(t, sess, dur, sid)
	// Installed and CLOSED by someone else, which is exactly what makes the staging's
	// close return nil.
	other := installClosedDelivery(t, c, sess, cfg, 1)

	d := addTestDrain(sess, sid, dur, sid, 1, map[uint64]*drainSeq{46: {claimed: true}})
	sess.mu.Lock()
	d.cc, d.staged = cfg, true
	sess.mu.Unlock()

	// sub is nil: this packet closed nothing.
	sess.finishDowngradeDrains(c, mqttDowngradeConversion{drains: []mqttConvertedDrain{{d: d}}})

	sess.subsMu.Lock()
	stillClosed := other.mqtt.closed
	sess.subsMu.Unlock()
	if !stillClosed {
		t.Fatal("a nil capture must reopen NOTHING: it means this packet closed nothing, " +
			"so opening whatever is installed would act on another owner's subscription")
	}
}
