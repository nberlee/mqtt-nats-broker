// Copyright 2025 The mqtt-nats-bridge Authors
// SPDX-License-Identifier: Apache-2.0

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testDowngradeDurable is the durable name of the plain consumer serving one filter, read
// before a downgrade converts it. Keyed on the SID — the NATS-converted subject — because
// that is what sess.cons and sess.drains are keyed on; keying by MQTT filter finds nothing
// whether or not the broker is correct.
func testDowngradeDurable(t *testing.T, sess *mqttSession, filter string) (durable, sid string) {
	t.Helper()
	conv, _ := mqttToNATSSubjectConversion([]byte(filter), false)
	sid = string(conv)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	cc, ok := sess.cons[sid]
	if !ok || cc == nil {
		t.Fatalf("no consumer for sid %q (filter %q): the fixture never reached the route it names (cons=%v)",
			sid, filter, sess.cons)
	}
	return cc.Durable, sid
}

// testDrainClaimsLocked is testDrainClaims for a caller already holding sess.mu.
func testDrainClaimsLocked(sess *mqttSession) int {
	n := 0
	for _, d := range sess.drainByDur {
		n += d.claimedCount()
	}
	return n
}

// TestMQTTQoS0ReSubscribeKeepsAStartedQoS1Exchange is T1: the acceptance test for
// docs/ROADMAP.md's "A QoS 1/2 -> QoS 0 re-SUBSCRIBE destroys outstanding exchanges".
//
// [MQTT-3.8.4-3] requires a re-SUBSCRIBE on an identical filter to REPLACE the
// subscription without interrupting the flow of publications, so a PUBLISH already
// transmitted must still complete. The downgrade therefore converts the QoS 1 consumer
// into a drain instead of deleting it.
//
// The durable is checked BEFORE the last claim is released: deletion afterwards is
// correct, so a check made after the PUBACK would pass against a broker that destroyed
// the exchange outright.
func TestMQTTQoS0ReSubscribeKeepsAStartedQoS1Exchange(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dg/one"
	conn, sess, pi := testDrainSetup(t, s, o, "qos0dg1", topic, 1)
	defer conn.c.Close()

	dur, sid := testDowngradeDurable(t, sess, topic)

	// The downgrade. Granted 0, and the filter must SUCCEED: refusing it would be its
	// own conformance break.
	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	// The consumer is gone from cons and the drain owns the sid (I7), with the started
	// sequence still claimed.
	sess.mu.Lock()
	_, stillCons := sess.cons[sid]
	drains, claims := len(sess.drains), testDrainClaimsLocked(sess)
	sess.mu.Unlock()
	if stillCons {
		t.Fatal("the downgraded consumer must leave sess.cons: the drain owns the sid (I7)")
	}
	if drains != 1 {
		t.Fatalf("the downgrade must convert its consumer into a drain, got %d drains", drains)
	}
	if claims != 1 {
		t.Fatalf("the drain must still claim the started sequence, got %d claims", claims)
	}
	// Positively: the durable still exists. An assertion that the exchange "was not
	// lost" would also pass if nothing had ever been delivered.
	if !testDurableExists(t, s, dur) {
		t.Fatalf("the durable %q must survive the downgrade: deleting it deletes the message with it", dur)
	}

	// And the exchange completes: the client acknowledges the identifier it is holding.
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess); n != 0 {
			return fmt.Errorf("the PUBACK must release the drain's claim, still %d", n)
		}
		return nil
	})
}

// TestMQTTQoS0ReSubscribeCompletesAQoS2ExchangePastPubRec is T2's discriminating half:
// the QoS 2 handshake must still reach PUBCOMP after the downgrade.
//
// PUBREC is sent AFTER the downgrade deliberately. The other order is regression
// coverage only: an ordinary PUBREC removes the PUBLISH tracking, stores the PUBREL and
// keeps its owner slot, so it proves nothing about the conversion — and a drain intent is
// recorded only where PUBREC finds an existing drain.
func TestMQTTQoS0ReSubscribeCompletesAQoS2ExchangePastPubRec(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dg/two"
	conn, sess, pi := testDrainSetup(t, s, o, "qos0dg2", topic, 2)
	defer conn.c.Close()

	dur, _ := testDowngradeDurable(t, sess, topic)

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	if !testDurableExists(t, s, dur) {
		t.Fatalf("the durable %q must survive the downgrade", dur)
	}

	// The exchange is still the client's to complete, at QoS 2, under its original
	// identifier: [MQTT-4.3.3-1] makes an in-flight QoS 2 message categorically a QoS 2
	// message and [MQTT-4.4.0-1] binds the identifier to the exchange.
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, conn.r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)

	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess); n != 0 {
			return fmt.Errorf("PUBCOMP must complete the drained exchange, %d claims left", n)
		}
		return nil
	})
}

// TestMQTTQoS0ReSubscribeDeliversFreshMessagesOnceAtQoS0 is T3, and it is the coexistence
// assertion no existing test makes: while the drain is still completing what it started,
// a NEW message on the filter must reach the client exactly once, at QoS 0, from the raw
// subscription — the drain discarding its own copy rather than delivering a second.
//
// Three things about the shape, each of which a first draft got wrong:
//
//   - The started exchange is left OUTSTANDING. Completing it first retires the drain
//     (finishDrainIfEmpty, correctly), so the fixture would then be testing life after
//     retirement and would pass against a broker that never converted anything.
//   - The fresh message is published at QoS 1, so it ENTERS the messages stream and the
//     drain genuinely has a copy to discard. At QoS 0 it never reaches the durable, so a
//     broker delivering both copies would still pass.
//   - AckWait is long, so the started message's redelivery cannot arrive inside the
//     window and be mistaken for the drain delivering the fresh one.
func TestMQTTQoS0ReSubscribeDeliversFreshMessagesOnceAtQoS0(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dg/three"
	conn, sess, _ := testDrainSetup(t, s, o, "qos0dg3", topic, 1)
	defer conn.c.Close()

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dg3pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, topic, 7, []byte("fresh"))

	// One copy, at QoS 0: flags 0, no identifier, nothing to acknowledge.
	testMQTTCheckPubMsg(t, conn.c, conn.r, topic, 0, []byte("fresh"))
	// And no second copy. A drain that DELIVERED its copy instead of discarding it would
	// send the same payload again, at QoS 1, with an identifier.
	testMQTTExpectNothing(t, conn.r)

	// The drain is still draining, and the fresh sequence was never claimed by it — which
	// is what "discarded" means here.
	sess.mu.Lock()
	drains, claims := len(sess.drains), testDrainClaimsLocked(sess)
	sess.mu.Unlock()
	if drains != 1 {
		t.Fatalf("the drain must still be draining while the raw QoS 0 subscription serves, got %d", drains)
	}
	if claims != 1 {
		t.Fatalf("the drain must claim its started sequence and NOT the fresh one, got %d claims", claims)
	}
}

// TestMQTTQoS0ReSubscribeRecordNamesTheDrainNotTheConsumer is T4 and T4b: the acceptance
// evidence is the PERSISTED RECORD, not the in-memory maps.
//
// The record must carry the downgraded membership and the drain in ONE write — either the
// filter is at QoS 0 and its drain exists, or neither is persisted (I4) — with no Cons
// entry for the sid, because the drain owns it.
//
// T4b is the second half and it is what a missing assertion would hide: the record must
// NOT name the transmitted register's nonce among CancelledRegs. A never-transmitted
// verdict on a delivery that is about to be drained is a lie, and any save in the packet
// would make it durable.
//
// The oracle is the marshalled body, captured by the same harness the revocable-downgrade
// tests use: saveWithDetail marshals and releases sess.mu before storing, so the live maps
// are not what was written, and reading the record back from inside the write's own
// completion wedges the packet.
func TestMQTTQoS0ReSubscribeRecordNamesTheDrainNotTheConsumer(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dg/record"
	conn, sess, _ := testDrainSetup(t, s, o, "qos0dg4", topic, 1)
	defer conn.c.Close()

	_, sid := testDowngradeDurable(t, sess, topic)

	// The nonce of the register the started delivery is transmitted under. Read before
	// the downgrade, because the conversion moves the entry into the drain.
	var nonce string
	sess.mu.Lock()
	for _, pending := range sess.pendingPublish {
		if pending != nil && pending.regNonce != _EMPTY_ {
			nonce = pending.regNonce
		}
	}
	sess.mu.Unlock()
	if nonce == _EMPTY_ {
		t.Fatal("the fixture must leave a transmitted delivery holding an in-flight register")
	}

	w := &testSubRevokeWrittenRecords{}
	defer w.armWrittenRecords(sess)()

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	records, err := w.landed()
	if err != nil {
		t.Fatalf("observing the written records: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("the downgrade must PERSIST: a conversion left only in memory is lost")
	}
	ps := records[len(records)-1]

	// Two-value read: a MISSING key also reads as zero, and a missing key is a removal,
	// not a downgrade.
	qos, ok := ps.Subs[sid]
	if !ok || qos != 0 {
		t.Fatalf("the record must carry the filter at QoS 0, got qos=%d present=%v (Subs=%v)", qos, ok, ps.Subs)
	}
	if _, named := ps.Cons[sid]; named {
		t.Fatalf("the record must NOT name a consumer for the converted sid: the drain owns it (Cons=%v)", ps.Cons)
	}
	pd, drained := ps.Drains[sid]
	if !drained || pd == nil {
		t.Fatalf("the record must name the drain for %q, or the durable is referenced nowhere (Drains=%v)", sid, ps.Drains)
	}
	if len(pd.Started) != 1 {
		t.Fatalf("the persisted drain must claim the started sequence, got %+v", pd.Started)
	}
	if pd.QoS != 1 {
		t.Fatalf("the drain must keep the PRE-downgrade delivery terms, got QoS %d: the "+
			"exchange it owes was transmitted under those, and the QoS 0 filter is served "+
			"by the raw subscription instead", pd.QoS)
	}
	// T4b.
	if carriesNonce(ps, nonce) {
		t.Fatalf("the record must not declare the TRANSMITTED register %q never transmitted: "+
			"its exchange is being drained, not destroyed (CancelledRegs=%v)", nonce, ps.CancelledRegs)
	}
}

// TestMQTTQoS0ReSubscribeFallsBackWhenDrainingIsRefused is T14: the stated residual.
//
// A drain must outlive an offline session, so a stream-forced consumer inactivity limit
// disqualifies it — the durable would be reaped with its started messages, since
// $MQTT_msgs is interest-retained. Where one is set the downgrade falls back to today's
// destructive teardown and says so, exactly as UNSUBSCRIBE already does.
//
// The assertion is POSITIVE on the fallback — the teardown seam actually firing, and the
// warning actually logged — rather than "no drain exists", which would also pass against
// a broker that silently did nothing at all.
func TestMQTTQoS0ReSubscribeFallsBackWhenDrainingIsRefused(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dglim/topic"
	conn, sess, _ := testDrainSetup(t, s, o, "qos0dglim", topic, 1)
	defer conn.c.Close()

	// Imposed out of band, as a deployment would.
	si, err := sess.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookupStream: %v", err)
	}
	cfg := si.Config
	cfg.ConsumerLimits.InactiveThreshold = time.Hour
	if _, err := sess.jsa.updateStream(&cfg); err != nil {
		// Never skipped: a skip would let this test pass while proving nothing about the
		// guard it exists for.
		t.Fatalf("could not impose a consumer inactivity limit on %q: %v", mqttStreamName, err)
	}

	dur, sid := testDowngradeDurable(t, sess, topic)

	// The destructive teardown's own seam. Installed process-globally, so this test does
	// not call t.Parallel() (TestParallelTestsTouchNoProcessGlobalState), and restored at
	// the point of installation so a t.Fatal below cannot leak it into later tests.
	var tornDown atomic.Bool
	restore := setMQTTTestDeferredDeleteHook(func(gotSid, gotDur string) {
		if gotSid == sid && gotDur == dur {
			tornDown.Store(true)
		}
	})
	defer restore()

	l := &captureWarnLogger{warn: make(chan string, 10)}
	s.SetLogger(l, false, false)

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	if !tornDown.Load() {
		t.Fatal("under a consumer inactivity limit the downgrade must take the ordinary " +
			"destructive teardown, and that has to be observed rather than inferred")
	}
	if n := testDrainCount(sess); n != 0 {
		t.Fatalf("a drain was created under a consumer inactivity limit (%d); it would be "+
			"reaped with its started messages", n)
	}
	select {
	case w := <-l.warn:
		if !strings.Contains(w, "inactivity") {
			t.Fatalf("the refusal must say what it costs, got %q", w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the refusal must be logged: it is a stated conformance residual, not a silent one")
	}
}

// TestMQTTQoS0DowngradeSavesWhenOnlyTheConversionChanged is the save-condition contract,
// and its fixture is the FIFTH provenance the scope ruling put in scope: a session already
// holding a QoS 0 membership AND a live consumer for the same filter, which is what a
// failed drain adoption leaves behind.
//
// Because the membership is already 0, the packet's membership loop changes nothing, so
// `needUpdate` would be false and no record would be written — leaving the conversion in
// memory with nothing persisted. The extended condition ("or the conversion changed
// state") is what forces the save, and this is the only fixture that can prove it: in an
// ordinary positive-QoS downgrade the membership change forces the save anyway.
//
// The QoS 0 membership is poked directly, because the producer of that state is the
// separately owned failed-adoption defect and reproducing it here would test that instead.
func TestMQTTQoS0DowngradeSavesWhenOnlyTheConversionChanged(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dgsave/topic"
	conn, sess, _ := testDrainSetup(t, s, o, "qos0dgsave", topic, 1)
	defer conn.c.Close()

	_, sid := testDowngradeDurable(t, sess, topic)

	// The state a failed adoption leaves: membership at QoS 0, consumer still live.
	sess.mu.Lock()
	sess.subs[sid] = 0
	sess.mu.Unlock()

	w := &testSubRevokeWrittenRecords{}
	defer w.armWrittenRecords(sess)()

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	records, err := w.landed()
	if err != nil {
		t.Fatalf("observing the written records: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("a conversion with no membership change must still WRITE: left unpersisted, " +
			"the drain exists only in memory and its durable is named nowhere")
	}
	ps := records[len(records)-1]
	if _, drained := ps.Drains[sid]; !drained {
		t.Fatalf("the written record must name the drain (Drains=%v)", ps.Drains)
	}
}

// TestMQTTQoS0DowngradeRollbackKeepsTheConsumerLive is T7: a definitely-failed final save
// leaves the pre-packet QoS 1/2 subscription exactly as it was, with its exchange intact
// and no drain in either map.
//
// The assertion is taken at the ROLLBACK's own save, because a failed packet closes the
// connection immediately afterwards: checking later cannot tell "the rollback restored it"
// from "teardown removed everything". By the second hook call rollbackSubscribePacket has
// already un-converted and restored the maps, and the connection is still up.
func TestMQTTQoS0DowngradeRollbackKeepsTheConsumerLive(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dgrb2/topic"
	conn, sess, _ := testDrainSetup(t, s, o, "qos0dgrb2", topic, 1)
	defer conn.c.Close()

	dur, sid := testDowngradeDurable(t, sess, topic)

	var saves atomic.Int32
	var consAtRollback, drainsAtRollback, claimsAtRollback atomic.Int32
	var sampled atomic.Bool
	restore := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != sess {
			return nil
		}
		switch saves.Add(1) {
		case 1:
			return errors.New("definite failure of the packet's final save")
		case 2:
			// Nothing here calls t.Fatal: this is a broker goroutine.
			hs.mu.Lock()
			if _, ok := hs.cons[sid]; ok {
				consAtRollback.Store(1)
			}
			drainsAtRollback.Store(int32(len(hs.drains)))
			claimsAtRollback.Store(int32(testDrainClaimsLocked(hs)))
			np := len(hs.pendingPublish)
			hs.mu.Unlock()
			if np == 1 {
				sampled.Store(true)
			}
		}
		return nil
	})
	defer restore()

	// Written directly: the failed save closes the connection rather than answering.
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(2 + 2 + len(topic) + 1)
	w.WriteUint16(2)
	w.WriteBytes([]byte(topic))
	w.WriteByte(0)
	if _, err := testMQTTWrite(conn.c, w.Bytes()); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}

	// Wait for EVIDENCE THE PACKET RAN — the rollback's own save — not for an end state.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if saves.Load() < 2 {
			return fmt.Errorf("waiting for the failed save and the rollback's own")
		}
		return nil
	})

	if consAtRollback.Load() != 1 {
		t.Fatal("the rollback must restore the pre-packet consumer under its sid: the " +
			"downgrade never became durable, so the client's QoS 1 subscription still stands")
	}
	if n := drainsAtRollback.Load(); n != 0 {
		t.Fatalf("a rolled-back conversion must leave NO drain, got %d", n)
	}
	if n := claimsAtRollback.Load(); n != 0 {
		t.Fatalf("a rolled-back conversion must leave no drain claims, got %d", n)
	}
	if !sampled.Load() {
		t.Fatal("the outstanding exchange must survive the rollback: it was transmitted, " +
			"and the client is still holding its identifier")
	}
	// The durable is the pre-packet one and must NOT be released: the deferred delete
	// never ran, so it is still the live subscription's.
	if !testDurableExists(t, s, dur) {
		t.Fatalf("the rollback must keep the pre-packet durable %q", dur)
	}
}

// TestMQTTQoS0ThenPositiveInOnePacketConvertsNothing is T11: with the same filter carried
// at QoS 0 and then at QoS > 0, the later occurrence REVOKES the downgrade, so there is
// nothing left to convert and the consumer stays live.
func TestMQTTQoS0ThenPositiveInOnePacketConvertsNothing(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dgrev/topic"
	conn, sess, pi := testDrainSetup(t, s, o, "qos0dgrev", topic, 1)
	defer conn.c.Close()

	dur, sid := testDowngradeDurable(t, sess, topic)

	testMQTTSub(t, 2, conn.c, conn.r,
		[]*mqttFilter{{filter: topic, qos: 0}, {filter: topic, qos: 1}}, []byte{0, 1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	cc, live := sess.cons[sid]
	drains := len(sess.drains)
	sess.mu.Unlock()
	if !live || cc == nil {
		t.Fatal("the revoking occurrence must leave the consumer live in sess.cons")
	}
	if cc.Durable != dur {
		t.Fatalf("the revocation must keep the SAME durable, got %q want %q", cc.Durable, dur)
	}
	if drains != 0 {
		t.Fatalf("a revoked downgrade must convert nothing, got %d drains", drains)
	}
	// And the subscription still serves: the exchange completes through it.
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)
}

// TestMQTTQoS0DowngradeRollbackReleasesAPacketCreatedDurable is T9, and it exists because
// the population it covers IS reachable — a conclusion I got wrong once and a holistic
// review corrected.
//
// The claim had been that a consumer this packet CREATED can never hold a transmitted
// delivery at the same packet's boundary, because processSubs holds the subscription
// barrier throughout. That is not what the barrier does: wait blocks only until every
// SUBSCRIBE *in flight when it sampled the barrier* finishes, and "SUBSCRIBEs entering
// afterwards are deliberately not waited for" (mqttSubsBarrier.wait). So a publisher that
// sampled the barrier BEFORE this packet started is never blocked by it, and its store can
// land after the packet creates its DeliverNew consumer.
//
// This fixture forces exactly that, with mqttTestDurableWritePauseV parking the publish
// between its authorization gate and its durable write:
//
//	publish parked -> occurrence 1 creates the consumer -> store released -> delivery
//	transmitted and tracked -> occurrence 2 downgrades -> conversion -> final save fails
//	definitely -> un-convert -> rollback releases the durable this packet created.
//
// Without the un-convert that runs before rollbackSubscribePacket enumerates sess.cons,
// the converted consumer has left that map and is neither restored nor released: its
// durable ends up named nowhere in the rolled-back record and deleted by nobody.
func TestMQTTQoS0DowngradeRollbackReleasesAPacketCreatedDurable(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dgpc/topic"
	cl, r := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgpc", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer cl.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	sess := testMQTTResidentSession(t, s, "qos0dgpc")

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgpcpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	conv, _ := mqttToNATSSubjectConversion([]byte(topic), false)
	sid := string(conv)

	// Park the publish just before its durable write. It has already passed the
	// subscription barrier, which is the whole point.
	release := make(chan struct{})
	parked := make(chan struct{}, 1)
	var once sync.Once
	pause := func() {
		once.Do(func() {
			select {
			case parked <- struct{}{}:
			default:
			}
			<-release
		})
	}
	mqttTestDurableWritePauseV.Store(&pause)
	defer mqttTestDurableWritePauseV.Store(nil)

	// The publisher blocks in the hook, so the write goes on its own goroutine — and it is
	// hand-rolled rather than testMQTTPublish, because that helper can t.Fatalf and a
	// failure from a non-test goroutine panics the whole test binary.
	var pubErr atomic.Value
	go func() {
		pw := newMQTTWriter(0)
		pw.WriteByte(mqttPacketPub | 0x2)
		pw.WriteVarInt(2 + len(topic) + 2 + 3)
		pw.WriteBytes([]byte(topic))
		pw.WriteUint16(11)
		pw.Write([]byte("mid"))
		if _, err := testMQTTWrite(pc, pw.Bytes()); err != nil {
			pubErr.Store(err.Error())
		}
	}()
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		if e, ok := pubErr.Load().(string); ok {
			t.Fatalf("writing the parked publish: %s", e)
		}
		t.Fatal("the publish never reached its durable-write seam: the fixture cannot " +
			"produce a delivery that lands after the consumer is created")
	}

	// At occurrence 1's commit boundary: release the store and wait for the delivery to
	// be TRACKED. Nothing here calls t.Fatal — it runs on the subscriber's read loop, and
	// a late failure from a broker goroutine panics the whole binary.
	var createdDur, hookErr atomic.Value
	var atFirst sync.Once
	restorePause := setMQTTTestSubPause(func(at string) {
		if at != "provisioned:"+sid {
			return
		}
		atFirst.Do(func() {
			sess.mu.Lock()
			if cc := sess.cons[sid]; cc != nil {
				createdDur.Store(cc.Durable)
			}
			sess.mu.Unlock()
			close(release)
			deadline := time.Now().Add(5 * time.Second)
			for {
				sess.mu.Lock()
				n := len(sess.cpending)
				sess.mu.Unlock()
				if n > 0 {
					return
				}
				if time.Now().After(deadline) {
					hookErr.Store("no delivery was tracked between the two occurrences")
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	})
	defer restorePause()

	var saves atomic.Int32
	restoreSave := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != sess {
			return nil
		}
		if saves.Add(1) == 1 {
			return errors.New("definite failure of the packet's final save")
		}
		return nil
	})
	defer restoreSave()

	// One packet: the filter at QoS 1, then the same filter at QoS 0.
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(2 + 2*(2+len(topic)+1))
	w.WriteUint16(2)
	w.WriteBytes([]byte(topic))
	w.WriteByte(1)
	w.WriteBytes([]byte(topic))
	w.WriteByte(0)
	if _, err := testMQTTWrite(cl, w.Bytes()); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}

	checkFor(t, 8*time.Second, 25*time.Millisecond, func() error {
		if saves.Load() < 2 {
			return fmt.Errorf("waiting for the failed save and the rollback's own")
		}
		return nil
	})
	if e, ok := hookErr.Load().(string); ok && e != "" {
		t.Fatalf("the fixture never reached the route it names: %s", e)
	}
	dur, _ := createdDur.Load().(string)
	if dur == "" {
		t.Fatal("the fixture never observed the consumer the first occurrence created")
	}
	checkFor(t, 8*time.Second, 50*time.Millisecond, func() error {
		if testDurableExists(t, s, dur) {
			return fmt.Errorf("the rollback must release the durable %q this packet created "+
				"and then converted: the rolled-back record names it nowhere", dur)
		}
		return nil
	})
}

// TestMQTTQoS0DowngradeChecksDrainingAllowedOncePerPacket is T14b and M13's designated
// test. The precondition is a property of the messages STREAM, not of a filter, and
// drainingAllowed does a stream lookup with no lock held — so a packet downgrading two
// filters must ask once, not once per filter.
//
// The counter is scoped to the eligibility lookup specifically: counting every stream
// lookup the packet makes would not discriminate, because provisioning makes its own.
func TestMQTTQoS0DowngradeChecksDrainingAllowedOncePerPacket(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const t1, t2 = "qos0dg2f/one", "qos0dg2f/two"
	conn, _, _ := testDrainSetup(t, s, o, "qos0dg2f", t1, 1)
	defer conn.c.Close()
	// A second QoS 1 filter, so the packet below carries TWO downgrade candidates.
	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: t2, qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	var lookups atomic.Int32
	restore := setMQTTTestDrainingAllowedHook(func() { lookups.Add(1) })
	defer restore()

	testMQTTSub(t, 3, conn.c, conn.r,
		[]*mqttFilter{{filter: t1, qos: 0}, {filter: t2, qos: 0}}, []byte{0, 0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	if n := lookups.Load(); n != 1 {
		t.Fatalf("the drain precondition must be evaluated ONCE per packet, got %d for two "+
			"downgraded filters: it is a property of the stream, and the lookup runs with "+
			"no lock held", n)
	}
}

// TestMQTTQoS0DowngradeSucceedsBesideAFailedSiblingFilter is T12: a packet whose downgrade
// succeeds while a sibling filter fails must end coherently — the conversion persisted, the
// failed filter answered 0x80, and no packet-wide rollback.
//
// It was asked for as coverage of a suspected regression in the final save's error
// handling, and that regression does NOT exist: every per-filter failure arm assigns an
// err shadowed inside the loop body, so the outer err the final save reads is always nil.
// Restoring the conditional assignment leaves this test passing, which is the empirical
// half of that refutation. The test is kept because the mixed-outcome packet is worth
// pinning on its own account: it is the only fixture here where a conversion has to
// survive a sibling's failure arm running cancelFilterDeferred and rollbackSharedSub
// beside it.
//
// The failed filter is LAST deliberately, since that is the ordering in which a stale
// error would have survived had the shadow not existed.
func TestMQTTQoS0DowngradeSucceedsBesideAFailedSiblingFilter(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const good, bad = "qos0dgsib/good", "qos0dgsib/bad"
	conn, sess, pi := testDrainSetup(t, s, o, "qos0dgsib", good, 1)
	defer conn.c.Close()

	dur, sid := testDowngradeDurable(t, sess, good)

	// The LAST filter fails its consumer provisioning.
	restore := setMQTTTestQOS12SubFailHook(func(durable string) error {
		if strings.Contains(durable, sess.idHash) && durable != dur {
			return errors.New("test: delivery subscription refused")
		}
		return nil
	})
	defer restore()

	// One packet: the held filter downgraded to QoS 0, then a NEW filter that fails.
	// SUBACK must carry both per-filter answers rather than the connection closing.
	testMQTTSub(t, 2, conn.c, conn.r,
		[]*mqttFilter{{filter: good, qos: 0}, {filter: bad, qos: 1}},
		[]byte{0, mqttSubAckFailure})
	testMQTTFlush(t, conn.c, nil, conn.r)

	// The successful downgrade PERSISTED and converted: a rollback here would have undone
	// it, which is the regression this test pins.
	sess.mu.Lock()
	_, stillCons := sess.cons[sid]
	drains, claims := len(sess.drains), testDrainClaimsLocked(sess)
	sess.mu.Unlock()
	if stillCons {
		t.Fatal("the successful downgrade must stand: a stale per-filter error must not " +
			"send a confirmed save into the rollback arm")
	}
	if drains != 1 || claims != 1 {
		t.Fatalf("the conversion must survive its failed sibling (drains=%d claims=%d)", drains, claims)
	}
	if !testDurableExists(t, s, dur) {
		t.Fatalf("the drained durable %q must survive", dur)
	}
	// And the exchange still completes, which is what the client actually cares about.
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess); n != 0 {
			return fmt.Errorf("the PUBACK must complete the drained exchange, %d claims left", n)
		}
		return nil
	})
}

// TestMQTTQoS0DowngradeAmbiguousSaveCompletesOnLandedResolution is T8's integration arm,
// which holistic round 2 required after finding the unit versions bypassed production
// entirely: they built the drain by hand and called the resolver helpers, so removing the
// real retainOnLanded registration would have left them passing while stranding a
// converted drain.
//
// This drives a real downgrade through processSubs with the final save's response LOST, so
// the packet takes its saveMaybeLanded arm and keeps its state; the resolver then proves
// those exact bytes durable and the retained continuation commits the drain and reopens
// its delivery. The oracle is the client's exchange completing afterwards, not a map.
func TestMQTTQoS0DowngradeAmbiguousSaveCompletesOnLandedResolution(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dgamb/topic"
	conn, sess, pi := testDrainSetup(t, s, o, "qos0dgamb", topic, 1)
	defer conn.c.Close()

	dur, sid := testDowngradeDurable(t, sess, topic)

	// Lose the response to the packet's final save exactly once: the write LANDS, its
	// acknowledgement does not, which is what makes the outcome ambiguous rather than
	// failed.
	// The held state is sampled INSIDE the hook, which is the only point it is guaranteed
	// observable: the conversion runs before the save, and the resolver can commit the
	// drain as soon as the connection closes. Nothing here calls t.Fatal — it is a broker
	// goroutine — so the observations go into atomics.
	var lost atomic.Int32
	var heldStaged, heldClosed atomic.Bool
	// observed closes AFTER both samples are stored. lost.Add is not a substitute: it
	// becomes visible before the hook takes sess.mu, so waiting on it can read the
	// initial false values while the hook is still paused and report a broken ambiguity
	// barrier that is working fine.
	observed := make(chan struct{})
	var once sync.Once
	restore := setMQTTTestSessStoreResultHook(
		func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if hs != sess || err != nil {
				return resp, err
			}
			if lost.Add(1) == 1 {
				hs.mu.Lock()
				if d := hs.drains[sid]; d != nil {
					heldStaged.Store(d.staged)
					if cc := d.cc; cc != nil {
						hs.c.mu.Lock()
						sub := hs.c.subs[cc.DeliverSubject]
						hs.c.mu.Unlock()
						hs.subsMu.Lock()
						heldClosed.Store(sub != nil && sub.mqtt != nil && sub.mqtt.closed)
						hs.subsMu.Unlock()
					}
				}
				hs.mu.Unlock()
				once.Do(func() { close(observed) })
				return nil, errors.New("test: session record response lost")
			}
			return resp, err
		})
	// Deferred IMMEDIATELY: every failure path below must restore a process-global hook,
	// or it leaks into every later test in the binary.
	defer restore()

	// No SUBACK: an unresolved packet closes the connection instead of answering.
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(2 + 2 + len(topic) + 1)
	w.WriteUint16(2)
	w.WriteBytes([]byte(topic))
	w.WriteByte(0)
	if _, err := testMQTTWrite(conn.c, w.Bytes()); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}

	// Wait for the OBSERVATIONS to be stored, not merely for the response to have been
	// lost: the two are ordered by this channel and by nothing else.
	select {
	case <-observed:
	case <-time.After(8 * time.Second):
		t.Fatal("the final save's response was never lost, so the packet took its ordinary " +
			"confirmed arm and this test proves nothing")
	}

	// At the ambiguous save the conversion must be HELD: staged, not committed, with its
	// delivery still closed. Committing or reopening there would act on a record whose
	// fate is unknown.
	if !heldStaged.Load() {
		t.Fatal("at the ambiguous save the drain must still be STAGED: committing it would " +
			"ack-discard fresh deliveries a rolled-back record may still claim")
	}
	if !heldClosed.Load() {
		t.Fatal("at the ambiguous save the drain's delivery must still be CLOSED: reopening " +
			"it would retransmit under a membership that may never have landed")
	}

	// The resolver proves the bytes durable; the retained continuation then commits and
	// reopens. A fresh connection is what drives resolution, as it would in production.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgamb", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	checkFor(t, 8*time.Second, 50*time.Millisecond, func() error {
		sess2 := testMQTTResidentSession(t, s, "qos0dgamb")
		sess2.mu.Lock()
		defer sess2.mu.Unlock()
		d := sess2.drains[sid]
		if d == nil {
			return fmt.Errorf("the drain must survive the resolution: the record names it")
		}
		if d.staged {
			return fmt.Errorf("the landed resolution must COMMIT the drain")
		}
		return nil
	})
	// And the durable is still there to deliver from.
	if !testDurableExists(t, s, dur) {
		t.Fatalf("the drained durable %q must survive an ambiguous-then-landed save", dur)
	}
	_ = pi
}
