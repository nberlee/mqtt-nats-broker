// Copyright 2025 The mqtt-nats-broker Authors
// SPDX-License-Identifier: Apache-2.0

//go:build mqtt_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"io"
	"net"
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
// NOT name the transmitted register's nonce among VoidedRegs. A never-transmitted
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
			"its exchange is being drained, not destroyed (VoidedRegs=%v)", nonce, ps.VoidedRegs)
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

	// And the record must not name the consumer this route destroys. THIS is the arm that
	// reaches the release with dd.convert FALSE: a stream inactivity limit means the
	// descriptor is never even offered to the conversion, so a release that skipped a
	// non-candidate would leave the record naming a durable the teardown just deleted.
	// The other tests all go through candidates, so none of them can see it.
	asm := testMQTTGetAccountSessionManager(t, s, "qos0dglim")
	ps, _ := testReconcileRecord(t, asm, "qos0dglim")
	key, _ := mqttToNATSSubjectConversion([]byte(topic), true)
	if qos, ok := ps.Subs[string(key)]; !ok || qos != 0 {
		t.Fatalf("the membership must be PRESENT at QoS 0, got %v (present %v)", qos, ok)
	}
	if cc := ps.Cons[sid]; cc != nil {
		t.Fatalf("the destructive fallback must still release its consumer reference: the "+
			"durable is deleted, so a record naming it is a reference to nothing, got %+v", cc)
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

// testRollbackDowngradeFixture is T9's shared route: a persistent subscriber, a publish parked
// just before its durable write, and ONE SUBSCRIBE packet carrying the same filter at QoS 1 and
// then at QoS 0 whose final save fails definitely, so the packet rolls back. Occurrence 1 creates
// the consumer; at its commit boundary the fixture captures that durable, releases the parked
// store and calls atProvisioned, which decides how far the delivery gets before occurrence 2
// downgrades. It returns once the failed save and the rollback's own have both run.
//
// A publisher that sampled the subscription barrier BEFORE this packet started is never blocked
// by it — wait blocks only until every SUBSCRIBE in flight when it sampled finishes, and "SUBSCRIBEs
// entering afterwards are deliberately not waited for" (mqttSubsBarrier.wait) — so its store can
// land after the packet creates its DeliverNew consumer. That is the population: a delivery on a
// consumer the rolled-back packet itself created.
//
// Nothing in the hooks calls t.Fatal: they run on the subscriber's read loop, and a late failure
// from a broker goroutine panics the whole binary. They report through hookErr.
func testRollbackDowngradeFixture(t *testing.T, s *Server, o *Options, clientID, topic string,
	atProvisioned func(sess *mqttSession, dur string) string, atConverted func()) (
	sess *mqttSession, cl net.Conn, sid, dur string) {

	t.Helper()
	var r *mqttReader
	cl, r = testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { cl.Close() })
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	sess = testMQTTResidentSession(t, s, clientID)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID + "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { pc.Close() })
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	conv, _ := mqttToNATSSubjectConversion([]byte(topic), false)
	sid = string(conv)

	// Park the publish just before its durable write. It has already passed the subscription
	// barrier, which is the whole point.
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
	t.Cleanup(func() { mqttTestDurableWritePauseV.Store(nil) })

	// The publisher blocks in the hook, so the write goes on its own goroutine — and it is
	// hand-rolled rather than testMQTTPublish, because that helper can t.Fatalf.
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

	var createdDur, hookErr atomic.Value
	var atFirst sync.Once
	restorePause := setMQTTTestSubPause(func(at string) {
		if at == "converted:"+sid {
			// sess.mu is HELD here: signal only.
			if atConverted != nil {
				atConverted()
			}
			return
		}
		if at != "provisioned:"+sid {
			return
		}
		atFirst.Do(func() {
			sess.mu.Lock()
			d := ""
			if cc := sess.cons[sid]; cc != nil {
				d = cc.Durable
			}
			sess.mu.Unlock()
			createdDur.Store(d)
			close(release)
			if e := atProvisioned(sess, d); e != "" {
				hookErr.Store(e)
			}
		})
	})
	t.Cleanup(restorePause)

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
	t.Cleanup(restoreSave)

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
	dur, _ = createdDur.Load().(string)
	if dur == "" {
		t.Fatal("the fixture never observed the consumer the first occurrence created")
	}
	return sess, cl, sid, dur
}

// testRolledBackHold reads the created durable's name ownership and resource, and reports what
// stands: the ownership state, the disposal reason, and whether exactly the exchange nonce holds
// it for replay. It never fails the test, so a checkFor can poll it.
func testRolledBackHold(s *Server, sess *mqttSession, dur, nonce string) (mqttNameOwnState, string, bool, error) {
	asm := sess.asm
	own, _, oerr := asm.jsa.loadNameOwnershipPhysical(asm.sessStreamEpoch, mqttOwnedStreamMsgs, dur)
	if oerr != nil || own == nil {
		return 0, "", false, fmt.Errorf("the ownership of %q could not be read: %v", dur, oerr)
	}
	res, _, rerr := asm.jsa.loadOutboundResource(sess.id, asm.sessStreamEpoch, own.OwnerGen, own.OwnerInc)
	if rerr != nil || res == nil {
		return own.State, "", false, fmt.Errorf("the resource of %q could not be read: %v", dur, rerr)
	}
	reason := ""
	if res.Disposal != nil {
		reason = res.Disposal.Reason
	}
	held := len(res.Deps) == 1 && res.Deps[0].Nonce == nonce && res.Deps[0].Role == mqttRoleSourceReplay
	return own.State, reason, held, nil
}

// TestMQTTQoS0DowngradeRollbackReleasesAPacketCreatedDurable is T9: the rollback of a packet that
// created a consumer and then converted it into a drain must still RELEASE it, even when a delivery
// on it is STARTED.
//
// Without the un-convert that runs before rollbackSubscribePacket enumerates sess.cons, the
// converted consumer has left that map and is neither restored nor released: its durable ends up
// named nowhere in the rolled-back record and decided by nobody.
//
// The conversion happens only for a delivery that is STARTED at the boundary, so the fixture holds
// the delivery's send section OPEN — at its reserved window, before intent — until the packet's
// final-update boundary has converted the consumer, and then lets the section proceed. (A refusing
// window hook is a staged crash meant to be followed by a restart, so it is not used to shape a
// live process.)
//
// Release is not deletion. A release DECIDES the consumer (`rolled-back`) and hands it to the
// retirement owner, which WITHHOLDS the delete while an obligation still holds the consumer
// (F-105 7c-lc): the started exchange is one. The test stops at that held state. What becomes of
// the exchange afterwards is a stated residual in docs/CONFORMANCE.md (a possibly-attempted
// PUBLISH on a consumer a failed SUBSCRIBE created is not retransmitted on resume), and no
// assertion here makes it a contract.
func TestMQTTQoS0DowngradeRollbackReleasesAPacketCreatedDurable(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID, topic = "qos0dgpc", "qos0dgpc/topic"
	inSection := make(chan struct{})
	converted := make(chan struct{})
	var secOnce, convOnce sync.Once
	var openAtConversion, sawConversion atomic.Bool
	var nonce atomic.Value

	// The delivery's section, held OPEN at its reserved window until the conversion.
	restoreWin := setMQTTTestMarkerSendWindowHook(func(hs *mqttSession, pi uint16, window string) bool {
		if hs.id != clientID || window != mqttMarkerSendReserved {
			return true
		}
		hs.mu.Lock()
		if p := hs.pendingPublish[pi]; p != nil {
			nonce.Store(p.regNonce)
		}
		hs.mu.Unlock()
		secOnce.Do(func() { close(inSection) })
		select {
		case <-converted:
		case <-time.After(5 * time.Second):
		}
		return true
	})
	defer restoreWin()

	atProvisioned := func(_ *mqttSession, _ string) string {
		select {
		case <-inSection:
			return ""
		case <-time.After(5 * time.Second):
			return "the delivery never opened its send section between the two occurrences"
		}
	}
	atConverted := func() {
		convOnce.Do(func() {
			select {
			case <-inSection:
				openAtConversion.Store(true)
			default:
			}
			sawConversion.Store(true)
			close(converted)
		})
	}
	sess, _, _, dur := testRollbackDowngradeFixture(t, s, o, clientID, topic, atProvisioned, atConverted)
	if !sawConversion.Load() || !openAtConversion.Load() {
		t.Fatalf("the fixture never reached the conversion the un-convert unwinds: converted %v, "+
			"section open at it %v", sawConversion.Load(), openAtConversion.Load())
	}
	n, _ := nonce.Load().(string)
	if n == "" {
		t.Fatal("the fixture never captured the started exchange")
	}

	// RELEASED: the rolled-back record names the durable nowhere, and the durable is DECIDED
	// rolled-back, held by exactly the started exchange.
	checkFor(t, 8*time.Second, 50*time.Millisecond, func() error {
		if inCons, inDrains := testRecordNamesDurableNoFatal(sess, dur); inCons || inDrains {
			return fmt.Errorf("the rolled-back record still names %q (cons %v, drains %v)", dur, inCons, inDrains)
		}
		_, reason, held, err := testRolledBackHold(s, sess, dur, n)
		if err != nil {
			return err
		}
		if reason != mqttDisposalRolledBack {
			return fmt.Errorf("the rollback did not release %q: its disposal is %q, want %q", dur, reason, mqttDisposalRolledBack)
		}
		if !held {
			return fmt.Errorf("%q is not held by the started exchange %q", dur, n)
		}
		return nil
	})

	// WITHHELD while held: the owner must not delete a consumer an obligation still replays from.
	time.Sleep(time.Second)
	state, _, held, err := testRolledBackHold(s, sess, dur, n)
	if err != nil {
		t.Fatal(err)
	}
	if !testDurableExists(t, s, dur) || state != mqttNameOwnLive || !held {
		t.Fatalf("a decided consumer still held by its exchange must stay: exists %v, ownership %v, held %v",
			testDurableExists(t, s, dur), state, held)
	}
}

// TestMQTTQoS0DowngradeRollbackWithdrawsAnAdmittedUnsentDelivery is T9's other held arm: the
// delivery's exchange is ADMITTED — locally confirmed and marker-backed — but its send section never
// opens before the packet rolls back, so nothing was written. The rolled-back consumer is held by that
// exchange exactly as a started one would hold it, and what ends the hold is WITHDRAWAL, not
// retransmission: the next accepted resume finds the unsent marker standing and withdraws the
// exchange (op-withdraw-unsent, Publish -> WithdrawnUnsent), after which the owner retires the name
// and deletes the consumer. The client is owed nothing, and receives nothing, at any point.
func TestMQTTQoS0DowngradeRollbackWithdrawsAnAdmittedUnsentDelivery(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID, topic = "qos0dgau", "qos0dgau/topic"
	admitted := make(chan struct{})
	var admOnce sync.Once
	var createdDur, parkErr, nonce atomic.Value

	// Between the confirmed admission and the send section, held until the rollback has DECIDED
	// the created consumer — completion observed in the store, not inferred from a save count.
	restoreAdm := func() func() {
		h := func(hs *mqttSession, pi uint16) {
			if hs.id != clientID {
				return
			}
			hs.mu.Lock()
			p := hs.pendingPublish[pi]
			switch {
			case p == nil:
				parkErr.Store("no pending entry at the admitted window")
			case p.markerSeq == 0 || p.regPending || p.transmitted || p.sendNonce != _EMPTY_:
				parkErr.Store(fmt.Sprintf("the entry is not admitted, marker-backed and unopened: marker %d, "+
					"pending %v, transmitted %v, section %q", p.markerSeq, p.regPending, p.transmitted, p.sendNonce))
			default:
				nonce.Store(p.regNonce)
			}
			hs.mu.Unlock()
			admOnce.Do(func() { close(admitted) })
			deadline := time.Now().Add(8 * time.Second)
			for time.Now().Before(deadline) {
				d, _ := createdDur.Load().(string)
				if d == _EMPTY_ {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if _, reason, _, err := testRolledBackHold(s, hs, d, _EMPTY_); err == nil && reason == mqttDisposalRolledBack {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			parkErr.Store("the rollback never decided the created consumer while the delivery was held")
		}
		mqttTestDeliveryAdmittedPauseV.Store(&h)
		return func() { mqttTestDeliveryAdmittedPauseV.Store(nil) }
	}()
	defer restoreAdm()

	atProvisioned := func(_ *mqttSession, d string) string {
		createdDur.Store(d)
		select {
		case <-admitted:
			return ""
		case <-time.After(5 * time.Second):
			return "the delivery's admission never landed between the two occurrences"
		}
	}
	sess, cl, _, dur := testRollbackDowngradeFixture(t, s, o, clientID, topic, atProvisioned, nil)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if e, ok := parkErr.Load().(string); ok {
			return errors.New(e)
		}
		if n, _ := nonce.Load().(string); n == "" {
			return fmt.Errorf("the delivery has not reached its admitted window")
		}
		return nil
	})
	n := nonce.Load().(string)

	// HELD by the admitted exchange, and nothing was written to the client.
	checkFor(t, 8*time.Second, 50*time.Millisecond, func() error {
		state, reason, held, err := testRolledBackHold(s, sess, dur, n)
		if err != nil {
			return err
		}
		if reason != mqttDisposalRolledBack || !held || state != mqttNameOwnLive {
			return fmt.Errorf("%q: disposal %q, held %v, ownership %v; want rolled-back, held, LIVE", dur, reason, held, state)
		}
		return nil
	})
	if !testDurableExists(t, s, dur) {
		t.Fatalf("the owner deleted %q while its admitted exchange still held it", dur)
	}
	if err := cl.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatalf("arming the read deadline: %v", err)
	}
	if b, err := io.ReadAll(cl); len(b) != 0 {
		t.Fatalf("the client received %d bytes (%x, %v) from a delivery that was never sent", len(b), b, err)
	}
	cl.Close()

	// The next accepted resume withdraws the unsent exchange; the hold ends and the owner retires
	// the name and deletes the consumer. Nothing is transmitted.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	asm := sess.asm
	checkFor(t, 8*time.Second, 50*time.Millisecond, func() error {
		own, _, oerr := asm.jsa.loadNameOwnershipPhysical(asm.sessStreamEpoch, mqttOwnedStreamMsgs, dur)
		if oerr != nil || own == nil {
			return fmt.Errorf("the ownership of %q could not be read: %v", dur, oerr)
		}
		x, _, xerr := asm.jsa.loadOutboundExchange(sess.id, asm.sessStreamEpoch, own.OwnerGen, n)
		if xerr != nil || x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
			return fmt.Errorf("the admitted exchange %q is not withdrawn: %+v, err %v", n, x, xerr)
		}
		if own.State != mqttNameOwnRetired {
			return fmt.Errorf("the name %q is not retired: %v", dur, own.State)
		}
		if testDurableExists(t, s, dur) {
			return fmt.Errorf("the withdrawn exchange's consumer %q still exists", dur)
		}
		return nil
	})
	if _, _, got := testMQTTReadPacketReady(r2, 500*time.Millisecond); got {
		t.Fatal("the resume transmitted a delivery that was never sent")
	}
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

// TestMQTTQoS0DowngradeSavesWhenOnlyTheReleaseChanged is the sibling of
// TestMQTTQoS0DowngradeSavesWhenOnlyTheConversionChanged, and it is the ONLY test that
// reaches the production save-condition closure for the CONSUMER RELEASE.
//
// Same fifth-provenance fixture — a QoS 0 membership beside a live consumer, which is what
// a failed drain adoption leaves — but with NOTHING STARTED, so the conversion refuses and
// reports no work. The membership loop changes nothing either. The release's own return
// value is then the only thing that can set needUpdate, and without it the entry leaves
// sess.cons while the record keeps naming a durable the teardown is about to delete: the
// defect this item fixes, inverted.
//
// The QoS 0 membership is poked directly, for the reason its sibling gives: the producer of
// that state is the separately owned failed-adoption defect, and reproducing it here would
// test that instead.
func TestMQTTQoS0DowngradeSavesWhenOnlyTheReleaseChanged(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dgrel/topic"
	cl, r := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgrel"}, o.MQTT.Host, o.MQTT.Port)
	defer cl.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, cl, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
	testMQTTFlush(t, cl, nil, r)

	sess := testMQTTResidentSession(t, s, "qos0dgrel")
	dur, sid := testDowngradeDurable(t, sess, topic)

	// The state a failed adoption leaves: membership at QoS 0, consumer still live. No
	// delivery has started, which is what keeps the conversion out of this.
	sess.mu.Lock()
	sess.subs[sid] = 0
	sess.mu.Unlock()

	w := &testSubRevokeWrittenRecords{}
	defer w.armWrittenRecords(sess)()

	testMQTTSub(t, 2, cl, r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, cl, nil, r)

	records, err := w.landed()
	if err != nil {
		t.Fatalf("observing the written records: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("a RELEASE with no membership change and no conversion must still WRITE: " +
			"unpersisted, the record goes on naming a consumer whose durable this packet " +
			"deletes, which is exactly the stale reference this item removes")
	}
	ps := records[len(records)-1]
	if qos, ok := ps.Subs[sid]; !ok || qos != 0 {
		t.Fatalf("the record must keep the membership PRESENT at QoS 0, got %v (present %v)", qos, ok)
	}
	if cc := ps.Cons[sid]; cc != nil {
		t.Fatalf("the written record must not name the released consumer, got %+v", cc)
	}
	if d := ps.Drains[sid]; d != nil {
		t.Fatalf("nothing was started, so no drain may be named either, got %+v", d)
	}
	// And the teardown really does follow, so the omission is not premature.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if testSubRevokeConsumerLives(t, sess, dur) {
			return fmt.Errorf("durable %q still exists", dur)
		}
		return nil
	})
}

// TestMQTTQoS0DowngradeRollbackReleasesADestroyedPacketCreatedDurable is the un-remove's
// production oracle, and it is deliberately the DESTRUCTIVE twin of
// TestMQTTQoS0DowngradeRollbackReleasesAPacketCreatedDurable.
//
// That test parks a publish so the packet-created consumer holds a TRANSMITTED delivery,
// which makes the second occurrence CONVERT — so the un-convert is what saves it and the
// consumer release never runs. With nothing started the same packet takes the destructive
// arm instead: the release takes the entry out of sess.cons at the final-update boundary,
// and rollbackSubscribePacket discovers this packet's own consumers by WALKING sess.cons.
// Without the un-remove the durable this packet created is named nowhere in the rolled-back
// record and released by nobody.
//
// The oracle is the teardown seam firing for that durable, observed rather than inferred.
func TestMQTTQoS0DowngradeRollbackReleasesADestroyedPacketCreatedDurable(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dgpcd/topic"
	cl, r := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgpcd"}, o.MQTT.Host, o.MQTT.Port)
	defer cl.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	sess := testMQTTResidentSession(t, s, "qos0dgpcd")

	conv, _ := mqttToNATSSubjectConversion([]byte(topic), false)
	sid := string(conv)

	// The teardown seam, installed process-globally, which is why this test is not
	// parallel. It records the durable released for THIS sid.
	var released atomic.Value
	restoreDel := setMQTTTestDeferredDeleteHook(func(gotSid, gotDur string) {
		if gotSid == sid {
			released.Store(gotDur)
		}
	})
	defer restoreDel()

	// Definite failure of the packet's final save, then the rollback's own save.
	var saves atomic.Int32
	restore := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != sess {
			return nil
		}
		if saves.Add(1) == 1 {
			return errors.New("definite failure of the packet's final save")
		}
		return nil
	})
	defer restore()

	// [{topic,1},{topic,0}] in ONE packet: the first occurrence CREATES the durable, the
	// second stages its destruction. Written directly, because the failed save closes the
	// connection rather than answering.
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(2 + (2 + len(topic) + 1) + (2 + len(topic) + 1))
	w.WriteUint16(2)
	w.WriteBytes([]byte(topic))
	w.WriteByte(1)
	w.WriteBytes([]byte(topic))
	w.WriteByte(0)
	if _, err := testMQTTWrite(cl, w.Bytes()); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}

	// Wait for EVIDENCE THE PACKET RAN — the rollback's own save — not for an end state.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if saves.Load() < 2 {
			return fmt.Errorf("waiting for the failed save and the rollback's own")
		}
		return nil
	})
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if released.Load() == nil {
			return fmt.Errorf("waiting for the rolled-back packet to release the durable it created")
		}
		return nil
	})
}

// TestMQTTQoS0DowngradeAmbiguousLandedSaveLeavesTheDurableToTheNextTenure is the ambiguous-landed
// route's oracle, and it observes BOTH stated residuals rather than describing them.
//
// processSubs returns on its saveMaybeLanded arm before the deferred-delete loop, and a
// DESTRUCTIVE descriptor registers no continuation at all — only a converted one does. So
// when the write lands, the record carries the QoS 0 membership and names the durable
// nowhere, while the durable is still alive and its name LIVE. Under Design B the entry has
// also left sess.cons, so neither a same-object reconnect nor a record restore reaches
// processJSConsumer's existing-consumer arm, and the orphan sweep withholds a LIVE name:
// convergence is the next ACCEPTED tenure's settlement (outbound_tenure_settle.go), which
// decides the durable tenure-superseded, and the retirement owner retires and deletes it.
//
// The ACCOUNTING half of that window was a second residual until holistic round 3 refused
// it: the sweep deletes through as.jsa.deleteConsumer and cannot refund what
// sess.deleteConsumer would have, so a charge nothing gives back costs a subscription slot
// for the life of the session. A refund-only landed continuation now gives it back at the
// RESOLUTION, which is why this test asserts the refund at its resolution barrier and the
// durable at the sweep: the two convergences have different mechanisms and this test
// separates them deliberately. The availability consequence is its sibling,
// TestMQTTQoS0DowngradeAmbiguousLandedSaveRefundsTheCharge.
//
// NOTHING is started here, which is what puts the descriptor on the destructive arm.
func TestMQTTQoS0DowngradeAmbiguousLandedSaveLeavesTheDurableToTheNextTenure(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dgambd/topic"
	cl, r := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgambd"}, o.MQTT.Host, o.MQTT.Port)
	defer cl.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, cl, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
	testMQTTFlush(t, cl, nil, r)

	sess := testMQTTResidentSession(t, s, "qos0dgambd")
	dur, sid := testDowngradeDurable(t, sess, topic)
	// Taken while the connection is still UP: the helper finds the manager through the live
	// client, and the unresolved SUBSCRIBE below closes that connection.
	asm := testMQTTGetAccountSessionManager(t, s, "qos0dgambd")
	sess.mu.Lock()
	chargedBefore := sess.tmaxack
	sess.mu.Unlock()
	if chargedBefore == 0 {
		t.Fatal("the fixture never reached the route it names: a QoS 1 subscription must have " +
			"charged tmaxack, or the refund asserted below is unobservable")
	}

	// The teardown must NOT run on this route. Observed rather than assumed.
	var tornDown atomic.Bool
	restoreDel := setMQTTTestDeferredDeleteHook(func(gotSid, _ string) {
		if gotSid == sid {
			tornDown.Store(true)
		}
	})
	defer restoreDel()

	// Lose the response to the packet's final save exactly once: the write LANDS, its
	// acknowledgement does not.
	var lost atomic.Int32
	observed := make(chan struct{})
	var once sync.Once
	restore := setMQTTTestSessStoreResultHook(
		func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if hs != sess || err != nil {
				return resp, err
			}
			if lost.Add(1) == 1 {
				once.Do(func() { close(observed) })
				return nil, errors.New("test: session record response lost")
			}
			return resp, err
		})
	defer restore()

	// No SUBACK: an unresolved packet closes the connection instead of answering.
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(2 + 2 + len(topic) + 1)
	w.WriteUint16(2)
	w.WriteBytes([]byte(topic))
	w.WriteByte(0)
	if _, err := testMQTTWrite(cl, w.Bytes()); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}
	select {
	case <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("the packet's final save never reached the store-result hook, so the " +
			"ambiguity this test needs was never created")
	}

	// The landed record: membership at QoS 0, durable named nowhere.
	var landedSeq uint64
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ps, seq := testReconcileRecord(t, asm, "qos0dgambd")
		landedSeq = seq
		if qos, ok := ps.Subs[sid]; !ok || qos != 0 {
			return fmt.Errorf("waiting for the landed record to carry the QoS 0 membership, got %v (present %v)", qos, ok)
		}
		if cc := ps.Cons[sid]; cc != nil {
			return fmt.Errorf("the landed record still names %q", cc.Durable)
		}
		if d := ps.Drains[sid]; d != nil {
			return fmt.Errorf("the landed record names a drain, so this is not the destructive route")
		}
		return nil
	})
	if tornDown.Load() {
		t.Fatal("the fixture never reached the route it names: the teardown ran, so this is " +
			"not the ambiguous arm and the residual below is not what is being observed")
	}

	// SETTLE THE AMBIGUITY. Neither the record's visibility nor a FALSE isSeqUncertain() is
	// proof that a resolution happened: the store hook signals BEFORE it returns the
	// injected error, so when `observed` closes the packet has not yet reached its
	// saveMaybeLanded arm and the session is not uncertain YET. A poll for
	// !isSeqUncertain() therefore passes on the value the flag has BEFORE arming, and
	// proves nothing about the classification it is named for.
	//
	// So wait for the POSITIVE fact — the ambiguity published, or the landed sequence
	// already adopted, which only settleLandedAttempt does — and then run the resolution
	// from here, on the route reconcileResidentRecordState takes for an unbound uncertain
	// session. A RETURNED verdict is the completion evidence a poll cannot give.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		armed, adopted := sess.seqUncertain, sess.seq
		sess.mu.Unlock()
		if !armed && adopted < landedSeq {
			return fmt.Errorf("waiting for the packet to publish the ambiguity of its final save")
		}
		return nil
	})
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if err := asm.lockSession(sess, nil); err != nil {
			return fmt.Errorf("waiting for the session's ownership gate: %w", err)
		}
		res := s.resolveUncertainSession(asm, sess)
		asm.unlockSession(sess)
		if res != mqttUncertainOurs {
			return fmt.Errorf("the resolver must classify a write that LANDED as ours, got %v", res)
		}
		return nil
	})
	// And the record fence must be IDLE. settleLandedAttempt clears the uncertainty before
	// it runs the landed continuations, and those continuations run inside the resolver's
	// own transaction — so an idle fence, not a cleared flag, is what proves the pass that
	// settled this session (the one above, or a periodic pass that got there first) is not
	// still running one behind the assertions below.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		tok, ok := sess.tryBeginRecordTx()
		if !ok {
			return fmt.Errorf("waiting for the resolver to release the record fence")
		}
		if err := sess.endRecordTx(tok); err != nil {
			return fmt.Errorf("releasing the probe transaction: %w", err)
		}
		return nil
	})
	if sess.isSeqUncertain() {
		t.Fatal("the session must be RESOLVED before the residual is read: while it is " +
			"uncertain every save is refused, so nothing below would be the residual")
	}
	if tornDown.Load() {
		t.Fatal("the resolution must not run the teardown for a DESTRUCTIVE descriptor: only " +
			"a converted one registers a continuation, and that difference is the residual")
	}

	// THE CHARGE IS REFUNDED BY THE RESOLUTION, not by the sweep. The refund runs as a
	// landed continuation inside the resolver's own transaction, which the idle-fence check
	// above has just proven finished, so this is asserted HERE — before either prompt path
	// and before the sweep — because that is what pins WHICH mechanism gives it back. It
	// replaces an earlier assertion that the charge was kept: holistic round 3 weighed that
	// residual as lost availability (a resident session can be refused at the aggregate
	// MaxAckPending ceiling) rather than lost tidiness, and the refund-only continuation is
	// its fix. The DURABLE is still the sweep's, which is the residual that remains.
	sess.mu.Lock()
	chargedAfter := sess.tmaxack
	_, stillCharged := sess.consCharged[dur]
	sess.mu.Unlock()
	if chargedAfter != 0 || stillCharged {
		t.Fatalf("the resolution must refund the released durable's charge: tmaxack must be "+
			"back to 0 (was %d before the packet, %d now) and consCharged must no longer "+
			"name %q (present %v)", chargedBefore, chargedAfter, dur, stillCharged)
	}

	// The durable is ALIVE and named nowhere, and the resolution decided nothing about it.
	if !testDurableExists(t, s, dur) {
		t.Fatalf("the durable %q must still exist on this route: the teardown never ran", dur)
	}

	// PROMPT PATH 1 — a same-object reconnect. The session is still resident, so its
	// sess.cons is what processJSConsumer would consult; the release already emptied it, so
	// the restore reaches qos == 0 && !exists and issues NO consumer delete. What reclaims the
	// durable is the accepted tenure's SETTLEMENT: provisioned under the previous tenure and
	// named by nothing this tenure holds, it is decided tenure-superseded, and the retirement
	// owner retires its name and deletes it.
	cl2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgambd"}, o.MQTT.Host, o.MQTT.Port)
	defer cl2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, cl2, nil, r2)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		own, _, oerr := asm.jsa.loadNameOwnershipPhysical(asm.sessStreamEpoch, mqttOwnedStreamMsgs, dur)
		if oerr != nil || own == nil {
			return fmt.Errorf("the ownership of %q could not be read: %v", dur, oerr)
		}
		res, _, rerr := asm.jsa.loadOutboundResource("qos0dgambd", asm.sessStreamEpoch, own.OwnerGen, own.OwnerInc)
		if rerr != nil || res == nil {
			return fmt.Errorf("the resource of %q could not be read: %v", dur, rerr)
		}
		if res.Disposal == nil || res.Disposal.Reason != mqttDisposalTenureSuperseded {
			return fmt.Errorf("the accepted tenure has not decided %q tenure-superseded: %+v", dur, res.Disposal)
		}
		if own.State != mqttNameOwnRetired {
			return fmt.Errorf("the name %q is not retired yet: %v", dur, own.State)
		}
		if testDurableExists(t, s, dur) {
			return fmt.Errorf("the retired durable %q still exists", dur)
		}
		return nil
	})
	if tornDown.Load() {
		t.Fatalf("the reconnect must not reach the deferred teardown for %q: the consumer entry "+
			"is gone from sess.cons, and the settlement, not the teardown, reclaims it", dur)
	}
	cl2.Close()
	// Let that close finish before the session is evicted: a close still in flight would
	// be acting on an object the eviction below has already replaced.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		bound := sess.c != nil
		sess.mu.Unlock()
		if bound {
			return fmt.Errorf("waiting for the reconnect's close to detach the session")
		}
		return nil
	})

	// PROMPT PATH 2 — a restore from the RECORD, which is what a restart or another broker
	// does. The resident session is evicted first so the CONNECT below cannot rebind this
	// object, and it is driven as a REAL CONNECT: hydrating a session with
	// createOrRestoreSession would prove only that Cons deserialization preserves an
	// absence, because a resurrected reference would come from CONNECT's own
	// reconciliation — processSubs -> processJSConsumer — which hydration never runs.
	asm.removeSession(sess, true)
	cl3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgambd"}, o.MQTT.Host, o.MQTT.Port)
	defer cl3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, cl3, nil, r3)
	restored := testMQTTResidentSession(t, s, "qos0dgambd")
	if restored == sess {
		t.Fatal("the fixture never reached the route it names: the eviction did not take, so " +
			"this is a second same-object reconnect and not a record-sourced restore")
	}
	restored.mu.Lock()
	restoredEntry := restored.cons[sid]
	restored.mu.Unlock()
	if restoredEntry != nil {
		t.Fatalf("a record-sourced restore must not resurrect the released reference, got %+v", restoredEntry)
	}
	if tornDown.Load() {
		t.Fatalf("a record restore must not reach the deferred teardown for %q either", dur)
	}
	cl3.Close()

}

// TestMQTTQoS0DowngradeAmbiguousLandedSaveRefundsTheCharge is the availability half of the
// same route, and it is what holistic round 3 asked for: the resident session must be able
// to subscribe again after the resolution.
//
// The leak it guards is not tidiness. `tmaxack` bounds the session's aggregate
// MaxAckPending at mqttMaxAckTotalLimit, so a charge nothing gives back is a subscription
// slot the client never gets back either — one lost save acknowledgement near the ceiling
// and every later QoS 1/2 SUBSCRIBE is refused for the life of the session.
//
// The fixture puts ONE consumer close to the ceiling (MaxAckPending 40000, so a second
// charge would ask for 80000) rather than filling it with many: the arithmetic is then
// unambiguous, and the assertion is a real grant rather than an inspection of a counter.
func TestMQTTQoS0DowngradeAmbiguousLandedSaveRefundsTheCharge(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	// Two of these would exceed mqttMaxAckTotalLimit (0xFFFF), which is the whole point.
	o.MQTT.MaxAckPending = 40000
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "qos0dgrefund/topic"
	const other = "qos0dgrefund/other"
	cl, r := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgrefund"}, o.MQTT.Host, o.MQTT.Port)
	defer cl.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, cl, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
	testMQTTFlush(t, cl, nil, r)

	sess := testMQTTResidentSession(t, s, "qos0dgrefund")
	dur, sid := testDowngradeDurable(t, sess, topic)
	asm := testMQTTGetAccountSessionManager(t, s, "qos0dgrefund")
	sess.mu.Lock()
	chargedBefore := sess.tmaxack
	sess.mu.Unlock()
	if chargedBefore != 40000 {
		t.Fatalf("the fixture never reached the route it names: the QoS 1 subscription must "+
			"have charged the configured MaxAckPending, got tmaxack=%d", chargedBefore)
	}

	// Lose the response to the packet's final save exactly once: the write LANDS, its
	// acknowledgement does not.
	var lost atomic.Int32
	observed := make(chan struct{})
	var once sync.Once
	restore := setMQTTTestSessStoreResultHook(
		func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if hs != sess || err != nil {
				return resp, err
			}
			if lost.Add(1) == 1 {
				once.Do(func() { close(observed) })
				return nil, errors.New("test: session record response lost")
			}
			return resp, err
		})
	defer restore()

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(2 + 2 + len(topic) + 1)
	w.WriteUint16(2)
	w.WriteBytes([]byte(topic))
	w.WriteByte(0)
	if _, err := testMQTTWrite(cl, w.Bytes()); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}
	select {
	case <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("the packet's final save never reached the store-result hook, so the " +
			"ambiguity this test needs was never created")
	}

	// The landed record: the membership at QoS 0, the durable named nowhere. Keyed on the
	// SID and requiring the membership to be PRESENT, or an absent key would satisfy this
	// barrier without the packet ever having written.
	var landedSeq uint64
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ps, seq := testReconcileRecord(t, asm, "qos0dgrefund")
		if qos, ok := ps.Subs[sid]; !ok || qos != 0 {
			return fmt.Errorf("waiting for the landed record to carry the QoS 0 membership, got %v (present %v)", qos, ok)
		}
		if cc := ps.Cons[sid]; cc != nil {
			return fmt.Errorf("the landed record still names %q", cc.Durable)
		}
		landedSeq = seq
		return nil
	})

	// The same positive barrier as the sibling test: arm, then a RETURNED verdict, then an
	// idle fence, because the refund is a landed continuation and runs inside it.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		armed, adopted := sess.seqUncertain, sess.seq
		sess.mu.Unlock()
		if !armed && adopted < landedSeq {
			return fmt.Errorf("waiting for the packet to publish the ambiguity of its final save")
		}
		return nil
	})
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if err := asm.lockSession(sess, nil); err != nil {
			return fmt.Errorf("waiting for the session's ownership gate: %w", err)
		}
		res := s.resolveUncertainSession(asm, sess)
		asm.unlockSession(sess)
		if res != mqttUncertainOurs {
			return fmt.Errorf("the resolver must classify a write that LANDED as ours, got %v", res)
		}
		return nil
	})
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		tok, ok := sess.tryBeginRecordTx()
		if !ok {
			return fmt.Errorf("waiting for the resolver to release the record fence")
		}
		if err := sess.endRecordTx(tok); err != nil {
			return fmt.Errorf("releasing the probe transaction: %w", err)
		}
		return nil
	})

	// THE AVAILABILITY ASSERTION. A reconnect subscribes to a DIFFERENT filter at QoS 1,
	// which needs another 40000: it is granted only if the released durable's charge came
	// back. Without the refund the provisioning arm refuses with "max_ack_pending for all
	// consumers would be 80000", and the client sees 0x80.
	cl2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0dgrefund"}, o.MQTT.Host, o.MQTT.Port)
	defer cl2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, cl2, r2, []*mqttFilter{{filter: other, qos: 1}}, []byte{1})
	testMQTTFlush(t, cl2, nil, r2)

	// And the counter agrees with the grant: exactly one consumer is charged, the new one.
	sess.mu.Lock()
	chargedAfter := sess.tmaxack
	_, staleCharged := sess.consCharged[dur]
	sess.mu.Unlock()
	if staleCharged || chargedAfter != 40000 {
		t.Fatalf("after the refund and one new subscription the session must carry exactly one "+
			"charge: tmaxack=%d (want 40000) and consCharged must not still name the released "+
			"durable %q (present %v)", chargedAfter, dur, staleCharged)
	}
}
