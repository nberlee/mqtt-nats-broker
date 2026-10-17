// Copyright 2025 The mqtt-nats-broker Authors
// SPDX-License-Identifier: Apache-2.0

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A QoS 0 membership over a live Cons entry, and what a CONNECT restore owes it.
//
// The state under test is a record carrying Subs[f] == 0 while Cons[f] still names a
// durable holding unacknowledged QoS 1/2 exchanges. Resume reconciliation collects
// positive-QoS memberships only, so such an entry is never verified, and the restore used
// to reach processJSConsumer's existing-consumer arm and delete the durable INLINE — with
// its started deliveries, because $MQTT_msgs is interest-retained.
//
// Every cold arm here restarts the broker against the SAME StoreDir. A DISCONNECT is not
// enough: it clears sess.c but keeps a persistent session in asm.sessions, and the next
// CONNECT finds that resident object instead of loading the record, so a fixture built on
// disconnect/reconnect never reaches the route it names.

// testQoS0OverLiveConsRecord drives the production schedule that leaves Subs at QoS 0 while
// Cons still names a live durable, and returns the durable, the sid and the packet
// identifier of the exchange the client is holding.
//
// The schedule is a FAILED DRAIN ADOPTION, which is what the roadmap bullet names as the
// producer, and every step is ordinary client behaviour plus one injected filter failure:
//
//  1. SUBSCRIBE a wildcard at QoS 1 and let one PUBLISH reach the client unacknowledged.
//  2. Re-SUBSCRIBE the same filter at QoS 0. The downgrade converts the consumer into a
//     drain, so the record now holds Subs 0 and Drains.
//  3. Re-SUBSCRIBE at QoS 1 again, with the LEVEL-UP companion made to fail. The main sid
//     adopts the drain back into sess.cons, the companion's failure fails the whole filter,
//     and sess.update therefore skips it — so the membership stays at the QoS 0 the
//     downgrade persisted while cons names the adopted durable.
//
// A wildcard is required: the level-up companion is the only half that can fail after the
// main half has already adopted.
func testQoS0OverLiveConsRecord(t *testing.T, s *Server, o *Options, clientID, filter string,
	qos byte) (conn *mqttTestConn, dur, sid string, pi uint16) {

	t.Helper()
	// A wildcard filter is required (see above), so the setup cannot publish to the filter
	// string itself: "#" is illegal in a PUBLISH topic ([MQTT-3.3.2-2]).
	//
	// It publishes STRICTLY BELOW the wildcard, not at its parent level. "q/one/#" matches
	// "q/one" too ([MQTT-4.7.1-2]), but that match is served by the LEVEL-UP companion, so
	// publishing there starts the exchange on the wrong consumer and the main sid has
	// nothing to convert.
	pubTopic := strings.TrimSuffix(filter, "#") + "x"

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: filter, qos: qos}}, []byte{qos})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID + "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	flags := mqttPubQos1
	if qos == 2 {
		flags = mqttPubQoS2
	}
	testMQTTPublish(t, pc, pr, qos, false, false, pubTopic, 1, []byte("payload"))
	pi = testMQTTCheckPubMsgNoAck(t, c, r, pubTopic, flags, []byte("payload"))

	conn = &mqttTestConn{c: c, r: r}
	sess := testMQTTResidentSession(t, s, clientID)

	// The sid is the NATS-converted FILTER, which for a wildcard needs wcOk — the shared
	// testDowngradeDurable helper passes false and so cannot name a wildcard's sid at all.
	conv, cerr := mqttToNATSSubjectConversion([]byte(filter), true)
	if cerr != nil {
		t.Fatalf("converting filter %q: %v", filter, cerr)
	}
	sid = string(conv)
	sess.mu.Lock()
	cc0 := sess.cons[sid]
	sess.mu.Unlock()
	if cc0 == nil {
		t.Fatalf("no consumer for sid %q (filter %q): the fixture never reached the route it names", sid, filter)
	}
	dur = cc0.Durable

	// Step 2: the downgrade converts rather than destroys (d554f13).
	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: filter, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)
	sess.mu.Lock()
	converted := sess.drains[sid] != nil
	sess.mu.Unlock()
	if !converted {
		t.Fatalf("step 2 did not convert: no drain under sid %q, so the adoption in step 3 has nothing to adopt", sid)
	}

	// Step 3: the re-SUBSCRIBE adopts, then its level-up companion fails.
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	fwcsid := sid[:len(sid)-2] + mqttMultiLevelSidSuffix
	asm.mu.Lock()
	asm.testSubFilterFail = func(exit, failSid string) error {
		if exit != "consumer-fwc" || failSid != fwcsid {
			return nil
		}
		return fmt.Errorf("test: the level-up companion fails after the main half adopted")
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testSubFilterFail = nil
		asm.mu.Unlock()
	}()

	testMQTTSendSubOnly(t, 3, conn.c, []*mqttFilter{{filter: filter, qos: 1}})
	codes := testMQTTReadSubAckTolerantOfPubs(t, conn.c, conn.r, 3)
	if len(codes) != 1 || codes[0] != mqttSubAckFailure {
		t.Fatalf("the level-up failure must fail the whole filter, got SUBACK %v", codes)
	}
	testMQTTFlush(t, conn.c, nil, conn.r)

	// The state itself, asserted rather than assumed: the fixture is worthless if the
	// schedule produced something else.
	sess.mu.Lock()
	memberQoS, memberPresent := sess.subs[sid]
	cc := sess.cons[sid]
	stillDraining := sess.drains[sid] != nil
	sess.mu.Unlock()
	if !memberPresent || memberQoS != 0 {
		t.Fatalf("the fixture needs a PRESENT QoS 0 membership for sid %q, got present=%v qos=%d",
			sid, memberPresent, memberQoS)
	}
	if cc == nil || cc.Durable != dur {
		t.Fatalf("the fixture needs cons[%q] naming the adopted durable %q, got %v", sid, dur, cc)
	}
	if stillDraining {
		t.Fatalf("the adoption must have taken the drain out of sess.drains for sid %q", sid)
	}

	// Step 4, and WITHOUT it this fixture reaches nothing. The failed filter is skipped by
	// sess.update, so step 3 persisted nothing: the stored record still described step 2's
	// drain, and a cold restart would restore an ordinary drain and never reach the QoS 0
	// existing-consumer arm at all. The in-memory maps asserted above are not what the
	// restore reads.
	//
	// An ordinary SUBSCRIBE on an unrelated filter is what carries the adopted state into
	// the record: the builder serialises the WHOLE session, so it writes Subs at 0 for this
	// sid alongside the Cons entry the adoption put back.
	testMQTTSub(t, 4, conn.c, conn.r, []*mqttFilter{{filter: clientID + "/keepalive", qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	// The RECORD, which is the only thing the cold restore will see.
	ps := testPersistedRecord(t, sess)
	if q, ok := ps.Subs[sid]; !ok || q != 0 {
		t.Fatalf("the stored record must carry a PRESENT QoS 0 membership for %q, got present=%v qos=%d (subs=%v)",
			sid, ok, q, ps.Subs)
	}
	inCons, inDrains := testRecordNamesDurable(t, sess, dur)
	if !inCons || inDrains {
		t.Fatalf("the stored record must name durable %q in Cons and NOT in Drains — that pair is the "+
			"state under test — got inCons=%v inDrains=%v", dur, inCons, inDrains)
	}
	return conn, dur, sid, pi
}

// testRestartPreservingStore restarts the broker against the same JetStream StoreDir, which
// is what makes a restore COLD: the resident session object is gone and the next CONNECT
// must load the record.
func testRestartPreservingStore(t *testing.T, s *Server, o *Options) *Server {
	t.Helper()
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	return testMQTTRunServer(t, o)
}

// testAssertColdRestore fails unless the session was rebuilt from the record rather than
// found resident. pubRelVerified is false on every fresh restore by construction, and a
// resident object that has already served a connection has it set.
func testAssertColdRestore(t *testing.T, sess *mqttSession, prior *mqttSession) {
	t.Helper()
	if sess == prior {
		t.Fatal("the restore was not cold: the same resident session object was reused, so no record was loaded")
	}
}

// TestMQTTRestoreQoS0OverLiveConsumerCompletesQoS1Exchange is T1, the acceptance test.
//
// A cold restore of Subs 0 plus a live Cons entry whose durable holds an unacknowledged
// QoS 1 exchange must COMPLETE that exchange. [MQTT-3.1.2-4] requires a resumed session to
// resume its communications with its existing state, and [MQTT-4.4.0-1] requires the
// retransmission.
//
// The durable is checked BEFORE the exchange completes. Deleting it after the last claim
// is ordinary retirement, so a check made after the PUBACK would pass against a broker that
// destroyed the exchange outright.
func TestMQTTRestoreQoS0OverLiveConsumerCompletesQoS1Exchange(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const topic = "q0r/one/#"
	conn, dur, sid, pi := testQoS0OverLiveConsRecord(t, s, o, "q0r1", topic, 1)
	prior := testMQTTResidentSession(t, s, "q0r1")
	conn.c.Close()

	s = testRestartPreservingStore(t, s, o)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0r1"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	sess := testMQTTResidentSession(t, s, "q0r1")
	testAssertColdRestore(t, sess, prior)

	// The restore must have CONVERTED rather than deleted: the durable is alive and the
	// record names it as a drain.
	if !testDurableExists(t, s, dur) {
		t.Fatalf("the durable %q must survive the restore: deleting it deletes the message with it", dur)
	}
	sess.mu.Lock()
	d := sess.drains[sid]
	_, stillCons := sess.cons[sid]
	sess.mu.Unlock()
	if d == nil {
		t.Fatalf("the restore must convert the QoS 0 membership's consumer into a drain, drains=%v", sess.drains)
	}
	if stillCons {
		t.Fatal("a converted consumer must leave sess.cons: the drain owns the sid (I7)")
	}

	// And it retransmits, under the ORIGINAL identifier, at the exchange's own QoS —
	// which is the entry's, not the QoS 0 membership's.
	rpi := testMQTTCheckPubMsgNoAck(t, c, r, "q0r/one/x", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if rpi != pi {
		t.Fatalf("the retransmission must reuse identifier %d, got %d", pi, rpi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, rpi)
	testMQTTFlush(t, c, nil, r)
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess); n != 0 {
			return fmt.Errorf("the PUBACK must release the drain's claim, still %d", n)
		}
		return nil
	})
}

// TestMQTTRestoreQoS0OverLiveConsumerCompletesQoS2ExchangeThroughPubComp is T2.
//
// [MQTT-4.3.3-1] makes an in-flight QoS 2 message categorically a QoS 2 message, so the
// restored exchange must reach PUBCOMP even though the membership that survives is QoS 0.
func TestMQTTRestoreQoS0OverLiveConsumerCompletesQoS2ExchangeThroughPubComp(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const topic = "q0r/two/#"
	conn, dur, _, pi := testQoS0OverLiveConsRecord(t, s, o, "q0r2", topic, 2)
	prior := testMQTTResidentSession(t, s, "q0r2")
	conn.c.Close()

	s = testRestartPreservingStore(t, s, o)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0r2"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	sess := testMQTTResidentSession(t, s, "q0r2")
	testAssertColdRestore(t, sess, prior)

	if !testDurableExists(t, s, dur) {
		t.Fatalf("the durable %q must survive the restore", dur)
	}

	rpi := testMQTTCheckPubMsgNoAck(t, c, r, "q0r/two/x", mqttPubQoS2|mqttPubFlagDup, []byte("payload"))
	if rpi != pi {
		t.Fatalf("the retransmission must reuse identifier %d, got %d", pi, rpi)
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, rpi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, rpi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, rpi)
	testMQTTFlush(t, c, nil, r)
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess); n != 0 {
			return fmt.Errorf("PUBCOMP must release the drain's claim, still %d", n)
		}
		return nil
	})
}

// TestMQTTRestoreConvertedDrainIsPersistedAtQoSZero is T19.
//
// The record's drain carries QoS 0 — the membership's value, which is the honest one for a
// drain with no subscription — WHILE the exchange completes at its own QoS. The two are
// independent, and that independence is what a drain-wide derived maximum would destroy:
// such a scalar would write 2 here for a drain whose only exchange is QoS 1, and the send
// section would still transmit at the entry's QoS, so no wire assertion can witness it.
func TestMQTTRestoreConvertedDrainIsPersistedAtQoSZero(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const topic = "q0r/qz/#"
	conn, _, sid, pi := testQoS0OverLiveConsRecord(t, s, o, "q0rqz", topic, 2)
	prior := testMQTTResidentSession(t, s, "q0rqz")
	conn.c.Close()

	s = testRestartPreservingStore(t, s, o)
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rqz"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	sess := testMQTTResidentSession(t, s, "q0rqz")
	testAssertColdRestore(t, sess, prior)

	sess.mu.Lock()
	d := sess.drains[sid]
	sess.mu.Unlock()
	if d == nil {
		t.Fatal("the restore must have converted, or there is no drain to inspect")
	}
	// The STORED record, not d.persisted() in memory: a serialization helper called on the
	// live object would pass even if the record on the stream said something else, and the
	// record is what the next resume and every sibling read.
	ps := testPersistedRecord(t, sess)
	pd := ps.Drains[sid]
	if pd == nil {
		t.Fatalf("the stored record must name a drain under sid %q, got drains=%v", sid, ps.Drains)
	}
	if pd.QoS != 0 {
		t.Fatalf("a restore-converted drain must be PERSISTED at QoS 0 — a drain has no subscription "+
			"QoS, and an exchange's QoS lives on its own entry — got %d", pd.QoS)
	}

	// Independently: the QoS 2 exchange still completes as a QoS 2 exchange.
	rpi := testMQTTCheckPubMsgNoAck(t, c, r, "q0r/qz/x", mqttPubQoS2|mqttPubFlagDup, []byte("payload"))
	if rpi != pi {
		t.Fatalf("the retransmission must reuse identifier %d, got %d", pi, rpi)
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, rpi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, rpi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, rpi)
}

// TestMQTTRestoreQoS0NothingOwedPersistsTheReferenceRemovalBeforeDeleting is T11.
//
// With no started delivery there is nothing to preserve, so stageDrain refuses and the
// ordinary teardown stands. The boundary must still SAVE first: the record that carries the
// QoS 0 membership must not name a durable the same restore then deletes, or three recovery
// surfaces read it as live.
func TestMQTTRestoreQoS0NothingOwedPersistsTheReferenceRemovalBeforeDeleting(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const topic = "q0r/empty/#"
	conn, dur, sid, _ := testQoS0OverLiveConsRecord(t, s, o, "q0re", topic, 1)

	// Complete the exchange BEFORE the restart, so the durable holds nothing started.
	sess := testMQTTResidentSession(t, s, "q0re")
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, 1)
	testMQTTFlush(t, conn.c, nil, conn.r)
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if n := len(sess.cpending[dur]); n != 0 {
			return fmt.Errorf("the durable still has %d pending mappings", n)
		}
		return nil
	})
	prior := sess
	conn.c.Close()

	s = testRestartPreservingStore(t, s, o)
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0re"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	sess = testMQTTResidentSession(t, s, "q0re")
	testAssertColdRestore(t, sess, prior)

	// No drain, no cons entry, and the durable is deleted — the ordinary teardown.
	sess.mu.Lock()
	d, _ := sess.drains[sid], sess.cons[sid]
	_, stillCons := sess.cons[sid]
	sess.mu.Unlock()
	if d != nil {
		t.Fatal("nothing was started, so the restore must NOT convert: the teardown is correct here")
	}
	if stillCons {
		t.Fatalf("the reference to %q must leave sess.cons before the teardown", dur)
	}
	// THE oracle, and it is about the RECORD rather than the maps: the save that carries
	// the reference removal must have happened, or the stored record names a durable this
	// restore then deletes — and three recovery surfaces read such an entry as live. An
	// in-memory check passes either way, because the teardown removes the entry itself.
	inCons, inDrains := testRecordNamesDurable(t, sess, dur)
	if inCons || inDrains {
		t.Fatalf("the restore must persist the reference removal BEFORE deleting durable %q, "+
			"got inCons=%v inDrains=%v", dur, inCons, inDrains)
	}
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if testDurableExists(t, s, dur) {
			return fmt.Errorf("the durable %q is still there", dur)
		}
		return nil
	})
}

// TestMQTTRestoreDrainingNotAllowedRetainsAHeldConsumer is T6, and it is what stops the
// conversion from being unconditional.
//
// A stream-imposed ConsumerLimits.InactiveThreshold gives every consumer a delete timer it
// cannot opt out of, so a drain would be reaped with its started messages. The restore must
// then refuse the conversion — with a STARTED claim present, so that the eligibility guard is
// what is being tested rather than stageDrain's emptiness. Until F-105 7c-lc the fallback was the
// destructive teardown; it is now the downgrade's disposal decision, which waits for the
// obligation holding the consumer (renamed from ...StillTearsDown).
func TestMQTTRestoreDrainingNotAllowedRetainsAHeldConsumer(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const topic = "q0r/nodrain/#"
	conn, dur, sid, _ := testQoS0OverLiveConsRecord(t, s, o, "q0rnd", topic, 1)
	prior := testMQTTResidentSession(t, s, "q0rnd")
	conn.c.Close()

	// The limit goes on the messages stream, imposed out of band as a deployment would,
	// which is what drainingAllowed reads. Never skipped on failure: a skip would let this
	// test pass while proving nothing about the guard it exists for.
	si, lerr := prior.jsa.lookupStream(mqttStreamName)
	if lerr != nil || si == nil {
		t.Fatalf("lookupStream: %v", lerr)
	}
	cfg := si.Config
	cfg.ConsumerLimits.InactiveThreshold = time.Hour
	if _, uerr := prior.jsa.updateStream(&cfg); uerr != nil {
		t.Fatalf("could not impose a consumer inactivity limit on %q: %v", mqttStreamName, uerr)
	}

	s = testRestartPreservingStore(t, s, o)
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rnd"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	sess := testMQTTResidentSession(t, s, "q0rnd")
	testAssertColdRestore(t, sess, prior)

	sess.mu.Lock()
	d := sess.drains[sid]
	sess.mu.Unlock()
	if d != nil {
		t.Fatal("a stream inactivity limit forbids draining, so the restore must tear the consumer down " +
			"rather than convert it: a drain reaped with its messages is worse than the teardown")
	}
	// NOT TORN DOWN UNDER ITS STARTED DELIVERY (F-105 7c-lc). The restore still refuses to convert
	// to a drain, and the downgrade's disposal decision stands; but the started claim's obligation
	// holds the source consumer, and deleting a consumer an admitted obligation holds is the defect
	// F-105 removes. The downgrade's purge cancelled that obligation's register WITHOUT unsent
	// evidence (it was transmitted), so its exchange stays at Publish until the generation is
	// discarded — F-106's accepted cost — and its hold retains the consumer for as long. The
	// client-visible outcome is the baseline's (the started delivery is not redelivered); the
	// added cost is the retained consumer, stated in docs/CONFORMANCE.md.
	time.Sleep(2 * time.Second)
	if !testDurableExists(t, s, dur) {
		t.Fatalf("the durable %q was deleted while its started delivery's obligation still held it", dur)
	}
	if rr := sess.retirementPendingFor(dur); !rr {
		t.Fatalf("the downgrade's disposal of %q is not in the retirement owner's custody", dur)
	}
}

// TestMQTTDrainFilterSubjectComesFromTheDurable is T5.
//
// d.subject feeds isMQTTReservedSubscription, which asks about the FILTER. Deriving it from
// the delivery inbox — which is what both pre-existing stagers used to do — answers a
// different question and always answers "not reserved".
func TestMQTTDrainFilterSubjectComesFromTheDurable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		filter   string
		wantSubj string
		wantResv bool
	}{
		{"multi-level wildcard", ">", ">", true},
		{"single-level wildcard", "*", "*", true},
		{"level-up companion of a wildcard", "*.>", "*.>", true},
		{"ordinary filter", "foo.bar", "foo.bar", false},
		{"ordinary wildcard below a level", "foo.>", "foo.>", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc := &ConsumerConfig{FilterSubject: mqttStreamSubjectPrefix + tc.filter}
			got := mqttDrainFilterSubject(cc)
			if got != tc.wantSubj {
				t.Fatalf("filter subject: got %q want %q", got, tc.wantSubj)
			}
			if resv := isMQTTReservedSubscription(got); resv != tc.wantResv {
				t.Fatalf("reserved flag for %q: got %v want %v", got, resv, tc.wantResv)
			}
		})
	}
	// A nil config must not panic: the stagers call this before their own nil checks in
	// some orders, and returning empty is the same "not reserved" a missing filter means.
	if got := mqttDrainFilterSubject(nil); got != _EMPTY_ {
		t.Fatalf("a nil config must derive the empty subject, got %q", got)
	}
}

// TestMQTTResumedDrainInstallsTheReservedFlagFromTheFilter is T5b, and its oracle is the
// INSTALLED FLAG rather than traffic.
//
// Traffic cannot discriminate this. A fresh "$"-topic sequence on a committed drain is
// discarded in the drain block before reserved filtering ever runs, and a legitimate "#"
// subscription never transmitted a "$"-topic in the first place, so "no $ delivery" passes
// just as well with the flag broken.
//
// It covers the UNSUBSCRIBE stager, which is one of the two that carried the defect.
func TestMQTTResumedDrainInstallsTheReservedFlagFromTheFilter(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// "#" is the reserved-matching filter: its NATS form is ">". The shared testDrainSetup
	// cannot be used, because it publishes to the filter string and "#" is illegal in a
	// PUBLISH topic ([MQTT-3.3.2-2]).
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rresv"}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "#", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rresvpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "resv/x", 1, []byte("payload"))
	testMQTTCheckPubMsgNoAck(t, c, r, "resv/x", mqttPubQos1, []byte("payload"))

	conn := &mqttTestConn{c: c, r: r}
	sess := testMQTTResidentSession(t, s, "q0rresv")
	defer conn.c.Close()

	// UNSUBSCRIBE stages the drain, keeping the started exchange.
	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: "#"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	var d *mqttDrain
	for _, cand := range sess.drainByDur {
		d = cand
	}
	var subj string
	if d != nil {
		subj = d.subject
	}
	sess.mu.Unlock()
	if d == nil {
		t.Fatal("the UNSUBSCRIBE must have staged a drain, or there is nothing to inspect")
	}
	if subj != ">" {
		t.Fatalf("a staged drain's subject must be its durable's FILTER subject %q, got %q — "+
			"deriving it from the delivery inbox is what cleared the reserved flag", ">", subj)
	}
	if !isMQTTReservedSubscription(subj) {
		t.Fatalf("the drain of a %q filter must be reserved-marked, subject %q", "#", subj)
	}

	// And the flag actually reaches the installed subscription that restoreDrains builds.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rresv"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		cur := sess.drainByDur[d.cc.Durable]
		if cur == nil {
			return fmt.Errorf("the drain is gone")
		}
		cl := sess.c
		if cl == nil {
			return fmt.Errorf("no client bound yet")
		}
		cl.mu.Lock()
		sub := cl.subs[cur.cc.DeliverSubject]
		cl.mu.Unlock()
		if sub == nil || sub.mqtt == nil {
			return fmt.Errorf("restoreDrains has not installed the delivery subscription yet")
		}
		if !sub.mqtt.reserved {
			return fmt.Errorf("the restored drain's subscription must carry the reserved flag " +
				"([MQTT-4.7.2-1]); it is what stops a $-topic being delivered to a wildcard")
		}
		return nil
	})
}

// testMQTTReadAnyPubMsgNoAck reads the next PUBLISH whatever its topic, identifier or
// payload, for fixtures where more than one exchange is outstanding and JetStream chooses
// the order. Asserting a fixed order there would be asserting the scheduler.
func testMQTTReadAnyPubMsgNoAck(t *testing.T, r *mqttReader) (pi uint16, flags byte, payload []byte) {
	t.Helper()
	f, p, _, pl := testMQTTGetPubMsgEx(t, nil, r, _EMPTY_, nil)
	return p, f, pl
}

// TestMQTTRestoreMixedQoSDrainRetransmitsEachAtItsOwnQoS is T4.
//
// One converted drain holds a QoS 1 and a QoS 2 exchange at once — a QoS 2 grant delivers
// min(published, granted), so publishing at each QoS puts both on the same durable. Each
// must go out at ITS OWN QoS, which is the entry's, not any per-drain value.
func TestMQTTRestoreMixedQoSDrainRetransmitsEachAtItsOwnQoS(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const filter = "q0r/mix/#"
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rmix"}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: filter, qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rmixpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "q0r/mix/a", 1, []byte("one"))
	pi1 := testMQTTCheckPubMsgNoAck(t, c, r, "q0r/mix/a", mqttPubQos1, []byte("one"))
	testMQTTPublish(t, pc, pr, 2, false, false, "q0r/mix/b", 2, []byte("two"))
	pi2 := testMQTTCheckPubMsgNoAck(t, c, r, "q0r/mix/b", mqttPubQoS2, []byte("two"))

	conn := &mqttTestConn{c: c, r: r}
	sess := testMQTTResidentSession(t, s, "q0rmix")
	conv, cerr := mqttToNATSSubjectConversion([]byte(filter), true)
	if cerr != nil {
		t.Fatalf("converting %q: %v", filter, cerr)
	}
	sid := string(conv)
	sess.mu.Lock()
	cc0 := sess.cons[sid]
	sess.mu.Unlock()
	if cc0 == nil {
		t.Fatalf("no consumer for sid %q: the fixture never reached its route", sid)
	}
	dur := cc0.Durable

	// Drive the same failed-adoption schedule as the shared helper, inline because this
	// fixture needed two publishes at different QoS before it.
	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: filter, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)
	asm := testMQTTGetAccountSessionManager(t, s, "q0rmix")
	fwcsid := sid[:len(sid)-2] + mqttMultiLevelSidSuffix
	asm.mu.Lock()
	asm.testSubFilterFail = func(exit, failSid string) error {
		if exit != "consumer-fwc" || failSid != fwcsid {
			return nil
		}
		return fmt.Errorf("test: the level-up companion fails after the main half adopted")
	}
	asm.mu.Unlock()
	testMQTTSendSubOnly(t, 3, conn.c, []*mqttFilter{{filter: filter, qos: 1}})
	if codes := testMQTTReadSubAckTolerantOfPubs(t, conn.c, conn.r, 3); len(codes) != 1 || codes[0] != mqttSubAckFailure {
		t.Fatalf("the level-up failure must fail the whole filter, got %v", codes)
	}
	asm.mu.Lock()
	asm.testSubFilterFail = nil
	asm.mu.Unlock()
	testMQTTSub(t, 4, conn.c, conn.r, []*mqttFilter{{filter: "q0rmix/keepalive", qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if inCons, inDrains := testRecordNamesDurable(t, sess, dur); !inCons || inDrains {
		t.Fatalf("the fixture needs the durable in Cons and not Drains, got inCons=%v inDrains=%v", inCons, inDrains)
	}
	prior := sess
	conn.c.Close()

	s = testRestartPreservingStore(t, s, o)
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rmix"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	sess = testMQTTResidentSession(t, s, "q0rmix")
	testAssertColdRestore(t, sess, prior)

	// Both come back, EACH at its own QoS. Order is JetStream's, so accept either and key
	// the expected flags off the identifier.
	for range 2 {
		pi, flags, payload := testMQTTReadAnyPubMsgNoAck(t, r2)
		switch pi {
		case pi1:
			if flags&mqttPubQoS2 == mqttPubQoS2 {
				t.Fatalf("the QoS 1 exchange %d must retransmit at QoS 1, got flags 0x%x", pi, flags)
			}
			testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
		case pi2:
			if flags&mqttPubQoS2 != mqttPubQoS2 {
				t.Fatalf("the QoS 2 exchange %d must retransmit at QoS 2, got flags 0x%x — a drain-wide "+
					"maximum or a membership-derived QoS is what gets this wrong", pi, flags)
			}
			testMQTTSendPIPacket(mqttPacketPubRec, t, c2, pi)
			testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
			testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
		default:
			t.Fatalf("unexpected identifier %d (payload %q); want %d or %d", pi, payload, pi1, pi2)
		}
	}
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess); n != 0 {
			return fmt.Errorf("both exchanges completed, so the drain must hold no claim, still %d", n)
		}
		return nil
	})
}

// testMQTTExpectRefusedResume fails unless the CONNECT was refused. A pre-CONNACK refusal
// reaches the client either as a failure CONNACK or as a closed connection, and which one
// depends on how far the refusal got; what must never appear is a SUCCESS CONNACK, because
// that is the broker telling the client its session is usable.
//
// It reads the socket directly rather than through mqttReader, whose helpers turn a closed
// connection into t.Fatal — and a closed connection is one of the two correct answers here.
func testMQTTExpectRefusedResume(t *testing.T, c net.Conn) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(testMQTTTimeout)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	var buf [64]byte
	n, err := c.Read(buf[:])
	if err != nil {
		return // closed, or nothing sent: a refusal
	}
	if n < 4 || buf[0]&mqttPacketMask != mqttPacketConnectAck {
		return // not a CONNACK at all
	}
	if buf[3] == mqttConnAckRCConnectionAccepted {
		t.Fatal("the resume must be REFUSED: a success CONNACK tells the client the session is usable")
	}
}

// TestMQTTRestoreDefiniteSaveFailureRestoresTheConsumerAndRefusesTheConnect is T7.
//
// A DEFINITE failure of the boundary's save means nothing was stored, so everything this
// boundary did in memory must be undone inside the still-open transaction — or the resident
// session serves under state the record does not have — and the CONNECT must be refused.
//
// The durable is the thing that must NOT be touched: it holds the client's exchange, and a
// restore that could not establish what it owes must leave it exactly as it found it.
func TestMQTTRestoreDefiniteSaveFailureRestoresTheConsumerAndRefusesTheConnect(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const filter = "q0r/deffail/#"
	conn, dur, sid, _ := testQoS0OverLiveConsRecord(t, s, o, "q0rdf", filter, 1)
	conn.c.Close()

	s = testRestartPreservingStore(t, s, o)

	// Fail the BOUNDARY's save, and only it. persistRecord is the single store boundary for
	// a session record, and the accepting CONNECT's own save runs first — failing that one
	// instead would refuse the resume before the boundary ever ran, so the test would prove
	// nothing about the rollback it names.
	var saves atomic.Int32
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
		if saves.Add(1) <= 1 {
			return nil // the accepting CONNECT's save
		}
		return errors.New("test: the restore-side conversion's save is refused")
	})
	defer restore()

	// WithError, because a refusal that closes the connection is one of the two correct
	// answers and testMQTTConnect turns that into t.Fatal.
	c, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "q0rdf"}, o.MQTT.Host, o.MQTT.Port, 0)
	if c != nil {
		defer c.Close()
	}
	if cerr == nil {
		// It answered: then the answer must be a failure CONNACK, never a success one.
		testMQTTExpectRefusedResume(t, c)
	}

	restore()

	// The durable is untouched, which is the point: the exchange is still the client's.
	if !testDurableExists(t, s, dur) {
		t.Fatalf("a definite save failure must not delete durable %q: nothing was stored, so "+
			"nothing authorised a teardown", dur)
	}
	// And the record still describes the pre-restore state.
	sess := testMQTTResidentSession(t, s, "q0rdf")
	inCons, inDrains := testRecordNamesDurable(t, sess, dur)
	if !inCons || inDrains {
		t.Fatalf("nothing was stored, so the record must still name %q in Cons and not in Drains, "+
			"got inCons=%v inDrains=%v", dur, inCons, inDrains)
	}
	// The resident maps were rolled back to match it: the consumer entry is back and no
	// drain survives. Without the un-convert and the cons restore the session would hold a
	// drain the record knows nothing about.
	sess.mu.Lock()
	cc := sess.cons[sid]
	d := sess.drains[sid]
	sess.mu.Unlock()
	if d != nil {
		t.Fatalf("the converted drain must be un-converted on a definite failure, sid %q", sid)
	}
	if cc == nil || cc.Durable != dur {
		t.Fatalf("the consumer entry for sid %q must be restored naming %q, got %v", sid, dur, cc)
	}
}

// TestMQTTRestoreLaterFailureDoesNotRollBackAConfirmedConversion is T20.
//
// The conversion is a fact about the RECORD, not about this connection. Once it is durable,
// a later step refusing the CONNECT must leave it standing — the client's exchanges are owed
// either way, and the next resume picks the drain up from Drains.
//
// It drives the requested-lifetime save, which the roadmap bullet's own analysis said did
// not exist: applyRequestedLifetime runs AFTER processSubs and saves whenever the terms
// moved, so a client reconnecting with a different Session Expiry Interval reaches it.
func TestMQTTRestoreLaterFailureDoesNotRollBackAConfirmedConversion(t *testing.T) {
	o := testMQTTDefaultOptions()
	// v5 must be ENABLED, or the CONNECT below is refused for being v5 at all and this test
	// passes without ever reaching the exit it names.
	o.MQTT.MaxProtocolVersion = 5
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const filter = "q0r/later/#"
	conn, dur, sid, _ := testQoS0OverLiveConsRecord(t, s, o, "q0rlate", filter, 1)
	conn.c.Close()

	s = testRestartPreservingStore(t, s, o)

	// Let the conversion's save land, then refuse the NEXT session-record write, which on
	// this connection is the lifetime grant.
	// Saves 1 (accept) and 2 (the conversion) land; the NEXT one is refused, and on this
	// connection that is the lifetime grant.
	var saves atomic.Int32
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
		if saves.Add(1) <= 2 {
			return nil
		}
		return errors.New("test: a later restore-path save is refused")
	})
	defer restore()

	// v5 with a Session Expiry Interval, so applyRequestedLifetime sees terms that MOVED and
	// therefore saves. It returns without writing when they are unchanged, which is why an
	// ordinary reconnect cannot drive this exit.
	c, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "q0rlate",
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTExpectRefusedResume(t, c)
	restore()

	// The refusal must have come from the LATER save, not from something upstream: three
	// session-record writes means accept, conversion, and the lifetime grant that failed.
	// Without this the test would pass if the CONNECT were refused for any other reason.
	if n := saves.Load(); n < 3 {
		t.Fatalf("the fixture must reach the requested-lifetime save: only %d session-record "+
			"writes were attempted, so the refusal came from somewhere else", n)
	}

	// The refusal did NOT undo the conversion: the durable lives and the record names it as
	// a drain. Deliberately not asserting that a later resume completes the exchange — a
	// subsequent authoritative transition (expiry, a granted zero lifetime, a sibling
	// teardown) may legitimately intervene, and claiming otherwise would overstate this.
	if !testDurableExists(t, s, dur) {
		t.Fatalf("a later refusal must not delete the converted durable %q", dur)
	}
	sess := testMQTTResidentSession(t, s, "q0rlate")
	inCons, inDrains := testRecordNamesDurable(t, sess, dur)
	if inCons || !inDrains {
		t.Fatalf("the confirmed conversion must stand after a later refusal: the record must name "+
			"%q in Drains and not in Cons, got inCons=%v inDrains=%v (sid %q)", dur, inCons, inDrains, sid)
	}
	// And the RESIDENT session must agree with it. A refused CONNECT does not evict a
	// persistent session, so an un-convert here would leave memory holding a cons entry
	// while the record says Drains — and the next save would write that divergence back,
	// resurrecting the very Subs-0-over-a-live-Cons state this item removes.
	sess.mu.Lock()
	d := sess.drains[sid]
	_, backInCons := sess.cons[sid]
	sess.mu.Unlock()
	if d == nil || backInCons {
		t.Fatalf("the resident session must still hold the drain for sid %q and no cons entry, "+
			"got drain=%v inCons=%v — rolling a CONFIRMED conversion back diverges memory from "+
			"the record", sid, d != nil, backInCons)
	}
}

// TestMQTTNoLocalDoesNotAckAwayAClaimedDrainedExchange is T17.
//
// The No Local arm ACKNOWLEDGES the source and returns. Applying it to a claimed drained
// sequence therefore does not merely withhold a retransmission — it destroys the message
// while leaving the drain's claim standing.
//
// This arm is the PRE-EXISTING route, and it needs no restore at all: a live re-SUBSCRIBE
// turns No Local on ([MQTT-3.8.4-3] replaces the subscription), and a later UNSUBSCRIBE
// stages a drain that copies the NEW option over an exchange started under the old one.
// Fixing only the restore-converted drain's field would leave exactly this standing.
func TestMQTTNoLocalDoesNotAckAwayAClaimedDrainedExchange(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.MaxProtocolVersion = 5 // No Local is a v5 subscription option
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "q0r/nolocal"
	// One v5 connection both subscribes and publishes, because No Local is about the
	// PUBLISHING client's identity.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "q0rnl"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttReasonSuccess {
		t.Fatalf("CONNACK reason %x", rc)
	}

	// Granted WITHOUT No Local, so the self-published message is delivered.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 0x01}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	// The v5 form: a 3.1.1 PUBLISH has no properties field and a v5 connection rejects it
	// as malformed.
	testMQTTPublishV5(t, c, 1, 10, topic, []byte("mine"))
	testMQTTReadPIPacket(mqttPacketPubAck, t, r, 10)
	_, pi := testMQTTReadPublishV5(t, r, topic, []byte("mine"))

	sess := testMQTTResidentSession(t, s, "q0rnl")
	conv, _ := mqttToNATSSubjectConversion([]byte(topic), false)
	sid := string(conv)
	sess.mu.Lock()
	cc := sess.cons[sid]
	sess.mu.Unlock()
	if cc == nil {
		t.Fatalf("no consumer for sid %q: the fixture never reached its route", sid)
	}
	dur := cc.Durable

	// The replacement turns No Local ON (option bit 0x04), while the exchange above is
	// still outstanding.
	if codes := testMQTTSubV5(t, c, r, 2, []mqttV5SubFilter{{topic: topic, opts: 0x01 | 0x04}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	// UNSUBSCRIBE stages a drain, which copies the CURRENT option — No Local — over an
	// exchange the client is already holding under a grant that did not set it.
	testMQTTUnsubV5(t, c, r, 3, []string{topic})
	testMQTTFlush(t, c, nil, r)

	sess.mu.Lock()
	d := sess.drainByDur[dur]
	claims := testDrainClaimsLocked(sess)
	sess.mu.Unlock()
	if d == nil {
		t.Fatalf("the UNSUBSCRIBE must have staged a drain for durable %q", dur)
	}
	if claims != 1 {
		t.Fatalf("the drain must claim the started exchange, got %d claims", claims)
	}

	// The redelivery must reach the client, and the exchange must complete. If No Local were
	// applied, the source would be acked away and this would time out with the claim stuck.
	// DUP is deliberately NOT the oracle: it is derived from JetStream's delivery count, so
	// asserting it here would pin the scheduler rather than the rule. The identifier and the
	// completion are what matter.
	_, rpi := testMQTTReadPublishV5(t, r, topic, []byte("mine"))
	if rpi != pi {
		t.Fatalf("the drained retransmission must reuse identifier %d, got %d", pi, rpi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, rpi)
	testMQTTFlush(t, c, nil, r)
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess); n != 0 {
			return fmt.Errorf("the PUBACK must release the drain's claim, still %d", n)
		}
		return nil
	})
}

// TestMQTTRestoreLevelUpCompanionConvertsWithItsParent is T10.
//
// A wildcard owns two sids. Where BOTH carry a QoS 0 membership over a live consumer, both
// must convert: a per-sid decision that converted one and destroyed the other would lose
// half the client's exchanges, and the filter is the atomicity boundary everywhere else.
func TestMQTTRestoreLevelUpCompanionConvertsWithItsParent(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const filter = "q0r/both/#"
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rboth"}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: filter, qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rbothpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	// One below the wildcard (the main sid) and one AT its parent level (the level-up
	// companion), so both consumers hold a started exchange.
	testMQTTPublish(t, pc, pr, 1, false, false, "q0r/both/x", 1, []byte("below"))
	piMain := testMQTTCheckPubMsgNoAck(t, c, r, "q0r/both/x", mqttPubQos1, []byte("below"))
	testMQTTPublish(t, pc, pr, 1, false, false, "q0r/both", 2, []byte("parent"))
	piFwc := testMQTTCheckPubMsgNoAck(t, c, r, "q0r/both", mqttPubQos1, []byte("parent"))

	sess := testMQTTResidentSession(t, s, "q0rboth")
	conv, _ := mqttToNATSSubjectConversion([]byte(filter), true)
	sid := string(conv)
	fwcsid := sid[:len(sid)-2] + mqttMultiLevelSidSuffix
	sess.mu.Lock()
	ccMain, ccFwc := sess.cons[sid], sess.cons[fwcsid]
	sess.mu.Unlock()
	if ccMain == nil || ccFwc == nil {
		t.Fatalf("both halves must have consumers, got main=%v fwc=%v", ccMain, ccFwc)
	}
	durMain, durFwc := ccMain.Durable, ccFwc.Durable

	// Downgrade converts BOTH into drains, then a failing re-SUBSCRIBE adopts both back.
	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: filter, qos: 0}}, []byte{0})
	testMQTTFlush(t, c, nil, r)
	asm := testMQTTGetAccountSessionManager(t, s, "q0rboth")
	asm.mu.Lock()
	asm.testSubFilterFail = func(exit, failSid string) error {
		if exit == "consumer-fwc" && failSid == fwcsid {
			return fmt.Errorf("test: the level-up companion fails after both halves adopted")
		}
		return nil
	}
	asm.mu.Unlock()
	testMQTTSendSubOnly(t, 3, c, []*mqttFilter{{filter: filter, qos: 1}})
	if codes := testMQTTReadSubAckTolerantOfPubs(t, c, r, 3); len(codes) != 1 || codes[0] != mqttSubAckFailure {
		t.Fatalf("the level-up failure must fail the whole filter, got %v", codes)
	}
	asm.mu.Lock()
	asm.testSubFilterFail = nil
	asm.mu.Unlock()
	testMQTTSub(t, 4, c, r, []*mqttFilter{{filter: "q0rboth/keepalive", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	for _, d := range []string{durMain, durFwc} {
		if inCons, inDrains := testRecordNamesDurable(t, sess, d); !inCons || inDrains {
			t.Fatalf("the fixture needs durable %q in Cons and not Drains, got inCons=%v inDrains=%v",
				d, inCons, inDrains)
		}
	}
	prior := sess
	c.Close()

	s = testRestartPreservingStore(t, s, o)
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rboth"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	sess = testMQTTResidentSession(t, s, "q0rboth")
	testAssertColdRestore(t, sess, prior)

	// BOTH durables survive as drains.
	sess.mu.Lock()
	dMain, dFwc := sess.drains[sid], sess.drains[fwcsid]
	sess.mu.Unlock()
	if dMain == nil || dFwc == nil {
		t.Fatalf("both halves of the wildcard must convert, got main=%v fwc=%v", dMain != nil, dFwc != nil)
	}
	for _, d := range []string{durMain, durFwc} {
		if !testDurableExists(t, s, d) {
			t.Fatalf("durable %q must survive the restore", d)
		}
	}

	// And both exchanges complete.
	seen := map[uint16]bool{}
	for range 2 {
		pi, _, payload := testMQTTReadAnyPubMsgNoAck(t, r2)
		if pi != piMain && pi != piFwc {
			t.Fatalf("unexpected identifier %d (payload %q); want %d or %d", pi, payload, piMain, piFwc)
		}
		seen[pi] = true
		testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
	}
	if len(seen) != 2 {
		t.Fatalf("both exchanges must retransmit, saw %v", seen)
	}
}

// Scheduling evidence disappears at PUBACK, but §6 now retains the terminal
// register until source completion. A refused claim-removal save must still
// withhold the source, claim and durable; the terminal carrier prevents replay.
func TestMQTTRestoreMissingExchangeEvidenceWithholdsWithoutSettling(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const filter = "q0r/noev/#"
	conn, dur, sid, _ := testQoS0OverLiveConsRecord(t, s, o, "q0rnoev", filter, 1)
	conn.c.Close()
	s = testRestartPreservingStore(t, s, o)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rnoev"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	sess := testMQTTResidentSession(t, s, "q0rnoev")

	rpi := testMQTTCheckPubMsgNoAck(t, c, r, "q0r/noev/x", mqttPubQos1|mqttPubFlagDup, []byte("payload"))

	// The save fails definitely after protocol advancement. The scheduling entry
	// is gone, but the terminal source carrier must remain.
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
		return errors.New("test: the drained completion's claim-removal save is refused")
	})
	defer restore()

	testMQTTSendPIPacket(mqttPacketPubAck, t, c, rpi)
	testMQTTFlush(t, c, nil, r)

	// The claim survives the failed save. Without this the rest of the test would be
	// asserting things about a state it never reached.
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess); n != 1 {
			return fmt.Errorf("the failed save must leave the claim standing, got %d", n)
		}
		sess.mu.Lock()
		known := sess.claimedExchangeQoSKnownLocked(dur, testDrainOnlySeqLocked(sess, dur))
		sess.mu.Unlock()
		if known {
			return fmt.Errorf("the exchange evidence is still present, so C2's route is not reached")
		}
		return nil
	})

	testDrainRetainedTerminalSource(t, sess, dur, rpi)
	// Now let AckWait re-offer it. Nothing may be settled by that redelivery.
	beforeExists := testDurableExists(t, s, dur)
	time.Sleep(1500 * time.Millisecond)

	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("the withheld redelivery must NOT clear the claim, got %d claims", n)
	}
	if testDurableExists(t, s, dur) != beforeExists {
		t.Fatalf("the withheld redelivery must not delete durable %q", dur)
	}
	sess.mu.Lock()
	d := sess.drains[sid]
	sess.mu.Unlock()
	if d == nil {
		t.Fatalf("the drain for sid %q must still be there", sid)
	}
	// And nothing went to the client: a retransmission at an invented QoS is the defect.
	if err := c.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	var buf [64]byte
	if n, rerr := c.Read(buf[:]); rerr == nil && n > 0 && buf[0]&mqttPacketMask == mqttPacketPub {
		t.Fatalf("a claimed sequence with no exchange QoS must be WITHHELD, not retransmitted; "+
			"got a PUBLISH (%d bytes)", n)
	}
	// The decisive oracle, and the one that separates WITHHOLDING from SETTLING: the source
	// message must still be unacknowledged on the durable. Acknowledging it would consume it
	// from an interest-retained stream — destroying a message whose exchange this broker
	// cannot prove complete — while leaving the claim standing, which is strictly worse than
	// the retention the residual accepts.
	if ap := testConsumerAckPending(t, s, dur); ap == 0 {
		t.Fatalf("the withheld redelivery must leave the source UNACKNOWLEDGED on durable %q: "+
			"ack_pending is 0, so it was consumed", dur)
	}
	testDrainRetainedTerminalSource(t, sess, dur, rpi)
}

// Check the durable premise independently of the missing scheduling entry.
func testDrainRetainedTerminalSource(t *testing.T, sess *mqttSession, dur string, pi uint16) {
	t.Helper()
	// The acknowledgement's terminal advance lands after the handler returns, so the carrier is
	// awaited — the SAME predicate, never a weaker one: an exact terminal pair, the retained
	// source carrier and its installed suppression, all at once.
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		sess.mu.Lock()
		subject := mqttInflightSubject(sess.idHash, sess.gen, pi)
		held := false
		for key := range sess.terminalSources {
			held = held || (key.subject == subject && key.dur == dur)
		}
		sess.mu.Unlock()
		r, smsg, err := sess.asm.loadInflightRecord(subject)
		if err != nil || r == nil || smsg == nil || r.Dur != dur ||
			!mqttTerminalSourceCarrier(r) || !held {
			return fmt.Errorf("missing terminal source carrier/suppression: record=%+v err=%v held=%v", r, err, held)
		}
		ref := mqttSlotRefFromRecord(subject, smsg.Sequence, r)
		if ref == nil || !sess.terminalSourcePair(*ref, r) {
			return fmt.Errorf("the source carrier must have its exact terminal exchange")
		}
		return nil
	})
}

// testConsumerAckPending reports how many messages the durable has delivered and not had
// acknowledged. Zero after a withhold means the source was consumed, which a withhold must
// never do.
func testConsumerAckPending(t *testing.T, s *Server, durable string) int {
	t.Helper()
	js, err := s.nrc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	ci, err := js.ConsumerInfo(mqttStreamName, durable)
	if err != nil {
		t.Fatalf("ConsumerInfo(%q): %v", durable, err)
	}
	return ci.NumAckPending
}

// testDrainOnlySeqLocked returns the single claimed sequence of the named durable's drain,
// for fixtures that hold exactly one. sess.mu must be held.
func testDrainOnlySeqLocked(sess *mqttSession, dur string) uint64 {
	d := sess.drainByDur[dur]
	if d == nil {
		return 0
	}
	for seq, st := range d.seqs {
		if st != nil && st.claimed {
			return seq
		}
	}
	return 0
}

// §6 replaces the old deleted-register/retransmission contract for an
// acknowledged ordinary exchange. Positive subscription QoS cannot resurrect
// that exchange while its drain's claim-removal save is refused.
func TestMQTTPositiveQoSDrainRetainsTerminalSourceUntilClaimRemoval(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "q0r/posqos"
	conn, sess, pi := testDrainSetup(t, s, o, "q0rpos", topic, 1)
	defer conn.c.Close()

	// UNSUBSCRIBE stages a drain that keeps the SUBSCRIPTION's QoS 1.
	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	sess.mu.Lock()
	var dur string
	for d := range sess.drainByDur {
		dur = d
	}
	sess.mu.Unlock()
	if dur == _EMPTY_ {
		t.Fatal("the UNSUBSCRIBE must have staged a drain")
	}

	restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
		return errors.New("test: the drained completion's claim-removal save is refused")
	})
	defer restore()

	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)
	checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess); n != 1 {
			return fmt.Errorf("the failed save must leave the claim standing, got %d", n)
		}
		sess.mu.Lock()
		known := sess.claimedExchangeQoSKnownLocked(dur, testDrainOnlySeqLocked(sess, dur))
		sess.mu.Unlock()
		if known {
			return fmt.Errorf("the exchange evidence is still present, so this is not the state under test")
		}
		return nil
	})

	testDrainRetainedTerminalSource(t, sess, dur, pi)
	testMQTTExpectNothingFor(t, conn.r, 1500*time.Millisecond)
	if ap := testConsumerAckPending(t, s, dur); ap == 0 {
		t.Fatal("a refused claim removal must leave the source unacknowledged")
	}
	testDrainRetainedTerminalSource(t, sess, dur, pi)

	// Healthy storage must complete the installed owner without another MQTT ACK.
	restore()
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if testDrainClaims(sess) != 0 {
			return errors.New("claim removal has not converged")
		}
		sess.mu.Lock()
		subject := mqttInflightSubject(sess.idHash, sess.gen, pi)
		sess.mu.Unlock()
		_, smsg, err := sess.asm.loadInflightRecord(subject)
		if err != nil {
			return err
		}
		if smsg != nil {
			return errors.New("terminal source carrier has not been released")
		}
		return nil
	})
}

// TestMQTTRestoreRecordNamesTheDurableContinuously is T9.
//
// The protection against a sibling's orphan sweep is that the record names the durable at
// every point: in Cons before the conversion, in Drains after it, never in neither. The
// sweep spares only sessions resident on the SWEEPING broker and keys eligibility on the
// consumer's creation time, so adoption starts no new grace and a gap is a deletion.
//
// The oracle is the COMMITTED BODY, not merely "not named in neither": converting after the
// save would still leave the durable named in Cons at every sample, which the weaker
// wording accepts.
func TestMQTTRestoreRecordNamesTheDurableContinuously(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const filter = "q0r/naming/#"
	conn, dur, sid, _ := testQoS0OverLiveConsRecord(t, s, o, "q0rname", filter, 1)
	conn.c.Close()
	s = testRestartPreservingStore(t, s, o)

	// Sampled from INSIDE the conversion's own critical section, which is the only point at
	// which "the record before this save" is guaranteed observable. Nothing here calls
	// t.Fatal: it runs on a broker goroutine.
	var sawCons, sawNeither atomic.Bool
	var sampled atomic.Int32
	var hookASM atomic.Pointer[mqttAccountSessionManager]
	restoreHook := setMQTTTestSubPause(func(tag string) {
		if tag != "converted:"+sid || sampled.Add(1) != 1 {
			return
		}
		sess := testMQTTResidentSessionOrNil(hookASM.Load(), "q0rname")
		if sess == nil {
			return
		}
		inCons, inDrains := testRecordNamesDurableNoFatal(sess, dur)
		sawCons.Store(inCons)
		sawNeither.Store(!inCons && !inDrains)
	})
	defer restoreHook()

	// A throwaway connection just to reach the account's session manager, which the hook
	// needs and which only exists once some client has bound.
	pc, prr := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rnameprobe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, prr, mqttConnAckRCConnectionAccepted, false)
	hookASM.Store(testMQTTGetAccountSessionManager(t, s, "q0rnameprobe"))
	pc.Close()

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q0rname"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	sess := testMQTTResidentSession(t, s, "q0rname")
	restoreHook()

	// The COMMITTED BODY first, because it is the oracle that distinguishes a conversion
	// inside the boundary's critical section from one that runs after the save: the latter
	// leaves the durable named in Cons at every sample, which the "never in neither" check
	// alone would accept.
	inCons, inDrains := testRecordNamesDurable(t, sess, dur)
	if inCons || !inDrains {
		t.Fatalf("the committed record must name %q in Drains and not in Cons — a conversion that "+
			"runs after its save commits the pre-conversion body — got inCons=%v inDrains=%v (sid %q)",
			dur, inCons, inDrains, sid)
	}
	if sampled.Load() == 0 {
		t.Fatal("the conversion hook never fired: the fixture did not reach the boundary it names")
	}
	if sawNeither.Load() {
		t.Fatalf("the record named durable %q in NEITHER map during the conversion: a sibling "+
			"orphan sweep deletes exactly that state", dur)
	}
	if !sawCons.Load() {
		t.Fatalf("before the boundary's save the record must still name %q in Cons", dur)
	}
}

// testRecordNamesDurableNoFatal is testRecordNamesDurable without t, for the same reason.
func testRecordNamesDurableNoFatal(sess *mqttSession, durable string) (inCons, inDrains bool) {
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	smsg, err := sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil || smsg == nil {
		return false, false
	}
	ps := &mqttPersistedSession{}
	if json.Unmarshal(smsg.Data, ps) != nil {
		return false, false
	}
	for _, cc := range ps.Cons {
		if cc != nil && cc.Durable == durable {
			inCons = true
		}
	}
	for _, pd := range ps.Drains {
		if pd != nil && pd.Cons != nil && pd.Cons.Durable == durable {
			inDrains = true
		}
	}
	return inCons, inDrains
}

// TestMQTTRestoreAmbiguousLandedSaveKeepsTheStagedDrain is T8.
//
// The boundary's write LANDS and its acknowledgement is lost. Nothing may be committed,
// torn down or deleted on that outcome: the drain stays STAGED, which keeps its durable and
// leaves it neither retired nor reaped, so whichever record turns out to be durable no
// exchange is lost. The CONNECT is refused, because a restore that cannot prove what it owes
// must not serve.
func TestMQTTRestoreAmbiguousLandedSaveKeepsTheStagedDrain(t *testing.T) {
	o := testMQTTDefaultOptions()
	// Short, because the resume below needs the source RE-OFFERED: the refused connection
	// already took its delivery, so the retransmission waits out AckWait rather than
	// arriving at once the way a first post-restart offer does.
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	// A pre-CONNACK refusal closes without a readLoop resolution, so the PERIODIC pass is
	// what settles the attempt. Shortened so the resume below meets a resolved session
	// rather than an unresolved one. Process-global, so this test does not call t.Parallel().
	restoreSweep := setMQTTSessionExpirySweepInterval(250 * time.Millisecond)
	defer restoreSweep()

	const filter = "q0r/amb/#"
	conn, dur, sid, pi := testQoS0OverLiveConsRecord(t, s, o, "q0ramb", filter, 1)
	conn.c.Close()
	s = testRestartPreservingStore(t, s, o)

	// Lose the response to the boundary's session-record write. The accepting CONNECT's own
	// save is now the outbound-ownership metadata commit (F-105 cutover), which commits through
	// commitMetadataAttempt and never reaches this store-result seam, so the boundary save is
	// the FIRST (and only) write this hook sees. The write itself lands, which is what makes the
	// outcome ambiguous rather than failed.
	var seen atomic.Int32
	var heldStaged atomic.Bool
	var sampled atomic.Bool
	restore := setMQTTTestSessStoreResultHook(
		func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if err != nil {
				return resp, err
			}
			if seen.Add(1) != 1 {
				return resp, err
			}
			// Sampled INSIDE the hook: after this returns, the connection can close and
			// the resolver can commit the drain, so anything read later is a race.
			hs.mu.Lock()
			if d := hs.drains[sid]; d != nil {
				heldStaged.Store(d.staged)
				sampled.Store(true)
			}
			hs.mu.Unlock()
			return nil, errors.New("test: the boundary save's response is lost")
		})
	defer restore()

	c, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "q0ramb"}, o.MQTT.Host, o.MQTT.Port, 0)
	if c != nil {
		defer c.Close()
	}
	if cerr == nil {
		testMQTTExpectRefusedResume(t, c)
	}
	restore()

	if !sampled.Load() {
		t.Fatal("the ambiguous write never met a converted drain: the fixture did not reach its route")
	}
	if !heldStaged.Load() {
		t.Fatal("an ambiguous write must leave the drain STAGED: committing it would act on bytes " +
			"nothing has proven durable")
	}
	// AFTER the refusal, not only inside the hook. The in-hook sample proves staging BEFORE
	// the outcome was classified; this proves the ambiguity committed nothing, which is the
	// contract.
	sess := testMQTTResidentSession(t, s, "q0ramb")
	sess.mu.Lock()
	d := sess.drains[sid]
	stagedAfter := d != nil && d.staged
	sess.mu.Unlock()
	if d == nil {
		t.Fatalf("the drain for sid %q must survive an ambiguous write", sid)
	}
	if !stagedAfter {
		t.Fatal("the drain must still be STAGED after the refusal: nothing may be committed until " +
			"those exact bytes are proven durable")
	}
	// Nothing was destroyed. The durable is what holds the client's exchange.
	if !testDurableExists(t, s, dur) {
		t.Fatalf("an ambiguous write must not delete durable %q", dur)
	}
	// The planned COLD resume, and it has to be cold: merely reconnecting to the same
	// running server leaves the persistent session RESIDENT, so the retransmission could
	// come from retained in-memory state and would establish nothing about the landed
	// record. Restarting with the store preserved is what discards that state.
	s = testRestartPreservingStore(t, s, o)
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "q0ramb"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	sess2 := testMQTTResidentSession(t, s, "q0ramb")
	testAssertColdRestore(t, sess2, sess)
	// The drain came from the RECORD: the ambiguous write landed, so Drains names it.
	sess2.mu.Lock()
	loaded := sess2.drains[sid]
	sess2.mu.Unlock()
	if loaded == nil {
		t.Fatalf("the cold resume must load the drain for sid %q from the landed record", sid)
	}
	rpi := testMQTTCheckPubMsgNoAck(t, c2, r2, "q0r/amb/x", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	// The ORIGINAL identifier: [MQTT-4.4.0-1] binds it to the exchange, and without this a
	// regression that reassigns one still satisfies the PUBLISH check and the zero-claims
	// assertion below.
	if rpi != pi {
		t.Fatalf("the cold resume must retransmit under the original identifier %d, got %d", pi, rpi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, rpi)
	testMQTTFlush(t, c2, nil, r2)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if n := testDrainClaims(sess2); n != 0 {
			return fmt.Errorf("the exchange must complete once the ambiguity resolves, %d claims left", n)
		}
		return nil
	})
}

// The uncertain-cleanup refusal (settleRestoredDowngrades' isSeqUncertain guard) has NO
// test here, and that is recorded rather than papered over.
//
// The holistic review asked for one. Three fixtures were built and none reached the route:
// a destructive descriptor with nothing owed, one with a started exchange under a stream
// inactivity limit, and the two-half wildcard the finding's own description implies. All
// three made exactly TWO session-record writes on the restore path — the accept and the
// boundary — because proveVoidedRegs saves only when the void-verdict revision has moved
// since the last save, and the boundary's own record already carried those verdicts.
//
// The guard is kept: it is cheap, it fails closed, and the alternative is a success CONNACK
// over a record this broker cannot speak for. But it is a HELPER CONTRACT with no
// demonstrated producer, not a covered route, and testQoS0OverLiveConsTwoHalves is left
// behind because it is what a future attempt should start from.

// testQoS0OverLiveConsTwoHalves is testQoS0OverLiveConsRecord for a wildcard whose BOTH
// sids hold a started exchange, which is what gives the restore two deferred descriptors.
func testQoS0OverLiveConsTwoHalves(t *testing.T, s *Server, o *Options, clientID, filter string) (
	conn *mqttTestConn, durMain string, durFwc string) {

	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: filter, qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID + "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	base := strings.TrimSuffix(filter, "/#")
	testMQTTPublish(t, pc, pr, 1, false, false, base+"/x", 1, []byte("below"))
	testMQTTCheckPubMsgNoAck(t, c, r, base+"/x", mqttPubQos1, []byte("below"))
	testMQTTPublish(t, pc, pr, 1, false, false, base, 2, []byte("parent"))
	testMQTTCheckPubMsgNoAck(t, c, r, base, mqttPubQos1, []byte("parent"))

	sess := testMQTTResidentSession(t, s, clientID)
	conv, cerr := mqttToNATSSubjectConversion([]byte(filter), true)
	if cerr != nil {
		t.Fatalf("converting %q: %v", filter, cerr)
	}
	sid := string(conv)
	fwcsid := sid[:len(sid)-2] + mqttMultiLevelSidSuffix
	sess.mu.Lock()
	ccMain, ccFwc := sess.cons[sid], sess.cons[fwcsid]
	sess.mu.Unlock()
	if ccMain == nil || ccFwc == nil {
		t.Fatalf("both halves need consumers, got main=%v fwc=%v", ccMain, ccFwc)
	}
	durMain, durFwc = ccMain.Durable, ccFwc.Durable

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: filter, qos: 0}}, []byte{0})
	testMQTTFlush(t, c, nil, r)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.Lock()
	asm.testSubFilterFail = func(exit, failSid string) error {
		if exit == "consumer-fwc" && failSid == fwcsid {
			return fmt.Errorf("test: the level-up companion fails after both halves adopted")
		}
		return nil
	}
	asm.mu.Unlock()
	testMQTTSendSubOnly(t, 3, c, []*mqttFilter{{filter: filter, qos: 1}})
	if codes := testMQTTReadSubAckTolerantOfPubs(t, c, r, 3); len(codes) != 1 || codes[0] != mqttSubAckFailure {
		t.Fatalf("the level-up failure must fail the whole filter, got %v", codes)
	}
	asm.mu.Lock()
	asm.testSubFilterFail = nil
	asm.mu.Unlock()
	testMQTTSub(t, 4, c, r, []*mqttFilter{{filter: clientID + "/keepalive", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	for _, d := range []string{durMain, durFwc} {
		if inCons, inDrains := testRecordNamesDurable(t, sess, d); !inCons || inDrains {
			t.Fatalf("the fixture needs durable %q in Cons and not Drains, got inCons=%v inDrains=%v",
				d, inCons, inDrains)
		}
	}
	return &mqttTestConn{c: c, r: r}, durMain, durFwc
}

// TestMQTTRestoreDefiniteFailureWithdrawsTheTransmittedVerdicts is T7b, and it is a
// DIFFERENT contract from T7's.
//
// T7 exercises a CONVERTED descriptor, whose consumer the un-convert restores. This one
// exercises the destructive population: draining is forbidden, so the staging voids the
// durable's TRANSMITTED registers and records "never transmitted" verdicts for exchanges
// the client is actually holding. If the boundary's save then definitely fails, those
// verdicts must be WITHDRAWN along with the restored Cons entry — otherwise a later
// resident save persists an authorization to abandon a live register, and the identifier is
// reserved for the life of the session with no message behind it.
//
// mayDrain() is packet-wide, so this cannot be combined with a converting descriptor.
func TestMQTTRestoreDefiniteFailureWithdrawsTheTransmittedVerdicts(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const filter = "q0r/withdraw/#"
	conn, dur, sid, _ := testQoS0OverLiveConsRecord(t, s, o, "q0rwd", filter, 1)
	prior := testMQTTResidentSession(t, s, "q0rwd")
	conn.c.Close()

	// Draining forbidden, so the descriptor takes the destructive route and its staging
	// voids the transmitted register rather than preserving it for a drain.
	si, lerr := prior.jsa.lookupStream(mqttStreamName)
	if lerr != nil || si == nil {
		t.Fatalf("lookupStream: %v", lerr)
	}
	cfg := si.Config
	cfg.ConsumerLimits.InactiveThreshold = time.Hour
	if _, uerr := prior.jsa.updateStream(&cfg); uerr != nil {
		t.Fatalf("imposing a consumer inactivity limit: %v", uerr)
	}

	s = testRestartPreservingStore(t, s, o)

	// Fail the BOUNDARY's save definitely. Save 1 is the accepting CONNECT's.
	var saves atomic.Int32
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
		if saves.Add(1) <= 1 {
			return nil
		}
		return errors.New("test: the restore-side boundary's save is refused")
	})
	defer restore()

	c, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "q0rwd"}, o.MQTT.Host, o.MQTT.Port, 0)
	if c != nil {
		defer c.Close()
	}
	if cerr == nil {
		testMQTTExpectRefusedResume(t, c)
	}
	restore()

	if n := saves.Load(); n < 2 {
		t.Fatalf("the fixture must reach the boundary's save: only %d session-record writes were made", n)
	}

	sess := testMQTTResidentSession(t, s, "q0rwd")

	// The verdicts are withdrawn. cancelledRegNonces is where a standing "never
	// transmitted" verdict lives, and any left there is one a later save would make
	// durable against a register whose delivery is still owed.
	sess.mu.Lock()
	standing := len(sess.cancelledRegNonces)
	cc := sess.cons[sid]
	d := sess.drains[sid]
	sess.mu.Unlock()
	if standing != 0 {
		t.Fatalf("a definite boundary failure must WITHDRAW the transmitted-register verdicts it "+
			"staged: %d are still standing, and the next save would persist them", standing)
	}
	// The released Cons entry is back, with the same identity: the record still names this
	// durable, so memory must agree or the next save drops it.
	if d != nil {
		t.Fatalf("draining was forbidden, so nothing may have been converted for sid %q", sid)
	}
	if cc == nil || cc.Durable != dur {
		t.Fatalf("the released consumer entry for sid %q must be restored naming %q, got %v", sid, dur, cc)
	}
	// And nothing was destroyed: the durable still holds the client's exchange.
	if !testDurableExists(t, s, dur) {
		t.Fatalf("a definite save failure must not delete durable %q", dur)
	}
	if inCons, inDrains := testRecordNamesDurable(t, sess, dur); !inCons || inDrains {
		t.Fatalf("nothing was stored, so the record must still name %q in Cons, got inCons=%v inDrains=%v",
			dur, inCons, inDrains)
	}
}

// TestMQTTRestoreAmbiguousSettlesOnTheResidentObject is T8b, the contract T8 cannot reach.
//
// A refused CONNECT does NOT evict a persistent session, so the resolver can settle the
// attempt on the very object that stays resident. The landed continuation is what commits
// the drain THERE — unstaging it and clearing the durable's inactivity timer. Without it the
// resident drain stays staged indefinitely, and staged means nothing may be torn down or
// acked, so its exchanges cannot complete on that object at all.
//
// The oracle is that the SAME resident object is unstaged, which an omitted continuation
// fails while every assertion in T8 still passes.
func TestMQTTRestoreAmbiguousSettlesOnTheResidentObject(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	// The periodic pass is what settles an ambiguity whose connection was refused: the
	// pre-CONNACK refusal closes without a readLoop resolution. Shortened so the test
	// exercises that resolution instead of timing out ahead of it. Process-global, so this
	// test does not call t.Parallel().
	restoreSweep := setMQTTSessionExpirySweepInterval(250 * time.Millisecond)
	defer restoreSweep()

	const filter = "q0r/ambres/#"
	conn, dur, sid, _ := testQoS0OverLiveConsRecord(t, s, o, "q0rambres", filter, 1)
	conn.c.Close()
	s = testRestartPreservingStore(t, s, o)

	// Lose the response to the boundary's session-record write. As in T8, the accepting CONNECT's
	// own save is the outbound-ownership metadata commit (F-105 cutover), which never reaches this
	// store-result seam, so the boundary save is the FIRST write this hook sees. Sampled INSIDE the
	// hook, the restore's drain for sid must be staged: that is what proves the lost write is the
	// boundary's and not some other save.
	var seen atomic.Int32
	var atBoundary atomic.Bool
	restore := setMQTTTestSessStoreResultHook(
		func(hs *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if err != nil || seen.Add(1) != 1 {
				return resp, err
			}
			hs.mu.Lock()
			if d := hs.drains[sid]; d != nil && d.staged {
				atBoundary.Store(true)
			}
			hs.mu.Unlock()
			return nil, errors.New("test: the boundary save's response is lost")
		})
	defer restore()

	c, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "q0rambres"}, o.MQTT.Host, o.MQTT.Port, 0)
	if c != nil {
		defer c.Close()
	}
	if cerr == nil {
		testMQTTExpectRefusedResume(t, c)
	}
	restore()

	if seen.Load() < 1 || !atBoundary.Load() {
		t.Fatalf("the fixture must lose the boundary's save while the restore's drain is staged: "+
			"%d writes seen, at the boundary %v", seen.Load(), atBoundary.Load())
	}

	// The SAME object the refusal left resident.
	sess := testMQTTResidentSession(t, s, "q0rambres")
	sess.mu.Lock()
	d := sess.drains[sid]
	sess.mu.Unlock()
	if d == nil {
		t.Fatalf("the refusal must leave the drain for sid %q on the resident session", sid)
	}

	// The resolver proves the attempted bytes durable and the continuation commits the drain
	// on THIS object. Nothing else in this window unstages one.
	checkFor(t, 15*time.Second, 50*time.Millisecond, func() error {
		sess.mu.Lock()
		still := sess.drains[sid]
		staged := still != nil && still.staged
		sess.mu.Unlock()
		if still == nil {
			return fmt.Errorf("the drain vanished instead of being committed")
		}
		if staged {
			return fmt.Errorf("the landed continuation has not committed the resident drain yet")
		}
		return nil
	})
	if !testDurableExists(t, s, dur) {
		t.Fatalf("durable %q must survive the resolution", dur)
	}
}
