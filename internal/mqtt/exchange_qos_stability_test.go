//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// A started exchange's QoS is a property of the EXCHANGE, not of the subscription it
// arrived through. These fixtures pin that: a re-SUBSCRIBE may replace a filter's
// maximum QoS at any time ([MQTT-3.8.4-3]) without interrupting a publication already
// in flight, so every retransmission of a started message carries the QoS it was first
// transmitted at.
//
// Every re-SUBSCRIBE here lands BEFORE the redelivery it is meant to affect, and the
// redelivery is forced by NAKing the delivery's own ack subject rather than by waiting
// out AckWait. Both matter. A test that let the redelivery race the SUBACK would pass
// against the broken code whenever the downgrade lost the race, and one that slept for
// AckWait would be load-sensitive on exactly the machines that run it slowest.
//
// DUP is deliberately NOT used as an oracle anywhere in this file: it is derived from
// the JetStream delivery count, so a redelivery carries DUP=1 whether or not this
// mechanism works (F-072).

// testExchangeQoSAckSubject returns the JetStream ack subject of the delivery currently
// outstanding under pi, which is what a test NAKs to schedule a re-offer.
//
// Read under sess.mu and asserted non-empty: an empty subject would make the NAK a
// silent no-op, and the test would then observe no redelivery and could not tell that
// from a redelivery the broker refused to make.
func testExchangeQoSAckSubject(t testing.TB, sess *mqttSession, pi uint16) string {
	t.Helper()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	ack := sess.pendingPublish[pi]
	if ack == nil {
		t.Fatalf("no pending entry for packet identifier %d", pi)
	}
	if ack.jsAckSubject == _EMPTY_ {
		t.Fatalf("pending entry for packet identifier %d carries no ack subject", pi)
	}
	return ack.jsAckSubject
}

// testExchangeQoSEntryQoS returns the recorded exchange QoS the pending entry carries.
func testExchangeQoSEntryQoS(t testing.TB, sess *mqttSession, pi uint16) byte {
	t.Helper()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	ack := sess.pendingPublish[pi]
	if ack == nil {
		t.Fatalf("no pending entry for packet identifier %d", pi)
	}
	return ack.qos
}

// TestExchangeQoSStableAcrossDowngrade is the item's primary fixture, on the PLAIN
// non-drain route: no UNSUBSCRIBE, no drain, no reclaim. A live QoS 2 subscription with
// an unacknowledged PUBLISH in flight, one SUBSCRIBE re-granting the same filter at
// QoS 1, and the next re-offer.
//
// Kills the mutation that recomputes the wire QoS from the subscription on the started
// path. Under it the retransmission goes out at QoS 1 under the same identifier, the
// client answers PUBACK, and mqttProcessPublishReceived deletes the QoS 2 owner slot
// with no PUBREL ever stored — the exactly-once handshake abandoned.
func TestExchangeQoSStableAcrossDowngrade(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	// Long, deliberately: the redelivery in this test is scheduled by an explicit NAK,
	// and an AckWait short enough to fire on its own would reintroduce the race the
	// NAK exists to remove.
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pubc, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pubc.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pubc, pubr, 2, false, false, "foo", 1, []byte("msg"))

	// The first transmission: QoS 2, and PUBREC deliberately withheld so the exchange
	// stays started for the rest of the test.
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("msg"))

	sess := testSubRevokeSession(t, s, "sub")
	if got := testExchangeQoSEntryQoS(t, sess, pi); got != 2 {
		t.Fatalf("the exchange must record QoS 2 from its first transmission, got %d", got)
	}
	reply := testExchangeQoSAckSubject(t, sess, pi)

	// The downgrade, and it is COMPLETE before the re-offer is scheduled: the SUBACK is
	// read here and the NAK is only sent afterwards.
	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	sess.jsa.sendMsg(reply, AckNak)

	// The whole assertion: still QoS 2, still the same identifier. Asserted against the
	// value the defect produces (1), so it cannot pass by matching a zero or a default.
	rpi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2|mqttPubFlagDup, []byte("msg"))
	if rpi != pi {
		t.Fatalf("the retransmission must reuse the exchange's identifier %d, got %d", pi, rpi)
	}

	// And the handshake still completes, which is what the QoS matters for.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
}

// TestExchangeQoSDowngradeKeepsPubRelOwed is the downstream half: a conforming client
// answers the retransmission for the QoS it actually received. Because the header says
// QoS 2, that answer is a PUBREC, and the exchange still owes a PUBREL.
//
// The observation point is named rather than implied — after the PUBREC, the register
// must still exist and the session must still owe a PUBREL under pi.
func TestExchangeQoSDowngradeKeepsPubRelOwed(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pubc, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pubc.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pubc, pubr, 2, false, false, "foo", 1, []byte("msg"))

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("msg"))
	sess := testSubRevokeSession(t, s, "sub")
	reply := testExchangeQoSAckSubject(t, sess, pi)

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	sess.jsa.sendMsg(reply, AckNak)

	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2|mqttPubFlagDup, []byte("msg"))

	// The client answers what it received. A PUBREC, because the PUBLISH said QoS 2.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)

	// THE observation point: the second phase is live, not deleted. Under the defect
	// the client would have answered PUBACK instead and this state would not exist.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		sess.mu.Lock()
		_, owed := sess.pendingPubRel[pi]
		sess.mu.Unlock()
		if !owed {
			return fmt.Errorf("the exchange must still owe a PUBREL under identifier %d", pi)
		}
		return nil
	})
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
}

// TestExchangeQoSStableAcrossUpgrade is the other direction, and it is asserted against
// the OPPOSITE default from the downgrade test: 1 where the defect produces 2. Neither
// can pass by matching a zero value or a fixture setting.
//
// The publication is QoS 2 under an initial QoS 1 GRANT, which is load-bearing. A QoS 1
// publication would recompute to 1 even after the grant is upgraded, so the mutation
// would be invisible and the test would prove nothing.
func TestExchangeQoSStableAcrossUpgrade(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	pubc, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pubc.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	// Published at QoS 2, granted QoS 1: transmitted at QoS 1 by min(published, granted).
	testMQTTPublish(t, pubc, pubr, 2, false, false, "foo", 1, []byte("msg"))

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg"))
	sess := testSubRevokeSession(t, s, "sub")
	if got := testExchangeQoSEntryQoS(t, sess, pi); got != 1 {
		t.Fatalf("the exchange must record QoS 1 from its first transmission, got %d", got)
	}
	reply := testExchangeQoSAckSubject(t, sess, pi)

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)
	sess.jsa.sendMsg(reply, AckNak)

	rpi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg"))
	if rpi != pi {
		t.Fatalf("the retransmission must reuse the exchange's identifier %d, got %d", pi, rpi)
	}
	// A QoS 1 exchange completes with a PUBACK. Under the defect the retransmission
	// would have been QoS 2, the client would answer PUBREC, and the slot transition
	// would be refused — the abandoned handshake in the other direction.
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
}

// TestExchangeQoSFirstTransmissionHonorsNewGrant is the guard against overshooting:
// freezing an exchange's QoS must not freeze the SUBSCRIPTION's. A message published
// AFTER a downgrade is a first transmission and takes the new maximum.
//
// Without this, a fix that pinned QoS globally — or one that read a stale subscription
// value — would pass every other fixture in this file.
func TestExchangeQoSFirstTransmissionHonorsNewGrant(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pubc, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pubc.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)

	// One exchange started at QoS 2 and left outstanding, so the session is in exactly
	// the state the other fixtures assert about while this one checks the new grant.
	testMQTTPublish(t, pubc, pubr, 2, false, false, "foo", 1, []byte("first"))
	firstPI := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("first"))

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	// A NEW publication after the downgrade: no prior entry, so the subscription's
	// maximum is the whole input and QoS 1 is correct.
	testMQTTPublish(t, pubc, pubr, 2, false, false, "foo", 2, []byte("second"))
	secondPI := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("second"))
	if secondPI == firstPI {
		t.Fatalf("the new publication must take its own identifier, got %d twice", secondPI)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, secondPI)

	testMQTTSendPIPacket(mqttPacketPubRec, t, c, firstPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, firstPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, firstPI)
}

// TestExchangeQoSRecoveredFromRegisterAcrossDowngrade is the fixture that exercises the
// DURABLE half of the carrier: the in-memory entry is destroyed and rebuilt from the
// in-flight register, so the QoS the retransmission carries can only have come from the
// record.
//
// A same-server reconnect would NOT do: it keeps the session's pending maps, and
// same-object recovery gives an existing entry precedence over the record. Only a fresh
// session object rebuilds through mqttRecoveredPending, which is why this restarts the
// server against the same store directory.
//
// This is the fixture that kills the mutation dropping the recovered register's QoS.
// The four in-process fixtures above cannot: they never reconstruct an entry, so a
// carrier that recovers nothing still passes every one of them.
func TestExchangeQoSRecoveredFromRegisterAcrossDowngrade(t *testing.T) {
	o := testMQTTDefaultOptions()
	// SHORT here, unlike every fixture above, and deliberately so. Those schedule
	// their re-offer with an explicit NAK because the downgrade has to be proven to
	// land first; this one cannot, because a restart discards the in-memory ack
	// subject there is nothing left to NAK. It does not need to: the downgrade is
	// PERSISTED before the shutdown, so the restart boundary orders it ahead of any
	// redelivery and no timing assumption is doing the work. AckWait is only what
	// makes the redelivery arrive inside the client read deadline.
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, cp, rp, 2, false, false, "foo", 1, []byte("msg"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	// Transmitted at QoS 2, PUBREC withheld: the register now holds QoS 2 for this
	// exchange and that record is the only place the fact survives the restart.
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("msg"))

	// The downgrade, while the exchange is outstanding. It is persisted with the
	// session, so the restored subscription is QoS 1 and the recomputation the defect
	// performs would produce QoS 1 after recovery too.
	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)

	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// QoS 2, from the register, under the original identifier — with the restored
	// subscription sitting at QoS 1 the whole time.
	rpi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2|mqttPubFlagDup, []byte("msg"))
	if rpi != pi {
		t.Fatalf("the resumed retransmission must reuse identifier %d, got %d", pi, rpi)
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, rpi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, rpi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, rpi)
}

// TestExchangeQoSDrainPubRelRecreatedUnderLoweredDrain is the drain half of this item,
// and it is the same root cause as the fixtures above: a SUBSCRIPTION fact standing in
// for an EXCHANGE fact.
//
// The three PUBREL gates keyed on the DRAIN's QoS. A re-SUBSCRIBE can lower that below
// the QoS of exchanges already started under it, because stageDrain copies whatever
// sub.mqtt.qos says at staging time — so a QoS 1 drain routinely holds a QoS 2 exchange,
// and a QoS 2 exchange past PUBREC inside one never had its PUBREL recreated. That is
// loss, not a wrong header byte.
//
// The ordering is load-bearing and was corrected by a review round: the PUBREC must come
// AFTER the UNSUBSCRIBE. A successful PUBREC untracks the PUBLISH and its source mapping,
// and staging enumerates exactly those mappings, so a PUBREC-first sequence produces a
// drain with no intent at all and the fixture would prove nothing.
func TestExchangeQoSDrainPubRelRecreatedUnderLoweredDrain(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	conn, sess, pi := testDrainSetup(t, s, o, "loweredrain", "loweredrain/topic", 2)

	// THE DOWNGRADE, before the UNSUBSCRIBE, so the drain is staged at QoS 1 while it
	// holds a QoS 2 exchange. This is what makes d.qos an unsound gate.
	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: "loweredrain/topic", qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	testMQTTUnsub(t, 3, conn.c, conn.r, []*mqttFilter{{filter: "loweredrain/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	// Positively assert the drain really is at QoS 1 — the whole premise. Asserted
	// rather than assumed, because a drain that happened to be staged at QoS 2 would
	// make this fixture pass against the old gate and prove nothing.
	sess.mu.Lock()
	var drainQoS byte = 0xff
	for _, d := range sess.drains {
		drainQoS = d.qos
	}
	sess.mu.Unlock()
	if drainQoS != 1 {
		t.Fatalf("the drain must be staged at QoS 1 for this fixture to reach its route, got %d", drainQoS)
	}

	// Fail only the PUBREL store. The intent is written first, so it lands; the record
	// does not — which is the state whose recreation the drain-QoS gate prevented.
	restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			return errors.New("test: PUBREL store refused")
		}
		return nil
	})
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	testDrainWaitConnClosed(t, conn.r)
	restore()
	if !testDrainIntentRecorded(sess) {
		t.Fatal("the intent must have been persisted before the failing store")
	}

	conn.c.Close()
	s = testDrainQoS2Restart(t, s, o)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "loweredrain", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// Recreated under the ORIGINAL identifier. Under the old d.qos < 2 gate the drain is
	// skipped entirely and this read times out: the exchange can never complete.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)
}

// TestExchangeQoSZeroWithholdsWithoutAcking pins the fail-closed arm: an entry carrying
// no per-exchange QoS is withheld, WITHOUT acknowledging the source, rather than
// transmitted at a guessed QoS.
//
// This is a focused injection, not a production route. §4.2(a)'s audit found no ordinary
// removal that leaves a source mapping behind a missing entry, so the arm is defensive —
// and a defensive arm with no test is untested behaviour, not verified unreachability.
// The distinction is recorded rather than glossed: if only this fixture fails under the
// paired mutation, that is what it proves.
//
// The injected state has to be complete enough to REACH the send section. The cpending
// mapping is the part that matters: without it trackPublish allocates a fresh identifier
// and stores a register, replacing the injected zero with confirmed evidence, and the
// fixture would pass against broken code.
func TestExchangeQoSZeroWithholdsWithoutAcking(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pubc, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pubc.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pubc, pubr, 2, false, false, "foo", 1, []byte("msg"))

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("msg"))
	sess := testSubRevokeSession(t, s, "sub")
	reply := testExchangeQoSAckSubject(t, sess, pi)

	// Zero the recorded QoS on the live, otherwise-sendable entry, leaving its identity,
	// its register and its source mapping exactly as they are. That is the state §4.3
	// must refuse to guess at.
	sess.mu.Lock()
	ack := sess.pendingPublish[pi]
	if ack == nil {
		sess.mu.Unlock()
		t.Fatalf("no pending entry for identifier %d", pi)
	}
	if ack.regPending || ack.regCancelled || ack.regForeign {
		sess.mu.Unlock()
		t.Fatalf("the injected entry must be otherwise sendable, got %+v", ack)
	}
	jsDur, sseq := ack.jsDur, ack.sseq
	// The mapping is what makes the re-offer find THIS entry rather than allocate a new
	// identifier. Asserted, not assumed.
	if got, ok := sess.cpending[jsDur][sseq]; !ok || got != pi {
		sess.mu.Unlock()
		t.Fatalf("the source mapping must name identifier %d, got %d ok=%v", pi, got, ok)
	}
	ack.qos = 0
	sess.mu.Unlock()

	// Force the re-offer. It must be withheld: no PUBLISH, and no source acknowledgement.
	sess.jsa.sendMsg(reply, AckNak)

	// Nothing on the wire. The error is required to be a TIMEOUT specifically: an EOF or
	// a reset would mean the connection was torn down, and this fixture's whole claim —
	// withheld, still reserved, still live — would be indistinguishable from a broker that
	// had simply dropped the client.
	var buf [512]byte
	r.reader.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, err := r.reader.Read(buf[:])
	if err == nil {
		t.Fatalf("a zero-QoS entry must not be transmitted; got %d bytes, first %#x", n, buf[0])
	}
	var nerr net.Error
	if !errors.As(err, &nerr) || !nerr.Timeout() {
		t.Fatalf("expected the read to time out with the connection intact, got %v", err)
	}

	// The identifier is still held and the entry still there: withheld, not settled. That
	// is the positive half of the assertion, and it is what tells a withhold apart from a
	// delivery that was acknowledged away.
	sess.mu.Lock()
	still := sess.pendingPublish[pi]
	_, mapped := sess.cpending[jsDur][sseq]
	sess.mu.Unlock()
	if still == nil {
		t.Fatalf("the withheld entry must stay reserved, not be released")
	}
	if !mapped {
		t.Fatalf("the withheld delivery must keep its source mapping, not be acknowledged away")
	}

	// And NO send reservation leaked. The zero is tested before beginSendLocked reserves
	// the identifier, so the fail-closed return has no token to release; a version that
	// reserved first and returned without releasing would strand this identifier for the
	// life of the session, and every later re-offer would meet its own refusal.
	//
	// Both halves are checked, because they are separate state: the token on the entry
	// and the hold in the registry.
	sess.mu.Lock()
	leakedToken := still.sendNonce
	_, leakedHold := sess.sendsInFlight[pi]
	sess.mu.Unlock()
	if leakedToken != _EMPTY_ {
		t.Fatalf("the withhold leaked a send token %q", leakedToken)
	}
	if leakedHold {
		t.Fatalf("the withhold leaked a send hold for identifier %d", pi)
	}
}

// TestExchangeQoSWireMatchesPreservedRecord closes the loop the unit-level replacement
// fixtures leave open: they assert the durable record and the pending entry, but neither
// puts a PUBLISH on the wire. If the callback read its QoS before the store settled, the
// record could hold the occupant's preserved value while the client received the caller's
// — and every register-side assertion would still pass.
//
// So this asserts the two AGREE, end to end, for the same exchange: the byte on the wire
// and the byte in the record.
//
// The replacement is simulated at the register boundary rather than with a second broker.
// That is deliberate: the arm this exercises is reached whenever a register for the same
// delivery already exists with a different QoS, and injecting that occupant makes the
// schedule deterministic instead of racing two recovery scans. What is NOT claimed is
// coverage of the cross-broker allocation race itself.
func TestExchangeQoSWireMatchesPreservedRecord(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	sess := testSubRevokeSession(t, s, "sub")
	asm := testMQTTGetAccountSessionManager(t, s, "sub")

	// The occupant: a register for the delivery that is ABOUT to be allocated, carrying
	// QoS 2, written as if by another broker. Installed through the store seam on the
	// create-only attempt so the caller's own create collides with it.
	var injected bool
	asm.mu.Lock()
	asm.testInflightStore = func(subject string, expectSeq uint64, rec *mqttInflightRecord) (uint64, error) {
		if expectSeq == 0 && !injected {
			injected = true
			occupant := *rec
			occupant.QoS = 2
			occupant.Nonce = nuid.Next()
			if _, err := asm.jsa.storeInflightCAS(subject, 0, &occupant); err != nil {
				return 0, err
			}
			// Our own create now fails as it would against a real occupant, and the
			// retry loop reaches the same-source replacement arm.
			return asm.jsa.storeInflightCAS(subject, 0, rec)
		}
		return asm.jsa.storeInflightCAS(subject, expectSeq, rec)
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testInflightStore = nil
		asm.mu.Unlock()
	}()

	// Publish at QoS 1: the caller's computed QoS is 1, the occupant's is 2, so the
	// preserved value and the caller's value DIFFER. Without that they would be
	// indistinguishable and the fixture would prove nothing.
	pubc, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pubc.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pubc, pubr, 1, false, false, "foo", 1, []byte("msg"))

	// The wire byte. QoS 2, from the preserved record -- not the QoS 1 the caller computed.
	flags, pi := testMQTTGetPubMsg(t, c, r, "foo", []byte("msg"))
	wireQoS := (flags & mqttPubFlagQoS) >> 1
	if !injected {
		t.Fatal("the occupant was never injected, so the replacement arm was not reached")
	}

	// The record byte, for the SAME identifier.
	subject := mqttInflightSubject(sess.idHash, sess.gen, pi)
	rec, _, err := asm.jsa.loadInflightRecord(subject)
	if err != nil || rec == nil {
		t.Fatalf("load the register for identifier %d: rec=%v err=%v", pi, rec, err)
	}

	// THE assertion: they agree, and they agree on the PRESERVED value.
	if byte(wireQoS) != rec.QoS {
		t.Fatalf("the wire QoS %d and the register QoS %d disagree for identifier %d", wireQoS, rec.QoS, pi)
	}
	if rec.QoS != 2 {
		t.Fatalf("the replacement must preserve the occupant's QoS 2, got %d", rec.QoS)
	}

	// Completed as a QoS 2 exchange, which is what the preserved byte commits the broker to.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
}

// TestExchangeQoSPreRecDrainWithheldWhenPubRelSetupFails covers the drain predicate's
// THIRD term, which the recreation fixture above cannot reach.
//
// The exchange here is transmitted but NOT past PUBREC, so it carries no persisted
// intent, and its drain was staged at QoS 1 by an earlier re-SUBSCRIBE. Neither of the
// first two terms holds. Only the recovered register's QoS says this drain may still owe
// a PUBREL — as soon as the client answers the retransmission, which after this change
// correctly goes out at QoS 2.
//
// So when re-establishing PUBREL delivery FAILS on resume, this drain must be withheld:
// restoring source delivery for it would put a QoS 2 PUBLISH on the wire with no path
// for the PUBREL the client is then owed. Without the third term the drain is not in the
// withheld set at all, and the source is restored regardless — which is the mutation
// this fixture kills.
func TestExchangeQoSPreRecDrainWithheldWhenPubRelSetupFails(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	cisub := &mqttConnInfo{clientID: "prerec", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "prerec/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "prerecpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 2, false, false, "prerec/topic", 1, []byte("payload"))
	testMQTTDisconnect(t, pc, nil)
	pc.Close()

	// Transmitted at QoS 2. PUBREC is NEVER sent, so no intent is ever recorded — that is
	// what distinguishes this fixture from the recreation one.
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "prerec/topic", mqttPubQoS2, []byte("payload"))

	// Lower the subscription, then drain it: the drain is staged at QoS 1 while holding a
	// QoS 2 exchange.
	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "prerec/topic", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	testMQTTUnsub(t, 3, c, r, []*mqttFilter{{filter: "prerec/topic"}})
	testMQTTFlush(t, c, nil, r)

	sess := testSubRevokeSession(t, s, "prerec")
	sess.mu.Lock()
	var drainQoS byte = 0xff
	var anyIntent bool
	for _, d := range sess.drains {
		drainQoS = d.qos
		for _, st := range d.seqs {
			if st.intent {
				anyIntent = true
			}
		}
	}
	sess.mu.Unlock()
	if drainQoS != 1 {
		t.Fatalf("the drain must be staged at QoS 1, got %d", drainQoS)
	}
	if anyIntent {
		t.Fatal("this fixture requires NO past-PUBREC intent; the third term is what it tests")
	}

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()

	// Fail the PUBREL delivery re-establishment on resume. That is the arm which consults
	// the withheld set, and it is the only route to it.
	restore := setMQTTTestPubRelSubFailHook(func() error {
		return errors.New("test: PUBREL delivery re-establishment refused")
	})
	defer restore()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)

	c2, r2 := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// WITHHELD: no PUBLISH for the drained QoS 2 exchange. Asserted as a timeout with the
	// connection intact, so it is distinguishable from a teardown — a bare absence would
	// not be.
	var buf [512]byte
	r2.reader.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := r2.reader.Read(buf[:])
	if err == nil {
		t.Fatalf("the drain must be withheld while PUBREL delivery is unavailable; got %d bytes, first %#x", n, buf[0])
	}
	var nerr net.Error
	if !errors.As(err, &nerr) || !nerr.Timeout() {
		t.Fatalf("expected a withhold with the connection intact, got %v", err)
	}
	_ = pi
}
