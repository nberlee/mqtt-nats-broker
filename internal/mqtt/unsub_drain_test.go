//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testDrainSetup connects a persistent QoS 1/2 subscriber, publishes one message,
// and returns the subscriber's connection, its reader, its session and the packet
// identifier of the (unacknowledged) delivery.
func testDrainSetup(t *testing.T, s *Server, o *Options, clientID, topic string, qos byte) (*mqttTestConn, *mqttSession, uint16) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: qos}}, []byte{qos})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID + "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, qos, false, false, topic, 1, []byte("payload"))

	flags := mqttPubQos1
	if qos == 2 {
		flags = mqttPubQoS2
	}
	pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, flags, []byte("payload"))
	return &mqttTestConn{c: c, r: r}, testMQTTResidentSession(t, s, clientID), pi
}

type mqttTestConn struct {
	c net.Conn
	r *mqttReader
}

// testConsumerCount reports how many ACTIVE (non-draining) consumers the session
// holds. I7 means a drained consumer must not appear here.
func testConsumerCount(sess *mqttSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.cons)
}

// testDrainClaims reports how many sequences are still claimed across all drains.
func testDrainClaims(sess *mqttSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	n := 0
	for _, d := range sess.drainByDur {
		n += d.claimedCount()
	}
	return n
}

// testDrainCount reports how many consumers are currently draining.
func testDrainCount(sess *mqttSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.drainByDur)
}

// TestMQTTDrainDiscardsFreshMessages is test 13: a drained filter completes what
// it started and delivers NOTHING new. The client unsubscribed, so it is not owed
// anything published afterwards — but the durable has to stay alive to redeliver
// what it already sent, which is exactly why fresh messages need an explicit
// discard rather than simply not arriving.
func TestMQTTDrainDiscardsFreshMessages(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "fresh", "fresh/topic", 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "fresh/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	// Publish AFTER the unsubscribe: it must never reach the client.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "freshpub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "fresh/topic", 2, []byte("after"))

	// The STARTED delivery is still redelivered, and only ever with the original
	// payload.
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "fresh/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)

	// Nothing else arrives: the fresh message was discarded, not queued.
	testMQTTExpectNothing(t, conn.r)

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainSurvivesReconnect is test 15: the drain is durable state, so a
// disconnect and reconnect does not lose the delivery the client is still owed —
// and the filter stays unsubscribed, because the drain is deliberately not a
// membership.
func TestMQTTDrainSurvivesReconnect(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "reconn", "reconn/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "reconn/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	conn.c.Close()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "reconn"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	sess2 := testMQTTResidentSession(t, s, "reconn")
	sess2.mu.Lock()
	_, stillSubscribed := sess2.subs["reconn/topic"]
	sess2.mu.Unlock()
	if stillSubscribed {
		t.Error("the filter must stay unsubscribed; a drain is not a membership")
	}
	if n := testDrainCount(sess2); n != 1 {
		t.Fatalf("the drain did not survive the reconnect, got %v", n)
	}

	got := testMQTTCheckPubMsgNoAck(t, c2, r2, "reconn/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v (same process)", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, got)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainAdoptedByResubscribe is test 18: re-SUBSCRIBING to a draining
// filter reclaims the drain rather than building a second consumer beside it.
//
// Because a drain is invisible in sess.cons (I7), a re-SUBSCRIBE that did not
// adopt would create a SECOND durable on the same filter and deliver every new
// message twice.
func TestMQTTDrainAdoptedByResubscribe(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "adopt", "adopt/topic", 1)
	defer conn.c.Close()

	sess.mu.Lock()
	durBefore := ""
	for _, cc := range sess.cons {
		durBefore = cc.Durable
	}
	sess.mu.Unlock()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "adopt/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: "adopt/topic", qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	nDrains := len(sess.drainByDur)
	nCons := len(sess.cons)
	durAfter := ""
	for _, cc := range sess.cons {
		durAfter = cc.Durable
	}
	sess.mu.Unlock()
	if nDrains != 0 {
		t.Errorf("the drain should have been adopted, %d still draining", nDrains)
	}
	if nCons != 1 {
		t.Fatalf("expected exactly one consumer for the filter, got %d: a re-SUBSCRIBE built a second one", nCons)
	}
	if durAfter != durBefore {
		t.Errorf("the adopted consumer is %q, want the original %q", durAfter, durBefore)
	}

	// The started delivery is still owed and still completes...
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "adopt/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)

	// ...and new messages flow again, exactly once.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "adoptpub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "adopt/topic", 3, []byte("again"))
	next := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "adopt/topic", mqttPubQos1, []byte("again"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, next)
	testMQTTExpectNothing(t, conn.r)
}

// TestMQTTDrainDiscardsWithheldDeliveries is test 14 (I5): only a delivery that
// was actually TRANSMITTED is drained. A message withheld by the session's
// in-flight cap was never put on the wire, so [MQTT-3.10.4-3] does not cover it
// and it must be DISCARDED rather than enrolled as started — enrolling it would
// keep the durable alive for a message the client has no idea about.
//
// The cap has to be the MQTT-level one (Receive Maximum), not MaxAckPending:
// MaxAckPending makes JetStream withhold the message at the consumer, so it never
// reaches trackPublish and sess.deferred stays empty, and the test would pass
// whatever stageDrain did with it.
func TestMQTTDrainDiscardsWithheldDeliveries(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// The consumer's MaxAckPending is paced to the client's Receive Maximum at
	// CREATE time and then outlives the connection, so subscribe with no client
	// limit first and reconnect with Receive Maximum 1. That leaves the MQTT-level
	// cap BELOW the consumer's, which is the only way a delivery is withheld by
	// trackPublish (and so recorded in sess.deferred) rather than by JetStream.
	// A v5 session ends at connection close unless it asks not to, so both
	// connections carry a Session Expiry Interval — otherwise the reconnect would
	// find nothing to resume.
	c0, r0 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "withheld", props: mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r0)
	testMQTTSubV5(t, c0, r0, 1, []mqttV5SubFilter{{topic: "withheld/topic", opts: 1}})
	testMQTTFlush(t, c0, nil, r0)
	testMQTTDisconnect(t, c0, nil)
	c0.Close()

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "withheld",
		props:    append(mqttV5ConnPropsSessionExpiry(300), mqttV5ConnPropsReceiveMax(1)...),
	}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "withheldpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "withheld/topic", 1, []byte("payload"))
	_, pi := testMQTTReadPublishV5(t, r, "withheld/topic", []byte("payload"))

	// The second cannot be transmitted while the first is unacknowledged.
	testMQTTPublish(t, pc, pr, 1, false, false, "withheld/topic", 2, []byte("withheld"))
	testMQTTExpectNothing(t, r)

	sess := testMQTTResidentSession(t, s, "withheld")
	sess.mu.Lock()
	deferredBefore := len(sess.deferred)
	sess.mu.Unlock()
	if deferredBefore == 0 {
		t.Fatal("expected a withheld delivery in sess.deferred; the test is not exercising I5")
	}

	testMQTTUnsubV5(t, c, r, 2, []string{"withheld/topic"})
	testMQTTFlush(t, c, nil, r)

	sess.mu.Lock()
	var claimed int
	for _, d := range sess.drainByDur {
		claimed = d.claimedCount()
	}
	deferredAfter := len(sess.deferred)
	sess.mu.Unlock()
	if claimed != 1 {
		t.Fatalf("the drain claims %d sequence(s), want exactly the one that was transmitted", claimed)
	}
	if deferredAfter != 0 {
		t.Errorf("%d withheld delivery(ies) still tracked; they were never sent and must be dropped", deferredAfter)
	}

	// Completing the transmitted one retires the drain; the withheld message is
	// never delivered.
	_, got := testMQTTReadPublishV5(t, r, "withheld/topic", []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, got)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
	testMQTTExpectNothing(t, r)
}

// TestMQTTDrainRefusedUnderStreamConsumerLimit is test 26: where the messages
// stream imposes a consumer inactivity limit, draining must be REFUSED rather
// than attempted.
//
// A drain has to outlive an offline session, and that limit gives every consumer
// a delete timer it cannot opt out of — so a drained durable would be reaped with
// its messages ($MQTT_msgs is interest-retained). Refusing degrades to the old
// behavior loudly; creating a drain anyway would look compliant and silently lose
// the very messages the drain exists to protect.
func TestMQTTDrainRefusedUnderStreamConsumerLimit(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, _ := testDrainSetup(t, s, o, "limited", "limited/topic", 1)
	defer conn.c.Close()

	// Impose the limit out of band, as a deployment would.
	si, err := sess.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookupStream: %v", err)
	}
	cfg := si.Config
	cfg.ConsumerLimits.InactiveThreshold = time.Hour
	if _, err := sess.jsa.updateStream(&cfg); err != nil {
		// Never skip: a skip here would let this test pass while proving nothing
		// about the guard it exists for.
		t.Fatalf("could not impose a consumer inactivity limit on %q: %v", mqttStreamName, err)
	}

	sess.mu.Lock()
	var subDur string
	for _, cc := range sess.cons {
		subDur = cc.Durable
	}
	sess.mu.Unlock()
	if subDur == "" {
		t.Fatal("expected an active consumer before the unsubscribe")
	}

	l := &captureWarnLogger{warn: make(chan string, 10)}
	s.SetLogger(l, false, false)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "limited/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	if n := testDrainCount(sess); n != 0 {
		t.Fatalf("a drain was created under a consumer inactivity limit (%d); it would be reaped with its messages", n)
	}

	// Refusing to drain is a silent downgrade of a spec guarantee unless it is
	// announced, so the warning is part of the behavior, not decoration.
	var warned string
	for done := time.Now().Add(5 * time.Second); time.Now().Before(done) && warned == _EMPTY_; {
		select {
		case msg := <-l.warn:
			if strings.Contains(msg, "consumer inactivity limit") && strings.Contains(msg, "MQTT-3.10.4-3") {
				warned = msg
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if warned == _EMPTY_ {
		t.Fatal("refusing to drain must warn that started QoS 1/2 deliveries are being dropped and [MQTT-3.10.4-3] is not honored")
	}

	// And the fallback really is today's destructive teardown: the durable goes,
	// taking the started delivery with it.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if ci, err := sess.jsa.lookupConsumer(mqttStreamName, subDur); err == nil && ci != nil {
			return fmt.Errorf("the consumer %q survived, so this was not the documented fallback", subDur)
		}
		return nil
	})
	testMQTTExpectNothing(t, conn.r)
}

// TestMQTTDrainNotAdoptedByQoS0Resubscribe pins the other half of adoption: a
// re-SUBSCRIBE at QoS 0 does NOT reclaim the drain.
//
// Adoption exists because a QoS 1/2 re-SUBSCRIBE would otherwise build a second
// durable on the same filter and double-deliver. A QoS 0 subscription creates no
// durable at all, so there is nothing to collide with: the drain keeps completing
// what it started, at its own QoS, while the new subscription delivers fresh
// messages at QoS 0.
func TestMQTTDrainNotAdoptedByQoS0Resubscribe(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "qos0adopt", "qos0adopt/topic", 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "qos0adopt/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: "qos0adopt/topic", qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("a QoS 0 re-SUBSCRIBE must not adopt the drain, %d draining", n)
	}

	// The started QoS 1 delivery still completes at ITS QoS, not the new one.
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "qos0adopt/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})

	// And the QoS 0 subscription is live and delivers at QoS 0.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0adoptpub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "qos0adopt/topic", 7, []byte("fresh"))
	testMQTTCheckPubMsg(t, conn.c, conn.r, "qos0adopt/topic", 0, []byte("fresh"))
}

// TestMQTTDrainAdoptionPreservesConsumerAccounting pins that adoption moves the
// consumer without touching the books.
//
// Adoption creates no consumer — it hands the existing durable back to cons — so
// it must not charge tmaxack a second time. The mirror-image bug is just as real:
// a drain finishing after an adoption must not refund a charge the adoption still
// owns. Both would silently corrupt the account-wide MaxAckPending budget, which
// is only ever observed as unrelated subscriptions being refused much later.
func TestMQTTDrainAdoptionPreservesConsumerAccounting(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "acct", "acct/topic", 1)
	defer conn.c.Close()

	readAccounting := func() (int, int, string) {
		t.Helper()
		sess.mu.Lock()
		defer sess.mu.Unlock()
		dur := ""
		for _, cc := range sess.cons {
			dur = cc.Durable
		}
		if dur == "" {
			for _, d := range sess.drainByDur {
				dur = d.cc.Durable
			}
		}
		return sess.tmaxack, sess.consCharged[dur], dur
	}

	baseAck, baseCharge, dur := readAccounting()
	if baseCharge == 0 {
		t.Fatal("a consumer created by this process should carry a charge")
	}

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "acct/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	if ack, charge, _ := readAccounting(); ack != baseAck || charge != baseCharge {
		t.Fatalf("draining must not change the books: tmaxack %v->%v, charge %v->%v", baseAck, ack, baseCharge, charge)
	}

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: "acct/topic", qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	ack, charge, durAfter := readAccounting()
	if durAfter != dur {
		t.Fatalf("adoption should have reused durable %q, got %q", dur, durAfter)
	}
	if ack != baseAck {
		t.Fatalf("adoption charged tmaxack again: %v -> %v (it creates no consumer)", baseAck, ack)
	}
	if charge != baseCharge {
		t.Fatalf("adoption changed the durable's charge: %v -> %v", baseCharge, charge)
	}

	// Completing the adopted delivery must not refund a charge the adoption owns.
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "acct/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)
	testMQTTFlush(t, conn.c, nil, conn.r)

	if ack, charge, _ := readAccounting(); ack != baseAck || charge != baseCharge {
		t.Fatalf("completing an adopted delivery refunded a live consumer: tmaxack %v->%v, charge %v->%v",
			baseAck, ack, baseCharge, charge)
	}
}

// TestMQTTDrainDurablesDeletedOnCleanStart is test 21: a drain is session state,
// so discarding the session must delete its durables too.
//
// Nothing else would: a drain is deliberately absent from the record's Cons map,
// so a teardown that only walks Cons leaves the durable behind holding interest in
// its filter forever — with the orphan sweep taught to treat drains as referenced,
// nothing would ever reap it either.
func TestMQTTDrainDurablesDeletedOnCleanStart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, _ := testDrainSetup(t, s, o, "cleanstart", "cleanstart/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "cleanstart/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	var drainDur string
	for dur := range sess.drainByDur {
		drainDur = dur
	}
	sess.mu.Unlock()
	if drainDur == "" {
		t.Fatal("expected a draining durable")
	}
	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur); err != nil || ci == nil {
		t.Fatalf("the drained durable should exist before the discard (err=%v)", err)
	}

	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	// A clean-start CONNECT discards the session, which must take the drain with it.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "cleanstart", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur)
		if err == nil && ci != nil {
			return fmt.Errorf("the drained durable %q outlived its session", drainDur)
		}
		return nil
	})
}

// TestMQTTDrainSurvivesOrphanSweep is test 22: the orphan sweep must count a drain
// as a reference.
//
// A drain is deliberately not in the record's Cons map, so to the sweep it looks
// exactly like an abandoned consumer. Reaping it would delete the started
// deliveries with it, because the messages stream is interest-retained — the loss
// the drain exists to prevent, caused by the machinery meant to tidy up after it.
func TestMQTTDrainSurvivesOrphanSweep(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond

	// Every consumer is old enough to be a candidate, so the grace window cannot
	// make this test pass by simply skipping the drain.
	//
	// Overridden BEFORE the server starts and restored by a defer registered
	// before the shutdown one: deferred calls run last-registered-first, so this
	// ordering is what puts the restore AFTER the server (and the periodic sweep
	// goroutine that reads this variable) has stopped.
	origGrace := mqttOrphanSweepGrace
	mqttOrphanSweepGrace = -time.Second
	defer func() { mqttOrphanSweepGrace = origGrace }()

	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "sweep", "sweep/topic", 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "sweep/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	var drainDur string
	for dur := range sess.drainByDur {
		drainDur = dur
	}
	sess.mu.Unlock()
	if drainDur == "" {
		t.Fatal("expected a draining durable")
	}

	asm := testMQTTGetGlobalASM(t, s)
	asm.sweepOrphanedConsumers(s)

	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur); err != nil || ci == nil {
		t.Fatalf("the orphan sweep deleted the drained durable %q (err=%v); its started deliveries went with it", drainDur, err)
	}
	// And the delivery it was holding still completes.
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "sweep/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)
}

// TestMQTTDrainDiscardsOfflineBacklog is test 24. A drain's durable keeps interest
// in its filter while the session is offline, so messages published meanwhile
// accumulate in the stream. None of them is owed to the client — it unsubscribed —
// so the reconnect must discard the backlog and deliver only what was started.
func TestMQTTDrainDiscardsOfflineBacklog(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "backlog", "backlog/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "backlog/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "backlogpub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	for i := 0; i < 3; i++ {
		testMQTTPublish(t, pc, pr, 1, false, false, "backlog/topic", uint16(10+i), []byte(fmt.Sprintf("offline-%d", i)))
	}

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "backlog", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// Only the started delivery is owed, and only its payload may appear.
	got := testMQTTCheckPubMsgNoAck(t, c2, r2, "backlog/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, got)
	testMQTTExpectNothing(t, r2)
}

// TestMQTTDrainLevelUpCoversBothConsumers is test 27. A "foo/#" subscription is
// two JetStream consumers, because "#" also matches the parent level "foo": the
// main durable filters foo.> and a companion filters foo. UNSUBSCRIBE removes one
// MQTT filter but must drain BOTH, or the started delivery on whichever consumer
// was overlooked is deleted with its durable.
func TestMQTTDrainLevelUpCoversBothConsumers(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "levelup"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "levelup/#", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "levelupub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	// One message per consumer: "levelup/bar" matches the main filter, bare
	// "levelup" matches the level-up companion.
	testMQTTPublish(t, pc, pr, 1, false, false, "levelup/bar", 1, []byte("child"))
	testMQTTPublish(t, pc, pr, 1, false, false, "levelup", 2, []byte("parent"))

	// Both arrive unacknowledged; the order between two consumers is not fixed.
	seen := map[string]uint16{}
	for i := 0; i < 2; i++ {
		_, pi, topic, payload := testMQTTReadPubPacket(t, r)
		seen[string(payload)] = pi
		if topic != "levelup/bar" && topic != "levelup" {
			t.Fatalf("unexpected topic %q", topic)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("expected one started delivery per consumer, got %v", seen)
	}

	sess := testMQTTResidentSession(t, s, "levelup")
	if n := testConsumerCount(sess); n != 2 {
		t.Fatalf("a \"#\" subscription should hold two consumers, got %d", n)
	}

	testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: "levelup/#"}})
	testMQTTFlush(t, c, nil, r)

	if n := testDrainCount(sess); n != 2 {
		t.Fatalf("both consumers of a level-up filter must drain, got %d", n)
	}
	if n := testConsumerCount(sess); n != 0 {
		t.Fatalf("a drained consumer must leave cons (I7), got %d", n)
	}

	// Both started deliveries still complete.
	for i := 0; i < 2; i++ {
		flags, pi, _, payload := testMQTTReadPubPacket(t, r)
		if flags&mqttPubFlagDup == 0 {
			t.Errorf("a redelivered started message must carry DUP, flags=%x", flags)
		}
		want, ok := seen[string(payload)]
		if !ok {
			t.Fatalf("redelivered an unexpected payload %q", payload)
		}
		if pi != want {
			t.Errorf("redelivered %q with identifier %v, want the original %v", payload, pi, want)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	}
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drains still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainSurvivesConsumerInactivityThreshold is test 25: with the broker's
// consumer_inactive_threshold set, a drain must still outlive an offline period
// LONGER than that threshold.
//
// An ordinary subscription's durable is created with that deletion timer, which is
// right for an abandoned subscription and fatal for a drain: it exists precisely to
// survive a disconnect, and reaping it takes the started deliveries with it,
// because the messages stream is interest-retained.
func TestMQTTDrainSurvivesConsumerInactivityThreshold(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	o.MQTT.ConsumerInactiveThreshold = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "inact", "inact/topic", 1)

	sess.mu.Lock()
	var subDur string
	for _, cc := range sess.cons {
		subDur = cc.Durable
	}
	sess.mu.Unlock()
	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, subDur); err != nil || ci == nil {
		t.Fatalf("lookupConsumer: %v", err)
	} else if ci.Config.InactiveThreshold == 0 {
		t.Fatal("this test needs a subscription durable that starts with an inactivity timer")
	}

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "inact/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	// The drain sheds the timer at commit.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ci, err := sess.jsa.lookupConsumer(mqttStreamName, subDur)
		if err != nil || ci == nil {
			return fmt.Errorf("the drained durable is gone: %v", err)
		}
		if ci.Config.InactiveThreshold != 0 {
			return fmt.Errorf("the drained durable still carries a %v inactivity timer", ci.Config.InactiveThreshold)
		}
		return nil
	})

	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	// Offline for comfortably longer than the threshold the durable was born with.
	time.Sleep(2 * time.Second)

	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, subDur); err != nil || ci == nil {
		t.Fatalf("the drained durable was reaped while offline (err=%v); its started delivery went with it", err)
	}

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "inact", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	got := testMQTTCheckPubMsgNoAck(t, c2, r2, "inact/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, got)
}

// TestMQTTDrainInactivityClearRetriedOnResume pins the retry: clearing the timer
// is best-effort at commit, so a resume must try again while it is still set.
//
// Without the retry a single failed clear (a JetStream hiccup during UNSUBSCRIBE)
// leaves the durable reapable for the rest of the session's life, and the failure
// is silent. The threshold is re-imposed out of band here, which is exactly the
// state a failed clear leaves behind.
func TestMQTTDrainInactivityClearRetriedOnResume(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	o.MQTT.ConsumerInactiveThreshold = time.Hour
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "inactretry", "inactretry/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "inactretry/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	var drainDur string
	var cfg ConsumerConfig
	for dur, d := range sess.drainByDur {
		drainDur, cfg = dur, *d.cc
	}
	sess.mu.Unlock()
	if drainDur == "" {
		t.Fatal("expected a draining durable")
	}

	// Put the timer back, as a failed clear would have left it, both on the durable
	// and in the drain the record is built from.
	cfg.InactiveThreshold = time.Hour
	if _, err := sess.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName, Config: cfg}); err != nil {
		t.Fatalf("re-imposing the inactivity timer: %v", err)
	}
	sess.mu.Lock()
	if d := sess.drainByDur[drainDur]; d != nil {
		d.cc.InactiveThreshold = time.Hour
	}
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persisting the un-cleared drain: %v", err)
	}

	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "inactretry", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur)
		if err != nil || ci == nil {
			return fmt.Errorf("the drained durable is gone: %v", err)
		}
		if ci.Config.InactiveThreshold != 0 {
			return fmt.Errorf("the resume did not retry the clear: still %v", ci.Config.InactiveThreshold)
		}
		return nil
	})

	got := testMQTTCheckPubMsgNoAck(t, c2, r2, "inactretry/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, got)
}

// TestMQTTDrainStageRollbackKeepsTheSubscription is test 19 (I4): staging a drain
// and removing the membership are one transaction. If the packet's save fails,
// NEITHER is persisted — the subscription is intact, its consumer is intact, and
// the started delivery still completes on it.
//
// The dangerous shape is the opposite: a half-applied UNSUBSCRIBE that has torn
// the membership down in memory but persisted nothing, leaving a filter that is
// neither subscribed nor draining.
func TestMQTTDrainStageRollbackKeepsTheSubscription(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "stagerb", props: mqttV5ConnPropsSessionExpiry(30)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "stagerb/topic", opts: 1}})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "stagerbpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "stagerb/topic", 1, []byte("payload"))

	_, pi := testMQTTReadPublishV5(t, r, "stagerb/topic", []byte("payload"))
	sess := testMQTTResidentSession(t, s, "stagerb")
	consBefore := testConsumerCount(sess)
	if consBefore != 1 {
		t.Fatalf("expected one consumer before the unsubscribe, got %d", consBefore)
	}

	restore := setMQTTTestSessSaveHook(func(*mqttSession) error { return errDrainTestSaveFailed })
	codes := testMQTTUnsubV5(t, c, r, 2, []string{"stagerb/topic"})
	restore()

	if len(codes) != 1 || codes[0] != mqttReasonImplementationSpecificError {
		t.Fatalf("expected UNSUBACK 0x83 when the packet cannot be persisted, got %v", codes)
	}
	if n := testDrainCount(sess); n != 0 {
		t.Fatalf("a rolled-back UNSUBSCRIBE must leave no drain, got %d", n)
	}
	if n := testConsumerCount(sess); n != consBefore {
		t.Fatalf("the subscription's consumer must survive a rolled-back UNSUBSCRIBE: %d -> %d", consBefore, n)
	}

	// The started delivery still completes on the intact subscription...
	qos, got := testMQTTReadPublishV5(t, r, "stagerb/topic", []byte("payload"))
	if qos != 1 {
		t.Errorf("the redelivery must keep its QoS, got %v", qos)
	}
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, got)

	// ...and the filter is still subscribed, so new messages arrive.
	testMQTTPublish(t, pc, pr, 1, false, false, "stagerb/topic", 2, []byte("still-subscribed"))
	_, next := testMQTTReadPublishV5(t, r, "stagerb/topic", []byte("still-subscribed"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, next)
}

// TestMQTTDrainCompletionSaveFailureLosesNothing is test 20 (I1): the ack to
// JetStream must never outrun the record.
//
// When the save that removes a claim fails, nothing may be acknowledged — the
// message stays pending and is redelivered. Acking first and persisting later
// would lose the message outright on any failure in between, which is the one
// outcome the drain exists to prevent.
func TestMQTTDrainCompletionSaveFailureLosesNothing(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "compfail", "compfail/topic", 1)
	_ = pi
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "compfail/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	// PUBACK while the record cannot be written: the completion must not stick.
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error { return errDrainTestSaveFailed })
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)

	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("a completion that could not be persisted must leave the drain claiming the message, got %d drains", n)
	}
	if claimed := testDrainClaims(sess); claimed != 1 {
		t.Fatalf("expected the sequence to stay claimed, got %d claims", claimed)
	}

	// Unblocked, the redelivery completes it and the drain tears down: redelivered,
	// not lost.
	restore()
	// The redelivery may carry a FRESH identifier: the PUBACK released the original
	// before the save was attempted, and a repeated QoS 1 delivery is permitted.
	// What must hold is that the message comes back rather than disappearing.
	flags, got, topic, payload := testMQTTReadPubPacket(t, conn.r)
	if topic != "compfail/topic" || string(payload) != "payload" {
		t.Fatalf("expected the unpersisted completion to be redelivered, got %q %q", topic, payload)
	}
	if flags&mqttPubFlagDup == 0 {
		t.Errorf("a redelivery of an already-transmitted message must carry DUP, flags=%x", flags)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainRestoredClaimRedeliveredAfterRestart is the QoS 1 half of test 16:
// across a broker restart the drain is rebuilt from the record alone, and the
// claim it carries is redelivered with DUP rather than treated as a fresh message
// (which the committed drain would discard).
//
// After a restart the in-memory tracking maps are empty, so the delivery path
// cannot tell a started delivery from a new one — only the record can, which is
// why the claim, not the memory, is the authority.
func TestMQTTDrainRestoredClaimRedeliveredAfterRestart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, _ := testDrainSetup(t, s, o, "restart1", "restart1/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "restart1/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "restart1", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// A fresh identifier is expected here (the documented packet-identifier
	// continuity deviation); what must hold is that the payload arrives, with DUP.
	flags, pi2, topic, payload := testMQTTReadPubPacket(t, r2)
	if topic != "restart1/topic" || string(payload) != "payload" {
		t.Fatalf("expected the started delivery to be redelivered, got %q %q", topic, payload)
	}
	if flags&mqttPubFlagDup == 0 {
		t.Errorf("a delivery that was transmitted before the restart must carry DUP, flags=%x", flags)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi2)

	sess2 := testMQTTResidentSession(t, s, "restart1")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
	testMQTTExpectNothing(t, r2)
}

// TestMQTTDrainDurablesDeletedOnSessionExpiry is the other half of test 21:
// expiry deletes drain durables too.
//
// Expiry does not run through the connection's teardown — it rebuilds a transient
// session from the persisted record and clears that — so a drain is only deleted
// if the record's Drains are restored onto that transient session. Miss it and the
// durable outlives the session it belonged to, holding interest in its filter
// forever, with the orphan sweep taught to treat drains as referenced and the
// record gone.
func TestMQTTDrainDurablesDeletedOnSessionExpiry(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, _ := testDrainSetup(t, s, o, "expiredrain", "expiredrain/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "expiredrain/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	var drainDur string
	for dur := range sess.drainByDur {
		drainDur = dur
	}
	idHash := sess.idHash
	sess.mu.Unlock()
	if drainDur == "" {
		t.Fatal("expected a draining durable")
	}

	// Disconnect with an already-elapsed Session Expiry Interval, then drive the
	// expiry path directly rather than waiting out the sweep interval.
	sess.mu.Lock()
	sess.expiryInterval = 1
	sess.mu.Unlock()
	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	asm := testMQTTGetGlobalASM(t, s)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		asm.expireSessionFromRecord(s, idHash)
		if ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur); err == nil && ci != nil {
			return fmt.Errorf("the drained durable %q outlived the expired session", drainDur)
		}
		return nil
	})
}

// TestMQTTDrainAdoptionRolledBackOnFailedSave pins that a re-SUBSCRIBE which
// adopts a drain and then cannot persist the packet puts the drain BACK.
//
// Adoption is not an ordinary membership change: it deletes the drain and moves
// its durable into cons. The consumer snapshot alone cannot undo that — restoring
// cons without the drain leaves a session holding NEITHER, while the record still
// carries the drain. The next save then erases a durable that is still owed
// started deliveries, and with the orphan sweep treating drains as referenced,
// nothing reaps it either.
func TestMQTTDrainAdoptionRolledBackOnFailedSave(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// v5, so a failed SUBSCRIBE reports 0x83 per filter instead of closing the
	// connection — which is what makes the rollback observable from the client.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "adoptrb", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "adoptrb/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "adoptrbpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "adoptrb/topic", 1, []byte("payload"))
	testMQTTReadPublishV5(t, r, "adoptrb/topic", []byte("payload"))

	sess := testMQTTResidentSession(t, s, "adoptrb")
	if codes := testMQTTUnsubV5(t, c, r, 2, []string{"adoptrb/topic"}); len(codes) != 1 || codes[0] != mqttReasonSuccess {
		t.Fatalf("UNSUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	sess.mu.Lock()
	var drainDur string
	for dur := range sess.drainByDur {
		drainDur = dur
	}
	sess.mu.Unlock()

	// The save must fail AFTER the adoption, or the packet never reaches the
	// rollback and this test proves nothing. Adoption is observable: it is the
	// moment the drain leaves drainByDur.
	var adopted atomic.Bool
	restore := setMQTTTestSessSaveHook(func(sp *mqttSession) error {
		sp.mu.Lock()
		_, stillDraining := sp.drainByDur[drainDur]
		sp.mu.Unlock()
		if stillDraining {
			return nil // pre-adoption saves proceed
		}
		adopted.Store(true)
		return errDrainTestSaveFailed
	})
	// The SUBACK is deliberately not read: a SUBSCRIBE whose rolled-back record
	// cannot be confirmed closes the connection (the rollback path says so), so the
	// observable outcome is the session state, not a reason code.
	testDrainSendSubscribeV5(t, c, 3, "adoptrb/topic", 1)
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !adopted.Load() {
			return errors.New("the packet has not reached a post-adoption save yet")
		}
		return nil
	})
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if testDrainCount(sess) != 1 {
			return fmt.Errorf("the drain has not been restored yet (drains=%d cons=%d)",
				testDrainCount(sess), testConsumerCount(sess))
		}
		return nil
	})
	restore()

	sess.mu.Lock()
	_, backAsDrain := sess.drainByDur[drainDur]
	inCons := false
	for _, cc := range sess.cons {
		if cc.Durable == drainDur {
			inCons = true
		}
	}
	sess.mu.Unlock()
	if !backAsDrain {
		t.Fatal("a rolled-back adoption must put the drain back: the record still carries it, and the next save would erase its durable")
	}
	if inCons {
		t.Fatal("a rolled-back adoption must not leave the durable in cons as well (I7)")
	}
	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur); err != nil || ci == nil {
		t.Fatalf("the drained durable must survive a rolled-back adoption (err=%v)", err)
	}
}

// testDrainSendSubscribeV5 writes a v5 SUBSCRIBE without reading its SUBACK, for
// the cases where the broker answers by closing the connection.
func testDrainSendSubscribeV5(t *testing.T, c net.Conn, pi uint16, filter string, opts byte) {
	t.Helper()
	vh := newMQTTWriter(0)
	vh.WriteUint16(pi)
	vh.WriteVarInt(0) // no properties
	vh.WriteBytes([]byte(filter))
	vh.WriteByte(opts)
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("sending SUBSCRIBE: %v", err)
	}
}

// TestMQTTSubscribeAmbiguousSaveIsNotRolledBack drives a real SUBSCRIBE whose
// final record write MAY have landed, and pins that the packet is NOT undone.
//
// rollbackSubscribePacket restores the pre-packet membership and consumer maps and
// re-persists them, then tears down what the packet created. If the failed write
// actually landed, that discards state the record now holds, and the consumers the
// stored record still names have already been deleted. Which way memory reconciles is
// the readback's to decide, so rolling back at failure time commits to "it did not
// land" at the one moment nobody can know that.
func TestMQTTSubscribeAmbiguousSaveIsNotRolledBack(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambsub", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	// One confirmed subscription first, so the session exists and is resident.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ambsub/first", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTResidentSession(t, s, "ambsub")

	// The next packet's record write lands but its response is lost. The readback is
	// held off as well: every ambiguous path now resolves immediately (see
	// resolveUncertainNow), so without this the session would settle before the
	// retained state could be observed.
	var suppressRead atomicOnceBool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressRead.get() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	defer restoreRead()
	suppressRead.set(true)
	var once atomicOnce
	restore := setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err == nil && once.fire() {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	testDrainSendSubscribeV5(t, c, 2, "ambsub/second", 1)

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return errors.New("the ambiguous write has not been recorded yet")
		}
		return nil
	})
	restore()

	// sess.subs is keyed by the NATS subject form of the filter, not the MQTT one.
	sess.mu.Lock()
	_, kept := sess.subs["ambsub.second"]
	var dur string
	for sid, cc := range sess.cons {
		if strings.Contains(sid, "ambsub.second") {
			dur = cc.Durable
		}
	}
	nsubs := len(sess.subs)
	sess.mu.Unlock()

	if !kept {
		t.Fatalf("the attempted membership was rolled back after an ambiguous write (subs=%d); "+
			"the record may already carry it, and only the readback can decide how memory reconciles", nsubs)
	}
	if dur == "" {
		t.Fatal("the packet's consumer entry was removed after an ambiguous write")
	}
	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, dur); err != nil || ci == nil {
		t.Fatalf("the packet's consumer was torn down after an ambiguous write (err=%v); "+
			"the stored record may still name it", err)
	}
}

// atomicOnce fires exactly once.
type atomicOnce struct{ done atomic.Bool }

func (o *atomicOnce) fire() bool { return o.done.CompareAndSwap(false, true) }

// fired reports whether the one-shot has already been consumed, for a test that must
// prove its injection actually ran rather than assume it.
func (o *atomicOnce) fired() bool { return o.done.Load() }

// TestMQTTSharedSubscribeAmbiguousSaveAbandonsThePacket pins that an ambiguous
// EARLY shared-membership write stops the packet instead of marking the filter
// failed and carrying on.
//
// Carrying on is doubly wrong. The final update() skips filters marked failed, so
// the client would be told the subscription failed while the membership is durably
// stored — and the rest of the packet would run against a session whose record
// state is unknown, where every later save is refused anyway. The attempted state
// must survive for the readback to recognise.
func TestMQTTSharedSubscribeAmbiguousSaveAbandonsThePacket(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambshared", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ambshared/first", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTResidentSession(t, s, "ambshared")

	// The shared filter's EARLY membership write lands but its response is lost.
	var suppressRead atomicOnceBool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressRead.get() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	defer restoreRead()
	suppressRead.set(true)
	var once atomicOnce
	restore := setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err == nil && once.fire() {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	testDrainSendSubscribeV5(t, c, 2, "$share/g1/ambshared/shared", 1)

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return errors.New("the ambiguous write has not been recorded yet")
		}
		return nil
	})
	restore()

	sess.mu.Lock()
	shared := len(sess.sharedSubs)
	sess.mu.Unlock()
	if shared != 1 {
		t.Fatalf("the attempted shared membership must survive an ambiguous write, got %d: "+
			"the record may already carry it, and only the readback can decide how memory reconciles", shared)
	}

	// The packet was abandoned, so no SUBACK claiming failure is sent: the
	// connection closes instead.
	testDrainWaitConnClosedT(t, r)
}

// TestMQTTForcedSharedLeaveAmbiguousSaveKeepsTheRemoval pins the same rule on the
// 3.1.1 resume path, which force-leaves shared subscriptions a 3.1.1 client can
// neither observe nor UNSUBSCRIBE.
//
// If the removal landed and its response was lost, restoring the memberships in
// memory means the readback adopts the stored (empty) record while the session
// still holds them — and the next save resurrects a phantom holder that no
// UNSUBSCRIBE can ever clear.
func TestMQTTForcedSharedLeaveAmbiguousSaveKeepsTheRemoval(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// A v5 client establishes the shared membership...
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "forcedleave", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/g1/forcedleave/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTResidentSession(t, s, "forcedleave")
	sess.mu.Lock()
	sharedBefore := len(sess.sharedSubs)
	sess.mu.Unlock()
	if sharedBefore != 1 {
		t.Fatalf("expected one shared membership, got %d", sharedBefore)
	}
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// ...and a 3.1.1 resume force-leaves it, with the write's response lost.
	// Target the FORCED-LEAVE write specifically: it is the one whose record no
	// longer carries any shared membership. Firing on the first write of the
	// CONNECT would instead make an earlier save ambiguous, and this branch would
	// never be exercised.
	var suppressRead atomicOnceBool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressRead.get() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	defer restoreRead()
	suppressRead.set(true)
	var once atomicOnce
	restore := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err != nil {
			return resp, err
		}
		sp.mu.Lock()
		leaving := len(sp.sharedSubs) == 0
		sp.mu.Unlock()
		if leaving && once.fire() {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	// The CONNECT is expected to be refused (the record write failed), so the
	// helper's CONNACK read would just report EOF; what matters is the session
	// state the attempt leaves behind.
	c2, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "forcedleave", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 0)
	if c2 != nil {
		defer c2.Close()
	}
	_ = cerr
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return errors.New("the ambiguous write has not been recorded yet")
		}
		return nil
	})
	restore()

	sess.mu.Lock()
	sharedAfter := len(sess.sharedSubs)
	sess.mu.Unlock()
	if sharedAfter != 0 {
		t.Fatalf("the forced leave was reverted after an ambiguous write (%d membership(s) restored): "+
			"the record may already omit them, and a later save would resurrect a holder no UNSUBSCRIBE can clear", sharedAfter)
	}
}

// testDrainWaitConnClosedT waits for the broker to close the connection.
func testDrainWaitConnClosedT(t *testing.T, r *mqttReader) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var buf [256]byte
	for time.Now().Before(deadline) {
		if err := r.reader.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		if _, err := r.reader.Read(buf[:]); err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			r.reader.SetReadDeadline(time.Time{}) //nolint:errcheck
			return
		}
	}
	t.Fatal("the broker never closed the connection: the packet was not abandoned")
}

// TestMQTTUnsubAmbiguousLandedSaveStillCommits is finding 2's clearest case: the
// UNSUBSCRIBE's record write LANDS but its response is lost.
//
// Everything the packet does after that save — committing the staged drains,
// clearing their inactivity timers, releasing shared durables, dropping the
// client-facing subscriptions — is skipped when the save reports an error. Without a
// landed continuation the record says "unsubscribed" while the runtime still holds
// the subscription and the drain is still STAGED, so a fresh message on the drained
// filter is left to AckWait instead of being discarded, and nothing will ever finish
// the packet.
func TestMQTTUnsubAmbiguousLandedSaveStillCommits(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// v5, so a failed UNSUBSCRIBE reports a reason code instead of closing the
	// connection — the state has to be inspectable afterwards.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unsublanded", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "unsublanded/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "unsublandedpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "unsublanded/topic", 1, []byte("payload"))
	_, pi := testMQTTReadPublishV5(t, r, "unsublanded/topic", []byte("payload"))

	sess := testMQTTResidentSession(t, s, "unsublanded")

	// The UNSUBSCRIBE's write lands; only its response is replaced by an error.
	// Identified by what it is about to store: a STAGED drain exists only between
	// stageDrain and this packet's confirming save, so no earlier write can match.
	var once atomicOnce
	restore := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err != nil {
			return resp, err
		}
		sp.mu.Lock()
		var staged bool
		for _, d := range sp.drainByDur {
			staged = staged || d.staged
		}
		sp.mu.Unlock()
		if staged && once.fire() {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	codes := testMQTTUnsubV5(t, c, r, 2, []string{"unsublanded/topic"})
	if len(codes) != 1 {
		t.Fatalf("UNSUBACK = %v", codes)
	}

	// Production must resolve this itself: the client is still connected, so the
	// periodic pass would skip the session.
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		if sess.isSeqUncertain() {
			return errors.New("still unresolved: nothing in production is retrying the readback")
		}
		return nil
	})
	restore()

	// The landed continuation ran: the drain is COMMITTED, not staged.
	sess.mu.Lock()
	var staged bool
	nd := len(sess.drainByDur)
	for _, d := range sess.drainByDur {
		staged = staged || d.staged
	}
	nsubs := len(sess.subs)
	sess.mu.Unlock()
	if nd != 1 {
		t.Fatalf("expected the drain to survive, got %d (subs=%d)", nd, nsubs)
	}
	if staged {
		t.Fatal("the record says unsubscribed but the drain is still STAGED: the landed continuation never ran, " +
			"so fresh messages on the drained filter are left to AckWait instead of being discarded")
	}
	if nsubs != 0 {
		t.Fatalf("the record says unsubscribed but %d membership(s) remain in memory", nsubs)
	}

	// And the started delivery still completes, which is the drain's whole purpose.
	_, got := testMQTTReadPublishV5(t, r, "unsublanded/topic", []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, got)
}

// TestMQTTSubscribeAmbiguousIsSettledForwardE2E drives the REAL SUBSCRIBE path for a
// write that never reaches the stream.
//
// The packet's state is KEPT — the record may already hold it — and what settles the
// question is the resolver re-attempting the attempt's exact bytes, not a readback
// concluding from the base sequence that the write died. So the membership and the
// consumer this packet created both stand, and the record ends up naming them: no
// SUBACK was sent, but a durable subscription the client was never acknowledged for
// is exactly what a landed original with a lost response leaves behind.
//
// The fixture waits for the ambiguity BEFORE lifting suppression. An earlier version
// polled for the absence of the packet's state and was satisfied by the first sample,
// which the broker had not processed yet — it passed without the packet ever failing.
//
// Mutation M1: revert the at-base arm to clearing the uncertainty and the packet is
// undone instead: the membership disappears and its durable is deleted.
func TestMQTTSubscribeAmbiguousIsSettledForwardE2E(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "subunwind", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	// One confirmed subscription, so there is a pre-packet state to compare against.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "subunwind/first", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTResidentSession(t, s, "subunwind")

	// The DURABLES on the stream, not the in-memory map: only an actual deletion
	// distinguishes a settled packet from an undone one.
	dursBefore := testDrainStreamDurables(t, sess)

	// The next packet's record write never reaches the stream. Suppression stays on
	// until the ambiguity is REAL, or the packet under test never fails at all.
	mqttTestSessStoreSuppressV.Store(true)
	testDrainSendSubscribeV5(t, c, 2, "subunwind/second", 1)
	checkFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return errors.New("the packet's write has not gone ambiguous yet")
		}
		return nil
	})
	mqttTestSessStoreSuppressV.Store(false)

	// The connection is closed by the failed packet, so the resolution comes from the
	// pass over resident sessions — driven here rather than waited on.
	asm := testMQTTGetGlobalASM(t, s)
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		asm.reconcileResidentRecordState(s)
		if sess.isSeqUncertain() {
			return errors.New("still unresolved")
		}
		return nil
	})

	// Settled forward: the packet's membership stands, and the consumer it created is
	// still there — referenced by the record the re-attempt made durable.
	sess.mu.Lock()
	_, second := sess.subs["subunwind.second"]
	_, first := sess.subs["subunwind.first"]
	sess.mu.Unlock()
	if !second {
		t.Fatal("the packet's membership was undone: an ambiguous write is settled by making its bytes durable, not by rolling them back")
	}
	if !first {
		t.Fatal("the pre-packet subscription must survive")
	}
	if extra := testDrainNewDurables(t, sess, dursBefore); len(extra) == 0 {
		t.Fatal("the packet's durable was deleted although the record names its subscription")
	}
	// The record itself, which is what a restart would read.
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	smsg, err := sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil {
		t.Fatalf("loading the settled record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
		t.Fatalf("unmarshal the settled record: %v", uerr)
	}
	if _, durable := ps.Subs["subunwind.second"]; !durable {
		t.Fatalf("the settled record must carry the packet's subscription, got %v", ps.Subs)
	}
}

// testDrainStreamDurables snapshots the consumer names on the messages stream.
func testDrainStreamDurables(t *testing.T, sess *mqttSession) map[string]bool {
	t.Helper()
	cis, err := sess.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	out := make(map[string]bool, len(cis))
	for _, ci := range cis {
		if ci != nil {
			out[ci.Name] = true
		}
	}
	return out
}

// testDrainNewDurables lists consumers that appeared since the snapshot.
func testDrainNewDurables(t *testing.T, sess *mqttSession, before map[string]bool) []string {
	t.Helper()
	var extra []string
	for name := range testDrainStreamDurables(t, sess) {
		if !before[name] {
			extra = append(extra, name)
		}
	}
	return extra
}
