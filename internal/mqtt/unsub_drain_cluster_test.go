//go:build mqttport_fulltests

// Replicated-JetStream (R3) tests for the UNSUBSCRIBE drain, running against a
// genuine 3-node embedded cluster with TWO independent brokers (see
// jetstream_cluster_harness_test.go). The shared-store harness cannot express what
// matters here: a drain reconstructed by a broker that never saw the delivery, from
// the session record alone.

package mqtt

import (
	"fmt"
	"testing"
	"time"
)

// TestMQTTDrainClusterResumesOnAnotherBroker is plan test 28: a started QoS 2
// delivery survives UNSUBSCRIBE and is then completed by a DIFFERENT broker.
//
// Broker B has none of A's memory — no cpending, no pendingPublish, no phase — so
// everything it does here comes from the record: that the filter is unsubscribed,
// that a durable must nonetheless survive, and which stream sequence the client is
// still owed. That is the whole point of persisting the drain rather than keeping
// it in the connection.
func TestMQTTDrainClusterResumesOnAnotherBroker(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	// A short AckWait, because the drained delivery is redelivered by AckWait on
	// the surviving consumer: this is a resume, not a restart, so nothing resets
	// the consumer's delivery state.
	shortAckWait := func(o *Options) { o.MQTT.AckWait = time.Second }
	a := c.startBroker(t, 0, shortAckWait)
	b := c.startBroker(t, 1, shortAckWait)

	const clientID = "cldrain"
	const topic = "cldrain/topic"

	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(300)},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	defer ca.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, ra); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK on broker A: reason=%v", reason)
	}
	if codes := testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{{topic: topic, opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK on broker A: %v", codes)
	}
	testMQTTFlush(t, ca, nil, ra)

	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "pub", cleanStart: true},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	defer pc.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, pr); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK for the publisher: reason=%v", reason)
	}
	testMQTTPublishV5(t, pc, 2, 1, topic, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	// Delivered on A and left unacknowledged: this is the delivery [MQTT-3.10.4-3]
	// says must be completed.
	qos, piA := testMQTTReadPublishV5(t, ra, topic, []byte("payload"))
	if qos != 2 {
		t.Fatalf("delivered QoS=%v, want 2", qos)
	}

	if codes := testMQTTUnsubV5(t, ca, ra, 2, []string{topic}); len(codes) != 1 || codes[0] != mqttReasonSuccess {
		t.Fatalf("UNSUBACK on broker A: %v", codes)
	}
	testMQTTFlush(t, ca, nil, ra)

	sessA := testMQTTResidentSession(t, a.s, clientID)
	if n := testDrainCount(sessA); n != 1 {
		t.Fatalf("broker A should hold one draining consumer, got %d", n)
	}
	if n := testDrainClaims(sessA); n != 1 {
		t.Fatalf("broker A should claim exactly the started delivery, got %d", n)
	}

	testMQTTDisconnect(t, ca, nil)
	ca.Close()

	// Resume on B. Nothing of A's state is available to it.
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(300)},
		b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cb.Close()
	sp, reason, _ := testMQTTReadConnAckV5(t, rb)
	if reason != mqttReasonSuccess {
		t.Fatalf("CONNACK on broker B: reason=%v", reason)
	}
	if !sp {
		t.Fatal("broker B must report Session Present: it is resuming a persisted session, not starting one")
	}

	// B redelivers the claim and completes the exchange. The identifier is B's own
	// (the documented packet-identifier continuity deviation across a failover).
	qos, piB := testMQTTReadPublishV5(t, rb, topic, []byte("payload"))
	if qos != 2 {
		t.Fatalf("broker B delivered QoS=%v, want 2", qos)
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, cb, piB)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rb, piB)
	testMQTTSendPIPacket(mqttPacketPubComp, t, cb, piB)
	testMQTTFlush(t, cb, nil, rb)
	_ = piA

	sessB := testMQTTResidentSession(t, b.s, clientID)
	checkFor(t, testMQTTTimeout, 50*time.Millisecond, func() error {
		if n := testDrainCount(sessB); n != 0 {
			return fmt.Errorf("broker B still holds %d drain(s) after the exchange completed", n)
		}
		return nil
	})

	// The filter is NOT resubscribed by the resume: a drain is not a membership, so
	// a message published now must not reach the client.
	testMQTTPublishV5(t, pc, 1, 2, topic, []byte("after"))
	testMQTTReadPubAck(t, pr, 2)
	testMQTTExpectNothingFor(t, rb, 2*time.Second)
}

// TestMQTTDrainClusterPubRecToPubCompAcrossBrokers is plan test 17(i): a QoS 2
// exchange that is past PUBREC on broker A is completed by broker B under the
// ORIGINAL packet identifier.
//
// This is the guarantee the durable intent buys. Everywhere else across a failover
// a delivery is re-offered with a fresh identifier (the documented
// packet-identifier continuity deviation), because nothing durable ties an
// in-flight PUBLISH to its identifier. Past the intent there is such a record, so
// the second phase completes as the same exchange rather than restarting as a new
// one — and the client, which is still holding that identifier, can PUBCOMP it.
func TestMQTTDrainClusterPubRecToPubCompAcrossBrokers(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	shortAckWait := func(o *Options) { o.MQTT.AckWait = time.Second }
	a := c.startBroker(t, 0, shortAckWait)
	b := c.startBroker(t, 1, shortAckWait)

	const clientID = "clq2"
	const topic = "clq2/topic"

	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(300)},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	defer ca.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, ra); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK on broker A: reason=%v", reason)
	}
	if codes := testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{{topic: topic, opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK on broker A: %v", codes)
	}
	testMQTTFlush(t, ca, nil, ra)

	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "pub", cleanStart: true},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	defer pc.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, pr); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK for the publisher: reason=%v", reason)
	}
	testMQTTPublishV5(t, pc, 2, 1, topic, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	qos, piA := testMQTTReadPublishV5(t, ra, topic, []byte("payload"))
	if qos != 2 {
		t.Fatalf("delivered QoS=%v, want 2", qos)
	}

	if codes := testMQTTUnsubV5(t, ca, ra, 2, []string{topic}); len(codes) != 1 || codes[0] != mqttReasonSuccess {
		t.Fatalf("UNSUBACK on broker A: %v", codes)
	}
	testMQTTFlush(t, ca, nil, ra)

	// Move past PUBREC on A: the intent is durable and the PUBREL is stored. The
	// client deliberately does NOT PUBCOMP, so the exchange is left half-finished.
	testMQTTSendPIPacket(mqttPacketPubRec, t, ca, piA)
	testMQTTReadPIPacket(mqttPacketPubRel, t, ra, piA)

	sessA := testMQTTResidentSession(t, a.s, clientID)
	checkFor(t, testMQTTTimeout, 50*time.Millisecond, func() error {
		if n := testDrainClaims(sessA); n != 0 {
			return fmt.Errorf("broker A still claims %d source(s) after a confirmed PUBREL", n)
		}
		return nil
	})

	testMQTTDisconnect(t, ca, nil)
	ca.Close()

	// B has never seen this exchange. It must replay the stored PUBREL under the
	// SAME identifier the client is holding.
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(300)},
		b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cb.Close()
	sp, reason, _ := testMQTTReadConnAckV5(t, rb)
	if reason != mqttReasonSuccess {
		t.Fatalf("CONNACK on broker B: reason=%v", reason)
	}
	if !sp {
		t.Fatal("broker B must report Session Present")
	}

	testMQTTReadPIPacket(mqttPacketPubRel, t, rb, piA)
	testMQTTSendPIPacket(mqttPacketPubComp, t, cb, piA)
	testMQTTFlush(t, cb, nil, rb)

	// The payload is never resent: the exchange was past PUBREC before the move.
	testMQTTExpectNothingFor(t, rb, 2*time.Second)
}

// TestMQTTDrainClusterLateForeignPubRelStore is plan test 17(f): broker A's PUBREL
// store lands AFTER broker B has taken the session over and recreated the same
// PUBREL from the intent.
//
// The intent's CAS orders durable state between owners, but it cannot stop a
// superseded broker from completing a store it had already issued. Two records for
// one exchange would mean the client either sees a second PUBREL or — worse — one
// record redelivers forever because nothing ever acks it. What makes that safe is
// the deterministic Nats-Msg-Id derived from the exchange identity plus attempt,
// which collapses the duplicate, backed by a delivery-side rule that acks a
// duplicate copy instead of forwarding it.
func TestMQTTDrainClusterLateForeignPubRelStore(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	shortAckWait := func(o *Options) { o.MQTT.AckWait = time.Second }
	a := c.startBroker(t, 0, shortAckWait)
	b := c.startBroker(t, 1, shortAckWait)

	const clientID = "cllate"
	const topic = "cllate/topic"

	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(300)},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	defer ca.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, ra); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK on broker A: reason=%v", reason)
	}
	if codes := testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{{topic: topic, opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK on broker A: %v", codes)
	}
	testMQTTFlush(t, ca, nil, ra)

	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "pub", cleanStart: true},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	defer pc.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, pr); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK for the publisher: reason=%v", reason)
	}
	testMQTTPublishV5(t, pc, 2, 1, topic, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	if qos, _ := testMQTTReadPublishV5(t, ra, topic, []byte("payload")); qos != 2 {
		t.Fatalf("delivered QoS=%v, want 2", qos)
	}
	if codes := testMQTTUnsubV5(t, ca, ra, 2, []string{topic}); len(codes) != 1 || codes[0] != mqttReasonSuccess {
		t.Fatalf("UNSUBACK on broker A: %v", codes)
	}
	testMQTTFlush(t, ca, nil, ra)

	sessA := testMQTTResidentSession(t, a.s, clientID)
	sessA.mu.Lock()
	var dur string
	var sseq uint64
	var claimPI uint16
	for d, dr := range sessA.drainByDur {
		for seq, st := range dr.seqs {
			dur, sseq, claimPI = d, seq, st.pi
		}
	}
	idHash, pubRelSubject := sessA.idHash, sessA.pubRelSubject
	sessA.mu.Unlock()
	if dur == "" {
		t.Fatal("broker A should hold a draining consumer with a claimed sequence")
	}

	// Stand in for A: persist the intent as A would have, then leave A's store
	// un-issued. A real pause-and-release needs a seam inside another broker's
	// process; writing the same record A would have written is the same fact.
	if _, definite, err := sessA.recordPubRelIntent(nil, dur, sseq, claimPI); err != nil {
		t.Fatalf("recording the intent (definite=%v): %v", definite, err)
	}
	testMQTTDisconnect(t, ca, nil)
	ca.Close()

	// B takes over, finds the intent with no record, and recreates the PUBREL.
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(300)},
		b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cb.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, rb); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK on broker B: reason=%v", reason)
	}
	testMQTTReadPIPacket(mqttPacketPubRel, t, rb, claimPI)

	// NOW release A's store: the superseded broker completes the write it had
	// already begun, under the identity the exchange defines.
	msgId := mqttPubRelMsgId(idHash, dur, sseq, 0)
	natsMsg, headerLen := mqttNewCorrelatedPubRel(claimPI, dur, sseq, msgId)
	if _, err := sessA.jsa.storeMsg(pubRelSubject, headerLen, natsMsg); err != nil {
		t.Fatalf("A's late store: %v", err)
	}

	// Exactly one PUBREL reaches the client, and PUBCOMP completes the exchange.
	testMQTTSendPIPacket(mqttPacketPubComp, t, cb, claimPI)
	testMQTTFlush(t, cb, nil, rb)
	testMQTTExpectNothingFor(t, rb, 3*time.Second)
}

// TestMQTTDrainClusterDefiniteIntentFailureResumesElsewhere is the cross-broker
// half of plan test 17(e): an intent write that definitely did not land must leave
// the exchange restartable on ANOTHER broker, not only on the one that failed.
//
// The same-broker case is carried by the in-memory rollback, because a reconnect
// reuses the session object. Broker B has no such help: everything it does comes
// from the record, so this is what proves the rollback was not merely cosmetic —
// that the failed intent left no trace which would make B recreate a PUBREL for an
// exchange the client never PUBREC'd durably, or withhold a payload it is owed.
func TestMQTTDrainClusterDefiniteIntentFailureResumesElsewhere(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	shortAckWait := func(o *Options) { o.MQTT.AckWait = time.Second }
	a := c.startBroker(t, 0, shortAckWait)
	b := c.startBroker(t, 1, shortAckWait)

	const clientID = "cldefinite"
	const topic = "cldefinite/topic"

	ca, ra := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(300)},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	defer ca.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, ra); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK on broker A: reason=%v", reason)
	}
	if codes := testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{{topic: topic, opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK on broker A: %v", codes)
	}
	testMQTTFlush(t, ca, nil, ra)

	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "pub", cleanStart: true},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	defer pc.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, pr); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK for the publisher: reason=%v", reason)
	}
	testMQTTPublishV5(t, pc, 2, 1, topic, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	if qos, _ := testMQTTReadPublishV5(t, ra, topic, []byte("payload")); qos != 2 {
		t.Fatalf("delivered QoS=%v, want 2", qos)
	}
	piA := uint16(0)
	sessA := testMQTTResidentSession(t, a.s, clientID)
	sessA.mu.Lock()
	for p := range sessA.pendingPublish {
		piA = p
	}
	sessA.mu.Unlock()
	if piA == 0 {
		t.Fatal("expected an in-flight delivery on broker A")
	}

	if codes := testMQTTUnsubV5(t, ca, ra, 2, []string{topic}); len(codes) != 1 || codes[0] != mqttReasonSuccess {
		t.Fatalf("UNSUBACK on broker A: %v", codes)
	}
	testMQTTFlush(t, ca, nil, ra)
	if n := testDrainClaims(sessA); n != 1 {
		t.Fatalf("broker A should claim the started delivery, got %d", n)
	}

	// The intent write is refused outright. Nothing is stored, so nothing about
	// this PUBREC may survive — in memory or in the record.
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error { return errDrainTestSaveFailed })
	testMQTTSendPIPacket(mqttPacketPubRec, t, ca, piA)
	testDrainWaitConnClosed(t, ra)
	restore()
	ca.Close()

	sessA.mu.Lock()
	_, wedged := sessA.pendingPubRel[piA]
	sessA.mu.Unlock()
	if wedged {
		t.Fatalf("identifier %v is wedged on broker A", piA)
	}

	// Broker B rebuilds from the record alone. No intent means the exchange never
	// got past PUBREC durably, so the payload is re-offered and completes.
	cb, rb := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(300)},
		b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cb.Close()
	sp, reason, _ := testMQTTReadConnAckV5(t, rb)
	if reason != mqttReasonSuccess {
		t.Fatalf("CONNACK on broker B: reason=%v", reason)
	}
	if !sp {
		t.Fatal("broker B must report Session Present")
	}

	qos, piB := testMQTTReadPublishV5(t, rb, topic, []byte("payload"))
	if qos != 2 {
		t.Fatalf("broker B delivered QoS=%v, want 2", qos)
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, cb, piB)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rb, piB)
	testMQTTSendPIPacket(mqttPacketPubComp, t, cb, piB)
	testMQTTFlush(t, cb, nil, rb)

	sessB := testMQTTResidentSession(t, b.s, clientID)
	checkFor(t, testMQTTTimeout, 50*time.Millisecond, func() error {
		if n := testDrainCount(sessB); n != 0 {
			return fmt.Errorf("broker B still holds %d drain(s)", n)
		}
		return nil
	})
	testMQTTExpectNothingFor(t, rb, 2*time.Second)
}
