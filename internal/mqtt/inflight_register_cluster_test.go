//go:build mqttport_fulltests

// Replicated-JetStream (R3) tests for outbound in-flight identifier continuity,
// running against a genuine 3-node embedded cluster (see
// jetstream_cluster_harness_test.go).
//
// The restart tests in inflight_register_e2e_test.go establish the mechanism, but
// they replace one broker with another over the same store. What they cannot
// establish is the case the roadmap item is actually about: a client resuming on a
// DIFFERENT broker that never saw the delivery start, with the durable state reached
// over a replicated stream rather than a single-node one. Broker B here has none of
// A's memory — no cpending, no pendingPublish — so the identifier it re-offers can
// only have come from `$MQTT_inflight`.

package mqtt

import (
	"testing"
	"time"
)

// TestMQTTInflightReplicatedStream is always run: it asserts the register stream
// really is Replicas=3, current on every node, and carries the configuration the
// protocol needs. Without this the failover tests below could pass against a
// single-replica stream and prove nothing about HA.
func TestMQTTInflightReplicatedStream(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)
	defer b.cleanup()

	c.waitStreamCurrent(t, testGlobalAccount, mqttInflightStreamName)

	asm := b.s.mqtt.sessmgr.sessions[globalAccountName]
	si, err := asm.jsa.lookupStream(mqttInflightStreamName)
	if err != nil {
		t.Fatalf("lookup %s: %v", mqttInflightStreamName, err)
	}
	if si.Config.Replicas != 3 {
		t.Fatalf("%s has %d replica(s), want 3: the failover tests would prove nothing",
			mqttInflightStreamName, si.Config.Replicas)
	}
	if err := validateInflightStream(si, 3, asm.sessStreamEpoch); err != nil {
		t.Fatalf("%s does not carry the required configuration: %v", mqttInflightStreamName, err)
	}
}

// A started QoS 1 delivery resumed on ANOTHER broker keeps its packet identifier.
//
// Mutation check: skip the recoverInflightRegisters call in mqttProcessConnect and
// broker B re-offers under identifier 1 instead of the original.
func TestMQTTInflightQoS1IdentifierAcrossBrokers(t *testing.T) {
	testInflightAcrossBrokers(t, 1)
}

// The same for QoS 2, stopped before PUBREC, and the exchange then completes on the
// broker that never saw it start.
func TestMQTTInflightQoS2IdentifierAcrossBrokers(t *testing.T) {
	testInflightAcrossBrokers(t, 2)
}

func testInflightAcrossBrokers(t *testing.T, qos byte) {
	t.Helper()
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	shortAckWait := func(o *Options) { o.MQTT.AckWait = 2 * time.Second }
	a := c.startBroker(t, 0, shortAckWait)
	defer a.cleanup()
	b := c.startBroker(t, 1, shortAckWait)
	defer b.cleanup()

	const clientID = "inflight-failover"
	pubFlags := mqttPubQos1
	if qos == 2 {
		pubFlags = mqttPubQoS2
	}

	// Subscribe on broker A.
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "foo", qos: qos}}, []byte{qos})
	testMQTTFlush(t, sub, nil, subr)

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "inflight-failover-pub", cleanSess: true},
		a.opts.MQTT.Host, a.opts.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)

	// Burn two identifiers on completed exchanges, so the one under test is not the
	// value a fresh allocator would hand out anyway (see the single-broker tests).
	for _, payload := range []string{"one", "two"} {
		testMQTTPublish(t, pub, pubr, qos, false, false, "foo", 1, []byte(payload))
		pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", pubFlags, []byte(payload))
		if qos == 1 {
			testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
		} else {
			testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
			testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
			testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
		}
		testMQTTFlush(t, sub, nil, subr)
	}

	// The delivery under test: taken on A, never acknowledged.
	testMQTTPublish(t, pub, pubr, qos, false, false, "foo", 1, []byte("payload"))
	firstPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", pubFlags, []byte("payload"))
	if firstPI <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce; the test would prove nothing", firstPI)
	}
	sub.Close()

	// Resume on broker B, which never saw any of this.
	sub2, sub2r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false},
		b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer sub2.Close()
	testMQTTCheckConnAck(t, sub2r, mqttConnAckRCConnectionAccepted, true)

	gotPI := testMQTTCheckPubMsgNoAck(t, sub2, sub2r, "foo", pubFlags|mqttPubFlagDup, []byte("payload"))
	if gotPI != firstPI {
		t.Fatalf("broker B re-offered the delivery under packet identifier %d, want the original %d", gotPI, firstPI)
	}

	// And it completes there, under that identifier.
	if qos == 1 {
		testMQTTSendPIPacket(mqttPacketPubAck, t, sub2, gotPI)
	} else {
		testMQTTSendPIPacket(mqttPacketPubRec, t, sub2, gotPI)
		testMQTTReadPIPacket(mqttPacketPubRel, t, sub2r, gotPI)
		testMQTTSendPIPacket(mqttPacketPubComp, t, sub2, gotPI)
	}
	testMQTTFlush(t, sub2, nil, sub2r)
}
