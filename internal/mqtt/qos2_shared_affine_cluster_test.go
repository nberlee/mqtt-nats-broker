//go:build mqttport_fulltests

// Replicated-JetStream (R3) tests for session-affine ownership of a started
// shared-subscription QoS 2 delivery, against a genuine 3-node embedded cluster
// (see jetstream_cluster_harness_test.go).
//
// These exist because the property being claimed is a DURABILITY property across
// BROKERS, and a single-process test cannot establish it: the whole point of the
// assignment claim is that a contender on a DIFFERENT broker, which shares no
// memory with the session that won, honors the winner. A same-process test can be
// satisfied by in-memory tracking that a second broker would never see.
//
// What they establish, precisely:
//
//   - TestQ2AffineReplicatedAssignmentStream (always-run): $MQTT_qos2assign really
//     is Replicas=3, current on every node, and satisfies the validator that gates
//     broker startup; $MQTT_out does too.
//   - TestQ2AffineCrossBrokerAffinity (always-run): a delivery started to a member
//     on one broker is never handed to a member on another broker when its owner
//     leaves, and completes through PUBCOMP when that owner returns.
//   - TestQ2AffineOwnerBrokerLossKeepsAssignment (gated on
//     MQTTNB_CLUSTER_FAILOVER_TESTS=1): losing the OWNER'S BROKER outright does not
//     release the message to the group; the owner collects it from a surviving
//     broker.
//
// What is deliberately NOT claimed: packet-identifier continuity across a broker
// loss. The assertions below are only that the message reaches the SAME session,
// is not redistributed, and is not lost — a redelivery with DUP set and a freshly
// allocated identifier satisfies them, which is the documented HA deviation (see
// docs/CONFORMANCE.md and the header of qos2_shared_affine.go). A test that
// asserted the identifier was preserved would be asserting more than the protocol
// provides.

package mqtt

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// testQ2AffClusterMember joins a v5 client to a shared group at QoS 2 on a given
// broker, with a Session Expiry Interval so a disconnect SUSPENDS the session
// rather than discarding it — a discarded session's assignment is legitimately
// terminal, which would make "the delivery waited for its owner" pass for the
// wrong reason.
func testQ2AffClusterMember(t *testing.T, b *testClusterBroker, clientID, group, topic string) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/" + group + "/" + topic, opts: 2}})
	if len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("Expected SUBACK [2] for %s, got %v", clientID, codes)
	}
	testMQTTFlush(t, c, nil, r)
	return c, r
}

// testQ2AffClusterResume reconnects a suspended session without re-subscribing —
// how a client comes back for a delivery started before it left.
func testQ2AffClusterResume(t *testing.T, b *testClusterBroker, clientID string) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	return c, r
}

// testQ2AffClusterPublish completes one inbound QoS 2 exchange. QoS 2 inbound is
// required: the effective delivery QoS is the minimum of publish and subscription
// QoS, so a QoS 1 publish never enters the affine path at all.
func testQ2AffClusterPublish(t *testing.T, b *testClusterBroker, clientID, topic, payload string) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	pi := uint16(1)
	testMQTTPublishV5(t, c, 2, pi, topic, []byte(payload))
	if reason, _ := testMQTTReadPubRecV5(t, r, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBREC reason 0x%x", reason)
	}
	testQoS2SendPubRel(t, c, pi)
	if reason, _ := testMQTTReadPubCompV5(t, r, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason 0x%x", reason)
	}
}

// testQ2AffClusterComplete finishes the outbound handshake for pi, tolerating the
// duplicate PUBLISH redeliveries a short AckWait produces while the test works.
func testQ2AffClusterComplete(t *testing.T, c net.Conn, r *mqttReader, pi uint16) {
	t.Helper()
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	deadline := time.Now().Add(testMQTTTimeout)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r, 2*time.Second)
		if !ready {
			continue
		}
		start := r.pos
		switch pb & mqttPacketMask {
		case mqttPacketPubRel:
			r.pos = start + pl
			testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
			return
		case mqttPacketPub:
			r.pos = start + pl // duplicate redelivery
		default:
			t.Fatalf("unexpected packet 0x%x while awaiting PUBREL", pb&mqttPacketMask)
		}
	}
	t.Fatal("no PUBREL arrived for a PUBREC'd QoS 2 delivery")
}

// testQ2AffClusterClaims reads every assignment claim through one broker.
func testQ2AffClusterClaims(t *testing.T, b *testClusterBroker) map[string]*mqttQ2AffClaim {
	t.Helper()
	asm := testMQTTGetGlobalASM(t, b.s)
	out := map[string]*mqttQ2AffClaim{}
	if err := asm.scanQ2AffClaims(func(subject string, claim *mqttQ2AffClaim, _ uint64) error {
		out[subject] = claim
		return nil
	}); err != nil {
		t.Fatalf("scanning assignment claims: %v", err)
	}
	return out
}

// TestQ2AffineReplicatedAssignmentStream asserts the durable state this protocol
// arbitrates with is genuinely replicated and genuinely conforming. It is the
// discriminator between real cluster-wide arbitration and a single-node store: a
// claim that lives on one node cannot stop a contender on another.
func TestQ2AffineReplicatedAssignmentStream(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	js, err := b.nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo(mqttQoS2AssignStreamName)
		if err != nil {
			return fmt.Errorf("stream info: %w", err)
		}
		if si.Config.Replicas != 3 {
			return fmt.Errorf("Config.Replicas=%d, want 3\n%s", si.Config.Replicas, c.describe())
		}
		if si.Cluster == nil || si.Cluster.Leader == "" || len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("cluster info incomplete: %+v\n%s", si.Cluster, c.describe())
		}
		return nil
	})
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2AssignStreamName)

	// Checked through the production validators rather than a restatement of
	// them: these are the same checks that refuse to start a broker.
	asm := testMQTTGetGlobalASM(t, b.s)
	si, err := asm.jsa.lookupStream(mqttQoS2AssignStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookup %s: si=%v err=%v", mqttQoS2AssignStreamName, si, err)
	}
	if verr := validateQoS2AssignStream(si, 3); verr != nil {
		t.Fatalf("the R3 assignment stream the broker created does not satisfy its own validator: %v", verr)
	}
	si, err = asm.jsa.lookupStream(mqttOutStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookup %s: si=%v err=%v", mqttOutStreamName, si, err)
	}
	if verr := validateOutStream(si, 3); verr != nil {
		t.Fatalf("the R3 outgoing stream the broker created does not satisfy its own validator: %v", verr)
	}

	// A real assignment must land on the replicated stream, and be gone once both
	// sides of the exchange are proven complete.
	cs, rs := testQ2AffClusterMember(t, b, "q2aff-rep", "repg", "q2aff/rep")
	defer cs.Close()
	testQ2AffClusterPublish(t, b, "q2aff-rep-pub", "q2aff/rep", "replicated")

	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rs, testMQTTTimeout)
	if !ok || qos != 2 || string(payload) != "replicated" {
		t.Fatalf("shared QoS 2 delivery: ok=%v qos=%d payload=%q", ok, qos, payload)
	}
	if n := len(testQ2AffClusterClaims(t, b)); n != 1 {
		t.Fatalf("expected exactly one assignment claim on the replicated stream, got %d", n)
	}
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2AssignStreamName)

	testQ2AffClusterComplete(t, cs, rs, pi)
	checkFor(t, testMQTTTimeout, 100*time.Millisecond, func() error {
		if n := len(testQ2AffClusterClaims(t, b)); n != 0 {
			return fmt.Errorf("%d assignment claims survived a completed exchange", n)
		}
		return nil
	})
}

// TestQ2AffineCrossBrokerAffinity is the property no single-process test can
// establish: the member that must NOT receive the message is served by a
// different broker, with its own memory, its own consumers and its own view of
// who is delivering what. The only thing tying the two together is the durable
// assignment claim.
//
// Before this change the started message was pending on the shared GROUP durable,
// so AckWait offered it to whichever member was reachable — very much including
// one on another broker.
func TestQ2AffineCrossBrokerAffinity(t *testing.T) {
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	const group, topic = "xbg", "q2aff/xb"
	ca, ra := testQ2AffClusterMember(t, bA, "xbA", group, topic)
	cb, rb := testQ2AffClusterMember(t, bB, "xbB", group, topic)
	defer cb.Close()

	testQ2AffClusterPublish(t, bA, "xb-pub", topic, "crossbroker")

	// Identify the owner by who the group actually delivered to. The probe uses a
	// short timeout on purpose: it is a "did THIS member get it" question, and the
	// loser must not burn the whole cluster timeout answering no.
	const probe = 5 * time.Second
	ownerID, ownerBroker, ownerConn, otherReader := "xbA", bA, ca, rb
	_, payload, qos, _, ok := testMQTTTryReadPublishV5(ra, probe)
	if !ok {
		_, payload, qos, _, ok = testMQTTTryReadPublishV5(rb, probe)
		if !ok {
			ca.Close()
			t.Fatal("neither member received the shared QoS 2 delivery")
		}
		ownerID, ownerBroker, ownerConn, otherReader = "xbB", bB, cb, ra
	}
	if qos != 2 || string(payload) != "crossbroker" {
		t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
	}

	// The claim must be readable from BOTH brokers and name the same winner:
	// arbitration that only one broker can see is not arbitration.
	claimsA, claimsB := testQ2AffClusterClaims(t, bA), testQ2AffClusterClaims(t, bB)
	if len(claimsA) != 1 || len(claimsB) != 1 {
		t.Fatalf("assignment visible from A: %d, from B: %d (want 1 each)", len(claimsA), len(claimsB))
	}
	for subj, ca := range claimsA {
		cbClaim, found := claimsB[subj]
		if !found {
			t.Fatalf("assignment %q is not visible from the other broker", subj)
		}
		if ca.Winner != cbClaim.Winner || ca.Winner != getHash(ownerID) {
			t.Fatalf("the two brokers disagree about the owner: %q vs %q (delivery went to %q)",
				ca.Winner, cbClaim.Winner, ownerID)
		}
	}

	// The owner leaves with the exchange unfinished.
	ownerConn.Close()

	// The member on the OTHER broker must never see it.
	if _, pl, _, _, got := testMQTTTryReadPublishV5(otherReader, 5*time.Second); got {
		t.Fatalf("a started shared QoS 2 delivery crossed to a member on another broker (payload %q)", pl)
	}

	// The owner returns to its own broker and completes what it started. The
	// identifier may be a fresh one (documented HA deviation); what is asserted is
	// that the SAME session gets the message and the exchange completes.
	cr, rr := testQ2AffClusterResume(t, ownerBroker, ownerID)
	defer cr.Close()
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rr, testMQTTTimeout)
	if !ok {
		t.Fatal("the owning session did not receive its started delivery on resume")
	}
	if qos != 2 || string(payload) != "crossbroker" {
		t.Fatalf("unexpected redelivery: payload=%q qos=%d", payload, qos)
	}
	testQ2AffClusterComplete(t, cr, rr, pi)
	testMQTTExpectNothing(t, otherReader)
}

// TestQ2AffineOwnerBrokerLossKeepsAssignment removes the owner's BROKER, not just
// its connection. Nothing in memory survives that, so the only thing that can
// stop the surviving broker's member from taking the message is the durable
// claim — and the only thing that can still deliver it to its owner is the
// generation-scoped consumer named in that claim.
//
// Gated, because it stops a broker mid-test and depends on cluster timing.
func TestQ2AffineOwnerBrokerLossKeepsAssignment(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 60 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	const group, topic = "lossg", "q2aff/loss"
	ca, ra := testQ2AffClusterMember(t, bA, "lossA", group, topic)
	cb, rb := testQ2AffClusterMember(t, bB, "lossB", group, topic)
	defer cb.Close()

	testQ2AffClusterPublish(t, bA, "loss-pub", topic, "brokerloss")

	// This test needs the member on bA to be the owner, because bA is the broker
	// it stops. Which member the group chooses is not ours to decide, so skip
	// rather than assert something the test did not set up.
	if _, _, _, _, ok := testMQTTTryReadPublishV5(ra, 5*time.Second); !ok {
		ca.Close()
		t.Skip("the group chose the member on the surviving broker; this case needs the other one")
	}
	if n := len(testQ2AffClusterClaims(t, bB)); n != 1 {
		t.Fatalf("expected the assignment to be visible from the surviving broker, got %d claims", n)
	}

	// Lose the owner's broker outright: connection, session memory and all.
	ca.Close()
	<-bA.s.BeginShutdown()

	// The surviving broker's member must not be offered the started delivery.
	if _, pl, _, _, got := testMQTTTryReadPublishV5(rb, 10*time.Second); got {
		t.Fatalf("losing the owner's broker released a started QoS 2 delivery to another member (payload %q)", pl)
	}

	// The owner reconnects through the SURVIVING broker and collects it. A fresh
	// packet identifier here is expected and permitted; loss or redistribution is
	// not.
	cr, rr := testQ2AffClusterResume(t, bB, "lossA")
	defer cr.Close()
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rr, testMQTTTimeout)
	if !ok {
		t.Fatal("the message was lost when its owner's broker was stopped")
	}
	if qos != 2 || string(payload) != "brokerloss" {
		t.Fatalf("unexpected delivery after broker loss: payload=%q qos=%d", payload, qos)
	}
	testQ2AffClusterComplete(t, cr, rr, pi)
	testMQTTExpectNothing(t, rb)
}
