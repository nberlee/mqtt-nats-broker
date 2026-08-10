//go:build mqttport_fulltests

// Replicated-JetStream (R3) tests for the delayed-Will outbox, running against a
// genuine 3-node embedded cluster (see jetstream_cluster_harness_test.go) rather
// than the shared-store two-broker simulation in will_outbox_test.go.
//
//   - TestMQTTWillOutboxReplicatedStream (always-run): proves $MQTT_willq is
//     really Replicas=3 and replicated to every node.
//   - TestMQTTWillOutboxReplicatedLeaderFailover (gated on
//     MQTTNB_CLUSTER_FAILOVER_TESTS=1): proves a delayed Will committed before a
//     stream-leader node is lost is still delivered by a surviving broker, with no
//     duplicate observed across a full lease-recovery window.

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// TestMQTTWillOutboxReplicatedStream asserts the Will outbox stream is created R3
// on a real cluster and is current on every node — the discriminator between real
// replicated storage and the shared-store harness. Always-run.
func TestMQTTWillOutboxReplicatedStream(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	// Stream metadata reports R3: a leader plus two replica peers.
	js, err := b.nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo(mqttWillQStreamName)
		if err != nil {
			return fmt.Errorf("stream info: %w", err)
		}
		if si.Config.Replicas != 3 {
			return fmt.Errorf("Config.Replicas=%d, want 3", si.Config.Replicas)
		}
		if si.Cluster == nil {
			return fmt.Errorf("no cluster info on stream")
		}
		if si.Cluster.Leader == "" {
			return fmt.Errorf("no stream leader")
		}
		if len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("stream replica peers=%d, want 2", len(si.Cluster.Replicas))
		}
		return nil
	})

	// Authoritative replication proof: current on every node (a JS API read alone
	// can be served by the leader and would not prove per-node sync).
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillQStreamName)

	// Durable enqueue of an INERT record (FireAt far in the future) so the active
	// claim loop cannot publish or delete it while we inspect replication.
	asm := testMQTTGetGlobalASM(t, b.s)
	willID := nuid.Next()
	cidHash := "smoke-" + nuid.Next()
	subject := mqttWillQSubject(asm.domainTk, cidHash, willID)
	rec := &mqttWillOutbox{
		WillID:           willID,
		ClientIDHash:     cidHash,
		Gen:              mqttNewSessGen(),
		Topic:            "will/smoke",
		Msg:              []byte("inert"),
		FireAt:           time.Now().Add(time.Hour).UnixNano(),
		State:            mqttWillPending,
		Account:          asm.jsa.c.acc.GetName(),
		OriginSessionSeq: 1,
		OriginSessEpoch:  asm.sessStreamEpoch,
	}
	seq, err := asm.jsa.storeWillOutboxCAS(subject, 0, rec)
	if err != nil {
		t.Fatalf("store inert record: %v", err)
	}

	// Readable back, and replicated to every node.
	got, _, err := asm.jsa.loadWillOutbox(asm.domainTk, subject)
	if err != nil || got == nil {
		t.Fatalf("load inert record: err=%v got=%v", err, got)
	}
	if got.WillID != willID {
		t.Fatalf("loaded WillID=%q, want %q", got.WillID, willID)
	}
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillQStreamName)

	// Delete it and confirm the replicas reconverge (terminal delete replicates).
	if err := asm.jsa.deleteMsg(mqttWillQStreamName, seq, true); err != nil {
		t.Fatalf("delete inert record: %v", err)
	}
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillQStreamName)
	if n := testMQTTCountWillRecords(t, asm); n != 0 {
		t.Fatalf("outbox not empty after delete, got %d records", n)
	}
}

// TestMQTTWillOutboxReplicatedLeaderFailover: a delayed Will committed to the R3
// outbox is delivered by surviving broker B after the $MQTT_willq stream-leader
// node and the originating broker A are both stopped — quorum preserved, no
// duplicate. Gated: failure injection + elections are slow under -race.
func TestMQTTWillOutboxReplicatedLeaderFailover(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}

	// The Will fires ~10s out (setup margin so A is stopped before FireAt); the
	// default 10s read deadline is too tight, so widen it for this test.
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	preA := bA.nc.ConnectedServerName()
	preB := bB.nc.ConnectedServerName()
	t.Logf("pre-failover connections: A=%q B=%q", preA, preB)

	// Subscriber via broker B (the survivor).
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "flsub", cleanStart: true}, bB.opts.MQTT.Host, bB.opts.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/failover", opts: 0}})

	// Will client via broker A: ~10s delay, surviving session; then drop it.
	will := &mqttWill{topic: []byte("will/failover"), message: []byte("from-A"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "flpub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(10), props: mqttV5ConnPropsSessionExpiry(60)}, bA.opts.MQTT.Host, bA.opts.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// Wait until the pending record is durable and current on all three nodes.
	asmA := testMQTTGetGlobalASM(t, bA.s)
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if n := testMQTTCountWillRecords(t, asmA); n != 1 {
			return fmt.Errorf("want 1 pending outbox record, got %d\n%s", n, c.describe())
		}
		return nil
	})
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillQStreamName)

	// Capture the leader only AFTER the record is durable, then stop ONLY that node
	// (2/3 quorum preserved for the claim/publish/delete commits).
	leader := c.waitStreamLeader(t, testGlobalAccount, mqttWillQStreamName)
	t.Logf("stopping $MQTT_willq stream-leader node %q", leader.name)
	c.stopNode(leader)
	// Stop broker A so the origin cannot publish the Will.
	<-bA.s.BeginShutdown()

	// A new leader is elected among the two survivors and the stream is current.
	newLeader := c.waitStreamLeader(t, testGlobalAccount, mqttWillQStreamName)
	t.Logf("new $MQTT_willq stream-leader node %q", newLeader.name)
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillQStreamName)

	// Bridge B must remain operational or reconnect (its node may or may not have
	// been the one stopped) — do NOT assert a specific reconnect.
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		if !bB.nc.IsConnected() {
			return fmt.Errorf("broker B not connected after failover (A: %q, B: %q->%q)\n%s",
				preA, preB, bB.nc.ConnectedServerName(), c.describe())
		}
		return nil
	})

	// B delivers the handed-off Will (one observed delivery).
	testMQTTReadPublishV5(t, rs, "will/failover", []byte("from-A"))

	// The native outbox record is removed after the publish.
	asmB := testMQTTGetGlobalASM(t, bB.s)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if n := testMQTTCountWillRecords(t, asmB); n != 0 {
			return fmt.Errorf("outbox record not removed after publish, got %d\n%s", n, c.describe())
		}
		return nil
	})

	// No duplicate across a full lease-recovery window (a short "expect nothing"
	// would only prove no immediate second packet).
	testMQTTExpectNothingFor(t, rs, mqttWillClaimLease+mqttWillClaimInterval)
}

// TestMQTTWillOutboxReplicatedNodeCatchup: a follower that is OFFLINE while the
// surviving quorum publishes and deletes a delayed Will must, after it restarts and
// catches up, converge to an empty outbox — the record must not resurrect and the
// Will must not re-fire. Gated: restart + Raft catch-up is slow under -race.
func TestMQTTWillOutboxReplicatedNodeCatchup(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}

	// The Will fires ~10s out; widen the default 10s read deadline (flaky under -race).
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	// Subscriber and Will client both through the single bridge.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "cusub", cleanStart: true}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/catchup", opts: 0}})

	will := &mqttWill{topic: []byte("will/catchup"), message: []byte("from-bridge"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "cupub", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(10), props: mqttV5ConnPropsSessionExpiry(60)}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()

	// Exactly one outbox record, and it is pending (count alone would not prove state).
	asm := testMQTTGetGlobalASM(t, b.s)
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		recs := testMQTTListWillRecords(t, asm)
		if len(recs) != 1 {
			return fmt.Errorf("want 1 outbox record, got %d\n%s", len(recs), c.describe())
		}
		if recs[0].rec.State != mqttWillPending {
			return fmt.Errorf("want pending record, got state %q\n%s", recs[0].rec.State, c.describe())
		}
		return nil
	})
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillQStreamName)

	// Pick a victim FOLLOWER: neither the stream leader nor the node serving the
	// bridge's NATS connection, so its restart cannot trigger an election or a bridge
	// reconnect and the test isolates pure replica catch-up.
	leader := c.waitStreamLeader(t, testGlobalAccount, mqttWillQStreamName)
	bridgeNode := b.nc.ConnectedServerName()
	var victim *testJSClusterNode
	for _, n := range c.nodes {
		if n.running && n.name != leader.name && n.name != bridgeNode {
			victim = n
			break
		}
	}
	if victim == nil {
		t.Fatalf("no eligible follower: leader=%q bridge=%q\n%s", leader.name, bridgeNode, c.describe())
	}
	t.Logf("stopping follower %q (leader=%q bridge=%q)", victim.name, leader.name, bridgeNode)
	c.stopNode(victim) // its local store keeps the pending record

	// The surviving quorum (leader + other follower) claims, publishes and deletes.
	testMQTTReadPublishV5(t, rs, "will/catchup", []byte("from-bridge"))
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		if n := testMQTTCountWillRecords(t, asm); n != 0 {
			return fmt.Errorf("outbox record not removed after publish, got %d\n%s", n, c.describe())
		}
		return nil
	})

	// Restart the stale follower; it must rejoin and catch up the publish+delete it
	// missed while offline.
	c.restartNode(t, victim)
	c.waitFormed(t, 3)
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillQStreamName)

	// The caught-up replica must not resurrect the record...
	if n := testMQTTCountWillRecords(t, asm); n != 0 {
		t.Fatalf("outbox not empty after follower catch-up, got %d\n%s", n, c.describe())
	}
	// ...nor re-fire the Will across a full lease-recovery window.
	testMQTTExpectNothingFor(t, rs, mqttWillClaimLease+mqttWillClaimInterval)
}

// testMQTTExpectNothingFor asserts no packet arrives on r for the full window d.
// Unlike testMQTTExpectNothing (~100ms) it spans a lease-recovery interval, and it
// treats ONLY a read-deadline timeout as success: EOF, connection reset, or any
// other read error means the connection died and must NOT count as "no duplicate".
// n is checked independently of err because net.Conn.Read may return data together
// with an error.
func testMQTTExpectNothingFor(t testing.TB, r *mqttReader, d time.Duration) {
	t.Helper()
	if r.hasMore() {
		t.Fatalf("expected no duplicate, got buffered %v", r.buf[r.pos:])
	}
	deadline := time.Now().Add(d)
	if err := r.reader.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set duplicate-check deadline: %v", err)
	}
	defer r.reader.SetReadDeadline(time.Time{}) //nolint:errcheck

	var buf [128]byte
	n, err := r.reader.Read(buf[:])
	if n > 0 {
		t.Fatalf("unexpected duplicate data: %v", buf[:n])
	}
	if err == nil {
		t.Fatal("duplicate check returned without data or timeout")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("connection ended while checking for duplicate: %v", err)
	}
}

// TestMQTTWillOutboxReplicatedWatermark (plan test 16, always-run): the durable
// per-client cancellation marker stream $MQTT_willcancel is created R3 and current
// on every node, and a watermark advanced through the bridge is replicated and
// readable back as a valid current-epoch marker.
func TestMQTTWillOutboxReplicatedWatermark(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	js, err := b.nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo(mqttWillCancelStreamName)
		if err != nil {
			return fmt.Errorf("marker stream info: %w", err)
		}
		if si.Config.Replicas != 3 {
			return fmt.Errorf("Config.Replicas=%d, want 3", si.Config.Replicas)
		}
		if si.Config.MaxMsgsPerSubject != 1 {
			return fmt.Errorf("MaxMsgsPerSubject=%d, want 1", si.Config.MaxMsgsPerSubject)
		}
		if si.Cluster == nil || si.Cluster.Leader == "" || len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("marker stream not R3-clustered: %+v", si.Cluster)
		}
		return nil
	})
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillCancelStreamName)

	// Advance a watermark through the bridge and prove it replicated + reads back.
	asm := testMQTTGetGlobalASM(t, b.s)
	cidHash := "wmrepl-" + nuid.Next()
	if err := asm.advanceWillCancelWatermark(cidHash, 42); err != nil {
		t.Fatalf("advance watermark: %v", err)
	}
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillCancelStreamName)
	wm, _, status := asm.loadWillCancelWatermark(cidHash)
	if status != mqttWillCancelValid || wm.SessionSeq != 42 || wm.SessEpoch != asm.sessStreamEpoch {
		t.Fatalf("watermark not durable/valid after replication: status=%v wm=%+v", status, wm)
	}
}

// TestMQTTWillOutboxReplicatedWatermarkLeaderFailover (plan test 16, gated): a
// watermark committed to the R3 marker stream remains authoritative across the loss
// of its stream-leader node, and CONNECT-side advances recover after the election.
func TestMQTTWillOutboxReplicatedWatermarkLeaderFailover(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}

	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	c.waitStreamCurrent(t, testGlobalAccount, mqttWillCancelStreamName)
	asm := testMQTTGetGlobalASM(t, b.s)
	cidHash := "wmfailover-" + nuid.Next()
	if err := asm.advanceWillCancelWatermark(cidHash, 7); err != nil {
		t.Fatalf("advance watermark: %v", err)
	}
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillCancelStreamName)

	// Stop ONLY the marker stream leader (2/3 quorum preserved).
	leader := c.waitStreamLeader(t, testGlobalAccount, mqttWillCancelStreamName)
	t.Logf("stopping $MQTT_willcancel stream-leader node %q", leader.name)
	c.stopNode(leader)

	// A new leader is elected among survivors and the marker stream is current.
	newLeader := c.waitStreamLeader(t, testGlobalAccount, mqttWillCancelStreamName)
	t.Logf("new $MQTT_willcancel stream-leader node %q", newLeader.name)
	c.waitStreamCurrent(t, testGlobalAccount, mqttWillCancelStreamName)

	// The pre-failover watermark is still authoritative.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		wm, _, status := asm.loadWillCancelWatermark(cidHash)
		if status != mqttWillCancelValid || wm.SessionSeq != 7 {
			return fmt.Errorf("watermark lost across failover: status=%v wm=%+v\n%s", status, wm, c.describe())
		}
		return nil
	})

	// A CONNECT-side advance recovers post-election (monotonic raise to 9).
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		if err := asm.advanceWillCancelWatermark(cidHash, 9); err != nil {
			return fmt.Errorf("advance after failover: %w\n%s", err, c.describe())
		}
		return nil
	})
	if wm, _, status := asm.loadWillCancelWatermark(cidHash); status != mqttWillCancelValid || wm.SessionSeq != 9 {
		t.Fatalf("post-failover advance not durable: status=%v wm=%+v", status, wm)
	}

	// A REAL CONNECT through the surviving broker must succeed post-election — the
	// gating watermark advance on the production path works after the leader loss.
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 30 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()
	checkFor(t, 25*time.Second, 250*time.Millisecond, func() error {
		if !b.nc.IsConnected() {
			return fmt.Errorf("broker not connected yet\n%s", c.describe())
		}
		return nil
	})
	cc, cr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmfailover-real", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(60)}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cc.Close()
	testMQTTReadConnAckV5(t, cr)
	if _, _, status := asm.loadWillCancelWatermark(getHash("wmfailover-real")); status != mqttWillCancelValid {
		t.Fatalf("real post-election CONNECT must have advanced a valid marker, got %v", status)
	}
}
