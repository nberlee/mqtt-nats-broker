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
	// The SAME source every production writer uses. Writing the shim account name
	// here instead would make this fixture the last producer in the tree of a
	// record shape the broker no longer writes, and deliverWillRecord's account
	// fence would refuse it -- invisibly, because this record is deliberately inert
	// and is deleted before it can ever be delivered.
	svcAccount, svcOK := b.s.serviceAccountForWill()
	if !svcOK {
		t.Fatal("the service account must be established before an outbox record can be written")
	}
	willID := nuid.Next()
	clientID := "smoke-" + nuid.Next()
	cidHash := getHash(clientID)
	subject := mqttWillQSubject(asm.domainTk, cidHash, willID)
	rec := &mqttWillOutbox{
		WillID:           willID,
		ClientIDHash:     cidHash,
		ClientID:         clientID,
		Gen:              mqttNewSessGen(),
		Topic:            "will/smoke",
		Msg:              []byte("inert"),
		FireAt:           time.Now().Add(time.Hour).UnixNano(),
		State:            mqttWillPending,
		Account:          svcAccount,
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

// testAcceptFactsFor returns every permanent acceptance fact currently stored for a
// client hash in this broker's incarnation, oldest first.
func testAcceptFactsFor(t *testing.T, asm *mqttAccountSessionManager, cidHash string) []*mqttAcceptFact {
	t.Helper()
	filter := mqttAcceptClientEpochFilter(asm.domainTk, cidHash, asm.sessStreamEpoch)
	var out []*mqttAcceptFact
	for seq := uint64(1); ; {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				t.Fatalf("walk acceptance facts: %v", err)
			}
			return out
		}
		seq = smsg.Sequence + 1
		f, derr := mqttDecodeAcceptFact(asm.domainTk, asm.sessStreamEpoch, smsg)
		if derr != nil {
			t.Fatalf("decode acceptance fact on %q: %v", smsg.Subject, derr)
		}
		out = append(out, f)
	}
}

// TestMQTTWillOutboxReplicatedAcceptFacts is T17's always-run half: $MQTT_sess is
// created R3 and current on every node, it carries ALL FOUR subject spaces, and a real
// CONNECT through the bridge leaves a permanent acceptance fact that is replicated
// and reads back valid.
//
// SMOKE COVERAGE ONLY. It runs against a real cluster, but no mutation is paired
// with it, so it is not evidence for any cluster-shaped claim — see CLAUDE.md. What
// it does establish is that the atomic batch actually works against a replicated
// stream, which no single-node test can.
func TestMQTTWillOutboxReplicatedAcceptFacts(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	js, err := b.nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo(mqttSessStreamName)
		if err != nil {
			return fmt.Errorf("session stream info: %w", err)
		}
		if si.Config.Replicas != 3 {
			return fmt.Errorf("Config.Replicas=%d, want 3", si.Config.Replicas)
		}
		// THE EXACT SET, by name. A count was the earlier assertion and it went stale the
		// moment the handoff outcomes gained their own space — worse, a count would have
		// been satisfied by any three spaces, including a wrong one. Naming them means a
		// missing space and an unexpected space are both failures here, which is what a
		// stream whose validator refuses an unsupported shape actually needs.
		want := map[string]struct{}{
			mqttSessStreamSubjectPrefix + ">":      {},
			mqttAcceptSubjectPrefix + ">":          {},
			mqttHandoffSubjectPrefix + ">":         {},
			mqttTeardownReceiptSubjectPrefix + ">": {},
		}
		if len(si.Config.Subjects) != len(want) {
			return fmt.Errorf("Config.Subjects=%v, want the record, accept-fact and handoff-outcome spaces",
				si.Config.Subjects)
		}
		for _, subj := range si.Config.Subjects {
			if _, ok := want[subj]; !ok {
				return fmt.Errorf("Config.Subjects=%v carries %q, which is no space this broker provisions",
					si.Config.Subjects, subj)
			}
			delete(want, subj)
		}
		if len(want) != 0 {
			return fmt.Errorf("Config.Subjects=%v is missing %v", si.Config.Subjects, want)
		}
		if si.Cluster == nil || si.Cluster.Leader == "" || len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("session stream not R3-clustered: %+v", si.Cluster)
		}
		// AllowAtomicPublish is asserted through the broker's OWN validator rather
		// than this client's StreamConfig, which does not carry the field: the
		// account manager refuses to initialize without it, so a broker that got
		// this far already proved it. Left as a note rather than a silent omission.
		return nil
	})
	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	asm := testMQTTGetGlobalASM(t, b.s)
	const cid = "factrepl"
	cc, cr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(60)}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cc.Close()
	testMQTTReadConnAckV5(t, cr)

	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)
	facts := testAcceptFactsFor(t, asm, getHash(cid))
	if len(facts) != 1 {
		t.Fatalf("one accepted CONNECT must leave exactly one acceptance fact, got %d", len(facts))
	}
	if facts[0].ClientID != cid {
		t.Fatalf("the fact must carry the EXACT client id: got %q want %q", facts[0].ClientID, cid)
	}
	if facts[0].At == 0 {
		t.Fatal("the fact must be dated: an undated acceptance suppresses unconditionally")
	}
}

// TestMQTTWillOutboxReplicatedAcceptFactsLeaderFailover is T17's gated half: a
// committed acceptance fact stays authoritative across the loss of its stream-leader
// node, and a CONNECT after the election commits a new one.
//
// Same limitation: smoke coverage, no paired mutation, not evidence for a
// cluster-shaped claim.
func TestMQTTWillOutboxReplicatedAcceptFactsLeaderFailover(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}

	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)
	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)
	asm := testMQTTGetGlobalASM(t, b.s)

	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 30 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	const before = "factfailover-before"
	cc, cr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: before, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(60)}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cc.Close()
	testMQTTReadConnAckV5(t, cr)
	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)
	if n := len(testAcceptFactsFor(t, asm, getHash(before))); n != 1 {
		t.Fatalf("pre-failover CONNECT must leave one acceptance fact, got %d", n)
	}

	// Stop ONLY the session stream leader (2/3 quorum preserved).
	leader := c.waitStreamLeader(t, testGlobalAccount, mqttSessStreamName)
	t.Logf("stopping $MQTT_sess stream-leader node %q", leader.name)
	c.stopNode(leader)

	newLeader := c.waitStreamLeader(t, testGlobalAccount, mqttSessStreamName)
	t.Logf("new $MQTT_sess stream-leader node %q", newLeader.name)
	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	// The pre-failover fact is permanent: nothing rewrites its subject and nothing
	// reaps it, so an election cannot cost it.
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		facts := testAcceptFactsFor(t, asm, getHash(before))
		if len(facts) != 1 || facts[0].ClientID != before {
			return fmt.Errorf("acceptance fact lost across failover: %+v\n%s", facts, c.describe())
		}
		return nil
	})

	// And a real CONNECT through the surviving broker still commits its batch
	// post-election — the atomic accept works after a leader loss.
	checkFor(t, 25*time.Second, 250*time.Millisecond, func() error {
		if !b.nc.IsConnected() {
			return fmt.Errorf("broker not connected yet\n%s", c.describe())
		}
		return nil
	})
	const after = "factfailover-after"
	cc2, cr2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: after, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(60)}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cc2.Close()
	testMQTTReadConnAckV5(t, cr2)
	if n := len(testAcceptFactsFor(t, asm, getHash(after))); n != 1 {
		t.Fatalf("a post-election CONNECT must commit its acceptance fact, got %d facts", n)
	}
}
