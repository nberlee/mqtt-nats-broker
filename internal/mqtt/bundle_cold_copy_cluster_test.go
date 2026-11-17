//go:build mqtt_fulltests

package mqtt

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// A replicated bundle's two supported backups (docs/OPERATIONS.md, "Backup and
// restore"), compared on the delivery state they keep.
//
// A network snapshot on a clustered JetStream writes every consumer with
// acknowledgements pending at its ACK FLOOR (nats-server stream_backup.go): the sparse
// acknowledgements above the floor are lost, so after a restore every stored delivery
// above the floor is offered again. A QoS 1 delivery the client acknowledged there is
// sent again, which is the stated residual. A completed QoS 2 delivery there is NOT: its
// register stays pubrel_terminal for as long as its source may be re-offered (stored and
// above the floor, pubrel_action.go), and recovery turns it into suppression before the
// source's interest opens, so the re-offer is withheld. A cold copy of every server's
// store keeps the consumer state as persisted and offers neither again.

// testReplicatedBundleScenario is the state both arms capture: on S's one QoS 2 durable
// q2a awaits PUBREC below a completed q2b; on S's QoS 1 durable m1 is unacknowledged
// below an acknowledged m2; K's durables keep every source stored; an inbound QoS 2 hold
// and a retained message ride along.
type testReplicatedBundleScenario struct {
	q2a, q2b, m1, m2 uint16
	q2bSeq, m2Seq    uint64
}

func testBuildReplicatedBundleScenario(t *testing.T, c *testJetStreamCluster, b *testClusterBroker) testReplicatedBundleScenario {
	t.Helper()
	var sc testReplicatedBundleScenario
	host, port := b.opts.MQTT.Host, b.opts.MQTT.Port

	ck, rk := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "cc-k", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	testMQTTReadConnAckV5(t, rk)
	testMQTTSubV5(t, ck, rk, 1, []mqttV5SubFilter{{topic: "cc/q2", opts: 1}, {topic: "cc/q1", opts: 1}})

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "cc-s", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "cc/q2", opts: 2}, {topic: "cc/q1", opts: 1}})

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "cc-p", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	testMQTTReadConnAckV5(t, rp)

	testMQTTPubV5Props(t, cp, rp, 2, false, 1, "cc/q2", []byte("q2a"), nil)
	_, sc.q2a = testMQTTReadPublishV5(t, rs, "cc/q2", []byte("q2a"))
	testMQTTPubV5Props(t, cp, rp, 2, false, 2, "cc/q2", []byte("q2b"), nil)
	_, sc.q2b = testMQTTReadPublishV5(t, rs, "cc/q2", []byte("q2b"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, cs, sc.q2b)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rs, sc.q2b)
	testMQTTSendPIPacket(mqttPacketPubComp, t, cs, sc.q2b)
	testMQTTPubV5Props(t, cp, rp, 1, false, 3, "cc/q1", []byte("m1"), nil)
	_, sc.m1 = testMQTTReadPublishV5(t, rs, "cc/q1", []byte("m1"))
	testMQTTPubV5Props(t, cp, rp, 1, false, 4, "cc/q1", []byte("m2"), nil)
	_, sc.m2 = testMQTTReadPublishV5(t, rs, "cc/q1", []byte("m2"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, cs, sc.m2)
	testMQTTPublishV5(t, cp, 2, 7, "cc/q2", []byte("q2-held"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, rp, 7)
	testMQTTPubV5Props(t, cp, rp, 1, true, 8, "cc/ret", []byte("ret"), nil)
	cs.Close()
	ck.Close()
	cp.Close()

	// The preconditions both arms rest on, read before the capture: S's durables each
	// hold exactly the one unacknowledged delivery, their ack floors are below the
	// completed q2b and the acknowledged m2, and K keeps those sources stored.
	nc, err := nats.Connect(c.nodes[0].ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()
	seqOf := func(payload string) uint64 {
		t.Helper()
		for seq := uint64(1); ; seq++ {
			m, err := js.GetMsg(mqttStreamName, seq)
			if err != nil {
				t.Fatalf("no stored %q: %v", payload, err)
			}
			if string(m.Data) == payload {
				return seq
			}
		}
	}
	sc.q2bSeq, sc.m2Seq = seqOf("q2b"), seqOf("m2")
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		var sQ2, sQ1 bool
		for ci := range js.ConsumersInfo(mqttStreamName) {
			switch {
			case ci.NumAckPending == 1 && ci.Delivered.Stream >= sc.q2bSeq && ci.AckFloor.Stream < sc.q2bSeq &&
				ci.Config.FilterSubject == mqttStreamSubjectPrefix+"cc.q2":
				sQ2 = true
			case ci.NumAckPending == 1 && ci.Delivered.Stream >= sc.m2Seq && ci.AckFloor.Stream < sc.m2Seq &&
				ci.Config.FilterSubject == mqttStreamSubjectPrefix+"cc.q1":
				sQ1 = true
			}
		}
		if !sQ2 || !sQ1 {
			return fmt.Errorf("S's durables are not yet one pending below an acknowledged later delivery (q2 %v, q1 %v)", sQ2, sQ1)
		}
		return nil
	})
	return sc
}

// testServeReplicatedBundle resumes S and P on b and returns every packet S read: q2a
// and m1 retransmitted with DUP, the held inbound exchange completed, then a bounded
// window for anything else, a re-offer of q2b or m2 included.
func testServeReplicatedBundle(t *testing.T, b *testClusterBroker, sc testReplicatedBundleScenario) map[string][]testBackupPacket {
	t.Helper()
	host, port := b.opts.MQTT.Host, b.opts.MQTT.Port
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "cc-s", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	defer cs.Close()
	if present, _, _ := testMQTTReadConnAckV5(t, rs); !present {
		t.Fatal("S resumed without its session")
	}
	all := map[string][]testBackupPacket{}
	collect := func(want ...string) map[string][]testBackupPacket {
		got := testBackupCollect(t, rs, want, testBackupRetransmitWithin)
		for k, v := range got {
			all[k] = append(all[k], v...)
		}
		return got
	}
	got := collect("q2a", "m1")
	if p := got["q2a"][0]; !p.dup || p.pi != sc.q2a {
		t.Fatalf("q2a retransmitted as %+v, want DUP with identifier %d", p, sc.q2a)
	}
	if p := got["m1"][0]; !p.dup || p.pi != sc.m1 {
		t.Fatalf("m1 retransmitted as %+v, want DUP with identifier %d", p, sc.m1)
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, cs, sc.q2a)
	collect("PUBREL " + strconv.Itoa(int(sc.q2a)))
	testMQTTSendPIPacket(mqttPacketPubComp, t, cs, sc.q2a)
	testMQTTSendPIPacket(mqttPacketPubAck, t, cs, sc.m1)

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "cc-p", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	defer cp.Close()
	if present, _, _ := testMQTTReadConnAckV5(t, rp); !present {
		t.Fatal("P resumed without its session")
	}
	testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, cp, 7)
	testMQTTReadPIPacket(mqttPacketPubComp, t, rp, 7)
	h := collect("q2-held")["q2-held"][0]
	testMQTTSendPIPacket(mqttPacketPubRec, t, cs, h.pi)
	collect("PUBREL " + strconv.Itoa(int(h.pi)))
	testMQTTSendPIPacket(mqttPacketPubComp, t, cs, h.pi)
	for {
		pk, ok := testBackupReadPacket(t, rs, 3*time.Second)
		if !ok {
			break
		}
		key := pk.payload
		if pk.typ == mqttPacketPubRel {
			key = "PUBREL " + strconv.Itoa(int(pk.pi))
		}
		all[key] = append(all[key], pk)
		if pk.typ == mqttPacketPub && pk.qos == 1 {
			testMQTTSendPIPacket(mqttPacketPubAck, t, cs, pk.pi)
		}
	}
	allowed := map[string]bool{"PUBREL " + strconv.Itoa(int(sc.q2a)): true, "PUBREL " + strconv.Itoa(int(h.pi)): true}
	for key := range all {
		if strings.HasPrefix(key, "PUBREL ") && !allowed[key] {
			t.Fatalf("%s was sent: a completed exchange's slot was admitted again", key)
		}
	}
	if n := len(all["q2b"]); n != 0 {
		t.Fatalf("the completed q2b was delivered to S's application again (%d times)", n)
	}

	cr, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "cc-r", cleanStart: true}, host, port)
	defer cr.Close()
	testMQTTReadConnAckV5(t, rr)
	testMQTTSubV5(t, cr, rr, 1, []mqttV5SubFilter{{topic: "cc/ret", opts: 0}})
	testMQTTReadPubCheckRetainV5(t, cr, rr, "cc/ret", []byte("ret"), true)
	return all
}

func testSkipUnlessClusterFailover(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
}

// The network backup of a replicated bundle, every broker stopped, restored into the
// same cluster: the completed QoS 2 delivery above S's ack floor is not delivered again,
// and the acknowledged QoS 1 delivery above it is (docs/CONFORMANCE.md).
func TestNetworkRestoreOfAReplicatedBundleReoffersAboveTheAckFloor(t *testing.T) {
	testSkipUnlessClusterFailover(t)
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)
	sc := testBuildReplicatedBundleScenario(t, c, b)
	b.cleanup()

	nc, err := nats.Connect(c.nodes[0].ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	set := testSnapshotBundle(t, nc)
	testDeleteBundle(t, nc)
	testRestoreBundle(t, nc, set)
	for _, name := range mqttTestBrokerStreamNames {
		c.waitStreamLeader(t, c.account, name)
		c.waitStreamCurrent(t, c.account, name)
	}

	all := testServeReplicatedBundle(t, c.startBroker(t, 0), sc)
	m2 := all["m2"]
	if len(m2) != 1 {
		t.Fatalf("the acknowledged m2 was delivered %d times after the restore; the residual says once more", len(m2))
	}
	// A new delivery: its register ended with the PUBACK, so the re-offer allocates afresh and
	// is a first transmission, DUP 0. Its identifier is not asserted: nothing keeps it.
	if m2[0].dup {
		t.Fatalf("the re-offered m2 carried DUP 1; a delivery allocated afresh is a first transmission")
	}
}

// The cold copy of a replicated bundle, every broker and every server stopped: the
// consumer state is kept as persisted, so neither the completed q2b nor the
// acknowledged m2 comes back.
func TestColdStoreCopyOfAReplicatedBundleKeepsConsumerState(t *testing.T) {
	testSkipUnlessClusterFailover(t)
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)
	sc := testBuildReplicatedBundleScenario(t, c, b)
	b.cleanup()

	for _, n := range c.nodes {
		c.stopNode(n)
	}
	for _, n := range c.nodes {
		// Beside the originals, under the cluster's own root: removed only after every
		// node has stopped (see testNewJetStreamCluster).
		dst := filepath.Join(filepath.Dir(n.opts.StoreDir), "copy-"+n.name)
		testBackupCopyDir(t, n.opts.StoreDir, dst)
		n.opts.StoreDir = dst
	}
	for _, n := range c.nodes {
		c.restartNode(t, n)
	}
	c.waitFormed(t, 3)
	for _, name := range mqttTestBrokerStreamNames {
		c.waitStreamLeader(t, c.account, name)
		c.waitStreamCurrent(t, c.account, name)
	}

	all := testServeReplicatedBundle(t, c.startBroker(t, 0), sc)
	if n := len(all["m2"]); n != 0 {
		t.Fatalf("the acknowledged m2 was delivered again (%d times): the copy lost S's acknowledgement", n)
	}
}
