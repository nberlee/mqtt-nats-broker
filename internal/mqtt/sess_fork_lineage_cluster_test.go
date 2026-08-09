//go:build mqttport_fulltests

package mqtt

import (
	"testing"
	"time"
)

// The lineage over REAL replicated JetStream.
//
// Everything else in this feature runs against one embedded stream, where "the fields
// serialize" and "the fields survive replication and a broker restart" are the same
// statement. They are not: $MQTT_sess is Replicas=3 here, so the record a second broker
// reads has been through Raft, and the parent pointer a restore adopts is one that
// travelled.
//
// The round-trip alone would not be enough — it proves only serialization — so the second
// half is the one that matters: a session restored on ANOTHER broker must stamp the FIRST
// broker's identity as its parent. That is the chain the whole detection rests on, and it
// is exactly what a restore that dropped the pair would break silently.

// TestMQTTSessLineageReplicatedStream is always run: it asserts $MQTT_sess really is
// Replicas=3 and current on every node, so the assertions below say something about HA
// rather than about a single-replica stream.
func TestMQTTSessLineageReplicatedStream(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)
	defer b.cleanup()

	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	si, err := b.s.mqtt.sessmgr.sessions[globalAccountName].jsa.lookupStream(mqttSessStreamName)
	if err != nil {
		t.Fatalf("lookup %s: %v", mqttSessStreamName, err)
	}
	if si.Config.Replicas != 3 {
		t.Fatalf("%s has %d replica(s), want 3: the lineage assertions would prove nothing about HA",
			mqttSessStreamName, si.Config.Replicas)
	}
	// MaxMsgsPer:1 is what makes a fork DESTRUCTIVE rather than merely concurrent — the
	// later apply discards the earlier record — so the finding depends on it holding here.
	if si.Config.MaxMsgsPer != 1 {
		t.Fatalf("%s has max_msgs_per_subject %d, want 1", mqttSessStreamName, si.Config.MaxMsgsPer)
	}
}

// TestMQTTSessLineageAcrossBrokers is the chain assertion.
//
// Broker A's record carries a minted identity; broker B restores it, adopts that identity
// as its acknowledged one, and its own next write names it as PARENT. Without the restore
// adoption the second broker's write would carry an empty parent, and from then on every
// sibling of that base is unprovable — so detection would be inert after any handoff even
// on a fully upgraded cluster, with nothing failing anywhere.
func TestMQTTSessLineageAcrossBrokers(t *testing.T) {
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	a := c.startBroker(t, 0)
	defer a.cleanup()
	b := c.startBroker(t, 1)
	defer b.cleanup()

	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	const cid = "lineage-across"
	// MQTT 3.1.1 with cleanSess false: a NEVER-EXPIRING persistent session, so it is
	// resumable on the other broker without a v5 Session Expiry Interval property.
	ci := &mqttConnInfo{clientID: cid, cleanSess: false}

	ca, ra := testMQTTConnect(t, ci, a.opts.MQTT.Host, a.opts.MQTT.Port)
	testMQTTCheckConnAck(t, ra, mqttConnAckRCConnectionAccepted, false)
	// A subscription, so the session has state worth resuming and a second record write.
	testMQTTSub(t, 1, ca, ra, []*mqttFilter{{filter: "lineage/across", qos: 1}}, []byte{1})
	testMQTTFlush(t, ca, nil, ra)

	sessA := testMQTTGetClient(t, a.s, cid).mqtt.sess
	if sessA == nil {
		t.Fatal("broker A must be bound to a session")
	}
	sessA.mu.Lock()
	widA, pwidA := sessA.wid, sessA.pwid
	sessA.mu.Unlock()
	if widA == _EMPTY_ {
		t.Fatal("a record written through real replicated JetStream must carry a minted identity")
	}
	if pwidA == _EMPTY_ {
		t.Fatal("the SUBSCRIBE's save must be parented to the accept save's record")
	}
	testMQTTDisconnect(t, ca, nil)
	ca.Close()

	// Broker B resumes the same client ID. Its session is a fresh object built from the
	// replicated record, so whatever it holds it got from that record.
	cb, rb := testMQTTConnect(t, ci, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer cb.Close()
	testMQTTCheckConnAck(t, rb, mqttConnAckRCConnectionAccepted, true)

	sessB := testMQTTGetClient(t, b.s, cid).mqtt.sess
	if sessB == nil {
		t.Fatal("broker B must be bound to a session")
	}
	sessB.mu.Lock()
	widB, pwidB := sessB.wid, sessB.pwid
	sessB.mu.Unlock()

	// Broker B's CONNECT performs an accept save of its own, so by now its acknowledged
	// pair is that write's — and its PARENT must be broker A's identity. That is the link
	// in the chain a dropped restore would break.
	if pwidB != widA {
		t.Fatalf("broker B's write must be parented to broker A's record: PWID=%q want %q "+
			"(B's own WID %q)", pwidB, widA, widB)
	}
	if widB == _EMPTY_ || widB == widA {
		t.Fatalf("broker B must mint its own identity: WID=%q (A's %q)", widB, widA)
	}
}

// TestMQTTSessLineageSurvivesNodeRestart pins that the pair is DURABLE, not merely
// in-flight state: it is written to the replicated log and read back after the node
// holding it has been stopped and restarted.
func TestMQTTSessLineageSurvivesNodeRestart(t *testing.T) {
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	a := c.startBroker(t, 0)
	defer a.cleanup()

	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	const cid = "lineage-restart"
	ci := &mqttConnInfo{clientID: cid, cleanSess: false}
	ca, ra := testMQTTConnect(t, ci, a.opts.MQTT.Host, a.opts.MQTT.Port)
	testMQTTCheckConnAck(t, ra, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, ca, ra, []*mqttFilter{{filter: "lineage/restart", qos: 1}}, []byte{1})
	testMQTTFlush(t, ca, nil, ra)
	sess := testMQTTGetClient(t, a.s, cid).mqtt.sess
	sess.mu.Lock()
	wantWID, wantPWID := sess.wid, sess.pwid
	sess.mu.Unlock()
	testMQTTDisconnect(t, ca, nil)
	ca.Close()

	// Take the $MQTT_sess leader down and bring it back, so the record is served from a
	// log that was replicated rather than from one process's memory.
	leader := c.waitStreamLeader(t, testGlobalAccount, mqttSessStreamName)
	c.stopNode(leader)
	c.waitStreamLeader(t, testGlobalAccount, mqttSessStreamName)
	c.restartNode(t, leader)
	c.waitFormed(t, 3)
	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	asm := a.s.mqtt.sessmgr.sessions[globalAccountName]
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(cid))
	if err != nil {
		t.Fatalf("load the session record after the restart: %v", err)
	}
	ps := mqttDecodeSessionRecord(smsg)
	if ps == nil {
		t.Fatal("the record must still decode after the restart")
	}
	if ps.WID != wantWID || ps.PWID != wantPWID {
		t.Fatalf("the lineage must survive replication and a node restart: got (%q,%q), want (%q,%q)",
			ps.WID, ps.PWID, wantWID, wantPWID)
	}
}
