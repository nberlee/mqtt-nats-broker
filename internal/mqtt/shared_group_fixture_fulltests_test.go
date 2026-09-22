//go:build mqttport_fulltests

package mqtt

import (
	"net"
	"testing"
	"time"
)

// testSharedBroker is the supported QoS 1 shared-subscription fixture. It is
// deliberately independent of the parked affine fixture so shared group
// ownership and durable verification remain required alpha coverage.
type testSharedBroker struct {
	s     *Server
	o     *Options
	asm   *mqttAccountSessionManager
	jsa   *mqttJSA
	topic string
	group string
}

func testSharedSetup(t *testing.T, name string) *testSharedBroker {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.ExperimentalSharedQoS2 = false
	o.MQTT.AckWait = 400 * time.Millisecond
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	return &testSharedBroker{s: s, o: o, topic: name + "/topic", group: name + "grp"}
}

func (b *testSharedBroker) member(t *testing.T, id string, expiry uint32) (net.Conn, *mqttReader) {
	t.Helper()
	return b.memberFilter(t, id, expiry, b.topic)
}

// memberFilter is member for an explicit topic filter (the subject part of the $share key),
// so the "foo/#" tests can join a wildcard that acquires on two group consumers.
func (b *testSharedBroker) memberFilter(t *testing.T, id string, expiry uint32, topic string) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(expiry),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{
		topic: "$share/" + b.group + "/" + topic,
		opts:  1,
	}})
	if len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("Expected SUBACK [1] for %s, got %v", id, codes)
	}
	testMQTTFlush(t, c, nil, r)
	return c, r
}

func (b *testSharedBroker) groupDurable(t *testing.T) string {
	return b.groupDurableAt(t, 1)
}

// groupDurableAt is groupDurable for an explicit incarnation, so a test can name the
// SUCCESSOR consumer a retirement mounts.
func (b *testSharedBroker) groupDurableAt(t *testing.T, inc uint64) string {
	t.Helper()
	subj, err := mqttTopicToNATSPubSubject([]byte(b.topic))
	if err != nil {
		t.Fatalf("subject conversion: %v", err)
	}
	_, _, durable := mqttSharedNames(b.group, string(subj), inc)
	return durable
}

// qkey is the group key of this fixture's ($share group, topic).
func (b *testSharedBroker) qkey(t *testing.T) string {
	t.Helper()
	subj, err := mqttTopicToNATSPubSubject([]byte(b.topic))
	if err != nil {
		t.Fatalf("subject conversion: %v", err)
	}
	return mqttSharedGroupKey(b.group, string(subj))
}

// loadGroup reads this fixture's group record through the production decoder. b.bind must
// have run.
func (b *testSharedBroker) loadGroup(t *testing.T) mqttSharedGroupLoad {
	t.Helper()
	return b.jsa.loadSharedGroup(b.asm.sessStreamEpoch, b.qkey(t))
}

// loadLedger reads a session generation's ledger through the production decoder.
func (b *testSharedBroker) loadLedger(t *testing.T, id, gen string) mqttSharedLedgerLoad {
	t.Helper()
	return b.jsa.loadSharedLedger(id, b.asm.sessStreamEpoch, gen)
}

func (b *testSharedBroker) bind(t *testing.T, clientID string) {
	t.Helper()
	cl := testMQTTGetClient(t, b.s, clientID)
	b.asm, b.jsa = cl.mqtt.asm, cl.mqtt.sess.jsa
}
