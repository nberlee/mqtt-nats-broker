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
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(expiry),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{
		topic: "$share/" + b.group + "/" + b.topic,
		opts:  1,
	}})
	if len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("Expected SUBACK [1] for %s, got %v", id, codes)
	}
	testMQTTFlush(t, c, nil, r)
	return c, r
}

func (b *testSharedBroker) groupDurable(t *testing.T) string {
	t.Helper()
	subj, err := mqttTopicToNATSPubSubject([]byte(b.topic))
	if err != nil {
		t.Fatalf("subject conversion: %v", err)
	}
	_, _, durable := mqttSharedNames(b.group, string(subj))
	return durable
}

func (b *testSharedBroker) bind(t *testing.T, clientID string) {
	t.Helper()
	cl := testMQTTGetClient(t, b.s, clientID)
	b.asm, b.jsa = cl.mqtt.asm, cl.mqtt.sess.jsa
}
