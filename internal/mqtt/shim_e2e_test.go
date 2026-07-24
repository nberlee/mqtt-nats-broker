package mqtt

import (
	"fmt"
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
)

// startBroker brings up an MQTT broker bound to an in-process JetStream server
// on a random port and returns its "tcp://host:port" address.
func startBroker(t *testing.T) (string, *Server) {
	t.Helper()
	_, nc := runNATS(t)
	opts := &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}
	s := NewServer(nc, opts, "e2e-node")
	if err := s.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(s.Shutdown)
	addr := s.Addr()
	if addr == nil {
		t.Fatal("no listener address")
	}
	return fmt.Sprintf("tcp://%s", addr.String()), s
}

// connectClient connects an MQTT 3.1.1 client (paho.mqtt.golang is v3.1.1-only;
// genuine MQTT 5.0 clients use v5Client in v5_test.go).
func connectClient(t *testing.T, broker, id string) pmqtt.Client {
	t.Helper()
	opts := pmqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(id).
		SetProtocolVersion(4).
		SetConnectTimeout(5 * time.Second).
		SetCleanSession(true)
	c := pmqtt.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("connect timed out")
	}
	if err := tok.Error(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { c.Disconnect(200) })
	return c
}

// TestE2EPubSubQoS0 stands up the full broker and drives a real MQTT client
// through CONNECT -> SUBSCRIBE -> PUBLISH -> receive, end to end over TCP and
// NATS/JetStream. This exercises the connection I/O loop, the parser, the
// session manager, and the routing transport together.
func TestE2EPubSubQoS0(t *testing.T) {
	broker, _ := startBroker(t)

	sub := connectClient(t, broker, "subscriber")
	pub := connectClient(t, broker, "publisher")

	got := make(chan string, 1)
	tok := sub.Subscribe("test/topic", 0, func(_ pmqtt.Client, m pmqtt.Message) {
		got <- string(m.Payload())
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	if tok := pub.Publish("test/topic", 0, false, "hello-mqtt"); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}

	select {
	case msg := <-got:
		if msg != "hello-mqtt" {
			t.Errorf("payload = %q, want hello-mqtt", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message delivery")
	}
}
