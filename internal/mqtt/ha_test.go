package mqtt

import (
	"fmt"
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// brokerOn starts an MQTT broker bound to an already-running NATS server, with a
// distinct node name (required for JSA reply routing). Returns its tcp:// addr.
func brokerOn(t *testing.T, url, node string) string {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(nc.Close)
	s := NewServer(nc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}, node)
	if err := s.Start(); err != nil {
		t.Fatalf("start broker %s: %v", node, err)
	}
	t.Cleanup(s.Shutdown)
	return fmt.Sprintf("tcp://%s", s.Addr().String())
}

func persistentClient(t *testing.T, broker, id string, onMsg pmqtt.MessageHandler) pmqtt.Client {
	t.Helper()
	opts := pmqtt.NewClientOptions().
		AddBroker(broker).SetClientID(id).
		SetCleanSession(false).SetProtocolVersion(4).
		SetConnectTimeout(5 * time.Second)
	if onMsg != nil {
		opts.SetDefaultPublishHandler(onMsg)
	}
	c := pmqtt.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connect %s: %v", id, tok.Error())
	}
	return c
}

// TestHASessionTakeover verifies the core HA promise: a persistent-session
// client can subscribe on one broker instance, disconnect, and resume its
// session (receiving messages queued while offline) after reconnecting to a
// DIFFERENT broker instance backed by the same NATS/JetStream cluster.
func TestHASessionTakeover(t *testing.T) {
	srv := runJetStreamServerHA(t)
	brokerA := brokerOn(t, srv.ClientURL(), "node-a")
	brokerB := brokerOn(t, srv.ClientURL(), "node-b")

	// Subscribe on A with a persistent session, then disconnect.
	subA := persistentClient(t, brokerA, "roamer", nil)
	if tok := subA.Subscribe("ha/topic", 1, nil); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe on A: %v", tok.Error())
	}
	subA.Disconnect(100)

	// Publish while the client is offline (via a fresh client on B).
	pub := persistentClient(t, brokerB, "ha-pub", nil)
	if tok := pub.Publish("ha/topic", 1, false, "roamed"); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish on B: %v", tok.Error())
	}
	pub.Disconnect(100)

	// Reconnect the roamer to broker B (a different instance) and expect the
	// queued message to be redelivered from the resumed session.
	got := make(chan string, 1)
	subB := persistentClient(t, brokerB, "roamer", func(_ pmqtt.Client, m pmqtt.Message) {
		got <- string(m.Payload())
	})
	defer subB.Disconnect(100)

	select {
	case msg := <-got:
		if msg != "roamed" {
			t.Fatalf("payload = %q, want roamed", msg)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("timed out: session did not take over across instances")
	}
}

func runJetStreamServerHA(t *testing.T) *natsserver.Server {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}
