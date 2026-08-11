package mqtt

import (
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
)

// connectClientID connects a 3.1.1 client with an explicit (possibly unusual)
// client ID.
func connectClientID(t *testing.T, broker, id string) pmqtt.Client {
	t.Helper()
	opts := pmqtt.NewClientOptions().
		AddBroker(broker).SetClientID(id).SetProtocolVersion(4).
		SetConnectTimeout(5 * time.Second).SetCleanSession(true)
	c := pmqtt.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connect with client ID %q: %v", id, tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// TestClientIDArbitraryChars verifies the broker accepts client IDs containing
// characters that are not valid NATS subject tokens (spaces, dots, etc.) — as
// mainstream MQTT brokers do and MQTT 5.0 [3.1.3.1] permits — and that such a
// client works end to end, including QoS2 (whose dedup subject embeds a hash of
// the client ID).
func TestClientIDArbitraryChars(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)

	for _, id := range []string{"clean retained", "id.with.dots", "weird/+#*id", "clientеÜ"} {
		id := id
		t.Run(id, func(t *testing.T) {
			sub := connectClientID(t, broker, id+"-sub")
			pub := connectClientID(t, broker, id+"-pub")

			got := make(chan string, 1)
			if tok := sub.Subscribe("cid/topic", 2, func(_ pmqtt.Client, m pmqtt.Message) {
				got <- string(m.Payload())
			}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
				t.Fatalf("subscribe: %v", tok.Error())
			}
			// QoS2 exercises mqttQoS2InternalSubject, which hashes the client ID.
			if tok := pub.Publish("cid/topic", 2, false, "hello"); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
				t.Fatalf("publish: %v", tok.Error())
			}
			select {
			case msg := <-got:
				if msg != "hello" {
					t.Fatalf("payload = %q, want hello", msg)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for QoS2 delivery")
			}
		})
	}
}
