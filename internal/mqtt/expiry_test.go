package mqtt

import (
	"context"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func u32(v uint32) *uint32 { return &v }

// TestV5RetainedMessageExpiry verifies the MQTT 5.0 Message Expiry Interval on a
// retained message: after the interval elapses the retained message is dropped
// (JetStream per-message TTL), so a late subscriber does not receive it; a
// subscriber joining within the interval does.
func TestV5RetainedMessageExpiry(t *testing.T) {
	broker, _ := startBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pub := v5Client(t, broker, "exp-pub", nil)

	// Retained message that expires after 5 seconds. Long enough that the early
	// subscriber's connect+subscribe+receive round trip (below) has real margin
	// under a loaded CI/sandbox scheduler; a 1s window was flaking there.
	if _, err := pub.Publish(ctx, &paho.Publish{
		Topic: "exp/topic", QoS: 1, Retain: true, Payload: []byte("ephemeral"),
		Properties: &paho.PublishProperties{MessageExpiry: u32(5)},
	}); err != nil {
		t.Fatalf("publish retained w/ expiry: %v", err)
	}

	// Subscriber within the interval receives it.
	early := make(chan string, 1)
	subEarly := v5Client(t, broker, "exp-early", func(pr paho.PublishReceived) (bool, error) {
		early <- string(pr.Packet.Payload)
		return true, nil
	})
	if _, err := subEarly.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "exp/topic", QoS: 1}},
	}); err != nil {
		t.Fatalf("early subscribe: %v", err)
	}
	select {
	case msg := <-early:
		if msg != "ephemeral" {
			t.Fatalf("early payload = %q, want ephemeral", msg)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("early subscriber did not receive the retained message")
	}

	// Wait for the message to expire, then a late subscriber must get nothing.
	time.Sleep(6 * time.Second)

	late := make(chan string, 1)
	subLate := v5Client(t, broker, "exp-late", func(pr paho.PublishReceived) (bool, error) {
		late <- string(pr.Packet.Payload)
		return true, nil
	})
	if _, err := subLate.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "exp/topic", QoS: 1}},
	}); err != nil {
		t.Fatalf("late subscribe: %v", err)
	}
	select {
	case msg := <-late:
		t.Fatalf("late subscriber received an expired retained message: %q", msg)
	case <-time.After(1500 * time.Millisecond):
		// Expected: nothing delivered.
	}
}
