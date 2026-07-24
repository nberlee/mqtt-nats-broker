package mqtt

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// v5Client dials the broker and completes an MQTT 5.0 CONNECT using the genuine
// v5 client (paho.golang). paho.mqtt.golang is v3.1.1-only, so v5 behavior must
// be exercised with this client.
func v5Client(t *testing.T, brokerTCP, id string, onPub func(paho.PublishReceived) (bool, error)) *paho.Client {
	t.Helper()
	addr := strings.TrimPrefix(brokerTCP, "tcp://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cfg := paho.ClientConfig{Conn: conn}
	if onPub != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPub}
	}
	c := paho.NewClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ca, err := c.Connect(ctx, &paho.Connect{ClientID: id, KeepAlive: 30, CleanStart: true})
	if err != nil {
		t.Fatalf("v5 connect: %v", err)
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("v5 CONNACK reason = %d, want 0", ca.ReasonCode)
	}
	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return c
}

// TestV5Connect verifies a genuine MQTT 5.0 CONNECT is accepted (v5 always-on).
func TestV5Connect(t *testing.T) {
	broker, _ := startBroker(t)
	v5Client(t, broker, "v5-connect", nil)
}

// TestV5PubSub drives a real MQTT 5.0 SUBSCRIBE/PUBLISH round trip, including a
// v5 User Property carried on the publish.
func TestV5PubSub(t *testing.T) {
	broker, _ := startBroker(t)

	got := make(chan *paho.Publish, 1)
	sub := v5Client(t, broker, "v5-sub", func(pr paho.PublishReceived) (bool, error) {
		got <- pr.Packet
		return true, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := sub.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "v5/topic", QoS: 1}},
	}); err != nil {
		t.Fatalf("v5 subscribe: %v", err)
	}

	pub := v5Client(t, broker, "v5-pub", nil)
	_, err := pub.Publish(ctx, &paho.Publish{
		Topic:   "v5/topic",
		QoS:     1,
		Payload: []byte("v5-hello"),
		Properties: &paho.PublishProperties{
			User: paho.UserProperties{{Key: "trace", Value: "abc"}},
		},
	})
	if err != nil {
		t.Fatalf("v5 publish: %v", err)
	}

	select {
	case p := <-got:
		if string(p.Payload) != "v5-hello" {
			t.Errorf("payload = %q, want v5-hello", p.Payload)
		}
		if p.Properties == nil || len(p.Properties.User) == 0 || p.Properties.User.Get("trace") != "abc" {
			t.Errorf("missing/incorrect v5 user property: %+v", p.Properties)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for v5 message")
	}
}

// TestV5SubscribeThenImmediatePublish reproduces the Eclipse Paho interop
// pattern (test_request_response / test_subscribe_options): SUBACK for a QoS 2
// subscription must imply the backing JetStream consumer's deliver-subject
// interest is live, so a publish issued immediately after the SUBACK is
// delivered promptly rather than waiting out AckWait. Each iteration uses a
// fresh topic so a new consumer (and deliver-subject subscription) is created.
func TestV5SubscribeThenImmediatePublish(t *testing.T) {
	broker, _ := startBroker(t)

	got := make(chan *paho.Publish, 16)
	sub := v5Client(t, broker, "v5-race-sub", func(pr paho.PublishReceived) (bool, error) {
		got <- pr.Packet
		return true, nil
	})
	pub := v5Client(t, broker, "v5-race-pub", nil)

	for i := 0; i < 20; i++ {
		topic := fmt.Sprintf("v5/race/%d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := sub.Subscribe(ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 2}},
		}); err != nil {
			cancel()
			t.Fatalf("iter %d: subscribe: %v", i, err)
		}
		// Publish the instant Subscribe returns (i.e. the SUBACK was received).
		if _, err := pub.Publish(ctx, &paho.Publish{
			Topic: topic, QoS: 1, Payload: []byte("race"),
		}); err != nil {
			cancel()
			t.Fatalf("iter %d: publish: %v", i, err)
		}
		select {
		case <-got:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatalf("iter %d: message not delivered within 3s of SUBACK", i)
		}
		cancel()
	}
}

// TestV5PublishRacingSubscribe reproduces the Eclipse Paho interop suite bug
// (test_request_response / test_subscribe_options wait on the wrong callback
// queue): a QoS 1 publish is fired while the QoS 2 SUBSCRIBE is still in
// flight, before its SUBACK arrives. The publish reaches the broker after the
// SUBSCRIBE (the small stagger below orders the wire writes). $MQTT_msgs uses
// interest-based retention, so without the subs barrier (mqttSubsBarrier) the
// message could reach the stream before the subscription's consumer
// registered interest and be discarded outright — permanently lost, not
// merely delayed. The barrier makes the store wait out in-flight SUBSCRIBEs,
// so a publish processed after SUBSCRIBE processing has begun must arrive.
func TestV5PublishRacingSubscribe(t *testing.T) {
	broker, _ := startBroker(t)

	got := make(chan *paho.Publish, 16)
	sub := v5Client(t, broker, "v5-presub-sub", func(pr paho.PublishReceived) (bool, error) {
		got <- pr.Packet
		return true, nil
	})
	pub := v5Client(t, broker, "v5-presub-pub", nil)

	for i := 0; i < 20; i++ {
		topic := fmt.Sprintf("v5/presub/%d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		subDone := make(chan error, 1)
		go func() {
			_, err := sub.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 2}},
			})
			subDone <- err
		}()
		// Let the SUBSCRIBE hit the wire first; land the publish inside the
		// broker's consumer-creation window, before the SUBACK returns.
		time.Sleep(time.Millisecond)
		if _, err := pub.Publish(ctx, &paho.Publish{
			Topic: topic, QoS: 1, Payload: []byte("race"),
		}); err != nil {
			cancel()
			t.Fatalf("iter %d: publish: %v", i, err)
		}
		if err := <-subDone; err != nil {
			cancel()
			t.Fatalf("iter %d: subscribe: %v", i, err)
		}
		select {
		case <-got:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatalf("iter %d: message lost to subscribe/publish race", i)
		}
		cancel()
	}
}
