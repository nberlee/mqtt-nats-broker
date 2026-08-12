package mqtt

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
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
	t.Parallel()
	broker, _ := startBroker(t)
	v5Client(t, broker, "v5-connect", nil)
}

// TestV5PubSub drives a real MQTT 5.0 SUBSCRIBE/PUBLISH round trip, including a
// v5 User Property carried on the publish.
func TestV5PubSub(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

// TestV5SubsBarrierHoldsAPublishUntilInterestExists REPLACES TestV5PublishRacingSubscribe,
// which reproduced the Eclipse Paho interop suite pattern (test_request_response /
// test_subscribe_options) by firing a QoS 1 publish 1 ms after a QoS 2 SUBSCRIBE hit the
// wire and demanding delivery.
//
// That test was structurally invalid: a 1 ms sleep in one goroutine cannot order two
// independent TCP connections through the Go scheduler, the sockets and the broker read
// loops, so it could not establish its own precondition — that SUBSCRIBE processing had
// BEGUN — and therefore could demand delivery outside the barrier's contract. It held only
// as long as the suite was light enough for the stagger to win by luck, and suite growth
// made it fail about one run in three on an 8-core machine with no broker defect anywhere
// (the deletion was decided 2026-08-12; the fresh-consumer-after-SUBACK half of the interop
// pattern remains covered by TestV5SubscribeThenImmediatePublish above).
//
// $MQTT_msgs is interest-retained, so a QoS 1 message stored while a subscription's consumer
// create is still in flight finds no interest and is DISCARDED OUTRIGHT — the sequence is
// burned and the message is permanently invisible, not merely late. mqttSubsBarrier is what
// closes that window: SUBSCRIBE holds it for its whole processing, and a store waits it out.
//
// The stagger-based test cannot tell a barrier violation from a publish that simply arrived
// before SUBSCRIBE processing began. This one can, because the ordering is imposed rather
// than raced: the SUBSCRIBE is blocked INSIDE the barrier, before any consumer exists, and
// the publish is issued there. Two assertions, and the first is the one with teeth:
//
//   - while the barrier is held the QoS 1 PUBACK must NOT arrive, which is the store waiting
//     rather than being discarded;
//   - once released, the publish completes AND the message is delivered.
//
// NOT parallel: it installs a process-global hook.
func TestV5SubsBarrierHoldsAPublishUntilInterestExists(t *testing.T) {
	broker, _ := startBroker(t)

	got := make(chan *paho.Publish, 4)
	sub := v5Client(t, broker, "v5-barrier-sub", func(pr paho.PublishReceived) (bool, error) {
		got <- pr.Packet
		return true, nil
	})
	pub := v5Client(t, broker, "v5-barrier-pub", nil)

	const topic = "v5/barrier/one"
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	gate := newTestHookGate(t)
	restore := setMQTTTestSubsBarrierHeldHook(func(filters []*mqttFilter) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		for _, f := range filters {
			// processSubs receives the filter already converted to its NATS subject form,
			// which is what sess.subs is keyed by.
			if f.filter != topic && f.filter != "v5.barrier.one" {
				continue
			}
			once.Do(func() { close(entered) })
			<-release
		}
	})
	t.Cleanup(func() {
		// Unblock before uninstalling, so a failing assertion never strands the SUBSCRIBE.
		select {
		case <-release:
		default:
			close(release)
		}
		restore()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	subDone := make(chan error, 1)
	go func() {
		_, err := sub.Subscribe(ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 2}},
		})
		subDone <- err
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the SUBSCRIBE never reached the barrier seam")
	}

	// The publish is issued with the barrier HELD and no consumer in existence.
	pubDone := make(chan error, 1)
	go func() {
		_, err := pub.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: []byte("barrier")})
		pubDone <- err
	}()

	select {
	case err := <-pubDone:
		t.Fatalf("the QoS 1 store must WAIT on the barrier: its PUBACK arrived while a "+
			"SUBSCRIBE was still being processed, so the message was stored (or discarded) "+
			"with no interest registered (err=%v)", err)
	case p := <-got:
		t.Fatalf("nothing can be delivered before the subscription's consumer exists: %q", p.Payload)
	case <-time.After(750 * time.Millisecond):
		// Held, which is the guarantee.
	}

	close(release)

	if err := <-subDone; err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case err := <-pubDone:
		if err != nil {
			t.Fatalf("publish after the barrier released: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the publish never completed after the barrier released")
	}
	select {
	case p := <-got:
		if string(p.Payload) != "barrier" {
			t.Fatalf("payload = %q, want barrier", p.Payload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a message stored behind the barrier must be delivered once interest exists: " +
			"this is the interest-retention loss the barrier exists to prevent")
	}
}
