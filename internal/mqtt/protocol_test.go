package mqtt

import (
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
)

// These tests drive real MQTT 3.1.1 clients against the standalone broker over
// TCP + in-process JetStream, covering the core protocol behaviors mined from
// NATS-server's upstream MQTT suite (retained, QoS1/2, sessions, will,
// wildcards, unsubscribe). See CONTRIBUTING.md for why the upstream suite does not
// cover these on its own (it assumes an embedded NATS server started from config
// files).

// recvChan subscribes and returns a channel of received payloads.
func recvChan(t *testing.T, c pmqtt.Client, topic string, qos byte) <-chan string {
	t.Helper()
	ch := make(chan string, 16)
	tok := c.Subscribe(topic, qos, func(_ pmqtt.Client, m pmqtt.Message) {
		ch <- string(m.Payload())
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe %q: %v", topic, tok.Error())
	}
	return ch
}

func mustPublish(t *testing.T, c pmqtt.Client, topic string, qos byte, retained bool, payload string) {
	t.Helper()
	tok := c.Publish(topic, qos, retained, payload)
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish %q: %v", topic, tok.Error())
	}
}

func expectMsg(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("payload = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %q", want)
	}
}

func expectNoMsg(t *testing.T, ch <-chan string, within time.Duration) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("unexpected message %q", got)
	case <-time.After(within):
	}
}

func TestProtocolQoS1(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)
	sub := connectClient(t, broker, "qos1-sub")
	pub := connectClient(t, broker, "qos1-pub")

	ch := recvChan(t, sub, "q1/topic", 1)
	mustPublish(t, pub, "q1/topic", 1, false, "at-least-once")
	expectMsg(t, ch, "at-least-once")
}

func TestProtocolQoS2(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)
	sub := connectClient(t, broker, "qos2-sub")
	pub := connectClient(t, broker, "qos2-pub")

	ch := recvChan(t, sub, "q2/topic", 2)
	mustPublish(t, pub, "q2/topic", 2, false, "exactly-once")
	expectMsg(t, ch, "exactly-once")
}

func TestProtocolRetained(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)
	pub := connectClient(t, broker, "ret-pub")

	// Publish retained before any subscriber exists.
	mustPublish(t, pub, "ret/topic", 1, true, "kept")

	// A late subscriber must receive the retained message.
	sub := connectClient(t, broker, "ret-sub")
	ch := recvChan(t, sub, "ret/topic", 1)
	expectMsg(t, ch, "kept")

	// An empty retained publish clears it.
	mustPublish(t, pub, "ret/topic", 1, true, "")
	sub2 := connectClient(t, broker, "ret-sub2")
	ch2 := recvChan(t, sub2, "ret/topic", 1)
	expectNoMsg(t, ch2, 700*time.Millisecond)
}

func TestProtocolWildcards(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)
	sub := connectClient(t, broker, "wc-sub")
	pub := connectClient(t, broker, "wc-pub")

	plus := recvChan(t, sub, "sensors/+/temp", 0)
	mustPublish(t, pub, "sensors/room1/temp", 0, false, "21")
	expectMsg(t, plus, "21")

	hash := recvChan(t, sub, "home/#", 0)
	mustPublish(t, pub, "home/kitchen/light", 0, false, "on")
	expectMsg(t, hash, "on")
}

func TestProtocolUnsubscribe(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)
	sub := connectClient(t, broker, "unsub")
	pub := connectClient(t, broker, "unsub-pub")

	ch := recvChan(t, sub, "u/topic", 0)
	mustPublish(t, pub, "u/topic", 0, false, "first")
	expectMsg(t, ch, "first")

	if tok := sub.Unsubscribe("u/topic"); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("unsubscribe: %v", tok.Error())
	}
	mustPublish(t, pub, "u/topic", 0, false, "second")
	expectNoMsg(t, ch, 700*time.Millisecond)
}

func TestProtocolMultipleSubscribers(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)
	pub := connectClient(t, broker, "multi-pub")
	subA := connectClient(t, broker, "multi-a")
	subB := connectClient(t, broker, "multi-b")

	chA := recvChan(t, subA, "fanout", 0)
	chB := recvChan(t, subB, "fanout", 0)
	mustPublish(t, pub, "fanout", 0, false, "broadcast")
	expectMsg(t, chA, "broadcast")
	expectMsg(t, chB, "broadcast")
}

func TestProtocolPersistentSession(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)

	// Subscriber connects with a persistent session (clean=false) at QoS1, then
	// disconnects. Messages published while offline must be queued and delivered
	// on reconnect.
	persistentOpts := func(id string) *pmqtt.ClientOptions {
		return pmqtt.NewClientOptions().
			AddBroker(broker).SetClientID(id).
			SetCleanSession(false).SetProtocolVersion(4).
			SetConnectTimeout(5 * time.Second)
	}

	sub := pmqtt.NewClient(persistentOpts("persist-sub"))
	if tok := sub.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("sub connect: %v", tok.Error())
	}
	if tok := sub.Subscribe("persist/topic", 1, nil); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	sub.Disconnect(100)

	// Publish while the subscriber is offline.
	pub := connectClient(t, broker, "persist-pub")
	mustPublish(t, pub, "persist/topic", 1, false, "queued-while-offline")

	// Reconnect with the same client id and a message handler; expect the queued
	// message to be delivered from the session's JetStream consumer.
	got := make(chan string, 1)
	ropts := persistentOpts("persist-sub").SetDefaultPublishHandler(func(_ pmqtt.Client, m pmqtt.Message) {
		got <- string(m.Payload())
	})
	sub2 := pmqtt.NewClient(ropts)
	if tok := sub2.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("sub reconnect: %v", tok.Error())
	}
	defer sub2.Disconnect(100)

	select {
	case msg := <-got:
		if msg != "queued-while-offline" {
			t.Fatalf("payload = %q, want queued-while-offline", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for queued message after reconnect")
	}
}

// NOTE: Will-message delivery on ungraceful disconnect needs a raw-socket
// client (paho's Disconnect is graceful, which suppresses the will) and is a
// TODO for a future raw-client test.
