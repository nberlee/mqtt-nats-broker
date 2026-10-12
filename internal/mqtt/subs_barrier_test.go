package mqtt

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// testSubsBarrierOf returns the subs barrier of the account the MQTT client cid is bound
// to. It is the untagged counterpart of testMQTTGetAccountSessionManager, which lives in the
// tagged suite. The barrier is a value field, so the pointer is what a sampled hook sees.
func testSubsBarrierOf(t *testing.T, s *Server, cid string) *mqttSubsBarrier {
	t.Helper()
	var asm *mqttAccountSessionManager
	s.mu.Lock()
	for _, c := range s.clients {
		c.mu.Lock()
		if c.isMqtt() && c.mqtt.cid == cid {
			asm = c.mqtt.asm
		}
		c.mu.Unlock()
		if asm != nil {
			break
		}
	}
	s.mu.Unlock()
	if asm == nil {
		t.Fatalf("no account session manager for MQTT client %q", cid)
	}
	return &asm.jsa.subsBarrier
}

// testSubsBarrierObserveSamples installs the sampled hook for barrier b only and returns the
// channel each of b's waits reports its sample size on. The hook is gated so that it cannot
// outlive the test.
func testSubsBarrierObserveSamples(t *testing.T, b *mqttSubsBarrier) <-chan int {
	t.Helper()
	samples := make(chan int, 16)
	gate := newTestHookGate(t)
	restore := setMQTTTestSubsBarrierSampledHook(func(sb *mqttSubsBarrier, sampled int) {
		if sb != b || !gate.enter() {
			return
		}
		defer gate.leave()
		samples <- sampled
	})
	t.Cleanup(restore)
	return samples
}

func testSubsBarrierNextSample(t *testing.T, samples <-chan int, want int, what string) {
	t.Helper()
	select {
	case got := <-samples:
		if got != want {
			t.Fatalf("%s sampled %d live entries, want %d", what, got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never sampled the barrier", what)
	}
}

// TestMQTTSubsBarrierWaitIgnoresLaterEntrants pins the barrier's contract on the type: a
// wait covers exactly the entries live when it sampled. An entry that arrives after the
// sample cannot extend the wait, which is what let continuously overlapping SUBSCRIBEs hold
// a QoS 1/2 store (and a Will phase with its gate) for ever; and an entry that WAS sampled
// still holds it, which is the interest-retention guarantee the barrier exists for.
//
// The ordering "B entered after W1 sampled" is established by the sampled seam, not by a
// sleep: a waiter delayed past B's entry would legitimately include B, and a timed
// observation could not tell that apart from the defect.
//
// NOT parallel: it installs a process-global hook.
func TestMQTTSubsBarrierWaitIgnoresLaterEntrants(t *testing.T) {
	b := &mqttSubsBarrier{}
	samples := testSubsBarrierObserveSamples(t, b)

	waitAsync := func(abort <-chan struct{}) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			b.wait(abort)
			close(done)
		}()
		return done
	}
	mustReturn := func(done <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal(what)
		}
	}
	mustHold := func(done <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-done:
			t.Fatal(what)
		case <-time.After(250 * time.Millisecond):
		}
	}

	// An empty barrier is a single uncontended check.
	mustReturn(waitAsync(nil), "a wait on an empty barrier must return at once")
	testSubsBarrierNextSample(t, samples, 0, "the wait on an empty barrier")

	a := b.enter()
	w1 := waitAsync(nil)
	testSubsBarrierNextSample(t, samples, 1, "W1")

	// B enters strictly after W1's snapshot.
	bEntry := b.enter()
	mustHold(w1, "W1 returned while the entry it sampled (A) was still live")
	b.leave(a)
	mustReturn(w1, "W1 did not return once the only entry it sampled left: an entry that "+
		"arrived after its sample (B) extended the wait, which is the defect that let "+
		"overlapping SUBSCRIBEs hold a store indefinitely")

	// B was live at W2's sample, so W2 must wait for it.
	w2 := waitAsync(nil)
	testSubsBarrierNextSample(t, samples, 1, "W2")
	mustHold(w2, "W2 returned while the entry it sampled (B) was still live: a racing "+
		"SUBSCRIBE's consumer would not exist when the store lands, and interest retention "+
		"discards the message")
	b.leave(bEntry)
	mustReturn(w2, "W2 did not return after the entry it sampled left")

	// Shutdown's abort releases a waiter whose sampled entry is still held.
	held := b.enter()
	abort := make(chan struct{})
	w3 := waitAsync(abort)
	testSubsBarrierNextSample(t, samples, 1, "W3")
	mustHold(w3, "W3 returned while its sampled entry was held and abort was open")
	close(abort)
	mustReturn(w3, "closing abort did not release a waiter blocked on a held entry")
	b.leave(held)
}

// TestV5PublishWaitsOnlyForSubscribesItSampled is the wire form of the contract above, on
// a real broker with real clients. Two SUBSCRIBEs on different topics are each held INSIDE
// the barrier, before their consumers exist:
//
//   - P1 is published while only SUB1 is held, and SUB2 enters only after P1 has sampled.
//     Releasing SUB1 must complete P1 (PUBACK and delivery) although SUB2 is still held;
//     before the fix P1 waited for SUB2 as well, so overlapping SUBSCRIBEs could hold it
//     for ever.
//   - P2 is published while SUB2 is held, so it sampled SUB2 and must NOT complete until
//     SUB2 is released, after which it is delivered: the barrier still protects every
//     SUBSCRIBE in flight at the sample.
//
// NOT parallel: it installs process-global hooks.
func TestV5PublishWaitsOnlyForSubscribesItSampled(t *testing.T) {
	broker, s := startBroker(t)

	got1 := make(chan *paho.Publish, 4)
	sub1 := v5Client(t, broker, "v5-sampled-sub1", func(pr paho.PublishReceived) (bool, error) {
		got1 <- pr.Packet
		return true, nil
	})
	got2 := make(chan *paho.Publish, 4)
	sub2 := v5Client(t, broker, "v5-sampled-sub2", func(pr paho.PublishReceived) (bool, error) {
		got2 <- pr.Packet
		return true, nil
	})
	pub := v5Client(t, broker, "v5-sampled-pub", nil)
	samples := testSubsBarrierObserveSamples(t, testSubsBarrierOf(t, s, "v5-sampled-pub"))

	const topic1, topic2 = "v5/sampled/one", "v5/sampled/two"
	type heldSub struct {
		subject string
		gate    *testHookGate
		once    sync.Once
		entered chan struct{}
		release chan struct{}
	}
	newHeld := func(subject string) *heldSub {
		return &heldSub{subject: subject, gate: newTestHookGate(t),
			entered: make(chan struct{}), release: make(chan struct{})}
	}
	// processSubs receives the filter in its NATS subject form.
	held := []*heldSub{newHeld("v5.sampled.one"), newHeld("v5.sampled.two")}
	restore := setMQTTTestSubsBarrierHeldHook(func(filters []*mqttFilter) {
		for _, h := range held {
			for _, f := range filters {
				if f.filter != h.subject {
					continue
				}
				// The topic is chosen BEFORE its gate is entered: one shared gate would be
				// held across the release wait and serialize the two SUBSCRIBEs.
				if !h.gate.enter() {
					return
				}
				h.once.Do(func() { close(h.entered) })
				<-h.release
				h.gate.leave()
				return
			}
		}
	})
	releaseAll := func() {
		for _, h := range held {
			select {
			case <-h.release:
			default:
				close(h.release)
			}
		}
	}
	// Unblock before uninstalling, so a failing assertion never strands a SUBSCRIBE, and
	// before the gates (registered earlier) close and wait for the bodies.
	t.Cleanup(func() {
		releaseAll()
		restore()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	subscribe := func(c *paho.Client, topic string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := c.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}},
			})
			done <- err
		}()
		return done
	}
	publish := func(topic, payload string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := pub.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: []byte(payload)})
			done <- err
		}()
		return done
	}
	awaitEntered := func(h *heldSub, what string) {
		t.Helper()
		select {
		case <-h.entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never reached the barrier seam", what)
		}
	}
	mustComplete := func(done <-chan error, what string) {
		t.Helper()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never completed", what)
		}
	}
	mustBeDelivered := func(got <-chan *paho.Publish, want, what string) {
		t.Helper()
		select {
		case p := <-got:
			if string(p.Payload) != want {
				t.Fatalf("%s: payload %q, want %q", what, p.Payload, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s was never delivered: a message stored behind the barrier must "+
				"reach the subscription it waited for", what)
		}
	}

	sub1Done := subscribe(sub1, topic1)
	awaitEntered(held[0], "SUB1")

	p1 := publish(topic1, "p1")
	testSubsBarrierNextSample(t, samples, 1, "P1")

	// SUB2 enters strictly after P1's snapshot.
	sub2Done := subscribe(sub2, topic2)
	awaitEntered(held[1], "SUB2")

	close(held[0].release)
	mustComplete(sub1Done, "SUB1")
	select {
	case err := <-p1:
		if err != nil {
			t.Fatalf("P1: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("P1 did not complete once the only SUBSCRIBE it sampled was released: a " +
			"SUBSCRIBE that entered after the publish (SUB2, still held) extended its wait")
	}
	mustBeDelivered(got1, "p1", "P1")

	p2 := publish(topic2, "p2")
	testSubsBarrierNextSample(t, samples, 1, "P2")
	select {
	case err := <-p2:
		t.Fatalf("P2 completed while the SUBSCRIBE it sampled (SUB2) was still in flight, "+
			"so it was stored with no interest registered (err=%v)", err)
	case p := <-got2:
		t.Fatalf("nothing can be delivered before SUB2's consumer exists: %q", p.Payload)
	case <-time.After(750 * time.Millisecond):
	}

	close(held[1].release)
	mustComplete(sub2Done, "SUB2")
	mustComplete(p2, "P2 after SUB2 was released")
	mustBeDelivered(got2, "p2", "P2")
}
