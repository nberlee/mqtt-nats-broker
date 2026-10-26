//go:build mqtt_fulltests

// Tests for what is owed to a connection's Will when its close races a new
// connection for the same client ID.
//
// Two goroutines decide a Will's fate: the closing connection's teardown and a
// CONNECT taking the session over. Before the disposition state machine
// (mqttWillDisposition) the loser of that race silently dropped the Will —
// mqttHandleClosedClient gave up as soon as it could not take the session gate,
// and the takeover cleared the Will unconditionally. These tests pin both
// orderings, and they assert the exact number of Will publications rather than
// "at least one": the teardown runs twice for a graceful DISCONNECT, so
// publishing once is as much the property under test as publishing at all.

package mqtt

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// testMQTTSetTeardownSeam installs (or clears) the instance-scoped seam that
// runs in mqttHandleClosedClient after the closing connection has classified
// what its Will is owed and before it contends for the session gate.
func testMQTTSetTeardownSeam(asm *mqttAccountSessionManager, fn func(clientID string)) {
	asm.mu.Lock()
	asm.testBeforeTeardownLock = fn
	asm.mu.Unlock()
}

// testMQTTSetResumeNakSeam installs (or clears) the instance-scoped seam that
// runs on the resume path after subscriptions are restored and before
// resendPendingRedelivery NAKs anything.
func testMQTTSetResumeNakSeam(asm *mqttAccountSessionManager, fn func(clientID string)) {
	asm.mu.Lock()
	asm.testBeforeResumeNaks = fn
	asm.mu.Unlock()
}

// testMQTTTeardownBarrier is a one-shot barrier for the teardown seam: the first
// teardown of clientID parks in it until released, and every later one (the
// second invocation a graceful DISCONNECT always produces) passes straight
// through.
type testMQTTTeardownBarrier struct {
	clientID string
	reached  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func testMQTTNewTeardownBarrier(clientID string) *testMQTTTeardownBarrier {
	return &testMQTTTeardownBarrier{
		clientID: clientID,
		reached:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (b *testMQTTTeardownBarrier) hook(clientID string) {
	if clientID != b.clientID {
		return
	}
	b.once.Do(func() { close(b.reached) })
	<-b.release
}

// waitReached blocks until the teardown of the barrier's client ID has parked.
func (b *testMQTTTeardownBarrier) waitReached(t testing.TB) {
	t.Helper()
	select {
	case <-b.reached:
	case <-time.After(testMQTTTimeout):
		t.Fatal("the closing connection's teardown never reached the barrier")
	}
}

func (b *testMQTTTeardownBarrier) unblock() { close(b.release) }

// testMQTTSetTakeoverDiscardSeam installs (or clears) the instance-scoped seam
// that runs between a takeover rebinding the session and it deciding the
// displaced connection's Will.
func testMQTTSetTakeoverDiscardSeam(asm *mqttAccountSessionManager, fn func(clientID string)) {
	asm.mu.Lock()
	asm.testBeforeTakeoverDiscard = fn
	asm.mu.Unlock()
}

// testMQTTCountPublishes counts the PUBLISH packets arriving on r until it goes
// quiet, waiting first for one to show up at all and then idle between any
// further ones. It is how these tests say "exactly one Will" instead of "a Will
// arrived": a second copy is a failure, and only a quiet reader proves there
// isn't one.
func testMQTTCountPublishes(t testing.TB, r *mqttReader, wantTopic string, first, idle time.Duration) int {
	t.Helper()
	n, timeout := 0, first
	for {
		topic, _, _, _, ok := testMQTTTryReadPublishV5(r, timeout)
		if !ok {
			return n
		}
		if topic != wantTopic {
			t.Fatalf("expected PUBLISH on %q, got one on %q", wantTopic, topic)
		}
		n++
		timeout = idle
	}
}

// testMQTTWillWitness connects a subscriber on its own client ID and subscribes
// it to topic at QoS 0. It exists to observe Will publications without being the
// client whose Will is under test.
func testMQTTWillWitness(t testing.TB, o *Options, id, topic string) (*mqttReader, func()) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 0}})
	return r, func() { c.Close() }
}

// TestMQTTWillDispositionRacingTakeover covers the orderings between a
// connection closing with a Will and a same-client-ID CONNECT taking its session
// over. Spec5 [3.1.2.5], [3.14.4], [MQTT-3.1.2-8].
func TestMQTTWillDispositionRacingTakeover(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	const topic = "wdisp/topic"

	// A DISCONNECT with Reason Code 0x04 makes the Will owed immediately. A
	// reconnect arriving before the teardown has finished is too late to cancel
	// it: the Will Delay Interval it would have to be cancelled within is zero and
	// already elapsed. Spec5 [3.14.4].
	t.Run("disconnect with will, reconnect wins the race", func(t *testing.T) {
		rwit, closeWit := testMQTTWillWitness(t, o, "wdisp-w1", topic)
		defer closeWit()

		will := &mqttWill{topic: []byte(topic), message: []byte("owed"), qos: 0}
		cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdisp1", cleanStart: true, will: will},
			o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, rw)

		barrier := testMQTTNewTeardownBarrier("wdisp1")
		testMQTTSetTeardownSeam(asm, barrier.hook)
		defer testMQTTSetTeardownSeam(asm, nil)

		if _, err := testMQTTWrite(cw, []byte{mqttPacketDisconnect, 1, mqttReasonDisconnectWithWill}); err != nil {
			t.Fatalf("Error writing DISCONNECT: %v", err)
		}
		cw.Close()
		barrier.waitReached(t)

		// The reconnect completes end to end while the teardown is parked, so it
		// takes the session over before the Will has been published.
		cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdisp1", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)
		barrier.unblock()

		if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
			t.Fatalf("expected exactly 1 Will publication, got %d", n)
		}
	})

	// An abnormal close owes the Will just as much, and the teardown is where that
	// is observed. This pins the ordering "the close was observed first, then the
	// session was rebound" — the other ordering is the plain takeover below.
	t.Run("abnormal close observed first, then reconnect", func(t *testing.T) {
		rwit, closeWit := testMQTTWillWitness(t, o, "wdisp-w2", topic)
		defer closeWit()

		will := &mqttWill{topic: []byte(topic), message: []byte("dropped"), qos: 0}
		cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdisp2", cleanStart: true, will: will},
			o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, rw)

		barrier := testMQTTNewTeardownBarrier("wdisp2")
		testMQTTSetTeardownSeam(asm, barrier.hook)
		defer testMQTTSetTeardownSeam(asm, nil)

		cw.Close() // no DISCONNECT at all
		barrier.waitReached(t)

		cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdisp2", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)
		barrier.unblock()

		if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
			t.Fatalf("expected exactly 1 Will publication, got %d", n)
		}
	})

	// The other ordering: the CONNECT arrives while the connection is still up, so
	// nothing has observed a close and the Will is still armed. A takeover is not
	// a Will publication trigger, and it is the takeover itself that closes the
	// displaced connection, so the Will goes away with it.
	t.Run("takeover of a live connection publishes nothing", func(t *testing.T) {
		rwit, closeWit := testMQTTWillWitness(t, o, "wdisp-w3", topic)
		defer closeWit()

		will := &mqttWill{topic: []byte(topic), message: []byte("takenover"), qos: 0}
		cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdisp3", cleanStart: true, will: will},
			o.MQTT.Host, o.MQTT.Port)
		defer cw.Close()
		testMQTTReadConnAckV5(t, rw)

		cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdisp3", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)

		if n := testMQTTCountPublishes(t, rwit, topic, time.Second, 250*time.Millisecond); n != 0 {
			t.Fatalf("expected no Will publication on takeover, got %d", n)
		}
	})

	// A normal DISCONNECT discards the Will, and a racing reconnect must not
	// resurrect it. Spec [MQTT-3.1.2-8].
	t.Run("normal disconnect publishes nothing", func(t *testing.T) {
		rwit, closeWit := testMQTTWillWitness(t, o, "wdisp-w4", topic)
		defer closeWit()

		will := &mqttWill{topic: []byte(topic), message: []byte("discarded"), qos: 0}
		cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdisp4", cleanStart: true, will: will},
			o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, rw)

		barrier := testMQTTNewTeardownBarrier("wdisp4")
		testMQTTSetTeardownSeam(asm, barrier.hook)
		defer testMQTTSetTeardownSeam(asm, nil)

		// Written directly rather than through testMQTTDisconnect: that helper waits
		// for the server to close the connection, which the parked teardown has not
		// got to yet. A v5 DISCONNECT with no reason code is the normal one.
		if _, err := testMQTTWrite(cw, []byte{mqttPacketDisconnect, 0}); err != nil {
			t.Fatalf("Error writing DISCONNECT: %v", err)
		}
		cw.Close()
		barrier.waitReached(t)

		cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdisp4", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)
		barrier.unblock()

		if n := testMQTTCountPublishes(t, rwit, topic, time.Second, 250*time.Millisecond); n != 0 {
			t.Fatalf("expected no Will publication after a normal DISCONNECT, got %d", n)
		}
	})
}

// TestMQTTWillDispositionDelayed covers the delayed half of the disposition:
// a Will Delay Interval is exactly the window in which a new connection may
// still cancel a Will, so an owed-but-delayed Will stays cancellable — and,
// when nobody reconnects, still gets published exactly once. Spec5 [3.1.2.5].
func TestMQTTWillDispositionDelayed(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "wdispd/topic"

	t.Run("disconnect with will honours the delay and is cancelled by a reconnect", func(t *testing.T) {
		rwit, closeWit := testMQTTWillWitness(t, o, "wdispd-w1", topic)
		defer closeWit()

		will := &mqttWill{topic: []byte(topic), message: []byte("delayed"), qos: 0}
		cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdispd1", cleanStart: true, will: will,
			willProps: mqttV5WillDelayProps(2), props: mqttV5ConnPropsSessionExpiry(30)}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, rw)

		if _, err := testMQTTWrite(cw, []byte{mqttPacketDisconnect, 1, mqttReasonDisconnectWithWill}); err != nil {
			t.Fatalf("Error writing DISCONNECT: %v", err)
		}
		cw.Close()
		// Reason 0x04 still defers: it says publish the Will, not publish it now.
		checkFor(t, testMQTTTimeout, 15*time.Millisecond, func() error {
			if n := testMQTTNumPendingWills(t, s, "wdispd-w1"); n != 1 {
				return fmt.Errorf("expected 1 pending will, got %d", n)
			}
			return nil
		})

		cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdispd1", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)

		if n := testMQTTCountPublishes(t, rwit, topic, 3*time.Second, 250*time.Millisecond); n != 0 {
			t.Fatalf("expected the delayed Will to be cancelled by the reconnect, got %d publications", n)
		}
	})

	// The same Will with nobody reconnecting: the teardown must still hand it to
	// the durable outbox. Consuming the Will and publishing it are one operation,
	// so a delayed disposition that was consumed but never enqueued would look
	// exactly like a cancelled one — this is what tells them apart.
	t.Run("disconnect with will publishes once after the delay", func(t *testing.T) {
		rwit, closeWit := testMQTTWillWitness(t, o, "wdispd-w2", topic)
		defer closeWit()

		will := &mqttWill{topic: []byte(topic), message: []byte("fired"), qos: 0}
		cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wdispd2", cleanStart: true, will: will,
			willProps: mqttV5WillDelayProps(1), props: mqttV5ConnPropsSessionExpiry(30)}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, rw)

		if _, err := testMQTTWrite(cw, []byte{mqttPacketDisconnect, 1, mqttReasonDisconnectWithWill}); err != nil {
			t.Fatalf("Error writing DISCONNECT: %v", err)
		}
		cw.Close()

		if n := testMQTTCountPublishes(t, rwit, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
			t.Fatalf("expected exactly 1 delayed Will publication, got %d", n)
		}
	})
}

// TestMQTTWillDispositionUsesOwnSessionExpiry pins WHOSE Session Expiry Interval
// classifies a closing connection's Will.
//
// A takeover overwrites the session's interval with the replacement's the moment
// it rebinds, and only then decides the displaced connection's Will — so for a
// moment the session carries one connection's Will and another's interval.
// Classifying from the session there gets it backwards in both directions, and
// both are checked here.
//
// The interleaving is forced, not hoped for: the CONNECT parks between rebinding
// and discarding, the displaced connection then closes and classifies (parking in
// turn at the teardown barrier, which is how the test knows classification has
// happened), and only then is the CONNECT released to make its decision.
func TestMQTTWillDispositionUsesOwnSessionExpiry(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	const topic = "wdispx/topic"

	// race drives the interleaving above. oldExpiry/newExpiry are the two
	// connections' Session Expiry Intervals; every Will here carries a Will Delay
	// Interval, so which of the two intervals is used decides delayed vs immediate.
	race := func(t *testing.T, id string, oldExpiry, newExpiry uint32, payload string) int {
		t.Helper()
		rwit, closeWit := testMQTTWillWitness(t, o, id+"-w", topic)
		defer closeWit()

		will := &mqttWill{topic: []byte(topic), message: []byte(payload), qos: 0}
		old := &mqttV5ConnInfo{clientID: id, cleanStart: true, will: will,
			willProps: mqttV5WillDelayProps(2)}
		if oldExpiry > 0 {
			old.props = mqttV5ConnPropsSessionExpiry(oldExpiry)
		}
		cw, rw := testMQTTConnectV5(t, old, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, rw)

		teardown := testMQTTNewTeardownBarrier(id)
		testMQTTSetTeardownSeam(asm, teardown.hook)
		defer testMQTTSetTeardownSeam(asm, nil)

		rebound := make(chan struct{})
		releaseTakeover := make(chan struct{})
		var once sync.Once
		testMQTTSetTakeoverDiscardSeam(asm, func(cid string) {
			if cid != id {
				return
			}
			once.Do(func() { close(rebound) })
			<-releaseTakeover
		})
		defer testMQTTSetTakeoverDiscardSeam(asm, nil)

		// The replacement runs on its own goroutine: it parks mid-takeover, so the
		// test has to keep going to drive the other side of the race.
		type connected struct {
			c net.Conn
			r *mqttReader
		}
		done := make(chan connected, 1)
		go func() {
			repl := &mqttV5ConnInfo{clientID: id, cleanStart: true}
			if newExpiry > 0 {
				repl.props = mqttV5ConnPropsSessionExpiry(newExpiry)
			}
			c, r := testMQTTConnectV5(t, repl, o.MQTT.Host, o.MQTT.Port)
			done <- connected{c, r}
		}()

		select {
		case <-rebound:
		case <-time.After(testMQTTTimeout):
			t.Fatal("the replacement CONNECT never reached the takeover barrier")
		}

		// Session rebound, replacement's interval already stored. Now the displaced
		// connection closes and classifies its Will.
		cw.Close()
		teardown.waitReached(t)

		releaseClose := func() { close(releaseTakeover) }
		releaseClose()
		cs := <-done
		defer cs.c.Close()
		testMQTTReadConnAckV5(t, cs.r)
		teardown.unblock()

		return testMQTTCountPublishes(t, rwit, topic, 3*time.Second, 500*time.Millisecond)
	}

	// The displaced connection's own interval is non-zero, so its Will is delayed
	// — and a delayed Will is exactly what the reconnect is entitled to cancel.
	// Reading the replacement's zero interval instead would call it immediate,
	// which the takeover then preserves and publishes.
	t.Run("delayed will is not made immediate by the replacement", func(t *testing.T) {
		if n := race(t, "wdispx1", 30, 0, "must-not-fire"); n != 0 {
			t.Fatalf("expected the delayed Will to be cancelled by the reconnect, got %d publications", n)
		}
	})

	// The mirror image: the displaced connection's interval is zero, so its Will
	// is due immediately whatever its Will Delay says, and the takeover may not
	// cancel it. Reading the replacement's interval would call it delayed and
	// discard it.
	t.Run("immediate will is not made delayed by the replacement", func(t *testing.T) {
		if n := race(t, "wdispx2", 0, 30, "must-fire"); n != 1 {
			t.Fatalf("expected exactly 1 Will publication, got %d", n)
		}
	})
}

// TestMQTTResumeDoesNotNakFreshDelivery covers the resume side: NAK'ing the
// session's pending deliveries to force immediate redelivery [MQTT-4.4.0-1] must
// not catch a delivery that only started during the resume itself. Such a
// message is being transmitted for the first time, and NAK'ing it hands the
// client a duplicate it was never owed.
func TestMQTTResumeDoesNotNakFreshDelivery(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	const clientID, topic = "resumenak", "resumenak/topic"

	// A persistent session with a QoS 1 subscription, then offline. A v5 session
	// only survives the connection with a non-zero Session Expiry Interval; Clean
	// Start 0 on its own is not enough. Spec5 [3.1.2.11.2].
	persist := &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(30)}
	cs, rs := testMQTTConnectV5(t, persist, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	testMQTTDisconnect(t, cs, nil)
	cs.Close()

	// Published while offline, so the delivery starts when the resume restores
	// the subscription — precisely the window resendPendingRedelivery runs in.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "resumenak-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 1, false, 1, topic, []byte("fresh"), nil)

	// Hold the resume open until that fresh delivery is demonstrably tracked, so
	// the NAK pass runs with it in the pending map every single time rather than
	// once in a while.
	testMQTTSetResumeNakSeam(asm, func(cid string) {
		if cid != clientID {
			return
		}
		asm.mu.RLock()
		sess := asm.sessions[cid]
		asm.mu.RUnlock()
		if sess == nil {
			return
		}
		deadline := time.Now().Add(testMQTTTimeout)
		for time.Now().Before(deadline) {
			sess.mu.Lock()
			n := len(sess.pendingPublish)
			sess.mu.Unlock()
			if n > 0 {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	defer testMQTTSetResumeNakSeam(asm, nil)

	c2, r2 := testMQTTConnectV5(t, persist, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if sp, _, _ := testMQTTReadConnAckV5(t, r2); !sp {
		t.Fatal("Expected session present on resume")
	}

	// Deliberately not acknowledged: an unacked message is what a NAK would
	// duplicate, and the default AckWait is far longer than this window, so a
	// second copy could only come from the NAK pass.
	if n := testMQTTCountPublishes(t, r2, topic, testMQTTTimeout, 500*time.Millisecond); n != 1 {
		t.Fatalf("expected the message exactly once across the resume, got %d", n)
	}
}
