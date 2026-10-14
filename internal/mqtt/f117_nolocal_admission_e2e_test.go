//go:build mqttport_fulltests

package mqtt

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// F-117: No Local is decided when the server takes OWNERSHIP of a message, not when the
// message is later forwarded. [MQTT-4.5.0-1] adds an owned message to the Session State of the
// clients whose subscriptions match at that moment; [MQTT-3.8.4-3] replaces a subscription
// with a NEW one, and [MQTT-3.8.4-4] forbids losing messages by doing so. So a re-SUBSCRIBE
// that changes No Local governs messages owned after it, and never re-judges one owned before.
//
// The transition tests own a self-published message under one No Local value, replace the
// subscription with the other value, and only then let the message reach the subscriber. The
// QoS 1 tests hold the delivery callback before its lock, so the replacement provably lands
// between ownership and forwarding; the QoS 2 tests need no hold, because ownership is at
// PUBREC and the replacement is sent before the PUBREL.
//
// A negative assertion is only made after the callback that decided it has RETURNED (the done
// hook), never after a bare timeout: another durable's delivery proves nothing about this one.
// NOT parallel: the seams are process-global, and every one is restored by t.Cleanup.

const f117NoLocal = byte(0x04)

// f117Exit is one QoS 1/2 delivery callback of the watched session that has returned, and
// whether it took the withheld (skipped) exit, which neither delivers nor acks.
type f117Exit struct {
	sub     *subscription
	skipped bool
}

// f117Watcher observes every QoS 1/2 delivery callback of one session, and can hold the first
// one before its lock.
type f117Watcher struct {
	clientID string
	parked   chan struct{}
	release  chan struct{}
	exits    chan f117Exit

	mu      sync.Mutex
	held    *subscription
	skipped map[*subscription]bool
}

func (w *f117Watcher) mine(sub *subscription) bool {
	return sub != nil && sub.client != nil && sub.client.mqtt != nil &&
		sub.client.mqtt.sess != nil && sub.client.mqtt.sess.id == w.clientID
}

// f117Watch installs the watcher. With park, the session's FIRST delivery callback is held
// at the pre-lock seam until release is closed.
func f117Watch(t *testing.T, clientID string, park bool) *f117Watcher {
	w := &f117Watcher{
		clientID: clientID,
		parked:   make(chan struct{}),
		release:  make(chan struct{}),
		exits:    make(chan f117Exit, 256),
		skipped:  map[*subscription]bool{},
	}
	if !park {
		close(w.release)
	}
	var parkOnce sync.Once
	pre := func(sub *subscription) {
		if !w.mine(sub) {
			return
		}
		w.mu.Lock()
		w.skipped[sub] = false
		hold := park && w.held == nil
		if hold {
			w.held = sub
		}
		w.mu.Unlock()
		if hold {
			parkOnce.Do(func() { close(w.parked) })
			<-w.release
		}
	}
	skipped := func(sub *subscription, _ string) {
		if !w.mine(sub) {
			return
		}
		w.mu.Lock()
		w.skipped[sub] = true
		w.mu.Unlock()
	}
	done := func(sub *subscription) {
		if !w.mine(sub) {
			return
		}
		w.mu.Lock()
		e := f117Exit{sub: sub, skipped: w.skipped[sub]}
		w.mu.Unlock()
		select {
		case w.exits <- e:
		default:
		}
	}
	restorePre := setMQTTTestDeliveryPreLockHook(pre)
	restoreSkipped := setMQTTTestDeliverySkippedHook(skipped)
	restoreDone := setMQTTTestDeliveryDoneHook(done)
	t.Cleanup(func() {
		select {
		case <-w.release:
		default:
			close(w.release)
		}
		restorePre()
		restoreSkipped()
		restoreDone()
	})
	return w
}

// heldSub is the parked callback's subscription.
func (w *f117Watcher) heldSub() *subscription {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.held
}

// awaitExit waits for the next returned callback that satisfies match.
func (w *f117Watcher) awaitExit(t *testing.T, what string, match func(f117Exit) bool) f117Exit {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case e := <-w.exits:
			if match(e) {
				return e
			}
		case <-deadline:
			t.Fatalf("fixture: %s never happened", what)
		}
	}
}

func f117Await(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("fixture: %s never happened", what)
	}
}

func f117Connect(t *testing.T, o *Options, clientID string) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	testMQTTReadConnAckV5(t, r)
	return c, r
}

func f117Session(t *testing.T, s *Server, clientID string) *mqttSession {
	t.Helper()
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("fixture: no resident session for %q", clientID)
	}
	return sess
}

// f117NextNonPublish reads the next packet that is not a PUBLISH, skipping any PUBLISH frames
// before it, and reports how many it skipped.
func f117NextNonPublish(t *testing.T, r *mqttReader) (b byte, pl, skipped int) {
	t.Helper()
	for {
		b, pl = testMQTTReadPacket(t, r)
		if b&mqttPacketMask != mqttPacketPub {
			return b, pl, skipped
		}
		r.pos += pl
		skipped++
	}
}

// f117ReadPubResponse reads the identifier and reason code of a v5 PUBREC or PUBCOMP of type
// want, after any PUBLISH frames.
func f117ReadPubResponse(t *testing.T, r *mqttReader, want byte, pi uint16) (reason byte, skipped int) {
	t.Helper()
	b, pl, skipped := f117NextNonPublish(t, r)
	if b&mqttPacketMask != want {
		t.Fatalf("Expected packet type 0x%x, got 0x%x", want, b&mqttPacketMask)
	}
	start := r.pos
	rpi, err := r.readUint16("pi")
	if err != nil || rpi != pi {
		t.Fatalf("Expected pi=%v, got %v (err=%v)", pi, rpi, err)
	}
	reason = mqttReasonSuccess
	if r.pos-start < pl {
		if reason, err = r.readByte("reason"); err != nil {
			t.Fatalf("Error reading reason: %v", err)
		}
	}
	r.pos = start + pl
	return reason, skipped
}

// An own QoS 1 message owned while No Local is OFF was admitted to the session; turning No
// Local ON before it is forwarded must not destroy it. [MQTT-3.8.4-4].
func TestF117QoS1OwnedBeforeNoLocalOnIsDelivered(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const id, topic = "f117-on", "f117/on"
	c, r := f117Connect(t, o, id)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})

	w := f117Watch(t, id, true)
	testMQTTPubV5Props(t, c, r, 1, false, 2, topic, []byte("owned-off"), nil)
	f117Await(t, w.parked, "the own message's delivery reaching the pre-lock seam")

	testMQTTSubV5(t, c, r, 3, []mqttV5SubFilter{{topic: topic, opts: 1 | f117NoLocal}})
	close(w.release)
	held := w.heldSub()
	w.awaitExit(t, "the held delivery callback returning", func(e f117Exit) bool { return e.sub == held })

	testMQTTReadPubV5Props(t, c, r, topic, []byte("owned-off"))
}

// An own QoS 1 message owned while No Local is ON was never admitted; turning No Local OFF
// before its delivery callback runs must not forward it. [MQTT-3.8.3-3].
func TestF117QoS1OwnedUnderNoLocalStaysSuppressed(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const id, topic = "f117-off", "f117/off"
	c, r := f117Connect(t, o, id)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1 | f117NoLocal}})

	w := f117Watch(t, id, true)
	testMQTTPubV5Props(t, c, r, 1, false, 2, topic, []byte("owned-on"), nil)
	f117Await(t, w.parked, "the own message's delivery reaching the pre-lock seam")

	testMQTTSubV5(t, c, r, 3, []mqttV5SubFilter{{topic: topic, opts: 1}})
	close(w.release)
	held := w.heldSub()
	w.awaitExit(t, "the held delivery callback returning", func(e f117Exit) bool { return e.sub == held })

	// The callback has had its turn, so silence here is its decision, not a timeout. The
	// following own publish is owned under the replacement and must be delivered, which also
	// proves the subscription is live.
	testMQTTExpectNothing(t, r)
	testMQTTPubV5SelfEcho(t, c, r, 4, topic, []byte("owned-after"))
}

// QoS 2 ownership is at PUBREC ([MQTT-4.3.3]), so a replacement sent between PUBREC and
// PUBREL changes nothing for that message, in either direction.
func TestF117QoS2OwnershipIsAtPubRec(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	pubrecThenResubscribe := func(t *testing.T, c net.Conn, r *mqttReader, pi uint16, topic string, payload []byte, opts byte) {
		t.Helper()
		vh := newMQTTWriter(0)
		vh.WriteBytes([]byte(topic))
		vh.WriteUint16(pi)
		vh.WriteVarInt(0)
		vh.Write(payload)
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketPub | (2 << 1))
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
			t.Fatalf("Error writing PUBLISH: %v", err)
		}
		testMQTTReadPIPacket(mqttPacketPubRec, t, r, pi)
		testMQTTSubV5(t, c, r, pi+1, []mqttV5SubFilter{{topic: topic, opts: opts}})
		pubrel := [4]byte{mqttPacketPubRel | 0x2, 0x2, byte(pi >> 8), byte(pi)}
		if _, err := testMQTTWrite(c, pubrel[:]); err != nil {
			t.Fatalf("Error writing PUBREL: %v", err)
		}
		testMQTTReadPIPacket(mqttPacketPubComp, t, r, pi)
	}

	t.Run("owned with No Local OFF is delivered", func(t *testing.T) {
		const topic = "f117/q2on"
		c, r := f117Connect(t, o, "f117-q2on")
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
		pubrecThenResubscribe(t, c, r, 10, topic, []byte("q2-owned-off"), 1|f117NoLocal)
		testMQTTReadPubV5Props(t, c, r, topic, []byte("q2-owned-off"))
	})

	t.Run("owned with No Local ON is suppressed", func(t *testing.T) {
		const topic = "f117/q2off"
		c, r := f117Connect(t, o, "f117-q2off")
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1 | f117NoLocal}})
		w := f117Watch(t, "f117-q2off", false) // observe, do not hold
		pubrecThenResubscribe(t, c, r, 10, topic, []byte("q2-owned-on"), 1)
		w.awaitExit(t, "the released message's delivery callback returning", func(e f117Exit) bool { return !e.skipped })
		testMQTTExpectNothing(t, r)
		testMQTTPubV5SelfEcho(t, c, r, 20, topic, []byte("q2-owned-after"))
	})
}

// No Local is per SUBSCRIPTION: when two of the publisher's own filters overlap and only one
// has No Local, the other still admits the message. [MQTT-3.8.3-3] binds the exclusion to the
// subscription that carries the option.
func TestF117OverlappingFiltersAreJudgedPerFilter(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const id = "f117-ov"
	c, r := f117Connect(t, o, id)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{
		{topic: "f117/ov/#", opts: 1 | f117NoLocal},
		{topic: "f117/ov/x", opts: 1},
	})

	w := f117Watch(t, id, false)
	// Exactly one echo, through the filter without No Local.
	testMQTTPubV5SelfEcho(t, c, r, 2, "f117/ov/x", []byte("overlap"))
	// Both filters' callbacks have had their turn before silence is asserted.
	seen := map[*subscription]bool{}
	for len(seen) < 2 {
		e := w.awaitExit(t, "both overlapping filters' delivery callbacks returning", func(e f117Exit) bool { return !e.skipped })
		seen[e.sub] = true
	}
	testMQTTExpectNothing(t, r)
}

// A multi-level filter also receives its parent level, through its level-up companion, and
// the publisher's exclusion must reach that companion too, including under a single-level
// wildcard (`f117/w/+/#` receives `f117/w/x`).
func TestF117FwcParentLevelIsStamped(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for i, tc := range []struct{ name, filter, topic string }{
		{"literal parent", "f117/p/#", "f117/p"},
		{"wildcard parent", "f117/w/+/#", "f117/w/x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := fmt.Sprintf("f117-fwc-%d", i)
			control := fmt.Sprintf("f117/ctl/%d", i)
			c, r := f117Connect(t, o, id)
			testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{
				{topic: tc.filter, opts: 1 | f117NoLocal},
				{topic: control, opts: 1},
			})
			w := f117Watch(t, id, false)
			testMQTTPubV5Props(t, c, r, 1, false, 2, tc.topic, []byte("parent"), nil)
			w.awaitExit(t, "the companion's delivery callback returning", func(e f117Exit) bool { return !e.skipped })
			testMQTTExpectNothing(t, r)
			testMQTTPubV5SelfEcho(t, c, r, 3, control, []byte("live"))
		})
	}
}

// The stamp names the filter's session KEY, and a restored level-up companion must carry that
// key rather than its own shortened subject, or an owned backlog excluded before the
// disconnect is delivered after the resume.
func TestF117RestoredFwcBacklogKeepsItsStamp(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const id, control = "f117-restore", "f117/rctl"
	ci := &mqttV5ConnInfo{clientID: id, props: mqttV5ConnPropsSessionExpiry(30)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{
		{topic: "f117/r/#", opts: 1 | f117NoLocal},
		{topic: control, opts: 1},
	})

	sess := f117Session(t, s, id)
	sess.mu.Lock()
	oldClient := sess.c
	sess.mu.Unlock()
	if oldClient == nil {
		t.Fatal("fixture: the session has no attached connection")
	}

	// Owned and excluded; only the companion's consumer matches the parent level.
	old := f117Watch(t, id, true)
	testMQTTPubV5Props(t, c, r, 1, false, 2, "f117/r", []byte("backlog"), nil)
	f117Await(t, old.parked, "the backlog's delivery reaching the pre-lock seam")

	// The server must have detached the connection before the held callback resumes, or the
	// callback could still decide on the live subscription and ack the message away here.
	c.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		sess.mu.Lock()
		detached := sess.c != oldClient
		sess.mu.Unlock()
		if detached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture: the server never detached the closed connection")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(old.release)
	held := old.heldSub()
	if e := old.awaitExit(t, "the held callback returning", func(e f117Exit) bool { return e.sub == held }); !e.skipped {
		t.Fatal("fixture: the held callback did not take the withheld exit, so it delivered or acked the backlog")
	}

	// Installed BEFORE the reconnect: a restored callback captures the done hook at entry,
	// before waiting on the phase gate, so a watcher installed afterwards could miss it.
	restored := f117Watch(t, id, false)
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if sp, _, _ := testMQTTReadConnAckV5(t, r2); !sp {
		t.Fatal("fixture: expected Session Present on the resume")
	}
	restored.awaitExit(t, "the restored companion's redelivery callback returning", func(e f117Exit) bool {
		return e.sub != held && !e.skipped
	})
	testMQTTExpectNothing(t, r2)
	testMQTTPubV5SelfEcho(t, c2, r2, 5, control, []byte("live"))
}

// A retransmitted QoS 2 PUBLISH on an identifier the broker already holds is answered from
// the hold, whose stamp was budgeted when it was accepted; a stamp that only a NEW hold would
// write must not refuse it. A fresh identifier is still refused for it.
func TestF117DupQoS2NotRefusedForAGrownStamp(t *testing.T) {
	const topic = "f117/d/a/b/c/d/e"
	payload := []byte("dup-boundary")
	subj, err := mqttTopicToNATSPubSubject([]byte(topic))
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	pp := &mqttPublish{topic: []byte(topic), subject: subj, msg: payload, sz: len(payload)}
	stampOf := func(n int) string {
		if n == 0 {
			return _EMPTY_
		}
		return strings.TrimSuffix(strings.Repeat(strings.Repeat("A", 22)+",", n), ",")
	}
	maxFor := func(n int) int {
		st := stampOf(n)
		return mqttMaxQoS2FormSize(pp, mqttComputeNatsMsgSize(pp, false, 0, st, false, _EMPTY_, _EMPTY_), false, st)
	}
	base := maxFor(0)
	n := 1
	for maxFor(n) <= base {
		n++
		if n > 64 {
			t.Fatalf("fixture: no stamp of up to 64 tags moves the QoS 2 budget past %d", base)
		}
	}
	if !(maxFor(n) > base) {
		t.Fatalf("fixture: %d tags do not move the budget past %d", n, base)
	}

	// N distinct filters, all matching the topic: each level literal or '+'.
	levels := strings.Split(topic, "/")
	var filters []mqttV5SubFilter
	for mask := 0; len(filters) < n && mask < 1<<len(levels); mask++ {
		parts := make([]string, len(levels))
		for i, l := range levels {
			if mask&(1<<i) != 0 {
				parts[i] = "+"
			} else {
				parts[i] = l
			}
		}
		filters = append(filters, mqttV5SubFilter{topic: strings.Join(parts, "/"), opts: f117NoLocal}) // QoS 0: no consumers
	}
	if len(filters) < n {
		t.Fatalf("fixture: only %d distinct filters for %d tags", len(filters), n)
	}

	o := testMQTTDefaultOptionsV5()
	o.MaxPayload = int32(base)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	c, r := f117Connect(t, o, "f117-dup")

	send := func(pi uint16, dup bool) {
		t.Helper()
		vh := newMQTTWriter(0)
		vh.WriteBytes([]byte(topic))
		vh.WriteUint16(pi)
		vh.WriteVarInt(0)
		vh.Write(payload)
		flags := byte(2 << 1)
		if dup {
			flags |= 0x08
		}
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketPub | flags)
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
			t.Fatalf("Error writing PUBLISH: %v", err)
		}
	}

	send(10, false)
	if reason, _ := testMQTTReadPubRecV5(t, r, 10); reason != mqttReasonSuccess {
		t.Fatalf("fixture: the unstamped PUBLISH must fit, got PUBREC 0x%x", reason)
	}
	testMQTTSubV5(t, c, r, 11, filters)

	send(10, true)
	if reason, _ := testMQTTReadPubRecV5(t, r, 10); reason != mqttReasonSuccess {
		t.Fatalf("a DUP of a held exchange must be answered from its hold, got PUBREC 0x%x", reason)
	}
	pubrel := [4]byte{mqttPacketPubRel | 0x2, 0x2, 0, 10}
	if _, err := testMQTTWrite(c, pubrel[:]); err != nil {
		t.Fatalf("Error writing PUBREL: %v", err)
	}
	// QoS 2 fans out at PUBREL, so the filters subscribed after the PUBREC may receive the
	// release as QoS 0 frames, before or after the PUBCOMP (the PUBREL window
	// docs/CONFORMANCE.md states, witnessed by TestF117PubRelWindowSelfEchoResidual). They are
	// not this test's business: it neither counts nor asserts them.
	if reason, _ := f117ReadPubResponse(t, r, mqttPacketPubComp, 10); reason != mqttReasonSuccess {
		t.Fatalf("Expected PUBCOMP 0x00, got 0x%x", reason)
	}

	// Control: the same publication on a FRESH identifier would write the grown stamp.
	send(12, false)
	if reason, _ := f117ReadPubResponse(t, r, mqttPacketPubRec, 12); reason != mqttReasonQuotaExceeded {
		t.Fatalf("a fresh identifier whose stamp overflows max_payload must be refused 0x%x, got 0x%x", mqttReasonQuotaExceeded, reason)
	}
}

// KNOWN RESIDUAL, characterized rather than endorsed: change this test when the QoS 2 PUBREL
// window is repaired. QoS 2 fans out and stores at PUBREL, so a subscription created between
// PUBREC and PUBREL receives a message that existed before it ([MQTT-4.5.0-1] admits it only to
// subscriptions matching at PUBREC). The No Local stamp is taken at PUBREC, when the publisher's
// own No Local filter created in that interval did not exist, so it carries no tag for it and
// the publisher receives its own message. The live-option check F-117 replaced suppressed this
// one case by accident; docs/CONFORMANCE.md states the window and this consequence.
func TestF117PubRelWindowSelfEchoResidual(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "f117/window"
	c, r := f117Connect(t, o, "f117-window")

	vh := newMQTTWriter(0)
	vh.WriteBytes([]byte(topic))
	vh.WriteUint16(10)
	vh.WriteVarInt(0)
	vh.Write([]byte("window"))
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPub | (2 << 1))
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing PUBLISH: %v", err)
	}
	if reason, _ := testMQTTReadPubRecV5(t, r, 10); reason != mqttReasonSuccess {
		t.Fatalf("Expected PUBREC 0x00, got 0x%x", reason)
	}
	testMQTTSubV5(t, c, r, 11, []mqttV5SubFilter{{topic: topic, opts: f117NoLocal}})
	pubrel := [4]byte{mqttPacketPubRel | 0x2, 0x2, 0, 10}
	if _, err := testMQTTWrite(c, pubrel[:]); err != nil {
		t.Fatalf("Error writing PUBREL: %v", err)
	}

	// The self-echo and the PUBCOMP, in either order.
	var echoed, completed bool
	for !(echoed && completed) {
		b, pl := testMQTTReadPacket(t, r)
		start := r.pos
		switch b & mqttPacketMask {
		case mqttPacketPub:
			got, err := r.readBytes("topic", false)
			if err != nil || string(got) != topic {
				t.Fatalf("Expected the self-echo on %q, got %q (err=%v)", topic, got, err)
			}
			echoed = true
		case mqttPacketPubComp:
			completed = true
		default:
			t.Fatalf("Unexpected packet type 0x%x", b&mqttPacketMask)
		}
		r.pos = start + pl
	}
}

// A drain's subscription is adopted IN PLACE as ordinary membership when its filter is
// subscribed again, so it must already carry the filter's key: an adopted subscription with
// no key never matches a tag, and every later self-publish would bypass No Local. Both the
// main drain and a level-up companion's drain are restored across a reconnect, then adopted
// by a No Local re-SUBSCRIBE.
func TestF117AdoptedRestoredDrainKeepsNoLocal(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const id, filter = "f117-drain", "f117/da/#"
	const mainSid, fwcSid = "f117.da.>", "f117.da" + mqttMultiLevelSidSuffix
	ci := &mqttV5ConnInfo{clientID: id, props: mqttV5ConnPropsSessionExpiry(30)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 1}})

	// One started, unacknowledged delivery on each consumer, so UNSUBSCRIBE drains both.
	p, rp := f117Connect(t, o, id+"-pub")
	testMQTTPubV5Props(t, p, rp, 1, false, 1, "f117/da", []byte("old-parent"), nil)
	testMQTTPubV5Props(t, p, rp, 1, false, 2, "f117/da/x", []byte("old-child"), nil)
	for range 2 {
		testMQTTReadPubV5Packet(t, r) // read, never acknowledged
	}
	testMQTTUnsubV5(t, c, r, 3, []string{filter})
	c.Close()

	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if sp, _, _ := testMQTTReadConnAckV5(t, r2); !sp {
		t.Fatal("fixture: expected Session Present on the resume")
	}
	sess := f117Session(t, s, id)
	drained := func(sid string) bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.drains[sid] != nil
	}
	deadline := time.Now().Add(10 * time.Second)
	for !(drained(mainSid) && drained(fwcSid)) {
		if time.Now().After(deadline) {
			t.Fatal("fixture: both drains must be restored by the resume, or adoption is not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}

	testMQTTSubV5(t, c2, r2, 4, []mqttV5SubFilter{{topic: filter, opts: 1 | f117NoLocal}})
	if drained(mainSid) || drained(fwcSid) {
		t.Fatal("fixture: the No Local re-SUBSCRIBE must ADOPT both drains")
	}
	// Acknowledge whatever the drains redeliver, so only the own publishes remain.
	for {
		b, pl, ok := testMQTTReadPacketReady(r2, 500*time.Millisecond)
		if !ok {
			break
		}
		start := r2.pos
		if b&mqttPacketMask == mqttPacketPub && mqttGetQoS(b&mqttPacketFlagMask) > 0 {
			_, _ = r2.readBytes("topic", false)
			pi, _ := r2.readUint16("pi")
			testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
		}
		r2.pos = start + pl
	}

	w := f117Watch(t, id, false)
	testMQTTPubV5Props(t, c2, r2, 1, false, 5, "f117/da", []byte("own-parent"), nil)
	testMQTTPubV5Props(t, c2, r2, 1, false, 6, "f117/da/x", []byte("own-child"), nil)
	seen := map[*subscription]bool{}
	for len(seen) < 2 {
		e := w.awaitExit(t, "both adopted subscriptions' callbacks returning", func(e f117Exit) bool { return !e.skipped })
		seen[e.sub] = true
	}
	testMQTTExpectNothing(t, r2)
}
