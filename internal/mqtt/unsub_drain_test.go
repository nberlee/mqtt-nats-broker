//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testDrainSetup connects a persistent QoS 1/2 subscriber, publishes one message,
// and returns the subscriber's connection, its reader, its session and the packet
// identifier of the (unacknowledged) delivery.
// burnIn, when given, completes that many exchanges BEFORE the delivery this leaves
// started, so the returned identifier is not 1. A caller that compares identifiers across
// a restart needs that: at 1 the comparison holds even against a broker that restored
// nothing, because a fresh allocator produces 1 as well. Variadic so the existing call
// sites — none of which compares identifiers — are unaffected.
func testDrainSetup(t *testing.T, s *Server, o *Options, clientID, topic string, qos byte, burnIn ...int) (*mqttTestConn, *mqttSession, uint16) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: qos}}, []byte{qos})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID + "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	flags := mqttPubQos1
	if qos == 2 {
		flags = mqttPubQoS2
	}
	if len(burnIn) > 0 {
		for i := 0; i < burnIn[0]; i++ {
			testMQTTPublish(t, pc, pr, qos, false, false, topic, uint16(10+i), []byte("burn"))
			bpi := testMQTTCheckPubMsgNoAck(t, c, r, topic, flags, []byte("burn"))
			if qos == 2 {
				testMQTTSendPIPacket(mqttPacketPubRec, t, c, bpi)
				testMQTTReadPIPacket(mqttPacketPubRel, t, r, bpi)
				testMQTTSendPIPacket(mqttPacketPubComp, t, c, bpi)
			} else {
				testMQTTSendPIPacket(mqttPacketPubAck, t, c, bpi)
			}
		}
		testMQTTFlush(t, c, nil, r)
	}

	testMQTTPublish(t, pc, pr, qos, false, false, topic, 1, []byte("payload"))
	pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, flags, []byte("payload"))
	return &mqttTestConn{c: c, r: r}, testMQTTResidentSession(t, s, clientID), pi
}

type mqttTestConn struct {
	c net.Conn
	r *mqttReader
}

// testConsumerCount reports how many ACTIVE (non-draining) consumers the session
// holds. I7 means a drained consumer must not appear here.
func testConsumerCount(sess *mqttSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.cons)
}

// testDrainClaims reports how many sequences are still claimed across all drains.
func testDrainClaims(sess *mqttSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	n := 0
	for _, d := range sess.drainByDur {
		n += d.claimedCount()
	}
	return n
}

// testDrainCount reports how many consumers are currently draining.
func testDrainCount(sess *mqttSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.drainByDur)
}

// TestMQTTDrainDiscardsFreshMessages is test 13: a drained filter completes what
// it started and delivers NOTHING new. The client unsubscribed, so it is not owed
// anything published afterwards — but the durable has to stay alive to redeliver
// what it already sent, which is exactly why fresh messages need an explicit
// discard rather than simply not arriving.
func TestMQTTDrainDiscardsFreshMessages(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "fresh", "fresh/topic", 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "fresh/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	// Publish AFTER the unsubscribe: it must never reach the client.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "freshpub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "fresh/topic", 2, []byte("after"))

	// The STARTED delivery is still redelivered, and only ever with the original
	// payload.
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "fresh/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)

	// Nothing else arrives: the fresh message was discarded, not queued.
	testMQTTExpectNothing(t, conn.r)

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainSurvivesReconnect is test 15: the drain is durable state, so a
// disconnect and reconnect does not lose the delivery the client is still owed —
// and the filter stays unsubscribed, because the drain is deliberately not a
// membership.
func TestMQTTDrainSurvivesReconnect(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "reconn", "reconn/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "reconn/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	conn.c.Close()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "reconn"}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	sess2 := testMQTTResidentSession(t, s, "reconn")
	sess2.mu.Lock()
	_, stillSubscribed := sess2.subs["reconn/topic"]
	sess2.mu.Unlock()
	if stillSubscribed {
		t.Error("the filter must stay unsubscribed; a drain is not a membership")
	}
	if n := testDrainCount(sess2); n != 1 {
		t.Fatalf("the drain did not survive the reconnect, got %v", n)
	}

	got := testMQTTCheckPubMsgNoAck(t, c2, r2, "reconn/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v (same process)", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, got)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainAdoptedByResubscribe is test 18: re-SUBSCRIBING to a draining
// filter reclaims the drain rather than building a second consumer beside it.
//
// Because a drain is invisible in sess.cons (I7), a re-SUBSCRIBE that did not
// adopt would create a SECOND durable on the same filter and deliver every new
// message twice.
func TestMQTTDrainAdoptedByResubscribe(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "adopt", "adopt/topic", 1)
	defer conn.c.Close()

	sess.mu.Lock()
	durBefore := ""
	for _, cc := range sess.cons {
		durBefore = cc.Durable
	}
	sess.mu.Unlock()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "adopt/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: "adopt/topic", qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	nDrains := len(sess.drainByDur)
	nCons := len(sess.cons)
	durAfter := ""
	for _, cc := range sess.cons {
		durAfter = cc.Durable
	}
	sess.mu.Unlock()
	if nDrains != 0 {
		t.Errorf("the drain should have been adopted, %d still draining", nDrains)
	}
	if nCons != 1 {
		t.Fatalf("expected exactly one consumer for the filter, got %d: a re-SUBSCRIBE built a second one", nCons)
	}
	if durAfter != durBefore {
		t.Errorf("the adopted consumer is %q, want the original %q", durAfter, durBefore)
	}

	// The started delivery is still owed and still completes...
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "adopt/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)

	// ...and new messages flow again, exactly once.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "adoptpub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "adopt/topic", 3, []byte("again"))
	next := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "adopt/topic", mqttPubQos1, []byte("again"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, next)
	testMQTTExpectNothing(t, conn.r)
}

// TestMQTTDrainDiscardsWithheldDeliveries is test 14 (I5): only a delivery that
// was actually TRANSMITTED is drained. A message withheld by the session's
// in-flight cap was never put on the wire, so [MQTT-3.10.4-3] does not cover it
// and it must be DISCARDED rather than enrolled as started — enrolling it would
// keep the durable alive for a message the client has no idea about.
//
// The cap has to be the MQTT-level one (Receive Maximum), not MaxAckPending:
// MaxAckPending makes JetStream withhold the message at the consumer, so it never
// reaches trackPublish and sess.deferred stays empty, and the test would pass
// whatever stageDrain did with it.
func TestMQTTDrainDiscardsWithheldDeliveries(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// The consumer's MaxAckPending is paced to the client's Receive Maximum at
	// CREATE time and then outlives the connection, so subscribe with no client
	// limit first and reconnect with Receive Maximum 1. That leaves the MQTT-level
	// cap BELOW the consumer's, which is the only way a delivery is withheld by
	// trackPublish (and so recorded in sess.deferred) rather than by JetStream.
	// A v5 session ends at connection close unless it asks not to, so both
	// connections carry a Session Expiry Interval — otherwise the reconnect would
	// find nothing to resume.
	c0, r0 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "withheld", props: mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r0)
	testMQTTSubV5(t, c0, r0, 1, []mqttV5SubFilter{{topic: "withheld/topic", opts: 1}})
	testMQTTFlush(t, c0, nil, r0)
	testMQTTDisconnect(t, c0, nil)
	c0.Close()

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID: "withheld",
		props:    append(mqttV5ConnPropsSessionExpiry(300), mqttV5ConnPropsReceiveMax(1)...),
	}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "withheldpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "withheld/topic", 1, []byte("payload"))
	_, pi := testMQTTReadPublishV5(t, r, "withheld/topic", []byte("payload"))

	// From here until the read after UNSUBSCRIBE, AckWait (250 ms) re-offers the
	// transmitted message whenever it lapses, between any two of the reads below.
	redelivered := filterRedeliveries(t, r, "withheld/topic", []byte("payload"), pi)

	// The second cannot be transmitted while the first is unacknowledged.
	testMQTTPublish(t, pc, pr, 1, false, false, "withheld/topic", 2, []byte("withheld"))
	testMQTTExpectNothing(t, r)

	sess := testMQTTResidentSession(t, s, "withheld")
	sess.mu.Lock()
	deferredBefore := len(sess.deferred)
	sess.mu.Unlock()
	if deferredBefore == 0 {
		t.Fatal("expected a withheld delivery in sess.deferred; the test is not exercising I5")
	}

	testMQTTUnsubV5(t, c, r, 2, []string{"withheld/topic"})
	testMQTTFlush(t, c, nil, r)

	sess.mu.Lock()
	var claimed int
	for _, d := range sess.drainByDur {
		claimed = d.claimedCount()
	}
	deferredAfter := len(sess.deferred)
	sess.mu.Unlock()
	if claimed != 1 {
		t.Fatalf("the drain claims %d sequence(s), want exactly the one that was transmitted", claimed)
	}
	if deferredAfter != 0 {
		t.Errorf("%d withheld delivery(ies) still tracked; they were never sent and must be dropped", deferredAfter)
	}

	// Completing the transmitted one retires the drain; the withheld message is
	// never delivered.
	redelivered.remove()
	_, got := testMQTTReadPublishV5(t, r, "withheld/topic", []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, got)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
	testMQTTExpectNothing(t, r)
}

// redeliveryFilter sits under a test's reader while a transmitted QoS 1 message stays
// unacknowledged under a short AckWait. JetStream re-offers it each time AckWait lapses,
// and that retransmission is legitimate: what I5 forbids is transmitting the WITHHELD
// message, not resending the transmitted one. The filter drops exactly that resend, a DUP
// QoS 1 PUBLISH with the message's topic, packet identifier and payload, and passes every
// other frame through, so the assertions above it see any withheld message, any other
// identifier and every control packet as they would with no timer running.
type redeliveryFilter struct {
	r       *mqttReader
	conn    mqttIOReader
	topic   string
	payload []byte
	pi      uint16
	in, out []byte
	dropped int
}

func filterRedeliveries(t *testing.T, r *mqttReader, topic string, payload []byte, pi uint16) *redeliveryFilter {
	t.Helper()
	if r.hasMore() || len(r.pbuf) > 0 {
		t.Fatal("the reader holds unread bytes; the filter would not see them")
	}
	f := &redeliveryFilter{r: r, conn: r.reader, topic: topic, payload: payload, pi: pi}
	r.reader = f
	return f
}

// remove puts the connection back under the reader, handing it the bytes the filter
// had read but not yet passed on.
func (f *redeliveryFilter) remove() {
	leftover := append(f.out, f.in...)
	f.r.reader = f.conn
	if len(leftover) > 0 {
		f.r.reset(leftover)
	}
}

func (f *redeliveryFilter) SetReadDeadline(d time.Time) error { return f.conn.SetReadDeadline(d) }

func (f *redeliveryFilter) Read(p []byte) (int, error) {
	for len(f.out) == 0 {
		if frame, rest, ok := testMQTTNextFrame(f.in); ok {
			f.in = rest
			if f.isRedelivery(frame) {
				f.dropped++
			} else {
				f.out = append(f.out, frame...)
			}
			continue
		}
		var buf [512]byte
		n, err := f.conn.Read(buf[:])
		f.in = append(f.in, buf[:n]...)
		if err != nil {
			if _, _, ok := testMQTTNextFrame(f.in); !ok {
				return 0, err
			}
		}
	}
	n := copy(p, f.out)
	f.out = f.out[n:]
	return n, nil
}

// isRedelivery reports whether frame is a DUP QoS 1 PUBLISH of exactly the filtered
// message in MQTT 5.0 framing: its topic, its identifier, an empty property section
// (the broker sends the resend with none) and its payload. Anything else, a malformed
// frame included, is not dropped and reaches the assertions.
func (f *redeliveryFilter) isRedelivery(frame []byte) bool {
	// The resend is short, so its remaining length is one byte (the minimal encoding of a
	// length below 128); a longer encoding of the same length is malformed and passes.
	if frame[0] != mqttPacketPub|mqttPubFlagDup|mqttPubQos1 || frame[1]&0x80 != 0 || int(frame[1]) != len(frame)-2 {
		return false
	}
	body := frame[2:]
	if len(body) < 2 {
		return false
	}
	tl := int(body[0])<<8 | int(body[1])
	if len(body) < 2+tl+3 || string(body[2:2+tl]) != f.topic {
		return false
	}
	body = body[2+tl:]
	if uint16(body[0])<<8|uint16(body[1]) != f.pi || body[2] != 0 {
		return false
	}
	return string(body[3:]) == string(f.payload)
}

// testMQTTNextFrame splits the first whole MQTT packet off b, if b holds one.
func testMQTTNextFrame(b []byte) (frame, rest []byte, ok bool) {
	if len(b) < 2 {
		return nil, b, false
	}
	length, mult, i := 0, 1, 1
	for ; ; i++ {
		if i >= len(b) || i > 4 {
			return nil, b, false
		}
		length += int(b[i]&0x7f) * mult
		mult *= 128
		if b[i]&0x80 == 0 {
			break
		}
	}
	end := i + 1 + length
	if end > len(b) {
		return nil, b, false
	}
	return b[:end], b[end:], true
}

// TestRedeliveryFilterDropsOnlyTheExactResend pins what the filter may hide: the exact DUP
// QoS 1 resend, and nothing that differs from it in any field.
func TestRedeliveryFilterDropsOnlyTheExactResend(t *testing.T) {
	f := &redeliveryFilter{topic: "withheld/topic", payload: []byte("payload"), pi: 1}
	frame := func(first byte, topic string, pi uint16, props []byte, payload string) []byte {
		body := append([]byte{byte(len(topic) >> 8), byte(len(topic))}, topic...)
		body = append(body, byte(pi>>8), byte(pi))
		body = append(body, props...)
		body = append(body, payload...)
		return append([]byte{first, byte(len(body))}, body...)
	}
	dupQoS1 := mqttPacketPub | mqttPubFlagDup | mqttPubQos1
	exact := []byte{58, 26, 0, 14, 119, 105, 116, 104, 104, 101, 108, 100, 47, 116, 111, 112, 105, 99, 0, 1, 0, 112, 97, 121, 108, 111, 97, 100}
	for _, c := range []struct {
		name  string
		frame []byte
		drop  bool
	}{
		{"the exact resend (as CI received it)", exact, true},
		{"the withheld message", frame(dupQoS1, "withheld/topic", 1, []byte{0}, "withheld"), false},
		{"another identifier", frame(dupQoS1, "withheld/topic", 2, []byte{0}, "payload"), false},
		{"another topic", frame(dupQoS1, "other/topic", 1, []byte{0}, "payload"), false},
		{"not marked DUP", frame(mqttPacketPub|mqttPubQos1, "withheld/topic", 1, []byte{0}, "payload"), false},
		{"QoS 2", frame(mqttPacketPub|mqttPubFlagDup|mqttPubQoS2, "withheld/topic", 1, []byte{0}, "payload"), false},
		{"a property present", frame(dupQoS1, "withheld/topic", 1, []byte{2, 0x01, 0x01}, "payload"), false},
		{"an invalid property", frame(dupQoS1, "withheld/topic", 1, []byte{2, 0x01, 0xff}, "payload"), false},
		{"an overlong property length", frame(dupQoS1, "withheld/topic", 1, []byte{0x80, 0x80, 0x80, 0x80, 0x00}, "payload"), false},
		// Malformed: claims 5 property bytes, then the payload follows where an empty
		// section's would; only the property-length check refuses it.
		{"a property length with no properties", frame(dupQoS1, "withheld/topic", 1, []byte{5}, "payload"), false},
		{"truncated", frame(dupQoS1, "withheld/topic", 1, nil, ""), false},
		// The pinned resend with its remaining length (26) encoded in 2, 3 and 4 bytes.
		{"a two-byte remaining length", append([]byte{58, 0x9a, 0x00}, exact[2:]...), false},
		{"a three-byte remaining length", append([]byte{58, 0x9a, 0x80, 0x00}, exact[2:]...), false},
		{"a four-byte remaining length", append([]byte{58, 0x9a, 0x80, 0x80, 0x00}, exact[2:]...), false},
	} {
		if got := f.isRedelivery(c.frame); got != c.drop {
			t.Errorf("%s: dropped = %v, want %v", c.name, got, c.drop)
		}
	}
}

// TestMQTTDrainRefusedUnderStreamConsumerLimit is test 26: where the messages
// stream imposes a consumer inactivity limit, draining must be REFUSED rather
// than attempted.
//
// A drain has to outlive an offline session, and that limit gives every consumer
// a delete timer it cannot opt out of — so a drained durable would be reaped with
// its messages ($MQTT_msgs is interest-retained). Refusing degrades to the old
// behavior loudly; creating a drain anyway would look compliant and silently lose
// the very messages the drain exists to protect.
func TestMQTTDrainRefusedUnderStreamConsumerLimit(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "limited", "limited/topic", 1)
	defer conn.c.Close()

	// Impose the limit out of band, as a deployment would.
	si, err := sess.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookupStream: %v", err)
	}
	cfg := si.Config
	cfg.ConsumerLimits.InactiveThreshold = time.Hour
	if _, err := sess.jsa.updateStream(&cfg); err != nil {
		// Never skip: a skip here would let this test pass while proving nothing
		// about the guard it exists for.
		t.Fatalf("could not impose a consumer inactivity limit on %q: %v", mqttStreamName, err)
	}

	sess.mu.Lock()
	var subDur string
	for _, cc := range sess.cons {
		subDur = cc.Durable
	}
	sess.mu.Unlock()
	if subDur == "" {
		t.Fatal("expected an active consumer before the unsubscribe")
	}

	l := &captureWarnLogger{warn: make(chan string, 10)}
	s.SetLogger(l, false, false)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "limited/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	if n := testDrainCount(sess); n != 0 {
		t.Fatalf("a drain was created under a consumer inactivity limit (%d); it would be reaped with its messages", n)
	}

	// Refusing to drain is a silent downgrade of a spec guarantee unless it is
	// announced, so the warning is part of the behavior, not decoration.
	var warned string
	for done := time.Now().Add(5 * time.Second); time.Now().Before(done) && warned == _EMPTY_; {
		select {
		case msg := <-l.warn:
			if strings.Contains(msg, "consumer inactivity limit") && strings.Contains(msg, "MQTT-3.10.4-3") {
				warned = msg
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if warned == _EMPTY_ {
		t.Fatal("refusing to drain must warn that started QoS 1/2 deliveries are being dropped and [MQTT-3.10.4-3] is not honored")
	}

	// And the fallback is NO LONGER a destructive teardown (F-105 7c-lc). The started delivery's
	// obligation holds its source consumer, so the UNSUBSCRIBE's disposal decision waits for that
	// obligation's ending: deleting the consumer under it is the defect F-105 removes. The consumer
	// survives while the delivery is outstanding...
	time.Sleep(time.Second)
	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, subDur); err != nil || ci == nil {
		t.Fatalf("the consumer %q was deleted while a started delivery still held it (err=%v)", subDur, err)
	}
	// ...and is retired once the client completes it: the PUBACK ends the obligation, its batch
	// releases the hold, and the retirement owner retires the name and deletes the consumer.
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if ci, err := sess.jsa.lookupConsumer(mqttStreamName, subDur); err == nil && ci != nil {
			return fmt.Errorf("the consumer %q survived its last obligation's ending", subDur)
		}
		return nil
	})
	own, _, oerr := sess.jsa.loadNameOwnershipPhysical(sess.asm.sessStreamEpoch, mqttOwnedStreamMsgs, subDur)
	if oerr != nil || own == nil || own.State != mqttNameOwnRetired {
		t.Fatalf("the retired consumer's name is not RETIRED: %+v (%v)", own, oerr)
	}
	testMQTTExpectNothing(t, conn.r)
}

// TestMQTTDrainNotAdoptedByQoS0Resubscribe pins the other half of adoption: a
// re-SUBSCRIBE at QoS 0 does NOT reclaim the drain.
//
// Adoption exists because a QoS 1/2 re-SUBSCRIBE would otherwise build a second
// durable on the same filter and double-deliver. A QoS 0 subscription creates no
// durable at all, so there is nothing to collide with: the drain keeps completing
// what it started, at its own QoS, while the new subscription delivers fresh
// messages at QoS 0.
func TestMQTTDrainNotAdoptedByQoS0Resubscribe(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "qos0adopt", "qos0adopt/topic", 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "qos0adopt/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: "qos0adopt/topic", qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("a QoS 0 re-SUBSCRIBE must not adopt the drain, %d draining", n)
	}

	// The started QoS 1 delivery still completes at ITS QoS, not the new one.
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "qos0adopt/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})

	// And the QoS 0 subscription is live and delivers at QoS 0.
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "qos0adoptpub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "qos0adopt/topic", 7, []byte("fresh"))
	testMQTTCheckPubMsg(t, conn.c, conn.r, "qos0adopt/topic", 0, []byte("fresh"))
}

// TestMQTTDrainAdoptionPreservesConsumerAccounting pins that adoption moves the
// consumer without touching the books.
//
// Adoption creates no consumer — it hands the existing durable back to cons — so
// it must not charge tmaxack a second time. The mirror-image bug is just as real:
// a drain finishing after an adoption must not refund a charge the adoption still
// owns. Both would silently corrupt the account-wide MaxAckPending budget, which
// is only ever observed as unrelated subscriptions being refused much later.
func TestMQTTDrainAdoptionPreservesConsumerAccounting(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "acct", "acct/topic", 1)
	defer conn.c.Close()

	readAccounting := func() (int, int, string) {
		t.Helper()
		sess.mu.Lock()
		defer sess.mu.Unlock()
		dur := ""
		for _, cc := range sess.cons {
			dur = cc.Durable
		}
		if dur == "" {
			for _, d := range sess.drainByDur {
				dur = d.cc.Durable
			}
		}
		return sess.tmaxack, sess.consCharged[dur], dur
	}

	baseAck, baseCharge, dur := readAccounting()
	if baseCharge == 0 {
		t.Fatal("a consumer created by this process should carry a charge")
	}

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "acct/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	if ack, charge, _ := readAccounting(); ack != baseAck || charge != baseCharge {
		t.Fatalf("draining must not change the books: tmaxack %v->%v, charge %v->%v", baseAck, ack, baseCharge, charge)
	}

	testMQTTSub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: "acct/topic", qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	ack, charge, durAfter := readAccounting()
	if durAfter != dur {
		t.Fatalf("adoption should have reused durable %q, got %q", dur, durAfter)
	}
	if ack != baseAck {
		t.Fatalf("adoption charged tmaxack again: %v -> %v (it creates no consumer)", baseAck, ack)
	}
	if charge != baseCharge {
		t.Fatalf("adoption changed the durable's charge: %v -> %v", baseCharge, charge)
	}

	// Completing the adopted delivery must not refund a charge the adoption owns.
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "acct/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)
	testMQTTFlush(t, conn.c, nil, conn.r)

	if ack, charge, _ := readAccounting(); ack != baseAck || charge != baseCharge {
		t.Fatalf("completing an adopted delivery refunded a live consumer: tmaxack %v->%v, charge %v->%v",
			baseAck, ack, baseCharge, charge)
	}
}

// TestMQTTDrainDurablesDeletedOnCleanStart is test 21: a drain is session state,
// so discarding the session must delete its durables too.
//
// Nothing else would: a drain is deliberately absent from the record's Cons map,
// so a teardown that only walks Cons leaves the durable behind holding interest in
// its filter forever — with the orphan sweep taught to treat drains as referenced,
// nothing would ever reap it either.
func TestMQTTDrainDurablesDeletedOnCleanStart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, _ := testDrainSetup(t, s, o, "cleanstart", "cleanstart/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "cleanstart/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	var drainDur string
	for dur := range sess.drainByDur {
		drainDur = dur
	}
	sess.mu.Unlock()
	if drainDur == "" {
		t.Fatal("expected a draining durable")
	}
	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur); err != nil || ci == nil {
		t.Fatalf("the drained durable should exist before the discard (err=%v)", err)
	}

	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	// A clean-start CONNECT discards the session, which must take the drain with it.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "cleanstart", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur)
		if err == nil && ci != nil {
			return fmt.Errorf("the drained durable %q outlived its session", drainDur)
		}
		return nil
	})
}

// TestMQTTDrainSurvivesOrphanSweep is test 22: the orphan sweep must count a drain
// as a reference.
//
// A drain is deliberately not in the record's Cons map, so to the sweep it looks
// exactly like an abandoned consumer. Reaping it would delete the started
// deliveries with it, because the messages stream is interest-retained — the loss
// the drain exists to prevent, caused by the machinery meant to tidy up after it.
func TestMQTTDrainSurvivesOrphanSweep(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond

	// Every consumer is old enough to be a candidate, so the grace window cannot
	// make this test pass by simply skipping the drain.
	//
	// Overridden BEFORE the server starts and restored by a defer registered
	// before the shutdown one: deferred calls run last-registered-first, so this
	// ordering is what puts the restore AFTER the server (and the periodic sweep
	// goroutine that reads this variable) has stopped.
	origGrace := mqttOrphanSweepGrace.Get()
	mqttOrphanSweepGrace.Set(-time.Second)
	defer func() { mqttOrphanSweepGrace.Set(origGrace) }()

	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "sweep", "sweep/topic", 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "sweep/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	var drainDur string
	for dur := range sess.drainByDur {
		drainDur = dur
	}
	sess.mu.Unlock()
	if drainDur == "" {
		t.Fatal("expected a draining durable")
	}

	asm := testMQTTGetGlobalASM(t, s)
	asm.sweepOrphanedConsumers(s)

	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur); err != nil || ci == nil {
		t.Fatalf("the orphan sweep deleted the drained durable %q (err=%v); its started deliveries went with it", drainDur, err)
	}
	// And the delivery it was holding still completes.
	got := testMQTTCheckPubMsgNoAck(t, conn.c, conn.r, "sweep/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, got)
}

// TestMQTTDrainDiscardsOfflineBacklog is test 24. A drain's durable keeps interest
// in its filter while the session is offline, so messages published meanwhile
// accumulate in the stream. None of them is owed to the client — it unsubscribed —
// so the reconnect must discard the backlog and deliver only what was started.
func TestMQTTDrainDiscardsOfflineBacklog(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "backlog", "backlog/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "backlog/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "backlogpub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	for i := 0; i < 3; i++ {
		testMQTTPublish(t, pc, pr, 1, false, false, "backlog/topic", uint16(10+i), []byte(fmt.Sprintf("offline-%d", i)))
	}

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "backlog", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// Only the started delivery is owed, and only its payload may appear.
	got := testMQTTCheckPubMsgNoAck(t, c2, r2, "backlog/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, got)
	testMQTTExpectNothing(t, r2)
}

// TestMQTTDrainLevelUpCoversBothConsumers is test 27. A "foo/#" subscription is
// two JetStream consumers, because "#" also matches the parent level "foo": the
// main durable filters foo.> and a companion filters foo. UNSUBSCRIBE removes one
// MQTT filter but must drain BOTH, or the started delivery on whichever consumer
// was overlooked is deleted with its durable.
func TestMQTTDrainLevelUpCoversBothConsumers(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "levelup"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "levelup/#", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "levelupub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	// One message per consumer: "levelup/bar" matches the main filter, bare
	// "levelup" matches the level-up companion.
	testMQTTPublish(t, pc, pr, 1, false, false, "levelup/bar", 1, []byte("child"))
	testMQTTPublish(t, pc, pr, 1, false, false, "levelup", 2, []byte("parent"))

	// Both arrive unacknowledged; the order between two consumers is not fixed. The 250 ms AckWait
	// can bring one consumer's retransmission before the other's first delivery under load; a
	// retransmission is the same delivery (DUP, its own identifier), so reading continues until each
	// consumer's first delivery has been seen.
	seen := map[string]uint16{}
	for reads := 0; len(seen) < 2; reads++ {
		if reads >= 20 {
			t.Fatalf("expected one started delivery per consumer, got %v", seen)
		}
		flags, pi, topic, payload := testMQTTReadPubPacket(t, r)
		if topic != "levelup/bar" && topic != "levelup" {
			t.Fatalf("unexpected topic %q", topic)
		}
		if prev, ok := seen[string(payload)]; ok {
			if flags&mqttPubFlagDup == 0 || pi != prev {
				t.Fatalf("%q arrived again as flags %x identifier %d, want a retransmission under %d", payload, flags, pi, prev)
			}
			continue
		}
		seen[string(payload)] = pi
	}
	if len(seen) != 2 {
		t.Fatalf("expected one started delivery per consumer, got %v", seen)
	}

	sess := testMQTTResidentSession(t, s, "levelup")
	if n := testConsumerCount(sess); n != 2 {
		t.Fatalf("a \"#\" subscription should hold two consumers, got %d", n)
	}

	testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: "levelup/#"}})
	testMQTTFlush(t, c, nil, r)

	if n := testDrainCount(sess); n != 2 {
		t.Fatalf("both consumers of a level-up filter must drain, got %d", n)
	}
	if n := testConsumerCount(sess); n != 0 {
		t.Fatalf("a drained consumer must leave cons (I7), got %d", n)
	}

	// Both started deliveries still complete.
	for i := 0; i < 2; i++ {
		flags, pi, _, payload := testMQTTReadPubPacket(t, r)
		if flags&mqttPubFlagDup == 0 {
			t.Errorf("a redelivered started message must carry DUP, flags=%x", flags)
		}
		want, ok := seen[string(payload)]
		if !ok {
			t.Fatalf("redelivered an unexpected payload %q", payload)
		}
		if pi != want {
			t.Errorf("redelivered %q with identifier %v, want the original %v", payload, pi, want)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	}
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drains still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainSurvivesConsumerInactivityThreshold is test 25: a drain must outlive an
// offline period.
//
// Under the F-105 cutover the enrolled source consumer is created with NO inactivity
// threshold (mqttClearConsumerInactivity), because its lifetime is its resource's, not a
// timer's; the broker has no per-consumer inactivity option at all. So there is no
// per-consumer timer for the drain to shed: it is SAFE FROM BIRTH, and it survives the
// offline period because it never carried a timer that would reap it (the messages stream
// being interest-retained, a reap would take its started deliveries with it). A
// stream-forced ConsumerLimits.InactiveThreshold is the one source no consumer config can
// escape; admission refuses it, which is not this test's subject.
func TestMQTTDrainSurvivesConsumerInactivityThreshold(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "inact", "inact/topic", 1)

	sess.mu.Lock()
	var subDur string
	for _, cc := range sess.cons {
		subDur = cc.Durable
	}
	sess.mu.Unlock()
	// Born safe: the enrolled durable starts with no inactivity threshold, so there is no
	// shed step and never a window in which it is reapable.
	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, subDur); err != nil || ci == nil {
		t.Fatalf("lookupConsumer: %v", err)
	} else if ci.Config.InactiveThreshold != 0 {
		t.Fatalf("an enrolled subscription durable must be born with NO inactivity timer, got %v",
			ci.Config.InactiveThreshold)
	}

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "inact/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	// Offline for a while. The durable was never armed with a timer, so nothing reaps it.
	time.Sleep(2 * time.Second)

	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, subDur); err != nil || ci == nil {
		t.Fatalf("the drained durable was reaped while offline (err=%v); its started delivery went with it", err)
	}

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "inact", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	got := testMQTTCheckPubMsgNoAck(t, c2, r2, "inact/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, got)
}

// TestMQTTDrainInactivityClearRetriedOnResume was RETIRED with the mechanism it tested.
//
// It pinned the best-effort per-consumer inactivity-timer clear and its retry on resume
// (clearDrainInactivityTimers). The F-105 cutover deleted that mechanism: an enrolled
// source consumer is created with NO inactivity threshold from the outset, so there is
// no timer to clear at commit and no un-cleared-drain state to retry — the scenario this
// test simulated by re-imposing the timer out of band is unreachable through the
// production path. The surviving offline-survival property is asserted by
// TestMQTTDrainSurvivesConsumerInactivityThreshold (safe-from-birth); the stream-forced
// limit, which no consumer config can escape, is still refused at runtime
// (drainingAllowed).

// TestMQTTDrainStageRollbackKeepsTheSubscription is test 19 (I4): staging a drain
// and removing the membership are one transaction. If the packet's save fails,
// NEITHER is persisted — the subscription is intact, its consumer is intact, and
// the started delivery still completes on it.
//
// The dangerous shape is the opposite: a half-applied UNSUBSCRIBE that has torn
// the membership down in memory but persisted nothing, leaving a filter that is
// neither subscribed nor draining.
func TestMQTTDrainStageRollbackKeepsTheSubscription(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "stagerb", props: mqttV5ConnPropsSessionExpiry(30)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "stagerb/topic", opts: 1}})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "stagerbpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "stagerb/topic", 1, []byte("payload"))

	_, pi := testMQTTReadPublishV5(t, r, "stagerb/topic", []byte("payload"))
	sess := testMQTTResidentSession(t, s, "stagerb")
	consBefore := testConsumerCount(sess)
	if consBefore != 1 {
		t.Fatalf("expected one consumer before the unsubscribe, got %d", consBefore)
	}

	restore := setMQTTTestSessSaveHook(func(*mqttSession) error { return errDrainTestSaveFailed })
	codes := testMQTTUnsubV5(t, c, r, 2, []string{"stagerb/topic"})
	restore()

	if len(codes) != 1 || codes[0] != mqttReasonImplementationSpecificError {
		t.Fatalf("expected UNSUBACK 0x83 when the packet cannot be persisted, got %v", codes)
	}
	if n := testDrainCount(sess); n != 0 {
		t.Fatalf("a rolled-back UNSUBSCRIBE must leave no drain, got %d", n)
	}
	if n := testConsumerCount(sess); n != consBefore {
		t.Fatalf("the subscription's consumer must survive a rolled-back UNSUBSCRIBE: %d -> %d", consBefore, n)
	}

	// The started delivery still completes on the intact subscription...
	qos, got := testMQTTReadPublishV5(t, r, "stagerb/topic", []byte("payload"))
	if qos != 1 {
		t.Errorf("the redelivery must keep its QoS, got %v", qos)
	}
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, got)

	// ...and the filter is still subscribed, so new messages arrive.
	testMQTTPublish(t, pc, pr, 1, false, false, "stagerb/topic", 2, []byte("still-subscribed"))
	_, next := testMQTTReadPublishV5(t, r, "stagerb/topic", []byte("still-subscribed"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, next)
}

// TestMQTTDrainCompletionSaveFailureLosesNothing is test 20 (I1): the ack to
// JetStream must never outrun the record.
//
// When the save that removes a claim fails, the JetStream source stays pending.
// Unit C rev3 section 6 requires the already acknowledged MQTT exchange to retain
// its terminal source carrier and converge without another PUBLISH or client ACK.
// This replaces the former fresh-identifier retransmission contract (holistic review).
func TestMQTTDrainCompletionSaveFailureLosesNothing(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "compfail", "compfail/topic", 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "compfail/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	// PUBACK while the record cannot be written: the completion must not stick.
	//
	// THE FAULT IS AIMED AT THE SAVE THIS TEST NAMES — the one that removes the claim — and
	// not at whatever save runs next. The PUBACK's finalize can be PARKED on the section that
	// transmitted the PUBLISH, so the claim-removal save runs on the settlement's goroutine;
	// a fixture that failed every save and restored on its own schedule could restore BEFORE
	// that save, which then succeeded, acked the source and retired the drain — leaving
	// nothing to redeliver and no failure to recover from. Prerequisite saves (the release
	// request) are deliberately allowed to land.
	var refused atomic.Int64
	restore := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != sess {
			return nil
		}
		hs.mu.Lock()
		removing := false
		for _, d := range hs.drainByDur {
			for _, st := range d.seqs {
				if st != nil && !st.claimed && st.unclaimUnconfirmed {
					removing = true
				}
			}
		}
		hs.mu.Unlock()
		if !removing {
			return nil
		}
		refused.Add(1)
		return errDrainTestSaveFailed
	})
	t.Cleanup(restore)
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)

	// BOTH HALVES OBSERVED before the fault is lifted: the claim-removal save was refused,
	// and the rollback that failure owes has completed. Entering the hook proves neither.
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if refused.Load() == 0 {
			return fmt.Errorf("the claim-removal save has not been refused yet")
		}
		sess.mu.Lock()
		defer sess.mu.Unlock()
		for _, d := range sess.drainByDur {
			for _, st := range d.seqs {
				if st == nil {
					continue
				}
				if !st.claimed || st.unclaimUnconfirmed {
					return fmt.Errorf("the refused save has not rolled back yet: claimed=%v unconfirmed=%v",
						st.claimed, st.unclaimUnconfirmed)
				}
			}
		}
		return nil
	})

	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("a completion that could not be persisted must leave the drain claiming the message, got %d drains", n)
	}
	if claimed := testDrainClaims(sess); claimed != 1 {
		t.Fatalf("expected the sequence to stay claimed, got %d claims", claimed)
	}

	// The MQTT ACK is durable even though source cleanup is not. Retain the
	// exact carrier, source and identifier while the claim-removal save fails.
	sess.mu.Lock()
	subject := mqttInflightSubject(sess.idHash, sess.gen, pi)
	sess.mu.Unlock()
	terminal, observed, err := sess.asm.loadInflightRecord(subject)
	if err != nil || terminal == nil || observed == nil || terminal.State != mqttInflightPublishTerminal {
		t.Fatalf("the terminal source carrier must survive: rec=%+v err=%v", terminal, err)
	}
	ref := mqttSlotRefFromRecord(subject, observed.Sequence, terminal)
	if ref == nil || !sess.terminalSourcePair(*ref, terminal) {
		t.Fatal("the MQTT ACK must leave an exact terminal exchange/register pair")
	}
	if state := sess.asm.inflightSourceState(terminal); state != mqttInflightSourceLive {
		t.Fatalf("the failed save must leave the source live, got %v", state)
	}
	sess.mu.Lock()
	held := sess.sendReservedLocked(pi)
	suppressed := sess.sourceRetiredLocked(terminal.Dur, terminal.Seq)
	sess.mu.Unlock()
	if !held || !suppressed {
		t.Fatalf("failed save lost identifier/source retention: held=%v suppressed=%v", held, suppressed)
	}
	if packet, _, ok := testMQTTReadPacketReady(conn.r, 2*o.MQTT.AckWait); ok {
		t.Fatalf("terminal source was retransmitted while its save failed: packet=%x", packet)
	}
	testMQTTFlush(t, conn.c, nil, conn.r)

	// No second client ACK: the installed owner retries persistence, then settles
	// the source and releases the carrier and its identifier.
	restore()
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		cur, msg, err := sess.asm.loadInflightRecord(subject)
		if err != nil || cur != nil || msg != nil {
			return fmt.Errorf("terminal carrier not released: rec=%+v err=%v", cur, err)
		}
		state := sess.asm.inflightSourceState(terminal)
		if state != mqttInflightSourceComplete && state != mqttInflightSourceLost {
			return fmt.Errorf("source can still be reoffered: %v", state)
		}
		sess.mu.Lock()
		held := sess.sendReservedLocked(pi)
		suppressed := sess.sourceRetiredLocked(terminal.Dur, terminal.Seq)
		sess.mu.Unlock()
		if held || suppressed {
			return fmt.Errorf("terminal cleanup remains: held=%v suppressed=%v", held, suppressed)
		}
		return nil
	})
	if packet, _, ok := testMQTTReadPacketReady(conn.r, 2*o.MQTT.AckWait); ok {
		t.Fatalf("terminal cleanup retransmitted an acknowledged PUBLISH: packet=%x", packet)
	}
	testMQTTFlush(t, conn.c, nil, conn.r)
}

// TestMQTTDrainRestoredClaimRedeliveredAfterRestart is the QoS 1 half of test 16:
// across a broker restart the drain is rebuilt from the record alone, and the
// claim it carries is redelivered with DUP rather than treated as a fresh message
// (which the committed drain would discard).
//
// After a restart the in-memory tracking maps are empty, so the delivery path
// cannot tell a started delivery from a new one — only the record can, which is
// why the claim, not the memory, is the authority.
func TestMQTTDrainRestoredClaimRedeliveredAfterRestart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, piOrig := testDrainSetup(t, s, o, "restart1", "restart1/topic", 1, 2)
	if piOrig <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce; the continuity comparison after the restart would prove nothing", piOrig)
	}

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "restart1/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	o.MQTT.Port = -1
	testSetStoreDir(o, dir)
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "restart1", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The ORIGINAL identifier, with DUP. Continuity is no longer a deviation on any
	// outbound path: the in-flight register is restored before source interest is, and
	// drain recovery reserves the identifier the drain recorded. This previously discarded
	// the original and accepted whatever arrived, so a regression here would have passed.
	flags, pi2, topic, payload := testMQTTReadPubPacket(t, r2)
	if topic != "restart1/topic" || string(payload) != "payload" {
		t.Fatalf("expected the started delivery to be redelivered, got %q %q", topic, payload)
	}
	if flags&mqttPubFlagDup == 0 {
		t.Errorf("a delivery that was transmitted before the restart must carry DUP, flags=%x", flags)
	}
	if pi2 != piOrig {
		t.Fatalf("the redelivery after the restart used packet identifier %d, want the original %d: the drain's in-flight register did not restore it",
			pi2, piOrig)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi2)

	sess2 := testMQTTResidentSession(t, s, "restart1")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
	testMQTTExpectNothing(t, r2)
}

// TestMQTTDrainDurablesDeletedOnSessionExpiry is the other half of test 21:
// expiry deletes drain durables too.
//
// Expiry does not run through the connection's teardown — it rebuilds a transient
// session from the persisted record and clears that — so a drain is only deleted
// if the record's Drains are restored onto that transient session. Miss it and the
// durable outlives the session it belonged to, holding interest in its filter
// forever, with the orphan sweep taught to treat drains as referenced and the
// record gone.
func TestMQTTDrainDurablesDeletedOnSessionExpiry(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, _ := testDrainSetup(t, s, o, "expiredrain", "expiredrain/topic", 1)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "expiredrain/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	var drainDur string
	for dur := range sess.drainByDur {
		drainDur = dur
	}
	idHash := sess.idHash
	sess.mu.Unlock()
	if drainDur == "" {
		t.Fatal("expected a draining durable")
	}

	// Disconnect with an already-elapsed Session Expiry Interval, then drive the
	// expiry path directly rather than waiting out the sweep interval.
	sess.mu.Lock()
	sess.expiryInterval = 1
	sess.mu.Unlock()
	testMQTTDisconnect(t, conn.c, nil)
	conn.c.Close()

	asm := testMQTTGetGlobalASM(t, s)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		asm.expireSessionFromRecord(s, idHash)
		if ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur); err == nil && ci != nil {
			return fmt.Errorf("the drained durable %q outlived the expired session", drainDur)
		}
		return nil
	})
}

// TestMQTTDrainAdoptionRolledBackOnFailedSave pins that a re-SUBSCRIBE which
// adopts a drain and then cannot persist the packet puts the drain BACK.
//
// Adoption is not an ordinary membership change: it deletes the drain and moves
// its durable into cons. The consumer snapshot alone cannot undo that — restoring
// cons without the drain leaves a session holding NEITHER, while the record still
// carries the drain. The next save then erases a durable that is still owed
// started deliveries, and with the orphan sweep treating drains as referenced,
// nothing reaps it either.
func TestMQTTDrainAdoptionRolledBackOnFailedSave(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// v5, so a failed SUBSCRIBE reports 0x83 per filter instead of closing the
	// connection — which is what makes the rollback observable from the client.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "adoptrb", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "adoptrb/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "adoptrbpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "adoptrb/topic", 1, []byte("payload"))
	testMQTTReadPublishV5(t, r, "adoptrb/topic", []byte("payload"))

	sess := testMQTTResidentSession(t, s, "adoptrb")
	if codes := testMQTTUnsubV5(t, c, r, 2, []string{"adoptrb/topic"}); len(codes) != 1 || codes[0] != mqttReasonSuccess {
		t.Fatalf("UNSUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	sess.mu.Lock()
	var drainDur string
	for dur := range sess.drainByDur {
		drainDur = dur
	}
	sess.mu.Unlock()

	// The save must fail AFTER the adoption, or the packet never reaches the
	// rollback and this test proves nothing. Adoption is observable: it is the
	// moment the drain leaves drainByDur.
	var adopted atomic.Bool
	restore := setMQTTTestSessSaveHook(func(sp *mqttSession) error {
		sp.mu.Lock()
		_, stillDraining := sp.drainByDur[drainDur]
		sp.mu.Unlock()
		if stillDraining {
			return nil // pre-adoption saves proceed
		}
		adopted.Store(true)
		return errDrainTestSaveFailed
	})
	// The SUBACK is deliberately not read: a SUBSCRIBE whose rolled-back record
	// cannot be confirmed closes the connection (the rollback path says so), so the
	// observable outcome is the session state, not a reason code.
	testDrainSendSubscribeV5(t, c, 3, "adoptrb/topic", 1)
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !adopted.Load() {
			return errors.New("the packet has not reached a post-adoption save yet")
		}
		return nil
	})
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if testDrainCount(sess) != 1 {
			return fmt.Errorf("the drain has not been restored yet (drains=%d cons=%d)",
				testDrainCount(sess), testConsumerCount(sess))
		}
		return nil
	})
	restore()

	sess.mu.Lock()
	_, backAsDrain := sess.drainByDur[drainDur]
	inCons := false
	for _, cc := range sess.cons {
		if cc.Durable == drainDur {
			inCons = true
		}
	}
	sess.mu.Unlock()
	if !backAsDrain {
		t.Fatal("a rolled-back adoption must put the drain back: the record still carries it, and the next save would erase its durable")
	}
	if inCons {
		t.Fatal("a rolled-back adoption must not leave the durable in cons as well (I7)")
	}
	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, drainDur); err != nil || ci == nil {
		t.Fatalf("the drained durable must survive a rolled-back adoption (err=%v)", err)
	}
}

// testDrainSendSubscribeV5 writes a v5 SUBSCRIBE without reading its SUBACK, for
// the cases where the broker answers by closing the connection.
func testDrainSendSubscribeV5(t *testing.T, c net.Conn, pi uint16, filter string, opts byte) {
	t.Helper()
	vh := newMQTTWriter(0)
	vh.WriteUint16(pi)
	vh.WriteVarInt(0) // no properties
	vh.WriteBytes([]byte(filter))
	vh.WriteByte(opts)
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("sending SUBSCRIBE: %v", err)
	}
}

// TestMQTTSubscribeAmbiguousSaveIsNotRolledBack drives a real SUBSCRIBE whose
// final record write MAY have landed, and pins that the packet is NOT undone.
//
// rollbackSubscribePacket restores the pre-packet membership and consumer maps and
// re-persists them, then tears down what the packet created. If the failed write
// actually landed, that discards state the record now holds, and the consumers the
// stored record still names have already been deleted. Which way memory reconciles is
// the readback's to decide, so rolling back at failure time commits to "it did not
// land" at the one moment nobody can know that.
func TestMQTTSubscribeAmbiguousSaveIsNotRolledBack(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambsub", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	// One confirmed subscription first, so the session exists and is resident.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ambsub/first", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTResidentSession(t, s, "ambsub")

	// The next packet's record write lands but its response is lost. The readback is
	// held off as well: every ambiguous path now resolves immediately (see
	// resolveUncertainNow), so without this the session would settle before the
	// retained state could be observed.
	var suppressRead atomicOnceBool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressRead.get() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	defer restoreRead()
	suppressRead.set(true)
	var once atomicOnce
	restore := setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err == nil && once.fire() {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	testDrainSendSubscribeV5(t, c, 2, "ambsub/second", 1)

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return errors.New("the ambiguous write has not been recorded yet")
		}
		return nil
	})
	restore()

	// sess.subs is keyed by the NATS subject form of the filter, not the MQTT one.
	sess.mu.Lock()
	_, kept := sess.subs["ambsub.second"]
	var dur string
	for sid, cc := range sess.cons {
		if strings.Contains(sid, "ambsub.second") {
			dur = cc.Durable
		}
	}
	nsubs := len(sess.subs)
	sess.mu.Unlock()

	if !kept {
		t.Fatalf("the attempted membership was rolled back after an ambiguous write (subs=%d); "+
			"the record may already carry it, and only the readback can decide how memory reconciles", nsubs)
	}
	if dur == "" {
		t.Fatal("the packet's consumer entry was removed after an ambiguous write")
	}
	if ci, err := sess.jsa.lookupConsumer(mqttStreamName, dur); err != nil || ci == nil {
		t.Fatalf("the packet's consumer was torn down after an ambiguous write (err=%v); "+
			"the stored record may still name it", err)
	}
}

// atomicOnce fires exactly once.
type atomicOnce struct{ done atomic.Bool }

func (o *atomicOnce) fire() bool { return o.done.CompareAndSwap(false, true) }

// fired reports whether the one-shot has already been consumed, for a test that must
// prove its injection actually ran rather than assume it.
func (o *atomicOnce) fired() bool { return o.done.Load() }

// TestMQTTSharedSubscribeAmbiguousSaveAbandonsThePacket pins that an ambiguous
// EARLY shared-membership write stops the packet instead of marking the filter
// failed and carrying on.
//
// Carrying on is doubly wrong. The final update() skips filters marked failed, so
// the client would be told the subscription failed while the membership is durably
// stored — and the rest of the packet would run against a session whose record
// state is unknown, where every later save is refused anyway. The attempted state
// must survive for the readback to recognise.
func TestMQTTSharedSubscribeAmbiguousSaveAbandonsThePacket(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambshared", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ambshared/first", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTResidentSession(t, s, "ambshared")

	// The shared filter's EARLY membership write lands but its response is lost.
	var suppressRead atomicOnceBool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressRead.get() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	defer restoreRead()
	suppressRead.set(true)
	var once atomicOnce
	restore := setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err == nil && once.fire() {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	testDrainSendSubscribeV5(t, c, 2, "$share/g1/ambshared/shared", 1)

	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return errors.New("the ambiguous write has not been recorded yet")
		}
		return nil
	})
	restore()

	sess.mu.Lock()
	shared := len(sess.sharedSubs)
	sess.mu.Unlock()
	if shared != 1 {
		t.Fatalf("the attempted shared membership must survive an ambiguous write, got %d: "+
			"the record may already carry it, and only the readback can decide how memory reconciles", shared)
	}

	// The packet was abandoned, so no SUBACK claiming failure is sent: the
	// connection closes instead.
	testDrainWaitConnClosedT(t, r)
}

// TestMQTTForcedSharedLeaveAmbiguousSaveKeepsTheRemoval pins the same rule on the
// 3.1.1 resume path, which force-leaves shared subscriptions a 3.1.1 client can
// neither observe nor UNSUBSCRIBE.
//
// If the removal landed and its response was lost, restoring the memberships in
// memory means the readback adopts the stored (empty) record while the session
// still holds them — and the next save resurrects a phantom holder that no
// UNSUBSCRIBE can ever clear.
func TestMQTTForcedSharedLeaveAmbiguousSaveKeepsTheRemoval(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// A v5 client establishes the shared membership...
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "forcedleave", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/g1/forcedleave/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTResidentSession(t, s, "forcedleave")
	sess.mu.Lock()
	sharedBefore := len(sess.sharedSubs)
	sess.mu.Unlock()
	if sharedBefore != 1 {
		t.Fatalf("expected one shared membership, got %d", sharedBefore)
	}
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// ...and a 3.1.1 resume force-leaves it, with the write's response lost.
	// Target the FORCED-LEAVE write specifically: it is the one whose record no
	// longer carries any shared membership. Firing on the first write of the
	// CONNECT would instead make an earlier save ambiguous, and this branch would
	// never be exercised.
	var suppressRead atomicOnceBool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressRead.get() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	defer restoreRead()
	// The readback suppression is armed ONLY once the forced-leave write is intercepted, not
	// before the CONNECT. The accepting CONNECT's own save is now the outbound-ownership metadata
	// commit (F-105 cutover), and its handoff predecessor check READS $MQTT_sess; arming the
	// suppression up front would fail that read and refuse the accept before the forced-leave
	// write ever runs. Suppressing it at the moment the forced-leave response is lost keeps the
	// resolver from settling that specific ambiguous write, which is the whole point.
	var once atomicOnce
	restore := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err != nil {
			return resp, err
		}
		sp.mu.Lock()
		leaving := len(sp.sharedSubs) == 0
		sp.mu.Unlock()
		if leaving && once.fire() {
			suppressRead.set(true)
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	// The CONNECT is expected to be refused (the record write failed), so the
	// helper's CONNACK read would just report EOF; what matters is the session
	// state the attempt leaves behind.
	c2, _, cerr := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "forcedleave", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 0)
	if c2 != nil {
		defer c2.Close()
	}
	_ = cerr
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return errors.New("the ambiguous write has not been recorded yet")
		}
		return nil
	})
	restore()

	sess.mu.Lock()
	sharedAfter := len(sess.sharedSubs)
	sess.mu.Unlock()
	if sharedAfter != 0 {
		t.Fatalf("the forced leave was reverted after an ambiguous write (%d membership(s) restored): "+
			"the record may already omit them, and a later save would resurrect a holder no UNSUBSCRIBE can clear", sharedAfter)
	}
}

// testDrainWaitConnClosedT waits for the broker to close the connection.
func testDrainWaitConnClosedT(t *testing.T, r *mqttReader) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var buf [256]byte
	for time.Now().Before(deadline) {
		if err := r.reader.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		if _, err := r.reader.Read(buf[:]); err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			r.reader.SetReadDeadline(time.Time{}) //nolint:errcheck
			return
		}
	}
	t.Fatal("the broker never closed the connection: the packet was not abandoned")
}

// TestMQTTUnsubAmbiguousLandedSaveStillCommits is finding 2's clearest case: the
// UNSUBSCRIBE's record write LANDS but its response is lost.
//
// Everything the packet does after that save — committing the staged drains,
// clearing their inactivity timers, releasing shared durables, dropping the
// client-facing subscriptions — is skipped when the save reports an error. Without a
// landed continuation the record says "unsubscribed" while the runtime still holds
// the subscription and the drain is still STAGED, so a fresh message on the drained
// filter is left to AckWait instead of being discarded, and nothing will ever finish
// the packet.
func TestMQTTUnsubAmbiguousLandedSaveStillCommits(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// v5, so a failed UNSUBSCRIBE reports a reason code instead of closing the
	// connection — the state has to be inspectable afterwards.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unsublanded", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "unsublanded/topic", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "unsublandedpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "unsublanded/topic", 1, []byte("payload"))
	_, pi := testMQTTReadPublishV5(t, r, "unsublanded/topic", []byte("payload"))

	sess := testMQTTResidentSession(t, s, "unsublanded")

	// The UNSUBSCRIBE's write lands; only its response is replaced by an error.
	// Identified by what it is about to store: a STAGED drain exists only between
	// stageDrain and this packet's confirming save, so no earlier write can match.
	var once atomicOnce
	restore := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err != nil {
			return resp, err
		}
		sp.mu.Lock()
		var staged bool
		for _, d := range sp.drainByDur {
			staged = staged || d.staged
		}
		sp.mu.Unlock()
		if staged && once.fire() {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	codes := testMQTTUnsubV5(t, c, r, 2, []string{"unsublanded/topic"})
	if len(codes) != 1 {
		t.Fatalf("UNSUBACK = %v", codes)
	}

	// Production must resolve this itself: the client is still connected, so the
	// periodic pass would skip the session.
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		if sess.isSeqUncertain() {
			return errors.New("still unresolved: nothing in production is retrying the readback")
		}
		return nil
	})
	restore()

	// The landed continuation ran: the drain is COMMITTED, not staged.
	sess.mu.Lock()
	var staged bool
	nd := len(sess.drainByDur)
	for _, d := range sess.drainByDur {
		staged = staged || d.staged
	}
	nsubs := len(sess.subs)
	sess.mu.Unlock()
	if nd != 1 {
		t.Fatalf("expected the drain to survive, got %d (subs=%d)", nd, nsubs)
	}
	if staged {
		t.Fatal("the record says unsubscribed but the drain is still STAGED: the landed continuation never ran, " +
			"so fresh messages on the drained filter are left to AckWait instead of being discarded")
	}
	if nsubs != 0 {
		t.Fatalf("the record says unsubscribed but %d membership(s) remain in memory", nsubs)
	}

	// And the started delivery still completes, which is the drain's whole purpose.
	_, got := testMQTTReadPublishV5(t, r, "unsublanded/topic", []byte("payload"))
	if got != pi {
		t.Errorf("redelivered identifier = %v, want the original %v", got, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, got)
}

// TestMQTTSubscribeAmbiguousIsSettledForwardE2E drives the REAL SUBSCRIBE path for a
// write that never reaches the stream.
//
// The packet's state is KEPT — the record may already hold it — and what settles the
// question is the resolver re-attempting the attempt's exact bytes, not a readback
// concluding from the base sequence that the write died. So the membership and the
// consumer this packet created both stand, and the record ends up naming them: no
// SUBACK was sent, but a durable subscription the client was never acknowledged for
// is exactly what a landed original with a lost response leaves behind.
//
// The fixture waits for the ambiguity BEFORE lifting suppression. An earlier version
// polled for the absence of the packet's state and was satisfied by the first sample,
// which the broker had not processed yet — it passed without the packet ever failing.
//
// Mutation M1: revert the at-base arm to clearing the uncertainty and the packet is
// undone instead: the membership disappears and its durable is deleted.
func TestMQTTSubscribeAmbiguousIsSettledForwardE2E(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "subunwind", props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	// One confirmed subscription, so there is a pre-packet state to compare against.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "subunwind/first", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTResidentSession(t, s, "subunwind")

	// The DURABLES on the stream, not the in-memory map: only an actual deletion
	// distinguishes a settled packet from an undone one.
	dursBefore := testDrainStreamDurables(t, sess)

	// The next packet's record write never reaches the stream. Suppression stays on
	// until the ambiguity is REAL, or the packet under test never fails at all.
	mqttTestSessStoreSuppressV.Store(true)
	testDrainSendSubscribeV5(t, c, 2, "subunwind/second", 1)
	checkFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return errors.New("the packet's write has not gone ambiguous yet")
		}
		return nil
	})
	mqttTestSessStoreSuppressV.Store(false)

	// The connection is closed by the failed packet, so the resolution comes from the
	// pass over resident sessions — driven here rather than waited on.
	asm := testMQTTGetGlobalASM(t, s)
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		asm.reconcileResidentRecordState(s)
		if sess.isSeqUncertain() {
			return errors.New("still unresolved")
		}
		return nil
	})

	// Settled forward: the packet's membership stands, and the consumer it created is
	// still there — referenced by the record the re-attempt made durable.
	sess.mu.Lock()
	_, second := sess.subs["subunwind.second"]
	_, first := sess.subs["subunwind.first"]
	sess.mu.Unlock()
	if !second {
		t.Fatal("the packet's membership was undone: an ambiguous write is settled by making its bytes durable, not by rolling them back")
	}
	if !first {
		t.Fatal("the pre-packet subscription must survive")
	}
	if extra := testDrainNewDurables(t, sess, dursBefore); len(extra) == 0 {
		t.Fatal("the packet's durable was deleted although the record names its subscription")
	}
	// The record itself, which is what a restart would read.
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	smsg, err := sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil {
		t.Fatalf("loading the settled record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
		t.Fatalf("unmarshal the settled record: %v", uerr)
	}
	if _, durable := ps.Subs["subunwind.second"]; !durable {
		t.Fatalf("the settled record must carry the packet's subscription, got %v", ps.Subs)
	}
}

// testDrainStreamDurables snapshots the consumer names on the messages stream.
func testDrainStreamDurables(t *testing.T, sess *mqttSession) map[string]bool {
	t.Helper()
	cis, err := sess.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	out := make(map[string]bool, len(cis))
	for _, ci := range cis {
		if ci != nil {
			out[ci.Name] = true
		}
	}
	return out
}

// testDrainNewDurables lists consumers that appeared since the snapshot.
func testDrainNewDurables(t *testing.T, sess *mqttSession, before map[string]bool) []string {
	t.Helper()
	var extra []string
	for name := range testDrainStreamDurables(t, sess) {
		if !before[name] {
			extra = append(extra, name)
		}
	}
	return extra
}

// TestQuarantinedCompanionDrainRefusesTheWholeFilterOverSubscribe is the same property as
// TestQuarantineRefusesEverySidOfAWildcardFilter, driven through a real SUBSCRIBE so it
// reaches processSubs, the adoption site and the session save.
//
// A wildcard filter owns a main sid AND a level-up companion, each with its own consumer
// and, after an UNSUBSCRIBE, its own drain. A per-sid refusal ADOPTED the usable main drain
// into sess.cons and only then failed on the companion. Per-filter failure cleanup removes
// subscriptions and returned consumers but does not put an adopted drain back, and the
// persisted record omits a consumer with no sess.subs membership — so the next SUCCESSFUL
// save dropped the main drain's Started obligations and its durable identity outright.
//
// The quarantined shape is planted in the RECORD, between a restart and the reconnect,
// because it is a shape this broker cannot write: recordPubRelIntent validates the exchange
// identity before it saves. Restore is what turns it into a quarantine, so the fixture
// reaches the same route a real recovery would.
//
// The final unrelated SUBSCRIBE is what makes the loss observable: without a successful
// save afterwards the dropped obligations stay latent in memory.
func TestQuarantinedCompanionDrainRefusesTheWholeFilterOverSubscribe(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second // no redelivery may race the assertions
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	const filter = "pairsid/#"
	const mainSid = "pairsid.>"
	companionSid := mainSid[:len(mainSid)-2] + mqttMultiLevelSidSuffix

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "pairsid"}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: filter, qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "pairsidpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// One unacknowledged QoS 2 delivery on EACH sid: `pairsid/x` reaches the main consumer
	// and `pairsid` reaches the level-up companion. Both are still in the PUBLISH phase, so
	// the UNSUBSCRIBE claims each of them and BOTH sids get a drain.
	for _, topic := range []string{"pairsid/x", "pairsid"} {
		testMQTTPublish(t, pc, pr, 2, false, false, topic, 1, []byte("payload"))
		testMQTTCheckPubMsgNoAck(t, c, r, topic, mqttPubQoS2, []byte("payload"))
	}
	testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: filter}})
	testMQTTFlush(t, c, nil, r)
	if n := testDrainCount(testMQTTResidentSession(t, s, "pairsid")); n != 2 {
		t.Fatalf("the wildcard must leave TWO drains, one per sid, got %d", n)
	}
	pc.Close()
	testMQTTDisconnect(t, c, nil)
	c.Close()

	s = testDrainQoS2Restart(t, s, o)

	// A throwaway client materializes the account session manager the record is
	// rewritten through; the restarted broker has none until something connects.
	probe, probeR := testMQTTConnect(t, &mqttConnInfo{clientID: "pairsidprobe", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer probe.Close()
	testMQTTCheckConnAck(t, probeR, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetGlobalASM(t, s)

	idHash := string(getHash("pairsid"))
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, idHash)
	if err != nil || smsg == nil {
		t.Fatalf("loading the persisted record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decoding the persisted record: %v", err)
	}
	planted, mainSeq, compSeq := ps.Drains[mainSid], uint64(0), uint64(0)
	comp := ps.Drains[companionSid]
	if planted == nil || comp == nil || len(planted.Started) != 1 || len(comp.Started) != 1 {
		t.Fatalf("the record must carry one claimed sequence per sid, got main=%+v comp=%+v",
			planted, comp)
	}
	mainSeq = planted.Started[0].Seq
	// Take the companion's claim past PUBREC with NO exchange identity — the shape
	// restore quarantines. The main sid keeps its plain PUBLISH-phase claim, so it stays
	// usable and adoptable, which is the precondition for the partial adoption.
	comp.Started[0].PubRel = true
	comp.Started[0].PI = 7
	comp.Started[0].OwnerNonce = _EMPTY_
	compSeq = comp.Started[0].Seq
	body, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("re-encoding the record: %v", err)
	}
	if _, err := asm.jsa.storeSessionMsg(asm.domainTk, idHash, 0, body); err != nil {
		t.Fatalf("planting the quarantined record: %v", err)
	}

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "pairsid", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	sess := testMQTTResidentSession(t, s, "pairsid")

	var mainDur, compDur string
	sess.mu.Lock()
	mainD, compD := sess.drains[mainSid], sess.drains[companionSid]
	if mainD != nil && mainD.cc != nil {
		mainDur = mainD.cc.Durable
	}
	quarantined := false
	if compD != nil && compD.cc != nil {
		compDur = compD.cc.Durable
		if st := compD.seqs[compSeq]; st != nil {
			quarantined = st.ownerUnusable
		}
	}
	// The main sid keeps its plain PUBLISH-phase claim above, so it stays UNHELD even after
	// F-094 widened the predicate to every past-PUBREC intent — which is the precondition
	// for the partial adoption this test exists to prevent.
	usable := sess.drainSidHoldLocked(mainSid) == mqttDrainNotHeld
	sess.mu.Unlock()
	if mainD == nil || compD == nil || mainDur == _EMPTY_ || compDur == _EMPTY_ {
		t.Fatalf("restore must keep BOTH drains, got main=%v comp=%v", mainD, compD)
	}
	if !quarantined {
		t.Fatal("the planted companion claim has no exchange identity and must restore QUARANTINED; " +
			"without that the refusal below proves nothing")
	}
	if !usable {
		t.Fatal("the main drain must restore USABLE: a refusal driven by the main sid alone " +
			"would not exercise the companion at all")
	}

	// The re-SUBSCRIBE must fail as a WHOLE FILTER, even though its main sid is fine.
	testMQTTSub(t, 3, c2, r2, []*mqttFilter{{filter: filter, qos: 2}}, []byte{mqttSubAckFailure})
	testMQTTFlush(t, c2, nil, r2)

	sess.mu.Lock()
	stillMain, stillComp := sess.drains[mainSid], sess.drains[companionSid]
	nCons := len(sess.cons)
	byDurMain, byDurComp := sess.drainByDur[mainDur], sess.drainByDur[compDur]
	sess.mu.Unlock()
	if stillMain != mainD {
		t.Errorf("the refused filter took the main drain away: %v", stillMain)
	}
	if stillComp != compD {
		t.Errorf("the refused filter took the companion drain away: %v", stillComp)
	}
	if byDurMain != mainD || byDurComp != compD {
		t.Errorf("both durables must still resolve to their drain, got %v and %v", byDurMain, byDurComp)
	}
	if nCons != 0 {
		t.Errorf("a refused filter must install NO consumer, got %d", nCons)
	}

	// Now force a SUCCESSFUL save with an unrelated filter: this is the save that used to
	// drop the adopted drain's obligations. Both drains, both Started entries and both
	// durable identities must survive it in the record.
	testMQTTSub(t, 4, c2, r2, []*mqttFilter{{filter: "pairsid-other", qos: 1}}, []byte{1})
	testMQTTFlush(t, c2, nil, r2)

	saved := testLoadDrainRecord(t, sess)
	for sid, want := range map[string]uint64{mainSid: mainSeq, companionSid: compSeq} {
		pd := saved.Drains[sid]
		if pd == nil {
			t.Fatalf("the save dropped the drain on %q: %+v", sid, saved.Drains)
		}
		var found bool
		for _, ds := range pd.Started {
			if ds.Seq == want {
				found = true
			}
		}
		if !found {
			t.Errorf("the save dropped %q's claim on sequence %d: %+v", sid, want, pd.Started)
		}
		if pd.Cons == nil || pd.Cons.Durable == _EMPTY_ {
			t.Errorf("the save dropped %q's durable identity: %+v", sid, pd)
		}
	}
}
