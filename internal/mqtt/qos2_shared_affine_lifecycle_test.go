//go:build mqttport_fulltests && mqttport_experimental_shared_qos2

// Conformance and invariant tests for session-affine ownership of a STARTED
// shared-subscription QoS 2 delivery (qos2_shared_affine.go).
//
// The property under test is [MQTT-4.8.2-4]/[MQTT-4.8.2-5]: once the server has
// begun delivering a shared message to one client it completes that delivery with
// THAT client, and the message may only die with that client's session — never be
// handed to another member. Every test here is written so it FAILS on the
// pre-change behavior, where a leaving member's pending state was dropped without
// acking and AckWait handed the started message to somebody else.

package mqtt

import (
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

type testQ2AffBroker struct {
	s     *Server
	o     *Options
	asm   *mqttAccountSessionManager
	jsa   *mqttJSA
	topic string
	group string
}

// testQ2AffSetup starts a broker with a short AckWait, so a delivery that is
// never acked is re-offered inside a test's lifetime — which is exactly the
// mechanism that used to redistribute a started message.
func testQ2AffSetup(t *testing.T, name string) *testQ2AffBroker {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 400 * time.Millisecond
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	return &testQ2AffBroker{s: s, o: o, topic: name + "/topic", group: name + "grp"}
}

// member connects a v5 client and joins the shared group at QoS 2. A non-zero
// Session Expiry Interval is essential rather than incidental: with the v5
// default of 0 a disconnect DISCARDS the session, and a discarded session's
// assignments are legitimately terminal — so a "the delivery waited for its
// owner" assertion would pass or fail for the wrong reason.
func (b *testQ2AffBroker) member(t *testing.T, id string, expiry uint32) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(expiry),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/" + b.group + "/" + b.topic, opts: 2}})
	if len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("Expected SUBACK [2] for %s, got %v", id, codes)
	}
	testMQTTFlush(t, c, nil, r)
	return c, r
}

// reconnect resumes an existing session without re-subscribing, which is how a
// client comes back to collect a delivery that was started before it left.
func (b *testQ2AffBroker) reconnect(t *testing.T, id string, expiry uint32) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(expiry),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	return c, r
}

// publish sends a QoS 2 PUBLISH and completes the inbound handshake. QoS 2 is
// required, not stylistic: the effective delivery QoS is the minimum of the
// publish QoS and the subscription QoS, so a QoS 1 publish to a QoS 2 shared
// subscription is delivered at QoS 1 and never enters the affine path at all.
func (b *testQ2AffBroker) publish(t *testing.T, pi uint16, payload string) {
	t.Helper()
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: fmt.Sprintf("%s-pub-%d", b.group, pi), cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPublishV5(t, cp, 2, pi, b.topic, []byte(payload))
	if reason, _ := testMQTTReadPubRecV5(t, rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBREC reason 0x%x for %q", reason, payload)
	}
	testQoS2SendPubRel(t, cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason 0x%x for %q", reason, payload)
	}
}

// groupDurable is the JS durable this test's shared group converges on. Tests
// that write synthetic claims need a REAL one: an assignment whose group consumer
// is authoritatively gone is legitimately un-re-offerable, which the reap treats
// as proof that arbitration is over.
func (b *testQ2AffBroker) groupDurable(t *testing.T) string {
	t.Helper()
	subj, err := mqttTopicToNATSPubSubject([]byte(b.topic))
	if err != nil {
		t.Fatalf("subject conversion: %v", err)
	}
	_, _, durable := mqttSharedNames(b.group, string(subj), 1)
	return durable
}

// completeExchange finishes the outbound QoS 2 handshake for pi, tolerating
// duplicate PUBLISH redeliveries. AckWait is deliberately short in these tests,
// so an unacknowledged copy is re-offered while the test is still working; those
// DUPs are correct protocol behavior and must not be mistaken for the PUBREL.
func (b *testQ2AffBroker) completeExchange(t *testing.T, c net.Conn, r *mqttReader, pi uint16) {
	t.Helper()
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r, time.Second)
		if !ready {
			continue
		}
		start := r.pos
		switch pb & mqttPacketMask {
		case mqttPacketPubRel:
			got, err := r.readUint16("pi")
			if err != nil {
				t.Fatalf("reading the PUBREL packet identifier: %v", err)
			}
			r.pos = start + pl
			if got != pi {
				t.Fatalf("PUBREL for packet identifier %d, want %d", got, pi)
			}
			testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
			return
		case mqttPacketPub:
			r.pos = start + pl // a duplicate redelivery; keep looking
		default:
			t.Fatalf("unexpected packet 0x%x while awaiting PUBREL", pb&mqttPacketMask)
		}
	}
	t.Fatal("no PUBREL arrived for a PUBREC'd QoS 2 delivery")
}

// affineConsumers returns the affine QoS 2 durables that exist on $MQTT_out for
// a client hash. Asserting on the STREAM rather than on session fields is what
// makes an unwind test mutation-verified: clearing the session's view of a
// consumer that is still alive is precisely the orphan this feature must not
// create.
func (b *testQ2AffBroker) affineConsumers(t *testing.T, idHash string) []string {
	t.Helper()
	cis, err := b.jsa.consumerList(mqttOutStreamName)
	if err != nil {
		t.Fatalf("listing consumers on %s: %v", mqttOutStreamName, err)
	}
	var out []string
	prefix := mqttQ2AffineConsumerDurablePrefix + idHash + "_"
	for _, ci := range cis {
		if ci != nil && strings.HasPrefix(ci.Name, prefix) {
			out = append(out, ci.Name)
		}
	}
	return out
}

// bind captures the account manager once at least one MQTT client exists.
func (b *testQ2AffBroker) bind(t *testing.T, clientID string) {
	t.Helper()
	cl := testMQTTGetClient(t, b.s, clientID)
	b.asm, b.jsa = cl.mqtt.asm, cl.mqtt.sess.jsa
}

// claims returns every assignment claim currently on $MQTT_qos2assign, keyed by
// subject. Reading the durable record is the only way to assert about ownership:
// the wire shows who received a message, never who owns it.
func (b *testQ2AffBroker) claims(t *testing.T) map[string]*mqttQ2AffClaim {
	t.Helper()
	out := map[string]*mqttQ2AffClaim{}
	if err := b.asm.scanQ2AffClaims(func(subject string, claim *mqttQ2AffClaim, _ uint64) error {
		out[subject] = claim
		return nil
	}); err != nil {
		t.Fatalf("scanning assignment claims: %v", err)
	}
	return out
}

// waitForClaim polls until a claim exists that satisfies pred, or fails. Polling
// (rather than a fixed sleep) because the move runs on a JetStream delivery
// callback, asynchronously to the publish that triggered it.
func (b *testQ2AffBroker) waitForClaim(t *testing.T, what string, pred func(*mqttQ2AffClaim) bool) (string, *mqttQ2AffClaim) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last map[string]*mqttQ2AffClaim
	for time.Now().Before(deadline) {
		last = b.claims(t)
		for subj, c := range last {
			if pred(c) {
				return subj, c
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for an assignment claim that is %s; claims: %v", what, last)
	return _EMPTY_, nil
}

func (b *testQ2AffBroker) waitForNoClaims(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(b.claims(t)) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("assignment claims were not reaped: %s", b.dumpClaims(t))
}

func (b *testQ2AffBroker) dumpClaims(t *testing.T) string {
	t.Helper()
	var sb strings.Builder
	for subj, c := range b.claims(t) {
		fmt.Fprintf(&sb, "\n  %s -> %+v", subj, *c)
	}
	return sb.String()
}

// unsubscribe sends an UNSUBSCRIBE and waits for its UNSUBACK, tolerating
// duplicate PUBLISH redeliveries that the short AckWait interleaves.
func (b *testQ2AffBroker) unsubscribe(t *testing.T, c net.Conn, r *mqttReader, pi uint16, topics []string) {
	t.Helper()
	w := newMQTTWriter(0)
	vh := newMQTTWriter(0)
	vh.WriteUint16(pi)
	vh.WriteVarInt(0) // empty properties
	for _, tp := range topics {
		vh.WriteBytes([]byte(tp))
	}
	w.WriteByte(mqttPacketUnsub | mqttUnsubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("writing UNSUBSCRIBE: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r, time.Second)
		if !ready {
			continue
		}
		start := r.pos
		switch pb & mqttPacketMask {
		case mqttPacketUnsubAck:
			r.pos = start + pl
			return
		case mqttPacketPub:
			r.pos = start + pl // a duplicate redelivery; keep looking
		default:
			t.Fatalf("unexpected packet 0x%x while awaiting UNSUBACK", pb&mqttPacketMask)
		}
	}
	t.Fatal("no UNSUBACK arrived")
}

// THE headline conformance test. A shared QoS 2 message is delivered to one
// member, which then goes away WITHOUT completing the exchange. Before this
// change the message was pending on the group durable, so AckWait handed it to
// the other member. It must now wait for the client it was started to, and
// complete with that client when it returns.
func TestQ2AffineStartedDeliveryStaysWithChosenMember(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "stay")
	ca, ra := b.member(t, "stayA", 300)
	cb, rb := b.member(t, "stayB", 300)
	defer cb.Close()
	b.bind(t, "stayA")

	b.publish(t, 1, "affine")

	// Whichever member the group chose is the OWNER from here on.
	var ownerID string
	var ownerConn net.Conn
	var otherReader *mqttReader
	topic, payload, qos, _, ok := testMQTTTryReadPublishV5(ra, 2*time.Second)
	if ok {
		ownerID, ownerConn, otherReader = "stayA", ca, rb
	} else {
		topic, payload, qos, _, ok = testMQTTTryReadPublishV5(rb, 2*time.Second)
		if !ok {
			ca.Close()
			t.Fatal("neither member received the shared QoS 2 delivery")
		}
		ownerID, ownerConn, otherReader = "stayB", cb, ra
	}
	if qos != 2 || topic != b.topic || string(payload) != "affine" {
		t.Fatalf("unexpected delivery: topic=%q payload=%q qos=%d", topic, payload, qos)
	}

	// The claim must name the owner and say a copy exists: until it does, nothing
	// authorizes a delivery.
	_, claim := b.waitForClaim(t, "materialized", func(c *mqttQ2AffClaim) bool { return c.Phase == mqttQ2AffMaterialized })
	if claim.Winner != getHash(ownerID) {
		t.Fatalf("claim names %q, but %q received the delivery", claim.Winner, ownerID)
	}

	// The owner leaves WITHOUT a PUBREC: the exchange is started but unfinished.
	ownerConn.Close()

	// Several AckWait intervals: this is the exact window in which the old
	// behavior redistributed the message.
	if _, pl, _, _, got := testMQTTTryReadPublishV5(otherReader, 2*time.Second); got {
		t.Fatalf("a started shared QoS 2 delivery was redistributed to the other member (payload %q)", pl)
	}

	// The owner comes back and completes the exchange it started, with DUP set.
	cr, rr := b.reconnect(t, ownerID, 300)
	defer cr.Close()
	topic, payload, qos, pi, ok := testMQTTTryReadPublishV5(rr, 5*time.Second)
	if !ok {
		t.Fatal("the owning session did not receive its started delivery on resume")
	}
	if qos != 2 || topic != b.topic || string(payload) != "affine" {
		t.Fatalf("unexpected redelivery: topic=%q payload=%q qos=%d", topic, payload, qos)
	}
	b.completeExchange(t, cr, rr, pi)

	// The other member must never see it, before or after completion.
	testMQTTExpectNothing(t, otherReader)
	// And the completed assignment must not linger: both sides are now proven.
	b.waitForNoClaims(t)
}

// Leaving the GROUP (UNSUBSCRIBE) is the other half of [MQTT-4.8.2-5]: the
// started delivery is not the group's to reassign, and the leaver still owes the
// client a completed exchange. This is the case releaseSharedConsumer used to
// break by dropping pending state without acking.
func TestQ2AffineUnsubscribeMidWindowKeepsDelivery(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "unsub")
	ca, ra := b.member(t, "unsubA", 300)
	defer ca.Close()
	cb, rb := b.member(t, "unsubB", 300)
	defer cb.Close()
	b.bind(t, "unsubA")

	b.publish(t, 1, "leaving")

	ownerC, ownerR, otherR := ca, ra, rb
	_, _, _, pi, ok := testMQTTTryReadPublishV5(ra, 2*time.Second)
	if !ok {
		if _, _, _, p, got := testMQTTTryReadPublishV5(rb, 2*time.Second); got {
			ownerC, ownerR, otherR, pi = cb, rb, ra, p
		} else {
			t.Fatal("neither member received the shared QoS 2 delivery")
		}
	}
	b.waitForClaim(t, "materialized", func(c *mqttQ2AffClaim) bool { return c.Phase == mqttQ2AffMaterialized })

	// The owner leaves the group with the exchange still open.
	b.unsubscribe(t, ownerC, ownerR, 2, []string{"$share/" + b.group + "/" + b.topic})

	if _, pl, _, _, got := testMQTTTryReadPublishV5(otherR, 2*time.Second); got {
		t.Fatalf("leaving the group redistributed a started QoS 2 delivery (payload %q)", pl)
	}
	// The owner still completes it: the exchange lives on its own consumer, which
	// group membership never governed.
	b.completeExchange(t, ownerC, ownerR, pi)
	testMQTTExpectNothing(t, otherR)
	b.waitForNoClaims(t)
}

// Dying with the session is PERMITTED ([MQTT-4.8.2-5]); being handed to another
// member is not. A discarded session's claim must go terminal, and the message
// must not reappear anywhere.
func TestQ2AffineSessionEndDiscardsRatherThanRedistributes(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "discard")
	// Expiry 0 on BOTH members: whichever one the group chooses, closing it ENDS
	// its session rather than suspending it, so the test never depends on which
	// member won.
	ca, ra := b.member(t, "discardA", 0)
	cb, rb := b.member(t, "discardB", 0)
	defer ca.Close()
	defer cb.Close()
	b.bind(t, "discardA")

	b.publish(t, 1, "dies")

	ownerConn, otherReader := ca, rb
	if _, _, _, _, ok := testMQTTTryReadPublishV5(ra, 3*time.Second); !ok {
		if _, _, _, _, got := testMQTTTryReadPublishV5(rb, 3*time.Second); !got {
			t.Fatal("neither member received the shared QoS 2 delivery")
		}
		ownerConn, otherReader = cb, ra
	}
	b.waitForClaim(t, "materialized", func(c *mqttQ2AffClaim) bool { return c.Phase == mqttQ2AffMaterialized })

	ownerConn.Close() // ends the session

	if _, pl, _, _, got := testMQTTTryReadPublishV5(otherReader, 2*time.Second); got {
		t.Fatalf("a started QoS 2 delivery was redistributed after its session ended (payload %q)", pl)
	}
	// The assignment is terminal, or already reaped once both sides were proven.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cl := b.claims(t)
		if len(cl) == 0 {
			return
		}
		allTerminal := true
		for _, c := range cl {
			if !c.Discarded {
				allTerminal = false
			}
		}
		if allTerminal {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the ended session's assignment was neither discarded nor reaped: %v", b.claims(t))
}

// The delivery gate. A copy becomes visible to its consumer the moment it is
// stored, which is BEFORE the claimed->materialized CAS commits. Publishing in
// that window would let the client PUBREC it and ack the copy away, leaving
// recovery to see a `claimed` claim with no copy — which it resolves by creating
// a SECOND copy. So: no PUBLISH until the claim durably says materialized.
func TestQ2AffineDeliveryGatedOnMaterializedClaim(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "gate")
	ca, ra := b.member(t, "gateA", 300)
	defer ca.Close()
	b.bind(t, "gateA")

	var held atomic.Bool
	held.Store(true)
	b.asm.mu.Lock()
	b.asm.testQ2AffPreMaterialize = func(string, uint64) error {
		if held.Load() {
			return fmt.Errorf("materialization held by the test")
		}
		return nil
	}
	b.asm.mu.Unlock()

	b.publish(t, 1, "gated")

	// The copy exists (the store precedes the CAS) but the claim does not
	// authorize it, so nothing may reach the client.
	b.waitForClaim(t, "claimed", func(c *mqttQ2AffClaim) bool { return c.Phase == mqttQ2AffClaimed })
	if _, pl, _, _, got := testMQTTTryReadPublishV5(ra, 1500*time.Millisecond); got {
		t.Fatalf("a PUBLISH was sent under a claim that is not materialized (payload %q)", pl)
	}

	// Release the gate: AckWait re-offers the source, the claim materializes, and
	// the already-stored copy is delivered from the owner's own durable.
	held.Store(false)
	b.asm.mu.Lock()
	b.asm.testQ2AffPreMaterialize = nil
	b.asm.mu.Unlock()

	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(ra, 5*time.Second)
	if !ok {
		t.Fatal("the delivery never arrived after the claim was materialized")
	}
	if qos != 2 || string(payload) != "gated" {
		t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
	}
	b.completeExchange(t, ca, ra, pi)
	b.waitForNoClaims(t)
}

// An absent winner must not stall the group. A valid persistent session may
// simply never reconnect, so a contender materializes the copy ON THE WINNER'S
// BEHALF — onto the winner's own durable — and releases the group delivery. The
// message then waits at the winner, not in the group's queue.
func TestQ2AffineAbsentWinnerMaterializedOnBehalf(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "onbehalf")
	ca, ra := b.member(t, "obA", 300)
	cb, rb := b.member(t, "obB", 300)
	defer cb.Close()
	b.bind(t, "obA")

	// Hold every materialization so the first claim stays at `claimed`.
	var held atomic.Bool
	held.Store(true)
	b.asm.mu.Lock()
	b.asm.testQ2AffPreMaterialize = func(string, uint64) error {
		if held.Load() {
			return fmt.Errorf("materialization held by the test")
		}
		return nil
	}
	b.asm.mu.Unlock()

	b.publish(t, 1, "behalf")
	_, claim := b.waitForClaim(t, "claimed", func(c *mqttQ2AffClaim) bool { return c.Phase == mqttQ2AffClaimed })

	winnerA := claim.Winner == getHash("obA")
	winnerID := "obA"
	winnerConn, otherReader := ca, rb
	if !winnerA {
		winnerID, winnerConn, otherReader = "obB", cb, ra
	}
	// The winner disappears with its SESSION intact: the record and the affine
	// consumer both survive, which is exactly what an on-behalf materializer has
	// to verify (record first, then consumer).
	winnerConn.Close()

	held.Store(false)
	b.asm.mu.Lock()
	b.asm.testQ2AffPreMaterialize = nil
	b.asm.mu.Unlock()

	// The surviving member takes the group redelivery and materializes for the
	// winner. It must NOT deliver the message itself.
	b.waitForClaim(t, "materialized on behalf of the absent winner", func(c *mqttQ2AffClaim) bool {
		return c.Phase == mqttQ2AffMaterialized && c.Winner == claim.Winner
	})
	if _, pl, _, _, got := testMQTTTryReadPublishV5(otherReader, 1500*time.Millisecond); got {
		t.Fatalf("the contender delivered a message assigned to another session (payload %q)", pl)
	}

	// The winner returns and collects it from its own durable.
	cw, rw := b.reconnect(t, winnerID, 300)
	defer cw.Close()
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rw, 5*time.Second)
	if !ok {
		t.Fatal("the absent winner did not receive the delivery materialized on its behalf")
	}
	if qos != 2 || string(payload) != "behalf" {
		t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
	}
	b.completeExchange(t, cw, rw, pi)
	testMQTTExpectNothing(t, otherReader)
}

// testQ2AffLiveSourceSeq stores one QoS 1 message on topic and returns its
// $MQTT_msgs sequence, leaving it UNACKED so it stays on the stream. A plain
// subscriber holds the interest that keeps it there ($MQTT_msgs is
// interest-retained) and deliberately never acknowledges it.
//
// Tests that hand-write a claim need this: a claim's source sequence is not an
// arbitrary number, and one pointing at a message that does not exist is itself a
// proof that the delivery can never be re-offered.
func testQ2AffLiveSourceSeq(t *testing.T, b *testQ2AffBroker, topic string) uint64 {
	t.Helper()
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "srcholder", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	t.Cleanup(func() { cs.Close() })
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	testMQTTFlush(t, cs, nil, rs)

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "srcpub", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPublishV5(t, cp, 1, 1, topic, []byte("held"))
	testMQTTReadPubAck(t, rp, 1)

	subj, err := mqttTopicToNATSPubSubject([]byte(topic))
	if err != nil {
		t.Fatalf("subject conversion: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		smsg, err := b.jsa.loadLastMsgFor(mqttStreamName, mqttStreamSubjectPrefix+string(subj))
		if err == nil && smsg != nil {
			return smsg.Sequence
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the source message on %q never appeared on %s", topic, mqttStreamName)
	return 0
}

// The reap rule, against a real stream. A claim may be deleted only when BOTH
// sides are proven, and a terminal fact is NOT a substitute for the source ack:
// deleting a discarded claim whose group ack never committed lets the source
// redelivery elect a fresh winner, which is the central violation reached through
// the discard side-door.
func TestQ2AffineReapRequiresBothSidesProven(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "reap")
	ca, _ := b.member(t, "reapA", 300)
	defer ca.Close()
	b.bind(t, "reapA")

	// Both of the reaper's source-side proofs must be denied, or it would
	// legitimately conclude SourceAcked and the "unproven source" this test is
	// about would not exist:
	//   - the group durable must really exist (a consumer that is gone can never
	//     re-offer anything), and
	//   - the claim's SOURCE MESSAGE must really exist (a message that is gone can
	//     never be re-offered either).
	// So the fixture borrows the sequence of a genuinely-stored, still-unacked
	// message rather than inventing one.
	groupDur := b.groupDurable(t)
	sourceSeq := testQ2AffLiveSourceSeq(t, b, "reap/keep")
	subject := mqttQ2AffClaimSubject(groupDur, sourceSeq)
	write := func(t *testing.T, c *mqttQ2AffClaim) {
		t.Helper()
		_, seq, err := b.asm.loadQ2AffClaim(subject)
		if err != nil {
			t.Fatalf("loading the claim: %v", err)
		}
		if _, err := b.asm.storeQ2AffClaimCAS(subject, seq, c); err != nil {
			t.Fatalf("writing the claim: %v", err)
		}
	}
	exists := func(t *testing.T) bool {
		t.Helper()
		c, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil {
			t.Fatalf("loading the claim: %v", err)
		}
		return c != nil
	}

	base := &mqttQ2AffClaim{
		Winner:       getHash("reapA"),
		Gen:          mqttNewQ2AffGen(),
		GroupDurable: groupDur,
		SourceSeq:    sourceSeq,
		ClaimHash:    mqttQ2AffClaimHash(groupDur, sourceSeq),
		AffDurable:   "$MQTT_Q2AFF_reaptest",
		Phase:        mqttQ2AffMaterialized,
	}
	// The copy subject is DERIVED from the record's own identity, and nothing ever
	// stores a message on it — so "copy absent" is still a real answer from the
	// stream, and the record is now one a real election could have written. A
	// hand-built subject is refused outright since F-065.
	testQ2AffSelfConsistent(base)

	t.Run("discarded but source unproven survives", func(t *testing.T) {
		c := *base
		c.Discarded = true
		write(t, &c)
		b.asm.reapQ2AffClaim(b.s, subject)
		if !exists(t) {
			t.Fatal("a discarded claim whose group ack was never proven was reaped; a source redelivery could now elect a new winner")
		}
	})

	t.Run("source acked but affine unproven survives", func(t *testing.T) {
		c := *base
		c.SourceAcked = true
		c.Phase = mqttQ2AffClaimed // no copy proven either way
		write(t, &c)
		b.asm.reapQ2AffClaim(b.s, subject)
		if !exists(t) {
			t.Fatal("a claim whose affine side is unproven was reaped; its copy would be stranded behind the fail-closed delivery gate")
		}
	})

	t.Run("both sides proven is reaped", func(t *testing.T) {
		c := *base
		c.SourceAcked = true
		c.AffineDone = true
		write(t, &c)
		b.asm.reapQ2AffClaim(b.s, subject)
		if exists(t) {
			t.Fatal("a claim with both sides proven was not reaped")
		}
	})
}

// A claim transition must never erase a fact another writer already proved. The
// scenario is real: clear() discards a claim while a contender is recording the
// source ack it just confirmed. Asserted at the CAS layer, because the reap that
// follows a discard in clear() would otherwise remove the evidence.
func TestQ2AffineDiscardPreservesProvenSourceAck(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "preserve")
	ca, _ := b.member(t, "presA", 300)
	defer ca.Close()
	b.bind(t, "presA")

	gen := mqttNewQ2AffGen()
	idHash := getHash("presA")
	groupDur := b.groupDurable(t)
	subject := mqttQ2AffClaimSubject(groupDur, 7)
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, &mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: 7,
		ClaimHash:   mqttQ2AffClaimHash(groupDur, 7),
		Nonce:       testQ2AffFixtureNonce,
		CopySubject: mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, 7), testQ2AffFixtureNonce),
		Phase:       mqttQ2AffMaterialized, SourceAcked: true,
	}); err != nil {
		t.Fatalf("writing the claim: %v", err)
	}

	got, _, err := b.asm.advanceQ2AffClaim(subject, idHash, gen, _EMPTY_, _EMPTY_, func(c *mqttQ2AffClaim) { c.Discarded = true })
	if err != nil {
		t.Fatalf("discarding: %v", err)
	}
	if got == nil {
		t.Fatal("the discard reported no claim")
	}
	if !got.Discarded {
		t.Fatal("the claim was not discarded")
	}
	if !got.SourceAcked {
		t.Fatal("the discard erased a proven source ack; a lost group ack could now reopen the assignment")
	}
	if got.Phase != mqttQ2AffMaterialized {
		t.Fatalf("the discard demoted the phase to %q", got.Phase)
	}
	// And the same must be true of the DURABLE record, not just the value returned.
	stored, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil || stored == nil {
		t.Fatalf("reloading the claim: %v", err)
	}
	if !stored.SourceAcked || !stored.Discarded || stored.Phase != mqttQ2AffMaterialized {
		t.Fatalf("the stored claim lost a proven fact: %+v", stored)
	}

	// A transition must also refuse to touch a claim owned by a different
	// generation, rather than reshape it.
	if _, _, err := b.asm.advanceQ2AffClaim(subject, idHash, mqttNewQ2AffGen(), _EMPTY_, _EMPTY_,
		func(c *mqttQ2AffClaim) { c.AffineDone = true }); err == nil {
		t.Fatal("a transition was applied to a claim owned by another generation")
	}
}

// The affine names are generation-scoped, and the persisted record must name the
// consumer before the gate opens. A save failure must leave nothing usable
// behind: a half-provisioned path a move could latch onto is worse than no path
// at all, because $MQTT_out is interest-retained and would silently discard the
// copy it stored.
func TestQ2AffineProvisioningRollsBackOnSaveFailure(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var fail atomic.Bool
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error {
		if fail.Load() {
			return fmt.Errorf("save refused by the test")
		}
		return nil
	})
	defer restore()

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "rollback", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	fail.Store(true)
	codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/rb/rb/topic", opts: 2}})
	fail.Store(false)
	if len(codes) != 1 || codes[0] < mqttSubAckFailure {
		t.Fatalf("expected the shared QoS 2 subscribe to fail, got %v", codes)
	}

	cl := testMQTTGetClient(t, s, "rollback")
	sess := cl.mqtt.sess
	sess.mu.Lock()
	ready, consumer, gen := sess.q2AffReady, sess.q2AffConsumer, sess.q2AffGen
	sess.mu.Unlock()
	if ready {
		t.Fatal("the readiness gate opened despite a failed save")
	}
	if consumer != nil || gen != _EMPTY_ {
		t.Fatalf("a failed provisioning left state behind: consumer=%v gen=%q", consumer, gen)
	}
}

// The affine consumer's MaxAckPending must be charged to the session ONCE, not
// once per reconnect. The delivery subscription and the readiness gate belong to
// the connection, so a resume re-runs provisioning; billing there would grow
// tmaxack without bound until shared QoS 2 subscribes started failing against
// mqttMaxAckTotalLimit — with nothing having changed but the reconnect count.
func TestQ2AffineResumeDoesNotRechargeAckBudget(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "charge")
	ca, _ := b.member(t, "chargeA", 300)
	b.bind(t, "chargeA")

	sess := testMQTTGetClient(t, b.s, "chargeA").mqtt.sess
	sess.mu.Lock()
	first := sess.tmaxack
	sess.mu.Unlock()
	if first == 0 {
		t.Fatal("the affine consumer was never charged at all")
	}

	for i := 0; i < 3; i++ {
		ca.Close()
		cr, _ := b.reconnect(t, "chargeA", 300)
		sess.mu.Lock()
		got := sess.tmaxack
		sess.mu.Unlock()
		if got != first {
			t.Fatalf("after %d resume(s) tmaxack is %d, want %d: the affine consumer is being recharged per reconnect", i+1, got, first)
		}
		ca = cr
	}
	ca.Close()
}

// clear() is not only a teardown: the expired-session path reuses the very same
// session object as a fresh session. A move fence left closed by that teardown
// would make every later shared QoS 2 subscribe on the reused object fail with
// "session is terminating".
func TestQ2AffineClearLeavesSessionReusable(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "reuse")
	ca, _ := b.member(t, "reuseA", 300)
	defer ca.Close()
	b.bind(t, "reuseA")

	cl := testMQTTGetClient(t, b.s, "reuseA")
	sess := cl.mqtt.sess
	if err := sess.clear(true); err != nil {
		t.Fatalf("clearing the session: %v", err)
	}
	sess.mu.Lock()
	closing, gen, ready := sess.q2AffClosing, sess.q2AffGen, sess.q2AffReady
	sess.mu.Unlock()
	if closing {
		t.Fatal("clear() left the move fence closed; a reused session object could never provision shared QoS 2 again")
	}
	if gen != _EMPTY_ || ready {
		t.Fatalf("clear() left affine state behind: gen=%q ready=%v", gen, ready)
	}
	// And the cleared object really can provision again — at its NEW record
	// generation, since clear() ended the old one.
	if err := testSessRecordTx(t, sess, func(tok recordTx) error {
		_, err := sess.ensureQ2AffineConsumerSubscription(cl, tok)
		return err
	}); err != nil {
		t.Fatalf("a cleared session could not re-provision its affine path: %v", err)
	}
	sess.mu.Lock()
	ready, gen = sess.q2AffReady, sess.q2AffGen
	sess.mu.Unlock()
	if !ready || gen == _EMPTY_ {
		t.Fatalf("re-provisioning did not open the gate: ready=%v gen=%q", ready, gen)
	}
}

// A persisted affine consumer whose identity does not match the session's
// generation must block the resume rather than be replaced: the consumer we
// cannot account for may still hold an in-flight PUBLISH, and re-provisioning
// over it would strand or delete that message.
func TestQ2AffineUnaccountableConsumerFailsClosed(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "closed", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	cl := testMQTTGetClient(t, s, "closed")
	sess := cl.mqtt.sess
	// A record from a generation this session does not own.
	sess.mu.Lock()
	sess.q2AffGen = mqttNewQ2AffGen()
	sess.q2AffConsumer = &ConsumerConfig{
		Durable:        mqttQ2AffineConsumerDurablePrefix + "someoneelse",
		FilterSubject:  mqttQ2AffineSubjectPrefix + "someoneelse.>",
		DeliverSubject: mqttQ2AffineDeliverPrefix + "someoneelse",
	}
	sess.mu.Unlock()

	if err := testSessRecordTx(t, sess, func(tok recordTx) error {
		_, err := sess.ensureQ2AffineConsumerSubscription(cl, tok)
		return err
	}); err == nil {
		t.Fatal("an unaccountable affine consumer was accepted; it must fail closed")
	} else if !strings.Contains(err.Error(), "unaccountable") {
		t.Fatalf("unexpected failure reason: %v", err)
	}
	sess.mu.Lock()
	ready := sess.q2AffReady
	sess.mu.Unlock()
	if ready {
		t.Fatal("the readiness gate opened over an unaccountable consumer")
	}
}

// A SUBSCRIBE whose FINAL session save fails must leave no affine delivery path
// behind. It costs a JetStream consumer, a MaxAckPending charge and a persisted
// generation — none of which a packet that never took effect should leave in
// place, and all of which the roadmap item required to be unwound.
func TestQ2AffineSubscribeRollbackUnwindsProvisioning(t *testing.T) {
	b := testQ2AffSetup(t, "subrb")
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "subrb", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	b.bind(t, "subrb")

	sess := testMQTTGetClient(t, b.s, "subrb").mqtt.sess
	sess.mu.Lock()
	before := sess.tmaxack
	sess.mu.Unlock()

	// Fail only the FINAL packet save. Save #1 is the affine provisioning's own
	// gating write and #2 the shared group's early membership persist; both must
	// SUCCEED, or this would exercise the (separately covered) provisioning and
	// per-filter failure paths instead of the packet rollback it is about. The
	// packet carries a plain filter alongside the shared one so the final update
	// has something left to persist — with the shared filter alone, the early
	// join already wrote it and the final save is skipped.
	var count int
	restore := setMQTTTestSessSaveHook(func(ps *mqttSession) error {
		if ps.id != "subrb" {
			return nil
		}
		count++
		if count == 3 {
			return fmt.Errorf("final packet save refused by the test")
		}
		return nil
	})
	defer restore()

	vh := newMQTTWriter(0)
	vh.WriteUint16(1)
	vh.WriteVarInt(0)
	vh.WriteBytes([]byte("$share/rbg/rb/topic"))
	vh.WriteByte(2)
	vh.WriteBytes([]byte("rb/plain"))
	vh.WriteByte(1)
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("writing SUBSCRIBE: %v", err)
	}
	// No SUBACK: a rolled-back packet closes the connection.
	var buf [16]byte
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if n, err := c.Read(buf[:]); err == nil && n > 0 && buf[0]&mqttPacketMask == mqttPacketSubAck {
		t.Fatal("expected no SUBACK from a rolled-back SUBSCRIBE")
	}

	testQ2AffAssertUnwound(t, b, sess, getHash("subrb"), before,
		"a rolled-back SUBSCRIBE left affine provisioning behind")
}

// testQ2AffAssertUnwound waits for every trace of an affine path to be gone: the
// session's view of it, the durable on $MQTT_out, and its share of the
// MaxAckPending budget. The budget check allows the PUBREL consumer's charge,
// which any QoS 2 subscription provisions and which this feature does not own.
func testQ2AffAssertUnwound(t *testing.T, b *testQ2AffBroker, sess *mqttSession, idHash string, before int, what string) {
	t.Helper()
	maxAckPending := mqttDefaultMaxAckPending
	deadline := time.Now().Add(10 * time.Second)
	var gen string
	var consumer *ConsumerConfig
	var ready bool
	var after int
	var durables []string
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		gen, consumer, ready, after = sess.q2AffGen, sess.q2AffConsumer, sess.q2AffReady, sess.tmaxack
		sess.mu.Unlock()
		durables = b.affineConsumers(t, idHash)
		if gen == _EMPTY_ && consumer == nil && !ready && len(durables) == 0 && after <= before+maxAckPending {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: gen=%q consumer=%v ready=%v durables=%v tmaxack=%d (was %d, at most %d allowed for the PUBREL consumer)",
		what, gen, consumer, ready, durables, after, before, before+maxAckPending)
}

// An assignment claim that cannot be read or attributed must ABORT the scan.
// Skipping it lets clear() delete the session record and the affine consumer
// while a live claim still names that generation — and a claim is honored
// indefinitely, so its source delivery would stall forever with nothing left able
// to complete it.
func TestQ2AffineUnreadableClaimFailsScanClosed(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "corrupt")
	ca, _ := b.member(t, "corruptA", 300)
	defer ca.Close()
	b.bind(t, "corruptA")

	gen := mqttNewQ2AffGen()
	idHash := getHash("corruptA")
	groupDur := b.groupDurable(t)

	t.Run("undecodable record", func(t *testing.T) {
		subject := mqttQ2AffClaimSubject(groupDur, 900)
		if _, err := b.jsa.storeMsg(subject, 0, []byte("this is not a claim record")); err != nil {
			t.Fatalf("writing a corrupt record: %v", err)
		}
		if err := b.asm.scanQ2AffClaims(func(string, *mqttQ2AffClaim, uint64) error { return nil }); err == nil {
			t.Fatal("the scan reported success over a record it could not decode")
		}
		if err := b.asm.discardQ2AffClaimsFor(idHash, gen); err == nil {
			t.Fatal("clear() would have completed while an unreadable claim was still out there")
		}
	})

	t.Run("record with no usable identity", func(t *testing.T) {
		subject := mqttQ2AffClaimSubject(groupDur, 901)
		// Structurally valid JSON, but nothing that can be attributed to a session.
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, &mqttQ2AffClaim{Phase: mqttQ2AffClaimed}); err != nil {
			t.Fatalf("writing an identity-less record: %v", err)
		}
		if err := b.asm.scanQ2AffClaims(func(string, *mqttQ2AffClaim, uint64) error { return nil }); err == nil {
			t.Fatal("the scan accepted a claim with no usable identity; it cannot be proven not to be ours")
		}
	})
}

// Losing the affine consumer out of band is unproven delivery, and recovery must
// not launder it into a completed one. $MQTT_out is interest-retained, so deleting
// the consumer deletes the copies it held; an absent copy under a materialized
// claim then means two different things, and only one of them is a success. The
// claim must end up recorded as LOST — terminal, so nothing waits on it, but never
// reaped, so the durable evidence survives.
//
// "Unproven", not "lost": the inference behind LOST is sound for the JetStream
// copy ack, which is recorded before the copy is acked, but NOT for the client's
// PUBREC, which is answered earlier still — so this shape can also be a message
// that arrived (F-064's residual 1). Nothing this fixture asserts depends on which
// it was; the direction it pins is that recovery must not record COMPLETION it
// cannot prove.
func TestQ2AffineConsumerLossIsRecordedAsLossNotCompletion(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "lostcons")
	ca, ra := b.member(t, "lostA", 300)
	b.bind(t, "lostA")

	sess := testMQTTGetClient(t, b.s, "lostA").mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	if gen == _EMPTY_ {
		t.Fatal("the affine path was never provisioned")
	}
	_, _, durable := mqttQ2AffineNames(getHash("lostA"), gen)

	// Get a real copy pending on that consumer: delivered, but NOT PUBREC'd, so
	// the copy is still on $MQTT_out when the consumer disappears.
	b.publish(t, 1, "willbelost")
	if _, payload, qos, _, ok := testMQTTTryReadPublishV5(ra, 5*time.Second); !ok || qos != 2 || string(payload) != "willbelost" {
		t.Fatalf("shared QoS 2 delivery: ok=%v qos=%d payload=%q", ok, qos, payload)
	}
	subject, _ := b.waitForClaim(t, "materialized", func(c *mqttQ2AffClaim) bool { return c.Phase == mqttQ2AffMaterialized })

	// The consumer is destroyed out of band. Since F-036 course E no sweep route reclaims
	// one by ABSENCE, so this fixture stages the state by hand rather than reproducing that
	// route. (An operator, or a lifecycle teardown, can still delete one — this fixture
	// simply does not depend on either.) Its pending copy goes with it.
	ca.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		gone := sess.disconnectedAt != 0
		sess.mu.Unlock()
		if gone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := b.jsa.deleteConsumer(mqttOutStreamName, durable, false); err != nil {
		t.Fatalf("deleting the affine consumer: %v", err)
	}

	cr, _ := b.reconnect(t, "lostA", 300)
	defer cr.Close()

	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		claim, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil {
			t.Fatalf("loading the claim: %v", err)
		}
		if claim == nil {
			t.Fatal("the assignment for an unproven QoS 2 delivery was REAPED; the only durable evidence of it is gone")
		}
		if claim.AffineDone {
			t.Fatal("a QoS 2 delivery that was never proven was recorded as completed")
		}
		if claim.Lost {
			if !claim.terminal() {
				t.Fatal("a lost assignment must be terminal so no contender waits on it")
			}
			if claim.reapable(true) {
				t.Fatal("a lost assignment must never be reapable")
			}
			// And the session keeps working: the durable is recreated.
			if _, err := b.jsa.lookupConsumer(mqttOutStreamName, durable); err != nil {
				t.Fatalf("the affine consumer was not recreated: %v", err)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	claim, _, _ := b.asm.loadQ2AffClaim(subject)
	t.Fatalf("the lost delivery was never recorded as lost: %+v", claim)
}

// A CONTENDER must not reap a lost delivery. If the affine consumer disappears
// before the group ack commits, the source is redelivered to another member; that
// contender confirms the ack and runs the reaper — on a broker that has never
// seen the owner's consumer and cannot know whether it survived. Concluding
// "materialized with no copy left ⇒ delivered" there deletes the only durable
// record of a message the broker accepted and could not prove it delivered, and
// it happens BEFORE the owner ever resumes to record it as lost. ("Could not
// prove", not "never delivered": the client's PUBREC precedes the affine record,
// so this shape can also be a message that arrived — F-064's residual 1. The
// evidence is worth keeping either way, which is all this fixture asserts.)
//
// The reaper call below is not a stand-in: it is literally what the contender
// runs after confirmSourceAckAndRecord (moveStartedSharedQoS2's foreign-winner
// branch). Driving it directly is what makes the ordering deterministic — an
// unconfirmed source ack cannot be produced from the wire, since the broker acks
// the group delivery as soon as it can prove the copy present.
func TestQ2AffineContenderCannotReapALostDelivery(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "contreap")
	ca, ra := b.member(t, "contA", 300)
	b.bind(t, "contA")

	sess := testMQTTGetClient(t, b.s, "contA").mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	_, _, durable := mqttQ2AffineNames(getHash("contA"), gen)

	b.publish(t, 1, "contended")
	if _, payload, qos, _, ok := testMQTTTryReadPublishV5(ra, 5*time.Second); !ok || qos != 2 || string(payload) != "contended" {
		t.Fatalf("shared QoS 2 delivery: ok=%v qos=%d payload=%q", ok, qos, payload)
	}
	// Wait for the assignment to reach its settled pre-delivery state, so the
	// rewind below is not racing the broker's own source-ack transition.
	subject, _ := b.waitForClaim(t, "materialized and source-acked", func(c *mqttQ2AffClaim) bool {
		return c.Phase == mqttQ2AffMaterialized && c.SourceAcked
	})

	// Put the assignment back into the state that exists between materializing the
	// copy and the group ack committing, then destroy the consumer in that window.
	// The copy goes with it (interest retention). Retried against the record's
	// latest sequence: this is a CAS, and it is competing with the broker.
	var rewound mqttQ2AffClaim
	var rewErr error
	for attempt := 0; attempt < 5; attempt++ {
		claim, seq, err := b.asm.loadQ2AffClaim(subject)
		if err != nil || claim == nil {
			t.Fatalf("loading the claim: %v (claim=%v)", err, claim)
		}
		rewound = *claim
		rewound.SourceAcked = false
		if _, rewErr = b.asm.storeQ2AffClaimCAS(subject, seq, &rewound); rewErr == nil {
			break
		}
		if !isQoS2CASRejection(rewErr) {
			t.Fatalf("rewinding the claim: %v", rewErr)
		}
	}
	if rewErr != nil {
		t.Fatalf("rewinding the claim: %v", rewErr)
	}
	ca.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		gone := sess.disconnectedAt != 0
		sess.mu.Unlock()
		if gone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := b.jsa.deleteConsumer(mqttOutStreamName, durable, false); err != nil {
		t.Fatalf("deleting the affine consumer: %v", err)
	}
	if state, _ := b.asm.loadQ2AffCopy(rewound.CopySubject); state != mqttQ2AffCopyAbsent {
		t.Fatalf("the copy should have gone with its consumer, state=%v", state)
	}

	// The contender's reaper runs. It may prove the SOURCE side (that is its job),
	// but it must not touch the affine side.
	b.asm.reapQ2AffClaim(b.s, subject)

	after, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("reloading the claim: %v", err)
	}
	if after == nil {
		t.Fatal("a contender reaped an unproven QoS 2 delivery's assignment; the owner can no longer record it as lost, and the evidence is gone")
	}
	if after.AffineDone {
		t.Fatal("a contender concluded the affine side was complete from an absent copy it could not account for")
	}

	// Only the owner's own resume can tell "acked away" from "dropped with the
	// consumer", and it records the loss.
	cr, _ := b.reconnect(t, "contA", 300)
	defer cr.Close()
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cl, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil {
			t.Fatalf("loading the claim: %v", err)
		}
		if cl == nil {
			t.Fatal("the assignment was reaped instead of being recorded as lost")
		}
		if cl.Lost {
			if cl.AffineDone {
				t.Fatal("a claim recorded both as lost and as a completed delivery")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	cl, _, _ := b.asm.loadQ2AffClaim(subject)
	t.Fatalf("the owner's resume never recorded the loss: %+v", cl)
}

// A completed assignment that was never reaped must be reaped by recovery. The
// reap is the LAST step of the delivery completion, so a broker that stops after
// the copy's ack commits — or whose ack response is merely lost — leaves a
// finished claim behind that nothing else would ever revisit: the copy is gone so
// it cannot redeliver, and the source is already acked so no contender looks at
// it. Skipping AffineDone claims in recovery leaked those forever.
func TestQ2AffineRecoveryReapsCompletedButUnreapedClaims(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "unreaped")
	ca, _ := b.member(t, "unreapA", 300)
	b.bind(t, "unreapA")

	sess := testMQTTGetClient(t, b.s, "unreapA").mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	idHash := getHash("unreapA")
	groupDur := b.groupDurable(t)

	// Disconnect, keeping the session (and so the affine durable, and so the
	// stream's interest in its subject prefix) alive. A connected client would
	// consume the copy the second subtest stores — correctly, through the gate's
	// terminal branch — and there would be nothing live left to assert about.
	ca.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		gone := sess.disconnectedAt != 0
		sess.mu.Unlock()
		if gone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Run("copy gone: the claim is deleted", func(t *testing.T) {
		subject := mqttQ2AffClaimSubject(groupDur, 5150)
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, &mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: 5150,
			ClaimHash:   mqttQ2AffClaimHash(groupDur, 5150),
			Nonce:       testQ2AffFixtureNonce,
			CopySubject: mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, 5150), testQ2AffFixtureNonce),
			AffDurable:  "$MQTT_Q2AFF_unreaped",
			Phase:       mqttQ2AffMaterialized, SourceAcked: true, AffineDone: true,
		}); err != nil {
			t.Fatalf("writing the claim: %v", err)
		}
		b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)
		got, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil {
			t.Fatalf("loading the claim: %v", err)
		}
		if got != nil {
			t.Fatalf("a completed assignment was left behind forever: %+v", got)
		}
	})

	t.Run("source ack committed but never recorded: the claim is deleted", func(t *testing.T) {
		// The ordinary crash window on the SOURCE side: the group ack commits (which
		// removes the message from the interest-retained stream) and the CAS that
		// records SourceAcked then fails. The group consumer is still very much
		// alive, so the group-wide proof does not apply — only the per-message one
		// does, and without it the claim would be retained forever.
		const absentSeq = uint64(999999)
		subject := mqttQ2AffClaimSubject(groupDur, absentSeq)
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, &mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: absentSeq,
			ClaimHash:   mqttQ2AffClaimHash(groupDur, absentSeq),
			Nonce:       testQ2AffFixtureNonce,
			CopySubject: mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, absentSeq), testQ2AffFixtureNonce),
			AffDurable:  "$MQTT_Q2AFF_unreaped",
			Phase:       mqttQ2AffMaterialized, AffineDone: true,
		}); err != nil {
			t.Fatalf("writing the claim: %v", err)
		}
		if _, err := b.jsa.lookupConsumer(mqttStreamName, groupDur); err != nil {
			t.Fatalf("this subtest needs a LIVE group consumer, but %q is not there: %v", groupDur, err)
		}
		b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)
		got, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil {
			t.Fatalf("loading the claim: %v", err)
		}
		if got != nil {
			t.Fatalf("a completed assignment whose source message is authoritatively gone was retained forever: %+v", got)
		}
	})

	t.Run("copy still live: the claim is retained", func(t *testing.T) {
		// The other side of the same rule: AffineDone is recorded BEFORE the copy is
		// acked, so a live copy under a completed claim is the crash window between
		// those two steps. Deleting the claim there would strand the copy behind the
		// delivery gate.
		subject := mqttQ2AffClaimSubject(groupDur, 5151)
		copySubject := mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, 5151), testQ2AffFixtureNonce)
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, &mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: 5151,
			ClaimHash: mqttQ2AffClaimHash(groupDur, 5151), Nonce: testQ2AffFixtureNonce,
			CopySubject: copySubject,
			AffDurable:  "$MQTT_Q2AFF_unreaped",
			Phase:       mqttQ2AffMaterialized, SourceAcked: true, AffineDone: true,
		}); err != nil {
			t.Fatalf("writing the claim: %v", err)
		}
		// A real message on the claim's copy subject: the session's affine consumer
		// holds interest on the whole generation prefix, so it is retained.
		env := &mqttQ2AffineEnvelope{
			natsSubject: "unreaped.topic", msg: []byte("still here"),
			claimSubject: subject, gen: gen, msgID: mqttQ2AffMsgId(groupDur, 5151, testQ2AffFixtureNonce),
		}
		msg, hdrLen := mqttNewQ2AffineRecord(env)
		if _, err := b.asm.storeQ2AffCopy(copySubject, hdrLen, msg); err != nil {
			t.Fatalf("storing the copy: %v", err)
		}
		if state, _ := b.asm.loadQ2AffCopy(copySubject); state != mqttQ2AffCopyPresent {
			t.Fatalf("the copy was not retained; this subtest cannot test what it claims (state=%v)", state)
		}

		b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)
		got, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil {
			t.Fatalf("loading the claim: %v", err)
		}
		if got == nil {
			t.Fatal("a claim whose copy is still live was reaped; the copy is now stranded behind the delivery gate")
		}
	})
}

// The continuity decision must be readable from the DURABLE state alone, by a
// broker that observed nothing. The dangerous sequence is: the consumer is
// destroyed, some broker notices and recreates it (empty), and then a DIFFERENT
// broker — or the same one after a restart — reconciles. Anything that depended
// on "I saw the consumer disappear" is gone by then, and the recreated consumer
// looks perfectly intact.
//
// This test recreates the consumer before reconciling and reconciles through a
// path that has seen nothing at all, so it fails against any per-broker
// continuity signal.
func TestQ2AffineLossSurvivesConsumerRecreateAndHandoff(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "handoff")
	ca, ra := b.member(t, "hoA", 300)
	defer ca.Close()
	b.bind(t, "hoA")

	sess := testMQTTGetClient(t, b.s, "hoA").mqtt.sess
	sess.mu.Lock()
	gen, cfg := sess.q2AffGen, sess.q2AffConsumer
	sess.mu.Unlock()
	if gen == _EMPTY_ || cfg == nil {
		t.Fatal("the affine path was never provisioned")
	}
	idHash := getHash("hoA")
	_, _, durable := mqttQ2AffineNames(idHash, gen)

	// A real copy, delivered but not PUBREC'd, so it is still on $MQTT_out.
	b.publish(t, 1, "handoffloss")
	if _, payload, qos, _, ok := testMQTTTryReadPublishV5(ra, 5*time.Second); !ok || qos != 2 || string(payload) != "handoffloss" {
		t.Fatalf("shared QoS 2 delivery: ok=%v qos=%d payload=%q", ok, qos, payload)
	}
	subject, _ := b.waitForClaim(t, "materialized", func(c *mqttQ2AffClaim) bool { return c.Phase == mqttQ2AffMaterialized })

	// The consumer is destroyed — taking its pending copy with it — and then
	// recreated, exactly as a broker that noticed would do before it went away.
	if _, err := b.jsa.deleteConsumer(mqttOutStreamName, durable, false); err != nil {
		t.Fatalf("deleting the affine consumer: %v", err)
	}
	if _, err := b.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttOutStreamName, Config: *cfg}); err != nil {
		t.Fatalf("recreating the affine consumer: %v", err)
	}
	if state, _ := b.asm.loadQ2AffCopy(mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(b.groupDurable(t), 1), testQ2AffFixtureNonce)); state == mqttQ2AffCopyPresent {
		t.Fatal("the copy should have gone with its consumer")
	}

	// Reconcile as a broker that has observed nothing: the consumer is present and
	// looks healthy, and no in-memory flag from anywhere is consulted.
	b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)

	got, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got == nil {
		t.Fatal("the assignment for an unproven QoS 2 delivery was reaped after the consumer was recreated")
	}
	if got.AffineDone {
		t.Fatal("a recreated consumer made a lost delivery look completed; the continuity decision is not durable")
	}
	if !got.Lost {
		t.Fatalf("the loss was not recorded: %+v", got)
	}
}

// A claim already recorded as Lost must never be revisited into AffineDone by a
// later resume — by then the consumer has been recreated and looks intact, so the
// ordinary inference would fire and leave a record claiming both that the delivery
// was proven and that it could not be proven.
func TestQ2AffineLostClaimIsNotRevisited(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "lostonce")
	ca, _ := b.member(t, "lostoA", 300)
	defer ca.Close()
	b.bind(t, "lostoA")

	sess := testMQTTGetClient(t, b.s, "lostoA").mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	idHash := getHash("lostoA")
	groupDur := b.groupDurable(t)
	subject := mqttQ2AffClaimSubject(groupDur, 4711)
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, &mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: 4711,
		ClaimHash:   mqttQ2AffClaimHash(groupDur, 4711),
		Nonce:       testQ2AffFixtureNonce,
		CopySubject: mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, 4711), testQ2AffFixtureNonce),
		AffDurable:  "$MQTT_Q2AFF_lostonce",
		Phase:       mqttQ2AffMaterialized, SourceAcked: true, Lost: true,
	}); err != nil {
		t.Fatalf("writing the claim: %v", err)
	}

	// A resume with an intact consumer: the ordinary path would infer completion
	// from the absent copy.
	b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)

	got, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got == nil {
		t.Fatal("a lost assignment was reaped")
	}
	if got.AffineDone {
		t.Fatal("a lost assignment was revisited and marked as a completed delivery")
	}
	if !got.Lost {
		t.Fatal("the lost marker was dropped")
	}
}

// A SUBSCRIBE whose shared QoS 2 filter fails PER-FILTER (rather than rolling the
// whole packet back) must not leave the affine path behind either. Provisioning
// runs in the first loop, before any filter is known to succeed, so a failed
// membership persist would otherwise keep a consumer, a generation and a
// MaxAckPending charge for a subscription that does not exist.
func TestQ2AffinePerFilterFailureLeavesNoProvisioning(t *testing.T) {
	b := testQ2AffSetup(t, "pff")
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pff", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	b.bind(t, "pff")

	sess := testMQTTGetClient(t, b.s, "pff").mqtt.sess
	sess.mu.Lock()
	before := sess.tmaxack
	sess.mu.Unlock()

	// Save #1 is the affine provisioning's own gating write (must succeed); #2 is
	// the shared group's early membership persist, whose failure is reported as a
	// per-filter SUBACK failure and leaves the packet otherwise intact.
	var count int
	restore := setMQTTTestSessSaveHook(func(ps *mqttSession) error {
		if ps.id != "pff" {
			return nil
		}
		count++
		if count == 2 {
			return fmt.Errorf("membership persist refused by the test")
		}
		return nil
	})
	defer restore()

	codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/pffg/pff/topic", opts: 2}})
	restore()
	if len(codes) != 1 || codes[0] < mqttSubAckFailure {
		t.Fatalf("expected a per-filter SUBACK failure, got %v", codes)
	}

	testQ2AffAssertUnwound(t, b, sess, getHash("pff"), before,
		"a SUBSCRIBE whose only shared QoS 2 filter failed left affine provisioning behind")
}

// Per-(Topic, QoS) ordering is a preserved invariant (docs/PORTING.md); shared
// subscriptions do not waive it. Two same-topic QoS 2 messages claimed in source
// order must reach the owning client in that order, because they are stored in
// order onto one generation-scoped subject and consumed in stream order by one
// consumer.
//
// Ordering is asserted over FIRST arrivals: a duplicate redelivery of a message
// already seen is correct protocol behavior (the short AckWait here makes it
// likely) and says nothing about ordering.
func TestQ2AffineOrderingPreserved(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "order")
	ca, ra := b.member(t, "orderA", 300)
	defer ca.Close()
	b.bind(t, "orderA")

	const n = 8
	for i := 0; i < n; i++ {
		b.publish(t, uint16(i+1), fmt.Sprintf("m%02d", i))
	}

	seen := map[string]bool{}
	var order []string
	deadline := time.Now().Add(20 * time.Second)
	for len(order) < n && time.Now().Before(deadline) {
		_, payload, qos, pi, ok := testMQTTTryReadPublishV5(ra, 2*time.Second)
		if !ok {
			continue
		}
		if qos != 2 {
			t.Fatalf("a shared QoS 2 delivery arrived at QoS %d", qos)
		}
		if !seen[string(payload)] {
			seen[string(payload)] = true
			order = append(order, string(payload))
		}
		// PUBREC without waiting for the PUBREL: several exchanges are in flight,
		// so their PUBRELs interleave with the remaining PUBLISHes.
		testMQTTSendPIPacket(mqttPacketPubRec, t, ca, pi)
	}
	if len(order) != n {
		t.Fatalf("received %d of %d messages: %v", len(order), n, order)
	}
	for i, got := range order {
		if want := fmt.Sprintf("m%02d", i); got != want {
			t.Fatalf("message %d is %q, want %q: same-topic QoS 2 ordering was not preserved (%v)", i, got, want, order)
		}
	}
}

// One source message delivered to TWO shared groups produces TWO independent
// assignments and TWO copies. If their deduplication keys collided, the second
// group's copy would be silently dropped and its member would never receive the
// message at all.
func TestQ2AffineTwoGroupsGetIndependentAssignments(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "two")
	c1, r1 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "grp1", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c1.Close()
	testMQTTReadConnAckV5(t, r1)
	testMQTTSubV5(t, c1, r1, 1, []mqttV5SubFilter{{topic: "$share/one/" + b.topic, opts: 2}})
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "grp2", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	testMQTTSubV5(t, c2, r2, 1, []mqttV5SubFilter{{topic: "$share/two/" + b.topic, opts: 2}})
	testMQTTFlush(t, c2, nil, r2)
	b.bind(t, "grp1")

	b.publish(t, 1, "both")

	conns := []net.Conn{c1, c2}
	for i, r := range []*mqttReader{r1, r2} {
		_, payload, qos, pi, ok := testMQTTTryReadPublishV5(r, 5*time.Second)
		if !ok {
			t.Fatalf("group %d did not receive the message: its copy was dropped", i+1)
		}
		if qos != 2 || string(payload) != "both" {
			t.Fatalf("group %d got payload %q at QoS %d", i+1, payload, qos)
		}
		b.completeExchange(t, conns[i], r, pi)
	}
	// Two groups, two assignments — and both reaped once complete.
	b.waitForNoClaims(t)
}

// The claim record must round-trip through JSON with every field it arbitrates
// on. A field that silently fails to persist would be read back as its zero
// value, and for the three proof fields the zero value is "not proven" — which
// stalls, rather than reopens, an assignment. Still worth pinning: a dropped
// Winner or Gen WOULD reopen it.
func TestQ2AffClaimRecordRoundTrip(t *testing.T) {
	t.Parallel()
	in := &mqttQ2AffClaim{
		Winner: "abc", Gen: mqttNewQ2AffGen(), GroupDurable: "$MQTT_SHARED_x", SourceSeq: 99,
		ClaimHash: "0123456789abcdef", Nonce: testQ2AffFixtureNonce,
		CopySubject: "$MQTT.out.q2.abc.g.h", AffDurable: "$MQTT_Q2AFF_abc_g",
		SubID: 7, Retained: true, Phase: mqttQ2AffMaterialized,
		SourceAcked: true, AffineDone: true, Discarded: true,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := &mqttQ2AffClaim{}
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if *out != *in {
		t.Fatalf("round trip changed the claim:\n got %+v\nwant %+v", out, in)
	}
}

// An ambiguous AFFINE PROVISIONING save is settled FORWARD — the resolver makes
// the attempt's exact bytes durable — so the record it commits names a delivery
// path the packet never established a membership for: shared membership is
// persisted in a LATER loop, and every save is refused while the session is
// unresolved. Nothing in the packet can reclaim it (the unused-path unwind's own
// confirming save is refused for the same reason) and no sweep collects it either —
// F-036 course E removed that route entirely. The settlement therefore owns
// the reclaim, and runs it under the resolver's token.
//
// What licenses that removal is the FAILED PACKET, not a claim that the
// generation is unreachable: a second shared QoS 2 filter would re-enter
// provisioning with the consumer present, skip the save and open the readiness
// gate while the session is still unresolved, so a move could transiently claim
// under this generation before the membership rollback. This single-filter
// fixture does not exercise that, and does not pin it either.
//
// Measured before the fix: the settled record carried Q2AffGen/Q2Aff, the
// $MQTT_Q2AFF_* durable survived on $MQTT_out, and tmaxack held the affine
// consumer's 1024-slot charge with no membership anywhere.
//
// Mutation M1: drop the retainOnLanded registration in the saveMaybeLanded arm of
// ensureQ2AffineConsumerSubscription and exactly that state comes back.
func TestQ2AffineAmbiguousProvisioningIsUnwoundAtSettlement(t *testing.T) {
	a := testQ2AffArmAmbiguousProvisioning(t, "ambprov", 300)
	a.settle(t)
	b, sess, c, r, idHash, before := a.b, a.sess, a.c, a.r, a.idHash, a.before

	// The record a restart would read no longer names the path...
	ps := testQ2AffLoadRecord(t, sess)
	if ps.Q2AffGen != _EMPTY_ || ps.Q2Aff != nil {
		t.Fatalf("the settled record still names the affine path: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}
	// ...while everything the settlement was NOT about is untouched: the reclaim
	// removes a path, not the session.
	if qos, ok := ps.Subs["ambprov.plain"]; !ok || qos != 2 {
		t.Fatalf("the pre-packet subscription did not survive the reclaim: %v", ps.Subs)
	}
	// ...and the durable, the session's view of it and its share of the
	// MaxAckPending budget are all gone.
	testQ2AffAssertPathReclaimed(t, b, sess, idHash, before,
		"an ambiguous affine provisioning save left its path behind after settling")

	// The session is left REUSABLE, which is the difference between reclaiming a
	// path and breaking one: a later shared QoS 2 SUBSCRIBE provisions from scratch
	// and succeeds.
	codes := testMQTTSubV5(t, c, r, 3, []mqttV5SubFilter{{topic: "$share/ambprovg2/" + b.topic, opts: 2}})
	if len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("a shared QoS 2 SUBSCRIBE after the reclaim failed: %v", codes)
	}
	if durs := b.affineConsumers(t, idHash); len(durs) != 1 {
		t.Fatalf("the re-subscribe did not provision a fresh affine path, got %v", durs)
	}
}

// testQ2AffAssertPathReclaimed waits for every trace of the affine path to be
// gone. Unlike testQ2AffAssertUnwound the budget check is EXACT: this fixture
// provisions the PUBREL consumer before the packet under test, so the affine
// delta must return to zero and any surviving slack would be the leak.
func testQ2AffAssertPathReclaimed(t *testing.T, b *testQ2AffBroker, sess *mqttSession, idHash string, before int, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var gen string
	var consumer *ConsumerConfig
	var ready bool
	var after int
	var durables []string
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		gen, consumer, ready, after = sess.q2AffGen, sess.q2AffConsumer, sess.q2AffReady, sess.tmaxack
		sess.mu.Unlock()
		durables = b.affineConsumers(t, idHash)
		if gen == _EMPTY_ && consumer == nil && !ready && len(durables) == 0 && after == before {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: gen=%q consumer=%v ready=%v durables=%v tmaxack=%d (pre-packet %d)",
		what, gen, consumer, ready, durables, after, before)
}

// testQ2AffAmbiguousProvisioning drives a shared QoS 2 SUBSCRIBE whose AFFINE
// PROVISIONING save is made ambiguous, confirms that it is that save and no
// other that went ambiguous, and settles it. It returns the broker, the session,
// the still-open connection, the id hash and the pre-packet MaxAckPending charge,
// so a caller asserts only about the affine delta.
type testQ2AffAmbig struct {
	b      *testQ2AffBroker
	sess   *mqttSession
	c      net.Conn
	r      *mqttReader
	idHash string
	before int
}

// prepare, when given, runs on the fresh broker BEFORE any client connects. It
// exists for the peer manager the takeover fixtures need: every account session
// manager sweeps expiries, orphaned consumers and orphaned QoS 2 state once at
// creation, so a peer built later would sweep an account that already holds the
// half-provisioned path under test.
func testQ2AffArmAmbiguousProvisioning(t *testing.T, name string, expiry uint32, prepare ...func(*testQ2AffBroker)) *testQ2AffAmbig {
	t.Helper()
	b := testQ2AffSetup(t, name)
	for _, p := range prepare {
		p(b)
	}
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   name,
		cleanStart: true,
		props:      mqttV5ConnPropsSessionExpiry(expiry),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	testMQTTReadConnAckV5(t, r)

	// A confirmed PLAIN QoS 2 subscription first. It settles the record and
	// provisions the PUBREL consumer, so the packet under test creates exactly one
	// resource — the affine path — and every charge assertion is an affine delta
	// rather than a bare total. No sweep judges an affine consumer at all since F-036
	// course E, and finishQ2AffineUnwind refunds only the affine increment, so a PUBREL
	// consumer created INSIDE the ambiguous packet would be a second unaccounted resource.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: name + "/plain", opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("expected SUBACK [2] for the plain QoS 2 filter, got %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, name)
	sess := testMQTTGetClient(t, b.s, name).mqtt.sess
	idHash := getHash(name)
	sess.mu.Lock()
	before := sess.tmaxack
	sess.mu.Unlock()
	if durs := b.affineConsumers(t, idHash); len(durs) != 0 {
		t.Fatalf("a plain QoS 2 subscription provisioned an affine path: %v", durs)
	}

	// The provisioning save is issued and never reaches the stream. Suppression
	// stays on until the ambiguity is REAL, or the packet under test never fails
	// at all.
	mqttTestSessStoreSuppressV.Store(true)
	t.Cleanup(func() { mqttTestSessStoreSuppressV.Store(false) })
	codes := testMQTTSubV5(t, c, r, 2, []mqttV5SubFilter{{topic: "$share/" + name + "g/" + b.topic, opts: 2}})
	if len(codes) != 1 || codes[0] < mqttSubAckFailure {
		t.Fatalf("expected a per-filter SUBACK failure, got %v", codes)
	}
	checkFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return fmt.Errorf("the packet's write has not gone ambiguous yet")
		}
		return nil
	})

	// THE ARMING CONFIRMATION. A prior probe of this item never reached it, so the
	// leak was filed NOT MEASURED. Membership is written in a LATER loop whose save
	// is refused while the session is unresolved, so an installed generation with no
	// shared membership is what proves the PROVISIONING save is the pending write.
	sess.mu.Lock()
	gen, shared := sess.q2AffGen, len(sess.sharedSubs)
	sess.mu.Unlock()
	if gen == _EMPTY_ {
		t.Fatal("the ambiguity did not arm on the affine provisioning save: no generation is installed")
	}
	if shared != 0 {
		t.Fatalf("%d shared membership(s) survived the packet, so the pending write is not the provisioning save", shared)
	}
	// The crash residual, pinned before the settlement can reclaim it: the path is
	// provisioned and the record may already name it.
	if durs := b.affineConsumers(t, idHash); len(durs) != 1 {
		t.Fatalf("expected exactly one affine consumer in the unresolved window, got %v", durs)
	}
	return &testQ2AffAmbig{b: b, sess: sess, c: c, r: r, idHash: idHash, before: before}
}

// settle lifts the suppression and drives the settlement to completion. The
// connection stays up (the filter failed per-filter) but its own writes are
// refused while the session is unresolved, so the periodic pass is what settles
// it — driven here rather than waited on.
func (a *testQ2AffAmbig) settle(t *testing.T) {
	t.Helper()
	mqttTestSessStoreSuppressV.Store(false)
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		a.b.asm.reconcileResidentRecordState(a.b.s)
		if a.sess.isSeqUncertain() {
			return fmt.Errorf("still unresolved")
		}
		return nil
	})
}

// settleUntilContinuation drives the settlement until the reclaim continuation's
// own save has actually run. The arms below all end with the session in a state
// that is NOT "resolved", so waiting on resolution would either hang or pass for
// the wrong reason; the continuation having run is the real event.
func (a *testQ2AffAmbig) settleUntilContinuation(t *testing.T, fired *atomic.Bool) {
	t.Helper()
	mqttTestSessStoreSuppressV.Store(false)
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		a.b.asm.reconcileResidentRecordState(a.b.s)
		if !fired.Load() {
			return fmt.Errorf("the reclaim continuation's own save has not run yet")
		}
		return nil
	})
}

// testQ2AffOnContinuationSave installs a one-shot PRE-STORE hook that fires on the
// FIRST save() the settlement performs for this session — the reclaim
// continuation's own write. There is nothing else it can fire on: the resolver
// re-attempts the original bytes through storeFramedSessionMsg, which never calls
// save().
//
// The post-store result hook is deliberately NOT the seam here. It rewrites an
// outcome AFTER the store landed, so a test using it to model a refused cleanup
// could be asserting against a record that is in fact durable.
func testQ2AffOnContinuationSave(t *testing.T, id string, act func() error) *atomic.Bool {
	t.Helper()
	fired := &atomic.Bool{}
	restore := setMQTTTestSessSaveHook(func(sp *mqttSession) error {
		if sp.id != id || !fired.CompareAndSwap(false, true) {
			return nil
		}
		return act()
	})
	t.Cleanup(restore)
	return fired
}

// The OTHER packet shape, and the one the roadmap bullet's stated mechanism was
// written about. With a filter that succeeds alongside the shared QoS 2 one,
// `needUpdate` is true, so the packet's final `sess.update` really is attempted —
// and refused, because the provisioning save left the session unresolved. That
// routes through `rollbackSubscribePacket` instead of the unused-path branch, and
// its own confirming save is refused for the same reason, so it re-attaches the
// path it detached. The end state is therefore identical to the single-filter
// shape, which is what makes the reclaim's single owner correct for both.
//
// Mutation M1: drop the retainOnLanded registration and this shape leaks the path
// exactly as the other one does.
func TestQ2AffineAmbiguousProvisioningIsUnwoundAfterPacketRollback(t *testing.T) {
	b := testQ2AffSetup(t, "ambroll")
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   "ambroll",
		cleanStart: true,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ambroll/plain", opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("expected SUBACK [2] for the plain QoS 2 filter, got %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, "ambroll")
	sess := testMQTTGetClient(t, b.s, "ambroll").mqtt.sess
	idHash := getHash("ambroll")
	sess.mu.Lock()
	before := sess.tmaxack
	sess.mu.Unlock()

	// Two filters: the shared QoS 2 one provisions (and its save is suppressed) in
	// the FIRST loop, and the plain QoS 1 one gives the final update something to
	// persist — without it `needUpdate` is false and this is the other branch.
	mqttTestSessStoreSuppressV.Store(true)
	t.Cleanup(func() { mqttTestSessStoreSuppressV.Store(false) })
	vh := newMQTTWriter(0)
	vh.WriteUint16(2)
	vh.WriteVarInt(0)
	vh.WriteBytes([]byte("$share/ambrollg/" + b.topic))
	vh.WriteByte(2)
	vh.WriteBytes([]byte("ambroll/second"))
	vh.WriteByte(1)
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("writing SUBSCRIBE: %v", err)
	}
	// A rolled-back packet sends no SUBACK; it closes the connection.
	var buf [16]byte
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if n, err := c.Read(buf[:]); err == nil && n > 0 && buf[0]&mqttPacketMask == mqttPacketSubAck {
		t.Fatal("expected no SUBACK: a packet whose final update is refused is rolled back")
	}
	checkFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return fmt.Errorf("the packet's write has not gone ambiguous yet")
		}
		return nil
	})
	sess.mu.Lock()
	gen, shared := sess.q2AffGen, len(sess.sharedSubs)
	sess.mu.Unlock()
	if gen == _EMPTY_ {
		t.Fatal("the ambiguity did not arm on the affine provisioning save: no generation is installed")
	}
	if shared != 0 {
		t.Fatalf("%d shared membership(s) survived the rollback, so the pending write is not the provisioning save", shared)
	}
	// The rollback re-attached the path it detached, because its own save was
	// refused too. This is the state the roadmap bullet measured.
	if durs := b.affineConsumers(t, idHash); len(durs) != 1 {
		t.Fatalf("expected exactly one affine consumer after the rollback, got %v", durs)
	}
	// The budget is asserted EXACTLY at both ends, and both are needed. This packet
	// charges two consumers after the snapshot — the affine one and the plain QoS 1
	// one that a refused rollback deliberately keeps alive, because nothing may be
	// released while the record cannot be confirmed. A lower bound here would be
	// satisfied by the plain consumer alone, so a missing affine charge and a
	// missing refund would cancel out and pass.
	sess.mu.Lock()
	rolledBack := sess.tmaxack
	sess.mu.Unlock()
	if want := before + 2*mqttDefaultMaxAckPending; rolledBack != want {
		t.Fatalf("after the rollback tmaxack is %d, want %d (pre-packet %d, plus the affine and the plain QoS 1 consumer)", rolledBack, want, before)
	}

	mqttTestSessStoreSuppressV.Store(false)
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		b.asm.reconcileResidentRecordState(b.s)
		if sess.isSeqUncertain() {
			return fmt.Errorf("still unresolved")
		}
		return nil
	})

	testQ2AffAssertPathReclaimed(t, b, sess, idHash, before+mqttDefaultMaxAckPending,
		"a rolled-back packet's ambiguous affine provisioning left its path behind after settling")
	ps := testQ2AffLoadRecord(t, sess)
	if ps.Q2AffGen != _EMPTY_ || ps.Q2Aff != nil {
		t.Fatalf("the settled record still names the affine path: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}
	// The rollback stands: the pre-packet subscription survives and the packet's
	// own plain filter does not.
	if qos, ok := ps.Subs["ambroll.plain"]; !ok || qos != 2 {
		t.Fatalf("the pre-packet subscription did not survive the rollback: %v", ps.Subs)
	}
	if _, ok := ps.Subs["ambroll.second"]; ok {
		t.Fatalf("the rolled-back packet's own filter became durable: %v", ps.Subs)
	}
}

// A live shared QoS 2 membership outranks the reclaim, and the guard that says
// so is a FAIL-CLOSED BACKSTOP over the session's IN-MEMORY state rather than a
// reader of its record — which is why this drives it there directly.
//
// No record shape can produce its precondition any more. A persisted qos-2
// membership with no affine generation is refused at CONNECT, in the expiry sweep
// and at expiry itself (mqttValidateRecordAffinePair), and the live SUBSCRIBE that
// MINTS a generation cannot produce one either: when its provisioning save goes
// ambiguous the filter is marked failed, and sess.update skips a failed filter, so
// no membership is ever installed. The fixture asserts exactly that (no shared
// membership survived the packet), which is what leaves the map empty for this
// test to seed. The guard is kept because what it prevents is irreversible —
// assignments discarded out from under a delivery already started, the
// redistribution [MQTT-4.8.2-5] forbids.
//
// The fixture deliberately leaves NO outstanding affine claim. With one, the later
// q2AffineGenerationOwnsClaims arm retains the path anyway and the assertions below
// would hold against a broken guard.
//
// Mutation M-F: drop the hasSharedQoS2Sub() guard in
// unwindProvisionedQ2AffineOnLanded and the settlement reclaims a path a live
// membership owns.
func TestQ2AffineReclaimYieldsToALiveSharedMembership(t *testing.T) {
	a := testQ2AffArmAmbiguousProvisioning(t, "ambmem", 300)

	// The membership is keyed the way the broker keys it, so hasSharedQoS2Sub sees
	// what a real shared QoS 2 subscription would have left behind.
	subj, err := mqttTopicToNATSPubSubject([]byte(a.b.topic))
	if err != nil {
		t.Fatalf("subject conversion: %v", err)
	}
	key := (&mqttFilter{shared: true, group: "ambmemg", filter: string(subj)}).sessionKey()
	if _, _, ok := mqttParseSharedKey(key); !ok {
		t.Fatalf("the seeded shared key %q does not parse back as a shared membership", key)
	}
	a.sess.mu.Lock()
	if a.sess.sharedSubs == nil {
		a.sess.sharedSubs = make(map[string]mqttSharedMembership)
	}
	a.sess.sharedSubs[key] = testSharedMembership(key, 2)
	a.sess.mu.Unlock()

	a.settle(t)

	// The guard returns before prepareQ2AffineUnwind, so nothing was detached:
	// memory still describes the path...
	a.sess.mu.Lock()
	gen, consumer := a.sess.q2AffGen, a.sess.q2AffConsumer
	a.sess.mu.Unlock()
	if gen == _EMPTY_ || consumer == nil {
		t.Fatalf("the settlement reclaimed an affine path a live shared QoS 2 membership owns: gen=%q consumer=%v", gen, consumer)
	}
	// ...the durable is still on the stream...
	durs := a.b.affineConsumers(t, a.idHash)
	if len(durs) != 1 || durs[0] != consumer.Durable {
		t.Fatalf("the affine durable did not survive: memory names %q, the stream carries %v", consumer.Durable, durs)
	}
	// ...and the settled record still names it, so a restart finds the same path.
	ps := testQ2AffLoadRecord(t, a.sess)
	if ps.Q2AffGen != gen || ps.Q2Aff == nil {
		t.Fatalf("the settled record stopped naming the retained path: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}

	// RETAINED means USABLE, which only a delivery proves. The re-subscribe is a
	// real one: the packet under test failed per-filter, so no live queue
	// subscription was ever built. What this pins is that the retained path is the
	// SAME one — reused rather than re-provisioned — and that a started QoS 2
	// delivery completes over it.
	if codes := testMQTTSubV5(t, a.c, a.r, 3, []mqttV5SubFilter{{topic: "$share/ambmemg/" + a.b.topic, opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("re-subscribing to the retained shared filter failed: %v", codes)
	}
	testMQTTFlush(t, a.c, nil, a.r)
	if after := a.b.affineConsumers(t, a.idHash); len(after) != 1 || after[0] != durs[0] {
		t.Fatalf("the re-subscribe did not REUSE the retained path: had %v, now %v", durs, after)
	}
	a.sess.mu.Lock()
	reusedGen := a.sess.q2AffGen
	a.sess.mu.Unlock()
	if reusedGen != gen {
		t.Fatalf("the re-subscribe re-provisioned under a new generation: %q, want the retained %q", reusedGen, gen)
	}

	a.b.publish(t, 1, "retained")
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(a.r, 10*time.Second)
	if !ok {
		t.Fatal("no delivery arrived over the retained affine path")
	}
	if qos != 2 || string(payload) != "retained" {
		t.Fatalf("unexpected delivery over the retained path: payload=%q qos=%d", payload, qos)
	}
	a.b.completeExchange(t, a.c, a.r, pi)
}

// The un-minted resume shape: a record naming a GENERATION but no consumer
// config, with no membership and an outstanding assignment. Its resume reaches
// the very same provisioning save with existing == nil, so only `minted` tells
// the two apart — and this generation may owe a delivery already started to it.
// Registering the continuation here would discard that assignment and delete a
// consumer a started delivery is waiting on, which is precisely the redistribution
// [MQTT-4.8.2-5] forbids.
//
// Mutation M8: drop the `minted` restriction on the registration and the claim is
// discarded and the durable deleted.
func TestQ2AffineAmbiguousResumeKeepsAnUnmintedGeneration(t *testing.T) {
	b := testQ2AffSetup(t, "ambunmint")
	// A real member, so the group durable the claim names genuinely exists: an
	// assignment whose group consumer is gone is legitimately un-re-offerable, and
	// the reap would then be entitled to remove it for a reason this test is not
	// about.
	ca, _ := b.member(t, "ambunmintA", 300)
	defer ca.Close()
	b.bind(t, "ambunmintA")

	idHash, gen := getHash("ambunmint"), mqttNewQ2AffGen()
	_, _, durable := mqttQ2AffineNames(idHash, gen)
	groupDur := b.groupDurable(t)
	sourceSeq := testQ2AffLiveSourceSeq(t, b, "ambunmint/keep")
	claimSubject := mqttQ2AffClaimSubject(groupDur, sourceSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, sourceSeq)
	_, seq, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim slot: %v", err)
	}
	if _, err := b.asm.storeQ2AffClaimCAS(claimSubject, seq, &mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: sourceSeq,
		ClaimHash:   claimHash,
		Nonce:       testQ2AffFixtureNonce,
		CopySubject: mqttQ2AffCopySubject(idHash, gen, claimHash, testQ2AffFixtureNonce),
		AffDurable:  durable,
		Phase:       mqttQ2AffMaterialized,
	}); err != nil {
		t.Fatalf("seeding the outstanding assignment: %v", err)
	}
	testQ2AffSeedRecord(t, b, "ambunmint", func(ps *mqttPersistedSession) {
		// A generation with no consumer config and no membership. Restore copies the
		// two affine fields independently, so this is a record shape the broker can
		// really produce.
		ps.Q2AffGen = gen
	})

	armed := testQ2AffSuppressProvisioningSave(t, "ambunmint")
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   "ambunmint",
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	sess := testMQTTGetClient(t, b.s, "ambunmint").mqtt.sess
	testQ2AffSettleAmbiguousProvisioning(t, b, sess, armed)

	// Nothing of the un-minted generation may have been touched.
	if got, _, err := b.asm.loadQ2AffClaim(claimSubject); err != nil {
		t.Fatalf("re-loading the assignment: %v", err)
	} else if got == nil {
		t.Fatal("the settlement reaped an assignment owed by a generation it did not mint")
	} else if got.Discarded {
		t.Fatal("the settlement discarded an assignment a started delivery owes; a minted-only reclaim must never reach an un-minted generation")
	}
	if durs := b.affineConsumers(t, idHash); len(durs) != 1 || durs[0] != durable {
		t.Fatalf("expected the un-minted generation's durable %q to survive, got %v", durable, durs)
	}
	ps := testQ2AffLoadRecord(t, sess)
	if ps.Q2AffGen != gen {
		t.Fatalf("the settled record changed the un-minted generation: got %q want %q", ps.Q2AffGen, gen)
	}
}

// The reclaim's own save can fail DEFINITELY, and then nothing was stored: the
// settled record still names the path, the consumer is still alive, and memory
// must go back to describing it. Detaching memory anyway would orphan a live
// consumer the record still names — the exact shape the commit order exists to
// prevent — and the path silently reverts to being the teardown residual.
//
// Mutation M6: delete the consumer BEFORE the record save in
// unwindProvisionedQ2AffineOnLanded and this test finds a record naming a
// consumer that no longer exists.
//
// Mutation M9: skip restoreQ2AffineUnwind in the definite arm and memory stops
// describing a consumer that is still alive and still record-referenced.
func TestQ2AffineReclaimSaveRefusedKeepsThePath(t *testing.T) {
	a := testQ2AffArmAmbiguousProvisioning(t, "ambdef", 300)
	fired := testQ2AffOnContinuationSave(t, "ambdef", func() error {
		return fmt.Errorf("the reclaim's cleanup save is refused by the test")
	})
	a.settleUntilContinuation(t, fired)

	// Nothing was stored, so the record is still the provisioning bytes.
	ps := testQ2AffLoadRecord(t, a.sess)
	if ps.Q2AffGen == _EMPTY_ || ps.Q2Aff == nil {
		t.Fatalf("a refused cleanup save must leave the record naming the path: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}
	// ...and the consumer it names must still exist. A record naming a deleted
	// consumer is unrecoverable: a restart would restore a path whose copies can
	// never be delivered.
	durs := a.b.affineConsumers(t, a.idHash)
	if len(durs) != 1 || durs[0] != ps.Q2Aff.Durable {
		t.Fatalf("the record names %q but the stream carries %v", ps.Q2Aff.Durable, durs)
	}
	// ...and memory must describe it again, or the next save writes a record that
	// silently drops a consumer nothing will then delete.
	a.sess.mu.Lock()
	gen, consumer, ready := a.sess.q2AffGen, a.sess.q2AffConsumer, a.sess.q2AffReady
	a.sess.mu.Unlock()
	if gen != ps.Q2AffGen || consumer == nil {
		t.Fatalf("memory stopped describing a live, record-referenced path: gen=%q consumer=%v", gen, consumer)
	}
	if ready {
		t.Fatal("readiness must stay closed: the record naming this path was never re-confirmed")
	}
}

// The reclaim's own save can be AMBIGUOUS in turn. That is supported rather than
// exceptional: the detached view is kept (it matches what may have landed), the
// delete is withheld for want of proof and registered as a fresh continuation,
// and the resolution reports the session unresolved so the periodic pass keeps
// working on it. The NEXT settlement is what deletes the durable.
func TestQ2AffineReclaimSaveAmbiguousDefersTheDelete(t *testing.T) {
	a := testQ2AffArmAmbiguousProvisioning(t, "ambamb", 300)
	fired := testQ2AffOnContinuationSave(t, "ambamb", func() error {
		// Ambiguous the same way the original was: issued, never landed.
		mqttTestSessStoreSuppressV.Store(true)
		return nil
	})
	a.settleUntilContinuation(t, fired)

	// Unresolved AGAIN, and nothing deleted while it is: the resources stay live
	// until the record is proven to no longer name them.
	if !a.sess.isSeqUncertain() {
		t.Fatal("an ambiguous cleanup save must re-arm uncertainty, or the delete below is authorized by nothing")
	}
	if durs := a.b.affineConsumers(t, a.idHash); len(durs) != 1 {
		t.Fatalf("the durable was deleted while the cleanup write was unresolved, got %v", durs)
	}

	// The next settlement proves the cleanup's bytes durable and runs the delete.
	a.settle(t)
	testQ2AffAssertPathReclaimed(t, a.b, a.sess, a.idHash, a.before,
		"a re-ambiguous cleanup save never completed its deferred delete")
	if ps := testQ2AffLoadRecord(t, a.sess); ps.Q2AffGen != _EMPTY_ || ps.Q2Aff != nil {
		t.Fatalf("the settled cleanup record still names the path: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}
}

// A cleanup save can be REJECTED rather than merely refused, and a rejection is a
// different fact: it says the subject MOVED. The reclaim's definite arm restores
// the path — nothing of ours may be deleted on the strength of a write that never
// landed — and the rejection settlement takes it from there. After a definite
// rejection every moved record is FOREIGN irrespective of its bytes, so the
// session is disposed and the consumer is left ALIVE for whoever the winning
// record makes its owner. This is the clustered displacement-racing-the-cleanup shape
// reduced to the arm that actually decides it.
//
// The AFFINE claim's owner is decided by the winning record, which is a durable
// arbitration this test may rely on. The SESSION's successor is not: a moved record shows
// only that the session is no longer ours (F-084), which is all the disposal acts on.
func TestQ2AffineReclaimSaveRejectedIsSettledAsForeign(t *testing.T) {
	var peer *mqttAccountSessionManager
	a := testQ2AffArmAmbiguousProvisioning(t, "ambrej", 300, func(b *testQ2AffBroker) {
		peer = testQ2AffPeerASM(t, b, "node2")
	})
	dropped := testQ2AffDropPersistObservations(t, a.b, a.idHash)

	fired := testQ2AffOnContinuationSave(t, "ambrej", func() error {
		// A real winner moves the subject out from under the cleanup save, which then
		// reaches JetStream and is genuinely rejected. The rejection has to be the
		// stream's: a synthetic one would arm a marker the stream never justified.
		testQ2AffForeignStore(t, peer, "ambrej")
		return nil
	})
	a.settleUntilContinuation(t, fired)

	if dropped.Load() == 0 {
		t.Fatal("the persist observation was never dropped, so processSessionPersist could have done the eviction below on its own")
	}
	a.sess.mu.Lock()
	gen, consumer, pending := a.sess.q2AffGen, a.sess.q2AffConsumer, a.sess.recordRejectionPendingLocked()
	a.sess.mu.Unlock()
	if gen == _EMPTY_ || consumer == nil {
		t.Fatalf("a rejected cleanup save must leave memory describing the still-live path: gen=%q consumer=%v", gen, consumer)
	}
	if !pending {
		t.Fatal("a definite CAS rejection must arm the record-rejection marker")
	}

	// A BOUND marker is deliberately not probed by the periodic pass, so end the
	// connection and drive the unbound route. The close's own save is CAS-rejected
	// too — the subject has genuinely moved — so it cannot advance sess.seq and
	// settle the marker out from under the probe.
	a.c.Close()
	checkFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		a.sess.mu.Lock()
		bound := a.sess.c != nil
		a.sess.mu.Unlock()
		if bound {
			return fmt.Errorf("the session has not been unbound yet")
		}
		return nil
	})
	a.sess.mu.Lock()
	stillPending := a.sess.recordRejectionPendingLocked()
	a.sess.mu.Unlock()
	if !stillPending {
		t.Fatal("the marker was settled before the pass could probe it, so everything below would prove nothing")
	}

	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		a.b.asm.reconcileResidentRecordState(a.b.s)
		if testMQTTResidentSessionOrNil(a.b.asm, "ambrej") != nil {
			return fmt.Errorf("a probe that found a foreign record has not disposed the session yet")
		}
		return nil
	})
	// Never deleted by us. The winning record decides the consumer's owner, and this
	// one omits the affine fields — which used to make it the orphan sweep's work and now
	// makes it a stated leak (F-036 course E), but either way not this path's to delete.
	if durs := a.b.affineConsumers(t, a.idHash); len(durs) != 1 {
		t.Fatalf("the disposal deleted an affine consumer it does not own, got %v", durs)
	}
}

// THE RACE THE ORDERING EXISTS FOR. A remote broker writes the session record and
// PRESERVES the affine generation — the q2aff-aware write, which carries the
// affine fields through — so the generation that record authorizes is the one this
// reclaim was about to remove, and an assignment under it is a delivery already started to
// the winner. The reclaim's cleanup CAS loses to that winner, and because the
// unwind was prepared WITHOUT disposing, nothing of the winner's was touched:
// the claim is still live and the restore is complete rather than partial.
//
// Mutation: prepare the unwind with disposeNow true (the order the other two
// unwind sites still use) and the claim comes back Discarded — a started QoS 2
// delivery terminated for a session that is still live, which the monotonic
// discard makes unrecoverable.
func TestQ2AffineReclaimRejectedByAGenerationPreservingWinnerKeepsItsClaim(t *testing.T) {
	var (
		peer      *mqttAccountSessionManager
		groupDur  string
		sourceSeq uint64
	)
	// The peer manager and the live source message are both built BEFORE the
	// ambiguity is armed: every account manager sweeps once at creation, and the
	// store suppressor is global, so a client connecting afterwards could not
	// persist its own session.
	a := testQ2AffArmAmbiguousProvisioning(t, "ambrace", 300, func(b *testQ2AffBroker) {
		peer = testQ2AffPeerASM(t, b, "node2")
		cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambrace-src", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)
		b.bind(t, "ambrace-src")
		groupDur = b.groupDurable(t)
		sourceSeq = testQ2AffLiveSourceSeq(t, b, "ambrace/keep")
	})
	dropped := testQ2AffDropPersistObservations(t, a.b, a.idHash)

	a.sess.mu.Lock()
	gen := a.sess.q2AffGen
	a.sess.mu.Unlock()
	_, _, durable := mqttQ2AffineNames(a.idHash, gen)

	// A real assignment under THIS generation — the started delivery the winner
	// owns. Written to $MQTT_qos2assign, which the session-record suppressor does
	// not touch.
	claimSubject := mqttQ2AffClaimSubject(groupDur, sourceSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, sourceSeq)

	fired := testQ2AffOnContinuationSave(t, "ambrace", func() error {
		// BOTH the takeover and the winner's assignment appear at the cleanup save —
		// after the reclaim's started-delivery guard has already scanned and found
		// nothing. That is the window this test is about: the guard cannot see an
		// assignment a remote winner creates after it runs, so what protects that
		// delivery is the CAS-first ordering, not the guard.
		_, seq, err := a.b.asm.loadQ2AffClaim(claimSubject)
		if err != nil {
			return fmt.Errorf("loading the claim slot: %w", err)
		}
		if _, err := a.b.asm.storeQ2AffClaimCAS(claimSubject, seq, &mqttQ2AffClaim{
			Winner: a.idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: sourceSeq,
			ClaimHash:   claimHash,
			Nonce:       testQ2AffFixtureNonce,
			CopySubject: mqttQ2AffCopySubject(a.idHash, gen, claimHash, testQ2AffFixtureNonce),
			AffDurable:  durable,
			Phase:       mqttQ2AffMaterialized,
		}); err != nil {
			return fmt.Errorf("seeding the winner's assignment: %w", err)
		}
		// The winner takes the record and KEEPS the affine fields, so the generation
		// stays authorized — it is the winner's now, not ours.
		testQ2AffForeignStoreWith(t, peer, "ambrace", func(ps *mqttPersistedSession) {
			ps.Q2AffGen = gen
			ps.Q2Aff = &ConsumerConfig{Durable: durable}
		})
		return nil
	})
	a.settleUntilContinuation(t, fired)

	if dropped.Load() == 0 {
		t.Fatal("the persist observation was never dropped, so processSessionPersist could have evicted the session before the cleanup save ran")
	}
	// The cleanup CAS was rejected by the winner, so the assignment it owns must be
	// untouched. A discard here is not recoverable: it is monotonic, and the
	// restore below cannot put it back.
	got, _, err := a.b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("re-loading the winner's assignment: %v", err)
	}
	if got == nil {
		t.Fatal("the reclaim reaped an assignment owned by the record that beat it")
	}
	if got.Discarded {
		t.Fatal("a cleanup save that LOST its CAS discarded the winner's started QoS 2 delivery; ownership must be arbitrated before anything irreversible")
	}
	if got.Gen != gen {
		t.Fatalf("the seeded assignment changed generation: got %q want %q", got.Gen, gen)
	}
	// ...and the restore is complete: memory describes the live path again, and its
	// consumer — which the winning record still names — was never deleted.
	a.sess.mu.Lock()
	memGen, consumer := a.sess.q2AffGen, a.sess.q2AffConsumer
	a.sess.mu.Unlock()
	if memGen != gen || consumer == nil {
		t.Fatalf("the rejected cleanup did not restore the path: gen=%q consumer=%v", memGen, consumer)
	}
	if durs := a.b.affineConsumers(t, a.idHash); len(durs) != 1 || durs[0] != durable {
		t.Fatalf("the reclaim deleted a consumer the winning record still names, got %v", durs)
	}
}

// A STARTED DELIVERY outranks the reclaim, because a failed packet is not an ended
// session. `Discarded` means the owning session ended, and [MQTT-4.8.2-5] requires a
// live session to complete a delivery already started to it — so a generation that
// still owns an assignment keeps its path, and session teardown becomes the event
// that ends it.
//
// The assignment here is SEEDED rather than raced. The shape that produces one in
// production is a packet with a SECOND shared QoS 2 filter: it re-enters
// provisioning with the consumer already present, skips the save and opens the
// readiness gate while the session is still unresolved, so a move can claim under
// the generation before the membership rollback. Racing that is not deterministic;
// what the guard turns on is only whether the generation owns a live assignment at
// settlement, which a seeded real claim establishes exactly.
//
// Mutation M12: drop the q2AffineGenerationOwnsClaims guard and the settlement
// discards an assignment whose session is still live and still owes its delivery.
func TestQ2AffineReclaimKeepsAPathThatStillOwesADelivery(t *testing.T) {
	var (
		groupDur  string
		sourceSeq uint64
	)
	a := testQ2AffArmAmbiguousProvisioning(t, "ambowes", 300, func(b *testQ2AffBroker) {
		cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambowes-src", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)
		b.bind(t, "ambowes-src")
		groupDur = b.groupDurable(t)
		sourceSeq = testQ2AffLiveSourceSeq(t, b, "ambowes/keep")
	})
	a.sess.mu.Lock()
	gen := a.sess.q2AffGen
	a.sess.mu.Unlock()
	_, _, durable := mqttQ2AffineNames(a.idHash, gen)

	claimSubject := mqttQ2AffClaimSubject(groupDur, sourceSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, sourceSeq)
	_, seq, err := a.b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim slot: %v", err)
	}
	if _, err := a.b.asm.storeQ2AffClaimCAS(claimSubject, seq, &mqttQ2AffClaim{
		Winner: a.idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: sourceSeq,
		ClaimHash:   claimHash,
		Nonce:       testQ2AffFixtureNonce,
		CopySubject: mqttQ2AffCopySubject(a.idHash, gen, claimHash, testQ2AffFixtureNonce),
		AffDurable:  durable,
		Phase:       mqttQ2AffMaterialized,
	}); err != nil {
		t.Fatalf("seeding the started delivery's assignment: %v", err)
	}

	a.settle(t)

	// The assignment is untouched: only the end of the session may discard it.
	got, _, err := a.b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("re-loading the assignment: %v", err)
	}
	if got == nil {
		t.Fatal("the reclaim reaped an assignment its live session still owes")
	}
	if got.Discarded {
		t.Fatal("the reclaim discarded a started delivery on a FAILED PACKET; Discarded means the owning session ended, and this session is still live")
	}
	// ...and so is the path that delivery is owed over.
	if ps := testQ2AffLoadRecord(t, a.sess); ps.Q2AffGen != gen || ps.Q2Aff == nil {
		t.Fatalf("the record stopped naming a path a started delivery needs: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}
	if durs := a.b.affineConsumers(t, a.idHash); len(durs) != 1 || durs[0] != durable {
		t.Fatalf("the reclaim deleted the consumer a started delivery is waiting on, got %v", durs)
	}
	a.sess.mu.Lock()
	memGen, consumer := a.sess.q2AffGen, a.sess.q2AffConsumer
	a.sess.mu.Unlock()
	if memGen != gen || consumer == nil {
		t.Fatalf("the retained path was not restored in memory: gen=%q consumer=%v", memGen, consumer)
	}
}

// A TERMINAL assignment is not an outstanding obligation, and must not block the
// reclaim. `Lost` is the case that matters: it is deliberately never reaped —
// it is the only durable evidence of a QoS 2 delivery the
// broker accepted and could not prove — so a guard that treated it as owed would retain this path for the
// life of the session, which for a never-expiring one is the broker's. That is
// precisely the leak the reclaim exists to close, so the guard turns on
// `terminal()` rather than on `Discarded` alone.
//
// Mutation M14: narrow the guard's predicate back to `!claim.Discarded` and the
// path, its consumer and its MaxAckPending charge survive the settlement.
func TestQ2AffineReclaimIsNotBlockedByATerminalClaim(t *testing.T) {
	var (
		groupDur  string
		sourceSeq uint64
	)
	a := testQ2AffArmAmbiguousProvisioning(t, "ambterm", 300, func(b *testQ2AffBroker) {
		cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambterm-src", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)
		b.bind(t, "ambterm-src")
		groupDur = b.groupDurable(t)
		sourceSeq = testQ2AffLiveSourceSeq(t, b, "ambterm/keep")
	})
	a.sess.mu.Lock()
	gen := a.sess.q2AffGen
	a.sess.mu.Unlock()
	_, _, durable := mqttQ2AffineNames(a.idHash, gen)

	claimSubject := mqttQ2AffClaimSubject(groupDur, sourceSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, sourceSeq)
	_, seq, err := a.b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim slot: %v", err)
	}
	if _, err := a.b.asm.storeQ2AffClaimCAS(claimSubject, seq, &mqttQ2AffClaim{
		Winner: a.idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: sourceSeq,
		ClaimHash:   claimHash,
		Nonce:       testQ2AffFixtureNonce,
		CopySubject: mqttQ2AffCopySubject(a.idHash, gen, claimHash, testQ2AffFixtureNonce),
		AffDurable:  durable,
		Phase:       mqttQ2AffMaterialized,
		Lost:        true,
	}); err != nil {
		t.Fatalf("seeding the terminal assignment: %v", err)
	}

	a.settle(t)

	testQ2AffAssertPathReclaimed(t, a.b, a.sess, a.idHash, a.before,
		"a terminal assignment blocked the reclaim, retaining the path for the life of the session")
	if ps := testQ2AffLoadRecord(t, a.sess); ps.Q2AffGen != _EMPTY_ || ps.Q2Aff != nil {
		t.Fatalf("the settled record still names the path: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}
	// The loss record itself survives: nothing here may erase the evidence.
	if got, _, err := a.b.asm.loadQ2AffClaim(claimSubject); err != nil {
		t.Fatalf("re-loading the terminal assignment: %v", err)
	} else if got == nil {
		t.Fatal("the reclaim reaped a Lost assignment; it is the only durable evidence that an accepted message could not be proven delivered")
	} else if !got.Lost {
		t.Fatal("the reclaim erased the Lost marker")
	}
}

// Disposal happens after the cleanup record is confirmed, so it can fail with the
// record already saying the path is gone. Only the CONSUMER DELETE waits in that
// case — the scan that terminates assignments is account-wide and fails closed, so
// deleting an interest-retained consumer whose claims might be outstanding would
// take their copies with it. A consumer no record names used to be the orphan sweep's;
// since F-036 course E nothing collects it, so it is RETAINED and stated as a leak.
// Everything LOCAL still converges: the delivery
// subscription goes and the MaxAckPending charge comes back exactly, because
// nothing else can ever reach them once the record stops naming the path — no sweep
// could refund another broker's budget in any case.
//
// Mutation: return from dispose() without releaseQ2AffineUnwindLocally and the
// charge is stranded for the life of the session, which is a slice of
// mqttMaxAckTotalLimit lost per occurrence.
func TestQ2AffineReclaimDiscardFailureStillReleasesTheLocalHalf(t *testing.T) {
	var groupDur string
	a := testQ2AffArmAmbiguousProvisioning(t, "ambdisc", 300, func(b *testQ2AffBroker) {
		cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambdisc-src", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
		defer cs.Close()
		testMQTTReadConnAckV5(t, rs)
		b.bind(t, "ambdisc-src")
		groupDur = b.groupDurable(t)
	})
	a.sess.mu.Lock()
	gen := a.sess.q2AffGen
	a.sess.mu.Unlock()
	_, deliver, durable := mqttQ2AffineNames(a.idHash, gen)

	// A claim record that cannot be decoded makes the account-wide scan — and so
	// discardQ2AffClaimsFor — FAIL CLOSED. This is the production failure, not a
	// seam: the scan must abort rather than conclude anything about a record it
	// cannot read. Written to $MQTT_qos2assign, which the session-record suppressor
	// does not touch.
	//
	// Written AT the cleanup save, so it lands after the reclaim's started-delivery
	// guard has scanned (that guard fails closed on the same record, and would
	// otherwise retain the path and never reach the disposal this test is about)
	// and before the disposal that follows the confirmed CAS.
	badClaim := mqttQ2AffClaimSubject(groupDur, 4242)
	fired := testQ2AffOnContinuationSave(t, "ambdisc", func() error {
		if _, err := a.b.jsa.storeMsg(badClaim, 0, []byte("this is not a claim record")); err != nil {
			return fmt.Errorf("writing the undecodable claim: %w", err)
		}
		return nil
	})

	a.settle(t)

	if !fired.Load() {
		t.Fatal("the undecodable claim was never written, so no disposal failure was exercised")
	}
	if err := a.b.asm.discardQ2AffClaimsFor(a.idHash, gen); err == nil {
		t.Fatal("the fixture's undecodable claim does not make the discard fail, so nothing below is exercised")
	}

	// The record no longer names the path, so the reclaim's CAS did land.
	if ps := testQ2AffLoadRecord(t, a.sess); ps.Q2AffGen != _EMPTY_ || ps.Q2Aff != nil {
		t.Fatalf("the cleanup record did not land, so the disposal failure below was never reached: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}
	// The local half converged EXACTLY: no subscription, and the affine charge back.
	a.sess.mu.Lock()
	after, memGen, consumer := a.sess.tmaxack, a.sess.q2AffGen, a.sess.q2AffConsumer
	a.sess.mu.Unlock()
	if after != a.before {
		t.Fatalf("the discard failure stranded the MaxAckPending charge: tmaxack=%d, want the pre-packet %d", after, a.before)
	}
	if memGen != _EMPTY_ || consumer != nil {
		t.Fatalf("the session still describes a path its record no longer names: gen=%q consumer=%v", memGen, consumer)
	}
	if testQ2AffHasSubscription(t, a.b, "ambdisc", deliver) {
		t.Fatalf("the affine delivery subscription on %q outlived the path", deliver)
	}
	// The consumer is deliberately NOT deleted: its claims could not be proven
	// terminated. Two things this does NOT assert. It does not assert that the
	// orphan sweep then reclaims it — it never does now (F-036 course E removed that
	// route), and even before that the same undecodable record aborted the sweep's
	// affine inventory, so the consumer is retained either way. And it does not assert that
	// NO assignment was discarded: the discard streams, mutating each matching
	// claim inside the scan, so claims reached before the malformed record are
	// discarded while the rest are not. This fixture seeds no owned assignments, so
	// it pins neither half of that. Both residuals are stated in
	// docs/CONFORMANCE.md and filed in docs/ROADMAP.md.
	if durs := a.b.affineConsumers(t, a.idHash); len(durs) != 1 || durs[0] != durable {
		t.Fatalf("the reclaim deleted a consumer whose assignments it could not terminate, got %v", durs)
	}
}

// testQ2AffHasSubscription reports whether a client still holds a subscription on
// subject. Asserting on the CLIENT's subscription table is the point: an unwind
// that forgets the delivery subscription leaves a live callback for a path the
// record no longer names.
func testQ2AffHasSubscription(t *testing.T, b *testQ2AffBroker, clientID, subject string) bool {
	t.Helper()
	cl := testMQTTGetClient(t, b.s, clientID)
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for _, sub := range cl.subs {
		if string(sub.subject) == subject {
			return true
		}
	}
	return false
}

// 10164 at the BASE sequence is contention, never a verdict: a write for the
// subject is in process and the record may still be exactly where we left it. The
// marker is KEPT with memory restored to naming the path, and the settlement's
// probe re-attempts the write from CURRENT memory. What this pins is that
// composition — restore-then-probe — because the probe persists what the restore
// put back, so the record it lands must still name the live path.
//
// Mutation M9: skip restoreQ2AffineUnwind in the reclaim's definite arm and the
// probe marshals a record WITHOUT the affine path while the consumer lives on,
// referenced by nothing.
func TestQ2AffineReclaimContentionProbeRepersistsThePath(t *testing.T) {
	// mqttSessionNeverExpire is load-bearing, not incidental: at a finite expiry the
	// close below persists its disconnect time, and a LANDED save advances sess.seq,
	// which invalidates the marker's recordRejectedSeq == sess.seq predicate. The
	// probe would then never run and this test would pass on the close's own save.
	a := testQ2AffArmAmbiguousProvisioning(t, "ambcont", mqttSessionNeverExpire)

	// A production-shaped contention rejection, assembled from two seams and
	// truthfully synthetic: the store is suppressed (so nothing is written and the
	// record genuinely still holds the settled provisioning bytes) and its error is
	// rewritten to the constant-sequence rejection the stream would return.
	rewrote := &atomic.Bool{}
	restoreResult := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sp.id != "ambcont" || err == nil || !mqttTestSessStoreSuppressV.Load() {
			return resp, err
		}
		mqttTestSessStoreSuppressV.Store(false)
		rewrote.Store(true)
		return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
	})
	defer restoreResult()
	fired := testQ2AffOnContinuationSave(t, "ambcont", func() error {
		mqttTestSessStoreSuppressV.Store(true)
		return nil
	})
	a.settleUntilContinuation(t, fired)
	if !rewrote.Load() {
		t.Fatal("the contention rejection was never injected, so the cleanup save failed for some other reason")
	}

	a.sess.mu.Lock()
	gen, consumer, pending := a.sess.q2AffGen, a.sess.q2AffConsumer, a.sess.recordRejectionPendingLocked()
	a.sess.mu.Unlock()
	if gen == _EMPTY_ || consumer == nil {
		t.Fatalf("contention must leave memory describing the still-live path: gen=%q consumer=%v", gen, consumer)
	}
	if !pending {
		t.Fatal("a definite CAS rejection must arm the record-rejection marker")
	}

	a.c.Close()
	checkFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		a.sess.mu.Lock()
		bound := a.sess.c != nil
		a.sess.mu.Unlock()
		if bound {
			return fmt.Errorf("the session has not been unbound yet")
		}
		return nil
	})
	a.sess.mu.Lock()
	stillPending := a.sess.recordRejectionPendingLocked()
	a.sess.mu.Unlock()
	if !stillPending {
		t.Fatal("the marker was settled before the pass could probe it, so everything below would prove nothing")
	}

	a.b.asm.reconcileResidentRecordState(a.b.s)

	// The probe landed and persisted CURRENT memory, which the restore put back.
	ps := testQ2AffLoadRecord(t, a.sess)
	if ps.Q2AffGen != gen || ps.Q2Aff == nil {
		t.Fatalf("the probe persisted a record that drops a live affine path: Q2AffGen=%q (want %q) Q2Aff=%+v", ps.Q2AffGen, gen, ps.Q2Aff)
	}
	durs := a.b.affineConsumers(t, a.idHash)
	if len(durs) != 1 || durs[0] != ps.Q2Aff.Durable {
		t.Fatalf("the record names %q but the stream carries %v", ps.Q2Aff.Durable, durs)
	}
}

// The residual this item does NOT close: a crash between the ambiguous
// provisioning save and its settlement. The record names the path, so a restart
// restores it, the resume re-establishes it WITHOUT recharging, and session
// teardown reclaims it — which is the documented cost, one durable consumer until
// the session ends.
//
// The ambiguity is armed in the LANDED-RESPONSE-LOST form on purpose: a write that
// was never issued leaves no record for the restart to read, and every assertion
// below would then hold vacuously.
//
// Mutation M4: skip the q2AffDur deletion in the teardown and the durable outlives
// the session it belongs to.
func TestQ2AffineAmbiguousProvisioningSurvivesRestartUntilTeardown(t *testing.T) {
	b := testQ2AffSetup(t, "ambrestart")
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   "ambrestart",
		cleanStart: true,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ambrestart/plain", opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("expected SUBACK [2] for the plain QoS 2 filter, got %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, "ambrestart")
	idHash := getHash("ambrestart")

	// The provisioning save LANDS and its response is lost; the readback that would
	// classify it is unavailable, so the session is still unresolved at the restart.
	suppressRead := &atomic.Bool{}
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressRead.Load() {
			return fmt.Errorf("test: readback unavailable")
		}
		return nil
	})
	suppressRead.Store(true)
	lost := &atomic.Bool{}
	restoreResult := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sp.id != "ambrestart" || err != nil {
			return resp, err
		}
		sp.mu.Lock()
		provisioning := sp.q2AffGen != _EMPTY_ && sp.q2AffConsumer != nil && !sp.q2AffReady
		sp.mu.Unlock()
		if provisioning && lost.CompareAndSwap(false, true) {
			return nil, fmt.Errorf("test: session record response lost")
		}
		return resp, err
	})
	codes := testMQTTSubV5(t, c, r, 2, []mqttV5SubFilter{{topic: "$share/ambrestartg/" + b.topic, opts: 2}})
	if len(codes) != 1 || codes[0] < mqttSubAckFailure {
		t.Fatalf("expected a per-filter SUBACK failure, got %v", codes)
	}
	sess := testMQTTGetClient(t, b.s, "ambrestart").mqtt.sess
	checkFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return fmt.Errorf("the provisioning write has not gone ambiguous yet")
		}
		return nil
	})
	if !lost.Load() {
		t.Fatal("the response-lost seam never fired, so no record was written for the restart to read")
	}
	restoreResult()
	if durs := b.affineConsumers(t, idHash); len(durs) != 1 {
		t.Fatalf("expected the affine durable in the residual window, got %v", durs)
	}

	// The readback stays UNAVAILABLE until this broker is down. Restoring it here
	// would let the old broker's periodic pass settle the write and run the reclaim,
	// and the restart would then be reading a record the residual window never
	// produced.
	//
	// Only the MQTT layer cycles; JetStream keeps the record and the consumer.
	dir := b.o.StoreDir
	c.Close()
	b.s.Shutdown()
	suppressRead.Store(false)
	restoreRead()
	b.o.Port = -1
	b.o.MQTT.Port = -1
	b.o.StoreDir = dir
	b.s = testMQTTRunServer(t, b.o)
	t.Cleanup(func() { testMQTTShutdownServer(b.s) })

	// A throwaway client, for two reasons. It gives this side of the restart a JSA to
	// read the record through; and the record must be QUIESCENT before the resume
	// under test, because the shut-down broker settles its own uncertainty on the way
	// out and discharges the deferred disconnect it parked — a record write that
	// lands after the new CONNECT has already read the subject would CAS-reject the
	// accept save and close the connection.
	cq, rq := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ambrestart-reader", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, rq)
	b.bind(t, "ambrestart-reader")
	ps := testQ2AffAwaitStableRecord(t, b, idHash)
	cq.Close()

	// THE ANTI-VACUITY CHECK: the record the restart reads really does name the path,
	// so the resume below has something to re-establish. Without it a write that
	// never landed would make every assertion here hold for nothing.
	if ps.Q2AffGen == _EMPTY_ || ps.Q2Aff == nil {
		t.Fatalf("the ambiguous write did not land, so this fixture cannot test a restart: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   "ambrestart",
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	b.bind(t, "ambrestart")
	sess2 := testMQTTGetClient(t, b.s, "ambrestart").mqtt.sess

	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		sess2.mu.Lock()
		gen := sess2.q2AffGen
		sess2.mu.Unlock()
		if gen == _EMPTY_ {
			return fmt.Errorf("the resumed session has not re-established the affine path yet")
		}
		return nil
	})
	sess2.mu.Lock()
	gen2, tmaxack := sess2.q2AffGen, sess2.tmaxack
	sess2.mu.Unlock()
	if durs := b.affineConsumers(t, idHash); len(durs) != 1 {
		t.Fatalf("the resume did not re-establish exactly the restored path, got %v", durs)
	}
	// A durable that came OUT of the record was never charged against this process's
	// budget, so nothing the resume re-establishes may bill it: charging here would
	// grow tmaxack once per restart until shared QoS 2 subscribes started failing.
	_, _, durable := mqttQ2AffineNames(idHash, gen2)
	if tmaxack != 0 {
		t.Fatalf("the resume charged consumers restored from the record: tmaxack=%d, want 0", tmaxack)
	}

	// Session teardown is what finally reclaims it.
	if err := sess2.clear(true); err != nil {
		t.Fatalf("clearing the session: %v", err)
	}
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		if durs := b.affineConsumers(t, idHash); len(durs) != 0 {
			return fmt.Errorf("the teardown left %v behind (expected %q gone)", durs, durable)
		}
		return nil
	})
}

// A new CONNECT can be what settles the ambiguity, and then the reclaim runs from
// that resolution with the SUBSCRIBE's own connection long closed. The captured
// client is exactly the dead one, so this is the empirical check that the delete
// half of the unwind tolerates it: the assertion is that the reclaim COMPLETED,
// not that nothing crashed.
func TestQ2AffineReclaimRunsFromAReconnectResolution(t *testing.T) {
	a := testQ2AffArmAmbiguousProvisioning(t, "ambconn", 300)
	mqttTestSessStoreSuppressV.Store(false)

	// The connection that provisioned the path goes away entirely.
	a.c.Close()
	checkFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		a.sess.mu.Lock()
		bound := a.sess.c != nil
		a.sess.mu.Unlock()
		if bound {
			return fmt.Errorf("the session has not been unbound yet")
		}
		return nil
	})

	// The reconnect resolves the session, and the reclaim runs under ITS resolution.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   "ambconn",
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, a.b.o.MQTT.Host, a.b.o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)

	sess := testMQTTGetClient(t, a.b.s, "ambconn").mqtt.sess
	testQ2AffAssertPathReclaimed(t, a.b, sess, a.idHash, a.before,
		"a reclaim driven by a reconnect's resolution did not complete")
	if ps := testQ2AffLoadRecord(t, sess); ps.Q2AffGen != _EMPTY_ || ps.Q2Aff != nil {
		t.Fatalf("the record still names the path after the reclaim: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}
}

// A competing remote write during the unresolved window ends in a FOREIGN verdict, and a
// foreign verdict is not ours to act on: the record is not this broker's to write, the
// session is evicted, and the continuation never runs. The affine consumer must
// survive that: whether it is the winner's is decided by the winning record, never by us
// deleting it on the way out. (It is no longer "or the orphan sweep's" — F-036 course E
// removed that route, so an unclaimed consumer is retained rather than collected.)
//
// Mutation M7: delete the affine consumer at eviction in
// disposeForeignStateLocked and the surviving-consumer assertion fails.
func TestQ2AffineForeignDispositionLeavesTheConsumerAlone(t *testing.T) {
	var peer *mqttAccountSessionManager
	a := testQ2AffArmAmbiguousProvisioning(t, "ambforeign", 300, func(b *testQ2AffBroker) {
		peer = testQ2AffPeerASM(t, b, "node2")
	})
	dropped := testQ2AffDropPersistObservations(t, a.b, a.idHash)

	// Suppression off, so the resolver's readback is the thing that decides — and
	// what it finds is a record that is not ours, at a sequence past our base.
	mqttTestSessStoreSuppressV.Store(false)
	testQ2AffForeignStore(t, peer, "ambforeign")

	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		a.b.asm.reconcileResidentRecordState(a.b.s)
		if testMQTTResidentSessionOrNil(a.b.asm, "ambforeign") != nil {
			return fmt.Errorf("the displaced session has not been disposed yet")
		}
		return nil
	})
	if dropped.Load() == 0 {
		t.Fatal("the persist observation was never dropped, so processSessionPersist did the eviction and the resolver's foreign branch was never exercised")
	}
	if durs := a.b.affineConsumers(t, a.idHash); len(durs) != 1 {
		t.Fatalf("a foreign disposition deleted a consumer the winning record decides the owner of, got %v", durs)
	}
	// And the winner's record is untouched by us.
	ps := testQ2AffLoadRecord(t, a.sess)
	if ps.Q2AffGen != _EMPTY_ || ps.Q2Aff != nil {
		t.Fatalf("the foreign record was rewritten with our affine path: Q2AffGen=%q Q2Aff=%+v", ps.Q2AffGen, ps.Q2Aff)
	}
}

// testQ2AffPeerASM builds a SECOND account session manager over the same NATS
// connection. The peer identity is what makes a direct store look remote:
// processSessionPersist ignores a reply subject carrying its own JSA id before it
// consults anything else, so a store issued through the local manager could never
// exercise the remote-write path at all.
func testQ2AffPeerASM(t *testing.T, b *testQ2AffBroker, node string) *mqttAccountSessionManager {
	t.Helper()
	s := &Server{nrc: b.s.nrc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: node}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	t.Cleanup(func() { close(s.quitCh) })
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatalf("%s: getOrCreateMQTTAccountSessionManager: %v", node, err)
	}
	return asm
}

// testQ2AffForeignStore persists a record for id through a peer manager,
// unconditionally, which is what moves the $MQTT_sess subject out from under the
// local session. The record deliberately omits the affine fields: it stands for an
// older or simply different winner, and after a definite rejection the bytes make
// no difference to the verdict anyway.
func testQ2AffForeignStore(t *testing.T, peer *mqttAccountSessionManager, id string) {
	t.Helper()
	testQ2AffForeignStoreWith(t, peer, id, func(*mqttPersistedSession) {})
}

// testQ2AffForeignStoreWith is testQ2AffForeignStore with the winner's record
// content under the caller's control — for the remote write that PRESERVES the affine
// fields, which is the q2aff-aware branch and the one that leaves the generation
// authorized by somebody else's record.
func testQ2AffForeignStoreWith(t *testing.T, peer *mqttAccountSessionManager, id string, mutate func(*mqttPersistedSession)) {
	t.Helper()
	ps := &mqttPersistedSession{ID: id, Gen: mqttNewSessGen(), ExpiryInterval: 300}
	mutate(ps)
	body, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshalling the foreign record: %v", err)
	}
	if _, err := peer.jsa.storeSessionMsg(peer.domainTk, getHash(id), 0, body); err != nil {
		t.Fatalf("storing the foreign record for %q: %v", id, err)
	}
}

// testQ2AffDropPersistObservations makes this broker never learn of a remote
// persist for one client ID — the state a lost core-NATS notification leaves.
// Without it processSessionPersist evicts the session on its own, and every
// assertion about what the settlement did would hold for the wrong reason.
func testQ2AffDropPersistObservations(t *testing.T, b *testQ2AffBroker, idHash string) *atomic.Int64 {
	t.Helper()
	dropped := &atomic.Int64{}
	b.asm.mu.Lock()
	b.asm.testSessPersistDrop = func(h string) bool {
		if h != idHash {
			return false
		}
		dropped.Add(1)
		return true
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testSessPersistDrop = nil
		b.asm.mu.Unlock()
	})
	return dropped
}

// testQ2AffSettleAmbiguousProvisioning waits for the targeted provisioning save
// to have gone ambiguous, asserts the seam actually fired (an unarmed hook would
// leave every assertion below testing an ordinary success), lifts suppression and
// drives the periodic pass until the write is settled.
func testQ2AffSettleAmbiguousProvisioning(t *testing.T, b *testQ2AffBroker, sess *mqttSession, armed *atomic.Bool) {
	t.Helper()
	checkFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		if !sess.isSeqUncertain() {
			return fmt.Errorf("the provisioning write has not gone ambiguous yet")
		}
		return nil
	})
	if !armed.Load() {
		t.Fatal("the suppressor never fired on a provisioning save, so the ambiguous write is some other one")
	}
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	if gen == _EMPTY_ {
		t.Fatal("no affine generation is installed, so the pending write is not the provisioning save")
	}
	mqttTestSessStoreSuppressV.Store(false)
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		b.asm.reconcileResidentRecordState(b.s)
		if sess.isSeqUncertain() {
			return fmt.Errorf("still unresolved")
		}
		return nil
	})
}

// testQ2AffSuppressProvisioningSave makes the AFFINE PROVISIONING save of one
// session — and no other write — ambiguous, by arming the store suppressor from
// inside the PRE-STORE save hook at the moment that session first carries a
// provisioned but unpersisted affine path.
//
// Targeting matters on the CONNECT paths: the accept save runs first, and
// suppressing that one would make a different write ambiguous and never reach the
// arm under test. The returned flag reports whether the hook fired at all.
func testQ2AffSuppressProvisioningSave(t *testing.T, id string) *atomic.Bool {
	t.Helper()
	armed := &atomic.Bool{}
	restore := setMQTTTestSessSaveHook(func(sp *mqttSession) error {
		if sp.id != id {
			return nil
		}
		sp.mu.Lock()
		provisioning := sp.q2AffGen != _EMPTY_ && sp.q2AffConsumer != nil && !sp.q2AffReady
		sp.mu.Unlock()
		if provisioning && armed.CompareAndSwap(false, true) {
			mqttTestSessStoreSuppressV.Store(true)
		}
		return nil
	})
	t.Cleanup(func() {
		restore()
		mqttTestSessStoreSuppressV.Store(false)
	})
	return armed
}

// testQ2AffSeedRecord persists a synthetic session record for a client that has
// never connected. The generation and the expiry are mandatory rather than
// decorative: restore FAILS CLOSED on a record with no valid session generation,
// so "a record without the affine fields" describes those two fields only, never
// a partial schema.
func testQ2AffSeedRecord(t *testing.T, b *testQ2AffBroker, id string, mutate func(*mqttPersistedSession)) {
	t.Helper()
	ps := &mqttPersistedSession{
		ID:             id,
		Gen:            mqttNewSessGen(),
		ExpiryInterval: 300,
	}
	mutate(ps)
	body, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshalling the seeded record: %v", err)
	}
	if _, err := b.jsa.storeSessionMsg(b.asm.domainTk, getHash(id), 0, body); err != nil {
		t.Fatalf("seeding the session record for %q: %v", id, err)
	}
}

// testQ2AffAwaitStableRecord polls the record for one client hash until its
// sequence stops moving, and returns the settled contents. It exists for the
// restart fixture: the broker being replaced finishes its own record work
// asynchronously, and a CONNECT that reads the subject mid-write has its accept
// save CAS-rejected.
func testQ2AffAwaitStableRecord(t *testing.T, b *testQ2AffBroker, idHash string) *mqttPersistedSession {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + b.asm.domainTk + idHash
	var last uint64
	var stable int
	var ps *mqttPersistedSession
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		smsg, err := b.jsa.loadLastMsgFor(mqttSessStreamName, subject)
		if err != nil {
			t.Fatalf("loading the record for %q: %v", idHash, err)
		}
		if smsg.Sequence == last {
			if stable++; stable >= 3 {
				return ps
			}
		} else {
			last, stable = smsg.Sequence, 0
			ps = &mqttPersistedSession{}
			if err := json.Unmarshal(smsg.Data, ps); err != nil {
				t.Fatalf("unmarshalling the record for %q: %v", idHash, err)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the record for %q never stopped moving (last sequence %d)", idHash, last)
	return nil
}

// testQ2AffLoadRecord returns the session record as a restart would read it.
func testQ2AffLoadRecord(t *testing.T, sess *mqttSession) *mqttPersistedSession {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	smsg, err := sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil {
		t.Fatalf("loading the settled record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("unmarshalling the settled record: %v", err)
	}
	return ps
}

// TestQ2AffEveryWriteCarriesItsExpectation pins the claim F-033's audit rests on:
// EVERY write this protocol makes to $MQTT_qos2assign carries a per-subject
// expectation, the election is create-only, and every later transition CASes at a
// sequence that record actually held. The audit's clause (a)/(b) verdict is otherwise
// four line numbers in a dossier, which nothing would notice going stale; this makes
// it executable for this stream. Modelled on TestQoS2InEveryWriteCarriesItsExpectation.
//
// TWO oracles, because neither alone is sufficient and that is measured rather than
// assumed:
//
//   - The RAW EMITTED BYTES, through the process-global pre-store hook, which receives
//     the fully encoded message. This is the only oracle that can see a header the
//     encoder failed to write: the instance seam below observes the expectSeq ARGUMENT,
//     which is unchanged when storeQ2AffClaimCAS stops emitting it.
//   - The CALL-SITE ARGUMENTS, through the instance seam, delegating to the real
//     implementation so the flow stays real. This is the only oracle that can see a call
//     site passing the wrong base, which the raw bytes cannot distinguish from a
//     legitimate CAS at a nonzero sequence.
//
// The call-site oracle checks a CHAIN rather than a fixed value, and that is what makes
// it survive a real CAS rejection: the source ack and the affine completion are written
// by unordered paths, so a transition may legitimately retry and commit at whatever
// sequence became current. The invariant that holds regardless is that a later
// transition's base is a sequence this subject HAD ALREADY HELD WHEN THE CALL WAS MADE —
// never zero, and never a sequence produced later. The snapshot is taken per invocation
// for exactly that reason; validating against the final list would accept a base that did
// not yet exist. It deliberately does NOT require the base to be the most recent one: an
// older-but-real base is rejected by the server's own per-subject CAS, which is the
// mechanism under test rather than something this oracle should duplicate.
//
// Deliberately NOT parallel: mqttTestStoreMsgHookV is process-global, and testHookGate
// bounds a closure's lifetime rather than arbitrating two installers.
func TestQ2AffEveryWriteCarriesItsExpectation(t *testing.T) {
	b := testQ2AffSetup(t, "everywrite")

	type rawStore struct {
		subject string
		expects int
		// The expectation VALUE actually emitted on the wire. Counting the header is not
		// enough: an encoder that always writes 0 emits exactly one header and passes a
		// presence check, while turning every transition into a rejected create-only CAS.
		value string
	}
	type argStore struct {
		writer    string
		subject   string
		expectSeq uint64
		// The sequences this subject had ACTUALLY HELD at the moment of this call.
		// Snapshotted per invocation, never read from the final list: validating a
		// call against sequences produced after it would accept a base that did not
		// exist yet, which is the whole property this oracle exists to check.
		heldThen []uint64
		// What THIS invocation actually put on the wire, captured causally rather than
		// by matching two independent lists in order. Ordering broke down under the
		// unordered SourceAcked/AffineDone writers and their reaper: argument order and
		// store order can differ, which produced both false failures and a false pass
		// for a permuted encoder defect.
		emitted   string
		emitCount int
		emitSeen  int
	}

	var mu sync.Mutex
	var orphans []rawStore
	var args []*argStore

	// storeMu serializes this broker's claim stores so that at most one is in flight,
	// which is what makes `cur` an unambiguous attribution target. The raw store fires
	// synchronously inside the delegation below, so with the serialization it can only
	// belong to the invocation currently holding storeMu. curMu is separate and narrow
	// because the raw hook runs INSIDE the delegation and must not re-enter storeMu.
	var storeMu sync.Mutex
	var curMu sync.Mutex
	var cur *argStore
	// Every sequence each claim subject has actually held, in order.
	held := map[string][]uint64{}

	// The raw oracle. Registered gate-first so the restore stops new calls and the gate
	// then waits out any already inside (see testHookGate).
	gate := newTestHookGate(t)
	restore := setMQTTTestStoreMsgHook(func(subject string, hdrLen int, msg []byte) error {
		// Filter BEFORE the gate, never after. gate.enter() returns holding a mutex, so
		// entering first would serialize every storeMsg in the process — including other
		// tests' servers, which run concurrently with this one — on a single lock for the
		// whole of this test.
		if !strings.HasPrefix(subject, mqttQoS2AssignStreamSubjectPrefix) {
			return nil
		}
		if !gate.enter() {
			return nil
		}
		defer gate.leave()
		n := mqttCountHeader(JSExpectedLastSubjSeq, msg[:hdrLen])
		v := string(getHeader(JSExpectedLastSubjSeq, msg[:hdrLen]))
		curMu.Lock()
		if cur != nil && cur.subject == subject {
			cur.emitted, cur.emitCount = v, n
			cur.emitSeen++
		} else {
			// A store on our prefix that no claim-store invocation of ours is inside.
			// Recorded rather than ignored: an unattributable emission means the
			// pairing assumption is wrong, and the oracle must say so instead of
			// silently checking fewer writes.
			mu.Lock()
			orphans = append(orphans, rawStore{subject: subject, expects: n, value: v})
			mu.Unlock()
		}
		curMu.Unlock()
		return nil
	})
	t.Cleanup(restore)

	// The member has to exist before the account manager does, and the seam below lives on
	// it. Connecting writes no claim — nothing has been delivered yet — so installing the
	// seam here still precedes every write the exchange makes.
	c, r := b.member(t, "everywriteA", 300)
	defer c.Close()
	b.bind(t, "everywriteA")

	// The call-site oracle. One seam carries BOTH writers, because both the election and
	// every transition are funnelled through as.storeQ2AffClaimCAS — which is itself the
	// audit's central structural claim, so classifying them by anything other than "is
	// this the first write to this subject" would assume what is under test.
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimStore = func(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
		storeMu.Lock()
		defer storeMu.Unlock()

		mu.Lock()
		writer := "W2-transition"
		if len(held[subject]) == 0 {
			writer = "W1-elect"
		}
		a := &argStore{writer: writer, subject: subject, expectSeq: expectSeq,
			heldThen: append([]uint64(nil), held[subject]...)}
		args = append(args, a)
		mu.Unlock()

		curMu.Lock()
		cur = a
		curMu.Unlock()
		seq, err := b.asm.jsa.storeQ2AffClaimCAS(subject, expectSeq, rec)
		curMu.Lock()
		cur = nil
		curMu.Unlock()

		if err == nil {
			mu.Lock()
			held[subject] = append(held[subject], seq)
			mu.Unlock()
		}
		return seq, err
	}
	b.asm.mu.Unlock()

	// The oracles are evaluated in a DEFERRED block, and that is load-bearing rather than
	// stylistic. A call site passing the wrong expectation breaks the exchange, so a flow
	// assertion below fatals first and an inline check would never be reached — the test
	// would still "fail", but for the flow rather than for the expectation, and a mutation
	// killed that way discriminates nothing. Deferring means a wrong base is reported as
	// the wrong base, by name.
	defer func() {
		// Shut the raw hook FIRST, and wait out any call already inside it. The oracles
		// below read the per-invocation emission fields (`emitted`, `emitCount`,
		// `emitSeen`), which that hook writes from the DELIVERY goroutine under `curMu`
		// while this block holds only `mu` — a mutex mismatch, and a real data race the
		// paired soak caught under `-race`. Taking `curMu` here instead would invert the
		// hook's own curMu→mu order and could deadlock; closing the gate removes the
		// concurrent writer altogether, which is what the gate is for. The gate's own
		// t.Cleanup closes it again, which is idempotent.
		//
		// The window is not hypothetical: an exchange's last claim transitions are written
		// by the JetStream delivery callback, so a store can still be in flight when the
		// subtest returns.
		gate.close()

		mu.Lock()
		defer mu.Unlock()

		// --- Oracle 1: every invocation emitted exactly one expectation header, carrying
		// the value that call site asked for. Both halves matter and neither implies the
		// other: counting alone passes an encoder that writes a CONSTANT, and the
		// call-site oracle below cannot see the wire at all.
		//
		// Attribution is CAUSAL, not positional. Each raw store is recorded onto the
		// invocation that was in flight when it fired, so a reordering between the
		// unordered SourceAcked/AffineDone writers cannot pair an argument with another
		// call's bytes. Cardinality is asserted in both directions.
		if len(args) == 0 {
			t.Error("no claim store was observed at all; the seam is not installed")
		}
		for _, a := range args {
			switch {
			case a.emitSeen == 0:
				t.Errorf("%s to %q reached the CAS helper but emitted no store; the raw hook or its prefix filter is wrong",
					a.writer, a.subject)
			case a.emitSeen > 1:
				t.Errorf("%s to %q emitted %d stores for one invocation; the pairing assumption is broken",
					a.writer, a.subject, a.emitSeen)
			}
			if a.emitCount != 1 {
				t.Errorf("%s to %q emitted %d %s headers, want exactly 1",
					a.writer, a.subject, a.emitCount, JSExpectedLastSubjSeq)
			}
			if want := strconv.FormatUint(a.expectSeq, 10); a.emitSeen > 0 && a.emitted != want {
				t.Errorf("%s to %q asked for expectation %s but emitted %q on the wire",
					a.writer, a.subject, want, a.emitted)
			}
		}
		// An emission on our own subjects that belonged to no invocation would mean a
		// writer bypassing the CAS helper, which is precisely what the audit claims does
		// not exist. Other tests' brokers use different group durables, so their stores
		// never land on these subjects.
		ours := map[string]bool{}
		for _, a := range args {
			ours[a.subject] = true
		}
		for _, o := range orphans {
			if ours[o.subject] {
				t.Errorf("a store to %q carried expectation %q but went through no claim-store call site; some writer bypasses storeQ2AffClaimCAS",
					o.subject, o.value)
			}
		}

		// --- Oracle 2: each writer was reached, with the base the audit claims. ---
		seen := map[string]bool{}
		for _, a := range args {
			seen[a.writer] = true
			switch a.writer {
			case "W1-elect":
				// Create-only. A nonzero expectation here means the election is conditioned
				// on a base it read, which is a different protocol from the documented one —
				// and one under which two members could both believe they elected.
				if a.expectSeq != 0 {
					t.Errorf("W1-elect to %q used expectation %d, want 0 (create-only)",
						a.subject, a.expectSeq)
				}
			case "W2-transition":
				// A transition must name a sequence this record actually held. Zero would
				// make it a create — which, on a subject whose claim still exists, is the
				// write that reopens a started delivery's ownership.
				if a.expectSeq == 0 {
					t.Errorf("W2-transition to %q used expectation 0, want the sequence it loaded",
						a.subject)
					continue
				}
				ok := false
				for _, sq := range a.heldThen {
					if sq == a.expectSeq {
						ok = true
						break
					}
				}
				if !ok {
					t.Errorf("W2-transition to %q used expectation %d, which that subject had not held at the time of the call (held then: %v)",
						a.subject, a.expectSeq, a.heldThen)
				}
			}
		}
		for _, w := range []string{"W1-elect", "W2-transition"} {
			if !seen[w] {
				t.Errorf("%s was never reached; the inventory claims both writers and this run exercised %v",
					w, seen)
			}
		}
	}()

	// A real shared QoS 2 exchange: the election (W1), the materialized transition, the
	// source ack and the affine completion (all W2), then the reap.
	b.publish(t, 1, "everywrite")

	_, pi := testMQTTReadPublishV5(t, r, b.topic, []byte("everywrite"))
	b.completeExchange(t, c, r, pi)
}

// affDurableFor is the affine consumer durable for one incarnation, which
// mqttQ2AffineNames returns as its third value.
// testQ2AffSelfConsistent makes a seeded claim describe a shape a real election
// could have produced: it mints an incarnation if the fixture did not choose one and
// derives CopySubject from the record's OWN winner, generation, claim hash and
// nonce.
//
// It exists because several fixtures predate F-065 and built CopySubject from
// unrelated values — a fresh random generation, a hand-written claim hash — which no
// election ever emits. q2AffClaimUsableForDecision now refuses such a record on every
// direct decision path, correctly: a copy subject that does not follow from the
// record makes the reaper inspect a copy the record does not own. Deriving here is a
// FIXTURE correction, not a relaxed assertion — the assertions are untouched; what
// changes is that the staged state is now reachable.
//
// A fixture that is ABOUT incarnations must set Nonce itself, with distinct values.
func testQ2AffSelfConsistent(c *mqttQ2AffClaim) *mqttQ2AffClaim {
	if c.Nonce == _EMPTY_ {
		c.Nonce = testQ2AffFixtureNonce
	}
	c.CopySubject = mqttQ2AffCopySubject(c.Winner, c.Gen, c.ClaimHash, c.Nonce)
	return c
}

// testQ2AffAwaitClaimFor returns the single claim elected for a group, waiting for
// the election to land. Returns a COPY, so a later transition cannot rewrite what an
// assertion already read.
func testQ2AffAwaitClaimFor(t *testing.T, as *mqttAccountSessionManager, groupDur string) *mqttQ2AffClaim {
	t.Helper()
	var found *mqttQ2AffClaim
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		found = nil
		if err := as.scanQ2AffClaims(func(_ string, cl *mqttQ2AffClaim, _ uint64) error {
			if cl.GroupDurable == groupDur {
				c := *cl
				found = &c
			}
			return nil
		}); err != nil {
			return err
		}
		if found == nil {
			return fmt.Errorf("no claim elected for %q yet", groupDur)
		}
		return nil
	})
	return found
}

// testQ2AffSessionClient builds the client the affine terminal paths expect: they reach
// the account manager and the session THROUGH it, so a bare &client{} makes
// mqttFinishQ2AffineDelivery return false before doing anything and a test would then be
// asserting on a no-op.
func testQ2AffSessionClient(b *testQ2AffBroker, sess *mqttSession) *client {
	c := &client{srv: b.s}
	c.mqtt = &mqtt{asm: b.asm, sess: sess}
	return c
}

func affDurableFor(idHash, gen string) string {
	_, _, durable := mqttQ2AffineNames(idHash, gen)
	return durable
}

// --------------------------------------------------------------------------
// Fixtures for the three defects F-033's consequence audit found. Each pins the
// DEFECTIVE behaviour, so each one's discriminating mutation is the candidate
// remedy: apply the remedy named in the comment and the fixture must fail. That
// is the inverse of the usual direction and it is deliberate — a finding's
// fixture exists so the defect cannot be fixed silently or reintroduced quietly.
// --------------------------------------------------------------------------

// TestQ2AffReapRefusesACopyStoredDuringItsSourceAck is the regression test F-065's
// defect fixture BECAME when the remedy landed. It used to assert the claim was
// reaped and to instruct whoever fixed it to come back and invert it; this is that
// inversion rather than a new fixture, so the interleaving it stages is exactly the
// one the finding was filed against.
//
// reapQ2AffClaim may perform a SourceAcked transition that is a full JetStream round
// trip with retries. R1 requires the copy reading reapable() consumes to be taken
// AFTER that transition and to be the last observation before the delete, so a copy
// stored during the round trip is seen and the claim is kept.
//
// The interleaving is staged through testQ2AffClaimStore, which intercepts exactly
// the transition whose duration is the window. That is the real window, not a
// simulation of one: the hook runs where the CAS runs.
//
// What must NOT be left behind: source proven, claim absent, copy PRESENT — the
// state reapable's own comment says must never exist, because the delivery gate takes
// its claim == nil branch and NAKs that copy forever.
//
// Paired mutation M1: move loadQ2AffCopy back above the advance. The reap then acts
// on the pre-transition reading, deletes the claim, and this test fails.
func TestQ2AffReapRefusesACopyStoredDuringItsSourceAck(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "stalecopy")
	c, _ := b.member(t, "stalecopyA", 300)
	b.bind(t, "stalecopyA")

	groupDur := b.groupDurable(t)
	idHash := getHash("stalecopyA")
	// The member's REAL affine generation, not a synthetic one. $MQTT_out is
	// interest-retained, so a copy stored on a subject no live consumer filters is
	// kept by nothing — the fixture would then assert on a copy that never existed
	// and would prove F-063's mechanism instead of F-065's.
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("the member has no affine generation, so no consumer would retain the racing copy")
	}
	// A source sequence that is authoritatively GONE, so the reaper performs the
	// SourceAcked advance itself (q2AffSourceUnofferable) — that advance is the window
	// under test, so the fixture must actually enter it.
	const goneSeq = uint64(999999)
	subject := mqttQ2AffClaimSubject(groupDur, goneSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, goneSeq)
	copySubject := mqttQ2AffCopySubject(idHash, gen, claimHash, testQ2AffFixtureNonce)

	claim := &mqttQ2AffClaim{
		Winner:       idHash,
		Gen:          gen,
		GroupDurable: groupDur,
		SourceSeq:    goneSeq,
		ClaimHash:    claimHash,
		Nonce:        testQ2AffFixtureNonce,
		CopySubject:  copySubject,
		AffDurable:   affDurableFor(idHash, gen),
		Phase:        mqttQ2AffMaterialized,
		Discarded:    true, // terminal on the discard axis, so only the copy side gates the reap
	}
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, claim); err != nil {
		t.Fatalf("seeding the claim: %v", err)
	}
	if st, _ := b.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("the copy must start absent for this window to be the one under test, got %v", st)
	}

	// The client leaves, keeping its session and its affine consumer. Without this the
	// live delivery subscription consumes the racing copy the instant it lands — the
	// gate finds the claim still present and Discarded, disposes of it, and the fixture
	// would observe an absent copy for a reason that has nothing to do with F-065.
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Interpose the concurrent materializer INSIDE the SourceAcked transition.
	var stored atomic.Bool
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimStore = func(s string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
		if s == subject && rec.SourceAcked && stored.CompareAndSwap(false, true) {
			env := &mqttQ2AffineEnvelope{
				natsSubject:  b.topic,
				msg:          []byte("stranded"),
				claimSubject: subject,
				gen:          gen,
				msgID:        mqttQ2AffMsgId(groupDur, goneSeq, testQ2AffFixtureNonce),
			}
			msg, hdrLen := mqttNewQ2AffineRecord(env)
			if _, err := b.asm.jsa.storeMsg(copySubject, hdrLen, msg); err != nil {
				t.Errorf("storing the racing copy: %v", err)
			}
		}
		return b.asm.jsa.storeQ2AffClaimCAS(s, expectSeq, rec)
	}
	b.asm.mu.Unlock()

	b.asm.reapQ2AffClaim(b.s, subject)

	if !stored.Load() {
		t.Fatal("the racing copy was never stored; the reaper did not enter its SourceAcked transition, so this run did not exercise the window")
	}
	gotClaim, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	st, _ := b.asm.loadQ2AffCopy(copySubject)
	if st != mqttQ2AffCopyPresent {
		t.Fatalf("the racing copy is not on the stream (%v); the interleaving did not complete", st)
	}
	if gotClaim == nil {
		t.Fatal("the claim was reaped out from under a copy that exists: reapable() consumed a copy reading older than the reap's own SourceAcked transition (F-065)")
	}
	// Declining to reap must not mean declining to prove the source side, or the claim
	// could never become reapable at all.
	if !gotClaim.SourceAcked {
		t.Fatal("the reap declined without recording the source side it had just proven")
	}
}

// TestQ2AffResumeAdoptsConsumerKeepingItsCreated pins the mechanism that was behind F-036's
// third manifestation, the "adoption blind spot" — DISSOLVED by course E, which removed the
// delete rather than guarding it. The mechanism is still worth pinning, because it is the
// argument that refused every guarded version.
//
// The removed step (5) re-verified that the consumer it was about to delete was still
// the INSTANCE quarantine observed, by comparing ConsumerInfo.Created. That discriminates a
// delete-and-recreate, which is what the grace exists to protect. It does NOT discriminate a
// session that RESUMES onto the consumer it finds: a resume preserves the incarnation
// generation, derives the same durable name, and affine provisioning is a create-or-UPDATE,
// so no new instance is created and Created is unchanged.
//
// This is MECHANISM EVIDENCE, not a defect pin, and the distinction matters. Reaching the
// window the removed delete had between its record re-read and its instance re-lookup
// additionally required a session resident with that generation while its record was
// absent — F-036's premise, and not something this fixture stages. What this fixture owns is the half a fixer is most likely to get wrong: it proves
// the instance fingerprint is not the defense it reads as, so ANY future re-proposal of a
// by-name affine consumer delete that leans on it would still destroy a live adopted
// consumer. Course E's remedy was to remove the delete instead.
//
// It asserts a property of ordinary provisioning, so it is not paired with a remedy
// mutation. The mutation it does discriminate is a
// provisioning change to delete-and-recreate on resume, which would silently destroy
// retained copies on every reconnect.
func TestQ2AffResumeAdoptsConsumerKeepingItsCreated(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "adopt")
	c, _ := b.member(t, "adoptA", 300)
	b.bind(t, "adoptA")

	idHash := getHash("adoptA")
	durables := b.affineConsumers(t, idHash)
	if len(durables) != 1 {
		t.Fatalf("expected exactly one affine consumer for the member, got %v", durables)
	}
	durable := durables[0]

	before, err := b.asm.jsa.lookupConsumer(mqttOutStreamName, durable)
	if err != nil || before == nil {
		t.Fatalf("looking up the affine consumer: ci=%v err=%v", before, err)
	}
	created := before.Created

	// Leave and come back on the same session. This is the resume that window admitted,
	// and the generation it comes back on is the one such a delete would have condemned.
	// Rejoining the group, not a bare reconnect: affine provisioning runs from the
	// shared-subscription path, so a resume that never re-subscribes would leave the
	// consumer untouched and this fixture would assert an unchanged Created for a reason
	// that has nothing to do with adoption.
	testMQTTDisconnect(t, c, nil)
	c.Close()
	c2, _ := b.member(t, "adoptA", 300)
	defer c2.Close()

	after, err := b.asm.jsa.lookupConsumer(mqttOutStreamName, durable)
	if err != nil || after == nil {
		t.Fatalf("the resumed session has no affine consumer %q: ci=%v err=%v", durable, after, err)
	}
	if !after.Created.Equal(created) {
		t.Fatalf("the adoption property no longer holds: the resume changed ConsumerInfo.Created "+
			"from %v to %v, so an instance fingerprint WOULD discriminate an adoption and course E's "+
			"refutation would need re-checking", created, after.Created)
	}
	// Pinned: a live resumed session carrying an instance fingerprint indistinguishable
	// from the one a by-name delete would have condemned.
	t.Log("adoption property reproduced: an adopting resume leaves ConsumerInfo.Created unchanged, " +
		"so an instance check cannot discriminate it")
}

// The split F-064 delivers, asserted from both sides.
//
// buildSessionRefs keys refs.affine on ps.Q2AffGen alone, so a live matching record
// makes the sweep skip EVERY claim of that generation — reconciliation and the discard
// alike. (It also skipped the consumer pass; F-036 course E removed that pass, so there
// is no consumer decision left to skip.) That has NOT changed and is not a defect: the sweep
// has no payload to materialize with, so it could only ever resolve such a claim by
// erasing it, and reaping a claim of a live generation needs the reaper fence that
// is F-065's remedy.
//
// What has changed is that the stall no longer waits for the owner. A group
// redelivery — which does have the payload — restores the destroyed consumer and
// materializes onto it, so the source is released instead of being re-offered
// forever against no MaxDeliver.
//
// Both halves are asserted here because either alone is misleading: the first would
// read as F-064 still being open, and the second would not show that the sweep is
// deliberately still passive.
func TestQ2AffLiveRecordStrandedClaimIsResolvedByAContender(t *testing.T) {
	t.Parallel()
	t.Run("a sweep pass alone leaves it, which is F-065's half", func(t *testing.T) {
		t.Parallel()
		testQ2AffSweepLeavesAStrandedClaim(t)
	})

	t.Run("a group redelivery resolves it, which is this item's half", func(t *testing.T) {
		t.Parallel()
		st := testQ2AffStageContender(t, "strandedfix", nil, nil, nil)
		st.deleteWinnerDurable(t)
		// One redelivery is enough: the contender restores the winner's consumer,
		// materializes onto it and releases the group source.
		testQ2AffAwaitMaterialized(t, st)
	})
}

func testQ2AffSweepLeavesAStrandedClaim(t *testing.T) {
	b := testQ2AffSetup(t, "stranded")
	c, _ := b.member(t, "strandedA", 0xFFFFFFFF) // never expires: "unbounded" is STAGED, not inferred
	defer c.Close()
	b.bind(t, "strandedA")

	idHash := getHash("strandedA")
	rec := testQ2AffAwaitStableRecord(t, b, idHash)
	if rec.Q2AffGen == _EMPTY_ {
		t.Fatal("the member has no affine generation, so there is no live reference to test")
	}
	gen := rec.Q2AffGen

	// The affine consumer destroyed out of band — the route docs/CONFORMANCE.md already
	// documents, and the one F-036's adoption blind spot also produces.
	durable := affDurableFor(idHash, gen)
	if _, err := b.asm.jsa.deleteConsumer(mqttOutStreamName, durable, false); err != nil {
		t.Fatalf("deleting the affine consumer out of band: %v", err)
	}

	// A nonterminal claim on a source that genuinely still exists, so "the group can
	// still re-offer it" is a property of the stream rather than of the fixture.
	groupDur := b.groupDurable(t)
	sourceSeq := testQ2AffLiveSourceSeq(t, b, "stranded/keep")
	subject := mqttQ2AffClaimSubject(groupDur, sourceSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, sourceSeq)
	claim := &mqttQ2AffClaim{
		Winner:       idHash,
		Gen:          gen,
		GroupDurable: groupDur,
		SourceSeq:    sourceSeq,
		ClaimHash:    claimHash,
		Nonce:        testQ2AffFixtureNonce,
		CopySubject:  mqttQ2AffCopySubject(idHash, gen, claimHash, testQ2AffFixtureNonce),
		AffDurable:   durable,
		Phase:        mqttQ2AffClaimed,
	}
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, claim); err != nil {
		t.Fatalf("seeding the stranded claim: %v", err)
	}

	// Neutralise the quarantine grace, and sweep TWICE. Both are load-bearing:
	// observedOrphanFor RECORDS an observation on its first call and only reports the
	// resource settled on a later one, so a single pass at the default grace would leave
	// this claim untouched for a reason that has nothing to do with F-064 — the fixture
	// would pass against a fully reconciling sweep and prove nothing.
	zero := time.Duration(0)
	b.asm.mu.Lock()
	b.asm.testOrphanGrace = &zero
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testOrphanGrace = nil
		b.asm.mu.Unlock()
	})
	b.asm.sweepOrphanedQoS2State(b.s)
	b.asm.sweepOrphanedQoS2State(b.s)

	got, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("loading the claim after the sweep: %v", err)
	}
	if got == nil {
		t.Fatal("the sweep reaped a nonterminal claim; that would be a different (and worse) defect than F-064")
	}
	if got.Discarded || got.Lost || got.AffineDone {
		// A failure, not a skip. The sweep has no payload, so the only way it could
		// have resolved this claim is by asserting a fact it cannot prove —
		// recording a delivery that never happened, or discarding a message the
		// client is still owed.
		t.Fatalf("the sweep resolved the stranded claim (Discarded=%v Lost=%v AffineDone=%v); it has no payload, so it cannot have proven any of them",
			got.Discarded, got.Lost, got.AffineDone)
	}
	if got.Phase != mqttQ2AffClaimed {
		t.Fatalf("the claim's phase moved to %v without a terminal fact", got.Phase)
	}
	// Pinned: the sweep stays passive under a live reference. Its reap half is
	// F-065's, and the stall itself is closed by the redelivery route instead.
	t.Log("the sweep left a nonterminal claim of a live generation untouched, which is F-065's half of F-064")
}

// TestQ2AffOwnClaimRefusesSourceAckWhenTheStoreKeptNothing is F-063's own-claim route,
// as a regression test. It began life as that finding's defect pin, asserting the
// released source; the remedy inverted it, which is what the pin was written to force.
//
// $MQTT_out is interest-retained, so a copy stored onto a subject no consumer filters
// SUCCEEDS AND KEEPS NOTHING, and the own-claim path is where nothing else can catch that:
// beginQ2AffOp admits the move on local state alone (a readiness flag, a generation and a
// consumer POINTER), so the store is the first operation that touches the durable consumer
// at all. materializeQ2AffCopy therefore proves the copy with a PRESENCE CHECK after the
// store, and the group source is released only against that proof.
//
// Staged with no seam at all, because none is needed: deleting the affine consumer out of
// band leaves the live session's cached pointer intact, which is precisely the state the
// admission gate cannot see. Everything after that is the ordinary production path.
//
// Asserted: the claim stays at `claimed`, carries no terminal fact, is never source-acked,
// and its source message is still on $MQTT_msgs — the accepted message is still owned by
// somebody. What that refusal strands is F-064's to resolve, and its pin above reproduces
// it; this fixture's business is only that nothing was released against a store response.
//
// Discriminating mutation: verify only on a REJECTED create-only CAS (mutation 1), which is
// what the code did when F-063 was filed. The store then succeeds unverified, the claim
// advances to materialized, and the source is acknowledged — so this fixture fails on the
// released source rather than on silence.
func TestQ2AffOwnClaimRefusesSourceAckWhenTheStoreKeptNothing(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "keptnothing")
	c, _ := b.member(t, "keptnothingA", 300)
	defer c.Close()
	b.bind(t, "keptnothingA")
	moves := testQ2AffWatchMoves(t, b)

	idHash := getHash("keptnothingA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("the member has no affine generation, so there is no consumer to remove")
	}

	// The consumer goes; the session's cached pointer stays. beginQ2AffOp still admits.
	if _, err := b.asm.jsa.deleteConsumer(mqttOutStreamName, affDurableFor(idHash, gen), false); err != nil {
		t.Fatalf("deleting the affine consumer out of band: %v", err)
	}

	// An ordinary QoS 2 publish to the shared group. The member's own move elects a claim
	// and stores the copy onto nothing.
	b.publish(t, 1, "keptnothing")

	subject, claim := testQ2AffOnlyClaim(t, b)
	if !claim.ownedBy(idHash, gen) {
		t.Fatalf("the claim on %q names %q/%q, not the member under test: this run did not exercise the own-claim route",
			subject, claim.Winner, claim.Gen)
	}

	// The move RAN and refused. Without this the assertions below would also pass on a
	// run where the group delivery never reached the member at all.
	moves.await(t, subject, mqttQ2AffMoveDeferred, "the owner refused to release a source for a store that kept nothing")
	moves.never(t, subject, mqttQ2AffMoveOwned, mqttQ2AffMoveTerminal)

	// The copy has to be AUTHORITATIVELY absent, not merely unreadable, or the assertion
	// would rest on a failed lookup rather than on interest retention having kept nothing.
	if st, _ := b.asm.loadQ2AffCopy(claim.CopySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("the copy on %q reads %v, want absent: the store kept something after all",
			claim.CopySubject, st)
	}
	testQ2AffAssertNotReleased(t, b, moves, subject, mqttQ2AffClaimed, 2)
	// Positive evidence that the message is still the group's to re-offer, rather than
	// released and gone. A POSITIVE load, not `!q2AffSourceGone`: that production predicate
	// is fail-closed and answers false both when the source is present and when it could
	// not be read, so negating it would let an indeterminate read pass for evidence.
	testQ2AffAssertSourcePresent(t, b.asm, claim, "after the refusal")
}

// TestQ2AffStaleClaimOnAReusedSourceSequenceEatsTheMessage pins F-066's flagship row.
//
// A claim is keyed `(group durable, source sequence)`. That identifies one source message
// only within one incarnation of $MQTT_msgs, and a recreation of that stream leaves
// $MQTT_qos2assign's claims behind with their sequence counter reset underneath them.
//
// Recreating it ALONE is what startup USED TO instruct, and that reachability is what
// made this F-066's flagship row. F-066's enforcement (2026-09-01) withdrew it: the nine
// streams are one recreation unit and a proper subset is refused before anything is
// provisioned, so the state below is now reachable only through an UNSUPPORTED operation
// — administrative deletion or a partial restore. The behaviour this test pins is
// unchanged and still worth pinning: it is defence in depth against exactly that.
//
// The fixture stages the STATE that procedure produces rather than the procedure: a
// terminal claim already occupying the subject the very next published message will
// compute. That is exactly what a survivor of a recreation looks like, and staging it
// directly keeps the test to one broker with no stream surgery — the half this fixture
// proves is "a stale claim on a reused sequence eats the message", not "a recreation
// produces stale claims", which is the operator procedure and is argued in the dossier.
//
// Asserted: the client receives NOTHING. The mover takes its terminal arm and, because the
// stale claim's group ack is unproven, confirms it against the NEWLY reused sequence before
// recording it — so the accepted message is released from the shared group with no copy and
// no delivery.
//
// Discriminating mutation: carry the $MQTT_msgs incarnation into the claim subject (the
// remedy). The new message then computes a different subject, does not collide with the
// stale claim, and is delivered — so this fixture fails at "a PUBLISH arrived".
func TestQ2AffStaleClaimOnAReusedSourceSequenceEatsTheMessage(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "reused")
	c, r := b.member(t, "reusedA", 300)
	defer c.Close()
	b.bind(t, "reusedA")

	// The sequence the next PUBLISH will occupy on $MQTT_msgs. Read from the stream
	// rather than assumed: a fabricated number would make the collision an artifact of
	// the fixture instead of the real key the mover computes.
	si, err := b.asm.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		t.Fatalf("looking up %s: si=%v err=%v", mqttStreamName, si, err)
	}
	nextSeq := si.State.LastSeq + 1

	groupDur := b.groupDurable(t)
	// Constructed LITERALLY, not through mqttQ2AffClaimSubject, and that is what makes
	// the mutation below discriminate. A claim that survived a recreation was written by
	// the code as it stands today, so the fixture must pin today's subject SHAPE. Building
	// it through the helper would move the seeded claim in lockstep with any remedy that
	// changes the key, the collision would persist, and the fixture would pass against the
	// fix — which is exactly what the first version of this test did.
	subject := mqttQoS2AssignStreamSubjectPrefix + groupDur + "." + strconv.FormatUint(nextSeq, 10)
	claimHash := mqttQ2AffClaimHash(groupDur, nextSeq)

	// A claim left over from the previous incarnation: terminal on the discard axis with
	// its group ack still UNPROVEN — the resting state the protocol requires, since a
	// discarded claim must survive precisely until some contender proves that ack.
	// The CLAIM SUBJECT above is what this fixture pins, and it stays literal. The COPY
	// subject is derived from the record's own identity instead: since F-065 a record
	// whose copy subject does not follow from its own fields is refused on every direct
	// decision path, so a hand-built one would make the mover decline before it ever
	// reached the terminal arm this row asserts — the fixture would then fail for a
	// reason that has nothing to do with F-066. Note what this does NOT change: the
	// collision is on the claim subject, which carries no incarnation, so F-065's nonce
	// does not close this row.
	staleGen := mqttNewQ2AffGen()
	stale := &mqttQ2AffClaim{
		Winner:       getHash("someone-else"),
		Gen:          staleGen,
		GroupDurable: groupDur,
		SourceSeq:    nextSeq,
		ClaimHash:    claimHash,
		Nonce:        testQ2AffFixtureNonce,
		CopySubject:  mqttQ2AffCopySubject(getHash("someone-else"), staleGen, claimHash, testQ2AffFixtureNonce),
		AffDurable:   "$MQTT_Q2AFF_stalefromlastincarnation",
		Phase:        mqttQ2AffMaterialized,
		Discarded:    true,
		// SourceAcked is seeded FALSE deliberately, and it is what makes the reap below
		// evidence rather than a tautology. With it true the mover's terminal arm takes
		// the fire-and-forget sendAck shortcut and the later reap would merely re-observe
		// the bit this fixture wrote — proving nothing about the NEW source. False forces
		// the arm through confirmSourceAckAndRecord, which confirms the reused sequence's
		// ack with JetStream and only then records it, so a claim that subsequently
		// vanishes can only have been reaped on a SourceAcked that that confirmation
		// wrote. It is also the faithful resting state: a discarded claim whose group ack
		// was never proven is exactly what must survive until some contender proves it.
		SourceAcked: false,
	}
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, stale); err != nil {
		t.Fatalf("seeding the stale claim: %v", err)
	}

	b.publish(t, 1, "reused")

	// The publish must have taken EXACTLY the predicted sequence, or the collision this
	// fixture is about never happened and a silent client proves nothing. Checked as
	// equality, not >=: a later sequence means some other activity intervened.
	if got := b.lastSourceSeq(t); got != nextSeq {
		t.Skipf("the publish landed at %d, not the predicted %d; concurrent stream activity, not a result", got, nextSeq)
	}

	if _, _, ready := testMQTTReadPacketReady(r, 2*time.Second); ready {
		// The remedy landing reaches exactly this branch.
		t.Fatal("a PUBLISH arrived, so the stale claim did not capture the reused sequence: F-066's flagship row is closed — replace this defect fixture with a regression test for the incarnation-scoped key")
	}

	// Silence alone would not prove the message was RELEASED — a stall, a deferral or any
	// other delivery failure looks identical on the wire. The positive evidence is that the
	// stale claim has been REAPED. reapQ2AffClaim deletes only a claim whose source ack is
	// PROVEN, the claim was seeded with that proof ABSENT, and the only thing that can
	// supply it on this route is the terminal arm's confirmSourceAckAndRecord against the
	// newly reused sequence. A vanished claim is therefore positive evidence that THIS
	// message's group delivery was irreversibly acknowledged and the accepted message is
	// gone — not merely that nothing has been delivered yet, which a stall would also
	// produce.
	//
	// A generous deadline, deliberately: the poll exits the instant the reap is observed,
	// so a large bound costs a passing run nothing. The group-delivery callback driving the
	// mover is asynchronous and under a loaded full-suite run can be scheduled well after
	// the silence check above; a tight bound would turn that scheduling delay into a
	// spurious failure of an assertion about arbitration.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cl, _, lerr := b.asm.loadQ2AffClaim(subject)
		if lerr != nil {
			t.Fatalf("loading the stale claim: %v", lerr)
		}
		if cl == nil {
			// F-066, pinned: source released, nothing delivered, claim reaped.
			t.Log("F-066 reproduced: a stale terminal claim on a reused source sequence swallowed an accepted message")
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the stale claim was never reaped, so the mover did not take its terminal arm: this run did not exercise the acknowledge-and-walk-away path the row asserts")
}

// lastSourceSeq is $MQTT_msgs' current last sequence, so a fixture can assert that a
// publish landed exactly where it predicted rather than merely at or beyond it.
func (b *testQ2AffBroker) lastSourceSeq(t *testing.T) uint64 {
	t.Helper()
	si, err := b.asm.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		t.Fatalf("looking up %s: si=%v err=%v", mqttStreamName, si, err)
	}
	return si.State.LastSeq
}

// ---------------------------------------------------------------------------
// F-064 — the owner's provisioning path gains no new refusals
// ---------------------------------------------------------------------------

// testQ2AffOwnerArm imposes one condition that on-behalf restoration teaches the
// CONTENDER to refuse, and that the owner must go on ignoring. lookups, when
// non-nil, reports how many affine consumer lookups the arm observed.
type testQ2AffOwnerArm struct {
	name  string
	apply func(t *testing.T, b *testQ2AffBroker) (lookups func() int32)
}

// A function rather than a package-level var, deliberately: a var here is
// process-global state, and TestParallelTestsTouchNoProcessGlobalState correctly
// refuses to let a t.Parallel() test reach one. Same reason as
// testBrokerStreamSpecs (stream_prereqs_test.go).
func testQ2AffOwnerArms() []testQ2AffOwnerArm {
	return []testQ2AffOwnerArm{
		{
			// A stream-forced inactivity limit imposed AFTER startup, so
			// validateOutStream has already run and passed. nats-server substitutes the
			// stream's limit into any create whose own InactiveThreshold is zero, and the
			// affine config deliberately sends zero — so this is the condition under
			// which the owner creates a durable the stream may later reap. A contender
			// refuses it outright; the owner accepts it, exactly as it does today.
			name: "a stream inactivity limit imposed after startup",
			apply: func(t *testing.T, b *testQ2AffBroker) func() int32 {
				t.Helper()
				si, err := b.jsa.lookupStream(mqttOutStreamName)
				if err != nil || si == nil {
					t.Fatalf("looking up %s: %v", mqttOutStreamName, err)
				}
				cfg := si.Config
				// Minutes rather than milliseconds, deliberately. This arm pins that the
				// owner ACCEPTS the substituted threshold; a short one would reap the
				// durable during the disconnect window and fail the resume shapes for a
				// reason this fixture does not own.
				cfg.ConsumerLimits.InactiveThreshold = 5 * time.Minute
				if _, err := b.jsa.updateStream(&cfg); err != nil {
					t.Fatalf("imposing a consumer inactivity limit on %s: %v", mqttOutStreamName, err)
				}
				return nil
			},
		},
		{
			// The seam that catches a lookup added to the owner's path. The owner
			// performs none, so failing every lookup must change nothing; a preflight
			// would turn this into a failed provision, which is what distinguishes a
			// preflight from the post-failure recovery the owner is allowed.
			name: "every affine consumer lookup failing",
			apply: func(t *testing.T, b *testQ2AffBroker) func() int32 {
				t.Helper()
				var n atomic.Int32
				b.asm.mu.Lock()
				b.asm.testQ2AffConsumerLookup = func(string) (*ConsumerInfo, error) {
					n.Add(1)
					return nil, fmt.Errorf("test: the affine consumer lookup is unavailable")
				}
				b.asm.mu.Unlock()
				return n.Load
			},
		},
	}
}

// testQ2AffRestartSameStore cycles only the MQTT layer, leaving JetStream and
// every record and consumer in place. The resume shapes below need it: within one
// process a reconnect finds q2AffReady still set and takes
// ensureQ2AffineConsumerSubscription's fast path, so provisioning would never run
// and the shape would assert nothing.
func testQ2AffRestartSameStore(t *testing.T, b *testQ2AffBroker) {
	t.Helper()
	dir := b.o.StoreDir
	b.s.Shutdown()
	b.o.Port = -1
	b.o.MQTT.Port = -1
	b.o.StoreDir = dir
	b.s = testMQTTRunServer(t, b.o)
	t.Cleanup(func() { testMQTTShutdownServer(b.s) })
}

// testQ2AffReadRecordAfterRestart binds this side of a restart to an account
// manager and returns the quiescent record. The throwaway client is what runs the
// startup stream validation, which must happen BEFORE an arm is imposed:
// validateOutStream refuses a nonzero ConsumerLimits.InactiveThreshold outright,
// so arming first would fail the broker at boot rather than test the owner.
func testQ2AffReadRecordAfterRestart(t *testing.T, b *testQ2AffBroker, name, idHash string) *mqttPersistedSession {
	t.Helper()
	cq, rq := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: name, cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, rq)
	b.bind(t, name)
	ps := testQ2AffAwaitStableRecord(t, b, idHash)
	cq.Close()
	return ps
}

// Decision 4's guard, and the only fixture in this file that fails when the
// implementation is MORE defensive than it needs to be.
//
// On-behalf restoration teaches a contender a set of preconditions before it may
// recreate somebody else's affine durable. None of them may reach the owner's own
// provisioning, because a refusal costs different things on the two paths. On the
// contender it is free: nothing is stored, no source is acked, and the stall is
// left exactly as it is today with no client connected to notice. On the owner it
// provably costs a live connection — a refusal while a saved shared filter is
// being restored at CONNECT is silently skipped (mqtt.go:7089-7093, :7187-7190)
// because there is no SUBACK on that path, and the only remedy that removes the
// silence closes the connection, which publishes the client's Will and, at a zero
// Session Expiry Interval, deletes the session.
//
// "No closed connection, no Will, no cleared session" is deliberately NOT the
// oracle here, and must never become it: a per-filter refusal never closes the
// connection anyway, so an implementation that silently dropped the restored
// filter would satisfy all three while delivering nothing. Every shape below
// asserts a POSITIVE outcome instead — a success reason code, or a message
// carried end to end over the path the arm was supposed to have broken.
func TestQ2AffOwnerProvisioningGainsNoNewRefusals(t *testing.T) {
	t.Parallel()
	for _, arm := range testQ2AffOwnerArms() {
		t.Run(arm.name, func(t *testing.T) {
			t.Parallel()

			// (1) A fresh shared SUBSCRIBE. The owner mints a generation and creates
			// the durable with no prior state of any kind.
			t.Run("a fresh shared subscribe", func(t *testing.T) {
				t.Parallel()
				b := testQ2AffSetup(t, "ownfresh")
				// Bootstrap the account manager first, so the arm is imposed out of
				// band on a running broker — as a deployment change would be — rather
				// than before the startup validation that would refuse it.
				boot, bootr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ownfresh-boot", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
				testMQTTReadConnAckV5(t, bootr)
				b.bind(t, "ownfresh-boot")
				boot.Close()

				lookups := arm.apply(t, b)

				// member asserts the SUBACK reason code is 2, which is this shape's
				// first oracle: a refusal here would surface as 0x80.
				c, r := b.member(t, "ownfreshA", 300)
				defer c.Close()
				b.publish(t, 1, "fresh")
				_, payload, qos, pi, ok := testMQTTTryReadPublishV5(r, 10*time.Second)
				if !ok {
					t.Fatal("a fresh shared QoS 2 subscribe provisioned no working affine path")
				}
				if qos != 2 || string(payload) != "fresh" {
					t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
				}
				b.completeExchange(t, c, r, pi)
				testQ2AffAssertNoOwnerLookup(t, lookups)
			})

			// (2) A resume that RESTORES a saved shared filter. This is the route
			// through processSubs (mqtt.go:7089) — the one with no SUBACK to carry a
			// refusal, and the one round 32 found a remedy had missed.
			t.Run("a resume restoring a saved shared filter", func(t *testing.T) {
				t.Parallel()
				b := testQ2AffSetup(t, "ownsaved")
				c, _ := b.member(t, "ownsavedA", 300)
				b.bind(t, "ownsavedA")
				idHash := getHash("ownsavedA")
				c.Close()
				testQ2AffRestartSameStore(t, b)

				ps := testQ2AffReadRecordAfterRestart(t, b, "ownsaved-reader", idHash)
				// Anti-vacuity: without a saved shared filter this shape restores
				// nothing and would pass against any implementation at all.
				if len(ps.SharedSubs) == 0 {
					t.Fatalf("no saved shared filter for the resume to restore: %+v", ps)
				}

				lookups := arm.apply(t, b)

				cr, rr := b.reconnect(t, "ownsavedA", 300)
				defer cr.Close()
				// The delivery is the oracle, not the absence of an error: the filter
				// is restored with no SUBACK, so only a message arriving over it proves
				// the restoration installed anything.
				b.publish(t, 2, "saved")
				_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rr, 10*time.Second)
				if !ok {
					t.Fatal("the restored shared filter delivered nothing: its affine provisioning was refused and the failure silently skipped")
				}
				if qos != 2 || string(payload) != "saved" {
					t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
				}
				b.completeExchange(t, cr, rr, pi)
				testQ2AffAssertNoOwnerLookup(t, lookups)
			})

			// (3) A resume with NO saved shared filter, whose record still names the
			// affine generation. This is the other route (mqtt.go:10481, guarded at
			// :10477), and it is reachable only this way: while any shared filter is
			// saved, the restoration pass above provisions first and this call finds
			// the path already ready.
			t.Run("a resume with no saved shared filter", func(t *testing.T) {
				t.Parallel()
				b := testQ2AffSetup(t, "ownbare")
				c, r := b.member(t, "ownbareA", 300)
				b.bind(t, "ownbareA")
				idHash := getHash("ownbareA")

				// Leave a started delivery UNCOMPLETED, then leave the group. The
				// affine path outlives the membership because the copy is owed to the
				// client regardless of it — which is what leaves a record naming a
				// generation with no shared filter beside it.
				b.publish(t, 1, "bare")
				if _, _, _, _, ok := testMQTTTryReadPublishV5(r, 10*time.Second); !ok {
					t.Fatal("the sole group member never received the delivery this shape must strand")
				}
				b.unsubscribe(t, c, r, 2, []string{"$share/" + b.group + "/" + b.topic})
				c.Close()
				testQ2AffRestartSameStore(t, b)

				ps := testQ2AffReadRecordAfterRestart(t, b, "ownbare-reader", idHash)
				// Anti-vacuity, in both directions: a generation to re-establish, and
				// no shared filter, or the shape covers the other route instead.
				if ps.Q2AffGen == _EMPTY_ || ps.Q2Aff == nil {
					t.Fatalf("the record names no affine path for the resume to re-establish: %+v", ps)
				}
				if len(ps.SharedSubs) != 0 {
					t.Fatalf("a shared filter survived the unsubscribe, so this shape would exercise the restoration route instead: %+v", ps.SharedSubs)
				}

				lookups := arm.apply(t, b)

				cr, rr := b.reconnect(t, "ownbareA", 300)
				defer cr.Close()
				// The copy stranded before the restart is redelivered from the durable
				// this resume had to re-establish. Nothing else can produce it: the
				// session holds no subscription of any kind.
				_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rr, 10*time.Second)
				if !ok {
					t.Fatal("the resume did not re-establish the affine path its record named, so the stranded copy was never redelivered")
				}
				if qos != 2 || string(payload) != "bare" {
					t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
				}
				b.completeExchange(t, cr, rr, pi)

				sess := testMQTTGetClient(t, b.s, "ownbareA").mqtt.sess
				sess.mu.Lock()
				ready := sess.q2AffReady
				sess.mu.Unlock()
				if !ready {
					t.Fatal("the affine readiness gate never opened on a resume that had a generation to re-establish")
				}
				testQ2AffAssertNoOwnerLookup(t, lookups)
			})
		})
	}
}

// testQ2AffAssertNoOwnerLookup pins the negative directly, rather than leaving it
// to the delivery oracles alone: the owner's provisioning performs no affine
// consumer lookup, so the seam must never have been reached.
func testQ2AffAssertNoOwnerLookup(t *testing.T, lookups func() int32) {
	t.Helper()
	if lookups == nil {
		return
	}
	if n := lookups(); n != 0 {
		t.Fatalf("the owner's provisioning performed %d affine consumer lookup(s); it must perform none", n)
	}
}

// ---------------------------------------------------------------------------
// F-064 — the canonical affine consumer configuration
// ---------------------------------------------------------------------------

// testQ2AffFieldDisposition is one ConsumerConfig field's verdict in the audit
// below. There are exactly two, and the bijection is what makes the audit
// closed: a dependency bump that adds a field fails naming it, rather than
// silently landing in whichever bucket a default would have chosen.
type testQ2AffFieldDisposition struct {
	// variable: the record supplies this field's value, so a contender must
	// accept a difference in it and bound it separately.
	variable bool
	// mutate changes the field to a value that is NOT the canonical one, so the
	// comparison must refuse it. Required for a pinned field.
	mutate func(*ConsumerConfig)
	// reason is the audit. Non-empty prose is enforced: the reason IS the record
	// of why this field is safe to pin or unsafe to.
	reason string
}

// testQ2AffineConfigDispositions is the audit itself, one entry per
// ConsumerConfig field. Written as a function rather than a package var for the
// same reason as testQ2AffOwnerArms: a var here is process-global state.
func testQ2AffineConfigDispositions() map[string]testQ2AffFieldDisposition {
	tm := time.Unix(1755000000, 0).UTC()
	return map[string]testQ2AffFieldDisposition{
		// The three the record owns. A contender replays them from the winner's
		// record rather than imposing its own, so a difference here is legitimate
		// and is bounded by q2AffVariableFieldsInBounds instead.
		"AckWait":       {variable: true, reason: "the owner's broker AckWait, which may legitimately differ from the contender's; bounded positive"},
		"MaxAckPending": {variable: true, reason: "the owner's broker MaxAckPending; bounded to 1..mqttMaxAckTotalLimit and to the live stream limit"},
		"MemoryStorage": {variable: true, reason: "immutable once created, so a contender adopts the durable's own value rather than imposing one; both values are broker-produced"},

		// The identity. Derived from (idHash, gen), so a deviation names a
		// consumer this session does not own.
		"Durable":        {mutate: func(c *ConsumerConfig) { c.Durable += "x" }, reason: "derived from idHash and the generation; a different durable is a different session's consumer"},
		"FilterSubject":  {mutate: func(c *ConsumerConfig) { c.FilterSubject += ".x" }, reason: "the generation-scoped copy subject; a different filter reads another session's copies"},
		"DeliverSubject": {mutate: func(c *ConsumerConfig) { c.DeliverSubject += "x" }, reason: "the generation-scoped delivery subject the owner's subscription is bound to"},

		// The delivery semantics. Both are immutable in JetStream, so a
		// create-or-update cannot converge a durable that differs in either.
		"AckPolicy":     {mutate: func(c *ConsumerConfig) { c.AckPolicy = AckNone }, reason: "AckExplicit is what gates delivery on the client's PUBREC; the zero value is AckNone, which would ack on send"},
		"DeliverPolicy": {mutate: func(c *ConsumerConfig) { c.DeliverPolicy = natsserver.DeliverAll }, reason: "DeliverNew; the zero value is DeliverAll, which would replay the subject from its start"},

		// Pinned at zero. Each would change what the consumer delivers, how often,
		// or whether it survives — and each is a field some other writer could set.
		"MaxDeliver":         {mutate: func(c *ConsumerConfig) { c.MaxDeliver = 1 }, reason: "a delivery cap would abandon an owed QoS 2 message after N attempts, reopening the stall this feature closes"},
		"InactiveThreshold":  {mutate: func(c *ConsumerConfig) { c.InactiveThreshold = time.Minute }, reason: "an inactivity delete would reap the durable while its client is offline, dropping every copy it holds"},
		"HeadersOnly":        {mutate: func(c *ConsumerConfig) { c.HeadersOnly = true }, reason: "the copy's payload IS the message; delivering headers only would deliver an empty PUBLISH"},
		"DeliverGroup":       {mutate: func(c *ConsumerConfig) { c.DeliverGroup = "g" }, reason: "a queue group on the affine consumer would share this session's private delivery with whoever else joined it"},
		"PauseUntil":         {mutate: func(c *ConsumerConfig) { c.PauseUntil = &tm }, reason: "a paused consumer delivers nothing, which is the stall in another form"},
		"FlowControl":        {mutate: func(c *ConsumerConfig) { c.FlowControl = true }, reason: "flow control changes the delivery protocol the affine callback expects"},
		"Heartbeat":          {mutate: func(c *ConsumerConfig) { c.Heartbeat = time.Second }, reason: "idle heartbeats are delivered to the same subject and would reach the affine callback as if they were copies"},
		"ReplayPolicy":       {mutate: func(c *ConsumerConfig) { c.ReplayPolicy = natsserver.ReplayOriginal }, reason: "original-rate replay would pace an owed redelivery by the original publish spacing"},
		"RateLimit":          {mutate: func(c *ConsumerConfig) { c.RateLimit = 1024 }, reason: "a rate limit throttles an owed QoS 2 redelivery"},
		"OptStartSeq":        {mutate: func(c *ConsumerConfig) { c.OptStartSeq = 7 }, reason: "meaningful only under a start-sequence DeliverPolicy, which is itself pinned; a stray value is a record this broker did not write"},
		"OptStartTime":       {mutate: func(c *ConsumerConfig) { c.OptStartTime = &tm }, reason: "as OptStartSeq; also the only other time pointer, so it pins that DeepEqual is comparing pointees"},
		"BackOff":            {mutate: func(c *ConsumerConfig) { c.BackOff = []time.Duration{time.Second} }, reason: "a redelivery backoff schedule overrides AckWait for an owed delivery; also pins that the comparison sees slices"},
		"FilterSubjects":     {mutate: func(c *ConsumerConfig) { c.FilterSubjects = []string{"a.b"} }, reason: "a multi-filter consumer would read subjects outside this session's generation"},
		"Metadata":           {mutate: func(c *ConsumerConfig) { c.Metadata = map[string]string{"k": "v"} }, reason: "not set by this broker; also pins that the comparison sees maps"},
		"Name":               {mutate: func(c *ConsumerConfig) { c.Name = "n" }, reason: "the broker names the consumer through Durable; a separate Name is a record it did not write"},
		"Description":        {mutate: func(c *ConsumerConfig) { c.Description = "d" }, reason: "not set by this broker"},
		"SampleFrequency":    {mutate: func(c *ConsumerConfig) { c.SampleFrequency = "100%" }, reason: "ack sampling publishes to a system subject; not set by this broker"},
		"MaxWaiting":         {mutate: func(c *ConsumerConfig) { c.MaxWaiting = 1 }, reason: "a pull-consumer field; the affine consumer is push, so a value here is a record this broker did not write"},
		"MaxRequestBatch":    {mutate: func(c *ConsumerConfig) { c.MaxRequestBatch = 1 }, reason: "a pull-consumer field, as MaxWaiting"},
		"MaxRequestExpires":  {mutate: func(c *ConsumerConfig) { c.MaxRequestExpires = time.Second }, reason: "a pull-consumer field, as MaxWaiting"},
		"MaxRequestMaxBytes": {mutate: func(c *ConsumerConfig) { c.MaxRequestMaxBytes = 1 }, reason: "a pull-consumer field, as MaxWaiting"},
		"Replicas":           {mutate: func(c *ConsumerConfig) { c.Replicas = 3 }, reason: "the affine consumer inherits the stream's replication; an override is a record this broker did not write"},
		"Direct":             {mutate: func(c *ConsumerConfig) { c.Direct = true }, reason: "a server-set field; a record carrying it was not built from a request this broker sent"},
		"Sourcing":           {mutate: func(c *ConsumerConfig) { c.Sourcing = true }, reason: "a server-set field, as Direct"},
		"PriorityGroups":     {mutate: func(c *ConsumerConfig) { c.PriorityGroups = []string{"p"} }, reason: "priority delivery would arbitrate a private consumer's messages between claimants; not set by this broker"},
		"PriorityPolicy":     {mutate: func(c *ConsumerConfig) { c.PriorityPolicy = natsserver.PriorityOverflow }, reason: "as PriorityGroups"},
		"PinnedTTL":          {mutate: func(c *ConsumerConfig) { c.PinnedTTL = time.Minute }, reason: "a priority-pinning lifetime; as PriorityGroups"},
	}
}

// testQ2AffineConfigFields reflects every ConsumerConfig field name.
func testQ2AffineConfigFields(t *testing.T) []string {
	t.Helper()
	tp := reflect.TypeOf(ConsumerConfig{})
	fields := make([]string, 0, tp.NumField())
	for i := 0; i < tp.NumField(); i++ {
		fields = append(fields, tp.Field(i).Name)
	}
	return fields
}

// The drift guard for the canonical comparison, and the reason that comparison
// is whole-struct rather than a list.
//
// A list of interesting fields cannot close a struct that a dependency bump can
// extend, and this design's earlier revisions each wrote one that the next
// review round falsified — MaxDeliver, HeadersOnly, DeliverGroup, PauseUntil,
// FlowControl and Heartbeat were each found outside a list that was believed
// complete, and MaxDeliver alone reopens the stall. So the check compares the
// whole struct, and THIS test is what keeps the audit behind it honest: every
// field is named, exactly once, as either supplied by the record or pinned at
// the canonical value, and every pinned one is mutated and must be refused.
//
// It fails on a dependency bump, by design. That failure is the review: somebody
// has to decide which bucket the new field belongs in and write down why.
func TestQ2AffineConsumerConfigFieldCoverage(t *testing.T) {
	t.Parallel()
	fields := testQ2AffineConfigFields(t)
	disp := testQ2AffineConfigDispositions()

	// Bijection, iterated in both directions. A field with no disposition is a
	// dependency bump nobody audited; a disposition with no field is an audit
	// entry that has rotted.
	for _, f := range fields {
		if _, ok := disp[f]; !ok {
			t.Errorf("ConsumerConfig field %q has no disposition: a dependency bump added a field the affine audit does not cover", f)
		}
	}
	for f := range disp {
		if !slices.Contains(fields, f) {
			t.Errorf("the affine audit names ConsumerConfig field %q, which no longer exists", f)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	const idHash, gen = "abcdefgh", "Zq1Zq1Zq1Zq1Zq1Zq1Zq1Z"
	const ackWait, maxAckPending = 3 * time.Second, 17

	// The baseline must be accepted, or every refusal below is vacuous.
	base := buildQ2AffineConsumerConfig(idHash, gen, ackWait, maxAckPending, true)
	if err := q2AffConfigMatchesCanonical(&base, idHash, gen); err != nil {
		t.Fatalf("the canonical configuration is not accepted as canonical: %v", err)
	}
	// And it must be accepted for a session it does NOT belong to only if the
	// names really are derived — a comparison that ignored idHash and gen would
	// accept this everywhere.
	if err := q2AffConfigMatchesCanonical(&base, "hgfedcba", gen); err == nil {
		t.Fatal("a configuration built for one session was accepted as canonical for another")
	}

	for _, f := range fields {
		f := f
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			fd := disp[f]
			if fd.reason == "" {
				t.Fatalf("the disposition for %q carries no reason; the reason IS the audit", f)
			}
			if fd.variable {
				if fd.mutate != nil {
					t.Fatalf("%q is dispositioned variable but also carries a mutator", f)
				}
				return
			}
			if fd.mutate == nil {
				t.Fatalf("%q is pinned but carries no mutator, so nothing proves it is checked", f)
			}

			mutated := buildQ2AffineConsumerConfig(idHash, gen, ackWait, maxAckPending, true)
			fd.mutate(&mutated)

			// The mutator must actually change its own field, and only it. A
			// mutator that changed nothing would make the refusal below impossible
			// to fail, and one that changed a second field would credit this field
			// with a refusal another field earned.
			changed := testQ2AffChangedFields(t, &base, &mutated)
			if len(changed) != 1 || changed[0] != f {
				t.Fatalf("the mutator for %q changed %v, want exactly [%s]", f, changed, f)
			}
			if err := q2AffConfigMatchesCanonical(&mutated, idHash, gen); err == nil {
				t.Fatalf("a configuration deviating in %q was accepted as replayable", f)
			}
		})
	}
}

// testQ2AffChangedFields names the top-level ConsumerConfig fields that differ.
func testQ2AffChangedFields(t *testing.T, a, b *ConsumerConfig) []string {
	t.Helper()
	va, vb := reflect.ValueOf(*a), reflect.ValueOf(*b)
	tp := va.Type()
	var out []string
	for i := 0; i < tp.NumField(); i++ {
		if !reflect.DeepEqual(va.Field(i).Interface(), vb.Field(i).Interface()) {
			out = append(out, tp.Field(i).Name)
		}
	}
	return out
}

// The bounds on the three fields the comparison above deliberately accepts from
// the record. Without them a canonical-shaped record could still name a consumer
// that cannot be provisioned — which a contender would discover only by having
// its create rejected, after it had already decided the record was replayable.
func TestQ2AffineVariableFieldBounds(t *testing.T) {
	t.Parallel()
	const idHash, gen = "abcdefgh", "Zq1Zq1Zq1Zq1Zq1Zq1Zq1Z"
	canonical := func(ackWait time.Duration, maxAckPending int) *ConsumerConfig {
		c := buildQ2AffineConsumerConfig(idHash, gen, ackWait, maxAckPending, false)
		return &c
	}
	for _, tc := range []struct {
		name             string
		cfg              *ConsumerConfig
		streamMaxAckPend int
		wantErr          bool
	}{
		{"a provisionable pair", canonical(time.Second, 100), 0, false},
		{"at the global cap", canonical(time.Second, mqttMaxAckTotalLimit), 0, false},
		{"exactly at the stream limit", canonical(time.Second, 100), 100, false},
		{"a zero ack_wait", canonical(0, 100), 0, true},
		{"a negative ack_wait", canonical(-time.Second, 100), 0, true},
		{"a zero max_ack_pending", canonical(time.Second, 0), 0, true},
		{"a negative max_ack_pending", canonical(time.Second, -1), 0, true},
		{"above the global cap", canonical(time.Second, mqttMaxAckTotalLimit+1), 0, true},
		{"above the stream limit", canonical(time.Second, 101), 100, true},
		// A negative stream limit reads as unlimited: the broker option it would
		// have to exceed is a uint16, so no record can be out of bounds against it.
		{"a negative stream limit imposes nothing", canonical(time.Second, 100), -1, false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := q2AffVariableFieldsInBounds(tc.cfg, tc.streamMaxAckPend)
			if tc.wantErr && err == nil {
				t.Fatalf("expected %s to be refused", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected %s to be accepted, got %v", tc.name, err)
			}
		})
	}
}

// One constructor, asserted by ENTRY rather than by result.
//
// The property is that both writers of an affine consumer configuration go
// through buildQ2AffineConsumerConfig. No assertion about the configurations
// themselves can establish it: a literal copied to one call site produces bytes
// identical to the constructor's, so the two are indistinguishable downstream.
// Only observing that the constructor was entered from that site can tell them
// apart, which is what the hook here does.
//
// It matters because the contender decides whether a persisted config is
// replayable by comparing it WHOLE against what the constructor returns. A second
// literal anywhere is a second canonical shape, and a record written through one
// would be refused by the other — the session would simply stop being resumable
// on behalf of anyone.
//
// Deliberately NOT parallel: mqttTestQ2AffineConfigBuiltHookV is process-global,
// so a test running beside this one would have its own provisioning counted here.
func TestQ2AffineConsumerConfigHasOneConstructor(t *testing.T) {
	var mu sync.Mutex
	callers := map[string]int{}
	hook := func(caller string) {
		mu.Lock()
		callers[caller]++
		mu.Unlock()
	}
	mqttTestQ2AffineConfigBuiltHookV.Store(&hook)
	t.Cleanup(func() { mqttTestQ2AffineConfigBuiltHookV.Store(nil) })

	// One stage drives both sites: the connected member provisions its own affine
	// path, and then restores the absent winner's on its behalf.
	st := testQ2AffStageContender(t, "onector", nil, nil, nil)
	st.deleteWinnerDurable(t)
	testQ2AffAwaitMaterialized(t, st)

	mu.Lock()
	seen := map[string]int{}
	for k, v := range callers {
		seen[k] = v
	}
	mu.Unlock()

	for _, want := range []string{"ensureQ2AffineConsumerSubscription", "materializeQ2AffOnBehalf"} {
		if seen[want] == 0 {
			t.Fatalf("%s did not build its configuration through the shared constructor; entries seen: %v", want, seen)
		}
	}
	// And no OTHER production caller has appeared. Test helpers legitimately build
	// one to stage a record, so they are named and excluded rather than letting the
	// check degrade to "at least two".
	for caller := range seen {
		switch caller {
		// q2AffConfigMatchesCanonical is the third legitimate production caller
		// and the reason the other two must be exclusive: it builds the canonical
		// config to compare a persisted one AGAINST, so it defines what "canonical"
		// means. A writer that bypassed the constructor would be measured against
		// this one and refused.
		case "ensureQ2AffineConsumerSubscription", "materializeQ2AffOnBehalf",
			"q2AffConfigMatchesCanonical",
			"testQ2AffSyntheticWinnerWith", "testQ2AffineConfigDispositions":
		default:
			if !strings.HasPrefix(caller, "testQ2Aff") && !strings.HasPrefix(caller, "TestQ2Aff") {
				t.Fatalf("an unexpected call site builds an affine consumer configuration: %q (all: %v)", caller, seen)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// F-064 — the owner recovers from a storage conflict it did not cause
// ---------------------------------------------------------------------------

// testQ2AffStrandAffinePathAcrossReload leaves a session with an undelivered
// affine copy (undelivered here is a FACT of the fixture's own staging, not an
// inference from an absent copy), an affine durable created under the OLD MQTT.ConsumerMemoryStorage,
// a record naming both, and NO saved shared filter — then restarts the MQTT layer
// with the option flipped, which is what an operator reload looks like from the
// broker's side.
//
// No shared filter, deliberately: with one, the CONNECT restoration pass
// provisions first and the resume route under test is never reached.
func testQ2AffStrandAffinePathAcrossReload(t *testing.T, b *testQ2AffBroker, id, payload string) (idHash string, oldStorage bool) {
	t.Helper()
	c, r := b.member(t, id, 300)
	b.bind(t, id)
	idHash = getHash(id)

	b.publish(t, 1, payload)
	if _, _, _, _, ok := testMQTTTryReadPublishV5(r, 10*time.Second); !ok {
		t.Fatal("the sole group member never received the delivery this fixture must strand")
	}
	b.unsubscribe(t, c, r, 2, []string{"$share/" + b.group + "/" + b.topic})
	c.Close()

	oldStorage = b.o.MQTT.ConsumerMemoryStorage
	b.o.MQTT.ConsumerMemoryStorage = !oldStorage
	testQ2AffRestartSameStore(t, b)

	ps := testQ2AffReadRecordAfterRestart(t, b, id+"-reader", idHash)
	// Anti-vacuity, three ways. Without a generation there is nothing to
	// re-establish; with a shared filter the wrong route runs; and if the option
	// did not actually reach getOpts() no conflict arises and the whole fixture
	// passes for the wrong reason.
	if ps.Q2AffGen == _EMPTY_ || ps.Q2Aff == nil {
		t.Fatalf("the record names no affine path to re-establish: %+v", ps)
	}
	if len(ps.SharedSubs) != 0 {
		t.Fatalf("a shared filter survived, so the restoration route would run instead: %+v", ps.SharedSubs)
	}
	if ps.Q2Aff.MemoryStorage != oldStorage {
		t.Fatalf("the record's persisted storage is %v, want the pre-reload %v", ps.Q2Aff.MemoryStorage, oldStorage)
	}
	if got := b.s.getOpts().MQTT.ConsumerMemoryStorage; got == oldStorage {
		t.Fatalf("the reload did not reach getOpts(): consumer_memory_storage is still %v", got)
	}
	// And the durable really is still there, carrying the old type — the conflict
	// this fixture is about cannot happen against an absent one.
	ci, err := b.jsa.lookupConsumer(mqttOutStreamName, ps.Q2Aff.Durable)
	if err != nil || ci == nil || ci.Config == nil {
		t.Fatalf("the affine durable %q did not survive the restart: %v", ps.Q2Aff.Durable, err)
	}
	if ci.Config.MemoryStorage != oldStorage {
		t.Fatalf("the surviving durable's storage is %v, want the pre-reload %v", ci.Config.MemoryStorage, oldStorage)
	}
	return idHash, oldStorage
}

// testQ2AffCountLookups installs a COUNTING PASSTHROUGH on the affine consumer
// lookup seam. Counting matters as much as the count: the owner's provisioning
// performs no lookup, so any lookup observed here belongs to the recovery, and
// "exactly one" is what pins the retry bound.
func testQ2AffCountLookups(t *testing.T, b *testQ2AffBroker) func() int32 {
	t.Helper()
	var n atomic.Int32
	jsa := b.jsa
	b.asm.mu.Lock()
	b.asm.testQ2AffConsumerLookup = func(durable string) (*ConsumerInfo, error) {
		n.Add(1)
		return jsa.lookupConsumer(mqttOutStreamName, durable)
	}
	b.asm.mu.Unlock()
	return n.Load
}

// subSharedTolerant sends a shared QoS 2 SUBSCRIBE and returns the SUBACK's
// reason codes and properties, skipping PUBLISH packets that arrive first.
//
// The interleaving is real rather than defensive. ensureQ2AffineConsumerSubscription
// installs the delivery subscription BEFORE it creates the durable, so where the
// durable already exists and holds a pending copy, that copy is pushed to the
// client the moment the subscription appears — which is before the create can
// fail. testMQTTSubV5PropsEx treats anything that is not a SUBACK as fatal, so a
// fixture that drives provisioning against a durable with pending state needs
// this instead.
func (b *testQ2AffBroker) subSharedTolerant(t *testing.T, c net.Conn, r *mqttReader, pi uint16, topic string) ([]byte, *mqttProperties) {
	t.Helper()
	vh := newMQTTWriter(0)
	vh.WriteUint16(pi)
	vh.WriteVarInt(0)
	vh.WriteBytes([]byte(topic))
	vh.WriteByte(2)
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("writing SUBSCRIBE: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r, time.Second)
		if !ready {
			continue
		}
		start := r.pos
		switch pb & mqttPacketMask {
		case mqttPacketSubAck:
			rpi, err := r.readUint16("suback pi")
			if err != nil || rpi != pi {
				t.Fatalf("SUBACK for packet identifier %d, want %d (err=%v)", rpi, pi, err)
			}
			props, err := r.readProperties(mqttPacketSubAck)
			if err != nil {
				t.Fatalf("reading SUBACK properties: %v", err)
			}
			var codes []byte
			for r.pos-start < pl {
				rc, err := r.readByte("suback reason code")
				if err != nil {
					t.Fatalf("reading a SUBACK reason code: %v", err)
				}
				codes = append(codes, rc)
			}
			return codes, props
		case mqttPacketPub:
			r.pos = start + pl // a copy the pre-create subscription let through
		default:
			t.Fatalf("unexpected packet 0x%x while awaiting SUBACK", pb&mqttPacketMask)
		}
	}
	t.Fatal("no SUBACK arrived")
	return nil, nil
}

// The owner recovers from a storage conflict it did not cause.
//
// MemoryStorage is immutable on a JetStream consumer, and the owner rebuilds its
// create request from the CURRENT broker option while the durable keeps whatever
// type it was made with. So an operator who reloads consumer_memory_storage
// breaks affine resume for every session whose durable predates the reload —
// permanently, since every later resume rebuilds the same rejected request.
//
// That defect is PRE-EXISTING and reachable with no contender anywhere, which is
// why this fixture stages none. It is also what makes on-behalf restoration safe
// to add: without the recovery, a contender that legitimately creates the durable
// from the winner's persisted value would leave the owner in exactly this state,
// and F-064 would have introduced the very thing it is here to prevent.
func TestQ2AffOwnerRecoversFromAStorageConflictItDidNotCause(t *testing.T) {
	t.Parallel()

	// The reload, with no contender at all. This needs an EXISTING durable: with
	// none, the owner simply creates one under the new option and no conflict
	// arises, so an absent-durable staging would assert nothing.
	t.Run("an operator reload with no contender anywhere", func(t *testing.T) {
		t.Parallel()
		b := testQ2AffSetup(t, "ownrec")
		idHash, oldStorage := testQ2AffStrandAffinePathAcrossReload(t, b, "ownrecA", "recovered")
		lookups := testQ2AffCountLookups(t, b)

		cr, rr := b.reconnect(t, "ownrecA", 300)
		defer cr.Close()

		// The delivery is the oracle. A resume that failed logs and carries on, so
		// nothing on the wire says "refused" — only the stranded copy arriving says
		// the durable was re-established.
		_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rr, 10*time.Second)
		if !ok {
			t.Fatal("the resume did not recover from the storage conflict: the stranded copy was never redelivered")
		}
		if qos != 2 || string(payload) != "recovered" {
			t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
		}
		b.completeExchange(t, cr, rr, pi)

		sess := testMQTTGetClient(t, b.s, "ownrecA").mqtt.sess
		sess.mu.Lock()
		ready, cfg := sess.q2AffReady, sess.q2AffConsumer
		sess.mu.Unlock()
		if !ready {
			t.Fatal("the affine readiness gate never opened after the recovery")
		}
		// The session's view — which is what the record persists — must name what
		// was actually created, not the request that was refused.
		if cfg == nil || cfg.MemoryStorage != oldStorage {
			t.Fatalf("the session kept the REFUSED request's storage: got %+v, want MemoryStorage=%v", cfg, oldStorage)
		}
		// Exactly one, which is the bound. Zero would mean the recovery never ran
		// and the delivery came from somewhere else; more than one would mean it
		// loops.
		if n := lookups(); n != 1 {
			t.Fatalf("the recovery performed %d affine consumer lookup(s), want exactly 1", n)
		}
		_ = idHash
	})

	// The recovery's own lookup fails. The caller must be told why the CREATE
	// failed, not why the recovery could not run — the lookup's error is never
	// substituted for the original.
	//
	// Driven through a SUBSCRIBE rather than the resume, deliberately: a resume
	// failure is only logged, so the distinction between the two errors is not
	// observable there. A shared SUBSCRIBE reaches the same function and puts the
	// reason on the SUBACK, where a client can see it.
	t.Run("the recovery's own lookup fails", func(t *testing.T) {
		t.Parallel()
		b := testQ2AffSetup(t, "ownrecf")
		testQ2AffStrandAffinePathAcrossReload(t, b, "ownrecfA", "notrecovered")

		var lookups atomic.Int32
		b.asm.mu.Lock()
		b.asm.testQ2AffConsumerLookup = func(string) (*ConsumerInfo, error) {
			lookups.Add(1)
			return nil, fmt.Errorf("test: the affine consumer lookup is unavailable")
		}
		b.asm.mu.Unlock()

		cr, rr := b.reconnect(t, "ownrecfA", 300)
		defer cr.Close()

		codes, props := b.subSharedTolerant(t, cr, rr, 1, "$share/"+b.group+"/"+b.topic)
		if len(codes) != 1 || codes[0] < mqttSubAckFailure {
			t.Fatalf("expected the shared QoS 2 subscribe to fail while the recovery cannot run, got %v", codes)
		}
		if props == nil {
			t.Fatal("the SUBACK carried no properties, so no reason reached the client")
		}
		// THE assertion: the reported failure is the create's, not the lookup's.
		if !strings.Contains(props.reasonString, "storage type can not be updated") {
			t.Fatalf("the SUBACK reported %q; it must report the original create failure", props.reasonString)
		}
		if strings.Contains(props.reasonString, "lookup is unavailable") {
			t.Fatalf("the recovery's own lookup error was substituted for the create failure: %q", props.reasonString)
		}
		if n := lookups.Load(); n == 0 {
			t.Fatal("the recovery never attempted its lookup, so this subtest exercised nothing")
		}

		// The durable is NOT torn down: this call did not mint the generation, so
		// its rollback undoes only what it did. Asserting this is what keeps the
		// failure recoverable rather than terminal.
		ps := testQ2AffAwaitStableRecord(t, b, getHash("ownrecfA"))
		if ci, err := b.jsa.lookupConsumer(mqttOutStreamName, ps.Q2Aff.Durable); err != nil || ci == nil {
			t.Fatalf("the failed recovery destroyed the affine durable %q: %v", ps.Q2Aff.Durable, err)
		}

		// And it really is recoverable: with the lookup working, the same subscribe
		// succeeds and the stranded copy arrives.
		b.asm.mu.Lock()
		b.asm.testQ2AffConsumerLookup = nil
		b.asm.mu.Unlock()
		codes, _ = b.subSharedTolerant(t, cr, rr, 2, "$share/"+b.group+"/"+b.topic)
		if len(codes) != 1 || codes[0] != 2 {
			t.Fatalf("the subscribe did not succeed once the recovery could run: %v", codes)
		}
		_, payload, _, pi, ok := testMQTTTryReadPublishV5(rr, 10*time.Second)
		if !ok {
			t.Fatal("the stranded copy was never redelivered after the recovery succeeded")
		}
		if string(payload) != "notrecovered" {
			t.Fatalf("unexpected delivery: payload=%q", payload)
		}
		b.completeExchange(t, cr, rr, pi)
	})

	// The owner's recovery must not MAKE things worse than the failure it is
	// recovering from, and this is the case where retrying would.
	//
	// A create refused for an immutable mismatch mutates nothing: JetStream checks
	// convergence before it applies anything. The retry, being accepted, does apply
	// — and a create-or-update carrying a zero InactiveThreshold has the stream's
	// limit substituted into it, arming an existing durable's deletion timer. So
	// under a stream mutated after startup, retrying could leave a session that is
	// still unresumable AND a durable that will now be reaped with the copies it
	// holds. Today the mismatch simply fails and the durable survives untouched.
	t.Run("a stream mutated after startup suspends the recovery", func(t *testing.T) {
		t.Parallel()
		b := testQ2AffSetup(t, "ownrecl")
		idHash, oldStorage := testQ2AffStrandAffinePathAcrossReload(t, b, "ownreclA", "notreaped")
		ps := testQ2AffAwaitStableRecord(t, b, idHash)

		si, err := b.jsa.lookupStream(mqttOutStreamName)
		if err != nil || si == nil {
			t.Fatalf("looking up %s: %v", mqttOutStreamName, err)
		}
		cfg := si.Config
		cfg.ConsumerLimits.InactiveThreshold = 30 * time.Minute
		if _, err := b.jsa.updateStream(&cfg); err != nil {
			t.Fatalf("imposing a consumer inactivity limit on %s: %v", mqttOutStreamName, err)
		}

		cr, _ := b.reconnect(t, "ownreclA", 300)
		defer cr.Close()
		// Give the resume time to attempt and fail. Deliberately NOT asserted by
		// "nothing was delivered": ensureQ2AffineConsumerSubscription subscribes to
		// the deliver subject BEFORE it creates the durable, and the durable still
		// exists holding the stranded copy, so that copy is pushed whether or not
		// provisioning then succeeds.
		time.Sleep(2 * time.Second)
		sess := testMQTTGetClient(t, b.s, "ownreclA").mqtt.sess
		sess.mu.Lock()
		ready := sess.q2AffReady
		sess.mu.Unlock()
		if ready {
			t.Fatal("the recovery ran under a stream limit that would reap the durable it repaired")
		}

		// THE assertion: the durable is exactly as the failing create left it —
		// still carrying the old storage type, and still with NO deletion timer.
		// A retry would have armed one.
		ci, err := b.jsa.lookupConsumer(mqttOutStreamName, ps.Q2Aff.Durable)
		if err != nil || ci == nil || ci.Config == nil {
			t.Fatalf("the durable did not survive the refused recovery: %v", err)
		}
		if ci.Config.InactiveThreshold != 0 {
			t.Fatalf("the recovery armed a deletion timer on a durable the failing create had left alone: %v", ci.Config.InactiveThreshold)
		}
		if ci.Config.MemoryStorage != oldStorage {
			t.Fatalf("the durable's storage changed to %v, want the untouched %v", ci.Config.MemoryStorage, oldStorage)
		}
	})

	// The negative control, and the reason the classifier matches one exact
	// flattened cause rather than "the create failed".
	//
	// JetStream forbids changing several consumer fields, and reports every one of
	// them through the same generic error 10012 with the reason flattened into its
	// description. Only the storage-type cause is recoverable: a durable whose
	// DELIVER POLICY differs is not one this broker can converge, and looking it up
	// to retry would be a round trip that cannot change the outcome — the shape a
	// classifier matching any create error would produce.
	t.Run("a different immutable mismatch does not enter the retry", func(t *testing.T) {
		t.Parallel()
		b := testQ2AffSetup(t, "ownrecn")
		c, r := b.member(t, "ownrecnA", 300)
		b.bind(t, "ownrecnA")
		idHash := getHash("ownrecnA")
		b.unsubscribe(t, c, r, 2, []string{"$share/" + b.group + "/" + b.topic})
		c.Close()
		testQ2AffRestartSameStore(t, b)

		ps := testQ2AffReadRecordAfterRestart(t, b, "ownrecn-reader", idHash)
		if ps.Q2AffGen == _EMPTY_ || ps.Q2Aff == nil {
			t.Fatalf("the record names no affine path: %+v", ps)
		}

		// Replace the durable with one that differs ONLY in an immutable field
		// that is not the storage type, so the create below fails with the same
		// outer error and a different cause.
		if _, err := b.jsa.deleteConsumer(mqttOutStreamName, ps.Q2Aff.Durable, false); err != nil {
			t.Fatalf("removing the affine durable: %v", err)
		}
		clash := buildQ2AffineConsumerConfig(idHash, ps.Q2AffGen, ps.Q2Aff.AckWait, ps.Q2Aff.MaxAckPending, ps.Q2Aff.MemoryStorage)
		clash.DeliverPolicy = natsserver.DeliverAll
		if _, err := b.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttOutStreamName, Config: clash}); err != nil {
			t.Fatalf("staging a durable with a clashing deliver policy: %v", err)
		}

		lookups := testQ2AffCountLookups(t, b)
		cr, rr := b.reconnect(t, "ownrecnA", 300)
		defer cr.Close()
		codes, props := b.subSharedTolerant(t, cr, rr, 1, "$share/"+b.group+"/"+b.topic)
		if len(codes) != 1 || codes[0] < mqttSubAckFailure {
			t.Fatalf("expected the shared QoS 2 subscribe to fail against an unconvergeable durable, got %v", codes)
		}
		if props == nil || !strings.Contains(props.reasonString, "deliver policy can not be updated") {
			t.Fatalf("the SUBACK did not report the deliver-policy conflict: %+v", props)
		}
		// THE discriminating assertion. Both the correct classifier and one that
		// matched any create error end in the same refusal; only the absence of a
		// lookup says the retry was never entered.
		if n := lookups(); n != 0 {
			t.Fatalf("a non-storage immutable mismatch entered the recovery: %d lookup(s)", n)
		}
	})
}

// ---------------------------------------------------------------------------
// F-064 — on-behalf restoration of a destroyed affine consumer
// ---------------------------------------------------------------------------

// testQ2AffContenderStage is a claimed assignment owned by an absent winner, with
// exactly one connected member to contend for it.
//
// A SYNTHETIC winner — a record and a durable with no client — rather than a
// second member that disconnects. The reason is the one testQ2AffSyntheticWinner
// documents: a winner with a client competes for the group delivery, so the mover
// can turn out to be the winner itself and take the own-claim path, consuming the
// one-shot seams these fixtures stage and letting them pass with nothing entered.
type testQ2AffContenderStage struct {
	b       *testQ2AffBroker
	winner  string // the winner's idHash
	gen     string
	durable string
	subject string // the claim subject under test

	// creates returns every create-or-update the contender issued, in order.
	// Counting them is how "refused with no create attempted" is asserted, which
	// most of D1's refusals are otherwise indistinguishable from.
	creates func() []ConsumerConfig
	// results returns every resolved move disposition for the claim under test.
	results func() []mqttQ2AffMoveResult
	lookups func() int32

	// setCreate and setLookup replace the INNER half of the seams the stage owns.
	// A fixture must go through these rather than assigning the asm fields, which
	// would clobber the stage's counters and silently make creates() report zero —
	// a failure that reads as "no create was attempted" when in fact none was
	// observed.
	setCreate func(func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error))
	setLookup func(func(string) (*ConsumerInfo, error))
	// realCreate issues a create-or-update straight to JetStream, bypassing the
	// stage's counter. A fixture simulating a RIVAL uses it, so the rival's create
	// is not counted as one of the contender's attempts.
	realCreate func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error)

	// release lifts the copy-store hold the stage installs before publishing.
	// Every fixture must call it, directly or through deleteWinnerDurable.
	//
	// The hold exists because the contender materializes as soon as the claim is
	// elected, which RACES a fixture that then destroys the winner's durable: when
	// materialization wins, the copy is stored, the delete drops it, and its
	// Nats-Msg-Id survives in the duplicate window so the restoration's own store
	// keeps nothing. That is residual 3 arriving in a fixture that does not own it,
	// and it is intermittent — it surfaced once in a full-suite run after eleven
	// clean targeted ones. Holding the store removes the racer instead of racing
	// it.
	release func(t *testing.T)
}

// stageContender wires the seams, stages the winner, seeds the claim and
// publishes. record and consumer deviate the staged record and durable; either
// may be nil. perturb runs after staging and before the publish, which is where a
// fixture changes the stream or installs its own seam.
func testQ2AffStageContender(t *testing.T, name string,
	record func(*mqttPersistedSession), consumer func(*ConsumerConfig),
	perturb func(st *testQ2AffContenderStage)) *testQ2AffContenderStage {
	return testQ2AffStageContenderSeeded(t, name, record, consumer, perturb, nil)
}

// testQ2AffStageContenderSeeded additionally adjusts the elected CLAIM before it
// is written, which is how a phase this broker would not otherwise produce on
// demand — a materialized claim whose source was never acknowledged — is staged.
func testQ2AffStageContenderSeeded(t *testing.T, name string,
	record func(*mqttPersistedSession), consumer func(*ConsumerConfig),
	perturb func(st *testQ2AffContenderStage), seed func(*mqttQ2AffClaim)) *testQ2AffContenderStage {
	t.Helper()
	b := testQ2AffSetup(t, name)
	c, _ := b.member(t, name+"A", 300)
	t.Cleanup(func() { c.Close() })
	b.bind(t, name+"A")

	wHash, wGen := testQ2AffSyntheticWinnerWith(t, b, name+"W", record, consumer)
	st := &testQ2AffContenderStage{b: b, winner: wHash, gen: wGen, durable: affDurableFor(wHash, wGen)}

	var (
		mu      sync.Mutex
		creates []ConsumerConfig
		// Keyed by subject and filtered at READ time, not written into a
		// single-claim slice at hook time. Two reasons, and both are defects the
		// first version had: the hook runs on a server goroutine while the test
		// goroutine is still assigning st.subject, which is a data race; and the
		// first move can resolve before the seeding helper has reported the
		// subject at all, so a hook that filtered early would silently drop it.
		results map[string][]mqttQ2AffMoveResult
	)
	results = map[string][]mqttQ2AffMoveResult{}
	var lookups atomic.Int32
	var held atomic.Bool
	held.Store(true)
	jsa := b.jsa
	innerCreate := func(cfg *ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
		return jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttOutStreamName, Config: *cfg})
	}
	innerLookup := func(durable string) (*ConsumerInfo, error) {
		return jsa.lookupConsumer(mqttOutStreamName, durable)
	}
	b.asm.mu.Lock()
	b.asm.testQ2AffConsumerCreate = func(cfg *ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
		mu.Lock()
		creates = append(creates, *cfg)
		inner := innerCreate
		mu.Unlock()
		return inner(cfg)
	}
	b.asm.testQ2AffConsumerLookup = func(durable string) (*ConsumerInfo, error) {
		lookups.Add(1)
		mu.Lock()
		inner := innerLookup
		mu.Unlock()
		return inner(durable)
	}
	b.asm.testQ2AffCopyStore = func(subject string, hdrLen int, msg []byte) (uint64, error) {
		if held.Load() {
			return 0, fmt.Errorf("the copy store is held by the test until its staging is complete")
		}
		return testQ2AffRealCopyStore(b, subject, hdrLen, msg)
	}
	b.asm.testQ2AffMoveDone = func(subject string, res mqttQ2AffMoveResult) {
		mu.Lock()
		results[subject] = append(results[subject], res)
		mu.Unlock()
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffConsumerCreate, b.asm.testQ2AffConsumerLookup, b.asm.testQ2AffMoveDone = nil, nil, nil
		b.asm.testQ2AffCopyStore = nil
		b.asm.mu.Unlock()
	})
	st.creates = func() []ConsumerConfig {
		mu.Lock()
		defer mu.Unlock()
		return append([]ConsumerConfig(nil), creates...)
	}
	st.results = func() []mqttQ2AffMoveResult {
		mu.Lock()
		defer mu.Unlock()
		return append([]mqttQ2AffMoveResult(nil), results[st.subject]...)
	}
	st.lookups = lookups.Load
	st.setCreate = func(f func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error)) {
		mu.Lock()
		innerCreate = f
		mu.Unlock()
	}
	st.setLookup = func(f func(string) (*ConsumerInfo, error)) {
		mu.Lock()
		innerLookup = f
		mu.Unlock()
	}
	// The unhooked create, for a fixture that wants to drive a real one from
	// inside its own replacement.
	st.realCreate = innerCreate
	st.release = func(t *testing.T) {
		t.Helper()
		held.Store(false)
	}

	if perturb != nil {
		perturb(st)
	}

	seeded := testQ2AffSeedElectedClaim(t, b, func(c *mqttQ2AffClaim) {
		c.Winner, c.Gen = wHash, wGen
		c.Nonce = testQ2AffFixtureNonce
		c.CopySubject = mqttQ2AffCopySubject(wHash, wGen, c.ClaimHash, testQ2AffFixtureNonce)
		c.AffDurable = st.durable
		if seed != nil {
			seed(c)
		}
	})
	b.publish(t, 1, name+"-payload")
	st.subject = testQ2AffAwaitSeeded(t, seeded)
	return st
}

// awaitRefusal waits until the contender has resolved the claim as DEFERRED at
// least once — which is the observable form of "proved nothing, wrote nothing,
// acked nothing" — and fails if it ever resolves any other way.
func (st *testQ2AffContenderStage) awaitRefusal(t *testing.T, what string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range st.results() {
			if r != mqttQ2AffMoveDeferred {
				t.Fatalf("%s: the contender resolved the assignment as %v; it must refuse", what, r)
			}
		}
		if len(st.results()) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: the contender never resolved the assignment at all, so nothing was exercised", what)
}

// assertNoCreate is the discriminating half of most refusal fixtures. Asserting
// only the refusal does not distinguish "refused before touching the durable"
// from "issued a create-or-update that was then rejected" — and for the
// inactivity-threshold arm those differ by whether a live durable's deletion
// timer was restarted.
func (st *testQ2AffContenderStage) assertNoCreate(t *testing.T, what string) {
	t.Helper()
	if got := st.creates(); len(got) != 0 {
		t.Fatalf("%s: the contender issued %d create-or-update(s) before refusing: %+v", what, len(got), got)
	}
}

// assertCopyAbsent proves nothing was materialized. Used with assertNoSourceAck.
func (st *testQ2AffContenderStage) assertCopyAbsent(t *testing.T, what string) {
	t.Helper()
	claim := testQ2AffLoadClaim(t, st.b, st.subject)
	if claim.Phase != mqttQ2AffClaimed {
		t.Fatalf("%s: the claim advanced to %v; nothing should have been materialized", what, claim.Phase)
	}
	if claim.SourceAcked {
		t.Fatalf("%s: the contender acknowledged the group source", what)
	}
	if state, _ := st.b.asm.loadQ2AffCopy(claim.CopySubject); state == mqttQ2AffCopyPresent {
		t.Fatalf("%s: a copy was stored despite the refusal", what)
	}
}

// A record the broker cannot replay exactly is refused, and no create is
// attempted for it.
//
// The comparison behind this is whole-struct rather than a list of interesting
// fields, so the table below is not a sample of what happens to be checked — it
// is a sample of a closed rule. TestQ2AffineConsumerConfigFieldCoverage is what
// keeps the rule closed; this fixture is what proves it reaches a real contender.
func TestQ2AffContenderRefusesAnUnreplayableConsumerConfig(t *testing.T) {
	t.Parallel()

	// The two shapes that are not merely a deviating field.
	t.Run("no consumer configuration at all", func(t *testing.T) {
		t.Parallel()
		st := testQ2AffStageContender(t, "unrepnil",
			func(ps *mqttPersistedSession) { ps.Q2Aff = nil }, nil, nil)
		st.release(t)
		st.awaitRefusal(t, "a record carrying a generation but no consumer configuration")
		st.assertNoCreate(t, "a nil consumer configuration")
		st.assertCopyAbsent(t, "a nil consumer configuration")
	})

	t.Run("a names-only stub", func(t *testing.T) {
		t.Parallel()
		// The shape restoreQ2AffineUnwind really produces: three names and nothing
		// else, which buildPersistedSessionLocked then persists verbatim. Its zero
		// values are not incomplete but WRONG — AckPolicy zero is AckNone, which
		// would ack on send, and DeliverPolicy zero is DeliverAll, which would
		// replay the subject from its start.
		st := testQ2AffStageContender(t, "unrepstub",
			func(ps *mqttPersistedSession) {
				ps.Q2Aff = &ConsumerConfig{
					Durable:        ps.Q2Aff.Durable,
					FilterSubject:  ps.Q2Aff.FilterSubject,
					DeliverSubject: ps.Q2Aff.DeliverSubject,
				}
			}, nil, nil)
		st.release(t)
		st.awaitRefusal(t, "a names-only consumer configuration")
		st.assertNoCreate(t, "a names-only consumer configuration")
		st.assertCopyAbsent(t, "a names-only consumer configuration")
	})

	// A deviation in any single field, and a variable field outside its bound.
	for _, tc := range []struct {
		name   string
		deform func(*ConsumerConfig)
	}{
		{"a delivery cap", func(c *ConsumerConfig) { c.MaxDeliver = 1 }},
		{"headers only", func(c *ConsumerConfig) { c.HeadersOnly = true }},
		{"a queue group", func(c *ConsumerConfig) { c.DeliverGroup = "g" }},
		{"a pause", func(c *ConsumerConfig) { tm := time.Unix(1755000000, 0).UTC(); c.PauseUntil = &tm }},
		{"flow control and heartbeats", func(c *ConsumerConfig) { c.FlowControl = true; c.Heartbeat = time.Second }},
		{"a negative max_ack_pending", func(c *ConsumerConfig) { c.MaxAckPending = -1 }},
		{"a max_ack_pending above the global cap", func(c *ConsumerConfig) { c.MaxAckPending = mqttMaxAckTotalLimit + 1 }},
		{"a zero ack_wait", func(c *ConsumerConfig) { c.AckWait = 0 }},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := testQ2AffStageContender(t, "unrep"+strconv.Itoa(len(tc.name)),
				func(ps *mqttPersistedSession) { tc.deform(ps.Q2Aff) }, nil, nil)
			st.release(t)
			st.awaitRefusal(t, tc.name)
			st.assertNoCreate(t, tc.name)
			st.assertCopyAbsent(t, tc.name)
		})
	}
}

// A record whose max_ack_pending the stream would reject is refused BEFORE any
// create, not by letting JetStream reject it.
//
// The distinction is the whole assertion. JetStream would refuse an over-limit
// consumer by itself, so a fixture that only checked the final state would pass
// against an implementation with no pre-check at all.
func TestQ2AffContenderRefusesToRestoreBeyondTheStreamAckLimit(t *testing.T) {
	t.Parallel()
	// The limit is set ABOVE the consumers that already exist and the record is
	// deviated above the limit, rather than the other way round: JetStream refuses
	// a stream update that would violate a live consumer (error 10069), so a limit
	// low enough to catch the default 1024 cannot be imposed at all.
	const streamLimit, recordWants = 2048, 4096
	st := testQ2AffStageContender(t, "acklimit",
		func(ps *mqttPersistedSession) { ps.Q2Aff.MaxAckPending = recordWants }, nil,
		func(st *testQ2AffContenderStage) {
			si, err := st.b.jsa.lookupStream(mqttOutStreamName)
			if err != nil || si == nil {
				t.Fatalf("looking up %s: %v", mqttOutStreamName, err)
			}
			cfg := si.Config
			cfg.ConsumerLimits.MaxAckPending = streamLimit
			if _, err := st.b.jsa.updateStream(&cfg); err != nil {
				t.Fatalf("imposing a consumer ack limit on %s: %v", mqttOutStreamName, err)
			}
		})
	st.release(t)
	// Anti-vacuity: the record must be inside the GLOBAL cap, or the refusal below
	// would be the global bound firing rather than the stream limit.
	if recordWants > mqttMaxAckTotalLimit {
		t.Fatalf("this fixture needs a value inside the global cap %d, got %d", mqttMaxAckTotalLimit, recordWants)
	}
	st.awaitRefusal(t, "a record above the stream's consumer ack limit")
	st.assertNoCreate(t, "a record above the stream's consumer ack limit")
	st.assertCopyAbsent(t, "a record above the stream's consumer ack limit")
}

// The inactivity-threshold arms, whose oracles differ and must not be conflated.
func TestQ2AffContenderRefusesAConsumerTheStreamWouldReap(t *testing.T) {
	t.Parallel()

	// Both signs of a limit imposed BEFORE the create. The oracle is that no
	// create-or-update was issued AT ALL — because for an already-present durable
	// a create-or-update applies the substituted threshold first and restarts the
	// server's deletion timer, so refusing afterwards refuses a durable the
	// refusal itself just made reapable.
	//
	// The negative case is what catches a `> 0` predicate. InactiveThreshold is a
	// signed Duration; the server substitutes the stream limit unconditionally and
	// its own limit rejection fires only for a positive limit, so a negative one
	// would slip past `> 0` and mutate the durable before the returned-config arm
	// refused it.
	for _, tc := range []struct {
		name  string
		limit time.Duration
	}{
		{"a positive limit", 5 * time.Minute},
		{"a negative limit", -5 * time.Minute},
	} {
		tc := tc
		t.Run("imposed before the create: "+tc.name, func(t *testing.T) {
			t.Parallel()
			st := testQ2AffStageContender(t, "reap"+strconv.Itoa(int(tc.limit/time.Minute)+9), nil, nil,
				func(st *testQ2AffContenderStage) {
					si, err := st.b.jsa.lookupStream(mqttOutStreamName)
					if err != nil || si == nil {
						t.Fatalf("looking up %s: %v", mqttOutStreamName, err)
					}
					cfg := si.Config
					cfg.ConsumerLimits.InactiveThreshold = tc.limit
					if _, err := st.b.jsa.updateStream(&cfg); err != nil {
						t.Fatalf("imposing a consumer inactivity limit on %s: %v", mqttOutStreamName, err)
					}
				})
			st.release(t)
			st.awaitRefusal(t, tc.name)
			st.assertNoCreate(t, tc.name)
			st.assertCopyAbsent(t, tc.name)

			// And the already-present durable was not touched on the way to
			// refusing: its effective threshold is still zero, so no deletion timer
			// was armed and none was cleared.
			ci, err := st.b.jsa.lookupConsumer(mqttOutStreamName, st.durable)
			if err != nil || ci == nil || ci.Config == nil {
				t.Fatalf("the winner's durable is unreadable after the refusal: %v", err)
			}
			if ci.Config.InactiveThreshold != 0 {
				t.Fatalf("%s: the refusal mutated the durable's inactivity threshold to %v", tc.name, ci.Config.InactiveThreshold)
			}
		})
	}

	// The RACE the pre-check cannot close: a stream update landing between the
	// pre-check and the create, so the returned effective threshold is nonzero.
	// The pre-check is not atomic with the create, and this arm is what catches
	// that.
	//
	// Run in BOTH signs, and the negative one is the point. A positive substituted
	// threshold is refused by `!= 0` and by a `> 0` mutant alike, so it
	// discriminates nothing; only a negative one separates them. The debris differs
	// too — a positive limit arms the server's deletion timer, a negative one
	// clears it and leaves a timerless durable nothing reaps on a threshold — but
	// that is documented in docs/CONFORMANCE.md rather than asserted here, because
	// a positive-only mutant leaves the same timerless durable and the observation
	// would not discriminate either.
	for _, tc := range []struct {
		name  string
		limit time.Duration
	}{
		{"a positive limit", 3 * time.Minute},
		{"a negative limit", -3 * time.Minute},
	} {
		tc := tc
		t.Run("imposed between the pre-check and the create: "+tc.name, func(t *testing.T) {
			t.Parallel()
			st := testQ2AffStageContender(t, "reaprace"+strconv.Itoa(int(tc.limit/time.Minute)+9), nil, nil,
				func(st *testQ2AffContenderStage) {
					// The pre-check reads the stream through this seam and sees a
					// clean one; the create below then lands against a stream that
					// has since acquired the limit.
					jsa := st.b.jsa
					st.b.asm.mu.Lock()
					st.b.asm.testQ2AffOutStreamInfo = func() (*StreamInfo, error) {
						si, err := jsa.lookupStream(mqttOutStreamName)
						if err != nil || si == nil {
							return si, err
						}
						clean := *si
						cfg := si.Config
						cfg.ConsumerLimits.InactiveThreshold = 0
						clean.Config = cfg
						return &clean, nil
					}
					st.b.asm.mu.Unlock()
					t.Cleanup(func() {
						st.b.asm.mu.Lock()
						st.b.asm.testQ2AffOutStreamInfo = nil
						st.b.asm.mu.Unlock()
					})
					si, err := jsa.lookupStream(mqttOutStreamName)
					if err != nil || si == nil {
						t.Fatalf("looking up %s: %v", mqttOutStreamName, err)
					}
					cfg := si.Config
					cfg.ConsumerLimits.InactiveThreshold = tc.limit
					if _, err := jsa.updateStream(&cfg); err != nil {
						t.Fatalf("imposing a consumer inactivity limit on %s: %v", mqttOutStreamName, err)
					}
				})
			st.release(t)
			// The create IS issued here — that is what distinguishes this arm from
			// the pre-check one — and the refusal comes from the returned config.
			deadline := time.Now().Add(15 * time.Second)
			for time.Now().Before(deadline) && len(st.creates()) == 0 {
				time.Sleep(20 * time.Millisecond)
			}
			if len(st.creates()) == 0 {
				t.Fatalf("%s: no create was issued, so the post-create arm was never reached", tc.name)
			}
			st.awaitRefusal(t, tc.name)
			st.assertCopyAbsent(t, tc.name)
		})
	}

	// A stream lookup that FAILS is indeterminate, not "no limits".
	t.Run("the stream lookup fails", func(t *testing.T) {
		t.Parallel()
		st := testQ2AffStageContender(t, "reapsi", nil, nil, func(st *testQ2AffContenderStage) {
			st.b.asm.mu.Lock()
			st.b.asm.testQ2AffOutStreamInfo = func() (*StreamInfo, error) {
				return nil, fmt.Errorf("test: %s is unreadable", mqttOutStreamName)
			}
			st.b.asm.mu.Unlock()
			t.Cleanup(func() {
				st.b.asm.mu.Lock()
				st.b.asm.testQ2AffOutStreamInfo = nil
				st.b.asm.mu.Unlock()
			})
		})
		st.release(t)
		st.awaitRefusal(t, "an unreadable stream")
		st.assertNoCreate(t, "an unreadable stream")
	})
}

// A record carrying a teardown marker is refused, and no create is attempted.
func TestQ2AffContenderRefusesToRestoreUnderTeardown(t *testing.T) {
	t.Parallel()
	st := testQ2AffStageContender(t, "tdown",
		func(ps *mqttPersistedSession) {
			ps.Teardown = &mqttSessTeardown{}
		}, nil, nil)
	st.release(t)
	st.awaitRefusal(t, "a record carrying a teardown marker")
	st.assertNoCreate(t, "a record carrying a teardown marker")
	st.assertCopyAbsent(t, "a record carrying a teardown marker")
}

// A durable whose identity does not match the claim is never materialized onto.
func TestQ2AffContenderRefusesADurableThatIsNotTheClaimsOwn(t *testing.T) {
	t.Parallel()
	st := testQ2AffStageContender(t, "wrongdur", nil,
		func(c *ConsumerConfig) { c.FilterSubject = mqttOutSubjectPrefix + "someone.else" }, nil)
	st.release(t)
	st.awaitRefusal(t, "a durable filtering another session's subject")
	st.assertNoCreate(t, "a durable filtering another session's subject")
	st.assertCopyAbsent(t, "a durable filtering another session's subject")
}

// Storage adoption: the create-or-update carries the DURABLE's immutable storage
// type, never the contender's own local option and never the record's value where
// a durable exists to be adopted.
//
// "The redelivery succeeded" is not the oracle. A contender forcing its own local
// option would hit the immutable rejection and refuse, which is visible; but a
// contender reading the RECORD's value when a durable exists would silently
// diverge whenever the two disagree, and only the carried-storage assertion
// catches that.
func TestQ2AffContenderAdoptsAnExistingConsumersStorage(t *testing.T) {
	t.Parallel()
	// The durable really is memory-backed while the record and the contender's own
	// option both say otherwise, so all three sources are distinguishable.
	st := testQ2AffStageContender(t, "adopt", nil,
		func(c *ConsumerConfig) { c.MemoryStorage = true }, nil)
	st.release(t)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && len(st.creates()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	creates := st.creates()
	if len(creates) == 0 {
		t.Fatal("the contender never issued a create-or-update, so no adoption was exercised")
	}
	if !creates[0].MemoryStorage {
		t.Fatalf("the contender's create carried MemoryStorage=false; it must adopt the durable's own true: %+v", creates[0])
	}
	if st.b.o.MQTT.ConsumerMemoryStorage {
		t.Fatal("this fixture needs the contender's LOCAL option to differ from the durable's, or adoption is indistinguishable from imposing it")
	}

	// The durable's own storage is unchanged, and the assignment completes.
	ci, err := st.b.jsa.lookupConsumer(mqttOutStreamName, st.durable)
	if err != nil || ci == nil || ci.Config == nil {
		t.Fatalf("the winner's durable is unreadable: %v", err)
	}
	if !ci.Config.MemoryStorage {
		t.Fatal("the adoption changed the durable's storage type, which is immutable")
	}
	testQ2AffAwaitMaterialized(t, st)
}

// testQ2AffAwaitMaterialized waits for the claim under test to be materialized on
// the winner's behalf and its group source acknowledged.
func testQ2AffAwaitMaterialized(t *testing.T, st *testQ2AffContenderStage) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		claim := testQ2AffLoadClaim(t, st.b, st.subject)
		if claim.Phase == mqttQ2AffMaterialized && claim.SourceAcked {
			if state, _ := st.b.asm.loadQ2AffCopy(claim.CopySubject); state == mqttQ2AffCopyPresent {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("the assignment was never materialized on the absent winner's behalf: %s", st.b.dumpClaims(t))
}

// An on-behalf restoration restores an EXISTING logical consumer, so it is not
// charged to the contender session's MaxAckPending budget. Charging it would bill
// a contender for somebody else's consumer, once per redelivery.
func TestQ2AffOnBehalfRestorationIsNotChargedToTmaxack(t *testing.T) {
	t.Parallel()
	st := testQ2AffStageContender(t, "notmax", nil, nil, nil)
	st.release(t)
	sess := testMQTTGetClient(t, st.b.s, "notmaxA").mqtt.sess
	sess.mu.Lock()
	before := sess.tmaxack
	sess.mu.Unlock()

	testQ2AffAwaitMaterialized(t, st)

	sess.mu.Lock()
	after := sess.tmaxack
	sess.mu.Unlock()
	if after != before {
		t.Fatalf("the on-behalf restoration charged the contender's ack budget: %d -> %d", before, after)
	}
}

// deleteWinnerDurable destroys the winner's affine consumer out of band, which
// is F-064's premise: CONSUMER.DELETE carries no precondition, and $MQTT_out is
// interest-retained, so this drops every copy the durable held.
func (st *testQ2AffContenderStage) deleteWinnerDurable(t *testing.T) {
	t.Helper()
	if _, err := st.b.jsa.deleteConsumer(mqttOutStreamName, st.durable, false); err != nil {
		t.Fatalf("destroying the winner's affine consumer %q: %v", st.durable, err)
	}
	if _, err := st.b.jsa.lookupConsumer(mqttOutStreamName, st.durable); !IsNatsErr(err, JSConsumerNotFoundErr) {
		t.Fatalf("the winner's durable %q did not go away: %v", st.durable, err)
	}
	// Only now may a copy be written: the durable this fixture is about is gone,
	// and no store has recorded a msg-id that would dedupe the restoration's own.
	st.release(t)
}

// THE headline fixture: a contender restores a destroyed affine consumer on
// behalf of an absent winner, and the group delivery stops stalling.
//
// Before this, nothing owner-independent could RESOLVE such an assignment. The
// dispatcher rereads the claim on every redelivery and the sweep skips any claim
// of a referenced generation, so the source was re-offered forever while holding
// a MaxAckPending slot — enough of them and the shared subscription stops
// delivering for every member.
//
// A REAL member as the winner here, rather than the synthetic record-plus-durable
// the refusal fixtures use, because the closing assertion is [MQTT-4.8.2-4]: the
// winner comes back and the delivery is completed WITH IT. That needs a session
// that really subscribed, since the PUBREL half of the exchange runs off the
// PUBREL consumer its subscription provisions. The winner is disconnected before
// the contention starts, so the racing-mover hazard testQ2AffSyntheticWinner
// avoids does not arise.
func TestQ2AffContenderRestoresADestroyedAffineConsumer(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "restore")
	ca, ra := b.member(t, "restoreA", 300)
	cb, rb := b.member(t, "restoreB", 300)
	defer cb.Close()
	b.bind(t, "restoreA")

	// Hold the COPY STORE, not the claimed->materialized CAS, so the assignment
	// is still `claimed` when its consumer is destroyed and NO copy has ever been
	// written for it.
	//
	// Holding the CAS instead looks equivalent and is not. The copy is stored
	// before that CAS, so a held CAS leaves a copy on the winner's subject; the
	// out-of-band consumer delete then drops it, but the store's Nats-Msg-Id
	// survives in $MQTT_out's duplicate window, and the restoration's own store
	// therefore lands nothing. That is residual 3 — measured here, not assumed —
	// and it would make this fixture fail for a reason it does not own.
	var held atomic.Bool
	held.Store(true)
	b.asm.mu.Lock()
	b.asm.testQ2AffCopyStore = func(subject string, hdrLen int, msg []byte) (uint64, error) {
		if held.Load() {
			return 0, fmt.Errorf("the copy store is held by the test")
		}
		return testQ2AffRealCopyStore(b, subject, hdrLen, msg)
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyStore = nil
		b.asm.mu.Unlock()
	})

	b.publish(t, 1, "restored")
	_, claim := b.waitForClaim(t, "claimed", func(c *mqttQ2AffClaim) bool { return c.Phase == mqttQ2AffClaimed })

	winnerID, winnerConn, otherReader := "restoreA", ca, rb
	if claim.Winner != getHash("restoreA") {
		winnerID, winnerConn, otherReader = "restoreB", cb, ra
	}
	winnerConn.Close()

	// THE premise, out of band: CONSUMER.DELETE carries no precondition, and
	// $MQTT_out is interest-retained, so this drops every copy the durable held.
	if _, err := b.jsa.deleteConsumer(mqttOutStreamName, claim.AffDurable, false); err != nil {
		t.Fatalf("destroying the winner's affine consumer %q: %v", claim.AffDurable, err)
	}
	if _, err := b.jsa.lookupConsumer(mqttOutStreamName, claim.AffDurable); !IsNatsErr(err, JSConsumerNotFoundErr) {
		t.Fatalf("the winner's durable %q did not go away: %v", claim.AffDurable, err)
	}

	held.Store(false)
	b.asm.mu.Lock()
	b.asm.testQ2AffCopyStore = nil
	b.asm.mu.Unlock()

	// The surviving member takes the redelivery and restores what the winner
	// owns. It must not deliver the message itself.
	b.waitForClaim(t, "materialized on behalf of the absent winner", func(c *mqttQ2AffClaim) bool {
		return c.Phase == mqttQ2AffMaterialized && c.Winner == claim.Winner
	})
	if _, pl, _, _, got := testMQTTTryReadPublishV5(otherReader, 1500*time.Millisecond); got {
		t.Fatalf("the contender delivered a message assigned to another session (payload %q)", pl)
	}

	ci, err := b.jsa.lookupConsumer(mqttOutStreamName, claim.AffDurable)
	if err != nil || ci == nil || ci.Config == nil {
		t.Fatalf("the affine consumer was not restored: %v", err)
	}
	if verr := mqttValidateQ2AffineNames(ci.Config, claim.Winner, claim.Gen); verr != nil {
		t.Fatalf("the restored consumer is not the one the claim names: %v", verr)
	}
	if ci.Config.AckPolicy != AckExplicit || ci.Config.DeliverPolicy != DeliverNew {
		t.Fatalf("the restored consumer has the wrong delivery semantics: %+v", ci.Config)
	}
	if ci.Config.InactiveThreshold != 0 {
		t.Fatalf("the restored consumer carries an inactivity threshold of %v", ci.Config.InactiveThreshold)
	}

	// [MQTT-4.8.2-4]: the winner returns and the delivery is completed WITH IT.
	// This is what the feature is for; a restored durable that delivered nothing
	// would satisfy every claim-record assertion above.
	cw, rw := b.reconnect(t, winnerID, 300)
	defer cw.Close()
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rw, 15*time.Second)
	if !ok {
		t.Fatal("the winner returned and never received the delivery restored on its behalf")
	}
	if qos != 2 || string(payload) != "restored" {
		t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
	}
	b.completeExchange(t, cw, rw, pi)
	testMQTTExpectNothing(t, otherReader)
}

// The refusals that guard the restoration, each with an oracle a mutant cannot
// satisfy by reaching the same final state.
func TestQ2AffContenderRefusesAnIndeterminateRestoration(t *testing.T) {
	t.Parallel()

	// An indeterminate LOOKUP is not an absence, and all three shapes are
	// reachable from a request that SUCCEEDED: lookupConsumer returns
	// cir.ConsumerInfo unchecked, so a malformed reply yields nil info, and a
	// non-nil ConsumerInfo may carry a nil Config.
	for _, tc := range []struct {
		name string
		hook func(string) (*ConsumerInfo, error)
	}{
		{"a transport error", func(string) (*ConsumerInfo, error) {
			return nil, fmt.Errorf("test: the lookup request failed")
		}},
		{"a successful reply carrying no consumer info", func(string) (*ConsumerInfo, error) {
			return nil, nil
		}},
		{"a consumer info carrying no config", func(string) (*ConsumerInfo, error) {
			return &ConsumerInfo{}, nil
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := testQ2AffStageContender(t, "indet"+strconv.Itoa(len(tc.name)), nil, nil,
				func(st *testQ2AffContenderStage) { st.setLookup(tc.hook) })
			st.release(t)
			st.awaitRefusal(t, tc.name)
			// THE assertion: no create was attempted. Reading any of these as
			// "absent" would create a durable against an unknown one.
			st.assertNoCreate(t, tc.name)
			st.assertCopyAbsent(t, tc.name)
		})
	}

	// A create whose RESPONSE cannot be trusted. "Nothing stored and no source
	// acked" does not discriminate here: with the durable absent, a mutant that
	// proceeds stores a copy no consumer retains, finds it gone on the re-read and
	// reaches the same final state. The oracle is that materializeQ2AffCopy was
	// never ENTERED, witnessed by the copy store — sound in this staging precisely
	// because the copy is provably absent, so entering means attempting a store.
	alter := func(real func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error), f func(*ConsumerConfig)) func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
		return func(cfg *ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
			resp, err := real(cfg)
			if resp == nil || resp.ConsumerInfo == nil || resp.Config == nil {
				return resp, err
			}
			altered := *resp.Config
			f(&altered)
			info := *resp.ConsumerInfo
			info.Config = &altered
			return &JSApiConsumerCreateResponse{ConsumerInfo: &info}, err
		}
	}
	for _, tc := range []struct {
		name string
		hook func(*testQ2AffContenderStage) func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error)
	}{
		{"a nil create response", func(*testQ2AffContenderStage) func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
			return func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error) { return nil, nil }
		}},
		{"a response naming a different durable", func(st *testQ2AffContenderStage) func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
			return alter(st.realCreate, func(c *ConsumerConfig) { c.Durable += "x" })
		}},
		{"a response naming a different filter", func(st *testQ2AffContenderStage) func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
			return alter(st.realCreate, func(c *ConsumerConfig) { c.FilterSubject = mqttOutSubjectPrefix + "someone.else" })
		}},
		{"a response carrying a substituted inactivity threshold", func(st *testQ2AffContenderStage) func(*ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
			return alter(st.realCreate, func(c *ConsumerConfig) { c.InactiveThreshold = time.Minute })
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var stores atomic.Int32
			st := testQ2AffStageContender(t, "badresp"+strconv.Itoa(len(tc.name)), nil, nil,
				func(st *testQ2AffContenderStage) {
					st.setCreate(tc.hook(st))
					st.b.asm.mu.Lock()
					st.b.asm.testQ2AffCopyStore = func(subject string, hdrLen int, msg []byte) (uint64, error) {
						stores.Add(1)
						return testQ2AffRealCopyStore(st.b, subject, hdrLen, msg)
					}
					st.b.asm.mu.Unlock()
					t.Cleanup(func() {
						st.b.asm.mu.Lock()
						st.b.asm.testQ2AffCopyStore = nil
						st.b.asm.mu.Unlock()
					})
				})
			st.deleteWinnerDurable(t)
			st.awaitRefusal(t, tc.name)
			if n := stores.Load(); n != 0 {
				t.Fatalf("%s: materialization was entered %d time(s) after an untrustworthy create response", tc.name, n)
			}
			st.assertCopyAbsent(t, tc.name)
		})
	}
}

// With no durable to adopt from, the create carries the RECORD's persisted
// storage type — never the contender's own local option. This is what keeps a
// contender-created durable compatible with the owner's later create.
func TestQ2AffContenderCreatesFromTheRecordsStorageWhenAbsent(t *testing.T) {
	t.Parallel()
	st := testQ2AffStageContender(t, "absstore",
		func(ps *mqttPersistedSession) { ps.Q2Aff.MemoryStorage = true },
		func(c *ConsumerConfig) { c.MemoryStorage = true }, nil)
	if st.b.o.MQTT.ConsumerMemoryStorage {
		t.Fatal("this fixture needs the contender's LOCAL option to differ from the record's, or the two sources are indistinguishable")
	}
	st.deleteWinnerDurable(t)

	testQ2AffAwaitMaterialized(t, st)
	creates := st.creates()
	if len(creates) == 0 {
		t.Fatal("no create-or-update was issued, so nothing was restored")
	}
	if !creates[0].MemoryStorage {
		t.Fatalf("the create carried the contender's local storage option rather than the record's: %+v", creates[0])
	}
}

// The storage conflict a racing restoration produces, and the bound on the retry
// that resolves it.
//
// The race is SIMULATED rather than run with two live contenders, deliberately:
// two brokers both finding the durable absent and both creating it is not
// schedulable on demand, and a fixture that waited for it would be
// timing-dependent. The seam instead does what the rival would have done — really
// creates the durable with the other storage type — and then lets the requested
// create fail for real. The error the classifier sees is a genuine server error
// with a genuine flattened cause, not a fabricated one.
func TestQ2AffRacingAbsenceConverge(t *testing.T) {
	t.Parallel()

	t.Run("the loser adopts the winner's storage and converges", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int32
		st := testQ2AffStageContender(t, "raceconv", nil, nil, func(st *testQ2AffContenderStage) {
			real := st.realCreate
			st.setCreate(func(cfg *ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
				if attempts.Add(1) == 1 {
					rival := *cfg
					rival.MemoryStorage = !cfg.MemoryStorage
					if _, err := real(&rival); err != nil {
						t.Errorf("staging the rival's create: %v", err)
					}
				}
				return real(cfg)
			})
		})
		st.deleteWinnerDurable(t)

		testQ2AffAwaitMaterialized(t, st)
		got := st.creates()
		if len(got) < 2 {
			t.Fatalf("the conflict was never retried: %d create(s) issued", len(got))
		}
		if got[0].MemoryStorage == got[1].MemoryStorage {
			t.Fatalf("the retry did not adopt the durable's storage: %v then %v", got[0].MemoryStorage, got[1].MemoryStorage)
		}
		// THE assertion that the retry is IN-ATTEMPT. Without it this subtest does
		// not discriminate: with no retry at all the contender refuses, AckWait
		// re-offers, and the next attempt finds the rival's durable and adopts it —
		// reaching two creates and the same final state by a slower route. A
		// deferred disposition is the observable difference, and there must be none.
		for _, r := range st.results() {
			if r == mqttQ2AffMoveDeferred {
				t.Fatalf("the conflict was resolved by a later redelivery rather than by the in-attempt retry: %v", st.results())
			}
		}
		ci, err := st.b.jsa.lookupConsumer(mqttOutStreamName, st.durable)
		if err != nil || ci == nil || ci.Config == nil {
			t.Fatalf("the durable is unreadable after convergence: %v", err)
		}
		if ci.Config.MemoryStorage != got[1].MemoryStorage {
			t.Fatalf("the retry carried MemoryStorage=%v but the durable has %v", got[1].MemoryStorage, ci.Config.MemoryStorage)
		}
	})

	// The relookup finds the durable GONE again. Equality with the request that
	// was just refused is not a reason to stop here: the obstacle has been removed,
	// so the identical request is exactly the retry that now succeeds. Refusing
	// would defer restoration to another AckWait redelivery for no reason.
	t.Run("the relookup finds it absent again", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int32
		st := testQ2AffStageContender(t, "raceabs", nil, nil, func(st *testQ2AffContenderStage) {
			real, jsa := st.realCreate, st.b.jsa
			st.setCreate(func(cfg *ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
				if attempts.Add(1) == 1 {
					rival := *cfg
					rival.MemoryStorage = !cfg.MemoryStorage
					if _, err := real(&rival); err != nil {
						t.Errorf("staging the rival's create: %v", err)
					}
					resp, err := real(cfg)
					if _, derr := jsa.deleteConsumer(mqttOutStreamName, cfg.Durable, false); derr != nil {
						t.Errorf("removing the rival's durable: %v", derr)
					}
					return resp, err
				}
				return real(cfg)
			})
		})
		st.deleteWinnerDurable(t)

		testQ2AffAwaitMaterialized(t, st)
		got := st.creates()
		if len(got) < 2 {
			t.Fatalf("the conflict was never retried after the durable vanished: %d create(s) issued", len(got))
		}
		// With nothing to adopt, the retry carries the RECORD's value — which here
		// equals the refused request. That equality is precisely what an earlier
		// version of this code read as "nothing to do".
		if got[1].MemoryStorage != got[0].MemoryStorage {
			t.Fatalf("the retry did not fall back to the record's storage: %v then %v", got[0].MemoryStorage, got[1].MemoryStorage)
		}
		for _, r := range st.results() {
			if r == mqttQ2AffMoveDeferred {
				t.Fatalf("the restoration was deferred to a later redelivery instead of retrying in-attempt: %v", st.results())
			}
		}
	})

	t.Run("an unrelated immutable mismatch does not enter the retry", func(t *testing.T) {
		t.Parallel()
		var attempts atomic.Int32
		st := testQ2AffStageContender(t, "racenon", nil, nil, func(st *testQ2AffContenderStage) {
			real := st.realCreate
			st.setCreate(func(cfg *ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
				if attempts.Add(1) == 1 {
					// A rival differing in an immutable field the retry must not
					// touch: same outer error 10012, different flattened cause.
					rival := *cfg
					rival.DeliverPolicy = natsserver.DeliverAll
					if _, err := real(&rival); err != nil {
						t.Errorf("staging the rival's create: %v", err)
					}
				}
				return real(cfg)
			})
		})
		st.deleteWinnerDurable(t)
		st.awaitRefusal(t, "a deliver-policy mismatch")
		if n := len(st.creates()); n != 1 {
			t.Fatalf("a non-storage immutable mismatch was retried: %d create(s) issued", n)
		}
		st.assertCopyAbsent(t, "a deliver-policy mismatch")
	})

	t.Run("a second conflict refuses rather than looping", func(t *testing.T) {
		t.Parallel()
		st := testQ2AffStageContender(t, "racetwice", nil, nil, func(st *testQ2AffContenderStage) {
			real, jsa := st.realCreate, st.b.jsa
			st.setCreate(func(cfg *ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
				// Every attempt loses the same way: the rival re-creates the durable
				// with whatever this attempt did NOT ask for, so the conflict recurs
				// and an unbounded retry would spin here forever.
				if _, err := jsa.deleteConsumer(mqttOutStreamName, cfg.Durable, false); isErrorOtherThan(err, JSConsumerNotFoundErr) {
					t.Errorf("clearing the durable between attempts: %v", err)
				}
				rival := *cfg
				rival.MemoryStorage = !cfg.MemoryStorage
				if _, err := real(&rival); err != nil {
					t.Errorf("staging the rival's create: %v", err)
				}
				return real(cfg)
			})
		})
		st.deleteWinnerDurable(t)
		st.awaitRefusal(t, "a recurring storage conflict")
		st.assertCopyAbsent(t, "a recurring storage conflict")

		// THE bound: two creates per attempt and never a third.
		if n := len(st.creates()); n%2 != 0 || n == 0 {
			t.Fatalf("the retry is not bounded at one per attempt: %d create(s) issued", n)
		}
	})
}

// Residual 8, pinned: the stall is NOT closed for a MATERIALIZED assignment whose
// copy is gone.
//
// This is the half of F-064's own title this item does not deliver, and it is the
// easiest thing in the change to gloss, so it is asserted rather than described.
// D1 is the dispatcher's `default` arm, reached only for a claim that has not been
// materialized. A foreign `materialized` claim with no copy takes the arm above
// it, where sourceAckAuthorized grants nothing: the contender proves nothing,
// writes nothing and acks nothing, and the source keeps its MaxAckPending slot
// until the owner resumes and records a terminal fact.
//
// The claim is SEEDED materialized with its source unacknowledged, because that
// combination is the population and the broker will not produce it on demand:
// materializing normally releases the source in the same operation, after which
// nothing re-offers the delivery and the arm under test is never re-entered. An
// earlier version of this fixture drove materialization the ordinary way and was
// vacuous for exactly that reason — M20 survived it.
//
// It fails if somebody routes this population into on-behalf restoration, which
// would be a duplicate PUBLISH for an exchange that may already be past PUBREC —
// the reason the re-store arm was cut from this design.
func TestQ2AffMaterializedClaimWithNoCopyStillWaitsForItsOwner(t *testing.T) {
	t.Parallel()
	var entered atomic.Int32
	st := testQ2AffStageContenderSeeded(t, "resid8", nil, nil,
		func(st *testQ2AffContenderStage) {
			// Count entries into on-behalf restoration by counting what only it
			// does here: look the winner's durable up. Nothing else on this path
			// makes that call, so any count above zero is D1 being entered.
			st.setLookup(func(durable string) (*ConsumerInfo, error) {
				entered.Add(1)
				return st.b.jsa.lookupConsumer(mqttOutStreamName, durable)
			})
		},
		func(c *mqttQ2AffClaim) {
			c.Phase = mqttQ2AffMaterialized
			c.SourceAcked = false
		})
	st.release(t)

	// Anti-vacuity: the arm under test must actually be reached, repeatedly,
	// which is what "the source is still being re-offered" means here.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && len(st.results()) < 2 {
		time.Sleep(50 * time.Millisecond)
	}
	got := st.results()
	if len(got) < 2 {
		t.Fatalf("the assignment was resolved fewer than twice (%v), so the re-offer this residual is about never happened", got)
	}
	for _, r := range got {
		if r != mqttQ2AffMoveDeferred {
			t.Fatalf("a contender resolved a materialized claim whose copy is gone as %v; that population must wait for its owner", r)
		}
	}

	claim := testQ2AffLoadClaim(t, st.b, st.subject)
	if claim.Phase != mqttQ2AffMaterialized {
		t.Fatalf("the claim left the materialized phase: %+v", *claim)
	}
	if claim.AffineDone || claim.Lost || claim.Discarded {
		t.Fatalf("a contender recorded a terminal fact for a materialized claim whose copy is gone: %+v", *claim)
	}
	if claim.SourceAcked {
		t.Fatal("a contender released the group source for a claim it could prove nothing about")
	}
	if state, _ := st.b.asm.loadQ2AffCopy(claim.CopySubject); state == mqttQ2AffCopyPresent {
		t.Fatal("a replacement copy was materialized for a claim that may already be past PUBREC")
	}
	// THE discriminating assertion. Every check above is also satisfied by a
	// restoration that ran and then failed; only this one says it was never
	// entered.
	if n := entered.Load(); n != 0 {
		t.Fatalf("on-behalf restoration was entered %d time(s) for a materialized claim; that population is residual 8 and must wait for its owner", n)
	}
}

// The regression this item would have introduced without the owner's recovery,
// staged the way it would actually arise: a CONTENDER creates the durable first.
//
// With the durable absent, a contender legitimately creates it from the winner's
// PERSISTED storage type — the only value it may impose. If an operator has since
// reloaded consumer_memory_storage, the owner then rebuilds its own request from
// the CURRENT option, meets an immutable mismatch, and without the recovery could
// never resume. Trading F-064's stall for a permanently unresumable session is not
// a trade this item may make, which is why decision 4 admits one post-failure
// recovery on the owner's path.
//
// The oracle is a FRESH delivery, not a stranded one. Recreating a durable with
// the other storage type necessarily destroys it first, and $MQTT_out is
// interest-retained, so any copy it held is gone — a stranded-copy oracle would
// fail here for a reason this fixture does not own. What the regression is about
// is whether the session can be resumed and served at all.
func TestQ2AffOwnerResumesAfterAContenderCreatedItsDurable(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "ownctd")
	c, r := b.member(t, "ownctdA", 300)
	b.bind(t, "ownctdA")
	idHash := getHash("ownctdA")

	// Leave the group so the record keeps the affine path with no shared filter
	// beside it, which is what puts the resume on the standalone route.
	b.unsubscribe(t, c, r, 2, []string{"$share/" + b.group + "/" + b.topic})
	c.Close()

	ps := testQ2AffAwaitStableRecord(t, b, idHash)
	if ps.Q2AffGen == _EMPTY_ || ps.Q2Aff == nil {
		t.Fatalf("the record names no affine path: %+v", ps)
	}
	recorded := ps.Q2Aff.MemoryStorage

	// A contender's restoration, reproduced exactly: the durable is destroyed and
	// recreated from the RECORD's persisted storage, which is what
	// materializeQ2AffOnBehalf does when it finds nothing to adopt.
	if _, err := b.jsa.deleteConsumer(mqttOutStreamName, ps.Q2Aff.Durable, false); err != nil {
		t.Fatalf("destroying the affine durable: %v", err)
	}
	restored := buildQ2AffineConsumerConfig(idHash, ps.Q2AffGen, ps.Q2Aff.AckWait, ps.Q2Aff.MaxAckPending, recorded)
	if _, err := b.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttOutStreamName, Config: restored}); err != nil {
		t.Fatalf("recreating the durable the way a contender would: %v", err)
	}

	// The operator reload, which is what turns the contender's correct choice into
	// a conflict for the owner.
	b.o.MQTT.ConsumerMemoryStorage = !recorded
	testQ2AffRestartSameStore(t, b)
	if got := b.s.getOpts().MQTT.ConsumerMemoryStorage; got == recorded {
		t.Fatalf("the reload did not reach getOpts(): consumer_memory_storage is still %v", got)
	}
	testQ2AffReadRecordAfterRestart(t, b, "ownctd-reader", idHash)
	lookups := testQ2AffCountLookups(t, b)

	cr, rr := b.reconnect(t, "ownctdA", 300)
	defer cr.Close()
	// Polled, not read once: the CONNACK's arrival does not order this read against the
	// tail of the resume still running behind it, so an immediate read races the work it
	// is trying to observe.
	sess := testMQTTGetClient(t, b.s, "ownctdA").mqtt.sess
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) && !ready {
		sess.mu.Lock()
		ready = sess.q2AffReady
		sess.mu.Unlock()
		if !ready {
			time.Sleep(25 * time.Millisecond)
		}
	}
	if !ready {
		t.Fatal("the owner could not re-establish its affine path against a durable a contender created for it: F-064 traded a stall for an unresumable session")
	}
	if n := lookups(); n != 1 {
		t.Fatalf("the recovery performed %d affine consumer lookup(s), want exactly 1", n)
	}

	// And the session really is serviceable, not merely non-failing: it rejoins
	// the group and a shared QoS 2 message is carried end to end.
	codes, _ := b.subSharedTolerant(t, cr, rr, 1, "$share/"+b.group+"/"+b.topic)
	if len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("the resumed session could not rejoin the shared group: %v", codes)
	}
	b.publish(t, 2, "after-recovery")
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rr, 10*time.Second)
	if !ok {
		t.Fatal("the resumed session received nothing after rejoining the group")
	}
	if qos != 2 || string(payload) != "after-recovery" {
		t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
	}
	b.completeExchange(t, cr, rr, pi)
}

// --------------------------------------------------------------------------
// F-065: a claim incarnation has an identity, and a superseded incarnation can
// neither be written to nor have its copy adopted.
// --------------------------------------------------------------------------

// TestQ2AffReplacementIncarnationKeepsItsOwnCopy is route C, the interleaving that
// took three review rounds to state correctly.
//
// A claim subject's election is create-only, so it can be won AGAIN after a reap —
// by a group delivery callback that was already dispatched when the reap happened.
// Before F-065 both incarnations derived the SAME copy subject, so the replacement
// adopted the copy a superseded materializer was about to withdraw, and that
// materializer's advance (which asserted only Winner+Gen) mutated the REPLACEMENT's
// record. Two things stop it now: the copy subject carries the incarnation, and
// advanceQ2AffClaim refuses a write whose expected nonce does not match the record
// it loaded.
//
// The ordering below is the only one the create-only election permits, which is what
// round 5 corrected: the replacement cannot win the CAS until the first claim is
// gone. So C1 is completed and reaped FIRST, and the stale actor is held across it.
//
// Paired mutations: M6 (drop the nonce from advanceQ2AffClaim's identity check) —
// the stale advance is then accepted; M9 (derive the copy subject without the nonce)
// — the two incarnations then share a subject and the withdrawal destroys C2's copy.
func TestQ2AffReplacementIncarnationKeepsItsOwnCopy(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "reincarn")
	c, _ := b.member(t, "reincarnA", 300)
	b.bind(t, "reincarnA")
	idHash := getHash("reincarnA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("the member has no affine generation, so no consumer would retain either copy")
	}
	groupDur := b.groupDurable(t)

	// The client leaves but keeps its session and its affine consumer, so both copies
	// are retained by interest rather than consumed the instant they land.
	testMQTTDisconnect(t, c, nil)
	c.Close()

	const srcSeq = uint64(778899)
	subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, srcSeq)

	// C1: the superseded incarnation. A stale materializer proved its copy present.
	c1 := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
		ClaimHash: claimHash, Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(idHash, gen),
		Phase:      mqttQ2AffClaimed,
	})
	env1 := &mqttQ2AffineEnvelope{
		natsSubject: b.topic, msg: []byte("superseded"),
		claimSubject: subject, gen: gen,
		msgID: mqttQ2AffMsgId(groupDur, srcSeq, c1.Nonce),
	}
	msg1, hdr1 := mqttNewQ2AffineRecord(env1)
	seq1, err := b.asm.storeQ2AffCopy(c1.CopySubject, hdr1, msg1)
	if err != nil {
		t.Fatalf("staging the superseded copy: %v", err)
	}
	if st, _ := b.asm.loadQ2AffCopy(c1.CopySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the superseded copy is not on the stream (%v); interest retention did not keep it", st)
	}

	// C1 is completed and REAPED, which is what frees the subject for a replacement.
	// Seeded directly: the point of the fixture is the state after the reap.
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, c1); err != nil {
		t.Fatalf("seeding C1: %v", err)
	}
	_, c1seq, _ := b.asm.loadQ2AffClaim(subject)
	if err := b.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, c1seq, true); err != nil {
		t.Fatalf("reaping C1: %v", err)
	}

	// C2: the replacement, elected by the still-running delivery, with its OWN
	// incarnation and therefore its own copy subject.
	c2 := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
		ClaimHash: claimHash, Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(idHash, gen),
		Phase:      mqttQ2AffClaimed,
	})
	if c2.CopySubject == c1.CopySubject {
		t.Fatal("two incarnations of one claim subject derived the SAME copy subject: the replacement can adopt the copy the superseded materializer is about to withdraw (F-065 route C)")
	}
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, c2); err != nil {
		t.Fatalf("electing C2: %v", err)
	}
	c2claim, c2seq, _ := b.asm.loadQ2AffClaim(subject)

	// C2 materializes its own copy, through production code.
	env2 := &mqttQ2AffineEnvelope{
		natsSubject: b.topic, msg: []byte("replacement"),
		claimSubject: subject, gen: gen,
	}
	if !b.asm.materializeQ2AffCopy(b.s, c2claim, subject, c2seq, env2) {
		t.Fatal("the replacement incarnation could not materialize its own copy")
	}

	// Now the stale actor resumes: its advance must be REFUSED, and its withdrawal
	// must remove only its own copy.
	updated, _, aerr := b.asm.advanceQ2AffClaim(subject, c1.Winner, c1.Gen, c1.Nonce, _EMPTY_,
		func(cl *mqttQ2AffClaim) { cl.Phase = mqttQ2AffMaterialized })
	if aerr == nil {
		t.Fatal("a superseded incarnation's transition was ACCEPTED: it has just written a fact about its own copy onto the replacement's record (F-065)")
	}
	if !b.asm.q2AffCopyIsDisowned(subject, c1, updated, aerr) {
		t.Fatal("the superseded copy was not recognised as disowned, so nothing will ever withdraw it")
	}
	b.asm.withdrawQ2AffCopy(b.s, c1.CopySubject, seq1)

	// The replacement is untouched: its record still says materialized, and its copy
	// is still there.
	after, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil || after == nil {
		t.Fatalf("the replacement claim is gone: %v", err)
	}
	if after.Nonce != c2.Nonce {
		t.Fatalf("the record no longer names the replacement incarnation: got %q, want %q", after.Nonce, c2.Nonce)
	}
	if after.Phase != mqttQ2AffMaterialized {
		t.Fatalf("the replacement's phase was rolled back to %v", after.Phase)
	}
	if st, _ := b.asm.loadQ2AffCopy(c2.CopySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the replacement's own copy was destroyed by the superseded materializer's withdrawal (%v)", st)
	}
	if st, _ := b.asm.loadQ2AffCopy(c1.CopySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("the superseded copy survived its withdrawal (%v); it can never be delivered and would NAK forever", st)
	}
}

// TestQ2AffWithdrawsACopyNothingCanAuthorize covers the withdrawal rule's arms.
//
// A materialization attempt that PROVED a copy present withdraws it once the same
// attempt proves, authoritatively, that no claim of that copy's own incarnation
// remains to authorize it. The rule is deliberately conditional: a copy no attempt is
// currently examining is not collected by anything, which is the residual F-065's
// re-scoped guarantee states.
//
// Each subtest names the mutation it kills, because two prior review rounds struck
// out mutation claims that changed a code path without changing an observable.
func TestQ2AffWithdrawsACopyNothingCanAuthorize(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "withdraw")
	c, _ := b.member(t, "withdrawA", 300)
	b.bind(t, "withdrawA")
	idHash := getHash("withdrawA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation, so no consumer would retain a staged copy")
	}
	groupDur := b.groupDurable(t)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// seed elects one claim incarnation and returns it with its loaded sequence.
	seed := func(t *testing.T, srcSeq uint64, phase mqttQ2AffPhase, mut func(*mqttQ2AffClaim)) (*mqttQ2AffClaim, string, uint64) {
		t.Helper()
		subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
		cl := testQ2AffSelfConsistent(&mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
			ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq), Nonce: mqttNewQ2AffGen(),
			AffDurable: affDurableFor(idHash, gen), Phase: phase,
		})
		if mut != nil {
			mut(cl)
		}
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, cl); err != nil {
			t.Fatalf("seeding the claim: %v", err)
		}
		got, seq, err := b.asm.loadQ2AffClaim(subject)
		if err != nil || got == nil {
			t.Fatalf("loading the seeded claim: %v", err)
		}
		return got, subject, seq
	}
	env := func() *mqttQ2AffineEnvelope {
		return &mqttQ2AffineEnvelope{natsSubject: b.topic, msg: []byte("body")}
	}
	deleteClaim := func(t *testing.T, subject string) {
		t.Helper()
		_, seq, err := b.asm.loadQ2AffClaim(subject)
		if err != nil {
			t.Fatalf("loading before delete: %v", err)
		}
		if err := b.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); err != nil {
			t.Fatalf("deleting the claim: %v", err)
		}
	}

	// M2: delete the withdrawal call. The stranded copy then stays present.
	t.Run("claim reaped between the proof and the CAS: the copy is withdrawn", func(t *testing.T) {
		cl, subject, seq := seed(t, 811001, mqttQ2AffClaimed, nil)
		b.asm.mu.Lock()
		b.asm.testQ2AffPreMaterialize = func(string, uint64) error { deleteClaim(t, subject); return nil }
		b.asm.mu.Unlock()
		defer func() { b.asm.mu.Lock(); b.asm.testQ2AffPreMaterialize = nil; b.asm.mu.Unlock() }()

		if b.asm.materializeQ2AffCopy(b.s, cl, subject, seq, env()) {
			t.Fatal("materialization reported success against a claim that no longer exists")
		}
		if st, _ := b.asm.loadQ2AffCopy(cl.CopySubject); st != mqttQ2AffCopyAbsent {
			t.Fatalf("the copy survived with no claim able to authorize it (%v): the delivery gate can only NAK it, forever", st)
		}
	})

	// M3: withdraw on the error arm without taking the fresh read. A live claim's
	// copy is then destroyed on an answer that was never authoritative.
	t.Run("an indeterminate transition retains the copy", func(t *testing.T) {
		cl, subject, seq := seed(t, 811002, mqttQ2AffClaimed, nil)
		b.asm.mu.Lock()
		b.asm.testQ2AffClaimStore = func(string, uint64, *mqttQ2AffClaim) (uint64, error) {
			return 0, fmt.Errorf("backend unavailable")
		}
		b.asm.mu.Unlock()
		defer func() { b.asm.mu.Lock(); b.asm.testQ2AffClaimStore = nil; b.asm.mu.Unlock() }()

		if b.asm.materializeQ2AffCopy(b.s, cl, subject, seq, env()) {
			t.Fatal("materialization reported success although its transition never committed")
		}
		if st, _ := b.asm.loadQ2AffCopy(cl.CopySubject); st != mqttQ2AffCopyPresent {
			t.Fatalf("an indeterminate answer destroyed a live claim's copy (%v); it must fail closed", st)
		}
		if after, _, _ := b.asm.loadQ2AffClaim(subject); after == nil {
			t.Fatal("the claim itself disappeared")
		}
	})

	// M5: drop the !terminal() test. Route B's copy then survives with nothing able
	// to deliver it — the interleaving that needs no reaper race at all.
	t.Run("a terminal claim of our own incarnation: the copy is withdrawn", func(t *testing.T) {
		cl, subject, seq := seed(t, 811003, mqttQ2AffClaimed, nil)
		b.asm.mu.Lock()
		b.asm.testQ2AffPreMaterialize = func(string, uint64) error {
			// The claim completes under us, exactly as route B describes.
			if _, _, err := b.asm.advanceQ2AffClaim(subject, _EMPTY_, _EMPTY_, cl.Nonce, _EMPTY_,
				func(c *mqttQ2AffClaim) { c.AffineDone = true }); err != nil {
				t.Errorf("completing the claim mid-flight: %v", err)
			}
			return nil
		}
		b.asm.mu.Unlock()
		defer func() { b.asm.mu.Lock(); b.asm.testQ2AffPreMaterialize = nil; b.asm.mu.Unlock() }()

		if b.asm.materializeQ2AffCopy(b.s, cl, subject, seq, env()) {
			t.Fatal("materialization reported success for a claim that had already completed")
		}
		if st, _ := b.asm.loadQ2AffCopy(cl.CopySubject); st != mqttQ2AffCopyAbsent {
			t.Fatalf("a copy stored under a completed claim survived (%v): it duplicates a PUBLISH the client already answered", st)
		}
	})

	// M7 / M8: the AMBIGUOUS store — it LANDED and its response was lost. Revision 7
	// requires this, and it is the case that most needs the presence proof: the store
	// reports nothing, so a design keyed on the response would abandon a copy that is
	// really there, and one that returned early on a non-CAS error would never look.
	// Either regression leaves a real copy NAK-looping behind the gate forever.
	t.Run("a store that landed but reported an error is still resolved", func(t *testing.T) {
		cl, subject, seq := seed(t, 811005, mqttQ2AffClaimed, nil)
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyStore = func(subj string, hdrLen int, msg []byte) (uint64, error) {
			if _, err := b.asm.jsa.storeMsg(subj, hdrLen, msg); err != nil {
				t.Errorf("landing the copy: %v", err)
			}
			return 0, fmt.Errorf("test: the store response was lost")
		}
		b.asm.testQ2AffPreMaterialize = func(string, uint64) error { deleteClaim(t, subject); return nil }
		b.asm.mu.Unlock()
		defer func() {
			b.asm.mu.Lock()
			b.asm.testQ2AffCopyStore, b.asm.testQ2AffPreMaterialize = nil, nil
			b.asm.mu.Unlock()
		}()

		if b.asm.materializeQ2AffCopy(b.s, cl, subject, seq, env()) {
			t.Fatal("materialization reported success against a claim that no longer exists")
		}
		if st, _ := b.asm.loadQ2AffCopy(cl.CopySubject); st != mqttQ2AffCopyAbsent {
			t.Fatalf("a copy that LANDED while its response was lost survived with nothing able to authorize it (%v): the attempt either returned before the presence check or took its sequence from the missing response", st)
		}
	})

	// The property is about the PROOF, not about who stored the copy: a call that found
	// the copy ALREADY present still resolves it, because the copy's incarnation is
	// what has ended and a copy nothing can authorize must not be left NAK-looping just
	// because some other actor stored it. Round 2 required the guarantee to be stated
	// this way, since the narrower "only what I stored" version was not what the code
	// did.
	//
	// M4: require an own store before withdrawing. The copy then survives here.
	// T5, with revision 7's OWN M4: the claim subject is taken over by a DIFFERENT
	// generation after this call proved its copy, so advanceQ2AffClaim returns
	// errQ2AffUnresolved with a non-nil claim. That arm must withdraw. Recorded because
	// the ledger previously labelled a different mutation M4 — the own-store guard — so
	// the documented kill did not establish this behaviour at all.
	t.Run("the subject taken over by another generation: the copy is withdrawn", func(t *testing.T) {
		cl, subject, seq := seed(t, 811006, mqttQ2AffClaimed, nil)
		b.asm.mu.Lock()
		b.asm.testQ2AffPreMaterialize = func(string, uint64) error {
			deleteClaim(t, subject)
			other := testQ2AffSelfConsistent(&mqttQ2AffClaim{
				Winner: idHash, Gen: mqttNewQ2AffGen(), GroupDurable: groupDur, SourceSeq: 811006,
				ClaimHash: mqttQ2AffClaimHash(groupDur, 811006), Nonce: mqttNewQ2AffGen(),
				Phase: mqttQ2AffClaimed,
			})
			other.AffDurable = affDurableFor(other.Winner, other.Gen)
			if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, other); err != nil {
				t.Errorf("installing the other generation's claim: %v", err)
			}
			return nil
		}
		b.asm.mu.Unlock()
		defer func() { b.asm.mu.Lock(); b.asm.testQ2AffPreMaterialize = nil; b.asm.mu.Unlock() }()

		if b.asm.materializeQ2AffCopy(b.s, cl, subject, seq, env()) {
			t.Fatal("materialization reported success although the subject is held by another generation")
		}
		if st, _ := b.asm.loadQ2AffCopy(cl.CopySubject); st != mqttQ2AffCopyAbsent {
			t.Fatalf("the copy survived under a subject held by another generation (%v): nothing can ever authorize it, because a claim naming a different winner or generation derives a different copy subject", st)
		}
	})

	t.Run("a copy this call only OBSERVED is still withdrawn", func(t *testing.T) {
		cl, subject, seq := seed(t, 811004, mqttQ2AffClaimed, nil)
		// Pre-store the copy, so the call finds it present and stores nothing.
		e := env()
		e.claimSubject, e.gen, e.msgID = subject, gen, mqttQ2AffMsgId(groupDur, 811004, cl.Nonce)
		msg, hdr := mqttNewQ2AffineRecord(e)
		if _, err := b.asm.storeQ2AffCopy(cl.CopySubject, hdr, msg); err != nil {
			t.Fatalf("pre-storing the copy: %v", err)
		}
		b.asm.mu.Lock()
		b.asm.testQ2AffPreMaterialize = func(string, uint64) error { deleteClaim(t, subject); return nil }
		b.asm.mu.Unlock()
		defer func() { b.asm.mu.Lock(); b.asm.testQ2AffPreMaterialize = nil; b.asm.mu.Unlock() }()

		if b.asm.materializeQ2AffCopy(b.s, cl, subject, seq, env()) {
			t.Fatal("materialization reported success against a claim that no longer exists")
		}
		if st, _ := b.asm.loadQ2AffCopy(cl.CopySubject); st != mqttQ2AffCopyAbsent {
			t.Fatalf("a copy whose incarnation has ended survived because this call had not stored it itself (%v)", st)
		}
	})
}

// TestQ2AffCopyDedupKeyIsScopedToTheIncarnation is the subtlest of the non-recovery
// wirings, which is why Codex's plan review asked for it early.
//
// A dedupe entry OUTLIVES its message (measured, see
// TestQ2AffCopyStoreWithNoInterestSequenceAndDedupe). So the key has to be scoped to
// the claim INCARNATION twice over:
//
//   - Across incarnations it must DIFFER, or a replacement's store onto its own fresh
//     subject is silently discarded for the whole Duplicates window and that claim
//     never materializes.
//   - Within one incarnation it must be STABLE, or every retry stores a second copy —
//     which is the attempt-scoped identity F-063 refuted.
//
// The stability half is asserted the way round 4 required: by DELETING the copy and
// retrying, so the dedupe entry is the only thing that can suppress the second store.
// Asserting only "two incarnations both land" would not kill M10, because deriving the
// key from the pre-election envelope also makes two stores land.
func TestQ2AffCopyDedupKeyIsScopedToTheIncarnation(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "dedupinc")
	c, _ := b.member(t, "dedupincA", 300)
	b.bind(t, "dedupincA")
	idHash := getHash("dedupincA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation, so no consumer would retain a staged copy")
	}
	groupDur := b.groupDurable(t)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	const srcSeq = uint64(922001)
	subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
	materialize := func(t *testing.T, nonce string) *mqttQ2AffClaim {
		t.Helper()
		cl := testQ2AffSelfConsistent(&mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
			ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq), Nonce: nonce,
			AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffClaimed,
		})
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, cl); err != nil {
			t.Fatalf("electing incarnation %s: %v", nonce, err)
		}
		got, seq, err := b.asm.loadQ2AffClaim(subject)
		if err != nil || got == nil {
			t.Fatalf("loading incarnation %s: %v", nonce, err)
		}
		// Deliberately built as the MOVER builds it — before the election, carrying
		// this attempt's own candidate values — so the production path is the thing
		// that has to rebind the key from the persisted claim.
		env := &mqttQ2AffineEnvelope{
			natsSubject: b.topic, msg: []byte("body-" + nonce),
			claimSubject: subject, gen: gen,
			msgID: mqttQ2AffMsgId(groupDur, srcSeq, mqttNewQ2AffGen()),
		}
		if !b.asm.materializeQ2AffCopy(b.s, got, subject, seq, env) {
			t.Fatalf("incarnation %s could not materialize its own copy", nonce)
		}
		if st, _ := b.asm.loadQ2AffCopy(got.CopySubject); st != mqttQ2AffCopyPresent {
			t.Fatalf("incarnation %s materialized but its copy is not on the stream (%v): its store was deduped away against a previous incarnation's key", nonce, st)
		}
		return got
	}

	first := materialize(t, mqttNewQ2AffGen())

	// Reap the first incarnation and elect a second on the same claim subject.
	_, seq, _ := b.asm.loadQ2AffClaim(subject)
	if err := b.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); err != nil {
		t.Fatalf("reaping the first incarnation: %v", err)
	}
	second := materialize(t, mqttNewQ2AffGen())
	if second.CopySubject == first.CopySubject {
		t.Fatal("the two incarnations shared a copy subject")
	}

	// Stability within ONE incarnation. This half has to be staged on a claim that is
	// still `claimed`: once a claim is materialized, materializeQ2AffCopy refuses to
	// re-store for it at all (F-063), so a retry there proves nothing about the dedup
	// key — which is how the first version of this assertion passed against M10.
	const staySeq = uint64(922002)
	staySubject := mqttQ2AffClaimSubject(groupDur, staySeq)
	stay := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: staySeq,
		ClaimHash: mqttQ2AffClaimHash(groupDur, staySeq), Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffClaimed,
	})
	if _, err := b.asm.storeQ2AffClaimCAS(staySubject, 0, stay); err != nil {
		t.Fatalf("electing the stability incarnation: %v", err)
	}
	// A first attempt of THIS incarnation stores its copy under the incarnation's own
	// key, and the phase deliberately stays `claimed`.
	firstEnv := &mqttQ2AffineEnvelope{
		natsSubject: b.topic, msg: []byte("first-attempt"),
		claimSubject: staySubject, gen: gen,
		msgID: mqttQ2AffMsgId(groupDur, staySeq, stay.Nonce),
	}
	fm, fh := mqttNewQ2AffineRecord(firstEnv)
	if _, err := b.asm.storeQ2AffCopy(stay.CopySubject, fh, fm); err != nil {
		t.Fatalf("storing the first attempt's copy: %v", err)
	}
	if _, sm := b.asm.loadQ2AffCopy(stay.CopySubject); sm != nil {
		if err := b.asm.jsa.deleteMsg(mqttOutStreamName, sm.Sequence, true); err != nil {
			t.Fatalf("deleting the first attempt's copy: %v", err)
		}
	}
	if st, _ := b.asm.loadQ2AffCopy(stay.CopySubject); st != mqttQ2AffCopyAbsent {
		t.Fatal("the copy was not removed, so the retry below would prove nothing")
	}

	// The retry carries a FRESH candidate key, which is what the mover hands in: the
	// envelope is built before the election, so every attempt arrives with its own
	// candidate nonce. Rebinding it from the PERSISTED claim is the only thing that can
	// make this retry dedup against the first attempt above.
	cur, curSeq, _ := b.asm.loadQ2AffClaim(staySubject)
	retry := &mqttQ2AffineEnvelope{
		natsSubject: b.topic, msg: []byte("retry"),
		claimSubject: staySubject, gen: gen,
		msgID: mqttQ2AffMsgId(groupDur, staySeq, mqttNewQ2AffGen()),
	}
	b.asm.materializeQ2AffCopy(b.s, cur, staySubject, curSeq, retry)
	if st, _ := b.asm.loadQ2AffCopy(stay.CopySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("a retry of the SAME incarnation stored a second copy (%v): its key came from the attempt rather than from the persisted claim, which is the identity F-063 refuted", st)
	}
}

// TestQ2AffGateDisposesASupersededIncarnationsCopy covers the delivery gate's new
// arm, and the boundary that makes it legitimate.
//
// A live claim naming a DIFFERENT copy subject is positive evidence that the copy in
// hand belongs to an incarnation that is over, so the gate disposes of it. That is
// not the same as an ABSENT claim, which stays fail-closed and NAKs — an absence is
// never a positive fact, so it never licenses destroying an accepted message
// (feedback_absence_is_not_a_fact). It used to be producible by a SUPPORTED operation,
// F-066's single-stream recreation; that route is closed, and a whole-bundle reset
// removes the copy along with the claim rather than leaving this state, so what
// remains producible is administrative deletion or a partial restore. Both
// halves are asserted here, because the arm is only safe if the boundary holds.
//
// Paired mutation M11: drop the gate's CopySubject comparison. The superseded copy is
// then NAK'd forever instead of disposed of.
func TestQ2AffGateDisposesASupersededIncarnationsCopy(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "gatesup")
	c, r := b.member(t, "gatesupA", 300)
	defer c.Close()
	b.bind(t, "gatesupA")
	idHash := getHash("gatesupA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)

	const srcSeq = uint64(733001)
	subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, srcSeq)

	// The live claim is incarnation TWO, materialized, with its own copy subject.
	live := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
		ClaimHash: claimHash, Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(idHash, gen),
		Phase:      mqttQ2AffMaterialized,
	})
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, live); err != nil {
		t.Fatalf("seeding the live claim: %v", err)
	}

	// The copy on the stream belongs to incarnation ONE, which the live claim does not
	// name. Stored on the SUPERSEDED subject, which the consumer's generation filter
	// still covers — so the gate really does receive it.
	supersededNonce := mqttNewQ2AffGen()
	supersededSubject := mqttQ2AffCopySubject(idHash, gen, claimHash, supersededNonce)
	if supersededSubject == live.CopySubject {
		t.Fatal("the two incarnations share a copy subject, so this fixture cannot stage the case")
	}
	env := &mqttQ2AffineEnvelope{
		natsSubject: b.topic, msg: []byte("from-a-dead-incarnation"),
		claimSubject: subject, gen: gen,
		msgID: mqttQ2AffMsgId(groupDur, srcSeq, supersededNonce),
	}
	msg, hdr := mqttNewQ2AffineRecord(env)
	if _, err := b.asm.storeQ2AffCopy(supersededSubject, hdr, msg); err != nil {
		t.Fatalf("staging the superseded copy: %v", err)
	}

	// The client is connected, so the gate runs on delivery. It must NOT publish, and
	// the copy must go away rather than NAK-loop.
	if _, _, ready := testMQTTReadPacketReady(r, 1500*time.Millisecond); ready {
		t.Fatal("a PUBLISH arrived for a copy belonging to a superseded incarnation: the live claim does not authorize it")
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		st, _ := b.asm.loadQ2AffCopy(supersededSubject)
		if st == mqttQ2AffCopyAbsent {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the superseded copy is still on the stream: the gate NAK'd it instead of disposing of it, and with no MaxDeliver it will do so forever")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The live claim is untouched: nothing was asserted about ITS delivery.
	after, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil || after == nil {
		t.Fatalf("the live claim vanished: %v", err)
	}
	if after.AffineDone || after.Lost || after.Discarded {
		t.Fatalf("disposing of a superseded copy wrote a terminal fact onto the LIVE claim: %+v", after)
	}
}

// TestQ2AffUnattributableClaimIsRefusedButDoesNotWedgeTheAccount pins the SPLIT
// between the two validators, which round 3 required and round 4 completed.
//
// A claim whose copy subject does not follow from its own identity cannot be acted
// on: the reaper reads that subject to decide whether to DELETE the claim, and
// recovery reads it before writing the incarnation-specific Lost and source-gone
// dispositions. Both refuse it.
//
// But it must still be ATTRIBUTABLE. mqttQ2AffClaimIdentityOK is the shared floor for
// the account-wide scans, where one failure aborts the whole walk — and from clear()
// that fails the discard, strands the teardown marker and locks an unrelated client ID
// out of its next CONNECT. Winner+Gen still attributes such a claim to its session, so
// the floor deliberately does NOT check the nonce.
//
// Paired mutations: M12a (drop the nonce from the operational validator) — the reaper
// then deletes a claim on a copy subject the record does not own; M13 (add the nonce
// checks to mqttQ2AffClaimIdentityOK) — the account-wide discard then aborts.
func TestQ2AffUnattributableClaimIsRefusedButDoesNotWedgeTheAccount(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "unattrib")
	c, _ := b.member(t, "unattribA", 300)
	b.bind(t, "unattribA")
	idHash := getHash("unattribA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)
	b.asm.mu.RLock()
	sess := b.asm.sessions["unattribA"]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}
	testMQTTDisconnect(t, c, nil)
	c.Close()

	const srcSeq = uint64(644001)
	subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
	// Terminal and source-proven, so the ONLY thing standing between this record and a
	// reap is the refusal under test.
	bad := &mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
		ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq),
		Nonce:     mqttNewQ2AffGen(),
		// Derived for a DIFFERENT incarnation: a shape no election can emit.
		CopySubject: mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, srcSeq), mqttNewQ2AffGen()),
		AffDurable:  affDurableFor(idHash, gen),
		Phase:       mqttQ2AffMaterialized, SourceAcked: true, AffineDone: true,
	}
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, bad); err != nil {
		t.Fatalf("seeding the unattributable claim: %v", err)
	}
	if err := q2AffClaimUsableForDecision(subject, bad); err == nil {
		t.Fatal("the operational validator accepted a copy subject that does not follow from the record's own identity")
	}
	// Every unattributable SHAPE the plan names, not just the mismatched subject. A nonce
	// of the wrong LENGTH is the interesting one: its copy subject still derives from it
	// consistently, so only the minted-form check can refuse it.
	for name, mut := range map[string]func(*mqttQ2AffClaim){
		"no incarnation":        func(c *mqttQ2AffClaim) { c.Nonce = _EMPTY_ },
		"short incarnation":     func(c *mqttQ2AffClaim) { c.Nonce = "tooshort" },
		"out-of-alphabet nonce": func(c *mqttQ2AffClaim) { c.Nonce = strings.Repeat(".", mqttQ2AffGenLen) },
	} {
		shape := *bad
		mut(&shape)
		shape.CopySubject = mqttQ2AffCopySubject(shape.Winner, shape.Gen, shape.ClaimHash, shape.Nonce)
		if err := q2AffClaimUsableForDecision(subject, &shape); err == nil {
			t.Fatalf("a claim with a %s was accepted even though its copy subject derives from it: the incarnation would then be passed to the fence, where an unmintable value cannot match any real record", name)
		}
	}

	// The reaper refuses it. Without the refusal it would delete this claim on what a
	// copy subject the record does not own happens to say.
	b.asm.reapQ2AffClaim(b.s, subject)
	if got, _, err := b.asm.loadQ2AffClaim(subject); err != nil || got == nil {
		t.Fatal("the reaper acted on a claim whose copy subject does not follow from its own identity: it inspected a copy the record does not own")
	}

	// Recovery skips it WITHOUT failing, so an unrelated session's reconciliation is
	// unaffected.
	b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)
	if got, _, err := b.asm.loadQ2AffClaim(subject); err != nil || got == nil {
		t.Fatal("recovery acted on the unattributable claim")
	}

	// The shape above only proves recovery does not REAP, because its AffineDone arm
	// delegates to reapQ2AffClaim, whose own validator refuses the same record — so
	// removing the guard from recovery alone would leave that assertion green. This shape
	// reaches recovery's own WRITE instead: materialized, not terminal, copy absent, and
	// an incarnation that is malformed but SELF-CONSISTENT, so the derivation check passes
	// and the nonce-equality fence cannot save it either. Without the guard, recovery
	// marks it LOST — a terminal, never-reapable verdict against a record the broker
	// cannot attribute.
	const lostSeq = uint64(644003)
	lostSubject := mqttQ2AffClaimSubject(groupDur, lostSeq)
	shortNonce := strings.Repeat("q", mqttQ2AffGenLen-1)
	lostClaim := &mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: lostSeq,
		ClaimHash: mqttQ2AffClaimHash(groupDur, lostSeq), Nonce: shortNonce,
		AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffMaterialized,
	}
	lostClaim.CopySubject = mqttQ2AffCopySubject(idHash, gen, lostClaim.ClaimHash, shortNonce)
	if q2AffClaimUsableForDecision(lostSubject, lostClaim) == nil {
		t.Fatal("the staged claim is attributable, so it cannot exercise recovery's own refusal")
	}
	if _, err := b.asm.storeQ2AffClaimCAS(lostSubject, 0, lostClaim); err != nil {
		t.Fatalf("seeding the claim recovery would mark lost: %v", err)
	}
	b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)
	lostAfter, _, err := b.asm.loadQ2AffClaim(lostSubject)
	if err != nil || lostAfter == nil {
		t.Fatalf("the claim vanished: %v", err)
	}
	if lostAfter.Lost {
		t.Fatal("recovery recorded LOST against a claim the broker cannot attribute: that verdict is terminal and never reapable, so an unattributable record becomes a permanent loss report")
	}

	// The account-wide floor still ATTRIBUTES it, so the walk completes and the
	// teardown discard lands. This is the half that must not tighten.
	if !mqttQ2AffClaimIdentityOK(bad) {
		t.Fatal("the shared attribution floor rejected a claim its winner and generation identify: one such record would abort every account-wide walk, and from clear() would strand the teardown marker")
	}
	if err := b.asm.discardQ2AffClaimsFor(idHash, gen); err != nil {
		t.Fatalf("the account-wide discard was aborted by one unattributable claim: %v", err)
	}
	after, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("loading after the discard: %v", err)
	}
	if after == nil || !after.Discarded {
		t.Fatalf("teardown could not discard a claim its own session owns: %+v", after)
	}
}

// TestQ2AffRetiredAffineSourceDischargesItsOwnIncarnation covers the recovery route
// round 5 found unowned, and the disposition round 6 found missing from it.
//
// A successful PUBREC stores the correlated PUBREL BEFORE it records AffineDone. A
// crash between those two leaves both the PUBREL and the in-flight register; on
// restart the PUBREL wins over pendingPublish, so the register is classified RETIRED
// and its claim reference lands in mqttRetiredSource. Nothing used to discharge the
// affine side from there: the affine callback went straight to trackPublish, which
// returns identifier zero because the restored PUBREL already reserves that source,
// and returned without acknowledging the copy — so the copy NAK-looped and the claim
// never completed. F-065's own harm, re-entered through recovery.
//
// Two dispositions are asserted, because the incarnation is what distinguishes them:
//
//   - the SAME incarnation: AffineDone is recorded, then the copy is acked, then the
//     retirement and its register are cleared. Persist before ack, ack before forget.
//   - a REPLACEMENT incarnation: the copy is disposed of and the retirement cleared,
//     but NOTHING is written onto the replacement — it made no delivery and must not
//     be credited with one. Round 6's finding: without this the path either NAK-loops
//     the copy forever or strands the retirement.
//
// Paired mutations: M16b (drop the incarnation from mqttRetiredSource, so the retired
// route cannot fence its write) and M16c (make the mismatch disposition NAK, or ack
// without clearing).
func TestQ2AffRetiredAffineSourceDischargesItsOwnIncarnation(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "retired")
	c, _ := b.member(t, "retiredA", 300)
	b.bind(t, "retiredA")
	idHash := getHash("retiredA")
	sessRec := testQ2AffAwaitStableRecord(t, b, idHash)
	gen := sessRec.Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)
	// Taken while the client is still connected: sessions is keyed by CLIENT ID, and a
	// disconnect removes the resident entry.
	b.asm.mu.RLock()
	sess := b.asm.sessions["retiredA"]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}
	sess.mu.Lock()
	sessGen := sess.gen
	sess.mu.Unlock()
	testMQTTDisconnect(t, c, nil)
	c.Close()

	var supersededCopy atomic.Value
	supersededCopy.Store(_EMPTY_)
	stage := func(t *testing.T, srcSeq uint64) (*mqttQ2AffClaim, string, uint64) {
		t.Helper()
		subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
		cl := testQ2AffSelfConsistent(&mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
			ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq), Nonce: mqttNewQ2AffGen(),
			AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffMaterialized,
		})
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, cl); err != nil {
			t.Fatalf("seeding the claim: %v", err)
		}
		env := &mqttQ2AffineEnvelope{
			natsSubject: b.topic, msg: []byte("retired-body"),
			claimSubject: subject, gen: gen,
			msgID: mqttQ2AffMsgId(groupDur, srcSeq, cl.Nonce),
		}
		msg, hdr := mqttNewQ2AffineRecord(env)
		seq, err := b.asm.storeQ2AffCopy(cl.CopySubject, hdr, msg)
		if err != nil {
			t.Fatalf("staging the copy: %v", err)
		}
		return cl, subject, seq
	}

	// The confirmed ack is stubbed, because the ORDER is what this test pins and each
	// step has to be able to succeed or fail on its own. A real confirmed ack needs a
	// live JetStream delivery, which cannot be staged around a seeded claim.
	ackOK := func() func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyAck = func(string) error { return nil }
		b.asm.mu.Unlock()
		return func() { b.asm.mu.Lock(); b.asm.testQ2AffCopyAck = nil; b.asm.mu.Unlock() }
	}
	ackFails := func() func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyAck = func(string) error { return fmt.Errorf("ack unconfirmed") }
		b.asm.mu.Unlock()
		return func() { b.asm.mu.Lock(); b.asm.testQ2AffCopyAck = nil; b.asm.mu.Unlock() }
	}
	retired := func(t *testing.T, jsDur string, copySeq uint64) bool {
		t.Helper()
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.sourceRetiredLocked(jsDur, copySeq)
	}

	t.Run("an unconfirmed copy ack retains the retirement", func(t *testing.T) {
		cl, subject, _ := stage(t, 555003)
		const jsDur, copySeq = "$MQTT_Q2AFF_retiredunconf", uint64(43)
		sess.mu.Lock()
		sess.retireSourceLocked(jsDur, copySeq, &mqttRetiredSource{
			pi: 9, nonce: "regnonce3", q2AffClaim: subject, q2AffCopy: cl.CopySubject, durable: true,
		})
		sess.mu.Unlock()
		defer ackFails()()

		sess.settleRetiredAffineSource(testQ2AffSessionClient(b, sess), jsDur, copySeq, "ack.subject", cl.CopySubject, subject, cl.CopySubject)

		got, _, _ := b.asm.loadQ2AffClaim(subject)
		if got == nil || !got.AffineDone {
			t.Fatalf("the affine side was not persisted BEFORE the ack was attempted: %+v", got)
		}
		if !retired(t, jsDur, copySeq) {
			t.Fatal("the retirement was cleared although the copy ack was never confirmed: a lost ack then leaves the copy re-offerable with the state that governs it already gone")
		}
	})

	t.Run("a failed AffineDone acknowledges nothing", func(t *testing.T) {
		cl, subject, _ := stage(t, 555004)
		const jsDur, copySeq = "$MQTT_Q2AFF_retiredfailed", uint64(44)
		sess.mu.Lock()
		sess.retireSourceLocked(jsDur, copySeq, &mqttRetiredSource{
			pi: 10, nonce: "regnonce4", q2AffClaim: subject, q2AffCopy: cl.CopySubject, durable: true,
		})
		sess.mu.Unlock()
		var acked bool
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyAck = func(string) error { acked = true; return nil }
		b.asm.testQ2AffClaimStore = func(string, uint64, *mqttQ2AffClaim) (uint64, error) {
			return 0, fmt.Errorf("backend unavailable")
		}
		b.asm.mu.Unlock()
		defer func() {
			b.asm.mu.Lock()
			b.asm.testQ2AffCopyAck, b.asm.testQ2AffClaimStore = nil, nil
			b.asm.mu.Unlock()
		}()

		sess.settleRetiredAffineSource(testQ2AffSessionClient(b, sess), jsDur, copySeq, "ack.subject", cl.CopySubject, subject, cl.CopySubject)

		if acked {
			t.Fatal("the copy was acknowledged although its affine side was never recorded: an absent copy under a claim with no AffineDone is indistinguishable from a lost one")
		}
		if !retired(t, jsDur, copySeq) {
			t.Fatal("the retirement was cleared although nothing was proven")
		}
	})

	t.Run("same incarnation: the affine side is recorded and the retirement cleared", func(t *testing.T) {
		defer ackOK()()
		cl, subject, _ := stage(t, 555001)
		const jsDur, copySeq = "$MQTT_Q2AFF_retiredsame", uint64(41)
		sess.mu.Lock()
		sess.retireSourceLocked(jsDur, copySeq, &mqttRetiredSource{
			pi: 7, nonce: "regnonce", q2AffClaim: subject, q2AffCopy: cl.CopySubject, durable: true,
		})
		installed := sess.sourceRetiredLocked(jsDur, copySeq)
		sess.mu.Unlock()
		if !installed {
			t.Fatal("the retirement was not installed, so this run exercises nothing")
		}

		sess.settleRetiredAffineSource(testQ2AffSessionClient(b, sess), jsDur, copySeq,
			"ack.subject", cl.CopySubject, subject, cl.CopySubject)

		got, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil {
			t.Fatalf("loading the claim: %v", err)
		}
		if got == nil || !got.AffineDone {
			t.Fatalf("the retired route did not record the affine side against its own incarnation: %+v", got)
		}
		if retired(t, jsDur, copySeq) {
			t.Fatal("the retirement survived a settled affine source, so every later redelivery repeats the work")
		}
	})

	t.Run("replacement incarnation: the copy is disposed and nothing is written to it", func(t *testing.T) {
		// The stubbed ack RECORDS its reply and removes the staged copy, because a
		// confirmed acknowledgement is what disposes of it in production. Review found
		// the earlier version proved neither: it returned success without touching the
		// copy, so it passed even if the mismatch branch cleared the retirement without
		// ever confirming an acknowledgement.
		var ackedReply atomic.Value
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyAck = func(reply string) error {
			ackedReply.Store(reply)
			if _, sm := b.asm.loadQ2AffCopy(supersededCopy.Load().(string)); sm != nil {
				if derr := b.asm.jsa.deleteMsg(mqttOutStreamName, sm.Sequence, true); derr != nil {
					t.Errorf("disposing of the acknowledged copy: %v", derr)
				}
			}
			return nil
		}
		b.asm.mu.Unlock()
		defer func() { b.asm.mu.Lock(); b.asm.testQ2AffCopyAck = nil; b.asm.mu.Unlock() }()
		cl, subject, _ := stage(t, 555002)
		supersededCopy.Store(cl.CopySubject)
		// The claim on the subject is now a LATER incarnation.
		_, seq, _ := b.asm.loadQ2AffClaim(subject)
		if err := b.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); err != nil {
			t.Fatalf("reaping the first incarnation: %v", err)
		}
		replacement := testQ2AffSelfConsistent(&mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: 555002,
			ClaimHash: mqttQ2AffClaimHash(groupDur, 555002), Nonce: mqttNewQ2AffGen(),
			AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffClaimed,
		})
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, replacement); err != nil {
			t.Fatalf("electing the replacement: %v", err)
		}

		// A REAL persisted register, so the exact-sequence delete has something to
		// target and its omission is observable. regSeq == 0 made the earlier version
		// unable to detect that omission at all.
		const jsDur, copySeq = "$MQTT_Q2AFF_retiredrepl", uint64(42)
		regNonce := mqttNewInflightNonce()
		regSubject := mqttInflightSubject(idHash, sessGen, 8)
		regSeq, rerr := b.asm.jsa.storeInflightCAS(regSubject, 0, freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
			PI: 8, Dur: jsDur, Seq: copySeq, QoS: 2, Nonce: regNonce,
			Q2AffClaim: subject, Q2AffCopy: cl.CopySubject, Retired: true,
		}))
		if rerr != nil {
			t.Fatalf("persisting the retired register: %v", rerr)
		}
		sess.mu.Lock()
		sess.retireSourceLocked(jsDur, copySeq, &mqttRetiredSource{
			pi: 8, regSeq: regSeq, nonce: regNonce, q2AffClaim: subject, q2AffCopy: cl.CopySubject, durable: true,
		})
		sess.mu.Unlock()

		sess.settleRetiredAffineSource(testQ2AffSessionClient(b, sess), jsDur, copySeq,
			"ack.subject", cl.CopySubject, subject, cl.CopySubject)

		after, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil || after == nil {
			t.Fatalf("the replacement claim is gone: %v", err)
		}
		if after.Nonce != replacement.Nonce {
			t.Fatalf("the record no longer names the replacement: %+v", after)
		}
		if after.AffineDone || after.Lost || after.Discarded {
			t.Fatalf("a superseded incarnation's settlement credited the REPLACEMENT with a delivery it never made: %+v", after)
		}
		if retired(t, jsDur, copySeq) {
			t.Fatal("the retirement survived: the superseded copy is disposed of but its retirement and register are stranded, so the copy NAK-loops forever")
		}
		// The three things review required this subtest to prove, none of which the
		// earlier version could: the acknowledgement was CONFIRMED, the superseded copy
		// is gone, and the register was deleted at its exact sequence.
		if ackedReply.Load() == nil {
			t.Fatal("the retirement was cleared without confirming the copy's acknowledgement")
		}
		if st, _ := b.asm.loadQ2AffCopy(cl.CopySubject); st != mqttQ2AffCopyAbsent {
			t.Fatalf("the superseded copy survived its disposal (%v)", st)
		}
		checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
			rec, _, lerr := b.asm.jsa.loadInflightRecord(regSubject)
			if lerr != nil {
				return fmt.Errorf("loading the register: %w", lerr)
			}
			if rec != nil {
				return fmt.Errorf("the retired register was not deleted at its exact sequence: %+v", rec)
			}
			return nil
		})
	})

	// The other side of that disposal, and the reason it is guarded rather than
	// unconditional: the copy IN HAND may be the SUCCESSOR'S own. Disposing of it on the
	// strength of a stale carrier destroys a live claim's message — the same loss the
	// carried pair exists to prevent, committed by the remedy for it.
	t.Run("a successor's own copy in hand is disposed of by nothing here", func(t *testing.T) {
		var acked atomic.Bool
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyAck = func(string) error { acked.Store(true); return nil }
		b.asm.mu.Unlock()
		defer func() { b.asm.mu.Lock(); b.asm.testQ2AffCopyAck = nil; b.asm.mu.Unlock() }()

		const srcSeq = uint64(555005)
		cl, subject, _ := stage(t, srcSeq)
		_, seq, _ := b.asm.loadQ2AffClaim(subject)
		if err := b.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); err != nil {
			t.Fatalf("reaping the first incarnation: %v", err)
		}
		replacement := testQ2AffSelfConsistent(&mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
			ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq), Nonce: mqttNewQ2AffGen(),
			AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffMaterialized,
		})
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, replacement); err != nil {
			t.Fatalf("electing the replacement: %v", err)
		}
		// The successor's copy is REAL and present, so its survival is an observation
		// rather than the absence of a fixture.
		env := &mqttQ2AffineEnvelope{
			natsSubject: b.topic, msg: []byte("successor-body"),
			claimSubject: subject, gen: gen,
			msgID: mqttQ2AffMsgId(groupDur, srcSeq, replacement.Nonce),
		}
		msg, hdr := mqttNewQ2AffineRecord(env)
		if _, err := b.asm.storeQ2AffCopy(replacement.CopySubject, hdr, msg); err != nil {
			t.Fatalf("staging the successor's copy: %v", err)
		}
		const jsDur, copySeq = "$MQTT_Q2AFF_retiredsucc", uint64(45)
		sess.mu.Lock()
		sess.retireSourceLocked(jsDur, copySeq, &mqttRetiredSource{
			pi: 11, nonce: "regnonce5", q2AffClaim: subject, q2AffCopy: cl.CopySubject, durable: true,
		})
		sess.mu.Unlock()

		// The retirement carries the SUPERSEDED copy; the delivery in hand is the
		// SUCCESSOR'S.
		sess.settleRetiredAffineSource(testQ2AffSessionClient(b, sess), jsDur, copySeq,
			"ack.subject", replacement.CopySubject, subject, cl.CopySubject)

		if acked.Load() {
			t.Fatal("the successor's own copy was acknowledged away on the strength of a stale carrier: that message was never delivered and can never be delivered now")
		}
		if st, _ := b.asm.loadQ2AffCopy(replacement.CopySubject); st != mqttQ2AffCopyPresent {
			t.Fatal("the successor's copy is gone")
		}
		after, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil || after == nil {
			t.Fatalf("the replacement claim is gone: %v", err)
		}
		if after.AffineDone || after.Lost || after.Discarded {
			t.Fatalf("a stale carrier wrote to the successor's claim: %+v", after)
		}
		if !retired(t, jsDur, copySeq) {
			t.Fatal("the retirement was cleared although nothing about it was settled")
		}
	})
}

// TestQ2AffFinishRefusesAForeignIncarnation pins the carried incarnation on the path
// where nothing else can supply it.
//
// The terminal transition runs from PUBREC, expiry and the oversize discard, and all
// three see a packet identifier and nothing else — no delivered subject to derive the
// incarnation from. So the incarnation travels with the claim subject: in memory on
// the pending entry, and durably in the in-flight register for the restart case.
// Without it the transition credits whatever claim occupies the subject at PUBREC
// time, which after a reap-and-re-election is a claim that made no delivery at all.
//
// Deliberately separate from the retirement route, which has a delivered copy subject to
// compare its carried one against; here there is nothing but the carrier. That distinction
// is why this mutation pairs here and not there.
//
// Paired mutations M16a/M16b: stop carrying the incarnation (pending entry, or the
// in-flight register for the restart case), leaving the transition fenced by nothing.
func TestQ2AffFinishRefusesAForeignIncarnation(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "finishinc")
	c, _ := b.member(t, "finishincA", 300)
	defer c.Close()
	b.bind(t, "finishincA")
	idHash := getHash("finishincA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)
	b.asm.mu.RLock()
	sess := b.asm.sessions["finishincA"]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}
	cc := testQ2AffSessionClient(b, sess)
	cc.mqtt = &mqtt{asm: b.asm, sess: sess}

	const srcSeq = uint64(466001)
	subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
	// The copy the delivery carried, belonging to an incarnation since reaped. It is a
	// WELL-FORMED copy subject of this very claim, so the refusal below is attributable to
	// the incarnation mismatch and not to a malformed carrier — a bare nonce, or any value
	// that is not a copy subject at all, is refused one step earlier and would prove
	// nothing about the fence.
	supersededCopy := mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, srcSeq), mqttNewQ2AffGen())
	// The claim now on the subject is a REPLACEMENT: same winner, same generation, same
	// claim hash — only the incarnation differs, which is the whole point.
	replacement := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
		ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq), Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffMaterialized,
	})
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, replacement); err != nil {
		t.Fatalf("electing the replacement: %v", err)
	}
	if replacement.CopySubject == supersededCopy {
		t.Fatal("the two incarnations derived one copy subject, so this fixture stages nothing")
	}

	// A PUBREC belonging to the OLD incarnation must not complete the replacement.
	cc.mqttFinishQ2AffineDelivery(subject, supersededCopy, _EMPTY_)
	after, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil || after == nil {
		t.Fatalf("the replacement claim is gone: %v", err)
	}
	if after.AffineDone {
		t.Fatal("a PUBREC belonging to a reaped incarnation recorded the affine side of the REPLACEMENT: that claim made no delivery, and its own copy would become reapable behind a completion it never earned (F-065)")
	}

	// The matching incarnation DOES complete it, so the refusal is a fence rather than
	// a blanket refusal that would stop every delivery from ever completing.
	cc.mqttFinishQ2AffineDelivery(subject, replacement.CopySubject, _EMPTY_)
	done, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil || done == nil {
		t.Fatalf("loading after the matching completion: %v", err)
	}
	if !done.AffineDone {
		t.Fatal("the transition refused its OWN incarnation, so no delivery could ever complete")
	}
}

// TestQ2AffRetiredAffineSourceRecoveredEndToEnd is the fixture the holistic review
// demanded, and the reason it exists is worth stating: the first version of this
// coverage seeded an in-memory mqttRetiredSource directly, stubbed the confirmed ack
// and called the settlement function, so it created no persisted register, performed
// no restart, and asserted no deletion. It could pass while register validation,
// propagation, classification and deletion were all broken — and they were: the
// review found the register deleted after a FAILED transition, a nonceless register
// silently opting out of the incarnation fence, and the settlement ignoring its own
// durable identity.
//
// So this one drives the whole route: a real shared QoS 2 delivery, a real PUBREC
// whose affine transition FAILS (leaving the correlated PUBREL stored and the register
// as the only surviving carrier of the claim and its incarnation), a real restart, and
// then the redelivery that must settle it. Nothing is stubbed except the one claim CAS
// that has to fail.
func TestQ2AffRetiredAffineSourceRecoveredEndToEnd(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	b := &testQ2AffBroker{s: s, o: o, topic: "e2eretired/topic", group: "e2eretiredgrp"}

	c, r := b.member(t, "e2eretiredA", 300)
	b.bind(t, "e2eretiredA")
	idHash := getHash("e2eretiredA")
	asm := b.asm
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)

	// The affine transition fails at PUBREC. The PUBREL is already stored by then, so
	// this is exactly the crash window: nothing records AffineDone, and the register
	// must therefore SURVIVE as the only carrier of the claim and its incarnation.
	// Route everything except the AffineDone write to the real implementation.
	// The hook also pins the ORDER, deterministically rather than by racing AckWait: at
	// the moment the AffineDone write is attempted the retirement must ALREADY be
	// installed. This handler has removed the pending entry and its source mapping by
	// then, so a redelivery arriving during these retries would allocate a fresh
	// identifier and re-send a payload the client has already answered with PUBREC —
	// which is precisely what installing the retirement afterwards allows.
	var retiredWhenAttempted atomic.Bool
	var affineAttempts atomic.Int32
	asm.mu.Lock()
	asm.testQ2AffClaimStore = func(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
		if rec.AffineDone {
			affineAttempts.Add(1)
			sess := func() *mqttSession {
				asm.mu.RLock()
				defer asm.mu.RUnlock()
				return asm.sessions["e2eretiredA"]
			}()
			if sess != nil {
				sess.mu.Lock()
				if len(sess.srcAckOwed) > 0 {
					retiredWhenAttempted.Store(true)
				}
				sess.mu.Unlock()
			}
			return 0, fmt.Errorf("test: the affine completion is unproven")
		}
		return asm.jsa.storeQ2AffClaimCAS(subject, expectSeq, rec)
	}
	asm.mu.Unlock()

	b.publish(t, 1, "e2e-retired")
	topic, payload, qos, pi, ok := testMQTTTryReadPublishV5(r, 5*time.Second)
	if !ok || qos != 2 || topic != b.topic || string(payload) != "e2e-retired" {
		t.Fatalf("the member did not receive the shared QoS 2 delivery: topic=%q payload=%q qos=%d ok=%v", topic, payload, qos, ok)
	}
	// The client completes the handshake IN FULL, PUBCOMP included, because that is the
	// ordinary case and it is the one that exposes the defect holistic round 2 found:
	// retaining the register is not enough on its own, since a PUBCOMP that finds no
	// retirement acknowledges the PUBREL and removes the last durable evidence that this
	// source is past PUBREC. An earlier version of this fixture stopped at PUBREL and
	// therefore passed while that route was broken.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	testMQTTFlush(t, c, nil, r)

	// The claim must NOT be complete, and its register must still be there: that pair
	// is the state the restart has to recover from.
	claim := testQ2AffAwaitClaimFor(t, asm, groupDur)
	if claim == nil {
		t.Fatal("no claim was elected for the shared delivery")
	}
	if claim.AffineDone {
		t.Fatal("the affine side was recorded although its write was made to fail: this run does not stage the window")
	}
	if claim.Nonce == _EMPTY_ {
		t.Fatal("the elected claim carries no incarnation")
	}
	asm.mu.Lock()
	asm.testQ2AffClaimStore = nil
	asm.mu.Unlock()

	// The register subject carries the SESSION generation, which is a different
	// identity from the affine one — mixing them up finds nothing and reads as a
	// deleted register.
	asm.mu.RLock()
	sess := asm.sessions["e2eretiredA"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the member's session is not resident")
	}
	sess.mu.Lock()
	sessGen := sess.gen
	sess.mu.Unlock()
	if affineAttempts.Load() == 0 {
		t.Fatal("the affine completion was never attempted, so this run does not stage the window")
	}
	if !retiredWhenAttempted.Load() {
		t.Fatal("the source was not retired BEFORE the affine completion was attempted: the pending entry and its source mapping are already gone, so an AckWait redelivery inside those retries allocates a fresh identifier and re-sends a payload the client has answered with PUBREC")
	}
	rec, _, err := asm.jsa.loadInflightRecord(mqttInflightSubject(idHash, sessGen, pi))
	if err != nil {
		t.Fatalf("loading the register: %v", err)
	}
	if rec == nil {
		t.Fatal("the in-flight register was deleted after a FAILED affine transition: it was the only surviving carrier of the claim and its incarnation, so nothing can reconstruct the retirement")
	}
	if rec.Q2AffClaim == _EMPTY_ || rec.Q2AffCopy != claim.CopySubject {
		t.Fatalf("the register does not carry the claim AND the copy it was delivered under: %+v (claim copy %q)", rec, claim.CopySubject)
	}
	c.Close()

	// Restart, so recovery is the thing that rebuilds the retirement.
	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)
	b.s = s

	// Reconnect FIRST: the account manager is looked up through a connected client.
	c2, r2 := b.member(t, "e2eretiredA", 300)
	defer c2.Close()
	asm = testMQTTGetAccountSessionManager(t, s, "e2eretiredA")

	// The client is past PUBREC, so the stored PUBREL is legitimately REPLAYED on
	// resume and must be answered; what must never arrive is another PUBLISH, which
	// would re-send the payload of an exchange the client has already answered. So the
	// assertion is on the packet TYPE, not on silence.
	deadline := time.Now().Add(3 * o.MQTT.AckWait)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r2, 500*time.Millisecond)
		if !ready {
			continue
		}
		start := r2.pos
		switch pb & mqttPacketMask {
		case mqttPacketPubRel:
			got, rerr := r2.readUint16("pi")
			if rerr != nil {
				t.Fatalf("reading the replayed PUBREL: %v", rerr)
			}
			r2.pos = start + pl
			testMQTTSendPIPacket(mqttPacketPubComp, t, c2, got)
		case mqttPacketPub:
			r2.pos = start + pl
			t.Fatal("the payload was re-published after recovery: the client had already answered PUBREC, so this is a duplicate QoS 2 delivery")
		default:
			r2.pos = start + pl
		}
	}
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		got, _, lerr := asm.loadQ2AffClaim(mqttQ2AffClaimSubject(claim.GroupDurable, claim.SourceSeq))
		if lerr != nil {
			return fmt.Errorf("loading the claim: %w", lerr)
		}
		if got == nil {
			return nil // reaped: both sides proven, which is the completed outcome
		}
		if !got.AffineDone {
			return fmt.Errorf("the recovered retirement never recorded the affine side against its own incarnation: %+v", got)
		}
		return nil
	})
	if st, _ := asm.loadQ2AffCopy(claim.CopySubject); st == mqttQ2AffCopyPresent {
		t.Fatal("the affine copy is still on the stream after recovery settled its source: it can only NAK from here")
	}
}

// TestQ2AffRegisterWithoutAnIncarnationIsRefused pins the refusal the holistic review
// found missing, and the reason it must BE a refusal rather than a tolerated gap:
// _EMPTY_ is the deliberate OPT-OUT value at advanceQ2AffClaim's incarnation fence, so
// a restored register naming an affine claim with no valid incarnation would silently
// disable the fence and let a stale exchange record AffineDone against a replacement
// claim — the exact write F-065 exists to prevent. There are no releases, so that
// shape does not exist; meeting one means the state is not interpretable.
//
// The register carries the COPY SUBJECT, so the incarnation is read back OUT of it,
// which is why every refused case here is a subject rather than a bare nonce: a
// truncated subject, or one whose last token is not a NUID, yields no incarnation and
// must be refused for the same reason an absent one is.
//
// Paired mutation: drop the Q2AffCopy clause from mqttInflightRecordIdentityOK.
func TestQ2AffRegisterWithoutAnIncarnationIsRefused(t *testing.T) {
	t.Parallel()
	const groupDur = "$MQTT_SHARED_abcdef0123456789"
	idHash, affGen := getHash("regincarn"), mqttNewQ2AffGen()
	claimHash := mqttQ2AffClaimHash(groupDur, 11)
	base := func() *mqttInflightRecord {
		return freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
			PI: 3, Dur: groupDur, Seq: 11, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		})
	}
	ordinary := base()
	if !mqttInflightRecordIdentityOK(ordinary) {
		t.Fatal("an ordinary QoS 2 register carrying no affine claim was refused")
	}
	withBoth := base()
	withBoth.Q2AffClaim = mqttQ2AffClaimSubject(groupDur, 11)
	withBoth.Q2AffCopy = mqttQ2AffCopySubject(idHash, affGen, claimHash, mqttNewQ2AffGen())
	if !mqttInflightRecordIdentityOK(withBoth) {
		t.Fatal("a register carrying an affine claim AND the copy it was delivered under was refused")
	}
	for name, copySubject := range map[string]string{
		"empty":                  _EMPTY_,
		"a bare incarnation":     mqttNewQ2AffGen(),
		"outside the namespace":  "foo." + idHash + "." + affGen + "." + claimHash + "." + mqttNewQ2AffGen(),
		"no incarnation token":   mqttQ2AffineSubjectPrefix + idHash + "." + affGen + "." + claimHash,
		"an extra token":         mqttQ2AffCopySubject(idHash, affGen, claimHash, mqttNewQ2AffGen()) + ".x",
		"incarnation not a NUID": mqttQ2AffCopySubject(idHash, affGen, claimHash, "abc"),
		"wrong length NUID":      mqttQ2AffCopySubject(idHash, affGen, claimHash, mqttNewQ2AffGen()+"x"),
	} {
		t.Run(name, func(t *testing.T) {
			rec := base()
			rec.Q2AffClaim = mqttQ2AffClaimSubject(groupDur, 11)
			rec.Q2AffCopy = copySubject
			if mqttInflightRecordIdentityOK(rec) {
				t.Fatalf("a register naming an affine claim with %s as its copy was ACCEPTED: recovery would restore it, PUBREC would pass a value the fence reads as no incarnation, and the fence would opt out — marking a replacement claim complete for a delivery it never made", name)
			}
		})
	}
}

// TestQ2AffRetirementSettlesFromItsDurableIdentity pins the second half of the same
// review finding: the retirement settlement must decide from the identity it PERSISTED,
// not from arguments derived at the current delivery.
//
// That is the whole point of persisting it. The register is what survived the restart;
// a delivered subject only says which copy is in hand, never which incarnation the
// exchange that retired this source belonged to. When the two disagree, the durable one
// is the truth — and if the settlement prefers the arguments, the persisted identity is
// dead data, which is exactly why a mutation removing it once survived.
//
// Paired mutation: drop the srcAckOwed lookup so the caller's arguments win.
func TestQ2AffRetirementSettlesFromItsDurableIdentity(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "durident")
	c, _ := b.member(t, "duridentA", 300)
	b.bind(t, "duridentA")
	idHash := getHash("duridentA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)
	b.asm.mu.RLock()
	sess := b.asm.sessions["duridentA"]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}
	testMQTTDisconnect(t, c, nil)
	c.Close()

	const srcSeq = uint64(377001)
	subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
	live := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
		ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq), Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffMaterialized,
	})
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, live); err != nil {
		t.Fatalf("seeding the live claim: %v", err)
	}

	// The retirement carries the LIVE incarnation, because that is the exchange that
	// retired this source. The caller's arguments name a different one.
	const jsDur, copySeq = "$MQTT_Q2AFF_durident", uint64(77)
	sess.mu.Lock()
	sess.retireSourceLocked(jsDur, copySeq, &mqttRetiredSource{
		pi: 5, nonce: "regnonce", q2AffClaim: subject, q2AffCopy: live.CopySubject, durable: true,
	})
	sess.mu.Unlock()
	b.asm.mu.Lock()
	b.asm.testQ2AffCopyAck = func(string) error { return nil }
	b.asm.mu.Unlock()
	defer func() { b.asm.mu.Lock(); b.asm.testQ2AffCopyAck = nil; b.asm.mu.Unlock() }()

	staleCopy := mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, 999999), mqttNewQ2AffGen())
	sess.settleRetiredAffineSource(testQ2AffSessionClient(b, sess), jsDur, copySeq, "ack.subject",
		_EMPTY_, mqttQ2AffClaimSubject(groupDur, 999999), staleCopy)

	got, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got == nil {
		return // reaped: both sides proven, the completed outcome
	}
	if !got.AffineDone {
		t.Fatal("the settlement decided from its caller's arguments rather than from the identity it persisted: the retirement it was asked to settle names a live incarnation, so the persisted identity is the truth and the arguments are stale")
	}
}

// TestQ2AffNegativePubRecAcquiresNoRetirement pins the boundary of the retirement
// machinery, and the shape of this fixture was ruled by review rather than chosen by
// me — my previous two attempts in this area each passed while avoiding the route they
// claimed to cover.
//
// The affine PUBREC arm is selected by the claim subject alone, so it also sees a
// NEGATIVE PUBREC. That path creates no pendingPubRel entry and stores no PUBREL, so
// there is no PUBCOMP — and therefore no event at which an unlanded durable retirement
// mark could ever be retried. Retiring there would install machinery whose retry point
// does not exist. A rejected delivery keeps its pre-F-065 behaviour instead: complete
// the affine side if it can be, then release the register.
//
// The negative PUBREC is written on the WIRE with a reason code >= 0x80 and goes
// through the read loop. Calling the completion routine directly, seeding a retirement,
// or using the success-shaped PUBREC helper would each dodge the route under test.
//
// Paired mutation: remove the isPubRec && !pubRecFailed gate. A retirement is then
// installed and a Retired register rewrite attempted, and both assertions below fire.
func TestQ2AffNegativePubRecAcquiresNoRetirement(t *testing.T) {
	b := testQ2AffSetup(t, "negrec")
	c, r := b.member(t, "negrecA", 300)
	defer c.Close()
	b.bind(t, "negrecA")
	idHash := getHash("negrecA")
	if gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen; gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	b.asm.mu.RLock()
	sess := b.asm.sessions["negrecA"]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}

	// Fail ONLY the AffineDone write, and count every attempt so the test can prove the
	// transition was actually reached rather than skipped.
	var affineAttempts atomic.Int32
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimStore = func(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
		if rec.AffineDone {
			affineAttempts.Add(1)
			return 0, fmt.Errorf("test: the affine completion is unproven")
		}
		return b.asm.jsa.storeQ2AffClaimCAS(subject, expectSeq, rec)
	}
	b.asm.mu.Unlock()
	sess.mu.Lock()
	sessGen := sess.gen
	sess.mu.Unlock()

	b.publish(t, 1, "rejected")
	topic, payload, qos, pi, ok := testMQTTTryReadPublishV5(r, 5*time.Second)
	if !ok || qos != 2 || topic != b.topic || string(payload) != "rejected" {
		t.Fatalf("the member did not receive the shared QoS 2 delivery: topic=%q payload=%q qos=%d ok=%v", topic, payload, qos, ok)
	}

	// A NEGATIVE PUBREC, on the wire: reason 0x80, through the read loop.
	if _, err := testMQTTWrite(c, []byte{mqttPacketPubRec, 3, byte(pi >> 8), byte(pi), 0x80}); err != nil {
		t.Fatalf("writing the negative PUBREC: %v", err)
	}
	testMQTTFlush(t, c, nil, r)

	// The transition must have been attempted and failed: otherwise this run proves
	// nothing about what happens when it does.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if affineAttempts.Load() == 0 {
			return fmt.Errorf("the affine completion was never attempted")
		}
		return nil
	})
	// No PUBREL, in memory or on the wire: a rejected delivery has no PUBREL phase, and
	// that absence is precisely why no retry point exists.
	sess.mu.Lock()
	pubRels := len(sess.pendingPubRel)
	owed := len(sess.srcAckOwed)
	sess.mu.Unlock()
	if pubRels != 0 {
		t.Fatalf("a rejected delivery entered the PUBREL phase (%d pending)", pubRels)
	}
	if owed != 0 {
		t.Fatalf("a retirement was installed for a rejected delivery (%d owed): there is no PUBCOMP on this path, so an unlanded durable mark could never be retried", owed)
	}
	// The register is RELEASED, as it was before F-065, and nothing rewrites it as
	// Retired. Polled, because a register delete is deliberately asynchronous and
	// ignores failure (R5) — so the claim pinned here is the narrower one review
	// insisted on: this branch no longer deliberately retains or rewrites such a
	// register, not that a stale one can never transiently exist.
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		rec, _, lerr := b.asm.jsa.loadInflightRecord(mqttInflightSubject(idHash, sessGen, pi))
		if lerr != nil {
			return fmt.Errorf("loading the register: %w", lerr)
		}
		if rec == nil {
			return nil
		}
		if rec.Retired {
			return fmt.Errorf("the register was rewritten as RETIRED for a rejected delivery: %+v", rec)
		}
		return fmt.Errorf("the register was retained for a rejected delivery: %+v", rec)
	})
	// No PUBREL on the wire either. A PUBLISH redelivery, by contrast, IS expected: the
	// affine write is failing, so the copy stays unacknowledged and AckWait re-offers
	// it. That re-publishing of a rejected message is the PRE-EXISTING negative-PUBREC
	// behaviour this change restores rather than introduces, it converges as soon as the
	// claim store heals, and it is named in the dossier's "what this does not close"
	// list. Asserting silence here would assert something F-065 never promised.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r, 250*time.Millisecond)
		if !ready {
			continue
		}
		start := r.pos
		pt := pb & mqttPacketMask
		r.pos = start + pl
		if pt == mqttPacketPubRel {
			t.Fatal("the broker sent a PUBREL after a negative PUBREC: a rejected delivery has no PUBREL phase, which is exactly why there is no PUBCOMP to retry a durable retirement mark")
		}
	}
}

// TestQ2AffRestoredPendingCarriesItsIncarnation closes the gap review demonstrated: the
// OTHER recovery route — a restart BEFORE PUBREC, where no PUBREL exists so recovery
// restores the register into pendingPublish rather than classifying it retired — had no
// discriminating fixture. Removing the incarnation from those restored entries leaves the
// claim subject intact and passes _EMPTY_ to the fence, which is the deliberate OPT-OUT
// value, so a post-restart PUBREC would credit a replacement claim with the old
// incarnation's delivery.
//
// It asserts the CARRIER rather than the end-to-end exchange, deliberately and with the
// reason stated: on reconnect the affine consumer promptly redelivers its pending copy,
// and the gate then legitimately re-attributes it to whatever claim now holds the subject
// — so an end-to-end version cannot isolate the restored entry, and would pass or fail on
// the redelivery instead. What the restored entry does WITH a carried incarnation is
// covered by TestQ2AffFinishRefusesAForeignIncarnation; together they close the route.
//
// Paired mutation M16a': drop `q2AffCopy: rv.rec.Q2AffCopy` from the restored entry.
func TestQ2AffRestoredPendingCarriesItsIncarnation(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 30 * time.Second // no redelivery inside the window under test
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	b := &testQ2AffBroker{s: s, o: o, topic: "restnonce/topic", group: "restnoncegrp"}

	c, r := b.member(t, "restnonceA", 300)
	defer c.Close()
	b.bind(t, "restnonceA")
	idHash := getHash("restnonceA")
	if gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen; gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)

	// A REAL delivery, so the register recovery classifies is one the broker actually
	// wrote — a synthetic record does not survive classification, and a fixture that
	// staged one would assert on a path recovery never took.
	b.publish(t, 1, "restored")
	topic, payload, qos, pi, ok := testMQTTTryReadPublishV5(r, 5*time.Second)
	if !ok || qos != 2 || topic != b.topic || string(payload) != "restored" {
		t.Fatalf("no shared QoS 2 delivery: topic=%q payload=%q qos=%d ok=%v", topic, payload, qos, ok)
	}
	claim := testQ2AffAwaitClaimFor(t, b.asm, groupDur)
	if claim == nil || claim.Nonce == _EMPTY_ {
		t.Fatalf("no claim with an incarnation was elected: %+v", claim)
	}
	b.asm.mu.RLock()
	sess := b.asm.sessions["restnonceA"]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}
	sess.mu.Lock()
	sessGen := sess.gen
	sess.mu.Unlock()
	// The register must be durable before recovery can restore it.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		rec, _, lerr := b.asm.jsa.loadInflightRecord(mqttInflightSubject(idHash, sessGen, pi))
		if lerr != nil {
			return fmt.Errorf("loading the register: %w", lerr)
		}
		if rec == nil {
			return fmt.Errorf("no register yet")
		}
		if rec.Q2AffCopy != claim.CopySubject {
			return fmt.Errorf("the register carries copy %q, want %q", rec.Q2AffCopy, claim.CopySubject)
		}
		return nil
	})

	// Drop the in-memory view and let RECOVERY rebuild it, which is what a restart does.
	// The client has not answered, so no PUBREL owns this identifier and the register is
	// restored into pendingPublish rather than classified retired.
	sess.mu.Lock()
	delete(sess.pendingPublish, pi)
	sess.mu.Unlock()
	if _, err := sess.recoverInflightRegisters(testQ2AffSessionClient(b, sess)); err != nil {
		t.Fatalf("recovering the registers: %v", err)
	}

	sess.mu.Lock()
	restored := sess.pendingPublish[pi]
	sess.mu.Unlock()
	if restored == nil {
		t.Fatal("recovery did not restore the register into pendingPublish, so the carrier under test was never exercised")
	}
	if restored.q2AffClaim != mqttQ2AffClaimSubject(claim.GroupDurable, claim.SourceSeq) {
		t.Fatalf("the restored entry names claim %q", restored.q2AffClaim)
	}
	if restored.q2AffCopy != claim.CopySubject {
		t.Fatalf("the restored entry carries copy %q, want %q: the terminal transition would then be handed the OPT-OUT value and could credit a replacement claim with this delivery (F-065)", restored.q2AffCopy, claim.CopySubject)
	}

	// And the incarnation must reach the TRANSITION, not merely the map. Review found
	// that inspecting pendingPublish here while a separate test called the finish routine
	// directly left a gap between them: a mutation that kept recovery correct but dropped
	// the incarnation when the PUBREC path REMOVES the pending entry would pass both. So
	// the client answers for real, against a subject a replacement incarnation now holds.
	subject := mqttQ2AffClaimSubject(claim.GroupDurable, claim.SourceSeq)
	_, seq, _ := b.asm.loadQ2AffClaim(subject)
	if err := b.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); err != nil {
		t.Fatalf("reaping the delivered incarnation: %v", err)
	}
	replacement := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: claim.Winner, Gen: claim.Gen, GroupDurable: claim.GroupDurable,
		SourceSeq: claim.SourceSeq, ClaimHash: claim.ClaimHash, Nonce: mqttNewQ2AffGen(),
		AffDurable: claim.AffDurable, Phase: mqttQ2AffMaterialized,
	})
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, replacement); err != nil {
		t.Fatalf("electing the replacement: %v", err)
	}

	// No flush: this connection may legitimately carry a resume-style resend of the
	// pending PUBLISH, and a flush would read that instead of a PINGRESP. The claim record
	// is the observable that matters here.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)

	// Wait for a POSITIVE signal that the PUBREC was actually PROCESSED — its PUBREL
	// tracking — before asserting anything about the claim. Polling for the ABSENCE of
	// AffineDone succeeds on the first sample, before the broker has even read the packet,
	// which is exactly why the first version of this assertion could not fail.
	sessObj := func() *mqttSession {
		b.asm.mu.RLock()
		defer b.asm.mu.RUnlock()
		return b.asm.sessions["restnonceA"]
	}()
	if sessObj == nil {
		t.Fatal("the session is not resident")
	}
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		sessObj.mu.Lock()
		_, tracked := sessObj.pendingPubRel[pi]
		sessObj.mu.Unlock()
		if !tracked {
			return fmt.Errorf("the PUBREC has not been processed yet")
		}
		return nil
	})
	final, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil || final == nil {
		t.Fatalf("loading the replacement claim: %v", err)
	}
	if final.AffineDone {
		t.Fatal("a PUBREC answered from a RESTORED pending entry credited a REPLACEMENT claim with the delivery: the incarnation did not survive the journey from the register through recovery to the terminal transition, so the fence was handed its opt-out value (F-065)")
	}
}

// TestQ2AffForgedRegisterCannotCreditAnotherClaim covers the third leg of the affine
// pair's binding, which is the one neither of the other two can reach.
//
// A register carrying (claim, copy) is checked in two places: its own shape
// (mqttInflightRecordIdentityOK) and the claim naming the carried copy back
// (q2AffClaimForCarriedCopy). Both are satisfied by a register that names ANOTHER live
// claim's copy — every field is well formed and mutually consistent, it is simply about a
// different delivery. Interpreting one credits that claim with a delivery it never made,
// and the gate's terminal branch then disposes of its copy on the next redelivery: the
// message is lost, which is exactly the F-065 failure in a different disguise. The
// arbiter is the SOURCE, because for an affine delivery the source IS the copy.
//
// Both forgeries are staged on ONE session with TWO claims, which is what makes the
// result attributable: both claims name the same affine durable, so every register here
// is shape-valid and only its source distinguishes them. A two-session fixture would
// have been refused by the owner check instead, and would have proved nothing about this.
//
// Paired mutations, one per subcase, and they are what tell the two legs apart:
//   - drop the affinePairContradictedBySource block in recoverInflightRegisters: the
//     internally consistent forgery is then installed and its PUBREC records AffineDone
//     on the victim.
//   - drop the claim.CopySubject == carriedCopy clause in q2AffClaimForCarriedCopy: the
//     mismatched forgery is then accepted at the transition and does the same.
func TestQ2AffForgedRegisterCannotCreditAnotherClaim(t *testing.T) {
	type delivery struct {
		pi          uint16
		regSubject  string
		claim       string
		copySubject string
	}
	type staged struct {
		b      *testQ2AffBroker
		c      net.Conn
		r      *mqttReader
		sess   *mqttSession
		answer delivery // the exchange whose PUBREC the client sends
		victim delivery // the claim a forged register would credit
	}

	// stage brings a session to TWO in-flight affine QoS 2 deliveries, each with a durable
	// register carrying its own pair. Nothing synthetic: a hand-built register does not
	// survive classification, so a fixture that staged one would assert on a route
	// recovery never takes.
	stage := func(t *testing.T, name string) *staged {
		t.Helper()
		o := testMQTTDefaultOptionsV5()
		o.MQTT.AckWait = 30 * time.Second // no redelivery inside the window under test
		s := testMQTTRunServer(t, o)
		t.Cleanup(func() { testMQTTShutdownServer(s) })
		b := &testQ2AffBroker{s: s, o: o, topic: name + "/topic", group: name + "grp"}
		id := name + "A"
		c, r := b.member(t, id, 300)
		t.Cleanup(func() { c.Close() })
		b.bind(t, id)
		idHash := getHash(id)
		if gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen; gen == _EMPTY_ {
			t.Fatal("no affine generation")
		}
		b.publish(t, 1, "first")
		b.publish(t, 2, "second")
		var pis []uint16
		for i := 0; i < 2; i++ {
			topic, _, qos, pi, ok := testMQTTTryReadPublishV5(r, 10*time.Second)
			if !ok || qos != 2 || topic != b.topic {
				t.Fatalf("shared QoS 2 delivery %d: topic=%q qos=%d ok=%v", i, topic, qos, ok)
			}
			pis = append(pis, pi)
		}
		b.asm.mu.RLock()
		sess := b.asm.sessions[id]
		b.asm.mu.RUnlock()
		if sess == nil {
			t.Fatal("the session is not resident")
		}
		sess.mu.Lock()
		sessGen := sess.gen
		sess.mu.Unlock()
		var ds []delivery
		checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
			ds = ds[:0]
			for _, pi := range pis {
				subj := mqttInflightSubject(idHash, sessGen, pi)
				rec, _, lerr := b.asm.jsa.loadInflightRecord(subj)
				if lerr != nil {
					return fmt.Errorf("loading the register of %d: %w", pi, lerr)
				}
				if rec == nil {
					return fmt.Errorf("no register for %d yet", pi)
				}
				if rec.Q2AffClaim == _EMPTY_ || rec.Q2AffCopy == _EMPTY_ {
					return fmt.Errorf("the register for %d carries no affine pair yet", pi)
				}
				ds = append(ds, delivery{pi: pi, regSubject: subj, claim: rec.Q2AffClaim, copySubject: rec.Q2AffCopy})
			}
			return nil
		})
		if ds[0].claim == ds[1].claim || ds[0].copySubject == ds[1].copySubject {
			t.Fatalf("the two deliveries share an assignment, so there is no second claim to forge against: %+v", ds)
		}
		for _, d := range ds {
			if st, _ := b.asm.loadQ2AffCopy(d.copySubject); st != mqttQ2AffCopyPresent {
				t.Fatalf("the copy of %d is not staged, so its later presence proves nothing", d.pi)
			}
		}
		return &staged{b: b, c: c, r: r, sess: sess, answer: ds[0], victim: ds[1]}
	}

	// forge rewrites the answering delivery's register in place — same nonce, same
	// identifier, same source — so the ONLY thing wrong with it is the pair it carries.
	forge := func(t *testing.T, st *staged, copySubject string) {
		t.Helper()
		rec, sm, err := st.b.asm.jsa.loadInflightRecord(st.answer.regSubject)
		if err != nil || rec == nil || sm == nil {
			t.Fatalf("loading the register to forge: %v", err)
		}
		rec.Q2AffClaim = st.victim.claim
		rec.Q2AffCopy = copySubject
		if _, err := st.b.asm.jsa.storeInflightCAS(st.answer.regSubject, sm.Sequence, rec); err != nil {
			t.Fatalf("forging the register: %v", err)
		}
	}

	// recover drops the in-memory entry and lets RECOVERY rebuild it from the record,
	// which is the route a restart takes and the only route a forged register can enter by.
	recover := func(t *testing.T, st *staged) {
		t.Helper()
		st.sess.mu.Lock()
		delete(st.sess.pendingPublish, st.answer.pi)
		st.sess.mu.Unlock()
		if _, err := st.sess.recoverInflightRegisters(testQ2AffSessionClient(st.b, st.sess)); err != nil {
			t.Fatalf("recovering the registers: %v", err)
		}
	}

	// answerPubRec sends the PUBREC and then PROVES it was processed, which polling for
	// the absence of a write cannot: a refused forgery leaves nothing to wait for, so a
	// checkFor on "the victim is untouched" would succeed before the packet was even read.
	// The read loop is sequential per connection, so a PINGRESP that follows the PUBREC
	// is proof the PUBREC is done. A PUBLISH is tolerated in between only because a
	// redelivery is legal protocol; with AckWait at 30s none is expected.
	answerPubRec := func(t *testing.T, st *staged) {
		t.Helper()
		testMQTTSendPIPacket(mqttPacketPubRec, t, st.c, st.answer.pi)
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketPing)
		w.WriteByte(0)
		if _, err := st.c.Write(w.Bytes()); err != nil {
			t.Fatalf("sending PINGREQ: %v", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			pb, pl, ready := testMQTTReadPacketReady(st.r, time.Second)
			if !ready {
				continue
			}
			start := st.r.pos
			switch pb & mqttPacketMask {
			case mqttPacketPingResp:
				return
			case mqttPacketPub, mqttPacketPubRel:
				st.r.pos = start + pl // legal in-flight traffic; keep looking
			default:
				t.Fatalf("unexpected packet 0x%x while proving the PUBREC was processed", pb&mqttPacketMask)
			}
		}
		t.Fatal("no PINGRESP after the PUBREC, so nothing proves it was processed")
	}

	// victimUntouched is the loss this whole binding exists to prevent, stated once:
	// nothing was written to the victim's claim, and its copy is still there to deliver.
	//
	// It deliberately does NOT assert that the answering delivery's own copy survived.
	// jsAckSubject is memory-only — a JetStream reply subject is valid for one delivery
	// attempt — so a RECOVERED entry has no ack subject at all and can never acknowledge
	// its copy, forged or genuine. An assertion on that copy's presence would therefore
	// hold for every implementation, which is no assertion. What discriminates is
	// AffineDone on the answering delivery's OWN claim: refused here, recorded by the
	// control subtest below on the same field.
	victimUntouched := func(t *testing.T, st *staged) {
		t.Helper()
		v, _, err := st.b.asm.loadQ2AffClaim(st.victim.claim)
		if err != nil {
			t.Fatalf("loading the victim claim: %v", err)
		}
		if v == nil {
			t.Fatal("the victim's claim was REAPED on the strength of a forged register")
		}
		if v.AffineDone || v.Lost || v.Discarded {
			t.Fatalf("a forged register credited the victim's claim with a delivery it never made, so the gate's terminal branch will now dispose of its copy and the message is lost: %+v", v)
		}
		if state, _ := st.b.asm.loadQ2AffCopy(st.victim.copySubject); state != mqttQ2AffCopyPresent {
			t.Fatal("the victim's copy is gone, so its message can never be delivered")
		}
		own, _, err := st.b.asm.loadQ2AffClaim(st.answer.claim)
		if err != nil {
			t.Fatalf("loading the answering delivery's own claim: %v", err)
		}
		if own == nil || own.AffineDone {
			t.Fatalf("a refused pair still recorded a completion, on the answering delivery's own claim: %+v", own)
		}
		rec, _, err := st.b.asm.jsa.loadInflightRecord(st.answer.regSubject)
		if err != nil {
			t.Fatalf("loading the register: %v", err)
		}
		if rec == nil {
			t.Fatal("the refused register was DELETED, so the state an operator has to resolve was destroyed on the strength of a record that was refused as uninterpretable")
		}
		if rec.Q2AffClaim != st.victim.claim {
			t.Fatalf("the refused register was rewritten to name %q", rec.Q2AffClaim)
		}
	}

	t.Run("the pair's two fields disagree", func(t *testing.T) {
		st := stage(t, "forgemix")
		// Claim B with A's OWN copy. Recovery's source check cannot see anything wrong —
		// the carried copy IS this register's source — so the refusal has to come from the
		// claim, which names its own copy and not this one.
		forge(t, st, st.answer.copySubject)
		recover(t, st)
		st.sess.mu.Lock()
		restored := st.sess.pendingPublish[st.answer.pi]
		st.sess.mu.Unlock()
		if restored == nil || restored.q2AffClaim != st.victim.claim {
			t.Fatalf("the forged pair was not restored, so the transition under test never ran: %+v", restored)
		}
		answerPubRec(t, st)
		victimUntouched(t, st)
		st.sess.mu.Lock()
		_, held := st.sess.pendingPubRel[st.answer.pi]
		st.sess.mu.Unlock()
		if !held {
			t.Fatal("the identifier was released although its exchange is past PUBREC")
		}
	})

	t.Run("the pair is internally consistent", func(t *testing.T) {
		st := stage(t, "forgecons")
		// Claim B with B's OWN copy: every field agrees with every other, and only this
		// register's SOURCE says the pair belongs to a different delivery.
		forge(t, st, st.victim.copySubject)
		recover(t, st)
		st.sess.mu.Lock()
		restored := st.sess.pendingPublish[st.answer.pi]
		st.sess.mu.Unlock()
		if restored == nil {
			t.Fatal("the identifier was not reserved, so a later allocation could reuse one the client may hold")
		}
		if restored.q2AffClaim != _EMPTY_ || restored.q2AffCopy != _EMPTY_ {
			t.Fatalf("recovery installed a pair its own source contradicts: %+v", restored)
		}
		if !restored.regForeign {
			t.Fatal("the reservation is settleable, so the client's own acknowledgement will delete the refused record")
		}
		answerPubRec(t, st)
		victimUntouched(t, st)
		st.sess.mu.Lock()
		reserved := st.sess.pendingPublish[st.answer.pi] != nil
		st.sess.mu.Unlock()
		if !reserved {
			t.Fatal("the identifier was released, so it can be reused while the client may still hold it")
		}
	})

	// The control. Without it every assertion above passes for a fixture whose PUBREC
	// never reached the affine path at all — which is how four earlier affine fixtures in
	// this file passed for the wrong reason.
	t.Run("the genuine register completes", func(t *testing.T) {
		st := stage(t, "forgectl")
		recover(t, st)
		answerPubRec(t, st)
		// AffineDone AND the register gone, which is the pair of facts the two subcases
		// above are each denied. The copy is not asserted on: a recovered entry has no
		// ack subject (see victimUntouched), so the copy is disposed of by the delivery
		// gate's terminal branch on the next redelivery rather than here.
		checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
			cl, _, err := st.b.asm.loadQ2AffClaim(st.answer.claim)
			if err != nil {
				return fmt.Errorf("loading the claim: %w", err)
			}
			if cl == nil {
				return fmt.Errorf("the claim was reaped although its copy ack was never confirmed")
			}
			if !cl.AffineDone {
				return fmt.Errorf("the affine side is not recorded yet: %+v", cl)
			}
			rec, _, lerr := st.b.asm.jsa.loadInflightRecord(st.answer.regSubject)
			if lerr != nil {
				return fmt.Errorf("loading the register: %w", lerr)
			}
			if rec != nil {
				return fmt.Errorf("the register is still there: %+v", rec)
			}
			return nil
		})
	})
}

// testQ2AffOneDelivery brings a session to ONE in-flight affine QoS 2 delivery and
// returns everything the forgery fixtures below need: the claim it elected, the copy it
// carries, the packet identifier, and the register's subject.
//
// One delivery rather than two, deliberately: with a single copy on the affine consumer,
// that consumer's redelivery counter is unambiguous evidence that THIS copy was re-offered,
// which is what the retirement fixtures need as a positive signal. The second claim those
// fixtures forge against is elected by hand instead.
type testQ2AffOne struct {
	b           *testQ2AffBroker
	c           net.Conn
	r           *mqttReader
	sess        *mqttSession
	idHash      string
	affGen      string
	groupDur    string
	pi          uint16
	claim       string
	copySubject string
	regSubject  string
	jsDur       string
	sseq        uint64
}

func testQ2AffOneDelivery(t *testing.T, name string, ackWait time.Duration) *testQ2AffOne {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = ackWait
	srv := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(srv) })
	b := &testQ2AffBroker{s: srv, o: o, topic: name + "/topic", group: name + "grp"}
	id := name + "A"
	c, r := b.member(t, id, 300)
	t.Cleanup(func() { c.Close() })
	b.bind(t, id)
	idHash := getHash(id)
	affGen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if affGen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	b.publish(t, 1, "one")
	topic, _, qos, pi, ok := testMQTTTryReadPublishV5(r, 10*time.Second)
	if !ok || qos != 2 || topic != b.topic {
		t.Fatalf("no shared QoS 2 delivery: topic=%q qos=%d ok=%v", topic, qos, ok)
	}
	b.asm.mu.RLock()
	sess := b.asm.sessions[id]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}
	sess.mu.Lock()
	sessGen := sess.gen
	sess.mu.Unlock()
	out := &testQ2AffOne{
		b: b, c: c, r: r, sess: sess, idHash: idHash, affGen: affGen,
		groupDur: b.groupDurable(t), pi: pi,
		regSubject: mqttInflightSubject(idHash, sessGen, pi),
	}
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		rec, _, lerr := b.asm.jsa.loadInflightRecord(out.regSubject)
		if lerr != nil {
			return fmt.Errorf("loading the register: %w", lerr)
		}
		if rec == nil || rec.Q2AffClaim == _EMPTY_ || rec.Q2AffCopy == _EMPTY_ {
			return fmt.Errorf("the register does not carry its affine pair yet: %+v", rec)
		}
		out.claim, out.copySubject, out.jsDur, out.sseq = rec.Q2AffClaim, rec.Q2AffCopy, rec.Dur, rec.Seq
		return nil
	})
	return out
}

// testQ2AffElectByHand elects a self-consistent claim on an unrelated source sequence of
// the SAME group and session, so a register forged to name it is shape-valid: same winner,
// same affine generation, same affine durable. Its copy is stored only when asked for,
// because a fixture that needs an unambiguous redelivery signal must not put a second copy
// on the affine consumer.
func testQ2AffElectByHand(t *testing.T, st *testQ2AffOne, srcSeq uint64, withCopy bool) *mqttQ2AffClaim {
	t.Helper()
	claim := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: st.idHash, Gen: st.affGen, GroupDurable: st.groupDur, SourceSeq: srcSeq,
		ClaimHash: mqttQ2AffClaimHash(st.groupDur, srcSeq), Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(st.idHash, st.affGen), Phase: mqttQ2AffMaterialized,
	})
	subject := mqttQ2AffClaimSubject(st.groupDur, srcSeq)
	if _, err := st.b.asm.storeQ2AffClaimCAS(subject, 0, claim); err != nil {
		t.Fatalf("electing the second claim: %v", err)
	}
	if withCopy {
		env := &mqttQ2AffineEnvelope{
			natsSubject: st.b.topic, msg: []byte("second-body"),
			claimSubject: subject, gen: st.affGen,
			msgID: mqttQ2AffMsgId(st.groupDur, srcSeq, claim.Nonce),
		}
		msg, hdr := mqttNewQ2AffineRecord(env)
		if _, err := st.b.asm.storeQ2AffCopy(claim.CopySubject, hdr, msg); err != nil {
			t.Fatalf("staging the second claim's copy: %v", err)
		}
	}
	return claim
}

// testQ2AffForgeRegister rewrites a register's affine pair in place — same nonce, same
// identifier, same source — so the only thing wrong with it is the pair it carries.
func testQ2AffForgeRegister(t *testing.T, st *testQ2AffOne, claimSubject, copySubject string) {
	t.Helper()
	rec, sm, err := st.b.asm.jsa.loadInflightRecord(st.regSubject)
	if err != nil || rec == nil || sm == nil {
		t.Fatalf("loading the register to forge: %v", err)
	}
	rec.Q2AffClaim, rec.Q2AffCopy = claimSubject, copySubject
	if _, err := st.b.asm.jsa.storeInflightCAS(st.regSubject, sm.Sequence, rec); err != nil {
		t.Fatalf("forging the register: %v", err)
	}
}

// testQ2AffRecoverRegisters drops the in-memory entry and lets RECOVERY rebuild it from
// the record, which is the route a restart takes and the only one a forged register enters
// by.
func testQ2AffRecoverRegisters(t *testing.T, st *testQ2AffOne) {
	t.Helper()
	st.sess.mu.Lock()
	delete(st.sess.pendingPublish, st.pi)
	st.sess.mu.Unlock()
	if _, err := st.sess.recoverInflightRegisters(testQ2AffSessionClient(st.b, st.sess)); err != nil {
		t.Fatalf("recovering the registers: %v", err)
	}
}

// TestQ2AffForgedRegisterAcknowledgesNoForeignCopy is holistic round 15's H15, which found
// the first fix for the carried pair had moved the loss rather than closed it.
//
// A carrier naming a FOREIGN claim survives the immediate transition, because that refuses
// — but the PUBREC has already installed a retirement carrying the same forged pair, and
// the retirement outlives the transition. On the copy's next redelivery the retired
// settlement took its superseded-incarnation arm (the claim it loads names a different
// copy), confirm-acked the copy IN HAND and cleared the retirement. That copy belongs to
// the answering delivery's OWN claim, which never recorded AffineDone: its only copy is
// gone and its message can never be delivered. The immediate transition passing is exactly
// what made the earlier fixture pass — it stopped before AckWait.
//
// So a mismatch is superseded only WITHIN one assignment identity: same owner, same
// generation, same claim hash, differing in the incarnation token alone. A copy of a
// different assignment is refused instead, and refusing costs one NAK'd redelivery.
//
// Paired mutation M42: treat every copy mismatch as superseded (drop the assignment-identity
// derivation in q2AffClaimForCarriedCopy). The redelivery then acknowledges the answering
// claim's live copy away.
func TestQ2AffForgedRegisterAcknowledgesNoForeignCopy(t *testing.T) {
	// Short enough that the copy is genuinely re-offered inside the test, long enough that
	// the forge-recover-PUBREC sequence completes before the first redelivery.
	st := testQ2AffOneDelivery(t, "forgeack", 2*time.Second)
	// The victim claim carries NO copy: with one copy on the affine consumer, that
	// consumer's redelivery counter is unambiguous evidence that the answering delivery's
	// own copy was re-offered, which is the positive signal this fixture rests on.
	victim := testQ2AffElectByHand(t, st, 611001, false)
	victimSubject := mqttQ2AffClaimSubject(st.groupDur, 611001)

	// The forged pair names the VICTIM's claim while carrying the answering delivery's own
	// copy — so the register's source agrees with it and recovery installs it.
	testQ2AffForgeRegister(t, st, victimSubject, st.copySubject)
	testQ2AffRecoverRegisters(t, st)
	st.sess.mu.Lock()
	restored := st.sess.pendingPublish[st.pi]
	st.sess.mu.Unlock()
	if restored == nil || restored.q2AffClaim != victimSubject || restored.q2AffCopy != st.copySubject {
		t.Fatalf("the forged pair was not restored, so nothing under test ever runs: %+v", restored)
	}

	// A real PUBREC: the affine transition refuses (the victim's claim does not name this
	// copy), so a retirement carrying the forged pair is left behind. That retirement is
	// what the redelivery below settles.
	testMQTTSendPIPacket(mqttPacketPubRec, t, st.c, st.pi)
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		st.sess.mu.Lock()
		defer st.sess.mu.Unlock()
		if !st.sess.sourceRetiredLocked(st.jsDur, st.sseq) {
			return fmt.Errorf("the PUBREC has not installed its retirement yet")
		}
		return nil
	})
	before := testQ2AffRedeliveries(t, st)

	// The redelivery is REAL, and so is its JetStream reply subject: a synthetic one would
	// make the acknowledgement fail for want of a real subject and the defect would hide
	// behind that failure.
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		if testQ2AffRedeliveries(t, st) <= before {
			return fmt.Errorf("the copy has not been re-offered yet, so the retired settlement never ran")
		}
		return nil
	})

	if state, _ := st.b.asm.loadQ2AffCopy(st.copySubject); state != mqttQ2AffCopyPresent {
		t.Fatal("the answering delivery's OWN copy was acknowledged away by a retirement carrying a foreign claim: that claim never recorded AffineDone, so its message can never be delivered (F-065, H15)")
	}
	after, _, err := st.b.asm.loadQ2AffClaim(victimSubject)
	if err != nil || after == nil {
		t.Fatalf("the victim's claim is gone: %v", err)
	}
	if after.AffineDone || after.Lost || after.Discarded {
		t.Fatalf("the retirement wrote to a claim it never delivered for: %+v", after)
	}
	if after.Nonce != victim.Nonce {
		t.Fatalf("the victim's claim was replaced: %+v", after)
	}
	st.sess.mu.Lock()
	stillRetired := st.sess.sourceRetiredLocked(st.jsDur, st.sseq)
	st.sess.mu.Unlock()
	if !stillRetired {
		t.Fatal("the retirement was cleared although nothing about it was settled: the payload's suppression is gone while the copy is still re-offerable")
	}
}

// testQ2AffRedeliveries is the affine consumer's CUMULATIVE delivery count, which is how
// these fixtures prove a copy was RE-OFFERED rather than assuming it.
//
// Deliberately Delivered.Consumer and not NumRedelivered: the latter is how many messages
// are CURRENTLY in a redelivered state, so it sits at 1 for a single copy no matter how
// many times that copy is re-offered — measured, and it made the first version of this
// fixture time out against a consumer that was in fact re-offering thousands of times.
func testQ2AffRedeliveries(t *testing.T, st *testQ2AffOne) uint64 {
	t.Helper()
	ci, err := st.b.asm.jsa.lookupConsumer(mqttOutStreamName, affDurableFor(st.idHash, st.affGen))
	if err != nil || ci == nil {
		t.Fatalf("looking up the affine consumer: %v", err)
	}
	return ci.Delivered.Consumer
}

// TestQ2AffRegisterWithAnUnprovenSourceIsNotInstalled is holistic round 15's H16: the
// source binding has to be POSITIVE.
//
// The first form refused only a live source naming a DIFFERENT copy, on the reasoning that
// an absent or unreadable source is not evidence of a forgery. True, and beside the point:
// nothing licenses installing an authoritative pair on the strength of a source that was
// never read. An indeterminate consumer lookup, an indeterminate message read, or a
// consumer that is authoritatively GONE all admitted the forgery whole — and the forged
// pair here is internally consistent, so no other check can catch it.
//
// The consumer is deleted to reach that state, which makes the source knowably Lost. An
// unreadable one takes the identical branch: the predicate proves nothing without a live
// source whose subject IS the carried copy.
//
// Paired mutation M43: restore the contradiction-only form (refuse only when a LIVE source
// names a different subject). The forged pair is then installed and its PUBREC records
// AffineDone on a claim that delivered nothing.
func TestQ2AffRegisterWithAnUnprovenSourceIsNotInstalled(t *testing.T) {
	st := testQ2AffOneDelivery(t, "unprovensrc", 30*time.Second)
	victim := testQ2AffElectByHand(t, st, 622001, true)
	victimSubject := mqttQ2AffClaimSubject(st.groupDur, 622001)

	// Internally consistent: the register names the victim's claim AND the victim's own
	// copy, so only its source could ever say the pair belongs to a different delivery.
	testQ2AffForgeRegister(t, st, victimSubject, victim.CopySubject)

	// Destroying the affine consumer is what makes the source unprovable — and it is a
	// real state, not a contrivance: an out-of-band consumer deletion is the premise of
	// this feature's Lost outcome.
	if _, err := st.b.asm.jsa.deleteConsumer(mqttOutStreamName, affDurableFor(st.idHash, st.affGen), true); err != nil {
		t.Fatalf("deleting the affine consumer: %v", err)
	}
	testQ2AffRecoverRegisters(t, st)

	st.sess.mu.Lock()
	restored := st.sess.pendingPublish[st.pi]
	mapped, hasMapping := st.sess.cpending[st.jsDur][st.sseq]
	st.sess.mu.Unlock()
	if restored == nil {
		t.Fatal("the identifier was not reserved, so a later allocation could reuse one the client may hold")
	}
	if restored.q2AffClaim != _EMPTY_ || restored.q2AffCopy != _EMPTY_ {
		t.Fatalf("recovery installed an affine pair its own source never proved: %+v", restored)
	}
	if !restored.regForeign {
		t.Fatal("the reservation is settleable, so the client's own acknowledgement will delete the record an operator has to resolve")
	}
	if hasMapping {
		t.Fatalf("the unproven register's source was entered in the delivery map under identifier %d", mapped)
	}
	after, _, err := st.b.asm.loadQ2AffClaim(victimSubject)
	if err != nil || after == nil {
		t.Fatalf("the victim's claim is gone: %v", err)
	}
	if after.AffineDone || after.Lost || after.Discarded {
		t.Fatalf("a pair no source proved was credited to the claim it named: %+v", after)
	}
}

// TestQ2AffUnprovenRegisterPoisonsItsSibling is holistic round 15's H17.
//
// An unproven pair does not prove the identifier was never transmitted — the client may
// hold THAT identifier rather than a sibling's. The first fix set such a register aside on
// its own, before the duplicate-source grouping, which turned a two-member group into a
// one-member one: the sibling was then installed as an authoritative singleton WITH a
// cpending mapping, and the source's redelivery follows that mapping and re-sends the
// payload under a second identifier. Reserving both identifiers does not prevent that,
// because the mapping is what the redelivery consults.
//
// So an unproven register joins the grouping as an UNOWNED member and poisons every
// sibling of its source, exactly as a register whose claim could not be established does.
//
// Paired mutation M44: set the unproven register aside before the grouping (append to
// opaque and continue). The sibling is then installed with a source mapping.
func TestQ2AffUnprovenRegisterPoisonsItsSibling(t *testing.T) {
	st := testQ2AffOneDelivery(t, "poisonsib", 30*time.Second)
	victim := testQ2AffElectByHand(t, st, 633001, true)
	victimSubject := mqttQ2AffClaimSubject(st.groupDur, 633001)

	// A GENUINE sibling: a second register naming the SAME delivery on a different
	// identifier, which is what a superseded broker leaves behind. Its own pair is correct,
	// so nothing but the group makes it refusable.
	const siblingPI = uint16(4242)
	st.sess.mu.Lock()
	sessGen := st.sess.gen
	st.sess.mu.Unlock()
	siblingSubject := mqttInflightSubject(st.idHash, sessGen, siblingPI)
	if _, err := st.b.asm.jsa.storeInflightCAS(siblingSubject, 0, freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish,
		PI: siblingPI, Dur: st.jsDur, Seq: st.sseq, QoS: 2, Nonce: mqttNewInflightNonce(),
		Q2AffClaim: st.claim, Q2AffCopy: st.copySubject,
	})); err != nil {
		t.Fatalf("writing the sibling register: %v", err)
	}
	// The answering register's pair is now contradicted by its own source: it carries the
	// victim's copy while its delivery is its own.
	testQ2AffForgeRegister(t, st, victimSubject, victim.CopySubject)
	testQ2AffRecoverRegisters(t, st)

	st.sess.mu.Lock()
	answering := st.sess.pendingPublish[st.pi]
	sibling := st.sess.pendingPublish[siblingPI]
	mapped, hasMapping := st.sess.cpending[st.jsDur][st.sseq]
	st.sess.mu.Unlock()
	if answering == nil || sibling == nil {
		t.Fatalf("both identifiers must stay reserved: answering=%+v sibling=%+v", answering, sibling)
	}
	if hasMapping {
		t.Fatalf("the source was entered in the delivery map under identifier %d although one register naming it could not be interpreted: its redelivery now re-sends the payload under that identifier", mapped)
	}
	// The UNPROVEN member is the one nothing may settle. Its genuine sibling is this
	// session's own record and stays settleable, which is the existing duplicate-source
	// policy (only unowned members are marked foreign) and is deliberately not changed
	// here: what this finding is about is the source MAPPING, which is what a redelivery
	// consults.
	if !answering.regForeign {
		t.Fatal("the unproven register is settleable, so the client's own acknowledgement will delete the record an operator has to resolve")
	}
	if sibling.q2AffClaim != _EMPTY_ || sibling.q2AffCopy != _EMPTY_ {
		t.Fatalf("the sibling was installed with its pair although its group was poisoned: %+v", sibling)
	}
	for subject, what := range map[string]string{st.regSubject: "answering", siblingSubject: "sibling"} {
		rec, _, err := st.b.asm.jsa.loadInflightRecord(subject)
		if err != nil {
			t.Fatalf("loading the %s register: %v", what, err)
		}
		if rec == nil {
			t.Fatalf("the %s register was deleted although the group was never arbitrated", what)
		}
	}
}

// TestQ2AffRefusalBackoffIsDerivedFromTheDeliveryCount pins the wire form and the curve,
// because both are load-bearing and neither is visible from the end-to-end fixture below:
// a payload nats-server cannot parse is treated as a PLAIN NAK (consumer.go:3186-3200), so
// a typo here silently restores the storm this exists to bound.
//
// Paired mutation M45: return AckNak unchanged. The end-to-end pacing fixture then fails.
func TestQ2AffRefusalBackoffIsDerivedFromTheDeliveryCount(t *testing.T) {
	t.Parallel()
	parse := func(t *testing.T, raw []byte) time.Duration {
		t.Helper()
		prefix := string(AckNak) + " "
		if !strings.HasPrefix(string(raw), prefix) {
			t.Fatalf("a refusal must be a NAK with a delay payload, got %q", raw)
		}
		var opts struct {
			Delay int64 `json:"delay"`
		}
		if err := json.Unmarshal(raw[len(prefix):], &opts); err != nil {
			t.Fatalf("nats-server parses this payload as JSON and falls back to a PLAIN NAK when it cannot: %v (%q)", err, raw)
		}
		return time.Duration(opts.Delay)
	}
	// An unparseable reply subject yields delivery count zero, which must not become a
	// zero delay: that is the immediate NAK by another route.
	if d := parse(t, mqttPacedRefusalNak(0)); d != mqttPacedRefusalBaseDelay {
		t.Fatalf("delivery count 0 paced at %v, want the base delay %v", d, mqttPacedRefusalBaseDelay)
	}
	if d := parse(t, mqttPacedRefusalNak(1)); d != mqttPacedRefusalBaseDelay {
		t.Fatalf("the first delivery paced at %v, want the base delay %v: the common phase race must retry fast", d, mqttPacedRefusalBaseDelay)
	}
	if d := parse(t, mqttPacedRefusalNak(2)); d != 2*mqttPacedRefusalBaseDelay {
		t.Fatalf("the second delivery paced at %v, want %v", d, 2*mqttPacedRefusalBaseDelay)
	}
	prev := parse(t, mqttPacedRefusalNak(2))
	for dc := uint64(3); dc < 40; dc++ {
		d := parse(t, mqttPacedRefusalNak(dc))
		if d < prev {
			t.Fatalf("the backoff went BACKWARDS at delivery %d: %v after %v", dc, d, prev)
		}
		if d > mqttPacedRefusalMaxDelay {
			t.Fatalf("delivery %d paced at %v, past the cap %v", dc, d, mqttPacedRefusalMaxDelay)
		}
		prev = d
	}
	if prev != mqttPacedRefusalMaxDelay {
		t.Fatalf("the backoff settled at %v rather than the cap %v, so a sustained refusal never reaches its bound", prev, mqttPacedRefusalMaxDelay)
	}
}

// TestQ2AffSustainedRefusalIsPaced is holistic round 16's finding, and its own measurement
// is what makes the bound meaningful: before the backoff, ONE copy under an absent claim
// was re-offered 9214 times in ten seconds — a tight load-and-refuse loop against
// JetStream with a log line per turn.
//
// The state is not forgery-only, which is why this is a defect rather than a curiosity.
// docs/CONFORMANCE.md states three windows in which a copy legitimately outlives the claim
// that would authorize it, and an unreadable record or an unavailable backend produces the
// same shape for as long as it lasts. So the fixture stages the legitimate version: a live
// copy whose claim record is simply gone.
//
// Paired mutations M45 (return AckNak unchanged from mqttPacedRefusalNak) and M46 (send
// AckNak directly from the claim == nil arm). Either restores the storm and this fixture
// fails on the count.
func TestQ2AffSustainedRefusalIsPaced(t *testing.T) {
	// A short AckWait so the refusal loop actually starts inside the test; the pacing
	// under test is the NAK's delay, which is what governs every turn after the first.
	st := testQ2AffOneDelivery(t, "refusepace", time.Second)

	// The claim record is deleted, so the gate's claim == nil arm refuses every
	// redelivery. Nothing else can settle it: the copy stays, and the loop is sustained.
	_, seq, err := st.b.asm.loadQ2AffClaim(st.claim)
	if err != nil || seq == 0 {
		t.Fatalf("loading the claim to delete: %v (seq %d)", err, seq)
	}
	if derr := st.b.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); derr != nil {
		t.Fatalf("deleting the claim: %v", derr)
	}
	if state, _ := st.b.asm.loadQ2AffCopy(st.copySubject); state != mqttQ2AffCopyPresent {
		t.Fatal("the copy is gone, so no refusal loop can be staged")
	}
	before := testQ2AffRedeliveries(t, st)

	// The refusal must actually RUN — a bound that holds because nothing happened is no
	// bound at all — so the loop is first observed to start, and only then measured.
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if testQ2AffRedeliveries(t, st) <= before {
			return fmt.Errorf("the copy has not been re-offered yet, so no refusal has run")
		}
		return nil
	})
	start := testQ2AffRedeliveries(t, st)
	time.Sleep(5 * time.Second)
	grown := testQ2AffRedeliveries(t, st) - start
	// The backoff doubles from 100ms, so five seconds of sustained refusal admits a
	// handful of turns. The measured unbounded form managed several thousand per second,
	// so the threshold does not need to be tight to discriminate.
	const bound = 40
	if grown > bound {
		t.Fatalf("a sustained refusal was re-offered %d times in five seconds (bound %d): an immediate NAK turns every legitimate claim-absence window into a request storm against JetStream", grown, bound)
	}
}

// TestQ2AffGateRefusesACrossAssignmentCopy is holistic round 17's finding, and it is the
// same defect the retired settlement had one round earlier, in the one place I did not
// sweep: the delivery gate disposed of a copy on BARE subject inequality.
//
// The gate takes its claim subject from the copy's own HEADER. So a copy persisted on
// assignment A's copy subject while its header names a valid, attributable assignment B
// passes every other check and reaches the mismatch arm — where an acknowledgement destroys
// A's message and leaves A's claim without AffineDone, irrecoverably. Bare inequality is
// not the statement the disposal needs: only a copy of THE SAME assignment under another
// incarnation is unauthorizable by construction, and that is now one predicate
// (q2AffCopyIsSupersededIncarnationOf) shared by both paths.
//
// A cross-assignment copy is a persisted shape this broker does not write, so it is
// refused, not interpreted.
//
// Paired mutation M47: restore the broad disposal (ack whenever subject != CopySubject).
// The copy is then acknowledged away and this fixture fails on its presence.
func TestQ2AffGateRefusesACrossAssignmentCopy(t *testing.T) {
	st := testQ2AffOneDelivery(t, "crossassign", 2*time.Second)
	// Two hand-elected assignments of this same session and generation, so both are
	// attributable and owned: the copy will be stored on A's subject with B's header.
	a := testQ2AffElectByHand(t, st, 644001, false)
	b := testQ2AffElectByHand(t, st, 644002, false)
	bSubject := mqttQ2AffClaimSubject(st.groupDur, 644002)
	if a.CopySubject == b.CopySubject {
		t.Fatal("the two assignments derived one copy subject, so this fixture stages nothing")
	}

	before := testQ2AffRedeliveries(t, st)
	// The copy lives on A's subject and its header names B. Everything about it is
	// well formed; only the pairing is a shape this broker never writes.
	env := &mqttQ2AffineEnvelope{
		natsSubject: st.b.topic, msg: []byte("cross-assignment-body"),
		claimSubject: bSubject, gen: st.affGen,
		msgID: mqttQ2AffMsgId(st.groupDur, 644002, b.Nonce),
	}
	msg, hdr := mqttNewQ2AffineRecord(env)
	if _, err := st.b.asm.storeQ2AffCopy(a.CopySubject, hdr, msg); err != nil {
		t.Fatalf("staging the cross-assignment copy: %v", err)
	}

	// It must actually REACH the gate: the affine consumer's cumulative delivery count is
	// what says so, and without that the presence assertion below would hold for a copy
	// nothing ever looked at.
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if testQ2AffRedeliveries(t, st) <= before {
			return fmt.Errorf("the cross-assignment copy has not been delivered yet, so the gate never ran")
		}
		return nil
	})

	if state, _ := st.b.asm.loadQ2AffCopy(a.CopySubject); state != mqttQ2AffCopyPresent {
		t.Fatal("the gate acknowledged away a copy of a DIFFERENT assignment than the claim its header named: that assignment's claim never records AffineDone, so its message is lost and its claim is stranded (F-065, round 17)")
	}
	for subject, claim := range map[string]*mqttQ2AffClaim{
		mqttQ2AffClaimSubject(st.groupDur, 644001): a,
		bSubject: b,
	} {
		got, _, err := st.b.asm.loadQ2AffClaim(subject)
		if err != nil || got == nil {
			t.Fatalf("loading the claim on %q: %v", subject, err)
		}
		if got.AffineDone || got.Lost || got.Discarded {
			t.Fatalf("a cross-assignment copy wrote to the claim on %q: %+v", subject, got)
		}
		if got.Nonce != claim.Nonce {
			t.Fatalf("the claim on %q was replaced: %+v", subject, got)
		}
	}
}

// TestQ2AffCompletionAcknowledgesOnlyWhatItRecorded is holistic round 18's first finding,
// and it is a window between two loads rather than a missing check.
//
// q2AffClaimForCarriedCopy validates ONE load; advanceQ2AffClaim then performs its own. In
// between the record can be deleted — advanceQ2AffClaim reports that as (nil, 0, nil),
// because a claim that is simply gone is not an error for its other callers — or rewritten
// to something that keeps the incarnation and loses its attributability. Either way a nil
// error was being read as proof, and the copy was then confirm-acked: the irreversible
// half, with the completion durable NOWHERE. That is precisely the ambiguity the
// record-before-ack order exists to remove.
//
// The two subtests are the two shapes, and they are deliberately driven through the claim
// LOAD seam rather than by racing a real writer: what has to be pinned is which reading
// each decision rests on, and a race would prove that only by luck.
//
// Paired mutations M48 (drop the updated-record validation in mqttFinishQ2AffineDelivery)
// and M49 (drop the copy fence inside advanceQ2AffClaim's CAS loop).
func TestQ2AffCompletionAcknowledgesOnlyWhatItRecorded(t *testing.T) {
	b := testQ2AffSetup(t, "finishwin")
	c, _ := b.member(t, "finishwinA", 300)
	defer c.Close()
	b.bind(t, "finishwinA")
	idHash := getHash("finishwinA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)
	b.asm.mu.RLock()
	sess := b.asm.sessions["finishwinA"]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}
	cc := testQ2AffSessionClient(b, sess)

	// stage elects a real, self-consistent claim and returns it with the sequence its
	// record occupies, so a hooked load can hand back a record the CAS would accept.
	stage := func(t *testing.T, srcSeq uint64) (*mqttQ2AffClaim, string, uint64) {
		t.Helper()
		claim := testQ2AffSelfConsistent(&mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
			ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq), Nonce: mqttNewQ2AffGen(),
			AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffMaterialized,
		})
		subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
		seq, err := b.asm.storeQ2AffClaimCAS(subject, 0, claim)
		if err != nil {
			t.Fatalf("electing the claim: %v", err)
		}
		return claim, subject, seq
	}

	t.Run("the record disappears between the validation and the transition", func(t *testing.T) {
		claim, subject, seq := stage(t, 655001)
		var acked atomic.Bool
		var loads atomic.Int64
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyAck = func(string) error { acked.Store(true); return nil }
		b.asm.testQ2AffClaimLoad = func(subj string) (*mqttQ2AffClaim, uint64, error) {
			if subj != subject {
				return nil, 0, fmt.Errorf("unexpected subject %q", subj)
			}
			if loads.Add(1) == 1 {
				cp := *claim
				return &cp, seq, nil // what the validation sees
			}
			return nil, 0, nil // GONE by the time the transition reads it
		}
		b.asm.mu.Unlock()
		defer func() {
			b.asm.mu.Lock()
			b.asm.testQ2AffCopyAck, b.asm.testQ2AffClaimLoad = nil, nil
			b.asm.mu.Unlock()
		}()

		recorded, ackedOK := cc.mqttFinishQ2AffineDelivery(subject, claim.CopySubject, "ack.subject")

		if loads.Load() < 2 {
			t.Fatalf("the transition never performed its own load (%d), so this run does not stage the window", loads.Load())
		}
		if acked.Load() {
			t.Fatal("the copy was acknowledged although the record carrying its completion had DISAPPEARED: an absent copy under no claim at all is indistinguishable from a lost delivery, and nothing can ever record the completion now")
		}
		if recorded || ackedOK {
			t.Fatalf("the completion was reported as recorded=%v acked=%v with nothing durable behind it: the PUBREC path would then delete the register that is the exchange's last carrier", recorded, ackedOK)
		}
	})

	t.Run("the record keeps its incarnation and loses its attributability", func(t *testing.T) {
		claim, subject, seq := stage(t, 655002)
		var acked atomic.Bool
		var loads atomic.Int64
		// Same incarnation, so the nonce fence passes; a ClaimHash that no longer follows
		// from GroupDurable/SourceSeq, so the record is not attributable to its own
		// subject. The sequence is the REAL one, so a mutation that removes the in-loop
		// fence lands its CAS and the defect is observable rather than masked by a
		// rejected write.
		corrupt := *claim
		corrupt.ClaimHash = mqttQ2AffClaimHash(groupDur, 999999)
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyAck = func(string) error { acked.Store(true); return nil }
		b.asm.testQ2AffClaimLoad = func(subj string) (*mqttQ2AffClaim, uint64, error) {
			if subj != subject {
				return nil, 0, fmt.Errorf("unexpected subject %q", subj)
			}
			if loads.Add(1) == 1 {
				cp := *claim
				return &cp, seq, nil
			}
			cp := corrupt
			return &cp, seq, nil
		}
		b.asm.mu.Unlock()
		defer func() {
			b.asm.mu.Lock()
			b.asm.testQ2AffCopyAck, b.asm.testQ2AffClaimLoad = nil, nil
			b.asm.mu.Unlock()
		}()

		recorded, _ := cc.mqttFinishQ2AffineDelivery(subject, claim.CopySubject, "ack.subject")

		if loads.Load() < 2 {
			t.Fatalf("the transition never performed its own load (%d), so this run does not stage the window", loads.Load())
		}
		if acked.Load() {
			t.Fatal("the copy was acknowledged against a record that cannot be attributed to its own subject")
		}
		if recorded {
			t.Fatal("an unattributable record was reported as carrying the completion")
		}
		// And the write itself must not have happened: the fence is inside the CAS loop
		// precisely so an unproven record is never mutated. Read through the seam removal.
		b.asm.mu.Lock()
		b.asm.testQ2AffClaimLoad = nil
		b.asm.mu.Unlock()
		after, _, err := b.asm.loadQ2AffClaim(subject)
		if err != nil || after == nil {
			t.Fatalf("loading the record after the refusal: %v", err)
		}
		if after.AffineDone {
			t.Fatalf("the transition MUTATED a record it could not attribute: %+v", after)
		}
	})
}

// TestQ2AffRetiredSettlementBindsTheCopyInHand is holistic round 18's second finding: the
// superseded arm proved something about the CARRIER and then acknowledged whatever was
// DELIVERED.
//
// The two are not the same message. A third, cross-assignment delivered copy passed the
// successor guard, so a live assignment's message was acknowledged away with its claim left
// stranded — the same class as the gate's cross-assignment disposal one round earlier, in
// the other path. The delivered-versus-carried equality now runs BEFORE every arm that can
// acknowledge anything.
//
// Paired mutation M50: move that equality back below the switch, where the superseded arm
// bypasses it.
func TestQ2AffRetiredSettlementBindsTheCopyInHand(t *testing.T) {
	b := testQ2AffSetup(t, "settlebind")
	c, _ := b.member(t, "settlebindA", 300)
	defer c.Close()
	b.bind(t, "settlebindA")
	idHash := getHash("settlebindA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)
	b.asm.mu.RLock()
	sess := b.asm.sessions["settlebindA"]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}
	elect := func(t *testing.T, srcSeq uint64, nonce string) (*mqttQ2AffClaim, string) {
		t.Helper()
		claim := testQ2AffSelfConsistent(&mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
			ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq), Nonce: nonce,
			AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffMaterialized,
		})
		subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, claim); err != nil {
			t.Fatalf("electing the claim on %q: %v", subject, err)
		}
		return claim, subject
	}
	retired := func(t *testing.T, jsDur string, copySeq uint64) bool {
		t.Helper()
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.sourceRetiredLocked(jsDur, copySeq)
	}

	t.Run("a superseded carrier acknowledges no third assignment's copy", func(t *testing.T) {
		// The live claim is a REPLACEMENT incarnation, so the carrier below is genuinely
		// superseded and the arm under test is genuinely entered.
		const srcSeq = uint64(666001)
		live, subject := elect(t, srcSeq, mqttNewQ2AffGen())
		supersededCarrier := mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, srcSeq), mqttNewQ2AffGen())
		if supersededCarrier == live.CopySubject {
			t.Fatal("the carrier and the live claim derived one copy subject, so nothing is superseded here")
		}
		// A THIRD assignment, perfectly live, whose copy is the one in hand.
		third, thirdSubject := elect(t, 666002, mqttNewQ2AffGen())

		var acked atomic.Value
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyAck = func(reply string) error { acked.Store(reply); return nil }
		b.asm.mu.Unlock()
		defer func() { b.asm.mu.Lock(); b.asm.testQ2AffCopyAck = nil; b.asm.mu.Unlock() }()

		const jsDur, copySeq = "$MQTT_Q2AFF_settlebindthird", uint64(71)
		sess.mu.Lock()
		sess.retireSourceLocked(jsDur, copySeq, &mqttRetiredSource{
			pi: 21, nonce: "regnonce21", q2AffClaim: subject, q2AffCopy: supersededCarrier, durable: true,
		})
		sess.mu.Unlock()

		sess.settleRetiredAffineSource(testQ2AffSessionClient(b, sess), jsDur, copySeq,
			"ack.subject", third.CopySubject, subject, supersededCarrier)

		if acked.Load() != nil {
			t.Fatal("a superseded CARRIER licensed acknowledging a third assignment's DELIVERED copy: that assignment's claim never records AffineDone, so its message is lost and its claim is stranded")
		}
		if !retired(t, jsDur, copySeq) {
			t.Fatal("the retirement was cleared although nothing about it was settled")
		}
		for subj := range map[string]struct{}{subject: {}, thirdSubject: {}} {
			got, _, err := b.asm.loadQ2AffClaim(subj)
			if err != nil || got == nil {
				t.Fatalf("loading the claim on %q: %v", subj, err)
			}
			if got.AffineDone || got.Lost || got.Discarded {
				t.Fatalf("the settlement wrote to the claim on %q: %+v", subj, got)
			}
		}
	})

	t.Run("a live carrier acknowledges no other copy in hand", func(t *testing.T) {
		// Here the carrier IS the live claim's copy, so the helper succeeds and the
		// completion path would run — but the copy in hand is a different assignment's,
		// which is the refusal the equality above the switch exists for.
		const srcSeq = uint64(666003)
		live, subject := elect(t, srcSeq, mqttNewQ2AffGen())
		other, otherSubject := elect(t, 666004, mqttNewQ2AffGen())

		var acked atomic.Value
		b.asm.mu.Lock()
		b.asm.testQ2AffCopyAck = func(reply string) error { acked.Store(reply); return nil }
		b.asm.mu.Unlock()
		defer func() { b.asm.mu.Lock(); b.asm.testQ2AffCopyAck = nil; b.asm.mu.Unlock() }()

		const jsDur, copySeq = "$MQTT_Q2AFF_settlebindlive", uint64(72)
		sess.mu.Lock()
		sess.retireSourceLocked(jsDur, copySeq, &mqttRetiredSource{
			pi: 22, nonce: "regnonce22", q2AffClaim: subject, q2AffCopy: live.CopySubject, durable: true,
		})
		sess.mu.Unlock()

		sess.settleRetiredAffineSource(testQ2AffSessionClient(b, sess), jsDur, copySeq,
			"ack.subject", other.CopySubject, subject, live.CopySubject)

		if acked.Load() != nil {
			t.Fatal("the settlement acknowledged a copy its retirement does not name")
		}
		if !retired(t, jsDur, copySeq) {
			t.Fatal("the retirement was cleared on the strength of a copy it does not name")
		}
		for subj := range map[string]struct{}{subject: {}, otherSubject: {}} {
			got, _, err := b.asm.loadQ2AffClaim(subj)
			if err != nil || got == nil {
				t.Fatalf("loading the claim on %q: %v", subj, err)
			}
			if got.AffineDone {
				t.Fatalf("the settlement recorded a completion for a copy that was never in hand: %q %+v", subj, got)
			}
		}
	})
}

// TestQ2AffOversizeClaimIsDroppedBeforeElection drives the RUNTIME backstop, which is the
// half of the claim-size bound that matters for a QoS 1/2 Will: a Will store bypasses the
// PUBLISH admission preflight entirely, so an assignment claim that cannot be stored would
// otherwise be discovered only by an election that can never succeed — and the group
// delivery would then be re-offered forever.
//
// The payload window is chosen so the COPY fits and only the CLAIM does not, which is what
// makes the assertion attributable to the check under test rather than to the pre-existing
// affine-form bound.
//
// Paired mutation M18: delete the runtime claim-size check. The election then proceeds and
// writes a claim for a message whose claim record cannot be stored.
func TestQ2AffOversizeClaimIsDroppedBeforeElection(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "oversizeclaim")
	c, _ := b.member(t, "oversizeclaimA", 300)
	defer c.Close()
	b.bind(t, "oversizeclaimA")
	idHash := getHash("oversizeclaimA")
	if gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen; gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	b.asm.mu.RLock()
	sess := b.asm.sessions["oversizeclaimA"]
	b.asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the session is not resident")
	}
	groupDur := b.groupDurable(t)

	claimMax := mqttQ2AffClaimRecordMax()
	env := &mqttQ2AffineEnvelope{
		natsSubject:  b.topic,
		msg:          []byte("tiny"),
		claimSubject: mqttQ2AffClaimSubject(groupDur, 1),
		gen:          mqttNewQ2AffGen(),
		msgID:        mqttQ2AffMsgId(groupDur, 1, mqttNewQ2AffGen()),
	}
	copySize := mqttComputeQ2AffineSize(env)
	if claimMax <= copySize {
		t.Skipf("the claim form (%d) does not exceed the copy form (%d) here, so this window does not exist", claimMax, copySize)
	}
	// Above the copy, below the claim: only the claim check can drop this.
	maxPayload := int32(claimMax - 1)

	res := sess.moveStartedSharedQoS2(b.s, b.asm, &mqttQ2AffMoveInput{
		groupDurable: groupDur,
		natsSubject:  b.topic,
		msg:          []byte("tiny"),
		maxPayload:   maxPayload,
	})
	if res != mqttQ2AffMoveDropped {
		t.Fatalf("the move returned %v, want dropped: a message whose assignment claim exceeds the account max payload must be discarded, not elected", res)
	}
	// And nothing was written: an election here would be a claim the broker can never
	// store, so every later attempt at that subject wedges on it.
	if got := b.claims(t); len(got) != 0 {
		t.Fatalf("an assignment claim was elected for a message whose claim record cannot be stored: %v", got)
	}
}

// TestQ2AffMoverRefusesAnUnattributableClaim drives the MOVER's validation at its
// production site, which review found T11 was bypassing: it called the predicate directly
// and then exercised only the reaper and recovery, so removing the validator from
// moveStartedSharedQoS2 was unobservable and an unattributable claim could reach
// materialization.
//
// The claim subject is predicted the way F-066's fixture predicts it — from the next
// source sequence — because the mover derives it from the delivery's own reply subject,
// so a synthetic input elects on a different subject entirely and proves nothing. A
// prediction that misses SKIPS rather than passing: concurrent stream activity is not a
// result.
//
// Paired mutation: remove the q2AffClaimUsableForDecision call from the mover's
// post-rejection load. The delivery is then materialized against a record whose copy
// subject does not follow from its own identity.
func TestQ2AffMoverRefusesAnUnattributableClaim(t *testing.T) {
	b := testQ2AffSetup(t, "moverrefuse")
	c, r := b.member(t, "moverrefuseA", 300)
	defer c.Close()
	b.bind(t, "moverrefuseA")
	idHash := getHash("moverrefuseA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)

	// The subject the NEXT publish will elect on.
	nextSeq := b.lastSourceSeq(t) + 1
	subject := mqttQ2AffClaimSubject(groupDur, nextSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, nextSeq)
	bad := &mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: nextSeq,
		ClaimHash: claimHash, Nonce: mqttNewQ2AffGen(),
		// Derived for a DIFFERENT incarnation: a shape no election can emit.
		CopySubject: mqttQ2AffCopySubject(idHash, gen, claimHash, mqttNewQ2AffGen()),
		AffDurable:  affDurableFor(idHash, gen),
		// TERMINAL, and that is the point: a claimed record routes the mover into
		// materializeQ2AffCopy, whose own refusal then shadows this one and makes the
		// mutation unobservable. The terminal arm is where the mover acts ALONE — it
		// confirms the source acknowledgement and reaps — so it is the arm where its
		// refusal is load-bearing.
		Discarded: true,
		Phase:     mqttQ2AffMaterialized,
	}
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, bad); err != nil {
		t.Fatalf("seeding the unattributable claim: %v", err)
	}

	// A POSITIVE signal that the mover RAN, and with what disposition. Without it every
	// assertion below passes when the group callback is merely delayed — the inbound
	// publish only proves the source was stored, not that the move was attempted.
	moved := make(chan mqttQ2AffMoveResult, 4)
	b.asm.mu.Lock()
	b.asm.testQ2AffMoveDone = func(cs string, res mqttQ2AffMoveResult) {
		if cs == subject {
			select {
			case moved <- res:
			default:
			}
		}
	}
	b.asm.mu.Unlock()
	defer func() { b.asm.mu.Lock(); b.asm.testQ2AffMoveDone = nil; b.asm.mu.Unlock() }()

	b.publish(t, 1, "unattributable")
	if got := b.lastSourceSeq(t); got != nextSeq {
		t.Skipf("the publish landed at %d, not the predicted %d; concurrent stream activity, not a result", got, nextSeq)
	}
	select {
	case res := <-moved:
		if res != mqttQ2AffMoveDeferred {
			t.Fatalf("the mover resolved %v against an unattributable claim, want deferred: it must prove nothing, write nothing and acknowledge nothing", res)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the mover never ran for the predicted claim subject, so nothing below exercises its refusal")
	}

	// Nothing may be delivered against a record the broker cannot attribute, and the
	// record itself must be untouched: the mover proves nothing, writes nothing and
	// acknowledges nothing.
	if _, _, _, _, ok := testMQTTTryReadPublishV5(r, 2*time.Second); ok {
		t.Fatal("a PUBLISH was delivered against a claim whose copy subject does not follow from its own identity")
	}
	after, _, err := b.asm.loadQ2AffClaim(subject)
	if err != nil || after == nil {
		t.Fatalf("the unattributable claim was acted on: %v", err)
	}
	if after.SourceAcked {
		t.Fatalf("the mover CONFIRMED the source acknowledgement of a claim it cannot attribute, which is irreversible: %+v", after)
	}
	if st, _ := b.asm.loadQ2AffCopy(bad.CopySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("a copy was materialized onto a subject the record does not own (%v)", st)
	}
}

// TestQ2AffGateRefusesAnUnattributableClaim drives the DELIVERY GATE's refusal, which the
// earlier version of this coverage could not: it staged the copy while the member was
// DISCONNECTED, so the gate never ran and removing its refusal changed nothing. That was
// the eighth fixture in this change to bypass the wiring it claimed to cover, and it was
// caught by RUNNING the mutation rather than by reading the test.
//
// The claim carries an incarnation that is not a mintable value but IS the one its copy
// subject derives from, so the derivation check passes and only the minted-form check can
// refuse. The member stays CONNECTED, so the copy really reaches the gate.
//
// Paired mutation M20: remove the gate's q2AffClaimUsableForDecision arm. The copy is then
// published to the client against a record the broker cannot attribute.
func TestQ2AffGateRefusesAnUnattributableClaim(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "gaterefuse")
	c, r := b.member(t, "gaterefuseA", 300)
	defer c.Close()
	b.bind(t, "gaterefuseA")
	idHash := getHash("gaterefuseA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)

	// One character short of a minted incarnation: its copy subject derives from it
	// consistently, which is what isolates the minted-form check.
	shortNonce := strings.Repeat("z", mqttQ2AffGenLen-1)
	const srcSeq = uint64(755001)
	subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, srcSeq)
	bad := &mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
		ClaimHash: claimHash, Nonce: shortNonce,
		CopySubject: mqttQ2AffCopySubject(idHash, gen, claimHash, shortNonce),
		AffDurable:  affDurableFor(idHash, gen),
		Phase:       mqttQ2AffMaterialized,
	}
	if q2AffClaimUsableForDecision(subject, bad) == nil {
		t.Fatal("the staged claim is attributable, so this fixture cannot exercise the refusal")
	}
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, bad); err != nil {
		t.Fatalf("seeding the claim: %v", err)
	}
	env := &mqttQ2AffineEnvelope{
		natsSubject: b.topic, msg: []byte("unattributable"),
		claimSubject: subject, gen: gen,
		msgID: mqttQ2AffMsgId(groupDur, srcSeq, shortNonce),
	}
	// A POSITIVE signal that the GATE ran: it loads the claim named in the copy's header,
	// and nothing else in this fixture touches that subject. Without it, "no PUBLISH" and
	// "the copy is still present" both pass when the affine callback simply never fired.
	var gateLoads atomic.Int32
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimLoad = func(subj string) (*mqttQ2AffClaim, uint64, error) {
		if subj == subject {
			gateLoads.Add(1)
		}
		return b.asm.jsa.loadQ2AffClaim(subj)
	}
	b.asm.mu.Unlock()
	defer func() { b.asm.mu.Lock(); b.asm.testQ2AffClaimLoad = nil; b.asm.mu.Unlock() }()

	msg, hdr := mqttNewQ2AffineRecord(env)
	if _, err := b.asm.storeQ2AffCopy(bad.CopySubject, hdr, msg); err != nil {
		t.Fatalf("staging the copy: %v", err)
	}
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if gateLoads.Load() == 0 {
			return fmt.Errorf("the delivery gate has not run for this copy yet")
		}
		return nil
	})

	// Delivered to a CONNECTED member, so the gate really runs. It must publish nothing.
	if topic, payload, _, _, ok := testMQTTTryReadPublishV5(r, 2*time.Second); ok {
		t.Fatalf("the gate published a copy whose claim the broker cannot attribute: topic=%q payload=%q", topic, payload)
	}
	// And it must not dispose of it either: nothing here can prove what an unattributable
	// record's copy belongs to, so the copy is NAK'd and left for an actor with evidence.
	if st, _ := b.asm.loadQ2AffCopy(bad.CopySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the gate disposed of the copy (%v) instead of NAKing it", st)
	}
}

// TestQ2AffReapRevalidatesAfterItsOwnTransition pins the refusal on the claim the reaper's
// OWN SourceAcked transition returns, which review found unvalidated.
//
// That transition is identity-unfenced by design — a SourceAcked write is a fact about the
// source MESSAGE, so it applies to whichever incarnation holds the subject — and it
// therefore returns whatever record it loaded, which may be a concurrently rewritten
// occupant. The copy subject that decides whether the claim is DELETED comes from that
// record, so an unvalidated one lets a malformed rewrite redirect the absence check and
// strand the original copy with no claim to authorize or dispose of it.
//
// The rewrite is interposed on the load INSIDE the transition, which is the real window
// rather than a simulation of one. The load counter is the positive signal that the
// interposition actually happened — without it this test would pass whenever the reaper
// took a different path.
//
// Paired mutation M24: drop the post-transition re-validation. The reaper then reads the
// malformed record's copy subject, finds nothing there, and deletes the claim.
func TestQ2AffReapRevalidatesAfterItsOwnTransition(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "reapreval")
	c, _ := b.member(t, "reaprevalA", 300)
	b.bind(t, "reaprevalA")
	idHash := getHash("reaprevalA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// A source sequence that is authoritatively gone, so the reaper performs the
	// SourceAcked transition itself — that transition is the window under test.
	const goneSeq = uint64(866001)
	subject := mqttQ2AffClaimSubject(groupDur, goneSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, goneSeq)
	good := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: goneSeq,
		ClaimHash: claimHash, Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(idHash, gen),
		Phase:      mqttQ2AffMaterialized, Discarded: true,
	})
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, good); err != nil {
		t.Fatalf("seeding the claim: %v", err)
	}
	// A REAL copy on the claim's own subject: it is what must survive, and what a
	// redirected absence check would strand.
	env := &mqttQ2AffineEnvelope{
		natsSubject: b.topic, msg: []byte("stranded"),
		claimSubject: subject, gen: gen,
		msgID: mqttQ2AffMsgId(groupDur, goneSeq, good.Nonce),
	}
	msg, hdr := mqttNewQ2AffineRecord(env)
	if _, err := b.asm.storeQ2AffCopy(good.CopySubject, hdr, msg); err != nil {
		t.Fatalf("staging the copy: %v", err)
	}

	// Interpose a MALFORMED occupant on the load inside the transition: same identity, but
	// a copy subject that does not follow from it, pointing somewhere empty.
	var loads atomic.Int32
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimLoad = func(subj string) (*mqttQ2AffClaim, uint64, error) {
		cl, sq, err := b.asm.jsa.loadQ2AffClaim(subj)
		if subj != subject || cl == nil || err != nil {
			return cl, sq, err
		}
		if loads.Add(1) == 1 {
			return cl, sq, nil // the reaper's own first read: the good record
		}
		rewritten := *cl
		rewritten.CopySubject = mqttQ2AffCopySubject(idHash, gen, claimHash, mqttNewQ2AffGen())
		return &rewritten, sq, nil
	}
	b.asm.mu.Unlock()
	defer func() { b.asm.mu.Lock(); b.asm.testQ2AffClaimLoad = nil; b.asm.mu.Unlock() }()

	b.asm.reapQ2AffClaim(b.s, subject)

	if loads.Load() < 2 {
		t.Fatalf("the reaper loaded the claim %d time(s); it never entered its SourceAcked transition, so this run does not exercise the window", loads.Load())
	}
	if got, _, err := b.asm.loadQ2AffClaim(subject); err != nil || got == nil {
		t.Fatal("the claim was reaped on a copy subject taken from an unvalidated post-transition record: the real copy is still on the stream and now has no claim able to authorize or dispose of it")
	}
	if st, _ := b.asm.loadQ2AffCopy(good.CopySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the staged copy is gone (%v), so the assertion above proved nothing", st)
	}
}

// TestQ2AffMaterializeRefusesAnUnattributablePostTransitionRecord is the materialization
// half of the post-transition trust window — the same class as the reaper's, found one
// round later because closing it in one place did not close it in the other.
//
// advanceQ2AffClaim reloads the record and matches identity fields ONLY, so a concurrent
// rewrite that preserves winner, generation and incarnation while making CopySubject
// inconsistent gets `materialized` written onto it. Reporting that as success licenses the
// caller's IRREVERSIBLE group-source acknowledgement, and the delivery gate — which does
// validate — then NAKs the retained copy forever: the message is released from the group
// and can never be delivered.
//
// The rewrite is interposed on the load INSIDE the transition, and the load counter is the
// positive signal that the interposition actually happened.
//
// Paired mutation M25: drop the post-transition validation in materializeQ2AffCopy.
func TestQ2AffMaterializeRefusesAnUnattributablePostTransitionRecord(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "matreval")
	c, _ := b.member(t, "matrevalA", 300)
	b.bind(t, "matrevalA")
	idHash := getHash("matrevalA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("no affine generation")
	}
	groupDur := b.groupDurable(t)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	const srcSeq = uint64(977001)
	subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
	claimHash := mqttQ2AffClaimHash(groupDur, srcSeq)
	good := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
		ClaimHash: claimHash, Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffClaimed,
	})
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, good); err != nil {
		t.Fatalf("seeding the claim: %v", err)
	}
	cur, seq, _ := b.asm.loadQ2AffClaim(subject)

	// Identity fields preserved, CopySubject made inconsistent: the shape the fence
	// cannot catch, because the fence compares winner, generation and incarnation.
	var loads atomic.Int32
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimLoad = func(subj string) (*mqttQ2AffClaim, uint64, error) {
		cl, sq, err := b.asm.jsa.loadQ2AffClaim(subj)
		if subj != subject || cl == nil || err != nil {
			return cl, sq, err
		}
		loads.Add(1)
		rewritten := *cl
		rewritten.CopySubject = mqttQ2AffCopySubject(idHash, gen, claimHash, mqttNewQ2AffGen())
		return &rewritten, sq, nil
	}
	b.asm.mu.Unlock()
	defer func() { b.asm.mu.Lock(); b.asm.testQ2AffClaimLoad = nil; b.asm.mu.Unlock() }()

	env := &mqttQ2AffineEnvelope{natsSubject: b.topic, msg: []byte("attributable?")}
	ok := b.asm.materializeQ2AffCopy(b.s, cur, subject, seq, env)

	if loads.Load() == 0 {
		t.Fatal("the transition never reloaded the record, so this run does not exercise the window")
	}
	if ok {
		t.Fatal("materialization reported SUCCESS after writing `materialized` onto a record that cannot be attributed: its caller would then confirm the group-source acknowledgement irreversibly, while the delivery gate — which does validate — NAKs the retained copy forever")
	}
}

// TestQ2AffClaimMustAgreeWithItsOwnSubject pins the binding that closes review's bypass of
// the post-transition validator, and the shape is the whole point: a rewrite that preserves
// winner, generation and incarnation while changing ClaimHash and RECOMPUTING CopySubject
// from it is internally self-consistent. It passes the incarnation fence — which compares
// winner, generation and nonce only — and it passed the first version of the validator,
// which checked only that CopySubject followed from the record's own fields.
//
// Acting on such a record materializes and then IRREVERSIBLY releases a source under a
// claim hash belonging to some other (group, sequence); the delivery gate then reads the
// real copy's subject as a superseded incarnation and acknowledges it away without
// publishing. The message is lost, which is worse than the stranding F-065 set out to fix.
//
// So a record must agree with the SUBJECT it was found on and with its own source, which
// self-consistency does not give. Same discipline F-058 applies to Will records.
//
// Paired mutation M26: drop the ClaimHash / subject binding from the validator.
func TestQ2AffClaimMustAgreeWithItsOwnSubject(t *testing.T) {
	t.Parallel()
	const groupDur = "$MQTT_SHARED_0123456789abcdef_1"
	const srcSeq = uint64(4242)
	subject := mqttQ2AffClaimSubject(groupDur, srcSeq)
	idHash, gen := getHash("agreeA"), mqttNewQ2AffGen()

	good := testQ2AffSelfConsistent(&mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: srcSeq,
		ClaimHash: mqttQ2AffClaimHash(groupDur, srcSeq), Nonce: mqttNewQ2AffGen(),
		AffDurable: affDurableFor(idHash, gen), Phase: mqttQ2AffMaterialized,
	})
	if err := q2AffClaimUsableForDecision(subject, good); err != nil {
		t.Fatalf("a record that agrees with its own subject was refused: %v", err)
	}

	// The bypass: a DIFFERENT claim hash, with every derived name recomputed from it.
	forged := *good
	forged.ClaimHash = mqttQ2AffClaimHash(groupDur, srcSeq+1)
	forged.CopySubject = mqttQ2AffCopySubject(forged.Winner, forged.Gen, forged.ClaimHash, forged.Nonce)
	if want := mqttQ2AffCopySubject(forged.Winner, forged.Gen, forged.ClaimHash, forged.Nonce); forged.CopySubject != want {
		t.Fatal("the forged record is not self-consistent, so it does not stage the bypass")
	}
	if forged.ownedBy(good.Winner, good.Gen) != true || forged.Nonce != good.Nonce {
		t.Fatal("the forged record must preserve winner, generation and incarnation, or the identity fence would catch it and this proves nothing")
	}
	if err := q2AffClaimUsableForDecision(subject, &forged); err == nil {
		t.Fatal("a self-consistent record whose claim hash belongs to a DIFFERENT source was accepted: materializing on it would irreversibly release this source, and the gate would then acknowledge the real copy away as a superseded incarnation without publishing it")
	}

	// The same discipline in the other direction: a well-formed record found on the WRONG
	// subject is refused, because agreement with the subject is what makes it attributable.
	if err := q2AffClaimUsableForDecision(mqttQ2AffClaimSubject(groupDur, srcSeq+7), good); err == nil {
		t.Fatal("a record was accepted on a subject that does not follow from the group and source sequence it names")
	}
}
