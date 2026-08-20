//go:build mqttport_fulltests

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
	_, _, durable := mqttSharedNames(b.group, string(subj))
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
		// A copy subject that provably has no message on it, so "copy absent" is a
		// real answer from the stream rather than an assumption.
		CopySubject: mqttQ2AffCopySubject(getHash("reapA"), mqttNewQ2AffGen(), "deadbeefdeadbeef"),
		AffDurable:  "$MQTT_Q2AFF_reaptest",
		Phase:       mqttQ2AffMaterialized,
	}

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
		CopySubject: mqttQ2AffCopySubject(idHash, gen, "cafecafecafecafe"),
		Phase:       mqttQ2AffMaterialized, SourceAcked: true,
	}); err != nil {
		t.Fatalf("writing the claim: %v", err)
	}

	got, _, err := b.asm.advanceQ2AffClaim(subject, idHash, gen, func(c *mqttQ2AffClaim) { c.Discarded = true })
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
	if _, _, err := b.asm.advanceQ2AffClaim(subject, idHash, mqttNewQ2AffGen(),
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
		return sess.ensureQ2AffineConsumerSubscription(cl, tok)
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
		return sess.ensureQ2AffineConsumerSubscription(cl, tok)
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

	// The consumer is destroyed out of band, exactly as an operator (or a sweep
	// that no longer sees a session record) could. Its pending copy goes with it.
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
			CopySubject: mqttQ2AffCopySubject(idHash, gen, "1111111111111111"),
			AffDurable:  "$MQTT_Q2AFF_unreaped",
			Phase:       mqttQ2AffMaterialized, SourceAcked: true, AffineDone: true,
		}); err != nil {
			t.Fatalf("writing the claim: %v", err)
		}
		b.asm.recoverQ2AffClaims(idHash, gen)
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
			CopySubject: mqttQ2AffCopySubject(idHash, gen, "3333333333333333"),
			AffDurable:  "$MQTT_Q2AFF_unreaped",
			Phase:       mqttQ2AffMaterialized, AffineDone: true,
		}); err != nil {
			t.Fatalf("writing the claim: %v", err)
		}
		if _, err := b.jsa.lookupConsumer(mqttStreamName, groupDur); err != nil {
			t.Fatalf("this subtest needs a LIVE group consumer, but %q is not there: %v", groupDur, err)
		}
		b.asm.recoverQ2AffClaims(idHash, gen)
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
		copySubject := mqttQ2AffCopySubject(idHash, gen, "2222222222222222")
		if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, &mqttQ2AffClaim{
			Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: 5151,
			ClaimHash: mqttQ2AffClaimHash(groupDur, 5151), CopySubject: copySubject,
			AffDurable: "$MQTT_Q2AFF_unreaped",
			Phase:      mqttQ2AffMaterialized, SourceAcked: true, AffineDone: true,
		}); err != nil {
			t.Fatalf("writing the claim: %v", err)
		}
		// A real message on the claim's copy subject: the session's affine consumer
		// holds interest on the whole generation prefix, so it is retained.
		env := &mqttQ2AffineEnvelope{
			natsSubject: "unreaped.topic", msg: []byte("still here"),
			claimSubject: subject, gen: gen, msgID: mqttQ2AffMsgId(groupDur, 5151),
		}
		msg, hdrLen := mqttNewQ2AffineRecord(env)
		if _, err := b.asm.storeQ2AffCopy(copySubject, hdrLen, msg); err != nil {
			t.Fatalf("storing the copy: %v", err)
		}
		if state, _ := b.asm.loadQ2AffCopy(copySubject); state != mqttQ2AffCopyPresent {
			t.Fatalf("the copy was not retained; this subtest cannot test what it claims (state=%v)", state)
		}

		b.asm.recoverQ2AffClaims(idHash, gen)
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
	if state, _ := b.asm.loadQ2AffCopy(mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(b.groupDurable(t), 1))); state == mqttQ2AffCopyPresent {
		t.Fatal("the copy should have gone with its consumer")
	}

	// Reconcile as a broker that has observed nothing: the consumer is present and
	// looks healthy, and no in-memory flag from anywhere is consulted.
	b.asm.recoverQ2AffClaims(idHash, gen)

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
		CopySubject: mqttQ2AffCopySubject(idHash, gen, "beefbeefbeefbeef"),
		AffDurable:  "$MQTT_Q2AFF_lostonce",
		Phase:       mqttQ2AffMaterialized, SourceAcked: true, Lost: true,
	}); err != nil {
		t.Fatalf("writing the claim: %v", err)
	}

	// A resume with an intact consumer: the ordinary path would infer completion
	// from the absent copy.
	b.asm.recoverQ2AffClaims(idHash, gen)

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
		ClaimHash: "0123456789abcdef", CopySubject: "$MQTT.out.q2.abc.g.h", AffDurable: "$MQTT_Q2AFF_abc_g",
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
// confirming save is refused for the same reason) and the orphan sweep passes
// over it precisely because the record names it. The settlement therefore owns
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
	// rather than a bare total. The affine sweep excludes $MQTT_PUBREL_* and
	// finishQ2AffineUnwind refunds only the affine increment, so a PUBREL consumer
	// created INSIDE the ambiguous packet would be a second unaccounted resource.
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
		a.sess.sharedSubs = make(map[string]byte)
	}
	a.sess.sharedSubs[key] = 2
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
		CopySubject: mqttQ2AffCopySubject(idHash, gen, claimHash),
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
// record makes its owner. This is the clustered takeover-racing-the-cleanup shape
// reduced to the arm that actually decides it.
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
	// one omits the affine fields, which makes it the orphan sweep's work.
	if durs := a.b.affineConsumers(t, a.idHash); len(durs) != 1 {
		t.Fatalf("the disposal deleted an affine consumer it does not own, got %v", durs)
	}
}

// THE RACE THE ORDERING EXISTS FOR. A remote broker takes the session over and
// PRESERVES the affine generation — the q2aff-aware takeover, which carries the
// affine fields through — so the generation it now owns is the one this reclaim
// was about to remove, and an assignment under it is a delivery already started to
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
			CopySubject: mqttQ2AffCopySubject(a.idHash, gen, claimHash),
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
		CopySubject: mqttQ2AffCopySubject(a.idHash, gen, claimHash),
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
		CopySubject: mqttQ2AffCopySubject(a.idHash, gen, claimHash),
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
// take their copies with it, and the orphan sweep is the right owner for a
// consumer no record names. Everything LOCAL still converges: the delivery
// subscription goes and the MaxAckPending charge comes back exactly, because
// nothing else can ever reach them once the record stops naming the path — the
// sweep deletes consumers, it cannot refund another broker's budget.
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
	// orphan sweep then reclaims it — the same undecodable record aborts that
	// sweep's affine inventory too (orphan_qos2_sweep.go), so the consumer stays
	// pending until the record is repaired or removed. And it does not assert that
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

// A remote takeover during the unresolved window ends in a FOREIGN verdict, and a
// foreign verdict is not ours to act on: the record belongs to somebody else, the
// session is evicted, and the continuation never runs. The affine consumer must
// survive that — whether it is the winner's or the orphan sweep's is decided by
// the winning record, never by us deleting it on the way out.
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
	// what it finds is somebody else's record at a sequence past our base.
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
// exercise a takeover at all.
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
// content under the caller's control — for the takeover that PRESERVES the affine
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

// TestQ2AffReapUsesAStaleCopyReading pins F-065.
//
// reapQ2AffClaim reads the copy state, then may perform a SourceAcked transition
// that is a full JetStream round trip, then evaluates reapable() on the EARLIER
// reading. A copy stored during that round trip is invisible to it, so the claim
// is deleted out from under a copy that exists.
//
// The interleaving is staged through testQ2AffClaimStore, which intercepts exactly
// the transition whose duration is the window. That is the real window, not a
// simulation of one: the hook runs where the CAS runs.
//
// Leftover asserted: source proven, claim absent, copy PRESENT — the state
// reapable's own comment says must never exist, because the delivery gate takes its
// claim == nil branch and NAKs that copy forever.
//
// Discriminating mutation: make reapQ2AffClaim re-read the copy state after the
// SourceAcked advance and pass THAT to reapable. The reap then declines, the claim
// survives, and this fixture fails at "the claim was reaped".
func TestQ2AffReapUsesAStaleCopyReading(t *testing.T) {
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
	copySubject := mqttQ2AffCopySubject(idHash, gen, claimHash)

	claim := &mqttQ2AffClaim{
		Winner:       idHash,
		Gen:          gen,
		GroupDurable: groupDur,
		SourceSeq:    goneSeq,
		ClaimHash:    claimHash,
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
				msgID:        mqttQ2AffMsgId(groupDur, goneSeq),
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
	if gotClaim != nil {
		// Deliberately a FAILURE and not a skip. This fixture pins a defect, so the remedy
		// landing is exactly when someone must come back and replace it with a regression
		// test for the fix — a silent skip would let the pin rot into a no-op.
		t.Fatal("the reap declined, so F-065 is fixed: replace this defect fixture with a regression test asserting the claim survives a copy stored during the SourceAcked transition")
	}
	// F-065, pinned: the claim is gone and its copy is not.
	t.Log("F-065 reproduced: claim reaped at a stale copy reading, leaving a copy the delivery gate can only NAK")
}

// TestQ2AffResumeAdoptsConsumerKeepingItsCreated pins the mechanism behind F-036's third
// manifestation, the "adoption blind spot".
//
// The orphan sweep's step (5) re-verifies that the consumer it is about to delete is still
// the INSTANCE quarantine observed, by comparing ConsumerInfo.Created. That discriminates a
// delete-and-recreate, which is what the grace exists to protect. It does NOT discriminate a
// session that RESUMES onto the consumer it finds: a resume preserves the incarnation
// generation, derives the same durable name, and affine provisioning is a create-or-UPDATE,
// so no new instance is created and Created is unchanged.
//
// This is MECHANISM EVIDENCE, not a defect pin, and the distinction matters. Reaching the
// sweep's step-(4)-to-step-(6) window additionally requires a session resident with that
// generation while its record is absent — F-036's premise, and not something this fixture
// stages. The full interleaving belongs to F-036's fix, along with the seam it would need;
// there is none between those steps today. What this fixture owns is the half a fixer is
// most likely to get wrong: it proves the instance fingerprint is not the defense it reads
// as, so a fix that leans on it will still delete a live adopted consumer.
//
// It asserts a property of ordinary provisioning, so it is not paired with a remedy
// mutation: arbitrating the consumer delete — the actual fix — would leave Created
// unchanged and this test passing, correctly. The mutation it does discriminate is a
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

	// Leave and come back on the same session. This is the resume the sweep's window
	// admits, and the generation it comes back on is the one the sweep condemned.
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
		t.Fatalf("F-036's adoption blind spot no longer holds: the resume changed ConsumerInfo.Created from %v to %v, so the sweep's step-(5) fingerprint WOULD discriminate an adoption",
			created, after.Created)
	}
	// Pinned: a live resumed session carrying an instance fingerprint the sweep cannot
	// tell from the one it condemned.
	t.Log("F-036 adoption blind spot reproduced: an adopting resume leaves ConsumerInfo.Created unchanged, so step (5) cannot discriminate it")
}

// The split F-064 delivers, asserted from both sides.
//
// buildSessionRefs keys refs.affine on ps.Q2AffGen alone, so a live matching record
// makes the sweep skip EVERY claim of that generation — reconciliation, the discard,
// and the consumer pass alike. That has NOT changed and is not a defect: the sweep
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
		CopySubject:  mqttQ2AffCopySubject(idHash, gen, claimHash),
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
// only within one incarnation of $MQTT_msgs — and when that stream fails shape validation,
// startup instructs the operator to stop all brokers and recreate IT ALONE
// (stream_prereqs.go, stopAll), leaving $MQTT_qos2assign's claims behind with their
// sequence counter reset underneath them.
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
	stale := &mqttQ2AffClaim{
		Winner:       getHash("someone-else"),
		Gen:          mqttNewQ2AffGen(),
		GroupDurable: groupDur,
		SourceSeq:    nextSeq,
		ClaimHash:    claimHash,
		CopySubject:  mqttQ2AffCopySubject(getHash("someone-else"), mqttNewQ2AffGen(), claimHash),
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
		c.CopySubject = mqttQ2AffCopySubject(wHash, wGen, c.ClaimHash)
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
	// Polled, not read once: the success CONNACK is sent BEFORE the resume
	// re-establishes the affine path, so an immediate read races the work it is
	// trying to observe.
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
