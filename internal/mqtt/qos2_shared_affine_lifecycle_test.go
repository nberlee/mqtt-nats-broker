//go:build mqttport_fulltests

// Conformance and invariant tests for session-affine ownership of a STARTED
// shared-subscription QoS 2 delivery (qos2_shared_affine.go).
//
// The property under test is [MQTT-4.8.2-5]/[MQTT-4.8.2-6]: once the server has
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// Dying with the session is PERMITTED ([MQTT-4.8.2-6]); being handed to another
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

// Losing the affine consumer out of band is message loss, and recovery must not
// launder it into a completed delivery. $MQTT_out is interest-retained, so
// deleting the consumer deletes the copies it held; an absent copy under a
// materialized claim then means two different things, and only one of them is a
// success. The claim must end up recorded as LOST — terminal, so nothing waits on
// it, but never reaped, so the durable evidence of an undelivered QoS 2 message
// survives.
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
			t.Fatal("the assignment for an undelivered QoS 2 message was REAPED; the only durable evidence of the loss is gone")
		}
		if claim.AffineDone {
			t.Fatal("an undelivered QoS 2 message was recorded as a completed delivery")
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
// record of a message the broker accepted and never delivered, and it happens
// BEFORE the owner ever resumes to record it as lost.
//
// The reaper call below is not a stand-in: it is literally what the contender
// runs after confirmSourceAckAndRecord (moveStartedSharedQoS2's foreign-winner
// branch). Driving it directly is what makes the ordering deterministic — an
// unconfirmed source ack cannot be produced from the wire, since the broker acks
// the group delivery as soon as the claim materializes.
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
		t.Fatal("a contender reaped an undelivered QoS 2 message's assignment; the owner can no longer record it as lost, and the evidence is gone")
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
		t.Fatal("the assignment for an undelivered QoS 2 message was reaped after the consumer was recreated")
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
// ordinary inference would fire and leave a record claiming both that the message
// was delivered and that it was lost.
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
// redistribution [MQTT-4.8.2-6] forbids.
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
// [MQTT-4.8.2-6] forbids.
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
// it is the only durable evidence that a message the broker accepted was never
// delivered — so a guard that treated it as owed would retain this path for the
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
		t.Fatal("the reclaim reaped a Lost assignment; it is the only durable evidence that an accepted message was never delivered")
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

// TestQ2AffLiveRecordLeavesAStrandedClaimToOwnerResume pins F-064.
//
// buildSessionRefs keys refs.affine on ps.Q2AffGen alone, so a live matching record
// makes the sweep skip EVERY claim of that generation — reconciliation, the discard,
// and the consumer pass alike. recoverQ2AffClaims, the only writer of Lost, runs on the
// owner's own resume. So a nonterminal claim whose affine consumer is gone, under a
// session that never comes back, is reconciled by nothing at all: contenders keep
// failing the consumer lookup and the group's source keeps being re-offered, because
// no MaxDeliver bounds it.
//
// Discriminating mutation: give the sweep an owner-independent reconciler for claims
// under a live reference (the remedy). The claim then resolves and this fixture fails
// at "the claim was left untouched".
func TestQ2AffLiveRecordLeavesAStrandedClaimToOwnerResume(t *testing.T) {
	t.Parallel()
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
		// A failure, not a skip, for the same reason as F-065's fixture above.
		t.Fatalf("the sweep resolved the stranded claim (Discarded=%v Lost=%v AffineDone=%v), so F-064 is fixed: replace this defect fixture with a regression test for the owner-independent reconciler",
			got.Discarded, got.Lost, got.AffineDone)
	}
	if got.Phase != mqttQ2AffClaimed {
		t.Fatalf("the claim's phase moved to %v without a terminal fact", got.Phase)
	}
	// F-064, pinned: a full sweep pass, and the claim is exactly as it was.
	t.Log("F-064 reproduced: a live generation reference left a nonterminal claim, whose consumer is gone, entirely untouched by the sweep")
}

// TestQ2AffSourceAckedAfterAStoreThatKeptNothing pins F-063.
//
// $MQTT_out is interest-retained, so a copy stored onto a subject no consumer filters
// SUCCEEDS AND KEEPS NOTHING. The own-claim path never learns that: beginQ2AffOp admits the
// move on local state alone (a readiness flag, a generation and a consumer POINTER), and
// materializeQ2AffCopy re-reads only when the create-only CAS is REJECTED — a store that
// succeeds is never verified. The claim advances to materialized and the source is then
// acknowledged, which is irreversible and releases the message from the shared group.
//
// Staged with no seam at all, because none is needed: deleting the affine consumer out of
// band leaves the live session's cached pointer intact, which is precisely the state the
// admission gate cannot see. Everything after that is the ordinary production path.
//
// Leftover asserted: materialized + SourceAcked, copy AUTHORITATIVELY absent, and the group
// source gone — an accepted QoS 2 message that no redelivery can recover and that only a
// later owner resume would even record as Lost.
//
// Discriminating mutation: verify retention after the store and refuse to advance when it
// cannot be proven. The claim then never reaches materialized, the source is never acked,
// and this fixture fails.
//
// **That mutation closes THIS ROUTE, not the finding.** A post-store check still leaves a
// consumer deleted between the check and confirmSourceAckAndRecord, and the
// already-materialized contender route acknowledges the source at qos2_shared_affine.go
// without any such check at all. So this fixture going green again is evidence about the
// own-claim path only; F-063's premise is finding-wide and needs the on-behalf TOCTOU and
// the materialized-observation route covered too, each with its own staging. The failure
// message below says exactly that, deliberately, so a partial remedy cannot read as a
// closed finding.
func TestQ2AffSourceAckedAfterAStoreThatKeptNothing(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "keptnothing")
	c, _ := b.member(t, "keptnothingA", 300)
	defer c.Close()
	b.bind(t, "keptnothingA")

	idHash := getHash("keptnothingA")
	gen := testQ2AffAwaitStableRecord(t, b, idHash).Q2AffGen
	if gen == _EMPTY_ {
		t.Fatal("the member has no affine generation, so there is no consumer to remove")
	}

	// The consumer goes; the session's cached pointer stays. beginQ2AffOp still admits.
	if _, err := b.asm.jsa.deleteConsumer(mqttOutStreamName, affDurableFor(idHash, gen), false); err != nil {
		t.Fatalf("deleting the affine consumer out of band: %v", err)
	}

	// An ordinary QoS 2 publish to the shared group. The member's own move elects a claim,
	// stores the copy onto nothing, and acknowledges the source.
	b.publish(t, 1, "keptnothing")

	deadline := time.Now().Add(10 * time.Second)
	var claim *mqttQ2AffClaim
	var subject string
	for time.Now().Before(deadline) {
		for sub, cl := range b.claims(t) {
			if cl.ownedBy(idHash, gen) {
				claim, subject = cl, sub
				break
			}
		}
		if claim != nil && claim.SourceAcked && claim.Phase == mqttQ2AffMaterialized {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if claim == nil {
		t.Fatal("no assignment claim was elected for the member; the move never ran, so this run did not exercise the path")
	}
	if claim.Phase != mqttQ2AffMaterialized {
		t.Fatalf("claim on %q is %v, want materialized: the copy store did not report success",
			subject, claim.Phase)
	}
	if !claim.SourceAcked {
		// A remedy landing on the own-claim path reaches exactly this branch. Do NOT read
		// it as F-063 closed: the on-behalf TOCTOU and the materialized-observation route
		// are not staged here and are not covered by a post-store check.
		t.Fatalf("claim on %q was NOT source-acked, so the OWN-CLAIM route of F-063 is closed: re-point this fixture at a regression test for that route, and confirm the on-behalf TOCTOU and the materialized-observation route separately before closing the finding",
			subject)
	}
	// The copy has to be AUTHORITATIVELY absent, not merely unreadable, or the assertion
	// would rest on a failed lookup rather than on interest retention having kept nothing.
	if st, _ := b.asm.loadQ2AffCopy(claim.CopySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("the copy on %q reads %v, want absent: the store kept something after all",
			claim.CopySubject, st)
	}
	if claim.AffineDone || claim.Lost || claim.Discarded {
		t.Fatalf("claim on %q carries a terminal fact (AffineDone=%v Lost=%v Discarded=%v); nothing delivered or disposed of this message",
			subject, claim.AffineDone, claim.Lost, claim.Discarded)
	}
	// F-063, pinned: the group source was released on the strength of a store that
	// retained nothing, and the message is gone with no record that it was lost.
	t.Log("F-063 reproduced: materialized + SourceAcked with no copy — an accepted QoS 2 message released from the group and lost")
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
// Asserted: the client receives NOTHING. The mover takes its terminal arm, acknowledges
// the group source because the stale claim says the ack was already proven, and the
// accepted message is released from the shared group with no copy and no delivery.
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

	// A claim left over from the previous incarnation: terminal, and its source ack
	// already proven — the ordinary resting state of a claim awaiting only a reap.
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
