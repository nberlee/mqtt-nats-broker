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
	"strings"
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
	// And the cleared object really can provision again.
	if err := sess.ensureQ2AffineConsumerSubscription(cl); err != nil {
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

	if err := sess.ensureQ2AffineConsumerSubscription(cl); err == nil {
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

	// The consumer is destroyed out of band, exactly as an operator (or an older
	// broker's sweep) could. Its pending copy goes with it.
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
