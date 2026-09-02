//go:build mqttport_fulltests && mqttport_experimental_shared_qos2

// Replicated-JetStream (R3) tests for session-affine ownership of a started
// shared-subscription QoS 2 delivery, against a genuine 3-node embedded cluster
// (see jetstream_cluster_harness_test.go).
//
// These exist because the property being claimed is a DURABILITY property across
// BROKERS, and a single-process test cannot establish it: the whole point of the
// assignment claim is that a contender on a DIFFERENT broker, which shares no
// memory with the session that won, honors the winner. A same-process test can be
// satisfied by in-memory tracking that a second broker would never see.
//
// What they establish, precisely:
//
//   - TestQ2AffineReplicatedAssignmentStream (always-run): $MQTT_qos2assign really
//     is Replicas=3, current on every node, and satisfies the validator that gates
//     broker startup; $MQTT_out does too.
//   - TestQ2AffineCrossBrokerAffinity (always-run): a delivery started to a member
//     on one broker is never handed to a member on another broker when its owner
//     leaves, and completes through PUBCOMP when that owner returns.
//   - TestQ2AffClusterCopyLossOnDeletedConsumer (always-run): with the affine
//     consumers destroyed out of band, NO broker releases the group source — the
//     copy store succeeds and keeps nothing on every one of them, and the refusal
//     has to be read from the replicated claim rather than from either broker's
//     memory.
//   - TestQ2AffineOwnerBrokerLossKeepsAssignment (gated on
//     MQTTNB_CLUSTER_FAILOVER_TESTS=1): losing the OWNER'S BROKER outright does not
//     release the message to the group; the owner collects it from a surviving
//     broker.
//
// Packet-identifier continuity across a broker loss IS claimed and IS asserted. It was
// a documented HA deviation when this file was written, and the outbound in-flight
// register on `$MQTT_inflight` closed it: the identifier is durable and restored before
// the redelivery, which `docs/CONFORMANCE.md` states for every outbound path. The
// broker-loss test therefore captures the original identifier and requires the same one
// back. It previously accepted any identifier, on the strength of the old deviation, so a
// continuity regression on this path would have passed.

package mqtt

import (
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// testQ2AffClusterMember joins a v5 client to a shared group at QoS 2 on a given
// broker, with a Session Expiry Interval so a disconnect SUSPENDS the session
// rather than discarding it — a discarded session's assignment is legitimately
// terminal, which would make "the delivery waited for its owner" pass for the
// wrong reason.
func testQ2AffClusterMember(t *testing.T, b *testClusterBroker, clientID, group, topic string) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "$share/" + group + "/" + topic, opts: 2}})
	if len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("Expected SUBACK [2] for %s, got %v", clientID, codes)
	}
	testMQTTFlush(t, c, nil, r)
	return c, r
}

// testQ2AffClusterResume reconnects a suspended session without re-subscribing —
// how a client comes back for a delivery started before it left.
func testQ2AffClusterResume(t *testing.T, b *testClusterBroker, clientID string) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	return c, r
}

// testQ2AffClusterPublish completes one inbound QoS 2 exchange. QoS 2 inbound is
// required: the effective delivery QoS is the minimum of publish and subscription
// QoS, so a QoS 1 publish never enters the affine path at all.
func testQ2AffClusterPublish(t *testing.T, b *testClusterBroker, clientID, topic, payload string) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	pi := uint16(1)
	testMQTTPublishV5(t, c, 2, pi, topic, []byte(payload))
	if reason, _ := testMQTTReadPubRecV5(t, r, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBREC reason 0x%x", reason)
	}
	testQoS2SendPubRel(t, c, pi)
	if reason, _ := testMQTTReadPubCompV5(t, r, pi); reason != mqttReasonSuccess {
		t.Fatalf("PUBCOMP reason 0x%x", reason)
	}
}

// testQ2AffClusterComplete finishes the outbound handshake for pi, tolerating the
// duplicate PUBLISH redeliveries a short AckWait produces while the test works.
func testQ2AffClusterComplete(t *testing.T, c net.Conn, r *mqttReader, pi uint16) {
	t.Helper()
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	deadline := time.Now().Add(testMQTTTimeout)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r, 2*time.Second)
		if !ready {
			continue
		}
		start := r.pos
		switch pb & mqttPacketMask {
		case mqttPacketPubRel:
			r.pos = start + pl
			testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
			return
		case mqttPacketPub:
			r.pos = start + pl // duplicate redelivery
		default:
			t.Fatalf("unexpected packet 0x%x while awaiting PUBREL", pb&mqttPacketMask)
		}
	}
	t.Fatal("no PUBREL arrived for a PUBREC'd QoS 2 delivery")
}

// testQ2AffClusterClaims reads every assignment claim through one broker.
func testQ2AffClusterClaims(t *testing.T, b *testClusterBroker) map[string]*mqttQ2AffClaim {
	t.Helper()
	asm := testMQTTGetGlobalASM(t, b.s)
	out := map[string]*mqttQ2AffClaim{}
	if err := asm.scanQ2AffClaims(func(subject string, claim *mqttQ2AffClaim, _ uint64) error {
		out[subject] = claim
		return nil
	}); err != nil {
		t.Fatalf("scanning assignment claims: %v", err)
	}
	return out
}

// TestQ2AffineReplicatedAssignmentStream asserts the durable state this protocol
// arbitrates with is genuinely replicated and genuinely conforming. It is the
// discriminator between real cluster-wide arbitration and a single-node store: a
// claim that lives on one node cannot stop a contender on another.
func TestQ2AffineReplicatedAssignmentStream(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	js, err := b.nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo(mqttQoS2AssignStreamName)
		if err != nil {
			return fmt.Errorf("stream info: %w", err)
		}
		if si.Config.Replicas != 3 {
			return fmt.Errorf("Config.Replicas=%d, want 3\n%s", si.Config.Replicas, c.describe())
		}
		if si.Cluster == nil || si.Cluster.Leader == "" || len(si.Cluster.Replicas) != 2 {
			return fmt.Errorf("cluster info incomplete: %+v\n%s", si.Cluster, c.describe())
		}
		return nil
	})
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2AssignStreamName)

	// Checked through the production validators rather than a restatement of
	// them: these are the same checks that refuse to start a broker.
	asm := testMQTTGetGlobalASM(t, b.s)
	si, err := asm.jsa.lookupStream(mqttQoS2AssignStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookup %s: si=%v err=%v", mqttQoS2AssignStreamName, si, err)
	}
	if verr := validateQoS2AssignStream(si, 3, asm.sessStreamEpoch); verr != nil {
		t.Fatalf("the R3 assignment stream the broker created does not satisfy its own validator: %v", verr)
	}
	si, err = asm.jsa.lookupStream(mqttOutStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookup %s: si=%v err=%v", mqttOutStreamName, si, err)
	}
	if verr := validateOutStream(si, 3, mqttDefaultMaxAckPending, asm.sessStreamEpoch); verr != nil {
		t.Fatalf("the R3 outgoing stream the broker created does not satisfy its own validator: %v", verr)
	}

	// A real assignment must land on the replicated stream, and be gone once both
	// sides of the exchange are proven complete.
	cs, rs := testQ2AffClusterMember(t, b, "q2aff-rep", "repg", "q2aff/rep")
	defer cs.Close()
	testQ2AffClusterPublish(t, b, "q2aff-rep-pub", "q2aff/rep", "replicated")

	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rs, testMQTTTimeout)
	if !ok || qos != 2 || string(payload) != "replicated" {
		// A delivery that never arrives says nothing on its own: it can mean the
		// arbitration lost the message or that the cluster was not in a state to
		// deliver anything. Report which.
		t.Fatalf("shared QoS 2 delivery: ok=%v qos=%d payload=%q\nclaims=%d\n%s",
			ok, qos, payload, len(testQ2AffClusterClaims(t, b)), c.describe())
	}
	if n := len(testQ2AffClusterClaims(t, b)); n != 1 {
		t.Fatalf("expected exactly one assignment claim on the replicated stream, got %d\n%s", n, c.describe())
	}
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2AssignStreamName)

	testQ2AffClusterComplete(t, cs, rs, pi)
	checkFor(t, testMQTTTimeout, 100*time.Millisecond, func() error {
		if n := len(testQ2AffClusterClaims(t, b)); n != 0 {
			return fmt.Errorf("%d assignment claims survived a completed exchange\n%s", n, c.describe())
		}
		return nil
	})
}

// TestQ2AffineCrossBrokerAffinity is the property no single-process test can
// establish: the member that must NOT receive the message is served by a
// different broker, with its own memory, its own consumers and its own view of
// who is delivering what. The only thing tying the two together is the durable
// assignment claim.
//
// Before this change the started message was pending on the shared GROUP durable,
// so AckWait offered it to whichever member was reachable — very much including
// one on another broker.
func TestQ2AffineCrossBrokerAffinity(t *testing.T) {
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	const group, topic = "xbg", "q2aff/xb"
	ca, ra := testQ2AffClusterMember(t, bA, "xbA", group, topic)
	cb, rb := testQ2AffClusterMember(t, bB, "xbB", group, topic)
	defer cb.Close()

	testQ2AffClusterPublish(t, bA, "xb-pub", topic, "crossbroker")

	// Identify the owner by who the group actually delivered to. The probe uses a
	// short timeout on purpose: it is a "did THIS member get it" question, and the
	// loser must not burn the whole cluster timeout answering no.
	const probe = 5 * time.Second
	ownerID, ownerBroker, ownerConn, otherReader := "xbA", bA, ca, rb
	_, payload, qos, _, ok := testMQTTTryReadPublishV5(ra, probe)
	if !ok {
		_, payload, qos, _, ok = testMQTTTryReadPublishV5(rb, probe)
		if !ok {
			ca.Close()
			t.Fatal("neither member received the shared QoS 2 delivery")
		}
		ownerID, ownerBroker, ownerConn, otherReader = "xbB", bB, cb, ra
	}
	if qos != 2 || string(payload) != "crossbroker" {
		t.Fatalf("unexpected delivery: payload=%q qos=%d", payload, qos)
	}

	// The claim must be readable from BOTH brokers and name the same winner:
	// arbitration that only one broker can see is not arbitration.
	claimsA, claimsB := testQ2AffClusterClaims(t, bA), testQ2AffClusterClaims(t, bB)
	if len(claimsA) != 1 || len(claimsB) != 1 {
		t.Fatalf("assignment visible from A: %d, from B: %d (want 1 each)", len(claimsA), len(claimsB))
	}
	for subj, ca := range claimsA {
		cbClaim, found := claimsB[subj]
		if !found {
			t.Fatalf("assignment %q is not visible from the other broker", subj)
		}
		if ca.Winner != cbClaim.Winner || ca.Winner != getHash(ownerID) {
			t.Fatalf("the two brokers disagree about the owner: %q vs %q (delivery went to %q)",
				ca.Winner, cbClaim.Winner, ownerID)
		}
	}

	// The owner leaves with the exchange unfinished.
	ownerConn.Close()

	// The member on the OTHER broker must never see it.
	if _, pl, _, _, got := testMQTTTryReadPublishV5(otherReader, 5*time.Second); got {
		t.Fatalf("a started shared QoS 2 delivery crossed to a member on another broker (payload %q)", pl)
	}

	// The owner returns to its own broker and completes what it started. What is asserted
	// here is that the SAME session gets the message and the exchange completes; the
	// identifier is deliberately not compared, and that is a limit of this fixture rather
	// than a permitted deviation — continuity is durable on every outbound path (see
	// TestQ2AffineOwnerBrokerLossKeepsAssignment below, and the discriminating coverage in
	// inflight_register_e2e_test.go).
	cr, rr := testQ2AffClusterResume(t, ownerBroker, ownerID)
	defer cr.Close()
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rr, testMQTTTimeout)
	if !ok {
		t.Fatal("the owning session did not receive its started delivery on resume")
	}
	if qos != 2 || string(payload) != "crossbroker" {
		t.Fatalf("unexpected redelivery: payload=%q qos=%d", payload, qos)
	}
	testQ2AffClusterComplete(t, cr, rr, pi)
	testMQTTExpectNothing(t, otherReader)
}

// TestQ2AffClusterCopyLossOnDeletedConsumer is F-063 across brokers.
//
// $MQTT_out is interest-retained, so a copy stored onto a subject no consumer filters
// SUCCEEDS AND KEEPS NOTHING. Neither broker can tell that from its own state: the
// winner's admission gate consults a cached consumer POINTER, and a contender's
// on-behalf lookup happens before the store rather than binding to it. Only a
// presence check on the copy subject distinguishes the two, and it has to hold on
// EVERY broker, because any one of them acking the group source releases the message
// for all of them.
//
// Staged as the real mechanism: every affine consumer in the account is destroyed out
// of band, through the broker that does NOT own the sessions' connections, and the
// members stay connected without re-subscribing so nothing re-provisions. Whichever
// member the group elects, its copy has nowhere to live.
//
// **Both brokers are required to have decided, and that is enforced rather than assumed.**
// The group durable is queue-delivered to both members' subscriptions, so each redelivery
// runs the move on one broker or the other; the fixture waits until EACH has recorded a
// `deferred` disposition for this claim through its own `testQ2AffMoveDone` seam. Reading
// the replicated claim through both nodes would only prove replication. The two brokers
// refuse for different reasons and both are needed for the invariant: the elected winner
// refuses at the presence check (F-063's remedy), and the contender refuses when the
// winner's affine consumer lookup fails.
//
// AckWait is shortened on both brokers BEFORE they start, because the group consumer bakes
// it in at creation. With the 30-second default there is exactly one delivery inside any
// reasonable observation window, so only the first mover would ever be exercised — the
// first version of this fixture had that defect and claimed otherwise.
//
// Asserted: neither member receives a PUBLISH; through BOTH brokers the claim stays at
// `claimed`, carries no terminal fact and is never source-acked; and — independently of the
// claim — the SOURCE MESSAGE is still on `$MQTT_msgs`. That last one is not redundant:
// `confirmAck` commits before the `SourceAcked` CAS, so `SourceAcked == false` alone cannot
// distinguish "never acked" from "acked, record not yet updated". What the refusal strands
// is F-064's; this fixture's business is that nothing was released against a store response.
//
// Discriminating mutation: verify only on a REJECTED create-only CAS (mutation 1).
// Whichever broker moves then advances the claim to materialized and acknowledges the
// group source, so this fixture fails on a released source visible from both nodes.
func TestQ2AffClusterCopyLossOnDeletedConsumer(t *testing.T) {
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	shortAckWait := func(o *Options) { o.MQTT.AckWait = 400 * time.Millisecond }
	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0, shortAckWait)
	bB := c.startBroker(t, 1, shortAckWait)

	const group, topic = "kngrp", "q2aff/kn"
	ca, ra := testQ2AffClusterMember(t, bA, "knA", group, topic)
	defer ca.Close()
	cb, rb := testQ2AffClusterMember(t, bB, "knB", group, topic)
	defer cb.Close()

	// One disposition log per broker, installed before the publish. A refusal sends
	// nothing at all, so this seam is the only thing that tells "this broker refused"
	// from "this broker never saw a delivery".
	moves := map[string]*testQ2AffMoveLog{}
	for name, b := range map[string]*testClusterBroker{"A": bA, "B": bB} {
		asm := testMQTTGetGlobalASM(t, b.s)
		l := &testQ2AffMoveLog{seen: map[string][]mqttQ2AffMoveResult{}}
		moves[name] = l
		asm.mu.Lock()
		asm.testQ2AffMoveDone = func(subject string, res mqttQ2AffMoveResult) {
			l.mu.Lock()
			l.seen[subject] = append(l.seen[subject], res)
			l.mu.Unlock()
		}
		asm.mu.Unlock()
		t.Cleanup(func() {
			asm.mu.Lock()
			asm.testQ2AffMoveDone = nil
			asm.mu.Unlock()
		})
	}

	// Destroyed through bB's account manager: the deletion is an operation of a broker
	// that owns neither session's connection, which is the out-of-band route
	// docs/CONFORMANCE.md documents and the one no in-memory signal can observe.
	// Every affine durable, so the fixture does not depend on which member wins.
	asmB := testMQTTGetGlobalASM(t, bB.s)
	cis, err := asmB.jsa.consumerList(mqttOutStreamName)
	if err != nil {
		t.Fatalf("listing consumers on %s: %v", mqttOutStreamName, err)
	}
	var removed int
	for _, ci := range cis {
		if ci == nil || !strings.HasPrefix(ci.Name, mqttQ2AffineConsumerDurablePrefix) {
			continue
		}
		if _, derr := asmB.jsa.deleteConsumer(mqttOutStreamName, ci.Name, false); derr != nil {
			t.Fatalf("deleting the affine consumer %q out of band: %v", ci.Name, derr)
		}
		removed++
	}
	if removed < 2 {
		t.Fatalf("removed %d affine consumers, want one per member: this run staged nothing", removed)
	}

	testQ2AffClusterPublish(t, bA, "kn-pub", topic, "keptnothing")

	// The claim has to be visible from both brokers before anything is asserted about
	// it: an assertion made through one node says nothing about what the other did.
	var subject string
	checkFor(t, testMQTTTimeout, 100*time.Millisecond, func() error {
		claimsA, claimsB := testQ2AffClusterClaims(t, bA), testQ2AffClusterClaims(t, bB)
		if len(claimsA) != 1 || len(claimsB) != 1 {
			return fmt.Errorf("assignment visible from A: %d, from B: %d (want 1 each)\n%s",
				len(claimsA), len(claimsB), c.describe())
		}
		for subj := range claimsA {
			if _, found := claimsB[subj]; !found {
				return fmt.Errorf("assignment %q is not visible from the other broker", subj)
			}
			subject = subj
		}
		return nil
	})

	// Neither member may receive it: the copy has nowhere to live, so a delivery here
	// would mean the message came off the GROUP durable instead.
	if _, pl, _, _, got := testMQTTTryReadPublishV5(ra, 5*time.Second); got {
		t.Fatalf("a member received a shared QoS 2 delivery whose affine copy cannot exist (payload %q)", pl)
	}
	if _, pl, _, _, got := testMQTTTryReadPublishV5(rb, 2*time.Second); got {
		t.Fatalf("the member on the other broker received a delivery whose affine copy cannot exist (payload %q)", pl)
	}

	// Over a window, and through BOTH brokers. Each broker must ALSO have refused at
	// least once by the end of it: the loop keeps going until both logs carry a
	// `deferred` for this claim, and fails if either never does — a fixture that
	// tolerated silence from one node would assert nothing about that node.
	deadline := time.Now().Add(testMQTTTimeout)
	for time.Now().Before(deadline) {
		for _, b := range []*testClusterBroker{bA, bB} {
			asm := testMQTTGetGlobalASM(t, b.s)
			claim, _, lerr := asm.loadQ2AffClaim(subject)
			if lerr != nil {
				t.Fatalf("loading the claim through a broker: %v", lerr)
			}
			if claim == nil {
				t.Fatalf("the claim on %q was reaped; nothing may reap a claim whose source was never released\n%s", subject, c.describe())
			}
			if claim.SourceAcked {
				t.Fatalf("a broker released the group source with no proof the copy was retained: %+v\n%s", *claim, c.describe())
			}
			if claim.terminal() {
				t.Fatalf("the claim on %q acquired a terminal fact (Discarded=%v AffineDone=%v Lost=%v); nothing delivered or disposed of this message",
					subject, claim.Discarded, claim.AffineDone, claim.Lost)
			}
			if claim.Phase != mqttQ2AffClaimed {
				t.Fatalf("the claim on %q moved to phase %v with no copy behind it\n%s", subject, claim.Phase, c.describe())
			}
			// AUTHORITATIVELY absent, not merely unreadable: a fixture resting on a failed
			// lookup would assert nothing about interest retention.
			if st, _ := asm.loadQ2AffCopy(claim.CopySubject); st != mqttQ2AffCopyAbsent {
				t.Fatalf("the copy on %q reads %v, want absent: the store kept something after all", claim.CopySubject, st)
			}
			// The source itself, independently of the claim, and by a POSITIVE load through
			// THIS broker: `confirmAck` commits BEFORE the SourceAcked CAS, so an unreleased
			// source is what proves the ack never happened. Not `!q2AffSourceGone`, which is
			// fail-closed and answers false both when the source is present and when it could
			// not be read — the single-broker fixtures were corrected first and this one was
			// left behind, which is the whole reason the propagation is now swept.
			testQ2AffAssertSourcePresent(t, asm, claim, "during the cross-broker refusal window")
		}
		if testQ2AffClusterRefused(moves["A"], subject) && testQ2AffClusterRefused(moves["B"], subject) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("only these brokers ever decided on %q — A=%v B=%v; a node that never ran the move proves nothing about it\n%s",
		subject, moves["A"].results(subject), moves["B"].results(subject), c.describe())
}

// testQ2AffClusterRefused reports whether this broker resolved a move on subject as
// `deferred` — prove nothing, write nothing, ack nothing — and never as one of the
// dispositions that release the group source. Both halves matter: a broker that acked
// once and then deferred forever would satisfy the first alone.
func testQ2AffClusterRefused(l *testQ2AffMoveLog, subject string) bool {
	var deferred bool
	for _, res := range l.results(subject) {
		switch res {
		case mqttQ2AffMoveDeferred:
			deferred = true
		default:
			return false
		}
	}
	return deferred
}

// TestQ2AffineOwnerBrokerLossKeepsAssignment removes the owner's BROKER, not just
// its connection. Nothing in memory survives that, so the only thing that can
// stop the surviving broker's member from taking the message is the durable
// claim — and the only thing that can still deliver it to its owner is the
// generation-scoped consumer named in that claim.
//
// Gated, because it stops a broker mid-test and depends on cluster timing.
func TestQ2AffineOwnerBrokerLossKeepsAssignment(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 60 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	const group, topic = "lossg", "q2aff/loss"
	ca, ra := testQ2AffClusterMember(t, bA, "lossA", group, topic)

	// The member on bA is the SOLE member while the delivery under test is started, so
	// it is necessarily the owner — and bA is the broker this test stops. The previous
	// version subscribed both up front and SKIPPED whenever the group chose the other
	// member, so even an explicit invocation was not guaranteed to exercise anything.
	//
	// Two exchanges are completed first, so the session's packet-identifier allocator has
	// moved past 1 before the delivery that is left unfinished. Without that the
	// continuity assertion at the end would hold even if nothing were restored, because a
	// fresh allocator produces 1 as well.
	for i, payload := range []string{"burn-1", "burn-2"} {
		testQ2AffClusterPublish(t, bA, fmt.Sprintf("loss-burn-%d", i), topic, payload)
		_, got, qos, pi, ok := testMQTTTryReadPublishV5(ra, testMQTTTimeout)
		if !ok || qos != 2 || string(got) != payload {
			t.Fatalf("burn-in delivery %d: ok=%v qos=%d payload=%q", i, ok, qos, got)
		}
		testQ2AffClusterComplete(t, ca, ra, pi)
	}

	testQ2AffClusterPublish(t, bA, "loss-pub", topic, "brokerloss")
	_, _, _, firstPI, ok := testMQTTTryReadPublishV5(ra, testMQTTTimeout)
	if !ok {
		ca.Close()
		t.Fatal("the sole group member did not receive the shared QoS 2 delivery it must own")
	}
	if firstPI <= 1 {
		// The same guard, for the same reason, as
		// TestMQTTInflightQoS2IdentifierSurvivesRestartBeforePubRec: at 1 the comparison
		// after the failover cannot distinguish a restored identifier from a fresh one.
		t.Fatalf("packet identifier %d is what a fresh allocator would produce; the continuity assertion below would prove nothing", firstPI)
	}

	// The second member joins only NOW, on the broker that survives. It is a group
	// member while the delivery is in flight, which is what makes "never offered to
	// another member" meaningful, without having competed for ownership.
	cb, rb := testQ2AffClusterMember(t, bB, "lossB", group, topic)
	defer cb.Close()

	// Exactly one assignment: the two burn-in exchanges completed, so their claims are
	// reaped. Polled, because the reap is the last step of the affine completion.
	checkFor(t, testMQTTTimeout, 100*time.Millisecond, func() error {
		if n := len(testQ2AffClusterClaims(t, bB)); n != 1 {
			return fmt.Errorf("expected exactly one assignment visible from the surviving broker, got %d\n%s", n, c.describe())
		}
		return nil
	})

	// Lose the owner's broker outright: connection, session memory and all.
	ca.Close()
	<-bA.s.BeginShutdown()

	// The surviving broker's member must not be offered the started delivery.
	if _, pl, _, _, got := testMQTTTryReadPublishV5(rb, 10*time.Second); got {
		t.Fatalf("losing the owner's broker released a started QoS 2 delivery to another member (payload %q)", pl)
	}

	// The owner reconnects through the SURVIVING broker and collects it — with the SAME
	// packet identifier. Continuity is no longer a deviation on any outbound path: the
	// identifier is durable in the in-flight register on $MQTT_inflight and restored
	// before the redelivery. This assertion used to permit a fresh identifier, which left
	// the affine path's continuity unverified while the docs claimed it.
	//
	// It discriminates because of the burn-in above: firstPI is > 1, so a broker that
	// allocated a fresh identifier instead of restoring the register's would fail here.
	// TestMQTTInflightQoS2IdentifierSurvivesRestartBeforePubRec covers the same guarantee
	// across a LOCAL restart of one broker; what this adds is the cross-broker route —
	// the register is read on a node that never held the delivery.
	cr, rr := testQ2AffClusterResume(t, bB, "lossA")
	defer cr.Close()
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(rr, testMQTTTimeout)
	if !ok {
		t.Fatal("the message was lost when its owner's broker was stopped")
	}
	if qos != 2 || string(payload) != "brokerloss" {
		t.Fatalf("unexpected delivery after broker loss: payload=%q qos=%d", payload, qos)
	}
	if pi != firstPI {
		t.Fatalf("the redelivery after broker loss carries packet identifier %d, want the original %d: the in-flight register did not restore it",
			pi, firstPI)
	}
	testQ2AffClusterComplete(t, cr, rr, pi)
	testMQTTExpectNothing(t, rb)
}
