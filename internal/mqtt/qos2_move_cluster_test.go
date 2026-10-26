//go:build mqtt_fulltests

// Replicated-JetStream (R3) tests for the inbound QoS 2 move protocol, running
// against a genuine 3-node embedded cluster (see jetstream_cluster_harness_test.go).
//
// These exist because the property the move protocol claims is a DURABILITY
// property, and durability claims cannot be established by a hook that fails
// before its request is issued. Only a real cluster can produce the ambiguous
// outcomes the protocol was designed for: a commit whose response is lost, a
// stream-leader change between two delivery attempts, a node that dies after a
// delete committed.
//
// What they establish, precisely:
//
//   - TestQoS2MoveReplicatedStream (always-run): $MQTT_qos2in really is Replicas=3,
//     current on every node, and carries the move-protocol configuration.
//   - The gated tests (MQTTNB_CLUSTER_FAILOVER_TESTS=1) cover the terminal state
//     surviving a node loss, a committed delete followed by a node death, a
//     superseding generation across brokers, deduplication across a leader change,
//     and two concurrent PUBRELs for one generation.
//
// The guarantee these tests support, stated as the protocol states it: no silent
// loss on any PUBREL path; once the terminal state commits, suppression of
// re-delivery is indefinite and independent of the deduplication window and of
// both processes' lifetimes; across the unresolved interval between the onward
// store and that commit the durable path is exactly-once only while deduplication
// survives, otherwise at-least-once. Deduplication surviving a stream-leader
// change is asserted below; it does NOT extend to a nats-server process restart,
// which is residual 2 and is deliberately not claimed anywhere here.

package mqtt

import (
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// testQoS2ClusterPub connects a QoS 2 publisher to a cluster broker with a session
// that survives a broker-forced close (see connectPublisher in qos2_move_test.go
// for why the Session Expiry Interval is load-bearing rather than incidental).
func testQoS2ClusterPub(t *testing.T, b *testClusterBroker, clientID string) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	return c, r
}

// testQoS2ClusterSub subscribes a 3.1.1 QoS 2 witness. 3.1.1 because a v5 delivery
// prefixes a properties block every payload assertion would have to strip; the
// subscriber is a delivery counter, not a subject under test.
func testQoS2ClusterSub(t *testing.T, b *testClusterBroker, clientID, topic string) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, b.opts.MQTT.Host, b.opts.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)
	return c, r
}

// testQoS2ClusterConsume completes the subscriber's half of one QoS 2 exchange.
func testQoS2ClusterConsume(t *testing.T, c net.Conn, r *mqttReader, topic, payload string) {
	t.Helper()
	subPI := testMQTTCheckPubMsgNoAck(t, c, r, topic, mqttPubQoS2, []byte(payload))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, subPI)
}

// testQoS2FinalizeWatcher records the resolved status of every terminal commit on
// one account manager, so a test can assert what a finalize DECIDED.
//
// This exists because the losing side of a takeover race cannot be asked. A
// cross-broker takeover closes the earlier connection whatever its finalize
// concluded, so its PUBCOMP may never reach the wire — and to a reader, a stalled
// finalize is indistinguishable from a displaced one (testMQTTReadPacketReady
// reports !ok for a deadline expiry exactly as it does for a closed socket).
// Checking the broker's client registry does not separate them either: the takeover
// deregisters the client either way. Observing the status directly is the only
// evidence that is actually about the claim.
type testQoS2FinalizeWatcher struct {
	mu   sync.Mutex
	seen map[string]mqttQoS2FinalizeStatus // generation -> status
}

func testQoS2WatchFinalize(t *testing.T, asm *mqttAccountSessionManager) *testQoS2FinalizeWatcher {
	t.Helper()
	w := &testQoS2FinalizeWatcher{seen: make(map[string]mqttQoS2FinalizeStatus)}
	asm.mu.Lock()
	asm.testQoS2FinalizeDone = func(_, gen string, status mqttQoS2FinalizeStatus) {
		w.mu.Lock()
		w.seen[gen] = status
		w.mu.Unlock()
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testQoS2FinalizeDone = nil
		asm.mu.Unlock()
	})
	return w
}

// requireStatusIn waits for a finalize to resolve and asserts its status is one of
// want. A finalize that never resolves (a stall) fails here rather than being
// silently accepted.
//
// More than one status can be correct for a single race, so the expectation is a
// SET rather than a value — pinning one of several legitimate outcomes would make
// the test flaky and teach the next reader the wrong invariant. The set must still
// exclude every wrong outcome, which is what keeps it an assertion.
func (w *testQoS2FinalizeWatcher) requireStatusIn(t *testing.T, want []mqttQoS2FinalizeStatus, what string) {
	t.Helper()
	for deadline := time.Now().Add(testMQTTTimeout); time.Now().Before(deadline); {
		w.mu.Lock()
		n := len(w.seen)
		var got mqttQoS2FinalizeStatus
		var gen string
		for g, st := range w.seen {
			gen, got = g, st
		}
		w.mu.Unlock()
		if n > 0 {
			for _, ok := range want {
				if got == ok {
					t.Logf("%s: finalize for generation %q resolved as %v", what, gen, got)
					return
				}
			}
			t.Fatalf("%s: finalize for generation %q resolved as %v, want one of %v", what, gen, got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: no finalize resolved within %v — the commit stalled or was never reached, "+
		"which a silent reader would have accepted as a displaced connection", what, testMQTTTimeout)
}

// testQoS2ExpectNoLostExchangeReport drains whatever the losing connection managed
// to send and fails only on a PUBCOMP 0x92: that generation's onward store
// succeeded and its terminal state committed, so reporting the identifier as
// unknown would be a false report of a lost exchange. Silence is NOT asserted
// against here — the takeover legitimately closes the connection first, and the
// convergence claim itself is carried by the finalize watcher above.
func testQoS2ExpectNoLostExchangeReport(t *testing.T, r *mqttReader, pi uint16) {
	t.Helper()
	for {
		b, pl, ok := testMQTTReadPacketReady(r, 2*time.Second)
		if !ok {
			return
		}
		switch pt := b & mqttPacketMask; pt {
		case mqttPacketPubComp:
			// The reason byte is OPTIONAL: a success PUBCOMP uses the short form with
			// remaining length 2 and carries none. So bound the parse by the packet's
			// own remaining length, exactly as testMQTTReadPubCompV5 does — reading a
			// reason byte unconditionally would consume the fixed header of whatever
			// follows (typically the takeover's DISCONNECT, buffered in the same read)
			// and report it as a bogus reason code.
			start := r.pos
			gotPI, err := r.readUint16("pubcomp pi")
			if err != nil || gotPI != pi {
				t.Fatalf("PUBCOMP for pi=%v (err=%v), want %v", gotPI, err, pi)
			}
			reason := byte(mqttReasonSuccess)
			if r.pos-start < pl {
				if reason, err = r.readByte("pubcomp reason"); err != nil {
					t.Fatalf("reading the PUBCOMP reason: %v", err)
				}
			}
			if reason == mqttReasonPacketIDNotFound {
				t.Fatalf("the losing PUBREL reported 0x%x, but its generation committed: "+
					"a completed exchange was reported as an unknown identifier", reason)
			}
			if reason != mqttReasonSuccess {
				t.Fatalf("Unexpected PUBCOMP reason 0x%x from the losing PUBREL", reason)
			}
			return
		case mqttPacketDisconnect:
			return
		default:
			t.Fatalf("Unexpected packet type %v from the losing connection", pt)
		}
	}
}

// TestQoS2ExpectNoLostExchangeReportParsesShortPubComp covers the parse itself,
// because the cluster test above cannot: broker A is displaced by the takeover on
// every observed run, so the helper always takes the DISCONNECT branch and its
// PUBCOMP path is never reached there. The hazard is real but latent — it fires
// only if timing ever lets A's PUBCOMP out ahead of its DISCONNECT — so it is
// pinned here rather than left to a race that currently never occurs.
//
// A success PUBCOMP is the SHORT form (remaining length 2, no reason byte). Reading
// a reason unconditionally would consume the fixed header of whatever follows and
// report it as a bogus reason code; with a DISCONNECT (0xE0) buffered behind it,
// that byte would be read as reason 0xE0 and fail the test for no reason.
func TestQoS2ExpectNoLostExchangeReportParsesShortPubComp(t *testing.T) {
	const pi = uint16(7)
	for _, tc := range []struct {
		name  string
		write func(w *mqttWriter)
	}{
		{
			name: "short-form PUBCOMP followed by DISCONNECT in one read",
			write: func(w *mqttWriter) {
				w.WriteByte(mqttPacketPubComp)
				w.WriteByte(2) // remaining length: PI only, no reason byte
				w.WriteUint16(pi)
				w.WriteByte(mqttPacketDisconnect)
				w.WriteByte(0)
			},
		},
		{
			name: "long-form PUBCOMP with an explicit success reason",
			write: func(w *mqttWriter) {
				w.WriteByte(mqttPacketPubComp)
				w.WriteByte(3)
				w.WriteUint16(pi)
				w.WriteByte(mqttReasonSuccess)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := net.Pipe()
			defer cli.Close()
			defer srv.Close()

			w := newMQTTWriter(0)
			tc.write(w)
			go func() {
				_, _ = srv.Write(w.Bytes())
			}()

			// A false failure here would be a t.Fatal from inside the helper.
			testQoS2ExpectNoLostExchangeReport(t, &mqttReader{reader: cli}, pi)
		})
	}
}

// TestQoS2MoveReplicatedStream asserts $MQTT_qos2in is created R3 on a real
// cluster, is current on every node, and satisfies the move protocol's own
// validator — the same check that fails startup for a non-conforming stream.
// Always-run: this is the discriminator between real replicated hold state and a
// single-node store.
func TestQoS2MoveReplicatedStream(t *testing.T) {
	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)

	js, err := b.nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		si, err := js.StreamInfo(mqttQoS2IncomingMsgsStreamName)
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
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)

	// The configuration the state machine depends on, checked through the
	// production validator rather than a restatement of it.
	asm := testMQTTGetGlobalASM(t, b.s)
	si, err := asm.jsa.lookupStream(mqttQoS2IncomingMsgsStreamName)
	if err != nil || si == nil {
		t.Fatalf("lookup %s: si=%v err=%v", mqttQoS2IncomingMsgsStreamName, si, err)
	}
	if verr := validateQoS2InStream(si, 3, asm.sessStreamEpoch); verr != nil {
		t.Fatalf("the R3 stream the broker created does not satisfy its own validator: %v", verr)
	}

	// A committed move's terminal record must be replicated too, not just the held
	// one: the terminal state is what a surviving broker reads after a failover.
	cs, rs := testQoS2ClusterSub(t, b, "q2rep-sub", "q2rep/topic")
	defer cs.Close()
	cp, rp := testQoS2ClusterPub(t, b, "q2rep-pub")
	defer cp.Close()

	pi := uint16(51)
	testMQTTPublishV5(t, cp, 2, pi, "q2rep/topic", []byte("replicated"))
	testMQTTReadPubRecV5(t, rp, pi)
	subject := testMQTTGetClient(t, b.s, "q2rep-pub").mqttQoS2InternalSubject(pi)
	if rec, _, err := asm.jsa.loadQoS2Record(subject); err != nil || rec == nil || rec.state != mqttQoS2Held {
		t.Fatalf("Expected a held record on the R3 stream: rec=%v err=%v", rec, err)
	}
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)

	testQoS2SendPubRel(t, cp, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonSuccess, reason)
	}
	testQoS2ClusterConsume(t, cs, rs, "q2rep/topic", "replicated")
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)
}

// TestQoS2MoveSurvivesBrokerRestartAfterFinalize is what the roadmap item is
// asking for: a terminal state committed before a node is lost must still suppress
// re-delivery afterwards. Terminal cleanup is blocked so the tombstone survives,
// the stream-leader node and the originating broker are stopped, and the publisher
// retries its PUBREL through a surviving broker.
//
// The claim is precisely "the terminal state, not a deduplication window, is what
// suppresses the second delivery": the window would also have suppressed the
// stored copy here, so the assertion that matters is PUBCOMP 0x00 with the
// subscriber having received exactly one message.
func TestQoS2MoveSurvivesBrokerRestartAfterFinalize(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	const topic = "q2fail/topic"
	cs, rs := testQoS2ClusterSub(t, bB, "q2fail-sub", topic)
	defer cs.Close()

	// Block terminal cleanup so the tombstone is still there when A goes away.
	restore := setMQTTTestDeleteMsgHook(func(stream string, _ uint64) error {
		if stream == mqttQoS2IncomingMsgsStreamName {
			return fmt.Errorf("terminal cleanup blocked by test")
		}
		return nil
	})
	defer restore()

	cpA, rpA := testQoS2ClusterPub(t, bA, "q2fail-pub")
	pi := uint16(52)
	testMQTTPublishV5(t, cpA, 2, pi, topic, []byte("survivor"))
	testMQTTReadPubRecV5(t, rpA, pi)
	subject := testMQTTGetClient(t, bA.s, "q2fail-pub").mqttQoS2InternalSubject(pi)

	testQoS2SendPubRel(t, cpA, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rpA, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonSuccess, reason)
	}
	testQoS2ClusterConsume(t, cs, rs, topic, "survivor")

	// The tombstone is durable and replicated before anything is stopped.
	asmA := testMQTTGetGlobalASM(t, bA.s)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		rec, _, err := asmA.jsa.loadQoS2Record(subject)
		if err != nil {
			return err
		}
		if rec == nil || rec.state != mqttQoS2Done {
			return fmt.Errorf("want a terminal record, got %v\n%s", rec, c.describe())
		}
		return nil
	})
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)

	// Stop the hold stream's leader (quorum preserved) and the originating broker.
	leader := c.waitStreamLeader(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)
	t.Logf("stopping %s stream-leader node %q", mqttQoS2IncomingMsgsStreamName, leader.name)
	c.stopNode(leader)
	cpA.Close()
	<-bA.s.BeginShutdown()

	c.waitStreamLeader(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		if !bB.nc.IsConnected() {
			return fmt.Errorf("broker B not connected after failover\n%s", c.describe())
		}
		return nil
	})

	// The publisher reconnects to the surviving broker and retries the PUBREL it
	// believes was never answered.
	cpB, rpB := testQoS2ClusterPub(t, bB, "q2fail-pub")
	defer cpB.Close()
	testQoS2SendPubRel(t, cpB, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rpB, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected the terminal state to answer 0x%x after failover, got 0x%x", mqttReasonSuccess, reason)
	}
	// Exactly one delivery, across a window long enough that a late re-delivery
	// would have shown up.
	testMQTTExpectNothingFor(t, rs, 5*time.Second)
}

// TestQoS2MoveDeleteCommittedThenNodeDiesAnswers92 pins the honest outcome of the
// one window the protocol does NOT close: terminal cleanup commits, and the node
// dies before the client learns anything. The retry observes ABSENCE, which is
// indistinguishable from never-existed, so it is answered 0x92.
//
// The property under test is no loss and no duplicate — the subscriber received
// the message exactly once. The reason code is a consequence, not the claim: an
// earlier draft of this test described the same setup as "answers from the
// terminal state", which is self-contradictory, since cleanup has just deleted
// that state.
func TestQoS2MoveDeleteCommittedThenNodeDiesAnswers92(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	const topic = "q2gone/topic"
	cs, rs := testQoS2ClusterSub(t, bB, "q2gone-sub", topic)
	defer cs.Close()

	cpA, rpA := testQoS2ClusterPub(t, bA, "q2gone-pub")
	pi := uint16(53)
	testMQTTPublishV5(t, cpA, 2, pi, topic, []byte("delivered-once"))
	testMQTTReadPubRecV5(t, rpA, pi)
	subject := testMQTTGetClient(t, bA.s, "q2gone-pub").mqttQoS2InternalSubject(pi)

	testQoS2SendPubRel(t, cpA, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rpA, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonSuccess, reason)
	}
	testQoS2ClusterConsume(t, cs, rs, topic, "delivered-once")

	// Wait for terminal cleanup to actually commit — this test is about what
	// happens AFTER it does, so a race here would silently turn it into the
	// tombstone test above.
	asmA := testMQTTGetGlobalASM(t, bA.s)
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		rec, _, err := asmA.jsa.loadQoS2Record(subject)
		if err != nil {
			return err
		}
		if rec != nil {
			return fmt.Errorf("cleanup has not committed yet (%s record)\n%s", rec.state, c.describe())
		}
		return nil
	})
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)

	leader := c.waitStreamLeader(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)
	c.stopNode(leader)
	cpA.Close()
	<-bA.s.BeginShutdown()

	c.waitStreamLeader(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)
	c.waitStreamCurrent(t, testGlobalAccount, mqttQoS2IncomingMsgsStreamName)
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		if !bB.nc.IsConnected() {
			return fmt.Errorf("broker B not connected after failover\n%s", c.describe())
		}
		return nil
	})

	cpB, rpB := testQoS2ClusterPub(t, bB, "q2gone-pub")
	defer cpB.Close()
	testQoS2SendPubRel(t, cpB, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rpB, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("Expected 0x%x for an absent record, got 0x%x", mqttReasonPacketIDNotFound, reason)
	}
	// The claim that matters: exactly one delivery, no duplicate.
	testMQTTExpectNothingFor(t, rs, 5*time.Second)
}

// TestQoS2MoveSupersededAcrossNodes: broker A's finalize finds a generation that
// broker B put there, abandons without mutating, and B's own exchange completes
// untouched. Each payload is delivered exactly once. This is the cross-node form
// of the regression that rejected the first design.
func TestQoS2MoveSupersededAcrossNodes(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	const topic = "q2sup/topic"
	cs, rs := testQoS2ClusterSub(t, bB, "q2sup-sub", topic)
	defer cs.Close()

	cpA, rpA := testQoS2ClusterPub(t, bA, "q2sup-pub")
	defer cpA.Close()
	pi := uint16(54)
	testMQTTPublishV5(t, cpA, 2, pi, topic, []byte("first"))
	testMQTTReadPubRecV5(t, rpA, pi)
	subject := testMQTTGetClient(t, bA.s, "q2sup-pub").mqttQoS2InternalSubject(pi)

	// In the load..commit window on A, replace the record from broker B's JSA with
	// a genuinely new generation carrying an undelivered payload.
	asmA := testMQTTGetGlobalASM(t, bA.s)
	asmB := testMQTTGetGlobalASM(t, bB.s)
	var replacedGen string
	done := make(chan struct{})
	asmA.mu.Lock()
	asmA.testQoS2PreFinalize = func(gotSubject, gen string, seq uint64) error {
		if gotSubject != subject {
			return nil
		}
		select {
		case <-done:
			return nil // once only
		default:
		}
		close(done)
		if err := asmB.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, seq, true); err != nil {
			return fmt.Errorf("test seam could not purge from broker B: %w", err)
		}
		replacedGen = nuid.Next()
		subj, cerr := mqttTopicToNATSPubSubject([]byte(topic))
		if cerr != nil {
			return cerr
		}
		pp := &mqttPublish{topic: []byte(topic), subject: subj, msg: []byte("second"), sz: len("second"), flags: 2 << 1}
		msg, hdrLen := mqttNewDeliverableMessage(pp, true, 0, _EMPTY_, false, _EMPTY_, replacedGen)
		if _, err := asmB.jsa.storeQoS2HoldCAS(subject, 0, hdrLen, msg); err != nil {
			return fmt.Errorf("test seam could not store broker B's record: %w", err)
		}
		return nil
	}
	asmA.mu.Unlock()
	defer func() {
		asmA.mu.Lock()
		asmA.testQoS2PreFinalize = nil
		asmA.mu.Unlock()
	}()

	testQoS2SendPubRel(t, cpA, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rpA, pi); reason != mqttReasonPacketIDNotFound {
		t.Fatalf("Expected 0x%x for a superseded finalize, got 0x%x", mqttReasonPacketIDNotFound, reason)
	}

	// B's record was NOT mutated.
	rec, _, err := asmB.jsa.loadQoS2Record(subject)
	if err != nil || rec == nil {
		t.Fatalf("Expected broker B's record to survive: rec=%v err=%v\n%s", rec, err, c.describe())
	}
	if rec.state != mqttQoS2Held || rec.gen != replacedGen {
		t.Fatalf("Broker B's record was mutated: state=%v gen=%q (want held/%q)\n%s",
			rec.state, rec.gen, replacedGen, c.describe())
	}

	// Both payloads delivered exactly once: A's by its own onward store (which
	// preceded the commit), B's by its PUBREL.
	testQoS2ClusterConsume(t, cs, rs, topic, "first")
	testQoS2SendPubRel(t, cpA, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rpA, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected the superseding generation's PUBREL to succeed, got 0x%x", reason)
	}
	testQoS2ClusterConsume(t, cs, rs, topic, "second")
	testMQTTExpectNothingFor(t, rs, 3*time.Second)
}

// TestQoS2MoveDedupSurvivesLeaderChange: two onward stores of one generation,
// separated by a $MQTT_msgs stream-leader change, must still produce a single
// stored copy. Followers populate the deduplication map on apply, so the new
// leader knows the message id.
//
// This does NOT extend to a nats-server process restart: the map is then rebuilt
// only from messages still in the store, and $MQTT_msgs is interest-retention, so
// an already-acked copy is gone. That is residual 2 in qos2_move.go, and nothing
// here should be read as claiming otherwise.
func TestQoS2MoveDedupSurvivesLeaderChange(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	const topic = "q2dedup/topic"
	cs, rs := testQoS2ClusterSub(t, bB, "q2dedup-sub", topic)
	defer cs.Close()

	// Force a second delivery attempt by failing the first terminal commit.
	asmA := testMQTTGetGlobalASM(t, bA.s)
	asmA.mu.Lock()
	asmA.testQoS2FinalizeStore = func(subject, gen string, expectSeq uint64) (uint64, error) {
		return 0, fmt.Errorf("terminal commit blocked by test")
	}
	asmA.mu.Unlock()

	cpA, rpA := testQoS2ClusterPub(t, bA, "q2dedup-pub")
	pi := uint16(55)
	testMQTTPublishV5(t, cpA, 2, pi, topic, []byte("dedup"))
	testMQTTReadPubRecV5(t, rpA, pi)

	testQoS2SendPubRel(t, cpA, pi)
	// The commit failed, so the exchange is unfinished and the connection closed.
	for {
		bt, _, ok := testMQTTReadPacketReady(rpA, testMQTTTimeout)
		if !ok {
			break
		}
		if pt := bt & mqttPacketMask; pt == mqttPacketPubComp {
			t.Fatal("Expected no PUBCOMP after a blocked terminal commit")
		} else if pt == mqttPacketDisconnect {
			break
		}
	}
	cpA.Close()

	// The first onward store is durable and replicated before the leader moves.
	c.waitStreamCurrent(t, testGlobalAccount, mqttStreamName)
	msgsLeader := c.waitStreamLeader(t, testGlobalAccount, mqttStreamName)
	t.Logf("stopping %s stream-leader node %q", mqttStreamName, msgsLeader.name)
	c.stopNode(msgsLeader)
	c.waitStreamLeader(t, testGlobalAccount, mqttStreamName)
	c.waitStreamCurrent(t, testGlobalAccount, mqttStreamName)
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		if !bB.nc.IsConnected() {
			return fmt.Errorf("broker B not connected after failover\n%s", c.describe())
		}
		return nil
	})

	// Retry through broker B: same generation, therefore the same message id, so
	// the new leader must suppress the second stored copy.
	cpB, rpB := testQoS2ClusterPub(t, bB, "q2dedup-pub")
	defer cpB.Close()
	testQoS2SendPubRel(t, cpB, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rpB, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected the retry to succeed, got 0x%x", reason)
	}

	// Exactly one stored copy reached the QoS 2 subscriber.
	testQoS2ClusterConsume(t, cs, rs, topic, "dedup")
	testMQTTExpectNothingFor(t, rs, 5*time.Second)
}

// TestQoS2MoveConcurrentPubRelSameGeneration: the same client ID taking over on
// another broker while a PUBREL is in flight. Both PUBRELs load the same
// generation, both fan out (irreducible), both store with the SAME message id so
// one is suppressed, one wins the commit, and the loser converges — either finding
// the winner's tombstone under its own generation (committed) or finding the
// subject already reaped (gone), both of which mean the exchange completed. What it
// must never do is fail to recognise its own generation. The subscriber sees
// exactly one message.
//
// Note what is NOT claimed: the loser's client is not guaranteed to be answered.
// A cross-broker takeover displaces the earlier connection, so in practice broker
// A is disconnected before its PUBCOMP can reach the client — which is the
// takeover working as designed, not the move protocol failing.
//
// The convergence claim is therefore asserted against the finalize status itself
// (testQoS2FinalizeWatcher.requireStatusIn), not against the wire: to a reader, a
// stalled finalizer and a takeover-closed socket are indistinguishable, and the
// broker's client registry does not separate them either because the takeover
// deregisters the client whichever happened. testQoS2ExpectNoLostExchangeReport is
// only a secondary check that whatever A did manage to send was not a false report
// of a lost exchange.
func TestQoS2MoveConcurrentPubRelSameGeneration(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)

	const topic = "q2conc/topic"
	cs, rs := testQoS2ClusterSub(t, bB, "q2conc-sub", topic)
	defer cs.Close()

	cpA, rpA := testQoS2ClusterPub(t, bA, "q2conc-pub")
	defer cpA.Close()
	pi := uint16(56)
	testMQTTPublishV5(t, cpA, 2, pi, topic, []byte("concurrent"))
	testMQTTReadPubRecV5(t, rpA, pi)
	subject := testMQTTGetClient(t, bA.s, "q2conc-pub").mqttQoS2InternalSubject(pi)

	// Park A inside the load..commit window, take the session over on B, and let B
	// run its own PUBREL for the SAME generation before releasing A.
	asmA := testMQTTGetGlobalASM(t, bA.s)
	watcher := testQoS2WatchFinalize(t, asmA)
	release := make(chan struct{})
	parked := make(chan struct{})
	var once bool
	asmA.mu.Lock()
	asmA.testQoS2PreFinalize = func(gotSubject, gen string, seq uint64) error {
		if gotSubject != subject || once {
			return nil
		}
		once = true
		close(parked)
		<-release
		return nil
	}
	asmA.mu.Unlock()
	defer func() {
		asmA.mu.Lock()
		asmA.testQoS2PreFinalize = nil
		asmA.mu.Unlock()
	}()

	// Written inline rather than through testQoS2SendPubRel: that helper Fatalfs,
	// which is illegal off the test goroutine.
	go func() {
		pkt := [4]byte{mqttPacketPubRel | 0x2, 0x2, byte(pi >> 8), byte(pi)}
		if _, err := testMQTTWrite(cpA, pkt[:]); err != nil {
			t.Logf("writing broker A's PUBREL: %v", err)
		}
	}()
	select {
	case <-parked:
	case <-time.After(testMQTTTimeout):
		t.Fatalf("broker A never reached the pre-commit window\n%s", c.describe())
	}

	// The takeover: same client ID on broker B, resuming the session, then its own
	// PUBREL for the identifier A is still working on.
	cpB, rpB := testQoS2ClusterPub(t, bB, "q2conc-pub")
	defer cpB.Close()
	testQoS2SendPubRel(t, cpB, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rpB, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected broker B's PUBREL to succeed, got 0x%x", reason)
	}

	// Now let A finish. Its CAS loses, its re-read finds the terminal state under
	// its OWN generation, and it reports committed rather than superseded.
	close(release)

	// The convergence claim, asserted where the evidence actually is: A's finalize
	// must RESOLVE, and resolve to a convergent outcome. Two are legitimate and which
	// one occurs is a genuine race:
	//
	//   - Committed: A's re-read found B's tombstone under A's own generation.
	//   - Gone: B had already reaped that tombstone (invariant 4 makes the terminal
	//     delete asynchronous), so A found the subject empty — and since A's own
	//     onward store succeeded, the exchange is complete either way.
	//
	// Superseded would mean A failed to recognise its own generation, and
	// Indeterminate would mean it never resolved at all; both are rejected. A stalled
	// finalize fails here too, which is the point: to a reader, a stall and a
	// takeover-closed socket are indistinguishable.
	watcher.requireStatusIn(t,
		[]mqttQoS2FinalizeStatus{mqttQoS2FinalizeCommitted, mqttQoS2FinalizeGone},
		"broker A's losing PUBREL")

	// Whatever A managed to put on the wire before the takeover closed it must not
	// be a false report of a lost exchange.
	testQoS2ExpectNoLostExchangeReport(t, rpA, pi)

	// One stored copy despite two onward stores: same generation, same message id.
	testQoS2ClusterConsume(t, cs, rs, topic, "concurrent")
	testMQTTExpectNothingFor(t, rs, 5*time.Second)

	// The record converged to absent (or at worst an inert tombstone).
	asmB := testMQTTGetGlobalASM(t, bB.s)
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		rec, _, err := asmB.jsa.loadQoS2Record(subject)
		if err != nil {
			return err
		}
		if rec != nil && rec.state == mqttQoS2Held {
			return fmt.Errorf("record is still held after both PUBRELs\n%s", c.describe())
		}
		return nil
	})
}

// testQoS2WatchClaim records the resolved status of every release claim on one
// account manager, keyed by hold subject.
//
// It exists for the same reason testQoS2FinalizeWatcher does: the losing side of
// a cross-broker discard cannot be asked what it decided. The takeover closes its
// connection regardless, so its PUBCOMP may never reach the wire — and a releaser
// that correctly declines to deliver sends nothing at all, which is
// indistinguishable from a stalled one to a reader.
type testQoS2ClaimWatcher struct {
	mu   sync.Mutex
	seen map[string]mqttQoS2ClaimStatus
}

func testQoS2WatchClaim(t *testing.T, asm *mqttAccountSessionManager) *testQoS2ClaimWatcher {
	t.Helper()
	w := &testQoS2ClaimWatcher{seen: make(map[string]mqttQoS2ClaimStatus)}
	asm.mu.Lock()
	asm.testQoS2ClaimDone = func(holdSubject, _ string, status mqttQoS2ClaimStatus) {
		w.mu.Lock()
		w.seen[holdSubject] = status
		w.mu.Unlock()
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testQoS2ClaimDone = nil
		asm.mu.Unlock()
	})
	return w
}

// requireStatus waits for the claim on subject to resolve and asserts its status.
// A claim that never resolves fails here rather than being silently accepted as a
// displaced connection.
func (w *testQoS2ClaimWatcher) requireStatus(t *testing.T, subject string, want mqttQoS2ClaimStatus, what string) {
	t.Helper()
	for deadline := time.Now().Add(testMQTTTimeout); time.Now().Before(deadline); {
		w.mu.Lock()
		got, ok := w.seen[subject]
		w.mu.Unlock()
		if ok {
			if got != want {
				t.Fatalf("%s: claim on %q resolved as %v, want %v", what, subject, got, want)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: no claim resolved for %q within %v — it stalled or was never attempted, "+
		"which a silent reader would have accepted as a displaced connection", what, subject, testMQTTTimeout)
}

// TestQoS2ReleaseClusterDiscardBeatsRelease is the cross-broker form of the
// behavioral pin. Broker A is parked between loading the held record and claiming
// the release; broker B takes the session over with a clean start, which discards
// it. When A resumes, its claim must resolve Discarded and it must deliver
// nothing.
//
// A's own connection is closed by the takeover, so the verdict is read through the
// claim watcher rather than off the wire — the wire carries no evidence either way.
func TestQoS2ReleaseClusterDiscardBeatsRelease(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	cl := testNewJetStreamCluster(t, 3)
	bA := cl.startBroker(t, 0)
	bB := cl.startBroker(t, 1)

	const topic = "q2relclaim/topic"
	const clientID = "q2relclaim-pub"
	cs, rs := testQoS2ClusterSub(t, bB, "q2relclaim-sub", topic)
	defer cs.Close()

	cpA, rpA := testQoS2ClusterPub(t, bA, clientID)
	defer cpA.Close()
	pi := uint16(61)
	testMQTTPublishV5(t, cpA, 2, pi, topic, []byte("discarded-across-brokers"))
	testMQTTReadPubRecV5(t, rpA, pi)
	subject := testMQTTGetClient(t, bA.s, clientID).mqttQoS2InternalSubject(pi)

	asmA := testMQTTGetGlobalASM(t, bA.s)
	watcher := testQoS2WatchClaim(t, asmA)

	// Park A between its load and its claim, and let B discard the session there.
	// The takeover has to complete before A's claim runs, or the race under test is
	// not the one exercised — so the seam blocks on it rather than merely starting it.
	discarded := make(chan struct{})
	asmA.mu.Lock()
	asmA.testQoS2PreClaim = func(holdSubject, _ string, _ uint64) error {
		if holdSubject != subject {
			return nil
		}
		select {
		case <-discarded:
		case <-time.After(testMQTTTimeout):
			t.Errorf("the takeover never completed; the race under test was not exercised")
		}
		return nil
	}
	asmA.mu.Unlock()
	t.Cleanup(func() {
		asmA.mu.Lock()
		asmA.testQoS2PreClaim = nil
		asmA.mu.Unlock()
	})

	go func() {
		testQoS2SendPubRel(t, cpA, pi)
	}()

	// B takes the session over with a clean start, which discards it: the discard
	// claims each release before destroying its hold, so A's later claim must lose.
	cpB, rpB := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: true,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, bB.opts.MQTT.Host, bB.opts.MQTT.Port)
	defer cpB.Close()
	testMQTTReadConnAckV5(t, rpB)
	close(discarded)

	watcher.requireStatus(t, subject, mqttQoS2ClaimDiscarded, "release racing a cross-broker discard")

	// The message was NOT delivered out of the discarded session. This is the whole
	// point: before the claim, A would have fanned out here and read the resulting
	// absence as proof the exchange completed.
	testMQTTExpectNothingFor(t, rs, 3*time.Second)

	// And the discard marker survives, so a still later releaser would lose too.
	asmB := testMQTTGetGlobalASM(t, bB.s)
	cur, _, err := asmB.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(subject))
	if err != nil {
		t.Fatalf("load claim: %v", err)
	}
	if cur == nil || cur.State != mqttQoS2RelStateDiscarded {
		t.Fatalf("claim = %+v, want the discard marker to survive\n%s", cur, cl.describe())
	}
}

// TestQoS2ReleaseClusterReleaseBeatsDiscard is the mirror order, and pins the half
// that protects the message rather than the session: when the releaser claims
// first, a discard on another broker must NOT destroy the hold record. Its payload
// is what a retry delivers if the releasing broker dies mid-release, so destroying
// it would turn this spec deviation into silent loss.
//
// The discard waits briefly for the release to finish, so the exchange completes
// normally and exactly one delivery reaches the witness.
func TestQoS2ReleaseClusterReleaseBeatsDiscard(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	cl := testNewJetStreamCluster(t, 3)
	bA := cl.startBroker(t, 0)
	bB := cl.startBroker(t, 1)

	const topic = "q2relwin/topic"
	const clientID = "q2relwin-pub"
	cs, rs := testQoS2ClusterSub(t, bB, "q2relwin-sub", topic)
	defer cs.Close()

	cpA, rpA := testQoS2ClusterPub(t, bA, clientID)
	defer cpA.Close()
	pi := uint16(62)
	testMQTTPublishV5(t, cpA, 2, pi, topic, []byte("release-wins"))
	testMQTTReadPubRecV5(t, rpA, pi)
	subject := testMQTTGetClient(t, bA.s, clientID).mqttQoS2InternalSubject(pi)

	// A claims first and completes the exchange.
	testQoS2SendPubRel(t, cpA, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rpA, pi); reason != mqttReasonSuccess {
		t.Fatalf("Expected reason 0x%x, got 0x%x", mqttReasonSuccess, reason)
	}
	testQoS2ClusterConsume(t, cs, rs, topic, "release-wins")

	// Now B discards the session. A's exchange already completed, so there is
	// nothing left to preserve and nothing more to deliver.
	cpB, rpB := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: true,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, bB.opts.MQTT.Host, bB.opts.MQTT.Port)
	defer cpB.Close()
	testMQTTReadConnAckV5(t, rpB)

	testMQTTExpectNothingFor(t, rs, 3*time.Second)

	asmB := testMQTTGetGlobalASM(t, bB.s)
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		rec, _, err := asmB.jsa.loadQoS2Record(subject)
		if err != nil {
			return err
		}
		if rec != nil && rec.state == mqttQoS2Held {
			return fmt.Errorf("a completed exchange left a held record behind\n%s", cl.describe())
		}
		return nil
	})
}
