//go:build mqttport_fulltests

package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// mqttTopicFor turns an internal NATS subject prefix into the MQTT topic filter
// a client would use to reach it, e.g. "$MQTT.msgs." -> "$MQTT/msgs/#".
func mqttTopicFor(prefix string) string {
	return strings.ReplaceAll(prefix, ".", "/") + "#"
}

// reservedPrefixes is every subject prefix inside the broker's reserved
// namespace that carries real traffic. Kept as one list so a new internal
// prefix is a visible omission here rather than a silent hole.
//
// Five entries below were added after that intent had already been missed once
// each: teardown, qos2 assign, inflight, affine delivery and the interval
// register were introduced with their features and never registered here. The
// guards deny "$MQTT.>" wholesale rather than enumerating prefixes, so none of
// them was ever a live hole — but a list whose whole purpose is to make an
// omission visible only does that if it is complete. The two affine prefixes
// under "$MQTT.out." are covered by the outbound entry's own wildcard.
var reservedPrefixes = map[string]string{
	"deliver subject":   mqttSubPrefix,
	"pubrel delivery":   mqttPubRelDeliverySubjectPrefix,
	"affine delivery":   mqttQ2AffineDeliverPrefix,
	"shared deliver":    mqttSharedSubDeliverPrefix,
	"sessions":          mqttSessStreamSubjectPrefix,
	"messages":          mqttStreamSubjectPrefix,
	"retained messages": mqttRetainedMsgsStreamSubject,
	"jsa replies":       mqttJSARepliesPrefix,
	"will queue":        mqttWillQSubjectPrefix,
	"accept facts":      mqttAcceptSubjectPrefix,
	"teardown":          mqttTeardownSubjectPrefix,
	"interval register": mqttOriginSubjectPrefix,
	"qos2 inbound":      mqttQoS2IncomingMsgsStreamSubjectPrefix,
	"qos2 assign":       mqttQoS2AssignStreamSubjectPrefix,
	"inflight":          mqttInflightStreamSubjectPrefix,
	"outbound":          mqttOutSubjectPrefix,
	"pubrel outbound":   mqttPubRelSubjectPrefix,
}

// TestMQTTDenySubscribeToReservedNamespace covers the whole "$MQTT.>" namespace,
// not just the three delivery prefixes upstream rejected. Every internal subject
// carries real traffic -- $MQTT.msgs.> is every QoS 1/2 message in the account
// and $MQTT.rmsgs.> every retained message -- so an explicit filter into any of
// them is a read-side bypass of the client's own subscribe permissions.
func TestMQTTDenySubscribeToReservedNamespace(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	pi := uint16(1)
	for name, prefix := range reservedPrefixes {
		t.Run(name, func(t *testing.T) {
			testMQTTSub(t, pi, mc, r,
				[]*mqttFilter{{filter: mqttTopicFor(prefix), qos: 1}},
				[]byte{mqttSubAckFailure})
			pi++
		})
	}

	// The bare prefix and a non-wildcard filter are denied too: the rule is the
	// namespace, not the shape of the filter.
	for _, topic := range []string{"$MQTT/msgs/foo", "$MQTT/#", "$MQTT/sess/abc"} {
		t.Run(topic, func(t *testing.T) {
			testMQTTSub(t, pi, mc, r,
				[]*mqttFilter{{filter: topic, qos: 1}},
				[]byte{mqttSubAckFailure})
			pi++
		})
	}
}

// TestMQTTReservedNamespaceNoMessageBypass is the regression that motivates the
// deny: every QoS 1/2 PUBLISH is stored with an ordinary NATS publish to
// $MQTT.msgs.<subject>, so a client allowed to subscribe there would read all of
// them regardless of its own per-topic permissions.
func TestMQTTReservedNamespaceNoMessageBypass(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// The eavesdropper subscribes to the internal messages subject.
	spy, spyR := testMQTTConnect(t, &mqttConnInfo{clientID: "spy", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer spy.Close()
	testMQTTCheckConnAck(t, spyR, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, spy, spyR,
		[]*mqttFilter{{filter: mqttTopicFor(mqttStreamSubjectPrefix), qos: 1}},
		[]byte{mqttSubAckFailure})

	// A normal client publishes a QoS 1 message, which is stored to
	// $MQTT.msgs.foo.bar on the way through.
	pub, pubR := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubR, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubR, 1, false, false, "foo/bar", 1, []byte("secret"))

	testMQTTExpectNothing(t, spyR)
}

// TestMQTTWildcardDoesNotReachReserved guards the other half of the rule: a
// wildcard subscription still works normally and still cannot see the reserved
// namespace, which is what [MQTT-4.7.2-1] requires and what made the narrow
// upstream check look unnecessary.
func TestMQTTWildcardDoesNotReachReserved(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subR := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subR, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subR, []*mqttFilter{{filter: "#", qos: 0}}, []byte{0})
	testMQTTFlush(t, sub, nil, subR)

	pub, pubR := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubR, mqttConnAckRCConnectionAccepted, false)

	// A QoS 1 publish reaches "#" as a normal delivery...
	testMQTTPublish(t, pub, pubR, 1, false, false, "foo/bar", 1, []byte("msg"))
	testMQTTCheckPubMsg(t, sub, subR, "foo/bar", 0, []byte("msg"))
	// ...and the internal store traffic it generated does not.
	testMQTTExpectNothing(t, subR)
}

// TestMQTTPublishReservedAckAsymmetry pins the documented per-version behaviour
// of the PUBLISH denial: the message is dropped in both protocol versions, but
// only v5 can say so. 3.1.1 has no reason codes, so the client sees an ordinary
// success PUBACK for a message that was thrown away.
func TestMQTTPublishReservedAckAsymmetry(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Watch the internal subject on the service connection: nothing a client
	// publishes may ever land there.
	seen := make(chan *nats.Msg, 8)
	nsub, err := s.nrc.Subscribe(mqttStreamSubjectPrefix+">", func(m *nats.Msg) {
		select {
		case seen <- m:
		default:
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = nsub.Unsubscribe() }()
	if err := s.nrc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	for _, tc := range []struct {
		name       string
		v5         bool
		wantReason byte
	}{
		{"v5 reports not authorized", true, mqttReasonNotAuthorized},
		{"3.1.1 acknowledges a dropped publish", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				c net.Conn
				r *mqttReader
			)
			if tc.v5 {
				c, r = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: tc.name, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
				if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
					t.Fatalf("CONNACK reason = 0x%02x", reason)
				}
			} else {
				c, r = testMQTTConnect(t, &mqttConnInfo{clientID: tc.name, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			}
			defer c.Close()

			// v5 PUBLISH carries a properties field; the 3.1.1 packet does not.
			if tc.v5 {
				testMQTTPublishV5(t, c, 1, 1, "$MQTT/msgs/foo", []byte("forged"))
			} else {
				testMQTTSendPublishPacket(t, c, 1, false, false, "$MQTT/msgs/foo", 1, []byte("forged"))
			}
			b, pl := testMQTTReadPacket(t, r)
			if pt := b & mqttPacketMask; pt != mqttPacketPubAck {
				t.Fatalf("expected PUBACK, got %x", pt)
			}
			start := r.pos
			if _, err := r.readUint16("packet identifier"); err != nil {
				t.Fatalf("read pi: %v", err)
			}
			// 3.1.1 PUBACK is PI only (2 bytes); v5 appends the reason code.
			var reason byte
			if pl > 2 {
				var err error
				if reason, err = r.readByte("reason code"); err != nil {
					t.Fatalf("read reason: %v", err)
				}
			}
			if reason != tc.wantReason {
				t.Errorf("PUBACK reason = 0x%02x, want 0x%02x", reason, tc.wantReason)
			}
			// Skip any properties (v5 reason string) so the reader stays aligned.
			r.pos = start + pl
		})
	}

	select {
	case m := <-seen:
		t.Fatalf("a client PUBLISH reached the internal subject %q", m.Subject)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestMQTTWillTopicReservedRejected closes the last way a client could publish
// into the reserved namespace. A Will never passes through mqttProcessPub: the
// immediate one is delivered by mqttInitiateMsgDelivery and a delayed one is
// replayed from the outbox over the service connection, where the client's own
// NATS permissions no longer apply. So the topic is rejected at CONNECT.
func TestMQTTWillTopicReservedRejected(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	forgedWill := func() *mqttWill {
		return &mqttWill{
			topic:   []byte("$MQTT/msgs/victim"),
			message: []byte("forged"),
			qos:     1,
		}
	}

	t.Run("3.1.1", func(t *testing.T) {
		c, r := testMQTTConnect(t, &mqttConnInfo{
			clientID: "will-311", cleanSess: true, will: forgedWill(),
		}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCNotAuthorized, false)
	})

	t.Run("v5", func(t *testing.T) {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
			clientID: "will-v5", cleanStart: true, will: forgedWill(),
		}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		_, reason, _ := testMQTTReadConnAckV5(t, r)
		if reason != mqttReasonNotAuthorized {
			t.Errorf("CONNACK reason = 0x%02x, want 0x%02x", reason, mqttReasonNotAuthorized)
		}
	})

	// A delayed Will is the more dangerous path: it is replayed from the outbox
	// over the service connection, where the client's own NATS permissions no
	// longer apply. Rejecting at CONNECT means no outbox record is ever created.
	t.Run("delayed v5", func(t *testing.T) {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
			clientID:   "will-delayed",
			cleanStart: true,
			will:       forgedWill(),
			willProps:  mqttV5WillDelayProps(30),
			props:      mqttV5ConnPropsSessionExpiry(60),
		}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		_, reason, _ := testMQTTReadConnAckV5(t, r)
		if reason != mqttReasonNotAuthorized {
			t.Fatalf("CONNACK reason = 0x%02x, want 0x%02x", reason, mqttReasonNotAuthorized)
		}
		// Nothing may have been enqueued for replay. Use the accepted client's
		// account manager to reach the outbox stream.
		ok, okR := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "probe", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		defer ok.Close()
		if _, reason, _ := testMQTTReadConnAckV5(t, okR); reason != mqttReasonSuccess {
			t.Fatalf("probe CONNACK reason = 0x%02x", reason)
		}
		asm := testMQTTGetAccountSessionManager(t, s, "probe")
		if _, err := asm.jsa.loadNextMsgFromSeq(mqttWillQStreamName, mqttWillQAccountFilter(asm.domainTk), 0); err == nil {
			t.Error("a delayed-Will outbox record exists; the rejected CONNECT must not have enqueued one")
		}
	})

	// A Will outside the namespace is still accepted, so the check is not simply
	// rejecting every Will.
	c, r := testMQTTConnect(t, &mqttConnInfo{
		clientID:  "will-ok",
		cleanSess: true,
		will:      &mqttWill{topic: []byte("foo/bar"), message: []byte("bye"), qos: 1},
	}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
}

// TestMQTTInternalSubsReleasedOnClientClose guards the consequence of moving the
// broker's internal subscriptions to the service connection: they are no longer
// torn down by the per-client connection closing, so closeConnection must
// unsubscribe each one explicitly or every connect/disconnect cycle leaks
// interest on the shared connection.
func TestMQTTInternalSubsReleasedOnClientClose(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// A QoS 2 subscription creates the deliver subject and the PUBREL delivery
	// subscription -- the two most likely to be left behind.
	connectAndLeave := func(id string) {
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/bar", qos: 2}}, []byte{2})
		testMQTTFlush(t, c, nil, r)
		c.Close()
	}

	// Warm up once so one-time plumbing (streams, account manager, retained
	// consumer) is established before the baseline is taken.
	connectAndLeave("warmup")
	var base int
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		base = s.nrc.NumSubscriptions()
		return nil
	})

	const cycles = 5
	for i := 0; i < cycles; i++ {
		connectAndLeave(fmt.Sprintf("churn-%d", i))
	}

	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		if n := s.nrc.NumSubscriptions(); n > base {
			return fmt.Errorf("service-connection subscriptions grew from %d to %d over %d connect/disconnect cycles", base, n, cycles)
		}
		return nil
	})
}

// testReservedSessionRecord seeds a session record carrying a subscription in
// the reserved namespace, as a broker predating the namespace deny would have
// persisted. Returns the client ID.
func testReservedSessionRecordEx(t *testing.T, s *Server, o *Options, seed func(*mqttAccountSessionManager, *mqttSession)) string {
	t.Helper()
	cid := "legacy-" + nuid.Next()

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("seed CONNACK reason = 0x%02x", reason)
	}
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	asm.mu.Lock()
	sess := asm.sessions[cid]
	asm.mu.Unlock()
	if sess == nil {
		t.Fatalf("no session for %q", cid)
	}
	// The seed creates real JetStream durables, so it runs without the session
	// lock held; only the map mutation needs it.
	sess.mu.Lock()
	seed(asm, sess)
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	testMQTTDisconnect(t, c, nil)
	c.Close()
	// The fixture is not settled until the disconnect is persisted: that save
	// stamps DisconnectedAt and moves the record's stream sequence, so a raw
	// snapshot taken ahead of it would shift under a "record unchanged" assertion.
	testWaitSessionRecordSettled(t, s, asm, cid)
	return cid
}

// testWaitSessionRecordSettled waits until the seed client is gone from the server
// AND its disconnect is visible in the persisted record. Both, because the two are
// not ordered: the client leaves s.clients on close while the disconnect-path save
// runs on a server goroutine that outlives the socket.
func testWaitSessionRecordSettled(t *testing.T, s *Server, asm *mqttAccountSessionManager, cid string) {
	t.Helper()
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		s.mu.Lock()
		for _, cl := range s.clients {
			cl.mu.Lock()
			mqttState := cl.mqtt
			var id string
			if mqttState != nil {
				id = mqttState.cid
			}
			cl.mu.Unlock()
			if id == cid {
				s.mu.Unlock()
				return fmt.Errorf("client %q is still registered", cid)
			}
		}
		s.mu.Unlock()
		if ps := testLoadPersistedSession(t, asm, cid); ps.DisconnectedAt == 0 {
			return fmt.Errorf("the disconnect of %q is not persisted yet", cid)
		}
		return nil
	})
}

// testEvictResidentSession removes a session from BOTH residency indexes, which is
// what makes the next CONNECT go through createOrRestoreSession at all:
// processConnect takes the resident path when it finds the client ID, so a fixture
// left resident never reaches the restore boundary under test. addSession and
// removeSession keep sessions and sessByHash in step, so clearing one index alone
// leaves the other holding a stale object.
func testEvictResidentSession(t *testing.T, asm *mqttAccountSessionManager, cid string) {
	t.Helper()
	asm.mu.Lock()
	delete(asm.sessions, cid)
	delete(asm.sessByHash, string(getHash(cid)))
	asm.mu.Unlock()
}

// testCreateLegacyDurable creates a REAL JetStream durable on the messages
// stream, so a refusal test can prove the durable actually SURVIVES rather than
// that a map entry stayed put — a record write and a consumer delete are
// independent, and the old in-place migration deleted these. Returns the
// ConsumerConfig to seed into the session record.
func testCreateLegacyDurable(t *testing.T, asm *mqttAccountSessionManager, durable, deliver, filter, group string) *ConsumerConfig {
	t.Helper()
	ccr := &CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			Durable:        durable,
			DeliverSubject: deliver,
			DeliverGroup:   group,
			FilterSubject:  filter,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			MaxAckPending:  1024,
		},
	}
	if _, err := asm.jsa.createDurableConsumer(ccr); err != nil {
		t.Fatalf("create durable %q: %v", durable, err)
	}
	return &ccr.Config
}

// testDurableExists reports whether a durable is still present on the messages
// stream, asked of JetStream directly rather than inferred from broker state.
func testDurableExists(t *testing.T, s *Server, durable string) bool {
	t.Helper()
	// The legacy JetStreamContext is used deliberately: these are push consumers
	// (they have a DeliverSubject), and the newer jetstream package's
	// stream.Consumer refuses to return one.
	js, err := s.nrc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	switch _, err := js.ConsumerInfo(mqttStreamName, durable); {
	case err == nil:
		return true
	case errors.Is(err, nats.ErrConsumerNotFound):
		return false
	default:
		t.Fatalf("consumer info %q: %v", durable, err)
		return false
	}
}

// testWriteLegacyPersistedSession stores a hand-built session record straight
// into $MQTT_sess, bypassing buildPersistedSessionLocked.
//
// That bypass is the point: buildPersistedSessionLocked deliberately drops every
// consumer with a DeliverGroup, so a record seeded through sess.save() can never
// contain a shared Cons entry. Reproducing an *old-format* record -- written by a
// broker that did persist them -- requires writing the JSON directly. The client
// ID is not connected, so the record is only reachable by a restore.
func testWriteLegacyPersistedSession(t *testing.T, asm *mqttAccountSessionManager, cid string, ps *mqttPersistedSession) {
	t.Helper()
	// "Legacy" here means the pre-reserved-namespace SUBSCRIPTION format on an
	// otherwise current record, which is what isolates one key shape per fixture: a
	// generationless record is refused for its generation too, so it could not tell
	// the two refusals apart. The one fixture that DOES need that shape — the
	// realistic pre-deny record, since the deny predates generations — writes it
	// through testWriteLegacyPersistedSessionRaw instead.
	if ps.Gen == _EMPTY_ {
		ps.Gen = mqttNewSessGen()
	}
	testWriteLegacyPersistedSessionRaw(t, asm, cid, ps)
}

// testWriteLegacyPersistedSessionRaw stores the record exactly as given, filling in
// only the identity fields the subject implies. No generation is minted, so this is
// the way to reproduce a record written before generation-scoped naming existed.
func testWriteLegacyPersistedSessionRaw(t *testing.T, asm *mqttAccountSessionManager, cid string, ps *mqttPersistedSession) {
	t.Helper()
	ps.ID = cid
	ps.Origin = asm.jsa.id
	b, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshal legacy session: %v", err)
	}
	if _, err := asm.jsa.storeSessionMsg(asm.domainTk, string(getHash(cid)), 0, b); err != nil {
		t.Fatalf("store legacy session for %q: %v", cid, err)
	}
}

// testLoadPersistedSessionByID reads a session record for a client ID that need
// not be connected, using a throwaway client to reach the account manager.
func testLoadPersistedSessionByID(t *testing.T, s *Server, o *Options, cid string) *mqttPersistedSession {
	t.Helper()
	probeID := "probe-load-" + nuid.Next()
	probe, probeR := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: probeID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer probe.Close()
	testMQTTReadConnAckV5(t, probeR)
	return testLoadPersistedSession(t, testMQTTGetAccountSessionManager(t, s, probeID), cid)
}

// testLoadPersistedSession reads the session record back out of the $MQTT_sess
// stream and decodes it. Asserting against this rather than against the resident
// mqttSession is what proves the record was NOT rewritten: the refusal never
// publishes a session, so the only durable evidence either way is in the stream.
// For "unchanged", prefer testLoadRawSessionMsg — decoding drops the sequence and
// normalises the JSON, so it cannot tell "not written" from "rewritten identically".
func testLoadPersistedSession(t *testing.T, asm *mqttAccountSessionManager, cid string) *mqttPersistedSession {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, string(getHash(cid)))
	if err != nil {
		t.Fatalf("load persisted session for %q: %v", cid, err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decode persisted session: %v", err)
	}
	return ps
}

// testLoadRawSessionMsg returns the session record's stream sequence and its exact
// stored bytes. Decoding drops the sequence and normalises the JSON, so only the raw
// pair can distinguish "never written" from "rewritten to the same value" — and a
// rewrite is precisely what the refusal must not do.
func testLoadRawSessionMsg(t *testing.T, asm *mqttAccountSessionManager, cid string) (uint64, []byte) {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, string(getHash(cid)))
	if err != nil {
		t.Fatalf("load raw session record for %q: %v", cid, err)
	}
	return smsg.Sequence, smsg.Data
}

// testReservedProbeASM connects a throwaway client purely to reach the account
// session manager, and keeps it connected for the subtest: the fixtures write
// records and create consumers through it, and the refused client IDs never get a
// session of their own to reach it from.
func testReservedProbeASM(t *testing.T, s *Server, o *Options) *mqttAccountSessionManager {
	t.Helper()
	probeID := "probe-reserved-" + nuid.Next()
	probe, probeR := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: probeID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { probe.Close() })
	if _, reason, _ := testMQTTReadConnAckV5(t, probeR); reason != mqttReasonSuccess {
		t.Fatalf("probe CONNACK reason = 0x%02x", reason)
	}
	return testMQTTGetAccountSessionManager(t, s, probeID)
}

// testReservedRefusedConnect sends one v5 CONNECT and requires the broker to close
// the connection without answering it.
//
// testMQTTExpectDisconnect is the deliberate oracle: testMQTTReadConnAckV5 fatals on
// EOF, so it cannot express "closed", and testExpectNoConnAck accepts a read timeout,
// which a CONNECT wedged before the validator would satisfy. This one tells a real
// EOF/reset apart from a timeout.
func testReservedRefusedConnect(t *testing.T, o *Options, cid string, cleanStart bool) {
	t.Helper()
	c, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   cid,
		cleanStart: cleanStart,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTExpectDisconnect(t, c)
}

// TestMQTTReservedRecordRefusesRestore pins the fail-closed restore boundary that
// replaced the old in-place migration. A session record carrying a subscription in
// the reserved "$MQTT.>" namespace can only have been written by a development
// broker predating the namespace deny, and there is no released persisted-state
// contract to migrate from (CONTRIBUTING.md), so the CONNECT is REFUSED and the
// operator recreates the bridge-owned state.
//
// Every row asserts the three facts a migration, a discard, or a skip-only
// regression would each break: the CONNECT gets no CONNACK, the record is
// byte-identical at the same stream sequence afterwards, and every durable the
// fixture created still exists. The rows differ in WHERE the reserved key sits,
// because the validator has to look in four maps and through two key shapes, and in
// which competing restore branch would otherwise have run first.
func TestMQTTReservedRecordRefusesRestore(t *testing.T) {
	reserved := mqttStreamSubjectPrefix + ">"
	sharedKey := "$share/g/" + reserved
	_, sharedDeliver, sharedDurable := mqttSharedNames("g", reserved)
	levelUp := reserved[:len(reserved)-2] + mqttMultiLevelSidSuffix

	for _, tc := range []struct {
		name string
		// key is the offending key the diagnostic must name. Each fixture carries
		// exactly ONE reserved key, so which one is reported does not depend on map
		// iteration order.
		key string
		// setup seeds the fixture and returns the client ID plus every durable that
		// must still exist afterwards.
		setup func(t *testing.T, s *Server, o *Options, asm *mqttAccountSessionManager, waitSweepPass func(*mqttAccountSessionManager)) (string, []string)
	}{
		{
			// Plain filter with its consumer. A plain consumer IS persisted by
			// buildPersistedSessionLocked, so seeding through the session works.
			name: "plain filter with its consumer",
			key:  reserved,
			setup: func(t *testing.T, s *Server, o *Options, asm *mqttAccountSessionManager, _ func(*mqttAccountSessionManager)) (string, []string) {
				cid := testReservedSessionRecordEx(t, s, o, func(seedASM *mqttAccountSessionManager, sess *mqttSession) {
					cc := testCreateLegacyDurable(t, seedASM, "legacy-plain", mqttSubPrefix+"legacy", reserved, _EMPTY_)
					sess.subs = map[string]byte{reserved: 1}
					sess.cons = map[string]*ConsumerConfig{reserved: cc}
				})
				testEvictResidentSession(t, asm, cid)
				return cid, []string{"legacy-plain"}
			},
		},
		{
			// Shared membership with NO consumer entry: the realistic record, since
			// buildPersistedSessionLocked never persists a consumer that has a
			// DeliverGroup. Exercises the parse-shared-key arm on SharedSubs, where
			// the key itself does not start with "$MQTT." at all.
			name: "shared membership only",
			key:  sharedKey,
			setup: func(t *testing.T, s *Server, o *Options, asm *mqttAccountSessionManager, _ func(*mqttAccountSessionManager)) (string, []string) {
				cid := testReservedSessionRecordEx(t, s, o, func(seedASM *mqttAccountSessionManager, sess *mqttSession) {
					testCreateLegacyDurable(t, seedASM, sharedDurable, sharedDeliver, reserved, "g")
					sess.sharedSubs = map[string]byte{sharedKey: 1}
				})
				testEvictResidentSession(t, asm, cid)
				return cid, []string{sharedDurable}
			},
		},
		{
			// Old-format record carrying the shared consumer entry and NO membership,
			// so the shared arm has to be applied to Cons independently: a
			// membership-only validator passes every other shared shape. The old
			// migration was driven by the memberships alone and never reached this
			// record at all, so here the durable-survival assertion discriminates
			// against the validator mutations rather than against the purge.
			name: "shared consumer entry only (old format)",
			key:  sharedKey,
			setup: func(t *testing.T, s *Server, o *Options, asm *mqttAccountSessionManager, _ func(*mqttAccountSessionManager)) (string, []string) {
				cid := "legacy-shared-cons-" + nuid.Next()
				// A real consumer, not just the JSON: a direct record write stores
				// bytes, which would make the survival assertion vacuous.
				cc := testCreateLegacyDurable(t, asm, sharedDurable, sharedDeliver, reserved, "g")
				testWriteLegacyPersistedSession(t, asm, cid, &mqttPersistedSession{
					Cons: map[string]*ConsumerConfig{sharedKey: cc},
					// Never-expire keeps the expiry sweep away entirely: it skips
					// these records, so nothing can stamp DisconnectedAt and move the
					// sequence under the "unchanged" assertion.
					ExpiryInterval: mqttSessionNeverExpire,
				})
				return cid, []string{sharedDurable}
			},
		},
		{
			// The synthetic level-up sid as the ONLY reserved key. With a Subs
			// membership present the validator could refuse without ever inspecting
			// Cons, so this row isolates both the plain-prefix arm on Cons and the
			// level-up shape ("$MQTT.msgs fwc", which is not a subject).
			name: "level-up consumer entry only",
			key:  levelUp,
			setup: func(t *testing.T, s *Server, o *Options, asm *mqttAccountSessionManager, _ func(*mqttAccountSessionManager)) (string, []string) {
				cid := "legacy-levelup-" + nuid.Next()
				idHash := string(getHash(cid))
				// Both halves of a multi-level filter's consumer pair are real; the
				// record names only the level-up one, which is what keeps the reported
				// key deterministic. Both must survive.
				testCreateLegacyDurable(t, asm, idHash+"_main", mqttSubPrefix+"main", reserved, _EMPTY_)
				fwc := testCreateLegacyDurable(t, asm, idHash+"_fwc", mqttSubPrefix+"fwc", reserved[:len(reserved)-2], _EMPTY_)
				testWriteLegacyPersistedSession(t, asm, cid, &mqttPersistedSession{
					Cons:           map[string]*ConsumerConfig{levelUp: fwc},
					ExpiryInterval: mqttSessionNeverExpire,
				})
				return cid, []string{idHash + "_main", idHash + "_fwc"}
			},
		},
		{
			// A reserved key in Drains only. No writer can produce this — drains
			// postdate the namespace deny — which is exactly why it is checked: the
			// refusal exists for records no current writer emits, and
			// restoreDrainsFromRecord derives consumer state from these keys.
			name: "drain entry only",
			key:  reserved,
			setup: func(t *testing.T, s *Server, o *Options, asm *mqttAccountSessionManager, _ func(*mqttAccountSessionManager)) (string, []string) {
				cid := "legacy-drain-" + nuid.Next()
				idHash := string(getHash(cid))
				cc := testCreateLegacyDurable(t, asm, idHash+"_drain", mqttSubPrefix+"drain", reserved, _EMPTY_)
				testWriteLegacyPersistedSession(t, asm, cid, &mqttPersistedSession{
					Drains: map[string]*mqttPersistedDrain{
						reserved: {Cons: cc, Subject: reserved, QoS: 1},
					},
					ExpiryInterval: mqttSessionNeverExpire,
				})
				return cid, []string{idHash + "_drain"}
			},
		},
		{
			// An already-overdue deadline, which pins the validator sitting BEFORE the
			// overdue-discard branch: that branch builds a transient session from the
			// record and calls clear(), deleting record-owned state before anything
			// refuses. Every other row is never-expiring, so a validator moved below
			// that branch would pass them all.
			name: "overdue deadline",
			key:  reserved,
			setup: func(t *testing.T, s *Server, o *Options, asm *mqttAccountSessionManager, waitSweepPass func(*mqttAccountSessionManager)) (string, []string) {
				// The startup sweep must be BEHIND us before an overdue record exists:
				// it arms an immediate expiry timer for one, which would delete the
				// fixture before the CONNECT reached the validator. That pass is
				// launched asynchronously with no completion signal of its own, so the
				// guard is the sweep-pass hook, not a sleep; the raised interval keeps
				// any later pass from running at all.
				waitSweepPass(asm)
				cid := "legacy-overdue-" + nuid.Next()
				idHash := string(getHash(cid))
				cc := testCreateLegacyDurable(t, asm, idHash+"_overdue", mqttSubPrefix+"overdue", reserved, _EMPTY_)
				testWriteLegacyPersistedSession(t, asm, cid, &mqttPersistedSession{
					Subs:           map[string]byte{reserved: 1},
					Cons:           map[string]*ConsumerConfig{reserved: cc},
					ExpiryInterval: 1,
					DisconnectedAt: time.Now().Add(-time.Hour).Unix(),
				})
				return cid, []string{idHash + "_overdue"}
			},
		},
		{
			// NO GENERATION — the record shape that actually existed. The namespace deny
			// (3610eb3) predates generation-scoped naming, so a broker old enough to have
			// written a reserved membership wrote no `gen` either. This row pins the
			// reserved check sitting BEFORE the generation check: with the order reversed
			// the only realistic record gets the generation refusal instead, and the
			// key-naming diagnostic this finding requires never reaches it. The
			// full-diagnostic assertion is what discriminates — the refusal itself would
			// happen either way, which is exactly why a "connection closed" oracle alone
			// would not have caught this.
			name: "no generation (the realistic pre-deny shape)",
			key:  reserved,
			setup: func(t *testing.T, s *Server, o *Options, asm *mqttAccountSessionManager, _ func(*mqttAccountSessionManager)) (string, []string) {
				cid := "legacy-nogen-" + nuid.Next()
				idHash := string(getHash(cid))
				cc := testCreateLegacyDurable(t, asm, idHash+"_nogen", mqttSubPrefix+"nogen", reserved, _EMPTY_)
				testWriteLegacyPersistedSessionRaw(t, asm, cid, &mqttPersistedSession{
					Subs:           map[string]byte{reserved: 1},
					Cons:           map[string]*ConsumerConfig{reserved: cc},
					ExpiryInterval: mqttSessionNeverExpire,
				})
				return cid, []string{idHash + "_nogen"}
			},
		},
		{
			// A CLAIMED teardown, which pins the validator sitting before the teardown
			// adoption: finishAdoptedTeardown copies Cons and Drains durable names
			// verbatim and derives shared durables from every SharedSubs key, and the
			// CONNECT path does not validate those targets. The marker is genuine (a
			// nonempty nonce is what a release matches on) and the durable is named
			// "<idHash>_..." so mqttValidateTeardownTargets would accept the record —
			// the refusal has to win against a marker the adopters would really act on.
			name: "claimed teardown",
			key:  reserved,
			setup: func(t *testing.T, s *Server, o *Options, asm *mqttAccountSessionManager, _ func(*mqttAccountSessionManager)) (string, []string) {
				cid := "legacy-teardown-" + nuid.Next()
				idHash := string(getHash(cid))
				cc := testCreateLegacyDurable(t, asm, idHash+"_teardown", mqttSubPrefix+"teardown", reserved, _EMPTY_)
				testWriteLegacyPersistedSession(t, asm, cid, &mqttPersistedSession{
					Subs: map[string]byte{reserved: 1},
					Cons: map[string]*ConsumerConfig{reserved: cc},
					Teardown: &mqttSessTeardown{
						Origin:    asm.jsa.id,
						Nonce:     nuid.Next(),
						StartedAt: time.Now().Unix(),
					},
					ExpiryInterval: mqttSessionNeverExpire,
				})
				return cid, []string{idHash + "_teardown"}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			// Only the STARTUP expiry sweep may run: a periodic pass could arm an
			// immediate timer on the overdue fixture between its write and the CONNECT.
			defer setMQTTSessionExpirySweepInterval(time.Hour)()
			var passMu sync.Mutex
			passes := map[*mqttAccountSessionManager]int{}
			defer setMQTTTestSessExpirySweepPassHook(func(as *mqttAccountSessionManager) {
				passMu.Lock()
				passes[as]++
				passMu.Unlock()
			})()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			// Counted per account manager, because sweep goroutines outlive the
			// servers that started them: a bare "a pass completed" signal could be a
			// previous test's sweeper rather than this one's startup pass.
			waitSweepPass := func(asm *mqttAccountSessionManager) {
				t.Helper()
				checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
					passMu.Lock()
					defer passMu.Unlock()
					if passes[asm] == 0 {
						return fmt.Errorf("the startup session-expiry sweep has not finished a pass yet")
					}
					return nil
				})
			}

			asm := testReservedProbeASM(t, s, o)
			// This test is about what the CONNECT restore path does with a record it
			// refuses to interpret: nothing. The session-record watch would otherwise
			// observe the record this fixture PLANTS and — for the overdue row, with no
			// session resident — reconcile its expiry immediately, deleting the record
			// and its durables through a mechanism this test is not about. That is the
			// documented expiry behavior for such a record (the expiry path and the
			// orphan sweep DO finish it); the watch only makes it prompt instead of
			// waiting for the periodic sweep. Suppressed so the assertions below speak
			// about the refusal alone.
			testMQTTSuppressRecordWatch(t, asm)
			cid, durables := tc.setup(t, s, o, asm, waitSweepPass)

			// Prove the fixture is what it claims BEFORE the refusal, so nothing
			// below can pass vacuously. The membership check reads the maps directly
			// rather than asking the validator, which is the thing under test.
			for _, d := range durables {
				if !testDurableExists(t, s, d) {
					t.Fatalf("seed durable %q was not created; the survival assertion would prove nothing", d)
				}
			}
			seq, data := testLoadRawSessionMsg(t, asm, cid)
			seeded := &mqttPersistedSession{}
			if err := json.Unmarshal(data, seeded); err != nil {
				t.Fatalf("decode seeded record: %v", err)
			}
			_, inSubs := seeded.Subs[tc.key]
			_, inShared := seeded.SharedSubs[tc.key]
			_, inCons := seeded.Cons[tc.key]
			_, inDrains := seeded.Drains[tc.key]
			if !inSubs && !inShared && !inCons && !inDrains {
				t.Fatalf("seeded record carries %q in none of Subs, SharedSubs, Cons, Drains; there is nothing to refuse", tc.key)
			}

			requireIntact := func(what string) {
				t.Helper()
				for _, d := range durables {
					if !testDurableExists(t, s, d) {
						t.Errorf("%s: JetStream durable %q was deleted; a refusal destroys nothing", what, d)
					}
				}
				gotSeq, gotData := testLoadRawSessionMsg(t, asm, cid)
				if gotSeq != seq {
					t.Errorf("%s: session record moved from sequence %v to %v; the refusal wrote to it", what, seq, gotSeq)
				}
				if !bytes.Equal(gotData, data) {
					t.Errorf("%s: session record content changed\nbefore: %s\n after: %s", what, data, gotData)
				}
			}

			// The resume is refused, and nothing was migrated or discarded on the way.
			testReservedRefusedConnect(t, o, cid, false)
			requireIntact("after the refused resume")

			// A CLEAN START attempt is refused identically. Stated consequence, not an
			// oversight: createOrRestoreSession runs before processConnect's
			// clean-session check, and the discard it would take derives shared
			// durable names from the record's own keys and deletes consumers — the
			// same migration under another name. Recreating the streams is the only
			// exit, which is the finding's guidance.
			testReservedRefusedConnect(t, o, cid, true)
			requireIntact("after the refused clean-start attempt")

			// The diagnostic has to be the ACTIONABLE one: the finding and
			// CONTRIBUTING.md both require stop-and-recreate guidance naming the
			// offending key. Compared in full rather than fragment by fragment,
			// because a negated message ("do not stop ..., do not recreate ...")
			// contains every fragment while instructing the opposite.
			_, _, err := asm.createOrRestoreSession(cid, s.getOpts())
			want := fmt.Sprintf("restoring session %q: the record contains a "+
				"subscription in the reserved %q namespace (%q), written by a pre-release "+
				"development broker; this format is not supported: stop this account's "+
				"brokers and recreate the bridge-owned JetStream state (the $MQTT_* "+
				"streams)", cid, mqttPrefix, tc.key)
			if err == nil || err.Error() != want {
				t.Fatalf("createOrRestoreSession error:\n got: %v\nwant: %s", err, want)
			}
		})
	}
}
