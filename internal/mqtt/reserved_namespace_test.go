//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
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
var reservedPrefixes = map[string]string{
	"deliver subject":   mqttSubPrefix,
	"pubrel delivery":   mqttPubRelDeliverySubjectPrefix,
	"shared deliver":    mqttSharedSubDeliverPrefix,
	"sessions":          mqttSessStreamSubjectPrefix,
	"messages":          mqttStreamSubjectPrefix,
	"retained messages": mqttRetainedMsgsStreamSubject,
	"jsa replies":       mqttJSARepliesPrefix,
	"will queue":        mqttWillQSubjectPrefix,
	"will cancel":       mqttWillCancelSubjectPrefix,
	"qos2 inbound":      mqttQoS2IncomingMsgsStreamSubjectPrefix,
	"outbound":          mqttOutSubjectPrefix,
	"pubrel outbound":   mqttPubRelSubjectPrefix,
}

// TestMQTTDenySubscribeToReservedNamespace covers the whole "$MQTT.>" namespace,
// not just the three delivery prefixes upstream rejected. Every internal subject
// carries real traffic -- $MQTT.msgs.> is every QoS 1/2 message in the account
// and $MQTT.rmsgs.> every retained message -- so an explicit filter into any of
// them is a read-side bypass of the client's own subscribe permissions.
func TestMQTTDenySubscribeToReservedNamespace(t *testing.T) {
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
	return cid
}

// testCreateLegacyDurable creates a REAL JetStream durable on the messages
// stream, so a purge test can prove the durable is actually deleted rather than
// just that a map entry disappeared. Returns the ConsumerConfig to seed into the
// session record.
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
// mqttSession is what proves the purge was actually persisted: a resumed client
// gets the in-memory session from asm.sessions, so inspecting that map would
// still pass if save() silently stopped writing.
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

// TestMQTTPurgeLegacyReservedSubs covers the cleanup half of the deny. A denied
// filter is merely skipped during restore, and the restore path never persists,
// so without an explicit purge a legacy "$MQTT." entry would sit in the record
// and be re-denied on every reconnect forever.
//
// Every fixture uses a real JetStream durable and asserts against the persisted
// record, so neither a broken save() nor a broken consumer delete can pass.
func TestMQTTPurgeLegacyReservedSubs(t *testing.T) {
	reserved := mqttStreamSubjectPrefix + ">"
	sharedKey := "$share/g/" + reserved
	_, sharedDeliver, sharedDurable := mqttSharedNames("g", reserved)

	for _, tc := range []struct {
		name string
		key  string
		// wantCons is true when the fixture's record is expected to carry a Cons
		// entry for key, so the "no longer present" assertion is not vacuous.
		wantCons bool
		// setup builds the session record and returns the client ID plus the
		// durables that must be gone once the purge has run.
		setup func(t *testing.T, s *Server, o *Options) (string, []string)
	}{
		{
			// Plain filter with a consumer entry. A plain consumer IS persisted by
			// buildPersistedSessionLocked, so seeding through the session works.
			name:     "plain",
			key:      reserved,
			wantCons: true,
			setup: func(t *testing.T, s *Server, o *Options) (string, []string) {
				cid := testReservedSessionRecordEx(t, s, o, func(asm *mqttAccountSessionManager, sess *mqttSession) {
					cc := testCreateLegacyDurable(t, asm, "legacy-plain", mqttSubPrefix+"legacy", reserved, _EMPTY_)
					sess.subs = map[string]byte{reserved: 1}
					sess.cons = map[string]*ConsumerConfig{reserved: cc}
				})
				return cid, []string{"legacy-plain"}
			},
		},
		{
			// Shared filter with NO consumer entry: the realistic current-format
			// record, because buildPersistedSessionLocked never persists a consumer
			// that has a DeliverGroup. The purge must derive the durable from the
			// session key rather than walking sess.cons.
			name: "shared without consumer entry",
			key:  sharedKey,
			setup: func(t *testing.T, s *Server, o *Options) (string, []string) {
				cid := testReservedSessionRecordEx(t, s, o, func(asm *mqttAccountSessionManager, sess *mqttSession) {
					testCreateLegacyDurable(t, asm, sharedDurable, sharedDeliver, reserved, "g")
					sess.sharedSubs = map[string]byte{sharedKey: 1}
				})
				return cid, []string{sharedDurable}
			},
		},
		{
			// Old-format record that DOES carry the shared consumer entry, so the
			// purge takes the releaseSharedConsumer branch rather than deriving the
			// durable from the key. Seeding this through sess.save() is impossible
			// (the shared consumer would be stripped on the way out), so the record
			// is written directly and the client ID is never connected beforehand --
			// the resume must restore it from the stream.
			name:     "shared with consumer entry (old format)",
			key:      sharedKey,
			wantCons: true,
			setup: func(t *testing.T, s *Server, o *Options) (string, []string) {
				cid := "legacy-oldfmt-" + nuid.Next()
				// A throwaway client just to reach the account session manager.
				probe, probeR := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "probe-" + cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
				defer probe.Close()
				testMQTTReadConnAckV5(t, probeR)
				asm := testMQTTGetAccountSessionManager(t, s, "probe-"+cid)

				cc := testCreateLegacyDurable(t, asm, sharedDurable, sharedDeliver, reserved, "g")
				testWriteLegacyPersistedSession(t, asm, cid, &mqttPersistedSession{
					SharedSubs:     map[string]byte{sharedKey: 1},
					Cons:           map[string]*ConsumerConfig{sharedKey: cc},
					ExpiryInterval: 300,
				})
				return cid, []string{sharedDurable}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			cid, durables := tc.setup(t, s, o)
			for _, d := range durables {
				if !testDurableExists(t, s, d) {
					t.Fatalf("seed durable %q was not created; the deletion assertion below would prove nothing", d)
				}
			}

			// Prove the fixture is what it claims BEFORE the resume, so the
			// post-purge assertions cannot pass vacuously.
			seeded := testLoadPersistedSessionByID(t, s, o, cid)
			_, seededPlain := seeded.Subs[tc.key]
			_, seededShared := seeded.SharedSubs[tc.key]
			if !seededPlain && !seededShared {
				t.Fatalf("seeded record does not contain %q; nothing to purge", tc.key)
			}
			if _, ok := seeded.Cons[tc.key]; ok != tc.wantCons {
				t.Fatalf("seeded record Cons[%q] present = %v, want %v", tc.key, ok, tc.wantCons)
			}

			// Resume with v5: a 3.1.1 resume force-leaves shared subscriptions
			// before the reserved-namespace check ever sees them, which would make
			// the shared fixtures pass for the wrong reason.
			c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			sp, reason, _ := testMQTTReadConnAckV5(t, r)
			if reason != mqttReasonSuccess {
				t.Fatalf("resume CONNACK reason = 0x%02x, want success", reason)
			}
			if !sp {
				t.Fatal("expected the seeded session to be present on resume")
			}

			// The CONNACK is sent before the saved subscriptions are restored, so
			// poll. Assert against the PERSISTED record, not the resident session.
			asm := testMQTTGetAccountSessionManager(t, s, cid)
			checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
				ps := testLoadPersistedSession(t, asm, cid)
				if _, ok := ps.Subs[tc.key]; ok {
					return fmt.Errorf("reserved filter %q is still in the persisted Subs", tc.key)
				}
				if _, ok := ps.SharedSubs[tc.key]; ok {
					return fmt.Errorf("reserved filter %q is still in the persisted SharedSubs", tc.key)
				}
				if _, ok := ps.Cons[tc.key]; ok {
					return fmt.Errorf("consumer entry for %q is still in the persisted record", tc.key)
				}
				for _, d := range durables {
					if testDurableExists(t, s, d) {
						return fmt.Errorf("JetStream durable %q survived the purge", d)
					}
				}
				return nil
			})

			// And it stays gone across a full reload: evict the resident session so
			// the second resume must read the record back from $MQTT_sess.
			c.Close()
			asm.mu.Lock()
			delete(asm.sessions, cid)
			asm.mu.Unlock()

			c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
			defer c2.Close()
			if _, reason, _ := testMQTTReadConnAckV5(t, r2); reason != mqttReasonSuccess {
				t.Fatalf("second resume CONNACK reason = 0x%02x", reason)
			}
			asm.mu.Lock()
			sess2 := asm.sessions[cid]
			asm.mu.Unlock()
			if sess2 == nil {
				t.Fatal("no session after the reloading resume")
			}
			sess2.mu.Lock()
			_, stillSubs := sess2.subs[tc.key]
			_, stillShared := sess2.sharedSubs[tc.key]
			sess2.mu.Unlock()
			if stillSubs || stillShared {
				t.Errorf("reserved filter %q came back after a reload from the stream; the purge did not persist", tc.key)
			}
		})
	}
}

// TestMQTTPurgeLevelUpConsumer pins that a multi-level reserved filter releases
// BOTH of its consumers. "$MQTT.msgs.>" owns a main sid and a synthetic level-up
// sid; dropping only the main one leaks a real durable after the membership is
// gone, which is invisible in the session record.
func TestMQTTPurgeLevelUpConsumer(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	reserved := mqttStreamSubjectPrefix + ">"
	levelUp := reserved[:len(reserved)-2] + mqttMultiLevelSidSuffix

	cid := testReservedSessionRecordEx(t, s, o, func(asm *mqttAccountSessionManager, sess *mqttSession) {
		main := testCreateLegacyDurable(t, asm, "legacy-main", mqttSubPrefix+"main", reserved, _EMPTY_)
		fwc := testCreateLegacyDurable(t, asm, "legacy-fwc", mqttSubPrefix+"fwc", reserved[:len(reserved)-2], _EMPTY_)
		sess.subs = map[string]byte{reserved: 1}
		sess.cons = map[string]*ConsumerConfig{reserved: main, levelUp: fwc}
	})
	for _, d := range []string{"legacy-main", "legacy-fwc"} {
		if !testDurableExists(t, s, d) {
			t.Fatalf("seed durable %q was not created", d)
		}
	}

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if sp, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess || !sp {
		t.Fatalf("resume CONNACK reason=0x%02x sessionPresent=%v", reason, sp)
	}

	asm := testMQTTGetAccountSessionManager(t, s, cid)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		ps := testLoadPersistedSession(t, asm, cid)
		for _, sid := range []string{reserved, levelUp} {
			if _, ok := ps.Cons[sid]; ok {
				return fmt.Errorf("consumer entry for %q is still in the persisted record", sid)
			}
		}
		// The level-up durable is the one a main-sid-only cleanup would leak.
		for _, d := range []string{"legacy-main", "legacy-fwc"} {
			if testDurableExists(t, s, d) {
				return fmt.Errorf("JetStream durable %q survived the purge", d)
			}
		}
		return nil
	})
}

// TestMQTTPurgeReservedSubsSaveFailureRollsBack covers the failure branch. If the
// record cannot be persisted, the in-memory maps must be restored exactly rather
// than left disagreeing with the stream, and the CONNECT must still succeed --
// the filter is denied either way, so a failed purge is a deferred cleanup, not
// a connection error.
func TestMQTTPurgeReservedSubsSaveFailureRollsBack(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	reserved := mqttStreamSubjectPrefix + ">"
	cid := testReservedSessionRecordEx(t, s, o, func(asm *mqttAccountSessionManager, sess *mqttSession) {
		cc := testCreateLegacyDurable(t, asm, "legacy-rollback", mqttSubPrefix+"rb", reserved, _EMPTY_)
		sess.subs = map[string]byte{reserved: 1}
		sess.cons = map[string]*ConsumerConfig{reserved: cc}
	})

	// Fail only the purge's save. It is identifiable precisely: sess.update has
	// already removed the reserved key from subs, but dropCons has not yet run,
	// so the consumer entry is still there. Failing every save instead would tear
	// down the CONNECT itself and test nothing about the rollback.
	saveErr := errors.New("injected save failure")
	restoreHook := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		sess.mu.Lock()
		_, inSubs := sess.subs[reserved]
		_, inCons := sess.cons[reserved]
		sess.mu.Unlock()
		if !inSubs && inCons {
			return saveErr
		}
		return nil
	})
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	// Saves run on server goroutines that outlive the client socket, so the hook
	// must stay installed until the server has finished with this client. Closing
	// the connection is not enough: a disconnect-path save still in flight would
	// race the hook reset. Wait for the client to leave s.clients first.
	defer func() {
		c.Close()
		checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, cl := range s.clients {
				cl.mu.Lock()
				id := cl.mqtt.cid
				cl.mu.Unlock()
				if id == cid {
					return fmt.Errorf("client %q still registered", cid)
				}
			}
			return nil
		})
		restoreHook()
	}()
	sp, reason, _ := testMQTTReadConnAckV5(t, r)
	if reason != mqttReasonSuccess {
		t.Fatalf("resume CONNACK reason = 0x%02x; a failed purge must not fail the CONNECT", reason)
	}
	if !sp {
		t.Fatal("expected the seeded session to be present on resume")
	}

	// The membership must be intact in memory: a half-applied purge would leave
	// the record and memory disagreeing.
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	asm.mu.Lock()
	sess := asm.sessions[cid]
	asm.mu.Unlock()
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if _, ok := sess.subs[reserved]; !ok {
			return fmt.Errorf("subs[%q] was not rolled back after the save failure", reserved)
		}
		if _, ok := sess.cons[reserved]; !ok {
			return fmt.Errorf("cons[%q] was dropped despite the save failure", reserved)
		}
		return nil
	})
	// Give the purge a moment to have (wrongly) deleted the durable, then check.
	if !testDurableExists(t, s, "legacy-rollback") {
		t.Error("durable was deleted even though the purge failed to persist; teardown must follow a confirmed save")
	}
}
