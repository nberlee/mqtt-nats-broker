//go:build mqttport_fulltests

// The JetStream acknowledgement layout a v2.16 server emits by default, met for real.
//
// A consumer's acknowledgement reply has two layouts: v1
// ($JS.ACK.<stream>.<consumer>.<dc>.<sseq>.<dseq>.<ts>.<pending>) and v2, which inserts the
// JetStream domain and the account hash before the stream. The pinned server emits v1 unless
// js_ack_fc_v2 is set; upstream makes v2 the default in 2.16. The cluster harness sets it on
// every node, and this witness runs the broker under it with a NAMED account, a CONFIGURED
// domain and the recommended client ACL — the shape in which a parser that mis-offsets the two
// inserted tokens cannot pass by coincidence of `_` and `$G`.
//
// Every reply the server emits is fed to the broker's own parsers, and each arm checks the
// behavior from the consumer's own accounting (ack floor, pending count), never from the
// message having arrived: a QoS 1 message arrives whether or not its acknowledgement ever
// reaches the consumer.

package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

const (
	ackLayoutDomain  = "hub"
	ackLayoutAccount = "APP"
	// ackLayoutDeniedSubject is what bobd may not receive; his subscription filter still
	// matches it, so the deny is a delivery decision, not a SUBACK refusal.
	ackLayoutDeniedSubject = "t.deny"
)

// ackLayoutConf is the account half of every node's configuration. The client users carry the
// recommended ACL (docs/ARCHITECTURE.md "Recommended NATS permissions"): everything except the
// broker's reserved namespace. svc is the broker's service identity.
var ackLayoutConf = `
jetstream { domain: ` + ackLayoutDomain + ` }
accounts {
  ` + ackLayoutAccount + `: {
    jetstream: enabled
    users: [
      { user: svc, password: svcpw, permissions: { publish: ">", subscribe: ">" } }
      { user: alice, password: pw, permissions: {
          publish:   { allow: [">"], deny: ["$MQTT.>"] }
          subscribe: { allow: [">"], deny: ["$MQTT.>"] }
      } }
      { user: bob, password: pw, permissions: {
          publish:   { allow: [">"], deny: ["$MQTT.>"] }
          subscribe: { allow: [">"], deny: ["$MQTT.>"] }
      } }
      { user: bobd, password: pw, permissions: {
          publish:   { allow: [">"], deny: ["$MQTT.>"] }
          subscribe: { allow: [">"], deny: ["$MQTT.>", "` + ackLayoutDeniedSubject + `"] }
      } }
    ]
  }
}
`

// testAckLayoutServiceDeny, when non-empty, is denied to svc's publish permission. Nil in the
// witness; the ACL mutation sets it to the v2 layout's prefix.
var testAckLayoutServiceDeny []string

// ackLayoutNodeOpts parses the account configuration afresh for every start, so no server
// ever adopts another's *Account.
func ackLayoutNodeOpts(t *testing.T) func(*natsserver.Options) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "accounts.conf")
	if err := os.WriteFile(path, []byte(ackLayoutConf), 0o600); err != nil {
		t.Fatalf("write account config: %v", err)
	}
	return func(o *natsserver.Options) {
		fo, err := natsserver.ProcessConfigFile(path)
		if err != nil {
			t.Fatalf("parse account config: %v", err)
		}
		for _, u := range fo.Users {
			if u.Username == "svc" && len(testAckLayoutServiceDeny) > 0 {
				u.Permissions = &natsserver.Permissions{
					Publish:   &natsserver.SubjectPermission{Allow: []string{">"}, Deny: testAckLayoutServiceDeny},
					Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
				}
			}
		}
		o.Accounts, o.Users, o.JetStreamDomain = fo.Accounts, fo.Users, fo.JetStreamDomain
	}
}

// startAckLayoutBroker is startBroker for the account configuration: the service connection
// authenticates as svc, the broker addresses JetStream in the configured domain, and every MQTT
// client is dialled as its own NATS user, so the client ACLs bind.
func (c *testJetStreamCluster) startAckLayoutBroker(t *testing.T, preferred int, ackWait time.Duration) *testClusterBroker {
	t.Helper()
	urls := strings.Join(c.orderedURLs(preferred), ",")
	nc, err := nats.Connect(urls, nats.UserInfo("svc", "svcpw"), nats.DontRandomize(),
		nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond))
	if err != nil {
		t.Fatalf("broker NATS connect: %v", err)
	}
	opts := testMQTTDefaultOptionsV5()
	opts.MQTT.StreamReplicas = 3
	opts.StreamReplicas = 3
	opts.MQTT.JsDomain = ackLayoutDomain
	opts.MQTT.AckWait = ackWait
	s := NewServer(nc, opts, "bridge-"+nuid.Next())
	s.SetClientDialer(func(_, user, pass string) (*nats.Conn, error) {
		return nats.Connect(urls, nats.UserInfo(user, pass), nats.DontRandomize())
	})
	if err := s.Start(); err != nil {
		nc.Close()
		t.Fatalf("broker start: %v", err)
	}
	if err := s.readyForConnections(5 * time.Second); err != nil {
		<-s.BeginShutdown()
		nc.Close()
		t.Fatalf("broker not ready: %v", err)
	}
	opts.MQTT.Port = s.Addr().(*net.TCPAddr).Port
	b := &testClusterBroker{s: s, nc: nc, opts: opts}
	t.Cleanup(b.cleanup)
	return b
}

// ackObservation is one message seen on $JS.ACK.>: the reply subject and what was sent on it.
type ackObservation struct {
	subject string
	payload string
}

func (o ackObservation) positive() bool {
	return o.payload == _EMPTY_ || strings.HasPrefix(o.payload, string(AckAck))
}

func (o ackObservation) negative() bool {
	return strings.HasPrefix(o.payload, string(AckNak))
}

// v2Tokens splits a v2 reply by the test's own positions, independent of the broker's parsers
// so a mutated parser cannot mis-key what the test is looking for.
func (o ackObservation) v2Tokens() (stream, durable string, sseq uint64, ok bool) {
	toks := strings.Split(o.subject, ".")
	if len(toks) != expectedNumReplyTokensV2 {
		return _EMPTY_, _EMPTY_, 0, false
	}
	n, err := strconv.ParseUint(toks[7], 10, 64)
	if err != nil {
		return _EMPTY_, _EMPTY_, 0, false
	}
	return toks[4], toks[5], n, true
}

// ackObserver records every acknowledgement in the account. It is the service identity's
// subscription, so it sees the broker's own submissions on the wire.
type ackObserver struct {
	mu   sync.Mutex
	seen []ackObservation
}

func newAckObserver(t *testing.T, nc *nats.Conn) *ackObserver {
	t.Helper()
	o := &ackObserver{}
	sub, err := nc.Subscribe("$JS.ACK.>", func(m *nats.Msg) {
		o.mu.Lock()
		o.seen = append(o.seen, ackObservation{subject: m.Subject, payload: string(m.Data)})
		o.mu.Unlock()
	})
	if err != nil {
		t.Fatalf("observe $JS.ACK.>: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("register the observer: %v", err)
	}
	return o
}

func (o *ackObserver) all() []ackObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]ackObservation(nil), o.seen...)
}

// of returns the observations acknowledging {stream, durable, sseq}.
func (o *ackObserver) of(stream, durable string, sseq uint64) []ackObservation {
	var out []ackObservation
	for _, ob := range o.all() {
		if s, d, n, ok := ob.v2Tokens(); ok && s == stream && d == durable && n == sseq {
			out = append(out, ob)
		}
	}
	return out
}

// awaitPositive waits for a positive acknowledgement of {stream, durable, sseq} and returns it.
func (o *ackObserver) awaitPositive(t *testing.T, d time.Duration, stream, durable string, sseq uint64) ackObservation {
	t.Helper()
	var got ackObservation
	checkFor(t, d, 50*time.Millisecond, func() error {
		for _, ob := range o.of(stream, durable, sseq) {
			if ob.positive() {
				got = ob
				return nil
			}
		}
		return fmt.Errorf("no positive acknowledgement of %s/%s seq %d among %v", stream, durable, sseq, o.of(stream, durable, sseq))
	})
	return got
}

func (o *ackObserver) positives(stream, durable string, sseq uint64) int {
	n := 0
	for _, ob := range o.of(stream, durable, sseq) {
		if ob.positive() {
			n++
		}
	}
	return n
}

// assertParsersRead feeds a reply the server emitted to the broker's two parsers and requires
// the source it names.
func assertParsersRead(t *testing.T, ob ackObservation, stream, durable string, sseq uint64) {
	t.Helper()
	if got, _, _, _, _ := ackReplyInfo(ob.subject); got != sseq {
		t.Fatalf("ackReplyInfo(%q) read stream sequence %d, want %d", ob.subject, got, sseq)
	}
	s, d, n, ok := mqttParseSourceAckReply(ob.subject)
	if !ok || s != stream || d != durable || n != sseq {
		t.Fatalf("mqttParseSourceAckReply(%q) = %q, %q, %d, %v; want %q, %q, %d", ob.subject, s, d, n, ok, stream, durable, sseq)
	}
}

// ackLayoutJS addresses JetStream in the configured domain on the service identity.
func ackLayoutJS(t *testing.T, nc *nats.Conn) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.NewWithDomain(nc, ackLayoutDomain)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

func ackLayoutConsumer(t *testing.T, js jetstream.JetStream, stream, durable string) *jetstream.ConsumerInfo {
	t.Helper()
	ci, err := ackLayoutConsumerInfo(js, stream, durable)
	if err != nil {
		t.Fatal(err)
	}
	return ci
}

// ackLayoutConsumerInfo is the consumer's own accounting. An error is returned rather than
// fatal so a poll can ride out a leader change, during which the info request goes unanswered.
func ackLayoutConsumerInfo(js jetstream.JetStream, stream, durable string) (*jetstream.ConsumerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The broker's consumers are push consumers, which js.Consumer refuses to bind.
	cons, err := js.PushConsumer(ctx, stream, durable)
	if err != nil {
		return nil, fmt.Errorf("consumer %s/%s: %w", stream, durable, err)
	}
	ci, err := cons.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("consumer info %s/%s: %w", stream, durable, err)
	}
	return ci, nil
}

// awaitSettled waits until the consumer's own accounting says sseq was acknowledged.
func awaitSettled(t *testing.T, js jetstream.JetStream, d time.Duration, stream, durable string, sseq uint64) *jetstream.ConsumerInfo {
	t.Helper()
	var ci *jetstream.ConsumerInfo
	checkFor(t, d, 100*time.Millisecond, func() error {
		var err error
		if ci, err = ackLayoutConsumerInfo(js, stream, durable); err != nil {
			return err
		}
		if ci.AckFloor.Stream < sseq || ci.NumAckPending != 0 {
			return fmt.Errorf("%s/%s ack floor %d, %d pending; want floor >= %d and nothing pending",
				stream, durable, ci.AckFloor.Stream, ci.NumAckPending, sseq)
		}
		return nil
	})
	return ci
}

// stepDownConsumerLeader moves a consumer's leadership and waits for a different leader.
func stepDownConsumerLeader(t *testing.T, nc *nats.Conn, js jetstream.JetStream, stream, durable string) {
	t.Helper()
	before := ackLayoutConsumer(t, js, stream, durable)
	if before.Cluster == nil || before.Cluster.Leader == _EMPTY_ {
		t.Fatalf("consumer %s/%s has no leader to step down", stream, durable)
	}
	subj := fmt.Sprintf("$JS.%s.API.CONSUMER.LEADER.STEPDOWN.%s.%s", ackLayoutDomain, stream, durable)
	resp, err := nc.Request(subj, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("step down %s/%s: %v", stream, durable, err)
	}
	var r struct {
		Success bool `json:"success"`
		Error   *struct {
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(resp.Data, &r); err != nil || !r.Success {
		t.Fatalf("step down %s/%s refused: %s (%v)", stream, durable, resp.Data, err)
	}
	checkFor(t, 30*time.Second, 100*time.Millisecond, func() error {
		ci, err := ackLayoutConsumerInfo(js, stream, durable)
		if err != nil {
			return err
		}
		if ci.Cluster == nil || ci.Cluster.Leader == _EMPTY_ || ci.Cluster.Leader == before.Cluster.Leader {
			return fmt.Errorf("%s/%s leader still %v (was %q)", stream, durable, ci.Cluster, before.Cluster.Leader)
		}
		return nil
	})
}

// ackLayoutSession returns the broker's resident session for a client.
func ackLayoutSession(t *testing.T, b *testClusterBroker, clientID string) *mqttSession {
	t.Helper()
	asm := asmOf(t, b.s)
	var sess *mqttSession
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		asm.mu.RLock()
		sess = asm.sessions[clientID]
		asm.mu.RUnlock()
		if sess == nil {
			return fmt.Errorf("no resident session for %q", clientID)
		}
		return nil
	})
	return sess
}

// trackedSource returns the source consumer and stream sequence the session tracks pi under.
func trackedSource(t *testing.T, sess *mqttSession, pi uint16) (durable string, sseq uint64) {
	t.Helper()
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		p := sess.pendingPublish[pi]
		if p == nil || p.jsDur == _EMPTY_ || p.sseq == 0 {
			return fmt.Errorf("identifier %d not tracked yet", pi)
		}
		durable, sseq = p.jsDur, p.sseq
		return nil
	})
	return durable, sseq
}

func ackLayoutConnect(t *testing.T, b *testClusterBroker, ci *mqttV5ConnInfo) (net.Conn, *mqttReader, bool) {
	t.Helper()
	c, r := testMQTTConnectV5(t, ci, b.opts.MQTT.Host, b.opts.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	sp, reason, _ := testMQTTReadConnAckV5(t, r)
	if reason != mqttReasonSuccess {
		t.Fatalf("CONNECT %q refused: 0x%x", ci.clientID, reason)
	}
	return c, r, sp
}

func ackLayoutPublish(t *testing.T, b *testClusterBroker, qos byte, topic, payload string) {
	t.Helper()
	c, r, _ := ackLayoutConnect(t, b, &mqttV5ConnInfo{clientID: "pub-" + nuid.Next(), cleanStart: true, user: "alice", pass: "pw"})
	testMQTTPublishV5(t, c, qos, 1, topic, []byte(payload))
	switch qos {
	case 1:
		if reason, _ := testMQTTReadPubAckV5(t, r, 1); reason != mqttReasonSuccess {
			t.Fatalf("PUBACK reason 0x%x", reason)
		}
	case 2:
		if reason, _ := testMQTTReadPubRecV5(t, r, 1); reason != mqttReasonSuccess {
			t.Fatalf("PUBREC reason 0x%x", reason)
		}
		testQoS2SendPubRel(t, c, 1)
		if reason, _ := testMQTTReadPubCompV5(t, r, 1); reason != mqttReasonSuccess {
			t.Fatalf("PUBCOMP reason 0x%x", reason)
		}
	}
	testMQTTDisconnect(t, c, nil)
}

func ackLayoutReadPublish(t *testing.T, r *mqttReader, d time.Duration, topic string) uint16 {
	t.Helper()
	got, _, _, pi, ok := testMQTTTryReadPublishV5(r, d)
	if !ok || got != topic {
		t.Fatalf("expected a PUBLISH on %q, got topic=%q ok=%v", topic, got, ok)
	}
	return pi
}

// ackLayoutReadPubRel reads a PUBREL for pi, skipping its v5 reason code and properties.
func ackLayoutReadPubRel(t *testing.T, r *mqttReader, d time.Duration, pi uint16) {
	t.Helper()
	b, pl, ready := testMQTTReadPacketReady(r, d)
	if !ready {
		t.Fatalf("no PUBREL for %d within %v", pi, d)
	}
	if pt := b & mqttPacketMask; pt != mqttPacketPubRel {
		t.Fatalf("expected PUBREL (%x), got %x", mqttPacketPubRel, pt)
	}
	start := r.pos
	got, err := r.readUint16("pubrel pi")
	if err != nil || got != pi {
		t.Fatalf("expected PUBREL pi=%d, got %d (err=%v)", pi, got, err)
	}
	r.pos = start + pl
}

// TestMQTTAckReplyLayoutV2Replicated is the witness. One cluster, four brokers: A and B create
// their consumers with a long AckWait, where a redelivery must NOT happen; C and D with a short
// one, where a redelivery is the controlled trigger.
func TestMQTTAckReplyLayoutV2Replicated(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run the replicated acknowledgement-layout witness")
	}
	c := testNewJetStreamClusterWith(t, 3, ackLayoutAccount, ackLayoutNodeOpts(t))
	a := c.startAckLayoutBroker(t, 0, 5*time.Minute)
	b := c.startAckLayoutBroker(t, 1, 5*time.Minute)
	cb := c.startAckLayoutBroker(t, 2, 2*time.Second)
	db := c.startAckLayoutBroker(t, 0, 2*time.Second)
	js := ackLayoutJS(t, a.nc)
	obs := newAckObserver(t, a.nc)

	t.Run("QoS1", func(t *testing.T) { ackLayoutQoS1(t, a, js, obs) })
	t.Run("QoS1Takeover", func(t *testing.T) { ackLayoutQoS1Takeover(t, a, b, js, obs) })
	t.Run("QoS2PubRel", func(t *testing.T) { ackLayoutQoS2PubRel(t, cb, db, js, obs) })
	t.Run("SharedHandBack", func(t *testing.T) { ackLayoutSharedHandBack(t, cb, db, js, obs) })

	// The layout oracle: every acknowledgement observed is v2 with this domain and this
	// account, none is v1 (without the flag every one has nine tokens), and each reads back
	// through the broker's parsers as the source its own tokens name.
	seen := obs.all()
	if len(seen) == 0 {
		t.Fatal("the observer saw no acknowledgement at all")
	}
	hash := getHash(ackLayoutAccount)
	for _, ob := range seen {
		toks := strings.Split(ob.subject, ".")
		if len(toks) != expectedNumReplyTokensV2 || toks[2] != ackLayoutDomain || toks[3] != hash {
			t.Fatalf("acknowledgement %q is not the v2 layout for domain %q and account hash %q", ob.subject, ackLayoutDomain, hash)
		}
		stream, durable, sseq, ok := ob.v2Tokens()
		if !ok {
			t.Fatalf("acknowledgement %q does not name a stream sequence", ob.subject)
		}
		assertParsersRead(t, ob, stream, durable, sseq)
		d, n, shared, malformed := mqttSourceAckScope(ob.subject)
		wantShared := stream == mqttStreamName && mqttIsSharedDurable(durable)
		if malformed || shared != wantShared || (shared && (d != durable || n != sseq)) {
			t.Fatalf("mqttSourceAckScope(%q) = %q, %d, shared=%v, malformed=%v; want shared=%v",
				ob.subject, d, n, shared, malformed, wantShared)
		}
	}
}

// ackLayoutQoS1: a plain QoS 1 delivery acknowledged on its v2 reply.
func ackLayoutQoS1(t *testing.T, a *testClusterBroker, js jetstream.JetStream, obs *ackObserver) {
	const id, topic = "ack-q1", "t/q1"
	c, r, _ := ackLayoutConnect(t, a, &mqttV5ConnInfo{clientID: id, cleanStart: true, user: "alice", pass: "pw"})
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	ackLayoutPublish(t, a, 1, topic, "q1")
	pi := ackLayoutReadPublish(t, r, 5*time.Second, topic)
	dur, sseq := trackedSource(t, ackLayoutSession(t, a, id), pi)
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)

	ob := obs.awaitPositive(t, 10*time.Second, mqttStreamName, dur, sseq)
	assertParsersRead(t, ob, mqttStreamName, dur, sseq)
	awaitSettled(t, js, 10*time.Second, mqttStreamName, dur, sseq)
}

// ackLayoutQoS1Takeover: broker B acknowledges on the v2 reply broker A RECORDED, against a
// consumer whose leader moved, with no redelivery in between.
func ackLayoutQoS1Takeover(t *testing.T, a, b *testClusterBroker, js jetstream.JetStream, obs *ackObserver) {
	const id, topic = "ack-q1t", "t/q1t"
	ci := &mqttV5ConnInfo{clientID: id, user: "alice", pass: "pw", props: mqttV5ConnPropsSessionExpiry(300)}
	c, r, _ := ackLayoutConnect(t, a, ci)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}})
	ackLayoutPublish(t, a, 1, topic, "q1t")
	pi := ackLayoutReadPublish(t, r, 5*time.Second, topic)
	sess := ackLayoutSession(t, a, id)
	dur, sseq := trackedSource(t, sess, pi)
	sess.mu.Lock()
	regSubject := sess.inflightSubjectLocked(pi)
	sess.mu.Unlock()
	testMQTTDisconnect(t, c, nil)

	// The cut: A was never acknowledged for this delivery, so it has nothing to submit. Held over
	// a window, so a late submission by A would be seen here rather than credited to B.
	var before *jetstream.ConsumerInfo
	for i := 0; i < 2; i++ {
		if i > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		before = ackLayoutConsumer(t, js, mqttStreamName, dur)
		if before.NumAckPending != 1 || before.NumRedelivered != 0 || obs.positives(mqttStreamName, dur, sseq) != 0 {
			t.Fatalf("before the takeover: %d pending, %d redelivered, %d positive acks; want 1, 0, 0",
				before.NumAckPending, before.NumRedelivered, obs.positives(mqttStreamName, dur, sseq))
		}
	}

	// What A recorded is the server's v2 reply for this exact delivery.
	var recorded string
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		st, err := js.Stream(ctx, mqttInflightStreamName)
		if err != nil {
			return err
		}
		m, err := st.GetLastMsgForSubject(ctx, regSubject)
		if err != nil {
			return err
		}
		rec, err := mqttDecodeInflightRecord(m.Data)
		if err != nil {
			return err
		}
		recorded = rec.SourceAckReply
		return nil
	})
	if s, d, n, ok := (ackObservation{subject: recorded}).v2Tokens(); !ok || s != mqttStreamName || d != dur || n != sseq {
		t.Fatalf("the register records %q, not the v2 reply of %s/%s seq %d", recorded, mqttStreamName, dur, sseq)
	}

	stepDownConsumerLeader(t, a.nc, js, mqttStreamName, dur)

	// The client answers the delivery it already holds, on B, without waiting for anything.
	c2, r2, sp := ackLayoutConnect(t, b, ci)
	if !sp {
		t.Fatal("expected the session to be present on B")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)

	ob := obs.awaitPositive(t, 15*time.Second, mqttStreamName, dur, sseq)
	if ob.subject != recorded {
		t.Fatalf("B acknowledged on %q, not on the reply A recorded (%q)", ob.subject, recorded)
	}
	assertParsersRead(t, ob, mqttStreamName, dur, sseq)
	after := awaitSettled(t, js, 15*time.Second, mqttStreamName, dur, sseq)
	if after.Delivered.Consumer != before.Delivered.Consumer || after.NumRedelivered != 0 {
		t.Fatalf("the source was redelivered across the takeover (delivered %d -> %d, %d redelivered): "+
			"the acknowledgement is not attributable to the recorded reply",
			before.Delivered.Consumer, after.Delivered.Consumer, after.NumRedelivered)
	}
	testMQTTDisconnect(t, c2, nil)
	_ = r2
}

// ackLayoutQoS2PubRel: the PUBREL consumer's v2 reply, delivered by a new leader to a broker
// that took the session over, is acknowledged at PUBCOMP. (The source reply of a QoS 2 delivery
// is confirmed at PUBREC, so this arm is about the PUBREL consumer only.)
func ackLayoutQoS2PubRel(t *testing.T, cb, db *testClusterBroker, js jetstream.JetStream, obs *ackObserver) {
	const id, topic = "ack-q2", "t/q2"
	ci := &mqttV5ConnInfo{clientID: id, user: "alice", pass: "pw", props: mqttV5ConnPropsSessionExpiry(300)}
	c, r, _ := ackLayoutConnect(t, cb, ci)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 2}})
	ackLayoutPublish(t, cb, 2, topic, "q2")
	pi := ackLayoutReadPublish(t, r, 5*time.Second, topic)
	sess := ackLayoutSession(t, cb, id)
	dur, sseq := trackedSource(t, sess, pi)
	sess.mu.Lock()
	_, _, relDur := mqttPubRelNames(sess.idHash, sess.gen)
	sess.mu.Unlock()

	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	src := obs.awaitPositive(t, 10*time.Second, mqttStreamName, dur, sseq)
	assertParsersRead(t, src, mqttStreamName, dur, sseq)
	awaitSettled(t, js, 10*time.Second, mqttStreamName, dur, sseq)

	ackLayoutReadPubRel(t, r, 10*time.Second, pi)
	testMQTTDisconnect(t, c, nil)
	var relSeq uint64
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		rel, err := ackLayoutConsumerInfo(js, mqttOutStreamName, relDur)
		if err != nil {
			return err
		}
		if rel.NumAckPending != 1 {
			return fmt.Errorf("PUBREL consumer %s has %d pending, want 1", relDur, rel.NumAckPending)
		}
		relSeq = rel.Delivered.Stream
		return nil
	})

	stepDownConsumerLeader(t, cb.nc, js, mqttOutStreamName, relDur)

	c2, r2, sp := ackLayoutConnect(t, db, ci)
	if !sp {
		t.Fatal("expected the session to be present on D")
	}
	// Redelivered by the new leader once the 2 s AckWait lapses.
	ackLayoutReadPubRel(t, r2, 20*time.Second, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)

	ob := obs.awaitPositive(t, 15*time.Second, mqttOutStreamName, relDur, relSeq)
	if _, _, dc, _, _ := ackReplyInfo(ob.subject); dc < 2 {
		t.Fatalf("PUBREL acknowledged on %q, delivery count %d: not the redelivery to D", ob.subject, dc)
	}
	assertParsersRead(t, ob, mqttOutStreamName, relDur, relSeq)
	awaitSettled(t, js, 15*time.Second, mqttOutStreamName, relDur, relSeq)
	testMQTTDisconnect(t, c2, nil)
}

// ackLayoutSharedHandBack: a shared group's source, admitted to bob and then denied to him on
// resume, is held — handed back on its real v2 reply with the paced NAK, and refused by the
// positive-acknowledgement gate — until another member takes it.
func ackLayoutSharedHandBack(t *testing.T, cb, db *testClusterBroker, js jetstream.JetStream, obs *ackObserver) {
	const filter, topic = "$share/g/t/+", "t/deny"
	bob := &mqttV5ConnInfo{clientID: "ack-bob", user: "bob", pass: "pw", props: mqttV5ConnPropsSessionExpiry(300)}
	c, r, _ := ackLayoutConnect(t, cb, bob)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 1}})
	ackLayoutPublish(t, cb, 1, topic, "held")
	pi := ackLayoutReadPublish(t, r, 5*time.Second, topic)
	dur, sseq := trackedSource(t, ackLayoutSession(t, cb, bob.clientID), pi)
	if !mqttIsSharedDurable(dur) {
		t.Fatalf("bob's delivery came from %q, not a shared group durable", dur)
	}
	testMQTTDisconnect(t, c, nil)
	if ci := ackLayoutConsumer(t, js, mqttStreamName, dur); ci.NumAckPending != 1 {
		t.Fatalf("the group holds %d unacknowledged messages, want 1", ci.NumAckPending)
	}

	// bob resumes on D as a user denied the subject. The re-offer at AckWait reaches the
	// exchange D recovered, so the deny installs a HandBack and the source is handed back.
	bobd := *bob
	bobd.user = "bobd"
	cd, rd, sp := ackLayoutConnect(t, db, &bobd)
	if !sp {
		t.Fatal("expected bob's session to be present on D")
	}
	var nak ackObservation
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		for _, ob := range obs.of(mqttStreamName, dur, sseq) {
			if ob.negative() {
				nak = ob
				return nil
			}
		}
		return fmt.Errorf("no hand-back of %s seq %d yet", dur, sseq)
	})
	_, _, dc, _, _ := ackReplyInfo(nak.subject)
	if want := string(mqttPacedRefusalNak(dc)); nak.payload != want {
		t.Fatalf("hand-back on %q carried %q, want the paced refusal %q", nak.subject, nak.payload, want)
	}
	assertParsersRead(t, nak, mqttStreamName, dur, sseq)
	if d, n, shared, malformed := mqttSourceAckScope(nak.subject); !shared || malformed || d != dur || n != sseq {
		t.Fatalf("mqttSourceAckScope(%q) = %q, %d, shared=%v, malformed=%v; want the group source in scope",
			nak.subject, d, n, shared, malformed)
	}
	sess := ackLayoutSession(t, db, bob.clientID)
	sess.mu.Lock()
	held := sess.sourceHandBackHeldLocked(dur, sseq)
	sess.mu.Unlock()
	if !held {
		t.Fatal("D's session holds no HandBack for the denied source")
	}

	// The gate, on the held source's real reply: a positive acknowledgement is refused and
	// handed back instead. Attributed by the EXACT reply: an automatic refusal of a later
	// redelivery carries a different delivery count, so it cannot stand in for this one.
	pacedOn := func(subject string) int {
		n := 0
		for _, ob := range obs.of(mqttStreamName, dur, sseq) {
			if ob.subject == subject && ob.payload == string(mqttPacedRefusalNak(dc)) {
				n++
			}
		}
		return n
	}
	before := pacedOn(nak.subject)
	if res, err := sess.submitSourceAck(nak.subject, false, nil); err != nil || res != mqttSourceAckHandedBack {
		t.Fatalf("submitSourceAck on a held source = %v, %v; want it handed back", res, err)
	}
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if pacedOn(nak.subject) <= before {
			return fmt.Errorf("the gate sent no paced hand-back on %q", nak.subject)
		}
		return nil
	})
	if n := obs.positives(mqttStreamName, dur, sseq); n != 0 {
		t.Fatalf("the held source was positively acknowledged %d times", n)
	}
	if ci := ackLayoutConsumer(t, js, mqttStreamName, dur); ci.AckFloor.Stream >= sseq {
		t.Fatalf("the group's ack floor reached %d while the source was held", ci.AckFloor.Stream)
	}

	// alice joins, then bob leaves; her delivery may arrive at any point after her SUBACK.
	ca, ra, _ := ackLayoutConnect(t, cb, &mqttV5ConnInfo{clientID: "ack-alice4", cleanStart: true, user: "alice", pass: "pw"})
	testMQTTSubV5(t, ca, ra, 1, []mqttV5SubFilter{{topic: filter, opts: 1}})
	testMQTTUnsubV5(t, cd, rd, 2, []string{filter})
	apI := ackLayoutReadPublish(t, ra, mqttPacedRefusalMaxDelay+10*time.Second, topic)
	testMQTTSendPIPacket(mqttPacketPubAck, t, ca, apI)
	ob := obs.awaitPositive(t, 15*time.Second, mqttStreamName, dur, sseq)
	assertParsersRead(t, ob, mqttStreamName, dur, sseq)
	awaitSettled(t, js, 15*time.Second, mqttStreamName, dur, sseq)
	testMQTTDisconnect(t, ca, nil)
	testMQTTDisconnect(t, cd, nil)
}
