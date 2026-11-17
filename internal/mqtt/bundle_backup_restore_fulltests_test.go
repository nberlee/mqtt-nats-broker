//go:build mqtt_fulltests

package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// The supported backup and restore of the broker-owned bundle (docs/OPERATIONS.md,
// "Backup and restore"): every broker stopped, all seven streams backed up with their
// consumers, and a restore of all seven from one set before any broker starts. These
// tests drive the procedure against real nats-servers whose lifetime they own, so a
// broker can be stopped while its server keeps serving the backup.

// testBackupNATS starts a standalone JetStream server on dir and connects to it. The
// returned stop shuts it down (idempotent), for the cold-copy case that needs the store
// at rest.
func testBackupNATS(t *testing.T, dir string) (*natsserver.Server, *nats.Conn, func()) {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dir,
	})
	if err != nil {
		t.Fatalf("new nats-server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		t.Fatal("nats-server not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatalf("connect: %v", err)
	}
	stop := func() {
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
	}
	t.Cleanup(stop)
	return ns, nc, stop
}

// testBroker is one broker process of these tests: its server, its options (the MQTT
// port is bound by Start), and stop, which the procedure's first step calls and the
// test cleanup calls again harmlessly.
type testBroker struct {
	s    *Server
	o    *Options
	stop func()
}

// testBackupBroker starts one broker on ns with its own NATS connection, as a
// separate process would.
func testBackupBroker(t *testing.T, ns *natsserver.Server, node string) testBroker {
	t.Helper()
	b, err := testBackupBrokerStart(t, ns, node, mqttBundleSettleBudget)
	if err != nil {
		t.Fatalf("broker %s: %v", node, err)
	}
	return b
}

func testBackupBrokerStart(t *testing.T, ns *natsserver.Server, node string, settle time.Duration) (testBroker, error) {
	t.Helper()
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	o := testMQTTDefaultOptionsV5()
	s := NewServer(nc, o, node)
	s.bundleSettleBudget = settle
	if err := s.Start(context.Background()); err != nil {
		nc.Close()
		return testBroker{}, err
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			<-s.BeginShutdown()
			nc.Close()
		})
	}
	t.Cleanup(stop)
	if err := s.readyForConnections(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	return testBroker{s: s, o: o, stop: stop}, nil
}

// testV5PropsMessageExpiry is a PUBLISH property block carrying only a Message Expiry
// Interval.
func testV5PropsMessageExpiry(secs uint32) []byte {
	w := newMQTTWriter(0)
	w.WriteByte(mqttPropMessageExpiry)
	w.WriteUint32(secs)
	return w.Bytes()
}

// testBackupPacket is one packet a client read: a PUBLISH (topic, payload, QoS, DUP)
// or a PUBREL, by packet identifier.
type testBackupPacket struct {
	typ     byte
	dup     bool
	qos     byte
	pi      uint16
	topic   string
	payload string
}

// testBackupReadPacket reads one packet within d, decoding a PUBLISH or a PUBREL;
// any other packet type fails the test, since nothing else is owed to these clients.
func testBackupReadPacket(t *testing.T, r *mqttReader, d time.Duration) (testBackupPacket, bool) {
	t.Helper()
	b, pl, ok := testMQTTReadPacketReady(r, d)
	if !ok {
		return testBackupPacket{}, false
	}
	start := r.pos
	switch b & mqttPacketMask {
	case mqttPacketPub:
		p := testBackupPacket{typ: mqttPacketPub, dup: b&mqttPubFlagDup != 0, qos: mqttGetQoS(b & mqttPacketFlagMask)}
		topic, err := r.readBytes("topic", false)
		if err != nil {
			t.Fatalf("PUBLISH topic: %v", err)
		}
		p.topic = string(topic)
		if p.qos > 0 {
			if p.pi, err = r.readUint16("pi"); err != nil {
				t.Fatalf("PUBLISH pi: %v", err)
			}
		}
		if _, err := r.readProperties(mqttPropsContextPubOut); err != nil {
			t.Fatalf("PUBLISH properties: %v", err)
		}
		n := pl - (r.pos - start)
		p.payload = string(r.buf[r.pos : r.pos+n])
		r.pos += n
		return p, true
	case mqttPacketPubRel:
		pi, err := r.readUint16("pubrel pi")
		if err != nil {
			t.Fatalf("PUBREL pi: %v", err)
		}
		r.pos = start + pl
		return testBackupPacket{typ: mqttPacketPubRel, pi: pi}, true
	default:
		t.Fatalf("unexpected packet type %x", b&mqttPacketMask)
		return testBackupPacket{}, false
	}
}

// testBackupCollect reads packets until every key in want has been seen or d elapses,
// and returns every packet read. A PUBLISH's key is its payload, a PUBREL's
// "PUBREL <pi>".
func testBackupCollect(t *testing.T, r *mqttReader, want []string, d time.Duration) map[string][]testBackupPacket {
	t.Helper()
	got := map[string][]testBackupPacket{}
	deadline := time.Now().Add(d)
	missing := func() []string {
		var m []string
		for _, k := range want {
			if len(got[k]) == 0 {
				m = append(m, k)
			}
		}
		return m
	}
	for len(missing()) > 0 {
		left := time.Until(deadline)
		if left <= 0 {
			t.Fatalf("still owed %v after %v; read %v", missing(), d, got)
		}
		p, ok := testBackupReadPacket(t, r, left)
		if !ok {
			continue
		}
		key := p.payload
		if p.typ == mqttPacketPubRel {
			key = "PUBREL " + strconv.Itoa(int(p.pi))
		}
		got[key] = append(got[key], p)
	}
	return got
}

// testBackupNothingMore reads for d and fails on any packet: what the bounded
// exactly-once windows assert.
func testBackupNothingMore(t *testing.T, r *mqttReader, d time.Duration, why string) {
	t.Helper()
	if p, ok := testBackupReadPacket(t, r, d); ok {
		t.Fatalf("%s: read %+v", why, p)
	}
}

// testBackupState is what the captured bundle holds, and the identifiers the restore
// must give back.
type testBackupState struct {
	q1Unacked uint16 // QoS 1 to S awaiting PUBACK
	q2NoRec   uint16 // QoS 2 to S awaiting PUBREC
	q2Rel     uint16 // QoS 2 to S whose PUBREL awaits PUBCOMP
	expPI     uint16 // QoS 1 to S whose source expires between the snapshots
	expSeq    uint64 // that source's $MQTT_msgs sequence
	willAt    time.Time
}

// testBackupRetransmitWithin bounds the wait for a restored session's retransmissions.
// After a restart the broker has no live acknowledgement subject to NAK for a delivery it
// rebuilt from its register, so the redelivery waits for the consumer's AckWait
// (resendPendingRedelivery); a restore that kept the consumer's pending state, a cold copy,
// retransmits up to one AckWait after the original delivery.
const testBackupRetransmitWithin = mqttDefaultAckWait + 10*time.Second

const (
	testBackupWillDelay  = 15 // seconds; long enough to stay pending across the broker's stop
	testBackupExpiry     = 8  // seconds; long enough to outlive the setup to the first snapshot
	testBackupSessExpiry = 300
)

// testBackupBuildState drives broker A into every state the procedure promises to
// carry: outbound QoS 1 awaiting PUBACK, QoS 2 awaiting PUBREC and PUBCOMP, a source
// that will expire between the snapshots, a queued delivery for an offline session,
// a retained message, an open inbound QoS 2 hold and a pending delayed Will.
func testBackupBuildState(t *testing.T, b testBroker) testBackupState {
	t.Helper()
	var st testBackupState
	host, port := b.o.MQTT.Host, b.o.MQTT.Port

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bk-s", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{
		{topic: "bk/q1", opts: 1}, {topic: "bk/q2", opts: 2}, {topic: "bk/will", opts: 1}, {topic: "bk/exp", opts: 1},
	})

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bk-p", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	testMQTTReadConnAckV5(t, rp)

	testMQTTPubV5Props(t, cp, rp, 1, false, 1, "bk/q1", []byte("q1-unacked"), nil)
	_, st.q1Unacked = testMQTTReadPublishV5(t, rs, "bk/q1", []byte("q1-unacked"))

	testMQTTPubV5Props(t, cp, rp, 2, false, 2, "bk/q2", []byte("q2-norec"), nil)
	_, st.q2NoRec = testMQTTReadPublishV5(t, rs, "bk/q2", []byte("q2-norec"))

	testMQTTPubV5Props(t, cp, rp, 2, false, 3, "bk/q2", []byte("q2-rel"), nil)
	_, st.q2Rel = testMQTTReadPublishV5(t, rs, "bk/q2", []byte("q2-rel"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, cs, st.q2Rel)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rs, st.q2Rel)

	testMQTTPublishV5Props(t, cp, 1, 4, "bk/exp", testV5PropsMessageExpiry(testBackupExpiry), []byte("exp"))
	testMQTTReadPubAck(t, rp, 4)
	_, st.expPI = testMQTTReadPublishV5(t, rs, "bk/exp", []byte("exp"))

	// S goes away without a DISCONNECT, its session kept for its expiry interval.
	cs.Close()
	testBackupWaitDetached(t, b.s, "bk-s")

	testMQTTPubV5Props(t, cp, rp, 1, false, 5, "bk/q1", []byte("q1-queued"), nil)
	testMQTTPubV5Props(t, cp, rp, 1, true, 6, "bk/ret/a", []byte("ret-a"), nil)

	// The open inbound hold: PUBREC read, PUBREL never sent.
	testMQTTPublishV5(t, cp, 2, 7, "bk/q2", []byte("q2-held"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, rp, 7)
	cp.Close()

	will := &mqttWill{topic: []byte("bk/will"), message: []byte("w-bye"), qos: 1}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bk-w", cleanStart: true, will: will,
		willProps: mqttV5WillDelayProps(testBackupWillDelay), props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	testMQTTReadConnAckV5(t, rw)
	cw.Close()
	st.willAt = time.Now()
	return st
}

// testBackupWaitDetached waits until the broker no longer holds clientID's
// connection, so what is published next is queued for the offline session.
func testBackupWaitDetached(t *testing.T, s *Server, clientID string) {
	t.Helper()
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, c := range s.clients {
			c.mu.Lock()
			live := c.isMqtt() && c.mqtt.cid == clientID
			c.mu.Unlock()
			if live {
				return fmt.Errorf("%q still connected", clientID)
			}
		}
		return nil
	})
}

// testBackupWillPending asserts W's delayed Will is still pending in $MQTT_willq.
func testBackupWillPending(t *testing.T, js nats.JetStreamContext) {
	t.Helper()
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		m, err := js.GetLastMsg(mqttWillQStreamName, mqttWillQSubjectPrefix+getHash("bk-w")+".*")
		if err != nil {
			return fmt.Errorf("no Will outbox record: %v", err)
		}
		if !strings.Contains(string(m.Data), `"state":"pending"`) {
			return fmt.Errorf("Will outbox record not pending: %s", m.Data)
		}
		return nil
	})
}

// testBackupConsumers lists every consumer of the seven streams: durable names with
// their creation time, and the names and inactivity thresholds of the ephemerals.
type testBackupConsumers struct {
	durables   map[string]time.Time
	durableIT  map[string]time.Duration
	ephemerals map[string]time.Duration
}

func testBackupListConsumers(t *testing.T, js nats.JetStreamContext) testBackupConsumers {
	t.Helper()
	out := testBackupConsumers{durables: map[string]time.Time{}, durableIT: map[string]time.Duration{}, ephemerals: map[string]time.Duration{}}
	for _, stream := range mqttTestBrokerStreamNames {
		for ci := range js.ConsumersInfo(stream) {
			key := stream + " > " + ci.Name
			if ci.Config.Durable != _EMPTY_ {
				out.durables[key] = ci.Created
				out.durableIT[key] = ci.Config.InactiveThreshold
			} else {
				out.ephemerals[key] = ci.Config.InactiveThreshold
			}
		}
	}
	return out
}

func testBackupStoreDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp(t.TempDir(), "js")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// testBackupInflightRegister reads the in-flight register S's session holds for pi.
func testBackupInflightRegister(t *testing.T, js nats.JetStreamContext, pi uint16) (dur string, seq uint64) {
	t.Helper()
	m, err := js.GetLastMsg(mqttSessStreamName, mqttInflightStreamSubjectPrefix+getHash("bk-s")+".*."+strconv.Itoa(int(pi)))
	if err != nil {
		t.Fatalf("no in-flight register for identifier %d: %v", pi, err)
	}
	var rec struct {
		Dur string `json:"dur"`
		Seq uint64 `json:"seq"`
	}
	if err := json.Unmarshal(m.Data, &rec); err != nil || rec.Seq == 0 {
		t.Fatalf("in-flight register %s: %q (%v)", m.Subject, m.Data, err)
	}
	return rec.Dur, rec.Seq
}

func testBackupSourcePresent(js nats.JetStreamContext, seq uint64) bool {
	_, err := js.GetMsg(mqttStreamName, seq)
	return err == nil
}

// A backup taken with every broker stopped, restored before any broker starts, serves
// every state it captured: the procedure in docs/OPERATIONS.md, "Backup and restore",
// once into a fresh server (disaster recovery) and once into the same account after
// the seven were deleted (a rollback).
func TestBackupRestoreServesTheBundleItCaptured(t *testing.T) {
	t.Parallel()
	for _, into := range []string{"a fresh server", "the same account after deleting the seven"} {
		t.Run("into "+into, func(t *testing.T) {
			t.Parallel()
			ns1, nc1, _ := testBackupNATS(t, testBackupStoreDir(t))
			js1, _ := nc1.JetStream()
			a := testBackupBroker(t, ns1, "bk-a")
			st := testBackupBuildState(t, a)

			// Step 1: every broker stopped. The Will must still be pending after the
			// stop, or the test proves only that a Will fires, not that a pending one
			// survives the restore.
			a.stop()
			testBackupWillPending(t, js1)

			// The expiry boundary: the register is captured with $MQTT_sess while
			// its source still exists, and the source is gone before $MQTT_msgs is
			// captured — an outage's progression between two members' snapshots.
			if _, seq := testBackupInflightRegister(t, js1, st.expPI); !testBackupSourcePresent(js1, seq) {
				t.Fatalf("the expiring source %d was gone before the first snapshot: raise testBackupExpiry", seq)
			} else {
				st.expSeq = seq
			}
			before := testBackupListConsumers(t, js1)
			set := map[string]testStreamArchive{mqttSessStreamName: testSnapshotStream(t, nc1, mqttSessStreamName, false)}
			checkFor(t, 3*testBackupExpiry*time.Second, 100*time.Millisecond, func() error {
				if testBackupSourcePresent(js1, st.expSeq) {
					return fmt.Errorf("source %d not expired yet", st.expSeq)
				}
				return nil
			})
			for _, name := range mqttTestBrokerStreamNames[1:] {
				set[name] = testSnapshotStream(t, nc1, name, false)
			}
			// The Will falls due while every broker is stopped.
			time.Sleep(time.Until(st.willAt.Add(testBackupWillDelay*time.Second + time.Second)))

			ns2, nc2 := ns1, nc1
			if into == "a fresh server" {
				ns2, nc2, _ = testBackupNATS(t, testBackupStoreDir(t))
			} else {
				testDeleteBundle(t, nc1)
			}
			testRestoreBundle(t, nc2, set)
			js2, _ := nc2.JetStream()

			// The restored consumers, read before any broker runs: every durable of
			// run A is back with its inactivity threshold zero, and every ephemeral
			// of run A expires by its own threshold.
			restored := testBackupListConsumers(t, js2)
			for key := range before.durables {
				if _, ok := restored.durables[key]; !ok {
					t.Fatalf("durable %s was not restored (restored: %v)", key, restored.durables)
				}
				if it := restored.durableIT[key]; it != 0 {
					t.Fatalf("restored durable %s has inactivity threshold %v, want 0", key, it)
				}
			}
			for key, it := range restored.ephemerals {
				if it <= 0 {
					t.Fatalf("restored ephemeral %s of the stopped run has no inactivity threshold", key)
				}
			}

			b := testBackupBroker(t, ns2, "bk-b")
			testBackupServesTheCapturedState(t, b, st)

			// Adopted, not recreated: every restored durable still carries the creation
			// time it had before broker B started; B's own ephemerals are new names.
			after := testBackupListConsumers(t, js2)
			for key, created := range restored.durables {
				got, ok := after.durables[key]
				if !ok {
					t.Fatalf("restored durable %s is gone after broker B served the session", key)
				}
				if !got.Equal(created) {
					t.Fatalf("durable %s was recreated by broker B (created %v, restored %v)", key, got, created)
				}
			}
			own := 0
			for key := range after.ephemerals {
				if _, restoredOne := restored.ephemerals[key]; !restoredOne {
					own++
				}
			}
			if own == 0 {
				t.Fatalf("broker B serves with no ephemeral observer of its own (after %v, restored %v)",
					after.ephemerals, restored.ephemerals)
			}
		})
	}
}

// testBackupServesTheCapturedState is what a restored bundle owes S, P, W and a
// retained reader, served by broker b.
func testBackupServesTheCapturedState(t *testing.T, b testBroker, st testBackupState) {
	t.Helper()
	host, port := b.o.MQTT.Host, b.o.MQTT.Port

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bk-s", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	defer func() { cs.Close() }()
	if present, rc, _ := testMQTTReadConnAckV5(t, rs); !present || rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("S resumed with session present %v, reason %x; want the restored session", present, rc)
	}
	got := testBackupCollect(t, rs, []string{
		"q1-unacked", "q2-norec", "PUBREL " + strconv.Itoa(int(st.q2Rel)), "q1-queued", "w-bye",
	}, testBackupRetransmitWithin)
	for key, want := range map[string]uint16{"q1-unacked": st.q1Unacked, "q2-norec": st.q2NoRec} {
		p := got[key][0]
		if !p.dup || p.pi != want {
			t.Fatalf("%s retransmitted with DUP %v identifier %d, want DUP and %d", key, p.dup, p.pi, want)
		}
	}
	if n := len(got["exp"]); n != 0 {
		t.Fatalf("the delivery whose source expired was sent again %d times; its payload is gone", n)
	}

	// The identifier of the delivery whose source expired between the snapshots is
	// reserved until S acknowledges it (inflight_register.go, a possibly transmitted
	// PUBLISH over a source nothing can re-offer).
	held := func() bool {
		sess := testMQTTGetClient(t, b.s, "bk-s").mqtt.sess
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.pendingPublish[st.expPI] != nil || sess.custody.holdsPI(st.expPI) || sess.sendReservedLocked(st.expPI)
	}
	if !held() {
		t.Fatalf("identifier %d may be held by S; the restored session must keep it reserved", st.expPI)
	}

	testMQTTSendPIPacket(mqttPacketPubAck, t, cs, st.q1Unacked)
	testMQTTSendPIPacket(mqttPacketPubComp, t, cs, st.q2Rel)
	testMQTTSendPIPacket(mqttPacketPubRec, t, cs, st.q2NoRec)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rs, st.q2NoRec)
	testMQTTSendPIPacket(mqttPacketPubComp, t, cs, st.q2NoRec)
	testMQTTSendPIPacket(mqttPacketPubAck, t, cs, got["q1-queued"][0].pi)
	testMQTTSendPIPacket(mqttPacketPubAck, t, cs, got["w-bye"][0].pi)
	testMQTTSendPIPacket(mqttPacketPubAck, t, cs, st.expPI)
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		if held() {
			return fmt.Errorf("identifier %d still reserved after S acknowledged it", st.expPI)
		}
		return nil
	})

	// P completes its held QoS 2 PUBLISH; S receives it once.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bk-p", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	defer cp.Close()
	if present, _, _ := testMQTTReadConnAckV5(t, rp); !present {
		t.Fatal("P resumed without its session")
	}
	testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, cp, 7)
	testMQTTReadPIPacket(mqttPacketPubComp, t, rp, 7)
	held2 := testBackupCollect(t, rs, []string{"q2-held"}, 10*time.Second)["q2-held"][0]
	testMQTTSendPIPacket(mqttPacketPubRec, t, cs, held2.pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rs, held2.pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, cs, held2.pi)
	testBackupNothingMore(t, rs, 2*time.Second, "after completing every restored exchange")
	cs.Close()
	cs, rs = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bk-s", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, host, port)
	testMQTTReadConnAckV5(t, rs)
	testBackupNothingMore(t, rs, 2*time.Second, "a reconnect after every exchange completed")

	// Retained: a new literal and a new wildcard subscriber read the restored message;
	// one published after the restore is replayed to a further new wildcard
	// subscriber, which only the new run's retained index can serve.
	for i, filter := range []string{"bk/ret/a", "bk/ret/#"} {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bk-r" + strconv.Itoa(i), cleanStart: true}, host, port)
		testMQTTReadConnAckV5(t, r)
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 0}})
		testMQTTReadPubCheckRetainV5(t, c, r, "bk/ret/a", []byte("ret-a"), true)
		c.Close()
	}
	testMQTTPubV5Props(t, cp, rp, 1, true, 8, "bk/ret/b", []byte("ret-b"), nil)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "bk-r9", cleanStart: true}, host, port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "bk/ret/#", opts: 0}})
	seen := map[string]bool{}
	for range 2 {
		_, topic, payload, _, _ := testMQTTReadPubV5Packet(t, r)
		seen[topic+"="+string(payload)] = true
	}
	if !seen["bk/ret/a=ret-a"] || !seen["bk/ret/b=ret-b"] {
		t.Fatalf("a new wildcard subscriber after the restore read %v, want both retained messages", seen)
	}
}

// A restore of six of the seven is refused at broker start, and the missing member is
// not created: the procedure's "all seven" is enforced, not advised.
func TestRestoreRefusesAPartialBundle(t *testing.T) {
	t.Parallel()
	ns, nc, _ := testBackupNATS(t, testBackupStoreDir(t))
	a := testBackupBroker(t, ns, "part-a")
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "part-s", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, a.o.MQTT.Host, a.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	c.Close()
	a.stop()

	set := testSnapshotBundle(t, nc)
	testDeleteBundle(t, nc)
	var six []string
	for _, name := range mqttTestBrokerStreamNames {
		if name != mqttQoS2AssignStreamName {
			six = append(six, name)
		}
	}
	testRestoreBundle(t, nc, set, six...)

	_, err := testBackupBrokerStart(t, ns, "part-b", 0)
	if err == nil {
		t.Fatal("a broker started on six of the seven restored streams; the bundle must be refused")
	}
	if !strings.Contains(err.Error(), "of the 7 broker-owned streams") {
		t.Fatalf("refused for another reason: %v", err)
	}
	js, _ := nc.JetStream()
	if _, serr := js.StreamInfo(mqttQoS2AssignStreamName); serr == nil {
		t.Fatalf("the refused start created %s", mqttQoS2AssignStreamName)
	}
}

// Members of two backup sets taken across a reset carry different recovery epochs, and
// the broker refuses the mix: the check that keeps a restore from assembling a bundle
// out of two incarnations.
func TestRestoreRefusesMembersOfTwoIncarnations(t *testing.T) {
	t.Parallel()
	ns, nc, _ := testBackupNATS(t, testBackupStoreDir(t))
	testBackupBroker(t, ns, "inc-a").stop()
	x := testSnapshotBundle(t, nc)

	testDeleteBundle(t, nc)
	testBackupBroker(t, ns, "inc-b").stop()
	y := testSnapshotBundle(t, nc)
	if x[mqttSessStreamName].config.Metadata[mqttSessEpochMetaKey] == y[mqttSessStreamName].config.Metadata[mqttSessEpochMetaKey] {
		t.Fatal("the reset did not mint a new recovery epoch; the test cannot reach the epoch check")
	}

	testDeleteBundle(t, nc)
	testRestoreBundle(t, nc, y, mqttSessStreamName)
	testRestoreBundle(t, nc, x, mqttTestBrokerStreamNames[1:]...)
	_, err := testBackupBrokerStart(t, ns, "inc-c", 0)
	if err == nil {
		t.Fatal("a broker started on a bundle assembled from two incarnations")
	}
	if !strings.Contains(err.Error(), "belongs to a different incarnation of the broker-owned bundle") {
		t.Fatalf("refused for another reason: %v", err)
	}
}

// A restore that fails part-way can leave its stream present, stamped and configured
// like a complete one but missing records, and the broker cannot tell: the reason the
// procedure restarts a restore from scratch on any failure (docs/CONFORMANCE.md).
func TestFailedRestoreCanBeAdmitted(t *testing.T) {
	t.Parallel()
	ns, nc, _ := testBackupNATS(t, testBackupStoreDir(t))
	a := testBackupBroker(t, ns, "fail-a")
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "fail-p", cleanStart: true}, a.o.MQTT.Host, a.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	// Enough incompressible retained payload for the archive to span several S2
	// blocks, so a cut at a chunk end lands between records.
	const n = 64
	payload := func(i int) []byte {
		b := make([]byte, 64*1024)
		x := uint32(i*2654435761 + 1)
		for j := range b {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			b[j] = byte(x)
		}
		return b
	}
	for i := range n {
		testMQTTPubV5Props(t, c, r, 1, true, uint16(i+1), "fail/big/"+strconv.Itoa(i), payload(i), nil)
	}
	c.Close()
	a.stop()

	set := testSnapshotBundle(t, nc)
	testDeleteBundle(t, nc)
	testRestoreBundle(t, nc, set, mqttTestBrokerStreamNames[:len(mqttTestBrokerStreamNames)-1]...)

	// Cut $MQTT_rmsgs' archive at a chunk end whose decoded prefix holds the stream
	// metadata and early records but not the last retained topic.
	rm := set[mqttRetainedMsgsStreamName]
	if rm.name != mqttTestBrokerStreamNames[len(mqttTestBrokerStreamNames)-1] {
		t.Fatalf("the cut member must be the last restored, got %s", rm.name)
	}
	last := []byte("fail/big/" + strconv.Itoa(n-1))
	var cut []byte
	for _, end := range testS2ChunkEnds(rm.data) {
		dec, err := testS2Decode(rm.data[:end])
		if err != nil {
			t.Fatalf("prefix to chunk end %d does not decode: %v", end, err)
		}
		if end > len(rm.data)/3 && strings.Contains(string(dec), "fail/big/0") && !strings.Contains(string(dec), string(last)) {
			cut = rm.data[:end]
			break
		}
	}
	if cut == nil {
		t.Fatal("no chunk end separates the early records from the last one; the archive is too small to cut")
	}
	if err := testRestoreStream(nc, rm, cut); err == nil {
		t.Fatal("a restore from a cut archive reported success; the operator would have no signal")
	}

	js, _ := nc.JetStream()
	si, err := js.StreamInfo(mqttRetainedMsgsStreamName)
	if err != nil {
		t.Fatalf("the failed restore left no %s; the residual this pins is a PRESENT partial stream: %v", mqttRetainedMsgsStreamName, err)
	}
	if si.Config.Metadata[mqttStoreFormatMetaKey] != mqttStoreFormat || si.Config.Metadata[mqttSessEpochMetaKey] == _EMPTY_ {
		t.Fatalf("the partial stream lost its stamps: %v", si.Config.Metadata)
	}
	if _, err := js.GetLastMsg(mqttRetainedMsgsStreamName, mqttRetainedMsgsStreamSubject+"fail.big."+strconv.Itoa(n-1)); err == nil {
		t.Fatal("the last retained record is present; the cut did not leave the stream partial")
	}
	if _, err := testBackupBrokerStart(t, ns, "fail-b", 0); err != nil {
		t.Fatalf("the broker refused the partially restored bundle (%v); the residual says it is admitted", err)
	}
}

// A rolling restart within one build keeps every session: each broker in turn stops
// and a replacement starts while the other serves. It witnesses the same-build
// replacement only; two releases of one format coexisting is the contract stated in
// docs/ARCHITECTURE.md, not something one build can test.
func TestRollingRestartWithinOneBuildKeepsSessions(t *testing.T) {
	t.Parallel()
	ns, _, _ := testBackupNATS(t, testBackupStoreDir(t))
	b1 := testBackupBroker(t, ns, "roll-1")
	b2 := testBackupBroker(t, ns, "roll-2")
	o1, o2 := b1.o, b2.o

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "roll-s", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, o1.MQTT.Host, o1.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "roll/t", opts: 1}})

	b1.stop()
	c.Close()
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "roll-p", cleanStart: true}, o2.MQTT.Host, o2.MQTT.Port)
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 1, false, 1, "roll/t", []byte("while-1-down"), nil)
	cp.Close()

	c, r = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "roll-s", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, o2.MQTT.Host, o2.MQTT.Port)
	if present, _, _ := testMQTTReadConnAckV5(t, r); !present {
		t.Fatal("the session did not survive its broker's stop")
	}
	_, pi := testMQTTReadPublishV5(t, r, "roll/t", []byte("while-1-down"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)

	o1r := testBackupBroker(t, ns, "roll-1r").o
	b2.stop()
	c.Close()
	c, r = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "roll-s", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(testBackupSessExpiry)}, o1r.MQTT.Host, o1r.MQTT.Port)
	defer c.Close()
	if present, _, _ := testMQTTReadConnAckV5(t, r); !present {
		t.Fatal("the session did not survive the second step of the rolling restart")
	}
	cp, rp = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "roll-p", cleanStart: true}, o1r.MQTT.Host, o1r.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 1, false, 2, "roll/t", []byte("after-roll"), nil)
	testMQTTReadPublishV5(t, r, "roll/t", []byte("after-roll"))
}

// testBackupCopyDir copies a stopped server's store directory.
func testBackupCopyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	})
	if err != nil {
		t.Fatalf("copy %s: %v", src, err)
	}
}

// A copy of the store taken with the broker and its server stopped serves the bundle
// it holds: the backup the embedded topology uses (docs/OPERATIONS.md, "Cold copy").
func TestColdStoreCopyServesTheBundle(t *testing.T) {
	t.Parallel()
	dir := testBackupStoreDir(t)
	ns1, _, stop1 := testBackupNATS(t, dir)
	a := testBackupBroker(t, ns1, "cold-a")
	st := testBackupBuildState(t, a)
	a.stop()
	stop1()

	copyDir := testBackupStoreDir(t)
	testBackupCopyDir(t, dir, copyDir)
	time.Sleep(time.Until(st.willAt.Add(testBackupWillDelay*time.Second + time.Second)))

	ns2, _, _ := testBackupNATS(t, copyDir)
	testBackupServesTheCapturedState(t, testBackupBroker(t, ns2, "cold-b"), st)
}
