//go:build mqtt_fulltests

package mqtt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The persisted-format golden fixtures (docs/ARCHITECTURE.md, "The persisted format").
//
// testdata/storeformat/<format>/ holds a nats-server store directory a broker of the
// format's build wrote while running storeFormatScenario, beside an `outcome` file
// naming what every later build must do with it: `read` or `refuse`. A store
// directory rather than a logical dump, because sequences are identities in this
// design and a replay would renumber them.
//
// Until the first release format 1 is not frozen, and its fixture is regenerated with
// any change to it:
//
//	MQTTNB_WRITE_STORE_FORMAT_FIXTURE=1 go test -tags mqtt_fulltests -run TestWriteStoreFormatFixture ./internal/mqtt/
//
// From the first release a released format's fixture is never regenerated: a
// breaking change bumps the format, and the old fixture's outcome records what the
// new build does with it.

const storeFormatFixtureRoot = "testdata/storeformat"

// The scenario's names. Each step leaves state on a named stream; the reader reads
// that state back through the broker, never by decoding the store itself.
const (
	goldenSub     = "golden-sub"    // persistent, golden/q at QoS 1 and golden/q2 at QoS 2
	goldenPub     = "golden-pub"    // publisher, clean session
	goldenQ2In    = "golden-q2in"   // inbound QoS 2 held: PUBREC received, PUBREL withheld
	goldenQ2Out   = "golden-q2out"  // outbound QoS 2: PUBREC sent, PUBCOMP withheld
	goldenShared  = "golden-shared" // MQTT 5 shared member, never-expiring session
	goldenGone    = "golden-gone"   // a session discarded by a clean start: a teardown watermark
	goldenGroup   = "goldengrp"
	goldenPayload = "golden"
)

// storeFormatStartNATS starts an embedded nats-server on dir.
func storeFormatStartNATS(t *testing.T, dir string) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dir, NoSigs: true,
	})
	if err != nil {
		t.Fatalf("nats-server could not open the store at %s (a backend failure, not a format verdict): %v", dir, err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(30 * time.Second) {
		ns.Shutdown()
		t.Fatalf("nats-server did not become ready on the store at %s (a backend failure, not a format verdict)", dir)
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatalf("connecting to the fixture's nats-server: %v", err)
	}
	return ns, nc
}

// storeFormatStopNATS stops the embedded server and waits for its stores to close.
func storeFormatStopNATS(nc *nats.Conn, ns *natsserver.Server) {
	nc.Close()
	ns.Shutdown()
	ns.WaitForShutdown()
}

// storeFormatBroker starts a broker on nc; Start's error is returned, not fatal, so
// the reader can judge a refusal.
func storeFormatBroker(nc *nats.Conn, node string) (*Server, *Options, error) {
	o := &Options{MaxPayload: MAX_PAYLOAD_SIZE}
	o.MQTT.Host = "127.0.0.1"
	o.MQTT.Port = -1
	o.MQTT.StreamReplicas = 1
	s := NewServer(nc, o, node)
	return s, o, s.Start(context.Background())
}

// storeFormatStopBroker stops a broker and waits for its WHOLE lifecycle, failing on
// timeout: Shutdown returns at ShutdownDrainTimeout while persistence continues, so
// a store copied after it could miss the broker's last writes.
func storeFormatStopBroker(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := s.ShutdownContext(ctx); err != nil {
		t.Fatalf("the broker did not complete its shutdown: %v", err)
	}
}

func storeFormatConnect(t *testing.T, o *Options, id string, clean bool) (net.Conn, *mqttReader) {
	t.Helper()
	return testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: clean}, o.MQTT.Host, o.MQTT.Port)
}

func storeFormatConnectShared(t *testing.T, o *Options) (net.Conn, *mqttReader, bool) {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   goldenShared,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(mqttSessionNeverExpire),
	}, o.MQTT.Host, o.MQTT.Port)
	present, rc, _ := testMQTTReadConnAckV5(t, r)
	if rc != 0 {
		t.Fatalf("%s CONNACK reason %#x", goldenShared, rc)
	}
	return c, r, present
}

// storeFormatScenario drives a fresh broker into the state the fixture freezes.
func storeFormatScenario(t *testing.T, o *Options) {
	t.Helper()
	// 1. A persistent subscriber, then a QoS 1 message queued for it while it is away.
	c, r := storeFormatConnect(t, o, goldenSub, false)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "golden/q", qos: 1}, {filter: "golden/q2", qos: 2}}, []byte{1, 2})
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	pc, pr := storeFormatConnect(t, o, goldenPub, true)
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "golden/q", 1, []byte(goldenPayload))
	// 2. A retained message.
	testMQTTPublish(t, pc, pr, 1, false, true, "golden/retained", 2, []byte(goldenPayload))

	// 3. An inbound QoS 2 exchange held between PUBREC and PUBREL.
	ic, ir := storeFormatConnect(t, o, goldenQ2In, false)
	testMQTTCheckConnAck(t, ir, mqttConnAckRCConnectionAccepted, false)
	testMQTTSendPublishPacket(t, ic, 2, false, false, "golden/q2", 7, []byte(goldenPayload))
	testMQTTReadPIPacket(mqttPacketPubRec, t, ir, 7)
	testMQTTDisconnect(t, ic, nil)
	ic.Close()

	// 4. An outbound QoS 2 exchange: the client acknowledged receipt (PUBREC), the
	//    broker sent PUBREL, the client withholds PUBCOMP.
	oc, or := storeFormatConnect(t, o, goldenQ2Out, false)
	testMQTTCheckConnAck(t, or, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, oc, or, []*mqttFilter{{filter: "golden/out", qos: 2}}, []byte{2})
	testMQTTFlush(t, oc, nil, or)
	testMQTTPublish(t, pc, pr, 2, false, false, "golden/out", 3, []byte(goldenPayload))
	pi := testMQTTCheckPubMsgNoAck(t, oc, or, "golden/out", mqttPubQoS2, []byte(goldenPayload))
	testMQTTSendPIPacket(mqttPacketPubRec, t, oc, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, or, pi)
	testMQTTDisconnect(t, oc, nil)
	oc.Close()

	// 5. A shared-group member. MQTT 5: 3.1.1 parses "$share/..." as a literal filter.
	sc, sr, _ := storeFormatConnectShared(t, o)
	codes := testMQTTSubV5(t, sc, sr, 1, []mqttV5SubFilter{{topic: "$share/" + goldenGroup + "/golden/s", opts: 1}})
	if len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("shared SUBACK %v, want [1]", codes)
	}
	testMQTTFlush(t, sc, nil, sr)
	testMQTTDisconnect(t, sc, nil)
	sc.Close()

	// 6. A session discarded by a clean start: the teardown leaves a watermark.
	gc, gr := storeFormatConnect(t, o, goldenGone, false)
	testMQTTCheckConnAck(t, gr, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, gc, nil)
	gc.Close()
	gc, gr = storeFormatConnect(t, o, goldenGone, true)
	testMQTTCheckConnAck(t, gr, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, gc, nil)
	gc.Close()

	testMQTTDisconnect(t, pc, nil)
	pc.Close()
}

// storeFormatScenarioStreams are the streams the scenario must leave non-empty, with
// the minimum record count each. $MQTT_willq and $MQTT_qos2assign stay empty: a
// delayed Will needs a session-expiry clock, and the assignment stream is affine-only.
var storeFormatScenarioStreams = map[string]uint64{
	mqttSessStreamName:             1,
	mqttTeardownStreamName:         1,
	mqttStreamName:                 1,
	mqttQoS2IncomingMsgsStreamName: 1,
	mqttInflightStreamName:         1,
	mqttSharedLifecycleStreamName:  2, // the group record and the member's acquisition ledger
	mqttOutStreamName:              1,
	mqttRetainedMsgsStreamName:     1,
}

// storeFormatRunConsumers lists every per-run ephemeral consumer on the two streams
// that carry them, failing on any listing error so a failed listing never reads as
// absence.
func storeFormatRunConsumers(t *testing.T, js jetstream.JetStream) map[string][]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	found := map[string][]string{}
	for _, stream := range []string{mqttSessStreamName, mqttRetainedMsgsStreamName} {
		st, err := js.Stream(ctx, stream)
		if err != nil {
			t.Fatalf("looking up %s: %v", stream, err)
		}
		lister := st.ListConsumers(ctx)
		for ci := range lister.Info() {
			if _, ok := ci.Config.Metadata[mqttEphemeralRunKey]; ok {
				found[stream] = append(found[stream], ci.Name)
			}
		}
		if err := lister.Err(); err != nil {
			t.Fatalf("listing the consumers on %s: %v", stream, err)
		}
	}
	return found
}

// TestWriteStoreFormatFixture regenerates testdata/storeformat/<mqttStoreFormat>/. It
// is skipped unless MQTTNB_WRITE_STORE_FORMAT_FIXTURE=1.
func TestWriteStoreFormatFixture(t *testing.T) {
	if os.Getenv("MQTTNB_WRITE_STORE_FORMAT_FIXTURE") != "1" {
		t.Skip("set MQTTNB_WRITE_STORE_FORMAT_FIXTURE=1 to regenerate the persisted-format fixture")
	}
	store := t.TempDir()
	ns, nc := storeFormatStartNATS(t, store)
	s, o, err := storeFormatBroker(nc, "golden-writer")
	if err != nil {
		storeFormatStopNATS(nc, ns)
		t.Fatalf("starting the generating broker: %v", err)
	}
	storeFormatScenario(t, o)
	storeFormatStopBroker(t, s)

	// The broker is quiesced: nothing can recreate an observer now. Remove the two
	// per-run consumers — random per-run names and an inactivity threshold, so left in
	// place they would age out of the frozen store — and prove them gone.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for stream, names := range storeFormatRunConsumers(t, js) {
		for _, name := range names {
			if err := js.DeleteConsumer(ctx, stream, name); err != nil {
				t.Fatalf("deleting the per-run consumer %s on %s: %v", name, stream, err)
			}
		}
	}
	if left := storeFormatRunConsumers(t, js); len(left) != 0 {
		t.Fatalf("per-run consumers survived their deletion: %v", left)
	}
	// Every claimed record exists, so a step that silently wrote nothing fails here
	// instead of freezing an empty stream.
	for stream, min := range storeFormatScenarioStreams {
		st, err := js.Stream(ctx, stream)
		if err != nil {
			t.Fatalf("looking up %s: %v", stream, err)
		}
		if got := st.CachedInfo().State.Msgs; got < min {
			t.Fatalf("the scenario left %d records on %s, want at least %d", got, stream, min)
		}
	}
	storeFormatStopNATS(nc, ns)

	dst := filepath.Join(storeFormatFixtureRoot, mqttStoreFormat)
	if err := os.RemoveAll(dst); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(dst, os.DirFS(store)); err != nil {
		t.Fatalf("copying the store into %s: %v", dst, err)
	}
	if err := os.WriteFile(filepath.Join(dst, "outcome"), []byte("read\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "README"), []byte(storeFormatProvenance(t, dst)), 0o644); err != nil {
		t.Fatal(err)
	}
}

// storeFormatProvenance records what wrote the fixture: the build, the nats-server
// module, the scenario, and each store file's size and digest.
func storeFormatProvenance(t *testing.T, dir string) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "Persisted format %s golden fixture.\n\n", mqttStoreFormat)
	b.WriteString("Generated by TestWriteStoreFormatFixture (store_format_fixture_fulltests_test.go),\n")
	b.WriteString("scenario storeFormatScenario: one broker, StreamReplicas 1, account $G, no JetStream domain.\n")
	// A test binary carries no VCS stamp, so the commit is asked of git; a dirty tree
	// is recorded as such rather than passed off as the commit.
	head, herr := exec.Command("git", "rev-parse", "HEAD").Output()
	dirty, derr := exec.Command("git", "status", "--porcelain").Output()
	switch {
	case herr != nil || derr != nil:
		b.WriteString("commit: unknown (git unavailable)\n")
	case len(strings.TrimSpace(string(dirty))) > 0:
		fmt.Fprintf(&b, "commit: %s plus uncommitted changes\n", strings.TrimSpace(string(head)))
	default:
		fmt.Fprintf(&b, "commit: %s\n", strings.TrimSpace(string(head)))
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == "github.com/nats-io/nats-server/v2" {
				fmt.Fprintf(&b, "nats-server module: %s\n", d.Version)
			}
		}
	}
	b.WriteString("\nFiles (bytes, sha256):\n")
	var lines []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if rel == "README" || rel == "outcome" {
			return nil
		}
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		sum := sha256.Sum256(data)
		lines = append(lines, fmt.Sprintf("%s %d %s", rel, len(data), hex.EncodeToString(sum[:])))
		return nil
	})
	if err != nil {
		t.Fatalf("hashing the fixture: %v", err)
	}
	sort.Strings(lines)
	b.WriteString(strings.Join(lines, "\n"))
	b.WriteString("\n")
	return b.String()
}

// TestStoreFormatFixtures holds this build to every fixture's recorded outcome.
func TestStoreFormatFixtures(t *testing.T) {
	entries, err := os.ReadDir(storeFormatFixtureRoot)
	if err != nil {
		t.Fatalf("reading %s: %v", storeFormatFixtureRoot, err)
	}
	if len(entries) == 0 {
		t.Fatalf("%s holds no fixture", storeFormatFixtureRoot)
	}
	sawCurrent := false
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		format := e.Name()
		sawCurrent = sawCurrent || format == mqttStoreFormat
		t.Run("format "+format, func(t *testing.T) {
			src := filepath.Join(storeFormatFixtureRoot, format)
			outcome, err := os.ReadFile(filepath.Join(src, "outcome"))
			if err != nil {
				t.Fatalf("the fixture has no outcome: %v", err)
			}
			store := t.TempDir()
			if err := os.CopyFS(store, os.DirFS(src)); err != nil {
				t.Fatalf("copying the fixture: %v", err)
			}
			ns, nc := storeFormatStartNATS(t, store)
			defer storeFormatStopNATS(nc, ns)
			s, o, err := storeFormatBroker(nc, "golden-reader")
			switch want := strings.TrimSpace(string(outcome)); want {
			case "refuse":
				if err == nil {
					storeFormatStopBroker(t, s)
					t.Fatalf("format %s must be refused, and the broker started", format)
				}
				if !strings.Contains(err.Error(), mqttStoreFormatMetaKey) {
					t.Fatalf("format %s was refused, but not by the persisted-format verdict: %v", format, err)
				}
			case "read":
				if err != nil {
					t.Fatalf("format %s must be read, and the broker refused it: %v", format, err)
				}
				defer storeFormatStopBroker(t, s)
				storeFormatReadBack(t, s, o)
			default:
				t.Fatalf("unknown outcome %q", want)
			}
		})
	}
	if !sawCurrent {
		t.Fatalf("no fixture for the format this build writes (%s)", mqttStoreFormat)
	}
}

// storeFormatAwaitBytes waits up to d for the connection's next bytes and hands them to
// r, so the following testMQTTReadPacket parses them under its own, shorter deadline.
func storeFormatAwaitBytes(t *testing.T, c net.Conn, r *mqttReader, d time.Duration) {
	t.Helper()
	if r.hasMore() {
		return
	}
	if err := c.SetReadDeadline(time.Now().Add(d)); err != nil {
		t.Fatalf("arming the read deadline: %v", err)
	}
	var buf [512]byte
	n, err := c.Read(buf[:])
	if err != nil {
		t.Fatalf("nothing arrived within %v: %v", d, err)
	}
	r.reset(copyBytes(buf[:n]))
}

// storeFormatReadBack reads every scenario step's state back through the broker.
func storeFormatReadBack(t *testing.T, s *Server, o *Options) {
	t.Helper()
	// 1. The persistent subscriber's session and its queued QoS 1 message.
	t.Logf("read-back step 1")
	c, r := storeFormatConnect(t, o, goldenSub, false)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTCheckPubMsg(t, c, r, "golden/q", mqttPubQos1, []byte(goldenPayload))

	// 2. The retained message, to a new subscriber.
	t.Logf("read-back step 2")
	pc, pr := storeFormatConnect(t, o, goldenPub, true)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, pc, pr, []*mqttFilter{{filter: "golden/retained", qos: 1}}, []byte{1})
	testMQTTCheckPubMsg(t, pc, pr, "golden/retained", mqttPubQos1|mqttPubFlagRetain, []byte(goldenPayload))

	// 3. The held inbound QoS 2 exchange completes, and the message reaches the
	//    subscriber's restored QoS 2 subscription.
	t.Logf("read-back step 3")
	ic, ir := storeFormatConnect(t, o, goldenQ2In, false)
	defer ic.Close()
	testMQTTCheckConnAck(t, ir, mqttConnAckRCConnectionAccepted, true)
	testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, ic, 7)
	testMQTTReadPIPacket(mqttPacketPubComp, t, ir, 7)
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "golden/q2", mqttPubQoS2, []byte(goldenPayload))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)

	// 4. The outbound exchange resumes at PUBREL.
	t.Logf("read-back step 4")
	oc, or := storeFormatConnect(t, o, goldenQ2Out, false)
	defer oc.Close()
	testMQTTCheckConnAck(t, or, mqttConnAckRCConnectionAccepted, true)
	// After a restart the stored PUBREL is redelivered once its AckWait has run from the
	// generator's delivery, not at once: the immediate redelivery needs the original
	// connection's per-attempt ack subject, which a restarted broker does not have (the
	// stated residual "Promptness, not continuity, still depends on the reconnect being
	// same-process" in docs/CONFORMANCE.md). So the wait is the contract's — AckWait and a
	// margin — and only matters for a fixture younger than AckWait.
	storeFormatAwaitBytes(t, oc, or, mqttDefaultAckWait+15*time.Second)
	b, _ := testMQTTReadPacket(t, or)
	if pt := b & mqttPacketMask; pt != mqttPacketPubRel {
		t.Fatalf("%s: want the withheld exchange's PUBREL, got packet %#x", goldenQ2Out, pt)
	}
	opi, err := or.readUint16("packet identifier")
	if err != nil {
		t.Fatal(err)
	}
	testMQTTSendPIPacket(mqttPacketPubComp, t, oc, opi)

	// 5. The shared member's restored subscription, WITHOUT subscribing again: a
	//    resubscription would mask a lost one.
	t.Logf("read-back step 5")
	sc, sr, present := storeFormatConnectShared(t, o)
	defer sc.Close()
	if !present {
		t.Fatalf("%s: the never-expiring session was not restored", goldenShared)
	}
	testMQTTPublish(t, pc, pr, 1, false, false, "golden/s", 2, []byte(goldenPayload))
	qos, spi := testMQTTReadPublishV5(t, sr, "golden/s", []byte(goldenPayload))
	if qos != 1 {
		t.Fatalf("%s: the shared delivery arrived at QoS %d, want 1", goldenShared, qos)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sc, spi)

	// 6. The discarded session's watermark is still read, and the client has no session.
	t.Logf("read-back step 6")
	cl := testMQTTGetClient(t, s, goldenSub)
	sess := cl.mqtt.sess
	if _, _, st := sess.jsa.loadGenRetirement(sess.domainTk, getHash(goldenGone)); st != mqttRetirementValid {
		t.Fatalf("%s: the teardown watermark reads as %v, want valid", goldenGone, st)
	}
	gc, gr := storeFormatConnect(t, o, goldenGone, false)
	defer gc.Close()
	testMQTTCheckConnAck(t, gr, mqttConnAckRCConnectionAccepted, false)
}
