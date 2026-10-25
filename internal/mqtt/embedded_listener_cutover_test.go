package mqtt

import (
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// embeddedListenerStreams are the streams nats-server's own MQTT listener creates in an
// account the first time a server initializes MQTT for it. Each name is also a member
// of this broker's bundle, which is why the two cannot share an account's state.
var embeddedListenerStreams = []string{
	mqttSessStreamName, mqttStreamName, mqttRetainedMsgsStreamName,
	mqttQoS2IncomingMsgsStreamName, mqttOutStreamName,
}

// TestEmbeddedListenerCutover is the witness behind docs/MIGRATION.md: what this broker
// does with an account that nats-server's embedded MQTT listener has served, and that
// the documented cutover (restart the servers without `mqtt {}`, delete the listener's
// five streams, start the broker) carries no session across.
func TestEmbeddedListenerCutover(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	mqttPort := testFreePort(t)

	// 1. The embedded listener serves a persistent-session client, so it creates its
	//    streams and a session.
	srv := testStartUpstreamServer(t, store, mqttPort)
	upstream := fmt.Sprintf("tcp://127.0.0.1:%d", mqttPort)
	c := testPersistentClient(t, upstream, "cutover-client")
	if tok := c.Subscribe("cutover/topic", 1, nil); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe on the embedded listener: %v", tok.Error())
	}
	c.Disconnect(200)
	// Control: the embedded listener kept that session.
	if !testPersistentConnect(t, upstream, "cutover-client") {
		t.Fatal("the embedded listener did not resume its own persistent session; the cutover check below would prove nothing")
	}

	nc := testServiceConn(t, srv)
	if got := testStreamNames(t, nc); !slices.Equal(got, sortedCopy(embeddedListenerStreams)) {
		t.Fatalf("the embedded listener created %v, want exactly %v", got, sortedCopy(embeddedListenerStreams))
	}

	// 2. This broker refuses the account by its bundle preflight: five of its ten names
	//    are present. The refusal must be the preflight's, not a per-stream validator
	//    rejecting the listener's shapes, which would also refuse but for another reason.
	b, bc, _ := newBringUpServerOn(t, nc, "cutover-refused")
	_, err := b.getOrCreateMQTTAccountSessionManager(bc)
	if err == nil {
		t.Fatal("the broker started on the embedded listener's streams")
	}
	present, absent := testBundleRefusalLists(t, err)
	slices.Sort(present)
	if !slices.Equal(present, sortedCopy(embeddedListenerStreams)) {
		t.Fatalf("the refusal lists %v as present, want the embedded listener's %v: %v", present, embeddedListenerStreams, err)
	}
	if len(absent) != len(mqttBrokerStreamNames(testBundleSpecs()))-len(embeddedListenerStreams) {
		t.Fatalf("the refusal lists %v as absent: %v", absent, err)
	}
	testStopBringUpServer(t, b)

	// 3. The servers restart without `mqtt {}`; the listener's state persists until it
	//    is deleted.
	nc.Close()
	srv.Shutdown()
	srv.WaitForShutdown()
	srv = testStartUpstreamServer(t, store, 0)
	nc = testServiceConn(t, srv)
	if got := testStreamNames(t, nc); !slices.Equal(got, sortedCopy(embeddedListenerStreams)) {
		t.Fatalf("after the restart without mqtt {} the account holds %v, want the listener's five", got)
	}

	// 4. Delete the five and start the broker: it provisions its own ten, and the
	//    client that had a session on the embedded listener has none here.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range embeddedListenerStreams {
		if err := js.DeleteStream(name); err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
	}
	broker := NewServer(nc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}, "cutover-broker")
	if err := broker.Start(); err != nil {
		t.Fatalf("the broker did not start on the emptied account: %v", err)
	}
	t.Cleanup(broker.Shutdown)
	if got := testStreamNames(t, nc); !slices.Equal(got, sortedCopy(mqttBrokerStreamNames(testBundleSpecs()))) {
		t.Fatalf("the broker provisioned %v", got)
	}
	if testPersistentConnect(t, "tcp://"+broker.Addr().String(), "cutover-client") {
		t.Fatal("a session from the embedded listener was resumed by the broker (Session Present 1)")
	}
}

// testStartUpstreamServer starts nats-server with JetStream on store, and with its own
// MQTT listener on mqttPort when that is non-zero.
func testStartUpstreamServer(t *testing.T, store string, mqttPort int) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		ServerName: "embedded-listener", Host: "127.0.0.1", Port: -1,
		JetStream: true, StoreDir: store, NoSigs: true,
	}
	if mqttPort != 0 {
		opts.MQTT = natsserver.MQTTOpts{Host: "127.0.0.1", Port: mqttPort}
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func testServiceConn(t *testing.T, srv *natsserver.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// testPersistentClient connects an MQTT 3.1.1 client with a persistent session. The
// embedded listener only speaks 3.1.1.
func testPersistentClient(t *testing.T, broker, id string) pmqtt.Client {
	t.Helper()
	c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker(broker).SetClientID(id).
		SetProtocolVersion(4).SetConnectTimeout(5 * time.Second).SetCleanSession(false).
		SetAutoReconnect(false))
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connect %s to %s: %v", id, broker, tok.Error())
	}
	return c
}

// testPersistentConnect connects id with a persistent session, disconnects, and
// reports the CONNACK's Session Present flag.
func testPersistentConnect(t *testing.T, broker, id string) bool {
	t.Helper()
	c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker(broker).SetClientID(id).
		SetProtocolVersion(4).SetConnectTimeout(5 * time.Second).SetCleanSession(false).
		SetAutoReconnect(false))
	tok := c.Connect()
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connect %s to %s: %v", id, broker, tok.Error())
	}
	defer c.Disconnect(200)
	return tok.(*pmqtt.ConnectToken).SessionPresent()
}

func testStreamNames(t *testing.T, nc *nats.Conn) []string {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for name := range js.StreamNames() {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func sortedCopy(s []string) []string {
	c := slices.Clone(s)
	slices.Sort(c)
	return c
}

func testFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
