package imagesmoke

import (
	"archive/tar"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

const embeddedConfigPath = "etc/mqtt-nats-broker/nats.conf"

// credentials are the four variables the embedded image's configuration requires.
var credentials = map[string]string{
	"MQTTNB_NATS_USER":    "svc",
	"MQTTNB_NATS_PASS":    "svcpass",
	"MQTTNB_DEFAULT_USER": "anon",
	"MQTTNB_DEFAULT_PASS": "anonpass",
}

// envOpts renders credentials as docker -e options, without skip and with override.
func envOpts(skip string, override map[string]string) []string {
	var opts []string
	for k, v := range credentials {
		if k == skip {
			continue
		}
		if o, ok := override[k]; ok {
			v = o
		}
		opts = append(opts, "-e", k+"="+v)
	}
	return opts
}

// TestEmbeddedImageContents: the embedded image is the external image's two files plus
// its NATS configuration, and a /data store owned by the runtime user, declared a volume.
func TestEmbeddedImageContents(t *testing.T) {
	f := newEmbeddedFixture(t)
	user := strings.TrimSpace(mustDocker(t, "image", "inspect", "--format", "{{.Config.User}}", f.image))
	if want := fmt.Sprintf("%d:%d", runtimeUID, runtimeGID); user != want {
		t.Errorf("image user = %q, want %q", user, want)
	}
	env := mustDocker(t, "image", "inspect", "--format", "{{json .Config.Env}}", f.image)
	if !strings.Contains(env, "MQTTNB_NATS_EMBEDDED_CONFIG=/"+embeddedConfigPath) {
		t.Errorf("image env %s does not point at the baked configuration", env)
	}
	if vols := mustDocker(t, "image", "inspect", "--format", "{{json .Config.Volumes}}", f.image); !strings.Contains(vols, `"/data"`) {
		t.Errorf("image volumes = %s, want /data", vols)
	}

	headers, _ := imageFiles(t, f.image)
	var files []string
	for p, h := range headers {
		if h.Typeflag != tar.TypeDir {
			files = append(files, p)
		}
	}
	if len(files) != 3 || headers[binaryPath] == nil || headers[bundlePath] == nil || headers[embeddedConfigPath] == nil {
		t.Errorf("image files = %v, want exactly the binary, the CA bundle and %s", files, embeddedConfigPath)
	}
	if h := headers[embeddedConfigPath]; h != nil && (h.Uid != 0 || h.Mode&0o004 == 0) {
		t.Errorf("configuration uid %d mode %o, want root-owned and readable by the runtime user", h.Uid, h.Mode&0o777)
	}
	h := headers["data"]
	if h == nil || h.Typeflag != tar.TypeDir || h.Uid != runtimeUID || h.Gid != runtimeGID || h.Mode&0o777 != 0o755 {
		t.Errorf("/data = %+v, want a 0755 directory owned %d:%d", h, runtimeUID, runtimeGID)
	}
}

// TestEmbeddedImageDefaultInvocation runs the image exactly as shipped, with only the
// four credentials and no volume flag (Docker creates /data from the image): an MQTT
// client without credentials is served, and wrong credentials are refused.
func TestEmbeddedImageDefaultInvocation(t *testing.T) {
	f := newEmbeddedFixture(t)
	b := f.runBroker(envOpts("", nil), nil)
	b.waitReady()
	dialRoundTripper(t, b.addr("1883"), "anonymous").roundTrip(t, "embedded default")
	// In-process, the refusal waits out nats-server's write deadline (an upstream
	// defect, a stated residual in docs/CONFORMANCE.md); 15 s bounds it.
	for _, level := range []byte{4, 5} {
		refusedWithin(t, b.addr("1883"), level, "anon", "wrong", 15*time.Second)
	}
}

// TestEmbeddedImageRefusesMissingCredentials: the baked configuration does not start
// without its credentials: an unset one fails the configuration parse, an empty user
// the parser's user check, an empty password the broker's.
func TestEmbeddedImageRefusesMissingCredentials(t *testing.T) {
	f := newEmbeddedFixture(t)
	for i, tc := range []struct {
		skip     string
		override map[string]string
		want     string
	}{
		{skip: "MQTTNB_NATS_USER", want: "can not be found"},
		{skip: "MQTTNB_NATS_PASS", want: "can not be found"},
		{skip: "MQTTNB_DEFAULT_USER", want: "can not be found"},
		{skip: "MQTTNB_DEFAULT_PASS", want: "can not be found"},
		{override: map[string]string{"MQTTNB_NATS_USER": ""}, want: "requires a user"},
		{override: map[string]string{"MQTTNB_DEFAULT_USER": ""}, want: "requires a user"},
		{override: map[string]string{"MQTTNB_NATS_PASS": ""}, want: `user "svc": password must not be empty`},
		{override: map[string]string{"MQTTNB_DEFAULT_PASS": ""}, want: `user "anon": password must not be empty`},
	} {
		b := f.runBrokerNamed(fmt.Sprintf("broker%d", i), envOpts(tc.skip, tc.override), nil)
		if code := b.waitExit(time.Minute); code == 0 {
			t.Errorf("case %d (%s %v): exited 0", i, tc.skip, tc.override)
			continue
		}
		if logs := b.logs(); !strings.Contains(logs, tc.want) {
			t.Errorf("case %d (%s %v): log lacks %q:\n%s", i, tc.skip, tc.override, tc.want, logs)
		}
	}
}

// TestEmbeddedImageNoUsersConfigKeepsThePassthroughError: a mounted configuration with
// no users cannot name the broker's service identity, and auth passthrough refuses to
// start with its existing error rather than serving anonymously.
func TestEmbeddedImageNoUsersConfigKeepsThePassthroughError(t *testing.T) {
	f := newEmbeddedFixture(t)
	vol := f.volume("nousers", []tarEntry{{name: "nats.conf", body: []byte("listen: 127.0.0.1:4222\njetstream { store_dir: /data }\n")}})
	b := f.runBroker([]string{"-v", vol + ":/etc/custom:ro", "-e", "MQTTNB_NATS_EMBEDDED_CONFIG=/etc/custom/nats.conf"}, nil)
	if code := b.waitExit(time.Minute); code == 0 {
		t.Fatal("exited 0 with a configuration that names no user")
	}
	if logs := b.logs(); !strings.Contains(logs, "MQTT auth passthrough") || !strings.Contains(logs, "named no user") {
		t.Fatalf("log lacks the passthrough identity error:\n%s", logs)
	}
}

// publishRetained connects without credentials and publishes a retained QoS 1 message.
func publishRetained(t *testing.T, addr, topic, payload string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := paho.NewClient(paho.ClientConfig{Conn: conn})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if ca, err := c.Connect(ctx, &paho.Connect{ClientID: "retainer", CleanStart: true, KeepAlive: 30}); err != nil || ca.ReasonCode != 0 {
		t.Fatalf("connect: %v %+v", err, ca)
	}
	if _, err := c.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Retain: true, Payload: []byte(payload)}); err != nil {
		t.Fatalf("publish retained: %v", err)
	}
	_ = c.Disconnect(&paho.Disconnect{})
}

// TestEmbeddedImagePersistsAcrossRestart: the JetStream store lives on the /data volume
// and survives the container. The volume is created empty, so its ownership is the
// image's, as on a first deployment.
func TestEmbeddedImagePersistsAcrossRestart(t *testing.T) {
	f := newEmbeddedFixture(t)
	vol := f.name("data")
	cleanup(t, "volume", "rm", "-f", vol)
	mustDocker(t, "volume", "create", vol)
	opts := append(envOpts("", nil), "-v", vol+":/data")

	first := f.runBrokerNamed("first", opts, nil)
	first.waitReady()
	publishRetained(t, first.addr("1883"), "persist/me", "survived")
	w := dialWatcher(t, first.addr("1883"), "watcher")
	first.signal("TERM")
	if e := w.waitDisconnect(t, 30*time.Second); e.reason != reasonServerShuttingDown {
		t.Errorf("DISCONNECT reason %#x, want %#x", e.reason, reasonServerShuttingDown)
	}
	if code := first.waitExit(30 * time.Second); code != 0 {
		t.Fatalf("exit code %d, want 0", code)
	}
	mustDocker(t, "rm", first.name)

	second := f.runBrokerNamed("second", opts, nil)
	second.waitReady()
	rt := dialRoundTripperOn(t, second.addr("1883"), "after-restart", "persist/me")
	select {
	case got := <-rt.got:
		if got != "survived" {
			t.Fatalf("retained message = %q, want survived", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the retained message did not survive the restart")
	}
}

// TestEmbeddedImageClusterSurvivesLeaderLoss runs the repository's clustered example on
// three containers, with stream replicas 3, and kills the node leading the broker's
// message stream outright: the other two elect a new leader and keep serving.
func TestEmbeddedImageClusterSurvivesLeaderLoss(t *testing.T) {
	f := newEmbeddedFixture(t)
	conf, err := os.ReadFile("../../embedded/nats-cluster.conf")
	if err != nil {
		t.Fatal(err)
	}
	vol := f.volume("clusterconf", []tarEntry{{name: "nats-cluster.conf", body: conf}})
	nodes := make([]*broker, 3)
	for i := range nodes {
		name := fmt.Sprintf("broker-%d", i)
		opts := append(envOpts("", nil),
			"--network-alias", name,
			"-v", vol+":/etc/cluster:ro",
			"-e", "MQTTNB_NATS_EMBEDDED_CONFIG=/etc/cluster/nats-cluster.conf",
			"-e", "MQTTNB_EMBEDDED_SERVER_NAME="+name,
			"-e", "MQTTNB_EMBEDDED_ROUTE_USER=route",
			"-e", "MQTTNB_EMBEDDED_ROUTE_PASS=routepass",
			"-e", "MQTTNB_EMBEDDED_ROUTES=[nats-route://route:routepass@broker-0:6222, nats-route://route:routepass@broker-1:6222, nats-route://route:routepass@broker-2:6222]")
		nodes[i] = f.runBrokerNamed(name, opts, []string{"-stream-replicas", "3"})
	}
	for _, n := range nodes {
		n.waitProbe("/readyz", 200, 2*time.Minute)
	}
	if logs := nodes[0].logs(); !strings.Contains(logs, "replicas 3") {
		t.Fatalf("the broker did not create its streams with 3 replicas:\n%s", logs)
	}

	// The node whose server announced leadership of the message stream.
	const leaderLine = "new stream leader for 'MQTT > $MQTT_msgs'"
	var leader int
	waitForCond(t, 30*time.Second, func() bool {
		for i, n := range nodes {
			if strings.Contains(n.logs(), leaderLine) {
				leader = i
				return true
			}
		}
		return false
	})
	rest := []*broker{}
	before := map[*broker]int{}
	for i, n := range nodes {
		if i != leader {
			rest = append(rest, n)
			before[n] = strings.Count(n.logs(), leaderLine)
		}
	}
	mustDocker(t, "kill", "-s", "KILL", nodes[leader].name)
	// A survivor announces leadership after the loss.
	waitForCond(t, 60*time.Second, func() bool {
		for _, n := range rest {
			if strings.Count(n.logs(), leaderLine) > before[n] {
				return true
			}
		}
		return false
	})

	// The killed node may also have led the meta group or other streams, which re-elect
	// on their own schedule; the survivors must converge and serve within a minute. Each
	// attempt is a fresh pair of clients, so one caught mid-election does not poison
	// the next.
	deadline := time.Now().Add(time.Minute)
	for attempt := 0; ; attempt++ {
		err := crossBrokerRoundTrip(rest[0].addr("1883"), rest[1].addr("1883"), attempt)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the surviving brokers did not deliver between each other within a minute of the loss: %v", err)
		}
		t.Logf("attempt %d after the loss: %v", attempt, err)
		time.Sleep(2 * time.Second)
	}
}

// crossBrokerRoundTrip subscribes QoS 1 through one broker and publishes through another,
// and reports whether the message arrived.
func crossBrokerRoundTrip(pubAddr, subAddr string, attempt int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connect := func(addr, id string, onPub func(paho.PublishReceived) (bool, error)) (*paho.Client, error) {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			return nil, err
		}
		cc := paho.ClientConfig{Conn: conn}
		if onPub != nil {
			cc.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPub}
		}
		c := paho.NewClient(cc)
		if ca, err := c.Connect(ctx, &paho.Connect{ClientID: id, CleanStart: true, KeepAlive: 30}); err != nil {
			return nil, fmt.Errorf("connect %s: %w", id, err)
		} else if ca.ReasonCode != 0 {
			return nil, fmt.Errorf("connect %s: reason %#x", id, ca.ReasonCode)
		}
		return c, nil
	}
	topic := fmt.Sprintf("cluster/after/%d", attempt)
	got := make(chan string, 1)
	sub, err := connect(subAddr, fmt.Sprintf("sub-after-loss-%d", attempt), func(pr paho.PublishReceived) (bool, error) {
		select {
		case got <- string(pr.Packet.Payload):
		default:
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	defer func() { _ = sub.Disconnect(&paho.Disconnect{}) }()
	if _, err := sub.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}}}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	pub, err := connect(pubAddr, fmt.Sprintf("pub-after-loss-%d", attempt), nil)
	if err != nil {
		return err
	}
	defer func() { _ = pub.Disconnect(&paho.Disconnect{}) }()
	if _, err := pub.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: []byte("still serving")}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	select {
	case p := <-got:
		if p != "still serving" {
			return fmt.Errorf("received %q", p)
		}
		return nil
	case <-ctx.Done():
		return errors.New("no delivery between the surviving brokers")
	}
}

func waitForCond(t *testing.T, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", within)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// refusedWithin sends a CONNECT with credentials at the given protocol level and
// requires the broker to refuse it within d: the connection closes without a successful
// CONNACK (a refusing CONNACK before the close is accepted too).
func refusedWithin(t *testing.T, addr string, level byte, user, pass string, d time.Duration) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', level, 0xc2, 0, 30} // clean start, user, password
	if level == 5 {
		body = append(body, 0) // no properties
	}
	for _, f := range []string{fmt.Sprintf("refused-v%d", level), user, pass} {
		body = append(body, byte(len(f)>>8), byte(len(f)))
		body = append(body, f...)
	}
	if _, err := conn.Write(append([]byte{0x10, byte(len(body))}, body...)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(d))
	r := bufio.NewReader(conn)
	for {
		typ, pkt, err := readPacket(r)
		var ne net.Error
		switch {
		case errors.As(err, &ne) && ne.Timeout():
			t.Fatalf("MQTT %d CONNECT with wrong credentials: no refusal within %s", level, d)
		case errors.Is(err, io.EOF) || err != nil:
			return // closed: refused
		}
		if typ == packetConnack && len(pkt) >= 2 && pkt[1] == 0 {
			t.Fatalf("MQTT %d CONNECT with wrong credentials was accepted", level)
		}
	}
}
