package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// TestMain lets the subprocess tests run this binary as the broker itself: with
// MQTTNB_TEST_RUN_BROKER set it is main(), reading its flags from os.Args.
func TestMain(m *testing.M) {
	if os.Getenv("MQTTNB_TEST_RUN_BROKER") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// writeConf writes a nats-server configuration file and returns its path.
func writeConf(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// jetStreamBlock is a JetStream block storing in dir with no consumer default stated.
func jetStreamBlock(dir string) string {
	return fmt.Sprintf("jetstream { store_dir: %q }\n", dir)
}

// twoUserAccount is the shape the image's default configuration has: one JetStream
// account, the service user, and a default user kept out of the reserved namespace.
const twoUserAccount = `
accounts {
  MQTT {
    jetstream: enabled
    users: [
      { user: svc, password: svcpass }
      { user: anon, password: anonpass,
        permissions: { publish: { deny: ["$MQTT.>", "$JS.>"] }, subscribe: { deny: ["$MQTT.>", "$JS.>"] } } }
    ]
  }
}
`

// embeddedConfig is a broker configuration for an embedded server at path.
func embeddedConfig(t *testing.T, path string) *config.Config {
	t.Helper()
	c := config.Default()
	c.NATSEmbeddedConfig = path
	c.NATSUser, c.NATSPass = "svc", "svcpass"
	c.DefaultUser, c.DefaultPass = "anon", "anonpass"
	c.JSTimeout = 2 * time.Second
	return c
}

// startEmbedded starts the embedded server as run() does and stops it at test end.
func startEmbedded(t *testing.T, cfg *config.Config) *server.Server {
	t.Helper()
	srv, err := startEmbeddedNATS(context.Background(), cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("startEmbeddedNATS: %v", err)
	}
	t.Cleanup(func() { stopEmbeddedNATS(srv) })
	return srv
}

// TestEmbeddedOptionsEnforced: the parsed configuration becomes the broker's backend or
// is refused, naming the setting.
func TestEmbeddedOptionsEnforced(t *testing.T) {
	store := t.TempDir()
	js := jetStreamBlock(store)
	for _, tc := range []struct {
		name     string
		conf     string
		jsDomain string
		refused  string // a fragment of the refusal; empty means accepted
	}{
		{name: "unset consumer default becomes unlimited", conf: js},
		{name: "explicit 0 becomes unlimited", conf: fmt.Sprintf("jetstream { store_dir: %q, limits { default_max_consumers: 0 } }", store)},
		{name: "unlimited stays", conf: fmt.Sprintf("jetstream { store_dir: %q, limits { default_max_consumers: -1 } }", store)},
		{name: "a cap is refused", conf: fmt.Sprintf("jetstream { store_dir: %q, limits { default_max_consumers: 500 } }", store), refused: "default_max_consumers"},
		{name: "another negative is refused", conf: fmt.Sprintf("jetstream { store_dir: %q, limits { default_max_consumers: -2 } }", store), refused: "default_max_consumers"},
		{name: "no jetstream", conf: "listen: 127.0.0.1:-1", refused: "jetstream must be enabled"},
		{name: "no store_dir", conf: "jetstream: enabled", refused: "store_dir"},
		{name: "nats-server mqtt listener", conf: js + "mqtt { port: 1884 }", refused: "mqtt listener"},
		{name: "websocket listener", conf: js + "websocket { port: 8081, no_tls: true }", refused: "websocket"},
		{name: "leafnode listener", conf: js + "leafnodes { port: 7422 }", refused: "leafnodes"},
		{name: "leafnode remote without a port", conf: js + `leafnodes { remotes: [ { url: "nats-leaf://127.0.0.1:7422" } ] }`, refused: "leafnodes"},
		{name: "gateway", conf: js + "gateway { name: x, port: 7222 }", refused: "gateways"},
		{name: "log_file", conf: js + `log_file: "/tmp/x.log"`, refused: "log_file"},
		{name: "syslog", conf: js + "syslog: true", refused: "syslog"},
		{name: "pid_file", conf: js + `pid_file: "/tmp/x.pid"`, refused: "pid_file"},
		{name: "unauthenticated cluster routes", conf: js + "cluster { name: c, listen: 127.0.0.1:-1 }", refused: "cluster routes must be authenticated"},
		{name: "empty route password", conf: js + `cluster { name: c, listen: 127.0.0.1:-1, authorization: { user: r, password: "" } }`, refused: "cluster authorization: password"},
		{name: "authenticated cluster routes", conf: js + "cluster { name: c, listen: 127.0.0.1:-1, authorization: { user: r, password: p } }"},
		{name: "empty password", conf: js + `authorization { users: [ { user: a, password: "" } ] }`, refused: `user "a": password must not be empty`},
		{name: "domain this server does not serve", conf: js, jsDomain: "other", refused: "js-domain"},
		{name: "domain other than the server's", conf: fmt.Sprintf("jetstream { store_dir: %q, domain: d }", store), jsDomain: "other", refused: "js-domain"},
		{name: "the server's domain", conf: fmt.Sprintf("jetstream { store_dir: %q, domain: d }", store), jsDomain: "d"},
		{name: "no broker domain against a server domain", conf: fmt.Sprintf("jetstream { store_dir: %q, domain: d }", store)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := server.ProcessConfigFile(writeConf(t, tc.conf))
			if err != nil {
				t.Fatalf("ProcessConfigFile: %v", err)
			}
			err = enforceEmbeddedOptions(opts, tc.jsDomain)
			if tc.refused != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refused) {
					t.Fatalf("enforce = %v, want a refusal naming %q", err, tc.refused)
				}
				return
			}
			if err != nil {
				t.Fatalf("enforce = %v, want accepted", err)
			}
			if got := opts.JetStreamLimits.DefaultMaxConsumers; got != -1 {
				t.Errorf("default_max_consumers = %d after enforcement, want -1", got)
			}
			if !opts.NoSigs {
				t.Error("NoSigs is false after enforcement: nats-server would install its own signal handler")
			}
		})
	}

	// An empty user name never reaches enforcement from a file: the upstream parser
	// refuses it first. Enforcement's own branch is checked on constructed options.
	if _, err := server.ProcessConfigFile(writeConf(t, js+`authorization { users: [ { user: "", password: p } ] }`)); err == nil ||
		!strings.Contains(err.Error(), "requires a user") {
		t.Errorf("ProcessConfigFile with an empty user = %v, want the parser's refusal", err)
	}
	opts := &server.Options{JetStream: true, StoreDir: store, Users: []*server.User{
		{Username: "named", Password: "secret-of-named"}, {Username: "", Password: "secret-of-nameless"},
	}}
	err := enforceEmbeddedOptions(opts, "")
	if err == nil || !strings.Contains(err.Error(), "user must not be empty") {
		t.Fatalf("enforce with an empty user = %v, want a refusal", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Errorf("refusal %q prints a password", err)
	}
}

// TestEmbeddedServerAppliesUnlimitedConsumers: with no consumer default in the file, the
// running server lets a stream that asks for no cap hold more than upstream's 1000.
func TestEmbeddedServerAppliesUnlimitedConsumers(t *testing.T) {
	cfg := embeddedConfig(t, writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount))
	srv := startEmbedded(t, cfg)
	nc, err := nats.Connect("", nats.InProcessServer(srv), nats.UserInfo("svc", "svcpass"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "S", Subjects: []string{"s.>"}, MaxConsumers: -1})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1001 {
		if _, err := s.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: fmt.Sprintf("c%d", i)}); err != nil {
			t.Fatalf("consumer %d: %v", i, err)
		}
	}
}

// connByName returns the server's record of the connection named name, or nil.
func connByName(t *testing.T, srv *server.Server, name string) *server.ConnInfo {
	t.Helper()
	cz, err := srv.Connz(&server.ConnzOptions{Limit: 1024, Username: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cz.Conns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// embeddedBroker is a broker serving MQTT over an embedded server, as run() wires it.
type embeddedBroker struct {
	srv  *server.Server
	cfg  *config.Config
	addr string
}

func startEmbeddedBroker(t *testing.T, cfg *config.Config) *embeddedBroker {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	srv := startEmbedded(t, cfg)
	// The URL in-process mode never uses is pointed at the server's real listener, so a
	// regression to dialing it connects and is caught by the in-process assertions
	// rather than failing earlier at a closed default port.
	cfg.NATSURLs = srv.ClientURL()
	be, err := backend.Connect(context.Background(), cfg, srv, log)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(be.Close)
	addr := reservedAddr(t)
	cfg.MQTTHost = "127.0.0.1"
	cfg.MQTTPort = mustPort(t, addr)
	ms, err := serveMQTT(cfg, be, mqttOptions(cfg, nil), log)
	if err != nil {
		t.Fatalf("serveMQTT: %v", err)
	}
	t.Cleanup(func() { <-ms.BeginShutdown() })
	return &embeddedBroker{srv: srv, cfg: cfg, addr: addr}
}

// v5Connect connects an MQTT 5.0 client; user empty means no credentials.
func v5Connect(t *testing.T, addr, clientID, user, pass string, onPub func(paho.PublishReceived) (bool, error)) (*paho.Client, *paho.Connack, error) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cc := paho.ClientConfig{Conn: conn}
	if onPub != nil {
		cc.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPub}
	}
	c := paho.NewClient(cc)
	cp := &paho.Connect{ClientID: clientID, CleanStart: true, KeepAlive: 30}
	if user != "" {
		cp.Username, cp.UsernameFlag = user, true
		cp.Password, cp.PasswordFlag = []byte(pass), true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca, err := c.Connect(ctx, cp)
	if err == nil {
		t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{}) })
	}
	return c, ca, err
}

// roundTripQoS1 subscribes and publishes QoS 1 on topic through one client and waits for
// the message to come back.
func roundTripQoS1(t *testing.T, addr, clientID, user, pass, topic string) {
	t.Helper()
	got := make(chan string, 4)
	c, ca, err := v5Connect(t, addr, clientID, user, pass, func(pr paho.PublishReceived) (bool, error) {
		got <- string(pr.Packet.Payload)
		return true, nil
	})
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("connect %s: %v %+v", clientID, err, ca)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}}}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := c.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: []byte(clientID)}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case p := <-got:
		if p != clientID {
			t.Fatalf("received %q, want %q", p, clientID)
		}
	case <-ctx.Done():
		t.Fatal("QoS 1 publish never came back")
	}
}

// TestEmbeddedModeServesMQTTInProcess: the broker serves MQTT over an embedded server,
// and the server sees the broker's connections as in-process ones, which have no peer
// address; a TCP dial of its listener would show 127.0.0.1.
func TestEmbeddedModeServesMQTTInProcess(t *testing.T) {
	cfg := embeddedConfig(t, writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount))
	b := startEmbeddedBroker(t, cfg)
	roundTripQoS1(t, b.addr, "in-process", "", "", "rt/in-process")

	svc := connByName(t, b.srv, cfg.NATSName)
	if svc == nil {
		t.Fatalf("no server connection named %q", cfg.NATSName)
	}
	if svc.IP != "" {
		t.Errorf("service connection has peer %s:%d, want in-process", svc.IP, svc.Port)
	}
}

// TestEmbeddedAuthPassthroughInProcess runs the repository's default configuration: an
// MQTT client without credentials is admitted as the default user over its own
// in-process connection, wrong credentials are refused, and the reserved namespace is
// closed to the default user by NATS itself, not only by the broker's own guards.
func TestEmbeddedAuthPassthroughInProcess(t *testing.T) {
	baked, err := os.ReadFile("../../embedded/nats.conf")
	if err != nil {
		t.Fatal(err)
	}
	const bakedStore = "store_dir: /data"
	if strings.Count(string(baked), bakedStore) != 1 {
		t.Fatalf("embedded/nats.conf: want exactly one %q to rewrite", bakedStore)
	}
	conf := strings.Replace(string(baked), bakedStore, fmt.Sprintf("store_dir: %q", t.TempDir()), 1)
	conf = strings.Replace(conf, "listen: 127.0.0.1:4222", "listen: 127.0.0.1:-1", 1)
	t.Setenv("MQTTNB_NATS_USER", "svc")
	t.Setenv("MQTTNB_NATS_PASS", "svcpass")
	t.Setenv("MQTTNB_DEFAULT_USER", "anon")
	t.Setenv("MQTTNB_DEFAULT_PASS", "anonpass")
	cfg := embeddedConfig(t, writeConf(t, conf))
	b := startEmbeddedBroker(t, cfg)

	roundTripQoS1(t, b.addr, "anonymous", "", "", "rt/anonymous")
	perClient := connByName(t, b.srv, cfg.NATSName+":mqtt:anonymous")
	if perClient == nil {
		t.Fatal("no per-client connection for the anonymous MQTT client")
	}
	if perClient.IP != "" || perClient.AuthorizedUser != "anon" {
		t.Errorf("per-client connection: peer %q user %q, want in-process as anon", perClient.IP, perClient.AuthorizedUser)
	}

	// Refused at both protocol levels. In-process, the refusal waits out nats-server's
	// write deadline (an upstream defect, a stated residual in docs/CONFORMANCE.md), so
	// the bound here is 15 s; an answer still pending then fails the test.
	for _, level := range []byte{4, 5} {
		refusedWithin(t, b.addr, level, "anon", "nope", 15*time.Second)
	}

	// NATS-level checks, with positive controls, on direct in-process connections.
	check := func(user, pass string, reservedAllowed bool) {
		t.Helper()
		nc, err := nats.Connect("", nats.InProcessServer(b.srv), nats.UserInfo(user, pass), nats.PermissionErrOnSubscribe(true))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		for _, subj := range []string{"app.x", "$MQTT.probe.x"} {
			reserved := strings.HasPrefix(subj, "$MQTT.")
			sub, err := nc.SubscribeSync(subj)
			if err != nil {
				t.Fatal(err)
			}
			if err := nc.Publish(subj, []byte("x")); err != nil {
				t.Fatal(err)
			}
			_ = nc.Flush()
			_, err = sub.NextMsg(time.Second)
			allowed := !reserved || reservedAllowed
			if allowed && err != nil {
				t.Errorf("%s: publish+subscribe on %s: %v, want delivered", user, subj, err)
			}
			if !allowed && !errors.Is(err, nats.ErrPermissionViolation) {
				t.Errorf("%s: subscribe on %s: %v, want a permission violation", user, subj, err)
			}
			_ = sub.Unsubscribe()
		}
		if !reservedAllowed {
			// Publish is refused independently of the subscription: a second connection
			// with the service user listens, and nothing may arrive.
			svc, err := nats.Connect("", nats.InProcessServer(b.srv), nats.UserInfo("svc", "svcpass"))
			if err != nil {
				t.Fatal(err)
			}
			defer svc.Close()
			sub, _ := svc.SubscribeSync("$MQTT.probe.pub")
			_ = svc.Flush()
			_ = nc.Publish("$MQTT.probe.pub", []byte("x"))
			_ = nc.Flush()
			if _, err := sub.NextMsg(time.Second); err == nil {
				t.Errorf("%s published into the reserved namespace", user)
			}
			if !errors.Is(nc.LastError(), nats.ErrPermissionViolation) {
				t.Errorf("%s: last error %v, want the publish permission violation", user, nc.LastError())
			}
		}
	}
	check("anon", "anonpass", false)
	check("svc", "svcpass", true)
}

// unformableCluster is a three-node cluster configuration whose other two nodes do not
// exist, so its JetStream never gets a meta leader.
func unformableCluster(t *testing.T) string {
	return fmt.Sprintf(`server_name: lonely
listen: 127.0.0.1:-1
cluster { name: c, listen: 127.0.0.1:-1, authorization: { user: route, password: routepass },
  routes: [ nats-route://route:routepass@%s, nats-route://route:routepass@%s ] }
`, reservedAddr(t), reservedAddr(t)) + jetStreamBlock(t.TempDir()) + twoUserAccount
}

func waitFor(t *testing.T, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", within)
		}
		time.Sleep(20 * time.Millisecond)
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
		first, err := r.ReadByte()
		var ne net.Error
		switch {
		case errors.As(err, &ne) && ne.Timeout():
			t.Fatalf("MQTT %d CONNECT with wrong credentials: no refusal within %s", level, d)
		case err != nil:
			return // closed: refused
		}
		n, err := r.ReadByte()
		if err != nil {
			return
		}
		pkt := make([]byte, n)
		if _, err := io.ReadFull(r, pkt); err != nil {
			return
		}
		if first == 0x20 && len(pkt) >= 2 && pkt[1] == 0 {
			t.Fatalf("MQTT %d CONNECT with wrong credentials was accepted", level)
		}
	}
}

// TestEmbeddedJetStreamWaitFailsOnPermanentErrors: a misconfiguration is reported at
// once, never waited on as if the cluster were still forming.
func TestEmbeddedJetStreamWaitFailsOnPermanentErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		conf string
		want string
	}{
		{
			name: "account without JetStream",
			conf: `accounts { A { users: [ { user: svc, password: svcpass } ] } }`,
			want: "not enabled for account",
		},
		{
			name: "service user refused the JetStream API",
			conf: `accounts { A { jetstream: enabled, users: [ { user: svc, password: svcpass,
			  permissions: { publish: { deny: ["$JS.API.>"] } } } ] } }`,
			want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := embeddedConfig(t, writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+tc.conf))
			cfg.JSTimeout = time.Second
			srv := startEmbedded(t, cfg)
			rec := &recordingHandler{}
			start := time.Now()
			// Bounded, so a permanent error misread as transient fails here as a wait
			// that never ended rather than hanging the package.
			ctx, cancel := context.WithTimeout(context.Background(), cfg.JSTimeout+5*time.Second)
			defer cancel()
			_, err := backend.Connect(ctx, cfg, srv, slog.New(rec))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Connect = %v, want a permanent failure %q", err, tc.want)
			}
			if took := time.Since(start); took > cfg.JSTimeout+time.Second {
				t.Errorf("Connect took %s, want one probe", took)
			}
			if rec.logged(slog.LevelInfo, "waiting for JetStream") {
				t.Error("a permanent failure was waited on")
			}
		})
	}
	t.Run("a domain the server does not serve", func(t *testing.T) {
		cfg := embeddedConfig(t, writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount))
		cfg.JSDomain = "other"
		cfg.JSTimeout = time.Second
		ctx, cancel := context.WithTimeout(context.Background(), cfg.JSTimeout+time.Second)
		defer cancel()
		srv, err := startEmbeddedNATS(ctx, cfg, slog.New(slog.DiscardHandler))
		if err == nil {
			defer stopEmbeddedNATS(srv)
			_, err = backend.Connect(ctx, cfg, srv, slog.New(slog.DiscardHandler))
		}
		if err == nil || !strings.Contains(err.Error(), "js-domain") {
			t.Fatalf("startup = %v, want the domain refused", err)
		}
	})
}

// brokerProcess is this test binary re-executed as the broker.
type brokerProcess struct {
	cmd      *exec.Cmd
	httpAddr string
	mqttAddr string
	mu       sync.Mutex
	lines    []map[string]any
	// nonJSON holds every stdout line that is not a JSON object: the broker's log is
	// one JSON stream, so a test may require it to stay empty.
	nonJSON []string
	stderr  strings.Builder
	exited  chan struct{}
	exitErr error
}

func startBrokerProcess(t *testing.T, env []string, args ...string) *brokerProcess {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := &brokerProcess{httpAddr: reservedAddr(t), mqttAddr: reservedAddr(t), exited: make(chan struct{})}
	p.cmd = exec.Command(exe, append([]string{"-http-addr", p.httpAddr, "-port", portOf(t, p.mqttAddr)}, args...)...)
	p.cmd.Env = append(append(os.Environ(), "MQTTNB_TEST_RUN_BROKER=1"), env...)
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = &p.stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.cmd.Process.Kill()
		<-p.exited
		if t.Failed() {
			t.Logf("broker stdout:\n%v\nstderr:\n%s", p.messages(), p.stderr.String())
		}
	})
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			var m map[string]any
			p.mu.Lock()
			if json.Unmarshal(sc.Bytes(), &m) == nil {
				p.lines = append(p.lines, m)
			} else {
				p.nonJSON = append(p.nonJSON, sc.Text())
			}
			p.mu.Unlock()
		}
		p.exitErr = p.cmd.Wait()
		close(p.exited)
	}()
	return p
}

// mustPort is the numeric port of a host:port address.
func mustPort(t *testing.T, addr string) int {
	t.Helper()
	n, err := strconv.Atoi(portOf(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func portOf(t *testing.T, addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func (p *brokerProcess) messages() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, m := range p.lines {
		msg, _ := m["msg"].(string)
		out = append(out, msg)
	}
	return out
}

// index is the position of the first log message containing fragment, or -1.
func (p *brokerProcess) index(fragment string) int {
	for i, m := range p.messages() {
		if strings.Contains(m, fragment) {
			return i
		}
	}
	return -1
}

func (p *brokerProcess) readyz() int {
	resp, err := (&http.Client{Timeout: time.Second}).Get("http://" + p.httpAddr + "/readyz")
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func (p *brokerProcess) waitExit(t *testing.T, within time.Duration) int {
	t.Helper()
	select {
	case <-p.exited:
	case <-time.After(within):
		t.Fatalf("broker still running after %s", within)
	}
	var ee *exec.ExitError
	if errors.As(p.exitErr, &ee) {
		return ee.ExitCode()
	}
	return 0
}

// TestEmbeddedShutdownOrder runs the broker as a process and signals it for real: the
// broker, not nats-server, handles SIGTERM, drains, closes its NATS connection, and only
// then stops the embedded server. A SIGTERM during the JetStream wait stops the server
// before run returns its error.
func TestEmbeddedShutdownOrder(t *testing.T) {
	env := []string{"MQTTNB_NATS_USER=svc", "MQTTNB_NATS_PASS=svcpass", "MQTTNB_DEFAULT_USER=anon", "MQTTNB_DEFAULT_PASS=anonpass"}

	t.Run("ready", func(t *testing.T) {
		conf := writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount)
		p := startBrokerProcess(t, env, "-nats-embedded-config", conf, "-drain-delay", "2s")
		waitFor(t, 30*time.Second, func() bool { return p.readyz() == http.StatusOK })
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		select {
		case <-p.exited:
			t.Fatal("the process exited within 1s of SIGTERM, inside a 2s drain grace")
		default:
		}
		if code := p.readyz(); code != http.StatusServiceUnavailable {
			t.Errorf("/readyz during the grace = %d, want 503", code)
		}
		if code := p.waitExit(t, 30*time.Second); code != 0 {
			t.Fatalf("exit code %d, want 0", code)
		}
		sig, closed, stopped := p.index("shutdown signal received"), p.index("NATS service connection closed"), p.index("Initiating Shutdown")
		if sig < 0 || closed < 0 || stopped < 0 || !(sig < closed && closed < stopped) {
			t.Fatalf("log order signal=%d connection-closed=%d server-shutdown=%d, want signal < closed < shutdown", sig, closed, stopped)
		}
	})

	t.Run("during the JetStream wait", func(t *testing.T) {
		p := startBrokerProcess(t, env, "-nats-embedded-config", writeConf(t, unformableCluster(t)))
		waitFor(t, 60*time.Second, func() bool { return p.index("waiting for JetStream") >= 0 })
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if code := p.waitExit(t, 30*time.Second); code != 1 {
			t.Fatalf("exit code %d, want 1 (startup did not complete)", code)
		}
		if p.index("Initiating Shutdown") < 0 {
			t.Error("the embedded server was not shut down on the way out")
		}
		if !strings.Contains(p.stderr.String(), "fatal:") || !strings.Contains(p.stderr.String(), "context canceled") {
			t.Errorf("stderr = %q, want the fatal cancellation", p.stderr.String())
		}
	})
}

// TestEmbeddedClusterFormsAfterFirstProbeFails: three brokers, each with its own
// embedded peer, form one JetStream cluster through their configurations' routes. The
// first broker starts alone, so its first JetStream probe fails and it waits; the cluster
// forms once the others start, every broker connects, and the broker's stream is placed
// on all three peers.
func TestEmbeddedClusterFormsAfterFirstProbeFails(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	routes := []string{reservedAddr(t), reservedAddr(t), reservedAddr(t)}
	var routeList []string
	for _, r := range routes {
		routeList = append(routeList, "nats-route://route:routepass@"+r)
	}
	nodeConfig := func(i int) *config.Config {
		conf := fmt.Sprintf("server_name: node-%d\nlisten: 127.0.0.1:-1\ncluster { name: c, listen: %q, authorization: { user: route, password: routepass }, routes: [ %s ] }\n",
			i, routes[i], strings.Join(routeList, ", ")) + jetStreamBlock(t.TempDir()) + twoUserAccount
		cfg := embeddedConfig(t, writeConf(t, conf))
		cfg.StreamReps = 3
		cfg.JSTimeout = 2 * time.Second
		return cfg
	}

	type node struct {
		cfg *config.Config
		srv *server.Server
		be  *backend.Backend
	}
	connect := func(n *node, log *slog.Logger, done chan<- error) {
		be, err := backend.Connect(context.Background(), n.cfg, n.srv, log)
		n.be = be
		done <- err
	}
	nodes := make([]*node, 3)
	for i := range nodes {
		nodes[i] = &node{cfg: nodeConfig(i)}
	}

	// Node 0 alone: its probe must fail and wait.
	nodes[0].srv = startEmbedded(t, nodes[0].cfg)
	rec := &recordingHandler{}
	done0 := make(chan error, 1)
	go connect(nodes[0], slog.New(rec), done0)
	waitFor(t, 60*time.Second, func() bool { return rec.logged(slog.LevelInfo, "waiting for JetStream") })
	select {
	case err := <-done0:
		t.Fatalf("node 0 Connect returned %v while alone, want it waiting", err)
	default:
	}

	// The others join; every Connect completes.
	dones := []chan error{done0}
	for i := 1; i < 3; i++ {
		nodes[i].srv = startEmbedded(t, nodes[i].cfg)
		d := make(chan error, 1)
		go connect(nodes[i], slog.New(slog.DiscardHandler), d)
		dones = append(dones, d)
	}
	for i, d := range dones {
		select {
		case err := <-d:
			if err != nil {
				t.Fatalf("node %d Connect: %v", i, err)
			}
			t.Cleanup(nodes[i].be.Close)
		case <-time.After(2 * time.Minute):
			t.Fatalf("node %d Connect did not complete after the cluster formed", i)
		}
	}

	// Brokers on nodes 0 and 2: a subscriber on one receives a publish made on the other.
	addrs := make([]string, 3)
	for _, i := range []int{0, 2} {
		addr := reservedAddr(t)
		nodes[i].cfg.MQTTHost = "127.0.0.1"
		nodes[i].cfg.MQTTPort = mustPort(t, addr)
		ms, err := serveMQTT(nodes[i].cfg, nodes[i].be, mqttOptions(nodes[i].cfg, nil), slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatalf("serveMQTT on node %d: %v", i, err)
		}
		t.Cleanup(func() { <-ms.BeginShutdown() })
		addrs[i] = addr
	}
	got := make(chan string, 1)
	sub, ca, err := v5Connect(t, addrs[2], "sub-on-2", "", "", func(pr paho.PublishReceived) (bool, error) {
		got <- string(pr.Packet.Payload)
		return true, nil
	})
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("subscriber connect: %v %+v", err, ca)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := sub.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "cluster/rt", QoS: 1}}}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	pub, ca, err := v5Connect(t, addrs[0], "pub-on-0", "", "", nil)
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("publisher connect: %v %+v", err, ca)
	}
	if _, err := pub.Publish(ctx, &paho.Publish{Topic: "cluster/rt", QoS: 1, Payload: []byte("across the cluster")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case p := <-got:
		if p != "across the cluster" {
			t.Fatalf("received %q", p)
		}
	case <-ctx.Done():
		t.Fatal("the publish on node 0 never reached the subscriber on node 2")
	}

	// The broker's message stream is replicated on all three peers.
	js, err := jetstream.New(nodes[1].be.Conn())
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		s, err := js.Stream(ctx, "$MQTT_msgs")
		if err != nil {
			return false
		}
		info, err := s.Info(ctx)
		if err != nil || info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 2 {
			return false
		}
		for _, r := range info.Cluster.Replicas {
			if !r.Current {
				return false
			}
		}
		return true
	})
}

// TestEmbeddedDefaultUserCannotAdministerJetStream runs the repository's default
// configuration: an anonymous MQTT client, whose topics map onto NATS subjects as the
// default user, can neither delete nor purge the broker's streams.
func TestEmbeddedDefaultUserCannotAdministerJetStream(t *testing.T) {
	baked, err := os.ReadFile("../../embedded/nats.conf")
	if err != nil {
		t.Fatal(err)
	}
	conf := strings.Replace(string(baked), "store_dir: /data", fmt.Sprintf("store_dir: %q", t.TempDir()), 1)
	conf = strings.Replace(conf, "listen: 127.0.0.1:4222", "listen: 127.0.0.1:-1", 1)
	t.Setenv("MQTTNB_NATS_USER", "svc")
	t.Setenv("MQTTNB_NATS_PASS", "svcpass")
	t.Setenv("MQTTNB_DEFAULT_USER", "anon")
	t.Setenv("MQTTNB_DEFAULT_PASS", "anonpass")
	cfg := embeddedConfig(t, writeConf(t, conf))
	b := startEmbeddedBroker(t, cfg)
	svc, err := nats.Connect("", nats.InProcessServer(b.srv), nats.UserInfo("svc", "svcpass"))
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	requireMQTTCannotAdministerJetStream(t, b.addr, svc)
}

// TestEmbeddedClusterRefusesAnUnauthenticatedPeer: reaching an embedded node's route
// port is not joining its cluster. A peer that presents no route credentials is refused;
// one that presents them is routed (the positive control).
func TestEmbeddedClusterRefusesAnUnauthenticatedPeer(t *testing.T) {
	routeAddr := reservedAddr(t)
	// A clustered JetStream node needs a route configured; this one leads nowhere.
	conf := fmt.Sprintf("server_name: node\nlisten: 127.0.0.1:-1\ncluster { name: c, listen: %q, authorization: { user: route, password: routepass }, routes: [ nats-route://route:routepass@%s ] }\n",
		routeAddr, reservedAddr(t)) + jetStreamBlock(t.TempDir()) + twoUserAccount
	node := startEmbedded(t, embeddedConfig(t, writeConf(t, conf)))

	peer := func(name, userinfo string) *server.Server {
		t.Helper()
		u, err := url.Parse("nats-route://" + userinfo + routeAddr)
		if err != nil {
			t.Fatal(err)
		}
		p, err := server.NewServer(&server.Options{ServerName: name, Host: "127.0.0.1", Port: -1, NoSigs: true,
			Cluster: server.ClusterOpts{Name: "c", Host: "127.0.0.1", Port: -1}, Routes: []*url.URL{u}})
		if err != nil {
			t.Fatal(err)
		}
		go p.Start()
		if !p.ReadyForConnections(10 * time.Second) {
			t.Fatal("peer not ready")
		}
		t.Cleanup(func() { p.Shutdown(); p.WaitForShutdown() })
		return p
	}

	stranger := peer("stranger", "")
	time.Sleep(3 * time.Second)
	if n := node.NumRoutes(); n != 0 {
		t.Fatalf("a peer without route credentials was routed (%d routes)", n)
	}
	stranger.Shutdown()
	stranger.WaitForShutdown()

	peer("member", "route:routepass@")
	waitFor(t, 15*time.Second, func() bool { return node.NumRoutes() > 0 })
}
