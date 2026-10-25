package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/config"
	"github.com/nberlee/mqtt-nats-broker/internal/testcerts"
)

const natsAuthExample = "../../docs/deploy/nats-auth/nats.conf"

// setNATSAuthExampleEnv supplies the passwords the example reads from the environment.
func setNATSAuthExampleEnv(t *testing.T) {
	t.Setenv("SERVICE_PASSWORD", "svcpass")
	t.Setenv("ANONYMOUS_PASSWORD", "anonpass")
	t.Setenv("ALICE_PASSWORD", "alicepass")
	t.Setenv("BOB_PASSWORD", "bobpass")
	t.Setenv("OUTSIDER_PASSWORD", "outsiderpass")
}

// recordBrokerLog sends the broker's own log (the default logger) to a recorder for the
// rest of the test. It swaps a process-wide logger, so a test that calls it must not run
// in parallel with another that does.
func recordBrokerLog(t *testing.T) *recordingHandler {
	t.Helper()
	h := &recordingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}

// startExampleBroker runs the broker in external mode against srv with cfg's
// credentials, and returns its MQTT address.
func startExampleBroker(t *testing.T, srv *server.Server, cfg *config.Config) string {
	t.Helper()
	cfg.NATSURLs = srv.ClientURL()
	log := slog.New(slog.DiscardHandler)
	be, err := backend.Connect(context.Background(), cfg, nil, log)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(be.Close)
	tlsSrc, err := resolveTLS(cfg, log)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	addr := reservedAddr(t)
	cfg.MQTTHost, cfg.MQTTPort = "127.0.0.1", mustPort(t, addr)
	ms, err := serveMQTT(cfg, be, mqttOptions(cfg, tlsSrc), log)
	if err != nil {
		t.Fatalf("serveMQTT: %v", err)
	}
	t.Cleanup(func() { <-ms.BeginShutdown() })
	return addr
}

// publishReason publishes at QoS 1 and returns the PUBACK reason code.
func publishReason(t *testing.T, c *paho.Client, topic string) byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pr, err := c.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Payload: []byte(topic)})
	if pr == nil {
		t.Fatalf("publish %s: no PUBACK: %v", topic, err)
	}
	return pr.ReasonCode
}

// natsUserAccount connects to srv directly as user and returns the account its
// $SYS.REQ.USER.INFO reply names: the control that the identity itself is valid.
func natsUserAccount(t *testing.T, url string, opts ...nats.Option) string {
	t.Helper()
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("direct NATS connect: %v", err)
	}
	defer nc.Close()
	m, err := nc.Request("$SYS.REQ.USER.INFO", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("USER.INFO: %v", err)
	}
	var reply struct {
		Data struct {
			Account string `json:"account"`
		} `json:"data"`
	}
	if err := json.Unmarshal(m.Data, &reply); err != nil {
		t.Fatalf("USER.INFO reply %s: %v", m.Data, err)
	}
	return reply.Data.Account
}

// TestUserPasswordExampleAuthorizesPerUserTopics runs the broker against the
// user/password example docs/AUTHENTICATION.md documents and checks what it promises:
// each MQTT identity is held to its own topics, the reserved namespace and the JetStream
// API are refused with a reason an MQTT 5 client can see, and a user from another
// account is refused although it authenticates to NATS.
func TestUserPasswordExampleAuthorizesPerUserTopics(t *testing.T) {
	setNATSAuthExampleEnv(t)
	logs := recordBrokerLog(t)
	srv := startConfiguredServer(t, natsAuthExample, false)
	dial := func(user, pass string, opts ...nats.Option) *nats.Conn {
		t.Helper()
		nc, err := nats.Connect(srv.ClientURL(), append([]nats.Option{nats.UserInfo(user, pass)}, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		return nc
	}
	for _, u := range [][2]string{{"anonymous", "anonpass"}, {"alice", "alicepass"}, {"bob", "bobpass"}, {"outsider", "outsiderpass"}} {
		requireOffBrokerSubjects(t, dial, u[0], u[1])
	}

	cfg := config.Default()
	cfg.NATSUser, cfg.NATSPass = "mqtt-broker", "svcpass"
	cfg.DefaultUser, cfg.DefaultPass = "anonymous", "anonpass"
	addr := startExampleBroker(t, srv, cfg)

	// bob reads every sensor; alice reports hers.
	got := make(chan string, 4)
	bob, ca, err := v5Connect(t, addr, "dashboard", "bob", "bobpass", func(pr paho.PublishReceived) (bool, error) {
		got <- pr.Packet.Topic
		return true, nil
	})
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("bob connect: %v %+v", err, ca)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sa, _ := bob.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
		{Topic: "commands/alice/#", QoS: 1}, // bob may send commands, not read them
		{Topic: "sensors/#", QoS: 1},
	}})
	if sa == nil || len(sa.Reasons) != 2 || sa.Reasons[0] != 0x80 || sa.Reasons[1] != 1 {
		t.Fatalf("bob's SUBACK = %+v, want [0x80 1]: the denied filter refused, the allowed one granted", sa)
	}

	alice, ca, err := v5Connect(t, addr, "device-alice", "alice", "alicepass", nil)
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("alice connect: %v %+v", err, ca)
	}
	if rc := publishReason(t, alice, "sensors/alice/temp"); rc != 0 {
		t.Fatalf("alice's own sensor topic: PUBACK 0x%02x, want 0", rc)
	}
	select {
	case topic := <-got:
		if topic != "sensors/alice/temp" {
			t.Fatalf("bob received %q", topic)
		}
	case <-ctx.Done():
		t.Fatal("bob never received alice's reading")
	}
	for _, topic := range []string{"sensors/bob/temp", "$JS/API/INFO", "$MQTT/x"} {
		if rc := publishReason(t, alice, topic); rc != 0x87 {
			t.Errorf("alice publishing %s: PUBACK 0x%02x, want 0x87 (Not authorized)", topic, rc)
		}
	}
	select {
	case topic := <-got:
		t.Fatalf("a refused publication was delivered: %s", topic)
	case <-time.After(500 * time.Millisecond):
	}

	// outsider is a valid NATS identity in OTHER, and the broker refuses it. What refuses
	// it in a deployment is that its identity cannot be learned: the server answers the
	// USER.INFO request in the requester's account, and the broker collects the reply in
	// its own, so the request times out. The account comparison behind it is reached only
	// by the unit test TestIdentityShapesThatMustRefuseConnect (internal/mqtt).
	if acc := natsUserAccount(t, srv.ClientURL(), nats.UserInfo("outsider", "outsiderpass")); acc != "OTHER" {
		t.Fatalf("outsider's own USER.INFO names account %q, want OTHER", acc)
	}
	if _, ca, err := v5Connect(t, addr, "outsider", "outsider", "outsiderpass", nil); err == nil && ca.ReasonCode == 0 {
		t.Fatal("a user from another account was admitted")
	}
	waitFor(t, 5*time.Second, func() bool {
		return logs.logged(slog.LevelWarn, `refusing CONNECT for client "outsider": $SYS.REQ.USER.INFO: nats: timeout`)
	})
}

// TestCertificateOnlyClientIsTheDefaultIdentity pins what -tls-ca does and does not do
// for identity: it verifies a client certificate and maps nothing, so a client that
// presents a certificate and no user name is an anonymous client. Against a NATS
// server that refuses credential-free connections, it is refused unless the default
// identity is configured, and then holds exactly that identity's rights.
func TestCertificateOnlyClientIsTheDefaultIdentity(t *testing.T) {
	setNATSAuthExampleEnv(t)
	srv := startConfiguredServer(t, natsAuthExample, false)

	dir := t.TempDir()
	ca, err := testcerts.NewAuthority("example-ca")
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, data []byte) string {
		t.Helper()
		p, err := testcerts.Write(dir, name, data)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	srvCert, srvKey, err := ca.Issue("server", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	cliCert, cliKey, err := ca.Issue("device-carol")
	if err != nil {
		t.Fatal(err)
	}
	clientPair, err := tls.X509KeyPair(cliCert, cliKey)
	if err != nil {
		t.Fatal(err)
	}
	certOnly := func(addr, clientID string) (*paho.Client, *paho.Connack, error) {
		t.Helper()
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr,
			&tls.Config{RootCAs: ca.Pool(), Certificates: []tls.Certificate{clientPair}, MinVersion: tls.VersionTLS12})
		if err != nil {
			t.Fatalf("TLS dial: %v", err)
		}
		// paho serializes a packet's buffers only on a conn that is a sync.Locker. A
		// *tls.Conn writes each buffer separately, so without the wrapper the pinger's
		// first PINGREQ, sent at once, can land inside the first PUBLISH.
		c := paho.NewClient(paho.ClientConfig{Conn: packets.NewThreadSafeConn(conn)})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ack, err := c.Connect(ctx, &paho.Connect{ClientID: clientID, CleanStart: true, KeepAlive: 30})
		if err == nil {
			t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{}) })
		}
		return c, ack, err
	}
	newCfg := func() *config.Config {
		cfg := config.Default()
		cfg.NATSUser, cfg.NATSPass = "mqtt-broker", "svcpass"
		cfg.TLSCert, cfg.TLSKey, cfg.TLSCA = write("server.pem", srvCert), write("server.key", srvKey), write("ca.pem", ca.CertPEM)
		return cfg
	}

	// Control: with no default identity the certificate alone authenticates nothing.
	without := startExampleBroker(t, srv, newCfg())
	if _, ack, err := certOnly(without, "carol-without"); err == nil && ack.ReasonCode == 0 {
		t.Fatal("a certificate-only client was admitted with no default identity configured")
	}

	cfg := newCfg()
	cfg.DefaultUser, cfg.DefaultPass = "anonymous", "anonpass"
	with := startExampleBroker(t, srv, cfg)
	c, ack, err := certOnly(with, "carol-with")
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("a certificate-only client with a default identity configured: %v %+v", err, ack)
	}
	if rc := publishReason(t, c, "public/hello"); rc != 0 {
		t.Fatalf("the default identity's own topic: PUBACK 0x%02x, want 0", rc)
	}
	if rc := publishReason(t, c, "sensors/alice/temp"); rc != 0x87 {
		t.Fatalf("a topic the default identity may not publish: PUBACK 0x%02x, want 0x87", rc)
	}
}

// TestMQTTOnlyConnectionTypeIsRefusedThroughTheBroker pins a migration fact: a NATS
// user restricted to MQTT connections, the way users of nats-server's embedded listener
// may be configured, cannot authenticate through the broker, because auth passthrough
// dials a standard client connection. The two users are identical except for that
// restriction.
func TestMQTTOnlyConnectionTypeIsRefusedThroughTheBroker(t *testing.T) {
	conn := func(types ...string) map[string]struct{} {
		m := map[string]struct{}{}
		for _, ct := range types {
			m[ct] = struct{}{}
		}
		return m
	}
	opts := &server.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true, JetStream: true, StoreDir: t.TempDir(),
		Users: []*server.User{
			{Username: "svc", Password: "svcpass"},
			{Username: "carol", Password: "carolpass", AllowedConnectionTypes: conn("STANDARD")},
			{Username: "carol-mqtt", Password: "carolpass", AllowedConnectionTypes: conn("MQTT")},
		},
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })

	cfg := config.Default()
	cfg.NATSUser, cfg.NATSPass = "svc", "svcpass"
	addr := startExampleBroker(t, srv, cfg)
	if _, ca, err := v5Connect(t, addr, "carol", "carol", "carolpass", nil); err != nil || ca.ReasonCode != 0 {
		t.Fatalf("a user allowed STANDARD connections: %v %+v", err, ca)
	}
	if _, ca, err := v5Connect(t, addr, "carol-mqtt", "carol-mqtt", "carolpass", nil); err == nil && ca.ReasonCode == 0 {
		t.Fatal("a user allowed only MQTT connections was admitted through the broker")
	}
}
