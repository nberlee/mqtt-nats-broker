package mqtt

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// runNATSAuth starts an in-process JetStream server that requires credentials,
// with a service user (for the broker) and an application user (for MQTT auth
// passthrough). Returns the client URL.
func runNATSAuth(t *testing.T) string {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		Users: []*natsserver.User{
			{Username: "svc", Password: "svcpw"},
			// The deny auth passthrough requires of an MQTT identity, carried here so
			// the fixture matches a supported deployment. The broker cannot enforce
			// it -- the identity reply is unsigned and lands in the reserved
			// namespace, so a principal able to publish there can answer for anyone
			// -- which is why docs/ARCHITECTURE.md states it as a prerequisite.
			{Username: "alice", Password: "pw", Permissions: &natsserver.Permissions{
				Publish:   &natsserver.SubjectPermission{Allow: []string{">"}, Deny: []string{mqttPrefix + ">"}},
				Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

func startAuthBroker(t *testing.T) string {
	t.Helper()
	_, addr := startAuthBrokerTuned(t, nil, nil)
	return fmt.Sprintf("tcp://%s", addr)
}

// startAuthBrokerTuned is startAuthBroker with the two hooks the CONNECT-deadline
// tests need: tune adjusts this server's Options (never a package-level var, so
// callers keep t.Parallel()), and dials -- when non-nil -- counts per-client NATS
// dials. Both are per server, which is what makes the counter function-local to
// the calling test.
func startAuthBrokerTuned(t *testing.T, tune func(*Options), dials *atomic.Int64) (*Server, string) {
	t.Helper()
	url := runNATSAuth(t)

	svc, err := nats.Connect(url, nats.UserInfo("svc", "svcpw"))
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(svc.Close)

	o := &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}
	if tune != nil {
		tune(o)
	}
	s := NewServer(svc, o, "auth-node")
	s.SetClientDialer(func(_, user, pass string) (*nats.Conn, error) {
		if dials != nil {
			dials.Add(1)
		}
		return nats.Connect(url, nats.UserInfo(user, pass))
	})
	if err := s.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(s.Shutdown)
	return s, s.Addr().String()
}

// TestMQTTConnectDeadlineUnderAuthPassthrough is the finding's literal pin: with a
// client dialer installed, a peer that opens a socket and sends no CONNECT is
// closed by the deadline. The valid-credential connect on the same broker is what
// shows the auth-passthrough path is genuinely in play rather than the bound being
// measured on a broker that never authenticates anyone.
func TestMQTTConnectDeadlineUnderAuthPassthrough(t *testing.T) {
	t.Parallel()
	_, addr := startAuthBrokerTuned(t, func(o *Options) { o.MQTT.AuthTimeout = 0.3 }, nil)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	expectClosedBetween(t, c, 200*time.Millisecond, 5*time.Second)

	if err := tryConnect("tcp://"+addr, "authed", "alice", "pw"); err != nil {
		t.Fatalf("expected the same broker to accept valid credentials, got: %v", err)
	}
}

// TestMQTTAuthRejectionIsNotTheConnectDeadline pins that CONNECT authentication is
// unchanged: with a five-second window, a CONNECT carrying bad credentials is
// refused far inside it, so the refusal is the dialer's and not the deadline's.
func TestMQTTAuthRejectionIsNotTheConnectDeadline(t *testing.T) {
	t.Parallel()
	_, addr := startAuthBrokerTuned(t, func(o *Options) { o.MQTT.AuthTimeout = 5 }, nil)

	start := time.Now()
	if err := tryConnect("tcp://"+addr, "bad-creds", "alice", "wrong-password"); err == nil {
		t.Fatal("expected connect rejection for invalid creds, got success")
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("rejection took %v with a 5s CONNECT window: that is the deadline closing the socket, not the dialer refusing the credentials", el)
	}
}

// TestMQTTExpiredConnectDialsNoNATSConnection pins the ORDERING the refusal
// depends on: a CONNECT that lost the race to the deadline is refused before
// isClientAuthorized, so it dials no per-client NATS connection, looks up no
// account session manager and binds no session state -- there is nothing to
// unwind.
//
// Driven by an in-package call rather than a socket, and deliberately. Through a
// real socket the callback has already closed rawConn and the readLoop has already
// exited, so the CONNECT never reaches mqttProcessConnect at all and "no dial"
// would hold just as well against a refusal placed after the dial.
//
// The credentials are valid on purpose: a refusal moved after isClientAuthorized
// would dial successfully and move the counter. Invalid credentials would fail the
// dial for an unrelated reason and would not distinguish the two placements. c.acc
// is wired for the same reason -- past the dial, execution reaches
// getOrCreateMQTTAccountSessionManager, which calls acc.GetName() under c.mu, so a
// nil account would turn this mutation check into a panic instead of a failed
// assertion.
func TestMQTTExpiredConnectDialsNoNATSConnection(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	s, _ := startAuthBrokerTuned(t, nil, &dials)

	broker, peer := net.Pipe()
	defer broker.Close()
	defer peer.Close()

	c := &client{srv: s, nc: broker, rawConn: broker, mqtt: &mqtt{cid: "lost-the-race"}}
	// registerWithAccount takes c.mu itself, so it runs before the admission state
	// is forced rather than inside that critical section.
	c.registerWithAccount(s.globalAccount())
	c.mu.Lock()
	// Credentials live where the CONNECT parser writes them, not on the proto.
	c.opts.Username, c.opts.Password = "alice", "pw"
	c.connectAdmission = mqttConnectExpired
	c.mu.Unlock()

	err := s.mqttProcessConnect(c, &mqttConnectProto{flags: mqttConnFlagCleanSession}, false)
	if !errors.Is(err, errMQTTConnectDeadlineExpired) {
		t.Fatalf("expected %v, got %v", errMQTTConnectDeadlineExpired, err)
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("a CONNECT that lost the race to the deadline dialed %d per-client NATS connection(s); the refusal must precede isClientAuthorized", n)
	}
}

func tryConnect(broker, id, user, pass string) error {
	opts := pmqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(id).
		SetUsername(user).
		SetPassword(pass).
		SetConnectTimeout(5 * time.Second).
		SetConnectRetry(false)
	c := pmqtt.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("connect timed out")
	}
	err := tok.Error()
	c.Disconnect(100)
	return err
}

// TestAuthPassthroughAccept verifies a client with valid NATS credentials
// connects successfully.
func TestAuthPassthroughAccept(t *testing.T) {
	t.Parallel()
	broker := startAuthBroker(t)
	if err := tryConnect(broker, "good", "alice", "pw"); err != nil {
		t.Fatalf("expected accept for valid creds, got: %v", err)
	}
}

// TestAuthPassthroughReject verifies a client with invalid NATS credentials is
// rejected (the per-client NATS dial fails, so CONNECT is not authorized).
func TestAuthPassthroughReject(t *testing.T) {
	t.Parallel()
	broker := startAuthBroker(t)
	if err := tryConnect(broker, "bad", "alice", "wrong-password"); err == nil {
		t.Fatal("expected connect rejection for invalid creds, got success")
	}
}

// authClient connects an authenticated MQTT 3.1.1 client (per-client NATS dial).
func authClient(t *testing.T, broker, id string) pmqtt.Client {
	t.Helper()
	opts := pmqtt.NewClientOptions().
		AddBroker(broker).SetClientID(id).SetProtocolVersion(4).
		SetUsername("alice").SetPassword("pw").
		SetConnectTimeout(5 * time.Second).SetCleanSession(true)
	c := pmqtt.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("auth connect %s: %v", id, tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// TestAuthPassthroughStoredDelivery verifies retained-message replay works over
// per-client (auth-passthrough) NATS connections, not just the shared service
// connection — i.e. the closeConnection subscription cleanup is correct in both
// connection modes.
func TestAuthPassthroughStoredDelivery(t *testing.T) {
	t.Parallel()
	broker := startAuthBroker(t)

	pub := authClient(t, broker, "auth-ret-pub")
	if tok := pub.Publish("auth/ret", 1, true, "kept"); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish retained: %v", tok.Error())
	}

	sub := authClient(t, broker, "auth-ret-sub")
	got := make(chan string, 1)
	if tok := sub.Subscribe("auth/ret", 1, func(_ pmqtt.Client, m pmqtt.Message) {
		got <- string(m.Payload())
	}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	select {
	case msg := <-got:
		if msg != "kept" {
			t.Fatalf("retained payload = %q, want kept", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for retained message over per-client conn")
	}
}
