package mqtt

import (
	"fmt"
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// runNATSAuth starts an in-process JetStream server that requires credentials,
// with a service user (for the bridge) and an application user (for MQTT auth
// passthrough). Returns the client URL.
func runNATSAuth(t *testing.T) string {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		Users: []*natsserver.User{
			{Username: "svc", Password: "svcpw"},
			{Username: "alice", Password: "pw"},
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
	url := runNATSAuth(t)

	svc, err := nats.Connect(url, nats.UserInfo("svc", "svcpw"))
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(svc.Close)

	s := NewServer(svc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}, "auth-node")
	s.SetClientDialer(func(_, user, pass string) (*nats.Conn, error) {
		return nats.Connect(url, nats.UserInfo(user, pass))
	})
	if err := s.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(s.Shutdown)
	return fmt.Sprintf("tcp://%s", s.Addr().String())
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
