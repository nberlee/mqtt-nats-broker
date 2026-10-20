package backend

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// runJetStreamServer starts an in-process NATS server with JetStream enabled on a
// random port and returns its client URL.
func runJetStreamServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func TestConnect_JetStreamAvailable(t *testing.T) {
	srv := runJetStreamServer(t)

	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()

	be, err := Connect(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer be.Close()

	if !be.Conn().IsConnected() {
		t.Fatal("service connection not connected")
	}
}

// TestConnect_ProbeRepliesInsideTheReservedNamespace pins where the startup JetStream probe
// is answered. The default inbox, _INBOX., is reachable by an MQTT `#` subscriber in the same
// account; the reply must arrive under $MQTT., which no MQTT client can subscribe to.
//
// The observer subscribes to both prefixes and FLUSHES before the probe runs, so an empty
// _INBOX. channel is an absence the server could have filled, not a subscription that had not
// arrived yet; the $MQTT.inbox. arrival is the positive control that the observer sees the
// probe's traffic at all.
func TestConnect_ProbeRepliesInsideTheReservedNamespace(t *testing.T) {
	srv := runJetStreamServer(t)

	obs, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("observer connect: %v", err)
	}
	defer obs.Close()
	defaultInbox := make(chan *nats.Msg, 16)
	reserved := make(chan *nats.Msg, 16)
	if _, err := obs.ChanSubscribe("_INBOX.>", defaultInbox); err != nil {
		t.Fatalf("subscribe _INBOX.>: %v", err)
	}
	if _, err := obs.ChanSubscribe(serviceInboxPrefix+".>", reserved); err != nil {
		t.Fatalf("subscribe %s.>: %v", serviceInboxPrefix, err)
	}
	if err := obs.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("flush the observer: %v", err)
	}

	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()
	be, err := Connect(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer be.Close()

	select {
	case m := <-reserved:
		if !strings.HasPrefix(m.Subject, serviceInboxPrefix+".") {
			t.Fatalf("the probe reply arrived on %q, outside %s.", m.Subject, serviceInboxPrefix)
		}
		var resp struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(m.Data, &resp); err != nil || resp.Type != natsserver.JSApiAccountInfoResponseType {
			t.Fatalf("the message on %q is not the account-info reply (type %q, %v)", m.Subject, resp.Type, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no probe reply arrived under %s.", serviceInboxPrefix)
	}
	select {
	case m := <-defaultInbox:
		t.Fatalf("the probe reply was published on %q, which an MQTT `#` subscriber can receive", m.Subject)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestConnect_FailsWithoutJetStream: the probe is the only JetStream check before the MQTT
// listener opens, so a service account without JetStream must be refused here.
func TestConnect_FailsWithoutJetStream(t *testing.T) {
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)

	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()
	cfg.JSTimeout = 2 * time.Second
	be, err := Connect(context.Background(), cfg, slog.Default())
	if err == nil {
		be.Close()
		t.Fatal("Connect succeeded against a server without JetStream")
	}
}

func TestConnect_NoServer(t *testing.T) {
	cfg := config.Default()
	cfg.NATSURLs = "nats://127.0.0.1:1" // nothing listening
	cfg.JSTimeout = 500 * time.Millisecond

	_, err := Connect(context.Background(), cfg, slog.Default())
	if err == nil {
		t.Fatal("expected connect error, got nil")
	}
}

func TestDialClient_AnonymousDefaults(t *testing.T) {
	srv := runJetStreamServer(t)

	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()

	be, err := Connect(context.Background(), cfg, slog.Default())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer be.Close()

	nc, err := be.DialClient("client-1", "", "")
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	defer nc.Close()
	if !nc.IsConnected() {
		t.Fatal("per-client connection not connected")
	}
}
