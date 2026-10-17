package backend

import (
	"context"
	"log/slog"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"

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
	if be.JetStream() == nil {
		t.Fatal("JetStream context is nil")
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
