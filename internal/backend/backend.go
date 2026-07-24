// Package backend owns the connection(s) to the external NATS/JetStream cluster
// that provide the broker's highly-available state: sessions, retained messages,
// QoS1/2 persistence, and message routing.
//
// It exposes:
//   - a long-lived *service connection* used for JetStream asset management
//     (stream/consumer create, message store/load) — the transport that the
//     ported mqttJSA layer plugs into;
//   - a per-MQTT-client dial (Auth passthrough) so that NATS enforces
//     authorization for each MQTT client's publishes and subscriptions.
package backend

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nberlee/mqtt-nats-bridge/internal/config"
)

// Backend holds the service connection to NATS and its JetStream context.
type Backend struct {
	cfg *config.Config
	log *slog.Logger

	nc *nats.Conn
	js jetstream.JetStream
}

// Connect establishes the service connection to NATS and verifies that
// JetStream is available (the broker cannot provide QoS/retained/session
// guarantees without it).
func Connect(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Backend, error) {
	opts := serviceOptions(cfg)
	nc, err := nats.Connect(cfg.NATSURLs, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}

	var js jetstream.JetStream
	if cfg.JSDomain != "" {
		js, err = jetstream.NewWithDomain(nc, cfg.JSDomain)
	} else {
		js, err = jetstream.New(nc)
	}
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("init JetStream: %w", err)
	}

	// Probe JetStream so we fail fast on a cluster without JS enabled or
	// without permissions on the service account.
	pctx, cancel := context.WithTimeout(ctx, cfg.JSTimeout)
	defer cancel()
	if _, err := js.AccountInfo(pctx); err != nil {
		nc.Close()
		return nil, fmt.Errorf("JetStream not available on service connection: %w", err)
	}

	log.Info("connected to NATS backend",
		"urls", cfg.NATSURLs,
		"server", nc.ConnectedServerName(),
		"cluster", nc.ConnectedClusterName(),
		"js_domain", cfg.JSDomain,
	)
	return &Backend{cfg: cfg, log: log, nc: nc, js: js}, nil
}

// serviceOptions builds nats.Options for the long-lived service connection.
func serviceOptions(cfg *config.Config) []nats.Option {
	opts := []nats.Option{
		nats.Name(cfg.NATSName),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.ReconnectBufSize(16 * 1024 * 1024),
	}
	if cfg.NATSCreds != "" {
		opts = append(opts, nats.UserCredentials(cfg.NATSCreds))
	}
	if cfg.NATSUser != "" {
		opts = append(opts, nats.UserInfo(cfg.NATSUser, cfg.NATSPass))
	}
	return opts
}

// Conn returns the service NATS connection (used by the JetStream-assets transport).
func (b *Backend) Conn() *nats.Conn { return b.nc }

// JetStream returns the service JetStream context.
func (b *Backend) JetStream() jetstream.JetStream { return b.js }

// DialClient opens a per-MQTT-client NATS connection using the supplied
// credentials, implementing auth passthrough. When user is empty the
// configured default credentials (if any) are used for anonymous clients.
//
// The returned connection carries that client's NATS authorization; all of the
// client's MQTT publishes and subscriptions ride it so NATS enforces per-client
// permissions. Close it when the MQTT client disconnects.
func (b *Backend) DialClient(clientID, user, pass string) (*nats.Conn, error) {
	if user == "" {
		user, pass = b.cfg.DefaultUser, b.cfg.DefaultPass
	}
	opts := []nats.Option{
		nats.Name(fmt.Sprintf("%s:mqtt:%s", b.cfg.NATSName, clientID)),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}
	if user != "" {
		opts = append(opts, nats.UserInfo(user, pass))
	}
	nc, err := nats.Connect(b.cfg.NATSURLs, opts...)
	if err != nil {
		return nil, fmt.Errorf("auth passthrough dial for client %q: %w", clientID, err)
	}
	return nc, nil
}

// Close tears down the service connection.
func (b *Backend) Close() {
	if b.nc != nil {
		b.nc.Drain() //nolint:errcheck // best effort on shutdown
	}
}
