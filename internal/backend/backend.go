// Package backend owns the connection(s) to the external NATS/JetStream cluster
// that provide the broker's highly-available state: sessions, retained messages,
// QoS1/2 persistence, and message routing.
//
// It exposes:
//   - a long-lived *service connection* used for JetStream asset management
//     (stream/consumer create, message store/load) — the transport that the
//     ported mqttJSA layer plugs into;
//   - a per-MQTT-client dial (Auth passthrough) so that NATS enforces
//     authorization for each MQTT client's subscriptions, and so the MQTT layer
//     can discover that client's account and permissions and police the routes
//     NATS cannot see -- the durable stores and QoS 1/2 delivery ride the service
//     connection, not this one.
package backend

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// Backend holds the service connection to NATS.
type Backend struct {
	cfg *config.Config
	log *slog.Logger

	nc *nats.Conn
}

// serviceInboxPrefix is where a library request on the service connection is answered.
// nats.go's default, _INBOX., is outside the broker's reserved namespace: an MQTT `#`
// subscription maps to NATS `>`, the `$`-topic rule does not cover _INBOX, so an MQTT client
// connected through another broker in the same account would receive the reply. Under
// $MQTT. the broker refuses MQTT subscriptions and publications, and the recommended client
// ACL denies both directions, so a reply here is neither observable nor forgeable by a
// client. The mqtt package builds its own reply subjects ($MQTT.JSA., $MQTT.uinfo.); this
// prefix governs only requests the client library makes itself.
const serviceInboxPrefix = "$MQTT.inbox"

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
	// without permissions on the service account. This is the only check that runs
	// before the MQTT listener opens: the mqtt package's first JetStream request is
	// made after it, when the account's session manager is created.
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
	return &Backend{cfg: cfg, log: log, nc: nc}, nil
}

// serviceOptions builds nats.Options for the long-lived service connection.
func serviceOptions(cfg *config.Config) []nats.Option {
	opts := []nats.Option{
		nats.Name(cfg.NATSName),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		// The reconnect buffer is KEPT, and the reasoning is recorded because it was
		// removed once and put back.
		//
		// Removing it was an attempt to make "an authorized write cannot cross a
		// service-account epoch" absolute: a buffered publish can be accepted while
		// the link is up and flushed after a reconnect, so if the connection came back
		// bound to a different account the write lands somewhere it was never
		// authorized for. But the buffer is only the outermost of several layers that
		// can hold a write across a reconnect -- the JSA send queue behind it does the
		// same -- so removing it narrowed nothing and COST something reachable: with no
		// buffer, a service blip makes a QoS 0 Will's publish fail immediately, and the
		// outbox worker then finalizes a Will that was never fanned out. An operator
		// repointing credentials mid-flight is rare; a NATS blip is not.
		//
		// The cross-epoch window is therefore a stated residual rather than something
		// this layer pretends to close. See docs/CONFORMANCE.md.
		nats.ReconnectBufSize(16 * 1024 * 1024),
		nats.CustomInboxPrefix(serviceInboxPrefix),
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

// DialClient opens a per-MQTT-client NATS connection using the supplied
// credentials, implementing auth passthrough. When user is empty the
// configured default credentials (if any) are used for anonymous clients.
//
// The returned connection carries that client's NATS authorization; the client's
// MQTT publishes and subscriptions ride it so NATS can enforce per-client
// permissions, and the MQTT layer asks it who it is so it can enforce the rest
// itself. Close it when the MQTT client disconnects.
//
// MaxReconnects(-1) means this connection outlives an outage and RE-AUTHENTICATES
// on each reconnect, possibly under changed permissions -- which is why the MQTT
// layer ties its identity snapshot to this connection's liveness rather than
// caching it for the session (see authz_identity.go).
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
