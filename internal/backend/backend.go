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
	"errors"
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
	// inProcess is the embedded server every connection is dialed through, or nil when
	// the broker connects to NATS by URL. Per-client connections must take the same route
	// as the service connection: they authenticate against the same server.
	inProcess nats.InProcessConnProvider
	// closed is closed by the connection's ClosedHandler, which nats.go runs once the
	// connection is finally closed: after a drain has finished, or when it gave up.
	closed chan struct{}
}

// serviceDrainTimeout bounds the service connection's drain at shutdown: how long nats.go
// waits for the subscriptions to finish before it closes anyway. Its publish flush after
// that has its own five-second bound inside nats.go.
const serviceDrainTimeout = 10 * time.Second

// ServiceCloseBudget is the longest Close waits for the closed callback. Above the drain
// and the flush, with slack for the callback itself: nats.go runs connection callbacks
// one at a time, so the closed callback can queue behind another one. Past it Close
// forces the connection closed rather than let shutdown hang on a callback. A
// deployment's termination grace budgets for it (docs/deploy/kubernetes).
const ServiceCloseBudget = serviceDrainTimeout + 10*time.Second

// serviceCloseWait is the bound Close applies: ServiceCloseBudget, a var so a test can
// shorten it.
var serviceCloseWait = ServiceCloseBudget

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
// guarantees without it). inProcess is the embedded server to dial through, or nil to
// connect to cfg.NATSURLs.
func Connect(ctx context.Context, cfg *config.Config, inProcess nats.InProcessConnProvider, log *slog.Logger) (*Backend, error) {
	closed := make(chan struct{})
	opts := append(serviceOptions(cfg),
		nats.DrainTimeout(serviceDrainTimeout),
		nats.ClosedHandler(func(*nats.Conn) { close(closed) }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			// Asynchronous errors had no handler at all, so a drain that timed out at
			// shutdown — or a subscription that fell behind — was silent.
			log.Warn("NATS service connection error", "err", err)
		}),
	)
	if inProcess != nil {
		opts = append(opts, nats.InProcessServer(inProcess))
	}
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
	if inProcess != nil {
		err = waitJetStream(ctx, cfg, nc, js, log)
	} else {
		pctx, cancel := context.WithTimeout(ctx, cfg.JSTimeout)
		_, err = js.AccountInfo(pctx)
		cancel()
	}
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("JetStream not available on service connection: %w", err)
	}

	urls := cfg.NATSURLs
	if inProcess != nil {
		urls = "" // not dialed
	}
	log.Info("connected to NATS backend",
		"urls", urls,
		"in_process", inProcess != nil,
		"server", nc.ConnectedServerName(),
		"cluster", nc.ConnectedClusterName(),
		"js_domain", cfg.JSDomain,
	)
	return &Backend{cfg: cfg, log: log, nc: nc, inProcess: inProcess, closed: closed}, nil
}

// jsReadyRetryWait is the pause between JetStream probes while an embedded server's
// JetStream is not answering yet. A var so a test can lengthen it.
var jsReadyRetryWait = time.Second

// jsWaitLogEvery bounds how often the wait reports that it is still waiting.
const jsWaitLogEvery = 10 * time.Second

// waitJetStream probes JetStream until it answers, for an embedded server only. The
// broker process is then part of the JetStream it waits for: in a clustered cold start
// the meta leader needs a quorum of pods, so failing on the first probe would turn every
// cold start into a crash loop that tears the local peer down with it. Only conditions
// that mean "not answering yet" are waited on; anything else fails at once, so a
// misconfiguration never looks like a cluster still forming.
//
// nc must be the fresh connection Connect just made, with nothing published or
// subscribed on it but these probes: a permission violation is read from nc.LastError,
// which is connection-wide and sticky, and is this probe's only because nothing else
// can have caused one yet.
func waitJetStream(ctx context.Context, cfg *config.Config, nc *nats.Conn, js jetstream.JetStream, log *slog.Logger) error {
	var lastLogged time.Time
	for {
		pctx, cancel := context.WithTimeout(ctx, cfg.JSTimeout)
		_, err := js.AccountInfo(pctx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !jetStreamNotReadyYet(err, nc) {
			return err
		}
		if now := time.Now(); lastLogged.IsZero() || now.Sub(lastLogged) >= jsWaitLogEvery {
			log.Info("waiting for JetStream", "err", err)
			lastLogged = now
		}
		t := time.NewTimer(jsReadyRetryWait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// jsErrCodeClusterNotAvail is nats-server's JSClusterNotAvailErr, "JetStream system
// temporarily unavailable": the answer while a cluster has no meta leader. nats.go
// names no constant for it.
const jsErrCodeClusterNotAvail jetstream.ErrorCode = 10008

// jetStreamNotReadyYet reports whether a failed probe of an embedded server means its
// JetStream is not answering yet. Startup has already refused a server without
// JetStream and a domain the server does not serve, the two permanent causes of "not
// enabled" and of no responders, which the client reports alike.
func jetStreamNotReadyYet(err error, nc *nats.Conn) bool {
	var apiErr *jetstream.APIError
	switch {
	case errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount):
		return false
	case errors.Is(err, jetstream.ErrJetStreamNotEnabled):
		return true
	case errors.As(err, &apiErr):
		return apiErr.ErrorCode == jsErrCodeClusterNotAvail
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		// A request the server refused to carry also ends in a timeout; nats.go records
		// the refusal on the connection.
		return !errors.Is(nc.LastError(), nats.ErrPermissionViolation)
	}
	return false
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
	if b.inProcess != nil {
		opts = append(opts, nats.InProcessServer(b.inProcess))
	}
	nc, err := nats.Connect(b.cfg.NATSURLs, opts...)
	if err != nil {
		return nil, fmt.Errorf("auth passthrough dial for client %q: %w", clientID, err)
	}
	return nc, nil
}

// Close drains the service connection and returns once it is closed.
//
// Drain is asynchronous: it returns at once and finishes the subscriptions, then flushes
// what is still buffered, on its own goroutine. Returning without waiting let the process
// exit in the middle of that, losing whatever had not been flushed (a QoS 0 Will fan-out
// issued late in shutdown, for one). Close returning means the connection is closed, or
// was forced closed at serviceCloseWait; it does not certify that every buffered publish
// reached the server when the drain itself timed out (that is logged).
func (b *Backend) Close() {
	if b.nc == nil {
		return
	}
	// An error means the connection is already closed or could not drain (nats.go closes
	// a reconnecting connection instead of draining it); either way the closed callback
	// follows, so the wait below is the same.
	_ = b.nc.Drain()
	if testCloseWaitEntered != nil {
		testCloseWaitEntered()
	}
	t := time.NewTimer(serviceCloseWait)
	defer t.Stop()
	select {
	case <-b.closed:
	case <-t.C:
		b.log.Warn("NATS service connection did not finish closing in time; closing it now",
			"wait", serviceCloseWait)
		b.nc.Close()
	}
	// An embedded server is stopped only after this line: the drain above needed it.
	b.log.Info("NATS service connection closed")
}

// testCloseWaitEntered runs when Close has started the drain and is about to wait for it.
// Nil in production.
var testCloseWaitEntered func()
