package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// startEmbeddedNATS loads the operator's nats-server configuration, enforces the
// broker's server prerequisites on it, starts the server and waits until it accepts
// connections. It returns nil, nil in external mode. On an error nothing is left
// running.
func startEmbeddedNATS(ctx context.Context, cfg *config.Config, log *slog.Logger) (*server.Server, error) {
	if cfg.NATSEmbeddedConfig == "" {
		return nil, nil
	}
	opts, err := server.ProcessConfigFile(cfg.NATSEmbeddedConfig)
	if err != nil {
		return nil, fmt.Errorf("embedded NATS configuration: %w", err)
	}
	if err := enforceEmbeddedOptions(opts, cfg.JSDomain); err != nil {
		return nil, fmt.Errorf("embedded NATS configuration %s: %w", cfg.NATSEmbeddedConfig, err)
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("embedded NATS server: %w", err)
	}
	srv.SetLoggerV2(natsLogger{log.With("component", "nats-server")}, opts.Debug, opts.Trace, false)
	go srv.Start()
	for !srv.ReadyForConnections(250 * time.Millisecond) {
		if ctx.Err() != nil {
			stopEmbeddedNATS(srv)
			return nil, fmt.Errorf("embedded NATS server not ready: %w", ctx.Err())
		}
	}
	log.Info("embedded NATS server ready", "server", srv.Name(), "config", cfg.NATSEmbeddedConfig)
	return srv, nil
}

// stopEmbeddedNATS shuts the embedded server down and waits for it. Shutdown steps down
// the raft leaderships the server holds and stops the file store in order. nil-safe.
func stopEmbeddedNATS(srv *server.Server) {
	if srv == nil {
		return
	}
	srv.Shutdown()
	srv.WaitForShutdown()
}

// enforceEmbeddedOptions makes a parsed configuration the broker's backend or refuses
// it. Everything it does not mention is the operator's. It writes two values: NoSigs,
// because the process's signals are the broker's (nats-server's handler would exit the
// process before the MQTT drain and reload the file around this function on SIGHUP), and
// an unset consumer default, which it sets to the one value the broker's streams need.
// Errors name the setting and never its value, since some are credentials.
func enforceEmbeddedOptions(opts *server.Options, jsDomain string) error {
	opts.NoSigs = true

	if !opts.JetStream {
		return errors.New("jetstream must be enabled: sessions, QoS 1/2 and retained messages are stored in it")
	}
	// The default is a temporary directory: unwritable under a read-only root, and gone
	// with the pod.
	if opts.StoreDir == "" {
		return errors.New("jetstream store_dir must be set")
	}
	// Zero is what both an absent and an explicit 0 parse to, and the server reads it as
	// "use the default", which caps every stream at 1000 consumers (F-095). The broker's
	// streams require no cap.
	switch opts.JetStreamLimits.DefaultMaxConsumers {
	case 0:
		opts.JetStreamLimits.DefaultMaxConsumers = -1
	case -1:
	default:
		return fmt.Errorf("jetstream limits default_max_consumers must be -1 (unlimited) or unset, not %d: "+
			"the broker's streams carry one consumer per subscription", opts.JetStreamLimits.DefaultMaxConsumers)
	}
	// An unserved domain has no responder, which a client cannot tell from JetStream
	// not being up yet; startup would wait for it forever.
	if jsDomain != "" && jsDomain != opts.JetStreamDomain {
		return fmt.Errorf("js-domain %q is not this server's jetstream domain %q", jsDomain, opts.JetStreamDomain)
	}

	// Judged on effect: an empty block parses to the same options as an absent one and
	// enables nothing.
	switch {
	case opts.MQTT.Port != 0:
		return errors.New("the mqtt listener must not be enabled: this broker serves MQTT")
	case opts.Websocket.Port != 0:
		return errors.New("the websocket listener must not be enabled")
	case opts.LeafNode.Port != 0 || len(opts.LeafNode.Remotes) > 0:
		return errors.New("leafnodes are not supported in embedded mode")
	case opts.Gateway.Port != 0 || opts.Gateway.Name != "" || len(opts.Gateway.Gateways) > 0:
		return errors.New("gateways are not supported in embedded mode")
	}

	// A route listener without authentication lets anyone who reaches it join the
	// cluster and read or inject account traffic, past every client ACL.
	if opts.Cluster.Port != 0 && opts.Cluster.Username == "" && !opts.Cluster.TLSMap &&
		!opts.Cluster.TLSCheckKnownURLs && opts.CustomRouterAuthentication == nil {
		return errors.New("cluster routes must be authenticated: set cluster authorization (or a TLS-verified route map)")
	}
	if opts.Cluster.Username != "" && opts.Cluster.Password == "" {
		return errors.New("cluster authorization: password must not be empty")
	}

	// The process's outputs are the broker's: its logger replaces the server's, and the
	// root filesystem may be read-only.
	switch {
	case opts.LogFile != "":
		return errors.New("log_file must not be set: the server logs through the broker")
	case opts.Syslog || opts.RemoteSyslog != "":
		return errors.New("syslog must not be set: the server logs through the broker")
	case opts.PidFile != "":
		return errors.New("pid_file must not be set")
	case opts.PortsFileDir != "":
		return errors.New("ports_file_dir must not be set")
	}

	// An environment reference set to "" yields a user anyone can authenticate as.
	for i, u := range opts.Users {
		if u.Username == "" {
			return fmt.Errorf("users[%d]: user must not be empty", i)
		}
		if u.Password == "" {
			return fmt.Errorf("user %q: password must not be empty", u.Username)
		}
	}
	return nil
}

// natsLogger writes the embedded server's log through the broker's logger, so the
// process emits one stream of JSON lines.
type natsLogger struct{ log *slog.Logger }

func (l natsLogger) Noticef(format string, v ...any) { l.log.Info(fmt.Sprintf(format, v...)) }
func (l natsLogger) Warnf(format string, v ...any)   { l.log.Warn(fmt.Sprintf(format, v...)) }
func (l natsLogger) Errorf(format string, v ...any)  { l.log.Error(fmt.Sprintf(format, v...)) }
func (l natsLogger) Debugf(format string, v ...any)  { l.log.Debug(fmt.Sprintf(format, v...)) }
func (l natsLogger) Tracef(format string, v ...any)  { l.log.Debug(fmt.Sprintf(format, v...)) }

// Fatalf exits, as nats-server's own logger does: the server calls it where it cannot
// continue and does not expect it to return.
func (l natsLogger) Fatalf(format string, v ...any) {
	l.log.Error(fmt.Sprintf(format, v...))
	os.Exit(1)
}
