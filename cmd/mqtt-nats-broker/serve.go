package main

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/config"
	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
)

// mqttTLSHandshakeTimeout bounds the server-side TLS handshake, in seconds.
const mqttTLSHandshakeTimeout = 2

// resolveTLS builds the MQTT listener's TLS configuration, returning nil when no
// TLS is configured. Any failure here is fatal to startup: an invalid or
// unreadable certificate, key, or client CA must never fall through to a
// plaintext listener. Callers must invoke this before establishing the NATS
// connection or binding the health server, so a broken TLS configuration is
// reported without side effects and regardless of whether NATS is reachable.
func resolveTLS(cfg *config.Config) (*tls.Config, error) {
	tc, err := cfg.TLSConfig()
	if err != nil {
		return nil, fmt.Errorf("MQTT TLS configuration: %w", err)
	}
	if cfg.TLSEnabled() && tc == nil {
		return nil, fmt.Errorf("MQTT TLS configuration: TLS is configured but no TLS config was produced")
	}
	return tc, nil
}

// mqttOptions builds the MQTT server options. tlsCfg comes from resolveTLS: a
// nil value means the listener serves plaintext, which at this point is a
// validated decision rather than a silent fallback.
func mqttOptions(cfg *config.Config, tlsCfg *tls.Config) *mqtt.Options {
	opts := &mqtt.Options{
		// The accept path's bound is the top-level field; MQTTOpts has none.
		MaxConn: cfg.MaxConns,
		MQTT: mqtt.MQTTOpts{
			Host:                   cfg.MQTTHost,
			Port:                   cfg.MQTTPort,
			ExperimentalSharedQoS2: false, // Parked; intentionally not configurable in production.
			// The option field carries upstream's name; the mechanism it feeds is
			// the CONNECT deadline (internal/mqtt/connect_deadline.go).
			AuthTimeout:         cfg.ConnectTimeout.Seconds(),
			StreamReplicas:      cfg.StreamReps,
			JsDomain:            cfg.JSDomain,
			KeepAliveMaximum:    cfg.KeepAliveMax,
			ResponseInformation: cfg.ResponseInfo,
		},
		StreamReplicas: cfg.StreamReps,
	}
	if tlsCfg != nil {
		opts.MQTT.TLSConfig = tlsCfg
		opts.MQTT.TLSTimeout = mqttTLSHandshakeTimeout
	}
	return opts
}

// serveMQTT starts the MQTT listener backed by the NATS/JetStream service
// connection, with per-client auth passthrough when enabled. opts is the
// mqttOptions result run already validated before any side effect; Start
// validates it again with the same function. It returns the running *mqtt.Server
// (whose BeginShutdown/ShutdownContext expose completion so the caller can wait
// for the full drain before tearing down dependencies) or an error if the
// listener failed to start. On error nothing is left running.
func serveMQTT(cfg *config.Config, be *backend.Backend, opts *mqtt.Options, log *slog.Logger) (*mqtt.Server, error) {
	nodeName, _ := os.Hostname()
	if nodeName == "" {
		nodeName = cfg.NATSName
	}

	s := mqtt.NewServer(be.Conn(), opts, nodeName)
	if cfg.AuthPassthrough {
		s.SetClientDialer(be.DialClient)
	}
	if err := s.Start(); err != nil {
		// Tear the half-started server down so it leaks nothing on the error path.
		s.Shutdown()
		return nil, fmt.Errorf("start MQTT listener: %w", err)
	}
	log.Info("MQTT listener started", "addr", s.Addr(), "node", nodeName, "tls", opts.MQTT.TLSConfig != nil)
	return s, nil
}
