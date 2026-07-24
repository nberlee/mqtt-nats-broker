package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/nberlee/mqtt-nats-bridge/internal/backend"
	"github.com/nberlee/mqtt-nats-bridge/internal/config"
	"github.com/nberlee/mqtt-nats-bridge/internal/mqtt"
)

// serveMQTT starts the MQTT listener backed by the NATS/JetStream service
// connection, with per-client auth passthrough when enabled. It returns the
// running *mqtt.Server (whose BeginShutdown/ShutdownContext expose completion so
// the caller can wait for the full drain before tearing down dependencies) or an
// error if the listener failed to start. On error nothing is left running.
func serveMQTT(cfg *config.Config, be *backend.Backend, log *slog.Logger) (*mqtt.Server, error) {
	nodeName, _ := os.Hostname()
	if nodeName == "" {
		nodeName = cfg.NATSName
	}

	opts := &mqtt.Options{
		Host: cfg.MQTTHost,
		MQTT: mqtt.MQTTOpts{
			Host:                cfg.MQTTHost,
			Port:                cfg.MQTTPort,
			MaxConn:             cfg.MaxConns,
			StreamReplicas:      cfg.StreamReps,
			JsDomain:            cfg.JSDomain,
			KeepAliveMaximum:    int32(cfg.KeepAliveMax),
			ResponseInformation: cfg.ResponseInfo,
		},
		StreamReplicas: cfg.StreamReps,
	}
	if tc, err := cfg.TLSConfig(); err == nil && tc != nil {
		opts.MQTT.TLSConfig = tc
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
	log.Info("MQTT listener started", "addr", s.Addr(), "node", nodeName)
	return s, nil
}
