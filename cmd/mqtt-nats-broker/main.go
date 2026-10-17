// Command mqtt-nats-broker is a standalone, horizontally-scalable MQTT 3.1.1 / 5.0
// broker that uses an external NATS/JetStream cluster as its highly-available
// backend for sessions, retained messages, QoS1/2 persistence, and routing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := config.Load(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	log := newLogger(cfg.LogLevel)
	log.Info("starting mqtt-nats-broker", "version", version)

	// The soft memory limit is in place before anything allocates on the
	// broker's behalf, so the GC paces against the container ceiling from the
	// first connection on.
	applyGoMemoryLimit(log)

	// Resolve the listener TLS configuration before anything observable starts:
	// a broken certificate, key, or client CA must fail startup outright rather
	// than degrade to a plaintext listener, and must be reported even when NATS
	// is unreachable.
	tlsCfg, err := resolveTLS(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	be, err := backend.Connect(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer be.Close()

	// ready gates /readyz: false until the MQTT listener is up, and flipped back
	// to false on shutdown so a load balancer / k8s Service deregisters this
	// instance before its connections are drained.
	var ready atomic.Bool

	// Health/metrics endpoint (readiness = ready flag AND NATS connected).
	var httpSrv *http.Server
	if cfg.HTTPAddr != "" {
		httpSrv = startHTTP(cfg.HTTPAddr, be, &ready, log)
	}
	shutdownHTTP := func() {
		if httpSrv == nil {
			return
		}
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
	}

	// Start the MQTT listener backed by the NATS/JetStream connection. On failure
	// the deferred be.Close() and the explicit HTTP shutdown keep cleanup
	// reachable so a failed startup leaks nothing.
	srv, err := serveMQTT(cfg, be, tlsCfg, log)
	if err != nil {
		shutdownHTTP()
		return err
	}
	ready.Store(true)

	<-ctx.Done()
	log.Info("shutdown signal received, draining")
	// The first signal already cancelled ctx. To let a genuine second signal cut
	// the grace period short, install the replacement handler BEFORE releasing the
	// original NotifyContext handler with stop(): registering after stop() would
	// leave a gap in which a signal falls through to default (immediate)
	// termination, bypassing the graceful drain. Signals are not replayed, so the
	// already-delivered first signal is not copied into sigCh.
	var sigCh chan os.Signal
	if cfg.DrainDelay > 0 {
		sigCh = make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	}
	stop()

	// Flip readiness off immediately, then (optionally) wait out the pre-drain
	// grace period so the Service/LB stops routing new connections here before we
	// disconnect existing clients. The health server stays up through the drain
	// returning 503, not connection-refused.
	ready.Store(false)
	if cfg.DrainDelay > 0 {
		log.Info("draining, waiting for endpoint deregistration", "delay", cfg.DrainDelay)
		waitDrainGrace(cfg.DrainDelay, sigCh)
		signal.Stop(sigCh)
	}

	// Begin the MQTT shutdown lifecycle and wait for it to fully complete before
	// tearing down dependencies (backend/HTTP). We do not advance teardown merely
	// because a bounded status wait elapsed: an orchestrator SIGKILL at the pod
	// grace deadline is the final bound.
	done := srv.BeginShutdown()
	<-done

	shutdownHTTP()
	return nil
}

// waitDrainGrace blocks for delay so a Service/LB can deregister this instance,
// returning early if a signal arrives on sigCh (a genuine second SIGTERM/SIGINT
// cuts the grace short). A non-positive delay returns immediately.
func waitDrainGrace(delay time.Duration, sigCh <-chan os.Signal) {
	if delay <= 0 {
		return
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
	case <-sigCh:
	}
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func startHTTP(addr string, be *backend.Backend, ready *atomic.Bool, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// Ready only when the listener is up (and not draining) AND NATS is
		// connected. During shutdown ready flips false so probes see 503 while the
		// listener is still accepting, giving the LB time to deregister.
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("draining"))
			return
		}
		if be.Conn().IsConnected() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("nats disconnected"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("health server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server error", "err", err)
		}
	}()
	return srv
}
