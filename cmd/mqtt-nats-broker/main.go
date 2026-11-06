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
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/config"
	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
)

// httpShutdownTimeout bounds the health server's shutdown after the MQTT lifecycle has
// completed; a deployment's termination grace budgets for it (docs/deploy/kubernetes).
const httpShutdownTimeout = 5 * time.Second

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	return runContext(context.Background(), args)
}

// runContext is run under parent: a signal or parent's end begins the shutdown, so a test
// can stop a run without signalling its own process.
func runContext(parent context.Context, args []string) error {
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
	tlsSrc, err := resolveTLS(cfg, log)
	if err != nil {
		return err
	}
	// The MQTT options are judged by the broker's own validation boundary, the one
	// Start runs, but here, before NATS is dialed or the health port is bound, so
	// an out-of-range setting fails startup with nothing to clean up.
	opts := mqttOptions(cfg, tlsSrc)
	if err := mqtt.ValidateOptions(opts); err != nil {
		return fmt.Errorf("invalid MQTT configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ready gates /readyz: false until the MQTT listener is up and initialized, and
	// flipped back to false on shutdown so a load balancer / k8s Service deregisters
	// this instance before its connections are drained. backendRef is the backend once
	// it is connected, for /readyz's connection check; nil before.
	var ready atomic.Bool
	var backendRef atomic.Pointer[backend.Backend]

	// The health endpoint is bound BEFORE the embedded server starts, synchronously so a
	// taken port fails startup here: /healthz then answers through local store recovery,
	// the JetStream wait and the MQTT initialization's retries, none of which is bounded
	// by a probe any more (issue #177); /readyz stays 503 until the broker serves.
	// Readiness = ready flag AND NATS connected. There is no metrics endpoint.
	var httpSrv *http.Server
	if cfg.HTTPAddr != "" {
		httpSrv, err = startHTTP(cfg.HTTPAddr, &backendRef, &ready, log)
		if err != nil {
			return err
		}
	}
	// The profiler, when asked for, lives exactly as long as the health endpoint.
	var pprofSrv *http.Server
	if cfg.PprofAddr != "" {
		pprofSrv, _ = startPprof(cfg.PprofAddr, log)
	}
	shutdownHTTP := func() {
		sctx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		defer cancel()
		for _, srv := range []*http.Server{httpSrv, pprofSrv} {
			if srv != nil {
				_ = srv.Shutdown(sctx)
			}
		}
	}
	// Every return from here shuts HTTP down; it runs after the embedded server and the
	// backend have stopped (defers are LIFO), so the health port outlives them.
	defer shutdownHTTP()

	// An embedded server starts before anything dials it and stops after everything
	// that needed it: the defers run LIFO, so the backend's drain finishes first.
	ns, err := startEmbeddedNATS(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer stopEmbeddedNATS(ns)
	var inProcess nats.InProcessConnProvider
	if ns != nil {
		inProcess = ns
	}

	be, err := backend.Connect(ctx, cfg, inProcess, log)
	if err != nil {
		return err
	}
	defer be.Close()
	backendRef.Store(be)

	// Start the MQTT listener backed by the NATS/JetStream connection, retrying its
	// initialization until JetStream answers or a signal ends startup. On failure the
	// deferred be.Close() and HTTP shutdown keep cleanup reachable so a failed startup
	// leaks nothing.
	srv, err := serveMQTT(ctx, cfg, be, opts, log)
	if err != nil {
		return err
	}
	// The TLS files are re-read from here until the server has fully stopped: the
	// listener still accepts during the drain grace period, so a renewal landing
	// then must still reach new handshakes. The deferred Stop runs after
	// BeginShutdown's completion below and joins the watcher.
	if tlsSrc != nil {
		tlsSrc.Start(cfg.TLSReloadInterval)
		defer tlsSrc.Stop()
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

// startHTTP binds the health endpoint synchronously and serves it in the background: a
// port that cannot be bound is a startup error, not a log line, so "the health port is
// answering" holds from the moment this returns.
func startHTTP(addr string, be *atomic.Pointer[backend.Backend], ready *atomic.Bool, log *slog.Logger) (*http.Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("health server: %w", err)
	}
	srv := &http.Server{Addr: addr, Handler: healthMux(be, ready), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("health server listening", "addr", l.Addr().String())
		if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server error", "err", err)
		}
	}()
	return srv, nil
}

// healthMux serves the probe endpoints: /healthz answers while the process serves HTTP,
// /readyz only while ready is set and the service connection is up. be is nil until the
// backend is connected, which /readyz answers as not ready.
func healthMux(be *atomic.Pointer[backend.Backend], ready *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// Ready only when the listener is up (and not draining) AND NATS is
		// connected. During shutdown ready flips false so probes see 503 while the
		// listener is still accepting, giving the LB time to deregister.
		var b *backend.Backend
		if be != nil {
			b = be.Load()
		}
		if b == nil {
			// Bound before the embedded server starts: recovery and the JetStream wait.
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("starting"))
			return
		}
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("draining"))
			return
		}
		if b.Conn().IsConnected() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("nats disconnected"))
	})
	return mux
}
