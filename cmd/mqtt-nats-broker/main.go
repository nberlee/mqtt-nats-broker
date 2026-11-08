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
	"github.com/nberlee/mqtt-nats-broker/internal/operational"
)

// httpShutdownTimeout bounds the health server's shutdown after the MQTT lifecycle has
// completed; a deployment's termination grace budgets for it (docs/deploy/kubernetes).
const httpShutdownTimeout = 5 * time.Second

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	status, report := exitStatus(run(os.Args[1:]))
	if report != "" {
		fmt.Fprintln(os.Stderr, report)
	}
	os.Exit(status)
}

// exitStatus is the exit status run's outcome gives, and the stderr line that reports it:
// a stop is not a failure, whether the broker served or was stopped while starting.
func exitStatus(err error) (int, string) {
	var stopped *startupStopped
	if err == nil || errors.As(err, &stopped) {
		return operational.ExitStopped, ""
	}
	return operational.ExitFailed, operational.FatalPrefix + err.Error()
}

// startupStopped is a startup that a shutdown signal ended before the broker served.
type startupStopped struct{ err error }

func (e *startupStopped) Error() string { return "startup stopped: " + e.err.Error() }
func (e *startupStopped) Unwrap() error { return e.err }

// startupOutcome is what a startup step's error means once the step has returned: a
// cancellation while the signal context is done is a requested stop, anything else is the
// failure it reports. A definite refusal never wraps a cancellation, so a signal that
// arrives while one is being returned leaves it a failure. The MQTT core's sentinel covers
// a startup it abandoned, whose cause need not be context.Canceled.
func startupOutcome(ctx context.Context, log *slog.Logger, err error) error {
	if err == nil || ctx.Err() == nil {
		return err
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, mqtt.ErrStartupCancelled) {
		return err
	}
	operational.StartupStopped.Log(log, operational.Err(err))
	return &startupStopped{err}
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
	operational.BrokerStarting.Log(log, operational.Str(version))

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

	// readiness is Starting until the MQTT listener is up and initialized, then Serving.
	// A requested stop is ctx ending, which /readyz reads itself, so a load balancer / k8s
	// Service deregisters this instance from the signal on, in every phase, before its
	// connections are drained. backendRef is the backend once it is connected, for
	// /readyz's connection check; nil before.
	var readiness atomic.Int32
	var backendRef atomic.Pointer[backend.Backend]

	// The health endpoint is bound BEFORE the embedded server starts, synchronously so a
	// taken port fails startup here: /healthz then answers through local store recovery,
	// the JetStream wait and the MQTT initialization's retries, none of which is bounded
	// by a probe any more (issue #177); /readyz stays 503 until the broker serves.
	// There is no metrics endpoint.
	var httpSrv *http.Server
	if cfg.HTTPAddr != "" {
		httpSrv, err = startHTTP(ctx, cfg.HTTPAddr, &backendRef, &readiness, log)
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
		return startupOutcome(ctx, log, err)
	}
	defer stopEmbeddedNATS(ns)
	var inProcess nats.InProcessConnProvider
	if ns != nil {
		inProcess = ns
	}

	be, err := backend.Connect(ctx, cfg, inProcess, log)
	if err != nil {
		return startupOutcome(ctx, log, err)
	}
	defer be.Close()
	backendRef.Store(be)

	// Start the MQTT listener backed by the NATS/JetStream connection, retrying its
	// initialization until JetStream answers or a signal ends startup. On failure the
	// deferred be.Close() and HTTP shutdown keep cleanup reachable so a failed startup
	// leaks nothing.
	srv, err := serveMQTT(ctx, cfg, be, opts, log)
	if err != nil {
		return startupOutcome(ctx, log, err)
	}
	// The TLS files are re-read from here until the server has fully stopped: the
	// listener still accepts during the drain grace period, so a renewal landing
	// then must still reach new handshakes. The deferred Stop runs after
	// BeginShutdown's completion below and joins the watcher.
	if tlsSrc != nil {
		tlsSrc.Start(cfg.TLSReloadInterval)
		defer tlsSrc.Stop()
	}
	readiness.Store(int32(operational.Serving))

	<-ctx.Done()
	operational.ShutdownSignalReceived.Log(log)
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

	// /readyz has answered draining since ctx ended; (optionally) wait out the pre-drain
	// grace period so the Service/LB stops routing new connections here before we
	// disconnect existing clients. The health server stays up through the drain
	// returning 503, not connection-refused.
	if cfg.DrainDelay > 0 {
		operational.DrainDelayWaiting.Log(log, operational.Dur(cfg.DrainDelay))
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
// answering" holds from the moment this returns. stop is the signal context.
func startHTTP(stop context.Context, addr string, be *atomic.Pointer[backend.Backend], readiness *atomic.Int32, log *slog.Logger) (*http.Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("health server: %w", err)
	}
	connected := func() bool {
		b := be.Load()
		return b != nil && b.Conn().IsConnected()
	}
	srv := &http.Server{Addr: addr, Handler: healthMux(stop, readiness, connected), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		operational.HealthServerListening.Log(log, operational.Addr(l.Addr()))
		if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			operational.HealthServerError.Log(log, operational.Err(err))
		}
	}()
	return srv, nil
}

// healthMux serves the probe endpoints from the operational interface's definition:
// /healthz whenever the server serves, /readyz from readiness, whether stop has ended,
// and connected, which is consulted only once the broker serves.
func healthMux(stop context.Context, readiness *atomic.Int32, connected func() bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(operational.HealthzPath, func(w http.ResponseWriter, _ *http.Request) {
		status, body := operational.Healthz()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
	mux.HandleFunc(operational.ReadyzPath, func(w http.ResponseWriter, _ *http.Request) {
		status, body := operational.Readyz(operational.Readiness(readiness.Load()), stop.Err() != nil, connected())
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
	return mux
}
