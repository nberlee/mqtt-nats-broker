package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
	"github.com/nberlee/mqtt-nats-broker/internal/operational"
)

// The operational interface's process-level witnesses: what the probes answer in each
// phase of a real run, the exit status and stderr a run ends with, and the records a run
// writes (docs/ARCHITECTURE.md, "The operational interface"). Expectations are literals,
// not read from the definition, so a definition that drifts with its producers fails.

// TestStartupOutcomeClassifiesCancellation: only a cancellation, with the signal context
// done, is a stop; a refusal stays a failure whatever the context.
func TestStartupOutcomeClassifiesCancellation(t *testing.T) {
	done, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()
	refusal := errors.New("unsupported NATS server: 2.14.0 is below the floor")
	notCanceled := errors.New("a cause that is not context.Canceled")
	for _, tc := range []struct {
		name    string
		ctx     context.Context
		err     error
		stopped bool
	}{
		{"a context.Canceled chain", done, fmt.Errorf("JetStream not available on service connection: %w", context.Canceled), true},
		{"the MQTT core's abandonment with another cause", done, fmt.Errorf("start MQTT listener: %w", fmt.Errorf("%w: %w", mqtt.ErrStartupCancelled, notCanceled)), true},
		{"a refusal while the context is done", done, refusal, false},
		{"a cancellation while the context is live", live, fmt.Errorf("x: %w", context.Canceled), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := startupOutcome(tc.ctx, slog.New(slog.DiscardHandler), tc.err)
			if !errors.Is(got, tc.err) {
				t.Fatalf("startupOutcome lost the error: %v", got)
			}
			status, report := exitStatus(got)
			if tc.stopped && (status != 0 || report != "") {
				t.Errorf("exit %d %q, want 0 and nothing on stderr", status, report)
			}
			if !tc.stopped && (status != 1 || report != "fatal: "+tc.err.Error()) {
				t.Errorf("exit %d %q, want 1 and the fatal line", status, report)
			}
		})
	}
	if got := startupOutcome(done, slog.New(slog.DiscardHandler), nil); got != nil {
		t.Errorf("startupOutcome(nil) = %v", got)
	}
}

// startPartialBundleServer runs a JetStream server whose account holds one of the ten
// broker-owned streams, so a broker's stream preflight waits on the partial bundle (30 s)
// after it has connected and before it serves.
func startPartialBundleServer(t *testing.T) *server.Server {
	t.Helper()
	opts, err := server.ProcessConfigFile(writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount))
	if err != nil {
		t.Fatal(err)
	}
	opts.NoSigs = true
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL(), nats.UserInfo("svc", "svcpass"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "$MQTT_out",
		Subjects: []string{"partial.bundle.>"}, Metadata: map[string]string{"mqtt_nats_broker_format": "1"}}); err != nil {
		t.Fatal(err)
	}
	return srv
}

// TestReadinessFollowsTheLifecycle: /readyz says starting until the broker serves, ready
// while it serves, and draining from the stop on.
func TestReadinessFollowsTheLifecycle(t *testing.T) {
	isolateGoMemoryLimit(t, memlimit.Limit(1<<30))
	embeddedServiceEnv(t)

	t.Run("connected, held in the stream preflight", func(t *testing.T) {
		srv := startPartialBundleServer(t)
		httpAddr := reservedAddr(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := runInBackground(ctx, []string{"-nats", srv.ClientURL(), "-http-addr", httpAddr,
			"-port", portOf(t, reservedAddr(t)), "-stream-replicas", "1"})
		// The backend connects before the preflight: wait for it, then for the hold.
		waitFor(t, 30*time.Second, func() bool { return srv.NumClients() > 0 })
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if code, body := probe(httpAddr, "/readyz"); code != http.StatusServiceUnavailable || body != "starting" {
				t.Fatalf("/readyz while the preflight waits = %d %q, want 503 \"starting\"", code, body)
			}
			time.Sleep(200 * time.Millisecond)
		}
		cancel()
		select {
		case err := <-done:
			if status, report := exitStatus(err); status != 0 || report != "" {
				t.Fatalf("run = %v: exit %d %q, want a stop (0)", err, status, report)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("run did not return after its context ended")
		}
	})

	t.Run("served, then held in the drain delay", func(t *testing.T) {
		httpAddr := reservedAddr(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conf := writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount)
		done := runInBackground(ctx, []string{"-nats-embedded-config", conf, "-http-addr", httpAddr,
			"-port", portOf(t, reservedAddr(t)), "-stream-replicas", "1", "-drain-delay", "5s"})
		waitFor(t, 60*time.Second, func() bool { code, _ := probe(httpAddr, "/readyz"); return code == http.StatusOK })
		if code, body := probe(httpAddr, "/readyz"); code != http.StatusOK || body != "ready" {
			t.Fatalf("/readyz while serving = %d %q, want 200 \"ready\"", code, body)
		}
		cancel()
		// The drain delay holds the run for 5 s after the stop.
		time.Sleep(500 * time.Millisecond)
		if code, body := probe(httpAddr, "/readyz"); code != http.StatusServiceUnavailable || body != "draining" {
			t.Errorf("/readyz in the drain delay = %d %q, want 503 \"draining\"", code, body)
		}
		if code, body := probe(httpAddr, "/healthz"); code != http.StatusOK || body != "ok" {
			t.Errorf("/healthz in the drain delay = %d %q, want 200 \"ok\"", code, body)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("run = %v, want a completed shutdown", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("run did not return after its context ended")
		}
	})
}

// TestAStartupRefusalExitsOneWithFatal: a broker that cannot start exits 1 and says why on
// stderr, keeping stdout a JSON stream.
func TestAStartupRefusalExitsOneWithFatal(t *testing.T) {
	p := startBrokerProcess(t, nil, "-stream-replicas", "0")
	if code := p.waitExit(t, 30*time.Second); code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
	lines := strings.Split(strings.TrimRight(p.stderr.String(), "\n"), "\n")
	if last := lines[len(lines)-1]; !strings.HasPrefix(last, "fatal: ") {
		t.Errorf("stderr's last line = %q, want it to begin \"fatal: \"", last)
	}
	p.requireOneJSONStream(t)
}

// embeddedFatalEnv makes the re-executed test binary call the embedded server's Fatalf
// instead of running the broker.
const embeddedFatalEnv = "MQTTNB_TEST_EMBEDDED_FATAL"

// TestEmbeddedFatalExitsOne: the embedded server's fatal error ends the process with exit
// 1 and an ERROR record marked as the server's; it writes no fatal line of its own.
func TestEmbeddedFatalExitsOne(t *testing.T) {
	p := startBrokerProcess(t, []string{embeddedFatalEnv + "=1"})
	if code := p.waitExit(t, 30*time.Second); code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
	r := p.record("embedded fatal 7")
	if r == nil || r["level"] != "ERROR" || r["component"] != "nats-server" {
		t.Fatalf("record = %v, want an ERROR record with component nats-server", r)
	}
	if strings.Contains(p.stderr.String(), "fatal:") {
		t.Errorf("stderr = %q, want no fatal line", p.stderr.String())
	}
}

// runEmbeddedFatal is the re-executed side of TestEmbeddedFatalExitsOne.
func runEmbeddedFatal() {
	embeddedServerLogger(newLogger("info")).Fatalf("embedded fatal %d", 7)
}

// requireRecord finds the record with exactly msg and requires its level and its keys:
// the envelope's three and exactly the fields given, each of the JSON type its value has.
func requireRecord(t *testing.T, p *brokerProcess, msg, level string, fields map[string]any) map[string]any {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range p.lines {
		if m["msg"] != msg {
			continue
		}
		if m["level"] != level {
			t.Errorf("%q: level %v, want %s", msg, m["level"], level)
		}
		var keys, want []string
		for k := range m {
			keys = append(keys, k)
		}
		want = append(want, "time", "level", "msg")
		for k, v := range fields {
			want = append(want, k)
			got, ok := m[k]
			if !ok {
				continue
			}
			if v == nil {
				continue // present; its value is the run's
			}
			if fmt.Sprintf("%T", got) != fmt.Sprintf("%T", v) {
				t.Errorf("%q: %s is %T (%v), want %T", msg, k, got, got, v)
			}
		}
		sort.Strings(keys)
		sort.Strings(want)
		if strings.Join(keys, ",") != strings.Join(want, ",") {
			t.Errorf("%q: keys %v, want %v", msg, keys, want)
		}
		return m
	}
	t.Errorf("no record %q", msg)
	return nil
}

// TestLifecycleRecordsCarryTheirFrozenFields: a started and stopped embedded broker writes
// its lifecycle records with their frozen levels, keys and JSON types.
func TestLifecycleRecordsCarryTheirFrozenFields(t *testing.T) {
	p := startLoggingBroker(t, "info")
	p.stop(t)
	str, num, flag := "", float64(0), false
	requireRecord(t, p, "starting mqtt-nats-broker", "INFO", map[string]any{"version": str})
	if r := requireRecord(t, p, "health server listening", "INFO", map[string]any{"addr": str}); r != nil && r["addr"] != p.httpAddr {
		t.Errorf("health server addr %v, want %s", r["addr"], p.httpAddr)
	}
	requireRecord(t, p, "embedded NATS server ready", "INFO", map[string]any{"server": str, "config": str})
	requireRecord(t, p, "connected to NATS backend", "INFO", map[string]any{"urls": str, "in_process": flag,
		"server": str, "server_version": str, "jetstream_api_level": num, "cluster": str, "js_domain": str})
	if r := requireRecord(t, p, "MQTT listener started", "INFO", map[string]any{"addr": str, "node": str, "tls": flag}); r != nil && !strings.HasSuffix(fmt.Sprint(r["addr"]), ":"+portOf(t, p.mqttAddr)) {
		t.Errorf("MQTT listener addr %v, want the listener's address, port %s", r["addr"], portOf(t, p.mqttAddr))
	}
	requireRecord(t, p, "shutdown signal received, draining", "INFO", nil)
	requireRecord(t, p, "NATS service connection closed", "INFO", nil)
}

// TestTheDefinitionNamesTheRecordsTheWitnessesRead keeps the literal messages above tied to
// frozen records: a witness that reads a record the definition no longer has would pass
// against nothing.
func TestTheDefinitionNamesTheRecordsTheWitnessesRead(t *testing.T) {
	frozen := map[string]bool{}
	for _, r := range operational.Records {
		frozen[r.Msg] = true
	}
	for _, msg := range []string{"starting mqtt-nats-broker", "health server listening", "embedded NATS server ready",
		"connected to NATS backend", "MQTT listener started", "shutdown signal received, draining",
		"NATS service connection closed", "shutdown signal received during startup"} {
		if !frozen[msg] {
			t.Errorf("%q is not a frozen record", msg)
		}
	}
}
