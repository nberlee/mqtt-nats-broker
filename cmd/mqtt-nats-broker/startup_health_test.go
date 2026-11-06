package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
)

// embeddedServiceEnv sets the credentials twoUserAccount's users carry.
func embeddedServiceEnv(t *testing.T) {
	t.Setenv("MQTTNB_NATS_USER", "svc")
	t.Setenv("MQTTNB_NATS_PASS", "svcpass")
	t.Setenv("MQTTNB_DEFAULT_USER", "anon")
	t.Setenv("MQTTNB_DEFAULT_PASS", "anonpass")
}

// probe answers one GET of path on the health port: its status and body, or 0 when
// nothing answered.
func probe(addr, path string) (int, string) {
	resp, err := (&http.Client{Timeout: time.Second}).Get("http://" + addr + path)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// runInBackground runs runContext until ctx ends and returns its result.
func runInBackground(ctx context.Context, args []string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- runContext(ctx, args) }()
	return done
}

// TestRunServesHealthWhileTheEmbeddedServerRecovers: the health endpoint answers from
// before the embedded server starts, so the window nats-server spends recovering its
// store is not one a liveness probe can kill (issue #177). /readyz is not ready.
func TestRunServesHealthWhileTheEmbeddedServerRecovers(t *testing.T) {
	isolateGoMemoryLimit(t, memlimit.Limit(1<<30))
	embeddedServiceEnv(t)
	held := make(chan struct{})
	release := make(chan struct{})
	h := func() {
		close(held)
		<-release
	}
	embeddedStartHookV.Store(&h)
	t.Cleanup(func() { embeddedStartHookV.Store(nil) })

	httpAddr := reservedAddr(t)
	conf := writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runInBackground(ctx, []string{"-nats-embedded-config", conf, "-http-addr", httpAddr,
		"-port", portOf(t, reservedAddr(t)), "-stream-replicas", "1"})

	select {
	case <-held:
	case err := <-done:
		t.Fatalf("run returned before the embedded server started: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the embedded server never reached its start")
	}
	if code, body := probe(httpAddr, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz while the embedded server is held before starting = %d %q, want 200", code, body)
	}
	if code, body := probe(httpAddr, "/readyz"); code != http.StatusServiceUnavailable || body != "starting" {
		t.Errorf("/readyz while the embedded server is held = %d %q, want 503 \"starting\"", code, body)
	}

	close(release)
	waitFor(t, 60*time.Second, func() bool { code, _ := probe(httpAddr, "/readyz"); return code == http.StatusOK })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run after cancellation of a started broker: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("run did not return after its context ended")
	}
}

// TestRunServesHealthDuringTheJetStreamWait: a peer whose cluster never forms waits for
// JetStream with /healthz answering and /readyz not ready, and its context ends it.
func TestRunServesHealthDuringTheJetStreamWait(t *testing.T) {
	isolateGoMemoryLimit(t, memlimit.Limit(1<<30))
	embeddedServiceEnv(t)
	httpAddr := reservedAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runInBackground(ctx, []string{"-nats-embedded-config", writeConf(t, unformableCluster(t)),
		"-http-addr", httpAddr, "-port", portOf(t, reservedAddr(t)), "-stream-replicas", "1"})

	waitFor(t, 60*time.Second, func() bool { code, _ := probe(httpAddr, "/healthz"); return code == http.StatusOK })
	// Still waiting: the cluster cannot form, so nothing past the JetStream wait runs.
	time.Sleep(3 * time.Second)
	if code, body := probe(httpAddr, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz during the JetStream wait = %d %q, want 200", code, body)
	}
	if code, body := probe(httpAddr, "/readyz"); code != http.StatusServiceUnavailable || body != "starting" {
		t.Errorf("/readyz during the JetStream wait = %d %q, want 503 \"starting\"", code, body)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("run = %v, want the cancelled JetStream wait", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("run did not return after its context ended")
	}
	if code, _ := probe(httpAddr, "/healthz"); code != 0 {
		t.Errorf("the health server still answers (%d) after run returned", code)
	}
}

// TestRunFailsWhenTheHealthPortCannotBeBound: the health port is bound synchronously, so a
// taken port fails startup before NATS is started or dialed, naming the health server.
func TestRunFailsWhenTheHealthPortCannotBeBound(t *testing.T) {
	isolateGoMemoryLimit(t, memlimit.Limit(1<<30))
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	// NATS on a closed port: a bind error reported only as a log line would let run go on
	// and fail on NATS instead.
	err = run([]string{"-nats", "nats://" + reservedAddr(t), "-http-addr", taken.Addr().String(), "-stream-replicas", "1"})
	if err == nil || !strings.Contains(err.Error(), "health server") {
		t.Fatalf("run() = %v, want the health server's bind error", err)
	}
}
