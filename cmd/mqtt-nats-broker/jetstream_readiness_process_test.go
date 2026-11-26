package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The JetStream observation behind /readyz in a real run (jetstream_readiness.go): a serving
// broker withdraws when JetStream stops answering it and returns, with no MQTT traffic, when it
// answers again (a server-wide DisableJetStream/EnableJetStream would be the obvious lever, but
// nats-server 2.15.2 races its own API dispatch there under -race); the embedded server's own
// health is part of the observation; and shutdown does not wait for a round that is held.

// embeddedHealthFailFileEnv makes the re-executed test binary fail the embedded server health
// check while the named file exists.
const embeddedHealthFailFileEnv = "MQTTNB_TEST_EMBEDDED_HEALTH_FAIL_FILE"

func failEmbeddedServerHealthWhile(path string) {
	embeddedServerHealthCheck = func(ns *server.Server) error {
		if _, err := os.Stat(path); err == nil {
			return errors.New("meta replica held behind by the test")
		}
		return embeddedServerHealth(ns)
	}
}

func (p *brokerProcess) readyzAnswer() (int, string) { return probe(p.httpAddr, "/readyz") }

// faultCursor is the position of the last record read once the readiness records have
// settled: none has arrived for two seconds. A position is what the parent has READ, so a
// transition the broker wrote before the fault can still be in the pipe and land past it; the
// quiet window makes that unlikely, it does not rule it out. A test that must attribute a
// record to its fault matches what only that fault produces (waitRecordWhere); otherwise the
// /readyz answers are the attributed witness and the records past the cursor corroborate them.
func (p *brokerProcess) faultCursor(t *testing.T) int {
	t.Helper()
	count := func() int {
		p.mu.Lock()
		defer p.mu.Unlock()
		n := 0
		for _, m := range p.lines {
			if msg, _ := m["msg"].(string); strings.HasPrefix(msg, "JetStream readiness probe") {
				n++
			}
		}
		return n
	}
	deadline := time.Now().Add(30 * time.Second)
	last, quietSince := count(), time.Now()
	for time.Since(quietSince) < 2*time.Second {
		if time.Now().After(deadline) {
			t.Fatal("the readiness records did not settle before the fault")
		}
		time.Sleep(100 * time.Millisecond)
		if n := count(); n != last {
			last, quietSince = n, time.Now()
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.lines) - 1
}

// waitRecordAfter is the first record the parent read past position after whose message
// contains msg, and its position, waited for, since the observation is stored before its record
// is written and stdout is read asynchronously. Past a position is not after a fault: see
// faultCursor.
func (p *brokerProcess) waitRecordAfter(t *testing.T, msg string, after int, within time.Duration) (map[string]any, int) {
	t.Helper()
	return p.waitRecordWhere(t, msg, after, within, func(map[string]any) bool { return true })
}

// waitRecordWhere is waitRecordAfter for the first such record that also satisfies match.
func (p *brokerProcess) waitRecordWhere(t *testing.T, msg string, after int, within time.Duration, match func(map[string]any) bool) (map[string]any, int) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		p.mu.Lock()
		for i := after + 1; i < len(p.lines); i++ {
			if m, _ := p.lines[i]["msg"].(string); strings.Contains(m, msg) && match(p.lines[i]) {
				r := p.lines[i]
				p.mu.Unlock()
				return r, i
			}
		}
		p.mu.Unlock()
		if !time.Now().Before(deadline) {
			t.Fatalf("no matching %q record after position %d within %s", msg, after, within)
			return nil, -1
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (p *brokerProcess) waitReadyz(t *testing.T, within time.Duration, status int, body string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if c, b := p.readyzAnswer(); c == status && b == body {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c, b := p.readyzAnswer()
	t.Fatalf("/readyz = %d %q after %s, want %d %q", c, b, within, status, body)
}

// startReadinessTestServer runs a nats-server with the image's account shape and returns it.
func startReadinessTestServer(t *testing.T, conf string) *server.Server {
	t.Helper()
	opts, err := server.ProcessConfigFile(writeConf(t, conf))
	if err != nil {
		t.Fatal(err)
	}
	opts.NoSigs = true
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(30 * time.Second) {
		t.Fatal("server not ready")
	}
	return srv
}

var readinessServiceEnv = []string{"MQTTNB_NATS_USER=svc", "MQTTNB_NATS_PASS=svcpass",
	"MQTTNB_DEFAULT_USER=anon", "MQTTNB_DEFAULT_PASS=anonpass"}

// TestReadinessFollowsTheSessionStreamWhileServing: a serving broker whose session stream stops
// answering is withdrawn while its connection stays up, and is ready again, with no MQTT client,
// once the stream answers.
func TestReadinessFollowsTheSessionStreamWhileServing(t *testing.T) {
	srv := startReadinessTestServer(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount)
	t.Cleanup(srv.Shutdown)
	p := startBrokerProcess(t, readinessServiceEnv, "-nats", srv.ClientURL())
	p.waitReadyz(t, 60*time.Second, http.StatusOK, "ready")
	service := connByName(t, srv, "mqtt-nats-broker")
	if service == nil {
		t.Fatal("no service connection")
	}

	nc, err := nats.Connect(srv.ClientURL(), nats.UserInfo("svc", "svcpass"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	stream, err := js.Stream(ctx, "$MQTT_sess")
	if err != nil {
		t.Fatal(err)
	}
	cfg := stream.CachedInfo().Config
	before := p.faultCursor(t)
	if err := js.DeleteStream(ctx, "$MQTT_sess"); err != nil {
		t.Fatal(err)
	}
	p.waitReadyz(t, 30*time.Second, http.StatusServiceUnavailable, "jetstream not answering")
	// Only the deletion makes the session stream info answer "stream not found".
	r, failedAt := p.waitRecordWhere(t, "JetStream readiness probe failed", before, 30*time.Second, func(r map[string]any) bool {
		err, _ := r["err"].(string)
		return strings.Contains(err, "stream not found")
	})
	if r["level"] != "WARN" || r["check"] != "session stream info" {
		t.Fatalf("failed record = %v, want a WARN naming the session stream info check", r)
	}

	if _, err := js.CreateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	p.waitReadyz(t, 30*time.Second, http.StatusOK, "ready")
	if r, _ := p.waitRecordAfter(t, "JetStream readiness probe succeeded again", failedAt, 30*time.Second); r["level"] != "INFO" {
		t.Fatalf("succeeded record = %v, want an INFO", r)
	}
	if now := connByName(t, srv, "mqtt-nats-broker"); now == nil || now.Cid != service.Cid {
		t.Fatalf("the service connection was replaced (%v -> %v): the witness is a reconnect, not JetStream", service, now)
	}
}

// startReadinessTestCluster runs three routed JetStream servers; restart starts the named ones
// again on their own routes and stores.
func startReadinessTestCluster(t *testing.T) (nodes []*server.Server, restart func(...int)) {
	t.Helper()
	routes := []string{reservedAddr(t), reservedAddr(t), reservedAddr(t)}
	var routeList []string
	for _, r := range routes {
		routeList = append(routeList, "nats-route://route:routepass@"+r)
	}
	stores := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	conf := func(i int) string {
		return fmt.Sprintf("server_name: node-%d\nlisten: 127.0.0.1:-1\ncluster { name: c, listen: %q, authorization: { user: route, password: routepass }, routes: [ %s ] }\n",
			i, routes[i], strings.Join(routeList, ", ")) + jetStreamBlock(stores[i]) + twoUserAccount
	}
	nodes = make([]*server.Server, 3)
	for i := range nodes {
		nodes[i] = startReadinessTestServer(t, conf(i))
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Shutdown()
		}
	})
	return nodes, func(which ...int) {
		for _, i := range which {
			nodes[i] = startReadinessTestServer(t, conf(i))
		}
	}
}

// TestEmbeddedServerHealthAnswersTheMetaReplica: the check reads the server's own meta replica, so
// a peer that has lost its meta quorum reports it, and reports health again once quorum returns.
func TestEmbeddedServerHealthAnswersTheMetaReplica(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	nodes, restart := startReadinessTestCluster(t)
	waitFor(t, 60*time.Second, func() bool { return embeddedServerHealth(nodes[0]) == nil })
	for _, i := range []int{1, 2} {
		nodes[i].Shutdown()
		nodes[i].WaitForShutdown()
	}
	waitFor(t, 60*time.Second, func() bool { return embeddedServerHealth(nodes[0]) != nil })
	restart(1, 2)
	waitFor(t, 60*time.Second, func() bool { return embeddedServerHealth(nodes[0]) == nil })
}

// TestReadinessFollowsTheMetaLeaderWhileServing: a broker on a cluster that loses its meta
// quorum, while its own server and connection stay up, is withdrawn; it is ready again once the
// peers return.
func TestReadinessFollowsTheMetaLeaderWhileServing(t *testing.T) {
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	nodes, restart := startReadinessTestCluster(t)
	// The external topology does not wait for a meta leader at startup: elect one first.
	waitFor(t, 60*time.Second, func() bool {
		for _, n := range nodes {
			if n.JetStreamIsLeader() {
				return true
			}
		}
		return false
	})
	p := startBrokerProcess(t, readinessServiceEnv, "-nats", nodes[0].ClientURL(), "-stream-replicas", "3")
	p.waitReadyz(t, 120*time.Second, http.StatusOK, "ready")
	service := connByName(t, nodes[0], "mqtt-nats-broker")
	if service == nil {
		t.Fatal("no service connection on node 0")
	}

	before := p.faultCursor(t)
	for _, i := range []int{1, 2} {
		nodes[i].Shutdown()
		nodes[i].WaitForShutdown()
	}
	p.waitReadyz(t, 60*time.Second, http.StatusServiceUnavailable, "jetstream not answering")
	// Either request can be the first to fail as quorum goes, and a failing probe logs once. The
	// /readyz answers are this test's witness; the records past the cursor corroborate them.
	r, failedAt := p.waitRecordAfter(t, "JetStream readiness probe failed", before, 30*time.Second)
	if r["check"] != "account info" && r["check"] != "session stream info" {
		t.Fatalf("failed record = %v, want a JetStream request's check", r)
	}

	restart(1, 2)
	p.waitReadyz(t, 120*time.Second, http.StatusOK, "ready")
	p.waitRecordAfter(t, "JetStream readiness probe succeeded again", failedAt, 30*time.Second)
	if now := connByName(t, nodes[0], "mqtt-nats-broker"); now == nil || now.Cid != service.Cid {
		t.Fatalf("the service connection was replaced (%v -> %v)", service, now)
	}
}

// TestReadinessConsultsTheEmbeddedServerHealth: with the embedded server, a meta replica its own
// health check reports behind keeps the broker from turning ready, through the wiring runContext
// builds, while the probe's requests are answered.
func TestReadinessConsultsTheEmbeddedServerHealth(t *testing.T) {
	held := filepath.Join(t.TempDir(), "meta-behind")
	if err := os.WriteFile(held, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	conf := writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount)
	p := startBrokerProcess(t, []string{embeddedHealthFailFileEnv + "=" + held,
		"MQTTNB_NATS_USER=svc", "MQTTNB_NATS_PASS=svcpass", "MQTTNB_DEFAULT_USER=anon", "MQTTNB_DEFAULT_PASS=anonpass"},
		"-nats-embedded-config", conf)
	p.waitReadyz(t, 60*time.Second, http.StatusServiceUnavailable, "jetstream not answering")
	r, failedAt := p.waitRecordAfter(t, "JetStream readiness probe failed", -1, 30*time.Second)
	if r["check"] != "embedded server health" {
		t.Fatalf("failed record = %v, want the embedded server health check", r)
	}
	// Served: stdout is read in order, so the listener record is in by now, before the failure.
	if l := p.index("MQTT listener started"); l < 0 || l > failedAt {
		t.Fatalf("listener record at %d, failure record at %d: the broker had not served", l, failedAt)
	}
	if err := os.Remove(held); err != nil {
		t.Fatal(err)
	}
	p.waitReadyz(t, 30*time.Second, http.StatusOK, "ready")
	if r, _ := p.waitRecordAfter(t, "JetStream readiness probe succeeded again", failedAt, 30*time.Second); r["level"] != "INFO" {
		t.Fatalf("succeeded record = %v, want an INFO", r)
	}
}

// TestShutdownDoesNotWaitForAHeldReadinessRound: the MQTT shutdown lifecycle completes while a
// round is held inside the embedded check; only the teardown after it waits for the round.
func TestShutdownDoesNotWaitForAHeldReadinessRound(t *testing.T) {
	isolateGoMemoryLimit(t, memlimit.Limit(1<<30))
	embeddedServiceEnv(t)
	var hold atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releasedOnce sync.Once
	releaseRound := func() { releasedOnce.Do(func() { close(release) }) }
	saved := embeddedServerHealthCheck
	embeddedServerHealthCheck = func(ns *server.Server) error {
		if hold.Load() {
			enteredOnce.Do(func() { close(entered) })
			<-release
			return errors.New("released")
		}
		return embeddedServerHealth(ns)
	}
	completed := make(chan struct{})
	testLifecycleCompleted = func() { close(completed) }

	httpAddr := reservedAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	conf := writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount)
	// returned closes when the run returns; runErr is its result, read only after.
	returned := make(chan struct{})
	var runErr error
	go func() {
		defer close(returned)
		runErr = runContext(ctx, []string{"-nats-embedded-config", conf, "-http-addr", httpAddr,
			"-port", portOf(t, reservedAddr(t)), "-stream-replicas", "1"})
	}()
	defer func() {
		cancel()
		releaseRound()
		select {
		case <-returned:
		case <-time.After(2 * time.Minute):
			t.Error("the run did not return during cleanup")
		}
		embeddedServerHealthCheck = saved
		testLifecycleCompleted = nil
	}()

	waitFor(t, 60*time.Second, func() bool { code, _ := probe(httpAddr, "/readyz"); return code == http.StatusOK })
	hold.Store(true)
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("no round entered the embedded check")
	}
	cancel()
	select {
	case <-completed:
	case <-returned:
		t.Fatalf("run returned %v before the MQTT lifecycle's completion was observed", runErr)
	case <-time.After(60 * time.Second):
		t.Fatal("the MQTT shutdown lifecycle did not complete while a readiness round was held")
	}
	select {
	case <-returned:
		t.Fatalf("run returned %v while a readiness round was held", runErr)
	case <-time.After(500 * time.Millisecond):
	}
	releaseRound()
	select {
	case <-returned:
		if runErr != nil {
			t.Fatalf("run = %v, want a completed shutdown", runErr)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("run did not return after the round was released")
	}
}
