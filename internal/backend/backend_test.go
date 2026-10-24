package backend

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// runJetStreamServer starts an in-process NATS server with JetStream enabled on a
// random port and returns its client URL.
func runJetStreamServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func TestConnect_JetStreamAvailable(t *testing.T) {
	srv := runJetStreamServer(t)

	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()

	be, err := Connect(context.Background(), cfg, nil, slog.Default())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer be.Close()

	if !be.Conn().IsConnected() {
		t.Fatal("service connection not connected")
	}
}

// TestConnect_ProbeRepliesInsideTheReservedNamespace pins where the startup JetStream probe
// is answered. The default inbox, _INBOX., is reachable by an MQTT `#` subscriber in the same
// account; the reply must arrive under $MQTT., which no MQTT client can subscribe to.
//
// The observer subscribes to both prefixes and FLUSHES before the probe runs, so an empty
// _INBOX. channel is an absence the server could have filled, not a subscription that had not
// arrived yet; the $MQTT.inbox. arrival is the positive control that the observer sees the
// probe's traffic at all.
func TestConnect_ProbeRepliesInsideTheReservedNamespace(t *testing.T) {
	srv := runJetStreamServer(t)

	obs, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("observer connect: %v", err)
	}
	defer obs.Close()
	defaultInbox := make(chan *nats.Msg, 16)
	reserved := make(chan *nats.Msg, 16)
	if _, err := obs.ChanSubscribe("_INBOX.>", defaultInbox); err != nil {
		t.Fatalf("subscribe _INBOX.>: %v", err)
	}
	if _, err := obs.ChanSubscribe(serviceInboxPrefix+".>", reserved); err != nil {
		t.Fatalf("subscribe %s.>: %v", serviceInboxPrefix, err)
	}
	if err := obs.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("flush the observer: %v", err)
	}

	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()
	be, err := Connect(context.Background(), cfg, nil, slog.Default())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer be.Close()

	select {
	case m := <-reserved:
		if !strings.HasPrefix(m.Subject, serviceInboxPrefix+".") {
			t.Fatalf("the probe reply arrived on %q, outside %s.", m.Subject, serviceInboxPrefix)
		}
		var resp struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(m.Data, &resp); err != nil || resp.Type != natsserver.JSApiAccountInfoResponseType {
			t.Fatalf("the message on %q is not the account-info reply (type %q, %v)", m.Subject, resp.Type, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no probe reply arrived under %s.", serviceInboxPrefix)
	}
	select {
	case m := <-defaultInbox:
		t.Fatalf("the probe reply was published on %q, which an MQTT `#` subscriber can receive", m.Subject)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestConnect_FailsWithoutJetStream: the probe is the only JetStream check before the MQTT
// listener opens, so a service account without JetStream must be refused here.
func TestConnect_FailsWithoutJetStream(t *testing.T) {
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)

	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()
	cfg.JSTimeout = 2 * time.Second
	be, err := Connect(context.Background(), cfg, nil, slog.Default())
	if err == nil {
		be.Close()
		t.Fatal("Connect succeeded against a server without JetStream")
	}
}

func TestConnect_NoServer(t *testing.T) {
	cfg := config.Default()
	cfg.NATSURLs = "nats://127.0.0.1:1" // nothing listening
	cfg.JSTimeout = 500 * time.Millisecond

	_, err := Connect(context.Background(), cfg, nil, slog.Default())
	if err == nil {
		t.Fatal("expected connect error, got nil")
	}
}

func TestDialClient_AnonymousDefaults(t *testing.T) {
	srv := runJetStreamServer(t)

	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()

	be, err := Connect(context.Background(), cfg, nil, slog.Default())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer be.Close()

	nc, err := be.DialClient("client-1", "", "")
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	defer nc.Close()
	if !nc.IsConnected() {
		t.Fatal("per-client connection not connected")
	}
}

// closeFixture connects a Backend and gives it one subscription whose handler is parked
// inside a delivery, so a drain cannot finish until the handler returns.
func closeFixture(t *testing.T) (be *Backend, release func(), inside <-chan struct{}) {
	t.Helper()
	srv := runJetStreamServer(t)
	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()
	be, err := Connect(context.Background(), cfg, nil, slog.Default())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	gate, entered := make(chan struct{}), make(chan struct{})
	var once bool
	if _, err := be.Conn().Subscribe("drain.barrier", func(*nats.Msg) {
		if !once {
			once = true
			close(entered)
		}
		<-gate
	}); err != nil {
		t.Fatal(err)
	}
	if err := be.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	pub, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	if err := pub.Publish("drain.barrier", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := pub.Flush(); err != nil {
		t.Fatal(err)
	}
	var released bool
	release = func() {
		if !released {
			released = true
			close(gate)
		}
	}
	t.Cleanup(release)
	return be, release, entered
}

// Close returns only once the service connection's drain has finished: Drain itself is
// asynchronous, and returning at once let the process exit with the drain — and the flush
// of whatever was still buffered — unfinished. Not parallel: the seam is a package var.
func TestBackendCloseWaitsForDrain(t *testing.T) {
	t.Run("waits for a drain in progress", func(t *testing.T) {
		be, release, inside := closeFixture(t)
		select {
		case <-inside:
		case <-time.After(5 * time.Second):
			t.Fatal("the handler never received the barrier message")
		}
		waiting := make(chan struct{})
		testCloseWaitEntered = func() { close(waiting) }
		t.Cleanup(func() { testCloseWaitEntered = nil })
		returned := make(chan struct{})
		go func() { be.Close(); close(returned) }()
		select {
		case <-waiting:
		case <-returned:
			t.Fatal("Close returned without waiting for the drain")
		case <-time.After(5 * time.Second):
			t.Fatal("Close never started its drain")
		}
		select {
		case <-returned:
			t.Fatal("Close returned while a subscription was still draining")
		case <-time.After(500 * time.Millisecond):
		}
		release()
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not return once the drain could finish")
		}
		if !be.Conn().IsClosed() {
			t.Fatal("Close returned with the connection still open")
		}
	})
	t.Run("already closed", func(t *testing.T) {
		be, _, _ := closeFixture(t)
		be.Conn().Close()
		returned := make(chan struct{})
		go func() { be.Close(); close(returned) }()
		select {
		case <-returned:
		case <-time.After(2 * time.Second):
			t.Fatal("Close on a closed connection did not return at once")
		}
	})
	t.Run("a drain that never finishes is bounded", func(t *testing.T) {
		be, _, inside := closeFixture(t)
		<-inside
		prev := serviceCloseWait
		serviceCloseWait = time.Second
		t.Cleanup(func() { serviceCloseWait = prev })
		start := time.Now()
		be.Close()
		if d := time.Since(start); d > 5*time.Second {
			t.Fatalf("Close took %v with a blocked handler; the bound did not apply", d)
		}
		if !be.Conn().IsClosed() {
			t.Fatal("Close returned at its bound without closing the connection")
		}
	})
}

// runUnformableClusterNode starts one node of a three-node JetStream cluster whose other
// two nodes do not exist, so its JetStream never gets a meta leader.
func runUnformableClusterNode(t *testing.T) *natsserver.Server {
	t.Helper()
	closedRoute := func() *url.URL {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		_ = l.Close()
		return &url.URL{Scheme: "nats-route", Host: addr}
	}
	opts := &natsserver.Options{
		ServerName: "lonely",
		Host:       "127.0.0.1",
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		NoSigs:     true,
		Cluster:    natsserver.ClusterOpts{Name: "c", Host: "127.0.0.1", Port: -1},
		Routes:     []*url.URL{closedRoute(), closedRoute()},
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

// recorder keeps the messages logged through it.
type recorder struct {
	mu   sync.Mutex
	msgs []string
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, rec.Message)
	return nil
}
func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }
func (r *recorder) saw(msg string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.msgs, msg)
}

// TestEmbeddedJetStreamWaitHonorsContext: while an in-process server's JetStream cannot
// form, Connect waits between probes, and a cancellation ends that wait at once rather
// than at the next probe.
func TestEmbeddedJetStreamWaitHonorsContext(t *testing.T) {
	old := jsReadyRetryWait
	jsReadyRetryWait = time.Minute
	t.Cleanup(func() { jsReadyRetryWait = old })

	srv := runUnformableClusterNode(t)
	cfg := config.Default()
	cfg.JSTimeout = 500 * time.Millisecond
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := Connect(ctx, cfg, srv, slog.New(rec))
		done <- err
	}()
	deadline := time.Now().Add(30 * time.Second)
	for !rec.saw("waiting for JetStream") {
		if time.Now().After(deadline) {
			t.Fatal("Connect never reported waiting for JetStream")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Connect = %v, want the cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect still waiting 2s after cancellation")
	}
}
