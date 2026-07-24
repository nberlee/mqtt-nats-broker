//go:build mqttport_fulltests

// Harness shims that let the ported upstream MQTT test suite run against the
// standalone broker: an embedded nats-server provides NATS+JetStream (the
// upstream suite assumes the MQTT server embeds one), and the upstream
// Server/assertion helpers are mapped onto the shim Server. See docs/PORTING.md
// ("Testing strategy").

package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// MAX_PAYLOAD_SIZE mirrors the upstream nats-server default max payload used
// throughout the ported suite.
const MAX_PAYLOAD_SIZE = 1024 * 1024

// globalAccountName is the name of the default/global NATS account, matching
// the shim's single-account model (see Server.globalAccount).
const globalAccountName = "$G"

// JetStreamStoreDir is the subdirectory name nats-server uses under a
// account/instance store dir for JetStream state; kept for source
// compatibility with upstream helpers that trim it from a store dir path.
const JetStreamStoreDir = "jetstream"

// DummyLogger mirrors upstream server.DummyLogger closely enough for the
// ported suite's embedding and log-capture assertions.
type DummyLogger struct {
	sync.Mutex
	Msg     string
	AllMsgs []string
}

func (l *DummyLogger) record(format string, v ...any) {
	l.Lock()
	defer l.Unlock()
	l.Msg = fmt.Sprintf(format, v...)
	l.AllMsgs = append(l.AllMsgs, l.Msg)
}

func (l *DummyLogger) Noticef(format string, v ...any) { l.record(format, v...) }
func (l *DummyLogger) Warnf(format string, v ...any)   { l.record(format, v...) }
func (l *DummyLogger) Errorf(format string, v ...any)  { l.record(format, v...) }
func (l *DummyLogger) Fatalf(format string, v ...any)  { l.record(format, v...) }
func (l *DummyLogger) Debugf(format string, v ...any)  { l.record(format, v...) }
func (l *DummyLogger) Tracef(format string, v ...any)  { l.record(format, v...) }

// captureWarnLogger, captureErrorLogger, captureFatalLogger, captureDebugLogger
// mirror the upstream test loggers of the same names (server/client_test.go,
// server/leafnode_test.go, server/websocket_test.go, server/jwt_test.go): each
// forwards one log level into a channel. The shim's Server.SetLogger adapter
// routes slog records to these methods so the inherited log assertions run.

type captureWarnLogger struct {
	DummyLogger
	warn chan string
}

func (l *captureWarnLogger) Warnf(format string, v ...any) {
	select {
	case l.warn <- fmt.Sprintf(format, v...):
	default:
	}
}

type captureErrorLogger struct {
	DummyLogger
	errCh chan string
}

func (l *captureErrorLogger) Errorf(format string, v ...any) {
	select {
	case l.errCh <- fmt.Sprintf(format, v...):
	default:
	}
}

type captureFatalLogger struct {
	DummyLogger
	fatalCh chan string
}

func (l *captureFatalLogger) Fatalf(format string, v ...any) {
	select {
	case l.fatalCh <- fmt.Sprintf(format, v...):
	default:
	}
}

type captureDebugLogger struct {
	DummyLogger
	dbgCh chan string
}

func (l *captureDebugLogger) Debugf(format string, v ...any) {
	select {
	case l.dbgCh <- fmt.Sprintf(format, v...):
	default:
	}
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

func require_NoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func require_NotNil[T any](t testing.TB, vs ...T) {
	t.Helper()
	for _, v := range vs {
		r := reflect.ValueOf(v)
		switch r.Kind() {
		case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
			if r.IsNil() {
				t.Fatalf("Expected a non-nil value")
			}
		}
	}
}

func require_Len(t testing.TB, a, b int) {
	t.Helper()
	if a != b {
		t.Fatalf("require len, but got: %v != %v", a, b)
	}
}

func require_Error(t testing.TB, err error, expected ...error) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected an error, got none")
	}
	if len(expected) == 0 {
		return
	}
	for _, e := range expected {
		if errors.Is(err, e) {
			return
		}
	}
	t.Fatalf("Expected one of %v, got: %v", expected, err)
}

func require_True(t testing.TB, b bool) {
	t.Helper()
	if !b {
		t.Fatalf("Expected true, got false")
	}
}

func require_False(t testing.TB, b bool) {
	t.Helper()
	if b {
		t.Fatalf("Expected false, got true")
	}
}

func require_Equal[T comparable](t testing.TB, a, b T) {
	t.Helper()
	if a != b {
		t.Fatalf("Expected %v, got %v", b, a)
	}
}

// ---------------------------------------------------------------------------
// NATS client helpers
// ---------------------------------------------------------------------------

func natsConnect(t testing.TB, url string, opts ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	return nc
}

// jsClientConnect, natsSubSync, natsNexMsg, natsFlush, natsPub mirror the
// upstream test helpers of the same names (server/jetstream_helpers_test.go,
// server/gateway_test.go).

func jsClientConnect(t testing.TB, s *Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func natsSubSync(t testing.TB, nc *nats.Conn, subj string) *nats.Subscription {
	t.Helper()
	sub, err := nc.SubscribeSync(subj)
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	return sub
}

func natsNexMsg(t testing.TB, sub *nats.Subscription, timeout time.Duration) *nats.Msg {
	t.Helper()
	msg, err := sub.NextMsg(timeout)
	if err != nil {
		t.Fatalf("Failed getting next message: %v", err)
	}
	return msg
}

func natsFlush(t testing.TB, nc *nats.Conn) {
	t.Helper()
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
}

func natsPub(t testing.TB, nc *nats.Conn, subj string, payload []byte) {
	t.Helper()
	if err := nc.Publish(subj, payload); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
}

// checkFor polls fn until it returns nil or totalWait elapses, sleeping sleepDur
// between attempts. Mirrors the upstream test helper of the same name.
func checkFor(t testing.TB, totalWait, sleepDur time.Duration, fn func() error) {
	t.Helper()
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	if err != nil {
		t.Fatal(err.Error())
	}
}

// checkClientsCount polls the server's connected-client count until it matches
// expected or times out.
func checkClientsCount(t testing.TB, s *Server, expected int) {
	t.Helper()
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		s.mu.Lock()
		n := len(s.clients)
		s.mu.Unlock()
		if n != expected {
			return fmt.Errorf("expected %d clients, got %d", expected, n)
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Embedded-server pairing: the upstream suite assumes the MQTT *Server it
// drives IS an embedded nats-server. Here each shim Server started by
// testMQTTRunServer is paired with a real embedded natsserver.Server that
// backs it via its NATS connection.
// ---------------------------------------------------------------------------

var (
	testEmbeddedMu   sync.Mutex
	testEmbeddedNATS = map[*Server]*natsserver.Server{}
	// testEmbeddedNATSByDir tracks the live embedded nats-server (if any) for a
	// given StoreDir. The ported suite's restart idiom is
	// s.Shutdown(); o.StoreDir = <same dir>; s = testMQTTRunServer(t, o) —
	// s.Shutdown() only stops the shim's MQTT listener (Server.Shutdown,
	// shim_public.go), not the paired embedded nats-server (by design: in
	// production the bridge restarts against an external, never-restarted
	// NATS/JetStream deployment). Without this registry, testMQTTRunServer
	// would start a SECOND embedded nats-server pointed at the same on-disk
	// store while the first is still running — a file-store conflict that
	// corrupts JetStream state (observed as stale/reverted retained messages).
	// Reusing the still-live instance instead matches production restart
	// semantics: only the MQTT layer cycles, JetStream stays up.
	testEmbeddedNATSByDir = map[string]*natsserver.Server{}
)

// mqttTestLogger is the upstream server.Logger method set that the ported
// suite's capture loggers (DummyLogger and the captureX loggers) implement.
type mqttTestLogger interface {
	Noticef(format string, v ...any)
	Warnf(format string, v ...any)
	Errorf(format string, v ...any)
	Fatalf(format string, v ...any)
	Debugf(format string, v ...any)
	Tracef(format string, v ...any)
}

// slogToTestLogger is an slog.Handler that forwards records to a typed test
// logger, mapping slog levels back to the upstream Logger methods so the
// ported suite's log-capture assertions work. The shim's Server.Fatalf and
// Server.Errorf both emit at slog.Error (shim_engine.go), so an Error record is
// fanned out to BOTH Errorf and Fatalf; each captureX logger wires exactly one
// of them to a channel and no-ops the rest (via embedded DummyLogger), so only
// the intended channel observes it.
type slogToTestLogger struct{ l mqttTestLogger }

func (h slogToTestLogger) Enabled(context.Context, slog.Level) bool { return true }
func (h slogToTestLogger) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h slogToTestLogger) WithGroup(string) slog.Handler            { return h }

func (h slogToTestLogger) Handle(_ context.Context, r slog.Record) error {
	msg := r.Message
	switch {
	case r.Level >= slog.LevelError:
		h.l.Errorf("%s", msg)
		h.l.Fatalf("%s", msg)
	case r.Level >= slog.LevelWarn:
		h.l.Warnf("%s", msg)
	case r.Level >= slog.LevelInfo:
		h.l.Noticef("%s", msg)
	default:
		h.l.Debugf("%s", msg)
		h.l.Tracef("%s", msg)
	}
	return nil
}

// SetLogger installs a test capture logger by routing the shim's slog output to
// it (the shim is slog-based, not the upstream Logger interface). The Server's
// log field is atomic, so this is safe on an already-running server.
func (s *Server) SetLogger(l any, debug, trace bool) {
	tl, ok := l.(mqttTestLogger)
	if !ok {
		return
	}
	s.log.Store(slog.New(slogToTestLogger{l: tl}))
}

// readyForConnections polls the MQTT listener address until it is bound or d
// elapses.
func (s *Server) readyForConnections(d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if s.Addr() != nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("server not ready for connections after %v", d)
}

// Name returns this instance's node name (see Server.NodeName).
func (s *Server) Name() string { return s.NodeName() }

// GlobalAccount exposes the shim's single global account (see
// Server.globalAccount), matching upstream's exported accessor name.
func (s *Server) GlobalAccount() *Account { return s.globalAccount() }

// testMQTTNewServerFromOptions mirrors the upstream single-arg NewServer(o) for
// call sites that construct a *Server directly (outside testMQTTRunServer): it
// pairs a fresh embedded nats-server + connection, matching testMQTTRunServer's
// pairing so testMQTTShutdownServer/ClientURL/JetStreamConfig work on the
// result. The shim's NewServer performs no option validation, so err is always
// nil; kept as a return value for call-site compatibility with the upstream
// idiom (var s, err = NewServer(o)).
func testMQTTNewServerFromOptions(t testing.TB, o *Options) (*Server, error) {
	t.Helper()
	if o.StoreDir == _EMPTY_ {
		o.StoreDir = t.TempDir()
	}
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: o.StoreDir,
	})
	if err != nil {
		t.Fatalf("Error creating embedded nats-server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatalf("Error connecting to embedded nats-server: %v", err)
	}
	s := NewServer(nc, o, o.ServerName)
	testEmbeddedMu.Lock()
	testEmbeddedNATS[s] = ns
	testEmbeddedMu.Unlock()
	return s, nil
}

// testMQTTRestartServerSameStore replaces oldS (already s.Shutdown(), NOT
// testMQTTShutdownServer'd so its paired embedded nats-server/JetStream store
// is still running) with a new *Server bound to a fresh connection to that
// SAME embedded nats-server, so restart-persistence tests resume against the
// same JetStream state.
func testMQTTRestartServerSameStore(t testing.TB, oldS *Server, o *Options) *Server {
	t.Helper()
	testEmbeddedMu.Lock()
	ns := testEmbeddedNATS[oldS]
	delete(testEmbeddedNATS, oldS)
	testEmbeddedMu.Unlock()
	if ns == nil {
		t.Fatal("testMQTTRestartServerSameStore: oldS has no paired embedded nats-server")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to embedded nats-server: %v", err)
	}
	s := NewServer(nc, o, o.ServerName)
	testEmbeddedMu.Lock()
	testEmbeddedNATS[s] = ns
	testEmbeddedMu.Unlock()
	return s
}

// ClientURL returns the paired embedded nats-server's client URL.
func (s *Server) ClientURL() string {
	testEmbeddedMu.Lock()
	ns := testEmbeddedNATS[s]
	testEmbeddedMu.Unlock()
	if ns == nil {
		return _EMPTY_
	}
	return ns.ClientURL()
}

// JetStreamConfigShim is a minimal stand-in for upstream's JetStreamConfig,
// exposing only the field the ported suite reads.
type JetStreamConfigShim struct {
	StoreDir string
}

// JetStreamConfig returns the store dir of the paired embedded nats-server.
func (s *Server) JetStreamConfig() *JetStreamConfigShim {
	testEmbeddedMu.Lock()
	ns := testEmbeddedNATS[s]
	testEmbeddedMu.Unlock()
	if ns == nil {
		return &JetStreamConfigShim{}
	}
	return &JetStreamConfigShim{StoreDir: ns.JetStreamConfig().StoreDir}
}

// sizedString mirrors the upstream benchmark helper of the same name
// (server/websocket_test.go): a pseudo-random string of the given length.
var sizedStringCh = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")

func sizedString(sz int) string {
	b := make([]byte, sz)
	for i := range b {
		b[i] = sizedStringCh[rand.Intn(len(sizedStringCh))]
	}
	return string(b)
}

// testCreateAllowedConnectionTypes mirrors the upstream helper of the same
// name (server/websocket_test.go).
func testCreateAllowedConnectionTypes(list []string) map[string]struct{} {
	if len(list) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(list))
	for _, l := range list {
		m[l] = struct{}{}
	}
	return m
}

// NewAccount mirrors upstream's server.NewAccount(name) constructor.
func NewAccount(name string) *Account {
	return &Account{name: name}
}

// DefaultOptions mirrors upstream's server.DefaultOptions() for the fields the
// ported suite relies on.
func DefaultOptions() *Options {
	return &Options{
		Host:       "127.0.0.1",
		Port:       -1,
		MaxPayload: MAX_PAYLOAD_SIZE,
		JetStream:  true,
	}
}
