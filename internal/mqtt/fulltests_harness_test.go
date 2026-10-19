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
	"runtime"
	"sync"
	"testing"
	"time"
	"weak"

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

// testMQTTWaitRetained blocks until the broker's retained-message index carries
// an entry for the given NATS subject (the dotted form of the MQTT topic).
//
// A retained message is stored after its live fan-out, so observing the live
// PUBLISH does not mean a subscribe issued straight afterwards would replay it.
// This waits for the store instead of resubscribing until one happens to catch
// it, which conflates store readiness with subscription behaviour.
func testMQTTWaitRetained(t testing.TB, s *Server, subject string) {
	t.Helper()
	asm := testMQTTGetGlobalASM(t, s)
	checkFor(t, testMQTTTimeout, 10*time.Millisecond, func() error {
		asm.mu.RLock()
		defer asm.mu.RUnlock()
		if asm.retmsgs == nil {
			return fmt.Errorf("no retained index yet")
		}
		if ref, ok := asm.retmsgs.Find([]byte(subject)); !ok || ref.deleted {
			return fmt.Errorf("no retained message for %q yet", subject)
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

// testEmbeddedRegistry owns the pairing between a shim *Server and the embedded
// nats-server backing it, and the StoreDir reuse map beside it.
//
// It owns its lock as well as its maps, and neither is reachable from outside:
// that is what makes testMQTTRunServer callable from parallel tests. A bare
// package-level map would be shared mutable state that any future line could
// touch unlocked, and the parallel-safety guard would have to be told to ignore
// it — an exemption that stays true only until someone edits nearby. With the
// state behind a type there is nothing to exempt, because there is no unlocked
// path to write.
//
// Entries are keyed by a *Server, or by the StoreDir, that exactly one test owns,
// so concurrent tests touch disjoint keys.
//
// byDir tracks the live embedded nats-server (if any) for a given StoreDir. The
// ported suite's restart idiom is
// s.Shutdown(); testSetStoreDir(o, <same dir>); s = testMQTTRunServer(t, o) —
// s.Shutdown() only stops the shim's MQTT listener (Server.Shutdown,
// shim_public.go), not the paired embedded nats-server (by design: in production
// the bridge restarts against an external, never-restarted NATS/JetStream
// deployment). Without it, testMQTTRunServer would start a SECOND embedded
// nats-server pointed at the same on-disk store while the first is still
// running — a file-store conflict that corrupts JetStream state (observed as
// stale/reverted retained messages). Reusing the still-live instance instead
// matches production restart semantics: only the MQTT layer cycles, JetStream
// stays up.
// embeddedServer is one embedded nats-server and the lifecycle state of the
// StoreDir it owns. A directory is in exactly one of three states, and the
// channels are what let a caller wait for the state it needs without holding the
// registry lock across a multi-second server start or shutdown:
//
//	creating  ready is open   — one caller is starting it, everybody else waits
//	live      ready is closed — refs shims are paired with it
//	closing   closing non-nil — the last ref went away and shutdown is running
//
// The states matter because two nats-servers on one on-disk store corrupt
// JetStream state, which the comment above records as observed, not theoretical.
// Splitting "is there one?" from "make one" is what allows that, and splitting
// "forget it" from "shut it down" allows the same collision in the other
// direction: a new acquisition adopting a directory whose server is mid-shutdown.
type embeddedServer struct {
	dir     string
	ready   chan struct{} // closed once ns and err are final
	ns      *natsserver.Server
	err     error
	refs    int           // shims currently paired with it
	closing chan struct{} // non-nil once the last ref went away
}

func (e *embeddedServer) server() *natsserver.Server {
	<-e.ready
	return e.ns
}

type testEmbeddedRegistry struct {
	mu       sync.Mutex
	byDir    map[string]*embeddedServer
	byServer map[*Server]*embeddedServer
}

var testEmbedded = &testEmbeddedRegistry{
	byDir:    map[string]*embeddedServer{},
	byServer: map[*Server]*embeddedServer{},
}

// acquire returns the embedded server for dir, starting it exactly once however
// many callers arrive together, and taking a reference for the pairing that must
// follow. A caller that finds the directory mid-shutdown waits for that to finish
// rather than racing a second server onto the same store.
func (r *testEmbeddedRegistry) acquire(dir string, start func() (*natsserver.Server, error)) (*embeddedServer, error) {
	for {
		r.mu.Lock()
		e, ok := r.byDir[dir]
		if ok && e.closing != nil {
			r.mu.Unlock()
			<-e.closing
			continue // the directory is free now; look again
		}
		if ok {
			r.reapLocked(e)
			e.refs++
			r.mu.Unlock()
			<-e.ready
			if e.err != nil {
				r.releaseEntry(e)
				return nil, e.err
			}
			return e, nil
		}
		e = &embeddedServer{dir: dir, ready: make(chan struct{}), refs: 1}
		r.byDir[dir] = e
		r.mu.Unlock()

		e.ns, e.err = start()
		close(e.ready)
		if e.err != nil {
			// Drop the failed entry so a later attempt is not stuck with it.
			r.mu.Lock()
			if r.byDir[dir] == e {
				delete(r.byDir, dir)
			}
			r.mu.Unlock()
			return nil, e.err
		}
		return e, nil
	}
}

// pair records which embedded server a shim is using. The reference it consumes
// was taken by acquire, or by transfer from the shim being replaced.
func (r *testEmbeddedRegistry) pair(s *Server, e *embeddedServer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byServer[s] = e
}

func (r *testEmbeddedRegistry) paired(s *Server) *natsserver.Server {
	r.mu.Lock()
	e := r.byServer[s]
	r.mu.Unlock()
	if e == nil {
		return nil
	}
	return e.server()
}

// transfer hands a shim's reference to its caller, which must pair a replacement
// shim with it. This is the restart idiom — the MQTT layer cycles while the
// embedded server stays up — so the reference moves rather than being dropped.
func (r *testEmbeddedRegistry) transfer(s *Server) *embeddedServer {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.byServer[s]
	delete(r.byServer, s)
	return e
}

// release drops a shim's reference and, when it was the last one, shuts the
// embedded server down. The directory stays reserved until that shutdown
// returns: a caller arriving in between waits in acquire rather than starting a
// second server on the same store.
func (r *testEmbeddedRegistry) release(s *Server) {
	r.mu.Lock()
	e := r.byServer[s]
	delete(r.byServer, s)
	r.mu.Unlock()
	if e != nil {
		r.releaseEntry(e)
	}
}

// TestEmbeddedRegistryReclaimsARestartedShim pins the accounting for the restart
// idiom, which is the one shape reference counting cannot express on its own: a
// raw s.Shutdown() followed by another testMQTTRunServer against the same
// StoreDir, where the first shim never releases what it took.
//
// Without reaping, that reference is never returned, the count never reaches
// zero, and the embedded nats-server, its goroutines and its store survive to
// process exit. The assertion is on the registry rather than on any symptom
// because the symptom is a slow leak that no test would fail on.
func TestEmbeddedRegistryReclaimsARestartedShim(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)

	// The restart idiom: the shim stops, the embedded server deliberately does
	// not, and the replacement re-acquires the same store.
	s.Shutdown()
	s2 := testMQTTRunServer(t, o)
	testMQTTShutdownServer(s2)

	testEmbedded.mu.Lock()
	defer testEmbedded.mu.Unlock()
	if e, ok := testEmbedded.byDir[testStoreDir(o)]; ok {
		t.Fatalf("the store is still registered after every shim using it went away "+
			"(refs=%d): its embedded nats-server will never be shut down", e.refs)
	}
	for shim, e := range testEmbedded.byServer {
		if e.dir == testStoreDir(o) {
			t.Fatalf("shim %p is still paired with %q after shutdown", shim, testStoreDir(o))
		}
	}
}

// reapLocked drops references held by shims that are no longer running.
//
// The restart idiom the ported suite uses is a raw s.Shutdown() followed by
// another testMQTTRunServer against the same StoreDir — deliberately NOT
// testMQTTShutdownServer, because the embedded server has to stay up across the
// restart. That shim never releases its reference, so counting alone would keep
// every restarted store's server alive until process exit. Reaping is what makes
// reference counting fit an idiom that cannot hand its reference over: a shim
// that has been shut down is not using the server any more, whatever it forgot
// to say.
// The signal is shutdownDone, NOT isRunning. BeginShutdown clears running
// synchronously and then drains in the background, and Shutdown returns after
// shutdownDrainTimeout whether or not that finished — so a shim can be
// not-running while it is still persisting sessions and Wills. Reaping on
// running would let the replacement's final release shut the embedded server
// down underneath that drain. A drain that never completes holds its reference,
// which is the safe direction: the next acquire or release for the same store
// reaps it once it does.
func (r *testEmbeddedRegistry) reapLocked(e *embeddedServer) {
	for s, paired := range r.byServer {
		if paired != e {
			continue
		}
		select {
		case <-s.shutdownDone:
			delete(r.byServer, s)
			e.refs--
		default:
		}
	}
}

func (r *testEmbeddedRegistry) releaseEntry(e *embeddedServer) {
	r.mu.Lock()
	r.reapLocked(e)
	e.refs--
	if e.refs < 0 {
		r.mu.Unlock()
		// Every reference is created by acquire and destroyed exactly once, by
		// reapLocked when its shim died without saying so or by this decrement.
		// Going negative means one was destroyed twice, which would let a live
		// store's server be shut down under a test still using it.
		panic(fmt.Sprintf("testEmbeddedRegistry: reference count for %q went negative", e.dir))
	}
	if e.refs > 0 {
		r.mu.Unlock()
		return
	}
	e.closing = make(chan struct{})
	r.mu.Unlock()

	if ns := e.server(); ns != nil {
		ns.Shutdown()
	}

	r.mu.Lock()
	if r.byDir[e.dir] == e {
		delete(r.byDir, e.dir)
	}
	r.mu.Unlock()
	close(e.closing)
}

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
	if testStoreDir(o) == _EMPTY_ {
		testSetStoreDir(o, t.TempDir())
	}
	entry, err := testEmbedded.acquire(testStoreDir(o), func() (*natsserver.Server, error) {
		ns, err := natsserver.NewServer(&natsserver.Options{
			Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: testStoreDir(o),
		})
		if err != nil {
			return nil, fmt.Errorf("creating embedded nats-server: %w", err)
		}
		go ns.Start()
		if !ns.ReadyForConnections(10 * time.Second) {
			ns.Shutdown()
			return nil, errors.New("embedded nats-server not ready")
		}
		return ns, nil
	})
	if err != nil {
		t.Fatalf("Error starting embedded nats-server: %v", err)
	}
	// The reference acquire took is released unless pairing commits it: t.Fatalf
	// unwinds this goroutine only, and the rest of the parallel run continues with
	// the entry stranded otherwise.
	paired := false
	defer func() {
		if !paired {
			testEmbedded.releaseEntry(entry)
		}
	}()
	nc, err := nats.Connect(entry.server().ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to embedded nats-server: %v", err)
	}
	s := NewServer(nc, o, testServerName(o))
	testEmbedded.pair(s, entry)
	paired = true
	return s, nil
}

// testMQTTRestartServerSameStore replaces oldS (already s.Shutdown(), NOT
// testMQTTShutdownServer'd so its paired embedded nats-server/JetStream store
// is still running) with a new *Server bound to a fresh connection to that
// SAME embedded nats-server, so restart-persistence tests resume against the
// same JetStream state.
func testMQTTRestartServerSameStore(t testing.TB, oldS *Server, o *Options) *Server {
	t.Helper()
	entry := testEmbedded.transfer(oldS)
	if entry == nil {
		t.Fatal("testMQTTRestartServerSameStore: oldS has no paired embedded nats-server")
	}
	// transfer already took the old shim's ownership away, so a failure before
	// pairing would strand the reference with no shim left to release it.
	paired := false
	defer func() {
		if !paired {
			testEmbedded.releaseEntry(entry)
		}
	}()
	nc, err := nats.Connect(entry.server().ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to embedded nats-server: %v", err)
	}
	s := NewServer(nc, o, testServerName(o))
	testEmbedded.pair(s, entry)
	paired = true
	return s
}

// ClientURL returns the paired embedded nats-server's client URL.
func (s *Server) ClientURL() string {
	ns := testEmbedded.paired(s)
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
	ns := testEmbedded.paired(s)
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

// testHarnessInputs is what the harness reads beside an *Options and the broker never
// does: the embedded nats-server store a broker pairs with, and the node name it is
// started under. The zero value is the old fields' zero value: no store yet (the
// harness allocates one on the first run and records it), and an empty node name
// (NewServer generates one).
type testHarnessInputs struct {
	storeDir   string
	serverName string
}

// testHarnessTable holds the inputs per *Options. The key is weak and the entry is
// dropped by a cleanup when the Options itself is collected, so the inputs live exactly
// as long as the Options does: across every restart a test makes with it, and in a
// parent test that reuses an Options a subtest touched first. A struct copy of an
// Options does not carry them; copy them with the helpers. Like testEmbedded, it is
// one mutex-owned registry the parallel tests share: each test reaches only the entries
// of the Options it created, and every access holds mu.
type testHarnessTable struct {
	mu sync.Mutex
	m  map[weak.Pointer[Options]]*testHarnessInputs
}

var testHarness = &testHarnessTable{m: make(map[weak.Pointer[Options]]*testHarnessInputs)}

// update runs f on o's inputs, creating them on first use. The cleanup captures only
// the weak key, never o, or o could never be collected; KeepAlive holds o reachable
// until the entry has been read or written, or the cleanup could remove the entry
// between weak.Make and the lookup.
func (h *testHarnessTable) update(o *Options, f func(*testHarnessInputs)) {
	key := weak.Make(o)
	h.mu.Lock()
	in, ok := h.m[key]
	if !ok {
		in = &testHarnessInputs{}
		h.m[key] = in
		runtime.AddCleanup(o, h.forget, key)
	}
	f(in)
	h.mu.Unlock()
	runtime.KeepAlive(o)
}

func (h *testHarnessTable) forget(key weak.Pointer[Options]) {
	h.mu.Lock()
	delete(h.m, key)
	h.mu.Unlock()
}

func (h *testHarnessTable) read(o *Options) testHarnessInputs {
	key := weak.Make(o)
	h.mu.Lock()
	var in testHarnessInputs
	if p, ok := h.m[key]; ok {
		in = *p
	}
	h.mu.Unlock()
	runtime.KeepAlive(o)
	return in
}

// testStoreDir is the embedded nats-server store o's broker pairs with ("" before the
// first run allocates one).
func testStoreDir(o *Options) string { return testHarness.read(o).storeDir }

// testSetStoreDir points o's broker at a store: a restart onto the same JetStream
// state, or a second broker sharing another's.
func testSetStoreDir(o *Options, dir string) {
	testHarness.update(o, func(in *testHarnessInputs) { in.storeDir = dir })
}

// testServerName is the node name the harness starts o's broker under.
func testServerName(o *Options) string { return testHarness.read(o).serverName }

func testSetServerName(o *Options, name string) {
	testHarness.update(o, func(in *testHarnessInputs) { in.serverName = name })
}

// The harness inputs are not broker configuration, so nothing in the suite would notice
// one being lost or merged: a restart test sharing the "" store key keeps its data
// either way. This pins what each input does to the pairing directly.
func TestFulltestsHarnessStoreInputs(t *testing.T) {
	a, b := testMQTTDefaultOptions(), testMQTTDefaultOptions()
	sa := testMQTTRunServer(t, a)
	defer testMQTTShutdownServer(sa)
	sb := testMQTTRunServer(t, b)

	// (a) Two fresh options get their own stores and their own embedded servers.
	dirA, dirB := testStoreDir(a), testStoreDir(b)
	if dirA == _EMPTY_ || dirB == _EMPTY_ || dirA == dirB {
		t.Fatalf("each run must record its own store: a=%q b=%q", dirA, dirB)
	}
	if sa.ClientURL() == sb.ClientURL() {
		t.Fatalf("separate stores must pair with separate embedded servers: both %q", sa.ClientURL())
	}

	// (b) Re-pointing an already-run options at another store moves its next broker there.
	testMQTTShutdownServer(sb)
	testSetStoreDir(b, dirA)
	sb2 := testMQTTRunServer(t, b)
	defer testMQTTShutdownServer(sb2)
	if sb2.ClientURL() != sa.ClientURL() {
		t.Fatalf("a re-pointed store must pair with that store's embedded server: got %q, want %q",
			sb2.ClientURL(), sa.ClientURL())
	}

	// (c) A fresh options whose first store write copies another's shares that server.
	c := testMQTTDefaultOptions()
	testSetStoreDir(c, testStoreDir(a))
	sc := testMQTTRunServer(t, c)
	defer testMQTTShutdownServer(sc)
	if sc.ClientURL() != sa.ClientURL() {
		t.Fatalf("a copied store must pair with that store's embedded server: got %q, want %q",
			sc.ClientURL(), sa.ClientURL())
	}

	// (d) The default node name is generated once and kept; an explicit empty one sticks.
	d := testMQTTDefaultOptions()
	if n := testServerName(d); n == _EMPTY_ || n != testServerName(d) {
		t.Fatalf("the default node name must be generated and stable: %q", n)
	}
	testSetServerName(d, _EMPTY_)
	if n := testServerName(d); n != _EMPTY_ {
		t.Fatalf("an explicit empty node name must read back empty, got %q", n)
	}
}

// DefaultOptions mirrors upstream's server.DefaultOptions() for the fields the
// ported suite relies on.
func DefaultOptions() *Options {
	return &Options{MaxPayload: MAX_PAYLOAD_SIZE}
}
