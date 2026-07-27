// Behavioral shims for the NATS-server `client` and `Server` types plus the
// configuration structs (`Options`/`MQTTOpts`) the MQTT core reads. These
// replace NATS-server's internal runtime with a standalone engine backed by the
// nats.go client and a plain net.Conn (routing, connection I/O, and auth
// passthrough). See docs/PORTING.md.

package mqtt

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// ---------------------------------------------------------------------------
// Client flags
// ---------------------------------------------------------------------------

type clientFlag uint16

const (
	connectReceived clientFlag = 1 << iota
	expectConnect
)

func (cf *clientFlag) set(c clientFlag)       { *cf |= c }
func (cf *clientFlag) clear(c clientFlag)     { *cf &^= c }
func (cf clientFlag) isSet(c clientFlag) bool { return cf&c != 0 }
func (cf *clientFlag) setIfNotSet(c clientFlag) bool {
	if *cf&c == 0 {
		*cf |= c
		return true
	}
	return false
}

type readCacheFlag uint16

const hasMappings readCacheFlag = 1 << iota

func (rf readCacheFlag) isSet(c readCacheFlag) bool { return rf&c != 0 }
func (rf *readCacheFlag) set(c readCacheFlag)       { *rf |= c }

// ---------------------------------------------------------------------------
// Permissions (auth is passed through to NATS, so in-broker checks are
// permissive: NATS enforces per-client publish/subscribe authorization).
// ---------------------------------------------------------------------------

type permissions struct{}

// websocket is a placeholder; MQTT-over-WebSocket is a later milestone.
type websocket struct{}

// PinnedCertSet mirrors nats-server's set of pinned certificate fingerprints.
type PinnedCertSet map[string]struct{}

// ---------------------------------------------------------------------------
// Options / MQTTOpts — the configuration surface the MQTT core reads. Populated
// from internal/config by the server bootstrap.
// ---------------------------------------------------------------------------

type Options struct {
	Host        string
	Port        int
	ServerName  string
	MaxConn     int
	MaxPayload  int32
	MaxSubs     int
	AllowNonTLS bool
	// PingInterval: test-suite compatibility with upstream's NATS-core ping/pong
	// tuning; unused by the broker (the shim has no separate NATS-core ping
	// loop distinct from MQTT keep-alive).
	PingInterval time.Duration

	AuthTimeout float64
	Username    string
	Password    string
	Token       string
	// Authorization: test-suite compatibility with upstream's single-token
	// top-level auth field name; unused by the broker (authorization is
	// enforced by NATS via auth passthrough, see Server.isClientAuthorized).
	Authorization string
	NoAuthUser    string
	Users         []*User
	Nkeys         []*NkeyUser

	TLSConfig      *tls.Config
	TLSPinnedCerts PinnedCertSet

	MaxTracedMsgLen int

	JetStream          bool
	JetStreamDomain    string
	JsAccDefaultDomain map[string]string
	StreamReplicas     int

	// StoreDir, NoLog, Debug, Trace: test-suite compatibility with the upstream
	// embedded-server harness (mqttport_fulltests); unused by the broker itself
	// (logging is slog-based, see shim_engine.go's Server.logger). StoreDir is
	// read by testMQTTRunServer to seed the paired embedded nats-server's store.
	StoreDir string
	NoLog    bool
	Debug    bool
	Trace    bool

	Cluster  ClusterOpts
	Gateway  GatewayOpts
	LeafNode LeafNodeOpts
	Routes   []*url.URL

	MQTT MQTTOpts
}

// Clone returns a shallow copy of o, mirroring upstream server.Options.Clone()
// for the option-validation test suite (each field is reassigned wholesale by
// callers, not mutated in place).
func (o *Options) Clone() *Options {
	clone := *o
	return &clone
}

type ClusterOpts struct{ Port int }
type GatewayOpts struct{ Port int }
type LeafNodeOpts struct {
	Port    int
	Remotes []any
}

type MQTTOpts struct {
	Host        string
	Port        int
	MaxConn     int
	AllowNonTLS bool

	Username   string
	Password   string
	Token      string
	NoAuthUser string

	AuthTimeout    float64
	TLSTimeout     float64
	TLSConfig      *tls.Config
	TLSPinnedCerts PinnedCertSet

	AckWait                   time.Duration
	ConsumerInactiveThreshold time.Duration
	ConsumerMemoryStorage     bool
	ConsumerReplicas          int
	StreamReplicas            int
	JSAPITimeout              time.Duration
	JsDomain                  string
	MaxAckPending             uint16

	// MQTT 5 knobs. ReceiveMaximum/TopicAliasMaximum/KeepAliveMaximum are signed
	// so validateMQTTOptions' range checks (which include a "< 0" bound) are
	// meaningful; -1 encodes "disabled" (0 = default) for the first two, as
	// upstream does.
	MaxProtocolVersion byte
	ReceiveMaximum     int
	TopicAliasMaximum  int
	KeepAliveMaximum   int32
	// ResponseInformation is the base response topic returned in a CONNACK
	// Response Information property when a v5 client sets Request Response
	// Information = 1. Empty = never sent. Spec5 [3.2.2.3.15].
	ResponseInformation string

	// Internal tuning flags carried over from upstream.
	downgradeQoS2Sub bool
	rejectQoS2Pub    bool
}

// Info is the subset of NATS-server ServerInfo the MQTT core consults.
type Info struct {
	AuthRequired bool
}

// ---------------------------------------------------------------------------
// Server shim
// ---------------------------------------------------------------------------

type Server struct {
	mu   sync.Mutex
	opts *Options
	info Info
	// log is nil (→ slog.Default()) in production; it is atomic so the test
	// harness's SetLogger can install a capture logger on an already-running
	// server without racing the log reads on server goroutines.
	log atomic.Pointer[slog.Logger]

	// nrc is the long-lived service connection to the external NATS/JetStream
	// cluster, used by the internal (JSA) client for asset management and by the
	// JSA request/reply transport.
	nrc *nats.Conn

	// dialClient, when set, dials a per-MQTT-client NATS connection from the
	// CONNECT credentials (auth passthrough). When nil, clients share the service
	// connection.
	dialClient func(clientID, user, pass string) (*nats.Conn, error)

	mqtt srvMQTT

	accounts     sync.Map
	clients      map[uint64]*client
	users        map[string]*User
	ipQueues     sync.Map
	totalClients int

	quitCh chan struct{}
	grWG   sync.WaitGroup
	// clientWG tracks accepted client readLoops (reserved under s.mu at
	// registration, released once via client.finish). Shutdown Phase 1 waits on
	// it while quitCh is still open so draining clients can persist sessions and
	// enqueue delayed Wills; grWG (background/JSA goroutines) is waited later.
	clientWG sync.WaitGroup

	// connSetupWG tracks accepted connections whose setup goroutine is still
	// running, i.e. the whole of createMQTTClient — including the TLS handshake,
	// which is why shutdown waits on it only AFTER draining clients (see
	// runShutdown): a client parked in its handshake is already registered, so the
	// drain closing its socket is what lets setup finish. Waiting earlier would
	// deadlock the drain against that socket, and would let a blocking handshake
	// hold up shutdown indefinitely. The group exists so no setup goroutine
	// outlives the lifecycle; connGateClosed (guarded by s.mu, mirroring the Will
	// dispatch gate) refuses admissions once shutdown starts and orders the close
	// after every Add, so no Add can race the zero-count Wait.
	connSetupWG    sync.WaitGroup
	connGateClosed bool

	// Shutdown lifecycle (see shim_public.go). shutdownOnce guards a single run
	// of the four-phase lifecycle; shutdownDone closes when every phase finished.
	shutdownOnce sync.Once
	shutdownDone chan struct{}

	// Delayed-Will dispatch coordination (Layer 2 outbox). Fire timers and the
	// claim loops both dispatch publisher workers; shutdown Phase 2 closes the
	// admission gate and drains both dispatch sources (willTimerWG, willClaimWG)
	// before Phase 3 waits on willWorkerWG, so no worker Add races a zero-count
	// Wait. willClaimStopCh signals the claim loops to stop. The willTimers path
	// (trackWillTimer) is unused — the outbox fires on the claim ticker, not
	// per-Will timers — so willTimerWG stays at zero.
	willDispatchMu     sync.Mutex
	willDispatchClosed bool
	willTimers         map[*time.Timer]struct{}
	willClaimStopCh    chan struct{}
	willTimerWG        sync.WaitGroup
	willClaimWG        sync.WaitGroup
	willWorkerWG       sync.WaitGroup

	ldm         bool
	clusterName string

	running atomic.Bool
	gacc    *Account
	gcid    atomic.Uint64
}

func (s *Server) getOpts() *Options {
	if s.opts == nil {
		return &Options{}
	}
	return s.opts
}

// globalAccount returns the single account. gacc is initialized in NewServer so
// this is a race-free read: the eager account-manager init (Start) and the
// accept loop both reach it concurrently, so it must not lazily write s.gacc.
func (s *Server) globalAccount() *Account {
	if s.gacc == nil {
		s.gacc = &Account{name: "$G"}
	}
	return s.gacc
}

func (s *Server) logger() *slog.Logger {
	if l := s.log.Load(); l != nil {
		return l
	}
	return slog.Default()
}

func (s *Server) Noticef(format string, v ...any) { s.logger().Info(fmt.Sprintf(format, v...)) }
func (s *Server) Warnf(format string, v ...any)   { s.logger().Warn(fmt.Sprintf(format, v...)) }
func (s *Server) Debugf(format string, v ...any)  { s.logger().Debug(fmt.Sprintf(format, v...)) }
func (s *Server) Errorf(format string, v ...any)  { s.logger().Error(fmt.Sprintf(format, v...)) }
func (s *Server) Fatalf(format string, v ...any)  { s.logger().Error(fmt.Sprintf(format, v...)) }

func (s *Server) isRunning() bool      { return s.running.Load() }
func (s *Server) isShuttingDown() bool { return !s.running.Load() }

// startGoRoutine runs f under the server's wait group, mirroring the upstream
// bookkeeping used for graceful shutdown. As upstream, it only Add()s here; the
// provided function is responsible for calling grWG.Done() itself.
func (s *Server) startGoRoutine(f func()) bool {
	s.grWG.Add(1)
	go f()
	return true
}

// NodeName returns this instance's unique id, used as the JSA reply-routing
// token. It must be non-empty and unique per bridge instance; if unset, a stable
// id is generated lazily so JSA subjects are always valid.
func (s *Server) NodeName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clusterName == _EMPTY_ {
		s.clusterName = "mnb-" + nuid.Next()
	}
	return s.clusterName
}

func (s *Server) cachedClusterName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clusterName
}

// JetStreamEnabled / JetStreamIsClustered: the standalone broker always runs
// against external JetStream; clustering is a property of that cluster, exposed
// via config/replicas rather than an embedded server.
func (s *Server) JetStreamEnabled() bool     { return true }
func (s *Server) JetStreamIsClustered() bool { return s.getOpts().StreamReplicas > 1 }

// isClientAuthorized implements auth passthrough: it dials a per-client NATS
// connection using the CONNECT credentials. Success (NATS accepted the
// credentials) authorizes the MQTT client and binds its routing connection so
// NATS enforces that client's publish/subscribe permissions. Failure rejects the
// CONNECT with "not authorized". When no dialer is configured, the client falls
// back to the shared service connection (used in tests).
func (s *Server) isClientAuthorized(c *client) bool {
	if s.dialClient == nil {
		if c.nrc == nil {
			c.nrc = s.nrc
		}
		return true
	}
	var user, pass string
	var cid string
	c.mu.Lock()
	user, pass = c.opts.Username, c.opts.Password
	if c.mqtt != nil {
		cid = c.mqtt.cid
	}
	c.mu.Unlock()

	nc, err := s.dialClient(cid, user, pass)
	if err != nil {
		s.Debugf("MQTT auth passthrough rejected client %q: %v", cid, err)
		return false
	}
	c.mu.Lock()
	c.nrc = nc
	c.ownNRC = true
	c.mu.Unlock()
	return true
}

// SetClientDialer installs the per-client NATS dialer used for auth passthrough.
func (s *Server) SetClientDialer(fn func(clientID, user, pass string) (*nats.Conn, error)) {
	s.dialClient = fn
}

// acceptConnections runs the listener accept loop, handing each connection to
// createFn (which builds the MQTT client and starts its read/write loops).
//
// createFn runs on its own goroutine (as upstream nats-server does): it performs
// the TLS handshake, so calling it inline would let a single client that connects
// and never sends a ClientHello stall every subsequent accept for the whole
// handshake timeout. Accepting serially is the problem — the loop cannot reach
// the next connection until the current one finishes its handshake — and MaxConn
// does not make that safe: it caps how many clients are registered, not how long
// one of them may hold up the loop, and it is unlimited by default. Each dispatch
// holds a connSetupWG token so shutdown can wait for in-flight setups; when the
// gate is already closed the connection is closed immediately rather than left
// hanging.
func (s *Server) acceptConnections(l net.Listener, name string, createFn func(net.Conn), errFn func(error)) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if s.isShuttingDown() {
				return
			}
			if errFn != nil {
				errFn(err)
			}
			// Listener closed or transient error; exit the loop on permanent close.
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		if !s.admitConnSetup() {
			conn.Close()
			return
		}
		go func() {
			defer s.connSetupWG.Done()
			createFn(conn)
		}()
	}
}

// admitConnSetup reserves a connection-setup token unless shutdown has closed
// the gate. On true the caller owns one connSetupWG token and must release it
// with s.connSetupWG.Done() when setup finishes; on false the caller must not
// proceed and should close the connection.
func (s *Server) admitConnSetup() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connGateClosed {
		return false
	}
	s.connSetupWG.Add(1)
	return true
}

// closeConnSetupGate refuses further connection-setup admissions. Taking s.mu
// orders it after every successful admitConnSetup Add, so the subsequent
// connSetupWG.Wait cannot race an Add on a zero counter.
func (s *Server) closeConnSetupGate() {
	s.mu.Lock()
	s.connGateClosed = true
	s.mu.Unlock()
}

// createInternalAccountClient returns an internal client bound to the global
// account, used by the session manager for JetStream-assets traffic. Its NATS
// routing connection is the server's service connection.
func (s *Server) createInternalAccountClient() *client {
	return &client{srv: s, acc: s.globalAccount(), nrc: s.nrc, kind: 0}
}

// ---------------------------------------------------------------------------
// Client shim
// ---------------------------------------------------------------------------

type clientOpts struct {
	Username string
	Password string
	Token    string
	Echo     bool
}

// pubArg carries the parsed publish arguments the core threads through delivery.
type pubArg struct {
	subject []byte
	reply   []byte
	hdr     int
	hdb     []byte
	size    int
	szb     []byte
	mapped  []byte
}

type outbound struct {
	fsp int32
}

type client struct {
	mu  sync.Mutex
	srv *Server
	acc *Account
	// nc is the protocol-facing connection: the raw socket, or the TLS wrapper
	// once the handshake has upgraded it. It is written only by the setup
	// goroutine (createMQTTClient) before readLoop starts, and every later reader
	// is ordered after that write, so it needs no lock — but that also means
	// nothing may touch it concurrently with setup.
	nc net.Conn
	// rawConn is the accepted socket, assigned once at construction and never
	// replaced. Teardown paths close THIS rather than nc: the drain runs
	// concurrently with a client that may still be completing its TLS handshake
	// (registration precedes the handshake), so reading nc there would race the
	// upgrade. Closing the underlying socket tears down the TLS connection layered
	// on it just the same, and needs no lock, which keeps the drain from having to
	// wait on a handshake that holds c.mu.
	rawConn net.Conn
	cid     uint64
	kind    int
	ncs     atomic.Value // string label, lazily set

	opts    clientOpts
	flags   clientFlag
	in      struct{ flags readCacheFlag }
	out     outbound
	pa      pubArg
	perms   *permissions
	mperms  *permissions
	subs    map[string]*subscription
	mpay    int32
	msubs   int32
	trace   bool
	headers bool

	start time.Time
	last  time.Time

	ws   *websocket
	mqtt *mqtt

	// nrc is the NATS routing connection for this client: the per-client
	// (auth-passthrough) connection for real MQTT clients, or the service
	// connection for the internal JSA client. All MQTT publishes/subscriptions
	// and JSA requests for this client ride nrc so NATS enforces authorization.
	nrc *nats.Conn
	// ownNRC is true when nrc is a per-client connection this client owns and
	// must close on disconnect (as opposed to the shared service connection).
	ownNRC bool

	wmu        sync.Mutex // serializes writes to nc from parse + delivery goroutines
	authTimer  *time.Timer
	closed     atomic.Bool
	finishOnce sync.Once // releases the clientWG lifecycle token exactly once
	// connAckWritten is set true only after a success CONNACK has been written to
	// the socket without error. The shutdown drain uses it to decide whether to
	// send a server DISCONNECT: a pre-CONNACK connection (whose negotiated v5
	// fields are not yet stable) gets only a socket close.
	connAckWritten atomic.Bool
}

// msgParts splits a NATS message into its header and payload segments (mirrors
// nats-server: client.go).
func (c *client) msgParts(data []byte) (hdr []byte, msg []byte) {
	if c.pa.hdr > 0 {
		return data[:c.pa.hdr], data[c.pa.hdr:]
	}
	return nil, data
}

func (c *client) String() string {
	if v := c.ncs.Load(); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return fmt.Sprintf("cid:%d", c.cid)
}

func (c *client) clog() *slog.Logger {
	if c.srv != nil {
		return c.srv.logger()
	}
	return slog.Default()
}

// fmtLog prefixes a client log line with the client's label (which, after
// CONNECT, includes the client id — see the c.ncs.Store in mqttParse), matching
// nats-server's per-client log prefixing so logs carry client context.
func (c *client) fmtLog(format string, v ...any) string {
	return c.String() + " - " + fmt.Sprintf(format, v...)
}
func (c *client) Errorf(format string, v ...any)   { c.clog().Error(c.fmtLog(format, v...)) }
func (c *client) Warnf(format string, v ...any)    { c.clog().Warn(c.fmtLog(format, v...)) }
func (c *client) Debugf(format string, v ...any)   { c.clog().Debug(c.fmtLog(format, v...)) }
func (c *client) Tracef(format string, v ...any)   { c.clog().Debug(c.fmtLog(format, v...)) }
func (c *client) traceInOp(op string, arg []byte)  {}
func (c *client) traceOutOp(op string, arg []byte) {}

func (c *client) isClosed() bool    { return c.closed.Load() }
func (c *client) isWebsocket() bool { return false }

func (c *client) setAuthTimer(d time.Duration) {
	c.authTimer = time.AfterFunc(d, func() { c.authViolation() })
}
func (c *client) clearAuthTimer() bool {
	if c.authTimer == nil {
		return true
	}
	return c.authTimer.Stop()
}

// Permission checks: permissive in-broker (NATS enforces via auth passthrough).
func (c *client) pubAllowedFullCheck(subject string, fullCheck, hasLock bool) bool { return true }
func (c *client) checkDenySub(subject string) bool                                 { return false }
func (c *client) selectMappedSubject() bool                                        { return false }

// registerWithAccount binds this client to an account (single-account v1).
func (c *client) registerWithAccount(a *Account) { c.mu.Lock(); c.acc = a; c.mu.Unlock() }

// initClient finishes wiring an accepted MQTT client: assigns a unique id and
// sets up the buffering MQTT reader over the socket. The NATS routing connection
// is attached at CONNECT time by isClientAuthorized (auth passthrough dials a
// per-client connection); the service connection is a pre-CONNECT fallback.
func (c *client) initClient() {
	if c.srv != nil {
		c.cid = c.srv.gcid.Add(1)
		if c.nrc == nil {
			c.nrc = c.srv.nrc
		}
	}
	if c.mqtt != nil && c.mqtt.r == nil {
		c.mqtt.r = &mqttReader{reader: c.nc}
	}
}

// readLoop reads from the client socket and feeds the MQTT parser. The
// mqttReader accumulates partial packets across reads.
func (c *client) readLoop(pre []byte) {
	// Release the clientWG lifecycle token exactly once when the read loop exits
	// (idempotent with abortSetup for the setup-failure paths).
	defer c.finish()
	nc := c.nc
	if nc == nil {
		return
	}
	if c.mqtt != nil && c.mqtt.r == nil {
		c.mqtt.r = &mqttReader{reader: nc}
	}
	// disconnect handles an ungraceful drop or parse error: run the MQTT
	// closed-client cleanup (publishes the Will, detaches/persists the session —
	// nil-safe before CONNECT) before tearing down the connection. A graceful
	// DISCONNECT is already handled inside mqttParse, and closeConnection is
	// idempotent, so a double call here is harmless.
	disconnect := func() {
		if c.srv != nil {
			c.srv.mqttHandleClosedClient(c)
		}
		c.closeConnection(ClientClosed)
	}

	if len(pre) > 0 {
		if err := c.mqttParse(pre); err != nil {
			// A parse failure is a protocol violation (e.g. a non-CONNECT first
			// packet); log it at Error so it is visible, as nats-server does. A
			// graceful DISCONNECT or a clean socket close is not a parse error —
			// it surfaces via the Read error below, not here.
			c.Errorf("MQTT parse error: %v", err)
			disconnect()
			return
		}
	}
	buf := make([]byte, 32*1024)
	for {
		n, err := nc.Read(buf)
		if n > 0 {
			if perr := c.mqttParse(buf[:n]); perr != nil {
				c.Errorf("MQTT parse error: %v", perr)
				disconnect()
				return
			}
		}
		if err != nil {
			disconnect()
			return
		}
	}
}

// writeLoop is a no-op: queueOutbound writes synchronously under wmu. It exists
// to satisfy the createMQTTClient bootstrap and to own one grWG token.
func (c *client) writeLoop() {
	if c.srv != nil {
		c.srv.grWG.Done()
	}
}

func (c *client) flushSignal()                           {}
func (c *client) flushClients(d time.Duration) time.Time { return time.Time{} }
func (c *client) addToPCD(cc *client)                    {}
func (c *client) maxConnExceeded()                       { c.closeConnection(WriteError) }
func (c *client) maxPayloadViolation(sz int, max int32) {
	c.Warnf("MQTT maximum payload exceeded: %d > %d", sz, max)
	c.closeConnection(WriteError)
}
func (c *client) authViolation() bool { c.closeConnection(WriteError); return true }

// doTLSServerHandshake upgrades the client socket to TLS (nats-server:
// server.go). It is called from createMQTTClient before the readLoop goroutine
// is started, and every reader of c.nc is ordered after that point, so replacing
// c.nc needs no lock. This holds only because concurrent teardown (the shutdown
// drain, which by design does not take c.mu) closes c.rawConn instead of reading
// c.nc — see the field comment.
//
// A nil cfg is an error rather than a pass-through: the caller only reaches this
// function when TLS is required, and continuing in the clear there would serve
// plaintext on a listener the operator configured for TLS. c.nc is replaced only
// after a successful handshake, so every subsequent read/write goes through TLS
// and the *tls.Conn assertion made by the caller holds.
//
// pinned is accepted for signature compatibility with the upstream call site;
// certificate pinning is not implemented in this broker (validatePinnedCerts is
// a no-op).
func (c *client) doTLSServerHandshake(kind string, cfg *tls.Config, timeout float64, pinned PinnedCertSet) error {
	if cfg == nil {
		err := fmt.Errorf("%s: TLS required but no TLS configuration is set", kind)
		c.Errorf("TLS handshake error: %v", err)
		return err
	}
	tlsConn := tls.Server(c.nc, cfg)
	deadline := time.Now().Add(secondsToDuration(mqttTLSTimeout(timeout)))
	if err := tlsConn.SetDeadline(deadline); err != nil {
		tlsConn.Close()
		c.Errorf("TLS handshake error on %s connection: %v", kind, err)
		return err
	}
	if err := tlsConn.Handshake(); err != nil {
		tlsConn.Close()
		c.Errorf("TLS handshake error on %s connection: %v", kind, err)
		return err
	}
	if err := tlsConn.SetDeadline(time.Time{}); err != nil {
		tlsConn.Close()
		c.Errorf("TLS handshake error on %s connection: %v", kind, err)
		return err
	}
	c.nc = tlsConn
	return nil
}

// writeSocket writes the full buffer to the client socket, serialized against
// concurrent writers (the parse goroutine and NATS delivery goroutines) by wmu.
// A non-zero deadline bounds the write (used by the shutdown drain so a client
// that stopped reading cannot block indefinitely). It returns the write error
// and NEVER tears down the connection, so a caller holding c.mu cannot deadlock
// on closeConnection (which re-locks c.mu). A partial write with no error is
// reported as io.ErrShortWrite.
func (c *client) writeSocket(data []byte, deadline time.Time) error {
	if c.nc == nil {
		return nil
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if !deadline.IsZero() {
		_ = c.nc.SetWriteDeadline(deadline)
	}
	for len(data) > 0 {
		n, err := c.nc.Write(data)
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// writeDirect writes a complete frame under a bounded deadline, bypassing the
// enqueue path. Used by shutdownDisconnect to send the server DISCONNECT without
// risking an unbounded blocked write or full teardown from within the drain.
func (c *client) writeDirect(data []byte, deadline time.Time) error {
	return c.writeSocket(data, deadline)
}

// queueOutbound writes bytes to the client socket. On a write error it closes
// ONLY the socket, not the full connection: this may run with c.mu held (via
// mqttEnqueueDisconnect / mqttEnqueueFittingProto), and closeConnection re-locks
// c.mu. Closing the socket unblocks readLoop, whose disconnect() performs the
// single, ordered teardown (mqttHandleClosedClient before closeConnection). A
// double socket close is harmless.
func (c *client) queueOutbound(data []byte) {
	if err := c.writeSocket(data, time.Time{}); err != nil && c.rawConn != nil {
		_ = c.rawConn.Close()
	}
}
func (c *client) enqueueProto(data []byte) { c.queueOutbound(data) }

// enqueueProtoErr writes data and returns the socket write error, closing only
// the socket on failure (as queueOutbound does). Used by the CONNACK path, which
// records successful delivery and must distinguish a write failure from success.
func (c *client) enqueueProtoErr(data []byte) error {
	err := c.writeSocket(data, time.Time{})
	if err != nil && c.rawConn != nil {
		_ = c.rawConn.Close()
	}
	return err
}

// closeConnection tears down the client connection. It unsubscribes the
// client's NATS routing subscriptions (so they do not leak on a shared service
// connection) and, for auth-passthrough clients, drains the owned per-client
// NATS connection.
func (c *client) closeConnection(reason ClosedState) {
	if c.closed.Swap(true) {
		return
	}
	// Mirror upstream's per-client teardown log (carries the client id via the
	// c.Debugf prefix once CONNECT has set the label).
	if c.mqtt != nil {
		c.Debugf("Client connection closed")
	}
	if c.authTimer != nil {
		c.authTimer.Stop()
	}
	if c.rawConn != nil {
		_ = c.rawConn.Close()
	}
	if c.srv != nil && c.cid != 0 {
		c.srv.mu.Lock()
		delete(c.srv.clients, c.cid)
		c.srv.mu.Unlock()
	}

	// Drop this client's NATS subscriptions. On an owned per-client connection
	// Drain() would suffice, but on the shared service connection each sub must
	// be explicitly unsubscribed or a stale handler keeps consuming/acking the
	// durable consumer's messages.
	c.mu.Lock()
	subs := c.subs
	c.subs = nil
	c.mu.Unlock()
	for _, sub := range subs {
		if ns, ok := sub.nsub.(*nats.Subscription); ok && ns != nil {
			_ = ns.Unsubscribe()
		}
	}

	if c.ownNRC && c.nrc != nil {
		c.nrc.Drain() //nolint:errcheck // best effort on disconnect
	}
}

// finish releases this client's shutdown-drain lifecycle token (clientWG),
// exactly once. It runs from readLoop's defer for a client whose readLoop
// started, and from abortSetup for a setup failure after registration but before
// readLoop start. The token is reserved under s.mu when the client is inserted
// into s.clients (createMQTTClient), so Shutdown's Phase-1 wait cannot race a
// client into existence after it snapshots the set.
func (c *client) finish() {
	c.finishOnce.Do(func() {
		if c.srv != nil {
			c.srv.clientWG.Done()
		}
	})
}

// abortSetup tears down a client that failed setup after registration in
// s.clients but before its readLoop started: closeConnection removes it from
// s.clients and closes the socket, and finish releases the lifecycle token, so
// no path leaks the token, the socket, or a map entry.
func (c *client) abortSetup(reason ClosedState) {
	c.closeConnection(reason)
	c.finish()
}

// shutdownDisconnect is the per-client drain step. For a connection that
// completed a v5 CONNACK it sends a server DISCONNECT (0x8B "server shutting
// down") under a bounded write deadline, then closes the socket; a pre-CONNACK
// or 3.1.1 connection gets only a socket close. It deliberately does NOT run
// MQTT/session/Will cleanup: closing the socket unblocks readLoop, whose
// disconnect() performs that cleanup exactly once in the correct order
// (mqttHandleClosedClient before closeConnection). The DISCONNECT bytes are
// built under c.mu so this cannot race CONNECT parsing mutating the negotiated
// v5 fields.
func (c *client) shutdownDisconnect(deadline time.Time) {
	var pkt []byte
	if c.connAckWritten.Load() {
		c.mu.Lock()
		pkt = c.mqttBuildDisconnect(mqttReasonServerShuttingDown, mqttReasonStrServerShuttingDown)
		c.mu.Unlock()
	}
	if len(pkt) > 0 {
		_ = c.writeDirect(pkt, deadline) // best effort; the socket close follows regardless
	}
	// Close the raw socket, never c.nc: this runs concurrently with a client that
	// may still be upgrading c.nc to TLS. Closing the socket underneath the TLS
	// layer aborts that handshake just as effectively.
	if c.rawConn != nil {
		_ = c.rawConn.Close()
	}
}

// drainClients disconnects every currently-connected client: it snapshots the
// client set under s.mu (releasing the lock before per-client work, since
// closeConnection re-locks s.mu to deregister), then sends each a server
// DISCONNECT and closes its socket. Must run after running has been set false so
// the accept path (createMQTTClient) refuses new CONNECTs and cannot register a
// client after the snapshot.
func (s *Server) drainClients(deadline time.Time) {
	s.mu.Lock()
	conns := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		c.shutdownDisconnect(deadline)
	}
}

// processSub registers interest for subject on the client's NATS routing
// connection. Incoming NATS messages are reframed (headers + payload) and
// dispatched to the subscription's delivery callback (icb), reproducing the
// NATS-server delivery contract the MQTT core expects.
func (c *client) processSub(subject, queue, sid []byte, cb msgHandler, noForward bool) (*subscription, error) {
	// Re-subscribing an existing sid (e.g. MQTT re-SUBSCRIBE to an already
	// subscribed topic, which reuses the same delivery-subject sid) must
	// replace, not add to, the interest: otherwise the old NATS subscription's
	// handler closure — still bound to the old *subscription (stale MQTT
	// metadata like subID/qos/noLocal) — keeps running alongside the new one,
	// leaking a duplicate subscription that races the new one for delivery.
	c.mu.Lock()
	old := c.subs[string(sid)]
	c.mu.Unlock()
	if old != nil {
		if ns, ok := old.nsub.(*nats.Subscription); ok && ns != nil {
			_ = ns.Unsubscribe()
		}
	}

	sub := &subscription{client: c, subject: subject, queue: queue, sid: sid, icb: cb}
	if c.nrc != nil && cb != nil {
		// Per-subscription delivery client carries the pubArg (header length)
		// that msgParts needs. nats.go dispatches a subscription's messages on a
		// single goroutine, so mutating dc.pa here is race-free.
		dc := &client{srv: c.srv, kind: c.kind}
		handler := func(m *nats.Msg) {
			rmsg, hdrLen := frameNATSMsg(m)
			dc.pa.hdr = hdrLen
			cb(sub, dc, sub.client.acc, m.Subject, m.Reply, rmsg)
		}
		var (
			ns  *nats.Subscription
			err error
		)
		if len(queue) > 0 {
			ns, err = c.nrc.QueueSubscribe(string(subject), string(queue), handler)
		} else {
			ns, err = c.nrc.Subscribe(string(subject), handler)
		}
		if err != nil {
			return nil, err
		}
		sub.nsub = ns

		// Interest propagation: QoS 1/2 and PUBREL deliveries ride an internal
		// deliver subject ("$MQTT.sub.<nuid>") that a JetStream push consumer,
		// already created synchronously, will publish to. nats.go's Subscribe only
		// buffers the SUB locally, so without a round-trip the SUBACK can be sent
		// before the NATS server has registered the interest — a message published
		// immediately after the SUBACK then misses the consumer's first delivery
		// attempt and waits out AckWait. Flush here so interest provably exists
		// before the SUBSCRIBE handler emits SUBACK, regardless of which
		// connection (per-client or shared service) carries the subscription.
		if bytes.HasPrefix(subject, []byte(mqttSubPrefix)) {
			if ferr := c.nrc.Flush(); ferr != nil {
				_ = ns.Unsubscribe()
				return nil, ferr
			}
		}

		// Auth passthrough: a per-client connection carries that MQTT client's NATS
		// subscribe permissions. nats.go's async Subscribe does not report a
		// server-side permissions violation inline, but the server answers a denied
		// SUB with an -ERR that the read loop records in LastError before it reads
		// the PONG that Flush waits for. So a Flush round-trip makes a new
		// permission-violation LastError observable synchronously; we surface it as
		// a subscribe error, which the SUBSCRIBE handler turns into a SUBACK failure
		// (0x80). Only per-client connections are permission-scoped; the shared
		// service connection has full rights. Internal delivery subjects ($MQTT.*,
		// e.g. PUBREL delivery) are the bridge's own plumbing, not user topics, so
		// they are exempt from the check (and a user SUBSCRIBE to them is already
		// rejected earlier).
		if c.ownNRC && !bytes.HasPrefix(subject, []byte(mqttPrefix)) {
			before := c.nrc.LastError()
			if ferr := c.nrc.Flush(); ferr != nil {
				_ = ns.Unsubscribe()
				return nil, ferr
			}
			if after := c.nrc.LastError(); after != nil && after != before &&
				errors.Is(after, nats.ErrPermissionViolation) {
				_ = ns.Unsubscribe()
				// Log the full NATS detail, but return a concise error: its text
				// becomes the SUBACK Reason String, and a long one bloats the
				// properties block (some clients mishandle multi-byte property-length
				// varints) and would leak internal backend detail to the client.
				c.Errorf("MQTT: subscribe to %q denied by NATS: %v", string(subject), after)
				return nil, errMQTTNotAuthorizedToSubscribe
			}
		}
	}
	c.mu.Lock()
	if c.subs == nil {
		c.subs = make(map[string]*subscription)
	}
	c.subs[string(sid)] = sub
	c.mu.Unlock()
	return sub, nil
}

// processUnsub removes interest for the given sid and unsubscribes the
// underlying NATS subscription.
func (c *client) processUnsub(sid []byte) error {
	c.mu.Lock()
	sub := c.subs[string(sid)]
	delete(c.subs, string(sid))
	c.mu.Unlock()
	if sub != nil {
		if ns, ok := sub.nsub.(*nats.Subscription); ok && ns != nil {
			return ns.Unsubscribe()
		}
	}
	return nil
}

// processInboundClientMsg publishes an outbound message (framed by the caller in
// c.pa) to the client's NATS routing connection. This is the single seam through
// which both JSA requests (internal client) and MQTT publishes (per-client
// connection) reach NATS.
func (c *client) processInboundClientMsg(msg []byte) (bool, bool) {
	if c.nrc == nil {
		return false, false
	}
	data := msg
	// size is the declared payload length (header+body, excluding the wire's
	// trailing CRLF); it is legitimately 0 for an empty-payload ack, so the
	// trim must apply even then, not just when size > 0 - otherwise a bare ack
	// publishes with the untrimmed trailing "\r\n" still attached, which
	// matches none of nats-server's ack payload cases (processAck in
	// consumer.go) and is silently dropped, never freeing the consumer's
	// ack-pending slot.
	if c.pa.size >= 0 && c.pa.size <= len(msg) {
		data = msg[:c.pa.size]
	}
	m := nats.NewMsg(string(c.pa.subject))
	if len(c.pa.reply) > 0 {
		m.Reply = string(c.pa.reply)
	}
	if c.pa.hdr > 0 && c.pa.hdr <= len(data) {
		m.Header = parseFramedHeader(data[:c.pa.hdr])
		// The ClientInfo header is an internal NATS-server trusted header; it is
		// meaningless (and ignored) for an external JetStream client.
		m.Header.Del(ClientInfoHdr)
		m.Data = data[c.pa.hdr:]
	} else {
		m.Data = data
	}
	if err := c.nrc.PublishMsg(m); err != nil {
		c.Errorf("nats publish to %q failed: %v", m.Subject, err)
		return false, false
	}

	// MQTT retained-message hook. Upstream nats-server invokes this inside
	// processInboundClientMsg (client.go) for MQTT clients once permissions pass;
	// it stores/clears the retained copy in $MQTT_rmsgs. It no-ops for
	// non-retained publishes. The internal JSA client has c.mqtt == nil and is
	// correctly skipped.
	if c.mqtt != nil {
		c.mqttHandlePubRetain()
	}
	return true, false
}

// frameNATSMsg reconstructs the on-wire representation the MQTT delivery
// callbacks expect: a header block (NATS/1.0 framing) followed by the payload.
// It returns the framed bytes and the header length (0 when there are no
// headers, matching msgParts' expectations).
func frameNATSMsg(m *nats.Msg) (rmsg []byte, hdrLen int) {
	if len(m.Header) == 0 {
		return m.Data, 0
	}
	var bb bytes.Buffer
	bb.WriteString(hdrLine)
	for k, vs := range m.Header {
		for _, v := range vs {
			bb.WriteString(k)
			bb.WriteString(": ")
			bb.WriteString(v)
			bb.WriteString(_CRLF_)
		}
	}
	bb.WriteString(_CRLF_)
	hdrLen = bb.Len()
	bb.Write(m.Data)
	return bb.Bytes(), hdrLen
}

// parseFramedHeader parses a NATS/1.0 header block into a nats.Header,
// preserving the EXACT header-key case. MQTT uses case-sensitive NATS headers
// (e.g. "Nmqtt-RFlags"); the stdlib textproto reader would canonicalize them
// (→ "Nmqtt-Rflags") and break the round-trip, so we split lines manually the
// same way nats.go and mqttSliceHeaders do.
func parseFramedHeader(hdr []byte) nats.Header {
	h := nats.Header{}
	// Skip the "NATS/1.0\r\n" status line.
	idx := bytes.Index(hdr, []byte(_CRLF_))
	if idx < 0 {
		return h
	}
	for _, line := range bytes.Split(hdr[idx+2:], []byte(_CRLF_)) {
		if len(line) == 0 {
			continue
		}
		c := bytes.IndexByte(line, ':')
		if c <= 0 {
			continue
		}
		key := string(bytes.TrimRight(line[:c], " \t"))
		val := string(bytes.TrimLeft(line[c+1:], " \t"))
		h[key] = append(h[key], val)
	}
	return h
}

// ClosedState is aliased to nats-server's close-reason type; the reason
// constants (WriteError, ClientClosed, DuplicateClientID) come from the alias
// layer.
type ClosedState = closedStateAlias

// ---------------------------------------------------------------------------
// Listener / TLS helpers (nats-server: util.go, server.go)
// ---------------------------------------------------------------------------

func natsListen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

func tlsVersion(ver uint16) string { return tls.VersionName(ver) }

// tlsHandshakeMQTT is the handshake-kind label passed to doTLSServerHandshake.
const tlsHandshakeMQTT = "mqtt"

// mqttTLSTimeout returns the effective TLS handshake bound in seconds:
// MQTTOpts.TLSTimeout when set, mqttDefaultTLSTimeout otherwise. Resolving at
// the point of use (rather than mutating Options at startup) matches the other
// MQTT option defaults, e.g. mqttReceiveMax.
func mqttTLSTimeout(timeout float64) float64 {
	if timeout <= 0 {
		return mqttDefaultTLSTimeout
	}
	return timeout
}

// tlsMixConn lets a connection replay bytes already read before the TLS upgrade
// (nats-server: server.go).
type tlsMixConn struct {
	net.Conn
	pre *bytes.Buffer
}

func (c *tlsMixConn) Read(b []byte) (int, error) {
	if c.pre != nil {
		n, err := c.pre.Read(b)
		if c.pre.Len() == 0 {
			c.pre = nil
		}
		return n, err
	}
	return c.Conn.Read(b)
}

func validateNoAuthUser(o *Options, noAuthUser string) error { return nil }
func validatePinnedCerts(_ PinnedCertSet) error              { return nil }
