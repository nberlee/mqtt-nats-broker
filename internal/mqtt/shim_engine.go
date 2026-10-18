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

type readCacheFlag uint16

const hasMappings readCacheFlag = 1 << iota

func (rf readCacheFlag) isSet(c readCacheFlag) bool { return rf&c != 0 }

// ---------------------------------------------------------------------------
// Permissions. Delivery and persistence ride the full-rights SERVICE connection,
// so NATS cannot police them and the broker does it here, from the identity
// snapshot isClientAuthorized takes at CONNECT (authz_identity.go). NATS still
// enforces the client's own connection: SUBSCRIBE admission, and QoS 0 on a user
// topic. F-079, F-080, F-081.
// ---------------------------------------------------------------------------

// mqttClientOrigin distinguishes a real MQTT client from an internal
// continuation acting on a decision taken earlier and recorded durably.
//
// It carries only the distinction that cannot be derived. Whether a real client
// is governed by an identity at all depends on whether the broker has per-client
// identities, which is the server's business and is read from there
// (mqttAuthzApplies) rather than copied onto every client.
type mqttClientOrigin uint8

const (
	// mqttOriginClient is a real MQTT client. Whether an identity governs it is
	// then a question about the SERVER -- see mqttAuthzApplies -- so this is the
	// zero value and a construction site that stamps nothing gets it. Under auth
	// passthrough that is the strict arm, which is the direction an omission must
	// fail in.
	mqttOriginClient mqttClientOrigin = iota
	// mqttOriginCapturedDecision is an internal continuation -- a replayed Will,
	// or anything else createInternalAccountClient builds. It carries no snapshot
	// and never will; its authority is the decision taken at capture, which the
	// durable record embodies. Stamped POSITIVELY, because inferring it from an
	// absent snapshot would turn every missing snapshot into a grant.
	mqttOriginCapturedDecision
)

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
	// enforced by NATS for SUBSCRIBE admission, and by the broker itself for
	// everything that rides the service connection -- see isClientAuthorized).
	Authorization string
	NoAuthUser    string
	Users         []*User
	Nkeys         []*NkeyUser

	TLSConfig      *tls.Config
	TLSPinnedCerts PinnedCertSet

	MaxTracedMsgLen int

	JetStream      bool
	StreamReplicas int

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
	AllowNonTLS bool

	Username   string
	Password   string
	Token      string
	NoAuthUser string

	AuthTimeout    float64
	TLSTimeout     float64
	TLSConfig      *tls.Config
	TLSPinnedCerts PinnedCertSet

	AckWait               time.Duration
	ConsumerMemoryStorage bool
	ConsumerReplicas      int
	StreamReplicas        int
	JSAPITimeout          time.Duration
	JsDomain              string
	MaxAckPending         uint16

	// MQTT 5 knobs. ReceiveMaximum/TopicAliasMaximum/KeepAliveMaximum are int
	// so ValidateOptions' range checks (which include a "< 0" bound) see the
	// value as configured, with no narrowing conversion in front of them; -1 encodes "disabled" (0 = default) for the first two, as
	// upstream does.
	MaxProtocolVersion byte
	ReceiveMaximum     int
	TopicAliasMaximum  int
	KeepAliveMaximum   int
	// ResponseInformation is the base response topic returned in a CONNACK
	// Response Information property when a v5 client sets Request Response
	// Information = 1. Empty = never sent. Spec5 [3.2.2.3.15].
	ResponseInformation string

	// ExperimentalSharedQoS2 enables the session-affine shared-subscription
	// QoS 2 implementation. It is intentionally not exposed by the production
	// command. With the zero-value default, a shared subscription requesting
	// QoS 2 is granted and persisted at QoS 1; non-shared QoS 2 is unaffected.
	// Persisted state created with this option has no compatibility guarantee.
	ExperimentalSharedQoS2 bool

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

	// svcID is what the broker knows about the account its own streams ride, and
	// how currently it knows it. Established before the listener opens; see
	// authz_identity.go.
	svcID serviceIdentity

	mqtt srvMQTT

	clients      map[uint64]*client
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
	// Wait. willClaimStopCh signals the claim loops to stop; willTimers holds the
	// armed per-Will fire timers (scheduleWillTimer), which closeWillDispatch
	// stops. The timer is the promptness path — it fires a Will at its Will Delay
	// Interval — and the claim loop is the backstop for a peer's records, a crash
	// during the delay, or a lost claim race.
	//
	// Keyed by the Will's outbox subject (unique per Will event) so a cancelled
	// Will can release its timer immediately. Without that identity a cancelled
	// record's timer would live until its original FireAt, and MQTT permits a Will
	// Delay of 0xFFFFFFFF seconds (~136 years), so reconnect churn would retain
	// timers, callbacks and account-manager references for the process lifetime.
	willDispatchMu     sync.Mutex
	willDispatchClosed bool
	// owedWills holds authorized Wills whose destination account is momentarily
	// unavailable, so an ordinary NATS blip defers them instead of destroying them.
	// See will_blip_retry.go.
	owedWills mqttOwedWillQueue

	// owedDelayedWills holds delayed-Will ENQUEUES refused before they had any
	// effect, re-attempted when the authorized account returns. Separate from
	// owedWills because its terminal action is the opposite: it re-enqueues rather
	// than publishing, which is what keeps a delayed Will's acceptance-fact
	// arbitration and its FireAt intact. See will_delayed_retry.go.
	owedDelayedWills mqttDelayedWillQueue
	willTimers       map[string]*time.Timer
	willClaimStopCh  chan struct{}
	willTimerWG      sync.WaitGroup
	willClaimWG      sync.WaitGroup
	willWorkerWG     sync.WaitGroup

	// THE PUBLISH SEND BARRIER (F-098). A QoS 1/2 PUBLISH send section takes a DURABLE
	// action on its exchange's slot and ends it by draining that action, and the drain is
	// a JetStream write. A graceful shutdown that tore JetStream down while a section was
	// open therefore left a live token on the record — and on the next start no reader can
	// tell it from a peer's, because nothing durable says the holder's process is gone.
	// The identifier, its Receive Maximum capacity and the session's whole outbound flow
	// then stay blocked until an operator intervenes.
	//
	// So sections are ADMITTED through a gate and JOINED before the JSA teardown, exactly
	// as publisher workers are. The gate is closed and the wait performed while requests
	// still work, which is what lets each HOLDER confirm its own drain — no shutdown pass
	// clears a token it does not hold, because inferring holder death is precisely what
	// F-098 refuses.
	publishSendMu     sync.Mutex
	publishSendClosed bool
	publishSendWG     sync.WaitGroup
	// owedDrains holds PUBLISH-action drains that did NOT resolve, keyed by the exchange
	// subject and the exact action token, with the captured identity needed to re-attempt
	// them. It is deliberately independent of the entry maps and of the retry timers: an
	// entry can change phase or vanish, and timer admission closes at Phase 2, while the
	// obligation is to a durable record and outlives both.
	owedDrainsMu sync.Mutex
	owedDrains   map[string]mqttOwedDrain
	// owedTerminals holds terminal cleanups refused worker admission because shutdown had
	// begun; they are finished in Phase 3b alongside the owed drains.
	owedTerminalsMu sync.Mutex
	owedTerminals   map[string]*mqttOwedTerminal
	// Packet-derived PUBREC work remains owned even before any durable intent exists.
	owedIntentsMu   sync.Mutex
	owedIntents     map[mqttIntentResolutionKey]*mqttIntentResolution
	owedDisposalsMu sync.Mutex
	owedDisposals   map[*mqttDisposal]struct{}
	// owedActions holds PUBREL-action drains that could not resolve and were refused worker
	// admission because shutdown had begun. Without them the final pass covers only the
	// PUBLISH side, and a PUBREL token survives the shutdown that could have ended it.
	owedActionsMu sync.Mutex
	owedActions   map[string]mqttOwedAction
	// owedRetained holds retained terminal cleanup owners refused worker admission because
	// shutdown had begun; each gets one last attempt in Phase 3b.
	owedRetainedMu sync.Mutex
	owedRetained   map[*mqttRetainedCleanup]*mqttSession

	ldm bool
	// nodeName identifies this bridge instance (see NodeName). It is not a NATS
	// cluster name and must never be presented as one.
	nodeName string

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
	if s.nodeName == _EMPTY_ {
		s.nodeName = "mnb-" + nuid.Next()
	}
	return s.nodeName
}

// JetStreamIsClustered: the standalone broker always runs against external
// JetStream; clustering is a property of that cluster, exposed via
// config/replicas rather than an embedded server.
func (s *Server) JetStreamIsClustered() bool { return s.getOpts().StreamReplicas > 1 }

// isClientAuthorized implements auth passthrough: it dials a per-client NATS
// connection using the CONNECT credentials, then asks that connection WHO IT IS.
//
// The dial alone only proves the credentials authenticated. It says nothing about
// which account they belong to, and nothing about what they may publish or
// receive -- and since delivery and persistence ride the service connection, the
// broker has to know both to enforce anything at all. One $SYS.REQ.USER.INFO
// request supplies them (authz_identity.go), and this is the only place in the
// broker where a per-client identity exists to ask.
//
// Everything here fails CLOSED. An unknown identity is not a weakly-privileged
// one; it is no identity, and the CONNECT is refused. Refusal reaches
// closeConnection, which drains the connection dialled just above, so a refused
// CONNECT leaks nothing.
//
// F-079, F-080, F-081. Note what this does NOT do: the refusal is a bare socket
// close, because authViolation sends no CONNACK. That is F-061's, filed and
// deliberately post-alpha; nothing here makes it worse or better.
func (s *Server) isClientAuthorized(c *client) bool {
	if s.dialClient == nil {
		// No per-client identity exists in this mode, so there is nothing to
		// enforce and nothing to snapshot: the client IS the service credential.
		// createMQTTClient has already stamped mqttOriginServiceIdentity.
		if c.nrc == nil {
			c.nrc = s.nrc
		}
		return true
	}
	// The service account must be known before any client can be compared against
	// it, and it must be known to be CURRENT -- a service connection that is down
	// cannot tell us where a buffered write would land.
	svcAccount, svcOK := s.serviceAccount()
	if !svcOK {
		s.Warnf("MQTT: refusing CONNECT: the service connection's account is not currently established")
		return false
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
	// Owned by THIS function until it is installed on the client, and closed on
	// every path that does not install it. Ordinary teardown only drains a
	// connection the client owns (ownNRC), so a refusal after a successful dial
	// used to leak one -- and DialClient sets MaxReconnects(-1), so each leaked
	// connection reconnects forever. An authenticated client that cannot be
	// identified could accumulate them remotely, one per CONNECT attempt.
	adopted := false
	defer func() {
		if !adopted {
			nc.Close()
		}
	}()
	// Handlers BEFORE the request, so a drop while it is in flight is observed
	// rather than missed. Neither setter replays an event that already happened,
	// which is why IsConnected is re-checked below rather than trusted from here.
	c.armIdentityInvalidation(nc)
	if !nc.IsConnected() {
		s.Debugf("MQTT: per-client NATS connection for %q was already down when identity was requested", cid)
		return false
	}

	id, ierr := mqttFetchIdentity(nc, s.nrc)
	if ierr != nil {
		s.Warnf("MQTT: refusing CONNECT for client %q: %v", cid, ierr)
		return false
	}
	if id.account != svcAccount {
		// The v1 same-account constraint docs/ARCHITECTURE.md states. Until now
		// nothing checked it, so credentials valid in another account
		// authenticated while every durable operation landed in this one's
		// streams. F-080.
		s.Warnf("MQTT: refusing CONNECT for client %q: identity %q is bound to account %q, not the service account %q",
			cid, id.user, id.account, svcAccount)
		return false
	}
	// The window the handler could not cover: a drop between arming and here is
	// reported by neither setter, so the connection is asked directly. After this
	// point the handler owns invalidation.
	if !nc.IsConnected() {
		s.Debugf("MQTT: per-client NATS connection for %q dropped during identity discovery", cid)
		return false
	}

	c.mu.Lock()
	c.nrc = nc
	c.ownNRC = true
	c.perms, c.mperms = id.pub, id.sub
	c.authzSvcAccount = svcAccount
	c.authzValid = true
	c.mu.Unlock()
	adopted = true // the client owns it now, and its teardown drains it
	c.armIdentityExpiry(id.expires)
	return true
}

// armIdentityInvalidation ties the snapshot's life to the connection's.
//
// A per-client connection reconnects indefinitely (backend.DialClient sets
// MaxReconnects(-1)) and RE-AUTHENTICATES each time, possibly under changed
// permissions, so a snapshot that outlived its connection would authorize durable
// writes on behalf of an identity that may no longer hold them.
//
// Invalidation is the primitive and the socket close is only its consequence:
// closing alone merely fails the NEXT read, while a PUBLISH already in hand
// inside mqttParse, or a delivery callback already running, would go on to use the
// snapshot. Every gate consults authzValid, so clearing it stops those too.
//
// Our own teardown Drains the connection, which also fires the closed callback;
// the second rawConn.Close() is harmless and the invalidation is correct anyway,
// since the client is going away.
func (c *client) armIdentityInvalidation(nc *nats.Conn) {
	invalidate := func() {
		c.mu.Lock()
		c.authzValid = false
		c.mu.Unlock()
		if c.rawConn != nil {
			// The socket only. The ordered teardown belongs to readLoop's
			// disconnect(), exactly as for the CONNECT deadline.
			_ = c.rawConn.Close()
		}
	}
	nc.SetDisconnectErrHandler(func(*nats.Conn, error) { invalidate() })
	nc.SetClosedHandler(func(*nats.Conn) { invalidate() })
}

// armIdentityExpiry bounds the snapshot when the server reported a finite
// lifetime for the identity.
//
// Zero is NOT read as "unlimited": nats-server truncates the remaining duration
// to seconds, so zero means both "no expiry" and "under a second remains". A
// nonzero value therefore adds a bound, and zero adds none -- the connection
// lifecycle above is what carries the guarantee in that case, because an identity
// that really expires loses its connection.
func (c *client) armIdentityExpiry(d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.AfterFunc(d, func() {
		c.mu.Lock()
		c.authzValid = false
		c.mu.Unlock()
		if c.rawConn != nil {
			_ = c.rawConn.Close()
		}
	})
	c.mu.Lock()
	c.authzExpiry = t
	c.mu.Unlock()
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
	// mqttOriginCapturedDecision: this client has no identity of its own and never
	// will. Everything it publishes -- a replayed Will above all -- acts on a
	// decision taken while a real client's snapshot was valid and recorded
	// durably, so asking it for a snapshot would refuse every internal
	// continuation. It is stamped positively rather than inferred from the absent
	// snapshot, because inferring it would make every missing snapshot a grant.
	return &client{srv: s, acc: s.globalAccount(), nrc: s.nrc, kind: 0, origin: mqttOriginCapturedDecision}
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
	// outBind is this connection's immutable output identity (F-105). Assigned once at
	// construction and never replaced, which is what lets a durable action captured against
	// it stay bound to THIS connection across a resident reconnect that rebinds the session
	// object. Nil only for a client built outside createMQTTClient, such as a unit fixture,
	// where output is unrestricted exactly as it was before.
	outBind *mqttConnBinding

	// No per-connection record of executed acquisitions: the consume-once fact lives on each
	// acquisition's holder (mqttExecCell, outbound_frame_write.go), so a long-lived connection
	// keeps no execution history per transmission (F-105 integrated gate, round 1).

	cid  uint64
	kind int
	ncs  atomic.Value // string label, lazily set

	opts  clientOpts
	flags clientFlag
	in    struct{ flags readCacheFlag }
	pa    pubArg
	// perms and mperms are the authenticated identity's compiled publish and
	// subscribe permissions, written once under c.mu by isClientAuthorized and
	// read under c.mu by the publication gate and the delivery guards. A nil
	// value means "no permissions configured", i.e. unrestricted -- see
	// mqttSubjectPermissions.
	perms   *mqttSubjectPermissions
	mperms  *mqttSubjectPermissions
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
	// and JSA requests for this client ride nrc so NATS can enforce authorization
	// -- which it does for SUBSCRIBE admission and for QoS 0 on a user topic. A
	// publish denial on nrc is asynchronous and is never observed, and the QoS 1/2,
	// retained and QoS 2 hold stores do not ride nrc at all: they ride the service
	// connection. The broker polices those itself, from the identity snapshot taken
	// on THIS connection at CONNECT (authz_identity.go).
	nrc *nats.Conn
	// ownNRC is true when nrc is a per-client connection this client owns and
	// must close on disconnect (as opposed to the shared service connection).
	ownNRC bool

	wmu sync.Mutex // serializes writes to nc from parse + delivery goroutines
	// connectTimer is the CONNECT deadline (connect_deadline.go), and
	// connectAdmission is the arbitration between its callback and the CONNECT it
	// races. Both are written and read under c.mu -- including the Stop in
	// closeConnection, which is what makes the accesses provably race-free rather
	// than race-free by the field never being written.
	connectTimer     *time.Timer
	connectAdmission mqttConnectAdmission
	// testConnectDeadlinePause, when non-nil, is invoked by THIS connection's
	// deadline callback with c.mu HELD and before the arbitration, so a test can
	// park the callback exactly where it contends with closeConnection's timer
	// stop. Without it the overlap can only be hoped for: time.AfterFunc gives no
	// upper bound on when its goroutine starts, and sync.Mutex grants no ordering
	// to waiters. Instance-scoped, set and read under c.mu. Nil in production.
	// See connectDeadlineExpired.
	testConnectDeadlinePause func()
	closed                   atomic.Bool
	finishOnce               sync.Once // releases the clientWG lifecycle token exactly once
	// connAckWritten is set true only after a success CONNACK has been written to
	// the socket without error, by mqttEnqueueConnAck under c.mu. Two readers:
	// the shutdown drain decides whether to send a server DISCONNECT at all (a
	// pre-CONNACK connection, whose negotiated v5 fields are not yet stable, gets
	// only a socket close), and mqttNotifyTakenOver gates the takeover
	// notification on it — a client must see its CONNACK before any other packet
	// [MQTT-3.2.0-1], and a displaced connection can still be bound before its
	// CONNACK (mqttAbortAcceptedConnect). That second reader loads it under c.mu,
	// which is why the store lives inside the CONNACK write's critical section.
	connAckWritten atomic.Bool

	// origin says on whose authority this client's publications act. Set at
	// construction and never changed; the zero value is the strictest
	// (mqttClientOrigin).
	origin mqttClientOrigin
	// authzValid is the identity snapshot's epoch. It is true only between a
	// successful CONNECT-time identity fetch and the moment the per-client NATS
	// connection stops being usable, and it is what every gate consults --
	// closing the socket is only the consequence, never the primitive, because a
	// PUBLISH already in hand inside mqttParse would otherwise still use the
	// snapshot of an identity that has gone. Written and read under c.mu.
	authzValid bool
	// authzSvcAccount is the service account this snapshot was discovered against.
	// Every gate compares it, so an identity established for one account can never
	// authorize a write into another -- which is the fact the invariant names. It
	// is deliberately the ACCOUNT and not a per-reconnect generation: pinning to an
	// incarnation made an ordinary same-account reconnect strand every live client
	// permanently.
	authzSvcAccount string
	// authzExpiry invalidates the snapshot when the server reported a finite
	// lifetime for the identity. Stopped in closeConnection alongside
	// connectTimer, for the same reason.
	authzExpiry *time.Timer
	// willAuthorized records, at CONNECT and while the snapshot is provably
	// valid, whether this connection's Will may be published -- over its FULL
	// effective-subject set, derived certificate subject included.
	//
	// It is captured rather than re-derived because every site that discharges a
	// Will runs AFTER the connection is gone: the delayed and takeover gates, the
	// uncertainty-parked gate, and the immediate path all execute inside
	// mqttHandleClosedClient, by which time the epoch above is already invalid.
	// Asking there would suppress an authorized Will on every NATS blip.
	willAuthorized bool
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

// mqttAuthzApplies reports whether a per-client identity governs this client's
// publications and deliveries at all.
//
// Two ways it does not, and they are different in kind. An internal continuation
// is stamped mqttOriginCapturedDecision at construction and acts on a decision
// already taken and durably recorded -- that has to be a POSITIVE property,
// because inferring it from an absent snapshot would make every failure to take
// one a grant. Whereas "this broker has no per-client identities at all" is a
// property of the SERVER, not of the client: with no dialer configured, every
// client rides the service connection and IS the service credential, so there is
// nothing per-client to enforce and nothing to snapshot.
//
// Deriving the second from s.dialClient rather than stamping it on each client
// keeps one fact in one place, and it does not weaken anything: where a dialer IS
// configured, no client can become a service identity by omission, so a
// construction site that forgets to stamp anything still lands in the strict arm.
func (c *client) mqttAuthzApplies() bool {
	if c.origin == mqttOriginCapturedDecision {
		return false
	}
	return c.srv != nil && c.srv.dialClient != nil
}

// snapshotUsableLocked reports whether this client's identity snapshot may still
// be acted on.
//
// The flag alone is not enough, for the same reason it was not enough on the
// service side: the disconnect callback is asynchronous, so it can fire DURING
// isClientAuthorized -- clearing the flag and closing the socket -- and the
// installation that follows would then write `valid = true` back over it,
// resurrecting a snapshot whose connection is already gone. Consulting the live
// connection makes that ordering irrelevant: a dead connection is never usable,
// whatever the flag says and whoever wrote it last.
//
// NO FIXTURE DISCRIMINATES THIS, and the reason is worth recording rather than
// leaving for someone to rediscover: in the window it hardens, the same handler
// has already closed the MQTT socket, so the CONNECT fails and no client survives
// to use the resurrected snapshot. An attempt to test it passed identically with
// the liveness term removed -- it was measuring the socket close, not this -- and
// was deleted rather than kept as coverage it does not provide. What can outlive
// that window is the Will decision captured during the CONNECT, and that is sound
// on its own terms: the snapshot it was computed from was genuine, and the
// destination account is checked again at discharge.
//
// c.mu held.
func (c *client) snapshotUsableLocked() bool {
	return c.authzValid && c.nrc != nil && c.nrc.IsConnected()
}

// willPublishAuthorized reports whether this connection's Will was AUTHORIZED --
// a permanent fact about the Will's subjects, decided at CONNECT.
//
// It says nothing about whether the destination is reachable right now; that is
// willDestinationAccount's business, and conflating the two would turn a
// transient outage into a silently discarded Will.
//
// It reads the decision captured at CONNECT rather than asking now, because every
// caller runs on the teardown path: the delayed and takeover gates, the
// uncertainty-parked gate and the immediate path all execute after the connection
// is gone and its snapshot has been invalidated, so asking would refuse every
// Will rather than the denied ones.
//
// Where no identity governs, there was nothing to capture and nothing to refuse.
// Reading the captured flag unconditionally would drop EVERY Will on a broker
// running -auth-passthrough=false, which is what the Will fixtures caught.
//
// c.mu held.
func (c *client) willPublishAuthorized() bool {
	if !c.mqttAuthzApplies() {
		return true
	}
	return c.willAuthorized
}

// willDestinationAccount is the account this connection's Will was authorized
// for, or false when no identity governs it.
//
// It is deliberately SEPARATE from willPublishAuthorized, because folding the two
// into one boolean loses the distinction that matters most on a teardown path: a
// captured DENY is permanent and the Will must never be published, whereas a
// service account that is momentarily unavailable is an UNKNOWN, and answering
// "not authorized" to it would silently destroy an authorized Will on every NATS
// blip -- the caller has already consumed it by then. The callers therefore treat
// the two differently: denied discards, unknown declines to consume.
func (c *client) willDestinationAccount() (string, bool) {
	if !c.mqttAuthzApplies() {
		return _EMPTY_, true
	}
	return c.authzSvcAccount, true
}

// pubAllowedLocked is pubAllowedFullCheck with c.mu held.
//
// Where an identity governs, an absent or invalidated snapshot is not a weaker
// answer than a deny -- it is no answer, and it refuses.
func (c *client) pubAllowedLocked(subject string) bool {
	if !c.mqttAuthzApplies() {
		return true
	}
	if !c.snapshotUsableLocked() {
		return false
	}
	// The SERVICE connection's epoch, not this client's. While it is down nothing
	// can be known about where a durable write would land: nats.go buffers the
	// publish and flushes it on reconnect BEFORE the reconnect callback runs, so a
	// callback that discovers the account changed discovers it after the effect has
	// already been written into the new account. Refusing here is what stops the
	// effect being buffered at all, and it costs little -- with the service
	// connection down a JetStream store would time out anyway, so this is the same
	// outcome reached sooner and said out loud.
	if acc, ok := c.srv.serviceAccount(); !ok || acc != c.authzSvcAccount {
		return false
	}
	return c.perms.allows(subject)
}

// mqttSubDeliveryVerdict is a delivery guard's answer. It is three-valued
// because "the snapshot says no" and "there is no snapshot" call for opposite
// disposals: the first is a fact about the message and is terminal, the second
// is a fact about the broker and must not be written onto the message. Collapsing
// them into a bool satisfies every non-delivery assertion by DESTROYING the
// message instead of withholding it.
type mqttSubDeliveryVerdict uint8

const (
	mqttSubDeliverAllow mqttSubDeliveryVerdict = iota
	// mqttSubDeliverDenied: the snapshot proves the account denies this concrete
	// subject (and queue) for this identity. Terminal -- acknowledge, so a stable
	// deny does not redeliver forever.
	mqttSubDeliverDenied
	// mqttSubDeliverIndeterminate: no valid snapshot, or provenance that cannot be
	// derived. Withhold WITHOUT acknowledging; the message must survive for the
	// client's next connection.
	mqttSubDeliverIndeterminate
)

// checkDenySubVerdict evaluates the DENY side of the identity's subscribe
// permissions for one concrete delivery. Only the deny side: NATS already applied
// the allow side when it accepted the subscription, so re-testing it here would
// withhold deliveries NATS admitted.
//
// queue is the subscription's queue group, and it must be the group the delivery
// actually belongs to -- see the affine guard, whose sub.queue is nil and whose
// group comes from its claim instead.
func (c *client) checkDenySubVerdict(subject, queue string) mqttSubDeliveryVerdict {
	if !c.mqttAuthzApplies() {
		return mqttSubDeliverAllow
	}
	c.mu.Lock()
	valid, mperms, wantAcc := c.snapshotUsableLocked(), c.mperms, c.authzSvcAccount
	c.mu.Unlock()
	if !valid {
		return mqttSubDeliverIndeterminate
	}
	// The SERVICE epoch, and it must be the SAME one the snapshot was taken in.
	// nats.go resends service subscriptions before it queues the reconnect
	// callback, so a delivery can arrive from a freshly connected account while the
	// epoch is still invalid and before the mismatch handler has closed anything --
	// an allowed subject reaching a socket whose identity was established in the
	// previous account. Indeterminate, never a deny: the message must survive.
	if acc, ok := c.srv.serviceAccount(); !ok || acc != wantAcc {
		return mqttSubDeliverIndeterminate
	}
	if mperms.deniesSub(subject, queue) {
		return mqttSubDeliverDenied
	}
	return mqttSubDeliverAllow
}

func (c *client) selectMappedSubject() bool { return false }

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
	// The immutable output identity (F-105), minted HERE because it names the connection by
	// cid and the cid is assigned just above — minting it at the struct literal gave every
	// client the same binding, since cid was still zero there. Before any write: the reader
	// below is the first thing that can lead to one.
	c.outBind = mqttBindConnection(c.cid)
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
// certificate pinning is not implemented in this broker, and ValidateOptions
// refuses a non-empty pin set at Start, so pinned is always empty here.
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

// mqttWriteDeadline bounds EVERY MQTT application write. It restores a bound
// this port dropped rather than inventing a policy: upstream nats-server
// installs opts.WriteDeadline on every flush (server/client.go:1842), CLEARS it
// afterwards (:1844), and closes a client that misses it as
// SlowConsumerWriteDeadline (:1962). The shim replaced upstream's writeLoop and
// outbound buffer with a synchronous write under wmu and a no-op writeLoop, and
// the deadline went with them, leaving every PUBLISH, ack, CONNACK and courtesy
// DISCONNECT able to block its writer for as long as a non-reading client lasts.
// Same value as upstream's DEFAULT_FLUSH_DEADLINE. A var, not a const, only so
// tests can shorten it; there is no operator knob.
var mqttWriteDeadline = 10 * time.Second

// writeSocket writes the full buffer to the client socket, serialized against
// concurrent writers (the parse goroutine and NATS delivery goroutines) by wmu.
// Every write is bounded by mqttWriteDeadline; cap is an optional ABSOLUTE
// earlier bound (the shutdown drain's shared deadline), zero meaning no cap. An
// already-past cap is installed as-is so the write fails at once, which is what
// a spent drain budget does today. It returns the write error and NEVER tears
// down the connection, so a caller holding c.mu cannot deadlock on
// closeConnection (which re-locks c.mu). A partial write with no error is
// reported as io.ErrShortWrite.
//
// The deadline is CLEARED before returning, and that is not tidiness. c.nc may
// be a *tls.Conn, and TLS writes bytes this function never sees: a TLS 1.3
// KeyUpdate response is written from Read (crypto/tls/conn.go). A deadline left
// installed expires through subsequent idle time and hands that write a
// PERMANENT outbound error which no later MQTT write can repair. The clear runs
// under wmu (defers run in reverse order) and on every exit including
// io.ErrShortWrite; its own result is ignored, as upstream's is, because on the
// production transport a concurrent close is what makes it fail and that leaves
// no healthy connection to rescue.
func (c *client) writeSocket(data []byte, cap time.Time) error {
	if c.nc == nil {
		return nil
	}
	if h := mqttTestWriterPreLockHookV.Load(); h != nil {
		(*h)(c)
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	// THE OUTPUT GATE, checked under wmu (F-105). Certification seals the gate and then
	// crosses this lock, so a writer that gets here after the seal declines, and one that
	// got here before it has returned before certification completes. That pairing is what
	// upgrades "no new writes are permitted" into "no write is in progress"; testing the
	// gate outside this lock would establish neither.
	//
	// A sealed gate is not an error to report upward: the connection is being torn down by
	// the caller that sealed it, and the write simply does not happen.
	if c.outBind != nil && !c.outBind.mayWrite() {
		return nil
	}
	d := time.Now().Add(mqttWriteDeadline)
	if !cap.IsZero() && cap.Before(d) {
		d = cap
	}
	_ = c.nc.SetWriteDeadline(d)
	defer func() { _ = c.nc.SetWriteDeadline(time.Time{}) }()
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

// queueOutbound writes bytes to the client socket. On a write error it closes
// ONLY the socket, not the full connection: this may run with c.mu held (via
// mqttEnqueueFittingProto), and closeConnection re-locks
// c.mu. Closing the socket unblocks readLoop, whose disconnect() performs the
// single, ordered teardown (mqttHandleClosedClient before closeConnection). A
// double socket close is harmless. This error path is also the slow-consumer
// disconnect: a client that cannot absorb a write within mqttWriteDeadline loses
// its socket here, which is upstream's SlowConsumerWriteDeadline behaviour
// reached through the port's existing close path rather than a new one.
//
// It takes no cap. The one writer that needs an earlier bound, the shutdown
// drain's DISCONNECT, passes it to writeActionlessFrameBy, which hands it to
// writeSocket the same way.
func (c *client) queueOutbound(data []byte) {
	if err := c.writeSocket(data, time.Time{}); err != nil && c.rawConn != nil {
		_ = c.rawConn.Close()
	}
}
func (c *client) enqueueProto(data []byte) { c.queueOutbound(data) }

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
	// CERTIFY, which closes the transport as its second step (F-105 8a): seal the output gate,
	// close outside the writer lock so a writer blocked in the socket returns, cross the writer
	// barrier, record the certificate. A bare close stops future writes and says nothing about
	// the one in progress. This is never reached holding wmu (TestCloseConnectionNeverRunsUnder
	// WriteLock); a caller that did would deadlock at the barrier.
	if c.outBind != nil {
		mqttCertifyClosed(c, c.outBind, "connection closed")
	} else if c.rawConn != nil {
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
	// Stop the CONNECT deadline here rather than on the CONNECT path: this is the
	// teardown every connection that can have an armed timer reaches, including a
	// malformed or non-CONNECT first packet and a peer hang-up. Timer.Stop does
	// not wait for a running AfterFunc, so stopping under c.mu cannot deadlock
	// against the callback, which takes c.mu itself.
	if c.connectTimer != nil {
		c.connectTimer.Stop()
	}
	// Same reason as the CONNECT deadline: this is the teardown every connection
	// reaches, and a live AfterFunc would otherwise outlast the client it bounds.
	if c.authzExpiry != nil {
		c.authzExpiry.Stop()
	}
	// The snapshot dies with the connection. Anything still in flight that
	// consults it from here on gets the indeterminate answer, not the old one.
	c.authzValid = false
	subs := c.subs
	// The MAP-SWAP teardown case: every subscription leaves c.subs at once, so all of their
	// retained frames are detached under c.mu here and ABANDONED after the unlock. (The other
	// two departure sites are processUnsub, which also abandons, and processSub's same-sid
	// replacement, which TRANSFERS instead — detaching is not releasing.) F-091.
	//
	// Needed for TOTALITY rather than for correctness: mqttHandleClosedClient nils sess.c
	// before this runs, so the holds this connection owned are already inert by the shared
	// predicate. Including it is cheaper to defend than a case analysis about which site may
	// skip it.
	var retained []mqttRetainedFrame
	for _, sub := range subs {
		retained = append(retained, c.detachRetainedFramesLocked(sub)...)
	}
	c.subs = nil
	c.mu.Unlock()
	c.releaseDetachedRetainedFrames(retained)
	for _, sub := range subs {
		if ns, ok := sub.nsub.(*nats.Subscription); ok && ns != nil {
			_ = ns.Unsubscribe()
		}
	}

	if c.ownNRC && c.nrc != nil {
		_ = c.nrc.Drain() // best effort on disconnect
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
// completed a v5 CONNACK and still holds its one DISCONNECT claim, it ATTEMPTS a
// server DISCONNECT (0x8B "server shutting down") under a bounded write deadline,
// then closes the socket; a pre-CONNACK or 3.1.1 connection, and one a displacement
// has already notified, get only a socket close. It deliberately does NOT run
// MQTT/session/Will cleanup: closing the socket unblocks readLoop, whose
// disconnect() performs that cleanup exactly once in the correct order
// (mqttHandleClosedClient before closeConnection). The DISCONNECT bytes are
// built under c.mu so this cannot race CONNECT parsing mutating the negotiated
// v5 fields. It is the one server-DISCONNECT writer that supplies its own cap to
// the enqueue path, so it claims the connection's one notification itself
// (mqttClaimDisconnectLocked): a displacement that already notified this client
// must not be followed by a 0x8B [MQTT-3.14.4-1]. What the displacement said —
// 0x8E where it held the successor, 0x89 where it could only show the record is
// not ours (F-084) — does not change the rule; spending the claim does.
//
// deadline is the drain's shared absolute budget, passed as a cap so the write
// is bounded by whichever of it and mqttWriteDeadline is sooner. It does NOT
// bound this function's own c.mu and wmu waits: an incumbent writer may already
// hold a deadline later than the remaining budget. Since shutdownDrainTimeout
// equals mqttWriteDeadline the cap is usually the drain deadline itself, so what
// this removes is the stalled-write drain OBSTRUCTION, not the drain's total
// duration — a later client in the serial walk can still meet an already-past
// cap and get no courtesy DISCONNECT. Bounding the drain end to end needs a real
// budget and is separate work.
func (c *client) shutdownDisconnect(deadline time.Time) {
	var pkt []byte
	if c.connAckWritten.Load() {
		c.mu.Lock()
		// Build before claiming: a nil build (3.1.1, or a client Maximum Packet
		// Size too small for even the short form) sends nothing, so it must not
		// spend the claim. Contenders may build; only a claimant writes. Unlike
		// the enqueue path, the claim here is a RESERVATION taken before the
		// write: it is consumed inside this critical section and the write runs
		// after the unlock, so a competing 0x8E can be suppressed by a 0x8B that
		// is then never written. Stated as a residual rather than fixed — closing
		// it would mean holding c.mu across a deadline-bounded socket write.
		if built := c.mqttBuildDisconnect(mqttReasonServerShuttingDown, mqttReasonStrServerShuttingDown); built != nil &&
			c.mqttClaimDisconnectLocked() {
			pkt = built
		}
		c.mu.Unlock()
	}
	if len(pkt) > 0 {
		// The cap is the MINIMUM of the drain's shared budget and this frame's own
		// write interval, and it is computed HERE rather than inside the writer.
		// That placement is the point: writeSocket's now+mqttWriteDeadline is
		// evaluated after wmu is acquired, so leaving the minimum to it would hand a
		// shutdown write that had waited out a contending writer a FRESH full
		// interval instead of the one it started with. Computing it before entering
		// makes the wmu wait consume the cap, which is what a drain budget means.
		// An already-spent budget stays in the past and fails the write at once.
		cap := time.Now().Add(mqttWriteDeadline)
		if !deadline.IsZero() && deadline.Before(cap) {
			cap = deadline
		}
		// Best effort; the socket close follows regardless. The actionless boundary's
		// close-on-error is harmless here for the same reason, and a refusal writes nothing.
		_ = c.writeActionlessFrameBy(pkt, cap)
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
//
// The second return value carries the buffered retained frames a DISPLACED same-sid
// subscription was still holding, and the caller owes them a home: they are TRANSFERRED to
// the replacement rather than released, because they are still owed to the client.
//
// Releasing them here would be wrong twice over. It would DISCARD a retained replay a
// successful duplicate-filter SUBSCRIBE still owes — the same filter twice, the first entry
// asking for the replay — and it would need `sess.mu`, which this generic function must not
// take: `mqttProcessSubs` holds both `as.mu` and `sess.mu` across `sess.processSub`, which
// reaches this call, and a Go mutex is not reentrant, so taking it here self-deadlocks the
// read loop while it holds the account lock. Both were F-091 holistic-review findings.
func (c *client) processSub(subject, queue, sid []byte, cb msgHandler, noForward bool) (*subscription, mqttDisplacedRetained, error) {
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
	// Internal plumbing rides the service connection, user topics ride the
	// client's own. The broker creates several subscriptions on behalf of a
	// client that are not user topics at all -- the QoS 1/2 deliver subject
	// ($MQTT.sub.<nuid>), PUBREL delivery ($MQTT.deliver.pubrel.<hash>) and the
	// shared-subscription deliver subject ($MQTT.qsub.<key>). Creating those on
	// the permission-scoped per-client connection had two costs: the client's
	// NATS identity could read them directly, bypassing MQTT entirely; and it
	// made "deny subscribe: [$MQTT.>]" undeployable, since NATS evaluates deny
	// before allow so the three could not be carved back out. Putting them on the
	// service connection closes the namespace to client credentials and lets the
	// account deny the whole prefix.
	//
	// Routing is unchanged: sub.client is still c and the handler closes over
	// sub, so deliveries reach the right MQTT client, and teardown still runs
	// through c.subs (see closeConnection, which unsubscribes every stored nsub
	// rather than relying on the connection closing).
	nrc := c.nrc
	if bytes.HasPrefix(subject, []byte(mqttPrefix)) && c.srv != nil && c.srv.nrc != nil {
		nrc = c.srv.nrc
	}
	if nrc != nil && cb != nil {
		// Per-subscription delivery client carries the pubArg (header length)
		// that msgParts needs. nats.go dispatches a subscription's messages on a
		// single goroutine, so mutating dc.pa here is race-free.
		// Inherits the logical client's origin so it is never an authority in its
		// own right; every authorization decision below consults sub.client, not dc.
		dc := &client{srv: c.srv, kind: c.kind, origin: c.origin}
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
			ns, err = nrc.QueueSubscribe(string(subject), string(queue), handler)
		} else {
			ns, err = nrc.Subscribe(string(subject), handler)
		}
		if err != nil {
			return nil, mqttDisplacedRetained{}, err
		}
		sub.nsub = ns

		// Interest propagation: QoS 1/2 and PUBREL deliveries ride an internal
		// deliver subject ("$MQTT.sub.<nuid>") that a JetStream push consumer,
		// already created synchronously, will publish to. nats.go's Subscribe only
		// buffers the SUB locally, so without a round-trip the SUBACK can be sent
		// before the NATS server has registered the interest — a message published
		// immediately after the SUBACK then misses the consumer's first delivery
		// attempt and waits out AckWait. Flush here so interest provably exists
		// before the SUBSCRIBE handler emits SUBACK. Flush the connection the
		// subscription was actually created on (for a $MQTT. subject that is the
		// service connection), or the round-trip proves nothing.
		if bytes.HasPrefix(subject, []byte(mqttSubPrefix)) {
			if ferr := nrc.Flush(); ferr != nil {
				_ = ns.Unsubscribe()
				return nil, mqttDisplacedRetained{}, ferr
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
		// service connection has full rights.
		//
		// The $MQTT. exclusion is structural rather than an exemption: such a
		// subject was just routed to the service connection above, so it is never
		// on a permission-scoped connection and there is nothing to check. It also
		// means the account may safely deny subscribe on "$MQTT.>" for client
		// identities -- the broker's own plumbing does not go through them.
		if c.ownNRC && !bytes.HasPrefix(subject, []byte(mqttPrefix)) {
			before := c.nrc.LastError()
			if ferr := c.nrc.Flush(); ferr != nil {
				_ = ns.Unsubscribe()
				return nil, mqttDisplacedRetained{}, ferr
			}
			if after := c.nrc.LastError(); after != nil && after != before &&
				errors.Is(after, nats.ErrPermissionViolation) {
				_ = ns.Unsubscribe()
				// Log the full NATS detail, but return a concise error: its text
				// becomes the SUBACK Reason String, and a long one bloats the
				// properties block (some clients mishandle multi-byte property-length
				// varints) and would leak internal backend detail to the client.
				c.Errorf("MQTT: subscribe to %q denied by NATS: %v", string(subject), after)
				return nil, mqttDisplacedRetained{}, errMQTTNotAuthorizedToSubscribe
			}
		}
	}
	c.mu.Lock()
	if c.subs == nil {
		c.subs = make(map[string]*subscription)
	}
	// The DISPLACED entry is looked up here, in the same critical section that overwrites
	// it, not from the unlocked lookup at the head of this function: that one is only
	// deciding whether to unsubscribe the old NATS interest, and a frame could be buffered
	// onto the old subscription after it.
	//
	// A subscription leaving c.subs must not take an owed retained replay with it (F-091).
	// Its frames are MOVED to the replacement, which is the entry every later flush and
	// every later teardown will find, so the replay is still delivered on a successful
	// packet and the reservation is still released on a failed one — by the ordinary
	// written and abandoned paths, with no special case here.
	displaced := c.detachDisplacedRetainedLocked(c.subs[string(sid)])
	c.subs[string(sid)] = sub
	c.mu.Unlock()
	return sub, displaced, nil
}

// processUnsub removes interest for the given sid and unsubscribes the
// underlying NATS subscription.
func (c *client) processUnsub(sid []byte) error {
	c.mu.Lock()
	sub := c.subs[string(sid)]
	// This departure is a genuine teardown, so its retained frames are ABANDONED: nothing will
	// ever write them, and the identifier would otherwise stay reserved against reallocation
	// for the session's life. (A same-sid replacement in processSub detaches too, but
	// TRANSFERS instead — see detachRetainedFramesLocked.) F-091.
	retained := c.detachRetainedFramesLocked(sub)
	delete(c.subs, string(sid))
	c.mu.Unlock()
	c.releaseDetachedRetainedFrames(retained)
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
//
// It does not touch the retained stream: an MQTT publication's retained update is
// mqttInitiateMsgDeliveryJSA's, and runs whether or not this send succeeded. F-060.
func (c *client) processInboundClientMsg(msg []byte) (bool, bool) {
	if c.nrc == nil {
		return false, false
	}
	// Test seam: a live send that fails for an MQTT publisher only. A natural failure
	// (a closed per-client connection, an oversized payload) also breaks the client or
	// the retained store, so it cannot show the retained update being attempted
	// independently. Scoped to one subject so parallel tests are unaffected, and the
	// internal JSA client (c.mqtt == nil) never is. Nil in production.
	if h := mqttTestFanOutFailV.Load(); h != nil && c.mqtt != nil && (*h)(string(c.pa.subject)) {
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
	return true, false
}

// mqttTestFanOutFailV makes processInboundClientMsg report a failed live send for an
// MQTT publisher on the subjects it selects. See there. Nil in production.
var mqttTestFanOutFailV atomic.Pointer[func(subject string) bool]

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
