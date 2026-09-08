// Public entrypoint for the standalone MQTT broker: construct a Server bound to
// an external NATS connection and start/stop the MQTT listener.

package mqtt

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// shutdownDrainTimeout bounds how long the convenience Shutdown() blocks waiting
// for the asynchronous shutdown lifecycle to complete. Timing out does not tear
// dependencies down under still-running cleanup; the lifecycle keeps running in
// the background and an orchestrator SIGKILL is the ultimate bound.
const shutdownDrainTimeout = 10 * time.Second

// NewServer builds an MQTT broker server bound to nc (the external NATS/JetStream
// service connection). opts carries the MQTT listener configuration; if nil,
// defaults are used. nodeName must be unique per instance (used for JSA reply
// routing); empty means one is generated.
func NewServer(nc *nats.Conn, opts *Options, nodeName string) *Server {
	if opts == nil {
		opts = &Options{}
	}
	if opts.MaxPayload <= 0 {
		opts.MaxPayload = 1024 * 1024 // 1MB default, as nats-server
	}
	s := &Server{
		nrc:             nc,
		opts:            opts,
		nodeName:        nodeName,
		clients:         make(map[uint64]*client),
		quitCh:          make(chan struct{}),
		shutdownDone:    make(chan struct{}),
		willClaimStopCh: make(chan struct{}),
		willTimers:      make(map[string]*time.Timer),
		// Initialized here so globalAccount() never lazily writes s.gacc: the eager
		// account-manager init and the accept loop reach it concurrently.
		gacc: &Account{name: "$G"},
	}
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	s.running.Store(true)
	return s
}

// Start begins listening for MQTT connections. It returns any listener setup
// error (Fatalf in the shim only logs).
func (s *Server) Start() error {
	// Before the listener, never after it. Under auth passthrough every CONNECT is
	// judged against the account the broker's own streams ride, so a listener
	// opened first would accept connections the broker cannot yet judge -- a
	// window, not a theoretical one, since the identity request is a round trip.
	// See authz_identity.go.
	if s.dialClient != nil {
		if err := s.establishServiceIdentity(); err != nil {
			return fmt.Errorf("MQTT auth passthrough: %w", err)
		}
	}
	s.startMQTT()
	if s.mqtt.listenerErr != nil {
		return s.mqtt.listenerErr
	}
	// Eagerly initialize the (single global) account session manager so every
	// replica runs the delayed-Will claim loop regardless of client traffic —
	// otherwise a surviving replica would not fire a handed-off Will until some
	// client happened to initialize the account. See will_outbox.go.
	if _, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient()); err != nil {
		// Atomic startup contract: tear down the already-open listener and any
		// account-manager goroutines before returning, so a failed Start leaves
		// nothing running for direct callers (not just serveMQTT).
		s.Shutdown()
		return err
	}
	return nil
}

// Addr returns the bound MQTT listener address (useful when Port was 0/-1).
func (s *Server) Addr() net.Addr {
	if s.mqtt.listener == nil {
		return nil
	}
	return s.mqtt.listener.Addr()
}

// Shutdown stops the broker and blocks up to shutdownDrainTimeout for the
// asynchronous shutdown lifecycle to complete. It is the convenience form of
// BeginShutdown/Done: on timeout it returns while cleanup goroutines keep
// running in the background (an orchestrator SIGKILL is the ultimate bound) and
// deliberately does NOT tear dependencies down under them. Callers that require
// the strict session/Will persistence guarantee should wait on the channel
// returned by BeginShutdown (or use ShutdownContext) rather than this bounded
// form.
//
// A completed lifecycle establishes the happens-before the test harness relies
// on: everything the drained clients' readLoops and the background goroutines
// last touched (e.g. package-level tunables read once at goroutine start) is
// ordered before whatever runs after the lifecycle finishes.
func (s *Server) Shutdown() {
	done := s.BeginShutdown()
	select {
	case <-done:
	case <-time.After(shutdownDrainTimeout):
	}
}

// BeginShutdown starts the shutdown lifecycle (idempotent) and returns a channel
// closed once every phase has completed. It marks the server not-running and
// closes the listener synchronously so the accept path refuses new CONNECTs
// immediately, then runs the phased drain in the background.
func (s *Server) BeginShutdown() <-chan struct{} {
	s.shutdownOnce.Do(func() {
		s.running.Store(false)
		if s.mqtt.listener != nil {
			_ = s.mqtt.listener.Close()
		}
		go s.runShutdown()
	})
	return s.shutdownDone
}

// ShutdownContext begins shutdown and waits for full completion or ctx
// cancellation, returning ctx.Err() if the context is done first and nil on
// completion. Unlike Shutdown it lets the caller choose the bound.
func (s *Server) ShutdownContext(ctx context.Context) error {
	done := s.BeginShutdown()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runShutdown executes the four-phase shutdown lifecycle exactly once and closes
// shutdownDone when finished. The phase ordering is load-bearing:
//
//	Phase 1 disconnects clients and waits for their readLoop cleanup while quitCh
//	  is still open, so a draining client can persist its session (and, once
//	  Layer 2 lands, enqueue a delayed Will) through the still-live account
//	  managers.
//	Phase 2 closes the Will dispatch gate and drains both dispatch sources (fire
//	  timers and claim loops) so no publisher worker Add can race Phase 3's Wait.
//	Phase 3 waits for active publisher workers (willWorkerWG) to complete; each
//	  publish is bounded by the JSA request timeout, well under the claim lease.
//	Phase 4 finally stops the JSA/account infrastructure (quitCh) and the
//	  remaining background goroutines (grWG).
//
// Phases 2 and 3 drive the Layer 2 delayed-Will outbox: both dispatch sources
// stop on the gate — the claim loops (willClaimWG) and the per-Will fire timers
// (willTimerWG, armed via scheduleWillTimer) — and publisher workers
// (willWorkerWG, admitted via admitWillWorker) drain before quitCh closes.
func (s *Server) runShutdown() {
	defer close(s.shutdownDone)

	deadline := time.Now().Add(shutdownDrainTimeout)

	// Phase waits are UNBOUNDED so shutdownDone closes only on true completion:
	// the caller (main) tears down the backend when it closes, so a premature
	// close would reintroduce the dependency-teardown race. Only the convenience
	// Shutdown() bounds its own return; a genuinely wedged goroutine is bounded by
	// the orchestrator's process deadline (k8s SIGKILL). To keep a graceful drain
	// from actually depending on SIGKILL, the publish path honors willClaimStopCh
	// (closed in Phase 2) so a worker blocked in the subscribe barrier unblocks
	// before Phase 3 waits on it — see mqttSubsBarrier.wait.

	// Phase 1: disconnect clients and wait for readLoop cleanup while quitCh is open.
	//
	// Ordering note: the connection-setup gate is closed first, but its Wait comes
	// LAST. A setup token spans the whole of createMQTTClient, including the TLS
	// handshake, so waiting on it before draining would deadlock the drain against
	// the very socket it needs to close: a client parked in its handshake is
	// already registered (registration precedes the handshake), and closing its
	// socket is what makes the handshake return. Draining first therefore unblocks
	// setup rather than waiting on it.
	//
	// Nothing can register behind the drain snapshot: registration happens under
	// s.mu behind an isRunning check, and running was set false before this
	// lifecycle started, so a setup goroutine reaching that section after the
	// snapshot closes its connection instead of registering. The final wait is
	// therefore only about not leaving setup goroutines running past shutdown.
	s.closeConnSetupGate()
	s.drainClients(deadline)
	s.clientWG.Wait()
	s.connSetupWG.Wait()

	// Phase 2: stop Will dispatch (claim loops + timers) and unblock the subscribe
	// barrier so in-flight publishers can finish.
	s.closeWillDispatch()
	s.willTimerWG.Wait()
	s.willClaimWG.Wait()

	// Phase 3: wait for publisher workers (and tracked control ops) to complete.
	s.willWorkerWG.Wait()

	// Anything still held for a service account that never came back. Reported
	// rather than published: the destination is still unestablished, and the whole
	// point of the fence is not to publish into an account nobody has identified.
	s.discardOwedImmediateWills()
	s.discardOwedDelayedWills()

	// Phase 4: stop JSA/account infrastructure and remaining background goroutines.
	s.mu.Lock()
	quitCh := s.quitCh
	s.mu.Unlock()
	if quitCh != nil {
		select {
		case <-quitCh:
		default:
			close(quitCh)
		}
	}
	s.grWG.Wait()
}

// admitWillWorker reserves a publisher-worker token under the dispatch gate.
// Returns false if the gate is already closed (shutdown Phase 2 has begun), in
// which case the caller must not dispatch. On true the caller owns one
// willWorkerWG token and must call s.willWorkerWG.Done() when the worker exits.
func (s *Server) admitWillWorker() bool {
	s.willDispatchMu.Lock()
	defer s.willDispatchMu.Unlock()
	if s.willDispatchClosed {
		return false
	}
	s.willWorkerWG.Add(1)
	return true
}

// scheduleWillTimer arms a delayed-Will fire timer, keyed by the Will's outbox
// subject, that shutdown can stop and drain before it waits on publisher
// workers. Returns false if the dispatch gate is already closed (shutdown Phase
// 2 has begun), in which case nothing is armed and the caller must leave the
// durable record for the claim loop or a peer.
//
// Creation, registration and the willTimerWG Add all happen under
// willDispatchMu, giving this and closeWillDispatch a single linearization
// point: shutdown either closes the gate before the timer exists, or finds it in
// willTimers. Taking an already-created *time.Timer instead would be unsound —
// time.AfterFunc arms immediately, so a zero (or already-elapsed) delay could
// fire before registration and escape the drain entirely.
//
// The callback cannot run its body until registration completes: it first blocks
// on the registered channel, which is closed only after the map insert and the
// Add. That is what makes a zero-duration timer safe without depending on
// scheduler timing. The callback then releases its own token exactly once.
//
// Returns false if a timer is already armed for the key, leaving the existing
// one in place. Outbox subjects embed a fresh WillID per Will event, so one key
// is armed at most once; rejecting rather than replacing keeps the token
// accounting trivially correct (a displaced timer has no unambiguous owner for
// its release once its map entry is gone).
func (s *Server) scheduleWillTimer(key string, delay time.Duration, fire func()) bool {
	if delay < 0 {
		delay = 0
	}
	registered := make(chan struct{})

	s.willDispatchMu.Lock()
	defer s.willDispatchMu.Unlock()
	if s.willDispatchClosed {
		return false
	}
	if _, exists := s.willTimers[key]; exists {
		return false
	}
	var t *time.Timer
	t = time.AfterFunc(delay, func() {
		<-registered
		defer s.untrackWillTimer(key, t)
		fire()
	})
	s.willTimers[key] = t
	s.willTimerWG.Add(1)
	close(registered)
	return true
}

// untrackWillTimer removes a fire timer from the tracked set and releases its
// willTimerWG token exactly once. Safe to call whether the timer fired or was
// stopped by closeWillDispatch. The identity check means a callback for a
// displaced timer cannot remove the entry that replaced it.
func (s *Server) untrackWillTimer(key string, t *time.Timer) {
	s.willDispatchMu.Lock()
	cur, ok := s.willTimers[key]
	ok = ok && cur == t
	if ok {
		delete(s.willTimers, key)
	}
	s.willDispatchMu.Unlock()
	if ok {
		s.willTimerWG.Done()
	}
}

// cancelWillTimer stops and releases the fire timer for an outbox subject whose
// record is no longer pending (cancelled by a reconnect or takeover, or already
// claimed). Without this a cancelled Will's timer would survive until its
// original FireAt, which MQTT allows to be ~136 years out — reconnect churn
// would then retain timers and their captured account-manager references for the
// lifetime of the process, even though the durable records are reaped in seconds.
//
// A no-op when no timer is armed for the subject, which is the normal case on a
// peer broker: only the enqueueing broker holds the local timer.
func (s *Server) cancelWillTimer(key string) {
	s.willDispatchMu.Lock()
	t, ok := s.willTimers[key]
	s.willDispatchMu.Unlock()
	if !ok {
		return
	}
	// Same ownership rule as closeWillDispatch: only a Stop() that actually
	// prevented the callback makes us the releaser. The entry must stay in the map
	// until that is known — deleting first would strand the token when the
	// callback had already won, because its untrackWillTimer would find nothing.
	if t.Stop() {
		s.untrackWillTimer(key, t)
	}
}

// willTimerRef identifies one armed fire timer: its outbox subject and the exact
// timer object, so a later release can verify identity rather than just the key.
type willTimerRef struct {
	key string
	t   *time.Timer
}

// snapshotWillTimersWithPrefix captures the fire timers whose outbox subject
// starts with prefix — every armed Will for one client — WITHOUT stopping them.
//
// Pairs with stopWillTimers around a cancellation scan. The capture must happen
// BEFORE the scan: a successful scan only proves no record was pending while it
// ran, and that proof is already stale by the time it returns. A delayed-Will
// enqueue blocked behind the scan can land immediately after, arm its timer, and
// still hold a genuinely pending record — sweeping "everything matching the
// prefix" after the scan would stop that new timer and drop the Will back to the
// 5s scan latency this whole change exists to remove. Capturing first excludes
// anything armed during or after the scan.
func (s *Server) snapshotWillTimersWithPrefix(prefix string) []willTimerRef {
	s.willDispatchMu.Lock()
	defer s.willDispatchMu.Unlock()
	var matched []willTimerRef
	for key, t := range s.willTimers {
		if strings.HasPrefix(key, prefix) {
			matched = append(matched, willTimerRef{key, t})
		}
	}
	return matched
}

// stopWillTimers releases a previously captured set of fire timers.
//
// This is the backstop for a record the cancellation scan never returned: one
// cancelled by a peer and then reaped by a claim loop is already gone when this
// broker scans, so the scan completes having never seen the subject. Timers are
// keyed by exact subject, so without this the origin broker could not name its
// own timer and it would stay armed until FireAt — up to ~136 years out. Records
// the scan did see are already handled per-record; this is idempotent over them.
//
// Only call after a scan that conclusively completed: a transient failure may
// have left records pending, and their timers must survive.
func (s *Server) stopWillTimers(refs []willTimerRef) {
	for _, ref := range refs {
		// Same ownership rule as everywhere else: only a Stop() that actually
		// prevented the callback makes us the releaser.
		if ref.t.Stop() {
			s.untrackWillTimer(ref.key, ref.t)
		}
	}
}

// closeWillDispatch shuts the delayed-Will dispatch gate: after it returns no
// fire-timer or claim-loop dispatch can admit a new publisher worker, the claim
// loops are signaled to stop (willClaimStopCh), and every tracked fire timer is
// stopped and its token released. Called in shutdown Phase 2 before waiting on
// willTimerWG/willClaimWG.
func (s *Server) closeWillDispatch() {
	s.willDispatchMu.Lock()
	if s.willDispatchClosed {
		s.willDispatchMu.Unlock()
		return
	}
	s.willDispatchClosed = true
	close(s.willClaimStopCh)
	timers := make([]willTimerRef, 0, len(s.willTimers))
	for key, t := range s.willTimers {
		timers = append(timers, willTimerRef{key, t})
	}
	s.willDispatchMu.Unlock()

	// Stop each tracked timer. A timer we successfully Stop() will not fire, so
	// its callback will not run untrackWillTimer; release its token here. A timer
	// that already fired (Stop returns false) releases its own token from the
	// callback, so skip it. Nothing can arm behind this: the gate is closed above,
	// so scheduleWillTimer refuses.
	s.stopWillTimers(timers)
}
