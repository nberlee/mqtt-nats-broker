// Startup retries: Start keeps the eager account-manager initialization going through a
// JetStream that is not answering yet, within ONE server run, instead of exiting on the
// first unanswered request (issue #177).
//
// Why within one run: the per-run ephemeral identity (ephemeral_identity.go) is what keeps
// the retained observer and the session-record watch at one consumer per purpose however
// many attempts a run makes. A fresh Server per attempt would mint a fresh identity and
// strand every earlier attempt's consumers until InactiveThreshold.
//
// Why without a budget: in embedded mode the process IS a JetStream peer, so an exit
// restarts the local recovery the initialization is waiting for; and no finite default can
// tell slow recovery from permanent unavailability. Only a refusal ends startup, or a
// signal. What counts as "not answered yet" is mqttStartupRetryable, and nothing else.
package mqtt

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nberlee/mqtt-nats-broker/internal/operational"
)

var (
	// errMQTTJSARequestTimeout is what every JSA request wait that ran out of time matches
	// with errors.Is. The request was queued, so its outcome is ambiguous; the timeout's
	// own message is unchanged (mqttJSATimeoutError).
	errMQTTJSARequestTimeout = errors.New("JetStream API request timed out")

	// ErrStartupCancelled ends an initialization attempt whose startup was cancelled:
	// Start's context ended, or Start met a refusal and is tearing the server down. It is
	// exported because the command reads it, with its signal context done, as a requested
	// stop rather than a failure: the backoff wraps context.Cause, which need not be
	// context.Canceled.
	ErrStartupCancelled = errors.New("MQTT startup cancelled")

	// errMQTTOwnEphemeralStray is the retained observer's CREATE refused because THIS run's
	// earlier attempt still holds the name: its CREATE was accepted and applied after this
	// attempt's definite delete. Retryable, because the next attempt's definite delete
	// removes it. See mqttResolveObserverOccupant.
	errMQTTOwnEphemeralStray = errors.New("an earlier initialization attempt of this server run still holds the retained observer's name")

	// errMQTTObserverOccupantGone is the retained observer's CREATE refused for an
	// occupant that no longer holds the name when looked up. Retryable: the next attempt
	// deletes definitely and creates again.
	errMQTTObserverOccupantGone = errors.New("the consumer that refused the retained observer's create no longer holds its name")
)

// mqttJSATimeoutError is a JSA request wait that ran out of time. Its message is the
// wait's own; it matches errMQTTJSARequestTimeout.
type mqttJSATimeoutError struct {
	cause error
}

func (e *mqttJSATimeoutError) Error() string { return e.cause.Error() }

func (e *mqttJSATimeoutError) Unwrap() []error {
	return []error{errMQTTJSARequestTimeout, e.cause}
}

// requestTimeout builds the error for a wait that ran out of time.
func (jsa *mqttJSA) requestTimeout(cause error) error {
	return jsa.timeoutError(&mqttJSATimeoutError{cause: cause})
}

// startupAbandonCh is the startup cancellation this assistant's waits observe, or nil (a
// receive on nil never fires) once Start disarmed it or for an assistant built outside
// Start's window.
func (jsa *mqttJSACore) startupAbandonCh() <-chan struct{} {
	if p := jsa.startupAbandon.Load(); p != nil {
		return *p
	}
	return nil
}

// mqttStartupWindow is Start's retry window: the startup cancellation every
// initialization attempt inside it observes, Start's own or a CONNECT's, and the terminal
// failure any of those attempts met — a permission violation, or a request the connection
// definitely refused to publish.
//
// One cancellation for every attempt, because a manager can only be CREATED inside the
// window: Start opens it before the listener accepts anything, a successful Start leaves a
// manager installed, and a failed one leaves a server that refuses to create one.
type mqttStartupWindow struct {
	mu      sync.Mutex
	abandon chan struct{} // nil outside the window
	closed  bool
	// failure is the first terminal failure an attempt inside the window met. It ends
	// startup whichever caller drove that attempt.
	failure error
}

// startupAbandonCh returns the window's cancellation, or nil outside the window.
func (s *Server) startupAbandonCh() chan struct{} {
	w := &s.mqtt.startup
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.abandon
}

// openStartupWindow arms the window for one Start, before its listener opens, and closes
// its cancellation when ctx ends. The returned function stops that watch and is called
// once on every return from Start.
func (s *Server) openStartupWindow(ctx context.Context) (stopWatch func()) {
	w := &s.mqtt.startup
	w.mu.Lock()
	w.abandon = make(chan struct{})
	w.closed = false
	w.failure = nil
	w.mu.Unlock()

	stop := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		select {
		case <-ctx.Done():
			s.cancelStartup()
		case <-stop:
		}
	}()
	return func() {
		close(stop)
		<-watched
	}
}

// cancelStartup closes the window's cancellation, at most once.
func (s *Server) cancelStartup() {
	w := &s.mqtt.startup
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.abandon != nil && !w.closed {
		close(w.abandon)
		w.closed = true
	}
}

// failStartup records err as the window's terminal failure, if none is yet, and cancels
// startup so every waiting attempt — and Start's backoff — ends now.
func (s *Server) failStartup(err error) {
	w := &s.mqtt.startup
	w.mu.Lock()
	inWindow := w.abandon != nil
	if inWindow && w.failure == nil {
		w.failure = err
	}
	w.mu.Unlock()
	if inWindow {
		s.cancelStartup()
	}
}

// startupFailure is the window's recorded terminal failure, or nil.
func (s *Server) startupFailure() error {
	w := &s.mqtt.startup
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failure
}

// closeStartupWindow ends the window and disarms the installed managers' assistants: no
// request that begins its wait afterwards can observe startup cancellation. A request
// already waiting holds the channel it loaded and may still report it once.
func (s *Server) closeStartupWindow() {
	w := &s.mqtt.startup
	w.mu.Lock()
	w.abandon = nil
	w.closed = false
	w.failure = nil
	w.mu.Unlock()

	sm := &s.mqtt.sessmgr
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, asm := range sm.sessions {
		asm.jsa.startupAbandon.Store(nil)
	}
}

// startupPermissionViolation reports a permission violation the service connection
// recorded since this manager's initialization attempt began, or nil.
//
// A POLICY, not an attribution: the service connection is shared, so its LastError cannot
// say which request was denied, and a violation a later error replaced before this check
// is missed — that request then keeps timing out, and the backend's asynchronous error
// handler has already logged the violation as it happened.
func (as *mqttAccountSessionManager) startupPermissionViolation() error {
	return mqttPermissionViolationSince(as.jsa.c.srv.serviceLastError(), as.startupBaseline)
}

// startupPermissionViolationSince is the verdict on a FAILED attempt inside the window that
// began with the service connection at baseline: err itself when it is a permission
// violation, a violation recorded since baseline, or nil. Outside the window, nil.
func (s *Server) startupPermissionViolationSince(baseline, err error) error {
	if s.startupAbandonCh() == nil {
		return nil
	}
	if errors.Is(err, nats.ErrPermissionViolation) {
		return err
	}
	return mqttPermissionViolationSince(s.serviceLastError(), baseline)
}

func mqttPermissionViolationSince(current, baseline error) error {
	if current != nil && current != baseline && errors.Is(current, nats.ErrPermissionViolation) {
		return fmt.Errorf("the NATS service connection was denied during MQTT startup: %w", current)
	}
	return nil
}

func (s *Server) serviceLastError() error {
	if s.nrc == nil {
		return nil
	}
	return s.nrc.LastError()
}

// mqttAbandoned reports whether ch is closed; a nil ch never is.
func mqttAbandoned(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// mqttStartupRetryable reports whether an initialization attempt's error means JetStream
// did not answer yet: a request with no answer, or an asset with no serving leader. Every
// other error is a refusal and ends startup.
func mqttStartupRetryable(err error) bool {
	if err == nil || errors.Is(err, ErrStartupCancelled) {
		return false
	}
	// A request with no answer in time. A request the connection DEFINITELY refused to
	// publish ends in a timeout too, but the sender has already ended startup with it
	// through the window (failStartup), which Start consults before this; any other publish
	// error is as ambiguous as the timeout it ends in.
	if errors.Is(err, errMQTTJSARequestTimeout) {
		return true
	}
	if errors.Is(err, errMQTTOwnEphemeralStray) || errors.Is(err, errMQTTObserverOccupantGone) {
		return true
	}
	// A subscription's flush during initialization (processSub, and the reply
	// subscription's barrier) that got no PONG in time, or ran during a reconnect.
	if errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrConnectionReconnecting) {
		return true
	}
	if ae := jsAPIError(err); ae != nil {
		// No responders: the transport status mqttJSAReplyStatus reports, with no
		// JetStream error code. A nonzero code is JetStream's own answer and is judged by
		// it, never by its HTTP-style status.
		if ae.ErrCode == 0 {
			return ae.Code == 503
		}
		// Not 10194/10195 (offline with a reason): the server also uses those for assets
		// it does not support.
		return IsNatsErr(ae, JSClusterNotAvailErr, JSStreamOfflineErr, JSConsumerOfflineErr)
	}
	return false
}

// mqttDefiniteLocalRefusal reports a publish error nats.go returns before writing anything.
func mqttDefiniteLocalRefusal(err error) bool {
	return errors.Is(err, nats.ErrMaxPayload) || errors.Is(err, nats.ErrBadSubject) ||
		errors.Is(err, nats.ErrHeadersNotSupported)
}

// The startup backoff and the cadence of its log line.
const (
	mqttStartupBackoffInitial = time.Second
	mqttStartupBackoffMax     = 15 * time.Second
	mqttStartupLogEvery       = 10 * time.Second
)

// Test seams for Start's retry loop. nil in production.
var (
	// mqttTestStartupBackoffHookV runs as Start enters a backoff wait, with the attempt
	// count and the attempt's error.
	mqttTestStartupBackoffHookV atomic.Pointer[func(attempt int, err error)]
	// mqttTestStartupTerminalHookV runs after an attempt returned a terminal error and
	// released the manager lock, before Start handles it.
	mqttTestStartupTerminalHookV atomic.Pointer[func(err error)]
	// mqttTestStartupBackoffV replaces the first backoff, so a test does not wait seconds.
	mqttTestStartupBackoffV atomic.Pointer[time.Duration]
	// mqttTestStartupAttemptHookV runs when an initialization attempt has built its JSA
	// assistant, before its first request, so a test can arm that assistant's own
	// per-request seams (testDropRequest, testObserveRequest).
	mqttTestStartupAttemptHookV atomic.Pointer[func(jsa *mqttJSA)]
	// mqttTestServiceSubscriptionHookV runs before each of an initialization attempt's own
	// service subscriptions, with its subject.
	mqttTestServiceSubscriptionHookV atomic.Pointer[func(subject string)]
	// mqttTestStartupBeforeEagerHookV runs in Start after the listener opened and before
	// the eager initialization begins.
	mqttTestStartupBeforeEagerHookV atomic.Pointer[func()]
)

// initializeWithRetries runs the eager account-manager initialization until it succeeds,
// fails with a refusal, or ctx ends. It returns the terminal error, nil on success. Start
// has opened the window (openStartupWindow) before its listener.
//
// On success the window closes and the installed manager is disarmed. On failure the
// window is LEFT open, its cancellation already closed by whatever ended startup — the
// refusing attempt (failStartup, before it released the manager lock) or ctx's watcher, on
// the one channel every attempt loaded — so an attempt that begins after this returns,
// a CONNECT that takes the manager lock before the caller's shutdown refuses creation,
// abandons at once.
func (s *Server) initializeWithRetries(ctx context.Context) (err error) {
	defer func() {
		if err == nil {
			s.closeStartupWindow()
		}
	}()
	abandon := s.startupAbandonCh()

	backoff := mqttStartupBackoffInitial
	if p := mqttTestStartupBackoffV.Load(); p != nil {
		backoff = *p
	}
	var lastLogged time.Time
	for attempt := 1; ; attempt++ {
		// A CONNECT's attempt may already have ended startup — before the eager
		// initialization began, or during the backoff. No attempt begins after that.
		if perr := s.startupFailure(); perr != nil {
			return perr
		}
		_, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
		if err == nil {
			// The attempt that installed the manager — this one, or a CONNECT's during the
			// backoff — passed its own permission verdict before installing it.
			return nil
		}
		if h := mqttTestStartupTerminalHookV.Load(); h != nil && !mqttStartupRetryable(err) {
			(*h)(err)
		}
		// A terminal failure any attempt in the window met, a CONNECT's included, or a
		// refusal a failed attempt's sender met after its waiter had given up.
		if perr := s.startupFailure(); perr != nil {
			return perr
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w (last attempt: %v)", ErrStartupCancelled, ctx.Err(), err)
		}
		if !mqttStartupRetryable(err) {
			return err
		}
		if now := time.Now(); lastLogged.IsZero() || now.Sub(lastLogged) >= mqttStartupLogEvery {
			operational.MQTTInitializationRetry.Log(s.logger(), operational.Int(int64(attempt)), operational.Err(err))
			lastLogged = now
		}
		if h := mqttTestStartupBackoffHookV.Load(); h != nil {
			(*h)(attempt, err)
		}
		t := time.NewTimer(backoff)
		select {
		case <-abandon:
			// ctx ended, or an attempt during the backoff met a terminal failure.
			t.Stop()
			if perr := s.startupFailure(); perr != nil {
				return perr
			}
			return fmt.Errorf("%w: %w (last attempt: %v)", ErrStartupCancelled, context.Cause(ctx), err)
		case <-t.C:
		}
		backoff = min(backoff*2, mqttStartupBackoffMax)
	}
}
