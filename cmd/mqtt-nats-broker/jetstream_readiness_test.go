package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
)

// The JetStream observation behind /readyz (jetstream_readiness.go): what a round stores, when a
// stored success stops counting, what the records report, and what a signal leaves behind.

const (
	probeFailedMsg   = "JetStream readiness probe failed"
	probeAnsweredMsg = "JetStream readiness probe succeeded again"
)

// testReadiness is an observation with a settable clock and reconnect count, recording its log.
type testReadiness struct {
	*jetStreamReadiness
	rec        *recordingHandler
	clock      atomic.Pointer[time.Time]
	reconnectN atomic.Uint64
}

func newTestReadiness(probe func() error) *testReadiness {
	tr := &testReadiness{rec: &recordingHandler{}}
	start := time.Unix(1_000_000, 0)
	tr.clock.Store(&start)
	tr.jetStreamReadiness = &jetStreamReadiness{
		probe:      probe,
		reconnects: tr.reconnectN.Load,
		now:        func() time.Time { return *tr.clock.Load() },
		interval:   time.Millisecond,
		stale:      10 * time.Second,
		log:        slog.New(tr.rec),
		done:       make(chan struct{}),
	}
	return tr
}

func (tr *testReadiness) advance(d time.Duration) {
	next := tr.clock.Load().Add(d)
	tr.clock.Store(&next)
}

func (tr *testReadiness) count(msg string) int {
	tr.rec.mu.Lock()
	defer tr.rec.mu.Unlock()
	n := 0
	for _, r := range tr.rec.records {
		if r.Message == msg {
			n++
		}
	}
	return n
}

func (tr *testReadiness) stored() bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return !tr.okAt.IsZero()
}

var errAccountInfoUnanswered = &mqtt.ReadinessProbeError{Check: mqtt.ReadinessCheckAccountInfo, Err: errors.New("timeout")}

func TestJetStreamReadinessIsUnknownUntilARoundSucceeds(t *testing.T) {
	tr := newTestReadiness(nil)
	if tr.answering() {
		t.Fatal("answering before any round completed")
	}
	tr.commit(context.Background(), tr.now(), 0, nil)
	if !tr.answering() {
		t.Fatal("not answering after a successful round")
	}
}

// TestAFailedRoundWithdrawsAtOnce: a failure clears the success; it is not left to age out.
func TestAFailedRoundWithdrawsAtOnce(t *testing.T) {
	tr := newTestReadiness(nil)
	tr.commit(context.Background(), tr.now(), 0, nil)
	tr.commit(context.Background(), tr.now(), 0, errAccountInfoUnanswered)
	if tr.answering() {
		t.Fatal("answering after a failed round, the success still inside its bound")
	}
}

// TestProbeRecordsLogOnlyChanges: a record per change of outcome, never per round.
func TestProbeRecordsLogOnlyChanges(t *testing.T) {
	tr := newTestReadiness(nil)
	ctx := context.Background()
	for _, err := range []error{errAccountInfoUnanswered, errAccountInfoUnanswered, nil, nil, errAccountInfoUnanswered} {
		tr.commit(ctx, tr.now(), 0, err)
	}
	if n := tr.count(probeFailedMsg); n != 2 {
		t.Errorf("%d failed records, want 2", n)
	}
	if n := tr.count(probeAnsweredMsg); n != 1 {
		t.Errorf("%d answered records, want 1", n)
	}
	if !tr.rec.loggedRecord(slog.LevelWarn, probeFailedMsg, "check", "account info", "err", errAccountInfoUnanswered.Error()) {
		t.Error("the failed record does not carry its check and error")
	}
	if !tr.rec.loggedRecord(slog.LevelInfo, probeAnsweredMsg) {
		t.Error("the answered record is not an Info record without fields")
	}
}

func TestAStaleSuccessIsNotReady(t *testing.T) {
	tr := newTestReadiness(nil)
	tr.commit(context.Background(), tr.now(), 0, nil)
	tr.advance(10 * time.Second)
	if !tr.answering() {
		t.Fatal("a success exactly at its bound stopped counting")
	}
	tr.advance(time.Millisecond)
	if tr.answering() {
		t.Fatal("a success older than its bound still counts")
	}
}

func TestASuccessBeforeAReconnectIsNotReady(t *testing.T) {
	tr := newTestReadiness(nil)
	tr.commit(context.Background(), tr.now(), 0, nil)
	tr.reconnectN.Store(1)
	if tr.answering() {
		t.Fatal("a success begun before the last reconnect still counts")
	}
}

// TestARoundSpanningAReconnectDoesNotCount: the epoch is the one the round began under.
func TestARoundSpanningAReconnectDoesNotCount(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var calls atomic.Int32
	tr := newTestReadiness(func() error {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
		return nil
	})
	tr.interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); <-tr.done }()
	go tr.run(ctx)
	<-entered
	tr.reconnectN.Store(1)
	close(release)
	waitFor(t, 5*time.Second, tr.stored)
	if tr.answering() {
		t.Fatal("a round that began before the reconnect made the broker ready")
	}
	cancel()
	<-tr.done

	// The next round, begun under the new count, does.
	tr2 := newTestReadiness(func() error { return nil })
	tr2.reconnectN.Store(1)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer func() { cancel2(); <-tr2.done }()
	go tr2.run(ctx2)
	waitFor(t, 5*time.Second, tr2.answering)
}

// TestTheLoopObservesTheSignalAtEachPoint: wherever the signal lands, the round in flight is
// discarded, no further round starts, nothing is logged, and the loop ends.
func TestTheLoopObservesTheSignalAtEachPoint(t *testing.T) {
	type gate struct{ entered, release chan struct{} }
	newGate := func() gate { return gate{make(chan struct{}, 1), make(chan struct{})} }
	for _, tc := range []struct {
		name string
		// arm installs the point at which the test cancels; it returns the gate to wait on,
		// or nil when the cancellation happens before the loop starts.
		arm func(tr *testReadiness, cancel context.CancelFunc) *gate
		// rounds is how many rounds must complete before the cancellation.
		rounds int32
	}{
		{"before the first round", func(_ *testReadiness, cancel context.CancelFunc) *gate { cancel(); return nil }, 0},
		{"between rounds", nil, 1},
		{"while the reconnect count is read", func(tr *testReadiness, _ context.CancelFunc) *gate {
			g := newGate()
			tr.reconnects = func() uint64 { g.entered <- struct{}{}; <-g.release; return 0 }
			return &g
		}, 0},
		{"during the account info request", nil, 0},
		{"during the session stream request", nil, 0},
		{"after the probe returned, before the commit", func(tr *testReadiness, cancel context.CancelFunc) *gate {
			tr.beforeCommit = cancel
			return nil
		}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var probes atomic.Int32
			step := map[string]gate{"account info": newGate(), "session stream": newGate()}
			var held string
			switch tc.name {
			case "during the account info request":
				held = "account info"
			case "during the session stream request":
				held = "session stream"
			}
			tr := newTestReadiness(func() error {
				probes.Add(1)
				for _, s := range []string{"account info", "session stream"} {
					if s == held {
						g := step[s]
						g.entered <- struct{}{}
						<-g.release
						// What a request the shutdown cut short returns.
						return errAccountInfoUnanswered
					}
				}
				return nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var g *gate
			if tc.arm != nil {
				g = tc.arm(tr, cancel)
			}
			if tc.rounds > 0 {
				tr.interval = time.Hour
			}
			go tr.run(ctx)
			switch {
			case held != "":
				hg := step[held]
				<-hg.entered
				cancel()
				close(hg.release)
			case g != nil:
				<-g.entered
				cancel()
				close(g.release)
			case tc.rounds > 0:
				waitFor(t, 5*time.Second, func() bool { return probes.Load() == tc.rounds && tr.stored() })
				cancel()
			}
			select {
			case <-tr.done:
			case <-time.After(5 * time.Second):
				t.Fatal("the loop did not end after the signal")
			}
			if tc.rounds == 0 && tr.stored() {
				t.Error("a round ended by the signal was stored")
			}
			if got := probes.Load(); got > max(tc.rounds, 1) || (tc.rounds == 0 && tc.name == "before the first round" && got != 0) ||
				(tc.name == "while the reconnect count is read" && got != 0) {
				t.Errorf("%d probes ran", got)
			}
			if n := tr.count(probeFailedMsg) + tr.count(probeAnsweredMsg); n != 0 {
				t.Errorf("%d records logged for a round the signal ended", n)
			}
		})
	}
}

// TestTheObservationIsReadableWhileAProbeIsHeld: the handler never waits on a round.
func TestTheObservationIsReadableWhileAProbeIsHeld(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	tr := newTestReadiness(func() error { close(entered); <-release; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	go tr.run(ctx)
	<-entered
	read := make(chan bool)
	go func() { read <- tr.answering() }()
	select {
	case <-read:
	case <-time.After(5 * time.Second):
		t.Fatal("answering waited on a held probe")
	}
	cancel()
	close(release)
	<-tr.done
}

// TestTheLoopNeverOverlapsRounds: a held round holds the loop.
func TestTheLoopNeverOverlapsRounds(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	tr := newTestReadiness(func() error { calls.Add(1); <-release; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	go tr.run(ctx)
	waitFor(t, 5*time.Second, func() bool { return calls.Load() == 1 })
	time.Sleep(200 * time.Millisecond) // two hundred intervals
	if n := calls.Load(); n != 1 {
		t.Errorf("%d rounds started while one was held", n)
	}
	cancel()
	once.Do(func() { close(release) })
	<-tr.done
}

// blockingHandler holds every record until released.
type blockingHandler struct {
	entered chan struct{}
	release chan struct{}
}

func (h *blockingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *blockingHandler) Handle(context.Context, slog.Record) error {
	h.entered <- struct{}{}
	<-h.release
	return nil
}
func (h *blockingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *blockingHandler) WithGroup(string) slog.Handler      { return h }

// TestAStalledLogNeverHoldsTheReadiness: the record of a change is written outside the
// observation's mutex, so /readyz reads the committed outcome while the log is stuck.
func TestAStalledLogNeverHoldsTheReadiness(t *testing.T) {
	tr := newTestReadiness(nil)
	h := &blockingHandler{entered: make(chan struct{}, 1), release: make(chan struct{})}
	tr.log = slog.New(h)
	committed := make(chan struct{})
	go func() {
		tr.commit(context.Background(), tr.now(), 0, errAccountInfoUnanswered)
		close(committed)
	}()
	<-h.entered
	read := make(chan bool, 1)
	go func() { read <- tr.answering() }()
	select {
	case got := <-read:
		if got {
			t.Error("answering while the failure's record is being written")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("answering waited on the log")
	}
	close(h.release)
	<-committed
}

// TestASuccessDatesFromItsRoundsStart: a round whose last call stalls past the bound commits a
// success that is already stale; the answers it carries are as old as the round.
func TestASuccessDatesFromItsRoundsStart(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	tr := newTestReadiness(func() error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	tr.interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); <-tr.done }()
	go tr.run(ctx)
	<-entered
	tr.advance(11 * time.Second)
	close(release)
	waitFor(t, 5*time.Second, tr.stored)
	if tr.answering() {
		t.Fatal("a success committed 11 s after its round began counts as fresh past the 10 s bound")
	}
}

// TestARoundIsDatedAfterItsReconnectCount: a reconnect completing while the count is read pairs
// the new count with a start after it, never with the instant before the read.
func TestARoundIsDatedAfterItsReconnectCount(t *testing.T) {
	tr := newTestReadiness(func() error { return nil })
	tr.interval = time.Hour
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	tr.reconnects = func() uint64 {
		once.Do(func() { close(entered); <-release })
		return tr.reconnectN.Load()
	}
	before := *tr.clock.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); <-tr.done }()
	go tr.run(ctx)
	<-entered
	tr.advance(5 * time.Second)
	tr.reconnectN.Store(1)
	close(release)
	waitFor(t, 5*time.Second, tr.stored)
	tr.mu.Lock()
	okAt, epoch := tr.okAt, tr.okEpoch
	tr.mu.Unlock()
	if epoch != 1 || !okAt.After(before) {
		t.Fatalf("round stored count %d at %v, want count 1 dated after the read began (%v)", epoch, okAt, before)
	}
}
