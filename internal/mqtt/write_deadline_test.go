package mqtt

import (
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// F-035. Every MQTT application write is bounded by mqttWriteDeadline, installed
// and cleared in writeSocket, which is the single socket writer and the only
// production acquirer of c.wmu. These tests assert that property directly; the
// courtesy-DISCONNECT sites the roadmap bullet names are consequences of it, not
// separate mechanisms, which is why there is no per-site arming to test.
//
// Nothing here may run in parallel: the tests shorten the package-level
// mqttWriteDeadline, and a t.Parallel() sibling would observe the override.

// testStallConn is the fixture contract this item needs, and each clause is load
// bearing:
//
//   - it signals EVERY write's entry, not just the first, because a single
//     channel identifies only the first of two contenders;
//   - SetWriteDeadline supports installation, REPLACEMENT and CLEARING, and a
//     change wakes a writer already blocked, which is what a real socket does and
//     what makes the timeout observable at all;
//   - Close is idempotent, concurrent-safe and releases blocked writers;
//   - it has a writable mode and a prefix mode, so a successful write and a
//     mid-frame stall are the same fixture in different states;
//   - releaseAll unblocks every writer during cleanup, so a failing assertion
//     does not hang the package.
type testStallConn struct {
	mu       sync.Mutex
	wake     chan struct{} // closed and replaced on every state change
	closed   bool
	writable bool
	// prefix is how many more bytes a non-writable conn will absorb before it
	// stalls, which is what makes a partial write reachable.
	prefix int
	// deadlines records every SetWriteDeadline argument in order. The final entry
	// is what discriminates a cleared deadline from one merely overwritten by the
	// next write.
	deadlines []time.Time
	written   []byte
	entered   chan int // one send per write entry, carrying its byte count
}

func newTestStallConn() *testStallConn {
	return &testStallConn{wake: make(chan struct{}), entered: make(chan int, 64)}
}

// bumpLocked wakes everything waiting on a state change. The channel is closed
// rather than sent on so every waiter sees it, and replaced so the next wait is
// fresh.
func (s *testStallConn) bumpLocked() {
	close(s.wake)
	s.wake = make(chan struct{})
}

func (s *testStallConn) setWritable(w bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writable = w
	s.bumpLocked()
}

func (s *testStallConn) setPrefix(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prefix = n
	s.bumpLocked()
}

// releaseAll is cleanup, not an assertion helper: it makes a blocked writer
// return so a failing test cannot leave a goroutine parked forever.
func (s *testStallConn) releaseAll() { _ = s.Close() }

func (s *testStallConn) lastDeadline() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.deadlines) == 0 {
		return time.Time{}, false
	}
	return s.deadlines[len(s.deadlines)-1], true
}

func (s *testStallConn) allDeadlines() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.deadlines...)
}

func (s *testStallConn) bytesWritten() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.written...)
}

func (s *testStallConn) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// awaitEntry waits for the NEXT write entry. Every contender gets its own call,
// which is how a two-contender test tells the incumbent's entry from the
// challenger's.
func (s *testStallConn) awaitEntry(t *testing.T, what string) int {
	t.Helper()
	select {
	case n := <-s.entered:
		return n
	case <-time.After(5 * time.Second):
		t.Fatalf("no write entry for %s", what)
		return 0
	}
}

func (s *testStallConn) Write(b []byte) (int, error) {
	s.mu.Lock()
	select {
	case s.entered <- len(b):
	default:
	}
	total := 0
	for {
		if s.closed {
			s.mu.Unlock()
			return total, net.ErrClosed
		}
		if s.writable || s.prefix > 0 {
			n := len(b)
			if !s.writable && s.prefix < n {
				n = s.prefix
			}
			if !s.writable {
				s.prefix -= n
			}
			s.written = append(s.written, b[:n]...)
			b = b[n:]
			total += n
			if len(b) == 0 {
				s.mu.Unlock()
				return total, nil
			}
			// A prefix ran out mid-buffer: fall through and stall, which is the
			// partial-write case. Report progress with the eventual error.
		}
		d := time.Time{}
		if len(s.deadlines) > 0 {
			d = s.deadlines[len(s.deadlines)-1]
		}
		now := time.Now()
		if !d.IsZero() && !now.Before(d) {
			s.mu.Unlock()
			return total, os.ErrDeadlineExceeded
		}
		wake := s.wake
		s.mu.Unlock()
		var fire <-chan time.Time
		if !d.IsZero() {
			tm := time.NewTimer(d.Sub(now))
			fire = tm.C
			select {
			case <-wake:
			case <-fire:
			}
			tm.Stop()
		} else {
			<-wake
		}
		s.mu.Lock()
	}
}

func (s *testStallConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.bumpLocked()
	return nil
}

func (s *testStallConn) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadlines = append(s.deadlines, t)
	// A deadline change must reach a writer already blocked; without this the
	// fixture would make an unbounded write look bounded whenever the deadline
	// happened to be installed first.
	s.bumpLocked()
	return nil
}

// Read blocks until the conn is closed, which is what lets a REAL readLoop sit on
// this fixture: a Read that returned at once would make the loop exit before the
// write under test ever timed out, and the teardown it triggers is the thing some
// of these tests are trying to observe.
func (s *testStallConn) Read([]byte) (int, error) {
	s.mu.Lock()
	for !s.closed {
		wake := s.wake
		s.mu.Unlock()
		<-wake
		s.mu.Lock()
	}
	s.mu.Unlock()
	return 0, net.ErrClosed
}
func (s *testStallConn) LocalAddr() net.Addr  { return testStubAddr{} }
func (s *testStallConn) RemoteAddr() net.Addr { return testStubAddr{} }
func (s *testStallConn) SetDeadline(time.Time) error {
	return nil
}
func (s *testStallConn) SetReadDeadline(time.Time) error { return nil }

type testStubAddr struct{}

func (testStubAddr) Network() string { return "test" }
func (testStubAddr) String() string  { return "test" }

// testShortWriteDeadline shortens the package deadline for one test and restores
// it in cleanup. Assertion budgets are captured BEFORE the shortening so they do
// not scale with it, and the restore runs after every goroutine has been joined.
func testShortWriteDeadline(t *testing.T, d time.Duration) {
	t.Helper()
	prev := mqttWriteDeadline
	mqttWriteDeadline = d
	t.Cleanup(func() { mqttWriteDeadline = prev })
}

// testStalledClient wires a stalled connection into a client the way production
// does: nc is what writeSocket writes to, rawConn is what the error path closes.
// A fixture leaving rawConn unwired would show a socket that never closes no
// matter which path ran.
func testStalledClient(t *testing.T) (*client, *testStallConn) {
	t.Helper()
	sc := newTestStallConn()
	t.Cleanup(sc.releaseAll)
	c := &client{nc: sc, rawConn: sc}
	return c, sc
}

func testIsTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

// T1. The property, per writer. writeSocket itself returns the timeout and
// RELEASES wmu but deliberately does NOT close the socket — closing is the
// wrapper's job, and a caller holding c.mu must not reach closeConnection. Each
// wrapper returns bounded AND leaves the socket closed through the existing
// error path, which is the slow-consumer disconnect.
func TestF035EveryWriteIsDeadlineBounded(t *testing.T) {
	budget := 5 * time.Second
	testShortWriteDeadline(t, 150*time.Millisecond)

	t.Run("writeSocket returns the timeout and does not close", func(t *testing.T) {
		c, sc := testStalledClient(t)
		done := make(chan error, 1)
		go func() { done <- c.writeSocket([]byte("frame"), time.Time{}) }()
		sc.awaitEntry(t, "writeSocket")
		select {
		case err := <-done:
			if !testIsTimeout(err) {
				t.Fatalf("writeSocket returned %v, want a timeout", err)
			}
		case <-time.After(budget):
			t.Fatal("writeSocket never returned: the write is not deadline bounded")
		}
		if sc.isClosed() {
			t.Fatal("writeSocket closed the socket; it must leave teardown to its caller")
		}
		// wmu must be free, or the bound buys nothing for the next writer. Taking
		// it is the whole assertion: this goroutine only reaches the close if the
		// timed-out writer let go.
		released := make(chan struct{})
		go func() {
			c.wmu.Lock()
			defer c.wmu.Unlock()
			close(released)
		}()
		select {
		case <-released:
		case <-time.After(budget):
			t.Fatal("writeSocket returned without releasing wmu")
		}
	})

	for _, tc := range []struct {
		name  string
		write func(c *client, b []byte)
	}{
		{"queueOutbound", func(c *client, b []byte) { c.queueOutbound(b) }},
		{"enqueueProto", func(c *client, b []byte) { c.enqueueProto(b) }},
		{"enqueueProtoErr", func(c *client, b []byte) { _ = c.enqueueProtoErr(b) }},
		{"queueOutboundBy uncapped", func(c *client, b []byte) { c.queueOutboundBy(b, time.Time{}) }},
		// The one wrapper that holds c.mu ACROSS the write, which is what makes an
		// unbounded write here block every path that needs this client's mutex. Its
		// bool return must keep meaning what it meant (a disconnected-state refusal);
		// a timeout is a new cause of an existing failure, not a new outcome.
		{"mqttEnqueueProtoLocked", func(c *client, b []byte) {
			if !c.mqttEnqueueProtoLocked(b, false) {
				panic("mqttEnqueueProtoLocked refused a write it was not asked to gate")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, sc := testStalledClient(t)
			done := make(chan struct{})
			go func() { tc.write(c, []byte("frame")); close(done) }()
			sc.awaitEntry(t, tc.name)
			select {
			case <-done:
			case <-time.After(budget):
				t.Fatalf("%s never returned: the write is not deadline bounded", tc.name)
			}
			if !sc.isClosed() {
				t.Fatalf("%s left the socket open after a write timeout; the slow consumer is never disconnected", tc.name)
			}
		})
	}
}

// T2. The regression test for the TLS defect in the design: a deadline left
// installed after a SUCCESSFUL write expires through idle time and hands a
// TLS 1.3 KeyUpdate response — written from Read, never through writeSocket — a
// permanent outbound error. Expressed without a TLS peer.
//
// The discriminator is the FINAL SetWriteDeadline argument being the zero time.
// A follow-up MQTT write proves nothing on its own, because it installs a fresh
// deadline anyway; what models the bypassing TLS writer is the direct transport
// write below, issued after the shortened deadline would have elapsed.
func TestF035WriteDeadlineIsClearedAfterASuccessfulWrite(t *testing.T) {
	testShortWriteDeadline(t, 100*time.Millisecond)
	c, sc := testStalledClient(t)
	sc.setWritable(true)

	if err := c.writeSocket([]byte("frame"), time.Time{}); err != nil {
		t.Fatalf("a writable conn returned %v", err)
	}
	if d, ok := sc.lastDeadline(); !ok || !d.IsZero() {
		t.Fatalf("the last deadline installed was %v, want the zero time: a stale deadline breaks a TLS write that never passes through writeSocket", d)
	}
	all := sc.allDeadlines()
	if len(all) != 2 || all[0].IsZero() {
		t.Fatalf("deadlines installed = %v, want one nonzero install then one clear", all)
	}

	// The bypassing writer: a direct transport write, issued well after the
	// shortened deadline would have fired, must still succeed.
	time.Sleep(200 * time.Millisecond)
	if _, err := sc.Write([]byte("tls-keyupdate")); err != nil {
		t.Fatalf("a transport write that bypasses writeSocket failed with %v: the deadline was left installed", err)
	}
}

// T3. A mid-frame timeout. The conn absorbs a real prefix and then stalls, so the
// write returns a timeout with partial progress and the wrapper closes. The
// deadline must be cleared on the ERROR path too, not only on success.
func TestF035PartialWriteTimesOutAndCloses(t *testing.T) {
	budget := 5 * time.Second
	testShortWriteDeadline(t, 150*time.Millisecond)
	c, sc := testStalledClient(t)
	sc.setPrefix(3)

	done := make(chan struct{})
	go func() { c.queueOutbound([]byte("frame-body")); close(done) }()
	sc.awaitEntry(t, "the partial write")
	select {
	case <-done:
	case <-time.After(budget):
		t.Fatal("a mid-frame stall never returned")
	}
	if got := string(sc.bytesWritten()); got != "fra" {
		t.Fatalf("bytes absorbed = %q, want the 3-byte prefix: the fixture did not exercise a partial write", got)
	}
	if !sc.isClosed() {
		t.Fatal("a mid-frame timeout left the socket open")
	}
	if d, ok := sc.lastDeadline(); !ok || !d.IsZero() {
		t.Fatalf("the last deadline after an ERROR return was %v, want the zero time", d)
	}
}

// T4. The two-contender case, and it is labelled as such: this is not a universal
// mutex bound. sync.Mutex gives no elapsed-time guarantee with many contenders,
// and a c.mu section can contain many bounded writes, so what is proved here is
// that an incumbent's timeout releases wmu for a waiter — the mechanism — not a
// figure for how long any lock is held.
func TestF035WriteMutexContenderIsReleased(t *testing.T) {
	budget := 10 * time.Second
	testShortWriteDeadline(t, 200*time.Millisecond)
	c, sc := testStalledClient(t)

	incumbent := make(chan error, 1)
	go func() { incumbent <- c.writeSocket([]byte("incumbent"), time.Time{}) }()
	sc.awaitEntry(t, "the incumbent write")

	// The challenger's ATTEMPT is synchronised separately from its acquisition, and
	// that separation is the point: observing it only once it is past wmu would not
	// establish that it was ever contending while the incumbent was blocked, so the
	// test would pass against a challenger that simply started late.
	attempting := make(chan struct{})
	challenger := make(chan error, 1)
	go func() {
		close(attempting)
		challenger <- c.writeSocket([]byte("challenger"), time.Time{})
	}()
	<-attempting
	// The challenger is now in or about to enter wmu.Lock() while the incumbent holds
	// it. Give it a moment to actually block there, then confirm the incumbent is
	// still the one inside: a challenger that had already acquired wmu would have
	// signalled its own write entry by now.
	time.Sleep(50 * time.Millisecond)
	select {
	case n := <-sc.entered:
		t.Fatalf("a second write (%d bytes) entered the conn while the incumbent held wmu: the mutex is not serializing writers, so this test proves nothing about its release", n)
	default:
	}

	select {
	case err := <-incumbent:
		if !testIsTimeout(err) {
			t.Fatalf("the incumbent returned %v, want a timeout", err)
		}
	case <-time.After(budget):
		t.Fatal("the incumbent never returned")
	}
	sc.awaitEntry(t, "the challenger write past wmu")
	select {
	case err := <-challenger:
		if !testIsTimeout(err) {
			t.Fatalf("the challenger returned %v, want a timeout", err)
		}
	case <-time.After(budget):
		t.Fatal("the challenger never acquired wmu: an incumbent's timeout does not release the write mutex")
	}
}

// T8(ii). Cap selection, kept away from any socket an incumbent's timeout has
// already closed. The installed deadline is the EARLIER of the drain's absolute
// budget and now+mqttWriteDeadline, and an already-past cap is installed as-is so
// the write fails at once — which is what a spent drain budget does today.
func TestF035CapSelectsTheEarlierBound(t *testing.T) {
	testShortWriteDeadline(t, 500*time.Millisecond)

	t.Run("a shorter remaining budget wins", func(t *testing.T) {
		c, sc := testStalledClient(t)
		sc.setWritable(true)
		cap := time.Now().Add(50 * time.Millisecond)
		if err := c.writeSocket([]byte("x"), cap); err != nil {
			t.Fatalf("write: %v", err)
		}
		all := sc.allDeadlines()
		if len(all) != 2 {
			t.Fatalf("deadlines = %v, want an install and a clear", all)
		}
		if !all[0].Equal(cap) {
			t.Fatalf("installed %v, want the cap %v: the drain's budget was ignored", all[0], cap)
		}
	})

	t.Run("a longer remaining budget loses to mqttWriteDeadline", func(t *testing.T) {
		c, sc := testStalledClient(t)
		sc.setWritable(true)
		cap := time.Now().Add(time.Hour)
		before := time.Now()
		if err := c.writeSocket([]byte("x"), cap); err != nil {
			t.Fatalf("write: %v", err)
		}
		all := sc.allDeadlines()
		if len(all) != 2 {
			t.Fatalf("deadlines = %v, want an install and a clear", all)
		}
		if !all[0].Before(before.Add(mqttWriteDeadline + 100*time.Millisecond)) {
			t.Fatalf("installed %v, want at most now+mqttWriteDeadline: a far cap must not widen the bound", all[0])
		}
	})

	t.Run("an already-past cap is installed as-is", func(t *testing.T) {
		c, sc := testStalledClient(t)
		sc.setWritable(true)
		cap := time.Now().Add(-time.Minute)
		err := c.writeSocket([]byte("x"), cap)
		if err != nil && !testIsTimeout(err) {
			t.Fatalf("write returned %v", err)
		}
		all := sc.allDeadlines()
		if len(all) == 0 || !all[0].Equal(cap) {
			t.Fatalf("installed %v, want the past cap %v verbatim", all, cap)
		}
	})
}

// testDrainableClient is a v5 connection registered the way the drain finds one:
// CONNACK written (so shutdownDisconnect builds and claims), its DISCONNECT claim
// unspent, and a stalled socket. Without connAckWritten the drain skips both the
// mutex and the write, and every assertion below would pass against code that
// never wrote anything.
func testDrainableClient(t *testing.T, s *Server) (*client, *testStallConn) {
	t.Helper()
	sc := newTestStallConn()
	t.Cleanup(sc.releaseAll)
	c := &client{srv: s, nc: sc, rawConn: sc, mpay: 1024 * 1024}
	c.mqtt = &mqtt{proto: mqttProtoLevel5, pp: &mqttPublish{}, cp: &mqttConnectProto{}}
	c.initClient()
	c.connAckWritten.Store(true)
	return c, sc
}

// T8. The shutdown drain, in its two separable halves — round 3 faulted an earlier
// version for conflating them, because a client whose socket an incumbent's timeout
// has already closed proves nothing about which deadline was selected.
//
// (i) is the obstruction: `drainClients` walks its snapshot SERIALLY, and one
// client with a goroutine parked in a socket write used to hang the whole walk, so
// shutdown phases 2 to 4 never ran. It is a drain-UNIT test — no readLoop runs — so
// what it proves is that the obstruction is gone, not that the lifecycle completes.
//
// (ii) is cap selection, driven through the real production route
// (`shutdownDisconnect` → `queueOutboundBy` → `writeSocket`) rather than by calling
// the writer directly, so a change that dropped the cap anywhere along it is caught.
func TestF035DrainReturnsWithAStalledClient(t *testing.T) {
	t.Run("the stalled-write obstruction is removed", func(t *testing.T) {
		budget := 10 * time.Second
		testShortWriteDeadline(t, 300*time.Millisecond)
		s := &Server{opts: &Options{}, clients: map[uint64]*client{}}

		c, sc := testDrainableClient(t, s)
		s.clients[1] = c
		// An incumbent delivery goroutine parked in the socket write, holding wmu —
		// the exact state that used to hang the serial walk forever.
		incumbent := make(chan struct{})
		go func() { _ = c.writeSocket([]byte("incumbent delivery"), time.Time{}); close(incumbent) }()
		sc.awaitEntry(t, "the incumbent delivery write")

		drained := make(chan struct{})
		go func() { s.drainClients(time.Now().Add(budget)); close(drained) }()
		select {
		case <-drained:
		case <-time.After(budget):
			t.Fatal("drainClients did not return with one stalled client: the serial walk is still obstructed, so shutdown phases 2 to 4 never run")
		}
		<-incumbent
	})

	t.Run("the drain's budget caps the write when it is sooner", func(t *testing.T) {
		testShortWriteDeadline(t, 30*time.Second)
		s := &Server{opts: &Options{}, clients: map[uint64]*client{}}
		c, sc := testDrainableClient(t, s)
		sc.setWritable(true)
		s.clients[1] = c

		deadline := time.Now().Add(200 * time.Millisecond)
		s.drainClients(deadline)

		all := sc.allDeadlines()
		if len(all) == 0 {
			t.Fatal("the drain installed no deadline at all: it did not reach the bounded writer")
		}
		if !all[0].Equal(deadline) {
			t.Fatalf("the drain installed %v, want its own budget %v: the cap is not reaching writeSocket through shutdownDisconnect and queueOutboundBy", all[0], deadline)
		}
		if !all[len(all)-1].IsZero() {
			t.Fatalf("the drain's write left %v installed, want it cleared", all[len(all)-1])
		}
	})

	t.Run("mqttWriteDeadline caps the write when the budget is later", func(t *testing.T) {
		testShortWriteDeadline(t, 200*time.Millisecond)
		s := &Server{opts: &Options{}, clients: map[uint64]*client{}}
		c, sc := testDrainableClient(t, s)
		sc.setWritable(true)
		s.clients[1] = c

		before := time.Now()
		s.drainClients(before.Add(time.Hour))

		all := sc.allDeadlines()
		if len(all) == 0 {
			t.Fatal("the drain installed no deadline at all")
		}
		if !all[0].Before(before.Add(mqttWriteDeadline + 100*time.Millisecond)) {
			t.Fatalf("the drain installed %v, want at most now+mqttWriteDeadline: a far drain budget must not widen the bound", all[0])
		}
	})

	t.Run("an already-spent budget is installed as-is", func(t *testing.T) {
		testShortWriteDeadline(t, 30*time.Second)
		s := &Server{opts: &Options{}, clients: map[uint64]*client{}}
		c, sc := testDrainableClient(t, s)
		sc.setWritable(true)
		s.clients[1] = c

		spent := time.Now().Add(-time.Minute)
		s.drainClients(spent)

		all := sc.allDeadlines()
		if len(all) == 0 || !all[0].Equal(spent) {
			t.Fatalf("the drain installed %v, want the spent budget %v verbatim: a later client in the serial walk must fail its write at once rather than get a fresh 10s", all, spent)
		}
	})
}

// T5. The delivery section, which is the population the F-035 dossier did NOT name:
// it named `wmu` only. `mqttEnqueuePublishMsgTo` holds the RECEIVER's `c.mu` across
// its `queueOutbound` calls, so before the bound an ordinary QoS 0 delivery to a
// stalled subscriber blocked every path that needed that client's mutex — including,
// gate-held, the whole Will phase of every disposal.
//
// Driven through the real function rather than by taking `cc.mu` by hand: a fixture
// that locked the mutex itself would prove nothing about where the delivery path
// actually blocks.
func TestF035DeliverySectionReleasesTheClientMutex(t *testing.T) {
	budget := 10 * time.Second
	testShortWriteDeadline(t, 300*time.Millisecond)

	cc, sc := testStalledClient(t)
	cc.mqtt = &mqtt{proto: mqttProtoLevel5, pp: &mqttPublish{}, cp: &mqttConnectProto{}}
	cc.initClient()
	// A real subscription: sub.mqtt must be non-nil or the retained-flush branch
	// panics before the section is ever entered.
	sub := &subscription{client: cc, mqtt: &mqttSub{}}
	sender := &client{}
	sender.mqtt = &mqtt{proto: mqttProtoLevel5, pp: &mqttPublish{}, cp: &mqttConnectProto{}}
	sender.initClient()

	delivered := make(chan bool, 1)
	go func() {
		delivered <- sender.mqttEnqueueQoS0PublishMsgTo(cc, sub, false, 0,
			[]byte("f035/delivery"), []byte("payload"), nil)
	}()
	sc.awaitEntry(t, "the delivery's socket write inside cc.mu")

	// The contender's attempt is synchronised separately from its acquisition, so
	// that reaching cc.mu is what the assertion observes rather than when it started.
	attempting := make(chan struct{})
	contended := make(chan struct{})
	go func() {
		close(attempting)
		// Taking cc.mu is the whole assertion: this goroutine reaches the close
		// only once the blocked delivery has let go of it.
		cc.mu.Lock()
		defer cc.mu.Unlock()
		close(contended)
	}()
	<-attempting
	select {
	case <-contended:
		t.Fatal("cc.mu was free while the delivery was blocked in its socket write: the fixture never reached the section it names")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-contended:
	case <-time.After(budget):
		t.Fatal("a path needing the stalled subscriber's c.mu never got it: the delivery section is not bounded, so every disposal's Will phase can block on an ordinary QoS 0 delivery")
	}

	select {
	case ok := <-delivered:
		// A timeout is a new CAUSE of an existing failure, never a new return
		// outcome: false here means "packet too large", whose caller acknowledges
		// and removes state. A delivery that timed out must not claim that.
		if !ok {
			t.Fatal("a write timeout made mqttEnqueuePublishMsgTo return false, which its caller reads as a packet-size discard and answers by acknowledging and removing in-flight state")
		}
	case <-time.After(budget):
		t.Fatal("the delivery never returned")
	}
}

// T9, lifecycle half. The slow-consumer chain end to end over a REAL readLoop:
// a blocked write, its deadline failing it, the raw socket closed by the wrapper's
// error path, and the ordinary teardown running as a consequence. A stub conn's
// closed flag does not establish that chain, which is why this one uses `net.Pipe`
// wired into `readLoop` with lifecycle accounting — the construction
// `TestMQTTBoundRejectionNoReentryAfterForeign` already uses for the same reason.
//
// The TCP-specific half — establishing backpressure by shrinking a real socket
// buffer — is explicitly OWED and recorded in docs/ROADMAP.md. `net.Pipe` is
// unbuffered and synchronous, so a write with no reader is blocked by construction
// rather than by a buffer-fill argument that would have to be proved.
func TestF035SlowConsumerIsDisconnected(t *testing.T) {
	budget := 10 * time.Second
	testShortWriteDeadline(t, 400*time.Millisecond)

	e := testNewRejectionEnv(t, "f035slowconsumer")
	brokerSide, clientSide := net.Pipe()
	defer clientSide.Close()

	c := &client{srv: e.s, acc: e.s.globalAccount(), nrc: e.s.nrc,
		nc: brokerSide, rawConn: brokerSide, mpay: 1024 * 1024}
	c.mqtt = &mqtt{sess: e.sess, asm: e.asm, proto: mqttProtoLevel5, pp: &mqttPublish{}, cp: &mqttConnectProto{}}
	c.initClient()
	c.mu.Lock()
	c.flags.set(connectReceived)
	c.mu.Unlock()
	c.connAckWritten.Store(true)

	e.s.clientWG.Add(1) // readLoop's defer releases the lifecycle token
	go c.readLoop(nil)

	// The peer never reads. net.Pipe is unbuffered, so this write IS blocked — no
	// buffer-fill inference is needed — and the only thing that can end it is the
	// deadline writeSocket installed.
	wrote := make(chan struct{})
	go func() { c.queueOutbound([]byte("a frame nobody will read")); close(wrote) }()
	select {
	case <-wrote:
	case <-time.After(budget):
		t.Fatal("the write to a non-reading peer never returned: it is not deadline bounded")
	}

	// The raw socket is closed by the wrapper's error path, which is the slow-consumer
	// disconnect. The peer observing its own end broken is the transport-level proof,
	// and it is taken WITHOUT resuming peer reads.
	_ = clientSide.SetReadDeadline(time.Now().Add(budget))
	buf := make([]byte, 8)
	if _, err := clientSide.Read(buf); err == nil {
		t.Fatal("the peer's end was still open after its write timed out: the slow consumer is never disconnected")
	} else if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the peer's end neither delivered data nor broke within %v: no closure followed the write timeout", budget)
	}

	// And the ordinary teardown ran as a consequence — the readLoop's own disconnect(),
	// not a close this test performed. Its lifecycle token being released is the
	// observable end of that chain.
	done := make(chan struct{})
	go func() { e.s.clientWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(budget):
		t.Fatal("the readLoop never exited after its socket was closed: the write timeout closes the socket but the ordinary teardown does not follow")
	}
}

// testWillClient is a v5 connection carrying an ARMED delayed Will over a socket
// nobody reads, with a REAL readLoop attached so an actual teardown can run.
func testWillClient(t *testing.T, e *testRejectionEnv, delaySecs uint32) (*client, *testStallConn) {
	t.Helper()
	sc := newTestStallConn()
	t.Cleanup(sc.releaseAll)
	c := &client{srv: e.s, acc: e.s.globalAccount(), nrc: e.s.nrc,
		nc: sc, rawConn: sc, mpay: 1024 * 1024}
	c.mqtt = &mqtt{sess: e.sess, asm: e.asm, proto: mqttProtoLevel5,
		pp: &mqttPublish{}, cp: &mqttConnectProto{}, cid: e.clientID}
	c.mqtt.cp.will = &mqttWill{
		topic:   []byte("f035/will"),
		subject: []byte("f035.will"),
		message: []byte("last words"),
		props:   &mqttProperties{willDelay: delaySecs},
	}
	c.mqtt.willDisposition = mqttWillArmed
	// A non-zero expiry is what makes the CLOSE classify this Will as owed-DELAYED
	// rather than owed-now. mqttHandleClosedClient reads it through
	// mqttSessionExpiry, so it has to live on the connection, not be passed in.
	c.mqtt.sessionExpiry = 3600
	c.initClient()
	c.mu.Lock()
	c.flags.set(connectReceived)
	c.mu.Unlock()
	c.connAckWritten.Store(true)
	e.sess.mu.Lock()
	e.sess.c = c
	e.sess.mu.Unlock()
	e.s.clientWG.Add(1) // readLoop's defer releases the lifecycle token
	go c.readLoop(nil)
	return c, sc
}

func testJoinTeardown(t *testing.T, s *Server, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() { s.clientWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the readLoop never exited (%s): the teardown this ordering depends on did not run", what)
	}
}

// T10. A delayed Will survives a write-timeout close, in BOTH orderings of the
// ORDINARY TEARDOWN against a fence-based disposal, and the outbox record it
// produces carries the disposal's captured FENCE rather than whatever the session
// says now.
//
// The teardown is driven, never simulated: no test here assigns the disposition
// enum or calls mqttOweWill by hand. A real readLoop sits on the connection, an
// actual write timeout closes the socket underneath it, and the loop's own
// disconnect() runs mqttHandleClosedClient, which is what classifies the Will. The
// write timeout is a NEW producer of that early close, which is why both orderings
// are driven rather than argued about.
//
// The session is deliberately RESET between the fence capture and the consumption,
// so the fence's generation and sequence DIFFER from the session's current ones: an
// implementation that sampled the session instead of using the fence would pass a
// fixture where they happened to agree.
//
// FireAt is bracketed against the CURRENT, consumption-based anchoring. That is
// deliberately not an assertion that consumption-time anchoring is RIGHT — it is a
// filed roadmap item that it is wrong — only that the value produced is the one this
// code is specified to produce today.
func TestF035DelayedWillSurvivesAWriteTimeoutClose(t *testing.T) {
	const delaySecs = 600

	// closeObservedAt is the anchor the Will Delay runs from (mqtt.closeObservedAt).
	// When the teardown observed the close before the Will was consumed, that instant
	// is the anchor and FireAt is EXACTLY it plus the delay -- so this asserts equality
	// rather than a window, which is strictly stronger than the consumption-bracketed
	// window this used to check and is the property the anchor delivers. Zero means the
	// Will was still ARMED at consumption, so the consuming route's own action is the
	// close, and the [lo, hi] bracket around that call is then the right check.
	assertOutbox := func(t *testing.T, e *testRejectionEnv, fence *mqttWillFence, closeObservedAt int64, lo, hi time.Time) {
		t.Helper()
		filter := mqttWillQClientFilter(e.asm.domainTk, getHash(e.clientID))
		var rec *mqttWillOutbox
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if r, _, err := e.asm.jsa.loadWillOutbox(e.asm.domainTk, filter); err == nil && r != nil {
				rec = r
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if rec == nil {
			t.Fatal("no delayed-Will outbox record was written: a write-timeout close lost the Will")
		}
		if rec.Gen != fence.gen {
			t.Fatalf("the outbox record carries generation %q, want the disposal's captured fence %q: the disposal sampled the RESET session instead of its fence", rec.Gen, fence.gen)
		}
		if rec.OriginSessionSeq != fence.seq {
			t.Fatalf("the outbox record carries origin sequence %d, want the fence's %d", rec.OriginSessionSeq, fence.seq)
		}
		fireAt := time.Unix(0, rec.FireAt)
		if closeObservedAt != 0 {
			want := time.Unix(0, closeObservedAt).Add(delaySecs * time.Second)
			if !fireAt.Equal(want) {
				t.Fatalf("FireAt %v, want exactly %v: the %ds delay must run from the OBSERVED CLOSE, not from the consumption that followed it",
					fireAt, want, delaySecs)
			}
			// And the anchor must genuinely predate the consumption, or the assertion
			// above would hold for a broker that still measured from now.
			if !time.Unix(0, closeObservedAt).Before(lo) {
				t.Fatalf("the close observation %v is not before the consumption at %v, so this fixture cannot tell the two anchors apart",
					time.Unix(0, closeObservedAt), lo)
			}
			return
		}
		if fireAt.Before(lo.Add(delaySecs*time.Second)) || fireAt.After(hi.Add(delaySecs*time.Second)) {
			t.Fatalf("FireAt %v is outside [%v, %v] for a %ds delay", fireAt,
				lo.Add(delaySecs*time.Second), hi.Add(delaySecs*time.Second), delaySecs)
		}
	}

	// conflictRecomputation makes the delay-recomputation inputs DISAGREE with the
	// disposition the teardown recorded. proto is the only input that does that
	// cleanly: mqttWillIsDelayed reads it, and nothing that computes the fire time
	// does, so a recomputing implementation says "not delayed" and publishes at once
	// while the effective delay these tests assert is unchanged. (Zeroing the expiry
	// also flips the recomputation, but it caps the delay to zero in the CORRECT code
	// too, so it would discriminate nothing.)
	conflictRecomputation := func(c *client) {
		c.mu.Lock()
		c.mqtt.proto = mqttProtoLevel
		c.mu.Unlock()
	}

	assertDischarged := func(t *testing.T, c *client, what string) {
		t.Helper()
		c.mu.Lock()
		got := c.mqtt.willDisposition
		c.mu.Unlock()
		if got != mqttWillDischarged {
			t.Fatalf("%s: the Will is %v, want discharged — it must be consumed exactly once across the teardown and the disposal", what, got)
		}
	}

	// (a) The readLoop's teardown wins the race to OBSERVE the close, but the session
	// has already been detached — which is exactly what a displacer does before it
	// disposes. The teardown then marks the Will owed-delayed and deliberately leaves
	// it: mqttPublishOwedWill discharges an owed-NOW Will and nothing else, so the
	// delayed one is still there for the disposal, which consumes it under its fence.
	//
	// This is the race section 3.5 names, and the write timeout is a new producer of
	// it: before F-035 a still-connected peer could not cause this close at all.
	t.Run("the teardown observes the close first, the disposal consumes under its fence", func(t *testing.T) {
		testShortWriteDeadline(t, 8*time.Second)
		e := testNewRejectionEnv(t, "f035willteardown")
		c, sc := testWillClient(t, e, delaySecs)

		blocked := make(chan struct{})
		go func() { c.queueOutbound([]byte("a frame nobody will read")); close(blocked) }()
		sc.awaitEntry(t, "the blocked write whose timeout closes this connection")

		// The detach a displacer performs, taken while the write is still blocked.
		fence := e.sess.mqttCaptureWillFence()
		e.sess.mu.Lock()
		e.sess.c = nil
		e.sess.mu.Unlock()

		// Now the ACTUAL write timeout closes the socket and the readLoop's own
		// disconnect() runs mqttHandleClosedClient. Nothing here assigns the
		// disposition; the teardown classifies it.
		<-blocked
		testJoinTeardown(t, e.s, "teardown-observes-first")

		c.mu.Lock()
		got := c.mqtt.willDisposition
		c.mu.Unlock()
		if got != mqttWillOwedDelayed {
			t.Fatalf("the ordinary teardown left the Will as %v, want owed-delayed: this ordering never reached the state it names, so what follows would not be testing the fence route", got)
		}

		// The reset is what makes the fence discriminating: after it the session's own
		// generation and sequence no longer match the captured ones, so a disposal
		// that sampled the session would carry the wrong identity.
		e.sess.mu.Lock()
		e.sess.gen = mqttNewSessGen()
		e.sess.seq++
		e.sess.mu.Unlock()
		conflictRecomputation(c)

		// Through the real fence route, not by calling mqttHandleWill with the
		// fence's fields spread by hand: a fixture that did that would never reach
		// mqttConsumeWillWithFence, and a change that sampled the session there
		// instead of using the fence would go unnoticed.
		// Read BEFORE the consume, which clears it with the Will.
		c.mu.Lock()
		closeObservedAt := c.mqtt.closeObservedAt
		c.mu.Unlock()
		if closeObservedAt == 0 {
			t.Fatal("the teardown left no close observation, so the delay would be measured from the consumption: this ordering is exactly the one the anchor exists for")
		}

		lo := time.Now()
		c.mqttConsumeWillWithFence(e.asm, e.sess, fence)
		hi := time.Now()
		assertOutbox(t, e, fence, closeObservedAt, lo, hi)
		assertDischarged(t, c, "teardown-observes-first")
	})

	// (b) The other ordering: a disposal consumes the Will while the write is still
	// blocked and the teardown has not run at all. The Will is still ARMED here, so
	// mqttHandleWill's recomputation IS the specified path — no conflict is forced,
	// because forcing one would break the correct code rather than discriminate
	// against a wrong one. What this ordering establishes is the fence identity and
	// that the teardown which follows cannot take the Will a second time.
	t.Run("the disposal consumes while the write is still blocked", func(t *testing.T) {
		testShortWriteDeadline(t, 8*time.Second)
		e := testNewRejectionEnv(t, "f035willdisposal")
		c, sc := testWillClient(t, e, delaySecs)

		blocked := make(chan struct{})
		go func() { c.queueOutbound([]byte("a frame nobody will read")); close(blocked) }()
		// The write's ENTRY, not merely its launch: without this the disposal could
		// run before the write had started and the ordering would prove nothing.
		sc.awaitEntry(t, "the blocked write this ordering runs underneath")

		c.mu.Lock()
		got := c.mqtt.willDisposition
		c.mu.Unlock()
		if got != mqttWillArmed {
			t.Fatalf("the Will is already %v before the disposal ran: this ordering collapsed into the other one", got)
		}

		fence := e.sess.mqttCaptureWillFence()
		e.sess.mu.Lock()
		e.sess.gen = mqttNewSessGen()
		e.sess.seq++
		e.sess.mu.Unlock()

		// ARMED here, so no close was separately observed and this consumption IS the
		// close: the anchor is now, and the [lo, hi] bracket is the right check. Passing
		// zero is the assertion, not a fallback.
		lo := time.Now()
		c.mqttConsumeWillWithFence(e.asm, e.sess, fence)
		hi := time.Now()
		assertOutbox(t, e, fence, 0, lo, hi)
		before, _, _ := e.asm.jsa.loadWillOutbox(e.asm.domainTk,
			mqttWillQClientFilter(e.asm.domainTk, getHash(e.clientID)))

		// Now let the write time out and the teardown follow. It must NOT produce a
		// second obligation: mqttTakeWillLocked consumed the Will once already, and
		// the disposition staying discharged is what proves the teardown could not
		// take it again.
		<-blocked
		testJoinTeardown(t, e.s, "disposal-first")
		assertDischarged(t, c, "disposal-first")
		after, _, _ := e.asm.jsa.loadWillOutbox(e.asm.domainTk,
			mqttWillQClientFilter(e.asm.domainTk, getHash(e.clientID)))
		if before == nil || after == nil || before.WillID != after.WillID {
			t.Fatalf("the teardown enqueued a SECOND Will after the disposal had already taken it: %+v then %+v", before, after)
		}
	})
}

// The shutdown cap must be captured BEFORE the writer is entered, so that time spent
// waiting on wmu is spent out of the drain's budget rather than being forgiven. This
// is the case the uncontended cap tests cannot see: with no contender, "computed
// before entering" and "computed after acquiring wmu" produce the same instant.
func TestF035ShutdownCapIsCapturedBeforeContending(t *testing.T) {
	testShortWriteDeadline(t, 2*time.Second)
	s := &Server{opts: &Options{}, clients: map[uint64]*client{}}
	c, sc := testDrainableClient(t, s)
	s.clients[1] = c

	// An incumbent holds wmu and will lose it to its own timeout, so the drain's
	// write waits a real, measurable interval before it can install anything.
	incumbent := make(chan struct{})
	go func() { _ = c.writeSocket([]byte("incumbent"), time.Time{}); close(incumbent) }()
	sc.awaitEntry(t, "the incumbent write")

	// The conn stays UNREADABLE throughout, deliberately: making it writable here
	// would let the incumbent complete at once and there would be no wmu wait to
	// measure. The drain's own write then also times out, which does not matter —
	// the assertion is on the deadline it INSTALLED, not on the write succeeding.
	//
	// A drain budget LONGER than mqttWriteDeadline, so the cap is the write interval
	// rather than the budget, and the question is only when that interval started.
	start := time.Now()
	s.drainClients(start.Add(time.Hour))
	<-incumbent

	// The LAST non-zero deadline is the drain's install: each writer installs one and
	// clears it, and the drain wrote last. Indexing by position instead would pick up
	// the incumbent's CLEAR, whose zero time compares as earlier than anything and
	// would make this test pass against the very defect it exists to catch.
	all := sc.allDeadlines()
	var drainDeadline time.Time
	for i := len(all) - 1; i >= 0; i-- {
		if !all[i].IsZero() {
			drainDeadline = all[i]
			break
		}
	}
	if drainDeadline.IsZero() {
		t.Fatal("the drain installed no deadline of its own")
	}
	// Captured before contending, the cap is at most start+mqttWriteDeadline.
	// Computed after wmu was won, it would be roughly the incumbent's timeout later.
	if drainDeadline.After(start.Add(mqttWriteDeadline + 200*time.Millisecond)) {
		t.Fatalf("the drain's write got a deadline of %v, more than mqttWriteDeadline after the drain step began at %v: the cap was computed AFTER the wmu wait, so contention silently renewed the budget", drainDeadline, start)
	}
}
