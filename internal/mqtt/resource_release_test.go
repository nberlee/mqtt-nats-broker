// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

// Resources that must be released by the path that ends them: a shutdown registry entry
// whose retry ended, a subscription installed after its connection closed, an accept loop
// that met an error, and the pending-binding buckets of a persistent session. See
// docs/findings/resource-lifetime.md for the census these come from.

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// owedDrainCount reports how many shutdown-registry drains name subject.
func owedDrainCount(s *Server, subject string) int {
	s.owedDrainsMu.Lock()
	defer s.owedDrainsMu.Unlock()
	n := 0
	for _, o := range s.owedDrains {
		if o.tok.slot.name.subject == subject {
			n++
		}
	}
	return n
}

// A drain retry that finds its generation gone ends without another attempt, so the
// shutdown registry entry naming it must go with it: nothing else would ever clear it, and it
// keeps the session reachable until shutdown. Both exits — the one taken when the retry is
// scheduled and the one taken when it fires.
func TestOwedDrainClearedOnStaleGeneration(t *testing.T) {
	t.Parallel()
	t.Run("at schedule", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "owed-drain-stale-schedule")
		e.sess.mu.Lock()
		gen := e.sess.gen
		e.sess.mu.Unlock()
		tok := mqttSendToken{token: "tok-schedule", actionHeld: true}
		tok.slot.name = mqttInflightName{gen: gen + "-discarded", pi: 7, subject: "test.owed.schedule"}
		e.s.noteOwedDrain(e.sess, tok)
		if owedDrainCount(e.s, tok.slot.name.subject) != 1 {
			t.Fatal("the fixture did not register the owed drain")
		}
		e.sess.schedulePublishDrainReconcile(e.c, tok)
		if n := owedDrainCount(e.s, tok.slot.name.subject); n != 0 {
			t.Fatalf("a stale-generation retry left %d registry entries", n)
		}
	})
	t.Run("at fire", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "owed-drain-stale-fire")
		e.sess.mu.Lock()
		gen := e.sess.gen
		e.sess.mu.Unlock()
		tok := mqttSendToken{token: "tok-fire", actionHeld: true}
		tok.slot.name = mqttInflightName{gen: gen, pi: 8, subject: "test.owed.fire"}
		e.s.noteOwedDrain(e.sess, tok)
		e.sess.schedulePublishDrainReconcile(e.c, tok)
		// The generation moves while the retry waits: what a clear's re-mint does.
		e.sess.mu.Lock()
		e.sess.gen = gen + "-reminted"
		e.sess.mu.Unlock()
		deadline := time.Now().Add(mqttActionReconcileDelay + 5*time.Second)
		for owedDrainCount(e.s, tok.slot.name.subject) != 0 {
			if time.Now().After(deadline) {
				t.Fatal("the retry fired on a moved generation and left its registry entry")
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
}

// subscribeAfterCloseFixture is a client on a real service connection, able to create
// $MQTT. subscriptions the way the broker does for a QoS 1/2 deliver subject.
func subscribeAfterCloseFixture(t *testing.T) (*client, *nats.Conn) {
	t.Helper()
	_, nc := runNATS(t)
	s := &Server{nrc: nc, opts: &Options{}}
	c := &client{srv: s, nrc: nc, acc: s.globalAccount()}
	return c, nc
}

func noopMsgHandler(*subscription, *client, *Account, string, string, []byte) {}

// A subscription a closed client creates is unsubscribed by that client, not installed.
// closeConnection sweeps c.subs once; an entry installed after the sweep would ride the
// service connection with no owner left to remove it.
func TestSubscribeAfterCloseLeavesNoSubscription(t *testing.T) {
	t.Parallel()
	c, nc := subscribeAfterCloseFixture(t)
	before := nc.NumSubscriptions()
	c.closeConnection(ClientClosed)
	_, _, err := c.processSub([]byte(mqttSubPrefix+"after-close"), nil, []byte("sid-1"), noopMsgHandler, false)
	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("subscribing on a closed client returned %v, want ErrConnectionClosed", err)
	}
	c.mu.Lock()
	n := len(c.subs)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("a closed client holds %d subscriptions", n)
	}
	if got := nc.NumSubscriptions(); got != before {
		t.Fatalf("the service connection holds %d subscriptions, %d before: one outlived its client", got, before)
	}
}

// The same, with the close landing INSIDE the window: after processSub created the NATS
// subscription and before it installs it. That is the interleaving the sweep alone cannot
// cover.
func TestSubscribeRacingCloseLeavesNoSubscription(t *testing.T) {
	c, nc := subscribeAfterCloseFixture(t)
	before := nc.NumSubscriptions()
	parked, release := make(chan struct{}), make(chan struct{})
	hook := func(hc *client) {
		if hc != c {
			return
		}
		close(parked)
		<-release
	}
	mqttTestProcessSubBeforeInstallV.Store(&hook)
	t.Cleanup(func() { mqttTestProcessSubBeforeInstallV.Store(nil) })

	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		_, _, err := c.processSub([]byte(mqttSubPrefix+"racing-close"), nil, []byte("sid-2"), noopMsgHandler, false)
		done <- result{err}
	}()
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("processSub never reached the install window")
	}
	// The close runs to completion while the subscription exists but is not installed.
	c.closeConnection(ClientClosed)
	close(release)
	var r result
	select {
	case r = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processSub did not return")
	}
	if !errors.Is(r.err, ErrConnectionClosed) {
		t.Fatalf("an install after the close returned %v, want ErrConnectionClosed", r.err)
	}
	c.mu.Lock()
	n := len(c.subs)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("the install landed after the sweep: %d subscriptions held by a closed client", n)
	}
	if got := nc.NumSubscriptions(); got != before {
		t.Fatalf("the service connection holds %d subscriptions, %d before", got, before)
	}
}

// fakeAcceptError is what Accept returns on descriptor exhaustion: temporary, not a timeout.
type fakeAcceptError struct {
	temporary bool
}

func (e fakeAcceptError) Error() string   { return "accept: " + syscall.EMFILE.Error() }
func (e fakeAcceptError) Timeout() bool   { return false }
func (e fakeAcceptError) Temporary() bool { return e.temporary }

// scriptedListener returns its scripted errors in order, then one connection, then `after`
// on every call (nil: block until closed).
type scriptedListener struct {
	mu      sync.Mutex
	errs    []error
	conn    net.Conn
	after   error
	closed  chan struct{}
	once    sync.Once
	accepts []time.Time
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	l.accepts = append(l.accepts, time.Now())
	if len(l.errs) > 0 {
		err := l.errs[0]
		l.errs = l.errs[1:]
		l.mu.Unlock()
		return nil, err
	}
	if c := l.conn; c != nil {
		l.conn = nil
		l.mu.Unlock()
		return c, nil
	}
	after := l.after
	l.mu.Unlock()
	if after != nil {
		return nil, after
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *scriptedListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *scriptedListener) Addr() net.Addr { return &net.TCPAddr{} }

// An Accept error while the server runs is retried, not fatal: the loop used to return on any
// error that was not a timeout, which left the listener open with nobody accepting. A
// persistent non-temporary error backs off too, and shutdown still ends the loop from inside
// its backoff wait.
func TestAcceptLoopSurvivesErrors(t *testing.T) {
	t.Parallel()
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	l := &scriptedListener{
		errs: []error{
			fakeAcceptError{temporary: true}, fakeAcceptError{temporary: true}, fakeAcceptError{temporary: true},
			errors.New("persistent accept failure"), errors.New("persistent accept failure"),
		},
		conn:   server,
		after:  errors.New("still failing"),
		closed: make(chan struct{}),
	}
	s := &Server{opts: &Options{}, quitCh: make(chan struct{})}
	s.running.Store(true)
	accepted := make(chan net.Conn, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		s.acceptConnections(l, "test", func(c net.Conn) { accepted <- c })
	}()
	select {
	case c := <-accepted:
		if c != server {
			t.Fatal("the accepted connection is not the scripted one")
		}
	case <-exited:
		t.Fatal("the accept loop exited on an error while the server was running")
	case <-time.After(10 * time.Second):
		t.Fatal("the scripted connection was never accepted")
	}
	l.mu.Lock()
	times := append([]time.Time(nil), l.accepts...)
	l.mu.Unlock()
	if len(times) < 6 {
		t.Fatalf("%d Accept calls, want at least 6", len(times))
	}
	// Five errors, so four gaps between failed attempts: each must be a backoff, and the
	// backoff doubles, so the last gap is clearly larger than the first.
	first, last := times[1].Sub(times[0]), times[5].Sub(times[4])
	if first < mqttAcceptMinBackoff/2 || last < 4*mqttAcceptMinBackoff {
		t.Fatalf("no backoff between failed accepts: first gap %v, last gap %v", first, last)
	}

	// Shutdown from inside a backoff wait: the listener keeps failing, so the loop is backing
	// off (by now at its cap). Stopping the server ends it at once, not at the next attempt.
	time.Sleep(3 * mqttAcceptMaxBackoff / 2)
	s.running.Store(false)
	stopped := time.Now()
	close(s.quitCh)
	_ = l.Close()
	select {
	case <-exited:
		if d := time.Since(stopped); d > mqttAcceptMaxBackoff/2 {
			t.Fatalf("the accept loop took %v to notice shutdown: its backoff wait ignores quitCh", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the accept loop did not exit after shutdown")
	}
}

// The pending-binding buckets are reachable only through their type's methods, so no caller
// can hold a bucket and leave it empty: every production `.cpending` selector outside the
// type's own file is either the receiver of a method call or the target of a reset
// assignment. An index, a range or a len on it is the alias this guard exists to forbid.
func TestConsumerPendingIsTouchedOnlyThroughItsMethods(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	seen := 0
	for _, name := range censusProductionFiles(t) {
		if name == "qos_consumer_pending.go" {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		allowed := map[*ast.SelectorExpr]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			switch d := n.(type) {
			case *ast.CallExpr:
				if fsel, ok := d.Fun.(*ast.SelectorExpr); ok {
					if recv, ok := fsel.X.(*ast.SelectorExpr); ok && recv.Sel.Name == "cpending" {
						allowed[recv] = true
					}
				}
			case *ast.AssignStmt:
				for _, lhs := range d.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "cpending" {
						allowed[sel] = true
					}
				}
			}
			return true
		})
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "cpending" {
				return true
			}
			seen++
			if !allowed[sel] {
				t.Errorf("%s: .cpending used directly rather than through mqttConsumerPending's methods",
					fset.Position(sel.Pos()))
			}
			return true
		})
	}
	if seen < 10 {
		t.Fatalf("only %d .cpending selectors found: the guard is not reading the code it guards", seen)
	}
}

// The type itself: a bucket exists exactly while it holds an entry, whichever mutator
// removed the last one.
func TestConsumerPendingDropsAnEmptiedBucket(t *testing.T) {
	t.Parallel()
	var p mqttConsumerPending
	p.put("d1", 1, 10)
	p.put("d1", 2, 11)
	p.put("d2", 5, 12)
	p.deleteSeq("d1", 1)
	if len(p["d1"]) == 0 {
		t.Fatal("a bucket with an entry left was dropped")
	}
	if p.deleteIf("d1", 2, 99) {
		t.Fatal("deleteIf removed an entry naming another identifier")
	}
	if !p.deleteIf("d1", 2, 11) {
		t.Fatal("deleteIf did not remove its exact entry")
	}
	if _, ok := p["d1"]; ok {
		t.Fatal("deleteIf left an empty bucket")
	}
	p.deleteSeq("d2", 5)
	if len(p) != 0 {
		t.Fatalf("deleteSeq left %d empty bucket(s)", len(p))
	}
	p.put("d3", 1, 1)
	cp := p.entries("d3")
	delete(cp, 1)
	if len(p["d3"]) == 0 {
		t.Fatal("entries handed out the live bucket, not a copy")
	}
}
