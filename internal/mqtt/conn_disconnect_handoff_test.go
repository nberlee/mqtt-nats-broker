//go:build mqtt_fulltests

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

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The inbound pipeline's failure position hands its close to the reader: a client still sending
// when an ordinary backend failure ends its connection reads the server's DISCONNECT, because the
// reader drains what it sends, within the linger's bounds, before the close.

// handoffFix is one v5 client whose queued $MQTT_msgs store of identifier 1 is held and then
// fails, so the failure reaches the pipeline worker's failure position, not the reader.
type handoffFix struct {
	t    *testing.T
	s    *Server
	o    *Options
	ws   bool
	cid  string
	conn net.Conn

	store *inboundGate // the held store of identifier 1, released as a failure

	mu       sync.Mutex
	phases   []string
	failing  atomic.Bool // the failure position has started (before its DISCONNECT)
	failPark *inboundGate
	drained  chan struct{}
	drainOne sync.Once
}

var errHandoffStore = errors.New("handoff test: store failed")

func newHandoffFix(t *testing.T, ws bool, cid string, opt func(*Options)) *handoffFix {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	if ws {
		o.MQTT.Websocket = WebsocketOpts{Host: "127.0.0.1", Port: -1}
	}
	if opt != nil {
		opt(o)
	}
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	f := &handoffFix{t: t, s: s, o: o, ws: ws, cid: cid, drained: make(chan struct{})}
	f.store = newInboundGate()
	t.Cleanup(f.store.release)
	subj := f.subject(1)
	setInboundHook(t, &mqttTestStoreMsgResultHookV, func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if subject == subj {
			f.store.park(nil)
			return nil, errHandoffStore
		}
		return resp, err
	})
	setInboundHook(t, &mqttTestInboundFailV, func(c *client, phase string) {
		if !f.mine(c) {
			return
		}
		f.mu.Lock()
		f.phases = append(f.phases, phase)
		park := f.failPark
		f.mu.Unlock()
		if phase == "before-claim" {
			f.failing.Store(true)
			if park != nil {
				park.park(nil)
			}
		}
	})
	setInboundHook(t, &mqttTestHandoffDrainV, func(c *client) {
		if f.mine(c) {
			f.drainOne.Do(func() { close(f.drained) })
		}
	})
	return f
}

func (f *handoffFix) mine(c *client) bool { return c.mqtt != nil && c.mqtt.cid == f.cid }

func (f *handoffFix) topic(pi uint16) string { return "ho/" + f.cid + "/" + strconv.Itoa(int(pi)) }

func (f *handoffFix) subject(pi uint16) string {
	return mqttStreamSubjectPrefix + "ho." + f.cid + "." + strconv.Itoa(int(pi))
}

func (f *handoffFix) pub(pi uint16) []byte {
	return inboundAckBenchPublish(1, f.topic(pi), pi, []byte("handoff"))
}

// parkFailure makes the failure position park after p.failed and before its DISCONNECT.
func (f *handoffFix) parkFailure() *inboundGate {
	g := newInboundGate()
	f.t.Cleanup(g.release)
	f.mu.Lock()
	f.failPark = g
	f.mu.Unlock()
	return g
}

// dial connects the client over the fixture's transport and reads a success CONNACK.
func (f *handoffFix) dial(props []byte) {
	f.t.Helper()
	ci := &mqttV5ConnInfo{clientID: f.cid, cleanStart: true, props: props}
	var c net.Conn
	if f.ws {
		c = wsTestDial(f.t, f.s.WSAddr().String())
	} else {
		var err error
		if c, err = net.Dial("tcp", net.JoinHostPort(f.o.MQTT.Host, strconv.Itoa(f.o.MQTT.Port))); err != nil {
			f.t.Fatalf("dial: %v", err)
		}
	}
	f.t.Cleanup(func() { c.Close() })
	if _, err := c.Write(mqttV5CreateConnect(ci)); err != nil {
		f.t.Fatalf("CONNECT: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	b, body, err := testLingerReadPacket(c)
	if err != nil || b&mqttPacketMask != mqttPacketConnectAck || len(body) < 2 || body[1] != 0 {
		f.t.Fatalf("CONNACK: type %#x body %v err %v", b, body, err)
	}
	f.conn = c
}

// queueBehindHeldStore sends identifiers 1 and 2 in one write, so 1 is queued rather than run
// inline (another packet is buffered behind it), and waits for its store to be held. When idle is
// set it also waits until the reader is back in a socket Read after dispatching both.
func (f *handoffFix) queueBehindHeldStore(idle bool) {
	f.t.Helper()
	var dispatched atomic.Bool
	reading := make(chan struct{})
	var once sync.Once
	if idle {
		setInboundHook(f.t, &mqttTestInboundSwitchV, func(c *client, _ mqttInboundKind, pi uint16, _ bool) {
			if f.mine(c) && pi == 2 {
				dispatched.Store(true)
			}
		})
		setInboundHook(f.t, &mqttTestReaderReadV, func(c *client) {
			if f.mine(c) && dispatched.Load() {
				once.Do(func() { close(reading) })
			}
		})
	}
	if _, err := f.conn.Write(append(f.pub(1), f.pub(2)...)); err != nil {
		f.t.Fatalf("publish: %v", err)
	}
	f.store.wait(f.t, "the store of identifier 1")
	if idle {
		select {
		case <-reading:
		case <-time.After(10 * time.Second):
			f.t.Fatal("the reader never returned to a socket Read")
		}
	}
}

func (f *handoffFix) handedOver() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.phases {
		if p == "handed-over" {
			return true
		}
	}
	return false
}

// writer sends QoS 1 PUBLISHes every pace until stopped, and reports the first write error.
type handoffWriter struct {
	stop chan struct{}
	done chan error
}

func (f *handoffFix) startWriting(pace time.Duration) *handoffWriter {
	w := &handoffWriter{stop: make(chan struct{}), done: make(chan error, 1)}
	go func() {
		pi := uint16(100)
		for {
			select {
			case <-w.stop:
				w.done <- nil
				return
			default:
			}
			_ = f.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := f.conn.Write(f.pub(pi)); err != nil {
				w.done <- err
				return
			}
			pi++
			if pi == 0 {
				pi = 100
			}
			time.Sleep(pace)
		}
	}()
	return w
}

func (w *handoffWriter) end() error {
	close(w.stop)
	return <-w.done
}

// readDisconnect reads until the server's DISCONNECT and returns its reason.
func (f *handoffFix) readDisconnect() (byte, error) {
	_ = f.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		b, body, err := testLingerReadPacket(f.conn)
		if err != nil {
			return 0, fmt.Errorf("read before the DISCONNECT: %w", err)
		}
		if b&mqttPacketMask == mqttPacketDisconnect {
			if len(body) == 0 {
				return mqttReasonSuccess, nil
			}
			return body[0], nil
		}
	}
}

// stillOpen reports an error unless the server keeps the connection open for d: a read that
// times out, rather than returning EOF or a reset, while the client keeps sending.
func (f *handoffFix) stillOpen(d time.Duration) error {
	_ = f.conn.SetReadDeadline(time.Now().Add(d))
	b := make([]byte, 64)
	n, err := f.conn.Read(b)
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return nil
	}
	return fmt.Errorf("the server did not keep the connection open after its DISCONNECT (%d bytes, %v)", n, err)
}

func (f *handoffFix) awaitDrain() {
	f.t.Helper()
	select {
	case <-f.drained:
	case <-time.After(10 * time.Second):
		f.t.Fatal("the reader never started the handed-over drain")
	}
}

func (f *handoffFix) requireHandedOver() {
	f.t.Helper()
	if !f.handedOver() {
		f.t.Fatal("fixture: the failure never reached the pipeline worker's handoff")
	}
}

// expectStillWritingClientReadsDisconnect: the client, writing from writer (started already, or
// started here after the DISCONNECT when nil), reads a DISCONNECT with the failure's reason, no
// write fails, and the server keeps the connection open while the client keeps sending.
func (f *handoffFix) expectStillWritingClientReadsDisconnect(w *handoffWriter) {
	f.t.Helper()
	reason, rerr := f.readDisconnect()
	if w == nil {
		w = f.startWriting(time.Millisecond)
	}
	oerr := f.stillOpen(300 * time.Millisecond)
	werr := w.end()
	if rerr != nil {
		f.t.Fatalf("the client lost the server's DISCONNECT: %v (write error: %v)", rerr, werr)
	}
	if reason != mqttReasonUnspecifiedError {
		f.t.Fatalf("DISCONNECT reason %#x, want the store failure's %#x", reason, mqttReasonUnspecifiedError)
	}
	if werr != nil {
		f.t.Fatalf("a send failed around the server's DISCONNECT: %v", werr)
	}
	if oerr != nil {
		f.t.Fatal(oerr)
	}
	f.requireHandedOver()
	f.awaitDrain()
}

func handoffTransports(t *testing.T, fn func(t *testing.T, ws bool)) {
	t.Run("tcp", func(t *testing.T) { fn(t, false) })
	t.Run("websocket", func(t *testing.T) { fn(t, true) })
}

// TestMQTTv5PipelineFailureDisconnectReachesAClientStillWriting: a client pipelining QoS 1
// PUBLISHes about every millisecond when a queued store fails reads the server's DISCONNECT, with
// no send failing, on TCP and on WebSocket.
func TestMQTTv5PipelineFailureDisconnectReachesAClientStillWriting(t *testing.T) {
	handoffTransports(t, func(t *testing.T, ws bool) {
		for it := 0; it < 10; it++ {
			f := newHandoffFix(t, ws, fmt.Sprintf("howrite%d", it), nil)
			f.dial(nil)
			f.queueBehindHeldStore(false)
			w := f.startWriting(time.Millisecond)
			time.Sleep(20 * time.Millisecond)
			f.store.release()
			f.expectStillWritingClientReadsDisconnect(w)
			f.conn.Close()
			inboundAwaitClientGone(t, f.s, f.cid)
		}
	})
}

// TestMQTTv5PipelineFailureDisconnectAtEachReaderPosition: the failure lands while the reader is
// (a) in a socket Read and (b) blocked waiting for space in the pipeline; either way the client,
// sending after the DISCONNECT, reads it with no send failing.
func TestMQTTv5PipelineFailureDisconnectAtEachReaderPosition(t *testing.T) {
	handoffTransports(t, func(t *testing.T, ws bool) {
		t.Run("socket read", func(t *testing.T) {
			f := newHandoffFix(t, ws, "hoposread", nil)
			f.dial(nil)
			f.queueBehindHeldStore(true)
			f.store.release()
			f.expectStillWritingClientReadsDisconnect(nil)
		})
		t.Run("pipeline wait", func(t *testing.T) {
			mqttInboundWindow.Set(2)
			defer mqttInboundWindow.Set(1024)
			f := newHandoffFix(t, ws, "hoposwait", nil)
			f.dial(nil)
			waiting := make(chan struct{})
			var once sync.Once
			setInboundHook(t, &mqttTestInboundWaitV, func(c *client, which string) {
				if f.mine(c) && which == "space" {
					once.Do(func() { close(waiting) })
				}
			})
			if _, err := f.conn.Write(append(append(f.pub(1), f.pub(2)...), f.pub(3)...)); err != nil {
				t.Fatalf("publish: %v", err)
			}
			f.store.wait(t, "the store of identifier 1")
			select {
			case <-waiting:
			case <-time.After(10 * time.Second):
				t.Fatal("the reader never waited for space in the pipeline")
			}
			f.store.release()
			f.expectStillWritingClientReadsDisconnect(nil)
		})
	})
}

// TestMQTTv5PipelineFailureHandoffIsBounded: a client that reads the DISCONNECT and then stays
// silent with its side open (no keep-alive) is closed once the bound passes.
func TestMQTTv5PipelineFailureHandoffIsBounded(t *testing.T) {
	mqttDisconnectLinger.Set(time.Second)
	defer mqttDisconnectLinger.Set(5 * time.Second)
	f := newHandoffFix(t, false, "hobound", nil)
	f.dial(nil)
	f.queueBehindHeldStore(true)
	f.store.release()
	if _, err := f.readDisconnect(); err != nil {
		t.Fatal(err)
	}
	if d := testLingerAwaitServerClose(t, f.conn, 5*time.Second); d > 1500*time.Millisecond {
		t.Fatalf("the server closed %v after the DISCONNECT, past its 1 s bound", d)
	}
	f.requireHandedOver()
}

// TestMQTTv5PipelineFailureNothingAfterItIsParsed: what the client sends after the failure's
// DISCONNECT is never interpreted: no packet of it is dispatched, a QoS 0 PUBLISH is not
// published, and a DISCONNECT ending its session (expiry 0) does not end it.
func TestMQTTv5PipelineFailureNothingAfterItIsParsed(t *testing.T) {
	f := newHandoffFix(t, false, "hoparse", nil)
	ws, wr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "howatch", cleanStart: true}, f.o.MQTT.Host, f.o.MQTT.Port)
	defer ws.Close()
	testMQTTReadConnAckV5(t, wr)
	testMQTTSubV5(t, ws, wr, 1, []mqttV5SubFilter{{topic: "ho/watched", opts: 0}})
	f.dial(mqttV5ConnPropsSessionExpiry(60))
	f.queueBehindHeldStore(true)
	var parsed atomic.Int32
	setInboundHook(t, &mqttTestInboundDispatchV, func(c *client, _ byte, _ bool, _ bool) {
		if f.mine(c) && f.failing.Load() {
			parsed.Add(1)
		}
	})
	f.store.release()
	if _, err := f.readDisconnect(); err != nil {
		t.Fatal(err)
	}
	testMQTTPublishV5(t, f.conn, 0, 0, "ho/watched", []byte("after the DISCONNECT"))
	sub := []byte{mqttPacketSub | 0x2, 0}
	body := []byte{0, 7, 0} // identifier 7, no properties
	body = append(body, wsMQTTString("ho/other")...)
	body = append(body, 0)
	sub[1] = byte(len(body))
	if _, err := f.conn.Write(append(sub, body...)); err != nil {
		t.Fatalf("SUBSCRIBE: %v", err)
	}
	if _, err := f.conn.Write(mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 0)); err != nil {
		t.Fatalf("DISCONNECT: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	f.conn.Close()
	f.requireHandedOver()
	f.awaitDrain()
	inboundAwaitClientGone(t, f.s, f.cid)
	if n := parsed.Load(); n != 0 {
		t.Fatalf("%d packets sent after the failure were dispatched", n)
	}
	if _, _, ok := testMQTTReadPacketReady(wr, 500*time.Millisecond); ok {
		t.Fatal("a PUBLISH sent after the failure's DISCONNECT was published")
	}
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: f.cid, props: mqttV5ConnPropsSessionExpiry(60)}, f.o.MQTT.Host, f.o.MQTT.Port)
	defer c2.Close()
	if sp, _, _ := testMQTTReadConnAckV5(t, r2); !sp {
		t.Fatal("the session ended: a DISCONNECT sent after the failure's DISCONNECT was acted on")
	}
}

// TestMQTTv5PipelineFailureReaderExitBeforeTheWriteStillDrains: the reader leaves its parse on the
// failed pipe before the failure position has written anything (the failure parked before its
// DISCONNECT); it joins the pipe, so it still sees the handoff and drains.
func TestMQTTv5PipelineFailureReaderExitBeforeTheWriteStillDrains(t *testing.T) {
	f := newHandoffFix(t, false, "hoexit", nil)
	park := f.parkFailure()
	f.dial(nil)
	f.queueBehindHeldStore(true)
	sc := clientByID(t, f.s, f.cid)
	f.store.release()
	park.wait(t, "the failure position")
	if _, err := f.conn.Write([]byte{mqttPacketPing, 0}); err != nil {
		t.Fatalf("PINGREQ: %v", err)
	}
	// Only the reader's own exit fires the latch while the failure is parked before its write.
	checkFor(t, 10*time.Second, 2*time.Millisecond, func() error {
		if !mqttLatchFired(sc.mqtt.abortLatch()) {
			return errors.New("the reader has not left its parse")
		}
		return nil
	})
	park.release()
	f.expectStillWritingClientReadsDisconnect(nil)
}

// TestMQTTv5PipelineFailurePreventedDoesNotHandOff: a DISCONNECT the sealed output gate prevented
// hands nothing over: the connection closes at once.
func TestMQTTv5PipelineFailurePreventedDoesNotHandOff(t *testing.T) {
	f := newHandoffFix(t, false, "hoprev", nil)
	f.dial(nil)
	f.queueBehindHeldStore(true)
	sealOutput(clientByID(t, f.s, f.cid).outBind)
	f.store.release()
	if d := testLingerAwaitServerClose(t, f.conn, 4*time.Second); d > time.Second {
		t.Fatalf("a connection whose DISCONNECT was prevented stayed open %v", d)
	}
	if f.handedOver() {
		t.Fatal("a prevented DISCONNECT handed the close over")
	}
}

// TestMQTTv5PipelineFailureDisconnectIsTheLastFrame: the failure's DISCONNECT is written and the
// output gate sealed holding the socket's write lock, so no writer can put a frame after it.
func TestMQTTv5PipelineFailureDisconnectIsTheLastFrame(t *testing.T) {
	f := newHandoffFix(t, false, "holast", nil)
	var unlocked, staged atomic.Int32
	h := func(c *client) {
		if !f.mine(c) || !f.failing.Load() || staged.Add(1) != 1 {
			return
		}
		// THE ORACLE: the write lock is held across the write and the seal.
		if c.wmu.TryLock() {
			c.wmu.Unlock()
			unlocked.Add(1)
		}
		// Secondary: a writer staged at the lock finds the gate sealed when it gets there.
		done := make(chan struct{})
		go func() {
			c.queueOutbound([]byte{mqttPacketPingResp, 0})
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
		}
	}
	mqttTestAfterSealingWriteV.Store(&h)
	defer mqttTestAfterSealingWriteV.Store(nil)
	f.dial(nil)
	f.queueBehindHeldStore(true)
	f.store.release()
	if _, err := f.readDisconnect(); err != nil {
		t.Fatal(err)
	}
	if staged.Load() == 0 {
		t.Fatal("fixture: the failure's DISCONNECT was not a sealing write")
	}
	if unlocked.Load() != 0 {
		t.Fatal("the socket's write lock was released between the DISCONNECT and the seal")
	}
	_ = f.conn.SetReadDeadline(time.Now().Add(time.Second))
	if b, _, err := testLingerReadPacket(f.conn); err == nil {
		t.Fatalf("a packet of type %#x followed the failure's DISCONNECT", b&mqttPacketMask)
	}
}

// handoffAfterFailureSequence parks the reader at its own error (match), lets the earlier queued
// failure reach the failure position and park before its DISCONNECT, releases the reader, and,
// once the reader has left its parse (it fired the latch), the failure position.
func (f *handoffFix) handoffAfterFailureSequence(send []byte, match func(error) bool) {
	f.t.Helper()
	park := f.parkFailure()
	reader := newInboundGate()
	f.t.Cleanup(reader.release)
	var once sync.Once
	setInboundHook(f.t, &mqttTestReadLoopErrorV, func(c *client, err error) {
		if f.mine(c) && match(err) {
			once.Do(func() { reader.park(nil) })
		}
	})
	sc := clientByID(f.t, f.s, f.cid)
	if _, err := f.conn.Write(send); err != nil {
		f.t.Fatalf("send: %v", err)
	}
	reader.wait(f.t, "the reader's own error")
	f.store.release()
	park.wait(f.t, "the failure position")
	reader.release()
	checkFor(f.t, 10*time.Second, 2*time.Millisecond, func() error {
		if !mqttLatchFired(sc.mqtt.abortLatch()) {
			return errors.New("the reader has not left its parse")
		}
		return nil
	})
	park.release()
}

// expectOneDisconnect: exactly one DISCONNECT reaches the client, with the failure's reason.
func (f *handoffFix) expectOneDisconnect() {
	f.t.Helper()
	reason, err := f.readDisconnect()
	if err != nil {
		f.t.Fatal(err)
	}
	if reason != mqttReasonUnspecifiedError {
		f.t.Fatalf("DISCONNECT reason %#x, want the earlier failure's %#x", reason, mqttReasonUnspecifiedError)
	}
	_ = f.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if b, _, err := testLingerReadPacket(f.conn); err == nil {
		f.t.Fatalf("a packet of type %#x followed the DISCONNECT", b&mqttPacketMask)
	}
	f.requireHandedOver()
}

// TestMQTTv5PipelineFailureOwnsTheDisconnectOverALaterMalformedPacket: a malformed packet the
// reader finds after a queued failure does not send its own DISCONNECT: the failure precedes it,
// and its DISCONNECT is the one the client reads.
func TestMQTTv5PipelineFailureOwnsTheDisconnectOverALaterMalformedPacket(t *testing.T) {
	f := newHandoffFix(t, false, "homalformed", nil)
	f.dial(nil)
	f.queueBehindHeldStore(true)
	// A PUBREL with fixed-header flags 0: malformed.
	f.handoffAfterFailureSequence([]byte{mqttPacketPubRel, 2, 0, 9}, func(err error) bool {
		return errors.Is(err, errMQTTMalformedPacket)
	})
	f.expectOneDisconnect()
}

// TestMQTTv5PipelineFailureOwnsTheDisconnectOverALaterOversizedPacket: the same for a packet over
// the Maximum Packet Size: no 0x95, and no close underneath the failure position.
func TestMQTTv5PipelineFailureOwnsTheDisconnectOverALaterOversizedPacket(t *testing.T) {
	f := newHandoffFix(t, false, "hooversized", func(o *Options) { o.MaxPayload = 1024 })
	f.dial(nil)
	f.queueBehindHeldStore(true)
	f.handoffAfterFailureSequence(inboundAckBenchPublish(0, "ho/big", 0, make([]byte, 4096)), func(err error) bool {
		return err == ErrMaxPayload
	})
	f.expectOneDisconnect()
}

// handoffSendPastCeiling writes more than the drain's payload ceiling, and then expects the server
// to close within 3 s: the drain's own close, since the bound is far longer.
func handoffSendPastCeiling(t *testing.T, c net.Conn) {
	t.Helper()
	chunk := make([]byte, 64*1024)
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	for sent := 0; sent < 2*mqttDisconnectLingerBytes; sent += len(chunk) {
		if _, err := c.Write(chunk); err != nil {
			break // already closed
		}
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	b := make([]byte, 4096)
	for {
		_, err := c.Read(b)
		if err == nil {
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("the socket stayed open after the drain reached its ceiling")
		}
		return
	}
}

// parkTeardown holds cid's teardown at its entry until the test ends.
func parkTeardown(t *testing.T, cid string) {
	g := newInboundGate()
	t.Cleanup(g.release)
	setInboundHook(t, &mqttTestClosedClientEntryV, func(c *client) {
		if c.mqtt != nil && c.mqtt.cid == cid {
			g.park(nil)
		}
	})
}

// TestMQTTv5ErrorDisconnectDrainEndsInAClose: a drain that stops at its payload ceiling closes the
// socket itself, whatever the teardown after it does; a teardown that waits does not keep a socket
// nobody reads open. Both arms: the pipeline worker's handoff and the reader's own linger.
func TestMQTTv5ErrorDisconnectDrainEndsInAClose(t *testing.T) {
	mqttDisconnectLinger.Set(30 * time.Second)
	defer mqttDisconnectLinger.Set(5 * time.Second)
	t.Run("pipeline worker", func(t *testing.T) {
		f := newHandoffFix(t, false, "hoceil", nil)
		f.dial(nil)
		f.queueBehindHeldStore(true)
		parkTeardown(t, f.cid)
		f.store.release()
		if _, err := f.readDisconnect(); err != nil {
			t.Fatal(err)
		}
		handoffSendPastCeiling(t, f.conn)
		f.requireHandedOver()
	})
	t.Run("reader", func(t *testing.T) {
		_, o := testLingerServer(t, 4)
		c := testLingerViolate(t, o, "lgceil", 5, nil)
		parkTeardown(t, "lgceil")
		if _, err := testLingerAcknowledgeToDisconnect(c, 0); err != nil {
			t.Fatalf("fixture: %v", err)
		}
		handoffSendPastCeiling(t, c)
	})
}
