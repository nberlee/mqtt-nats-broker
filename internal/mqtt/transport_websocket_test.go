package mqtt

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// wsTestFrame is one client frame as a test wants it on the wire, including the malformed
// shapes a conforming client never sends.
type wsTestFrame struct {
	op       wsOpCode
	nonFinal bool
	rsv      byte
	payload  []byte
	unmasked bool
	// lenForm forces the 126 (16-bit) or 127 (64-bit) length encoding; 0 is minimal.
	lenForm int
	// declLen, when nonzero, is the declared payload length regardless of len(payload).
	declLen uint64
}

func (f wsTestFrame) bytes() []byte {
	var b bytes.Buffer
	b0 := byte(f.op) | f.rsv
	if !f.nonFinal {
		b0 |= wsFinalBit
	}
	b.WriteByte(b0)
	l := uint64(len(f.payload))
	if f.declLen != 0 {
		l = f.declLen
	}
	var mask byte
	if !f.unmasked {
		mask = wsMaskBit
	}
	form := f.lenForm
	if form == 0 {
		switch {
		case l <= 125:
		case l < 1<<16:
			form = 126
		default:
			form = 127
		}
	}
	switch form {
	case 126:
		b.WriteByte(mask | 126)
		_ = binary.Write(&b, binary.BigEndian, uint16(l))
	case 127:
		b.WriteByte(mask | 127)
		_ = binary.Write(&b, binary.BigEndian, l)
	default:
		b.WriteByte(mask | byte(l))
	}
	p := append([]byte(nil), f.payload...)
	if !f.unmasked {
		var key [4]byte
		_, _ = rand.Read(key[:])
		b.Write(key[:])
		for i := range p {
			p[i] ^= key[i&3]
		}
	}
	b.Write(p)
	return b.Bytes()
}

func wsBin(p string) []byte    { return wsTestFrame{op: wsBinaryMessage, payload: []byte(p)}.bytes() }
func wsPing(p string) []byte   { return wsTestFrame{op: wsPingMessage, payload: []byte(p)}.bytes() }
func wsClose(b []byte) []byte  { return wsTestFrame{op: wsCloseMessage, payload: b}.bytes() }
func wsStatus(code int) []byte { return binary.BigEndian.AppendUint16(nil, uint16(code)) }

// wsTestServerFrame is one frame the server wrote.
type wsTestServerFrame struct {
	op      wsOpCode
	fin     bool
	payload []byte
}

// wsTestReadServerFrame reads one server frame, which must be unmasked (RFC 6455 §5.1).
func wsTestReadServerFrame(r io.Reader) (wsTestServerFrame, error) {
	var h [2]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return wsTestServerFrame{}, err
	}
	if h[1]&wsMaskBit != 0 {
		return wsTestServerFrame{}, errors.New("server frame is masked")
	}
	l := uint64(h[1] & 0x7F)
	switch l {
	case 126:
		var e [2]byte
		if _, err := io.ReadFull(r, e[:]); err != nil {
			return wsTestServerFrame{}, err
		}
		l = uint64(binary.BigEndian.Uint16(e[:]))
	case 127:
		var e [8]byte
		if _, err := io.ReadFull(r, e[:]); err != nil {
			return wsTestServerFrame{}, err
		}
		l = binary.BigEndian.Uint64(e[:])
	}
	p := make([]byte, l)
	if _, err := io.ReadFull(r, p); err != nil {
		return wsTestServerFrame{}, err
	}
	return wsTestServerFrame{op: wsOpCode(h[0] & 0x0F), fin: h[0]&wsFinalBit != 0, payload: p}, nil
}

// wsTestTCPPair returns both ends of a loopback TCP connection.
func wsTestTCPPair(t testing.TB) (client, server net.Conn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	acc := make(chan net.Conn, 1)
	go func() {
		c, _ := l.Accept()
		acc <- c
	}()
	client, err = net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server = <-acc
	if server == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

// wsReadN reads exactly n stream bytes from w through reads of at most chunk bytes.
func wsReadN(t testing.TB, w *wsConn, n, chunk int) []byte {
	t.Helper()
	out := make([]byte, 0, n)
	buf := make([]byte, chunk)
	for len(out) < n {
		k, err := w.Read(buf)
		if k == 0 && err == nil {
			t.Fatal("Read returned (0, nil)")
		}
		out = append(out, buf[:k]...)
		if err != nil {
			t.Fatalf("Read after %d bytes: %v", len(out), err)
		}
	}
	return out
}

// [MQTT-6.0.0-2]: a data frame can hold part of a packet or several packets, so the adapter is
// a stream: frame boundaries, fragmentation, interleaved control frames and empty frames are
// all invisible to the reader.
func TestWSConnReadIsAStreamAcrossFrames(t *testing.T) {
	t.Parallel()
	cli, srv := wsTestTCPPair(t)
	w := newWSConn(srv, nil)
	var in bytes.Buffer
	in.Write(wsBin("ab"))                                                                     // one packet's head
	in.Write(wsTestFrame{op: wsBinaryMessage, payload: []byte("cd"), nonFinal: true}.bytes()) // ...its tail opens a fragmented message
	in.Write(wsPing("hi"))                                                                    // control frame inside it
	in.Write(wsTestFrame{op: wsContinuationFrame, nonFinal: true}.bytes())                    // empty continuation
	in.Write(wsTestFrame{op: wsContinuationFrame, payload: []byte("efgh")}.bytes())           // two packets, final
	in.Write(wsTestFrame{op: wsBinaryMessage}.bytes())                                        // empty message
	in.Write(wsBin("ij"))
	if _, err := cli.Write(in.Bytes()); err != nil {
		t.Fatal(err)
	}
	// One-byte reads: the masking offset must carry across reads within a frame.
	if got := wsReadN(t, w, 10, 1); string(got) != "abcdefghij" {
		t.Fatalf("stream = %q, want abcdefghij", got)
	}
	f, err := wsTestReadServerFrame(cli)
	if err != nil {
		t.Fatal(err)
	}
	if f.op != wsPongMessage || string(f.payload) != "hi" {
		t.Fatalf("got %v %q, want a Pong echoing the Ping payload", f.op, f.payload)
	}
}

// A frame header and masking key split byte by byte across socket reads are assembled.
func TestWSConnReadAssemblesSplitHeaders(t *testing.T) {
	t.Parallel()
	cli, srv := net.Pipe()
	t.Cleanup(func() { cli.Close(); srv.Close() })
	w := newWSConn(srv, nil)
	frame := wsTestFrame{op: wsBinaryMessage, payload: bytes.Repeat([]byte("x"), 300)}.bytes()
	go func() {
		for i := range frame {
			if _, err := cli.Write(frame[i : i+1]); err != nil {
				return
			}
		}
	}()
	if got := wsReadN(t, w, 300, 7); !bytes.Equal(got, bytes.Repeat([]byte("x"), 300)) {
		t.Fatal("payload mismatch")
	}
}

// Every refused frame fails the connection with a Close carrying the stated status. Text
// frames are refused by MQTT itself [MQTT-6.0.0-1]; the rest are RFC 6455 §5.
func TestWSConnRefusesMalformedFrames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		frames [][]byte
		code   int
	}{
		{"text frame", [][]byte{wsTestFrame{op: wsTextMessage, payload: []byte("x")}.bytes()}, 1003},
		{"unmasked", [][]byte{wsTestFrame{op: wsBinaryMessage, payload: []byte("x"), unmasked: true}.bytes()}, 1002},
		{"rsv1", [][]byte{wsTestFrame{op: wsBinaryMessage, rsv: 0x40, payload: []byte("x")}.bytes()}, 1002},
		{"rsv2", [][]byte{wsTestFrame{op: wsBinaryMessage, rsv: 0x20, payload: []byte("x")}.bytes()}, 1002},
		{"rsv3", [][]byte{wsTestFrame{op: wsBinaryMessage, rsv: 0x10, payload: []byte("x")}.bytes()}, 1002},
		{"opcode 3", [][]byte{wsTestFrame{op: 3}.bytes()}, 1002},
		{"opcode 4", [][]byte{wsTestFrame{op: 4}.bytes()}, 1002},
		{"opcode 5", [][]byte{wsTestFrame{op: 5}.bytes()}, 1002},
		{"opcode 6", [][]byte{wsTestFrame{op: 6}.bytes()}, 1002},
		{"opcode 7", [][]byte{wsTestFrame{op: 7}.bytes()}, 1002},
		{"opcode 11", [][]byte{wsTestFrame{op: 11}.bytes()}, 1002},
		{"opcode 12", [][]byte{wsTestFrame{op: 12}.bytes()}, 1002},
		{"opcode 13", [][]byte{wsTestFrame{op: 13}.bytes()}, 1002},
		{"opcode 14", [][]byte{wsTestFrame{op: 14}.bytes()}, 1002},
		{"opcode 15", [][]byte{wsTestFrame{op: 15}.bytes()}, 1002},
		{"continuation without start", [][]byte{wsTestFrame{op: wsContinuationFrame, payload: []byte("x")}.bytes()}, 1002},
		{"data inside an unfinished message", [][]byte{
			wsTestFrame{op: wsBinaryMessage, payload: []byte("x"), nonFinal: true}.bytes(),
			wsBin("y"),
		}, 1002},
		{"control payload 126", [][]byte{wsTestFrame{op: wsPingMessage, payload: make([]byte, 126)}.bytes()}, 1002},
		{"fragmented ping", [][]byte{wsTestFrame{op: wsPingMessage, nonFinal: true}.bytes()}, 1002},
		{"non-minimal 16-bit length", [][]byte{wsTestFrame{op: wsBinaryMessage, payload: []byte("x"), lenForm: 126}.bytes()}, 1002},
		{"non-minimal 64-bit length", [][]byte{wsTestFrame{op: wsBinaryMessage, payload: []byte("x"), lenForm: 127}.bytes()}, 1002},
		{"64-bit length high bit", [][]byte{wsTestFrame{op: wsBinaryMessage, declLen: 1 << 63}.bytes()}, 1002},
		{"close payload of one byte", [][]byte{wsClose([]byte{3})}, 1002},
		{"close reason not UTF-8", [][]byte{wsClose(append(wsStatus(1000), 0xff, 0xfe))}, 1007},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cli, srv := wsTestTCPPair(t)
			w := newWSConn(srv, nil)
			for _, f := range tc.frames {
				if _, err := cli.Write(f); err != nil {
					t.Fatal(err)
				}
			}
			buf := make([]byte, 16)
			var err error
			for err == nil {
				_, err = w.Read(buf)
			}
			var perr *wsProtocolError
			if !errors.As(err, &perr) || perr.code != tc.code {
				t.Fatalf("Read error = %v, want a protocol error with close %d", err, tc.code)
			}
			_ = cli.SetReadDeadline(time.Now().Add(5 * time.Second))
			f, ferr := wsTestReadServerFrame(cli)
			if ferr != nil || f.op != wsCloseMessage || len(f.payload) != 2 || int(binary.BigEndian.Uint16(f.payload)) != tc.code {
				t.Fatalf("server frame = %+v, %v; want Close %d", f, ferr, tc.code)
			}
			if _, err := w.Read(buf); err == nil {
				t.Fatal("a failed connection read again")
			}
		})
	}
}

// Payload received before a malformed frame is delivered before the failure, as bytes
// before a reset are on TCP.
func TestWSConnDeliversPayloadBeforeAFailure(t *testing.T) {
	t.Parallel()
	cli, srv := wsTestTCPPair(t)
	w := newWSConn(srv, nil)
	_, _ = cli.Write(append(wsBin("abc"), wsTestFrame{op: wsTextMessage}.bytes()...))
	if got := wsReadN(t, w, 3, 16); string(got) != "abc" {
		t.Fatalf("got %q", got)
	}
	if _, err := w.Read(make([]byte, 16)); err == nil {
		t.Fatal("the text frame was not refused")
	}
}

// A peer Close is echoed with the peer's status, or with none when it sent none (1005 never
// goes on the wire), and ends the stream. Reserved and unassigned statuses are refused.
func TestWSConnPeerClose(t *testing.T) {
	t.Parallel()
	for _, code := range []int{1000, 1001, 3000, 4999, -1} {
		cli, srv := wsTestTCPPair(t)
		w := newWSConn(srv, nil)
		payload := []byte(nil)
		if code > 0 {
			payload = append(wsStatus(code), "bye"...)
		}
		_, _ = cli.Write(wsClose(payload))
		_ = cli.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := w.Read(make([]byte, 8)); err != io.EOF {
			t.Fatalf("status %d: Read = %v, want io.EOF", code, err)
		}
		f, err := wsTestReadServerFrame(cli)
		if err != nil || f.op != wsCloseMessage {
			t.Fatalf("status %d: echo = %+v, %v", code, f, err)
		}
		if code < 0 && len(f.payload) != 0 {
			t.Fatalf("no-status Close echoed %x, want an empty payload", f.payload)
		}
		if code > 0 && (len(f.payload) != 2 || int(binary.BigEndian.Uint16(f.payload)) != code) {
			t.Fatalf("status %d echoed as %x", code, f.payload)
		}
		if _, err := w.Write([]byte("late")); !errors.Is(err, errWSCloseSent) {
			t.Fatalf("status %d: Write after Close = %v, want errWSCloseSent", code, err)
		}
	}
	for _, code := range []int{999, 1004, 1005, 1006, 1015, 1016, 2999, 5000} {
		cli, srv := wsTestTCPPair(t)
		w := newWSConn(srv, nil)
		_, _ = cli.Write(wsClose(wsStatus(code)))
		var perr *wsProtocolError
		if _, err := w.Read(make([]byte, 8)); !errors.As(err, &perr) || perr.code != 1002 {
			t.Fatalf("status %d: Read = %v, want a 1002 refusal", code, err)
		}
	}
}

// The declared length of a frame is a count of bytes still to come, never an allocation: a
// 64 MiB claim with ten bytes sent costs nothing near 64 MiB, and a 2^62 claim still delivers
// what was sent.
func TestWSConnDeclaredLengthIsNotAllocated(t *testing.T) {
	cli, srv := wsTestTCPPair(t)
	w := newWSConn(srv, nil)
	f := wsTestFrame{op: wsBinaryMessage, payload: []byte("0123456789"), declLen: 64 << 20}.bytes()
	// Strip the payload the encoder appended beyond the ten bytes actually sent.
	if _, err := cli.Write(f); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32<<10)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	n, err := w.Read(buf)
	runtime.ReadMemStats(&after)
	if err != nil || n != 10 {
		t.Fatalf("Read = %d, %v; want the ten bytes sent", n, err)
	}
	if d := after.TotalAlloc - before.TotalAlloc; d > 1<<20 {
		t.Fatalf("reading a 64 MiB claim allocated %d bytes", d)
	}

	cli2, srv2 := wsTestTCPPair(t)
	w2 := newWSConn(srv2, nil)
	_, _ = cli2.Write(wsTestFrame{op: wsBinaryMessage, payload: []byte("abc"), declLen: 1 << 62}.bytes())
	if got := wsReadN(t, w2, 3, 16); string(got) != "abc" {
		t.Fatalf("got %q", got)
	}
}

// wsTestConn is an instrumented underlying connection. Reads come from a pipe the test
// feeds; every write is recorded with the write deadline in force when it started, and a
// write can be held at a gate.
type wsTestConn struct {
	net.Conn // the read side: one end of a net.Pipe

	mu       sync.Mutex
	writeDL  time.Time
	installs []time.Time
	writes   []wsTestWrite
	out      bytes.Buffer
	// gate, when set, holds the next write whose bytes satisfy hold until it is closed.
	gate     chan struct{}
	hold     func(p []byte) bool
	held     chan struct{}
	failNext error
	short    bool
}

type wsTestWrite struct {
	dl time.Time
	n  int
}

func newWSTestConn(t testing.TB) (*wsTestConn, net.Conn) {
	feed, srv := net.Pipe()
	t.Cleanup(func() { feed.Close(); srv.Close() })
	return &wsTestConn{Conn: srv, held: make(chan struct{}, 8)}, feed
}

func (c *wsTestConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	gate, hold := c.gate, c.hold
	if gate != nil && hold != nil && hold(p) {
		c.gate, c.hold = nil, nil
		c.mu.Unlock()
		c.held <- struct{}{}
		<-gate
		c.mu.Lock()
	}
	defer c.mu.Unlock()
	c.writes = append(c.writes, wsTestWrite{dl: c.writeDL, n: len(p)})
	if err := c.failNext; err != nil {
		c.failNext = nil
		if c.short {
			c.out.Write(p[:len(p)/2])
			return len(p) / 2, err
		}
		return 0, err
	}
	c.out.Write(p)
	return len(p), nil
}

func (c *wsTestConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDL = t
	c.installs = append(c.installs, t)
	return nil
}

func (c *wsTestConn) currentWriteDL() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeDL
}

func (c *wsTestConn) frames(t testing.TB) []wsTestServerFrame {
	t.Helper()
	c.mu.Lock()
	r := bytes.NewReader(c.out.Bytes())
	c.mu.Unlock()
	var out []wsTestServerFrame
	for r.Len() > 0 {
		f, err := wsTestReadServerFrame(r)
		if err != nil {
			t.Fatalf("server output does not parse as whole frames: %v", err)
		}
		out = append(out, f)
	}
	return out
}

// holdPayload gates the next write that is exactly payload p.
func (c *wsTestConn) holdPayload(p []byte) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gate = make(chan struct{})
	c.hold = func(b []byte) bool { return bytes.Equal(b, p) }
	return c.gate
}

// The frame slot keeps each frame atomic on the wire: a Pong provoked while an application
// frame is half written waits for it, and both parse as whole frames.
func TestWSConnPongWaitsForAHalfWrittenFrame(t *testing.T) {
	t.Parallel()
	tc, feed := newWSTestConn(t)
	w := newWSConn(tc, nil)
	payload := bytes.Repeat([]byte("p"), 1000)
	gate := tc.holdPayload(payload)
	wrote := make(chan error, 1)
	go func() { _, err := w.Write(payload); wrote <- err }()
	<-tc.held // the header is out, the payload is held

	go func() { _, _ = feed.Write(wsPing("x")) }()
	readErr := make(chan error, 1)
	go func() { _, err := w.Read(make([]byte, 8)); readErr <- err }()
	time.Sleep(100 * time.Millisecond)
	tc.mu.Lock()
	onWire := tc.out.Len()
	tc.mu.Unlock()
	if onWire != 4 { // the 16-bit-length header of the held frame, and nothing else
		t.Fatalf("%d bytes on the wire while the application frame is half written, want its 4-byte header", onWire)
	}
	close(gate)
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(tc.frames(t)) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	fs := tc.frames(t)
	if len(fs) != 2 || fs[0].op != wsBinaryMessage || fs[1].op != wsPongMessage {
		t.Fatalf("frames = %+v, want the binary frame then the Pong", fs)
	}
	feed.Close()
	<-readErr
}

// Concurrent application writes and Pongs under -race: every byte on the wire belongs to a
// whole frame.
func TestWSConnConcurrentWritesAndPongs(t *testing.T) {
	t.Parallel()
	cli, srv := wsTestTCPPair(t)
	w := newWSConn(srv, nil)
	const writers, perWriter, pings = 4, 200, 200
	go func() {
		for i := 0; i < pings; i++ {
			if _, err := cli.Write(wsPing("pp")); err != nil {
				return
			}
		}
		_, _ = cli.Write(wsBin("z"))
	}()
	go func() { _, _ = w.Read(make([]byte, 8)) }()
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				if _, err := w.Write(bytes.Repeat([]byte("d"), 300)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	r := bufio.NewReader(cli)
	bins, pongs := 0, 0
	_ = cli.SetReadDeadline(time.Now().Add(20 * time.Second))
	for bins < writers*perWriter || pongs < pings {
		f, err := wsTestReadServerFrame(r)
		if err != nil {
			t.Fatalf("after %d binary frames and %d Pongs: %v", bins, pongs, err)
		}
		switch {
		case f.op == wsBinaryMessage && bytes.Equal(f.payload, bytes.Repeat([]byte("d"), 300)):
			bins++
		case f.op == wsPongMessage && string(f.payload) == "pp":
			pongs++
		default:
			t.Fatalf("unexpected frame %+v", f)
		}
	}
	wg.Wait()
}

// A torn frame and a sent Close are terminal: nothing is written after either.
func TestWSConnTerminalWriteStates(t *testing.T) {
	t.Parallel()
	tc, feed := newWSTestConn(t)
	w := newWSConn(tc, nil)
	tc.mu.Lock()
	tc.failNext, tc.short = errors.New("injected"), true
	tc.mu.Unlock()
	if _, err := w.Write([]byte("abcdef")); err == nil {
		t.Fatal("the torn write reported success")
	}
	tc.mu.Lock()
	nw := len(tc.writes)
	tc.mu.Unlock()
	go func() { _, _ = feed.Write(wsPing("x")) }()
	if _, err := w.Read(make([]byte, 8)); !errors.Is(err, errWSFrameWriteFailed) {
		t.Fatalf("Read after a torn frame = %v, want the Pong refused with errWSFrameWriteFailed", err)
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, errWSFrameWriteFailed) {
		t.Fatalf("Write after a torn frame = %v", err)
	}
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if len(tc.writes) != nw {
		t.Fatalf("%d writes reached the socket after the torn frame", len(tc.writes)-nw)
	}
}

func near(a, b time.Time, slack time.Duration) bool {
	d := a.Sub(b)
	return d > -slack && d < slack
}

// The core's write deadline reaches the socket at once, set and cleared, and a Pong's write
// runs under min(core deadline, its own bound).
func TestWSConnWriteDeadlineReachesTheSocket(t *testing.T) {
	t.Parallel()
	tc, feed := newWSTestConn(t)
	w := newWSConn(tc, nil)
	capDL := time.Now().Add(time.Second)
	_ = w.SetWriteDeadline(capDL)
	if !tc.currentWriteDL().Equal(capDL) {
		t.Fatal("the set did not reach the socket")
	}
	// A Pong with the earlier core cap in force writes under the cap, not under its own
	// later bound.
	go func() { _, _ = feed.Write(append(wsPing("x"), wsBin("y")...)) }()
	if _, err := w.Read(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	tc.mu.Lock()
	pongDL := tc.writes[len(tc.writes)-1].dl
	tc.mu.Unlock()
	if !pongDL.Equal(capDL) {
		t.Fatalf("Pong written under %v, want the earlier cap %v", pongDL, capDL)
	}
	if !tc.currentWriteDL().Equal(capDL) {
		t.Fatal("the cap was not restored after the Pong")
	}
	_ = w.SetWriteDeadline(time.Time{})
	if !tc.currentWriteDL().IsZero() {
		t.Fatal("the clear did not reach the socket")
	}
}

// (a)+(c): a Pong waiting behind application frame A, whose deadline is near, is not failed
// at A's deadline once A completes and the core clears it, and writes under its own bound.
func TestWSConnWaitingPongFollowsAClearedDeadline(t *testing.T) {
	t.Parallel()
	tc, feed := newWSTestConn(t)
	w := newWSConn(tc, nil)
	payload := []byte("application-frame-A")
	gate := tc.holdPayload(payload)
	_ = w.SetWriteDeadline(time.Now().Add(400 * time.Millisecond))
	wrote := make(chan error, 1)
	go func() { _, err := w.Write(payload); wrote <- err }()
	<-tc.held
	go func() { _, _ = feed.Write(append(wsPing("x"), wsBin("y")...)) }()
	readErr := make(chan error, 1)
	go func() { _, err := w.Read(make([]byte, 8)); readErr <- err }()
	time.Sleep(100 * time.Millisecond)
	// The core clears A's deadline, and A's old deadline then passes while the slot is
	// still held: a waiter judging a snapshot would give up here.
	_ = w.SetWriteDeadline(time.Time{})
	time.Sleep(500 * time.Millisecond)
	close(gate)
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
	if err := <-readErr; err != nil {
		t.Fatalf("Read = %v: the Pong was failed at the cleared deadline", err)
	}
	fs := tc.frames(t)
	if len(fs) != 2 || fs[1].op != wsPongMessage {
		t.Fatalf("frames = %+v", fs)
	}
	tc.mu.Lock()
	pongDL := tc.writes[len(tc.writes)-1].dl
	tc.mu.Unlock()
	if pongDL.IsZero() || !near(pongDL, time.Now().Add(mqttWriteDeadline.Get()), 2*time.Second) {
		t.Fatalf("Pong written under %v, want its own bound", pongDL)
	}
	if !tc.currentWriteDL().IsZero() {
		t.Fatal("the cleared core deadline was not restored after the Pong")
	}
}

// (a): the core clears its deadline while a Pong is mid-write, as writeSocket does once its
// own frame completed. The socket must hold the Pong's own bound at once (neither the stale
// core value, which would fail the Pong spuriously, nor nothing, which would let a non-reading
// peer hold it), and the cleared value once the Pong is done.
func TestWSConnClearDuringAPongKeepsThePongBound(t *testing.T) {
	t.Parallel()
	tc, feed := newWSTestConn(t)
	w := newWSConn(tc, nil)
	stale := time.Now().Add(time.Second)
	_ = w.SetWriteDeadline(stale)
	if _, err := w.Write([]byte("application-frame-A")); err != nil {
		t.Fatal(err)
	}
	gate := tc.holdPayload(wsTestServerFramePong("x"))
	go func() { _, _ = feed.Write(append(wsPing("x"), wsBin("y")...)) }()
	readErr := make(chan error, 1)
	go func() { _, err := w.Read(make([]byte, 8)); readErr <- err }()
	<-tc.held // the Pong holds the slot, its header write parked
	if got := tc.currentWriteDL(); !got.Equal(stale) {
		t.Fatalf("socket deadline %v during the Pong, want the earlier core deadline %v", got, stale)
	}
	_ = w.SetWriteDeadline(time.Time{})
	got := tc.currentWriteDL()
	if got.IsZero() || got.Equal(stale) || !near(got, time.Now().Add(mqttWriteDeadline.Get()), 2*time.Second) {
		t.Fatalf("after the clear the socket holds %v during the Pong, want the Pong's own bound", got)
	}
	close(gate)
	if err := <-readErr; err != nil {
		t.Fatal(err)
	}
	if !tc.currentWriteDL().IsZero() {
		t.Fatal("the cleared core deadline was not restored after the Pong")
	}
}

// (d): the same with A's deadline extended rather than cleared.
func TestWSConnWaitingPongFollowsAnExtendedDeadline(t *testing.T) {
	t.Parallel()
	tc, feed := newWSTestConn(t)
	w := newWSConn(tc, nil)
	payload := []byte("application-frame-A")
	gate := tc.holdPayload(payload)
	_ = w.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
	wrote := make(chan error, 1)
	go func() { _, err := w.Write(payload); wrote <- err }()
	<-tc.held
	go func() { _, _ = feed.Write(append(wsPing("x"), wsBin("y")...)) }()
	readErr := make(chan error, 1)
	go func() { _, err := w.Read(make([]byte, 8)); readErr <- err }()
	time.Sleep(100 * time.Millisecond)
	_ = w.SetWriteDeadline(time.Now().Add(5 * time.Second))
	time.Sleep(400 * time.Millisecond) // past the original deadline
	close(gate)
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
	if err := <-readErr; err != nil {
		t.Fatalf("Read = %v: the Pong was failed at the superseded deadline", err)
	}
}

// (b): an earlier cap installed while a Pong is in flight reaches the socket at once.
func TestWSConnCapDuringAPongReachesTheSocket(t *testing.T) {
	t.Parallel()
	tc, feed := newWSTestConn(t)
	w := newWSConn(tc, nil)
	pong := wsTestServerFramePong("x")
	gate := tc.holdPayload(pong)
	go func() { _, _ = feed.Write(append(wsPing("x"), wsBin("y")...)) }()
	readErr := make(chan error, 1)
	go func() { _, err := w.Read(make([]byte, 8)); readErr <- err }()
	<-tc.held
	ctl := tc.currentWriteDL()
	if ctl.IsZero() {
		t.Fatal("no bound installed for the Pong")
	}
	capDL := time.Now().Add(50 * time.Millisecond)
	setDone := make(chan struct{})
	go func() { _ = w.SetWriteDeadline(capDL); close(setDone) }()
	select {
	case <-setDone:
	case <-time.After(2 * time.Second):
		t.Fatal("SetWriteDeadline blocked behind the Pong")
	}
	if !tc.currentWriteDL().Equal(capDL) {
		t.Fatalf("socket deadline %v during the Pong, want the earlier cap %v", tc.currentWriteDL(), capDL)
	}
	close(gate)
	<-readErr
	if !tc.currentWriteDL().Equal(capDL) {
		t.Fatal("after the Pong the socket does not hold the core's deadline")
	}
}

// wsTestServerFramePong is the wire form of a Pong the server writes, as one buffer: header
// and payload go out in two writes, so this matches the header write.
func wsTestServerFramePong(p string) []byte {
	return []byte{wsFinalBit | byte(wsPongMessage), byte(len(p))}
}

// A Pong waiting for the slot behind a write the peer never absorbs gives up at the read
// deadline - MQTT keep-alive - rather than waiting for the holder.
func TestWSConnPongWaitEndsAtTheReadDeadline(t *testing.T) {
	t.Parallel()
	tc, feed := newWSTestConn(t)
	w := newWSConn(tc, nil)
	payload := []byte("never-absorbed")
	gate := tc.holdPayload(payload)
	t.Cleanup(func() { close(gate) })
	go func() { _, _ = w.Write(payload) }()
	<-tc.held
	_ = w.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	go func() { _, _ = feed.Write(wsPing("x")) }()
	start := time.Now()
	_, err := w.Read(make([]byte, 8))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read = %v, want the Pong's wait to end at the read deadline", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("the Pong waited %v", el)
	}
}

// A Pong to a peer that pings but never reads ends at the read deadline, not at
// mqttWriteDeadline.
func TestWSConnPongToANonReadingPeerEndsAtTheReadDeadline(t *testing.T) {
	t.Parallel()
	feed, srv := net.Pipe() // synchronous: an unread write blocks
	t.Cleanup(func() { feed.Close(); srv.Close() })
	w := newWSConn(srv, nil)
	_ = w.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	go func() { _, _ = feed.Write(wsPing("x")) }()
	start := time.Now()
	if _, err := w.Read(make([]byte, 8)); err == nil {
		t.Fatal("Read succeeded with the Pong unwritable")
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("the blocked Pong held the reader for %v", el)
	}
}
