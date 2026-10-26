// Copyright 2020-2026 The NATS Authors
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

// MQTT over WebSocket: the RFC 6455 frame layer, presented to the protocol core as the
// byte stream MQTT over TCP already is. The frame header codec, the unmasking and the
// close-status rules are adapted from nats-server v2.15.1 server/websocket.go. Not
// carried over: compression, the no-masking negotiation, cookies, forwarded addresses,
// browser heuristics, leafnode routing, and upstream's acceptance of text frames, which
// MQTT forbids [MQTT-6.0.0-1]. The opening handshake is transport_websocket_upgrade.go.

package mqtt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
	"unicode/utf8"
)

type wsOpCode byte

// RFC 6455 §5.2.
const (
	wsContinuationFrame wsOpCode = 0
	wsTextMessage       wsOpCode = 1
	wsBinaryMessage     wsOpCode = 2
	wsCloseMessage      wsOpCode = 8
	wsPingMessage       wsOpCode = 9
	wsPongMessage       wsOpCode = 10

	wsFinalBit = 1 << 7
	wsRsvBits  = 0x70
	wsMaskBit  = 1 << 7

	wsMaxControlPayloadSize = 125
)

// RFC 6455 §7.4.1.
const (
	wsCloseStatusProtocolError      = 1002
	wsCloseStatusUnsupportedData    = 1003
	wsCloseStatusNoStatusReceived   = 1005
	wsCloseStatusInvalidPayloadData = 1007
)

var (
	// errWSFrameWriteFailed is every write after a frame was torn: the stream is corrupt from
	// that byte on, so nothing may follow it, not even a Pong.
	errWSFrameWriteFailed = errors.New("websocket: an earlier frame write failed")
	// errWSCloseSent is every write after this side's Close frame: no data frame follows a
	// Close (RFC 6455 §5.5.1).
	errWSCloseSent = errors.New("websocket: close frame already sent")
)

// wsProtocolError is a peer frame this server refuses. Code is the Close status sent
// before the connection is failed.
type wsProtocolError struct {
	code   int
	reason string
}

func (e *wsProtocolError) Error() string {
	return fmt.Sprintf("websocket protocol error (close %d): %s", e.code, e.reason)
}

// wsConn is one upgraded WebSocket connection as a net.Conn: Read yields the payload bytes
// of binary data frames as one stream and Write sends each call as one binary frame, so the
// core's packet reader, which never assumed packet boundaries on TCP, needs none here
// either [MQTT-6.0.0-2]. Every bound the core installs - the keep-alive read deadline, the
// per-write deadline, the CONNECT timer closing rawConn - reaches the socket underneath.
//
// One goroutine reads (the client's readLoop). Writers are the core's, serialized above by
// c.wmu, plus the reader itself, which answers a Ping with a Pong and a Close with a Close.
// The frame slot fsem makes each frame atomic on the wire between those two.
type wsConn struct {
	conn net.Conn
	// logf reports why a peer frame was refused; the connection then fails like any read
	// error. It must not take c.mu: it runs on the read goroutine with the slot possibly held.
	logf func(format string, v ...any)

	// Read state, owned by the single reader.
	rem     uint64  // payload bytes still to come in the current data frame
	msgOpen bool    // a binary message has begun and its final frame is still to come
	mkey    [4]byte // the current frame's masking key
	mpos    int     // offset into mkey of the next payload byte
	rerr    error   // terminal read outcome, returned by every later Read
	ctl     [wsMaxControlPayloadSize]byte

	// fsem is the frame slot: a writer holds it for exactly one frame. A channel rather than
	// a mutex so that a wait can be abandoned at a deadline (acquire).
	fsem chan struct{}
	// broken and closeSent are the terminal write states; read and written only by the
	// holder of fsem.
	broken    bool
	closeSent bool

	// dmu guards the deadline state. It is a leaf: nothing is acquired while holding it,
	// and it is never held across a wait for fsem.
	dmu sync.Mutex
	// appDL is the write deadline the core last set; readDL the read deadline. ctlDL is the
	// bound of the control write in progress, meaningful while ctlActive.
	appDL, readDL, ctlDL time.Time
	ctlActive            bool
	// dlChanged is closed and replaced on every deadline change, waking slot waiters so they
	// re-evaluate their bound against the current state.
	dlChanged chan struct{}
}

func newWSConn(conn net.Conn, logf func(format string, v ...any)) *wsConn {
	return &wsConn{conn: conn, logf: logf, fsem: make(chan struct{}, 1), dlChanged: make(chan struct{})}
}

// earliestDeadline is the earlier of two deadlines, where zero means none.
func earliestDeadline(a, b time.Time) time.Time {
	switch {
	case a.IsZero():
		return b
	case b.IsZero():
		return a
	case b.Before(a):
		return b
	}
	return a
}

// Read returns at least one payload byte or an error, never (0, nil): control frames and
// empty data frames are consumed inside the call. That is also why WebSocket traffic cannot
// refresh MQTT keep-alive: the read deadline the core armed before calling Read is never
// touched here, so a client that only pings is still closed at the keep-alive bound.
func (w *wsConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for w.rem == 0 {
		if w.rerr != nil {
			return 0, w.rerr
		}
		if err := w.nextDataFrame(); err != nil {
			w.rerr = err
			return 0, err
		}
	}
	if uint64(len(p)) > w.rem {
		p = p[:w.rem]
	}
	// The declared length only counts what is still to come; nothing is allocated by it.
	n, err := w.conn.Read(p)
	w.unmask(p[:n])
	w.rem -= uint64(n)
	if err != nil {
		// Bytes already received are delivered first; the failure is the next call's.
		w.rerr = err
		if n > 0 {
			return n, nil
		}
		return 0, err
	}
	if n == 0 {
		return w.Read(p)
	}
	return n, nil
}

// nextDataFrame consumes frame headers until one opens payload bytes, handling control
// frames and skipping empty data frames on the way.
func (w *wsConn) nextDataFrame() error {
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(w.conn, hdr[:2]); err != nil {
			return err
		}
		b0, b1 := hdr[0], hdr[1]
		final := b0&wsFinalBit != 0
		op := wsOpCode(b0 & 0x0F)
		// No extension is ever negotiated, so every RSV bit must be clear (RFC 6455 §5.2).
		if b0&wsRsvBits != 0 {
			return w.fail(wsCloseStatusProtocolError, "RSV bits set without a negotiated extension")
		}
		// A client MUST mask every frame it sends, and the server MUST fail one that is not
		// masked (RFC 6455 §5.1).
		if b1&wsMaskBit == 0 {
			return w.fail(wsCloseStatusProtocolError, "unmasked client frame")
		}
		length := uint64(b1 & 0x7F)
		isControl := op >= wsCloseMessage
		if isControl && length > wsMaxControlPayloadSize {
			return w.fail(wsCloseStatusProtocolError, "control frame payload over 125 bytes")
		}
		switch length {
		case 126:
			if _, err := io.ReadFull(w.conn, hdr[:2]); err != nil {
				return err
			}
			if length = uint64(binary.BigEndian.Uint16(hdr[:2])); length < 126 {
				return w.fail(wsCloseStatusProtocolError, "non-minimal 16-bit payload length")
			}
		case 127:
			if _, err := io.ReadFull(w.conn, hdr[:8]); err != nil {
				return err
			}
			length = binary.BigEndian.Uint64(hdr[:8])
			if length&(1<<63) != 0 {
				return w.fail(wsCloseStatusProtocolError, "64-bit payload length with the high bit set")
			}
			if length < 1<<16 {
				return w.fail(wsCloseStatusProtocolError, "non-minimal 64-bit payload length")
			}
		}
		if _, err := io.ReadFull(w.conn, w.mkey[:]); err != nil {
			return err
		}
		w.mpos = 0

		switch op {
		case wsCloseMessage, wsPingMessage, wsPongMessage:
			if !final {
				return w.fail(wsCloseStatusProtocolError, "fragmented control frame")
			}
			payload := w.ctl[:length]
			if _, err := io.ReadFull(w.conn, payload); err != nil {
				return err
			}
			w.unmask(payload)
			if err := w.handleControl(op, payload); err != nil {
				return err
			}
		case wsTextMessage:
			return w.fail(wsCloseStatusUnsupportedData, "text frame: MQTT is carried in binary frames only")
		case wsBinaryMessage:
			if w.msgOpen {
				return w.fail(wsCloseStatusProtocolError, "new message before the previous one's final frame")
			}
			w.msgOpen = !final
			if w.rem = length; w.rem > 0 {
				return nil
			}
		case wsContinuationFrame:
			if !w.msgOpen {
				return w.fail(wsCloseStatusProtocolError, "continuation frame without a started message")
			}
			w.msgOpen = !final
			if w.rem = length; w.rem > 0 {
				return nil
			}
		default:
			return w.fail(wsCloseStatusProtocolError, fmt.Sprintf("reserved opcode %d", op))
		}
	}
}

// handleControl answers a Ping and a Close; a Pong needs nothing. A Close ends the stream
// with io.EOF after it is echoed: that is a transport close, not an MQTT DISCONNECT, so the
// core's teardown publishes the Will.
func (w *wsConn) handleControl(op wsOpCode, payload []byte) error {
	switch op {
	case wsPingMessage:
		return w.writeControl(wsPongMessage, payload)
	case wsCloseMessage:
		if len(payload) == 1 {
			return w.fail(wsCloseStatusProtocolError, "close frame payload of one byte")
		}
		var echo []byte
		if len(payload) >= 2 {
			status := int(binary.BigEndian.Uint16(payload[:2]))
			if !wsIsValidCloseStatus(status) {
				return w.fail(wsCloseStatusProtocolError, fmt.Sprintf("invalid close status %d", status))
			}
			if !utf8.Valid(payload[2:]) {
				return w.fail(wsCloseStatusInvalidPayloadData, "close reason is not valid UTF-8")
			}
			// Echo the status the peer sent (RFC 6455 §5.5.1). Without one, the echo
			// carries none either: 1005 must never appear on the wire.
			echo = payload[:2]
		}
		if err := w.writeControl(wsCloseMessage, echo); err != nil {
			return err
		}
		return io.EOF
	}
	return nil
}

// wsIsValidCloseStatus reports whether a peer may send status in a Close frame (RFC 6455
// §7.4): 1005, 1006 and 1015 are reserved for reporting, 1004 and 1016-2999 are unassigned
// to endpoints, and anything outside 1000-4999 is invalid.
func wsIsValidCloseStatus(status int) bool {
	switch {
	case status < 1000 || status > 4999:
		return false
	case status == 1004 || status == 1005 || status == 1006 || status == 1015:
		return false
	case status >= 1016 && status <= 2999:
		return false
	}
	return true
}

// fail refuses the peer's frame: it reports why, sends a Close carrying code (best effort),
// and returns the error the connection fails with.
func (w *wsConn) fail(code int, reason string) error {
	perr := &wsProtocolError{code: code, reason: reason}
	if w.logf != nil {
		w.logf("%v", perr)
	}
	var body [2]byte
	binary.BigEndian.PutUint16(body[:], uint16(code))
	_ = w.writeControl(wsCloseMessage, body[:])
	return perr
}

// unmask applies the frame's masking key, continuing from where the previous call on the
// same frame stopped.
func (w *wsConn) unmask(b []byte) {
	for i := range b {
		b[i] ^= w.mkey[w.mpos&3]
		w.mpos++
	}
}

// Write sends p as one binary frame and reports all of p written or none: a torn frame
// leaves the stream unusable, and the core closes the connection on any write error.
func (w *wsConn) Write(p []byte) (int, error) {
	if err := w.acquire(func() time.Time { return w.appDL }); err != nil {
		return 0, err
	}
	defer w.release()
	if err := w.writableLocked(); err != nil {
		return 0, err
	}
	if err := w.writeFrameLocked(wsBinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// writeControl sends a Pong or a Close from the read goroutine. Its bound is fixed at
// invocation: mqttWriteDeadline from now, or the read deadline if that is sooner. The read
// deadline is what keeps a peer that pings but never reads from holding this goroutine past
// MQTT keep-alive: on TCP that peer's read would time out then, and here the Pong it
// provoked gives up at the same instant, whether it is writing or waiting for the slot.
func (w *wsConn) writeControl(op wsOpCode, payload []byte) error {
	w.dmu.Lock()
	ctlDL := earliestDeadline(time.Now().Add(mqttWriteDeadline.Get()), w.readDL)
	w.dmu.Unlock()
	if err := w.acquire(func() time.Time {
		return earliestDeadline(ctlDL, earliestDeadline(w.readDL, w.appDL))
	}); err != nil {
		return err
	}
	defer w.release()
	if err := w.writableLocked(); err != nil {
		return err
	}
	w.dmu.Lock()
	w.ctlActive, w.ctlDL = true, ctlDL
	err := w.installLocked()
	w.dmu.Unlock()
	if err == nil {
		err = w.writeFrameLocked(op, payload)
	} else {
		w.broken = true
	}
	w.dmu.Lock()
	w.ctlActive = false
	_ = w.installLocked()
	w.dmu.Unlock()
	if err == nil && op == wsCloseMessage {
		w.closeSent = true
	}
	return err
}

// acquire takes the frame slot, giving up when bound - evaluated under dmu against the
// CURRENT deadlines, never a snapshot - has passed. Every deadline change wakes the wait to
// re-evaluate, and a timer firing is only a prompt to re-evaluate: a bound that was cleared
// or extended meanwhile keeps waiting. A zero bound waits without one.
func (w *wsConn) acquire(bound func() time.Time) error {
	var t *time.Timer
	defer func() {
		if t != nil {
			t.Stop()
		}
	}()
	for {
		w.dmu.Lock()
		dl := bound()
		changed := w.dlChanged
		w.dmu.Unlock()
		var expired <-chan time.Time
		if !dl.IsZero() {
			d := time.Until(dl)
			if d <= 0 {
				return os.ErrDeadlineExceeded
			}
			if t == nil {
				t = time.NewTimer(d)
			} else {
				t.Reset(d)
			}
			expired = t.C
		}
		select {
		case w.fsem <- struct{}{}:
			return nil
		case <-changed:
		case <-expired:
		}
	}
}

func (w *wsConn) release() { <-w.fsem }

func (w *wsConn) writableLocked() error {
	switch {
	case w.broken:
		return errWSFrameWriteFailed
	case w.closeSent:
		return errWSCloseSent
	}
	return nil
}

// writeFrameLocked writes one complete, unmasked frame (servers never mask, RFC 6455 §5.1).
// Any failure, including a short write, marks the connection broken before the slot is
// released, so no other frame can be appended to a torn one.
func (w *wsConn) writeFrameLocked(op wsOpCode, payload []byte) error {
	var hdr [10]byte
	hdr[0] = wsFinalBit | byte(op)
	n := 2
	switch l := len(payload); {
	case l <= wsMaxControlPayloadSize:
		hdr[1] = byte(l)
	case l < 1<<16:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:], uint16(l))
		n = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:], uint64(l))
		n = 10
	}
	bufs := net.Buffers{hdr[:n], payload}
	want := int64(n + len(payload))
	written, err := bufs.WriteTo(w.conn)
	if err == nil && written != want {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.broken = true
	}
	return err
}

// installLocked is the one place the socket's write deadline is set. It always installs the
// effective value of the current state - the core's deadline, cut short by a control write's
// bound while one is in flight - so no interleaving of the setters with a control write can
// leave a stale deadline on the socket or let a control write outlast an earlier cap.
func (w *wsConn) installLocked() error {
	eff := w.appDL
	if w.ctlActive {
		eff = earliestDeadline(eff, w.ctlDL)
	}
	return w.conn.SetWriteDeadline(eff)
}

func (w *wsConn) signalLocked() {
	close(w.dlChanged)
	w.dlChanged = make(chan struct{})
}

// SetWriteDeadline reaches the socket at once, so a pending write sees the new deadline and
// a clear really clears: a deadline left installed would expire through idle time and fail
// the next write, the failure writeSocket's clear exists to prevent.
func (w *wsConn) SetWriteDeadline(t time.Time) error {
	w.dmu.Lock()
	defer w.dmu.Unlock()
	w.appDL = t
	w.signalLocked()
	return w.installLocked()
}

func (w *wsConn) SetReadDeadline(t time.Time) error {
	w.dmu.Lock()
	w.readDL = t
	w.signalLocked()
	w.dmu.Unlock()
	return w.conn.SetReadDeadline(t)
}

func (w *wsConn) SetDeadline(t time.Time) error {
	if err := w.SetReadDeadline(t); err != nil {
		return err
	}
	return w.SetWriteDeadline(t)
}

// Close closes the socket without a Close frame. The core tears a connection down by
// closing rawConn, never c.nc, so this is not the broker's close path; see
// docs/CONFORMANCE.md "MQTT over WebSocket".
func (w *wsConn) Close() error         { return w.conn.Close() }
func (w *wsConn) LocalAddr() net.Addr  { return w.conn.LocalAddr() }
func (w *wsConn) RemoteAddr() net.Addr { return w.conn.RemoteAddr() }
