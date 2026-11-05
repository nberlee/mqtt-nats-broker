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
	"io"
	"net"
	"slices"
	"testing"
	"time"
)

// scriptedReadConn answers each Read with a full or a one-byte read, as scripted, records the
// size of every buffer it was handed, and ends with EOF.
type scriptedReadConn struct {
	net.Conn
	script []bool // true: fill the buffer; false: one byte
	sizes  []int
	first  []byte
}

func (s *scriptedReadConn) Read(p []byte) (int, error) {
	s.sizes = append(s.sizes, len(p))
	if len(s.script) == 0 {
		return 0, io.EOF
	}
	full := s.script[0]
	s.script = s.script[1:]
	n := 1
	if full {
		n = len(p)
	}
	for i := range n {
		p[i] = 'x'
	}
	if len(s.first) > 0 {
		copy(p, s.first)
		s.first = nil
	}
	return n, nil
}

func (s *scriptedReadConn) Close() error                     { return nil }
func (s *scriptedReadConn) SetReadDeadline(time.Time) error  { return nil }
func (s *scriptedReadConn) SetWriteDeadline(time.Time) error { return nil }
func (s *scriptedReadConn) SetDeadline(time.Time) error      { return nil }
func (s *scriptedReadConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (s *scriptedReadConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }

// The read buffer follows the traffic: it starts small, doubles on each read that fills it up
// to the ceiling, and halves after a run of short reads, never below the start.
func TestReadBufferFollowsTraffic(t *testing.T) {
	t.Run("policy", func(t *testing.T) {
		for _, tc := range []struct{ cur, n, shorts, wantSize, wantShorts int }{
			{512, 512, 0, 1024, 0},
			{16384, 16384, 2, 32768, 0},
			{32768, 32768, 0, 32768, 0},
			{32768, 1, 0, 32768, 1},
			{32768, 1, 2, 16384, 0},
			{512, 1, 5, 512, 6},
			{4096, 3000, 2, 4096, 0},
		} {
			size, shorts := mqttNextReadBufSize(tc.cur, tc.n, tc.shorts)
			if size != tc.wantSize || shorts != tc.wantShorts {
				t.Errorf("mqttNextReadBufSize(%d, %d, %d) = %d, %d; want %d, %d",
					tc.cur, tc.n, tc.shorts, size, shorts, tc.wantSize, tc.wantShorts)
			}
		}
	})
	t.Run("readLoop", func(t *testing.T) {
		// One CONNECT claiming a 1,000,000-byte body, never completed: every read is a partial
		// packet, so the parser only buffers and the loop's own sizing is all that is observed.
		full := slices.Repeat([]bool{true}, 8)
		short := slices.Repeat([]bool{false}, 7)
		conn := &scriptedReadConn{script: append(full, short...), first: []byte{mqttPacketConnect, 0xC0, 0x84, 0x3D}}
		c := &client{nc: conn, rawConn: conn, mqtt: &mqtt{}, mpay: 2 << 20}
		c.readLoop(nil)
		want := []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 32768, // the full reads
			32768, 32768, 32768, 16384, 16384, 16384, 8192, // the short reads
			8192} // EOF
		if !slices.Equal(conn.sizes, want) {
			t.Fatalf("read buffer sizes %v, want %v", conn.sizes, want)
		}
	})
}
