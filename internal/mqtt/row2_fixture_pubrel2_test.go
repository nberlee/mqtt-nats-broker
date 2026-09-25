// Copyright 2026 The mqtt-nats-bridge Authors
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

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Fixtures for the PUBREL-side row-2 tests (F-105 row 2): the output boundary observed on the
// wire, the connection's frame guard read directly, and a second entitled broker.

// testRow2Timeout bounds every wait on an asynchronous drain in these tests.
const testRow2Timeout = 5 * time.Second

// testRow2Wire gives the fixture connection a recording transport, so what the authorized boundary
// and the raw emitter put on the wire is observable byte for byte.
func testRow2Wire(e *testInflightEnv) *testFrameConn {
	tc := &testFrameConn{}
	e.c.mu.Lock()
	e.c.nc, e.c.rawConn = tc, tc
	e.c.mu.Unlock()
	return tc
}

// testRow2PubRelFrame is the four bytes of a success PUBREL for pi.
func testRow2PubRelFrame(pi uint16) []byte {
	return []byte{mqttPacketPubRel | 0x2, 0x2, byte(pi >> 8), byte(pi)}
}

// testRow2GuardState reads one acquisition's entry in a connection's frame guard.
func testRow2GuardState(g *mqttFrameGuard, gen, lin string, acq uint64) (mqttFrameState, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	st, ok := g.state[mqttActionKey{gen: gen, nonce: lin, acq: acq}]
	return st, ok
}

// testRow2GuardHasLineage reports whether the guard holds ANY entry for this lineage.
func testRow2GuardHasLineage(g *mqttFrameGuard, gen, lin string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k := range g.state {
		if k.gen == gen && k.nonce == lin {
			return true
		}
	}
	return false
}

// testRow2Gen reads the session generation.
func testRow2Gen(e *testInflightEnv) string {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return e.sess.gen
}

// testRow2Ref reads a slot and the reference an acquisition is taken from.
func testRow2Ref(t *testing.T, e *testInflightEnv, subject string) (mqttInflightSlotRef, *mqttInflightRecord, uint64) {
	t.Helper()
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the slot %q: %+v (%v)", subject, cur, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	if ref == nil {
		t.Fatalf("the slot %q yields no reference", subject)
	}
	return *ref, cur, smsg.Sequence
}

// testRow2Action loads the action record for (gen, lin, acq), or nil when there is none.
func testRow2Action(t *testing.T, e *testInflightEnv, gen, lin string, acq uint64) *mqttOutboundAction {
	t.Helper()
	msg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttActionSubject(getHash(e.sess.id), gen, lin, acq))
	if err != nil || msg == nil {
		return nil
	}
	a, derr := mqttDecodeOutboundAction(msg.Data, e.sess.id, e.asm.sessStreamEpoch, gen, lin, acq)
	if derr != nil {
		t.Fatalf("the action record for %s/%d does not decode: %v", lin, acq, derr)
	}
	return a
}

// testRow2Holder reads the identifier's registered PUBREL holder.
func testRow2Holder(e *testInflightEnv, pi uint16) *mqttSendAction {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if h := e.sess.sendsInFlight[pi]; h != nil {
		return h.action
	}
	return nil
}

// testRow2Eventually polls cond until it holds or the deadline passes.
func testRow2Eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// testRow2FailLoadsWhile makes every register read fail while fail is set, over the pass-through
// seams, so a resolving read after a lost response is indeterminate.
func testRow2FailLoadsWhile(e *testInflightEnv, fail *atomic.Bool) {
	testInstallPassThroughSeams(e)
	e.asm.mu.Lock()
	load := e.asm.testInflightLoad
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if fail.Load() {
			return nil, nil, errors.New("test: the resolving read is unavailable")
		}
		return load(subject)
	}
	e.asm.mu.Unlock()
}

// testRow2AcceptedClient builds a connection for (asm, sess) accepted under the standing serving
// authority: identity, output binding and entitlement, as an accepted CONNECT leaves one.
func testRow2AcceptedClient(t *testing.T, e *testInflightEnv, asm *mqttAccountSessionManager, sess *mqttSession,
	c *client) {
	t.Helper()
	c.mqtt = &mqtt{asm: asm, sess: sess}
	c.cid = e.s.gcid.Add(1)
	c.outBind = mqttBindConnection(c.cid)
	al := asm.jsa.loadOutboundAuthority(sess.id, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		t.Fatalf("no serving authority to accept the second connection under: %+v", al)
	}
	c.setOutboundEntitlement(mqttOutboundEntitlement{Gen: al.rec.Gen, Tenure: al.rec.Tenure})
	if _, ok := c.outboundEntitlement(); !ok {
		t.Fatalf("the second connection carries no entitlement")
	}
}

// testRow2StallConn writes the first two bytes of a frame, then blocks the rest until released:
// a partial write in flight.
type testRow2StallConn struct {
	net.Conn
	mu        sync.Mutex
	written   []byte
	firstDone chan struct{}
	release   chan struct{}
	once      sync.Once
}

func newTestRow2StallConn() *testRow2StallConn {
	return &testRow2StallConn{firstDone: make(chan struct{}), release: make(chan struct{})}
}

func (c *testRow2StallConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	first := len(c.written) == 0
	c.mu.Unlock()
	if first && len(b) > 2 {
		c.mu.Lock()
		c.written = append(c.written, b[:2]...)
		c.mu.Unlock()
		c.once.Do(func() { close(c.firstDone) })
		return 2, nil
	}
	<-c.release
	c.mu.Lock()
	c.written = append(c.written, b...)
	c.mu.Unlock()
	return len(b), nil
}

func (c *testRow2StallConn) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.written...)
}

func (c *testRow2StallConn) Close() error                     { return nil }
func (c *testRow2StallConn) SetWriteDeadline(time.Time) error { return nil }
