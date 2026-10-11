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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The F-088 cross-tenure witnesses (PS-1, PS-2) and W6a, on production input.
//
// A send section's durable action token is acquired by the broker that opened the section, and
// the section's writer validates its ORIGINAL binding, not the current serving authority. A
// client that resumes on a PEER broker while that section is open is therefore served by a new
// tenure whose recovery derived the entry from the register and its still-standing unsent
// marker: to the peer the entry looks PROVEN-UNSENT, while the displaced broker may yet record
// intent (the marker's confirmed delete) and write. These tests put that schedule on two real
// Servers sharing one JetStream — the supported multi-frontend topology — with real clients and
// real CONNECTs, and force exactly one interleaving: broker A's section parked at RESERVED by the
// send-window hook. Everything else is production.
//
// AckWait is 30 s on both brokers. A JetStream redelivery of message 1 to B inside the witness
// would let B acquire and transmit it itself, ending the proven-unsent premise; the window is
// CHECKED (elapsed time, and a B-section observer), never assumed.
//
// Not exercised by any test in this file: abrupt process death, and the AckWait race that bounds
// the window in production (B's teardown before B's own acquisition is held by construction).

const (
	testF088XAckWait = 30 * time.Second
	// testF088XWindow bounds message 1's delivery on A to the completion of B's UNSUBSCRIBE save,
	// well inside AckWait: beyond it a redelivery to B could be in flight.
	testF088XWindow = 20 * time.Second
	// testF088XColdRead is the cold re-offer's read deadline: recovery installs the acting entry
	// without an ack subject, so nothing can NAK it and the re-offer waits for JetStream's own
	// redelivery after AckWait.
	testF088XColdRead = testF088XAckWait + 15*time.Second
)

// testF088Sections is the one process-global send-window hook, in three roles: it PARKS broker
// A's first section at RESERVED (and, once armed, at WROTE), it RECORDS every window A's section
// reaches, and it OBSERVES every section broker B opens. Sessions are told apart by their
// manager, never by client ID: both brokers' sessions carry the same one.
type testF088Sections struct {
	asmA, asmB *mqttAccountSessionManager

	mu       sync.Mutex
	pi       uint16 // message 1's identifier on A, captured at the park
	aWindows []string
	bOpened  []string // "pi/window" for every section window B reached
	bOnPI    bool     // B reached a section window for message 1's identifier

	parked      chan uint16
	parkOnce    sync.Once
	release     chan struct{}
	releaseOnce sync.Once

	// abortReserved, set before the release, makes A's parked section return false at
	// RESERVED: a holder that drains its token WITHOUT intent (W-MKDEBT, W-PS2T).
	abortReserved atomic.Bool

	wroteArmed   atomic.Bool
	wroteReached chan struct{}
	wroteOnce    sync.Once
	wroteRelease chan struct{}
	wroteRelOnce sync.Once

	// ended closes when A's released section for message 1 has drained its action and is about
	// to release its local hold (endPublishSend's drained window).
	released  atomic.Bool
	ended     chan struct{}
	endedOnce sync.Once
}

// testF088InstallSections installs the hook. Its release is registered as a cleanup AFTER the
// servers', so it runs BEFORE them: a parked section holds the shutdown barrier
// (admitPublishSend), and a failed test must not leave shutdown waiting on it.
func testF088InstallSections(t *testing.T, asmA, asmB *mqttAccountSessionManager) *testF088Sections {
	t.Helper()
	h := &testF088Sections{
		asmA: asmA, asmB: asmB,
		parked: make(chan uint16, 1), release: make(chan struct{}),
		wroteReached: make(chan struct{}), wroteRelease: make(chan struct{}),
		ended: make(chan struct{}),
	}
	restoreEnd := setMQTTTestPublishDrainedPause(func(pi uint16) {
		h.mu.Lock()
		mine := h.pi != 0 && pi == h.pi
		h.mu.Unlock()
		if mine && h.released.Load() {
			h.endedOnce.Do(func() { close(h.ended) })
		}
	})
	restore := setMQTTTestMarkerSendWindowHook(func(sess *mqttSession, pi uint16, window string) bool {
		switch {
		case asmB != nil && sess.asm == asmB:
			h.mu.Lock()
			h.bOpened = append(h.bOpened, fmt.Sprintf("%d/%s", pi, window))
			if h.pi != 0 && pi == h.pi {
				h.bOnPI = true
			}
			h.mu.Unlock()
		case sess.asm == asmA:
			h.mu.Lock()
			first := h.pi == 0 && window == mqttMarkerSendReserved
			if first {
				h.pi = pi
			}
			mine := pi == h.pi
			if mine {
				h.aWindows = append(h.aWindows, window)
			}
			h.mu.Unlock()
			if first {
				h.parkOnce.Do(func() { h.parked <- pi })
				<-h.release
				if h.abortReserved.Load() {
					// The section, already holding its acquired token, ends with no intent and
					// no write: the deferred endPublishSend drains the token, the marker stands.
					return false
				}
			}
			if mine && window == mqttMarkerSendWrote && h.wroteArmed.Load() {
				h.wroteOnce.Do(func() { close(h.wroteReached) })
				<-h.wroteRelease
			}
		}
		return true
	})
	t.Cleanup(func() {
		h.releaseReserved()
		h.releaseWrote()
		restore()
		restoreEnd()
	})
	return h
}

func (h *testF088Sections) releaseReserved() {
	h.releaseOnce.Do(func() {
		h.released.Store(true)
		close(h.release)
	})
}
func (h *testF088Sections) releaseWrote() { h.wroteRelOnce.Do(func() { close(h.wroteRelease) }) }

func (h *testF088Sections) windowsA() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.aWindows...)
}

func (h *testF088Sections) sawA(window string) bool {
	for _, w := range h.windowsA() {
		if w == window {
			return true
		}
	}
	return false
}

func (h *testF088Sections) openedB() ([]string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.bOpened...), h.bOnPI
}

// testF088Attempt is one authority transition broker B attempted over message 1's register.
type testF088Attempt struct {
	kind mqttTransitionKind
	txn  string
	err  error
	seq  uint64
	body *mqttInflightRecord // the register member the batch wrote
}

func (a testF088Attempt) String() string {
	b := "<nil>"
	if a.body != nil {
		b = fmt.Sprintf("{nonce %s state %v cancelled %v origin %q token %q}", a.body.Nonce, a.body.State,
			a.body.Cancelled, a.body.CancelOrigin, a.body.ActionToken)
	}
	return fmt.Sprintf("%s txn=%s seq=%d err=%v body=%s", a.kind, a.txn, a.seq, a.err, b)
}

// testF088RecordAttempts arms jsa's transition seam to record EVERY attempt whose batch names
// subject, refused ones included: the boundary checks run inside the wrapped send, so a refusal
// is the send's error.
func testF088RecordAttempts(t *testing.T, jsa *mqttJSA, subject string) func() []testF088Attempt {
	t.Helper()
	var mu sync.Mutex
	var seen []testF088Attempt
	testArmTransitionSeam(t, jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		var body *mqttInflightRecord
		names := false
		for _, m := range tr.Extra {
			if m.subj != subject {
				continue
			}
			names = true
			if r, derr := mqttDecodeInflightRecord(m.body); derr == nil {
				body = r
			}
		}
		seq, err := send()
		if names && tr.Attempt != nil {
			mu.Lock()
			seen = append(seen, testF088Attempt{kind: tr.Attempt.Kind, txn: tr.Attempt.Txn, err: err, seq: seq, body: body})
			mu.Unlock()
		}
		return seq, err
	})
	return func() []testF088Attempt {
		mu.Lock()
		defer mu.Unlock()
		return append([]testF088Attempt(nil), seen...)
	}
}

// testF088Packet is one packet the client read.
type testF088Packet struct {
	typ     byte
	flags   byte
	pi      uint16
	topic   string
	payload string
}

func (p testF088Packet) String() string {
	if p.typ == mqttPacketPub {
		return fmt.Sprintf("PUBLISH pi=%d dup=%v payload=%q", p.pi, p.flags&mqttPubFlagDup != 0, p.payload)
	}
	return fmt.Sprintf("packet 0x%x pi=%d", p.typ, p.pi)
}

// testF088Read reads one packet within wait. eof reports the connection ending (a read that gave
// up well before its wait), as distinct from a timeout.
func testF088Read(t *testing.T, r *mqttReader, wait time.Duration) (p testF088Packet, ok, eof bool) {
	t.Helper()
	t0 := time.Now()
	b, pl, got := testMQTTReadPacketReady(r, wait)
	if !got {
		return p, false, time.Since(t0) < wait/2
	}
	p.typ, p.flags = b&mqttPacketMask, b&mqttPacketFlagMask
	switch p.typ {
	case mqttPacketPub:
		flags, pi, topic, payload := testMQTTGetPubMsgExEx(t, nil, r, b, pl, _EMPTY_, nil)
		p.flags, p.pi, p.topic, p.payload = flags, pi, topic, string(payload)
	case mqttPacketPubAck, mqttPacketPubRec, mqttPacketPubRel, mqttPacketPubComp, mqttPacketUnsubAck:
		pi, err := r.readUint16("packet identifier")
		if err != nil {
			t.Fatalf("reading a packet identifier: %v", err)
		}
		p.pi = pi
		r.pos += pl - 2
	default:
		r.pos += pl
	}
	return p, true, false
}

// testF088Unsub sends UNSUBSCRIBE foo and reads until its UNSUBACK, recording every packet that
// arrives first.
func testF088Unsub(t *testing.T, c net.Conn, r *mqttReader, upi uint16, trace *[]testF088Packet) {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketUnsub | mqttUnsubscribeFlags)
	w.WriteVarInt(2 + 2 + len("foo"))
	w.WriteUint16(upi)
	w.WriteBytes([]byte("foo"))
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("writing UNSUBSCRIBE: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		p, ok, eof := testF088Read(t, r, time.Until(deadline))
		if eof {
			t.Fatalf("fixture: the connection on B ended before the UNSUBACK; packets %v", *trace)
		}
		if !ok {
			break
		}
		*trace = append(*trace, p)
		if p.typ == mqttPacketUnsubAck && p.pi == upi {
			return
		}
	}
	t.Fatalf("fixture: no UNSUBACK for %d within 10s; packets %v", upi, *trace)
}

// testF088XWorld is one client whose message 1 is parked in broker A's open send section while
// the client resumes on broker B under a new tenure.
type testF088XWorld struct {
	oA, oB     *Options
	sA, sB     *Server
	asmA, asmB *mqttAccountSessionManager
	sec        *testF088Sections

	cid, idHash, gen, epoch string
	tenureA, tenureB        mqttOutboundEntitlement

	// The client's ORIGINAL connection to A, kept open: its reader is the only wire witness of
	// what A's displaced writer did.
	cA net.Conn
	rA *mqttReader
	// The server side of that connection, for the writer and gate observations.
	ccA *client
	cB  net.Conn
	rB  *mqttReader

	sessA, sessB *mqttSession

	pi                uint16 // message 1's identifier, allocated on A
	nonce, lin        string // message 1's allocation and exchange lineage
	regSubj, markSubj string
	srcDur            string
	srcSeq            uint64
	pi2               uint16 // message 2's identifier, allocated on B
	lin2              string
	t0                time.Time // message 1's section opened on A
	writerReached     atomic.Bool
	pkts              []testF088Packet // every packet the client read on B
	pubConn           net.Conn
	pubR              *mqttReader

	// order is the race ordering the harness forces (see testF088Order).
	order         testF088Order
	regSeqB       uint64 // message 1's register sequence as B's entry names it
	evictHold     chan struct{}
	evictHoldOnce sync.Once
	evictReached  atomic.Bool // A's eviction worker reached the hold for this client
	// coldOffers counts the offers of message 1's source delivery that reach A's delivery callback
	// after the cold resume (set by coldOnA).
	coldOffers        func() int
	tokenA            string // the action token A's section acquired
	acqA              uint64 // that acquisition's identity on the register
	bindRun, bindConn string // A's original connection's binding identity
	certHold          chan struct{}
	certHoldOnce      sync.Once
	certHeld          atomic.Bool // A's persist reached the hold
}

// testF088Order is the race ordering between A's displacement and B's recovery. Every one is an
// ordering production can take; none is unreachable.
type testF088Order int

const (
	// testF088AFirst: A's certificate persisted before B's recovery cleared A's token (F-121's
	// schedule, as run 1 resolved naturally). CHECKED: anything else fails the fixture.
	testF088AFirst testF088Order = iota
	// testF088BFirstCertHeld: A's closure-certificate persist is HELD, so B's recovery runs with A's
	// token still on the register. A's gate is sealed regardless: sealing precedes persistence.
	testF088BFirstCertHeld
	// testF088BFirstEvictHeld: A's eviction worker is HELD before its record load
	// (testEvictPauseBeforeLoad), so A never certifies its connection: A's session stays resident,
	// its gate stays open, and its writer can really write to the old connection.
	testF088BFirstEvictHeld
)

func (o testF088Order) bFirst() bool { return o != testF088AFirst }

func (o testF088Order) String() string {
	return [...]string{"A-first", "B-first (certificate persist held)", "B-first (eviction held)"}[o]
}

// releaseEvict lets A's held eviction worker proceed.
func (w *testF088XWorld) releaseEvict() {
	if w.evictHold != nil {
		w.evictHoldOnce.Do(func() { close(w.evictHold) })
	}
}

// requireCertifiedWhileParked checks, while A is still PARKED at RESERVED and before it is released,
// that A's EXACT binding holds an in-memory closure certificate: the gate was sealed before A's
// intent could run. The INTENT window fires after recordSendIntent returns, too late to prove it.
func (w *testF088XWorld) requireCertifiedWhileParked(t *testing.T) {
	t.Helper()
	b := w.ccA.outBind
	if c := b.closeCert(); !mqttBindingCertifies(b, c) || b.mayWrite() {
		t.Fatalf("fixture: A's binding %s/%s must be certified (gate sealed) while A is parked at RESERVED: cert %+v, gate open %v",
			w.bindRun, w.bindConn, c, b.mayWrite())
	}
}

// releaseCert lets A's held certificate persist proceed.
func (w *testF088XWorld) releaseCert() {
	if w.certHold != nil {
		w.certHoldOnce.Do(func() { close(w.certHold) })
	}
}

// testF088AObservation is what A's displaced section did once released.
type testF088AObservation struct {
	intent        bool // the INTENT window fired: the marker's delete was confirmed
	markerGone    bool
	writerReached bool            // the writer reached wmu (its binding checks passed), nothing more
	gateOpen      bool            // A's output gate after the section
	oldPkt        *testF088Packet // message 1's PUBLISH, if the old reader read it
	oldOther      []testF088Packet
	oldEOF        bool
	reg           *mqttInflightRecord
}

func (o testF088AObservation) String() string {
	old := "nothing read within 2s"
	switch {
	case o.oldPkt != nil:
		old = "read " + o.oldPkt.String()
	case o.oldEOF:
		old = "connection ended, nothing read"
	}
	return fmt.Sprintf("intent(marker delete confirmed)=%v markerGone=%v writer reached wmu=%v A output gate open=%v "+
		"old reader: %s (other packets %v); register after the section: %+v", o.intent, o.markerGone, o.writerReached,
		o.gateOpen, old, o.oldOther, o.reg)
}

// wireReceipt reports whether the old connection's reader received message 1's PUBLISH.
func (o testF088AObservation) wireReceipt(pi uint16) bool {
	return o.oldPkt != nil && o.oldPkt.typ == mqttPacketPub && o.oldPkt.pi == pi
}

// testF088WarnLog records every warning a server logs.
type testF088WarnLog struct {
	DummyLogger
	mu    sync.Mutex
	warns []string
}

func (l *testF088WarnLog) Warnf(format string, v ...any) {
	l.mu.Lock()
	l.warns = append(l.warns, fmt.Sprintf(format, v...))
	l.mu.Unlock()
}

func (l *testF088WarnLog) matching(sub string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, m := range l.warns {
		if strings.Contains(m, sub) {
			out = append(out, m)
		}
	}
	return out
}

// testF088XHarness runs steps 1-6b of the plan: message 1 parked in A's section with the token
// acquired and the marker standing, the client resumed on B under a new tenure, and message 2
// started and unacknowledged on B so a real UNSUBSCRIBE drain is staged.
//
// order selects the ordering (testF088Order). For either B-first ordering the REACH of the premise is
// that B's entry acts, names A's allocation, has slot.actionLive over A's own token still on the
// register, and — since F-088's fix I — reads POSSIBLY ATTEMPTED with the marker retained as
// deletion debt (it read PROVEN-UNSENT before the fix: witness runs 1-3). For A-first the ordering
// itself is checked: A's certificate cleared the token before B's read, so B's entry is proven-unsent.
func testF088XHarness(t *testing.T, cid string, order testF088Order) *testF088XWorld {
	t.Helper()
	return testF088XHarnessWith(t, cid, order, nil)
}

// testF088XHarnessWith is testF088XHarness with beforeResume, when non-nil, run once message 1 is
// parked on A and before the client resumes on B: the point where a test seeds PERSISTED state
// (a verdict made durable on A's session record) for B's recovery to meet.
func testF088XHarnessWith(t *testing.T, cid string, order testF088Order, beforeResume func(w *testF088XWorld)) *testF088XWorld {
	t.Helper()
	w := &testF088XWorld{cid: cid, idHash: getHash(cid), order: order}
	w.oA = testMQTTDefaultOptions()
	w.oA.MQTT.AckWait = testF088XAckWait
	w.sA = testMQTTRunServer(t, w.oA)
	t.Cleanup(func() { testMQTTShutdownServer(w.sA) }) // owns the shared nats-server: last
	w.oB = testMQTTDefaultOptions()
	w.oB.MQTT.AckWait = testF088XAckWait
	w.oB.StoreDir = w.oA.StoreDir
	w.sB = testMQTTRunServer(t, w.oB)
	t.Cleanup(func() { w.sB.Shutdown() })
	probe, probeR := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-probe", cleanSess: true}, w.oB.MQTT.Host, w.oB.MQTT.Port)
	testMQTTCheckConnAck(t, probeR, mqttConnAckRCConnectionAccepted, false)
	w.asmB = testMQTTGetAccountSessionManager(t, w.sB, cid+"-probe")
	probe.Close()

	// (1) The subscriber on A, persistent, QoS 1 on foo.
	w.cA, w.rA = testInflightSubscriber(t, w.oA, cid, 1)
	t.Cleanup(func() { w.cA.Close() })
	w.asmA, w.sessA = testResidentSession(t, w.sA, cid)
	w.sessA.mu.Lock()
	w.ccA = w.sessA.c
	w.gen, w.epoch, w.tenureA = w.sessA.gen, w.asmA.sessStreamEpoch, w.sessA.boundEnt
	w.sessA.mu.Unlock()
	if w.ccA == nil || w.ccA.outBind == nil {
		t.Fatalf("fixture: A's connection carries no output binding")
	}
	w.bindRun, w.bindConn = w.ccA.outBind.run, w.ccA.outBind.conn

	// (2) The park, and the writer observation on A's original connection.
	w.sec = testF088InstallSections(t, w.asmA, w.asmB)
	ccA := w.ccA
	pre := func(c *client) {
		if c == ccA {
			w.writerReached.Store(true)
		}
	}
	mqttTestWriterPreLockHookV.Store(&pre)
	t.Cleanup(func() { mqttTestWriterPreLockHookV.Store(nil) })
	switch order {
	case testF088BFirstEvictHeld:
		// The hold on A's eviction worker, for this client only. Registered after the servers so
		// its release runs before their shutdown (the worker holds the will-worker group).
		w.evictHold = make(chan struct{})
		idHash := w.idHash
		w.asmA.mu.Lock()
		w.asmA.testEvictPauseBeforeLoad = func(h string) {
			if h != idHash {
				return
			}
			w.evictReached.Store(true)
			<-w.evictHold
		}
		w.asmA.mu.Unlock()
		t.Cleanup(func() {
			w.releaseEvict()
			w.asmA.mu.Lock()
			w.asmA.testEvictPauseBeforeLoad = nil
			w.asmA.mu.Unlock()
		})
	case testF088BFirstCertHeld:
		// The hold on A's certificate persist, filtered to A's binding. Registered after the
		// servers so its release runs before their shutdown.
		w.certHold = make(chan struct{})
		run, conn := w.bindRun, w.bindConn
		hold := func(c *mqttDurableCloseCert) bool {
			if c.Run != run || c.Conn != conn {
				return true
			}
			w.certHeld.Store(true)
			<-w.certHold
			return true
		}
		prev := mqttTestCloseCertPersistV.Swap(&hold)
		t.Cleanup(func() {
			w.releaseCert()
			mqttTestCloseCertPersistV.Store(prev)
		})
	}

	// (3) Message 1: A's real delivery callback admits it, opens the section, acquires the token
	// and parks.
	w.pubConn, w.pubR = testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, w.oA.MQTT.Host, w.oA.MQTT.Port)
	t.Cleanup(func() { w.pubConn.Close() })
	testMQTTCheckConnAck(t, w.pubR, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, w.pubConn, w.pubR, 1, false, false, "foo", 1, []byte("one"))
	select {
	case w.pi = <-w.sec.parked:
	case <-time.After(10 * time.Second):
		t.Fatal("fixture: A's send section for message 1 never opened")
	}
	w.t0 = time.Now()
	w.regSubj = mqttInflightSubject(w.idHash, w.gen, w.pi)
	w.markSubj = w.regSubj + "." + mqttInflightMarkerSuffix
	reg, _, err := w.asmA.jsa.loadInflightRecord(w.regSubj)
	if err != nil || reg == nil || reg.ActionToken == _EMPTY_ {
		t.Fatalf("fixture: A's parked section must hold the ACQUIRED action token: %+v (%v)", reg, err)
	}
	if smsg, _ := w.asmA.jsa.loadLastMsgFor(mqttInflightStreamName, w.markSubj); smsg == nil {
		t.Fatalf("fixture: the unsent marker must stand while A is parked at RESERVED")
	}
	w.nonce, w.lin, w.srcDur, w.srcSeq, w.tokenA = reg.Nonce, reg.Lin, reg.Dur, reg.Seq, reg.ActionToken
	w.acqA = reg.Acq
	if x, _, xerr := w.asmA.jsa.loadOutboundExchange(cid, w.epoch, w.gen, w.lin); xerr != nil || x == nil ||
		x.Phase != mqttExchangePublish {
		t.Fatalf("fixture: message 1's exchange must stand at Publish: %+v (%v)", x, xerr)
	}

	if beforeResume != nil {
		beforeResume(w)
	}

	// (5) The client resumes on B. A's original connection stays open on the client side.
	w.cB, w.rB = testInflightReconnect(t, w.oB, cid)
	t.Cleanup(func() { w.cB.Close() })
	var asmB *mqttAccountSessionManager
	asmB, w.sessB = testResidentSession(t, w.sB, cid)
	if asmB != w.asmB {
		t.Fatalf("fixture: the probe reached a different session manager on B")
	}

	// (6) The tenure change, then the ordering-specific premise.
	w.sessB.mu.Lock()
	genB := w.sessB.gen
	w.tenureB = w.sessB.boundEnt
	w.sessB.mu.Unlock()
	if genB != w.gen || w.tenureB.Tenure == _EMPTY_ || w.tenureB.Tenure == w.tenureA.Tenure {
		t.Fatalf("REACH: B must serve generation %q under a NEW tenure: gen %q, tenure A %q, B %q",
			w.gen, genB, w.tenureA.Tenure, w.tenureB.Tenure)
	}
	if order != testF088BFirstEvictHeld {
		// Under the eviction hold A stays resident by construction; that test asserts A's eviction
		// after releasing it.
		w.requireANotResident(t)
	} else {
		checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
			if !w.evictReached.Load() {
				return fmt.Errorf("fixture: A's eviction worker never reached the hold")
			}
			return nil
		})
	}
	regB, _, _ := w.asmB.jsa.loadInflightRecord(w.regSubj)
	w.sessB.mu.Lock()
	ack := w.sessB.pendingPublish[w.pi]
	var desc string
	looksUnsent := ack != nil && ack.acts() && ack.regNonce == w.nonce && ack.markerSeq != 0 &&
		w.sessB.provenUnsentReleasableLocked(ack)
	// POSSIBLY ATTEMPTED with the marker kept as debt: what B must derive over A's outstanding
	// output token (F-088 fix I).
	possiblyAttempted := ack != nil && ack.acts() && ack.regNonce == w.nonce && ack.markerSeq != 0 &&
		ack.transmitted && !w.sessB.provenUnsentReleasableLocked(ack)
	actionLive := ack != nil && ack.slot.actionLive
	if ack != nil {
		w.regSeqB = ack.regSeq
		desc = fmt.Sprintf("{acts %v nonce %s markerSeq %d actionLive %v transmitted %v sendNonce %q releasable %v}",
			ack.acts(), ack.regNonce, ack.markerSeq, ack.slot.actionLive, ack.transmitted, ack.sendNonce,
			w.sessB.provenUnsentReleasableLocked(ack))
	}
	w.sessB.mu.Unlock()
	if order.bFirst() {
		tokenStands := regB != nil && regB.Nonce == w.nonce && regB.ActionToken == w.tokenA
		if !possiblyAttempted || !actionLive || !tokenStands {
			t.Errorf("REACH: %v (certificate persist held: %v, eviction held: %v), B's entry for %d must act, "+
				"name A's allocation %s, carry the live action and read POSSIBLY ATTEMPTED (transmitted, the marker "+
				"retained as debt, not releasable), over A's token %q still on the register: entry %s; register "+
				"after B's resume %+v",
				order, w.certHeld.Load(), w.evictReached.Load(), w.pi, w.nonce, w.tokenA, desc, regB)
			obs := w.observeA(t)
			t.Logf("after the failed premise, A's section: %v", obs)
			t.FailNow()
		}
		t.Logf("premise reached on B (%v; certificate persist held=%v, eviction held=%v): entry %d %s",
			order, w.certHeld.Load(), w.evictReached.Load(), w.pi, desc)
	} else {
		cert, cerr := w.asmB.jsa.loadDurableCloseCert(cid, w.epoch, w.gen, w.bindRun, w.bindConn)
		if !looksUnsent || actionLive || regB == nil || regB.ActionToken != _EMPTY_ || cert == nil {
			t.Fatalf("fixture: the A-FIRST ordering was not obtained: B's entry must read proven-unsent with "+
				"actionLive=false over a register A's persisted certificate cleared: entry %s; register %+v; "+
				"A's certificate %+v (%v)", desc, regB, cert, cerr)
		}
		if smsg, _ := w.asmB.jsa.loadLastMsgFor(mqttInflightStreamName, w.markSubj); smsg == nil {
			t.Fatalf("fixture: the marker must still stand before A is released (A parked at RESERVED)")
		}
		t.Logf("A-first ordering checked: certificate %+v persisted, token cleared; entry %d %s", *cert, w.pi, desc)
	}

	// (6b) Message 2, delivered by B through B's own section and NOT acknowledged.
	w.startMessage2(t)
	return w
}

// requireANotResident waits for broker A to let go of the displaced session.
func (w *testF088XWorld) requireANotResident(t *testing.T) {
	t.Helper()
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		w.asmA.mu.RLock()
		_, resident := w.asmA.sessions[w.cid]
		w.asmA.mu.RUnlock()
		if resident {
			return fmt.Errorf("REACH: broker A still holds the displaced session resident")
		}
		return nil
	})
}

// startMessage2 is step 6b: message 2 published, delivered to the client on B through B's own
// section, and left unacknowledged, so B's UNSUBSCRIBE stages a real drain.
func (w *testF088XWorld) startMessage2(t *testing.T) {
	t.Helper()
	testMQTTPublish(t, w.pubConn, w.pubR, 1, false, false, "foo", 2, []byte("two"))
	deadline := time.Now().Add(10 * time.Second)
	for w.pi2 == 0 && time.Now().Before(deadline) {
		p, ok, eof := testF088Read(t, w.rB, time.Until(deadline))
		if eof {
			t.Fatalf("fixture: the client's connection on B ended; packets %v", w.pkts)
		}
		if !ok {
			break
		}
		w.pkts = append(w.pkts, p)
		if p.typ == mqttPacketPub && p.payload == "two" {
			w.pi2 = p.pi
		}
	}
	if w.pi2 == 0 {
		t.Fatalf("fixture: message 2 never reached the client on B; packets %v", w.pkts)
	}
	if w.pi2 == w.pi {
		t.Fatalf("CONTRACT: B allocated message 1's identifier %d to message 2 while message 1's exchange is un-ended", w.pi)
	}
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		w.sessB.mu.Lock()
		defer w.sessB.mu.Unlock()
		a2 := w.sessB.pendingPublish[w.pi2]
		if a2 == nil || !a2.acts() || !a2.transmitted {
			return fmt.Errorf("fixture: message 2 must stand started on B (acting, transmitted): %+v", a2)
		}
		return nil
	})
	reg2, _, _ := w.asmB.jsa.loadInflightRecord(mqttInflightSubject(w.idHash, w.gen, w.pi2))
	if reg2 == nil {
		t.Fatalf("fixture: message 2's register does not load")
	}
	w.lin2 = reg2.Lin
}

// observeA releases A's parked section and reports what it did, every observation through
// production's own hooks and the stores: the confirmed marker delete (INTENT), the enqueue's
// return (WROTE, which is not proof of a write), the writer reaching its lock, A's output gate,
// the old connection's reader, and A's token drain.
func (w *testF088XWorld) observeA(t *testing.T) testF088AObservation {
	t.Helper()
	w.sec.releaseReserved()
	// The section ends through endPublishSend, after its drain of the action.
	select {
	case <-w.sec.ended:
	case <-time.After(15 * time.Second):
		t.Fatalf("fixture: A's released section never reached its end; windows %v; state %s",
			w.sec.windowsA(), w.state(w.sessB, w.asmB))
	}
	var o testF088AObservation
	o.reg, _, _ = w.asmA.jsa.loadInflightRecord(w.regSubj)
	o.intent = w.sec.sawA(mqttMarkerSendIntent)
	if smsg, _ := w.asmA.jsa.loadLastMsgFor(mqttInflightStreamName, w.markSubj); smsg == nil {
		o.markerGone = true
	}
	o.writerReached = w.writerReached.Load()
	o.gateOpen = w.ccA.outBind.mayWrite()
	// The old connection's reader: a PUBLISH of message 1 is wire receipt; an ended connection
	// or silence is not.
	// Every packet up to message 1's PUBLISH is kept, so another delivery on the old connection
	// is reported rather than mistaken for it.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p, ok, eof := testF088Read(t, w.rA, time.Until(deadline))
		if !ok {
			o.oldEOF = eof
			break
		}
		if p.typ == mqttPacketPub && p.pi == w.pi {
			o.oldPkt = &p
			break
		}
		o.oldOther = append(o.oldOther, p)
	}
	t.Logf("A's displaced section: windows %v; %v", w.sec.windowsA(), o)
	if o.intent && !o.markerGone {
		t.Fatalf("fixture: the INTENT window fired but the marker still stands")
	}
	return o
}

// checkWindow fails the FIXTURE when the premise's window was not held: the elapsed time since
// message 1's section opened on A exceeds the bound, or B opened any section for message 1.
func (w *testF088XWorld) checkWindow(t *testing.T, when string) {
	t.Helper()
	if el := time.Since(w.t0); el > testF088XWindow {
		t.Fatalf("fixture window expired at %s: %v since message 1's section opened on A (bound %v)", when, el, testF088XWindow)
	}
	if opened, onPI := w.sec.openedB(); onPI {
		t.Fatalf("fixture: B opened a section for message 1's identifier %d by %s: %v", w.pi, when, opened)
	}
}

// state dumps everything the contract assertions read, for a failure message.
func (w *testF088XWorld) state(sess *mqttSession, asm *mqttAccountSessionManager) string {
	var b strings.Builder
	x, xseq, xerr := asm.jsa.loadOutboundExchange(w.cid, w.epoch, w.gen, w.lin)
	fmt.Fprintf(&b, "exchange %+v@%d (%v); ", x, xseq, xerr)
	reg, _, rerr := asm.jsa.loadInflightRecord(w.regSubj)
	fmt.Fprintf(&b, "register %+v (%v); ", reg, rerr)
	smsg, _ := asm.jsa.loadLastMsgFor(mqttInflightStreamName, w.markSubj)
	fmt.Fprintf(&b, "marker present %v; ", smsg != nil)
	if sess != nil {
		sess.mu.Lock()
		if a := sess.pendingPublish[w.pi]; a != nil {
			fmt.Fprintf(&b, "entry {kind %v nonce %s transmitted %v markerSeq %d sendNonce %q settle %v regCancelled %v}; ",
				a.kind, a.regNonce, a.transmitted, a.markerSeq, a.sendNonce, a.sendSettle.kind, a.regCancelled)
		} else {
			b.WriteString("entry absent; ")
		}
		fmt.Fprintf(&b, "ledger holdsPI %v chargesPI %v; sendReserved %v; occupancy %d; ",
			sess.custody.holdsPI(w.pi), sess.custody.chargesPI(w.pi), sess.sendReservedLocked(w.pi),
			sess.receiveMaxOccupancyLocked())
		var routes []string
		for _, ab := range sess.abandonedAcks {
			if ab != nil && ab.name.pi == w.pi {
				routes = append(routes, fmt.Sprintf("%+v", *ab))
			}
		}
		fmt.Fprintf(&b, "ack bindings on %d: %v; ", w.pi, routes)
		insertRev, held := sess.cancelledRegNonces[w.nonce]
		fmt.Fprintf(&b, "verdict held %v insertRev %d savedRev %d", held, insertRev, sess.voidedRegsSavedRev)
		sess.mu.Unlock()
	}
	return b.String()
}

// released reports nil once message 1's exchange is Terminal and its identifier and charge are
// released on sess.
func (w *testF088XWorld) released(sess *mqttSession, asm *mqttAccountSessionManager) error {
	x, _, err := asm.jsa.loadOutboundExchange(w.cid, w.epoch, w.gen, w.lin)
	if err != nil || x == nil || x.Phase != mqttExchangeTerminal {
		return fmt.Errorf("the exchange is not Terminal")
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if a := sess.pendingPublish[w.pi]; a != nil && a.regNonce == w.nonce {
		return fmt.Errorf("the entry still stands")
	}
	if sess.custody.holdsPI(w.pi) || sess.custody.chargesPI(w.pi) || sess.sendReservedLocked(w.pi) {
		return fmt.Errorf("the identifier or its charge is still held")
	}
	return nil
}

// awaitReleased polls released; a miss is a CONTRACT failure reported with the dump.
func (w *testF088XWorld) awaitReleased(t *testing.T, sess *mqttSession, asm *mqttAccountSessionManager, what string) bool {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := w.released(sess, asm)
		if err == nil {
			return true
		}
		if time.Now().After(deadline) {
			t.Errorf("%s: %v after 15s without another offer or resume; state: %s; packets %v",
				what, err, w.state(sess, asm), w.pkts)
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// checkNoConflict is the assertion both contracts share: B never opened a section for message
// 1's identifier (so never allocated or transmitted it) while the exchange was un-ended, and the
// generation holds no exchange beyond message 1's and message 2's (no second exchange for the
// source delivery).
func (w *testF088XWorld) checkNoConflict(t *testing.T, asm *mqttAccountSessionManager) {
	t.Helper()
	if opened, onPI := w.sec.openedB(); onPI {
		t.Errorf("CONTRACT: B opened a section for message 1's identifier %d: %v", w.pi, opened)
	}
	subjects, _, err := asm.jsa.streamSubjectsPage(mqttInflightStreamName, mqttExchangeWalkFilter(w.idHash, w.gen), 0)
	if err != nil {
		t.Errorf("listing the generation's exchanges: %v", err)
		return
	}
	for s := range subjects {
		lin := s[strings.LastIndexByte(s, '.')+1:]
		if lin != w.lin && lin != w.lin2 {
			x, _, _ := asm.jsa.loadOutboundExchange(w.cid, w.epoch, w.gen, lin)
			t.Errorf("CONTRACT: a further exchange %q exists beside message 1's %q and message 2's %q: %+v",
				lin, w.lin, w.lin2, x)
		}
	}
}

// testF088Cancel is one cancellation recovery's classification met.
type testF088Cancel struct {
	pi                    uint16
	nonce                 string
	recCancelled, verdict bool
	markerSeq             uint64
}

// coldOnA disconnects the client from B and reconnects it to A, a THIRD tenure, with recovery's
// cancellation classification observed and A's advances captured. It returns the resumed
// session, the classifications met, the advance capture, and the new connection.
func (w *testF088XWorld) coldOnA(t *testing.T) (*mqttSession, []testF088Cancel, func() *mqttInflightRecord, net.Conn, *mqttReader) {
	t.Helper()
	// A persistent session stays resident on B after a plain disconnect; the reconnect to A takes
	// it over across brokers, which is asserted after the CONNECT below.
	w.cB.Close()
	var mu sync.Mutex
	var seen []testF088Cancel
	w.asmA.mu.Lock()
	w.asmA.testRecoverCancellation = func(pi uint16, nonce string, recCancelled, verdict bool, markerSeq uint64) {
		mu.Lock()
		seen = append(seen, testF088Cancel{pi, nonce, recCancelled, verdict, markerSeq})
		mu.Unlock()
	}
	w.asmA.mu.Unlock()
	t.Cleanup(func() {
		w.asmA.mu.Lock()
		w.asmA.testRecoverCancellation = nil
		w.asmA.mu.Unlock()
	})
	var amu sync.Mutex
	var advanced *mqttInflightRecord
	testArmTransitionSeam(t, &w.asmA.jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err == nil && tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionAdvanceExchange {
			for _, m := range tr.Extra {
				if m.subj != w.regSubj {
					continue
				}
				if r, derr := mqttDecodeInflightRecord(m.body); derr == nil && r.Nonce == w.nonce {
					amu.Lock()
					advanced = r
					amu.Unlock()
				}
			}
		}
		return seq, err
	})
	// A real reconnecting client is jailed for mqttSessJailDur after A's takeover marked the client
	// ID a flapper (DuplicateClientID); the witness waits that out rather than be refused.
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		w.asmA.mu.Lock()
		tm, jailed := w.asmA.flappers[w.cid]
		w.asmA.mu.Unlock()
		if jailed && time.Since(tm) <= mqttSessJailDur {
			return fmt.Errorf("fixture: the client ID is still jailed on A")
		}
		return nil
	})
	var offers atomic.Int64
	restoreOffers := setMQTTTestDeliveryOfferedHook(func(sub *subscription, reply string) {
		if sub == nil || sub.client == nil || sub.client.srv != w.sA {
			return
		}
		if mqttSourceAckReplyBinds(reply, mqttStreamName, w.srcDur, w.srcSeq) {
			offers.Add(1)
		}
	})
	t.Cleanup(restoreOffers)
	w.coldOffers = func() int { return int(offers.Load()) }
	c3, r3 := testInflightReconnect(t, w.oA, w.cid)
	t.Cleanup(func() { c3.Close() })
	_, sess3 := testResidentSession(t, w.sA, w.cid)
	sess3.mu.Lock()
	ten3 := sess3.boundEnt
	sess3.mu.Unlock()
	if ten3.Tenure == _EMPTY_ || ten3.Tenure == w.tenureA.Tenure || ten3.Tenure == w.tenureB.Tenure {
		t.Fatalf("fixture: the reconnect to A must take a THIRD tenure: A %q, B %q, now %q",
			w.tenureA.Tenure, w.tenureB.Tenure, ten3.Tenure)
	}
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		w.asmB.mu.RLock()
		_, resident := w.asmB.sessions[w.cid]
		w.asmB.mu.RUnlock()
		if resident {
			return fmt.Errorf("fixture: broker B still holds the session resident after the takeover on A")
		}
		return nil
	})
	mu.Lock()
	met := append([]testF088Cancel(nil), seen...)
	mu.Unlock()
	return sess3, met, func() *mqttInflightRecord {
		amu.Lock()
		defer amu.Unlock()
		return advanced
	}, c3, r3
}

// coldContract runs the cold contract after coldOnA: message 1 restored ACTING, re-offered with
// DUP=1 within AckWait + 15 s, ended by its PUBACK, and the advance clears the cancellation.
func (w *testF088XWorld) coldContract(t *testing.T, sess3 *mqttSession, met []testF088Cancel, byRecord, byVerdict bool,
	advanced func() *mqttInflightRecord, c3 net.Conn, r3 *mqttReader) {
	t.Helper()
	var hit *testF088Cancel
	for i := range met {
		if met[i].pi == w.pi && met[i].nonce == w.nonce {
			hit = &met[i]
		}
	}
	if hit == nil || hit.markerSeq != 0 || (byRecord && !hit.recCancelled) || (byVerdict && !hit.verdict) {
		t.Fatalf("REACH (cold): recovery's cancellation classification must meet %d/%s markerless, cancelled "+
			"by record=%v verdict=%v; met %+v; state %s", w.pi, w.nonce, byRecord, byVerdict, met, w.state(sess3, w.asmA))
	}
	t.Logf("cold classification met: %+v", *hit)
	sess3.mu.Lock()
	ent := sess3.pendingPublish[w.pi]
	acting := ent != nil && ent.regNonce == w.nonce && ent.acts()
	sess3.mu.Unlock()
	if !acting {
		t.Errorf("CONTRACT (cold): message 1 must be restored ACTING on A's third tenure; state %s", w.state(sess3, w.asmA))
		return
	}
	start := time.Now()
	deadline := start.Add(testF088XColdRead)
	var got *testF088Packet
	var cold []testF088Packet
	for got == nil && time.Now().Before(deadline) {
		p, ok, eof := testF088Read(t, r3, time.Until(deadline))
		if eof {
			t.Fatalf("fixture: the client's connection on A ended while awaiting the re-offer; packets %v", cold)
		}
		if !ok {
			break
		}
		cold = append(cold, p)
		if p.typ == mqttPacketPub && p.pi == w.pi {
			got = &cold[len(cold)-1]
		}
	}
	if got == nil && w.coldOffers() > 0 {
		// Not a timeout: JetStream's re-offer of message 1 REACHED A's delivery callback, and no
		// PUBLISH followed. That is the cold contract failing, never a fixture result.
		ci, cerr := w.asmA.jsa.lookupConsumer(mqttStreamName, w.srcDur)
		t.Errorf("CONTRACT (cold): message 1's re-offer reached A's delivery callback %d time(s) within %v and no "+
			"PUBLISH of identifier %d followed; consumer %q %+v (%v); packets %v; state %s",
			w.coldOffers(), testF088XColdRead, w.pi, w.srcDur, ci, cerr, cold, w.state(sess3, w.asmA))
		return
	}
	if got == nil {
		ci, cerr := w.asmA.jsa.lookupConsumer(mqttStreamName, w.srcDur)
		t.Fatalf("fixture timeout: no PUBLISH of identifier %d within %v (consumer lifecycle outcome if the "+
			"source cannot re-offer: consumer %q %+v (%v)); packets %v; state %s",
			w.pi, testF088XColdRead, w.srcDur, ci, cerr, cold, w.state(sess3, w.asmA))
	}
	t.Logf("cold re-offer after %v: %v (packets %v; offers of message 1 reaching A: %d)",
		time.Since(start).Round(time.Millisecond), *got, cold, w.coldOffers())
	if got.payload != "one" || got.flags&mqttPubFlagDup == 0 {
		t.Errorf("CONTRACT (cold): message 1's retransmission must carry DUP=1 and its payload, got %v", *got)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c3, w.pi)
	w.awaitReleased(t, sess3, w.asmA, "CONTRACT (C1 closure, cold PUBACK on A after the observed retransmission)")
	if a := advanced(); a == nil || a.Cancelled || a.CancelOrigin != _EMPTY_ {
		t.Errorf("CONTRACT (cold): the PUBACK's advance must clear Cancelled and CancelOrigin, got %+v", a)
	}
}

// requireTombstoneLanded waits for B's teardown, finding no marker naming A's allocation, to land
// the standalone occupant tombstone over the register A drained, with the exchange still at
// Publish. label names what a miss is (WP1's REACH, or F-121's characterization).
func (w *testF088XWorld) requireTombstoneLanded(t *testing.T, label string, attempts func() []testF088Attempt) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, a := range attempts() {
			if a.kind != mqttTransitionTombstoneRegister || a.err != nil {
				continue
			}
			if a.body == nil || !a.body.Cancelled || a.body.CancelOrigin != mqttCancelOriginOccupant || a.body.Nonce != w.nonce {
				t.Fatalf("%s: the landed tombstone does not cancel A's allocation %s as an occupant: %v", label, w.nonce, a)
			}
			x, _, err := w.asmB.jsa.loadOutboundExchange(w.cid, w.epoch, w.gen, w.lin)
			if err != nil || x == nil || x.Phase != mqttExchangePublish {
				t.Fatalf("%s: the tombstone must leave the exchange at Publish: %+v (%v)", label, x, err)
			}
			t.Logf("%s: tombstone landed: %v", label, a)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: no tombstone-register transition landed for %s on B; attempts %v; state %s",
				label, w.regSubj, attempts(), w.state(w.sessB, w.asmB))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// requireEnrolledNoTombstone asserts what B's UNSUBSCRIBE does with a POSSIBLY ATTEMPTED message 1
// (F-088 fix I): its drain CLAIMS the delivery as started, B keeps the acting entry, and no
// tombstone reaches the commit seam.
func (w *testF088XWorld) requireEnrolledNoTombstone(t *testing.T, label string, attempts func() []testF088Attempt) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		w.sessB.mu.Lock()
		st := w.sessB.drainSeqLocked(w.srcDur, w.srcSeq)
		claimed := st != nil && st.claimed
		a := w.sessB.pendingPublish[w.pi]
		kept := a != nil && a.regNonce == w.nonce && a.acts() && a.transmitted
		w.sessB.mu.Unlock()
		for _, at := range attempts() {
			if at.kind == mqttTransitionTombstoneRegister {
				t.Fatalf("%s: a possibly-attempted delivery must be enrolled, never tombstoned: %v", label, at)
			}
		}
		if claimed && kept {
			t.Logf("%s: message 1 enrolled in the drain (claimed), entry kept, no tombstone", label)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: B's UNSUBSCRIBE must enrol message 1 (drain claimed %v, entry kept %v); attempts %v; state %s",
				label, claimed, kept, attempts(), w.state(w.sessB, w.asmB))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// testF088PS1 is WP1 through the REACH for the cold test. A-FIRST since F-088's fix I: a B-first
// recovery now reads A's outstanding token as a possible attempt and enrols the delivery, so the
// only schedule left that hands B's teardown a tombstone input is A-first — A's certificate
// cleared the token before B's read, and A's post-certification intent (F-121) then removes the
// marker B's proven-unsent entry relied on.
func testF088PS1(t *testing.T, cid string) (*testF088XWorld, func() []testF088Attempt) {
	t.Helper()
	w := testF088XHarness(t, cid, testF088AFirst)
	// (7) A's real section continues: intent, the writer, and its own drain of the token.
	obs := w.observeA(t)
	if !obs.intent {
		t.Logf("observed: A recorded NO intent; the register is not possibly-attempted on this run")
	}
	if obs.wireReceipt(w.pi) {
		t.Logf("observed: message 1 REACHED the client on A's old connection")
	}
	attempts := testF088RecordAttempts(t, &w.asmB.jsa, w.regSubj)
	testF088Unsub(t, w.cB, w.rB, 7, &w.pkts)
	w.checkWindow(t, "B's UNSUBSCRIBE")
	w.requireTombstoneLanded(t, "REACH", attempts)
	return w, attempts
}

// PS-1, COLD. A-first (re-checked under F-088's fix I, which removed the B-first input): A's
// certificate cleared its token before B's recovery, so B derives a proven-unsent entry; A's
// released section then records intent after certification (F-121) with its gate sealed, so
// nothing is written; B's UNSUBSCRIBE (a real drain, message 2 started) lands the standalone
// occupant tombstone over the now markerless register. No PUBACK on B; the client resumes on A under a
// third tenure. Recovery's classification must meet message 1 markerless and cancelled, restore it
// ACTING, re-offer it with DUP=1, and its PUBACK — legitimate, because it answers a retransmission
// the client was observed to receive — must end it with the advance clearing the cancellation.
//
// Witness: the testRecoverCancellation seam (REACH, cold), the entry's kind, the client's reader
// (DUP), the exchange phase and the captured advance. Input: production-reached on two real
// Servers sharing one JetStream; A's delivery held at RESERVED, the A-first ordering checked; the
// re-offer is JetStream's own AckWait redelivery (read deadline AckWait + 15 s). Not exercised:
// abrupt process death; the production AckWait race.
//
// Mutations: X1 (no standalone CAS when withdrawIfEvidenced returns 0: the tombstone REACH fails),
// X3 (the genuine-cancellation arm drops its marker conjunct: message 1 is not restored acting).
func TestF088CrossTenureTombstoneOverAPossiblyAttemptedRegisterCold(t *testing.T) {
	// Not parallel: the send-window, writer and certificate hooks are process-wide.
	w, _ := testF088PS1(t, "f088-ps1-cold")
	sess3, met, advanced, c3, r3 := w.coldOnA(t)
	w.coldContract(t, sess3, met, true, false, advanced, c3, r3)
	w.checkNoConflict(t, w.asmA)
}

// testF088SeedPersistedVerdict makes a cancellation verdict naming nonce DURABLE on sess's session
// record: staged through the production entry under the object's accepted entitlement and covered
// by a landed save. This is SEEDED PERSISTED STATE, stated as such: since F-088's fix I no
// production schedule this file drives stages it over a possibly-attempted register (witness run 3
// is the production reach evidence for the shape).
func testF088SeedPersistedVerdict(t *testing.T, sess *mqttSession, nonce string) {
	t.Helper()
	sess.mu.Lock()
	staged := sess.stageCancellationVerdictLocked(mqttTestLicenceLocked(sess, nil), nonce)
	sess.mu.Unlock()
	if !staged {
		t.Fatalf("fixture: the verdict for %s must be staged under the accepted entitlement", nonce)
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("fixture: the save that makes the verdict durable: %v", err)
	}
	sess.mu.Lock()
	insertRev, held := sess.cancelledRegNonces[nonce]
	covered := held && insertRev <= sess.voidedRegsSavedRev
	sess.mu.Unlock()
	if !covered {
		t.Fatalf("fixture: the verdict for %s must be DURABLY covered (held %v insert %d)", nonce, held, insertRev)
	}
}

// testF088RecordCarriesVerdict reads the session record back from the stream and reports whether it
// carries nonce among its verdicts.
func testF088RecordCarriesVerdict(t *testing.T, asm *mqttAccountSessionManager, cid, nonce string) bool {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, getHash(cid))
	if err != nil || smsg == nil {
		t.Fatalf("reading the session record of %q: %v", cid, err)
	}
	var ps mqttPersistedSession
	if uerr := json.Unmarshal(smsg.Data, &ps); uerr != nil {
		t.Fatalf("decoding the session record of %q: %v", cid, uerr)
	}
	for _, n := range ps.CancelledRegs {
		if n == nonce {
			return true
		}
	}
	return false
}

// testF088VerdictRevisions observes, from recovery's cancellation seam, the verdict revisions the
// recovering object held BEFORE the restore could retire anything (P8-3): credited and equal is the
// precondition the retirement's revision move is measured against.
type testF088VerdictRevisions struct {
	mu            sync.Mutex
	met           []testF088Cancel
	seen          bool
	rev, savedRev uint64
}

func testF088ObserveVerdictRevisions(t *testing.T, asm *mqttAccountSessionManager, cid string) *testF088VerdictRevisions {
	t.Helper()
	v := &testF088VerdictRevisions{}
	asm.mu.Lock()
	asm.testRecoverCancellation = func(pi uint16, nonce string, recCancelled, verdict bool, markerSeq uint64) {
		// The seam runs in recovery's classification, outside the session and manager locks; the
		// object is read without blocking so the observation can never stall the recovery.
		var rev, saved uint64
		ok := false
		if asm.mu.TryRLock() {
			s := asm.sessions[cid]
			asm.mu.RUnlock()
			if s != nil && s.mu.TryLock() {
				rev, saved, ok = s.voidedRegsRev, s.voidedRegsSavedRev, true
				s.mu.Unlock()
			}
		}
		v.mu.Lock()
		v.met = append(v.met, testF088Cancel{pi, nonce, recCancelled, verdict, markerSeq})
		if ok && !v.seen {
			v.seen, v.rev, v.savedRev = true, rev, saved
		}
		v.mu.Unlock()
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testRecoverCancellation = nil
		asm.mu.Unlock()
	})
	return v
}

// requireVerdictRetired is the 12.3 assertion over the recovering object: before recovery the
// verdict revisions were credited and equal; the restoration retired the nonce in memory and MOVED
// the revision; the revision-keyed persistence (persistVoidedRegs) then wrote, and the record read
// back from the stream omits the nonce.
func requireVerdictRetired(t *testing.T, label string, v *testF088VerdictRevisions, sess *mqttSession,
	asm *mqttAccountSessionManager, cid, nonce string) {
	t.Helper()
	v.mu.Lock()
	seen, before, beforeSaved, met := v.seen, v.rev, v.savedRev, append([]testF088Cancel(nil), v.met...)
	v.mu.Unlock()
	if !seen {
		t.Fatalf("%s: recovery's classification never met the verdict-covered register (met %+v)", label, met)
	}
	if before != beforeSaved {
		t.Fatalf("%s: before recovery the verdict revisions must be credited and equal, got rev %d saved %d", label, before, beforeSaved)
	}
	sess.mu.Lock()
	_, held := sess.cancelledRegNonces[nonce]
	after := sess.voidedRegsRev
	sess.mu.Unlock()
	if held {
		t.Errorf("%s: the restoration must retire the superseded verdict for %s in memory", label, nonce)
	}
	if after <= before {
		t.Errorf("%s: the retirement must move the verdict revision (before %d, after %d), or no revision-keyed save records it",
			label, before, after)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !sess.persistVoidedRegs(nil) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if testF088RecordCarriesVerdict(t, asm, cid, nonce) {
		t.Errorf("%s: the persisted record must omit the retired verdict %s", label, nonce)
	}
	t.Logf("%s: verdict retired (revision %d -> %d), record omits it; classification met %+v", label, before, after, met)
}

// testF088AwaitPublish reads r until a PUBLISH of pi arrives or the deadline passes, returning it and
// every packet read.
func testF088AwaitPublish(t *testing.T, r *mqttReader, pi uint16, deadline time.Time) (*testF088Packet, []testF088Packet) {
	t.Helper()
	var pkts []testF088Packet
	for time.Now().Before(deadline) {
		p, ok, eof := testF088Read(t, r, time.Until(deadline))
		if eof {
			t.Fatalf("fixture: the connection ended while awaiting the PUBLISH of %d; packets %v", pi, pkts)
		}
		if !ok {
			break
		}
		pkts = append(pkts, p)
		if p.typ == mqttPacketPub && p.pi == pi {
			return &pkts[len(pkts)-1], pkts
		}
	}
	return nil, pkts
}

// W-PS2C. A PERSISTED verdict over a possibly-attempted register with its marker ABSENT is
// superseded by the restoration (F-088 fix II), and the exchange completes through a real
// retransmission.
//
// SEEDED PERSISTED STATE, stated as such: message 1 is really transmitted on A (the client reads it,
// A's intent deleted the marker); the verdict is then staged and saved on A's record through the
// production entry (testF088SeedPersistedVerdict). Its old producer — B withdrawing a
// proven-unsent-looking register over A's standing token — no longer exists under fix I; witness
// run 3 stays the production reach evidence. The client resumes on B: a FRESH object, so the install
// is fresh. B's recovery must restore message 1 ACTING and retire the verdict (12.3: revisions equal
// before, moved after, the record omits it), BEFORE any acknowledgement cleanup could; JetStream's
// AckWait re-offer must then reach the client as a real DUP=1 PUBLISH (before fix II the nonce
// fence refused every send: witness run 3, 2/2), and its PUBACK ends the exchange.
//
// Not exercised: abrupt process death. Mutations: M-F3 (the fresh-install retire call removed:
// retirement and the re-offer fail), M-F6 (voidedRegsRev++ suppressed: the revision move fails).
func TestF088CrossTenureVerdictOverAPossiblyAttemptedRegisterCold(t *testing.T) {
	// Not parallel: the recovery seam and the offer hook are account- and process-wide.
	const cid = "f088-ps2c"
	oA := testMQTTDefaultOptions()
	oA.MQTT.AckWait = testF088XAckWait
	sA := testMQTTRunServer(t, oA)
	t.Cleanup(func() { testMQTTShutdownServer(sA) })
	oB := testMQTTDefaultOptions()
	oB.MQTT.AckWait = testF088XAckWait
	oB.StoreDir = oA.StoreDir
	sB := testMQTTRunServer(t, oB)
	t.Cleanup(func() { sB.Shutdown() })
	probe, probeR := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-probe", cleanSess: true}, oB.MQTT.Host, oB.MQTT.Port)
	testMQTTCheckConnAck(t, probeR, mqttConnAckRCConnectionAccepted, false)
	asmB := testMQTTGetAccountSessionManager(t, sB, cid+"-probe")
	probe.Close()

	cA, rA := testInflightSubscriber(t, oA, cid, 1)
	t.Cleanup(func() { cA.Close() })
	asmA, sessA := testResidentSession(t, sA, cid)
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, oA.MQTT.Host, oA.MQTT.Port)
	t.Cleanup(func() { pub.Close() })
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("one"))
	first, ok, _ := testF088Read(t, rA, 10*time.Second)
	if !ok || first.typ != mqttPacketPub || first.payload != "one" {
		t.Fatalf("fixture: the client must receive message 1 on A, read %v (ok %v)", first, ok)
	}
	pi := first.pi
	sessA.mu.Lock()
	gen, idHash := sessA.gen, sessA.idHash
	sessA.mu.Unlock()
	w := &testF088XWorld{cid: cid, idHash: idHash, gen: gen, epoch: asmA.sessStreamEpoch, asmA: asmA, asmB: asmB,
		sA: sA, sB: sB, oA: oA, oB: oB, pi: pi}
	w.regSubj = mqttInflightSubject(idHash, gen, pi)
	w.markSubj = w.regSubj + "." + mqttInflightMarkerSuffix
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		reg, _, _ := asmA.jsa.loadInflightRecord(w.regSubj)
		m, _ := asmA.jsa.loadLastMsgFor(mqttInflightStreamName, w.markSubj)
		if reg == nil || reg.ActionToken != _EMPTY_ || m != nil {
			return fmt.Errorf("fixture: message 1 must be transmitted on A (marker gone, token drained): %+v marker %v", reg, m != nil)
		}
		w.nonce, w.lin, w.srcDur, w.srcSeq = reg.Nonce, reg.Lin, reg.Dur, reg.Seq
		return nil
	})
	testF088SeedPersistedVerdict(t, sessA, w.nonce)
	cA.Close()

	// The cold resume on B, a fresh object, with the verdict revisions observed before the restore.
	revs := testF088ObserveVerdictRevisions(t, asmB, cid)
	var offers atomic.Int64
	restoreOffers := setMQTTTestDeliveryOfferedHook(func(sub *subscription, reply string) {
		if sub != nil && sub.client != nil && sub.client.srv == sB && mqttSourceAckReplyBinds(reply, mqttStreamName, w.srcDur, w.srcSeq) {
			offers.Add(1)
		}
	})
	t.Cleanup(restoreOffers)
	cB, rB := testInflightReconnect(t, oB, cid)
	t.Cleanup(func() { cB.Close() })
	_, sessB := testResidentSession(t, sB, cid)
	w.sessB = sessB
	sessB.mu.Lock()
	ent := sessB.pendingPublish[pi]
	acting := ent != nil && ent.regNonce == w.nonce && ent.acts() && ent.transmitted
	sessB.mu.Unlock()
	if !acting {
		t.Fatalf("CONTRACT: B must restore message 1 ACTING and possibly attempted; state %s", w.state(sessB, asmB))
	}
	requireVerdictRetired(t, "W-PS2C", revs, sessB, asmB, cid, w.nonce)

	got, pkts := testF088AwaitPublish(t, rB, pi, time.Now().Add(testF088XColdRead))
	if got == nil {
		if offers.Load() > 0 {
			t.Fatalf("CONTRACT: message 1's re-offer reached B's delivery callback %d time(s) and no PUBLISH followed; "+
				"packets %v; state %s", offers.Load(), pkts, w.state(sessB, asmB))
		}
		t.Fatalf("fixture timeout: no re-offer of message 1 within %v; packets %v; state %s", testF088XColdRead, pkts, w.state(sessB, asmB))
	}
	if got.payload != "one" || got.flags&mqttPubFlagDup == 0 {
		t.Errorf("CONTRACT: the retransmission must carry DUP=1 and message 1's payload, got %v", *got)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, cB, pi)
	w.awaitReleased(t, sessB, asmB, "CONTRACT (C1 closure, PUBACK answering the observed retransmission)")
	t.Logf("W-PS2C: retransmission %v (offers %d), completed", *got, offers.Load())
}

// W-PS2T. A PERSISTED verdict + a STANDING marker + a recovered output token (P8-1): the
// token-derived possible attempt supersedes the verdict too, the marker is kept as deletion debt,
// and after the holder drains WITHOUT intent B's own retransmission confirms that delete before it
// writes.
//
// B-first with A's eviction held so B reads A's token; the verdict is SEEDED PERSISTED STATE on A's
// record before the resume (testF088SeedPersistedVerdict). B's recovery must keep message 1
// possibly attempted with the marker as debt and retire the verdict (12.3). A's eviction is then
// released and its parked section ends WITHOUT intent (the Reserved hook returns false): the token
// drains, the marker still stands. JetStream's AckWait re-offer reaches B: its section must reach
// INTENT (the marker's confirmed delete) before WROTE, the client reads DUP=1, and the PUBACK ends
// the exchange. The harness's "B never opens this PI" check is not applied: B's retransmission is
// the point.
//
// Not exercised: abrupt process death. Mutations: M-F3/M-F3b-adjacent retirement removal (the
// verdict fence refuses B's send), M-F4 (marker debt erased: no INTENT before WROTE).
func TestF088RecoveredTokenWithPersistedVerdictRetransmitsAfterNoIntentDrain(t *testing.T) {
	// Not parallel: the send-window, writer, eviction and recovery hooks are process- or account-wide.
	var revs *testF088VerdictRevisions
	w := testF088XHarnessWith(t, "f088-ps2t", testF088BFirstEvictHeld, func(w *testF088XWorld) {
		testF088SeedPersistedVerdict(t, w.sessA, w.nonce)
		revs = testF088ObserveVerdictRevisions(t, w.asmB, w.cid)
	})
	requireVerdictRetired(t, "W-PS2T", revs, w.sessB, w.asmB, w.cid, w.nonce)
	testF088DrainWithoutIntentThenRetransmit(t, w, "W-PS2T")
}

// bSectionOrder reports where B's first INTENT and first WROTE window for message 1's identifier
// fall among every window B reached. WROTE fires after the enqueue returns, so the client can read
// the PUBLISH before it is recorded: the read waits (up to 5 s) for it rather than judge a
// half-recorded section.
func (w *testF088XWorld) bSectionOrder() (intentAt, wroteAt int, opened []string) {
	wantIntent := fmt.Sprintf("%d/%s", w.pi, mqttMarkerSendIntent)
	wantWrote := fmt.Sprintf("%d/%s", w.pi, mqttMarkerSendWrote)
	for end := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		opened, _ = w.sec.openedB()
		intentAt, wroteAt = -1, -1
		for i, o := range opened {
			if o == wantIntent && intentAt < 0 {
				intentAt = i
			}
			if o == wantWrote && wroteAt < 0 {
				wroteAt = i
			}
		}
		if wroteAt >= 0 || time.Now().After(end) {
			return intentAt, wroteAt, opened
		}
	}
}

// testF088DrainWithoutIntentThenRetransmit releases A's eviction, ends A's parked section WITHOUT
// intent, and requires B's own retransmission to confirm the retained marker's delete before its
// write, carry DUP=1, and complete on its PUBACK.
func testF088DrainWithoutIntentThenRetransmit(t *testing.T, w *testF088XWorld, label string) {
	t.Helper()
	w.releaseEvict()
	w.requireANotResident(t)
	w.sec.abortReserved.Store(true)
	obs := w.observeA(t)
	if obs.intent || obs.markerGone || obs.oldPkt != nil || obs.reg == nil || obs.reg.ActionToken != _EMPTY_ {
		t.Fatalf("fixture (%s): A's section must drain its token WITHOUT intent, the marker standing: %v", label, obs)
	}
	got, pkts := testF088AwaitPublish(t, w.rB, w.pi, time.Now().Add(testF088XColdRead))
	if got == nil {
		t.Fatalf("%s: B never retransmitted message 1 within %v; packets %v; state %s", label, testF088XColdRead, pkts,
			w.state(w.sessB, w.asmB))
	}
	intentAt, wroteAt, opened := w.bSectionOrder()
	if intentAt < 0 || wroteAt < 0 || intentAt > wroteAt {
		t.Errorf("%s: B's retransmission must confirm the retained marker's delete (INTENT) before its write (WROTE): %v",
			label, opened)
	}
	if m, _ := w.asmB.jsa.loadLastMsgFor(mqttInflightStreamName, w.markSubj); m != nil {
		t.Errorf("%s: the marker must be gone after B's retransmission", label)
	}
	if got.payload != "one" || got.flags&mqttPubFlagDup == 0 {
		t.Errorf("%s: B's retransmission must carry DUP=1 and message 1's payload, got %v", label, *got)
	}
	w.pkts = append(w.pkts, pkts...)
	testMQTTSendPIPacket(mqttPacketPubAck, t, w.cB, w.pi)
	w.awaitReleased(t, w.sessB, w.asmB, "CONTRACT (C1 closure, PUBACK answering B's retransmission)")
}

// W-MKDEBT. A recovered output token whose holder then drains WITHOUT intent leaves the marker
// standing over a register B classified possibly attempted: the marker is DELETION DEBT, and B's
// later transmission must confirm its delete before writing — or a real PUBLISH would stand beside
// an unsent marker and a later recovery would read it never sent (C-W3's counterexample).
//
// B-first with A's eviction held (so B reads A's token), A parked at RESERVED after acquisition. B's
// recovery must classify transmitted with the marker retained. A's section then ends without
// intent (the Reserved hook returns false): token gone, marker present. B's AckWait re-offer must
// reach INTENT before WROTE and the client reads DUP=1. A SECOND recovery before the client
// acknowledges — a cold resume on A, a fresh object — must then classify message 1 transmitted with
// no marker. The harness's "B never opens this PI" check is not applied: B's retransmission is the
// point.
//
// Not exercised: abrupt process death. Mutation: M-F4 (Site 1 sets markerSeq=0: no INTENT before
// WROTE, the marker survives, the second recovery reads it unsent).
func TestF088RecoveredTokenNoIntentDrainKeepsMarkerDebt(t *testing.T) {
	// Not parallel: the send-window, writer and eviction hooks are process- or account-wide.
	w := testF088XHarness(t, "f088-mkdebt", testF088BFirstEvictHeld)
	w.releaseEvict()
	w.requireANotResident(t)
	w.sec.abortReserved.Store(true)
	obs := w.observeA(t)
	if obs.intent || obs.markerGone || obs.oldPkt != nil || obs.reg == nil || obs.reg.ActionToken != _EMPTY_ {
		t.Fatalf("fixture: A's section must drain its token WITHOUT intent, the marker standing: %v", obs)
	}
	got, pkts := testF088AwaitPublish(t, w.rB, w.pi, time.Now().Add(testF088XColdRead))
	if got == nil {
		t.Fatalf("CONTRACT: B never retransmitted message 1 within %v; packets %v; state %s", testF088XColdRead, pkts,
			w.state(w.sessB, w.asmB))
	}
	intentAt, wroteAt, opened := w.bSectionOrder()
	if intentAt < 0 || wroteAt < 0 || intentAt > wroteAt {
		t.Errorf("CONTRACT: B's transmission must confirm the retained marker's delete (INTENT) before its write (WROTE): %v", opened)
	}
	if got.payload != "one" || got.flags&mqttPubFlagDup == 0 {
		t.Errorf("CONTRACT: B's transmission must carry DUP=1 and message 1's payload, got %v", *got)
	}
	// The second recovery, before any acknowledgement: a cold resume on A (a fresh object).
	sess3, _, _, c3, _ := w.coldOnA(t)
	sess3.mu.Lock()
	e3 := sess3.pendingPublish[w.pi]
	ok := e3 != nil && e3.regNonce == w.nonce && e3.transmitted && e3.markerSeq == 0
	sess3.mu.Unlock()
	if !ok {
		t.Errorf("CONTRACT: the second recovery must classify message 1 transmitted with no marker; state %s", w.state(sess3, w.asmA))
	}
	if m, _ := w.asmA.jsa.loadLastMsgFor(mqttInflightStreamName, w.markSubj); m != nil {
		t.Errorf("CONTRACT: a real transmission must not leave the unsent marker standing")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c3, w.pi)
	w.awaitReleased(t, sess3, w.asmA, "CONTRACT (C1 closure, PUBACK after the second recovery)")
}

// F-121, PINNED DEFECTIVE BEHAVIOUR, in BOTH race orderings. This test characterizes a defect; it
// does not state the intended contract. F-121's fix must INVERT these assertions deliberately, with
// an ordering fix and assertions of the corrected behaviour — never by loosening them.
//
// The schedule: the takeover displaces A, whose certification SEALS A's gate. A's parked section,
// released, then DELETES THE UNSENT MARKER (a confirmed intent) after its binding was certified,
// and its write is prevented by the sealed gate: no PUBLISH left. That is F-121, in both orderings.
//
// What it does to B differs by ordering:
//   - A-FIRST (the remaining stranding): A's certificate cleared the token before B's read, so B
//     derives a proven-unsent entry that A's late intent then makes stale. B's UNSUBSCRIBE withdraws
//     it and lands the standalone occupant tombstone; the exchange stays at Publish, B holds no entry
//     and no AckRoute, and the ledger keeps holding and charging the identifier: a markerless
//     reservation no legitimate acknowledgement will ever release.
//   - B-FIRST (repaired by F-088's fix I): B read A's token and kept the delivery possibly attempted,
//     so the UNSUBSCRIBE enrols it and nothing is stranded; before the fix this ordering stranded
//     exactly as A-first does (witness runs 2-3).
//
// Per-ordering premises, CHECKED (a different ordering is a fixture failure):
//   - A-first: A's certificate persisted and B's resume cleared the token (actionLive=false).
//   - B-first: A's certificate persist held, A's token live at B's recovery (actionLive=true).
//   - Both: A's EXACT binding holds an in-memory certificate while A is still PARKED at RESERVED,
//     before release — the gate was sealed before A's intent could run.
//
// Witness: the binding's certificate and the marker standing before release, then the INTENT window
// and the marker's absence; A's gate and the old connection's reader; B's landed tombstone-register
// transition; B's exchange, entry, ack bindings and ledger. The source consumer's
// NumAckPending/AckFloor are RECORDED as read, not inferred. Input: production-reached, two real
// Servers on one JetStream. Not exercised: abrupt process death; a transmission that actually
// reached the client (that removal losing completion is F-088's contract, witnessed by
// TestF088CrossTenureLegitimateTransmissionThenCancellation).
func TestF121IntentDeleteAfterCertificationStrandsAWithdrawnExchange(t *testing.T) {
	// Not parallel: the send-window, writer and certificate hooks are process-wide.
	t.Run("A-first", func(t *testing.T) { testF121Run(t, "f121-a-first", testF088AFirst) })
	t.Run("B-first", func(t *testing.T) { testF121Run(t, "f121-b-first", testF088BFirstCertHeld) })
}

func testF121Run(t *testing.T, cid string, order testF088Order) {
	w := testF088XHarness(t, cid, order)
	if smsg, _ := w.asmA.jsa.loadLastMsgFor(mqttInflightStreamName, w.markSubj); smsg == nil {
		t.Fatalf("fixture: the marker must still stand before A is released (A parked at RESERVED)")
	}
	w.requireCertifiedWhileParked(t)
	w.releaseCert()
	obs := w.observeA(t)
	if !obs.intent || !obs.markerGone {
		t.Errorf("F-121: A's released section must confirm its marker delete AFTER its certification: %v", obs)
	}
	if obs.gateOpen {
		t.Errorf("F-121: A's output gate must be sealed by the displacement: %v", obs)
	}
	if obs.oldPkt != nil {
		t.Errorf("F-121: no PUBLISH may reach A's old connection through a sealed gate, read %v", *obs.oldPkt)
	}
	attempts := testF088RecordAttempts(t, &w.asmB.jsa, w.regSubj)
	testF088Unsub(t, w.cB, w.rB, 7, &w.pkts)
	w.checkWindow(t, "B's UNSUBSCRIBE")
	if order.bFirst() {
		// B-FIRST, REPAIRED by F-088's fix I: B's recovery read A's outstanding token and kept the
		// delivery POSSIBLY ATTEMPTED with the marker as debt, so the UNSUBSCRIBE enrols it — no
		// withdrawal, no tombstone — and the exchange stays owed at Publish on B's acting entry. The
		// post-certification intent asserted above is F-121's and is unchanged; it no longer
		// strands anything here. The conservative cost: nothing was written, yet the delivery is
		// retained and will be offered again with DUP=1 (CONFORMANCE).
		w.requireEnrolledNoTombstone(t, "F-121 (B-first, repaired)", attempts)
		if x, _, _ := w.asmB.jsa.loadOutboundExchange(w.cid, w.epoch, w.gen, w.lin); x == nil || x.Phase != mqttExchangePublish {
			t.Errorf("F-121 (B-first, repaired): the preserved exchange must stand at Publish, got %+v", x)
		}
		t.Logf("F-121 (%v) state on B: %s", order, w.state(w.sessB, w.asmB))
		return
	}
	w.requireTombstoneLanded(t, "F-121", attempts)
	// The stranded reservation, polled until B's removal has settled.
	var why string
	deadline := time.Now().Add(10 * time.Second)
	for {
		why = _EMPTY_
		x, _, _ := w.asmB.jsa.loadOutboundExchange(w.cid, w.epoch, w.gen, w.lin)
		if x == nil || x.Phase != mqttExchangePublish {
			why += "the exchange is not at Publish; "
		}
		w.sessB.mu.Lock()
		if a := w.sessB.pendingPublish[w.pi]; a != nil && a.regNonce == w.nonce {
			why += "B still holds an entry for message 1; "
		}
		for _, ab := range w.sessB.abandonedAcks {
			if ab != nil && ab.name.pi == w.pi {
				why += fmt.Sprintf("an ack binding stands on %d: %+v; ", w.pi, *ab)
			}
		}
		if !w.sessB.custody.holdsPI(w.pi) || !w.sessB.custody.chargesPI(w.pi) {
			why += "the ledger no longer holds and charges the identifier; "
		}
		w.sessB.mu.Unlock()
		if why == _EMPTY_ || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if why != _EMPTY_ {
		t.Errorf("F-121: the stranded reservation was not observed: %s state %s", why, w.state(w.sessB, w.asmB))
	}
	ci, cerr := w.asmB.jsa.lookupConsumer(mqttStreamName, w.srcDur)
	if ci != nil {
		t.Logf("source consumer %q as observed: NumAckPending %d AckFloor.Stream %d (message 1 at stream seq %d) NumRedelivered %d",
			w.srcDur, ci.NumAckPending, ci.AckFloor.Stream, w.srcSeq, ci.NumRedelivered)
	} else {
		t.Logf("source consumer %q as observed: does not read back (%v)", w.srcDur, cerr)
	}
	t.Logf("F-121 (%v) state on B: %s", order, w.state(w.sessB, w.asmB))
}

// W-LEGIT. A LEGITIMATE transmission followed by a cross-tenure cancellation: F-088's contract on
// the window C-W2 named.
//
// B-first with A's EVICTION held (testEvictPauseBeforeLoad, for this client only): A never
// certifies its connection, so its gate stays open while the client resumes on B under a new
// tenure. B's recovery reads A's outstanding output token and derives the entry POSSIBLY ATTEMPTED
// with the marker kept as deletion debt (F-088 fix I; before it, B derived proven-unsent and the
// delivery was lost — witness runs 3, 4/4). A's released section records intent and its writer
// WRITES message 1 to the old connection — the client's old reader READS it, which is decisive.
// B's UNSUBSCRIBE (a real drain, message 2 started) then ENROLS message 1 in the drain as a started
// delivery: no withdrawal, no tombstone. The client's PUBACK on B answers the PUBLISH it received —
// a LEGITIMATE acknowledgement — and C1's closure statement requires the exchange to reach
// Terminal with its identifier, charge and send reservation released, without another offer or
// resume.
//
// A's eviction is released after A's write and token drain are observed and before the PUBACK
// (plan rev 11, 13.1), so F-122's displaced-subscription churn cannot starve B's advances.
//
// Fixture validity, checked before any contract outcome is classified: the eviction hold was
// reached and A's gate was open with no certificate before A's release; the cancellation window's
// elapsed bound holds; B opened no section for message 1 (re-asserted through completion). The gate
// claim is scoped to this schedule: the seam does not rule out unrelated transport failures or an
// A-side rejection. A failure of the contract is F-088's contract failure, recorded as such.
//
// Witness: B's recovered classification (premise), the old connection's reader (wire receipt), A's
// token drain, the drain's claim of message 1 with no tombstone at the commit seam, the exchange
// phase and B's ledger after the PUBACK. Input: production-reached on two real Servers sharing one
// JetStream, real clients and packets; A's delivery held at RESERVED and its eviction worker held
// before its record load. Not exercised: abrupt process death; the production timing of A's
// eviction against B's resume.
//
// Mutations: X6 (recovery ignores ActionToken: the premise fails), X4a only if this route is the
// AckRoute's (it is not: the drain enrols the entry, so completion is the entry's own).
func TestF088CrossTenureLegitimateTransmissionThenCancellation(t *testing.T) {
	// Not parallel: the send-window, writer and eviction hooks are process-wide or account-wide.
	w := testF088XHarness(t, "f088-legit", testF088BFirstEvictHeld)

	// (2) FIXTURE VALIDITY at the release: the hold reached, A's gate open, no certificate.
	b := w.ccA.outBind
	if c := b.closeCert(); c != nil || !b.mayWrite() || !w.evictReached.Load() {
		t.Fatalf("fixture: before A's release the eviction hold must be reached (%v) and A's gate open (%v) with no "+
			"certificate (%+v)", w.evictReached.Load(), b.mayWrite(), c)
	}
	obs := w.observeA(t)
	if !obs.wireReceipt(w.pi) || obs.oldPkt.payload != "one" {
		t.Fatalf("REACH (W-LEGIT): the client's old reader must READ message 1's PUBLISH (pi %d, payload \"one\"): %v",
			w.pi, obs)
	}
	if !obs.intent || obs.reg == nil || obs.reg.Nonce != w.nonce || obs.reg.ActionToken != _EMPTY_ {
		t.Fatalf("REACH (W-LEGIT): A must record intent and drain its own token: %v", obs)
	}
	t.Logf("legitimate transmission observed: %v (dup=%v)", *obs.oldPkt, obs.oldPkt.flags&mqttPubFlagDup != 0)
	// A's eviction is released once A's write and token drain are observed and BEFORE the PUBACK
	// (plan rev 11, 13.1): a held eviction keeps the displaced resident subscription re-offering
	// and refusing admissions (F-122), and the authority writes of that churn starve B's
	// acknowledgement advances — the fixture would cause the outcome (1 failure in 6 runs).
	w.releaseEvict()
	w.requireANotResident(t)

	// (3) B's UNSUBSCRIBE: message 1 ENROLLED in the drain as a started delivery, no tombstone.
	attempts := testF088RecordAttempts(t, &w.asmB.jsa, w.regSubj)
	testF088Unsub(t, w.cB, w.rB, 7, &w.pkts)
	w.requireEnrolledNoTombstone(t, "W-LEGIT", attempts)
	w.checkWindow(t, "B's UNSUBSCRIBE")

	// (4) The client's PUBACK on B, answering the PUBLISH it received.
	testMQTTSendPIPacket(mqttPacketPubAck, t, w.cB, w.pi)

	// (5) CONTRACT, classified only after the fixture is re-checked through completion.
	var rerr error
	deadline := time.Now().Add(15 * time.Second)
	for {
		if rerr = w.released(w.sessB, w.asmB); rerr == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if opened, onPI := w.sec.openedB(); onPI {
		t.Fatalf("fixture: B opened a section for message 1's identifier %d before the outcome: %v", w.pi, opened)
	}
	if rerr != nil {
		t.Errorf("CONTRACT (C1 closure, legitimate PUBACK on B for the PUBLISH the client received): %v after 15s "+
			"without another offer or resume; attempts %v; state %s; packets on B %v",
			rerr, attempts(), w.state(w.sessB, w.asmB), w.pkts)
	} else {
		t.Logf("CONTRACT held: the legitimate PUBACK on B ended the exchange and released the identifier, charge and reservation")
	}
	for _, at := range attempts() {
		if at.kind == mqttTransitionTombstoneRegister {
			t.Errorf("W-LEGIT: a tombstone reached the commit seam over the completed delivery: %v", at)
		}
	}

	w.checkNoConflict(t, w.asmB)
}

// W-ACKEARLY. The client's acknowledgement of a legitimately written PUBLISH arrives on B BEFORE the
// writing holder's durable token has drained: B (which recovered the delivery possibly attempted
// over that token) must still complete the exchange once the holder's section ends.
//
// B-first with A's eviction held, as W-LEGIT. A's section is parked at WROTE — after its enqueue,
// with its token still on the register — and the client's old reader has READ message 1. A's
// eviction is then released while the token still stands (plan rev 11, 13.1; a held eviction's
// F-122 churn starved every advance for 15 s). The PUBACK is sent on B; B PROCESSING it is observed
// as the registration of its acknowledged terminal retry, with the token still standing; only then
// is A's write barrier released.
//
// Not exercised: abrupt process death; a wire-driven delay of the PUBACK. CONTRACT: Terminal, with
// the identifier, charge and send reservation released, without another offer or resume.
func TestF088CrossTenureAckBeforeTheHoldersDrainCompletes(t *testing.T) {
	// Not parallel: the send-window, writer and eviction hooks are process- or account-wide.
	w := testF088XHarness(t, "f088-ackearly", testF088BFirstEvictHeld)
	b := w.ccA.outBind
	if c := b.closeCert(); c != nil || !b.mayWrite() || !w.evictReached.Load() {
		t.Fatalf("fixture: before A's release the eviction hold must be reached (%v) and A's gate open (%v) with no "+
			"certificate (%+v)", w.evictReached.Load(), b.mayWrite(), c)
	}
	w.sec.wroteArmed.Store(true)
	w.sec.releaseReserved()
	select {
	case <-w.sec.wroteReached:
	case <-time.After(10 * time.Second):
		t.Fatalf("fixture: A's section never reached WROTE; windows %v", w.sec.windowsA())
	}
	p, ok, _ := testF088Read(t, w.rA, 5*time.Second)
	if !ok || p.typ != mqttPacketPub || p.pi != w.pi || p.payload != "one" {
		t.Fatalf("REACH: the client's old reader must READ message 1's PUBLISH before the PUBACK, read %v (ok %v)", p, ok)
	}
	// A's eviction is released now, with the section still parked after its write: a displaced
	// resident subscription otherwise keeps re-offering and refusing admissions (F-122), and the
	// authority writes that churn produces out-race B's acknowledgement advance (observed: every
	// attempt lost its authority CAS for 15 s). The token is the section's and still stands.
	w.releaseEvict()
	w.requireANotResident(t)
	if reg, _, _ := w.asmB.jsa.loadInflightRecord(w.regSubj); reg == nil || reg.ActionToken != w.tokenA {
		t.Fatalf("REACH: A's durable token must still stand when the PUBACK is sent, register %+v", reg)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, w.cB, w.pi)
	// B PROCESSES the acknowledgement while A's token still stands: its handler registers the
	// ACKNOWLEDGED terminal retry for exactly this allocation, source, acquisition and store epoch
	// (armTerminalRetryLocked). Observed on the registration, not on the advance: the first
	// advance sits behind the standing-token guard. Only then is A's write barrier released.
	want := mqttTerminalRetryKey{
		source:       mqttTerminalSourceKey{subject: w.regSubj, nonce: w.nonce, dur: w.srcDur, sseq: w.srcSeq},
		acq:          w.acqA,
		acknowledged: true,
		epoch:        w.epoch,
	}
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		w.sessB.mu.Lock()
		_, registered := w.sessB.terminalRetries[want]
		var keys []mqttTerminalRetryKey
		for k := range w.sessB.terminalRetries {
			keys = append(keys, k)
		}
		w.sessB.mu.Unlock()
		if !registered {
			return fmt.Errorf("REACH: B has not registered the acknowledged terminal retry %+v; registered %+v", want, keys)
		}
		return nil
	})
	if reg, _, _ := w.asmB.jsa.loadInflightRecord(w.regSubj); reg == nil || reg.ActionToken != w.tokenA {
		t.Fatalf("REACH: B must have processed the PUBACK while A's token still stood, register %+v", reg)
	}
	w.sec.releaseWrote()
	w.awaitReleased(t, w.sessB, w.asmB, "CONTRACT (C1 closure, PUBACK on B before the holder's drain)")
	if opened, onPI := w.sec.openedB(); onPI {
		t.Errorf("CONTRACT: B must not re-offer message 1 to complete it: %v", opened)
	}
}

// W-DRN. A drain whose claimed delivery was RECOVERED (no jsAckSubject) still finishes.
//
// The W-LEGIT schedule through the enrolment (message 1 legitimately written by A and enrolled in
// B's drain, message 2 started on B); the client then acknowledges both messages.
//
//   - BASELINE: the claim retires, the exchange completes, the resources are released and the
//     drain's consumer is deleted IN-SESSION.
//   - ONE FAILED SAVE (the existing save-failure seam, mqttTestSessSaveHookV): the failure is
//     consumed and, before any resume, the claim retires, the exchange completes and the resources
//     are released. The rest of this arm is the F-124 CHARACTERIZATION OF THE CURRENT DEFECTIVE
//     CONSEQUENCE, re-expressed when F-124 lands: the retry that marks the removal runs with no
//     connection, so the consumer's disposal is not decided in-session; at the next resume the
//     F-105 7f executor decides it under the resuming entitlement and the consumer is then
//     authoritatively absent (a CLASSIFIED consumer-not-found; any other lookup error fails). This
//     arm earns no credit for in-session disposal after a failure.
//
// A's eviction is released after the enrolment and before the acknowledgements: a displaced
// resident subscription otherwise keeps re-offering and refusing admissions (F-122), and the drain
// consumer would keep a live subscriber.
func TestF088RecoveredDrainClaimRetiresDespiteAFailedSave(t *testing.T) {
	// Not parallel: the send-window, writer, eviction and save hooks are process- or account-wide.
	t.Run("baseline", func(t *testing.T) { testF088RecoveredDrainRun(t, "f088-drn-base", false) })
	t.Run("one failed save", func(t *testing.T) { testF088RecoveredDrainRun(t, "f088-drn-fail", true) })
}

func testF088RecoveredDrainRun(t *testing.T, cid string, inject bool) {
	w := testF088XHarness(t, cid, testF088BFirstEvictHeld)
	obs := w.observeA(t)
	if !obs.wireReceipt(w.pi) {
		t.Fatalf("REACH: the client's old reader must READ message 1's PUBLISH: %v", obs)
	}
	attempts := testF088RecordAttempts(t, &w.asmB.jsa, w.regSubj)
	testF088Unsub(t, w.cB, w.rB, 7, &w.pkts)
	w.requireEnrolledNoTombstone(t, "W-DRN", attempts)
	w.sessB.mu.Lock()
	ent := w.sessB.pendingPublish[w.pi]
	noAckSubject := ent != nil && ent.jsAckSubject == _EMPTY_
	w.sessB.mu.Unlock()
	if !noAckSubject {
		t.Fatalf("fixture: message 1's recovered entry must carry no jsAckSubject, got %+v", ent)
	}
	w.releaseEvict()
	w.requireANotResident(t)
	var failed atomic.Int32
	if inject {
		restore := setMQTTTestSessSaveHook(func(s *mqttSession) error {
			if s == w.sessB && failed.CompareAndSwap(0, 1) {
				return errors.New("test: one injected save failure")
			}
			return nil
		})
		t.Cleanup(restore)
	} else {
		failed.Store(1) // nothing to consume on the baseline arm
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, w.cB, w.pi)
	testMQTTSendPIPacket(mqttPacketPubAck, t, w.cB, w.pi2)
	// (1) The claim retires, the exchange completes and the resources are released — message 2's
	// as well as message 1's, both released asynchronously by their terminal cleanup — the injected
	// failure consumed.
	deadline := time.Now().Add(15 * time.Second)
	for {
		w.sessB.mu.Lock()
		d := w.sessB.drainForDurableLocked(w.srcDur)
		m2 := w.sessB.pendingPublish[w.pi2]
		m2Held := w.sessB.custody.holdsPI(w.pi2) || w.sessB.custody.chargesPI(w.pi2) || w.sessB.sendReservedLocked(w.pi2)
		w.sessB.mu.Unlock()
		rerr := w.released(w.sessB, w.asmB)
		if d == nil && rerr == nil && m2 == nil && !m2Held && failed.Load() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("CONTRACT: the drain's claim must retire, the exchange complete and the resources be released despite "+
				"one failed save (drain %v, released %v, message 2 entry %+v held %v, injected %d); state %s",
				d != nil, rerr, m2, m2Held, failed.Load(), w.state(w.sessB, w.asmB))
		}
		time.Sleep(50 * time.Millisecond)
	}
	// (2) The drain's consumer. Its resource is read through its name ownership, the inc captured
	// now so it stays addressable whatever the deletion does to the ownership.
	own, _, oerr := w.asmB.jsa.loadNameOwnershipPhysical(w.epoch, mqttOwnedStreamMsgs, w.srcDur)
	if oerr != nil || own == nil {
		t.Fatalf("fixture: the drain consumer %q must have a name ownership: %+v (%v)", w.srcDur, own, oerr)
	}
	inc := own.OwnerInc
	// consumerGone reports a CLASSIFIED consumer-not-found; any other lookup error fails the test.
	consumerGone := func(within time.Duration) bool {
		for end := time.Now().Add(within); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
			_, cerr := w.asmB.jsa.lookupConsumer(mqttStreamName, w.srcDur)
			if cerr == nil {
				continue
			}
			if isErrorOtherThan(cerr, JSConsumerNotFoundErr) {
				t.Fatalf("the drain consumer %q lookup failed with an unclassified error: %v", w.srcDur, cerr)
			}
			return true
		}
		return false
	}
	if !inject {
		// BASELINE: disposed of IN-SESSION.
		if !consumerGone(10 * time.Second) {
			r, _, _ := w.asmB.jsa.loadOutboundResource(w.cid, w.epoch, w.gen, inc)
			t.Fatalf("CONTRACT: the retired drain's consumer %q must be deleted in-session; resource %+v; state %s",
				w.srcDur, r, w.state(w.sessB, w.asmB))
		}
		t.Logf("W-DRN baseline: drain retired, consumer %q deleted in-session", w.srcDur)
		return
	}
	// INJECTED ARM — F-124 CHARACTERIZATION of the current defective consequence: the retry that
	// marked the drain's removal ran with no connection, so its disposal decision carries no
	// entitlement and is not recorded; the consumer is left for recovery. These assertions pin that
	// consequence and are re-expressed when F-124 lands.
	if consumerGone(10 * time.Second) {
		t.Fatalf("F-124 characterization: the consumer %q was deleted in-session; F-124's consequence no longer "+
			"holds — re-express this arm", w.srcDur)
	}
	if r, _, _ := w.asmB.jsa.loadOutboundResource(w.cid, w.epoch, w.gen, inc); r == nil || r.Disposal != nil {
		t.Fatalf("F-124 characterization: before the resume the drain consumer's resource must carry NO disposal "+
			"decision, got %+v", r)
	}
	// The next resume: the F-105 7f executor decides the consumer under the resuming tenure, and the
	// consumer is then authoritatively absent.
	w.cB.Close()
	cB2, _ := testInflightReconnect(t, w.oB, w.cid)
	t.Cleanup(func() { cB2.Close() })
	_, sessR := testResidentSession(t, w.sB, w.cid)
	sessR.mu.Lock()
	entR := sessR.boundEnt
	sessR.mu.Unlock()
	var r *mqttOutboundResource
	deadline = time.Now().Add(15 * time.Second)
	for {
		r, _, _ = w.asmB.jsa.loadOutboundResource(w.cid, w.epoch, w.gen, inc)
		if r != nil && r.Disposal != nil &&
			(r.State == mqttResourceDeleteAuthorized || r.State == mqttResourceCleanupComplete) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the next resume's F-105 7f executor must decide the drain consumer's disposal and authorize its "+
				"deletion, resource %+v", r)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if r.Disposal.Gen != w.gen || r.Disposal.Tenure != entR.Tenure {
		t.Errorf("the 7f decision must be taken under the resuming entitlement %+v, got %+v", entR, *r.Disposal)
	}
	if !consumerGone(15 * time.Second) {
		t.Errorf("the drain consumer %q must be authoritatively absent after the resume's decision; resource %+v", w.srcDur, r)
	}
	t.Logf("W-DRN injected (F-124 characterization): resume decision %+v, resource state %v; consumer %q not found",
		*r.Disposal, r.State, w.srcDur)
}

// W-RESID. A RESIDENT same-object reconnect over this object's OWN live section, which holds the
// output token with the marker standing: recovery leaves the resident in place (in-memory wins) and
// must still reclassify it possibly attempted — monotone, the marker debt kept — and retire a
// verdict naming its allocation (Site 4 (ii) and (iii)). The section then settles: normally (intent,
// then its write — prevented, since the reconnect sealed the displaced connection's gate) or by
// aborting without intent.
//
// The displaced connection's closure-certificate persist is HELD through the reconnect: a persisted
// certificate lets the resume clear the token before its walk (W6a observed exactly that), which
// would hide the token this witness is about. The verdict is SEEDED PERSISTED STATE on the session's
// own record (testF088SeedPersistedVerdict).
//
// Mutations: M-F7 (Site 4 (ii) removed: the resident stays unsent-classified), M-F3b (the resident
// retire call removed: the verdict survives).
func TestF088ResidentRecoveryReclassifiesTheLiveSection(t *testing.T) {
	// Not parallel: the send-window and certificate hooks are process-wide.
	t.Run("intent", func(t *testing.T) { testF088ResidentRun(t, "f088-resid-intent", false) })
	t.Run("abort", func(t *testing.T) { testF088ResidentRun(t, "f088-resid-abort", true) })
}

func testF088ResidentRun(t *testing.T, cid string, abort bool) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = testF088XAckWait
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	cOld, _ := testInflightSubscriber(t, o, cid, 1)
	t.Cleanup(func() { cOld.Close() })
	asm, sess := testResidentSession(t, s, cid)
	sess.mu.Lock()
	ccOld, gen, idHash := sess.c, sess.gen, sess.idHash
	sess.mu.Unlock()
	sec := testF088InstallSections(t, asm, nil)
	certHold := make(chan struct{})
	var holdOnce sync.Once
	releaseCert := func() { holdOnce.Do(func() { close(certHold) }) }
	run, conn := ccOld.outBind.run, ccOld.outBind.conn
	hold := func(c *mqttDurableCloseCert) bool {
		if c.Run == run && c.Conn == conn {
			<-certHold
		}
		return true
	}
	prev := mqttTestCloseCertPersistV.Swap(&hold)
	t.Cleanup(func() {
		releaseCert()
		mqttTestCloseCertPersistV.Store(prev)
	})

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { pub.Close() })
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("one"))
	var pi uint16
	select {
	case pi = <-sec.parked:
	case <-time.After(10 * time.Second):
		t.Fatal("fixture: the send section never opened")
	}
	regSubj := mqttInflightSubject(idHash, gen, pi)
	markSubj := regSubj + "." + mqttInflightMarkerSuffix
	reg, _, err := asm.jsa.loadInflightRecord(regSubj)
	if err != nil || reg == nil || reg.ActionToken == _EMPTY_ {
		t.Fatalf("fixture: the parked section must hold the ACQUIRED action token: %+v (%v)", reg, err)
	}
	nonce := reg.Nonce
	testF088SeedPersistedVerdict(t, sess, nonce)
	sess.mu.Lock()
	resident := sess.pendingPublish[pi]
	sess.mu.Unlock()
	if resident == nil || resident.transmitted || resident.markerSeq == 0 || resident.sendNonce == _EMPTY_ {
		t.Fatalf("fixture: the resident must be this object's unsent-classified entry with its section open, got %+v", resident)
	}
	revs := testF088ObserveVerdictRevisions(t, asm, cid)

	cNew, _ := testInflightReconnect(t, o, cid)
	t.Cleanup(func() { cNew.Close() })
	sess.mu.Lock()
	ack := sess.pendingPublish[pi]
	same := ack == resident
	reclassified := ack != nil && ack.transmitted && ack.markerSeq != 0 && ack.sendNonce != _EMPTY_
	sess.mu.Unlock()
	if !same {
		t.Fatalf("fixture: the resident entry must stay in place (in-memory wins), got %+v", ack)
	}
	if !reclassified {
		t.Errorf("CONTRACT (Site 4): the resident must read possibly attempted with its marker debt kept and its "+
			"section still open, got %+v", ack)
	}
	requireVerdictRetired(t, "W-RESID", revs, sess, asm, cid, nonce)

	releaseCert()
	if abort {
		sec.abortReserved.Store(true)
	}
	sec.releaseReserved()
	select {
	case <-sec.ended:
	case <-time.After(15 * time.Second):
		t.Fatalf("fixture: the section never reached its end; windows %v", sec.windowsA())
	}
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		a := sess.pendingPublish[pi]
		if a == nil || a.regNonce != nonce || a.sendNonce != _EMPTY_ {
			return fmt.Errorf("the section must settle with the entry kept, got %+v", a)
		}
		return nil
	})
	sess.mu.Lock()
	a := sess.pendingPublish[pi]
	transmitted, markerSeq := a.transmitted, a.markerSeq
	sess.mu.Unlock()
	m, _ := asm.jsa.loadLastMsgFor(mqttInflightStreamName, markSubj)
	if !transmitted {
		t.Errorf("CONTRACT: the possibly-attempted classification is monotone, got transmitted=false")
	}
	if abort && (markerSeq == 0 || m == nil) {
		t.Errorf("CONTRACT: a section that aborts without intent must leave the marker debt (entry %d, stream %v)", markerSeq, m != nil)
	}
	if !abort && (markerSeq != 0 || m != nil) {
		t.Errorf("CONTRACT: a section that records intent consumes the marker debt (entry %d, stream %v)", markerSeq, m != nil)
	}
	t.Logf("W-RESID (abort=%v): windows %v, transmitted %v markerSeq %d marker on stream %v", abort, sec.windowsA(),
		transmitted, markerSeq, m != nil)
}

// W6a. A RESIDENT same-process reconnect runs recovery without waiting for the displaced
// connection's open section: a marker-backed register over a source nothing can re-offer is
// consumed, and its live entry is CONDEMNED with RecoveryDrop over the section's send nonce
// (parked, not deleted). If the displaced writer's PUBLISH reached the client, the client's
// PUBACK must REPLACE the parked RecoveryDrop (D4: the answer beats a parked non-answer), and the
// section's end must complete the exchange.
//
// Witness: the testRecoverDropCondemn seam at the condemn site (REACH), the entry's parked settle
// kind after the PUBACK, the exchange phase after the section ends. Input: production-reached on
// one real broker: a real delivery held at RESERVED and then at WROTE, a really deleted source
// consumer (Lost), a real second CONNECT. Not exercised: abrupt process death. If the displaced
// writer was refused, no legitimate PUBACK exists and the outcome is recorded, not credited.
//
// Mutation: X5 (an ACK kind does not replace a parked kind at condemnSendLocked), credited only
// when the replacement is observed.
func TestF088D4W6aRecoveryDropOverALiveSection(t *testing.T) {
	// Not parallel: the send-window and writer hooks are process-wide.
	const cid = "f088-w6a"
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = testF088XAckWait
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })

	cOld, rOld := testInflightSubscriber(t, o, cid, 1)
	t.Cleanup(func() { cOld.Close() })
	asm, sess := testResidentSession(t, s, cid)
	sess.mu.Lock()
	ccOld, gen, idHash := sess.c, sess.gen, sess.idHash
	sess.mu.Unlock()
	epoch := asm.sessStreamEpoch
	sec := testF088InstallSections(t, asm, nil)
	var writerReached atomic.Bool
	pre := func(c *client) {
		if c == ccOld {
			writerReached.Store(true)
		}
	}
	mqttTestWriterPreLockHookV.Store(&pre)
	t.Cleanup(func() { mqttTestWriterPreLockHookV.Store(nil) })

	// (1) The section parked at RESERVED with the token acquired and the marker standing.
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { pub.Close() })
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("one"))
	var pi uint16
	select {
	case pi = <-sec.parked:
	case <-time.After(10 * time.Second):
		t.Fatal("fixture: the send section never opened")
	}
	regSubj := mqttInflightSubject(idHash, gen, pi)
	markSubj := regSubj + "." + mqttInflightMarkerSuffix
	reg, _, err := asm.jsa.loadInflightRecord(regSubj)
	if err != nil || reg == nil || reg.ActionToken == _EMPTY_ {
		t.Fatalf("fixture: the parked section must hold the ACQUIRED action token: %+v (%v)", reg, err)
	}
	if smsg, _ := asm.jsa.loadLastMsgFor(mqttInflightStreamName, markSubj); smsg == nil {
		t.Fatal("fixture: the unsent marker must stand at RESERVED")
	}
	nonce, lin, srcDur := reg.Nonce, reg.Lin, reg.Dur
	dump := func() string {
		var b strings.Builder
		x, _, xerr := asm.jsa.loadOutboundExchange(cid, epoch, gen, lin)
		r, _, _ := asm.jsa.loadInflightRecord(regSubj)
		m, _ := asm.jsa.loadLastMsgFor(mqttInflightStreamName, markSubj)
		fmt.Fprintf(&b, "exchange %+v (%v); register %+v; marker present %v; ", x, xerr, r, m != nil)
		sess.mu.Lock()
		if a := sess.pendingPublish[pi]; a != nil {
			fmt.Fprintf(&b, "entry {nonce %s sendNonce %q settle %v transmitted %v}; ", a.regNonce, a.sendNonce,
				a.sendSettle.kind, a.transmitted)
		} else {
			b.WriteString("entry absent; ")
		}
		fmt.Fprintf(&b, "holdsPI %v chargesPI %v sendReserved %v; windows %v",
			sess.custody.holdsPI(pi), sess.custody.chargesPI(pi), sess.sendReservedLocked(pi), sec.windowsA())
		sess.mu.Unlock()
		return b.String()
	}

	// (2) The source made non-reofferable through production's own route: its consumer deleted.
	if _, err := asm.jsa.deleteConsumer(mqttStreamName, srcDur, false); err != nil {
		t.Fatalf("fixture: deleting the source consumer %q: %v", srcDur, err)
	}
	if ci, lerr := asm.jsa.lookupConsumer(mqttStreamName, srcDur); lerr == nil && ci != nil {
		t.Fatalf("fixture: the source consumer %q still exists, so the source cannot classify Lost", srcDur)
	}

	// (3) The resident same-process reconnect, with the condemn site observed.
	type condemn struct {
		pi        uint16
		sendNonce string
		parked    bool
	}
	var cmu sync.Mutex
	var condemned []condemn
	asm.mu.Lock()
	asm.testRecoverDropCondemn = func(cpi uint16, sendNonce string, parked bool) {
		cmu.Lock()
		condemned = append(condemned, condemn{cpi, sendNonce, parked})
		cmu.Unlock()
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testRecoverDropCondemn = nil
		asm.mu.Unlock()
	})
	cNew, rNew := testInflightReconnect(t, o, cid)
	t.Cleanup(func() { cNew.Close() })
	cmu.Lock()
	seen := append([]condemn(nil), condemned...)
	cmu.Unlock()
	reached := false
	for _, c := range seen {
		reached = reached || (c.pi == pi && c.sendNonce != _EMPTY_ && c.parked)
	}
	if !reached {
		t.Fatalf("REACH: recovery's RecoveryDrop must condemn %d over the open section's send nonce and park; "+
			"condemn calls %+v; state %s", pi, seen, dump())
	}
	t.Logf("REACH: RecoveryDrop parked over the open section: %+v", seen)

	// (4) The WROTE blocker FIRST, then the release of RESERVED.
	sec.wroteArmed.Store(true)
	sec.releaseReserved()
	wrote := false
	deadline := time.Now().Add(10 * time.Second)
	for !wrote {
		select {
		case <-sec.wroteReached:
			wrote = true
			continue
		case <-time.After(20 * time.Millisecond):
		}
		sess.mu.Lock()
		a := sess.pendingPublish[pi]
		open := a != nil && a.regNonce == nonce && a.sendNonce != _EMPTY_
		sess.mu.Unlock()
		if !open {
			break // the section ended before the write
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture: A's section neither wrote nor ended within 10s; state %s", dump())
		}
	}
	intent := sec.sawA(mqttMarkerSendIntent)
	gateOpen := ccOld.outBind != nil && ccOld.outBind.mayWrite()
	var oldRead string
	var gotPub bool
	if wrote {
		p, ok, eof := testF088Read(t, rOld, 2*time.Second)
		switch {
		case ok:
			oldRead = "read " + p.String()
			gotPub = p.typ == mqttPacketPub && p.pi == pi
		case eof:
			oldRead = "connection ended, nothing read"
		default:
			oldRead = "nothing read within 2s"
		}
	}
	t.Logf("displaced section: windows %v intent=%v wrote-window=%v writer reached wmu=%v old gate open=%v old reader: %s",
		sec.windowsA(), intent, wrote, writerReached.Load(), gateOpen, oldRead)

	if !gotPub {
		// No legitimate acknowledgement exists: the outcome is recorded, X5 earns no credit.
		// What IS asserted: the parked RecoveryDrop, executed at the section's end, withdraws the
		// never-written exchange and releases the identifier, its charge and its reservation.
		sec.releaseWrote()
		deadline = time.Now().Add(15 * time.Second)
		for {
			x, _, _ := asm.jsa.loadOutboundExchange(cid, epoch, gen, lin)
			r, _, _ := asm.jsa.loadInflightRecord(regSubj)
			m, _ := asm.jsa.loadLastMsgFor(mqttInflightStreamName, markSubj)
			sess.mu.Lock()
			released := !sess.custody.holdsPI(pi) && !sess.custody.chargesPI(pi) && !sess.sendReservedLocked(pi)
			sess.mu.Unlock()
			if x != nil && x.Phase == mqttExchangeWithdrawnUnsent && (r == nil || r.Nonce != nonce) && m == nil && released {
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("CONTRACT: the parked RecoveryDrop must end the never-written exchange WithdrawnUnsent with its "+
					"register and marker gone and the identifier, charge and reservation released; state %s", dump())
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Logf("OUTCOME: a RecoveryDrop over a section whose PUBLISH did not reach the client (X5 not credited); "+
			"state after the section's end: %s", dump())
		_ = rNew
		return
	}

	// (5) The client answers on its serving connection while the section is parked post-write.
	testMQTTSendPIPacket(mqttPacketPubAck, t, cNew, pi)
	replaced := false
	deadline = time.Now().Add(5 * time.Second)
	for !replaced && time.Now().Before(deadline) {
		sess.mu.Lock()
		a := sess.pendingPublish[pi]
		replaced = a != nil && a.regNonce == nonce && a.sendNonce != _EMPTY_ && a.sendSettle.kind == mqttSendSettleAckComplete
		sess.mu.Unlock()
		if !replaced {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !replaced {
		t.Errorf("CONTRACT (D4): the PUBACK must replace the parked RecoveryDrop before the section ends; state %s", dump())
	} else {
		t.Logf("OBSERVED: the PUBACK replaced the parked RecoveryDrop (X5 creditable)")
	}

	// (6) The section ends; the exchange must complete with nothing stranded.
	sec.releaseWrote()
	deadline = time.Now().Add(15 * time.Second)
	for {
		x, _, _ := asm.jsa.loadOutboundExchange(cid, epoch, gen, lin)
		sess.mu.Lock()
		a := sess.pendingPublish[pi]
		gone := (a == nil || a.regNonce != nonce) && !sess.custody.holdsPI(pi) && !sess.sendReservedLocked(pi)
		sess.mu.Unlock()
		if x != nil && x.Phase == mqttExchangeTerminal && gone {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("CONTRACT: the exchange must complete and no parked non-answer strand it; state %s", dump())
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = rNew
}
