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

//go:build mqtt_fulltests

package mqtt

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// The runtime half of the restated output boundary (F-105 WP-F step 1).
//
// Two raw writers stay outside both boundaries because they are MIXED: they carry supported QoS 0
// and actionless acknowledgements beside chapter P's slot-only QoS 1/2. The structural check can
// pin how many raw writes each site makes; it cannot say which traffic passes through them. This
// test watches the frames themselves, on real clients, across every supported route a QoS 1/2
// PUBLISH or a PUBREL takes - live delivery, retransmission, retained replay, shared delivery, and
// the resends a resumed session and a restarted server perform - and asserts none of them crosses
// a mixed site. Each site is also shown to be REACHED by this fixture, separately, so a silent
// hook cannot pass for a clean one.

type testMixedFrame struct {
	seq   int
	site  string
	cid   string
	first byte
}

type testMixedSiteWatch struct {
	mu     sync.Mutex
	srvs   map[*Server]bool
	frames []testMixedFrame
}

// newTestMixedSiteWatch installs the hook for the life of the test. Frames from servers this test
// did not register are ignored, so a concurrently running test cannot pollute the record; the hook
// itself never touches t, so a late call after the test cannot fail the binary.
func newTestMixedSiteWatch(t *testing.T) *testMixedSiteWatch {
	w := &testMixedSiteWatch{srvs: map[*Server]bool{}}
	restore := setMQTTTestMixedSiteFrame(func(c *client, site string, frame []byte) {
		if len(frame) == 0 {
			return
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		if !w.srvs[c.srv] {
			return
		}
		cid := _EMPTY_
		if c.mqtt != nil {
			cid = c.mqtt.cid // both sites call the hook with c.mu held
		}
		w.frames = append(w.frames, testMixedFrame{seq: len(w.frames), site: site, cid: cid, first: frame[0]})
	})
	t.Cleanup(restore)
	return w
}

func (w *testMixedSiteWatch) watch(s *Server) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.srvs[s] = true
}

func (w *testMixedSiteWatch) mark() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.frames)
}

// carriesObligation is the property: a QoS 1/2 PUBLISH or a PUBREL at a mixed site.
func (f testMixedFrame) carriesObligation() bool {
	switch f.first & mqttPacketMask {
	case mqttPacketPub:
		return mqttGetQoS(f.first&mqttPacketFlagMask) > 0
	case mqttPacketPubRel:
		return true
	}
	return false
}

// requireNoObligation is scoped to frames at or after since, so each schedule answers for its
// own traffic and a failure in one cannot be what fails the next.
func (w *testMixedSiteWatch) requireNoObligation(t *testing.T, since int, what string) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, f := range w.frames[since:] {
		if f.carriesObligation() {
			t.Fatalf("%s: a supported obligation left through a mixed site: %q wrote 0x%02x for %q",
				what, f.site, f.first, f.cid)
		}
	}
}

// requireSaw asserts the fixture REACHED site with a frame of packet type pt for cid, at or after
// since. Without it a removed hook would pass requireNoObligation vacuously.
func (w *testMixedSiteWatch) requireSaw(t *testing.T, site, cid string, pt byte, since int, what string) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, f := range w.frames[since:] {
		if f.site == site && f.cid == cid && f.first&mqttPacketMask == pt {
			return
		}
	}
	t.Fatalf("%s: the fixture never reached %q with packet type 0x%02x for %q, so its silence proves "+
		"nothing", what, site, pt, cid)
}

type testMixedRx struct {
	topic string
	qos   byte
	dup   bool
	pi    uint16
}

// testMixedClient is a v5 client with a persistent session that reads whatever arrives and
// acknowledges it as told.
type testMixedClient struct {
	ci   *mqttV5ConnInfo
	conn net.Conn
	r    *mqttReader
	pubs []testMixedRx
	rels []uint16
}

func testMixedConnect(t *testing.T, o *Options, clientID string, wantPresent bool) *testMixedClient {
	t.Helper()
	m := &testMixedClient{ci: &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(3600)}}
	m.reconnect(t, o, wantPresent)
	return m
}

func (m *testMixedClient) reconnect(t *testing.T, o *Options, wantPresent bool) {
	t.Helper()
	m.conn, m.r = testMQTTConnectV5(t, m.ci, o.MQTT.Host, o.MQTT.Port)
	conn := m.conn
	t.Cleanup(func() { conn.Close() })
	if present, _, _ := testMQTTReadConnAckV5(t, m.r); present != wantPresent {
		t.Fatalf("%s: session present=%v, want %v", m.ci.clientID, present, wantPresent)
	}
	m.pubs, m.rels = nil, nil
}

// pump reads packets until done reports true. ackPub answers a QoS 1 PUBLISH with PUBACK and a
// QoS 2 one with PUBREC; ackRel answers a PUBREL with PUBCOMP.
func (m *testMixedClient) pump(t *testing.T, ackPub, ackRel bool, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: timed out; received publishes %+v and PUBRELs %v", m.ci.clientID, m.pubs, m.rels)
		}
		b, pl, ok := testMQTTReadPacketReady(m.r, time.Until(deadline))
		if !ok {
			t.Fatalf("%s: connection failed while waiting; received %+v / %v", m.ci.clientID, m.pubs, m.rels)
		}
		body := m.r.buf[m.r.pos : m.r.pos+pl]
		m.r.pos += pl
		switch b & mqttPacketMask {
		case mqttPacketPub:
			rx := testMixedRx{qos: mqttGetQoS(b & mqttPacketFlagMask), dup: b&mqttPubFlagDup != 0}
			tl := int(binary.BigEndian.Uint16(body))
			rx.topic = string(body[2 : 2+tl])
			if rx.qos > 0 {
				rx.pi = binary.BigEndian.Uint16(body[2+tl:])
			}
			m.pubs = append(m.pubs, rx)
			if ackPub {
				switch rx.qos {
				case 1:
					testMQTTSendPIPacket(mqttPacketPubAck, t, m.conn, rx.pi)
				case 2:
					testMQTTSendPIPacket(mqttPacketPubRec, t, m.conn, rx.pi)
				}
			}
		case mqttPacketPubRel:
			pi := binary.BigEndian.Uint16(body)
			m.rels = append(m.rels, pi)
			if ackRel {
				testMQTTSendPIPacket(mqttPacketPubComp, t, m.conn, pi)
			}
		}
	}
}

func (m *testMixedClient) got(topic string, qos byte) func() bool {
	return func() bool {
		for _, p := range m.pubs {
			if p.topic == topic && p.qos == qos {
				return true
			}
		}
		return false
	}
}

func (m *testMixedClient) gotRel(pi uint16) func() bool {
	return func() bool {
		for _, r := range m.rels {
			if r == pi {
				return true
			}
		}
		return false
	}
}

func (m *testMixedClient) piOf(t *testing.T, topic string) uint16 {
	t.Helper()
	for _, p := range m.pubs {
		if p.topic == topic {
			return p.pi
		}
	}
	t.Fatalf("%s: no PUBLISH on %q received", m.ci.clientID, topic)
	return 0
}

func testMixedAll(fs ...func() bool) func() bool {
	return func() bool {
		for _, f := range fs {
			if !f() {
				return false
			}
		}
		return true
	}
}

// testMixedPublish publishes from a fresh clean-start client, completing its own handshake.
func testMixedPublish(t *testing.T, o *Options, cid, topic string, qos byte, retain bool) {
	t.Helper()
	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, qos, retain, 1, topic, []byte("payload"), nil)
}

// testMixedRequireSettled waits until clientID's session holds no outstanding outbound exchange.
func testMixedRequireSettled(t *testing.T, s *Server, clientID string) {
	t.Helper()
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		asm.mu.RLock()
		sess := asm.sessions[clientID]
		asm.mu.RUnlock()
		if sess == nil {
			return fmt.Errorf("no resident session")
		}
		sess.mu.Lock()
		np, nr := len(sess.pendingPublish), len(sess.pendingPubRel)
		sess.mu.Unlock()
		if np != 0 || nr != 0 {
			return fmt.Errorf("%d PUBLISH and %d PUBREL exchanges still outstanding", np, nr)
		}
		return nil
	})
}

// TestF105SupportedObligationsNeverLeaveThroughAMixedSite is T2a (live routes) and T2b (recovery
// routes) of WP-F step 1.
//
// Deliberately NOT parallel: the hook is process-global, and its restore must not interleave
// with another installation of it.
func TestF105SupportedObligationsNeverLeaveThroughAMixedSite(t *testing.T) {
	w := newTestMixedSiteWatch(t)
	o := testMQTTDefaultOptionsV5()
	// The restart arm's resend rides redelivery, as in the inflight restart tests.
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	w.watch(s)
	t.Cleanup(func() { testMQTTShutdownServer(s) })

	t.Run("T2a live routes", func(t *testing.T) {
		since := w.mark()
		sub := testMixedConnect(t, o, "mx-sub", false)
		testMQTTSubV5(t, sub.conn, sub.r, 1, []mqttV5SubFilter{{topic: "mx/#", opts: 2}})

		// Ordinary QoS 1, ordinary QoS 2 to PUBCOMP (an ordinary PUBREL), and the QoS 0 delivery
		// that reaches the raw PUBLISH branch on purpose.
		testMixedPublish(t, o, "mx-pub-1", "mx/1", 1, false)
		sub.pump(t, true, true, sub.got("mx/1", 1))
		testMixedPublish(t, o, "mx-pub-2", "mx/2", 2, false)
		sub.pump(t, true, true, testMixedAll(sub.got("mx/2", 2), func() bool { return sub.gotRel(sub.piOf(t, "mx/2"))() }))
		testMixedPublish(t, o, "mx-pub-0", "mx/0", 0, false)
		sub.pump(t, true, true, sub.got("mx/0", 0))

		// A QoS 2 PUBLISH RETRANSMISSION: no PUBREC, then a resumed session.
		testMixedPublish(t, o, "mx-pub-r", "mx/r", 2, false)
		sub.pump(t, false, false, sub.got("mx/r", 2))
		sub.conn.Close()
		sub.reconnect(t, o, true)
		sub.pump(t, true, true, func() bool {
			for _, p := range sub.pubs {
				if p.topic == "mx/r" && p.qos == 2 && p.dup {
					return sub.gotRel(p.pi)()
				}
			}
			return false
		})

		// Retained QoS 1 and QoS 2 delivered on SUBSCRIBE; the QoS 2 one runs the retained PUBREL
		// producer.
		testMixedPublish(t, o, "mx-pub-r1", "mxr/1", 1, true)
		testMixedPublish(t, o, "mx-pub-r2", "mxr/2", 2, true)
		testMQTTSubV5(t, sub.conn, sub.r, 2, []mqttV5SubFilter{{topic: "mxr/#", opts: 2}})
		sub.pump(t, true, true, testMixedAll(sub.got("mxr/1", 1), sub.got("mxr/2", 2),
			func() bool { return sub.gotRel(sub.piOf(t, "mxr/2"))() }))

		// A non-affine shared-group QoS 1 delivery.
		sh := testMixedConnect(t, o, "mx-sh", false)
		testMQTTSubV5(t, sh.conn, sh.r, 1, []mqttV5SubFilter{{topic: "$share/g/mxs", opts: 1}})
		testMixedPublish(t, o, "mx-pub-s", "mxs", 1, false)
		sh.pump(t, true, true, sh.got("mxs", 1))

		testMixedRequireSettled(t, s, "mx-sub")
		testMixedRequireSettled(t, s, "mx-sh")
		w.requireNoObligation(t, since, "T2a")
		// (c) EACH site reached, separately.
		w.requireSaw(t, mqttMixedSitePublishRaw, "mx-sub", mqttPacketPub, since, "T2a raw PUBLISH branch")
		w.requireSaw(t, mqttMixedSiteProtoLocked, "mx-pub-1", mqttPacketPubAck, since, "T2a locked emitter")
		w.requireSaw(t, mqttMixedSiteProtoLocked, "mx-pub-2", mqttPacketPubRec, since, "T2a locked emitter")
		w.requireSaw(t, mqttMixedSiteProtoLocked, "mx-pub-2", mqttPacketPubComp, since, "T2a locked emitter")
	})

	t.Run("T2b(1) PUBREL resend on resume", func(t *testing.T) {
		start := w.mark()
		m := testMixedConnect(t, o, "mx-rel", false)
		testMQTTSubV5(t, m.conn, m.r, 1, []mqttV5SubFilter{{topic: "mxp", opts: 2}})
		testMixedPublish(t, o, "mx-pub-p", "mxp", 2, false)
		m.pump(t, true, false, testMixedAll(m.got("mxp", 2), func() bool { return m.gotRel(m.piOf(t, "mxp"))() }))
		pi := m.piOf(t, "mxp")
		m.conn.Close()
		m.reconnect(t, o, true)
		since := w.mark()
		m.pump(t, true, true, m.gotRel(pi))
		testMixedRequireSettled(t, s, "mx-rel")
		testMQTTFlush(t, m.conn, nil, m.r)
		w.requireNoObligation(t, start, "T2b(1)")
		w.requireSaw(t, mqttMixedSiteProtoLocked, "mx-rel", mqttPacketPingResp, since, "T2b(1) resumed connection")
	})

	t.Run("T2b(2) retained PUBREL resend on resume", func(t *testing.T) {
		start := w.mark()
		testMixedPublish(t, o, "mx-pub-rr", "mxrr", 2, true)
		m := testMixedConnect(t, o, "mx-rr", false)
		testMQTTSubV5(t, m.conn, m.r, 1, []mqttV5SubFilter{{topic: "mxrr", opts: 2}})
		m.pump(t, true, false, testMixedAll(m.got("mxrr", 2), func() bool { return m.gotRel(m.piOf(t, "mxrr"))() }))
		pi := m.piOf(t, "mxrr")
		m.conn.Close()
		m.reconnect(t, o, true)
		since := w.mark()
		m.pump(t, true, true, m.gotRel(pi))
		testMixedRequireSettled(t, s, "mx-rr")
		testMQTTFlush(t, m.conn, nil, m.r)
		w.requireNoObligation(t, start, "T2b(2)")
		w.requireSaw(t, mqttMixedSiteProtoLocked, "mx-rr", mqttPacketPingResp, since, "T2b(2) resumed connection")
	})

	t.Run("T2b(3) recovery resend across a restart", func(t *testing.T) {
		start := w.mark()
		m := testMixedConnect(t, o, "mx-rs", false)
		testMQTTSubV5(t, m.conn, m.r, 1, []mqttV5SubFilter{{topic: "mxq/#", opts: 2}})
		testMixedPublish(t, o, "mx-pub-q1", "mxq/1", 1, false)
		m.pump(t, false, false, m.got("mxq/1", 1))
		testMixedPublish(t, o, "mx-pub-q2", "mxq/2", 2, false)
		m.pump(t, false, false, m.got("mxq/2", 2))
		pi2 := m.piOf(t, "mxq/2")
		testMQTTSendPIPacket(mqttPacketPubRec, t, m.conn, pi2)
		m.pump(t, false, false, m.gotRel(pi2))
		m.conn.Close()

		s = testInflightRestart(t, s, o)
		w.watch(s)
		t.Cleanup(func() { testMQTTShutdownServer(s) })
		since := w.mark()
		m.reconnect(t, o, true)
		m.pump(t, true, true, testMixedAll(m.got("mxq/1", 1), m.gotRel(pi2)))
		testMixedRequireSettled(t, s, "mx-rs")
		testMQTTFlush(t, m.conn, nil, m.r)
		w.requireNoObligation(t, start, "T2b(3)")
		w.requireSaw(t, mqttMixedSiteProtoLocked, "mx-rs", mqttPacketPingResp, since, "T2b(3) resumed connection")
	})
}
