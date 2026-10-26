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
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// A RESUMED CLIENT MAY ACKNOWLEDGE BEFORE THE BROKER RETRANSMITS (F-105 WP-F W2, row C5b).
//
// [MQTT-4.4.0-1] obliges the broker to resend unacknowledged PUBLISH and PUBREL packets when a
// session resumes, but nothing obliges the client to wait for them: a client that persisted the
// PUBLISH it received on the old connection may answer it on the new one before any resend. The
// acknowledgement names the identifier, the identifier names the session's exchange, and the
// exchange is the obligation - not the connection that last carried it. So the new connection's
// acknowledgement ends (PUBACK) or advances (PUBREC) the exchange at once, and the resend that was
// owed for it is then not owed: no DUP PUBLISH follows.
//
// Mutation (C5b): an acknowledgement is refused unless the slot's action is bound to THIS
// connection (mqttActionBoundTo inserted into the ack paths). Target: the exchange reaching
// Terminal / PubRecObserved from the new connection's acknowledgement.

// testW2ResumeRig is one persistent subscriber whose QoS 1/2 delivery was received on its first
// connection and never answered there.
type testW2ResumeRig struct {
	o    *Options
	s    *Server
	cid  string
	pi   uint16
	id   string
	gen  string
	lin  string
	pub  net.Conn
	pubr *mqttReader
	sub  net.Conn
	subr *mqttReader
}

func testW2NewResumeRig(t *testing.T, cid string, qos byte, ackWait time.Duration) *testW2ResumeRig {
	t.Helper()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = ackWait
	r := &testW2ResumeRig{o: o, cid: cid}
	r.s = testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(r.s) })
	r.sub, r.subr = testInflightSubscriber(t, o, cid, qos)
	r.pub, r.pubr = testMQTTConnect(t, &mqttConnInfo{clientID: cid + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { r.pub.Close() })
	testMQTTCheckConnAck(t, r.pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, r.pub, r.pubr, qos, false, false, "foo", 1, []byte("payload"))
	flags := byte(mqttPubQos1)
	if qos == 2 {
		flags = mqttPubQoS2
	}
	r.pi = testMQTTCheckPubMsgNoAck(t, r.sub, r.subr, "foo", flags, []byte("payload"))
	asm := testMQTTGetAccountSessionManager(t, r.s, cid)
	asm.mu.RLock()
	sess := asm.sessions[cid]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("fixture: no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash := sess.idHash
	r.id, r.gen = sess.id, sess.gen
	sess.mu.Unlock()
	rec, _, err := asm.loadInflightRecord(mqttInflightSubject(idHash, r.gen, r.pi))
	if err != nil || rec == nil || rec.Lin == _EMPTY_ {
		t.Fatalf("fixture: the delivery's register names no exchange: %+v (%v)", rec, err)
	}
	r.lin = rec.Lin
	return r
}

// reconnect drops the first connection (on the store route, restarting the broker on the same
// store) and resumes the session on a second one, Session Present.
func (r *testW2ResumeRig) reconnect(t *testing.T, restart bool) {
	t.Helper()
	r.sub.Close()
	if restart {
		r.s = testInflightRestart(t, r.s, r.o)
	}
	r.sub, r.subr = testInflightReconnect(t, r.o, r.cid)
	t.Cleanup(func() { r.sub.Close() })
}

func (r *testW2ResumeRig) phase(t *testing.T) (mqttExchangePhase, error) {
	t.Helper()
	asm := testMQTTGetAccountSessionManager(t, r.s, r.cid)
	x, _, err := asm.jsa.loadOutboundExchange(r.id, asm.sessStreamEpoch, r.gen, r.lin)
	if err != nil {
		return 0, err
	}
	return x.Phase, nil
}

// waitPhase waits for the exchange to reach want. The previous connection's close may still
// hold the slot's action token for a moment, so this is an eventual state, not an immediate one.
func (r *testW2ResumeRig) waitPhase(t *testing.T, want mqttExchangePhase, why string) {
	t.Helper()
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		ph, err := r.phase(t)
		if err != nil {
			return err
		}
		if ph != want {
			return fmt.Errorf("%s: the exchange stands at phase %d, want %d", why, ph, want)
		}
		return nil
	})
}

func TestF105ResumedConnectionAcksBeforeRetransmission(t *testing.T) {
	// NOT parallel: the pre-send and PUBREL delivery seams are process-wide.
	for _, tc := range []struct {
		name string
		qos  byte
	}{{"PUBACK", 1}, {"PUBREC", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			// The retransmission seam, installed before any server starts and armed only once the
			// first connection is gone: every re-offer of the delivery is declined BEFORE tracking,
			// so nothing is retransmitted on the second connection until the test releases it.
			var holding atomic.Bool
			var declined atomic.Int64
			t.Cleanup(setMQTTTestInflightPreSendHook(func(string, string) bool {
				if holding.Load() {
					declined.Add(1)
					return false
				}
				return true
			}))
			// AckWait short, so an owed re-offer of the source would surface inside the no-DUP
			// window below.
			r := testW2NewResumeRig(t, "w2-ack-resume-"+tc.name, tc.qos, time.Second)
			holding.Store(true)
			r.reconnect(t, false)
			// ROUTE WITNESS: the resumed session's retransmission was attempted and held.
			checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
				if declined.Load() == 0 {
					return fmt.Errorf("the resumed session never re-offered the delivery")
				}
				return nil
			})

			// The client answers at once, on the new connection, before any resend.
			if tc.qos == 1 {
				testMQTTSendPIPacket(mqttPacketPubAck, t, r.sub, r.pi)
				// TARGET.
				r.waitPhase(t, mqttExchangeTerminal, "a resumed connection's PUBACK before retransmission")
			} else {
				testMQTTSendPIPacket(mqttPacketPubRec, t, r.sub, r.pi)
				// TARGET.
				r.waitPhase(t, mqttExchangePubRecObserved, "a resumed connection's PUBREC before retransmission")
				testMQTTReadPIPacket(mqttPacketPubRel, t, r.subr, r.pi)
			}

			// Released: the resend was owed only while the exchange was unanswered.
			holding.Store(false)
			if tc.qos == 2 {
				testMQTTSendPIPacket(mqttPacketPubComp, t, r.sub, r.pi)
			}
			testMQTTExpectNothingFor(t, r.subr, 3*r.o.MQTT.AckWait)
			if tc.qos == 2 {
				r.waitPhase(t, mqttExchangeTerminal, "the PUBCOMP after the resumed PUBREC")
			}
		})
	}

	// PUBCOMP BEFORE THE PUBREL IS RE-SENT. Observed on both routes (2026-10-06): on the RESIDENT
	// route the early PUBCOMP completes the exchange at once and the held PUBREL redelivery is then
	// withheld ("no owner slot names exchange ..."); on the STORE-RESTORED route the early PUBCOMP
	// is dropped (the exchange stays at PubRecObserved), the broker re-sends the PUBREL under the
	// same identifier, and the client's second PUBCOMP completes it. The rule both must satisfy: an
	// early PUBCOMP is either accepted - and then no PUBREL follows - or dropped, and then the
	// PUBREL IS re-sent and the second PUBCOMP completes the exchange. A dropped PUBCOMP with no
	// resend would strand the exchange; a PUBREL after an accepted PUBCOMP is a spurious resend.
	for _, route := range []struct {
		name    string
		restart bool
	}{{"resident", false}, {"store-restored", true}} {
		t.Run("PUBCOMP/"+route.name, func(t *testing.T) {
			var holding atomic.Bool
			var held atomic.Int64
			release := make(chan struct{})
			t.Cleanup(setMQTTTestPubRelDeliveryDelay(func() {
				if holding.Load() {
					held.Add(1)
					<-release
				}
			}))
			var once atomic.Bool
			releaseOnce := func() {
				if once.CompareAndSwap(false, true) {
					holding.Store(false)
					close(release)
				}
			}
			// AckWait longer than the hold: a PUBREL held past it is re-offered by JetStream and
			// every queued copy is transmitted on release, which is a duplicate-PUBREL schedule
			// of this fixture's own making, not the early-PUBCOMP question.
			r := testW2NewResumeRig(t, "w2-pubcomp-"+route.name, 2, 5*time.Second)
			// Registered after the server, so it runs BEFORE the shutdown: a held callback must
			// not outlive the test into the server's teardown.
			t.Cleanup(releaseOnce)
			testMQTTSendPIPacket(mqttPacketPubRec, t, r.sub, r.pi)
			testMQTTReadPIPacket(mqttPacketPubRel, t, r.subr, r.pi)
			holding.Store(true)
			r.reconnect(t, route.restart)
			// ROUTE WITNESS: the resumed PUBREL redelivery is held, so the PUBCOMP below precedes it.
			checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
				if held.Load() == 0 {
					return fmt.Errorf("the resumed session never redelivered its PUBREL")
				}
				return nil
			})
			if ph, err := r.phase(t); err != nil || ph != mqttExchangePubRecObserved {
				t.Fatalf("fixture: the exchange stands at %d (%v) before the early PUBCOMP, want PubRecObserved", ph, err)
			}
			testMQTTSendPIPacket(mqttPacketPubComp, t, r.sub, r.pi)
			accepted := false
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if ph, err := r.phase(t); err == nil && ph == mqttExchangeTerminal {
					accepted = true
					break
				}
				time.Sleep(25 * time.Millisecond)
			}
			releaseOnce()
			if accepted {
				t.Logf("%s: the early PUBCOMP completed the exchange", route.name)
				testMQTTExpectNothingFor(t, r.subr, r.o.MQTT.AckWait+time.Second)
				return
			}
			t.Logf("%s: the early PUBCOMP was dropped; requiring the PUBREL resend", route.name)
			testMQTTReadPIPacket(mqttPacketPubRel, t, r.subr, r.pi)
			testMQTTSendPIPacket(mqttPacketPubComp, t, r.sub, r.pi)
			r.waitPhase(t, mqttExchangeTerminal, "the second PUBCOMP after the PUBREL resend")
			testMQTTExpectNothingFor(t, r.subr, r.o.MQTT.AckWait+time.Second)
		})
	}
}
