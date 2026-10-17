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

//go:build mqttport_fulltests

package mqtt

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// An ordinary QoS 1 PUBACK advances the durable F-105 EXCHANGE to terminal, not just its slot.
//
// This is the row-3 completion flip reaching production: an ordinary obligation carries a
// durable exchange, so the client's acknowledgement must move the exchange off PUBLISH — a
// slot-only write would leave the exchange at PUBLISH for recovery to resend under a fresh
// identifier, the very at-most-once break the exchange exists to close.
//
// Mutation check: revert the `case mqttInflightPublish:` arm of attemptTerminalRegisterRelease
// to the slot-only advancePublishTerminal for an ordinary slot, and the exchange stays at
// PUBLISH here.
func TestF105OrdinaryPubAckAdvancesExchangeToTerminal(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "f105-ordinary-puback", 1)
	defer sub.Close()

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "f105-ordinary-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("payload"))

	asm := testMQTTGetAccountSessionManager(t, s, "f105-ordinary-puback")
	asm.mu.RLock()
	sess := asm.sessions["f105-ordinary-puback"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	gen, epoch := sess.gen, sess.asm.sessStreamEpoch
	sess.mu.Unlock()

	// Capture the exchange this identifier names BEFORE the PUBACK, while the register stands.
	reg, _, lerr := sess.jsa.loadInflightRecord(mqttInflightSubject(getHash(sess.id), gen, pi))
	if lerr != nil || reg == nil {
		t.Fatalf("the delivered register does not load back: %v", lerr)
	}
	nonce := reg.Lin
	if x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, epoch, gen, nonce); xerr != nil ||
		x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("the admitted exchange does not stand at PUBLISH before the PUBACK: %+v (%v)", x, xerr)
	}

	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)

	// The PUBACK advances the exchange to terminal. It PERSISTS there — terminal release is a
	// later row (drain + dependency discharge), so the record is readable at terminal here.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, epoch, gen, nonce)
		if xerr != nil {
			return fmt.Errorf("loading the exchange: %w", xerr)
		}
		if x == nil {
			return fmt.Errorf("the exchange is gone rather than standing at terminal")
		}
		if x.Phase != mqttExchangeTerminal {
			return fmt.Errorf("the exchange stands at phase %d after the PUBACK, want terminal", x.Phase)
		}
		return nil
	})
}

// testF105QoS2Delivered drives one ordinary QoS 2 delivery to the point where its PUBREC can
// arrive, and returns the subscriber, its session, the identifier and the exchange nonce the
// admission co-created with the register.
func testF105QoS2Delivered(t *testing.T, o *Options, s *Server, clientID string) (net.Conn, *mqttReader,
	*mqttSession, uint16, string) {
	t.Helper()
	sub, subr := testInflightSubscriber(t, o, clientID, 2)
	t.Cleanup(func() { sub.Close() })

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: clientID + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { pub.Close() })
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte("payload"))
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte("payload"))

	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	reg, _, lerr := sess.jsa.loadInflightRecord(mqttInflightSubject(getHash(sess.id), gen, pi))
	if lerr != nil || reg == nil {
		t.Fatalf("the delivered register does not load back: %v", lerr)
	}
	if x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, sess.asm.sessStreamEpoch, gen, reg.Lin); xerr != nil ||
		x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("the admitted exchange does not stand at PUBLISH before the PUBREC: %+v (%v)", x, xerr)
	}
	return sub, subr, sess, pi, reg.Lin
}

// testF105ExchangePhaseIs waits for the exchange to stand at want.
func testF105ExchangePhaseIs(t *testing.T, sess *mqttSession, nonce string, want mqttExchangePhase) {
	t.Helper()
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, sess.asm.sessStreamEpoch, gen, nonce)
		if xerr != nil {
			return fmt.Errorf("loading the exchange: %w", xerr)
		}
		if x == nil || x.Phase != want {
			return fmt.Errorf("the exchange stands at %+v, want phase %d", x, want)
		}
		return nil
	})
}

// A successful PUBREC advances the ordinary QoS 2 exchange to PUBREC-observed together with its
// slot, and recovery then reads the obligation as a RELEASE owed, never a delivery.
//
// The verdict is what is asserted, deliberately: the recovery executor withholds both deliver
// and release today, so "the payload is not re-sent" is not observable yet — but the verdict is
// what a deliver executor will act on, and a PUBLISH-phase exchange reads as delivery owed for a
// message the client has already accepted.
//
// Mutation M1: route the PUBREC's exchange arm back to the slot-only CAS, and the exchange stays
// at PUBLISH and the verdict is deliver.
func TestF105OrdinaryPubRecAdvancesExchangeToPubRecObserved(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr, sess, pi, nonce := testF105QoS2Delivered(t, o, s, "f105-ordinary-pubrec")

	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)

	testF105ExchangePhaseIs(t, sess, nonce, mqttExchangePubRecObserved)
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	reg, _, lerr := sess.jsa.loadInflightRecord(mqttInflightSubject(getHash(sess.id), gen, pi))
	if lerr != nil || reg == nil || !mqttInflightPastPubRec(reg.State) {
		t.Fatalf("the slot is not past PUBREC after a successful PUBREC: %+v (%v)", reg, lerr)
	}
	v := sess.jsa.classifyOutstandingDuty(sess.id, sess.asm.sessStreamEpoch, gen,
		&mqttDutyEntry{Kind: mqttDutyExchange, Ref: nonce})
	if v.Disposition != mqttDutyOwesWork || v.Work != mqttDutyWorkRelease {
		t.Fatalf("recovery reads a PUBREC'd obligation as %+v, want the release owed and never the delivery", v)
	}

	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
}

// A successful PUBREC whose atomic advance loses its CAS is RE-ATTEMPTED, never classified as
// "not recorded" — which would roll the PUBREC back and close the connection.
//
// The contention is deterministic and sits where a concurrent writer's would: inside the
// advance's own seam, between the read the batch is conditioned on and its commit, the slot is
// rewritten unchanged, so its sequence moves and the batch's register member is refused.
//
// Mutation M4: a single advance attempt, and the PUBREC is rolled back — no PUBREL, the
// connection closes.
func TestF105OrdinaryPubRecReattemptsALostBatch(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr, sess, pi, nonce := testF105QoS2Delivered(t, o, s, "f105-pubrec-reattempt")
	sess.mu.Lock()
	subj := mqttInflightSubject(getHash(sess.id), sess.gen, pi)
	sess.mu.Unlock()

	var attempts atomic.Int32
	var refused atomic.Bool
	asm := sess.asm
	asm.mu.Lock()
	asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.To != mqttExchangePubRecObserved || req.Nonce != nonce {
			return write()
		}
		if attempts.Add(1) == 1 {
			// THE RACE: another writer rewrites the slot after the read this batch rests on.
			moved := false
			for i := 0; i < 3 && !moved; i++ {
				cur, smsg, err := asm.jsa.loadInflightRecord(subj)
				if err != nil || cur == nil || smsg == nil {
					continue
				}
				same := *cur
				_, werr := asm.jsa.storeInflightCAS(subj, smsg.Sequence, &same)
				moved = werr == nil
			}
			if !moved {
				t.Errorf("could not move the slot's sequence under the batch")
			}
			out, err := write()
			refused.Store(err != nil)
			return out, err
		}
		return write()
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testAckAdvance = nil
		asm.mu.Unlock()
	})

	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)

	if !refused.Load() {
		t.Fatal("the first batch was not refused, so the re-attempt was never needed and nothing here is tested")
	}
	if n := attempts.Load(); n < 2 {
		t.Fatalf("the refused batch was never re-attempted (%d attempt)", n)
	}
	testF105ExchangePhaseIs(t, sess, nonce, mqttExchangePubRecObserved)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
}

// A SHARED slot's acknowledgement completes slot-only, without an exchange lookup.
//
// A shared-group delivery is admitted by the pre-F-105 writer until 7a, so it has no exchange;
// the class gate recognises it by its durable's prefix, BEFORE any read, and ends it on the slot
// alone. Without that prefix answer the gate would look the exchange up, find none, and refuse —
// and a refused terminal cleanup writes nothing and parks on its continuation, so the register
// would stand on the identifier indefinitely.
//
// Mutation M2: delete the shared/affine prefix exemption, and the register is never removed.
func TestF105SharedPubAckCompletesSlotOnly(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "f105-shared-puback"
	sub, subr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	testMQTTReadConnAckV5(t, subr)
	testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})

	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 1, false, 1, "foo", []byte("payload"), nil)
	_, pi := testMQTTReadPublishV5(t, subr, "foo", []byte("payload"))

	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the subscriber's session must be resident")
	}
	sess.mu.Lock()
	subj := mqttInflightSubject(getHash(sess.id), sess.gen, pi)
	sess.mu.Unlock()
	reg, _, lerr := sess.jsa.loadInflightRecord(subj)
	if lerr != nil || reg == nil {
		t.Fatalf("the shared delivery's register does not load: %+v (%v)", reg, lerr)
	}
	if !strings.HasPrefix(reg.Dur, mqttSharedConsumerDurablePrefix) {
		t.Fatalf("the fixture must deliver through a SHARED durable, got %q", reg.Dur)
	}

	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		cur, _, err := sess.jsa.loadInflightRecord(subj)
		if err != nil {
			return fmt.Errorf("reading the register: %w", err)
		}
		if cur != nil {
			return fmt.Errorf("the acknowledged shared register still stands: %+v", cur)
		}
		return nil
	})
}
