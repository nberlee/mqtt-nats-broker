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
	"fmt"
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
