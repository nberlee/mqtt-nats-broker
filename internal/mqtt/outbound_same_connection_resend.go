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

// A transmission is not repeated on the MQTT 5.0 connection that may hold it. JetStream
// re-offers an unacknowledged delivery at AckWait, and MQTT 5.0 resends a PUBLISH or a PUBREL
// only on a reconnect with a session ([MQTT-4.4.0-1]); on a 5.0 connection that may already hold
// the packet, the re-offer is absorbed. A 3.1.1 connection keeps the retry its §4.4 permits. The PUBLISH half is the entry's transmittedOn, refused in beginSendLocked;
// the PUBREL half is the send hold's owner-scoped PUBCOMP debt, refused in mqttDeliverPubRelCb.
// See docs/ARCHITECTURE.md, "A transmission is not repeated on the connection that may hold it".

package mqtt

// mqttTestSameConnectionResendRefusedHook observes a re-offer refused because the connection may
// already hold the packet: the packet type (mqttPacketPub or mqttPacketPubRel) and the identifier.
// Nil in production.
var mqttTestSameConnectionResendRefusedHook = new(mqttKnob[func(packet byte, pi uint16)])

// pubRelHeldOnLocked is the send hold under pi when a PUBREL under it possibly reached cc and is
// unanswered: the hold's PUBCOMP debt stands and is owed by cc. Nil otherwise. Units for provably
// unemitted frames are given back (issue #118), and the identifier cannot change exchange while the
// debt stands, so this is a fact about THIS exchange's PUBREL on THIS connection. A predecessor's
// debt is not cc's: the opener resets it. The one reader of the registry for this fact.
//
// Session lock held on entry.
func (sess *mqttSession) pubRelHeldOnLocked(cc *client, pi uint16) *mqttSendHold {
	h := sess.sendsInFlight[pi]
	if h == nil || cc == nil || cc.cid == 0 || h.owner != cc.cid || h.pubCompOwed <= 0 {
		return nil
	}
	return h
}

// pubRelResendLocked judges a stored PUBREL re-offer to cc BEFORE its opener runs (the opener's
// own unit would make every debt look standing). resend is true when the PUBREL is held on cc and
// nobody asked for it again: it is not written. answer is true when it is held but a duplicate
// PUBREC owes it as the answer; the caller binds that answer to the token it then opens
// (bindPubRelAnswerLocked), so the section's end can restore it if the frame never left.
//
// Session lock held on entry.
func (sess *mqttSession) pubRelResendLocked(cc *client, pi uint16) (resend, answer bool) {
	if cc == nil || cc.mqtt == nil || cc.mqtt.proto != mqttProtoLevel5 {
		return false, false // 3.1.1 keeps the retry its §4.4 permits, and takes no answer
	}
	h := sess.pubRelHeldOnLocked(cc, pi)
	if h == nil {
		return false, false
	}
	if h.answerOwed {
		return false, true
	}
	return true, false
}

// bindPubRelAnswerLocked moves the owed answer onto the section cc just opened under pi, so a
// concurrent re-offer cannot take it too. In the same locked section as the opener, which reused
// the hold (its owner is the answering connection and the open unit keeps the debt standing), so
// the hold read here is the one the token names.
//
// Session lock held on entry.
func (sess *mqttSession) bindPubRelAnswerLocked(cc *client, pi uint16) {
	if h := sess.pubRelHeldOnLocked(cc, pi); h != nil {
		h.answerOwed, h.answering = false, true
	}
}

// owePubRelAnswerLocked records that a successful duplicate PUBREC from c owes one PUBREL in
// answer ([MQTT-4.3.3]). Only on the hold's owner connection: a debt owed to another connection is
// not what the re-offer will be judged against, so there is nothing to override there.
//
// Session lock held on entry.
func (sess *mqttSession) owePubRelAnswerLocked(c *client, pi uint16) {
	if h := sess.pubRelHeldOnLocked(c, pi); h != nil {
		h.answerOwed = true
	}
}
