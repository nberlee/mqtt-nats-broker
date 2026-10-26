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
	"testing"
)

// F-097 WP6 (plan rev 20): the D4 witness gate. condemnSendLocked is FIRST-WINS: a continuation
// parked on an open send section keeps the first kind it was given. D4 asks whether a production
// route parks a NON-ACK kind first and then loses the client's acknowledgement to it.

// W6b. Drain-owned DenyRelease: the section is OPEN (its write not yet returned), the drain
// claims the delivery, an ACL-denied redelivery parks DenyRelease; the client's PUBACK arrives on
// the same connection either after the write returned or while it is still out. RECORDS the
// parked kind and whether the exchange completes.
func TestF097D4W6bDrainOwnedDenyReleaseThenPubAck(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"ack-after-write", "ack-during-write"} {
		order := order
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			w := testWithdrawAdmitted(t, "f097-w6b-"+order[:9], 1)
			f := &testLateAckFix{testWithdrawFix: w, reply: testInflightAckSubject(testRealPubRelSrcDur, w.sseq, 2)}
			testCustodyRM1(t, f.e.sess)
			f.e.bind()
			tok := f.e.openSection(t, f.pi, testRealPubRelSrcDur, f.sseq)
			intent := f.e.sess.recordSendIntent(f.e.c, f.pi, tok)
			if intent == mqttIntentDefiniteFailure || intent == mqttIntentAmbiguous {
				t.Fatalf("fixture: the send intent must land, got %d", intent)
			}
			f.claimByDrain(t)
			if r := f.e.sess.releaseTransmittedIdentifier(f.e.c, testRealPubRelSrcDur, f.reply); r != mqttAbandonNone {
				t.Fatalf("fixture: a drain-claimed deny takes the untrack route, got %v", r)
			}
			parked := func() mqttSendSettleKind {
				f.e.sess.mu.Lock()
				defer f.e.sess.mu.Unlock()
				if p := f.e.sess.pendingPublish[f.pi]; p != nil {
					return p.sendSettle.kind
				}
				return mqttSendSettleNone
			}
			if k := parked(); k != mqttSendSettleDenyRelease {
				t.Fatalf("fixture: the deny must PARK DenyRelease on the open section, got %d", k)
			}
			switch order {
			case "ack-after-write":
				f.e.sess.endPublishSend(f.e.c, f.pi, tok, intent, true)
				if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
					t.Fatalf("PUBACK: %v", err)
				}
			case "ack-during-write":
				if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
					t.Fatalf("PUBACK: %v", err)
				}
				t.Logf("W6b during-write: parked kind after the PUBACK = %d (DenyRelease=%d, AckComplete=%d)",
					parked(), mqttSendSettleDenyRelease, mqttSendSettleAckComplete)
				f.e.sess.endPublishSend(f.e.c, f.pi, tok, intent, true)
			}
			b := f.binding()
			t.Logf("W6b %s: binding after the section = %+v", order, b)
			testCustodyEventually(t, "the client's PUBACK ends the exchange", func() bool {
				x := f.exchange(t)
				return x != nil && x.Phase == mqttExchangeTerminal
			})
		})
	}
}
