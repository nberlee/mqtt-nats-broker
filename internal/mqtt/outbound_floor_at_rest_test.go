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
	"testing"
	"time"
)

// TestFloorRetiresAModestSpanAtRest: every admission reads the duty of every position in its
// generation's span, so a session at rest must not keep a hundred completed positions in it. A span
// of 100 completed obligations — above the slack, and far below the 256 the floor once waited for —
// is retired by the floor without any further traffic.
func TestFloorRetiresAModestSpanAtRest(t *testing.T) {
	// Fixed numbers, deliberately not derived from the slack: the floor must bring a span of 100
	// completed positions down to 64 or fewer at rest.
	const n, atRest = 100, 64
	f := testWithdrawAdmitted(t, "floor-at-rest", 1)
	e := f.e
	sess := e.sess
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch
	gen := f.gen
	for i := 0; i < n; i++ {
		al := jsa.loadOutboundAuthority(sess.id, epoch)
		txn := mqttNewInflightNonce()
		x := &mqttOutboundExchange{
			ID: sess.id, Epoch: epoch, Gen: gen, Nonce: mqttNewInflightNonce(),
			PI: 900 + uint16(i), QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		tr, err := testAdmitPublishTr(t, jsa, al.rec, al.seq, txn, "op-"+txn, x)
		if err != nil {
			t.Fatalf("composing %d: %v", i, err)
		}
		if _, err := jsa.commitAuthorityTransition(tr); err != nil {
			t.Fatalf("admitting %d: %v", i, err)
		}
		if _, err := testAdvanceLive(t, jsa, sess.id, epoch, gen, x.Nonce, mqttExchangeTerminal); err != nil {
			t.Fatalf("ending %d: %v", i, err)
		}
		testDropEndedSlot(t, jsa, sess.id, epoch, gen, x.Nonce)
	}
	// The resident process observes its ledger, as any admission's census does, and goes quiet.
	sess.reconstructCustody(gen)

	deadline := time.Now().Add(30 * time.Second)
	var led *mqttGenLedger
	for time.Now().Before(deadline) {
		led, _, _ = jsa.loadGenLedger(sess.id, epoch, gen)
		if led != nil && mqttLedgerSpan(led) <= atRest {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if led == nil || mqttLedgerSpan(led) > atRest {
		t.Fatalf("a session at rest kept %d positions of completed history in its span (ledger %+v)",
			mqttLedgerSpan(led), led)
	}
}
