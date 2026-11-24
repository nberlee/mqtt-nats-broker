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
	"errors"
	"fmt"
	"time"
)

// What a foreground operation does when its generation's receipt budget is spent (issue #192).
//
// The refusal is definite and pre-submission: nothing was stamped or sent. It is NOT contention: a
// contention ladder retrying under its announcement would keep every background writer yielded,
// the receipt driver's aborts and progress writes among them, and those are what return the budget.
// So the session's generation is put under PRESSURE: for mqttReceiptPressure no fresh delivery is
// allocated (the offer is withheld, as at the in-flight cap, and the source re-offers after
// AckWait), the refused allocation releases its identifier when it was never submitted, and the
// receipt driver and the floor are kicked. The pressure only suppresses work; it never authorizes a
// write, and the budget at prepare stays the only arbiter.

// mqttReceiptPressure is how long a generation's fresh deliveries are withheld after a budget
// refusal: one AckWait.
const mqttReceiptPressure = 30 * time.Second

// errMQTTReceiptPressure reports an attempt refused before submission at a spent receipt budget.
// Definite: nothing was sent.
var errMQTTReceiptPressure = errors.New("the generation's receipt budget is spent")

// relieveReceiptBudget is a foreground operation's answer to its attempt's refusal at a spent
// receipt budget of generation gen: the generation is put under pressure and the refusal becomes
// errMQTTReceiptPressure, which ends the caller's ladder without a retry. Any other error is
// returned unchanged.
//
// Session lock NOT held on entry.
func (sess *mqttSession) relieveReceiptBudget(err error, gen string) error {
	if !errors.Is(err, errMQTTReceiptBudget) {
		return err
	}
	sess.mu.Lock()
	if sess.gen == gen {
		now := time.Now()
		sess.receiptPressureGen, sess.receiptPressureUntil = gen, now.Add(mqttReceiptPressure)
		sess.armFloorLocked(gen, now)
	}
	sess.mu.Unlock()
	if sess.jsa != nil {
		sess.jsa.receipts().kickDriver()
	}
	return fmt.Errorf("%w: %v", errMQTTReceiptPressure, err)
}

// receiptPressureLocked reports whether the session's generation is under pressure now: no fresh
// delivery is allocated until it lapses. A generation change ends it.
//
// Session lock held on entry.
func (sess *mqttSession) receiptPressureLocked(now time.Time) bool {
	return sess.receiptPressureGen != _EMPTY_ && sess.receiptPressureGen == sess.gen && now.Before(sess.receiptPressureUntil)
}

// releaseUnsubmittedAllocation ends an allocation whose attempt was refused before submission at a
// spent budget: the identifier is FREED rather than excluded, since no register of it can exist,
// and the offer is withheld rather than NAKed, so the source re-offers after AckWait. It falls back
// to the ordinary abandonment for anything it cannot prove: the entry moved, an attempt of the
// allocation may have landed or is unsettled, or the ledger holds its exchange.
//
// Session lock NOT held on entry.
func (sess *mqttSession) releaseUnsubmittedAllocation(lic mqttDestroyLicence, pi uint16, jsDur string, sseq uint64, nonce string) {
	sess.mu.Lock()
	ack := sess.pendingPublish[pi]
	proven := ack != nil && ack.regPending && !ack.regCancelled && ack.jsDur == jsDur && ack.sseq == sseq &&
		ack.regNonce == nonce && nonce != _EMPTY_ && sess.custody != nil
	if proven {
		lin := ack.ev.lin
		if lin == _EMPTY_ {
			lin = nonce
		}
		proven = !sess.custody.admissionMayHaveLanded(sess.gen, nonce) && !sess.custody.hasExchange(sess.gen, lin)
	}
	if proven {
		sess.untrackPublishEntry(pi, mqttUntrackUnanswered)
		sess.mu.Unlock()
		return
	}
	sess.mu.Unlock()
	sess.abandonInflightRegister(lic, pi, jsDur, sseq, nonce)
}
