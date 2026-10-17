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

import "time"

// AN OWED WITHDRAWAL (F-088 / F-097, WP3 "withdrawOwed lifecycle").
//
// A cancellation whose unsent marker still stands is a GENUINE never-transmitted decision, and
// its withdrawal is what ends the obligation (WithdrawnUnsent) and releases its custody charge.
// When that withdrawal's landing is UNKNOWN (mqttWithdrawUnresolved), nothing may be concluded and
// nothing may be destroyed — but the obligation still needs ending, and before this record the
// only thing that would retry it was the next resume. The record names exactly what the retry
// needs; its owner is the custody dispatcher (WP4), which runs ONE withdrawUnsent per unit.
//
// What it is NOT: evidence. It authorizes nothing by itself — the retried withdrawal re-reads the
// register, the exchange and the marker, and the write boundary re-checks them, so a moved or
// transmitted obligation is refused there, never ended on the strength of this record.
type mqttWithdrawOwed struct {
	gen     string
	epoch   string
	subject string
	nonce   string
	lin     string
	regSeq  uint64
	// lic is the licence the cancellation was DECIDED under, captured from the deciding
	// connection's call and carried, never re-derived from the session's current binding. A
	// retry under it is refused by the boundary once a successor serves (mqttWithdrawUnlicensed):
	// the decision was not the successor's, and its own recovery reconstructs the withdrawal under
	// its own licence. Its background admission is not carried: the dispatcher's unit is its own
	// caller.
	lic mqttDestroyLicence

	// Dispatcher scheduling (WP4): the next time a unit may run, and the current backoff.
	nextEligibleAt time.Time
	delay          time.Duration
}

// recordWithdrawOwed records an owed withdrawal at a failure exit whose landing is unknown. One
// record per obligation (lineage): a later failure for the same obligation REPLACES the earlier
// record (its register sequence is the fresher observation), and the replacement is a new pointer,
// so a dispatcher unit holding the old one cannot retire the new one (compare-delete by identity).
//
// Session lock NOT held on entry.
func (sess *mqttSession) recordWithdrawOwed(gen, subject, nonce, lin string, regSeq uint64, lic mqttDestroyLicence) {
	if sess == nil || sess.asm == nil || !lic.entOK || lic.ent.isZero() || gen == _EMPTY_ ||
		subject == _EMPTY_ || nonce == _EMPTY_ || lin == _EMPTY_ {
		return
	}
	w := &mqttWithdrawOwed{
		gen: gen, epoch: sess.asm.sessStreamEpoch, subject: subject, nonce: nonce, lin: lin,
		regSeq: regSeq, lic: lic.withAdmission(nil),
	}
	sess.mu.Lock()
	if sess.gen == gen {
		if sess.withdrawOwed == nil {
			sess.withdrawOwed = make(map[string]*mqttWithdrawOwed)
		}
		sess.withdrawOwed[lin] = w
		sess.kickCustodyItemsLocked()
	}
	sess.mu.Unlock()
}

// retireWithdrawOwedLocked removes w only if it is still the recorded item for its obligation:
// pointer identity, so a delayed unit never retires a newer record of the same obligation.
//
// Session lock held on entry.
func (sess *mqttSession) retireWithdrawOwedLocked(w *mqttWithdrawOwed) bool {
	if w == nil || sess.withdrawOwed[w.lin] != w {
		return false
	}
	delete(sess.withdrawOwed, w.lin)
	return true
}
