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

import "sync/atomic"

// A discarded generation's SHARED CLOSING belongs to its recovery owner (issue #107 7e).
//
// The two teardown routes close a generation's acquisition ledger inline, but the clean-start
// HANDOFF never did: its finalization writes the discarding closure and enrols the affine target,
// shared groups are deliberately not enrolled (outbound_close_enrol.go), and nothing else ever
// sealed or released the displaced generation's acquisitions — they stayed listed, their
// incarnations held, for ever. The root the discarding closure planted is the durable owner that
// was missing: it persists until certified, so a crash after the finalization, a failed closing or
// a deferred one is retried by the next visit, and the prompt drive reaches it at once.
//
// The licence is the committed discarding closure, exactly as for 7d's discard acts. A sealed
// release needs no fence (shared_release_fence.go), so the closing waits on nothing the discard
// does — and nothing about the discard waits on it: the step runs only AFTER a sweeping visit's
// execution and persistence, or at the certify point after conclusion's roll decision. What it
// gates is CERTIFICATION alone: a discarded root certifies only when its shared ledger reads SEALED
// and empty. Never on absence — absence is not the permanent exclusion (an installation or a join
// can create an open ledger over it); the closing writes the seal over absence itself.

// The closing's work-unit prices: every helper it dispatches priced at its full bounded cost
// before it starts (the recovery pass's accounting contract, outbound_recovery_pass.go). A unit is
// not a round trip, but each price below is the loop it covers, multiplied out.
const (
	// One closure read.
	mqttSharedClosingClosureUnits = 1
	// sealSharedLedger's counted attempts. A contention retry the closing's allowance absorbs is
	// charged as it is taken, at the same per-attempt price (shared_closing_contention.go).
	mqttSharedClosingSealUnits = mqttSharedAcquireAttempts * mqttSharedSealAttemptUnits
	// One sealed release episode (executeSharedRelease): its counted attempts, absorbed retries
	// charged as taken; then, for a last holder, the consumer delete and
	// retireSharedIncarnation's loop of group read, prior re-read and batch.
	mqttSharedClosingReleaseUnits = mqttSharedReleaseAttempts*mqttSharedReleaseAttemptUnits + 1 + mqttSharedReleaseAttempts*3
	// sharedClosingComplete: one ledger read.
	mqttSharedClosingCheckUnits = 1
	// mqttSharedClosingVisitEpisodes caps the release episodes one visit dispatches, so a large
	// or persistently failing closing never takes the pass's budget from other roots.
	mqttSharedClosingVisitEpisodes = 4
)

// mqttTestRecoverySharedClosingHookV substitutes one visit's shared-closing step. Returns
// (complete, true) to replace the step, (_, false) to run it. Nil in production.
var mqttTestRecoverySharedClosingHookV atomic.Pointer[func(root *mqttRecoveryRoot) (bool, bool)]

// stepSharedClosing advances a discarded generation's shared closing within this visit's budget and
// reports whether the ledger now reads SEALED and empty. For a root whose closure is absent it
// reports true: such a generation's shared ledger is not this owner's. An unreadable closure, an exhausted budget, a stop, or any store failure reports false and
// leaves the durable ledger for the next visit, which continues from it (a released entry reads
// unlisted).
func (as *mqttAccountSessionManager) stepSharedClosing(s *Server, root *mqttRecoveryRoot,
	b *mqttRecoveryBudget, stop func() bool) bool {

	if h := mqttTestRecoverySharedClosingHookV.Load(); h != nil {
		if done, override := (*h)(root); override {
			return done
		}
	}
	if !b.spend(mqttSharedClosingClosureUnits) {
		return false
	}
	_, _, cst := as.jsa.loadGenClosure(root.ID, root.Epoch, root.Gen)
	switch {
	case cst == mqttOutboundAbsent:
		return true
	case cst != mqttOutboundValid:
		return false
	}
	if !b.spend(mqttSharedClosingSealUnits) {
		return false
	}
	// One allowance for this visit's seal and releases. Each retry it absorbs is paid from the
	// visit's budget before it is taken, and a stop ends it; either leaves the durable ledger for
	// the next visit.
	cw := newMqttSharedContention(func(units int) bool {
		if stop != nil && stop() {
			return false
		}
		return b.spend(units)
	})
	cw.admit = func() bool { return as.admitRecoveryAttempt(root.ID, b) }
	sealed, err := as.jsa.sealSharedLedger(root.ID, root.Epoch, root.Gen, cw)
	if err != nil {
		s.Debugf("MQTT recovery pass: sealing the shared acquisitions of %q/%q: %v", root.ID, root.Gen, err)
		return false
	}
	episodes := 0
	var acqs []mqttSharedAcqRef
	if sealed != nil { // nil: the generation never acquired, so it has no ledger to release from
		acqs = sealed.Acqs
	}
	for _, a := range acqs {
		if (stop != nil && stop()) || episodes >= mqttSharedClosingVisitEpisodes ||
			!b.spend(mqttSharedClosingReleaseUnits) {
			return false
		}
		episodes++
		if _, rerr := as.jsa.executeSharedRelease(root.Epoch, root.ID, root.Gen, a, cw); rerr != nil {
			s.Debugf("MQTT recovery pass: releasing shared acquisition %q of %q/%q: %v", a.Acq, root.ID, root.Gen, rerr)
		}
	}
	if !b.spend(mqttSharedClosingCheckUnits) {
		return false
	}
	if cerr := as.jsa.sharedClosingComplete(root.ID, root.Epoch, root.Gen); cerr != nil {
		s.Debugf("MQTT recovery pass: the shared closing of %q/%q is not complete: %v", root.ID, root.Gen, cerr)
		return false
	}
	return true
}
