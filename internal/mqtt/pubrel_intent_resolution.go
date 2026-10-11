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

package mqtt

import "time"

// The key is immutable even when classification refreshes the resolver's CAS hint.
type mqttIntentResolutionKey struct {
	sess  *mqttSession
	name  mqttInflightName
	epoch string
	nonce string
	lin   string
	dur   string
	seq   uint64
	acq   uint64
	qos   byte
}

type mqttIntentResolution struct {
	key         mqttIntentResolutionKey
	srv         *Server
	c           *client
	ack         mqttPending
	placeholder *mqttPending
	// running covers both an admitted timer and its attempt, under owedIntentsMu.
	running bool
}

func (r *mqttIntentResolution) schedule() {
	s := r.srv
	s.owedIntentsMu.Lock()
	if s.owedIntents[r.key] != r || r.running {
		s.owedIntentsMu.Unlock()
		return
	}
	if !s.admitWillWorker() {
		// The same reachable owner remains owed before refusal returns.
		s.owedIntentsMu.Unlock()
		return
	}
	r.running = true
	s.owedIntentsMu.Unlock()
	time.AfterFunc(mqttActionReconcileDelay, func() {
		defer s.willWorkerWG.Done()
		done := r.attempt(false)
		r.finish(done)
		if !done {
			r.schedule()
		}
	})
}

func (r *mqttIntentResolution) finish(done bool) {
	s := r.srv
	s.owedIntentsMu.Lock()
	defer s.owedIntentsMu.Unlock()
	r.running = false
	if done && s.owedIntents[r.key] == r {
		delete(s.owedIntents, r.key)
	}
}

// attempt classifies before retrying. No failed read means "not recorded", and an
// unanswered abandoned origin retains the successful-PUBREC meaning, not a PUBLISH.
func (r *mqttIntentResolution) attempt(final bool) bool {
	sess := r.key.sess
	sess.mu.Lock()
	stale := sess.gen != r.key.name.gen
	// A RETAINED exchange a resume quarantined is acted on by nothing — not even this intent's
	// register CAS (F-105 7c, consult 107).
	quarantined := sess.retainedQuarantinedLocked(r.ack.regNonce)
	sess.mu.Unlock()
	if stale || quarantined {
		return true // This generation was discarded, not displaced by another serving tenure.
	}
	if sess.asm == nil || sess.asm.sessStreamEpoch != r.key.epoch {
		return false // No evidence settles a captured obligation in another store epoch.
	}
	out := sess.resolvePubRelIntent(r.c, &r.ack)
	if out == mqttPubRelIntentStillOwed {
		// THE OWED SUCCESSFUL PUBREC'S CONTINUATION (F-088 WP2): a HandBack's or an AckRoute's bound
		// allocation retries its intent arrow; intendPubRelSlot re-applies its own captured-acq
		// check and the authority batch re-validates the register at its read sequence.
		out = sess.intendPubRelSlot(r.c, &r.ack)
	}
	switch out {
	case mqttPubRelIntentUnresolved, mqttPubRelIntentStillOwed:
		return false
	case mqttPubRelIntentConfirmed:
		if b := r.ack.slot.abandonedAck; b != nil && b.payload != _EMPTY_ {
			// An ABANDONED RETAINED exchange (F-105 7c): no materializer owns its PUBREL — its
			// advance created the canonical — so the confirmation installs and emits it here.
			sess.continueAbandonedRetainedPubRel(r.c, r.key.name.pi, &r.ack)
			return true
		}
		if r.ack.slot.state == mqttInflightPubRelIntended {
			// A materializer timer can be refused when shutdown closes admission
			// during an already-running resolver. Keep this owner until the durable
			// intent has actually been handed to a canonical PUBREL (also off shutdown).
			sess.attemptOwedMaterialization(r.c, &mqttOwedMaterialization{slot: r.ack.slot})
			out = sess.resolvePubRelIntent(r.c, &r.ack)
			return out == mqttPubRelIntentEnded || out == mqttPubRelIntentOther ||
				(out == mqttPubRelIntentConfirmed && r.ack.slot.state != mqttInflightPubRelIntended)
		}
		return true // The existing materializer owns the durable intent.
	case mqttPubRelIntentEnded, mqttPubRelIntentOther:
		return true
	default:
		sess.mu.Lock()
		if cur := sess.pendingPubRel[r.key.name.pi]; cur != nil &&
			cur == r.placeholder && cur.jsAckSubject == _EMPTY_ {
			delete(sess.pendingPubRel, r.key.name.pi)
			sess.retrackPublishLocked(r.key.name.pi, &r.ack)
		}
		sess.mu.Unlock()
		return true
	}
}

// finishOwedIntents is the shutdown settlement phase, after workers have joined and
// before JetStream stops. Snapshotting does not detach ownership. Only a completed or
// transferred exact instance is removed; failed final attempts remain reachable.
func (s *Server) finishOwedIntents() {
	s.owedIntentsMu.Lock()
	var owed []*mqttIntentResolution
	for _, r := range s.owedIntents {
		if !r.running {
			r.running = true
			owed = append(owed, r)
		}
	}
	s.owedIntentsMu.Unlock()
	for _, r := range owed {
		r.finish(r.attempt(true))
	}
}
