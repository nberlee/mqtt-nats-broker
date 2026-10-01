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

import (
	"errors"
	"time"
)

// A retained replay this connection can no longer serve (F-105 7c).
//
// A retained PUBLISH is re-checked against the connection that will carry it BEFORE its output
// is acquired: an acquisition under this connection's tenure makes the delivery this connection's
// quota, and the current-connection veto then forbids abandoning it. Two refusals exist, the
// subscribe deny (acl-deny) and a frame larger than the client's Maximum Packet Size
// (packet-too-large; Spec5 [3.1.2.11.4]: discard it without sending and behave as if it had
// completed sending). Which disposition follows is decided by what the marker proves:
//
//   - PROVEN UNSENT (the allocation's unsent marker stands): withdrawn, WithdrawnUnsent. Nothing was
//     ever on the wire, so the identifier and the charge go with the exchange.
//   - POSSIBLY SENT (the marker is gone): ABANDONED, the retained arm of the abandonment fact. The
//     charge is released and the identifier held until a valid protocol completion or the
//     generation's discard, exactly as an ordinary abandoned delivery. No source is settled: a
//     retained delivery was never a source delivery.
//
// An INDETERMINATE deny verdict decides nothing: the exchange stays reserved and is replayed again
// at the next resume.
//
// Either disposition makes the copy's material releasable (the ended exchange, or the retained
// abandonment fact), and the reclaimer is woken for it.

// mqttRetainedReplayVerdict is what the carrying connection says about one retained PUBLISH.
type mqttRetainedReplayVerdict uint8

const (
	mqttRetainedReplayServable mqttRetainedReplayVerdict = iota + 1
	mqttRetainedReplayIndeterminate
	mqttRetainedReplayRefused
)

// retainedReplayVerdict judges one retained PUBLISH against this connection, before any output
// is acquired: the subscribe deny over the copy's topic first, then the frame as built against the
// Maximum Packet Size. A refusal carries its abandonment reason.
func (cc *client) retainedReplayVerdict(f mqttRetainedFrame, m *mqttRetainedManifest) (mqttRetainedReplayVerdict, string) {
	subj, err := mqttTopicToNATSPubSubject([]byte(m.Topic))
	if err != nil {
		return mqttRetainedReplayIndeterminate, _EMPTY_
	}
	// Retained replay never rides a shared subscription (Spec5 [4.8.2]), so there is no queue.
	switch cc.checkDenySubVerdict(string(subj), _EMPTY_) {
	case mqttSubDeliverAllow:
	case mqttSubDeliverDenied:
		return mqttRetainedReplayRefused, mqttAbandonReasonACLDeny
	default:
		return mqttRetainedReplayIndeterminate, _EMPTY_
	}
	// The DUP flag changes no length, so the frame's size is decided before the send section.
	if _, fits := cc.mqttPrepareRetainedPublish(f, m, false); !fits {
		return mqttRetainedReplayRefused, mqttAbandonReasonPacketTooLarge
	}
	return mqttRetainedReplayServable, _EMPTY_
}

// disposeUnservableRetained ends a retained PUBLISH this connection refused, by what its marker
// proves: withdrawn when proven unsent, abandoned when possibly sent. m is the admitted copy the
// allocation carries, whose payload the abandonment fact names.
//
// No lock held on entry.
func (sess *mqttSession) disposeUnservableRetained(c *client, pi uint16, nonce string, m *mqttRetainedManifest,
	reason string, background bool) {

	ent, entOK := c.outboundEntitlement()
	sess.mu.Lock()
	p := sess.pendingPublish[pi]
	if p == nil || p.kind != mqttPendingRetained || p.regNonce != nonce || p.sendNonce != _EMPTY_ {
		// Not this allocation's to dispose any more: whatever removed it, or the live section
		// over it, owns what remains.
		sess.mu.Unlock()
		return
	}
	inc, possiblySent := p.retainedInc, p.transmitted
	var ev mqttDenyEvidence
	if possiblySent && entOK && m != nil {
		ev = mqttDenyEvidence{Gen: sess.gen, Tenure: ent.Tenure, Subject: sess.inflightSubjectLocked(pi),
			Nonce: p.regNonce, Lin: p.ev.lin, PI: pi, Reason: reason, Payload: m.Payload}
	}
	sess.mu.Unlock()
	if !possiblySent {
		// Proven unsent: WITHDRAWN through the retained door, which keeps the allocation until the
		// withdrawal is confirmed and owns it until then.
		sess.withdrawRetainedUnsent(c, pi, nonce, background)
		return
	}
	if ev.Payload == _EMPTY_ {
		// No entitlement or no copy to name: nothing here can decide the abandonment.
		sess.oweRetainedClose(c, pi, nonce, "abandonment")
		return
	}
	sess.ownRetainedAbandonment(&mqttRetainedAbandonment{c: c, ev: ev, inc: inc})
}

// mqttRetainedAbandonment is one durable abandonment decision the retained driver owns until it
// stands and is applied to memory, or until its ownership is proven to have passed.
type mqttRetainedAbandonment struct {
	// c is the connection the decision was made on; past the bound it is closed.
	c   *client
	ev  mqttDenyEvidence
	inc string
	// closeRequested: c's close was requested once, past the bound.
	closeRequested bool
	// handles are the submitted attempts not yet settled. While any stands UNRESOLVED nothing is
	// re-submitted: a second attempt over an attempt that may have landed decides nothing new.
	handles []*mqttTransitionHandle
	// landed: an attempt is known committed; only the readback and the local application remain.
	landed  bool
	attempt int
	nextAt  time.Time
}

// mqttRetainedAbandonStep is what one step of an owned abandonment established.
type mqttRetainedAbandonStep uint8

const (
	// mqttRetainedAbandonRetry: not settled; the driver steps it again after its backoff.
	mqttRetainedAbandonRetry mqttRetainedAbandonStep = iota + 1
	// mqttRetainedAbandonSettled: the abandonment stands and memory carries it (the ACK-only
	// binding installed, the entry gone), or the exchange has moved past it.
	mqttRetainedAbandonSettled
	// mqttRetainedAbandonPassed: the decision is no longer this owner's — the captured
	// entitlement no longer serves, or this connection's own acquisition vetoes it. The next
	// resume decides again.
	mqttRetainedAbandonPassed
)

// stepRetainedAbandonment performs one step of one owned abandonment.
//
// THE ORDER IS THE CONTRACT. Outstanding attempts are settled first and an unresolved one blocks
// re-submission; then the register is read back, and an abandonment that stands is applied to
// memory; only a register still at its PUBLISH is decided again, and only while the captured
// entitlement is the serving one — a durable read, never this connection's liveness.
//
// No lock held on entry.
func (sess *mqttSession) stepRetainedAbandonment(a *mqttRetainedAbandonment) mqttRetainedAbandonStep {
	as, jsa := sess.asm, sess.jsa
	if as == nil || jsa == nil {
		return mqttRetainedAbandonRetry
	}
	if len(a.handles) > 0 {
		var keep []*mqttTransitionHandle
		for _, h := range a.handles {
			switch sess.settleCustodyHandle(h) {
			case mqttTransitionLanded, mqttTransitionSuperseded:
				a.landed = true
			case mqttTransitionNeverLanded:
			default:
				keep = append(keep, h)
			}
		}
		a.handles = keep
		if len(keep) > 0 && !a.landed {
			return mqttRetainedAbandonRetry
		}
		a.handles = nil
	}
	ev := a.ev
	// A QUARANTINED allocation is decided by nothing, so no attempt is even submitted (consult 107).
	sess.mu.Lock()
	quarantined := sess.retainedQuarantinedLocked(ev.Nonce)
	sess.mu.Unlock()
	if quarantined {
		return mqttRetainedAbandonPassed
	}
	rec, smsg, err := as.loadInflightRecord(ev.Subject)
	if err != nil {
		return mqttRetainedAbandonRetry // unreadable is not absent
	}
	mine := smsg != nil && rec != nil && rec.Nonce == ev.Nonce && rec.Lin == ev.Lin && rec.PI == ev.PI
	switch {
	case mine && rec.State == mqttInflightDeliveryAbandoned:
		if rec.Payload != ev.Payload || rec.AbandonReason != ev.Reason || rec.AbandonTenure != ev.Tenure {
			// Another decision's abandonment of this allocation: it is that decision's to apply.
			return mqttRetainedAbandonPassed
		}
		return sess.applyRetainedAbandonment(a, rec)
	case !mine || rec.State != mqttInflightPublish:
		// The exchange moved past this decision (an answer advanced or ended it, or the subject
		// holds a successor): its own path owns what remains.
		return mqttRetainedAbandonSettled
	case a.landed:
		// Committed, yet the register reads PUBLISH: a readback cannot disagree with a landed
		// abandonment for long; read again rather than decide again.
		return mqttRetainedAbandonRetry
	}
	auth := jsa.loadOutboundAuthority(sess.id, as.sessStreamEpoch)
	switch {
	case auth.status != mqttOutboundValid || auth.rec == nil:
		return mqttRetainedAbandonRetry
	case auth.rec.Gen != ev.Gen || auth.rec.Tenure != ev.Tenure || auth.rec.State == mqttAuthorityClosed:
		return mqttRetainedAbandonPassed // a successor's or no one's: positively not this decision's
	case auth.rec.State != mqttAuthorityServing:
		return mqttRetainedAbandonRetry // a preparation can be cancelled back to this tenure
	}
	acq, verr := sess.abandonCurrentOutputVeto(ev)
	if errors.Is(verr, errMQTTAbandonCurrentOutput) {
		return mqttRetainedAbandonPassed
	}
	if verr != nil {
		return mqttRetainedAbandonRetry
	}
	// ONE ADMITTED ATTEMPT, before the op's own reads (outbound_foreground_priority.go).
	if !sess.admitBackgroundAuthority() {
		return mqttRetainedAbandonRetry
	}
	out, serr := sess.submitAbandonDeniedDelivery(ev, acq)
	switch {
	case serr == nil:
		a.landed = true
	case out.Handle != nil:
		a.handles = append(a.handles, out.Handle)
	}
	return mqttRetainedAbandonRetry
}

// applyRetainedAbandonment applies a standing retained abandonment to memory: the ACK-only binding
// first, then the exact resident entry goes (settleAbandonedDeliveryLocked); the ledger releases
// the charge on the observation, and the copy's reclamation is woken. Settled only once memory
// carries the binding — a refused application is retried, never reported done.
//
// No lock held on entry.
func (sess *mqttSession) applyRetainedAbandonment(a *mqttRetainedAbandonment, rec *mqttInflightRecord) mqttRetainedAbandonStep {
	ev := a.ev
	sess.mu.Lock()
	if sess.gen != ev.Gen || sess.retainedQuarantinedLocked(ev.Nonce) {
		sess.mu.Unlock()
		// A successor incarnation restores it from the record; a quarantine authorizes nothing.
		return mqttRetainedAbandonPassed
	}
	applied := sess.settleAbandonedDeliveryLocked(ev, rec)
	sess.mu.Unlock()
	if !applied {
		return mqttRetainedAbandonRetry
	}
	sess.hintCustody(ev.Gen, ev.Lin)
	sess.wakeRetainedReclaim(ev.Gen, a.inc)
	return mqttRetainedAbandonSettled
}
