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
)

// Retiring a source consumer (F-105 7c-lc).
//
// A route that ends a subscription — an UNSUBSCRIBE, a QoS 0 downgrade, a rolled-back SUBSCRIBE,
// a drain's retirement, a failed provisioning, a resume repair's undo — no longer deletes the
// consumer. It decides, and hands the decision here. The consumer is a resource an admitted
// obligation may still hold, and its physical name is the thing that obligation's replay depends
// on, so the delete happens only after:
//
//  1. the DISPOSAL decision stands on the resource, written under the entitlement the route
//     decided with (opRequestDisposal; a decision is never re-made under a successor);
//  2. every hold on it has been released — by its obligation's ending in that ending's batch, or,
//     for an acknowledgement ending, by a release-holds batch (outbound_release_holds.go), which
//     this step composes itself for the ended holders it finds;
//  3. the deletion is AUTHORIZED, which retires the physical name in the same batch, so no
//     obligation can depend on it again and no replacement can ever claim it;
//  4. the delete is performed and its outcome CLASSIFIED, and recorded with the cleanup duty's
//     ending in one batch. An ambiguous delete records nothing and is re-attempted: the name is
//     retired and never reused, so a repeated delete cannot reach a successor.
//
// Each call is ONE bounded step, driven by the session's retirement owner (the same owner that
// reclaims retained copies), which charges and paces it. The owner stops when this session object
// is no longer resident; an unfinished retirement of a DISCARDED generation is then the recovery
// executor's, which acts from the resource record (outbound_discard_exec.go). One whose generation
// still serves — the route crashed, or its entitlement moved — is settled by the next accepted
// tenure of that generation (outbound_tenure_settle.go, F-105 7f).

// mqttSourceRetirement is one route's decision, in custody until the consumer is retired.
type mqttSourceRetirement struct {
	// gen and name identify the consumer; inc is its resource incarnation, empty until known.
	gen, name, inc string
	reason         string
	// stream is the stream whose name ownership names the consumer: the source stream when
	// empty. A PUBREL consumer's is $MQTT_out (F-105 7d); its only producer is its generation's
	// discard, which the recovery executor retires, so no live route sets it today.
	stream mqttOwnedStream
	// ent is the serving entitlement the route decided under.
	ent mqttOutboundEntitlement
	// txn names the disposal write, so a lost response is recognisable as this decision.
	txn string
	// claims are the claim attempts of a provisioning that failed or is ambiguous: each is settled
	// by permanent arbitration before anything is decided about the incarnation they claimed.
	claims []*mqttTransitionHandle
	// claimInc is the incarnation those attempts would have created.
	claimInc string
	// disposed is set once the decision is known to stand.
	disposed bool
	// warned suppresses a repeated report of a contradiction this owner withholds on.
	warned bool
}

// mqttSourceClaim is one SUBSCRIBE's claim of a fresh source consumer name: the incarnation it
// proposes, the entitlement it claimed under, and every attempt it submitted.
type mqttSourceClaim struct {
	gen, name, inc string
	ent            mqttOutboundEntitlement
	handles        []*mqttTransitionHandle
}

// custodyFailedClaim takes an exhausted or failed claim into custody: any of its attempts may
// have landed, so each is settled by arbitration and a landed incarnation is disposed of.
func (sess *mqttSession) custodyFailedClaim(cl *mqttSourceClaim) {
	if cl == nil || len(cl.handles) == 0 {
		return // nothing was submitted, so nothing can land
	}
	if h := sess.testSourceDisposalSite.Load(); h != nil {
		(*h)("claim-failed", cl.name)
	}
	sess.handSourceRetirement(&mqttSourceRetirement{gen: cl.gen, name: cl.name,
		reason: mqttDisposalProvisioningFailed, ent: cl.ent, claims: cl.handles, claimInc: cl.inc})
}

// handSourceRetirement takes a route's decision into the owner's custody and wakes it.
//
// No lock held on entry. Never blocks on JetStream: a route holding a record transaction may call
// it (deviation D1 — the first attempt is the owner's, outside any fence).
func (sess *mqttSession) handSourceRetirement(it *mqttSourceRetirement) {
	if it == nil || sess.asm == nil {
		return
	}
	if it.txn == _EMPTY_ {
		it.txn = mqttNewInflightNonce()
	}
	key := it.inc
	if key == _EMPTY_ {
		// The incarnation is learned from the name's ownership, or from the claims' arbitration:
		// keyed by the decision until then.
		key = "decision-" + it.txn
	}
	rc := &sess.retainedReclaim
	rc.mu.Lock()
	if rc.pending == nil {
		rc.pending = make(map[mqttRetainedReclaimKey]*mqttRetainedReclaimState)
	}
	k := mqttRetainedReclaimKey{gen: it.gen, inc: key, src: true, tenure: it.ent.Tenure}
	if st := rc.pending[k]; st == nil {
		rc.pending[k] = &mqttRetainedReclaimState{src: it}
	} else {
		st.withheld = 0
	}
	start := !rc.running
	rc.running = true
	rc.mu.Unlock()
	if start {
		go sess.runRetainedReclaim()
	}
}

// disposeSourceConsumer is what a route that ends a subscription does with its source consumer,
// in place of deleting it (F-105 7c-lc): the local charge is refunded as before, and the decision
// is handed to the retirement owner under the entitlement of the connection that decided. A route
// with no accepted entitlement has decided nothing this broker may record: the consumer is left,
// for the recovery executor (7d), and never deleted raw.
//
// No lock held on entry; safe under an open record transaction (it issues no request).
func (sess *mqttSession) disposeSourceConsumer(c *client, cc *ConsumerConfig, reason, site string) {
	if cc == nil {
		return
	}
	sess.mu.Lock()
	sess.refundConsumerChargeLocked(cc.Durable)
	gen := sess.gen
	sess.mu.Unlock()
	ent, _ := c.outboundEntitlement()
	sess.handSourceDisposal(gen, cc.Durable, _EMPTY_, reason, ent, site)
}

// handSourceDisposal hands one decision about a named consumer (and, where the route knows it,
// its incarnation) to the owner. The site names the route for the test seam.
func (sess *mqttSession) handSourceDisposal(gen, name, inc, reason string, ent mqttOutboundEntitlement, site string) {
	if h := sess.testSourceDisposalSite.Load(); h != nil {
		(*h)(site, name)
	}
	if sess.testSourceDisposalDrop.Load() {
		return
	}
	if ent.isZero() {
		if sess.jsa != nil && sess.jsa.c != nil {
			sess.jsa.c.Warnf("MQTT: consumer %q of %q was ended by %s with no accepted entitlement, so no "+
				"disposal decision is recorded: it is left for recovery and not deleted", name, sess.id, site)
		}
		return
	}
	sess.handSourceRetirement(&mqttSourceRetirement{gen: gen, name: name, inc: inc, reason: reason, ent: ent})
}

// wakeSourceRetirements resets the backoff of every source retirement in custody: an ending that
// released a hold is the event their withheld step was waiting for. A hint; each step re-reads.
func (sess *mqttSession) wakeSourceRetirements() {
	rc := &sess.retainedReclaim
	rc.mu.Lock()
	for k, st := range rc.pending {
		if k.src {
			st.withheld = 0
		}
	}
	rc.mu.Unlock()
}

// retireSourceStep performs one step of one source consumer's retirement.
//
// No lock held on entry.
func (sess *mqttSession) retireSourceStep(it *mqttSourceRetirement) (mqttReclaimStep, error) {
	if it == nil || sess.asm == nil {
		return mqttReclaimDone, nil
	}
	jsa := sess.jsa
	id, epoch := sess.id, sess.asm.sessStreamEpoch

	// (0) A PROVISIONING THAT FAILED OR IS AMBIGUOUS: every submitted claim is settled first, by
	// permanent arbitration. An absent ownership is never the verdict — a queued claim can land
	// after any negative read; the attempt's own outcome subject is the only thing that can end it.
	if len(it.claims) > 0 {
		landed := false
		var open []*mqttTransitionHandle
		for _, h := range it.claims {
			switch jsa.resolveTransitionHandle(h) {
			case mqttTransitionLanded, mqttTransitionSuperseded:
				landed = true
			case mqttTransitionNeverLanded:
			default:
				// Abort it: the ABORTED outcome takes the attempt's create-only outcome subject,
				// after which it can never land. A lost abort is re-resolved on the next step.
				if aerr := jsa.abortTransition(&h.attempt); aerr != nil {
					open = append(open, h)
				}
			}
		}
		if len(open) > 0 {
			it.claims = open
			if landed {
				it.inc = it.claimInc
			}
			return mqttReclaimWithheld, fmt.Errorf("claim of %q not yet settled", it.name)
		}
		it.claims = nil
		if !landed && it.inc == _EMPTY_ {
			// NOTHING LANDED, positively: no claim, no resource, no consumer this broker made.
			return mqttReclaimDone, nil
		}
		it.inc = it.claimInc
		return mqttReclaimProgress, nil
	}

	// (1) WHICH INCARNATION. A route names a consumer; its resource is the name's owner.
	if it.inc == _EMPTY_ {
		stream := it.stream
		if stream == _EMPTY_ {
			stream = mqttOwnedStreamMsgs
		}
		own, _, oerr := jsa.loadNameOwnershipPhysical(epoch, stream, it.name)
		if oerr != nil {
			return mqttReclaimWithheld, fmt.Errorf("the ownership of %q could not be read: %w", it.name, oerr)
		}
		if own == nil || own.ID != id || own.OwnerGen != it.gen {
			// NO OWNER OF OURS: the delete is withheld, never performed raw. Production claims
			// every name before its CREATE, so this is a population it does not write.
			if !it.warned && sess.jsa.c != nil {
				sess.jsa.c.Warnf("MQTT: not deleting consumer %q of %q: no name-ownership record of this "+
					"generation stands for it, and a missing owner is never permission", it.name, id)
			}
			return mqttReclaimDone, nil
		}
		it.inc = own.OwnerInc
		return mqttReclaimProgress, nil
	}

	// (2) THE DECISION. Idempotent: a lost response, or another route's decision, is found standing.
	if !it.disposed {
		_, derr := jsa.opRequestDisposal(&mqttRequestDisposalRequest{
			ID: id, Epoch: epoch, Txn: it.txn, Gen: it.gen, Inc: it.inc,
			Reason: it.reason, Entitlement: it.ent,
		})
		switch {
		case derr == nil:
			it.disposed = true
			return mqttReclaimProgress, nil
		case errors.Is(derr, errMQTTDisposalEntitlementPreparing):
			// A HANDOFF IS PREPARING over this decision's tenure (F-105 7f): withheld, not dropped. A
			// cancellation restores the tenure and the decision lands; a finalization moves it, and
			// the next attempt drops the item to the successor's settlement.
			return mqttReclaimWithheld, derr
		case errors.Is(derr, errMQTTDisposalEntitlementMoved):
			// THE ENTITLEMENT MOVED ON: the decision is never re-made under the successor. The
			// successor's own settlement decides the consumer under ITS entitlement if nothing it
			// holds names it (F-105 7f), and a discarded generation's recovery executor does (7d).
			if sess.jsa.c != nil {
				sess.jsa.c.Debugf("MQTT: disposal of consumer %q of %q dropped: %v", it.name, id, derr)
			}
			return mqttReclaimDone, nil
		default:
			return mqttReclaimWithheld, derr
		}
	}

	r, rSeq, rerr := jsa.loadOutboundResource(id, epoch, it.gen, it.inc)
	if rerr != nil || r == nil {
		return mqttReclaimWithheld, fmt.Errorf("resource %q of %q could not be read: %v", it.inc, it.name, rerr)
	}
	switch r.State {
	case mqttResourceLive:
		if r.Disposal == nil {
			// Read before our own decision's write was visible; re-attempted, idempotently.
			it.disposed = false
			return mqttReclaimWithheld, fmt.Errorf("the disposal of %q is not visible yet", it.name)
		}
		if len(r.Deps) > 0 {
			// HELD. A hold whose obligation already ENDED is RELEASE-OWED (an acknowledgement
			// ending's, outbound_release_holds.go): the retirement waits on exactly those edges, so
			// it releases them itself rather than waiting for a releaser that may not run here.
			var owed []string
			for _, d := range r.Deps {
				if !mqttHoldRole(d.Role) || len(owed) == mqttHoldReleaseBatch {
					continue
				}
				x, _, xerr := jsa.loadOutboundExchange(id, epoch, r.Gen, d.Nonce)
				if xerr == nil && x != nil && mqttExchangeReleasesHolds(x.Phase) {
					owed = append(owed, d.Nonce)
				}
			}
			if len(owed) > 0 {
				out, rerr := jsa.opReleaseHolds(id, epoch, r.Gen, owed, nil, mqttNewInflightNonce(), nil)
				if rerr != nil {
					return mqttReclaimWithheld, fmt.Errorf("releasing %d ended obligation(s)' holds on %q: %w",
						len(owed), it.name, rerr)
				}
				if len(out.cleared) > 0 {
					return mqttReclaimProgress, nil
				}
			}
			return mqttReclaimWithheld, fmt.Errorf("%d obligation(s) still hold %q", len(r.Deps), it.name)
		}
		txn := mqttNewInflightNonce()
		if _, aerr := jsa.opAuthorizeDeletion(id, epoch, txn, "op-retire-source", it.gen, it.inc); aerr != nil {
			return mqttReclaimWithheld, aerr
		}
		return mqttReclaimProgress, nil

	case mqttResourceDeleteAuthorized:
		stream := mqttStreamName
		if r.Kind == mqttResourcePubRelConsumer {
			stream = mqttOutStreamName
		}
		outcome, ok := sess.deleteRetiredConsumer(stream, r.Name)
		if !ok {
			// AN AMBIGUOUS DELETE RECORDS NOTHING: the duty stays open and the delete is repeated.
			return mqttReclaimWithheld, fmt.Errorf("the delete of %q was unresolved", r.Name)
		}
		ord, oerr := jsa.resourceDutyOrdinal(id, epoch, it.gen, it.inc, mqttDutyCleanup)
		if oerr != nil {
			return mqttReclaimWithheld, oerr
		}
		d, _, derr := jsa.loadDutyEntryAt(id, epoch, it.gen, ord)
		if derr != nil || d == nil {
			return mqttReclaimWithheld, fmt.Errorf("cleanup duty %d of %q could not be read: %v", ord, it.inc, derr)
		}
		root := &mqttRecoveryRoot{ID: id, Epoch: epoch, Gen: it.gen}
		if cerr := jsa.completeCleanupAndResolve(root, r, rSeq, mqttSweptDuty{Ord: ord, Entry: d},
			mqttNewInflightNonce(), outcome); cerr != nil {
			return mqttReclaimWithheld, cerr
		}
		return mqttReclaimProgress, nil

	case mqttResourceCleanupComplete:
		// Both duties end: the cleanup's (completeCleanupAndResolve did it) and the provisioning's.
		return jsa.resolveRetainedPayloadDuties(id, epoch, it.gen, it.inc)
	}
	return mqttReclaimWithheld, fmt.Errorf("resource %q stands in a state this retirement does not continue", it.inc)
}

// deleteRetiredConsumer performs the physical delete of a RETIRED consumer name and classifies its
// own result, exactly as the affine executor does: removed, already gone, or unresolved (ok false,
// which records nothing). A test seam may supply the observed outcome.
func (sess *mqttSession) deleteRetiredConsumer(stream, name string) (mqttCleanupOutcome, bool) {
	var err error
	if h := sess.testRetiredConsumerDelete.Load(); h != nil {
		err = (*h)(stream, name)
	} else {
		_, err = sess.jsa.deleteConsumer(stream, name, false)
	}
	switch {
	case err == nil:
		return mqttCleanupRemoved, true
	case !isErrorOtherThan(err, JSConsumerNotFoundErr):
		return mqttCleanupGone, true
	}
	return _EMPTY_, false
}
