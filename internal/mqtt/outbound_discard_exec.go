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
	"fmt"
	"sync/atomic"
)

// A discarded generation's retirement, as the recovery executor performs it (F-105 7d).
//
// A teardown closes its generation DISCARDING and transfers what it held to the generation's
// recovery root. From there ONE executor — the recovery pass, and the prompt drive that calls the
// same visit (outbound_recovery_kick.go) — retires it, one CAS'd transition or one classified
// physical act per step, re-classifying from the records on the next visit:
//
//	exchange, holds not released      -> discard-exchange: Discarded, holds released in the batch
//	exchange, rel or own register     -> delete them, the rel FIRST (the register carries the rel's
//	                                     identity proof), confirmed, at exact sequences
//	source / PUBREL consumer, no decision -> the generation-discarded disposal (closure-licensed)
//	disposed, still held              -> waits: its holders are ended through their own duties
//	disposed, nothing holds it        -> authorize deletion, which retires the name
//	deletion authorized               -> delete, classify, record the outcome and end the duty
//
// The DISCARD CENSUS is what makes the exchange half complete. A delivery duty may already be
// resolved while its obligation still owes discard work — an abandoned delivery's duty resolves
// while it holds, a shared one holds nothing, an ended retained one may still have its rel — so for
// a discarded root the sweep also returns RESOLVED exchange duties, and the visit counts one as
// unresolved exactly while its work remains. No terminal duty is reopened and nothing is allocated
// after the seal: what changes is only which ordinals the root may certify past. The census is
// COMPLETE because the write boundary admits no obligation without a duty entry naming it in its own
// generation (checkInventoryMembers' coverage rule), so no second, resource-side route is needed.

// classifyDiscardedExchangeDuty classifies an exchange duty — outstanding, or resolved and swept by
// the census — of a generation whose committed closure discards.
//
// For a resolved duty, MayResolve means "no discard work remains" and nothing is performed.
func (jsa *mqttJSA) classifyDiscardedExchangeDuty(id, epoch, gen string, d *mqttDutyEntry) mqttDutyVerdict {
	x, _, err := jsa.loadOutboundExchange(id, epoch, gen, d.Ref)
	if err != nil || x == nil {
		return mqttDutyVerdict{
			Disposition: mqttDutyWithheld,
			Why: fmt.Sprintf("obligation %q of a discarded generation could not be read (%v), and "+
				"what this broker cannot read it may neither end nor call finished", d.Ref, err),
		}
	}
	if !mqttExchangeReleasesHolds(x.Phase) {
		return mqttDutyVerdict{
			Disposition: mqttDutyOwesWork, Work: mqttDutyWorkDiscardEnd,
			Why: fmt.Sprintf("the obligation stands at phase %d in a discarded generation: nothing "+
				"will deliver, release or drain it, so its discard is owed", int(x.Phase)),
		}
	}
	st, serr := jsa.loadDiscardCleanup(id, epoch, gen, x)
	if serr != nil {
		return mqttDutyVerdict{
			Disposition: mqttDutyWithheld,
			Why:         fmt.Sprintf("the records of discarded obligation %q could not be read: %v", d.Ref, serr),
		}
	}
	if st.relSeq != 0 || st.regSeq != 0 {
		return mqttDutyVerdict{
			Disposition: mqttDutyOwesWork, Work: mqttDutyWorkDiscardCleanup,
			Why: "the obligation released its holds, but its canonical PUBREL or its register still stands",
		}
	}
	return mqttDutyVerdict{
		Disposition: mqttDutyMayResolve,
		Why:         "the obligation released its holds and its records are gone",
	}
}

// mqttDiscardCleanupState is what of a discarded obligation's own records still stands.
type mqttDiscardCleanupState struct {
	rel    *mqttRetainedRel
	relSeq uint64
	reg    *mqttInflightRecord
	regSeq uint64
}

// loadDiscardCleanup reads an obligation's canonical PUBREL and the register at its identifier.
//
// The register counts only when it NAMES this obligation (its lineage is the exchange's nonce): an
// identifier is allocation, not identity, and another obligation's register at the same identifier
// is not this one's to delete. Anything unreadable, or a record this broker cannot interpret, is an
// error: it may be the one that still needs deleting.
func (jsa *mqttJSA) loadDiscardCleanup(id, epoch, gen string, x *mqttOutboundExchange) (mqttDiscardCleanupState, error) {
	var st mqttDiscardCleanupState
	idHash := getHash(id)
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, mqttRetainedRelSubject(idHash, gen, x.Nonce))
	switch {
	case err != nil && isErrorOtherThan(err, JSNoMessageFoundErr):
		return st, fmt.Errorf("its canonical PUBREL could not be read: %w", err)
	case err == nil && smsg != nil:
		rel, derr := mqttDecodeRetainedRel(smsg.Data, id, epoch, gen, x.Nonce)
		if derr != nil {
			return st, fmt.Errorf("its canonical PUBREL is not a record this broker writes: %w", derr)
		}
		st.rel, st.relSeq = rel, smsg.Sequence
	}
	reg, rmsg, rerr := jsa.loadInflightRecord(mqttInflightSubject(idHash, gen, x.PI))
	switch {
	case rerr != nil:
		return st, fmt.Errorf("the register at identifier %d could not be read: %w", x.PI, rerr)
	case reg == nil && rmsg != nil:
		return st, fmt.Errorf("the register at identifier %d is not a record this broker writes", x.PI)
	case reg != nil && reg.Lin == x.Nonce:
		st.reg, st.regSeq = reg, rmsg.Sequence
	}
	return st, nil
}

// performDiscardWork performs ONE step of a discarded generation's retirement.
//
// It reports whether a durable act was performed (a committed transition or a classified delete),
// which is what the prompt drive and the tests count; every step re-reads what it acts on.
func (as *mqttAccountSessionManager) performDiscardWork(s *Server, root *mqttRecoveryRoot,
	d mqttSweptDuty, v mqttDutyVerdict, b *mqttRecoveryBudget) {

	jsa := &as.jsa
	switch v.Work {
	case mqttDutyWorkDiscardEnd:
		if !b.spend(2) {
			return
		}
		as.discardOneObligation(s, root, d.Entry.Ref)

	case mqttDutyWorkDiscardCleanup:
		if !b.spend(3) {
			return
		}
		x, _, xerr := jsa.loadOutboundExchange(root.ID, root.Epoch, root.Gen, d.Entry.Ref)
		if xerr != nil || x == nil || !mqttExchangeReleasesHolds(x.Phase) {
			return
		}
		st, serr := jsa.loadDiscardCleanup(root.ID, root.Epoch, root.Gen, x)
		if serr != nil {
			return
		}
		switch {
		case st.relSeq != 0:
			// THE REL FIRST, read back as THIS obligation's: its identity is proven against the
			// register's when that still stands, which is why the register outlives it.
			if st.rel.PI != x.PI || (st.reg != nil && st.reg.RelID != _EMPTY_ && st.reg.RelID != st.rel.Identity) {
				s.Warnf("MQTT recovery pass: the canonical PUBREL of discarded obligation %q/%q/%q is "+
					"not the one its register names; it is left standing", root.ID, root.Gen, x.Nonce)
				return
			}
			if derr := as.deleteDiscardRecord(st.relSeq); derr != nil {
				s.Debugf("MQTT recovery pass: deleting the canonical PUBREL of %q/%q/%q: %v",
					root.ID, root.Gen, x.Nonce, derr)
			}
		case st.regSeq != 0:
			if derr := as.deleteDiscardRecord(st.regSeq); derr != nil {
				s.Debugf("MQTT recovery pass: deleting the register of %q/%q/%q: %v",
					root.ID, root.Gen, x.Nonce, derr)
			}
		}

	case mqttDutyWorkDispose:
		if !b.spend(2) {
			return
		}
		r, _, rerr := jsa.loadOutboundResource(root.ID, root.Epoch, root.Gen, d.Entry.Ref)
		if rerr != nil || r == nil {
			return
		}
		if r.Kind == mqttResourcePubRelConsumer {
			// THE PUBREC PROOFS BEFORE THE PUBREL CONSUMER. Its records go with it ($MQTT_out is
			// interest-retained), and an affine assignment may have nowhere else recording that the
			// client answered PUBREC. Where the generation enrolled an affine consumer, its
			// readiness — which asserts those proofs secured — must stand first.
			if ok, why := as.pubRelDisposalPrerequisites(root); !ok {
				s.Debugf("MQTT recovery pass: the PUBREL consumer of %q/%q is withheld: %s", root.ID, root.Gen, why)
				return
			}
		}
		txn := mqttNewInflightNonce()
		if _, err := jsa.opDisposeDiscarded(root.ID, root.Epoch, txn, root.Gen, r.Inc); err != nil {
			s.Debugf("MQTT recovery pass: recording the discard of %q/%q/%q: %v", root.ID, root.Gen, r.Inc, err)
		}
	}
}

// discardOneObligation ends one obligation of the root's discarded generation.
func (as *mqttAccountSessionManager) discardOneObligation(s *Server, root *mqttRecoveryRoot, nonce string) {
	txn := mqttNewInflightNonce()
	_, err := as.jsa.opDiscardExchange(root.ID, root.Epoch, txn, root.Gen, nonce)
	if err != nil && !errors.Is(err, errMQTTExchangeAlreadyReleased) {
		s.Debugf("MQTT recovery pass: discarding obligation %q of %q/%q: %v", nonce, root.ID, root.Gen, err)
	}
}

// deleteDiscardRecord deletes one record of a discarded obligation at its exact sequence, confirmed.
// The next visit re-reads: a deleted sequence is not proof that its subject is now empty.
func (as *mqttAccountSessionManager) deleteDiscardRecord(seq uint64) error {
	return as.jsa.deleteMsg(mqttInflightStreamName, seq, true)
}

// pubRelDisposalPrerequisites reports whether a discarded generation's PUBREL consumer may be
// disposed of: it may unless the generation enrolled an affine consumer whose readiness — the
// assertion that the PUBREC proofs its PUBREL records carry are secured — does not stand yet.
func (as *mqttAccountSessionManager) pubRelDisposalPrerequisites(root *mqttRecoveryRoot) (bool, string) {
	res, unreadable, err := as.enumerateGenerationResources(root.ID, root.Epoch, root.Gen)
	if err != nil || unreadable > 0 {
		return false, fmt.Sprintf("the generation's resources could not be enumerated (%v, %d unreadable)", err, unreadable)
	}
	affine := false
	for i := range res {
		if res[i].Kind == mqttResourceAffineConsumer {
			affine = true
			break
		}
	}
	if !affine {
		return true, _EMPTY_
	}
	ready, lerr := as.jsa.loadAffineReadiness(root.ID, root.Epoch, root.Gen)
	if lerr != nil {
		return false, fmt.Sprintf("the affine readiness could not be read: %v", lerr)
	}
	if ready == nil {
		return false, "the affine readiness does not stand yet"
	}
	return true, _EMPTY_
}

// mqttTestRecoveryConsumerDeleteHookV, when set, replaces the executor's physical consumer delete,
// so a test can observe the resource at the moment of the delete or supply its outcome.
var mqttTestRecoveryConsumerDeleteHookV atomic.Pointer[func(stream, name string) error]

// deleteRecoveryConsumer performs the physical delete of a deletion-authorized consumer and
// classifies its OWN result: removed, already gone, or unresolved (ok false, which records
// nothing). `replaced` is never produced: it would claim another incarnation holds the name, which
// this executor does not observe — and an authorized name is RETIRED, so none can.
func (as *mqttAccountSessionManager) deleteRecoveryConsumer(s *Server, stream string,
	r *mqttOutboundResource) (mqttCleanupOutcome, bool) {

	var err error
	if h := mqttTestRecoveryConsumerDeleteHookV.Load(); h != nil {
		err = (*h)(stream, r.Name)
	} else {
		_, err = as.jsa.deleteConsumer(stream, r.Name, false)
	}
	switch {
	case err == nil:
		return mqttCleanupRemoved, true
	case !isErrorOtherThan(err, JSConsumerNotFoundErr):
		// ALREADY ABSENT WHEN ACTED ON. A positive answer, not an assumption.
		return mqttCleanupGone, true
	default:
		s.Debugf("MQTT recovery pass: deleting consumer %q of %q/%q was unresolved: %v",
			r.Name, r.ID, r.Gen, err)
		return _EMPTY_, false
	}
}

// mqttResourceConsumerStream is the stream a consumer resource's durable lives on.
func mqttResourceConsumerStream(k mqttResourceKind) (string, bool) {
	switch k {
	case mqttResourceSourceConsumer:
		return mqttStreamName, true
	case mqttResourcePubRelConsumer, mqttResourceAffineConsumer:
		return mqttOutStreamName, true
	}
	return _EMPTY_, false
}
