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
	"sync/atomic"
)

// What a recovery pass may DO about an outstanding duty (F-105, row 6).
//
// # The permitted operations are NAMED, not inferred
//
// classifyOutstandingDuty says what a duty owes. It says nothing about whether THIS actor may do
// it, and a future executor that read a verdict as permission would still be refused by the act it
// attempted — but "would be refused" is not a design. So the set this pass performs is written
// down, and everything else is withheld by a stated rule:
//
//	authorize-deletion, for an AFFINE resource   -> performed
//	cleanup, for an AFFINE resource              -> performed: the physical delete, then the
//	                                                completion and the duty's resolution in ONE batch
//	may-resolve                                  -> performed: the entry is continued to terminal
//	the affine readiness work, for a DISCARDING
//	closed generation                            -> performed, under the pass's budget
//	the provisioning SEAL of a ready affine
//	resource                                     -> performed
//	provision / cleanup, for a RETAINED PAYLOAD  -> performed, BEFORE classification: one step of
//	                                                the copy's reclamation (F-105 7c,
//	                                                outbound_retained_reclaim.go), whose boundary
//	                                                arms decide whether the copy may close
//	authorize-deletion / cleanup, a SOURCE or
//	PUBREL consumer of a DISCARDED generation    -> performed (F-105 7d); in a live generation their
//	                                                retirement is the session's
//	                                                (outbound_source_retire.go). Shared group
//	                                                durables are no resource kind at all
//	                                                (shared_lifecycle.go)
//	discard-end / discard-cleanup / dispose,
//	a DISCARDED generation                       -> performed (F-105 7d, outbound_discard_exec.go),
//	                                                each licensed by the committed discarding closure
//	deliver, release                             -> WITHHELD: both need an action acquired under a
//	                                                SERVING authority, which this pass holds none of;
//	                                                in a discarded generation they are not owed
//	drain                                        -> WITHHELD by rule. Acquisition fences delivery
//	                                                and release; it does not by itself prove a
//	                                                background drain impossible, so this is stated
//	                                                rather than inherited
//	provision                                    -> WITHHELD: the materialization half is not built
//
// # What this pass may act under
//
// A retired generation's disposal runs under the CLOSED authority the client still carries:
// mqttTransitionDisposeResource is permitted under any authority state, and a resource member may
// name an EARLIER generation, which is the whole reason that kind exists. Nothing deletes the
// authority record, so a restarted broker still finds it — and an ABSENT one WITHHOLDS every duty
// rather than licensing anything, because an absence is not permission.

// executeRecoveryDuty acts on one swept duty, within the pass's budget.
//
// discarded says the root's generation's committed closure discards, read once by the visit
// (F-105 7d). For a CENSUS entry — a resolved exchange duty of a discarded generation — it reports
// whether discard work remained when the entry was classified, which is what keeps the root from
// certifying past it; for every other entry the result is unused (it is unresolved by being swept).
func (as *mqttAccountSessionManager) executeRecoveryDuty(s *Server, root *mqttRecoveryRoot,
	d mqttSweptDuty, b *mqttRecoveryBudget, stop func() bool, discarded bool) (remains bool) {

	if d.Entry == nil {
		// A MISSING OR MALFORMED ENTRY IS AN INCONSISTENCY, never completed work. It is already
		// in this sweep's accumulator; there is nothing to act on.
		return true
	}
	// THE KIND IS EXCLUDED BEFORE THE CLASSIFIER RUNS, not after it: one record read decides
	// whether this pass may act on the duty at all, so no classification is performed for a
	// duty whose verdict would be thrown away.
	if !b.spend(1) {
		return true
	}
	if !d.Census && as.dispatchRetainedPayloadDuty(s, root, d, b) {
		return true
	}
	if d.Entry.Kind == mqttDutyCleanup && !as.transferredCleanupKind(root, d.Entry.Ref, discarded) {
		return true
	}
	// A discarded generation's exchange is classified with its canonical PUBREL and register too.
	cost := 2
	if discarded && d.Entry.Kind == mqttDutyExchange {
		cost = 4
	}
	if !b.spend(cost) {
		return true
	}
	v := as.jsa.classifyOutstandingDutyIn(root.ID, root.Epoch, root.Gen, discarded, d.Entry)
	if d.Census {
		// A RESOLVED DUTY IS NEVER RESOLVED AGAIN. MayResolve here means the obligation owes no
		// discard work, which is the whole of what the census asks.
		if v.Disposition != mqttDutyOwesWork {
			return v.Disposition != mqttDutyMayResolve
		}
		if as.recoveryActorIsAuthorized(s, root, "performing discard work on") {
			as.performRecoveryWork(s, root, d, v, b)
		}
		return true
	}
	switch v.Disposition {
	case mqttDutyMayResolve:
		as.resolveRecoveryDuty(s, root, d, b)
	case mqttDutyOwesWork:
		if !as.recoveryActorIsAuthorized(s, root, "performing work on") {
			return true
		}
		as.performRecoveryWork(s, root, d, v, b)
	default:
		// WITHHELD. The duty stays outstanding and the sweep comes back for it, which is the
		// honest answer for conditional work that is not yet possible and for anything this
		// broker could not read.
		//
		// A cleanup duty withheld on an UNSEALED affine resource is the case this pass can
		// advance rather than merely re-read: the readiness work, and then the seal. That work
		// MUTATES — it secures proofs, marks assignments discarded and may reap them — so it
		// needs the same authority the acting branches do.
		if !as.recoveryActorIsAuthorized(s, root, "advancing the prerequisites of") {
			return true
		}
		as.advanceAffinePrerequisites(s, root, d, b, stop)
	}
	return true
}

// dispatchRetainedPayloadDuty performs one step of a retained copy's reclamation for either of its
// duties, and reports whether the duty was a retained payload's (F-105 7c).
//
// BEFORE CLASSIFICATION, because the classifier's answers for this kind are the wrong question: it
// withholds a cleanup while the owner still carries its retained-material edge and a provision while
// it is unsealed, and releasing that edge and sealing are exactly the reclamation's own steps. What
// may happen is the reclamation's boundary arms' to decide — the close admits only a copy never
// admitted (under the tenure fence) or one whose exchange ended, carries the retained abandonment
// fact, or whose generation was discarded — so a pass acting on a copy that is still needed is
// refused at the write, never by a read here.
func (as *mqttAccountSessionManager) dispatchRetainedPayloadDuty(s *Server, root *mqttRecoveryRoot,
	d mqttSweptDuty, b *mqttRecoveryBudget) bool {

	if d.Entry.Kind != mqttDutyProvision && d.Entry.Kind != mqttDutyCleanup {
		return false
	}
	r, _, rerr := as.jsa.loadOutboundResource(root.ID, root.Epoch, root.Gen, d.Entry.Ref)
	if rerr != nil || r == nil || r.Kind != mqttResourceRetainedPayload {
		// Unreadable falls to the classifier, which withholds what it cannot read.
		return false
	}
	if !as.recoveryActorIsAuthorized(s, root, "reclaiming a retained copy of") || !b.spend(2) {
		return true
	}
	if _, err := as.jsa.reclaimRetainedPayloadStep(as, root.ID, root.Epoch, root.Gen, r.Inc); err != nil {
		s.Debugf("MQTT recovery pass: reclaiming retained copy %q of %q/%q: %v", r.Inc, root.ID, root.Gen, err)
	}
	return true
}

// performRecoveryWork executes the work kinds this pass is permitted to perform.
func (as *mqttAccountSessionManager) performRecoveryWork(s *Server, root *mqttRecoveryRoot,
	d mqttSweptDuty, v mqttDutyVerdict, b *mqttRecoveryBudget) {

	switch v.Work {
	case mqttDutyWorkAuthorizeDeletion, mqttDutyWorkCleanup:
	case mqttDutyWorkDiscardEnd, mqttDutyWorkDiscardCleanup, mqttDutyWorkDispose:
		// A DISCARDED generation's retirement (F-105 7d), each licensed by its closure.
		as.performDiscardWork(s, root, d, v, b)
		return
	default:
		// deliver, release, drain and provision: WITHHELD by the rule above.
		return
	}
	if !b.spend(1) {
		return
	}
	r, rseq, rerr := as.jsa.loadOutboundResource(root.ID, root.Epoch, root.Gen, d.Entry.Ref)
	if rerr != nil || r == nil {
		// A resource this broker cannot read is one it may neither act on nor call finished.
		return
	}
	stream, isConsumer := mqttResourceConsumerStream(r.Kind)
	if !isConsumer || (r.Kind != mqttResourceAffineConsumer && !mqttResourceLogicalUseKind(r.Kind)) {
		// THE TRANSFERRED CONSUMER KINDS: the affine one, and — in a discarded generation, the only
		// place their cleanup duty classifies as owed work without a live route's decision — the
		// source and PUBREL consumers (F-105 7d). Shared group durables are not a resource kind at
		// all; they are released by arbitration on their own group record (shared_lifecycle_ops.go).
		return
	}

	if v.Work == mqttDutyWorkAuthorizeDeletion {
		if !b.spend(2) {
			return
		}
		txn := mqttNewInflightNonce()
		if _, aerr := as.jsa.opAuthorizeDeletion(root.ID, root.Epoch, txn, "op-"+txn,
			root.Gen, r.Inc); aerr != nil {
			s.Debugf("MQTT recovery pass: authorizing the cleanup of %q/%q/%q: %v",
				root.ID, root.Gen, r.Inc, aerr)
		}
		return
	}

	// CLEANUP. Deletion is authorized and the classified outcome is not recorded.
	if !b.spend(3) {
		return
	}
	outcome, ok := as.deleteRecoveryConsumer(s, stream, r)
	if !ok {
		// AN AMBIGUOUS DELETE RECORDS NOTHING. The duty stays DELETE_AUTHORIZED and the next pass
		// re-acts, which is safe here for a reason worth stating rather than assuming: the affine
		// durable's name carries the AFFINE generation, and a source or PUBREL name was RETIRED by
		// the authorization itself, so a late or duplicated delete names a consumer no live session
		// can hold — a fresh one is never given a retired name.
		return
	}
	txn := mqttNewInflightNonce()
	if cerr := as.jsa.completeCleanupAndResolve(root, r, rseq, d, txn, outcome); cerr != nil {
		s.Debugf("MQTT recovery pass: completing the cleanup of %q/%q/%q: %v",
			root.ID, root.Gen, r.Inc, cerr)
	}
}

// resolveRecoveryDuty continues a duty entry to terminal.
//
// COMPLETING A RESOURCE DOES NOT COMPLETE ITS INVENTORY ENTRY, and this is the act that does. The
// boundary re-reads the standing entry and judges the ending against the records — a duty whose
// thing has not ended is refused here whatever this pass believes.
func (as *mqttAccountSessionManager) resolveRecoveryDuty(s *Server, root *mqttRecoveryRoot,
	d mqttSweptDuty, b *mqttRecoveryBudget) {

	if !b.spend(2) {
		return
	}
	txn := mqttNewInflightNonce()
	if err := as.jsa.opResolveDuty(root, d, txn); err != nil {
		s.Debugf("MQTT recovery pass: resolving duty %d of %q/%q: %v", d.Ord, root.ID, root.Gen, err)
	}
}

// advanceAffinePrerequisites moves a WITHHELD affine cleanup one step closer to actionable.
//
// A cleanup duty for an affine resource is withheld while its provisioning is PENDING — "a delayed
// create may still land under its name" — and the seal is what ends that. The seal needs the
// readiness assertion, so this is where the two meet:
//
//	readiness stands, provisioning PENDING -> SEAL it. This is the finisher for the window a
//	                                          teardown leaves when it crashes after writing
//	                                          readiness, and for the one where no teardown runs at
//	                                          all after the readiness work has been done.
//	no readiness                           -> the pass performs the readiness work itself
//	                                          (performAffineReadiness: securing the PUBREC proofs
//	                                          and establishing the assignments' discard
//	                                          disposition, under its one readiness attempt); it is
//	                                          NOT inferred from the resource alone.
func (as *mqttAccountSessionManager) advanceAffinePrerequisites(s *Server, root *mqttRecoveryRoot,
	d mqttSweptDuty, b *mqttRecoveryBudget, stop func() bool) {

	if d.Entry == nil || d.Entry.Kind != mqttDutyCleanup {
		return
	}
	if !b.spend(1) {
		return
	}
	r, rseq, rerr := as.jsa.loadOutboundResource(root.ID, root.Epoch, root.Gen, d.Entry.Ref)
	if rerr != nil || r == nil {
		return
	}
	if r.Kind != mqttResourceAffineConsumer || r.State != mqttResourceLive ||
		r.Prov != mqttProvisioningPending || len(r.Deps) > 0 {
		return
	}
	if !b.spend(1) {
		return
	}
	ready, lerr := as.jsa.loadAffineReadiness(root.ID, root.Epoch, root.Gen)
	if lerr != nil {
		// UNREADABLE IS NOT ABSENT. A readiness record this broker cannot interpret may be the
		// one whose prerequisites license this seal.
		s.Debugf("MQTT recovery pass: the affine readiness of %q/%q could not be read: %v",
			root.ID, root.Gen, lerr)
		return
	}
	if ready == nil {
		// NO ASSERTION YET, so the pass does the work itself. This is the finisher for the
		// window a closure leaves when no teardown ever runs for that generation — a discarding
		// finalization replaces the predecessor's metadata, so there is no marked record for the
		// teardown sweep to adopt, and "the successor's acceptance will do it" names a live
		// caller rather than a restartable one.
		as.performAffineReadiness(s, root, r, b, stop)
		return
	}
	// THE ASSERTION MUST BE ABOUT THIS CONSUMER. The write boundary checks this too, and the two
	// are not redundant: this tells the pass why it may not compose the seal, and the boundary
	// refuses a batch assembled by anything at all.
	if ready.AffGen != r.AffGen || ready.Durable != r.Name || ready.PubRelDur != r.PubRelDur {
		s.Warnf("MQTT recovery pass: the affine readiness of %q/%q covers %q/%q and the enrolled "+
			"consumer is %q/%q, so it asserts nothing about this cleanup",
			root.ID, root.Gen, ready.AffGen, ready.Durable, r.AffGen, r.Name)
		return
	}
	if !b.spend(2) {
		return
	}
	txn := mqttNewInflightNonce()
	if serr := as.jsa.opSealAffineProvisioning(root, r, rseq, txn); serr != nil {
		s.Debugf("MQTT recovery pass: sealing the provisioning of %q/%q/%q: %v",
			root.ID, root.Gen, r.Inc, serr)
	}
}

// concludeRecoveryRoot rolls a finished sweep, or retires the generation.
//
// # Retirement is CERTIFICATE FIRST, then the root
//
// The certificate is what satisfies the release gate after the root is gone, so writing it first
// is the fence: a crash between the two leaves both, which converges — the next pass re-derives
// retirement, validates the standing certificate and deletes the root again. The other order would
// leave a generation with neither, and the gate would then refuse a release for ever on evidence
// that was destroyed rather than superseded.
//
// AN ABSENT READ AFTER A TIMEOUT IS NOT PROOF THE CERTIFICATE NEVER LANDED. The original
// publication may still commit, so absence means retry the IDENTICAL certificate; only a present,
// validated, matching one settles success, and the root delete waits for that.
func (as *mqttAccountSessionManager) concludeRecoveryRoot(s *Server, root *mqttRecoveryRoot,
	rootSeq uint64, b *mqttRecoveryBudget, stop func() bool) {

	if !b.spend(1) {
		return
	}
	led, _, lerr := as.jsa.loadGenLedger(root.ID, root.Epoch, root.Gen)
	if lerr != nil {
		return
	}
	// DELETIONS AUTHORIZED BELOW A SEALED FLOOR FINISH HERE (F-112), one bounded step per visit,
	// before the root may retire: a root that retired with them outstanding would leave them
	// nobody's.
	if led != nil && led.Del < led.Lo {
		if !b.spendProgress(1) {
			return
		}
		if _, derr := as.jsa.deleteRetiredPositionsStep(root.ID, root.Epoch, root.Gen); derr != nil {
			s.Debugf("MQTT recovery pass: deleting retired positions of %q/%q was not confirmed: %v",
				root.ID, root.Gen, derr)
		}
		if led, _, lerr = as.jsa.loadGenLedger(root.ID, root.Epoch, root.Gen); lerr != nil {
			return
		}
	}
	if mqttRootRetires(root, led) {
		// A DISCARDED GENERATION CERTIFIES ONLY WITH ITS SHARED LEDGER SEALED AND EMPTY (F-105
		// 7e). Checked here, after the roll decision, so an unresolved sweep still rolls whatever
		// the shared closing does; the root stays until a visit completes it.
		if !as.stepSharedClosing(s, root, b, stop) {
			return
		}
		as.retireRecoveryRoot(s, root, rootSeq, b)
		return
	}
	// CAUGHT UP, NOT COMPLETED. An OPEN generation whose sweep has finished, or one with
	// unresolved work, is revisited for growth or for sealing: the next sweep starts at this
	// one's accumulator, or past its bound when it left none.
	bound := uint64(0)
	if led != nil {
		bound = led.Hi
		if led.State == mqttGenLedgerSealed {
			bound = led.SealedHi
		}
	}
	next, rerr := mqttRollSweep(root, bound)
	if rerr != nil || next == nil {
		return
	}
	if !b.spendProgress(1) {
		return
	}
	if _, perr := as.jsa.persistSweep(next, rootSeq); perr != nil {
		s.Debugf("MQTT recovery pass: rolling the sweep of %q/%q was not confirmed: %v",
			root.ID, root.Gen, perr)
	}
}

// retireRecoveryRoot writes the completion certificate and then removes the root.
func (as *mqttAccountSessionManager) retireRecoveryRoot(s *Server, root *mqttRecoveryRoot,
	rootSeq uint64, b *mqttRecoveryBudget) {

	if !b.spendProgress(3) {
		return
	}
	cert := &mqttGenCompletionCertificate{
		ID: root.ID, Epoch: root.Epoch, Gen: root.Gen, SealedHi: root.Bound,
	}
	if err := as.jsa.publishGenCompletionCert(cert); err != nil {
		// NOT CONFIRMED, so the root stays. The next visit re-derives retirement and retries the
		// identical certificate; nothing here concludes the publication failed.
		s.Debugf("MQTT recovery pass: certifying %q/%q was not confirmed: %v", root.ID, root.Gen, err)
		return
	}
	if derr := as.jsa.deleteMsg(mqttInflightStreamName, rootSeq, true); derr != nil {
		// The certificate stands, so the generation is certified even though its root remains.
		// The next pass finds the root, re-derives retirement, validates the certificate it
		// already wrote, and deletes the root again. A conflict or an ambiguity never escalates
		// to an unconditional delete.
		s.Debugf("MQTT recovery pass: removing the root of %q/%q was not confirmed: %v",
			root.ID, root.Gen, derr)
	}
}

// performAffineReadiness does the two prerequisite acts and then asserts them.
//
// # The entitlement is the generation's own durable CLOSURE
//
// Not a teardown's transient target, not a marked record, and not a liveness check: the closure
// says this generation ENDED and its `Discard` member says its obligations were authorized to be
// abandoned. Those are the two facts the acts rest on, and both are permanent. A generation whose
// closure does NOT discard never enrols an affine target at all, so this is unreachable for it —
// and it is refused here as well, because a rule that is only unreachable is not a rule.
//
// # The targets come from the resource record
//
// The affine durable, its affine generation and the PUBREL consumer whose records carry the proofs
// are all on the enrolment, so the work needs no session object and no membership: everything a
// transient teardown would have held is named durably. The session built here is a HANDLE to the
// JSA and the account manager, created on the retiring generation so every name it derives is
// that incarnation's; it provisions nothing.
//
// # Partial work never licenses the assertion
//
// Both helpers run under this pass's budget, charging per record. An exhausted charge fails the
// act, and readiness is written only after BOTH report completion — so an interrupted walk leaves
// exactly the durable facts it already committed (a secured proof, a discarded claim), which are
// repeatable, and the next pass walks from the start.
func (as *mqttAccountSessionManager) performAffineReadiness(s *Server, root *mqttRecoveryRoot,
	r *mqttOutboundResource, b *mqttRecoveryBudget, stop func() bool) {

	if !b.spend(1) {
		return
	}
	closed, _, status := as.jsa.loadGenClosure(root.ID, root.Epoch, root.Gen)
	if status != mqttOutboundValid {
		// AN UNREADABLE OR ABSENT CLOSURE ESTABLISHES NOTHING. Without it there is no
		// entitlement, so the pass does no act at all.
		return
	}
	if !closed.Discard {
		// NOT THIS GENERATION'S OBLIGATIONS TO ABANDON. Nothing should have enrolled an affine
		// target here, and the pass refuses to act as though something had.
		s.Warnf("MQTT recovery pass: generation %q of client %q carries an enrolled affine "+
			"consumer and a NON-discarding closure, so no discard disposition authorizes its "+
			"cleanup; nothing is done", root.Gen, root.ID)
		return
	}

	opts := s.getOpts()
	sess := mqttSessionCreate(&as.jsa, root.ID, getHash(root.ID), root.Gen, 0, _EMPTY_, _EMPTY_, opts)
	sess.asm = as
	sess.domainTk = as.domainTk
	target := &mqttTeardownTarget{
		id: root.ID, idHash: getHash(root.ID), gen: root.Gen,
		pubRelDur: r.PubRelDur, q2AffGen: r.AffGen, q2AffDur: r.Name,
	}
	// ONE ATTEMPT PER PASS, run to COMPLETION, and CANCELLABLE between RECORD UNITS.
	//
	// The walk this work performs is account-wide and cannot be filtered: assignment claims are
	// keyed by group durable, not by client. Charging it against the per-operation budget would
	// not bound it, it would STARVE it — a stable population larger than one pass means the
	// budget expires before the end, every pass repeats the same prefix, and readiness is never
	// published for any generation. So the bound is a COUNT, and what protects the sweep
	// goroutine is cancellation rather than accounting.
	//
	// SAY THE GRANULARITY EXACTLY, because the useful contract is narrower than "every store
	// operation". The check below runs between the enumeration's own reads and before each
	// record's processing unit begins. A unit already ENTERED runs to its end: it may perform
	// nested reads, up to mqttQ2AffClaimAttempts load/CAS rounds, and its mutations — and on the
	// discard side one reap, which re-reads the claim and may itself perform another bounded
	// advance and a delete. Cancellation is a stop between units, not a transactional prohibition
	// on writes already licensed by the unit in progress. What that buys is what the sweep
	// goroutine needs: shutdown waits for one bounded unit rather than for the whole account.
	//
	// The spending of the attempt also ENDS THE PASS after this root (walkRecoveryShard), which
	// is what stops a root whose resolution fails permanently from taking the attempt forever.
	if !b.takeReadinessAttempt() {
		return
	}
	charge := func() bool { return stop == nil || !stop() }
	// SIGNALLED, so a test can request shutdown at the first store operation of this walk rather
	// than at some call count it had to measure. What bounds the walk is cancellation, and a
	// cancellation is only demonstrable from inside it.
	if h := mqttTestRecoveryReadinessHookV.Load(); h != nil {
		(*h)()
	}

	// THE PUBREC PROOFS FIRST, because they live in the records the cleanup will destroy, and
	// then the discard disposition, which is what tells a contender the delivery died with its
	// session rather than being available to redistribute. This is the same order the teardown
	// runs them in, and the order is the point rather than the caller.
	if perr := sess.secureQ2AffPubRelProofsCharged(target, charge); perr != nil {
		s.Debugf("MQTT recovery pass: securing the PUBREC proofs of %q/%q: %v",
			root.ID, root.Gen, perr)
		return
	}
	if derr := as.discardQ2AffClaimsForCharged(getHash(root.ID), r.AffGen, charge); derr != nil {
		s.Debugf("MQTT recovery pass: discarding the assignments of %q/%q: %v",
			root.ID, r.AffGen, derr)
		return
	}
	if !b.spendProgress(1) {
		return
	}
	if rerr := as.jsa.publishAffineReadiness(&mqttAffineReadiness{
		ID: root.ID, Epoch: root.Epoch, Gen: root.Gen,
		AffGen: r.AffGen, Durable: r.Name, PubRelDur: r.PubRelDur,
	}); rerr != nil {
		s.Debugf("MQTT recovery pass: asserting the affine readiness of %q/%q: %v",
			root.ID, root.Gen, rerr)
	}
}

// transferredCleanupKind reports whether this cleanup duty's resource is one this change's pass
// may act on at all.
//
// Read from the resource record rather than inferred, and asked BEFORE the duty classifier, so
// no verdict is computed for a duty this pass will not act on. An unreadable resource is NOT
// transferred: a record this broker cannot read is one it may neither act on nor reason about.
//
// The affine consumer is transferred by its enrolment; a source or PUBREL consumer only in a
// DISCARDED generation (F-105 7d), where no live route remains to retire it. In a live generation
// their retirement is the session's (outbound_source_retire.go).
func (as *mqttAccountSessionManager) transferredCleanupKind(root *mqttRecoveryRoot, inc string, discarded bool) bool {
	r, _, err := as.jsa.loadOutboundResource(root.ID, root.Epoch, root.Gen, inc)
	if err != nil || r == nil {
		return false
	}
	return r.Kind == mqttResourceAffineConsumer || (discarded && mqttResourceLogicalUseKind(r.Kind))
}

// mqttTestRecoveryReadinessHookV fires once per readiness walk the recovery pass performs, after
// the attempt is taken and before the first store operation.
//
// A package-level seam because the walk is reached through helpers that hold no test state, and
// the fact under test is WHEN cancellation lands.
var mqttTestRecoveryReadinessHookV atomic.Pointer[func()]

func setMQTTTestRecoveryReadinessHook(h func()) func() {
	prev := mqttTestRecoveryReadinessHookV.Load()
	if h == nil {
		mqttTestRecoveryReadinessHookV.Store(nil)
	} else {
		mqttTestRecoveryReadinessHookV.Store(&h)
	}
	return func() { mqttTestRecoveryReadinessHookV.Store(prev) }
}

// recoveryActorIsAuthorized reads the client's authority before this pass MUTATES anything on its
// behalf, and says so in the operator's words when it cannot.
//
// # Why this is one check and not one per branch
//
// Every duty whose act is a RECORD is protected without it: the act CASes the authority and
// `commitAuthorityTransition` refuses a batch with no predecessor. That made a per-branch check
// look redundant — and it was, right up until a branch whose act is PHYSICAL. The consumer delete
// CASes nothing, so nothing downstream refuses it; the readiness work secures proofs, marks
// assignments discarded and may reap them, and the seal that would have been refused comes after
// all of it. Both were reached with no authority read, and in both the refusal arrived after the
// irreversible half.
//
// So the check is placed where the pass DISPATCHES work, covering the family rather than the
// instance: `mqttResourceDeleteAuthorized` and an enrolled closure say a past batch licensed an
// act, never that this actor may still perform one.
func (as *mqttAccountSessionManager) recoveryActorIsAuthorized(s *Server, root *mqttRecoveryRoot,
	doing string) bool {

	load := as.jsa.loadOutboundAuthority(root.ID, root.Epoch)
	if load.status == mqttOutboundValid && load.rec != nil {
		return true
	}
	s.Debugf("MQTT recovery pass: the authority of %q could not be established (status %v), so "+
		"%s %q is withheld rather than performed", root.ID, load.status, doing, root.Gen)
	return false
}
