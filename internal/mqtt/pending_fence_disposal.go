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

import "sync/atomic"

// A pending cancellation fence is disposed of only behind a confirmed authority advance (F-119).
//
// A teardown that cancels an allocation whose admission store is still in flight writes the
// create-only PENDING FENCE on the empty subject and forgets the nonce verdict at once. The fence
// is then the only thing standing between the cancelled allocation and a late landing: an attempt
// of that nonce that meets it loses its register create-only CAS. Deleting the fence while such an
// attempt can still commit lets it land on the empty subject, and a resume with no verdict then
// restores the cancelled delivery acting — a lost cancellation.
//
// For an ORDINARY (authority-class) fence of the serving generation the one door therefore deletes
// only after three things hold, checked here in this order:
//
//  1. the deciding connection's licence names the serving generation AND tenure, compared with the
//     predecessor the advance CASes — a displaced connection, on any broker, deletes nothing;
//  2. QUIESCENCE: the session's custody ledger, being the fence's generation's, holds no admission
//     of the fence's nonce whose ladder has not returned. Liveness comes from the LADDER, not from
//     the scheduling entry, which a drain removes while its store is still in flight; a ledger of
//     another generation makes this refuse, because a local generation mismatch is never read as
//     retirement (the clean-start re-mint precedes the authority handoff);
//  3. a CONFIRMED advance of the serving authority's revision under that licence (kind
//     fence-pending-disposal): every attempt of the nonce prepared before it CASes an authority
//     sequence it moves past, so none of them can land whenever the store processes it.
//
// Together they leave no attempt that can meet the empty subject: (3) kills every attempt already
// prepared, (2) proves no ladder of that nonce can prepare another (a nonce is allocated once and
// ladderDone never reverts), and (1) leaves every other tenure's attempts to the entitlement compare
// admission already makes.
//
// A fence whose generation the stored authority no longer SERVES needs none of it: an admission
// requires a serving authority at its own generation, a generation is never re-proposed, and every
// attempt prepared while it served CASes a sequence the change has since moved past. A preparing,
// reserving or absent authority refuses instead — a cancelled intent restores its predecessor's
// generation, and nothing here may read a transient state as retirement.
//
// The slot-only (experimental affine) class has no authority to advance, and keeps its plain
// disposal; its cancellation durability is chapter P's.

// mqttTestPendingFenceDisposalHookV observes each decision for a test: whether an advance was
// committed and the result. Nil in production.
var mqttTestPendingFenceDisposalHookV atomic.Pointer[func(subject string, advanced, ok bool)]

// mqttTestPendingFenceBeforeCommitHookV runs immediately before the advance's commit gate, after
// every earlier gate passed. Nil in production.
var mqttTestPendingFenceBeforeCommitHookV atomic.Pointer[func(subject string)]

// mqttFencePendingDisposalTransition composes the advance: the serving predecessor reinstalled
// exactly, its revision advanced, nothing else.
func mqttFencePendingDisposalTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn string) *mqttAuthorityTransition {
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: "op-fence-pending-disposal",
			Kind: mqttTransitionFencePendingDisposal, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: prev.State, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
			Intent: prev.Intent, Creation: prev.Creation,
		},
	}
}

// fenceBeforePendingFenceDisposal reports whether the one door may delete the pending fence cur on
// subject now. It is called at both door entrances, immediately before the delete, only for the
// pending-fence disposition. gate is the caller's dispatch gate; every store request this issues —
// the authority load, the commit's predecessor readback and batch, and each read and write of the
// resolution and abort — runs on a JSA view that asks (and charges) it once per request, immediately
// before that request is queued (withRequestGate). Nil for a foreground caller. A false return writes nothing further and keeps
// the fence: a refused advance, a compare that fails, a live ladder, an unreadable authority, a
// closed gate or a refused background admission.
//
// Session lock NOT held on entry.
func (sess *mqttSession) fenceBeforePendingFenceDisposal(subject string, cur *mqttInflightRecord,
	lic mqttDestroyLicence, gate mqttDoorGate) (ok bool) {

	if cur == nil {
		return false
	}
	if mqttDurIsSlotOnlyClass(cur.Dur) {
		return true
	}
	advanced := false
	if h := mqttTestPendingFenceDisposalHookV.Load(); h != nil {
		defer func() { (*h)(subject, advanced, ok) }()
	}
	name, perr := mqttParseInflightSubject(subject)
	if perr != nil || sess.asm == nil || sess.jsa == nil {
		return false
	}
	jsa := sess.jsa
	if gate != nil {
		jsa = jsa.withRequestGate(gate.open)
	}
	al := jsa.loadOutboundAuthority(sess.id, sess.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		return false
	}
	prev, prevSeq := al.rec, al.seq
	switch {
	case prev.State == mqttAuthorityClosed:
		// A closed authority admits nothing and never serves this generation again.
		return true
	case prev.State != mqttAuthorityServing:
		return false
	case prev.Gen != name.gen:
		return true
	}
	if !lic.entOK || lic.ent.Gen != prev.Gen || lic.ent.Tenure != prev.Tenure {
		return false
	}
	sess.mu.Lock()
	quiescent := sess.custody.forGen(name.gen) && !sess.custody.admissionLadderLive(name.gen, cur.Nonce)
	sess.mu.Unlock()
	if !quiescent {
		return false
	}
	if lic.admit != nil && !lic.admit() {
		return false
	}
	if h := mqttTestPendingFenceBeforeCommitHookV.Load(); h != nil {
		(*h)(subject)
	}
	out, err := jsa.commitOp(mqttFencePendingDisposalTransition(prev, prevSeq, mqttNewInflightNonce()))
	if err == nil {
		advanced = true
		return true
	}
	if out.Handle == nil {
		return false
	}
	v := jsa.resolveTransitionHandle(out.Handle)
	if v == mqttTransitionUnresolved {
		_ = jsa.abortTransition(&out.Handle.attempt)
		v = jsa.resolveTransitionHandle(out.Handle)
	}
	advanced = v == mqttTransitionLanded || v == mqttTransitionSuperseded
	return advanced
}
