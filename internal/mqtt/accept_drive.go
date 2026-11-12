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
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Driving one CONNECT's acceptance (issue #133, plan 5.1).
//
// The acceptance is one batch, so there is nothing to finalize, cancel or abandon afterwards:
// it landed or it did not. What remains is telling those apart when the store's answer is
// lost, and the rule is the batch's own atomicity: an acceptance landed iff the authority
// names its nonce, and a frozen batch whose authority expectation is stale can never land.

// errMQTTForkFenceMoved is an accepting CONNECT finding the outbound authority past the sequence
// fork repair read its consumers under (issue #107 7f): something — a disposal among them — wrote it
// since, so what the merge reinstated is no longer proven to serve. Nothing landed; the CONNECT
// re-drives from the authoritative restore, which re-runs fork repair on fresh reads.
var errMQTTForkFenceMoved = errors.New("the outbound authority moved past fork repair's reads")

// mqttTestForkFenceRedriveV observes the accept loop taking the fork-fence re-drive. nil in
// production.
var mqttTestForkFenceRedriveV atomic.Pointer[func(clientID string)]

// mqttOwnershipStepReattempts is the per-CONNECT allowance of re-attempts of an authority write
// (the bind's) proven never to have landed.
const mqttOwnershipStepReattempts = 4

// errMQTTAcceptRecordMoved reports that the acceptance batch was refused because the session
// record it would replace moved: the CONNECT re-reads the record and decides again.
var errMQTTAcceptRecordMoved = errors.New("the session record this acceptance would replace moved")

// mqttTestAcceptAuthorityReadHookV runs before an acceptance reads the authority it will
// displace: the window between fork repair's reads and the acceptance's own. nil in production.
var mqttTestAcceptAuthorityReadHookV atomic.Pointer[func(id string)]

// mqttAcceptTestReadToSendHookV runs between an acceptance's or a bind's authority read and its
// send, with the stage ("accept" or "bind"): the window a pre-admitted background write
// invalidates. nil in production.
var mqttAcceptTestReadToSendHookV atomic.Pointer[func(id, stage string)]

func mqttAcceptReadToSend(id, stage string) {
	if h := mqttAcceptTestReadToSendHookV.Load(); h != nil {
		(*h)(id, stage)
	}
}

// acceptOwnership commits this CONNECT's acceptance and returns the generation, tenure and
// acceptance nonce it installed. retryOK charges the CONNECT's create-retry allowance for a
// re-send after same-subject contention (10164); stepAllow charges its per-CONNECT allowance of
// re-attempts for every other re-send or recomposition, which a re-drive does not replenish.
func (sess *mqttSession) acceptOwnership(tok recordTx, clientID, sessEpoch string, cleanStart bool,
	targets mqttCleanStartRetireTargets, budget *mqttOwnershipBudget, retryOK, stepAllow func() error,
	willDelayCapable bool) (gen, tenure, nonce string, anchor *mqttWillAnchorRef, err error) {

	jsa := sess.jsa.withDeadline(budget.deadline())
	id := sess.id
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	// THE ACCEPTANCE IS FOREGROUND AUTHORITY WORK: while it stands, no background authority
	// write of this client (a generation's retirement, the lifecycle sweep, an abort) is
	// admitted, so the composition contends only with what was admitted before it.
	fg := sess.announceForegroundAuthority()
	defer fg.release()
	var retire mqttClosingTargets
	if cleanStart {
		retire.AffineDurable, retire.AffineGen, retire.PubRelDurable =
			mqttAffineRetireDescriptor(targets.AffineDurable, targets.AffineGen, targets.PubRelDurable)
	}
	// THE BUDGET OUTLASTS THE PRE-ADMITTED WRITERS: each composition after the first follows a
	// batch PROVEN never to land because the authority moved under it (resolveAcceptance's
	// retry), which is contention, so it draws on the announcement's derived budget and the
	// absolute deadline, never on stepAllow; a re-send of an unchanged batch charges stepAllow
	// inside resolveAcceptance, and a moved record returns to the CONNECT's own cycle bound.
	for attempt := 0; attempt < fg.budget(mqttForegroundAcquireAttempts); attempt++ {
		if attempt > 0 {
			if rerr := mqttOwnershipStepAllowed(budget, time.Now(), "acceptance recomposition"); rerr != nil {
				return _EMPTY_, _EMPTY_, _EMPTY_, nil, rerr
			}
		}
		if sess.asm != nil {
			sess.asm.mu.RLock()
			entry := sess.asm.testAcceptEntry
			sess.asm.mu.RUnlock()
			if entry != nil {
				if herr := entry(); herr != nil {
					return _EMPTY_, _EMPTY_, _EMPTY_, nil, herr
				}
			}
		}
		if h := mqttTestAcceptAuthorityReadHookV.Load(); h != nil {
			(*h)(id)
		}
		al := jsa.loadOutboundAuthority(id, epoch)
		// THE FORK FENCE (issue #107 7f): fork repair read the consumers it reinstated under the
		// authority at sess.forkFence, so the acceptance must displace exactly that authority. A
		// later one was written after those reads, and the CONNECT re-runs fork repair instead.
		sess.mu.Lock()
		fence := sess.forkFence
		sess.mu.Unlock()
		if fence != 0 && al.status == mqttOutboundValid && al.seq != fence {
			return _EMPTY_, _EMPTY_, _EMPTY_, nil, fmt.Errorf("accepting %q over authority sequence %d, but fork "+
				"repair read its reinstated consumers at %d: %w", id, al.seq, fence, errMQTTForkFenceMoved)
		}
		var reg mqttRegistryLoad
		if al.status == mqttOutboundAbsent {
			reg = jsa.loadLifecycleRegistryShard(epoch, mqttLifecycleRegistryShardFor(getHash(id)))
		}
		sess.mu.Lock()
		r := mqttAcceptRequest{
			ID: id, Epoch: epoch, Txn: mqttNewInflightNonce(), Tenure: mqttNewInflightNonce(),
			Nonce: mqttNewInflightNonce(), At: time.Now().UnixNano(), CleanStart: cleanStart,
			PriorExpiry: sess.expiryInterval, PriorClean: sess.clean, Retire: retire, RecordExists: sess.seq != 0,
			DisplacedMarker: sess.displacedMarker, WillDelayCapable: willDelayCapable,
		}
		sess.mu.Unlock()
		ca, cerr := jsa.composeAcceptance(r, al, reg)
		mqttAcceptReadToSend(id, "accept")
		if cerr != nil {
			return _EMPTY_, _EMPTY_, _EMPTY_, nil, fmt.Errorf("the acceptance of %q cannot be composed: %w", id, cerr)
		}
		// THE RECORD CARRIES THE GENERATION THE AUTHORITY INSTALLS. A resume keeps it; a
		// creation, a Clean Start and a reopen take the lifecycle's next one, which only the
		// authority read could say.
		sess.mu.Lock()
		if sess.gen != ca.Gen {
			sess.deriveGenerationLocked(ca.Gen)
		}
		sess.mu.Unlock()
		var ref *mqttWillAnchorRef
		if ca.Anchor != nil {
			ref = &mqttWillAnchorRef{L: ca.L, Ord: ca.Anchor.Ord}
		}
		detail, serr := sess.saveAcceptShape(jsa, tok, clientID, sessEpoch, ca.T)
		if serr == nil {
			return ca.Gen, r.Tenure, r.Nonce, ref, nil
		}
		landed, retry, rerr := sess.resolveAcceptance(jsa, detail, r.Nonce, retryOK, stepAllow)
		switch {
		case rerr != nil:
			return _EMPTY_, _EMPTY_, _EMPTY_, nil, rerr
		case landed:
			return ca.Gen, r.Tenure, r.Nonce, ref, nil
		case !retry:
			return _EMPTY_, _EMPTY_, _EMPTY_, nil, serr
		}
	}
	return _EMPTY_, _EMPTY_, _EMPTY_, nil, fmt.Errorf("the acceptance of %q did not land within %d compositions", id, fg.budget(mqttForegroundAcquireAttempts))
}

// resolveAcceptance classifies a failed acceptance batch: landed (adopted here), proven never
// to land (retry with a fresh composition), or an error the CONNECT refuses on.
func (sess *mqttSession) resolveAcceptance(jsa *mqttJSA, detail mqttSessSaveDetail, nonce string,
	retryOK, stepAllow func() error) (landed, retry bool, err error) {

	t := detail.acceptShape
	if t == nil {
		return false, false, detail.rawErr
	}
	a := t.Attempt
	for {
		// SAME-SUBJECT CONTENTION (10164) DECIDED NOTHING: the batch is atomic and a rejected
		// send stored no member, so the SAME frozen batch is re-sent within the CONNECT's
		// create-retry allowance, never recomposed.
		if mqttIsMetadataContention(detail.rawErr) {
			if rerr := retryOK(); rerr != nil {
				return false, false, rerr
			}
			authSeq, cerr := jsa.commitAuthorityTransition(t)
			if cerr == nil {
				sess.adoptCommittedMetadata(detail, authSeq+1)
				return true, false, nil
			}
			detail.rawErr = cerr
			continue
		}
		al := jsa.loadOutboundAuthority(a.ID, a.Epoch)
		switch {
		case al.status == mqttOutboundValid && al.rec != nil && al.rec.Life != nil && al.rec.Life.Accept.Nonce == nonce:
			// LANDED. The record is the batch's last member, right after the authority.
			sess.adoptCommittedMetadata(detail, al.seq+1)
			return true, false, nil
		case al.status != mqttOutboundValid && al.status != mqttOutboundAbsent:
			return false, false, fmt.Errorf("the acceptance of %q is unresolved: its authority reads %v", a.ID, al.status)
		}
		stale := (t.Prev == nil) != (al.status == mqttOutboundAbsent) || (t.Prev != nil && al.seq != t.PrevSeq)
		if stale {
			// The frozen batch expects an authority that no longer stands, so it can never land.
			// If its record CAS was what refused it, the record moved too.
			if ce := jsAPIError(detail.rawErr); ce != nil && mqttIsRecordCASRejection(ce) && sess.recordMovedFrom(jsa, detail) {
				return false, false, errMQTTAcceptRecordMoved
			}
			return false, true, nil
		}
		if ce := jsAPIError(detail.rawErr); ce != nil && mqttIsRecordCASRejection(ce) {
			// The authority stands as frozen and the batch was refused: the record moved.
			return false, false, errMQTTAcceptRecordMoved
		}
		// The authority stands as frozen and the outcome is unknown: re-send the SAME batch,
		// which lands or is refused, and decide again.
		if rerr := stepAllow(); rerr != nil {
			return false, false, rerr
		}
		authSeq, cerr := jsa.commitAuthorityTransition(t)
		if cerr == nil {
			sess.adoptCommittedMetadata(detail, authSeq+1)
			return true, false, nil
		}
		detail.rawErr = cerr
	}
}

// recordMovedFrom reports whether the session record is no longer at the base the failed
// acceptance expected.
func (sess *mqttSession) recordMovedFrom(jsa *mqttJSA, detail mqttSessSaveDetail) bool {
	smsg, err := jsa.loadSessionMsg(detail.domainTk, detail.cidHash)
	if err != nil {
		return !isErrorOtherThan(err, JSNoMessageFoundErr) && detail.baseSeq != 0
	}
	return smsg == nil || smsg.Sequence != detail.baseSeq
}

// errMQTTAcceptanceLost reports that this CONNECT's pending acceptance was superseded or
// abandoned before it could bind: the CONNECT refuses before CONNACK and never serves.
var errMQTTAcceptanceLost = errors.New("this connection's acceptance is no longer pending")

// bindAcceptance binds this CONNECT's pending acceptance before CONNACK (plan 5.1's bind
// resolver, authority-only: the bind moves no record).
func (sess *mqttSession) bindAcceptance(budget *mqttOwnershipBudget, nonce string, retryOK func() error) error {
	jsa := sess.jsa.withDeadline(budget.deadline())
	id := sess.id
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	// Foreground, as the acceptance is: no background authority write is admitted beside it.
	fg := sess.announceForegroundAuthority()
	defer fg.release()
	var frozen *mqttAuthorityTransition
	// Charged by cause, as the acceptance is: a recomposition after the authority moved is
	// contention (the derived budget and the deadline); a re-send of the unchanged frozen bind,
	// whose outcome is unknown, charges retryOK.
	for attempt := 0; attempt < fg.budget(mqttForegroundAcquireAttempts); attempt++ {
		if attempt > 0 {
			if rerr := mqttOwnershipStepAllowed(budget, time.Now(), "bind re-attempt"); rerr != nil {
				return rerr
			}
		}
		al := jsa.loadOutboundAuthority(id, epoch)
		if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
			return fmt.Errorf("binding the acceptance of %q: its authority reads %v", id, al.status)
		}
		p := al.rec.Life.Pending
		if frozen != nil && (p == nil || p.Nonce != nonce) {
			// The pending acceptance is gone: the bind landed, or something else displaced it.
			// Only the bind's own outcome says which.
			if h, herr := mqttNewTransitionHandle(frozen); herr == nil {
				if v := jsa.resolveTransitionHandle(h); v == mqttTransitionLanded || v == mqttTransitionSuperseded {
					return nil
				}
			}
		}
		switch {
		case p == nil || p.Nonce != nonce:
			return errMQTTAcceptanceLost
		case frozen == nil || al.seq != frozen.PrevSeq:
			// No bind sent yet, or the authority moved past the frozen one's expectation (which
			// can therefore never land): compose against what stands.
			t, cerr := mqttComposeBind(al.rec, al.seq, mqttNewInflightNonce(), nonce)
			if cerr != nil {
				return cerr
			}
			frozen = t
		default:
			// The authority stands as the frozen bind expects and its outcome is unknown: the
			// same batch is re-sent, charged to the CONNECT's re-attempt allowance.
			if rerr := retryOK(); rerr != nil {
				return rerr
			}
		}
		mqttAcceptReadToSend(id, "bind")
		if _, err := jsa.commitAuthorityTransition(frozen); err == nil {
			return nil
		}
	}
	return fmt.Errorf("binding the acceptance of %q did not land within %d attempts", id, fg.budget(mqttForegroundAcquireAttempts))
}

// observeAcceptanceCommit fires the accepted-CONNECT store observation seam (sess_accept_fact.go)
// for an acceptance batch: the fact, the record, and the authority in the receipt's place, since
// the authority is what now records which acceptance won. Built only when there is an observer.
func (jsa *mqttJSA) observeAcceptanceCommit(phase mqttAcceptStorePhase, batchID string,
	t *mqttAuthorityTransition, seq uint64) error {

	h := jsa.acceptObs.Load()
	if h == nil || t.Record == nil {
		return nil
	}
	obs := mqttAcceptStoreObs{
		Phase: phase, RecSubj: t.Record.subj, RecDigest: mqttRecordDigest(t.Record.body),
		BatchID: batchID, Seq: seq,
	}
	if t.Auth != nil {
		body, _ := json.Marshal(t.Auth)
		obs.RecptSubj = mqttOutboundAuthoritySubject(getHash(t.Attempt.ID))
		obs.RecptDigest = mqttRecordDigest(body)
	}
	return (*h)(obs)
}
