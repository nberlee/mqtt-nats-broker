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
	"sync"
	"time"
)

// Reclaiming one retained copy (F-105 7c).
//
// ONE protocol, re-entrant from any landed predecessor, every fact read from the store:
//
//  1. the copy's eligibility — never admitted, or its exchange ended / carrying the retained
//     abandonment fact / its generation closed with Discard — is decided by the BOUNDARY inside
//     the closing batch, never by an earlier read here;
//  2. close the manifest; 3. release the retained-material edge; 4. seal the owner's provisioning
//     (the seal requires the manifest CLOSED); 5. authorize deletion; 6. purge the chunks and the
//     manifest (announce, delete, confirm); 7. once the manifest reads absent for a
//     DeleteAuthorized owner, record CleanupComplete and resolve both duties.
//
// No drain proof: no acquired action reads material. The exchange's own records — its register
// and its canonical PUBREL — are not this protocol's; they go with the exchange.
//
// Each call performs ONE bounded step and reports whether more remains, so the live owner and the
// recovery pass charge it per step.

// mqttReclaimStep is one call's result.
type mqttReclaimStep uint8

const (
	// mqttReclaimDone: the copy is reclaimed and both duties resolved.
	mqttReclaimDone mqttReclaimStep = iota + 1
	// mqttReclaimProgress: a step landed; call again.
	mqttReclaimProgress
	// mqttReclaimWithheld: not eligible now (a wake is not a permission); retry later.
	mqttReclaimWithheld
)

// reclaimRetainedPayloadStep performs one step of one copy's reclamation.
func (jsa *mqttJSA) reclaimRetainedPayloadStep(as *mqttAccountSessionManager, id, epoch, gen, inc string) (mqttReclaimStep, error) {
	r, rSeq, rerr := jsa.loadOutboundResource(id, epoch, gen, inc)
	if rerr != nil || r == nil {
		return mqttReclaimWithheld, fmt.Errorf("payload owner %q of generation %q could not be read: %v", inc, gen, rerr)
	}
	if r.Kind != mqttResourceRetainedPayload {
		return mqttReclaimWithheld, fmt.Errorf("resource %q is not a retained payload", inc)
	}
	if r.State == mqttResourceCleanupComplete {
		return jsa.resolveRetainedPayloadDuties(id, epoch, gen, inc)
	}
	smsg, lerr := jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(getHash(id), gen, r.Name))
	present := lerr == nil && smsg != nil
	if lerr != nil && isErrorOtherThan(lerr, JSNoMessageFoundErr) {
		return mqttReclaimWithheld, fmt.Errorf("the manifest of %q could not be read: %w", r.Name, lerr)
	}
	var m *mqttRetainedManifest
	if present {
		cur, derr := mqttDecodeRetainedManifest(smsg.Data, id, epoch, gen, r.Name)
		if derr != nil {
			return mqttReclaimWithheld, fmt.Errorf("the manifest of %q is unsupported: %w", r.Name, derr)
		}
		if cur.Inc != inc {
			return mqttReclaimWithheld, fmt.Errorf("the manifest at %q belongs to materialization %q, not %q", r.Name, cur.Inc, inc)
		}
		m = cur
	}
	txn := mqttNewInflightNonce()
	switch {
	case m != nil && m.State != mqttManifestClosed:
		if _, err := jsa.opCloseRetainedManifest(txn, "op-reclaim-close", m, smsg.Sequence); err != nil {
			return mqttReclaimWithheld, err
		}
		return mqttReclaimProgress, nil
	case r.State == mqttResourceLive && len(r.Deps) > 0:
		for _, d := range r.Deps {
			if d.Role == mqttRoleRetainedMaterial {
				if _, err := jsa.opReleaseDependency(id, epoch, txn, "op-reclaim-release", gen, inc, d); err != nil {
					return mqttReclaimWithheld, err
				}
				return mqttReclaimProgress, nil
			}
		}
		return mqttReclaimWithheld, fmt.Errorf("payload owner %q carries an edge this protocol does not release", inc)
	case r.State == mqttResourceLive && r.Prov != mqttProvisioningSealed:
		if _, err := jsa.opSealProvisioning(id, epoch, txn, "op-reclaim-seal", gen, inc); err != nil {
			return mqttReclaimWithheld, err
		}
		return mqttReclaimProgress, nil
	case r.State == mqttResourceLive:
		if _, err := jsa.opAuthorizeDeletion(id, epoch, txn, "op-reclaim-authorize", gen, inc); err != nil {
			return mqttReclaimWithheld, err
		}
		return mqttReclaimProgress, nil
	case r.State == mqttResourceDeleteAuthorized && m != nil:
		if _, err := as.purgeMaterialStep(id, epoch, txn, m, smsg.Sequence); err != nil {
			return mqttReclaimWithheld, err
		}
		return mqttReclaimProgress, nil
	case r.State == mqttResourceDeleteAuthorized:
		// The manifest read answered "no message" for a DeleteAuthorized owner: the store's positive
		// answer, which the purge's last act produced.
		ord, oerr := jsa.resourceDutyOrdinal(id, epoch, gen, inc, mqttDutyCleanup)
		if oerr != nil {
			return mqttReclaimWithheld, oerr
		}
		root := &mqttRecoveryRoot{ID: id, Epoch: epoch, Gen: gen}
		d, _, derr := jsa.loadDutyEntryAt(id, epoch, gen, ord)
		if derr != nil || d == nil {
			return mqttReclaimWithheld, fmt.Errorf("cleanup duty %d of %q could not be read: %v", ord, inc, derr)
		}
		if err := jsa.completeCleanupAndResolve(root, r, rSeq, mqttSweptDuty{Ord: ord, Entry: d}, txn,
			mqttCleanupRemoved); err != nil {
			return mqttReclaimWithheld, err
		}
		return mqttReclaimProgress, nil
	}
	return mqttReclaimWithheld, fmt.Errorf("payload owner %q stands in a state this protocol does not continue", inc)
}

// resolveRetainedPayloadDuties resolves whichever of the payload's two duties is still open.
func (jsa *mqttJSA) resolveRetainedPayloadDuties(id, epoch, gen, inc string) (mqttReclaimStep, error) {
	root := &mqttRecoveryRoot{ID: id, Epoch: epoch, Gen: gen}
	for _, kind := range []mqttDutyKind{mqttDutyCleanup, mqttDutyProvision} {
		ord, oerr := jsa.resourceDutyOrdinal(id, epoch, gen, inc, kind)
		if oerr != nil {
			return mqttReclaimWithheld, oerr
		}
		d, _, derr := jsa.loadDutyEntryAt(id, epoch, gen, ord)
		if derr != nil || d == nil {
			return mqttReclaimWithheld, fmt.Errorf("duty %d of %q could not be read: %v", ord, inc, derr)
		}
		if d.State == mqttDutyTerminal {
			continue
		}
		if err := jsa.opResolveDuty(root, mqttSweptDuty{Ord: ord, Entry: d}, mqttNewInflightNonce()); err != nil {
			return mqttReclaimWithheld, err
		}
		return mqttReclaimProgress, nil
	}
	return mqttReclaimDone, nil
}

// resourceDutyOrdinal finds the ordinal of one of a resource's duties by walking its ledger: the
// retained payload's and the source consumer's retirements both end in its cleanup duty.
func (jsa *mqttJSA) resourceDutyOrdinal(id, epoch, gen, inc string, kind mqttDutyKind) (uint64, error) {
	led, _, lerr := jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil {
		return 0, lerr
	}
	if led == nil {
		return 0, fmt.Errorf("generation %q has no ledger, so resource %q has no duty", gen, inc)
	}
	bound := led.Hi
	if led.State == mqttGenLedgerSealed {
		bound = led.SealedHi
	}
	for ord := uint64(1); ord <= bound; ord++ {
		d, _, derr := jsa.loadDutyEntryAt(id, epoch, gen, ord)
		if derr != nil {
			return 0, derr
		}
		if d == nil {
			return 0, fmt.Errorf("duty %d of generation %q is missing", ord, gen)
		}
		if d.Kind == kind && d.Ref == inc {
			return ord, nil
		}
	}
	return 0, fmt.Errorf("resource %q carries no %q duty in generation %q", inc, kind, gen)
}

// mqttRetainedReclaimKey names one retirement the owner drives: a retained copy's reclamation, or
// (src) a source consumer's retirement (F-105 7c-lc). A source item whose incarnation is not yet
// known — a claim still being arbitrated — is keyed by its claim transaction in inc.
type mqttRetainedReclaimKey struct {
	gen, inc string
	src      bool
}

// mqttRetainedReclaim is a session's live reclamation owner: keyed by copy, capped backoff. A
// separate goroutine from the replay, so neither can starve the other.
//
// A KEY IS DROPPED ON A POSITIVE ANSWER ONLY. A copy whose exchange may still need it (the close
// boundary's errMQTTRetainedMaterialNeeded) is dropped after the bound: its exchange's ending —
// PUBACK, a negative acknowledgement, PUBCOMP's finalization, the abandonment, the withdrawal — is
// the wake that brings it back. Any OTHER withheld step is a read or a write that failed, which
// says nothing about the copy, so that key is never dropped while this session object is the
// client ID's resident session; once it is not, the closure root and the next CONNECT's discovery
// own the copy.
type mqttRetainedReclaim struct {
	mu      sync.Mutex
	pending map[mqttRetainedReclaimKey]*mqttRetainedReclaimState
	running bool
}

// mqttRetainedReclaimState is one copy's standing in the reclaimer: its withheld attempts SINCE ITS
// LAST WAKE. A "material still needed" answer says nothing about a wake that arrived while that
// attempt was out — the exchange's ending, typically — so every wake restarts the count, and the
// bound is only ever reached by answers newer than the copy's last wake.
type mqttRetainedReclaimState struct {
	withheld int
	// src is the source retirement's work item; nil for a retained copy. Only the owner goroutine
	// reads or advances it once it is scheduled.
	src *mqttSourceRetirement
}

// wake schedules (gen, inc) and starts the owner if idle.
func (rc *mqttRetainedReclaim) wake(sess *mqttSession, gen, inc string) {
	rc.mu.Lock()
	if rc.pending == nil {
		rc.pending = make(map[mqttRetainedReclaimKey]*mqttRetainedReclaimState)
	}
	k := mqttRetainedReclaimKey{gen: gen, inc: inc}
	st := rc.pending[k]
	if st == nil {
		st = &mqttRetainedReclaimState{}
		rc.pending[k] = st
	}
	st.withheld = 0
	start := !rc.running
	rc.running = true
	rc.mu.Unlock()
	if start {
		go sess.runRetainedReclaim()
	}
}

// runRetainedReclaim drives every pending copy one step at a time, round-robin, until none
// remains; a withheld copy backs off without blocking the others.
func (sess *mqttSession) runRetainedReclaim() {
	rc := &sess.retainedReclaim
	for {
		resident := sess.resident() // read outside rc.mu: as.mu is never taken under it
		rc.mu.Lock()
		if len(rc.pending) == 0 || sess.asm == nil || sess.jsa == nil || !resident {
			rc.pending = nil
			rc.running = false
			rc.mu.Unlock()
			return
		}
		keys := make([]mqttRetainedReclaimKey, 0, len(rc.pending))
		for k := range rc.pending {
			keys = append(keys, k)
		}
		rc.mu.Unlock()
		progressed := false
		for _, k := range keys {
			// ONE ADMITTED ATTEMPT per step (outbound_foreground_priority.go). Refused only at
			// shutdown, which hands every copy to the restart's recovery.
			if !sess.admitBackgroundAuthority() {
				rc.mu.Lock()
				rc.running = false
				rc.mu.Unlock()
				return
			}
			var step mqttReclaimStep
			var err error
			if k.src {
				rc.mu.Lock()
				var it *mqttSourceRetirement
				if st := rc.pending[k]; st != nil {
					it = st.src
				}
				rc.mu.Unlock()
				step, err = sess.retireSourceStep(it)
			} else {
				step, err = sess.jsa.reclaimRetainedPayloadStep(sess.asm, sess.id, sess.asm.sessStreamEpoch, k.gen, k.inc)
			}
			if h := sess.testReclaimAfterStep.Load(); h != nil {
				(*h)(k, step, err)
			}
			rc.mu.Lock()
			st := rc.pending[k]
			switch {
			case st == nil:
			case step == mqttReclaimDone:
				delete(rc.pending, k)
				progressed = true
			case step == mqttReclaimProgress:
				st.withheld = 0
				progressed = true
			default:
				st.withheld++
				if errors.Is(err, errMQTTRetainedMaterialNeeded) && st.withheld >= mqttRetainedStepAttempts {
					delete(rc.pending, k)
				}
			}
			rc.mu.Unlock()
		}
		if !progressed {
			// Everything withheld: wait before re-reading, capped.
			min := -1
			rc.mu.Lock()
			for _, st := range rc.pending {
				if min < 0 || st.withheld < min {
					min = st.withheld
				}
			}
			rc.mu.Unlock()
			time.Sleep(mqttRetainedStepBackoff(min))
		}
	}
}

// resident reports whether this object is still the client ID's session: a superseded or removed
// object's workers stop, its durable records owned by its successor's resume and the closure root.
func (sess *mqttSession) resident() bool {
	as := sess.asm
	if as == nil {
		return false
	}
	as.mu.Lock()
	defer as.mu.Unlock()
	return as.sessions[sess.id] == sess
}

// servesGeneration reports whether this resident object still serves generation gen.
func (sess *mqttSession) servesGeneration(gen string) bool {
	if !sess.resident() {
		return false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.gen == gen
}
