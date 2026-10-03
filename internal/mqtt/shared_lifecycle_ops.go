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
	"sort"
)

// The shared-group lifecycle's OPERATIONS: what a session does with the two records
// (F-105, the shared-group lifecycle transfer). The records and their write boundary are in
// shared_lifecycle.go; this file is the protocol that drives them.
//
//   - ACQUIRE (J1): one acquisition per consumer a filter needs, under this CONNECT's tenure,
//     with a settlement loop that re-attempts the IDENTICAL batch and, on exhaustion, fences
//     the nonce for ever rather than guessing.
//   - RELEASE (L1, L2): the execution half of a release VERDICT the session record carries.
//     Any reader may execute a verdict; the CAS makes executors converge. The last release of
//     an incarnation retires it, and retirement is the consumer's deletion classified and then
//     recorded — a delete that returned success is not a completed retirement.
//   - INSTALL: a resume's tenure, written after its accept save landed and before it
//     reconciles or serves, under the non-regression rule.
//   - SEAL, T3, T4: the closing's fence, the release of everything the sealed ledger lists,
//     and the gate that withholds the record's release until the ledger is sealed and empty.

// mqttSharedReleaseAttempts bounds one execution episode of a verdict. A bound on the
// EPISODE: exhausting it is an error the caller reports, and the verdict stays in the record
// for the next executor.
const mqttSharedReleaseAttempts = 4

// ---------------------------------------------------------------------------
// The session's side: tenure and verdicts
// ---------------------------------------------------------------------------

// sharedEpoch is the account's recovery epoch every record here is stamped with.
func (sess *mqttSession) sharedEpoch() string {
	if sess.asm == nil {
		return _EMPTY_
	}
	return sess.asm.sessStreamEpoch
}

// mintSharedTenure fixes this CONNECT's tenure once its accept save has landed: the record
// sequence that save took, and a nonce minted now. Immutable for the attempt; every join and
// fence this connection composes names exactly this.
func (sess *mqttSession) mintSharedTenure() {
	sess.mu.Lock()
	sess.sharedTenure = mqttSharedTenure{Seq: sess.seq, ID: mqttNewInflightNonce()}
	sess.mu.Unlock()
}

// addAcqReleaseVerdictsLocked records the release verdicts for one removed membership, in
// memory, for the record write the caller is about to make to carry.
//
// sess.mu must be held.
func (sess *mqttSession) addAcqReleaseVerdictsLocked(key string, m mqttSharedMembership) {
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		return
	}
	if sess.acqReleases == nil {
		sess.acqReleases = make(map[string]mqttSharedAcqRef)
	}
	sess.acqReleases[m.Main.Acq] = mqttSharedAcqRef{Group: mqttSharedGroupKey(group, subject), Inc: m.Main.Inc, Acq: m.Main.Acq}
	if m.Up != nil && mqttNeedSubForLevelUp(subject) {
		sess.acqReleases[m.Up.Acq] = mqttSharedAcqRef{Group: mqttSharedGroupKey(group, subject[:len(subject)-2]), Inc: m.Up.Inc, Acq: m.Up.Acq}
	}
}

// addAcqReleaseVerdictsForDroppedLocked records a release verdict for every acquisition the
// LIVE membership holds that `kept` does not name, and returns the nonces it added so a write
// that definitely failed can withdraw them again.
//
// Compared by acquisition NONCE, never by key: a re-SUBSCRIBE of a key the session already
// held REUSED its references, so that key appears on both sides with the same acquisitions and
// is owed nothing, while a key the packet ACQUIRED appears only on the live side. This is the
// whole-packet rollback's verdict write — the record it is about to store names `kept`, so
// every acquisition outside it is abandoned by that write and must be released by it.
//
// sess.mu must be held.
func (sess *mqttSession) addAcqReleaseVerdictsForDroppedLocked(kept map[string]mqttSharedMembership) []string {
	named := make(map[string]struct{})
	for _, m := range kept {
		for _, a := range m.acquisitions() {
			named[a.Acq] = struct{}{}
		}
	}
	var added []string
	for key, m := range sess.sharedSubs {
		group, subject, ok := mqttParseSharedKey(key)
		if !ok {
			continue
		}
		if _, ok := named[m.Main.Acq]; !ok {
			if sess.acqReleases == nil {
				sess.acqReleases = make(map[string]mqttSharedAcqRef)
			}
			sess.acqReleases[m.Main.Acq] = mqttSharedAcqRef{Group: mqttSharedGroupKey(group, subject), Inc: m.Main.Inc, Acq: m.Main.Acq}
			added = append(added, m.Main.Acq)
		}
		if m.Up != nil && mqttNeedSubForLevelUp(subject) {
			if _, ok := named[m.Up.Acq]; !ok {
				if sess.acqReleases == nil {
					sess.acqReleases = make(map[string]mqttSharedAcqRef)
				}
				sess.acqReleases[m.Up.Acq] = mqttSharedAcqRef{Group: mqttSharedGroupKey(group, subject[:len(subject)-2]), Inc: m.Up.Inc, Acq: m.Up.Acq}
				added = append(added, m.Up.Acq)
			}
		}
	}
	return added
}

// withdrawAcqReleaseVerdictsLocked forgets verdicts a DEFINITELY failed write never made
// durable, so the record the session still owns is not followed by an execution nothing
// authorized.
//
// sess.mu must be held.
func (sess *mqttSession) withdrawAcqReleaseVerdictsLocked(nonces []string) {
	for _, n := range nonces {
		delete(sess.acqReleases, n)
	}
}

// acqReleasesSortedLocked renders the verdicts for the record, in nonce order so the record
// bytes do not depend on map iteration.
//
// sess.mu must be held.
func (sess *mqttSession) acqReleasesSortedLocked() []mqttSharedAcqRef {
	if len(sess.acqReleases) == 0 {
		return nil
	}
	out := make([]mqttSharedAcqRef, 0, len(sess.acqReleases))
	for _, r := range sess.acqReleases {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Acq < out[j].Acq })
	return out
}

// mqttAcqReleaseSet turns a persisted verdict list back into the session's map.
func mqttAcqReleaseSet(list []mqttSharedAcqRef) map[string]mqttSharedAcqRef {
	if len(list) == 0 {
		return nil
	}
	out := make(map[string]mqttSharedAcqRef, len(list))
	for _, r := range list {
		out[r.Acq] = r
	}
	return out
}

// executeOwnVerdicts executes every release verdict this session carries and forgets the
// settled ones, so the next save drops them. Called after the record write that made the
// verdicts durable has been CONFIRMED — never before: a verdict executed ahead of its own
// durability is a release nothing could recover if this broker died in between.
//
// A verdict that does not settle within its episode stays; the next resume, the verdict
// pass or the closing executes it.
//
// sess.mu must NOT be held.
func (sess *mqttSession) executeOwnVerdicts(c *client) {
	sess.mu.Lock()
	pending := sess.acqReleasesSortedLocked()
	id, gen, epoch := sess.id, sess.gen, sess.sharedEpoch()
	sess.mu.Unlock()
	if len(pending) == 0 || epoch == _EMPTY_ {
		return
	}
	var settled []string
	for _, r := range pending {
		ok, err := sess.jsa.executeSharedRelease(epoch, id, gen, r)
		if err != nil && c != nil {
			c.Debugf("MQTT: releasing shared acquisition %q of session %q: %v", r.Acq, id, err)
		}
		if ok {
			settled = append(settled, r.Acq)
		}
	}
	if len(settled) == 0 {
		return
	}
	sess.mu.Lock()
	for _, a := range settled {
		delete(sess.acqReleases, a)
	}
	if len(sess.acqReleases) == 0 {
		sess.acqReleases = nil
	}
	sess.mu.Unlock()
}

// ---------------------------------------------------------------------------
// ACQUIRE
// ---------------------------------------------------------------------------

// mqttSharedAcquireOutcome is how one acquisition attempt ended.
type mqttSharedAcquireOutcome uint8

const (
	// mqttSharedAcquired: the acquisition is listed under this tenure. Proceed to J2.
	mqttSharedAcquired mqttSharedAcquireOutcome = iota + 1
	// mqttSharedAcquireFailed: nothing is held and nothing can land. The filter fails.
	mqttSharedAcquireFailed
	// mqttSharedAcquireHeldButFailed: the settlement loop was exhausted and the abort fence
	// then found the acquisition LISTED — an attempt landed after all. Abort settlement never
	// re-enters the join, so the filter fails and the caller owes the acquisition a release
	// verdict and its execution, exactly as for a J2 that failed.
	mqttSharedAcquireHeldButFailed
)

var (
	errMQTTSharedAcquireExhausted = errors.New("the shared acquisition could not be settled within its attempts")
	errMQTTSharedAcquireAborted   = errors.New("the shared acquisition was aborted after its attempts were exhausted")
)

// acquireShared performs J1 for one consumer a filter needs: group (ShareName) and subject
// name the logical group; the acquisition nonce is minted here and kept for every attempt.
//
// The incarnation is taken from the group record the batch CASes against, never chosen
// here. maxAckPending is the configured per-consumer charge, which derives the ledger bound.
//
// sess.mu must NOT be held.
func (sess *mqttSession) acquireShared(c *client, group, subject string, maxAckPending int) (mqttSharedAcquisition, mqttSharedAcquireOutcome, error) {
	sess.mu.Lock()
	tenure, id, gen, epoch := sess.sharedTenure, sess.id, sess.gen, sess.sharedEpoch()
	sess.mu.Unlock()
	if tenure.isZero() {
		return mqttSharedAcquisition{}, mqttSharedAcquireFailed, fmt.Errorf("session %q has no shared tenure: its CONNECT has not been accepted", id)
	}
	if epoch == _EMPTY_ {
		return mqttSharedAcquisition{}, mqttSharedAcquireFailed, fmt.Errorf("session %q has no account recovery epoch", id)
	}
	qkey := mqttSharedGroupKey(group, subject)
	acq := mqttNewInflightNonce()
	jsa := sess.jsa

	// The highest ledger sequence any attempt of `acq` was composed against, and whether one
	// was composed against absence. What the abort fence must move past.
	var composedMax uint64
	composedOverAbsence := false

	for attempt := 0; attempt < mqttSharedAcquireAttempts; attempt++ {
		mqttTestSharedPause("acquire:before-read:" + subject)
		l := jsa.loadSharedLedger(id, epoch, gen)
		g := jsa.loadSharedGroup(epoch, qkey)
		switch l.status {
		case mqttOutboundValid:
			if l.rec.State == mqttSharedLedgerSealed {
				// This generation is closing (3.6.1). If `acq` is listed the closing releases
				// it; if not it can never land. Nothing is written.
				return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedLedgerSealed
			}
			if l.rec.Tenure != tenure {
				// Superseded on $MQTT_shared: a newer CONNECT of this generation owns
				// acquisition. Whether `acq` is listed or not it is the successor's to
				// reconcile. No fence, no retry — a stale tenure never acquires again.
				return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedTenureSuperseded
			}
			if entry, listed := l.rec.lists(acq); listed {
				// An earlier attempt landed (its acknowledgement was lost).
				return mqttSharedAcquisition{Inc: entry.Inc, Acq: acq}, mqttSharedAcquired, nil
			}
		case mqttOutboundAbsent:
		default:
			// Unreadable: unresolved. Re-attempt after the read settles.
			continue
		}
		if g.status != mqttOutboundValid && g.status != mqttOutboundAbsent {
			continue
		}
		b, cerr := mqttComposeSharedJoin(epoch, id, gen, qkey, acq, tenure, g, l, mqttSharedLedgerMaxAcqs(maxAckPending))
		if cerr != nil {
			if errors.Is(cerr, errMQTTSharedRetiringFull) {
				// Finish the entries this broker can before refusing: a consumer already gone
				// classifies as `gone` and clears its entry. Then read again.
				if finished := jsa.finishRetiringInline(c, epoch, qkey, g); finished > 0 {
					continue
				}
				return mqttSharedAcquisition{}, mqttSharedAcquireFailed, cerr
			}
			return mqttSharedAcquisition{}, mqttSharedAcquireFailed, cerr
		}
		if l.status == mqttOutboundAbsent {
			composedOverAbsence = true
		} else if l.seq > composedMax {
			composedMax = l.seq
		}
		mqttTestSharedPause("acquire:before-send:" + subject)
		res := jsa.commitSharedLifecycleBatch(b)
		switch res.Outcome {
		case mqttSharedWriteLanded:
			return mqttSharedAcquisition{Inc: b.Acq.Inc, Acq: acq}, mqttSharedAcquired, nil
		case mqttSharedWriteRejected:
			// A CAS refused it and nothing was stored. Re-read and re-compose the IDENTICAL
			// acquisition against the fresh sequences (project_record_rejection_probe).
			continue
		default:
			// Ambiguous: the batch may still land. The re-read at the top decides — listed
			// means it landed; unlisted at an unchanged sequence means both attempts contend
			// for one CAS; unlisted at a moved sequence means every issued attempt is excluded
			// and this is a fresh attempt of the same nonce.
			continue
		}
	}

	// EXHAUSTED. Fail the filter and settle `acq` by a fence that EXISTS. From here no attempt
	// of `acq` is ever issued again.
	return sess.abortSharedAcquisition(id, gen, epoch, tenure, acq, composedMax, composedOverAbsence)
}

// abortSharedAcquisition is the abort settlement of one exhausted acquisition: a fence over
// the ledger (a byte-identical BUMP over a present ledger under our tenure, a create-only INIT
// over an absent one), then consult 2's outcome table over the ledger as read afterwards.
func (sess *mqttSession) abortSharedAcquisition(id, gen, epoch string, tenure mqttSharedTenure, acq string,
	composedMax uint64, composedOverAbsence bool) (mqttSharedAcquisition, mqttSharedAcquireOutcome, error) {

	jsa := sess.jsa
	fenced := false
	for attempt := 0; attempt < mqttSharedAcquireAttempts; attempt++ {
		l := jsa.loadSharedLedger(id, epoch, gen)
		switch l.status {
		case mqttOutboundAbsent:
			if fenced {
				// The init fence landed and the ledger reads absent afterwards? Impossible for
				// a permanent record; treat as unresolved and read again.
				continue
			}
			b, cerr := mqttComposeSharedInstall(epoch, id, gen, tenure, l)
			if cerr != nil || b == nil {
				return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedAcquireAborted
			}
			res := jsa.commitSharedLifecycleBatch(b)
			if res.Outcome == mqttSharedWriteLanded {
				// Every outstanding attempt expecting absence is excluded for ever, and no attempt
				// was composed against a later sequence: abort settlement issues none.
				return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedAcquireAborted
			}
			// Refused or ambiguous: something exists, or may. The re-read decides.
			fenced = res.Outcome == mqttSharedWriteAmbiguous
			continue
		case mqttOutboundValid:
		default:
			continue
		}
		rec := l.rec
		if rec.State == mqttSharedLedgerSealed {
			// Excluded or already released, or the closing owns it if listed.
			return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedLedgerSealed
		}
		if rec.Tenure != tenure {
			// Superseded: the successor's installation excluded our attempts and owns `acq`
			// if listed. Nothing written.
			return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedTenureSuperseded
		}
		if entry, listed := rec.lists(acq); listed {
			// An attempt landed after all. The filter still fails; the caller writes the
			// release verdict and executes it.
			return mqttSharedAcquisition{Inc: entry.Inc, Acq: acq}, mqttSharedAcquireHeldButFailed, errMQTTSharedAcquireAborted
		}
		// Unlisted. SETTLED when the sequence is past every sequence an attempt was composed
		// against — the fence landed, or a later write did. An attempt composed over absence is
		// excluded by the record's very presence.
		if l.seq > composedMax && (composedOverAbsence || composedMax > 0 || fenced) {
			return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedAcquireAborted
		}
		if composedMax == 0 && !composedOverAbsence {
			// No attempt was ever sent (every read was unreadable). Nothing to fence.
			return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedAcquireExhausted
		}
		b, cerr := mqttComposeSharedBump(epoch, id, gen, tenure, l)
		if cerr != nil {
			return mqttSharedAcquisition{}, mqttSharedAcquireFailed, cerr
		}
		res := jsa.commitSharedLifecycleBatch(b)
		if res.Outcome == mqttSharedWriteLanded {
			return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedAcquireAborted
		}
		fenced = fenced || res.Outcome == mqttSharedWriteAmbiguous
		// Refused: the subject moved — an attempt may have landed. Ambiguous: the fence may
		// have. Either way the re-read decides.
	}
	return mqttSharedAcquisition{}, mqttSharedAcquireFailed, errMQTTSharedAcquireExhausted
}

// ---------------------------------------------------------------------------
// RELEASE
// ---------------------------------------------------------------------------

// executeSharedRelease is L1 (and L2 when L1 retires the incarnation) for one verdict.
//
// It reports SETTLED when the ledger, read after this executor's own batch or after its CAS
// refusal, no longer lists the acquisition — the only writers that remove an entry are
// executors of that entry's own verdict and the closing after it sealed, so "unlisted" after
// our own write or refusal is our release having landed, ours or another executor's. An
// absent ledger settles it too: the ledger is permanent, so absence means no acquisition of
// this generation ever landed. An unreadable record settles nothing.
//
// It is the ONE route to L1, and it runs the release fence first (shared_release_fence.go): an
// OPEN ledger's acquisition is closed, the authority fenced and the inventory checked for a
// dependant still owing delivery from the source before L1 is composed; a SEALED ledger, or a
// generation whose discarding closure stands, needs no fence. errMQTTSharedReleaseWithheld reports
// a dependant: nothing is released and the verdict stands.
func (jsa *mqttJSA) executeSharedRelease(epoch, id, gen string, acq mqttSharedAcqRef) (bool, error) {
	var lastErr error
	fenced := false
	for attempt := 0; attempt < mqttSharedReleaseAttempts; attempt++ {
		l := jsa.loadSharedLedger(id, epoch, gen)
		switch l.status {
		case mqttOutboundAbsent:
			return true, nil
		case mqttOutboundValid:
		default:
			lastErr = fmt.Errorf("the ledger of generation %q reads %v", gen, l.status)
			continue
		}
		if _, listed := l.rec.lists(acq.Acq); !listed {
			return true, nil
		}
		if l.rec.State != mqttSharedLedgerSealed && !fenced {
			// STEP 1: close. Done only when a read shows it closed, so the re-read at the top
			// decides a refused or ambiguous close.
			if !l.rec.isClosed(acq.Acq) {
				b, cerr := mqttComposeSharedClose(epoch, id, gen, acq, l)
				if cerr != nil {
					return false, cerr
				}
				mqttTestSharedPause("release:before-close:" + acq.Acq)
				if res := jsa.commitSharedLifecycleBatch(b); res.Outcome != mqttSharedWriteLanded {
					lastErr = res.Err
					continue
				}
			}
			mqttTestSharedPause("release:after-step1:" + acq.Acq)
			// STEP 1b: under a committed discarding closure no admission or output acquisition of
			// the generation can land, and its recovery owner ends every exchange. Anything but a
			// valid discarding closure — absent, preserving, unreadable — takes the fence.
			if jsa.requireDiscardingClosure(id, epoch, gen) != nil {
				// STEP 2, then STEP 3.
				if ferr := jsa.fenceSharedRelease(id, epoch, gen); ferr != nil {
					return false, ferr
				}
				mqttTestSharedPause("release:after-fence:" + acq.Acq)
				if derr := jsa.sharedReleaseDependants(id, epoch, gen, acq); derr != nil {
					return false, derr
				}
			}
			// The fence holds for the rest of the episode: the acquisition stays closed, so no
			// admission naming it can land after this point. L1's own CAS retries re-read only.
			fenced = true
			continue
		}
		g := jsa.loadSharedGroup(epoch, acq.Group)
		if g.status != mqttOutboundValid {
			lastErr = fmt.Errorf("the group record of %q reads %v", acq.Group, g.status)
			continue
		}
		b, cerr := mqttComposeSharedRelease(epoch, id, gen, acq, g, l)
		if cerr != nil {
			// The consistency refusal included: nothing is released on a contradiction.
			return false, cerr
		}
		mqttTestSharedPause("release:before-send:" + acq.Acq)
		res := jsa.commitSharedLifecycleBatch(b)
		switch res.Outcome {
		case mqttSharedWriteLanded:
			if b.GroupNext.Live == nil {
				// THE LAST HOLDER LEFT: the incarnation is retiring, and this executor performs
				// the retirement. A failure here leaves the retiring entry standing for the
				// sweep; the release itself is settled.
				jsa.retireSharedIncarnation(epoch, acq.Group, acq.Inc)
			}
			return true, nil
		case mqttSharedWriteRejected:
			lastErr = res.Err
			continue
		default:
			lastErr = res.Err
			continue
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("the release of acquisition %q did not settle within one episode", acq.Acq)
	}
	return false, lastErr
}

// mqttSharedRetireOutcome classifies one consumer deletion, as the affine executor does.
type mqttSharedRetireOutcome uint8

const (
	mqttSharedRetireRemoved mqttSharedRetireOutcome = iota + 1
	mqttSharedRetireGone
	mqttSharedRetireAmbiguous
)

// deleteSharedIncarnationConsumer performs the physical act and classifies its OWN result.
// An ambiguous result records nothing and leaves the retiring entry for the sweep.
func (jsa *mqttJSA) deleteSharedIncarnationConsumer(qkey string, inc uint64) mqttSharedRetireOutcome {
	if h := mqttTestSharedDeleteHookV.Load(); h != nil {
		if out, override := (*h)(qkey, inc); override {
			return out
		}
	}
	durable := mqttSharedConsumerDurablePrefix + qkey + "_" + mqttSharedIncToken(inc)
	_, err := jsa.deleteConsumer(mqttStreamName, durable, false)
	switch {
	case err == nil:
		return mqttSharedRetireRemoved
	case !isErrorOtherThan(err, JSConsumerNotFoundErr):
		return mqttSharedRetireGone
	default:
		return mqttSharedRetireAmbiguous
	}
}

// retireSharedIncarnation is L2: delete the incarnation's consumer, classify, and record the
// completion by removing the retiring entry. Repeat-safe: a repeat finds the consumer gone.
func (jsa *mqttJSA) retireSharedIncarnation(epoch, qkey string, inc uint64) error {
	switch jsa.deleteSharedIncarnationConsumer(qkey, inc) {
	case mqttSharedRetireAmbiguous:
		return fmt.Errorf("deleting the consumer of incarnation %d of %q was unresolved; the retiring entry stands for the sweep", inc, qkey)
	}
	var lastErr error
	for attempt := 0; attempt < mqttSharedReleaseAttempts; attempt++ {
		g := jsa.loadSharedGroup(epoch, qkey)
		if g.status != mqttOutboundValid {
			lastErr = fmt.Errorf("the group record of %q reads %v", qkey, g.status)
			continue
		}
		if !g.rec.isRetiring(inc) {
			return nil
		}
		b, cerr := mqttComposeSharedRetireDone(epoch, qkey, inc, g)
		if cerr != nil {
			return cerr
		}
		res := jsa.commitSharedLifecycleBatch(b)
		if res.Outcome == mqttSharedWriteLanded {
			return nil
		}
		lastErr = res.Err
	}
	return lastErr
}

// finishRetiringInline is what a joiner does when the group's retiring list is full: it
// attempts every listed retirement and reports how many it completed. A consumer already gone
// classifies as `gone` and clears its entry; an ambiguous delete leaves its entry.
func (jsa *mqttJSA) finishRetiringInline(c *client, epoch, qkey string, g mqttSharedGroupLoad) int {
	if g.status != mqttOutboundValid {
		return 0
	}
	finished := 0
	for _, inc := range g.rec.Retiring {
		if err := jsa.retireSharedIncarnation(epoch, qkey, inc); err != nil {
			if c != nil {
				c.Debugf("MQTT: finishing retirement of incarnation %d of %q inline: %v", inc, qkey, err)
			}
			continue
		}
		finished++
	}
	return finished
}

// ---------------------------------------------------------------------------
// INSTALL, SEAL, T3, T4
// ---------------------------------------------------------------------------

// installSharedTenure is a resume's step 1 after its accept save landed: it projects the
// record's ownership decision onto the ledger under the non-regression rule. It returns the
// ledger as the installation left it (the acquisitions this tenure now owns the disposition
// of), or a typed refusal — sealed, or superseded — that refuses the CONNECT.
func (jsa *mqttJSA) installSharedTenure(id, epoch, gen string, tenure mqttSharedTenure) (*mqttSharedLedger, error) {
	var lastErr error
	for attempt := 0; attempt < mqttSharedAcquireAttempts; attempt++ {
		l := jsa.loadSharedLedger(id, epoch, gen)
		if l.status != mqttOutboundValid && l.status != mqttOutboundAbsent {
			lastErr = fmt.Errorf("the ledger of generation %q reads %v", gen, l.status)
			continue
		}
		b, cerr := mqttComposeSharedInstall(epoch, id, gen, tenure, l)
		if cerr != nil {
			return nil, cerr
		}
		if b == nil {
			// Already installed — a lost acknowledgement of an earlier attempt.
			return l.rec, nil
		}
		mqttTestSharedPause("install:before-send")
		res := jsa.commitSharedLifecycleBatch(b)
		switch res.Outcome {
		case mqttSharedWriteLanded:
			return b.LedgerNext, nil
		default:
			// Refused or ambiguous: re-read. Our tenure standing means installed; an older
			// standing means re-attempt; a newer means superseded; a seal means closing.
			lastErr = res.Err
			continue
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("installing the shared tenure of generation %q did not settle within one episode", gen)
	}
	return nil, lastErr
}

// sealSharedLedger is T2, the closing's fence: authorized by the teardown claim the caller
// holds, never by tenure. It returns the sealed ledger, whose acquisitions T3 releases.
func (jsa *mqttJSA) sealSharedLedger(id, epoch, gen string) (*mqttSharedLedger, error) {
	var lastErr error
	for attempt := 0; attempt < mqttSharedAcquireAttempts; attempt++ {
		l := jsa.loadSharedLedger(id, epoch, gen)
		if l.status != mqttOutboundValid && l.status != mqttOutboundAbsent {
			lastErr = fmt.Errorf("the ledger of generation %q reads %v", gen, l.status)
			continue
		}
		b, cerr := mqttComposeSharedSeal(epoch, id, gen, l)
		if cerr != nil {
			return nil, cerr
		}
		if b == nil {
			return l.rec, nil
		}
		mqttTestSharedPause("seal:before-send")
		res := jsa.commitSharedLifecycleBatch(b)
		if res.Outcome == mqttSharedWriteLanded {
			return b.LedgerNext, nil
		}
		// Refused (a tenure installation or a join landed first — the re-read seals what it
		// preserved) or ambiguous (the re-read decides).
		lastErr = res.Err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("sealing the shared ledger of generation %q did not settle within one episode", gen)
	}
	return nil, lastErr
}

// releaseSealedAcquisitions is T3: L1 and L2 for every acquisition the sealed ledger lists.
// An entry that cannot be released (its group record unreadable) is SKIPPED and reported;
// the gate below withholds on it. Repeat-safe: a repeat finds the released ones unlisted.
func (jsa *mqttJSA) releaseSealedAcquisitions(id, epoch, gen string, sealed *mqttSharedLedger) error {
	if sealed == nil {
		return nil
	}
	var errs []error
	for _, a := range sealed.Acqs {
		if _, err := jsa.executeSharedRelease(epoch, id, gen, a); err != nil {
			errs = append(errs, fmt.Errorf("acquisition %q: %w", a.Acq, err))
		}
	}
	return errors.Join(errs...)
}

// sharedClosingComplete is T4: the ledger reads SEALED with no acquisition listed. Anything
// else withholds the record's release — an unreadable ledger, an open one, a listed entry.
func (jsa *mqttJSA) sharedClosingComplete(id, epoch, gen string) error {
	l := jsa.loadSharedLedger(id, epoch, gen)
	switch l.status {
	case mqttOutboundValid:
	case mqttOutboundAbsent:
		return fmt.Errorf("the shared acquisition ledger of generation %q is absent after its seal was written", gen)
	default:
		return fmt.Errorf("the shared acquisition ledger of generation %q reads %v, which licenses no release", gen, l.status)
	}
	if l.rec.State != mqttSharedLedgerSealed {
		return fmt.Errorf("the shared acquisition ledger of generation %q is not sealed", gen)
	}
	if len(l.rec.Acqs) != 0 {
		return fmt.Errorf("the shared acquisition ledger of generation %q still lists %d acquisition(s) after the closing's release: %v",
			gen, len(l.rec.Acqs), l.rec.Acqs)
	}
	return nil
}

// closeSharedGeneration runs T2, T3 and T4 for one retiring generation, on both teardown
// routes. It is called between recordGenRetirement and deleteTeardownTargets, naming the
// RETIRING generation captured before any reset. An error withholds the record's release
// with the record still marked, so the adoption repeats it.
func (jsa *mqttJSA) closeSharedGeneration(id, epoch, gen string) error {
	sealed, err := jsa.sealSharedLedger(id, epoch, gen)
	if err != nil {
		return fmt.Errorf("sealing the shared acquisitions of generation %q: %w", gen, err)
	}
	mqttTestSharedPause("closing:after-seal")
	if rerr := jsa.releaseSealedAcquisitions(id, epoch, gen, sealed); rerr != nil {
		// Reported, and the gate decides: an entry that could not be released leaves the
		// ledger non-empty and the gate withholds.
		if gerr := jsa.sharedClosingComplete(id, epoch, gen); gerr != nil {
			return fmt.Errorf("releasing the shared acquisitions of generation %q: %v; %w", gen, rerr, gerr)
		}
		return nil
	}
	return jsa.sharedClosingComplete(id, epoch, gen)
}

// ---------------------------------------------------------------------------
// The SUBSCRIBE and CONNECT halves: acquire a filter, install and reconcile a resume
// ---------------------------------------------------------------------------

// acquireSharedFilter is J0 and J1 for one shared filter on the ACQUIRE arm: it admits the whole
// filter against the charge ceiling, then reserves one acquisition per consumer the filter
// needs — its own group, and the level-up parent of a multi-level filter — each under this
// CONNECT's tenure.
//
// It returns the membership the record must carry (J2), or an error that fails the filter.
// When an acquisition LANDED and the filter still fails — an attempt landed after its
// settlement was exhausted, or the companion failed after the main half acquired — the
// membership is returned WITH the error, so the caller writes a release verdict for it and
// executes it exactly as for a failed membership save.
func (sess *mqttSession) acquireSharedFilter(c *client, f *mqttFilter, subject string) (*mqttSharedMembership, error) {
	opts := c.srv.getOpts()
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}
	needUp := mqttNeedSubForLevelUp(subject)

	// J0 — ADMISSION FOR THE WHOLE FILTER, ahead of any reservation. A consumer this session
	// already holds an acquisition on (through another key of the same group — the overlapping
	// level-up case) is charged already; every other consumer the filter needs costs one
	// charge, and the ceiling is applied to their SUM, so one remaining charge refuses a
	// multi-level filter before either of its acquisitions is reserved.
	heldGroups := make(map[string]struct{})
	sess.mu.Lock()
	tmaxack := sess.tmaxack
	for key, m := range sess.sharedSubs {
		g, sub, ok := mqttParseSharedKey(key)
		if !ok {
			continue
		}
		heldGroups[mqttSharedGroupKey(g, sub)] = struct{}{}
		if m.Up != nil && mqttNeedSubForLevelUp(sub) {
			heldGroups[mqttSharedGroupKey(g, sub[:len(sub)-2])] = struct{}{}
		}
	}
	sess.mu.Unlock()
	needed := 0
	if _, held := heldGroups[mqttSharedGroupKey(f.group, subject)]; !held {
		needed++
	}
	if needUp {
		if _, held := heldGroups[mqttSharedGroupKey(f.group, subject[:len(subject)-2])]; !held {
			needed++
		}
	}
	if after := tmaxack + needed*maxAckPending; after > mqttMaxAckTotalLimit {
		return nil, fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
			after, mqttMaxAckTotalLimit)
	}

	// J1 — one acquisition per consumer, main first.
	main, out, err := sess.acquireShared(c, f.group, subject, maxAckPending)
	switch out {
	case mqttSharedAcquired:
	case mqttSharedAcquireHeldButFailed:
		return &mqttSharedMembership{QoS: f.qos, Main: main}, err
	default:
		return nil, err
	}
	m := &mqttSharedMembership{QoS: f.qos, Main: main}
	if !needUp {
		return m, nil
	}
	up, out2, err2 := sess.acquireShared(c, f.group, subject[:len(subject)-2], maxAckPending)
	switch out2 {
	case mqttSharedAcquired:
		m.Up = &up
		return m, nil
	case mqttSharedAcquireHeldButFailed:
		m.Up = &up
		return m, err2
	default:
		// The main half is held and the companion is not: the filter fails and the main
		// acquisition is owed its release.
		return m, err2
	}
}

// installAndReconcileShared is a RESUME's disposition step, run after its accept save landed
// (the tenure is that save's sequence) and before anything is served:
//
//  1. install the tenure on the generation's ledger (refused as superseded or sealed ⇒ the
//     CONNECT is refused);
//  2. execute the release verdicts the record carries;
//  3. reconcile the ledger AS THE INSTALLATION LEFT IT: an acquisition the record neither holds
//     nor has a verdict for is an UNCOMMITTED join, cancelled by a SUBSEQUENT record CAS (never
//     by a ledger write, and never on the accept save that preceded the ledger read) and then
//     released; a record acquisition the ledger does not list is impossible by construction
//     and refuses the CONNECT;
//  4. group consistency: every committed acquisition names the incarnation its group serves.
//
// Nothing destructive here runs from a snapshot: every removal runs from a verdict this
// broker wrote under the record CAS it owns, which is what two brokers legitimately resuming
// ONE generation contend on.
func (sess *mqttSession) installAndReconcileShared(c *client, tok recordTx) error {
	sess.mu.Lock()
	id, gen, epoch, tenure := sess.id, sess.gen, sess.sharedEpoch(), sess.sharedTenure
	sess.mu.Unlock()
	if epoch == _EMPTY_ {
		return fmt.Errorf("resuming session %q: no account recovery epoch", id)
	}
	if tenure.isZero() {
		return fmt.Errorf("resuming session %q: its tenure was not minted", id)
	}
	led, err := sess.jsa.installSharedTenure(id, epoch, gen, tenure)
	if err != nil {
		return fmt.Errorf("resuming session %q: installing its shared tenure: %w", id, err)
	}
	mqttTestSharedPause("resume:after-install")

	sess.executeOwnVerdicts(c)

	// The record's view: every acquisition it holds or already has a verdict for.
	sess.mu.Lock()
	named := make(map[string]struct{})
	var committed []mqttSharedAcqRef
	for key, m := range sess.sharedSubs {
		g, sub, ok := mqttParseSharedKey(key)
		if !ok {
			continue
		}
		named[m.Main.Acq] = struct{}{}
		committed = append(committed, mqttSharedAcqRef{Group: mqttSharedGroupKey(g, sub), Inc: m.Main.Inc, Acq: m.Main.Acq})
		if m.Up != nil && mqttNeedSubForLevelUp(sub) {
			named[m.Up.Acq] = struct{}{}
			committed = append(committed, mqttSharedAcqRef{Group: mqttSharedGroupKey(g, sub[:len(sub)-2]), Inc: m.Up.Inc, Acq: m.Up.Acq})
		}
	}
	for a := range sess.acqReleases {
		named[a] = struct{}{}
	}
	sess.mu.Unlock()

	var cancel []mqttSharedAcqRef
	if led != nil {
		for _, a := range led.Acqs {
			if _, ok := named[a.Acq]; !ok {
				cancel = append(cancel, a)
			}
		}
	}
	for _, a := range committed {
		if _, listed := led.lists(a.Acq); !listed {
			return fmt.Errorf("resuming session %q: its record holds shared acquisition %q on group %q, which the "+
				"generation's ledger does not list; a membership without an acquisition behind it is a shape this "+
				"broker never writes", id, a.Acq, a.Group)
		}
	}
	if len(cancel) > 0 {
		// CANCEL BY RECORD WRITE. The verdicts go into the record under this CONNECT's transaction
		// at the sequence it owns; exactly one of {this write, the originating tenure's membership
		// save naming the same acquisition} lands, and the loser is refused before it acts.
		sess.mu.Lock()
		if sess.acqReleases == nil {
			sess.acqReleases = make(map[string]mqttSharedAcqRef)
		}
		for _, a := range cancel {
			sess.acqReleases[a.Acq] = a
		}
		sess.mu.Unlock()
		mqttTestSharedPause("resume:before-cancel-save")
		if serr := sess.save(tok); serr != nil {
			return fmt.Errorf("resuming session %q: cancelling %d uncommitted shared acquisition(s): %w", id, len(cancel), serr)
		}
		sess.executeOwnVerdicts(c)
	}

	// GROUP CONSISTENCY, read from the group records: a committed acquisition on an incarnation
	// its group no longer serves is a contradiction, refused rather than repaired.
	for _, a := range committed {
		g := sess.jsa.loadSharedGroup(epoch, a.Group)
		if g.status != mqttOutboundValid {
			return fmt.Errorf("resuming session %q: the group record of %q reads %v (%v), so acquisition %q cannot be verified",
				id, a.Group, g.status, g.err, a.Acq)
		}
		if g.rec.Live == nil || g.rec.Live.Inc != a.Inc {
			return fmt.Errorf("resuming session %q: its acquisition %q holds incarnation %d of group %q, which the group "+
				"does not serve (%v): a committed acquisition on a non-live incarnation is an inconsistency this broker "+
				"refuses to serve", id, a.Acq, a.Inc, a.Group, g.rec.Live)
		}
	}
	return nil
}
