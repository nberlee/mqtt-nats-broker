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
	"fmt"
	"strings"
)

// The session side of the custody ledger (F-105 cutover, row 5): the capacity computation,
// the identifier conjunction's ledger term, the reconstruction census, and the single-flight
// observer that is the only thing that ever releases an exchange's charge.

// mqttCustodyReobserveBudget bounds one re-observation pass. Every target is restamped when it
// is attempted, so a pass never re-reads the same stable targets while a stale one waits.
var mqttCustodyReobserveBudget = 8

// ---------------------------------------------------------------------------
// Capacity
// ---------------------------------------------------------------------------

// receiveMaxOccupancyLocked is the Receive Maximum occupancy: every custody charge the ledger
// holds, plus every scheduling entry the ledger does NOT own (the shared and affine classes
// until 7a, retained until 7c, and an ordinary entry between tracking and its conversion).
//
// An entry is the ledger's when it is marked custody, or when the ledger holds its
// IDENTIFIER. The mark is what keeps a released charge from resurrecting through an entry that
// outlived it. The identifier covers entries installed with no lineage to mark by — a
// PUBREL-derived reservation recovery restores, a PUBREL delivered after a restart with no
// resident entry to carry the mark from — which would otherwise be charged a second time
// beside the exchange the ledger already charges. It cannot hide a different obligation: the
// allocator never hands out an identifier the ledger holds, so an entry on one is the ledger's
// own exchange's.
//
// Lock held on entry.
func (sess *mqttSession) receiveMaxOccupancyLocked() int {
	n := sess.custody.charges()
	for pi, p := range sess.pendingPublish {
		if !sess.ledgerOwnsEntryLocked(pi, p) {
			n++
		}
	}
	for pi, p := range sess.pendingPubRel {
		if !sess.ledgerOwnsEntryLocked(pi, p) {
			n++
		}
	}
	return n
}

// ledgerOwnsEntryLocked reports whether a scheduling entry's Receive Maximum charge is the
// custody ledger's rather than the projection's.
//
// Lock held on entry.
func (sess *mqttSession) ledgerOwnsEntryLocked(pi uint16, p *mqttPending) bool {
	return p.custody || sess.custody.holdsPI(pi)
}

// capacityAvailableLocked is how many NEW QoS 1/2 PUBLISHes may be admitted now. Two budgets,
// deliberately not one number: the client's Receive Maximum, which is a statement about what
// the CLIENT holds unacknowledged (Spec5 [4.9]) and counts custody; and the local scheduling
// budget, pendingPublish against MaxAckPending, which only paces this broker's own scheduling
// (JetStream enforces the consumer's MaxAckPending itself). An incomplete reconstruction
// admits nothing new.
//
// Lock held on entry.
func (sess *mqttSession) capacityAvailableLocked() int {
	if sess.custody.withholdsNew() {
		return 0
	}
	avail := int(sess.maxp) - len(sess.pendingPublish)
	if sess.rmax != 0 {
		if r := int(sess.rmax) - sess.receiveMaxOccupancyLocked(); r < avail {
			avail = r
		}
	}
	if avail < 0 {
		return 0
	}
	return avail
}

// replayAllowanceLocked is the resume pacing pass's allowance for re-offering already-tracked
// deliveries (the replay set). Each replay member is ALREADY counted at most once — by the
// ledger when it is a custody entry the ledger still charges, by the projection when it is not
// a custody entry — so exactly those counts are taken back out, and every other known
// occupancy stays counted. An incomplete reconstruction does not zero it: a replay adds no
// occupancy at the client, and the unknown remainder only withholds NEW work.
//
// Lock held on entry.
func (sess *mqttSession) replayAllowanceLocked(replay map[uint16]*mqttPending) int {
	counted := 0
	for pi, p := range replay {
		switch {
		case !sess.ledgerOwnsEntryLocked(pi, p):
			counted++ // counted by the projection
		case p.custody && sess.custody.countsEntry(p):
			counted++ // counted by the ledger, by its lineage or its allocation
		case !p.custody && sess.custody.chargesPI(pi):
			counted++ // counted by the ledger through the charged holder of its identifier
		}
	}
	avail := int(sess.maxp) - (len(sess.pendingPublish) - len(replay))
	if sess.rmax != 0 {
		if r := int(sess.rmax) - (sess.receiveMaxOccupancyLocked() - counted); r < avail {
			avail = r
		}
	}
	if avail < 0 {
		return 0
	}
	return avail
}

// resumeDeferredAllowanceLocked is how many deferred, never-transmitted deliveries the resume
// pacing pass may re-offer after replaying `replayed` of its allowance: the remainder, and none
// at all while the custody reconstruction is incomplete — a deferred delivery is NEW work, and
// some occupancy is unknown.
//
// Lock held on entry.
func (sess *mqttSession) resumeDeferredAllowanceLocked(replayAllowance, replayed int) int {
	if sess.custody.withholdsNew() {
		return 0
	}
	if rem := replayAllowance - replayed; rem > 0 {
		return rem
	}
	return 0
}

// ---------------------------------------------------------------------------
// Admission conversion
// ---------------------------------------------------------------------------

// openAdmissionCustodyLocked converts a tracked ordinary allocation to ledger custody: in ONE
// critical section the admission reservation is created and the scheduling entry marked as
// custody, so the slot moves from the projection to the ledger and is never uncounted. It
// refuses when the generation moved or the entry is gone or names another allocation: the
// delivery was abandoned between tracking and admission, and nothing may be submitted for it.
//
// Lock held on entry.
func (sess *mqttSession) openAdmissionCustodyLocked(gen string, pi uint16, nonce string) bool {
	if sess.gen != gen {
		return false
	}
	p := sess.pendingPublish[pi]
	if p == nil || p.regNonce != nonce {
		return false
	}
	if sess.custody == nil {
		// A session object built without the constructor (test rigs) has no ledger yet; it
		// gets its generation's, exactly as setIncarnationLocked would have installed.
		sess.custody = newMQTTCustodyLedger(gen)
	}
	if !sess.custody.openAdmission(gen, nonce, pi) {
		return false
	}
	p.custody = true
	return true
}

// ---------------------------------------------------------------------------
// Attempt settlement
// ---------------------------------------------------------------------------

// settleCustodyHandle decides one submitted attempt: resolve, and if unresolved take the
// attempt's outcome subject with the create-only abort and resolve again — the re-read, never
// the abort's own result, decides. Unresolved after that concludes nothing.
//
// Session lock NOT held on entry.
func (sess *mqttSession) settleCustodyHandle(h *mqttTransitionHandle) mqttTransitionVerdict {
	jsa := sess.jsa
	hook := mqttTestCustodyResolveHook(sess)
	v := jsa.resolveTransitionHandle(h)
	if hook != nil {
		if hv, ok := hook(h, mqttCustodyStepResolve); ok {
			v = hv
		}
	}
	if v == mqttTransitionUnresolved {
		if hook != nil {
			if hv, ok := hook(h, mqttCustodyStepAbort); ok {
				return hv
			}
		}
		_ = jsa.abortTransition(&h.attempt)
		v = jsa.resolveTransitionHandle(h)
	}
	return v
}

// recordAdmissionAttempt records one attempt of an admission's write and settles it. A
// successful write is Landed; a failure with no handle sent nothing and records nothing; a
// failure with a handle is recorded as unsettled FIRST (so a reconciler racing it can see it)
// and then settled. Returns whether the attempt's exchange is known to have landed.
//
// Session lock NOT held on entry.
func (sess *mqttSession) recordAdmissionAttempt(gen, nonce, lin string, out mqttOpOutcome, err error) bool {
	if err == nil {
		sess.mu.Lock()
		sess.custody.attemptLanded(gen, nonce, nil, lin)
		sess.mu.Unlock()
		return true
	}
	if out.Handle == nil {
		return false
	}
	a := mqttCustodyAttempt{handle: out.Handle, lin: lin}
	sess.mu.Lock()
	sess.custody.recordAttempt(gen, nonce, a)
	sess.mu.Unlock()
	return sess.settleCustodyAttempt(gen, nonce, a)
}

// settleCustodyAttempt settles one recorded attempt. Returns true when it Landed.
//
// Session lock NOT held on entry.
func (sess *mqttSession) settleCustodyAttempt(gen, nonce string, a mqttCustodyAttempt) bool {
	switch sess.settleCustodyHandle(a.handle) {
	case mqttTransitionLanded, mqttTransitionSuperseded:
		sess.mu.Lock()
		sess.custody.attemptLanded(gen, nonce, a.handle, a.lin)
		sess.mu.Unlock()
		return true
	case mqttTransitionNeverLanded:
		sess.mu.Lock()
		released := sess.custody.attemptNeverLanded(gen, nonce, a.handle)
		sess.mu.Unlock()
		if released {
			sess.releaseDeferredAfterCustody()
		}
	}
	return false
}

// finishAdmissionCustody ends an admission's ladder: its reservation goes once nothing it
// submitted is unsettled. Deferred on every return of admitInflightPublish.
//
// Session lock NOT held on entry.
func (sess *mqttSession) finishAdmissionCustody(gen, nonce string) {
	sess.mu.Lock()
	released := sess.custody.finishAdmission(gen, nonce)
	sess.mu.Unlock()
	if released {
		sess.releaseDeferredAfterCustody()
	}
}

// releaseDeferredAfterCustody re-offers withheld deliveries after a charge was released,
// exactly as a map removal did before the ledger owned the charge.
//
// Session lock NOT held on entry.
func (sess *mqttSession) releaseDeferredAfterCustody() {
	sess.mu.Lock()
	subjects := sess.deferredNaksLocked()
	sess.mu.Unlock()
	for _, s := range subjects {
		sess.jsa.sendMsg(s, AckNak)
	}
}

// ---------------------------------------------------------------------------
// Hints and the observer
// ---------------------------------------------------------------------------

// hintCustody schedules an exact observation of the exchange of lineage lin. It NEVER
// releases: a continuation that stopped (an acknowledgement advance whatever its outcome, a
// drain, a withdrawal, a replacement) says only that the exchange may have ended, and only the
// exchange itself says whether it did.
//
// Session lock NOT held on entry.
func (sess *mqttSession) hintCustody(gen, lin string) {
	if lin == _EMPTY_ {
		return
	}
	sess.mu.Lock()
	if !sess.custody.hasExchange(gen, lin) {
		sess.mu.Unlock()
		return
	}
	if sess.custodyHints == nil {
		sess.custodyHints = make(map[string]struct{})
	}
	sess.custodyHints[lin] = struct{}{}
	start := sess.startCustodyObserverLocked()
	sess.mu.Unlock()
	if start {
		go sess.runCustodyObserver()
	}
}

// requestCustodyPassLocked asks for a bounded re-observation pass. Called by the gates when
// they withhold: capacity exhausted, or an identifier withheld. The periodic driver is the
// existing AckWait re-offer of a withheld delivery, which re-enters the gate; there is no
// timer. The caller starts the observer when this returns true.
//
// Lock held on entry.
func (sess *mqttSession) requestCustodyPassLocked() bool {
	if sess.custody == nil || sess.jsa == nil || sess.asm == nil {
		return false
	}
	sess.custodyPassWanted = true
	return sess.startCustodyObserverLocked()
}

// kickCustodyPassLocked is requestCustodyPassLocked for a caller that cannot start a goroutine
// after unlocking: it starts it itself. The observer takes the lock only after it runs.
//
// Lock held on entry.
func (sess *mqttSession) kickCustodyPassLocked() {
	if sess.requestCustodyPassLocked() {
		go sess.runCustodyObserver()
	}
}

// Lock held on entry.
func (sess *mqttSession) startCustodyObserverLocked() bool {
	if sess.custodyObserving {
		return false
	}
	sess.custodyObserving = true
	return true
}

// runCustodyObserver is the single-flight observer. It drains hints first, then at most one
// bounded pass per request, and exits when nothing is asked of it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runCustodyObserver() {
	for {
		sess.mu.Lock()
		gen := sess.gen
		var work []mqttCustodyTarget
		census := false
		switch {
		case len(sess.custodyHints) > 0:
			for lin := range sess.custodyHints {
				work = append(work, mqttCustodyTarget{kind: mqttCustodyTargetExchange, key: lin})
			}
			sess.custodyHints = nil
		case sess.custodyPassWanted:
			sess.custodyPassWanted = false
			census = sess.custody.withholdsNew()
			work = sess.custody.targets(gen, mqttCustodyReobserveBudget)
		default:
			sess.custodyObserving = false
			sess.mu.Unlock()
			return
		}
		sess.mu.Unlock()

		released := false
		if census {
			sess.reconstructCustody(gen)
			released = true // a completed census may reopen capacity
		}
		for _, t := range work {
			if sess.observeCustodyTarget(gen, t) {
				released = true
			}
		}
		if released {
			sess.releaseDeferredAfterCustody()
		}
	}
}

// observeCustodyTarget settles one target and reports whether a charge was released.
//
// Session lock NOT held on entry.
func (sess *mqttSession) observeCustodyTarget(gen string, t mqttCustodyTarget) bool {
	as := sess.asm
	if as == nil {
		return false
	}
	switch t.kind {
	case mqttCustodyTargetExchange:
		sess.mu.Lock()
		at, ok := sess.custody.beginObservation(gen, t.key)
		if !ok {
			for _, b := range sess.abandonedAcks {
				if b.name.gen == gen && b.lin == t.key {
					ok = true
					break
				}
			}
		}
		sess.mu.Unlock()
		if !ok {
			return false
		}
		var x *mqttOutboundExchange
		if hook := mqttTestCustodyDecodeHook(sess); hook == nil || !hook(mqttExchangeSubject(getHash(sess.id), gen, t.key)) {
			var err error
			x, _, err = sess.jsa.loadOutboundExchange(sess.id, as.sessStreamEpoch, gen, t.key)
			if err != nil || x == nil || x.Nonce != t.key {
				x = nil
			}
		}
		sess.mu.Lock()
		sess.observeAbandonedAckEndLocked(x)
		released := sess.custody.applyExchangeObservation(gen, t.key, at, x)
		sess.mu.Unlock()
		return released
	case mqttCustodyTargetAdmission:
		sess.mu.Lock()
		attempts := sess.custody.unsettledAttempts(gen, t.key)
		sess.custody.touchAdmission(gen, t.key)
		sess.mu.Unlock()
		for _, a := range attempts {
			sess.settleCustodyAttempt(gen, t.key, a)
		}
		// settleCustodyAttempt already re-offered on its own release.
		return false
	case mqttCustodyTargetUncertain:
		sess.mu.Lock()
		ok := sess.custody.beginUncertainRead(gen, t.key)
		sess.mu.Unlock()
		if !ok {
			return false
		}
		x := sess.readCustodySubject(gen, t.key)
		if x == nil {
			return false
		}
		sess.mu.Lock()
		sess.custody.resolveUncertain(gen, t.key, x)
		sess.mu.Unlock()
		// Resolving an uncertainty always frees capacity or allocation in some way.
		return true
	}
	return false
}

// readCustodySubject re-reads one exchange subject individually, returning the exchange only
// when it decodes as this generation's.
//
// Session lock NOT held on entry.
func (sess *mqttSession) readCustodySubject(gen, subject string) *mqttOutboundExchange {
	nonce, ok := mqttExchangeNonceFromSubject(subject)
	if !ok {
		return nil
	}
	if hook := mqttTestCustodyDecodeHook(sess); hook != nil && hook(subject) {
		return nil
	}
	x, _, err := sess.jsa.loadOutboundExchange(sess.id, sess.asm.sessStreamEpoch, gen, nonce)
	if err != nil || x == nil {
		return nil
	}
	return x
}

// mqttExchangeNonceFromSubject extracts the exchange nonce from an exchange subject.
func mqttExchangeNonceFromSubject(subject string) (string, bool) {
	prefix := mqttF105SubjectPrefix()
	if !strings.HasPrefix(subject, prefix) {
		return _EMPTY_, false
	}
	toks := strings.Split(subject[len(prefix):], ".")
	if len(toks) != 4 || toks[3] == _EMPTY_ {
		return _EMPTY_, false
	}
	return toks[3], true
}

// ---------------------------------------------------------------------------
// Reconstruction
// ---------------------------------------------------------------------------

// mqttCustodyCensus is one completeness-aware read of a generation's exchanges: the readable
// ones, and every subject that could not be read as one (unparseable, undecodable, or
// enumerated and then absent).
type mqttCustodyCensus struct {
	live      []*mqttOutboundExchange
	uncertain []string
}

// enumerateCustodyExchanges reads the generation's exchanges for the custody ledger.
//
// It is NOT enumerateGenerationObligations, which reads actions too and treats a missing
// stream and a vanished exchange as settled emptiness: for custody both are uncertainty. A
// missing inflight stream is an error (the stream is a standing store of the account, and its
// absence is not evidence that no exchange exists), and a first page is an error.
//
// Session lock NOT held on entry.
func (sess *mqttSession) enumerateCustodyExchanges(gen string) (*mqttCustodyCensus, error) {
	as := sess.asm
	if as == nil || sess.jsa == nil {
		return nil, fmt.Errorf("session %q has no store to read its exchanges from", sess.id)
	}
	if hook := mqttTestCustodyCensusHook(sess); hook != nil {
		if err := hook(); err != nil {
			return nil, err
		}
	}
	subjects, whole, err := sess.jsa.streamSubjectsComplete(mqttInflightStreamName,
		mqttExchangeWalkFilter(sess.idHash, gen))
	if err != nil {
		return nil, fmt.Errorf("enumerating the outbound exchanges of %q/%q: %w", sess.id, gen, err)
	}
	if !whole {
		return nil, fmt.Errorf("the outbound exchanges of %q/%q came back as a first page", sess.id, gen)
	}
	out := &mqttCustodyCensus{}
	for subject := range subjects {
		nonce, ok := mqttExchangeNonceFromSubject(subject)
		if !ok {
			out.uncertain = append(out.uncertain, subject)
			continue
		}
		if hook := mqttTestCustodyDecodeHook(sess); hook != nil && hook(subject) {
			out.uncertain = append(out.uncertain, subject)
			continue
		}
		// Through the exchange's own loader, which returns every read error — an absence, an
		// undecodable body, a failed read — as an error. Each is UNCERTAIN here, by subject: an
		// enumerated exchange that then reads absent is unexplained, an undecodable one is
		// refused rather than interpreted, and a failed read is re-tried by the re-observation
		// pass. All three hold a charge and withhold allocation until an individual re-read
		// decodes the exchange; none is read as an obligation that is not there.
		x, _, lerr := sess.jsa.loadOutboundExchange(sess.id, as.sessStreamEpoch, gen, nonce)
		if lerr != nil || x == nil {
			out.uncertain = append(out.uncertain, subject)
			continue
		}
		out.live = append(out.live, x)
	}
	return out, nil
}

// reconstructCustody merges a census into the ledger. A failed census leaves it incomplete.
//
// Session lock NOT held on entry.
func (sess *mqttSession) reconstructCustody(gen string) {
	census, err := sess.enumerateCustodyExchanges(gen)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if err != nil {
		sess.custody.failReconstruction(gen)
		return
	}
	for _, x := range census.live {
		sess.observeAbandonedAckEndLocked(x)
	}
	sess.custody.mergeCensus(gen, census.live, census.uncertain, true)
}

// rebuildCustodyForResume is the resume's reconstruction, run BEFORE any register is routed
// and before delivery or allocation opens: the ledger is marked incomplete, the census merged,
// and a failure leaves it withholding.
//
// Session lock NOT held on entry.
func (sess *mqttSession) rebuildCustodyForResume(gen string) {
	sess.mu.Lock()
	sess.custody.beginReconstruction(gen)
	sess.mu.Unlock()
	sess.reconstructCustody(gen)
}

// markRecoveredCustodyLocked marks every scheduling entry recovery installed for a lineage the
// ledger charges as a custody entry, so the projection stops counting it beside the ledger.
// An entry whose exchange the census did not find keeps its scheduling count, as before.
//
// Lock held on entry.
func (sess *mqttSession) markRecoveredCustodyLocked(gen string) {
	for _, p := range sess.pendingPublish {
		if p.lin != _EMPTY_ && sess.custody.hasExchange(gen, p.lin) {
			p.custody = true
		}
	}
	for _, p := range sess.pendingPubRel {
		if p.lin != _EMPTY_ && sess.custody.hasExchange(gen, p.lin) {
			p.custody = true
		}
	}
}

// ---------------------------------------------------------------------------
// Test seams, instance-scoped
// ---------------------------------------------------------------------------

// The two steps of an attempt's settlement a test seam may fault: the first resolution, and
// the abort (with its re-resolution).
const (
	mqttCustodyStepResolve = "resolve"
	mqttCustodyStepAbort   = "abort"
)

func mqttTestCustodyResolveHook(sess *mqttSession) func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
	if sess.asm == nil {
		return nil
	}
	sess.asm.mu.RLock()
	defer sess.asm.mu.RUnlock()
	return sess.asm.testCustodyResolve
}

func mqttTestCustodyDecodeHook(sess *mqttSession) func(subject string) bool {
	if sess.asm == nil {
		return nil
	}
	sess.asm.mu.RLock()
	defer sess.asm.mu.RUnlock()
	return sess.asm.testCustodyDecodeFault
}

func mqttTestCustodyCensusHook(sess *mqttSession) func() error {
	if sess.asm == nil {
		return nil
	}
	sess.asm.mu.RLock()
	defer sess.asm.mu.RUnlock()
	return sess.asm.testCustodyCensus
}
