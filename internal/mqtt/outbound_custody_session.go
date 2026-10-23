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
	"fmt"
	"strings"
	"time"
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
// holds, plus every scheduling entry the ledger does NOT own (the experimental affine class,
// chapter P's; retained until 7c; and an ordinary entry between tracking and its conversion).
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
	sess.mu.Unlock()
	sess.startCustodyDispatcher()
}

// requestCustodyPassLocked asks for a bounded re-observation pass. Called by the gates when
// they withhold: capacity exhausted, or an identifier withheld. The withholding also LATCHES
// demand (F-097 WP4): the dispatcher keeps re-running bounded passes on its own tracked timer
// until the withheld delivery has been allocated, or until nothing can still be waiting
// (noAllocationCanBeWaitingLocked), so a peer's completion or a failed read is
// rediscovered without waiting for AckWait's re-offer. The caller starts the dispatcher (outside
// the lock) when this returns true.
//
// Lock held on entry.
func (sess *mqttSession) requestCustodyPassLocked() bool {
	if sess.custody == nil || sess.jsa == nil || sess.asm == nil {
		return false
	}
	sess.custodyPassWanted = true
	sess.custodyDemand = true
	return !sess.custodyObserving
}

// kickCustodyPassLocked is requestCustodyPassLocked for a caller that cannot start after
// unlocking: the start site runs on its own goroutine, which admits itself as a worker and
// takes the lock only after it runs.
//
// Lock held on entry.
func (sess *mqttSession) kickCustodyPassLocked() {
	if sess.requestCustodyPassLocked() {
		go sess.startCustodyDispatcher()
	}
}

// kickCustodyItemsLocked starts the dispatcher for a newly recorded ITEM (an owed withdrawal, an
// admission continuity record). It latches no allocation demand.
//
// Lock held on entry.
func (sess *mqttSession) kickCustodyItemsLocked() {
	if sess.custody != nil && sess.jsa != nil && sess.asm != nil && !sess.custodyObserving {
		go sess.startCustodyDispatcher()
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
				if b.name.gen == gen && b.ev.lin == t.key {
					ok = true
					break
				}
			}
		}
		if !ok && sess.custody.forGen(gen) {
			// A LINEAGE WITH NO HOLDER AND NO BINDING IS DECIDED (H-3): a census that observed it
			// ended removed both, so nothing is left to read and its retry ends here, the one exit
			// a retry of it still reaches. Kept, it would re-run every cap interval until the
			// generation is discarded. A ledger that is not this generation's says nothing about
			// the holder, so the retry stands.
			sess.noteCustodyReadLocked(gen, t.key, false)
		}
		sess.mu.Unlock()
		if !ok {
			return false
		}
		var x *mqttOutboundExchange
		absent := false
		if hook := mqttTestCustodyDecodeHook(sess); hook == nil || !hook(mqttExchangeSubject(getHash(sess.id), gen, t.key)) {
			var err error
			x, _, err = sess.jsa.loadOutboundExchange(sess.id, as.sessStreamEpoch, gen, t.key)
			if err != nil || x == nil || x.Nonce != t.key {
				absent = err != nil && mqttExchangeReadAbsent(err)
				x = nil
			}
		}
		// A HELD LINEAGE THAT READS ABSENT MAY HAVE BEEN RECLAIMED behind the floor by any broker
		// (F-112). The holder exists because this process decoded or landed it — the landing
		// proof — so a complete inventory without a representative naming it is a positive
		// ending, observed as one.
		if absent && sess.jsa.classifyExchangeAbsence(sess.id, as.sessStreamEpoch, gen, t.key,
			mqttLandingDecodedRead) == mqttAbsenceReclaimed {
			sess.mu.Lock()
			released := sess.acceptReclaimedLineageLocked(gen, t.key)
			sess.noteCustodyReadLocked(gen, t.key, false)
			sess.mu.Unlock()
			return released
		}
		sess.mu.Lock()
		released := sess.acceptExchangeObservationLocked(gen, []*mqttOutboundExchange{x}, func() bool {
			return sess.custody.applyExchangeObservation(gen, t.key, at, x)
		})
		// AN INDETERMINATE READ IS RETRIED ON THE DISPATCHER'S OWN CADENCE (F-097 WP4): before,
		// a failed read released nothing and nothing re-read it until unrelated traffic hinted
		// again, so at Receive Maximum 1 the withheld delivery waited for AckWait. A readable
		// exchange is progress, whatever its phase, and clears the retry.
		sess.noteCustodyReadLocked(gen, t.key, x == nil && sess.custody.hasExchange(gen, t.key))
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
		r := sess.readCustodySubject(gen, t.key)
		sess.mu.Lock()
		defer sess.mu.Unlock()
		switch {
		case r.x != nil:
			sess.acceptExchangeObservationLocked(gen, []*mqttOutboundExchange{r.x}, func() bool {
				return sess.custody.resolveUncertain(gen, t.key, r.x)
			})
		case r.resourceDuty:
			// A resource duty never held custody: the custody uncertainty goes, and nothing is
			// concluded about the resource's work — except that a retained copy it names is
			// handed to discovery, so the uncertainty does not silently erase reclamation work
			// (F-105 7c, consult 99).
			sess.custody.dischargeUncertain(gen, t.key)
			if r.resourceRef != _EMPTY_ {
				go sess.discoverRetainedPayloads(gen, []string{r.resourceRef})
			}
		case r.exchangeSubject != _EMPTY_:
			// The duty decoded and names an exchange that will not read yet: the uncertainty
			// moves onto that exchange's subject, which the next pass re-reads directly.
			sess.custody.convertUncertain(gen, t.key, r.exchangeSubject)
			return false
		case r.reclaimedLin != _EMPTY_:
			// RECLAIMED (F-112): the uncertainty is discharged, and the lineage is released as the
			// ending it proves.
			sess.custody.dischargeUncertain(gen, t.key)
			sess.acceptReclaimedLineageLocked(gen, r.reclaimedLin)
		case r.retiredPosition:
			// A PEER RETIRED THIS POSITION (F-112). The uncertainty stands — its charge held, its
			// allocation withheld — and becomes a CENSUS demand: only a complete census, which
			// reads every representative inside the floor, can discharge it (reconstructCustody).
			sess.custody.censusWanted = true
			return false
		default:
			return false
		}
		// Resolving an uncertainty always frees capacity or allocation in some way.
		return true
	}
	return false
}

// mqttCustodySubjectRead is what an individual re-read of an uncertain subject found: the
// decoded exchange; or, for a DUTY subject, that the duty is a resource duty (no custody), or the
// subject of the exchange a decoded exchange duty names but which did not read. All zero: nothing
// decoded, the uncertainty stands.
type mqttCustodySubjectRead struct {
	x               *mqttOutboundExchange
	resourceDuty    bool
	resourceRef     string
	exchangeSubject string
	// retiredPosition: the duty subject read absent or as a relocation marker, and the floor has
	// since passed it (F-112). The POSITION is retired, but whatever it held may have been carried
	// to a representative this re-read cannot see, so it discharges nothing on its own.
	retiredPosition bool
	// reclaimedLin: an EXCHANGE subject that read absent and classified RECLAIMED (F-112): its
	// uncertainty was recorded beside a decoded duty naming it, which is the landing proof, and a
	// complete inventory no longer names it.
	reclaimedLin string
}

// readCustodySubject re-reads one uncertain subject individually: an exchange subject by its
// exchange, a duty subject (F-105 7b: the inventory traversal records an unreadable duty by its
// own address, because its exchange is not known) by the duty and then the exchange it names.
//
// Session lock NOT held on entry.
func (sess *mqttSession) readCustodySubject(gen, subject string) mqttCustodySubjectRead {
	if nonce, ok := mqttExchangeNonceFromSubject(subject); ok {
		x := sess.readCustodyExchange(gen, nonce)
		if x == nil {
			// AN EXCHANGE UNCERTAINTY IS RECORDED ONLY BESIDE A DECODED DUTY NAMING IT (the
			// traversal, or the duty re-read below converting onto it), so its lineage landed; if it
			// now reads absent and a complete inventory no longer names it, it was reclaimed behind
			// the floor (F-112), and only that discharges it.
			if _, _, xerr := sess.jsa.loadOutboundExchange(sess.id, sess.asm.sessStreamEpoch, gen, nonce); xerr != nil &&
				mqttExchangeReadAbsent(xerr) &&
				sess.jsa.classifyExchangeAbsence(sess.id, sess.asm.sessStreamEpoch, gen, nonce,
					mqttLandingAdmittedDuty) == mqttAbsenceReclaimed {
				return mqttCustodySubjectRead{reclaimedLin: nonce}
			}
		}
		return mqttCustodySubjectRead{x: x}
	}
	dgen, ord, err := mqttParseDutySubject(subject, getHash(sess.id))
	if err != nil || dgen != gen {
		return mqttCustodySubjectRead{}
	}
	d, _, derr := sess.jsa.loadDutyEntryAt(sess.id, sess.asm.sessStreamEpoch, gen, ord)
	if (derr == nil && d == nil) || (d != nil && d.State == mqttDutyRelocated) {
		if led, _, lerr := sess.jsa.loadGenLedger(sess.id, sess.asm.sessStreamEpoch, gen); lerr == nil &&
			led != nil && led.Lo > ord {
			return mqttCustodySubjectRead{retiredPosition: true}
		}
		return mqttCustodySubjectRead{}
	}
	if derr != nil {
		return mqttCustodySubjectRead{}
	}
	if d.Kind != mqttDutyExchange {
		return mqttCustodySubjectRead{resourceDuty: true, resourceRef: d.Ref}
	}
	if x := sess.readCustodyExchange(gen, d.Ref); x != nil {
		return mqttCustodySubjectRead{x: x}
	}
	return mqttCustodySubjectRead{exchangeSubject: mqttExchangeSubject(getHash(sess.id), gen, d.Ref)}
}

// mqttExchangeNonceFromSubject extracts the exchange nonce from an exchange subject.
func mqttExchangeNonceFromSubject(subject string) (string, bool) {
	prefix := mqttF105SubjectPrefix()
	if !strings.HasPrefix(subject, prefix) {
		return _EMPTY_, false
	}
	toks := strings.Split(subject[len(prefix):], ".")
	if len(toks) != 4 || toks[0] != mqttExchangeClassTk || toks[3] == _EMPTY_ {
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
	// resources are the incarnations the generation's RESOURCE duties name, deduplicated: no
	// custody, but the CONNECT-time discovery of retained copies a crashed producer left (F-105
	// 7c) — filtered to payload owners after CONNACK.
	resources []string
	// ledger and ledgerSeq are the ledger the traversal read (nil when the generation has none):
	// the floor, the high-water and the deletion watermark it observed (F-112). Carried even
	// when the traversal fails later, so a resident caller can arm deletion demand from it.
	ledger    *mqttGenLedger
	ledgerSeq uint64
	// named is every exchange nonce a duty entry inside [Lo, Hi] names, Terminal ones included:
	// the representative set an absence is classified against (F-112, 3.6). Meaningful only
	// when complete.
	named map[string]struct{}
	// complete reports that every position in [Lo, Hi] was read. Uncertain subjects may still
	// stand: complete means the traversal reached the end, not that every entry decoded.
	complete bool
}

// traverseGenerationInventory reads the generation's exchanges for the custody ledger BY THE
// INVENTORY THAT ADMITTED THEM (F-105 7b, row 6): the generation ledger's high-water ordinal
// `Hi`, then every duty entry 1..Hi at its exact address, then the exchange each exchange duty
// names. It replaces a subject census of the same exchanges — two enumerations of one population
// is one too many, and only the inventory has exact addresses: a subject walk cannot tell "never
// admitted" from "not listed".
//
// It reads [Lo, Hi], the positions the floor has not retired (F-112): every position below Lo is
// retired, its duty either reclaimed or carried to a representative inside the range.
//
// Every ordinal in the range is covered, TERMINAL entries included: an abandoned delivery's duty
// resolves while its identifier stays held, so skipping resolved duties (as the closed-generation
// sweep may) would lose custody. A duty entry that is missing, unreadable or undecodable is
// UNCERTAIN by its duty subject, and an exchange that will not read is uncertain by its exchange
// subject: never a hole, never an obligation that is not there. A resource duty carries no
// custody. An ABSENT ledger is an empty inventory: the ledger is created create-only inside the
// first allocation's own batch and nothing deletes a live generation's ledger, so any landed
// admission implies it. A ledger that will not read or decode fails the census.
//
// Its callers run it only after the ownership fence (the accepted CONNECT's finalize, or the
// session's own serving tenure for the background reconstruction), so every admission either
// landed before the fence and is below Hi, or fails its authority CAS.
//
// Session lock NOT held on entry.
func (sess *mqttSession) traverseGenerationInventory(gen string) (*mqttCustodyCensus, error) {
	as := sess.asm
	if as == nil || sess.jsa == nil {
		return nil, fmt.Errorf("session %q has no store to read its exchanges from", sess.id)
	}
	if hook := mqttTestCustodyCensusHook(sess); hook != nil {
		if err := hook(); err != nil {
			return nil, err
		}
	}
	return mqttTraverseGenerationInventory(sess.jsa, as, sess.id, as.sessStreamEpoch, gen,
		func(subject string) bool {
			hook := mqttTestCustodyDecodeHook(sess)
			return hook != nil && hook(subject)
		})
}

// mqttTraverseGenerationInventory is the inventory traversal itself, on the store alone, so a
// reader with no session — the shared release fence run by the verdict sweep or a closing (F-105
// 7e) — enumerates a generation's exchanges exactly as the custody census does: one
// implementation. The duty subjects are batch-read through the account manager when there is one
// (its loader carries the test seam the custody census uses), else straight from the store;
// unreadable, when non-nil, lets a test mark an exchange subject unreadable. The ordering contract
// is the caller's: read it only after the fence that stops further admissions, so every admission
// landed before it is below Hi.
func mqttTraverseGenerationInventory(jsa *mqttJSA, as *mqttAccountSessionManager,
	id, epoch, gen string, unreadable func(subject string) bool) (*mqttCustodyCensus, error) {

	// THE FLOOR CAN MOVE UNDER A TRAVERSAL (F-112): another broker's floor advance retires
	// positions this traversal has not read yet, and its deletion step removes them. A position
	// that reads missing, as a relocation marker, or naming an absent exchange is therefore
	// re-judged against a fresh ledger read: if the floor crossed it, the traversal restarts from
	// the new floor; if not, it is a genuine hole and stays UNCERTAIN exactly as before the floor
	// existed. The trigger is the floor CROSSING the observed position, never a bare sequence
	// change, so admissions and watermark updates cause no restart; every restart is caused by a
	// floor advance that retired a position, i.e. by real progress, and they are still bounded.
	var last *mqttCustodyCensus
	for attempt := 0; attempt <= mqttTraversalRestarts; attempt++ {
		out, restart, err := mqttTraverseInventoryOnce(jsa, as, id, epoch, gen, unreadable)
		if err != nil || !restart {
			return out, err
		}
		last = out
	}
	// EXHAUSTED: INCOMPLETE, which every caller already treats as withholding.
	return last, fmt.Errorf("reading the inventory of %q/%q: the floor moved under %d consecutive "+
		"traversals", id, gen, mqttTraversalRestarts+1)
}

// mqttTraversalRestarts bounds the restarts one traversal takes for a floor that crossed it.
const mqttTraversalRestarts = 8

// mqttTraverseInventoryOnce is one pass over [Lo, Hi]. It reports restart when the floor crossed
// a position this pass could not read as standing. The census carries the ledger observation
// even when the pass fails later, so a resident caller can arm deletion demand from it.
func mqttTraverseInventoryOnce(jsa *mqttJSA, as *mqttAccountSessionManager,
	id, epoch, gen string, unreadable func(subject string) bool) (*mqttCustodyCensus, bool, error) {

	led, ledSeq, lerr := jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil {
		return nil, false, fmt.Errorf("reading the inventory of %q/%q: %w", id, gen, lerr)
	}
	out := &mqttCustodyCensus{named: map[string]struct{}{}}
	if led == nil {
		out.complete = true
		return out, false, nil
	}
	out.ledger, out.ledgerSeq = led, ledSeq
	// crossed re-reads the ledger and reports whether the floor has passed ordinal ord.
	crossed := func(ord uint64) (bool, error) {
		now, _, nerr := jsa.loadGenLedger(id, epoch, gen)
		if nerr != nil {
			return false, fmt.Errorf("re-reading the inventory of %q/%q: %w", id, gen, nerr)
		}
		return now != nil && now.Lo > ord, nil
	}
	idHash := getHash(id)
	for lo := led.Lo; lo <= led.Hi; lo += mqttInflightLoadBatch {
		hi := min(led.Hi, lo+mqttInflightLoadBatch-1)
		subjects := make([]string, 0, hi-lo+1)
		for ord := lo; ord <= hi; ord++ {
			subjects = append(subjects, mqttDutySubject(idHash, gen, ord))
		}
		var resps []*JSApiMsgGetResponse
		var berr error
		if as != nil {
			resps, berr = as.loadInflightRecordsMulti(subjects)
		} else {
			resps, berr = jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
		}
		if berr != nil {
			return out, false, fmt.Errorf("reading the duties of %q/%q: %w", id, gen, berr)
		}
		for i, subject := range subjects {
			ord := lo + uint64(i)
			var resp *JSApiMsgGetResponse
			if i < len(resps) {
				resp = resps[i]
			}
			d := mqttDecodeDutyResponse(id, epoch, gen, ord, resp)
			if d == nil || d.State == mqttDutyRelocated {
				c, cerr := crossed(ord)
				if cerr != nil {
					return out, false, cerr
				}
				if c {
					return out, true, nil
				}
				out.uncertain = append(out.uncertain, subject)
				continue
			}
			if d.Kind != mqttDutyExchange {
				// A resource duty carries no custody; it is collected for retained discovery.
				if len(out.resources) == 0 || out.resources[len(out.resources)-1] != d.Ref {
					out.resources = append(out.resources, d.Ref)
				}
				continue
			}
			out.named[d.Ref] = struct{}{}
			xsubj := mqttExchangeSubject(idHash, gen, d.Ref)
			if unreadable != nil && unreadable(xsubj) {
				out.uncertain = append(out.uncertain, xsubj)
				continue
			}
			// The exchange's own loader returns every read failure — an absence, an undecodable
			// body, a failed read — as an error. A duty INSIDE the floor naming an exchange that
			// will not read is uncertain, unless the floor crossed the duty's own position while
			// this pass was reading: the naming duty's ordinal decides (F-112).
			x, _, xerr := jsa.loadOutboundExchange(id, epoch, gen, d.Ref)
			if xerr != nil || x == nil || x.Nonce != d.Ref {
				c, cerr := crossed(ord)
				if cerr != nil {
					return out, false, cerr
				}
				if c {
					return out, true, nil
				}
				out.uncertain = append(out.uncertain, xsubj)
				continue
			}
			out.live = append(out.live, x)
		}
	}
	out.complete = true
	return out, false, nil
}

// mqttDecodeDutyResponse decodes one batched duty read, or returns nil for anything that is not a
// readable, valid entry of this generation at this ordinal.
func mqttDecodeDutyResponse(id, epoch, gen string, ord uint64, resp *JSApiMsgGetResponse) *mqttDutyEntry {
	if resp == nil || mqttClassifyMsgGetResponse(resp) != mqttInflightResponsePresent || resp.Message == nil {
		return nil
	}
	d, err := mqttDecodeDutyEntry(resp.Message.Data, id, epoch, gen, ord)
	if err != nil {
		return nil
	}
	return d
}

// readCustodyExchange reads one exchange through its own loader, which returns every read error
// — an absence, an undecodable body, a failed read — as an error; each is nil here.
//
// Session lock NOT held on entry.
func (sess *mqttSession) readCustodyExchange(gen, nonce string) *mqttOutboundExchange {
	if hook := mqttTestCustodyDecodeHook(sess); hook != nil &&
		hook(mqttExchangeSubject(getHash(sess.id), gen, nonce)) {
		return nil
	}
	x, _, err := sess.jsa.loadOutboundExchange(sess.id, sess.asm.sessStreamEpoch, gen, nonce)
	if err != nil || x == nil || x.Nonce != nonce {
		return nil
	}
	return x
}

// reconstructCustody merges a census into the ledger. A failed census leaves it incomplete.
//
// Session lock NOT held on entry.
func (sess *mqttSession) reconstructCustody(gen string) *mqttCustodyCensus {
	census, err := sess.traverseGenerationInventory(gen)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if err != nil {
		sess.custody.failReconstruction(gen)
		// The ledger the failed traversal read still says whether deletion is owed (F-112).
		if census != nil && sess.noteLedgerObservationLocked(gen, census.ledger) {
			go sess.startCustodyDispatcher()
		}
		return nil
	}
	sess.acceptExchangeObservationLocked(gen, census.live, func() bool {
		sess.custody.mergeCensus(gen, census.live, census.uncertain, true)
		return false
	})
	// A COMPLETE CENSUS DISCHARGES THE UNCERTAINTY OF A RETIRED POSITION (F-112): it read every
	// position inside the floor, so a duty a peer carried out of that position was observed at its
	// representative, and one it reclaimed owes nothing. A position inside the floor stays
	// uncertain: it is a genuine hole.
	if census.complete && census.ledger != nil {
		sess.custody.dischargeRetiredDutyUncertainty(gen, getHash(sess.id), census.ledger.Lo)
	}
	// A RESIDENT LEDGER READ ARMS THE FLOOR when deletion is owed or the span is worth advancing
	// (F-112): a floor committed before a crash is finished by the next resident process without
	// an ending or an admission to arm it.
	if sess.noteLedgerObservationLocked(gen, census.ledger) {
		go sess.startCustodyDispatcher()
	}
	return census
}

// rebuildCustodyForResume is the resume's reconstruction, run BEFORE any register is routed
// and before delivery or allocation opens: the ledger is marked incomplete, the census merged,
// and a failure leaves it withholding.
//
// Session lock NOT held on entry.
func (sess *mqttSession) rebuildCustodyForResume(gen string) *mqttCustodyCensus {
	sess.mu.Lock()
	sess.custody.beginReconstruction(gen)
	sess.mu.Unlock()
	return sess.reconstructCustody(gen)
}

// markRecoveredCustodyLocked marks every scheduling entry recovery installed for a lineage the
// ledger charges as a custody entry, so the projection stops counting it beside the ledger.
// An entry whose exchange the census did not find keeps its scheduling count, as before.
//
// Lock held on entry.
func (sess *mqttSession) markRecoveredCustodyLocked(gen string) {
	for _, p := range sess.pendingPublish {
		if p.kind == mqttPendingOrdinary && sess.custody.hasExchange(gen, p.ev.lin) {
			p.custody = true
		}
	}
	for _, p := range sess.pendingPubRel {
		if p.kind == mqttPendingOrdinary && sess.custody.hasExchange(gen, p.ev.lin) {
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

// ---------------------------------------------------------------------------
// Pending projections are a view over custody (F-105 7b, consult 88)
// ---------------------------------------------------------------------------

// acceptExchangeObservationLocked is the ONE place the session applies decoded exchange
// observations to the custody ledger — the census merge, the target observation and the uncertain
// re-read all enter here — and the one place a released lineage's pending projections go.
//
// For each observation the abandoned-ACK binding it ends is released, then apply updates the
// ledger, then every lineage the ledger RELEASED (an accepted ENDED observation; never a
// charge-only release, never a missing or unreadable read) loses its projections in the same
// critical section. That is the release half of the invariant "an ordinary projection that
// recovery installs exists only while custody holds its lineage"; the recovery gate and setPendingLocked
// are the install half. With both under sess.mu one precedes the other for any install and any
// release: release first and the install is refused; install first and the release removes it.
//
// Lock held on entry.
func (sess *mqttSession) acceptExchangeObservationLocked(gen string, xs []*mqttOutboundExchange, apply func() bool) bool {
	for _, x := range xs {
		sess.observeAbandonedAckEndLocked(x)
	}
	released := apply()
	ended := sess.custody.drainReleased()
	for _, lin := range ended {
		sess.removeLineageProjectionsLocked(gen, lin)
	}
	// AN ENDING ARMS THE FLOOR (F-112): it is what makes a position resolvable and, once its
	// register goes, reclaimable. Inside a dispatcher run the loop takes the unit; outside one,
	// the newly set latch starts the dispatcher.
	if len(ended) > 0 && sess.armFloorLocked(gen, time.Time{}) {
		go sess.startCustodyDispatcher()
	}
	// A shared dependant that left source delivery may unblock a withheld release (F-105 7e).
	sess.kickVerdictsForObservedLocked(gen, xs)
	return released
}

// acceptReclaimedLineageLocked applies a RECLAIMED absence (F-112) as the ending it proves: the
// holder's charge and identifier go, every projection of the lineage goes, and every binding of
// it goes — a reclaimed exchange ended AND its register is positively gone, which is the strongest
// release proof a binding has (mqttBindingReleaseEndedAndDeleted). No exchange body is fabricated
// for it.
//
// Lock held on entry.
func (sess *mqttSession) acceptReclaimedLineageLocked(gen, lin string) bool {
	if gen != sess.gen || sess.custody == nil {
		return false
	}
	for _, b := range sess.abandonedAcks {
		if b != nil && b.name.gen == gen && b.ev.lin == lin {
			sess.removeAbandonedAckBindingLocked(b, mqttBindingReleaseEndedAndDeleted)
		}
	}
	released := sess.custody.releaseReclaimed(gen, lin)
	ended := sess.custody.drainReleased()
	for _, l := range ended {
		sess.removeLineageProjectionsLocked(gen, l)
	}
	if len(ended) > 0 && sess.armFloorLocked(gen, time.Time{}) {
		go sess.startCustodyDispatcher()
	}
	return released
}

// removeLineageProjectionsLocked removes every pending projection of lineage lin in generation
// gen, with the cpending mappings it owns. Selection is the lineage ALONE: an inherited-lineage
// replacement continues the ended exchange and goes with it; a fresh-lineage replacement on the
// same packet identifier carries another lineage and stays. Removal is removal, not a demotion
// flag, which would leave the entry counted by capacity and by the allocator. An open send's
// settlement is its action's (token-keyed), not the entry's.
//
// Lock held on entry.
func (sess *mqttSession) removeLineageProjectionsLocked(gen, lin string) {
	if lin == _EMPTY_ || gen != sess.gen {
		return
	}
	// Every entry carrying the released obligation's evidence goes, WHATEVER ITS KIND: a
	// reservation that names an ended exchange holds its identifier for nothing.
	for pi, p := range sess.pendingPublish {
		if p != nil && p.ev.gen == gen && p.ev.lin == lin {
			sess.untrackPublishEntry(pi, mqttUntrackEnded) // the exchange positively ended
		}
	}
	for pi, p := range sess.pendingPubRel {
		if p == nil || p.ev.gen != gen || p.ev.lin != lin {
			continue
		}
		switch {
		case p.kind == mqttPendingHold && (p.terminalReconcile || mqttInflightTerminalPhase(p.slot.state) ||
			p.awaitingPubCompOwner != 0):
			// A TERMINAL-phase entry holds its identifier through the slot's cleanup, which the
			// exchange's ending PRECEDES: its lifetime is nonce-keyed (releaseTerminalReservation,
			// the PUBCOMP conversion, the reconcile continuation), not the lineage's.
		case p.kind == mqttPendingOrdinary && p.jsAckSubject != _EMPTY_ && sess.pubCompEmittableOwnerLocked(pi) != 0:
			// A PUBREL this connection possibly transmitted and whose PUBCOMP it still owes: the
			// ending revokes its authority, not the handler's ability to settle the slot through
			// it. An open attempt that has not entered its write is not one — it can no longer
			// emit once this entry is gone — so it alone never makes a released delivery (F-116).
			sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectExactly(p),
				mqttNewReleasedDelivery(p, sess.pubCompEmittableOwnerLocked(pi)))
		default:
			sess.untrackPubRel(pi)
		}
	}
}

// retireReleasedDeliveriesLocked retires every released delivery whose PUBCOMP owner is no
// longer live (F-089's owner liveness: the bound connection, or one a two-phase retirement is
// still detaching): no answer can reach it any more. A slot cleanup the completion worker still
// owns keeps its identifier through the worker's terminal reservation; otherwise the entry and
// its mapping go. Each retirement replaces exactly the entry it examined.
//
// Lock held on entry.
func (sess *mqttSession) retireReleasedDeliveriesLocked() {
	for pi, p := range sess.pendingPubRel {
		if p == nil || p.awaitingPubCompOwner == 0 || sess.holdOwnerLiveLocked(p.awaitingPubCompOwner) {
			continue
		}
		sess.retireReleasedDeliveryLocked(pi, p)
	}
}

// retireReleasedDeliveryLocked is the disposition of ONE released delivery no PUBCOMP will
// answer: the owner is gone (above), or its last owed unit was an attempt whose frame never
// left (endSendCore, F-116). The keeping of the identifier through the worker's terminal
// reservation is the same in both.
//
// Lock held on entry.
func (sess *mqttSession) retireReleasedDeliveryLocked(pi uint16, p *mqttPending) {
	if sess.pubCompOwed[mqttPubCompKey{pi: pi, nonce: p.regNonce}] != nil && sess.terminalRefOKLocked(p.slot) {
		if sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectExactly(p), mqttNewTerminalReservation(p.slot, p)) {
			if m := sess.cpending[p.jsDur]; m != nil && m[p.sseq] == pi {
				delete(m, p.sseq)
			}
		}
		return
	}
	sess.untrackPubRel(pi)
}

// ordinaryProjectionHeldLocked is the recovery gate (F-105 7b): the VALIDATED register an
// install is restored from names a lineage the custody ledger holds. It gates every recovered
// ordinary-class record, whatever kind its entry will have; a refusal installs nothing, and the
// identifier is the ledger's if it is held and nobody's if it is not. (The entry's evidence is
// its constructor's; setPendingLocked applies the same condition again to an Ordinary entry.)
//
// Lock held on entry.
func (sess *mqttSession) ordinaryProjectionHeldLocked(gen string, rec *mqttInflightRecord) bool {
	return rec != nil && rec.Lin != _EMPTY_ && sess.custody.hasExchange(gen, rec.Lin)
}

// recoveryInstallRefusedLocked applies the gate to one entry the resume is about to install or
// reactivate: true when rec is an ORDINARY register and the gate refuses. The experimental
// slot-only class has no exchange and is outside the invariant.
//
// Lock held on entry.
func (sess *mqttSession) recoveryInstallRefusedLocked(rec *mqttInflightRecord, p *mqttPending) bool {
	if rec == nil || mqttDurIsSlotOnlyClass(rec.Dur) {
		return false
	}
	return p == nil || !sess.ordinaryProjectionHeldLocked(sess.gen, rec)
}

// recoveryRestoreRefusedLocked is recoveryInstallRefusedLocked for an entry the resume RESTORES
// from its validated register, with the one addition an unresolved inventory read needs (consult
// 93). The register proves its admission batch landed — register, exchange and duty are written
// together — so when the inventory could not decide the lineage's membership (the traversal
// failed, a duty ordinal was unreadable, or this lineage's own exchange is uncertain), the
// lineage is held PROVISIONALLY on the register's identifier before the gate runs. That holder
// creates no admission, overrides no complete exclusion (a complete inventory without the
// lineage took the unpaired refusal) and authorizes no transmission by itself; it keeps the
// delivery and its source mapping until the uncertainty resolves, and the resolution drives it.
// A genuine cancellation (marker standing, no recovered output token) never reaches a restore, so
// never seeds custody.
//
// Lock held on entry.
func (sess *mqttSession) recoveryRestoreRefusedLocked(census *mqttCustodyCensus, rec *mqttInflightRecord, p *mqttPending) bool {
	sess.seedUndecidedRecoveredCustodyLocked(census, rec)
	return sess.recoveryInstallRefusedLocked(rec, p)
}

// seedUndecidedRecoveredCustodyLocked seeds a restored exchange's lineage PROVISIONALLY when this
// resume's inventory could not decide its membership and custody does not already hold it. A
// complete census decides, and seeds nothing.
//
// Lock held on entry.
func (sess *mqttSession) seedUndecidedRecoveredCustodyLocked(census *mqttCustodyCensus, rec *mqttInflightRecord) {
	// A CANCELLED register reaching a restore is possibly-attempted (F-088): recovery diverts a
	// genuine cancellation — its marker standing with no recovered output token — to the
	// cancellation disposition before any restore, so the cancellation bit alone no longer
	// excludes a lineage from custody.
	if rec != nil && !mqttDurIsSlotOnlyClass(rec.Dur) && rec.Lin != _EMPTY_ &&
		!sess.custody.hasExchange(sess.gen, rec.Lin) &&
		sess.inventoryUndecidedLocked(census, rec.Lin) {
		sess.custody.seedProvisionalExchange(sess.gen, rec.Lin, rec.PI)
	}
}

// inventoryUndecidedLocked reports whether this resume's inventory could not decide lin's
// membership: no census, an unreadable duty ordinal (which names no lineage, so it leaves every
// one undecided), or lin's own exchange subject uncertain. An unrelated exchange's uncertainty
// decides nothing about lin.
//
// Lock held on entry.
func (sess *mqttSession) inventoryUndecidedLocked(census *mqttCustodyCensus, lin string) bool {
	if census == nil {
		return true
	}
	dutyPrefix := mqttF105SubjectPrefix() + mqttDutyClassTk + "."
	for _, subj := range census.uncertain {
		if strings.HasPrefix(subj, dutyPrefix) {
			return true
		}
	}
	return sess.custody.subjectUncertain(sess.gen, mqttExchangeSubject(sess.idHash, sess.gen, lin))
}
