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
	"sort"
	"strings"
)

// The custody ledger (F-105 cutover, row 5).
//
// An ordinary QoS 1/2 exchange holds a Receive Maximum charge and its packet identifier from
// the moment its admission is reserved until the EXCHANGE durably ends — not until some map
// entry that schedules it is removed. The scheduling maps (pendingPublish, pendingPubRel) are
// removed by metadata dispositions, abandonments and tombstones that prove neither
// completion nor non-transmission, and a charge they released let a second PUBLISH go out at
// Receive Maximum 1 while the client still held the first (Spec5 [4.9]).
//
// So the charge has one owner, this ledger, and exactly two ways to lose it:
//
//   - an ADMISSION reservation (one admitInflightPublish call) is removed only once its ladder
//     has returned and every attempt it submitted is positively settled — Landed, whose
//     exchange entry already carries the charge, or NeverLanded by the create-only abort;
//   - an EXCHANGE entry is removed only by an exact read of that exchange at an ENDED phase,
//     which is absorbing, or by the generation being discarded.
//
// The two kinds live in separate maps and their rules never touch each other: a create's
// allocation nonce IS its lineage, so both entries coexist under one string from the landing
// until the admission settles. Everything else — a hint, a register delete, a map removal, a
// continuation that stopped on absence — at most schedules an observation.
//
// Every field is touched only by the methods in this file, and every method enters through
// forGen: a callback of a generation this object no longer serves changes nothing.

// mqttCustodyAttempt is one SUBMITTED admission attempt, immutable once recorded: the handle
// commitOp froze before the batch was sent, and the lineage THAT attempt wrote. The ladder's
// record is mutable across attempts, so the lineage is copied here.
type mqttCustodyAttempt struct {
	handle *mqttTransitionHandle
	lin    string
}

// mqttCustodyAdmission is the reservation of ONE admission.
type mqttCustodyAdmission struct {
	pi uint16
	// ladderDone: the admission returned, so no further attempt will start. The write
	// callback runs synchronously inside the ladder, so an attempt in flight implies this is
	// false — which is why no separate in-flight count exists.
	ladderDone bool
	// landed: some attempt committed, so its exchange entry carries the slot and removing
	// this reservation frees no capacity.
	landed       bool
	unsettled    []mqttCustodyAttempt
	lastObserved uint64
}

// mqttCustodyExchange is an admitted exchange's custody.
type mqttCustodyExchange struct {
	pi           uint16
	pendingDrain bool
	lastObserved uint64
	// observedAt is the tick at which the last applied LIVE observation began, so an older
	// read cannot regress pendingDrain. Removal is never gated by it: ended phases absorb.
	observedAt uint64
	// chargeReleased: the exchange's delivery was ABANDONED (DeliveryAbandoned). Its Receive
	// Maximum charge is gone (Spec5 §4.9: the quota is connection-local) while its identifier
	// is still held — the two projections split here. Monotone: set by observing the phase
	// and never cleared, so a stale Publish read or census cannot recharge it.
	chargeReleased bool
}

// mqttCustodyLedger is one generation's custody.
type mqttCustodyLedger struct {
	ledgerGen      string
	heldAdmissions map[string]*mqttCustodyAdmission // by allocation nonce
	heldExchanges  map[string]*mqttCustodyExchange  // by exchange lineage
	// uncertain holds SUBJECTS read unreadable: an exchange subject, or — from the inventory
	// traversal, when the duty entry itself would not read — a duty subject.
	// Each holds a charge and withholds all allocation (its identifier is unknown) until an
	// individual re-read decodes it or the generation is discarded. Kept BY SUBJECT: a count
	// recomputed by the next census would forget a subject that disappears from it, and an
	// absence is not evidence of a discharge.
	uncertainSubjects map[string]uint64
	// heldPIs indexes the identifiers held by admissions and exchanges; chargedPIs those whose
	// holder still carries a Receive Maximum charge (every holder but an abandoned exchange).
	heldPIs    map[uint16]int
	chargedPIs map[uint16]int
	// reconstructionIncomplete: the reconstruction of a resumed generation is not (yet) whole.
	reconstructionIncomplete bool
	observeTick              uint64
	// released collects the LINEAGES whose exchange entry an accepted ended observation removed,
	// until the session drains it in the same critical section (acceptExchangeObservationLocked).
	// A lineage is reported whether or not a charge went with it: an abandoned exchange's charge
	// was released long before its ending, and its pending projection must still go (F-105 7b).
	released []string
	// resumeApplies counts the resumes in flight; while one is, endedDuringResume remembers every
	// lineage an accepted ENDED observation reached, so a resume that captured the register
	// before the ending can never seed a provisional holder for it after the release (consult
	// 93's veto). Emptied when the last resume ends, so it never outlives the window it guards.
	resumeApplies     int
	endedDuringResume map[string]struct{}
}

func newMQTTCustodyLedger(gen string) *mqttCustodyLedger {
	return &mqttCustodyLedger{
		ledgerGen:         gen,
		heldAdmissions:    make(map[string]*mqttCustodyAdmission),
		heldExchanges:     make(map[string]*mqttCustodyExchange),
		uncertainSubjects: make(map[string]uint64),
		heldPIs:           make(map[uint16]int),
		chargedPIs:        make(map[uint16]int),
	}
}

// forGen is the ledger's ONE generation guard. Every method that acts on the ledger for a
// caller enters through it, so a late callback of a discarded generation cannot charge,
// release or observe anything in its successor's ledger.
func (l *mqttCustodyLedger) forGen(gen string) bool {
	return l != nil && gen != _EMPTY_ && l.ledgerGen == gen
}

// holdPI records a new charged holder of the identifier.
func (l *mqttCustodyLedger) holdPI(pi uint16) {
	if pi != 0 {
		l.heldPIs[pi]++
		l.chargedPIs[pi]++
	}
}

// dropPI removes a holder of the identifier; charged says whether it still carried a charge.
func (l *mqttCustodyLedger) dropPI(pi uint16, charged bool) {
	if pi == 0 {
		return
	}
	mqttCustodyDecrement(l.heldPIs, pi)
	if charged {
		mqttCustodyDecrement(l.chargedPIs, pi)
	}
}

func mqttCustodyDecrement(m map[uint16]int, pi uint16) {
	if n := m[pi]; n <= 1 {
		delete(m, pi)
	} else {
		m[pi] = n - 1
	}
}

// releaseCharge marks an exchange's charge released while its identifier stays held.
func (l *mqttCustodyLedger) releaseCharge(e *mqttCustodyExchange) bool {
	if e.chargeReleased {
		return false
	}
	e.chargeReleased = true
	if e.pi != 0 {
		mqttCustodyDecrement(l.chargedPIs, e.pi)
	}
	return true
}

func (l *mqttCustodyLedger) nextTick() uint64 {
	l.observeTick++
	return l.observeTick
}

// ---------------------------------------------------------------------------
// Admission reservations
// ---------------------------------------------------------------------------

// openAdmission creates the reservation of one admission. It reports false when the
// generation is not this ledger's or the reservation already exists.
func (l *mqttCustodyLedger) openAdmission(gen, nonce string, pi uint16) bool {
	if !l.forGen(gen) || nonce == _EMPTY_ {
		return false
	}
	if _, ok := l.heldAdmissions[nonce]; ok {
		return false
	}
	l.heldAdmissions[nonce] = &mqttCustodyAdmission{pi: pi, lastObserved: l.nextTick()}
	l.holdPI(pi)
	return true
}

// recordAttempt adds a submitted attempt to its admission's unsettled set.
func (l *mqttCustodyLedger) recordAttempt(gen, nonce string, a mqttCustodyAttempt) {
	if !l.forGen(gen) || a.handle == nil {
		return
	}
	if adm := l.heldAdmissions[nonce]; adm != nil {
		adm.unsettled = append(adm.unsettled, a)
	}
}

// attemptLanded records that the admission's attempt committed: the exchange of the lineage
// it wrote is charged FIRST, in the same critical section that drops the handle, so the slot
// is never uncounted. A nil handle names a successful return, which recorded nothing.
func (l *mqttCustodyLedger) attemptLanded(gen, nonce string, h *mqttTransitionHandle, lin string) {
	if !l.forGen(gen) {
		return
	}
	adm := l.heldAdmissions[nonce]
	if adm == nil {
		return
	}
	l.ensureExchangeLocked(lin, adm.pi)
	adm.landed = true
	l.dropAttempt(adm, h)
	l.tryRemoveAdmission(nonce)
}

// attemptNeverLanded records that the attempt positively never committed. It drops that
// attempt only; it never touches an exchange entry, whatever lineage the attempt wrote.
func (l *mqttCustodyLedger) attemptNeverLanded(gen, nonce string, h *mqttTransitionHandle) bool {
	if !l.forGen(gen) {
		return false
	}
	adm := l.heldAdmissions[nonce]
	if adm == nil {
		return false
	}
	l.dropAttempt(adm, h)
	return l.tryRemoveAdmission(nonce)
}

// finishAdmission marks the admission's ladder returned and removes the reservation when
// nothing it submitted is still unsettled. Deferred on every return of admitInflightPublish.
func (l *mqttCustodyLedger) finishAdmission(gen, nonce string) bool {
	if !l.forGen(gen) {
		return false
	}
	adm := l.heldAdmissions[nonce]
	if adm == nil {
		return false
	}
	adm.ladderDone = true
	return l.tryRemoveAdmission(nonce)
}

func (l *mqttCustodyLedger) dropAttempt(adm *mqttCustodyAdmission, h *mqttTransitionHandle) {
	if h == nil {
		return
	}
	for i, a := range adm.unsettled {
		if a.handle == h {
			adm.unsettled = append(adm.unsettled[:i], adm.unsettled[i+1:]...)
			return
		}
	}
}

// tryRemoveAdmission applies the admission removal rule: the ladder returned and nothing it
// submitted is unsettled. Every submitted attempt is then Landed (and its exchange carries
// the charge) or positively NeverLanded, so the admission's own charge is no longer needed.
// It reports whether the removal FREED capacity: not when an attempt landed, whose exchange
// entry still holds the slot.
func (l *mqttCustodyLedger) tryRemoveAdmission(nonce string) bool {
	adm := l.heldAdmissions[nonce]
	if adm == nil || !adm.ladderDone || len(adm.unsettled) != 0 {
		return false
	}
	delete(l.heldAdmissions, nonce)
	l.dropPI(adm.pi, true)
	return !adm.landed
}

// unsettledAttempts returns the admission's unsettled attempts, for the reconciler.
func (l *mqttCustodyLedger) unsettledAttempts(gen, nonce string) []mqttCustodyAttempt {
	if !l.forGen(gen) {
		return nil
	}
	adm := l.heldAdmissions[nonce]
	if adm == nil {
		return nil
	}
	return append([]mqttCustodyAttempt(nil), adm.unsettled...)
}

// ---------------------------------------------------------------------------
// Exchange custody
// ---------------------------------------------------------------------------

func (l *mqttCustodyLedger) ensureExchangeLocked(lin string, pi uint16) {
	if lin == _EMPTY_ {
		return
	}
	if _, ok := l.heldExchanges[lin]; ok {
		return
	}
	l.heldExchanges[lin] = &mqttCustodyExchange{pi: pi, lastObserved: l.nextTick()}
	l.holdPI(pi)
}

// beginObservation stamps an exchange target as observed now and returns the tick the read
// begins at. Every attempt — successful or not — moves the target to the back of the
// fairness order.
func (l *mqttCustodyLedger) beginObservation(gen, lin string) (uint64, bool) {
	if !l.forGen(gen) {
		return 0, false
	}
	x := l.heldExchanges[lin]
	if x == nil {
		return 0, false
	}
	t := l.nextTick()
	x.lastObserved = t
	return t, true
}

// applyExchangeObservation applies an exact read of the exchange of lineage lin, begun at
// tick at, and reports whether a charge was released. x is nil when no readable exchange was
// found, which changes nothing: an admitted exchange that reads absent is not free capacity.
//
// The disposition is mqttCustodyDisposition's:
//
//   - ENDED removes the entry — charge and identifier — whenever the read was taken: ended
//     phases are absorbing and the exchange subject is create-only per lineage;
//   - ABANDONED releases the charge and keeps the identifier (the entry stays), monotonically;
//   - held pending drain, and held (a successful PUBREC keeps the quota, Spec5 [4.9]), hold —
//     and never re-charge an entry whose charge an abandonment already released;
//   - an unsupported phase holds: a record this broker cannot interpret is not free capacity.
func (l *mqttCustodyLedger) applyExchangeObservation(gen, lin string, at uint64, x *mqttOutboundExchange) bool {
	if !l.forGen(gen) || x == nil {
		return false
	}
	e := l.heldExchanges[lin]
	if e == nil {
		return false
	}
	switch d, pending := mqttCustodyDisposition(x); d {
	case mqttCustodyEnded:
		return l.releaseExchange(lin, e)
	case mqttCustodyAbandoned:
		return l.releaseCharge(e)
	default:
		if at < e.observedAt {
			return false
		}
		e.observedAt = at
		e.pendingDrain = pending
		return false
	}
}

// mqttCustodyDisp is what an exchange's phase says about its custody.
type mqttCustodyDisp uint8

const (
	// mqttCustodyHeld: charge and identifier held.
	mqttCustodyHeld mqttCustodyDisp = iota + 1
	// mqttCustodyAbandoned: the charge is released, the identifier held.
	mqttCustodyAbandoned
	// mqttCustodyEnded: both released.
	mqttCustodyEnded
)

// mqttCustodyDisposition reads an exchange's custody from its phase. The charge half is
// mqttExchangeCharge's — the one definition of what a phase says about the charge — and the
// identifier half is mqttExchangePhaseEnded's, which an abandonment deliberately does not
// satisfy. An exchange whose charge cannot be decided holds both.
func mqttCustodyDisposition(x *mqttOutboundExchange) (mqttCustodyDisp, bool) {
	d, err := mqttExchangeCharge(x, false)
	if err != nil {
		return mqttCustodyHeld, false
	}
	if d != mqttChargeReleasable {
		return mqttCustodyHeld, d == mqttChargeHeldPendingDrain
	}
	if mqttExchangePhaseEnded(x.Phase) {
		return mqttCustodyEnded, false
	}
	return mqttCustodyAbandoned, false
}

// releaseExchange removes an exchange entry on an accepted ENDED observation — charge and
// identifier — and reports its lineage for the session to drop the pending projections that
// continue it. It reports whether a charge went with it.
func (l *mqttCustodyLedger) releaseExchange(lin string, e *mqttCustodyExchange) bool {
	l.noteEnded(lin)
	delete(l.heldExchanges, lin)
	l.dropPI(e.pi, !e.chargeReleased)
	l.released = append(l.released, lin)
	return !e.chargeReleased
}

// drainReleased returns and clears the lineages released since the last drain.
func (l *mqttCustodyLedger) drainReleased() []string {
	if l == nil || len(l.released) == 0 {
		return nil
	}
	out := l.released
	l.released = nil
	return out
}

// adoptReadExchange installs what a census or an uncertain re-read found: a held exchange is
// charged, an abandoned one holds its identifier only. An ENDED one adds nothing — and RELEASES
// an entry that already holds it (F-105 7b): ended phases are absorbing and the exchange subject
// is create-only per lineage, so an ended read is as final from a census as from an observation.
// An entry whose charge was already released is never recharged.
func (l *mqttCustodyLedger) adoptReadExchange(x *mqttOutboundExchange) mqttCustodyDisp {
	d, pending := mqttCustodyDisposition(x)
	if d == mqttCustodyEnded {
		l.noteEnded(x.Nonce)
		if e := l.heldExchanges[x.Nonce]; e != nil {
			l.releaseExchange(x.Nonce, e)
		}
		return d
	}
	l.ensureExchangeLocked(x.Nonce, x.PI)
	e := l.heldExchanges[x.Nonce]
	if e == nil {
		return d
	}
	if d == mqttCustodyAbandoned {
		l.releaseCharge(e)
	} else if !e.chargeReleased {
		e.pendingDrain = pending
	}
	return d
}

// noteEnded records an accepted ENDED observation of lin while a resume is in flight.
func (l *mqttCustodyLedger) noteEnded(lin string) {
	if l.resumeApplies == 0 || lin == _EMPTY_ {
		return
	}
	if l.endedDuringResume == nil {
		l.endedDuringResume = make(map[string]struct{})
	}
	l.endedDuringResume[lin] = struct{}{}
}

// beginResume opens the veto window for one resume; endResume closes it.
func (l *mqttCustodyLedger) beginResume() {
	if l != nil {
		l.resumeApplies++
	}
}

func (l *mqttCustodyLedger) endResume() {
	if l == nil || l.resumeApplies == 0 {
		return
	}
	l.resumeApplies--
	if l.resumeApplies == 0 {
		l.endedDuringResume = nil
	}
}

// seedProvisionalExchange holds lin on pi, charged, for a resume that captured a validated
// admitted register while the inventory could not decide lin's membership (consult 93). The
// holder makes lin an exact observation target, so its resolution drives it like any other: a
// live read adopts it without a second charge, an ended read releases it and its projections.
// Refused for an ending this resume window already accepted. Reports whether lin is held.
func (l *mqttCustodyLedger) seedProvisionalExchange(gen, lin string, pi uint16) bool {
	if !l.forGen(gen) || lin == _EMPTY_ || pi == 0 {
		return false
	}
	if _, ended := l.endedDuringResume[lin]; ended {
		return false
	}
	l.ensureExchangeLocked(lin, pi)
	return true
}

// ---------------------------------------------------------------------------
// Reconstruction and uncertainty
// ---------------------------------------------------------------------------

// beginReconstruction marks the ledger incomplete until a whole census is merged.
func (l *mqttCustodyLedger) beginReconstruction(gen string) {
	if l.forGen(gen) {
		l.reconstructionIncomplete = true
	}
}

// mergeCensus merges a census of the generation's exchanges: every exchange at a non-ended
// phase is charged, an ended one releases any entry holding it (adoptReadExchange), and every
// vanished or unreadable subject becomes uncertain. A merge racing a live release at worst
// re-adds an entry the next observation removes again. whole reports whether the census may
// clear incompleteness.
func (l *mqttCustodyLedger) mergeCensus(gen string, live []*mqttOutboundExchange, uncertain []string, whole bool) {
	if !l.forGen(gen) {
		return
	}
	for _, x := range live {
		if x != nil {
			l.adoptReadExchange(x)
		}
	}
	for _, s := range uncertain {
		if _, ok := l.uncertainSubjects[s]; !ok {
			l.uncertainSubjects[s] = l.nextTick()
		}
	}
	if whole {
		l.reconstructionIncomplete = false
	}
}

// failReconstruction leaves the ledger incomplete: capacity and allocation withhold.
func (l *mqttCustodyLedger) failReconstruction(gen string) {
	if l.forGen(gen) {
		l.reconstructionIncomplete = true
	}
}

// subjectUncertain reports whether subject is uncertain in gen's ledger.
func (l *mqttCustodyLedger) subjectUncertain(gen, subject string) bool {
	if !l.forGen(gen) {
		return false
	}
	_, ok := l.uncertainSubjects[subject]
	return ok
}

// beginUncertainRead stamps an uncertain subject as observed now.
func (l *mqttCustodyLedger) beginUncertainRead(gen, subject string) bool {
	if !l.forGen(gen) {
		return false
	}
	if _, ok := l.uncertainSubjects[subject]; !ok {
		return false
	}
	l.uncertainSubjects[subject] = l.nextTick()
	return true
}

// resolveUncertain applies an individual re-read of an uncertain subject that DECODED to an
// exchange: an ended exchange drops the uncertainty (and releases any entry holding it); a live
// one is converted to an exchange entry in the same section. A re-read that did not decode
// leaves it uncertain.
func (l *mqttCustodyLedger) resolveUncertain(gen, subject string, x *mqttOutboundExchange) bool {
	if !l.forGen(gen) || x == nil {
		return false
	}
	if _, ok := l.uncertainSubjects[subject]; !ok {
		return false
	}
	d := l.adoptReadExchange(x)
	delete(l.uncertainSubjects, subject)
	return d != mqttCustodyHeld
}

// convertUncertain moves a DUTY subject's uncertainty onto the subject of the exchange the duty
// was re-read to name, in one step, so the charge it holds is never uncounted in between.
func (l *mqttCustodyLedger) convertUncertain(gen, from, to string) {
	if !l.forGen(gen) {
		return
	}
	if _, ok := l.uncertainSubjects[from]; !ok {
		return
	}
	delete(l.uncertainSubjects, from)
	if _, ok := l.uncertainSubjects[to]; !ok {
		l.uncertainSubjects[to] = l.nextTick()
	}
}

// dischargeUncertain drops the custody uncertainty of a DUTY subject whose re-read decoded to a
// resource duty (provision or cleanup): a resource carries no Receive Maximum charge and no
// identifier, so it never held custody. It declares nothing about the resource's work, which
// the inventory still records exactly as it stands.
func (l *mqttCustodyLedger) dischargeUncertain(gen, subject string) bool {
	if !l.forGen(gen) {
		return false
	}
	if _, ok := l.uncertainSubjects[subject]; !ok {
		return false
	}
	delete(l.uncertainSubjects, subject)
	return true
}

// ---------------------------------------------------------------------------
// What the capacity and allocation gates read
// ---------------------------------------------------------------------------

// charges is the ledger's Receive Maximum occupancy.
func (l *mqttCustodyLedger) charges() int {
	if l == nil {
		return 0
	}
	n := len(l.heldAdmissions) + len(l.uncertainSubjects)
	for _, e := range l.heldExchanges {
		if !e.chargeReleased {
			n++
		}
	}
	return n
}

// withholdsNew reports that no NEW work may be admitted: the reconstruction is incomplete.
func (l *mqttCustodyLedger) withholdsNew() bool {
	return l != nil && l.reconstructionIncomplete
}

// withholdsAllocation reports that no identifier may be allocated: the reconstruction is
// incomplete, or some exchange's identifier is unknown.
func (l *mqttCustodyLedger) withholdsAllocation() bool {
	return l != nil && (l.reconstructionIncomplete || len(l.uncertainSubjects) > 0)
}

// holdsPI reports whether an admission or an exchange holds the identifier.
func (l *mqttCustodyLedger) holdsPI(pi uint16) bool {
	return l != nil && l.heldPIs[pi] > 0
}

// chargesPI reports whether a holder of the identifier still carries a Receive Maximum charge.
func (l *mqttCustodyLedger) chargesPI(pi uint16) bool {
	return l != nil && l.chargedPIs[pi] > 0
}

// countsEntry reports whether the ledger carries the charge of a custody scheduling entry:
// by its exchange lineage, or — before its admission settled — by its allocation nonce.
func (l *mqttCustodyLedger) countsEntry(p *mqttPending) bool {
	if l == nil || p == nil {
		return false
	}
	if p.lin != _EMPTY_ {
		if e, ok := l.heldExchanges[p.lin]; ok && !e.chargeReleased {
			return true
		}
	}
	if p.regNonce != _EMPTY_ {
		if _, ok := l.heldAdmissions[p.regNonce]; ok {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Re-observation targets
// ---------------------------------------------------------------------------

type mqttCustodyTargetKind uint8

const (
	mqttCustodyTargetExchange mqttCustodyTargetKind = iota + 1
	mqttCustodyTargetAdmission
	mqttCustodyTargetUncertain
)

// mqttCustodyTarget is one thing a re-observation pass can settle.
type mqttCustodyTarget struct {
	kind mqttCustodyTargetKind
	key  string // lineage, allocation nonce, or subject
	at   uint64
}

// targets returns up to budget targets in ascending lastObserved order: exchange entries,
// admissions with unsettled attempts, and uncertain subjects. Every attempt restamps its
// target, so a target that stays live or errors moves to the back and every target is reached
// within ceil(n/budget) passes.
func (l *mqttCustodyLedger) targets(gen string, budget int) []mqttCustodyTarget {
	if !l.forGen(gen) || budget <= 0 {
		return nil
	}
	all := make([]mqttCustodyTarget, 0, len(l.heldExchanges)+len(l.heldAdmissions)+len(l.uncertainSubjects))
	for lin, x := range l.heldExchanges {
		all = append(all, mqttCustodyTarget{mqttCustodyTargetExchange, lin, x.lastObserved})
	}
	for n, a := range l.heldAdmissions {
		if len(a.unsettled) > 0 {
			all = append(all, mqttCustodyTarget{mqttCustodyTargetAdmission, n, a.lastObserved})
		}
	}
	for s, t := range l.uncertainSubjects {
		all = append(all, mqttCustodyTarget{mqttCustodyTargetUncertain, s, t})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].at != all[j].at {
			return all[i].at < all[j].at
		}
		return strings.Compare(all[i].key, all[j].key) < 0
	})
	if len(all) > budget {
		all = all[:budget]
	}
	return all
}

// touchAdmission restamps an admission target.
func (l *mqttCustodyLedger) touchAdmission(gen, nonce string) {
	if !l.forGen(gen) {
		return
	}
	if a := l.heldAdmissions[nonce]; a != nil {
		a.lastObserved = l.nextTick()
	}
}

// hasExchange reports whether the lineage is charged, for the hint path's quick exit.
func (l *mqttCustodyLedger) hasExchange(gen, lin string) bool {
	if !l.forGen(gen) {
		return false
	}
	_, ok := l.heldExchanges[lin]
	return ok
}
