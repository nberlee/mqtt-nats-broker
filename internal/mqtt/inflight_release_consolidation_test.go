// C-IV's consolidation: the release authority table, the observation-bound release token,
// the extracted post-delete confirmation, and the identifier exclusion a removal site hands
// off (F-098). Plus the two HEAD defects this item closes, F-103 and F-104.

package mqtt

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nuid"
)

// TestSlotAuthorityPermitsReleaseIsTotal enumerates BOTH enums rather than sampling, so a
// disposition added later without a row fails here instead of inheriting a licence.
//
// The property it pins: Withhold and Reserved license NO release, and mqttSlotReplacePublish
// is not in the release class at all — it is destructive, being bound to the transmission
// identity, but it installs a successor rather than freeing an identifier.
func TestSlotAuthorityPermitsReleaseIsTotal(t *testing.T) {
	t.Parallel()
	auths := []mqttSlotAuthority{
		mqttSlotWithhold, mqttSlotTerminalAuthority,
		mqttSlotProvenUnsentAuthority, mqttSlotReservedAuthority,
	}
	// Every disposition the package defines, so a new one is a compile-visible addition
	// here and an automatic failure below.
	disps := []mqttInflightDisposition{
		mqttSlotDeleteComplete, mqttSlotDeleteTerminal, mqttSlotDeleteNeverTransmitted,
		mqttSlotMarkRetired, mqttSlotMarkCancelled, mqttSlotDrainAction,
		mqttSlotAcquireAction, mqttSlotTransferCustody, mqttSlotReplacePublish,
		mqttSlotAdvancePhase, mqttSlotDeletePublishTerminal, mqttSlotRebindCanonical,
		mqttSlotAdvanceAcknowledgedTerminal,
	}
	want := map[mqttSlotAuthority]map[mqttInflightDisposition]bool{
		mqttSlotTerminalAuthority: {
			mqttSlotDeleteTerminal:        true,
			mqttSlotDeletePublishTerminal: true,
		},
		mqttSlotProvenUnsentAuthority: {
			mqttSlotDeleteComplete:         true,
			mqttSlotDeleteNeverTransmitted: true,
		},
	}
	for _, a := range auths {
		for _, d := range disps {
			got := mqttSlotAuthorityPermitsRelease(a, d)
			if got != want[a][d] {
				t.Errorf("authority %d disposition %d: permits=%v, want %v", a, d, got, want[a][d])
			}
		}
	}
	// Stated separately because it is the invariant, not a row: "Reserved permits no
	// RELEASE" rather than "no destructive disposition".
	if !mqttSlotDestructiveDisposition(mqttSlotReplacePublish) {
		t.Fatal("replacement must remain destructive: it is bound to the transmission identity")
	}
	for _, a := range auths {
		if mqttSlotAuthorityPermitsRelease(a, mqttSlotReplacePublish) {
			t.Fatalf("replacement is not a release and no authority may license it as one (authority %d)", a)
		}
	}
}

// TestReleaseTokenRefusesAStaleObservation drives the two mismatches INDEPENDENTLY, with
// otherwise admissible inputs so no pre-existing fence can mask a mutation: an identity that
// moved, and a sequence that moved.
func TestReleaseTokenRefusesAStaleObservation(t *testing.T) {
	t.Parallel()
	const subject = "$MQTT.inflight.h.g.7"
	rec := &mqttInflightRecord{PI: 7, Nonce: "nnnnnnnnnnnnnnnnnnnnnn", Acq: 4}
	bound := mqttSlotIdentityOf(subject, rec)

	good := mqttSlotRelease{auth: mqttSlotProvenUnsentAuthority, bound: bound, seq: 100}
	if !good.permits(subject, rec, 100, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("an authority computed against this exact observation must be spendable on it")
	}

	// THE COUNTER MOVED. The evidence names Acq=4 while the occupant transmitted again, so
	// the release would discharge an obligation that is not this caller's (C-I).
	moved := &mqttInflightRecord{PI: 7, Nonce: rec.Nonce, Acq: 5}
	if good.permits(subject, moved, 100, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("a token bound to Acq=4 must not be spendable against an Acq=5 occupant")
	}

	// THE SEQUENCE MOVED, with the identity unchanged: a rewrite landed between the
	// observation and the boundary, so this authority is not about the record in hand.
	if good.permits(subject, rec, 101, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("a token observed at sequence 100 must not be spendable at sequence 101")
	}

	// The zero value carries Withhold, so a site that forgot to obtain one refuses rather
	// than inherits a licence.
	var forgotten mqttSlotRelease
	if forgotten.permits(subject, rec, 100, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("the zero release token must license nothing")
	}
}

// TestExclusionMergeIsAConjunction pins the merge, adoption and lift rules together, because
// each of them is only safe given the others.
func TestExclusionMergeIsAConjunction(t *testing.T) {
	t.Parallel()
	const (
		pi   = uint16(5)
		subA = "$MQTT.inflight.h.genA.5"
		subB = "$MQTT.inflight.h.genB.5"
		nonA = "aaaaaaaaaaaaaaaaaaaaaa"
		nonB = "bbbbbbbbbbbbbbbbbbbbbb"
	)
	newSess := func() *mqttSession { return &mqttSession{} }

	// EMPTY THEN NAMED must stay un-adoptable: promoting it would let a later nonce match
	// discharge a reservation whose occupant was never identified.
	s := newSess()
	s.mu.Lock()
	s.excludeIdentifierLocked(pi, subA, _EMPTY_)
	s.excludeIdentifierLocked(pi, subA, nonA)
	adopted := s.adoptIdentifierExclusionLocked(pi, subA, nonA)
	s.mu.Unlock()
	if adopted {
		t.Fatal("an exclusion carrying an unidentified bind must never become adoptable")
	}

	// NAMED THEN EMPTY, the reverse order, must be un-adoptable too.
	s = newSess()
	s.mu.Lock()
	s.excludeIdentifierLocked(pi, subA, nonA)
	s.excludeIdentifierLocked(pi, subA, _EMPTY_)
	adopted = s.adoptIdentifierExclusionLocked(pi, subA, nonA)
	s.mu.Unlock()
	if adopted {
		t.Fatal("an empty bind arriving over a named one must not leave the name adoptable")
	}

	// TWO DIFFERING NONCES: neither may adopt.
	s = newSess()
	s.mu.Lock()
	s.excludeIdentifierLocked(pi, subA, nonA)
	s.excludeIdentifierLocked(pi, subA, nonB)
	a1 := s.adoptIdentifierExclusionLocked(pi, subA, nonA)
	a2 := s.adoptIdentifierExclusionLocked(pi, subA, nonB)
	s.mu.Unlock()
	if a1 || a2 {
		t.Fatal("two differing identities make the exclusion permanently un-adoptable")
	}

	// A SINGLE NAMED BIND is the one adoptable shape — the recovered exchange reclaiming
	// its own exclusion.
	s = newSess()
	s.mu.Lock()
	s.excludeIdentifierLocked(pi, subA, nonA)
	adopted = s.adoptIdentifierExclusionLocked(pi, subA, nonA)
	held := slotExclusionHeldLocked(s.sendsInFlight[pi])
	s.mu.Unlock()
	if !adopted || held {
		t.Fatalf("a recovered exchange must adopt its own exclusion: adopted=%v stillHeld=%v", adopted, held)
	}

	// CONFLICTING SUBJECTS: one constituent's absence never discharges the conjunction.
	// subB carries a different GENERATION, which is exactly the case that matters because
	// exclusions survive a discard.
	s = newSess()
	s.mu.Lock()
	s.excludeIdentifierLocked(pi, subA, _EMPTY_)
	s.excludeIdentifierLocked(pi, subB, _EMPTY_)
	inst := s.slotExclusionInstLocked(pi)
	s.liftIdentifierExclusionLocked(pi, inst, map[string]bool{subA: true})
	partial := slotExclusionHeldLocked(s.sendsInFlight[pi])
	s.liftIdentifierExclusionLocked(pi, inst, map[string]bool{subA: true, subB: true})
	whole := slotExclusionHeldLocked(s.sendsInFlight[pi])
	s.mu.Unlock()
	if !partial {
		t.Fatal("absence of ONE bound subject must not lift a conjunction")
	}
	if whole {
		t.Fatal("absence of EVERY bound subject must lift it")
	}

	// A DELAYED ABSENCE, carrying an instance taken before a later install, must not clear
	// the reservation it never observed.
	s = newSess()
	s.mu.Lock()
	s.excludeIdentifierLocked(pi, subA, _EMPTY_)
	stale := s.slotExclusionInstLocked(pi)
	s.excludeIdentifierLocked(pi, subB, _EMPTY_)
	s.liftIdentifierExclusionLocked(pi, stale, map[string]bool{subA: true, subB: true})
	survived := slotExclusionHeldLocked(s.sendsInFlight[pi])
	s.mu.Unlock()
	if !survived {
		t.Fatal("a reservation strengthened after the observation must survive a stale lift")
	}

	// ABA ACROSS A HOLD'S DELETION AND RECREATION. Two readers observe the same instance,
	// one lifts, the hold is recreated — and the second reader must not lift the NEW
	// exclusion. A per-hold counter reset on recreation is exactly what this refuses.
	s = newSess()
	s.mu.Lock()
	s.excludeIdentifierLocked(pi, subA, _EMPTY_)
	first := s.slotExclusionInstLocked(pi)
	s.liftIdentifierExclusionLocked(pi, first, map[string]bool{subA: true})
	if _, stillThere := s.sendsInFlight[pi]; stillThere {
		s.mu.Unlock()
		t.Fatal("the hold must be gone once its last term is lifted")
	}
	s.excludeIdentifierLocked(pi, subA, _EMPTY_)
	s.liftIdentifierExclusionLocked(pi, first, map[string]bool{subA: true})
	recreatedHeld := slotExclusionHeldLocked(s.sendsInFlight[pi])
	s.mu.Unlock()
	if !recreatedHeld {
		t.Fatal("a stale instance from a previous hold must not lift the recreated exclusion")
	}

	// SET SEMANTICS: an equivalent restriction adds no protection and must not accumulate.
	s = newSess()
	s.mu.Lock()
	for i := 0; i < 5; i++ {
		s.excludeIdentifierLocked(pi, subA, nonA)
	}
	binds := len(s.sendsInFlight[pi].slotOwed.binds)
	s.mu.Unlock()
	if binds != 1 {
		t.Fatalf("an equivalent bind must not be duplicated: got %d binds", binds)
	}
}

// TestExclusionIsTheSixthLivenessTerm pins the property that makes the exclusion effective
// everywhere at once: the hold is undeletable while it stands, and bumpPI will not mint it.
func TestExclusionIsTheSixthLivenessTerm(t *testing.T) {
	t.Parallel()
	const pi = uint16(9)
	sess := &mqttSession{}
	sess.mu.Lock()
	sess.excludeIdentifierLocked(pi, "$MQTT.inflight.h.g.9", _EMPTY_)
	live := sess.sendHoldLiveLocked(sess.sendsInFlight[pi])
	reserved := sess.sendReservedLocked(pi)
	// The sweep asks the same predicate, so an excluded hold must survive it.
	sess.sweepDeadSendHoldsLocked()
	_, survived := sess.sendsInFlight[pi]
	sess.mu.Unlock()
	if !live || !reserved {
		t.Fatalf("an exclusion must make the hold live and the identifier reserved: live=%v reserved=%v", live, reserved)
	}
	if !survived {
		t.Fatal("the sweep must not reap a hold carrying an exclusion")
	}
}

// TestStagedVerdictIsNotProvenUnsent is F-103's oracle.
//
// mqttSlotEvidenceFor used to read BARE MEMBERSHIP in voidedRegNonces, which includes a
// verdict that is merely STAGED — voidRegNonceLocked stamps an insertion revision that the
// rest of the tree compares against voidedRegsSavedRev before treating the verdict as
// durable. So C-IV's one existing caller could be handed a proven-unsent authority on a fact
// no restart can see, and free an identifier over it.
//
// This drives the authority function directly, because the defect is in what counts as
// evidence rather than in any one caller.
func TestStagedVerdictIsNotProvenUnsent(t *testing.T) {
	t.Parallel()
	const (
		subject = "$MQTT.inflight.h.g.3"
		nonce   = "cccccccccccccccccccccc"
	)
	rec := &mqttInflightRecord{PI: 3, Nonce: nonce, State: mqttInflightPublish}
	ev := mqttSlotEvidence{
		readable: true,
		rec:      rec,
		bound:    mqttSlotIdentityOf(subject, rec),
		// The marker is authoritatively ABSENT, so the exchange is possibly-attempted and
		// the VERDICT is the only thing that could make it proven-unsent. That isolation is
		// the point: nothing else can carry this fixture.
		marker: mqttSlotMarkerAbsent,
	}
	if got := mqttSlotReleaseAuthority(ev); got != mqttSlotReservedAuthority {
		t.Fatalf("a possibly-attempted slot with no durable cancellation must RESERVE, got %d", got)
	}
	// RE-EXPRESSED by F-097/F-088 (plan rev 10, WP3 edit 1). This fixture used to assert that a
	// DURABLY COVERED verdict makes the slot releasable; that is the deleted premise (a
	// cancellation decision is not a proof of non-transmission), and the evidence member that
	// carried the verdict is gone. What stays true, and is asserted: the record's own
	// cancellation bit, the other carrier of the same decision, does not release a
	// possibly-attempted slot either; only the standing marker does.
	rec.Cancelled, rec.CancelOrigin = true, mqttCancelOriginOccupant // F-088: provenance is written
	if got := mqttSlotReleaseAuthority(ev); got == mqttSlotProvenUnsentAuthority {
		t.Fatal("a cancellation over a possibly-attempted slot must not yield proven-unsent authority")
	}
	ev.marker = mqttSlotMarkerPresent
	if got := mqttSlotReleaseAuthority(ev); got != mqttSlotProvenUnsentAuthority {
		t.Fatalf("a standing marker for this allocation is the proof of non-transmission, got %d", got)
	}
}

// TestEvidenceLoaderRequiresADurablyCoveredVerdict is F-103 at the LOADER, which is where the
// staged/durable distinction actually has to be drawn — the fixture above pins the function's
// reading of the term, this one pins how the term is computed.
func TestEvidenceLoaderRequiresADurablyCoveredVerdict(t *testing.T) {
	t.Parallel()
	const nonce = "dddddddddddddddddddddd"
	sess := &mqttSession{}
	sess.mu.Lock()
	sess.voidRegNonceLocked(nonce)
	stagedRev := sess.voidedRegsRev
	insertRev, held := sess.voidedRegNonces[nonce]
	savedRev := sess.voidedRegsSavedRev
	sess.mu.Unlock()
	if !held {
		t.Fatal("the nonce must be staged")
	}
	if insertRev <= savedRev {
		t.Fatalf("a freshly staged verdict must NOT be durably covered: insertRev=%d savedRev=%d", insertRev, savedRev)
	}
	// The credit point is what makes it durable, and it is keyed on the revision the write
	// MARSHALLED — a nonce staged after that marshal is durable nowhere.
	sess.mu.Lock()
	sess.creditVoidedRegsLocked(stagedRev)
	insertRev, _ = sess.voidedRegNonces[nonce]
	nowSaved := sess.voidedRegsSavedRev
	sess.mu.Unlock()
	if insertRev > nowSaved {
		t.Fatalf("a credited save must make the verdict durably covered: insertRev=%d savedRev=%d", insertRev, nowSaved)
	}
}

// TestConfirmationDistinguishesAbsenceFromReplacement is the extracted post-delete
// confirmation's oracle, and the reason it has FOUR outcomes rather than a bool.
//
// deleteInflightRegisterConfirmed reads an ALREADY-ABSENT sequence as success, so a
// same-identity rewrite between a caller's load and its delete is indistinguishable from a
// removal. Only an empty subject frees an identifier; a different occupant discharges the
// caller's own obligation but never the successor's identifier; and an unreadable or
// indeterminate read licenses nothing at all.
func TestConfirmationDistinguishesAbsenceFromReplacement(t *testing.T) {
	t.Parallel()
	// The four outcomes must be distinct values, and `discharged` must cover exactly the two
	// established ones — a caller that treats Refuted or Unresolved as discharged would
	// release a hold over a record a rewrite left racing its delete.
	if mqttReleaseEstablishedAbsent == mqttReleaseEstablishedOther {
		t.Fatal("absence and replacement must be distinguishable: only absence frees an identifier")
	}
	for _, tc := range []struct {
		outcome    mqttReleaseConfirmed
		discharged bool
		name       string
	}{
		{mqttReleaseEstablishedAbsent, true, "absent"},
		{mqttReleaseEstablishedOther, true, "replaced"},
		{mqttReleaseRefuted, false, "our own record survives"},
		{mqttReleaseUnresolved, false, "indeterminate or unreadable"},
	} {
		if got := tc.outcome.discharged(); got != tc.discharged {
			t.Errorf("%s: discharged=%v, want %v", tc.name, got, tc.discharged)
		}
	}
	// The zero value is Unresolved, so a forgotten assignment licenses nothing.
	var forgotten mqttReleaseConfirmed
	if forgotten != mqttReleaseUnresolved || forgotten.discharged() {
		t.Fatal("the zero confirmation must be Unresolved and discharge nothing")
	}
}

// TestEvidenceForRefusesAStagedVerdict is F-103's REAL oracle, and it exists because the two
// fixtures above did not kill the defect.
//
// Mutating mqttSlotEvidenceFor back to bare membership SURVIVED them: one drives the
// authority function with a hand-built evidence value, the other drives the staging
// mechanics, and neither asks the LOADER what it puts in the verdict field. A fixture that
// cannot reach the code it names proves nothing about it, so this one goes through
// mqttSlotEvidenceFor against a real stored register.
func TestEvidenceForRefusesAStagedVerdict(t *testing.T) {
	e := testNewInflightEnv(t, "client-f103-evidence")
	const pi = uint16(11)
	nonce := mqttNewInflightNonce()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	rec := freshRegisterLineage(&mqttInflightRecord{
		PI: pi, Dur: "SRC", Seq: 7, QoS: 2, Nonce: nonce,
		State: mqttInflightPublish,
		Claim: e.sess.regClaim, ClaimSeq: 1,
	})
	if _, err := e.asm.jsa.storeInflightCAS(subject, 0, rec); err != nil {
		t.Fatalf("seeding the register: %v", err)
	}

	// STAGED ONLY: the nonce is in the set, but the save carrying it has not landed.
	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked(nonce)
	staged := e.sess.voidedRegsRev
	e.sess.mu.Unlock()

	ev, cur, _ := e.sess.mqttSlotEvidenceFor(subject, mqttSlotIdentity{subject: subject, nonce: nonce})
	if cur == nil {
		t.Fatal("the register must load: this fixture is about what the loader concludes from it")
	}
	// With the marker absent this slot is possibly-attempted, so a staged verdict must not
	// free it.
	if got := mqttSlotReleaseAuthority(ev); got == mqttSlotProvenUnsentAuthority {
		t.Fatal("a staged verdict must not license releasing a possibly-attempted identifier")
	}

	// RE-EXPRESSED by F-097/F-088 (plan rev 10, WP3 edit 1). This fixture used to assert that,
	// once the save lands, the DURABLY COVERED verdict reaches the evidence and so licenses the
	// release. That is the deleted premise: a void verdict proves nothing about
	// transmission, and the loader no longer reads verdicts at all. Asserted instead: a durably
	// covered verdict still leaves the possibly-attempted slot reserved.
	e.sess.mu.Lock()
	e.sess.creditVoidedRegsLocked(staged)
	e.sess.mu.Unlock()
	ev, _, _ = e.sess.mqttSlotEvidenceFor(subject, mqttSlotIdentity{subject: subject, nonce: nonce})
	if got := mqttSlotReleaseAuthority(ev); got == mqttSlotProvenUnsentAuthority {
		t.Fatal("a durably covered verdict must not license releasing a possibly-attempted identifier (F-088)")
	}
}

// TestVerdictSurvivesADeleteThatRemovedNothing is F-104's oracle.
//
// deleteInflightRegisterConfirmed reports SUCCESS for an already-absent sequence. So a
// caller holding an observation at sequence S, while a same-identity rewrite moved the
// record to S2, deletes S, is told it succeeded, and retires the void verdict —
// leaving the SURVIVING register with neither its verdict nor necessarily a marker, which
// makes it permanently unreleasable. A retained verdict is re-derivable evidence; a
// forgotten one is unrecoverable, so anything short of a proven absence must keep it.
func TestVerdictSurvivesADeleteThatRemovedNothing(t *testing.T) {
	e := testNewInflightEnv(t, "client-f104-verdict")
	const pi = uint16(12)
	nonce := mqttNewInflightNonce()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	rec := freshRegisterLineage(&mqttInflightRecord{
		PI: pi, Dur: "SRC", Seq: 9, QoS: 2, Nonce: nonce,
		State: mqttInflightPublish,
		Claim: e.sess.regClaim, ClaimSeq: 1,
	})
	seq, err := e.asm.jsa.storeInflightCAS(subject, 0, rec)
	if err != nil {
		t.Fatalf("seeding the register: %v", err)
	}

	// A DURABLE void verdict names this allocation.
	e.sess.mu.Lock()
	e.sess.voidRegNonceLocked(nonce)
	e.sess.creditVoidedRegsLocked(e.sess.voidedRegsRev)
	e.sess.mu.Unlock()

	// A SAME-IDENTITY REWRITE lands: a retirement mark, a custody write — anything that
	// moves the record to a new sequence without changing whose exchange it is.
	rewritten := *rec
	rewritten.Retired = true
	if _, err := e.asm.jsa.storeInflightCAS(subject, seq, &rewritten); err != nil {
		t.Fatalf("rewriting the register: %v", err)
	}

	// The caller deletes at the sequence IT observed, which is now absent, so the confirmed
	// delete reports success. The verdict must nonetheless survive, because the register the
	// verdict describes is still there.
	if !e.sess.reapRegisterWithVerdict(seq, nonce, true, rec, subject, nil) {
		t.Fatal("the delete of an already-absent sequence reports success: that is the premise of this fixture")
	}
	e.sess.mu.Lock()
	_, stillHeld := e.sess.voidedRegNonces[nonce]
	e.sess.mu.Unlock()
	if !stillHeld {
		t.Fatal("the verdict was retired against a delete that removed nothing (F-104): the surviving register is now unreleasable")
	}

	// And the positive half, so the gate is not simply refusing everything: once the record
	// is genuinely gone, the same call retires the verdict.
	cur, smsg, lerr := e.asm.loadInflightRecord(subject)
	if lerr != nil || cur == nil {
		t.Fatalf("the rewritten register must be readable: %v", lerr)
	}
	if !e.sess.reapRegisterWithVerdict(smsg.Sequence, nonce, true, cur, subject, nil) {
		t.Fatal("deleting the record at its current sequence must succeed")
	}
	e.sess.mu.Lock()
	_, heldAfter := e.sess.voidedRegNonces[nonce]
	e.sess.mu.Unlock()
	if heldAfter {
		t.Fatal("a CONFIRMED absence must retire the verdict, or it accumulates for the session's life")
	}
}

// TestRemovalHandsOffIdentifierExclusion is the removal sites' oracle: the entry goes
// exactly as it did before, and the IDENTIFIER's exclusion is handed off in the same
// critical section.
//
// The two operations are separate, and conflating them is what broke three contracts in
// review: the drain's cancellation handoff needs its entry REMOVED (cancelRegPendingLocked
// records only a nonce verdict, so a retained entry would let the returning store's
// confirmation admit a delivery the drain does not claim), while the identifier must stay
// excluded because that store may still land a register a peer can act on.
func TestRemovalHandsOffIdentifierExclusion(t *testing.T) {
	t.Parallel()
	const pi = uint16(4)
	nonce := "eeeeeeeeeeeeeeeeeeeeee"
	sess := &mqttSession{idHash: "hash", gen: "gen"}
	subject := sess.inflightSubjectLocked(pi)

	sess.mu.Lock()
	sess.pendingPublish = map[uint16]*mqttPending{pi: {regNonce: nonce, regPending: true}}
	// The removal, exactly as a site performs it: hand off, then untrack.
	sess.excludeIdentifierLocked(pi, subject, nonce)
	sess.untrackPublishEntry(pi, testUntrackNoBinding)
	_, entryGone := sess.pendingPublish[pi]
	reserved := sess.sendReservedLocked(pi)
	charged := sess.receiveMaxOccupancyLocked()
	sess.mu.Unlock()

	if entryGone {
		t.Fatal("the entry must still be removed: its removal is the site's own contract")
	}
	if !reserved {
		t.Fatal("the identifier must stay excluded: the durable register may have landed")
	}
	if charged != 0 {
		t.Fatalf("the exclusion must charge no Receive Maximum capacity, got %d", charged)
	}

	// And bumpPI must skip it, which is the whole point — it consults this registry and
	// never the durable register, so nothing else can refuse the allocation.
	sess.mu.Lock()
	sess.last_pi = pi - 1
	minted := sess.bumpPI()
	sess.mu.Unlock()
	if minted == pi {
		t.Fatalf("bumpPI must not hand out an excluded identifier, got %d", minted)
	}
	if minted == 0 {
		t.Fatal("the exclusion must skip one identifier, not exhaust the space")
	}
}

// TestInflightSubjectLockedCarriesTheGeneration pins the property the conjunction's subject
// binding rests on: an exclusion names the generation it was taken in, so an absence of the
// CURRENT generation's subject cannot discharge one retained for an older generation.
func TestInflightSubjectLockedCarriesTheGeneration(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{idHash: "hash", gen: "genA"}
	sess.mu.Lock()
	a := sess.inflightSubjectLocked(3)
	sess.gen = "genB"
	b := sess.inflightSubjectLocked(3)
	sess.idHash = _EMPTY_
	none := sess.inflightSubjectLocked(3)
	sess.mu.Unlock()
	if a == b {
		t.Fatal("two generations must not share a slot subject, or an exclusion could be lifted by the wrong one")
	}
	if none != _EMPTY_ {
		t.Fatal("a session that cannot name a subject must return none rather than guess")
	}
}

// TestPubRelOpenerCarriesTheExclusionAcrossAReplacement is round 9's finding 2.
//
// beginPubRelSendLocked REPLACES a hold whose connection owner differs and deliberately
// preserves only `action`, so a successor can carry a durable token across the replacement.
// An exclusion starts with owner zero, so without being carried it would be silently erased
// by a legitimate recovered PUBREL — losing the identifier's protection for an exchange a
// peer may still be transmitting.
//
// The opener must not be BLOCKED by the exclusion either: PUBREL custody is legitimate, and
// refusing it here would strand the exchange instead of protecting it.
func TestPubRelOpenerCarriesTheExclusionAcrossAReplacement(t *testing.T) {
	t.Parallel()
	const pi = uint16(6)
	subject := "$MQTT.inflight.h.g.6"
	sess := &mqttSession{idHash: "h", gen: "g"}
	// A hold owned by a PREVIOUS connection, carrying an exclusion.
	sess.mu.Lock()
	sess.sendsInFlight = map[uint16]*mqttSendHold{pi: {owner: 111}}
	sess.excludeIdentifierLocked(pi, subject, _EMPTY_)
	before := sess.slotExclusionInstLocked(pi)
	sess.mu.Unlock()
	if before == 0 {
		t.Fatal("the exclusion must be installed before the replacement")
	}

	// A DIFFERENT connection opens a PUBREL send, which replaces the hold.
	cc := &client{cid: 222}
	sess.mu.Lock()
	sess.c = cc
	tok, ok := sess.beginPubRelSendLocked(cc, pi)
	carried := sess.slotExclusionInstLocked(pi)
	stillReserved := sess.sendReservedLocked(pi)
	sess.mu.Unlock()

	if !ok || tok == _EMPTY_ {
		t.Fatal("the exclusion must not block legitimate PUBREL custody: that would strand the exchange")
	}
	if carried != before {
		t.Fatalf("the replacement must carry the exclusion, exactly as it carries the action: inst %d -> %d", before, carried)
	}
	if !stillReserved {
		t.Fatal("the identifier must remain reserved across the hold replacement")
	}
}

// A DEFERRED SETTLEMENT HANDS OFF THE IDENTIFIER'S EXCLUSION BEFORE IT UNTRACKS (F-098,
// consult D-7). When a removal path condemns an entry whose send section is still open,
// `endSendWith` performs the untrack later — and for a settlement whose durable delete may
// be refused by a freshness fence, fail, or never have been issued, that untrack would
// otherwise be the moment the identifier became allocatable over a slot nobody has confirmed
// gone. The register is still there; nothing has proven its subject empty.
//
// The two arms are the discrimination, and neither proves the property alone:
//
//   - a REGISTER-BACKED denial leaves the identifier EXCLUDED, because a durable record
//     stands behind it;
//   - a REGISTERLESS RETAINED send leaves it ALLOCATABLE, because a retained send has no
//     durable slot at all (R6) and an exclusion bound to its minted nonce would reserve an
//     identifier against a register that never existed.
//
// Without the second arm the first would pass against an implementation that excluded
// unconditionally, which is a different and wrong rule.
//
// Mutation: delete the mqttSendSettleOwesExclusion install from endSendWith's default arm.
// The register-backed arm then reports the identifier allocatable.
func TestMQTTDeferredSettlementExcludesOnlyARegisterBackedIdentifier(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-deferred-exclusion")
	dur := e.sess.idHash + "_deferred"
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	e.sess.c = e.c
	if e.c.cid == 0 {
		// beginRetainedSendLocked refuses a zero cid outright — zero is an uninitialized
		// connection identity, not one — so the retained arm below needs a real one.
		e.c.cid = 1
	}
	token, ok := e.sess.beginSendLocked(e.c, pi, dur, sseq)
	parked := ok && e.sess.condemnSendLocked(pi, mqttSendSettleDenyRelease)
	e.sess.mu.Unlock()
	if !ok || !parked {
		t.Fatal("fixture: the denial must park on the open send section rather than delete the entry")
	}

	e.sess.endSend(e.c, pi, token.token)
	e.sess.mu.Lock()
	after := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if after != nil {
		t.Fatalf("the parked untrack must still run: entry=%+v", after)
	}
	if testInflightMints(t, e.sess, pi) {
		t.Fatalf("identifier %d became allocatable when its denial settled: the register's "+
			"delete may have been refused or never issued, and nothing has confirmed the "+
			"subject empty", pi)
	}

	// THE REGISTERLESS ARM, and what it actually shows is STRUCTURAL. A retained send owns
	// no register, and it turns out it cannot reach the deferred-settlement path at all: a
	// settlement is parked by condemnSendLocked, which requires the entry's `sendNonce`,
	// and only beginSendLocked installs one — a call that matches the entry against a
	// non-empty source durable. A retained send opens its section through
	// beginRetainedSendLocked, which records `retainedGen` on the HOLD and leaves the entry
	// without a nonce.
	//
	// So the exemption in endSendWith is a defensive agreement between two mechanisms, not a
	// live case, and this asserts the fact that makes it so. Asserting it here is what stops
	// the guard being read as evidence that a retained frame settles through that path —
	// and it is the assertion that would fail first if a future change gave a retained entry
	// a send nonce, which is exactly when that guard starts carrying weight.
	e.sess.mu.Lock()
	rpi := e.sess.trackPublishRetained(1)
	retainedOpened := rpi != 0 && e.sess.beginRetainedSendLocked(e.c, rpi)
	retainedNonce := _EMPTY_
	if rpi != 0 && e.sess.pendingPublish[rpi] != nil {
		retainedNonce = e.sess.pendingPublish[rpi].sendNonce
	}
	retainedCondemned := retainedOpened && e.sess.condemnSendLocked(rpi, mqttSendSettleRecoveryDrop)
	e.sess.mu.Unlock()
	if !retainedOpened {
		t.Fatalf("fixture: the retained send must open its own section (pi=%d)", rpi)
	}
	if retainedNonce != _EMPTY_ {
		t.Fatalf("a retained entry carries no send nonce, got %q: if it now does, the "+
			"registerless exemption in endSendWith has become a LIVE case and needs its own "+
			"end-to-end oracle", retainedNonce)
	}
	if retainedCondemned {
		t.Fatal("a retained send was condemned onto the deferred-settlement path: it has no " +
			"durable slot, so nothing there could be about its identifier")
	}
}

// AN UNCONFIRMED DELETE IS NOT A RELEASE (F-098). `confirmSlotReleased` is the one
// definition of what a delete actually established, and it exists because an exact-sequence
// delete reports success for an already-absent sequence — so "the delete returned nil" and
// "this exchange's record is gone" are different facts.
//
// The partition is asserted directly because the DIRECTION of the error case is the whole
// safety property: an indeterminate read must be Unresolved, never EstablishedAbsent. A
// read that failed proves nothing, and treating it as an absence lifts an identifier's
// exclusion and reports a client obligation discharged on the strength of a failed round
// trip.
//
// Mutation: return mqttReleaseEstablishedAbsent for the load-error case. This test fails at
// its first row, and TestMQTTTerminalCleanupHoldsAnUnconfirmedRelease fails behaviourally.
func TestConfirmSlotReleasedNeverReadsAFailedReadAsAnAbsence(t *testing.T) {
	t.Parallel()
	const subject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.7"
	ours := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 7, Dur: "$MQTT_abc", Seq: 41, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	other := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 7, Dur: "$MQTT_other", Seq: 99, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	for _, tc := range []struct {
		name string
		load func(string) (*mqttInflightRecord, *StoredMsg, error)
		want mqttReleaseConfirmed
		why  string
	}{
		{
			name: "an indeterminate read is UNRESOLVED, never an absence",
			load: func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return nil, nil, errors.New("test: indeterminate")
			},
			want: mqttReleaseUnresolved,
			why:  "a read that failed establishes nothing, and an absence is the one fact that frees an identifier",
		},
		{
			name: "an empty subject is an established ABSENCE",
			load: func(string) (*mqttInflightRecord, *StoredMsg, error) { return nil, nil, nil },
			want: mqttReleaseEstablishedAbsent,
			why:  "the subject provably holds nothing",
		},
		{
			name: "present but unreadable is UNRESOLVED",
			load: func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return nil, &StoredMsg{Sequence: 5, Subject: subject}, nil
			},
			want: mqttReleaseUnresolved,
			why:  "a record this broker cannot interpret is not an absence",
		},
		{
			name: "our own record, rewritten, REFUTES the release",
			load: func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return ours, &StoredMsg{Sequence: 6, Subject: subject}, nil
			},
			want: mqttReleaseRefuted,
			why:  "the delete raced a same-identity rewrite and removed nothing",
		},
		{
			name: "a successor is an established release of OURS",
			load: func(string) (*mqttInflightRecord, *StoredMsg, error) {
				return other, &StoredMsg{Sequence: 7, Subject: subject}, nil
			},
			want: mqttReleaseEstablishedOther,
			why:  "ours is gone, but the identifier belongs to the successor now",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			as := &mqttAccountSessionManager{}
			as.mu.Lock()
			as.testInflightLoad = tc.load
			as.mu.Unlock()
			sess := &mqttSession{asm: as}
			if got := sess.confirmSlotReleased(subject, ours); got != tc.want {
				t.Fatalf("confirmSlotReleased = %d, want %d: %s", got, tc.want, tc.why)
			}
		})
	}
	// AND THE TWO ESTABLISHED FORMS BOTH DISCHARGE, while neither withholding form does.
	// The difference between them matters to the IDENTIFIER, not to the obligation.
	for r, want := range map[mqttReleaseConfirmed]bool{
		mqttReleaseUnresolved:        false,
		mqttReleaseRefuted:           false,
		mqttReleaseEstablishedAbsent: true,
		mqttReleaseEstablishedOther:  true,
	} {
		if got := r.discharged(); got != want {
			t.Errorf("discharged(%d) = %v, want %v", r, got, want)
		}
	}
}

// THE SAME PROPERTY THROUGH ITS PRODUCTION ROUTE, because the partition above is a helper
// test and a helper test cannot show that anything consults it.
//
// Terminal cleanup deletes the slot and then CONFIRMS the subject. Both arms delete
// successfully; they differ only in what the confirming read returns, and that difference
// alone decides whether the obligation is reported discharged and whether the identifier's
// exclusion is lifted.
//
// The seam is triggered BY THE DELETE — the hook performs the real delete and then arms the
// failure — so the failing read is the confirmation and not some earlier probe. A read
// count would encode the current number of probes and break when one is added.
//
// Mutation: return mqttReleaseEstablishedAbsent for confirmSlotReleased's load-error case.
// The indeterminate arm then reports the obligation discharged and frees the identifier.
func TestMQTTTerminalCleanupHoldsAnUnconfirmedRelease(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		failAfterDel  bool
		wantDischarge bool
		wantMintable  bool
	}{
		{"a confirmed absence lifts the exclusion", false, true, true},
		{"an indeterminate confirmation holds it", true, false, false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "client-unconfirmed-"+nuid.Next())
			dur := e.sess.idHash + "_" + nuid.Next()
			sseq, reply := e.publishOfferedSource(t, dur)
			pi := e.trackAdmitted(t, dur, sseq, 1, reply)
			subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

			e.sess.mu.Lock()
			ack := e.sess.pendingPublish[pi]
			e.sess.mu.Unlock()
			if ack == nil {
				t.Fatal("fixture: the tracked entry must be resident")
			}
			// The acknowledged PUBLISH's own phase advance, which is what gives terminal
			// cleanup its authority (arm 3).
			terminal, before := testRegisterAt(t, e, pi)
			testAdvanceTerminalPair(t, e, subject, before.Sequence, terminal)
			// An exclusion to observe. Installed through the production helper, bound to
			// this allocation, exactly as a removal site that could not confirm the slot
			// would leave it.
			e.sess.mu.Lock()
			e.sess.excludeIdentifierLocked(pi, subject, ack.regNonce)
			delete(e.sess.pendingPublish, pi)
			e.sess.mu.Unlock()
			if testInflightMints(t, e.sess, pi) {
				t.Fatalf("fixture: identifier %d must be excluded before the cleanup runs", pi)
			}

			cur, smsg, lerr := e.asm.loadInflightRecord(subject)
			if lerr != nil || cur == nil || smsg == nil {
				t.Fatalf("fixture: the publish_terminal slot must be readable: rec=%+v err=%v", cur, lerr)
			}

			var armed atomic.Bool
			jsa := &e.asm.jsa
			e.asm.mu.Lock()
			e.asm.testInflightDelete = func(seq uint64) error {
				err := jsa.deleteMsg(mqttInflightStreamName, seq, true)
				if tc.failAfterDel {
					armed.Store(true) // every read from here on is the confirmation
				}
				return err
			}
			e.asm.testInflightLoad = func(subj string) (*mqttInflightRecord, *StoredMsg, error) {
				if armed.Load() && subj == subject {
					return nil, nil, errors.New("test: the confirming read was indeterminate")
				}
				return jsa.loadInflightRecord(subj)
			}
			e.asm.mu.Unlock()

			got := e.sess.recoverPublishTerminalSlot(e.c, subject, smsg.Sequence, cur)
			if got != tc.wantDischarge {
				t.Fatalf("terminal cleanup discharged = %v, want %v: a delete whose subject "+
					"consequence could not be read establishes nothing", got, tc.wantDischarge)
			}
			if mintable := testInflightMints(t, e.sess, pi); mintable != tc.wantMintable {
				t.Fatalf("identifier %d allocatable = %v, want %v: only a CONFIRMED absence "+
					"lifts an exclusion", pi, mintable, tc.wantMintable)
			}
		})
	}
}

// A COLD RECOVERY DROP RESERVES ITS IDENTIFIER UNTIL THE DELETE IS PROVEN (F-098, holistic
// review round 1, finding 1).
//
// A staged drop is not a completed one. The drop loop runs after the apply phase and its
// delete can be REFUSED by the freshness fence — a peer rewrote the slot, or acquired an
// action on it — or can simply fail. On a COLD resume there is no resident `pendingPublish`
// entry to carry the obligation, so if recovery finishes without reserving the identifier
// then `bumpPI`, and with it `trackPublishRetained`, hands it out while the durable register
// still stands and a peer may still be transmitting under it.
//
// The first repair covered only the retired/source-complete arm. This covers the rest of the
// candidate population, which is the larger one: a marker-proven PUBLISH whose source is
// Complete or Lost, and a cancellation candidate routed by nonce.
//
// Both arms are needed and neither proves the property alone. The REFUSED arm shows the
// identifier held when the delete does not land; the CONFIRMED arm shows it given back when
// the subject is proven empty, which is what stops the fix being "reserve everything
// forever" — a rule that would pass the first assertion and strand every recovered
// identifier for the life of the session.
//
// Mutation: install the drop-candidate exclusions inside the resident-entry branch, or drop
// the apply-phase loop entirely. The refused arm then reports the identifier allocatable.
func TestMQTTColdRecoveryDropReservesUntilItsDeleteIsProven(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		blockDelete  bool
		wantMintable bool
	}{
		{"a refused delete keeps the identifier reserved", true, false},
		{"a confirmed absence gives it back", false, true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "cold-drop-"+nuid.Next())
			dur := e.sess.idHash + "_" + nuid.Next()
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 1)
			subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

			// The source is acknowledged, so this register's classification reaches the
			// drop population rather than being reserved as possibly-attempted.
			if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
				t.Fatalf("delete the source message: %v", err)
			}

			if tc.blockDelete {
				// THE DELETE DOES NOT LAND. A refusal and a failure are the same thing to
				// the identifier: in both, recovery ends with the durable register standing
				// and nothing having confirmed its subject empty.
				e.asm.mu.Lock()
				e.asm.testInflightDelete = func(uint64) error {
					return errors.New("test: the register delete did not land")
				}
				e.asm.mu.Unlock()
			}

			// A COLD resume: no in-memory entry and no hold survives, so the only thing
			// that can reserve this identifier is what recovery installs from the durable
			// record itself.
			e.forgetInMemory()
			e.sess.mu.Lock()
			e.sess.sendsInFlight = nil
			e.sess.mu.Unlock()

			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}

			if mintable := testInflightMints(t, e.sess, pi); mintable != tc.wantMintable {
				t.Fatalf("identifier %d allocatable = %v, want %v (subject %q): a cold resume "+
					"has no resident entry, so an unproven drop must leave the identifier "+
					"reserved and a proven one must release it", pi, mintable, tc.wantMintable, subject)
			}
		})
	}
}

// A REASSERTED EXCLUSION INVALIDATES EVERY ABSENCE PROOF TAKEN BEFORE IT (F-098, holistic
// review round 2, finding 1).
//
// The exclusion's binds are a SET, so an identical restriction is not stored twice. That is
// storage deduplication, and an earlier revision let it deduplicate the EVENT too: a
// reassertion of an existing bind returned without advancing the instance counter. This is
// the schedule that made unsafe, and none of it needs a crash or an exotic shape:
//
//  1. a release continuation captures instance N and reads the subject as ABSENT;
//  2. a peer installs a successor on that subject;
//  3. terminal cleanup's mismatch arm reasserts the same (subject, EMPTY nonce) bind —
//     unnamed, because a successor is not ours to name;
//  4. the continuation from step 1 lifts on its stale absence, and the identifier becomes
//     allocatable while the successor's register stands.
//
// Capturing the instance before the read — which every lift site does — does not fence step
// 3, because the reassertion happens after the capture and changed nothing observable. Only
// advancing the counter on reassertion does.
//
// The assertion is about the IDENTIFIER, through the allocator, not about the struct: a test
// that only inspected binds would pass against an implementation that kept the bind and lost
// the reservation.
//
// Mutation: restore the bare `return` in excludeIdentifierLocked's duplicate-bind arm. The
// stale lift then succeeds and the final assertion fails.
func TestReassertingAnExclusionInvalidatesAStaleAbsenceProof(t *testing.T) {
	t.Parallel()
	const (
		pi      = uint16(9)
		subject = mqttInflightStreamSubjectPrefix + "abcdefgh.gggggggggggggggggggggg.9"
	)
	sess := &mqttSession{}

	// A removal site that could not name the occupant leaves the unnamed shape.
	sess.mu.Lock()
	sess.excludeIdentifierLocked(pi, subject, _EMPTY_)
	// THE CONTINUATION'S CAPTURE, taken before its confirming read, exactly as
	// recoverPublishTerminalSlot, finalizeTerminalSlot and settleRecoveryDropExclusion take
	// it. Everything after this models what happens while that read is out.
	stale := sess.slotExclusionInstLocked(pi)
	sess.mu.Unlock()
	if stale == 0 {
		t.Fatal("fixture: the exclusion must be installed before its instance is captured")
	}
	if testInflightMints(t, sess, pi) {
		t.Fatalf("fixture: identifier %d must be excluded to begin with", pi)
	}

	// ...the read returns ABSENT, and in that window a successor takes the subject and the
	// mismatch arm reasserts the SAME unnamed bind.
	sess.mu.Lock()
	sess.excludeIdentifierLocked(pi, subject, _EMPTY_)
	binds := len(sess.sendsInFlight[pi].slotOwed.binds)
	fresh := sess.slotExclusionInstLocked(pi)
	sess.mu.Unlock()
	if binds != 1 {
		t.Fatalf("an equivalent restriction must not be stored twice, got %d binds", binds)
	}
	if fresh == stale {
		t.Fatal("the reassertion left the instance unchanged, so every absence observed " +
			"before it still appears current: deduplicating the STORAGE must not " +
			"deduplicate the EVENT")
	}

	// The continuation now tries to spend its stale proof.
	sess.mu.Lock()
	sess.liftIdentifierExclusionLocked(pi, stale, map[string]bool{subject: true})
	sess.mu.Unlock()
	if testInflightMints(t, sess, pi) {
		t.Fatalf("identifier %d became allocatable on an absence observed BEFORE the "+
			"exclusion was reasserted: a successor's register still stands on %q", pi, subject)
	}

	// AND THE FRESH PROOF STILL WORKS, or the fix would be "never lift anything", which
	// passes the assertion above and strands every identifier it touches.
	sess.mu.Lock()
	sess.liftIdentifierExclusionLocked(pi, fresh, map[string]bool{subject: true})
	sess.mu.Unlock()
	if !testInflightMints(t, sess, pi) {
		t.Fatalf("identifier %d stayed excluded against an absence observed AFTER the "+
			"reassertion: a current proof must still discharge the restriction", pi)
	}
}

// EXPIRY OF A RECOVERED, REGISTER-BACKED ENTRY RESERVES ITS IDENTIFIER UNTIL THE DELETE IS
// PROVEN (F-098, holistic review round 3).
//
// Message Expiry may drop a PUBLISH proven never transmitted, and that drop is licensed. What
// is not licensed is treating the untrack as the moment the identifier becomes free: the
// register delete can be refused by the freshness fence — a peer acquired or rewrote the slot
// — or can simply fail, and the expiry path ignores its return by design, because the source
// acknowledgement must not wait on it.
//
// The population is narrow and is exactly the one recovery's own exclusions cannot reach: a
// RECOVERED never-transmitted entry, restored as a live source and expired on a later
// delivery. At recovery time nothing was being removed, so no exclusion was owed then.
//
// Driven through `dropProvenUnsentPublishEntry`, the function all FOUR sites of this class
// call — Message Expiry and a Maximum Packet Size refusal, on the plain delivery path and on
// the affine one — rather than a re-implementation of its steps. Asserted through the
// ALLOCATOR, because the property is about the identifier and not about a map.
//
// Mutation: drop the excludeIdentifierLocked call from dropProvenUnsentPublishEntry. The
// refused arm then reports the identifier allocatable, for all four sites at once.
func TestMQTTExpiryOfARecoveredEntryReservesUntilItsDeleteIsProven(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		blockDelete  bool
		wantMintable bool
	}{
		{"a refused delete keeps the identifier reserved", true, false},
		{"a confirmed absence gives it back", false, true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "expiry-drop-"+nuid.Next())
			dur := e.sess.idHash + "_" + nuid.Next()
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 1)

			// RECOVERED, which is the shape that reaches expiry holding a register: the
			// entry is rebuilt from the durable record rather than from this process's
			// memory of writing it.
			e.forgetInMemory()
			e.sess.mu.Lock()
			e.sess.sendsInFlight = nil
			e.sess.mu.Unlock()
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}
			e.sess.mu.Lock()
			_, restored := e.sess.pendingPublish[pi]
			e.sess.mu.Unlock()
			if !restored {
				t.Fatalf("fixture: recovery must restore identifier %d as a live entry", pi)
			}

			if tc.blockDelete {
				e.asm.mu.Lock()
				e.asm.testInflightDelete = func(uint64) error {
					return errors.New("test: the register delete did not land")
				}
				e.asm.mu.Unlock()
			}

			if removed := e.sess.dropProvenUnsentPublishEntry(pi, mqttTestLicence(e.sess, nil)); removed == nil {
				t.Fatal("the expired entry must be removed: expiry drops a PUBLISH proven " +
					"never transmitted")
			}
			e.sess.mu.Lock()
			_, stillTracked := e.sess.pendingPublish[pi]
			e.sess.mu.Unlock()
			if stillTracked {
				t.Fatalf("identifier %d is still tracked after its message expired", pi)
			}

			if mintable := testInflightMints(t, e.sess, pi); mintable != tc.wantMintable {
				t.Fatalf("identifier %d allocatable = %v, want %v: an expiry whose register "+
					"delete did not land must leave the identifier reserved, and one whose "+
					"subject is proven empty must release it", pi, mintable, tc.wantMintable)
			}
		})
	}
}

// A RETAINED TERMINAL SLOT KEEPS ITS IDENTIFIER EXCLUDED WHILE RELEASING ITS CAPACITY
// (F-098, holistic review round 5).
//
// `mqttTerminalRetainedReoffer` is the disposition where finalizeTerminalSlot deliberately
// KEEPS the register, because the source may still re-offer. The terminal reservation must go
// — the client already completed this exchange, so holding its entry would consume Receive
// Maximum for a finished delivery and stall a client whose Receive Maximum is 1 — but the
// register is still standing by construction and nothing has confirmed it gone.
//
// Capacity and identifier are two different things, and this site had merged them. The two
// assertions below are exactly that separation: the identifier stays excluded, AND allocation
// still works on another identifier, which is what proves the capacity really was released
// rather than the whole space wedged.
//
// The cold case is the one that bites: after a restart there is no resident send hold left to
// carry the obligation, and `bumpPI` asks the send registry rather than the durable record.
// The store's fail-closed behaviour over a retained terminal slot does not cover a
// registerless retained send, which allocates without consulting any register at all (R6).
//
// Mutation: call releaseTerminalReservation instead of releaseRetainedTerminalReservation at
// any of the three retained-reoffer sites. The exclusion assertion fails.
func TestMQTTRetainedTerminalSlotExcludesItsIdentifierButNotItsCapacity(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "retained-terminal-"+nuid.Next())
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 2)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

	cur, smsg, lerr := e.asm.loadInflightRecord(subject)
	if lerr != nil || cur == nil || smsg == nil {
		t.Fatalf("fixture: the register must be readable: rec=%+v err=%v", cur, lerr)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	if ref == nil {
		t.Fatal("fixture: the register must yield a reference")
	}

	// A COLD resume: no entry and no hold, which is the state a restart leaves and the one
	// in which nothing but this call can reserve the identifier.
	e.forgetInMemory()
	e.sess.mu.Lock()
	e.sess.sendsInFlight = nil
	e.sess.mu.Unlock()

	e.sess.releaseRetainedTerminalReservation(e.c, *ref)

	if testInflightMints(t, e.sess, pi) {
		t.Fatalf("identifier %d became allocatable when its RETAINED terminal reservation "+
			"was released: the register on %q is deliberately kept, so nothing has confirmed "+
			"the identifier free", pi, subject)
	}
	// ...and the capacity really did come back: another identifier still allocates. Without
	// this, "exclude everything" would satisfy the assertion above and wedge the session.
	e.sess.mu.Lock()
	other := e.sess.trackPublishRetained(1)
	e.sess.mu.Unlock()
	if other == 0 || other == pi {
		t.Fatalf("retained allocation returned %d, want a different nonzero identifier: the "+
			"retained terminal slot must hold ONE identifier, not the whole space", other)
	}
}

// EVERY RETAINED-REOFFER ARM USES THE RESERVING RELEASE (F-098, holistic review round 5).
//
// The regression above proves what `releaseRetainedTerminalReservation` DOES; it cannot prove
// that the three arms which must call it actually do. Mutating a call site left that test
// green, which is the "a fixture must reach its route" failure in miniature — so the call
// sites get their own tripwire.
//
// It reads the source, deliberately. The three arms live in three files and two of them are
// reached only through a live PUBCOMP or a cold resume with a re-offerable source, which no
// unit fixture here drives end to end; an inventory that fails loudly when a fourth arm
// appears, or when one reverts to the bare release, is worth more than three fixtures that
// each almost reach their route.
//
// TWO predicates, because one was not enough and the second exists for a reason. The first
// walks the arms of a `mqttTerminalRetainedReoffer` switch case. The second walks every
// production call of the bare `releaseTerminalReservation` and refuses one gated on
// `settled()` — which is TRUE for a retained slot, so it can never license a release on its
// own. Round 6 found a FOURTH route through exactly that gate, in the retry worker, which the
// first predicate could not see because the route names no case at all.
//
// Every OTHER disposition may legitimately call the bare release: those slots are gone rather
// than retained.
func TestEveryRetainedReofferArmReservesTheIdentifier(t *testing.T) {
	t.Parallel()
	for _, file := range []string{
		"inflight_register.go",
		"mqtt.go",
		"pubrel_action.go",
	} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		lines := strings.Split(string(src), "\n")
		for i, ln := range lines {
			if !strings.Contains(ln, "case mqttTerminalRetainedReoffer:") {
				continue
			}
			// The arm runs to the next case/closing brace at the same depth; scanning a
			// bounded window is enough to catch a release inside it and cannot reach the
			// following arm's body in any current shape.
			var arm []string
			for j := i + 1; j < len(lines) && j < i+22; j++ {
				if strings.Contains(lines[j], "\tcase ") || strings.Contains(lines[j], "\tdefault:") {
					break
				}
				arm = append(arm, lines[j])
			}
			body := strings.Join(arm, "\n")
			reserving := strings.Contains(body, "releaseRetainedTerminalReservation(")
			bare := strings.Contains(body, "sess.releaseTerminalReservation(")
			excluding := strings.Contains(body, "excludeIdentifierLocked(")
			if bare && !reserving {
				t.Errorf("%s:%d: a retained-reoffer arm calls the BARE releaseTerminalReservation: "+
					"the register is deliberately KEPT here, so the identifier is not free even "+
					"though the capacity is (F-098)", file, i+1)
			}
			if !reserving && !excluding {
				t.Errorf("%s:%d: a retained-reoffer arm neither reserves the identifier nor "+
					"excludes it; every arm of this disposition owes one or the other", file, i+1)
			}
		}
		// THE SECOND PREDICATE: a bare release gated on settled().
		for i, ln := range lines {
			if !strings.Contains(ln, "sess.releaseTerminalReservation(") {
				continue
			}
			// The window reaches back over the WHOLE switch, not just the immediate
			// condition: a release gated on settled() is acceptable exactly when the same
			// switch dispatches the retained disposition first, and that arm's body sits
			// between the two.
			var gate []string
			for j := i - 1; j >= 0 && j > i-16; j-- {
				gate = append(gate, lines[j])
				if strings.Contains(lines[j], "switch ") {
					break
				}
			}
			g := strings.Join(gate, "\n")
			if strings.Contains(g, ".settled()") && !strings.Contains(g, "mqttTerminalRetainedReoffer") {
				t.Errorf("%s:%d: a bare releaseTerminalReservation is gated on settled(), which "+
					"is TRUE for mqttTerminalRetainedReoffer — a retained slot keeps its "+
					"register, so settled() alone never licenses freeing its identifier (F-098)",
					file, i+1)
			}
		}
	}
}
