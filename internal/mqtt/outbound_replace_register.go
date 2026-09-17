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
	"strconv"
	"strings"
)

// Replacing an in-flight register inside an authority transition (F-105, row 1).
//
// Until the ordinary PUBLISH path was converted, this mechanism CREATED a register and never
// replaced one, and the write boundary refused a nonzero register expectation outright. That
// refusal was right for as long as replacement lived in storeInflightRegister, because
// validating an expectation here as well would have been a SECOND definition of a legal
// register step.
//
// The conversion removes the first definition rather than adding a second. Replacement is
// still the slot state machine's decision — mqttSlotAdmitsDisposition and mqttDecideMarker
// own which occupant may be displaced by which allocation — and what changes is only that the
// decision is carried out INSIDE the admitting batch instead of by a writer called before it.
//
// # Why this cannot be a caller's word
//
// The boundary re-derives. A replacing member arrives with an expectation and a body; this
// check reads the occupant the expectation names, re-runs the machine's gate against it, and
// then validates that the batch's WHOLE RESULTING STATE implements the disposition it
// selected. A caller that composed the wrong batch is refused here even though every member
// is individually legal — which is the property review rounds 5 to 21 established and round
// 21 sharpened: the batch's own members are not the whole state.
//
// # The two modes, and why they owe different things
//
// The occupant's unsent marker partitions it into the only two states that matter to a client
// obligation, and the modes are not symmetrical:
//
//   - PAIR replacement, when a marker NAMES the occupant. The marker is proof nothing was
//     ever put on the socket under it, so the occupant is PROVEN-UNSENT: there is no client
//     obligation to inherit. The new register starts a FRESH lineage, and the batch owes both
//     a fresh exchange admission and the predecessor exchange's authorized ending — otherwise
//     two obligations stand on one identifier, which is exactly what checkIdentifierExclusivity
//     exists to refuse.
//
//   - REGISTER-ONLY replacement, when the marker is authoritatively ABSENT. Absence is NOT
//     read as transmitted: the send intent deletes the marker BEFORE the bytes leave, so it
//     means POSSIBLY-ATTEMPTED, and the client may already hold this exchange. The obligation
//     CONTINUES — same exchange, same charge, same actions, same dependencies — so the new
//     register inherits the occupant's lineage and the batch admits no exchange at all.
//
// In BOTH modes the occupant's QoS is preserved, for the reason storeInflightRegister states
// where it assigns it: this writer cannot prove the occupant was never transmitted, and its
// QoS is the only durable trace of what the client may be holding.

// mqttReplacementMode is which of the two dispositions a replacing register member carries.
// It is DERIVED here from the batch and the store, never supplied.
type mqttReplacementMode int

const (
	// mqttReplacePair replaces the register and its occupant's marker together: the
	// occupant was proven unsent, so a fresh obligation takes the identifier.
	mqttReplacePair mqttReplacementMode = iota + 1
	// mqttReplaceRegisterOnly rewrites the register alone: the occupant was possibly
	// attempted, so its obligation continues under the new allocation's record.
	mqttReplaceRegisterOnly
)

// checkRegisterReplacements validates every register member that REPLACES an occupant.
//
// Unconditional over the batch's members, so no kind and no caller can route a replacement
// past it: a replacement is identified by carrying a nonzero expectation on a register
// subject, which is a property of the member and not of the transition's name.
func (jsa *mqttJSA) checkRegisterReplacements(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	// The batch's markers, by slot key, so a mode can be derived from what the batch does
	// rather than from what its composer meant.
	markers := map[string]mqttTransitionMember{}
	for _, e := range extra {
		if mqttIsF105Subject(e.subj) {
			continue
		}
		if name, merr := mqttParseInflightMarkerSubject(e.subj); merr == nil {
			markers[mqttSlotKey(name)] = e
		}
	}

	// A MARKER MAY BE REWRITTEN ONLY AS HALF OF A PAIR REPLACEMENT, and that is THIS rule.
	//
	// mqttCheckTransitionMember stopped refusing a nonzero marker expectation, because it
	// cannot see whether a replacing register stands beside it; this is where that permission
	// is spent.
	//
	// It is NOT subsumed by checkPairBindings, and the first mutation run said it was — on a
	// fixture that carried a marker overwrite and NO register at all, which is the one shape
	// that rule does cover. checkPairBindings requires *a* register in the batch, not a
	// *replacing* one, so a marker rewritten beside a CREATE-ONLY register passes it. That
	// shape is reachable: a teardown ends an obligation and deletes its register while the
	// marker survives, so a fresh admission on that identifier is legitimate and can carry a
	// marker rewrite the store itself would accept — after which recovery reads a matching
	// marker, calls a possibly-transmitted delivery a first transmission, and puts DUP=0 on a
	// retransmission, the one direction [MQTT-3.3.1-1] forbids.
	// TestF105MarkerMembersAreShapedAndCreateOnly is that shape, and it is this rule's oracle.
	replacing := map[string]struct{}{}
	for _, e := range extra {
		if mqttIsF105Subject(e.subj) || e.expect == 0 {
			continue
		}
		if name, perr := mqttParseInflightSubject(e.subj); perr == nil {
			replacing[mqttSlotKey(name)] = struct{}{}
		}
	}
	for key, m := range markers {
		if m.expect == 0 {
			continue
		}
		if _, ok := replacing[key]; !ok {
			return fmt.Errorf("transition %q rewrites the unsent marker on %q at sequence %d "+
				"with no register replacing its slot: a marker is created with its register "+
				"and deleted by a transmission, and an overwrite beside anything but the "+
				"register displacing its occupant describes a transmitted allocation as "+
				"unsent again", a.Txn, m.subj, m.expect)
		}
	}

	for _, e := range extra {
		if mqttIsF105Subject(e.subj) || e.expect == 0 {
			continue
		}
		name, perr := mqttParseInflightSubject(e.subj)
		if perr != nil {
			continue
		}
		rec, derr := mqttDecodeInflightRecord(e.body)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported replacing register on %q: %w",
				a.Txn, e.subj, derr)
		}

		// THE EXACT OBSERVED PREDECESSOR, re-read here. A replacement decided against a
		// record this boundary never saw is a caller's conclusion, and the expectation alone
		// does not carry what the decision turned on — the occupant's delivery, its phase and
		// its lineage are all in the body, not in the sequence.
		cur, smsg, lerr := jsa.loadInflightRecord(e.subj)
		if lerr != nil {
			return fmt.Errorf("transition %q could not read the register it replaces on %q: %w",
				a.Txn, e.subj, lerr)
		}
		if smsg == nil || cur == nil {
			// AN UNREADABLE OR ABSENT OCCUPANT REFUSES, and it is deliberately not treated
			// as a free subject: a record this broker cannot interpret may be the one
			// holding the identifier. The CAS would fail anyway for an absent one; refusing
			// here is what stops an unreadable one being replaced on the strength of a
			// sequence that happens to match.
			return fmt.Errorf("transition %q replaces the register on %q at sequence %d, but "+
				"no register this broker can read stands there", a.Txn, e.subj, e.expect)
		}
		if smsg.Sequence != e.expect {
			return fmt.Errorf("transition %q replaces the register on %q at sequence %d while "+
				"sequence %d stands: a replacement is decided against an exact occupant",
				a.Txn, e.subj, e.expect, smsg.Sequence)
		}

		// SAME DELIVERY ONLY. Every replacement arm in the converted ladder is scoped to an
		// occupant naming the same (Dur, Seq); an occupant naming another delivery is
		// quarantined, reaped or abandoned, and never overwritten by an admission.
		if cur.Dur != rec.Dur || cur.Seq != rec.Seq {
			return fmt.Errorf("transition %q replaces the register on %q, which names delivery "+
				"%s/%d, with one naming %s/%d: a replacement answers the SAME delivery",
				a.Txn, e.subj, cur.Dur, cur.Seq, rec.Dur, rec.Seq)
		}

		// THE MACHINE'S GATE, RE-RUN. A past-PUBREC slot is the identifier's continuing
		// authority: replacing it with a fresh PUBLISH record would destroy its canonical
		// PUBREL identity and make an answered payload eligible for retransmission.
		if !mqttSlotAdmitsDisposition(cur, mqttSlotRefFromRecord(e.subj, smsg.Sequence, cur),
			mqttSlotReplacePublish) {
			return fmt.Errorf("transition %q replaces the register on %q, whose occupant in "+
				"state %q admits no replacement", a.Txn, e.subj, string(cur.State))
		}

		// THE OCCUPANT'S QoS IS PRESERVED IN BOTH MODES. Taking the admitting caller's QoS
		// instead destroys the only durable trace of what the client may already hold, and
		// for 2 -> 1 it licenses the slot deletion that abandons the exactly-once handshake.
		if rec.QoS != cur.QoS {
			return fmt.Errorf("transition %q replaces the register on %q at QoS %d while its "+
				"occupant holds QoS %d: a replacement preserves the occupant's QoS, which is "+
				"the only durable trace of what the client may be holding",
				a.Txn, e.subj, rec.QoS, cur.QoS)
		}

		// THE MODE IS THE MARKER BOUNDARY'S ANSWER, NOT THE BATCH'S SHAPE.
		//
		// The batch says which mode its composer INTENDED; the store says which one is true,
		// because the disposition is a fact about the occupant and not about the members.
		// Deriving it from the members alone admits two composer-bypassing batches that are
		// each individually legal:
		//
		//   - a register-only batch while a marker STANDS for the occupant. The occupant was
		//     proven unsent, so the disposition was PAIR; landing it orphans a marker naming
		//     the occupant's nonce beside a register carrying a new one, and the next reader
		//     of that slot meets a marker about an allocation that is gone.
		//   - a pair batch whose marker member CASes at a sequence holding a STALE marker for
		//     some other allocation. The CAS passes on the sequence, and a marker about a
		//     different exchange would have been taken as proof this occupant was unsent.
		//
		// THE DECISION IS mqttDecideMarker'S, NOT THIS FUNCTION'S. An earlier revision wrote
		// its own three-way table here and that was a second definition of the same decision
		// — the thing this whole conversion exists to remove. The boundary loads the marker,
		// types it as an OBSERVATION of the occupant, and asks the same machinery
		// storeInflightRegister asks, under the same operation.
		mkObs := jsa.loadOccupantMarkerLoad(e.subj).observe(mqttMarkerReloaded, cur.Nonce)
		v, verr := mqttDecideMarker(
			mqttMarkerContext{
				op:         mqttMarkerOpReplaceOccupant,
				caller:     mqttMarkerCallerReplace,
				gatePassed: true,
			},
			mqttRegisterObservation{kind: mqttRegisterOurs, seq: smsg.Sequence},
			mkObs,
		)
		if verr != nil {
			return fmt.Errorf("transition %q could not decide the unsent marker beside the "+
				"register it replaces on %q: %w", a.Txn, e.subj, verr)
		}

		var mode mqttReplacementMode
		switch v.disposition {
		case mqttMarkerReplacePair:
			mode = mqttReplacePair
		case mqttMarkerReplaceRegisterOnly:
			mode = mqttReplaceRegisterOnly
		default:
			// WITHHOLD. Neither "the occupant was proven unsent" nor "it was possibly
			// attempted" is established — an unreadable body, a marker naming another
			// allocation, or a read that did not resolve — so nothing may displace it. The
			// allocation becomes a marker-opaque reservation over the occupant rather than
			// a replacement decided under a guess.
			return fmt.Errorf("transition %q replaces the register on %q while the unsent "+
				"marker beside it establishes neither disposition: the identifier is "+
				"withheld rather than replaced", a.Txn, e.subj)
		}

		m, paired := markers[mqttSlotKey(name)]
		switch {
		case mode == mqttReplacePair && !paired:
			return fmt.Errorf("transition %q rewrites the register on %q alone while a marker "+
				"naming its occupant stands: the occupant was PROVEN UNSENT, so its marker is "+
				"replaced with the displacing allocation's in the same batch and no interval "+
				"shows a marker naming a record that is gone", a.Txn, e.subj)
		case mode == mqttReplacePair && m.expect != mkObs.seq:
			// THE EXPECTATION MUST NAME THE MARKER THIS VALIDATOR INSPECTED, which is a
			// different fact from the store's CAS. The CAS establishes "the marker at commit
			// matches the expectation"; this establishes "the expectation names the body that
			// was decided on". Without it a transaction can validate PRESENCE and then commit
			// against ABSENCE — a create-only expectation is invalid while the inspected
			// marker stands, and becomes acceptable to the store once that marker is deleted.
			return fmt.Errorf("transition %q replaces the unsent marker beside %q at sequence "+
				"%d while the marker it decided on stands at %d",
				a.Txn, e.subj, m.expect, mkObs.seq)
		case mode == mqttReplaceRegisterOnly && paired:
			return fmt.Errorf("transition %q writes an unsent marker beside the register it "+
				"replaces on %q while no marker stands for its occupant: an absent marker "+
				"means POSSIBLY ATTEMPTED, and minting one describes an allocation the client "+
				"may already hold as never sent", a.Txn, e.subj)
		}

		if err := jsa.checkReplacementPredecessorObligation(a, name.gen, e.subj, mode, rec, cur); err != nil {
			return err
		}
		if err := mqttCheckReplacementResultingState(a, extra, e.subj, mode, rec, cur); err != nil {
			return err
		}
	}
	return nil
}

// checkReplacementPredecessorObligation requires the occupant to have a durable obligation
// behind it, and requires that obligation to agree with the occupant.
//
// WITHOUT THIS THE WHOLE REPLACEMENT PATH RESTS ON A LINEAGE STRING. checkPairBindings exempts
// a replacing register from "every register is admitted with its obligation", and the resulting
// state check compares lineages — so a readable ORPHAN register whose exchange no longer exists
// could be rewritten with no obligation behind it at all, and a pair replacement never had to
// establish that the predecessor it displaces is real. An allocation with nothing behind it
// answers to nobody and excludes nobody, which is the defect the admission rule existed for.
//
// NEVER MANUFACTURED. A missing or unreadable predecessor exchange is REFUSED, not synthesised
// from the register: minting an obligation nothing admitted is the inference this mechanism
// exists to remove, and doing it during a replacement would be migration wearing another word.
func (jsa *mqttJSA) checkReplacementPredecessorObligation(a *mqttTransitionAttempt, gen, regSubj string,
	mode mqttReplacementMode, rec, cur *mqttInflightRecord) error {

	// ONE CONDITION, ONE RULE. An earlier revision split this into an error branch and a nil
	// branch and labelled the second dead, because loadOutboundExchange reports a missing
	// exchange as an error. Mutation showed the label was wrong in the way that matters: the
	// two branches COVER EACH OTHER, so removing either one alone left the other refusing and
	// no single mutation discriminated. Merged, the rule has one enforcement point and one
	// oracle, and a mutation of it is a real kill rather than a masked one.
	//
	// NEVER MANUFACTURED. An unreadable or missing predecessor exchange is REFUSED, not
	// synthesised from the register: minting an obligation nothing admitted is the inference
	// this mechanism exists to remove, and doing it during a replacement would be migration
	// wearing another word.
	x, _, xerr := jsa.loadOutboundExchange(a.ID, a.Epoch, gen, cur.Lin)
	if xerr != nil || x == nil {
		return fmt.Errorf("transition %q replaces the register on %q, whose obligation %q this "+
			"broker cannot read: an allocation with nothing behind it answers to nobody, and a "+
			"replacement never invents the obligation it displaces (%v)",
			a.Txn, regSubj, cur.Lin, xerr)
	}

	// THE OBLIGATION MUST AGREE WITH THE OCCUPANT. An exchange naming another identifier or
	// another QoS is not this slot's obligation, and treating it as one would let a
	// replacement borrow an unrelated obligation as its licence.
	if x.PI != cur.PI {
		return fmt.Errorf("transition %q replaces the register on %q, whose obligation %q is "+
			"allocated identifier %d rather than %d", a.Txn, regSubj, cur.Lin, x.PI, cur.PI)
	}
	if x.QoS != cur.QoS {
		return fmt.Errorf("transition %q replaces the register on %q, whose obligation %q holds "+
			"QoS %d while the register holds %d", a.Txn, regSubj, cur.Lin, x.QoS, cur.QoS)
	}

	if mode != mqttReplaceRegisterOnly {
		return nil
	}
	// THE CONTINUED OBLIGATION MUST STILL PERMIT A PUBLISH, and that is asked of
	// mqttActionPermittedAtPhase rather than restated here.
	//
	// An earlier revision asked only whether the exchange had ENDED, which let four phases
	// through that should not pass: PubRecObserved, PubRelOwed and both pending-drain phases
	// are all live, and none of them may carry another PUBLISH. The concrete batch that
	// exploited it needed no exchange member at all — admit a pair, delete its marker,
	// advance the exchange to PubRecObserved by its own authorized transition, then submit
	// ONLY a replacing PUBLISH register at the occupant's exact sequence with the lineage and
	// QoS inherited. Every other check passed, and the result was a fresh PUBLISH allocation
	// for an obligation the client had already answered for.
	//
	// So the rule is the one the protocol already states: "a PUBLISH is the delivery itself,
	// so it may only be acquired while the exchange is still at its publish". That function
	// is the one authoritative statement of it, and it SUBSUMES the ended test — Terminal and
	// Rejected are not the publish phase either — so there is one rule here and not two
	// saying overlapping parts of the same thing.
	if perr := mqttActionPermittedAtPhase(x.QoS, x.Phase, mqttActionPublish); perr != nil {
		return fmt.Errorf("transition %q continues obligation %q on %q with a fresh PUBLISH "+
			"allocation, which that obligation no longer admits: %w",
			a.Txn, cur.Lin, regSubj, perr)
	}
	// AND THE RESULT IMPLEMENTS THIS OPERATION'S DISPOSITION, WHICH IS PUBLISH.
	//
	// checkPairBindings requires a register to start in PUBLISH only beside a NEWLY ADMITTED
	// exchange, so a register-only replacement would otherwise be free to substitute a
	// different individually valid phase — moving the slot past PUBREC with no transition
	// authorizing the move. Phase advancement belongs to its own authorized transition.
	if rec.State != mqttInflightPublish {
		return fmt.Errorf("transition %q continues obligation %q on %q with a register in "+
			"state %q: a replacement re-allocates a PUBLISH, and advancing the slot is the "+
			"business of the transition that authorizes the advance",
			a.Txn, cur.Lin, regSubj, string(rec.State))
	}
	return nil
}

// mqttCheckReplacementResultingState validates that the batch implements the disposition the
// replacement selected — the round-21 property applied to replacement.
//
// S ⊕ Δ, not Δ. The question is not whether each member is legal but whether the state the
// batch RESULTS IN carries exactly one obligation for this identifier, with the predecessor
// either continued or validly ended.
func mqttCheckReplacementResultingState(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	regSubj string, mode mqttReplacementMode, rec, cur *mqttInflightRecord) error {

	admits, ends := mqttBatchExchangeDispositions(a, extra)

	switch mode {
	case mqttReplacePair:
		// PROVEN UNSENT: a fresh obligation, and the predecessor must END here.
		if rec.Lin != rec.Nonce {
			return fmt.Errorf("transition %q replaces the pair on %q with a register whose "+
				"lineage %q is inherited: a marker naming the occupant proves it was never "+
				"transmitted, so the allocation that displaces it is a FRESH obligation and "+
				"its lineage is its own", a.Txn, regSubj, rec.Lin)
		}
		if _, ok := admits[rec.Lin]; !ok {
			return fmt.Errorf("transition %q replaces the pair on %q without admitting the "+
				"obligation %q the new allocation answers for", a.Txn, regSubj, rec.Lin)
		}
		// THE PREDECESSOR NOT BEING LEFT STANDING IS checkIdentifierExclusivity'S RULE.
		//
		// Overwriting a register does not remove its exchange, so a pair replacement that
		// never mentions its predecessor leaves two obligations on one identifier. Requiring
		// an ending member here looked like the way to say that, and a mutation showed it is
		// both redundant and WRONG. Redundant, because with it removed the batch is still
		// refused — "admits obligation X on identifier 7, which obligation Y still holds at
		// phase 1: an acknowledgement names the identifier and nothing else". Wrong, because
		// it is stricter than the fact it was standing in for: a predecessor already in a
		// terminal phase holds the identifier no longer, and demanding an ending for it would
		// refuse a batch that is entirely legal.
		//
		// So the exclusivity walk keeps the rule, and it keeps it over the state the batch
		// RESULTS IN rather than over the members this function can see.

	case mqttReplaceRegisterOnly:
		// POSSIBLY ATTEMPTED: the SAME obligation continues, so nothing is admitted.
		if rec.Lin != cur.Lin {
			return fmt.Errorf("transition %q rewrites the register on %q with lineage %q while "+
				"its occupant carries %q: an authoritatively absent marker means POSSIBLY "+
				"ATTEMPTED, not transmitted, so the obligation continues and its lineage "+
				"continues with it", a.Txn, regSubj, rec.Lin, cur.Lin)
		}
		if _, ok := admits[rec.Lin]; ok {
			return fmt.Errorf("transition %q rewrites the register on %q AND admits obligation "+
				"%q: the occupant's obligation is being continued, not replaced, and admitting "+
				"one duplicates its charge and its dependencies", a.Txn, regSubj, rec.Lin)
		}
		if _, ok := ends[rec.Lin]; ok {
			return fmt.Errorf("transition %q rewrites the register on %q and ends obligation %q "+
				"in the same batch: a continued obligation is not ended by the act that "+
				"continues it", a.Txn, regSubj, rec.Lin)
		}
	}
	return nil
}

// mqttBatchExchangeDispositions reports, by exchange nonce, which obligations this batch
// ADMITS create-only and which it moves to an ended phase.
//
// Derived from the members themselves rather than from the transition's kind, for the reason
// checkExchangeOverwrites is keyed on the member: an exchange member at a nonzero expectation
// IS an advance whichever kind carries it.
func mqttBatchExchangeDispositions(a *mqttTransitionAttempt,
	extra []mqttTransitionMember) (admits, ends map[string]struct{}) {

	admits, ends = map[string]struct{}{}, map[string]struct{}{}
	for _, e := range extra {
		cls, ok := mqttF105ObligationClass(e.subj)
		if !ok || cls != mqttExchangeClassTk {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			continue
		}
		nonce := toks[3]
		if e.expect == 0 {
			admits[nonce] = struct{}{}
			continue
		}
		// AN ADVANCE IS AN ENDING ONLY IF ITS BODY SAYS SO. A body this boundary cannot
		// decode is not an ending, and is left to mqttCheckObligationMember, which refuses
		// it outright — so nothing is silently admitted by this skip.
		x, derr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, toks[2], nonce)
		if derr != nil || x == nil {
			continue
		}
		if mqttExchangePhaseEnded(x.Phase) || mqttExchangePhaseEndedPendingDrain(x.Phase) {
			ends[nonce] = struct{}{}
		}
	}
	return admits, ends
}

// loadOccupantMarkerLoad reads the unsent marker beside one register subject as a
// mqttMarkerLoad, so the boundary can type it as an OBSERVATION and hand it to the same
// mqttDecideMarker table every other caller uses.
//
// It mirrors mqttAccountSessionManager.loadInflightMarker's classification exactly, and does
// not call it: that method carries the instance-scoped test seams and the reload hook, which
// belong to the session-side writers. What must not differ is the three-way split — an
// indeterminate read is NEVER an absence, and a body this broker cannot decode is present and
// unattributed rather than missing.
func (jsa *mqttJSA) loadOccupantMarkerLoad(regSubj string) mqttMarkerLoad {
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, regSubj+"."+mqttInflightMarkerSuffix)
	switch {
	case err != nil && isErrorOtherThan(err, JSNoMessageFoundErr):
		return mqttMarkerLoad{kind: mqttInflightResponseIndeterminate}
	case err != nil || smsg == nil:
		return mqttMarkerLoad{kind: mqttInflightResponseAbsent}
	}
	m, derr := mqttDecodeInflightMarker(smsg.Data)
	return mqttMarkerLoad{kind: mqttInflightResponsePresent, seq: smsg.Sequence, marker: m, decodeErr: derr}
}

// mqttSlotKey is the (generation, identifier) a register and its marker share.
func mqttSlotKey(name *mqttInflightName) string {
	return name.gen + "." + strconv.FormatUint(uint64(name.pi), 10)
}
