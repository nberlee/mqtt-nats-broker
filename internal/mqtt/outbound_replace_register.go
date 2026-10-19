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

		if cur.Dur == rec.Dur && cur.Seq == rec.Seq && cur.SourceAckReply != rec.SourceAckReply {
			return fmt.Errorf("transition %q changes the source ACK reply on %q", a.Txn, e.subj)
		}

		// A WITHDRAWAL IS NOT A REPLACEMENT EITHER, and under its own kind it is recognised first
		// and judged only by its own rule: the occupant is not displaced, it is ENDED, and its
		// body is kept whole with Cancelled set. Under every other kind a member that turns a
		// publish occupant into a tombstone is refused outright — a tombstone written beside
		// anything but the exchange's withdrawal would claim "never transmitted" with none of the
		// evidence the withdrawal rule loads.
		if a.Kind == mqttTransitionWithdrawUnsent {
			if err := jsa.checkWithdrawalRegister(a, extra, e.subj, cur, rec); err != nil {
				return err
			}
			continue
		}
		// AN ABANDONED REGISTER IS THE DURABLE DISPOSAL RECORD: no transition rewrites it, and
		// only an abandon-delivery transition writes one, beside its exchange's abandonment —
		// with ONE exception (unit C2): a client acknowledgement of the abandoned delivery moves
		// it along its acknowledgement arrow (publish_terminal, pubrel_intended) carrying every
		// other field, the abandonment's included, unchanged. That member is recognised here and
		// judged below, with the exchange it pairs with, by checkAckPhaseAdvanceMembers.
		if cur.State == mqttInflightDeliveryAbandoned &&
			!(a.Kind == mqttTransitionAdvanceExchange && mqttIsAckPhaseAdvance(cur, rec)) {
			return fmt.Errorf("transition %q of kind %q rewrites the abandoned register on %q: an "+
				"abandoned delivery's register is kept until its generation is discarded", a.Txn, a.Kind, e.subj)
		}
		if a.Kind == mqttTransitionAbandonDelivery {
			if err := jsa.checkAbandonmentRegister(a, extra, e.subj, cur, rec); err != nil {
				return err
			}
			continue
		}
		if rec.State == mqttInflightDeliveryAbandoned {
			return fmt.Errorf("transition %q of kind %q turns the register on %q into an abandoned one: "+
				"only an abandon-delivery transition writes one, beside the exchange it abandons",
				a.Txn, a.Kind, e.subj)
		}
		// A CANONICAL RE-BIND, recognised by its own kind and judged only by its own rule: the exact
		// untokened pubrel_ready occupant with ONLY its canonical sequence moved, beside an exchange
		// that stands at PubRecObserved (F-105 7b).
		if a.Kind == mqttTransitionRebindCanonical {
			if err := jsa.checkCanonicalRebindRegister(a, name.gen, e.subj, cur, rec); err != nil {
				return err
			}
			continue
		}
		// A CANCELLATION DECISION, recognised by its own kind and judged only by its own rule: the
		// exact untokened publish occupant with Cancelled set and nothing else changed.
		if a.Kind == mqttTransitionTombstoneRegister {
			want := *cur
			want.Cancelled, want.CancelOrigin = true, mqttCancelOriginOccupant
			if cur.State != mqttInflightPublish || cur.ActionToken != _EMPTY_ || cur.Cancelled || want != *rec {
				return fmt.Errorf("transition %q tombstones the register on %q with a body that is not its exact "+
					"untokened publish occupant with Cancelled set", a.Txn, e.subj)
			}
			continue
		}
		if rec.Cancelled && !cur.Cancelled {
			return fmt.Errorf("transition %q of kind %q turns the register on %q into a cancellation "+
				"tombstone: only a withdraw-unsent or tombstone-register transition writes one",
				a.Txn, a.Kind, e.subj)
		}

		// A TOKEN ACQUISITION IS NOT A REPLACEMENT (F-105 row 2, §4.3 point 1). An acquire-action
		// transition may CAS the register to install an output-custody token: the occupant's
		// identity, delivery, lineage and QoS are all PRESERVED, and only the token, the custody it
		// is bound to, and the acquisition counter change (plus, for the PUBREL direct arrow, the
		// canonical tuple and `pubrel_intended -> pubrel_ready`). It is validated as an
		// acquisition, never as a same-delivery replacement, so the marker/mode/QoS rules below —
		// which are about DISPLACING an occupant with a fresh allocation — must not be applied to
		// it. The exact-sequence CAS above still fenced it.
		//
		// RECOGNITION IS MANDATORY for this kind. An acquire-action member that is not a
		// recognised acquisition is REFUSED, never reconsidered as a replacement: falling through
		// is how a batch over a token-bearing predecessor substituting its own token passed the
		// register-only replacement rules and overwrote live custody. An acquisition never
		// carries a replacement.
		if a.Kind == mqttTransitionAcquireAction {
			if !mqttIsActionTokenAcquisition(cur, rec) {
				return fmt.Errorf("transition %q of kind %q writes the register on %q with a member that "+
					"is not a token acquisition of its exact occupant: an acquisition installs a token on "+
					"a slot that carries none and changes nothing else it does not name, and it never "+
					"replaces an occupant", a.Txn, a.Kind, e.subj)
			}
			kind, ok := mqttAcquiredActionsIn(a, extra)[rec.Lin+"."+strconv.FormatUint(rec.Acq, 10)]
			if ok {
				if kerr := mqttActionKindRegisterState(kind.kind, cur.State, rec.State); kerr != nil {
					return fmt.Errorf("transition %q acquires a %q action on %q: %w", a.Txn, kind.kind,
						e.subj, kerr)
				}
			}
			if err := jsa.checkRefutedCancellation(a, e.subj, cur, rec); err != nil {
				return err
			}
			// (A token member with no action member is checkActionTokenMembers' refusal.)
			continue
		}

		// LIVE CUSTODY IS PRESERVED, whatever other kind of transition carries the member (an
		// acquisition's recognised predecessor carries no token, so it is judged above). A token on
		// the exact predecessor is an output lease someone may be executing under right now, and
		// the only authority write that may end it is its holder's own drain. Keyed on the member
		// rather than on the kind for the reason every rule in this function is: a replacement
		// that keeps every field and clears the token passes the replacement rules below, which
		// judge displacement and not custody, and one kind name away from wherever a narrower rule
		// was scoped.
		if err := mqttCheckLiveCustodyPreserved(a, e.subj, cur, rec); err != nil {
			return err
		}

		// AN ACKNOWLEDGEMENT RE-PHASE IS NOT A REPLACEMENT EITHER (F-105 row 3). An
		// advance-exchange transition may CAS the register to publish_terminal or
		// pubrel_intended to record a client acknowledgement beside the exchange advance: the
		// occupant's identity, delivery, lineage and QoS are PRESERVED and only its state moves.
		// checkAckPhaseAdvanceMembers validates it against the exchange member; the
		// marker/mode/QoS rules below — which are about DISPLACING an occupant with a fresh
		// allocation — must not be applied to it. The exact-sequence CAS above still fenced it.
		if a.Kind == mqttTransitionAdvanceExchange && (mqttIsAckPhaseAdvance(cur, rec) || mqttIsDrainTokenClearance(cur, rec)) {
			if err := jsa.checkRefutedCancellation(a, e.subj, cur, rec); err != nil {
				return err
			}
			continue
		}

		// SAME DELIVERY ONLY. Every replacement arm in the converted ladder is scoped to an
		// occupant naming the same (Dur, Seq); an occupant naming another delivery is
		// quarantined, reaped or abandoned, and never overwritten by an admission.
		if cur.Dur != rec.Dur || cur.Seq != rec.Seq {
			return fmt.Errorf("transition %q replaces the register on %q, which names delivery "+
				"%s/%d, with one naming %s/%d: a replacement answers the SAME delivery",
				a.Txn, e.subj, cur.Dur, cur.Seq, rec.Dur, rec.Seq)
		}

		// A PENDING FENCE IS NEVER REPLACED (F-088, consult C4): it is not an exchange's identity
		// carrier, and its only exit is the dedicated disposal rule (mqttSlotDeletePendingFence),
		// after which the allocation creates on an empty subject. An OCCUPANT cancellation, by
		// contrast, may be replaced register-only under every check below, and the replacing body
		// is this allocation's own record, which carries neither Cancelled nor its provenance.
		if cur.CancelOrigin == mqttCancelOriginPendingCreate {
			return fmt.Errorf("transition %q replaces the register on %q, which is a pending cancellation "+
				"fence: a fence is disposed by its own rule, never replaced", a.Txn, e.subj)
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

// mqttIsActionTokenAcquisition reports whether rec is cur with ONLY an output-custody token
// installed: the token goes onto a slot that carried none, bound to the slot's existing custody,
// and the acquisition counter advances by exactly one. Every other field — identity, delivery,
// lineage, phase, QoS, custody — is the occupant's, unchanged. This is what distinguishes a token
// acquisition (§4.3 point 1) from a same-delivery replacement, which re-mints the allocation.
//
// ONE further arm, for the PUBREL direct arrow (F-105 row 2): from `pubrel_intended` the member may
// ALSO move the slot to `pubrel_ready` and install the canonical PUBREL tuple the delivered record
// proved, exactly the mutation that arrow performed as a standalone CAS before it moved into the
// authority batch. Only that pair of fields and that state move; an occupant already naming a
// canonical durable or sequence may not have either rewritten.
func mqttIsActionTokenAcquisition(cur, rec *mqttInflightRecord) bool {
	if cur == nil || rec == nil {
		return false
	}
	if cur.ActionToken != _EMPTY_ || rec.ActionToken == _EMPTY_ {
		return false
	}
	want := *cur
	want.ActionToken = rec.ActionToken
	want.ActionClaim = cur.Claim
	want.ActionClaimSeq = cur.ClaimSeq
	want.Acq = cur.Acq + 1
	// A retransmission's acquisition may clear a KNOWN-FALSE cancellation (F-088): the shape is
	// recognised here, and the write boundary admits it only on the marker's authoritative absence
	// (mqttRefutedCancellation) — a standing marker means a genuine cancellation, and that
	// acquisition is refused.
	if mqttClearsCancellation(cur, rec) {
		want.Cancelled, want.CancelOrigin = false, _EMPTY_
	}
	if cur.State == mqttInflightPubRelIntended && rec.State == mqttInflightPubRelReady &&
		rec.PubRelDurable != _EMPTY_ && rec.PubRelStreamSeq != 0 &&
		(cur.PubRelDurable == _EMPTY_ || cur.PubRelDurable == rec.PubRelDurable) &&
		(cur.PubRelStreamSeq == 0 || cur.PubRelStreamSeq == rec.PubRelStreamSeq) {
		want.State = mqttInflightPubRelReady
		want.PubRelDurable = rec.PubRelDurable
		want.PubRelStreamSeq = rec.PubRelStreamSeq
	}
	return want == *rec
}

// mqttActionKindRegisterState is the one statement of which register step an acquisition of each
// packet may ride on (F-105 row 2): a PUBLISH action on a slot still at `publish`, a PUBREL action
// on a `pubrel_ready` slot or — the direct arrow — a `pubrel_intended` slot it moves to
// `pubrel_ready`. The composer asks it before building the token member and the write boundary asks
// it again of the member it is handed, so a PUBREL lease cannot be installed on a PUBLISH slot, nor
// the reverse, by a batch the composer never built.
func mqttActionKindRegisterState(kind mqttActionKind, from, to mqttInflightState) error {
	switch kind {
	case mqttActionPublish:
		if from == mqttInflightPublish && to == mqttInflightPublish {
			return nil
		}
	case mqttActionPubRel:
		if to == mqttInflightPubRelReady &&
			(from == mqttInflightPubRelReady || from == mqttInflightPubRelIntended) {
			return nil
		}
	}
	return fmt.Errorf("a %q action may not ride a register step %q -> %q", kind, string(from), string(to))
}

// mqttCheckLiveCustodyPreserved is the boundary's statement that only a lease's holder ends it.
//
// If the exact predecessor carries a live token, the member must carry the SAME token, claim and
// claim sequence — unless it is the holder's own drain clearance on an advance-exchange
// (mqttIsDrainTokenClearance), the one authority write that legitimately ends live custody.
// Everything else that preserves the lease passes: an acknowledgement re-phase keeps the token by
// construction, and an acquisition's predecessor carries none. Register writes outside authority
// transitions (the slot machine's exact-token drains) are not boundary writes and are not judged
// here.
func mqttCheckLiveCustodyPreserved(a *mqttTransitionAttempt, subj string, cur, rec *mqttInflightRecord) error {
	if cur == nil || rec == nil || cur.ActionToken == _EMPTY_ {
		return nil
	}
	if rec.ActionToken == cur.ActionToken && rec.ActionClaim == cur.ActionClaim &&
		rec.ActionClaimSeq == cur.ActionClaimSeq {
		return nil
	}
	if a.Kind == mqttTransitionAdvanceExchange && mqttIsDrainTokenClearance(cur, rec) {
		return nil
	}
	return fmt.Errorf("transition %q of kind %q writes the register on %q over live output custody "+
		"(token %q) without preserving it: only the lease's holder ends it, by its own drain",
		a.Txn, a.Kind, subj, cur.ActionToken)
}

// mqttAcquiredAction is one acquisition an acquire-action batch creates, as its action member
// states it.
type mqttAcquiredAction struct {
	pi   uint16
	kind mqttActionKind
}

// mqttAcquiredActionsIn reads the acquisitions a batch creates from its action members, keyed
// "lineage.acq". An action member that does not decode is skipped here: its own member check
// (checkActionMembers) refuses it.
func mqttAcquiredActionsIn(a *mqttTransitionAttempt, extra []mqttTransitionMember) map[string]mqttAcquiredAction {
	acquired := map[string]mqttAcquiredAction{}
	for _, e := range extra {
		if cls, ok := mqttF105ObligationClass(e.subj); !ok || cls != mqttActionClassTk {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 5 {
			continue
		}
		acq, perr := strconv.ParseUint(toks[4], 10, 64)
		if perr != nil {
			continue
		}
		act, derr := mqttDecodeOutboundAction(e.body, a.ID, a.Epoch, toks[2], toks[3], acq)
		if derr != nil {
			continue
		}
		acquired[act.Nonce+"."+strconv.FormatUint(act.Acq, 10)] = mqttAcquiredAction{pi: act.PI, kind: act.Kind}
	}
	return acquired
}

// checkActionTokenMembers validates every register token-acquisition member an acquire-action
// transition carries, and binds it to the action RECORD acquired in the same batch (F-105 row 2,
// §4.3 point 1): the two must describe the SAME acquisition, so the token member's obligation
// (its lineage), identifier and advanced counter agree with an action record the batch creates.
// A token installed beside an action for a different exchange, identifier or counter is refused,
// so neither carrier can be forged past the other.
//
// IN BOTH DIRECTIONS. An action record with no token member beside it is refused too: without the
// register CAS nothing excludes a second acquisition while the first is outstanding, and create-only
// action subjects only stop a counter being reused.
func (jsa *mqttJSA) checkActionTokenMembers(a *mqttTransitionAttempt, extra []mqttTransitionMember) error {
	acquired := mqttAcquiredActionsIn(a, extra)
	tokened := map[string]struct{}{}
	for _, e := range extra {
		if mqttIsF105Subject(e.subj) || e.expect == 0 {
			continue
		}
		if _, perr := mqttParseInflightSubject(e.subj); perr != nil {
			continue
		}
		rec, derr := mqttDecodeInflightRecord(e.body)
		if derr != nil || rec.ActionToken == _EMPTY_ {
			continue // not a token member (a replacement is checked elsewhere)
		}
		// AN ACKNOWLEDGEMENT RE-PHASE PRESERVES the occupant's token, it does not install one
		// (F-105 row 3). checkAckPhaseAdvanceMembers proves rec is the occupant with only its
		// state moved, so the token in a pubrel_intended body is the lease already acquired,
		// not a new one — this rule is about INSTALLATION and must not fire on it.
		if a.Kind == mqttTransitionAdvanceExchange && mqttAdvanceKindSlotState(rec) {
			continue
		}
		if a.Kind != mqttTransitionAcquireAction {
			return fmt.Errorf("transition %q of kind %q installs an output-custody token on %q: a "+
				"token is installed only by an acquire-action transition", a.Txn, a.Kind, e.subj)
		}
		key := rec.Lin + "." + strconv.FormatUint(rec.Acq, 10)
		got, ok := acquired[key]
		if !ok {
			return fmt.Errorf("transition %q installs a token on %q for obligation %q at acquisition "+
				"%d, but no action record in the batch acquires it: the token and the record are one "+
				"acquisition and neither stands without the other", a.Txn, e.subj, rec.Lin, rec.Acq)
		}
		if got.pi != rec.PI {
			return fmt.Errorf("transition %q installs a token on identifier %d but its action record "+
				"names identifier %d: the two carriers must describe the same acquisition", a.Txn, rec.PI, got.pi)
		}
		tokened[key] = struct{}{}
	}
	for key := range acquired {
		if _, ok := tokened[key]; !ok {
			return fmt.Errorf("transition %q creates action %q with no output-custody token installed "+
				"beside it: the record and the token are one acquisition, and a record alone excludes "+
				"no second acquisition while the first is outstanding", a.Txn, key)
		}
	}
	return nil
}

// checkRetainedAckPredecessor applies the retained pair verdict to an acknowledgement advance's
// standing register and the exchange it advances, read at the member's expected sequence.
func (jsa *mqttJSA) checkRetainedAckPredecessor(a *mqttTransitionAttempt, cur, rec *mqttInflightRecord,
	gen, nonce string, expect uint64) error {
	x, xSeq, xerr := jsa.loadOutboundExchange(a.ID, a.Epoch, gen, nonce)
	if xerr != nil || x == nil {
		return fmt.Errorf("transition %q advances retained exchange %q whose standing record cannot be read (%v)",
			a.Txn, nonce, xerr)
	}
	if xSeq != expect {
		return fmt.Errorf("transition %q advances retained exchange %q from sequence %d, which no longer stands (%d)",
			a.Txn, nonce, expect, xSeq)
	}
	canon := mqttCanonicalRequired
	if rec.State == mqttInflightPubRelReady && cur.State != mqttInflightPubRelReady {
		canon = mqttCanonicalCreatedHere
	}
	agrees, reason, verr := jsa.retainedPairVerdict(a.ID, a.Epoch, gen, cur, x, canon)
	if verr != nil {
		return fmt.Errorf("transition %q advances retained exchange %q whose canonical PUBREL cannot be read (%v)",
			a.Txn, nonce, verr)
	}
	if !agrees {
		return fmt.Errorf("transition %q advances retained exchange %q over records that contradict each other: %s",
			a.Txn, nonce, reason)
	}
	return nil
}

// checkAckPhaseAdvanceMembers validates every in-flight slot member an advance-exchange
// transition carries, and binds it to the exchange member acquired in the same batch (F-105
// row 3): the slot re-phase and the phase move are ONE acknowledgement, so a slot advancing to
// publish_terminal can never ride beside an exchange moving to PUBREC-observed, and a re-phase
// can never name a different obligation, identifier or generation than the exchange member.
//
// It is also the gatekeeper the register-member generation exemption in mqttCheckTransitionMember
// defers to: that boundary lets an advance-exchange register member name an EARLIER generation
// (a G1 slot re-phased while G2 serves), and this rule is what makes that safe — every such
// member must be a genuine acknowledgement arrow bound to a same-generation exchange advance,
// and any other in-flight member on an advance is refused here outright.
func (jsa *mqttJSA) checkAckPhaseAdvanceMembers(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	xchPred map[string]mqttExchangePhase) error {
	if a.Kind != mqttTransitionAdvanceExchange {
		// The re-phasing slot member is the completion of the same acknowledgement the exchange
		// member records, so it rides ONLY on an advance. No other kind may carry an in-flight
		// register member that is not a replacement or a token acquisition (both checked
		// elsewhere), and a bare re-phase on another kind would record a client answer with no
		// phase move beside it.
		for _, e := range extra {
			if mqttIsF105Subject(e.subj) || e.expect == 0 {
				continue
			}
			if _, perr := mqttParseInflightSubject(e.subj); perr != nil {
				continue
			}
			rec, derr := mqttDecodeInflightRecord(e.body)
			if derr != nil {
				continue // its own member check refuses the shape
			}
			// The STATE-ONLY test here: a retained slot's pubrel_ready is an advance's destination
			// AND the state its PUBREL acquisitions write their token at, so it is not by itself a
			// re-phase on a non-advance kind (F-105 7c).
			if mqttAckRephaseState(rec.State) {
				return fmt.Errorf("transition %q of kind %q re-phases a slot to %q on %q: an acknowledgement "+
					"re-phase rides only on an advance-exchange transition, beside the phase move it completes",
					a.Txn, a.Kind, string(rec.State), e.subj)
			}
		}
		return nil
	}

	// THE EXCHANGE THIS ACKNOWLEDGEMENT ADVANCES. An advance carries exactly one exchange
	// member; the slot member is bound to it.
	var xch *mqttOutboundExchange
	var xchGen, xchNonce, xchSubj string
	var xchExpect uint64
	for _, e := range extra {
		cls, ok := mqttF105ObligationClass(e.subj)
		if !ok || cls != mqttExchangeClassTk {
			continue
		}
		toks := strings.Split(e.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			return fmt.Errorf("transition %q carries a malformed exchange subject %q", a.Txn, e.subj)
		}
		dec, derr := mqttDecodeOutboundExchange(e.body, a.ID, a.Epoch, toks[2], toks[3])
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported exchange on %q: %w", a.Txn, e.subj, derr)
		}
		if xch != nil {
			return fmt.Errorf("transition %q carries more than one exchange member: an advance moves ONE obligation", a.Txn)
		}
		xch, xchGen, xchNonce, xchSubj, xchExpect = dec, toks[2], toks[3], e.subj, e.expect
	}

	slots := 0
	for _, e := range extra {
		if mqttIsF105Subject(e.subj) {
			continue
		}
		name, perr := mqttParseInflightSubject(e.subj)
		if perr != nil {
			// A marker or any other in-flight member is not an acknowledgement re-phase, and an
			// advance carries nothing else on this stream.
			return fmt.Errorf("transition %q carries member %q, which is not an acknowledgement slot "+
				"re-phase an advance-exchange transition may carry", a.Txn, e.subj)
		}
		rec, derr := mqttDecodeInflightRecord(e.body)
		if derr != nil {
			return fmt.Errorf("transition %q carries an unsupported slot re-phase on %q: %w", a.Txn, e.subj, derr)
		}
		slots++
		cur, smsg, lerr := jsa.loadInflightRecord(e.subj)
		if lerr != nil {
			return fmt.Errorf("transition %q could not read the slot it re-phases on %q: %w", a.Txn, e.subj, lerr)
		}
		if smsg == nil || cur == nil {
			return fmt.Errorf("transition %q re-phases the slot on %q, but no slot this broker can read stands there",
				a.Txn, e.subj)
		}
		clearance := mqttIsDrainTokenClearance(cur, rec)
		if !clearance && !mqttIsAckPhaseAdvance(cur, rec) {
			return fmt.Errorf("transition %q re-phases the slot on %q from %q to %q, which is neither an "+
				"acknowledgement arrow nor a PUBREL holder's token clearance", a.Txn, e.subj,
				string(cur.State), string(rec.State))
		}
		if xch == nil {
			return fmt.Errorf("transition %q re-phases a slot on %q with no exchange member to bind it to", a.Txn, e.subj)
		}
		// BOUND TO THE EXCHANGE MEMBER: same generation (a G1 slot re-phases only beside a G1
		// exchange advance), same obligation identity (Lin == the exchange nonce) and the same
		// identifier. This is the pairing mqttCheckTransitionMember's generation exemption
		// defers to.
		if name.gen != xchGen {
			return fmt.Errorf("transition %q re-phases a slot in generation %q beside an exchange advance in "+
				"generation %q", a.Txn, name.gen, xchGen)
		}
		if rec.Lin != xchNonce || rec.PI != xch.PI {
			return fmt.Errorf("transition %q re-phases slot obligation %q/identifier %d beside an exchange advance "+
				"of %q/identifier %d", a.Txn, rec.Lin, rec.PI, xchNonce, xch.PI)
		}
		// AND TO THE EXCHANGE'S VALIDATED PREDECESSOR. checkExchangeOverwrites read the standing
		// exchange at the member's own expected sequence; that phase, and not the destination,
		// is what says WHICH arrow this is. The successor table admits two routes into Terminal
		// (a PUBCOMP from PUBREC-observed, a drain from pending-drain), so a destination check
		// would let a slot member of one ride beside the other.
		pred, ok := xchPred[xchSubj]
		if !ok {
			return fmt.Errorf("transition %q re-phases the slot on %q beside an exchange member whose standing "+
				"phase was never validated", a.Txn, e.subj)
		}
		// A RETAINED SLOT ADVANCES ONLY OVER A CONSISTENT PAIR (consult 109): the standing register and
		// the exchange's validated PREDECESSOR — read whole, at the exchange member's own expected
		// sequence — must agree by the one definition recovery restores pairs with. The advance
		// stays tenure-agnostic (a client's answer crosses tenures); what it may not do is advance a
		// contradiction a resume quarantines. A PUBREC creates the canonical this batch validates as
		// its own member; every other arrow requires the standing one where the phase has it.
		if cur.retainedSource() {
			if err := jsa.checkRetainedAckPredecessor(a, cur, rec, xchGen, xchNonce, xchExpect); err != nil {
				return err
			}
		}
		if clearance {
			// A PUBREL HOLDER'S DRAIN ENDS A PENDING-DRAIN COMPLETION, and nothing else: the token it
			// clears is the outstanding output that made the PUBCOMP pending-drain.
			if pred != mqttExchangeTerminalPendingDrain || xch.Phase != mqttExchangeTerminal {
				return fmt.Errorf("transition %q clears the PUBREL token on %q beside an exchange moving from phase "+
					"%d to %d: a token clearance completes only terminal-pending-drain -> terminal",
					a.Txn, e.subj, pred, xch.Phase)
			}
			continue
		}
		// THE SLOT STATE ANSWERS THE EXCHANGE PHASE, from the slot's own origin (mqttAckSlotArrow):
		// a PUBLISH ending takes publish_terminal, a successful PUBREC pubrel_intended, a PUBCOMP
		// pubrel_terminal. A PUBLISH pending-drain ending has no slot arrow and is refused here.
		if want, _ := mqttAckExchangeOrigin(cur.State); pred != want {
			return fmt.Errorf("transition %q re-phases a %q slot on %q beside an exchange standing at phase %d, "+
				"not %d", a.Txn, string(cur.State), e.subj, pred, want)
		}
		wantSlot, ok := mqttAckSlotArrow(cur.retainedSource(), cur.State, xch.Phase)
		if !ok || rec.State != wantSlot {
			return fmt.Errorf("transition %q re-phases the slot to %q while the exchange advances to phase %d: "+
				"the slot state must answer the acknowledgement's phase", a.Txn, string(rec.State), xch.Phase)
		}
		switch wantSlot {
		case mqttInflightPublishTerminal:
			// THE PUBLISH TERMINAL ARROW REFUSES A LIVE ACTION TOKEN, the store guard
			// casInflightSlot(mqttSlotAdvanceAcknowledgedTerminal) applies, reconstructed here
			// because the batch bypasses that path. The binding to the ANSWERED transmission is the
			// builder's (Acq == the packet's), fenced by the exact-sequence CAS; rec.Acq == cur.Acq
			// already, since an acknowledgement arrow moves only the state.
			if cur.ActionToken != _EMPTY_ {
				return fmt.Errorf("transition %q terminalises the slot on %q while a live action token stands: its "+
					"holder may still be writing the transmission it answers", a.Txn, e.subj)
			}
		case mqttInflightPubRelTerminal:
			// A PUBCOMP IS PENDING-DRAIN EXACTLY WHEN A PUBREL ACTION IS LIVE. Terminal over a live
			// token would license the release while a PUBREL may still be on the wire; pending-drain
			// over no token would leave an exchange nothing will ever drain.
			if live := cur.ActionToken != _EMPTY_; live != (xch.Phase == mqttExchangeTerminalPendingDrain) {
				return fmt.Errorf("transition %q records PUBCOMP phase %d on %q while its PUBREL action is live=%v: "+
					"terminal-pending-drain exactly when a token stands", a.Txn, xch.Phase, e.subj, live)
			}
		}
	}
	// AN EXCHANGE ADVANCES ONLY BESIDE ITS SLOT. Every acknowledgement records the client's
	// answer on the exchange AND re-phases the slot it answers, and a drain completion clears the
	// holder's token on that slot; both are one batch (opAdvanceExchangeAck,
	// opCompleteExchangeDrain). An exchange advanced alone would move the obligation while the
	// slot that recovery reads still says otherwise, so it is refused here rather than left to
	// the care of whoever composed it.
	if xch != nil && slots == 0 {
		return fmt.Errorf("transition %q advances exchange %q with no slot member beside it: an exchange "+
			"advances only beside the slot re-phase or token clearance that records the same answer",
			a.Txn, xchNonce)
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
	// An earlier revision asked only whether the exchange had ENDED, which let three phases
	// through that should not pass: PubRecObserved and both pending-drain phases
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
		if rec.Acq != cur.Acq {
			return fmt.Errorf("transition %q rewrites the register on %q with acquisition counter %d "+
				"while its occupant carries %d: a continued obligation keeps its transmission history",
				a.Txn, regSubj, rec.Acq, cur.Acq)
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

// checkRefutedCancellation is the write boundary's evidence half of mqttRefutedCancellation, asked
// of the two members that may clear a cancellation bit: an acknowledgement re-phase and an
// output-token acquisition. A member that does not clear one costs nothing (no marker read); one
// that does is admitted only when the unsent marker of this allocation, read here after the
// exact predecessor, is authoritatively absent. A standing marker is a genuine cancellation of a
// PUBLISH that never went out, and an unreadable read proves nothing: both refuse.
func (jsa *mqttJSA) checkRefutedCancellation(a *mqttTransitionAttempt, subj string, cur, rec *mqttInflightRecord) error {
	if !mqttClearsCancellation(cur, rec) {
		return nil
	}
	if !mqttRefutedCancellation(cur, rec, jsa.loadOccupantMarkerLoad(subj)) {
		return fmt.Errorf("transition %q of kind %q clears the cancellation on %q, but the unsent marker of "+
			"allocation %q does not read authoritatively absent: a cancellation is cleared only where the "+
			"client's answer or a retransmission's acquisition refutes it", a.Txn, a.Kind, subj, cur.Nonce)
	}
	return nil
}

// mqttSlotKey is the (generation, identifier) a register and its marker share.
func mqttSlotKey(name *mqttInflightName) string {
	return name.gen + "." + strconv.FormatUint(uint64(name.pi), 10)
}

// mqttMarkerNamesAllocation reports whether a marker read found a standing, decodable unsent
// marker naming exactly this ALLOCATION (a register's Nonce, not its lineage).
//
// ONE comparison, shared by the op that gathers a withdrawal's evidence and the boundary that
// re-checks it, because the two must agree about what counts: a marker that is absent,
// unreadable, undecodable or names another allocation proves nothing about this one. A full
// replacement writes a marker for its OWN fresh allocation, so a marker of another nonce on the
// same subject is exactly the confusion the nonce comparison prevents.
func mqttMarkerNamesAllocation(load mqttMarkerLoad, nonce string) bool {
	return nonce != _EMPTY_ && load.kind == mqttInflightResponsePresent && load.decodeErr == nil &&
		load.marker != nil && load.marker.Nonce == nonce
}

// checkWithdrawalRegister is the withdraw-unsent rule for the register member, and it VALIDATES
// THE EVIDENCE ITSELF rather than trusting the op that composed the batch — the same stance the
// replacement rules take above.
//
// The claim a withdrawal makes is "no PUBLISH of this obligation ever reached the client". An
// ordinary PUBLISH is written only by the holder of an output token acquired through the
// authority, and only after the allocation's unsent marker is durably gone (the section's
// confirmed intent delete, or an earlier one). So the claim holds exactly when, at the register
// sequence this batch is conditioned on, no token stands AND — read after that register — the
// marker naming the allocation still stands: any acquisition since rewrites the register, and
// the CAS on it then fails.
//
// cur is the exact predecessor checkRegisterReplacements already loaded at the member's
// expectation; this runs after that load, so the marker read below is ordered after it.
func (jsa *mqttJSA) checkWithdrawalRegister(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	subj string, cur, rec *mqttInflightRecord) error {

	// 1. A tokenless publish occupant of the obligation the paired exchange names.
	if cur.State != mqttInflightPublish {
		return fmt.Errorf("transition %q withdraws the register on %q in state %q: only an obligation "+
			"still at its publish can have been unsent", a.Txn, subj, string(cur.State))
	}
	if cur.ActionToken != _EMPTY_ {
		return fmt.Errorf("transition %q withdraws the register on %q while output token %q stands: "+
			"a holder may be writing its PUBLISH right now", a.Txn, subj, cur.ActionToken)
	}
	paired := false
	for _, x := range extra {
		if cls, ok := mqttF105ObligationClass(x.subj); !ok || cls != mqttExchangeClassTk {
			continue
		}
		toks := strings.Split(x.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			continue
		}
		xb, derr := mqttDecodeOutboundExchange(x.body, a.ID, a.Epoch, toks[2], toks[3])
		if derr != nil || xb == nil {
			continue
		}
		if xb.Phase == mqttExchangeWithdrawnUnsent && xb.Nonce == cur.Lin && xb.PI == cur.PI && xb.QoS == cur.QoS {
			paired = true
		}
	}
	if !paired {
		return fmt.Errorf("transition %q withdraws the register on %q without the withdrawal of its "+
			"obligation %q beside it", a.Txn, subj, cur.Lin)
	}

	// 2. The body is the occupant with Cancelled set, and nothing else changed: custody, the
	// acquisition counter, the allocation and the delivery all stay what they were.
	if cur.CancelOrigin == mqttCancelOriginPendingCreate {
		return fmt.Errorf("transition %q withdraws the register on %q, which is a pending cancellation "+
			"fence: a fence carries no obligation to withdraw", a.Txn, subj)
	}
	want := *cur
	want.Cancelled, want.CancelOrigin = true, mqttCancelOriginOccupant
	if want != *rec {
		return fmt.Errorf("transition %q withdraws the register on %q with a body that changes more "+
			"than its cancellation: a withdrawal ends the occupant, it does not rewrite it", a.Txn, subj)
	}

	// 3. The unsent marker of THIS allocation still stands, read after the occupant.
	if !mqttMarkerNamesAllocation(jsa.loadOccupantMarkerLoad(subj), cur.Nonce) {
		return fmt.Errorf("transition %q withdraws the register on %q, but no unsent marker naming its "+
			"allocation %q stands: without it a transmission may have happened, and the obligation "+
			"is not provably unsent", a.Txn, subj, cur.Nonce)
	}
	return nil
}
