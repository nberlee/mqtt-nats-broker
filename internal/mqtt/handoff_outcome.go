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
	"fmt"
	"strconv"
	"strings"
)

// The handoff metadata outcome (F-105).
//
// $MQTT_sess and $MQTT_inflight cannot commit together — a JetStream atomic batch is per
// stream — so an ownership handoff is three durable steps: PREPARE on $MQTT_inflight, which
// reserves the transition and temporarily freezes new admission without closing anything;
// COMMIT METADATA here on $MQTT_sess; then FINALIZE on $MQTT_inflight, which installs the
// new serving tenure or closes the predecessor's generation. CONNACK follows the final
// ownership result and nothing earlier.
//
// This file owns the middle step's permanent evidence. A recovery that finds a preparation
// with no finalization has exactly one question to answer — did the metadata half commit? —
// and it must answer it from a record that EXISTS. Looking for the session record's effect
// cannot do that: the record is a single-slot CAS, so a later write replaces the body this
// handoff would have left, and the reader would be inferring from an absence.
//
// So both verdicts contend, create-only, for ONE subject per intent. If the abort lands
// first the metadata batch's own create-only member can never succeed and the whole batch is
// refused; if the batch landed first the abort fails and the readback says COMMITTED. Two
// slots, or one overwritable slot, would let an abort stand beside a later commit and
// decide nothing — which is the shape that would let a displaced acceptance be finalized.
//
// The subject space is TOP-LEVEL rather than nested under "sess.", for the reason the
// acceptance facts are: every session walk in this package filters "sess.<domainTk>>" and
// decodes what it gets as a session record. A handoff outcome under that prefix would be
// read as a malformed session.

// mqttHandoffSubjectPrefix is the third subject space on $MQTT_sess.
const mqttHandoffSubjectPrefix = mqttPrefix + "handoff."

// mqttHandoffOutcomeSubject returns the permanent outcome subject of one handoff intent.
func mqttHandoffOutcomeSubject(domainTk, cidHash, intentID string) string {
	return mqttHandoffSubjectPrefix + domainTk + cidHash + "." + intentID
}

// mqttHandoffClientFilter enumerates one client's handoff outcomes, and only those.
func mqttHandoffClientFilter(domainTk, cidHash string) string {
	return mqttHandoffSubjectPrefix + domainTk + cidHash + ".*"
}

// mqttProposalKind names which ownership proposal an outcome answers.
type mqttProposalKind string

const (
	mqttProposalHandoff  mqttProposalKind = "handoff"
	mqttProposalCreation mqttProposalKind = "creation"
)

func mqttValidProposalKind(k mqttProposalKind) bool {
	return k == mqttProposalHandoff || k == mqttProposalCreation
}

// mqttHandoffVerdict is the permanent verdict of one handoff's metadata half. As elsewhere
// in F-105 the zero value names neither, so a body missing the member cannot decode as the
// permissive one.
type mqttHandoffVerdict uint8

const (
	mqttHandoffCommitted mqttHandoffVerdict = 1
	mqttHandoffAborted   mqttHandoffVerdict = 2
)

// mqttHandoffOutcome is that permanent verdict.
//
// It names the intent AND the predecessor the intent was conditioned on, so a recovery
// reading it in isolation still knows what was displaced. A committed outcome additionally
// carries the session sequence the metadata write landed at, which is what lets the
// finalization contend against the right record rather than against whatever stands there
// when recovery happens to run.
type mqttHandoffOutcome struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	// Intent is the proposal this outcome answers for, and names the subject.
	Intent  string             `json:"intent"`
	Verdict mqttHandoffVerdict `json:"verdict"`
	// Kind says which proposal shape this answers. It is mandatory, because the two have
	// DIFFERENT required members and a reader that guessed would either demand a
	// predecessor a first creation never had, or admit a handoff that named none.
	//
	// Both shapes share ONE subject per proposal, deliberately: the create-only mutual
	// exclusion between commit and abort is the whole fence, and splitting the slot by
	// shape would give a creation and a handoff of the same id two independent verdicts.
	Kind mqttProposalKind `json:"kind"`

	// The predecessor this handoff was conditioned on, in full. Present for a HANDOFF and
	// absent for a creation, which has no predecessor to name — that absence is the shape
	// difference, not a missing field.
	PrevGen      string `json:"prev_gen,omitempty"`
	PrevTenure   string `json:"prev_tenure,omitempty"`
	PrevOwnerTxn string `json:"prev_owner_txn,omitempty"`
	PrevSessWID  string `json:"prev_sess_wid,omitempty"`
	PrevSessSeq  uint64 `json:"prev_sess_seq,omitempty"`

	// SessWID is the write identity the metadata half INSTALLS. Committed outcomes only.
	//
	// A write identity and not a sequence, deliberately. An earlier revision of this record
	// carried the sequence the metadata write landed at, which cannot work: the body is
	// assembled BEFORE its own batch commits, the batch API returns the commit member's
	// sequence only afterwards, and a body may not be substituted after publication without
	// destroying the create-only permanence this record rests on. Inferring it from the
	// acknowledgement is forbidden outright. A WID is minted by the writer, so it is known
	// before publication and identifies the record by IDENTITY rather than by position —
	// which is what a repair needs anyway, since a replacement moves the position and the
	// identity is what says whether the record is still ours.
	SessWID string `json:"sess_wid,omitempty"`

	// Proposal is the immutable proposal this handoff was for, carried so a recovering
	// broker can finalize the right thing after the authority's inline intent has been
	// overwritten by a later transition. Committed outcomes only.
	ProposedGen    string `json:"proposed_gen,omitempty"`
	ProposedTenure string `json:"proposed_tenure,omitempty"`
}

// The member sets are indexed by (proposal kind, verdict): a first creation names no
// predecessor, so demanding one would refuse every legitimate creation receipt, and admitting
// a handoff without one would lose the binding that stops a delayed acceptance retargeting a
// successor.
var (
	mqttOutcomeBase = []string{"id", "epoch", "intent", "verdict", "kind"}
	mqttOutcomePrev = []string{"prev_gen", "prev_tenure", "prev_owner_txn",
		"prev_sess_wid", "prev_sess_seq"}
	mqttOutcomeProduced = []string{"sess_wid", "proposed_gen", "proposed_tenure"}
)

func mqttOutcomeMemberSet(kind mqttProposalKind, verdict mqttHandoffVerdict) map[string]struct{} {
	out := map[string]struct{}{}
	add := func(ks []string) {
		for _, k := range ks {
			out[k] = struct{}{}
		}
	}
	add(mqttOutcomeBase)
	if kind == mqttProposalHandoff {
		add(mqttOutcomePrev)
	}
	if verdict == mqttHandoffCommitted {
		add(mqttOutcomeProduced)
	}
	return out
}

// mqttDecodeHandoffOutcome decodes and fully validates one handoff outcome body.
//
// Verdict-scoped, exactly as the transition outcome is: an aborted metadata half installed
// no record, so a body claiming a session sequence for one is contradictory rather than
// over-specified.
func mqttDecodeHandoffOutcome(data []byte, wantID, wantEpoch, wantIntent string) (*mqttHandoffOutcome, error) {
	if err := mqttJSONMembersArePresent(data,
		map[string]struct{}{"id": {}, "epoch": {}, "intent": {}, "verdict": {}, "kind": {}},
		"a handoff outcome"); err != nil {
		return nil, err
	}
	probe := struct {
		Verdict mqttHandoffVerdict `json:"verdict"`
		Kind    mqttProposalKind   `json:"kind"`
	}{}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, err
	}
	if !mqttValidProposalKind(probe.Kind) {
		return nil, mqttShapeErr("a handoff outcome carries an unsupported proposal kind " +
			string(probe.Kind))
	}
	switch probe.Verdict {
	case mqttHandoffCommitted, mqttHandoffAborted:
	default:
		return nil, mqttShapeErr("a handoff outcome carries an unsupported verdict " +
			strconv.FormatUint(uint64(probe.Verdict), 10))
	}
	members := mqttOutcomeMemberSet(probe.Kind, probe.Verdict)
	required := members
	if err := mqttJSONMembersAreExactAndUnique(data, members, "a handoff outcome"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, required, "a handoff outcome"); err != nil {
		return nil, err
	}
	o := &mqttHandoffOutcome{}
	if err := json.Unmarshal(data, o); err != nil {
		return nil, err
	}
	if err := mqttValidateHandoffOutcome(o, wantID, wantEpoch, wantIntent); err != nil {
		return nil, err
	}
	return o, nil
}

// mqttValidateHandoffOutcome refuses a body this broker does not support.
func mqttValidateHandoffOutcome(o *mqttHandoffOutcome, wantID, wantEpoch, wantIntent string) error {
	if o == nil {
		return fmt.Errorf("handoff outcome is absent")
	}
	if o.ID != wantID {
		return fmt.Errorf("handoff outcome names client %q, not %q", o.ID, wantID)
	}
	if o.Epoch != wantEpoch {
		return fmt.Errorf("handoff outcome carries recovery epoch %q, not this account's %q", o.Epoch, wantEpoch)
	}
	if o.Intent != wantIntent {
		return fmt.Errorf("handoff outcome names intent %q on the subject of %q", o.Intent, wantIntent)
	}
	if !mqttValidProposalKind(o.Kind) {
		return fmt.Errorf("handoff outcome carries an unsupported proposal kind %q", o.Kind)
	}
	switch o.Kind {
	case mqttProposalHandoff:
		// A HANDOFF names its predecessor in full, and that requirement is exactly what
		// stops a delayed acceptance retargeting a successor.
		if !mqttValidSessGen(o.PrevGen) {
			return fmt.Errorf("handoff outcome names a malformed predecessor generation %q", o.PrevGen)
		}
		if !mqttF105IdentTk(o.PrevTenure) {
			return fmt.Errorf("handoff outcome names a malformed predecessor tenure %q", o.PrevTenure)
		}
		if !mqttF105IdentTk(o.PrevOwnerTxn) {
			return fmt.Errorf("handoff outcome names a malformed predecessor ownership transition %q", o.PrevOwnerTxn)
		}
		if o.PrevSessWID == _EMPTY_ {
			return fmt.Errorf("handoff outcome names no predecessor session write identity")
		}
		if o.PrevSessSeq == 0 {
			return fmt.Errorf("handoff outcome names a predecessor session record at sequence zero")
		}
	case mqttProposalCreation:
		// A FIRST CREATION has no predecessor, and a receipt claiming one would describe a
		// displacement that never happened.
		if o.PrevGen != _EMPTY_ || o.PrevTenure != _EMPTY_ || o.PrevOwnerTxn != _EMPTY_ ||
			o.PrevSessWID != _EMPTY_ || o.PrevSessSeq != 0 {
			return fmt.Errorf("a creation outcome names a predecessor it never displaced")
		}
	}
	switch o.Verdict {
	case mqttHandoffCommitted:
		if o.SessWID == _EMPTY_ {
			return fmt.Errorf("committed handoff outcome carries no session write identity")
		}
		// The metadata half REPLACES its predecessor, so it cannot claim to install the
		// identity it displaced.
		if o.Kind == mqttProposalHandoff && o.SessWID == o.PrevSessWID {
			return fmt.Errorf("committed handoff outcome installs the predecessor's own write identity %q", o.SessWID)
		}
		if !mqttValidSessGen(o.ProposedGen) {
			return fmt.Errorf("committed handoff outcome carries a malformed proposed generation %q", o.ProposedGen)
		}
		if !mqttF105IdentTk(o.ProposedTenure) {
			return fmt.Errorf("committed handoff outcome carries a malformed proposed tenure %q", o.ProposedTenure)
		}
	case mqttHandoffAborted:
		if o.SessWID != _EMPTY_ || o.ProposedGen != _EMPTY_ || o.ProposedTenure != _EMPTY_ {
			return fmt.Errorf("aborted handoff outcome describes a metadata write it did not perform")
		}
	default:
		return fmt.Errorf("handoff outcome carries an unsupported verdict %d", o.Verdict)
	}
	return nil
}

// mqttOwnershipProposal is what the metadata half commits, for EITHER proposal shape.
//
// One view rather than two parallel protocols. A first creation and a handoff differ in
// exactly one respect — whether a predecessor is displaced — and everything else about the
// metadata half is identical: one create-only permanent slot per proposal, a frozen attempt,
// an abort that races it. Two protocols would be two places for the create-only fence to
// drift, and the fence is the whole mechanism.
//
// Kind is set by the constructors below and NEVER by a caller. The outcome validator refuses
// a mismatched shape, but that is the backstop; this is the definition.
type mqttOwnershipProposal struct {
	Kind mqttProposalKind
	ID   string
	// What the proposal installs if it commits.
	Gen    string
	Tenure string
	// The displaced predecessor, in full. Zero throughout for a creation, which displaces
	// nothing — that emptiness is the shape difference and is checked, not assumed.
	PrevGen      string
	PrevTenure   string
	PrevOwnerTxn string
	PrevSessWID  string
	PrevSessSeq  uint64
}

// mqttProposalFromHandoff views a handoff intent as a proposal.
func mqttProposalFromHandoff(i *mqttHandoffIntent) (mqttOwnershipProposal, error) {
	if err := mqttValidateHandoffIntent(i); err != nil {
		return mqttOwnershipProposal{}, err
	}
	return mqttOwnershipProposal{
		Kind: mqttProposalHandoff, ID: i.ID, Gen: i.Gen, Tenure: i.Tenure,
		PrevGen: i.PrevGen, PrevTenure: i.PrevTenure, PrevOwnerTxn: i.PrevOwnerTxn,
		PrevSessWID: i.PrevSessWID, PrevSessSeq: i.PrevSessSeq,
	}, nil
}

// mqttProposalFromCreation views a first-creation intent as a proposal.
//
// It stays a SEPARATE type from the handoff intent because the handoff's full-predecessor
// requirement is load-bearing: a creation that could be expressed as a handoff with empty
// predecessor members would let a genuine handoff lose its binding by claiming to be one.
func mqttProposalFromCreation(i *mqttCreationIntent) (mqttOwnershipProposal, error) {
	if err := mqttValidateCreationIntent(i); err != nil {
		return mqttOwnershipProposal{}, err
	}
	return mqttOwnershipProposal{
		Kind: mqttProposalCreation, ID: i.ID, Gen: i.Gen, Tenure: i.Tenure,
	}, nil
}

// mqttOutcomeAnswersProposal reports whether a standing outcome answers for the proposal a
// caller is resolving, rather than merely sharing its subject.
//
// THE KIND IS COMPARED FIRST. One subject holds one proposal's verdict, so a receipt of the
// other shape under that subject is a contradiction — the id collision is improbable, but
// what makes it impossible is the check, not the improbability.
//
// The predecessor is the rest of it: an outcome conditioned on a different predecessor
// describes a different handoff, and acting on it would finalize against ownership nobody
// checked. For a creation the comparison is against emptiness, which is the same test.
func mqttOutcomeAnswersProposal(o *mqttHandoffOutcome, p mqttOwnershipProposal) error {
	if o.Intent != p.ID {
		return fmt.Errorf("handoff outcome answers intent %q, not %q", o.Intent, p.ID)
	}
	if o.Kind != p.Kind {
		return fmt.Errorf("the standing receipt for %q is a %s outcome but this is a %s proposal",
			p.ID, o.Kind, p.Kind)
	}
	if o.Verdict == mqttHandoffCommitted &&
		(o.ProposedGen != p.Gen || o.ProposedTenure != p.Tenure) {
		return fmt.Errorf("handoff outcome was for proposal %q/%q but the intent proposes %q/%q",
			o.ProposedGen, o.ProposedTenure, p.Gen, p.Tenure)
	}
	if o.PrevGen != p.PrevGen || o.PrevTenure != p.PrevTenure || o.PrevOwnerTxn != p.PrevOwnerTxn ||
		o.PrevSessWID != p.PrevSessWID || o.PrevSessSeq != p.PrevSessSeq {
		return fmt.Errorf("handoff outcome is conditioned on %q/%q/%q/%q/%d but the intent names %q/%q/%q/%q/%d",
			o.PrevGen, o.PrevTenure, o.PrevOwnerTxn, o.PrevSessWID, o.PrevSessSeq,
			p.PrevGen, p.PrevTenure, p.PrevOwnerTxn, p.PrevSessWID, p.PrevSessSeq)
	}
	return nil
}

// mqttIsHandoffSubject reports whether a subject is a handoff outcome. The session walks
// filter on the session prefix and would never see one, but the accept-fact readers and the
// record-absent paths take subjects from enumerations, so the classification is available
// rather than assumed.
func mqttIsHandoffSubject(subject string) bool {
	return strings.HasPrefix(subject, mqttHandoffSubjectPrefix)
}
