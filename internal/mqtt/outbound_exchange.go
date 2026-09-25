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
	"encoding/json"
	"fmt"
	"strconv"
)

// The outbound exchange and its actions (F-105).
//
// An EXCHANGE is the client-facing obligation: one outbound message this broker owes a
// client an outcome for. Its identity is (account epoch, client, generation, exchange
// nonce), and it is preserved across transfer and across allocation replacement. A packet
// identifier is an ALLOCATION belonging to an exchange, never its identity — which is why an
// identifier can be reserved while its exchange is still outstanding, and why two exchanges
// on one identifier at different times are different obligations.
//
// An ACTION is one acquired operation for an exchange: a PUBLISH to emit, a PUBREL to emit.
// It names the exchange, its own acquisition counter, and — this is the part that matters —
// the ORIGINAL tenure and connection that acquired it. A transfer moves the serving pointer
// and does not rewrite actions, because an action's completion authority is a different
// question from the authority to begin another one. An action acquired before a retirement
// may still execute afterwards, on its original connection; what it may not do is be
// redirected through a rebound one.
//
// Both records live on $MQTT_inflight, and both are installed by the SAME atomic batch that
// contends on the outbound authority. That is the admission boundary: an exchange that
// existed without an authority transition admitting it would be an obligation nothing
// authorized, which is precisely the state F-105 exists to make unreachable.

// The record-class tokens for the two classes this file owns. As with the arbitration
// records, a leading class token keeps them structurally out of the register machinery's
// reach: neither can satisfy mqttValidIDHashToken or mqttValidSessGen.
const (
	mqttExchangeClassTk = "xch"
	mqttActionClassTk   = "act"
)

// mqttExchangeSubject returns the durable subject of one exchange.
func mqttExchangeSubject(idHash, gen, nonce string) string {
	return mqttF105SubjectPrefix() + mqttExchangeClassTk + "." + idHash + "." + gen + "." + nonce
}

// mqttActionSubject returns the durable subject of one acquired action.
func mqttActionSubject(idHash, gen, nonce string, acq uint64) string {
	return mqttF105SubjectPrefix() + mqttActionClassTk + "." + idHash + "." + gen + "." +
		nonce + "." + strconv.FormatUint(acq, 10)
}

// mqttActionWalkFilter enumerates ONE exchange's acquired actions, and only those.
//
// Bounded by construction for the same reason the exchange filter is: an exchange holds one
// action per acquisition, and a retransmission is a new acquisition rather than an unbounded
// rewrite. It lives here, beside the subject it mirrors, because a caller assembling the
// pattern itself is a second statement of the layout that nothing keeps in step with the first.
func mqttActionWalkFilter(idHash, gen, nonce string) string {
	return mqttF105SubjectPrefix() + mqttActionClassTk + "." + idHash + "." + gen + "." +
		nonce + ".*"
}

// mqttExchangeWalkFilter enumerates one generation's exchanges, and only those. Bounded by
// construction: one generation holds at most as many exchanges as it has outstanding
// obligations, and each names its own subject.
func mqttExchangeWalkFilter(idHash, gen string) string {
	return mqttF105SubjectPrefix() + mqttExchangeClassTk + "." + idHash + "." + gen + ".*"
}

// mqttExchangePhase is where an exchange stands in its protocol.
//
// Numbered from 1 for the reason every other F-105 enum is: a missing or null member decodes
// to zero, and a zero naming a real phase would make an unsupported record read as the most
// permissive one.
type mqttExchangePhase uint8

const (
	// mqttExchangePublish is an outbound PUBLISH whose acknowledgement has not been
	// observed. For QoS 1 that is the whole exchange; for QoS 2 it precedes PUBREC.
	mqttExchangePublish mqttExchangePhase = 1
	// mqttExchangePubRecObserved records a successful PUBREC. From here no further PUBLISH
	// may be acquired: the client has taken ownership of the message and what is owed is
	// the release.
	mqttExchangePubRecObserved mqttExchangePhase = 2
	// 3 is RESERVED. It named a "PUBREL owed" phase that stated nothing PUBREC-observed does
	// not: every consumer treated the two alike, and the canonical PUBREL's identity is the
	// SLOT's fact, bound by the rebind's readback proof. Nothing ever wrote it. A persisted 3
	// is an unsupported shape and is REFUSED by mqttValidExchangePhase — never read as
	// PUBREC-observed, and never renumbered away (F-105 PUBCOMP arm, consult 23).
	// mqttExchangeTerminalPendingDrain is terminal evidence observed while output is still
	// outstanding.
	//
	// It is a phase of its own rather than "terminal", deliberately. Terminal evidence
	// seen during outstanding output does NOT release the exchange: a write acquired
	// earlier may still be executing, and releasing here would let a late first
	// transmission escape after its own release boundary. The drain, or a closure
	// certificate for the action's connection, is what moves it on.
	mqttExchangeTerminalPendingDrain mqttExchangePhase = 4
	// mqttExchangeTerminal is protocol completion with no outstanding output.
	mqttExchangeTerminal mqttExchangePhase = 5
	// mqttExchangeRejectedPendingDrain is a REFUSED PUBREC observed while output is still
	// outstanding. It stands to terminal-pending-drain as rejection stands to completion.
	mqttExchangeRejectedPendingDrain mqttExchangePhase = 6
	// mqttExchangeRejected is a QoS 2 exchange the receiver REFUSED.
	//
	// A PUBREC carrying a reason code >= 0x80 says the client will not take the message:
	// per [MQTT-4.3.3] the sender must NOT send a PUBREL and the exchange is over. That is
	// an ending, but it is NOT the ending mqttExchangeTerminal records — no ownership was
	// transferred and no release was ever owed — so it is its own phase rather than a
	// terminal reached by pretending the PUBREC succeeded. Routing a rejection through
	// PUBREC-observed would put a permanent record on the store claiming the client holds a
	// message it refused, and would oblige a release nothing may emit.
	mqttExchangeRejected mqttExchangePhase = 7
	// mqttExchangeSupersededUnsent is a proven-unsent PUBLISH obligation ended because its
	// packet identifier was re-allocated to a fresh delivery in a PAIR replacement (F-105 step
	// 5, Codex consult 15). It is its OWN absorbing ending, distinct from every acknowledgement
	// phase: no PUBACK, PUBREC or refusal was ever observed — the marker naming the occupant
	// proved the PUBLISH never left the broker — so recording completion (Terminal) or rejection
	// (Rejected) would fabricate a client answer that never came. It exists for both QoS values,
	// and it is reached ONLY through a validated proven-unsent replacement, never through the
	// generic phase-successor table. Absorbing: nothing may be acquired at it, and like every
	// ending it releases the identifier its predecessor held.
	mqttExchangeSupersededUnsent mqttExchangePhase = 8
	// mqttExchangeWithdrawnUnsent is an admitted PUBLISH obligation ended because its delivery
	// was destroyed WITHOUT EVER BEING TRANSMITTED — expired, oversize, cancelled, drained — and
	// no replacement takes its identifier. It is SupersededUnsent's standalone sibling, and a
	// phase of its own for the same reason: no client answer was ever observed, so recording
	// completion or rejection would fabricate one.
	//
	// Reached ONLY by a withdraw-unsent transition (opWithdrawUnsent), never through the
	// successor table, and only on positive whole-exchange evidence the write boundary checks
	// itself: the register at publish with no output token, and the unsent marker naming that
	// allocation still standing. Every ordinary write follows that marker's durable absence, so
	// a standing marker with no token is proof no PUBLISH of it ever reached the client.
	// Absorbing: nothing may be acquired at it.
	mqttExchangeWithdrawnUnsent mqttExchangePhase = 9
)

func mqttValidExchangePhase(p mqttExchangePhase) bool {
	switch p {
	case mqttExchangePublish, mqttExchangePubRecObserved,
		mqttExchangeTerminalPendingDrain, mqttExchangeTerminal,
		mqttExchangeRejectedPendingDrain, mqttExchangeRejected,
		mqttExchangeSupersededUnsent, mqttExchangeWithdrawnUnsent:
		return true
	}
	return false
}

// mqttExchangePhaseEnded reports whether a phase is an ENDING with no outstanding output —
// the phases at which an obligation may be released and its identifier reclaimed.
//
// There are two, because an exchange ends either by completing or by being refused, and a
// predicate is what keeps every consumer from enumerating one of them and forgetting the
// other.
func mqttExchangePhaseEnded(p mqttExchangePhase) bool {
	return p == mqttExchangeTerminal || p == mqttExchangeRejected ||
		p == mqttExchangeSupersededUnsent || p == mqttExchangeWithdrawnUnsent
}

// mqttExchangePhaseEndedPendingDrain reports whether a phase records an ending whose output
// has not drained. The evidence is durable; the release is not yet permitted.
func mqttExchangePhaseEndedPendingDrain(p mqttExchangePhase) bool {
	return p == mqttExchangeTerminalPendingDrain || p == mqttExchangeRejectedPendingDrain
}

// mqttOutboundExchange is the durable obligation.
type mqttOutboundExchange struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	// Nonce is the exchange's identity within its generation, minted at admission and never
	// rewritten. It survives an allocation replacement, which is what lets the identifier
	// move while the obligation does not.
	Nonce string `json:"nonce"`

	// PI is the identifier currently ALLOCATED to this exchange. It is allocation, not
	// identity: two exchanges may hold one identifier at different times and are different
	// obligations.
	PI    uint16            `json:"pi"`
	QoS   byte              `json:"qos"`
	Phase mqttExchangePhase `json:"phase"`

	// AdmitTxn is the transition that admitted this exchange, so an exchange found in
	// recovery can be tied back to the authority that authorized it. An exchange naming no
	// admission is an obligation nothing authorized and is refused.
	AdmitTxn string `json:"admit_txn"`
	// Tenure is the tenure under which it was admitted. It is NOT rewritten by a transfer:
	// ownership of the exchange follows the authority pointer, and this records where it
	// came from.
	Tenure string `json:"tenure"`
}

// mqttOutboundAction is one acquired operation for an exchange.
type mqttOutboundAction struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	Nonce string `json:"nonce"`
	// Acq is this action's acquisition counter within its exchange, so a retransmission is
	// a distinct action rather than an overwrite of the first.
	Acq uint64 `json:"acq"`

	// Kind is the packet this action emits.
	Kind mqttActionKind `json:"kind"`
	// PI is the identifier the emitted packet carries, frozen here: an action must not
	// follow a later allocation replacement, or the bytes on the wire would disagree with
	// what the client was told.
	PI uint16 `json:"pi"`

	// THE ORIGINAL BINDING. Run and Conn name the connection that acquired this action, and
	// Tenure the tenure it was acquired under. None is ever rewritten — not by a transfer,
	// not by a resident reconnect. A completion resolved against "the session's current
	// connection" would follow a rebind and answer for a successor; these fields are what a
	// captured handler resolves against instead.
	Run    string `json:"run"`
	Conn   string `json:"conn"`
	Tenure string `json:"tenure"`

	// AcqTxn is the transition that acquired it.
	AcqTxn string `json:"acq_txn"`
}

// mqttActionKind is the closed set of packets an action may emit.
type mqttActionKind string

const (
	mqttActionPublish mqttActionKind = "publish"
	mqttActionPubRel  mqttActionKind = "pubrel"
)

func mqttValidActionKind(k mqttActionKind) bool {
	return k == mqttActionPublish || k == mqttActionPubRel
}

var mqttExchangeMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "nonce": {}, "pi": {}, "qos": {}, "phase": {},
	"admit_txn": {}, "tenure": {},
}

var mqttActionMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "nonce": {}, "acq": {}, "kind": {}, "pi": {},
	"run": {}, "conn": {}, "tenure": {}, "acq_txn": {},
}

// mqttDecodeOutboundExchange decodes and fully validates one exchange body.
func mqttDecodeOutboundExchange(data []byte, wantID, wantEpoch, wantGen, wantNonce string) (*mqttOutboundExchange, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttExchangeMembers, "an outbound exchange"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttExchangeMembers, "an outbound exchange"); err != nil {
		return nil, err
	}
	e := &mqttOutboundExchange{}
	if err := json.Unmarshal(data, e); err != nil {
		return nil, err
	}
	if err := mqttValidateOutboundExchange(e, wantID, wantEpoch, wantGen, wantNonce); err != nil {
		return nil, err
	}
	return e, nil
}

// mqttDecodeOutboundAction decodes and fully validates one action body.
func mqttDecodeOutboundAction(data []byte, wantID, wantEpoch, wantGen, wantNonce string, wantAcq uint64) (*mqttOutboundAction, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttActionMembers, "an outbound action"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttActionMembers, "an outbound action"); err != nil {
		return nil, err
	}
	a := &mqttOutboundAction{}
	if err := json.Unmarshal(data, a); err != nil {
		return nil, err
	}
	if err := mqttValidateOutboundAction(a, wantID, wantEpoch, wantGen, wantNonce, wantAcq); err != nil {
		return nil, err
	}
	return a, nil
}

// mqttValidateOutboundExchange refuses a body this broker does not support.
func mqttValidateOutboundExchange(e *mqttOutboundExchange, wantID, wantEpoch, wantGen, wantNonce string) error {
	if e == nil {
		return fmt.Errorf("outbound exchange is absent")
	}
	if e.ID != wantID {
		return fmt.Errorf("outbound exchange names client %q, not %q", e.ID, wantID)
	}
	if e.Epoch != wantEpoch {
		return fmt.Errorf("outbound exchange carries recovery epoch %q, not this account's %q", e.Epoch, wantEpoch)
	}
	if e.Gen != wantGen {
		return fmt.Errorf("outbound exchange names generation %q on the subject of %q", e.Gen, wantGen)
	}
	if e.Nonce != wantNonce {
		return fmt.Errorf("outbound exchange names nonce %q on the subject of %q", e.Nonce, wantNonce)
	}
	if !mqttF105IdentTk(e.Nonce) {
		return fmt.Errorf("outbound exchange carries a malformed nonce %q", e.Nonce)
	}
	if e.PI == 0 {
		return fmt.Errorf("outbound exchange carries packet identifier zero")
	}
	if e.QoS != 1 && e.QoS != 2 {
		return fmt.Errorf("outbound exchange carries QoS %d: only 1 and 2 owe an acknowledgement", e.QoS)
	}
	if !mqttValidExchangePhase(e.Phase) {
		return fmt.Errorf("outbound exchange carries an unsupported phase %d", e.Phase)
	}
	// A QoS 1 exchange has no release, so the PUBREL-side phase describes an exchange that
	// cannot exist.
	//
	// The rejection phases are equally QoS 2's: they record a REFUSED PUBREC, and a QoS 1
	// exchange has no PUBREC to refuse. A failed PUBACK ends a QoS 1 exchange at terminal,
	// where nothing about ownership is claimed either way.
	if e.QoS == 1 && (e.Phase == mqttExchangePubRecObserved ||
		e.Phase == mqttExchangeRejectedPendingDrain || e.Phase == mqttExchangeRejected) {
		return fmt.Errorf("a QoS 1 outbound exchange carries phase %d, which belongs to QoS 2", e.Phase)
	}
	if !mqttF105IdentTk(e.AdmitTxn) {
		return fmt.Errorf("outbound exchange carries a malformed admitting transition %q", e.AdmitTxn)
	}
	if !mqttF105IdentTk(e.Tenure) {
		return fmt.Errorf("outbound exchange carries a malformed tenure %q", e.Tenure)
	}
	return nil
}

// mqttValidateOutboundAction refuses a body this broker does not support.
func mqttValidateOutboundAction(a *mqttOutboundAction, wantID, wantEpoch, wantGen, wantNonce string, wantAcq uint64) error {
	if a == nil {
		return fmt.Errorf("outbound action is absent")
	}
	if a.ID != wantID {
		return fmt.Errorf("outbound action names client %q, not %q", a.ID, wantID)
	}
	if a.Epoch != wantEpoch {
		return fmt.Errorf("outbound action carries recovery epoch %q, not this account's %q", a.Epoch, wantEpoch)
	}
	if a.Gen != wantGen {
		return fmt.Errorf("outbound action names generation %q on the subject of %q", a.Gen, wantGen)
	}
	if a.Nonce != wantNonce {
		return fmt.Errorf("outbound action names exchange %q on the subject of %q", a.Nonce, wantNonce)
	}
	if a.Acq != wantAcq {
		return fmt.Errorf("outbound action names acquisition %d on the subject of %d", a.Acq, wantAcq)
	}
	if !mqttValidActionKind(a.Kind) {
		return fmt.Errorf("outbound action carries an unsupported kind %q", a.Kind)
	}
	if a.PI == 0 {
		return fmt.Errorf("outbound action carries packet identifier zero")
	}
	// THE ORIGINAL BINDING IS MANDATORY. An action that cannot name the connection it was
	// acquired on has no completion route that survives a rebind, which is the whole reason
	// the binding is carried rather than looked up.
	if a.Run == _EMPTY_ || a.Conn == _EMPTY_ {
		return fmt.Errorf("outbound action names no originating connection")
	}
	if !mqttF105IdentTk(a.Tenure) {
		return fmt.Errorf("outbound action carries a malformed tenure %q", a.Tenure)
	}
	if !mqttF105IdentTk(a.AcqTxn) {
		return fmt.Errorf("outbound action carries a malformed acquiring transition %q", a.AcqTxn)
	}
	return nil
}

// mqttActionBoundTo reports whether an action was acquired on the connection b names.
//
// This is the check a captured completion handler makes instead of consulting the session's
// current connection: a resident reconnect rebinds that, and a handler resolved against it
// would answer for a successor's state.
func mqttActionBoundTo(a *mqttOutboundAction, b *mqttConnBinding) bool {
	return a != nil && b != nil && a.Run == b.run && a.Conn == b.conn
}

// mqttActionPermittedAtPhase reports whether an exchange standing at this phase may emit this
// packet at all.
//
// The completion rules answer a different question — whether MORE work may be acquired, and
// whether it must wait for a drain — and they leave the choice of packet to the caller. This
// is the choice: a PUBLISH is the delivery itself, so it may only be acquired while the
// exchange is still at its publish; a PUBREL is the release, so it may only be acquired once
// a successful PUBREC has transferred ownership and until it has been discharged.
//
// Stated once, here, because both halves are protocol facts about the phase and a second
// statement of them at an acquisition site is how a retransmission after PUBREC or a release
// with nothing behind it reaches the wire.
func mqttActionPermittedAtPhase(qos byte, phase mqttExchangePhase, kind mqttActionKind) error {
	if !mqttValidExchangePhase(phase) {
		return fmt.Errorf("an exchange at unsupported phase %d emits nothing", phase)
	}
	switch kind {
	case mqttActionPublish:
		if phase != mqttExchangePublish {
			return fmt.Errorf("an exchange at phase %d may not emit another PUBLISH: the "+
				"delivery is answered for", phase)
		}
		return nil
	case mqttActionPubRel:
		if qos != 2 {
			return fmt.Errorf("a QoS %d exchange owes no release", qos)
		}
		if phase != mqttExchangePubRecObserved {
			return fmt.Errorf("an exchange at phase %d owes no release", phase)
		}
		return nil
	}
	return fmt.Errorf("an exchange was asked to emit an unsupported packet %q", kind)
}

// mqttAdmitExchangeTransition assembles the transition that admits one exchange.
//
// The exchange and the authority contend in ONE batch, which is what makes admission a
// boundary rather than a convention: an exchange installed without a winning authority
// transition would be an obligation nothing authorized, and one authorized without its
// exchange landing would be an authority that admitted nothing.
func mqttAdmitExchangeTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	e *mqttOutboundExchange, led *mqttGenLedger, ledSeq uint64) (*mqttAuthorityTransition, error) {
	if prev == nil {
		return nil, fmt.Errorf("admitting an exchange requires the authority it is admitted under")
	}
	if e == nil {
		return nil, fmt.Errorf("admitting an exchange requires the exchange")
	}
	if e.AdmitTxn != txn {
		return nil, fmt.Errorf("the exchange names admitting transition %q, but this transition is %q",
			e.AdmitTxn, txn)
	}
	if e.Gen != prev.Gen || e.Tenure != prev.Tenure {
		return nil, fmt.Errorf("the exchange is admitted under %q/%q but its authority carries %q/%q",
			e.Gen, e.Tenure, prev.Gen, prev.Tenure)
	}
	body, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	inv, ierr := mqttAllocateInventoryMembers(prev.ID, prev.Epoch, e.Gen, led, ledSeq,
		[]mqttDutyDeclaration{{Kind: mqttDutyExchange, Ref: e.Nonce, State: mqttDutyPending}})
	if ierr != nil {
		return nil, fmt.Errorf("admitting an exchange: %w", ierr)
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionAdmitExchange, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityServing, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
		// Create-only: an exchange nonce is minted per admission, so a colliding subject
		// means this admission is a duplicate and must be refused rather than overwrite an
		// obligation that already exists. The duty entry beside it is what makes the
		// obligation DISCOVERABLE — an admission that installed one without the other would be
		// an obligation no recovery pass will ever resume.
		Extra: append([]mqttTransitionMember{{
			subj:   mqttExchangeSubject(getHash(prev.ID), e.Gen, e.Nonce),
			expect: 0,
			body:   body,
		}}, inv...),
	}, nil
}

// mqttAcquireActionTransition assembles the transition that acquires one action.
func mqttAcquireActionTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	a *mqttOutboundAction) (*mqttAuthorityTransition, error) {
	if prev == nil {
		return nil, fmt.Errorf("acquiring an action requires the authority it is acquired under")
	}
	if a == nil {
		return nil, fmt.Errorf("acquiring an action requires the action")
	}
	if a.AcqTxn != txn {
		return nil, fmt.Errorf("the action names acquiring transition %q, but this transition is %q",
			a.AcqTxn, txn)
	}
	if a.Gen != prev.Gen {
		return nil, fmt.Errorf("the action is acquired in generation %q but its authority carries %q",
			a.Gen, prev.Gen)
	}
	body, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionAcquireAction, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityServing, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
		// Create-only on the acquisition counter, so a retransmission is a NEW action rather
		// than an overwrite of the one that may still be executing.
		Extra: []mqttTransitionMember{{
			subj:   mqttActionSubject(getHash(prev.ID), a.Gen, a.Nonce, a.Acq),
			expect: 0,
			body:   body,
		}},
	}, nil
}

// mqttExchangePhaseSucceeds reports whether one phase may follow another for this QoS.
//
// The relation is enumerated rather than derived from the ordering, because "greater than"
// is not the protocol: a QoS 1 exchange has no PUBREC to observe and no release to owe, and
// an ordering check would let a PUBACK be recorded as ownership transfer. The relation is
// also acyclic and strictly forward, which is what makes an exchange unable to leave a
// terminal phase — a resurrected obligation would re-charge Receive Maximum and re-authorize
// output for a message the client already finished with.
func mqttExchangePhaseSucceeds(qos byte, from, to mqttExchangePhase) error {
	if !mqttValidExchangePhase(from) || !mqttValidExchangePhase(to) {
		return fmt.Errorf("an exchange cannot move between unsupported phases %d and %d", from, to)
	}
	if from == to {
		return fmt.Errorf("an exchange already stands at phase %d, so advancing to it records nothing", to)
	}
	switch qos {
	case 1:
		// PUBACK ends a QoS 1 exchange outright. Whether it lands terminal or
		// terminal-pending-drain is the OUTPUT's question, not the protocol's, and both are
		// reachable here for that reason.
		if from == mqttExchangePublish &&
			(to == mqttExchangeTerminal || to == mqttExchangeTerminalPendingDrain) {
			return nil
		}
	case 2:
		switch from {
		case mqttExchangePublish:
			// PUBREC is the ownership transfer. A QoS 2 exchange may NOT go from a publish
			// straight to terminal: there is no acknowledgement that ends it there, and
			// admitting one would discard a release the client is still owed.
			if to == mqttExchangePubRecObserved {
				return nil
			}
			// A REFUSED PUBREC ends it here instead, and only here: the refusal is the
			// answer to the PUBLISH, so an exchange that has already transferred ownership
			// or owes a release cannot be talked back into a rejection. Which of the two
			// rejection phases it lands in is the OUTPUT's question, exactly as for QoS 1.
			if to == mqttExchangeRejected || to == mqttExchangeRejectedPendingDrain {
				return nil
			}
		case mqttExchangePubRecObserved:
			// PUBCOMP ENDS IT, whatever its reason code (mqttPhaseForAck). Terminal when no
			// PUBREL output is outstanding; terminal-pending-drain when a PUBREL action is still
			// live, so the release boundary waits for that action's own drain.
			if to == mqttExchangeTerminal || to == mqttExchangeTerminalPendingDrain {
				return nil
			}
		case mqttExchangeRejectedPendingDrain:
			// The drain never changes WHICH ending was observed, so a refused exchange
			// drains to refused and never to terminal.
			if to == mqttExchangeRejected {
				return nil
			}
		}
	default:
		return fmt.Errorf("an exchange at QoS %d has no phase protocol", qos)
	}
	// The drain completing is the one move common to both, and it is the ONLY way out of
	// either pending-drain phase: they exist precisely because the ending was observed while
	// output could still execute. A drain never changes WHICH ending was observed.
	if from == mqttExchangeTerminalPendingDrain && to == mqttExchangeTerminal {
		return nil
	}
	return fmt.Errorf("a QoS %d exchange may not move from phase %d to phase %d", qos, from, to)
}

// mqttAdvanceExchangeTransition assembles the durable advance of one exchange's phase.
//
// THE PHASE IS A DURABLE FACT, not a field a handler edits in memory. Before this the only
// way an exchange reached PUBREC-observed or terminal was for a caller to write the whole
// record itself, which meant nothing checked the move: a PUBCOMP handler could put a QoS 2
// exchange straight from its publish to terminal and the release the client was owed would
// simply vanish, and no authority would have arbitrated any of it.
//
// It CASes the exchange at the sequence it was read, so two brokers observing the same
// acknowledgement produce one advance, and it contends on the authority in the same batch, so
// the advance is an act of the current owner even when the exchange belongs to an older
// generation the owner has already closed.
func mqttAdvanceExchangeTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	cur *mqttOutboundExchange, curSeq uint64, to mqttExchangePhase) (*mqttAuthorityTransition, error) {

	if prev == nil {
		return nil, fmt.Errorf("advancing an exchange requires the authority it is advanced under")
	}
	if cur == nil {
		return nil, fmt.Errorf("advancing an exchange requires the exchange as it stands")
	}
	// THE READ SEQUENCE IS MANDATORY. Zero would frame the member create-only, which for an
	// exchange that already exists is not an advance at all — it would be refused by the
	// store, but stating it here says why rather than leaving a caller to read a CAS error.
	if curSeq == 0 {
		return nil, fmt.Errorf("advancing exchange %q requires the sequence it was read at", cur.Nonce)
	}
	if verr := mqttValidateOutboundExchange(cur, prev.ID, prev.Epoch, cur.Gen, cur.Nonce); verr != nil {
		return nil, fmt.Errorf("refusing to advance an unsupported exchange: %w", verr)
	}
	if err := mqttExchangePhaseSucceeds(cur.QoS, cur.Phase, to); err != nil {
		return nil, err
	}

	// ONLY THE PHASE MOVES. The advance is built by copying the record as read, so an
	// identifier reallocation, a tenure or an admitting transition cannot ride along with a
	// completion — each of those is a different operation with a different authorization.
	next := *cur
	next.Phase = to
	body, err := json.Marshal(&next)
	if err != nil {
		return nil, err
	}

	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionAdvanceExchange, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		// The authority is reinstalled exactly as it stood, pending proposals included: an
		// advance is not an ownership event, and dropping a reservation or a preparation
		// here would abandon a proposal nobody resolved.
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: prev.State, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
			Intent: prev.Intent, Creation: prev.Creation,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttExchangeSubject(getHash(prev.ID), cur.Gen, cur.Nonce),
			expect: curSeq,
			body:   body,
		}},
	}, nil
}
