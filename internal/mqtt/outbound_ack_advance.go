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
)

// The acknowledgement-driven advance (F-105 row 3): one CLIENT ACKNOWLEDGEMENT moves the
// exchange's phase AND re-phases the durable in-flight slot, in ONE atomic authority batch.
//
// # Two records, one fact, one batch
//
// The exchange record and the register slot both govern whether this obligation may still
// emit or release its identifier, and an acknowledgement is one fact about both: the client
// answered. Splitting the two writes leaves a durable disagreement — a slot past PUBREC beside
// an exchange still at PUBLISH, or the reverse — that a peer or a restart must interpret, which
// is exactly the read-then-act shape F-105 exists to remove. JetStream batches are per stream,
// and both records live on `$MQTT_inflight`, so the two members commit together or not at all.
//
// # It records evidence; it does not admit work or drain an action
//
// The slot member is an ACK-DRIVEN PHASE ARROW, never a replacement that re-mints an
// allocation and never an action drain. It preserves the occupant's identity, delivery,
// lineage and QoS and moves ONLY its state, so it is recognised apart from a same-delivery
// replacement (mqttIsAckPhaseAdvance) exactly as the row-2 output-custody token is
// (mqttIsActionTokenAcquisition). checkAckPhaseAdvanceMembers binds it to the exchange member
// it rides beside; checkRegisterReplacements skips it so the displacement rules never run.
//
// # Standing authority, not serving
//
// An acknowledgement observes output that was already authorised, so this records no new work
// and needs no current entitlement — and a CLOSED generation's obligation must still reach its
// end under whoever serves now (TestF105ClosedGenerationsObligationStillEnds). That is why the
// op contends on the STANDING authority and why the register-member generation may lag the
// transition's own, exactly as the exchange member's may.

// mqttAdvanceExchangeAckRequest names one acknowledgement's advance of an obligation.
type mqttAdvanceExchangeAckRequest struct {
	ID    string
	Epoch string
	Txn   string
	Op    string

	// Gen is the exchange's OWN generation, which may lag the standing authority's: a G1
	// obligation is acknowledged on G1's connection while G2 serves, and only G2's authority
	// can commit the advance.
	Gen string

	// Nonce is the exchange (its per-OBLIGATION identity, equal to the slot's Lin), and To the
	// phase this acknowledgement establishes — DERIVED from the packet by mqttPhaseForAck,
	// never chosen by the caller.
	Nonce string
	To    mqttExchangePhase

	// Acq is the transmission this acknowledgement answers. A PUBACK or a refused PUBREC names
	// ONE transmission, so the terminal arrows are bound to it: terminalising a slot whose Acq
	// has moved on records a client answer over a retransmission the client never made
	// (holistic review round 3, finding 1). A successful PUBREC is deliberately NOT bound to it.
	Acq uint64

	// Register is the exact in-flight slot occupant this advance re-phases, read at RegisterSeq.
	// The slot member CASes it at RegisterSeq, so every predicate validated here against the
	// read record is still true at commit. It is REQUIRED: a pending-drain ending, whose output
	// is still outstanding, carries the exchange alone through opAdvanceExchange, and a retained
	// send (R6) has no durable slot at all.
	Register    *mqttInflightRecord
	RegisterSeq uint64
}

// mqttAckSlotStateForPhase maps the acknowledgement's exchange phase to the slot state that
// records the SAME event, and reports whether that phase re-phases a slot at all.
//
// Only the FINAL endings and the ownership transfer re-phase the slot. A pending-drain ending
// does not: a live action token is the outstanding output that makes the ending pending-drain,
// so the slot cannot terminalise while it stands. Those carry the exchange alone, and the
// drain-completion advance (pending-drain -> ended) re-phases the slot once the output is gone.
func mqttAckSlotStateForPhase(to mqttExchangePhase) (mqttInflightState, bool) {
	switch to {
	case mqttExchangeTerminal, mqttExchangeRejected:
		// A PUBACK, or a refused PUBREC, ended the PUBLISH phase with no outstanding output:
		// the slot goes to publish_terminal, the PUBLISH branch of terminal that names no
		// canonical PUBREL record.
		return mqttInflightPublishTerminal, true
	case mqttExchangePubRecObserved:
		// A successful PUBREC transferred ownership: the slot records the past-PUBREC intent,
		// which any custodian may carry forward to materialise the canonical PUBREL.
		return mqttInflightPubRelIntended, true
	}
	return "", false
}

// mqttIsAckPhaseAdvance reports whether rec is cur with ONLY its state moved along an
// acknowledgement arrow: an unanswered PUBLISH advancing to publish_terminal (a final ending)
// or to pubrel_intended (a successful PUBREC). Every other field is the occupant's, unchanged.
//
// This is the recogniser that keeps an acknowledgement re-phase out of the replacement machine,
// mirroring mqttIsActionTokenAcquisition for the row-2 token. It does not enforce the terminal
// arrow's token and transmission rules — that is checkAckPhaseAdvanceMembers' and the builder's
// job — because a recogniser that also judged legality would be a second definition of it.
func mqttIsAckPhaseAdvance(cur, rec *mqttInflightRecord) bool {
	if cur == nil || rec == nil || cur.State != mqttInflightPublish {
		return false
	}
	if rec.State != mqttInflightPublishTerminal && rec.State != mqttInflightPubRelIntended {
		return false
	}
	want := *cur
	want.State = rec.State
	return want == *rec
}

// mqttAdvanceExchangeAckTransition assembles the atomic exchange-advance-and-slot-re-phase.
func mqttAdvanceExchangeAckTransition(prev *mqttOutboundAuthority, prevSeq uint64,
	r *mqttAdvanceExchangeAckRequest, cur *mqttOutboundExchange, curSeq uint64) (*mqttAuthorityTransition, error) {

	if r == nil {
		return nil, fmt.Errorf("an acknowledgement advance requires its request")
	}
	if cur == nil {
		return nil, fmt.Errorf("an acknowledgement advance of exchange %q requires the exchange as it stands", r.Nonce)
	}
	if r.Register == nil {
		return nil, fmt.Errorf("an acknowledgement advance of exchange %q requires the in-flight slot it re-phases: "+
			"a pending-drain ending carries the exchange alone", r.Nonce)
	}
	if r.RegisterSeq == 0 {
		return nil, fmt.Errorf("re-phasing the slot of exchange %q requires the sequence it was read at", r.Nonce)
	}
	slotTo, ok := mqttAckSlotStateForPhase(r.To)
	if !ok {
		return nil, fmt.Errorf("phase %d is not an acknowledgement that re-phases a PUBLISH slot: only a final "+
			"ending or a successful PUBREC does, while a pending-drain ending carries the exchange alone", r.To)
	}

	reg := r.Register
	// THE SLOT IS THE SAME OBLIGATION, at its unanswered-PUBLISH origin. Lin is the exchange's
	// per-obligation identity and equals its nonce; PI is the identifier both name.
	if reg.State != mqttInflightPublish {
		return nil, fmt.Errorf("acknowledgement advance of exchange %q re-phases a slot standing at %q, not an unanswered PUBLISH",
			cur.Nonce, string(reg.State))
	}
	if reg.Lin != cur.Nonce || reg.PI != cur.PI {
		return nil, fmt.Errorf("acknowledgement advance of exchange %q/identifier %d names slot obligation %q/identifier %d",
			cur.Nonce, cur.PI, reg.Lin, reg.PI)
	}
	// THE TERMINAL ARROW IS TRANSMISSION-BOUND AND REFUSES A LIVE TOKEN. These are the guards
	// casInflightSlot(mqttSlotAdvanceAcknowledgedTerminal) applies at the store, reconstructed
	// here because the authority batch bypasses that path: the exact-sequence CAS then keeps
	// them true at commit. A PUBREC is deliberately neither (see intendPubRelSlot).
	if slotTo == mqttInflightPublishTerminal {
		if reg.Acq != r.Acq {
			return nil, fmt.Errorf("acknowledgement advance of exchange %q answers transmission %d while the slot "+
				"stands at transmission %d: an acknowledgement answers ONE transmission", cur.Nonce, r.Acq, reg.Acq)
		}
		if reg.ActionToken != _EMPTY_ {
			return nil, fmt.Errorf("acknowledgement advance of exchange %q terminalises a slot with a live action "+
				"token: its holder may still be writing the transmission it answers", cur.Nonce)
		}
	}

	// The exchange advance validates the exchange and the phase move; the slot member re-phases
	// the occupant beside it, both on $MQTT_inflight, in one batch.
	tr, err := mqttAdvanceExchangeTransition(prev, prevSeq, r.Txn, r.Op, cur, curSeq, r.To)
	if err != nil {
		return nil, err
	}
	next := *reg
	next.State = slotTo
	if verr := mqttValidateInflightShape(&next); verr != nil {
		return nil, fmt.Errorf("acknowledgement advance of exchange %q builds an unsupported slot shape: %w", cur.Nonce, verr)
	}
	body, merr := json.Marshal(&next)
	if merr != nil {
		return nil, merr
	}
	tr.Extra = append(tr.Extra, mqttTransitionMember{
		subj:   mqttInflightSubject(getHash(prev.ID), cur.Gen, reg.PI),
		expect: r.RegisterSeq,
		body:   body,
	})
	return tr, nil
}

// opAdvanceExchangeAck records one acknowledgement, moving the exchange and re-phasing its slot
// in one atomic batch.
func (jsa *mqttJSA) opAdvanceExchangeAck(r *mqttAdvanceExchangeAckRequest) (mqttOpOutcome, error) {
	if r == nil {
		return mqttOpOutcome{}, fmt.Errorf("an acknowledgement advance requires its request")
	}
	// STANDING AUTHORITY, not serving: an acknowledgement observes work already authorised, and
	// a closed generation's obligation must still reach its end under whoever serves now. Unlike
	// an acquisition, this authorises no new output, so it deliberately does NOT match the
	// current Gen/Tenure.
	auth, authSeq, err := jsa.requireAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no acknowledgement of its "+
			"may be recorded", r.ID)
	}
	cur, curSeq, lerr := jsa.loadOutboundExchange(r.ID, r.Epoch, r.Gen, r.Nonce)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	tr, terr := mqttAdvanceExchangeAckTransition(auth, authSeq, r, cur, curSeq)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
}
