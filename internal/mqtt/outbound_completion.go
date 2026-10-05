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

// Completion of an admitted obligation (F-105).
//
// THE CENTRAL RULE OF THIS FILE: completing an obligation that was already admitted does NOT
// require current serving authority. Beginning new work does.
//
// That split is what makes retirement survivable. If completion needed the serving tenure,
// then transfer and closure would themselves destroy the completion route of every
// obligation admitted before them — which is exactly the defect F-105 names. So a preparing
// authority, a transferred one and a closed one all still permit an admitted exchange to
// finish; what they withhold is permission to admit another.
//
// The second rule is the drain. Terminal evidence observed while output is still outstanding
// moves an exchange to terminal_pending_drain rather than releasing it: an action acquired
// earlier may still be executing, and releasing here would let a late first transmission
// escape after its own release boundary. What moves it on is positive settlement of that
// action, or a closure certificate for the connection it was acquired on — and a certificate
// can only be produced by the process that owns that connection.

// mqttCompletionOp is an operation a caller wants to perform on an admitted obligation.
type mqttCompletionOp uint8

const (
	// mqttOpExecuteAcquired emits the bytes of an action already acquired.
	mqttOpExecuteAcquired mqttCompletionOp = iota + 1
	// mqttOpAcquireMore acquires ANOTHER action for the same exchange — a retransmission,
	// or the PUBREL after a PUBREC.
	mqttOpAcquireMore
	// mqttOpObserveAck records an acknowledgement the client sent.
	mqttOpObserveAck
	// mqttOpMaterializeOwed materializes an owed canonical PUBREL. A durable duty with no
	// socket effect.
	mqttOpMaterializeOwed
	// mqttOpReleaseTerminal releases a terminal exchange and its allocation.
	mqttOpReleaseTerminal
)

// mqttCompletionVerdict is what the rules permit.
type mqttCompletionVerdict uint8

const (
	// mqttCompletionProceed permits the operation now.
	mqttCompletionProceed mqttCompletionVerdict = iota + 1
	// mqttCompletionAwaitDrain permits it only once outstanding output has drained — by
	// positive settlement of the action, or a closure certificate for its connection.
	mqttCompletionAwaitDrain
	// mqttCompletionRefuse forbids it. Refusal is never a disposal: the obligation stays.
	mqttCompletionRefuse
)

// mqttCompletionInput is what the rules decide from.
type mqttCompletionInput struct {
	Op mqttCompletionOp
	// Auth is the STANDING authority, which decides only whether NEW work may begin.
	Auth *mqttOutboundAuthority
	// Exchange is the admitted obligation.
	Exchange *mqttOutboundExchange
	// Outstanding reports whether an acquired action for this exchange may still emit
	// bytes. It is a fact about an action and its connection, not about the authority.
	Outstanding bool
}

// mqttCompletionAllowed applies the rules.
//
// The authority is consulted for exactly one class of operation — acquiring more work — and
// never to decide whether an admitted obligation may finish. A reader tempted to add an
// authority check to the other arms should read this file's header first: that check is the
// defect, not the safeguard.
func mqttCompletionAllowed(in mqttCompletionInput) (mqttCompletionVerdict, error) {
	if in.Exchange == nil {
		return mqttCompletionRefuse, fmt.Errorf("completion asked about no exchange")
	}
	if !mqttValidExchangePhase(in.Exchange.Phase) {
		return mqttCompletionRefuse, fmt.Errorf("exchange carries an unsupported phase %d", in.Exchange.Phase)
	}

	switch in.Op {
	case mqttOpExecuteAcquired:
		// An action already acquired executes on its original connection regardless of who
		// is serving now. This is the permission the whole design turns on: withdrawing it
		// would mean a transfer silently abandoned work the client is already owed.
		return mqttCompletionProceed, nil

	case mqttOpObserveAck:
		// Evidence the client sent is recorded whatever the authority says. Refusing it
		// would discard the only proof that an exchange progressed.
		return mqttCompletionProceed, nil

	case mqttOpMaterializeOwed:
		// A durable duty with no socket effect. Permitted after closure too: it is part of
		// finishing an obligation, not of starting one.
		if in.Exchange.QoS != 2 {
			return mqttCompletionRefuse, fmt.Errorf("only a QoS 2 exchange owes a release")
		}
		switch in.Exchange.Phase {
		case mqttExchangePubRecObserved:
			return mqttCompletionProceed, nil
		default:
			return mqttCompletionRefuse, nil
		}

	case mqttOpAcquireMore:
		// NEW WORK. This is the one arm the authority governs.
		if in.Auth == nil || in.Auth.State != mqttAuthorityServing {
			return mqttCompletionRefuse, nil
		}
		// A successful PUBREC ends this broker's right to send the PUBLISH again: the
		// client has taken the message and what remains is the release.
		if in.Exchange.Phase == mqttExchangePubRecObserved && in.Exchange.QoS == 2 {
			// The release is itself new output, so it waits for the PUBLISH to drain.
			if in.Outstanding {
				return mqttCompletionAwaitDrain, nil
			}
			return mqttCompletionProceed, nil
		}
		// An ENDED exchange acquires nothing further, and a refused one least of all: the
		// client said it will not take this message.
		// Nor does a DISCARDED one (F-105 7d): its generation ended, so nothing will ever owe it
		// output again, though it is not an ending.
		if mqttExchangePhaseEnded(in.Exchange.Phase) ||
			mqttExchangePhaseEndedPendingDrain(in.Exchange.Phase) ||
			in.Exchange.Phase == mqttExchangeDiscarded {
			return mqttCompletionRefuse, nil
		}
		// A retransmission must not race the transmission it repeats.
		if in.Outstanding {
			return mqttCompletionAwaitDrain, nil
		}
		return mqttCompletionProceed, nil

	case mqttOpReleaseTerminal:
		// Terminal evidence releases only once nothing can still emit for this exchange.
		// While output is outstanding the exchange is terminal PENDING DRAIN, which is a
		// different state and deliberately not a released one.
		if !mqttExchangePhaseEnded(in.Exchange.Phase) &&
			!mqttExchangePhaseEndedPendingDrain(in.Exchange.Phase) {
			return mqttCompletionRefuse, nil
		}
		if in.Outstanding {
			return mqttCompletionAwaitDrain, nil
		}
		return mqttCompletionProceed, nil
	}
	return mqttCompletionRefuse, fmt.Errorf("completion asked about an unsupported operation %d", in.Op)
}

// ---------------------------------------------------------------------------
// Discovering what a generation still owes
// ---------------------------------------------------------------------------

// mqttGenerationObligations is one generation's outstanding durable work.
type mqttGenerationObligations struct {
	exchanges []*mqttOutboundExchange
	actions   []*mqttOutboundAction
	// unreadableExchanges counts EXCHANGE records that could not be interpreted, and
	// unreadableOther counts everything else — an action, a malformed subject. They are
	// separate because capacity is charged per OBLIGATION: counting an exchange's
	// undecodable actions as further charges would bill one obligation several times, and
	// the accounting is meant to be conservative, not arbitrary.
	//
	// A caller may not treat a generation as finished while either is non-zero: a record
	// this broker cannot read is not a record that is absent.
	unreadableExchanges int
	unreadableOther     int
}

// enumerateGenerationObligations reads one generation's exchanges and their actions.
//
// Bounded by construction: the filter names one client and one generation, and a generation
// holds at most as many exchanges as it has outstanding obligations. Each body is decoded
// through its own decoder, so a record that does not describe what its subject claims is
// counted as unreadable rather than adopted.
func (as *mqttAccountSessionManager) enumerateGenerationObligations(id, epoch, gen string) (*mqttGenerationObligations, error) {
	idHash := getHash(id)
	out := &mqttGenerationObligations{}

	// COMPLETENESS-AWARE. This census is what a capacity projection counts custody from, and a
	// first page read as the whole answer UNDER-counts — which admits a delivery past Receive
	// Maximum while the client still holds the one this broker never saw.
	subjects, whole, err := as.jsa.streamSubjectsComplete(mqttInflightStreamName,
		mqttExchangeWalkFilter(idHash, gen))
	if err != nil {
		if isErrorOtherThan(err, JSStreamNotFoundErr) {
			return nil, fmt.Errorf("enumerating the outbound exchanges of %q/%q: %w", id, gen, err)
		}
		return out, nil
	}
	if !whole {
		return nil, fmt.Errorf("the outbound exchanges of %q/%q came back as a first page "+
			"rather than the whole answer, so this census would under-count custody", id, gen)
	}
	for subject := range subjects {
		toks := strings.Split(subject[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			out.unreadableExchanges++
			continue
		}
		smsg, lerr := as.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if lerr != nil {
			if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				return nil, fmt.Errorf("loading the outbound exchange on %q: %w", subject, lerr)
			}
			// Gone between the enumeration and the load. Not an obligation this pass owes
			// an answer for, and not an inference either: a later pass re-enumerates.
			continue
		}
		x, derr := mqttDecodeOutboundExchange(smsg.Data, id, epoch, gen, toks[3])
		if derr != nil {
			out.unreadableExchanges++
			continue
		}
		out.exchanges = append(out.exchanges, x)

		// Its actions, by the same rules.
		asubs, awhole, aerr := as.jsa.streamSubjectsComplete(mqttInflightStreamName,
			mqttActionWalkFilter(idHash, gen, toks[3]))
		if aerr != nil {
			if isErrorOtherThan(aerr, JSStreamNotFoundErr) {
				return nil, fmt.Errorf("enumerating the actions of exchange %q: %w", toks[3], aerr)
			}
			continue
		}
		if !awhole {
			return nil, fmt.Errorf("the actions of exchange %q came back as a first page rather "+
				"than the whole answer", toks[3])
		}
		for asubject := range asubs {
			atoks := strings.Split(asubject[len(mqttF105SubjectPrefix()):], ".")
			if len(atoks) != 5 {
				out.unreadableOther++
				continue
			}
			acq, cerr := mqttParseOrdinalToken(atoks[4])
			if cerr != nil {
				out.unreadableOther++
				continue
			}
			amsg, alerr := as.jsa.loadLastMsgFor(mqttInflightStreamName, asubject)
			if alerr != nil {
				if isErrorOtherThan(alerr, JSNoMessageFoundErr) {
					return nil, fmt.Errorf("loading the outbound action on %q: %w", asubject, alerr)
				}
				continue
			}
			act, aderr := mqttDecodeOutboundAction(amsg.Data, id, epoch, gen, toks[3], acq)
			if aderr != nil {
				out.unreadableOther++
				continue
			}
			out.actions = append(out.actions, act)
		}
	}
	return out, nil
}
