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

// Whole-exchange discharge evidence (F-105, rows 2 and 5).
//
// Two of this lane's contracts have the same shape and the same hazard, and both were carried as
// prose until a review required boundaries for them:
//
//   - "an unknown action is an OUTSTANDING one — a caller that cannot answer must say TRUE";
//   - "dischargedFor is connection-local, and so is HALF of the answer".
//
// In both, a LOCAL observation was standing in for a GLOBAL fact. A connection knows which
// acquisitions IT consumed and finished; it knows nothing about acquisitions made on a
// connection it cannot reach, and nothing at all after a restart. Releasing an identifier or
// freeing a charge on that basis frees something another connection may still emit under.
//
// # What replaces it: an enumerated aggregate, where UNKNOWN is not DISCHARGED
//
// The constituents are the exchange's own durable ACTION records. Every acquisition that could
// ever emit has one, written by the transition that acquired it — so the set is enumerable from
// the store rather than from any connection's memory, and it survives the connection and the
// broker.
//
// The rule is then simple and blunt: discharge requires EVERY enumerated action to be
// established finished. An action this broker cannot read, an enumeration it could not complete,
// and an action whose outcome is genuinely unknown all count the same way — NOT DISCHARGED. A
// review put the bound precisely: "unreachable or unknown constituents cannot silently count as
// discharged."
//
// # What this does NOT claim
//
// It is not a liveness check and it does not reach across to other connections. It establishes
// what the DURABLE RECORDS say, which is exactly the thing a local tracker cannot say. The
// connection-local half stays useful for what it always established — an acquisition consumed
// HERE whose execution has returned will emit nothing more — and is now one input rather than
// the answer.

// mqttDischargeState is what could be established about one acquisition.
type mqttDischargeState uint8

const (
	// mqttDischargeFinished: this acquisition cannot emit again. The only state that counts
	// towards release.
	mqttDischargeFinished mqttDischargeState = iota + 1
	// mqttDischargeOutstanding: it may still emit. Includes the case where this broker simply
	// cannot tell, which is deliberate — an unknown action is an outstanding one.
	mqttDischargeOutstanding
)

// mqttActionDischarge is one constituent of the aggregate.
type mqttActionDischarge struct {
	Acq   uint64
	State mqttDischargeState
	// Why records what established the state, so a refusal names the constituent that blocked
	// it rather than reporting that one did.
	Why string
}

// mqttDischargeProof is the aggregate over one exchange.
type mqttDischargeProof struct {
	Gen   string
	Nonce string
	// Constituents is every action this proof enumerated.
	Constituents []mqttActionDischarge
	// Complete is false when the ENUMERATION itself could not be completed. It is kept apart
	// from an outstanding constituent because the two are different failures: one says "this
	// action may still emit", the other says "there may be actions I never saw". Both block
	// release; conflating them would report a specific blocker where there is none.
	Complete bool
}

// Discharged reports whether the whole exchange is established finished.
//
// Requires a COMPLETE enumeration and every constituent finished. An incomplete enumeration is
// never discharged however many constituents it did establish — that is the difference between
// "all of them are done" and "all the ones I saw are done".
func (p mqttDischargeProof) Discharged() bool {
	if !p.Complete {
		return false
	}
	for _, c := range p.Constituents {
		if c.State != mqttDischargeFinished {
			return false
		}
	}
	return true
}

// Blocker names the first constituent preventing discharge, for a refusal message.
func (p mqttDischargeProof) Blocker() string {
	if !p.Complete {
		return "the action enumeration could not be completed, so there may be acquisitions " +
			"this broker never saw"
	}
	for _, c := range p.Constituents {
		if c.State != mqttDischargeFinished {
			return fmt.Sprintf("acquisition %d is outstanding: %s", c.Acq, c.Why)
		}
	}
	return _EMPTY_
}

// mqttLocalDrainOracle answers, for one acquisition, whether THIS process established that it
// finished.
//
// The connection-local half, passed in rather than consulted here. A caller that cannot reach
// the connection an acquisition was made on returns false, which is the honest answer and the
// one that keeps the aggregate conservative.
type mqttLocalDrainOracle func(gen, nonce string, acq uint64) bool

// proveExchangeDischarged enumerates one exchange's actions and reports what could be
// established about each.
//
// The local oracle is the ONLY thing that can establish "finished", and it can only do so for an
// acquisition this process consumed and saw return. Everything else — an action from a previous
// run, an action on a peer's connection, an action whose record will not decode — is
// OUTSTANDING, and the reason is recorded rather than flattened.
func (as *mqttAccountSessionManager) proveExchangeDischarged(id, epoch, gen, nonce string,
	local mqttLocalDrainOracle) (mqttDischargeProof, error) {

	proof := mqttDischargeProof{Gen: gen, Nonce: nonce}
	if local == nil {
		// WITHOUT A LOCAL ORACLE NOTHING CAN BE ESTABLISHED FINISHED, so the proof would be a
		// guaranteed refusal wearing the shape of evidence. Refusing to build it is clearer
		// than returning one that can never succeed.
		return proof, fmt.Errorf("proving discharge requires a drain oracle; without one no " +
			"acquisition can be established finished and the proof would refuse everything")
	}

	if !mqttF105IdentTk(gen) || !mqttF105IdentTk(nonce) {
		// A TOKEN THAT IS NOT ONE narrows or widens the filter below rather than naming an
		// exchange, and a proof built over the wrong subject space answers a question nobody
		// asked. Refusing is the only honest outcome.
		return proof, fmt.Errorf("proving discharge of %q/%q: neither token may be empty or "+
			"carry a subject separator or a wildcard", gen, nonce)
	}

	// THE EXCHANGE ANCHORS THE PROOF. Without this, a nonce nothing ever admitted — or the
	// right nonce under the wrong generation — matches no subject, enumerates cleanly, and
	// comes back DISCHARGED with no constituents at all. An exchange that does not exist
	// authorizes nothing, which acquisition already refuses on; proving one discharged is the
	// same claim read the other way round, and it is the one that releases the identifier.
	//
	// It also fixes the order a release must run in: prove first, delete the exchange record
	// after. A caller that cleaned up first would find nothing and call that proof.
	if _, _, xerr := as.jsa.loadOutboundExchange(id, epoch, gen, nonce); xerr != nil {
		return proof, fmt.Errorf("proving discharge of exchange %q under %q: %w", nonce, gen, xerr)
	}

	idHash := getHash(id)
	// COMPLETENESS-AWARE, because completeness is this function's whole claim. The server caps
	// a stream-info subject list at 100,000 and reports the true total in the envelope, so a
	// caller reading the map alone cannot tell every action from a FIRST PAGE — and a first
	// page whose members all finished would report a discharged exchange with an outstanding
	// acquisition sitting just past the cut.
	subjects, whole, err := as.jsa.streamSubjectsComplete(mqttInflightStreamName,
		mqttActionWalkFilter(idHash, gen, nonce))
	if err != nil {
		// AN UNRESOLVED ENUMERATION IS NOT AN EMPTY ONE, and there is no error it may be read
		// as one for. An absent stream is the tempting exception — nothing was ever acquired,
		// so the answer looks complete — but this stream is erasable and its absence is not a
		// fact this broker may license a release from. Complete therefore has exactly one
		// assignment in this function: after the loop that walked everything.
		return proof, fmt.Errorf("enumerating the actions of exchange %q under %q/%q: %w",
			nonce, id, gen, err)
	}

	for subject := range subjects {
		toks := strings.Split(subject[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 5 {
			continue
		}
		smsg, lerr := as.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
		if lerr != nil {
			if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
				return proof, fmt.Errorf("reading action %q: %w", subject, lerr)
			}
			// Gone between the enumeration and the load. Not this exchange's business either
			// way, and nothing is inferred from its absence.
			continue
		}
		acq, aerr := mqttParseOrdinalToken(toks[4])
		if aerr != nil {
			// A SUBJECT THIS BROKER CANNOT PLACE is not one it may skip: it could name an
			// acquisition of exactly this exchange.
			proof.Constituents = append(proof.Constituents, mqttActionDischarge{
				State: mqttDischargeOutstanding,
				Why:   fmt.Sprintf("the action subject %q cannot be placed: %v", subject, aerr),
			})
			continue
		}
		// The caller's nonce is what the decoder validates the body against.
		a, derr := mqttDecodeOutboundAction(smsg.Data, id, epoch, gen, nonce, acq)
		if derr != nil {
			// UNREADABLE IS OUTSTANDING. A record this broker cannot interpret may be an
			// acquisition of exactly this exchange, and it certainly is not one it can call
			// finished.
			proof.Constituents = append(proof.Constituents, mqttActionDischarge{
				State: mqttDischargeOutstanding,
				Why:   fmt.Sprintf("the action record on %q cannot be interpreted: %v", subject, derr),
			})
			continue
		}
		if local(gen, nonce, a.Acq) {
			proof.Constituents = append(proof.Constituents, mqttActionDischarge{
				Acq: a.Acq, State: mqttDischargeFinished,
				Why: "this process consumed the acquisition and its execution returned",
			})
			continue
		}
		proof.Constituents = append(proof.Constituents, mqttActionDischarge{
			Acq: a.Acq, State: mqttDischargeOutstanding,
			Why: fmt.Sprintf("acquired on %s/%s under tenure %q, which this process cannot "+
				"establish finished", a.Run, a.Conn, a.Tenure),
		})
	}
	// AND THE ENUMERATION'S OWN COMPLETENESS IS THE FLAG, not the fact that the loop finished.
	proof.Complete = whole
	return proof, nil
}

// mqttParseOrdinalToken reads an acquisition token from an action subject.
//
// Separate from mqttParseOrdinal because the two encodings are different: a duty ordinal is
// fixed-width and zero-padded so lexical order IS ordinal order, while an acquisition counter is
// written plainly. Sharing one parser would silently accept either form in both places, which is
// how a subject space stops being a check on anything.
func mqttParseOrdinalToken(tk string) (uint64, error) {
	v, err := strconv.ParseUint(tk, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed acquisition token %q: %w", tk, err)
	}
	if strconv.FormatUint(v, 10) != tk {
		return 0, fmt.Errorf("acquisition token %q does not round trip", tk)
	}
	return v, nil
}
