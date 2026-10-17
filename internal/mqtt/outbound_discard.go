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
	"errors"
	"fmt"
)

// The two acts a DISCARDED generation's retirement takes without an entitlement (F-105 7d).
//
// A teardown closes its generation DISCARDING before it releases the session record, and from that
// moment no connection of the generation exists to decide anything. What the retirement still has
// to do — end the obligations nothing will deliver, and record that the generation's consumers are
// ended — is therefore licensed by the one durable fact that says so: the generation's committed
// discarding closure, which is create-only and immutable. Both acts are checked against it at the
// WRITE BOUNDARY, by the member's own generation (requireDiscardingClosure), never by a caller's
// read; the reads here only compose.
//
//	discard-exchange     an obligation that has not released its holds -> Discarded, its holds
//	                     released in the same batch; an ENDED obligation keeps its own ending
//	generation-discarded the disposal decision on a source or PUBREL consumer of that generation,
//	                     after which the ordinary retirement (authorize, delete, complete) applies

// errMQTTExchangeAlreadyReleased reports that the obligation already released its holds — an
// ending, or a discard that landed first — so there is nothing for a discard to end.
var errMQTTExchangeAlreadyReleased = errors.New("the exchange already released its holds")

// mqttDiscardExchangeTransition composes the discard of one exchange: its phase, and nothing else
// about it, moves to Discarded. The caller appends its hold releases.
func mqttDiscardExchangeTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string,
	cur *mqttOutboundExchange, curSeq uint64) (*mqttAuthorityTransition, error) {

	if prev == nil {
		return nil, fmt.Errorf("discarding an exchange requires the authority it is ended under")
	}
	if cur == nil || curSeq == 0 {
		return nil, fmt.Errorf("discarding an exchange requires the exchange as it stands and its sequence")
	}
	if verr := mqttValidateOutboundExchange(cur, prev.ID, prev.Epoch, cur.Gen, cur.Nonce); verr != nil {
		return nil, fmt.Errorf("refusing to discard an unsupported exchange: %w", verr)
	}
	if mqttExchangeReleasesHolds(cur.Phase) {
		// AN ENDING IS PRESERVED. A completion or refusal that won the race is the evidence the
		// exchange already carries; rewriting it to Discarded would erase a client's answer.
		return nil, fmt.Errorf("exchange %q stands at phase %d: %w", cur.Nonce, cur.Phase, errMQTTExchangeAlreadyReleased)
	}
	next := *cur
	next.Phase = mqttExchangeDiscarded
	body, err := json.Marshal(&next)
	if err != nil {
		return nil, err
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionDiscardExchange, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		// Reinstalled exactly: a discard is not an ownership event, and it runs under the CLOSED
		// predecessor's authority or under a successor's, neither of which it may change.
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

// opDiscardExchange ends one obligation of a discarded generation and releases its holds.
//
// errMQTTExchangeAlreadyReleased is returned, without composing anything, for an exchange that
// already released its holds; the caller treats that as this step's work being done.
func (jsa *mqttJSA) opDiscardExchange(id, epoch, txn, gen, nonce string) (mqttOpOutcome, error) {
	auth, authSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no obligation of its "+
			"may be discarded", id)
	}
	cur, curSeq, lerr := jsa.loadOutboundExchange(id, epoch, gen, nonce)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	if cur == nil {
		return mqttOpOutcome{}, fmt.Errorf("exchange %q of generation %q does not stand", nonce, gen)
	}
	tr, terr := mqttDiscardExchangeTransition(auth, authSeq, txn, "op-discard-exchange", cur, curSeq)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	if herr := jsa.appendHoldReleases(tr, cur, mqttExchangeDiscarded); herr != nil {
		return mqttOpOutcome{}, herr
	}
	return jsa.commitOp(tr)
}

// opDisposeDiscarded records that a source or PUBREL consumer of a discarded generation is ended:
// the `generation-discarded` decision, licensed at the boundary by the closure and carrying no
// tenure. Idempotent on any decision already standing — a route's own, taken while the generation
// still served, is the one carried, and is never re-requested.
func (jsa *mqttJSA) opDisposeDiscarded(id, epoch, txn, gen, inc string) (mqttOpOutcome, error) {
	auth, authSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no resource of its "+
			"may be disposed of", id)
	}
	cur, curSeq, lerr := jsa.loadOutboundResource(id, epoch, gen, inc)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	if cur == nil {
		return mqttOpOutcome{}, fmt.Errorf("disposing of %q/%q: no resource stands", gen, inc)
	}
	if !mqttResourceLogicalUseKind(cur.Kind) {
		return mqttOpOutcome{}, fmt.Errorf("resource %q is kind %q, which is not retired by a disposal", inc, cur.Kind)
	}
	if cur.Disposal != nil {
		return mqttOpOutcome{}, nil
	}
	if cur.State != mqttResourceLive {
		return mqttOpOutcome{}, fmt.Errorf("resource %q is in state %d, so nothing can still end it", inc, cur.State)
	}
	next := *cur
	next.Disposal = &mqttResourceDisposal{Txn: txn, Reason: mqttDisposalGenerationDiscarded, Gen: gen}
	return jsa.commitResourceStep(auth, authSeq, txn, "op-dispose-"+mqttDisposalGenerationDiscarded, &next, curSeq)
}
