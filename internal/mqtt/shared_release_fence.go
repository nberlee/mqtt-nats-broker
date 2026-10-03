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
	"errors"
	"fmt"
	"sync/atomic"
)

// The shared acquisition release fence (F-105 7e; designed at 7a, consult 82).
//
// A shared-group QoS 1 exchange depends on the EXACT acquisition its admission recorded
// (SharedAcq), because the group consumer's re-offer of the source is how an unacknowledged
// delivery is retransmitted, and on an interest-retained stream that source lives only as long as
// some holder keeps the consumer. An open ledger's release therefore runs, in order:
//
//  1. CLOSE the acquisition on the ledger ($MQTT_shared), so a shared admission's readiness refuses
//     it from now on;
//  2. FENCE the authority ($MQTT_inflight): a CONFIRMED advance of its sequence past the one read
//     after the close, which every admission that read readiness before the close names in its CAS
//     and so loses;
//  3. ENUMERATE the generation's inventory, read after the fence, so every admission that landed
//     before it is below its Hi, and withhold while an exchange naming the acquisition still owes
//     delivery from the source (phase Publish), or while any subject is uncertain;
//  4. only then L1.
//
// Atomic batches are per stream, so steps 1 and 2 cannot be one write, and the order is the
// fence: an admission that reads the authority after step 2 reads the ledger after step 1.
//
// Two shortcuts, each an authorization rather than an observation. A SEALED ledger releases at
// once: a seal is written only by a closing under a won teardown claim or the committed discarding
// closure, so the generation never serves again and no retransmission of its is owed. And when
// the generation's DISCARDING closure stands, no admission or output acquisition of it can land
// (both need a serving authority at the generation) and its recovery owner ends every exchange, so
// steps 2 and 3 are skipped.

// errMQTTSharedReleaseWithheld is an episode that found an exchange still owing delivery from the
// source through this acquisition. Not a failure: the verdict stands and a later episode — the
// owner's re-trigger when a dependant leaves Publish, the verdict sweep, or the closing — releases
// it.
var errMQTTSharedReleaseWithheld = errors.New("the shared acquisition still has a dependant owing delivery from its source")

// mqttTestSharedFenceOutcomeHookV substitutes one fence attempt's commit outcome, so a test can
// present a rejection whose store state it controls. Returns (err, true) to replace the commit,
// (_, false) to let it run. Nil in production.
var mqttTestSharedFenceOutcomeHookV atomic.Pointer[func(id string, attempt int) (error, bool)]

// mqttTestSharedInventoryUnreadableHookV marks an exchange subject unreadable in the fence's
// inventory traversal. Nil in production.
var mqttTestSharedInventoryUnreadableHookV atomic.Pointer[func(subject string) bool]

// mqttFenceSharedReleaseTransition composes the fence: the serving predecessor reinstalled exactly,
// its revision advanced, nothing else.
func mqttFenceSharedReleaseTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn string) *mqttAuthorityTransition {
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: "op-fence-shared-release",
			Kind: mqttTransitionFenceSharedRelease, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: prev.State, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
			Intent: prev.Intent, Creation: prev.Creation,
		},
	}
}

// opFenceSharedRelease commits one fence over the authority prev read at prevSeq, which must be
// SERVING at gen. It reports only whether the batch landed; what a non-landed outcome means is
// decided by re-reading the store, never by the error (a 10164 says another batch is in process,
// not that one committed).
func (jsa *mqttJSA) opFenceSharedRelease(prev *mqttOutboundAuthority, prevSeq uint64, gen string, attempt int) error {
	if prev == nil || prev.State != mqttAuthorityServing || prev.Gen != gen {
		return fmt.Errorf("a shared release fence of generation %q requires its serving authority", gen)
	}
	if h := mqttTestSharedFenceOutcomeHookV.Load(); h != nil {
		if err, override := (*h)(prev.ID, attempt); override {
			return err
		}
	}
	_, err := jsa.commitOp(mqttFenceSharedReleaseTransition(prev, prevSeq, mqttNewInflightNonce()))
	return err
}

// fenceSharedRelease is step 2: called only after a ledger read showed the acquisition closed. It
// returns nil once no admission of gen that read the ledger before the close can land.
//
// The authority is read here, after the close, at sequence S. SERVING at gen: the fence is
// committed against S, and it is FENCED when the batch landed or a re-read shows a stored sequence
// past S — a committed write after the close, which every admission CASing S or earlier loses
// against. Unchanged at S re-attempts. Any other reading needs no write: RESERVING and PREPARING
// admit nothing and leave serving only through a write that moves the sequence, after which an
// admission reads the closed ledger; CLOSED or another generation never serves gen again (a
// generation is never re-proposed); ABSENT admits nothing (there is no missing-authority fallback).
func (jsa *mqttJSA) fenceSharedRelease(id, epoch, gen string) error {
	var lastErr error
	for attempt := 0; attempt < mqttSharedReleaseAttempts; attempt++ {
		al := jsa.loadOutboundAuthority(id, epoch)
		switch al.status {
		case mqttOutboundAbsent:
			return nil
		case mqttOutboundValid:
		default:
			lastErr = fmt.Errorf("the outbound authority of %q reads %v", id, al.status)
			continue
		}
		if al.rec.State != mqttAuthorityServing || al.rec.Gen != gen {
			return nil
		}
		s := al.seq
		cerr := jsa.opFenceSharedRelease(al.rec, s, gen, attempt)
		mqttTestSharedPause("fence:after-commit:" + id)
		if cerr == nil {
			return nil
		}
		lastErr = cerr
		// NOT A FENCE YET. Only a committed advance past S is one.
		if re := jsa.loadOutboundAuthority(id, epoch); re.status == mqttOutboundValid && re.seq > s {
			return nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("the shared release fence of generation %q did not settle within one episode", gen)
	}
	return fmt.Errorf("fencing the authority of %q: %w", id, lastErr)
}

// sharedReleaseDependants is step 3: the inventory, read after the fence, classified for this
// acquisition. An exchange naming it EXACTLY that still owes delivery from the source withholds;
// any uncertain subject withholds as an error, because an absence is not a fact.
//
// "Owes delivery from the source" is phase Publish alone: a pending-drain phase owes an output
// drain, a delivery abandoned on an ACL deny hands its source back without awaiting the consumer,
// and the ended and discarded phases transmit nothing. PubRecObserved cannot carry a shared
// acquisition (supported shared delivery is QoS 1).
func (jsa *mqttJSA) sharedReleaseDependants(id, epoch, gen string, acq mqttSharedAcqRef) error {
	var unreadable func(string) bool
	if h := mqttTestSharedInventoryUnreadableHookV.Load(); h != nil {
		unreadable = *h
	}
	census, err := mqttTraverseGenerationInventory(jsa, nil, id, epoch, gen, unreadable)
	if err != nil {
		return err
	}
	if len(census.uncertain) != 0 {
		return fmt.Errorf("the inventory of %q/%q has %d uncertain subject(s), so the dependants of acquisition %q "+
			"are unknown: %v", id, gen, len(census.uncertain), acq.Acq, census.uncertain)
	}
	for _, x := range census.live {
		if x.SharedAcq == nil || *x.SharedAcq != acq {
			continue
		}
		if x.Phase == mqttExchangePublish {
			return fmt.Errorf("%w: exchange %q (identifier %d) of %q/%q depends on acquisition %q",
				errMQTTSharedReleaseWithheld, x.Nonce, x.PI, id, gen, acq.Acq)
		}
	}
	return nil
}
