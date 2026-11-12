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
	"errors"
	"fmt"
	"time"
)

// Operating on Will anchors (issue #133, plan 5.3): the transitions the broker that holds a Will's
// disposition runs, and the claimant's decision from the anchor.

// mqttWillAnchorRef names one connection's Will anchor: the lifecycle that allocated it and its
// ordinal there. A connection whose acceptance allocated none carries none.
type mqttWillAnchorRef struct {
	L, Ord uint64
}

// willID is the outbox identity of the Will this anchor protects.
func (r mqttWillAnchorRef) willID() string {
	id, err := mqttWillToken(r.L, r.Ord)
	if err != nil {
		return _EMPTY_
	}
	return id
}

// errMQTTAnchorNotAdmitted: a background anchor transition was refused admission because
// foreground work of the client is announced. Nothing was sent; the obligation stays owed.
var errMQTTAnchorNotAdmitted = errors.New("the Will anchor transition was not admitted: foreground work is announced")

// runBackgroundAnchorTransition is runAnchorTransition for a background actor (the removal
// loop, the outbox scan's discharge, shutdown's last removal): registered with the client's
// coordinator for the transition and admitted before every attempt.
func (as *mqttAccountSessionManager) runBackgroundAnchorTransition(clientID string, ref mqttWillAnchorRef, op mqttAnchorOp, fireAt int64) error {
	deregister := as.registerForegroundActor(clientID)
	defer deregister()
	admit := func() bool {
		_, _, idle := as.admitForegroundOrIdle(clientID)
		return idle == nil
	}
	return as.runAnchorTransition(clientID, ref, op, fireAt, admit)
}

// mqttAnchorTransitionAttempts bounds one anchor transition's re-attempts. Each follows a read
// showing the anchor still stands in the state the transition changes.
const mqttAnchorTransitionAttempts = 4

// runAnchorTransition owes or removes one anchor, idempotently: it re-reads the anchor before
// every attempt and ends as soon as the target state stands. A lifecycle that no longer holds the
// anchor ends a removal (it is already gone) and refuses an owe (there is nothing to owe).
//
// admit, when set, is asked before every attempt: each one rewrites the client's authority, so a
// background caller yields to announced foreground work per attempt. Foreground callers (the
// connection's own close) pass nil; background ones go through runBackgroundAnchorTransition.
func (as *mqttAccountSessionManager) runAnchorTransition(clientID string, ref mqttWillAnchorRef, op mqttAnchorOp, fireAt int64, admit func() bool) error {
	as.mu.RLock()
	hook := as.testAnchorTransitionErr
	as.mu.RUnlock()
	if hook != nil {
		if err := hook(clientID, op); err != nil {
			return err
		}
	}
	var last error
	for attempt := 0; attempt < mqttAnchorTransitionAttempts; attempt++ {
		if admit != nil && !admit() {
			return errMQTTAnchorNotAdmitted
		}
		al := as.jsa.loadOutboundAuthority(clientID, as.sessStreamEpoch)
		switch {
		case al.status == mqttOutboundAbsent:
			if op == mqttAnchorRemove {
				return nil
			}
			return fmt.Errorf("the authority of %q is absent, so Will anchor %d cannot be owed", clientID, ref.Ord)
		case al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil:
			last = fmt.Errorf("the authority of %q reads %v", clientID, al.status)
			continue
		}
		life := al.rec.Life
		if life.L != ref.L {
			if op == mqttAnchorRemove {
				return nil
			}
			return fmt.Errorf("lifecycle %d of %q no longer stands, so Will anchor %d cannot be owed", ref.L, clientID, ref.Ord)
		}
		i, ok := mqttAnchorAt(life.Anchors, ref.Ord)
		switch {
		case !ok && op == mqttAnchorRemove:
			return nil
		case !ok:
			return fmt.Errorf("the Will anchor %d of %q is not standing", ref.Ord, clientID)
		case op == mqttAnchorOwe && life.Anchors[i].State == mqttAnchorOwed:
			return nil
		}
		t, cerr := mqttComposeAnchorTransition(al.rec, al.seq, mqttNewInflightNonce(), ref.Ord, op, fireAt)
		if cerr != nil {
			return cerr
		}
		if _, err := as.jsa.commitAuthorityTransition(t); err != nil {
			last = err
			continue
		}
		return nil
	}
	return fmt.Errorf("the Will anchor %d of %q was not updated within %d attempts: %v",
		ref.Ord, clientID, mqttAnchorTransitionAttempts, last)
}

// mqttAnchorVerdict is the claimant's decision from the anchor.
type mqttAnchorVerdict uint8

const (
	// mqttAnchorDefer: the evidence could not be read; act not at all.
	mqttAnchorDefer mqttAnchorVerdict = iota
	// mqttAnchorPublish: no acceptance after the Will's anchor came before its fire time.
	mqttAnchorPublish
	// mqttAnchorCancel: an acceptance came before the fire time, or the anchor was discharged,
	// so this record is not the anchor's to publish.
	mqttAnchorCancel
)

// mqttAnchorReading is what one read of a Will's evidence says about its anchor.
type mqttAnchorReading uint8

const (
	// mqttAnchorUnread: the evidence could not be read, or does not settle the anchor.
	mqttAnchorUnread mqttAnchorReading = iota
	// mqttAnchorStanding: the anchor stands; anchor carries it.
	mqttAnchorStanding
	// mqttAnchorDischarged: the authority carries the anchor's lifecycle, which allocated the
	// anchor and no longer holds it.
	mqttAnchorDischarged
	// mqttAnchorLifecycleRetired: the anchor's lifecycle retired, and every anchor was
	// discharged before it could.
	mqttAnchorLifecycleRetired
)

// readWillAnchor reads the anchor willID names from one read of the client's authority (and
// its registry shard when the authority no longer carries the anchor's lifecycle). The revision
// it returns is the authority's, when the authority was read.
func (as *mqttAccountSessionManager) readWillAnchor(clientID, willID string) (mqttAnchorReading, mqttWillAnchor, uint64) {
	l, ord, ok := mqttParseWillToken(willID)
	if !ok {
		// No anchor names this record. Every Will this broker enqueues carries its anchor's
		// identity, so this is not a record it wrote.
		return mqttAnchorUnread, mqttWillAnchor{}, 0
	}
	as.mu.RLock()
	hook := as.testAnchorEvidenceLoad
	as.mu.RUnlock()
	var al mqttAuthorityLoad
	if hook != nil {
		al = hook(clientID)
	} else {
		al = as.jsa.loadOutboundAuthority(clientID, as.sessStreamEpoch)
	}
	switch {
	case al.status == mqttOutboundValid && al.rec != nil && al.rec.Life != nil && al.rec.Life.L == l:
		life := al.rec.Life
		if i, found := mqttAnchorAt(life.Anchors, ord); found {
			return mqttAnchorStanding, life.Anchors[i], al.rec.Rev
		}
		if ord < life.NextAnchorOrd {
			return mqttAnchorDischarged, mqttWillAnchor{}, al.rec.Rev
		}
		return mqttAnchorUnread, mqttWillAnchor{}, 0
	case al.status == mqttOutboundValid || al.status == mqttOutboundAbsent:
		shard := mqttLifecycleRegistryShardFor(getHash(clientID))
		rl := as.jsa.loadLifecycleRegistryShard(as.sessStreamEpoch, shard)
		if rl.status != mqttOutboundValid && rl.status != mqttOutboundAbsent {
			return mqttAnchorUnread, mqttWillAnchor{}, 0
		}
		if rl.rec.classify(getHash(clientID), l) == mqttLifecycleRetired {
			return mqttAnchorLifecycleRetired, mqttWillAnchor{}, 0
		}
		return mqttAnchorUnread, mqttWillAnchor{}, 0
	default:
		return mqttAnchorUnread, mqttWillAnchor{}, 0
	}
}

// decideFromAnchor is the claimant's decision for one outbox record, at decision time. The
// revision it returns is the authority's, persisted with the decision as the evidence it was
// taken under.
func (as *mqttAccountSessionManager) decideFromAnchor(rec *mqttWillOutbox) (mqttAnchorVerdict, uint64) {
	reading, a, rev := as.readWillAnchor(rec.ClientID, rec.WillID)
	switch reading {
	case mqttAnchorStanding:
		if a.MinAt != 0 && a.MinAt < rec.FireAt {
			return mqttAnchorCancel, rev
		}
		return mqttAnchorPublish, rev
	case mqttAnchorDischarged:
		// A record that stands after its anchor's discharge (a late enqueue, or one
		// re-created after its terminal delete) is never the anchor's to publish.
		return mqttAnchorCancel, rev
	case mqttAnchorLifecycleRetired:
		return mqttAnchorCancel, 0
	default:
		return mqttAnchorDefer, 0
	}
}

// mqttTakeWillAnchorLocked consumes this connection's Will anchor with its Will, returning it
// with the account manager and client ID its transitions run under. c.mu must be held.
func (c *client) mqttTakeWillAnchorLocked() (*mqttWillAnchorRef, *mqttAccountSessionManager, string) {
	if c.mqtt == nil {
		return nil, nil, _EMPTY_
	}
	ref := c.mqtt.willAnchor
	c.mqtt.willAnchor = nil
	return ref, c.mqtt.asm, c.mqtt.cid
}

// mqttAnchorRemoval is one owed anchor removal.
type mqttAnchorRemoval struct {
	clientID string
	ref      mqttWillAnchorRef
}

// The anchor-removal retry loop's backoff bounds.
const (
	mqttAnchorRemovalRetryMin = 250 * time.Millisecond
	mqttAnchorRemovalRetryMax = 30 * time.Second
)

// dropWillAnchor owes the removal of an anchor whose Will will never be published, off the
// caller's path (it may run under c.mu). The removal is RETAINED and retried until it lands
// or the anchor reads absent (runAnchorTransition ends a removal of an anchor its lifecycle
// no longer holds, or a lifecycle the authority no longer carries). Only an orderly shutdown
// drops it, and the anchor then pins its lifecycle (plan 5.3's residual).
func (as *mqttAccountSessionManager) dropWillAnchor(clientID string, ref *mqttWillAnchorRef) {
	if as == nil || ref == nil || clientID == _EMPTY_ {
		return
	}
	as.mu.Lock()
	if as.anchorRemovals == nil {
		as.anchorRemovals = make(map[mqttAnchorRemoval]struct{})
	}
	as.anchorRemovals[mqttAnchorRemoval{clientID: clientID, ref: *ref}] = struct{}{}
	start := !as.anchorRemovalsRunning
	as.anchorRemovalsRunning = true
	as.mu.Unlock()
	if start {
		go as.runAnchorRemovals()
	}
}

// runAnchorRemovals is the one retry loop over the owed removals. It exits when none is
// left, or at shutdown.
func (as *mqttAccountSessionManager) runAnchorRemovals() {
	backoff := mqttAnchorRemovalRetryMin
	for {
		as.mu.Lock()
		if len(as.anchorRemovals) == 0 {
			as.anchorRemovalsRunning = false
			as.mu.Unlock()
			return
		}
		owed := make([]mqttAnchorRemoval, 0, len(as.anchorRemovals))
		for r := range as.anchorRemovals {
			owed = append(owed, r)
		}
		as.mu.Unlock()

		failed := 0
		for _, r := range owed {
			if err := as.runBackgroundAnchorTransition(r.clientID, r.ref, mqttAnchorRemove, 0); err != nil {
				failed++
				if as.jsa.c != nil {
					as.jsa.c.Debugf("MQTT: the Will anchor %d of client %q is not yet removed: %v", r.ref.Ord, r.clientID, err)
				}
				continue
			}
			as.mu.Lock()
			delete(as.anchorRemovals, r)
			as.mu.Unlock()
		}
		if failed == 0 {
			backoff = mqttAnchorRemovalRetryMin
			continue
		}
		select {
		case <-as.jsa.quitCh:
			as.mu.Lock()
			n := len(as.anchorRemovals)
			as.anchorRemovals, as.anchorRemovalsRunning = nil, false
			as.mu.Unlock()
			if n > 0 && as.jsa.c != nil {
				as.jsa.c.Warnf("MQTT: shutting down with %d Will anchor removal(s) not completed; each pins its client's lifecycle until it retires", n)
			}
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, mqttAnchorRemovalRetryMax)
	}
}

// settleWillAnchorAfterEnqueue applies an enqueue outcome's anchor action (mqttAnchorActionFor).
func (as *mqttAccountSessionManager) settleWillAnchorAfterEnqueue(clientID string, ref *mqttWillAnchorRef, outcome mqttWillEnqueueOutcome) {
	if action, _ := mqttAnchorActionFor(outcome); action == mqttWillAnchorDischarge {
		as.dropWillAnchor(clientID, ref)
	}
}

// mqttWillAnchorRefOf is the anchor an outbox identity names, or nil.
func mqttWillAnchorRefOf(willID string) *mqttWillAnchorRef {
	l, ord, ok := mqttParseWillToken(willID)
	if !ok {
		return nil
	}
	return &mqttWillAnchorRef{L: l, Ord: ord}
}

// dischargeWillAnchor removes the anchor of a TERMINAL outbox record, synchronously, and reports
// whether the record may now be deleted. A record no anchor names has nothing to discharge.
func (as *mqttAccountSessionManager) dischargeWillAnchor(rec *mqttWillOutbox) bool {
	l, ord, ok := mqttParseWillToken(rec.WillID)
	if !ok {
		return true
	}
	if err := as.runBackgroundAnchorTransition(rec.ClientID, mqttWillAnchorRef{L: l, Ord: ord}, mqttAnchorRemove, 0); err != nil {
		if as.jsa.c != nil {
			as.jsa.c.Debugf("MQTT: the anchor of terminal Will %q is not yet discharged: %v", rec.WillID, err)
		}
		return false
	}
	return true
}
