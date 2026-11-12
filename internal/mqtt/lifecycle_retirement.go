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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"
)

// Lifecycle retirement (issue #133): a client whose lifecycle is closed, with no generation
// current or outstanding, no pending acceptance and no Will anchor, has every record it ever
// wrote reclaimed, its authority included.
//
//  1. fenced: one batch, the authority's LAST write (Phase retiring) and the registry shard
//     moving the client from active to retiring{L, fenced}. The boundary refuses every later
//     transition of the authority; creation refuses while retiring[h] stands.
//  2. bounded: registry-only, recording the stream's last sequence read after the fence.
//  3. purge the client's filters below bound + 1.
//  4. complete: registry-only, removing retiring[h]. The lifecycle then reads RETIRED.
//
// A crash leaves retiring[h]; the sweep reads every registry shard and completes each entry
// from its phase. A stale reclaimer purges only below its immutable bound, which precedes every
// successor's write, because a successor is admitted only once the entry is gone.

const mqttTransitionRetireLifecycle mqttTransitionKind = "retire-lifecycle"

// mqttLifecyclePurgeFilters are every subject filter of the client of idHash. The authority and
// the lifecycle-scoped outcomes are keyed by the client alone; every generation's records below
// it. Name ownership (ob.own.<stream>.<name>), the registry (ob.lreg.<shard>), the recovery
// directory (ob.root.<shard>.<ord>) and the shared groups are keyed by no idHash token and are
// reached by neither.
func mqttLifecyclePurgeFilters(idHash string) []string {
	ob := mqttOutboundRecordSubjectPrefix() + "*." + idHash
	return []string{ob, ob + ".>", mqttSharedLifecycleStreamSubjectPrefix + mqttSharedLedgerClassTk + "." + idHash + ".>"}
}

// mqttLifecycleRetirable reports whether a lifecycle's authority shows nothing left to retire
// but itself.
func mqttLifecycleRetirable(a *mqttOutboundAuthority) bool {
	return a != nil && a.Life != nil && a.State == mqttAuthorityClosed && a.Life.Phase == mqttLifecycleClosed &&
		a.Life.Pending == nil && len(a.Life.Outstanding) == 0 && len(a.Life.Anchors) == 0
}

// mqttComposeRetireLifecycle composes the fence: the authority to Phase retiring and the
// registry shard moving the client from active to retiring.
func mqttComposeRetireLifecycle(prev *mqttOutboundAuthority, prevSeq uint64, reg mqttRegistryLoad, txn string) (*mqttAuthorityTransition, error) {
	if !mqttLifecycleRetirable(prev) {
		return nil, fmt.Errorf("the lifecycle of %q is not retirable", prev.ID)
	}
	if reg.status != mqttOutboundValid {
		return nil, fmt.Errorf("the registry shard of %q reads %v", prev.ID, reg.status)
	}
	h := getHash(prev.ID)
	if reg.rec.Active[h] != prev.Life.L {
		return nil, fmt.Errorf("the registry shard names lifecycle %d of %q active, not %d", reg.rec.Active[h], prev.ID, prev.Life.L)
	}
	next := mqttRegistryWithRetiring(reg.rec, h, mqttRetiringLifecycle{L: prev.Life.L, Phase: mqttLifecycleFenced})
	body, err := json.Marshal(next)
	if err != nil {
		return nil, err
	}
	nl := *prev.Life
	nl.Phase = mqttLifecycleRetiring
	auth := *prev
	auth.Life, auth.Rev, auth.Txn = &nl, prev.Rev+1, txn
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Scope: mqttLifeScope(prev.Life), Op: "op-" + txn,
			Kind: mqttTransitionRetireLifecycle, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq, Auth: &auth,
		Registry: &mqttTransitionMember{subj: mqttLifecycleRegistrySubject(reg.rec.Shard), expect: reg.seq, body: body},
	}, nil
}

// mqttRegistryWithRetiring is r with the client of h moved out of active and into retiring as e.
func mqttRegistryWithRetiring(r *mqttLifecycleRegistryShard, h string, e mqttRetiringLifecycle) *mqttLifecycleRegistryShard {
	next := *r
	next.Active = maps.Clone(r.Active)
	delete(next.Active, h)
	next.Retiring = maps.Clone(r.Retiring)
	if next.Retiring == nil {
		next.Retiring = map[string]mqttRetiringLifecycle{}
	}
	next.Retiring[h] = e
	return &next
}

// mqttCheckRetireLifecycleRules is the fence's kind rule: from a retirable authority, only the
// lifecycle's phase moves, to retiring, beside a registry member of the client's own shard.
func mqttCheckRetireLifecycleRules(t *mqttAuthorityTransition) error {
	prev, next := t.Prev, t.Auth
	if !mqttLifecycleRetirable(prev) || next == nil || next.Life == nil || next.Life.Phase != mqttLifecycleRetiring {
		return fmt.Errorf("a retire-lifecycle transition must take a retirable lifecycle to retiring")
	}
	if t.Closure != nil || t.Record != nil || len(t.Extra) != 0 {
		return fmt.Errorf("a retire-lifecycle transition writes nothing but the authority and the registry")
	}
	if t.Registry == nil || t.Registry.subj != mqttLifecycleRegistrySubject(mqttLifecycleRegistryShardFor(getHash(t.Attempt.ID))) {
		return fmt.Errorf("a retire-lifecycle transition carries no registry member of its client's shard")
	}
	p, n := *prev, *next
	pl, nl := *prev.Life, *next.Life
	nl.Phase = pl.Phase
	p.Life, n.Life = &pl, &nl
	p.Rev, p.Txn, n.Rev, n.Txn = 0, _EMPTY_, 0, _EMPTY_
	pb, _ := json.Marshal(&p)
	nb, _ := json.Marshal(&n)
	if !bytes.Equal(pb, nb) {
		return fmt.Errorf("a retire-lifecycle transition changes the authority beyond its phase")
	}
	return nil
}

// checkRetireLifecycleRegistry reads the registry shard the fence CASes and requires its member to
// be exactly that body with this client moved from active to retiring{L, fenced}: the registry is
// what every reader classifies the lifecycle by, so its body is checked against the store, not
// trusted from the composer.
func (jsa *mqttJSA) checkRetireLifecycleRegistry(t *mqttAuthorityTransition) error {
	a := t.Attempt
	h := getHash(a.ID)
	reg := jsa.loadLifecycleRegistryShard(a.Epoch, mqttLifecycleRegistryShardFor(h))
	if reg.status != mqttOutboundValid || reg.seq != t.Registry.expect {
		return fmt.Errorf("transition %q retires lifecycle %d over a registry shard that reads %v at %d, not at %d",
			a.Txn, t.Prev.Life.L, reg.status, reg.seq, t.Registry.expect)
	}
	if reg.rec.Active[h] != t.Prev.Life.L {
		return fmt.Errorf("transition %q retires lifecycle %d, which its registry shard does not name active", a.Txn, t.Prev.Life.L)
	}
	want, _ := json.Marshal(mqttRegistryWithRetiring(reg.rec, h, mqttRetiringLifecycle{L: t.Prev.Life.L, Phase: mqttLifecycleFenced}))
	if !bytes.Equal(want, t.Registry.body) {
		return fmt.Errorf("transition %q writes a registry shard other than the retirement of lifecycle %d", a.Txn, t.Prev.Life.L)
	}
	return nil
}

// errMQTTLifecycleNotRetirable: the lifecycle still has work, or its registry cannot be read.
var errMQTTLifecycleNotRetirable = errors.New("the lifecycle is not retirable")

// retireLifecycle fences the lifecycle of client id when it is retirable and then drives its
// registry entry to completion. admit is asked before the authority write.
func (jsa *mqttJSA) retireLifecycle(id, epoch string, admit func() bool) error {
	al := jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid || !mqttLifecycleRetirable(al.rec) {
		return errMQTTLifecycleNotRetirable
	}
	h := getHash(id)
	shard := mqttLifecycleRegistryShardFor(h)
	reg := jsa.loadLifecycleRegistryShard(epoch, shard)
	tr, err := mqttComposeRetireLifecycle(al.rec, al.seq, reg, mqttNewInflightNonce())
	if err != nil {
		return fmt.Errorf("%w: %v", errMQTTLifecycleNotRetirable, err)
	}
	if admit != nil && !admit() {
		return errMQTTLifecycleNotRetirable
	}
	if _, cerr := jsa.commitAuthorityTransition(tr); cerr != nil {
		// AMBIGUOUS OR CONTENDED: the registry says which; the sweep completes a landed fence.
		return cerr
	}
	return jsa.completeLifecycleRetirement(epoch, shard, h)
}

// mqttLifecycleRetireSteps bounds one drive of a registry entry: bound, purge, complete.
const mqttLifecycleRetireSteps = 6

// completeLifecycleRetirement drives the retiring entry of h in its registry shard from its phase
// to removal. Registry-only: nothing of the client reaches its authority again.
func (jsa *mqttJSA) completeLifecycleRetirement(epoch string, shard uint64, h string) error {
	for step := 0; step < mqttLifecycleRetireSteps; step++ {
		reg := jsa.loadLifecycleRegistryShard(epoch, shard)
		if reg.status != mqttOutboundValid {
			return fmt.Errorf("the registry shard %d reads %v", shard, reg.status)
		}
		e, ok := reg.rec.Retiring[h]
		if !ok {
			return nil
		}
		var next *mqttLifecycleRegistryShard
		switch e.Phase {
		case mqttLifecycleFenced:
			// THE BOUND IS READ AFTER THE FENCE COMMITTED: the authority took its last write in it.
			_, last, serr := jsa.streamSubjects(mqttSessStreamName, mqttLifecycleRegistrySubject(shard))
			if serr != nil {
				return serr
			}
			next = mqttRegistryWithRetiring(reg.rec, h, mqttRetiringLifecycle{L: e.L, Phase: mqttLifecycleBounded, Bound: last})
		case mqttLifecycleBounded:
			for _, f := range mqttLifecyclePurgeFilters(h) {
				if perr := jsa.purgeStream(mqttSessStreamName, f, e.Bound+1, false); perr != nil {
					return fmt.Errorf("purging %q: %w", f, perr)
				}
			}
			c := *reg.rec
			c.Retiring = maps.Clone(reg.rec.Retiring)
			delete(c.Retiring, h)
			next = &c
		default:
			return fmt.Errorf("the retiring entry of %q stands at unsupported phase %d", h, e.Phase)
		}
		body, merr := json.Marshal(next)
		if merr != nil {
			return merr
		}
		msg, hdr := mqttFrameBatchWrite(reg.seq, body, _EMPTY_, 0, false)
		// A conflict or an ambiguity is answered by the next read of the phase.
		_, _ = jsa.storeMsg(mqttLifecycleRegistrySubject(shard), hdr, msg)
	}
	return fmt.Errorf("the retirement of %q in shard %d did not converge in %d steps", h, shard, mqttLifecycleRetireSteps)
}

// teardownRecordAbsent reports whether client id's session record positively reads absent.
func (as *mqttAccountSessionManager) teardownRecordAbsent(id string) bool {
	_, err := as.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(as.domainTk, getHash(id)))
	return err != nil && IsNatsErr(err, JSNoMessageFoundErr)
}

// errMQTTSweepNotAdmitted: a lifecycle sweep's authority attempt was refused admission because
// foreground work of the client is announced. Nothing was sent; the next pass continues.
var errMQTTSweepNotAdmitted = errors.New("the lifecycle sweep's write was not admitted: foreground work is announced")

// sweepLifecycles is the ticker's lifecycle pass (accept_abandon.go, issue #133): for every
// authority, it abandons an acceptance that never bound, records a teardown release whose record
// is gone but whose authority half never landed, drives every finished generation to
// retirement, and retires the lifecycle once nothing is left; then it completes every registry
// entry left retiring by a crash. Each authority write is admitted only while the client has no
// announced foreground work; a refused one waits for the next pass.
func (as *mqttAccountSessionManager) sweepLifecycles(s *Server) {
	threshold := secondsToDuration(mqttConnectTimeout(s.getOpts())) + as.pendingAcceptanceMargin()
	now := time.Now()
	epoch := as.sessStreamEpoch
	walkErr := as.walkOutboundAuthorities(func(smsg *StoredMsg) error {
		var claimed struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(smsg.Data, &claimed) != nil || claimed.ID == _EMPTY_ {
			return nil
		}
		a, derr := mqttDecodeOutboundAuthority(smsg.Data, claimed.ID, epoch)
		if derr != nil || a.Life == nil {
			return nil
		}
		id := claimed.ID
		// A REGISTERED ACTOR for the visit to this client, admitted before every authority
		// attempt: an announcement landing mid-visit refuses the next one, and the work waits
		// for the next pass.
		deregister := as.registerForegroundActor(id)
		defer deregister()
		admit := func() bool {
			_, _, idle := as.admitForegroundOrIdle(id)
			return idle == nil
		}
		if a.Life.Pending != nil {
			al := mqttAuthorityLoad{rec: a, seq: smsg.Sequence, status: mqttOutboundValid}
			if err := as.abandonPendingAcceptance(id, al, now, threshold, admit); err != nil && as.jsa.c != nil {
				as.jsa.c.Debugf("MQTT: %v", err)
			}
		}
		as.driveLifecycleRetirement(id, epoch, a, admit, true)
		return nil
	})
	if walkErr != nil && as.jsa.c != nil {
		as.jsa.c.Debugf("MQTT: the lifecycle sweep did not complete: %v", walkErr)
	}
	// RETIREMENTS A CRASH LEFT BEHIND: exact reads of every shard, each entry completed from its
	// phase. A retiring entry has no authority left to walk.
	for shard := uint64(0); shard < mqttLifecycleRegistryShards; shard++ {
		reg := as.jsa.loadLifecycleRegistryShard(epoch, shard)
		if reg.status != mqttOutboundValid {
			continue
		}
		for h := range reg.rec.Retiring {
			if err := as.jsa.completeLifecycleRetirement(epoch, shard, h); err != nil && as.jsa.c != nil {
				as.jsa.c.Debugf("MQTT: completing the lifecycle retirement of %q: %v", h, err)
			}
		}
	}
}

// driveLifecycleRetirement takes client id's lifecycle as far toward retirement as its finished
// work allows, from the authority a as read: a teardown release whose record is gone but whose
// authority half never landed is recorded, every outstanding generation whose work is done is
// retired, and the lifecycle itself once nothing is left. admit is asked before every authority
// attempt. The lifecycle sweep runs it for every authority; a landed release kicks it for its
// client (kickLifecycleRetirement), so a burst of reconnects does not wait for the sweep.
//
// wholeLifecycle is the sweep's alone: fencing a whole lifecycle refuses every CONNECT of the
// client until the registry entry completes, which is right for a client long gone and wrong
// right after an ordinary disconnect, when the prompt drive runs and the client may reconnect at
// once. The prompt drive reclaims generations only.
func (as *mqttAccountSessionManager) driveLifecycleRetirement(id, epoch string, a *mqttOutboundAuthority, admit func() bool, wholeLifecycle bool) {
	for _, o := range a.Life.Outstanding {
		gen, gerr := mqttGenToken(a.Life.L, o.Ord)
		if gerr != nil {
			continue
		}
		if o.Phase == mqttOutstandingOpen && o.Teardown.Pending {
			// A RELEASE THAT DID NOT LAND (plan 5.2): the record was deleted and the
			// authority-only half failed or never ran. The record's absence and the positive
			// entry are the whole licence; a record that still stands is its adopter's.
			if !as.teardownRecordAbsent(id) {
				continue
			}
			if err := as.releaseTeardownEntry(id, gen, o.Teardown.Nonce, admit); err != nil {
				if as.jsa.c != nil {
					as.jsa.c.Debugf("MQTT: recording the release of generation %q of %q: %v", gen, id, err)
				}
				continue
			}
		}
		if as.testHoldGenerationRetirement.Load() {
			continue
		}
		if err := as.jsa.retireGeneration(id, epoch, gen, admit); err != nil &&
			!errors.Is(err, errMQTTGenerationNotRetirable) && as.jsa.c != nil {
			as.jsa.c.Debugf("MQTT: retiring generation %q of %q: %v", gen, id, err)
		}
	}
	if !wholeLifecycle {
		return
	}
	if err := as.jsa.retireLifecycle(id, epoch, admit); err != nil &&
		!errors.Is(err, errMQTTLifecycleNotRetirable) && as.jsa.c != nil {
		as.jsa.c.Debugf("MQTT: retiring the lifecycle of %q: %v", id, err)
	}
}

// kickLifecycleRetirement asks for one drive of client id's lifecycle retirement, off the
// caller's path. It is single-flight per client: a kick arriving while a drive runs asks for
// one more. A release that lands while its generation's recovery visit has already found it
// pending (and the reopen and displacement acceptances, which discharge pending releases in
// their own batch) would otherwise leave every such generation outstanding until the sweep, and
// a client reconnecting faster than that fills its generation slots and is refused.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) kickLifecycleRetirement(id string) {
	if as == nil || id == _EMPTY_ {
		return
	}
	as.mu.Lock()
	if as.lifecycleKicks == nil {
		as.lifecycleKicks = make(map[string]bool)
	}
	if _, running := as.lifecycleKicks[id]; running {
		as.lifecycleKicks[id] = true
		as.mu.Unlock()
		return
	}
	as.lifecycleKicks[id] = false
	as.mu.Unlock()
	go as.runLifecycleKick(id)
}

// runLifecycleKick runs client id's lifecycle drive until no kick is owed. A registered actor
// for each drive, it waits for the client's announced foreground work to clear before every
// authority attempt, so the CONNECT whose acceptance kicked it finishes first.
func (as *mqttAccountSessionManager) runLifecycleKick(id string) {
	for {
		as.runLifecycleKickOnce(id)
		as.mu.Lock()
		if again := as.lifecycleKicks[id]; !again {
			delete(as.lifecycleKicks, id)
			as.mu.Unlock()
			return
		}
		as.lifecycleKicks[id] = false
		as.mu.Unlock()
	}
}

func (as *mqttAccountSessionManager) runLifecycleKickOnce(id string) {
	deregister := as.registerForegroundActor(id)
	defer deregister()
	quit := as.jsa.quitCh
	admit := func() bool { return as.admitForegroundOrWait(id, quit) }
	epoch := as.sessStreamEpoch
	al := as.jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
		return
	}
	as.driveLifecycleRetirement(id, epoch, al.rec, admit, false)
}
