// Copyright 2020-2026 The NATS Authors
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

// Rolling back a failed SUBSCRIBE and restoring shared-subscription survivors.

package mqtt

import (
	"fmt"
	"strings"
)

// settleRestoredDowngrades is the CONNECT restore's counterpart to the SUBSCRIBE packet's
// final-update boundary: it converts the QoS 0 downgrades this restore staged into drains,
// makes that durable, and only then runs any teardown that is still owed.
//
// The restore has NO membership to persist — the record already says QoS 0, which is
// exactly why resume reconciliation skipped these filters and why nothing else on this path
// would ever write. So this save exists for one purpose: to move each durable from Cons to
// Drains before anything deletes it. Without it the restore reached processJSConsumer's
// existing-consumer arm and tore the consumer down inline, taking the started exchanges of
// a live durable with it, because $MQTT_msgs is interest-retained. [MQTT-3.1.2-4],
// [MQTT-4.4.0-1].
//
// It runs on the ACCEPT token, after the accepted-CONNECT batch has already committed
// (saveAcceptWithOwnership), so it splits no batch and rolls back no acceptance fact.
//
// A non-nil error refuses the CONNECT: the caller aborts the accepted resume. That is the
// right disposition even though the conversion is the only thing that failed, because a
// restore that cannot establish what it owes the client must not serve.
//
// No lock held on entry.
func (sess *mqttSession) settleRestoredDowngrades(c *client, tok recordTx,
	deferredDels []*mqttDeferredDel, stagedCancels []mqttRegCancellation, facts *mqttSubPacketFacts) error {

	wasUncertain := sess.isSeqUncertain()
	var conv mqttDowngradeConversion
	convert := func() bool {
		conv = sess.convertDowngradeCandidatesLocked(c, deferredDels)
		// And the downgrades that are NOT becoming drains give up their cons entry here,
		// so this record does not name a durable the teardown below deletes. ORed into
		// the return because the restore changes no membership: without it a record whose
		// Subs already said QoS 0 would report "nothing to update" and the reference would
		// leave memory while the record kept naming it.
		released := sess.releaseDowngradedConsumersLocked(deferredDels)
		return conv.any() || released
	}
	_, err := sess.updateWithDetailConverting(tok, nil, true, convert)
	stagedCancels = append(stagedCancels, conv.cancels...)

	if err != nil {
		if sess.classifySave(wasUncertain, err) == saveMaybeLanded {
			// The write MAY hold this conversion. Nothing is committed, torn down or
			// deleted until those exact bytes are proven: a staged drain keeps its
			// durable and is neither retired nor reaped, so whichever record turns out
			// to be durable, no exchange is lost. The resolver owns the settlement, and
			// it may reach a session object that OUTLIVES this refusal — a refused
			// CONNECT does not evict a persistent session — which is why these
			// continuations are load-bearing rather than tidy.
			if conv.any() {
				sess.retainOnLanded(func(recordTx) { sess.finishDowngradeDrains(c, conv) })
			}
			sess.retainOnLanded(func(recordTx) {
				sess.mu.Lock()
				sess.settleAdoptedRetentionsLocked()
				sess.mu.Unlock()
			})
			// A RELEASED destructive downgrade owes its MaxAckPending charge back, and
			// this is the only arm where nothing else gives it: the teardown below is
			// never reached and the sweep that reaps the durable has no session to
			// refund.
			if refunds := mqttReleasedDowngradeDurables(deferredDels); len(refunds) > 0 {
				sess.retainOnLanded(func(recordTx) { sess.refundReleasedDowngrades(refunds) })
			}
			c.Errorf("MQTT: the restore-side drain conversion for %q is unresolved; keeping the state: %v", sess.id, err)
			return err
		}
		// DEFINITELY not stored. Undo exactly what this boundary did, in the live path's
		// order and inside the still-open transaction so no other writer can persist the
		// state being undone.
		var reopen []*subscription
		var unvoid []string
		for _, dd := range deferredDels {
			if dd.closed != nil {
				reopen = append(reopen, dd.closed)
			}
			unvoid = append(unvoid, dd.voided...)
		}
		// Both are ordinarily inert on this path — a cold restore installs no delivery
		// subscription for a QoS 0 membership, so the staging closed nothing — but the
		// UN-VOID is not optional. A descriptor the stream forbade draining had its
		// TRANSMITTED registers voided at staging, and leaving those verdicts standing
		// while restoring the consumer would let a later save persist an authorization to
		// abandon registers whose deliveries are still owed.
		sess.reopenTeardownDelivery(reopen)
		sess.unvoidRegNonces(unvoid)
		sess.mu.Lock()
		sess.unconvertDowngradeDrainsLocked(conv)
		sess.restoreDowngradedConsumersLocked(deferredDels)
		sess.mu.Unlock()
		sess.secureRegCancellations(c, tok, stagedCancels, mqttLicenceOf(c))
		return err
	}

	// CONFIRMED. The record names every converted durable in Drains and names no
	// destroyed one at all, so the teardowns below are safe to run.
	if _, casRejected := sess.secureRegCancellationsClassified(c, tok, stagedCancels, mqttLicenceOf(c)); casRejected {
		facts.recordCASRejected = true
	}
	for _, dd := range deferredDels {
		// A CONVERTED staging has no teardown left: its durable is a drain now, the
		// record names it in Drains, and its started deliveries are still owed.
		if dd.revoked || dd.converted {
			continue
		}
		if sess.execDeferredConsumerDeleteClassified(c, tok, dd.sid, dd.cc, mqttDisposalDowngraded) {
			facts.recordCASRejected = true
		}
	}
	// Committed, but deliberately NOT given delivery here. This path closed no
	// subscription to reopen, and the resume installs source interest later —
	// recoverDrainPubRels, recoverDrains and restoreDrains all run after the accept
	// transaction is released and treat a converted drain exactly as they treat a
	// record-restored one. Reaching for a subscription now would duplicate that.
	sess.finishDowngradeDrains(c, conv)
	sess.mu.Lock()
	sess.settleAdoptedRetentionsLocked()
	sess.mu.Unlock()

	// The cleanup writes above can leave the record UNRESOLVED, not merely rejected, and
	// that has to refuse the resume too.
	//
	// A CAS rejection is reported through facts and handled by the caller. An AMBIGUOUS
	// cleanup write is a different outcome and nothing else on this path catches it: the
	// register cleanup can save (proveVoidedRegs), and a plain session that requests an
	// unchanged lifetime skips every later check — shared ownership arbitration needs
	// restored shared interest, applyRequestedLifetime returns at once when the terms did
	// not move, and the forced leave is 3.1.1-only. The CONNACK would then go out
	// successfully and the restore gate would open over a record this broker cannot speak
	// for.
	//
	// The conversion is NOT undone. It was proven durable before these writes ran, it is a
	// fact about the record rather than about this connection, and the client's exchanges
	// are owed whether or not this CONNECT succeeds. The resolver settles the outstanding
	// attempt on its own.
	//
	// NO DEMONSTRATED PRODUCER, and the tests say so rather than implying coverage. Three
	// fixtures were built for it and each made exactly two session-record writes on this
	// path — the accept and the boundary — because proveVoidedRegs saves only once the
	// void-verdict revision has moved past what the last save carried, and the boundary's
	// own record already carries this packet's verdicts. So this is a helper contract, kept
	// because it is cheap and fails closed: the alternative is a success CONNACK over a
	// record this broker cannot speak for. See qos0_restore_drain_test.go.
	if sess.isSeqUncertain() {
		return fmt.Errorf("the restore-side drain conversion for session %q left the record "+
			"unresolved during its consumer cleanup", sess.id)
	}
	return nil
}

// rollbackSharedSub undoes the early membership persistence done for a shared
// filter whose subscribe subsequently failed. A re-SUBSCRIBE of a filter that
// existed before this packet is restored to its prior QoS/RAP/subID and its
// membership kept (returns run=false, so the caller does NOT release the shared
// consumer); a newly-added filter is removed and the caller must release its
// resources (run=true). For non-shared filters or the CONNECT restore path this
// is a no-op that just asks the caller to run its normal cleanup. If the rollback
// persist fails it returns a non-nil error so the caller escalates to a hard
// processing error rather than leaving a phantom persisted membership. Spec5 [4.8.2].
//
// This is itself a mutate-before-save caller — update() changes the membership maps
// and then persists them — so it owes the same outcome discipline as every other
// one. A DEFINITE failure means the record still holds the previously persisted
// membership, so the compensating mutation must be undone or memory and record
// diverge and a later save writes the wrong one. An AMBIGUOUS failure keeps it: the
// record may already carry the rolled-back state, and the readback recognises this
// session by a digest of exactly those bytes. See sess_uncertain.go.
func (sess *mqttSession) rollbackSharedSub(c *client, tok recordTx, f *mqttFilter, fromSubProto, priorExisted bool, priorM mqttSharedMembership, priorRap bool, priorSubID int) (run bool, casRejected bool, err error) {
	if !fromSubProto || !f.shared {
		return true, casRejected, nil
	}
	// The state as it stands NOW — what this packet attempted and what the record
	// currently holds — so a definite failure can put it back exactly.
	sid := f.sessionKey()
	sess.mu.Lock()
	curM, curExisted := sess.sharedSubs[sid]
	_, curRap := sess.rapSubs[sid]
	curSubID := sess.subIDs[sid]
	sess.mu.Unlock()

	// Sampled BEFORE the write. It matters here more than anywhere: this function
	// runs on a failure path, so the session may ALREADY be unresolved from an
	// earlier ambiguous write in the same packet — in which case update()'s save is
	// refused at entry, which is a definite failure, and the compensating mutation
	// must be undone to leave the earlier attempt's bytes in place for its readback.
	// Without the sample, that refusal would read as "this write may have landed".
	wasUncertain := sess.isSeqUncertain()
	// The no-prior branch below REMOVES this membership, and its group durables are
	// released once that write is known durable — so their registers are voided in the same
	// write, with the delivery barrier down first. The prior-existing branch restores an
	// earlier membership instead: nothing is abandoned there, so nothing is voided.
	var voidedSent []string
	var closed []*subscription
	var sharedCancels []mqttRegCancellation
	if !priorExisted {
		sess.mu.Lock()
		// The doomed key is EXCLUDED from the surviving membership: this runs before the
		// removal below, so sharedSubs still names it, and counting it would make every
		// closure suppress itself. A durable a REMAINING key still reaches keeps its
		// delivery and its registrations — the overlapping level-up case. F-082.
		surv := sess.survivingSharedLocked(map[string]bool{sid: true})
		// The ATTEMPTED membership's targets: the acquisitions this packet minted are what
		// name the incarnations it joined, and the live map still carries them here.
		for _, t := range mqttSharedGroupNamesFor(sid, curM) {
			if surv.ownsDurable(t.durable) {
				continue
			}
			// By DELIVER SUBJECT, not through sess.cons: this filter's own join has no cons
			// entry yet (addJSConsToSess runs only once the whole filter succeeds), so a
			// cons walk left the barrier up for exactly the join being abandoned — and the
			// void snapshot below is then "only a moment in time", which is the window
			// closeTeardownDeliveryLocked exists to close.
			if sub := sess.closeSharedDeliveryLocked(c, t.deliver); sub != nil {
				closed = append(closed, sub)
			}
			cancels, sent := sess.voidRegistrationsLocked(t.durable, mqttLicenceOf(c))
			sharedCancels = append(sharedCancels, cancels...)
			voidedSent = append(voidedSent, sent...)
		}
		sess.mu.Unlock()
	}
	restoreCur := func() {
		sess.restoreSharedKeyInMemory(sid, curExisted, curM, curRap, curSubID)
		// The membership is back, so the durables are not abandoned after all.
		sess.reopenTeardownDelivery(closed)
		sess.unvoidRegNonces(voidedSent)
	}
	undo := func(e error) {
		switch sess.classifySave(wasUncertain, e) {
		case saveDefinitelyFailed:
			restoreCur()
		case saveMaybeLanded:
			// Once the compensating write is PROVEN durable the record no longer names
			// this filter, but the caller has already returned without releasing its
			// resources — so the release is owed here, from the resolver. Nothing else
			// would do it, and the durable plus its MaxAckPending charge would outlive
			// the membership.
			//
			// ONLY for the removal branch. The prior-existing branch writes an ADD that
			// KEEPS the membership, so NO release is owed whatever that write's outcome —
			// registering one there would hand the resolver a continuation that releases a
			// LIVE membership's group consumers, and only the own-hold fence inside it
			// stands between that and a silently dead subscription. Same rule the early
			// per-filter save states for its own ambiguous arm: nothing was deferred for
			// want of proof, so there is no landed continuation to register. F-082.
			if !priorExisted {
				sess.retainOnLanded(func(rtok recordTx) { sess.releaseFailedSharedFilter(c, rtok, f) })
			}
		}
	}

	if priorExisted {
		// The prior MEMBERSHIP is restored whole — its QoS and its acquisition references —
		// so the record goes back to naming exactly the incarnations it named before.
		prior := priorM
		restore := &mqttFilter{shared: true, group: f.group, filter: f.filter, qos: priorM.QoS, retainAsPublished: priorRap, subID: priorSubID, sharedM: &prior}
		if e := sess.update(tok, []*mqttFilter{restore}, true); e != nil {
			undo(e)
			return false, casRejected, e
		}
		// A failed re-SUBSCRIBE must keep delivering with the OLD state: the
		// processSub/processSharedJSConsumer refresh already overwrote the live
		// subscription's QoS/RAP/subID with the attempted values, so restore them.
		// Spec5 [4.8.2].
		sess.restoreSharedLiveMetadata(c, f, priorM, priorRap, priorSubID)
		return false, casRejected, nil
	}
	if e := sess.update(tok, []*mqttFilter{f}, false); e != nil {
		undo(e)
		return true, casRejected, e
	}
	// The removal is durable, and with it the RELEASE VERDICTS for the acquisitions this
	// filter held: execute them now. A filter that failed before it ever materialized has no
	// config for the caller's cleanup to route through, so this is where its acquisitions are
	// released; for one that did, the caller's release reaches the same executor and finds the
	// verdicts already settled.
	sess.executeSharedReleases(c)
	// The removal is durable, so a tombstone can release the cancelled registers now
	// instead of leaving them to the next resume.
	// CLASSIFIED, because this function returns NORMALLY after it: a rejection
	// swallowed here leaves the packet looking quiet while the record has moved.
	if _, cancelCAS := sess.secureRegCancellationsClassified(c, tok, sharedCancels, mqttLicenceOf(c)); cancelCAS {
		casRejected = true
	}
	return true, casRejected, nil
}

// addedCons is one consumer a SUBSCRIBE packet created, paired with its sid.
type addedCons struct {
	sid string
	cc  *ConsumerConfig
}

// mqttSharedSurvivors is the set of shared-subscription session keys this session STILL
// holds once the operation in progress completes.
//
// It answers the one question about a group consumer that is THIS session's alone — does a
// membership that SURVIVES this operation still reach it? — and it decides only LOCAL durable
// state. Whether the consumer itself goes is the group record's arbitrated holder count, moved
// by the release of each acquisition, which is unconditional (F-082, and the shared-group
// lifecycle transfer). Everything a whole-durable release does — the pending purge, the ack-budget
// refund, the deliver-subject unsubscribe, the delivery barrier — belongs to every key that
// reaches the durable, so a durable a SURVIVING key still reaches must not be touched.
//
// Each operation supplies its OWN set, because the surviving membership is not always the
// live map (F-083):
//
//   - an UNSUBSCRIBE's staging half runs BEFORE its removals are applied, so its set is the
//     live map minus the PACKET's whole doomed set — one packet can leave both overlapping
//     filters, and excluding only the key in hand would let each protect the other;
//   - an UNSUBSCRIBE's commit half runs after sess.update applied them, so the live map
//     already IS the surviving membership;
//   - a SUBSCRIBE rollback's authority is the PRE-PACKET snapshot, a historical value: its
//     live map still names the very key being rolled back.
//
// Taking the membership rather than reading sess.sharedSubs behind an exclusion list is what
// keeps that last case honest — an exclusion API would encode the UNSUBSCRIBE's ordering
// instead of the invariant.
//
// A set built under sess.mu is immutable thereafter. sess.mu alone only makes each map
// access race-free; what keeps the sample TRUE after the lock is released is the session's
// OWNERSHIP GATE, which every production route here holds (mqttProcessUnsubs and
// mqttProcessSubs take it for the packet; the landed continuations run under the resolver's
// fence, which holds it). A test driving these paths directly must take the gate itself.
// The truth is LOCAL to this session: it says nothing about another broker, and the
// non-snapshot join/delete race and the same-client-ID skip are unaffected by it.
type mqttSharedSurvivors map[string]mqttSharedMembership

// ownsDurable reports whether any surviving key reaches the given group durable.
func (s mqttSharedSurvivors) ownsDurable(durable string) bool {
	for key, m := range s {
		if mqttSharedOwnsDurable(key, m, durable) {
			return true
		}
	}
	return false
}

// targetsOwed returns the group consumers of keys that no surviving key still reaches — the
// targets this operation owes a whole-durable action on.
//
// Deduplicated by durable: two keys of one packet can name one group consumer, and acting
// twice on it would purge state the first pass already accounted for.
func (s mqttSharedSurvivors) targetsOwed(removed map[string]mqttSharedMembership) []mqttSharedGroupNames {
	var owed []mqttSharedGroupNames
	seen := make(map[string]struct{})
	for key, m := range removed {
		for _, t := range mqttSharedGroupNamesFor(key, m) {
			if _, dup := seen[t.durable]; dup {
				continue
			}
			if s.ownsDurable(t.durable) {
				continue
			}
			seen[t.durable] = struct{}{}
			owed = append(owed, t)
		}
	}
	return owed
}

// survivingSharedLocked snapshots this session's shared membership minus doomed, as the set
// that survives the operation in progress. doomed is nil where the removals have already
// been applied.
//
// sess.mu must be held.
func (sess *mqttSession) survivingSharedLocked(doomed map[string]bool) mqttSharedSurvivors {
	surv := make(mqttSharedSurvivors, len(sess.sharedSubs))
	for key, m := range sess.sharedSubs {
		if doomed[key] {
			continue
		}
		surv[key] = m
	}
	return surv
}

// mqttSharedSurvivorsOf views a membership snapshot as a surviving-key set, for a caller
// whose authority is a historical value rather than the live map.
func mqttSharedSurvivorsOf(membership map[string]mqttSharedMembership) mqttSharedSurvivors {
	surv := make(mqttSharedSurvivors, len(membership))
	for key, m := range membership {
		surv[key] = m
	}
	return surv
}

// releaseFailedSharedFilter releases what a shared filter joined, for a filter whose
// subscribe failed and whose membership REMOVAL is now known to be durable.
//
// It is the cleanup rollbackSharedSub's callers perform when they can see the
// removal persisted; when that write was ambiguous they return without doing it, so
// this is what a landed readback resumes.
//
// TWO different targets with two different rules, because "this session still reaches the
// consumer" and "this acquisition is released" are different facts (F-082, and the
// shared-group lifecycle transfer): every removed ACQUISITION gets a release verdict and
// exactly one execution, unconditionally; the LOCAL durable state is released only when no
// surviving key reaches the consumer:
//
//   - This session STILL HOLDS the durable through another of its own shared keys — the
//     overlapping level-up case, where "$share/g/foo" and "$share/g/foo/#" name one durable.
//     Nothing is owed and nothing may be touched: the purge, the refund and the unsubscribe
//     are all whole-durable, so releasing here would silently stop a live subscription
//     delivering.
//   - Another session holds it, or the scan REFUSED (an indeterminate walk, a record it
//     cannot read). This session's own state is still owed — it is not other members'
//     business — while the group consumer is left alone.
//   - The scan reported unheld: release this session's state AND delete the consumer.
//
// So own-hold is decided here, from sess.sharedSubs, and the scan is consulted only for the
// targets that survive it; for those its answer can no longer be this session's own
// membership. The names come from the filter key rather than from sess.cons, which is not the
// authority for a failed filter — see releaseSharedGroupTarget.
//
// PRECONDITION, and the whole design rests on it: every production path that runs a landed
// continuation holds the session's OWNERSHIP GATE as well as the record fence — the periodic
// pass and CONNECT take lockSession before resolveUncertainSession, resolveUncertainNow takes
// it for a resident session, and the live SUBSCRIBE tail keeps the packet's. That is what
// makes the own-hold sample still true after sess.mu is released; sess.mu alone would only
// make each map access race-free. A TEST calling resolveUncertainWrite directly does not
// inherit that guarantee and must take the gate itself.
//
// No lock held on entry.
func (sess *mqttSession) releaseFailedSharedFilter(c *client, tok recordTx, f *mqttFilter) {
	sid := f.sessionKey()
	// Own-hold first, so a durable this session still reaches through another key is never
	// scanned for and never touched. The removal is already durable on this route, so the
	// live map IS the surviving membership and nothing needs excluding.
	if f.sharedM == nil {
		return
	}
	sess.mu.Lock()
	owed := sess.survivingSharedLocked(nil).targetsOwed(map[string]mqttSharedMembership{sid: *f.sharedM})
	sess.mu.Unlock()
	if len(owed) == 0 {
		return
	}
	for _, t := range owed {
		// EVERY matching entry, not the first: a durable can be named by more than one
		// stale cons entry, and stopping at one would leave the other as a strand with no
		// membership to reach it by.
		//
		// The SUBSCRIBE rollback's definite arm is no longer one of the shapes that
		// produces such an entry — it restores the shared membership beside cons (F-039) —
		// but the sweep stays, because that closes one producer and does not establish
		// that no stale entry can exist.
		sess.mu.Lock()
		for csid, cfg := range sess.cons {
			if cfg != nil && cfg.Durable == t.durable {
				delete(sess.cons, csid)
			}
		}
		sess.mu.Unlock()
		// The consumer's fate is the group record's arbitrated count, executed from the
		// verdicts the compensating write made durable — never a scan's answer.
		sess.releaseSharedGroupTarget(c, tok, t.durable, t.deliver)
	}
}

// restoreSharedLiveMetadata resets the live QoS/RAP/subID on a shared filter's
// raw-subject subscription, its level-up subscription (if any), and its group
// deliver subscription to the given prior values, after a failed re-SUBSCRIBE
// refreshed them to the attempted values. Spec5 [4.8.2].
func (sess *mqttSession) restoreSharedLiveMetadata(c *client, f *mqttFilter, m mqttSharedMembership, rap bool, subID int) {
	sid := f.sessionKey()
	qos := m.QoS
	// The deliver subjects are the RESTORED membership's — the incarnations its acquisition
	// references name — never the attempted acquisition's and never whatever incarnation the
	// group happens to serve now.
	_, deliver, _ := mqttSharedNames(f.group, f.filter, m.Main.Inc)
	setFields := func(csid string) {
		c.mu.Lock()
		sub := c.subs[csid]
		c.mu.Unlock()
		if sub == nil {
			return
		}
		sess.mu.Lock()
		sess.subsMu.Lock()
		if sub.mqtt != nil {
			sub.mqtt.qos = qos
			sub.mqtt.retainAsPublished = rap
			sub.mqtt.subID = subID
		}
		sess.subsMu.Unlock()
		sess.mu.Unlock()
	}
	setFields(sid)     // raw-subject sub
	setFields(deliver) // group deliver sub
	if mqttNeedSubForLevelUp(f.filter) && m.Up != nil {
		setFields(sid[:len(sid)-2] + mqttMultiLevelSidSuffix)
		_, ldeliver, _ := mqttSharedNames(f.group, f.filter[:len(f.filter)-2], m.Up.Inc)
		setFields(ldeliver)
	}
}

// mqttSubSnapshot captures the pre-SUBSCRIBE state of the session's subscription
// maps so a failed packet can be reverted exactly. Spec5 [4.8.2].
type mqttSubSnapshot struct {
	subs    map[string]byte
	shared  map[string]mqttSharedMembership
	rap     map[string]struct{}
	noLocal map[string]struct{}
	subIDs  map[string]int
	// drains and drainByDur, because a re-SUBSCRIBE ADOPTS a draining consumer:
	// it removes the drain and moves the durable into cons. The consumer snapshot
	// alone cannot undo that — restoring cons without the drain leaves a session
	// holding neither, while the record still carries the drain, and the next save
	// then erases a durable that is still owed started deliveries.
	drains     map[string]*mqttDrain
	drainByDur map[string]*mqttDrain
	// adoptedDrains is the retention state, snapshotted for the same reason the drain maps
	// are: a rollback must put back what the packet was holding open, or a durable this
	// packet adopted comes back with no retention naming it and the next record names it
	// nowhere. Retention lifecycle step 1.
	adoptedDrains map[string]*mqttDrain
}

// snapshotSubs shallow-copies every subscription map under the session lock.
func (sess *mqttSession) snapshotSubs() *mqttSubSnapshot {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	s := &mqttSubSnapshot{
		subs:    make(map[string]byte, len(sess.subs)),
		shared:  make(map[string]mqttSharedMembership, len(sess.sharedSubs)),
		rap:     make(map[string]struct{}, len(sess.rapSubs)),
		noLocal: make(map[string]struct{}, len(sess.noLocalSubs)),
		subIDs:  make(map[string]int, len(sess.subIDs)),
	}
	for k, v := range sess.subs {
		s.subs[k] = v
	}
	for k, v := range sess.sharedSubs {
		s.shared[k] = v
	}
	for k := range sess.rapSubs {
		s.rap[k] = struct{}{}
	}
	for k := range sess.noLocalSubs {
		s.noLocal[k] = struct{}{}
	}
	for k, v := range sess.subIDs {
		s.subIDs[k] = v
	}
	if len(sess.drains) > 0 {
		s.drains = make(map[string]*mqttDrain, len(sess.drains))
		for k, v := range sess.drains {
			s.drains[k] = v
		}
	}
	if len(sess.drainByDur) > 0 {
		s.drainByDur = make(map[string]*mqttDrain, len(sess.drainByDur))
		for k, v := range sess.drainByDur {
			s.drainByDur[k] = v
		}
	}
	if len(sess.adoptedDrains) > 0 {
		s.adoptedDrains = make(map[string]*mqttDrain, len(sess.adoptedDrains))
		for k, v := range sess.adoptedDrains {
			s.adoptedDrains[k] = v
		}
	}
	return s
}

// retainRepairedDrain records a drain whose delivery subscription THIS PACKET
// installed, so a rollback restores the drain WITH that subscription rather than
// without it.
//
// The reclaim is not the packet's change to revert: the drain was entitled to a
// delivery subscription and its resume failed to install one. Where the pre-packet
// drainByDur lacks the entry — a restore whose processQOS12Sub failed rolls it back —
// restoring that map alone would leave a live subscription over a durable the reverse
// index no longer names, so a drained source would be delivered as an ordinary PUBLISH
// for the rest of the connection. That absence is not what DEFINES a repair, though:
// the blanket QoS 2 withholding leaves the entry in place while installing nothing, so
// the reclaim keys on the subscription and this recording is simply idempotent there.
//
// It is also what lets rollbackSubscribePacket stay unchanged: its adopted-drain
// exclusion asks whether the durable was a drain's before the packet, and after the
// reclaim the honest answer is yes.
func (s *mqttSubSnapshot) retainRepairedDrain(d *mqttDrain) {
	if s == nil || d == nil || d.cc == nil {
		return
	}
	if s.drains == nil {
		s.drains = make(map[string]*mqttDrain, 1)
	}
	if s.drainByDur == nil {
		s.drainByDur = make(map[string]*mqttDrain, 1)
	}
	s.drainByDur[d.cc.Durable] = d
}

// restoreSharedKeyInMemory reverts the in-memory shared-subscription maps for one
// session key to the given prior state, without persisting. Used when an early
// membership persist failed (update mutates the maps before it saves). Spec5 [4.8.2].
func (sess *mqttSession) restoreSharedKeyInMemory(sid string, existed bool, m mqttSharedMembership, rap bool, subID int) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if existed {
		if sess.sharedSubs == nil {
			sess.sharedSubs = make(map[string]mqttSharedMembership)
		}
		sess.sharedSubs[sid] = m
	} else {
		delete(sess.sharedSubs, sid)
	}
	sess.rapSubs, _ = mqttUpdateFilterOptSet(sess.rapSubs, sid, existed && rap)
	sess.subIDs, _ = mqttUpdateFilterSubID(sess.subIDs, sid, mqttZeroIf(!existed, subID))
}

// mqttAttemptedShared is one SUBSCRIBE packet's shared membership as it stood
// immediately BEFORE the packet's compensation, with the option entries belonging to
// those keys.
//
// rapSubs and subIDs are keyed by session key and carry PLAIN filters too, whose state
// the pre-packet snapshot owns — so only the keys the membership names are carried,
// never the whole map. Copying a whole option map here would let a plain filter's
// identifier survive a rollback that removed its subscription. F-039.
type mqttAttemptedShared struct {
	members map[string]mqttSharedMembership
	rap     map[string]struct{}
	subIDs  map[string]int
}

// mqttCaptureAttemptedSharedLocked samples the shared state as it stands NOW — the same
// sample rollbackSharedSub takes for one filter, taken here for the packet.
//
// The pre-packet snapshot is NOT a substitute: the packet's eager per-filter writes may
// already have made a DIFFERENT state durable, because a shared membership is persisted
// before its group consumer exists and the final save's failure did not undo that. This
// is NOT a claim that every key here was written by this packet, nor that the record
// currently agrees — a CAS-rejected eager write leaves this session continuing against a
// record another broker moved, which is facts.recordCASRejected's question and not this
// one's.
//
// The maps are COPIED. snap.shared is assigned by reference into sess.sharedSubs by the
// restoration below, so aliasing the live map would leave this capture pointing at
// whichever map that assignment detaches.
//
// sess.mu must be held.
func mqttCaptureAttemptedSharedLocked(sess *mqttSession) *mqttAttemptedShared {
	if len(sess.sharedSubs) == 0 {
		return nil
	}
	a := &mqttAttemptedShared{
		members: make(map[string]mqttSharedMembership, len(sess.sharedSubs)),
		rap:     make(map[string]struct{}),
		subIDs:  make(map[string]int),
	}
	for key, m := range sess.sharedSubs {
		a.members[key] = m
		if _, ok := sess.rapSubs[key]; ok {
			a.rap[key] = struct{}{}
		}
		if id, ok := sess.subIDs[key]; ok {
			a.subIDs[key] = id
		}
	}
	return a
}

// restoreAttemptedShared puts back the shared membership captured before the packet's
// compensation, on the DEFINITE-failure arm only.
//
// Without this the compensation's own failure discards shared state nothing undid, and
// the next save from this session — the disconnect stamp, or clear()'s teardown scan —
// writes a record that has dropped a holder whose group durable is still live. No sweep
// reclaims that durable: the only route that deleted a consumer from a candidate listing
// on $MQTT_msgs was removed, and $MQTT_msgs is interest-retained, so the orphan retains
// every matching publish until another session joins that exact $share group.
//
// Each key is restored through the same two helpers restoreSharedKeyInMemory uses, which
// CLEAR as well as set — so a restore to "no RAP" or "identifier 0" expresses itself
// rather than only an addition. F-039.
//
// No lock held on entry.
func (sess *mqttSession) restoreAttemptedShared(a *mqttAttemptedShared) {
	if a == nil {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for key, m := range a.members {
		if sess.sharedSubs == nil {
			sess.sharedSubs = make(map[string]mqttSharedMembership)
		}
		sess.sharedSubs[key] = m
		_, rap := a.rap[key]
		sess.rapSubs, _ = mqttUpdateFilterOptSet(sess.rapSubs, key, rap)
		sess.subIDs, _ = mqttUpdateFilterSubID(sess.subIDs, key, a.subIDs[key])
	}
}

// mqttRestoreDrainMap copies one snapshot drain map, omitting terminally retired objects.
// nil in, nil out, and an all-retired map collapses to nil so the session's own
// "empty means nil" convention is preserved.
//
// sess.mu must be held by the caller, which is what makes reading d.retired safe.
func mqttRestoreDrainMap(snap map[string]*mqttDrain) map[string]*mqttDrain {
	if len(snap) == 0 {
		return nil
	}
	out := make(map[string]*mqttDrain, len(snap))
	for k, d := range snap {
		if d == nil || d.retired {
			continue
		}
		out[k] = d
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// mqttZeroIf returns 0 when cond is true, else v. Helper for map-key clears.
func mqttZeroIf(cond bool, v int) int {
	if cond {
		return 0
	}
	return v
}

// rollbackSubscribePacket undoes every runtime and persisted change made by a
// SUBSCRIBE whose final session-record save failed. It restores the pre-packet
// subscription maps and re-persists them; only if that restore save is confirmed
// does it tear down the subscriptions and consumers that were NEWLY created in
// this packet — so a group durable is never deleted while the persisted record
// might still claim membership. A re-SUBSCRIBE of a pre-existing filter keeps its
// membership and runtime state. The connection is closing regardless. Spec5 [4.8.2].
func (sess *mqttSession) rollbackSubscribePacket(c *client, tok recordTx, subs []*subscription, consSnap map[string]*ConsumerConfig, snap *mqttSubSnapshot, q2AffPrePacket bool) {
	// Restore EVERYTHING the packet mutated — the membership maps AND sess.cons —
	// before the confirm-save, so the record is written from a fully rolled-back
	// view: a QoS0->QoS1 upgrade's new consumer must not be persisted under the
	// restored QoS0 subs key, and a QoS1->QoS0 downgrade's restored subscription
	// must be persisted WITH its pre-existing consumer config (its durable is
	// intact because downgrade deletes are deferred until a confirmed save, which
	// did not happen; a converted one has been un-converted just above, so it is here
	// to be classified rather than missing from this map). The pre-packet consumer snapshot is authoritative for these
	// QoS transitions, which the membership maps alone cannot express. Spec5 [4.8.2].
	var added []addedCons
	// The durables this packet ADOPTED, whose drains the restoration below reactivates.
	var reactivated []string
	sess.mu.Lock()
	for sid, cc := range sess.cons {
		if _, ok := consSnap[sid]; ok {
			continue
		}
		if cc != nil && snap.drainByDur[cc.Durable] != nil {
			// ADOPTED, not created. Its durable predates this packet and is being
			// handed back to the drain below, so it must not be counted among the
			// packet's own consumers — those get torn down once the rolled-back
			// record is confirmed, which here would delete a durable that still owes
			// started deliveries, taking them with it.
			//
			// Collected, because the drain about to be handed this durable back is the
			// PRE-ADOPTION object and claims nothing started since. See
			// reclassifyReactivatedDrainsLocked.
			reactivated = append(reactivated, cc.Durable)
			continue
		}
		added = append(added, addedCons{sid, cc})
	}
	// The consumers this packet created are about to be released, and the restored record is
	// written just below — so their registers are voided HERE, in that same write. Afterwards
	// the membership is the pre-packet one, which never named these durables, so nothing
	// would be left to explain a register that outlives them. The delivery barrier goes down
	// first, for the reason given in closeTeardownDeliveryLocked: a callback still live
	// during this snapshot would allocate behind it.
	var rollbackCancels []mqttRegCancellation
	var rollbackClosed []*subscription
	var rollbackVoided []string
	// What survives THIS ROLLBACK is the PRE-PACKET membership, not sess.sharedSubs — which
	// is still the attempted one here and names the very keys being rolled back. A shared
	// consumer this packet created can be the level-up companion of a filter the session
	// already held ("$share/g/foo.>" joins the durable of "$share/g/foo"), and everything
	// below is whole-durable, so acting on it would close a SURVIVING filter's delivery and
	// withdraw its registrations. F-083.
	rollbackSurv := mqttSharedSurvivorsOf(snap.shared)
	// The shared membership as it stands NOW, before the reset below replaces it with the
	// pre-packet snapshot. The definite-failure arm puts it back; nothing else in this
	// function reads it. Taken AFTER rollbackSurv deliberately: that set is the PRE-PACKET
	// membership and must stay so. See mqttCaptureAttemptedSharedLocked. F-039.
	attemptedShared := mqttCaptureAttemptedSharedLocked(sess)
	// THE RELEASE VERDICTS ride this write. The record about to be stored names the
	// pre-packet membership, so every acquisition the attempted membership holds outside it
	// is abandoned by that write and owed a release — recorded here, executed once the write
	// is confirmed, withdrawn again if it definitely failed. Without this the acquisitions a
	// rolled-back filter committed through its eager save would stay listed for ever, the
	// group's holder count one too high, and its incarnation never retired.
	rollbackVerdicts := sess.addAcqReleaseVerdictsForDroppedLocked(snap.shared)
	for _, a := range added {
		if a.cc == nil {
			continue
		}
		if a.cc.DeliverGroup != _EMPTY_ && rollbackSurv.ownsDurable(a.cc.Durable) {
			continue
		}
		if sub := sess.closeTeardownDeliveryLocked(c, a.cc); sub != nil {
			rollbackClosed = append(rollbackClosed, sub)
		}
		cancels, sent := sess.voidRegistrationsLocked(a.cc.Durable, mqttLicenceOf(c))
		rollbackCancels = append(rollbackCancels, cancels...)
		rollbackVoided = append(rollbackVoided, sent...)
	}
	sess.subs = snap.subs
	sess.sharedSubs = snap.shared
	sess.rapSubs = snap.rap
	sess.noLocalSubs = snap.noLocal
	sess.subIDs = snap.subIDs
	sess.cons = consSnap
	// A re-SUBSCRIBE that ADOPTED a drain moved its durable out of drains and into
	// cons. Restoring cons alone would leave the session holding neither, while the
	// record still carries the drain — and the next save would then erase a durable
	// that still owes started deliveries. Restore both maps together.
	//
	// PER ENTRY, skipping any drain that has since been RETIRED. The snapshot is shallow,
	// so it holds the same *mqttDrain pointers retirement acts on and a flag set after the
	// snapshot was taken is visible through it — which is exactly what a post-hoc ownership
	// check could not give us, since the resurrection arrives through a snapshot captured
	// EARLIER. Without this, a drain retired mid-packet comes back with its sid held for the
	// life of the session (st.intent is never cleared) and no reaper able to release it,
	// because the durable is gone.
	//
	// Each map contributes its OWN entries and neither is derived from the other. They
	// legitimately disagree: when restoreDrains' delivery subscription fails it deletes the
	// drainByDur entry while sess.drains[sid] survives, and that absence is a deliberate
	// rollback rather than a gap to fill — deriving the reverse index from every forward
	// entry would re-assert a registration that was explicitly removed. Conversely
	// retainRepairedDrain ADDS a reverse entry for a drain this packet repaired, and copying
	// drainByDur's own entries is what preserves it. Retirement removes both entries for one
	// object under a single sess.mu hold, so filtering per map cannot split an object across
	// the two.
	sess.drains, sess.drainByDur = mqttRestoreDrainMap(snap.drains), mqttRestoreDrainMap(snap.drainByDur)
	// The retention goes back with them. A rollback restores the pre-packet view, and a
	// durable this packet adopted is being handed back to its drain — so a retention the
	// packet created is no longer owed, while one it inherited still is. Restoring the
	// snapshot answers both without a per-entry rule.
	sess.adoptedDrains = mqttRestoreDrainMap(snap.adoptedDrains)
	// The reactivated drains are LIVE again from here, and the object the snapshot holds is
	// the pre-adoption one: it claims nothing started since the adoption. Classifying those
	// deliveries now, under this same lock, is what stops the reactivated drain acking away
	// a source it owes — and the cancel half is not optional, because a callback parked in
	// storeInflightRegister would otherwise confirm and transmit a sequence this drain does
	// not claim (confirmInflightRegister checks the pending entry's identity, never drain
	// membership).
	rollbackWithdrawn := sess.reclassifyReactivatedDrainsLocked(reactivated, mqttLicenceOf(c))
	sess.mu.Unlock()
	// Detach a session-affine QoS 2 path this packet provisioned, BEFORE the
	// confirm-save: the restored record must not name a consumer that is about to
	// be deleted. The deletion itself waits for that save to be confirmed, exactly
	// as the packet's other consumers do. A path that predates the packet is left
	// alone. Spec5 [4.8.2].
	var q2AffUnwind mqttQ2AffUnwind
	if !q2AffPrePacket {
		// disposeNow: unchanged here too; see the note at the unused-path unwind.
		q2AffUnwind = sess.prepareQ2AffineUnwind(c, true)
	}
	outcome, err := sess.saveClassified(tok)
	if err != nil {
		// Could not confirm the rolled-back record. Do not release/delete anything
		// (a durable could be orphaned while the record still claims membership);
		// put the packet's consumer entries back so the in-memory view matches the
		// still-live resources, and let a reconnect reconcile from the record.
		//
		// An AMBIGUOUS write is different: the rolled-back record may be exactly
		// what is stored. Re-adding the packet's consumers would then diverge from what
		// landed, so the rolled-back view stays and only the still-live resources are
		// spared; the resolver settles it by re-attempting those bytes, and the release
		// runs when it does.
		readd := func() {
			sess.mu.Lock()
			if sess.cons == nil {
				sess.cons = make(map[string]*ConsumerConfig)
			}
			for _, a := range added {
				sess.cons[a.sid] = a.cc
			}
			sess.mu.Unlock()
			// These consumers are still live, so they must deliver again. (The voided
			// registers stay voided: a withdrawn allocation is never reinstated — the
			// source is simply re-offered under a fresh one — and a transmitted register
			// source is simply re-offered under a fresh one.)
			sess.reopenTeardownDelivery(rollbackClosed)
			sess.unvoidRegNonces(rollbackVoided)
			// Same reasoning for the affine path: nothing was deleted, so the session
			// must go back to describing the resources that are still live.
			sess.restoreQ2AffineUnwind(q2AffUnwind)
			// And the shared membership, LAST — after the affine unwind, because a shared
			// QoS 2 membership with no affine generation is a record shape
			// mqttValidateRecordAffinePair refuses, so this order keeps the in-memory
			// pairing that shape rests on. Only on THIS arm: an ambiguous rollback may
			// have stored exactly the rolled-back record, and the resolver settles it by
			// re-attempting those bytes. See restoreAttemptedShared. F-039.
			sess.restoreAttemptedShared(attemptedShared)
			// And with it the verdicts this write was to carry: the record still names
			// the attempted acquisitions, so nothing authorizes their release.
			sess.mu.Lock()
			sess.withdrawAcqReleaseVerdictsLocked(rollbackVerdicts)
			sess.mu.Unlock()
		}
		switch outcome {
		case saveDefinitelyFailed:
			readd()
			// The withdrawn pairs are NOT deleted: nothing durably says the withdrawal
			// happened, so their registers and markers must stand. The verdicts do stay
			// — a withdrawn allocation is never reinstated, and the source is re-offered
			// under a fresh one.
		case saveMaybeLanded:
			// Once this rollback's own write is proven durable the record already
			// describes the rolled-back session and the releases below are owed —
			// nothing else resumes them, so the packet's consumers and their charges
			// would be leaked.
			//
			// The reactivated drains' withdrawn pairs go with it EXPLICITLY: commitDrains
			// collects d.withdrawn from STAGED drains only, and a snapshot-restored drain
			// is not staged, while the cancellation list is built from the packet's newly
			// created consumers and so excludes the adopted ones. Nothing else would
			// discover them.
			withdrawn := rollbackWithdrawn
			sess.retainOnLanded(func(rtok recordTx) {
				sess.finishSubscribeRollback(c, rtok, subs, added, rollbackCancels, snap, q2AffUnwind, rollbackSurv)
				sess.voidRegistersOnStream(c, withdrawn, mqttLicenceOf(c))
			})
		}
		c.Errorf("MQTT: unable to persist rolled-back session after SUBSCRIBE failure: %v", err)
		return
	}
	sess.finishSubscribeRollback(c, tok, subs, added, rollbackCancels, snap, q2AffUnwind, rollbackSurv)
	// Confirmed, so the verdicts naming these pairs are durable and the never-transmitted
	// registers and markers may go, exactly as commitDrains hands a staged drain's over.
	sess.voidRegistersOnStream(c, rollbackWithdrawn, mqttLicenceOf(c))
}

// finishSubscribeRollback releases what a SUBSCRIBE packet created, once the
// rolled-back record is CONFIRMED. Split out so a readback that later proves an
// ambiguous rollback write landed can resume it: otherwise the record describes the
// rolled-back session while the packet's consumers, their MaxAckPending charges and
// its raw subscriptions stay behind, referenced by nothing.
//
// surv is the membership that survives this rollback, built by the staging half from the
// PRE-PACKET snapshot and passed down rather than rebuilt here, for two reasons: the two
// halves must agree on one answer, and snap.shared is assigned BY REFERENCE into
// sess.sharedSubs by the restoration, so on the landed-continuation route — which runs from
// the resolver, arbitrarily later — rebuilding from it would read a map another packet may
// since have mutated. mqttSharedSurvivorsOf copies, so the set here is immutable. F-083.
//
// No lock held on entry.
func (sess *mqttSession) finishSubscribeRollback(c *client, tok recordTx, subs []*subscription, added []addedCons,
	cancels []mqttRegCancellation, snap *mqttSubSnapshot, q2AffUnwind mqttQ2AffUnwind,
	surv mqttSharedSurvivors) {
	// The rolled-back record is confirmed, so the restored view is the settled one: a
	// retention the rollback put back is accounted for by the pre-packet state it restored.
	sess.mu.Lock()
	sess.settleAdoptedRetentionsLocked()
	sess.mu.Unlock()
	// Durable in the rolled-back record already; the tombstone is cleanup.
	sess.secureRegCancellations(c, tok, cancels, mqttLicenceOf(c))
	sess.finishQ2AffineUnwind(c, q2AffUnwind)

	// A raw subscription "existed before" this packet if the restored membership
	// still holds its filter. The level-up sid ("<mainsid without .>> fwc") maps
	// back to the main membership key ("<...>.>").
	//
	// BOTH maps are consulted, and the sid is NOT classified by prefix. A plain filter can
	// carry the "$share/" prefix — mqttToNATSSubjectConversion encodes a literal '.' as "//",
	// so plain "$share.foo" has the sid "$share//foo" — and classifying it as shared looked
	// its key up in snap.shared alone, found nothing, and unsubscribed a subscription that
	// PREDATED the packet on a rollback that is supposed to restore it. The two keyspaces are
	// disjoint by construction (a shared key is "$share/<group>/<subject>", a plain one is a
	// NATS subject), so testing both is exact rather than merely safe. F-083.
	existedBefore := func(sid string) bool {
		key := sid
		if strings.HasSuffix(key, mqttMultiLevelSidSuffix) {
			key = strings.TrimSuffix(key, mqttMultiLevelSidSuffix) + ".>"
		}
		if _, ok := snap.shared[key]; ok {
			return true
		}
		_, ok := snap.subs[key]
		return ok
	}
	for _, sub := range subs {
		if sub != nil && !existedBefore(string(sub.sid)) {
			if uerr := c.processUnsub(sub.sid); uerr != nil {
				c.Warnf("MQTT: unable to remove subscription %q: %v", sub.sid, uerr)
			}
		}
	}
	for _, a := range added {
		if a.cc.DeliverGroup != _EMPTY_ {
			// A durable a SURVIVING membership still reaches is not this rollback's to
			// release LOCALLY: the local release is whole-durable — the purge, the refund and
			// the deliver-subject unsubscribe would all land on a live filter. The
			// acquisitions this rollback removed are released by their verdicts regardless.
			// F-083.
			if surv.ownsDurable(a.cc.Durable) {
				continue
			}
			sess.releaseSharedConsumer(c, tok, a.cc)
		} else {
			sess.execDeferredConsumerDelete(c, tok, a.sid, a.cc, mqttDisposalRolledBack)
		}
	}
	// The verdicts the rolled-back record carries are executed whether or not any local
	// durable state was owed: a packet whose every created consumer is still reached by a
	// surviving key released nothing above, and its acquisitions are owed a release all the
	// same. Idempotent beside the releases above, which settle the same verdicts.
	sess.executeSharedReleases(c)
}
