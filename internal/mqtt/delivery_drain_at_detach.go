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

// A QoS 1/2 offer already routed to a connection that detaches is owed to the session: its
// delivery subscription is drained rather than unsubscribed, and every drained offer is either
// recorded withheld for the successor's resume to NAK, or NAKed by its writer when that resume
// has already read the markers (issue #170).

package mqtt

import (
	"strings"
	"weak"

	"github.com/nats-io/nats.go"
)

// mqttOfferOrigin is the subscription and connection an offer reached. The zero value is the
// session's own bookkeeping, which records as the session's own connection does.
//
// It NAMES them and never keeps them: an origin outlives its connection on a pending exchange
// (mqttPending.origin) and in a late tag, and a strong reference there kept the whole
// disconnected client — its read buffer, its per-client NATS connection — reachable for as long
// as the session stayed resident. Identity survives collection (two weak pointers made from the
// same object compare equal, before and after it is reclaimed), and the one fact a decision
// reads from the subscription, its drain state, is the drain cell, which holds no reference
// back.
type mqttOfferOrigin struct {
	sub   weak.Pointer[subscription]
	cc    weak.Pointer[client]
	drain *mqttDetachDrain
}

// mqttOriginOf names the offer's subscription and connection.
func mqttOriginOf(sub *subscription, cc *client) mqttOfferOrigin {
	o := mqttOfferOrigin{sub: weak.Make(sub), cc: weak.Make(cc)}
	if sub != nil && sub.mqtt != nil {
		o.drain = sub.mqtt.drain
	}
	return o
}

// mqttDetachDrain is the drain-at-detach state of one ordinary non-shared QoS 1/2 delivery
// subscription: its at-most-one Drain (drainDeliveriesAtDetach), guarded by sess.mu. Claimed is
// set before the lock is released to call Drain, so no second teardown issues another or falls
// back to an unsubscribe that would cancel it. Issued is set only after Drain returned nil, in
// the section that runs the handoff: from then on the UNSUB precedes, on the service
// connection, any NAK published for this subscription's offers, which is what licenses the
// writer's late NAK. Claimed alone licenses nothing.
//
// A cell rather than fields of the subscription so that an origin can read the drain state
// after the subscription and its connection are gone, without keeping either.
type mqttDetachDrain struct {
	claimed bool
	issued  bool
}

// mqttWithheldOutcome is what recordWithheldAttemptLocked decided for one withheld offer.
type mqttWithheldOutcome uint8

const (
	// mqttWithheldRefused: nothing recorded (empty arguments, or a mapped sequence whose
	// exchange owns the re-offer).
	mqttWithheldRefused mqttWithheldOutcome = iota
	// mqttWithheldRecorded: a marker names the source for a freed slot or a resume.
	mqttWithheldRecorded
	// mqttWithheldNAKOwed: nothing recorded; the caller NAKs the reply after unlocking.
	mqttWithheldNAKOwed
)

// mqttLateMarker tags a marker another connection's offer recorded after the resume cutoff
// readBy: its origin subscription's drain, once issued, NAKs it (handOffLateMarkersLocked).
type mqttLateMarker struct {
	sub    weak.Pointer[subscription]
	readBy weak.Pointer[client]
	reply  string
}

// mqttDrainAtDetachTestHooks are one session's seams around the drain at detach and the resume
// cutoff. Every field may be nil; none runs under a lock unless it says so.
type mqttDrainAtDetachTestHooks struct {
	// beforeTeardown runs in closeConnection before its subscriptions are torn down, after the
	// connection was detached: the detach-to-close gap.
	beforeTeardown func(c *client)
	// teardownOp reports, per subscription closeConnection tears down, "drain" or
	// "unsubscribe" (and "claimed" for a drain another teardown already claimed).
	teardownOp func(c *client, sub *subscription, op string)
	// claimed runs after a subscription's drain was claimed and the lock released, before
	// Drain is called.
	claimed func(sub *subscription)
	// issued runs after Drain returned and its section ran, with Drain's error and the NAKs
	// the handoff owes (not yet published).
	issued func(sub *subscription, err error, handoff []string)
	// cutoff runs after resendPendingRedelivery read the markers and set the cutoff, with no
	// lock held.
	cutoff func(c *client)
	// decided runs UNDER sess.mu with every writer decision.
	decided func(dur string, sseq uint64, origin mqttOfferOrigin, outcome mqttWithheldOutcome)
	// lateNAK runs before each late NAK (the writer's or the handoff's) is published.
	lateNAK func(reply string)
}

func (sess *mqttSession) drainAtDetachHooks() *mqttDrainAtDetachTestHooks {
	return sess.testDrainAtDetach.Load()
}

// withheldDecidedLocked reports a writer decision to the test seam and returns it.
//
// Lock held on entry.
func (sess *mqttSession) withheldDecidedLocked(dur string, sseq uint64, origin mqttOfferOrigin, outcome mqttWithheldOutcome) mqttWithheldOutcome {
	if h := sess.drainAtDetachHooks(); h != nil && h.decided != nil {
		h.decided(dur, sseq, origin, outcome)
	}
	return outcome
}

// resumeCutoffPassedLocked reports that the bound connection's resume pass has read the
// markers: a marker written now is read by nobody until a freed slot or the next resume.
//
// Lock held on entry.
func (sess *mqttSession) resumeCutoffPassedLocked() bool {
	return sess.c != nil && sess.resumeReadBy == weak.Make(sess.c)
}

// setResumeCutoffLocked records that c's resume pass has read the markers. A new cutoff starts
// a new late-NAK allowance and forgets the previous cutoff's tags: a marker recorded after an
// earlier binding's read is in sess.deferred, which this read has just taken.
//
// Lock held on entry.
func (sess *mqttSession) setResumeCutoffLocked(c *client) {
	sess.resumeReadBy = weak.Make(c)
	sess.lateNAKed = nil
	sess.deferredLate = nil
}

// publishLateNAKs publishes the late NAKs a writer decision or a drain handoff owes, after the
// lock that decided them was released.
//
// No lock held on entry.
func (sess *mqttSession) publishLateNAKs(replies ...string) {
	for _, r := range replies {
		if r == _EMPTY_ {
			continue
		}
		if h := sess.drainAtDetachHooks(); h != nil && h.lateNAK != nil {
			h.lateNAK(r)
		}
		sess.jsa.sendMsg(r, AckNak)
	}
}

// mqttRecordSelection is what the session's record names, selected from memory: the ordinary
// consumers with a membership, and the drains (active and staged, not removal-marked, plus the
// retained adoptions). It is the ONE definition of that selection: the record builder
// serializes it, and the ownership property every marker reader applies derives from it.
type mqttRecordSelection struct {
	cons   map[string]*ConsumerConfig
	drains map[string]*mqttDrain
}

// recordSelectionLocked selects what the record names.
//
// Cons: a plain consumer only while its membership exists in Subs (the level-up sid "<X> fwc"
// maps back to the membership key "<X>.>"); a shared (DeliverGroup) consumer never, because
// SharedSubs is its sole authority. A consumer with no membership that a retained adoption
// holds — the adoption naming exactly this config — is named in Drains instead, or the record
// would leave it referenced nowhere and a sibling sweep would delete it with its started
// deliveries (I8).
//
// Drains: every drain, staged ones included — the staging packet's save is what commits a
// staged drain, carrying the membership change and the drain that replaces the consumer in one
// record (I4) — except one whose RETIREMENT TRANSITION marked it for removal (d.removalMarked,
// the only thing that authorizes omission: I3's "the save that clears the last claim removes
// the entry" is that transition's property, not the claim count's). The retained adoptions are
// named beside the active drains, never instead of them: an adopted sid is absent from
// sess.drains by construction.
//
// Lock held on entry.
func (sess *mqttSession) recordSelectionLocked() mqttRecordSelection {
	var sel mqttRecordSelection
	var adopted map[string]*mqttDrain
	if len(sess.cons) > 0 {
		sel.cons = make(map[string]*ConsumerConfig, len(sess.cons))
		for sid, cc := range sess.cons {
			if cc.DeliverGroup != _EMPTY_ {
				continue
			}
			key := sid
			if strings.HasSuffix(key, mqttMultiLevelSidSuffix) {
				key = strings.TrimSuffix(key, mqttMultiLevelSidSuffix) + ".>"
			}
			if _, ok := sess.subs[key]; !ok {
				if d := sess.adoptedDrainForLocked(sid); d != nil {
					if adopted == nil {
						adopted = make(map[string]*mqttDrain, 1)
					}
					adopted[sid] = d
				}
				continue
			}
			sel.cons[sid] = cc
		}
		if len(sel.cons) == 0 {
			sel.cons = nil
		}
	}
	for sid, d := range sess.drains {
		if d.removalMarked {
			continue
		}
		if sel.drains == nil {
			sel.drains = make(map[string]*mqttDrain, len(sess.drains)+len(adopted))
		}
		sel.drains[sid] = d
	}
	for sid, d := range adopted {
		if sel.drains == nil {
			sel.drains = make(map[string]*mqttDrain, len(adopted))
		}
		sel.drains[sid] = d
	}
	return sel
}

// mqttDurableSet is a set of JetStream durable names.
type mqttDurableSet map[string]struct{}

func (s mqttDurableSet) has(durable string) bool {
	_, ok := s[durable]
	return ok
}

// recordReferencedDurablesLocked is every durable the session's record references: the
// selected consumers and drains (recordSelectionLocked), every shared membership's group
// durables — main and level-up, each at its group incarnation (mqttSharedGroupNamesFor) — the
// affine QoS 2 durable and the PUBREL durable.
//
// THE OWNERSHIP PROPERTY OF A WITHHELD MARKER (issue #170): a marker is acted on only while
// its durable is referenced here. A predecessor's offer can be decided after whatever ended the
// session's ownership of its durable — a teardown, a drain retirement, a shared release, a
// downgrade — and its marker then names a source no reader may NAK or wait for. Readers skip
// such a marker, and delete it only once the absence is SETTLED (markerOwnedLocked): membership
// is removed tentatively only inside an open record transaction, and restored by a definite
// save failure before that transaction ends, so a marker skipped there must still be there
// for the restored membership.
//
// Lock held on entry.
func (sess *mqttSession) recordReferencedDurablesLocked() mqttDurableSet {
	sel := sess.recordSelectionLocked()
	set := make(mqttDurableSet, len(sel.cons)+len(sel.drains)+2*len(sess.sharedSubs)+2)
	for _, cc := range sel.cons {
		if cc != nil && cc.Durable != _EMPTY_ {
			set[cc.Durable] = struct{}{}
		}
	}
	for _, d := range sel.drains {
		if d.cc != nil && d.cc.Durable != _EMPTY_ {
			set[d.cc.Durable] = struct{}{}
		}
	}
	for key, m := range sess.sharedSubs {
		for _, t := range mqttSharedGroupNamesFor(key, m) {
			set[t.durable] = struct{}{}
		}
	}
	if sess.q2AffConsumer != nil && sess.q2AffConsumer.Durable != _EMPTY_ {
		set[sess.q2AffConsumer.Durable] = struct{}{}
	}
	if sess.pubRelConsumer != nil && sess.pubRelConsumer.Durable != _EMPTY_ {
		set[sess.pubRelConsumer.Durable] = struct{}{}
	}
	return set
}

// recordReferencesDurableLocked reports whether the record references durable: the same
// selection recordReferencedDurablesLocked collects, asked for one durable without
// materializing it. Each source compares the durable first and applies its selection rule
// (membership key, adoption identity, removal mark) only on a match, so a miss allocates
// nothing outside the shared memberships, whose group names are derived only for a durable
// carrying the shared prefix.
//
// Lock held on entry.
func (sess *mqttSession) recordReferencesDurableLocked(durable string) bool {
	if sess.q2AffConsumer != nil && sess.q2AffConsumer.Durable != _EMPTY_ && sess.q2AffConsumer.Durable == durable {
		return true
	}
	if sess.pubRelConsumer != nil && sess.pubRelConsumer.Durable != _EMPTY_ && sess.pubRelConsumer.Durable == durable {
		return true
	}
	if durable == _EMPTY_ {
		return false
	}
	// Cons, as recordSelectionLocked selects them: a plain consumer with its membership, or
	// one a retained adoption holds as exactly this config (named in Drains).
	for sid, cc := range sess.cons {
		if cc == nil || cc.Durable != durable || cc.DeliverGroup != _EMPTY_ {
			continue
		}
		key := sid
		if strings.HasSuffix(key, mqttMultiLevelSidSuffix) {
			key = strings.TrimSuffix(key, mqttMultiLevelSidSuffix) + ".>"
		}
		if _, ok := sess.subs[key]; ok {
			return true
		}
		if d := sess.adoptedDrainForLocked(sid); d != nil && d.cc.Durable == durable {
			return true
		}
	}
	// Drains, active and staged, except one its retirement transition marked for removal.
	for _, d := range sess.drains {
		if d.cc != nil && d.cc.Durable == durable && !d.removalMarked {
			return true
		}
	}
	if !strings.HasPrefix(durable, mqttSharedConsumerDurablePrefix) {
		return false
	}
	for key, m := range sess.sharedSubs {
		if mqttSharedOwnsDurable(key, m, durable) {
			return true
		}
	}
	return false
}

// mqttMarkerOwnershipMemo is how many distinct durables one reader pass asks
// recordReferencesDurableLocked about before it builds the referenced set once instead: the
// predicate is a scan of the session's sources, so a pass over markers of many durables pays
// one scan rather than one per durable.
const mqttMarkerOwnershipMemo = 16

// mqttMarkerOwnership is one reader pass's view of marker ownership: whether the record
// references each durable the pass meets, decided once per distinct durable, and whether an
// unreferenced marker's absence is settled for the whole pass.
type mqttMarkerOwnership struct {
	settled bool
	n       int
	memo    [mqttMarkerOwnershipMemo]struct {
		durable    string
		referenced bool
	}
	set mqttDurableSet
}

// markerOwnershipLocked starts a reader pass.
//
// SETTLED means no record transaction is open and no record write is unresolved. Every path
// that removes a membership and can put it back does both inside one open record transaction
// (the restore of a definite save failure runs before that transaction ends), and an
// ambiguous write keeps its removal and settles forward, never back. So with neither open,
// an unreferenced durable cannot be restored by the outcome of anything in flight.
//
// Lock held on entry.
func (sess *mqttSession) markerOwnershipLocked() mqttMarkerOwnership {
	return mqttMarkerOwnership{settled: !sess.recordTxOpen && !sess.seqUncertain}
}

// referenced reports whether the record references durable.
//
// Lock held on entry.
func (o *mqttMarkerOwnership) referenced(sess *mqttSession, durable string) bool {
	if o.set != nil {
		return o.set.has(durable)
	}
	for i := 0; i < o.n; i++ {
		if o.memo[i].durable == durable {
			return o.memo[i].referenced
		}
	}
	if o.n == len(o.memo) {
		o.set = sess.recordReferencedDurablesLocked()
		return o.set.has(durable)
	}
	r := sess.recordReferencesDurableLocked(durable)
	o.memo[o.n].durable, o.memo[o.n].referenced = durable, r
	o.n++
	return r
}

// markerOwnedLocked reports whether a reader may act on the marker k: its durable is one the
// record references. A marker that is not is skipped; once its absence is settled
// (markerOwnershipLocked) it is also deleted, with its late tag, so a marker that outlived
// its durable's ownership costs one reader pass rather than every pass for the session's life.
// A tentative absence keeps it: the transaction that removed the membership may restore it.
//
// Lock held on entry.
func (sess *mqttSession) markerOwnedLocked(o *mqttMarkerOwnership, k mqttDeferredKey) bool {
	if o.referenced(sess, k.jsDur) {
		return true
	}
	if o.settled {
		delete(sess.deferred, k)
		delete(sess.deferredLate, k)
	}
	return false
}

// referencedMarkerRemainsLocked reports whether any marker names a durable the record
// references: the "nothing is deferred" question every waiter on markers asks.
//
// Lock held on entry.
func (sess *mqttSession) referencedMarkerRemainsLocked() bool {
	if len(sess.deferred) == 0 {
		return false
	}
	own := sess.markerOwnershipLocked()
	for k := range sess.deferred {
		if sess.markerOwnedLocked(&own, k) {
			return true
		}
	}
	return false
}

// drainDeliveriesAtDetach tears down the drain-class delivery subscriptions among subs with a
// Drain instead of an unsubscribe, so the offers already routed to them reach their callbacks
// and are recorded withheld or late-NAKed rather than discarded (issue #170). Each subscription
// gets at most one Drain, from whichever teardown claims it first: the connection's own close,
// or the successor that displaced it. A subscription that is not drain class, or every one
// when shutdown is true, is left to the caller (returned), which unsubscribes it; one already
// claimed is neither drained again nor unsubscribed, which would cancel the drain.
//
// AT SHUTDOWN a detach is not a resume: the subscriptions are unsubscribed as before, and a
// drain another teardown already started is neither cancelled nor joined.
//
// No lock held on entry.
func (sess *mqttSession) drainDeliveriesAtDetach(c *client, subs []*subscription, shutdown bool) (unsubscribe []*subscription) {
	h := sess.drainAtDetachHooks()
	for _, sub := range subs {
		ns, _ := sub.nsub.(*nats.Subscription)
		sess.mu.Lock()
		drainClass := sub.mqtt != nil && sub.mqtt.drain != nil
		claimedElsewhere := drainClass && sub.mqtt.drain.claimed
		claim := drainClass && !claimedElsewhere && !shutdown && ns != nil
		if claim {
			sub.mqtt.drain.claimed = true
		}
		sess.mu.Unlock()
		switch {
		case claimedElsewhere:
			if h != nil && h.teardownOp != nil {
				h.teardownOp(c, sub, "claimed")
			}
			continue
		case !claim:
			if h != nil && h.teardownOp != nil {
				h.teardownOp(c, sub, "unsubscribe")
			}
			unsubscribe = append(unsubscribe, sub)
			continue
		}
		if h != nil && h.teardownOp != nil {
			h.teardownOp(c, sub, "drain")
		}
		if h != nil && h.claimed != nil {
			h.claimed(sub)
		}
		err := ns.Drain()
		var handoff []string
		sess.mu.Lock()
		if err == nil {
			sub.mqtt.drain.issued = true
			handoff = sess.handOffLateMarkersLocked(sub)
		}
		sess.mu.Unlock()
		if h != nil && h.issued != nil {
			h.issued(sub, err, handoff)
		}
		sess.publishLateNAKs(handoff...)
	}
	return unsubscribe
}

// mqttSessionForTeardown is the session this connection's subscriptions deliver for, or nil
// before CONNECT assigned one (no session subscription exists then).
//
// c.mu NOT held on entry.
func (c *client) mqttSessionForTeardown() *mqttSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mqtt == nil {
		return nil
	}
	return c.mqtt.sess
}

// drainDisplacedDeliveries is the successor's half of the drain at detach: right after a
// CONNECT bound the session away from a live predecessor, the predecessor's drain-class
// delivery subscriptions are drained now, before the successor restores its own, rather than
// whenever the predecessor's own close gets to them.
//
// No lock held on entry.
func (sess *mqttSession) drainDisplacedDeliveries(successor, predecessor *client) {
	if predecessor == nil || predecessor == successor {
		return
	}
	predecessor.mu.Lock()
	subs := make([]*subscription, 0, len(predecessor.subs))
	for _, sub := range predecessor.subs {
		subs = append(subs, sub)
	}
	predecessor.mu.Unlock()
	// Only the drain class: the predecessor's own close unsubscribes the rest, as before.
	var drainClass []*subscription
	sess.mu.Lock()
	for _, sub := range subs {
		if sub.mqtt != nil && sub.mqtt.drain != nil {
			drainClass = append(drainClass, sub)
		}
	}
	sess.mu.Unlock()
	shutdown := predecessor.srv != nil && predecessor.srv.isShuttingDown()
	_ = sess.drainDeliveriesAtDetach(predecessor, drainClass, shutdown)
}

// handOffLateMarkersLocked is the drain-start handoff: once sub's drain is issued, the markers
// its offers recorded after the current cutoff — while its subscription was not yet draining,
// or between the claim and the issue — are NAKed now instead of waiting for a freed slot or the
// next resume. Each source gets at most one late NAK per cutoff, from the writer or from here;
// a marker of a durable the record no longer references is skipped (markerOwnedLocked). Returns the
// replies to NAK after unlocking.
//
// Lock held on entry.
func (sess *mqttSession) handOffLateMarkersLocked(sub *subscription) []string {
	if len(sess.deferredLate) == 0 || !sess.resumeCutoffPassedLocked() {
		return nil
	}
	own := sess.markerOwnershipLocked()
	var naks []string
	id := weak.Make(sub)
	for k, t := range sess.deferredLate {
		if t.sub != id {
			continue
		}
		// A tag is a fact only while its marker still holds what was tagged, under this
		// cutoff; otherwise the marker was consumed, purged or rewritten since.
		if reply, ok := sess.deferred[k]; !ok || reply != t.reply || t.readBy != sess.resumeReadBy {
			delete(sess.deferredLate, k)
			continue
		}
		if _, spent := sess.lateNAKed[k]; spent {
			continue
		}
		if !sess.markerOwnedLocked(&own, k) {
			continue
		}
		if sess.lateNAKed == nil {
			sess.lateNAKed = make(map[mqttDeferredKey]struct{})
		}
		sess.lateNAKed[k] = struct{}{}
		delete(sess.deferred, k)
		delete(sess.deferredLate, k)
		naks = append(naks, t.reply)
	}
	return naks
}
