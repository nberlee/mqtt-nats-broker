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

// UNSUBSCRIBE and UNSUBACK, and the queued drain disposal.

package mqtt

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// mqttDrainDisposal is one queued drain retirement: the durable and the entitlement its omission
// was decided under (F-105 7c-lc).
type mqttDrainDisposal struct {
	durable string
	ent     mqttOutboundEntitlement
}

func (c *client) mqttParseUnsubs(r *mqttReader, b byte, pl int) (uint16, []*mqttFilter, error) {
	return c.mqttParseSubsOrUnsubs(r, b, pl, false)
}

// Process the UNSUBSCRIBE packet.
//
// Given the list of topics, this is going to unsubscribe the low level NATS subscriptions
// and delete the JS durable consumers when applicable.
//
// It returns the truthful-UNSUBACK restore for an AMBIGUOUS record write (nil
// otherwise), which the packet tail applies if that write is proven durable before
// it acknowledges. See mqttUnsubAckRestore.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (c *client) mqttProcessUnsubs(filters []*mqttFilter) (*mqttUnsubAckRestore, error) {
	// Those things are immutable, but since processing unsubs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	c.mu.Unlock()

	if err := asm.lockSession(sess, c); err != nil {
		return nil, err
	}
	defer asm.unlockSession(sess)

	// The whole UNSUBSCRIBE is one record transaction, opened before the first
	// membership mutation so no other writer can persist a half-applied packet.
	// Outermost readLoop entry point, so it takes the blocking form.
	packetTok := sess.beginRecordTx()
	defer func() {
		if err := sess.endRecordTx(packetTok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
	}()

	// The record token is a PARAMETER, never the packet's: this closure is reached from
	// commitUnsub, which also runs as a LANDED CONTINUATION under the resolver's
	// transaction — and every write below (the shared release, the cancellation
	// tombstones, the register deletes) is refused at entry under a closed one. A
	// nested closure capturing the enclosing `tok` is exactly how that leak survives a
	// signature change on its caller.
	// v5 UNSUBACK reports whether each subscription existed; the session maps are
	// guarded by the session-manager lock held here. Spec5 [3.11.3].
	for _, f := range filters {
		sid := f.sessionKey()
		var ok bool
		if f.shared {
			_, ok = sess.sharedSubs[sid]
		} else {
			_, ok = sess.subs[sid]
		}
		if !ok {
			f.reason = mqttReasonNoSubscriptionExisted
			f.reasonStr = "no matching subscription existed"
		}
	}
	// UNSUBSCRIBE is transactional: persist the removals FIRST, and only tear down
	// the NATS subscriptions and (shared) consumers once the record save is
	// confirmed. Otherwise a failed save could leave the record claiming a
	// subscription whose live join and group durable were already removed.
	// Snapshot every affected map so a save failure fully rolls back in memory.
	// Spec5 [4.8.2], [3.11.3].
	snap := sess.snapshotSubs()

	// [MQTT-3.10.4-3]: a QoS 1/2 message the broker has already STARTED sending
	// must still be completed. Convert each such consumer into a DRAIN instead of
	// tearing it down — staged now so the save below carries the membership removal
	// and the drain that replaces it in one record (I4). See unsub_drain.go.
	drainedSids := make(map[string]bool)
	// Checked ONCE per packet, and before any staging: the precondition is a
	// property of the stream, not of a filter, and it must be settled before the
	// record can claim a drain. See drainingAllowed.
	mayDrain := sess.drainingAllowed(c)
	sess.mu.Lock()
	for _, f := range filters {
		if !mayDrain {
			break
		}
		if f.shared {
			continue // shared QoS 2 is already session-affine; shared QoS 1 may redistribute
		}
		for _, sid := range mqttUnsubSids(f.sessionKey()) {
			cc, ok := sess.cons[sid]
			if !ok {
				continue
			}
			c.mu.Lock()
			sub := c.subs[cc.DeliverSubject]
			c.mu.Unlock()
			if sess.stageDrain(sid, cc, sub, mqttLicenceOf(c)) {
				drainedSids[sid] = true
			}
		}
	}
	stagedCancels, closedDeliveries, voidedSent := sess.stageUnsubCancellationsLocked(c, filters, drainedSids)
	// The MEMBERSHIPS this packet removes, captured while the live map still carries them:
	// the acquisitions they name are what the release after the record write acts on, and
	// the removal below erases them from the map.
	removedShared := make(map[string]mqttSharedMembership)
	for _, f := range filters {
		if !f.shared {
			continue
		}
		if m, ok := sess.sharedSubs[f.sessionKey()]; ok {
			removedShared[f.sessionKey()] = m
		}
	}
	sess.mu.Unlock()

	// Everything the UNSUBSCRIBE does AFTER its record write is confirmed. It is a
	// closure because the confirmation can arrive in two ways: inline, or later from
	// the resolver proving an ambiguous write durable. Skipping it in the second
	// case would leave the record saying "unsubscribed" while the runtime kept the
	// subscriptions, the consumers and the staged (uncommitted) drains — a
	// half-applied packet with nothing left to finish it.
	//
	// The record token is a PARAMETER, never the captured one: as a landed
	// continuation this runs under the RESOLVER'S transaction, and the packet's own
	// is closed by then — every write under it would be refused at entry.
	commitUnsub := func(tok recordTx) {
		// The staged cancellations are durable in the packet's own record now, so this
		// is cleanup: a tombstone lets the register go without waiting for a resume.
		sess.secureRegCancellations(c, tok, stagedCancels, mqttLicenceOf(c))
		// The leaving filters are already out of sharedSubs and persisted, so one scan
		// now decides every shared durable this packet may release; keys the session
		// still holds (e.g. "$share/g/foo" while "$share/g/foo.>" leaves) count as
		// holders. The staged drains are now durable, so fresh deliveries on their
		// filters become ack-discards rather than being left to AckWait.
		// Spec5 [4.8.2].
		sess.mu.Lock()
		withdrawn := sess.commitDrains()
		sess.mu.Unlock()
		// The confirmed-but-never-transmitted entries the staging withdrew (N-7.7): their
		// register and unsent marker go as never transmitted, the verdicts naming them
		// being durable in this packet's record.
		sess.voidRegistersOnStream(c, withdrawn, mqttLicenceOf(c))

		sess.releaseUnsubSharedTargets(c, tok, removedShared)
		for _, f := range filters {
			sid := f.sessionKey()
			for _, s := range mqttUnsubSids(sid) {
				// A DRAINED sid keeps its durable, its delivery subscription and its
				// pending deliveries — that is the whole point — so it is skipped here.
				// Only the client-facing subscription goes, because the client really did
				// unsubscribe. Shared filters are handled above, by KEY rather than by sid.
				if !drainedSids[s] && !f.shared {
					if cc := sess.plainConsumerForUnsub(s); cc != nil {
						sess.execDeferredConsumerDelete(c, tok, s, cc, mqttDisposalUnsubscribed)
					}
				}
				if err := c.processUnsub([]byte(s)); err != nil {
					c.Errorf("error unsubscribing from %q: %v", s, err)
				}
			}
		}
		// Settlement runs here: INSIDE the confirmed commit, and AFTER the teardown loop
		// above. Both halves of that placement are load-bearing, and each was got wrong
		// once.
		//
		// After the teardown, because for an unmembered adopted consumer with nothing left
		// owed stageDrain reports nothing staged — so cons[sid] is still the config the
		// retention describes and no active drain exists, and the helper correctly keeps
		// it. It is execDeferredConsumerDelete that removes the entry, so asking before it
		// ran held the object and its sequence map for the session's lifetime.
		//
		// Inside the commit, because a retention may only be released by a CONFIRMED
		// outcome. Settling before the packet's save released it on an outcome that might
		// still fail, and a definite failure restores cons through rollbackStagedDrains
		// while nothing restores the retention — so the next save would omit the durable
		// and a sibling sweep could delete its outstanding deliveries. That is message
		// loss, not a leak, which is why this sits where the record is already proven.
		sess.mu.Lock()
		sess.settleAdoptedRetentionsLocked()
		sess.mu.Unlock()
	}

	wasUncertain := sess.isSeqUncertain()
	if err := sess.update(packetTok, filters, false); err != nil {
		// Roll back the in-memory removals ONLY if nothing was stored. An ambiguous
		// write may already carry them, and the resolver settles it forward by
		// re-attempting exactly those bytes — restoring here would make the next save
		// overwrite an unsubscribe that is, or is about to be, durable. The packet
		// still reports failure; the session is unresolved until the resolver settles
		// it, so nothing else can be persisted meanwhile. See sess_uncertain.go.
		undoUnsub := func() {
			// The delivery barrier goes back too: nothing was persisted, so these
			// subscriptions are still the client's and must keep delivering, and their
			// transmitted deliveries still owe their identifiers.
			sess.reopenTeardownDelivery(closedDeliveries)
			sess.unvoidRegNonces(voidedSent)
			sess.mu.Lock()
			sess.subs = snap.subs
			sess.sharedSubs = snap.shared
			sess.rapSubs = snap.rap
			sess.noLocalSubs = snap.noLocal
			sess.subIDs = snap.subIDs
			sess.rollbackStagedDrains()
			sess.mu.Unlock()
		}
		var restore *mqttUnsubAckRestore
		switch sess.classifySave(wasUncertain, err) {
		case saveDefinitelyFailed:
			undoUnsub()
		case saveMaybeLanded:
			// Kept, because the record may hold the removal and the staged drains, and
			// the resolver makes exactly those bytes durable. The confirmed-path work
			// above is then owed and runs from the resolution instead — under ITS
			// token, since this packet's is closed by then.
			//
			// The restore flag is this ATTEMPT's own: the continuation that sets it is
			// the one registered here, so it can fire for nothing else — not for a
			// foreign verdict, and not for some later ambiguity B armed by another
			// writer. Set LAST, so a reader that observes it observes a durable and
			// fully applied unsubscribe. See mqttUnsubAckRestore.
			landed := &atomic.Bool{}
			sess.retainOnLanded(func(rtok recordTx) {
				commitUnsub(rtok)
				landed.Store(true)
			})
			// Snapshotted BEFORE the 0x83 overwrite below, reason strings included: the
			// encoder carries the first failed filter's string as the packet-level
			// Reason String, so restoring the byte alone would tell a client "unable to
			// persist unsubscribe" for a filter that in fact had no subscription.
			restore = newMQTTUnsubAckRestore(landed, filters)
			// Deliberately NOT resolved here: this packet's own record transaction is
			// still open, and the resolver must hold the fence — so an attempt from
			// inside would simply be refused. The readLoop resolves once the
			// transaction is released. See mqttResolveAfterPacket.
		}
		// v5 reports the failure per-filter (0x83) and still sends the UNSUBACK;
		// 3.1.1 has no per-filter code, so surface the error (connection closes).
		if c.mqtt.proto == mqttProtoLevel5 {
			for _, f := range filters {
				f.reason = mqttReasonImplementationSpecificError
				f.reasonStr = "unable to persist unsubscribe"
			}
			return restore, nil
		}
		return nil, err
	}
	commitUnsub(packetTok)
	return nil, nil
}

// mqttUnsubAckRestore is the truthful UNSUBACK a v5 UNSUBSCRIBE owes its client when
// the record write it reported as failed turns out to be DURABLE.
//
// The v5 UNSUBSCRIBE is the one operation this broker ACKNOWLEDGES over an ambiguous
// write: it overwrites every per-filter reason with 0x83 and still sends the
// UNSUBACK, so a write that landed leaves the client told the unsubscribe failed
// while the record says it happened. The resolver now settles such a write forward,
// which would make that contradiction deterministic — so the acknowledgment waits for
// the resolution the readLoop performs anyway (mqttResolveAfterPacket) and tells the
// truth whenever proof has arrived by then.
//
// The signal is the attempt's OWN landed continuation, never a session-state
// predicate: seqUncertain is cleared by a foreign disposition as well as by a
// resolution, and it can be re-armed by a different attempt entirely, so a coarse
// check would restore this packet's reasons on somebody else's outcome. A flag set
// by the continuation registered for THIS write cannot be either.
//
// reasons/strs are positional over the packet's filters, captured before the 0x83
// overwrite.
type mqttUnsubAckRestore struct {
	landed  *atomic.Bool
	reasons []byte
	strs    []string
}

// newMQTTUnsubAckRestore snapshots the per-filter reasons as they stood before the
// failure overwrite. landed is the flag the attempt's landed continuation sets — the
// SAME pointer, never a copy, since that is the whole handoff.
func newMQTTUnsubAckRestore(landed *atomic.Bool, filters []*mqttFilter) *mqttUnsubAckRestore {
	r := &mqttUnsubAckRestore{
		landed:  landed,
		reasons: make([]byte, len(filters)),
		strs:    make([]string, len(filters)),
	}
	for i, f := range filters {
		r.reasons[i], r.strs[i] = f.reason, f.reasonStr
	}
	return r
}

// applyIfLanded puts the true reasons back if this attempt has been proven durable.
//
// The flag LOAD is the linearization point of the guarantee: a verdict that arrives
// after it — including between here and the enqueue — leaves the honest 0x83 standing,
// which is the residual docs/CONFORMANCE.md states.
func (r *mqttUnsubAckRestore) applyIfLanded(filters []*mqttFilter) {
	if r == nil || !r.landed.Load() {
		return
	}
	for i, f := range filters {
		if i >= len(r.reasons) {
			break
		}
		f.reason, f.reasonStr = r.reasons[i], r.strs[i]
	}
}

// mqttAckUnsubscribe is the UNSUBSCRIBE packet's tail: make the UNSUBACK truthful if
// the packet's ambiguous write has since been proven durable, then acknowledge —
// unless this connection has been DISPLACED meanwhile, which is what a foreign
// verdict does. Reports whether the UNSUBACK was sent, so the caller traces exactly
// what went out.
//
// What ACTUALLY makes the guarantee hold is the DISCONNECT claim, read inside the
// enqueue's own c.mu critical section (mqttEnqueueProtoLocked). A displacement notifies
// through that claim, and the watch's eviction worker holds no lock of ours — so any check
// made and released BEFORE the enqueue is a check-then-act, and an acknowledgment
// appended after the displacement's DISCONNECT would violate [MQTT-3.14.4-1]. Only a check
// made WITH the write closes that window.
//
// The ownership sample below (`sess.c == c` under sess.mu, read that way rather than as
// c.isClosed(): a foreign disposition detaches synchronously and closes asynchronously,
// so the close flag lags the fact) is DEFENSIVE, deliberately. Every displacement that
// can reach this tail has a written CONNACK, so it consumes the claim, and the gate
// above refuses on its own; a mutation of the sample therefore SURVIVES, which is the
// honest signal that it guards nothing reachable today rather than something untested.
// It is kept because the cost is one comparison and it states a different fact — "this
// session is no longer mine" — that a future detach-without-notifying path would need,
// where the alternative is failing open.
//
// Runs from the client's readLoop. No lock held on entry.
func (c *client) mqttAckUnsubscribe(pi uint16, filters []*mqttFilter, restore *mqttUnsubAckRestore) bool {
	restore.applyIfLanded(filters)
	c.mu.Lock()
	sess := c.mqtt.sess
	c.mu.Unlock()
	if sess != nil {
		sess.mu.Lock()
		bound := sess.c == c
		sess.mu.Unlock()
		if !bound {
			return false
		}
	}
	// Test seam, fired in exactly the window the second gate exists for: after the
	// ownership sample, before the enqueue's critical section.
	if h := mqttTestUnsubAckWindowHookV.Load(); h != nil {
		(*h)(c)
	}
	return c.mqttEnqueueUnsubAck(pi, filters)
}

// mqttTestUnsubAckWindowHookV fires between the acknowledgment's ownership sample and
// its enqueue. See mqttAckUnsubscribe.
var mqttTestUnsubAckWindowHookV atomic.Pointer[func(c *client)]

func setMQTTTestUnsubAckWindowHook(h func(c *client)) func() {
	prev := mqttTestUnsubAckWindowHookV.Load()
	if h == nil {
		mqttTestUnsubAckWindowHookV.Store(nil)
	} else {
		mqttTestUnsubAckWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestUnsubAckWindowHookV.Store(prev) })
}

// plainConsumerForUnsub returns the resident PLAIN consumer config an UNSUBSCRIBE sid names,
// or nil when there is none or the sid names a shared group consumer.
//
// It exists because commitUnsub must read sess.cons under sess.mu and then act without it —
// execDeferredConsumerDeleteClassified takes the lock itself. The DeliverGroup test is a
// structural backstop, not the routing authority: commitUnsub already dispatches on f.shared,
// which is the parse-time truth. Nothing here may discriminate shared from plain LEXICALLY —
// a literal '.' encodes as "//" (mqttToNATSSubjectConversion), so the ordinary plain filter
// "$share.foo" has the sid "$share//foo" and a "$share/" prefix test would strand it. F-083.
//
// sess.mu must NOT be held: this takes it.
func (sess *mqttSession) plainConsumerForUnsub(sid string) *ConsumerConfig {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	cc, ok := sess.cons[sid]
	if !ok || cc == nil || cc.DeliverGroup != _EMPTY_ {
		return nil
	}
	return cc
}

// releaseUnsubSharedTargets discharges what an UNSUBSCRIBE's shared filters held, for a
// packet whose membership removals are already durable.
//
// THREE separate operations, because "this config is doomed" and "this durable is owed a
// release" are different questions and the second does not imply the first. A durable an
// overlapping key still holds is not owed — but the leaving key's own cons entries must go
// regardless, or they outlive the membership with nothing left to reach them by. F-083.
//
// The three dispositions per group consumer are F-082's, applied here: own-held discharges
// nothing and deletes nothing; a foreign holder or a REFUSED scan discharges this session's
// own state and withholds the delete; unheld does both. Own-held targets are dropped before
// the scan runs, so its answer can no longer be this session's own membership.
//
// PRECONDITION: the caller holds the session's OWNERSHIP GATE as well as an open record
// transaction — mqttProcessUnsubs takes it for the packet, and the landed continuation runs
// under the resolver's fence, which holds it. That is what keeps the surviving-membership
// sample true after sess.mu is released; sess.mu alone would only make each access
// race-free. A test driving this directly must take the gate itself.
//
// sess.mu must NOT be held: this takes it.
func (sess *mqttSession) releaseUnsubSharedTargets(c *client, tok recordTx, removed map[string]mqttSharedMembership) {
	if len(removed) == 0 {
		return
	}
	sharedKeys := make([]string, 0, len(removed))
	for key := range removed {
		sharedKeys = append(sharedKeys, key)
	}
	// (1) and (2) in ONE critical section: the doomed configs go, and the surviving
	// membership is sampled without a cons mutation of this same packet in between.
	// sess.update already applied the removals, so the live map IS what survives and nothing
	// needs excluding — the opposite of the staging half, which runs before them.
	sess.mu.Lock()
	for _, key := range sharedKeys {
		for _, sid := range mqttUnsubSids(key) {
			delete(sess.cons, sid)
		}
	}
	owed := sess.survivingSharedLocked(nil).targetsOwed(removed)
	sess.mu.Unlock()
	if len(owed) == 0 {
		// No local durable state is owed a release — every doomed durable is still reached by
		// a surviving key — but the ACQUISITIONS are, and their verdicts are in the record.
		sess.executeSharedReleases(c)
		return
	}
	for _, t := range owed {
		// (3) Every REMAINING config naming an owed durable, not the first: a durable can be
		// named by more than one stale entry, and one left behind has no membership to reach
		// it by. After (1) any such entry is stale by definition, since no surviving key
		// reaches an owed durable.
		//
		// The SUBSCRIBE rollback's definite arm no longer produces that shape (F-039), and
		// this backstop is kept regardless: it is a superset sweep, and narrowing it to the
		// producers we can currently name is how a sweep boundary stops converging.
		sess.mu.Lock()
		for csid, cfg := range sess.cons {
			if cfg != nil && cfg.Durable == t.durable {
				delete(sess.cons, csid)
			}
		}
		sess.mu.Unlock()
		// Outside sess.mu: the release takes it itself.
		sess.releaseSharedGroupTarget(c, tok, t.durable, t.deliver)
	}
}

// mqttUnsubSids returns the sid(s) one UNSUBSCRIBE filter covers: its own, plus
// the level-up companion a "foo/#" filter carries, which drains (and tears down)
// independently.
func mqttUnsubSids(sid string) []string {
	if !mqttNeedSubForLevelUp(sid) {
		return []string{sid}
	}
	return []string{sid, sid[:len(sid)-2] + mqttMultiLevelSidSuffix}
}

// mqttEnqueueUnsubAck enqueues an UNSUBACK and reports whether it was sent. A v5
// UNSUBACK whose reason-code payload alone exceeds the client's Maximum Packet
// Size is suppressed (returns false), and BOTH protocol paths refuse to append the
// packet behind a server-initiated DISCONNECT — checked in the same c.mu critical
// section as the write. See mqttEnqueueProtoLocked and mqttAckUnsubscribe.
func (c *client) mqttEnqueueUnsubAck(pi uint16, filters []*mqttFilter) bool {
	if c.mqtt.proto == mqttProtoLevel5 {
		// v5 UNSUBACK adds a properties block and one reason code per filter.
		// Spec5 [3.11].
		w := newMQTTWriter(7 + len(filters))
		// Carry the first failed filter's detail as the packet-level Reason
		// String, when the client permits it. Spec5 [3.11.2.1.2].
		var reasonStr string
		for _, f := range filters {
			if f.reason != 0 && f.reasonStr != _EMPTY_ {
				reasonStr = f.reasonStr
				break
			}
		}
		props := c.mqttReasonProps(mqttPacketUnsubAck, reasonStr)
		frame := func() []byte {
			var vh mqttWriter
			vh.WriteUint16(pi)
			vh.writeProperties(props)
			for _, f := range filters {
				vh.WriteByte(f.reason)
			}
			w.Reset()
			w.WriteByte(mqttPacketUnsubAck)
			w.WriteVarInt(vh.Len())
			w.Write(vh.Bytes())
			return w.Bytes()
		}
		buf := frame()
		// Drop the Reason String (only) if it overflows the client's max.
		if props != nil && !c.mqttFitsClientMax(len(buf)) {
			props = nil
			buf = frame()
		}
		// If the reason-code payload alone still overflows, the UNSUBACK cannot be
		// sent [MQTT-3.1.2-24]; the unsubscribe side effects stand.
		return c.mqttEnqueueFittingProtoUnlessDisconnected(buf)
	}
	w := newMQTTWriter(4)
	w.WriteByte(mqttPacketUnsubAck)
	w.WriteVarInt(2)
	w.WriteUint16(pi)
	return c.mqttEnqueueProtoLocked(w.Bytes(), true)
}

func mqttUnsubscribeTrace(pi uint16, filters []*mqttFilter) string {
	var sep string
	sb := strings.Builder{}
	sb.WriteString("[")
	for i, f := range filters {
		sb.WriteString(sep)
		sb.Write(f.ttopic)
		sb.WriteString(" (")
		sb.WriteString(f.filter)
		sb.WriteString(")")
		if i == 0 {
			sep = ", "
		}
	}
	sb.WriteString(fmt.Sprintf("] pi=%v", pi))
	return sb.String()
}
