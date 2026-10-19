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

// The JetStream consumers behind subscriptions: plain, PUBREL and shared-group consumers,
// their creation, release, deferred deletion and pending purge.

package mqtt

import (
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nuid"
)

// cleanupFailedSubClassified is cleanupFailedSub reporting whether any record write it
// performed was rejected by the per-subject CAS. Same reason as
// releaseSharedConsumerClassified: this runs on a failed filter and returns normally, so a
// swallowed rejection would open the gate as quiet.
func (sess *mqttSession) cleanupFailedSubClassified(c *client, tok recordTx, fromSubProto bool, sub *subscription, cc *ConsumerConfig, jssub *subscription) bool {
	var casRejected bool

	if sub != nil {
		if uerr := c.processUnsub(sub.sid); uerr != nil {
			c.Warnf("MQTT: unable to remove subscription %q: %v", sub.sid, uerr)
		}
	}
	if jssub != nil {
		if uerr := c.processUnsub(jssub.sid); uerr != nil {
			c.Warnf("MQTT: unable to remove subscription %q: %v", jssub.sid, uerr)
		}
	}
	// On the CONNECT RESTORE path the consumer is left exactly as it is, and the
	// cleanup is the connection-local subscriptions alone.
	//
	// This used to dispose of it, which was survivable while a restore failure was
	// dropped and the CONNECT accepted anyway — the next resume's reconciliation
	// would repair a deleted plain durable. It is the wrong disposition now that
	// the failure REFUSES the CONNECT: the record still names that durable, its
	// backlog is retained by its interest, and nothing about this connection going
	// away makes it unwanted. The reachable shape is a multi-level filter whose
	// level-up companion fails after the main half succeeded, which arrives here
	// with the main half's config in hand.
	//
	// What this connection did add to tmaxack is refunded by the caller, per
	// filter, from the amounts the joins reported — never derived from cc, which
	// says nothing about who charged it. See sess_restore_gate.go.
	if cc != nil && fromSubProto {
		// A shared group consumer must never be deleted outright on a local
		// failure — other members may hold it. Route through the shared release,
		// which executes the verdicts the rollback wrote against the group record's
		// arbitrated count (the caller has already rolled back this session's
		// membership). Spec5 [4.8.2].
		if cc.DeliverGroup != _EMPTY_ {
			// The SAME own-hold fence the landed continuation applies, because this is the
			// other half of the same failed-filter release and the release it calls is
			// whole-durable. A session holding "$share/g/foo" that fails "$share/g/foo.>"
			// arrives here with the LEVEL-UP config, whose durable and deliver subject are
			// the surviving filter's: releasing it would close that subscription, purge its
			// pending identifiers and refund its charge, and nothing about the consumer's
			// own fate would stop any of it — the acquisitions' verdicts decide the consumer,
			// not this session's local state. The failing filter's own membership is already
			// rolled back on this path, so the live map IS the surviving membership and no
			// key needs excluding. F-082.
			sess.mu.Lock()
			stillOwn := sess.survivingSharedLocked(nil).ownsDurable(cc.Durable)
			sess.mu.Unlock()
			if !stillOwn {
				casRejected = sess.releaseSharedConsumerClassified(c, tok, cc) || casRejected
			}
		} else {
			// The failed filter's membership is rolled back: a decision, handed to the retirement
			// owner rather than a raw delete (F-105 7c-lc).
			sess.disposeSourceConsumer(c, cc, mqttDisposalRolledBack, "failed-filter")
		}
	}
	return casRejected
}

// Make sure we are set up to deliver PUBREL messages to this QoS2-subscribed
// session.
func (sess *mqttSession) ensurePubRelConsumerSubscription(c *client) error {
	if hook := mqttTestPubRelSubFailHookV.Load(); hook != nil {
		if err := (*hook)(); err != nil {
			return err
		}
	}

	sess.mu.Lock()
	pubRelSubscribed := sess.pubRelSubscribed
	pubRelDeliverySubjectB := sess.pubRelDeliverySubjectB
	pubRelDeliverySubject := sess.pubRelDeliverySubject
	pubRelConsumer := sess.pubRelConsumer
	pubRelVerified := sess.pubRelVerified
	sess.mu.Unlock()

	// Subscribe before the consumer is created so we don't loose any messages.
	if !pubRelSubscribed {
		// Same completion ownership as every other session delivery callback; this is
		// the one registered outside sess.processSub, so it wraps explicitly and the
		// source guard asserts the pair is exhaustive (F-036 unit 1).
		_, _, err := c.processSub(pubRelDeliverySubjectB, nil, pubRelDeliverySubjectB,
			sess.leasedHandler(mqttDeliverPubRelCb, false), false)
		if err != nil {
			c.Errorf("Unable to create subscription for JetStream consumer on %q: %v", pubRelDeliverySubject, err)
			return err
		}
		sess.mu.Lock()
		sess.pubRelSubscribed = true
		sess.mu.Unlock()
	}

	// A non-nil config proves only that a consumer once existed. It may have been
	// deleted out of band, or reaped by a stream-forced InactiveThreshold — and
	// because $MQTT_out is interest-retained, a session in that state stores PUBRELs
	// that are silently dropped and never delivers a PUBREL again. So the durable is
	// VERIFIED once per connection, with the same semantics as q2AffReady: verify,
	// create when absent (charging tmaxack only then), and fail closed when it
	// cannot be verified. Every restore starts pubRelVerified false.
	if pubRelConsumer != nil && pubRelVerified {
		return nil
	}
	if pubRelConsumer != nil {
		ci, lerr := sess.jsa.lookupConsumer(mqttOutStreamName, pubRelConsumer.Durable)
		if lerr != nil && isErrorOtherThan(lerr, JSConsumerNotFoundErr) {
			// Indeterminate: do NOT assume it is there and do not recreate over it.
			c.Errorf("Unable to verify the PUBREL consumer for client %q: %v", sess.id, lerr)
			return lerr
		}
		if ci != nil {
			sess.mu.Lock()
			sess.pubRelVerified = true
			sess.mu.Unlock()
			return nil
		}
		// Verified ABSENT: fall through and recreate it. It is charged below like any
		// other create, and the deterministic name means the recreate is the same
		// consumer this session's record already refers to.
		c.Warnf("MQTT: recreating the missing PUBREL consumer for client %q", sess.id)
	}

	opts := c.srv.getOpts()
	ackWait := opts.MQTT.AckWait
	if ackWait == 0 {
		ackWait = mqttDefaultAckWait
	}
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}

	sess.mu.Lock()
	pubRelSubject := sess.pubRelSubject
	// The durable is generation-scoped along with the two subjects, so a teardown
	// issued by a retired incarnation cannot delete this one. See
	// sess_incarnation.go.
	_, _, pubRelDurable := mqttPubRelNames(sess.idHash, sess.gen)
	pubRelGen := sess.gen
	tmaxack := sess.tmaxack
	id := sess.id
	sess.mu.Unlock()

	// Check that the limit of subs' maxAckPending are not going over the limit
	if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
		return fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
			after, mqttMaxAckTotalLimit)
	}

	ccr := &CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			DeliverSubject: pubRelDeliverySubject,
			Durable:        pubRelDurable,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			FilterSubject:  pubRelSubject,
			AckWait:        ackWait,
			MaxAckPending:  maxAckPending,
			MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
		},
	}
	// SAFE INACTIVITY CONFIG FROM THE OUTSET (F-105 §5.1 step 3): the PUBREL consumer is
	// enrolled, so it is created with NO inactivity threshold — its lifetime is its resource's,
	// and $MQTT_out is interest-retained, so a reap would silently drop stored PUBRELs. The
	// definitive effective-config verify is at admission.
	mqttClearConsumerInactivity(&ccr.Config)
	// CLAIM THE PUBREL NAME BEFORE THE CREATE (F-105 §5.1, Codex consult 11 step 2). Unlike a
	// source consumer's fresh-per-subscribe name, the PUBREL durable is GENERATION-SCOPED and
	// STABLE across recreates within a generation, so its ownership is claimed once and a
	// recreate (verified-absent, above) re-materialises the physical consumer under the STANDING
	// ownership without re-provisioning — the resource record is create-only and a second
	// provision would be refused. Provision only on the FIRST claim (ownership absent); a name
	// that stands LIVE for this generation is already provisioned; a RETIRED or foreign-owned
	// name is refused, because a spent name is never reused.
	pubRelAuth, _, praerr := sess.servingAuthority()
	if praerr != nil {
		return fmt.Errorf("client %q: its outbound authority is not serving, so its PUBREL consumer "+
			"name %q cannot be claimed: %w", id, pubRelDurable, praerr)
	}
	// THE NAME AND ITS OWNER ARE ONE GENERATION (F-105 7c-lc). The name is derived from the
	// session's generation and its ownership compared with the serving authority's; admission
	// derives the same name from the authority's. If the two ever differ, the claim below would
	// bind one generation's name to another's ownership, and a same-owner rematerialization — the
	// sanctioned reuse of this stable name — would no longer be same-owner.
	if pubRelGen != pubRelAuth.Gen {
		return fmt.Errorf("client %q: its PUBREL consumer name %q belongs to generation %q but the serving "+
			"authority is %q, so it cannot be claimed or rematerialized", id, pubRelDurable, pubRelGen, pubRelAuth.Gen)
	}
	pubRelOwn, _, pubRelOwnErr := sess.jsa.loadNameOwnershipPhysical(sess.asm.sessStreamEpoch,
		mqttOwnedStreamOut, pubRelDurable)
	if pubRelOwnErr != nil {
		return fmt.Errorf("client %q: reading the ownership of PUBREL consumer name %q: %w",
			id, pubRelDurable, pubRelOwnErr)
	}
	switch {
	case pubRelOwn == nil:
		if _, perr := sess.jsa.opProvisionResource(id, sess.asm.sessStreamEpoch, mqttNewInflightNonce(),
			"op-provision-pubrel", pubRelAuth.Gen, pubRelAuth.Tenure, mqttNewInflightNonce(), pubRelDurable,
			mqttResourcePubRelConsumer); perr != nil {
			return fmt.Errorf("client %q: claiming PUBREL consumer name %q: %w", id, pubRelDurable, perr)
		}
	case pubRelOwn.State != mqttNameOwnLive || pubRelOwn.OwnerGen != pubRelAuth.Gen:
		return fmt.Errorf("client %q: PUBREL consumer name %q stands in ownership state %d owned by "+
			"generation %q, not this generation's LIVE own: a spent or foreign name is never reused",
			id, pubRelDurable, pubRelOwn.State, pubRelOwn.OwnerGen)
	default:
		// A LIVE NAME IS NOT A SERVING RESOURCE (F-105 7f). The same-owner rematerialization is
		// sanctioned for the resource the name's owner names, and only while that resource is
		// LIVE without a disposal decision: recreating a decided or retiring consumer would
		// resurrect what its retirement is about to delete.
		r, _, rerr := sess.jsa.loadOutboundResource(id, sess.asm.sessStreamEpoch, pubRelAuth.Gen, pubRelOwn.OwnerInc)
		if rerr != nil {
			return fmt.Errorf("client %q: reading the resource of PUBREL consumer name %q: %w", id, pubRelDurable, rerr)
		}
		if r == nil || r.Kind != mqttResourcePubRelConsumer || r.State != mqttResourceLive || r.Disposal != nil {
			return fmt.Errorf("client %q: PUBREL consumer name %q names a resource that is not a LIVE, undecided "+
				"PUBREL consumer, so it is not rematerialized", id, pubRelDurable)
		}
	}
	if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
		c.Errorf("Unable to add JetStream consumer for PUBREL for client %q: err=%v", id, err)
		return err
	}
	pubRelConsumer = &ccr.Config

	sess.mu.Lock()
	sess.pubRelConsumer = pubRelConsumer
	// Charged only where it was created, never on a restore — the same rule the
	// plain and shared consumers follow. See refundConsumerChargeLocked.
	sess.chargeConsumerLocked(pubRelDurable, maxAckPending)
	sess.pubRelVerified = true
	sess.mu.Unlock()

	return nil
}

// When invoked with a QoS of 0, looks for an existing JS durable consumer for
// the given sid and if one is found, delete the JS durable consumer and unsub
// the NATS subscription on the delivery subject.
//
// With a QoS > 0, creates or update the existing JS durable consumer along with
// its NATS subscription on a delivery subject.
//
// Session lock is acquired and released as needed. Session is in the locked
// map.

// claimSourceConsumerName claims a fresh source consumer name's LIVE ownership, its resource
// incarnation and its duties, under the entitlement pinned at its first read. It is ANNOUNCED as a
// foreground authority operation for its whole span — the pinning read, every attempt and every
// contention wait — so this broker's retained background writers yield to it
// (outbound_foreground_priority.go), and for nothing else of the SUBSCRIBE.
//
// Session lock NOT held on entry.
func (sess *mqttSession) claimSourceConsumerName(ent mqttOutboundEntitlement, subject, durName string) (*mqttSourceClaim, error) {
	release := sess.announceForegroundAuthority()
	defer release()
	srcInc := mqttNewInflightNonce()
	if h := sess.testClaimPinned.Load(); h != nil {
		(*h)()
	}
	// THE CONNECTION'S ENTITLEMENT, never the authority standing when the claim reads it (F-105
	// 7f). A SUBSCRIBE whose connection a takeover displaced between its gate and here would
	// otherwise claim under its successor's tenure, labelling the attempt with an entitlement
	// that never decided it; opProvisionResource refuses an entitlement that moved.
	if ent.isZero() {
		return nil, fmt.Errorf("subscribing %q: this connection holds no accepted entitlement, so "+
			"its source consumer name %q cannot be claimed", subject, durName)
	}
	auth := ent
	claim := &mqttSourceClaim{gen: ent.Gen, name: durName, inc: srcInc, ent: ent}
	// RE-ATTEMPT UNDER THE PINNED ENTITLEMENT (F-105 cutover row 3, holistic review rounds
	// 2-3). The completion advance (opAdvanceExchangeAck) rewrites the authority on every
	// client acknowledgement, so a concurrent advance can move the standing authority — its
	// sequence, its inventory ledger, or both — between this claim's servingAuthority read and
	// its CAS. Any such contention wrote nothing, so it is settled the way the admission
	// ladder settles every write: a bounded re-attempt on ANY error after opProvisionResource
	// re-reads the authority itself, rather than a predicate that must enumerate each
	// pre-submit check that concurrency can trip (the sequence check, the inventory ledger).
	// OWNERSHIP IS PINNED: the connection's entitlement is passed on every attempt, so a
	// genuine takeover fails opProvisionResource's own gen/tenure guard ("moved on") every attempt and the loop exhausts its budget rather than
	// claiming under a successor. The claim is create-only over a fresh incarnation, so
	// re-using srcInc is safe; an attempt that in fact LANDED late is found by the handle
	// arbitration below and its incarnation disposed of (F-105 7c-lc).
	//
	// SPACED (F-105 7c, consult 104): a retained replay's materialization is a burst of
	// authority transitions on this same session, started by the previous SUBSCRIBE, and
	// unspaced re-attempts lose every one of them to it. The wait precedes the next attempt's
	// fresh reads and holds no session lock. It mitigates a burst; sustained contention can
	// still exhaust the budget and refuse the subscription (docs/CONFORMANCE.md).
	//
	// EVERY SUBMITTED ATTEMPT'S HANDLE IS KEPT (F-105 7c-lc). A failed attempt may still have
	// landed — its response lost, its batch queued — and a negative read proves nothing about a
	// claim that may land after it. So an exhausted claim is not abandoned: its handles go to the
	// retirement owner, which settles each by permanent arbitration (resolve, else abort) and
	// disposes of the incarnation if any landed.
	var perr error
	for attempt := 0; attempt < mqttSubscribeClaimAttempts; attempt++ {
		if attempt > 0 {
			sess.authorityContentionWait()
		}
		var out mqttOpOutcome
		out, perr = sess.jsa.opProvisionResource(sess.id, sess.asm.sessStreamEpoch,
			mqttNewInflightNonce(), "op-provision-source", auth.Gen, auth.Tenure, srcInc, durName,
			mqttResourceSourceConsumer)
		if out.Handle != nil {
			claim.handles = append(claim.handles, out.Handle)
		}
		if perr == nil {
			claim.handles = nil
			return claim, nil
		}
	}
	sess.custodyFailedClaim(claim)
	return nil, fmt.Errorf("subscribing %q: claiming source consumer name %q: %w",
		subject, durName, perr)
}

// filterKey is the session key of the filter this consumer delivers for — for a level-up
// companion, its MAIN filter's key — stamped on the delivery subscription for the No Local
// decision (see mqttSub.filterKey).
func (sess *mqttSession) processJSConsumer(c *client, tok recordTx, subject, sid, filterKey string,
	qos byte, noLocal, retainAsPublished bool, subID int, fromSubProto bool) (*ConsumerConfig, *subscription, *ConsumerConfig, error) {

	sess.mu.Lock()
	// A re-SUBSCRIBE to a filter that is DRAINING reclaims that drain rather than
	// creating anything. Because of I7 the drain is invisible in cons, so without
	// this the code below would create a SECOND durable on the same filter and
	// deliver every new message twice. It must therefore run before the cons
	// lookup. A QoS 0 re-SUBSCRIBE deliberately does not adopt: the drain keeps
	// draining independently while the raw subscription delivers new messages at
	// QoS 0. See unsub_drain.go.
	if qos > 0 {
		sess.adoptDrainLocked(sid)
	}
	// A QUARANTINED drain owns its sid EXCLUSIVELY. The refusal is enforced per FILTER in
	// processSubs, before anything is adopted, because a wildcard owns two sids and a
	// per-sid refusal here left the main drain adopted while the companion failed. This
	// second line stays because provisioning is reached from more than one caller.
	if hold := sess.drainSidHoldLocked(sid); qos > 0 && hold != mqttDrainNotHeld {
		sess.mu.Unlock()
		c.Errorf("MQTT: refusing to provision filter %q of client %q: a drained delivery on that "+
			"filter %s", sid, sess.id, hold.reason())
		return nil, nil, nil, fmt.Errorf("mqtt: filter %q has a held drained delivery", sid)
	}
	cc, exists := sess.cons[sid]
	tmaxack := sess.tmaxack
	idHash := sess.idHash
	rmax := sess.rmax
	sess.mu.Unlock()

	// Check if we are already a JS consumer for this SID.
	if exists {
		// If current QoS is 0, it means that we need to delete the existing
		// one (that was QoS > 0)
		if qos == 0 {
			// When processing a SUBSCRIBE packet, DEFER the teardown until the
			// packet's final session save is confirmed — and where the consumer
			// still holds started deliveries the packet CONVERTS it into a drain at
			// that save instead of tearing it down at all: the save can still fail
			// and its rollback must restore the pre-packet QoS1/2 state, and any
			// save issued mid-packet (a later shared filter's early persist) must
			// serialize a record consistent with reality — Subs still says QoS1/2
			// until the final update, so the consumer entry must stay alongside it.
			//
			// The cons ENTRY, though, goes one step earlier than the teardown: at
			// the final-update boundary, in the same critical section that applies
			// the QoS 0 membership, because the membership and the consumer
			// reference are one fact and the record must not carry the first while
			// still naming the second. See releaseDowngradedConsumersLocked. The
			// caller executes execDeferredConsumerDelete after the confirmed save.
			// Spec5 [4.8.2].
			//
			// The CONNECT RESTORE defers for the same reason, and it used to delete
			// INLINE here instead. That destroyed the started exchanges of a durable
			// a QoS 0 membership still named — the pair a failed drain adoption
			// leaves behind — because resume reconciliation collects positive-QoS
			// memberships only, so such an entry is never verified and arrives here
			// with nothing to convert at. There is no boundary INSIDE this function
			// at which the conversion could be made durable before the teardown, so
			// what belongs here is the deferral; the restore's own boundary in
			// processSubs owns the rest. [MQTT-3.1.2-4], [MQTT-4.4.0-1].
			return nil, nil, cc, nil
		}
		// If this is called when processing SUBSCRIBE protocol, then if
		// the JS consumer already exists, we are done (it was created
		// during the processing of CONNECT).
		if fromSubProto {
			// A re-SUBSCRIBE to an existing QoS 1/2 filter can change the QoS, the
			// No Local option, and the Subscription Identifier, all read by the QoS
			// 1/2 delivery callback off the delivery subscription. The durable
			// consumer itself is unchanged, so refresh those fields here so the new
			// values take effect. Spec5 [MQTT-3.8.4-3], [3.8.3.1], [3.8.2.1.2].
			c.mu.Lock()
			sub := c.subs[cc.DeliverSubject]
			c.mu.Unlock()
			if sub != nil {
				// The QoS 1/2 callback reads these under sess.mu; the QoS0
				// callback (on the shadow sub) under subsMu. Write under both, as
				// processSub does, so both readers see a consistent value.
				sess.mu.Lock()
				sess.subsMu.Lock()
				if sub.mqtt != nil {
					sub.mqtt.qos = qos
					sub.mqtt.noLocal = noLocal
					sub.mqtt.retainAsPublished = retainAsPublished
					sub.mqtt.subID = subID
				}
				sess.subsMu.Unlock()
				sess.mu.Unlock()
			}
			return nil, nil, nil, nil
		}
		// CONNECT restore of an entry the resume reconciliation phase already
		// verified (or repaired): its delivery subscription was installed CLOSED
		// and phase-owned, and it is adopted IN PLACE — metadata refreshed and the
		// gate opened under the same dual-lock write the re-SUBSCRIBE refresh
		// uses, on the SAME subscription object. Re-issuing it through processSub
		// would unsubscribe the old subscription and install a new one, putting a
		// no-interest gap right back into the flow — the exact stream-forced
		// inactivity race the phase's subscribe-first rule closes. Deliveries that
		// arrived while closed were skipped unacked; AckWait redelivers them now.
		// See sess_resume_reconcile.go.
		c.mu.Lock()
		phaseSub := c.subs[cc.DeliverSubject]
		c.mu.Unlock()
		if phaseSub != nil {
			adopted := false
			sess.mu.Lock()
			sess.subsMu.Lock()
			if phaseSub.mqtt != nil && phaseSub.mqtt.phaseOwned {
				phaseSub.mqtt.qos = qos
				phaseSub.mqtt.noLocal = noLocal
				phaseSub.mqtt.retainAsPublished = retainAsPublished
				phaseSub.mqtt.subID = subID
				phaseSub.mqtt.closed = false
				phaseSub.mqtt.phaseOwned = false
				// Released LAST, in the same critical section: a delivery waiting
				// on the gate re-reads every field above once it wakes, so the
				// backlog JetStream pushed before the CONNACK is delivered as the
				// first transmission it is, in order, with this filter's live
				// options. See mqttSub.phaseGate.
				mqttReleasePhaseGateLocked(phaseSub)
				adopted = true
			}
			sess.subsMu.Unlock()
			sess.mu.Unlock()
			if adopted {
				return cc, phaseSub, nil, nil
			}
		}
	}
	// Here it means we don't have a JS consumer and if we are QoS 0,
	// we have nothing to do.
	if qos == 0 {
		return nil, nil, nil, nil
	}
	var err error
	var inbox string
	// charged is the MaxAckPending this call adds to tmaxack, recorded against the
	// durable so the refund is exactly this and happens exactly once.
	var charged int
	// srcClaim is the name this attempt claimed, nil on the exists path: only it is ever
	// disposed of by this attempt's failure (F-105 7c-lc).
	var srcClaim *mqttSourceClaim
	if exists {
		inbox = cc.DeliverSubject
	} else {
		inbox = mqttSubPrefix + nuid.Next()
		opts := c.srv.getOpts()
		ackWait := opts.MQTT.AckWait
		if ackWait == 0 {
			ackWait = mqttDefaultAckWait
		}
		maxAckPending := int(opts.MQTT.MaxAckPending)
		if maxAckPending == 0 {
			maxAckPending = mqttDefaultMaxAckPending
		}
		// Pace JetStream delivery to the client's Receive Maximum so the
		// in-flight gate rarely drops at the cap (a drop waits out AckWait and
		// reorders). The gate stays the hard bound across subscriptions; a
		// consumer that outlives this connection keeps this cap until deleted.
		if rmax != 0 && int(rmax) < maxAckPending {
			maxAckPending = int(rmax)
		}

		// Check that the limit of subs' maxAckPending are not going over the limit
		if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
			return nil, nil, nil, fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
				after, mqttMaxAckTotalLimit)
		}

		durName := idHash + "_" + nuid.Next()
		// CLAIM THE NAME BEFORE THE CREATE (F-105 §5.1, Codex consult 11 step 2). A source
		// consumer's physical CONSUMER.CREATE is a queued request this broker can neither fence
		// nor retract, so an outbound obligation depends on the NAME, not on the create landing.
		// Claim the name's LIVE ownership, its resource incarnation (PENDING until sealed) and
		// its inventory duties as ONE authority batch, THEN create the durable. A create that
		// fails has its claimed incarnation disposed of and retired by the session's retirement
		// owner; one that lands late lands under a RETIRED name the plain sweep reclaims and
		// nothing can adopt. Nothing may depend on the consumer until it is proven usable, and
		// the name is never reused. See outbound_ops.go opProvisionResource.
		subEnt, _ := c.outboundEntitlement()
		claim, cerr := sess.claimSourceConsumerName(subEnt, subject, durName)
		if cerr != nil {
			return nil, nil, nil, cerr
		}
		srcClaim = claim
		ccr := &CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: ConsumerConfig{
				DeliverSubject: inbox,
				Durable:        durName,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				FilterSubject:  mqttStreamSubjectPrefix + subject,
				AckWait:        ackWait,
				MaxAckPending:  maxAckPending,
				MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
			},
		}
		// SAFE INACTIVITY CONFIG FROM THE OUTSET (F-105 §5.1 step 3, Codex consult 4/11). An
		// ENROLLED source consumer's lifetime is its resource's, not a timer's, so it is created
		// with NO inactivity threshold — clearing it after dependence begins cannot establish
		// preservation. The supported-configuration boundary (docs/CONFORMANCE.md) forbids
		// imposing an inactivity limit on a depended-on consumer throughout its lifetime. The
		// effective-config verify happens at ADMISSION (readyDependencyInc: the stream's limit
		// and the consumer's own reported threshold), because a stream pre-read alone is not an
		// atomic guarantee.
		mqttClearConsumerInactivity(&ccr.Config)
		var createErr error
		sess.asm.mu.RLock()
		preCreate := sess.asm.testSourcePreCreate
		sess.asm.mu.RUnlock()
		if preCreate != nil {
			createErr = preCreate(sess, durName)
		}
		if createErr == nil {
			_, createErr = sess.jsa.createDurableConsumer(ccr)
		}
		if err := createErr; err != nil {
			c.Errorf("Unable to add JetStream consumer for subscription on %q: err=%v", subject, err)
			// THE CLAIM LANDED and the create may yet: the incarnation is disposed of, so a late
			// CREATE lands under a RETIRED name no obligation can adopt (F-105 7c-lc).
			sess.handSourceDisposal(srcClaim.gen, srcClaim.name, srcClaim.inc,
				mqttDisposalProvisioningFailed, srcClaim.ent, "create-failed")
			return nil, nil, nil, err
		}
		cc = &ccr.Config
		// Record the charge against this durable so its teardown refunds exactly
		// this, exactly once. A consumer restored from the record reaches
		// processQOS12Sub below without passing here and is therefore never charged —
		// and must never be refunded either. See refundConsumerChargeLocked.
		charged = maxAckPending
	}

	// This is an internal subscription on subject like "$MQTT.sub.<nuid>" that is setup
	// for the JS durable's deliver subject.
	sess.mu.Lock()
	if charged > 0 {
		sess.chargeConsumerLocked(cc.Durable, charged)
	}
	sub, err := sess.processQOS12Sub(c, []byte(inbox), []byte(inbox), nil, filterKey,
		isMQTTReservedSubscription(subject), qos, noLocal, retainAsPublished, false, subID, cc.Durable, mqttDeliverMsgCbQoS12)
	sess.mu.Unlock()

	if err != nil {
		// ONLY A CONSUMER THIS ATTEMPT CLAIMED is disposed of (F-105 7c-lc). A restored
		// subscription's consumer (the exists path) is the persisted subscription's durable, and
		// a local subscription failure is not a decision to end it: the CONNECT that restores it
		// refuses instead.
		if srcClaim != nil {
			sess.mu.Lock()
			sess.refundConsumerChargeLocked(cc.Durable)
			sess.mu.Unlock()
			sess.handSourceDisposal(srcClaim.gen, srcClaim.name, srcClaim.inc,
				mqttDisposalProvisioningFailed, srcClaim.ent, "post-create-failed")
		}
		c.Errorf("Unable to create subscription for JetStream consumer on %q: %v", subject, err)
		return nil, nil, nil, err
	}
	return cc, sub, nil, nil
}

// execDeferredConsumerDelete performs the irreversible teardown of a plain
// QoS1/2 consumer whose session entry was already removed (QoS downgrade to 0):
// mark its delivery subscription closed, purge its pending QoS 1/2 deliveries
// (so the packet identifiers do not leak against the in-flight cap), delete the
// durable, and unsubscribe the delivery subject. deleteConsumer is asynchronous,
// so an in-flight delivery callback could re-populate the maps after the purge;
// the closed flag (guarded by sess.mu and sess.subsMu, same as delivery) makes
// mqttDeliverMsgCbQoS12 skip instead while leaving sub.mqtt valid for an
// already-committed enqueue.
func (sess *mqttSession) execDeferredConsumerDelete(c *client, tok recordTx, sid string, cc *ConsumerConfig, reason string) {
	_ = sess.execDeferredConsumerDeleteClassified(c, tok, sid, cc, reason)
}

// execDeferredConsumerDeleteClassified is execDeferredConsumerDelete reporting whether
// either of its record writes was rejected by the per-subject CAS.
//
// The live SUBSCRIBE path runs this inside an otherwise-SUCCESSFUL packet — the QoS
// downgrade whose teardown was deferred until the save was confirmed, and which the
// conversion did not claim as a drain — so a rejection here
// is exactly the swallowed evidence that would let the operation gate open as "quiet". See
// mqttSubPacketFacts.recordCASRejected.
func (sess *mqttSession) execDeferredConsumerDeleteClassified(c *client, tok recordTx, sid string, cc *ConsumerConfig, reason string) bool {
	if h := mqttTestDeferredDeleteHookV.Load(); h != nil {
		(*h)(sid, cc.Durable)
	}
	c.mu.Lock()
	sub := c.subs[cc.DeliverSubject]
	c.mu.Unlock()

	sess.mu.Lock()
	// The entry usually left sess.cons already, at the packet's final-update boundary
	// (releaseDowngradedConsumersLocked), which is what keeps the record that carries
	// the QoS 0 membership from naming this durable. Every save issued BEFORE that
	// boundary still persists it, and must: until the membership is applied, the
	// pre-packet QoS 1/2 subscription is the truth and its consumer belongs beside it.
	//
	// So this removal is not the only one, and it is not redundant either — the restore
	// path reaches here with no packet and no boundary, and a rollback may have put the
	// entry back. Remove it only when present and still pointing at this consumer.
	if cur, ok := sess.cons[sid]; ok && cur == cc {
		delete(sess.cons, sid)
	}
	if sub != nil && sub.mqtt != nil {
		sess.subsMu.Lock()
		sub.mqtt.closed = true
		sess.subsMu.Unlock()
	}
	regs, cancels := sess.purgeConsumerPendingLocked(cc.Durable, mqttLicenceOf(c))
	sess.mu.Unlock()
	casRejected := sess.deleteInflightRegistersClassified(c, tok, regs, mqttLicenceOf(c))

	// Anything the purge found here arrived AFTER this packet staged its cancellations
	// (a delivery already in the callback when the durable's subscription was closed),
	// so its cancellation missed the membership-removal write. Keeping the consumer
	// would not redeem that: the membership is gone either way, so a register that lands
	// has nothing to redeliver through whether the durable lives or not — and an
	// unreferenced durable nothing reaps is worse than none. So it is reported and the
	// obligation is left to the convergence points.
	secured, cancelCAS := sess.secureRegCancellationsClassified(c, tok, cancels, mqttLicenceOf(c))
	casRejected = casRejected || cancelCAS
	if !secured {
		c.Errorf("MQTT: the cancelled in-flight registration(s) of consumer %q for client %q "+
			"are not durably recorded yet; their identifiers stay reserved until the cleanup "+
			"is discharged", cc.Durable, sess.id)
	}
	// THE CONSUMER IS NOT DELETED HERE (F-105 7c-lc). An admitted obligation may still hold it —
	// a delivery outstanding when the drain was refused, a delivery admitted before the
	// membership save — and its replay depends on that physical name. The route decides; the
	// retirement owner retires the consumer once its holds are released, retiring its name in
	// the same batch, and records the classified delete. Handed off, never awaited: this runs
	// under the record transaction (deviation D1).
	sess.disposeSourceConsumer(c, cc, reason, "deferred-delete:"+reason)
	if sub != nil {
		if uerr := c.processUnsub(sub.sid); uerr != nil {
			c.Warnf("MQTT: unable to remove subscription %q: %v", sub.sid, uerr)
		}
	}
	return casRejected
}

// buildSharedGroupConsumerConfig is the SINGLE constructor for a shared group durable's
// configuration. Every name in it is derived from (group, subject, incarnation) through
// mqttSharedNames, so members across the cluster holding the group's live incarnation
// converge on one consumer.
//
// It is a named constructor rather than an inline literal because the join path BUILDS
// with it and the resume path VALIDATES against it. Two derivations of "the configuration
// this broker provisions" would drift, and a validator comparing against a drifted second
// copy would either refuse healthy groups or accept shapes the join path would never
// create. Same reasoning, and the same shape, as buildQ2AffineConsumerConfig.
//
// The three parameters are exactly the fields a legitimate member may differ on, and they
// are parameters rather than reads of this broker's options so the validator can substitute
// the LIVE consumer's values when it compares. Everything else is fixed here, which is what
// makes a whole-struct comparison meaningful.
//
// No InactiveThreshold is set, deliberately: an all-offline group must keep its backlog
// rather than be reaped. A recreate issued while the stream imposes a
// ConsumerLimits.InactiveThreshold would inherit one anyway (nats-server applies the stream
// limit to a consumer that requests zero, at create time), which is why the join path
// refuses under a live stream limit rather than creating a reapable durable.
func buildSharedGroupConsumerConfig(group, subject string, inc uint64, ackWait time.Duration,
	maxAckPending int, memoryStorage bool) ConsumerConfig {

	queue, deliver, durable := mqttSharedNames(group, subject, inc)
	return ConsumerConfig{
		DeliverSubject: deliver,
		DeliverGroup:   queue,
		Durable:        durable,
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverNew,
		FilterSubject:  mqttStreamSubjectPrefix + subject,
		AckWait:        ackWait,
		MaxAckPending:  maxAckPending,
		MemoryStorage:  memoryStorage,
	}
}

// processSharedJSConsumer creates (or joins) the single JetStream consumer that
// backs an MQTT 5.0 Shared Subscription group for a (ShareName, subject) pair,
// and subscribes this session to its deliver subject as a queue member so
// JetStream delivers each stored message to exactly one connected member.
//
// The durable, deliver subject, and queue/DeliverGroup are all derived from the group and
// the incarnation the acquisition holds (mqttSharedNames), so members across the cluster on
// the group's live incarnation converge on one consumer. All
// members join regardless of granted QoS: a QoS0 member still receives stored
// (QoS 1/2 published) messages through this consumer, delivered at QoS0 (the raw
// subject queue sub only carries QoS0-published messages). The consumer config
// is derived from server options only — never the member's Receive Maximum —
// since CreateOrUpdate would let each joiner rewrite the shared config. No
// InactiveThreshold is set, so an all-offline group stalls and keeps its backlog
// rather than being reaped. On the subscribe failure path the consumer is
// returned so the caller's cleanup can release it. Spec5 [4.8.2].
//
// Runs from the client's readLoop. sess.mu not held on entry.
// The third result is the MaxAckPending THIS INVOCATION added to tmaxack, and 0
// when it reused a resident cons entry instead. The caller needs the distinction:
// cc comes back on both paths, so a cleanup refunding by cc.Durable would erase a
// charge an EARLIER connection made and leave tmaxack permanently short.
// verdicts carries the CONNECT restore's batched group-durable classification, keyed by
// sid, and is nil on the live SUBSCRIBE path. A filter with no resident entry takes the
// create path either way, which proves the durable by creating it.
//
// F-076 justified the nil with "a live SUBSCRIBE cannot reach the resume arm, because a
// connected session with a cons entry already has its deliver subscription". THAT IS FALSE
// and is not reinstated. F-078 recorded the chain it was false about: a SUBSCRIBE whose
// rollback save failed DEFINITELY had `readd` restore cons[sid] while sharedSubs stayed at
// the pre-packet snapshot, so a reconnect built its filters from a sharedSubs that lacked
// the key, never collected the sid, and left the resident cons entry with no deliver
// subscription.
//
// THAT CHAIN IS CLOSED. The rollback's definite arm now restores the shared membership
// beside cons (restoreAttemptedShared, F-039), so the reconnect's filters do carry the key
// and the sid is collected. What closed it is the membership restoration, NOT F-076's
// claim, which remains false for the reason above.
//
// The branch is retained anyway, because no positive proof of unreachability exists: a
// post-resume live SUBSCRIBE takes the refresh arm, UNSUBSCRIBE's ambiguous window leaves
// the subscription present-but-closed rather than absent (its own roadmap item), and a
// failed CONNECT restore refuses the CONNECT (F-050). That is a survey of the routes we
// know, not a demonstration that none remains — an absence is not a fact. So this arm
// still verifies nothing when it is reached with nil verdicts, and it still subscribes on
// a remembered config. What it does not do is serve unarbitrated: the packet proves
// ownership after its last record write and withholds transmission until it lands (F-078,
// sub_operation_gate.go). The verification gap is local and stands; the arbitration gap
// does not, and F-078's trigger — interest touched — is unchanged. Do not restore the old
// justification.
func (sess *mqttSession) processSharedJSConsumer(c *client, group, subject, sid string,
	acq mqttSharedAcquisition, qos byte, retainAsPublished bool, subID int, fromSubProto bool,
	verdicts map[string]mqttSharedGroupVerdict) (mqttSharedJoinResult, error) {

	sess.mu.Lock()
	cc, exists := sess.cons[sid]
	tmaxack := sess.tmaxack
	sess.mu.Unlock()

	// repairing marks the S1 route: a resident entry whose durable the pre-pass proved
	// AUTHORITATIVELY ABSENT, which takes the join arm below to recreate it. It is kept
	// apart from `exists` because the charge decision differs — this session already holds
	// the historic charge for that durable, so re-charging would double-count tmaxack.
	var repairing bool

	// The names are the ACQUISITION's: the incarnation the group record mounted when this
	// membership acquired (or the standing membership's, on a refresh or a restore). This
	// function materializes and subscribes; it never decides which incarnation.
	queue, deliver, durable := mqttSharedNames(group, subject, acq.Inc)

	// Already a member with a live deliver subscription (re-SUBSCRIBE): just
	// refresh the mutable per-subscription fields read by the delivery callback.
	if exists {
		c.mu.Lock()
		sub := c.subs[cc.DeliverSubject]
		c.mu.Unlock()
		if sub != nil {
			sess.mu.Lock()
			sess.subsMu.Lock()
			if sub.mqtt != nil {
				sub.mqtt.qos = qos
				sub.mqtt.retainAsPublished = retainAsPublished
				sub.mqtt.subID = subID
			}
			sess.subsMu.Unlock()
			sess.mu.Unlock()
			// A refresh REVIVES delivery interest under a membership this operation has not
			// revalidated, so it raises the arbitration trigger exactly as a create does.
			return mqttSharedJoinResult{cc: cc, sub: sub, installed: true}, nil
		}
		// exists but no live deliver sub: a session resume on a new connection.
		//
		// This used to fall straight through to (re)subscribe, on the strength of a config
		// remembered from an earlier connection — which is F-076. Nothing proved the group
		// durable still existed, so a client whose durable had been deleted out of band was
		// told Session Present = 1 and handed a subscription on a subject nothing publishes
		// to, silently, on every reconnect while the condition persisted.
		//
		// The restore path now classifies every resident shared sid in one batch BEFORE
		// this runs and hands the verdicts in. Acting on the verdict here, before
		// processQOS12Sub, keeps the join path's own rule: a group durable must not deliver
		// to this member until the join is proven. See shared_group_verify.go.
		if v, judged := verdicts[sid]; judged {
			switch {
			case mqttSharedGroupVerdictIsRepairable(v):
				// AUTHORITATIVELY ABSENT, and this broker recreates it (F-077). The pre-pass
				// deliberately does not fail the filter for this verdict, so control reaches
				// here; `exists` is forced false so the join arm below creates the durable and
				// thereby proves it, exactly as a filter with no resident entry does.
				//
				// Only this verdict. A non-canonical durable may be serving other members and
				// is not this broker's to normalize on their behalf; an indeterminate reading
				// established nothing, and recreating on it turns one broker's transient error
				// into a group-wide change. Both keep refusing.
				c.Warnf("MQTT: recreating the absent shared group consumer for filter %q of "+
					"session %q (messages published while it was gone were not retained)",
					sid, sess.id)
				exists, repairing = false, true
			case v.err != nil:
				return mqttSharedJoinResult{}, v.err
			default:
				// Adopt the LIVE configuration in place of the remembered one. The remembered
				// copy was captured on an earlier connection, and a member on another broker may
				// legitimately have moved a variable field since; keeping it would finish the
				// CONNECT holding state the account no longer agrees with.
				sess.mu.Lock()
				sess.cons[sid] = v.cfg
				cc = v.cfg
				sess.mu.Unlock()
			}
		}
	}

	opts := c.srv.getOpts()
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}
	var charged int
	// created is THIS invocation's own fact, kept apart from charged because S1 repairs a
	// durable this session already holds the charge for. Correctness must never be keyed on
	// an accounting quantity: the verify create below and the arbitration trigger both read
	// created, while the refund reads charged.
	var created bool
	if !exists || repairing {
		// A stream-level consumer InactiveThreshold would give the group durable
		// a delete timer, reaping it while all members are offline. Check the
		// LIVE stream config (it can change after startup) and refuse rather
		// than create a reapable shared consumer. FAIL CLOSED: if the lookup
		// itself fails we cannot prove the limit is absent, so refuse the join
		// (the client can retry) instead of risking a reapable durable. Spec5 [4.8.2].
		si, err := sess.jsa.lookupStream(mqttStreamName)
		if err != nil || si == nil {
			return mqttSharedJoinResult{}, fmt.Errorf("shared subscriptions unavailable: unable to verify messages stream consumer limits: %v", err)
		}
		if si.Config.ConsumerLimits.InactiveThreshold != 0 {
			return mqttSharedJoinResult{}, fmt.Errorf("shared subscriptions unavailable: messages stream imposes a consumer inactivity limit")
		}
		ackWait := opts.MQTT.AckWait
		if ackWait == 0 {
			ackWait = mqttDefaultAckWait
		}
		// Whether this session ALREADY holds the charge for this durable decides both the
		// ceiling check and the charge itself, so it is sampled once, here, ahead of both.
		//
		// The two must move together. A repair recreates a durable this object was charged
		// for on an earlier connection, so it adds nothing to tmaxack — and a ceiling check
		// that still evaluated tmaxack + maxAckPending would refuse to repair a session
		// sitting near the limit, on the strength of a charge already counted inside
		// tmaxack. That refusal is permanent for as long as the session stays near capacity,
		// which turns a recoverable absent durable back into the F-076 dead end this item
		// exists to end.
		sess.mu.Lock()
		_, alreadyCharged := sess.sharedCharged[durable]
		sess.mu.Unlock()
		if !alreadyCharged {
			if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
				return mqttSharedJoinResult{}, fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
					after, mqttMaxAckTotalLimit)
			}
		}
		ccr := &CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: buildSharedGroupConsumerConfig(group, subject, acq.Inc, ackWait, maxAckPending,
				opts.MQTT.ConsumerMemoryStorage),
		}
		// Create before subscribing: a pre-existing group durable must not deliver
		// to (or be ack-consumed by) this member until the join is proven. Create
		// is idempotent (ActionCreateOrUpdate). Spec5 [4.8.2].
		if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
			c.Errorf("Unable to add shared JetStream consumer for subscription on %q: err=%v", subject, err)
			return mqttSharedJoinResult{}, err
		}
		cc = &ccr.Config
		created = true
		mqttTestSharedArbitratePause("after-group-create:" + sid)
		// A REPAIR does not charge: this session object already holds sharedCharged for
		// this durable from the connection that first joined the group, and charging again
		// would leave tmaxack permanently overcounted for the life of the session.
		if !alreadyCharged {
			sess.mu.Lock()
			sess.tmaxack = tmaxack + maxAckPending
			if sess.sharedCharged == nil {
				sess.sharedCharged = make(map[string]int)
			}
			sess.sharedCharged[durable] = maxAckPending
			sess.mu.Unlock()
			charged = maxAckPending
		}
	}

	sess.mu.Lock()
	sub, err := sess.processQOS12Sub(c, []byte(deliver), []byte(deliver), []byte(queue), _EMPTY_, // No Local never applies to a shared subscription [MQTT-3.8.3-4]
		isMQTTReservedSubscription(subject), qos, false, retainAsPublished, true, subID, durable, mqttDeliverMsgCbQoS12)
	sess.mu.Unlock()
	if err != nil {
		c.Errorf("Unable to create subscription for shared JetStream consumer on %q: %v", subject, err)
		// Return cc (only when we created it this call) so the caller's cleanup,
		// after rolling back the early-persisted membership, can release it.
		if created {
			return mqttSharedJoinResult{cc: cc, charged: charged, created: created}, err
		}
		return mqttSharedJoinResult{}, err
	}
	// There is deliberately NO verify-create here. It used to re-issue the create after the
	// membership was persisted, to recreate a durable a concurrent leaver's queued delete had
	// removed underneath the join. That race no longer exists: while this acquisition is
	// listed by the ledger, no retirement of its incarnation can be authorized (the count
	// cannot reach zero), and a delete issued for an OLDER incarnation names a different
	// consumer. A delete racing this join cannot reach this consumer. Spec5 [4.8.2].
	return mqttSharedJoinResult{cc: cc, sub: sub, charged: charged, created: created, installed: true}, nil
}

// releaseSharedConsumer removes this session's local delivery subscription and
// pending state for a shared-subscription group consumer, and executes the release
// verdicts the caller's record write carried — the group record's arbitrated count
// decides whether the consumer itself goes. Pending QoS 1/2 deliveries are purged
// WITHOUT acking, so any in-flight messages redeliver to the remaining group members
// via AckWait. The caller must have already removed the leaving filter from
// sess.sharedSubs, in the write that carries its verdicts. Spec5 [4.8.2].
//
// Runs from the client's readLoop. sess.mu not held on entry.
func (sess *mqttSession) releaseSharedConsumer(c *client, tok recordTx, cc *ConsumerConfig) {
	_ = sess.releaseSharedConsumerClassified(c, tok, cc)
}

// releaseSharedConsumerClassified is releaseSharedConsumer reporting whether either of its
// register record writes was rejected by the per-subject CAS.
//
// It is reached from a live SUBSCRIBE's FAILED-FILTER cleanup, which returns NORMALLY — so a
// rejection swallowed here leaves the packet looking quiet and opens the operation gate while
// the record has demonstrably moved. See mqttSubPacketFacts.recordCASRejected.
func (sess *mqttSession) releaseSharedConsumerClassified(c *client, tok recordTx, cc *ConsumerConfig) bool {
	if cc == nil || cc.DeliverGroup == _EMPTY_ {
		return false
	}
	return sess.releaseSharedGroupTargetClassified(c, tok, cc.Durable, cc.DeliverSubject)
}

// releaseSharedGroupTarget is releaseSharedConsumer keyed on the group consumer's NAMES
// rather than on a resident config.
//
// It exists because sess.cons is not the authority for what a FAILED shared filter must
// release: addJSConsToSess runs only once the whole filter has succeeded, so the join a
// failing packet made is not in cons at all, while its ack-budget charge, its pending
// deliveries and its deliver-subject subscription are already real. The names are derived
// from the membership's own acquisition references (mqttSharedGroupNamesFor), which is what
// lets the failed-filter release discharge them without a config. F-082.
//
// No lock held on entry.
func (sess *mqttSession) releaseSharedGroupTarget(c *client, tok recordTx, durable, deliverSubject string) {
	_ = sess.releaseSharedGroupTargetClassified(c, tok, durable, deliverSubject)
}

// releaseSharedGroupTargetClassified is releaseSharedGroupTarget reporting whether either of
// its register record writes was rejected by the per-subject CAS.
//
// The caller decides WHETHER this session owes the release; this decides only what the
// release consists of. Everything below is whole-durable — the purge, the refund and the
// unsubscribe — so a durable this session still holds through another of its own shared keys
// must not reach here. BOTH halves of the failed-filter release apply that fence themselves
// (releaseFailedSharedFilter for the landed continuation, cleanupFailedSubClassified for the
// confirmed inline path); the other callers do not, and that is the defect F-083 is filed
// for, not an invariant this function may assume. F-082.
func (sess *mqttSession) releaseSharedGroupTargetClassified(c *client, tok recordTx,
	durable, deliverSubject string) bool {

	casRejected := sess.discardSharedGroupTargetStateClassified(c, tok, durable, deliverSubject)
	sess.executeSharedReleases(c)
	return casRejected
}

// discardSharedGroupTargetStateClassified is the FIRST half of a group-target release: everything
// that touches this session's own state and the record, and nothing that is irreversible for
// anybody else. It closes the delivery, purges the pending entries, refunds the ack-budget
// charge, discharges the register-stream obligations and unsubscribes the deliver subject, and
// it reports whether any of its record writes was rejected by its per-subject CAS.
//
// It is split from the delete for ONE caller's sake, and the reason is a correctness ordering
// rather than tidiness: this half performs record writes, and those writes can leave the session
// unresolved or arm the rejection marker. A caller that must not destroy another owner's durable
// therefore needs to settle the ownership question BETWEEN the two halves — after every write it
// is going to make, before the first thing it cannot undo. Callers with no such requirement
// compose the two back to back through releaseSharedGroupTargetClassified and see no change.
//
// sess.mu must NOT be held: this takes it.
func (sess *mqttSession) discardSharedGroupTargetStateClassified(c *client, tok recordTx,
	durable, deliverSubject string) bool {

	if durable == _EMPTY_ {
		return false
	}
	// CLOSE BEFORE PURGE, in ONE critical section, and that ordering is the whole reason this
	// looks fussier than it reads. A delivery callback takes sess.mu too: purge first and release
	// the lock, and a callback entering the gap observes closed=false, repopulates
	// pendingPublish/cpending, and proceeds toward storing its register — an entry absent from
	// regs, from cancels and from voidedRegNonces, after which the consumer may be deleted.
	// Splitting these two apart is exactly the regression a holistic round caught.
	var sub *subscription
	if c != nil && deliverSubject != _EMPTY_ {
		c.mu.Lock()
		sub = c.subs[deliverSubject]
		c.mu.Unlock()
	}
	sess.mu.Lock()
	sess.closeSharedGroupTargetDeliveryLocked(sub)
	regs, cancels := sess.purgeConsumerPendingLocked(durable, mqttLicenceOf(c))
	sess.refundSharedGroupChargeLocked(durable)
	sess.mu.Unlock()
	if c != nil && sub != nil {
		if uerr := c.processUnsub(sub.sid); uerr != nil {
			c.Warnf("MQTT: unable to remove subscription %q: %v", sub.sid, uerr)
		}
	}
	casRejected := sess.deleteInflightRegistersClassified(c, tok, regs, mqttLicenceOf(c))
	// As in execDeferredConsumerDelete: anything found here arrived after the staging,
	// and keeping the durable would not make its identifier recoverable.
	secured, cancelCAS := sess.secureRegCancellationsClassified(c, tok, cancels, mqttLicenceOf(c))
	casRejected = casRejected || cancelCAS
	if !secured && c != nil {
		c.Errorf("MQTT: the cancelled in-flight registration(s) of shared consumer %q for client %q "+
			"are not durably recorded yet; their identifiers stay reserved until the cleanup "+
			"is discharged", durable, sess.id)
	}
	return casRejected
}

// discardSharedGroupTargetLocalState is the part of a group-target release that touches only
// process-local state: it closes the delivery so nothing more is transmitted on it, refunds the
// ack-budget charge, and unsubscribes the deliver subject.
//
// It performs NO record write, which is why a caller that must order its writes against
// something irreversible can call it freely at any point. Separated for that caller's sake: the
// 3.1.1 forced leave collects every target's pending state in one pass and issues ONE aggregate
// register/record write pair, because a per-target write pair cannot be made safe — an ambiguous
// first write causes `persistRecord` to refuse every later one at entry, and resolving the first
// credits only its own revision.
//
// sess.mu must NOT be held: this takes it.
// closeSharedGroupTargetDeliveryLocked marks one group target's delivery closed, so nothing more
// is transmitted on it and no callback repopulates its pending state.
//
// It MUST run in the same sess.mu critical section as the purge that follows, and before it: a
// delivery callback takes sess.mu, so any gap between the two lets a callback observe an open
// subscription, reinstate a pending entry and head toward storing a register that the purge has
// already walked past.
//
// sess.mu must be held.
func (sess *mqttSession) closeSharedGroupTargetDeliveryLocked(sub *subscription) {
	if sub == nil || sub.mqtt == nil {
		return
	}
	sess.subsMu.Lock()
	sub.mqtt.closed = true
	sess.subsMu.Unlock()
}

// refundSharedGroupChargeLocked returns one group durable's MaxAckPending charge to the session's
// budget. Keyed by durable, which is what makes it right for a membership-only target: refunding
// by cons entry missed every durable no config named.
//
// sess.mu must be held.
func (sess *mqttSession) refundSharedGroupChargeLocked(durable string) {
	amt, ok := sess.sharedCharged[durable]
	if !ok {
		return
	}
	sess.tmaxack -= amt
	if sess.tmaxack < 0 {
		sess.tmaxack = 0
	}
	delete(sess.sharedCharged, durable)
}

// executeSharedReleases is the SECOND half: the irreversible one, and it is no longer a
// decision this session makes about other holders.
//
// Every acquisition this session released is a VERDICT in its record (written by the same
// transaction that removed the membership, see updateWithDetailConverting), and this executes
// the verdicts: one ledger+group CAS per acquisition that removes exactly that entry and
// decrements the group's holder count, and — when the count reaches zero — retires the
// incarnation and deletes its consumer. No walk of other sessions' records decides anything:
// whether the consumer goes is the group record's arbitrated count, and because the name
// carries the incarnation a late delete can never reach a successor a joiner mounted meanwhile.
// Because $MQTT_msgs is interest-retained, retiring an incarnation destroys its backlog, which
// is exactly what a group nobody holds is owed. Spec5 [4.8.2].
//
// Executed only after the record write carrying the verdicts is CONFIRMED (the callers' landed
// paths and continuations), never speculatively.
func (sess *mqttSession) executeSharedReleases(c *client) {
	sess.executeOwnVerdicts(c)
}

// purgeSharedPending drops this session's pending QoS 1/2 delivery state for its
// shared group consumers WITHOUT acking. Called when the session survives a
// disconnect, so a resume does not NAK-steal group messages (which AckWait
// redelivers to other members) and so the packet identifiers are freed. Spec5 [4.8.2].
//
// The targets are derived from the pending state itself, not from either view of the
// group membership, because both views provably miss real shapes. A session restored
// from its record has membership without configs — shared configs are excluded from
// the persisted Cons (buildPersistedSessionLocked) and rebuilt only by processSubs —
// while recoverInflightRegisters has already rebuilt its pending entries under the
// incarnation-qualified group durable names the registers carry, so a cons walk purges
// nothing. (The SUBSCRIBE
// rollback used to supply the mirror case — a config outside the membership, because its
// definite arm restored cons and not sharedSubs — and no longer does: F-039 restores both.
// The derivation below is unchanged, and must be: it never rested on that shape alone.)
// Transmitted entries with NEITHER view left are reachable, which is why even the union is
// not enough.
//
// Both operations that used to STRAND that shape now discharge it at the membership removal
// rather than leaving it to this close: UNSUBSCRIBE's no-cons branch (F-083) and the 3.1.1
// forced leave, which stages its void and purges its pending state over the same
// pending-map derivation this function uses (sharedPendingDurablesLocked). This purge remains
// the backstop for a session that simply disconnects.
//
// For broker-minted state the durable name IS the protocol fact "deliveries under this
// consumer are group deliveries": mqttSharedNames is the single source of the prefix, and
// no other class can collide (plain consumers are idHash_nuid, PUBREL and affine QoS 2
// copies carry their own prefixes, and a drained consumer keeps its plain name). For
// state restored from a register the name is trusted, the same trust every other restored
// identity gets.
func (sess *mqttSession) purgeSharedPending(lic mqttDestroyLicence) {
	regs, cancels := sess.collectSharedPendingPurge(lic)
	// No consumer is destroyed here — the group durable survives. Only the identifiers of
	// PROVEN-UNSENT allocations are given up (a register that landed afterwards would have the
	// next resume reserve one the session no longer holds); a possibly-transmitted delivery keeps
	// its identifier, its custody and its source mapping, so its resume retransmits it
	// (mqttPurgeSurvivingDisconnect).
	sess.secureRegCancellations(nil, recordTx{}, cancels, lic)
	sess.deleteInflightRegisters(nil, recordTx{}, regs, lic)
}

// purgeSharedPendingStaged is purgeSharedPending for a caller that owns the record
// write: the in-memory purge and the register-stream rungs run exactly as they do for
// the ordinary close, but a fallback verdict is only STAGED in memory — no record write
// happens here — and the caller's own classified save carries it. It reports whether any
// verdict is so owed.
//
// The deferred close is that caller, and the split is what makes its write ONE
// operation: a purge that saved for itself could land ambiguously and leave the
// caller's own save refused, with its parked close obligations already consumed. See
// applyDeferredDisconnect.
//
// Session lock NOT held on entry: the rungs issue JetStream requests.
func (sess *mqttSession) purgeSharedPendingStaged(c *client, lic mqttDestroyLicence) bool {
	regs, cancels := sess.collectSharedPendingPurge(lic)
	owed := sess.tombstoneCancellationsOnStream(c, cancels, lic)
	if sess.voidRegistersOnStream(c, regs, lic) {
		owed = true
	}
	return owed
}

// sharedPendingDurablesLocked returns every shared group durable this session still holds
// PENDING state for, derived from the pending maps rather than from either view of the group
// membership. It is collectSharedPendingPurge's derivation with the purge left out, for a
// caller that needs the target NAMES before it has decided to drop anything.
//
// The two structural views provably miss real shapes, which is why this exists: a session
// restored from its record has membership without configs, so a cons walk names nothing.
// (A rolled-back SUBSCRIBE used to supply the mirror shape — a config outside the
// membership — and no longer does, since its definite arm restores the shared membership
// beside cons, F-039. The derivation is unchanged and never rested on that shape alone.)
// The 3.1.1 forced leave needs the union of all three, and it needs it BEFORE its
// record write rather than after — the write that removes the membership is the write that
// must carry "these allocations were never transmitted" (see voidRegistrationsLocked).
//
// The durable NAME is the protocol fact, exactly as it is for the purge: mqttSharedNames is
// the single source of the prefix and no other class can collide with it.
//
// sess.mu must be held.
func (sess *mqttSession) sharedPendingDurablesLocked() []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(durable string) {
		if !strings.HasPrefix(durable, mqttSharedConsumerDurablePrefix) {
			return
		}
		if _, dup := seen[durable]; dup {
			return
		}
		seen[durable] = struct{}{}
		out = append(out, durable)
	}
	// The same three sources collectSharedPendingPurge walks, for the same reason: every map
	// purgeConsumerPendingLocked removes from is a source of targets.
	for durable := range sess.cpending {
		add(durable)
	}
	for k := range sess.deferred {
		add(k.jsDur)
	}
	for key := range sess.srcAckOwed {
		add(key.jsDur)
	}
	return out
}

// collectSharedPendingPurge drops the pending state in memory and reports what the
// register stream still owes for it: the registers to delete and the cancellations to
// tombstone. It is the derivation both purge forms share, and the rule it encodes is
// the load-bearing part — see purgeSharedPending.
//
// Session lock NOT held on entry.
func (sess *mqttSession) collectSharedPendingPurge(lic mqttDestroyLicence) ([]mqttVoidedReg, []mqttRegCancellation) {
	sess.mu.Lock()
	var regs []mqttVoidedReg
	var cancels []mqttRegCancellation
	purged := make(map[string]struct{})
	purge := func(durable string) {
		if !strings.HasPrefix(durable, mqttSharedConsumerDurablePrefix) {
			return
		}
		if _, dup := purged[durable]; dup {
			return
		}
		purged[durable] = struct{}{}
		pregs, pcancels := sess.purgeConsumerPendingModeLocked(durable, mqttPurgeSurvivingDisconnect, lic)
		regs = append(regs, pregs...)
		cancels = append(cancels, pcancels...)
	}
	// Every map purgeConsumerPendingLocked removes from is a source of targets, so no
	// pending entry it would drop is out of the collection's reach. A retirement has no
	// cpending entry by construction, and a withheld first transmission may outlive the
	// emptying of its consumer's cpending bucket.
	for durable := range sess.cpending {
		purge(durable)
	}
	for k := range sess.deferred {
		purge(k.jsDur)
	}
	for key := range sess.srcAckOwed {
		purge(key.jsDur)
	}
	sess.mu.Unlock()
	return regs, cancels
}

// mqttSharedDurablesFor returns the group durable name(s) a shared subscription membership
// maps to: the main durable, plus the level-up durable for a "foo.>" filter ('#' semantics).
// Nil if the key does not parse.
func mqttSharedDurablesFor(key string, m mqttSharedMembership) []string {
	targets := mqttSharedGroupNamesFor(key, m)
	if len(targets) == 0 {
		return nil
	}
	durs := make([]string, 0, len(targets))
	for _, t := range targets {
		durs = append(durs, t.durable)
	}
	return durs
}

// mqttSharedGroupNames is one group consumer a shared filter joins, with the deliver
// subject that consumer pushes to.
//
// The pair travels together because a caller releasing what a filter joined needs BOTH: the
// durable names the consumer, the pending state and the ack-budget charge, while the deliver
// subject names this session's subscription to it. Deriving one and rediscovering the other
// through sess.cons is what F-082 had to fix: addJSConsToSess runs only once the whole filter
// succeeds, so a cons walk cannot see the join a FAILING filter just made. It may still find a
// PRE-EXISTING entry for that durable, which is why the caller sweeps every match rather than
// treating absence as given. The SUBSCRIBE rollback's definite arm was the illustration here
// and is no longer one (F-039 restores the membership beside cons); the sweep is kept, since
// closing one producer proves nothing about the rest.
type mqttSharedGroupNames struct {
	durable string
	deliver string
}

// mqttSharedGroupNamesFor returns the group consumer(s) a shared-subscription membership
// joins: the subject's own, on the incarnation the membership's main acquisition names, plus
// its level-up parent for a multi-level filter (which needs a second consumer for the "a/#"
// matches "a" rule), on the incarnation its level-up acquisition names.
//
// THE single derivation of that mapping. mqttSharedDurablesFor projects the durables out of
// it and mqttSharedOwnsDurable tests membership against it, so a change to what a key joins
// cannot leave one of the three disagreeing with the others. A multi-level key whose
// membership carries no level-up acquisition yields the main target alone: the validators
// refuse such a membership everywhere it is read, and this derivation never invents an
// incarnation.
func mqttSharedGroupNamesFor(key string, m mqttSharedMembership) []mqttSharedGroupNames {
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		return nil
	}
	_, deliver, dur := mqttSharedNames(group, subject, m.Main.Inc)
	targets := []mqttSharedGroupNames{{durable: dur, deliver: deliver}}
	if mqttNeedSubForLevelUp(subject) && m.Up != nil {
		_, ldeliver, ldur := mqttSharedNames(group, subject[:len(subject)-2], m.Up.Inc)
		targets = append(targets, mqttSharedGroupNames{durable: ldur, deliver: ldeliver})
	}
	return targets
}

// mqttParseSharedKey splits a shared-subscription session key
// ("$share/{ShareName}/{NATS subject}") back into its ShareName and subject.
func mqttParseSharedKey(key string) (group, subject string, ok bool) {
	if !strings.HasPrefix(key, string(mqttSharedSubPrefix)) {
		return _EMPTY_, _EMPTY_, false
	}
	rest := key[len(mqttSharedSubPrefix):]
	idx := strings.IndexByte(rest, mqttTopicLevelSep)
	if idx <= 0 || idx == len(rest)-1 {
		return _EMPTY_, _EMPTY_, false
	}
	return rest[:idx], rest[idx+1:], true
}

// mqttSharedOwnsDurable reports whether a session holding the given shared subscription
// membership owns the given group durable. A "foo.>" filter owns both its own durable and
// the level-up ("foo") durable created for '#' semantics.
func mqttSharedOwnsDurable(key string, m mqttSharedMembership, durable string) bool {
	for _, t := range mqttSharedGroupNamesFor(key, m) {
		if t.durable == durable {
			return true
		}
	}
	return false
}
