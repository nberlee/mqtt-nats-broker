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

// Test hooks of the protocol core: atomic hook variables production code consults at named
// points, and their setters. Production never sets one; a nil hook is a no-op.

package mqtt

import (
	"sync/atomic"
	"time"
)

// Test hook: called once sweepSessionExpiries has finished a full pass over the
// records. A test seeding an already-overdue record needs to know that the
// STARTUP pass is behind it — that pass is launched asynchronously from the
// account manager's own goroutine with no completion signal, so a test that
// merely waited would race it arming an immediate expiry timer and deleting the
// fixture out from under the assertion.
//
// Atomic for the same reason as the save hook: the sweep goroutine outlives the
// test that created its account manager.
var mqttTestSessExpirySweepPassHookV atomic.Pointer[func(as *mqttAccountSessionManager)]

// Test hook: when set, sess.save() consults it and fails with the returned error
// instead of persisting. Used to exercise persistence-failure rollback.
//
// Held in an atomic because saves run on server goroutines that outlive the
// client connection a test just closed: clearing a plain global in a deferred
// cleanup races a disconnect-path save still in flight.
var mqttTestSessSaveHookV atomic.Pointer[func(sess *mqttSession) error]

// Test hook: counts every CONNACK the broker ATTEMPTS to enqueue, whatever becomes of it.
//
// The count is the observable a wire read cannot provide. A test asking "did this refusal tell
// the client anything?" must distinguish "never enqueued" from "enqueued into a socket that was
// already closing", and only the first is a site that stays silent by design. Reading the wire
// conflates the two, which lets a mutation that signals from the wrong place look correct.
var mqttTestConnAckAttemptHookV atomic.Pointer[func(rc byte)]

// Test hook: fired when the 3.1.1 forced leave's COMMIT completes, whichever arm ran it.
//
// It exists because the commit's effects are all maskable by something else: the in-memory
// membership is nil'd by the STAGING half whether or not the commit runs, and the group durable
// it releases can also be deleted later by the orphaned-consumer sweep. A test that watched
// either would pass against a broker that resolved its ambiguity and then skipped the commit
// entirely, which is precisely the mistake registering the landed continuation too late makes.
var mqttTestForcedLeaveCommitHookV atomic.Pointer[func()]

// Test hook: fired at the entry to the forced leave's PHASE 3 — after every record write it
// makes and after the ownership settlement, immediately before the release verdicts are
// executed (the ledger+group CASes that may retire an incarnation and delete its consumer).
//
// It exists because the invariant here is an ORDER, and no state check can express it: a test
// asking "was the durable deleted?" cannot distinguish "the code correctly declined" from "the
// code tried and the scan happened to say held". This hook makes the attempt itself observable,
// so a test can assert that an unsettled record state means phase 3 is never entered at all.
var mqttTestForcedLeaveReleaseHookV atomic.Pointer[func()]

// Test hook: fired immediately BEFORE the forced leave's ownership settlement.
//
// Its sibling fires immediately after, and the pair exists because the commit has TWO independent
// gates in front of its irreversible phase — the ownership settlement and the void-verdict
// durability check — and a fixture that enters the state after the first one cannot kill removing
// the first one. One hook per gate, so each is pinned by its own test.
var mqttTestForcedLeavePreSettleHookV atomic.Pointer[func(sess *mqttSession)]

// Test hook: fired AFTER the forced leave's ownership settlement and BEFORE its durability
// check, which is the one instant where a test can make a verdict owed that the settlement has
// already passed.
//
// It exists because the phase-3 authorization boundary is otherwise unreachable: reproducing a
// cleanup write that goes ambiguous *and* leaves a verdict uncredited needs a fixture nobody has
// built, and without one, removing or moving the durability gate is killed by nothing. Staging an
// uncredited nonce here is the smallest deterministic stand-in for that state.
var mqttTestForcedLeavePostSettleHookV atomic.Pointer[func(sess *mqttSession)]

// Test hook: when set, processQOS12Sub fails for the named durable. It exists
// because the delivery-subscription failures this broker must survive —
// restoreDrains leaving a drain un-indexed, a reclaim that cannot install — have no
// TARGETABLE natural trigger. A closed connection fails everything at once, and a
// Flush timeout is not aimable at one durable either, so neither could show a single
// drain failing while the rest of the packet proceeded. Keyed by durable, this can.
//
// Atomic for the same reason as the save hook above.
var mqttTestQOS12SubFailHookV atomic.Pointer[func(durable string) error]

// Test hook: when set, ensurePubRelConsumerSubscription fails. It is the producer of
// the blanket QoS 2 withholding — qos2DrainDurables puts EVERY QoS 2 drain into the
// resume's skip set when PUBREL setup fails, including drains carrying only
// PUBLISH-phase claims, which no hold covers.
//
// Atomic for the same reason as the save hook above.
var mqttTestPubRelSubFailHookV atomic.Pointer[func() error]

// Test hook: when set, it is invoked in mqttDeliverMsgCbQoS12 immediately BEFORE the
// callback takes sess.mu. That is the only point from which a test can name the
// interleaving drain retirement has to survive — a callback already dispatched by the
// client library, holding a valid *subscription, and not yet inside the critical section
// where it re-checks sub.mqtt.closed and looks its drain up. Precedent:
// setMQTTTestTryBeginPreLockHook.
//
// Atomic for the same reason as the save hook above.
var mqttTestDeliveryPreLockHookV atomic.Pointer[func(sub *subscription)]

// Test hook: fired at the ENTRY of mqttDeliverMsgCbQoS12 for every JetStream-backed QoS 1/2
// delivery, with the ack REPLY, before any disposition is taken: the transmission gates, the
// terminal-source hold, the drain branch and the send are all downstream of it. It observes
// that a source OFFER reached this broker's callback, which is a different fact from a
// transmission: a re-offer correctly withheld writes nothing, so a send-side seam
// (mqttTestInflightPreSendHookV) never fires for it, and the pre-lock hook carries no reply and
// so no stream sequence. A source still unacknowledged after AckWait is re-offered, and this is
// the seam that witnesses such a re-offer arriving at a given broker's callback.
//
// Atomic for the same reason as the save hook above.
var mqttTestDeliveryOfferedHookV atomic.Pointer[func(sub *subscription, reply string)]

// Test hook: fired in mqttDeliverMsgCbQoS12's CLOSED arm, immediately before it returns
// without acking. It carries the ack REPLY subject as well as the subscription, which is
// what the pre-lock and done hooks above cannot do — they take only a *subscription.
//
// Two facts at once, and both are needed by the same test: that the skip happened, observed
// positively rather than by an absence, and the subject to NAK so a re-offer is scheduled
// deterministically instead of waiting out AckWait. A NEW hook rather than a widened
// signature so no existing caller or test changes.
//
// Atomic for the same reason as the save hook above.
var mqttTestDeliverySkippedHookV atomic.Pointer[func(sub *subscription, reply string)]

// mqttTestSubPauseV pauses a LIVE SUBSCRIBE packet at one named boundary of its filter
// loop, so a test can act while the packet is in a specific state. The boundaries are the
// ones this item's ordering claims live across.
//
// LOCK OWNERSHIP IS PER LABEL and a fixture must read it here before acting: three of
// these run with NO session lock held, and one runs UNDER sess.mu, where a hook that
// acquires sess.mu deadlocks and one that expects a delivery to make progress waits
// forever.
//
//	"staged:<sid>"        no locks held. After one occurrence staged a deferred
//	                      downgrade: the delivery barrier is down, the packet runs on
//	                      "provisioned:<sid>"   no locks held. After one filter's installs
//	                      are complete on a live SUBSCRIBE, later filters not started
//	"revoked:<sid>"       no locks held. After a later occurrence revoked that staging and
//	                      before processJSConsumer's refresh arm re-applies the same terms
//	"converted:<sid>"     **sess.mu HELD.** Fired from inside the final-update boundary,
//	                      after a downgrade's consumer became a drain and before that
//	                      record is saved — the whole point of the boundary is that the
//	                      membership and the map move are one critical section, so the
//	                      hook cannot take sess.mu, cannot wait on a delivery callback,
//	                      and must only read what it is handed or store into an atomic
//
// The second is what makes "the terms are installed WITH the reopen" testable at all: a
// post-SUBACK publication cannot distinguish the revocation's write from the refresh's.
//
// EVERY occurrence of a filter reaches these, so a fixture that means one occurrence counts
// its own calls. Modelled on mqttTestRestorePauseHook, which does the same job for CONNECT.
var mqttTestSubPauseV atomic.Pointer[func(at string)]

func mqttTestSubPauseHook(at string) {
	if h := mqttTestSubPauseV.Load(); h != nil {
		(*h)(at)
	}
}

// mqttTestDeliveryAdmittedPauseV holds an ordinary QoS 1/2 delivery between its LOCALLY CONFIRMED
// admission and its send section: the exchange is admitted and marker-backed, and nothing has been
// reserved or written. It is the one window in which a SUBSCRIBE rollback meets an admitted,
// unsent, unopened exchange — the hold that withdrawal, not retransmission, ends. Nil in
// production; no lock held when it runs, so it may block.
var mqttTestDeliveryAdmittedPauseV atomic.Pointer[func(sess *mqttSession, pi uint16)]

// Test hook: fired from a DEFER registered beside the pre-lock hook, so it runs when the
// delivery callback returns through any of its exits below that point. It is the completion
// signal a negative-delivery assertion needs; see the paired seam in mqttDeliverMsgCbQoS12.
//
// Atomic for the same reason as the save hook above.
var mqttTestDeliveryDoneHookV atomic.Pointer[func(sub *subscription)]

// Test hook: fired once per execution of a consumer teardown a SUBSCRIBE packet deferred.
// It COUNTS, and that is why it exists: the teardown's observable effects are idempotent —
// a second run finds no cons entry, no pending registrations and an already-deleted durable
// — so "it ran twice for one consumer" is invisible to every assertion on state, and the
// at-most-one-live-staging invariant could not otherwise be pinned.
//
// Atomic for the same reason as the save hook above.
var mqttTestDeferredDeleteHookV atomic.Pointer[func(sid, durable string)]

// Test hook: the retired-drain consumer delete seam. It OBSERVES THE REAL noWait ARGUMENT,
// which is the point — a seam that only substituted an error could be satisfied by a
// helper-level timeout returning the same thing whether the request waited for an answer or
// not, so it could not discriminate a reversion to the fire-and-forget form.
//
// send=false suppresses the request; override=true supplies observed as the outcome to
// classify instead of issuing one. The ephemeral path's setMQTTTestConsumerDeleteHook is
// deliberately NOT widened to cover this, to keep the persisted-identity census bijection
// honest.
//
// Atomic for the same reason as the save hook above.
var mqttTestDrainDeleteHookV atomic.Pointer[func(durable string, noWait bool) (send bool, observed error, override bool)]

// Test hook: fired after retirement's critical section, with sess.mu RELEASED and both the
// delivery barrier and the map drop already applied. It is what supplies the schedule in
// which a paused delivery callback is released between the map drop and any LATER closure,
// so a build that moved the barrier after the unlock is measurably different from one that
// did not. A pre-lock hook alone names that interleaving without synchronizing it.
//
// Atomic for the same reason as the save hook above.
var mqttTestDrainRetirementUnlockedHookV atomic.Pointer[func(durable string)]

// Test hook: fired at the top of restoreDrains' per-item loop body — after the todo capture,
// after the inactivity-timer work, and with sess.mu RELEASED, before the per-item lock.
//
// It exists because setMQTTTestQOS12SubFailHook cannot serve here: that one fires inside
// restoreDrains' own sess.mu hold, so waiting there for retirement deadlocks, returning an
// error produces the same end state a dropped retirement check would, and returning nil
// supplies the reverse ordering. This is the only point from which a test can retire the
// captured object and then let the install proceed with nothing else failing.
//
// Atomic for the same reason as the save hook above.
var mqttTestDrainRestorePreInstallHookV atomic.Pointer[func(durable string)]

// Test hook: when set, it is invoked after a teardown claim is CONFIRMED and
// before the first deletion — the exact window a nonce re-check cannot close,
// where a paused broker can let a peer finish the whole teardown and create a
// fresh session before its own deletes land. Nothing else can reproduce that
// interleaving from outside, so the seam is deliberate.
//
// Atomic for the same reason as the save hook above.
var mqttTestTeardownClaimedHookV atomic.Pointer[func(sess *mqttSession, nonce string, markerSeq uint64)]

// Test hooks: when set and returning a non-nil error, loadLastMsgFor and
// deleteMsg fail with it WITHOUT issuing the JetStream request, so the stream
// is left exactly as it was and a retry can still succeed. Used to exercise the
// QoS2 PUBREL failure classification.
//
// Deliberately per-operation and predicated on (stream, subject) / (stream,
// sequence) rather than one hook on jsa.newRequest: the broker issues unrelated
// JetStream requests concurrently from other goroutines, and a blanket hook
// would fail those too and turn these tests into misleading noise. Atomics for
// the same reason as the save hook above.
// mqttTestStoreMsgHookV is the same idea for storeMsg, predicated on the publish
// subject (so a test can target only "$MQTT.msgs.>" or only "$MQTT.qos2.in.>" and
// leave session persistence and JSA traffic alone). It also receives the encoded
// message, which lets a test OBSERVE what was written — the Nats-Msg-Id stability
// assertions need that and cannot get it any other way. Returning a non-nil error
// fails the store without issuing the request; returning nil lets it proceed.
var (
	mqttTestLoadLastMsgHookV atomic.Pointer[func(stream, subject string) error]
	// mqttTestLoadNextMsgHookV fails a forward scan, so a test can make the
	// past-PUBREC barrier's walk unreadable and assert it fails CLOSED rather than
	// reading an unreadable scan as "no PUBREL owed".
	mqttTestLoadNextMsgHookV atomic.Pointer[func(stream, subject string, seq uint64) error]
	mqttTestDeleteMsgHookV   atomic.Pointer[func(stream string, seq uint64) error]
	mqttTestStoreMsgHookV    atomic.Pointer[func(subject string, hdrLen int, msg []byte) error]
	// mqttTestCloseCertPersistV observes every durable closure certificate a binding persists
	// (F-105 8c), before the write; returning false suppresses the write. Process-wide because
	// the persister runs in its own goroutine with no account in reach.
	mqttTestCloseCertPersistV atomic.Pointer[func(cert *mqttDurableCloseCert) bool]
)

// mqttTestExpiryNowV, when set, is the clock mqttForwardExpiry reads instead of time.Now.
// It exists so a fixture can make a Message Expiry Interval ELAPSE for the delivery gate
// while the stored message's JetStream TTL — real time, the same length — has not reaped
// it: the gate's decision on a possibly-attempted or unresolved-intent entry is otherwise
// reachable only in the race between the two clocks.
var mqttTestExpiryNowV atomic.Pointer[func() time.Time]

// setMQTTTestDeleteMsgHook installs (or, with nil, clears) the deleteMsg hook
// and returns a function that restores the previous value.
// mqttTestSuppressMsgTTLV drops the JetStream per-message TTL derived from a v5
// Message Expiry Interval, leaving the MQTT property itself intact. See
// mqttStoredMsgTTL.
var mqttTestSuppressMsgTTLV atomic.Bool

// mqttTestSessRecordBodyHookV observes the EXACT BYTES one session-record write is about
// to store, taken from the same marshal the store uses.
//
// It exists because the live session maps are not that payload: saveWithDetail marshals
// the record and RELEASES sess.mu before it stores, so anything a test reads off the
// session afterwards may already have moved. Reading the record back is not an
// alternative either — a JetStream read issued from inside the write's own completion
// blocks the packet that is making it.
//
// Fired after the lock is released, on bytes that are already immutable, so the hook may
// do whatever it likes with them. Paired with the result hook below, which is what says
// whether these bytes actually landed.
//
// Atomic for the same reason as the save hook above.
var mqttTestSessRecordBodyHookV atomic.Pointer[func(sess *mqttSession, body []byte)]

// mqttTestSessStoreResultHookV rewrites the outcome of a SESSION RECORD write
// after it has been issued, so a test can produce the one case the pre-write hook
// cannot: a write that LANDED and whose response was lost. That is what makes
// sess.seq stale and drives the three-way readback.
var mqttTestSessStoreResultHookV atomic.Pointer[func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error)]

// mqttTestSubsBarrierHeldHookV fires inside processSubs with the subs barrier HELD and
// before any JetStream consumer has been created, which is the window interest-based
// retention makes dangerous: a QoS 1/2 message stored there finds no interest and is
// discarded outright rather than delayed. A test blocks in it to prove the barrier holds a
// concurrent store, instead of staggering the two with a sleep and hoping. nil in production.
var mqttTestSubsBarrierHeldHookV atomic.Pointer[func(filters []*mqttFilter)]

// mqttTestSubsBarrierSampledHookV fires inside mqttSubsBarrier.wait after it has taken its
// snapshot of the live entries and released the barrier's lock, before it blocks. sampled is
// how many entries the waiter will wait for. It is the only way a test can establish that a
// SUBSCRIBE entered AFTER a waiter sampled, rather than inferring that order from a timeout.
// Tests filter on b, so other barriers' waits are ignored. nil in production.
var mqttTestSubsBarrierSampledHookV atomic.Pointer[func(b *mqttSubsBarrier, sampled int)]

// mqttTestSessStoreSuppressV drops a session-record store before it is issued and
// reports an unclassifiable error, producing an ambiguous failure whose write
// genuinely did NOT land. See storeSessionMsg.
var mqttTestSessStoreSuppressV atomic.Bool

// mqttTestPubRelPreForwardHookV fires AFTER a drained exchange's claim removal has
// been persisted and BEFORE the PUBREL is forwarded — the exact window a
// forward-first implementation would leave open. A test can inspect the persisted
// record there and prove the ordering, rather than inferring it from a state that
// both orderings eventually reach.
var mqttTestPubRelPreForwardHookV atomic.Pointer[func()]

// mqttTestPublishDrainedPauseV stalls a PUBLISH send section AFTER its durable action has
// been drained and BEFORE the local section is released — the exact window in which
// admission refuses a stored PUBREL on the LOCAL hold alone, with no durable token to be
// seen. Without a seam here that window is a few microseconds wide and unreachable from a
// test, so the local-hold half of the redrive waiter could only be asserted at the unit
// level, never on the wire.
var mqttTestPublishDrainedPauseV atomic.Pointer[func(pi uint16)]

// mqttTestPubRelDeliveryDelayV stalls PUBREL delivery, so a test can let the
// SOURCE redelivery win the race the past-PUBREC barrier exists to settle. Without
// it the callback always arrives first and the barrier looks redundant.
var mqttTestPubRelDeliveryDelayV atomic.Pointer[func()]

// mqttTestPubRelDeliveryDoneHookV is DEFERRED at the entry of mqttDeliverPubRelCb, so it fires
// when that callback RETURNS, through whichever exit, with the reply of the record it was
// handed. It is the completion signal a negative PUBREL assertion needs: the transmission is
// written inside the callback's send section, so "this delivery wrote no PUBREL" is decidable
// only once the callback that received it has returned, and the reply names which delivery
// that was. Paired with mqttTestPubRelWriteBlockV, which counts the writes themselves.
var mqttTestPubRelDeliveryDoneHookV atomic.Pointer[func(sub *subscription, reply string)]

// mqttTestPubRelWriteBlockV stalls a PUBREL transmission INSIDE its send section —
// after the identifier is reserved and before the socket write — so a test can hold a
// duplicate's write open while the client's PUBCOMP for an earlier copy arrives on the
// read loop. That interleaving is the whole of F-089's primary execution, and without a
// seam here it is unreachable from a test: the enqueue returns too quickly to be raced,
// so a fixture would have to call the primitive directly and would then prove nothing
// about this callback's own wiring.
// mqttTestCustodyTransferV counts ENTRIES to the inline custody repair, which is the only
// way to observe its once-per-delivery contract. Counting durable writes cannot: one
// legitimate invocation issues up to mqttInflightStoreAttempts of them, because
// casInflightSlot retries internally, so a write count conflates "the repair ran twice"
// with "one repair retried".
var mqttTestCustodyTransferV atomic.Pointer[func()]

var mqttTestPubRelWriteBlockV atomic.Pointer[func()]

// The two MIXED output sites (F-105 WP-F step 1): the raw writers that still carry supported QoS 0
// and actionless acknowledgements beside chapter P's slot-only QoS 1/2, and so stay outside both
// boundaries. A call count cannot say which traffic a site carries; this hook lets a test watch the
// frames themselves and show that no supported QoS 1/2 PUBLISH or PUBREL ever passes.
const (
	mqttMixedSiteProtoLocked = "mqttEnqueueProtoLocked"
	mqttMixedSitePublishRaw  = "mqttEnqueuePreparedPublishMsgTo raw branch"
)

var mqttTestMixedSiteFrameV atomic.Pointer[func(c *client, site string, frame []byte)]

// mqttTestObserveMixedSite reports one frame at a mixed site to the test hook, if one is set. It
// is handed the frame's FIRST part only: the fixed header is what says what the packet is.
func mqttTestObserveMixedSite(c *client, site string, frame []byte) {
	if h := mqttTestMixedSiteFrameV.Load(); h != nil {
		(*h)(c, site, frame)
	}
}

// mqttTestRetainedFlushBlockV stalls the SUBACK tail at the one instant F-091's execution needs
// held open: the items are staged and reachable, their packet identifiers are reserved, and
// NOTHING has been transferred to the driver or written yet (F-105 7c: the tail writes nothing
// itself). A test can then run clear() on another goroutine and observe whether the identifier
// survives the discard. It fires under c.mu, before the transfer.
var mqttTestRetainedFlushBlockV atomic.Pointer[func([]*subscription)]

// mqttTestStoreMsgResultHookV is the POST-store sibling of mqttTestStoreMsgHookV:
// it runs after the store has actually been issued, and rewrites what the caller
// sees. That is the half that matters for ambiguity — a store that landed, and
// may already have been delivered, whose response was lost — which the pre-store
// hook cannot model at all, since it prevents the store from happening.
var mqttTestStoreMsgResultHookV atomic.Pointer[func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error)]
