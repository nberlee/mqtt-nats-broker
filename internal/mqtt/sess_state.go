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

// The session (mqttSession), the persisted session and Will record shapes, session creation,
// and incarnation identity.

package mqtt

import (
	"sync"
	"sync/atomic"
	"time"
)

type mqttSession struct {
	// subsMu is a "quick" version of the session lock, sufficient for the QoS0
	// callback. It only guarantees that a new subscription is initialized, and
	// its retained messages if any have been queued up for delivery. The QoS12
	// callback uses the session lock.
	mu     sync.Mutex
	subsMu sync.RWMutex

	id     string // client ID
	idHash string // client ID hash
	c      *client
	jsa    *mqttJSA
	// retiringOwnerCID is the connection a two-phase retirement DETACHED before the
	// re-mint (F-036 unit 1). Its F-089 send-hold debts — an unanswerable PUBCOMP, an
	// unwritten retained frame — are still owed and still hold their identifiers: the
	// displaced connection is closed on another goroutine, so its write can outlive
	// the discard, which is why sendsInFlight survives it at all (F-087 WP5). Without
	// this the predicates would compare against a nil sess.c, read every one of those
	// debts as inert, and let the fresh incarnation mint the same identifier.
	// Cleared when the retiring owner's last hold goes, and at the next bind.
	retiringOwnerCID uint64

	// ACK-only allocations are independent of scheduling and Receive Maximum.
	// Both the index and its immutable values are protected by mu.
	abandonedAcks map[mqttAbandonedAckIndex]*mqttAllocationAckBinding
	// withdrawOwed is the owed withdrawals of the CURRENT generation, keyed by lineage (F-088,
	// outbound_withdraw_owed.go). Owned by the custody dispatcher; protected by mu; replaced
	// with the generation.
	withdrawOwed map[string]*mqttWithdrawOwed
	// admissionContinuity refuses the offers of a source delivery whose abandoned admission may
	// have landed (F-097 WP5, outbound_admit_continuity.go), keyed by (durable, sequence). Owned by
	// the custody dispatcher; protected by mu; replaced with the generation.
	admissionContinuity map[mqttDeferredKey]*mqttAdmissionContinuity
	// Observes the installation boundary under mu, before either scheduling link goes.
	testAbandonedAckInstalled func()
	// testRetainedAfterAdmit runs in the retained driver after an admission landed and before the
	// departure check and the transmission (F-105 7c). Nil outside tests; read under mu.
	testRetainedAfterAdmit func(pi uint16)

	// leases is the completion-ownership ledger of the CURRENT incarnation
	// (F-036 unit 1; see sess_lease.go). Written only by setIncarnationLocked —
	// a fresh tracker per incarnation — and read under sess.mu. Owners acquired
	// against a retired tracker are refused, which is what stops a displaced
	// route serving under a re-minted generation.
	leases *mqttIncarnationTracker
	// retiredGenProven memoizes the ONE retirement observation that is monotone
	// (F-036 unit 2): a valid watermark naming this session's own generation. Once a
	// generation is retired it never becomes rightful again, and the single-slot
	// watermark can lose the fact's DISCOVERABILITY but not the fact — so remembering
	// it costs a release nothing and survives the slot being replaced.
	//
	// It is keyed BY the generation rather than merely set, which is what makes a
	// re-mint invalidate it without setIncarnationLocked having to clear anything: a
	// fresh generation simply does not match. The three NEGATIVE outcomes are never
	// remembered, because each can be invalidated by a retirement landing afterwards
	// and a finite window never arbitrates ownership.
	//
	// Guarded by mu. See classifyQoS2ReleaseAgainstRetirement.
	retiredGenProven string
	// asm is the account session manager this session belongs to. Needed by the
	// shared QoS 2 affinity protocol, whose durable state (the assignment claims)
	// is account-scoped and reached through instance-scoped helpers, from paths
	// like clear() that are handed no other account context.
	asm         *mqttAccountSessionManager
	subs        map[string]byte     // Key is MQTT SUBSCRIBE filter, value is the subscription QoS
	noLocalSubs map[string]struct{} // Set of SUBSCRIBE filters that carry the v5 No Local option
	rapSubs     map[string]struct{} // Set of SUBSCRIBE filters that carry the v5 Retain As Published option
	subIDs      map[string]int      // SUBSCRIBE filter -> v5 Subscription Identifier (absent = none)
	// sharedSubs holds MQTT 5.0 Shared Subscriptions, keyed by session key
	// ("$share/{ShareName}/{NATS subject}") so distinct groups (and a plain
	// subscription) on the same subject do not collide. Kept separate from subs
	// so that every reader of the persisted record finds in Subs only literal topic
	// filters: a "$share/..." key is not one, and nothing may subscribe to it as
	// though it were. rapSubs/subIDs are shared with plain
	// subscriptions but keyed by session key (collision-free). Spec5 [4.8.2].
	sharedSubs map[string]mqttSharedMembership

	// The record transaction fence. Every write to this session's record — save or
	// delete — happens inside an open transaction, so a writer cannot persist a
	// mutation another transaction may still roll back, and so a writer from a
	// terminated generation cannot recreate the record. recordEpoch is the record
	// generation, bumped by clear(); recordTxID identifies the transaction that is
	// currently open, and recordTxSeqno mints those identities. See
	// sess_recordtx.go.
	recordTxOpen  bool
	recordTxID    uint64
	recordTxSeqno uint64
	recordEpoch   uint64
	recordTxCond  *sync.Cond
	// recordTxWaiters counts the blocking writers that have ANNOUNCED themselves for
	// the fence. Atomic, and deliberately not under mu: a blocking writer announces
	// itself BEFORE contending on mu, which is what makes the announce the
	// linearization point of waiter priority. A non-blocking acquisition linearizes
	// at its successful predicate load under mu (fence free AND count zero), so while
	// an announce remains outstanding no non-blocking caller whose load follows it can
	// acquire — later loads, after the waiter completes and withdraws, succeed
	// normally. At most ONE transaction already past its successful free-fence
	// predicate at the announce — blocking, try, or the inline handoff, since mu
	// serializes them all — may still open in wall time after it; it counts as
	// already admitted at the announce.
	recordTxWaiters atomic.Int64

	// seqUncertain marks a record write whose outcome is UNKNOWN (it may have
	// landed with its response lost), with pendingWrite holding that write's
	// identity so a readback can classify it. While unresolved, save, clear and the
	// record delete refuse at entry, and deferredDisconnect carries the MQTT
	// obligations the close path could not discharge — with their ORIGINAL
	// deadlines, so an outage cannot silently extend the session or its Will Delay.
	// See sess_uncertain.go.
	seqUncertain       bool
	pendingWrite       *mqttPendingWrite
	deferredDisconnect *mqttDeferredDisconnect
	// recordRejectedSeq is the sequence a DEFINITELY REJECTED record CAS was based
	// on, held as OUTSTANDING WORK rather than as a settled fact. The write did not
	// land, so sess.seq is still right and the session stays usable — but the subject
	// MOVED, so this broker's belief that it owns the session may be obsolete, and on
	// an unbound never-expiring session nothing else ever asks again. Zero means none.
	//
	// Guarded by mu; validity is the predicate recordRejectionPendingLocked, not the
	// field alone. recordRejectedArmed is whether there IS a marker, which the sequence
	// can no longer stand in for: a base-0 CAS is real since F-029, so zero is a
	// legitimate rejection base rather than "none". See sess_rejected_write.go.
	recordRejectedSeq   uint64
	recordRejectedArmed bool
	// gen is this session INCARNATION's durable generation. It namespaces every
	// resource whose name would otherwise derive from idHash alone — the PUBREL
	// durable, subject and delivery subject, and the inbound QoS 2 hold subjects —
	// because idHash is reused by a later session of the same client ID and a
	// deterministic name is therefore not an identity. Minted at creation,
	// re-minted by clear(), preserved by a reconnect or resume. Never empty for a
	// session restored from a record: a record without a generation predates them
	// and is REFUSED, not interpreted under the old names. See sess_incarnation.go.
	gen string
	// wid / pwid are the identity of the record write this broker was last
	// ACKNOWLEDGED for, and that record's own parent. Every save stamps a fresh wid
	// with pwid set to this one, so the durable chain is a parent pointer and a body
	// CLAIMING our own parent is recognisable as such. Adopted wherever seq is —
	// a confirmed save, an ambiguity resolved landed, a repair save proven durable —
	// because a lineage that lags the sequence would parent the next write to a
	// record that is no longer the base. See sess_fork_lineage.go.
	wid, pwid string
	// grantBody / grantBodyParent are the RECORD BYTES of the record wid names and of the
	// record pwid names — the fourth and fifth lineage facts, and the two a fork's repair is
	// computed from. Their delta is exactly what our acknowledged record carried, and reconciling
	// against the RETAINED body is the only thing that can tell "our addition was erased"
	// from "the survivor removed a base field". The baseline's standing is that this broker
	// HELD it, not that the survivor is proven to descend from it. See sess_fork_repair.go.
	//
	// BYTES, not a built grant: the body is already marshalled by the save that lands, so a
	// rotation costs two slice assignments and the grant is materialised only on the rare fork
	// path. They are immutable by construction — json.Marshal returns a fresh buffer and the
	// CAS framing copies into its own — which is also what makes them a stronger provenance
	// than any snapshot of live memory could be (buildPersistedSessionLocked returns
	// sess.subs / sess.subIDs / sess.sharedSubs by reference).
	//
	// Rotated at every adoption that can later mint (a confirmed save, an ambiguity resolved
	// landed, a repair save proven durable, and the resume restore — which has no known
	// parent here and so sets the parent nil), and cleared by clear(). A nil body means
	// nothing is repairable, which is the whole invariant: it is NOT "grantBodyParent always
	// names pwid", because a re-minted object's first write carries a PWID whose content this
	// broker never held, and a record over mqttGrantBodyMax is deliberately not retained at
	// all. Memory-only. See sess_fork_repair.go.
	grantBody, grantBodyParent []byte

	// forkFence is the outbound authority sequence fork repair sampled BEFORE it read whether the
	// consumers it reinstates still serve (F-105 7f); zero when this pass reinstated none. The
	// accepting CONNECT's own handoff preparation CASes exactly it, so a disposal landing after the
	// read moves the authority and refuses the accept rather than letting a decided consumer into
	// the record. forkNotServing names the consumers that read refused, for the merge alone. Both
	// are written under the accept transaction and reset by every pass.
	forkFence      uint64
	forkNotServing map[string]struct{}
	// boundEnt is the accepted entitlement of the connection most recently ACCEPTED on this
	// session object (F-105 7b): set in the accept loop beside the connection's own
	// setOutboundEntitlement, kept across a normal detach (a detached close still acts for the
	// session until a successor accepts), replaced only by the next accept. It is what a NEW
	// void verdict is staged against (stageVoidVerdictLocked): the verdict map is
	// shared by the object, so a stale connection's staging after a same-object successor
	// accepted would otherwise be persisted by the successor's own saves. boundEntOK is false
	// until some connection has been accepted; the zero value authorizes nothing.
	boundEnt   mqttOutboundEntitlement
	boundEntOK bool
	// regClaim is this session OBJECT's ownership identity for shared-delivery
	// in-flight registers. Unlike gen it is deliberately not persisted: a resume on
	// another broker constructs a new object and therefore takes a new claim, while a
	// same-broker rebind keeps the object and its claim. See inflight_register.go.
	regClaim string

	// pubRelRedrive is the identity-bound WAITER set for stored PUBRELs that admission
	// refused because a PUBLISH action was still live on their slot (F-098).
	//
	// It exists because exclusion without a continuation is indistinguishable from a
	// stall. The schedule is ordinary, not exotic: the PUBLISH bytes reach the client,
	// the client answers PUBREC immediately, the PUBREC handler stores the PUBREL, and
	// the PUBREL's own delivery callback — an independent goroutine — finds the PUBLISH
	// action still draining and correctly refuses. Nothing then woke it, so the exchange
	// waited out AckWait: 30 s by default, where a client, or a test, gives up in 10.
	// The refusal is right; the silence after it is not.
	//
	// KEYED BY THE STORED RECORD, not by the identifier alone and not by the reply.
	//
	// One reply per identifier was wrong in one direction and a raw list of replies in
	// the other. Several stored PUBREL records under one identifier is a SUPPORTED shape
	// — the slot names one canonical record and a stale broker's is a duplicate — so two
	// distinct records refused in the same window must BOTH keep a continuation; a single
	// slot silently abandoned one. But a record AckWait re-offers while the blocker still
	// stands is the SAME waiter at a new address, and appending it grows the set for the
	// life of a permanently blocked slot.
	//
	// So the identity is the record — its PUBREL consumer durable and its own stream
	// sequence — and the value is the newest delivery's reply. Repeated offers of one
	// record REPLACE its retry address; distinct records COEXIST. The delivery sequence
	// is carried for exactly one reason: a slow callback for an older delivery must not
	// overwrite a newer registration's address with a stale one.
	//
	// Values are dropped when consumed: a redrive is one-shot, and a later refusal
	// registers again.
	pubRelRedrive map[uint16]map[mqttPubRelRedriveKey]mqttPubRelRedriveWaiter

	cons map[string]*ConsumerConfig
	// drains holds consumers that survived the removal of their membership — an
	// UNSUBSCRIBE, or a re-SUBSCRIBE downgrading the filter to QoS 0 — with started
	// deliveries
	// still pending, keyed by sid; drainByDur is the same set keyed by durable, for
	// the delivery callback. A drained consumer lives EXCLUSIVELY here — its
	// cons[sid] entry is gone — so nothing can reach it through cons and create a
	// second durable on the same filter (I7). See unsub_drain.go.
	drains     map[string]*mqttDrain
	drainByDur map[string]*mqttDrain
	// adoptedDrains retains the drain object a re-SUBSCRIBE ADOPTED, keyed by sid, for as
	// long as its consumer is held under a sid whose membership key is absent.
	//
	// The invariant it exists for (I8): while such a durable is held, EVERY record written
	// names it — in Cons where the membership survives, in Drains where it does not. The
	// orphan sweep deletes any consumer no record references and protects only sessions
	// resident on the SWEEPING broker, so between adoption and the packet's final update a
	// sibling would otherwise delete a live session's durable with its started deliveries
	// inside it, on an interest-retained stream. No crash and no failure is needed.
	//
	// The object is out of drains and drainByDur, so nothing on the delivery path changes:
	// the callback finds no drain for that durable and the consumer behaves as the ordinary
	// consumer the client subscribed to. This map is read by the record builder and by
	// settlement, never by delivery.
	//
	// Released only by SETTLEMENT — a confirmed disposition that gives the sid ordinary
	// membership, hands responsibility to an active drain, or commits its deletion. A
	// changed cons pointer is not evidence that disposal was authorized. In-memory only:
	// what it supports is NAMING, and naming is carried across a crash by the record it
	// produced. See unsub_drain.go.
	adoptedDrains map[string]*mqttDrain
	// pendingDrainDisposals names retired drain durables whose consumer delete is owed
	// but could not be issued where retirement ran, because that goroutine was holding an
	// open record transaction. The disposal WAITS for an answer, and no save on the
	// session can proceed while the fence is held, so it is drained at the release
	// boundary in endRecordTxLocked instead. In-memory only. See unsub_drain.go.
	pendingDrainDisposals []mqttDrainDisposal
	// sharedCharged tracks the MaxAckPending this session has added to tmaxack
	// for each shared group durable it joined (durable -> amount), so a release
	// subtracts exactly what was charged and only once. In-memory only. Spec5 [4.8.2].
	sharedCharged map[string]int
	// sharedTenure is WHICH CONNECT of this generation may acquire shared group durables: the
	// stream sequence of this acceptance's own create-only fact, and the authority tenure the
	// acceptance was finalized under — one tenure, named on both streams (F-105 8b). Fixed for
	// the connection; every join and fence it composes names exactly this, and every acceptance
	// installs it on the generation's acquisition ledger before it reconciles or serves. See
	// mqttSharedTenure and mintAcceptanceTenure.
	sharedTenure mqttSharedTenure
	// acceptFact names the acceptance fact the adopted metadata batch committed: its subject and
	// the digest of its body. Set by adoptCommittedMetadata — the one adoption point of both a
	// live acknowledgement and a resolved commit — so the association survives a lost response,
	// and read back by mintAcceptanceTenure for the fact's stream sequence. In-memory only.
	acceptFact mqttAcceptFactRef
	// acqReleases are the shared-acquisition RELEASE VERDICTS this session owes, keyed by
	// acquisition nonce and persisted as mqttPersistedSession.AcqReleases: written into the
	// record by the transaction that removes or cancels a membership, executed after that
	// write is confirmed, and dropped only once the ledger read back no longer lists the
	// acquisition. Guarded by mu.
	acqReleases map[string]mqttSharedAcqRef
	// verdictKick is the owner's re-trigger of a WITHHELD release (F-105 7e): which generation's
	// run is active and whether another dependant left source delivery while it ran. See
	// shared_verdict_kick.go. Guarded by mu.
	verdictKick mqttVerdictKick
	// consCharged is the same accounting for PLAIN durables. A consumer restored
	// from the session record is never charged (only the create path charges), so
	// refunding its config's MaxAckPending on teardown drove tmaxack negative after
	// a resume and inflated the account's remaining budget. In-memory only.
	consCharged map[string]int
	// restoreGate is this CONNECT's "nothing may be transmitted yet" verdict, read
	// by every delivery callback BEFORE it touches sub.mqtt. Atomic rather than
	// lock-guarded because a PUBREL can enter its callback before the subscription
	// it arrived on has any metadata at all. See sess_restore_gate.go.
	restoreGate atomic.Pointer[mqttRestoreGate]
	// subGate is the CURRENT live-SUBSCRIBE packet's "nothing this packet touched may be
	// transmitted yet" verdict, and subGateGen mints its generation. Both are in-memory
	// only. Atomic for the same reason restoreGate is: a PUBREL can enter its callback
	// before the subscription it arrived on has any metadata at all.
	//
	// A waiter must satisfy the gate's OWNER and GENERATION, not merely a non-nil pointer
	// — see mqttSubGate, where the two together are what keep an abandoned verdict from
	// crossing into the next connection and a waiter from escaping the next packet's
	// gate, up to that waiter's final read. See sub_operation_gate.go.
	subGate        atomic.Pointer[mqttSubGate]
	subGateGen     atomic.Uint64
	pubRelConsumer *ConsumerConfig
	// pubRelVerified is the PUBREL path's equivalent of q2AffReady: this connection
	// has PROVEN the durable exists, rather than trusting a config restored from the
	// record. Every restore starts it false. See
	// ensurePubRelConsumerSubscription.
	pubRelVerified bool
	// uncertainLanded holds what callers deferred for want of proof that a record
	// write landed, for mutations retained because it MAY have. Run by the resolver
	// once it proves those exact bytes durable. See retainOnLanded.
	uncertainLanded []func(recordTx)

	pubRelSubscribed       bool
	pubRelDeliverySubject  string
	pubRelDeliverySubjectB []byte
	pubRelSubject          string
	seq                    uint64

	// Session-affine delivery of started shared-subscription QoS 2 messages.
	// q2AffGen is this session incarnation's durable generation: it namespaces the
	// copy subject, the consumer durable and the deliver subject, because idHash
	// alone is reused by a later clean session of the same client ID. Minted at
	// first provisioning, persisted, restored, and dropped by clear().
	//
	// q2AffReady is the ONLY thing a move may consult. It means "the consumer and
	// its delivery subscription exist AND the session record naming them is
	// saved" — a non-nil config proves neither, and $MQTT_out is interest-retained,
	// so a copy stored before the consumer exists is silently discarded by a store
	// that returns success. Every restore starts it false. See
	// ensureQ2AffineConsumerSubscription and qos2_shared_affine.go.
	q2AffGen              string
	q2AffConsumer         *ConsumerConfig
	q2AffSubscribed       bool
	q2AffDeliverySubject  string
	q2AffDeliverySubjectB []byte
	q2AffReady            bool
	// q2AffClosing / q2AffInFlight / q2AffCond are the move-operation lifecycle
	// fence. A move captures readiness under mu, releases it, then performs
	// JetStream operations, so a delayed claim CAS could otherwise land AFTER
	// clear() finished — creating a claim for a terminated generation that stalls
	// the group delivery forever, since contenders wait on a `claimed` record.
	// clear() sets q2AffClosing (no new move may start), then waits on q2AffCond
	// until q2AffInFlight drains, and only then scans and terminates the claims.
	q2AffClosing  bool
	q2AffInFlight int
	q2AffCond     *sync.Cond

	// qos2RelClosing / qos2RelInFlight / qos2RelCond are the same fence for the
	// INBOUND QoS 2 release (qos2_release_claim.go), and exist for the same reason:
	// a PUBREL claims the release and then performs JetStream round trips, so
	// without this a release admitted before clear() could still fan out after the
	// discard finished and its CONNACK reported Session Present false. The durable
	// claim arbitrates that across brokers; this closes it outright on THIS one,
	// where it can be closed. clear() sets qos2RelClosing (no new release may start)
	// and waits on qos2RelCond until qos2RelInFlight drains, before it scans.
	qos2RelClosing  bool
	qos2RelInFlight int
	qos2RelCond     *sync.Cond

	// pendingPublish maps packet identifiers (PI) to JetStream ACK subjects for
	// QoS1 and 2 PUBLISH messages pending delivery to the session's client.
	pendingPublish map[uint16]*mqttPending

	// pendingPubRel maps PIs to JetStream ACK subjects for QoS2 PUBREL
	// messages pending delivery to the session's client. It is NOT the whole of a
	// PUBREL's identifier reservation: a PUBCOMP untracks the entry while a duplicate
	// copy may still be unanswered, and sendsInFlight is what holds the identifier
	// across that (F-089).
	pendingPubRel map[uint16]*mqttPending
	// pubCompOwed holds every PUBCOMP completion or terminal cleanup of an ORDINARY exchange that
	// is not yet settled, keyed by identifier and exchange nonce. Owned by its completion worker
	// ALONE: no pending-entry replacement, untrack or teardown path edits it, which is what lets
	// it survive a redelivered PUBREL replacing the terminal placeholder (pubcomp_completion.go).
	pubCompOwed map[mqttPubCompKey]*mqttPubCompOwed
	// refusedCarriers remembers which PUBREL RECORDS this connection has already
	// reported as carrying an affine reference this broker does not write, keyed by
	// (PUBREL durable, stream sequence). A refused carrier is withheld and re-offered by
	// AckWait forever, so the diagnostic explaining it is said once per record rather
	// than once per attempt. See noteQ2AffCarrierRefused for why it is keyed by the
	// record and not by the packet identifier or the pending entry.
	refusedCarriers map[mqttDeferredKey]struct{}

	// cpending maps delivery attempts (that come with a JS ACK subject) to
	// existing PIs.
	cpending mqttConsumerPending // composite key: jsDur, sseq; see qos_consumer_pending.go

	// srcAckOwed names QoS 2 source deliveries that are past PUBREC but whose
	// acknowledgement could not be proven. Their payload must never be sent again — the
	// client already answered it — so the next delivery attempt of each is acknowledged
	// instead of delivered, and the entry is released once that acknowledgement is
	// confirmed. Keyed by the SOURCE rather than the packet identifier on purpose: the
	// guard in trackPublish tests pendingPubRel, which PUBCOMP removes, so an
	// identifier-keyed suppression stops working exactly when the exchange completes.
	// Created on first use; bounded by unproven acknowledgements, not by traffic. See
	// retireSourceLocked.
	srcAckOwed      map[mqttDeferredKey]*mqttRetiredSource
	terminalSources map[mqttTerminalSourceKey]*mqttTerminalSource
	// ackedSources is every source this session submitted a positive acknowledgement for,
	// bounded (outbound_acked_sources.go): a re-offer of one is declined at the allocation point.
	ackedSources    mqttAckedSources
	terminalRetries map[mqttTerminalRetryKey]*mqttTerminalRetry
	// retainedCleanups owns every retained terminal slot's cleanup until it completes
	// (retained_terminal_cleanup.go).
	retainedCleanups map[mqttTerminalSourceKey]*mqttRetainedCleanup
	// retainedDriver executes the retained replay queues SUBSCRIBE transfers (F-105 7c), and
	// retainedReclaim reclaims their copies; each guards itself.
	retainedDriver  mqttRetainedDriver
	retainedReclaim mqttRetainedReclaim
	// foreground is this broker's foreground-priority handshake over the retained background
	// writers (outbound_foreground_priority.go); testForegroundAnnounce observes an announcement
	// (true) and its release (false), testBackgroundAdmit an admitted background attempt. Both nil
	// outside tests, set before the session is used.
	foreground             mqttForegroundAuthority
	testForegroundAnnounce atomic.Pointer[func(announced bool)]
	testBackgroundAdmit    atomic.Pointer[func()]
	// testBackgroundWait observes a background admission starting to wait on an announcement;
	// testClaimPinned runs immediately BEFORE the SUBSCRIBE name claim's pinning authority read. Nil
	// outside tests.
	testBackgroundWait atomic.Pointer[func()]
	testClaimPinned    atomic.Pointer[func()]
	// testRetainedVerdict replaces a retained PUBLISH's verdict before its send section (F-105 7c).
	// Nil outside tests.
	testRetainedVerdict atomic.Pointer[func(mqttRetainedReplayVerdict, string) (mqttRetainedReplayVerdict, string)]
	// testRetainedInstallRefused, when it reports true, refuses one abandoned retained PUBREL
	// installation as setPendingLocked would (F-105 7c). Nil outside tests.
	testRetainedInstallRefused atomic.Pointer[func() bool]
	// resumeRetainedInstalled: the running resume pass reached its retained installation, so its
	// success owns what it restored (recoverInflightRegisters). Owned by the resume's goroutine.
	resumeRetainedInstalled bool
	// resumeRetainedUnowned: the retained exchanges that pass's installation could not give an
	// owner; any refuses the resume.
	resumeRetainedUnowned []string
	// retainedQuarantined: the retained exchanges the last applied resume QUARANTINED on a positive
	// store answer, with their evidence (consult 107). Session lock.
	retainedQuarantined []mqttRetainedQuarantined
	// testRecoveredRetainedRefuse, when it reports true for a subject, refuses that retained
	// restoration's installation as a conflicting resident would (F-105 7c). Nil outside tests.
	testRecoveredRetainedRefuse atomic.Pointer[func(subject string) bool]
	// testReclaimAfterStep runs after each reclamation step answers, before its result is applied
	// (F-105 7c). Nil outside tests.
	testReclaimAfterStep atomic.Pointer[func(k mqttRetainedReclaimKey, step mqttReclaimStep, err error)]
	// testRetiredConsumerDelete, when set, answers a retired source consumer's physical delete in
	// place of JetStream (F-105 7c-lc): the crash-window and ambiguity tests. Nil in production.
	testRetiredConsumerDelete atomic.Pointer[func(stream, name string) error]
	// testSourceDisposalSite, when set, observes each route that hands a source consumer's
	// disposal to the retirement owner (F-105 7c-lc), so a test proves the route it names was
	// reached. Nil in production.
	testSourceDisposalSite atomic.Pointer[func(site, name string)]
	// testSettleClassified, when set, observes each classification a tenure settlement makes
	// (F-105 7f): the consumer name and its class. Session-owned, so parallel tests never share it.
	testSettleClassified atomic.Pointer[func(name, class string)]
	// testSourceDisposalDrop, when set, drops every source disposal hand-off after the site seam
	// observed it, so a test measuring a different background writer can keep this one out of its
	// window. Never set in production.
	testSourceDisposalDrop atomic.Bool
	// testRetainedClassify runs in the resume's retained classification after its fresh exchange
	// read, once per register per pass (F-105 7c). Nil outside tests.
	testRetainedClassify atomic.Pointer[func(subject string)]
	// retainedDiscovery is the resume's census of resource incarnations, consumed after CONNACK.
	retainedDiscovery mqttRetainedDiscovery
	// testRetainedCleanupDelay replaces the owner's retry pacing. Nil outside tests.
	testRetainedCleanupDelay func(attempt int) time.Duration

	disposals           map[mqttDisposalKey]*mqttDisposal
	disposalsBySource   map[mqttDeferredKey]map[*mqttDisposal]struct{}
	disposalsByPI       map[uint16]map[*mqttDisposal]struct{}
	testDisposalNotify  func(*mqttDisposal)
	testDisposalHandoff func(*mqttDisposal, *mqttTerminalSource)
	// testDisposalSubmitted runs after a disposal's abandonment submission, before the step
	// re-reads the pair. Nil in production.
	testDisposalSubmitted func(*mqttDisposal)
	// testDisposalBeforeSettle runs after a step's pair read classified the exchange for source
	// settlement, before settleSource. Nil in production.
	testDisposalBeforeSettle func(*mqttDisposal)
	// testDisposalBeforePairRead runs at each step attempt before the register read, the one
	// seam that can hold a displaced object's disposal while its successor ends and replaces
	// the pair. Nil in production.
	testDisposalBeforePairRead func(*mqttDisposal)
	testDisposalDispatch       func()
	testDisposalRecovery       func(string, *mqttDisposal, *mqttDisposal)

	// Active recovery scopes; not reset by generation discard.
	terminalRecoveryDepth int

	// sendsInFlight holds every packet identifier a transmission still reserves. It is
	// THE authority on whether an identifier may be re-minted.
	//
	// PRESENCE IS NOT A RESERVATION; sendReservedLocked is. A hold whose write has
	// returned and whose acknowledgement debt belongs to a connection that is no longer
	// ours reserves nothing, and a reader that tested presence would strand the
	// identifier — see mqttSendHold. Every reservation reader asks sendReservedLocked;
	// the field itself is touched only by that predicate, by the helpers that MUTATE a
	// hold (beginSendLocked, beginPubRelSendLocked, endSend, consumePubRelAckLocked, and
	// for a retained frame beginRetainedSendLocked with its two release verbs), and by
	// the sweep, which runs at clear() and again at the CONNECT rebind once ownership has
	// moved.
	//
	// Deliberately NOT nilled by clear(): clear() resets last_pi to zero and a reused
	// session object can mint PI 1 for a new incarnation while the displaced
	// connection's PI 1 write is still blocked in a synchronous socket write. The
	// registry is what carries the reservation across the discard (F-087 WP5 class F2).
	sendsInFlight map[uint16]*mqttSendHold

	// inflightWalks counts this session object's register-recovery walks.
	inflightWalks uint64
	// inflightApplyCompletedWalk is the walk whose APPLY PHASE completed, and it exists to
	// distinguish two histories piSpaceIncomplete alone cannot: "the walk finished and the
	// only uncertainty is these attributable reservations" from "the walk never applied, so
	// an unknown population may have been missed entirely".
	//
	// Equality with inflightWalks means one of exactly two things — the incarnation's
	// initial baseline, or a completed latest apply — and both authorize attributing
	// uncertainty PER SOURCE. Inequality means the latest attempt did not apply, and the
	// restriction is session-wide. An enumeration failure returns before the apply phase
	// and exhausted reclassification never reaches it, so both leave this behind.
	//
	// INCARNATION-SCOPED, which is why setIncarnationLocked rebases it: clear() preserves
	// inflightWalks while discarding the pending maps and re-minting the generation, so a
	// stale mismatch would otherwise impose a failed predecessor's restriction on a fresh
	// session that never ran recovery at all. inflightWalks is deliberately NOT reset — the
	// opaque reservations' own walk stamp reads it.
	//
	// In-memory only. A restored session runs reconstruction before any drain can deliver.
	inflightApplyCompletedWalk uint64

	// voidedRegNonces holds the nonces of registers this session has given up — the
	// VOID-register verdicts. A teardown that abandons a register voids its nonce, whether
	// the allocation was still storing or already transmitted; recovery reads a register
	// carrying one of these exactly as it reads a cancellation tombstone: never owed to the
	// client, so its identifier and Receive Maximum slot are released rather than held. A
	// nonce is minted with its allocation and never reused, which is what a subject, a
	// packet identifier or a shared group durable (whose name recurs while its incarnation
	// stays live) could not offer. Guarded by mu, and persisted as mqttPersistedSession.VoidedRegs. See
	// voidRegNonceLocked and voidRegistrationsLocked.
	// A verdict is SUPERSEDED when recovery restores its allocation as a possibly-attempted
	// acting entry — marker absent, or a recovered output token beside a standing marker,
	// fresh install or same-object resident alike (F-088): the restoration owes the exchange
	// its retransmission, so the nonce is retired (retireSupersededVerdictLocked) and the
	// revision moves for the persistence that records it.
	// The VALUE is the voidedRegsRev at which the nonce was recorded, and it is what a
	// landed save may claim: a save whose captured revision is r provably carried every
	// nonce recorded at or before r, and none recorded after — so a release keyed on
	// membership alone would free a reservation whose verdict a CONCURRENT save never
	// serialized. See releaseSettledVoidReservationsLocked.
	voidedRegNonces map[string]uint64

	// settledVoidReservations indexes, by nonce, the cancelled reservations whose store
	// has SETTLED but whose verdict is not yet proven — the only reservations a later
	// proof edge (a landed save, a stream-rung retirement) still has to release. An index
	// rather than a pendingPublish walk, because retirement runs once per voided register
	// in a teardown and pendingPublish can legitimately hold tens of thousands of
	// entries: walking it per nonce is quadratic teardown work. Entries are rare — only a
	// cancellation whose every durable rung failed at its settle point parks here.
	// Guarded by mu. See voidVerdictProvenLocked and releaseSettledVoidReservationsLocked.
	settledVoidReservations map[string]uint16

	// voidedRegsRev counts changes to voidedRegNonces, and voidedRegsSavedRev is the
	// highest revision PROVEN to be in the session record. They are not a bool because the
	// writer cannot see the race it loses: an off-readLoop teardown appends while a
	// readLoop transaction that has already marshalled its record holds the fence, so that
	// transaction commits without the entry and the append was refused its own save.
	// Driving persistence off the gap between the two — rather than off "did I just change
	// something" — is what makes every convergence point retry until the record agrees.
	// Both guarded by mu. See persistVoidedRegs.
	voidedRegsRev      uint64
	voidedRegsSavedRev uint64
	// slotExclusionInst is the monotone source for every exclusion instance. NEVER reset,
	// including by clear(), which retains sendsInFlight — a counter that restarted would
	// let a stale absence observation lift an exclusion installed after it was taken.
	slotExclusionInst uint64

	// exclReclaimArmed says a reclamation continuation is scheduled for this session, so
	// the N held exclusions share ONE timer rather than one each — a per-identifier
	// continuation would make the timer count track the very residual the pass exists to
	// shrink. Set under mu by armExclusionReclaimLocked, cleared only by a pass that
	// found nothing left to do, lost residency, or was refused admission.
	exclReclaimArmed bool
	// exclReclaimCursor is the packet identifier the last pass VISITED, and the pass
	// resumes at its strict successor with wrap. A VALUE, never an index into the sorted
	// set: an index is not stable under mutation, and a churning set can then starve a
	// persistent entry forever by shifting its rank away from the cursor on every pass.
	// Advanced from the last visited entry even when that entry was lifted, so a lift
	// cannot rewind it. Guarded by mu. See exclusion_reclaim.go.
	exclReclaimCursor uint16
	// testExclReclaim instruments this session's reclaim passes (F-088 T36 rows 18-18d); nil
	// in production. Read under mu at each pass's start.
	testExclReclaim *mqttExclReclaimSeams

	// voidHandoffActive says a release-triggered persistence LOOP is running for this
	// session (attemptVoidVerdictHandoff). It is what keeps successive late stagings on one
	// frame: the loop's own fence and gate releases would otherwise re-enter the handoff
	// decision inside their own frames, recursing once per staged revision with a JetStream
	// round trip held in each. Cleared only in the same sess.mu critical section that finds
	// nothing left pending, so no staging can slip between the last check and the exit.
	// Guarded by mu. See voidVerdictHandoffDueLocked.
	voidHandoffActive bool

	// voidedRegsHandoffRev is the revision the last release-triggered persistence
	// ATTEMPT acquired the record fence at — the handoff watermark, and the reason the
	// handoff makes one attempt per newly staged revision instead of a retry storm or a
	// synchronous recursion without a bound. It advances only on acquisition, never on
	// the decision to try: a trigger that lost the fence stored nothing, and burning the
	// watermark on it would strand the nonce it was firing for. Generation-local, so it
	// is reset wherever voidedRegsRev is. Guarded by mu. See
	// voidVerdictHandoffDueLocked.
	voidedRegsHandoffRev uint64

	// piSpaceIncomplete is set when a resume could not fully reconstruct the durable
	// outbound identifier space, so some identifier the client holds may be unknown
	// here. While it is set bumpPI allocates NOTHING: a new identifier could collide
	// with a register or an unreplayed PUBREL that still owns it. Nothing else is
	// restricted, and a later resume that completes both scans clears it. See
	// inflight_register.go (R3).
	piSpaceIncomplete bool
	// custody is this generation's custody ledger (outbound_custody_ledger.go): the owner of
	// every ordinary exchange's Receive Maximum charge and packet identifier. Replaced
	// wholesale with the generation (setIncarnationLocked). custodyHints, custodyPassWanted
	// and custodyObserving drive its single-flight observer (outbound_custody_session.go).
	custody           *mqttCustodyLedger
	custodyHints      map[string]struct{}
	custodyPassWanted bool
	custodyObserving  bool
	// The autonomous dispatcher's own state (F-097 WP4, outbound_custody_dispatcher.go), all
	// protected by mu and replaced with the generation:
	//   - custodyDemand latches a withholding gate's demand until an allocation since succeeded
	//     with nothing deferred, or until its own deadline finds nothing deferred, no identifier
	//     unknown and no custody held (nothing can still be waiting); custodyDemandDelay is its
	//     backoff;
	//   - custodyRetry is the exchanges whose last read was indeterminate, by lineage;
	//   - custodyPassQueue is the targets of a requested pass not yet run (at most eight units
	//     run per iteration);
	//   - custodyRR is the round-robin cursor across the dispatcher's queues;
	//   - custodyTickAt is the deadline of the armed tick, zero when none, and custodyTickKey its
	//     timer key; custodyTickArm is the identity of the latest arm, increasing for the
	//     session's lifetime (never replaced with the generation), so every arm has its own key
	//     and a superseded arm's callback is inert.
	custodyDemand      bool
	custodyDemandDelay time.Duration
	custodyDemandDueAt time.Time // when the latched demand's next pass is requested
	custodyRetry       map[string]*mqttCustodyRetry
	custodyPassQueue   []mqttCustodyTarget
	custodyRR          int
	custodyTickAt      time.Time
	custodyTickKey     string
	custodyTickArm     uint64
	// The floor unit's state (F-112, outbound_floor_unit.go), protected by mu and replaced with the
	// generation: floorWanted latches its demand until a run finds nothing to do; floorDueAt is
	// when it is next due (zero: now) and floorDelay its no-progress backoff; floorHorizon is this
	// broker's scan toward a qualifying advance (scan state only).
	floorWanted  bool
	floorDueAt   time.Time
	floorDelay   time.Duration
	floorHorizon *mqttFloorHorizon
	// testCustodyDriver instruments the dispatcher (F-097 T2*); nil in production, read under mu.
	testCustodyDriver *mqttCustodyDriverSeams
	// resumeLineage carries, across the bounded re-runs of ONE recoverInflightRegisters
	// invocation, every sequence a custody rewrite authoritatively moved each register
	// through, keyed by the register's nonce. A pass that ends in Reclassify has already
	// LANDED its rewrites (A→B) but never reached its apply phase; the next pass rewrites
	// again (B→C) and, seeing only [B,C], would leave the live entry aimed at dead A —
	// which terminal cleanup's freshness fence then reads as a concurrent rewrite and
	// preserves forever. Seeding each pass's chain from this map makes it [A,B,C]. Live
	// only for the duration of the invocation; nil otherwise.
	resumeLineage map[string][]uint64

	// deferred tracks QoS 1/2 deliveries withheld by trackPublish because the
	// session was at its effective in-flight cap (Receive Maximum /
	// MaxAckPending). Keyed by JS durable + stream sequence; the value is the
	// latest delivery attempt's JS ack subject, NAK'd to force prompt
	// redelivery when the client frees a slot (Spec5 [4.9]). In-memory only,
	// naturally bounded by the consumers' MaxAckPending.
	deferred map[mqttDeferredKey]string

	// "Last used" publish packet identifier (PI). starting point searching for the next available.
	last_pi uint16

	// Maximum number of pending acks for this session.
	maxp uint16
	// rmax is the current connection's MQTT 5.0 Receive Maximum: the max number
	// of unacknowledged QoS1/2 PUBLISH the server may have in flight to the
	// client. 0 means no client-imposed limit (3.1.1, or not sent). The
	// effective in-flight cap is min(maxp, rmax). Set on each CONNECT.
	rmax    uint16
	tmaxack int
	clean   bool
	// expiryInterval is the effective MQTT 5.0 Session Expiry Interval in seconds:
	// 0 means the session ends when the connection closes, mqttSessionNeverExpire
	// means it never expires, any other value keeps the session for that many
	// seconds after the connection closes. For 3.1.1 this is derived from the
	// Clean Session flag (clean => 0, persistent => mqttSessionNeverExpire). Set on
	// each CONNECT and possibly overridden by a v5 DISCONNECT. Spec5 [3.1.2.11.2].
	expiryInterval uint32
	// disconnectedAt is the Unix time the client disconnected (0 while connected).
	// Persisted so a finite deadline survives a restart. It is stamped for a
	// NEVER-EXPIRING session too, which was a delayed-Will fix: a record fence read a
	// newer record with DisconnectedAt == 0 as a live newer connection and cancelled a
	// Will that session was owed. THAT FENCE IS GONE — suppression is decided from the
	// permanent acceptance facts and reads no session record — so on a never-expiring
	// session the stamp is currently inert, every remaining reader being an expiry path
	// that pre-filters on mqttSessionNeverExpire. Retained rather than removed; see
	// docs/CONFORMANCE.md, which says so rather than restating the old rationale.
	disconnectedAt int64
	domainTk       string

	// will is the session's pending delayed Will, persisted with the record so
	// it survives a server restart during the Will Delay Interval. nil when no
	// Will is pending. Spec5 [3.1.3.2.2].
	will *mqttPersistedWill
}

type mqttPersistedSession struct {
	Origin string          `json:"origin,omitempty"`
	ID     string          `json:"id,omitempty"`
	Clean  bool            `json:"clean,omitempty"`
	Subs   map[string]byte `json:"subs,omitempty"`
	// NoLocal and RetainAsPublished are the sets of Subs filters that carry the
	// v5 No Local / Retain As Published options; SubIDs maps a Subs filter to its
	// v5 Subscription Identifier. All kept separate from Subs (rather than packed
	// into its QoS byte) so that Subs' value stays a valid 0..2 QoS for every reader
	// of this record. Spec5 [3.8.3.1], [3.8.2.1.2].
	NoLocal           []string       `json:"no_local,omitempty"`
	RetainAsPublished []string       `json:"retain_as_published,omitempty"`
	SubIDs            map[string]int `json:"sub_ids,omitempty"`
	// SharedSubs is the persisted membership for MQTT 5.0 Shared Subscriptions (session
	// key -> the granted QoS plus the session's ACQUISITION REFERENCES, see
	// mqttSharedMembership). The group consumer's durable and deliver subject are derived
	// from the key AND the incarnation the reference names (mqttSharedGroupNamesFor); the
	// queue name from the key alone. No consumer config is persisted for them: the record
	// builder filters group-owned configs out of Cons as redundant.
	//
	// It is NOT a holder registry. Whether a group consumer may be deleted is decided by the
	// group record's CAS on $MQTT_shared (shared_lifecycle.go), never by walking these maps
	// across the record set; a membership's absence authorizes no release — a release is
	// authorized by a verdict in AcqReleases. Spec5 [4.8.2].
	SharedSubs map[string]mqttSharedMembership `json:"shared_subs,omitempty"`
	// AcqReleases are the RELEASE VERDICTS this session owes: acquisitions whose membership
	// removal (UNSUBSCRIBE, a forced leave, a failed filter's compensation, a rollback) or
	// cancellation (a resume finding an acquisition its record does not name) is durable
	// here, written by that very record transaction, and whose ledger release has not yet
	// been confirmed. The same discipline as VoidedRegs: any reader may EXECUTE a verdict
	// (the release is a CAS removing exactly that entry, so executors converge), only the
	// owner writes or clears one, and what clears one is always a positive fact — a later
	// save after the ledger read back no longer lists the acquisition — never a failure to
	// find it.
	AcqReleases []mqttSharedAcqRef         `json:"acq_releases,omitempty"`
	Cons        map[string]*ConsumerConfig `json:"cons,omitempty"`
	PubRel      *ConsumerConfig            `json:"pubrel,omitempty"`
	// Q2Aff / Q2AffGen are the session-affine QoS 2 delivery path for shared
	// subscriptions: the consumer that delivers copies of started deliveries this
	// session owns, and the generation that namespaces all three of its names.
	// Because the generation is in EVERY name, a leaked affine consumer can never
	// expose one session's in-flight message to another. A record carrying any
	// qos-2 entry in SharedSubs must carry a Q2AffGen; one that does not is refused
	// rather than interpreted (see mqttValidateRecordAffinePair).
	// Spec5 [4.8.2]; see qos2_shared_affine.go.
	Q2Aff    *ConsumerConfig `json:"q2aff,omitempty"`
	Q2AffGen string          `json:"q2affgen,omitempty"`
	// Gen is this session incarnation's generation, which namespaces every
	// resource named after idHash alone (the PUBREL path, the inbound QoS 2 hold
	// subjects). It is mandatory. A record without one predates generations and is
	// REFUSED rather than interpreted under the hash-only names it was written with
	// (F-057): nothing reclaims such a record, so the recovery is recreating the
	// broker-owned streams. See sess_incarnation.go.
	Gen string `json:"gen,omitempty"`
	// WID is this record write's own identity, minted once per write; PWID is the
	// WID of the record the write was derived from (empty only for a first create).
	// The pair is a PARENT POINTER.
	//
	// It used to exist because two writes derived from one base could both apply
	// across a leader transition. That is retired for two BROKER-AUTHORED CONDITIONAL
	// writes — see the same-base boundary in docs/CONFORMANCE.md. What the pointer
	// answers now is the route that boundary leaves open: an UNCONDITIONAL foreign
	// write puts a body in the single slot that a CAS cannot exclude, and sequence and
	// WID alone cannot tell such a body from a legitimate successor of ours.
	//
	// It establishes exactly one thing, and the wording matters: a record carrying our
	// PWID CLAIMS the same parent. It is not proof of descent — restore adopts WID and
	// PWID without validating provenance — and none is needed, because the claim being
	// made is negative: a body claiming our parent does not establish that anybody took
	// the session over.
	//
	// RECOGNITION ONLY: nothing repairs the erased write, and a record whose WID is
	// absent CONCEDES rather than accuses — classifyRecordLineage requires the
	// survivor to carry an identity of its own before the parent match can mean
	// anything. See sess_fork_lineage.go.
	WID  string `json:"wid,omitempty"`
	PWID string `json:"pwid,omitempty"`
	// Drains are the consumers that survived their membership's removal (an UNSUBSCRIBE,
	// or a QoS 0 re-SUBSCRIBE) because deliveries they
	// had already STARTED must still be completed ([MQTT-3.10.4-3]). Keyed by the
	// same sid as Cons, so a "foo/#" filter's level-up companion drains
	// independently. Deliberately kept OUT of Cons, because Cons restores straight
	// into sess.cons: a drain carried there would satisfy the cons[sid] lookup on a
	// re-SUBSCRIBE, take the fromSubProto early return, and leave the filter with no
	// delivery subscription. Drains restore through their own path and a
	// re-SUBSCRIBE adopts one explicitly. See unsub_drain.go.
	Drains map[string]*mqttPersistedDrain `json:"drains,omitempty"`

	// VoidedRegs are the NONCES of registers this session has given up — the
	// void-register verdicts. Written by the transaction that abandons them (a teardown's
	// membership-removal write, or a later convergence-point save for a verdict recorded
	// after that write), and read by recovery, which frees the identifier such a register
	// names instead of reserving it for the life of the session.
	//
	// Keyed by nonce, never by packet identifier, subject or durable: a teardown frees the
	// identifier for immediate reuse, subjects are reused within a generation, and a shared
	// group durable's name recurs for as long as its incarnation stays live, so a rejoin can
	// return the same name — a verdict scoped by any of those would eventually condemn a
	// genuine later delivery. A
	// nonce is minted with the allocation and never reused.
	//
	// WHAT DOES NOT REMOVE AN ENTRY, which is the part worth stating because this comment
	// once claimed the opposite: a resume scan that merely FAILED TO FIND a register
	// carrying it. No such removal exists, and its absence is deliberate — scan-time
	// absence cannot rule out a late create-only CAS landing afterwards, so the scan KEEPS
	// the verdict ("an absence is not a fact").
	//
	// What DOES remove one is always a positive fact that makes the verdict unnecessary or
	// not ours to hold: the register was never created, the cancellation is durable on the
	// register itself, the register is provably gone, a peer's claim on it means the
	// durable decision is the peer's and a competing verdict must not be persisted, or
	// recovery restored the allocation as a possibly-attempted acting entry, superseding the
	// verdict's "never transmitted" (F-088; retireSupersededVerdictLocked). That
	// list is illustrative, NOT a closed set — inflight_register.go's removal sites are
	// the authority, and two review rounds faulted an earlier attempt here for asserting
	// an exhaustive one. Absent such a fact the set is emptied when the generation retires
	// with its record. See inflight_register.go.
	VoidedRegs []string `json:"voided_regs,omitempty"`
	// Teardown, when present, means a broker has CLAIMED this record's teardown and
	// may be part way through deleting the incarnation's resources. Nobody may
	// resume that state: every reader that can INTERPRET the record finishes the
	// teardown (the deletions are generation-scoped; repeating them is safe for names nothing
	// can re-occupy, bounded per walk for the inbound QoS 2 hold walk — F-062 — and bounded
	// by its own enumeration watermark for the `$MQTT_inflight` reap — F-034) and releases
	// the record before starting fresh. A reader that refuses the record's schema does not
	// resume it either, but it does not finish it: the expiry path and the orphan
	// sweep pick a reserved-namespace record up only while its GENERATION is valid,
	// and one without a valid generation is adopted by nothing until the streams are
	// recreated (docs/ARCHITECTURE.md, teardown section). The record keeps its full
	// content under the marker precisely so any broker can finish the job, and so the
	// orphan sweep still reads Cons / SharedSubs / Drains as referenced.
	// See sess_teardown.go.
	Teardown *mqttSessTeardown  `json:"teardown,omitempty"`
	Will     *mqttPersistedWill `json:"will,omitempty"`
	// Session Expiry Interval (seconds) and disconnect Unix time (0 while
	// connected): together the expiry deadline, restart-safe. Spec5 [3.1.2.11.2].
	ExpiryInterval uint32 `json:"expiry,omitempty"`
	DisconnectedAt int64  `json:"disconnected,omitempty"`
}

// mqttPersistedWill was a delayed Will stored in the session record so it would
// still fire if the server restarted during its Will Delay Interval. Spec5
// [3.1.3.2.2].
//
// VESTIGIAL: nothing assigns sess.will in production, so the `will` field is
// always omitted from the record and this shape is never written or read back.
// The active durable path for a delayed Will is the $MQTT_willq outbox
// (mqttWillOutbox, will_outbox.go), and THAT is where a Will's authorization is
// carried -- a record there exists only because the decision passed at CONNECT.
// Not here.
type mqttPersistedWill struct {
	Topic  string `json:"topic"`
	Msg    []byte `json:"msg,omitempty"`
	QoS    byte   `json:"qos,omitempty"`
	Retain bool   `json:"retain,omitempty"`
	Props  []byte `json:"props,omitempty"` // encoded forwardable properties block
	User   string `json:"user,omitempty"`
	FireAt int64  `json:"fire,omitempty"` // Unix nanoseconds to publish at (truncation could fire early)
}

// Returns a new mqttSession object with max ack pending set based on
// option or use mqttDefaultMaxAckPending if no option set.
//
// gen is the session incarnation generation every hash-derived name is scoped by:
// a freshly minted one for a session that did not exist, and the record's own for a
// restore. It is never empty — a record carrying no generation is rejected by
// mqttSessGenFromRecord rather than resumed on hash-only names. See
// sess_incarnation.go.
//
// wid / pwid are the restored record's lineage, adopted ALONGSIDE seq for the same
// reason seq is: the first save after this restore parents itself to the record it
// was derived from, and a restore that dropped the pair would stamp an empty parent
// — making every sibling of the restored base unprovable, so detection would be inert
// after any restart. Both empty for a session that did not exist, and for a record
// written before this field. See sess_fork_lineage.go.
func mqttSessionCreate(jsa *mqttJSA, id, idHash, gen string, seq uint64, wid, pwid string, opts *Options) *mqttSession {
	maxp := opts.MQTT.MaxAckPending
	if maxp == 0 {
		maxp = mqttDefaultMaxAckPending
	}

	sess := &mqttSession{
		jsa:      jsa,
		id:       id,
		idHash:   idHash,
		seq:      seq,
		wid:      wid,
		pwid:     pwid,
		maxp:     maxp,
		regClaim: mqttNewInflightNonce(),
	}
	// A not-yet-reachable object has no owners to drain, so the constructor is
	// the one caller entitled to a trivial permit (F-036 unit 1, property P).
	sess.setIncarnationLocked(gen, mqttRetirementPermit{})
	return sess
}

// setIncarnationLocked adopts gen as this session's incarnation and derives every
// name scoped by it. Called at creation, on restore, and by clear() when it
// re-mints, so the derived names can never drift from the generation the record
// carries.
//
// sess.mu must be held, or the session must not yet be reachable.
//
// The permit is what makes property P — the generation changes only while no
// client is bound — mechanical (F-036 unit 1): it is mintable only by
// retireIncarnationLocked, which asserts sess.c == nil over a closed, drained
// tracker, or as the constructor's trivial permit for an object nothing else can
// reach. A mismatched permit here is memory-protocol corruption, not a runtime
// condition, so it fails loudly. Each incarnation gets a FRESH tracker; owners of
// the retired one keep their captured pointer and are refused there.
func (sess *mqttSession) setIncarnationLocked(gen string, permit mqttRetirementPermit) {
	if sess.leases != nil && permit.tr != sess.leases {
		panic("mqtt: setIncarnationLocked without a permit for the current incarnation")
	}
	// The recovery-outcome provenance is REBASED, because it describes an incarnation that
	// is ending. clear() preserves inflightWalks while discarding the pending maps and
	// re-minting, so without this a failed predecessor's mismatch would impose its
	// session-wide restriction on a fresh session that never ran recovery at all — and a
	// new attributable reservation, which sets piSpaceIncomplete without advancing either
	// counter, would then be misread as that failure. inflightWalks is deliberately not
	// reset: the opaque reservations' own walk stamp reads it.
	//
	// An ABORTED clear never reaches here, and an ordinary resident reconnect never calls
	// this, so both correctly preserve the existing provenance.
	sess.inflightApplyCompletedWalk = sess.inflightWalks
	// The retiring generation's custody tick is CANCELLED (F-097 WP4): its work goes with the
	// ledger below. The timer registry's lock is not taken under the session lock, so the
	// cancellation of the arm's STORED key is handed off; a tick that fires first is inert (its
	// callback starts nothing for a generation that no longer stands), and an arm not yet
	// registered cancels itself once it sees the generation gone.
	if key := sess.custodyTickKey; key != _EMPTY_ {
		if srv := sess.serverFor(nil); srv != nil {
			go srv.cancelWillTimer(key)
		}
	}
	sess.leases = newIncarnationTracker()
	sess.leases.onReopen = sess.rearmCustodyAfterReopen
	sess.gen = gen
	// Generation discard is the one release of custody that is not an observation: the
	// retiring generation's ledger goes whole, and every late callback that still names it
	// is refused by the fresh ledger's generation guard.
	sess.custody = newMQTTCustodyLedger(gen)
	sess.custodyHints = nil
	sess.custodyPassWanted = false
	// The dispatcher's queues and latch go with the ledger (F-097 WP4); the tick was cancelled
	// above (outbound_custody_dispatcher.go).
	sess.custodyDemand, sess.custodyDemandDelay, sess.custodyDemandDueAt = false, 0, time.Time{}
	sess.custodyRetry, sess.custodyPassQueue, sess.custodyRR = nil, nil, 0
	sess.custodyTickAt, sess.custodyTickKey = time.Time{}, _EMPTY_ // custodyTickArm keeps increasing
	sess.floorWanted, sess.floorDueAt, sess.floorDelay, sess.floorHorizon = false, time.Time{}, 0, nil
	// Owed withdrawals name the retiring generation's obligations; its successor's recovery
	// reconstructs any it still owes (F-088).
	sess.withdrawOwed = nil
	sess.admissionContinuity = nil // the retiring generation's admissions (F-097 WP5)
	subject, deliver, _ := mqttPubRelNames(sess.idHash, gen)
	sess.pubRelSubject = subject
	sess.pubRelDeliverySubject = deliver
	sess.pubRelDeliverySubjectB = []byte(deliver)
}
